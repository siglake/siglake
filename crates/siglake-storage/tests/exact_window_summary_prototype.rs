//! Task #270: cap-bounded prototype for exact summaries that replace a
//! depth-4 windowed `GROUP BY` boundary scan.
//!
//! The gated tests pin the two premises that matter: the current engine really
//! scans all four overlapping files with result caches disabled, and each
//! proposed summary returns the same exact counts as that scan. The ignored
//! release-mode report measures build/serialization CPU, query CPU, and the
//! serialized side/footer bytes of the candidate shapes.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::{Duration as StdDuration, Instant};

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use serde::Serialize;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning, TimeBounds};

const DEPTH: usize = 4;
const LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];
const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
const HOUR_NS: i64 = 60 * MINUTE_NS;

// This cap-bounded model uses a 6.9-hour span whose trailing quarter has the
// task card's no-complete-hour property, plus its observed depth, while using
// four million rows for a release-mode local measurement.
const SPAN_SECONDS: i64 = 24_800;
const WINDOW_LO_NS: i64 = 18_600 * SECOND_NS;
const WINDOW_HI_NS: i64 = SPAN_SECONDS * SECOND_NS;

type Counts = [u64; LEVELS.len()];
type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

#[derive(Debug, Serialize)]
struct Summary {
    column: &'static str,
    values: [&'static str; LEVELS.len()],
    /// Zero means exact timestamp points; otherwise this is an aligned bucket
    /// width. Keeping value names once avoids charging every point for them.
    width_ns: i64,
    counts: BTreeMap<i64, Counts>,
}

impl Summary {
    fn exact() -> Self {
        Self {
            column: "level",
            values: LEVELS,
            width_ns: 0,
            counts: BTreeMap::new(),
        }
    }

    fn bucketed(width_ns: i64) -> Self {
        Self {
            width_ns,
            ..Self::exact()
        }
    }

    fn add_exact(&mut self, timestamp_ns: i64, level: usize) {
        self.counts.entry(timestamp_ns).or_default()[level] += 1;
    }

    fn add_bucketed(&mut self, timestamp_ns: i64, level: usize) {
        let start = timestamp_ns.div_euclid(self.width_ns) * self.width_ns;
        self.counts.entry(start).or_default()[level] += 1;
    }
}

#[derive(Debug, Serialize)]
struct MultiResolutionSummary {
    exact: Summary,
    minute: Summary,
}

fn synthetic_timestamp_ns(row: usize, rows_per_file: usize) -> i64 {
    (row as i64 * SPAN_SECONDS / rows_per_file as i64) * SECOND_NS
}

fn synthetic_level(file: usize, row: usize) -> usize {
    (row.wrapping_mul(17).wrapping_add(file.wrapping_mul(3))) % LEVELS.len()
}

fn add_counts(total: &mut Counts, part: &Counts) {
    for (out, value) in total.iter_mut().zip(part) {
        *out += *value;
    }
}

fn scan_depth4(rows_per_file: usize, lo_ns: i64, hi_ns: i64) -> Counts {
    let mut out = Counts::default();
    for file in 0..DEPTH {
        for row in 0..rows_per_file {
            let timestamp_ns = synthetic_timestamp_ns(row, rows_per_file);
            if timestamp_ns >= lo_ns && timestamp_ns < hi_ns {
                out[synthetic_level(file, row)] += 1;
            }
        }
    }
    out
}

fn build_global_exact(rows_per_file: usize) -> Summary {
    let mut out = Summary::exact();
    for file in 0..DEPTH {
        for row in 0..rows_per_file {
            out.add_exact(
                synthetic_timestamp_ns(row, rows_per_file),
                synthetic_level(file, row),
            );
        }
    }
    out
}

fn build_global_bucketed(rows_per_file: usize, width_ns: i64) -> Summary {
    let mut out = Summary::bucketed(width_ns);
    for file in 0..DEPTH {
        for row in 0..rows_per_file {
            out.add_bucketed(
                synthetic_timestamp_ns(row, rows_per_file),
                synthetic_level(file, row),
            );
        }
    }
    out
}

fn build_global_multi_resolution(rows_per_file: usize) -> MultiResolutionSummary {
    let mut out = MultiResolutionSummary {
        exact: Summary::exact(),
        minute: Summary::bucketed(MINUTE_NS),
    };
    for file in 0..DEPTH {
        for row in 0..rows_per_file {
            let timestamp_ns = synthetic_timestamp_ns(row, rows_per_file);
            let level = synthetic_level(file, row);
            out.exact.add_exact(timestamp_ns, level);
            out.minute.add_bucketed(timestamp_ns, level);
        }
    }
    out
}

fn build_per_file_exact(rows_per_file: usize) -> Vec<Summary> {
    (0..DEPTH)
        .map(|file| {
            let mut out = Summary::exact();
            for row in 0..rows_per_file {
                out.add_exact(
                    synthetic_timestamp_ns(row, rows_per_file),
                    synthetic_level(file, row),
                );
            }
            out
        })
        .collect()
}

fn query_exact(summary: &Summary, lo_ns: i64, hi_ns: i64) -> Counts {
    let mut out = Counts::default();
    for counts in summary.counts.range(lo_ns..hi_ns).map(|(_, c)| c) {
        add_counts(&mut out, counts);
    }
    out
}

fn ceil_aligned(value: i64, width: i64) -> i64 {
    let floor = value.div_euclid(width) * width;
    if floor == value {
        value
    } else {
        floor + width
    }
}

fn query_multi_resolution(summary: &MultiResolutionSummary, lo_ns: i64, hi_ns: i64) -> Counts {
    let core_lo = ceil_aligned(lo_ns, MINUTE_NS);
    let core_hi = hi_ns.div_euclid(MINUTE_NS) * MINUTE_NS;
    if core_lo >= core_hi {
        return query_exact(&summary.exact, lo_ns, hi_ns);
    }

    let mut out = query_exact(&summary.exact, lo_ns, core_lo);
    for counts in summary
        .minute
        .counts
        .range(core_lo..core_hi)
        .map(|(_, c)| c)
    {
        add_counts(&mut out, counts);
    }
    add_counts(&mut out, &query_exact(&summary.exact, core_hi, hi_ns));
    out
}

fn query_per_file(summaries: &[Summary], lo_ns: i64, hi_ns: i64) -> Counts {
    let mut out = Counts::default();
    for summary in summaries {
        add_counts(&mut out, &query_exact(summary, lo_ns, hi_ns));
    }
    out
}

fn has_complete_hour(lo_ns: i64, hi_ns: i64) -> bool {
    ceil_aligned(lo_ns, HOUR_NS) < hi_ns.div_euclid(HOUR_NS) * HOUR_NS
}

fn median_elapsed(iterations: usize, mut f: impl FnMut()) -> StdDuration {
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        f();
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn micros(duration: StdDuration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(label_name, label_value)| {
                    key.key()
                        .labels()
                        .any(|l| l.key() == label_name && l.value() == label_value)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

#[test]
fn exact_summary_candidates_match_the_depth4_scan() {
    const ROWS_PER_FILE: usize = 20_000;

    assert!(
        !has_complete_hour(WINDOW_LO_NS, WINDOW_HI_NS),
        "the prototype must retain the frozen last-25% no-core-bucket shape"
    );
    let global = build_global_exact(ROWS_PER_FILE);
    let multi = build_global_multi_resolution(ROWS_PER_FILE);
    let per_file = build_per_file_exact(ROWS_PER_FILE);

    // Fractional bounds prove these are exact timestamp points, not rounded
    // one-second buckets. The frozen corpus happens to carry Unix seconds, but
    // the lookup remains correct for arbitrary SQL timestamp bounds.
    for (lo_ns, hi_ns) in [
        (WINDOW_LO_NS, WINDOW_HI_NS),
        (WINDOW_LO_NS + 123, WINDOW_HI_NS - 456),
        (19_237 * SECOND_NS + 91, 19_238 * SECOND_NS + 17),
    ] {
        let scanned = scan_depth4(ROWS_PER_FILE, lo_ns, hi_ns);
        assert_eq!(query_exact(&global, lo_ns, hi_ns), scanned);
        assert_eq!(query_multi_resolution(&multi, lo_ns, hi_ns), scanned);
        assert_eq!(query_per_file(&per_file, lo_ns, hi_ns), scanned);
    }
}

/// A real storage round-trip for the task's baseline: one logical boundary
/// range over four mutually overlapping Parquet files. The second execution is
/// intentional — with result caches disabled it must perform four more physical
/// scans instead of memoizing the first answer.
#[tokio::test(flavor = "current_thread")]
async fn result_cache_off_scans_all_four_depth_files_on_every_query() {
    const ROWS_PER_FILE: usize = 256;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            side_agg_write_behind: Some(false),
            result_caches: Some(false),
            ..Default::default()
        });
    let base = Utc.timestamp_opt(0, 0).unwrap();
    let mut expected = BTreeMap::<String, u64>::new();

    for file in 0..DEPTH {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|row| {
                let timestamp_ns = synthetic_timestamp_ns(row, ROWS_PER_FILE);
                let level = LEVELS[synthetic_level(file, row)];
                if (WINDOW_LO_NS..WINDOW_HI_NS).contains(&timestamp_ns) {
                    *expected.entry(level.to_string()).or_default() += 1;
                }
                let mut event = Event::now(format!("file={file} row={row} level={level}"));
                event.timestamp = base + Duration::nanoseconds(timestamp_ns);
                event.sourcetype = level.to_string();
                event
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let window = TimeBounds {
        start: Some(base + Duration::nanoseconds(WINDOW_LO_NS)),
        end: Some(base + Duration::nanoseconds(WINDOW_HI_NS)),
    };
    for _ in 0..2 {
        let got = ice
            .grouped_counts_with_summary("events", "sourcetype", None, Some(window))
            .await
            .unwrap()
            .expect("windowed group counts");
        assert_eq!(got.source_label(), "tier1_windowed_agg");
        let got: BTreeMap<String, u64> = got
            .iter()
            .map(|(value, count)| (value.expect("non-null level").to_string(), count))
            .collect();
        assert_eq!(got, expected);
    }

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_group_count_tier2_files_total",
            Some(("outcome", "boundary_scan")),
        ),
        2 * DEPTH as u64,
        "one logical range per query must scan every file in the depth-4 stack"
    );
    assert_eq!(
        counter_sum(&snapshot, "siglake_agg_result_cache_hits_total", None)
            + counter_sum(&snapshot, "siglake_agg_result_cache_misses_total", None),
        0,
        "the fixture explicitly disables whole-result caches"
    );
}

/// Run with:
/// `cargo test --release -p siglake-storage --test exact_window_summary_prototype \
///   -- --ignored --nocapture report_depth4_exact_window_summary_prototype`
#[test]
#[ignore = "measurement, not a gate"]
fn report_depth4_exact_window_summary_prototype() {
    const ROWS_PER_FILE: usize = 1_000_000;

    assert!(!has_complete_hour(WINDOW_LO_NS, WINDOW_HI_NS));
    let scanned = scan_depth4(ROWS_PER_FILE, WINDOW_LO_NS, WINDOW_HI_NS);
    let global = build_global_exact(ROWS_PER_FILE);
    let multi = build_global_multi_resolution(ROWS_PER_FILE);
    let per_file = build_per_file_exact(ROWS_PER_FILE);
    let hourly = build_global_bucketed(ROWS_PER_FILE, HOUR_NS);
    assert_eq!(query_exact(&global, WINDOW_LO_NS, WINDOW_HI_NS), scanned);
    assert_eq!(
        query_multi_resolution(&multi, WINDOW_LO_NS, WINDOW_HI_NS),
        scanned
    );
    assert_eq!(
        query_per_file(&per_file, WINDOW_LO_NS, WINDOW_HI_NS),
        scanned
    );

    let hourly_bytes = serde_json::to_vec(&hourly).unwrap().len();
    let global_bytes = serde_json::to_vec(&global).unwrap().len();
    let multi_bytes = serde_json::to_vec(&multi).unwrap().len();
    let per_file_bytes: Vec<usize> = per_file
        .iter()
        .map(|summary| serde_json::to_vec(summary).unwrap().len())
        .collect();

    let build_hourly = median_elapsed(5, || {
        black_box(build_global_bucketed(ROWS_PER_FILE, HOUR_NS));
    });
    let build_global = median_elapsed(5, || {
        black_box(build_global_exact(ROWS_PER_FILE));
    });
    let build_multi = median_elapsed(5, || {
        black_box(build_global_multi_resolution(ROWS_PER_FILE));
    });
    let build_per_file = median_elapsed(5, || {
        black_box(build_per_file_exact(ROWS_PER_FILE));
    });

    let serialize_hourly = median_elapsed(11, || {
        black_box(serde_json::to_vec(&hourly).unwrap());
    });
    let serialize_global = median_elapsed(11, || {
        black_box(serde_json::to_vec(&global).unwrap());
    });
    let serialize_multi = median_elapsed(11, || {
        black_box(serde_json::to_vec(&multi).unwrap());
    });
    let serialize_per_file = median_elapsed(11, || {
        for summary in &per_file {
            black_box(serde_json::to_vec(summary).unwrap());
        }
    });

    let query_scan = median_elapsed(31, || {
        black_box(scan_depth4(ROWS_PER_FILE, WINDOW_LO_NS, WINDOW_HI_NS));
    });
    let query_global = median_elapsed(101, || {
        black_box(query_exact(&global, WINDOW_LO_NS, WINDOW_HI_NS));
    });
    let query_multi = median_elapsed(101, || {
        black_box(query_multi_resolution(&multi, WINDOW_LO_NS, WINDOW_HI_NS));
    });
    let query_per_file = median_elapsed(101, || {
        black_box(query_per_file(&per_file, WINDOW_LO_NS, WINDOW_HI_NS));
    });

    println!(
        "depth={DEPTH} rows={} span_s={SPAN_SECONDS} window=[{}, {}) complete_hour=false",
        DEPTH * ROWS_PER_FILE,
        WINDOW_LO_NS / SECOND_NS,
        WINDOW_HI_NS / SECOND_NS,
    );
    println!("candidate,json_bytes,build_us,serialize_us,commit_cpu_us,query_us");
    println!(
        "current_global_hour,{hourly_bytes},{:.1},{:.1},{:.1},n/a",
        micros(build_hourly),
        micros(serialize_hourly),
        micros(build_hourly + serialize_hourly),
    );
    println!(
        "depth4_boundary_scan,0,n/a,n/a,n/a,{:.1}",
        micros(query_scan),
    );
    println!(
        "global_exact_points,{global_bytes},{:.1},{:.1},{:.1},{:.1}",
        micros(build_global),
        micros(serialize_global),
        micros(build_global + serialize_global),
        micros(query_global),
    );
    println!(
        "global_minute_plus_exact,{multi_bytes},{:.1},{:.1},{:.1},{:.1}",
        micros(build_multi),
        micros(serialize_multi),
        micros(build_multi + serialize_multi),
        micros(query_multi),
    );
    println!(
        "per_file_exact_points,{},{:.1},{:.1},{:.1},{:.1}",
        per_file_bytes.iter().sum::<usize>(),
        micros(build_per_file),
        micros(serialize_per_file),
        micros(build_per_file + serialize_per_file),
        micros(query_per_file),
    );
    println!(
        "per_file_footer_max_bytes={}",
        per_file_bytes.iter().copied().max().unwrap_or(0)
    );
}
