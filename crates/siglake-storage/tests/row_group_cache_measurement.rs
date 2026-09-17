//! Task #4847: the local measurement behind the row-group-cache disposition.
//!
//! `#[ignore]`d — it writes ~620k events and runs three cache policies over
//! four browse regimes, which is a measurement, not a gate. Run it and read the
//! table it prints:
//!
//! ```text
//! cargo test --release -p siglake-storage --test row_group_cache_measurement \
//!   -- --ignored --nocapture
//! ```
//!
//! Arms, all on one fixture, one process, one build (a within-build A/B — the
//! only comparison this box can make honestly):
//!
//! * `disabled`   — the packaged default, `0/0`: no decoded cache at all;
//! * `whole_file` — the shipped experimental policy, 8 GiB / 16,384 entries,
//!   which inserts only at end-of-stream;
//! * `row_group`  — #4847's prototype, same budget, inserting per row group.
//!
//! Regimes, crossing the two axes the disposition turns on — how deep the clip
//! reads, and whether the predicate reaches the reader:
//!
//! * `*/bound`  — the matches sit at the END of a row group, so the clip covers
//!   group 0 WHOLE. This is the case option (c) exists for.
//! * `*/inside` — they sit at its head, so the clip ends in the first batch and
//!   covers no whole group. This is where a production row-group size
//!   (>= `MIN_ROW_GROUP_ROWS` = 131,072 rows, targeting 256 MB uncompressed)
//!   puts a log-UI browse whose label is not rare.
//! * `push/*`   — `host = '<label>'`, which converts to an Iceberg predicate, so
//!   the READER prunes pages too. The cached arms strip the predicate to keep
//!   entries reusable, so they decode what pruning would have skipped.
//! * `resid/*`  — `lower(host) = '<label>'`, which does not convert, so every
//!   arm reads the same rows. The like-for-like comparison.
//!
//! Reported per (arm, regime): cold and warm browse wall time; the cache
//! counters and scan bytes for the cold run, the warm runs (per run) and the
//! closing drain; the installed cache's footprint in three currencies
//! (budget-priced, row extent, distinct retained allocations); and the peak
//! POPULATION memory aggregated across the concurrent partitions — the bound
//! #4494 recorded as "budget/4 per stream, concurrently per partition" with
//! nothing measuring it.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::QueryScanTuning;

const CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const CACHE_MAX_ENTRIES: usize = 16384;
const GROUP_ROWS: usize = 128 * 1024;
const BATCH_ROWS: usize = 8_192;
const TAIL_ROWS: usize = 3 * BATCH_ROWS;
const NEEDLES: usize = 200;
const FILES: usize = 4;
const REPEATS: usize = 5;

/// One browse shape: where its matches sit, and whether its predicate reaches
/// the reader.
///
/// Both label values appear in BOTH row groups, so row-group statistics cannot
/// drop either group for either shape — otherwise the pushdown arms would be
/// measuring pruning rather than decode reuse. What differs is WHERE in the
/// group the matches sit, and therefore how deep the clip reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Regime {
    name: &'static str,
    /// The label the browse matches: `late` sits at the end of each group,
    /// `early` at its head.
    label: &'static str,
    /// `true`: the predicate is `host = '<label>'`, which siglake converts to an
    /// Iceberg predicate — so the reader also gets page-index row selection.
    /// `false`: `lower(host) = '<label>'`, which it cannot convert, so nothing
    /// reaches the reader and every arm decodes the same rows. That is the
    /// like-for-like arm for a repeat-browse latency claim.
    pushdown: bool,
}

const REGIMES: [Regime; 4] = [
    Regime {
        name: "push/bound",
        label: "late",
        pushdown: true,
    },
    Regime {
        name: "push/inside",
        label: "early",
        pushdown: true,
    },
    Regime {
        name: "resid/bound",
        label: "late",
        pushdown: false,
    },
    Regime {
        name: "resid/inside",
        label: "early",
        pushdown: false,
    },
];

impl Regime {
    fn predicate(self) -> String {
        if self.pushdown {
            format!("host = '{}'", self.label)
        } else {
            format!("lower(host) = '{}'", self.label)
        }
    }

    fn browse(self) -> String {
        format!(
            "SELECT raw FROM events WHERE {} LIMIT 100",
            self.predicate()
        )
    }

    fn unclipped(self) -> String {
        format!("SELECT raw FROM events WHERE {}", self.predicate())
    }
}

/// `early` on the first `NEEDLES` rows of each group, `late` on its last
/// `NEEDLES` rows, `bulk` in between — so a `late` browse decodes its group to
/// the end and an `early` browse stops in its first batch, and neither group's
/// `host` statistics exclude either label.
fn fixture_events(file: usize) -> Vec<Event> {
    let rows = GROUP_ROWS + TAIL_ROWS;
    let mut events = Vec::with_capacity(rows);
    for i in 0..rows {
        let mut event = Event::now(format!(
            "file-{file} row-{i} checkout latency={} ms",
            i % 97
        ));
        let (group_start, group_rows) = if i < GROUP_ROWS {
            (0, GROUP_ROWS)
        } else {
            (GROUP_ROWS, TAIL_ROWS)
        };
        let offset = i - group_start;
        event.host = if offset < NEEDLES {
            "early".into()
        } else if offset >= group_rows - NEEDLES {
            "late".into()
        } else {
            "bulk".into()
        };
        events.push(event);
    }
    events
}

fn tuning(cache: bool, row_group_prototype: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: cache.then_some(CACHE_MAX_BYTES),
        file_cache_max_entries: cache.then_some(CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(FILES),
        batch_size: Some(BATCH_ROWS),
        file_cache_row_group_prototype: row_group_prototype,
        ..Default::default()
    }
}

async fn raws(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let column = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push(column.value(i).to_string());
        }
    }
    rows
}

fn counter(
    snapshot: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )],
    name: &str,
    outcome: &str,
) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == outcome)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(count) => *count,
            _ => 0,
        })
        .sum()
}

#[derive(Debug, Default)]
struct Counters {
    hit: u64,
    miss: u64,
    insert: u64,
    contended: u64,
    abandoned: u64,
    group_inserted: u64,
    partial_serve: u64,
    served_whole_task: u64,
    misaligned: u64,
    layout_read: u64,
    /// Sum of the `siglake_query_scan_partition_decoded_bytes` samples: what the
    /// scan EMITTED, cache-served batches included. It says how much the plan
    /// above the scan had to filter, not how much was read.
    decoded_bytes: u64,
    /// Sum of `siglake_query_scan_partition_fetched_bytes`: bytes the READER
    /// pulled. A group served from cache builds no reader, so this is where the
    /// work a cache removes shows up.
    fetched_bytes: u64,
}

impl Counters {
    fn read(snapshotter: &Snapshotter) -> Self {
        let snapshot = snapshotter.snapshot().into_vec();
        let shipped = "siglake_query_scan_file_cache_requests_total";
        let proto = "siglake_query_scan_file_cache_row_group_total";
        let histogram_sum = |name: &str| -> u64 {
            snapshot
                .iter()
                .filter(|(key, _, _, _)| key.key().name() == name)
                .map(|(_, _, _, value)| match value {
                    DebugValue::Histogram(samples) => samples
                        .iter()
                        .map(|sample| sample.into_inner() as u64)
                        .sum(),
                    _ => 0,
                })
                .sum()
        };
        let decoded_bytes = histogram_sum("siglake_query_scan_partition_decoded_bytes");
        let fetched_bytes = histogram_sum("siglake_query_scan_partition_fetched_bytes");
        Self {
            hit: counter(&snapshot, shipped, "hit"),
            miss: counter(&snapshot, shipped, "miss"),
            insert: counter(&snapshot, shipped, "insert"),
            contended: counter(&snapshot, shipped, "insert_skipped_contended"),
            abandoned: counter(&snapshot, shipped, "abandoned"),
            group_inserted: counter(&snapshot, proto, "group_inserted"),
            partial_serve: counter(&snapshot, proto, "partial_serve"),
            served_whole_task: counter(&snapshot, proto, "served_whole_task"),
            misaligned: counter(&snapshot, proto, "misaligned"),
            layout_read: counter(&snapshot, proto, "layout_read"),
            decoded_bytes,
            fetched_bytes,
        }
    }

    fn add(&mut self, other: Self) {
        self.hit += other.hit;
        self.miss += other.miss;
        self.insert += other.insert;
        self.contended += other.contended;
        self.abandoned += other.abandoned;
        self.group_inserted += other.group_inserted;
        self.partial_serve += other.partial_serve;
        self.served_whole_task += other.served_whole_task;
        self.misaligned += other.misaligned;
        self.layout_read += other.layout_read;
        self.decoded_bytes += other.decoded_bytes;
        self.fetched_bytes += other.fetched_bytes;
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: ~620k events written, three policies x two regimes"]
async fn row_group_cache_repeat_browse_measurement() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            // Rounds down to MIN_ROW_GROUP_ROWS: group 0 = 131,072 rows,
            // group 1 = the 24,576-row tail.
            target_row_group_bytes: Some(1),
            ..Default::default()
        });
    let build = std::time::Instant::now();
    for file in 0..FILES {
        ice.append_events(&fixture_events(file)).await.unwrap();
    }
    println!(
        "fixture: {FILES} files x {} rows ({} row groups each), written in {:.1}s",
        GROUP_ROWS + TAIL_ROWS,
        2,
        build.elapsed().as_secs_f64()
    );

    // Controls with no decoded cache, and the object/footer caches warmed, so
    // every arm below is compared on decode reuse rather than first-touch I/O.
    siglake_storage::configure_query_scan_tuning(tuning(false, false));
    let warm_ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
    ice.register_with_datafusion(&warm_ctx).await.unwrap();
    let mut expected_unclipped = Vec::new();
    for regime in REGIMES {
        let _ = raws(&warm_ctx, &regime.browse()).await;
        let mut rows = raws(&warm_ctx, &regime.unclipped()).await;
        rows.sort();
        // Two groups per file, `NEEDLES` matching rows in each.
        assert_eq!(rows.len(), 2 * NEEDLES * FILES, "{}", regime.name);
        expected_unclipped.push(rows);
    }

    println!(
        "\n{:<13} {:<11} {:>8} {:>8} {:>8}   then per phase: counters, scan bytes; \
         then the cache footprint and the population peak, in MiB",
        "regime", "arm", "cold_ms", "warm_ms", "warm_p50"
    );
    for (regime_idx, regime) in REGIMES.into_iter().enumerate() {
        for (arm, cache, prototype) in [
            ("disabled", false, false),
            ("whole_file", true, false),
            ("row_group", true, true),
        ] {
            siglake_storage::clear_decoded_file_cache();
            siglake_storage::clear_row_group_layout_cache();
            siglake_storage::configure_query_scan_tuning(tuning(cache, prototype));
            siglake_storage::reset_decoded_file_cache_population_peaks();
            let ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
            ice.register_with_datafusion(&ctx).await.unwrap();
            let _ = Counters::read(&snapshotter);

            let mut cold = Counters::default();
            let mut warm_counters = Counters::default();
            let mut timings = Vec::new();
            for run in 0..REPEATS {
                let started = std::time::Instant::now();
                let rows = raws(&ctx, &regime.browse()).await;
                timings.push(started.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(rows.len(), 100, "{arm}/{} run {run}", regime.name);
                for row in &rows {
                    assert!(
                        expected_unclipped[regime_idx].binary_search(row).is_ok(),
                        "{arm}/{} run {run}: returned a row the control's \
                         unclipped answer does not contain: {row}",
                        regime.name
                    );
                }
                // Let the populate streams' Drop-charged counters land.
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                let delta = Counters::read(&snapshotter);
                if run == 0 {
                    cold = delta;
                } else {
                    warm_counters.add(delta);
                }
            }
            // Read the cache and the population peaks HERE: what the browses
            // alone left behind, before the drain below adds its own entries.
            let footprint = siglake_storage::decoded_file_cache_footprint();
            let population = siglake_storage::decoded_file_cache_population_stats();

            // Exactness, not just the clipped prefix: the unclipped answer over
            // a partially populated file must equal the control's, row for row.
            let mut unclipped = raws(&ctx, &regime.unclipped()).await;
            unclipped.sort();
            assert_eq!(
                unclipped, expected_unclipped[regime_idx],
                "{arm}/{}: unclipped answer diverged from the cache-disabled control",
                regime.name
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let drain = Counters::read(&snapshotter);

            let mut warm = timings[1..].to_vec();
            warm.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let summary = |c: &Counters, runs: u64| {
                format!(
                    "hit={} miss={} ins={} cont={} aband={} rg_ins={} part={} whole={} mis={} foot={} out={:.1}MiB read={:.1}MiB",
                    c.hit / runs,
                    c.miss / runs,
                    c.insert,
                    c.contended,
                    c.abandoned,
                    c.group_inserted,
                    c.partial_serve,
                    c.served_whole_task,
                    c.misaligned,
                    c.layout_read,
                    mib(c.decoded_bytes) / runs as f64,
                    mib(c.fetched_bytes) / runs as f64,
                )
            };
            println!(
                "{:<13} {:<11} {:>8.1} {:>8.1} {:>8.1}\n    cold  {}\n    warm  {}\n    drain {}\n    cache entries={} priced={:.1} extent={:.1} retained={:.1} | population peak extent={:.1} retained={:.1} streams={}",
                regime.name,
                arm,
                timings[0],
                warm.iter().sum::<f64>() / warm.len() as f64,
                warm[warm.len() / 2],
                summary(&cold, 1),
                summary(&warm_counters, (REPEATS - 1) as u64),
                summary(&drain, 1),
                footprint.entries,
                mib(footprint.priced_bytes),
                mib(footprint.extent_bytes),
                mib(footprint.retained_bytes),
                mib(population.peak_extent_bytes),
                mib(population.peak_retained_bytes),
                population.peak_streams,
            );
        }
    }

    // ---------------------------------------------------------------------
    // Population memory against the number of groups in a file.
    //
    // The regimes above cannot separate the two policies' per-stream bounds:
    // their group 0 is 84% of the file, so "buffer the file" and "buffer a
    // group" are nearly the same quantity. One file of four floor-sized groups,
    // drained, separates them — and a drain is the shape the shipped policy was
    // built for, so both arms populate.
    // ---------------------------------------------------------------------
    let wide_tmp = tempfile::tempdir().unwrap();
    let wide = IcebergContext::open(wide_tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        });
    let groups = 4;
    let wide_rows = groups * GROUP_ROWS;
    let mut wide_events = Vec::with_capacity(wide_rows);
    for i in 0..wide_rows {
        let mut event = Event::now(format!("wide row-{i} checkout latency={} ms", i % 97));
        event.host = "bulk".into();
        wide_events.push(event);
    }
    wide.append_events(&wide_events).await.unwrap();
    drop(wide_events);
    println!("\none file, {groups} x {GROUP_ROWS} rows, drained once per arm:");
    for (arm, prototype) in [("whole_file", false), ("row_group", true)] {
        siglake_storage::clear_decoded_file_cache();
        siglake_storage::clear_row_group_layout_cache();
        siglake_storage::configure_query_scan_tuning(QueryScanTuning {
            file_cache_max_bytes: Some(CACHE_MAX_BYTES),
            file_cache_max_entries: Some(CACHE_MAX_ENTRIES),
            file_concurrency_limit: Some(1),
            batch_size: Some(BATCH_ROWS),
            file_cache_row_group_prototype: prototype,
            ..Default::default()
        });
        siglake_storage::reset_decoded_file_cache_population_peaks();
        let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
        wide.register_with_datafusion(&ctx).await.unwrap();
        let rows = raws(&ctx, "SELECT raw FROM events").await;
        assert_eq!(rows.len(), wide_rows);
        let footprint = siglake_storage::decoded_file_cache_footprint();
        let population = siglake_storage::decoded_file_cache_population_stats();
        println!(
            "  {arm:<11} cache entries={} priced={:.1} extent={:.1} retained={:.1} | \
             population peak extent={:.1} retained={:.1} streams={}",
            footprint.entries,
            mib(footprint.priced_bytes),
            mib(footprint.extent_bytes),
            mib(footprint.retained_bytes),
            mib(population.peak_extent_bytes),
            mib(population.peak_retained_bytes),
            population.peak_streams,
        );
    }

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::clear_row_group_layout_cache();
}
