//! Phase 2 (aggregation fast path) — footer + snapshot-aggregate caching tests.
//!
//! These assert the Phase 2 caches are TRANSPARENT (a warm repeat returns the
//! exact same result as the cold run) and EFFECTIVE (the warm repeat is served
//! from the per-file footer cache — verified via the `siglake_footer_cache_*`
//! counters), and that the snapshot-keyed unwindowed-aggregate cache is correctly
//! INVALIDATED on commit (a post-append aggregate reflects the new data, never a
//! stale cached value).
//!
//! The metrics-asserting test owns the process-global `DebuggingRecorder` install,
//! so it lives alone in this test binary; the data-correctness tests that don't
//! touch metrics live in `phase2_agg_cache_invalidation.rs`.

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, TimeBounds};

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key().labels().any(|l| l.key() == lk && l.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

/// Build a multi-file events table: four time-disjoint 1-hour files (hours 0..3),
/// one row per minute, `host` cycling — same shape as the Phase 1 windowed tests.
async fn multi_file_table() -> (tempfile::TempDir, IcebergContext, chrono::DateTime<Utc>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["h0", "h1", "h2"];
    for k in 0..4i64 {
        let start = k * 3600;
        let evs: Vec<Event> = (0..60)
            .map(|m| {
                let secs = start + m * 60;
                let mut e = Event::now(format!("row at {secs}s"));
                e.timestamp = base + Duration::seconds(secs);
                e.host = hosts[(secs as usize / 60) % hosts.len()].to_string();
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    (tmp, ice, base)
}

fn window(base: chrono::DateTime<Utc>, lo_secs: i64, hi_secs: i64) -> TimeBounds {
    TimeBounds {
        start: Some(base + Duration::seconds(lo_secs)),
        end: Some(base + Duration::seconds(hi_secs)),
    }
}

fn sorted(rows: &[(Option<String>, u64)]) -> Vec<(Option<String>, u64)> {
    let mut v = rows.to_vec();
    v.sort();
    v
}

/// Warm repeats equal cold results AND the second run is served from the per-file
/// footer cache (hit counter rises, miss counter does not) — for both the
/// windowed group-by and the windowed date-histogram. Covers the primary
/// (per-file footer) cache end to end.
#[tokio::test]
async fn warm_repeat_equals_cold_and_hits_footer_cache() {
    // This test validates the Phase 2 per-file FOOTER cache: it asserts a warm
    // windowed repeat re-reads its footers from cache. Phase 4 added a windowed
    // RESULT cache that sits IN FRONT of the footer layer and would otherwise
    // serve the warm repeat outright (zero footer reads). Disable it here so this
    // test keeps exercising the footer layer it was written for; Phase 4's own
    // suite (`phase4_agg_result_cache*`) covers the result cache. Set before any
    // `IcebergContext` starts serving queries.
    // Fix 2b added a windowed group-by served from the per-snapshot 2D time×group
    // side aggregate, which sits IN FRONT of the per-file footer layer and would
    // serve this window outright (zero footer reads). Disable it so this test keeps
    // exercising the footer cache it was written for; Fix 2b has its own coverage.

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_tmp, ice, base) = multi_file_table().await;
    let ice = ice.with_tuning(siglake_storage::iceberg::IcebergTuning {
        agg_result_cache_capacity: Some(0),
        windowed_group_agg: Some(false),
        ..Default::default()
    });

    // --- Windowed group-by over a window that fully contains files 1 & 2 (so the
    // partials come from the per-file group-count footer, not a boundary scan). ---
    let win = window(base, 3600, 10800);

    // NOTE: `DebuggingRecorder::snapshot()` is DESTRUCTIVE — it swaps each counter
    // to 0 and returns the delta since the previous snapshot. So each snapshot
    // below measures exactly the counter activity of the call(s) since the last.
    let cold = sorted(
        &ice.grouped_counts_with_summary("events", "host", None, Some(win))
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );

    let snap = snapshotter.snapshot().into_vec();
    let cold_misses = counter_sum(
        &snap,
        "siglake_footer_cache_misses_total",
        Some(("kind", "group_counts")),
    );
    let cold_hits = counter_sum(
        &snap,
        "siglake_footer_cache_hits_total",
        Some(("kind", "group_counts")),
    );
    assert!(
        cold_misses >= 2,
        "cold run must read the contained-file footers (misses), got {cold_misses}"
    );
    assert_eq!(
        cold_hits, 0,
        "cold run has nothing cached yet, so zero footer hits"
    );

    let warm = sorted(
        &ice.grouped_counts_with_summary("events", "host", None, Some(win))
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    assert_eq!(
        cold, warm,
        "warm windowed group-by must equal the cold result"
    );

    let snap = snapshotter.snapshot().into_vec();
    let warm_hits = counter_sum(
        &snap,
        "siglake_footer_cache_hits_total",
        Some(("kind", "group_counts")),
    );
    let warm_misses = counter_sum(
        &snap,
        "siglake_footer_cache_misses_total",
        Some(("kind", "group_counts")),
    );
    assert!(
        warm_hits >= cold_misses,
        "warm run must serve every previously-read footer from cache (hits {warm_hits} >= cold misses {cold_misses})"
    );
    assert_eq!(
        warm_misses, 0,
        "warm run must not re-read any contained-file footer (got {warm_misses} misses)"
    );

    // --- Windowed date-histogram over the same window with 30-MINUTE buckets so
    // each fully-contained 1-hour file spans two output buckets → it can't be
    // manifest-counted as a single bucket and instead re-buckets from its
    // TIME_BUCKETS footer, exercising the time-bucket footer cache. ---
    let interval_30m = 1_800_000_000_000;
    let hist_cold = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();
    // Drain the cold-run counters (deltas) so the warm snapshot is isolated.
    let _ = snapshotter.snapshot();

    let hist_warm = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        hist_cold, hist_warm,
        "warm windowed histogram must equal the cold result"
    );

    let snap = snapshotter.snapshot().into_vec();
    let tb_hits_warm = counter_sum(
        &snap,
        "siglake_footer_cache_hits_total",
        Some(("kind", "time_buckets")),
    );
    let tb_misses_warm = counter_sum(
        &snap,
        "siglake_footer_cache_misses_total",
        Some(("kind", "time_buckets")),
    );
    assert!(
        tb_hits_warm > 0,
        "warm windowed histogram must hit the time-bucket footer cache"
    );
    assert_eq!(
        tb_misses_warm, 0,
        "warm windowed histogram must not re-read any time-bucket footer (got {tb_misses_warm})"
    );

    // --- Unwindowed group-by: snapshot-aggregate cache. First run populates it,
    // second is a snapshot_agg HIT (skips Tier-1/Tier-2 entirely). ---
    let un_cold = sorted(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    let un_warm = sorted(
        &ice.grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .unwrap()
            .to_rows(),
    );
    assert_eq!(un_cold, un_warm, "warm unwindowed group-by must equal cold");
    let snap = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snap,
            "siglake_footer_cache_hits_total",
            Some(("kind", "snapshot_agg"))
        ) > 0,
        "warm unwindowed group-by must hit the snapshot-aggregate cache"
    );
}

/// Microbench (ignored): a warm repeat of a windowed group-by should be much
/// faster than the cold run, because the per-file footer reads are served from
/// cache. Run with `cargo test -p siglake-storage --test phase2_agg_cache --
/// --ignored --nocapture bench_warm_windowed_groupby`.
#[tokio::test]
#[ignore]
async fn bench_warm_windowed_groupby() {
    use std::time::Instant;

    // A wider table (more contained files → more footer reads to amortize).
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["h0", "h1", "h2", "h3", "h4"];
    let files = 64i64;
    for k in 0..files {
        let start = k * 3600;
        let evs: Vec<Event> = (0..60)
            .map(|m| {
                let secs = start + m * 60;
                let mut e = Event::now(format!("row {secs}"));
                e.timestamp = base + Duration::seconds(secs);
                e.host = hosts[(secs as usize / 60) % hosts.len()].to_string();
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    // Window covering all-but-the-edge files → mostly fully-contained footer reads.
    let win = window(base, 3600, (files - 1) * 3600);

    let t0 = Instant::now();
    let cold = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let cold_ms = t0.elapsed().as_secs_f64() * 1e3;

    let reps = 20;
    let t1 = Instant::now();
    for _ in 0..reps {
        let _ = ice
            .grouped_counts_with_summary("events", "host", None, Some(win))
            .await
            .unwrap()
            .unwrap();
    }
    let warm_ms = t1.elapsed().as_secs_f64() * 1e3 / reps as f64;

    eprintln!(
        "windowed group-by over {files} files: cold {cold_ms:.2} ms, warm (cached footers) {warm_ms:.2} ms ({} groups)",
        cold.len()
    );
    assert!(
        warm_ms < cold_ms,
        "warm windowed group-by ({warm_ms:.2} ms) should beat cold ({cold_ms:.2} ms)"
    );
}

/// The warm cycle's group-count prime: `warm_group_counts` reads the newest
/// file's footer as the column census and populates the snapshot-keyed merged
/// cache for every footer-covered column — so the first user query after a
/// commit is a cache hit instead of paying the once-per-snapshot merge (the
/// http_logs cold top_hosts measured ~6.8s on a 1.1M-key column).
#[tokio::test]
async fn warm_group_counts_primes_the_merged_cache() {
    let (_tmp, ice, _base) = multi_file_table().await;

    let warmed = ice.warm_group_counts("events").await.unwrap();
    assert!(warmed > 0, "events footers carry aggregated columns");

    // The warmed answer must be exact AND identical to a direct probe.
    let direct = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("host is footer-aggregated");
    let mut total = 0u64;
    direct.for_each(|_, count| total = total.saturating_add(count));
    let expected: u64 = 4 * 60; // multi_file_table writes 4 files x 60 rows
    assert_eq!(total, expected, "warmed merged counts must be exact");

    // A new commit moves the snapshot; re-warming picks up the new key and a
    // fresh probe reflects the appended rows.
    let evs: Vec<siglake_core::Event> = (0..10)
        .map(|i| siglake_core::Event::now(format!("warm extra {i}")))
        .collect();
    ice.append_events(&evs).await.unwrap();
    ice.warm_group_counts("events").await.unwrap();
    let after = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .expect("host still footer-aggregated");
    let mut total_after = 0u64;
    after.for_each(|_, count| total_after = total_after.saturating_add(count));
    assert_eq!(total_after, expected + 10, "post-commit warm must be fresh");
}
