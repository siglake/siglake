//! Phase 4 (aggregation fast path) — windowed-aggregation RESULT cache tests.
//!
//! Phase 1 makes a windowed group-by/histogram merge per-file footer partials +
//! scan the ≤2 boundary files on every query; Phase 4 caches the whole windowed
//! result keyed by `(table, snapshot, agg-spec, window, shard)` so a repeat is
//! served without touching footers or files. These assert the result cache is
//! TRANSPARENT (a warm repeat equals the cold run) and EFFECTIVE (the warm repeat
//! is served from the result cache — verified via `siglake_agg_result_cache_*`),
//! covering both the windowed group-by and the windowed date-histogram.
//!
//! This test owns the process-global `DebuggingRecorder` install, so it lives
//! alone in this test binary; the data-correctness/invalidation tests that don't
//! touch metrics live in `phase4_agg_result_cache_invalidation.rs`.

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
/// one row per minute, `host` cycling — same shape as the Phase 1/2 windowed tests.
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

/// Warm repeats equal cold results AND the second run is served from the windowed
/// RESULT cache (its hit counter rises, miss counter does not) — for both the
/// windowed group-by and the windowed date-histogram.
#[tokio::test]
async fn warm_windowed_repeat_equals_cold_and_hits_result_cache() {
    // Fix 2b serves a windowed group-by from the per-snapshot 2D time×group side
    // aggregate, resolving each boundary sub-range via a SEPARATE windowed call —
    // so a straddling window becomes two result-cache misses, not the single
    // whole-window miss this test asserts. Disable it so the test exercises the
    // result-cache layer it was written for; Fix 2b has its own coverage.

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_tmp, ice, base) = multi_file_table().await;
    let ice = ice.with_tuning(siglake_storage::iceberg::IcebergTuning {
        windowed_group_agg: Some(false),
        ..Default::default()
    });

    // A window that fully contains files 1 & 2 and straddles 0 & 3 (boundary scans
    // on the cold run; all of it should be served from the result cache when warm).
    let win = window(base, 1800, 12600);

    // NOTE: `DebuggingRecorder::snapshot()` is DESTRUCTIVE — each snapshot returns
    // the delta since the previous one.

    // --- Windowed group-by ---
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
        "siglake_agg_result_cache_misses_total",
        Some(("kind", "group_counts")),
    );
    let cold_hits = counter_sum(
        &snap,
        "siglake_agg_result_cache_hits_total",
        Some(("kind", "group_counts")),
    );
    assert_eq!(
        cold_misses, 1,
        "cold windowed group-by is one result-cache miss"
    );
    assert_eq!(cold_hits, 0, "cold run has nothing cached yet");

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
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_agg_result_cache_hits_total",
            Some(("kind", "group_counts"))
        ),
        1,
        "warm windowed group-by must be served from the result cache (one hit)"
    );
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_agg_result_cache_misses_total",
            Some(("kind", "group_counts"))
        ),
        0,
        "warm windowed group-by must not miss the result cache"
    );
    // A result-cache hit must not touch the per-file footer cache at all.
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_footer_cache_hits_total",
            Some(("kind", "group_counts"))
        ) + counter_sum(
            &snap,
            "siglake_footer_cache_misses_total",
            Some(("kind", "group_counts"))
        ),
        0,
        "a result-cache hit must skip the footer reads entirely"
    );

    // --- Windowed date-histogram over the same window with 30-min buckets ---
    let interval_30m = 1_800_000_000_000;
    let hist_cold = ice
        .date_histogram_counts("events", interval_30m, 0, None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_agg_result_cache_misses_total",
            Some(("kind", "date_histogram"))
        ),
        1,
        "cold windowed histogram is one result-cache miss"
    );

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
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_agg_result_cache_hits_total",
            Some(("kind", "date_histogram"))
        ),
        1,
        "warm windowed histogram must be served from the result cache (one hit)"
    );
    assert_eq!(
        counter_sum(
            &snap,
            "siglake_agg_result_cache_misses_total",
            Some(("kind", "date_histogram"))
        ),
        0,
        "warm windowed histogram must not miss the result cache"
    );

    // --- The UNWINDOWED path must NOT populate the result cache (Phase 2 owns it). ---
    let _ = ice
        .grouped_counts_with_summary("events", "host", None, None)
        .await
        .unwrap()
        .unwrap();
    let snap = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(&snap, "siglake_agg_result_cache_misses_total", None)
            + counter_sum(&snap, "siglake_agg_result_cache_hits_total", None),
        0,
        "the unwindowed group-by must not touch the windowed result cache"
    );
}

/// Microbench (ignored): a warm repeat of a windowed group-by served from the
/// result cache should be far faster than the cold run (no footer reads, no
/// boundary scans). Run with `cargo test -p siglake-storage --test
/// phase4_agg_result_cache -- --ignored --nocapture bench_warm_result_cache`.
#[tokio::test]
#[ignore]
async fn bench_warm_result_cache() {
    use std::time::Instant;

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
    let win = window(base, 1800, files * 3600 - 1800);

    let t0 = Instant::now();
    let cold = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let cold_dur = t0.elapsed();

    let t1 = Instant::now();
    let warm = ice
        .grouped_counts_with_summary("events", "host", None, Some(win))
        .await
        .unwrap()
        .unwrap();
    let warm_dur = t1.elapsed();

    assert_eq!(sorted(&cold.to_rows()), sorted(&warm.to_rows()));
    println!("cold windowed group-by: {cold_dur:?}; warm (result cache): {warm_dur:?}");
    assert!(
        warm_dur * 4 < cold_dur,
        "warm result-cache repeat ({warm_dur:?}) should be much faster than cold ({cold_dur:?})"
    );
}
