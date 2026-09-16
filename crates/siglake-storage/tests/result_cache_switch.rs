//! `SIGLAKE_QUERY_RESULT_CACHE=off` must actually stop result-level memoization.
//!
//! Why this exists: the 2026-07-27 1TB board was unusable as a performance
//! comparison because every shape collapsed onto a ~13ms result-cache-hit floor
//! — repeated queries were replaying a memoized answer, not executing. The
//! switch is what lets a benchmark measure the engine, so it needs a test that
//! fails if it silently stops working; a knob that quietly does nothing is
//! worse than no knob, because the resulting numbers still look plausible.
//!
//! The observable signal is the snapshot-aggregate cache's own counters: with
//! caches ON a repeated whole-table `GROUP BY` is a `snapshot_agg` HIT, with
//! them OFF it never is. The recorder is process-global and the switch is read
//! once per process via `OnceLock`, so this test owns its binary.

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;

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

async fn table() -> (tempfile::TempDir, IcebergContext) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let hosts = ["h0", "h1", "h2"];
    for k in 0..3i64 {
        let evs: Vec<Event> = (0..60)
            .map(|m| {
                let secs = k * 3600 + m * 60;
                let mut e = Event::now(format!("row at {secs}s"));
                e.timestamp = base + Duration::seconds(secs);
                e.host = hosts[(secs as usize / 60) % hosts.len()].to_string();
                e
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }
    (tmp, ice)
}

#[tokio::test(flavor = "current_thread")]
async fn result_cache_off_stops_memoizing_and_keeps_answers_exact() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_tmp, ice) = table().await;
    let ice = ice.with_tuning(siglake_storage::iceberg::IcebergTuning {
        result_caches: Some(false),
        ..Default::default()
    });

    let mut answers = Vec::new();
    for _ in 0..3 {
        let mut rows = (ice
            .grouped_counts_with_summary("events", "host", None, None)
            .await
            .unwrap()
            .expect("group counts"))
        .to_rows()
        .clone();
        rows.sort();
        answers.push(rows);
    }

    // Correctness is invariant: bypassing a cache must not change the answer.
    assert_eq!(answers[0], answers[1]);
    assert_eq!(answers[1], answers[2]);
    assert_eq!(
        answers[0].iter().map(|(_, n)| *n).sum::<u64>(),
        180,
        "three 60-row appends"
    );

    // …and none of those repeats was served from the snapshot-aggregate memo.
    let snap = snapshotter.snapshot().into_vec();
    let hits = counter_sum(
        &snap,
        "siglake_footer_cache_hits_total",
        Some(("kind", "snapshot_agg")),
    );
    assert_eq!(
        hits, 0,
        "with result caches off, no repeat may be served from the snapshot-aggregate cache"
    );
}
