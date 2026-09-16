//! Tier-2 plans a snapshot's live files once, then reuses the cached scan tasks.
//! The cached tasks retain delete-file associations while avoiding a manifest
//! walk on every grouped-count call.

use chrono::Utc;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::ScanShard;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

fn plan_file_samples(snapshot: &SnapshotVec) -> usize {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_file_list_cache_phase_seconds"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "phase" && label.value() == "plan_files")
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => samples.len(),
            _ => 0,
        })
        .sum()
}

fn event(file: usize) -> Event {
    Event {
        timestamp: Utc::now(),
        host: format!("host-{}", file % 3),
        source: format!("source-{}", file % 2),
        sourcetype: "cache:test".into(),
        index: "main".into(),
        raw: format!("row {file}"),
        attributes: None,
    }
}

#[tokio::test]
async fn repeated_tier2_group_by_reuses_the_snapshot_file_plan() {
    const FILES: usize = 170;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    for file in 0..FILES {
        ice.append_events(&[event(file)]).await.unwrap();
    }
    assert_eq!(
        ice.live_data_files(ice.events_table_ident())
            .await
            .unwrap()
            .len(),
        FILES
    );
    snapshotter.snapshot(); // discard setup metrics

    // Supplying a shard forces Tier-2 (the table-wide Tier-1 aggregate cannot
    // answer a shard). count=1 deliberately owns every file so the expected
    // result remains the full-table GROUP BY.
    let shard = Some(ScanShard { index: 0, count: 1 });
    let mut first = ice
        .grouped_counts_with_summary("events", "source", shard, None)
        .await
        .unwrap()
        .expect("first Tier-2 GROUP BY")
        .to_rows();
    first.sort();
    assert_eq!(
        first.iter().map(|(_, count)| count).sum::<u64>(),
        FILES as u64
    );
    let cold = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter(&cold, "siglake_file_list_cache_misses_total"),
        1,
        "the first Tier-2 call should plan this snapshot once"
    );
    assert_eq!(plan_file_samples(&cold), 1);

    let mut second = ice
        .grouped_counts_with_summary("events", "source", shard, None)
        .await
        .unwrap()
        .expect("repeated Tier-2 GROUP BY")
        .to_rows();
    second.sort();
    assert_eq!(second, first);
    let warm = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&warm, "siglake_file_list_cache_hits_total"), 1);
    assert_eq!(counter(&warm, "siglake_file_list_cache_misses_total"), 0);
    assert_eq!(
        plan_file_samples(&warm),
        0,
        "a warm Tier-2 call must not re-read the snapshot's manifests"
    );

    // The raw-page-only entry point shares the same cached task vector.
    let raw = ice
        .grouped_counts_raw_pages("events", "source", shard)
        .await
        .unwrap()
        .expect("raw-page grouped counts");
    assert_eq!(
        raw.iter().map(|(_, count)| count).sum::<u64>(),
        FILES as u64
    );
    let raw_warm = snapshotter.snapshot().into_vec();
    assert_eq!(counter(&raw_warm, "siglake_file_list_cache_hits_total"), 1);
    assert_eq!(plan_file_samples(&raw_warm), 0);

    // A commit invalidates both cached representations. The next Tier-2 call
    // must plan the new snapshot and include its new file.
    ice.append_events(&[event(FILES)]).await.unwrap();
    snapshotter.snapshot(); // discard append metrics
    let after_commit = ice
        .grouped_counts_with_summary("events", "source", shard, None)
        .await
        .unwrap()
        .expect("post-commit Tier-2 GROUP BY")
        .to_rows();
    assert_eq!(
        after_commit.iter().map(|(_, count)| count).sum::<u64>(),
        FILES as u64 + 1
    );
    let refreshed = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter(&refreshed, "siglake_file_list_cache_misses_total"),
        1
    );
    assert_eq!(plan_file_samples(&refreshed), 1);
}
