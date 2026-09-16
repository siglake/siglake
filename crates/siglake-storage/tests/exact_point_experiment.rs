#![cfg(feature = "experimental-exact-point-rollup")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, IcebergTuning, TimeBounds, EXACT_POINT_BOUNDARY_DATA_FILES_READ_TOTAL,
    EXACT_POINT_ROLLUP_BUILD_NANOSECONDS_TOTAL, EXACT_POINT_ROLLUP_QUERY_TOTAL,
};
use siglake_storage::ScanShard;

const DEPTH: usize = 4;
const ROWS_PER_FILE: usize = 128;
const LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];
type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(label_name, label_value)| {
                    key.key()
                        .labels()
                        .any(|value| value.key() == label_name && value.value() == label_value)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

fn owned_counts(counts: &siglake_storage::iceberg::GroupCounts) -> BTreeMap<String, u64> {
    counts
        .iter()
        .map(|(value, count)| (value.expect("non-null level").to_string(), count))
        .collect()
}

fn find_file(root: &Path, name: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(root).ok()? {
        let path = entry.ok()?.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, name) {
                return Some(found);
            }
        } else if path.file_name().and_then(|value| value.to_str()) == Some(name) {
            return Some(path);
        }
    }
    None
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_is_snapshot_pinned_unsharded_and_survives_rewrite() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let tmp = tempfile::tempdir().unwrap();
    let root = IcebergContext::open(tmp.path()).await.unwrap();
    let ice = root
        .for_namespace("exact_point_experiment_candidate")
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            side_agg_write_behind: Some(false),
            result_caches: Some(false),
            ..Default::default()
        });
    let base = Utc.timestamp_opt(0, 0).unwrap();
    let mut expected = BTreeMap::new();

    for file in 0..DEPTH {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|row| {
                let timestamp_ns = row as i64 * 1_000_000_000;
                let level = LEVELS[(row + file) % LEVELS.len()];
                if (32..96).contains(&row) {
                    *expected.entry(level.to_string()).or_default() += 1;
                }
                let mut event = Event::now(format!("file={file} row={row}"));
                event.timestamp = base + Duration::nanoseconds(timestamp_ns);
                event.sourcetype = level.to_string();
                event
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let observation = ice
        .experimental_exact_point_rollup_observation("events")
        .await
        .unwrap()
        .expect("candidate object");
    assert_eq!(observation.rows_covered, (DEPTH * ROWS_PER_FILE) as u64);
    assert_eq!(observation.timestamp_points, ROWS_PER_FILE);
    assert_eq!(observation.group_column, "sourcetype");
    assert!(observation.serialized_bytes > 0);
    let build_snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter(
            &build_snapshot,
            EXACT_POINT_ROLLUP_BUILD_NANOSECONDS_TOTAL,
            Some(("writer_role", "drain_append")),
        ) > 0
    );

    let window = TimeBounds {
        start: Some(base + Duration::seconds(32)),
        end: Some(base + Duration::seconds(96)),
    };
    let got = ice
        .grouped_counts_with_summary("events", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .expect("candidate counts");
    assert_eq!(got.source_label(), "experimental_exact_point");
    assert_eq!(owned_counts(&got), expected);
    let candidate_snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter(
            &candidate_snapshot,
            "siglake_group_count_tier2_files_total",
            Some(("outcome", "boundary_scan")),
        ),
        0
    );
    assert_eq!(
        counter(
            &candidate_snapshot,
            EXACT_POINT_BOUNDARY_DATA_FILES_READ_TOTAL,
            None,
        ),
        0,
        "zero boundary metrics must correspond to zero boundary data-file reads"
    );
    let candidate_executions = counter(
        &candidate_snapshot,
        EXACT_POINT_ROLLUP_QUERY_TOTAL,
        Some(("outcome", "served")),
    );
    assert_eq!(
        candidate_executions, 1,
        "positive execution observation pins the candidate route"
    );

    // A shard worker must receive a shard partial, never the global candidate.
    let mut shard_sum = BTreeMap::<String, u64>::new();
    for index in 0..2 {
        let partial = ice
            .grouped_counts_with_summary(
                "events",
                "sourcetype",
                Some(ScanShard { index, count: 2 }),
                Some(window),
            )
            .await
            .unwrap()
            .expect("shard partial");
        assert_ne!(partial.source_label(), "experimental_exact_point");
        for (level, count) in owned_counts(&partial) {
            *shard_sum.entry(level).or_default() += count;
        }
    }
    assert_eq!(shard_sum, expected);

    // A new append advances the object pin and changes the exact answer.
    let mut extra = Event::now("new snapshot");
    extra.timestamp = base + Duration::seconds(40);
    extra.sourcetype = "warn".to_string();
    ice.append_events(&[extra]).await.unwrap();
    *expected.entry("warn".to_string()).or_default() += 1;
    let advanced = ice
        .experimental_exact_point_rollup_observation("events")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(advanced.snapshot_id, observation.snapshot_id);
    assert_eq!(advanced.rows_covered, observation.rows_covered + 1);
    let got = ice
        .grouped_counts_with_summary("events", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owned_counts(&got), expected);

    // Compaction rebuilds the candidate contribution from replacement files,
    // serializes the complete parent map under the child snapshot, and leaves
    // the answer unchanged.
    let files = ice.live_data_files(ice.events_table_ident()).await.unwrap();
    ice.recluster_files(
        ice.events_table_ident(),
        files,
        &["host", "source", "sourcetype", "index"],
    )
    .await
    .unwrap();
    let rewritten = ice
        .experimental_exact_point_rollup_observation("events")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(rewritten.snapshot_id, advanced.snapshot_id);
    assert_eq!(rewritten.rows_covered, advanced.rows_covered);
    let rewrite_snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter(
            &rewrite_snapshot,
            EXACT_POINT_ROLLUP_BUILD_NANOSECONDS_TOTAL,
            Some(("writer_role", "compaction_rewrite")),
        ) > 0
    );
    let got = ice
        .grouped_counts_with_summary("events", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(owned_counts(&got), expected);

    // Unsupported columns preserve the old exact fallback and perform real
    // boundary reads; they cannot be mistaken for candidate elimination.
    let _ = snapshotter.snapshot();
    let host = ice
        .grouped_counts_with_summary("events", "host", None, Some(window))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(host.source_label(), "experimental_exact_point");
    let fallback_snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter(
            &fallback_snapshot,
            EXACT_POINT_BOUNDARY_DATA_FILES_READ_TOTAL,
            None,
        ) > 0
    );

    // A stale snapshot pin must likewise execute the physical fallback. This
    // corrupts only the disposable experimental object, never Iceberg state.
    let path = find_file(tmp.path(), "exact-point-v1.json").expect("experimental side object");
    let mut stale: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    stale["snapshot_id"] = serde_json::Value::from(-1);
    std::fs::write(&path, serde_json::to_vec(&stale).unwrap()).unwrap();
    let _ = snapshotter.snapshot();
    let stale_answer = ice
        .grouped_counts_with_summary("events", "sourcetype", None, Some(window))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(stale_answer.source_label(), "experimental_exact_point");
    let stale_snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter(
            &stale_snapshot,
            EXACT_POINT_BOUNDARY_DATA_FILES_READ_TOTAL,
            None,
        ) > 0
    );
    assert!(
        counter(
            &stale_snapshot,
            EXACT_POINT_ROLLUP_QUERY_TOTAL,
            Some(("reason", "stale_or_incomplete")),
        ) > 0
    );
}
