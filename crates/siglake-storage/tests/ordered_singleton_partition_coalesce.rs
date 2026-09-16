//! #4353: an ordered browse whose file count is at or below
//! `target_partitions` arrives with EVERY partition holding one file. Before
//! the fix the bounds/re-split path was entered only when some partition held
//! more than one file, so the small-`LIMIT` coalesce never ran: the plan kept
//! one independent scan stream per file and the SortPreservingMerge above
//! drained each of them (run 83's `windowed_browse_last25` — 5 planned files,
//! 5 partitions, `ordered_merge_overlap_partitions=0`, 371,613 rows scanned
//! for 100 returned). These tests pin `target_partitions` above the file count
//! so the singleton shape is reproduced regardless of the host's core count.
use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{OrderedScanLimit, PreferredScanOrder};

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

const FILES: i64 = 5;
const ROWS_PER_FILE: i64 = 2_000;

/// Five appends interleaved row by row, so every file spans the whole time
/// range: the level-0 overlap a live tail carries.
async fn overlapping_files(target_partitions: usize) -> (tempfile::TempDir, IcebergContext) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 30, 0).unwrap();
    for j in 0..FILES {
        let mut events = Vec::with_capacity(ROWS_PER_FILE as usize);
        for i in 0..ROWS_PER_FILE {
            let k = i * FILES + j;
            let mut e = Event::now(format!("row {k}"));
            e.timestamp = base + Duration::milliseconds(k * 100);
            events.push(e);
        }
        ice.append_events(&events).await.unwrap();
    }
    let files = ice
        .live_data_files(ice.events_table_ident())
        .await
        .unwrap()
        .len();
    assert_eq!(
        files, FILES as usize,
        "fixture must plan one file per append, under target_partitions={target_partitions}"
    );
    (tmp, ice)
}

/// A browse context: descending preference plus the small-`LIMIT` marker the
/// query server sets from `ORDER BY ... LIMIT n` (`sql.rs`), with
/// `target_partitions` pinned above the file count.
fn browse_context(target_partitions: usize, limit: usize) -> SessionContext {
    let base = siglake_storage::session_context_with_order(
        Some(target_partitions),
        None,
        Some(PreferredScanOrder { descending: true }),
    );
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(OrderedScanLimit { limit }));
    SessionContext::new_with_state(state)
}

fn newest_timestamps(n: i64) -> Vec<i64> {
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 30, 0).unwrap();
    let last = FILES * ROWS_PER_FILE - 1;
    ((last - n + 1)..=last)
        .rev()
        .map(|k| {
            (base + Duration::milliseconds(k * 100))
                .timestamp_nanos_opt()
                .unwrap()
        })
        .collect()
}

fn collect_ts(batches: Vec<arrow_array::RecordBatch>) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let a = siglake_core::column_nanos(b.column(0)).unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn small_limit_browse_coalesces_singleton_partitions_into_one_merge() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let (_tmp, ice) = overlapping_files(8).await;
    let ctx = browse_context(8, 100);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 100")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    // Pre-fix this reads `partitions:[5]`: one singleton scan partition per
    // file, each polled by the SortPreservingMerge above.
    assert!(
        plan_str.contains("SiglakeIcebergTableScan partitions:[1]"),
        "a small ordered LIMIT over 5 overlapping files must plan ONE scan partition:\n{plan_str}"
    );
    // One advertised partition satisfies `ORDER BY timestamp DESC` outright:
    // no blocking sort, and no SortPreservingMerge polling sibling streams.
    assert!(
        !plan_str.contains("SortExec"),
        "the coalesced browse must not fall back to a blocking sort:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortPreservingMergeExec"),
        "one advertised partition needs no cross-partition merge:\n{plan_str}"
    );

    assert_eq!(
        collect_ts(df.collect().await.unwrap()),
        newest_timestamps(100)
    );

    let snapshot = snapshotter.snapshot().into_vec();
    // The single partition is an OVERLAP partition: its files are k-way
    // merged, not concatenated. Pre-fix this counter stayed at 0 (the tuning
    // record's `ordered_merge_overlap_partitions=0`).
    assert!(
        counter_sum(
            &snapshot,
            "siglake_query_scan_ordered_merge_partitions_total"
        ) > 0,
        "the coalesced partition must plan an overlap merge"
    );
}

/// The coalesce is for SMALL limits only: a browse past
/// `SIGLAKE_ORDERED_SINGLE_PARTITION_MAX_LIMIT`'s bucket (here, no
/// `OrderedScanLimit` at all — the shape `sql.rs` leaves alone) keeps its
/// per-file scan parallelism.
#[tokio::test]
async fn ordered_scan_without_a_small_limit_keeps_its_singleton_partitions() {
    let (_tmp, ice) = overlapping_files(8).await;
    let ctx = siglake_storage::session_context_with_order(
        Some(8),
        None,
        Some(PreferredScanOrder { descending: true }),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    assert!(
        plan_str.contains(&format!(
            "SiglakeIcebergTableScan partitions:[{}]",
            FILES as usize
        )),
        "an ordered scan with no small LIMIT keeps one partition per file:\n{plan_str}"
    );
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "the per-file partitions are merged above the scan:\n{plan_str}"
    );
    let ts = collect_ts(df.collect().await.unwrap());
    assert_eq!(ts.len(), (FILES * ROWS_PER_FILE) as usize);
    assert_eq!(&ts[..100], &newest_timestamps(100)[..]);
}
