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
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{OrderedMergeGlobalBudget, OrderedScanLimit, PreferredScanOrder};

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

const MERGE_PARTITIONS: &str = "siglake_query_scan_ordered_merge_partitions_total";
const ORDERED_PLAN_CACHE: &str = "siglake_query_ordered_plan_cache_total";
const OUTPUT_ORDERING: &str = "siglake_query_scan_output_ordering_total";

/// One recorder per process — `install` refuses a second — and one planned
/// query at a time under it: the coalesce test reads exact cache and ordering
/// counter deltas, while the restored-singleton test asserts the merge counter
/// stayed at ZERO. `Snapshotter::snapshot` drains the registry, so each gate
/// holder's snapshot covers only its own planning.
static METRICS: std::sync::LazyLock<(tokio::sync::Mutex<()>, Snapshotter)> =
    std::sync::LazyLock::new(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        (tokio::sync::Mutex::new(()), snapshotter)
    });

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
        Some(PreferredScanOrder::timestamp(true)),
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
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    snapshotter.snapshot();

    let (_tmp, ice) = overlapping_files(8).await;
    let ctx = browse_context(8, 100);
    ice.register_with_datafusion(&ctx).await.unwrap();
    snapshotter.snapshot();

    let first = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 100")
        .await
        .unwrap();
    let first_plan = first.create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(first_plan.as_ref()).indent(true));
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

    let first_metrics = snapshotter.snapshot().into_vec();
    // The single partition is an OVERLAP partition: its files are k-way
    // merged, not concatenated. Pre-fix this counter stayed at 0 (the tuning
    // record's `ordered_merge_overlap_partitions=0`).
    assert_eq!(
        counter_sum(&first_metrics, MERGE_PARTITIONS, None),
        1,
        "the fresh coalesced plan must charge its one overlap partition"
    );
    assert_eq!(
        counter_sum(
            &first_metrics,
            ORDERED_PLAN_CACHE,
            Some(("outcome", "miss"))
        ),
        1,
        "the first browse must populate the ordered-plan cache"
    );
    assert_eq!(
        counter_sum(&first_metrics, ORDERED_PLAN_CACHE, Some(("outcome", "hit"))),
        0,
        "the first browse must not be served from the ordered-plan cache"
    );
    assert_eq!(
        counter_sum(
            &first_metrics,
            OUTPUT_ORDERING,
            Some(("outcome", "advertised"))
        ),
        1,
        "the fresh plan must advertise one ordered scan"
    );
    assert_eq!(
        collect_ts(collect(first_plan, ctx.task_ctx()).await.unwrap()),
        newest_timestamps(100)
    );

    let second = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 100")
        .await
        .unwrap();
    let second_plan = second.create_physical_plan().await.unwrap();
    let second_metrics = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &second_metrics,
            ORDERED_PLAN_CACHE,
            Some(("outcome", "hit"))
        ),
        1,
        "the identical second browse must hit the ordered-plan cache"
    );
    assert_eq!(
        counter_sum(
            &second_metrics,
            ORDERED_PLAN_CACHE,
            Some(("outcome", "miss"))
        ),
        0,
        "the identical second browse must not rebuild the ordered plan"
    );
    assert_eq!(
        counter_sum(&second_metrics, MERGE_PARTITIONS, None),
        1,
        "the cached plan must charge the same overlap partition as the fresh plan"
    );
    assert_eq!(
        counter_sum(
            &second_metrics,
            OUTPUT_ORDERING,
            Some(("outcome", "advertised"))
        ),
        1,
        "the cached plan must advertise one ordered scan"
    );
    assert_eq!(
        collect_ts(collect(second_plan, ctx.task_ctx()).await.unwrap()),
        newest_timestamps(100)
    );
}

/// #4366: the same browse under a global fan-in budget the coalesced merge
/// cannot fit keeps (restores) its singleton partitions, so it runs no merge —
/// and must not report one. Pre-fix the counter was charged inside the
/// arrangement loop, before the restore, and read 1 here.
#[tokio::test]
async fn restored_singleton_browse_reports_no_merge_partition() {
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    snapshotter.snapshot();

    let (_tmp, ice) = overlapping_files(8).await;
    let mut state = browse_context(8, 100).state();
    // The five files mutually overlap, so the coalesced partition needs five
    // concurrent streams; a budget of two refuses the arrangement.
    state
        .config_mut()
        .set_extension(Arc::new(OrderedMergeGlobalBudget { fan_in: 2 }));
    let ctx = SessionContext::new_with_state(state);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 100")
        .await
        .unwrap();
    let plan_str = format!(
        "{}",
        displayable(df.clone().create_physical_plan().await.unwrap().as_ref()).indent(true)
    );
    // The fallback is the pre-#4353 shape: one partition per file, still
    // advertised, merged by the SortPreservingMerge above.
    assert!(
        plan_str.contains(&format!(
            "SiglakeIcebergTableScan partitions:[{}]",
            FILES as usize
        )),
        "the refused arrangement must be put back to one partition per file:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "the restored singleton plan still advertises ordering:\n{plan_str}"
    );

    assert_eq!(
        collect_ts(df.collect().await.unwrap()),
        newest_timestamps(100)
    );

    assert_eq!(
        counter_sum(&snapshotter.snapshot().into_vec(), MERGE_PARTITIONS, None),
        0,
        "a plan restored to singleton partitions must not report an overlap merge"
    );
}

/// The coalesce is for SMALL limits only: a browse past
/// `SIGLAKE_ORDERED_SINGLE_PARTITION_MAX_LIMIT`'s bucket (here, no
/// `OrderedScanLimit` at all — the shape `sql.rs` leaves alone) keeps its
/// per-file scan parallelism.
#[tokio::test]
async fn ordered_scan_without_a_small_limit_keeps_its_singleton_partitions() {
    let (gate, snapshotter) = &*METRICS;
    let _held = gate.lock().await;
    snapshotter.snapshot();

    let (_tmp, ice) = overlapping_files(8).await;
    let ctx = siglake_storage::session_context_with_order(
        Some(8),
        None,
        Some(PreferredScanOrder::timestamp(true)),
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
