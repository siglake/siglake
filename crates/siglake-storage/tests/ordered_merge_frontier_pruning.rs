//! #4364: an ordered LIMIT's overlap merge admits file streams lazily by
//! manifest bound, leaving an older suffix unopened after the nth result is
//! known. The unpruned control proves the fixture really opens every file.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{OrderedScanLimit, PreferredScanOrder, SiglakeIcebergTableScan};

const FILES: usize = 6;
const LIMIT: usize = 100;

async fn overlapping_frontier_fixture(ice: &IcebergContext) {
    let base = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    // Every file also contains offset 0, so all six intervals overlap and the
    // control merge must open all six heads. Only the first two maxima can
    // contribute to the newest 100; the third maximum is below their nth row.
    for max_ms in [1000_i64, 990, 940, 880, 820, 760] {
        let mut events = Vec::with_capacity(LIMIT);
        for offset in (max_ms - 98)..=max_ms {
            let mut event = Event::now(format!("row-{max_ms}-{offset}"));
            event.timestamp = base + ChronoDuration::milliseconds(offset);
            events.push(event);
        }
        let mut old = Event::now(format!("row-{max_ms}-old"));
        old.timestamp = base;
        events.push(old);
        ice.append_events(&events).await.unwrap();
    }
    assert_eq!(
        ice.live_data_files(ice.events_table_ident())
            .await
            .unwrap()
            .len(),
        FILES
    );
}

fn browse_context(with_source_limit: bool) -> SessionContext {
    let base = siglake_storage::session_context_with_order(
        Some(1),
        None,
        Some(PreferredScanOrder { descending: true }),
    );
    if !with_source_limit {
        return base;
    }
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(OrderedScanLimit { limit: LIMIT }));
    SessionContext::new_with_state(state)
}

fn scan_node(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut scans = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            scans.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert_eq!(scans.len(), 1, "one scan leaf expected");
    scans.pop().unwrap()
}

async fn run(ice: &IcebergContext, with_source_limit: bool) -> (Vec<i64>, usize) {
    let ctx = browse_context(with_source_limit);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql("SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 100")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scan = scan_node(&plan);
    let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
        .await
        .unwrap();
    let settled = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(10)).await;
    assert!(settled.complete, "scan did not settle: {settled:?}");

    let timestamps = batches
        .iter()
        .flat_map(|batch| {
            let column = siglake_core::column_nanos(batch.column(0)).unwrap();
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    let metrics = scan.metrics().unwrap().aggregate_by_name();
    let files_read = metrics
        .sum_by_name("files_read")
        .map(|metric| metric.as_usize())
        .unwrap_or_default();
    (timestamps, files_read)
}

#[tokio::test]
async fn newest_frontier_leaves_older_overlapping_files_unopened() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    overlapping_frontier_fixture(&ice).await;

    let (expected, control_files) = run(&ice, false).await;
    assert_eq!(expected.len(), LIMIT);
    assert_eq!(
        control_files, FILES,
        "the unpruned control must open every overlapping file"
    );

    let (actual, pruned_files) = run(&ice, true).await;
    assert_eq!(actual, expected, "frontier pruning changed the newest 100");
    assert!(
        pruned_files < FILES,
        "frontier pruning opened all {FILES} files"
    );
    assert_eq!(
        pruned_files, 2,
        "only the two files contributing to the newest 100 should open"
    );
}
