use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};
use datafusion::physical_plan::displayable;
use iceberg::spec::NullOrder;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::TableIdent;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::PreferredScanOrder;

fn collect_rows(batches: Vec<arrow_array::RecordBatch>) -> Vec<(i64, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            let ts = siglake_core::column_nanos(batch.column(0)).unwrap();
            let raw = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|idx| (ts.value(idx), raw.value(idx).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn event_at(base: chrono::DateTime<Utc>, seconds: i64) -> Event {
    let mut event = Event::now(format!("row-{seconds}"));
    event.timestamp = base + Duration::seconds(seconds);
    event.raw = format!("row-{seconds}");
    event
}

async fn set_events_sort_descending(ice: &IcebergContext) {
    let ident = TableIdent::new(ice.namespace().clone(), "events".to_string());
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .replace_sort_order()
        .desc("timestamp", NullOrder::Last)
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
}

fn assert_output_ordering_desc(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    descending: bool,
) {
    let ordering = plan
        .properties()
        .output_ordering()
        .expect("reverse-scan plan should advertise output ordering");
    assert_eq!(ordering.len(), 1, "expected a single timestamp ordering");
    assert_eq!(
        ordering[0].options.descending, descending,
        "plan advertised the wrong timestamp direction: {ordering:?}"
    );
}

#[tokio::test]
async fn ascending_declared_table_serves_desc_limit_without_sort() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 12, 0, 0).unwrap();
    for offsets in [[0, 1, 2], [10, 11, 12], [20, 21, 22], [30, 31, 32]] {
        let events: Vec<Event> = offsets.into_iter().map(|s| event_at(base, s)).collect();
        ice.append_events(&events).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_order(
        Some(2),
        None,
        Some(PreferredScanOrder::timestamp(true)),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM events ORDER BY \"timestamp\" DESC LIMIT 5")
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "reverse ordered partitions should still early-stop:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "reverse-scan should avoid a blocking sort:\n{plan_str}"
    );
    assert_output_ordering_desc(&plan, true);
    assert_eq!(
        collect_rows(df.collect().await.unwrap()),
        [32, 31, 30, 22, 21]
            .into_iter()
            .map(|s| {
                (
                    (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap(),
                    format!("row-{s}"),
                )
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn descending_declared_table_serves_asc_limit_without_sort() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    set_events_sort_descending(&ice).await;
    let base = Utc.with_ymd_and_hms(2026, 6, 2, 12, 0, 0).unwrap();
    for offsets in [[0, 1, 2], [10, 11, 12], [20, 21, 22], [30, 31, 32]] {
        let events: Vec<Event> = offsets.into_iter().map(|s| event_at(base, s)).collect();
        ice.append_events(&events).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_order(
        Some(2),
        None,
        Some(PreferredScanOrder::timestamp(false)),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM events ORDER BY \"timestamp\" ASC LIMIT 5")
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        plan_str.contains("SortPreservingMergeExec"),
        "reverse-scanning a DESC table should early-stop in ASC order:\n{plan_str}"
    );
    assert!(
        !plan_str.contains("SortExec"),
        "ASC-on-DESC should avoid a blocking sort:\n{plan_str}"
    );
    assert_output_ordering_desc(&plan, false);
    assert_eq!(
        collect_rows(df.collect().await.unwrap()),
        [0, 1, 2, 10, 11]
            .into_iter()
            .map(|s| {
                (
                    (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap(),
                    format!("row-{s}"),
                )
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn reverse_scan_merges_overlapping_files_in_requested_direction() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 3, 12, 0, 0).unwrap();
    for offsets in [[0, 100], [1, 101], [2, 102], [3, 103]] {
        let events: Vec<Event> = offsets.into_iter().map(|s| event_at(base, s)).collect();
        ice.append_events(&events).await.unwrap();
    }

    let ctx = siglake_storage::session_context_with_order(
        Some(1),
        None,
        Some(PreferredScanOrder::timestamp(true)),
    );
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx
        .sql("SELECT \"timestamp\", raw FROM events ORDER BY \"timestamp\" DESC LIMIT 6")
        .await
        .unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let plan_str = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        !plan_str.contains("SortExec"),
        "reverse k-way merge should keep the scan ordered:\n{plan_str}"
    );
    assert_output_ordering_desc(&plan, true);
    assert_eq!(
        collect_rows(df.collect().await.unwrap()),
        [103, 102, 101, 100, 3, 2]
            .into_iter()
            .map(|s| {
                (
                    (base + Duration::seconds(s)).timestamp_nanos_opt().unwrap(),
                    format!("row-{s}"),
                )
            })
            .collect::<Vec<_>>()
    );
}

/// The `partitions:[N]` count the `SiglakeIcebergTableScan` advertised.
fn scan_partition_count(plan_str: &str) -> usize {
    let marker = "partitions:[";
    let start = plan_str.find(marker).expect("plan should contain the scan") + marker.len();
    let end = plan_str[start..].find(']').unwrap() + start;
    plan_str[start..end].parse().unwrap()
}

/// #4 scan parallelism: a pushed `LIMIT` whose scan advertises NO ordering should
/// fan out across `target_partitions` for parallel file opens; one that DOES
/// advertise an ordering (for an ordered early-stop) must keep its single in-order
/// partition.
#[tokio::test]
async fn unordered_limit_scan_parallelizes_but_ordered_limit_stays_single() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let base = Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap();
    // Four time-disjoint files.
    for offsets in [[0, 1, 2], [10, 11, 12], [20, 21, 22], [30, 31, 32]] {
        let events: Vec<Event> = offsets.into_iter().map(|s| event_at(base, s)).collect();
        ice.append_events(&events).await.unwrap();
    }
    // 4 target partitions, no query-requested order.
    let ctx = siglake_storage::session_context_with_order(Some(4), None, None);
    ice.register_with_datafusion(&ctx).await.unwrap();

    // (A) `timestamp` NOT projected -> the ordering gate refuses (`not_projected`),
    // so the pushed-limit scan is free to fan out across partitions.
    let plan_a = ctx
        .sql("SELECT raw FROM events LIMIT 4")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let a = format!("{}", displayable(plan_a.as_ref()).indent(true));
    assert!(
        scan_partition_count(&a) > 1,
        "unordered limit scan should parallelize:\n{a}"
    );

    // (B) `timestamp` projected over time-disjoint files -> the gate advertises an
    // ordering, so the scan keeps its single in-order partition (ordered early-stop).
    let plan_b = ctx
        .sql("SELECT \"timestamp\", raw FROM events LIMIT 4")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let b = format!("{}", displayable(plan_b.as_ref()).indent(true));
    assert_eq!(
        scan_partition_count(&b),
        1,
        "ordered limit scan must stay single-partition:\n{b}"
    );
}
