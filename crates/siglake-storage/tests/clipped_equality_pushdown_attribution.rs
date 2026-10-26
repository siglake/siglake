//! #7457 matched local attribution for clipped equality scans.
//!
//! All three arms run the same SQL over the same fifteen-file snapshot with the
//! same scheduler, reader settings, physical wrappers and scan projection.
//! All retain DataFusion's residual equality filter. The controls vary whether
//! the Iceberg reader receives the converted predicate and whether it applies
//! Parquet `RowSelection`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::{PredicatePushdownControl, QueryScanTuning, SiglakeIcebergTableScan};

const FILES: usize = 15;
const ROWS_PER_FILE: usize = 40_000;
const LIMIT: usize = 100;
const TARGET: &str = "us-east-2";
const PAIRS: usize = 7;

fn event(file: usize, row: usize) -> Event {
    let regions = ["us-east-1", TARGET, "eu-west-1", "ap-south-1"];
    let region = regions[row % regions.len()];
    let ordinal = file * ROWS_PER_FILE + row;
    Event {
        timestamp: Utc
            .timestamp_opt(1_700_000_000 + ordinal as i64, 0)
            .single()
            .unwrap(),
        host: format!("host-{:03}", row % 64),
        source: region.to_string(),
        sourcetype: "json".to_string(),
        index: "bench".to_string(),
        raw: format!(
            "file={file:02} row={row:06} region={region} service=checkout status=200 \
             detail=lorem ipsum dolor sit amet consectetur adipiscing elit"
        ),
        attributes: None,
    }
}

async fn build_fixture(ice: &IcebergContext) {
    for file in 0..FILES {
        let events = (0..ROWS_PER_FILE)
            .map(|row| event(file, row))
            .collect::<Vec<_>>();
        ice.append_events(&events).await.unwrap();
    }
}

fn context() -> SessionContext {
    let base = siglake_storage::session_context_with_target_partitions(Some(16));
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedScanLimit { limit: LIMIT }));
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedAdmissionWave {
            factor: 2,
        }));
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::OrderedScanTuning {
            bypass_reader_caches: true,
            ..Default::default()
        }));
    SessionContext::new_with_state(state)
}

fn tuning(control: PredicatePushdownControl) -> QueryScanTuning {
    QueryScanTuning {
        batch_size: Some(8_192),
        range_coalesce_bytes: Some(128 * 1024),
        range_fetch_concurrency: Some(4),
        predicate_pushdown_control: control,
        ..Default::default()
    }
}

fn scan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
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
    scans
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeProfile {
    name: String,
    elapsed_compute_ns: usize,
    output_rows: usize,
}

fn node_profiles(plan: &Arc<dyn ExecutionPlan>) -> Vec<NodeProfile> {
    let mut profiles = Vec::new();
    plan.apply(|node| {
        let aggregate = node.metrics().map(|metrics| metrics.aggregate_by_name());
        profiles.push(NodeProfile {
            name: node.name().to_string(),
            elapsed_compute_ns: aggregate
                .as_ref()
                .and_then(|metrics| metrics.elapsed_compute())
                .unwrap_or_default(),
            output_rows: aggregate
                .as_ref()
                .and_then(|metrics| metrics.output_rows())
                .unwrap_or_default(),
        });
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    profiles
}

#[derive(Debug, Clone)]
struct Run {
    collect: Duration,
    rows: Vec<String>,
    profiles: Vec<NodeProfile>,
    files_read: usize,
    row_groups_read: usize,
    rows_pruned_selection: usize,
    object_store_reads: usize,
    bytes_index: usize,
    bytes_data: usize,
    bytes_data_requested: usize,
    decoded_bytes: usize,
}

async fn run(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Run {
    let scans = scan_nodes(&plan);
    assert_eq!(scans.len(), 1, "one scan leaf expected");
    let started = Instant::now();
    let mut rows = Vec::new();
    {
        let task_ctx = siglake_storage::bounded_task_context_from(
            datafusion::execution::TaskContext::from(&ctx.state()),
        );
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let raw = batch
                .column_by_name("raw")
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            rows.extend(raw.iter().map(|value| value.unwrap().to_string()));
        }
    }
    let collect = started.elapsed();
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(settle.complete, "scan did not settle: {settle:?}");

    let metrics = scans[0].metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|metric| metric.as_usize())
            .unwrap_or_default()
    };
    Run {
        collect,
        rows,
        profiles: node_profiles(&plan),
        files_read: sum("files_read"),
        row_groups_read: sum("row_groups_read"),
        rows_pruned_selection: sum("rows_pruned_selection"),
        object_store_reads: sum("object_store_reads"),
        bytes_index: sum("bytes_index"),
        bytes_data: sum("bytes_data"),
        bytes_data_requested: sum("bytes_data_requested"),
        decoded_bytes: sum("decoded_bytes"),
    }
}

async fn plan(
    ctx: &SessionContext,
    sql: &str,
    control: PredicatePushdownControl,
) -> Arc<dyn ExecutionPlan> {
    siglake_storage::configure_query_scan_tuning(tuning(control));
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn assert_exact(run: &Run) {
    assert_eq!(run.rows.len(), LIMIT);
    assert!(run.rows.iter().all(|row| row.contains("region=us-east-2")));
    assert_eq!(run.rows.iter().collect::<HashSet<_>>().len(), LIMIT);
}

fn median_ms(runs: &[Run]) -> f64 {
    let mut micros = runs
        .iter()
        .map(|run| run.collect.as_micros())
        .collect::<Vec<_>>();
    micros.sort_unstable();
    micros[micros.len() / 2] as f64 / 1_000.0
}

fn median_count(runs: &[Run], value: impl Fn(&Run) -> usize) -> usize {
    let mut values = runs.iter().map(value).collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}

/// By-hand measurement: run alone on an idle lane. The deterministic checks
/// pin the matched control and its exact answers; wall time is retained for
/// attribution and is not a CI performance threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "matched local attribution; run alone with --ignored --nocapture"]
async fn clipped_equality_reader_pushdown_attribution() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    build_fixture(&ice).await;

    let pushed_ctx = context();
    let without_selection_ctx = context();
    let suppressed_ctx = context();
    ice.register_with_datafusion(&pushed_ctx).await.unwrap();
    ice.register_with_datafusion(&without_selection_ctx)
        .await
        .unwrap();
    ice.register_with_datafusion(&suppressed_ctx).await.unwrap();
    let sql = format!("SELECT timestamp, raw FROM events WHERE source = '{TARGET}' LIMIT {LIMIT}");

    // Warm table/manifest planning without reading data. Reader caches are
    // bypassed in every measured arm.
    let _ = plan(&pushed_ctx, &sql, PredicatePushdownControl::RetainResidual).await;
    let _ = plan(
        &without_selection_ctx,
        &sql,
        PredicatePushdownControl::RetainResidualWithoutRowSelection,
    )
    .await;
    let _ = plan(
        &suppressed_ctx,
        &sql,
        PredicatePushdownControl::SuppressReaderPredicate,
    )
    .await;

    let mut pushed_runs = Vec::new();
    let mut without_selection_runs = Vec::new();
    let mut suppressed_runs = Vec::new();
    for pair in 0..PAIRS {
        let pushed_plan = plan(&pushed_ctx, &sql, PredicatePushdownControl::RetainResidual).await;
        let without_selection_plan = plan(
            &without_selection_ctx,
            &sql,
            PredicatePushdownControl::RetainResidualWithoutRowSelection,
        )
        .await;
        let suppressed_plan = plan(
            &suppressed_ctx,
            &sql,
            PredicatePushdownControl::SuppressReaderPredicate,
        )
        .await;
        siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());

        let pushed_scan = scan_nodes(&pushed_plan).pop().unwrap();
        let without_selection_scan = scan_nodes(&without_selection_plan).pop().unwrap();
        let suppressed_scan = scan_nodes(&suppressed_plan).pop().unwrap();
        let pushed_fields = pushed_scan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let suppressed_fields = suppressed_scan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let without_selection_fields = without_selection_scan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(pushed_fields, suppressed_fields);
        assert_eq!(pushed_fields, without_selection_fields);
        assert!(
            pushed_fields.iter().any(|field| field == "source"),
            "residual needs source"
        );

        let pushed_display = datafusion::physical_plan::displayable(pushed_plan.as_ref())
            .indent(false)
            .to_string();
        let suppressed_display = datafusion::physical_plan::displayable(suppressed_plan.as_ref())
            .indent(false)
            .to_string();
        let without_selection_display =
            datafusion::physical_plan::displayable(without_selection_plan.as_ref())
                .indent(false)
                .to_string();
        assert!(pushed_display.contains("FilterExec"));
        assert!(without_selection_display.contains("FilterExec"));
        assert!(suppressed_display.contains("FilterExec"));
        assert!(pushed_display.contains("predicate:[source = \"us-east-2\"]"));
        assert!(without_selection_display.contains("predicate:[source = \"us-east-2\"]"));
        assert!(suppressed_display.contains("predicate:[]"));

        let order = pair % 3;
        let (pushed, without_selection, suppressed) = match order {
            0 => (
                run(&pushed_ctx, pushed_plan).await,
                run(&without_selection_ctx, without_selection_plan).await,
                run(&suppressed_ctx, suppressed_plan).await,
            ),
            1 => {
                let without_selection = run(&without_selection_ctx, without_selection_plan).await;
                let suppressed = run(&suppressed_ctx, suppressed_plan).await;
                let pushed = run(&pushed_ctx, pushed_plan).await;
                (pushed, without_selection, suppressed)
            }
            _ => {
                let suppressed = run(&suppressed_ctx, suppressed_plan).await;
                let pushed = run(&pushed_ctx, pushed_plan).await;
                let without_selection = run(&without_selection_ctx, without_selection_plan).await;
                (pushed, without_selection, suppressed)
            }
        };
        assert_exact(&pushed);
        assert_exact(&without_selection);
        assert_exact(&suppressed);
        assert_eq!(
            pushed
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>(),
            suppressed
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>(),
            "the physical wrappers must stay fixed"
        );
        assert_eq!(
            pushed
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>(),
            without_selection
                .profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>(),
            "disabling RowSelection must not change wrappers"
        );
        eprintln!(
            "CLIPPED-EQUALITY pair={pair} order={order} \
             pushed_collect={:?} pushed_profiles={:?} pushed_files={} pushed_row_groups={} \
             pushed_reads={} pushed_index={} pushed_data={} pushed_requested={} pushed_decoded={} \
             no_selection_collect={:?} no_selection_profiles={:?} no_selection_files={} \
             no_selection_row_groups={} no_selection_reads={} no_selection_index={} \
             no_selection_data={} no_selection_requested={} no_selection_decoded={} \
             suppressed_collect={:?} suppressed_profiles={:?} suppressed_files={} \
             suppressed_row_groups={} suppressed_reads={} suppressed_index={} \
             suppressed_data={} suppressed_requested={} suppressed_decoded={}",
            pushed.collect,
            pushed.profiles,
            pushed.files_read,
            pushed.row_groups_read,
            pushed.object_store_reads,
            pushed.bytes_index,
            pushed.bytes_data,
            pushed.bytes_data_requested,
            pushed.decoded_bytes,
            without_selection.collect,
            without_selection.profiles,
            without_selection.files_read,
            without_selection.row_groups_read,
            without_selection.object_store_reads,
            without_selection.bytes_index,
            without_selection.bytes_data,
            without_selection.bytes_data_requested,
            without_selection.decoded_bytes,
            suppressed.collect,
            suppressed.profiles,
            suppressed.files_read,
            suppressed.row_groups_read,
            suppressed.object_store_reads,
            suppressed.bytes_index,
            suppressed.bytes_data,
            suppressed.bytes_data_requested,
            suppressed.decoded_bytes,
        );
        assert_eq!(
            pushed.object_store_reads, without_selection.object_store_reads,
            "column and offset indexes are preloaded in both reader arms"
        );
        assert_eq!(pushed.bytes_index, without_selection.bytes_index);
        assert_eq!(pushed.decoded_bytes, without_selection.decoded_bytes);
        assert_eq!(
            pushed.rows_pruned_selection, 0,
            "interleaved labels give every page a matching min/max"
        );
        assert_eq!(without_selection.rows_pruned_selection, 0);
        assert_eq!(suppressed.rows_pruned_selection, 0);
        pushed_runs.push(pushed);
        without_selection_runs.push(without_selection);
        suppressed_runs.push(suppressed);
    }

    eprintln!(
        "CLIPPED-EQUALITY pairs={PAIRS} pushed_median_ms={:.3} \
         no_selection_median_ms={:.3} suppressed_median_ms={:.3} files_read={:?} \
         row_groups_read={:?} \
         object_store_reads={:?} bytes_index={:?} bytes_data={:?} \
         bytes_data_requested={:?} decoded_bytes={:?}",
        median_ms(&pushed_runs),
        median_ms(&without_selection_runs),
        median_ms(&suppressed_runs),
        (
            median_count(&pushed_runs, |run| run.files_read),
            median_count(&suppressed_runs, |run| run.files_read),
        ),
        (
            median_count(&pushed_runs, |run| run.row_groups_read),
            median_count(&suppressed_runs, |run| run.row_groups_read),
        ),
        (
            median_count(&pushed_runs, |run| run.object_store_reads),
            median_count(&suppressed_runs, |run| run.object_store_reads),
        ),
        (
            median_count(&pushed_runs, |run| run.bytes_index),
            median_count(&suppressed_runs, |run| run.bytes_index),
        ),
        (
            median_count(&pushed_runs, |run| run.bytes_data),
            median_count(&suppressed_runs, |run| run.bytes_data),
        ),
        (
            median_count(&pushed_runs, |run| run.bytes_data_requested),
            median_count(&suppressed_runs, |run| run.bytes_data_requested),
        ),
        (
            median_count(&pushed_runs, |run| run.decoded_bytes),
            median_count(&suppressed_runs, |run| run.decoded_bytes),
        ),
    );
}
