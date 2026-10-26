//! #7500 matched local attribution for clipped equality scans across layouts
//! that can and cannot prune.
//!
//! #7457 measured one layout: a label interleaved across every page at 25%
//! selectivity, where the reader predicate rejected no page and cost 23.6 ms of
//! paired-median collect time. Its report leaves the opposite case open — a
//! rare or clustered label, where row-group statistics or page selection can
//! drop data the residual-only route would have to decode.
//!
//! This test runs #7457's three residual-preserving arms unchanged over four
//! immutable snapshots. Query, projection, scheduler, reader settings and
//! clipped admission are fixed within each layout; only the fixture's placement
//! of the target label changes between layouts. Physical wrappers stay fixed
//! across arms too, except on `rare_clustered_single_file`, where pruning to one
//! file drops the clipped-limit shape — the test pins that difference rather
//! than hiding it.

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

/// One target row in every `RARE_PERIOD` rows: 80 per file, 1,200 in the
/// snapshot, 0.2% selectivity. Both rare layouts carry the same total so that
/// clustering is the only difference between them.
const RARE_PERIOD: usize = 500;
const RARE_ROWS: usize = FILES * ROWS_PER_FILE / RARE_PERIOD;
/// Clustered runs sit inside a file's first 20,000-row data page, so the file's
/// second page and every target-free file can be ruled out by min/max alone.
const CLUSTER_START: usize = 10_000;
/// `RareClustered` splits the same 1,200 rows over four files, which keeps more
/// than one file planned in every arm and so keeps the clipped-limit shape.
const CLUSTER_FILES: [usize; 4] = [3, 6, 9, 12];
/// `RareClusteredSingleFile` puts all 1,200 in one file.
const CLUSTER_FILE: usize = 7;

/// Non-target labels. All three sort below `us-east-2`, so a page or row group
/// holding only these has an upper bound strictly below the target and can be
/// dropped without reading it.
const OTHERS: [&str; 3] = ["ap-south-1", "eu-west-1", "us-east-1"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    /// #7457's fixture: four labels cycling row by row. The target is 25% of
    /// the rows and every page's min/max spans it.
    Interleaved,
    /// The target is one row in 500, spread over every file and every page.
    /// Statistics and page selection can drop nothing; only the reader
    /// predicate's selectivity differs from `Interleaved`.
    RareScattered,
    /// The same 1,200 target rows, in four contiguous 300-row runs, each inside
    /// one page of one file. Eleven files hold no target at all, and the four
    /// that do hold it in their first page only.
    RareClustered,
    /// All 1,200 rows in one page of one file. Statistics leave a single file
    /// planned, which is below the clipped-limit shape's two-partition floor.
    RareClusteredSingleFile,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Layout::Interleaved => "interleaved",
            Layout::RareScattered => "rare_scattered",
            Layout::RareClustered => "rare_clustered",
            Layout::RareClusteredSingleFile => "rare_clustered_single_file",
        }
    }

    /// Whether min/max bounds can rule out data for this layout's target.
    fn prunable(self) -> bool {
        matches!(
            self,
            Layout::RareClustered | Layout::RareClusteredSingleFile
        )
    }

    /// Whether every arm still plans more than one file. The clipped-limit
    /// coalescer requires two input partitions, so a layout that prunes to one
    /// file gives the reader-predicate arms a different physical shape.
    fn matched_shape(self) -> bool {
        !matches!(self, Layout::RareClusteredSingleFile)
    }

    fn region(self, file: usize, row: usize) -> &'static str {
        let clustered = |files: &[usize], run_rows: usize| {
            files.contains(&file) && (CLUSTER_START..CLUSTER_START + run_rows).contains(&row)
        };
        let target = match self {
            Layout::Interleaved => {
                return ["us-east-1", TARGET, "eu-west-1", "ap-south-1"][row % 4]
            }
            Layout::RareScattered => row.is_multiple_of(RARE_PERIOD),
            Layout::RareClustered => clustered(&CLUSTER_FILES, RARE_ROWS / CLUSTER_FILES.len()),
            Layout::RareClusteredSingleFile => clustered(&[CLUSTER_FILE], RARE_ROWS),
        };
        if target {
            TARGET
        } else {
            OTHERS[row % OTHERS.len()]
        }
    }
}

fn event(layout: Layout, file: usize, row: usize) -> Event {
    let region = layout.region(file, row);
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

async fn build_fixture(ice: &IcebergContext, layout: Layout) {
    for file in 0..FILES {
        let events = (0..ROWS_PER_FILE)
            .map(|row| event(layout, file, row))
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

/// Wrapper names with repartitioning dropped. A pruned plan reaches the filter
/// with fewer partitions than `target_partitions`, and DataFusion inserts a
/// `RepartitionExec` to fill them; that is the pruning showing up in the plan,
/// not a different query.
fn without_repartition(names: &[String]) -> Vec<&str> {
    names
        .iter()
        .map(String::as_str)
        .filter(|name| *name != "RepartitionExec")
        .collect()
}

fn wrapper_names(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut names = Vec::new();
    plan.apply(|node| {
        names.push(node.name().to_string());
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    names
}

/// The `partitions:[N]` the scan node reports. Manifest-level pruning happens
/// at planning time, so this is what the predicate left to execute.
fn planned_partitions(scan: &Arc<dyn ExecutionPlan>) -> usize {
    let text = datafusion::physical_plan::displayable(scan.as_ref())
        .one_line()
        .to_string();
    let (_, rest) = text.split_once("partitions:[").expect("scan display");
    let (count, _) = rest.split_once(']').expect("scan display");
    count.parse().expect("partition count")
}

#[derive(Debug, Clone)]
struct Run {
    collect: Duration,
    rows: Vec<String>,
    wrappers: Vec<String>,
    planned_partitions: usize,
    files_read: usize,
    row_groups_considered: usize,
    row_groups_pruned_stats: usize,
    row_groups_read: usize,
    rows_pruned_selection: usize,
    object_store_reads: usize,
    bytes_index: usize,
    bytes_data_requested: usize,
    decoded_bytes: usize,
}

impl Run {
    /// Per opened file. Wave-2 admission occasionally starts a second reader
    /// before the clipped fetch is satisfied, which scales every read counter
    /// by the number of files opened; the per-file value is what the arms can
    /// be compared on.
    fn per_file(&self, value: usize) -> usize {
        value / self.files_read.max(1)
    }
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
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)).await;
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
        wrappers: wrapper_names(&plan),
        planned_partitions: planned_partitions(&scans[0]),
        files_read: sum("files_read"),
        row_groups_considered: sum("row_groups_considered"),
        row_groups_pruned_stats: sum("row_groups_pruned_stats"),
        row_groups_read: sum("row_groups_read"),
        rows_pruned_selection: sum("rows_pruned_selection"),
        object_store_reads: sum("object_store_reads"),
        bytes_index: sum("bytes_index"),
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

fn assert_exact(run: &Run, arm: &str, layout: Layout) {
    assert_eq!(
        run.rows.len(),
        LIMIT,
        "{} {arm}: wrong row count",
        layout.name()
    );
    assert!(
        run.rows.iter().all(|row| row.contains("region=us-east-2")),
        "{} {arm}: non-matching row returned",
        layout.name()
    );
    assert_eq!(
        run.rows.iter().collect::<HashSet<_>>().len(),
        LIMIT,
        "{} {arm}: duplicate rows returned",
        layout.name()
    );
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

/// Median of the paired per-triple difference, in milliseconds. Pairing keeps
/// a slow triple from moving one arm's median without moving the other's.
fn median_paired_ms(left: &[Run], right: &[Run]) -> f64 {
    let mut diffs = left
        .iter()
        .zip(right)
        .map(|(l, r)| l.collect.as_micros() as i128 - r.collect.as_micros() as i128)
        .collect::<Vec<_>>();
    diffs.sort_unstable();
    diffs[diffs.len() / 2] as f64 / 1_000.0
}

struct LayoutResult {
    pushed: Vec<Run>,
    without_selection: Vec<Run>,
    suppressed: Vec<Run>,
}

async fn measure(layout: Layout) -> LayoutResult {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    build_fixture(&ice, layout).await;

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

    let mut result = LayoutResult {
        pushed: Vec::new(),
        without_selection: Vec::new(),
        suppressed: Vec::new(),
    };
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
        let fields = |scan: &Arc<dyn ExecutionPlan>| {
            scan.schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        };
        let pushed_fields = fields(&pushed_scan);
        assert_eq!(pushed_fields, fields(&suppressed_scan));
        assert_eq!(pushed_fields, fields(&without_selection_scan));
        assert!(
            pushed_fields.iter().any(|field| field == "source"),
            "residual needs source"
        );

        let display = |plan: &Arc<dyn ExecutionPlan>| {
            datafusion::physical_plan::displayable(plan.as_ref())
                .indent(false)
                .to_string()
        };
        let pushed_display = display(&pushed_plan);
        let without_selection_display = display(&without_selection_plan);
        let suppressed_display = display(&suppressed_plan);
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
        assert_exact(&pushed, "pushed", layout);
        assert_exact(&without_selection, "no_selection", layout);
        assert_exact(&suppressed, "suppressed", layout);
        assert_eq!(
            pushed.wrappers,
            without_selection.wrappers,
            "{}: disabling RowSelection must not change wrappers",
            layout.name()
        );
        if !layout.prunable() {
            assert_eq!(
                pushed.wrappers,
                suppressed.wrappers,
                "{}: the physical wrappers must stay fixed",
                layout.name()
            );
        } else if layout.matched_shape() {
            // On a layout the predicate CAN prune, the arms cannot be fully
            // plan-matched: file bounds drop files at planning time, so the
            // reader-predicate arms reach the filter with fewer partitions
            // than `target_partitions` and DataFusion repartitions to fill
            // them. Everything else, including the clipped-limit shape, is
            // identical. Pruning is the effect under measurement, so this is
            // reported rather than suppressed.
            assert_eq!(
                without_repartition(&pushed.wrappers),
                without_repartition(&suppressed.wrappers),
                "{}: only repartitioning may differ",
                layout.name()
            );
            assert!(pushed
                .wrappers
                .iter()
                .any(|name| name == "ClippedLimitCoalesceExec"));
            assert!(suppressed
                .wrappers
                .iter()
                .any(|name| name == "ClippedLimitCoalesceExec"));
        } else {
            // Not a defect and not matched either: statistics left one file
            // planned, and `apply_clipped_limit_backpressure` needs two input
            // partitions, so the reader-predicate arms lose the clipped shape
            // the suppressed arm still has. Wall time across these arms is a
            // plan-shape comparison as much as a pushdown one.
            assert_eq!(pushed.planned_partitions, 1);
            assert!(suppressed.planned_partitions > 1);
            assert!(!pushed
                .wrappers
                .iter()
                .any(|name| name == "ClippedLimitCoalesceExec"));
            assert!(suppressed
                .wrappers
                .iter()
                .any(|name| name == "ClippedLimitCoalesceExec"));
        }

        // A reader that never receives the predicate cannot prune, whatever
        // the layout. This is the control's floor, not a measurement.
        assert_eq!(
            suppressed.rows_pruned_selection,
            0,
            "{}: suppressed arm has no predicate to select with",
            layout.name()
        );
        assert_eq!(
            suppressed.row_groups_pruned_stats,
            0,
            "{}: suppressed arm has no predicate to prune statistics with",
            layout.name()
        );

        if layout.prunable() {
            assert!(
                pushed.planned_partitions < suppressed.planned_partitions,
                "{}: file bounds must leave the reader arm fewer partitions \
                 (pushed {} vs suppressed {})",
                layout.name(),
                pushed.planned_partitions,
                suppressed.planned_partitions
            );
            assert!(
                pushed.rows_pruned_selection > 0,
                "{}: the clustered run sits in one page, so selection must drop \
                 the other one",
                layout.name()
            );
            assert_eq!(
                without_selection.rows_pruned_selection,
                0,
                "{}: the no-selection arm keeps statistics but drops page selection",
                layout.name()
            );
        } else {
            assert_eq!(
                pushed.planned_partitions,
                suppressed.planned_partitions,
                "{}: nothing the predicate knows can drop a file",
                layout.name()
            );
            assert_eq!(
                pushed.rows_pruned_selection,
                0,
                "{}: every page's bounds span the target",
                layout.name()
            );
            assert_eq!(
                pushed.row_groups_pruned_stats,
                0,
                "{}: every row group's bounds span the target",
                layout.name()
            );
        }

        let line = |arm: &str, run: &Run| {
            format!(
                "{arm}_collect={:?} {arm}_planned={} {arm}_files={} \
                 {arm}_rg_considered={} \
                 {arm}_rg_pruned_stats={} {arm}_rg_read={} {arm}_sel_pruned={} \
                 {arm}_reads={} {arm}_index={} {arm}_requested={} {arm}_decoded={}",
                run.collect,
                run.planned_partitions,
                run.files_read,
                run.row_groups_considered,
                run.row_groups_pruned_stats,
                run.row_groups_read,
                run.rows_pruned_selection,
                run.object_store_reads,
                run.bytes_index,
                run.bytes_data_requested,
                run.decoded_bytes,
            )
        };
        eprintln!(
            "CLIPPED-LAYOUT layout={} pair={pair} order={order} {} {} {}",
            layout.name(),
            line("pushed", &pushed),
            line("no_selection", &without_selection),
            line("suppressed", &suppressed),
        );

        result.pushed.push(pushed);
        result.without_selection.push(without_selection);
        result.suppressed.push(suppressed);
    }
    result
}

fn report(layout: Layout, result: &LayoutResult) {
    let summary = |arm: &str, runs: &[Run]| {
        format!(
            "{arm}_median_ms={:.3} {arm}_planned={} {arm}_files={} \
             {arm}_rg_considered={} \
             {arm}_rg_pruned_stats={} {arm}_rg_read={} {arm}_sel_pruned={} \
             {arm}_reads_per_file={} {arm}_index_per_file={} \
             {arm}_requested_per_file={} {arm}_decoded_per_file={}",
            median_ms(runs),
            median_count(runs, |run| run.planned_partitions),
            median_count(runs, |run| run.files_read),
            median_count(runs, |run| run.row_groups_considered),
            median_count(runs, |run| run.row_groups_pruned_stats),
            median_count(runs, |run| run.row_groups_read),
            median_count(runs, |run| run.rows_pruned_selection),
            median_count(runs, |run| run.per_file(run.object_store_reads)),
            median_count(runs, |run| run.per_file(run.bytes_index)),
            median_count(runs, |run| run.per_file(run.bytes_data_requested)),
            median_count(runs, |run| run.per_file(run.decoded_bytes)),
        )
    };
    eprintln!(
        "CLIPPED-LAYOUT-SUMMARY layout={} pairs={PAIRS} {} {} {} \
         paired_pushed_minus_no_selection_ms={:.3} \
         paired_no_selection_minus_suppressed_ms={:.3} \
         paired_pushed_minus_suppressed_ms={:.3}",
        layout.name(),
        summary("pushed", &result.pushed),
        summary("no_selection", &result.without_selection),
        summary("suppressed", &result.suppressed),
        median_paired_ms(&result.pushed, &result.without_selection),
        median_paired_ms(&result.without_selection, &result.suppressed),
        median_paired_ms(&result.pushed, &result.suppressed),
    );
}

/// By-hand measurement: run alone on an idle lane. The deterministic checks pin
/// the matched control, its exact answers and each layout's pruning floor; wall
/// time is retained for attribution and is not a CI performance threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "matched local attribution; run alone with --ignored --nocapture"]
async fn clipped_equality_pushdown_layout_attribution() {
    for layout in [
        Layout::Interleaved,
        Layout::RareScattered,
        Layout::RareClustered,
        Layout::RareClusteredSingleFile,
    ] {
        let result = measure(layout).await;
        report(layout, &result);
    }
}
