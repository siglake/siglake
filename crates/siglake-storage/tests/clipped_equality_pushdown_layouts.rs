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

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning};
use siglake_storage::{PredicatePushdownControl, QueryScanTuning, SiglakeIcebergTableScan};

/// Logical 40,000-row append batches. The `events` table is day-partitioned,
/// so an append that crosses midnight produces two physical Parquet files.
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

fn context(wave: usize) -> SessionContext {
    let base = siglake_storage::session_context_with_target_partitions(Some(16));
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedScanLimit { limit: LIMIT }));
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedAdmissionWave {
            factor: wave,
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

static PREDICATE_INPUT_ROWS: AtomicUsize = AtomicUsize::new(0);
static PREDICATE_OUTPUT_ROWS: AtomicUsize = AtomicUsize::new(0);

fn observe_predicate_evaluation(input_rows: usize, output_rows: usize) {
    PREDICATE_INPUT_ROWS.fetch_add(input_rows, Ordering::Relaxed);
    PREDICATE_OUTPUT_ROWS.fetch_add(output_rows, Ordering::Relaxed);
}

fn reset_predicate_evaluation() {
    PREDICATE_INPUT_ROWS.store(0, Ordering::Relaxed);
    PREDICATE_OUTPUT_ROWS.store(0, Ordering::Relaxed);
}

fn predicate_evaluation() -> (usize, usize) {
    (
        PREDICATE_INPUT_ROWS.load(Ordering::Relaxed),
        PREDICATE_OUTPUT_ROWS.load(Ordering::Relaxed),
    )
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
    manifest_files_pruned_metrics: usize,
    files_read: usize,
    row_groups_considered: usize,
    row_groups_pruned_stats: usize,
    row_groups_read: usize,
    rows_pruned_selection: usize,
    object_store_reads: usize,
    bytes_index: usize,
    bytes_data_requested: usize,
    decoded_bytes: usize,
    per_file_work: Vec<PerFileWork>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PerFileWork {
    requested: usize,
    decoded: usize,
    opened: usize,
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

    let scan_metrics = scans[0].metrics().unwrap();
    let metrics = scan_metrics.aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|metric| metric.as_usize())
            .unwrap_or_default()
    };
    // The fixture has one file task per scan partition. Admission tickets name
    // those groups deterministically; a settled partition with `files_read=1`
    // is therefore one actually admitted/opened file. Keep its requested and
    // decoded bytes together instead of inferring a per-file mean from totals.
    let mut per_partition = BTreeMap::<usize, PerFileWork>::new();
    for metric in scan_metrics.iter() {
        let Some(partition) = metric.partition() else {
            continue;
        };
        let work = per_partition.entry(partition).or_default();
        match metric.value().name() {
            "files_read" => work.opened += metric.value().as_usize(),
            "bytes_data_requested" => work.requested += metric.value().as_usize(),
            "decoded_bytes" => work.decoded += metric.value().as_usize(),
            _ => {}
        }
    }
    let per_file_work = per_partition
        .into_values()
        .filter(|work| work.opened > 0)
        .collect::<Vec<_>>();

    Run {
        collect,
        rows,
        wrappers: wrapper_names(&plan),
        planned_partitions: planned_partitions(&scans[0]),
        manifest_files_pruned_metrics: scans[0]
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .unwrap()
            .manifest_files_pruned_metrics() as usize,
        files_read: sum("files_read"),
        row_groups_considered: sum("row_groups_considered"),
        row_groups_pruned_stats: sum("row_groups_pruned_stats"),
        row_groups_read: sum("row_groups_read"),
        rows_pruned_selection: sum("rows_pruned_selection"),
        object_store_reads: sum("object_store_reads"),
        bytes_index: sum("bytes_index"),
        bytes_data_requested: sum("bytes_data_requested"),
        decoded_bytes: sum("decoded_bytes"),
        per_file_work,
    }
}

async fn plan(
    ctx: &SessionContext,
    sql: &str,
    control: PredicatePushdownControl,
) -> Arc<dyn ExecutionPlan> {
    plan_with_tuning(ctx, sql, tuning(control)).await
}

async fn plan_with_tuning(
    ctx: &SessionContext,
    sql: &str,
    tuning: QueryScanTuning,
) -> Arc<dyn ExecutionPlan> {
    siglake_storage::configure_query_scan_tuning(tuning);
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn observed_tuning() -> QueryScanTuning {
    QueryScanTuning {
        predicate_evaluation_observer: Some(observe_predicate_evaluation),
        ..tuning(PredicatePushdownControl::RetainResidual)
    }
}

fn learned_cache_tuning() -> QueryScanTuning {
    QueryScanTuning {
        clipped_equality_learned_gate_prototype: true,
        ..tuning(PredicatePushdownControl::Production)
    }
}

fn learned_event(ordinal: usize, source: &str, attributes: Option<&str>) -> Event {
    Event {
        timestamp: Utc
            .timestamp_opt(1_710_000_000, (ordinal % 1_000_000_000) as u32)
            .single()
            .unwrap(),
        host: "fixture".to_string(),
        source: source.to_string(),
        sourcetype: "json".to_string(),
        index: "bench".to_string(),
        raw: format!("row={ordinal} source={source} attributes={attributes:?}"),
        attributes: attributes.map(str::to_string),
    }
}

fn scan_display(plan: &Arc<dyn ExecutionPlan>) -> String {
    datafusion::physical_plan::displayable(scan_nodes(plan)[0].as_ref())
        .one_line()
        .to_string()
}

fn assert_reader_predicate(plan: &Arc<dyn ExecutionPlan>, expected: bool) {
    let display = scan_display(plan);
    assert_eq!(
        !display.contains("predicate:[]"),
        expected,
        "unexpected reader predicate state: {display}"
    );
    assert!(
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string()
            .contains("FilterExec"),
        "the exact residual must remain above the learned scan"
    );
}

/// A learned signal is deliberately conservative: page selection must have
/// rejected nothing, and at least one eighth of the rows presented to the
/// reader predicate must have survived. A missing observation keeps today's
/// pushed predicate.
fn learned_gate_control(
    predicate_input_rows: usize,
    predicate_output_rows: usize,
    rows_pruned_selection: usize,
) -> PredicatePushdownControl {
    if predicate_input_rows > 0
        && rows_pruned_selection == 0
        && predicate_output_rows.saturating_mul(8) >= predicate_input_rows
    {
        PredicatePushdownControl::SuppressReaderPredicate
    } else {
        PredicatePushdownControl::RetainResidual
    }
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

    let pushed_ctx = context(2);
    let without_selection_ctx = context(2);
    let suppressed_ctx = context(2);
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
            match layout {
                Layout::RareClustered => assert_eq!(
                    pushed.rows_pruned_selection,
                    3 * 19_520 + 5_120,
                    "three 40,000-row files prune 19,520 rows; the fourth target-bearing \
                     file ends at the day boundary after 25,600 rows and prunes 5,120"
                ),
                Layout::RareClusteredSingleFile => assert_eq!(
                    pushed.rows_pruned_selection, 19_520,
                    "the 40,000-row target-bearing file prunes its 19,520-row second page"
                ),
                Layout::Interleaved | Layout::RareScattered => unreachable!(),
            }
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
             {arm}_reads={} {arm}_index={} \
             {arm}_requested={} {arm}_decoded={}",
            median_ms(runs),
            median_count(runs, |run| run.planned_partitions),
            median_count(runs, |run| run.files_read),
            median_count(runs, |run| run.row_groups_considered),
            median_count(runs, |run| run.row_groups_pruned_stats),
            median_count(runs, |run| run.row_groups_read),
            median_count(runs, |run| run.rows_pruned_selection),
            median_count(runs, |run| run.object_store_reads),
            median_count(runs, |run| run.bytes_index),
            median_count(runs, |run| run.bytes_data_requested),
            median_count(runs, |run| run.decoded_bytes),
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

#[derive(Debug, Clone, Copy)]
struct LearnedSignal {
    predicate_input_rows: usize,
    predicate_output_rows: usize,
    rows_pruned_selection: usize,
}

impl LearnedSignal {
    fn control(self) -> PredicatePushdownControl {
        learned_gate_control(
            self.predicate_input_rows,
            self.predicate_output_rows,
            self.rows_pruned_selection,
        )
    }
}

struct GateLayoutResult {
    signal: LearnedSignal,
    baseline: Vec<Run>,
    observed: Vec<Run>,
    gate: Vec<Run>,
}

async fn measured_observation(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> (Run, usize, usize) {
    reset_predicate_evaluation();
    let run = run(ctx, plan).await;
    let (predicate_input_rows, predicate_output_rows) = predicate_evaluation();
    assert!(
        predicate_input_rows > 0,
        "the observed pushed arm must evaluate a reader predicate"
    );
    assert!(
        predicate_output_rows <= predicate_input_rows,
        "a predicate cannot emit more rows than it sees"
    );
    (run, predicate_input_rows, predicate_output_rows)
}

/// Measure a snapshot-local learned gate. The first pushed execution acquires
/// the signal while returning the ordinary exact answer. A cold or absent
/// signal therefore falls back to the baseline; only later executions of the
/// same snapshot and equality can suppress the reader predicate.
async fn measure_learned_gate(layout: Layout) -> GateLayoutResult {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    build_fixture(&ice, layout).await;

    let baseline_ctx = context(2);
    let observed_ctx = context(2);
    let gate_ctx = context(2);
    ice.register_with_datafusion(&baseline_ctx).await.unwrap();
    ice.register_with_datafusion(&observed_ctx).await.unwrap();
    ice.register_with_datafusion(&gate_ctx).await.unwrap();
    let sql = format!("SELECT timestamp, raw FROM events WHERE source = '{TARGET}' LIMIT {LIMIT}");

    // Planning warm-up does not read data. The following ordinary pushed run
    // is the cold-cache fallback and supplies the first verified signal.
    let _ = plan(
        &baseline_ctx,
        &sql,
        PredicatePushdownControl::RetainResidual,
    )
    .await;
    let probe_plan = plan_with_tuning(&observed_ctx, &sql, observed_tuning()).await;
    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    let (probe, predicate_input_rows, predicate_output_rows) =
        measured_observation(&observed_ctx, probe_plan).await;
    assert_exact(&probe, "signal_probe", layout);
    let signal = LearnedSignal {
        predicate_input_rows,
        predicate_output_rows,
        rows_pruned_selection: probe.rows_pruned_selection,
    };
    let gate_control = signal.control();
    assert_eq!(
        gate_control == PredicatePushdownControl::SuppressReaderPredicate,
        layout == Layout::Interleaved,
        "only the common interleaved label should cross the measured threshold"
    );

    let mut result = GateLayoutResult {
        signal,
        baseline: Vec::new(),
        observed: Vec::new(),
        gate: Vec::new(),
    };
    for pair in 0..PAIRS {
        let baseline_plan = plan(
            &baseline_ctx,
            &sql,
            PredicatePushdownControl::RetainResidual,
        )
        .await;
        let observed_plan = plan_with_tuning(&observed_ctx, &sql, observed_tuning()).await;
        let gate_plan = plan(&gate_ctx, &sql, gate_control).await;
        siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
        let scan_fields = |plan: &Arc<dyn ExecutionPlan>| {
            scan_nodes(plan)[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        };
        let baseline_fields = scan_fields(&baseline_plan);
        assert_eq!(baseline_fields, scan_fields(&observed_plan));
        assert_eq!(baseline_fields, scan_fields(&gate_plan));
        assert!(baseline_fields.iter().any(|field| field == "source"));
        let display = |plan: &Arc<dyn ExecutionPlan>| {
            datafusion::physical_plan::displayable(plan.as_ref())
                .indent(false)
                .to_string()
        };
        let baseline_display = display(&baseline_plan);
        let observed_display = display(&observed_plan);
        let gate_display = display(&gate_plan);
        assert!(baseline_display.contains("FilterExec"));
        assert!(observed_display.contains("FilterExec"));
        assert!(gate_display.contains("FilterExec"));
        assert!(baseline_display.contains("predicate:[source = \"us-east-2\"]"));
        assert!(observed_display.contains("predicate:[source = \"us-east-2\"]"));

        let order = pair % 3;
        let (baseline, observed, observed_input, observed_output, gate) = match order {
            0 => {
                let baseline = run(&baseline_ctx, baseline_plan).await;
                let (observed, input, output) =
                    measured_observation(&observed_ctx, observed_plan).await;
                let gate = run(&gate_ctx, gate_plan).await;
                (baseline, observed, input, output, gate)
            }
            1 => {
                let (observed, input, output) =
                    measured_observation(&observed_ctx, observed_plan).await;
                let gate = run(&gate_ctx, gate_plan).await;
                let baseline = run(&baseline_ctx, baseline_plan).await;
                (baseline, observed, input, output, gate)
            }
            _ => {
                let gate = run(&gate_ctx, gate_plan).await;
                let baseline = run(&baseline_ctx, baseline_plan).await;
                let (observed, input, output) =
                    measured_observation(&observed_ctx, observed_plan).await;
                (baseline, observed, input, output, gate)
            }
        };
        assert_exact(&baseline, "baseline", layout);
        assert_exact(&observed, "observed", layout);
        assert_exact(&gate, "gate", layout);
        assert_eq!(baseline.wrappers, observed.wrappers);
        assert_eq!(baseline.planned_partitions, observed.planned_partitions);
        assert_eq!(
            baseline.rows_pruned_selection,
            observed.rows_pruned_selection
        );
        assert_eq!(
            learned_gate_control(
                observed_input,
                observed_output,
                observed.rows_pruned_selection,
            ),
            gate_control,
            "{}: repeated observations must keep the same gate decision",
            layout.name()
        );
        if gate_control == PredicatePushdownControl::RetainResidual {
            assert!(gate_display.contains("predicate:[source = \"us-east-2\"]"));
            assert_eq!(baseline.wrappers, gate.wrappers);
            assert_eq!(baseline.planned_partitions, gate.planned_partitions);
            assert_eq!(baseline.rows_pruned_selection, gate.rows_pruned_selection);
        } else {
            assert!(gate_display.contains("predicate:[]"));
        }

        eprintln!(
            "CLIPPED-GATE layout={} pair={pair} order={order} signal_input={} \
             signal_output={} signal_selection={} baseline_collect={:?} \
             observed_collect={:?} gate_collect={:?}",
            layout.name(),
            observed_input,
            observed_output,
            observed.rows_pruned_selection,
            baseline.collect,
            observed.collect,
            gate.collect,
        );
        result.baseline.push(baseline);
        result.observed.push(observed);
        result.gate.push(gate);
    }
    result
}

fn report_gate(layout: Layout, result: &GateLayoutResult) {
    eprintln!(
        "CLIPPED-GATE-SUMMARY layout={} pairs={PAIRS} signal_input={} \
         signal_output={} signal_selection={} decision={:?} \
         baseline_median_ms={:.3} observed_median_ms={:.3} gate_median_ms={:.3} \
         paired_observed_minus_baseline_ms={:.3} paired_gate_minus_baseline_ms={:.3}",
        layout.name(),
        result.signal.predicate_input_rows,
        result.signal.predicate_output_rows,
        result.signal.rows_pruned_selection,
        result.signal.control(),
        median_ms(&result.baseline),
        median_ms(&result.observed),
        median_ms(&result.gate),
        median_paired_ms(&result.observed, &result.baseline),
        median_paired_ms(&result.gate, &result.baseline),
    );
}

struct WaveLayoutResult {
    wave2: Vec<Run>,
    wave4: Vec<Run>,
}

fn assert_wave_run(run: &Run, wave: usize, layout: Layout) {
    assert_exact(run, &format!("wave{wave}"), layout);
    assert_eq!(
        run.per_file_work
            .iter()
            .map(|work| work.opened)
            .sum::<usize>(),
        run.files_read,
        "{} wave{wave}: admitted/opened file detail must match the settled total",
        layout.name()
    );
    assert_eq!(
        run.per_file_work
            .iter()
            .map(|work| work.requested)
            .sum::<usize>(),
        run.bytes_data_requested,
        "{} wave{wave}: per-file requested bytes must match the settled total",
        layout.name()
    );
    assert_eq!(
        run.per_file_work
            .iter()
            .map(|work| work.decoded)
            .sum::<usize>(),
        run.decoded_bytes,
        "{} wave{wave}: per-file decoded bytes must match the settled total",
        layout.name()
    );
}

fn wave_line(wave: usize, run: &Run) -> String {
    let mut requested = run
        .per_file_work
        .iter()
        .map(|work| work.requested)
        .collect::<Vec<_>>();
    requested.sort_unstable();
    let mut decoded = run
        .per_file_work
        .iter()
        .map(|work| work.decoded)
        .collect::<Vec<_>>();
    decoded.sort_unstable();
    format!(
        "wave{wave}_collect={:?} wave{wave}_planned={} \
         wave{wave}_admitted_opened={} wave{wave}_requested={} \
         wave{wave}_decoded={} wave{wave}_requested_per_file={requested:?} \
         wave{wave}_decoded_per_file={decoded:?}",
        run.collect,
        run.planned_partitions,
        run.files_read,
        run.bytes_data_requested,
        run.decoded_bytes,
    )
}

/// Hold #7500's pushed-predicate query, reader settings, plan and deterministic
/// file-group order fixed while changing only the admission growth multiplier.
/// Factor 4 is the proposed way to open a larger fraction of a four-partition
/// pruned scan; factor 2 is the shipped control.
async fn measure_waves(layout: Layout) -> WaveLayoutResult {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    build_fixture(&ice, layout).await;

    let wave2_ctx = context(2);
    let wave4_ctx = context(4);
    ice.register_with_datafusion(&wave2_ctx).await.unwrap();
    ice.register_with_datafusion(&wave4_ctx).await.unwrap();
    let sql = format!("SELECT timestamp, raw FROM events WHERE source = '{TARGET}' LIMIT {LIMIT}");

    // Warm planning metadata without reading data. Reader caches stay bypassed
    // in both arms, as in #7500.
    let _ = plan(&wave2_ctx, &sql, PredicatePushdownControl::RetainResidual).await;
    let _ = plan(&wave4_ctx, &sql, PredicatePushdownControl::RetainResidual).await;

    let mut result = WaveLayoutResult {
        wave2: Vec::new(),
        wave4: Vec::new(),
    };
    for pair in 0..PAIRS {
        let wave2_plan = plan(&wave2_ctx, &sql, PredicatePushdownControl::RetainResidual).await;
        let wave4_plan = plan(&wave4_ctx, &sql, PredicatePushdownControl::RetainResidual).await;
        siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());

        let wave2_scan = scan_nodes(&wave2_plan).pop().unwrap();
        let wave4_scan = scan_nodes(&wave4_plan).pop().unwrap();
        assert_eq!(
            wrapper_names(&wave2_plan),
            wrapper_names(&wave4_plan),
            "{}: wave factor must not change the physical wrappers",
            layout.name()
        );
        assert_eq!(
            planned_partitions(&wave2_scan),
            planned_partitions(&wave4_scan),
            "{}: wave factor must not change planned partitions",
            layout.name()
        );
        assert_eq!(
            wave2_scan
                .as_any()
                .downcast_ref::<SiglakeIcebergTableScan>()
                .unwrap()
                .file_attribution()
                .files,
            wave4_scan
                .as_any()
                .downcast_ref::<SiglakeIcebergTableScan>()
                .unwrap()
                .file_attribution()
                .files,
            "{}: both waves must retain the same planned file tasks",
            layout.name()
        );

        let (wave2, wave4) = if pair.is_multiple_of(2) {
            (
                run(&wave2_ctx, wave2_plan).await,
                run(&wave4_ctx, wave4_plan).await,
            )
        } else {
            let wave4 = run(&wave4_ctx, wave4_plan).await;
            let wave2 = run(&wave2_ctx, wave2_plan).await;
            (wave2, wave4)
        };
        assert_wave_run(&wave2, 2, layout);
        assert_wave_run(&wave4, 4, layout);
        if layout == Layout::RareClusteredSingleFile {
            assert!(!wave2
                .wrappers
                .iter()
                .any(|name| name == "ClippedLimitCoalesceExec"));
            assert_eq!(wave2.files_read, wave4.files_read);
            assert_eq!(wave2.bytes_data_requested, wave4.bytes_data_requested);
            assert_eq!(wave2.decoded_bytes, wave4.decoded_bytes);
        }

        eprintln!(
            "CLIPPED-WAVE layout={} pair={pair} order={} {} {}",
            layout.name(),
            pair % 2,
            wave_line(2, &wave2),
            wave_line(4, &wave4),
        );
        result.wave2.push(wave2);
        result.wave4.push(wave4);
    }
    result
}

fn report_waves(layout: Layout, result: &WaveLayoutResult) {
    eprintln!(
        "CLIPPED-WAVE-SUMMARY layout={} pairs={PAIRS} \
         wave2_median_ms={:.3} wave2_planned={} wave2_admitted_opened={} \
         wave2_requested={} wave2_decoded={} \
         wave4_median_ms={:.3} wave4_planned={} wave4_admitted_opened={} \
         wave4_requested={} wave4_decoded={} paired_wave4_minus_wave2_ms={:.3}",
        layout.name(),
        median_ms(&result.wave2),
        median_count(&result.wave2, |run| run.planned_partitions),
        median_count(&result.wave2, |run| run.files_read),
        median_count(&result.wave2, |run| run.bytes_data_requested),
        median_count(&result.wave2, |run| run.decoded_bytes),
        median_ms(&result.wave4),
        median_count(&result.wave4, |run| run.planned_partitions),
        median_count(&result.wave4, |run| run.files_read),
        median_count(&result.wave4, |run| run.bytes_data_requested),
        median_count(&result.wave4, |run| run.decoded_bytes),
        median_paired_ms(&result.wave4, &result.wave2),
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

/// #7513 by-hand measurement. The observed arm proves the signal's source and
/// incremental counting cost; the gate arm consumes a prior observation. A
/// missing observation takes the pushed baseline and is therefore not a
/// separate timed arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "matched local learned-gate attribution; run alone with --ignored --nocapture"]
async fn clipped_equality_learned_gate_attribution() {
    for layout in [
        Layout::Interleaved,
        Layout::RareScattered,
        Layout::RareClustered,
        Layout::RareClusteredSingleFile,
    ] {
        let result = measure_learned_gate(layout).await;
        report_gate(layout, &result);
    }
}

#[test]
fn learned_gate_requires_complete_high_survival_signal_without_selection() {
    assert_eq!(
        learned_gate_control(0, 0, 0),
        PredicatePushdownControl::RetainResidual,
        "an absent observation keeps current policy"
    );
    assert_eq!(
        learned_gate_control(40_000, 10_000, 0),
        PredicatePushdownControl::SuppressReaderPredicate,
        "the common interleaved fixture crosses the threshold"
    );
    assert_eq!(
        learned_gate_control(80_000, 160, 0),
        PredicatePushdownControl::RetainResidual,
        "the scattered rare fixture needs late materialization"
    );
    assert_eq!(
        learned_gate_control(20_480, 10_000, 1),
        PredicatePushdownControl::RetainResidual,
        "any verified page pruning keeps the reader predicate"
    );
}

/// #7525 correctness qualification for the real bounded observation cache.
/// Each case uses a distinct table UUID, so process-wide entries cannot bleed
/// between fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learned_cache_requires_all_earlier_pruning_signals_and_snapshot_identity() {
    // No early pruning: the cold observation publishes a suppressing entry.
    let common_tmp = tempfile::tempdir().unwrap();
    let common_ice = IcebergContext::open(common_tmp.path()).await.unwrap();
    let common = (0..8_000)
        .map(|row| {
            let source = if row % 4 == 0 {
                TARGET
            } else {
                OTHERS[row % 3]
            };
            learned_event(row, source, None)
        })
        .collect::<Vec<_>>();
    common_ice.append_events(&common).await.unwrap();
    let common_ctx = context(2);
    common_ice
        .register_with_datafusion(&common_ctx)
        .await
        .unwrap();
    let source_sql =
        format!("SELECT timestamp, raw FROM events WHERE source = '{TARGET}' LIMIT {LIMIT}");
    let cold = plan_with_tuning(&common_ctx, &source_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&cold, true);
    let cold_run = run(&common_ctx, cold).await;
    assert_eq!(cold_run.manifest_files_pruned_metrics, 0);
    assert_eq!(cold_run.row_groups_pruned_stats, 0);
    assert_eq!(cold_run.rows_pruned_selection, 0);
    let warm = plan_with_tuning(&common_ctx, &source_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&warm, false);
    let warm_run = run(&common_ctx, warm).await;
    assert_eq!(warm_run.rows.len(), LIMIT);
    assert!(warm_run
        .rows
        .iter()
        .all(|raw| raw.contains("source=us-east-2")));

    // A new snapshot is cold even when every other observation-key field is
    // unchanged.
    common_ice
        .append_events(
            &(8_000..8_100)
                .map(|row| learned_event(row, TARGET, None))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let next_snapshot_ctx = context(2);
    common_ice
        .register_with_datafusion(&next_snapshot_ctx)
        .await
        .unwrap();
    let next_snapshot =
        plan_with_tuning(&next_snapshot_ctx, &source_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&next_snapshot, true);

    // Manifest-only pruning: the surviving file is all matches, but the file
    // rejected before reader construction keeps the predicate on a repeat.
    let manifest_tmp = tempfile::tempdir().unwrap();
    let manifest_ice = IcebergContext::open(manifest_tmp.path()).await.unwrap();
    manifest_ice
        .append_events(
            &(0..4_000)
                .map(|row| learned_event(row, "us-east-1", None))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    manifest_ice
        .append_events(
            &(4_000..8_000)
                .map(|row| learned_event(row, TARGET, None))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let manifest_ctx = context(2);
    manifest_ice
        .register_with_datafusion(&manifest_ctx)
        .await
        .unwrap();
    let manifest_cold = plan_with_tuning(&manifest_ctx, &source_sql, learned_cache_tuning()).await;
    let manifest_run = run(&manifest_ctx, manifest_cold).await;
    assert!(manifest_run.manifest_files_pruned_metrics > 0);
    assert_eq!(manifest_run.row_groups_pruned_stats, 0);
    assert_eq!(manifest_run.rows_pruned_selection, 0);
    let manifest_warm = plan_with_tuning(&manifest_ctx, &source_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&manifest_warm, true);

    // Row-group-only pruning: force the writer's lower row-group bound, put
    // matches in the complete first group and misses in the second. The file
    // bounds span both values, and no page in the surviving group can prune.
    let row_group_tmp = tempfile::tempdir().unwrap();
    let row_group_ice = IcebergContext::open(row_group_tmp.path())
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            target_row_group_bytes: Some(1),
            ..Default::default()
        });
    let row_group_rows = 128 * 1024 + 4_096;
    row_group_ice
        .append_events(
            &(0..row_group_rows)
                .map(|row| {
                    let source = if row < 128 * 1024 {
                        TARGET
                    } else {
                        "us-east-1"
                    };
                    learned_event(row, source, None)
                })
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let row_group_ctx = context(2);
    row_group_ice
        .register_with_datafusion(&row_group_ctx)
        .await
        .unwrap();
    let row_group_cold =
        plan_with_tuning(&row_group_ctx, &source_sql, learned_cache_tuning()).await;
    let row_group_run = run(&row_group_ctx, row_group_cold).await;
    assert_eq!(row_group_run.manifest_files_pruned_metrics, 0);
    assert!(row_group_run.row_groups_pruned_stats > 0);
    assert_eq!(row_group_run.rows_pruned_selection, 0);
    let row_group_warm =
        plan_with_tuning(&row_group_ctx, &source_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&row_group_warm, true);

    // Nulls count as predicate input and never as accepted rows. A learned
    // suppressing hit still returns only exact non-null matches because the
    // DataFusion residual remains authoritative.
    let null_tmp = tempfile::tempdir().unwrap();
    let null_ice = IcebergContext::open(null_tmp.path()).await.unwrap();
    null_ice
        .append_events(
            &(0..8_000)
                .map(|row| learned_event(row, "fixture", row.is_multiple_of(4).then_some("kept")))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let null_ctx = context(2);
    null_ice.register_with_datafusion(&null_ctx).await.unwrap();
    let null_sql =
        format!("SELECT timestamp, raw FROM events WHERE attributes = 'kept' LIMIT {LIMIT}");
    let null_cold = plan_with_tuning(&null_ctx, &null_sql, learned_cache_tuning()).await;
    let null_cold_run = run(&null_ctx, null_cold).await;
    assert_eq!(null_cold_run.rows_pruned_selection, 0);
    let null_warm = plan_with_tuning(&null_ctx, &null_sql, learned_cache_tuning()).await;
    assert_reader_predicate(&null_warm, false);
    let null_warm_run = run(&null_ctx, null_warm).await;
    assert_eq!(null_warm_run.rows.len(), LIMIT);
    assert!(null_warm_run
        .rows
        .iter()
        .all(|raw| raw.contains("attributes=Some(\"kept\")")));

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
}

/// #7515 by-hand measurement. Factor 2 and factor 4 share each fixture and
/// differ only through `ClippedAdmissionWave`; collect time is evidence, not a
/// CI threshold. The single-file layout proves the factor is inert when
/// pruning removes the clipped-limit shape.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "matched local wave-factor attribution; run alone with --ignored --nocapture"]
async fn clipped_admission_wave_layout_attribution() {
    for layout in [
        Layout::Interleaved,
        Layout::RareScattered,
        Layout::RareClustered,
        Layout::RareClusteredSingleFile,
    ] {
        let result = measure_waves(layout).await;
        report_waves(layout, &result);
    }
}
