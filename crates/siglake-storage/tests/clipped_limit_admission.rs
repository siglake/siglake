//! #4865: an unordered, row-clipped `LIMIT n` must not start every partition.
//!
//! The shape the card measured on a fully compacted 15-file / 98.5M-row table:
//! one partition per file, no ordering to preserve, and a residual `FilterExec`
//! above the scan that DataFusion cannot push a limit through. Every partition
//! opens at once, every one lands its first batch within 2.0–3.4 ms of the
//! others, and by the time the global limit cancels them the scan has decoded
//! 419,840 rows to return 100. The same query over a layout with a small
//! unmerged tail decoded 597,696 bytes, because the tiny partition answered the
//! limit first and the whole-file partitions were cancelled having decoded
//! nothing.
//!
//! The fix is a staged start (`ScanAdmission`): scheduling only, so a partition
//! that waits still reads every row it would have read and the limit still
//! lives above the residual filter. These tests hold the LAYOUT fixed and vary
//! only the ramp, through the `ClippedAdmissionWave` session extension — `0` is
//! the pre-#4865 behavior — so the comparison is a matched A/B on identical
//! inputs rather than the benchmarks report's cross-build attribution.
//!
//! The controls are the ways a scheduling fix can silently lose rows: a common
//! match (the case the ramp is for), a match that only exists in the last file,
//! fewer matches than the limit asks for, and an `OFFSET`. Unordered SQL has no
//! defined row selection, so they check qualifying membership and cardinality,
//! never a particular set of rows.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::SiglakeIcebergTableScan;

const PARTITIONS: usize = 16;
const FILES: usize = 16;
const ROWS_PER_FILE: usize = 3_000;
/// Invisible to file statistics (`raw` spans the same range everywhere) and
/// present in every file, so no file is pruned and every partition would decode
/// if it were allowed to start.
const NEEDLE: &str = "queen";
const MATCHES_PER_FILE: usize = 64;
/// A late file, so the "too few matches" control cannot be answered by the head
/// of the ramp.
const FILES_LAST_MATCH: usize = FILES - 3;

/// These fixtures each build 16 files and six of them own four-worker runtimes.
/// Letting libtest run them together oversubscribes a four-core hosted runner
/// and turns the gated/ungated comparison into a measurement of its siblings.
static FIXTURE_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A row wide enough that a decoded batch is worth counting, and a `raw` whose
/// trigrams are the same in every file except for the needle.
fn payload(file: usize, row: usize, needle: bool) -> String {
    let body = if needle { NEEDLE } else { "pawn" };
    format!(
        "2026-09-17 service=checkout file={file:04} row={row:06} outcome={body} \
         detail=lorem ipsum dolor sit amet consectetur adipiscing elit sed do"
    )
}

/// `FILES` appends, each one data file. `needles(file)` says how many of that
/// file's rows carry the needle, placed at the head of the file.
async fn table_with(ice: &IcebergContext, needles: impl Fn(usize) -> usize) {
    table_with_files(ice, FILES, needles).await
}

/// `files` appends, each one data file — the scan plans one partition per data
/// file, so this is #7520's planned partition count. A table built here is the
/// same shape the pruned four-partition layout reaches by throwing files away;
/// the ramp reads the count, not how the count came about.
async fn table_with_files(ice: &IcebergContext, files: usize, needles: impl Fn(usize) -> usize) {
    for file in 0..files {
        let want = needles(file);
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|row| Event::now(payload(file, row, row < want)))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

/// A large-file version of [`table_with`] for the producer run-ahead
/// regression. Every row qualifies, so the complete residual predicate fills
/// `LIMIT 100` in the first source batch while the file still has many batches
/// available for a coalesce pump to decode speculatively.
async fn dense_table_with_rows(ice: &IcebergContext, rows_per_file: usize) {
    dense_table_with_files_and_rows(ice, FILES, rows_per_file).await
}

async fn dense_table_with_files_and_rows(ice: &IcebergContext, files: usize, rows_per_file: usize) {
    for file in 0..files {
        let events: Vec<Event> = (0..rows_per_file)
            .map(|row| Event::now(payload(file, row, true)))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

/// What the query server builds for `SELECT … WHERE <residual> LIMIT n` with no
/// `ORDER BY`: `ClippedScanLimit` and neither ordering extension. `wave` is the
/// admission ramp factor under test; `0` is the pre-#4865 scan.
fn clipped_context(limit: usize, wave: usize) -> SessionContext {
    clipped_context_with_group_order(limit, wave, None, false)
}

/// The same context with NO `ClippedAdmissionWave` extension, so the scan
/// resolves #7520's partition-sensitive default itself. This is the path the
/// query server takes; every other test here pins a factor to hold a matched
/// A/B, which by construction cannot see a default policy change.
///
/// `SIGLAKE_SCAN_CLIPPED_ADMISSION_WAVE` would also override it, but the tests
/// never set it — the resolver is pure and its override arm is covered by
/// `clipped_admission_wave_is_overridable_and_never_stalls` in
/// `query_provider.rs`.
fn clipped_context_default_wave(limit: usize) -> SessionContext {
    let base = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedScanLimit { limit }));
    SessionContext::new_with_state(state)
}

fn clipped_context_with_group_order(
    limit: usize,
    wave: usize,
    group_order: Option<siglake_storage::ClippedGroupOrder>,
    bypass_reader_caches: bool,
) -> SessionContext {
    let base = siglake_storage::session_context_with_target_partitions(Some(PARTITIONS));
    let mut state = base.state();
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedScanLimit { limit }));
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::ClippedAdmissionWave {
            factor: wave,
        }));
    if let Some(group_order) = group_order {
        state.config_mut().set_extension(Arc::new(group_order));
    }
    state
        .config_mut()
        .set_extension(Arc::new(siglake_storage::OrderedScanTuning {
            bypass_reader_caches,
            ..Default::default()
        }));
    SessionContext::new_with_state(state)
}

fn scan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if node
            .as_any()
            .downcast_ref::<SiglakeIcebergTableScan>()
            .is_some()
        {
            out.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    out
}

#[derive(Debug, PartialEq, Eq)]
struct NodeOutputRows {
    name: String,
    partitions: Vec<(usize, usize)>,
    evaluated_rows: Vec<(usize, usize)>,
}

impl NodeOutputRows {
    fn total(&self) -> usize {
        self.partitions.iter().map(|(_, rows)| rows).sum()
    }
}

/// Read `output_rows` without aggregating operators together. DataFusion keeps
/// one counter per output partition, so this also distinguishes a node's fetch
/// from work that ran ahead in another partition.
fn node_output_rows(plan: &Arc<dyn ExecutionPlan>) -> Vec<NodeOutputRows> {
    let mut out = Vec::new();
    plan.apply(|node| {
        let mut partitions = Vec::new();
        let mut evaluated_rows = Vec::new();
        if let Some(metrics) = node.metrics() {
            for metric in metrics.iter() {
                match metric.value() {
                    MetricValue::OutputRows(_) => partitions.push((
                        metric.partition().expect("output metric partition"),
                        metric.value().as_usize(),
                    )),
                    MetricValue::Ratio {
                        name,
                        ratio_metrics,
                    } if name == "selectivity" => evaluated_rows.push((
                        metric.partition().expect("selectivity metric partition"),
                        ratio_metrics.total(),
                    )),
                    _ => {}
                }
            }
        }
        out.push(NodeOutputRows {
            name: node.name().to_string(),
            partitions,
            evaluated_rows,
        });
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    out
}

/// The settled per-request scan attribution, summed across partitions, as the
/// query server's `summarize_plan_runtime` reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Work {
    partitions: usize,
    files_read: usize,
    files_pruned_bloom: usize,
    row_groups_read: usize,
    object_store_reads: usize,
    decoded_bytes: usize,
    /// NOT S3 traffic: the vendored reader's fetched-byte counter over a
    /// `file://` store. Reported because the card asks for it, compared only
    /// against the other arm of the same A/B.
    bytes_scanned: usize,
}

struct Run {
    rows: Vec<String>,
    work: Work,
    /// SQL-to-physical-plan wall time. The #7000 candidate performs no
    /// metadata or index probe, so this captures its CPU-only sorting cost.
    planning: Duration,
    /// Wall time to drain the root stream, i.e. what the caller waits for —
    /// measured BEFORE the settle, which waits on cancelled partitions the
    /// caller never sees. Reported because the card asks both arms' latency to
    /// be retained; nothing asserts on it. At this fixture's size a partition's
    /// whole read is one batch, so this is dominated by planning and is not a
    /// latency qualification of the fix. See the design doc's "Not measured
    /// here".
    drain: Duration,
}

/// Plan, drain and settle one arm, then read the scan node's counters. The
/// settle is what makes the numbers comparable: a `LIMIT` that closes the root
/// stream leaves the cancelled partitions folding on their own workers, and a
/// snapshot taken before they land reads a partial fold (#274).
async fn run_arm(ctx: &SessionContext, sql: &str, column: &str) -> Run {
    let planning_started = std::time::Instant::now();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let planning = planning_started.elapsed();
    run_planned_arm(ctx, plan, planning, column).await
}

/// Drain and settle an already-built plan. Supplying the plan lets a control
/// replace one physical node without changing any session option or replanning
/// against different state.
async fn run_planned_arm(
    ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    planning: Duration,
    column: &str,
) -> Run {
    let scans = scan_nodes(&plan);
    assert_eq!(scans.len(), 1, "one scan leaf expected in {plan:?}");
    let partitions = scans[0]
        .properties()
        .output_partitioning()
        .partition_count();

    let drain_started = std::time::Instant::now();
    let rows = {
        // Build a fresh execution context from the same session state that
        // produced this arm's plan.
        let task_ctx = siglake_storage::bounded_task_context_from(
            datafusion::execution::TaskContext::from(&ctx.state()),
        );
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), task_ctx).expect("execute the scan");
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            rows.extend(col.iter().map(|v| v.unwrap().to_string()));
        }
        rows
    };
    let drain = drain_started.elapsed();

    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(
        settle.complete && settle.live_partitions == 0,
        "arm must fully settle before it is read: {settle:?}"
    );

    let metrics = scans[0].metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    Run {
        rows,
        planning,
        drain,
        work: Work {
            partitions,
            files_read: sum("files_read"),
            files_pruned_bloom: sum("files_pruned_bloom"),
            row_groups_read: sum("row_groups_read"),
            object_store_reads: sum("object_store_reads"),
            decoded_bytes: sum("decoded_bytes"),
            bytes_scanned: sum("bytes_scanned"),
        },
    }
}

/// Test-local pre-#7339 control. The production optimizer first builds the
/// complete wave-2 plan, then this replaces only its clipped coalescer with
/// DataFusion's standard implementation while preserving the child and fetch.
fn replace_clipped_coalescer_with_standard(
    plan: Arc<dyn ExecutionPlan>,
) -> (Arc<dyn ExecutionPlan>, usize) {
    let mut replacements = 0;
    let old_children: Vec<_> = plan.children().into_iter().cloned().collect();
    let mut changed = false;
    let mut new_children = Vec::with_capacity(old_children.len());
    for child in old_children {
        let (replacement, child_replacements) =
            replace_clipped_coalescer_with_standard(child.clone());
        changed |= !Arc::ptr_eq(&child, &replacement);
        replacements += child_replacements;
        new_children.push(replacement);
    }
    let plan = if changed {
        plan.with_new_children(new_children).unwrap()
    } else {
        plan
    };

    if plan.name() != "ClippedLimitCoalesceExec" {
        return (plan, replacements);
    }
    let children = plan.children();
    assert_eq!(children.len(), 1, "clipped coalescer must have one child");
    let standard = CoalescePartitionsExec::new(children[0].clone()).with_fetch(plan.fetch());
    (Arc::new(standard), replacements + 1)
}

/// One bounded/control pair of the #7341 A/B: plan both arms from their own
/// session, replace the control's clipped coalescer, then drain them in the
/// order the caller picked. `check_shape` runs the one-time plan assertions,
/// which are the same for every pair.
async fn run_backpressure_pair(
    bounded_ctx: &SessionContext,
    control_ctx: &SessionContext,
    sql: &str,
    limit: usize,
    bounded_first: bool,
    check_shape: bool,
) -> (Run, Run) {
    let bounded_planning_started = std::time::Instant::now();
    let bounded_plan = bounded_ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let bounded_planning = bounded_planning_started.elapsed();
    let control_planning_started = std::time::Instant::now();
    let production_control_plan = control_ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let control_planning = control_planning_started.elapsed();
    let (control_plan, replacements) =
        replace_clipped_coalescer_with_standard(production_control_plan);
    if check_shape {
        assert_eq!(replacements, 1, "control must replace exactly one node");
        assert_eq!(bounded_plan.name(), "ClippedLimitCoalesceExec");
        assert_eq!(control_plan.name(), "CoalescePartitionsExec");
        assert_eq!(bounded_plan.fetch(), control_plan.fetch());
        assert_eq!(bounded_plan.fetch(), Some(limit));
        assert_eq!(
            datafusion::physical_plan::displayable(bounded_plan.children()[0].as_ref())
                .indent(false)
                .to_string(),
            datafusion::physical_plan::displayable(control_plan.children()[0].as_ref())
                .indent(false)
                .to_string(),
            "the arms must have identical physical plans below the coalescer"
        );
    }

    if bounded_first {
        let bounded = run_planned_arm(bounded_ctx, bounded_plan, bounded_planning, "raw").await;
        let control = run_planned_arm(control_ctx, control_plan, control_planning, "raw").await;
        (bounded, control)
    } else {
        let control = run_planned_arm(control_ctx, control_plan, control_planning, "raw").await;
        let bounded = run_planned_arm(bounded_ctx, bounded_plan, bounded_planning, "raw").await;
        (bounded, control)
    }
}

/// Scheduler-free #7000 arm: visit ticket-ordered groups one at a time until
/// the residual predicate has supplied the clip. This keeps the snapshot,
/// predicate and group membership fixed while isolating group ORDER from the
/// coalesce task race that an end-to-end LIMIT intentionally permits.
async fn run_sequential_groups(ctx: &SessionContext, sql: &str, limit: usize) -> Run {
    let planning_started = std::time::Instant::now();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let planning = planning_started.elapsed();
    let scan = scan_nodes(&plan).pop().expect("one scan leaf");
    let partitions = scan.properties().output_partitioning().partition_count();
    let task_ctx = siglake_storage::bounded_task_context_from(
        datafusion::execution::TaskContext::from(&ctx.state()),
    );

    let drain_started = std::time::Instant::now();
    let mut rows = Vec::new();
    for partition in 0..partitions {
        let source = drain_scan_partition(&scan, partition, task_ctx.clone()).await;
        rows.extend(
            source
                .into_iter()
                .filter(|row| row.contains(NEEDLE))
                .take(limit - rows.len()),
        );
        if rows.len() == limit {
            break;
        }
    }
    let drain = drain_started.elapsed();
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(
        settle.complete,
        "arm must settle before counters: {settle:?}"
    );

    let metrics = scan.metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|metric| metric.as_usize())
            .unwrap_or_default()
    };
    Run {
        rows,
        planning,
        drain,
        work: Work {
            partitions,
            files_read: sum("files_read"),
            files_pruned_bloom: sum("files_pruned_bloom"),
            row_groups_read: sum("row_groups_read"),
            object_store_reads: sum("object_store_reads"),
            decoded_bytes: sum("decoded_bytes"),
            bytes_scanned: sum("bytes_scanned"),
        },
    }
}

/// Which files the rows came from. `payload` stamps `file={file:04}` into
/// every row, so a drained stream names its own file set.
fn files_of(rows: &[String]) -> BTreeSet<usize> {
    rows.iter()
        .map(|row| {
            let at = row.find("file=").expect("every row carries its file");
            row[at + 5..at + 9]
                .parse()
                .expect("a four-digit file number")
        })
        .collect()
}

/// Plan the clipped query and hand back the scan leaf with its own task
/// context, for tests that drive partition streams directly instead of going
/// through the coalesce that normally spawns them.
async fn clipped_scan(
    ctx: &SessionContext,
    sql: &str,
) -> (
    Arc<dyn ExecutionPlan>,
    Arc<dyn ExecutionPlan>,
    Arc<datafusion::execution::TaskContext>,
    usize,
) {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scan = scan_nodes(&plan).pop().expect("one scan leaf");
    let partitions = scan.properties().output_partitioning().partition_count();
    // The session's own context, so the partition streams execute under the
    // same config the plan was built from (#2251).
    let task_ctx = siglake_storage::bounded_task_context_from(
        datafusion::execution::TaskContext::from(&ctx.state()),
    );
    (plan, scan, task_ctx, partitions)
}

/// Drain one scan partition to exhaustion; the rows are raw source rows, the
/// residual filter lives above the scan.
async fn drain_scan_partition(
    scan: &Arc<dyn ExecutionPlan>,
    partition: usize,
    task_ctx: Arc<datafusion::execution::TaskContext>,
) -> Vec<String> {
    let mut stream = scan.execute(partition, task_ctx).expect("execute");
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let col = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        rows.extend(col.iter().map(|v| v.unwrap().to_string()));
    }
    rows
}

fn assert_all_qualify(rows: &[String], sql: &str) {
    for row in rows {
        assert!(
            row.contains(NEEDLE),
            "a row that does not satisfy the residual filter came back from {sql}: {row}"
        );
    }
    let distinct: HashSet<&String> = rows.iter().collect();
    assert_eq!(distinct.len(), rows.len(), "duplicate rows from {sql}");
}

/// THE MEASUREMENT. One layout, two arms, the ramp the only difference.
///
/// The gated arm is scheduler-dependent: credits are paid per emitted batch and
/// per partition end, so a root stream that is slow to close widens the ramp
/// geometrically towards the fan-out it exists to prevent. On an idle lane the
/// gated arm decodes one or two row groups; under the full-workspace gate it
/// reached five (#5610: pair 0 gated 5 row groups, drain 13.3 ms against the
/// 3.3 ms of the pairs either side). The ungated arm, by contrast, is steady at
/// four row groups in every pair measured in either regime.
///
/// So the statistic has to tolerate a loaded box without being satisfied by
/// one. Two shapes were rejected before this one:
///
///   * the SUM over a fixed three pairs is what went red on the gate — one slow
///     gated pair carries the whole sum past the threshold;
///   * the FIRST pair to show the cut, taken over up to 32 pairs, is a minimum
///     over noisy draws. Under six concurrent copies of this binary at 24 test
///     threads on a 24-core box it passed 26 times in 36 with the ramp OFF on
///     both arms. It grades the luckiest pair, so it grades the scheduler.
///
/// The MEDIAN of each arm across a fixed odd number of pairs is neither. A pair
/// or two of gated-arm noise cannot move it, and with the ramp off both arms
/// decode the same four row groups, so the medians are equal and the assertion
/// fails. A/B'd at the shape the gate itself runs — one test binary at 24
/// threads, ambient load 32 — over 24 runs per arm: with the ramp on, every run
/// reported a gated median of 276,728 bytes against an ungated 553,456 and
/// passed; with `factor: 0` on both arms every run reported 553,456 against
/// 553,456 and failed. Pairs are still INTERLEAVED and the arm order still
/// alternates within them, because the arm that runs second in a pair finds the
/// footers cached and is handed the shorter window.
///
/// #6721's eight-batch credit pace also bounds the gated arm under contention:
/// this fixture's first partition is one source batch, so its terminal credit
/// can admit one successor but batch credits cannot race the ramp to full
/// fan-out. The matched A/B is therefore a regular regression test now; the
/// mechanism remains asserted separately without a scheduler in
/// `a_partition_behind_the_ramp_reads_nothing`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_admission_ramp_stops_every_partition_decoding_for_a_clipped_limit() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    // Odd, so the median is a pair that was actually measured.
    const PAIRS: usize = 7;
    const LIMIT: usize = 20;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let gated_ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&gated_ctx).await.unwrap();
    let ungated_ctx = clipped_context(LIMIT, 0);
    ice.register_with_datafusion(&ungated_ctx).await.unwrap();

    // Decoded bytes, not files read: a file counts as read the moment its
    // footer lands, so `files_read` measures how many partitions won a
    // scheduling race with the root stream closing (10/5/6 and 7/4/5 across three
    // otherwise identical ungated pairs on this box). Decoded bytes count the
    // batches that were actually built, which is the cost the card is about and
    // which the ramp bounds directly. With #6721's paced credit the gated arm
    // decodes one batch on this fixture; the old per-batch credit decoded two.
    let mut decoded: Vec<(usize, usize)> = Vec::new();
    let mut ungated_groups: Vec<usize> = Vec::new();
    let (mut gated_files, mut ungated_files) = (0usize, 0usize);
    for pair in 0..PAIRS {
        // The arm that runs SECOND in a pair finds the footers cached and
        // closes its root stream sooner, which is itself worth fan-out: with
        // the gated arm always first, a loaded box hands the ungated arm the
        // shorter window and the comparison tilts the wrong way. Alternating
        // the order pays that cost to both arms equally.
        let gated_first = pair % 2 == 0;
        let (gated, ungated) = if gated_first {
            let gated = run_arm(&gated_ctx, &sql, "raw").await;
            (gated, run_arm(&ungated_ctx, &sql, "raw").await)
        } else {
            let ungated = run_arm(&ungated_ctx, &sql, "raw").await;
            (run_arm(&gated_ctx, &sql, "raw").await, ungated)
        };
        eprintln!(
            "CLIPPED-ADMISSION pair={pair} gated_first={gated_first} gated={:?} drain={:?} \
             ungated={:?} drain={:?}",
            gated.work, gated.drain, ungated.work, ungated.drain
        );

        assert!(
            gated.work.partitions > 4,
            "the fixture must plan a wide scan for the ramp to hold anything back: {:?}",
            gated.work
        );
        assert_eq!(
            gated.work.partitions, ungated.work.partitions,
            "the arms must share a layout; only the ramp may differ"
        );
        // Both arms answer the query.
        assert_eq!(gated.rows.len(), LIMIT, "gated arm must fill the limit");
        assert_eq!(ungated.rows.len(), LIMIT, "ungated arm must fill the limit");
        assert_all_qualify(&gated.rows, &sql);
        assert_all_qualify(&ungated.rows, &sql);
        gated_files += gated.work.files_read;
        ungated_files += ungated.work.files_read;
        decoded.push((gated.work.decoded_bytes, ungated.work.decoded_bytes));
        ungated_groups.push(ungated.work.row_groups_read);
    }

    let median = |mut xs: Vec<usize>| {
        xs.sort_unstable();
        xs[xs.len() / 2]
    };
    let gated_median = median(decoded.iter().map(|(g, _)| *g).collect());
    let ungated_median = median(decoded.iter().map(|(_, u)| *u).collect());
    let ungated_group_median = median(ungated_groups.clone());
    eprintln!(
        "CLIPPED-ADMISSION pairs={PAIRS} median decoded_bytes gated={gated_median} \
         ungated={ungated_median} median ungated row_groups={ungated_group_median} \
         files gated={gated_files} ungated={ungated_files} per pair {decoded:?}"
    );
    // The ungated arm is the shape the card measured: more than one partition
    // decodes speculatively before the limit can cancel them. Checked, not
    // assumed — against the median too, because a single pair that answered out
    // of one partition compares the ramp against nothing. How FAR the fan-out
    // gets is bounded by how long the root stream takes to close rather than by
    // the partition count (at 32 partitions this fixture still only reached 6
    // files), so the floor is "more than one partition", not a fraction of the
    // plan.
    assert!(
        ungated_group_median > 1,
        "the ungated arm no longer reproduces the fan-out this test exists for: \
         row groups per pair {ungated_groups:?}"
    );
    assert!(
        gated_median * 3 <= ungated_median * 2,
        "the ramp must cut what an early-stopped clipped LIMIT decodes by at least a third at \
         the median of {PAIRS} pairs: gated {gated_median} ungated {ungated_median}, \
         decoded bytes (gated, ungated) {decoded:?} \
         (files gated {gated_files} ungated {ungated_files})"
    );
}

/// #7339/#7341: staging partition starts alone leaves avoidable work when an
/// admitted producer runs ahead through DataFusion's standard coalescer.
///
/// Both arms use fresh plans with the same 16-file snapshot, SQL, complete
/// residual predicate, wave-2 admission, fetch and reader-cache bypass. The
/// test-local control replaces exactly one production clipped coalescer with
/// DataFusion's standard coalescer. The production node combines a one-batch
/// shared channel with eager limit termination, so this attributes only their
/// combined effect. The result is unordered by contract, so exactness is
/// cardinality, predicate membership and distinct source rows.
///
/// #7485: a by-hand measurement, not a gate. All three counters it compares —
/// `files_read`, `object_store_reads` and `bytes_scanned` — move when a
/// partition wins a scheduling race with the root stream closing. A file counts
/// as read the moment its footer lands, and the early `LIMIT`'s own terminal
/// event pays the credit that admits the next reader (`finish` in
/// `crates/siglake-storage/src/query_provider.rs`, which widens the ramp for
/// liveness however a partition ended). Whether that reader opens its file
/// before teardown reaches it is a timing margin, so what these counters
/// separate is the margin, not the mechanism.
///
/// The margin is reliable on an idle lane and is gone on a loaded one. The
/// original single-pair form, which asserted the bounded arm read exactly one
/// file, went red on hosted CI with the bounded arm at two files against the
/// control's one (ci run 40332380261), and went red on the local gate with both
/// arms at two files (#7458 run 3230); one local draw under the whole target
/// put the bounded arm at four files against the control's three. #7458 then
/// graded it at the median of seven pairs, measuring every pair at bounded
/// 1 file / 4 reads / 1,687,906 bytes against control 2 / 7 / 3,349,138 on an
/// idle 32-core lane and with 24 spinners against it; that form still tied at
/// 2 files / 7 reads on the same lane at load 42, with the sign flipping
/// between pairs (bounded 1 against control 2 in one pair, 2 against 1 in
/// another). Run it on an idle lane when the #7341 repair is in question.
/// Taking the production coalescer out of BOTH arms leaves the medians equal
/// and fails the assertion, so the shape does grade the mechanism when the
/// margin survives.
///
/// #7487 closes admission when the fetch is filled, so the bounded arm now
/// reads one file because nothing can admit a second reader, not because
/// teardown won a race. The control arm still fans out by scheduling, so this
/// stays a by-hand measurement; `a_filled_clipped_limit_starts_no_further_reader`
/// is the gate on the bounded side.
///
/// `bounded_coalesce_fills_a_dense_residual_limit_exactly` keeps the part of
/// this shape that does not depend on the scheduler in the gate.
///
/// `decoded_bytes` is equal in both arms by assertion: this fixture answers
/// the limit from one source batch, so the repair moves footer fetches and
/// reader starts, not decoded work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scheduling-race measurement; run alone on an idle lane with --ignored --nocapture"]
async fn bounded_coalesce_and_eager_limit_bound_dense_residual_runahead() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 100;
    const DENSE_ROWS_PER_FILE: usize = 20_000;
    // Odd, so the median is a pair that was actually measured.
    const PAIRS: usize = 7;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    dense_table_with_rows(&ice, DENSE_ROWS_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let bounded_ctx = clipped_context_with_group_order(LIMIT, 2, None, true);
    ice.register_with_datafusion(&bounded_ctx).await.unwrap();
    let control_ctx = clipped_context_with_group_order(LIMIT, 2, None, true);
    ice.register_with_datafusion(&control_ctx).await.unwrap();

    let mut pairs: Vec<(Work, Work)> = Vec::new();
    for pair in 0..PAIRS {
        let bounded_first = pair % 2 == 0;
        let (bounded, control) = run_backpressure_pair(
            &bounded_ctx,
            &control_ctx,
            &sql,
            LIMIT,
            bounded_first,
            pair == 0,
        )
        .await;
        eprintln!(
            "CLIPPED-BACKPRESSURE-SAME-WAVE2 pair={pair} bounded_first={bounded_first} \
             bounded={:?} drain={:?} control={:?} drain={:?}",
            bounded.work, bounded.drain, control.work, control.drain
        );

        for (name, run) in [("bounded", &bounded), ("control", &control)] {
            assert_eq!(run.rows.len(), LIMIT, "{name} must fill the limit");
            assert_all_qualify(&run.rows, &sql);
            assert_eq!(
                run.rows.iter().collect::<HashSet<_>>().len(),
                LIMIT,
                "{name} must return distinct source rows"
            );
        }
        assert_eq!(bounded.work.partitions, control.work.partitions);
        assert_eq!(
            bounded.work.decoded_bytes, control.work.decoded_bytes,
            "the isolated fixture does not support a decoded-work reduction claim"
        );
        pairs.push((bounded.work, control.work));
    }

    let median = |mut xs: Vec<usize>| {
        xs.sort_unstable();
        xs[xs.len() / 2]
    };
    let medians = |f: fn(&Work) -> usize| {
        (
            median(pairs.iter().map(|(b, _)| f(b)).collect()),
            median(pairs.iter().map(|(_, c)| f(c)).collect()),
        )
    };
    let (bounded_files, control_files) = medians(|w| w.files_read);
    let (bounded_reads, control_reads) = medians(|w| w.object_store_reads);
    let (bounded_bytes, control_bytes) = medians(|w| w.bytes_scanned);
    eprintln!(
        "CLIPPED-BACKPRESSURE-SAME-WAVE2 pairs={PAIRS} median files_read bounded={bounded_files} \
         control={control_files} median object_store_reads bounded={bounded_reads} \
         control={control_reads} median bytes_scanned bounded={bounded_bytes} \
         control={control_bytes} per pair {pairs:?}"
    );
    assert!(
        control_files > 1,
        "the control no longer reproduces the avoidable reader start this test exists for: \
         per pair {pairs:?}"
    );
    assert!(
        bounded_files < control_files
            && bounded_reads < control_reads
            && bounded_bytes * 3 <= control_bytes * 2,
        "bounded buffering plus eager limit termination must reduce early physical reads at the \
         median of {PAIRS} pairs: bounded files {bounded_files} reads {bounded_reads} bytes \
         {bounded_bytes}, control files {control_files} reads {control_reads} bytes \
         {control_bytes}"
    );
}

/// The gated half of the measurement above: what the production clipped
/// coalescer must return on the dense residual shape, with no counter it does
/// not control. Every row qualifies and the first source batch carries far more
/// than `LIMIT`, so a one-batch shared channel plus eager limit termination has
/// to fill the limit from one batch without duplicating or dropping rows while
/// the partitions it cancelled are still tearing down. Unordered by contract,
/// so exactness is cardinality, predicate membership and distinct source rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_coalesce_fills_a_dense_residual_limit_exactly() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 100;
    const DENSE_ROWS_PER_FILE: usize = 20_000;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    dense_table_with_rows(&ice, DENSE_ROWS_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context_with_group_order(LIMIT, 2, None, true);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let planning_started = std::time::Instant::now();
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let planning = planning_started.elapsed();
    assert_eq!(plan.name(), "ClippedLimitCoalesceExec");
    assert_eq!(plan.fetch(), Some(LIMIT));

    let run = run_planned_arm(&ctx, plan, planning, "raw").await;
    assert_eq!(run.rows.len(), LIMIT, "the clipped limit must be filled");
    assert_all_qualify(&run.rows, &sql);
    assert_eq!(
        run.rows.iter().collect::<HashSet<_>>().len(),
        LIMIT,
        "the clipped limit must be filled from distinct source rows"
    );
}

/// #7487: a filled clipped `LIMIT` opens no further file.
///
/// The dense fixture fills `LIMIT 100` from the first source batch, so the
/// production coalescer closes admission as soon as that batch is in its
/// channel — before it repolls its input, and therefore before the terminal
/// credit that the repoll or the teardown pays. No queued partition can be
/// admitted after that, so the scan reads the one file the ramp opened
/// whatever the scheduler does.
///
/// What this gates is the outcome, not the mechanism: on an IDLE lane the old
/// behavior also read one file, because teardown beat the woken reader to its
/// footer (the same lane at load 42 read two files and seven ranges — the
/// measurement in `bounded_coalesce_and_eager_limit_bound_dense_residual_runahead`
/// and in the design doc). The interleaving itself is pinned by
/// `a_closed_ramp_pays_no_terminal_credit_and_starts_no_reader` in
/// `query_provider.rs`, which is hand-polled and does not depend on load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filled_clipped_limit_starts_no_further_reader() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 100;
    const DENSE_ROWS_PER_FILE: usize = 20_000;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    dense_table_with_rows(&ice, DENSE_ROWS_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context_with_group_order(LIMIT, 2, None, true);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let run = run_arm(&ctx, &sql, "raw").await;
    eprintln!("CLIPPED-CLOSE {:?} drain={:?}", run.work, run.drain);

    assert_eq!(run.rows.len(), LIMIT, "the clipped limit must be filled");
    assert_all_qualify(&run.rows, &sql);
    assert_eq!(
        run.rows.iter().collect::<HashSet<_>>().len(),
        LIMIT,
        "the clipped limit must be filled from distinct source rows"
    );
    assert_eq!(
        run.work.partitions, PARTITIONS,
        "the fixture must plan one partition per file for the close to hold any back"
    );
    assert_eq!(
        run.work.files_read, 1,
        "a filled fetch must leave every queued partition closed, not admitted: {:?}",
        run.work
    );
}

/// #7458: `sql execution profile` adds every plan node's `output_rows` before
/// comparing that sum with the scan leaf. Read the same clipped physical shape
/// one node and partition at a time so an 8,192-row repartition batch cannot be
/// mistaken for rows emitted by the filter above it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clipped_limit_output_metrics_separate_runahead_from_emitted_rows() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 100;
    const DENSE_ROWS_PER_FILE: usize = 20_000;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // Fifteen input file groups against sixteen target partitions reproduces
    // the candidate plan's RoundRobinBatch(16) node between scan and filter.
    for file in 0..15 {
        let events: Vec<Event> = (0..DENSE_ROWS_PER_FILE)
            .map(|row| Event::now(payload(file, row, true)))
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context_with_group_order(LIMIT, 2, None, true);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let expected_shape = [
        "ClippedLimitCoalesceExec",
        "FilterExec",
        "RepartitionExec",
        "SiglakeIcebergTableScan",
    ];
    let actual_shape: Vec<_> = node_output_rows(&plan)
        .into_iter()
        .map(|node| node.name)
        .collect();
    assert_eq!(actual_shape, expected_shape);

    let run = run_planned_arm(&ctx, plan.clone(), Duration::ZERO, "raw").await;
    assert_eq!(run.rows.len(), LIMIT);
    assert_all_qualify(&run.rows, &sql);

    let rows = node_output_rows(&plan);
    eprintln!("CLIPPED-LIMIT-OUTPUT-ROWS {rows:?} work={:?}", run.work);
    assert_eq!(
        rows[0].total(),
        LIMIT,
        "the final limit must emit its fetch"
    );
    assert!(
        rows[1]
            .partitions
            .iter()
            .all(|(_, output_rows)| *output_rows <= LIMIT),
        "FilterExec's per-partition fetch must be respected: {:?}",
        rows[1]
    );
    let contributing_filters: BTreeSet<_> = rows[1]
        .partitions
        .iter()
        .filter_map(|(partition, output_rows)| (*output_rows > 0).then_some(*partition))
        .collect();
    let contributing_repartitions: BTreeSet<_> = rows[2]
        .partitions
        .iter()
        .filter_map(|(partition, output_rows)| (*output_rows > 0).then_some(*partition))
        .collect();
    assert!(!contributing_filters.is_empty());
    assert_eq!(contributing_filters, contributing_repartitions);
    for partition in &contributing_filters {
        assert_eq!(
            rows[1]
                .partitions
                .iter()
                .find(|(candidate, _)| candidate == partition)
                .map(|(_, output_rows)| *output_rows),
            Some(LIMIT),
            "each contributing filter emits its fetch"
        );
        assert_eq!(
            rows[1]
                .evaluated_rows
                .iter()
                .find(|(candidate, _)| candidate == partition)
                .map(|(_, input_rows)| *input_rows),
            Some(8_192),
            "the filter evaluates one coalesced repartition batch"
        );
        assert_eq!(
            rows[2]
                .partitions
                .iter()
                .find(|(candidate, _)| candidate == partition)
                .map(|(_, output_rows)| *output_rows),
            Some(8_192),
            "the repartition emits one coalesced batch"
        );
    }

    let contributors = contributing_filters.len();
    let non_leaf_rows = rows[0].total() + rows[1].total() + rows[2].total();
    assert_eq!(non_leaf_rows, LIMIT + contributors * (8_192 + LIMIT));
    assert!(
        rows[3].total() >= rows[2].total(),
        "the settled scan cannot emit fewer rows than repartition reports downstream"
    );
}

/// #7000 local A/B. Group 0 under the shipped weight-first split is one of the
/// 12,000-row matching files. The 6,000-row non-matching file is deliberately
/// smaller and more compressible, so the metadata-only cost candidate moves it
/// to the head even though its whole-file trigram bloom then prunes it.
///
/// Reader caches are bypassed in both arms. The candidate performs no
/// metadata/index probe during planning; every object-store read below is
/// charged by the same execution counters. Sequential ticket draining removes
/// scheduler fan-out from the comparison.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn estimated_cost_order_measures_a_non_head_prunable_file() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    for file in 0..4 {
        let rows = if file == 0 { 6_000 } else { 12_000 };
        let events: Vec<Event> = (0..rows)
            .map(|row| {
                let raw = if file == 0 {
                    "file=0000 outcome=pawn fixed=fixed fixed=fixed fixed=fixed".to_string()
                } else {
                    format!(
                        "file={file:04} row={row:06} outcome={NEEDLE} unique-a={:016x} \
                         unique-b={:016x} unique-c={:016x}",
                        (row as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
                        (row as u64).wrapping_mul(0xd6e8_feb8_6659_fd93),
                        (row as u64).wrapping_mul(0xa076_1d64_78bd_642f),
                    )
                };
                Event::now(raw)
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let weight_ctx = clipped_context_with_group_order(
        LIMIT,
        2,
        Some(siglake_storage::ClippedGroupOrder::WeightFirst),
        true,
    );
    ice.register_with_datafusion(&weight_ctx).await.unwrap();
    let cost_ctx = clipped_context_with_group_order(
        LIMIT,
        2,
        Some(siglake_storage::ClippedGroupOrder::EstimatedBytesToLimit),
        true,
    );
    ice.register_with_datafusion(&cost_ctx).await.unwrap();

    // Warm the shared table/manifest planning caches for both timed arms. The
    // group-order candidate itself is CPU-only; cold catalog work must not be
    // attributed to whichever arm happens to plan first.
    for ctx in [&weight_ctx, &cost_ctx] {
        ctx.sql(&sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
    }
    let weight = run_sequential_groups(&weight_ctx, &sql, LIMIT).await;
    let cost = run_sequential_groups(&cost_ctx, &sql, LIMIT).await;
    eprintln!(
        "CLIPPED-GROUP-COST weight={:?} planning={:?} drain={:?} \
         cost={:?} planning={:?} drain={:?} planning_probe_reads=0",
        weight.work, weight.planning, weight.drain, cost.work, cost.planning, cost.drain
    );

    assert_eq!(weight.work.partitions, cost.work.partitions);
    assert_eq!(weight.rows.len(), LIMIT);
    assert_eq!(cost.rows.len(), LIMIT);
    assert_all_qualify(&weight.rows, &sql);
    assert_all_qualify(&cost.rows, &sql);
    assert_eq!(
        weight.work.files_pruned_bloom, 0,
        "the prunable file must not be group 0 under weight-first"
    );
    assert_eq!(
        cost.work.files_pruned_bloom, 1,
        "the cost candidate must move the cheap prunable file to group 0"
    );
    assert_eq!(
        cost.work.decoded_bytes, weight.work.decoded_bytes,
        "probing no metadata cannot reduce the readable group's decode"
    );
    assert!(
        cost.work.object_store_reads > weight.work.object_store_reads,
        "opening the prunable group must be charged: weight={:?} cost={:?}",
        weight.work,
        cost.work
    );
}

/// Sparse matches: the needle exists only in the LAST file, so the ramp has to
/// open every partition before the limit can be filled. This is the case a
/// staged start can break — and the one that says the ramp is scheduling and
/// not a source-row cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_needle_in_the_last_file_alone_still_fills_the_limit() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |file| {
        if file == FILES - 1 {
            MATCHES_PER_FILE
        } else {
            0
        }
    })
    .await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION sparse={:?}", run.work);
    assert_eq!(
        run.rows.len(),
        LIMIT,
        "a match reachable only through the whole ramp must still fill the limit"
    );
    // The rows ARE the proof that the ramp reached the last partition: nothing
    // else in the table qualifies. `files_read` says nothing here — the LIKE
    // trigram bloom prunes the needle-free files, so most partitions correctly
    // never open one.
    assert_all_qualify(&run.rows, &sql);
}

/// Fewer matches than the limit asks for: the scan cannot early-stop, so every
/// partition must run to exhaustion and every qualifying row must come back.
/// This is the liveness control — a ramp that failed to widen would hang here
/// or return short.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn too_few_matches_return_every_qualifying_row() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    // Seven matches in total, spread across three files, none of them the first.
    table_with(&ice, |file| match file {
        3 => 2,
        9 => 4,
        FILES_LAST_MATCH => 1,
        _ => 0,
    })
    .await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION insufficient={:?}", run.work);
    let expected: HashSet<String> = [(3usize, 2usize), (9, 4), (FILES_LAST_MATCH, 1)]
        .into_iter()
        .flat_map(|(file, want)| (0..want).map(move |row| payload(file, row, true)))
        .collect();
    let got: HashSet<String> = run.rows.iter().cloned().collect();
    assert_eq!(got, expected, "every qualifying row must come back");
    assert_eq!(run.rows.len(), 7);
}

/// `OFFSET` rides into the session folded into `ClippedScanLimit` (limit +
/// offset, `sql.rs::clipping_scan_limit`). The scan owes that many rows; the
/// page that comes back must still be `LIMIT` distinct qualifying rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offset_page_is_a_full_page_of_qualifying_rows() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 5;
    const OFFSET: usize = 10;
    let sql =
        format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT} OFFSET {OFFSET}");
    let ctx = clipped_context(LIMIT + OFFSET, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let run = run_arm(&ctx, &sql, "raw").await;

    eprintln!("CLIPPED-ADMISSION offset={:?}", run.work);
    assert_eq!(run.rows.len(), LIMIT, "the page must be full");
    assert_all_qualify(&run.rows, &sql);
}

/// THE MECHANISM, with the scheduler taken out of it. The end-to-end arms above
/// race a ramp against the root stream closing, so their margin is a
/// measurement. This one drives the partition streams by hand: a partition
/// behind the ramp is polled, stays `Pending` because nothing can widen it, and
/// — the claim the fix rests on — has fetched nothing at all, because the
/// reader below the gate was never polled.
#[tokio::test]
async fn a_partition_behind_the_ramp_reads_nothing() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    use futures::task::noop_waker_ref;
    use std::task::Context as TaskContext;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&format!(
            "SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}"
        ))
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let scan = scan_nodes(&plan).pop().expect("one scan leaf");
    let partitions = scan.properties().output_partitioning().partition_count();
    assert!(partitions > 4, "need a wide scan, got {partitions}");

    // The session's own context, so the partition streams execute under the
    // same config the plan was built from (#2251).
    let task_ctx = siglake_storage::bounded_task_context_from(
        datafusion::execution::TaskContext::from(&ctx.state()),
    );
    // Executed in order, so tickets follow partition index: partition 0 holds
    // ticket 0 and the rest are queued behind it.
    let mut streams: Vec<_> = (0..partitions)
        .map(|p| scan.execute(p, task_ctx.clone()).unwrap())
        .collect();

    let mut cx = TaskContext::from_waker(noop_waker_ref());
    let gated = partitions - 1;
    for poll in 0..8 {
        assert!(
            streams[gated].poll_next_unpin(&mut cx).is_pending(),
            "partition {gated} became ready on poll {poll} with nothing to widen the ramp"
        );
        tokio::task::yield_now().await;
    }

    // Dropping folds every partition's counters. Only the gated one was ever
    // polled, and it never reached the reader, so the whole plan read nothing.
    drop(streams);
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(settle.complete, "{settle:?}");
    let metrics = scan.metrics().unwrap().aggregate_by_name();
    let sum = |name: &str| {
        metrics
            .sum_by_name(name)
            .map(|m| m.as_usize())
            .unwrap_or_default()
    };
    assert_eq!(
        (
            sum("files_read"),
            sum("object_store_reads"),
            sum("bytes_scanned")
        ),
        (0, 0, 0),
        "a partition held by the ramp must not have started its read"
    );
}

/// An ORDER BY scan with no residual filter advertises its ordering, so
/// `preserve_task_order` keeps it out of the ramp entirely. Guard against a
/// future widening of the gate: a `SortPreservingMerge` polls every partition
/// for a first batch before it can emit anything, so gating one would deadlock
/// rather than slow down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordered_scan_is_outside_the_ramp() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let sql = format!("SELECT raw FROM events ORDER BY timestamp LIMIT {LIMIT}");
    let run = tokio::time::timeout(Duration::from_secs(60), run_arm(&ctx, &sql, "raw"))
        .await
        .expect("an ordered scan must not wait on the unordered ramp");
    eprintln!("CLIPPED-ADMISSION ordered={:?}", run.work);
    assert_eq!(run.rows.len(), LIMIT);
}

/// An aggregate drains every partition by construction. The query server never
/// sets `ClippedScanLimit` for one (`sql.rs::clipping_scan_limit` excludes
/// aggregates), but if it ever did the ramp must widen to completion rather
/// than stall — and the answer must still be exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_aggregate_under_the_hint_still_counts_every_row() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |file| if file % 2 == 0 { 3 } else { 0 }).await;

    let ctx = clipped_context(20, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let rows = tokio::time::timeout(
        Duration::from_secs(60),
        ctx.sql(&format!(
            "SELECT count(*) AS n FROM events WHERE raw LIKE '%{NEEDLE}%'"
        ))
        .await
        .unwrap()
        .collect(),
    )
    .await
    .expect("the ramp must widen to a full drain")
    .unwrap();
    let n = rows[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n as usize, FILES.div_ceil(2) * 3);
}

/// #6979: WHICH files the ramp opens first must not be a race between pumps.
///
/// `CoalescePartitionsExec::execute` calls the scan's `execute` inside the task
/// it spawns per partition, so the order tickets are taken in is the order the
/// scheduler happens to run those tasks. The test stands in for the scheduler
/// directly: each arm starts a different partition index FIRST on its own
/// freshly planned scan, which is exactly the freedom the runtime has, and the
/// file set that first stream reads has to come out the same every time.
///
/// The second half is the correctness side of the same change: with every
/// partition started in reverse order, the groups still partition the file set
/// — each file read by exactly one stream, every row accounted for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_group_opened_first_does_not_depend_on_which_partition_starts() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let (_, _, _, partitions) = clipped_scan(&ctx, &sql).await;
    assert!(partitions > 4, "need a wide scan, got {partitions}");

    let mut head: Option<BTreeSet<usize>> = None;
    for start in [0, partitions - 1, partitions / 2, 1] {
        // A fresh plan per arm, so each arm hands ticket 0 to a different
        // partition index.
        let (plan, scan, task_ctx, _) = clipped_scan(&ctx, &sql).await;
        let files = files_of(&drain_scan_partition(&scan, start, task_ctx).await);
        assert!(
            !files.is_empty(),
            "the first admitted stream read nothing starting at partition {start}"
        );
        eprintln!("CLIPPED-ADMISSION first-group start={start} files={files:?}");
        match &head {
            None => head = Some(files),
            Some(expected) => assert_eq!(
                &files, expected,
                "the first admitted stream read a different file set when partition {start} \
                 registered first"
            ),
        }
        let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
        assert!(settle.complete, "{settle:?}");
    }

    // Every partition, started in the most adverse order there is. Draining
    // them one at a time is enough to widen the ramp: a stream that ends pays
    // a credit, so the ticket behind it is admitted.
    let (plan, scan, task_ctx, _) = clipped_scan(&ctx, &sql).await;
    let mut covered: BTreeSet<usize> = BTreeSet::new();
    let mut rows_read = 0usize;
    for partition in (0..partitions).rev() {
        let rows = tokio::time::timeout(
            Duration::from_secs(60),
            drain_scan_partition(&scan, partition, task_ctx.clone()),
        )
        .await
        .unwrap_or_else(|_| panic!("partition {partition} never reached the front of the ramp"));
        rows_read += rows.len();
        for file in files_of(&rows) {
            assert!(
                covered.insert(file),
                "file {file} was read by two partitions"
            );
        }
    }
    assert_eq!(
        covered,
        (0..FILES).collect::<BTreeSet<_>>(),
        "ticket-ordered groups must still cover every file exactly once"
    );
    assert_eq!(rows_read, FILES * ROWS_PER_FILE, "every source row once");
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(30)).await;
    assert!(settle.complete, "{settle:?}");
}

/// The first group holds nothing the query wants: its files are pruned, it
/// ends having emitted no row, and the limit has to be filled from further up
/// the ramp. The target file is CHOSEN from the first group's own file set, so
/// the test keeps testing the exhausted-head case whatever the split is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_exhausted_first_group_still_fills_the_limit() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let probe_sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();

    let (probe_plan, scan, task_ctx, partitions) = clipped_scan(&ctx, &probe_sql).await;
    assert!(partitions > 4, "need a wide scan, got {partitions}");
    let head = files_of(&drain_scan_partition(&scan, 0, task_ctx).await);
    let settle =
        siglake_storage::settle_scan_partitions(&probe_plan, Duration::from_secs(30)).await;
    assert!(settle.complete, "{settle:?}");
    let target = (0..FILES)
        .find(|file| !head.contains(file))
        .expect("the first group cannot hold every file in a wide scan");
    eprintln!("CLIPPED-ADMISSION exhausted-head head={head:?} target={target}");

    let marker = format!("file={target:04}");
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{marker}%' LIMIT {LIMIT}");
    let run = tokio::time::timeout(Duration::from_secs(60), run_arm(&ctx, &sql, "raw"))
        .await
        .expect("an empty first group must not stall the ramp");
    eprintln!("CLIPPED-ADMISSION exhausted-head={:?}", run.work);
    assert_eq!(
        run.rows.len(),
        LIMIT,
        "the limit must fill from behind an empty first group"
    );
    for row in &run.rows {
        assert!(row.contains(&marker), "a row from the wrong file: {row}");
    }
}

/// Cancellation with streams still parked behind the ramp: the caller drops the
/// root stream after one batch, and every partition that began must still end.
/// A waiting stream owns a `begin` on the partition tracker, so one that was
/// dropped while parked without ending would leave the settle incomplete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_clipped_scan_ends_the_streams_behind_the_ramp() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with(&ice, |_| MATCHES_PER_FILE).await;

    const LIMIT: usize = 20;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    {
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), siglake_storage::bounded_task_context())
            .expect("execute the scan");
        let first = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .expect("the first batch must arrive before the cancel")
            .expect("a first batch")
            .unwrap();
        assert!(first.num_rows() > 0, "the fixture must match the needle");
        // The caller walks away mid-answer.
    }

    let settle = tokio::time::timeout(
        Duration::from_secs(90),
        siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)),
    )
    .await
    .expect("a cancelled clipped scan must settle, not hang");
    assert!(
        settle.complete,
        "a stream parked behind the ramp was dropped without ending: {settle:?}"
    );
}

/// Poll `stream` with a no-op waker: `true` if it never becomes ready.
///
/// Used only for the NEGATIVE case — a partition the ramp has not admitted.
/// An admitted partition is also pending on its first polls (it has a file to
/// open), so readiness here is proof of admission while pendency is proof of
/// admission only once something else has shown the credit landed. Each caller
/// below pays the credit and watches a nearer partition start before reading
/// this.
async fn stays_pending(stream: &mut datafusion::physical_plan::SendableRecordBatchStream) -> bool {
    use futures::task::noop_waker_ref;
    use std::task::Context as TaskContext;

    let mut cx = TaskContext::from_waker(noop_waker_ref());
    for _ in 0..8 {
        if stream.poll_next_unpin(&mut cx).is_ready() {
            return false;
        }
        tokio::task::yield_now().await;
    }
    true
}

/// Drain one already-executed partition stream to exhaustion, which pays the
/// ramp exactly one terminal credit. The fixture's files are one source batch
/// each, so the eight-batch pacing pays nothing on the way.
async fn drain_started_partition(
    stream: &mut datafusion::physical_plan::SendableRecordBatchStream,
) -> usize {
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    rows
}

/// #7520, the policy itself, on the session the query server builds: a scan
/// that planned two to four partitions still starts ONE reader, and its first
/// earned credit admits every remaining partition instead of doubling.
///
/// Deterministic because nothing but the drained partition can pay: the other
/// streams are executed (so tickets follow partition index) but unpolled, and
/// a partition behind the ramp reaches no reader. The four-partition arm is
/// the one #7515 measured — 12.037 ms of paired-median collect time at
/// unchanged bytes (`docs/CLIPPED_ADMISSION_WAVE_LAYOUTS_2026-10-26.md`); two
/// is unchanged from factor 2 and three follows the ramp's arithmetic, not a
/// measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_policy_fills_a_small_scan_at_its_first_credit() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 20;

    for files in [2usize, 3, 4] {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(tmp.path()).await.unwrap();
        table_with_files(&ice, files, |_| MATCHES_PER_FILE).await;

        let ctx = clipped_context_default_wave(LIMIT);
        ice.register_with_datafusion(&ctx).await.unwrap();
        let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
        let (plan, scan, task_ctx, partitions) = clipped_scan(&ctx, &sql).await;
        assert_eq!(
            partitions, files,
            "the fixture must plan one partition per file"
        );

        // Executed in order, so tickets follow partition index.
        let mut streams: Vec<_> = (0..partitions)
            .map(|p| scan.execute(p, task_ctx.clone()).unwrap())
            .collect();
        let last = partitions - 1;
        assert!(
            stays_pending(&mut streams[last]).await,
            "{files} partitions: the fill must happen at the first earned credit, not at scan start"
        );

        assert_eq!(
            drain_started_partition(&mut streams[0]).await,
            ROWS_PER_FILE,
            "{files} partitions: the admitted reader owns one whole file"
        );
        let batch = tokio::time::timeout(Duration::from_secs(60), streams[last].next())
            .await
            .unwrap_or_else(|_| {
                panic!("{files} partitions: the first credit must admit the last partition")
            })
            .expect("a batch")
            .unwrap();
        assert!(batch.num_rows() > 0);

        drop(streams);
        let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)).await;
        assert!(settle.complete, "{files} partitions: {settle:?}");
    }
}

/// The cap. Above four planned partitions the default is still factor 2, so
/// one credit admits the second partition and no more: #7515 measured that
/// widening the sixteen-partition rare-scattered layout doubled requested and
/// decoded bytes for a timing difference that went both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_policy_still_doubles_above_four_partitions() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 20;

    for files in [5usize, FILES] {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(tmp.path()).await.unwrap();
        table_with_files(&ice, files, |_| MATCHES_PER_FILE).await;

        let ctx = clipped_context_default_wave(LIMIT);
        ice.register_with_datafusion(&ctx).await.unwrap();
        let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
        let (plan, scan, task_ctx, partitions) = clipped_scan(&ctx, &sql).await;
        assert_eq!(partitions, files);

        let mut streams: Vec<_> = (0..partitions)
            .map(|p| scan.execute(p, task_ctx.clone()).unwrap())
            .collect();
        let last = partitions - 1;
        assert!(stays_pending(&mut streams[last]).await);

        assert_eq!(
            drain_started_partition(&mut streams[0]).await,
            ROWS_PER_FILE
        );
        // The credit landed and the ramp widened: partition 1 runs.
        tokio::time::timeout(Duration::from_secs(60), streams[1].next())
            .await
            .unwrap_or_else(|_| panic!("{files} partitions: one credit must admit partition 1"))
            .expect("a batch")
            .unwrap();
        assert!(
            stays_pending(&mut streams[last]).await,
            "{files} partitions: one credit must not open the whole scan"
        );

        drop(streams);
        let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)).await;
        assert!(settle.complete, "{files} partitions: {settle:?}");
    }
}

/// Override precedence, through a real plan rather than the resolver: an
/// explicit `ClippedAdmissionWave` is used as given at every planned width, so
/// a four-partition scan pinned to factor 2 still climbs `1 -> 2 -> 4` and a
/// scan pinned to `0` starts every partition at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_explicit_wave_overrides_the_partition_sensitive_default() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 20;
    const FOUR: usize = 4;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with_files(&ice, FOUR, |_| MATCHES_PER_FILE).await;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");

    let ctx = clipped_context(LIMIT, 2);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let (plan, scan, task_ctx, partitions) = clipped_scan(&ctx, &sql).await;
    assert_eq!(partitions, FOUR);
    let mut streams: Vec<_> = (0..partitions)
        .map(|p| scan.execute(p, task_ctx.clone()).unwrap())
        .collect();
    assert!(stays_pending(&mut streams[3]).await);
    assert_eq!(
        drain_started_partition(&mut streams[0]).await,
        ROWS_PER_FILE
    );
    tokio::time::timeout(Duration::from_secs(60), streams[1].next())
        .await
        .expect("factor 2 admits partition 1 on the first credit")
        .expect("a batch")
        .unwrap();
    assert!(
        stays_pending(&mut streams[3]).await,
        "an explicit factor 2 must keep the 1 -> 2 -> 4 ramp the default no longer uses"
    );
    drop(streams);
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)).await;
    assert!(settle.complete, "{settle:?}");

    // `0` still disables admission staging entirely.
    let ungated = clipped_context(LIMIT, 0);
    ice.register_with_datafusion(&ungated).await.unwrap();
    let (plan, scan, task_ctx, partitions) = clipped_scan(&ungated, &sql).await;
    assert_eq!(partitions, FOUR);
    let mut last = scan.execute(partitions - 1, task_ctx).unwrap();
    tokio::time::timeout(Duration::from_secs(60), last.next())
        .await
        .expect("an ungated scan starts every partition")
        .expect("a batch")
        .unwrap();
    drop(last);
    let settle = siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)).await;
    assert!(settle.complete, "{settle:?}");
}

/// Exactness under the default policy, across the widths the policy
/// distinguishes: one partition (never ramped), the filled two-to-four range,
/// and a width above the cap. Unordered SQL has no defined row selection, so
/// this checks qualifying membership and cardinality — a dense layout must
/// fill the clip, and a layout whose only matches live in the LAST file must
/// return every one of them, which it can only do by widening to that
/// partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_policy_answers_every_planned_width_exactly() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 20;
    const TAIL_MATCHES: usize = 7;
    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");

    for files in [1usize, 2, 3, 4, 5] {
        let dense = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(dense.path()).await.unwrap();
        table_with_files(&ice, files, |_| MATCHES_PER_FILE).await;
        let ctx = clipped_context_default_wave(LIMIT);
        ice.register_with_datafusion(&ctx).await.unwrap();
        let run = tokio::time::timeout(Duration::from_secs(120), run_arm(&ctx, &sql, "raw"))
            .await
            .unwrap_or_else(|_| panic!("{files} partitions: the dense arm must not stall"));
        assert_eq!(
            run.work.partitions, files,
            "the fixture must plan one partition per file"
        );
        assert_eq!(run.rows.len(), LIMIT, "{files} partitions: {:?}", run.work);
        assert_all_qualify(&run.rows, &sql);

        // Fewer matches than the clip asks for, and all of them in the last
        // file: the ramp has to reach the final partition to answer at all.
        let tail = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(tail.path()).await.unwrap();
        table_with_files(&ice, files, |file| {
            if file + 1 == files {
                TAIL_MATCHES
            } else {
                0
            }
        })
        .await;
        let ctx = clipped_context_default_wave(LIMIT);
        ice.register_with_datafusion(&ctx).await.unwrap();
        let run = tokio::time::timeout(Duration::from_secs(120), run_arm(&ctx, &sql, "raw"))
            .await
            .unwrap_or_else(|_| panic!("{files} partitions: the tail arm must not stall"));
        assert_eq!(
            run.rows.len(),
            TAIL_MATCHES,
            "{files} partitions: every qualifying row must come back: {:?}",
            run.work
        );
        assert_all_qualify(&run.rows, &sql);
        assert_eq!(
            files_of(&run.rows),
            BTreeSet::from([files - 1]),
            "{files} partitions: the matches live in the last file alone"
        );
    }
}

/// What a two-to-four-partition clipped plan actually looks like, and why the
/// fill is a scheduling change rather than a read-amount change.
///
/// A scan that planned fewer partitions than `target_partitions` gets a
/// `RepartitionExec: RoundRobinBatch(16), input_partitions=4` between it and
/// the residual filter, and that node drives EVERY input partition from its
/// own tasks. So the ends come fast, the ramp earns its credits whatever the
/// factor is, and both factors open all four files — which is what #7515
/// measured (identical requested and decoded bytes, 12.037 ms apart). #7487's
/// close still covers these partitions, but how many of them reach a reader
/// before the fetch fills is a race with those repartition tasks, so this
/// asserts the shape and the answer and leaves the counts to #7515's by-hand
/// comparison. The close's deterministic coverage is
/// `a_filled_clipped_limit_starts_no_further_reader` at full width and
/// `a_closed_ramp_pays_no_terminal_credit_and_starts_no_reader` in
/// `query_provider.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_clipped_scan_repartitions_above_the_ramp() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 100;
    const DENSE_ROWS_PER_FILE: usize = 20_000;
    const FOUR: usize = 4;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    dense_table_with_files_and_rows(&ice, FOUR, DENSE_ROWS_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context_default_wave(LIMIT);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let shape: Vec<String> = node_output_rows(&plan)
        .iter()
        .map(|node| node.name.clone())
        .collect();
    assert!(
        shape.iter().any(|name| name == "RepartitionExec"),
        "a sub-target-partition clipped scan is driven through a repartition: {shape:?}"
    );
    assert_eq!(
        shape.first().map(String::as_str),
        Some("ClippedLimitCoalesceExec"),
        "the clipped root must still own the fetch: {shape:?}"
    );

    let run = tokio::time::timeout(
        Duration::from_secs(120),
        run_planned_arm(&ctx, plan, Duration::ZERO, "raw"),
    )
    .await
    .expect("a filled clipped limit must not stall");
    eprintln!("CLIPPED-FILL-SHAPE {:?} drain={:?}", run.work, run.drain);
    assert_eq!(run.work.partitions, FOUR);
    assert_eq!(run.rows.len(), LIMIT);
    assert_all_qualify(&run.rows, &sql);
    assert_eq!(
        run.rows.iter().collect::<HashSet<_>>().len(),
        LIMIT,
        "the clipped limit must be filled from distinct source rows"
    );
}

/// Cancellation at the new width: a caller that walks away from a
/// four-partition default-policy scan must leave every stream ended, including
/// any the fill admitted in one step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_a_filled_small_scan_ends_every_stream() {
    let _fixture_guard = FIXTURE_SERIAL.lock().await;

    const LIMIT: usize = 20;
    const FOUR: usize = 4;

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path()).await.unwrap();
    table_with_files(&ice, FOUR, |_| MATCHES_PER_FILE).await;

    let sql = format!("SELECT raw FROM events WHERE raw LIKE '%{NEEDLE}%' LIMIT {LIMIT}");
    let ctx = clipped_context_default_wave(LIMIT);
    ice.register_with_datafusion(&ctx).await.unwrap();
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();

    {
        #[allow(clippy::disallowed_methods)]
        let mut stream = execute_stream(plan.clone(), siglake_storage::bounded_task_context())
            .expect("execute the scan");
        let first = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .expect("the first batch must arrive before the cancel")
            .expect("a first batch")
            .unwrap();
        assert!(first.num_rows() > 0, "the fixture must match the needle");
    }

    let settle = tokio::time::timeout(
        Duration::from_secs(90),
        siglake_storage::settle_scan_partitions(&plan, Duration::from_secs(60)),
    )
    .await
    .expect("a cancelled clipped scan must settle, not hang");
    assert!(
        settle.complete,
        "a stream parked behind the fill was dropped without ending: {settle:?}"
    );
}
