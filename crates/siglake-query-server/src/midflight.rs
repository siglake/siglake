//! Mid-flight breaker: collect a DataFusion stream while polling the
//! physical plan's leaf scans for `output_rows`. Abort with a
//! `breaker_tripped` error if the cumulative rows-scanned count
//! exceeds the configured limit.
//!
//! This complements (does not replace) the pre-flight bytes breaker
//! and the wall-clock timeout:
//!
//! - **Pre-flight** rejects queries up front based on the manifest
//!   cost walk. Fails closed if the estimator is wrong: an
//!   under-estimate slips through.
//! - **Mid-flight (this module)** catches the under-estimate case
//!   by watching real `output_rows` on the plan's leaves at every
//!   batch boundary.
//! - **Wall-clock** is the cross-format catch-all.
//!
//! v0 limitations: only the records path uses this — NDJSON
//! streaming uses `df.execute_stream()` directly today and would
//! need the same plan-construction refactor to plumb metrics. The
//! wall-clock timeout still protects NDJSON.

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};

#[derive(Debug, Default, Clone)]
pub struct PlanRuntimeStats {
    pub nodes: usize,
    pub leaf_output_rows: u64,
    pub elapsed_compute_nanos: u64,
    pub output_rows: u64,
    pub output_batches: u64,
    pub spill_count: u64,
    pub spilled_bytes: u64,
    pub bytes_scanned: u64,
    /// Per-request scan attribution (siglake scan node counters): what the
    /// plan assigned, what each pruning mechanism dropped, what the read
    /// actually cost. All zero when the plan has no siglake scan leaf.
    pub files_planned: u64,
    pub planned_bytes: u64,
    pub planned_rows: u64,
    pub files_read: u64,
    pub files_pruned_bloom: u64,
    pub row_groups_considered: u64,
    pub row_groups_pruned_bloom: u64,
    pub row_groups_pruned_stats: u64,
    pub row_groups_read: u64,
    pub rows_pruned_selection: u64,
    pub object_store_reads: u64,
    pub decoded_bytes: u64,
    /// F-5: fetched bytes by structure class (footer / index / column data /
    /// other), so a request can say WHY it read what it read.
    pub bytes_footer: u64,
    pub bytes_index: u64,
    pub bytes_data: u64,
    pub bytes_other: u64,
    /// Decoded file-batch cache outcomes: tasks served from cached Arrow
    /// batches (no reader built, so `files_read` and the byte counters do not
    /// move for them) and tasks that consulted the cache and read the file.
    pub file_cache_hits: u64,
    pub file_cache_misses: u64,
    /// #4890: tasks that consulted the cache and declined to populate it (they
    /// carry a predicate or a prune spec), and rows this request's populations
    /// were handed before they stopped.
    pub file_cache_bypasses: u64,
    pub file_cache_populate_rows: u64,
    /// Scan partition streams still live when this summary was taken. Their
    /// counters fold only when they finish, so a non-zero value means this
    /// summary is missing that many partitions' worth of scan attribution.
    /// The collect path settles the plan before summarising; this stays
    /// non-zero only when that wait hit its deadline.
    pub unsettled_partitions: u64,
    /// The scan-ordering gate's decision for the plan's siglake scan leaf
    /// ("advertised" or the refusal reason) — `None` when the plan has no
    /// siglake scan. Lets an individual breaker-tripped browse attribute
    /// WHICH path it executed (pod-level outcome counters can't).
    pub ordering_outcome: Option<&'static str>,
    /// Per-scan bounded file identities. The response layer merges these
    /// request-wide after all leaves have settled.
    pub file_attributions: Vec<siglake_storage::FileAttributionSnapshot>,
}
use futures::stream::StreamExt;

#[derive(Debug)]
pub enum CollectOutcome {
    Ok(Vec<RecordBatch>),
    /// `rows_scanned` exceeded `limit`. Partial batches are returned
    /// in case the caller wants to surface them.
    RowsScannedExceeded {
        rows_scanned: u64,
        limit: usize,
        partial: Vec<RecordBatch>,
    },
    /// The ACCUMULATED result crossed an [`AccumulationBound::Refuse`] bound at
    /// a batch boundary (#2184). No batches are returned: the only caller that
    /// asks for this bound refuses the whole request, and holding the partial
    /// result to hand back would be holding exactly the memory the bound
    /// exists to refuse.
    ///
    /// `rows` and `bytes` are what was accumulated when the bound was crossed,
    /// which is at most ONE produced batch past it — a stream that emits its
    /// whole result as a single batch is over the bound by that batch, and no
    /// bound read at a batch boundary can do better. They are NOT the size of
    /// the full result: everything past this point is never accumulated, which
    /// is the difference from a `collect()`-then-check.
    AccumulatedBoundExceeded {
        rows: usize,
        bytes: usize,
        max_rows: usize,
        max_bytes: usize,
        bound: AccumulatedBound,
    },
}

/// Which unit of an [`AccumulationBound::Refuse`] bound was crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulatedBound {
    Rows,
    Bytes,
}

/// What the collect loop does when the ACCUMULATED result reaches the caller's
/// bound. Distinct from `max_rows_scanned`, which bounds rows the plan READ.
#[derive(Debug, Clone, Copy)]
pub enum AccumulationBound {
    /// Accumulate the whole result. The rows-scanned breaker still applies.
    None,
    /// Stop accumulating at `max_rows` and answer `Ok` with what is held.
    ///
    /// For callers that then TRUNCATE to the same number — `batches_to_records(
    /// &batches, Some(max_rows))` does — so stopping early is not observable
    /// in the result, it just stops holding rows about to be discarded. The
    /// SQL paths' bound, and the reason this variant is not a refusal.
    StopAtRows(usize),
    /// Refuse the whole request if the result does not fit `max_rows` AND
    /// `max_bytes` of Arrow (`RecordBatch::get_array_memory_size`).
    ///
    /// The Jaeger read surface's bound (#2184): its `{data, total}` response
    /// cannot express a partial trace, so a truncated render there is a trace
    /// silently missing spans. `Ok` means the result is COMPLETE and inside
    /// both bounds; over either one it is
    /// [`CollectOutcome::AccumulatedBoundExceeded`] and nothing is returned.
    /// Exactly at a bound is a completion, not a refusal.
    Refuse { max_rows: usize, max_bytes: usize },
}

/// The execution context for a plan built from `df`: the DataFrame's OWN
/// session config and functions, on the shared bounded runtime.
///
/// Every `collect_plan_*` entry point takes one of these, and this is how a
/// caller that plans the DataFrame itself (because it wants the
/// `ExecutionPlan` afterwards for `stats.scan`) gets one. Take it BEFORE
/// `create_physical_plan()`, which consumes the DataFrame.
///
/// Until #2251 the collector executed with `siglake_storage::
/// bounded_task_context()`, i.e. a DEFAULT `SessionConfig`: a
/// `datafusion.execution.*` option set on the session that built the plan was
/// dropped at execution with no error and no log line. Measured in
/// `tests/jaeger_render_cost_measurement.rs::what_the_producer_hands_the_collector`
/// — a session `batch_size` of 1,024 produced byte-for-byte the 8,192-row
/// batches of the default — and regression-tested in
/// `tests/query_server/session_config_at_execution.rs`.
pub fn task_ctx_for(df: &DataFrame) -> Arc<TaskContext> {
    siglake_storage::bounded_task_context_from(df.task_ctx())
}

/// Collect `df` to record batches while polling the plan's leaf
/// `output_rows` count after each emitted batch. Returns
/// [`CollectOutcome::RowsScannedExceeded`] when the cumulative count
/// crosses `max_rows_scanned`.
pub async fn collect_with_rows_scanned_cap(
    df: DataFrame,
    max_rows_scanned: usize,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    let task_ctx = task_ctx_for(&df);
    let plan = df
        .create_physical_plan()
        .await
        .context("create physical plan")?;
    collect_plan_with_rows_scanned_cap(plan, task_ctx, max_rows_scanned, priority).await
}

/// Plan `df` and collect it under both bounds, through the exec pool.
///
/// The Jaeger read routes' entry point (#2184): they hold no `ExecutionPlan`
/// afterwards (no cost report, no `stats.scan` in their response shape), so
/// they hand the DataFrame over and read the outcome.
pub async fn collect_with_bounds(
    df: DataFrame,
    max_rows_scanned: usize,
    bound: AccumulationBound,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    let task_ctx = task_ctx_for(&df);
    let plan = df
        .create_physical_plan()
        .await
        .context("create physical plan")?;
    collect_plan_with_bounds_routed(plan, task_ctx, max_rows_scanned, bound, priority, false).await
}

/// Dedicated multi-thread runtime for plan EXECUTION. Scanning queries do
/// CPU-heavy decode inside async tasks; on the server's main runtime they
/// starve cheap requests of poll time — the 2026-07-15 load round measured
/// Tier-1 metadata answers jumping ~3ms→~12ms at just 8-way concurrency
/// from pure scheduling delay, and the plateau at ~113 QPS was the browses
/// monopolizing the accept runtime. Executing plans HERE keeps the
/// accept/fast-path runtime responsive under any scan load.
/// `SIGLAKE_QUERY_EXEC_THREADS`: worker count (default `max(2, cpus - 2)`;
/// `0` disables the offload and restores in-place execution).
fn exec_runtime() -> Option<&'static tokio::runtime::Runtime> {
    use std::sync::OnceLock;
    static RT: OnceLock<Option<tokio::runtime::Runtime>> = OnceLock::new();
    RT.get_or_init(|| {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let threads = std::env::var("SIGLAKE_QUERY_EXEC_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(|| cpus.saturating_sub(2).max(2));
        if threads == 0 {
            return None;
        }
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("siglake-exec")
            .enable_all()
            .build()
            .inspect(|_| tracing::info!(threads, "query exec runtime started"))
            .ok()
    })
    .as_ref()
}

pub async fn collect_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    match exec_runtime() {
        Some(rt) => rt
            .spawn(collect_plan_inner(plan, task_ctx))
            .await
            .context("exec runtime join")?,
        None => collect_plan_inner(plan, task_ctx).await,
    }
}

async fn collect_plan_inner(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let result = collect_plan_stream(plan.clone(), task_ctx).await;
    settle_scan_attribution(&plan).await;
    result
}

/// Drain `plan` to completion. The root stream is dropped on return, which is
/// what lets [`settle_scan_attribution`] run after it: the partition pumps
/// cannot finish while the stream that owns them is alive.
async fn collect_plan_stream(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let mut stream = execute_stream(plan.clone(), task_ctx).context("execute_stream")?;
    let mut batches = Vec::new();
    while let Some(b) = stream.next().await {
        let batch = b.context("stream item")?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Upper bound on the wait for a plan's scan partitions to finish after the
/// root stream is gone. The partitions an early `LIMIT` aborted unwind on their
/// own workers in a few scheduling hops; the bound only matters for a partition
/// stuck in a long synchronous poll, where holding the response indefinitely
/// would be worse than reporting the counters as incomplete.
pub const SCAN_SETTLE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

/// Wait for every siglake scan partition in `plan` to fold its counters, so the
/// `summarize_plan_runtime` snapshot the caller takes next is complete and no
/// request-attributable scan work lands after the response.
///
/// Why: the scan's per-partition counters (`files_read`, `row_groups_read`,
/// `object_store_reads`, fetched bytes) fold into the node metrics only when
/// the partition stream finishes or drops. When a `LIMIT` closes the root
/// stream, DataFusion aborts the pumps it spawned, but each abort takes effect
/// on that pump's next poll, on another worker. `render_records` used to
/// snapshot the plan as soon as collect returned, so an early-stopped
/// multi-partition scan could report leaf rows with zero reads while the reads
/// were still being folded (the 2026-09-03 50G `label_filter` round, task #274).
///
/// Must be called after the root stream has been dropped: the pumps drain from
/// it and cannot finish while it is alive.
async fn settle_scan_attribution(plan: &Arc<dyn ExecutionPlan>) {
    let settle = siglake_storage::settle_scan_partitions(plan, SCAN_SETTLE_DEADLINE).await;
    metrics::histogram!("siglake_query_scan_settle_seconds").record(settle.waited.as_secs_f64());
    if !settle.complete {
        metrics::counter!("siglake_query_scan_attribution_incomplete_total").increment(1);
        tracing::warn!(
            live_partitions = settle.live_partitions,
            waited_ms = settle.waited.as_secs_f64() * 1000.0,
            "scan partitions still unwinding at the settle deadline; this response's \
             stats.scan is missing their counters (unsettled_partitions)"
        );
    }
}

/// Occupancy of the query exec pool, released on Drop.
///
/// This exists because of a live failure on 2026-08-17: a query server that had
/// been up ~21h served Tier-1 aggregates in 44ms while EVERY pool-routed shape
/// timed out at 60s, even one whose planning pruned to a single file. The pool
/// gauge read 58 with the process idle, against 53 lifetime timeouts and 5
/// genuinely in-flight queries -- 53 + 5 = 58, exactly. Every timed-out query
/// had leaked its slot, permanently.
///
/// Two distinct defects produced that, and both are fixed here:
///
///  1. The decrement sat on the straight line AFTER `.await`. When the outer
///     future is dropped -- the 60s timeout, or a client hanging up -- that line
///     never runs. Accounting must live in Drop, which cancellation does run.
///  2. Worse, and the reason it was fatal rather than cosmetic: dropping a tokio
///     `JoinHandle` does NOT cancel the task. Each abandoned query kept scanning
///     on a pool of `max(2, cpus - 2)` threads (14 on that node), so once ~14
///     abandoned scans overlapped, the pool was dead and every subsequent
///     pool-routed query waited on a thread that would never free -- producing
///     more timeouts, and more abandoned scans. A death spiral, and it explains
///     why only a RESTART ever fixed it.
///
/// The inline lane never touches the pool, which is why aggregates kept working
/// throughout and made this look like a defect in filtered browses.
struct PoolSlot {
    abort: Option<tokio::task::AbortHandle>,
    in_pool: &'static std::sync::atomic::AtomicI64,
}

impl PoolSlot {
    fn acquire(abort: tokio::task::AbortHandle) -> Self {
        Self::acquire_counted(abort, &IN_POOL)
    }

    fn acquire_counted(
        abort: tokio::task::AbortHandle,
        in_pool: &'static std::sync::atomic::AtomicI64,
    ) -> Self {
        let depth = in_pool.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        metrics::gauge!("siglake_query_exec_pool_in_flight").set(depth as f64);
        Self {
            abort: Some(abort),
            in_pool,
        }
    }

    /// The join completed, so the task is finished: keep the slot accounting but
    /// do not report it as abandoned.
    fn disarm(&mut self) {
        self.abort = None;
    }
}

impl Drop for PoolSlot {
    fn drop(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort.abort();
            metrics::counter!("siglake_query_exec_pool_abandoned_total").increment(1);
        }
        let depth = self
            .in_pool
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
            - 1;
        metrics::gauge!("siglake_query_exec_pool_in_flight").set(depth as f64);
    }
}

static IN_POOL: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub async fn collect_plan_with_rows_scanned_cap(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    max_rows_scanned: usize,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    collect_plan_with_rows_scanned_cap_routed(plan, task_ctx, max_rows_scanned, priority, false)
        .await
}

/// Tier-1 priority lane: `inline=true` executes on the CALLING runtime,
/// skipping the exec pool entirely. For queries whose pre-flight estimate is
/// EXACT and small, pool routing is pure overhead in both directions — the
/// cheap query waits behind heavy browse decodes, and its (tiny) decode
/// occupies a pool slot a browse could use. The 07-18 mixed-load plateau
/// (~143 QPS with light shapes queueing behind ~100ms browses) is this
/// contention. Mis-estimates can't sneak in: only `cost.exact` plans below
/// the rows threshold ride the lane.
pub async fn collect_plan_with_rows_scanned_cap_routed(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    max_rows_scanned: usize,
    priority: crate::limits::Priority,
    inline: bool,
) -> Result<CollectOutcome> {
    collect_plan_with_bounds_routed(
        plan,
        task_ctx,
        max_rows_scanned,
        AccumulationBound::None,
        priority,
        inline,
    )
    .await
}

/// As [`collect_plan_with_rows_scanned_cap_routed`], with a bound on what the
/// collect ACCUMULATES as well as on what the plan scans.
///
/// Routed (not `_inner`) so a caller that reaches for the accumulation bound
/// also gets the exec pool's slot accounting and cancellation and the
/// scan-attribution settle. The Jaeger routes called `df.collect()` before
/// #2184 and had none of the three.
pub async fn collect_plan_with_bounds_routed(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    max_rows_scanned: usize,
    bound: AccumulationBound,
    priority: crate::limits::Priority,
    inline: bool,
) -> Result<CollectOutcome> {
    if inline {
        metrics::counter!("siglake_query_exec_route_total", "route" => "inline").increment(1);
        return collect_plan_with_caps_inner(plan, task_ctx, max_rows_scanned, bound, priority)
            .await;
    }
    metrics::counter!("siglake_query_exec_route_total", "route" => "pool").increment(1);
    match exec_runtime() {
        Some(rt) => {
            // Pool pressure attribution (1TB 32-way collapse diagnosis): how
            // many plans are in the pool right now, and how long this one
            // waited from spawn to first poll.
            let queued = std::time::Instant::now();
            let handle = rt.spawn(async move {
                metrics::histogram!("siglake_query_exec_pool_queue_seconds")
                    .record(queued.elapsed().as_secs_f64());
                collect_plan_with_caps_inner(plan, task_ctx, max_rows_scanned, bound, priority)
                    .await
            });
            // The slot is released -- and the spawned work CANCELLED -- by this
            // guard's Drop, so a query that goes away mid-flight cannot strand
            // either. See PoolSlot for why that is load-bearing.
            let mut slot = PoolSlot::acquire(handle.abort_handle());
            let result = handle.await.context("exec runtime join");
            slot.disarm();
            result?
        }
        None => {
            collect_plan_with_caps_inner(plan, task_ctx, max_rows_scanned, bound, priority).await
        }
    }
}

/// Collect with BOTH bounds: rows scanned (the mid-flight breaker) and rows
/// accumulated (peak memory).
///
/// The batch path had neither. `run_batch_query` called `df.collect()`, which
/// accumulates the whole result into an unaccounted `Vec<RecordBatch>` and only
/// then truncates in `batches_to_records`. With the default-order `LIMIT`
/// rewrite skipped for Batch and no rows-scanned cap passed through, a single
/// `SELECT raw FROM events` against a two-billion-row table collected until the
/// process died.
pub async fn collect_with_row_caps(
    df: DataFrame,
    max_rows_scanned: usize,
    max_rows_returned: usize,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    let task_ctx = task_ctx_for(&df);
    let plan = df
        .create_physical_plan()
        .await
        .context("create physical plan")?;
    collect_plan_with_caps_inner(
        plan,
        task_ctx,
        max_rows_scanned,
        AccumulationBound::StopAtRows(max_rows_returned),
        priority,
    )
    .await
}

/// As above, plus an optional bound on the rows ACCUMULATED.
///
/// The loop caps rows SCANNED but pushes every emitted batch into a `Vec` that
/// nothing bounds — and that `Vec` carries no memory reservation, so the
/// process-wide pool cannot see it. On the interactive path the injected
/// `ORDER BY ... LIMIT` keeps it small; the batch path skips that rewrite
/// deliberately, so nothing bounded it there at all.
///
/// [`AccumulationBound`] says what happens at the bound: nothing, stop-and-
/// truncate (the SQL paths), or refuse whole (the Jaeger routes, #2184).
async fn collect_plan_with_caps_inner(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    max_rows_scanned: usize,
    bound: AccumulationBound,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    let outcome =
        collect_plan_with_caps_stream(plan.clone(), task_ctx, max_rows_scanned, bound, priority)
            .await;
    // The root stream died with the callee's frame; every return path above
    // (limit satisfied, breaker tripped, error) leaves aborted partition pumps
    // behind, and every caller snapshots the plan's metrics next.
    settle_scan_attribution(&plan).await;
    outcome
}

async fn collect_plan_with_caps_stream(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
    max_rows_scanned: usize,
    bound: AccumulationBound,
    priority: crate::limits::Priority,
) -> Result<CollectOutcome> {
    let mut stream = execute_stream(plan.clone(), task_ctx).context("execute_stream")?;

    let mut batches = Vec::new();
    let mut output_rows_seen: usize = 0;
    let mut arrow_bytes_seen: usize = 0;
    while let Some(b) = stream.next().await {
        let batch = b.context("stream item")?;
        output_rows_seen += batch.num_rows();
        if matches!(bound, AccumulationBound::Refuse { .. }) {
            // Only the refusing bound reads bytes, so the SQL paths keep
            // paying nothing for a buffer walk they would not consult.
            arrow_bytes_seen += batch.get_array_memory_size();
        }
        batches.push(batch);

        match bound {
            AccumulationBound::None => {}
            // Stop accumulating once we hold every row the caller will render.
            // Everything past this point is collected, held, and then thrown
            // away.
            AccumulationBound::StopAtRows(cap) => {
                if output_rows_seen >= cap {
                    return Ok(CollectOutcome::Ok(batches));
                }
            }
            // Refuse instead, and only STRICTLY over the bound: exactly at it
            // the result may still be complete, and a complete result at the
            // ceiling is a 200. The stream's next batch (if there is one) is
            // what turns it into a refusal.
            AccumulationBound::Refuse {
                max_rows,
                max_bytes,
            } => {
                let crossed = if output_rows_seen > max_rows {
                    Some(AccumulatedBound::Rows)
                } else if arrow_bytes_seen > max_bytes {
                    Some(AccumulatedBound::Bytes)
                } else {
                    None
                };
                if let Some(bound) = crossed {
                    // Dropped HERE, before the caller is told: what the bound
                    // refuses is holding this much of a render at all.
                    drop(batches);
                    return Ok(CollectOutcome::AccumulatedBoundExceeded {
                        rows: output_rows_seen,
                        bytes: arrow_bytes_seen,
                        max_rows,
                        max_bytes,
                        bound,
                    });
                }
            }
        }

        // Prefer the plan's leaf `output_rows` metric (true "rows
        // scanned" for queries with aggregations). When that metric
        // is unpopulated (some IcebergStaticTableProvider paths
        // don't expose it through `partition_statistics`-equivalent
        // hooks), fall back to the pipeline's emitted-rows count.
        let leaf_rows = sum_leaf_output_rows(&plan);
        let scanned = leaf_rows.max(output_rows_seen as u64);
        if (scanned as usize) > max_rows_scanned {
            metrics::counter!("siglake_query_breaker_trips_total",
                "breaker" => "midflight_rows_scanned",
                "priority" => priority.label(),
            )
            .increment(1);
            return Ok(CollectOutcome::RowsScannedExceeded {
                rows_scanned: scanned,
                limit: max_rows_scanned,
                partial: batches,
            });
        }
    }
    Ok(CollectOutcome::Ok(batches))
}

pub fn summarize_plan_runtime(plan: &Arc<dyn ExecutionPlan>) -> PlanRuntimeStats {
    let mut out = PlanRuntimeStats::default();
    let _ = plan.apply(|node| {
        out.nodes += 1;
        if let Some(scan) = node
            .as_any()
            .downcast_ref::<siglake_storage::SiglakeIcebergTableScan>()
        {
            // Multiple scan leaves (e.g. a union) never disagree in practice —
            // they come from one gate decision per table per plan.
            out.ordering_outcome.get_or_insert(scan.ordering_outcome());
            // Read at the same instant as the counters below, so the summary
            // says how many partitions' folds it is missing, if any.
            out.unsettled_partitions += scan.live_partitions() as u64;
            out.file_attributions.push(scan.file_attribution());
        }
        if let Some(metrics) = node.metrics() {
            let aggregate = metrics.aggregate_by_name();
            out.elapsed_compute_nanos += aggregate.elapsed_compute().unwrap_or_default() as u64;
            out.output_rows += aggregate.output_rows().unwrap_or_default() as u64;
            out.output_batches += aggregate
                .sum_by_name("output_batches")
                .map(|metric| metric.as_usize() as u64)
                .unwrap_or_default();
            out.spill_count += aggregate.spill_count().unwrap_or_default() as u64;
            out.spilled_bytes += aggregate.spilled_bytes().unwrap_or_default() as u64;
            out.bytes_scanned += aggregate
                .sum_by_name("bytes_scanned")
                .map(|metric| metric.as_usize() as u64)
                .unwrap_or_default();
            let sum = |name: &str| {
                aggregate
                    .sum_by_name(name)
                    .map(|metric| metric.as_usize() as u64)
                    .unwrap_or_default()
            };
            out.files_planned += sum("files_scanned");
            out.planned_bytes += sum("planned_bytes");
            out.planned_rows += sum("planned_rows");
            out.files_read += sum("files_read");
            out.files_pruned_bloom += sum("files_pruned_bloom");
            out.row_groups_considered += sum("row_groups_considered");
            out.row_groups_pruned_bloom += sum("row_groups_pruned_bloom");
            out.row_groups_pruned_stats += sum("row_groups_pruned_stats");
            out.row_groups_read += sum("row_groups_read");
            out.rows_pruned_selection += sum("rows_pruned_selection");
            out.object_store_reads += sum("object_store_reads");
            out.decoded_bytes += sum("decoded_bytes");
            out.bytes_footer += sum("bytes_footer");
            out.bytes_index += sum("bytes_index");
            out.bytes_data += sum("bytes_data");
            out.bytes_other += sum("bytes_other");
            out.file_cache_hits += sum("file_cache_hits");
            out.file_cache_misses += sum("file_cache_misses");
            out.file_cache_bypasses += sum("file_cache_bypasses");
            out.file_cache_populate_rows += sum("file_cache_populate_rows");
            if node.children().is_empty() {
                out.leaf_output_rows += aggregate.output_rows().unwrap_or_default() as u64;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// Walk the physical-plan tree and sum the `output_rows` metric
/// across leaf nodes (TableScan / DataSourceExec / ParquetExec /
/// IcebergExec). Leaves are where rows enter the pipeline — summing
/// their output is the "rows scanned" total.
pub fn sum_leaf_output_rows(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut total: u64 = 0;
    let _ = plan.apply(|node| {
        if node.children().is_empty() {
            if let Some(metrics) = node.metrics() {
                if let Some(rows) = metrics.aggregate_by_name().output_rows() {
                    total += rows as u64;
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    total
}

/// The accumulation bound's boundary semantics (#2184).
///
/// These drive the collector over a `MemTable` of known batches, because that
/// is the only way to pin WHERE it stopped: a bound read at a batch boundary is
/// only worth anything if the batches past it are never produced, and the one
/// discriminating fact is the row count the refusal reports. A
/// `collect()`-then-check implementation reports the whole result;
/// [`the_refusing_bound_stops_accumulating_rather_than_collecting_it_all`]
/// fails against it.
#[cfg(test)]
mod accumulation_bound_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionConfig;
    use datafusion::prelude::SessionContext;

    fn batch_of(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let values = Int64Array::from((0..rows).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    /// One partition of `batches`, ONE target partition, so the stream the
    /// collector drains emits exactly these batches in this order — no
    /// repartition or coalesce merging them behind the bound.
    async fn collect(batches: Vec<RecordBatch>, bound: AccumulationBound) -> CollectOutcome {
        let schema = batches[0].schema();
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()),
        )
        .unwrap();
        let df = ctx.table("t").await.unwrap();
        collect_with_bounds(df, usize::MAX, bound, crate::limits::Priority::Interactive)
            .await
            .unwrap()
    }

    fn rows_of(outcome: &CollectOutcome) -> usize {
        match outcome {
            CollectOutcome::Ok(batches) => batches.iter().map(|b| b.num_rows()).sum(),
            other => panic!("expected a complete result, got {other:?}"),
        }
    }

    /// THE discriminating case. Ten thousand rows arrive as ten thousand
    /// batches; the bound is five. The refusal must report SIX rows — the five
    /// it was allowed plus the one batch that crossed the bound — because the
    /// other 9,994 were never produced, let alone held. Against a
    /// `df.collect()`-then-check implementation this reports 10,000 and fails,
    /// which is the assertion #2184's acceptance asks for.
    #[tokio::test]
    async fn the_refusing_bound_stops_accumulating_rather_than_collecting_it_all() {
        let outcome = collect(
            (0..10_000).map(|_| batch_of(1)).collect(),
            AccumulationBound::Refuse {
                max_rows: 5,
                max_bytes: usize::MAX,
            },
        )
        .await;
        let CollectOutcome::AccumulatedBoundExceeded { rows, bound, .. } = outcome else {
            panic!("the bound did not refuse: {outcome:?}");
        };
        assert_eq!(bound, AccumulatedBound::Rows);
        assert_eq!(
            rows, 6,
            "the collector accumulated past the bound: it stopped at {rows} rows, \
             not at the first batch over 5"
        );
    }

    /// cap-1, cap, cap+1 across many batches: exactly at the bound is a
    /// COMPLETE result, one over is a refusal. The `>=` the SQL bound uses
    /// would refuse the exact-boundary case, which on this surface would be a
    /// 413 for a result that fits.
    #[tokio::test]
    async fn the_row_bound_completes_at_the_boundary_and_refuses_one_over() {
        let ten = || (0..10).map(|_| batch_of(1)).collect::<Vec<_>>();
        for max_rows in [10, 11] {
            let outcome = collect(
                ten(),
                AccumulationBound::Refuse {
                    max_rows,
                    max_bytes: usize::MAX,
                },
            )
            .await;
            assert_eq!(rows_of(&outcome), 10, "max_rows {max_rows}");
        }
        let outcome = collect(
            ten(),
            AccumulationBound::Refuse {
                max_rows: 9,
                max_bytes: usize::MAX,
            },
        )
        .await;
        assert!(
            matches!(
                outcome,
                CollectOutcome::AccumulatedBoundExceeded {
                    rows: 10,
                    max_rows: 9,
                    bound: AccumulatedBound::Rows,
                    ..
                }
            ),
            "{outcome:?}"
        );
    }

    /// One batch bigger than the whole bound: the overshoot is that batch, and
    /// it is unavoidable — a bound read at a batch boundary cannot refuse a
    /// batch it has not been handed. Reported honestly (`rows` is 50, not 3).
    #[tokio::test]
    async fn an_oversized_single_batch_overshoots_by_exactly_that_batch() {
        let outcome = collect(
            vec![batch_of(50)],
            AccumulationBound::Refuse {
                max_rows: 3,
                max_bytes: usize::MAX,
            },
        )
        .await;
        assert!(
            matches!(
                outcome,
                CollectOutcome::AccumulatedBoundExceeded {
                    rows: 50,
                    bound: AccumulatedBound::Rows,
                    ..
                }
            ),
            "{outcome:?}"
        );
    }

    /// The byte bound is the one a row count cannot express, so it must bind on
    /// its own: same rows, same bound on rows, and the refusal comes from
    /// bytes. At exactly the accumulated size the result is complete.
    #[tokio::test]
    async fn the_byte_bound_binds_independently_of_the_row_bound() {
        let batches = (0..4).map(|_| batch_of(4)).collect::<Vec<_>>();
        let total: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        let outcome = collect(
            batches.clone(),
            AccumulationBound::Refuse {
                max_rows: usize::MAX,
                max_bytes: total,
            },
        )
        .await;
        assert_eq!(rows_of(&outcome), 16, "exactly at the byte bound");

        let outcome = collect(
            batches,
            AccumulationBound::Refuse {
                max_rows: usize::MAX,
                max_bytes: total - 1,
            },
        )
        .await;
        let CollectOutcome::AccumulatedBoundExceeded { bytes, bound, .. } = outcome else {
            panic!("the byte bound did not refuse: {outcome:?}");
        };
        assert_eq!(bound, AccumulatedBound::Bytes);
        assert_eq!(bytes, total, "the last batch is what crossed the bound");
    }

    /// The SQL callers' bound is UNCHANGED: it stops at the cap and answers
    /// `Ok` with the rows it holds, because they truncate to the same number.
    /// A refusal here would turn every over-cap `/api/v1/sql` browse into a
    /// 413 with no body.
    #[tokio::test]
    async fn the_sql_bound_still_stops_and_returns_what_it_holds() {
        let outcome = collect(
            (0..10).map(|_| batch_of(1)).collect(),
            AccumulationBound::StopAtRows(5),
        )
        .await;
        assert_eq!(
            rows_of(&outcome),
            5,
            "StopAtRows must stop AT the cap and return Ok"
        );
    }
}

#[cfg(test)]
mod pool_slot_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::Arc;

    /// Pool-slot tests use their own counter: unrelated query tests exercise the
    /// process-global `IN_POOL` concurrently and can change it across an await.
    static TEST_IN_POOL: AtomicI64 = AtomicI64::new(0);
    static SERIALIZE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_pool_in_flight() -> i64 {
        TEST_IN_POOL.load(Ordering::Relaxed)
    }

    fn test_slot(abort: tokio::task::AbortHandle) -> PoolSlot {
        PoolSlot::acquire_counted(abort, &TEST_IN_POOL)
    }

    /// The 2026-08-17 failure in miniature: a slot taken and then ABANDONED
    /// (the outer future dropped, as on a query timeout) must still be released.
    /// Before the Drop impl the decrement sat after `.await` and a cancelled
    /// query leaked its slot forever -- 53 timeouts, 53 leaked slots, a dead pool.
    #[tokio::test]
    async fn abandoning_a_slot_still_releases_it() {
        let _g = SERIALIZE.lock().await;
        let before = test_pool_in_flight();
        let h =
            tokio::spawn(async { tokio::time::sleep(std::time::Duration::from_secs(30)).await });
        {
            let _slot = test_slot(h.abort_handle());
            assert_eq!(test_pool_in_flight(), before + 1, "slot should be held");
        } // dropped WITHOUT disarm -- the cancellation path
        assert_eq!(
            test_pool_in_flight(),
            before,
            "an abandoned slot must be released by Drop"
        );
    }

    /// And abandoning must actually CANCEL the work. This is the half that made
    /// the leak fatal: dropping a JoinHandle leaves the task running, so each
    /// timed-out query kept scanning and starved the pool for everyone else.
    #[tokio::test]
    async fn abandoning_a_slot_cancels_the_spawned_work() {
        let _g = SERIALIZE.lock().await;
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let h = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            flag.store(true, Ordering::SeqCst);
        });
        {
            let _slot = test_slot(h.abort_handle());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            !ran.load(Ordering::SeqCst),
            "abandoned pool work must be aborted, not left running"
        );
    }

    /// The normal path: the join completed, so the task is already finished and
    /// must not be counted as abandoned -- but the slot is still released.
    #[tokio::test]
    async fn completing_a_slot_releases_it_without_aborting() {
        let _g = SERIALIZE.lock().await;
        let before = test_pool_in_flight();
        let ran = Arc::new(AtomicBool::new(false));
        let flag = ran.clone();
        let h = tokio::spawn(async move { flag.store(true, Ordering::SeqCst) });
        let mut slot = test_slot(h.abort_handle());
        h.await.unwrap();
        slot.disarm();
        drop(slot);
        assert_eq!(
            test_pool_in_flight(),
            before,
            "slot released on the happy path"
        );
        assert!(
            ran.load(Ordering::SeqCst),
            "completed work must not be aborted"
        );
    }
}
