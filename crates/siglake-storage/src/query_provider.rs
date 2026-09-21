use std::any::Any;
use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;

use std::task::{Context, Poll};
use std::time::Instant;

use chrono::{DateTime, Days, Months, TimeDelta, TimeZone, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, SchemaRef as ArrowSchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::common::stats::{Precision, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Like, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::scalar::ScalarValue;
use futures::future::{BoxFuture, FutureExt};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::{
    schema_to_arrow_schema, ArrowReaderBuilder, PromotedPruneSpec, RawPruneSpec, ScanCounters,
};
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{Datum, PrimitiveLiteral};
use iceberg::table::Table;
use iceberg::Result;
use siglake_bloom::Tokenizer;

use crate::index_manager::{loaded_table_index_config, text_field_tokenizers};

const DEFAULT_SCAN_DECODED_BUDGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_SCAN_DECOMPRESSION_FACTOR: u64 = 5;
const DEFAULT_ORDERED_DRAIN_BUFFER_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_ORDERED_MERGE_GLOBAL_FANIN: usize = 32;
const MIN_AGGREGATE_READER_BUDGET: usize = 2;
/// #4865: how fast the clipped-limit admission ramp opens. See
/// [`ScanAdmission`]; `2` doubles the admitted partition count on every batch
/// the scan emits and on every partition that ends, so a scan reaches full
/// fan-out in `log2(partitions)` steps and a `LIMIT` answered by the first
/// batch never opens more than two partitions.
const DEFAULT_SCAN_CLIPPED_ADMISSION_WAVE: usize = 2;

fn ordered_drain_buffer_bytes() -> u64 {
    ordered_drain_buffer_bytes_from(
        std::env::var("SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES")
            .ok()
            .as_deref(),
    )
}

/// Resolve the ordered-drain buffer budget from an explicit configured value.
///
/// `configured` is the raw `SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES` string, or
/// `None` when unset. Unparseable values fall back to the default. Pure: takes
/// the value as an argument rather than reading the environment so the
/// resolver can be tested without mutating process-global state, which a
/// parallel test in the same binary would observe.
fn ordered_drain_buffer_bytes_from(configured: Option<&str>) -> u64 {
    configured
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ORDERED_DRAIN_BUFFER_BYTES)
}

/// Resolve the ordered-merge global fan-in budget from the raw
/// `SIGLAKE_ORDERED_MERGE_GLOBAL_FANIN` string (`None` = unset). Unparseable
/// values fall back to the node-adaptive default. Pure for the same reason as
/// [`ordered_drain_buffer_bytes_from`].
fn ordered_merge_global_fanin_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or_else(|| adaptive_fanin_default(DEFAULT_ORDERED_MERGE_GLOBAL_FANIN, 8, 128))
}

/// The ordered-merge global fan-in budget for one scan: the session's
/// [`OrderedMergeGlobalBudget`] extension when set, else the environment, else
/// the node-adaptive default.
fn ordered_merge_global_fanin_for(state: &dyn Session) -> usize {
    match state.config().get_extension::<OrderedMergeGlobalBudget>() {
        Some(budget) => budget.fan_in,
        None => ordered_merge_global_fanin_from(
            std::env::var("SIGLAKE_ORDERED_MERGE_GLOBAL_FANIN")
                .ok()
                .as_deref(),
        ),
    }
}

/// Resolve the clipped-limit admission ramp factor from the raw
/// `SIGLAKE_SCAN_CLIPPED_ADMISSION_WAVE` string (`None` = unset). `0` turns the
/// ramp off (every partition starts at once, the pre-#4865 behavior, and the
/// negative-control arm of the local A/B); anything else is clamped to at least
/// 2, because a factor of 1 would never widen the ramp. Unparseable values fall
/// back to the default. Pure for the same reason as
/// [`ordered_drain_buffer_bytes_from`].
fn clipped_admission_wave_from(configured: Option<&str>) -> usize {
    match configured.and_then(|raw| raw.parse::<usize>().ok()) {
        Some(0) => 0,
        Some(factor) => factor.max(2),
        None => DEFAULT_SCAN_CLIPPED_ADMISSION_WAVE,
    }
}

/// The admission ramp factor for one scan: the session's
/// [`ClippedAdmissionWave`] extension when set, else the environment, else the
/// default.
fn clipped_admission_wave_for(state: &dyn Session) -> usize {
    match state.config().get_extension::<ClippedAdmissionWave>() {
        Some(wave) => wave.factor,
        None => clipped_admission_wave_from(
            std::env::var("SIGLAKE_SCAN_CLIPPED_ADMISSION_WAVE")
                .ok()
                .as_deref(),
        ),
    }
}

/// #90: size the ordered-merge fan-in caps to the node instead of the fixed
/// small-worker-safe constants. A freshly-drained layout legitimately sits at
/// overlap depth ~20+ until the depth trigger converges it to ≤12; on the
/// small-node defaults (16/32) the gate refuses ordered advertisement for
/// those minutes and browses fall to the TopK full-scan path (the 07-11/07-12
/// 413s). Merge streams buffer one batch each (projected columns, ~MBs), so a
/// 16-vCPU node comfortably carries 64/128. Env overrides always win; the
/// floor keeps small pods at today's behavior.
fn adaptive_fanin_default(floor: usize, per_cpu: usize, ceiling: usize) -> usize {
    let cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(2);
    cpus.saturating_mul(per_cpu).clamp(floor, ceiling)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct EffectiveReaderTuning {
    batch_size: Option<usize>,
    range_coalesce_bytes: Option<u64>,
    range_fetch_concurrency: Option<usize>,
    reversed_chunk_rows: Option<usize>,
    bypass_reader_caches: bool,
    ordered_drain_buffer_bytes: Option<u64>,
}

impl EffectiveReaderTuning {
    fn range_enabled(self) -> bool {
        self.range_coalesce_bytes.is_some() || self.range_fetch_concurrency.is_some()
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct EffectiveFileCacheTuning {
    max_bytes: Option<u64>,
    max_entries: Option<usize>,
    /// #4847 prototype gate; see [`crate::QueryScanTuning::file_cache_row_group_prototype`].
    row_group_prototype: bool,
    /// #4905 prototype gate; see
    /// [`crate::QueryScanTuning::file_cache_predicate_key_prototype`].
    predicate_key_prototype: bool,
}

/// Selectivity-aware ordered-policy override, injected through
/// `SessionConfig` by the query server for ordered-LIMIT browses whose
/// residual filter it measured as LOW-selectivity (group-count side
/// aggregates). See the `filtered_scan` classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedResidualHint {
    /// `true`: advertise ordering despite a residual filter (the query server
    /// measured it low-selectivity). `false`: force the refusal — set on the
    /// breaker-fallback retry (extensions can't be removed, only replaced).
    pub allow: bool,
}

/// Per-request cancellation flag, injected through `SessionConfig` and captured
/// onto the scan node at planning time.
///
/// WHY THIS EXISTS. Dropping the future that consumes a query cancels that
/// future -- but a DataFusion physical plan spawns its own partition pumps, and
/// tokio has no structured concurrency, so aborting a parent does not abort what
/// it spawned. Measured on a live 2,016,590,762-row peer on 2026-08-19: 35
/// minutes after 24 queries were cancelled, `perf` showed `SortExec`,
/// `ordered_task_stream` and `DataFrame::collect` still executing on a node
/// serving no requests. Repeating it on a converged layout wedged the host.
///
/// The pumps cannot be aborted from outside, but they all drain from the SCAN.
/// So the scan is where cancellation has to land: once this flag is set the
/// source stream ends, every pump above it sees end-of-input and unwinds, and
/// the work stops. Bounding the WORK rather than the future is the whole point.
///
/// Checked once per batch, which is a relaxed atomic load against a decode --
/// far too cheap to matter, and the batch boundary is the natural granularity
/// (a query that has produced no batch yet has not started scanning).
#[derive(Debug, Clone, Default)]
pub struct QueryCancel {
    flag: Arc<std::sync::atomic::AtomicBool>,
}

impl QueryCancel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop the scans belonging to this query. Idempotent.
    pub fn cancel(&self) {
        self.flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Cancels its `QueryCancel` on Drop, so the cancellation path -- which is the
/// one that leaked -- cannot be forgotten by a future author. Hold it for the
/// life of the request.
#[derive(Debug)]
pub struct CancelOnDrop(pub QueryCancel);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The query's explicit `LIMIT n` for a timestamp-ordered scan, injected
/// through `SessionConfig` (DataFusion holds ordered LIMITs at the sort and
/// never pushes them into the scan). A SMALL limit lets the ordering gate
/// coalesce the re-split to ONE partition: an early-stopping browse gains
/// nothing from scan parallelism, and every extra partition costs a
/// SortPreservingMerge first-poll (a first chunk decoded per partition —
/// the dominant term in the 07-17 browse cells).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedScanLimit {
    pub limit: usize,
}

/// The query's explicit `LIMIT n` where that limit clips THIS scan's output
/// row for row: an unordered, unaggregated select whose rows leave the plan as
/// they are produced. Injected through `SessionConfig` because DataFusion
/// cannot push a limit through the residual `FilterExec` that every text
/// predicate keeps above the scan, so the scan would otherwise never learn
/// that it only owes `n` rows.
///
/// Only consumer today: the text-index decline (see
/// [`text_index_decline_reason`]). It carries the value rather than a bare
/// flag so a later per-execution cost rule has the number to work with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClippedScanLimit {
    pub limit: usize,
}

/// Per-session override of the clipped-limit admission ramp factor (#4865).
/// `0` disables the ramp, so every partition starts decoding at once.
///
/// Injected through `SessionConfig` so a measurement can run the gated and
/// ungated arms in one process without `set_var`, which the binary's parallel
/// tests would observe. When absent the scan reads
/// `SIGLAKE_SCAN_CLIPPED_ADMISSION_WAVE` and falls back to
/// [`DEFAULT_SCAN_CLIPPED_ADMISSION_WAVE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClippedAdmissionWave {
    pub factor: usize,
}

/// Per-request preferred output direction for a `timestamp`-ordered scan.
/// Injected through `SessionConfig` so the scan can serve the opposite of the
/// declared on-disk direction when it can still prove correctness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreferredScanOrder {
    pub descending: bool,
}

/// Per-session override of the ordered-merge GLOBAL fan-in budget: the total
/// overlap streams one ordered scan may hold open across all its partitions
/// before it refuses to advertise ordering and falls back to a blocking sort.
/// Injected through `SessionConfig`; when absent the scan reads
/// `SIGLAKE_ORDERED_MERGE_GLOBAL_FANIN` and falls back to the node-adaptive
/// default.
///
/// Exists so a test can plan two scans under two budgets in one process
/// without `set_var`, which the binary's parallel tests would observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedMergeGlobalBudget {
    pub fan_in: usize,
}

/// Per-session overrides for ordered-scan planning. Absent fields preserve the
/// environment/default behavior; sessions can therefore exercise different
/// fan-in and eager-sort bounds safely in one process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderedScanTuning {
    pub merge_max_fan_in: Option<usize>,
    pub sort_cluster_max_rows: Option<u64>,
    pub reversed_chunk_rows: Option<usize>,
    pub bypass_reader_caches: bool,
    pub ordered_drain_buffer_bytes: Option<u64>,
}

impl EffectiveFileCacheTuning {
    fn enabled(self) -> bool {
        self.max_bytes.is_some() && self.max_entries.is_some()
    }

    fn entry_is_oversized(self, bytes: u64) -> bool {
        self.max_bytes.filter(|n| *n > 0).is_some_and(|max_bytes| {
            bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION) > max_bytes
        })
    }
}

#[derive(Clone)]
struct CachedFileBatches {
    bytes: u64,
    batches: Arc<Vec<RecordBatch>>,
}

/// An entry larger than `budget / MAX_FILE_CACHE_ENTRY_FRACTION` is not cached in
/// the decoded-batch cache (it would thrash). The byte-range object cache serves
/// those reads instead.
pub(crate) const MAX_FILE_CACHE_ENTRY_FRACTION: u64 = 4;

#[derive(Default)]
struct QueryFileBatchCache {
    bytes: u64,
    order: VecDeque<String>,
    entries: HashMap<String, CachedFileBatches>,
}

impl QueryFileBatchCache {
    fn get(&self, key: &str) -> Option<CachedFileBatches> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: String, entry: CachedFileBatches, tuning: EffectiveFileCacheTuning) {
        // Don't let one oversized entry (e.g. a whole file's decoded `raw` column)
        // monopolize and thrash the cache — it would evict everything useful, then
        // sit alone. Skip caching entries larger than a quarter of the byte budget;
        // the byte-range object cache still serves those reads efficiently (WI-12).
        if tuning.entry_is_oversized(entry.bytes) {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "skip_oversized"
            )
            .increment(1);
            return;
        }
        if let Some(prev) = self.entries.insert(key.clone(), entry.clone()) {
            self.bytes = self.bytes.saturating_sub(prev.bytes);
        }
        self.bytes = self.bytes.saturating_add(entry.bytes);
        self.order.push_back(key);

        let max_entries = tuning.max_entries.unwrap_or(0);
        let max_bytes = tuning.max_bytes.unwrap_or(0);
        while (!self.entries.is_empty())
            && ((max_entries > 0 && self.entries.len() > max_entries)
                || (max_bytes > 0 && self.bytes > max_bytes))
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            let should_remove = self.entries.contains_key(&oldest)
                && self
                    .order
                    .iter()
                    .all(|candidate| candidate.as_str() != oldest.as_str());
            if should_remove {
                if let Some(evicted) = self.entries.remove(&oldest) {
                    self.bytes = self.bytes.saturating_sub(evicted.bytes);
                    metrics::counter!(
                        "siglake_query_scan_file_cache_requests_total",
                        "outcome" => "evict"
                    )
                    .increment(1);
                }
            }
        }
        metrics::gauge!("siglake_query_scan_file_cache_bytes").set(self.bytes as f64);
        metrics::gauge!("siglake_query_scan_file_cache_entries").set(self.entries.len() as f64);
    }
}

fn query_file_batch_cache() -> &'static std::sync::Mutex<QueryFileBatchCache> {
    static CACHE: OnceLock<std::sync::Mutex<QueryFileBatchCache>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(QueryFileBatchCache::default()))
}

/// Drop every decoded-cache entry. Measurement hook: the cache is process-wide,
/// so an arm that must start cold needs this between arms.
pub fn clear_decoded_file_cache() {
    let mut cache = query_file_batch_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *cache = QueryFileBatchCache::default();
    metrics::gauge!("siglake_query_scan_file_cache_bytes").set(0.0);
    metrics::gauge!("siglake_query_scan_file_cache_entries").set(0.0);
}

/// What the installed decoded cache costs, in three currencies.
///
/// `priced_bytes` is what the cache charges against its own budget
/// (`get_array_memory_size`, whole buffer allocations, summed per batch and
/// therefore double-counting an allocation two batches share). `extent_bytes`
/// prices the rows the entries actually span (`get_slice_memory_size`).
/// `retained_bytes` sums the DISTINCT backing allocations the entries keep
/// alive, deduplicated across the whole cache by allocation base pointer — the
/// number that says what the process cannot give back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecodedFileCacheFootprint {
    pub entries: usize,
    pub batches: usize,
    pub rows: usize,
    pub priced_bytes: u64,
    pub extent_bytes: u64,
    pub retained_bytes: u64,
}

/// [`DecodedFileCacheFootprint`] for the installed cache. Walks every cached
/// batch, so it is a measurement hook, not something to call per request.
pub fn decoded_file_cache_footprint() -> DecodedFileCacheFootprint {
    let cache = query_file_batch_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut footprint = DecodedFileCacheFootprint {
        entries: cache.entries.len(),
        priced_bytes: cache.bytes,
        ..Default::default()
    };
    let mut allocations: Vec<(usize, usize)> = Vec::new();
    for entry in cache.entries.values() {
        for batch in entry.batches.iter() {
            footprint.batches += 1;
            footprint.rows += batch.num_rows();
            footprint.extent_bytes = footprint
                .extent_bytes
                .saturating_add(batch_extent_bytes(batch));
            for column in batch.columns() {
                collect_backing_allocations(&column.to_data(), &mut allocations);
            }
        }
    }
    footprint.retained_bytes = allocations
        .iter()
        .map(|(_, capacity)| *capacity as u64)
        .sum();
    footprint
}

/// Bytes a batch's rows span, by the measure the write paths use for a slice
/// (`ArrayData::get_slice_memory_size`) rather than the whole-buffer measure.
fn batch_extent_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| column.to_data().get_slice_memory_size().unwrap_or(0) as u64)
        .sum()
}

/// Distinct backing allocations one batch keeps alive, by allocation base
/// pointer and capacity, so two columns sharing a buffer (or a sliced view of
/// one) are counted once.
fn batch_retained_bytes(batch: &RecordBatch) -> u64 {
    let mut allocations: Vec<(usize, usize)> = Vec::new();
    for column in batch.columns() {
        collect_backing_allocations(&column.to_data(), &mut allocations);
    }
    allocations
        .iter()
        .map(|(_, capacity)| *capacity as u64)
        .sum()
}

/// Accumulate `(allocation base pointer, capacity)` for every buffer under
/// `data`, deduplicated. `Buffer::data_ptr` is the allocation, not the slice
/// start, so two slices of one allocation collapse to one entry; `capacity` is
/// the allocation's, which is why the pair identifies it.
fn collect_backing_allocations(
    data: &datafusion::arrow::array::ArrayData,
    out: &mut Vec<(usize, usize)>,
) {
    let push = |buffer: &datafusion::arrow::buffer::Buffer, out: &mut Vec<(usize, usize)>| {
        let entry = (buffer.data_ptr().as_ptr() as usize, buffer.capacity());
        if !out.contains(&entry) {
            out.push(entry);
        }
    };
    for buffer in data.buffers() {
        push(buffer, out);
    }
    if let Some(nulls) = data.nulls() {
        push(nulls.inner().inner(), out);
    }
    for child in data.child_data() {
        collect_backing_allocations(child, out);
    }
}

/// Off-pool bytes the populate streams hold, now and at their peak.
///
/// #4494 recorded the population memory risk as a bound — "up to budget/4 per
/// stream, concurrently per partition" — with nothing measuring it: the
/// `siglake_query_scan_file_cache_bytes` gauge is written from the INSERT, so a
/// population that never inserts is invisible in it, which is every population
/// on a clipped browse. These counters are charged as batches are buffered and
/// released when the stream inserts or drops, so a run can read the aggregate
/// across concurrent partitions and its peak. Both currencies of
/// [`DecodedFileCacheFootprint`] are tracked: the rows' extent and the distinct
/// backing allocations (deduplicated per batch — sharing ACROSS batches is not,
/// so retained is an upper bound there).
#[derive(Debug, Default)]
struct PopulationMeter {
    extent_bytes: std::sync::atomic::AtomicU64,
    retained_bytes: std::sync::atomic::AtomicU64,
    streams: std::sync::atomic::AtomicU64,
    peak_extent_bytes: std::sync::atomic::AtomicU64,
    peak_retained_bytes: std::sync::atomic::AtomicU64,
    peak_streams: std::sync::atomic::AtomicU64,
}

fn population_meter() -> &'static PopulationMeter {
    static METER: OnceLock<PopulationMeter> = OnceLock::new();
    METER.get_or_init(PopulationMeter::default)
}

/// Aggregate population memory across every live populate stream in the
/// process, and the peak since the last
/// [`reset_decoded_file_cache_population_peaks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecodedFileCachePopulationStats {
    pub inflight_extent_bytes: u64,
    pub inflight_retained_bytes: u64,
    pub inflight_streams: u64,
    pub peak_extent_bytes: u64,
    pub peak_retained_bytes: u64,
    pub peak_streams: u64,
}

pub fn decoded_file_cache_population_stats() -> DecodedFileCachePopulationStats {
    use std::sync::atomic::Ordering::Relaxed;
    let meter = population_meter();
    DecodedFileCachePopulationStats {
        inflight_extent_bytes: meter.extent_bytes.load(Relaxed),
        inflight_retained_bytes: meter.retained_bytes.load(Relaxed),
        inflight_streams: meter.streams.load(Relaxed),
        peak_extent_bytes: meter.peak_extent_bytes.load(Relaxed),
        peak_retained_bytes: meter.peak_retained_bytes.load(Relaxed),
        peak_streams: meter.peak_streams.load(Relaxed),
    }
}

/// Reset the peaks (not the in-flight totals, which belong to live streams) so
/// one arm's peak is not another's.
pub fn reset_decoded_file_cache_population_peaks() {
    use std::sync::atomic::Ordering::Relaxed;
    let meter = population_meter();
    meter.peak_extent_bytes.store(0, Relaxed);
    meter.peak_retained_bytes.store(0, Relaxed);
    meter.peak_streams.store(0, Relaxed);
}

/// One populate stream's charge against [`PopulationMeter`]. Releases exactly
/// what it charged on drop, so a cancelled population cannot leak the gauge.
#[derive(Debug, Default)]
struct PopulationCharge {
    extent_bytes: u64,
    retained_bytes: u64,
}

impl PopulationCharge {
    fn open() -> Self {
        use std::sync::atomic::Ordering::Relaxed;
        let meter = population_meter();
        let streams = meter.streams.fetch_add(1, Relaxed) + 1;
        meter.peak_streams.fetch_max(streams, Relaxed);
        Self::default()
    }

    fn charge(&mut self, batch: &RecordBatch) {
        use std::sync::atomic::Ordering::Relaxed;
        let extent = batch_extent_bytes(batch);
        let retained = batch_retained_bytes(batch);
        self.extent_bytes = self.extent_bytes.saturating_add(extent);
        self.retained_bytes = self.retained_bytes.saturating_add(retained);
        let meter = population_meter();
        let extent_total = meter.extent_bytes.fetch_add(extent, Relaxed) + extent;
        let retained_total = meter.retained_bytes.fetch_add(retained, Relaxed) + retained;
        meter.peak_extent_bytes.fetch_max(extent_total, Relaxed);
        meter.peak_retained_bytes.fetch_max(retained_total, Relaxed);
    }

    /// Give back what this stream holds, keeping its stream slot (it is still
    /// live — it just handed its batches to the cache, or dropped them).
    fn release_buffered(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        let meter = population_meter();
        meter
            .extent_bytes
            .fetch_sub(std::mem::take(&mut self.extent_bytes), Relaxed);
        meter
            .retained_bytes
            .fetch_sub(std::mem::take(&mut self.retained_bytes), Relaxed);
    }
}

impl Drop for PopulationCharge {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        self.release_buffered();
        population_meter().streams.fetch_sub(1, Relaxed);
    }
}

type TaskBatchStream = Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>;

#[derive(Debug)]
struct OrderedDrainBufferedBatch {
    batch: RecordBatch,
    bytes: u64,
}

enum OrderedDrainTaskState {
    Opening(BoxFuture<'static, DFResult<TaskBatchStream>>),
    Streaming(TaskBatchStream),
    Failed(Option<DataFusionError>),
    Done,
}

struct OrderedDrainTask {
    state: OrderedDrainTaskState,
    buffered: VecDeque<OrderedDrainBufferedBatch>,
    waiting_on_budget: bool,
}

impl OrderedDrainTask {
    fn new(open: BoxFuture<'static, DFResult<TaskBatchStream>>) -> Self {
        Self {
            state: OrderedDrainTaskState::Opening(open),
            buffered: VecDeque::new(),
            waiting_on_budget: false,
        }
    }
}

struct OrderedTaskDrain<S> {
    tasks: S,
    active: VecDeque<OrderedDrainTask>,
    tasks_exhausted: bool,
    concurrency_limit: usize,
    budget_bytes: u64,
    buffered_bytes: u64,
    metrics_path: &'static str,
}

impl<S> OrderedTaskDrain<S>
where
    S: Stream<Item = BoxFuture<'static, DFResult<TaskBatchStream>>> + Unpin,
{
    /// Build with the buffer budget passed explicitly instead of reading
    /// `SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES`. Lets a test exercise the
    /// backpressure path at a small budget without a `set_var`, which is
    /// process-global and would be observed by every other drain built in the
    /// same test binary.
    fn with_budget_bytes(
        tasks: S,
        concurrency_limit: usize,
        metrics_path: &'static str,
        budget_bytes: u64,
    ) -> Self {
        let drain = Self {
            tasks,
            active: VecDeque::new(),
            tasks_exhausted: false,
            concurrency_limit: concurrency_limit.max(1),
            budget_bytes,
            buffered_bytes: 0,
            metrics_path,
        };
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => metrics_path
        )
        .set(0.0);
        drain
    }

    fn set_buffered_gauge(&self) {
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => self.metrics_path
        )
        .set(self.buffered_bytes as f64);
    }

    fn release_buffered_bytes(&mut self, bytes: u64) {
        self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes);
        self.set_buffered_gauge();
    }

    fn front_ready_batch_or_advance(&mut self) -> Option<DFResult<RecordBatch>> {
        loop {
            let front = self.active.front_mut()?;
            if let Some(buffered) = front.buffered.pop_front() {
                let should_clear_wait = front.waiting_on_budget;
                let bytes = buffered.bytes;
                let batch = buffered.batch;
                let _ = front;
                self.release_buffered_bytes(bytes);
                if should_clear_wait {
                    if let Some(front) = self.active.front_mut() {
                        if self.buffered_bytes < self.budget_bytes {
                            front.waiting_on_budget = false;
                        }
                    }
                }
                return Some(Ok(batch));
            }
            match &mut front.state {
                OrderedDrainTaskState::Failed(err) => {
                    return Some(Err(err.take().expect("ordered drain stored error")));
                }
                OrderedDrainTaskState::Done => {
                    self.active.pop_front();
                }
                OrderedDrainTaskState::Opening(_) | OrderedDrainTaskState::Streaming(_) => {
                    return None;
                }
            }
        }
    }

    fn poll_front_live(&mut self, cx: &mut Context<'_>) -> Poll<Option<DFResult<RecordBatch>>> {
        let Some(front) = self.active.front_mut() else {
            return if self.tasks_exhausted {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        };
        loop {
            match &mut front.state {
                OrderedDrainTaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        front.state = OrderedDrainTaskState::Streaming(stream);
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Poll::Pending => return Poll::Pending,
                },
                OrderedDrainTaskState::Streaming(stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(None) => {
                        front.state = OrderedDrainTaskState::Done;
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                OrderedDrainTaskState::Failed(err) => {
                    return Poll::Ready(Some(Err(err.take().expect("ordered drain stored error"))))
                }
                OrderedDrainTaskState::Done => return Poll::Pending,
            }
        }
    }

    fn fill_active(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while !self.tasks_exhausted && self.active.len() < self.concurrency_limit {
            match Pin::new(&mut self.tasks).poll_next(cx) {
                Poll::Ready(Some(open)) => {
                    self.active.push_back(OrderedDrainTask::new(open));
                    progressed = true;
                }
                Poll::Ready(None) => {
                    self.tasks_exhausted = true;
                }
                Poll::Pending => break,
            }
        }
        progressed
    }

    fn poll_non_front_task(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let metrics_path = self.metrics_path;
        let Some(task) = self.active.get_mut(index) else {
            return false;
        };
        if !task.buffered.is_empty() {
            return false;
        }
        loop {
            match &mut task.state {
                OrderedDrainTaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        task.state = OrderedDrainTaskState::Streaming(stream);
                    }
                    Poll::Ready(Err(err)) => {
                        task.state = OrderedDrainTaskState::Failed(Some(err));
                        return true;
                    }
                    Poll::Pending => return false,
                },
                OrderedDrainTaskState::Streaming(stream) => {
                    if self.buffered_bytes >= self.budget_bytes {
                        if !task.waiting_on_budget {
                            metrics::counter!(
                                "siglake_query_ordered_drain_backpressure_total",
                                "path" => metrics_path
                            )
                            .increment(1);
                            task.waiting_on_budget = true;
                        }
                        return false;
                    }
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let bytes = batch.get_array_memory_size() as u64;
                            if self.buffered_bytes.saturating_add(bytes) > self.budget_bytes
                                && !task.waiting_on_budget
                            {
                                metrics::counter!(
                                    "siglake_query_ordered_drain_backpressure_total",
                                    "path" => metrics_path
                                )
                                .increment(1);
                                task.waiting_on_budget = true;
                            } else if self.buffered_bytes.saturating_add(bytes) <= self.budget_bytes
                            {
                                task.waiting_on_budget = false;
                            }
                            task.buffered
                                .push_back(OrderedDrainBufferedBatch { batch, bytes });
                            self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
                            self.set_buffered_gauge();
                            return true;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            task.state = OrderedDrainTaskState::Failed(Some(err));
                            return true;
                        }
                        Poll::Ready(None) => {
                            task.state = OrderedDrainTaskState::Done;
                            return true;
                        }
                        Poll::Pending => return false,
                    }
                }
                OrderedDrainTaskState::Failed(_) | OrderedDrainTaskState::Done => return false,
            }
        }
    }
}

impl<S> Stream for OrderedTaskDrain<S>
where
    S: Stream<Item = BoxFuture<'static, DFResult<TaskBatchStream>>> + Unpin,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let mut progressed = this.fill_active(cx);
            let front_len = this.active.len();
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.active.len() != front_len {
                progressed = true;
                continue;
            }
            match this.poll_front_live(cx) {
                Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
            let front_len = this.active.len();
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.active.len() != front_len {
                progressed = true;
                continue;
            }
            for index in 1..this.active.len() {
                progressed |= this.poll_non_front_task(index, cx);
                if this.buffered_bytes >= this.budget_bytes {
                    break;
                }
            }
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.tasks_exhausted && this.active.is_empty() {
                return Poll::Ready(None);
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

impl<S> Drop for OrderedTaskDrain<S> {
    fn drop(&mut self) {
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => self.metrics_path
        )
        .set(0.0);
    }
}

/// #4890: how one population ended. Decided in `poll_next`, read once in
/// `Drop`, and the complete label set of
/// `siglake_query_scan_file_cache_populate_rows{outcome}` — four values, none
/// of them derived from data, so the series set is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PopulateEnd {
    /// The partition stream was built and never polled: the plan's `LIMIT` was
    /// satisfied elsewhere before this task read anything. Zero rows decoded,
    /// and distinct from a clip that decoded nothing — `miss` and `abandoned`
    /// are charged for both.
    Unpolled,
    /// Polled at least once and dropped before end-of-stream or an error. A
    /// `LIMIT` satisfied from the first batches is this, and so is a cancelled
    /// or failed-elsewhere query: the stream cannot tell them apart, so the
    /// label does not claim to. A round reads it against its own record of
    /// which queries succeeded — the reader's `--stats` input is the responses
    /// that returned, and it says so on every shape that has clipped samples.
    Clipped,
    /// End-of-stream reached. The entry may or may not have landed (oversized,
    /// contended, already populated by another partition); the decode depth is
    /// the same either way.
    Completed,
    /// The inner stream yielded an error. Not a measurement of how deep a
    /// successful browse reads, and excluded from the reader's fractions.
    Failed,
}

impl PopulateEnd {
    fn label(self) -> &'static str {
        match self {
            PopulateEnd::Unpolled => "unpolled",
            PopulateEnd::Clipped => "clipped",
            PopulateEnd::Completed => "completed",
            PopulateEnd::Failed => "error",
        }
    }
}

struct CachePopulateStream {
    key: String,
    tuning: EffectiveFileCacheTuning,
    inner: TaskBatchStream,
    buffered: Vec<RecordBatch>,
    buffered_bytes: u64,
    oversized: bool,
    insert_done: bool,
    /// Off-pool population memory this stream holds — see [`PopulationMeter`].
    charge: PopulationCharge,
    /// #4890: rows this population has been handed, cumulative over the whole
    /// stream. Deliberately independent of `buffered`, which is emptied by an
    /// insert and cleared outright when the entry goes oversized: the question
    /// is how deep the read got, not what survived in the candidate.
    ///
    /// Exactly: rows in the batches the inner reader yielded to this stream,
    /// counted before DataFusion's residual filter above the scan drops any of
    /// them. The inner read carries no predicate and no prune spec (a task with
    /// either takes the bypass above), so nothing below it page-prunes; row
    /// selection the reader still applies — positional deletes, time bounds —
    /// happens under this number, and decoder work that never became a batch is
    /// not in it. It is decode depth offered to population, not total physical
    /// work.
    yielded_rows: u64,
    /// Terminal disposition for the `Drop` observation.
    end: PopulateEnd,
    /// Per-request twin of `yielded_rows` (→ `stats.scan.file_cache_populate_rows`),
    /// added per batch rather than at `Drop`: a clipped plan drops this stream
    /// after `SourceMetricsStream` has folded its counters, so anything charged
    /// from `Drop` would miss the request it belongs to.
    cache_counters: Arc<FileCacheCounters>,
}

impl CachePopulateStream {
    fn insert_buffered(&mut self, cache: &mut QueryFileBatchCache) {
        self.charge.release_buffered();
        if cache.get(&self.key).is_none() {
            cache.insert(
                self.key.clone(),
                CachedFileBatches {
                    bytes: self.buffered_bytes,
                    batches: Arc::new(std::mem::take(&mut self.buffered)),
                },
                self.tuning,
            );
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "insert"
            )
            .increment(1);
        }
    }
}

/// #4846: one increment per population that decoded batches, kept them, and was
/// dropped before its end-of-stream arm could insert them — a `LIMIT` satisfied
/// from the first batches, or a cancelled query, above a file that was never
/// read to its end.
///
/// Without it a round reads the miss counter and cannot tell "448 populations
/// that did not stick" from "448 task streams opened, none drained": a `miss` is
/// charged when the stream is OPENED (once per partition), so #4494 had to
/// establish which of the two it was by hand, from the ABSENCE of the four
/// sibling series the insert path writes.
///
/// Scope is abandonment of an ELIGIBLE population, not a new taxonomy:
///
/// - `oversized` is excluded because the `poll_next` arm that set it already
///   charged `skip_oversized`; counting the drop as well would bill one
///   abandoned population twice;
/// - a stream that failed sets `insert_done` in its error arm, and a key another
///   partition populated first leaves `insert_buffered` with nothing to do but
///   still sets `insert_done` — both are finished, not abandoned.
///
/// The outcomes still do not partition `miss`. `miss` is charged before the
/// inner stream is constructed, so a construction error leaves a miss with no
/// outcome at all, and `hit` and `bypass` never build one of these.
impl Drop for CachePopulateStream {
    fn drop(&mut self) {
        if !self.insert_done && !self.oversized {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "abandoned"
            )
            .increment(1);
        }
        // #4890: one observation per population, here rather than in the
        // end-of-stream arm so that every population produces exactly one —
        // including the ones that never reach it (the clipped browse this
        // exists to measure, and the task dropped before its first poll).
        metrics::histogram!(
            "siglake_query_scan_file_cache_populate_rows",
            "outcome" => self.end.label()
        )
        .record(self.yielded_rows as f64);
    }
}

impl Stream for CachePopulateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.insert_done {
            return Poll::Ready(None);
        }
        if this.end == PopulateEnd::Unpolled {
            // A poll that returns Pending still means the read started, so the
            // task is no longer one the plan opened and abandoned untouched.
            this.end = PopulateEnd::Clipped;
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                this.yielded_rows = this.yielded_rows.saturating_add(batch.num_rows() as u64);
                this.cache_counters.populate_rows.fetch_add(
                    batch.num_rows() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                if !this.oversized {
                    // Population retains `batch.clone()`, so a sliced array keeps its
                    // whole backing allocation alive. Price that retained allocation:
                    // `get_slice_memory_size` estimates a compact copy of the slice and
                    // could admit one whose live backing buffer exceeds the entry limit.
                    // This same total becomes `CachedFileBatches::bytes` at EOF.
                    let buffered_bytes = this
                        .buffered_bytes
                        .saturating_add(batch.get_array_memory_size() as u64);
                    if this.tuning.entry_is_oversized(buffered_bytes) {
                        this.buffered.clear();
                        this.buffered_bytes = 0;
                        this.charge.release_buffered();
                        this.oversized = true;
                        metrics::counter!(
                            "siglake_query_scan_file_cache_requests_total",
                            "outcome" => "skip_oversized"
                        )
                        .increment(1);
                    } else {
                        this.buffered_bytes = buffered_bytes;
                        this.charge.charge(&batch);
                        this.buffered.push(batch.clone());
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.insert_done = true;
                this.end = PopulateEnd::Failed;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                // DEGRADE, DO NOT SPIN. This used to `wake_by_ref()` and return
                // Pending when the lock was contended, which is a busy-wait
                // through the scheduler: the task is immediately requeued, polls
                // again, and contends again. Under concurrency that turns every
                // finishing scan stream into a core burning `notify_parked_local`
                // / `wake_by_ref` / `eventfd_write` — the exact profile captured
                // on a degraded 1TB node on 2026-08-23 (~20% of samples in tokio
                // wake machinery with zero queries in flight).
                //
                // The insert is an OPTIMIZATION: the entry is rebuildable by the
                // next reader. Skipping it costs one future cache miss; spinning
                // for it costs a core. Skip, count it, and finish.
                //
                // The cache itself uses a blocking mutex because both lookup and
                // insert critical sections are short and contain no await point.
                // That avoids the scheduler handoff and fair waiter queue of an
                // async mutex, which otherwise made this non-blocking try_lock
                // lose to concurrently opening partitions almost every time.
                if !this.oversized {
                    match query_file_batch_cache().try_lock() {
                        Ok(mut cache) => this.insert_buffered(&mut cache),
                        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                            this.insert_buffered(&mut poisoned.into_inner());
                        }
                        Err(std::sync::TryLockError::WouldBlock) => {
                            metrics::counter!(
                                "siglake_query_scan_file_cache_requests_total",
                                "outcome" => "insert_skipped_contended"
                            )
                            .increment(1);
                        }
                    }
                }
                this.insert_done = true;
                this.end = PopulateEnd::Completed;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// #4847: row-group-granular population, LOCAL QUALIFICATION PROTOTYPE.
//
// Shipped policy is unchanged and stays drained-scan-only: the gate is
// `QueryScanTuning::file_cache_row_group_prototype`, which has no environment
// variable, no CLI flag, no chart value and no operator field, so nothing
// outside this process can turn it on. The packaged cache is still off (0/0)
// and the operator's 4Gi clamp is untouched. The evidence this exists to
// produce is in `docs/DESIGN_row_group_decoded_cache_qualification.md`.
//
// The idea under test (option (c) on #4494): make the cache unit a ROW GROUP,
// so a read that stops early still leaves whole units behind — no prefix
// contract, no "this entry covers n rows" key. Two facts make it implementable
// without touching the fork:
//
//   * parquet-rs never lets a record batch straddle a row group, so a decoded
//     prefix that reaches a group boundary is exactly a set of whole batches
//     (asserted per batch below rather than assumed: a batch that overshoots a
//     boundary stops population for the task and is counted);
//   * the reader's ONLY row-group selector from outside is the task's byte
//     range (`filter_row_groups_by_byte_range`), and it accumulates synthetic
//     offsets from 4 over `compressed_size()`. Recomputing those offsets from
//     the footer therefore addresses any contiguous span of groups exactly.
// ---------------------------------------------------------------------------

/// Per-file row-group layout: rows and compressed bytes per group, in file
/// order, read once from the footer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct RowGroupLayout {
    rows: Vec<u64>,
    compressed: Vec<u64>,
}

impl RowGroupLayout {
    fn from_footer(groups: Vec<(u64, u64)>) -> Self {
        Self {
            rows: groups.iter().map(|(rows, _)| *rows).collect(),
            compressed: groups.iter().map(|(_, bytes)| *bytes).collect(),
        }
    }

    /// The groups a task's byte range selects, under the reader's own rule: a
    /// group is selected when it overlaps `[start, start + length)`, offsets
    /// accumulated from 4 over the compressed sizes. `(0, 0)` is the whole file,
    /// which is how the reader reads it too (it skips byte-range filtering
    /// entirely for that case).
    fn selected(&self, start: u64, length: u64) -> Vec<usize> {
        if start == 0 && length == 0 {
            return (0..self.rows.len()).collect();
        }
        let end = start.saturating_add(length);
        let mut offset = 4u64;
        let mut selected = Vec::new();
        for (idx, size) in self.compressed.iter().enumerate() {
            let group_end = offset.saturating_add(*size);
            if offset < end && start < group_end {
                selected.push(idx);
            }
            offset = group_end;
        }
        selected
    }

    /// A byte range selecting exactly groups `first..=last` under that rule.
    /// `None` when the span is empty or out of range, or when a group reports
    /// zero compressed bytes — the range would then be ambiguous and the task
    /// takes the whole-file path instead.
    fn byte_range(&self, first: usize, last: usize) -> Option<(u64, u64)> {
        if first > last || last >= self.compressed.len() {
            return None;
        }
        if self.compressed[first..=last].contains(&0) {
            return None;
        }
        let start = 4 + self.compressed[..first].iter().sum::<u64>();
        let length = self.compressed[first..=last].iter().sum::<u64>();
        Some((start, length))
    }
}

/// Footer layouts already read, keyed by data-file path. Data files are
/// immutable, so an entry never goes stale. Capped and cleared wholesale when
/// full: this is a prototype, and a layout is two small vectors per file.
const ROW_GROUP_LAYOUT_CACHE_MAX_ENTRIES: usize = 8192;

fn row_group_layout_cache() -> &'static std::sync::Mutex<HashMap<String, Arc<RowGroupLayout>>> {
    static LAYOUTS: OnceLock<std::sync::Mutex<HashMap<String, Arc<RowGroupLayout>>>> =
        OnceLock::new();
    LAYOUTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Drop every cached footer layout. Measurement hook, like
/// [`clear_decoded_file_cache`]: an arm that must pay its own footer reads
/// needs the layouts cold.
pub fn clear_row_group_layout_cache() {
    row_group_layout_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

fn row_group_prototype_outcome(outcome: &'static str) {
    metrics::counter!(
        "siglake_query_scan_file_cache_row_group_total",
        "outcome" => outcome
    )
    .increment(1);
}

async fn row_group_layout(
    file_io: &iceberg::io::FileIO,
    path: &str,
) -> Option<Arc<RowGroupLayout>> {
    if let Some(layout) = row_group_layout_cache()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(path)
        .cloned()
    {
        row_group_prototype_outcome("layout_hit");
        return Some(layout);
    }
    match crate::iceberg::row_group_layout_from_footer(file_io, path).await {
        Ok(groups) => {
            row_group_prototype_outcome("layout_read");
            let layout = Arc::new(RowGroupLayout::from_footer(groups));
            let mut cache = row_group_layout_cache()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if cache.len() >= ROW_GROUP_LAYOUT_CACHE_MAX_ENTRIES {
                cache.clear();
            }
            cache.insert(path.to_string(), layout.clone());
            Some(layout)
        }
        Err(err) => {
            row_group_prototype_outcome("layout_error");
            tracing::debug!(path, error = %err, "row-group layout read failed");
            None
        }
    }
}

/// Cache identity of one row group: the file, the group's index in it, and the
/// same projection/deletes/direction the whole-file key carries. The task's
/// byte range is deliberately NOT part of it — the group's own index pins the
/// bytes, so two different splits of one file address the same group. Nothing
/// else about identity changes, which is what keeps a segment answer equal to
/// the read it replaces.
fn row_group_segment_key(task: &FileScanTask, group: usize, reverse: bool) -> String {
    format!(
        "{}:rg={group}:{}:reverse={reverse}",
        task.data_file_path(),
        task_projection_delete_key(task)
    )
}

/// Populate one entry per completed row group, as the read passes each boundary.
///
/// The whole-file stream inserts in its end-of-stream arm and therefore inserts
/// nothing when a `LIMIT` is satisfied first (#4494). This one inserts at every
/// boundary it reaches, so a read that covers g whole groups leaves g entries
/// behind whether or not it ever finishes.
struct RowGroupPopulateStream {
    inner: TaskBatchStream,
    tuning: EffectiveFileCacheTuning,
    /// Segment keys and row counts for the groups this stream reads, in the
    /// order the reader emits them.
    keys: Vec<String>,
    group_rows: Vec<u64>,
    /// The group being filled.
    at: usize,
    filled_rows: u64,
    buffered: Vec<RecordBatch>,
    buffered_bytes: u64,
    /// This group will not be inserted (it crossed the entry bound), but its
    /// rows are still counted so the next boundary is found.
    group_skipped: bool,
    /// A batch crossed a group boundary — the alignment this rests on does not
    /// hold for this read, so stop populating rather than guess.
    misaligned: bool,
    charge: PopulationCharge,
}

impl RowGroupPopulateStream {
    fn discard_buffered(&mut self) {
        self.buffered.clear();
        self.buffered_bytes = 0;
        self.charge.release_buffered();
    }

    fn insert_group(&mut self) {
        let key = self.keys[self.at].clone();
        let entry = CachedFileBatches {
            bytes: self.buffered_bytes,
            batches: Arc::new(std::mem::take(&mut self.buffered)),
        };
        self.buffered_bytes = 0;
        self.charge.release_buffered();
        // Same non-blocking discipline as the whole-file path: the insert is an
        // optimization and the entry is rebuildable, so a contended lock is
        // counted and skipped, never waited on.
        match query_file_batch_cache().try_lock() {
            Ok(mut cache) => {
                if cache.get(&key).is_none() {
                    cache.insert(key, entry, self.tuning);
                    metrics::counter!(
                        "siglake_query_scan_file_cache_requests_total",
                        "outcome" => "insert"
                    )
                    .increment(1);
                    row_group_prototype_outcome("group_inserted");
                }
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                let mut cache = poisoned.into_inner();
                if cache.get(&key).is_none() {
                    cache.insert(key, entry, self.tuning);
                    metrics::counter!(
                        "siglake_query_scan_file_cache_requests_total",
                        "outcome" => "insert"
                    )
                    .increment(1);
                    row_group_prototype_outcome("group_inserted");
                }
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                metrics::counter!(
                    "siglake_query_scan_file_cache_requests_total",
                    "outcome" => "insert_skipped_contended"
                )
                .increment(1);
            }
        }
    }
}

/// A population dropped with a partial group buffered is abandoned in exactly
/// #4846's sense: it decoded batches, kept them, and never reached an insert.
/// It charges the same outcome, so the counter keeps one meaning across both
/// granularities. The difference the prototype is meant to show is that the
/// groups it DID complete are already in the cache by then.
impl Drop for RowGroupPopulateStream {
    fn drop(&mut self) {
        if !self.buffered.is_empty() {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "abandoned"
            )
            .increment(1);
        }
    }
}

impl Stream for RowGroupPopulateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if !this.misaligned && this.at < this.keys.len() {
                    let rows = batch.num_rows() as u64;
                    let group_rows = this.group_rows[this.at];
                    let filled = this.filled_rows + rows;
                    if filled > group_rows {
                        // parquet-rs does not produce such a batch; a row
                        // selection or a delete file would. Count it and stop
                        // populating — a segment must be a whole group or
                        // nothing.
                        this.misaligned = true;
                        this.discard_buffered();
                        row_group_prototype_outcome("misaligned");
                    } else {
                        if !this.group_skipped {
                            let buffered_bytes = this
                                .buffered_bytes
                                .saturating_add(batch.get_array_memory_size() as u64);
                            if this.tuning.entry_is_oversized(buffered_bytes) {
                                this.discard_buffered();
                                this.group_skipped = true;
                                metrics::counter!(
                                    "siglake_query_scan_file_cache_requests_total",
                                    "outcome" => "skip_oversized"
                                )
                                .increment(1);
                            } else {
                                this.buffered_bytes = buffered_bytes;
                                this.charge.charge(&batch);
                                this.buffered.push(batch.clone());
                            }
                        }
                        this.filled_rows = filled;
                        if this.filled_rows == group_rows {
                            if !this.group_skipped {
                                this.insert_group();
                            }
                            this.at += 1;
                            this.filled_rows = 0;
                            this.group_skipped = false;
                        }
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                this.discard_buffered();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                // Nothing to flush: a group that did not reach its boundary is
                // not a group. The partial buffer is released by Drop, which
                // also charges `abandoned`.
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// What the prototype can do with one task.
enum RowGroupPlan {
    /// Every group is cached: the task is served without a reader.
    Served(Vec<RecordBatch>),
    /// `served` groups are cached; the rest are read from `range` and
    /// populated.
    Partial {
        served: Vec<RecordBatch>,
        served_groups: usize,
        range: (u64, u64),
        keys: Vec<String>,
        group_rows: Vec<u64>,
    },
    /// Nothing is cached: read the whole task and populate per group.
    Populate {
        keys: Vec<String>,
        group_rows: Vec<u64>,
    },
}

/// Decide what the prototype does with `task`, or `None` to leave it to the
/// shipped whole-file path.
///
/// Refusals are deliberate and narrow: a reversed read (the ordered path, which
/// does not use this cache at all today), a task with delete files (its row
/// counts no longer match the footer's, so no group could ever close), and a
/// file whose footer would not read.
async fn row_group_plan(
    file_io: &iceberg::io::FileIO,
    task: &FileScanTask,
    reverse: bool,
) -> Option<RowGroupPlan> {
    if reverse || !task.deletes.is_empty() {
        row_group_prototype_outcome("refused");
        return None;
    }
    let layout = row_group_layout(file_io, task.data_file_path()).await?;
    let groups = layout.selected(task.start, task.length);
    if groups.is_empty() {
        row_group_prototype_outcome("refused");
        return None;
    }
    let keys: Vec<String> = groups
        .iter()
        .map(|group| row_group_segment_key(task, *group, reverse))
        .collect();
    let group_rows: Vec<u64> = groups.iter().map(|group| layout.rows[*group]).collect();

    let mut served = Vec::new();
    let mut served_groups = 0usize;
    {
        let cache = query_file_batch_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in &keys {
            match cache.get(key) {
                Some(entry) => {
                    served.extend(entry.batches.iter().cloned());
                    served_groups += 1;
                }
                None => break,
            }
        }
    }

    if served_groups == keys.len() {
        return Some(RowGroupPlan::Served(served));
    }
    let remaining_keys = keys[served_groups..].to_vec();
    let remaining_rows = group_rows[served_groups..].to_vec();
    if served_groups == 0 {
        return Some(RowGroupPlan::Populate {
            keys: remaining_keys,
            group_rows: remaining_rows,
        });
    }
    // A partial serve needs a reader over the groups that are NOT cached, which
    // only the byte range can express. If it cannot be expressed, read the whole
    // task and re-populate rather than serve a wrong prefix.
    match layout.byte_range(groups[served_groups], *groups.last().unwrap()) {
        Some(range) => Some(RowGroupPlan::Partial {
            served,
            served_groups,
            range,
            keys: remaining_keys,
            group_rows: remaining_rows,
        }),
        None => Some(RowGroupPlan::Populate { keys, group_rows }),
    }
}

#[derive(Debug, Clone)]
pub struct SiglakeStaticTableProvider {
    table: Table,
    snapshot_id: Option<i64>,
    schema: ArrowSchemaRef,
}

impl SiglakeStaticTableProvider {
    pub async fn try_new_from_table(table: Table) -> Result<Self> {
        let schema = Arc::new(schema_with_text_tokenizers(&table)?);
        Ok(Self {
            table,
            snapshot_id: None,
            schema,
        })
    }

    /// #89: pin the provider to a specific snapshot (time-travel read) and,
    /// since #2554, optionally to the Iceberg schema id the coordinator planned
    /// against.
    ///
    /// A coordinator ships its serving generation to workers so a fan-out reads
    /// ONE consistent generation cluster-wide — without it, a worker whose
    /// table cache is ahead of the coordinator's could double-serve a
    /// just-committed segment that the coordinator still counts in its buffer
    /// partial.
    ///
    /// The snapshot alone is not the generation a shard has to reproduce. An
    /// additive `migrate-schema` commits an `UpdateSchemaAction` and no data
    /// snapshot, so the schema id moves while the snapshot id stands still —
    /// and this provider's Arrow schema (what DataFusion plans against) is
    /// built from `current_schema()`. A worker whose metadata cache predates
    /// the migration therefore recognises the pinned snapshot and answers from
    /// the narrow schema, so a fragment referencing the new column fails as
    /// invalid SQL on that peer alone.
    ///
    /// `schema_id` is honoured against the schemas this table's metadata
    /// retains, which includes the historical ones: a worker that is AHEAD of
    /// the coordinator serves the pinned older schema rather than refusing.
    /// `None` (an older coordinator that ships no schema identity) keeps the
    /// pre-#2554 behaviour — snapshot only, current schema. Returns `None`
    /// when either half is absent from this table's metadata; the caller
    /// refreshes and, failing that, refuses.
    pub fn with_generation(&self, snapshot_id: i64, schema_id: Option<i32>) -> Option<Self> {
        self.table.metadata().snapshot_by_id(snapshot_id)?;
        Some(Self {
            table: self.table.clone(),
            snapshot_id: Some(snapshot_id),
            schema: self.schema_for(schema_id)?,
        })
    }

    /// The Arrow schema this provider serves for `schema_id`, or `None` when
    /// the table's metadata does not retain that schema.
    ///
    /// The current generation reuses the memoized schema (the tokenizer walk is
    /// paid once per cache entry); a historical one is built on demand, which a
    /// pinned shard pays only while the caches disagree.
    pub fn schema_for(&self, schema_id: Option<i32>) -> Option<ArrowSchemaRef> {
        let Some(schema_id) = schema_id else {
            return Some(self.schema.clone());
        };
        if schema_id == self.table.metadata().current_schema().schema_id() {
            return Some(self.schema.clone());
        }
        let iceberg_schema = self.table.metadata().schema_by_id(schema_id)?;
        schema_with_text_tokenizers_of(&self.table, iceberg_schema)
            .ok()
            .map(Arc::new)
    }
}

fn schema_with_text_tokenizers(table: &Table) -> Result<arrow_schema::Schema> {
    schema_with_text_tokenizers_of(table, table.metadata().current_schema())
}

/// [`schema_with_text_tokenizers`] against a NAMED Iceberg schema of `table`
/// rather than its current one — how a shard pinned to an older schema
/// generation is served (#2554). The tokenizer metadata comes from the index
/// config, which is a property of the table, not of the schema generation.
fn schema_with_text_tokenizers_of(
    table: &Table,
    iceberg_schema: &iceberg::spec::SchemaRef,
) -> Result<arrow_schema::Schema> {
    let mut schema = schema_to_arrow_schema(iceberg_schema)?;
    let Some(config) = loaded_table_index_config(table).map_err(|e| {
        iceberg::Error::new(
            iceberg::ErrorKind::Unexpected,
            format!("load index config for `{}`", table.identifier().name()),
        )
        .with_source(e)
    })?
    else {
        return Ok(schema);
    };
    let tokenizers = text_field_tokenizers(&config);
    if tokenizers.is_empty() {
        return Ok(schema);
    }
    let fields: Vec<arrow_schema::FieldRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let Some(tokenizer) = tokenizers.get(field.name()) else {
                return field.clone();
            };
            let mut metadata = field.metadata().clone();
            metadata.insert(
                crate::TEXT_TOKENIZER_FIELD_METADATA_KEY.to_string(),
                match tokenizer {
                    Tokenizer::Default => "default",
                    Tokenizer::Raw => "raw",
                    Tokenizer::Stem => "stem",
                }
                .to_string(),
            );
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    schema = arrow_schema::Schema::new(fields);
    Ok(schema)
}

#[derive(Debug)]
pub struct SiglakeIcebergTableScan {
    table: Table,
    plan_properties: Arc<PlanProperties>,
    projection: Option<Vec<String>>,
    projected_columns: Arc<Vec<String>>,
    limit: Option<usize>,
    /// True when DataFusion pushed any filter into this scan. Exact pushed
    /// filters remove the residual `FilterExec`, so statistics must only stay
    /// exact when this is false.
    has_pushed_filters: bool,
    predicates: Option<Predicate>,
    raw_prune_spec: Option<RawPruneSpec>,
    /// WS-7: `attr_get(attributes, key) = value` hints mapped onto promoted
    /// Utf8 columns — row groups prune via the column's stats in files that
    /// carry it (see the reader; conservative for pre-promotion files).
    promoted_prune: Vec<PromotedPruneSpec>,
    task_partitions: Arc<Vec<Vec<FileScanTask>>>,
    /// Per-request cancellation, captured from the session at planning time.
    /// `None` for callers that never inject one (tests, internal scans), which
    /// behaves exactly as before.
    cancel: Option<QueryCancel>,
    data_file_concurrency_limit: usize,
    reader_tuning: EffectiveReaderTuning,
    file_cache_tuning: EffectiveFileCacheTuning,
    metrics: ExecutionPlanMetricsSet,
    /// The planning-time residual-filter classification (dimensional/FTS/
    /// promoted predicates — NOT time-only windows). Drives the ordered
    /// chain's prefetch decision; see `sequential_task_chain`.
    residual_filtered: bool,
    /// Exact `(min, max)` of the `timestamp` column in its Arrow time unit across
    /// the scanned files, from the manifest bounds — populated only for an
    /// unfiltered scan whose output schema includes `timestamp`. Reported as
    /// column statistics so DataFusion answers `min/max(timestamp)` with zero IO.
    ts_bounds: Option<(i64, i64)>,
    /// WS-3: this scan advertises `timestamp` output ordering (declared
    /// direction), so each partition's batches must be emitted sorted: tasks
    /// drained strictly in order (disjoint runs arranged at planning), or
    /// k-way merged where `merge_partitions` says the files overlap.
    preserve_task_order: bool,
    /// Requested lead direction (for building the merge sort expr).
    sort_descending: bool,
    /// True when the reader must iterate files / row groups / rows in reverse
    /// to serve the opposite of the table's declared on-disk direction.
    reverse_scan: bool,
    /// Partition index → clusters → layer sizes (tasks laid out cluster by
    /// cluster, each cluster layer by layer, each layer in time order). Empty
    /// per-partition vec = plain in-order concat. A cluster with >1 layer
    /// k-way merges its LAYERS (each a sequential disjoint run) — fan-in =
    /// overlap depth. Outer vec empty when no ordering is advertised.
    partition_clusters: Arc<Vec<Vec<OrderedCluster>>>,
    /// What [`scan_output_ordering`] decided for this scan ("advertised" or
    /// the refusal reason) — surfaced per-request through the plan-runtime
    /// summary so a breaker-tripped browse self-identifies which path it took
    /// (pod-level counters can't attribute an individual 413).
    ordering_outcome: &'static str,
    /// Partition streams handed out by `execute` and not yet finished; see
    /// [`ScanPartitionTracker`] and [`settle_scan_partitions`].
    partition_tracker: Arc<ScanPartitionTracker>,
    /// #4865: the staged start for an unordered, row-clipped `LIMIT`. `None`
    /// for every other shape — an ordered scan, an aggregate, a single
    /// partition, or a session carrying no [`ClippedScanLimit`] — which is
    /// exactly the pre-#4865 behavior.
    admission: Option<Arc<ScanAdmission>>,
    /// The downstream ordered `LIMIT` (`OrderedScanLimit`), when the session
    /// carries one. Caps the per-cluster merge's OUTPUT batch and, when no
    /// residual filter sits above the scan, the ordered partition's output: a
    /// merge that builds a full 8192-row batch for a `LIMIT 100` keeps every
    /// contributing input batch alive to do it, which is what a small memory
    /// pool feels (3 wide overlapping files refused a 10-row browse on a 3 MiB
    /// pool).
    ordered_limit: Option<usize>,
    /// True when every row emitted by this ordered scan is eligible for the
    /// downstream LIMIT. Time-only predicates are exact in the reader; a
    /// residual predicate evaluated above the scan may need more than LIMIT
    /// source rows to produce LIMIT matches and therefore cannot be capped.
    ordered_source_limit_safe: bool,
    /// Why this scan's text predicate will not load a per-file inverted index
    /// ([`text_index_decline_reason`]), `None` when it may. Displayed in the
    /// plan so an EXPLAIN says which path the shape takes without reading a
    /// process-wide counter.
    text_index_decline: Option<&'static str>,
}

/// The Iceberg field-id stamped in an Arrow field's Parquet metadata, if any.
fn iceberg_field_id(field: &arrow_schema::Field) -> Option<i32> {
    field
        .metadata()
        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse().ok())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct GlobalTimestampBoundsCacheKey {
    table_uuid: uuid::Uuid,
    snapshot_id: i64,
    field_id: i32,
}

const GLOBAL_TIMESTAMP_BOUNDS_CACHE_CAP: usize = 256;

/// `(entries, insertion order)` for immutable-snapshot timestamp bounds.
type GlobalTimestampBoundsCacheInner = (
    HashMap<GlobalTimestampBoundsCacheKey, Option<(i64, i64)>>,
    VecDeque<GlobalTimestampBoundsCacheKey>,
);

static GLOBAL_TIMESTAMP_BOUNDS_CACHE: OnceLock<std::sync::Mutex<GlobalTimestampBoundsCacheInner>> =
    OnceLock::new();

fn global_timestamp_bounds_cache() -> &'static std::sync::Mutex<GlobalTimestampBoundsCacheInner> {
    GLOBAL_TIMESTAMP_BOUNDS_CACHE
        .get_or_init(|| std::sync::Mutex::new((HashMap::new(), VecDeque::new())))
}

/// The outer `Option` distinguishes a cache miss from a cached snapshot with
/// no timestamp bounds.
fn global_timestamp_bounds_cache_get(
    key: GlobalTimestampBoundsCacheKey,
) -> Option<Option<(i64, i64)>> {
    let guard = global_timestamp_bounds_cache().lock().ok()?;
    guard.0.get(&key).copied()
}

fn global_timestamp_bounds_cache_put(
    key: GlobalTimestampBoundsCacheKey,
    bounds: Option<(i64, i64)>,
) {
    if let Ok(mut guard) = global_timestamp_bounds_cache().lock() {
        let (map, order) = &mut *guard;
        if map.insert(key, bounds).is_none() {
            order.push_back(key);
            while map.len() > GLOBAL_TIMESTAMP_BOUNDS_CACHE_CAP {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                } else {
                    break;
                }
            }
        }
    }
}

/// Global `(min, max)` of the `timestamp` column across the alive data files of
/// `snapshot_id` (or the current snapshot), read from the manifest bounds.
/// Direct manifest loads bypass Iceberg's parsed-object cache, so retain the
/// result by `(table UUID, snapshot, field id)`. Entries are pure functions of
/// immutable snapshots and have no TTL; the bounded FIFO only ages old
/// snapshots out. A failed manifest walk is not cached and can recover on the
/// next plan. `None` if there's no snapshot or no file carries the bound.
async fn global_timestamp_bounds(
    table: &Table,
    snapshot_id: Option<i64>,
    ts_field_id: i32,
) -> Option<(i64, i64)> {
    use iceberg::spec::PrimitiveLiteral;
    let meta = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => meta.snapshot_by_id(id)?,
        None => meta.current_snapshot()?,
    };
    let key = GlobalTimestampBoundsCacheKey {
        table_uuid: meta.uuid(),
        snapshot_id: snapshot.snapshot_id(),
        field_id: ts_field_id,
    };
    if let Some(bounds) = global_timestamp_bounds_cache_get(key) {
        return bounds;
    }
    let manifest_list = match snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
    {
        Ok(manifest_list) => manifest_list,
        Err(_) => return None,
    };
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for mf in manifest_list.entries() {
        let manifest = match mf.load_manifest(table.file_io()).await {
            Ok(manifest) => manifest,
            Err(_) => return None,
        };
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let df = entry.data_file();
            if let Some(PrimitiveLiteral::Long(v)) =
                df.lower_bounds().get(&ts_field_id).map(|d| d.literal())
            {
                min = Some(min.map_or(*v, |m| m.min(*v)));
            }
            if let Some(PrimitiveLiteral::Long(v)) =
                df.upper_bounds().get(&ts_field_id).map(|d| d.literal())
            {
                max = Some(max.map_or(*v, |m| m.max(*v)));
            }
        }
    }
    let bounds = match (min, max) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    };
    global_timestamp_bounds_cache_put(key, bounds);
    bounds
}

/// Whether this scan may load a per-file inverted index for its text
/// predicate, and if not, the reason to attribute the refusal to.
///
/// The v1 index is a WHOLE-FILE structure: touching it at all costs a
/// deserialization proportional to the file's rows (~40 bytes of parsed index
/// per indexed row), independent of how many rows the query ends up wanting.
/// Both v1 declines below are the same argument from opposite ends of the plan
/// — the query wants a handful of rows and the index charges for all of them:
///
/// - `ordered_limit`: an index RowSelection batches by SELECTED rows, so sparse
///   postings span most of a large file before the first batch is emitted,
///   defeating the contiguous tail/head read that lets an ordered browse
///   short-circuit.
/// - `clipped_limit`: a bare `LIMIT n` over a text predicate stops the scan
///   after `n` matches, which for anything but a very rare term is a sliver of
///   the first file. #4329 measured the two against each other on 14 × 7.34M
///   rows with every index resident: `keyword` 20.3 ms indexed against 6.1 ms
///   scanned, `substring_scan` 813.1 ms against 4.5 ms
///   (`docs/DESIGN_inverted_index.md`).
///
/// The segmented reader can now estimate point-term document frequency before
/// it fetches postings. A clipped execution therefore carries its limit into
/// `RawPruneSpec`: v1 remains declined, while seg2 admits terms whose summed df
/// is no larger than the clip (#5040). Ordered scans retain the unconditional
/// decline because sparse postings still defeat their contiguous early stop.
///
/// Correctness does not turn on this: the index only ever produces a
/// superset RowSelection, and the engine re-evaluates the exact predicate
/// above the scan either way. File and row-group blooms stay active when the
/// index is declined.
fn text_index_decline_reason(
    has_text_spec: bool,
    ordered_limit: bool,
    clipped_limit: bool,
) -> Option<&'static str> {
    if !has_text_spec {
        return None;
    }
    if ordered_limit {
        Some("ordered_limit")
    } else if clipped_limit {
        Some("clipped_limit")
    } else {
        None
    }
}

impl SiglakeIcebergTableScan {
    async fn try_new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        state: &dyn Session,
    ) -> DFResult<Self> {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let projected_columns = Arc::new(get_schema_column_names(&output_schema));
        let projection = get_column_names(schema.clone(), projection);
        let has_pushed_filters = !filters.is_empty();
        let predicates = PredicateConverter::new(&schema).convert_filters(filters);
        let text_tokenizers = loaded_table_index_config(&table)
            .map_err(|e| DataFusionError::External(e.into()))?
            .map(|config| text_field_tokenizers(&config))
            .unwrap_or_default();
        let mut raw_prune_spec = extract_raw_prune_spec(filters, &text_tokenizers)?;
        let clipped_limit = limit.or_else(|| {
            state
                .config()
                .get_extension::<ClippedScanLimit>()
                .map(|limit| limit.limit)
        });
        let text_index_decline = text_index_decline_reason(
            raw_prune_spec.is_some(),
            state
                .config()
                .get_extension::<PreferredScanOrder>()
                .is_some()
                && state.config().get_extension::<OrderedScanLimit>().is_some(),
            clipped_limit.is_some(),
        );
        if let Some(reason) = text_index_decline {
            let spec = raw_prune_spec
                .as_mut()
                .expect("a reason is only returned for a text spec");
            if reason == "ordered_limit" {
                spec.inverted_index_row_selection = false;
            } else {
                spec.segmented_clipped_limit = clipped_limit;
            }
            metrics::counter!(
                "siglake_query_inverted_index_declined_total",
                "reason" => reason
            )
            .increment(1);
        }
        let promoted_prune = extract_promoted_prune(filters, &promoted_utf8_columns(&table));
        let tuning = crate::current_query_scan_tuning();
        // Per-request distributed-query shard, injected as a SessionConfig
        // extension by the query-server (absent for ordinary single-pod scans).
        let shard = state.config().get_extension::<ScanShard>().map(|a| *a);
        // Resolved ONCE per scan so the refusal, the cached-plan decision and
        // the plan-cache key all see the same budget.
        let global_fan_in_budget = ordered_merge_global_fanin_for(state);
        let mut task_partitions = plan_task_partitions(
            &table,
            snapshot_id,
            projection.as_ref(),
            predicates.as_ref(),
            limit,
            state.config().target_partitions(),
            tuning,
            shard,
        )
        .await?;
        // `partition_count` is computed after the #4 unordered-limit re-split below,
        // since that can change the partition count. `planned_files`/`planned_bytes`
        // are invariant under re-partitioning (same file set), so compute them here.
        let planned_files = task_partitions
            .iter()
            .map(|partition| partition.len())
            .sum();
        let planned_bytes = task_partitions
            .iter()
            .flatten()
            .map(|task| task.length)
            .sum::<u64>();
        let ordered_tuning = state
            .config()
            .get_extension::<OrderedScanTuning>()
            .map(|tuning| *tuning)
            .unwrap_or_default();
        let reader_tuning =
            effective_reader_tuning(tuning, planned_files, planned_bytes, ordered_tuning);
        let file_cache_tuning = effective_file_cache_tuning(tuning);
        // WS-3: advertise the scan's physical `timestamp ASC` ordering to
        // DataFusion when it provably holds, so the optimizer can elide a
        // redundant Sort and early-stop an ordered `LIMIT` (SortPreservingMerge
        // over the already-sorted partitions). May REARRANGE multi-file
        // partitions into time-ascending disjoint runs; when it advertises,
        // execution must drain each partition's tasks in order. See
        // [`scan_output_ordering`] for the safety gate.
        // Filtered scans keep the parallel pruned plan: with a residual
        // filter (dimensional predicate, FTS, LIKE) a bounded TopK over the
        // bloom/index-pruned parallel scan beats an ordered drain — the drain
        // must probe files in time order until the LIMIT fills (~7s p50 for
        // keyword@1TB vs 240ms under TopK). Ordered advertisement pays for
        // weak/no filters, where the blocking sort was a full-table scan.
        // A TIMESTAMP-ONLY predicate is a weak filter, not a residual one: it
        // prunes whole FILES via manifest bounds and composes with the ordered
        // drain — the canonical windowed browse (`timestamp >= X ORDER BY
        // timestamp LIMIT n`) must early-stop, not TopK-scan the entire
        // window (which trips the rows breaker on wide windows).
        // Selectivity-aware override (07-16 filed finding): the query server
        // measures a residual equality/IN filter's selectivity from the
        // group-count side aggregates and sets this hint when the filter is
        // LOW-selectivity — there an ordered early-stop drain (LIMIT/frac
        // rows) beats the full-window TopK the blanket refusal forces (a
        // 1/3-selective attribute browse full-scanned into the rows breaker).
        // The server also carries a TopK fallback for the temporal-skew case
        // (matches only deep in the scan direction), so a wrong hint degrades
        // to today's behavior instead of a 413.
        let allow_residual = state
            .config()
            .get_extension::<OrderedResidualHint>()
            .map(|h| h.allow)
            .unwrap_or(false);
        let filtered_scan = ((has_pushed_filters && !time_only_filters(filters))
            || raw_prune_spec.is_some()
            || !promoted_prune.is_empty())
            && !allow_residual;
        let ordering = if filtered_scan {
            metrics::counter!(
                "siglake_query_scan_output_ordering_total",
                "outcome" => "filtered"
            )
            .increment(1);
            ScanOrderingDecision::refused("filtered", 0, 0, global_fan_in_budget)
        } else {
            let requested_descending_hint = state
                .config()
                .get_extension::<PreferredScanOrder>()
                .map(|o| o.descending);
            let small_limit_hint = state
                .config()
                .get_extension::<OrderedScanLimit>()
                .map(|l| {
                    let cap = ordered_single_partition_max_limit();
                    cap > 0 && l.limit <= cap
                })
                .unwrap_or(false);
            let cache_tuning = OrderedPlanCacheTuning {
                global_fan_in_budget,
                merge_max_fan_in: ordered_merge_max_fanin_for(state),
                sort_cluster_max_rows: ordered_sort_cluster_max_rows_for(state),
            };
            let cache_key = requested_descending_hint.and_then(|desc| {
                ordered_plan_cache_key(
                    &table,
                    snapshot_id,
                    desc,
                    small_limit_hint,
                    &task_partitions,
                    cache_tuning,
                )
            });
            let cached = cache_key.and_then(ordered_plan_cache_get);
            let mut served_from_cache = None;
            if let (Some(cached_plan), Ok(idx)) =
                (cached.as_ref(), output_schema.index_of("timestamp"))
            {
                if apply_cached_ordered_plan(cached_plan, &mut task_partitions).is_some() {
                    metrics::counter!(
                        "siglake_query_ordered_plan_cache_total", "outcome" => "hit"
                    )
                    .increment(1);
                    metrics::counter!(
                        "siglake_query_scan_output_ordering_total", "outcome" => "advertised"
                    )
                    .increment(1);
                    served_from_cache = Some(ScanOrderingDecision::advertised(
                        ScanOrderingPlan {
                            exprs: vec![PhysicalSortExpr::new(
                                Arc::new(Column::new("timestamp", idx)),
                                SortOptions {
                                    descending: cached_plan.descending,
                                    nulls_first: cached_plan.descending,
                                },
                            )],
                            descending: cached_plan.descending,
                            reverse: cached_plan.reverse,
                            partition_clusters: cached_plan.clusters.clone(),
                        },
                        cached_plan.overlap_partition_count,
                        cached_plan.overlap_streams_total,
                        global_fan_in_budget,
                    ));
                }
            }
            match served_from_cache {
                Some(decision) => decision,
                None => {
                    metrics::counter!(
                        "siglake_query_ordered_plan_cache_total", "outcome" => "miss"
                    )
                    .increment(1);
                    let decision = scan_output_ordering(
                        &table,
                        snapshot_id,
                        &output_schema,
                        &mut task_partitions,
                        state,
                    )
                    .await;
                    if let (Some(key), Some(plan)) = (cache_key, decision.plan.as_ref()) {
                        ordered_plan_cache_put(
                            key,
                            CachedOrderedPlan {
                                order: task_partitions
                                    .iter()
                                    .map(|p| {
                                        p.iter()
                                            .map(|t| {
                                                (t.data_file_path().to_string(), t.start, t.length)
                                            })
                                            .collect()
                                    })
                                    .collect(),
                                clusters: plan.partition_clusters.clone(),
                                descending: plan.descending,
                                reverse: plan.reverse,
                                overlap_partition_count: decision.overlap_partition_count,
                                overlap_streams_total: decision.overlap_streams_total,
                            },
                        );
                    }
                    decision
                }
            }
        };
        let preserve_task_order = ordering.plan.is_some();
        // #4 scan parallelism: a pushed `LIMIT` collapses the scan to one task
        // partition (`partition_file_tasks`) so an ORDERED early-stop can drain a
        // single in-order stream. When `scan_output_ordering` advertises NO
        // ordering, result order is irrelevant — so re-split that one partition
        // across `target_partitions` for parallel file opens. Each partition still
        // carries the pushed `limit` and early-stops; the parent CoalescePartitions
        // + GlobalLimitExec truncate the union, so the result stays correct (at the
        // cost of bounded over-fetch). Only fires for the unordered-limit shape;
        // the ordered path keeps its single in-order partition untouched.
        if !preserve_task_order && limit.is_some() && task_partitions.len() == 1 {
            task_partitions = partition_file_tasks(
                task_partitions.into_iter().flatten().collect(),
                None,
                state.config().target_partitions(),
                tuning,
            );
        }
        let partition_count = task_partitions.len().max(1);
        // #4865: stage the start of an UNORDERED, row-clipped `LIMIT n` scan.
        // Scoped by query shape, not by layout: `ClippedScanLimit` is set only
        // where every operator between the scan and the limit passes rows
        // through unchanged (`sql.rs::clipping_scan_limit` — one plain table, no
        // join/CTE/GROUP BY/DISTINCT/aggregate/window/subquery, no ORDER BY), so
        // the rows this scan owes really are `n`. Ordered scans keep their
        // in-order drain and aggregates never carry the hint, so both are
        // untouched. See [`ScanAdmission`] for why waiting cannot drop a row.
        let clipped_admission = {
            let wave = clipped_admission_wave_for(state);
            (!preserve_task_order
                && partition_count > 1
                && wave > 0
                && state.config().get_extension::<ClippedScanLimit>().is_some())
            .then(|| Arc::new(ScanAdmission::new(partition_count, wave)))
        };
        let (sort_descending, reverse_scan, partition_clusters) = ordering
            .plan
            .as_ref()
            .map(|o| (o.descending, o.reverse, o.partition_clusters.clone()))
            .unwrap_or((false, false, Vec::new()));
        let eq_properties = match ordering.plan {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(output_schema.clone(), [ordering.exprs])
            }
            None => EquivalenceProperties::new(output_schema.clone()),
        };
        let plan_properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let decoded_budget_bytes = scan_decoded_budget_bytes();
        let decompression_factor = scan_decompression_factor();
        let aggregate_reader_budget = clamp_aggregate_reader_budget(
            effective_file_concurrency_limit(partition_count, tuning)
                .saturating_mul(partition_count),
            planned_files,
            planned_bytes,
            decoded_budget_bytes,
            decompression_factor,
        );
        // Ordered drains need ONE open stream per partition for LIVENESS: the
        // SortPreservingMerge above the scan polls every partition for a
        // first batch before it can emit anything, so a memory-derived budget
        // below partition_count doesn't bound memory — it convoys the merge
        // (uniform ~600ms warm first-batches) and can starve a partition past
        // the request timeout (the 07-12 diag's 60s windowed-browse 504,
        // budget clamped to 2 by a whole-file decode estimate). The estimate
        // itself doesn't apply here: the drain reads the projected columns
        // one row group at a time (~1MB observed vs the ~2GB whole-file
        // estimate that produced the clamp).
        let aggregate_reader_budget = ordered_reader_budget_floor(
            aggregate_reader_budget,
            preserve_task_order,
            partition_count,
        );
        let data_file_concurrency_limit = (aggregate_reader_budget / partition_count.max(1)).max(1);
        let est_decoded_per_file =
            estimated_decoded_bytes_per_file(planned_files, planned_bytes, decompression_factor);
        tracing::info!(
            planned_files,
            planned_bytes,
            partition_count,
            reader_budget = tuning.reader_budget,
            decoded_budget_bytes,
            decompression_factor,
            aggregate_reader_budget,
            est_decoded_per_file,
            batch_size = reader_tuning.batch_size,
            range_enabled = reader_tuning.range_enabled(),
            range_coalesce_bytes = reader_tuning.range_coalesce_bytes,
            range_fetch_concurrency = reader_tuning.range_fetch_concurrency,
            adaptive_min_bytes = tuning.range_adaptive_min_bytes,
            adaptive_min_files = tuning.range_adaptive_min_files,
            file_cache_enabled = file_cache_tuning.enabled(),
            file_cache_max_bytes = file_cache_tuning.max_bytes,
            file_cache_max_entries = file_cache_tuning.max_entries,
            scan_output_ordering_outcome = ordering.outcome,
            scan_output_reverse = reverse_scan,
            ordered_merge_overlap_partitions = ordering.overlap_partition_count,
            ordered_merge_overlap_streams_total = ordering.overlap_streams_total,
            ordered_merge_global_fan_in_budget = ordering.global_fan_in_budget,
            "siglake query scan reader tuning"
        );
        // Exact timestamp window for index-only min/max(timestamp). Only when the
        // scan is unfiltered (else boundary files only partially match, so the
        // file bounds aren't the answer) and `timestamp` is in the output schema.
        let ts_bounds = if !has_pushed_filters && raw_prune_spec.is_none() {
            match output_schema
                .field_with_name("timestamp")
                .ok()
                .and_then(iceberg_field_id)
            {
                Some(fid) => global_timestamp_bounds(&table, snapshot_id, fid).await,
                None => None,
            }
        } else {
            None
        };
        let ordering_outcome = ordering.outcome;

        Ok(Self {
            table,
            plan_properties,
            projection,
            projected_columns,
            limit,
            has_pushed_filters,
            predicates,
            raw_prune_spec,
            promoted_prune,
            task_partitions: Arc::new(task_partitions),
            // Captured here rather than read in execute(): a plan is executed
            // through whatever `TaskContext` the caller supplies, and only
            // one of them is the session's. The mid-flight collector's now is
            // (#2251), but a bare `TaskContext::default()` — which is what it
            // used, and what any hand-rolled `execute_stream` still is —
            // carries no session extensions at all, and a cancellation that
            // depends on the caller's choice of context is not a cancellation.
            cancel: state
                .config()
                .get_extension::<QueryCancel>()
                .map(|c| (*c).clone()),
            data_file_concurrency_limit,
            reader_tuning,
            file_cache_tuning,
            metrics: ExecutionPlanMetricsSet::new(),
            ts_bounds,
            preserve_task_order,
            sort_descending,
            reverse_scan,
            partition_clusters: Arc::new(partition_clusters),
            ordering_outcome,
            residual_filtered: filtered_scan,
            partition_tracker: Arc::new(ScanPartitionTracker::default()),
            admission: clipped_admission,
            ordered_limit: state
                .config()
                .get_extension::<OrderedScanLimit>()
                .map(|l| l.limit)
                .filter(|&l| l > 0),
            ordered_source_limit_safe: preserve_task_order
                && (!has_pushed_filters || time_only_filters(filters)),
            text_index_decline,
        })
    }

    /// Partition streams this node has handed out that have not yet finished
    /// (folded their counters). Non-zero when the node's metrics are read
    /// while an early-stopped scan is still unwinding — the caller's
    /// attribution snapshot is missing that many partitions' counters.
    pub fn live_partitions(&self) -> usize {
        self.partition_tracker.live()
    }

    /// See the `ordering_outcome` field: "advertised" or the gate's refusal
    /// reason for this scan.
    pub fn ordering_outcome(&self) -> &'static str {
        self.ordering_outcome
    }

    /// WS-3: build a sorted partition stream by k-way merging the per-file
    /// batch streams on `timestamp` in the declared direction — for partitions
    /// whose files OVERLAP in time (a disjoint run streams by plain ordered
    /// concat instead). Each per-file stream is internally sorted (storage
    /// invariant), so the merge yields a sorted partition; fan-in is bounded
    /// at planning by [`ORDERED_MERGE_MAX_FANIN`]. Streams fetch lazily on
    /// poll, so an early-stopping `LIMIT` above only decodes each file's head.
    /// One task's batch stream in scan order (reverse-aware). Construction is
    /// cheap and IO is lazy — nothing is fetched until the stream is polled.
    fn ordered_task_stream(
        &self,
        task: FileScanTask,
        file_io: iceberg::io::FileIO,
        _fetched_byte_counter: Arc<std::sync::atomic::AtomicU64>,
        scan_counters: Arc<ScanCounters>,
    ) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
        let reader_tuning = self.reader_tuning;
        let raw_prune_spec = self.raw_prune_spec.clone();
        let promoted_prune = self.promoted_prune.clone();
        let reverse_scan = self.reverse_scan;
        // #90: NEVER route an ordered per-task stream through the batch
        // cache — its fill decodes the WHOLE task before the first batch
        // (see the execute-path bypass), which defeats the lazy early-stop
        // this stream exists for and re-pays aborted fills on every browse
        // (the 07-12 windowed 504s came through this k-way-merge path after
        // the execute-path bypass landed). Ordered reads stream, always.
        {
            let task_stream: FileScanTaskStream =
                futures::stream::iter(std::iter::once(Ok(task))).boxed();
            let mut reader = ArrowReaderBuilder::new(file_io, iceberg::Runtime::current())
                .with_data_file_concurrency_limit(1)
                .with_row_selection_enabled(true)
                .with_scan_counters(Some(scan_counters))
                .with_raw_prune_spec(raw_prune_spec)
                .with_promoted_prune(promoted_prune);
            if reverse_scan {
                reader = reader.with_reverse(true);
            }
            if let Some(batch_size) = reader_tuning.batch_size {
                reader = reader.with_batch_size(batch_size.max(1));
            }
            if let Some(range_coalesce_bytes) = reader_tuning.range_coalesce_bytes {
                reader = reader.with_range_coalesce_bytes(range_coalesce_bytes.max(1));
            }
            if let Some(range_fetch_concurrency) = reader_tuning.range_fetch_concurrency {
                reader = reader.with_range_fetch_concurrency(range_fetch_concurrency.max(1));
            }
            if let Some(rows) = reader_tuning.reversed_chunk_rows {
                reader = reader.with_reversed_chunk_rows(rows);
            }
            if reader_tuning.bypass_reader_caches {
                reader = reader.with_cache_bypass(true);
            }
            Ok(reader
                .build()
                .read(task_stream)
                .map_err(|e| DataFusionError::External(e.into()))?
                .stream()
                .map(|result| result.map_err(|e| DataFusionError::External(e.into())))
                .boxed())
        }
    }

    /// A time-disjoint run of tasks streamed in order. Batches are emitted
    /// strictly in task order, but when the scan carries a residual filter the
    /// drain PREFETCHES ahead (byte-budgeted): a filtered ordered LIMIT must
    /// probe many pruned files before accumulating its rows, and doing that
    /// one file at a time serialized 748 S3 probes into multi-second FTS
    /// queries on the 1TB re-run (keyword 0.24s → 9s). An unfiltered ordered
    /// browse reads only the head, so it keeps the single-open-file shape.
    fn sequential_task_chain(
        &self,
        tasks: Vec<FileScanTask>,
        file_io: iceberg::io::FileIO,
        fetched_byte_counter: Arc<std::sync::atomic::AtomicU64>,
        scan_counters: Arc<ScanCounters>,
    ) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
        // Prefetch only for RESIDUAL filters (dimensional/FTS probes over
        // pruned files). A time-only window is a pushed predicate too, but it
        // fills from the head like an unfiltered browse — prefetching there
        // multiplied a windowed browse's leaf output by open-streams ×
        // buffered-batches (0.5–2.5M rows for a LIMIT 100 on the 200G
        // converged layout, layout-dependent), which is exactly what crossed
        // the mid-flight rows cap in the 07-16 windowed-browse 413s. Under
        // the current policy residual-filtered scans never advertise
        // ordering, so this chain sees them only if that policy changes —
        // the trigger stays for that case.
        let filtered = self.residual_filtered;
        let concurrency = if filtered { tasks.len().clamp(1, 8) } else { 1 };
        let mut futs: Vec<BoxFuture<'static, DFResult<TaskBatchStream>>> =
            Vec::with_capacity(tasks.len());
        for task in tasks {
            let stream = self.ordered_task_stream(
                task,
                file_io.clone(),
                fetched_byte_counter.clone(),
                scan_counters.clone(),
            )?;
            futs.push(Box::pin(async move { Ok(stream) }));
        }
        Ok(OrderedTaskDrain::with_budget_bytes(
            futures::stream::iter(futs),
            concurrency,
            "ordered_chain",
            self.reader_tuning
                .ordered_drain_buffer_bytes
                .unwrap_or_else(ordered_drain_buffer_bytes),
        )
        .boxed())
    }

    /// [`OrderedCluster::Sort`]: decode every file in the cluster (bounded
    /// concurrency, arrival order irrelevant), then sort the whole cluster in
    /// memory on `timestamp` in the scan direction. Chosen at planning only
    /// for clusters whose overlap depth exceeds the merge fan-in cap but
    /// whose rows fit [`ordered_sort_cluster_max_rows`]. The row gate limits
    /// work; [`eager_sort_record_batch_stream`] accounts the actual Arrow
    /// bytes against the shared query pool. Emits a single sorted batch — the
    /// cluster plays as one segment of its partition's time-disjoint sequence.
    fn eager_sorted_cluster_stream(
        &self,
        tasks: Vec<FileScanTask>,
        file_io: iceberg::io::FileIO,
        fetched_byte_counter: Arc<std::sync::atomic::AtomicU64>,
        scan_counters: Arc<ScanCounters>,
        schema: ArrowSchemaRef,
        reservation: datafusion::execution::memory_pool::MemoryReservation,
    ) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
        let mut streams = Vec::with_capacity(tasks.len());
        for task in tasks {
            streams.push(self.ordered_task_stream(
                task,
                file_io.clone(),
                fetched_byte_counter.clone(),
                scan_counters.clone(),
            )?);
        }
        Ok(eager_sort_record_batch_stream(
            streams,
            schema,
            self.sort_descending,
            reservation,
        ))
    }

    /// K-way merge of already-sorted batch streams on `timestamp` in the scan
    /// direction (the WS-3 per-cluster merge). For a source-safe ordered LIMIT,
    /// inputs are admitted in manifest-bound order. Once the buffered nth row
    /// is strictly ahead of the next input's bound, that input and the older
    /// suffix stay unopened; equal or unavailable bounds are never excluded.
    fn merge_record_streams(
        &self,
        mut inputs: Vec<OrderedMergeInput>,
        partition: usize,
        context: &TaskContext,
        schema: ArrowSchemaRef,
    ) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
        let reader_tuning = self.reader_tuning;
        let col = Column::new_with_schema("timestamp", schema.as_ref())?;
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(col),
            SortOptions {
                descending: self.sort_descending,
                nulls_first: self.sort_descending,
            },
        )])
        .ok_or_else(|| DataFusionError::Internal("empty ordered-merge ordering".into()))?;
        let reservation = MemoryConsumer::new(format!("SiglakeOrderedMerge[{partition}]"))
            .register(context.memory_pool());
        metrics::counter!("siglake_query_scan_ordered_merge_streams_total")
            .increment(inputs.len() as u64);
        let limit = self.ordered_limit.filter(|&limit| {
            let cap = ordered_single_partition_max_limit();
            self.ordered_source_limit_safe && cap > 0 && limit <= cap
        });
        if limit.is_none() || inputs.iter().any(|input| input.frontier_bound.is_none()) {
            return Ok(build_ordered_merge(
                inputs.into_iter().map(|input| input.stream).collect(),
                schema,
                &ordering,
                partition,
                reader_tuning.batch_size.unwrap_or(8192).max(1),
                reservation,
                None,
            )?
            .boxed());
        }

        if self.sort_descending {
            inputs.sort_by_key(|input| Reverse(input.frontier_bound));
        } else {
            inputs.sort_by_key(|input| input.frontier_bound);
        }
        let descending = self.sort_descending;
        let batch_size = reader_tuning
            .batch_size
            .unwrap_or(8192)
            .max(1)
            .min(limit.expect("checked above"));
        let prepare_schema = schema.clone();
        let prepared = async move {
            let admitted = frontier_pruned_merge_inputs(
                inputs,
                limit.expect("checked above"),
                descending,
                prepare_schema.clone(),
            )
            .await?;
            let col = Column::new_with_schema("timestamp", prepare_schema.as_ref())?;
            let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
                Arc::new(col),
                SortOptions {
                    descending,
                    nulls_first: descending,
                },
            )])
            .ok_or_else(|| DataFusionError::Internal("empty ordered-merge ordering".into()))?;
            build_ordered_merge(
                admitted,
                prepare_schema,
                &ordering,
                partition,
                batch_size,
                reservation,
                limit,
            )
        };
        Ok(futures::stream::once(prepared).try_flatten().boxed())
    }
}

fn build_ordered_merge(
    inputs: Vec<SendableRecordBatchStream>,
    schema: ArrowSchemaRef,
    ordering: &LexOrdering,
    partition: usize,
    batch_size: usize,
    reservation: datafusion::execution::memory_pool::MemoryReservation,
    fetch: Option<usize>,
) -> DFResult<SendableRecordBatchStream> {
    StreamingMergeBuilder::new()
        .with_streams(inputs)
        .with_schema(schema)
        .with_expressions(ordering)
        .with_metrics(BaselineMetrics::new(
            &ExecutionPlanMetricsSet::new(),
            partition,
        ))
        .with_batch_size(batch_size)
        .with_fetch(fetch)
        .with_reservation(reservation)
        .build()
}

/// Buffer at most `limit` rows from each newly admitted input. Those prefixes
/// contain every row that input could contribute to the global top N. Once the
/// exact nth value is strictly ahead of the next bound, all remaining inputs
/// are safe to leave unopened. Returning finite prefix streams also makes the
/// source-level limit explicit: this path is reached only when pushed filters
/// cannot remove rows above the scan.
async fn frontier_pruned_merge_inputs(
    inputs: Vec<OrderedMergeInput>,
    limit: usize,
    descending: bool,
    schema: ArrowSchemaRef,
) -> DFResult<Vec<SendableRecordBatchStream>> {
    let timestamp_idx = schema.index_of("timestamp")?;
    let mut admitted = Vec::new();
    let mut frontier_values = Vec::new();
    let mut inputs = inputs.into_iter().peekable();

    while let Some(mut input) = inputs.next() {
        let mut batches = Vec::new();
        let mut rows = 0usize;
        while rows < limit {
            let Some(batch) = input.stream.next().await.transpose()? else {
                break;
            };
            let take = (limit - rows).min(batch.num_rows());
            if take == 0 {
                continue;
            }
            let prefix = batch.slice(0, take);
            let Some(values) = timestamp_bound_values(&prefix, timestamp_idx) else {
                batches.push(batch);
                let mut all = admitted;
                all.push(Box::pin(RecordBatchStreamAdapter::new(
                    schema.clone(),
                    futures::stream::iter(batches.into_iter().map(Ok)).chain(input.stream),
                )) as SendableRecordBatchStream);
                for remaining in inputs {
                    all.push(remaining.stream);
                }
                return Ok(all);
            };
            frontier_values.extend(values);
            rows += take;
            batches.push(prefix);
        }
        admitted.push(Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        )) as SendableRecordBatchStream);

        if frontier_values.len() < limit {
            continue;
        }
        let nth = nth_ordered_value(&mut frontier_values, limit, descending);
        if bound_is_strictly_behind(
            nth,
            inputs.peek().and_then(|next| next.frontier_bound),
            descending,
        ) {
            break;
        }
    }
    Ok(admitted)
}

fn timestamp_bound_values(batch: &RecordBatch, timestamp_idx: usize) -> Option<Vec<i64>> {
    use datafusion::arrow::array::{Array, TimestampMicrosecondArray};

    let column = batch
        .column(timestamp_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()?;
    if column.null_count() != 0 {
        return None;
    }
    Some((0..column.len()).map(|row| column.value(row)).collect())
}

fn nth_ordered_value(values: &mut [i64], limit: usize, descending: bool) -> i64 {
    let nth = limit - 1;
    if descending {
        *values.select_nth_unstable_by(nth, |a, b| b.cmp(a)).1
    } else {
        *values.select_nth_unstable(nth).1
    }
}

fn bound_is_strictly_behind(nth: i64, bound: Option<i64>, descending: bool) -> bool {
    bound.is_some_and(|bound| if descending { bound < nth } else { bound > nth })
}

/// The physical output ordering to advertise for this scan, or a refusal when
/// it can't be proven. WS-3 early-stop: when a scan provably emits rows in
/// `timestamp ASC` order, DataFusion can elide a redundant `Sort` and turn an
/// ordered `LIMIT` into a `SortPreservingMerge` that streams just enough rows.
///
/// Soundness — all three must hold, else we return `None` (the optimizer keeps
/// its `Sort`, correct but not early-stopping):
/// Whether every pushed filter references ONLY the `timestamp` column (the
/// gate's required sort lead). Such predicates are time windows — consumed by
/// manifest file-pruning, invisible to row order within the surviving files —
/// so they don't disqualify ordered advertisement the way residual
/// dimensional/FTS filters do. Empty column sets (literal-only exprs) count
/// as NOT time-only, conservatively.
fn time_only_filters(filters: &[Expr]) -> bool {
    filters.iter().all(|f| {
        let mut cols = std::collections::HashSet::new();
        datafusion::logical_expr::utils::expr_to_columns(f, &mut cols).is_ok()
            && !cols.is_empty()
            && cols.iter().all(|c| c.name == "timestamp")
    })
}

/// 1. The table's declared Iceberg `SortOrder` leads with `timestamp` ascending
///    under the identity transform — the time-ordered-storage invariant means
///    every data file is physically `timestamp ASC` with a Parquet
///    `SortingColumn` footer (see `DESIGN_time_ordered_storage.md`).
/// 2. `timestamp` survives the projection (it's in the output schema), so the
///    advertised sort column exists.
/// 3. **Every task partition streams sorted.** A single file qualifies
///    trivially. A multi-file partition whose files form a time-DISJOINT run
///    is REARRANGED into that run (per-file `timestamp` bounds from manifest
///    stats; equal boundaries still concatenate in order) and drained in task
///    order — plain ordered concat. A multi-file partition whose files
///    OVERLAP is marked for a **k-way per-partition streaming merge** at
///    execute time (each file is internally sorted per the declared order, so
///    merging the per-file streams yields a sorted partition) — bounded by
///    [`ORDERED_MERGE_MAX_FANIN`] open streams; beyond that, or when a file
///    is missing its manifest bounds, the advertisement is refused — correct,
///    just not early-stopping. (A byte-range-split file — two tasks sharing a
///    path, identical bounds — routes to the merge, which handles it.)
///
/// Direction follows the table's DECLARED lead direction: fresh siglake tables
/// declare `timestamp ASC`, but the long-lived production `events` table still
/// carries the legacy `timestamp DESC` order — and the writer sorts by the
/// declared order, so its files really are DESC inside (round-74 finding).
/// Advertising the declared direction lets `ORDER BY timestamp DESC LIMIT n`
/// (newest-first — the canonical log search) early-stop on such tables.
async fn scan_output_ordering(
    table: &Table,
    snapshot_id: Option<i64>,
    output_schema: &ArrowSchemaRef,
    task_partitions: &mut Vec<Vec<FileScanTask>>,
    state: &dyn Session,
) -> ScanOrderingDecision {
    let global_fan_in_budget = ordered_merge_global_fanin_for(state);
    let merge_max_fan_in = ordered_merge_max_fanin_for(state);
    let sort_cluster_max_rows = ordered_sort_cluster_max_rows_for(state);
    // (2) timestamp projected.
    let idx = match output_schema.index_of("timestamp") {
        Ok(idx) => idx,
        Err(_) => {
            return ScanOrderingDecision::refused("not_projected", 0, 0, global_fan_in_budget);
        }
    };
    // (1) declared sort order leads with timestamp under the identity
    // transform; the declared direction is the direction we advertise.
    let meta = table.metadata();
    let iceberg_schema = meta.current_schema();
    let Some(lead) = meta.default_sort_order().fields.first() else {
        return ScanOrderingDecision::refused("no_sort_order", 0, 0, global_fan_in_budget);
    };
    if lead.transform != iceberg::spec::Transform::Identity {
        return ScanOrderingDecision::refused("non_identity_sort", 0, 0, global_fan_in_budget);
    }
    let declared_descending = lead.direction == iceberg::spec::SortDirection::Descending;
    let Some(lead_field) = iceberg_schema.field_by_id(lead.source_id) else {
        return ScanOrderingDecision::refused("missing_sort_field", 0, 0, global_fan_in_budget);
    };
    if lead_field.name != "timestamp" {
        return ScanOrderingDecision::refused("non_timestamp_sort", 0, 0, global_fan_in_budget);
    }
    // (1b) On-disk direction. A table that has ever REPLACED its sort order
    // (the DESC→ASC convergence migration) can hold files written under
    // either direction — the declared order only describes new writes.
    // Attribute each scanned file to its manifest-stamped `sort_order_id`
    // (pre-stamping files fall back to the recorded legacy order, then the
    // declared default) and advertise only when every file agrees; a mixed
    // set refuses (browses fall to TopK, still correct) until re-clustering
    // rewrites the stragglers. Single-order tables skip the walk entirely —
    // no ambiguity, no cost — preserving the round-74 declared-direction
    // behavior.
    let disk_descending = if meta.sort_orders_iter().len() <= 1 {
        declared_descending
    } else {
        let Some(ids) = per_file_sort_order_ids(table, snapshot_id).await else {
            metrics::counter!(
                "siglake_query_scan_output_ordering_total",
                "outcome" => "order_walk_failed"
            )
            .increment(1);
            return ScanOrderingDecision::refused("order_walk_failed", 0, 0, global_fan_in_budget);
        };
        let legacy_id = meta
            .properties()
            .get(crate::iceberg::LEGACY_SORT_ORDER_PROP)
            .and_then(|v| v.parse::<i64>().ok());
        let mut dir: Option<bool> = None;
        for task in task_partitions.iter().flatten() {
            let id = ids
                .get(task.data_file_path())
                .copied()
                .flatten()
                .map(i64::from)
                .or(legacy_id)
                .unwrap_or_else(|| meta.default_sort_order_id());
            let file_dir = meta
                .sort_order_by_id(id)
                .and_then(|order| order.fields.first())
                .filter(|f| {
                    f.transform == iceberg::spec::Transform::Identity
                        && f.source_id == lead.source_id
                })
                .map(|f| f.direction == iceberg::spec::SortDirection::Descending);
            let Some(file_dir) = file_dir else {
                metrics::counter!(
                    "siglake_query_scan_output_ordering_total",
                    "outcome" => "unknown_sort_order"
                )
                .increment(1);
                return ScanOrderingDecision::refused(
                    "unknown_sort_order",
                    0,
                    0,
                    global_fan_in_budget,
                );
            };
            match dir {
                None => dir = Some(file_dir),
                Some(prev) if prev != file_dir => {
                    metrics::counter!(
                        "siglake_query_scan_output_ordering_total",
                        "outcome" => "mixed_sort_direction"
                    )
                    .increment(1);
                    return ScanOrderingDecision::refused(
                        "mixed_sort_direction",
                        0,
                        0,
                        global_fan_in_budget,
                    );
                }
                _ => {}
            }
        }
        dir.unwrap_or(declared_descending)
    };
    let requested_descending = state
        .config()
        .get_extension::<PreferredScanOrder>()
        .map(|order| order.descending)
        .unwrap_or(disk_descending);
    let reverse = requested_descending != disk_descending;
    // (3) rearrange the file set for ordered streaming. Each partition becomes
    // a time-disjoint sequence of CLUSTERS: singleton clusters concat,
    // multi-file (overlapping) clusters k-way merge — fan-in bounded by the
    // LARGEST cluster (clusters play sequentially). Because DataFusion holds
    // an ordered LIMIT at the sort (never pushed into the scan), ordered plans
    // arrive with the normal weight-balanced partitioning, which interleaves
    // time ranges across partitions and inflates every partition's overlap —
    // the 1TB round refused every ordered query on the global budget that
    // way. So when ordering is requestable, RE-SPLIT the files
    // time-contiguously (row-balanced chunks of the same partition count) and,
    // if the concurrent-stream budget still doesn't fit (SortPreservingMerge
    // polls every partition, so Σ per-partition max-cluster streams can be
    // open at once), coalesce adjacent partitions — trading scan parallelism,
    // which an early-stopping ordered query doesn't use anyway, for the
    // advertisement.
    let mut partition_clusters: Vec<Vec<OrderedCluster>> = vec![Vec::new(); task_partitions.len()];
    let mut overlap_partition_count = 0usize;
    let mut overlap_streams_total = 0usize;
    // Small ordered LIMITs also need the rearrangement when the balanced split
    // already gave every partition ONE file (`planned_files <=
    // target_partitions`). Nothing about that shape is ordered-friendly: the
    // SPM above opens one independent scan stream per file and drains each of
    // them a head chunk at a time, so a `LIMIT 100` over five overlapping
    // level-0 files pays five separate drains and no merge (run 83's
    // `windowed_browse_last25`: 5 planned files, 5 partitions,
    // `ordered_merge_overlap_partitions=0`, 371,613 rows scanned on average
    // for 100 rows). Arranging them from manifest bounds coalesces the files
    // into ONE partition whose lazy k-way merge reads each overlapping file's
    // head and stops when the limit is met.
    let small_limit = state
        .config()
        .get_extension::<OrderedScanLimit>()
        .map(|l| {
            let cap = ordered_single_partition_max_limit();
            cap > 0 && l.limit <= cap
        })
        .unwrap_or(false);
    let planned_files: usize = task_partitions.iter().map(|p| p.len()).sum();
    let singleton_only = !task_partitions.iter().any(|p| p.len() > 1);
    // A singleton-only scan advertises ordering today WITHOUT ever reading
    // manifest bounds, so every gate the arrangement can trip (missing bounds,
    // cluster depth over the per-merge cap, the global stream budget) would be
    // a NEW refusal — a browse that streams today would fall to a whole-window
    // TopK. Keep the pre-arrangement partitioning in hand and restore it
    // instead: this path may only improve the plan, never withdraw it.
    let singleton_fallback: Option<Vec<Vec<FileScanTask>>> =
        (singleton_only && small_limit && planned_files > 1).then(|| task_partitions.clone());
    let mut fell_back_to_singletons: Option<&'static str> = None;
    'arrange: {
        if singleton_fallback.is_none() && singleton_only {
            break 'arrange;
        }
        let bounds = per_file_timestamp_bounds(table, snapshot_id, lead_field.id).await;
        let n_parts_orig = task_partitions.len().max(1);
        // Any file with missing bounds refuses the whole advertisement (a
        // boundless file can't be placed in time) — checked BEFORE touching
        // the partitioning so the fallback scan keeps its balanced split.
        if task_partitions
            .iter()
            .flatten()
            .any(|t| !bounds.contains_key(t.data_file_path()))
        {
            if singleton_fallback.is_some() {
                fell_back_to_singletons = Some("no_bounds");
                break 'arrange;
            }
            metrics::counter!(
                "siglake_query_scan_output_ordering_total",
                "outcome" => "no_bounds"
            )
            .increment(1);
            return ScanOrderingDecision::refused(
                "no_bounds",
                overlap_partition_count,
                overlap_streams_total,
                global_fan_in_budget,
            );
        }
        let mut keyed: Vec<(FileScanTask, (i64, i64))> = std::mem::take(task_partitions)
            .into_iter()
            .flatten()
            .map(|task| {
                let b = *bounds
                    .get(task.data_file_path())
                    .expect("bounds presence checked above");
                (task, b)
            })
            .collect();
        // Time-contiguous order in the requested direction.
        if requested_descending {
            keyed.sort_by_key(|(_, b)| Reverse((b.1, b.0)));
        } else {
            keyed.sort_by_key(|(_, b)| *b);
        }
        // Row-balanced contiguous chunks at the original partition count —
        // except for SMALL-limit ordered scans, which coalesce to ONE
        // partition up front: the SPM above polls every partition for a
        // first batch, so partitions are pure overhead for a browse that
        // early-stops after ~LIMIT rows.
        let n_parts = if small_limit {
            1
        } else {
            n_parts_orig.min(keyed.len().max(1))
        };
        let total_rows: u64 = keyed
            .iter()
            .map(|(t, _)| t.record_count.unwrap_or(t.length))
            .sum::<u64>()
            .max(1);
        let per_part = total_rows.div_ceil(n_parts as u64).max(1);
        let mut parts: Vec<Vec<(FileScanTask, (i64, i64))>> = Vec::with_capacity(n_parts);
        let mut current: Vec<(FileScanTask, (i64, i64))> = Vec::new();
        let mut acc = 0u64;
        for item in keyed {
            let w = item.0.record_count.unwrap_or(item.0.length);
            current.push(item);
            acc += w;
            if acc >= per_part && parts.len() + 1 < n_parts {
                parts.push(std::mem::take(&mut current));
                acc = 0;
            }
        }
        if !current.is_empty() {
            parts.push(current);
        }
        // Cluster each partition, LAYER each multi-file cluster (merge fan-in
        // = overlap depth), and coalesce adjacent partitions until the
        // concurrent-stream budget fits. Terminates: a single partition's
        // budget use is its max depth ≤ the per-merge cap ≤ the budget.
        loop {
            let mut plans_per_part: Vec<Vec<OrderedCluster>> = Vec::with_capacity(parts.len());
            let mut budget_used = 0usize;
            let mut refused_depth: Option<usize> = None;
            for part in &mut parts {
                let b: Vec<(i64, i64)> = part.iter().map(|(_, b)| *b).collect();
                let (perm, cluster_sizes) = cluster_partition(&b, requested_descending);
                // Apply the cluster permutation; idempotent under re-coalescing
                // (concatenating two permuted contiguous chunks re-clusters to
                // another valid arrangement).
                let mut tasks: Vec<Option<(FileScanTask, (i64, i64))>> =
                    std::mem::take(part).into_iter().map(Some).collect();
                *part = perm
                    .into_iter()
                    .map(|i| tasks[i].take().expect("permutation indexes each task once"))
                    .collect();
                // Layer each multi-file cluster in place.
                let mut plan: Vec<OrderedCluster> = Vec::with_capacity(cluster_sizes.len());
                let mut part_streams = 1usize;
                let mut off = 0usize;
                for &cs in &cluster_sizes {
                    if cs <= 1 {
                        plan.push(OrderedCluster::Run(1));
                        off += cs;
                        continue;
                    }
                    let cb: Vec<(i64, i64)> = part[off..off + cs].iter().map(|(_, b)| *b).collect();
                    let (lperm, layer_sizes) = layer_cluster(&cb, requested_descending);
                    let mut seg: Vec<Option<(FileScanTask, (i64, i64))>> =
                        part[off..off + cs].iter().cloned().map(Some).collect();
                    for (slot, &src) in part[off..off + cs].iter_mut().zip(lperm.iter()) {
                        *slot = seg[src].take().expect("layer permutation indexes once");
                    }
                    if layer_sizes.len() > merge_max_fan_in {
                        // Depth spike past the per-merge cap. When the cluster
                        // is SMALL (the newest tail: fresh commits piling up
                        // between compaction passes), an eager decode+sort of
                        // just that cluster preserves the whole scan's ordered
                        // advertisement — refusing here turned the canonical
                        // trailing-window browse into a full-window TopK that
                        // tripped the rows breaker (07-16 windowed-browse
                        // 413s). Oversized clusters still refuse: the eager
                        // sort holds the cluster decoded in memory.
                        let rows = part[off..off + cs]
                            .iter()
                            .map(|(t, _)| t.record_count)
                            .sum::<Option<u64>>();
                        match rows {
                            Some(r) if r <= sort_cluster_max_rows && sort_cluster_max_rows > 0 => {
                                metrics::counter!("siglake_query_scan_ordered_sort_clusters_total")
                                    .increment(1);
                                plan.push(OrderedCluster::Sort(cs));
                                part_streams =
                                    part_streams.max(cs.min(SORT_CLUSTER_DECODE_CONCURRENCY));
                                off += cs;
                                continue;
                            }
                            _ => {
                                refused_depth =
                                    Some(refused_depth.unwrap_or(0).max(layer_sizes.len()));
                            }
                        }
                    }
                    part_streams = part_streams.max(layer_sizes.len());
                    let mut layer_offset = off;
                    let layers = layer_sizes
                        .into_iter()
                        .map(|file_count| {
                            let layer = &part[layer_offset..layer_offset + file_count];
                            layer_offset += file_count;
                            let frontier_bound = if requested_descending {
                                layer
                                    .iter()
                                    .map(|(_, bounds)| bounds.1)
                                    .max()
                                    .expect("merge layer is non-empty")
                            } else {
                                layer
                                    .iter()
                                    .map(|(_, bounds)| bounds.0)
                                    .min()
                                    .expect("merge layer is non-empty")
                            };
                            OrderedMergeLayer {
                                file_count,
                                frontier_bound,
                            }
                        })
                        .collect();
                    plan.push(OrderedCluster::Merge(layers));
                    off += cs;
                }
                budget_used += part_streams;
                plans_per_part.push(plan);
            }
            if let Some(worst_depth) = refused_depth {
                if singleton_fallback.is_some() {
                    fell_back_to_singletons = Some("fan_in");
                    break 'arrange;
                }
                // One cluster's overlap DEPTH alone exceeds the per-merge cap
                // and it is too big for the eager-sort fallback.
                tracing::info!(
                    worst_depth,
                    cap = merge_max_fan_in,
                    partitions = parts.len(),
                    "ordered scan refused: cluster overlap depth exceeds merge fan-in cap"
                );
                *task_partitions = parts
                    .into_iter()
                    .map(|p| p.into_iter().map(|(t, _)| t).collect())
                    .collect();
                metrics::counter!(
                    "siglake_query_scan_output_ordering_total",
                    "outcome" => "fan_in"
                )
                .increment(1);
                return ScanOrderingDecision::refused(
                    "fan_in",
                    overlap_partition_count,
                    overlap_streams_total,
                    global_fan_in_budget,
                );
            }
            if budget_used <= global_fan_in_budget || parts.len() == 1 {
                overlap_streams_total = budget_used;
                overlap_partition_count = plans_per_part
                    .iter()
                    .filter(|plan| plan.iter().any(|c| !matches!(c, OrderedCluster::Run(_))))
                    .count();
                *task_partitions = parts
                    .into_iter()
                    .map(|p| p.into_iter().map(|(t, _)| t).collect())
                    .collect();
                partition_clusters = plans_per_part
                    .into_iter()
                    .map(|plan| {
                        if plan.iter().any(|c| !matches!(c, OrderedCluster::Run(_))) {
                            plan
                        } else {
                            Vec::new()
                        }
                    })
                    .collect();
                break;
            }
            // Coalesce adjacent pairs (time-contiguity is preserved) and retry.
            let mut merged: Vec<Vec<(FileScanTask, (i64, i64))>> =
                Vec::with_capacity(parts.len().div_ceil(2));
            let mut it = parts.into_iter();
            while let Some(mut a) = it.next() {
                if let Some(b) = it.next() {
                    a.extend(b);
                }
                merged.push(a);
            }
            parts = merged;
            metrics::counter!("siglake_query_scan_ordered_partition_coalesce_total").increment(1);
        }
    }
    if overlap_streams_total > global_fan_in_budget && singleton_fallback.is_some() {
        fell_back_to_singletons = Some("global_fan_in");
    }
    if let (Some(reason), Some(original)) = (fell_back_to_singletons, singleton_fallback) {
        // The arrangement could not be built within the gates; the scan keeps
        // the one-file-per-partition split it arrived with, which still
        // advertises ordering (each file's stream is sorted and the SPM above
        // merges them) at the pre-#4353 drain cost.
        tracing::debug!(
            reason,
            files = planned_files,
            "ordered scan kept its singleton partitions"
        );
        partition_clusters = vec![Vec::new(); original.len()];
        *task_partitions = original;
        overlap_partition_count = 0;
        overlap_streams_total = 0;
    }
    if overlap_streams_total > global_fan_in_budget {
        // Round-76 OOM: the legacy-DESC `events` table can pack many overlap
        // partitions under the per-partition cap and still explode globally.
        // Example: 8 partitions x up to 16 streams each = 128 open Parquet
        // decoders, each able to pin a decompressed row group, which OOM'd a
        // 12 GiB worker in seconds. Refusing the advertised ordering here
        // makes DataFusion keep a bounded `SortExec(fetch=n)` / TopK heap for
        // `ORDER BY ... LIMIT n`: still correct, slightly slower, but memory
        // bounded. Disjoint-run partitions do not count; they drain one stream
        // at a time and are already bytes-budgeted.
        metrics::counter!(
            "siglake_query_scan_output_ordering_total",
            "outcome" => "global_fan_in"
        )
        .increment(1);
        return ScanOrderingDecision::refused(
            "global_fan_in",
            overlap_partition_count,
            overlap_streams_total,
            global_fan_in_budget,
        );
    }
    let Some(col) = Column::new_with_schema("timestamp", output_schema).ok() else {
        metrics::counter!(
            "siglake_query_scan_output_ordering_total",
            "outcome" => "missing_sort_column"
        )
        .increment(1);
        return ScanOrderingDecision::refused(
            "missing_sort_column",
            overlap_partition_count,
            overlap_streams_total,
            global_fan_in_budget,
        );
    };
    debug_assert_eq!(col.index(), idx);
    // Charged once the plan is known to be advertised, not where the
    // arrangement is built: a plan the global-fan-in gate above refuses, or one
    // put back to its singleton partitions, runs no merge at all, and counting
    // its clusters overstated ordered merge planning (#4366). The restored
    // singleton case reads zero here because `overlap_partition_count` is reset
    // along with the partitioning.
    for _ in 0..overlap_partition_count {
        metrics::counter!("siglake_query_scan_ordered_merge_partitions_total").increment(1);
    }
    metrics::counter!(
        "siglake_query_scan_output_ordering_total",
        "outcome" => "advertised"
    )
    .increment(1);
    ScanOrderingDecision::advertised(
        ScanOrderingPlan {
            exprs: vec![PhysicalSortExpr::new(
                Arc::new(col),
                // Match the nulls order DataFusion's DEFAULT `ORDER BY` asks
                // for (DESC => NULLS FIRST, ASC => NULLS LAST). The "nulls are
                // academic on a non-nullable column" assumption only holds
                // when the ARROW schema says non-nullable — user-index tables
                // carry a NULLABLE timestamp, and there the equivalence check
                // compares SortOptions structurally, so a NULLS LAST
                // advertisement never satisfied an unadorned `ORDER BY
                // timestamp DESC` and every index browse fell to TopK (the
                // round-3 413s). Timestamps are never null at write, so the
                // claim is vacuously true in either direction.
                SortOptions {
                    descending: requested_descending,
                    nulls_first: requested_descending,
                },
            )],
            descending: requested_descending,
            reverse,
            partition_clusters,
        },
        overlap_partition_count,
        overlap_streams_total,
        global_fan_in_budget,
    )
}

/// Cap on the number of per-file streams a single partition's k-way ordered
/// merge will hold open concurrently. A `LIMIT`-only plan packs every file
/// into one partition, and an unbounded merge there would open a reader per
/// file; past this we refuse the ordering advertisement instead (blocking
/// sort — correct, and cheaper than thrashing the reader budget).
const ORDERED_MERGE_MAX_FANIN: usize = 16;

/// Per-merge fan-in cap (defaults to [`ORDERED_MERGE_MAX_FANIN`]). With
/// layered cluster merges the fan-in is the overlap DEPTH and each open
/// stream drains one file at a time, so memory-rich workers can safely raise
/// this — the 16 default is calibrated for small (~12 GiB) workers
/// (round-76 OOM).
fn ordered_merge_max_fanin() -> usize {
    std::env::var("SIGLAKE_ORDERED_MERGE_MAX_FANIN")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|&n| n >= 2)
        .unwrap_or_else(|| adaptive_fanin_default(ORDERED_MERGE_MAX_FANIN, 4, 64))
}

fn ordered_merge_max_fanin_for(state: &dyn Session) -> usize {
    state
        .config()
        .get_extension::<OrderedScanTuning>()
        .and_then(|tuning| tuning.merge_max_fan_in)
        .map(|fan_in| fan_in.max(2))
        .unwrap_or_else(ordered_merge_max_fanin)
}

struct ScanOrderingDecision {
    plan: Option<ScanOrderingPlan>,
    outcome: &'static str,
    overlap_partition_count: usize,
    overlap_streams_total: usize,
    global_fan_in_budget: usize,
}

impl ScanOrderingDecision {
    fn advertised(
        plan: ScanOrderingPlan,
        overlap_partition_count: usize,
        overlap_streams_total: usize,
        global_fan_in_budget: usize,
    ) -> Self {
        Self {
            plan: Some(plan),
            outcome: "advertised",
            overlap_partition_count,
            overlap_streams_total,
            global_fan_in_budget,
        }
    }

    fn refused(
        outcome: &'static str,
        overlap_partition_count: usize,
        overlap_streams_total: usize,
        global_fan_in_budget: usize,
    ) -> Self {
        Self {
            plan: None,
            outcome,
            overlap_partition_count,
            overlap_streams_total,
            global_fan_in_budget,
        }
    }
}

/// What [`scan_output_ordering`] decided: the sort exprs to advertise, the
/// declared direction, and each partition's cluster sizes (in task order —
/// empty when the partition streams by plain in-order concat; a multi-file
/// cluster k-way merges at execute, disjoint from its neighbors).
struct ScanOrderingPlan {
    exprs: Vec<PhysicalSortExpr>,
    descending: bool,
    reverse: bool,
    /// Partition → clusters (in task order). Empty per-partition vec = plain
    /// in-order concat. See [`OrderedCluster`] for the per-cluster execution
    /// shapes.
    partition_clusters: Vec<Vec<OrderedCluster>>,
}

/// How one time-disjoint CLUSTER of a partition's task list streams sorted at
/// execute. Clusters play sequentially in time order; the enum's file counts
/// consume the partition's task list front to back.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OrderedCluster {
    /// A time-disjoint run of this many files: ordered sequential chain.
    Run(usize),
    /// Overlapping files k-way merged over layers (each layer a sequential
    /// disjoint run); fan-in = layer count = overlap depth. Layer file counts
    /// sum to the cluster's file count, and their bounds drive lazy admission.
    Merge(Vec<OrderedMergeLayer>),
    /// Depth-spike fallback: overlap depth exceeds the per-merge fan-in cap
    /// but the cluster's rows fit [`ordered_sort_cluster_max_rows`] — decode
    /// all files (bounded concurrency, unordered) and sort in memory before
    /// emitting. Preserves the scan-wide ordered advertisement that a refusal
    /// would forfeit.
    Sort(usize),
}

/// One sequential, time-disjoint input to an overlapping-cluster merge.
/// `frontier_bound` is the input's first possible value in scan order (the
/// upper manifest bound for DESC, lower for ASC).
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderedMergeLayer {
    file_count: usize,
    frontier_bound: i64,
}

struct OrderedMergeInput {
    stream: SendableRecordBatchStream,
    frontier_bound: Option<i64>,
}

/// Row budget for [`OrderedCluster::Sort`] (`SIGLAKE_ORDERED_SORT_CLUSTER_MAX_ROWS`,
/// default 1 MiRows, `0` disables). Bounds its sort work; execution separately
/// admits the actual Arrow-byte peak through the shared query pool. The
/// depth-spiky newest tail this exists for (fresh commits accumulating between
/// compaction passes) is typically a few thousand rows.
fn ordered_sort_cluster_max_rows() -> u64 {
    std::env::var("SIGLAKE_ORDERED_SORT_CLUSTER_MAX_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(1_048_576)
}

fn ordered_sort_cluster_max_rows_for(state: &dyn Session) -> u64 {
    state
        .config()
        .get_extension::<OrderedScanTuning>()
        .and_then(|tuning| tuning.sort_cluster_max_rows)
        .unwrap_or_else(ordered_sort_cluster_max_rows)
}

/// Concurrent per-file decode streams inside one eager-sorted cluster.
const SORT_CLUSTER_DECODE_CONCURRENCY: usize = 8;

/// Per-row scratch reserved around Arrow 57's single-column timestamp sort.
/// `lexsort_to_indices` partitions rows into `u32` validity indices, sorts
/// `(u32, i64)` timestamp tuples, and builds a final `u32` index array. The
/// additional `usize` covers the generic lexsort index shape as headroom.
const SORT_CLUSTER_INDEX_WORKING_BYTES_PER_ROW: usize = std::mem::size_of::<usize>()
    + std::mem::size_of::<(u32, i64)>()
    + 2 * std::mem::size_of::<u32>();

/// Arrow buffers plus the batch and column-reference containers retained by
/// the eager collector. `get_array_memory_size` is deliberately conservative
/// for shared buffers, which is the safe direction for admission.
fn ordered_sort_batch_bytes(batch: &RecordBatch) -> Option<usize> {
    batch
        .get_array_memory_size()
        .checked_add(std::mem::size_of::<RecordBatch>())?
        .checked_add(
            batch
                .num_columns()
                .checked_mul(std::mem::size_of::<datafusion::arrow::array::ArrayRef>())?,
        )
}

/// Maximum live allocation during the eager transformation: collected input
/// plus one equally-sized concatenated/output representation, and the
/// timestamp sort's index/scratch vectors. The input's physical byte count is
/// an upper bound for a concatenation or full-row `take` of those same arrays.
fn ordered_sort_peak_bytes(retained_bytes: usize, rows: usize) -> Option<usize> {
    retained_bytes
        .checked_mul(2)?
        .checked_add(rows.checked_mul(SORT_CLUSTER_INDEX_WORKING_BYTES_PER_ROW)?)
}

fn try_resize_ordered_sort_reservation(
    reservation: &mut datafusion::execution::memory_pool::MemoryReservation,
    target: usize,
    stage: &'static str,
) -> DFResult<()> {
    reservation.try_resize(target).map_err(|source| {
        DataFusionError::Context(
            format!("OrderedCluster::Sort {stage} requires a {target}-byte query-pool reservation"),
            Box::new(source),
        )
    })
}

/// A one-batch stream which keeps the final Arrow output charged to the query
/// pool until its segment is dropped. Returning the reservation alongside the
/// future's batch is important: an ordinary `stream::once(async { ... })`
/// drops async locals before exposing the batch and would immediately make the
/// output unaccounted again.
struct ReservedEagerSortStream {
    future: Option<
        BoxFuture<
            'static,
            DFResult<(
                RecordBatch,
                datafusion::execution::memory_pool::MemoryReservation,
            )>,
        >,
    >,
    _output_reservation: Option<datafusion::execution::memory_pool::MemoryReservation>,
    emitted: bool,
}

impl Stream for ReservedEagerSortStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.emitted {
            return Poll::Ready(None);
        }
        let future = this
            .future
            .as_mut()
            .expect("eager-sort future exists until its only item is emitted");
        match future.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.future = None;
                this.emitted = true;
                Poll::Ready(Some(match result {
                    Ok((batch, reservation)) => {
                        this._output_reservation = Some(reservation);
                        Ok(batch)
                    }
                    Err(error) => Err(error),
                }))
            }
        }
    }
}

/// Collect and sort one [`OrderedCluster::Sort`] segment while charging every
/// retained representation to `reservation`. Collection grows incrementally;
/// the concatenation/sort/take peak is admitted in one preflight before any of
/// those allocations. Insufficient pool capacity therefore returns
/// `ResourcesExhausted` while the still-small retained input can be dropped.
fn eager_sort_record_batch_stream(
    streams: Vec<TaskBatchStream>,
    schema: ArrowSchemaRef,
    descending: bool,
    mut reservation: datafusion::execution::memory_pool::MemoryReservation,
) -> TaskBatchStream {
    let future = async move {
        use arrow::compute::{
            concat_batches, lexsort_to_indices, take_record_batch, SortColumn,
        };

        let mut batches = Vec::new();
        let mut retained_bytes = 0usize;
        let mut rows = 0usize;
        let mut decoded = futures::stream::iter(streams)
            .flatten_unordered(SORT_CLUSTER_DECODE_CONCURRENCY)
            .boxed();
        while let Some(batch) = decoded.next().await {
            let batch = batch?;
            let batch_bytes = ordered_sort_batch_bytes(&batch).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "OrderedCluster::Sort collection byte count overflowed usize".into(),
                )
            })?;
            retained_bytes = retained_bytes.checked_add(batch_bytes).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "OrderedCluster::Sort retained byte count overflowed usize".into(),
                )
            })?;
            rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "OrderedCluster::Sort row count overflowed usize".into(),
                )
            })?;
            try_resize_ordered_sort_reservation(
                &mut reservation,
                retained_bytes,
                "collection",
            )?;
            batches.push(batch);
        }

        let ts_idx = schema
            .index_of("timestamp")
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        if batches.is_empty() {
            let output = RecordBatch::new_empty(schema);
            let output_bytes = ordered_sort_batch_bytes(&output).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "OrderedCluster::Sort empty output byte count overflowed usize".into(),
                )
            })?;
            try_resize_ordered_sort_reservation(
                &mut reservation,
                output_bytes,
                "empty output",
            )?;
            return Ok((output, reservation));
        }

        let peak_bytes = ordered_sort_peak_bytes(retained_bytes, rows).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "OrderedCluster::Sort peak byte count overflowed usize".into(),
            )
        })?;
        try_resize_ordered_sort_reservation(&mut reservation, peak_bytes, "preflight")?;

        let combined = concat_batches(&schema, &batches)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        // The preflight covers input + concatenation. Release the input before
        // allocating indices/output so those later stages stay within the
        // same two-full-representation peak.
        drop(batches);
        let indices = lexsort_to_indices(
            &[SortColumn {
                values: combined.column(ts_idx).clone(),
                // Must match the advertised ordering (DESC ⇒ NULLS FIRST,
                // ASC ⇒ NULLS LAST — the aacbc66 structural-nulls rule).
                options: Some(SortOptions {
                    descending,
                    nulls_first: descending,
                }),
            }],
            None,
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let output = take_record_batch(&combined, &indices)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let output_bytes = ordered_sort_batch_bytes(&output).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "OrderedCluster::Sort output byte count overflowed usize".into(),
            )
        })?;
        if output_bytes > reservation.size() {
            return Err(DataFusionError::Internal(format!(
                "OrderedCluster::Sort peak estimate {peak_bytes} was smaller than its {output_bytes}-byte output"
            )));
        }

        drop(indices);
        drop(combined);
        reservation.shrink(reservation.size() - output_bytes);
        Ok((output, reservation))
    }
    .boxed();

    Box::pin(ReservedEagerSortStream {
        future: Some(future),
        _output_reservation: None,
        emitted: false,
    })
}

/// Snapshot-keyed cache of the ordering gate's ADVERTISED plan: the bounds
/// walk + time-contiguous re-split + cluster/layer computation is a pure
/// function of (table, snapshot, direction, small-limit bucket, exact task
/// set) — recomputing it cost every warm browse ~10–20ms of manifest IO and
/// arrangement work on 100+-file layouts (the dominant residual term in the
/// 07-18 browse cells). Entries are pure functions of immutable snapshots
/// (standing no-TTL invariant); the LRU simply ages old snapshots out.
/// Refusals are NOT cached (rare on settled layouts; some refusal branches
/// leave bespoke partition arrangements).
#[derive(Clone)]
struct CachedOrderedPlan {
    /// Partition → (path, start, length) in final task order.
    order: Vec<Vec<(String, u64, u64)>>,
    clusters: Vec<Vec<OrderedCluster>>,
    descending: bool,
    reverse: bool,
    overlap_partition_count: usize,
    overlap_streams_total: usize,
}

const ORDERED_PLAN_CACHE_CAP: usize = 128;

/// (entries, insertion order) — a hand-rolled FIFO-evicting cache.
type OrderedPlanCacheInner = (
    std::collections::HashMap<u64, CachedOrderedPlan>,
    std::collections::VecDeque<u64>,
);

static ORDERED_PLAN_CACHE: std::sync::OnceLock<std::sync::Mutex<OrderedPlanCacheInner>> =
    std::sync::OnceLock::new();

#[derive(Clone, Copy)]
struct OrderedPlanCacheTuning {
    global_fan_in_budget: usize,
    merge_max_fan_in: usize,
    sort_cluster_max_rows: u64,
}

fn ordered_plan_cache() -> &'static std::sync::Mutex<OrderedPlanCacheInner> {
    ORDERED_PLAN_CACHE.get_or_init(|| {
        std::sync::Mutex::new((
            std::collections::HashMap::new(),
            std::collections::VecDeque::new(),
        ))
    })
}

fn ordered_plan_cache_key(
    table: &Table,
    snapshot_id: Option<i64>,
    requested_descending: bool,
    small_limit: bool,
    task_partitions: &[Vec<FileScanTask>],
    tuning: OrderedPlanCacheTuning,
) -> Option<u64> {
    use std::hash::{Hash, Hasher};
    let snapshot =
        snapshot_id.or_else(|| table.metadata().current_snapshot().map(|s| s.snapshot_id()))?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    table.metadata().uuid().hash(&mut h);
    snapshot.hash(&mut h);
    requested_descending.hash(&mut h);
    small_limit.hash(&mut h);
    task_partitions.len().hash(&mut h);
    // The tuning knobs are inputs to the plan function too — hashing them
    // keeps the cache honest if they ever become dynamic. The global budget
    // already IS per session (`OrderedMergeGlobalBudget`), so it arrives as
    // the value this scan resolved rather than a fresh environment read.
    tuning.merge_max_fan_in.hash(&mut h);
    tuning.global_fan_in_budget.hash(&mut h);
    tuning.sort_cluster_max_rows.hash(&mut h);
    ordered_single_partition_max_limit().hash(&mut h);
    // Order-insensitive task-set digest: the balanced split's arrangement is
    // nondeterministic across requests; XOR of per-task hashes is stable.
    let mut set_digest: u64 = 0;
    let mut count: u64 = 0;
    for task in task_partitions.iter().flatten() {
        let mut th = std::collections::hash_map::DefaultHasher::new();
        task.data_file_path().hash(&mut th);
        task.start.hash(&mut th);
        task.length.hash(&mut th);
        set_digest ^= th.finish();
        count += 1;
    }
    set_digest.hash(&mut h);
    count.hash(&mut h);
    Some(h.finish())
}

fn ordered_plan_cache_get(key: u64) -> Option<CachedOrderedPlan> {
    let guard = ordered_plan_cache().lock().ok()?;
    guard.0.get(&key).cloned()
}

fn ordered_plan_cache_put(key: u64, plan: CachedOrderedPlan) {
    if let Ok(mut guard) = ordered_plan_cache().lock() {
        let (map, order) = &mut *guard;
        if map.insert(key, plan).is_none() {
            order.push_back(key);
            while map.len() > ORDERED_PLAN_CACHE_CAP {
                if let Some(old) = order.pop_front() {
                    map.remove(&old);
                } else {
                    break;
                }
            }
        }
    }
}

/// Rebuild `task_partitions` in the cached arrangement. `None` (cache
/// unusable) if any cached slot is missing from the fresh task set — the
/// key's task-set digest makes that effectively impossible, but collisions
/// fail safe into a recompute.
fn apply_cached_ordered_plan(
    cached: &CachedOrderedPlan,
    task_partitions: &mut Vec<Vec<FileScanTask>>,
) -> Option<()> {
    let mut by_key: std::collections::HashMap<(String, u64, u64), FileScanTask> =
        std::mem::take(task_partitions)
            .into_iter()
            .flatten()
            .map(|t| ((t.data_file_path().to_string(), t.start, t.length), t))
            .collect();
    let mut rebuilt = Vec::with_capacity(cached.order.len());
    for part in &cached.order {
        let mut tasks = Vec::with_capacity(part.len());
        for slot in part {
            tasks.push(by_key.remove(slot)?);
        }
        rebuilt.push(tasks);
    }
    if !by_key.is_empty() {
        return None;
    }
    *task_partitions = rebuilt;
    Some(())
}

/// Largest explicit LIMIT that coalesces an ordered scan to one partition
/// (`SIGLAKE_ORDERED_SINGLE_PARTITION_MAX_LIMIT`, default 1024, `0`
/// disables).
fn ordered_single_partition_max_limit() -> usize {
    std::env::var("SIGLAKE_ORDERED_SINGLE_PARTITION_MAX_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024)
}

/// Arrange tasks (given their `(min, max)` `timestamp` bounds) into a sequence
/// of transitively-overlapping CLUSTERS, ordered time-disjointly in the given
/// direction. Returns the in-order index permutation plus the cluster sizes
/// (in permutation order). Files within one cluster overlap (directly or
/// through a chain) and need a k-way merge; consecutive clusters are disjoint
/// and stream by plain ordered concat. A fully-disjoint run comes back as all
/// size-1 clusters.
///
/// This is what lets a 761-file `LIMIT` partition advertise ordering: the old
/// all-or-nothing disjoint check refused the whole partition on ANY overlap,
/// putting every file into one k-way merge whose fan-in the round-76 OOM cap
/// rejects. Clusters bound the merge fan-in by the WORST LOCAL OVERLAP —
/// which post-compaction is small — and clusters play sequentially, so the
/// concurrent-open-streams budget is the largest cluster, not the file count.
/// Equal boundary values are fine: adjacent equal keys still satisfy the
/// ordering.
fn cluster_partition(bounds: &[(i64, i64)], descending: bool) -> (Vec<usize>, Vec<usize>) {
    cluster_partition_inner(bounds, descending)
}

/// Decompose one CLUSTER's files into disjoint-run LAYERS by greedy interval
/// coloring: each file joins the first layer whose frontier it doesn't
/// overlap; a new layer opens only when the file overlaps every layer's
/// frontier. The k-way merge then runs over the LAYERS (each internally a
/// time-ordered disjoint run streaming sequentially), so merge fan-in equals
/// the cluster's maximum overlap DEPTH — not its chain length. This is what
/// makes ordered scans survive the wide-file shape: one L2/L3 file spanning
/// hours transitively chains every narrow file it overlaps into a single
/// cluster (the 1TB layout had chains past the 16-way cap), but the depth
/// stays ~the level count. Greedy-by-start is optimal for interval coloring,
/// so the layer count is the true depth. Returns the permutation (layer by
/// layer, each layer in time order) and the layer sizes.
fn layer_cluster(bounds: &[(i64, i64)], descending: bool) -> (Vec<usize>, Vec<usize>) {
    // (start, end) in scan direction: ASC = (min, max); DESC = (-max, -min),
    // so one code path handles both.
    let dir = |b: (i64, i64)| if descending { (-b.1, -b.0) } else { b };
    let mut order: Vec<usize> = (0..bounds.len()).collect();
    order.sort_by_key(|&i| dir(bounds[i]));
    // layers[l] = (frontier end, member indices in time order)
    let mut layers: Vec<(i64, Vec<usize>)> = Vec::new();
    for &i in &order {
        let (start, end) = dir(bounds[i]);
        match layers.iter_mut().find(|(frontier, _)| *frontier <= start) {
            Some((frontier, members)) => {
                *frontier = end;
                members.push(i);
            }
            None => layers.push((end, vec![i])),
        }
    }
    let sizes: Vec<usize> = layers.iter().map(|(_, m)| m.len()).collect();
    let perm: Vec<usize> = layers.into_iter().flat_map(|(_, m)| m).collect();
    (perm, sizes)
}

fn cluster_partition_inner(bounds: &[(i64, i64)], descending: bool) -> (Vec<usize>, Vec<usize>) {
    let mut idx: Vec<usize> = (0..bounds.len()).collect();
    let mut sizes: Vec<usize> = Vec::new();
    if descending {
        idx.sort_by_key(|&i| Reverse((bounds[i].1, bounds[i].0)));
        let mut cluster_lo = i64::MAX;
        for (pos, &i) in idx.iter().enumerate() {
            // Overlaps the open cluster while its max reaches back INTO it
            // (strictly above the cluster's running min).
            if pos > 0 && bounds[i].1 > cluster_lo {
                *sizes.last_mut().expect("cluster open") += 1;
                cluster_lo = cluster_lo.min(bounds[i].0);
            } else {
                sizes.push(1);
                cluster_lo = bounds[i].0;
            }
        }
    } else {
        idx.sort_by_key(|&i| bounds[i]);
        let mut cluster_hi = i64::MIN;
        for (pos, &i) in idx.iter().enumerate() {
            if pos > 0 && bounds[i].0 < cluster_hi {
                *sizes.last_mut().expect("cluster open") += 1;
                cluster_hi = cluster_hi.max(bounds[i].1);
            } else {
                sizes.push(1);
                cluster_hi = bounds[i].1;
            }
        }
    }
    (idx, sizes)
}

/// Per-file `sort_order_id` manifest stamps for the serving snapshot's live
/// files (`None` per file = written before stamping existed). `None` overall
/// = the manifest walk failed — the caller refuses the advertisement. These
/// direct loads bypass Iceberg's parsed-object cache; the ordered-plan cache
/// amortises the walk for an unchanged snapshot.
async fn per_file_sort_order_ids(
    table: &Table,
    snapshot_id: Option<i64>,
) -> Option<HashMap<String, Option<i32>>> {
    let meta = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => meta.snapshot_by_id(id),
        None => meta.current_snapshot(),
    }?;
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .ok()?;
    let mut out = HashMap::new();
    for mf in manifest_list.entries() {
        let manifest = mf.load_manifest(table.file_io()).await.ok()?;
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let df = entry.data_file();
            out.insert(df.file_path().to_string(), df.sort_order_id());
        }
    }
    Some(out)
}

/// Per-file `(min, max)` of the `timestamp` column for the alive data files of
/// the scanned snapshot, keyed by data-file path — the per-file sibling of
/// [`global_timestamp_bounds`]. These direct loads bypass Iceberg's
/// parsed-object cache; the ordered-plan cache amortises the walk for an
/// unchanged snapshot. A file without both bounds is absent from the map.
async fn per_file_timestamp_bounds(
    table: &Table,
    snapshot_id: Option<i64>,
    ts_field_id: i32,
) -> HashMap<String, (i64, i64)> {
    use iceberg::spec::PrimitiveLiteral;
    let mut out = HashMap::new();
    let meta = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => meta.snapshot_by_id(id),
        None => meta.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return out;
    };
    // Retry transient IO failures: a silently PARTIAL map here reads as
    // missing bounds at the gate — one flaky S3 manifest read intermittently
    // downgraded ordered browses to full-window TopK (a latent source of the
    // 07-16-class breaker trips). Persistent failure still returns partial
    // (the gate refuses, correct but slow) rather than erroring the scan.
    let mut manifest_list = None;
    for attempt in 0..3 {
        match snapshot
            .load_manifest_list(table.file_io(), &table.metadata_ref())
            .await
        {
            Ok(ml) => {
                manifest_list = Some(ml);
                break;
            }
            Err(e) if attempt < 2 => {
                tracing::warn!(attempt, error = %e, "manifest list load failed; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
            }
            Err(_) => {}
        }
    }
    let Some(manifest_list) = manifest_list else {
        return out;
    };
    for mf in manifest_list.entries() {
        let mut loaded = None;
        for attempt in 0..3 {
            match mf.load_manifest(table.file_io()).await {
                Ok(m) => {
                    loaded = Some(m);
                    break;
                }
                Err(e) if attempt < 2 => {
                    tracing::warn!(attempt, error = %e, "manifest load failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
                }
                Err(_) => {}
            }
        }
        let Some(manifest) = loaded else {
            return out;
        };
        for entry in manifest.entries() {
            if !entry.is_alive() {
                continue;
            }
            let df = entry.data_file();
            let lo = match df.lower_bounds().get(&ts_field_id).map(|d| d.literal()) {
                Some(PrimitiveLiteral::Long(v)) => *v,
                _ => continue,
            };
            let hi = match df.upper_bounds().get(&ts_field_id).map(|d| d.literal()) {
                Some(PrimitiveLiteral::Long(v)) => *v,
                _ => continue,
            };
            out.insert(df.file_path().to_string(), (lo, hi));
        }
    }
    out
}

/// Per-partition decoded file-batch cache outcomes, folded into the scan node's
/// counters at stream finish (→ per-request `stats.scan.file_cache_hits` /
/// `file_cache_misses`).
///
/// A hit returns cached Arrow batches before the vendored reader is built, so
/// none of the reader's counters move: the request reports leaf rows with
/// `files_read = 0` and no fetched bytes. Without this the response could not
/// distinguish that from counters that had not landed yet — the 2026-09-03 50G
/// `label_filter` round (rows_scanned 106,648 against files_read 0) cost a
/// diagnosis that started from the wrong premise.
#[derive(Default)]
struct FileCacheCounters {
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    /// #4890: tasks that consulted the cache, found nothing, and declined to
    /// populate because they carry a predicate or a prune spec (#4891). Without
    /// it a request with no population samples is unreadable: "nothing was
    /// decoded" and "every task was ineligible" look identical.
    bypasses: std::sync::atomic::AtomicU64,
    /// #4890: rows the request's populate streams were handed, summed over
    /// tasks — the per-request twin of
    /// `siglake_query_scan_file_cache_populate_rows`.
    populate_rows: std::sync::atomic::AtomicU64,
}

/// Live-partition accounting for one scan node: how many partition streams
/// have been handed out by `execute` and not yet finished.
///
/// The scan's per-request counters (`files_read`, `row_groups_read`,
/// `object_store_reads`, fetched bytes …) fold into the node's metrics only when
/// a partition stream finishes or drops. When an early `LIMIT` closes the root
/// stream, DataFusion aborts the partition pumps it spawned, but an abort takes
/// effect on the pump's next poll — on another worker, some time later. A
/// caller that snapshots the node's metrics as soon as the root stream ends
/// therefore reads a partial fold. [`settle_scan_partitions`] waits on this
/// tracker so the snapshot is taken after every partition has folded.
#[derive(Debug, Default)]
pub struct ScanPartitionTracker {
    live: std::sync::atomic::AtomicUsize,
    notify: tokio::sync::Notify,
}

impl ScanPartitionTracker {
    fn begin(&self) {
        self.live.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    /// Called after a partition's counters are folded, so a waiter that wakes
    /// on the last `end` reads complete metrics.
    fn end(&self) {
        if self.live.fetch_sub(1, std::sync::atomic::Ordering::AcqRel) == 1 {
            self.notify.notify_waiters();
        }
    }

    /// Partition streams handed out and not yet finished.
    pub fn live(&self) -> usize {
        self.live.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolves once every live partition stream has finished (folded its
    /// counters). Returns immediately when none is live.
    pub async fn wait_idle(&self) {
        loop {
            // Create the `Notified` BEFORE checking the count: tokio guarantees
            // it observes `notify_waiters` calls made after its creation, so an
            // `end` landing between the load and the await cannot be missed.
            let notified = self.notify.notified();
            if self.live() == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// #4865: staged start for the partitions of an UNORDERED, row-clipped
/// `LIMIT n` scan.
///
/// The shape: one partition per data file, no ordering to preserve, and a
/// residual `FilterExec` above the scan that DataFusion cannot push a limit
/// through — so every partition believes it owes the whole file and they all
/// open at once. On a fully compacted 15-file table that decoded 419,840 rows
/// to return 100 (task #4865; the same query answered from 597,696 bytes on a
/// layout whose small tail happened to satisfy the limit first). The partitions
/// are not small, so #4353's coalesce does not apply; what is wrong is that
/// they all start.
///
/// The gate is SCHEDULING ONLY. A partition that waits still reads every row it
/// would have read, and the limit still lives above the residual filter, so no
/// qualifying row can be lost — the ramp decides when a partition starts, never
/// how much of it is read.
///
/// Admission: a partition takes a ticket when it is executed and runs once
/// `admitted` passes its ticket. `admitted` starts at 1 and multiplies by
/// `wave` (default 2) each time the cumulative CREDITS reach the admitted width
/// — one credit per batch the scan emits and one per partition that ends.
/// Charging the width rather than every batch is what keeps the first wave
/// narrow: a limit answered out of the first batch leaves the ramp at `wave`,
/// where widening per batch would already have reached `wave^3` by the time the
/// root stream closed (measured on the local fixture: 4 files opened against
/// 2).
///
///   * A limit answered out of the first batch therefore opens `wave`
///     partitions, which is the case this exists for.
///   * A sparse term, or a term with too few matches, reaches full fan-out
///     after `partitions / wave` credits — 8 batches on the 15-file layout,
///     against the 430 batches one of its files holds — which is the case this
///     must not slow down.
///
/// Liveness, since a gated partition returns `Pending` to a pump that may be
/// the only one running: every terminal event pays a credit. If tickets are
/// waiting then `admitted < partitions`, the tickets below `admitted` are all
/// registered, and each of them pays at least the credit it owes by ending — so
/// the credits reach `admitted` and the ramp widens even if not one batch is
/// ever emitted. Tickets are handed out modulo the partition count, so a caller
/// that executes a single partition takes ticket 0 and is admitted at once, and
/// a plan executed a second time finds the ramp already open (it degrades to
/// the ungated behavior rather than blocking).
#[derive(Debug)]
struct ScanAdmission {
    /// Ticket counter; a ticket is `fetch_add % partitions`.
    next_ticket: std::sync::atomic::AtomicUsize,
    /// Highest ticket + 1 that may run. Monotone, capped at `partitions`.
    admitted: std::sync::atomic::AtomicUsize,
    /// Credits paid by the current wave; widens and resets at `admitted`.
    credits: std::sync::atomic::AtomicUsize,
    partitions: usize,
    wave: usize,
    notify: tokio::sync::Notify,
}

impl ScanAdmission {
    fn new(partitions: usize, wave: usize) -> Self {
        Self {
            next_ticket: std::sync::atomic::AtomicUsize::new(0),
            admitted: std::sync::atomic::AtomicUsize::new(1),
            credits: std::sync::atomic::AtomicUsize::new(0),
            partitions: partitions.max(1),
            wave: wave.max(2),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn register(&self) -> usize {
        self.next_ticket
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            % self.partitions
    }

    fn is_admitted(&self, ticket: usize) -> bool {
        ticket < self.admitted.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Pay one credit and widen the ramp once the credits reach the admitted
    /// width, waking whatever that admits. A no-op once every ticket is
    /// admitted, which is the steady state of any scan that is not
    /// early-stopped — so the common full-scan path pays one acquire load per
    /// batch and nothing else. Races between concurrent partitions can only
    /// widen a credit early or late.
    ///
    /// The count is CUMULATIVE and is never reset. A per-wave counter deadlocks
    /// the shape that pays nothing but its ends: with `admitted` at 2 and its
    /// first partition already finished (bloom-pruned, no batch), only one
    /// partition is left to pay and the wave never completes. Cumulative
    /// credits against `admitted` cannot: the `admitted` partitions pay one
    /// credit each just by ending, which is exactly the threshold.
    fn advance(&self) {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        let admitted = self.admitted.load(Acquire);
        if admitted >= self.partitions {
            return;
        }
        if self.credits.fetch_add(1, AcqRel) + 1 < admitted {
            return;
        }
        self.admitted.fetch_max(
            admitted.saturating_mul(self.wave).min(self.partitions),
            AcqRel,
        );
        self.notify.notify_waiters();
    }

    /// Resolves once `ticket` may run.
    async fn wait(&self, ticket: usize) {
        loop {
            // Created BEFORE the check, as in `ScanPartitionTracker::wait_idle`:
            // an `advance` landing between the load and the await is observed by
            // the `Notified` rather than lost.
            let notified = self.notify.notified();
            if self.is_admitted(ticket) {
                return;
            }
            notified.await;
        }
    }
}

/// Where one partition stream stands in the #4865 admission ramp.
enum AdmissionGate {
    /// Not ramped: no clipped limit, one partition, or an order-preserving scan.
    Open,
    /// Registered, not yet admitted. Nothing below this stream has been polled,
    /// so it has fetched nothing.
    Waiting {
        admission: Arc<ScanAdmission>,
        wait: BoxFuture<'static, ()>,
        since: Instant,
    },
    /// Running; its batches and its end widen the ramp for the others.
    Admitted(Arc<ScanAdmission>),
}

impl AdmissionGate {
    fn admission(&self) -> Option<&Arc<ScanAdmission>> {
        match self {
            AdmissionGate::Open => None,
            AdmissionGate::Waiting { admission, .. } | AdmissionGate::Admitted(admission) => {
                Some(admission)
            }
        }
    }
}

/// Outcome of [`settle_scan_partitions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanSettle {
    /// Every siglake scan partition in the plan had finished when this returned.
    pub complete: bool,
    /// Partition streams still live at return (0 when `complete`).
    pub live_partitions: usize,
    /// How long the wait took (zero when nothing was live).
    pub waited: std::time::Duration,
}

/// Wait, bounded by `deadline`, until every siglake scan partition stream in
/// `plan` has finished and folded its counters into the node metrics.
///
/// Call this after the root stream has been dropped and before reading the
/// plan's metrics for per-request attribution. Partitions a `LIMIT` aborted
/// unwind on their own workers, so the wait is normally a few scheduling hops;
/// the deadline exists so a partition stuck in a long synchronous poll cannot
/// hold a response indefinitely — the caller then reports the counters as
/// incomplete (`live_partitions > 0`) rather than pretending they are whole.
pub async fn settle_scan_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    deadline: std::time::Duration,
) -> ScanSettle {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut trackers: Vec<Arc<ScanPartitionTracker>> = Vec::new();
    let _ = plan.apply(|node| {
        if let Some(scan) = node.as_any().downcast_ref::<SiglakeIcebergTableScan>() {
            trackers.push(scan.partition_tracker.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    let live = |trackers: &[Arc<ScanPartitionTracker>]| -> usize {
        trackers.iter().map(|t| t.live()).sum()
    };
    if live(&trackers) == 0 {
        return ScanSettle {
            complete: true,
            live_partitions: 0,
            waited: std::time::Duration::ZERO,
        };
    }
    let started = Instant::now();
    let wait_all = async {
        for tracker in &trackers {
            tracker.wait_idle().await;
        }
    };
    let complete = tokio::time::timeout(deadline, wait_all).await.is_ok();
    ScanSettle {
        complete,
        live_partitions: live(&trackers),
        waited: started.elapsed(),
    }
}

struct SourceMetricsStream {
    inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    /// This partition's decode working set, held against the shared memory pool
    /// for the life of the stream and released on drop -- so a cancelled query
    /// returns its budget as well as stopping its scan.
    _decode_reservation: Option<datafusion::execution::memory_pool::MemoryReservation>,
    schema: ArrowSchemaRef,
    baseline: BaselineMetrics,
    partition: usize,
    projected_columns: Arc<Vec<String>>,
    planned_files: usize,
    planned_bytes: usize,
    planned_rows: usize,
    data_file_concurrency_limit: usize,
    started_at: Instant,
    stream_built_at: Instant,
    first_batch_at: Option<Instant>,
    last_batch_at: Option<Instant>,
    first_batch_rows: usize,
    inter_batch_gap_sum_secs: f64,
    inter_batch_gap_max_secs: f64,
    output_rows: usize,
    output_batches: usize,
    decoded_bytes: u64,
    fetched_byte_counter: Arc<std::sync::atomic::AtomicU64>,
    /// DataFusion node metric for ACTUAL object-store bytes read (post-prune),
    /// populated from `fetched_byte_counter` at finish. Surfaced per-query as
    /// `stats.bytes_scanned`.
    bytes_scanned: Count,
    /// Per-partition pruning/IO counters from the vendored reader, folded into
    /// `detail_metrics` at finish (→ per-request `stats.scan`).
    scan_counters: Arc<ScanCounters>,
    /// Decoded file-batch cache outcomes for this partition's tasks (the
    /// vendored reader never sees a hit, so these live beside `scan_counters`).
    file_cache_counters: Arc<FileCacheCounters>,
    detail_metrics: ScanDetailMetrics,
    /// Decremented once, after the fold, so `settle_scan_partitions` wakes to
    /// complete counters.
    partition_tracker: Arc<ScanPartitionTracker>,
    /// #4865: this partition's place in the clipped-limit admission ramp.
    /// `AdmissionGate::Open` for every other shape.
    gate: AdmissionGate,
    finished: bool,
}

/// DataFusion node counters carrying the per-request scan-attribution view:
/// what the scan opened, what each pruning mechanism dropped, and what the
/// read actually cost. Registered per partition; `summarize_plan_runtime`
/// sums them across partitions by name.
struct ScanDetailMetrics {
    files_read: Count,
    files_pruned_bloom: Count,
    row_groups_considered: Count,
    row_groups_pruned_bloom: Count,
    row_groups_pruned_stats: Count,
    row_groups_read: Count,
    rows_pruned_selection: Count,
    object_store_reads: Count,
    decoded_bytes: Count,
    /// F-5 byte classes: what the fetched bytes WERE, not which object they
    /// came from. `fetched_bytes` alone cannot separate a footer-bound cold
    /// aggregate from a column-bound wide scan, and those have opposite fixes.
    bytes_footer: Count,
    bytes_index: Count,
    bytes_data: Count,
    bytes_other: Count,
    /// File tasks served from the decoded file-batch cache (no reader built,
    /// so nothing else in this block moves for them) and tasks that consulted
    /// the cache, found nothing, and read the file.
    file_cache_hits: Count,
    file_cache_misses: Count,
    /// #4890: tasks that declined population (predicate or prune spec), and
    /// rows the request's populations were handed. The first says why a shape
    /// produced no decode-depth samples; the second is the depth itself,
    /// attributable to one query rather than to the process.
    file_cache_bypasses: Count,
    file_cache_populate_rows: Count,
}

impl ScanDetailMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let c = |name: &'static str| MetricBuilder::new(metrics).counter(name, partition);
        Self {
            files_read: c("files_read"),
            files_pruned_bloom: c("files_pruned_bloom"),
            row_groups_considered: c("row_groups_considered"),
            row_groups_pruned_bloom: c("row_groups_pruned_bloom"),
            row_groups_pruned_stats: c("row_groups_pruned_stats"),
            row_groups_read: c("row_groups_read"),
            rows_pruned_selection: c("rows_pruned_selection"),
            object_store_reads: c("object_store_reads"),
            decoded_bytes: c("decoded_bytes"),
            bytes_footer: c("bytes_footer"),
            bytes_index: c("bytes_index"),
            bytes_data: c("bytes_data"),
            bytes_other: c("bytes_other"),
            file_cache_hits: c("file_cache_hits"),
            file_cache_misses: c("file_cache_misses"),
            file_cache_bypasses: c("file_cache_bypasses"),
            file_cache_populate_rows: c("file_cache_populate_rows"),
        }
    }

    fn fold(&self, counters: &ScanCounters, decoded_bytes: u64, cache: &FileCacheCounters) {
        use std::sync::atomic::Ordering::Relaxed;
        self.file_cache_hits.add(cache.hits.load(Relaxed) as usize);
        self.file_cache_misses
            .add(cache.misses.load(Relaxed) as usize);
        self.file_cache_bypasses
            .add(cache.bypasses.load(Relaxed) as usize);
        self.file_cache_populate_rows
            .add(cache.populate_rows.load(Relaxed) as usize);
        self.files_read
            .add(counters.files_read.load(Relaxed) as usize);
        self.files_pruned_bloom
            .add(counters.files_pruned_bloom.load(Relaxed) as usize);
        self.row_groups_considered
            .add(counters.row_groups_considered.load(Relaxed) as usize);
        self.row_groups_pruned_bloom
            .add(counters.row_groups_pruned_bloom.load(Relaxed) as usize);
        self.row_groups_pruned_stats
            .add(counters.row_groups_pruned_stats.load(Relaxed) as usize);
        self.row_groups_read
            .add(counters.row_groups_read.load(Relaxed) as usize);
        self.rows_pruned_selection
            .add(counters.rows_pruned_selection.load(Relaxed) as usize);
        self.object_store_reads
            .add(counters.object_store_reads.load(Relaxed) as usize);
        self.decoded_bytes.add(decoded_bytes as usize);
        self.bytes_footer
            .add(counters.bytes_footer.load(Relaxed) as usize);
        self.bytes_index
            .add(counters.bytes_index.load(Relaxed) as usize);
        self.bytes_data
            .add(counters.bytes_data.load(Relaxed) as usize);
        self.bytes_other
            .add(counters.bytes_other.load(Relaxed) as usize);
    }
}

impl SourceMetricsStream {
    /// Poll the #4865 admission ramp. `Ready` for every ungated shape and for
    /// an already-admitted partition; `Pending` only while this partition is
    /// still queued behind the ramp, in which case the waker is held by the
    /// admission's `Notify`.
    fn poll_admission(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let admitted_since = match &mut self.gate {
            AdmissionGate::Open | AdmissionGate::Admitted(_) => return Poll::Ready(()),
            AdmissionGate::Waiting { wait, since, .. } => match wait.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => *since,
            },
        };
        metrics::histogram!("siglake_query_scan_clipped_admission_wait_seconds")
            .record(admitted_since.elapsed().as_secs_f64());
        let admission = self
            .gate
            .admission()
            .cloned()
            .expect("a Waiting gate carries its admission");
        self.gate = AdmissionGate::Admitted(admission);
        Poll::Ready(())
    }

    fn finish(&mut self, error: Option<&DataFusionError>) {
        if self.finished {
            return;
        }
        self.finished = true;
        // #4865 liveness: a partition that ends — exhausted, failed, or dropped
        // by an early `LIMIT` — widens the ramp, so the queued partitions cannot
        // be left waiting on a scan that has nothing left running.
        if let Some(admission) = self.gate.admission() {
            admission.advance();
        }
        let elapsed = self.started_at.elapsed();
        let stream_build_secs = self
            .stream_built_at
            .duration_since(self.started_at)
            .as_secs_f64();
        metrics::histogram!("siglake_query_scan_partition_wall_seconds")
            .record(elapsed.as_secs_f64());
        metrics::histogram!("siglake_query_scan_partition_stream_build_seconds")
            .record(stream_build_secs);
        let first_batch_secs = self
            .first_batch_at
            .map(|first| first.duration_since(self.started_at).as_secs_f64())
            .unwrap_or(elapsed.as_secs_f64());
        metrics::histogram!("siglake_query_scan_partition_first_batch_seconds")
            .record(first_batch_secs);
        metrics::histogram!("siglake_query_scan_partition_first_batch_rows")
            .record(self.first_batch_rows as f64);
        metrics::histogram!("siglake_query_scan_partition_output_rows")
            .record(self.output_rows as f64);
        metrics::histogram!("siglake_query_scan_partition_output_batches")
            .record(self.output_batches as f64);
        metrics::histogram!("siglake_query_scan_partition_decoded_bytes")
            .record(self.decoded_bytes as f64);
        let fetched_bytes = self.scan_counters.bytes_read();
        self.fetched_byte_counter
            .store(fetched_bytes, std::sync::atomic::Ordering::Relaxed);
        metrics::histogram!("siglake_query_scan_partition_fetched_bytes")
            .record(fetched_bytes as f64);
        // Real read bytes (post-prune) as a DataFusion node metric → per-query
        // stats.bytes_scanned reflects what the query actually fetched.
        self.bytes_scanned.add(fetched_bytes as usize);
        self.detail_metrics.fold(
            &self.scan_counters,
            self.decoded_bytes,
            &self.file_cache_counters,
        );
        if self.output_batches > 1 {
            metrics::histogram!("siglake_query_scan_partition_inter_batch_gap_seconds_mean")
                .record(self.inter_batch_gap_sum_secs / (self.output_batches - 1) as f64);
            metrics::histogram!("siglake_query_scan_partition_inter_batch_gap_seconds_max")
                .record(self.inter_batch_gap_max_secs);
        }
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        let stream_build_ms = stream_build_secs * 1000.0;
        let first_batch_ms = first_batch_secs * 1000.0;
        let mean_batch_gap_ms = if self.output_batches > 1 {
            (self.inter_batch_gap_sum_secs / (self.output_batches - 1) as f64) * 1000.0
        } else {
            0.0
        };
        let max_batch_gap_ms = self.inter_batch_gap_max_secs * 1000.0;
        tracing::info!(
            partition = self.partition,
            planned_files = self.planned_files,
            planned_bytes = self.planned_bytes,
            planned_rows = self.planned_rows,
            file_concurrency_limit = self.data_file_concurrency_limit,
            stream_build_ms,
            first_batch_ms,
            first_batch_rows = self.first_batch_rows,
            mean_batch_gap_ms,
            max_batch_gap_ms,
            output_rows = self.output_rows,
            output_batches = self.output_batches,
            decoded_bytes = self.decoded_bytes,
            fetched_bytes,
            projected_column_count = self.projected_columns.len(),
            projected_columns = ?self.projected_columns,
            amplification_ratio = output_rows_planned_rows_ratio(self.output_rows, self.planned_rows),
            elapsed_ms,
            error = error.map(|e| e.to_string()),
            "siglake query source partition profile"
        );
        // Last: everything above has landed in the node metrics, so a waiter
        // woken by this decrement reads a complete fold.
        self.partition_tracker.end();
    }
}

impl Drop for SourceMetricsStream {
    fn drop(&mut self) {
        self.finish(None);
    }
}

impl Stream for SourceMetricsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // #4865: nothing below this point is polled until the ramp admits this
        // partition, and the reader fetches only on poll — so a waiting
        // partition costs no object-store read and no decode.
        if self.poll_admission(cx).is_pending() {
            return self.baseline.record_poll(Poll::Pending);
        }
        let poll = self.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(Some(Ok(batch))) => {
                // A batch means the consumer is still asking for rows, so the
                // limit above the residual filter is not yet met: widen the
                // ramp by one wave.
                if let AdmissionGate::Admitted(admission) = &self.gate {
                    admission.advance();
                }
                let now = Instant::now();
                if self.first_batch_at.is_none() {
                    self.first_batch_at = Some(now);
                    self.first_batch_rows = batch.num_rows();
                } else if let Some(last) = self.last_batch_at {
                    let gap_secs = now.duration_since(last).as_secs_f64();
                    self.inter_batch_gap_sum_secs += gap_secs;
                    self.inter_batch_gap_max_secs = self.inter_batch_gap_max_secs.max(gap_secs);
                }
                self.last_batch_at = Some(now);
                self.decoded_bytes = self
                    .decoded_bytes
                    .saturating_add(batch.get_array_memory_size() as u64);
                self.output_rows += batch.num_rows();
                self.output_batches += 1;
            }
            Poll::Ready(Some(Err(err))) => self.finish(Some(err)),
            Poll::Ready(None) => self.finish(None),
            Poll::Pending => {}
        }
        self.baseline.record_poll(poll)
    }
}

impl datafusion::physical_plan::RecordBatchStream for SourceMetricsStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
}

fn task_cache_key(task: &FileScanTask) -> String {
    format!(
        "{}:{}:{}:{}",
        task.data_file_path(),
        task.start,
        task.length,
        task_projection_delete_key(task)
    )
}

/// The projection-and-deletes half of a cache key: everything about identity
/// that is not the file, the byte range or the direction. Shared by the
/// whole-file key and #4847's per-row-group key so the two cannot drift.
fn task_projection_delete_key(task: &FileScanTask) -> String {
    let fields = task
        .project_field_ids()
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let deletes = task
        .deletes
        .iter()
        .map(|delete| {
            let equality_ids = delete
                .equality_ids
                .as_ref()
                .map(|ids| ids.iter().map(i32::to_string).collect::<Vec<_>>().join(","))
                .unwrap_or_default();
            format!(
                "{}:{}:{:?}:{}:{}",
                delete.file_path,
                delete.file_size_in_bytes,
                delete.file_type,
                delete.partition_spec_id,
                equality_ids
            )
        })
        .collect::<Vec<_>>()
        .join("|");
    format!("{fields}:{deletes}")
}

fn task_cache_key_with_direction(task: &FileScanTask, reverse: bool) -> String {
    format!("{}:reverse={reverse}", task_cache_key(task))
}

/// #4905's predicate-keyed identity. The serialized bound predicate retains
/// field ids, operators, literal types and literal values; the existing key
/// keeps the file range, projection, delete set and direction distinctions.
/// This key is used only by the in-process qualification prototype.
fn task_cache_key_with_predicate_direction(task: &FileScanTask, reverse: bool) -> String {
    let base = task_cache_key_with_direction(task, reverse);
    match task.predicate.as_ref() {
        Some(predicate) => format!(
            "{base}:predicate={}",
            serde_json::to_string(predicate).expect("bound predicates serialize")
        ),
        None => base,
    }
}

/// The task a population reads under: the query's predicate removed, so the
/// entry is a function of the file and the projection alone and a later query
/// with a different predicate can reuse it.
///
/// Since #4891 the caller declines to populate a task that HAS a predicate, so
/// this strips nothing on the live paths; it stays as the statement of what an
/// entry is allowed to depend on, and as the guard if a predicate ever reaches
/// a populate call some other way.
fn cacheable_task(task: &FileScanTask) -> FileScanTask {
    let mut task = task.clone();
    task.predicate = None;
    task
}

#[allow(clippy::too_many_arguments)]
fn open_task_batch_stream_uncached(
    file_io: iceberg::io::FileIO,
    task: FileScanTask,
    reader_tuning: EffectiveReaderTuning,
    _byte_counter: Arc<std::sync::atomic::AtomicU64>,
    scan_counters: Arc<ScanCounters>,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    reverse: bool,
) -> DFResult<TaskBatchStream> {
    let task_stream: FileScanTaskStream = futures::stream::iter(std::iter::once(Ok(task))).boxed();
    // Enable page-index row selection: data files carry Parquet page statistics
    // (ColumnIndex/OffsetIndex, written by default), so a scan predicate prunes
    // *rows within a row group* via a `RowSelection`, not just whole row groups.
    // For time-ordered storage a `timestamp` range predicate skips pages whose
    // min/max fall outside the range. The page index is only loaded when a
    // predicate is present, so predicate-free scans pay nothing.
    let mut reader = ArrowReaderBuilder::new(file_io, iceberg::Runtime::current())
        .with_data_file_concurrency_limit(1)
        .with_row_selection_enabled(true)
        .with_scan_counters(Some(scan_counters))
        .with_raw_prune_spec(raw_prune_spec)
        .with_promoted_prune(promoted_prune);
    if reverse {
        reader = reader.with_reverse(true);
    }
    if let Some(batch_size) = reader_tuning.batch_size {
        reader = reader.with_batch_size(batch_size.max(1));
    }
    if let Some(range_coalesce_bytes) = reader_tuning.range_coalesce_bytes {
        reader = reader.with_range_coalesce_bytes(range_coalesce_bytes.max(1));
    }
    if let Some(range_fetch_concurrency) = reader_tuning.range_fetch_concurrency {
        reader = reader.with_range_fetch_concurrency(range_fetch_concurrency.max(1));
    }
    if let Some(rows) = reader_tuning.reversed_chunk_rows {
        reader = reader.with_reversed_chunk_rows(rows);
    }
    if reader_tuning.bypass_reader_caches {
        reader = reader.with_cache_bypass(true);
    }
    Ok(reader
        .build()
        .read(task_stream)
        .map_err(|e| DataFusionError::External(e.into()))?
        .stream()
        .map(|result| result.map_err(|e| DataFusionError::External(e.into())))
        .boxed())
}

#[allow(clippy::too_many_arguments)]
async fn open_task_batch_stream_cached(
    file_io: iceberg::io::FileIO,
    task: FileScanTask,
    reader_tuning: EffectiveReaderTuning,
    cache_tuning: EffectiveFileCacheTuning,
    byte_counter: Arc<std::sync::atomic::AtomicU64>,
    scan_counters: Arc<ScanCounters>,
    cache_counters: Arc<FileCacheCounters>,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    reverse: bool,
) -> DFResult<TaskBatchStream> {
    if !cache_tuning.enabled() {
        return open_task_batch_stream_uncached(
            file_io,
            task,
            reader_tuning,
            byte_counter,
            scan_counters,
            raw_prune_spec,
            promoted_prune,
            reverse,
        );
    }

    let fallback_key = task_cache_key_with_direction(&task, reverse);
    let key = if cache_tuning.predicate_key_prototype && task.predicate.is_some() {
        task_cache_key_with_predicate_direction(&task, reverse)
    } else {
        fallback_key.clone()
    };
    let hit = {
        let cache = query_file_batch_cache()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.get(&key).or_else(|| {
            // A predicate-free entry contains the whole task and remains a
            // valid fallback for a predicate-keyed lookup. The residual filter
            // retained during planning enforces the query predicate.
            (key != fallback_key)
                .then(|| cache.get(&fallback_key))
                .flatten()
        })
    };
    if let Some(hit) = hit {
        metrics::counter!(
            "siglake_query_scan_file_cache_requests_total",
            "outcome" => "hit"
        )
        .increment(1);
        // Per-request twin of the process counter: the reader is never built
        // for a hit, so this is the only trace the request keeps of the file.
        cache_counters
            .hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let batches = hit.batches.iter().cloned().collect::<Vec<_>>();
        return Ok(
            futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)).boxed(),
        );
    }

    // #4891: under the shipped policy, a task carrying a converted predicate
    // reads FEWER pages than any reusable population of it can. Such an entry
    // has to serve a query with a different predicate, so its populate read
    // strips `task.predicate` and decodes the whole projection where the
    // reader's page index would have
    // skipped most of it — measured 2.8x slower than the cache-disabled arm on
    // a `host = '<label>' LIMIT 100` browse, which then inserts nothing because
    // the clip drops the stream before end-of-stream (#4494, #4847). So a
    // predicate task takes the same bypass a raw or promoted prune takes: it
    // reads with its predicate intact, exactly as a cache-disabled install
    // does, and a cache-enabled install cannot lose to one on that shape.
    //
    // Only the POPULATE path is declined. The lookup above is unchanged, so an
    // entry a predicate-free scan left behind still serves this query, and
    // `filter_pushdown_with_file_cache` keeps DataFusion's residual filter
    // whenever the cache is on, so both routes answer exactly. #4905's
    // in-process qualification gate is the sole exception to the decline: it
    // keeps the predicate in both the key and the read.
    let predicate_population = cache_tuning.predicate_key_prototype && task.predicate.is_some();
    if (task.predicate.is_some() && !predicate_population)
        || raw_prune_spec.is_some()
        || !promoted_prune.is_empty()
    {
        metrics::counter!(
            "siglake_query_scan_file_cache_requests_total",
            "outcome" => "bypass"
        )
        .increment(1);
        cache_counters
            .bypasses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return open_task_batch_stream_uncached(
            file_io,
            task,
            reader_tuning,
            byte_counter,
            scan_counters,
            raw_prune_spec,
            promoted_prune,
            reverse,
        );
    }

    // #4847 prototype (off unless an in-process caller set it): per-row-group
    // entries. Placed after the prune bypass so filtering semantics are
    // untouched — a task with a raw or promoted prune still bypasses the cache
    // entirely, at either granularity.
    if cache_tuning.row_group_prototype {
        if let Some(plan) = row_group_plan(&file_io, &task, reverse).await {
            return row_group_task_stream(
                plan,
                file_io,
                task,
                reader_tuning,
                cache_tuning,
                byte_counter,
                scan_counters,
                cache_counters,
                reverse,
            );
        }
    }

    metrics::counter!(
        "siglake_query_scan_file_cache_requests_total",
        "outcome" => "miss"
    )
    .increment(1);
    cache_counters
        .misses
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let cacheable = if predicate_population {
        // The predicate is part of `key`, so the entry is allowed to contain
        // exactly the rows this reader produces. Insertion still happens only
        // at EOF; a clipped prefix never becomes a complete entry.
        task.clone()
    } else {
        cacheable_task(&task)
    };
    Ok(Box::pin(CachePopulateStream {
        key,
        tuning: cache_tuning,
        inner: open_task_batch_stream_uncached(
            file_io,
            cacheable,
            reader_tuning,
            byte_counter,
            scan_counters,
            None,
            Vec::new(),
            reverse,
        )?,
        buffered: Vec::new(),
        buffered_bytes: 0,
        oversized: false,
        insert_done: false,
        charge: PopulationCharge::open(),
        yielded_rows: 0,
        end: PopulateEnd::Unpolled,
        cache_counters,
    }))
}

/// Build the task stream for a [`RowGroupPlan`].
///
/// Counter discipline, chosen so the shipped series keep one meaning: `hit`
/// when the task needed no reader at all, `miss` when one was opened — which is
/// exactly what those two say today. The prototype's own detail (a task partly
/// served from cache, how many groups) goes to
/// `siglake_query_scan_file_cache_row_group_total`, which no dashboard or
/// pre-registration depends on.
#[allow(clippy::too_many_arguments)]
fn row_group_task_stream(
    plan: RowGroupPlan,
    file_io: iceberg::io::FileIO,
    task: FileScanTask,
    reader_tuning: EffectiveReaderTuning,
    cache_tuning: EffectiveFileCacheTuning,
    byte_counter: Arc<std::sync::atomic::AtomicU64>,
    scan_counters: Arc<ScanCounters>,
    cache_counters: Arc<FileCacheCounters>,
    reverse: bool,
) -> DFResult<TaskBatchStream> {
    let populate =
        |inner: TaskBatchStream, keys: Vec<String>, group_rows: Vec<u64>| -> TaskBatchStream {
            Box::pin(RowGroupPopulateStream {
                inner,
                tuning: cache_tuning,
                keys,
                group_rows,
                at: 0,
                filled_rows: 0,
                buffered: Vec::new(),
                buffered_bytes: 0,
                group_skipped: false,
                misaligned: false,
                charge: PopulationCharge::open(),
            })
        };
    match plan {
        RowGroupPlan::Served(batches) => {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "hit"
            )
            .increment(1);
            cache_counters
                .hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            row_group_prototype_outcome("served_whole_task");
            Ok(futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)).boxed())
        }
        RowGroupPlan::Partial {
            served,
            served_groups,
            range,
            keys,
            group_rows,
        } => {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "miss"
            )
            .increment(1);
            cache_counters
                .misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            row_group_prototype_outcome("partial_serve");
            metrics::histogram!("siglake_query_scan_file_cache_row_group_served")
                .record(served_groups as f64);
            // The derived task addresses exactly the groups that were not
            // served. Its rows, in order, are the ones the served batches do
            // not carry, so cached-then-read is the same row sequence the
            // whole-task read produces.
            let mut remainder = cacheable_task(&task);
            remainder.start = range.0;
            remainder.length = range.1;
            let inner = open_task_batch_stream_uncached(
                file_io,
                remainder,
                reader_tuning,
                byte_counter,
                scan_counters,
                None,
                Vec::new(),
                reverse,
            )?;
            Ok(
                futures::stream::iter(served.into_iter().map(Ok::<_, DataFusionError>))
                    .chain(populate(inner, keys, group_rows))
                    .boxed(),
            )
        }
        RowGroupPlan::Populate { keys, group_rows } => {
            metrics::counter!(
                "siglake_query_scan_file_cache_requests_total",
                "outcome" => "miss"
            )
            .increment(1);
            cache_counters
                .misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let inner = open_task_batch_stream_uncached(
                file_io,
                cacheable_task(&task),
                reader_tuning,
                byte_counter,
                scan_counters,
                None,
                Vec::new(),
                reverse,
            )?;
            Ok(populate(inner, keys, group_rows))
        }
    }
}

impl ExecutionPlan for SiglakeIcebergTableScan {
    fn name(&self) -> &str {
        "SiglakeIcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let tasks = self
            .task_partitions
            .get(partition)
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "partition {partition} out of bounds for SiglakeIcebergTableScan"
                ))
            })?;
        let metrics = self.metrics.clone();
        let schema = self.schema();
        let file_io = self.table.file_io().clone();
        let data_file_concurrency_limit = self.data_file_concurrency_limit;
        // DataFusion keeps an ordered LIMIT on SortPreservingMergeExec instead
        // of passing it through TableProvider::scan. The request extension is
        // therefore the only limit the ordered source sees. Cap each sorted
        // partition at that value: no partition can contribute more than the
        // query's global LIMIT, and ending here prevents a cooperative wrapper
        // above the scan from polling throwaway merge batches after the LIMIT
        // has its result.
        let source_limit = effective_ordered_source_limit(
            self.limit,
            self.ordered_limit,
            self.ordered_source_limit_safe,
        );
        let projected_columns = self.projected_columns.clone();
        let reader_tuning = self.reader_tuning;
        let file_cache_tuning = self.file_cache_tuning;
        let raw_prune_spec = self.raw_prune_spec.clone();
        let reverse_scan = self.reverse_scan;
        let planned_bytes: usize = tasks.iter().map(|task| task.length as usize).sum();
        let planned_files = tasks.len();
        let planned_rows: usize = tasks
            .iter()
            .map(|task| task.record_count.unwrap_or_default() as usize)
            .sum();
        let files_scanned = MetricBuilder::new(&metrics).counter("files_scanned", partition);
        files_scanned.add(planned_files);
        // `planned_bytes` = the assigned file-set size (pre-read). The misnamed
        // `bytes_scanned` counter used to carry this, so a bloom/page-pruned query
        // reported the whole file-set as "scanned" (~full corpus at 1TB). Record
        // planned separately and reserve `bytes_scanned` for ACTUAL read bytes,
        // populated from the fetched-byte counter at stream finish.
        let planned_bytes_metric = MetricBuilder::new(&metrics).counter("planned_bytes", partition);
        planned_bytes_metric.add(planned_bytes);
        let bytes_scanned = MetricBuilder::new(&metrics).counter("bytes_scanned", partition);
        let planned_rows_metric = MetricBuilder::new(&metrics).counter("planned_rows", partition);
        planned_rows_metric.add(planned_rows);

        let partition_started = Instant::now();
        let reader_build_started = Instant::now();
        // Per-scan counter of ACTUAL object-store bytes fetched by this
        // partition's reader (footer + data), via the vendored read path.
        let fetched_byte_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // Per-partition pruning/IO counters from the vendored reader — folded
        // into node metrics at stream finish so `summarize_plan_runtime` can
        // surface a per-request pruning-effectiveness view (stats.scan).
        let scan_counters = Arc::new(ScanCounters::default());
        let file_cache_counters = Arc::new(FileCacheCounters::default());
        let desired_file_concurrency = if source_limit.is_some() {
            1
        } else {
            data_file_concurrency_limit
        };
        // Account this partition's decode working set against the SHARED pool,
        // so concurrent scans contend for one budget instead of each helping
        // itself to a flat per-query 2 GiB. Reducing concurrency is the
        // degradation; the reservation lives on the stream and is released when
        // the stream drops -- including when a query is cancelled.
        // The WHOLE-FILE figure, deliberately, despite being a large over-estimate
        // of what a reader holds at any instant.
        //
        // A row-group-sized estimate was tried and REVERTED. It is more accurate
        // in the abstract and made things worse in fact: cheaper reservations are
        // granted more often, so the scan takes more concurrency. Measured at 1TB
        // on 2026-08-22 -- reservations went from 360 granted / 972 unreserved to
        // 16 granted / 0 unreserved, and the process OOM-killed at 31.6 GB against
        // a 12.4 GiB pool, where the whole-file build had survived every dose at
        // RSS 8.8 GiB.
        //
        // So the 73% "unreserved" rate was not the bound failing; it WAS the bound
        // working -- falling back to one file at a time is what kept memory low.
        // This number is a concurrency governor, not an accounting claim, and
        // over-estimating errs in the safe direction.
        let decoded_per_file = estimated_decoded_bytes_per_file(
            planned_files,
            planned_bytes as u64,
            scan_decompression_factor(),
        );
        let (execute_file_concurrency_limit, decode_reservation) = reserve_decode_budget(
            context.memory_pool(),
            desired_file_concurrency,
            decoded_per_file,
        );
        // WS-3: a partition whose files overlap in time can't stream sorted by
        // concatenation alone — its transitively-overlapping CLUSTERS merge,
        // chained in time order (`stream::iter().flatten()` polls one segment
        // at a time, so only one cluster's streams are open at once). Within a
        // cluster the merge runs over LAYERS (each a sequential disjoint run),
        // so the fan-in is the overlap DEPTH — the 1TB layout chains hundreds
        // of files through one wide L2, but its depth is ~the level count.
        let cluster_plan = self
            .partition_clusters
            .get(partition)
            .filter(|plan| {
                self.preserve_task_order
                    && plan.iter().any(|c| !matches!(c, OrderedCluster::Run(_)))
            })
            .cloned();
        let stream = if let Some(clusters) = cluster_plan {
            debug_assert_eq!(
                clusters
                    .iter()
                    .map(|c| match c {
                        OrderedCluster::Run(n) | OrderedCluster::Sort(n) => *n,
                        OrderedCluster::Merge(layers) => {
                            layers.iter().map(|layer| layer.file_count).sum()
                        }
                    })
                    .sum::<usize>(),
                tasks.len(),
                "cluster plan must cover the partition"
            );
            let mut rest = tasks;
            let mut segments: Vec<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> =
                Vec::with_capacity(clusters.len());
            for cluster in &clusters {
                let n: usize = match cluster {
                    OrderedCluster::Run(n) | OrderedCluster::Sort(n) => *n,
                    OrderedCluster::Merge(layers) => {
                        layers.iter().map(|layer| layer.file_count).sum()
                    }
                };
                let tail = rest.split_off(n.min(rest.len()));
                let mut cluster_tasks = std::mem::replace(&mut rest, tail);
                match cluster {
                    OrderedCluster::Run(_) => {
                        // Disjoint run: sequential chain, no merge.
                        segments.push(self.sequential_task_chain(
                            cluster_tasks,
                            file_io.clone(),
                            fetched_byte_counter.clone(),
                            scan_counters.clone(),
                        )?);
                    }
                    OrderedCluster::Sort(_) => {
                        let reservation =
                            MemoryConsumer::new(format!("SiglakeOrderedSort[{partition}]"))
                                .register(context.memory_pool());
                        segments.push(self.eager_sorted_cluster_stream(
                            cluster_tasks,
                            file_io.clone(),
                            fetched_byte_counter.clone(),
                            scan_counters.clone(),
                            schema.clone(),
                            reservation,
                        )?);
                    }
                    OrderedCluster::Merge(layer_plans) => {
                        let mut layers: Vec<OrderedMergeInput> =
                            Vec::with_capacity(layer_plans.len());
                        for layer_plan in layer_plans {
                            let tail = cluster_tasks
                                .split_off(layer_plan.file_count.min(cluster_tasks.len()));
                            let layer = std::mem::replace(&mut cluster_tasks, tail);
                            let chained = self.sequential_task_chain(
                                layer,
                                file_io.clone(),
                                fetched_byte_counter.clone(),
                                scan_counters.clone(),
                            )?;
                            layers.push(OrderedMergeInput {
                                stream: Box::pin(RecordBatchStreamAdapter::new(
                                    schema.clone(),
                                    chained,
                                )),
                                frontier_bound: Some(layer_plan.frontier_bound),
                            });
                        }
                        segments.push(self.merge_record_streams(
                            layers,
                            partition,
                            &context,
                            schema.clone(),
                        )?);
                    }
                }
            }
            futures::stream::iter(segments).flatten().boxed()
        } else if file_cache_tuning.enabled() && !self.preserve_task_order {
            // #90: the batch cache is for scans that CONSUME their tasks — a
            // miss decodes the whole task before the first batch comes back
            // (the fill is a task-granular future), and an early-stopped
            // browse cancels mid-fill so nothing is ever inserted. On big
            // converged files that turned ordered browses into serial
            // whole-file decodes: ~3s first-batch instead of streaming, and
            // repeated windowed browses re-paid the fill past the request
            // timeout every time (the 07-12 diag's warm 504s; 86ms uncached).
            // Order-preserving scans early-stop by design — they take the
            // streaming reader below and leave the cache to the parallel
            // full-scan paths it was built for.
            let counter = fetched_byte_counter.clone();
            let task_scan_counters = scan_counters.clone();
            let task_cache_counters = file_cache_counters.clone();
            let raw_prune_spec = raw_prune_spec.clone();
            let promoted_prune = self.promoted_prune.clone();
            let task_futures = futures::stream::iter(tasks.into_iter().map(move |task| {
                let file_io = file_io.clone();
                let counter = counter.clone();
                let task_scan_counters = task_scan_counters.clone();
                let task_cache_counters = task_cache_counters.clone();
                let raw_prune_spec = raw_prune_spec.clone();
                let promoted_prune = promoted_prune.clone();
                async move {
                    open_task_batch_stream_cached(
                        file_io,
                        task,
                        reader_tuning,
                        file_cache_tuning,
                        counter,
                        task_scan_counters,
                        task_cache_counters,
                        raw_prune_spec,
                        promoted_prune,
                        reverse_scan,
                    )
                    .await
                }
                .boxed()
            }));
            // WS-3: an order-advertising scan must emit batches in task order
            // (the tasks form a time-ascending disjoint run). Non-front files
            // may prefetch ahead, but their buffered batches are byte-budgeted
            // so an above-scan residual filter can't retain whole decoded
            // files waiting for the front task.
            if self.preserve_task_order {
                OrderedTaskDrain::with_budget_bytes(
                    task_futures,
                    execute_file_concurrency_limit.max(1),
                    "file_cache",
                    reader_tuning
                        .ordered_drain_buffer_bytes
                        .unwrap_or_else(ordered_drain_buffer_bytes),
                )
                .boxed()
            } else {
                task_futures
                    .buffer_unordered(execute_file_concurrency_limit.max(1))
                    .try_flatten()
                    .boxed()
            }
        } else {
            let task_stream: FileScanTaskStream =
                futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
            // We own the read path (vendored iceberg), so the reader reports
            // actual fetched bytes through the per-scan counter below.
            let mut reader = ArrowReaderBuilder::new(file_io, iceberg::Runtime::current())
                .with_data_file_concurrency_limit(execute_file_concurrency_limit)
                // Page-index row selection (see the single-task reader above):
                // composes with the raw-bloom + predicate row-group filtering —
                // those narrow row groups first, then the page index prunes rows
                // within the survivors.
                .with_row_selection_enabled(true)
                .with_scan_counters(Some(scan_counters.clone()))
                .with_raw_prune_spec(raw_prune_spec.clone())
                .with_promoted_prune(self.promoted_prune.clone())
                // WS-3: an order-advertising scan's tasks form a time-ascending
                // disjoint run — batches must come back in task order.
                .with_output_order_preserved(self.preserve_task_order);
            if self.reverse_scan {
                reader = reader.with_reverse(true);
            }
            if let Some(batch_size) = reader_tuning.batch_size {
                reader = reader.with_batch_size(batch_size.max(1));
            }
            if let Some(range_coalesce_bytes) = reader_tuning.range_coalesce_bytes {
                reader = reader.with_range_coalesce_bytes(range_coalesce_bytes.max(1));
            }
            if let Some(range_fetch_concurrency) = reader_tuning.range_fetch_concurrency {
                reader = reader.with_range_fetch_concurrency(range_fetch_concurrency.max(1));
            }
            if let Some(rows) = reader_tuning.reversed_chunk_rows {
                reader = reader.with_reversed_chunk_rows(rows);
            }
            if reader_tuning.bypass_reader_caches {
                reader = reader.with_cache_bypass(true);
            }
            reader
                .build()
                .read(task_stream)
                .map_err(|e| DataFusionError::External(e.into()))?
                .stream()
                .map(|result| result.map_err(|e| DataFusionError::External(e.into())))
                .boxed()
        };
        let stream = match source_limit {
            Some(limit) => limit_record_batch_stream(stream, limit).boxed(),
            None => stream,
        };
        // Cancellation lands HERE, at the source. The pumps DataFusion spawns
        // above this stream cannot be aborted from outside -- but they all drain
        // from it, so ending it makes them see end-of-input and unwind. Without
        // this a cancelled query keeps scanning to completion; measured still
        // running 35 minutes later, and enough of them wedge the host.
        let stream = match self.cancel.clone() {
            Some(cancel) => stream
                .take_while(move |_| futures::future::ready(!cancel.is_cancelled()))
                .boxed(),
            None => stream,
        };
        let stream_built_at = Instant::now();
        metrics::histogram!("siglake_query_scan_partition_stream_create_seconds").record(
            stream_built_at
                .duration_since(reader_build_started)
                .as_secs_f64(),
        );
        // #4865: take this partition's ticket HERE, while `execute` is still
        // running for the whole plan, so tickets follow execution order rather
        // than the order the pumps happen to be scheduled in. Nothing below the
        // gate has been polled yet, so a partition that waits has fetched
        // nothing.
        let gate = match &self.admission {
            Some(admission) => {
                let ticket = admission.register();
                let immediate = admission.is_admitted(ticket);
                metrics::counter!(
                    "siglake_query_scan_clipped_admission_total",
                    "outcome" => if immediate { "immediate" } else { "queued" }
                )
                .increment(1);
                if immediate {
                    AdmissionGate::Admitted(admission.clone())
                } else {
                    let waiting = admission.clone();
                    AdmissionGate::Waiting {
                        admission: admission.clone(),
                        wait: async move { waiting.wait(ticket).await }.boxed(),
                        since: Instant::now(),
                    }
                }
            }
            None => AdmissionGate::Open,
        };
        // Counted from here: the stream below owns the matching `end` (in
        // `finish`, reached by end-of-input, error, or drop).
        self.partition_tracker.begin();
        Ok(Box::pin(SourceMetricsStream {
            inner: stream,
            _decode_reservation: decode_reservation,
            schema,
            baseline: BaselineMetrics::new(&metrics, partition),
            partition,
            projected_columns,
            planned_files,
            planned_bytes,
            planned_rows,
            data_file_concurrency_limit,
            started_at: partition_started,
            stream_built_at,
            first_batch_at: None,
            last_batch_at: None,
            first_batch_rows: 0,
            inter_batch_gap_sum_secs: 0.0,
            inter_batch_gap_max_secs: 0.0,
            output_rows: 0,
            output_batches: 0,
            decoded_bytes: 0,
            fetched_byte_counter,
            bytes_scanned,
            scan_counters,
            file_cache_counters,
            detail_metrics: ScanDetailMetrics::new(&metrics, partition),
            partition_tracker: self.partition_tracker.clone(),
            gate,
            finished: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Report an **exact** row count from the planned files' manifest
    /// `record_count`s — but only when this scan applies no row-level filtering.
    /// Then the sum is exactly the rows the scan emits, and DataFusion's
    /// `AggregateStatistics` rule answers `count(*)` from it with **zero Parquet
    /// IO** (index-only aggregation). The exactness is per-scan, so it composes
    /// with distributed sharding: each worker reports its slice exactly and the
    /// coordinator sums the partial counts.
    ///
    /// A scan predicate (boundary files survive partition/file pruning but only
    /// partially match) or applicable delete files would make the sum an upper
    /// bound, not the answer — so in those cases we report unknown and the count
    /// falls back to a real scan.
    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        let schema = self.schema();
        let mut stats = Statistics::new_unknown(&schema);
        if self.has_pushed_filters || self.raw_prune_spec.is_some() {
            return Ok(stats);
        }
        let tasks: Vec<&FileScanTask> = match partition {
            None => self.task_partitions.iter().flatten().collect(),
            Some(p) => match self.task_partitions.get(p) {
                Some(part) => part.iter().collect(),
                None => return Ok(stats),
            },
        };
        if tasks.iter().any(|t| !t.deletes.is_empty()) {
            return Ok(stats);
        }
        // `Option<u64>` sums to `None` if any task lacks a manifest record_count.
        if let Some(total) = tasks.iter().map(|t| t.record_count).sum::<Option<u64>>() {
            stats.num_rows = Precision::Exact(total as usize);
        }

        // Exact min/max for the `timestamp` column (when computed for an
        // unfiltered scan) → DataFusion's AggregateStatistics rule answers
        // min/max(timestamp) with zero Parquet IO. `partition: None` reports
        // table-wide bounds; the same bounds are a valid (if loose) superset for
        // any single partition, so we report them for `Some(_)` too — count(*)
        // and per-shard partials stay exact.
        // `ts_bounds` is in the `timestamp` column's OWN unit (microseconds
        // since the 2026-09-06 contract), because that is what the manifest
        // literal holds — so the scalar has to be built to match the field, not
        // assumed nanosecond.
        if let Some((min, max)) = self.ts_bounds {
            if let Ok(field) = schema.field_with_name("timestamp") {
                if let DataType::Timestamp(unit, tz) = field.data_type() {
                    let scalar = |v: i64| match unit {
                        TimeUnit::Second => ScalarValue::TimestampSecond(Some(v), tz.clone()),
                        TimeUnit::Millisecond => {
                            ScalarValue::TimestampMillisecond(Some(v), tz.clone())
                        }
                        TimeUnit::Microsecond => {
                            ScalarValue::TimestampMicrosecond(Some(v), tz.clone())
                        }
                        TimeUnit::Nanosecond => {
                            ScalarValue::TimestampNanosecond(Some(v), tz.clone())
                        }
                    };
                    let idx = schema.index_of("timestamp").unwrap();
                    let mut cols: Vec<datafusion::common::ColumnStatistics> = schema
                        .fields()
                        .iter()
                        .map(|_| datafusion::common::ColumnStatistics::new_unknown())
                        .collect();
                    cols[idx].min_value = Precision::Exact(scalar(min));
                    cols[idx].max_value = Precision::Exact(scalar(max));
                    stats.column_statistics = cols;
                }
            }
        }
        Ok(stats)
    }
}

impl DisplayAs for SiglakeIcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "SiglakeIcebergTableScan partitions:[{}] projection:[{}] predicate:[{}]",
            self.task_partitions.len(),
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.predicates
                .clone()
                .map_or_else(String::new, |p| p.to_string()),
        )?;
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        // Only for a scan that HAS a text predicate: on every other plan the
        // field would be noise, and "no text index" would read as a refusal
        // rather than as nothing to refuse.
        if let Some(spec) = self.raw_prune_spec.as_ref() {
            match self.text_index_decline {
                Some(reason) => write!(f, " text_index:[declined:{reason}]")?,
                None if spec.inverted_index_row_selection => write!(f, " text_index:[allowed]")?,
                None => {}
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TableProvider for SiglakeStaticTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SiglakeIcebergTableScan::try_new(
                self.table.clone(),
                self.snapshot_id,
                self.schema.clone(),
                projection,
                filters,
                limit,
                state,
            )
            .await?,
        ))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let file_cache_enabled =
            effective_file_cache_tuning(crate::current_query_scan_tuning()).enabled();
        let text_tokenizers = loaded_table_index_config(&self.table)
            .map_err(|e| DataFusionError::External(e.into()))?
            .map(|config| text_field_tokenizers(&config))
            .unwrap_or_default();
        let promoted = promoted_utf8_columns(&self.table);
        filters
            .iter()
            .map(|expr| {
                pushdown_kind(expr, &self.schema, &text_tokenizers, &promoted)
                    .map(|kind| filter_pushdown_with_file_cache(kind, file_cache_enabled))
            })
            .collect()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "write operations are not supported on SiglakeStaticTableProvider".into(),
        ))
    }
}

fn filter_pushdown_with_file_cache(
    kind: TableProviderFilterPushDown,
    file_cache_enabled: bool,
) -> TableProviderFilterPushDown {
    // Shipped cache entries are whole-file decoded batches read under no
    // predicate, so an entry populated by one query is returned unfiltered to
    // the next one, whatever it asked for. Keep DataFusion's residual FilterExec
    // whenever that path is enabled; otherwise an Exact declaration would let a
    // hit expose rows that the SQL predicate rejects. This holds after #4891: a
    // predicate task now bypasses the cache on a miss, but it can still HIT an
    // entry a predicate-free scan left behind. It also remains mandatory for
    // #4905's in-process predicate-key prototype because planning cannot know
    // whether execution will find that key or use the predicate-free fallback.
    if file_cache_enabled && kind == TableProviderFilterPushDown::Exact {
        TableProviderFilterPushDown::Inexact
    } else {
        kind
    }
}

fn pushdown_kind(
    expr: &Expr,
    schema: &ArrowSchemaRef,
    text_tokenizers: &HashMap<String, Tokenizer>,
    promoted: &HashMap<String, String>,
) -> DFResult<TableProviderFilterPushDown> {
    if exact_pushdown_eligible(expr, schema) {
        return Ok(TableProviderFilterPushDown::Exact);
    }

    if PredicateConverter::new(schema).convert_filter(expr).is_some()
        || extract_raw_prune_spec(std::slice::from_ref(expr), text_tokenizers)?.is_some()
        // WS-7: admit `attr_get(attributes, key) = value` so the scan can
        // prune via the promoted column's stats (Inexact — re-applied above).
        || !extract_promoted_prune(std::slice::from_ref(expr), promoted).is_empty()
    {
        return Ok(TableProviderFilterPushDown::Inexact);
    }

    Ok(TableProviderFilterPushDown::Unsupported)
}

fn exact_pushdown_eligible(expr: &Expr, schema: &ArrowSchemaRef) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And | Operator::Or => {
                exact_pushdown_eligible(&binary.left, schema)
                    && exact_pushdown_eligible(&binary.right, schema)
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => comparison_pushdown_eligible(&binary.left, &binary.right, schema),
            _ => false,
        },
        Expr::Not(inner) => exact_pushdown_eligible(inner, schema),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            exact_scalar_column(inner, schema).is_some()
        }
        Expr::InList(inlist) if !inlist.negated => {
            let Some(col) = exact_scalar_column(&inlist.expr, schema) else {
                return false;
            };
            inlist.list.iter().all(|expr| {
                PredicateConverter::new(schema)
                    .const_datum_for_expr(expr, &col)
                    .is_some()
            })
        }
        _ => false,
    }
}

fn comparison_pushdown_eligible(left: &Expr, right: &Expr, schema: &ArrowSchemaRef) -> bool {
    exact_scalar_column(left, schema).is_some_and(|data_type| {
        PredicateConverter::new(schema)
            .const_datum_for_expr(right, &data_type)
            .is_some()
    }) || exact_scalar_column(right, schema).is_some_and(|data_type| {
        PredicateConverter::new(schema)
            .const_datum_for_expr(left, &data_type)
            .is_some()
    })
}

fn exact_scalar_column(expr: &Expr, schema: &ArrowSchemaRef) -> Option<DataType> {
    let column = PredicateConverter::new(schema).extract_column(expr)?;
    is_materialized_scalar_type(&column.data_type).then_some(column.data_type)
}

fn is_materialized_scalar_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Null
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Map(_, _)
    )
}

#[allow(clippy::too_many_arguments)]
async fn plan_task_partitions(
    table: &Table,
    snapshot_id: Option<i64>,
    column_names: Option<&Vec<String>>,
    predicates: Option<&Predicate>,
    limit: Option<usize>,
    target_partitions: usize,
    tuning: crate::QueryScanTuning,
    shard: Option<ScanShard>,
) -> DFResult<Vec<Vec<FileScanTask>>> {
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };
    // The projection this provider was asked for was resolved against
    // `schema_with_text_tokenizers`, i.e. the table's CURRENT schema, so the
    // scan has to resolve it against the same one (#2494). A snapshot carries
    // the schema it was WRITTEN under, and an additive `migrate-schema` commits
    // no data — so every retained snapshot still points at the narrow schema,
    // and a `SELECT *` planned over the widened provider was refused by the
    // scan ("Column N not found in table") until some later append happened to
    // commit a snapshot under the new schema id. The reader fills a projected
    // field id no data file carries with nulls, which is what a pre-widen row
    // is owed.
    let scan_builder = scan_builder.with_current_schema();
    let mut scan_builder = match column_names {
        Some(column_names) => scan_builder.select(column_names.clone()),
        None => scan_builder.select_all(),
    };
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred.clone());
    }
    let table_scan = scan_builder
        .build()
        .map_err(|e| DataFusionError::External(e.into()))?;
    let tasks = table_scan
        .plan_files()
        .await
        .map_err(|e| DataFusionError::External(e.into()))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| DataFusionError::External(e.into()))?;
    // Distributed-query sharding: keep only this worker's slice of the file
    // set before splitting across cores. Disjoint + covering across shards.
    let tasks = match shard {
        Some(s) => shard_file_tasks(tasks, s),
        None => tasks,
    };
    Ok(partition_file_tasks(
        tasks,
        limit,
        target_partitions,
        tuning,
    ))
}

fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| get_schema_column_names(&Arc::new(schema.project(v).unwrap())))
}

fn get_schema_column_names(schema: &ArrowSchemaRef) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<String>>()
}

fn limit_record_batch_stream(
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    limit: usize,
) -> Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> {
    if limit == 0 {
        return futures::stream::empty().boxed();
    }

    struct LimitRecordBatchStream {
        inner: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        remaining: usize,
    }

    impl Stream for LimitRecordBatchStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.remaining == 0 {
                return Poll::Ready(None);
            }

            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if batch.num_rows() <= this.remaining {
                        this.remaining -= batch.num_rows();
                        Poll::Ready(Some(Ok(batch)))
                    } else {
                        let sliced = batch.slice(0, this.remaining);
                        this.remaining = 0;
                        Poll::Ready(Some(Ok(sliced)))
                    }
                }
                other => other,
            }
        }
    }

    Box::pin(LimitRecordBatchStream {
        inner: stream,
        remaining: limit,
    })
}

/// Limit visible at the ordered scan source.
///
/// DataFusion pushes an unordered LIMIT through `TableProvider::scan`, but
/// retains an ordered LIMIT on the merge above the provider. Once this scan
/// advertises order with no residual filter, each partition is sorted in the
/// requested direction and can contribute at most the global limit. For an
/// unordered or residual-filtered scan the request hint cannot safely cap the
/// source.
fn effective_ordered_source_limit(
    pushed_limit: Option<usize>,
    ordered_limit: Option<usize>,
    ordered_source_limit_safe: bool,
) -> Option<usize> {
    match (
        pushed_limit,
        ordered_limit.filter(|_| ordered_source_limit_safe),
    ) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}

fn output_rows_planned_rows_ratio(output_rows: usize, planned_rows: usize) -> f64 {
    if planned_rows == 0 {
        0.0
    } else {
        output_rows as f64 / planned_rows as f64
    }
}

fn col_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Column(c) => Some(c.name.as_str()),
        Expr::Cast(c) => col_name(&c.expr),
        Expr::Alias(a) => col_name(&a.expr),
        _ => None,
    }
}

fn lit_str(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.as_str()),
        _ => None,
    }
}

fn normalize_raw_prune_spec(mut spec: RawPruneSpec) -> Option<RawPruneSpec> {
    for values in [
        &mut spec.all_terms,
        &mut spec.any_terms,
        &mut spec.substrings,
        &mut spec.index_substrings,
    ] {
        values.sort();
        values.dedup();
    }
    (!spec.is_empty()).then_some(spec)
}

fn spec_accepts_column(spec: &mut RawPruneSpec, column: &str) -> bool {
    if spec.column.is_empty() {
        spec.column = column.to_string();
        true
    } else {
        spec.column == column
    }
}

fn tokenizer_for_column<'a>(
    text_tokenizers: &'a HashMap<String, Tokenizer>,
    column: &str,
) -> Option<&'a Tokenizer> {
    text_tokenizers.get(column)
}

fn tokenize_match_query(name: &str, tokenizer: Tokenizer, query: &str) -> DFResult<Vec<String>> {
    let tokens = tokenizer.tokenize(query);
    if tokens.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "{name}: query must contain at least one indexed token",
        )));
    }
    Ok(tokens)
}

fn extract_match_udf_prune(
    filter: &Expr,
    spec: &mut RawPruneSpec,
    text_tokenizers: &HashMap<String, Tokenizer>,
) -> DFResult<bool> {
    let Expr::ScalarFunction(func) = filter else {
        return Ok(false);
    };
    if func.args.len() != 2 {
        return Ok(false);
    }
    let Some(column) = col_name(&func.args[0]) else {
        return Ok(false);
    };
    let Some(&tokenizer) = tokenizer_for_column(text_tokenizers, column) else {
        return Ok(false);
    };
    if !spec_accepts_column(spec, column) {
        return Ok(false);
    }
    let Some(query) = lit_str(&func.args[1]) else {
        return Ok(false);
    };
    let tokens = tokenize_match_query(func.func.name(), tokenizer, query)?;
    match func.func.name().to_ascii_lowercase().as_str() {
        "match_terms" | "match" => spec.all_terms.extend(tokens),
        "match_any" => spec.any_terms.extend(tokens),
        "match_phrase" => spec.all_terms.extend(tokens),
        "match_prefix" => spec.substrings.extend(tokens),
        _ => return Ok(false),
    }
    spec.fts_udf = true;
    Ok(true)
}

/// WS-7: the promoted Utf8 attr-key → column-name map from the table's
/// `siglake.promoted.v1` property (written by `ensure_promoted_columns`).
/// Empty for tables without declared promotions. Utf8 only: `attr_get`
/// yields strings, so only Utf8 promotions substitute value-for-value.
fn promoted_utf8_columns(table: &Table) -> HashMap<String, String> {
    siglake_core::promoted_columns_from_property(
        table
            .metadata()
            .properties()
            .get(siglake_core::PROMOTED_PROPERTY_KEY)
            .map(String::as_str),
    )
    .into_iter()
    .filter(|c| c.ty == siglake_core::PromotedType::Utf8)
    .map(|c| (c.attr_key, c.name))
    .collect()
}

/// WS-7: extract `attr_get(attributes, 'key') = 'value'` (and non-negated
/// `IN` lists) from pushed filters, mapped onto promoted Utf8 columns.
/// Conjunction members arrive as separate filters; nested `AND` trees are
/// walked too. Hints only — the engine re-applies the original predicate.
fn extract_promoted_prune(
    filters: &[Expr],
    promoted: &HashMap<String, String>,
) -> Vec<PromotedPruneSpec> {
    fn attr_get_key(expr: &Expr) -> Option<&str> {
        let Expr::ScalarFunction(func) = expr else {
            return None;
        };
        if func.func.name() != "attr_get" || func.args.len() != 2 {
            return None;
        }
        match (&func.args[0], &func.args[1]) {
            (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(key)), _))
                if col.name == "attributes" =>
            {
                Some(key)
            }
            _ => None,
        }
    }
    fn walk(expr: &Expr, promoted: &HashMap<String, String>, out: &mut Vec<PromotedPruneSpec>) {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                walk(&binary.left, promoted, out);
                walk(&binary.right, promoted, out);
            }
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                let (udf, other) = match (attr_get_key(&binary.left), attr_get_key(&binary.right)) {
                    (Some(key), None) => (key, &binary.right),
                    (None, Some(key)) => (key, &binary.left),
                    _ => return,
                };
                let (Some(column), Some(value)) = (promoted.get(udf), lit_str(other)) else {
                    return;
                };
                out.push(PromotedPruneSpec {
                    column: column.clone(),
                    values: vec![value.to_string()],
                });
            }
            Expr::InList(in_list) if !in_list.negated => {
                let Some(key) = attr_get_key(&in_list.expr) else {
                    return;
                };
                let Some(column) = promoted.get(key) else {
                    return;
                };
                let values: Option<Vec<String>> = in_list
                    .list
                    .iter()
                    .map(|e| lit_str(e).map(str::to_string))
                    .collect();
                if let Some(values) = values {
                    if !values.is_empty() {
                        out.push(PromotedPruneSpec {
                            column: column.clone(),
                            values,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if promoted.is_empty() {
        return out;
    }
    for filter in filters {
        walk(filter, promoted, &mut out);
    }
    out
}

/// Extract a conservative raw prune spec from pushed-down filters. Existing
/// `%...%` LIKE extraction stays on default-tokenized text columns only:
/// substring probes against stemmed term dictionaries can false-negate.
/// WI-2 FTS UDFs widen from the legacy `raw` column to any mapped text column
/// whose tokenizer is known at scan-planning time. The scan still re-evaluates
/// the original predicate/UDF, so this is acceleration only.
fn extract_raw_prune_spec(
    filters: &[Expr],
    text_tokenizers: &HashMap<String, Tokenizer>,
) -> DFResult<Option<RawPruneSpec>> {
    let mut spec = RawPruneSpec::default();
    for filter in filters {
        if extract_match_udf_prune(filter, &mut spec, text_tokenizers)? {
            continue;
        }
        let Expr::Like(Like {
            negated,
            case_insensitive,
            escape_char,
            expr,
            pattern,
        }) = filter
        else {
            continue;
        };
        let Some(column) = col_name(expr) else {
            continue;
        };
        if *negated || *case_insensitive || escape_char.is_some() {
            continue;
        }
        let Some(&tokenizer) = tokenizer_for_column(text_tokenizers, column) else {
            continue;
        };
        if tokenizer != Tokenizer::Default || !spec_accepts_column(&mut spec, column) {
            continue;
        }
        let Some(pattern) = lit_str(pattern) else {
            continue;
        };
        let Some(inner) = pattern.strip_prefix('%').and_then(|s| s.strip_suffix('%')) else {
            continue;
        };
        if inner.contains('%') || inner.contains('_') {
            continue;
        }
        if inner.chars().count() >= siglake_bloom::TRIGRAM_LEN {
            spec.substrings.push(inner.to_string());
            spec.index_substrings.push(inner.to_string());
        }
    }
    Ok(normalize_raw_prune_spec(spec))
}

/// Distributed-query scan shard (#7): a worker scans only the files whose
/// stable hash falls in its slot. `count` shards with the same `count`
/// partition the file set **disjointly** and **cover** it completely, so a
/// coordinator can fan a query out to `count` workers (one per shard) and
/// union/merge their partial results. Injected per-request via a DataFusion
/// `SessionConfig` extension. See `docs/DESIGN_distributed_query.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanShard {
    pub index: usize,
    pub count: usize,
}

impl ScanShard {
    /// `None` for a no-op shard (`count <= 1`, or `index >= count`) — the
    /// caller then scans the whole file set.
    pub fn new(index: usize, count: usize) -> Option<Self> {
        if count <= 1 || index >= count {
            None
        } else {
            Some(Self { index, count })
        }
    }

    /// Whether this shard owns the data file at `path`. Ownership is a pure
    /// function of the path (FNV-1a mod `count`), so the coordinator, every
    /// worker, and a test computing the expected split from the live-file list
    /// all agree without coordination.
    pub fn owns(&self, path: &str) -> bool {
        (stable_path_hash(path) % self.count as u64) as usize == self.index
    }
}

/// FNV-1a over the path — stable across processes (unlike `DefaultHasher`), so
/// every worker agrees on which shard owns a file.
fn stable_path_hash(s: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Keep only the file tasks owned by `shard` (deterministic by data-file path).
fn shard_file_tasks(tasks: Vec<FileScanTask>, shard: ScanShard) -> Vec<FileScanTask> {
    tasks
        .into_iter()
        .filter(|t| shard.owns(t.data_file_path()))
        .collect()
}

fn partition_file_tasks(
    mut tasks: Vec<FileScanTask>,
    limit: Option<usize>,
    target_partitions: usize,
    tuning: crate::QueryScanTuning,
) -> Vec<Vec<FileScanTask>> {
    if tasks.is_empty() {
        return vec![Vec::new()];
    }
    if limit.is_some() {
        return vec![tasks];
    }

    tasks.sort_by_key(|task| Reverse(task.record_count.unwrap_or(task.length)));
    let partition_count = adaptive_partition_count(&tasks, target_partitions, tuning)
        .min(tasks.len())
        .max(1);
    let mut partitions = vec![Vec::new(); partition_count];
    let mut partition_weights = vec![0u64; partition_count];

    for task in tasks {
        let idx = partition_weights
            .iter()
            .enumerate()
            .min_by_key(|(_, weight)| *weight)
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        partition_weights[idx] += task.record_count.unwrap_or(task.length);
        partitions[idx].push(task);
    }

    partitions
}

fn adaptive_partition_count(
    tasks: &[FileScanTask],
    target_partitions: usize,
    tuning: crate::QueryScanTuning,
) -> usize {
    let max_partitions = target_partitions.max(1);
    let planned_files = tasks.len();
    let planned_bytes = tasks.iter().map(|task| task.length).sum::<u64>();
    let bytes_target = tuning.adaptive_partition_min_bytes.and_then(|threshold| {
        (threshold > 0).then_some(planned_bytes.div_ceil(threshold) as usize)
    });
    let files_target = tuning
        .adaptive_partition_min_files
        .and_then(|threshold| (threshold > 0).then_some(planned_files.div_ceil(threshold)));
    let adaptive = match (bytes_target, files_target) {
        (Some(bytes_target), Some(files_target)) => bytes_target.max(files_target),
        (Some(bytes_target), None) => bytes_target,
        (None, Some(files_target)) => files_target,
        (None, None) => max_partitions,
    };
    adaptive.clamp(1, max_partitions)
}

fn effective_reader_tuning(
    tuning: crate::QueryScanTuning,
    planned_files: usize,
    planned_bytes: u64,
    ordered_tuning: OrderedScanTuning,
) -> EffectiveReaderTuning {
    let batch_size = tuning.batch_size;
    let range_configured =
        tuning.range_coalesce_bytes.is_some() || tuning.range_fetch_concurrency.is_some();
    if !range_configured {
        return EffectiveReaderTuning {
            batch_size,
            range_coalesce_bytes: None,
            range_fetch_concurrency: None,
            reversed_chunk_rows: ordered_tuning.reversed_chunk_rows,
            bypass_reader_caches: ordered_tuning.bypass_reader_caches,
            ordered_drain_buffer_bytes: tuning
                .ordered_drain_buffer_bytes
                .or(ordered_tuning.ordered_drain_buffer_bytes),
        };
    }
    let enable_range = match (
        tuning.range_adaptive_min_files,
        tuning.range_adaptive_min_bytes,
    ) {
        (None, None) => true,
        (min_files, min_bytes) => {
            min_files.is_some_and(|threshold| planned_files >= threshold)
                || min_bytes.is_some_and(|threshold| planned_bytes >= threshold)
        }
    };
    EffectiveReaderTuning {
        batch_size,
        range_coalesce_bytes: enable_range
            .then_some(tuning.range_coalesce_bytes)
            .flatten(),
        range_fetch_concurrency: enable_range
            .then_some(tuning.range_fetch_concurrency)
            .flatten(),
        reversed_chunk_rows: ordered_tuning.reversed_chunk_rows,
        bypass_reader_caches: ordered_tuning.bypass_reader_caches,
        ordered_drain_buffer_bytes: tuning
            .ordered_drain_buffer_bytes
            .or(ordered_tuning.ordered_drain_buffer_bytes),
    }
}

fn effective_file_concurrency_limit(
    partition_count: usize,
    tuning: crate::QueryScanTuning,
) -> usize {
    let partition_count = partition_count.max(1);
    let default_limit = if partition_count > 1 {
        std::thread::available_parallelism()
            .map(|n| (n.get() / partition_count).max(1))
            .unwrap_or(1)
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get().max(1))
            .unwrap_or(1)
    };
    let budget_limit = tuning
        .reader_budget
        .map(|budget| (budget / partition_count).max(1));
    match (tuning.file_concurrency_limit, budget_limit) {
        (Some(explicit_limit), Some(budget_limit)) => explicit_limit.min(budget_limit).max(1),
        (Some(explicit_limit), None) => explicit_limit.max(1),
        (None, Some(budget_limit)) => budget_limit.max(1),
        (None, None) => default_limit.max(1),
    }
}

fn effective_file_cache_tuning(tuning: crate::QueryScanTuning) -> EffectiveFileCacheTuning {
    EffectiveFileCacheTuning {
        max_bytes: tuning.file_cache_max_bytes.filter(|n| *n > 0),
        max_entries: tuning.file_cache_max_entries.filter(|n| *n > 0),
        row_group_prototype: tuning.file_cache_row_group_prototype,
        predicate_key_prototype: tuning.file_cache_predicate_key_prototype,
    }
}

fn scan_decoded_budget_bytes() -> u64 {
    std::env::var("SIGLAKE_SCAN_DECODED_BUDGET_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
        .unwrap_or(DEFAULT_SCAN_DECODED_BUDGET_BYTES)
}

fn scan_decompression_factor() -> u64 {
    std::env::var("SIGLAKE_SCAN_DECOMPRESSION_FACTOR")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
        .unwrap_or(DEFAULT_SCAN_DECOMPRESSION_FACTOR)
}

fn estimated_decoded_bytes_per_file(
    planned_files: usize,
    planned_bytes: u64,
    decompression_factor: u64,
) -> u64 {
    let planned_files = u64::try_from(planned_files).unwrap_or(u64::MAX);
    if planned_files == 0 || planned_bytes == 0 || decompression_factor == 0 {
        return 0;
    }
    planned_bytes
        .saturating_div(planned_files)
        .saturating_mul(decompression_factor)
}

/// #90: floor the reader budget at the partition count when the scan
/// advertises ordering — see the call site for the liveness argument. Not
/// applied to unordered scans, where the memory-derived clamp is the point.
fn ordered_reader_budget_floor(
    budget: usize,
    preserve_task_order: bool,
    partition_count: usize,
) -> usize {
    if preserve_task_order {
        budget.max(partition_count.max(1))
    } else {
        budget
    }
}

/// Reserve this partition's decode working set from the shared memory pool,
/// reducing file concurrency until it fits.
///
/// WHY. `scan_decoded_budget_bytes()` is a flat per-query constant (2 GiB by
/// default) that bounds how many files a scan decodes at once. It is per-query
/// and unaccounted, so N concurrent queries helped themselves to N x 2 GiB --
/// structurally the same defect as the unbounded DataFusion pool, in a second
/// budget, and invisible to it. Routing it through the same pool makes ONE
/// number bound the process.
///
/// Degrades rather than fails: a scan that cannot reserve its full working set
/// runs with fewer files open, which is slower. Failing the query instead would
/// turn memory pressure into an outage, and the scan is not a spillable operator
/// so the pool cannot reclaim from it.
///
/// At the floor (one file) it proceeds WITHOUT a reservation and says so on a
/// counter. That is a deliberate soft edge: one file's working set is a far cry
/// from N x 2 GiB, and blocking here would deadlock a pool that only this scan
/// can release.
fn reserve_decode_budget(
    pool: &Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    desired_concurrency: usize,
    decoded_per_file: u64,
) -> (
    usize,
    Option<datafusion::execution::memory_pool::MemoryReservation>,
) {
    use datafusion::execution::memory_pool::MemoryConsumer;
    if decoded_per_file == 0 || desired_concurrency == 0 {
        return (desired_concurrency.max(1), None);
    }
    let reservation = MemoryConsumer::new("siglake-scan-decode").register(pool);
    let mut concurrency = desired_concurrency.max(1);
    loop {
        let want = (concurrency as u64).saturating_mul(decoded_per_file);
        match usize::try_from(want) {
            Ok(bytes) if reservation.try_grow(bytes).is_ok() => {
                let outcome = if concurrency == desired_concurrency {
                    "granted"
                } else {
                    "reduced"
                };
                metrics::counter!(
                    "siglake_query_scan_decode_reservation_total",
                    "outcome" => outcome
                )
                .increment(1);
                return (concurrency, Some(reservation));
            }
            _ => {}
        }
        if concurrency <= 1 {
            metrics::counter!(
                "siglake_query_scan_decode_reservation_total",
                "outcome" => "unreserved"
            )
            .increment(1);
            return (1, None);
        }
        concurrency /= 2;
    }
}

fn clamp_aggregate_reader_budget(
    aggregate_reader_budget: usize,
    planned_files: usize,
    planned_bytes: u64,
    decoded_budget_bytes: u64,
    decompression_factor: u64,
) -> usize {
    let decoded_per_file =
        estimated_decoded_bytes_per_file(planned_files, planned_bytes, decompression_factor);
    if decoded_per_file == 0 {
        return aggregate_reader_budget.max(MIN_AGGREGATE_READER_BUDGET);
    }
    let decoded_limit = decoded_budget_bytes.saturating_div(decoded_per_file);
    let decoded_limit = usize::try_from(decoded_limit).unwrap_or(usize::MAX);
    aggregate_reader_budget
        .min(decoded_limit)
        .max(MIN_AGGREGATE_READER_BUDGET)
}

#[derive(Clone)]
struct ColumnRef {
    reference: Reference,
    data_type: DataType,
}

struct PredicateConverter<'a> {
    schema: &'a ArrowSchemaRef,
    now: DateTime<Utc>,
}

impl<'a> PredicateConverter<'a> {
    fn new(schema: &'a ArrowSchemaRef) -> Self {
        Self {
            schema,
            now: Utc::now(),
        }
    }

    fn convert_filters(&self, filters: &[Expr]) -> Option<Predicate> {
        filters
            .iter()
            .filter_map(|expr| self.convert_filter(expr))
            .reduce(Predicate::and)
    }

    fn convert_filter(&self, expr: &Expr) -> Option<Predicate> {
        match expr {
            Expr::BinaryExpr(binary) => match binary.op {
                Operator::And => combine_and(
                    self.convert_filter(&binary.left),
                    self.convert_filter(&binary.right),
                ),
                Operator::Or => combine_or(
                    self.convert_filter(&binary.left),
                    self.convert_filter(&binary.right),
                ),
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => self.convert_comparison(
                    &binary.left,
                    &binary.right,
                    to_predicate_op(binary.op)?,
                ),
                _ => None,
            },
            Expr::Between(between) if !between.negated => {
                let col = self.extract_column(&between.expr)?;
                let low = self.const_datum_for_expr(&between.low, &col.data_type)?;
                let high = self.const_datum_for_expr(&between.high, &col.data_type)?;
                Some(
                    Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::GreaterThanOrEq,
                        col.reference.clone(),
                        low,
                    ))
                    .and(Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::LessThanOrEq,
                        col.reference,
                        high,
                    ))),
                )
            }
            Expr::Not(inner) => self.convert_filter(inner).map(|p| !p),
            Expr::IsNull(expr) => self.extract_column(expr).map(|col| {
                Predicate::Unary(UnaryExpression::new(
                    PredicateOperator::IsNull,
                    col.reference,
                ))
            }),
            Expr::IsNotNull(expr) => self.extract_column(expr).map(|col| {
                Predicate::Unary(UnaryExpression::new(
                    PredicateOperator::NotNull,
                    col.reference,
                ))
            }),
            Expr::InList(inlist) if !inlist.negated => {
                let col = self.extract_column(&inlist.expr)?;
                let datums = inlist
                    .list
                    .iter()
                    .map(|expr| self.const_datum_for_expr(expr, &col.data_type))
                    .collect::<Option<Vec<_>>>()?;
                Some(col.reference.is_in(datums))
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) if escape_char.is_none() && !case_insensitive => {
                let col = self.extract_column(expr)?;
                let pattern = self.generic_datum_for_expr(pattern)?;
                let PrimitiveLiteral::String(pattern) = pattern.literal() else {
                    return None;
                };
                if pattern.ends_with('%') && !pattern[..pattern.len() - 1].contains(['%', '_']) {
                    let prefix = Datum::string(pattern[..pattern.len() - 1].to_string());
                    Some(if *negated {
                        col.reference.not_starts_with(prefix)
                    } else {
                        col.reference.starts_with(prefix)
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn convert_comparison(
        &self,
        left: &Expr,
        right: &Expr,
        op: PredicateOperator,
    ) -> Option<Predicate> {
        if let Some(col) = self.extract_column(left) {
            let datum = self.const_datum_for_expr(right, &col.data_type)?;
            return Some(Predicate::Binary(BinaryExpression::new(
                op,
                col.reference,
                datum,
            )));
        }
        if let Some(col) = self.extract_column(right) {
            let datum = self.const_datum_for_expr(left, &col.data_type)?;
            return Some(Predicate::Binary(BinaryExpression::new(
                reverse_predicate_op(op),
                col.reference,
                datum,
            )));
        }
        None
    }

    fn extract_column(&self, expr: &Expr) -> Option<ColumnRef> {
        match expr {
            Expr::Column(column) => {
                let field = self.schema.field_with_name(column.name()).ok()?;
                Some(ColumnRef {
                    reference: Reference::new(column.name()),
                    data_type: field.data_type().clone(),
                })
            }
            Expr::Cast(cast) => self.extract_column(&cast.expr),
            Expr::Alias(alias) => self.extract_column(&alias.expr),
            _ => None,
        }
    }

    fn const_datum_for_expr(&self, expr: &Expr, target_type: &DataType) -> Option<Datum> {
        if let Some(ts) = extract_timestamp_at(expr, self.now) {
            return timestamp_datum_for_type(ts, target_type);
        }
        match expr {
            Expr::Literal(value, _) => scalar_value_to_datum(value, Some(target_type)),
            Expr::Cast(cast) => self.const_datum_for_expr(&cast.expr, target_type),
            Expr::Alias(alias) => self.const_datum_for_expr(&alias.expr, target_type),
            _ => None,
        }
    }

    fn generic_datum_for_expr(&self, expr: &Expr) -> Option<Datum> {
        match expr {
            Expr::Literal(value, _) => scalar_value_to_datum(value, None),
            Expr::Cast(cast) => self.generic_datum_for_expr(&cast.expr),
            Expr::Alias(alias) => self.generic_datum_for_expr(&alias.expr),
            _ => None,
        }
    }
}

fn combine_and(left: Option<Predicate>, right: Option<Predicate>) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn combine_or(left: Option<Predicate>, right: Option<Predicate>) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.or(right)),
        _ => None,
    }
}

fn to_predicate_op(op: Operator) -> Option<PredicateOperator> {
    match op {
        Operator::Eq => Some(PredicateOperator::Eq),
        Operator::NotEq => Some(PredicateOperator::NotEq),
        Operator::Lt => Some(PredicateOperator::LessThan),
        Operator::LtEq => Some(PredicateOperator::LessThanOrEq),
        Operator::Gt => Some(PredicateOperator::GreaterThan),
        Operator::GtEq => Some(PredicateOperator::GreaterThanOrEq),
        _ => None,
    }
}

fn reverse_predicate_op(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        other => other,
    }
}

fn scalar_value_to_datum(value: &ScalarValue, target_type: Option<&DataType>) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int8(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(Datum::int(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Datum::long(*v as i64)),
        ScalarValue::UInt16(Some(v)) => Some(Datum::long(*v as i64)),
        ScalarValue::UInt32(Some(v)) => Some(Datum::long(*v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(Datum::long),
        ScalarValue::Float32(Some(v)) => Some(Datum::double(*v as f64)),
        ScalarValue::Float64(Some(v)) => Some(Datum::double(*v)),
        ScalarValue::Utf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::LargeUtf8(Some(v)) => Some(Datum::string(v.clone())),
        ScalarValue::Binary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::LargeBinary(Some(v)) => Some(Datum::binary(v.clone())),
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::Date64(Some(v)) => Some(Datum::date((*v / (24 * 60 * 60 * 1000)) as i32)),
        ScalarValue::TimestampNanosecond(Some(v), _) => {
            timestamp_from_raw(*v, TimeUnit::Nanosecond, target_type)
        }
        ScalarValue::TimestampMicrosecond(Some(v), _) => {
            timestamp_from_raw(*v, TimeUnit::Microsecond, target_type)
        }
        ScalarValue::TimestampMillisecond(Some(v), _) => {
            timestamp_from_raw(*v, TimeUnit::Millisecond, target_type)
        }
        ScalarValue::TimestampSecond(Some(v), _) => {
            timestamp_from_raw(*v, TimeUnit::Second, target_type)
        }
        _ => None,
    }
}

fn timestamp_from_raw(raw: i64, unit: TimeUnit, target_type: Option<&DataType>) -> Option<Datum> {
    let ts = match unit {
        TimeUnit::Second => Utc.timestamp_opt(raw, 0).single()?,
        TimeUnit::Millisecond => Utc.timestamp_millis_opt(raw).single()?,
        TimeUnit::Microsecond => Utc.timestamp_micros(raw).single()?,
        TimeUnit::Nanosecond => Utc.timestamp_nanos(raw),
    };
    timestamp_datum_for_type(ts, target_type?)
}

fn timestamp_datum_for_type(ts: DateTime<Utc>, target_type: &DataType) -> Option<Datum> {
    let nanos = ts.timestamp_nanos_opt()?;
    match target_type {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => Some(Datum::timestamptz_nanos(nanos)),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        | DataType::Timestamp(TimeUnit::Millisecond, Some(_))
        | DataType::Timestamp(TimeUnit::Second, Some(_)) => {
            Some(Datum::timestamptz_micros(ts.timestamp_micros()))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(Datum::timestamp_nanos(nanos)),
        DataType::Timestamp(TimeUnit::Microsecond, None)
        | DataType::Timestamp(TimeUnit::Millisecond, None)
        | DataType::Timestamp(TimeUnit::Second, None) => {
            Some(Datum::timestamp_micros(ts.timestamp_micros()))
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct IntervalValue {
    months: i32,
    days: i32,
    nanos: i64,
}

fn extract_timestamp_at(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match expr {
        Expr::Literal(value, _) => literal_timestamp(value),
        Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
        Expr::Alias(alias) => extract_timestamp_at(&alias.expr, now),
        Expr::ScalarFunction(func)
            if is_current_timestamp_function(func.func.name(), &func.args) =>
        {
            Some(now)
        }
        Expr::ScalarFunction(func) => extract_function_timestamp(func.func.name(), &func.args, now),
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Plus => {
                if let (Some(ts), Some(interval)) = (
                    extract_timestamp_at(&binary.left, now),
                    extract_interval_value(&binary.right),
                ) {
                    return apply_interval(ts, interval, false);
                }
                if let (Some(interval), Some(ts)) = (
                    extract_interval_value(&binary.left),
                    extract_timestamp_at(&binary.right, now),
                ) {
                    return apply_interval(ts, interval, false);
                }
                None
            }
            Operator::Minus => {
                if let (Some(ts), Some(interval)) = (
                    extract_timestamp_at(&binary.left, now),
                    extract_interval_value(&binary.right),
                ) {
                    return apply_interval(ts, interval, true);
                }
                None
            }
            _ => None,
        },
        _ => None,
    }
}

fn literal_timestamp(value: &ScalarValue) -> Option<DateTime<Utc>> {
    use chrono::NaiveDate;
    use chrono::NaiveDateTime;
    match value {
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(Utc.timestamp_nanos(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Utc.timestamp_micros(*v).single(),
        ScalarValue::TimestampMillisecond(Some(v), _) => Utc.timestamp_millis_opt(*v).single(),
        ScalarValue::TimestampSecond(Some(v), _) => Utc.timestamp_opt(*v, 0).single(),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s
            .parse::<DateTime<Utc>>()
            .ok()
            .or_else(|| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc))
            })
            .or_else(|| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            })
            .or_else(|| {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            }),
        _ => None,
    }
}

fn extract_function_timestamp(
    name: &str,
    args: &[Expr],
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let lower = name.to_ascii_lowercase();
    if is_current_timestamp_function(&lower, args) {
        return Some(now);
    }
    let value = args.first()?;
    match lower.as_str() {
        "to_timestamp" | "to_timestamp_seconds" => extract_epoch_seconds(value, now),
        "to_timestamp_millis" => extract_epoch_millis(value, now),
        "to_timestamp_micros" => extract_epoch_micros(value, now),
        "to_timestamp_nanos" => extract_epoch_nanos(value, now),
        _ => None,
    }
}

fn extract_epoch_seconds(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let scalar = extract_scalar(expr, now)?;
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_opt(v, 0).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_opt(v as i64, 0).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .ok()
            .and_then(|secs| Utc.timestamp_opt(secs, 0).single()),
        _ => literal_timestamp(&scalar),
    }
}

fn extract_epoch_millis(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let scalar = extract_scalar(expr, now)?;
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_millis_opt(v).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_millis_opt(v as i64).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .ok()
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single()),
        _ => literal_timestamp(&scalar),
    }
}

fn extract_epoch_micros(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let scalar = extract_scalar(expr, now)?;
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_micros(v).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_micros(v as i64).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .ok()
            .and_then(|micros| Utc.timestamp_micros(micros).single()),
        _ => literal_timestamp(&scalar),
    }
}

fn extract_epoch_nanos(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let scalar = extract_scalar(expr, now)?;
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::Int16(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::Int32(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::Int64(Some(v)) => Some(Utc.timestamp_nanos(v)),
        ScalarValue::UInt8(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::UInt16(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::UInt32(Some(v)) => Some(Utc.timestamp_nanos(v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .ok()
            .map(|nanos| Utc.timestamp_nanos(nanos)),
        _ => literal_timestamp(&scalar),
    }
}

fn extract_scalar(expr: &Expr, now: DateTime<Utc>) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => extract_scalar(&cast.expr, now),
        Expr::Alias(alias) => extract_scalar(&alias.expr, now),
        Expr::ScalarFunction(func)
            if is_current_timestamp_function(func.func.name(), &func.args) =>
        {
            Some(ScalarValue::TimestampNanosecond(
                Some(now.timestamp_nanos_opt()?),
                Some("+00:00".into()),
            ))
        }
        _ => None,
    }
}

fn is_current_timestamp_function(name: &str, args: &[Expr]) -> bool {
    args.is_empty()
        && matches!(
            name.to_ascii_lowercase().as_str(),
            "now" | "current_timestamp" | "current_timestamp()"
        )
}

fn extract_interval_value(expr: &Expr) -> Option<IntervalValue> {
    let scalar = match expr {
        Expr::Literal(value, _) => value,
        Expr::Cast(cast) => return extract_interval_value(&cast.expr),
        Expr::Alias(alias) => return extract_interval_value(&alias.expr),
        _ => return None,
    };
    match scalar {
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            let (months, days, nanos) =
                datafusion::arrow::datatypes::IntervalMonthDayNanoType::to_parts(*v);
            Some(IntervalValue {
                months,
                days,
                nanos,
            })
        }
        ScalarValue::IntervalDayTime(Some(v)) => {
            let (days, millis) = datafusion::arrow::datatypes::IntervalDayTimeType::to_parts(*v);
            Some(IntervalValue {
                months: 0,
                days,
                nanos: millis as i64 * 1_000_000,
            })
        }
        ScalarValue::IntervalYearMonth(Some(v)) => {
            let months = datafusion::arrow::datatypes::IntervalYearMonthType::to_months(*v);
            Some(IntervalValue {
                months,
                days: 0,
                nanos: 0,
            })
        }
        _ => None,
    }
}

fn apply_interval(
    ts: DateTime<Utc>,
    interval: IntervalValue,
    subtract: bool,
) -> Option<DateTime<Utc>> {
    let mut out = ts;
    if interval.months != 0 {
        let months = Months::new(interval.months.unsigned_abs());
        out = if subtract == (interval.months > 0) {
            out.checked_sub_months(months)?
        } else {
            out.checked_add_months(months)?
        };
    }
    if interval.days != 0 {
        let days = Days::new(interval.days.unsigned_abs() as u64);
        out = if subtract == (interval.days > 0) {
            out.checked_sub_days(days)?
        } else {
            out.checked_add_days(days)?
        };
    }
    if interval.nanos != 0 {
        let delta = TimeDelta::nanoseconds(interval.nanos.abs());
        out = if subtract == (interval.nanos > 0) {
            out.checked_sub_signed(delta)?
        } else {
            out.checked_add_signed(delta)?
        };
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};
    use futures::Future;
    use iceberg::expr::Bind;
    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{DataContentType, DataFileFormat, Schema};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use siglake_core::{events_schema_with, PromotedColumn, PromotedType};
    use std::ops::Not;
    use tokio::sync::oneshot;

    type SnapshotVec = Vec<(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )>;

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

    #[test]
    fn frontier_pruning_keeps_equal_and_missing_bounds() {
        assert!(bound_is_strictly_behind(100, Some(99), true));
        assert!(!bound_is_strictly_behind(100, Some(100), true));
        assert!(!bound_is_strictly_behind(100, None, true));

        assert!(bound_is_strictly_behind(100, Some(101), false));
        assert!(!bound_is_strictly_behind(100, Some(100), false));
        assert!(!bound_is_strictly_behind(100, None, false));
    }

    /// The decline is a three-input decision and the scan that consumes it
    /// needs a warehouse, a text corpus and a plan to reach. Driving the
    /// resolver directly is what makes each arm — including the two that must
    /// NOT decline — cost nothing to state.
    #[test]
    fn text_index_declines_clipped_and_ordered_limits_only() {
        // No text predicate: nothing to decline, whatever the limits say.
        assert_eq!(text_index_decline_reason(false, true, true), None);
        assert_eq!(text_index_decline_reason(false, false, true), None);

        // An unclipped text scan is the regime the index wins in (#4329:
        // 129.3 ms indexed against 1,733.5 ms scanned for a 0.001%-density
        // term over 102.76M rows). It keeps the index.
        assert_eq!(text_index_decline_reason(true, false, false), None);

        assert_eq!(
            text_index_decline_reason(true, false, true),
            Some("clipped_limit")
        );
        assert_eq!(
            text_index_decline_reason(true, true, false),
            Some("ordered_limit")
        );
        // Both hold only if a caller sets both extensions; the reason is the
        // ordered one, which is the older and more specific finding.
        assert_eq!(
            text_index_decline_reason(true, true, true),
            Some("ordered_limit")
        );
    }

    struct GatedBatchStream {
        gate: oneshot::Receiver<()>,
        batch: Option<RecordBatch>,
        done: bool,
    }

    impl Stream for GatedBatchStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if this.done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut this.gate).poll(cx) {
                Poll::Ready(_) => {
                    if let Some(batch) = this.batch.take() {
                        this.done = true;
                        Poll::Ready(Some(Ok(batch)))
                    } else {
                        this.done = true;
                        Poll::Ready(None)
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }

    fn string_batch(width: usize) -> RecordBatch {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("host", DataType::Utf8, false),
        ]));
        let payload = "x".repeat(width);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(datafusion::arrow::array::StringArray::from(vec![
                payload,
            ]))],
        )
        .unwrap()
    }

    fn task(path: &str, rows: u64) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: rows,
            start: 0,
            length: rows,
            record_count: Some(rows),
            data_file_path: path.to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
            statistics_blobs: vec![],
        }
    }

    fn pushdown_test_schema() -> ArrowSchemaRef {
        events_schema_with(&[
            PromotedColumn {
                attr_key: "region".into(),
                name: "region".into(),
                ty: PromotedType::Utf8,
            },
            PromotedColumn {
                attr_key: "http.status_code".into(),
                name: "status".into(),
                ty: PromotedType::Int64,
            },
        ])
    }

    #[test]
    fn scan_shards_partition_files_disjointly_and_cover() {
        // 200 files across COUNT shards: every file lands in exactly one shard,
        // and the union of all shards is the whole file set.
        const COUNT: usize = 4;
        let tasks: Vec<FileScanTask> = (0..200)
            .map(|i| task(&format!("data/part-{i:04}.parquet"), 100))
            .collect();
        let all: std::collections::HashSet<String> = tasks
            .iter()
            .map(|t| t.data_file_path().to_string())
            .collect();

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut sizes = Vec::new();
        for index in 0..COUNT {
            let shard = ScanShard::new(index, COUNT).unwrap();
            let owned = shard_file_tasks(tasks.clone(), shard);
            sizes.push(owned.len());
            for t in &owned {
                let p = t.data_file_path().to_string();
                assert!(
                    seen.insert(p),
                    "file owned by more than one shard (not disjoint)"
                );
            }
        }
        assert_eq!(seen, all, "shards must cover every file exactly once");
        // FNV spread should be roughly balanced (no degenerate empty shard).
        assert!(
            sizes.iter().all(|&n| n > 0),
            "a shard got no files: {sizes:?}"
        );

        // No-op shards: count<=1 or index>=count.
        assert!(ScanShard::new(0, 1).is_none());
        assert!(ScanShard::new(3, 3).is_none());
        assert!(ScanShard::new(0, 2).is_some());
    }

    #[test]
    fn task_cache_key_ignores_query_predicate_but_keeps_delete_shape() {
        let mut left = task("a", 100);
        let mut right = task("a", 100);
        left.project_field_ids = vec![1, 2];
        right.project_field_ids = vec![1, 2];
        left.predicate = Some(
            Predicate::AlwaysTrue
                .bind(left.schema.clone(), true)
                .unwrap(),
        );
        right.predicate = Some(
            Predicate::AlwaysFalse
                .bind(right.schema.clone(), true)
                .unwrap(),
        );
        assert_eq!(task_cache_key(&left), task_cache_key(&right));

        right.deletes.push(FileScanTaskDeleteFile {
            file_path: "delete-a.parquet".into(),
            file_size_in_bytes: 42,
            file_type: DataContentType::EqualityDeletes,
            partition_spec_id: 7,
            equality_ids: Some(vec![9, 11]),
        });
        assert_ne!(task_cache_key(&left), task_cache_key(&right));
    }

    #[test]
    fn predicate_cache_key_keeps_predicate_and_existing_identity() {
        let mut left = task("a", 100);
        let mut right = task("a", 100);
        left.project_field_ids = vec![1, 2];
        right.project_field_ids = vec![1, 2];
        left.predicate = Some(
            Predicate::AlwaysTrue
                .bind(left.schema.clone(), true)
                .unwrap(),
        );
        right.predicate = Some(
            Predicate::AlwaysFalse
                .bind(right.schema.clone(), true)
                .unwrap(),
        );
        assert_ne!(
            task_cache_key_with_predicate_direction(&left, false),
            task_cache_key_with_predicate_direction(&right, false),
            "predicate literals/operators are part of the prototype key"
        );
        assert_ne!(
            task_cache_key_with_predicate_direction(&left, false),
            task_cache_key_with_predicate_direction(&left, true),
            "direction remains part of the prototype key"
        );

        right.predicate = left.predicate.clone();
        right.start += 1;
        assert_ne!(
            task_cache_key_with_predicate_direction(&left, false),
            task_cache_key_with_predicate_direction(&right, false),
            "the existing byte range remains part of the prototype key"
        );
    }

    #[test]
    fn cacheable_task_strips_only_query_predicate() {
        let mut original = task("a", 100);
        original.project_field_ids = vec![1];
        original.predicate = Some(
            Predicate::AlwaysTrue
                .bind(original.schema.clone(), true)
                .unwrap(),
        );
        original.deletes.push(FileScanTaskDeleteFile {
            file_path: "delete-a.parquet".into(),
            file_size_in_bytes: 42,
            file_type: DataContentType::EqualityDeletes,
            partition_spec_id: 7,
            equality_ids: Some(vec![9, 11]),
        });
        let cached = cacheable_task(&original);
        assert!(cached.predicate.is_none());
        assert_eq!(cached.deletes, original.deletes);
        assert_eq!(cached.project_field_ids, original.project_field_ids);
        assert_eq!(cached.data_file_path, original.data_file_path);
    }

    #[test]
    fn partition_file_tasks_balances_largest_files_first() {
        let partitions = partition_file_tasks(
            vec![task("a", 100), task("b", 90), task("c", 10), task("d", 10)],
            None,
            2,
            crate::QueryScanTuning::default(),
        );
        assert_eq!(partitions.len(), 2);
        let weights = partitions
            .iter()
            .map(|partition| {
                partition
                    .iter()
                    .map(|task| task.record_count.unwrap_or_default())
                    .sum::<u64>()
            })
            .collect::<Vec<_>>();
        assert_eq!(weights, vec![110, 100]);
    }

    /// WS-3 cluster arrangement: ASC and DESC orderings, equal boundary
    /// values staying disjoint, overlaps grouping into clusters (transitively),
    /// and byte-range-split tasks (identical bounds) clustering together.
    #[test]
    fn cluster_partition_orders_and_groups() {
        // ASC: out-of-order disjoint ranges arrange into the ascending run —
        // all singleton clusters.
        assert_eq!(
            cluster_partition(&[(20, 25), (0, 5), (10, 15)], false),
            (vec![1, 2, 0], vec![1, 1, 1])
        );
        // DESC: same ranges arrange newest-first.
        assert_eq!(
            cluster_partition(&[(20, 25), (0, 5), (10, 15)], true),
            (vec![0, 2, 1], vec![1, 1, 1])
        );
        // Equal boundary (prev.max == next.min) stays disjoint in both directions.
        assert_eq!(
            cluster_partition(&[(10, 20), (0, 10)], false),
            (vec![1, 0], vec![1, 1])
        );
        assert_eq!(
            cluster_partition(&[(0, 10), (10, 20)], true),
            (vec![1, 0], vec![1, 1])
        );
        // Overlap clusters (no longer refuses the whole partition).
        assert_eq!(
            cluster_partition(&[(0, 12), (10, 20)], false),
            (vec![0, 1], vec![2])
        );
        assert_eq!(
            cluster_partition(&[(0, 12), (10, 20)], true),
            (vec![1, 0], vec![2])
        );
        // Transitive overlap: a–b overlap, b–c overlap ⇒ one 3-cluster, even
        // though a–c are disjoint.
        assert_eq!(
            cluster_partition(&[(0, 12), (10, 22), (20, 30)], false),
            (vec![0, 1, 2], vec![3])
        );
        // Mixed: overlapping pair boxed between disjoint singletons.
        assert_eq!(
            cluster_partition(&[(40, 50), (0, 5), (10, 22), (20, 30)], false),
            (vec![1, 2, 3, 0], vec![1, 2, 1])
        );
        // Byte-range-split file: two tasks share the file's bounds — one cluster.
        assert_eq!(
            cluster_partition(&[(0, 10), (0, 10)], false),
            (vec![0, 1], vec![2])
        );
        // Single task and empty are trivial.
        assert_eq!(cluster_partition(&[(3, 9)], false), (vec![0], vec![1]));
        assert_eq!(cluster_partition(&[], true), (vec![], vec![]));
        // Zero-width equal tasks (single distinct timestamp): equal boundaries
        // are order-insensitive ⇒ disjoint singletons.
        assert_eq!(
            cluster_partition(&[(5, 5), (5, 5)], false),
            (vec![0, 1], vec![1, 1])
        );
    }

    /// Layering: fan-in must equal the overlap DEPTH, not the chain length —
    /// the wide-file shape (one file spanning everything + N narrow disjoint
    /// files) is depth 2 however long the chain is.
    #[test]
    fn layer_cluster_depth_bounds_fanin() {
        // One wide file over 20 disjoint narrow files → 2 layers.
        let mut bounds: Vec<(i64, i64)> = vec![(0, 2050)];
        bounds.extend((0..20).map(|i| (i * 100, i * 100 + 50)));
        let (perm, sizes) = layer_cluster(&bounds, false);
        assert_eq!(sizes.len(), 2, "wide + narrows = depth 2: {sizes:?}");
        assert_eq!(sizes.iter().sum::<usize>(), 21);
        assert_eq!(perm.len(), 21);
        // DESC direction: same depth.
        let (_, sizes) = layer_cluster(&bounds, true);
        assert_eq!(sizes.len(), 2);

        // A staircase chain a-b, b-c, c-d … is depth 2 as well (alternating
        // layers), never chain-length fan-in.
        let chain: Vec<(i64, i64)> = (0..10).map(|i| (i * 10, i * 10 + 15)).collect();
        let (_, sizes) = layer_cluster(&chain, false);
        assert_eq!(sizes.len(), 2, "staircase chain is depth 2: {sizes:?}");

        // True depth-3 stack.
        let (_, sizes) = layer_cluster(&[(0, 100), (0, 100), (0, 100)], false);
        assert_eq!(sizes.len(), 3);

        // Layers are internally disjoint runs in time order.
        let bounds = vec![(0, 2050), (0, 50), (100, 150), (200, 250)];
        let (perm, sizes) = layer_cluster(&bounds, false);
        let layer0: Vec<usize> = perm[..sizes[0]].to_vec();
        for w in layer0.windows(2) {
            assert!(
                bounds[w[0]].1 <= bounds[w[1]].0,
                "layer must be a disjoint run: {perm:?} {sizes:?}"
            );
        }
    }

    #[test]
    fn partition_file_tasks_disables_parallel_split_for_limit() {
        let partitions = partition_file_tasks(
            vec![task("a", 100), task("b", 90)],
            Some(10),
            8,
            crate::QueryScanTuning::default(),
        );
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 2);
    }

    #[test]
    fn pushdown_kind_marks_only_exact_safe_trees_exact() {
        let schema = pushdown_test_schema();
        let tokenizers = HashMap::new();

        let eq = col("host").eq(lit("us-east-2"));
        assert_eq!(
            pushdown_kind(&eq, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );
        assert_eq!(
            filter_pushdown_with_file_cache(TableProviderFilterPushDown::Exact, true),
            TableProviderFilterPushDown::Inexact,
            "whole-file cache reads require DataFusion's residual filter"
        );
        assert_eq!(
            filter_pushdown_with_file_cache(TableProviderFilterPushDown::Exact, false),
            TableProviderFilterPushDown::Exact,
            "the cache-disabled exact fast path must remain available"
        );

        let range = col("status")
            .gt_eq(lit(500_i64))
            .and(col("status").lt(lit(600_i64)));
        assert_eq!(
            pushdown_kind(&range, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );

        let in_list = col("region").in_list(vec![lit("us-east-2"), lit("us-west-1")], false);
        assert_eq!(
            pushdown_kind(&in_list, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );

        let and_of_exact = eq.clone().and(range.clone());
        assert_eq!(
            pushdown_kind(&and_of_exact, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );

        let or_of_exact = eq.clone().or(col("host").eq(lit("us-west-1")));
        assert_eq!(
            pushdown_kind(&or_of_exact, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );

        let not_of_exact = eq.clone().not();
        assert_eq!(
            pushdown_kind(&not_of_exact, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Exact
        );

        let like = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("host")),
            pattern: Box::new(lit("us-%")),
            escape_char: None,
            case_insensitive: false,
        });
        assert_eq!(
            pushdown_kind(&like, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Inexact
        );

        let unsupported = col("missing").eq(lit("x"));
        assert_eq!(
            pushdown_kind(&unsupported, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Unsupported
        );

        let mixed_or = eq.or(like.clone());
        assert_eq!(
            pushdown_kind(&mixed_or, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Inexact
        );

        let mixed_and = col("host").eq(lit("us-east-2")).and(like);
        assert_eq!(
            pushdown_kind(&mixed_and, &schema, &tokenizers, &HashMap::new()).unwrap(),
            TableProviderFilterPushDown::Inexact
        );
    }

    #[tokio::test]
    async fn limit_record_batch_stream_slices_last_batch_and_stops() {
        let stream = futures::stream::iter(vec![
            Ok::<_, DataFusionError>(
                RecordBatch::try_new(
                    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                        datafusion::arrow::datatypes::Field::new("value", DataType::Int32, false),
                    ])),
                    vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                        1, 2, 3,
                    ]))],
                )
                .unwrap(),
            ),
            Ok::<_, DataFusionError>(
                RecordBatch::try_new(
                    Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
                        datafusion::arrow::datatypes::Field::new("value", DataType::Int32, false),
                    ])),
                    vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                        4, 5, 6,
                    ]))],
                )
                .unwrap(),
            ),
        ])
        .boxed();

        let batches = limit_record_batch_stream(stream, 4)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            4
        );
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn ordered_limit_caps_the_source_when_datafusion_does_not_push_it() {
        assert_eq!(
            effective_ordered_source_limit(None, Some(100), true),
            Some(100)
        );
        assert_eq!(
            effective_ordered_source_limit(Some(1_000), Some(100), true),
            Some(100),
            "the tighter limit must win"
        );
        assert_eq!(
            effective_ordered_source_limit(Some(50), Some(100), true),
            Some(50),
            "an existing pushed limit must not be widened"
        );
        assert_eq!(
            effective_ordered_source_limit(None, Some(100), false),
            None,
            "a residual-filtered partition may need to scan past 100 rows to find 100 matches"
        );
    }

    #[test]
    fn cache_population_drops_an_oversized_candidate_before_eof() {
        let batch = string_batch(64);
        let batch_bytes = batch.get_array_memory_size() as u64;
        let tuning = EffectiveFileCacheTuning {
            // One batch fits exactly; the second crosses the quarter-budget
            // entry limit while a third batch remains unread.
            max_bytes: Some(batch_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION)),
            max_entries: Some(1),
            row_group_prototype: false,
            predicate_key_prototype: false,
        };
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let inner: TaskBatchStream = futures::stream::iter([
                    Ok::<_, DataFusionError>(batch.clone()),
                    Ok(batch.clone()),
                    Ok(batch.clone()),
                ])
                .boxed();
                let mut stream = CachePopulateStream {
                    key: "oversized-before-eof".to_string(),
                    tuning,
                    inner,
                    buffered: Vec::new(),
                    buffered_bytes: 0,
                    oversized: false,
                    insert_done: false,
                    charge: PopulationCharge::open(),
                    yielded_rows: 0,
                    end: PopulateEnd::Unpolled,
                    cache_counters: Arc::new(FileCacheCounters::default()),
                };

                assert!(stream.next().await.unwrap().is_ok());
                assert_eq!(stream.buffered_bytes, batch_bytes);
                assert_eq!(stream.buffered.len(), 1);

                assert!(stream.next().await.unwrap().is_ok());
                assert!(stream.oversized);
                assert_eq!(stream.buffered_bytes, 0);
                assert!(stream.buffered.is_empty());
                assert!(!stream.insert_done, "the source still has another batch");

                assert!(stream.next().await.unwrap().is_ok());
                assert_eq!(stream.buffered_bytes, 0);
                assert!(stream.buffered.is_empty());
                assert!(stream.next().await.is_none());
            })
        });

        assert_eq!(
            counter_sum(
                &snapshotter.snapshot().into_vec(),
                "siglake_query_scan_file_cache_requests_total",
                Some(("outcome", "skip_oversized")),
            ),
            1,
            "an oversized candidate is abandoned exactly once"
        );
    }

    #[test]
    fn cache_population_prices_a_slice_by_its_retained_backing_allocation() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("value", DataType::Int32, false),
        ]));
        let owner = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                datafusion::arrow::array::Int32Array::from_iter_values(0..16_384_i32),
            )],
        )
        .unwrap();
        let slice = owner.slice(8_192, 1);
        let extent_bytes = batch_extent_bytes(&slice);
        let retained_bytes = batch_retained_bytes(&slice);
        let priced_bytes = slice.get_array_memory_size() as u64;
        drop(owner);

        assert_eq!(
            slice
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            8_192,
            "the slice remains backed after its unsliced owner is dropped"
        );
        assert!(
            retained_bytes > extent_bytes,
            "fixture must retain more than its compact slice extent: \
             retained={retained_bytes}, extent={extent_bytes}"
        );
        assert!(
            priced_bytes >= retained_bytes,
            "the admission price must cover the retained Arrow allocation: \
             priced={priced_bytes}, retained={retained_bytes}"
        );

        let stream = |key: &str, tuning| CachePopulateStream {
            key: key.to_string(),
            tuning,
            inner: futures::stream::iter([Ok::<_, DataFusionError>(slice.clone())]).boxed(),
            buffered: Vec::new(),
            buffered_bytes: 0,
            oversized: false,
            insert_done: false,
            charge: PopulationCharge::open(),
            yielded_rows: 0,
            end: PopulateEnd::Unpolled,
            cache_counters: Arc::new(FileCacheCounters::default()),
        };

        futures::executor::block_on(async {
            let extent_limit = EffectiveFileCacheTuning {
                // The compact slice extent fits exactly, while the allocation the
                // cached clone retains does not.
                max_bytes: Some(extent_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION)),
                max_entries: Some(1),
                row_group_prototype: false,
                predicate_key_prototype: false,
            };
            let mut refused = stream("sliced-refused", extent_limit);
            assert!(refused.next().await.unwrap().is_ok());
            assert!(refused.oversized);
            assert!(refused.buffered.is_empty());
            drop(refused);

            let retained_limit = EffectiveFileCacheTuning {
                max_bytes: Some(priced_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION)),
                max_entries: Some(1),
                row_group_prototype: false,
                predicate_key_prototype: false,
            };
            let mut admitted = stream("sliced-admitted", retained_limit);
            assert!(admitted.next().await.unwrap().is_ok());
            assert_eq!(admitted.buffered_bytes, priced_bytes);

            let mut cache = QueryFileBatchCache::default();
            admitted.insert_buffered(&mut cache);
            admitted.insert_done = true;
            assert_eq!(cache.get("sliced-admitted").unwrap().bytes, priced_bytes);
        });
    }

    /// #4846's counter and the three ways a population finishes without one.
    /// One test because all four arms read the same counter and the
    /// already-populated arm needs the process-wide cache, which parallel
    /// `#[test]` functions in this binary would otherwise interleave with.
    #[test]
    fn unfinished_eligible_populations_are_counted_once() {
        let batch = string_batch(64);
        let batch_bytes = batch.get_array_memory_size() as u64;
        let roomy = EffectiveFileCacheTuning {
            max_bytes: Some(batch_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION * 8)),
            max_entries: Some(8),
            row_group_prototype: false,
            predicate_key_prototype: false,
        };
        // One batch fits; the second crosses the quarter-budget entry bound.
        let tight = EffectiveFileCacheTuning {
            max_bytes: Some(batch_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION)),
            max_entries: Some(8),
            row_group_prototype: false,
            predicate_key_prototype: false,
        };
        let source = |batches: usize| -> TaskBatchStream {
            futures::stream::iter(
                std::iter::repeat_n(batch.clone(), batches).map(Ok::<_, DataFusionError>),
            )
            .boxed()
        };
        let populate = |key: &str, tuning, inner| CachePopulateStream {
            key: key.to_string(),
            tuning,
            inner,
            buffered: Vec::new(),
            buffered_bytes: 0,
            oversized: false,
            insert_done: false,
            charge: PopulationCharge::open(),
            yielded_rows: 0,
            end: PopulateEnd::Unpolled,
            cache_counters: Arc::new(FileCacheCounters::default()),
        };

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let outcome = |snapshot: &SnapshotVec, name: &str| {
            counter_sum(
                snapshot,
                "siglake_query_scan_file_cache_requests_total",
                Some(("outcome", name)),
            )
        };

        // Every drop happens inside the local recorder's scope: a
        // `CachePopulateStream` counts from `Drop`, so a stream that outlived
        // the closure would charge the global recorder instead.
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                // A clipped read: one batch taken of three, then dropped. The
                // buffered batch and the decode behind it are thrown away.
                let mut clipped = populate("abandoned-clipped", roomy, source(3));
                assert!(clipped.next().await.unwrap().is_ok());
                assert_eq!(clipped.buffered.len(), 1);
                drop(clipped);

                // Dropped before the first poll — the partition's task opened
                // (its `miss` is already charged) and the plan finished
                // elsewhere. Still an eligible population that kept nothing.
                drop(populate("abandoned-unpolled", roomy, source(3)));

                // Oversized, then dropped before end-of-stream: `poll_next`
                // charged `skip_oversized` when it crossed the bound, so this
                // must NOT be charged again.
                let mut oversized = populate("oversized-then-dropped", tight, source(3));
                assert!(oversized.next().await.unwrap().is_ok());
                assert!(oversized.next().await.unwrap().is_ok());
                assert!(oversized.oversized);
                assert!(!oversized.insert_done, "a third batch is still unread");
                drop(oversized);

                // An error finishes the population: nothing more will be read,
                // so there is nothing to abandon.
                let failing: TaskBatchStream = futures::stream::iter([
                    Ok(batch.clone()),
                    Err(DataFusionError::External("decode failed".into())),
                    Ok(batch.clone()),
                ])
                .boxed();
                let mut errored = populate("errored", roomy, failing);
                assert!(errored.next().await.unwrap().is_ok());
                assert!(errored.next().await.unwrap().is_err());
                assert!(errored.insert_done);
                drop(errored);

                // A key some other partition populated first: `insert_buffered`
                // has nothing to do and charges no `insert`, but the stream ran
                // to end-of-stream and is finished. Both passes over the same
                // key go through the process-wide cache.
                for pass in 1..=2 {
                    let mut drained = populate("already-populated", roomy, source(1));
                    assert!(drained.next().await.unwrap().is_ok());
                    assert!(drained.next().await.is_none());
                    assert!(drained.insert_done, "pass {pass} reached end-of-stream");
                    drop(drained);
                }
            })
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(
            outcome(&snapshot, "abandoned"),
            2,
            "the clipped and the unpolled population, and nothing else: {snapshot:?}"
        );
        assert_eq!(
            outcome(&snapshot, "skip_oversized"),
            1,
            "the oversized stream keeps its one charge from poll_next"
        );
        assert_eq!(
            outcome(&snapshot, "insert"),
            1,
            "only the first pass over `already-populated` inserts"
        );
    }

    #[test]
    fn partition_file_tasks_adapts_down_for_smaller_scans() {
        let partitions = partition_file_tasks(
            vec![
                task("a", 100),
                task("b", 90),
                task("c", 80),
                task("d", 70),
                task("e", 60),
                task("f", 50),
            ],
            None,
            8,
            crate::QueryScanTuning {
                adaptive_partition_min_files: Some(2),
                adaptive_partition_min_bytes: Some(200),
                ..Default::default()
            },
        );
        assert_eq!(partitions.len(), 3);
    }

    #[test]
    fn output_rows_planned_rows_ratio_handles_zero_and_nonzero() {
        assert_eq!(output_rows_planned_rows_ratio(10, 0), 0.0);
        assert_eq!(output_rows_planned_rows_ratio(9, 3), 3.0);
    }

    #[test]
    fn get_schema_column_names_preserves_field_order() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("zeta", DataType::Utf8, true),
            datafusion::arrow::datatypes::Field::new("alpha", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new("beta", DataType::Boolean, true),
        ]));
        assert_eq!(
            get_schema_column_names(&schema),
            vec!["zeta".to_string(), "alpha".to_string(), "beta".to_string()]
        );
    }

    #[test]
    fn effective_reader_tuning_leaves_ranges_disabled_when_unconfigured() {
        let tuning = effective_reader_tuning(
            crate::QueryScanTuning::default(),
            200,
            500_000_000,
            OrderedScanTuning::default(),
        );
        assert_eq!(
            tuning,
            EffectiveReaderTuning {
                batch_size: None,
                range_coalesce_bytes: None,
                range_fetch_concurrency: None,
                ..Default::default()
            }
        );
    }

    #[test]
    fn effective_reader_tuning_applies_range_settings_without_thresholds() {
        let tuning = effective_reader_tuning(
            crate::QueryScanTuning {
                batch_size: Some(65_536),
                range_coalesce_bytes: Some(4 * 1024 * 1024),
                range_fetch_concurrency: Some(32),
                ..Default::default()
            },
            93,
            260_334_769,
            OrderedScanTuning::default(),
        );
        assert_eq!(
            tuning,
            EffectiveReaderTuning {
                batch_size: Some(65_536),
                range_coalesce_bytes: Some(4 * 1024 * 1024),
                range_fetch_concurrency: Some(32),
                ..Default::default()
            }
        );
    }

    #[test]
    fn effective_reader_tuning_respects_adaptive_thresholds() {
        let base = crate::QueryScanTuning {
            batch_size: Some(65_536),
            range_coalesce_bytes: Some(4 * 1024 * 1024),
            range_fetch_concurrency: Some(32),
            range_adaptive_min_files: Some(128),
            range_adaptive_min_bytes: Some(320 * 1024 * 1024),
            ..Default::default()
        };
        let small = effective_reader_tuning(base, 93, 260_334_769, OrderedScanTuning::default());
        assert_eq!(
            small,
            EffectiveReaderTuning {
                batch_size: Some(65_536),
                range_coalesce_bytes: None,
                range_fetch_concurrency: None,
                ..Default::default()
            }
        );
        let large = effective_reader_tuning(base, 186, 520_669_538, OrderedScanTuning::default());
        assert_eq!(
            large,
            EffectiveReaderTuning {
                batch_size: Some(65_536),
                range_coalesce_bytes: Some(4 * 1024 * 1024),
                range_fetch_concurrency: Some(32),
                ..Default::default()
            }
        );
    }

    #[test]
    fn effective_file_concurrency_limit_respects_reader_budget() {
        let tuning = crate::QueryScanTuning {
            reader_budget: Some(32),
            ..Default::default()
        };
        assert_eq!(effective_file_concurrency_limit(8, tuning), 4);
        assert_eq!(effective_file_concurrency_limit(16, tuning), 2);
    }

    #[test]
    fn effective_file_concurrency_limit_caps_explicit_limit_by_budget() {
        let tuning = crate::QueryScanTuning {
            reader_budget: Some(32),
            file_concurrency_limit: Some(8),
            ..Default::default()
        };
        assert_eq!(effective_file_concurrency_limit(16, tuning), 2);
    }

    #[test]
    fn effective_file_concurrency_limit_keeps_explicit_limit_without_budget() {
        let tuning = crate::QueryScanTuning {
            file_concurrency_limit: Some(3),
            ..Default::default()
        };
        assert_eq!(effective_file_concurrency_limit(16, tuning), 3);
    }

    #[test]
    fn ordered_budget_floor_guarantees_merge_liveness() {
        // Ordered: budget below partition count floors up (liveness).
        assert_eq!(ordered_reader_budget_floor(2, true, 15), 15);
        // Ordered with ample budget: unchanged.
        assert_eq!(ordered_reader_budget_floor(64, true, 15), 64);
        // Unordered: the memory clamp stands.
        assert_eq!(ordered_reader_budget_floor(2, false, 15), 2);
        assert_eq!(ordered_reader_budget_floor(2, true, 0), 2);
    }

    #[test]
    fn adaptive_fanin_default_scales_with_cpus_within_bounds() {
        // Whatever the host, the result respects the [floor, ceiling] clamp.
        let v = adaptive_fanin_default(16, 4, 64);
        assert!((16..=64).contains(&v), "{v}");
        let g = adaptive_fanin_default(32, 8, 128);
        assert!((32..=128).contains(&g), "{g}");
        // Floor dominates tiny hosts: 1-cpu math would be 4, clamps to 16.
        assert_eq!(1usize.saturating_mul(4).clamp(16, 64), 16);
    }

    #[test]
    fn clamp_aggregate_reader_budget_keeps_budget_for_small_files() {
        let planned_files = 48;
        let planned_bytes = 48 * 1024 * 1024;
        assert_eq!(
            clamp_aggregate_reader_budget(
                32,
                planned_files,
                planned_bytes,
                DEFAULT_SCAN_DECODED_BUDGET_BYTES,
                DEFAULT_SCAN_DECOMPRESSION_FACTOR,
            ),
            32
        );
    }

    #[test]
    fn clamp_aggregate_reader_budget_floors_huge_files_to_two() {
        let planned_files = 48;
        let planned_bytes = 48 * 256 * 1024 * 1024;
        assert_eq!(
            clamp_aggregate_reader_budget(
                32,
                planned_files,
                planned_bytes,
                DEFAULT_SCAN_DECODED_BUDGET_BYTES,
                DEFAULT_SCAN_DECOMPRESSION_FACTOR,
            ),
            2
        );
    }

    #[test]
    fn clamp_aggregate_reader_budget_handles_zero_and_missing_values() {
        assert_eq!(
            clamp_aggregate_reader_budget(
                32,
                0,
                0,
                DEFAULT_SCAN_DECODED_BUDGET_BYTES,
                DEFAULT_SCAN_DECOMPRESSION_FACTOR,
            ),
            32
        );
        assert_eq!(
            clamp_aggregate_reader_budget(32, 4, 1024, DEFAULT_SCAN_DECODED_BUDGET_BYTES, 0,),
            32
        );
    }

    /// The env resolver behind `OrderedTaskDrain::new`: an explicit integer
    /// wins, anything else is the default. Drives the pure `_from` function so
    /// nothing here touches the process environment.
    #[test]
    fn ordered_drain_buffer_bytes_is_overridable_and_falls_back_to_the_default() {
        assert_eq!(ordered_drain_buffer_bytes_from(Some("65536")), 65536);
        for cleared in [None, Some(""), Some("-1"), Some("garbage")] {
            assert_eq!(
                ordered_drain_buffer_bytes_from(cleared),
                DEFAULT_ORDERED_DRAIN_BUFFER_BYTES,
                "budget must default for {cleared:?}"
            );
        }
    }

    /// #4865's ramp factor resolver. `0` is the documented off switch (and the
    /// negative-control arm of the local A/B); a factor of 1 would never widen
    /// the ramp, so it clamps to 2. Drives the pure `_from` function, so
    /// nothing here touches the process environment.
    #[test]
    fn clipped_admission_wave_is_overridable_and_never_stalls() {
        assert_eq!(clipped_admission_wave_from(Some("0")), 0);
        assert_eq!(clipped_admission_wave_from(Some("4")), 4);
        assert_eq!(
            clipped_admission_wave_from(Some("1")),
            2,
            "a factor of 1 would leave the ramp at its initial width forever"
        );
        for cleared in [None, Some(""), Some("-1"), Some("garbage")] {
            assert_eq!(
                clipped_admission_wave_from(cleared),
                DEFAULT_SCAN_CLIPPED_ADMISSION_WAVE,
                "wave must default for {cleared:?}"
            );
        }
    }

    /// The ramp itself: ticket 0 runs at once, the rest wait, and the admitted
    /// count doubles each time the cumulative credits reach it — at 1, 2, 4 and
    /// 8. Eight credits open all fifteen partitions; one credit opens two,
    /// which is the whole point.
    #[test]
    fn the_admission_ramp_widens_one_wave_at_a_time() {
        let admission = ScanAdmission::new(15, 2);
        let tickets: Vec<usize> = (0..15).map(|_| admission.register()).collect();
        assert_eq!(tickets, (0..15).collect::<Vec<_>>());
        assert!(admission.is_admitted(0));
        assert!(!admission.is_admitted(1));
        for (credits, expected) in [
            (0usize, 1usize),
            (1, 2),
            (2, 4),
            (3, 4),
            (4, 8),
            (7, 8),
            (8, 15),
            (40, 15),
        ] {
            let admission = ScanAdmission::new(15, 2);
            for _ in 0..credits {
                admission.advance();
            }
            let open = (0..15).filter(|t| admission.is_admitted(*t)).count();
            assert_eq!(open, expected, "after {credits} credits");
        }
    }

    /// Tickets are handed out modulo the partition count, so a caller that
    /// executes one partition (or executes the same plan twice) is never left
    /// holding a ticket the ramp can never reach.
    #[test]
    fn admission_tickets_wrap_so_a_lone_partition_runs() {
        let admission = ScanAdmission::new(4, 2);
        assert_eq!(admission.register(), 0);
        assert!(admission.is_admitted(0));
        let admission = ScanAdmission::new(4, 2);
        for _ in 0..4 {
            admission.register();
        }
        assert_eq!(admission.register(), 0, "a second round restarts at 0");
    }

    /// A gated partition's wait resolves as soon as the ramp reaches it, and
    /// not before. `timeout(ZERO)` is deliberately avoided (#2931): the future
    /// is hand-polled instead, so the assertion cannot race a timer tick.
    #[tokio::test]
    async fn a_gated_partition_waits_until_the_ramp_reaches_it() {
        let admission = Arc::new(ScanAdmission::new(4, 2));
        for _ in 0..4 {
            admission.register();
        }
        let waiting = admission.clone();
        let mut wait = async move { waiting.wait(3).await }.boxed();
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(
            wait.poll_unpin(&mut cx).is_pending(),
            "ticket 3 must not run while the ramp is at 1"
        );
        admission.advance();
        assert!(wait.poll_unpin(&mut cx).is_pending(), "ramp at 2");
        admission.advance();
        assert!(wait.poll_unpin(&mut cx).is_ready(), "ramp at 4");
    }

    #[tokio::test]
    async fn ordered_task_drain_counts_budget_backpressure() {
        // The 64 KiB budget is passed to the drain directly rather than via
        // SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES: the old `set_var` was never
        // removed, and env vars are process-global, so it shrank the budget of
        // every other OrderedTaskDrain built in this binary after it ran.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");

        let (front_tx, front_rx) = oneshot::channel();
        let front_batch = string_batch(16);
        let buffered_batch = string_batch(128 * 1024);
        let tasks = futures::stream::iter(vec![
            futures::future::ready(Ok::<TaskBatchStream, DataFusionError>(Box::pin(
                GatedBatchStream {
                    gate: front_rx,
                    batch: Some(front_batch.clone()),
                    done: false,
                },
            )))
            .boxed(),
            futures::future::ready(Ok::<TaskBatchStream, DataFusionError>(
                futures::stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                    buffered_batch.clone(),
                )])
                .boxed(),
            ))
            .boxed(),
        ]);

        let collect = tokio::spawn(async move {
            OrderedTaskDrain::with_budget_bytes(tasks, 2, "test", 65536)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        front_tx.send(()).unwrap();

        let batches = collect.await.unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
        assert!(
            counter_sum(
                &snapshotter.snapshot().into_vec(),
                "siglake_query_ordered_drain_backpressure_total",
                Some(("path", "test")),
            ) > 0,
            "non-front task must register budget backpressure"
        );
    }

    fn wide_ordered_sort_batch(
        schema: &ArrowSchemaRef,
        timestamps: &[i64],
        payload_bytes: usize,
    ) -> RecordBatch {
        let payloads = timestamps
            .iter()
            .map(|timestamp| format!("{timestamp}:{}", "x".repeat(payload_bytes)))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::TimestampNanosecondArray::from(
                    timestamps.to_vec(),
                )),
                Arc::new(datafusion::arrow::array::StringArray::from(payloads)),
            ],
        )
        .unwrap()
    }

    fn ordered_sort_test_streams(batches: &[RecordBatch]) -> Vec<TaskBatchStream> {
        batches
            .iter()
            .cloned()
            .map(|batch| {
                let stream: TaskBatchStream =
                    futures::stream::iter([Ok::<_, DataFusionError>(batch)]).boxed();
                stream
            })
            .collect()
    }

    /// Drives the execution helper used by `OrderedCluster::Sort` with rows
    /// whose payload dominates their row count. A row-only gate admits both
    /// attempts; the byte pool must reject the first before concatenation and
    /// retain correct timestamp order when the derived peak is admitted.
    #[tokio::test]
    async fn eager_ordered_sort_refuses_wide_rows_and_orders_when_admitted() {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            datafusion::arrow::datatypes::Field::new("raw", DataType::Utf8, false),
        ]));
        let batches = vec![
            wide_ordered_sort_batch(&schema, &[30, 10], 256 * 1024),
            wide_ordered_sort_batch(&schema, &[40, 20], 256 * 1024),
        ];
        let retained_bytes = batches
            .iter()
            .map(|batch| ordered_sort_batch_bytes(batch).unwrap())
            .sum::<usize>();
        let rows = batches.iter().map(RecordBatch::num_rows).sum();
        let peak_bytes = ordered_sort_peak_bytes(retained_bytes, rows).unwrap();

        // Enough for collection, deliberately not enough for its preflighted
        // concatenation/output + index peak.
        let refused_limit = retained_bytes + (peak_bytes - retained_bytes) / 2;
        let refused_pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(refused_limit));
        let refused_reservation =
            MemoryConsumer::new("wide-ordered-sort-refused").register(&refused_pool);
        let mut refused = eager_sort_record_batch_stream(
            ordered_sort_test_streams(&batches),
            schema.clone(),
            false,
            refused_reservation,
        );
        let error = refused
            .next()
            .await
            .expect("the sort stream emits its bounded failure")
            .expect_err("the transformation peak must not fit");
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "bounded refusal must remain classifiable as ResourcesExhausted: {error:?}"
        );
        assert!(
            error.to_string().contains("preflight"),
            "the wide rows should pass collection and fail before concat: {error}"
        );
        assert_eq!(
            refused_pool.reserved(),
            0,
            "a refused eager sort must release its collected-byte reservation"
        );

        let admitted_pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(peak_bytes + 1024));
        let admitted_reservation =
            MemoryConsumer::new("wide-ordered-sort-admitted").register(&admitted_pool);
        let mut admitted = eager_sort_record_batch_stream(
            ordered_sort_test_streams(&batches),
            schema,
            false,
            admitted_reservation,
        );
        let output = admitted
            .next()
            .await
            .expect("an admitted sort emits one batch")
            .expect("the derived peak fits");
        let timestamps = output
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::TimestampNanosecondArray>()
            .unwrap()
            .values()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![10, 20, 30, 40]);
        assert_eq!(output.num_rows(), 4, "the sort must retain every wide row");
        assert_eq!(
            admitted_pool.reserved(),
            ordered_sort_batch_bytes(&output).unwrap(),
            "the emitted output must remain charged to the pool"
        );
        assert!(admitted.next().await.is_none());
        drop(admitted);
        assert_eq!(
            admitted_pool.reserved(),
            0,
            "dropping the segment must release its output reservation"
        );
    }
}

#[cfg(test)]
mod query_cancel_tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap()
    }

    /// The take_while the scan wraps its source in. An ENDLESS source stands in
    /// for the real thing: a cancelled 2B-row scan that, before this, kept
    /// running for 35 minutes after the query was gone.
    #[tokio::test]
    async fn cancelling_ends_an_endless_scan() {
        let cancel = QueryCancel::new();
        let c = cancel.clone();
        let src = futures::stream::repeat_with(move || Ok::<_, DataFusionError>(batch()));
        let mut guarded = src
            .take_while(move |_| futures::future::ready(!c.is_cancelled()))
            .boxed();

        // It yields while the query is live.
        assert!(guarded.next().await.is_some(), "source must produce first");
        assert!(guarded.next().await.is_some());

        cancel.cancel();

        assert!(
            guarded.next().await.is_none(),
            "a cancelled scan must END; an endless source that keeps yielding is \
             exactly the defect — the pumps above it never see end-of-input"
        );
    }

    /// Without cancellation the stream is untouched, so scans for callers that
    /// inject no token (tests, internal scans) behave exactly as before.
    #[tokio::test]
    async fn an_uncancelled_scan_keeps_producing() {
        let cancel = QueryCancel::new();
        let c = cancel.clone();
        let mut guarded = futures::stream::iter(vec![
            Ok::<_, DataFusionError>(batch()),
            Ok(batch()),
            Ok(batch()),
        ])
        .take_while(move |_| futures::future::ready(!c.is_cancelled()))
        .boxed();
        let mut n = 0;
        while guarded.next().await.is_some() {
            n += 1;
        }
        assert_eq!(n, 3, "no cancellation must mean no behaviour change");
    }

    /// The guard is what makes the CANCELLATION path -- the one that leaked --
    /// impossible to forget: dropping it cancels.
    #[test]
    fn dropping_the_guard_cancels() {
        let cancel = QueryCancel::new();
        assert!(!cancel.is_cancelled());
        {
            let _g = CancelOnDrop(cancel.clone());
            assert!(!cancel.is_cancelled(), "live while the request runs");
        }
        assert!(
            cancel.is_cancelled(),
            "dropping the request must cancel its scans"
        );
    }

    #[test]
    fn cancel_is_idempotent() {
        let cancel = QueryCancel::new();
        cancel.cancel();
        cancel.cancel();
        assert!(cancel.is_cancelled());
    }
}

#[cfg(test)]
mod decode_budget_pool_tests {
    use super::reserve_decode_budget;
    use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool};
    use std::sync::Arc;

    fn pool(bytes: usize) -> Arc<dyn MemoryPool> {
        Arc::new(FairSpillPool::new(bytes))
    }

    const PER_FILE: u64 = 100 * 1024 * 1024; // 100 MiB

    /// Plenty of room: the scan gets the concurrency it asked for.
    #[test]
    fn a_roomy_pool_grants_the_desired_concurrency() {
        let p = pool(4 * 1024 * 1024 * 1024);
        let (concurrency, reservation) = reserve_decode_budget(&p, 8, PER_FILE);
        assert_eq!(concurrency, 8);
        assert!(reservation.is_some(), "should have reserved");
    }

    /// The point of the change: the reservation is visible to the pool, so a
    /// second scan sees less room. Previously each query took a flat per-query
    /// budget that no other query could observe.
    #[test]
    fn a_reservation_is_visible_to_the_next_scan() {
        let p = pool(1024 * 1024 * 1024); // 1 GiB = 10 files
        let (first, held) = reserve_decode_budget(&p, 8, PER_FILE);
        assert_eq!(first, 8);
        let (second, _) = reserve_decode_budget(&p, 8, PER_FILE);
        assert!(
            second < 8,
            "the second scan got full concurrency ({second}) while the first held \
             {first} files' worth — the budgets are not sharing a pool"
        );
        drop(held);
    }

    /// Degradation, not failure: a nearly-full pool still lets a scan run.
    #[test]
    fn a_full_pool_degrades_to_one_file_rather_than_failing() {
        let p = pool(256 * 1024 * 1024);
        let hog = MemoryConsumer::new("hog").register(&p);
        hog.try_grow(250 * 1024 * 1024).unwrap();

        let (concurrency, reservation) = reserve_decode_budget(&p, 16, PER_FILE);
        assert_eq!(
            concurrency, 1,
            "must floor at one file, not zero or a failure"
        );
        assert!(
            reservation.is_none(),
            "at the floor it proceeds unreserved by design — see the doc comment"
        );
    }

    /// Releasing gives the budget back, so a finished (or CANCELLED) query does
    /// not permanently shrink what everyone else can use.
    #[test]
    fn dropping_the_reservation_returns_the_budget() {
        let p = pool(1024 * 1024 * 1024);
        let (_, held) = reserve_decode_budget(&p, 10, PER_FILE);
        let (squeezed, _) = reserve_decode_budget(&p, 8, PER_FILE);
        drop(held);
        let (after, _) = reserve_decode_budget(&p, 8, PER_FILE);
        assert!(
            after > squeezed,
            "after releasing, concurrency should recover ({squeezed} -> {after})"
        );
    }

    /// An unknown per-file size must not divide by zero or reserve nonsense.
    #[test]
    fn an_unknown_file_size_reserves_nothing() {
        let p = pool(1024 * 1024 * 1024);
        let (concurrency, reservation) = reserve_decode_budget(&p, 8, 0);
        assert_eq!(concurrency, 8);
        assert!(reservation.is_none());
    }
}
