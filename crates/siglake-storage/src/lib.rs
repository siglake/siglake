//! Storage layer: writes Arrow [`RecordBatch`]es to Parquet on an
//! [`ObjectStore`] and registers Parquet directories as DataFusion tables.
//!
//! Phase 1 uses a single-file-per-batch write strategy with a sync
//! [`ArrowWriter`] buffering into memory before a single PUT. This is fine
//! for smoke tests and small-file ingest. The compactor in phase 2 will own
//! the streaming multipart write path and the row-group sizing strategy.
//!
//! Phase 1.5 adds the [`iceberg`] module: an Iceberg-backed catalog with
//! auto-created `siglake.events` table and a DataFusion table provider.

pub mod aws_credential;
pub mod catalog_claim;
pub mod consumed_proof;
pub mod iceberg;
pub mod index_manager;
pub mod merge;
mod query_provider;
pub mod schema_version;
pub mod subscribe;
pub mod wal_ledger;

pub use query_provider::{
    clear_decoded_file_cache, clear_row_group_layout_cache, decoded_file_cache_footprint,
    decoded_file_cache_population_stats, initialize_decoded_file_cache_metrics,
    reset_decoded_file_cache_population_peaks, settle_scan_partitions, CancelOnDrop,
    ClippedAdmissionWave, ClippedScanLimit, DecodedFileCacheFootprint,
    DecodedFileCachePopulationStats, FileAttributionSnapshot, OrderedMergeGlobalBudget,
    OrderedResidualHint, OrderedScanLimit, OrderedScanTuning, PreferredScanOrder, QueryCancel,
    QueryExecutionId, ScanPartitionTracker, ScanSettle, ScanShard, SiglakeIcebergTableScan,
    CANONICAL_EVENT_TIME_FIELD, FILE_ATTRIBUTION_CAP, UNATTRIBUTED_QUERY_EXECUTION_ID,
};

/// Arrow field-metadata key carrying the configured text tokenizer name for a
/// queryable column. The storage/query path stamps it from the table mapping so
/// match/search UDF evaluation can share the same tokenizer registry as index
/// build and prune extraction.
pub const TEXT_TOKENIZER_FIELD_METADATA_KEY: &str = "siglake.text_tokenizer";

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{OnceLock, RwLock};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::{SQLOptions, SessionContext};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryScanTuning {
    pub reader_budget: Option<usize>,
    pub file_concurrency_limit: Option<usize>,
    pub batch_size: Option<usize>,
    pub range_coalesce_bytes: Option<u64>,
    pub range_fetch_concurrency: Option<usize>,
    pub range_adaptive_min_bytes: Option<u64>,
    pub range_adaptive_min_files: Option<usize>,
    pub adaptive_partition_min_bytes: Option<u64>,
    pub adaptive_partition_min_files: Option<usize>,
    pub file_cache_max_bytes: Option<u64>,
    pub file_cache_max_entries: Option<usize>,
    pub ordered_drain_buffer_bytes: Option<u64>,
    /// #4847 LOCAL QUALIFICATION PROTOTYPE: populate the decoded cache per ROW
    /// GROUP instead of per drained file, so a read that stops early still
    /// leaves whole units behind.
    ///
    /// Deliberately has no environment variable, CLI flag, chart value or
    /// operator field: only an in-process caller can set it, which is the
    /// measurement fixture. Shipped policy stays drained-scan-only — see
    /// `docs/DESIGN_row_group_decoded_cache_qualification.md`.
    pub file_cache_row_group_prototype: bool,
    /// #4905 LOCAL QUALIFICATION PROTOTYPE: admit a fully drained read under a
    /// converted predicate, keyed by that predicate in addition to the shipped
    /// file/projection/delete/range/direction identity.
    ///
    /// Deliberately has no environment variable, CLI flag, chart value or
    /// operator field. Planning remains `Inexact`, so DataFusion keeps the
    /// residual filter on hits and misses; only an in-process measurement can
    /// enable this. See `docs/DESIGN_predicate_keyed_decoded_cache.md`.
    pub file_cache_predicate_key_prototype: bool,
    /// #5074 LOCAL QUALIFICATION PROTOTYPE: admit population buffers against
    /// the same process-wide byte budget as completed decoded-file entries.
    ///
    /// Deliberately has no environment variable, CLI flag, chart value or
    /// operator field. The cache defaults and packaged memory limits stay
    /// unchanged while the shared bound is qualified locally.
    pub file_cache_population_bound_prototype: bool,
    /// #5786 LOCAL QUALIFICATION CONTROL: retain the pre-#5786 policy whose
    /// completed entries are byte-bounded but whose live populations are not.
    ///
    /// Deliberately has no environment variable, CLI flag, chart value or
    /// operator field. Production always accounts populations against the
    /// configured decoded-file cache budget; this switch exists only for the
    /// matched measurement in `file_cache_budget_measurement.rs`.
    pub file_cache_unbounded_population_prototype: bool,
    /// #4959 LOCAL QUALIFICATION PROTOTYPE: retain the bounded file-task
    /// identities a scan planned, offered to the decoded cache, and attempted
    /// to open as labelled DataFusion node metrics.
    ///
    /// Deliberately has no environment variable, CLI flag, chart value or
    /// operator field. The public SQL response and distributed shard header
    /// remain aggregate-only while the representation is qualified in
    /// `docs/DESIGN_row_group_decoded_cache_qualification.md`.
    pub file_attribution_prototype: bool,
}

/// Effective process-wide read-cache configuration.
///
/// Resolve this once from the query server's CLI (whose values may have come
/// from flags or environment variables), then use the same value to configure
/// the caches and subtract their reservation from the query memory pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryReadCacheConfig {
    pub object_cache_max_bytes: u64,
    pub file_cache_max_bytes: Option<u64>,
    pub file_cache_max_entries: Option<usize>,
}

impl QueryReadCacheConfig {
    pub fn reserved_bytes(self) -> u64 {
        self.object_cache_max_bytes
            .saturating_add(self.file_cache_max_bytes.unwrap_or(0))
    }
}

/// Pure resolver for the query server's read-cache CLI/environment inputs.
///
/// The object cache keeps its historical 1 GiB fallback when no cgroup limit
/// is available. The experimental decoded-file cache is enabled only when both
/// of its limits are positive.
pub fn resolve_query_read_cache_config(
    cgroup_memory_limit: Option<u64>,
    object_cache_max_bytes: Option<u64>,
    file_cache_max_bytes: Option<u64>,
    file_cache_max_entries: Option<usize>,
) -> QueryReadCacheConfig {
    const DEFAULT_OBJECT_CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

    let derived_object_cache_bytes = crate::iceberg::derive_read_cache_bytes(cgroup_memory_limit)
        .map(|(object_cache_bytes, _)| object_cache_bytes)
        .unwrap_or(DEFAULT_OBJECT_CACHE_MAX_BYTES);
    let (file_cache_max_bytes, file_cache_max_entries) = match (
        file_cache_max_bytes.filter(|value| *value > 0),
        file_cache_max_entries.filter(|value| *value > 0),
    ) {
        (Some(max_bytes), Some(max_entries)) => (Some(max_bytes), Some(max_entries)),
        _ => (None, None),
    };

    QueryReadCacheConfig {
        object_cache_max_bytes: object_cache_max_bytes.unwrap_or(derived_object_cache_bytes),
        file_cache_max_bytes,
        file_cache_max_entries,
    }
}

/// Which half of the decoded-file cache's configuration is present when the
/// other is missing — the state in which the cache is OFF although an operator
/// asked for it.
///
/// Both limits have to be positive ([`resolve_query_read_cache_config`]), and
/// the chart and the operator both render BOTH variables with an explicit `0`.
/// So the likely way to ask for the cache and not get it is to override one of
/// them (`spec.extraEnv`, a single `--set`) and leave the other at its zero,
/// which resolved silently to "disabled" and logged the same `None` an
/// unconfigured pod logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileCacheHalfConfigured {
    /// A positive byte budget with no entry limit.
    BytesOnly,
    /// A positive entry limit with no byte budget.
    EntriesOnly,
}

/// Whether the decoded-file cache's two limits disagree about being on.
///
/// Pure, and separate from the resolver, because the resolver's answer is
/// deliberately the same in both cases — off — and the caller needs to say so
/// out loud rather than infer it from a `None`.
pub fn file_cache_half_configured(
    file_cache_max_bytes: Option<u64>,
    file_cache_max_entries: Option<usize>,
) -> Option<FileCacheHalfConfigured> {
    let bytes = file_cache_max_bytes.is_some_and(|value| value > 0);
    let entries = file_cache_max_entries.is_some_and(|value| value > 0);
    match (bytes, entries) {
        (true, false) => Some(FileCacheHalfConfigured::BytesOnly),
        (false, true) => Some(FileCacheHalfConfigured::EntriesOnly),
        _ => None,
    }
}

/// Positive byte and entry limits for the decoded-file cache, derived from the
/// same container memory limit every other cache is sized from.
///
/// NOTHING APPLIES THIS BY DEFAULT. The cache stays off unless an operator sets
/// both knobs ([`resolve_query_read_cache_config`]); this is the pair to set
/// when they decide to, and the pair the startup warning names when only one
/// half arrives. Keeping the recommendation in the same arithmetic as the
/// budget is the point: an operator who picks a number by hand picks it against
/// nothing, and these bytes come out of the query memory pool.
///
/// Bytes: `derive_read_cache_bytes`' second value (an eighth of the limit, 64
/// MiB floor, 8 GiB cap), which has always been documented as this cache's
/// sizing recommendation and until now had no entry-limit twin.
///
/// Entries: one per MiB of budget, clamped to at least
/// [`MAX_FILE_CACHE_ENTRY_FRACTION`] (below that the byte budget could never be
/// filled, since one entry may not exceed a quarter of it). An entry is one
/// data file's decoded batches under one projection, measured at 5.5 MiB for a
/// 65,536-row file of ~60-byte events (#3053), so at one per MiB the entry
/// bound cannot bite before the byte bound for any file of ordinary size — it
/// is there to stop the map growing without limit over the small files a
/// sub-second WAL drain produces, and the BYTES are the bound that means
/// anything to the pool.
pub fn derive_file_cache_limits(memory_limit_bytes: Option<u64>) -> Option<(u64, usize)> {
    let (_, file_cache_bytes) = crate::iceberg::derive_read_cache_bytes(memory_limit_bytes)?;
    let entries = (file_cache_bytes / (1024 * 1024)).max(MAX_FILE_CACHE_ENTRY_FRACTION) as usize;
    Some((file_cache_bytes, entries))
}

/// An entry larger than a quarter of the byte budget is never cached: one
/// oversized file would evict everything useful and then sit alone
/// (`QueryFileBatchCache::insert`).
pub const MAX_FILE_CACHE_ENTRY_FRACTION: u64 = query_provider::MAX_FILE_CACHE_ENTRY_FRACTION;

/// The smallest byte budget that can hold ONE entry of `decoded_bytes` at all.
///
/// The quarter rule is what makes a file cache budget a statement about file
/// SIZE and not just about total memory: a budget below this never holds a file
/// of that size, charges `outcome="skip_oversized"` on every population of one,
/// and still subtracts its bytes from the query memory pool. A compacted file is
/// the size that matters — `cold_target_file_bytes` (256 MiB) times the scan's
/// decompression estimate (5) is about 1.25 GiB decoded, so holding one takes a
/// 5 GiB file cache, which [`derive_file_cache_limits`] reaches at a 40 GiB
/// container. See `docs/DESIGN_source_file_cache_qualification.md`.
pub fn min_file_cache_bytes_for_entry(decoded_bytes: u64) -> u64 {
    decoded_bytes.saturating_mul(MAX_FILE_CACHE_ENTRY_FRACTION)
}

/// [`resolve_query_read_cache_config`] for raw environment values.
///
/// Kept pure so environment parsing is covered without mutating the process
/// environment in parallel tests.
pub fn query_read_cache_config_from(
    cgroup_memory_limit: Option<u64>,
    object_cache_max_bytes: Option<&str>,
    file_cache_max_bytes: Option<&str>,
    file_cache_max_entries: Option<&str>,
) -> QueryReadCacheConfig {
    resolve_query_read_cache_config(
        cgroup_memory_limit,
        object_cache_max_bytes.and_then(|value| value.parse::<u64>().ok()),
        file_cache_max_bytes.and_then(|value| value.parse::<u64>().ok()),
        file_cache_max_entries.and_then(|value| value.parse::<usize>().ok()),
    )
}

/// Effective process-wide budgets for the two text-index caches, which live in
/// the vendored Iceberg fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextIndexCacheConfig {
    pub parsed_index_max_bytes: u64,
    pub puffin_blob_max_bytes: u64,
}

impl TextIndexCacheConfig {
    pub fn reserved_bytes(self) -> u64 {
        self.parsed_index_max_bytes
            .saturating_add(self.puffin_blob_max_bytes)
    }
}

/// Pure resolver for the text-index cache budgets.
///
/// The fork cannot derive these itself: the cgroup read lives in this crate,
/// which depends on the fork. So they are resolved here — from the pod's memory
/// limit, or from an explicit override — and pushed in, exactly as the
/// byte-range object cache's budget is.
///
/// `Some(0)` for either knob keeps its documented meaning: the parsed cache off
/// (every text query deserializes again), or the serialized copy dropped.
/// Without a limit to read, today's constants stand.
pub fn resolve_text_index_cache_config(
    memory_limit_bytes: Option<u64>,
    parsed_index_max_bytes: Option<u64>,
    puffin_blob_max_bytes: Option<u64>,
) -> TextIndexCacheConfig {
    const DEFAULT_PARSED_INDEX_MAX_BYTES: u64 = 1024 * 1024 * 1024;
    const DEFAULT_PUFFIN_BLOB_MAX_BYTES: u64 = 256 * 1024 * 1024;

    let (derived_parsed, derived_blob) =
        crate::iceberg::derive_text_index_cache_bytes(memory_limit_bytes).unwrap_or((
            DEFAULT_PARSED_INDEX_MAX_BYTES,
            DEFAULT_PUFFIN_BLOB_MAX_BYTES,
        ));
    TextIndexCacheConfig {
        parsed_index_max_bytes: parsed_index_max_bytes.unwrap_or(derived_parsed),
        puffin_blob_max_bytes: puffin_blob_max_bytes.unwrap_or(derived_blob),
    }
}

/// [`resolve_text_index_cache_config`] for raw environment values, so the
/// parsing is covered without mutating the process environment.
pub fn text_index_cache_config_from(
    memory_limit_bytes: Option<u64>,
    parsed_index_max_bytes: Option<&str>,
    puffin_blob_max_bytes: Option<&str>,
) -> TextIndexCacheConfig {
    resolve_text_index_cache_config(
        memory_limit_bytes,
        parsed_index_max_bytes.and_then(|value| value.trim().parse::<u64>().ok()),
        puffin_blob_max_bytes.and_then(|value| value.trim().parse::<u64>().ok()),
    )
}

/// What a process does with the warehouse, for cache sizing.
///
/// The query server is not in here: it resolves its own budgets from its flags
/// ([`resolve_query_read_cache_config`], [`resolve_text_index_cache_config`]).
/// These are the roles of the `siglake` binary, which is every OTHER process
/// that opens a warehouse — the compactor pod, the ingest server, the sweeps
/// and rebuilds, and the two subcommands that run SQL in process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarehouseRole {
    /// Runs DataFusion SQL over the Iceberg tables in this process
    /// (`sql-direct`, `iceberg-demo`, `subscribe`), so a `LIKE` or an FTS
    /// predicate can reach the scan's index-pruning path and fill the
    /// text-index caches.
    InProcessQuery,
    /// Drains, compacts, sweeps, rebuilds. Never plans a text predicate.
    Maintenance,
}

/// Both cache configurations one role resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleCacheConfig {
    pub read: QueryReadCacheConfig,
    pub text_index: TextIndexCacheConfig,
}

/// Pure resolver for the `siglake` binary's cache budgets, by role.
///
/// WHY A ROLE AND NOT THE QUERY SERVER'S ANSWER. #4056 sized both text-index
/// caches from the pod's memory limit at the query server's startup; every
/// other process kept the fork's env-or-constant fallback, a flat 1 GiB of
/// parsed indexes plus 256 MiB of blobs whatever its limit — 1.25 GiB of
/// ceilings inside the 1Gi the chart packages for the compactor.
///
/// A maintenance process takes an explicit ZERO rather than a derivation,
/// because nothing it does can fill either cache. The only sites that insert
/// are the fork reader's index-pruning path (`inverted_index_row_selection`),
/// reached only from a plan carrying a `RawPruneSpec` — a text predicate
/// through the Iceberg table provider. Maintenance plans none: a Tier-2
/// aggregate rebuild counts from manifest stats, Parquet footers and raw pages,
/// and a delete task evaluates its predicate over a `MemTable` of the candidate
/// file's decoded rows. So the flat pair bounded caches that never take an
/// entry, and the zero costs a maintenance process nothing. (It is a budget,
/// not a refusal: were a maintenance read to reach that path after all, a zero
/// parsed budget means it re-parses per file, which is the documented
/// `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` behaviour.)
///
/// The READ caches are the same answer for both roles, and deliberately not the
/// query server's: the byte-range object cache stays the opt-in it has always
/// been (`SIGLAKE_OBJECT_CACHE_BYTES`, `0`/unset disabling it — exactly what the
/// fork holds when nothing configures it), and the decoded-file cache is off
/// because only the query server applies its limits
/// ([`configure_query_scan_tuning`]). Resolving them here is about honesty, not
/// enablement: a process that configures nothing leaves
/// [`reserved_cache_bytes_in_force`] to fall back on the query server's
/// resolver, which answers with a DERIVED object cache — a quarter of the pod —
/// for a process whose object cache is empty by construction.
///
/// Every override survives in both roles: an operator with
/// `SIGLAKE_OBJECT_CACHE_BYTES` or either text-index bound set fleet-wide keeps
/// what they set.
pub fn resolve_role_cache_config(
    role: WarehouseRole,
    memory_limit_bytes: Option<u64>,
    object_cache_max_bytes: Option<u64>,
    parsed_index_max_bytes: Option<u64>,
    puffin_blob_max_bytes: Option<u64>,
) -> RoleCacheConfig {
    let text_index = match role {
        WarehouseRole::InProcessQuery => resolve_text_index_cache_config(
            memory_limit_bytes,
            parsed_index_max_bytes,
            puffin_blob_max_bytes,
        ),
        WarehouseRole::Maintenance => TextIndexCacheConfig {
            parsed_index_max_bytes: parsed_index_max_bytes.unwrap_or(0),
            puffin_blob_max_bytes: puffin_blob_max_bytes.unwrap_or(0),
        },
    };
    RoleCacheConfig {
        read: QueryReadCacheConfig {
            object_cache_max_bytes: object_cache_max_bytes.unwrap_or(0),
            file_cache_max_bytes: None,
            file_cache_max_entries: None,
        },
        text_index,
    }
}

/// [`resolve_role_cache_config`] for raw environment values, so the parsing is
/// covered without mutating the process environment.
///
/// `SIGLAKE_OBJECT_CACHE_BYTES` is parsed the way the fork parses it — no trim,
/// unparseable means off — so recording the budget cannot disagree with the
/// cache that would have read the variable itself.
pub fn role_cache_config_from(
    role: WarehouseRole,
    memory_limit_bytes: Option<u64>,
    object_cache_max_bytes: Option<&str>,
    parsed_index_max_bytes: Option<&str>,
    puffin_blob_max_bytes: Option<&str>,
) -> RoleCacheConfig {
    resolve_role_cache_config(
        role,
        memory_limit_bytes,
        object_cache_max_bytes.and_then(|value| value.parse::<u64>().ok()),
        parsed_index_max_bytes.and_then(|value| value.trim().parse::<u64>().ok()),
        puffin_blob_max_bytes.and_then(|value| value.trim().parse::<u64>().ok()),
    )
}

/// Apply the resolved text-index cache budgets to the fork's caches. Must run
/// before the shared query memory pool is built, which subtracts what they
/// hold.
pub fn configure_text_index_caches(config: TextIndexCacheConfig) {
    // `::iceberg` = the external crate (this crate also has a local `iceberg`
    // module).
    ::iceberg::arrow::set_text_index_cache_max_bytes(
        config.parsed_index_max_bytes,
        config.puffin_blob_max_bytes,
    );
}

/// The text-index cache budgets IN FORCE, read back from the caches themselves
/// rather than re-derived here.
///
/// Asking the fork is the point: it applies the entry bound's zero-disable and
/// the environment fallback a process that never configured them still uses, so
/// the pool subtracts the bytes those caches will really hold. The same
/// question asked twice is how `siglake_query_memory_reserved_elsewhere_bytes`
/// once published 13.56 GiB against a pool that had subtracted 26.
fn text_index_cache_bytes_in_force() -> u64 {
    let (parsed, blob) = ::iceberg::arrow::text_index_cache_max_bytes_in_force();
    parsed.saturating_add(blob)
}

fn query_read_cache_config_cell() -> &'static OnceLock<QueryReadCacheConfig> {
    static CELL: OnceLock<QueryReadCacheConfig> = OnceLock::new();
    &CELL
}

/// Record and apply the query server's resolved process-wide read-cache
/// configuration. This must run before the shared query memory pool is built.
pub fn configure_query_read_caches(config: QueryReadCacheConfig) {
    query_read_cache_config_cell()
        .set(config)
        .expect("query read caches configured more than once");
    configure_object_cache_max_bytes(config.object_cache_max_bytes);
}

fn query_scan_tuning_cell() -> &'static RwLock<QueryScanTuning> {
    static CELL: OnceLock<RwLock<QueryScanTuning>> = OnceLock::new();
    CELL.get_or_init(|| RwLock::new(QueryScanTuning::default()))
}

pub fn configure_query_scan_tuning(tuning: QueryScanTuning) {
    *query_scan_tuning_cell()
        .write()
        .expect("query scan tuning lock poisoned") = tuning;
}

/// Set the byte-range object cache budget (bytes; 0 disables) — the cold-S3 hot
/// cache in the vendored FileIO layer that caches Parquet footers, column chunks,
/// and index sidecars so repeat/warm reads (including the FTS pruning path) don't
/// re-hit object storage. Process-global.
pub fn configure_object_cache_max_bytes(max_bytes: u64) {
    // `::iceberg` = the external crate (this crate also has a local `iceberg` module).
    ::iceberg::io::set_object_cache_max_bytes(max_bytes);
}

pub(crate) fn current_query_scan_tuning() -> QueryScanTuning {
    *query_scan_tuning_cell()
        .read()
        .expect("query scan tuning lock poisoned")
}

/// Build a `LocalFileSystem` object store rooted at `root`.
/// Creates `root` if it does not exist.
pub fn local_store(root: &Path) -> Result<Arc<dyn ObjectStore>> {
    std::fs::create_dir_all(root).with_context(|| format!("creating {}", root.display()))?;
    let store = LocalFileSystem::new_with_prefix(root)?;
    Ok(Arc::new(store))
}

/// Build a [`SessionContext`] configured for siglake' time-partitioned Parquet
/// layout: `events/YYYY/MM/DD/HH/*.parquet`.
///
/// Specifically, this disables `listing_table_ignore_subdirectory` so that
/// `ListingTable` recurses into nested partition directories. Without this,
/// DataFusion's default behavior is to read only files in the *immediate*
/// directory passed to it.
pub fn session_context() -> SessionContext {
    session_context_with_target_partitions(None)
}

pub fn session_context_with_target_partitions(target_partitions: Option<usize>) -> SessionContext {
    session_context_with_order(target_partitions, None, None)
}

/// As [`session_context_with_target_partitions`], but also injects a
/// distributed-query [`ScanShard`] (when `Some`) as a `SessionConfig`
/// extension. The Iceberg table provider reads it and scans only that shard's
/// files — the per-request hook the coordinator uses to fan a query out across
/// worker pods. See `docs/DESIGN_distributed_query.md`.
pub fn session_context_with(
    target_partitions: Option<usize>,
    shard: Option<ScanShard>,
) -> SessionContext {
    session_context_with_order(target_partitions, shard, None)
}

/// As [`session_context_with`], but also injects a preferred scan direction for
/// `ORDER BY timestamp` queries when the SQL layer proved one.
/// The siglake base [`SessionConfig`]: the process-wide invariants every query
/// shares. Per-query knobs (target partitions, shard / scan-order extensions)
/// are layered on top of a clone of this. Kept as one builder so the template
/// state and any non-template fallback agree exactly.
fn base_session_config() -> SessionConfig {
    SessionConfig::new()
        .set_bool(
            "datafusion.execution.listing_table_ignore_subdirectory",
            false,
        )
        // WS-3: when a scan advertises `timestamp ASC` output ordering, make
        // the optimizer keep it through repartitions (order-preserving
        // round-robin) instead of discarding it and re-sorting — without this,
        // any residual FilterExec's RepartitionExec destroys the ordering and
        // a windowed `ORDER BY timestamp LIMIT n` can't early-stop (observed
        // in AWS smoke round 73). No effect on plans with no useful ordering.
        .set_bool("datafusion.optimizer.prefer_existing_sort", true)
}

/// Phase 5 — a process-wide template [`SessionState`], built ONCE. Building a
/// `SessionContext` from scratch re-runs DataFusion's `with_default_features()`
/// (registering every built-in scalar/aggregate function, analyzer rule, and
/// optimizer rule) plus a fresh `RuntimeEnv` — measured at ~235µs/query, ~70% of
/// the per-`/api/v1/sql` overhead floor (`bench_per_query_overhead_floor`).
///
/// That setup is identical for every query, so we build it once and CLONE it per
/// query (~12µs — a ~18× cut). Cloning a `SessionState` is cheap (Arc bumps);
/// the catch is that the clone SHARES the catalog `Arc`, so without care a table
/// registered by one query would leak into a concurrent sibling. [`base_context`]
/// closes that hole by giving every per-query context a FRESH catalog list
/// (`register_catalog_list`), restoring full isolation (regression-tested in
/// `session_context_template_isolation`). The shared `RuntimeEnv` (memory pool,
/// object-store registry) is safe to share across contexts by design.
/// Fraction of the container's memory the query engine may use for operator
/// working set (`SIGLAKE_QUERY_MEMORY_FRACTION`, default 0.5).
///
/// Half, not more: the remainder has to cover the scan's decode buffers, the
/// object/file caches, the WAL buffer union and the allocator's own slack, none
/// of which the pool accounts for.
fn query_memory_fraction() -> f64 {
    std::env::var("SIGLAKE_QUERY_MEMORY_FRACTION")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|f| *f > 0.0 && *f <= 1.0)
        .unwrap_or(0.5)
}

/// Directory DataFusion uses for query spill files
/// (`SIGLAKE_QUERY_SPILL_DIR`). Empty values leave DataFusion's OS temporary
/// directory behaviour unchanged.
///
/// Pure so tests do not mutate the process environment.
pub fn query_spill_dir_from(configured: Option<&str>) -> Option<PathBuf> {
    configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn query_spill_dir() -> Option<PathBuf> {
    query_spill_dir_from(std::env::var("SIGLAKE_QUERY_SPILL_DIR").ok().as_deref())
}

/// Maximum bytes DataFusion may hold in query spill files
/// (`SIGLAKE_QUERY_SPILL_MAX_BYTES`). Missing, zero, and invalid values retain
/// DataFusion's current default cap.
///
/// Pure so tests do not mutate the process environment.
pub fn query_spill_max_bytes_from(configured: Option<&str>) -> Option<u64> {
    configured
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|bytes| *bytes > 0)
}

fn query_spill_max_bytes() -> Option<u64> {
    query_spill_max_bytes_from(
        std::env::var("SIGLAKE_QUERY_SPILL_MAX_BYTES")
            .ok()
            .as_deref(),
    )
}

fn with_query_spill_config(
    mut runtime: datafusion::execution::runtime_env::RuntimeEnvBuilder,
    directory: Option<PathBuf>,
    max_bytes: Option<u64>,
) -> datafusion::execution::runtime_env::RuntimeEnvBuilder {
    if let Some(directory) = directory {
        runtime = runtime.with_temp_file_path(directory);
    }
    if let Some(max_bytes) = max_bytes {
        runtime = runtime.with_max_temp_directory_size(max_bytes);
    }
    runtime
}

/// Total physical RAM from `/proc/meminfo`, for sizing when there is no cgroup
/// limit. `None` off Linux or if the file cannot be parsed.
pub fn system_memory_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Bytes the query engine's shared memory pool is allowed to hand out.
/// `None` (unbounded) only when the container limit cannot be read AND no
/// explicit override is set.
///
/// Pure, so the sizing is testable without a cgroup.
pub fn query_pool_bytes_from(
    limit: Option<u64>,
    fraction: f64,
    explicit: Option<u64>,
) -> Option<u64> {
    query_pool_bytes_full(limit, fraction, explicit, 0)
}

/// As [`query_pool_bytes_from`], minus memory this process is already committed
/// to holding elsewhere.
///
/// The caches are configured in BYTES while the pool was sized against the raw
/// machine limit, so they were double-counted: a 61 GB node with a 16 GiB object
/// cache and an 8 GiB file cache got a 30.9 GiB pool, committing 55 of 61 GB
/// before a single decode buffer. Over-commitment is how this whole failure mode
/// began, so the pool takes its fraction of what is actually LEFT.
pub fn query_pool_bytes_full(
    limit: Option<u64>,
    fraction: f64,
    explicit: Option<u64>,
    reserved_elsewhere: u64,
) -> Option<u64> {
    if let Some(bytes) = explicit {
        // 0 is the documented escape hatch back to an unbounded pool.
        return (bytes > 0).then_some(bytes);
    }
    let limit_total = limit?;
    // Never let cache configuration squeeze the engine to nothing. A bench peer
    // was found running 24 GiB of caches on a 30.8 GiB node -- 78% of RAM, from
    // absolute constants that ignore node size -- which left the pool at 3.4 GiB
    // and made every large query spill. Over-commitment causes OOM, but a pool
    // too small to work is its own outage, so the floor is a SHARE of the whole
    // limit rather than of the remainder.
    let after_caches = limit_total.saturating_sub(reserved_elsewhere);
    let floor_share = (limit_total as f64 * 0.25) as u64;
    if after_caches < floor_share {
        tracing::warn!(
            configured_caches = reserved_elsewhere,
            memory_limit = limit_total,
            "caches are configured to hold most of this machine; the query memory \
             pool is being floored, and cache sizes should be reduced"
        );
    }
    let limit = after_caches.max(floor_share);
    // A floor: a pool so small that ordinary queries cannot run would trade
    // OOM-kills for universal ResourcesExhausted, which is not an improvement.
    Some(((limit as f64 * fraction) as u64).max(256 * 1024 * 1024))
}

/// The limit to size against: the container's if there is one, otherwise the
/// machine's physical RAM.
///
/// The cgroup-only version of this left the pool UNBOUNDED on plain `docker run`
/// without `--memory`, on bare metal, and under systemd -- which is exactly the
/// configuration that OOM-killed hosts, so "no limit readable" is the last state
/// that should inherit no limit. Found on a live bench fleet whose containers
/// have no memory limit: the fix silently did nothing there.
pub fn memory_limit_for_sizing() -> Option<u64> {
    crate::iceberg::cgroup_memory_limit_bytes().or_else(system_memory_bytes)
}

/// What this process has ACTUALLY reserved outside the query pool, as
/// `(read_caches, metadata_caches, text_index_caches)` in bytes.
///
/// The query server records its resolved cache configuration before opening the
/// warehouse, so CLI flags and environment values take the same path. Other
/// binaries fall back to resolving the cache environment here.
///
/// They used to answer it separately, and disagreed the moment anyone set the
/// env vars — which is precisely when someone is debugging memory. Measured on
/// the 2026-08-29 bench fleet: the pool correctly subtracted 26 GiB (16 GiB
/// object + 8 GiB file cache from the env, + 2 GiB metadata), while
/// `siglake_query_memory_reserved_elsewhere_bytes` published the DERIVED 13.56
/// GiB. Reading the gauges, a 32 GiB node with 29.9 GiB committed looked like it
/// had 14 GiB spare. Same shape as the defect the pool fix itself corrected —
/// there the env vars were read where derived ones were in force; here the
/// mirror image — so the two sizings are now one function, not two copies.
///
/// The text-index caches (the fork's parsed inverted indexes and Puffin blobs)
/// are the third element for the same reason: they were outside this answer
/// entirely, and at the packaged 4Gi pod their old flat defaults were the whole
/// of the headroom the budget leaves for the process.
pub fn reserved_cache_bytes_in_force() -> (u64, u64, u64) {
    let config = query_read_cache_config_cell()
        .get()
        .copied()
        .unwrap_or_else(|| {
            let object_cache = std::env::var("SIGLAKE_OBJECT_CACHE_BYTES").ok();
            let file_cache_bytes = std::env::var("SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_BYTES").ok();
            let file_cache_entries =
                std::env::var("SIGLAKE_QUERY_SCAN_FILE_CACHE_MAX_ENTRIES").ok();
            query_read_cache_config_from(
                crate::iceberg::cgroup_memory_limit_bytes(),
                object_cache.as_deref(),
                file_cache_bytes.as_deref(),
                file_cache_entries.as_deref(),
            )
        });
    let metadata_caches = crate::iceberg::metadata_cache_budget_bytes() as u64;
    (
        config.reserved_bytes(),
        metadata_caches,
        text_index_cache_bytes_in_force(),
    )
}

/// The whole memory budget a pod of `limit` bytes would derive.
///
/// Exists so the budget can be CHECKED at pod sizes other than this one, from
/// the derivations alone — no cgroup, no environment. Every over-commitment in
/// this system's history was arithmetic nobody could evaluate without
/// deploying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    pub pool: u64,
    pub read_caches: u64,
    pub metadata_caches: u64,
    pub text_index_caches: u64,
}

impl MemoryBudget {
    /// Everything this pod has promised away.
    pub fn committed(self) -> u64 {
        self.pool
            .saturating_add(self.read_caches)
            .saturating_add(self.metadata_caches)
            .saturating_add(self.text_index_caches)
    }

    /// What is left for the process itself, in-flight Arrow batches, decode
    /// buffers and allocator slack.
    pub fn headroom(self, limit: u64) -> u64 {
        limit.saturating_sub(self.committed())
    }
}

pub fn memory_budget_for(limit: Option<u64>, fraction: f64) -> MemoryBudget {
    memory_budget_with_file_cache(limit, fraction, None)
}

/// [`memory_budget_for`] with the decoded-file cache turned ON at
/// `file_cache_max_bytes`.
///
/// The default budget cannot express this: the file cache is off, so
/// [`resolve_query_read_cache_config`] contributes nothing for it and the
/// arithmetic an operator would check before enabling it did not exist. Its
/// bytes come out of the same remainder the pool takes its fraction of — the
/// chart's `values.yaml` says so in prose, and this is that sentence as a
/// function.
pub fn memory_budget_with_file_cache(
    limit: Option<u64>,
    fraction: f64,
    file_cache_max_bytes: Option<u64>,
) -> MemoryBudget {
    // The entry limit never moves the budget; only bytes are reserved. Pass the
    // one the recommendation would use so the config resolves as enabled.
    let file_cache_max_entries = file_cache_max_bytes
        .filter(|bytes| *bytes > 0)
        .map(|bytes| (bytes / (1024 * 1024)).max(MAX_FILE_CACHE_ENTRY_FRACTION) as usize);
    let read_caches =
        resolve_query_read_cache_config(limit, None, file_cache_max_bytes, file_cache_max_entries)
            .reserved_bytes();
    let metadata = crate::iceberg::derive_metadata_cache_bytes(limit) as u64;
    let text_index_caches = resolve_text_index_cache_config(limit, None, None).reserved_bytes();
    let pool = query_pool_bytes_full(
        limit,
        fraction,
        None,
        read_caches + metadata + text_index_caches,
    )
    .unwrap_or(0);
    MemoryBudget {
        pool,
        read_caches,
        metadata_caches: metadata,
        text_index_caches,
    }
}

/// Whether the query pool is wrapped in DataFusion's `TrackConsumersPool`
/// (`SIGLAKE_QUERY_MEMORY_POOL_TRACK_CONSUMERS`; default on, `0`/`off`/`false`/
/// `no` disable it).
///
/// WHY. On 2026-09-01 `siglake_query_memory_pool_reserved_bytes` read 3.66 GiB
/// on an idle pod where the established healthy value is 0. A `FairSpillPool`
/// holds two counters and nothing else, so a scrape could say THAT bytes were
/// held but never by WHOM — and there are 28 reservation sites in the scan
/// alone before DataFusion's own operators. The tracking wrapper keeps one
/// entry per live `MemoryConsumer`, so a residual reservation names itself.
///
/// The cost is a second mutex acquisition on every grow/shrink, next to the one
/// `FairSpillPool` already takes. DataFusion's own `with_memory_limit()` wraps
/// by default for the same reason; the knob exists so a deployment can measure
/// the difference rather than take it on trust.
pub fn query_pool_tracks_consumers_from(configured: Option<&str>) -> bool {
    !matches!(
        configured.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("0" | "off" | "false" | "no")
    )
}

fn query_pool_tracks_consumers() -> bool {
    query_pool_tracks_consumers_from(
        std::env::var("SIGLAKE_QUERY_MEMORY_POOL_TRACK_CONSUMERS")
            .ok()
            .as_deref(),
    )
}

/// The process-wide query memory pool, held by its CONCRETE type.
///
/// `RuntimeEnv` only ever hands back an `Arc<dyn MemoryPool>`, and the trait
/// has no way to enumerate consumers. Keeping the typed handle here is what
/// lets [`query_memory_pool_top_consumers`] reach `report_top`.
enum QueryMemoryPool {
    Tracked(
        Arc<
            datafusion::execution::memory_pool::TrackConsumersPool<
                datafusion::execution::memory_pool::FairSpillPool,
            >,
        >,
    ),
    Plain(Arc<datafusion::execution::memory_pool::FairSpillPool>),
    Unbounded,
}

impl QueryMemoryPool {
    fn as_dyn(&self) -> Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>> {
        match self {
            Self::Tracked(pool) => Some(pool.clone()),
            Self::Plain(pool) => Some(pool.clone()),
            Self::Unbounded => None,
        }
    }
}

impl QueryMemoryPool {
    /// The bound in force, or `None` for an unbounded pool.
    fn limit_bytes(&self) -> Option<u64> {
        use datafusion::execution::memory_pool::MemoryLimit;
        match self.as_dyn()?.memory_limit() {
            MemoryLimit::Finite(bytes) => Some(bytes as u64),
            _ => None,
        }
    }
}

fn query_memory_pool_cell() -> &'static OnceLock<QueryMemoryPool> {
    static POOL: OnceLock<QueryMemoryPool> = OnceLock::new();
    &POOL
}

fn query_memory_pool() -> &'static QueryMemoryPool {
    query_memory_pool_cell().get_or_init(|| {
        build_query_memory_pool(
            std::env::var("SIGLAKE_QUERY_MEMORY_POOL_BYTES")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
        )
    })
}

/// Bound the process-wide query memory pool to `bytes` BEFORE anything has used
/// it, instead of through `SIGLAKE_QUERY_MEMORY_POOL_BYTES`. Returns whether the
/// pool now carries exactly that bound: `false` means it had already been built
/// (from the environment, or an earlier call with a different size) and the
/// existing pool was kept, since a `OnceLock` cannot be rebuilt and every
/// session template already shares it.
///
/// `0` means unbounded, as it does for the env var. This is how a test binary
/// exercises the pool WIRING (that sessions and task contexts share one bound)
/// at a known size without `set_var`, which its own parallel tests would race.
/// Production processes size from the environment and never call this.
pub fn preset_query_memory_pool_bytes(bytes: u64) -> bool {
    let pool = query_memory_pool_cell().get_or_init(|| build_query_memory_pool(Some(bytes)));
    pool.limit_bytes() == (bytes > 0).then_some(bytes)
}

/// Build the process-wide pool. `explicit` is the operator's override
/// (`SIGLAKE_QUERY_MEMORY_POOL_BYTES`, or a [`preset_query_memory_pool_bytes`]
/// call); `None` sizes from the memory limit and the caches in force.
fn build_query_memory_pool(explicit: Option<u64>) -> QueryMemoryPool {
    use datafusion::execution::memory_pool::{FairSpillPool, TrackConsumersPool};
    // BOUND THE QUERY ENGINE'S MEMORY.
    //
    // This was `RuntimeEnv::default()`, which uses an UNBOUNDED pool: sorts,
    // hash aggregates and joins had no ceiling and never spilled, so they
    // allocated until the kernel intervened. Measured on 2026-08-19 at 1TB:
    // three OOM kills at ~31.6 GB anon-rss on a 32 GB node, from 8 concurrent
    // `ORDER BY raw LIMIT 100`. Whole hosts became unreachable.
    //
    // siglake's own AdmissionController did not help and was never going to:
    // it prices estimated SCAN BYTES before a query runs, which is a
    // different quantity from what an operator actually holds while running.
    //
    // The pool is deliberately PROCESS-WIDE (this is a OnceLock and every
    // per-query context clones a session state sharing this Arc). A
    // per-query limit bounds one query and still lets N of them kill the
    // box; the thing that needed bounding is the total.
    //
    // FairSpill rather than Greedy so one large sort cannot starve every
    // other query -- and, more importantly, spillable operators SPILL to disk
    // at the limit instead of failing, so the common case degrades in speed
    // rather than in availability.
    let pool_bytes = {
        // Caches this process has already been told to hold. Read here
        // rather than plumbed: they are process-global settings and the pool
        // is a process-global object.
        //
        // BOTH sizings, because the env vars are the exception. When unset --
        // the normal Kubernetes path -- the caches are DERIVED from this same
        // cgroup limit at 25% + 12.5%. An env-only version subtracted nothing
        // there and handed the pool 50% on top of a derived 37.5%: on the
        // chart's default 4Gi query pod that is 3.50 GiB committed of 4.00,
        // before a single decode buffer. Subtracting whichever sizing is
        // actually in force brings it to 2.75 GiB.
        let (read_caches, metadata_caches, text_index_caches) = reserved_cache_bytes_in_force();
        // AND the metadata caches — footers, snapshot aggregates, windowed
        // results, file lists, scan costs. The pool subtracted two of at
        // least eight things this process holds, so the number it published
        // as its budget was never the process's budget: on the packaged 4Gi
        // pod it claimed 1.25 GiB while another 512 MiB sat in caches it had
        // never heard of.
        //
        // This is only correct BECAUSE those caches now have byte budgets
        // (step 2). Subtracting an entry-capped cache would mean subtracting
        // a number that does not bound anything.
        //
        // AND the two text-index caches in the fork, which were the last read
        // caches outside this sum. Their flat defaults (1 GiB parsed + 256 MiB
        // of blobs) were the whole of the 1.25 GiB the packaged 4Gi pod leaves
        // after the pool and the caches above it, so a pod with a large text
        // working set spent its process headroom twice.
        let reserved = read_caches + metadata_caches + text_index_caches;
        // NOT gauged here: this runs inside a `OnceLock` that initialises
        // before the metrics recorder exists, so the writes went nowhere.
        // `sample_query_memory_pool_gauges` publishes them on a timer.
        query_pool_bytes_full(
            memory_limit_for_sizing(),
            query_memory_fraction(),
            explicit,
            reserved,
        )
    };
    match pool_bytes {
        Some(bytes) => {
            let tracked = query_pool_tracks_consumers();
            tracing::info!(
                pool_bytes = bytes,
                track_consumers = tracked,
                "query memory pool bounded (spills at the limit)"
            );
            metrics::gauge!("siglake_query_memory_pool_bytes").set(bytes as f64);
            let inner = FairSpillPool::new(bytes as usize);
            if tracked {
                // `top` only sizes the consumer list in ResourcesExhausted
                // errors; DataFusion's own default is five.
                let top = std::num::NonZeroUsize::new(5).expect("5 is non-zero");
                QueryMemoryPool::Tracked(Arc::new(TrackConsumersPool::new(inner, top)))
            } else {
                QueryMemoryPool::Plain(Arc::new(inner))
            }
        }
        None => {
            // Nothing to size against and no override: keep the previous
            // behaviour rather than guess a limit, but say so loudly --
            // this is the configuration that OOM-killed hosts.
            tracing::warn!(
                "query memory pool is UNBOUNDED: neither a cgroup limit nor /proc/meminfo \
                     was readable, and SIGLAKE_QUERY_MEMORY_POOL_BYTES is unset. A large sort \
                     or aggregate can exhaust the machine."
            );
            metrics::gauge!("siglake_query_memory_pool_bytes").set(0.0);
            QueryMemoryPool::Unbounded
        }
    }
}

fn session_state_template() -> &'static datafusion::execution::session_state::SessionState {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::session_state::SessionStateBuilder;
    static TEMPLATE: OnceLock<datafusion::execution::session_state::SessionState> = OnceLock::new();
    TEMPLATE.get_or_init(|| {
        // The pool itself, with its sizing and the reasons for it, lives in
        // `query_memory_pool`; this only installs it.
        let spill_directory = query_spill_dir();
        let spill_max_bytes = query_spill_max_bytes();
        if spill_directory.is_some() || spill_max_bytes.is_some() {
            tracing::info!(
                ?spill_directory,
                ?spill_max_bytes,
                "query spill target configured (unset fields use DataFusion defaults)"
            );
        }
        let mut runtime =
            with_query_spill_config(RuntimeEnvBuilder::new(), spill_directory, spill_max_bytes);
        if let Some(pool) = query_memory_pool().as_dyn() {
            runtime = runtime.with_memory_pool(pool);
        }
        SessionStateBuilder::new()
            .with_config(base_session_config())
            .with_runtime_env(
                runtime
                    .build_arc()
                    .expect("query runtime environment must be constructible"),
            )
            .with_default_features()
            .build()
    })
}

/// The process-wide query `RuntimeEnv`, carrying the bounded memory pool.
///
/// Execution takes its memory pool from the `TaskContext`, NOT from the
/// `SessionState` the plan was built with. Three call sites built
/// `TaskContext::default()`, which mints a fresh `RuntimeEnv` with an UNBOUNDED
/// pool -- so the bound installed on the session was never consulted by the
/// operators that allocate. Measured: a 16.5 GiB pool and a process that still
/// reached 31.6 GB and was OOM-killed.
///
/// Anything executing a plan outside a `SessionContext` must build its
/// `TaskContext` with this.
pub fn shared_query_runtime_env() -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
    session_state_template().runtime_env().clone()
}

/// Current occupancy of the process-wide query memory pool, as
/// `(reserved_bytes, limit_bytes)`. `None` when the pool is unbounded.
///
/// NOTHING REPORTED THIS. The pool's LIMIT has been on a gauge since it was
/// bounded on 2026-08-20, but not a byte of its use — so a pool at 99% and a
/// pool at 1% were indistinguishable from a scrape, a slow reservation leak was
/// invisible until queries began failing, and the "is it the pool?" question on
/// the open release blocker could not be answered without attaching a debugger.
///
/// It is also the prerequisite for bounding anything else: every cache budget
/// below is a number someone has to be able to check after it ships.
pub fn query_memory_pool_usage() -> Option<(u64, u64)> {
    let pool = &session_state_template().runtime_env().memory_pool;
    let limit = pool.memory_limit();
    match limit {
        datafusion::execution::memory_pool::MemoryLimit::Finite(bytes) => {
            Some((pool.reserved() as u64, bytes as u64))
        }
        _ => None,
    }
}

/// The consumers currently registered with the query pool, largest reservation
/// first, in DataFusion's own report format (`name#id(can spill: b) consumed X,
/// peak Y`). `None` when consumer tracking is off or the pool is unbounded.
///
/// This is the answer to "WHO holds the bytes" that `reserved_bytes` cannot
/// give. A consumer listed here with `consumed 0.0 B` is still informative: its
/// `MemoryReservation` object is alive, which means the stream or operator that
/// owns it has not been dropped.
pub fn query_memory_pool_top_consumers(top: usize) -> Option<String> {
    match query_memory_pool() {
        QueryMemoryPool::Tracked(pool) => Some(pool.report_top(top)),
        QueryMemoryPool::Plain(_) | QueryMemoryPool::Unbounded => None,
    }
}

/// Sample the pool onto its gauges. Cheap (two atomic loads); call from a timer.
pub fn sample_query_memory_pool_gauges() {
    let Some((reserved, limit)) = query_memory_pool_usage() else {
        // UNBOUNDED. Publish it as zero rather than returning: an absent gauge
        // and a healthy one look identical on a dashboard, and "the pool is
        // unbounded" is the single most important thing this can say — it is
        // the configuration that OOM-killed hosts on 2026-08-19.
        metrics::gauge!("siglake_query_memory_pool_bytes").set(0.0);
        metrics::gauge!("siglake_query_memory_pool_available_bytes").set(0.0);
        metrics::gauge!("siglake_query_memory_pool_used_ratio").set(0.0);
        return;
    };
    metrics::gauge!("siglake_query_memory_pool_reserved_bytes").set(reserved as f64);
    metrics::gauge!("siglake_query_memory_pool_available_bytes")
        .set(limit.saturating_sub(reserved) as f64);
    // Utilisation as a ratio so an alert does not have to know the pod size.
    metrics::gauge!("siglake_query_memory_pool_used_ratio").set(if limit == 0 {
        0.0
    } else {
        reserved as f64 / limit as f64
    });
    // THE BUDGET ITSELF, re-published every tick.
    //
    // These were set inside `session_state_template`, a `OnceLock` that
    // initialises before the Prometheus recorder is installed — and
    // `metrics::gauge!` is a silent no-op with no recorder, so they were
    // dropped and never set again. `siglake_query_memory_pool_bytes` has
    // existed since 2026-08-20 and had therefore never appeared in a scrape.
    //
    // A budget that cannot be scraped cannot be checked, which is the entire
    // point of publishing one. Anything set once at construction is at the
    // mercy of initialisation order; a timer is not.
    metrics::gauge!("siglake_query_memory_pool_bytes").set(limit as f64);
    // IN FORCE, not derived: with the cache env vars set these differ, and the
    // gauge that matters is the one describing what this process actually did.
    let (read_caches, metadata_caches, text_index_caches) = reserved_cache_bytes_in_force();
    metrics::gauge!("siglake_query_memory_reserved_elsewhere_bytes")
        .set((read_caches + metadata_caches + text_index_caches) as f64);
    metrics::gauge!("siglake_cache_budget_bytes", "kind" => "read").set(read_caches as f64);
    metrics::gauge!("siglake_cache_budget_bytes", "kind" => "metadata").set(metadata_caches as f64);
    metrics::gauge!("siglake_cache_budget_bytes", "kind" => "text_index")
        .set(text_index_caches as f64);
}

/// A `TaskContext` that uses the bounded pool. Use instead of
/// `TaskContext::default()`, which does not.
///
/// Carries a DEFAULT `SessionConfig` and no functions, so this is for callers
/// that want the shared runtime and nothing else: a plan built by hand, or a
/// test asserting on the pool itself. A REQUEST path must use
/// [`bounded_task_context_from`] — this one discards every
/// `datafusion.execution.*` option the plan's session was built with,
/// silently, which is what #2251 was. `clippy.toml` disallows calling this, so
/// a caller that really does want the runtime alone writes
/// `#[allow(clippy::disallowed_methods)]` and says why.
pub fn bounded_task_context() -> Arc<datafusion::execution::TaskContext> {
    bounded_task_context_from(datafusion::execution::TaskContext::default())
}

/// The planning session's own `TaskContext`, re-pointed at the shared query
/// runtime: the session's `SessionConfig` and function registries, the
/// process-wide bounded memory pool.
///
/// Both halves are load-bearing, which is why neither `SessionContext::
/// task_ctx()` nor [`bounded_task_context`] can be used alone:
///
/// - `task_ctx()` alone carries a `RuntimeEnv` whose pool is the SESSION's.
///   For a context cloned from [`session_state_template`] that is in fact the
///   shared one, but nothing makes it so for a hand-built session, and the
///   pool accounting, the spill configuration and the object-store registry
///   all hang off the runtime this replaces.
/// - `bounded_task_context()` alone carries `SessionConfig::new()`, so an
///   option set on the session — `datafusion.execution.batch_size`,
///   `sort_spill_reservation_bytes`, anything an operator reads at execution
///   rather than at planning — is dropped with no error and no log line.
///
/// Takes the context by value because `DataFrame::task_ctx()` hands one over
/// owned; nothing is cloned here.
///
/// Cheaper than [`bounded_task_context`], incidentally, and by an order of
/// magnitude: measured in release on the dev box, `TaskContext::default()`
/// costs ~105µs a call — a fresh `SessionConfig`, i.e. a whole
/// `ConfigOptions`, is nearly all of it; the `RuntimeEnv` it then throws away
/// is only ~0.8µs — against ~10µs for taking the session's
/// (`TaskContext::from(&SessionState)`: clone the config and the three
/// function maps). Every collect paid the former.
pub fn bounded_task_context_from(
    session: datafusion::execution::TaskContext,
) -> Arc<datafusion::execution::TaskContext> {
    Arc::new(session.with_runtime(shared_query_runtime_env()))
}

/// Phase 5 — a fresh, isolated per-query `SessionContext` cloned from the shared
/// [`session_state_template`] (skipping the ~235µs default-feature build) with a
/// brand-new catalog list so registrations don't leak across concurrent queries.
fn base_context() -> SessionContext {
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemoryCatalogProviderList};
    use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemorySchemaProvider};

    let ctx = SessionContext::new_with_state(session_state_template().clone());
    // Replace the catalog `Arc` the clone shares with the template with a fresh
    // one — DataFusion's default catalog/schema names are `datafusion`/`public`.
    let default_catalog = ctx
        .copied_config()
        .options()
        .catalog
        .default_catalog
        .clone();
    let default_schema = ctx.copied_config().options().catalog.default_schema.clone();
    let list = Arc::new(MemoryCatalogProviderList::new());
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(&default_schema, Arc::new(MemorySchemaProvider::new()))
        .expect("register default schema on a fresh in-memory catalog");
    list.register_catalog(default_catalog, catalog);
    ctx.register_catalog_list(list);
    ctx
}

pub fn session_context_with_order(
    target_partitions: Option<usize>,
    shard: Option<ScanShard>,
    preferred_scan_order: Option<PreferredScanOrder>,
) -> SessionContext {
    let ctx = base_context();
    // Layer the per-query config knobs onto the cloned state's config. (The
    // base invariants — `prefer_existing_sort`, `listing_table_ignore_subdirectory`
    // — are already baked into the template's config and survive the clone.)
    let mut state = ctx.state();
    let config = state.config_mut();
    if let Some(n) = target_partitions {
        config.options_mut().execution.target_partitions = n.max(1);
    }
    if let Some(s) = shard {
        config.set_extension(Arc::new(s));
    }
    if let Some(order) = preferred_scan_order {
        config.set_extension(Arc::new(order));
    }
    SessionContext::new_with_state(state)
}

/// Read-only planning options for SQL this crate assembles around text it did
/// not write itself.
///
/// `SessionContext::sql` is `sql_with_options(sql, SQLOptions::new())`, and that
/// default permits DDL, DML and session statements — worse, it runs
/// `LogicalPlan::Ddl` EAGERLY inside the call, before the caller ever holds a
/// `DataFrame`. Task #1523 measured what the permissive default is worth on the
/// query server: `COPY (SELECT raw FROM events) TO '/abs/path.csv'` wrote tenant
/// rows to an arbitrary path, 200 OK, and `CREATE EXTERNAL TABLE … LOCATION` read
/// outside the warehouse during planning.
///
/// The compactor's delete-task executor is the same shape one process removed.
/// A task's `predicate_sql` is validated in `siglake-query-server`
/// (`delete_tasks_routes::validate_delete_predicate_fragment`, which requires the
/// wrapped fragment to parse as exactly one `Statement::Query`) but it reaches
/// [`iceberg::IcebergContext::execute_delete_tasks`] as a PERSISTED warehouse
/// object, read back by a different binary. No escape through the two
/// `SELECT … WHERE {fragment}` templates has been found and none is claimed here;
/// what is claimed is that the planner, not the distance to the validator, is
/// what should be refusing. So the storage side states the policy too.
///
/// `sql_with_options` verifies the plan BEFORE `execute_logical_plan`, so a
/// refusal costs the DDL nothing.
///
/// Regression: `tests/storage/delete_task_read_only_predicate.rs`.
pub fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// [`read_only_sql_options`] applied to one context. Use this instead of
/// `ctx.sql` for any statement built from a string this process did not
/// construct entirely from its own literals.
pub async fn plan_read_only_sql(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    ctx.sql_with_options(sql, read_only_sql_options()).await
}

/// Default Parquet writer properties for siglake:
/// - Parquet v2 (Parquet 2.0 page format) — required by user spec.
/// - ZSTD compression level 3 (good balance for log payloads).
/// - Dictionary encoding enabled (huge win on host/source/sourcetype/index).
/// - Page row count limit set so per-row-group statistics stay useful for pruning.
pub fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_dictionary_enabled(true)
        .set_data_page_row_count_limit(20_000)
        .set_max_row_group_row_count(Some(1_048_576))
        .build()
}

/// Serialize a [`RecordBatch`] to Parquet v2 bytes in memory and PUT to `path`.
pub async fn write_batch_as_parquet(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    batch: &RecordBatch,
) -> Result<u64> {
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    {
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch.schema(), Some(default_writer_properties()))?;
        writer.write(batch)?;
        writer.close()?;
    }
    let bytes_written = buf.len() as u64;
    store
        .put(path, PutPayload::from(Bytes::from(buf)))
        .await
        .with_context(|| format!("PUT {}", path))?;
    Ok(bytes_written)
}

/// Register a directory of Parquet files as a DataFusion table.
///
/// `url` must be a valid object-store URL (`file:///abs/path/`, `s3://bucket/prefix/`, …).
/// If `schema` is provided, it is used directly (avoids the listing round-trip and
/// makes empty directories queryable). Otherwise schema is inferred from the
/// first Parquet file found.
///
/// Recursion: this builds a [`ListingTable`] that recurses into subdirectories
/// via `object_store.list(prefix)`, which is necessary for time-partitioned
/// layouts (`events/YYYY/MM/DD/HH/*.parquet`).
pub async fn register_parquet_dir(
    ctx: &SessionContext,
    url: &str,
    table_name: &str,
    schema: Option<SchemaRef>,
) -> Result<()> {
    let table_url = ListingTableUrl::parse(url)?;
    let parquet_format = Arc::new(ParquetFormat::default());
    let listing_opts = ListingOptions::new(parquet_format).with_file_extension(".parquet");

    let config = ListingTableConfig::new(table_url).with_listing_options(listing_opts);
    let config = match schema {
        Some(s) => config.with_schema(s),
        None => config.infer_schema(&ctx.state()).await?,
    };
    let table = ListingTable::try_new(config)?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use datafusion::datasource::empty::EmptyTable;

    #[test]
    fn core_tokenizer_names_parse_in_bloom_registry() {
        for &name in siglake_core::index_config::ALLOWED_TEXT_TOKENIZERS {
            assert!(
                siglake_bloom::Tokenizer::parse(name).is_some(),
                "unrecognized tokenizer name `{name}`"
            );
        }
    }

    /// Phase 5 — two contexts built from the shared `session_state_template`
    /// must NOT share a table namespace: a table registered in one is invisible
    /// to the other. This is the isolation invariant that makes the cheap
    /// template clone safe for concurrent queries (the naive `state.clone()`
    /// shares the catalog `Arc` and would leak; `base_context` installs a fresh
    /// catalog list to close that).
    #[test]
    fn session_context_template_isolation() {
        let a = session_context_with_order(None, None, None);
        let b = session_context_with_order(None, None, None);
        let empty = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        a.register_table("probe", empty).unwrap();
        assert!(a.table_exist("probe").unwrap(), "own registration visible");
        assert!(
            !b.table_exist("probe").unwrap(),
            "a table registered in one context leaked into a sibling — \
             concurrent queries would corrupt each other's catalog"
        );
    }

    /// The per-query config knobs (target partitions + the shard / scan-order
    /// extensions) must still land on the cloned-template context — the base
    /// invariants live in the template, but these are layered per query.
    #[test]
    fn session_context_applies_per_query_config() {
        use query_provider::{PreferredScanOrder, ScanShard};
        let ctx = session_context_with_order(
            Some(7),
            Some(ScanShard { index: 1, count: 3 }),
            Some(PreferredScanOrder::timestamp(false)),
        );
        let state = ctx.state();
        let cfg = state.config();
        assert_eq!(cfg.target_partitions(), 7, "target_partitions applied");
        assert!(
            cfg.get_extension::<ScanShard>().is_some(),
            "shard extension applied"
        );
        assert!(
            cfg.get_extension::<PreferredScanOrder>().is_some(),
            "scan-order extension applied"
        );
        // Base invariant from the template survives the clone.
        assert!(
            cfg.options().optimizer.prefer_existing_sort,
            "template's prefer_existing_sort survived the clone"
        );
    }

    /// Both halves of the execution context a query runs with (#2251): the
    /// session's own config and functions, the process-wide bounded runtime.
    /// `bounded_task_context()` alone silently dropped the first half, which is
    /// what the tests above could not see.
    #[test]
    fn bounded_task_context_from_keeps_the_config_and_swaps_the_runtime() {
        use datafusion::execution::TaskContext;
        use query_provider::ScanShard;

        let ctx = session_context_with_order(Some(7), Some(ScanShard { index: 1, count: 3 }), None);
        let mut state = ctx.state();
        state.config_mut().options_mut().execution.batch_size = 1_024;
        let session = TaskContext::from(&state);
        let udfs = session.scalar_functions().len();
        assert!(
            udfs > 0,
            "the template registers DataFusion's own functions"
        );

        let task_ctx = bounded_task_context_from(session);
        let cfg = task_ctx.session_config();
        assert_eq!(cfg.batch_size(), 1_024, "execution option carried through");
        assert_eq!(cfg.target_partitions(), 7, "per-query knob carried through");
        assert!(
            cfg.get_extension::<ScanShard>().is_some(),
            "per-query extension carried through"
        );
        assert_eq!(
            task_ctx.scalar_functions().len(),
            udfs,
            "function registry carried through"
        );
        assert!(
            Arc::ptr_eq(&task_ctx.runtime_env(), &shared_query_runtime_env()),
            "execution must run on the SHARED runtime: the bounded memory pool, \
             the spill configuration and the object-store registry hang off it"
        );

        // The no-session helper is the same runtime with none of the config.
        #[allow(clippy::disallowed_methods)]
        let default = bounded_task_context();
        assert_eq!(default.session_config().batch_size(), 8_192);
        assert!(Arc::ptr_eq(
            &default.runtime_env(),
            &shared_query_runtime_env()
        ));
    }
}

#[cfg(test)]
mod query_memory_pool_tests {
    use super::query_pool_bytes_from;

    /// The default: a fraction of the container limit. Before this, the pool was
    /// unbounded and 8 concurrent sorts reached 31.6 GB on a 32 GB node.
    #[test]
    fn sizes_from_the_container_limit() {
        let gb32 = 32 * 1024 * 1024 * 1024u64;
        assert_eq!(query_pool_bytes_from(Some(gb32), 0.5, None), Some(gb32 / 2));
    }

    /// No cgroup limit and no override: stay unbounded rather than invent a
    /// number. The warning next to this is what makes it visible.
    #[test]
    fn unbounded_when_there_is_nothing_to_size_against() {
        assert_eq!(query_pool_bytes_from(None, 0.5, None), None);
    }

    /// An explicit override wins over the container limit, in both directions.
    #[test]
    fn explicit_override_wins() {
        let gb32 = 32 * 1024 * 1024 * 1024u64;
        assert_eq!(
            query_pool_bytes_from(Some(gb32), 0.5, Some(1234)),
            Some(1234)
        );
        assert_eq!(query_pool_bytes_from(None, 0.5, Some(1234)), Some(1234));
    }

    /// 0 is the documented escape hatch back to the old behaviour.
    #[test]
    fn zero_means_unbounded() {
        assert_eq!(query_pool_bytes_from(Some(1 << 30), 0.5, Some(0)), None);
    }

    /// A floor, so a tiny container does not turn every query into
    /// ResourcesExhausted -- that trades an OOM for a different total outage.
    #[test]
    fn a_tiny_container_still_gets_a_workable_floor() {
        let mb64 = 64 * 1024 * 1024u64;
        assert_eq!(
            query_pool_bytes_from(Some(mb64), 0.5, None),
            Some(256 * 1024 * 1024)
        );
    }

    #[test]
    fn fraction_scales_the_pool() {
        let gb32 = 32 * 1024 * 1024 * 1024u64;
        assert_eq!(
            query_pool_bytes_from(Some(gb32), 0.25, None),
            Some(gb32 / 4)
        );
    }

    /// Consumer tracking is ON unless explicitly refused: a residual
    /// reservation that cannot name its holder (2026-09-01, 3.66 GiB on an
    /// idle pod) is the failure this exists to end, so the default has to be
    /// the diagnosable one.
    #[test]
    fn consumer_tracking_defaults_on_and_only_explicit_values_turn_it_off() {
        use super::query_pool_tracks_consumers_from;
        assert!(query_pool_tracks_consumers_from(None));
        assert!(query_pool_tracks_consumers_from(Some("")));
        assert!(query_pool_tracks_consumers_from(Some("1")));
        assert!(query_pool_tracks_consumers_from(Some("garbage")));
        for off in ["0", "off", "OFF", "false", "no", " no "] {
            assert!(
                !query_pool_tracks_consumers_from(Some(off)),
                "{off:?} must disable tracking"
            );
        }
    }
}

#[cfg(test)]
mod query_spill_tests {
    use super::{query_spill_dir_from, query_spill_max_bytes_from, with_query_spill_config};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    #[test]
    fn spill_directory_resolver_preserves_unset_default() {
        assert_eq!(query_spill_dir_from(None), None);
        assert_eq!(query_spill_dir_from(Some("")), None);
        assert_eq!(query_spill_dir_from(Some("  ")), None);
        assert_eq!(
            query_spill_dir_from(Some(" /var/lib/siglake/spill ")),
            Some("/var/lib/siglake/spill".into())
        );
    }

    #[test]
    fn spill_max_bytes_resolver_accepts_only_positive_bytes() {
        assert_eq!(query_spill_max_bytes_from(None), None);
        assert_eq!(query_spill_max_bytes_from(Some("")), None);
        assert_eq!(query_spill_max_bytes_from(Some("invalid")), None);
        assert_eq!(query_spill_max_bytes_from(Some("0")), None);
        assert_eq!(
            query_spill_max_bytes_from(Some(" 8589934592 ")),
            Some(8_589_934_592)
        );
    }

    #[test]
    fn spill_configuration_reaches_datafusion_disk_manager() {
        let root = tempfile::tempdir().unwrap();
        let runtime = with_query_spill_config(
            RuntimeEnvBuilder::new(),
            Some(root.path().to_path_buf()),
            Some(123_456),
        )
        .build()
        .unwrap();

        assert_eq!(runtime.disk_manager.max_temp_directory_size(), 123_456);
        let dirs = runtime.disk_manager.temp_dir_paths();
        assert_eq!(dirs.len(), 1);
        assert!(
            dirs[0].starts_with(root.path()),
            "DataFusion temp directory {:?} is outside configured root {}",
            dirs[0],
            root.path().display()
        );
    }
}

#[cfg(test)]
mod memory_sizing_fallback_tests {
    use super::{memory_limit_for_sizing, system_memory_bytes};

    /// On Linux this must find something. If it returns None the query pool
    /// falls back to UNBOUNDED, which is the configuration that OOM-killed
    /// hosts -- so "no answer" is a failure, not a shrug.
    #[test]
    #[cfg(target_os = "linux")]
    fn system_memory_is_readable_on_linux() {
        let total = system_memory_bytes().expect("/proc/meminfo MemTotal must parse on Linux");
        assert!(
            total > 256 * 1024 * 1024,
            "implausible MemTotal: {total} bytes"
        );
    }

    /// The whole point of the fallback: a host with no cgroup limit still gets
    /// a bound. A bench fleet running containers without --memory hit exactly
    /// this and the pool silently stayed unbounded.
    #[test]
    #[cfg(target_os = "linux")]
    fn sizing_finds_a_limit_even_without_a_cgroup() {
        assert!(
            memory_limit_for_sizing().is_some(),
            "no limit to size against means an unbounded pool"
        );
    }
}

#[cfg(test)]
mod pool_reservation_tests {
    use super::query_pool_bytes_full;

    /// The measured over-commit: a 61 GB node with a 16 GiB object cache and an
    /// 8 GiB file cache was handed a 30.9 GiB pool -- 55 of 61 GB committed
    /// before any decode buffer.
    #[test]
    fn caches_are_subtracted_before_the_fraction() {
        let gb = 1024 * 1024 * 1024u64;
        let node = 61 * gb;
        let caches = 16 * gb + 8 * gb;
        let pool = query_pool_bytes_full(Some(node), 0.5, None, caches).unwrap();
        assert!(
            pool + caches < node,
            "pool {pool} + caches {caches} must fit inside {node}"
        );
        assert_eq!(pool, (node - caches) / 2);
    }

    /// With no caches configured the behaviour is unchanged.
    #[test]
    fn no_reservation_is_the_old_arithmetic() {
        let gb = 1024 * 1024 * 1024u64;
        assert_eq!(
            query_pool_bytes_full(Some(32 * gb), 0.5, None, 0),
            Some(16 * gb)
        );
    }

    /// Caches larger than the machine must not underflow into a huge pool, and
    /// must not floor to nothing either.
    #[test]
    fn absurd_reservations_do_not_underflow() {
        let gb = 1024 * 1024 * 1024u64;
        let pool = query_pool_bytes_full(Some(4 * gb), 0.5, None, 99 * gb).unwrap();
        assert!(pool <= 4 * gb, "must never exceed the machine: {pool}");
        assert!(
            pool >= 256 * 1024 * 1024,
            "must stay workable rather than wrapping or flooring to ~0: {pool}"
        );
    }

    /// The measured misconfiguration: 24 GiB of caches on a 30.8 GiB node left
    /// the pool at 3.4 GiB and made every large query spill. The floor keeps the
    /// engine usable; the warning tells the operator to fix the cache sizes.
    #[test]
    fn cache_overconfiguration_cannot_starve_the_pool() {
        let gb = 1024 * 1024 * 1024u64;
        let node = 30 * gb;
        let pool = query_pool_bytes_full(Some(node), 0.5, None, 24 * gb).unwrap();
        assert!(
            pool >= node / 8,
            "pool {pool} is a starvation share of {node}"
        );
    }
}

#[cfg(test)]
mod derived_cache_reservation_tests {
    use super::{query_pool_bytes_full, resolve_query_read_cache_config};

    /// The Kubernetes path. The object cache is derived from the cgroup limit
    /// while the file cache remains off, so the pool must subtract the resolved
    /// object budget rather than independently deriving both caches.
    ///
    /// On the chart's default 4Gi query pod: the object cache is 1 GiB, so the
    /// pool must fit in the remaining 3 GiB rather than taking 2 GiB on top.
    #[test]
    fn resolved_caches_and_the_pool_fit_inside_the_pod() {
        let gb = 1024 * 1024 * 1024u64;
        let pod = 4 * gb;
        let caches = resolve_query_read_cache_config(Some(pod), None, None, None).reserved_bytes();
        let pool = query_pool_bytes_full(Some(pod), 0.5, None, caches).unwrap();
        // Headroom, not just "fits": the process, in-flight Arrow batches,
        // decode buffers and allocator slack all live outside both numbers.
        let committed = (pool + caches) as f64 / pod as f64;
        assert!(
            committed <= 0.75,
            "caches + pool commit {:.0}% of the pod, leaving too little for the \
             process itself",
            committed * 100.0
        );
        // The pre-fix sizing must be visibly worse, or this test is about
        // arithmetic in general rather than about the fix.
        let unsubtracted = query_pool_bytes_full(Some(pod), 0.5, None, 0).unwrap();
        let before = (unsubtracted + caches) as f64 / pod as f64;
        assert!(
            before >= 0.75,
            "pre-fix commitment was {:.0}%, so there was nothing to fix",
            before * 100.0
        );
    }
}
