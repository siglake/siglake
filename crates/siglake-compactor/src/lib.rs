//! Compactor daemon: drains all sealed WAL segments into a single
//! sorted Iceberg `fast_append` commit per cycle.
//!
//! Per-cycle flow:
//!
//! 1. List + atomically claim every sealed WAL segment by renaming
//!    `{wal}/sealed/X.arrow` → `{wal}/processing/X.arrow`. (Atomic rename
//!    is our coordination primitive on local FS; phase 4 swaps this for a
//!    catalog row.)
//! 2. Read every `RecordBatch` from each claimed segment.
//! 3. Concatenate everything into one batch (cheap — same schema).
//! 4. Call [`IcebergContext::append_batch`], which **time-orders the rows**
//!    (the table's declared sort order) and writes one Parquet data file
//!    (with bloom filters on `host`/`source`/`sourcetype`/`index` + the
//!    declared `SortingColumn` footer) under a single `fast_append` commit.
//!    So tight row-group min/max stats come from the storage layer, not a
//!    compactor pre-sort.
//! 5. On success: delete every `processing/` file.
//!    On failure: rename them all back to `sealed/` for retry.
//!
//! All-or-nothing semantics: if any step fails after claim, every claimed
//! segment is released back to `sealed/` so the next cycle picks them up.
//! This means we either commit the whole batch or none of it — Iceberg's
//! optimistic concurrency means we never split a logical batch across
//! commits.
//!
//! One exception, and only for step 2 (#3143): a segment whose bytes do not
//! decode fails every batch it is ever claimed into, so after
//! `SIGLAKE_COMPACTOR_POISON_ATTEMPTS` consecutive read failures the drain
//! moves that file — and only that file — to `{wal}/poison/` with a note
//! saying why. Its batch siblings are released and commit on the next cycle.
//! Nothing there is deleted or automatically requeued; an operator moves the
//! file back into `sealed/` once its cause is fixed.
//!
//! Phase 4 will introduce target-row-group-byte sizing (multi-file
//! splitting when a batch exceeds ~256 MB compressed) and a partition
//! spec (`day(timestamp)`) so multi-day batches fan out correctly.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use tokio::time::sleep;

use opendal::Operator;
use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::mapping::map_carrier_batch_with_stats;
use siglake_storage::catalog_claim::{
    ConsumedProofWatermark, LocalCommittedSegment, ProofProvenance, SqlSegmentClaim,
};
use siglake_storage::consumed_proof::{ConsumedProofEntry, ConsumedProofRead, ReclaimProofSources};
use siglake_storage::iceberg::{
    AppendIncarnationMismatch, DeleteTaskState, IcebergContext, LevelPolicy, LeveledPassOptions,
    NonTerminalDeleteTask, NonTerminalDeleteTaskObservation, ObservedDeleteTaskClaim,
    ProofMaintenanceIncarnationMismatch, ReclusterPolicy, ShortAggregateOutcome,
};
use siglake_wal::{
    claim_segment, finish_segment, list_committed, list_index_dirs, list_orphaned, list_poisoned,
    list_sealed, list_tenant_dirs, list_visible, quarantine_poison_segment, read_segment,
    read_segment_from_bytes, recover_orphaned_processing, release_segment, sweep_committed_gated,
};

/// How long to retain `committed/` segments before sweep deletes them.
/// Gives secondary consumers (detector et al.) a wider catch-up window. This is
/// the **soft floor** — committed segments are never swept before this, and are
/// held longer if a fresh consumer hasn't processed them yet (see
/// [`sweep_committed_gated`]).
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(60);
/// Hard ceiling on `committed/` retention: a segment older than this is swept
/// regardless of consumer watermarks, so a stuck/lagging consumer can't grow
/// `committed/` without bound (detection loses segments only if it lags this far).
pub const CONSUMER_MAX_RETENTION: Duration = Duration::from_secs(3600);
/// A consumer whose watermark hasn't advanced within this window is treated as
/// dead and stops holding segments back from the sweep.
pub const CONSUMER_STALE_AFTER: Duration = Duration::from_secs(300);
/// Bound the local-FS compactor's in-memory working set. `0` disables
/// the corresponding limit.
pub const DEFAULT_FS_MAX_SEGMENTS: usize = 64;
pub const DEFAULT_FS_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Consecutive failed reads a local segment gets before the drain sets it
/// aside under `poison/` — see [`poison_attempts`].
pub const DEFAULT_POISON_ATTEMPTS: u32 = 3;
/// #4651: claim attempts one drain pass will spend on one segment before
/// leaving it in `sealed/` for the next cycle. Not a knob: the setting an
/// operator has for how many times a segment is tried is
/// `SIGLAKE_COMPACTOR_POISON_ATTEMPTS`, which counts across cycles and decides
/// whether the segment is set aside for good. This bound only stops one pass
/// from spending its whole cycle budget re-claiming a set that keeps failing,
/// and it has to be at least 2 for the same-cycle retry a transient commit
/// error gets (`a_transient_commit_error_is_retried_in_the_same_cycle`).
const MAX_PASS_CLAIM_ATTEMPTS: u32 = 3;

#[derive(Clone, Copy)]
struct FsBatchConfig {
    max_segments: usize,
    max_bytes: u64,
}

struct CommitOutcome {
    segments: usize,
    rows: u64,
    strict_residual_rows: u64,
}

/// One filesystem sweep's sealed backlog, summed per tenant label.
///
/// `siglake_compactor_sealed_pending{tenant}` is the count of sealed WAL
/// segments waiting to be claimed for a tenant, and it drives the compactor
/// HPA (Helm `autoscaling.compactor.customMetric`) — on this path only, where
/// the reading is one pod's own. The claim path publishes the whole shared
/// queue, which an HPA can only read per pod, so the chart refuses that
/// combination and `siglake-operator` does the division instead (#3718). A
/// tenant's segments are
/// spread over its events directory and one directory per managed index, so
/// the reading is only that count if it covers all of them: #3691 was every
/// directory `set`ting the same tenant-labelled gauge in turn, which left the
/// last one visited — often a small, quiet index — describing the tenant and
/// hiding whatever was queued ahead of it.
///
/// Each visited directory calls [`Self::observe_dir`], including the ones the
/// sweep skips and the ones that are empty, and the caller publishes once at
/// the end. An observation that is cut short publishes nothing: a partial
/// total reads as a drop in backlog, which is the one reading an autoscaler
/// must not be given on no evidence.
///
/// `siglake_compactor_segments_poisoned{tenant}` rides along (#3143): it is
/// per-tenant, summed over the same directories, and clobberable in exactly
/// the same way, so it is counted and published here rather than `set` by each
/// directory in turn.
#[derive(Default)]
struct SealedBacklog {
    by_tenant: BTreeMap<String, usize>,
    poisoned_by_tenant: BTreeMap<String, usize>,
}

impl SealedBacklog {
    /// Add one directory's sealed count, and the segments set aside under its
    /// `poison/`, to its tenant's totals. Zero still registers the label, so a
    /// tenant that drains to empty reports zero instead of keeping its last
    /// non-zero reading.
    ///
    /// A `poison/` that cannot be listed counts zero for this sweep: the
    /// reading is an operator's cue, and the drain has better ways to report a
    /// directory it cannot read than to refuse the backlog gauge over it.
    fn observe_dir(&mut self, tenant_label: &str, dir: &Path, sealed: usize) {
        *self.by_tenant.entry(tenant_label.to_string()).or_default() += sealed;
        let poisoned = list_poisoned(dir).map(|v| v.len()).unwrap_or(0);
        *self
            .poisoned_by_tenant
            .entry(tenant_label.to_string())
            .or_default() += poisoned;
    }

    /// Add segments this sweep set aside itself. The directory listing that
    /// opened the cycle predates them, and a level an operator alerts on
    /// should not wait for the next sweep to move.
    fn observe_poisoned(&mut self, tenant_label: &str, poisoned: usize) {
        *self
            .poisoned_by_tenant
            .entry(tenant_label.to_string())
            .or_default() += poisoned;
    }

    /// Publish one reading per tenant label seen this sweep, plus zero for
    /// labels that disappeared after the previous complete sweep.
    fn publish(&self, previously_published: &mut BTreeSet<String>) {
        let published_this_sweep: BTreeSet<_> = self.by_tenant.keys().cloned().collect();
        for tenant in previously_published.difference(&published_this_sweep) {
            metrics::gauge!(
                "siglake_compactor_sealed_pending",
                "tenant" => tenant.clone()
            )
            .set(0.0);
            metrics::gauge!(
                "siglake_compactor_segments_poisoned",
                "tenant" => tenant.clone()
            )
            .set(0.0);
        }
        for (tenant, sealed) in &self.by_tenant {
            metrics::gauge!(
                "siglake_compactor_sealed_pending",
                "tenant" => tenant.clone()
            )
            .set(*sealed as f64);
            metrics::gauge!(
                "siglake_compactor_segments_poisoned",
                "tenant" => tenant.clone()
            )
            .set(self.poisoned_by_tenant.get(tenant).copied().unwrap_or(0) as f64);
        }
        *previously_published = published_this_sweep;
    }
}

enum CommitTarget {
    Events,
    Index {
        index: String,
        config: Box<IndexConfig>,
        bloom_columns: Vec<String>,
        /// #2836: the table the ownership check placed this cycle's segments
        /// in, carried through to the append so the handle that writes the
        /// files and bases the transaction is checked against it. `None` when
        /// the check had no opinion (the index name resolved to no table).
        ///
        /// An index NAME is all the append would otherwise have, and the name
        /// can mean a different table by the time it resolves it.
        expect_table_uuid: Option<String>,
    },
}

impl CommitTarget {
    fn index_label(&self) -> &str {
        match self {
            CommitTarget::Events => "events",
            CommitTarget::Index { index, .. } => index.as_str(),
        }
    }

    /// Bind an already-resolved index target to the identity its ownership
    /// check verified. The catalog-claim drain resolves the target (which
    /// creates the table when a template applies) before it can ask what that
    /// table's uuid is, so the two steps cannot be one expression.
    fn expecting_table_uuid(self, uuid: Option<&str>) -> Self {
        match self {
            CommitTarget::Events => CommitTarget::Events,
            CommitTarget::Index {
                index,
                config,
                bloom_columns,
                ..
            } => CommitTarget::Index {
                index,
                config,
                bloom_columns,
                expect_table_uuid: uuid.map(str::to_string),
            },
        }
    }
}

/// Commit-accumulation gate (BIG-4 `#4b`). Opt-in, off by default.
///
/// A 2026-06-01 EKS/RDS/S3 characterization showed the Iceberg commit
/// is a fixed ~1.6 s (manifest + manifest-list + catalog `update_table`),
/// 92 % of append cost, and *grows* with snapshot history. Committing
/// every ~4 K-row sealed segment amortizes that fixed cost over far too
/// few rows (~2.5 K rows/s ceiling).
///
/// When set, the run loop only commits once the sealed queue has piled
/// up at least `target_bytes` **or** its oldest segment is at least
/// `max_age` old — turning many tiny commits into few large ones. The
/// age floor bounds query-visibility latency for low-traffic tables;
/// the byte target keeps a busy table from waiting needlessly.
#[derive(Clone, Copy)]
pub struct CommitBatchPolicy {
    /// Commit as soon as this many sealed bytes have accumulated.
    pub target_bytes: u64,
    /// …but never let the oldest sealed segment age past this before
    /// committing, regardless of accumulated size (freshness floor).
    pub max_age: Duration,
}

/// How long a drained segment's catalog row and mirror object are retained
/// (`SIGLAKE_COMMITTED_RETENTION_SECS`). Unset or empty defaults to 24 hours;
/// 0 explicitly disables purging.
///
/// Enabled values are floored at [`MIN_COMMITTED_RETENTION_SECS`] so the
/// ingester can retire its local copy before mirror catch-up can re-register it.
fn committed_retention() -> Option<Duration> {
    committed_retention_from(
        std::env::var("SIGLAKE_COMMITTED_RETENTION_SECS")
            .ok()
            .as_deref(),
    )
}

/// Smallest enabled committed-mirror retention accepted by the compactor.
///
/// This must outlive both default windows before an ingester removes its local
/// sealed copy: the 600-second `SIGLAKE_WAL_LOCAL_SWEEP_SETTLE_SECS` delay plus
/// one 300-second `SIGLAKE_WAL_LOCAL_SWEEP_SECS` cadence. Otherwise retention
/// can remove the mirror object and catalog row while the local copy remains,
/// and the mirror catch-up sweep can upload and register that drained segment
/// again. One extra second makes the retention strictly longer than their sum.
///
/// Claim reclaim does not extend this floor: abandoned rows stay `processing`
/// (and are therefore ineligible for retention), and proving one committed
/// stamps a new `committed_at_ms` from which retention starts.
pub const MIN_COMMITTED_RETENTION_SECS: u64 = 901;
const DEFAULT_COMMITTED_RETENTION_SECS: u64 = 86_400;

/// Operation-scoped exclusion shared by mirror reconciliation and committed
/// retention. Their separate election leases choose one worker for each sweep;
/// this second lease prevents a stale mirror listing from spanning the
/// object-first retention delete and recreating the purged catalog row.
const MIRROR_RECONCILIATION_LEASE_ID: &str = "__maintenance__mirror_reconciliation";
const MIRROR_RECONCILIATION_LEASE_PURPOSE: &str = "mirror_reconciliation";

/// Maximum sealed mirror objects registered in one reconciliation pass.
///
/// At the documented 50K EPS / 4,096-row segment floor, 1,024 objects per
/// default 60-second cadence exceeds the creation rate while keeping the
/// operation-scoped retention exclusion and serial catalog work bounded.
const MIRROR_SYNC_PAGE_OBJECTS: usize = 1_024;

/// Maximum committed mirror objects examined in one retention run.
///
/// Retention runs every 300 seconds and is also bounded by the default
/// 600-second drain watchdog. At the documented 50K EPS / 4,096-row segment
/// floor, 16,384 objects per run is more than twice the 7,325 objects created
/// during that worst-case watchdog window. Pages keep the catalog result set
/// small; concurrent object deletes and batched row deletes make reaching the
/// budget practical instead of merely raising the old 512-object ceiling.
const RETENTION_RUN_OBJECTS: usize = 16_384;
const RETENTION_PAGE_OBJECTS: usize = 512;
const RETENTION_OBJECT_DELETE_CONCURRENCY: usize = 32;
const RETENTION_INTERVAL: Duration = Duration::from_secs(300);

/// Whether the filesystem drain marks the `wal_segments` ledger so committed
/// retention can reclaim its mirror objects (`SIGLAKE_MIRROR_LEDGER_RECLAIM`).
///
/// OFF by default. Turning it on gives the filesystem drain a claim-store
/// dependency it does not have today, and the reclamation it enables deletes
/// objects — so it is an operator's explicit choice per
/// `docs/DESIGN_wal_mirror_reclamation.md`, not an upgrade's side effect.
pub fn mirror_ledger_reclaim_enabled() -> bool {
    mirror_ledger_reclaim_from(
        std::env::var("SIGLAKE_MIRROR_LEDGER_RECLAIM")
            .ok()
            .as_deref(),
    )
}

/// The pure half, so the default and the accepted spellings are tested without
/// mutating process-global environment.
pub fn mirror_ledger_reclaim_from(configured: Option<&str>) -> bool {
    matches!(
        configured.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

/// Resolve committed mirror retention from its raw environment value.
///
/// Pure so configuration parsing can be tested without mutating the
/// process-global environment observed by parallel tests in this binary.
fn committed_retention_from(configured: Option<&str>) -> Option<Duration> {
    match configured {
        None | Some("") => Some(Duration::from_secs(DEFAULT_COMMITTED_RETENTION_SECS)),
        Some(value) => match value.parse::<u64>() {
            Ok(0) | Err(_) => None,
            Ok(seconds) => Some(Duration::from_secs(
                seconds.max(MIN_COMMITTED_RETENTION_SECS),
            )),
        },
    }
}

/// How long a maintenance election holds (`SIGLAKE_MAINTENANCE_LEASE_SECS`,
/// default 300s). Must exceed a sweep's duration or a second node starts the
/// same work mid-flight; the 30s gauge sample and multi-minute expiry passes set
/// the floor.
fn maintenance_lease_ttl() -> Duration {
    Duration::from_secs(
        std::env::var("SIGLAKE_MAINTENANCE_LEASE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300)
            .max(60),
    )
}

/// Base delay for the drain's adaptive idle backoff
/// (`SIGLAKE_DRAIN_IDLE_BACKOFF_MS`, default 50ms), clamped to `poll_interval` so
/// it can never exceed the configured cadence.
fn drain_idle_backoff_base(poll_interval: Duration) -> Duration {
    idle_backoff_base_from(
        std::env::var("SIGLAKE_DRAIN_IDLE_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok()),
        poll_interval,
    )
}

/// The pure half, so the bounds can be tested without touching process-global
/// env (parallel tests that `set_var` race each other -- which this one did).
fn idle_backoff_base_from(ms: Option<u64>, poll_interval: Duration) -> Duration {
    Duration::from_millis(ms.unwrap_or(50).max(1)).min(poll_interval)
}

/// Records elapsed time into `name` on drop.
///
/// Needed because the blocks being timed exit through `continue` and match arms,
/// not a single tail expression -- a plain `record()` after the block silently
/// misses exactly the paths worth measuring. The 2026-08-14 rounds spent three
/// attributions on stages that turned out to be nested or unmeasured; the point
/// of these timers is that `loop_iteration` and `cycle` bound everything inside
/// them, so the accounting closes by construction rather than by inference.
struct StageTimer(&'static str, std::time::Instant);

impl Drop for StageTimer {
    fn drop(&mut self) {
        metrics::histogram!(self.0).record(self.1.elapsed().as_secs_f64());
    }
}

impl CommitBatchPolicy {
    /// Whether an accumulated queue of `bytes` whose oldest segment is
    /// `oldest_age` old is worth committing now.
    fn ready(&self, bytes: u64, oldest_age: Duration) -> bool {
        bytes >= self.target_bytes || oldest_age >= self.max_age
    }
}

/// Tier-2 re-clustering maintenance, attached via
/// [`Compactor::with_reclustering`]. When set, the run loop periodically heals
/// the events-table layout (merging undersized / time-overlapping files) on its
/// own cadence, separate from the ingest-commit cycle. Default: off.
#[derive(Clone)]
pub struct ReclusterConfig {
    /// Minimum wall-clock gap between re-clustering passes (flat mode; in leveled
    /// mode the per-level `level_intervals` pace the passes instead).
    pub interval: Duration,
    /// Bounds for a single pass (the out-of-order safety valve).
    pub policy: ReclusterPolicy,
    /// LSM-style leveled compaction (A.4.2). When set, the run loop runs a bounded
    /// per-level pass ([`IcebergContext::recluster_pass_leveled`]) — each
    /// compaction merges one size-derived level's bounded fan-in toward the next,
    /// so it stays seconds–minutes and keeps up under sustained writes — instead of
    /// the flat whole-partition pass that forms the 1–2 h giant merge at scale.
    /// `None` ⇒ the legacy flat pass. See [`LevelPolicy`].
    pub levels: Option<LevelPolicy>,
    /// Per-level pass cadence for leveled mode (index = level). The leading edge
    /// is rescanned far more often than the tail: L0 fills continuously and wants
    /// a fast tick, while re-scanning a slow cold level is pure catalog load with
    /// no candidates to find (each pass re-lists the table's live files). Levels
    /// beyond the vec use the last entry. Default **10 s / 60 s / 60 m**.
    pub level_intervals: Vec<Duration>,
}

impl ReclusterConfig {
    /// Re-cluster at most every `interval`, using the default bounded policy and
    /// the legacy flat pass (no leveling).
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            policy: ReclusterPolicy::default(),
            levels: None,
            level_intervals: vec![
                Duration::from_secs(10),
                Duration::from_secs(60),
                Duration::from_secs(3600),
            ],
        }
    }

    /// Cadence for `level` (levels past the configured vec inherit the last
    /// entry; an empty vec falls back to the flat-mode `interval`).
    fn level_interval(&self, level: usize) -> Duration {
        self.level_intervals
            .get(level)
            .or(self.level_intervals.last())
            .copied()
            .unwrap_or(self.interval)
    }
}

/// Periodic snapshot-metadata expiry (BIG-4 `#4c`), attached via
/// [`Compactor::with_snapshot_expiry`]. The run loop drops all but the
/// most-recent `retain_last` snapshots from the events table's metadata
/// on its own cadence, bounding the `snapshots` array that otherwise
/// grows every commit and dominates per-commit catalog cost. Non-
/// destructive (metadata only). Default: off.
#[derive(Clone, Copy)]
pub struct ExpireConfig {
    /// Minimum wall-clock gap between expiry passes.
    pub interval: Duration,
    /// Number of most-recent snapshots to retain.
    pub retain_last: usize,
}

impl ExpireConfig {
    /// Expire at most every `interval`, retaining `retain_last` snapshots.
    pub fn new(interval: Duration, retain_last: usize) -> Self {
        Self {
            interval,
            retain_last: retain_last.max(1),
        }
    }
}

/// Catalog-claim configuration. When attached to a [`Compactor`] the
/// per-cycle flow switches off `list_sealed` + FS rename and onto the
/// shared `wal_segments` table: a multi-pod deployment can race
/// safely because `try_claim` is an atomic SQL `UPDATE`.
#[derive(Clone)]
pub struct CatalogClaimConfig {
    pub claim: SqlSegmentClaim,
    /// Object store containing the WAL mirror — the compactor fetches
    /// each claimed segment's bytes from `<prefix>/<filename>` here.
    pub store: Operator,
    /// Root-relative prefix the WAL mirror writes under (e.g.
    /// `wal-mirror`). The fully-qualified URL of each registered
    /// segment is built as `<store-url-root>/<prefix>/<filename>`.
    pub prefix: String,
    /// Max segments per `try_claim` cycle.
    pub batch_size: usize,
    /// Last time this pod ran the recovery sync (full mirror re-list +
    /// re-register). Ingesters register their own uploads on the hot
    /// path, so the sweep only exists to catch an ingester that crashed
    /// between upload and register — running it EVERY cycle re-listed
    /// the whole prefix and re-INSERTed every object per drain per
    /// cycle, which collapsed the claim path once a backlog accumulated
    /// (round 2, 2026-07-14). Interval:
    /// `SIGLAKE_MIRROR_SYNC_INTERVAL_SECS` (default 60; 0 = every cycle).
    pub last_mirror_sync: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    /// Last time this pod swept for claims abandoned in `processing` by a dead
    /// worker. Tracked separately from `last_mirror_sync` so that disabling the
    /// mirror scan does not also disable dead-worker recovery.
    pub last_reclaim: std::sync::Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

/// Which half of the compactor's work this process performs.
///
/// Setting `SIGLAKE_RECLUSTER_INTERVAL_SECS=0` does NOT make a process
/// drain-only: snapshot expiry is default-on, live-file gauge sampling always
/// runs, aggregate folding is scheduled by the same loop, and the next
/// maintenance feature will land there too by default. The 2026-08-14 rounds
/// measured 15,188 metadata reads on tables the workload never touched -- four
/// times as many `load_table` calls as for the table under test -- from
/// whole-warehouse sweeps running on every drain.
///
/// The cost is small in node-seconds today. It scales with table count TIMES
/// drain count, which is the wrong shape for both axes we expect to grow.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum CompactorRole {
    /// Foreground WAL -> Iceberg only: claim, fetch, prepare, append, mark.
    Drain,
    /// Whole-warehouse maintenance only: reclustering, expiry, gauges, folding,
    /// delete tasks. Should be elected per table via the lease, not run N-up.
    Maintenance,
    /// Both, the single-process default.
    #[default]
    Combined,
}

impl CompactorRole {
    /// Whether this process performs the foreground drain.
    pub fn drains(self) -> bool {
        matches!(self, Self::Drain | Self::Combined)
    }

    /// Whether this process performs whole-warehouse maintenance.
    pub fn maintains(self) -> bool {
        matches!(self, Self::Maintenance | Self::Combined)
    }

    /// Parse a role name; anything unrecognized is an error rather than a silent
    /// fallback to `Combined`, because a typo'd role that quietly keeps doing
    /// maintenance is exactly the failure this type exists to prevent.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "drain" => Ok(Self::Drain),
            "maintenance" => Ok(Self::Maintenance),
            "combined" => Ok(Self::Combined),
            other => Err(format!(
                "unknown compactor role {other:?} (expected drain|maintenance|combined)"
            )),
        }
    }
}

/// Reasons a consumed-proof watermark can be refused. Index ids are catalog
/// data, so their complete label combinations cannot be registered at startup.
const PROOF_WATERMARK_UNPROVED: &str = "unproved_watermark";
const PROOF_WATERMARK_INCARNATION_MISMATCH: &str = "incarnation_mismatch";
const PROOF_WATERMARK_UNVERIFIED_APPEND: &str = "unverified_append_watermark";
const PROOF_WATERMARK_SKIPPED_REASONS: &[&str] = &[
    PROOF_WATERMARK_UNPROVED,
    PROOF_WATERMARK_INCARNATION_MISMATCH,
    PROOF_WATERMARK_UNVERIFIED_APPEND,
];

/// Compactor handle. Cheap to clone: holds an [`Arc<IcebergContext>`].
#[derive(Clone)]
pub struct Compactor {
    role: CompactorRole,
    wal_dir: PathBuf,
    ice: Arc<IcebergContext>,
    retention: Duration,
    fs_batch: FsBatchConfig,
    catalog: Option<CatalogClaimConfig>,
    /// Ledger-only mirror reclamation for the filesystem drain (#4913): the
    /// same claim store and mirror operator as [`Self::catalog`], attached
    /// WITHOUT the claim path. Set only when `catalog` is `None`; the claim
    /// drain already purges what it commits.
    mirror_ledger: Option<CatalogClaimConfig>,
    /// Per-WAL-directory segment ids this process has already seen durably
    /// `committed` in the ledger, so the per-cycle re-mark over `committed/`
    /// costs one catalog round trip per NEW file rather than per file. Shared
    /// across clones, replaced (not accumulated) on every pass so it stays
    /// bounded by the live `committed/` population. Correctness comes from the
    /// ledger; a restart just re-marks.
    mirror_marked: Arc<std::sync::Mutex<HashMap<PathBuf, BTreeSet<String>>>>,
    recluster: Option<ReclusterConfig>,
    expire: Option<ExpireConfig>,
    delete_tasks_enabled: bool,
    /// Last time each stalled delete task's WARN line was printed, so the
    /// observation reports a task once per bound interval rather than once per
    /// poll (1,200 lines per stall at the default 1s interval). Shared across
    /// clones: one process, one rate limit. Emptied by a restart, which
    /// re-reports every stalled task once.
    delete_task_stall_logged:
        Arc<std::sync::Mutex<HashMap<uuid::Uuid, chrono::DateTime<chrono::Utc>>>>,
    /// Tenant labels exported by the previous complete filesystem sweep.
    /// Shared across clones so a later sweep can explicitly zero labels for
    /// WAL directories that have disappeared.
    published_fs_backlog_tenants: Arc<std::sync::Mutex<BTreeSet<String>>>,
    commit_batch: Option<CommitBatchPolicy>,
    drain_concurrency: Option<usize>,
    drain_cycle_budget: Option<Duration>,
    /// See [`Self::with_poison_attempts`]; `None` reads the environment.
    poison_attempts: Option<u32>,
    /// #3143: consecutive read failures charged to each local segment, by file
    /// name. A name is unique across the whole WAL (uuidv7-derived, minted by
    /// the writer), so one map covers every tenant and index directory.
    ///
    /// Entries appear only for a segment a drain could not read, and leave on
    /// its next successful commit or on its set-aside, so the map is bounded
    /// by the failing part of the backlog. Shared across clones; emptied by a
    /// restart, which costs a poisoned segment one more round of attempts and
    /// cannot lose its bytes.
    segment_read_failures: Arc<std::sync::Mutex<HashMap<String, u32>>>,
    /// #4674: the `(iceberg namespace, table)` readings the previous complete
    /// inline-coverage census published. Shared across clones for the reason
    /// `published_fs_backlog_tenants` is: a series is never removed from a live
    /// process, so an index dropped while its object was unprovable would page
    /// until a restart unless a later pass explicitly zeroes it.
    published_inline_coverage: Arc<std::sync::Mutex<BTreeSet<(String, String)>>>,
    /// See [`Self::with_verified_owner_for_test`].
    verified_owner_for_test: Option<String>,
    /// See [`Self::with_transient_fs_commit_failures_for_test`].
    transient_fs_commit_failures_for_test: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl Compactor {
    pub fn new(wal_dir: impl Into<PathBuf>, ice: Arc<IcebergContext>) -> Self {
        Self::with_retention(wal_dir, ice, DEFAULT_RETENTION)
    }

    pub fn with_retention(
        wal_dir: impl Into<PathBuf>,
        ice: Arc<IcebergContext>,
        retention: Duration,
    ) -> Self {
        let s = Self {
            role: CompactorRole::default(),
            wal_dir: wal_dir.into(),
            ice,
            retention,
            fs_batch: FsBatchConfig {
                max_segments: DEFAULT_FS_MAX_SEGMENTS,
                max_bytes: DEFAULT_FS_MAX_BYTES,
            },
            catalog: None,
            mirror_ledger: None,
            mirror_marked: Default::default(),
            recluster: None,
            expire: None,
            delete_tasks_enabled: false,
            delete_task_stall_logged: Default::default(),
            published_fs_backlog_tenants: Default::default(),
            published_inline_coverage: Default::default(),
            commit_batch: None,
            drain_concurrency: None,
            drain_cycle_budget: None,
            poison_attempts: None,
            segment_read_failures: Default::default(),
            verified_owner_for_test: None,
            transient_fs_commit_failures_for_test: None,
        };
        s.quarantine_processing_orphans();
        s
    }

    /// Bound the local-FS claim set per cycle. `0` disables the
    /// corresponding limit.
    pub fn with_fs_batch_limits(mut self, max_segments: usize, max_bytes: u64) -> Self {
        self.fs_batch = FsBatchConfig {
            max_segments,
            max_bytes,
        };
        self
    }

    /// Pin the number of concurrent WAL drain batches for this compactor,
    /// bypassing `SIGLAKE_DRAIN_CONCURRENCY`. Builder — useful for running
    /// several drain scenarios safely in one test process.
    pub fn with_drain_concurrency(mut self, concurrency: usize) -> Self {
        self.drain_concurrency = Some(concurrency.clamp(1, 64));
        self
    }

    /// Pin how long one filesystem drain cycle keeps dispatching batches,
    /// bypassing `SIGLAKE_DRAIN_CYCLE_BUDGET_SECS`. Builder, floored at 100 ms
    /// so a cycle always has room to admit its first batch.
    ///
    /// Same use as [`Self::with_drain_concurrency`]: tests can bound retryable
    /// failure scenarios without changing the process environment.
    pub fn with_drain_cycle_budget(mut self, budget: Duration) -> Self {
        self.drain_cycle_budget = Some(budget.max(Duration::from_millis(100)));
        self
    }

    /// Pin how many consecutive failed reads a local segment gets before the
    /// drain sets it aside under `poison/`, bypassing
    /// `SIGLAKE_COMPACTOR_POISON_ATTEMPTS`. `0` disables the set-aside.
    ///
    /// Same use as [`Self::with_drain_concurrency`]: a caller that wants a
    /// different containment budget — or a test driving the set-aside — sets
    /// it here rather than in the process environment.
    pub fn with_poison_attempts(mut self, attempts: u32) -> Self {
        self.poison_attempts = Some(attempts);
        self
    }

    /// Test-only: the ownership verdict this drain treats as already verified,
    /// in place of the one it would compute at the top of the cycle.
    ///
    /// #2836's window is between that verdict and the append's own resolution
    /// of the index NAME to a table: the verdict is a fact about a moment that
    /// has already passed, and a `DELETE` + `POST` of the same id inside the
    /// window hands the append a different table. A test pins the window by
    /// capturing the real verdict, recreating the index, and draining under the
    /// captured one — everything after the verdict (per-segment identity gate,
    /// claim, read, concat, append, release) is the shipped path.
    ///
    /// The same shape as
    /// [`IcebergContext::read_pending_delete_tasks_for_test`], and for the same
    /// reason: threads and a barrier cannot pin the window deterministically,
    /// and no production caller may choose its own verdict.
    #[doc(hidden)]
    pub fn with_verified_owner_for_test(mut self, table_uuid: &str) -> Self {
        self.verified_owner_for_test = Some(table_uuid.to_string());
        self
    }

    /// Test-only: fail this many filesystem commit attempts before calling the
    /// real commit path. The injected error is deliberately untyped so tests
    /// can prove ordinary transient failures retain same-cycle retry behavior.
    #[doc(hidden)]
    pub fn with_transient_fs_commit_failures_for_test(mut self, failures: usize) -> Self {
        self.transient_fs_commit_failures_for_test =
            Some(Arc::new(std::sync::atomic::AtomicUsize::new(failures)));
        self
    }

    /// Phase 4.13h: scan `processing/` (top-level + every per-tenant
    /// subdir) for files left over from a prior compactor process
    /// that died mid-cycle, and move them to `orphans/` for ops to
    /// inspect. Each move increments
    /// `siglake_compactor_orphans_quarantined_total` so operators can
    /// alert on accumulation.
    ///
    /// Why quarantine + not retry: a segment in `processing/` may
    /// or may not be in Iceberg already (the `commit_claimed →
    /// finish_segment` race surfaced in round-12 is ambiguous on
    /// EFS), and re-committing risks Iceberg-side duplicates.
    /// The catalog-claim path is a no-op here — it doesn't use
    /// `processing/`, the SQL `wal_segments` table tracks claims
    /// instead.
    fn quarantine_processing_orphans(&self) {
        let try_one = |dir: &Path, tenant: Option<&str>| match recover_orphaned_processing(dir) {
            Ok(0) => {}
            Ok(n) => {
                tracing::warn!(
                    orphans = n,
                    dir = %dir.display(),
                    tenant = tenant.unwrap_or("default"),
                    "compactor quarantined orphaned processing/ segments (round-12 bug)"
                );
                metrics::counter!(
                    "siglake_compactor_orphans_quarantined_total",
                    "tenant" => tenant.unwrap_or("default").to_string()
                )
                .increment(n as u64);
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    dir = %dir.display(),
                    "orphan-processing recovery failed"
                );
            }
        };
        // Top-level (legacy single-tenant layout).
        try_one(&self.wal_dir, None);
        // Per-tenant layout (4.10c+).
        if let Ok(tenants) = list_tenant_dirs(&self.wal_dir) {
            for (tenant, dir) in tenants {
                try_one(&dir, Some(&tenant));
                if let Ok(indexes) = list_index_dirs(&dir) {
                    for (_index, index_dir) in indexes {
                        try_one(&index_dir, Some(&tenant));
                    }
                }
            }
        }
    }

    /// #81: automatic disposition of quarantined orphans, replacing the
    /// "for ops to inspect" dead-end with the proof the Phase-4 round applied
    /// manually (twice, exactly). For each `orphans/*.arrow` against the
    /// target table's CUMULATIVE consumed-segment set:
    ///
    /// - basename **present** → its rows are provably in Iceberg → delete the
    ///   file (re-committing would duplicate). Presence is definitive even
    ///   under snapshot expiry.
    /// - basename **absent** AND the retained history covers the segment's
    ///   life (oldest retained snapshot predates the seal mtime by a skew
    ///   margin, or the table has no snapshots at all) → provably never
    ///   committed → rename back into `sealed/`; the normal claim path
    ///   re-commits it this same cycle. At-most-once becomes at-least-once +
    ///   dedup-by-proof.
    /// - basename absent but expiry may have dropped the consuming snapshot
    ///   (segment older than the retained floor) → ambiguous → hold for ops,
    ///   exactly the pre-#81 behavior.
    ///
    /// Sound because the FS-rename path is single-compactor per WAL dir and
    /// orphans predate this process: no concurrent commit can be adding the
    /// segment while we reason about it. The catalog-claim path never uses
    /// `processing/`, so its dirs are always empty here.
    async fn dispose_orphans_at(
        &self,
        dir: &Path,
        tenant_label: &str,
        ice: &Arc<IcebergContext>,
        target: &CommitTarget,
    ) -> Result<()> {
        /// Absorbs clock skew between the sealing writer's filesystem mtime
        /// and the committing process's snapshot timestamps (both NTP-synced
        /// in real deployments; skew is ms). Deliberately small: a large
        /// margin would permanently hold orphans sealed shortly after a young
        /// table's first commit (the floor only moves forward via expiry).
        const HISTORY_SKEW_MARGIN_MS: i64 = 5_000;
        let orphans = list_orphaned(dir).context("listing orphaned segments")?;
        if orphans.is_empty() {
            return Ok(());
        }
        let (consumed, floor_ms) = ice
            .consumed_segments_and_history_floor(target.index_label())
            .await
            .context("loading consumed-segment history")?;
        let sealed_dir = dir.join(siglake_wal::SEALED_DIR);
        std::fs::create_dir_all(&sealed_dir)
            .with_context(|| format!("creating {}", sealed_dir.display()))?;
        let (mut deleted, mut requeued, mut held) = (0u64, 0u64, 0u64);
        for path in orphans {
            let Some(name) = path.file_name().and_then(|f| f.to_str()).map(String::from) else {
                continue;
            };
            if consumed.contains(&name) {
                std::fs::remove_file(&path)
                    .with_context(|| format!("deleting committed orphan {}", path.display()))?;
                deleted += 1;
                continue;
            }
            let sealed_at_ms = path
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64);
            let covered = match (floor_ms, sealed_at_ms) {
                (None, _) => true, // no snapshots: nothing was ever committed
                (Some(floor), Some(sealed_at)) => floor + HISTORY_SKEW_MARGIN_MS <= sealed_at,
                (Some(_), None) => false, // unreadable mtime: stay ambiguous
            };
            if covered {
                let dest = sealed_dir.join(&name);
                if dest.exists() {
                    held += 1; // never clobber a live sealed segment
                    continue;
                }
                std::fs::rename(&path, &dest)
                    .with_context(|| format!("requeue orphan {} -> sealed/", path.display()))?;
                requeued += 1;
            } else {
                held += 1;
            }
        }
        for (action, n) in [("deleted", deleted), ("requeued", requeued)] {
            if n > 0 {
                metrics::counter!(
                    "siglake_compactor_orphans_disposed_total",
                    "action" => action,
                    "tenant" => tenant_label.to_string()
                )
                .increment(n);
            }
        }
        metrics::gauge!(
            "siglake_compactor_orphans_held",
            "tenant" => tenant_label.to_string()
        )
        .set(held as f64);
        if deleted + requeued + held > 0 {
            tracing::info!(
                deleted,
                requeued,
                held,
                dir = %dir.display(),
                table = target.index_label(),
                "orphan auto-disposition"
            );
        }
        Ok(())
    }

    /// #3143: charge a failed drain batch to the individual segments that
    /// could not be read, and set aside the ones that have now failed
    /// [`poison_attempts`] consecutive reads. Returns what stays in the batch,
    /// for the caller to release back to `sealed/`, and how many segments this
    /// call set aside.
    ///
    /// A failure that names no segment — a catalog conflict, an append
    /// refusal, a store timeout — charges nothing and the whole batch is
    /// released, exactly as before. So does a segment that simply shared a
    /// batch with an unreadable one.
    ///
    /// A set-aside that itself fails leaves the segment in the batch: it goes
    /// back to `sealed/` and the next cycle tries again, which is the
    /// pre-#3143 behaviour and the right one while the volume is the thing
    /// that is unwell.
    fn set_aside_unreadable(
        &self,
        claimed: Vec<PathBuf>,
        err: &anyhow::Error,
        tenant_label: &str,
    ) -> (Vec<PathBuf>, usize) {
        let Some(unreadable) = err.downcast_ref::<UnreadableSegments>() else {
            return (claimed, 0);
        };
        let limit = self.poison_attempts.unwrap_or_else(poison_attempts);
        let mut set_aside: HashSet<PathBuf> = HashSet::new();
        for segment in &unreadable.segments {
            let Some(name) = segment.path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let attempts = {
                let mut ledger = self
                    .segment_read_failures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let entry = ledger.entry(name.to_string()).or_insert(0);
                *entry = entry.saturating_add(1);
                *entry
            };
            if limit == 0 || attempts < limit {
                tracing::warn!(
                    path = %segment.path.display(),
                    error = %segment.reason,
                    attempts,
                    limit,
                    tenant = tenant_label,
                    "could not read a claimed WAL segment; releasing it for retry"
                );
                continue;
            }
            match quarantine_poison_segment(&segment.path, &segment.reason, attempts) {
                Ok(dest) => {
                    tracing::error!(
                        path = %segment.path.display(),
                        held = %dest.display(),
                        error = %segment.reason,
                        attempts,
                        tenant = tenant_label,
                        "segment SET ASIDE: it failed to read on every attempt, so it is held \
                         under poison/ instead of failing every batch it joins. Its rows are \
                         durable and unqueryable; move the file back into sealed/ to retry it"
                    );
                    metrics::counter!(
                        "siglake_compactor_segments_poisoned_total",
                        "tenant" => tenant_label.to_string()
                    )
                    .increment(1);
                    self.segment_read_failures
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(name);
                    set_aside.insert(segment.path.clone());
                }
                Err(e) => tracing::error!(
                    path = %segment.path.display(),
                    error = %e,
                    tenant = tenant_label,
                    "could not set an unreadable segment aside; releasing it for retry"
                ),
            }
        }
        if set_aside.is_empty() {
            return (claimed, 0);
        }
        let kept = claimed
            .into_iter()
            .filter(|c| !set_aside.contains(c))
            .collect();
        (kept, set_aside.len())
    }

    /// Drop a committed batch's read-failure charges. The ledger counts
    /// CONSECUTIVE failures, so a segment that reads once is owed nothing for
    /// the cycles it failed before.
    fn forget_read_failures(&self, claimed: &[PathBuf]) {
        let mut ledger = self
            .segment_read_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ledger.is_empty() {
            return;
        }
        for path in claimed {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                ledger.remove(name);
            }
        }
    }

    /// Requeue abandoned claims, but only those PROVEN not to have committed.
    ///
    /// A drain can die after its Iceberg commit lands and before
    /// `mark_committed_batch`, leaving the row in 'processing' while its rows are
    /// already durable. Blindly flipping such a row back to 'sealed' hands it to
    /// another drain and commits the segment a SECOND time -- silent duplication,
    /// no error raised and no counter that reveals it.
    ///
    /// This is the same hazard #81 solved for filesystem orphans. The catalog
    /// path reads the durable table property first and the retained snapshot
    /// union as its rolling-upgrade fallback. Presence means committed; absence
    /// means never committed only when one source covers the claim.
    async fn reclaim_with_proof(&self, cfg: &CatalogClaimConfig) -> Result<()> {
        self.reclaim_with_proof_older_than(cfg, claim_reclaim_max_age())
            .await
    }

    async fn reclaim_with_proof_older_than(
        &self,
        cfg: &CatalogClaimConfig,
        max_age: Duration,
    ) -> Result<()> {
        let abandoned = cfg.claim.abandoned_claims(max_age, 1024).await?;
        if abandoned.is_empty() {
            return Ok(());
        }
        /// Same role as the constant in `dispose_orphans_at`: absorbs ms-scale
        /// clock skew between the claiming process and the committing
        /// process's snapshot timestamps.
        const HISTORY_SKEW_MARGIN_MS: i64 = 5_000;
        // One proof read per distinct target, not per segment.
        let mut proofs: std::collections::HashMap<String, Option<ReclaimProofSources>> =
            std::collections::HashMap::new();
        let (mut committed, mut requeue) = (Vec::new(), Vec::new());
        let mut unprovable = 0u64;
        // #2889: a reclaim decision is evidence about the generation the proof
        // was READ from, so the watermark it advances records that uuid.
        let mut provenance = ProofProvenance::default();
        for c in &abandoned {
            let key = format!("{}/{}", c.tenant, c.index_id);
            if !proofs.contains_key(&key) {
                let proof = self.reclaim_proof_for(&c.tenant, &c.index_id).await;
                proofs.insert(key.clone(), proof);
            }
            let Some(proof) = &proofs[&key] else {
                unprovable += 1;
                requeue.push(c.id.clone());
                continue;
            };
            let basename = format!("{}.arrow", c.id);
            let durable_contains = match &proof.durable {
                ConsumedProofRead::Valid(durable) => {
                    durable.contains(&basename) || durable.contains(&c.id)
                }
                ConsumedProofRead::Absent | ConsumedProofRead::Corrupt(_) => false,
            };
            if durable_contains
                || proof.retained.contains(&basename)
                || proof.retained.contains(&c.id)
            {
                committed.push(c.id.clone());
                provenance.record(&c.tenant, &c.index_id, &proof.table_uuid);
                continue;
            }
            // ABSENT from the consumed set. That means "never committed" only
            // if the proof actually COVERS this segment. The set is the union
            // of `siglake.consumed_segments` over the snapshots still in the
            // current metadata, and snapshot expiry is default-ON at
            // retain_last: 100 every 60s — while reclaim fires at 900s. On any
            // real fleet 100 commits is far less than 900 seconds of history,
            // and reclustering commits consume snapshot slots without carrying
            // the property, shortening it further. So the proving snapshot can
            // be gone: the segment WAS committed and looks like it never was.
            //
            // A commit of this segment can only have happened at or after it
            // was claimed, so the proof covers it exactly when the retained
            // history reaches back past the claim.
            let durable_floor = match &proof.durable {
                ConsumedProofRead::Valid(durable) => {
                    Some(durable.coverage_start_ms.unwrap_or(i64::MIN))
                }
                // Absent/corrupt durable state supplies no negative evidence.
                // The retained summaries remain an independent rollout source.
                ConsumedProofRead::Absent | ConsumedProofRead::Corrupt(_) => None,
            };
            let combined_floor = match (durable_floor, proof.retained_history_floor_ms) {
                (Some(durable), Some(retained)) => Some(durable.min(retained)),
                (Some(durable), None) => Some(durable),
                (None, retained) => retained,
            };
            let covered = match (combined_floor, c.claimed_at.timestamp_millis()) {
                // No snapshots at all: nothing was ever committed, so absence
                // is conclusive only when no corrupt durable state exists.
                (None, _) => matches!(proof.durable, ConsumedProofRead::Absent),
                (Some(floor), claimed_at) => {
                    floor.saturating_add(HISTORY_SKEW_MARGIN_MS) <= claimed_at
                }
            };
            if !covered {
                unprovable += 1;
            }
            // Requeue either way. This follows the existing fail-closed
            // disposition for an unreadable table: prefer duplication to
            // loss, because duplicate rows can at least be detected afterwards
            // and rows never written cannot be recovered. What changes is that
            // the unprovable case is now COUNTED and named rather than being
            // silently identical to a confident requeue — an operator seeing
            // this counter move should widen snapshot retention, since the
            // remedy is history, not a different guess.
            requeue.push(c.id.clone());
        }
        let m = cfg
            .claim
            .mark_reclaimed_committed(&committed, &provenance)
            .await?;
        let r = cfg.claim.requeue_claims(&requeue).await?;
        metrics::counter!("siglake_compactor_reclaim_unprovable_total").increment(unprovable);
        if m > 0 || r > 0 {
            tracing::warn!(
                already_committed = m,
                requeued = r,
                unprovable,
                "reclaimed abandoned claims by consumed-set proof"
            );
        }
        if unprovable > 0 {
            tracing::warn!(
                unprovable,
                "reclaim: the proving snapshots for these claims have already been expired, so \
                 they were requeued WITHOUT proof and may duplicate rows already committed. \
                 Inspect the durable consumed-proof state and, during a mixed-version rollout, \
                 keep snapshot retention above the reclaim window."
            );
        }
        Ok(())
    }

    /// Read the durable and retained proof sources from one table generation.
    /// An unreadable table is `None`, not an empty proof: the caller must count
    /// and fail closed rather than infer absence.
    async fn reclaim_proof_for(&self, tenant: &str, index_id: &str) -> Option<ReclaimProofSources> {
        let ice = if tenant == "default" {
            self.ice.clone()
        } else {
            match self.ice.for_namespace(&format!("tenant_{tenant}")).await {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    tracing::warn!(tenant, error = %e, "reclaim: namespace unreadable; \
                        treating claims as un-committed (may re-drain)");
                    return None;
                }
            }
        };
        let table_name = if index_id.is_empty() {
            CommitTarget::Events.index_label()
        } else {
            index_id
        };
        match ice.reclaim_proof_sources(table_name).await {
            Ok(proof) => {
                if let ConsumedProofRead::Corrupt(error) = &proof.durable {
                    tracing::warn!(
                        tenant,
                        index_id,
                        error,
                        "reclaim: durable consumed proof is \
                        corrupt; only retained-history evidence remains usable"
                    );
                }
                Some(proof)
            }
            Err(e) => {
                tracing::warn!(tenant, index_id, error = %e, "reclaim: table unreadable; \
                    treating claims as un-committed (may re-drain)");
                None
            }
        }
    }

    /// Retention for drained WAL segments: delete the mirror OBJECT first, then
    /// the catalog row.
    ///
    /// The order is the whole point. A committed row is the record that says
    /// "this segment was already drained". Delete it while the object still
    /// exists and any path that registers by listing the mirror -- the
    /// full-prefix recovery scan -- sees an unknown object, registers it, and
    /// the segment is drained a SECOND time. Nothing errors; the row count is
    /// simply wrong forever.
    ///
    /// Crashing between the two steps leaves a committed row whose object is
    /// gone. That is inert (no path re-reads it) and the next pass removes it.
    /// The reverse order has no such benign failure.
    async fn run_retention_once(&self) -> usize {
        self.run_retention_with(committed_retention()).await
    }

    /// [`Self::run_retention_once`] against an already-resolved window, so the
    /// `SIGLAKE_COMMITTED_RETENTION_SECS=0` opt-out can be driven through
    /// [`committed_retention_from`] in a test instead of the process
    /// environment. `None` deletes nothing, which is what `0` means.
    async fn run_retention_with(&self, max_age: Option<Duration>) -> usize {
        let Some(max_age) = max_age else {
            return 0;
        };
        self.run_retention_bounded(max_age, RETENTION_RUN_OBJECTS, RETENTION_PAGE_OBJECTS)
            .await
    }

    /// Drain eligible rows in small pages until the per-run object budget is
    /// exhausted or the oldest eligible page is empty.
    async fn run_retention_bounded(
        &self,
        max_age: Duration,
        object_budget: usize,
        page_objects: usize,
    ) -> usize {
        use futures::StreamExt as _;

        let Some(cfg) = self.retention_cfg() else {
            return 0;
        };
        let page_objects = page_objects.max(1);
        let mut examined = 0usize;
        let mut purged = 0usize;
        while examined < object_budget {
            let page_limit = page_objects.min(object_budget - examined);
            let rows = match cfg.claim.purgeable_committed(max_age, page_limit).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(error = %e, "retention: could not list purgeable rows");
                    break;
                }
            };
            if rows.is_empty() {
                break;
            }
            let selected = rows.len();
            examined += selected;

            // Every successful delete completes before any of the corresponding
            // catalog rows are handed to purge_committed_ids. Missing objects
            // count as success: an earlier interrupted pass commonly leaves
            // exactly that safe, inert state.
            let deletable: Vec<String> =
                futures::stream::iter(rows.into_iter().map(|(id, key)| {
                    let store = cfg.store.clone();
                    async move {
                        match store.delete(&key).await {
                            Ok(()) => Some(id),
                            Err(e) => {
                                // Keep the row. Dropping it now is the duplication
                                // hazard mirror reconciliation exists to prevent.
                                tracing::warn!(id = %id, key = %key, error = %e,
                                "retention: mirror delete failed; KEEPING the catalog row");
                                metrics::counter!(
                                    "siglake_compactor_retention_object_delete_errors_total"
                                )
                                .increment(1);
                                None
                            }
                        }
                    }
                }))
                .buffer_unordered(RETENTION_OBJECT_DELETE_CONCURRENCY)
                .filter_map(async |id| id)
                .collect()
                .await;

            if deletable.is_empty() {
                // Do not spend the rest of this run retrying the same failed
                // oldest page. The next scheduled pass retries it safely.
                break;
            }
            match cfg.claim.purge_committed_ids(&deletable).await {
                Ok(n) => {
                    purged += n as usize;
                    metrics::counter!("siglake_compactor_retention_purged_total").increment(n);
                }
                Err(e) => {
                    tracing::warn!(error = %e,
                        "retention: row purge failed (objects already deleted)");
                    break;
                }
            }
            if selected < page_limit {
                break;
            }
        }
        if purged > 0 {
            tracing::info!(
                purged,
                examined,
                budget = object_budget,
                "retention: mirror objects and catalog rows removed"
            );
        }
        purged
    }

    /// Try to become the single owner of a whole-warehouse maintenance sweep.
    ///
    /// Without this, every combined-role node runs every sweep: the 2026-08-14
    /// rounds had 8-16 drains each independently reloading metadata for every
    /// table in the warehouse, which is N times the catalog load for exactly one
    /// sweep's worth of benefit.
    ///
    /// Fails OPEN when there is no catalog (single-process and filesystem
    /// deployments have no one to contend with, and silently disabling their
    /// maintenance would be far worse than duplicating it). Iceberg's CAS
    /// remains the correctness backstop either way -- this is about wasted work,
    /// not about safety.
    async fn maintenance_lease(&self, purpose: &str) -> bool {
        let Some(cfg) = self.retention_cfg() else {
            return true;
        };
        let ttl = maintenance_lease_ttl();
        match cfg
            .claim
            .acquire_table_lease(&format!("__maintenance__{purpose}"), purpose, ttl)
            .await
        {
            Ok(true) => true,
            Ok(false) => {
                metrics::counter!("siglake_compactor_maintenance_lease_denied_total",
                    "purpose" => purpose.to_string())
                .increment(1);
                false
            }
            // A lease error must not stop maintenance forever; degrade to the old
            // everyone-runs-it behaviour rather than silently ceasing to expire
            // snapshots.
            Err(e) => {
                tracing::warn!(purpose, error = %e, "maintenance lease failed; running unelected");
                metrics::counter!("siglake_compactor_maintenance_lease_errors_total",
                    "purpose" => purpose.to_string())
                .increment(1);
                true
            }
        }
    }

    /// Acquire the correctness-critical exclusion shared by mirror sync and
    /// committed retention.
    ///
    /// This is deliberately separate from their long-lived election leases.
    /// Sharing an election lease directly would let the default 60-second
    /// mirror sync renew a 300-second lease forever, starving retention in a
    /// split drain/maintenance fleet. Unlike election, failure is closed: a
    /// skipped repair or retention pass retries safely, while running without
    /// exclusion can recreate a purged row from a stale listing.
    async fn acquire_mirror_reconciliation_lease(&self) -> bool {
        let Some(cfg) = self.retention_cfg() else {
            return true;
        };
        match cfg
            .claim
            .acquire_table_lease(
                MIRROR_RECONCILIATION_LEASE_ID,
                MIRROR_RECONCILIATION_LEASE_PURPOSE,
                maintenance_lease_ttl(),
            )
            .await
        {
            Ok(held) => held,
            Err(e) => {
                tracing::warn!(
                    purpose = MIRROR_RECONCILIATION_LEASE_PURPOSE,
                    error = %e,
                    "mirror reconciliation lease failed; skipping this pass"
                );
                metrics::counter!(
                    "siglake_compactor_maintenance_lease_errors_total",
                    "purpose" => MIRROR_RECONCILIATION_LEASE_PURPOSE
                )
                .increment(1);
                false
            }
        }
    }

    /// Release the operation-scoped mirror/retention exclusion. A failed
    /// release is safe: the lease TTL delays the next pass rather than allowing
    /// the two operations to overlap.
    async fn release_mirror_reconciliation_lease(&self) {
        let Some(cfg) = self.retention_cfg() else {
            return;
        };
        if let Err(e) = cfg
            .claim
            .release_table_lease(MIRROR_RECONCILIATION_LEASE_ID)
            .await
        {
            tracing::warn!(
                purpose = MIRROR_RECONCILIATION_LEASE_PURPOSE,
                error = %e,
                "mirror reconciliation lease release failed"
            );
            metrics::counter!(
                "siglake_compactor_maintenance_lease_errors_total",
                "purpose" => MIRROR_RECONCILIATION_LEASE_PURPOSE
            )
            .increment(1);
        }
    }

    /// Run a mirror/catalog mutation while renewing its shared exclusion.
    ///
    /// A full-prefix reconciliation can outlive the nominal maintenance TTL.
    /// Renewing at one third of the TTL keeps retention excluded for the whole
    /// listing. If renewal is lost, dropping `operation` is safe for both
    /// callers: mirror registration is idempotent, while object-first retention
    /// leaves an inert committed row when interrupted before its row purge.
    async fn with_mirror_reconciliation_lease<F, T>(&self, operation: F) -> Option<T>
    where
        F: std::future::Future<Output = T>,
    {
        if !self.acquire_mirror_reconciliation_lease().await {
            return None;
        }
        let renew_after = maintenance_lease_ttl() / 3;
        tokio::pin!(operation);
        loop {
            tokio::select! {
                result = &mut operation => {
                    self.release_mirror_reconciliation_lease().await;
                    return Some(result);
                }
                () = sleep(renew_after) => {
                    if !self.acquire_mirror_reconciliation_lease().await {
                        tracing::warn!(
                            purpose = MIRROR_RECONCILIATION_LEASE_PURPOSE,
                            "mirror reconciliation lease lost; cancelling this pass"
                        );
                        self.release_mirror_reconciliation_lease().await;
                        return None;
                    }
                }
            }
        }
    }

    /// Restrict this process to one half of the work. See [`CompactorRole`].
    pub fn with_role(mut self, role: CompactorRole) -> Self {
        self.role = role;
        self
    }

    /// Switch to the catalog-claim coordination model.
    ///
    /// The compactor stops scanning `<wal>/sealed/` and instead:
    ///
    /// 1. Lists `<prefix>/<filename>` objects in the WAL mirror bucket
    ///    and `INSERT IGNORE`s each into `wal_segments` (this is the
    ///    "sync from object store" pass — handles the case where the
    ///    ingester uploaded but crashed before registering).
    /// 2. Atomically claims a batch via [`SqlSegmentClaim::try_claim`].
    /// 3. Fetches each claimed segment's bytes from the object store,
    ///    decodes via [`read_segment_from_bytes`], and runs the existing
    ///    sort + Iceberg commit path.
    /// 4. On success: `mark_committed` for every claimed segment.
    ///    On failure: `release` so another pod (or this pod's next
    ///    cycle) can retry.
    pub fn with_catalog_claim(mut self, cfg: CatalogClaimConfig) -> Self {
        // PHASE 0: say which settings this path actually reads.
        //
        // `run_once` branches straight to `run_once_catalog` when a catalog is
        // configured, so SIGLAKE_DRAIN_CONCURRENCY, SIGLAKE_DRAIN_INFLIGHT_
        // BUDGET_MB and SIGLAKE_COMPACTOR_FS_CLAIM_MAX_{BYTES,SEGMENTS} are
        // filesystem-path only and have NO effect here. Every 2026-08 fleet
        // round varied exactly those and reported the results as drain levers;
        // the catalog claim size was hardcoded at --catalog-claim-batch 256
        // throughout. An inert knob that looks live invalidates a whole matrix,
        // so the effective config is now stated at startup and exported.
        let fs_only: Vec<&str> = [
            "SIGLAKE_DRAIN_CONCURRENCY",
            "SIGLAKE_DRAIN_INFLIGHT_BUDGET_MB",
            "SIGLAKE_COMPACTOR_FS_CLAIM_MAX_BYTES",
            "SIGLAKE_COMPACTOR_FS_CLAIM_MAX_SEGMENTS",
        ]
        .into_iter()
        .filter(|k| std::env::var(k).is_ok())
        .collect();
        if !fs_only.is_empty() {
            tracing::warn!(
                ignored = ?fs_only,
                "catalog-claim drain: these settings are FILESYSTEM-PATH ONLY and have no effect \
                 here; catalog claim size comes from --catalog-claim-batch"
            );
        }
        tracing::info!(
            catalog_claim_batch = cfg.batch_size,
            mirror_sync = ?mirror_sync_interval(),
            reclustering = self.recluster.is_some(),
            snapshot_expiry = self.expire.is_some(),
            "catalog-claim drain effective config"
        );
        metrics::gauge!("siglake_compactor_effective_catalog_claim_batch")
            .set(cfg.batch_size as f64);
        metrics::gauge!("siglake_compactor_effective_fs_only_settings_present")
            .set(fs_only.len() as f64);
        metrics::gauge!("siglake_compactor_effective_reclustering")
            .set(if self.recluster.is_some() { 1.0 } else { 0.0 });
        metrics::gauge!("siglake_compactor_effective_mirror_sync").set(
            if mirror_sync_interval().is_some() {
                1.0
            } else {
                0.0
            },
        );
        self.catalog = Some(cfg);
        self
    }

    /// Attach the claim store and the mirror operator for LEDGER-ONLY mirror
    /// reclamation, without the claim path (#4913, `docs/DESIGN_wal_mirror_reclamation.md`
    /// Option C).
    ///
    /// The filesystem drain commits out of local `sealed/` and never reads the
    /// mirror, so nothing deleted the objects the ingester uploads — the prefix
    /// grew for as long as the cluster ingested, and so did one `wal_segments`
    /// row per object. This mode marks the rows the ingester already wrote for
    /// the segments this drain committed, and lets the existing retention pass
    /// delete the object and then the row.
    ///
    /// What stays OFF is the point: no `try_claim`, no mirror-to-catalog
    /// reconciliation sweep, no abandoned-claim reclaim. This mode must never
    /// register an object it did not commit, because registering by listing is
    /// the only thing that could turn an unattributable object into a delete.
    ///
    /// Ignored when a catalog claim is already configured: that drain purges
    /// what it claimed, which is stronger evidence than this path has.
    pub fn with_mirror_ledger(mut self, cfg: CatalogClaimConfig) -> Self {
        if self.catalog.is_some() {
            tracing::warn!(
                "mirror ledger reclamation ignored: the catalog-claim drain already \
                 retains what it commits"
            );
            return self;
        }
        tracing::info!(
            mirror_prefix = %cfg.prefix,
            committed_retention = ?committed_retention(),
            "ledger-only mirror reclamation enabled (no claiming, no mirror listing)"
        );
        self.mirror_ledger = Some(cfg);
        self
    }

    /// The claim store and mirror operator committed retention deletes
    /// through: the claim drain's own config, or the filesystem drain's
    /// ledger-only one.
    fn retention_cfg(&self) -> Option<&CatalogClaimConfig> {
        self.catalog.as_ref().or(self.mirror_ledger.as_ref())
    }

    /// Enable periodic tier-2 re-clustering of the events table (off by default).
    /// The run loop runs a bounded [`IcebergContext::recluster_pass`] no more
    /// often than `cfg.interval`, only when the ingest cycle is idle.
    pub fn with_reclustering(mut self, cfg: ReclusterConfig) -> Self {
        self.recluster = Some(cfg);
        self
    }

    /// Enable periodic snapshot-metadata expiry of the events table (off by
    /// default). The run loop drops all but the most-recent
    /// `cfg.retain_last` snapshots no more often than `cfg.interval`, only
    /// when the ingest cycle is idle. See [`ExpireConfig`].
    pub fn with_snapshot_expiry(mut self, cfg: ExpireConfig) -> Self {
        self.expire = Some(cfg);
        self
    }

    /// Enable per-index delete-task execution during idle cycles. Off by
    /// default; the caller opts in explicitly (CLI env gate).
    pub fn with_delete_tasks(mut self, enabled: bool) -> Self {
        self.delete_tasks_enabled = enabled;
        self
    }

    /// Enable commit-accumulation batching (off by default). See
    /// [`CommitBatchPolicy`]. When set, the run loop defers a commit
    /// cycle until the sealed queue reaches `target_bytes` or its
    /// oldest segment reaches `max_age`, amortizing the fixed
    /// per-commit catalog cost over many more rows.
    pub fn with_commit_batching(mut self, policy: CommitBatchPolicy) -> Self {
        self.commit_batch = Some(policy);
        self
    }

    /// Whether the commit gate (if configured) says to hold off this
    /// cycle. `bytes`/`oldest_age` describe the currently-pending
    /// sealed queue. No policy ⇒ always ready (legacy behavior).
    /// An empty queue (`bytes == 0`) is never "deferred" — the caller
    /// already short-circuits the empty case.
    fn commit_deferred(&self, bytes: u64, oldest_age: Duration) -> bool {
        match self.commit_batch {
            Some(p) => bytes > 0 && !p.ready(bytes, oldest_age),
            None => false,
        }
    }

    /// Run a single bounded re-clustering pass over **every** managed index (the
    /// built-in `events` table plus every user-created index). Safe to call
    /// concurrently from multiple pods: the Iceberg overwrite's live-file guard
    /// means at most one racing pass commits and the rest fail and retry without
    /// corrupting data; a failure on one index is skipped so it can't starve the
    /// others. Returns the number of files removed across all healed partitions
    /// (0 if nothing needed healing or re-clustering is off).
    /// Best-effort count of sealed WAL segments awaiting drain across the legacy
    /// root + every tenant subdir (+ their per-index dirs). Cheap directory
    /// listing — the run loop uses it to let the drain preempt maintenance under
    /// backlog. The catalog-claim path keeps its segments in SQL (not `sealed/`),
    /// so this reads ~0 there; its backpressure gate is a later slice (peek the
    /// `wal_segments` queue). See [`max_sealed_for_recluster`].
    fn pending_sealed_total(&self) -> usize {
        pending_sealed_count(&self.wal_dir)
    }

    pub async fn run_recluster_once(&self) -> Result<usize> {
        self.run_recluster_once_shaped(&LeveledPassOptions::default())
            .await
    }

    /// The acknowledgement boundary this cycle's append may carry, given the
    /// incarnation the group's ownership check verified (#2889).
    ///
    /// The stored boundary and the append target are both reached by NAME, so
    /// they agree only when their incarnations do. `(None, None)` is the events
    /// table and an index the ownership check had no opinion about — the same
    /// "no verified identity" that leaves the append itself unfenced. Anything
    /// else that disagrees drops the boundary for this append; the terminal
    /// claim that follows re-establishes it under the verified uuid, so the
    /// next cycle carries it.
    fn watermark_for_incarnation(
        watermark: Option<&ConsumedProofWatermark>,
        verified_uuid: Option<&str>,
    ) -> Option<i64> {
        watermark
            .filter(|w| w.table_uuid.as_deref() == verified_uuid)
            .map(|w| w.acknowledged_through_ms)
    }

    /// Create the dashboard's zero-valued series once an index id is known.
    fn preregister_proof_watermark_skips(index_id: &str) {
        if index_id.is_empty() {
            return;
        }
        for reason in PROOF_WATERMARK_SKIPPED_REASONS {
            metrics::counter!(
                "siglake_compactor_proof_watermark_skipped_total",
                "reason" => *reason,
                "index" => index_id.to_string()
            )
            .increment(0);
        }
    }

    /// Match the exact entries in the current durable property against
    /// terminal catalog rows. Querying from the property side bounds the SQL
    /// work even when committed-row retention is disabled and the catalog has
    /// accumulated a long history.
    async fn terminal_consumed_proof_entries(
        ice: &Arc<IcebergContext>,
        claim: &SqlSegmentClaim,
        tenant: &str,
        index_id: &str,
        table_name: &str,
    ) -> Result<Vec<String>> {
        let ConsumedProofRead::Valid(proof) = ice.durable_consumed_proof(table_name).await? else {
            return Ok(Vec::new());
        };
        let mut property_ids_by_catalog_id: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for entry in proof.entries() {
            let catalog_id = entry
                .segment_id
                .strip_suffix(".arrow")
                .unwrap_or(&entry.segment_id)
                .to_string();
            property_ids_by_catalog_id
                .entry(catalog_id)
                .or_default()
                .push(entry.segment_id);
        }
        let candidates: Vec<String> = property_ids_by_catalog_id.keys().cloned().collect();
        let committed = claim
            .committed_segment_ids(tenant, index_id, &candidates)
            .await?;
        Ok(committed
            .into_iter()
            .filter_map(|id| property_ids_by_catalog_id.remove(&id))
            .flatten()
            .collect())
    }

    async fn compact_acknowledged_proofs(&self, ice: &Arc<IcebergContext>) -> Result<usize> {
        let Some(catalog) = self.catalog.as_ref() else {
            return Ok(0);
        };
        let namespace = ice
            .namespace()
            .as_ref()
            .last()
            .map(String::as_str)
            .unwrap_or("siglake");
        let tenant = namespace.strip_prefix("tenant_").unwrap_or("default");
        let mut idents = vec![ice.events_table_ident().clone()];
        for index in ice.list_indexes().await? {
            let ident = ice.index_table_ident(&index.index_id);
            if !idents.contains(&ident) {
                idents.push(ident);
            }
        }
        let mut compacted = 0usize;
        for ident in idents {
            let index_id = if ident == *ice.events_table_ident() {
                ""
            } else {
                ident.name()
            };
            Self::preregister_proof_watermark_skips(index_id);
            let Some(watermark) = catalog
                .claim
                .consumed_proof_watermark(tenant, index_id)
                .await?
            else {
                continue;
            };
            // #2889: the watermark and the ident are both a NAME, and after a
            // `DELETE` + `POST` of an index id both still speak for the dropped
            // incarnation. An index watermark with no recorded incarnation (a
            // row written before #2889) cannot be shown to belong to the table
            // this name resolves to, so it is not applied — the next terminal
            // claim re-establishes it. The events table has no
            // recreate-under-one-name path, so its `None` is an identity, not a
            // gap.
            if !index_id.is_empty() && watermark.table_uuid.is_none() {
                metrics::counter!(
                    "siglake_compactor_proof_watermark_skipped_total",
                    "reason" => PROOF_WATERMARK_UNPROVED,
                    "index" => index_id.to_string()
                )
                .increment(1);
                continue;
            }
            let terminal_proof_entries = Self::terminal_consumed_proof_entries(
                ice,
                &catalog.claim,
                tenant,
                index_id,
                ident.name(),
            )
            .await?;
            match ice
                .compact_consumed_proof_pruning(
                    &ident,
                    watermark.acknowledged_through_ms,
                    &terminal_proof_entries,
                    watermark.table_uuid.as_deref(),
                )
                .await
            {
                Ok(did) => compacted += usize::from(did),
                // A recreation between the watermark read and this commit is
                // the window the fence exists for, and it is not an error in
                // the pass: the other targets' maintenance still runs, and the
                // replacement's proof is left exactly as it was.
                Err(err) if err.is::<ProofMaintenanceIncarnationMismatch>() => {
                    metrics::counter!(
                        "siglake_compactor_proof_watermark_skipped_total",
                        "reason" => PROOF_WATERMARK_INCARNATION_MISMATCH,
                        "index" => index_id.to_string()
                    )
                    .increment(1);
                    tracing::warn!(index = index_id, error = %err, "skipping consumed-proof maintenance");
                }
                Err(err) => return Err(err),
            }
        }
        Ok(compacted)
    }

    /// Run one re-clustering pass shaped by `opts` (leveled mode only; the flat
    /// pass ignores it). The throttled backpressure slot passes `allowed_levels
    /// = [0]` + `max_total_bins = 1` — a single cheap L0 merge that can't
    /// re-starve the drain; the per-tier cadence passes the currently-due levels.
    async fn run_recluster_once_shaped(&self, opts: &LeveledPassOptions) -> Result<usize> {
        let Some(cfg) = self.recluster.as_ref() else {
            return Ok(0);
        };
        self.compact_acknowledged_proofs(&self.ice).await?;
        tracing::debug!(?opts, "re-clustering pass starting");
        let pass_start = std::time::Instant::now();
        let policy = cfg.policy;
        // Heals each index's worst-fragmented slice, using that index's own bloom
        // columns (events keeps its WS-7 promoted Utf8 columns; user indexes derive
        // theirs from the doc-mapping tag fields). Leveled mode (A.4.2) runs the
        // bounded per-level pass; otherwise the legacy flat whole-partition pass.
        // ENCLOSING UNIT for compaction, the counterpart of the drain's
        // `cycle_duration`. `merge_stage_nanos_total` reports input/assemble/write
        // but there is nothing for those shares to be shares OF -- and since chunk
        // pipelining they no longer partition wall time anyway (a whole fetch is
        // attributed to `input` while it overlaps encode). Without the whole, any
        // "unexplained" compaction time is a guess, which is exactly how the drain
        // arc lost three rounds. Reuses the existing `pass_start` so the metric
        // and the debug line measure the same span rather than two nearly-equal
        // ones that could drift apart.
        let stats = match &cfg.levels {
            Some(levels) => self
                .ice
                .recluster_all_indexes_leveled(levels, policy, opts)
                .await
                .context("recluster_all_indexes_leveled")?,
            None => self
                .ice
                .recluster_all_indexes(policy)
                .await
                .context("recluster_all_indexes")?,
        };
        metrics::histogram!("siglake_compactor_recluster_pass_duration_seconds")
            .record(pass_start.elapsed().as_secs_f64());
        let files_removed: usize = stats.iter().map(|s| s.files_removed).sum();
        let files_added: usize = stats.iter().map(|s| s.files_added).sum();
        let rows: usize = stats.iter().map(|s| s.rows).sum();
        if files_removed > 0 {
            tracing::info!(
                partitions = stats.len(),
                files_removed,
                files_added,
                rows,
                "tier-2 re-clustering pass healed index layout"
            );
            metrics::counter!("siglake_compactor_recluster_files_removed_total")
                .increment(files_removed as u64);
            metrics::counter!("siglake_compactor_recluster_files_added_total")
                .increment(files_added as u64);
            metrics::counter!("siglake_compactor_recluster_passes_total").increment(1);
            // Convergence RATE, which is what predicts settle time. A pass that
            // removes 400 files and adds 100 is net -300; settle duration is
            // roughly remaining-excess / net-per-pass / passes-per-minute. Without
            // this, settle time can only be observed after the fact, never
            // projected while a round is still running.
            metrics::counter!("siglake_compactor_recluster_files_net_removed_total")
                .increment(files_removed.saturating_sub(files_added) as u64);
            metrics::histogram!("siglake_compactor_recluster_pass_net_files")
                .record(files_removed as f64 - files_added as f64);
        }
        // Passes that heal NOTHING still cost a full live-file walk per table.
        // They are invisible in `recluster_passes_total` (which only counts
        // productive passes), so a settle that is spinning without progress looks
        // identical to one that is not running at all.
        if files_removed == 0 {
            metrics::counter!("siglake_compactor_recluster_passes_noop_total").increment(1);
        }
        tracing::debug!(
            files_removed,
            secs = pass_start.elapsed().as_secs_f64(),
            "re-clustering pass finished"
        );
        // WS-7: once backfill rewrites have covered every live file, flip the
        // property that gates the query server's attr_get→column rewrite.
        // No-op (property compare on the cached entry) when already flipped
        // or no promotions are declared; a failed check never fails the pass.
        if let Err(e) = self.ice.ensure_promotion_backfill_property().await {
            tracing::warn!(error = ?e, "promotion backfill property check failed");
        }
        // WS-7 auto-promotion (env-gated, default OFF): sample the newest
        // files' attributes JSON and promote hot string keys. Everything
        // downstream composes: the property-driven write path materializes
        // on the next commit, the backfill selector above rewrites old
        // files, and the completion property re-gates the rewrite.
        let min_fraction = auto_promote_min_fraction();
        if min_fraction > 0.0 && auto_promote_due() {
            match self
                .ice
                .auto_promote_hot_keys(min_fraction, auto_promote_max_columns(), 4, 4096)
                .await
            {
                Ok(newly) if !newly.is_empty() => {
                    metrics::counter!("siglake_compactor_auto_promotions_total")
                        .increment(newly.len() as u64);
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = ?e, "auto-promotion sampling failed"),
            }
        }
        Ok(files_removed)
    }

    /// Absorb the group-count deltas commits have written since the last fold.
    ///
    /// Its own cycle step rather than part of the re-clustering pass, because
    /// the two have nothing to do with each other: a deployment that turns
    /// re-clustering off would otherwise never fold, and the delta backlog would
    /// grow without bound behind a knob that says nothing about aggregates.
    ///
    /// Never load-bearing — a reader folds what is outstanding itself — so a
    /// failure costs read amplification until the next cycle and nothing else.
    /// Inert unless the cardinality knob is raised above the inline ceiling.
    async fn run_agg_fold_once(&self, min_backlog: usize) {
        for ice in self.aggregate_contexts("group-count delta fold").await {
            let namespace = ice.namespace().to_string();
            match ice.fold_group_count_deltas(min_backlog).await {
                Ok(outcomes) => {
                    for (table, o) in outcomes {
                        metrics::counter!("siglake_group_count_deltas_absorbed_total")
                            .increment(o.folded as u64);
                        metrics::counter!("siglake_group_count_deltas_deleted_total")
                            .increment(o.deleted as u64);
                        tracing::debug!(
                            namespace,
                            table,
                            folded = o.folded,
                            deleted = o.deleted,
                            pruned = o.pruned,
                            rebuilt = o.rebuilt,
                            repair_markers_deleted = o.repair_markers_deleted,
                            "folded group-count deltas"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    namespace,
                    error = ?e,
                    "group-count delta fold failed"
                ),
            }
        }
    }

    /// Every namespace whose aggregate artifacts this compactor maintains: the
    /// base one plus each `tenant_*` namespace the catalog reports.
    ///
    /// `stage` names the caller in the warning, because a namespace that cannot
    /// be opened costs a different thing to each sweep and an operator reading
    /// the line needs to know which one skipped it.
    async fn aggregate_contexts(&self, stage: &str) -> Vec<Arc<IcebergContext>> {
        let mut contexts = vec![self.ice.clone()];
        match self.ice.catalog().list_namespaces(None).await {
            Ok(namespaces) => {
                for namespace in namespaces {
                    let Some(name) = namespace
                        .as_ref()
                        .as_slice()
                        .first()
                        .filter(|_| namespace.len() == 1)
                    else {
                        continue;
                    };
                    if !name.starts_with("tenant_") || &namespace == self.ice.namespace() {
                        continue;
                    }
                    match self.ice.for_namespace(name).await {
                        Ok(ice) => contexts.push(Arc::new(ice)),
                        Err(e) => tracing::warn!(
                            namespace = %namespace,
                            error = ?e,
                            stage,
                            "aggregate maintenance could not open tenant namespace"
                        ),
                    }
                }
            }
            Err(e) => tracing::warn!(
                error = ?e,
                stage,
                "aggregate maintenance could not enumerate tenant namespaces; \
                 visiting the base namespace only"
            ),
        }
        contexts
    }

    /// Census every maintained table's group-count aggregate for a shortfall
    /// `record_count` says is real, and rebuild at most `max_repairs` of them
    /// (#3000).
    ///
    /// Its own step for the same reason the fold is: only a durable lost-delta
    /// marker used to make anything rebuild, so an aggregate that is merely
    /// short — a table upgraded across #2919, a commit killed between its
    /// commit and its delta PUT — stayed short for the life of the table and
    /// every `GROUP BY` on it paid the exact per-file tiers. The census is
    /// cheap; the rebuild is one Tier-2 query per maintained column, which is
    /// why it is budgeted per pass and off unless asked for.
    ///
    /// `max_repairs == 0` is the census alone: the counter and the log still
    /// name the table, nothing reads the files. The budget is global across
    /// namespaces, so a fleet-wide first enable cannot turn one pass into a
    /// whole-warehouse Tier-2 scan.
    async fn run_agg_short_repair_once(&self, max_repairs: usize) {
        let mut budget = max_repairs;
        for ice in self.aggregate_contexts("short group-count repair").await {
            let namespace = ice.namespace().to_string();
            match ice.repair_short_group_count_aggregates(budget).await {
                Ok(outcomes) => {
                    for (table, outcome) in outcomes {
                        if matches!(
                            outcome,
                            ShortAggregateOutcome::Repaired { .. } | ShortAggregateOutcome::Failed
                        ) {
                            budget = budget.saturating_sub(1);
                        }
                        tracing::debug!(
                            namespace,
                            table,
                            outcome = ?outcome,
                            "short group-count aggregate census"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    namespace,
                    error = ?e,
                    "short group-count aggregate census failed"
                ),
            }
        }
    }

    /// Name every maintained table whose inline aggregate object cannot prove
    /// coverage of its current snapshot (#4674), across every namespace this
    /// compactor maintains.
    ///
    /// Read-only and metadata-only: one HEAD and one GET of
    /// `siglake-aggregates.json` per table, no rebuild. The rebuild is an
    /// operator's `siglake rebuild-time-aggregates`, and automating it is
    /// #4675 — so unlike the short-aggregate pass beside it, this one has no
    /// budget to spend and nothing to opt into.
    ///
    /// A table the pass no longer reaches has its reading zeroed. The gauge is
    /// per `(iceberg namespace, table)` and a series is never removed from a
    /// live process, so an index dropped while its object was unprovable would
    /// otherwise page until a restart.
    ///
    /// The pass counter is the alert's liveness arm. `siglake_inline_coverage_unproven`
    /// is a last-observation gauge: a pod that stops censusing — it lost the
    /// `agg_fold` lease, the census was switched off, the watchdog cut it —
    /// keeps serving its last reading forever, and for a `> 0` alert that is
    /// stale-BAD, a page for a table somebody else already repaired.
    /// `SiglakeInlineCoverageUnproven` pairs the gauge with
    /// `increase(siglake_inline_coverage_census_total[1h]) > 0` on the same pod,
    /// so a pod that stopped looking drops out of the alert instead of paging
    /// from a stale reading.
    async fn run_inline_coverage_census_once(&self) {
        let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
        for ice in self.aggregate_contexts("inline-coverage census").await {
            let namespace = ice.namespace().to_string();
            for (table, outcome) in ice.census_inline_coverage().await {
                tracing::debug!(
                    namespace,
                    table,
                    outcome = ?outcome,
                    "inline-coverage census"
                );
                seen.insert((namespace.clone(), table));
            }
        }
        // A namespace this pass could not open is missing from `seen` entirely,
        // and zeroing its tables would read as "repaired" when the truth is
        // "unvisited". `aggregate_contexts` warns and skips such a namespace,
        // so hold the previous set when the pass reached nothing at all.
        if !seen.is_empty() {
            let mut published = self.published_inline_coverage.lock().unwrap();
            for (namespace, table) in vanished_inline_coverage(&published, &seen) {
                siglake_storage::iceberg::report_inline_coverage(&namespace, &table, false);
            }
            *published = seen;
        }
        metrics::counter!("siglake_inline_coverage_census_total").increment(1);
    }

    /// Run a single snapshot-metadata expiry pass over the events table,
    /// retaining the configured number of recent snapshots. Non-destructive
    /// (metadata only) and idempotent — a second pass at the same retention
    /// expires nothing. Returns the number of snapshots expired (0 if expiry
    /// is off or the table is already at/under retention). Safe to race
    /// across pods: the catalog CAS serializes commits.
    pub async fn run_expire_once(&self) -> Result<usize> {
        let Some(cfg) = self.expire else {
            return Ok(0);
        };
        self.compact_acknowledged_proofs(&self.ice).await?;
        // EVERY table, not just `events`. This expired only the events table, so
        // user indexes accumulated snapshots without bound — and all benchmark
        // data, plus every user-created index in production, lives in one.
        //
        // The 2026-08-10 1TB round measured `logs-bench` at **626 snapshots p50**
        // against a `retain_last` of 100, still climbing. metadata.json is
        // re-read and re-parsed on EVERY append, so its size multiplies directly
        // into `load_table` — the cost that grew ~240x across a round (40 ms
        // early, 18,930 ms late) and was the largest unexplained bucket in the
        // write path.
        //
        // Deduped the same way the other whole-warehouse sweeps do: `list_indexes`
        // reports `events` as a built-in index, so naive concatenation visits it
        // twice.
        let mut idents = vec![self.ice.events_table_ident().clone()];
        for config in self.ice.list_indexes().await.unwrap_or_default() {
            let ident = self.ice.index_table_ident(&config.index_id);
            if !idents.contains(&ident) {
                idents.push(ident);
            }
        }

        let mut total = 0usize;
        for ident in &idents {
            // One table's failure must not skip the rest — this runs on a timer
            // and a transient catalog error should cost one table one cycle.
            match self.ice.expire_snapshots(ident, cfg.retain_last).await {
                Ok(expired) => {
                    total += expired;
                    if expired > 0 {
                        tracing::info!(
                            table = %ident.name(),
                            expired,
                            retain_last = cfg.retain_last,
                            "expired old snapshots (BIG-4 #4c)"
                        );
                        metrics::counter!(
                            "siglake_compactor_snapshots_expired_total",
                            "table" => ident.name().to_string()
                        )
                        .increment(expired as u64);
                    }
                }
                Err(e) => tracing::warn!(
                    table = %ident.name(),
                    error = format!("{e:#}"),
                    "snapshot expiry failed for one table; continuing"
                ),
            }
        }
        if total > 0 {
            metrics::counter!("siglake_compactor_expire_passes_total").increment(1);
        }
        Ok(total)
    }

    /// Report delete tasks left non-terminal, once per namespace per sweep,
    /// BEFORE any of them executes.
    ///
    /// Ordering matters: the reported set is the one that predates this sweep,
    /// so a task this sweep is about to run is never counted as stalled, and a
    /// task stranded by this sweep's own cancellation surfaces on the next one.
    ///
    /// Read-only and non-fatal. An observation that fails must not make the
    /// sweep it is watching less reliable, so a failure is recorded as unknown
    /// coverage and execution proceeds; the coverage rule then withholds the
    /// gauge rather than publishing a low count that reads as recovery.
    ///
    /// Every gate on the delete stage is a gate on this too — execution
    /// disabled, a non-maintaining role, the `delete_tasks` lease held
    /// elsewhere, a throttled backlog, a watchdog trip, a tenant with tasks but
    /// no WAL directory. This is a signal about tasks, not a liveness check on
    /// the compactor, and no alert reads the absence of movement as health.
    async fn observe_nonterminal_delete_tasks(&self, contexts: &[(String, Arc<IcebergContext>)]) {
        let mut observations = Vec::with_capacity(contexts.len());
        // ONE observation per distinct NAMESPACE, not per context. Tenant
        // "default" IS the default namespace (`ice_for_tenant`), so a
        // deployment with a `<wal>/default/` subdir has the same namespace
        // twice in `contexts` — reporting it twice would double every counter
        // increment and every WARN for one stranded task.
        let mut seen = HashSet::new();
        for (tenant, ice) in contexts {
            if !seen.insert(ice.namespace().to_string()) {
                continue;
            }
            observations.push((
                tenant.clone(),
                ice.observe_nonterminal_delete_tasks()
                    .await
                    .map_err(|e| format!("{e:#}")),
            ));
        }
        let report = {
            let mut logged = match self.delete_task_stall_logged.lock() {
                Ok(logged) => logged,
                // A poisoned rate limit is not worth losing the signal over.
                Err(poisoned) => poisoned.into_inner(),
            };
            plan_delete_task_observation(
                chrono::Utc::now(),
                delete_task_stall_bound(),
                &observations,
                &mut logged,
            )
        };
        emit_delete_task_observation(&report);
    }

    /// Execute pending delete tasks across the default namespace and every
    /// tenant namespace discovered under the WAL root. Runs only while ingest
    /// is idle and only when explicitly enabled.
    pub async fn run_delete_tasks_once(&self) -> Result<usize> {
        if !self.delete_tasks_enabled {
            return Ok(0);
        }
        let mut contexts = vec![("default".to_string(), self.ice.clone())];
        for (tenant, _dir) in list_tenant_dirs(&self.wal_dir).context("list_tenant_dirs")? {
            contexts.push((tenant.clone(), self.ice_for_tenant(Some(&tenant)).await?));
        }

        self.observe_nonterminal_delete_tasks(&contexts).await;

        let mut tasks_completed = 0usize;
        let mut files_rewritten = 0usize;
        let mut rows_deleted = 0u64;
        for (tenant, ice) in contexts {
            self.compact_acknowledged_proofs(&ice).await?;
            let outcomes = ice.execute_all_delete_tasks().await?;
            let tenant_tasks: usize = outcomes.iter().map(|o| o.tasks_completed).sum();
            let tenant_files: usize = outcomes.iter().map(|o| o.files_rewritten).sum();
            let tenant_rows: u64 = outcomes.iter().map(|o| o.rows_deleted).sum();
            if tenant_tasks > 0 {
                tracing::info!(
                    tenant,
                    tasks_completed = tenant_tasks,
                    files_rewritten = tenant_files,
                    rows_deleted = tenant_rows,
                    "delete-task maintenance sweep completed"
                );
                metrics::counter!("siglake_compactor_delete_tasks_completed_total")
                    .increment(tenant_tasks as u64);
                metrics::counter!("siglake_compactor_delete_task_files_rewritten_total")
                    .increment(tenant_files as u64);
                metrics::counter!("siglake_compactor_delete_task_rows_deleted_total")
                    .increment(tenant_rows);
            }
            tasks_completed += tenant_tasks;
            files_rewritten += tenant_files;
            rows_deleted = rows_deleted.saturating_add(tenant_rows);
        }
        let _ = files_rewritten;
        let _ = rows_deleted;
        Ok(tasks_completed)
    }

    /// Drain all currently-sealed segments into a single Iceberg commit.
    /// Returns the number of segments included in that commit (0 if there
    /// were none to process).
    #[tracing::instrument(skip_all)]
    pub async fn run_once(&self) -> Result<usize> {
        if self.catalog.is_some() {
            return self.run_once_catalog().await;
        }
        // Per-tenant sweep: if the WAL root has tenant subdirs (each
        // its own `sealed/`), process each independently and commit to
        // `tenant_<tenant>` namespaces. Empty list = legacy single-tenant
        // layout, processed against the default IcebergContext.
        //
        // Phase 4.12.18 — bug #21: we *also* process the legacy
        // top-level `sealed/` whenever it has segments, regardless of
        // whether tenant subdirs are present. Background: the
        // backpressure router writes events to
        // `<wal>/<tenant>/sealed/` even in single-tenant mode (tenant
        // = "default"). If a deployment toggles backpressure on, then
        // off, the ingester reverts to the legacy top-level layout but
        // the `default/` subdir lingers. Without the
        // legacy-top-level fallback below, the compactor would
        // *exclusively* sweep `default/` (which is now empty) and
        // orphan every segment written at the top level.
        let tenants = list_tenant_dirs(&self.wal_dir).context("list_tenant_dirs")?;
        let legacy_pending = list_sealed(&self.wal_dir).map(|v| v.len()).unwrap_or(0)
            + list_orphaned(&self.wal_dir).map(|v| v.len()).unwrap_or(0);
        let mut total = 0;
        let mut backlog = SealedBacklog::default();
        if legacy_pending > 0 || tenants.is_empty() {
            total += self
                .run_once_at(
                    &self.wal_dir,
                    "default",
                    &self.ice.clone(),
                    &CommitTarget::Events,
                    None,
                    &mut backlog,
                )
                .await?;
        } else {
            // The legacy root was visited and had nothing: an observation of
            // zero, not an absence of one. Without it a deployment whose only
            // tenant subdir is named something other than `default` would keep
            // exporting whatever the legacy root last held.
            backlog.observe_dir("default", &self.wal_dir, 0);
            // Same toggle as above, seen from retention's side: a deployment
            // that drained at the top level and then moved to tenant subdirs
            // leaves a `committed/` tail here that no later cycle would reach,
            // because this branch is the one it lands in from now on.
            self.sweep_retention_at(&self.wal_dir, "default").await;
        }
        for (tenant, dir) in tenants {
            let ice = self.ice_for_tenant(Some(&tenant)).await?;
            total += self
                .run_once_at(
                    &dir,
                    &tenant,
                    &ice,
                    &CommitTarget::Events,
                    None,
                    &mut backlog,
                )
                .await?;
            for (index, index_dir) in list_index_dirs(&dir).context("list_index_dirs")? {
                let pending = list_sealed(&index_dir).with_context(|| {
                    format!("listing sealed segments in {}", index_dir.display())
                })?;
                // #81: an idle index dir still needs a pass when orphans are
                // quarantined — disposition may requeue them for commit.
                let orphaned = list_orphaned(&index_dir).map(|v| v.len()).unwrap_or(0);
                if pending.is_empty() && orphaned == 0 {
                    backlog.observe_dir(&tenant, &index_dir, 0);
                    // Skipping the drain must not skip retention: an index that
                    // stops receiving writes still has a `committed/` tail from
                    // its last drain, and this branch is the only one it will
                    // ever reach again.
                    self.sweep_retention_at(&index_dir, &tenant).await;
                    continue;
                }
                if ice
                    .ensure_index(&index)
                    .await
                    .with_context(|| format!("ensure_index {index}"))?
                    .is_none()
                {
                    metrics::counter!(
                        "siglake_compactor_index_unresolved_total",
                        "tenant" => tenant.clone(),
                        "index" => index.clone()
                    )
                    .increment(pending.len() as u64);
                    // An index that cannot be resolved is still a queue: these
                    // segments are waiting, and every later cycle stops here
                    // too, so leaving them out of the tenant's total is how a
                    // stuck index becomes invisible to the HPA.
                    backlog.observe_dir(&tenant, &index_dir, pending.len());
                    // The fifth exit, and the one the sweep-on-every-cycle
                    // change missed. Lower stakes than the other four — this
                    // path has pending work and means an index that cannot be
                    // resolved rather than one that went quiet — but an index
                    // stuck here is stuck here on EVERY later cycle too, so its
                    // `committed/` tail would be stranded for exactly the same
                    // reason.
                    self.sweep_retention_at(&index_dir, &tenant).await;
                    continue;
                }
                // #2661: `index_dir` is keyed by the index NAME, so a
                // `DELETE`+`POST` of the same id hands the replacement table
                // the dropped incarnation's segments, and this loop would
                // commit them — permanently — into a table they were never
                // destined for. The owner marker is the identity the name does
                // not carry; on a mismatch the segments are quarantined rather
                // than committed or deleted.
                let claim = match self.verified_owner_for_test.as_deref() {
                    Some(uuid) => WalDirClaim::Owned(uuid.to_string()),
                    None => {
                        self.claim_index_wal_dir(&ice, &index, &index_dir, &tenant)
                            .await?
                    }
                };
                let owner = match claim {
                    // `OwnedStrict` is the mirror path's verdict and
                    // `claim_index_wal_dir` never returns it; the filesystem
                    // path has no unstamped-segment ambiguity to resolve,
                    // because a stale directory's unstamped segments are moved
                    // out from under the marker before it is re-stamped — only
                    // segments naming the live table in their own header are
                    // left for the following cycle to drain.
                    WalDirClaim::Owned(uuid) | WalDirClaim::OwnedStrict(uuid) => Some(uuid),
                    WalDirClaim::Unidentified => None,
                    WalDirClaim::Refused => {
                        // `pending` predates the quarantine this claim just
                        // ran, so it counts segments that are no longer in
                        // `sealed/`. What the sweep KEPT is the backlog the
                        // next cycle drains.
                        let kept = list_sealed(&index_dir).map(|v| v.len()).unwrap_or(0);
                        backlog.observe_dir(&tenant, &index_dir, kept);
                        self.sweep_retention_at(&index_dir, &tenant).await;
                        continue;
                    }
                };
                let config = ice
                    .get_index(&index)
                    .await
                    .with_context(|| format!("get_index {index}"))?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "ensure_index succeeded but get_index({index}) returned None"
                        )
                    })?;
                // #2836: the append resolves the index NAME again, and the
                // name can mean a different table by then. Carry what the
                // check just verified into it.
                let target = CommitTarget::Index {
                    bloom_columns: utf8_bloom_columns(&config),
                    config: Box::new(config),
                    index,
                    expect_table_uuid: owner.clone(),
                };
                total += self
                    .run_once_at(
                        &index_dir,
                        &tenant,
                        &ice,
                        &target,
                        owner.as_deref(),
                        &mut backlog,
                    )
                    .await?;
            }
        }
        // Only a sweep that ran to the end publishes. Every `?` above leaves
        // the previous cycle's reading in place rather than exporting a total
        // that stops at whichever directory failed.
        let mut published_tenants = self
            .published_fs_backlog_tenants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        backlog.publish(&mut published_tenants);
        Ok(total)
    }

    /// #2661: decide whether `index_dir`'s segments belong to the table
    /// `index` resolves to right now, and under which identity to drain it.
    ///
    /// A per-index WAL directory carries no table identity of its own — the
    /// ingester never learns one — so this is the only place the drain can
    /// notice that the id it is about to commit under names a DIFFERENT table
    /// than the one whose rows are on disk. Three cases:
    ///
    /// - unmarked: a directory from before the marker existed, or one this
    ///   drain has not visited yet. Stamped and drained; from here on it has an
    ///   identity, which is what makes the next recreation detectable.
    /// - same table: drained, no write.
    /// - different table: the dropped incarnation's segments are moved to
    ///   `stale/<dropped-uuid>/` and the directory is re-stamped for the live
    ///   table. They are NOT deleted — a never-committed segment's rows may
    ///   exist nowhere else — and NOT committed, because the table they were
    ///   written for is gone. Reclaiming them is an operator decision on
    ///   visible files. Segments whose own header names the LIVE table stay
    ///   where they are (#2835): a writer that re-resolved the index before
    ///   this drain arrived has already acknowledged rows into this directory,
    ///   and the marker being displaced never spoke for them. The cycle still
    ///   refuses the index; the next one finds an `Owned` directory and drains
    ///   what was kept.
    ///
    /// In-flight drains: the check runs BEFORE this cycle claims anything out
    /// of `sealed/`, so nothing this process is committing can be quarantined
    /// under it. The quarantine also moves `processing/`, which is sound on
    /// the same assumption the FS path already rests on — it is the
    /// single-replica deployment, and `recover_orphaned_processing` has
    /// emptied `processing/` at startup for exactly that reason. Multi-replica
    /// drains run the catalog-claim path, where
    /// [`Self::claim_mirrored_index`] does this check against the mirror and
    /// the claim store arbitrates.
    ///
    /// Stale writers: an ingester still holding the dropped incarnation's
    /// writer open keeps appending to this directory, and whatever it seals
    /// after the quarantine lands in a directory that now says "replacement".
    /// The directory marker cannot speak for those; the identity in each
    /// segment's frame header can, which is why the returned uuid is carried
    /// into the per-segment gate ([`retain_owned_sealed`], #2693).
    async fn claim_index_wal_dir(
        &self,
        ice: &IcebergContext,
        index: &str,
        index_dir: &Path,
        tenant: &str,
    ) -> Result<WalDirClaim> {
        let Some(uuid) = ice
            .index_table_uuid(index)
            .await
            .with_context(|| format!("index_table_uuid {index}"))?
        else {
            return Ok(WalDirClaim::Unidentified);
        };
        match siglake_wal::classify_wal_owner(index_dir, &uuid) {
            siglake_wal::WalOwner::Owned => Ok(WalDirClaim::Owned(uuid)),
            siglake_wal::WalOwner::Unmarked => {
                siglake_wal::stamp_wal_owner(index_dir, &uuid)?;
                Ok(WalDirClaim::Owned(uuid))
            }
            siglake_wal::WalOwner::Stale(previous) => {
                let sweep = siglake_wal::quarantine_stale_wal_dir(index_dir, &uuid)?;
                tracing::warn!(
                    tenant,
                    index,
                    dropped_table = %previous,
                    live_table = %uuid,
                    quarantined = sweep.quarantined,
                    // #2857: what the sweep LEFT is the other half of the
                    // picture — segments the replacement's own writers already
                    // acknowledged here, which the next cycle drains normally.
                    kept = sweep.kept,
                    dir = %index_dir.display(),
                    "WAL directory belongs to a dropped incarnation of this index; \
                     segments quarantined under stale/ instead of committed"
                );
                // Unchanged meaning: refusal events, counted in segments
                // quarantined. Kept segments are not a refusal.
                metrics::counter!(
                    "siglake_compactor_wal_owner_mismatch_total",
                    "tenant" => tenant.to_string(),
                    "index" => index.to_string()
                )
                .increment(sweep.quarantined as u64);
                Ok(WalDirClaim::Refused)
            }
        }
    }

    /// [`Self::claim_index_wal_dir`] for the catalog-claim drain, whose
    /// segments live in the WAL mirror rather than on this pod's filesystem.
    ///
    /// Same cases and the same "unmarked is no opinion" rule, over
    /// [`siglake_wal::mirror::mirror_owner_key`]. There is no quarantine move
    /// here: the objects stay where they are, and the caller's release path
    /// takes the claims out of circulation. Copying them to a second prefix
    /// would double the bytes an operator then has to reason about, for a set
    /// that is already addressable by the dropped index's key prefix.
    ///
    /// #2729: with nothing to move, a stale marker used to be a permanent
    /// refusal of the whole prefix — and because the prefix is routed by index
    /// NAME, that refused the REPLACEMENT's own objects too, wedging the
    /// recreated index's ingest for good. The marker moves to the live table
    /// instead, and the per-segment identity from #2693 decides each object:
    /// the same "re-stamp, then gate per segment" shape
    /// [`Self::claim_index_wal_dir`] and [`retain_owned_sealed`] already have
    /// on the filesystem side. Because the re-stamped prefix then vouches for
    /// objects that were uploaded before it moved, the claim is STRICT: an
    /// object with no identity of its own is refused rather than adopted, for
    /// as long as the marker records a superseded owner.
    async fn claim_mirrored_index(
        &self,
        ice: &IcebergContext,
        cfg: &CatalogClaimConfig,
        tenant: &str,
        index: &str,
    ) -> Result<WalDirClaim> {
        let Some(uuid) = ice
            .index_table_uuid(index)
            .await
            .with_context(|| format!("index_table_uuid {index}"))?
        else {
            return Ok(WalDirClaim::Unidentified);
        };
        let marker = siglake_wal::mirror::classify_mirror_owner(
            &cfg.store,
            &cfg.prefix,
            tenant,
            index,
            &uuid,
        )
        .await?;
        match marker.state {
            // A prefix that has only ever named this table vouches for an
            // unstamped object under it; one that has named another does not.
            siglake_wal::WalOwner::Owned if marker.superseded.is_empty() => {
                Ok(WalDirClaim::Owned(uuid))
            }
            siglake_wal::WalOwner::Owned => Ok(WalDirClaim::OwnedStrict(uuid)),
            siglake_wal::WalOwner::Unmarked => {
                siglake_wal::mirror::stamp_mirror_owner(
                    &cfg.store,
                    &cfg.prefix,
                    tenant,
                    index,
                    &uuid,
                )
                .await?;
                Ok(WalDirClaim::Owned(uuid))
            }
            siglake_wal::WalOwner::Stale(previous) => {
                let mut superseded = Vec::with_capacity(marker.superseded.len() + 1);
                superseded.push(previous.clone());
                superseded.extend(marker.superseded.into_iter().filter(|u| u != &previous));
                siglake_wal::mirror::restamp_mirror_owner(
                    &cfg.store,
                    &cfg.prefix,
                    tenant,
                    index,
                    &uuid,
                    &superseded,
                )
                .await?;
                tracing::warn!(
                    tenant,
                    index,
                    dropped_table = %previous,
                    live_table = %uuid,
                    "mirror prefix was stamped for a dropped incarnation of this index; \
                     re-stamped for the live table, and every object under it is now \
                     admitted only on its own frame identity"
                );
                metrics::counter!(
                    "siglake_compactor_wal_owner_mismatch_total",
                    "tenant" => tenant.to_string(),
                    "index" => index.to_string()
                )
                .increment(1);
                Ok(WalDirClaim::OwnedStrict(uuid))
            }
        }
    }

    async fn ice_for_tenant(&self, tenant: Option<&str>) -> Result<Arc<IcebergContext>> {
        match tenant {
            // Tenant "default" IS the default namespace. The ingester labels
            // header-less traffic "default" and writes it under a
            // `<wal>/default/` subdir, but readers with no tenant claim (the
            // query server's open/no-OIDC path) resolve to the release
            // namespace — committing "default" to `tenant_default` made that
            // data invisible to default reads (WI-8 local-A/B finding). The
            // catalog-claim path and the delete-task sweep already treat
            // "default" as the main namespace; this aligns the FS path.
            Some("default") | None => Ok(self.ice.clone()),
            Some(t) => Ok(Arc::new(
                self.ice
                    .for_namespace(&format!("tenant_{t}"))
                    .await
                    .with_context(|| format!("open tenant namespace tenant_{t}"))?,
            )),
        }
    }

    /// Sweep retention scoped to one WAL directory, coordinated with secondary
    /// consumers (detector shards): a committed segment is kept past the soft
    /// floor (`self.retention`) until every fresh consumer has processed past
    /// it, bounded by a hard ceiling so a stuck consumer can't grow
    /// `committed/` without bound. Detection is therefore lossless for a
    /// consumer keeping up within the ceiling.
    ///
    /// This must run on EVERY cycle for a directory, not only cycles that
    /// commit. A segment enters `committed/` at age 0 (`finish_segment`'s
    /// rename), so it is never sweepable on the cycle that commits it — it
    /// needs a later cycle, `self.retention` on. When the sweep only ran on
    /// cycles that also had sealed work, the trailing retention window's worth
    /// of segments was stranded the moment writes to that directory stopped:
    /// no subsequent cycle got far enough to sweep them. That is per-directory,
    /// so a busy multi-tenant deployment still stranded the tail for every
    /// tenant and index that went quiet, and the volume scaled with the rate
    /// that had been running rather than with how idle the system was.
    /// In ledger-only reclamation the sweep is also gated on the mark being
    /// durable ([`Self::mark_mirror_ledger_at`]): a `committed/` file is the
    /// local evidence that the segment's rows are in Iceberg, and destroying
    /// it before the ledger says so would leave the mirror object with nothing
    /// left to prove it can be deleted. The hard ceiling still wins, so a
    /// catalog outage costs a bounded leak rather than an unbounded volume.
    async fn sweep_retention_at(&self, dir: &Path, tenant_label: &str) {
        let marked = self.mark_mirror_ledger_at(dir, tenant_label).await;
        match sweep_committed_gated(
            dir,
            self.retention,
            CONSUMER_MAX_RETENTION,
            CONSUMER_STALE_AFTER,
            marked.as_ref(),
        ) {
            Ok(swept) if swept.deleted == 0 => {}
            Ok(swept) => {
                tracing::debug!(
                    tenant = tenant_label,
                    deleted = swept.deleted,
                    unmarked = swept.unmarked,
                    "swept retention-expired segments"
                );
                metrics::counter!("siglake_compactor_segments_swept_total")
                    .increment(swept.deleted as u64);
                if swept.unmarked > 0 {
                    // The leak the ceiling buys, reported where an operator
                    // can see it rather than inferred from a growing prefix.
                    tracing::warn!(
                        tenant = tenant_label,
                        unmarked = swept.unmarked,
                        ceiling_secs = CONSUMER_MAX_RETENTION.as_secs(),
                        "swept locally-committed segments the ledger never accepted; \
                         their mirror objects are now unreclaimable by this drain"
                    );
                    metrics::counter!("siglake_compactor_mirror_unreclaimed_total")
                        .increment(swept.unmarked as u64);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "retention sweep failed");
            }
        }
    }

    /// Upsert a `committed` ledger row for every segment in `<dir>/committed/`,
    /// and return the file names whose mark is durable — the sweep gate.
    ///
    /// Driven off the DIRECTORY, not off the commit return, which is what
    /// makes it idempotent and crash-repairing: a compactor that dies between
    /// the Iceberg append and the mark finds the file still in `committed/` on
    /// its next cycle.
    ///
    /// A segment with a live `mirror-pending/` pin is skipped entirely. Its
    /// upload is still owed, and a mark would let retention delete a key the
    /// uploader is about to write — which leaks an object no pass revisits. A
    /// pin that never clears is covered by the sweep's hard ceiling.
    ///
    /// `None` (no gate) whenever ledger reclamation is off, which is every
    /// deployment that did not opt in.
    async fn mark_mirror_ledger_at(
        &self,
        dir: &Path,
        tenant_label: &str,
    ) -> Option<BTreeSet<String>> {
        let cfg = self.mirror_ledger.as_ref()?;
        let committed = match list_committed(dir) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, dir = %dir.display(),
                    "mirror ledger: could not list committed segments");
                // No gate rather than an empty one: a listing failure is not
                // evidence that nothing is marked, and an empty gate would
                // hold every file to the ceiling and then call it a leak.
                return None;
            }
        };
        if committed.is_empty() {
            return Some(BTreeSet::new());
        }
        let pinned = siglake_wal::mirror::mirror_pending_names(dir);
        let subdir = self.mirror_subdir_for(dir);
        let prefix = cfg.prefix.trim_matches('/');
        let mut already: BTreeSet<String> = BTreeSet::new();
        let mut pending = Vec::new();
        {
            let cache = self.mirror_marked.lock().expect("mirror mark cache");
            let seen = cache.get(dir);
            for path in &committed {
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                if pinned.contains(name) {
                    continue;
                }
                let id = name.trim_end_matches(".arrow").to_string();
                if seen.is_some_and(|s| s.contains(&id)) {
                    already.insert(name.to_string());
                    continue;
                }
                let suffix = if subdir.is_empty() {
                    name.to_string()
                } else {
                    format!("{subdir}/{name}")
                };
                let (tenant, index_id) = siglake_wal::mirror::parse_mirror_key_suffix(&suffix);
                pending.push(LocalCommittedSegment {
                    id,
                    tenant,
                    index_id,
                    segment_url: format!("{prefix}/{suffix}"),
                    bytes: path.metadata().map(|m| m.len() as i64).unwrap_or(0),
                });
            }
        }
        let marked = if pending.is_empty() {
            Vec::new()
        } else {
            match cfg.claim.mark_committed_local(&pending).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::warn!(error = %e, tenant = tenant_label,
                        "mirror ledger: marking locally-committed segments failed; \
                         holding their local evidence until the ceiling");
                    metrics::counter!("siglake_compactor_mirror_mark_errors_total").increment(1);
                    self.remember_marks(dir, &already);
                    return Some(already);
                }
            }
        };
        metrics::counter!("siglake_compactor_mirror_marked_total").increment(marked.len() as u64);
        for id in marked {
            already.insert(format!("{id}.arrow"));
        }
        self.remember_marks(dir, &already);
        Some(already)
    }

    /// Replace one WAL directory's remembered marks with the set this cycle
    /// confirmed. Replacing rather than accumulating is what bounds the cache
    /// by the live `committed/` population: an id whose file has been swept
    /// drops out, and re-marking a segment whose row retention already purged
    /// would resurrect it.
    fn remember_marks(&self, dir: &Path, marked: &BTreeSet<String>) {
        let ids = marked
            .iter()
            .map(|name| name.trim_end_matches(".arrow").to_string())
            .collect();
        self.mirror_marked
            .lock()
            .expect("mirror mark cache")
            .insert(dir.to_path_buf(), ids);
    }

    /// The mirror key subdir for one WAL directory: `""` for the legacy root,
    /// `<tenant>`, or `<tenant>/<index>` — the layout
    /// [`siglake_wal::mirror::catch_up_sweep`] uploads under and
    /// `parse_mirror_key_suffix` reads back.
    fn mirror_subdir_for(&self, dir: &Path) -> String {
        match dir.strip_prefix(&self.wal_dir) {
            Ok(rel) => rel
                .components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect::<Vec<_>>()
                .join("/"),
            // Not under this compactor's WAL root: the caller only ever passes
            // directories it discovered there, so treat it as the root rather
            // than inventing a key.
            Err(_) => String::new(),
        }
    }

    /// FS-rename path scoped to one WAL directory and one commit target. The
    /// target-specific append logic is parameterized, but claim/release/finish
    /// semantics remain identical across the events root and per-index WAL dirs.
    ///
    /// `backlog` collects this directory's sealed count under `tenant_label`;
    /// the caller publishes the tenant's total once the whole sweep is done
    /// (#3691). This function publishes no backlog gauge of its own.
    async fn run_once_at(
        &self,
        dir: &Path,
        tenant_label: &str,
        ice: &Arc<IcebergContext>,
        target: &CommitTarget,
        owner: Option<&str>,
        backlog: &mut SealedBacklog,
    ) -> Result<usize> {
        let cycle_start = std::time::Instant::now();
        // #81: auto-dispose quarantined orphans before listing — a requeued
        // segment re-enters sealed/ and commits in this same cycle.
        // Best-effort: a disposition error leaves the quarantine untouched.
        if let Err(e) = self
            .dispose_orphans_at(dir, tenant_label, ice, target)
            .await
        {
            tracing::warn!(error = %e, dir = %dir.display(), "orphan auto-disposition failed");
        }
        let sealed = retain_owned_sealed(
            list_sealed(dir).context("listing sealed segments")?,
            owner,
            tenant_label,
            target.index_label(),
        );
        backlog.observe_dir(tenant_label, dir, sealed.len());
        if sealed.is_empty() {
            metrics::counter!("siglake_compactor_cycles_total",
                "outcome" => "empty", "tenant" => tenant_label.to_string())
            .increment(1);
            // An idle directory is exactly where the previous drain's
            // `committed/` tail comes due: this is the only cycle shape that
            // will ever see those segments past the retention floor.
            self.sweep_retention_at(dir, tenant_label).await;
            return Ok(0);
        }
        // BIG-4 #4b: commit-accumulation gate. If batching is enabled
        // and the sealed queue hasn't yet reached the byte target or
        // the age floor, hold off this cycle so the fixed per-commit
        // catalog cost amortizes over more rows. Segments stay in
        // `sealed/` (unclaimed) until a later cycle clears the gate.
        if self.commit_batch.is_some() {
            let (pending_bytes, oldest_age) = fs_pending_stats(&sealed);
            if self.commit_deferred(pending_bytes, oldest_age) {
                metrics::counter!("siglake_compactor_cycles_total",
                    "outcome" => "deferred", "tenant" => tenant_label.to_string())
                .increment(1);
                metrics::counter!("siglake_compactor_commit_deferred_total",
                    "tenant" => tenant_label.to_string())
                .increment(1);
                // Deferring the commit must not defer the sweep: batching can
                // hold a low-rate tenant off for many consecutive cycles, and
                // `committed/` retention is independent of when the next
                // commit happens.
                self.sweep_retention_at(dir, tenant_label).await;
                return Ok(0);
            }
        }
        // B.1.3 continuous dispatch: keep up to `drain_concurrency()` commits in
        // flight CONTINUOUSLY for up to `drain_cycle_budget()` — top up a new
        // bounded batch the moment one lands (no barrier: the old
        // claim-N-then-join_all pass lasted as long as its SLOWEST commit while
        // the other workers idled, and the discriminator round showed the drain
        // is latency-bound with every resource idle). Batches are disjoint by
        // construction (each selection consumes a strict prefix of the remaining
        // sealed snapshot, then atomically renames into processing/; the snapshot
        // is re-listed when it runs dry so a long pass picks up newly-sealed
        // segments). Iceberg's optimistic concurrency serializes the actual
        // catalog commits; everything else overlaps.
        use futures::stream::{FuturesUnordered, StreamExt};

        let workers = self.drain_concurrency.unwrap_or_else(drain_concurrency);
        let deadline = cycle_start + self.drain_cycle_budget.unwrap_or_else(drain_cycle_budget);
        let byte_budget = drain_inflight_budget_bytes();
        let mut inflight_bytes = 0u64;
        let mut queue: Vec<PathBuf> = sealed;
        let mut offset = 0usize;
        // #4651: claim attempts this pass has spent on each segment, keyed by
        // segment NAME. A failed batch goes back to `sealed/` and the re-list
        // below hands it straight to the next selection, so without a bound the
        // pass re-claims and re-fails the same set — a rename and two fsyncs
        // per segment each way — for the whole cycle budget, as fast as the
        // failure returns. The bound is per segment rather than per batch so
        // re-grouping a failing segment with fresh siblings cannot buy it
        // another run of attempts, and it is pass-local: the next cycle starts
        // every segment at zero, which keeps recovery from a transient cause
        // one poll away and leaves #3143's `poison/` ledger the only accounting
        // that survives a cycle.
        let mut pass_attempts: HashMap<String, u32> = HashMap::new();
        let mut inflight = FuturesUnordered::new();
        let mut committed_segments = 0usize;
        let mut first_err: Option<anyhow::Error> = None;
        let mut stop_top_up = false;

        loop {
            // Top up to the concurrency target while budget remains.
            while !stop_top_up && inflight.len() < workers && std::time::Instant::now() < deadline {
                if offset >= queue.len() {
                    // Snapshot exhausted: re-list once per exhaustion to pick up
                    // segments sealed while this pass ran. processing/ renames make
                    // claimed files invisible to the fresh listing. Respect the
                    // commit-accumulation gate on refills (same #4b semantics as
                    // cycle entry: a sub-target tail waits for bytes or age).
                    let fresh = retain_owned_sealed(
                        list_sealed(dir).context("re-listing sealed segments")?,
                        owner,
                        tenant_label,
                        target.index_label(),
                    );
                    // #4651: withhold the segments this pass has already tried
                    // MAX_PASS_CLAIM_ATTEMPTS times. They stay in `sealed/`
                    // untouched for the next cycle; everything else in the
                    // fresh list — healthy siblings, segments sealed while this
                    // pass ran — is still claimable, so one failing segment
                    // does not stop the drain. The old length-comparison guard
                    // here could not fire: this block only runs with the
                    // snapshot exhausted, so the length it compared against was
                    // always 0 and an empty fresh list had already broken out.
                    let mut fresh = fresh;
                    fresh.retain(|p| !withhold_spent_segment(&mut pass_attempts, p, tenant_label));
                    if fresh.is_empty() {
                        break;
                    }
                    if self.commit_batch.is_some() {
                        let (pending_bytes, oldest_age) = fs_pending_stats(&fresh);
                        if self.commit_deferred(pending_bytes, oldest_age) {
                            break;
                        }
                    }
                    queue = fresh;
                    offset = 0;
                }
                let selected = self
                    .select_sealed_for_cycle(&queue[offset..])
                    .context("selecting sealed segments for cycle")?;
                if selected.is_empty() {
                    break;
                }
                let selected_bytes: u64 = selected
                    .iter()
                    .map(|c| std::fs::metadata(c).map(|m| m.len()))
                    .collect::<std::io::Result<Vec<_>>>()
                    .context("stat selected sealed segments")?
                    .into_iter()
                    .sum();
                // Byte-budget admission (task #64): stop topping up rather than
                // exceed the in-flight byte budget; the batch stays unclaimed
                // (offset unadvanced) and is re-selected once a completion frees
                // budget. An empty pipeline always admits — no deadlock.
                if !admit_batch(
                    inflight.len(),
                    workers,
                    inflight_bytes,
                    selected_bytes,
                    byte_budget,
                ) {
                    break;
                }
                offset += selected.len();
                // #4651: charge the attempt before the claim, so a claim that
                // fails half-way through the batch is counted too — that path
                // releases its partial claims and resumes topping up, which is
                // its own spin.
                for s in &selected {
                    charge_pass_attempt(&mut pass_attempts, s);
                }
                metrics::histogram!(
                    "siglake_compactor_claimed_segments",
                    "tenant" => tenant_label.to_string()
                )
                .record(selected.len() as f64);
                metrics::histogram!(
                    "siglake_compactor_claimed_bytes",
                    "tenant" => tenant_label.to_string()
                )
                .record(selected_bytes as f64);
                let mut claimed: Vec<PathBuf> = Vec::with_capacity(selected.len());
                let mut claim_err: Option<anyhow::Error> = None;
                for s in &selected {
                    match claim_segment(s) {
                        Ok(p) => claimed.push(p),
                        Err(e) => {
                            // Release this batch's partial claims and stop topping
                            // up; in-flight commits are unaffected and drain out.
                            tracing::warn!(path = %s.display(), error = %e, "claim failed; releasing partial batch");
                            release_all(&claimed);
                            metrics::counter!("siglake_compactor_cycles_total",
                                "outcome" => "claim_error", "tenant" => tenant_label.to_string())
                            .increment(1);
                            claim_err = Some(e);
                            break;
                        }
                    }
                }
                if let Some(e) = claim_err {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                    break;
                }
                let ice = ice.clone();
                let wal_dir = dir.to_path_buf();
                let transient_failures = self.transient_fs_commit_failures_for_test.clone();
                inflight_bytes = inflight_bytes.saturating_add(selected_bytes);
                inflight.push(async move {
                    let inject_failure = transient_failures.is_some_and(|remaining| {
                        remaining
                            .fetch_update(
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                                |n| n.checked_sub(1),
                            )
                            .is_ok()
                    });
                    let res = if inject_failure {
                        Err(anyhow::anyhow!(
                            "injected transient filesystem commit failure"
                        ))
                    } else {
                        commit_claimed(&ice, &wal_dir, &claimed, target).await
                    };
                    (claimed, selected_bytes, res)
                });
            }
            metrics::gauge!(
                "siglake_compactor_drain_inflight",
                "tenant" => tenant_label.to_string()
            )
            .set(inflight.len() as f64);
            let Some((claimed, batch_bytes, res)) = inflight.next().await else {
                break;
            };
            inflight_bytes = inflight_bytes.saturating_sub(batch_bytes);
            match res {
                Ok(outcome) => {
                    let mut finish_failures = 0u64;
                    for c in &claimed {
                        if let Err(e) = finish_segment(c) {
                            finish_failures += 1;
                            tracing::warn!(path = %c.display(), error = %e, "finishing committed segment");
                        }
                    }
                    // Phase 4.13h: explicit counter so a recurring failure isn't
                    // lost in the warn-log noise. The round-12 lifecycle bug showed
                    // processing/ growing to 749 entries without a single
                    // observable warn — this metric makes the same pattern visible
                    // to alerts.
                    if finish_failures > 0 {
                        metrics::counter!(
                            "siglake_compactor_finish_failures_total",
                            "tenant" => tenant_label.to_string()
                        )
                        .increment(finish_failures);
                    }
                    metrics::counter!("siglake_compactor_segments_committed_total",
                        "tenant" => tenant_label.to_string())
                    .increment(outcome.segments as u64);
                    metrics::counter!(
                        "siglake_compactor_rows_committed_total",
                        "tenant" => tenant_label.to_string()
                    )
                    .increment(outcome.rows);
                    if outcome.strict_residual_rows > 0 {
                        metrics::counter!(
                            "siglake_compactor_strict_residual_rows_total",
                            "tenant" => tenant_label.to_string(),
                            "index" => target.index_label().to_string()
                        )
                        .increment(outcome.strict_residual_rows);
                    }
                    committed_segments += outcome.segments;
                    // #3143: a segment that committed owes nothing for the
                    // cycles it failed before — the ledger counts CONSECUTIVE
                    // failures, so a storage hiccup never accumulates toward a
                    // set-aside.
                    self.forget_read_failures(&claimed);
                }
                Err(e) => {
                    // #3143: charge the failure to the segments it names, and
                    // set aside the ones that have spent their attempts. What
                    // comes back is the rest of the batch — healthy siblings,
                    // and segments with attempts left.
                    let (claimed, set_aside) = self.set_aside_unreadable(claimed, &e, tenant_label);
                    backlog.observe_poisoned(tenant_label, set_aside);
                    // #4651: a set-aside is progress — the segment that failed
                    // this batch has left `sealed/` for good, so what is left of
                    // the batch faces a different queue and gets its pass
                    // attempts back. Without this, #3143's siblings would be
                    // charged for the batches the poisoned segment took down
                    // and wait a cycle at the default attempt budget. Every
                    // forgiveness costs a segment out of the directory, so the
                    // claims one pass can make stay finite.
                    if set_aside > 0 {
                        for c in &claimed {
                            if let Some(name) = c.file_name().and_then(|n| n.to_str()) {
                                pass_attempts.remove(name);
                            }
                        }
                    }
                    // A failed batch releases its own segments back to sealed/ for
                    // retry; in-flight siblings are unaffected (their claims are
                    // disjoint and their commits independent).
                    release_all(&claimed);
                    metrics::counter!("siglake_compactor_cycles_total",
                        "outcome" => "commit_error", "tenant" => tenant_label.to_string())
                    .increment(1);
                    // A table uuid is never reused. Retrying this directory in
                    // the same cycle can only rediscover the same incarnation
                    // boundary, so stop dispatching new work while allowing
                    // already-started sibling batches to finish. The next
                    // ordinary cycle's ownership check quarantines the released
                    // segments. Other errors retain the bounded retry path.
                    if e.downcast_ref::<AppendIncarnationMismatch>().is_some() {
                        stop_top_up = true;
                    }
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        let result = match (committed_segments, first_err) {
            (0, Some(e)) => Err(e),
            (n, _) => {
                let elapsed = cycle_start.elapsed().as_secs_f64();
                metrics::counter!("siglake_compactor_cycles_total",
                    "outcome" => "ok", "tenant" => tenant_label.to_string())
                .increment(1);
                metrics::histogram!("siglake_compactor_commit_duration_seconds").record(elapsed);
                Ok(n)
            }
        };

        // Phase 4.13h: cycle-end gauge on processing/. Anything
        // here at this point shouldn't exist — either commit_claimed
        // succeeded and finish_segment cleared it, or commit_claimed
        // failed and release_all moved it back to sealed/. A
        // non-zero gauge is the round-12 bug surfacing in real time.
        let processing_pending = std::fs::read_dir(dir.join(siglake_wal::PROCESSING_DIR))
            .map(|d| d.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        metrics::gauge!(
            "siglake_compactor_processing_pending",
            "tenant" => tenant_label.to_string()
        )
        .set(processing_pending as f64);
        if processing_pending > 0 {
            tracing::warn!(
                pending = processing_pending,
                tenant = tenant_label,
                "compactor cycle ended with non-empty processing/ — segments stuck (Phase 4.13h)"
            );
        }

        self.sweep_retention_at(dir, tenant_label).await;

        result
    }

    /// Run forever, polling `wal/sealed/` every `poll_interval`.
    pub async fn run_loop(self: std::sync::Arc<Self>, poll_interval: Duration) -> Result<()> {
        // Maintenance (re-clustering, snapshot expiry) runs on its own cadence,
        // only while ingest is idle, so it never contends with the commit hot
        // path.
        let mut last_recluster = tokio::time::Instant::now();
        let mut last_expire = tokio::time::Instant::now();
        // While a fragmented layout is actively consolidating, keep issuing passes
        // back-to-back instead of waiting a full `interval` between each. A single
        // bounded pass only heals a slice, and convergence is geometric (~13 passes
        // to take a 4×-scale 448-file layout down to its target), so the old
        // one-pass-per-interval cadence left the layout unconsolidated for many
        // idle minutes — the WI-200G under-consolidation. `draining` bypasses both
        // the interval gate and the poll sleep so consolidation runs at full speed,
        // while `run_once` (commit) is still checked between every pass so resumed
        // ingest always preempts maintenance.
        let mut draining = false;
        // Defensive cap on a single back-to-back drain burst. Convergence is
        // geometric, so even a pathologically fragmented layout settles in well
        // under this many passes; the cap only exists so a degenerate
        // remove-then-re-add oscillation can't spin the loop without ever sleeping.
        // On hitting it we fall back to the interval cadence (which still makes
        // progress, just paced) rather than busy-looping.
        const DRAIN_BURST_CAP: u32 = 256;
        let mut drain_passes: u32 = 0;
        // Graded backpressure (A.4.3): count consecutive backlogged cycles so we can
        // run ONE throttled compaction every Nth of them (keeps files bounded under
        // sustained ingest without re-starving the drain). Reset when caught up.
        let mut backlog_cycles: u64 = 0;
        // Sample the live-file gauges on this cadence regardless of whether
        // compaction runs — otherwise the gauge goes stale under sustained ingest
        // (the backpressure gate suppresses the passes that would refresh it).
        let mut last_gauge = tokio::time::Instant::now();
        const GAUGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
        // R1 per-tier cadence (leveled mode): when each level last had a pass.
        // `None` = never ⇒ due immediately. The leading edge (L0) reruns on a fast
        // tick while slow cold levels are rescanned rarely — each pass re-lists the
        // table's live files, so needless cold scans are pure catalog load.
        let mut level_last_run: Vec<Option<tokio::time::Instant>> =
            vec![
                None;
                self.recluster
                    .as_ref()
                    .and_then(|c| c.levels.as_ref())
                    .map_or(0, |l| l.level_ceilings.len())
            ];
        // A.4.1 (continuous compaction): when set, run a bounded recluster pass
        // INTERLEAVED with the drain (paced by `interval`) rather than only when
        // ingest is idle, so compaction makes steady progress under a sustained
        // baseline write rate (logging is bursty but never fully idle). Default off
        // preserves drain-priority. See docs/DESIGN_continuous_compaction_and_ingest.md.
        let concurrent = std::env::var("SIGLAKE_COMPACTOR_CONCURRENT_RECLUSTER")
            .ok()
            .as_deref()
            == Some("1");
        // A.2 concurrent scheduler (#63, second half): recluster passes run on
        // a SPAWNED task so the drain keeps cycling while a multi-minute merge
        // executes — freshness no longer waits on maintenance at all. The
        // 2026-07-07 round measured single L1→L2 bins at 212–238s; with the
        // sequential loop every sealed segment arriving in that window waited
        // the full bin. Drain and pass commits race safely: appends and
        // rewrites touch disjoint files and both retry through the catalog's
        // optimistic concurrency (the same argument — proven cross-process in
        // the fleet round — applies in-process). One pass in flight at a time.
        let mut recluster_task: Option<tokio::task::JoinHandle<Result<usize>>> = None;
        let mut task_throttled = false;
        // Per-bin heartbeat from the in-flight re-clustering pass, so the
        // watchdog below can tell SLOW from WEDGED. See its comment.
        let recluster_progress = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut recluster_progress_seen: u64 = 0;
        // #80: watchdog ceilings — a drain cycle or maintenance await that
        // exceeds its ceiling is hung (credential/network wedge), not slow.
        let drain_watchdog = drain_watchdog_ceiling();
        let recluster_watchdog = recluster_watchdog_ceiling();
        let mut recluster_started: Option<tokio::time::Instant> = None;
        // Publish the effective role from the LOOP, not from `with_role`. The
        // builder runs before `metrics::init`, so a gauge set there goes to a
        // recorder that does not exist yet and is silently discarded -- which is
        // why the 2026-08-14 round could not tell an unapplied `--role drain`
        // from an applied one. Re-set each iteration so it is always present for
        // a scrape.
        let role_label = format!("{:?}", self.role).to_lowercase();
        let mut last_retention = tokio::time::Instant::now();
        let idle_backoff_base = drain_idle_backoff_base(poll_interval);
        let mut idle_backoff = idle_backoff_base;
        loop {
            // Bounds EVERYTHING below, including the `continue`s and the sleep, so
            // `iteration - cycle - maintenance` is the loop's own overhead rather
            // than a residual to be guessed at. Three attributions in a row put the
            // unexplained time in the wrong place (fetch: 4.8%, not 35%; sleep:
            // 5.2%, not 60%) because the outermost unit was never measured.
            let _iter_guard = StageTimer(
                "siglake_compactor_loop_iteration_duration_seconds",
                std::time::Instant::now(),
            );
            metrics::gauge!("siglake_compactor_effective_role",
                "role" => role_label.clone())
            .set(1.0);
            // Snapshot expiry, FIRST — before any `continue` can skip it.
            //
            // It has now been starved twice by two different early exits:
            //   `if throttled { continue; }`  -- comment literally says it skips
            //                                    expiry, and backpressure throttles
            //                                    constantly under sustained ingest
            //   `if draining && recluster_task.is_none() { continue; }`
            //
            // I moved it above the second and the 2026-08-11 round still measured
            // 291 snapshots with ZERO expiry passes, because the first still
            // skipped it. Placing it at the top of the loop is the only position
            // no future early-exit can silently starve.
            //
            // Cheap enough to deserve that slot: gated by its own interval
            // (default 60s), and a pass that finds nothing over `retain_last`
            // issues no commit at all. Starving it is self-defeating —
            // metadata.json is re-read and re-parsed on EVERY append, so an
            // unbounded snapshot list slows the very drain the `continue`s are
            // prioritising (load_table: 40 ms early in a round, 18,930 ms late).
            if let Some(cfg) = self.expire.filter(|_| self.role.maintains()) {
                if last_expire.elapsed() >= cfg.interval && self.maintenance_lease("expire").await {
                    last_expire = tokio::time::Instant::now();
                    // Counts REACHING expiry, not expiring anything. That is the
                    // property three fixes got wrong: the loop kept `continue`-ing
                    // past this block. `snapshots_expired_total` cannot express it
                    // — it stays 0 both when expiry is starved and when it runs
                    // with nothing over `retain_last`.
                    metrics::counter!("siglake_compactor_expire_attempts_total").increment(1);
                    let _exp_t = std::time::Instant::now();
                    let _exp_guard =
                        StageTimer("siglake_compactor_expire_duration_seconds", _exp_t);
                    match bounded(drain_watchdog, self.run_expire_once()).await {
                        Some(Err(e)) => {
                            tracing::error!(error = format!("{e:#}"), "snapshot expiry pass failed")
                        }
                        None => {
                            tracing::error!("snapshot expiry pass exceeded the watchdog ceiling");
                            metrics::counter!("siglake_compactor_watchdog_trips_total",
                                "stage" => "expire")
                            .increment(1);
                        }
                        Some(Ok(_)) => {}
                    }
                }
            }
            let cycle = async {
                self.run_once().await.unwrap_or_else(|e| {
                    // `%e` prints only the OUTERMOST context, so the 2026-08-10
                    // drain wedge logged `error=concat_batches` every 80s for
                    // five hours and said nothing about why. `{:#}` walks the
                    // anyhow chain, which is the difference between a diagnosis
                    // and a guess.
                    tracing::error!(error = format!("{e:#}"), "compactor cycle failed");
                    0
                })
            };
            let _cycle_t = std::time::Instant::now();
            let n = if !self.role.drains() {
                0
            } else {
                match drain_watchdog {
                    None => cycle.await,
                    Some(limit) => match tokio::time::timeout(limit, cycle).await {
                        Ok(n) => n,
                        Err(_) => {
                            tracing::error!(
                            limit_secs = limit.as_secs(),
                            "drain cycle exceeded the watchdog ceiling; aborting the pass and quarantining in-flight claims"
                        );
                            metrics::counter!("siglake_compactor_watchdog_trips_total",
                            "stage" => "drain")
                            .increment(1);
                            // Stranded processing/ claims from the aborted cycle go
                            // through the #81 disposition proof next cycle: deleted
                            // if their commit had landed, requeued if not — the
                            // abort stays exactly-once.
                            self.quarantine_processing_orphans();
                            0
                        }
                    },
                }
            };
            metrics::histogram!("siglake_compactor_cycle_duration_seconds")
                .record(_cycle_t.elapsed().as_secs_f64());
            if n > 0 {
                idle_backoff = idle_backoff_base;
                tracing::info!(segments = n, "compactor commit complete");
                // Ingest active — abandon any in-progress idle drain burst.
                draining = false;
                drain_passes = 0;
                // Drain-priority mode skips maintenance while ingesting; concurrent
                // mode falls through to ONE paced, bounded recluster pass
                // interleaved with the drain (no idle window required).
                if !concurrent {
                    continue;
                }
            }
            // Refresh the live-file gauges on their own cadence, independent of
            // compaction, so the layout is observable even while the backpressure
            // gate is suppressing the passes that would otherwise update it.
            if self.role.maintains() && last_gauge.elapsed() >= GAUGE_SAMPLE_INTERVAL {
                last_gauge = tokio::time::Instant::now();
                let levels = self.recluster.as_ref().and_then(|c| c.levels.as_ref());
                // #80: bounded — an observability sample must never wedge the
                // drain loop the way the Phase-4 credential outage did.
                let _g_t = std::time::Instant::now();
                let _g_guard = StageTimer("siglake_compactor_gauge_sample_duration_seconds", _g_t);
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    self.ice.sample_live_file_gauges(levels),
                )
                .await
                {
                    // A failed sample leaves the PREVIOUS value on the gauge, and
                    // a stale gauge is indistinguishable from a steady one. At 2B
                    // rows this timed out on every cycle of the 2026-08-03 round's
                    // settle, so `siglake_table_live_data_files` sat frozen at 886
                    // for hours while compaction was demonstrably adding and
                    // removing files — and the bench harness printed that frozen
                    // number on every settle line as though it were measured.
                    // The counter is what makes the staleness detectable; DEBUG
                    // hid it from every log that mattered.
                    //
                    // Since #1002 the count itself is read from the snapshot
                    // summary (O(1)) before the manifest walk, so on a timeout it
                    // is the WALKED gauges (`siglake_table_level_files`,
                    // `..._leading_edge_small_*`, `..._overlap_depth`) that are
                    // stale; `siglake_table_gauges_sampled_at_seconds` is set only
                    // when a table's walk completes. The counter keeps its
                    // meaning — "this sweep did not finish" — so the harness's
                    // STALE marker is unchanged.
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        metrics::counter!("siglake_table_live_data_files_sample_failures_total",
                            "reason" => "error")
                        .increment(1);
                        tracing::warn!(error = %e, "live-file gauge sample failed — gauge is STALE")
                    }
                    Err(_) => {
                        metrics::counter!("siglake_table_live_data_files_sample_failures_total",
                            "reason" => "timeout")
                        .increment(1);
                        tracing::warn!("live-file gauge sample timed out — gauge is STALE")
                    }
                }
            }
            // Backpressure-aware coexistence (design A.3/A.4.3). When the drain has
            // fallen behind (sealed backlog above the gate), the commit path wins:
            // instead of the full interleaved pass, run at most ONE *throttled*
            // compaction (a single bounded merge) every `compact_every` backlogged
            // cycles, and drain on the rest. This keeps files bounded under sustained
            // ingest — the static all-or-nothing gate let them grow to ~778 in the
            // 200G run — without re-starving the drain (the un-throttled leveled path
            // starved it to ~8k rows/s). `compact_every == 0` restores the hard skip.
            // The gate reads ~0 in catalog-claim mode.
            let backlog_gate = max_sealed_for_recluster();
            // Incremental group-count aggregate. Deliberately ABOVE the
            // backpressure and draining early-continues below, because those
            // starve it exactly when it matters: the 2026-08-03 1TB round ran
            // this ONCE in 7h48m — four minutes in — and deltas piled up to 865,
            // each of which a reader must GET to answer a group-count query.
            //
            // Placing it here does not make it greedy. `min_backlog` means a
            // busy cycle only absorbs once enough has accumulated to justify
            // rewriting the base (26.5MB on that table), while an idle cycle
            // takes whatever is there. A heavy ingest gets a few large folds
            // instead of either none at all or one every minute.
            if self.role.maintains() && agg_fold_due() && self.maintenance_lease("agg_fold").await {
                let idle = !draining && self.pending_sealed_total() == 0;
                let min_backlog = if idle { 1 } else { agg_fold_busy_backlog() };
                let _fold_guard = StageTimer(
                    "siglake_compactor_agg_fold_duration_seconds",
                    std::time::Instant::now(),
                );
                if bounded(drain_watchdog, self.run_agg_fold_once(min_backlog))
                    .await
                    .is_none()
                {
                    tracing::error!("group-count delta fold exceeded the watchdog ceiling");
                    metrics::counter!("siglake_compactor_watchdog_trips_total",
                        "stage" => "agg_fold")
                    .increment(1);
                }
                // The deficit census (#3000), inside the fold's lease and on its
                // own much slower interval. It runs AFTER the fold so it reads
                // the freshly folded base: the fold is what turns an outstanding
                // delta into coverage, and a census in front of it would read
                // every just-committed table as short.
                if let Some(interval) = agg_short_scan_interval() {
                    if agg_short_scan_due(interval) {
                        let budget = if agg_short_repair_enabled() {
                            agg_short_repair_max_tables()
                        } else {
                            0
                        };
                        if bounded(drain_watchdog, self.run_agg_short_repair_once(budget))
                            .await
                            .is_none()
                        {
                            // Safe to cut: the rebuild publishes in one CAS at
                            // the end, so a cancelled one leaves the aggregate
                            // exactly as short as it was and the next pass
                            // retries. What it costs is the reads it had done.
                            tracing::error!(
                                "short group-count aggregate repair exceeded the watchdog ceiling"
                            );
                            metrics::counter!("siglake_compactor_watchdog_trips_total",
                                "stage" => "agg_short_repair")
                            .increment(1);
                        }
                    }
                }
                // The inline object's coverage census (#4674), on the same
                // lease and its own interval. Separate from the census above
                // because it measures a different thing: that one is an
                // aggregate with a provable chain and too few rows, this one is
                // an object with the rows and no chain to prove them through.
                // After the fold for the same reason too — the fold is what
                // turns an outstanding delta into coverage.
                if inline_coverage_scan_interval().is_some_and(inline_coverage_scan_due)
                    && bounded(drain_watchdog, self.run_inline_coverage_census_once())
                        .await
                        .is_none()
                {
                    // Safe to cut: the census writes nothing anywhere. What a
                    // cut costs is the gauges it had not reached yet, which
                    // keep their previous reading until the next pass.
                    tracing::error!("inline-coverage census exceeded the watchdog ceiling");
                    metrics::counter!("siglake_compactor_watchdog_trips_total",
                        "stage" => "inline_coverage_census")
                    .increment(1);
                }
            }
            let mut throttled = false;
            if backlog_gate > 0 && self.pending_sealed_total() > backlog_gate {
                backlog_cycles = backlog_cycles.wrapping_add(1);
                let compact_every = backpressure_compact_every();
                if compact_every == 0 || !backlog_cycles.is_multiple_of(compact_every) {
                    draining = false;
                    drain_passes = 0;
                    metrics::counter!("siglake_compactor_maintenance_skipped_backpressure_total")
                        .increment(1);
                    continue;
                }
                // Throttled slot: fall through to run a single-bin compaction, then
                // loop straight back to draining (skip expiry/delete + the sleep).
                throttled = true;
                metrics::counter!("siglake_compactor_maintenance_throttled_backpressure_total")
                    .increment(1);
            } else {
                backlog_cycles = 0;
            }
            // Ingest is idle (or concurrent mode is on). Shape this cycle's
            // maintenance pass:
            //   - throttled slot: L0-only, one bin globally (already rationed to
            //     every Nth backlogged cycle) — runs regardless of cadence.
            //   - leveled + draining (idle burst): all levels, full speed.
            //   - leveled + paced: only the levels whose per-tier interval has
            //     elapsed (R1: L0 fast, cold levels slow); nothing due ⇒ no pass.
            //   - flat mode: the single legacy interval.
            // #80: abort a hung re-clustering pass. One pass is in flight at a
            // time, so a wedged one silently stops maintenance forever; past
            // the ceiling we abort the task (its in-flight work cancels at the
            // next await; rewrite commits are atomic, so the layout is simply
            // left for the next pass) and clear the slot.
            if let (Some(t), Some(started), Some(limit)) = (
                recluster_task.as_ref(),
                recluster_started,
                recluster_watchdog,
            ) {
                // PROGRESS-guarded, not a fixed wall-clock cap. The ceiling
                // exists to catch a WEDGED pass; a pass that is merely slow is
                // doing exactly what it was asked to. At 2B rows one legitimate
                // bin merge can outlast any fixed ceiling — especially with S3
                // write retries — and aborting mid-merge discards every byte it
                // uploaded, so the layout never improves and the next pass hits
                // the same wall. That is what happened on 2026-08-03: four trips,
                // zero completed settle passes, depth pinned at 30 for hours.
                //
                // Same lesson the drain tail learned when its fixed cap became
                // progress-guarded. A time budget on work whose duration scales
                // with data size will always break at some scale.
                let progressed = recluster_progress.load(std::sync::atomic::Ordering::Relaxed)
                    != recluster_progress_seen;
                if progressed {
                    // Committed at least one bin since the last check: restart
                    // the clock rather than killing useful work.
                    recluster_progress_seen =
                        recluster_progress.load(std::sync::atomic::Ordering::Relaxed);
                    recluster_started = Some(tokio::time::Instant::now());
                } else if !t.is_finished() && started.elapsed() > limit {
                    tracing::error!(
                        limit_secs = limit.as_secs(),
                        "re-clustering pass made no committed progress within the watchdog \
                         ceiling; aborting"
                    );
                    metrics::counter!("siglake_compactor_watchdog_trips_total",
                        "stage" => "recluster")
                    .increment(1);
                    t.abort();
                    let t = recluster_task.take().expect("checked is_some");
                    let _ = t.await; // resolves promptly (Cancelled) after abort
                    recluster_started = None;
                    draining = false;
                    drain_passes = 0;
                }
            }
            // Harvest a finished background pass (non-blocking; the await
            // resolves immediately once is_finished).
            if recluster_task.as_ref().is_some_and(|t| t.is_finished()) {
                let t = recluster_task.take().expect("checked is_some");
                recluster_started = None;
                let throttled_run = task_throttled;
                match t.await {
                    // Files removed ⇒ more to do; the next cycle schedules the
                    // next pass immediately — until the burst cap, then pace
                    // via the interval. The drain still runs every cycle.
                    Ok(Ok(removed)) => {
                        drain_passes += 1;
                        // Burst (back-to-back passes) only while ingest is idle
                        // (n == 0) AND not throttled — under backlog we ration
                        // compaction to the throttled slot and let the drain run.
                        draining = !throttled_run
                            && removed > 0
                            && n == 0
                            && drain_passes < DRAIN_BURST_CAP;
                        if removed == 0 {
                            if drain_passes > 1 {
                                tracing::info!(
                                    passes = drain_passes,
                                    "tier-2 re-clustering drained to quiescence"
                                );
                            }
                            drain_passes = 0;
                        }
                    }
                    Ok(Err(e)) => {
                        // Debug (`?`) prints the full anyhow context chain (+
                        // backtrace under RUST_BACKTRACE); Display (`%`) would
                        // collapse it to the outermost `recluster_pass` context
                        // and hide the real cause.
                        tracing::error!(error = ?e, "re-clustering pass failed");
                        draining = false;
                        drain_passes = 0;
                    }
                    Err(join_err) => {
                        tracing::error!(error = ?join_err, "re-clustering task panicked");
                        draining = false;
                        drain_passes = 0;
                    }
                }
            }
            if let (Some(cfg), None) = (
                self.recluster.as_ref().filter(|_| self.role.maintains()),
                recluster_task.as_ref(),
            ) {
                let now = tokio::time::Instant::now();
                // #63: passes yield between bins when the sealed backlog
                // crosses the preemption threshold (default: the backpressure
                // gate). With the drain running concurrently, a lone sealed
                // segment no longer needs to abort maintenance — preemption is
                // for real backlog pressure, where finishing every remaining
                // bin would steal IO the drain needs.
                let preempt: Option<std::sync::Arc<dyn Fn() -> bool + Send + Sync>> = {
                    let min = preempt_sealed_min();
                    if min == 0 {
                        None
                    } else {
                        let wal_root = self.wal_dir.clone();
                        Some(std::sync::Arc::new(move || {
                            pending_sealed_count(&wal_root) >= min
                        }))
                    }
                };
                let opts: Option<LeveledPassOptions> = if throttled {
                    Some(LeveledPassOptions {
                        allowed_levels: Some(vec![0]),
                        max_total_bins: Some(backpressure_max_bins()),
                        preempt: None,
                        on_bin: None,
                        bin_concurrency: None,
                    })
                } else if let Some(levels) = &cfg.levels {
                    if draining {
                        Some(LeveledPassOptions {
                            preempt: preempt.clone(),
                            ..Default::default()
                        })
                    } else {
                        let due: Vec<usize> = (0..levels.level_ceilings.len())
                            .filter(|&l| {
                                level_last_run[l]
                                    .is_none_or(|t| t.elapsed() >= cfg.level_interval(l))
                            })
                            .collect();
                        if due.is_empty() {
                            None
                        } else {
                            Some(LeveledPassOptions {
                                allowed_levels: Some(due),
                                max_total_bins: None,
                                preempt: preempt.clone(),
                                on_bin: None,
                                bin_concurrency: None,
                            })
                        }
                    }
                } else if draining || last_recluster.elapsed() >= cfg.interval {
                    Some(LeveledPassOptions {
                        preempt: preempt.clone(),
                        ..Default::default()
                    })
                } else {
                    None
                };
                if let Some(opts) = opts {
                    last_recluster = now;
                    // Stamp the levels this pass covered so their tickers restart.
                    match &opts.allowed_levels {
                        Some(levels) => {
                            for &l in levels {
                                if let Some(slot) = level_last_run.get_mut(l) {
                                    *slot = Some(now);
                                }
                            }
                        }
                        None => level_last_run.iter_mut().for_each(|s| *s = Some(now)),
                    }
                    task_throttled = throttled;
                    recluster_started = Some(tokio::time::Instant::now());
                    recluster_progress_seen =
                        recluster_progress.load(std::sync::atomic::Ordering::Relaxed);
                    let me = std::sync::Arc::clone(&self);
                    let progress = std::sync::Arc::clone(&recluster_progress);
                    let mut opts = opts;
                    opts.on_bin = Some(std::sync::Arc::new(move || {
                        progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }));
                    recluster_task = Some(tokio::spawn(async move {
                        me.run_recluster_once_shaped(&opts).await
                    }));
                }
            }
            // Throttled backlog slot: we ran one bounded merge; loop straight back to
            // draining (skip lower-priority expiry/delete + the sleep) so the commit
            // path keeps the lion's share of cycles under load.
            if throttled {
                continue;
            }
            // Mid-drain: re-check for commits and run the next consolidation pass
            // immediately rather than idling for `poll_interval` or interleaving
            // lower-priority maintenance. The loop top still runs `run_once` first,
            // so resumed ingest preempts the drain. Only while no pass is in
            // flight — with a background pass running there is nothing to
            // hurry toward, and skipping the sleep would busy-spin the loop
            // for the whole multi-minute merge.
            if draining && recluster_task.is_none() {
                continue;
            }
            if self.role.maintains()
                && committed_retention().is_some()
                && last_retention.elapsed() >= RETENTION_INTERVAL
                && self.maintenance_lease("retention").await
            {
                // Cadence is measured start-to-start. A large backlog that
                // consumes most of the bounded run must not then sit idle for
                // another 300 seconds before it can keep draining.
                let retention_started = tokio::time::Instant::now();
                let ran = self
                    .with_mirror_reconciliation_lease(async {
                        let _ret_guard = StageTimer(
                            "siglake_compactor_retention_duration_seconds",
                            std::time::Instant::now(),
                        );
                        bounded(drain_watchdog, self.run_retention_once()).await
                    })
                    .await
                    .is_some();
                if ran {
                    last_retention = retention_started;
                }
            }
            if self.delete_tasks_enabled
                && self.role.maintains()
                && self.maintenance_lease("delete_tasks").await
            {
                let _del_guard = StageTimer(
                    "siglake_compactor_delete_tasks_duration_seconds",
                    std::time::Instant::now(),
                );
                match bounded(drain_watchdog, self.run_delete_tasks_once()).await {
                    Some(Err(e)) => tracing::error!(error = %e, "delete-task sweep failed"),
                    None => {
                        tracing::error!("delete-task sweep exceeded the watchdog ceiling");
                        metrics::counter!("siglake_compactor_watchdog_trips_total",
                            "stage" => "delete_tasks")
                        .increment(1);
                    }
                    Some(Ok(_)) => {}
                }
            }
            // Idle only: sleep between WAL polls. With active ingest (concurrent
            // mode, n > 0), loop straight back to draining the next batch — the
            // interleaved recluster above is interval-paced, not per-cycle.
            if n == 0 {
                // An empty cycle does NOT mean idle. Several drains claim from one
                // queue, so a node that loses the race sees exactly what a node
                // with no work sees -- and then waits a full interval before
                // trying again. The 2026-08-14 ceiling round had 320 such cycles
                // while the backlog GREW at +294,956 rows/s.
                //
                // Back off from a short base so a race-loser retries almost
                // immediately, doubling to `poll_interval` so a genuinely idle
                // drain still polls at the configured cadence instead of spinning
                // on the catalog. Worth stating plainly: at the default 1s
                // interval this is only ~5% of the fleet's unexplained idle time,
                // not the bulk of it -- it is correct, not a throughput fix.
                // Time the ACTUAL sleep. Recording `idle_backoff` itself would
                // report the delay we ASKED for, which is not evidence about
                // where wall time went -- a loaded runtime can overshoot it
                // arbitrarily, and that overshoot is exactly the kind of thing
                // this accounting exists to catch.
                let _sleep_t = std::time::Instant::now();
                sleep(idle_backoff).await;
                metrics::histogram!("siglake_compactor_idle_sleep_duration_seconds")
                    .record(_sleep_t.elapsed().as_secs_f64());
                idle_backoff = (idle_backoff * 2).min(poll_interval);
            }
        }
    }

    #[allow(dead_code)]
    async fn commit_claimed_legacy(&self, claimed: &[PathBuf]) -> Result<usize> {
        Ok(
            commit_claimed(&self.ice, &self.wal_dir, claimed, &CommitTarget::Events)
                .await?
                .segments,
        )
    }

    fn select_sealed_for_cycle(&self, sealed: &[PathBuf]) -> Result<Vec<PathBuf>> {
        let mut selected = Vec::new();
        let mut total_bytes = 0u64;
        for path in sealed {
            let bytes = std::fs::metadata(path)
                .with_context(|| format!("stat {}", path.display()))?
                .len();
            let hit_segment_limit =
                self.fs_batch.max_segments > 0 && selected.len() >= self.fs_batch.max_segments;
            let hit_byte_limit = self.fs_batch.max_bytes > 0
                && total_bytes.saturating_add(bytes) > self.fs_batch.max_bytes;
            if !selected.is_empty() && (hit_segment_limit || hit_byte_limit) {
                break;
            }
            total_bytes = total_bytes.saturating_add(bytes);
            selected.push(path.clone());
        }
        Ok(selected)
    }
}

/// Peek the on-disk sealed queue for the commit-accumulation gate
/// (BIG-4 `#4b`): total bytes + age of the oldest segment. Mirrors
/// [`siglake_storage::catalog_claim::SqlSegmentClaim::peek_pending`] for
/// the FS path. Best-effort — a segment that vanishes mid-stat (claimed
/// by a concurrent cycle, swept) is skipped rather than erroring; the
/// gate only needs an approximate size.
/// Count sealed WAL segments awaiting drain across the legacy root + every
/// tenant subdir (+ their per-index dirs). Cheap directory listing; free
/// function so the #63 preemption closure can capture just the WAL root.
fn pending_sealed_count(wal_root: &Path) -> usize {
    let mut total = list_sealed(wal_root).map(|v| v.len()).unwrap_or(0);
    if let Ok(tenants) = list_tenant_dirs(wal_root) {
        for (_tenant, dir) in tenants {
            total += list_sealed(&dir).map(|v| v.len()).unwrap_or(0);
            if let Ok(indexes) = list_index_dirs(&dir) {
                for (_index, index_dir) in indexes {
                    total += list_sealed(&index_dir).map(|v| v.len()).unwrap_or(0);
                }
            }
        }
    }
    total
}

/// #63: sealed-backlog threshold at which a leveled pass yields BETWEEN BINS.
/// With the drain running concurrently to the pass (the A.2 scheduler),
/// preemption is no longer how freshness is served — it exists to stop a long
/// multi-bin pass from stealing IO once a REAL backlog builds. Default: the
/// backpressure gate ([`max_sealed_for_recluster`]), or 256 if that gate is
/// disabled. `0` disables preemption. The throttled backpressure slot is
/// exempt — it exists *because* the backlog is high and is already rationed to
/// one bin.
fn preempt_sealed_min() -> usize {
    std::env::var("SIGLAKE_COMPACTOR_PREEMPT_SEALED_MIN")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| match max_sealed_for_recluster() {
            0 => 256,
            gate => gate,
        })
}

/// The identity a per-index WAL directory may be drained under — the verdict of
/// [`Compactor::claim_index_wal_dir`] and of its mirror twin
/// [`Compactor::claim_mirrored_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum WalDirClaim {
    /// Drain it, and check every segment against this table (#2693). A segment
    /// with no identity of its own drains: the marker vouches for it.
    Owned(String),
    /// Drain it, and admit ONLY segments that name this table themselves
    /// (#2729): the mirror prefix has been stamped for a dropped incarnation at
    /// some point, so it cannot vouch for an unstamped object. Mirror path
    /// only — the filesystem path moves the dropped incarnation's segments out
    /// from under the marker instead.
    OwnedStrict(String),
    /// Drain it: the index name resolves to no table, so there is no identity
    /// to check anything against.
    Unidentified,
    /// Refuse the whole directory: it holds a dropped incarnation's segments,
    /// now quarantined under `stale/`. On the mirror path this is only the
    /// answer to an unreadable marker — a claim that cannot be verified is
    /// released for retry, never committed unverified.
    Refused,
}

/// What a mirrored segment's fetch produced: rows to commit, or a refusal that
/// no retry can change.
enum SegmentFetch {
    Ready(Vec<RecordBatch>),
    /// Quarantine the claim on this cycle, with this reason (#2746). Terminal
    /// because a table uuid is never reused, so re-fetching the bytes can only
    /// reach the same verdict.
    Refused(&'static str),
}

/// One segment's fetch-and-classify step on the catalog-claim drain.
type SegmentFetchFut = futures::future::BoxFuture<'static, anyhow::Result<SegmentFetch>>;

/// #2693: the object's own frame header names a table this index no longer
/// resolves to.
const STALE_HEADER_REFUSAL: &str =
    "the mirrored segment's frame header names a table this index no longer resolves to";
/// #2729: no identity in the object, under a prefix that has named a dropped
/// incarnation and so cannot vouch for it.
const UNSTAMPED_REFUSAL: &str =
    "the mirrored segment carries no table identity, under a prefix that has named a dropped \
     incarnation";

/// #2693: drop the sealed segments whose frame header names a table other than
/// `owner`, holding each one under `stale/<its uuid>/`.
///
/// The directory-level marker (#2661) is re-stamped for the replacement the
/// first time a drain visits the directory, so from that moment an ingester
/// still holding the dropped incarnation's writer seals into a directory that
/// vouches for the wrong table. A segment's own header does not move with the
/// directory, so this is the gate that actually holds. Unstamped segments
/// (legacy, or a writer with no identity to bind) drain as before.
///
/// A quarantine failure keeps the segment OUT of the batch: refusing to commit
/// is the safe half, and the move can be retried next cycle.
fn retain_owned_sealed(
    sealed: Vec<PathBuf>,
    owner: Option<&str>,
    tenant: &str,
    index: &str,
) -> Vec<PathBuf> {
    let Some(uuid) = owner else {
        return sealed;
    };
    let mut kept = Vec::with_capacity(sealed.len());
    for path in sealed {
        let siglake_wal::WalOwner::Stale(dropped) =
            siglake_wal::classify_segment_owner(&path, uuid)
        else {
            kept.push(path);
            continue;
        };
        match siglake_wal::quarantine_stale_segment(&path, &dropped) {
            Ok(held) => tracing::warn!(
                tenant,
                segment = %path.display(),
                held = %held.display(),
                dropped_table = %dropped,
                live_table = %uuid,
                "WAL segment was sealed for a dropped incarnation of this index by a writer \
                 that outlived it; held under stale/ instead of committed"
            ),
            Err(e) => tracing::warn!(
                tenant,
                segment = %path.display(),
                error = %e,
                "could not quarantine a dropped incarnation's segment; leaving it unclaimed"
            ),
        }
        metrics::counter!(
            "siglake_compactor_wal_stale_segments_total",
            "tenant" => tenant.to_string(),
            "index" => index.to_string()
        )
        .increment(1);
    }
    kept
}

#[cfg(test)]
mod retain_owned_sealed_tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    #[test]
    fn a_refused_filesystem_segment_counts_under_its_tenant_and_index() {
        let tmp = tempfile::tempdir().unwrap();
        let dropped = uuid::Uuid::now_v7();
        let live = uuid::Uuid::now_v7();
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            tmp.path(),
            "stale-writer",
            1,
            Duration::from_secs(60),
        )
        .unwrap();
        writer.bind_table_uuid(Some(dropped)).unwrap();
        writer
            .append_events(&[siglake_core::Event::now("stale")])
            .unwrap()
            .expect("one row seals the segment");

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            assert!(
                retain_owned_sealed(
                    list_sealed(tmp.path()).unwrap(),
                    Some(&live.to_string()),
                    "acme",
                    "logs",
                )
                .is_empty(),
                "the stale segment is refused"
            );
        }

        let refusals: Vec<(Vec<(String, String)>, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "siglake_compactor_wal_stale_segments_total"
            })
            .map(|(key, _, _, value)| {
                let mut labels: Vec<(String, String)> = key
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                labels.sort();
                let DebugValue::Counter(count) = value else {
                    panic!("{} is a counter", key.key().name())
                };
                (labels, count)
            })
            .collect();
        assert_eq!(
            refusals,
            vec![(
                vec![
                    ("index".to_string(), "logs".to_string()),
                    ("tenant".to_string(), "acme".to_string()),
                ],
                1,
            )],
            "one refusal, attributed to the filesystem directory's tenant and index"
        );
    }
}

fn fs_pending_stats(sealed: &[PathBuf]) -> (u64, Duration) {
    let mut bytes = 0u64;
    let mut oldest = Duration::ZERO;
    for p in sealed {
        let Ok(meta) = std::fs::metadata(p) else {
            continue;
        };
        bytes = bytes.saturating_add(meta.len());
        if let Ok(modified) = meta.modified() {
            if let Ok(age) = modified.elapsed() {
                oldest = oldest.max(age);
            }
        }
    }
    (bytes, oldest)
}

/// Sealed-backlog threshold above which the run loop yields the whole cycle to
/// draining instead of running maintenance (recluster/expire/delete). This is the
/// backpressure-aware coexistence the continuous-compaction design calls for
/// (A.3): the commit path always wins under load so a fast drain can't be starved
/// by compaction. `0` disables the gate (legacy: maintenance always eligible).
/// Default 256 ≈ a few drain batches of slack before compaction stands down.
fn max_sealed_for_recluster() -> usize {
    std::env::var("SIGLAKE_COMPACTOR_MAX_SEALED_FOR_RECLUSTER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
}

/// Graded backpressure budget (design A.4.3): when the drain is behind (sealed
/// backlog over the gate), run ONE *throttled* maintenance pass every this-many
/// backlogged cycles instead of skipping compaction entirely — so files stay
/// bounded even under *sustained* ingest (the static all-or-nothing gate let them
/// grow to ~778 in the 200G run) without re-starving the drain. `0` restores the
/// legacy hard-skip (no compaction at all while backlogged). Default 4 ≈ compaction
/// gets ~1 in 4 backlogged cycles, and each throttled pass is a single bounded merge.
/// Bins the *throttled* backpressure slot may merge (`allowed_levels = [0]`).
///
/// This is the remaining structural limiter on compaction throughput under
/// sustained ingest. The 2026-08-05 200G round found 13 of 14 productive passes
/// running exactly one bin, which leaves the (93%-efficient) concurrent
/// executor idle precisely when compaction is falling behind — and this slot
/// caps it at one by construction.
///
/// Default 1 = the historical behavior, deliberately. This slot exists to keep
/// files bounded WITHOUT re-starving the drain (the un-throttled leveled path
/// once starved it to ~8k rows/s), so raising it trades the drain's headroom
/// for compaction progress and must be measured, not assumed. Raise it only
/// together with `SIGLAKE_COMPACTOR_BIN_CONCURRENCY` and enough compactor
/// memory for that many in-flight bins.
fn backpressure_max_bins() -> usize {
    backpressure_max_bins_from(
        std::env::var("SIGLAKE_COMPACTOR_BACKPRESSURE_MAX_BINS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the backpressure bin cap from its raw environment value.
///
/// Pure so override behavior can be tested without mutating process-global
/// state observed by parallel tests in this binary.
fn backpressure_max_bins_from(configured: Option<&str>) -> usize {
    if let Some(n) = configured
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
    {
        return n;
    }
    // DERIVED, not 1. Capping in-flight bins at one while the memory/CPU
    // derivation says the pod can run four is a throttle with no memory
    // justification — and it is what keeps compaction behind precisely when
    // ingest is running, which is the whole of the "no interactive scans
    // during full-rate ingest" limitation.
    //
    // Measured 2026-08-27 at 1TB, fleet otherwise identical to its baseline:
    // the windowed browse during ingest went from 40.9s to 0.13s, depth at
    // 1.0B from 86 to 57, and convergence after ingest from ~100 minutes to
    // ~30 — with accept unaffected at ~1.11M rows/s and drains at 4-6 GB RSS,
    // no OOM kills.
    //
    // Deriving rather than hardcoding 4 is the safe half: `derive_bin_
    // concurrency` returns 1 for the packaged 1Gi/2-CPU compactor and 1 when
    // there is no cgroup limit at all, so a small install is unchanged and only
    // a pod with the memory to run concurrent bins stops being throttled.
    siglake_storage::iceberg::derived_bin_concurrency()
}

fn backpressure_compact_every() -> u64 {
    std::env::var("SIGLAKE_COMPACTOR_BACKPRESSURE_COMPACT_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(4)
}

/// #80 drain watchdog ceiling. The Phase-4 round saw the embedded compactor
/// wedge PERMANENTLY when the instance's credential/network path died
/// mid-drain: `run_once` hung inside an S3 call that never errored, the run
/// loop never cycled again, and bulk accepts stayed backpressure-blocked
/// until a container restart — long after the outage itself had cleared.
/// A drain cycle is bounded by design (`drain_cycle_budget`, default 30s,
/// plus commit tails), so a cycle that exceeds this ceiling is hung, not
/// slow. On a trip the loop aborts the cycle (in-flight commits are owned by
/// the cycle's `FuturesUnordered` and cancel with it), quarantines the
/// stranded `processing/` claims, and keeps cycling — the #81 disposition
/// proof then deletes any claim whose commit had already landed and requeues
/// the rest, so the abort stays exactly-once. `SIGLAKE_DRAIN_WATCHDOG_SECS`
/// (default 600; `0` disables; min 60).
/// Run `fut` under an optional watchdog ceiling: `Some(output)` normally,
/// `None` on a trip. `ceiling: None` never times out.
async fn bounded<F: std::future::Future>(ceiling: Option<Duration>, fut: F) -> Option<F::Output> {
    match ceiling {
        None => Some(fut.await),
        Some(limit) => tokio::time::timeout(limit, fut).await.ok(),
    }
}

/// Default of `SIGLAKE_DRAIN_WATCHDOG_SECS`, named because the non-terminal
/// delete-task bound is 2x the ceiling this resolves to
/// ([`delete_task_stall_bound_from`]): with the default spelled twice, raising
/// the drain ceiling would silently leave that bound measuring the old one.
const DRAIN_WATCHDOG_DEFAULT_SECS: u64 = 600;

fn drain_watchdog_ceiling() -> Option<Duration> {
    watchdog_ceiling_from(
        std::env::var("SIGLAKE_DRAIN_WATCHDOG_SECS").ok().as_deref(),
        DRAIN_WATCHDOG_DEFAULT_SECS,
    )
}

/// Pure parse for the watchdog ceilings: unset/unparseable → default, `0` →
/// disabled, anything else clamped to ≥60s (a ceiling below a legitimate
/// slow cycle would abort healthy work).
fn watchdog_ceiling_from(v: Option<&str>, default_secs: u64) -> Option<Duration> {
    let secs = v
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default_secs);
    (secs > 0).then(|| Duration::from_secs(secs.max(60)))
}

// ---------------------------------------------------------------------------
// Non-terminal delete tasks: the observation, the bound and what it reports.
// The signal designed under #2114 and implemented by #2245.
// ---------------------------------------------------------------------------

/// `state` label values of the two non-terminal delete-task series, and the
/// only two an alert can spell. Held to the pre-registration catalog by
/// `delete_task_series_are_preregistered`.
const DELETE_TASK_STATE_RUNNING: &str = "running";
const DELETE_TASK_STATE_PENDING_CLAIMED: &str = "pending_claimed";

// The two metric names are spelled as literals at their macro call sites
// (`emit_delete_task_observation`), because `scripts/check-chart.py` reads
// `metrics::{counter,gauge}!("<literal>"` to know what the code emits at all:
// behind a constant, the alert and the dashboard panel would both look like
// readers of a metric nothing exports.
//
// - `siglake_compactor_delete_tasks_stalled_total` is the ALERTING arm:
//   observations of a stalled task, NOT stalled tasks. Its rate is
//   proportional to sweep frequency and meaningless as a rate; only
//   `increase(...) > 0` says anything, which is what keeps the alert's window
//   independent of `SIGLAKE_DRAIN_WATCHDOG_SECS`.
// - `siglake_compactor_delete_tasks_nonterminal` is DASHBOARD-ONLY. A live pod
//   retains its last gauge value when the delete stage stops running
//   (execution disabled, lease lost, watchdog trip), so alerting on it reads
//   stale-healthy exactly when the truth is "unobserved". check-chart.py bans
//   the name from alert expressions so a later edit fails the chart check
//   instead of shipping that rule.

/// Task WARN lines one namespace observation prints before it summarises the
/// rest. A namespace with a hundred stranded tasks must not print a hundred
/// lines every sweep.
const DELETE_TASK_STALL_LINE_CAP: usize = 10;

/// The bound a non-terminal delete task is measured against: 2x the resolved
/// drain-watchdog ceiling. Pure twin of [`delete_task_stall_bound`]; tests
/// drive this one.
///
/// One ceiling is the enforced execution window — claim, `Running` write,
/// rewrite, commit and terminal write all happen inside the single
/// `bounded(drain_watchdog, run_delete_tasks_once())` future. The second is
/// slack, for two named reasons: `tokio::time::timeout` cancels only at an
/// await point, so a stage wedged in a synchronous stretch overruns its ceiling
/// rather than being cut; and the claim's `last_modified` is the STORE's clock,
/// so the comparison carries clock skew.
///
/// `None` (the watchdog disabled with `0`) means there is no bound at all and
/// nothing is ever stalled: the stage may legitimately run for hours, and
/// paging against an invented ceiling on a pod whose operator deliberately
/// removed the ceiling is a false page, not a signal.
fn delete_task_stall_bound_from(v: Option<&str>) -> Option<Duration> {
    watchdog_ceiling_from(v, DRAIN_WATCHDOG_DEFAULT_SECS).map(|ceiling| ceiling * 2)
}

fn delete_task_stall_bound() -> Option<Duration> {
    delete_task_stall_bound_from(std::env::var("SIGLAKE_DRAIN_WATCHDOG_SECS").ok().as_deref())
}

/// What one non-terminal observation of one task supports saying about it.
///
/// `Stalled` means only that the task has been non-terminal for longer than any
/// execution this compactor allows. It does not prove the executor is dead, and
/// it says nothing about whether the rewrite committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteTaskStall {
    /// Claimed inside the bound: indistinguishable from an execution in
    /// progress, which is what it usually is.
    Healthy,
    /// Non-terminal past the bound.
    Stalled,
    /// No age to compare: no claim, no `last_modified`, an age the two clocks
    /// disagree about the sign of, or no bound at all. Never `Healthy` — an
    /// unknown is not an all-clear.
    Unknown,
}

/// The `state` series one non-terminal task belongs to, or `None` for a task
/// this signal does not cover.
///
/// An unclaimed `pending` task is that `None`: it is identical to a task
/// waiting for the next sweep, or to one no sweep will ever run because
/// delete-task execution is switched off, and neither is a stranding this can
/// detect.
fn delete_task_series(task: &NonTerminalDeleteTask) -> Option<&'static str> {
    match (task.state, task.claim) {
        (DeleteTaskState::Running, _) => Some(DELETE_TASK_STATE_RUNNING),
        (DeleteTaskState::Pending, ObservedDeleteTaskClaim::Present { .. }) => {
            Some(DELETE_TASK_STATE_PENDING_CLAIMED)
        }
        _ => None,
    }
}

/// Wall seconds since the claim OBJECT was written, or `None` when there is no
/// age to be had.
///
/// This is an upper bound on execution duration, never a running duration: the
/// claim is created immediately before the `Running` write, and it is never
/// released (#1471, #2129), so the same number keeps growing after the task
/// strands. That is exactly what makes it usable as a stall signal and exactly
/// why it must never be printed as "running for".
///
/// The observer's clock and the store's clock are two clocks. A negative age is
/// therefore possible and is `None` — unknown — rather than 0.
fn delete_task_claim_age_seconds(
    observed_at: chrono::DateTime<chrono::Utc>,
    task: &NonTerminalDeleteTask,
) -> Option<i64> {
    match task.claim {
        ObservedDeleteTaskClaim::Present {
            last_modified: Some(at),
        } => match (observed_at - at).num_seconds() {
            age if age >= 0 => Some(age),
            _ => None,
        },
        _ => None,
    }
}

/// Pure classifier: one observation against one bound. No clock of its own, no
/// env, no I/O.
fn classify_nonterminal_delete_task(
    observed_at: chrono::DateTime<chrono::Utc>,
    task: &NonTerminalDeleteTask,
    bound: Option<Duration>,
) -> DeleteTaskStall {
    let Some(bound) = bound else {
        return DeleteTaskStall::Unknown;
    };
    match delete_task_claim_age_seconds(observed_at, task) {
        None => DeleteTaskStall::Unknown,
        Some(age) if age as u64 > bound.as_secs() => DeleteTaskStall::Stalled,
        Some(_) => DeleteTaskStall::Healthy,
    }
}

/// Pure rate limit: is something last done at `last` due again at `now`?
///
/// Never done is due. A `last` in the future is due too — a clock stepping
/// backwards must not silence the log until it catches up.
fn due(
    last: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
    every: Duration,
) -> bool {
    match last {
        None => true,
        Some(last) => match (now - last).to_std() {
            Ok(elapsed) => elapsed >= every,
            Err(_) => true,
        },
    }
}

/// One WARN line about one stalled task.
///
/// There is deliberately no `predicate_sql` field and no claimant uuid here, so
/// no emitter can print either: recovery needs the task id and the index, and
/// #2128's `GET /api/v1/delete-tasks/{id}` has the rest.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StalledDeleteTaskLine {
    tenant: String,
    index_id: String,
    task_id: uuid::Uuid,
    state: &'static str,
    claim_age_seconds: i64,
    submitted_age_seconds: i64,
    bound_seconds: u64,
}

/// Per-namespace totals: the DEBUG line every complete observation prints, plus
/// the WARN summary when the line cap held stalled tasks back.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeleteTaskNamespaceSummary {
    tenant: String,
    nonterminal: usize,
    stalled: usize,
    /// Non-terminal, covered by a series, and with no age to judge.
    unknown: usize,
    /// Stalled tasks whose line the cap suppressed. Drives the WARN summary.
    capped: usize,
    /// Stalled tasks whose line the per-task rate limit suppressed. Silence is
    /// the point of the rate limit, so this is DEBUG only.
    rate_limited: usize,
}

/// A namespace whose non-terminal set is not known. Never a zero.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DeleteTaskUnknownCoverage {
    tenant: String,
    reason: String,
    uninspected: usize,
}

/// Everything one sweep's observation emits, computed before anything is
/// logged or recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DeleteTaskObservationReport {
    lines: Vec<StalledDeleteTaskLine>,
    summaries: Vec<DeleteTaskNamespaceSummary>,
    unknown_coverage: Vec<DeleteTaskUnknownCoverage>,
    /// Counter increments by `state`, from COMPLETE namespace observations
    /// only.
    increments: BTreeMap<&'static str, u64>,
    /// Gauge values by `state`, both series and including 0 — or `None`, which
    /// means publish nothing and RETAIN the previous value.
    ///
    /// The gauge carries no namespace label, so it is one process-wide number:
    /// setting it per namespace would let a healthy tenant overwrite another
    /// tenant's count with zero. It is therefore the aggregate of every
    /// namespace, published only when every one of them was covered
    /// completely; a partial or cancelled pass leaves the last full number in
    /// place, because a lowered gauge is indistinguishable from recovery.
    gauge: Option<BTreeMap<&'static str, u64>>,
    bound: Option<Duration>,
}

/// One namespace's observation as the planner takes it: the observation, or why
/// there isn't one.
type DeleteTaskObservations = [(
    String,
    std::result::Result<NonTerminalDeleteTaskObservation, String>,
)];

/// Turn a sweep's observations into logs and metric movements. Pure apart from
/// the rate-limit map it advances, which is why the dedupe rule is testable
/// without a clock.
///
/// `logged` maps a task id to the last time its WARN line was printed. A pod
/// restart empties it, so the next observation re-reports every stalled task
/// once: this is a signal about tasks, not a deduplicated incident stream.
fn plan_delete_task_observation(
    now: chrono::DateTime<chrono::Utc>,
    bound: Option<Duration>,
    observations: &DeleteTaskObservations,
    logged: &mut HashMap<uuid::Uuid, chrono::DateTime<chrono::Utc>>,
) -> DeleteTaskObservationReport {
    let mut report = DeleteTaskObservationReport {
        bound,
        ..Default::default()
    };
    let mut totals: BTreeMap<&'static str, u64> = BTreeMap::new();
    totals.insert(DELETE_TASK_STATE_RUNNING, 0);
    totals.insert(DELETE_TASK_STATE_PENDING_CLAIMED, 0);
    let mut all_complete = true;
    let mut observed_ids: Vec<uuid::Uuid> = Vec::new();

    for (tenant, observation) in observations {
        let observation = match observation {
            Ok(observation) if observation.complete => observation,
            Ok(observation) => {
                all_complete = false;
                report.unknown_coverage.push(DeleteTaskUnknownCoverage {
                    tenant: tenant.clone(),
                    reason: "the observation did not inspect every non-terminal task".to_string(),
                    uninspected: observation.uninspected,
                });
                continue;
            }
            Err(reason) => {
                all_complete = false;
                report.unknown_coverage.push(DeleteTaskUnknownCoverage {
                    tenant: tenant.clone(),
                    reason: reason.clone(),
                    uninspected: 0,
                });
                continue;
            }
        };
        let mut summary = DeleteTaskNamespaceSummary {
            tenant: tenant.clone(),
            nonterminal: observation.tasks.len(),
            stalled: 0,
            unknown: 0,
            capped: 0,
            rate_limited: 0,
        };
        let mut lines_here = 0usize;
        for task in &observation.tasks {
            observed_ids.push(task.task_id);
            let Some(state) = delete_task_series(task) else {
                continue;
            };
            *totals.entry(state).or_default() += 1;
            match classify_nonterminal_delete_task(now, task, bound) {
                DeleteTaskStall::Healthy => {}
                DeleteTaskStall::Unknown => summary.unknown += 1,
                DeleteTaskStall::Stalled => {
                    summary.stalled += 1;
                    // The counter counts observations of a stalled task, not
                    // lines: it must move on every complete observation even
                    // when the log stays quiet, or an operator watching
                    // `increase()` would see the stall stop.
                    *report.increments.entry(state).or_default() += 1;
                    if !due(
                        logged.get(&task.task_id).copied(),
                        now,
                        bound.unwrap_or_default(),
                    ) {
                        summary.rate_limited += 1;
                        continue;
                    }
                    if lines_here >= DELETE_TASK_STALL_LINE_CAP {
                        // Not recorded as logged, so the cap defers the line
                        // rather than dropping it.
                        summary.capped += 1;
                        continue;
                    }
                    lines_here += 1;
                    logged.insert(task.task_id, now);
                    report.lines.push(StalledDeleteTaskLine {
                        tenant: tenant.clone(),
                        index_id: task.index_id.clone(),
                        task_id: task.task_id,
                        state,
                        claim_age_seconds: delete_task_claim_age_seconds(now, task).unwrap_or(-1),
                        submitted_age_seconds: (now - task.created_at).num_seconds(),
                        bound_seconds: bound.map(|b| b.as_secs()).unwrap_or(0),
                    });
                }
            }
        }
        report.summaries.push(summary);
    }

    if all_complete {
        report.gauge = Some(totals);
        // Only a complete pass over every namespace proves a task is gone, so
        // only then is dropping its rate-limit entry safe. Without this the map
        // grows for the life of the process.
        logged.retain(|task_id, _| observed_ids.contains(task_id));
    }
    report
}

/// Log and record a planned report. The only place these two signals are
/// emitted, and the only place the fields of a WARN line are chosen.
fn emit_delete_task_observation(report: &DeleteTaskObservationReport) {
    for line in &report.lines {
        tracing::warn!(
            tenant = %line.tenant,
            index_id = %line.index_id,
            task_id = %line.task_id,
            state = line.state,
            claim_age_seconds = line.claim_age_seconds,
            submitted_age_seconds = line.submitted_age_seconds,
            bound_seconds = line.bound_seconds,
            claim = "present",
            age_source = "claim_object",
            observation = "complete",
            "delete task has been non-terminal for longer than the sweep watchdog bound; \
             the claim age bounds execution duration from above and proves neither that the \
             executor is gone nor whether its rewrite committed. Recovery is an explicit \
             resubmission under a new task id."
        );
    }
    for summary in &report.summaries {
        if summary.capped > 0 {
            tracing::warn!(
                tenant = %summary.tenant,
                stalled = summary.stalled,
                reported = summary.stalled - summary.capped - summary.rate_limited,
                suppressed = summary.capped,
                "more delete tasks are non-terminal past the bound than this observation \
                 lists; the rest are counted, not printed"
            );
        }
        tracing::debug!(
            tenant = %summary.tenant,
            nonterminal = summary.nonterminal,
            stalled = summary.stalled,
            unknown_age = summary.unknown,
            rate_limited = summary.rate_limited,
            bound_seconds = report.bound.map(|b| b.as_secs()).unwrap_or(0),
            "delete-task non-terminal observation"
        );
    }
    for unknown in &report.unknown_coverage {
        tracing::warn!(
            tenant = %unknown.tenant,
            uninspected = unknown.uninspected,
            reason = %unknown.reason,
            "the set of non-terminal delete tasks is UNKNOWN for this namespace; no count is \
             published for this sweep, and the absence of a stalled report is not health"
        );
    }
    for (state, n) in &report.increments {
        metrics::counter!("siglake_compactor_delete_tasks_stalled_total", "state" => *state)
            .increment(*n);
    }
    if let Some(gauge) = &report.gauge {
        for (state, value) in gauge {
            metrics::gauge!("siglake_compactor_delete_tasks_nonterminal", "state" => *state)
                .set(*value as f64);
        }
    }
}

/// #80 sibling ceiling for the SPAWNED re-clustering pass. A hung pass
/// doesn't stall the drain (it runs on its own task), but it permanently
/// stops maintenance — one pass is in flight at a time, so a wedged one
/// blocks every future pass. Passes legitimately run for minutes (single
/// L1→L2 bins measured 212–238s), hence the larger default.
/// `SIGLAKE_RECLUSTER_WATCHDOG_SECS` (default 1800; `0` disables; min 60).
fn recluster_watchdog_ceiling() -> Option<Duration> {
    watchdog_ceiling_from(
        std::env::var("SIGLAKE_RECLUSTER_WATCHDOG_SECS")
            .ok()
            .as_deref(),
        1800,
    )
}

/// Number of WAL drain batches claimed + committed concurrently per cycle
/// (B.1.3 parallel drain). Each batch is a normal bounded claim (the fs_batch
/// caps apply per batch); the encode + S3-upload work overlaps across workers
/// while the catalog CAS serializes the actual commits (cheap retries via the
/// vendored `update_table_with_base`). Peak memory scales with this × the
/// per-batch byte cap (decoded). Default 1 preserves the sequential drain.
fn drain_concurrency() -> usize {
    std::env::var("SIGLAKE_DRAIN_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 64))
        .unwrap_or(1)
}

/// How many consecutive cycles may fail to read one local segment before the
/// drain sets it aside under `poison/` (#3143). `0` disables the set-aside and
/// restores the retry-forever behaviour.
///
/// Three, not one: a read can fail for reasons that are not the segment's (a
/// storage hiccup, a momentarily unreachable volume), and the cost of a wrong
/// verdict is an operator requeue. Three cycles of an otherwise healthy drain
/// is seconds, so a genuinely unreadable segment is contained long before the
/// backlog behind it matters.
fn poison_attempts() -> u32 {
    poison_attempts_from(
        std::env::var("SIGLAKE_COMPACTOR_POISON_ATTEMPTS")
            .ok()
            .as_deref(),
    )
}

/// Pure resolver for [`poison_attempts`]: unset or unparseable is the default.
fn poison_attempts_from(configured: Option<&str>) -> u32 {
    configured
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(DEFAULT_POISON_ATTEMPTS)
}

/// This drain's shard of the index keyspace, from
/// `SIGLAKE_DRAIN_SHARD_INDEX` / `SIGLAKE_DRAIN_SHARD_COUNT`.
///
/// Default `(0, 1)` = claim everything, the historical behaviour. Set together;
/// a count without an index is meaningless and is treated as unsharded.
fn drain_shard() -> (usize, usize) {
    let count = std::env::var("SIGLAKE_DRAIN_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    if count == 1 {
        return (0, 1);
    }
    let index = std::env::var("SIGLAKE_DRAIN_SHARD_INDEX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    (index % count, count)
}

/// Concurrent segment GETs within one catalog-claim group fetch
/// (`SIGLAKE_DRAIN_FETCH_CONCURRENCY`, default 16). The serial per-segment
/// GET was the catalog-claim drain's dominant wall cost (fleet round 3's
/// ~50K rows/s per-drain ceiling: a 256-claim batch paid 256 round-trip
/// latencies before its commit). Memory stays bounded by the claim batch —
/// the whole group is decoded before commit either way.
fn drain_fetch_concurrency() -> usize {
    std::env::var("SIGLAKE_DRAIN_FETCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(1, 64))
        .unwrap_or(16)
}

/// Wall-clock budget for one continuous-dispatch drain pass. The discriminator
/// round showed the drain is latency-bound with every resource idle, so the
/// pass keeps `drain_concurrency()` commits in flight CONTINUOUSLY (topping up
/// as each lands, re-listing `sealed/` when the snapshot runs dry) instead of
/// the old claim-N-then-barrier cycle whose wall time was its slowest commit.
/// The budget bounds how long a pass runs before returning to the run loop, so
/// maintenance (the gauge sampler, the graded-backpressure compaction slots)
/// keeps its cadence under a sustained backlog.
fn drain_cycle_budget() -> Duration {
    Duration::from_secs(
        std::env::var("SIGLAKE_DRAIN_CYCLE_BUDGET_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.clamp(5, 600))
            .unwrap_or(30),
    )
}

/// On-disk bytes of claimed batches admitted into flight simultaneously
/// (default 1 GiB). The batch bytes multiply ~4–6× through the
/// read → concat → sort → partition-split → encode pipeline, so a raw
/// concurrency knob alone has an OOM corner: 12 × 256 MiB batches killed a
/// 64 GB node. Admission requires BOTH this byte budget and the concurrency
/// cap; a single batch larger than the budget still admits alone (no
/// deadlock), it just flies solo.
fn drain_inflight_budget_bytes() -> u64 {
    std::env::var("SIGLAKE_DRAIN_INFLIGHT_BUDGET_MB")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|mb| mb.clamp(64, 65_536).saturating_mul(1024 * 1024))
        .unwrap_or(1024 * 1024 * 1024)
}

/// Pure admission rule for the continuous-dispatch top-up loop: admit the next
/// batch while under the pipeline-count cap AND the in-flight byte budget —
/// except that an empty pipeline always admits (a batch bigger than the whole
/// budget must not deadlock the drain; it just runs alone).
fn admit_batch(
    inflight_len: usize,
    workers: usize,
    inflight_bytes: u64,
    batch_bytes: u64,
    budget_bytes: u64,
) -> bool {
    if inflight_len >= workers {
        return false;
    }
    if inflight_len == 0 {
        return true;
    }
    inflight_bytes.saturating_add(batch_bytes) <= budget_bytes
}

/// #4651: should this pass leave `segment` in `sealed/` because it has already
/// spent [`MAX_PASS_CLAIM_ATTEMPTS`] claims on it? A path with no readable file
/// name is never withheld — it cannot be charged either, and refusing to claim
/// it would strand it.
///
/// Says so once per segment per pass, when the pass first declines to re-claim
/// it rather than when the last attempt was charged: the attempt that reaches
/// the bound may still be the one that commits. Whether the cause is a one-off
/// or recurs every cycle is only visible in the rate.
fn withhold_spent_segment(
    attempts: &mut HashMap<String, u32>,
    segment: &Path,
    tenant_label: &str,
) -> bool {
    let Some(name) = segment.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    let Some(spent) = attempts.get_mut(name) else {
        return false;
    };
    if *spent < MAX_PASS_CLAIM_ATTEMPTS {
        return false;
    }
    if *spent == MAX_PASS_CLAIM_ATTEMPTS {
        // One past the bound marks it reported, so a later re-list in the same
        // pass withholds it silently.
        *spent += 1;
        tracing::warn!(
            path = %segment.display(),
            attempts = MAX_PASS_CLAIM_ATTEMPTS,
            tenant = tenant_label,
            "segment failed every claim this drain pass made on it; it stays in sealed/ for the \
             next cycle instead of being re-claimed for the rest of this one"
        );
        metrics::counter!(
            "siglake_compactor_pass_claim_attempts_exhausted_total",
            "tenant" => tenant_label.to_string()
        )
        .increment(1);
    }
    true
}

/// Charge one claim attempt to `segment` for the rest of this pass.
fn charge_pass_attempt(attempts: &mut HashMap<String, u32>, segment: &Path) {
    if let Some(name) = segment.file_name().and_then(|n| n.to_str()) {
        let entry = attempts.entry(name.to_string()).or_insert(0);
        *entry = entry.saturating_add(1);
    }
}

fn utf8_bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

/// Read all claimed segments, sort, and commit as one Iceberg snapshot
/// against the supplied (possibly tenant-scoped) [`IcebergContext`].
/// Concatenate WAL batches that may not all present the same schema.
///
/// **This wedged a 1TB round.** `concat_batches` requires every batch to share
/// one schema, and the drain passed `batches[0].schema()` — so a claim spanning
/// an additive schema migration (WS-7 `attributes`, or an auto-promoted column
/// appearing mid-run) failed with `concat_batches`, failing the whole compactor
/// cycle. The same segments are re-claimed next cycle and fail again, so the
/// drain wedges PERMANENTLY: the 2026-08-10 round sat at 19.7M committed rows
/// against 2.02B accepted, with 220,616 sealed segments backed up and
/// `compactor cycle failed error=concat_batches` every ~80s.
///
/// A latent bug, not a new one — raising the claim from 256 to 1024 segments
/// just made spanning a migration boundary near-certain instead of occasional.
/// It could always wedge; it needed only one unlucky claim.
///
/// The trigger here was heterogeneous DOCUMENTS, not a migration: the
/// under-load freshness probe posts `{timestamp, host, raw, level, region}`
/// while corpus rows carry a different field set, so their WAL segments differ.
/// That is not a bench artifact — an OTel-native platform receives
/// heterogeneous documents continuously, so any claim can span two shapes.
///
/// Fix mirrors what the re-cluster path already does
/// (`align_batch_to_table_schema`): align every batch to one schema, null-filling
/// absent columns. That schema is the UNION of all fields present, not the
/// widest single batch — two batches can have equal width and neither contain
/// the other, and silently dropping a column is worse than the wedge.
fn concat_batches_unified(batches: &[RecordBatch]) -> Result<RecordBatch> {
    let first = batches[0].schema();
    // Steady state: every batch already matches, so this is the original call.
    if batches.iter().all(|b| b.schema() == first) {
        return arrow::compute::concat_batches(&first, batches.iter())
            .context("concat_batches (uniform)");
    }
    // UNION, not "the widest". Heterogeneous documents are not nested: a batch
    // of {timestamp, host, raw} and one of {timestamp, host, status} have equal
    // width and neither contains the other. Picking either loses a column, and
    // losing a column silently is worse than the wedge this fixes. Field order
    // follows first-seen so the steady-state layout is stable.
    let mut fields: Vec<arrow::datatypes::FieldRef> = Vec::new();
    for b in batches {
        for f in b.schema().fields() {
            if !fields.iter().any(|e| e.name() == f.name()) {
                fields.push(f.clone());
            }
        }
    }
    let target: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::new(fields));
    metrics::counter!("siglake_compactor_drain_schema_aligned_total").increment(1);
    let aligned: Vec<RecordBatch> = batches
        .iter()
        .map(|b| align_batch_to(b, &target))
        .collect::<Result<_>>()?;
    arrow::compute::concat_batches(&target, aligned.iter()).context("concat_batches (aligned)")
}

/// Project `batch` onto `target`, null-filling columns it lacks.
fn align_batch_to(
    batch: &RecordBatch,
    target: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let cols = target
        .fields()
        .iter()
        .map(|f| match batch.schema().index_of(f.name()) {
            Ok(i) => Ok(batch.column(i).clone()),
            Err(_) => Ok(arrow_array::new_null_array(f.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(target.clone(), cols).context("align WAL batch to the widest schema")
}

/// One claimed segment the drain could not read, and the error it got.
#[derive(Debug)]
struct UnreadableSegment {
    path: PathBuf,
    reason: String,
}

/// A filesystem drain batch that failed because specific segments could not be
/// read (#3143).
///
/// The point of the type is attribution: the drain charges the failure to
/// exactly these segments, and every other segment in the batch is released
/// untouched. An error of any other kind is the batch's — a catalog conflict,
/// a store timeout — and is charged to nothing.
#[derive(Debug)]
struct UnreadableSegments {
    segments: Vec<UnreadableSegment>,
}

impl std::fmt::Display for UnreadableSegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} claimed segment(s) could not be read: ",
            self.segments.len()
        )?;
        for (i, s) in self.segments.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{}: {}", s.path.display(), s.reason)?;
        }
        Ok(())
    }
}

impl std::error::Error for UnreadableSegments {}

async fn commit_claimed(
    ice: &Arc<IcebergContext>,
    wal_dir: &Path,
    claimed: &[PathBuf],
    target: &CommitTarget,
) -> Result<CommitOutcome> {
    // Phase 4.13l: sub-stage histograms to identify which step
    // dominates the per-cycle commit cost. Without these, the only
    // observable is `commit_duration_seconds` which lumps read +
    // concat + sort + Parquet-encode + S3 upload + Iceberg commit
    // into one number. Round 17 saw 8 s cycles at 1 M EPS; round
    // 18 attributed that latency to S3 PUT (49 %) and sequential
    // segment read (23 %).
    //
    // Phase 4.13m: read sealed segments in parallel via
    // `spawn_blocking` + `try_join_all`. Each `read_segment` is
    // independent sync I/O + Arrow IPC parse, so on a multi-core
    // compactor pod we get near-linear speedup limited by CPU
    // and PVC IOPS. Round 18 read phase was 2.05 s / cycle; we
    // expect 4 CPU to drop this to ~0.5 s.
    let read_start = std::time::Instant::now();
    let read_futures = claimed.iter().map(|c| {
        let path = c.clone();
        tokio::task::spawn_blocking(move || {
            read_segment(&path).with_context(|| format!("reading {}", path.display()))
        })
    });
    let read_results = futures::future::try_join_all(read_futures)
        .await
        .context("joining read_segment tasks")?;
    // #3143: name the segments that failed rather than returning the first
    // error for the whole batch. One unreadable file used to fail every batch
    // it landed in with nothing to say which file it was, and the drain
    // released the batch and re-claimed the same set forever.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut unreadable: Vec<UnreadableSegment> = Vec::new();
    for (path, r) in claimed.iter().zip(read_results) {
        match r {
            Ok(b) => batches.extend(b),
            Err(e) => unreadable.push(UnreadableSegment {
                path: path.clone(),
                reason: format!("{e:#}"),
            }),
        }
    }
    if !unreadable.is_empty() {
        return Err(anyhow::Error::new(UnreadableSegments {
            segments: unreadable,
        }));
    }
    metrics::histogram!("siglake_compactor_segment_read_duration_seconds")
        .record(read_start.elapsed().as_secs_f64());
    if batches.is_empty() {
        return Ok(CommitOutcome {
            segments: claimed.len(),
            rows: 0,
            strict_residual_rows: 0,
        });
    }

    // WS-6 real-time buffer: record which WAL segments this commit consumed in
    // the new snapshot's summary, so the query-side buffer can exclude them the
    // instant they land in Iceberg (atomic with the commit) — closing the brief
    // over-count window before `finish_segment` renames them to `committed/`.
    // The basenames match what the buffer lists under `processing/`.
    let claimed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let consumed: Vec<ConsumedProofEntry> = claimed
        .iter()
        .filter_map(|c| {
            c.file_name()
                .and_then(|f| f.to_str())
                .map(|segment_id| ConsumedProofEntry {
                    segment_id: segment_id.to_string(),
                    claimed_at_ms,
                })
        })
        .collect();
    let terminal_proof_entries =
        filesystem_terminal_consumed_proof_entries(ice, wal_dir, target.index_label())
            .await
            .with_context(|| {
                format!(
                    "certifying terminal filesystem consumed-proof entries in {}",
                    wal_dir.display()
                )
            })?;
    commit_batches(
        ice,
        batches,
        consumed,
        None,
        &terminal_proof_entries,
        claimed.len(),
        target,
    )
    .await
}

/// Certify durable proof entries from the filesystem lifecycle owned by one
/// drain cycle. A segment still under `sealed/`, `processing/`, or `orphans/`
/// can be retried or needs adjudication, so its positive proof must remain.
/// An entry under `committed/`, or absent after orphan disposition and prior
/// retention sweeps, is terminal and can be removed atomically with the next
/// append. Any proof or directory read failure leaves the append fail-closed.
async fn filesystem_terminal_consumed_proof_entries(
    ice: &Arc<IcebergContext>,
    wal_dir: &Path,
    table_name: &str,
) -> Result<Vec<String>> {
    let ConsumedProofRead::Valid(proof) = ice.durable_consumed_proof(table_name).await? else {
        return Ok(Vec::new());
    };

    let mut protected = HashSet::new();
    for path in list_visible(wal_dir).context("listing visible WAL segments")? {
        let is_committed = path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(siglake_wal::COMMITTED_DIR);
        if !is_committed {
            if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
                protected.insert(name.to_string());
            }
        }
    }
    for path in list_orphaned(wal_dir)
        .context("listing quarantined WAL segments")?
        .into_iter()
        // #3143: a segment set aside under `poison/` is also awaiting
        // adjudication — an operator can move it back into `sealed/` — so its
        // proof entry stays for the same reason an orphan's does.
        .chain(list_poisoned(wal_dir).context("listing WAL segments set aside")?)
    {
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            protected.insert(name.to_string());
        }
    }

    Ok(proof
        .entries()
        .filter_map(|entry| (!protected.contains(&entry.segment_id)).then_some(entry.segment_id))
        .collect())
}

/// Map (for index targets), concatenate, and commit a set of WAL batches to
/// the target table. Shared by the FS drain (which read the batches from
/// local segments) and the catalog-claim drain (which fetched them from the
/// object-store mirror) — fleet prerequisite 2 gives the catalog path the
/// same per-index commit machinery the FS path has.
async fn commit_batches(
    ice: &Arc<IcebergContext>,
    batches: Vec<RecordBatch>,
    consumed: Vec<ConsumedProofEntry>,
    acknowledged_through_ms: Option<i64>,
    terminal_proof_entries: &[String],
    segments: usize,
    target: &CommitTarget,
) -> Result<CommitOutcome> {
    if batches.is_empty() {
        return Ok(CommitOutcome {
            segments,
            rows: 0,
            strict_residual_rows: 0,
        });
    }
    // Carrier mapping, per batch, before anything else. Untimed until now, and
    // it runs for EVERY user index -- which is every table the benchmarks and the
    // product actually use; only the built-in `events` table skips it. It was the
    // 21.4% the cycle accounting could not close on 2026-08-14.
    let map_start = std::time::Instant::now();
    let (batches, strict_residual_rows) = match target {
        CommitTarget::Events => (batches, 0u64),
        CommitTarget::Index { config, .. } => {
            let mut mapped = Vec::with_capacity(batches.len());
            let mut strict_rows = 0u64;
            for batch in batches {
                let mapped_batch =
                    map_carrier_batch_with_stats(&batch, config).with_context(|| {
                        format!("map carrier batch into index `{}`", config.index_id)
                    })?;
                strict_rows += mapped_batch.strict_residual_rows as u64;
                mapped.push(mapped_batch.batch);
            }
            (mapped, strict_rows)
        }
    };
    metrics::histogram!("siglake_compactor_carrier_map_duration_seconds")
        .record(map_start.elapsed().as_secs_f64());
    let concat_start = std::time::Instant::now();
    let combined = concat_batches_unified(&batches).context("concat_batches")?;
    metrics::histogram!("siglake_compactor_concat_duration_seconds")
        .record(concat_start.elapsed().as_secs_f64());
    let row_count = combined.num_rows() as u64;
    let ice_start = std::time::Instant::now();
    match target {
        CommitTarget::Events => {
            // The storage layer time-orders rows on write (the table's declared
            // sort order), so the compactor no longer pre-sorts — append the
            // concat directly.
            ice.append_batch_with_consumed_proof_pruning(
                combined,
                &consumed,
                acknowledged_through_ms,
                terminal_proof_entries,
                None,
            )
            .await
            .context("IcebergContext::append_batch_with_consumed_proof")?;
        }
        CommitTarget::Index {
            index,
            bloom_columns,
            expect_table_uuid,
            ..
        } => {
            let bloom_refs: Vec<&str> = bloom_columns.iter().map(String::as_str).collect();
            // #2836: `index_table_ident` is a NAME. The append compares the
            // table that name resolves to against the identity the ownership
            // check verified before this cycle claimed anything, and refuses
            // before it writes a file if a recreation happened in between.
            ice.append_to_table_with_consumed_proof_pruning(
                &ice.index_table_ident(index),
                combined,
                &bloom_refs,
                &consumed,
                acknowledged_through_ms,
                terminal_proof_entries,
                expect_table_uuid.as_deref(),
            )
            .await
            .with_context(|| format!("append_to_table_with_consumed_proof({index})"))?;
        }
    }
    metrics::histogram!("siglake_compactor_iceberg_append_duration_seconds")
        .record(ice_start.elapsed().as_secs_f64());
    Ok(CommitOutcome {
        segments,
        rows: row_count,
        strict_residual_rows,
    })
}

impl Compactor {
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn iceberg(&self) -> &Arc<IcebergContext> {
        &self.ice
    }

    /// Catalog-claim execution path. See [`with_catalog_claim`].
    async fn run_once_catalog(&self) -> Result<usize> {
        let cfg = self
            .catalog
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("catalog claim not configured"))?;
        let cycle_start = std::time::Instant::now();

        // 1. Recovery sync: resume the durable wraparound scan under
        // `<prefix>/` and register any objects the ingesters failed to
        // self-register (crash between upload and register). Each elected pass
        // is bounded to one page and idempotent — `register` does
        // `ON CONFLICT DO NOTHING` — but still incurs listing and per-object
        // network calls, so it runs on an interval, not per cycle.
        let sync_due = match mirror_sync_interval() {
            None => false,
            Some(interval) => {
                let mut last = cfg.last_mirror_sync.lock().unwrap();
                let due = interval.is_zero() || last.is_none_or(|t| t.elapsed() >= interval);
                if due {
                    *last = Some(std::time::Instant::now());
                }
                due
            }
        };
        // Reclaim claims abandoned by dead workers (remediation plan Phase 1
        // item 7). Nothing did this before: a drain that died between claiming
        // and committing left its rows in `processing` permanently, invisible to
        // every worker because the claim query selects `status = 'sealed'`. Data
        // accepted, durable in the mirror, and unqueryable forever.
        //
        // Runs on the same cadence as the mirror sync check but independently of
        // it, because disabling the mirror scan (Phase 1 item 1) must not also
        // disable dead-worker recovery.
        if claim_reclaim_due(&cfg.last_reclaim) {
            let _reclaim_guard = StageTimer(
                "siglake_compactor_reclaim_duration_seconds",
                std::time::Instant::now(),
            );
            if let Err(e) = self.reclaim_with_proof(cfg).await {
                tracing::warn!(error = format!("{e:#}"), "claim reclaim failed");
            }
        }

        // ELECTED, and NON-FATAL.
        //
        // `sync_mirror_to_catalog` is the only path that reconciles "object in
        // the mirror, no catalog row" — the state an ingester dying between the
        // S3 PUT and its registration produces, and the one behind the
        // 2026-08-15 incident where 36,122 rows were accepted, durable and
        // permanently unqueryable. It used to run on EVERY drain every 60s with
        // no lease at all, unlike retention and delete-tasks which are both
        // elected: N drains each recursively listed the whole mirror prefix and
        // issued one INSERT per object, and the prefix grows monotonically
        // because committed-retention defaults to never-purge.
        //
        // Its Err also returned from this function BEFORE any claim happened,
        // so one S3 list hiccup meant that drain did zero work that cycle —
        // which made the obvious operational response "turn the mirror scan
        // off", removing the only owner of the silent-loss window entirely.
        // A failure is now logged and stepped over: reconciliation is a repair
        // path, so skipping it for one cycle costs nothing, while skipping the
        // drain costs ingest.
        //
        // Gated on the LEASE ALONE, deliberately NOT on `role.maintains()`.
        // The lease already reduces N workers to one and degrades to
        // everyone-runs-it if the lease itself fails, which is the right
        // direction for a repair path. Adding the role gate would have created
        // a configuration with no owner at all — a `--role drain` fleet with no
        // maintenance process, which is a real topology this project has run —
        // and that is the same defect in a new place.
        //
        // The operation-scoped lease below is separate from that election and
        // fails CLOSED: skipping one repair pass is safe, but reconciling while
        // retention purges can create a sealed row for an absent object.
        if sync_due && self.maintenance_lease("mirror_sync").await {
            let _ = self
                .with_mirror_reconciliation_lease(async {
                    // Phase 0: the scan's cost was never measured, only inferred.
                    let _t = std::time::Instant::now();
                    metrics::counter!("siglake_compactor_mirror_sync_total").increment(1);
                    let r = sync_mirror_to_catalog(
                        &cfg.store,
                        &cfg.prefix,
                        &cfg.claim,
                        MIRROR_SYNC_PAGE_OBJECTS,
                    )
                    .await;
                    metrics::histogram!("siglake_compactor_mirror_sync_duration_seconds")
                        .record(_t.elapsed().as_secs_f64());
                    if let Err(e) = r {
                        metrics::counter!("siglake_compactor_cycles_total",
                            "outcome" => "catalog_sync_error")
                        .increment(1);
                        tracing::warn!(
                            error = format!("{e:#}"),
                            "mirror-to-catalog reconciliation failed; continuing with the drain cycle. \
                             Un-registered mirror objects stay un-registered until the next pass — \
                             watch siglake_wal_mirror_register_abandoned_total."
                        );
                    }
                })
                .await;
        }

        // BIG-4 #4b: commit-accumulation gate. Peek the sealed queue
        // (cheap indexed aggregate) and hold off if batching is enabled
        // and the queue hasn't reached the byte target or age floor.
        // Multi-pod safe: deferring just doesn't claim this cycle; when
        // the gate clears every pod races the atomic `try_claim` as
        // usual.
        // Segments set aside: repeated drain failures, or a refusal no retry
        // can change (#2746). A gauge rather than only the counter+log at the
        // quarantine itself: this is the number an operator alerts on, and its
        // rising is the signal that some index does not exist, or that a
        // recreated one is holding a dropped incarnation's mirrored objects.
        // Absent this, the condition it replaced -- a
        // poison segment spinning at the head of the queue forever -- was
        // invisible until ingest stalled.
        if let Ok(n) = cfg.claim.quarantined_count().await {
            metrics::gauge!("siglake_catalog_claim_quarantined").set(n as f64);
        }
        // The backlog gauge is published from `peek_pending` EVERY cycle, not
        // only when the commit-batch gate defers.
        //
        // It used to be set to `claimed.len()` on the claim path — the segments
        // this worker took, bounded by the claim batch (256 by default). So the
        // one number an operator needs ("is the drain falling behind, and is
        // the gap growing?") reported a saturating constant precisely when the
        // answer was yes, and the real depth only when the queue was too SMALL
        // to commit. The 2026-08-18 round measured a true peak of 17,031
        // segments; this gauge would have read 256 throughout, and the bench
        // harness had already learned to distrust it and query Postgres
        // directly. Its own doc comment calls it "sealed segments waiting to be
        // claimed... Drives the compactor HPA", and the operator consumes it
        // against a target of 1.0 — so a ratio of 256/1 slammed to max on any
        // backlog and to min the moment a cycle claimed nothing.
        //
        // One indexed COUNT(*) per cycle, against cycles that take seconds.
        let _peek_t = std::time::Instant::now();
        let peek = cfg.claim.peek_pending().await;
        metrics::histogram!("siglake_compactor_peek_duration_seconds")
            .record(_peek_t.elapsed().as_secs_f64());
        let peeked = match peek {
            Ok(stats) => {
                metrics::gauge!("siglake_compactor_sealed_pending",
                    "tenant" => "default".to_string())
                .set(stats.segments as f64);
                metrics::gauge!("siglake_compactor_sealed_pending_bytes",
                    "tenant" => "default".to_string())
                .set(stats.bytes as f64);
                // The age of the oldest UNCLAIMED segment is the growth signal
                // a depth count cannot give: a steady depth with a rising age
                // is a drain that is not keeping up.
                metrics::gauge!("siglake_compactor_sealed_pending_oldest_age_seconds",
                    "tenant" => "default".to_string())
                .set(stats.oldest_age.as_secs_f64());
                Some(stats)
            }
            // Peek informs the gate and the gauge; if it fails, fall through to
            // the normal claim path rather than stalling commits. Leave the
            // gauges at their last value rather than publishing a zero, which
            // on a backlog signal reads as "caught up".
            Err(e) => {
                tracing::warn!(error = %e, "peek_pending failed; committing without gate this cycle");
                None
            }
        };
        if self.commit_batch.is_some() {
            if let Some(stats) = &peeked {
                if self.commit_deferred(stats.bytes, stats.oldest_age) {
                    metrics::counter!("siglake_compactor_cycles_total",
                        "outcome" => "deferred")
                    .increment(1);
                    metrics::counter!("siglake_compactor_commit_deferred_total",
                        "tenant" => "default".to_string())
                    .increment(1);
                    return Ok(0);
                }
            }
        }

        // 2. Atomically claim a batch.
        // Shard-routed claiming. Without routing, a batch spanning N indexes
        // becomes N commits (commit_claimed groups by (tenant, index_id)) — the
        // 2026-08-12 fleet round measured that as a 21% drain REGRESSION.
        let (shard_index, shard_count) = drain_shard();
        let _claim_t = std::time::Instant::now();
        let claim_result = cfg
            .claim
            .try_claim_sharded(cfg.batch_size, shard_index, shard_count)
            .await;
        metrics::histogram!("siglake_compactor_claim_duration_seconds")
            .record(_claim_t.elapsed().as_secs_f64());
        let claimed = match claim_result {
            Ok(c) => c,
            Err(e) => {
                metrics::counter!("siglake_compactor_cycles_total",
                    "outcome" => "claim_error")
                .increment(1);
                return Err(e);
            }
        };
        // NOT the place to publish `sealed_pending`: `claimed.len()` is the
        // claim batch, not the backlog. It is published above from
        // `peek_pending`, which asks the queue.
        metrics::gauge!(
            "siglake_compactor_claimed_last_cycle",
            "tenant" => "default".to_string()
        )
        .set(claimed.len() as f64);
        if claimed.is_empty() {
            metrics::counter!("siglake_compactor_cycles_total", "outcome" => "empty").increment(1);
            return Ok(0);
        }
        tracing::debug!(
            n = claimed.len(),
            "catalog-claim: draining WAL segments into per-(tenant, index) commits"
        );

        // 3. Group claimed segments by (tenant, index) and commit each group to
        // its own table in its own Iceberg namespace (fleet prereq 2: user-index
        // segments previously collapsed into the default tenant's events table).
        // Fetch + commit per group so one bad group doesn't poison the others.
        let mut by_group: std::collections::BTreeMap<(String, String), Vec<&_>> =
            std::collections::BTreeMap::new();
        for c in &claimed {
            by_group
                .entry((c.tenant.clone(), c.index_id.clone()))
                .or_default()
                .push(c);
        }

        let mut committed_total = 0usize;
        let mut committed_ids: Vec<String> = Vec::with_capacity(claimed.len());
        let mut released_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut commit_err: Option<anyhow::Error> = None;
        // #2889: what each group's commit was verified against, handed to the
        // terminal-claim transaction so the watermark it advances records the
        // incarnation the rows actually went into.
        let mut provenance = ProofProvenance::default();
        'outer: for ((tenant, index_id), group) in &by_group {
            let ice = match tenant.as_str() {
                "default" => self.ice.clone(),
                other => match self.ice.for_namespace(&format!("tenant_{other}")).await {
                    Ok(c) => Arc::new(c),
                    Err(e) => {
                        commit_err =
                            Some(e.context(format!("open tenant namespace tenant_{other}")));
                        break 'outer;
                    }
                },
            };
            // Resolve the commit target: '' = the built-in events table; any
            // other id resolves (auto-creating via templates) exactly like the
            // FS path. An unresolvable index releases its group for retry (a
            // later template/config may resolve it) without failing the cycle.
            let _resolve_t = std::time::Instant::now();
            let _resolve_guard = StageTimer(
                "siglake_compactor_index_resolve_duration_seconds",
                _resolve_t,
            );
            let target = if index_id.is_empty() {
                CommitTarget::Events
            } else {
                match ice.ensure_index(index_id).await {
                    Ok(Some(_)) => match ice.get_index(index_id).await {
                        // The identity is bound below, once the group's owner
                        // marker has been read (#2836).
                        Ok(Some(config)) => CommitTarget::Index {
                            bloom_columns: utf8_bloom_columns(&config),
                            config: Box::new(config),
                            index: index_id.clone(),
                            expect_table_uuid: None,
                        },
                        Ok(None) | Err(_) => {
                            metrics::counter!(
                                "siglake_compactor_index_unresolved_total",
                                "tenant" => tenant.clone(),
                                "index" => index_id.clone()
                            )
                            .increment(group.len() as u64);
                            for c in group {
                                let _ = cfg.claim.release(&c.id).await;
                                released_ids.insert(c.id.clone());
                            }
                            continue;
                        }
                    },
                    Ok(None) | Err(_) => {
                        metrics::counter!(
                            "siglake_compactor_index_unresolved_total",
                            "tenant" => tenant.clone(),
                            "index" => index_id.clone()
                        )
                        .increment(group.len() as u64);
                        for c in group {
                            let _ = cfg.claim.release(&c.id).await;
                            released_ids.insert(c.id.clone());
                        }
                        continue;
                    }
                }
            };
            // #2661: the group is routed by index NAME, exactly like the
            // filesystem path, so a `DELETE`+`POST` of the same id hands the
            // replacement table the dropped incarnation's mirrored segments.
            // The mirror carries its own owner marker under the group's key
            // prefix; a segment the marker and its own header cannot both place
            // in the live table is quarantined rather than committed, where
            // `requeue_quarantined` is the operator's deliberate way back — the
            // same disposition `stale/` gives the filesystem path, and it
            // deletes nothing either.
            let mut group_owner: Option<String> = None;
            // #2729: refuse objects that carry no identity, not just ones that
            // name the wrong table. Set when the prefix has been stamped for a
            // dropped incarnation, so it cannot speak for what predates it.
            let mut strict_owner = false;
            if !index_id.is_empty() {
                let claim = match self.verified_owner_for_test.as_deref() {
                    Some(uuid) => WalDirClaim::Owned(uuid.to_string()),
                    None => self
                        .claim_mirrored_index(&ice, cfg, tenant, index_id)
                        .await
                        .unwrap_or_else(|e| {
                            tracing::warn!(
                                tenant,
                                index = index_id,
                                error = %e,
                                "could not verify the mirrored index's owner marker; releasing \
                                 the group for retry rather than committing it unverified"
                            );
                            WalDirClaim::Refused
                        }),
                };
                match claim {
                    WalDirClaim::Owned(uuid) => group_owner = Some(uuid),
                    WalDirClaim::OwnedStrict(uuid) => {
                        group_owner = Some(uuid);
                        strict_owner = true;
                    }
                    WalDirClaim::Unidentified => {}
                    WalDirClaim::Refused => {
                        for c in group {
                            let _ = cfg.claim.release(&c.id).await;
                            released_ids.insert(c.id.clone());
                        }
                        continue;
                    }
                }
            }
            // #2836: from here on the commit is authorised for the table the
            // marker check just named, not for whatever the index name
            // resolves to when the append loads it.
            let target = target.expecting_table_uuid(group_owner.as_deref());
            // Drop HERE, not at end of scope. A guard living to the end of the
            // group body wrapped fetch + append + mark as well, and reported
            // index resolution as 232% of cycle wall -- which is how the
            // accounting identity caught it.
            drop(_resolve_guard);
            use futures::StreamExt as _;
            // Fetch + decode the group's segments CONCURRENTLY: the serial
            // per-segment GET was the drain's dominant wall cost (a 256-claim
            // batch paid 256 round-trip latencies per cycle — the ~50K rows/s
            // per-drain ceiling in fleet round 3). Segment order within a
            // commit is irrelevant: storage re-sorts every batch into the
            // table's declared order at write.
            let fetch_concurrency = drain_fetch_concurrency();
            // Materialized BoxFutures rather than a lazy borrowing iterator:
            // the borrow-through-stream chain leaked a higher-ranked lifetime
            // obligation into the embedded-drain future and broke downstream
            // crates' spawns with "FnOnce is not general enough".
            // `SegmentFetch::Refused` = the segment cannot be placed in the
            // table this group is for (#2693, #2729); it is quarantined, not
            // committed.
            let fetch_futs: Vec<SegmentFetchFut> = group
                .iter()
                .map(|c| {
                    // Root-relative mirror key — layout-agnostic (flat,
                    // per-tenant, or per-tenant/per-index).
                    let key = c.segment_url.clone();
                    let store = cfg.store.clone();
                    let owner = group_owner.clone();
                    let strict = strict_owner;
                    // #2745: the group knows both, and the refusal counter is
                    // the only numeric signal that a recreated index is holding
                    // objects back. Labelled `tenant="mirror"` it could not be
                    // broken down at all, so an operator reading a non-zero
                    // rate could not tell which index to look at.
                    let refused_tenant = tenant.clone();
                    let refused_index = index_id.clone();
                    let fut: futures::future::BoxFuture<'static, _> = Box::pin(async move {
                        let bs = store
                            .read(&key)
                            .await
                            .map_err(|e| anyhow::Error::from(e).context(format!("GET {key}")))?;
                        let body = bs.to_bytes();
                        // #2693: the mirror-side owner marker is per key PREFIX,
                        // so it says nothing about a segment a stale writer
                        // uploaded after the prefix was re-stamped. The frame
                        // header does. Refusing here quarantines this segment's
                        // claim rather than committing a dropped incarnation's
                        // rows into the replacement (#2746: terminal on the
                        // first cycle, since a table uuid is never reused).
                        //
                        // #2729: under `strict` the prefix has itself named a
                        // dropped table, so an UNSTAMPED object under it is
                        // refused on the same grounds — it may be a segment of
                        // that dropped incarnation, written before headers
                        // carried an identity, and nothing here can tell.
                        if let Some(uuid) = owner.as_deref() {
                            let refusal =
                                match siglake_wal::classify_segment_owner_bytes(&body, uuid) {
                                    siglake_wal::WalOwner::Stale(dropped) => {
                                        Some((dropped, STALE_HEADER_REFUSAL))
                                    }
                                    siglake_wal::WalOwner::Unmarked if strict => {
                                        Some(("unstamped".to_string(), UNSTAMPED_REFUSAL))
                                    }
                                    _ => None,
                                };
                            if let Some((dropped, reason)) = refusal {
                                metrics::counter!(
                                    "siglake_compactor_wal_stale_segments_total",
                                    "tenant" => refused_tenant,
                                    "index" => refused_index
                                )
                                .increment(1);
                                tracing::warn!(
                                    key = %key,
                                    dropped_table = %dropped,
                                    live_table = %uuid,
                                    "mirrored segment cannot be placed in the table this index \
                                     resolves to now; quarantining the claim instead of \
                                     committing it"
                                );
                                return Ok(SegmentFetch::Refused(reason));
                            }
                        }
                        // Object-store segments may be WS-8 framed (header +
                        // CRC + body) or legacy raw IPC — decode either.
                        read_segment_from_bytes(&body)
                            .context("read_segment_from_bytes")
                            .map(SegmentFetch::Ready)
                    });
                    fut
                })
                .collect();
            // The fetch stage had NO timer, and it is the largest block on the
            // drain path: a 512-segment claim issues 512 GETs at
            // `fetch_concurrency`. The 2026-08-14 attribution measured a
            // work-saturated 4-drain fleet at 64.6% "duty" with a GROWING
            // backlog -- the missing 35% could not be idleness, and this was
            // the only unmeasured stage it could be hiding in.
            //
            // Timed as stage WALL (not summed per-segment latency) so it is
            // directly comparable to concat/data_write, which are also per-call
            // wall times. Summing them across calls gives node-seconds.
            let _fetch_t = std::time::Instant::now();
            let fetched: Vec<anyhow::Result<SegmentFetch>> = futures::stream::iter(fetch_futs)
                .buffered(fetch_concurrency)
                .collect()
                .await;
            metrics::histogram!("siglake_compactor_segment_fetch_duration_seconds")
                .record(_fetch_t.elapsed().as_secs_f64());
            metrics::counter!("siglake_compactor_segments_fetched_total")
                .increment(group.len() as u64);
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut consumed: Vec<ConsumedProofEntry> = Vec::with_capacity(group.len());
            let mut fetch_err: Option<anyhow::Error> = None;
            let mut refused: Vec<(String, &'static str)> = Vec::new();
            for (c, result) in group.iter().zip(fetched) {
                match result {
                    Ok(SegmentFetch::Ready(mut bs)) => {
                        batches.append(&mut bs);
                        consumed.push(ConsumedProofEntry {
                            segment_id: format!("{}.arrow", c.id),
                            claimed_at_ms: c.claimed_at.timestamp_millis(),
                        });
                    }
                    Ok(SegmentFetch::Refused(reason)) => refused.push((c.id.clone(), reason)),
                    Err(e) => {
                        fetch_err = Some(e);
                        break;
                    }
                }
            }
            // Quarantined on this cycle, not committed and not deleted: the same
            // disposition `stale/` gives the FS path, and `requeue_quarantined`
            // is the way back. #2746: this used to `release`, which is
            // retry-shaped — twelve cycles, each paying a GET of the object's
            // bytes, to reach an answer a table uuid's never being reused made
            // terminal on the first one.
            for (id, reason) in &refused {
                let _ = cfg.claim.quarantine_terminal(id, reason).await;
                released_ids.insert(id.clone());
            }
            let group: Vec<_> = group
                .iter()
                .filter(|c| !refused.iter().any(|(id, _)| id == &c.id))
                .collect();
            if group.is_empty() {
                continue;
            }
            if let Some(e) = fetch_err {
                commit_err = Some(e);
                break 'outer;
            }
            Self::preregister_proof_watermark_skips(index_id);
            let stored = cfg
                .claim
                .consumed_proof_watermark(tenant, index_id)
                .await
                .with_context(|| {
                    format!("read consumed-proof watermark for {tenant}/{index_id}")
                })?;
            // #2889: the append writes this boundary onto the table the group's
            // ownership check named. A boundary established by a different
            // incarnation describes segments that table never held, so it is
            // dropped rather than carried — the rows still commit.
            let watermark =
                Self::watermark_for_incarnation(stored.as_ref(), group_owner.as_deref());
            if watermark.is_none() && stored.is_some() {
                metrics::counter!(
                    "siglake_compactor_proof_watermark_skipped_total",
                    "reason" => PROOF_WATERMARK_UNVERIFIED_APPEND,
                    "index" => index_id.clone()
                )
                .increment(1);
            }
            let terminal_proof_entries = Self::terminal_consumed_proof_entries(
                &ice,
                &cfg.claim,
                tenant,
                index_id,
                target.index_label(),
            )
            .await
            .with_context(|| {
                format!("read terminal consumed-proof entries for {tenant}/{index_id}")
            })?;
            let outcome = match commit_batches(
                &ice,
                batches,
                consumed,
                watermark,
                &terminal_proof_entries,
                group.len(),
                &target,
            )
            .await
            {
                Ok(o) => o,
                Err(e) => {
                    commit_err = Some(e);
                    break 'outer;
                }
            };
            metrics::counter!(
                "siglake_compactor_rows_committed_total",
                "tenant" => tenant.clone()
            )
            .increment(outcome.rows);
            if outcome.strict_residual_rows > 0 {
                metrics::counter!(
                    "siglake_compactor_strict_residual_rows_total",
                    "tenant" => tenant.clone(),
                    "index" => target.index_label().to_string()
                )
                .increment(outcome.strict_residual_rows);
            }
            committed_total += group.len();
            committed_ids.extend(group.iter().map(|c| c.id.clone()));
            if let Some(owner) = group_owner.as_deref() {
                // The append above was fenced against this uuid, so it is the
                // table these rows and their proof entries are in (#2889).
                provenance.record(tenant, index_id, owner);
            }
        }

        if let Some(e) = commit_err {
            // Release everything not already committed or explicitly released.
            for c in &claimed {
                if committed_ids.iter().any(|id| id == &c.id) || released_ids.contains(&c.id) {
                    continue;
                }
                if let Err(err) = cfg.claim.release(&c.id).await {
                    tracing::error!(id = %c.id, error = %err, "catalog release failed");
                }
            }
            // Successfully-committed groups still get their rows marked so a
            // retry can't double-apply them.
            let _mark_guard = StageTimer(
                "siglake_compactor_mark_committed_duration_seconds",
                std::time::Instant::now(),
            );
            if let Err(err) = cfg
                .claim
                .mark_committed_batch(&committed_ids, &provenance)
                .await
            {
                tracing::warn!(error = %err, "mark_committed_batch failed; rows will retry");
            }
            drop(_mark_guard);
            metrics::counter!("siglake_compactor_cycles_total",
                "outcome" => "commit_error")
            .increment(1);
            return Err(e);
        }

        // One catalog round-trip per chunk instead of one per segment (the
        // 256-claim batch previously paid 256 serial UPDATEs here).
        //
        // This is the SUCCESS path -- the one that runs on every productive
        // cycle. My first timer went on the error branch only, which would have
        // reported ~0 and looked like a cheap stage.
        {
            let _mark_guard = StageTimer(
                "siglake_compactor_mark_committed_duration_seconds",
                std::time::Instant::now(),
            );
            if let Err(e) = cfg
                .claim
                .mark_committed_batch(&committed_ids, &provenance)
                .await
            {
                tracing::warn!(error = %e, "mark_committed_batch failed; rows will retry");
            }
        }
        let elapsed = cycle_start.elapsed().as_secs_f64();
        metrics::counter!("siglake_compactor_cycles_total", "outcome" => "ok").increment(1);
        metrics::counter!("siglake_compactor_segments_committed_total")
            .increment(committed_total as u64);
        metrics::histogram!("siglake_compactor_commit_duration_seconds").record(elapsed);
        Ok(committed_total)
    }
}

/// List the WAL mirror bucket and INSERT IGNORE each new
/// `<prefix>/[<tenant>/]<filename>.arrow` object into `wal_segments`.
/// Two layouts are tolerated:
///
/// - **Flat** (legacy / single-tenant): `<prefix>/<id>.arrow` →
///   registered with tenant `"default"`.
/// - **Per-tenant**: `<prefix>/<tenant>/<id>.arrow` →
///   registered with the path-derived tenant.
///
/// The compactor calls this at the start of every cycle so an
/// ingester crash between upload-to-S3 and register-in-catalog
/// doesn't leave segments invisible to claim.
/// WS-7 auto-promotion knobs. `SIGLAKE_AUTO_PROMOTE_MIN_PCT` (percent of
/// sampled rows a key must appear in; 0 = auto-promotion OFF, the default),
/// `SIGLAKE_AUTO_PROMOTE_MAX_COLUMNS` (total promotion cap, default 16),
/// and a fixed 300s cadence between sampling runs.
fn auto_promote_min_fraction() -> f64 {
    std::env::var("SIGLAKE_AUTO_PROMOTE_MIN_PCT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|pct| pct / 100.0)
        .unwrap_or(0.0)
}

fn auto_promote_max_columns() -> usize {
    std::env::var("SIGLAKE_AUTO_PROMOTE_MAX_COLUMNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
}

fn auto_promote_due() -> bool {
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    let last = LAST.get_or_init(|| Mutex::new(None));
    let mut last = last.lock().unwrap();
    let due = last.is_none_or(|t| t.elapsed() >= std::time::Duration::from_secs(300));
    if due {
        *last = Some(std::time::Instant::now());
    }
    due
}

/// How many deltas must pile up before a BUSY cycle folds
/// (`SIGLAKE_AGG_FOLD_BUSY_BACKLOG`, default 256).
///
/// An idle compactor folds anything outstanding; a busy one waits for this, so
/// the base is rewritten once per few hundred commits rather than once a
/// minute. 256 keeps a reader's worst-case fold inside the range stage 0
/// measured as flat (~200 deltas, ~2.1ms) without making the write frequent.
fn agg_fold_busy_backlog() -> usize {
    std::env::var("SIGLAKE_AGG_FOLD_BUSY_BACKLOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(256)
}

/// How often to fold outstanding group-count deltas into the wide base
/// (`SIGLAKE_AGG_FOLD_INTERVAL_SECS`, default 60s).
///
/// This paces read amplification, not correctness: a query folds whatever is
/// outstanding itself, so the cadence only decides how many small objects it
/// reads to do so. The default comes from the stage-0 measurement — the fold is
/// flat to ~200 outstanding deltas (~2.1ms, most of it cloning the base) and
/// 11.5ms at 500 — so a minute is comfortably inside the budget even at a
/// commit every few seconds, and there is nothing to gain by folding harder.
fn agg_fold_due() -> bool {
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    let secs = std::env::var("SIGLAKE_AGG_FOLD_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60);
    let last = LAST.get_or_init(|| Mutex::new(None));
    let mut last = last.lock().unwrap();
    let due = last.is_none_or(|t| t.elapsed() >= std::time::Duration::from_secs(secs));
    if due {
        *last = Some(std::time::Instant::now());
    }
    due
}

/// How often to census the maintained tables for a group-count aggregate that
/// is short of `record_count` (`SIGLAKE_AGG_SHORT_SCAN_INTERVAL_SECS`, default
/// 900s; `0`, `off`, `disabled` or `never` switch the census off).
///
/// Much slower than the fold because it answers a different question. The fold
/// paces read amplification and a lagging one costs a reader small GETs; the
/// census looks for a condition that, once true, is true until something
/// rebuilds — an upgraded prefix, a delta lost with its marker. Fifteen minutes
/// bounds how long a table serves `GROUP BY` from the per-file tiers before an
/// operator is told, and keeps one whole-warehouse pass (one folded-wide view
/// and one inline object per table) off the minute cadence the fold runs at.
fn agg_short_scan_interval() -> Option<Duration> {
    agg_short_scan_interval_from(
        std::env::var("SIGLAKE_AGG_SHORT_SCAN_INTERVAL_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the census cadence from its raw environment value. Pure so the
/// disable words and the zero case are tested without `set_var`.
fn agg_short_scan_interval_from(configured: Option<&str>) -> Option<Duration> {
    let Some(raw) = configured else {
        return Some(Duration::from_secs(900));
    };
    let raw = raw.trim().to_ascii_lowercase();
    if matches!(raw.as_str(), "off" | "disabled" | "never" | "0") {
        return None;
    }
    Some(Duration::from_secs(raw.parse().unwrap_or(900).max(1)))
}

/// Whether a census that finds a real shortfall may REBUILD it
/// (`SIGLAKE_AGG_SHORT_REPAIR`, default off).
///
/// Off by default for the reason the post-rewrite index rebuild is (#4162): the
/// repair is the most expensive read the system has — one Tier-2 query per
/// maintained column, and on a table whose columns exceed the per-file footer
/// cap that is a raw-page decode of every live file — and turning it on for
/// every install would spend it on the first pass after an upgrade, unasked.
/// Off, the census still fires `siglake_group_count_short_aggregates_total`
/// and `SiglakeGroupCountAggregateShort` pages, and
/// `siglake rebuild-group-counts --table <t>` remains the operator's move.
fn agg_short_repair_enabled() -> bool {
    agg_short_repair_enabled_from(std::env::var("SIGLAKE_AGG_SHORT_REPAIR").ok().as_deref())
}

fn agg_short_repair_enabled_from(configured: Option<&str>) -> bool {
    matches!(
        configured.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// How many tables one census pass may rebuild
/// (`SIGLAKE_AGG_SHORT_REPAIR_MAX_TABLES`, default 1).
///
/// The budget is what keeps a first enable from turning into a whole-warehouse
/// Tier-2 scan: every table upgraded across #2919 is short at once, and a
/// warehouse's indexes are all short together. One per pass at the default
/// cadence drains a hundred-table warehouse in a day and leaves the compactor's
/// other stages their time.
fn agg_short_repair_max_tables() -> usize {
    agg_short_repair_max_tables_from(
        std::env::var("SIGLAKE_AGG_SHORT_REPAIR_MAX_TABLES")
            .ok()
            .as_deref(),
    )
}

fn agg_short_repair_max_tables_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|v| v.trim().parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

/// How often to census the maintained tables for an inline aggregate object
/// that cannot prove coverage (`SIGLAKE_INLINE_COVERAGE_SCAN_INTERVAL_SECS`,
/// default 900s; `0`, `off`, `disabled` or `never` switch the census off).
///
/// Fifteen minutes for the same reason the short-aggregate census runs at it:
/// the condition, once true, is true until an operator rebuilds, so the cadence
/// bounds how long a table serves windowed `GROUP BY` from the per-file tiers
/// before anything says which table it is. It is also what `for:` on
/// `SiglakeInlineCoverageUnproven` is sized against — 30 minutes is two
/// consecutive censuses agreeing, which is what makes the alert's condition
/// "persistent" rather than "seen once".
///
/// Default ON: the pass reads table metadata the compactor has cached and one
/// object per maintained table, with no rebuild behind it. The expensive half
/// of #3000 — a Tier-2 query per column — has no counterpart here, so there is
/// nothing to make opt-in.
fn inline_coverage_scan_interval() -> Option<Duration> {
    inline_coverage_scan_interval_from(
        std::env::var("SIGLAKE_INLINE_COVERAGE_SCAN_INTERVAL_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the inline-coverage census cadence from its raw environment value.
/// Pure so the disable words and the zero case are tested without `set_var`.
fn inline_coverage_scan_interval_from(configured: Option<&str>) -> Option<Duration> {
    let Some(raw) = configured else {
        return Some(Duration::from_secs(900));
    };
    let raw = raw.trim().to_ascii_lowercase();
    if matches!(raw.as_str(), "off" | "disabled" | "never" | "0") {
        return None;
    }
    Some(Duration::from_secs(raw.parse().unwrap_or(900).max(1)))
}

/// Which `(iceberg namespace, table)` readings a completed inline-coverage
/// census must zero: the ones the previous pass published and this one no
/// longer reaches.
///
/// A table this pass could not read is still in `seen` — the census returns
/// `Undetermined` for it rather than dropping it — so only a table that has
/// genuinely stopped being maintained is cleared. Pure, so the set arithmetic
/// is tested without a warehouse.
fn vanished_inline_coverage(
    published: &BTreeSet<(String, String)>,
    seen: &BTreeSet<(String, String)>,
) -> Vec<(String, String)> {
    published.difference(seen).cloned().collect()
}

/// Whether the inline-coverage census interval has elapsed since the last pass.
/// Its own stamp, not the short-aggregate census's: the two run on independent
/// intervals and sharing one would make whichever ran first suppress the other.
fn inline_coverage_scan_due(interval: Duration) -> bool {
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    let last = LAST.get_or_init(|| Mutex::new(None));
    let mut last = last.lock().unwrap();
    let due = last.is_none_or(|t| t.elapsed() >= interval);
    if due {
        *last = Some(std::time::Instant::now());
    }
    due
}

/// Whether the census interval has elapsed since the last pass.
fn agg_short_scan_due(interval: Duration) -> bool {
    use std::sync::{Mutex, OnceLock};
    static LAST: OnceLock<Mutex<Option<std::time::Instant>>> = OnceLock::new();
    let last = LAST.get_or_init(|| Mutex::new(None));
    let mut last = last.lock().unwrap();
    let due = last.is_none_or(|t| t.elapsed() >= interval);
    if due {
        *last = Some(std::time::Instant::now());
    }
    due
}

/// Default for `SIGLAKE_CLAIM_RECLAIM_MAX_AGE_SECS`, and what `0` and an
/// unparseable value resolve to.
const DEFAULT_CLAIM_RECLAIM_MAX_AGE_SECS: u64 = 900;

/// Default for `SIGLAKE_CLAIM_RECLAIM_INTERVAL_SECS`.
const DEFAULT_CLAIM_RECLAIM_INTERVAL_SECS: u64 = 60;

/// Why [`claim_reclaim_max_age_from`] substituted the default for a SET value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimReclaimMaxAgeFallback {
    /// Not a non-negative whole number of seconds.
    Unparseable,
    /// `0`, which would make every in-flight claim "abandoned" the moment it
    /// is taken.
    Zero,
}

/// How stale a `processing` claim must be before another worker reclaims it
/// (`SIGLAKE_CLAIM_RECLAIM_MAX_AGE_SECS`, default 900).
///
/// Must exceed the longest legitimate claim-to-commit time or live work is
/// yanked from a healthy worker. Size it from observed APPEND latency, not claim
/// latency — the commit is the slow part, and 1TB rounds have shown appends in
/// the tens of seconds with tails far above that.
///
/// `0` is NOT honoured (decided 2026-09-05, task #717): a zero max-age makes
/// every claim abandoned the moment it is taken, so each sweep would yank live
/// work from every healthy drain. It resolves to the default, as does an
/// unparseable value, and this wrapper logs the substitution once per process.
/// Positive values below the default are honoured: an operator who has
/// measured short appends may tighten it.
fn claim_reclaim_max_age() -> Duration {
    let raw = std::env::var("SIGLAKE_CLAIM_RECLAIM_MAX_AGE_SECS").ok();
    let (max_age, fallback) = claim_reclaim_max_age_from(raw.as_deref());
    if let Some(reason) = fallback {
        // Once, not per sweep: the sweep runs every minute for the life of the
        // process and the value cannot change underneath it.
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                configured = raw.as_deref().unwrap_or(""),
                reason = ?reason,
                default_secs = DEFAULT_CLAIM_RECLAIM_MAX_AGE_SECS,
                "SIGLAKE_CLAIM_RECLAIM_MAX_AGE_SECS ignored; using the default"
            );
        });
    }
    max_age
}

/// Resolve the reclaim max-age from its raw environment value.
///
/// Pure so the zero and garbage semantics can be tested without mutating
/// process-global state observed by parallel tests in this binary. The second
/// element says why the default was substituted for a set value so the env
/// wrapper can warn once; the twin itself never logs.
fn claim_reclaim_max_age_from(
    configured: Option<&str>,
) -> (Duration, Option<ClaimReclaimMaxAgeFallback>) {
    let default = Duration::from_secs(DEFAULT_CLAIM_RECLAIM_MAX_AGE_SECS);
    let Some(raw) = configured else {
        return (default, None);
    };
    match raw.parse::<u64>() {
        Ok(0) => (default, Some(ClaimReclaimMaxAgeFallback::Zero)),
        Ok(secs) => (Duration::from_secs(secs), None),
        Err(_) => (default, Some(ClaimReclaimMaxAgeFallback::Unparseable)),
    }
}

/// Reclaim sweep cadence (`SIGLAKE_CLAIM_RECLAIM_INTERVAL_SECS`, default 60).
///
/// `0` means EVERY CYCLE, not "off", the same rule as the mirror sync interval,
/// and there is no disable word: dead-worker recovery has no legitimate "off".
/// Zero is harmless rather than dangerous: a sweep that finds nothing is one
/// bounded catalog query per drain per cycle, and one that finds rows only
/// touches claims already older than the max-age. The cadence bounds how long a
/// dead worker's rows stay invisible, not how much live work is examined. An
/// unparseable value falls back to the default.
fn claim_reclaim_interval() -> Duration {
    claim_reclaim_interval_from(
        std::env::var("SIGLAKE_CLAIM_RECLAIM_INTERVAL_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the reclaim sweep cadence from its raw environment value.
///
/// Pure so the default, garbage and zero semantics can be tested without
/// mutating process-global state observed by parallel tests in this binary.
fn claim_reclaim_interval_from(configured: Option<&str>) -> Duration {
    Duration::from_secs(
        configured
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CLAIM_RECLAIM_INTERVAL_SECS),
    )
}

/// Whether a reclaim sweep is due on this drain, stamping `last` when it is.
fn claim_reclaim_due(last: &std::sync::Mutex<Option<std::time::Instant>>) -> bool {
    claim_reclaim_due_with(last, claim_reclaim_interval())
}

/// [`claim_reclaim_due`] with the cadence injected, so the zero-means-every-cycle
/// rule can be proven without the environment.
fn claim_reclaim_due_with(
    last: &std::sync::Mutex<Option<std::time::Instant>>,
    interval: Duration,
) -> bool {
    let mut l = last.lock().unwrap_or_else(|e| e.into_inner());
    let due = l.is_none_or(|t| t.elapsed() >= interval);
    if due {
        *l = Some(std::time::Instant::now());
    }
    due
}

/// How often each pod runs the mirror recovery sync
/// (`SIGLAKE_MIRROR_SYNC_INTERVAL_SECS`, default 60s; 0 = every cycle,
/// the pre-registrar behavior).
///
/// Mirror recovery scan interval, or `None` when explicitly disabled.
///
/// `off` / `disabled` / `never` disables it. **A value of 0 means EVERY LOOP**,
/// not "off" — the remediation plan calls this out specifically, because
/// overloading 0 is how a fleet-wide scan gets left on by accident.
///
/// Why cadence matters: one elected owner resumes a durable wraparound cursor
/// and examines at most 1,024 sealed objects per pass. That removes the old
/// whole-prefix and replica multipliers, but each page still incurs listing and
/// per-object registration network calls; the interval bounds that work.
fn mirror_sync_interval() -> Option<std::time::Duration> {
    mirror_sync_interval_from(
        std::env::var("SIGLAKE_MIRROR_SYNC_INTERVAL_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the mirror recovery cadence from its raw environment value.
///
/// Pure so the disable words and zero semantics can be tested without
/// mutating process-global state observed by parallel tests in this binary.
fn mirror_sync_interval_from(configured: Option<&str>) -> Option<std::time::Duration> {
    if let Some(v) = configured {
        let t = v.trim().to_ascii_lowercase();
        if matches!(t.as_str(), "off" | "disabled" | "never") {
            return None;
        }
    }
    let secs = configured.and_then(|v| v.parse().ok()).unwrap_or(60u64);
    Some(std::time::Duration::from_secs(secs))
}

#[derive(Debug)]
struct MirrorSyncObject {
    path: String,
    id: String,
    tenant: String,
    index_id: String,
    url: String,
    bytes: i64,
}

#[derive(Debug)]
struct MirrorSyncPage {
    objects: Vec<MirrorSyncObject>,
    end_of_prefix: bool,
}

fn unix_timestamp_millis() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn mirror_sync_cursor_id(store: &Operator, prefix: &str) -> String {
    let info = store.info();
    format!(
        "{}:{}:{}:{}",
        info.scheme(),
        info.name(),
        info.root(),
        prefix.trim_matches('/')
    )
}

fn mirror_sync_object(
    path: &str,
    content_length: u64,
    trimmed_prefix: &str,
) -> Option<MirrorSyncObject> {
    // Skip the `_active/` partial-segment blobs — they're periodic snapshots
    // of in-flight segments, not sealed.
    if path.contains("/_active/") || path.starts_with("_active/") {
        return None;
    }
    let file = path.rsplit('/').next().unwrap_or(path);
    let id = file.strip_suffix(".arrow")?;
    // Tenant + index derive from the segment's path relative to the prefix
    // (the ingester's mirror layout): one intermediate component is the tenant
    // (events table), two are `<tenant>/<index>` (a user index); any other
    // shape routes to the default tenant's events.
    let rel = path.strip_prefix(trimmed_prefix).unwrap_or(path);
    let rel = rel.trim_start_matches('/');
    let segments: Vec<&str> = rel.split('/').collect();
    let (tenant, index_id) = match segments.len() {
        2 => (segments[0].to_string(), String::new()),
        3 => (segments[0].to_string(), segments[1].to_string()),
        _ => ("default".to_string(), String::new()),
    };
    Some(MirrorSyncObject {
        path: path.to_string(),
        id: id.to_string(),
        tenant,
        index_id,
        url: format!("{trimmed_prefix}/{rel}"),
        bytes: content_length as i64,
    })
}

async fn list_mirror_sync_page(
    store: &Operator,
    prefix: &str,
    start_after: Option<&str>,
    object_budget: usize,
) -> Result<MirrorSyncPage> {
    use futures::stream::StreamExt;

    let object_budget = object_budget.max(1);
    let trimmed = prefix.trim_matches('/');
    let supports_start_after = store.info().full_capability().list_with_start_after;
    let list_prefix = format!("{trimmed}/");
    let mut request = store
        .lister_with(&list_prefix)
        .recursive(true)
        // OpenDAL documents this as a backend request-page hint, not the
        // operation bound. The loop below enforces the actual object budget.
        .limit(object_budget);
    if supports_start_after {
        if let Some(last_key) = start_after {
            request = request.start_after(last_key);
        }
    }
    let lister = request.await.context("list WAL mirror for catalog sync")?;
    let mut listing = lister.fuse();
    let mut objects = Vec::with_capacity(object_budget);
    while let Some(entry) = listing.next().await {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                return Err(anyhow::Error::from(e).context("list WAL mirror for catalog sync"))
            }
        };
        let path_str = entry.path();
        if !entry.metadata().is_file() {
            continue;
        }
        let Some(object) = mirror_sync_object(path_str, entry.metadata().content_length(), trimmed)
        else {
            continue;
        };
        // Memory and filesystem operators do not advertise native
        // `start_after`. Keep them correct (and hermetically testable) by
        // filtering a full ordered page locally; S3 uses server-side
        // continuation and remains bounded in listing cost.
        if !supports_start_after && start_after.is_some_and(|last| object.path.as_str() <= last) {
            continue;
        }
        objects.push(object);
        if supports_start_after && objects.len() == object_budget {
            return Ok(MirrorSyncPage {
                objects,
                end_of_prefix: false,
            });
        }
    }

    if !supports_start_after {
        objects.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        let end_of_prefix = objects.len() <= object_budget;
        objects.truncate(object_budget);
        return Ok(MirrorSyncPage {
            objects,
            end_of_prefix,
        });
    }
    Ok(MirrorSyncPage {
        objects,
        end_of_prefix: true,
    })
}

async fn sync_mirror_to_catalog(
    store: &Operator,
    prefix: &str,
    claim: &SqlSegmentClaim,
    object_budget: usize,
) -> Result<()> {
    let cursor_id = mirror_sync_cursor_id(store, prefix);
    let mut cursor = claim.mirror_sync_cursor(&cursor_id).await?;
    // This state travels with the durable cursor, not the elected pod. A new
    // owner therefore continues both the duration and retained-object count
    // instead of publishing a deceptively short partial rotation.
    let rotation_started_at_ms = cursor
        .rotation_started_at_ms
        .unwrap_or_else(unix_timestamp_millis);
    cursor.rotation_started_at_ms = Some(rotation_started_at_ms);
    let page =
        list_mirror_sync_page(store, prefix, cursor.last_key.as_deref(), object_budget).await?;
    metrics::histogram!("siglake_compactor_mirror_sync_objects").record(page.objects.len() as f64);
    let previous_rotation_objects = if cursor.last_key.is_some() {
        cursor.rotation_objects_examined
    } else {
        0
    };
    let rotation_objects_examined = previous_rotation_objects
        .checked_add(
            u64::try_from(page.objects.len()).context("mirror sync page object count overflow")?,
        )
        .context("mirror sync rotation object count overflow")?;
    for object in &page.objects {
        if claim
            .register(
                &object.id,
                &object.tenant,
                &object.index_id,
                &object.url,
                object.bytes,
                0,
            )
            .await
            .with_context(|| format!("register {}", object.id))?
        {
            metrics::counter!("siglake_compactor_mirror_sync_registered_total").increment(1);
        }
    }
    let next_last_key = if page.end_of_prefix {
        None
    } else {
        page.objects.last().map(|object| object.path.as_str())
    };
    if !claim
        .compare_and_set_mirror_sync_cursor(
            &cursor_id,
            &cursor,
            next_last_key,
            page.end_of_prefix,
            page.objects.len(),
        )
        .await?
    {
        anyhow::bail!(
            "mirror sync cursor changed while registering rotation {} after {:?}",
            cursor.rotation,
            cursor.last_key
        );
    }
    let completed_rotations = cursor.rotation + u64::from(page.end_of_prefix);
    metrics::gauge!("siglake_compactor_mirror_sync_rotations_completed")
        .set(completed_rotations as f64);
    // Publish only cursor progress that won its compare-and-set. On owner
    // handoff the durable count continues from the previous page; after a
    // wrap, the next rotation's first page resets the gauge to that page's
    // count rather than retaining the completed rotation's total.
    metrics::gauge!("siglake_compactor_mirror_sync_rotation_objects_examined")
        .set(rotation_objects_examined as f64);
    if page.end_of_prefix {
        let completed_at_ms = unix_timestamp_millis();
        let duration_ms = completed_at_ms.saturating_sub(rotation_started_at_ms);
        metrics::histogram!("siglake_compactor_mirror_sync_rotation_duration_seconds")
            .record(duration_ms as f64 / 1_000.0);
        metrics::histogram!("siglake_compactor_mirror_sync_rotation_objects")
            .record(rotation_objects_examined as f64);
        metrics::gauge!("siglake_compactor_mirror_sync_last_completed_timestamp_seconds")
            .set(completed_at_ms as f64 / 1_000.0);
        tracing::info!(
            rotation = completed_rotations,
            duration_ms,
            objects_examined = rotation_objects_examined,
            "mirror-to-catalog reconciliation rotation completed"
        );
    } else if let Some(last_completed_at_ms) = cursor.last_completed_at_ms {
        // Re-publish durable completion state after an owner handoff. The
        // timestamp is monotonic, so a dashboard max remains correct while a
        // previous owner's Prometheus series goes stale.
        metrics::gauge!("siglake_compactor_mirror_sync_last_completed_timestamp_seconds")
            .set(last_completed_at_ms as f64 / 1_000.0);
    }
    Ok(())
}

fn release_all(claimed: &[PathBuf]) {
    for c in claimed {
        if let Err(e) = release_segment(c) {
            tracing::error!(path = %c.display(), error = %e, "release failed; manual recovery needed");
        }
    }
}

#[cfg(test)]
mod watchdog_tests {
    use super::*;

    #[tokio::test]
    async fn bounded_passes_and_trips() {
        // No ceiling: never trips.
        assert_eq!(bounded(None, async { 7 }).await, Some(7));
        // Under the ceiling: passes through.
        assert_eq!(
            bounded(Some(Duration::from_secs(5)), async { 7 }).await,
            Some(7)
        );
        // A hung future trips to None instead of wedging the caller.
        let hang = async {
            std::future::pending::<()>().await;
            7
        };
        assert_eq!(bounded(Some(Duration::from_millis(20)), hang).await, None);
    }

    #[test]
    fn watchdog_ceilings_parse_default_disable_and_clamp() {
        // Unset → default.
        assert_eq!(
            watchdog_ceiling_from(None, 600),
            Some(Duration::from_secs(600))
        );
        // 0 → disabled.
        assert_eq!(watchdog_ceiling_from(Some("0"), 600), None);
        // Sub-minimum values clamp up (a tiny ceiling would abort healthy cycles).
        assert_eq!(
            watchdog_ceiling_from(Some("5"), 600),
            Some(Duration::from_secs(60))
        );
        // Garbage → default.
        assert_eq!(
            watchdog_ceiling_from(Some("nope"), 1800),
            Some(Duration::from_secs(1800))
        );
    }

    /// #3143: the attempt budget a local segment gets before the set-aside.
    #[test]
    fn poison_attempts_parse_default_and_disable() {
        assert_eq!(poison_attempts_from(None), DEFAULT_POISON_ATTEMPTS);
        assert_eq!(poison_attempts_from(Some("1")), 1);
        assert_eq!(poison_attempts_from(Some(" 5 ")), 5);
        // 0 keeps the pre-#3143 behaviour: retry forever, set nothing aside.
        assert_eq!(poison_attempts_from(Some("0")), 0);
        // Garbage and negatives → default, never an accidental disable.
        assert_eq!(poison_attempts_from(Some("nope")), DEFAULT_POISON_ATTEMPTS);
        assert_eq!(poison_attempts_from(Some("-1")), DEFAULT_POISON_ATTEMPTS);
        assert_eq!(poison_attempts_from(Some("")), DEFAULT_POISON_ATTEMPTS);
    }
}

/// #3143: the attempt ledger behind the `poison/` set-aside, driven directly
/// so each charge is one "cycle". The drain's own pass retries within a cycle,
/// which makes the same accounting hard to step through end to end.
#[cfg(test)]
mod poison_ledger_tests {
    use super::*;
    use siglake_core::Event;

    fn seal(dir: &Path, n: usize) -> Vec<PathBuf> {
        let mut w =
            siglake_wal::WalWriter::with_thresholds(dir, "ing-1", 1, Duration::from_secs(60))
                .unwrap();
        for i in 0..n {
            w.append_events(&[Event::now(format!("row-{i}"))])
                .unwrap()
                .expect("one row seals the segment");
        }
        list_sealed(dir).unwrap()
    }

    fn unreadable(path: &Path) -> anyhow::Error {
        anyhow::Error::new(UnreadableSegments {
            segments: vec![UnreadableSegment {
                path: path.to_path_buf(),
                reason: "CRC mismatch".to_string(),
            }],
        })
    }

    async fn compactor_at(wal: &Path, warehouse: &Path, attempts: u32) -> Compactor {
        let ice = Arc::new(IcebergContext::open(warehouse).await.unwrap());
        Compactor::new(wal, ice).with_poison_attempts(attempts)
    }

    #[tokio::test]
    async fn only_the_segments_a_failure_names_are_charged_for_it() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        let sealed = seal(&wal, 2);
        // The constructor sweeps `processing/`, so the compactor exists before
        // anything is claimed — as it does in the drain.
        let c = compactor_at(&wal, &tmp.path().join("warehouse"), 2).await;
        let claimed: Vec<PathBuf> = sealed.iter().map(|p| claim_segment(p).unwrap()).collect();

        // A failure that names no segment is the batch's, not any file's: a
        // catalog conflict or a store timeout charges nothing, however often
        // it repeats.
        for _ in 0..5 {
            let (kept, aside) = c.set_aside_unreadable(
                claimed.clone(),
                &anyhow::anyhow!("catalog commit conflict"),
                "default",
            );
            assert_eq!(kept, claimed);
            assert_eq!(aside, 0);
        }

        // The first charge is under the budget: the whole batch comes back.
        let (kept, aside) = c.set_aside_unreadable(claimed.clone(), &unreadable(&claimed[0]), "t");
        assert_eq!(kept, claimed, "nothing is set aside on one failed read");
        assert_eq!(aside, 0);
        assert!(list_poisoned(&wal).unwrap().is_empty());

        // The second spends it. Only the named segment moves; its sibling was
        // never charged for sharing a batch with it.
        let (kept, aside) = c.set_aside_unreadable(claimed.clone(), &unreadable(&claimed[0]), "t");
        assert_eq!(aside, 1);
        assert_eq!(kept, vec![claimed[1].clone()]);
        let held = list_poisoned(&wal).unwrap();
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].file_name(), claimed[0].file_name());
        assert_eq!(siglake_wal::read_poison_note(&held[0]).unwrap().attempts, 2);

        // And the sibling still has its full budget.
        let (_, aside) =
            c.set_aside_unreadable(vec![claimed[1].clone()], &unreadable(&claimed[1]), "t");
        assert_eq!(aside, 0, "the sibling starts from zero");
    }

    #[tokio::test]
    async fn a_segment_that_commits_owes_nothing_for_its_earlier_failures() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        let sealed = seal(&wal, 1);
        // The constructor sweeps `processing/`, so the compactor exists before
        // anything is claimed — as it does in the drain.
        let c = compactor_at(&wal, &tmp.path().join("warehouse"), 2).await;
        let claimed: Vec<PathBuf> = sealed.iter().map(|p| claim_segment(p).unwrap()).collect();

        let (_, aside) = c.set_aside_unreadable(claimed.clone(), &unreadable(&claimed[0]), "t");
        assert_eq!(aside, 0);
        // A cycle later it reads fine and commits.
        c.forget_read_failures(&claimed);
        // So the next failure is its first, not its last.
        let (kept, aside) = c.set_aside_unreadable(claimed.clone(), &unreadable(&claimed[0]), "t");
        assert_eq!(aside, 0, "the ledger counts CONSECUTIVE failures");
        assert_eq!(kept, claimed);
        assert!(list_poisoned(&wal).unwrap().is_empty());
    }

    /// `0` keeps the pre-#3143 behaviour for a deployment that wants it: the
    /// segment is released for retry however many times it fails.
    #[tokio::test]
    async fn a_zero_budget_never_sets_anything_aside() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        let sealed = seal(&wal, 1);
        // The constructor sweeps `processing/`, so the compactor exists before
        // anything is claimed — as it does in the drain.
        let c = compactor_at(&wal, &tmp.path().join("warehouse"), 0).await;
        let claimed: Vec<PathBuf> = sealed.iter().map(|p| claim_segment(p).unwrap()).collect();
        for _ in 0..10 {
            let (kept, aside) =
                c.set_aside_unreadable(claimed.clone(), &unreadable(&claimed[0]), "t");
            assert_eq!(kept, claimed);
            assert_eq!(aside, 0);
        }
        assert!(list_poisoned(&wal).unwrap().is_empty());
    }
}

#[cfg(test)]
mod agg_fold_tests {
    use super::*;
    use siglake_core::Event;
    use siglake_storage::iceberg::IcebergTuning;

    fn walk_files(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_files(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    #[tokio::test]
    async fn aggregate_maintenance_repairs_markers_in_catalog_tenant_namespaces() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let base = Arc::new(IcebergContext::open(&warehouse).await.unwrap().with_tuning(
            IcebergTuning {
                table_group_count_cardinality: Some(8_192),
                result_caches: Some(false),
                ..Default::default()
            },
        ));
        let tenant = base.for_namespace("tenant_acme").await.unwrap();
        let events: Vec<Event> = (0..4_100)
            .map(|i| {
                let mut event = Event::now(format!("row {i}"));
                event.host = format!("host-{i:04}");
                event
            })
            .collect();
        tenant.append_events(&events).await.unwrap();

        let mut deltas: Vec<_> = walk_files(&warehouse)
            .into_iter()
            .filter(|path| {
                path.to_string_lossy().contains("tenant_acme")
                    && path.to_string_lossy().contains("siglake-agg-deltas")
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "json")
            })
            .collect();
        assert_eq!(
            deltas.len(),
            1,
            "precondition: one tenant commit, one delta"
        );
        let delta = deltas.pop().unwrap();
        let sequence_number = delta
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        std::fs::remove_file(delta).unwrap();
        tenant
            .write_group_count_rebuild_marker_for_test(
                tenant.events_table_ident(),
                sequence_number,
                &[("host", 8_192)],
                &[],
            )
            .await
            .unwrap();

        assert_eq!(
            tenant
                .grouped_counts_with_summary("events", "host", None, None)
                .await
                .unwrap()
                .unwrap()
                .source_label(),
            "materialized",
            "the lost tenant delta must initially force the per-file path"
        );

        let compactor = Compactor::new(tmp.path().join("wal"), base);
        compactor.run_agg_fold_once(1).await;

        assert_eq!(
            tenant
                .grouped_counts_with_summary("events", "host", None, None)
                .await
                .unwrap()
                .unwrap()
                .source_label(),
            "tier1_wide",
            "the base compactor must discover and repair the tenant namespace"
        );
    }

    /// #3000 through the compactor's own plumbing: a tenant table short with no
    /// marker anywhere. The storage tests cover the census's rules; what this
    /// covers is that the pass reaches a `tenant_*` namespace at all, and that
    /// its budget is what decides whether any file is read.
    ///
    /// The cardinality here (8,192 against 8,200 distinct hosts over the two
    /// commits) also puts a DEMOTED sketch column in the folded base, which is
    /// the state the repair must survive — see the `None` sketch argument in
    /// `repair_short_group_count_aggregate`.
    #[tokio::test]
    async fn the_short_aggregate_census_reaches_tenant_namespaces_under_its_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let base = Arc::new(IcebergContext::open(&warehouse).await.unwrap().with_tuning(
            IcebergTuning {
                table_group_count_cardinality: Some(8_192),
                result_caches: Some(false),
                ..Default::default()
            },
        ));
        let tenant = base.for_namespace("tenant_acme").await.unwrap();
        let commit = |nth: usize| -> Vec<Event> {
            (0..4_100)
                .map(|i| {
                    let mut event = Event::now(format!("row {nth}-{i}"));
                    event.host = format!("host-{nth}-{i:04}");
                    event
                })
                .collect()
        };
        tenant.append_events(&commit(0)).await.unwrap();
        // No marker: the first commit's contribution simply vanishes, the way a
        // process killed between its commit and its delta PUT leaves it. The
        // second commit's delta lands, so nothing outstanding explains the gap.
        let mut deltas: Vec<_> = walk_files(&warehouse)
            .into_iter()
            .filter(|path| {
                path.to_string_lossy().contains("tenant_acme")
                    && path.to_string_lossy().contains("siglake-agg-deltas")
            })
            .collect();
        assert_eq!(deltas.len(), 1, "precondition: one commit, one delta");
        std::fs::remove_file(deltas.pop().unwrap()).unwrap();
        tenant.append_events(&commit(1)).await.unwrap();
        tenant
            .invalidate_cached_table(tenant.events_table_ident())
            .await;
        assert_eq!(
            tenant
                .grouped_counts_with_summary("events", "host", None, None)
                .await
                .unwrap()
                .unwrap()
                .source_label(),
            "materialized",
            "precondition: short, and no marker to explain it"
        );

        let compactor = Compactor::new(tmp.path().join("wal"), base);
        // A census with no budget reports and reads nothing — the default
        // install, where the counter and the alert are the whole output.
        compactor.run_agg_short_repair_once(0).await;
        assert!(
            !walk_files(&warehouse).iter().any(|path| path
                .file_name()
                .is_some_and(|name| name == "siglake-agg-wide.json")),
            "a census with no repair budget must not write a base object"
        );

        compactor.run_agg_short_repair_once(1).await;
        // This handle is a different `IcebergContext` from the one the pass
        // opened for the namespace, and holds its own memo of the folded base —
        // as a query pod in another process would.
        tenant
            .invalidate_cached_table(tenant.events_table_ident())
            .await;
        assert_eq!(
            tenant
                .grouped_counts_with_summary("events", "host", None, None)
                .await
                .unwrap()
                .unwrap()
                .source_label(),
            "tier1_wide",
            "with a budget, the tenant namespace's short aggregate is rebuilt"
        );
    }
}

#[cfg(test)]
mod backpressure_gate_tests {
    use super::*;
    use siglake_core::Event;
    use siglake_wal::WalWriter;
    use std::sync::Arc;

    fn seal_n(dir: &Path, writer_id: &str, n: usize) {
        // Threshold = n so a single append rolls exactly one sealed segment.
        let mut w = WalWriter::with_thresholds(dir, writer_id, n, Duration::from_secs(60)).unwrap();
        let evs: Vec<Event> = (0..n).map(|i| Event::now(format!("e{i}"))).collect();
        assert!(
            w.append_events(&evs).unwrap().is_some(),
            "should have sealed a segment"
        );
    }

    #[tokio::test]
    async fn pending_sealed_total_sums_legacy_and_tenant_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        let warehouse = tmp.path().join("warehouse");
        // One segment at the legacy top level, two under a tenant subdir.
        seal_n(&wal, "legacy", 4);
        seal_n(&wal.join("acme"), "t1", 4);
        seal_n(&wal.join("acme"), "t2", 4);

        let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let compactor = Compactor::new(&wal, ice);
        // 1 legacy + 2 tenant = 3 sealed segments awaiting drain.
        assert_eq!(compactor.pending_sealed_total(), 3);
    }

    #[test]
    fn max_sealed_gate_defaults_to_256() {
        // No override in this process ⇒ the documented default. (Other tests must
        // not set this env var; it's read fresh each call.)
        assert_eq!(max_sealed_for_recluster(), 256);
    }

    /// The throttled backpressure slot must stay single-bin unless an operator
    /// deliberately widens it. It exists to keep files bounded under sustained
    /// ingest WITHOUT re-starving the drain, so its default is the conservative
    /// side of that trade — the 2026-08-05 arc made it configurable precisely so
    /// the trade can be measured rather than assumed.
    #[test]
    fn backpressure_max_bins_follows_the_memory_derivation() {
        // No longer a hardcoded 1: throttling in-flight bins BELOW what the
        // memory/CPU derivation already permits is a cap with no justification,
        // and it is what held compaction back during ingest (2026-08-27: the
        // windowed browse went 40.9s -> 0.13s once it was lifted).
        //
        // It must track the derivation exactly — including the conservative
        // cases, which is what keeps a small install unchanged: the packaged
        // 1Gi/2-CPU compactor derives 1, and so does a container with no cgroup
        // limit at all.
        assert_eq!(
            backpressure_max_bins_from(None),
            siglake_storage::iceberg::derived_bin_concurrency(),
            "the backpressure slot must not throttle below the derived bin \
             concurrency, nor above it"
        );
        assert!(backpressure_max_bins_from(None) >= 1, "must never be zero");

        // An explicit override still wins.
        assert_eq!(backpressure_max_bins_from(Some("7")), 7);
        assert_eq!(
            backpressure_max_bins_from(Some("0")),
            siglake_storage::iceberg::derived_bin_concurrency(),
            "zero must fall back to the safe derivation"
        );
    }

    #[test]
    fn backpressure_compact_every_defaults_to_4() {
        // Graded budget: ~1 in 4 backlogged cycles runs a throttled compaction.
        assert_eq!(backpressure_compact_every(), 4);
    }

    #[test]
    fn byte_budget_admission_rules() {
        const MB: u64 = 1024 * 1024;
        // Under both caps → admit.
        assert!(admit_batch(1, 4, 100 * MB, 100 * MB, 1024 * MB));
        // Worker-count cap binds regardless of bytes.
        assert!(!admit_batch(4, 4, 0, 1, 1024 * MB));
        // Byte budget binds: an admitted batch may not push the in-flight sum over.
        assert!(!admit_batch(2, 4, 900 * MB, 200 * MB, 1024 * MB));
        // Exactly at the budget is fine.
        assert!(admit_batch(2, 4, 824 * MB, 200 * MB, 1024 * MB));
        // DEADLOCK GUARD: an empty pipeline always admits, even a batch larger
        // than the entire budget — it just flies solo.
        assert!(admit_batch(0, 4, 0, 10_000 * MB, 1024 * MB));
        // Overflow-safe.
        assert!(!admit_batch(1, 4, u64::MAX, u64::MAX, 1024 * MB));
    }
}

#[cfg(test)]
mod drain_schema_drift_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Array, Int64Array, StringArray};
    use std::sync::Arc;

    fn batch(cols: &[&str], n: usize) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|c| {
                Field::new(
                    *c,
                    if *c == "ts" {
                        DataType::Int64
                    } else {
                        DataType::Utf8
                    },
                    true,
                )
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<arrow_array::ArrayRef> = cols
            .iter()
            .map(|c| -> arrow_array::ArrayRef {
                if *c == "ts" {
                    Arc::new(Int64Array::from(vec![1i64; n]))
                } else {
                    Arc::new(StringArray::from(vec![*c; n]))
                }
            })
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    /// The 2026-08-10 wedge: a drain claim spanning two document shapes. The old
    /// code concatenated against `batches[0].schema()`, so the cycle failed with
    /// `concat_batches` and the same segments were re-claimed forever — 19.7M
    /// rows committed against 2.02B accepted, 218,569 segments backed up.
    #[test]
    fn drain_concat_survives_an_additive_schema_migration() {
        let narrow = batch(&["ts", "raw"], 3);
        let wide = batch(&["ts", "raw", "attributes"], 2);

        let out = concat_batches_unified(&[narrow, wide]).expect("must not wedge the drain");
        assert_eq!(out.num_rows(), 5, "every row survives");
        assert_eq!(out.num_columns(), 3, "widest schema wins");
        let attrs = out
            .column(out.schema().index_of("attributes").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            attrs.is_null(0),
            "pre-migration rows null-fill the new column"
        );
        assert!(attrs.is_valid(4), "post-migration rows keep their value");
    }

    /// Order-independence: the wide batch arriving first must behave identically.
    /// `batches[0].schema()` made the outcome depend on claim order, which is why
    /// this wedged intermittently before 4x claims made it near-certain.
    #[test]
    fn drain_concat_is_order_independent() {
        let out = concat_batches_unified(&[
            batch(&["ts", "raw", "attributes"], 2),
            batch(&["ts", "raw"], 3),
        ])
        .expect("wide-first must work too");
        assert_eq!(out.num_rows(), 5);
        assert_eq!(out.num_columns(), 3);
    }

    /// Neither shape contains the other — equal width, disjoint extra columns.
    /// "Take the widest batch" would silently drop a column here; the union must
    /// keep both.
    #[test]
    fn drain_concat_unions_disjoint_shapes() {
        let a = batch(&["ts", "raw", "level"], 2);
        let b = batch(&["ts", "raw", "status"], 3);
        let out = concat_batches_unified(&[a, b]).expect("disjoint shapes must union");
        assert_eq!(out.num_rows(), 5);
        assert_eq!(out.num_columns(), 4, "ts, raw, level, status");
        for c in ["ts", "raw", "level", "status"] {
            assert!(
                out.schema().index_of(c).is_ok(),
                "{c} must survive the union"
            );
        }
    }

    /// The steady state must stay on the original zero-copy path.
    #[test]
    fn drain_concat_uniform_batches_unchanged() {
        let out =
            concat_batches_unified(&[batch(&["ts", "raw"], 2), batch(&["ts", "raw"], 2)]).unwrap();
        assert_eq!(out.num_rows(), 4);
        assert_eq!(out.num_columns(), 2);
    }
}

#[cfg(test)]
mod watermark_incarnation_tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    fn watermark(ms: i64, uuid: Option<&str>) -> ConsumedProofWatermark {
        ConsumedProofWatermark {
            acknowledged_through_ms: ms,
            table_uuid: uuid.map(str::to_string),
        }
    }

    /// #2889: the stored boundary and the append target are both reached by
    /// NAME, so they speak for the same table only when their incarnations
    /// agree. Both `None` is the events table and an index whose ownership
    /// check had no opinion — the same "no verified identity" that leaves the
    /// append itself unfenced.
    #[test]
    fn a_boundary_applies_only_to_the_incarnation_that_established_it() {
        let cases = [
            (None, None, None),
            (Some(watermark(7, None)), None, Some(7)),
            (Some(watermark(7, Some("a"))), Some("a"), Some(7)),
            (Some(watermark(7, Some("a"))), Some("b"), None),
            (Some(watermark(7, Some("a"))), None, None),
            (Some(watermark(7, None)), Some("b"), None),
        ];
        for (stored, verified, expected) in cases {
            assert_eq!(
                Compactor::watermark_for_incarnation(stored.as_ref(), verified),
                expected,
                "stored {stored:?} against verified {verified:?}"
            );
        }
    }

    #[test]
    fn every_known_refusal_reason_is_preregistered_for_an_index() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            Compactor::preregister_proof_watermark_skips("logs");
        }

        let mut series: Vec<(String, String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key().name() == "siglake_compactor_proof_watermark_skipped_total"
            })
            .map(|(key, _, _, value)| {
                let labels: HashMap<_, _> = key
                    .key()
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect();
                let DebugValue::Counter(count) = value else {
                    panic!("proof watermark skip metric is a counter")
                };
                (
                    labels["reason"].to_string(),
                    labels["index"].to_string(),
                    count,
                )
            })
            .collect();
        series.sort();

        assert_eq!(
            series,
            vec![
                ("incarnation_mismatch".to_string(), "logs".to_string(), 0),
                ("unproved_watermark".to_string(), "logs".to_string(), 0),
                (
                    "unverified_append_watermark".to_string(),
                    "logs".to_string(),
                    0,
                ),
            ]
        );
    }

    #[test]
    fn events_do_not_get_impossible_refusal_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            Compactor::preregister_proof_watermark_skips("");
        }
        assert!(snapshotter.snapshot().into_vec().is_empty());
    }
}

#[cfg(test)]
mod durable_reclaim_proof_tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use opendal::services::Memory;
    use siglake_storage::consumed_proof::{
        ConsumedProof, CONSUMED_PROOF_MAX_BYTES, CONSUMED_PROOF_PROP,
    };

    use super::*;

    #[tokio::test]
    async fn mirror_drain_resumes_from_near_cap_by_pruning_terminal_catalog_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let claim_uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let claim = SqlSegmentClaim::connect(&claim_uri, "near-cap-test")
            .await
            .unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );

        let survivor = ConsumedProofEntry {
            segment_id: "still-processing.arrow".to_string(),
            claimed_at_ms: i64::MAX - 2,
        };
        let mut proof = ConsumedProof::empty(Some(10));
        proof.insert(survivor.clone()).unwrap();
        let mut low = 0usize;
        let mut high = CONSUMED_PROOF_MAX_BYTES;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let mut candidate = proof.clone();
            candidate
                .insert(ConsumedProofEntry {
                    segment_id: format!("terminal-{}", "x".repeat(mid)),
                    claimed_at_ms: i64::MAX - 1,
                })
                .unwrap();
            if candidate.encode().is_ok() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let terminal_id = format!("terminal-{}", "x".repeat(low));
        proof
            .insert(ConsumedProofEntry {
                segment_id: terminal_id.clone(),
                claimed_at_ms: i64::MAX - 1,
            })
            .unwrap();
        let table = ice
            .catalog()
            .load_table(ice.events_table_ident())
            .await
            .unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .set(CONSUMED_PROOF_PROP.to_string(), proof.encode().unwrap())
            .apply(tx)
            .unwrap();
        tx.commit(ice.catalog().as_ref()).await.unwrap();
        ice.invalidate_cached_table(ice.events_table_ident()).await;

        claim
            .register(&terminal_id, "default", "", "old", 1, 1)
            .await
            .unwrap();
        assert_eq!(claim.try_claim(1).await.unwrap()[0].id, terminal_id);
        claim.mark_committed(&terminal_id).await.unwrap();

        let source = tmp.path().join("source-wal");
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            &source,
            "near-cap",
            1,
            Duration::from_secs(60),
        )
        .unwrap();
        let segment = writer
            .append_events(&[siglake_core::Event::now("once")])
            .unwrap()
            .unwrap();
        let filename = segment.path.file_name().unwrap().to_str().unwrap();
        let new_id = filename.strip_suffix(".arrow").unwrap();
        let key = format!("wal-mirror/{filename}");
        let store = Operator::new(Memory::default()).unwrap().finish();
        store
            .write(&key, Bytes::from(std::fs::read(&segment.path).unwrap()))
            .await
            .unwrap();
        claim
            .register(new_id, "default", "", &key, segment.bytes as i64, 1)
            .await
            .unwrap();

        let compactor = Compactor::new(tmp.path().join("drain-wal"), ice.clone())
            .with_catalog_claim(CatalogClaimConfig {
                claim: claim.clone(),
                store,
                prefix: "wal-mirror".to_string(),
                batch_size: 1,
                last_mirror_sync: Default::default(),
                last_reclaim: Default::default(),
            });
        assert_eq!(compactor.run_once().await.unwrap(), 1);
        assert_eq!(compactor.run_once().await.unwrap(), 0);

        let sources = ice.reclaim_proof_sources("events").await.unwrap();
        let ConsumedProofRead::Valid(resumed) = sources.durable else {
            panic!("durable proof missing after resumed drain")
        };
        assert!(!resumed.contains(&terminal_id));
        assert!(resumed.contains(&survivor.segment_id));
        assert!(resumed.contains(filename));
        let rows: u64 = ice
            .live_data_files(ice.events_table_ident())
            .await
            .unwrap()
            .iter()
            .map(|file| file.record_count())
            .sum();
        assert_eq!(rows, 1, "retrying the drain must not duplicate rows");
    }

    /// Run 65's failure mode on both filesystem commit targets. The durable
    /// proof is valid but close enough to its cap that the next four segment
    /// IDs cannot be appended unless entries certified by the WAL lifecycle
    /// are removed in the same transaction.
    #[tokio::test]
    async fn filesystem_drain_resumes_from_near_cap_without_pruning_live_ids() {
        for index_id in [None, Some("near_cap_index")] {
            let tmp = tempfile::tempdir().unwrap();
            let wal_root = tmp.path().join("wal");
            let ice = Arc::new(
                IcebergContext::open(&tmp.path().join("warehouse"))
                    .await
                    .unwrap(),
            );

            let (wal_dir, ident, table_uuid) = if let Some(index_id) = index_id {
                std::fs::create_dir_all(wal_root.join("default/sealed")).unwrap();
                let mut config = IndexConfig::builtin_events();
                config.index_id = index_id.to_string();
                ice.create_index(&config).await.unwrap();
                let table_uuid = ice.index_table_uuid(index_id).await.unwrap().unwrap();
                (
                    wal_root.join("default").join(index_id),
                    ice.index_table_ident(index_id),
                    Some(table_uuid),
                )
            } else {
                (wal_root.clone(), ice.events_table_ident().clone(), None)
            };

            let sealed_survivor = "zzzz-still-sealed.arrow";
            let processing_survivor = "still-processing.arrow";
            let mut proof = ConsumedProof::empty(Some(10));
            for segment_id in [sealed_survivor, processing_survivor] {
                proof
                    .insert(ConsumedProofEntry {
                        segment_id: segment_id.to_string(),
                        claimed_at_ms: i64::MAX,
                    })
                    .unwrap();
            }

            // Fixed-width realistic filenames make the encoded contribution
            // constant, so the proof can be filled to within one entry of the
            // cap without repeatedly serializing an ever-growing map.
            let terminal_id = |n: usize| format!("terminal-{n:05}-{}.arrow", "x".repeat(80));
            let baseline_len = proof.encode().unwrap().len();
            let mut one_more = proof.clone();
            one_more
                .insert(ConsumedProofEntry {
                    segment_id: terminal_id(0),
                    claimed_at_ms: i64::MAX - 1,
                })
                .unwrap();
            let contribution = one_more.encode().unwrap().len() - baseline_len;
            let terminal_count = (CONSUMED_PROOF_MAX_BYTES - baseline_len) / contribution;
            assert!(terminal_count < 100_000, "the IDs must stay fixed-width");

            let committed_dir = wal_dir.join(siglake_wal::COMMITTED_DIR);
            std::fs::create_dir_all(&committed_dir).unwrap();
            let mut terminal_ids = Vec::with_capacity(terminal_count);
            for n in 0..terminal_count {
                let id = terminal_id(n);
                proof
                    .insert(ConsumedProofEntry {
                        segment_id: id.clone(),
                        claimed_at_ms: i64::MAX - 1,
                    })
                    .unwrap();
                std::fs::write(committed_dir.join(&id), []).unwrap();
                terminal_ids.push(id);
            }
            let encoded = proof.encode().unwrap();
            assert!(
                CONSUMED_PROOF_MAX_BYTES - encoded.len() < contribution,
                "precondition: proof is within one terminal entry of the cap"
            );

            let mut writer = siglake_wal::WalWriter::with_thresholds(
                &wal_dir,
                "near-cap-fs",
                1,
                Duration::from_secs(60),
            )
            .unwrap();
            if let Some(table_uuid) = table_uuid.as_deref() {
                writer
                    .bind_table_uuid(Some(uuid::Uuid::parse_str(table_uuid).unwrap()))
                    .unwrap();
            }
            let mut new_ids = Vec::new();
            for n in 0..4 {
                let segment = writer
                    .append_events(&[siglake_core::Event::now(format!("row-{n}"))])
                    .unwrap()
                    .unwrap();
                new_ids.push(
                    segment
                        .path
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string(),
                );
            }
            let mut without_pruning = proof.clone();
            for id in &new_ids {
                without_pruning
                    .insert(ConsumedProofEntry {
                        segment_id: id.clone(),
                        claimed_at_ms: i64::MAX,
                    })
                    .unwrap();
            }
            assert!(
                without_pruning.encode().is_err(),
                "precondition: the filesystem append crosses the proof cap"
            );

            let table = ice.catalog().load_table(&ident).await.unwrap();
            let tx = Transaction::new(&table);
            let tx = tx
                .update_table_properties()
                .set(CONSUMED_PROOF_PROP.to_string(), encoded)
                .apply(tx)
                .unwrap();
            tx.commit(ice.catalog().as_ref()).await.unwrap();
            ice.invalidate_cached_table(&ident).await;

            // Construct before placing the processing survivor: startup
            // recovery correctly quarantines leftovers from an older process,
            // while this file models a live sibling claim in the same process.
            let compactor = Compactor::new(&wal_root, ice.clone())
                .with_fs_batch_limits(4, 0)
                .with_drain_concurrency(1)
                .with_drain_cycle_budget(Duration::from_millis(100));
            std::fs::create_dir_all(wal_dir.join(siglake_wal::PROCESSING_DIR)).unwrap();
            std::fs::write(
                wal_dir
                    .join(siglake_wal::PROCESSING_DIR)
                    .join(processing_survivor),
                [],
            )
            .unwrap();
            // This sorts after UUID segment names. Scanning the thousands of
            // committed proof entries makes the first append outlive the
            // cycle's 100 ms dispatch budget, so the tail remains sealed.
            std::fs::write(
                wal_dir.join(siglake_wal::SEALED_DIR).join(sealed_survivor),
                [],
            )
            .unwrap();

            assert_eq!(compactor.run_once().await.unwrap(), 4);

            let ConsumedProofRead::Valid(resumed) = ice
                .durable_consumed_proof(index_id.unwrap_or("events"))
                .await
                .unwrap()
            else {
                panic!("durable proof missing after filesystem drain")
            };
            assert!(terminal_ids.iter().all(|id| !resumed.contains(id)));
            assert!(resumed.contains(sealed_survivor));
            assert!(resumed.contains(processing_survivor));
            assert!(new_ids.iter().all(|id| resumed.contains(id)));
            assert!(wal_dir
                .join(siglake_wal::SEALED_DIR)
                .join(sealed_survivor)
                .exists());
            assert!(wal_dir
                .join(siglake_wal::PROCESSING_DIR)
                .join(processing_survivor)
                .exists());

            let rows: u64 = ice
                .live_data_files(&ident)
                .await
                .unwrap()
                .iter()
                .map(|file| file.record_count())
                .sum();
            assert_eq!(rows, 4, "each drained segment must appear exactly once");
        }
    }

    /// Failure after the Iceberg append and before the catalog mark used to lose
    /// its only positive evidence once 100 newer snapshots displaced it. The
    /// table property must survive both expiry and a real rewrite commit.
    #[tokio::test(flavor = "current_thread")]
    async fn reclaim_proof_survives_snapshot_expiry_and_recluster_commits() {
        let tmp = tempfile::tempdir().unwrap();
        let claim_uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let claim = SqlSegmentClaim::connect(&claim_uri, "proof-test")
            .await
            .unwrap();
        claim
            .register("failed-mark", "default", "", "unused", 1, 1)
            .await
            .unwrap();
        let claimed = claim.try_claim(1).await.unwrap().pop().unwrap();

        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let original =
            siglake_core::events_to_record_batch(&[siglake_core::Event::now("failed-mark-row")])
                .unwrap();
        ice.append_batch_with_consumed_proof(
            original,
            &[ConsumedProofEntry {
                segment_id: "failed-mark.arrow".to_string(),
                claimed_at_ms: claimed.claimed_at.timestamp_millis(),
            }],
            None,
            None,
        )
        .await
        .unwrap();
        // Deliberate failpoint: do not call mark_committed_batch.
        let proving_snapshot = ice.current_events_snapshot_id().await.unwrap().unwrap();

        // More than 100 later snapshots, one of them a real data-file rewrite.
        for n in 0..2 {
            ice.append_events(&[siglake_core::Event::now(format!("pre-recluster-{n}"))])
                .await
                .unwrap();
        }
        let ident = ice.events_table_ident().clone();
        let files = ice.live_data_files(&ident).await.unwrap();
        assert!(files.len() >= 2);
        ice.recluster_files(
            &ident,
            files.into_iter().take(2).collect(),
            siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
        )
        .await
        .unwrap();
        for n in 0..99 {
            ice.append_events(&[siglake_core::Event::now(format!("post-recluster-{n}"))])
                .await
                .unwrap();
        }

        ice.expire_snapshots(&ident, 100).await.unwrap();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert!(
            table
                .metadata()
                .snapshots()
                .all(|snapshot| snapshot.snapshot_id() != proving_snapshot),
            "the snapshot-local proof must actually be expired"
        );
        let rows_before_reclaim: u64 = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|file| file.record_count())
            .sum();

        tokio::time::sleep(Duration::from_millis(5)).await;
        let store = Operator::new(Memory::default()).unwrap().finish();
        let cfg = CatalogClaimConfig {
            claim: claim.clone(),
            store,
            prefix: "wal-mirror".to_string(),
            batch_size: 1,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        };
        let compactor = Compactor::new(tmp.path().join("wal"), ice.clone());

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            metrics::counter!("siglake_compactor_reclaim_unprovable_total").increment(0);
            compactor
                .reclaim_with_proof_older_than(&cfg, Duration::ZERO)
                .await
                .unwrap();
            // Idempotence: the terminal row is no longer selected.
            compactor
                .reclaim_with_proof_older_than(&cfg, Duration::ZERO)
                .await
                .unwrap();
        }
        let unprovable = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, _, _, _)| key.key().name() == "siglake_compactor_reclaim_unprovable_total")
            .expect("counter was pre-registered in the test");
        assert!(matches!(unprovable.3, DebugValue::Counter(0)));

        let rows_after_reclaim: u64 = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .iter()
            .map(|file| file.record_count())
            .sum();
        assert_eq!(rows_after_reclaim, rows_before_reclaim);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let committed = claim.purgeable_committed(Duration::ZERO, 10).await.unwrap();
        assert_eq!(
            committed,
            vec![("failed-mark".to_string(), "unused".to_string())]
        );

        // The reclaim mark advanced the catalog watermark. A later maintenance
        // commit compacts the terminal ID even if the table receives no append.
        let watermark = claim
            .consumed_proof_watermark("default", "")
            .await
            .unwrap()
            .expect("the reclaim mark establishes a watermark");
        // The events table carries no incarnation (#2889): its name has no
        // recreate path to straddle, so there is nothing to fence against.
        assert_eq!(watermark.table_uuid, None);
        ice.compact_consumed_proof(&ident, watermark.acknowledged_through_ms, None)
            .await
            .unwrap();
        let sources = ice.reclaim_proof_sources("events").await.unwrap();
        let ConsumedProofRead::Valid(proof) = sources.durable else {
            panic!("durable proof missing after compaction")
        };
        assert!(!proof.contains("failed-mark.arrow"));
    }

    #[tokio::test]
    async fn reclaim_dual_read_accepts_legacy_snapshot_when_property_is_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let claim_uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let claim = SqlSegmentClaim::connect(&claim_uri, "legacy-test")
            .await
            .unwrap();
        claim
            .register("legacy", "default", "", "unused", 1, 1)
            .await
            .unwrap();
        let claimed = claim.try_claim(1).await.unwrap().pop().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.append_batch_with_consumed_proof(
            siglake_core::events_to_record_batch(&[siglake_core::Event::now("legacy-row")])
                .unwrap(),
            &[ConsumedProofEntry {
                segment_id: "legacy.arrow".to_string(),
                claimed_at_ms: claimed.claimed_at.timestamp_millis(),
            }],
            None,
            None,
        )
        .await
        .unwrap();

        // Model metadata last written by an old binary: the snapshot summary
        // exists, while the new durable property does not.
        let ident = ice.events_table_ident().clone();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_table_properties()
            .remove(siglake_storage::consumed_proof::CONSUMED_PROOF_PROP.to_string())
            .apply(tx)
            .unwrap();
        tx.commit(ice.catalog().as_ref()).await.unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;
        let cfg = CatalogClaimConfig {
            claim: claim.clone(),
            store: Operator::new(Memory::default()).unwrap().finish(),
            prefix: "wal-mirror".to_string(),
            batch_size: 1,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        };
        Compactor::new(tmp.path().join("wal"), ice)
            .reclaim_with_proof_older_than(&cfg, Duration::ZERO)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            claim.purgeable_committed(Duration::ZERO, 10).await.unwrap(),
            vec![("legacy".to_string(), "unused".to_string())]
        );
    }
}

#[cfg(test)]
mod committed_retention_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use opendal::services::Memory;

    use super::*;

    async fn claim(tmp: &std::path::Path) -> SqlSegmentClaim {
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("claim.db").display());
        SqlSegmentClaim::connect(&uri, "retention-test")
            .await
            .unwrap()
    }

    async fn compactor(
        tmp: &std::path::Path,
        claim: SqlSegmentClaim,
        store: Operator,
    ) -> Compactor {
        let ice = Arc::new(IcebergContext::open(&tmp.join("warehouse")).await.unwrap());
        Compactor::new(tmp.join("wal"), ice).with_catalog_claim(CatalogClaimConfig {
            claim,
            store,
            prefix: "wal-mirror".to_string(),
            batch_size: 1,
            last_mirror_sync: Default::default(),
            last_reclaim: Default::default(),
        })
    }

    async fn register_committed(claim: &SqlSegmentClaim, store: &Operator, ids: &[String]) {
        for id in ids {
            let key = format!("wal-mirror/{id}.arrow");
            store.write(&key, Bytes::from_static(b"x")).await.unwrap();
            claim.register(id, "default", "", &key, 1, 1).await.unwrap();
        }
        assert_eq!(claim.try_claim(ids.len()).await.unwrap().len(), ids.len());
        claim
            .mark_committed_batch(ids, &ProofProvenance::default())
            .await
            .unwrap();
        // The age predicate is strict at millisecond precision.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    #[test]
    fn missing_and_empty_default_to_twenty_four_hours() {
        for configured in [None, Some("")] {
            assert_eq!(
                committed_retention_from(configured),
                Some(Duration::from_secs(DEFAULT_COMMITTED_RETENTION_SECS))
            );
        }
    }

    #[test]
    fn zero_disables_committed_retention() {
        assert_eq!(committed_retention_from(Some("0")), None);
    }

    #[test]
    fn positive_seconds_enable_committed_retention() {
        assert_eq!(
            committed_retention_from(Some("1021")),
            Some(Duration::from_secs(1021))
        );
    }

    #[test]
    fn positive_values_below_the_recovery_floor_are_raised() {
        for configured in ["1", "900", "901"] {
            assert_eq!(
                committed_retention_from(Some(configured)),
                Some(Duration::from_secs(MIN_COMMITTED_RETENTION_SECS)),
                "{configured:?} must not shorten the local-WAL recovery window"
            );
        }
    }

    #[test]
    fn invalid_values_disable_committed_retention() {
        for configured in ["invalid", "-1"] {
            assert_eq!(
                committed_retention_from(Some(configured)),
                None,
                "{configured:?} must preserve the fail-closed never-purge behavior"
            );
        }
    }

    #[test]
    fn run_budget_outpaces_the_documented_creation_rate() {
        const DOCUMENTED_EPS: usize = 50_000;
        const ROWS_PER_SEGMENT: usize = 4_096;
        const DEFAULT_WATCHDOG_SECS: usize = 600;
        let created_during_watchdog =
            (DOCUMENTED_EPS * DEFAULT_WATCHDOG_SECS).div_ceil(ROWS_PER_SEGMENT);
        assert!(
            RETENTION_RUN_OBJECTS >= created_during_watchdog * 2,
            "the bounded run must retain at least 2x headroom over the documented segment rate"
        );
    }

    #[tokio::test]
    async fn retention_drains_pages_only_to_its_object_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Operator::new(Memory::default()).unwrap().finish();
        let claim = claim(tmp.path()).await;
        let ids: Vec<String> = (0..5).map(|n| format!("segment-{n}")).collect();
        register_committed(&claim, &store, &ids).await;
        let retention = compactor(tmp.path(), claim.clone(), store.clone()).await;

        assert_eq!(
            retention.run_retention_bounded(Duration::ZERO, 3, 2).await,
            3,
            "two catalog pages must drain, but the third object is the hard run bound"
        );
        let remaining = claim.purgeable_committed(Duration::ZERO, 10).await.unwrap();
        assert_eq!(remaining.len(), 2, "the run exceeded its object budget");
        for (_, key) in &remaining {
            assert!(
                store.exists(key).await.unwrap(),
                "retention removed an object without removing its catalog row"
            );
        }

        assert_eq!(
            retention.run_retention_bounded(Duration::ZERO, 10, 2).await,
            2
        );
        assert!(claim
            .purgeable_committed(Duration::ZERO, 10)
            .await
            .unwrap()
            .is_empty());
    }

    /// A failed object delete is the observable boundary that proves the
    /// catalog row is second. If the row disappeared here, mirror recovery
    /// could register the still-present object as new and drain it twice.
    #[cfg(unix)]
    #[tokio::test]
    async fn object_delete_failure_keeps_the_committed_row() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp = tempfile::tempdir().unwrap();
        let store_root = tmp.path().join("store");
        std::fs::create_dir_all(&store_root).unwrap();
        let store =
            Operator::new(opendal::services::Fs::default().root(store_root.to_str().unwrap()))
                .unwrap()
                .finish();
        let claim = claim(tmp.path()).await;
        let ids = vec!["segment".to_string()];
        register_committed(&claim, &store, &ids).await;
        let retention = compactor(tmp.path(), claim.clone(), store.clone()).await;

        let object_dir = store_root.join("wal-mirror");
        std::fs::set_permissions(&object_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let purged = retention.run_retention_bounded(Duration::ZERO, 1, 1).await;
        std::fs::set_permissions(&object_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(purged, 0);
        assert!(store.exists("wal-mirror/segment.arrow").await.unwrap());
        assert_eq!(
            claim
                .purgeable_committed(Duration::ZERO, 10)
                .await
                .unwrap()
                .len(),
            1,
            "the catalog row was removed before its mirror object"
        );

        assert_eq!(
            retention.run_retention_bounded(Duration::ZERO, 1, 1).await,
            1
        );
        assert!(!store.exists("wal-mirror/segment.arrow").await.unwrap());
        assert!(claim
            .purgeable_committed(Duration::ZERO, 10)
            .await
            .unwrap()
            .is_empty());
    }
}

/// Ledger-only mirror reclamation under the filesystem drain (#4913,
/// `docs/DESIGN_wal_mirror_reclamation.md` Option C).
///
/// Each test seeds what a default install seeds: a segment sealed into the
/// local WAL, its bytes in the mirror, and the `sealed` row the ingester's
/// registrar writes for that upload. What varies is the state the reclaimer
/// has to read it in.
#[cfg(test)]
mod mirror_ledger_reclaim_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use opendal::services::Memory;

    use super::*;

    struct Seeded {
        compactor: Compactor,
        claim: SqlSegmentClaim,
        store: Operator,
        wal: PathBuf,
        id: String,
        key: String,
    }

    /// A drain with ledger reclamation attached, a mirrored segment in
    /// `sealed/`, and the ingester's row for it. `retention` is the local
    /// `committed/` soft floor: `ZERO` lets the sweep act on the same cycle.
    async fn seed(tmp: &std::path::Path, retention: Duration, register: bool) -> Seeded {
        let wal = tmp.join("wal");
        let store = Operator::new(Memory::default()).unwrap().finish();
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("claim.db").display());
        let claim = SqlSegmentClaim::connect(&uri, "reclaim-test")
            .await
            .unwrap();

        let mut writer =
            siglake_wal::WalWriter::with_thresholds(&wal, "ing-a", 2, Duration::from_secs(60))
                .unwrap();
        let segment = writer
            .append_events(&[
                siglake_core::Event::now("one".to_string()),
                siglake_core::Event::now("two".to_string()),
            ])
            .unwrap()
            .expect("seals at the threshold");
        drop(writer);
        let id = segment
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap()
            .to_string();
        let key = format!("wal-mirror/{id}.arrow");
        let bytes = std::fs::read(&segment.path).unwrap();
        let len = bytes.len() as i64;
        store.write(&key, Bytes::from(bytes)).await.unwrap();
        if register {
            claim
                .register(&id, "default", "", &key, len, 2)
                .await
                .unwrap();
        }

        let ice = Arc::new(IcebergContext::open(&tmp.join("warehouse")).await.unwrap());
        let compactor = Compactor::with_retention(&wal, ice, retention).with_mirror_ledger(
            CatalogClaimConfig {
                claim: claim.clone(),
                store: store.clone(),
                prefix: "wal-mirror".to_string(),
                batch_size: 0,
                last_mirror_sync: Default::default(),
                last_reclaim: Default::default(),
            },
        );
        Seeded {
            compactor,
            claim,
            store,
            wal,
            id,
            key,
        }
    }

    async fn purgeable(claim: &SqlSegmentClaim) -> Vec<String> {
        claim
            .purgeable_committed(Duration::ZERO, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, key)| key)
            .collect()
    }

    fn committed_files(wal: &std::path::Path) -> Vec<String> {
        list_committed(wal)
            .unwrap_or_default()
            .iter()
            .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(str::to_string))
            .collect()
    }

    /// The whole point: a segment the local drain committed has its mirror
    /// object and its catalog row removed once `committedRetentionSecs` is up.
    #[tokio::test]
    async fn a_locally_committed_segment_is_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), DEFAULT_RETENTION, true).await;

        assert_eq!(s.compactor.run_once().await.unwrap(), 1);
        assert_eq!(
            purgeable(&s.claim).await,
            vec![s.key.clone()],
            "the drain's commit must transition the ingester's row"
        );
        // The age predicate is strict at millisecond precision.
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            s.compactor.run_retention_with(Some(Duration::ZERO)).await,
            1
        );
        assert!(!s.store.exists(&s.key).await.unwrap());
        assert!(purgeable(&s.claim).await.is_empty());
    }

    /// The negative control, driven through the resolver rather than the
    /// process environment: `SIGLAKE_COMMITTED_RETENTION_SECS=0` still means
    /// delete nothing, reclamation or not.
    #[tokio::test]
    async fn the_retention_opt_out_reclaims_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), DEFAULT_RETENTION, true).await;

        assert_eq!(s.compactor.run_once().await.unwrap(), 1);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            s.compactor
                .run_retention_with(committed_retention_from(Some("0")))
                .await,
            0
        );
        assert!(s.store.exists(&s.key).await.unwrap());
        assert_eq!(purgeable(&s.claim).await, vec![s.key]);
    }

    /// A segment with a live `mirror-pending/` pin still owes an upload.
    /// Marking it would let retention delete a key the uploader is about to
    /// write — an object no later pass revisits. So it is not marked, and its
    /// local evidence is held rather than swept.
    #[tokio::test]
    async fn a_pinned_segment_is_neither_marked_nor_swept() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), Duration::ZERO, true).await;
        let name = format!("{}.arrow", s.id);
        let pin = s.wal.join("mirror-pending").join(&name);
        std::fs::create_dir_all(pin.parent().unwrap()).unwrap();
        std::fs::hard_link(s.wal.join("sealed").join(&name), &pin).unwrap();

        assert_eq!(s.compactor.run_once().await.unwrap(), 1);
        assert!(
            purgeable(&s.claim).await.is_empty(),
            "a pinned segment must not be marked committed"
        );
        assert_eq!(
            committed_files(&s.wal),
            vec![name.clone()],
            "and its local evidence must be held, despite a zero retention floor"
        );

        // The upload lands and the pin clears: the next cycle marks it.
        std::fs::remove_file(&pin).unwrap();
        s.compactor.run_once().await.unwrap();
        assert_eq!(purgeable(&s.claim).await, vec![s.key.clone()]);
        assert!(
            committed_files(&s.wal).is_empty(),
            "once the mark is durable the local copy is releasable"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            s.compactor.run_retention_with(Some(Duration::ZERO)).await,
            1
        );
        assert!(!s.store.exists(&s.key).await.unwrap());
    }

    /// The crash the mark step is driven off `committed/` to repair: the
    /// Iceberg append returned and the rename happened, then the process died
    /// before marking. The next cycle finds the file and marks it, so the
    /// object is still reclaimed.
    #[tokio::test]
    async fn a_crash_between_the_append_and_the_mark_is_repaired() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), DEFAULT_RETENTION, true).await;
        let name = format!("{}.arrow", s.id);
        let committed = s.wal.join("committed").join(&name);
        std::fs::create_dir_all(committed.parent().unwrap()).unwrap();
        std::fs::rename(s.wal.join("sealed").join(&name), &committed).unwrap();

        // An idle cycle: nothing to commit, everything to repair.
        assert_eq!(s.compactor.run_once().await.unwrap(), 0);
        assert_eq!(purgeable(&s.claim).await, vec![s.key.clone()]);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            s.compactor.run_retention_with(Some(Duration::ZERO)).await,
            1
        );
        assert!(!s.store.exists(&s.key).await.unwrap());
    }

    /// The mark has to be an UPSERT. The uploader's registration is
    /// `ON CONFLICT DO NOTHING`, so a `committed` row written first survives a
    /// late catch-up registration and retention still collects the object. An
    /// update-only mark would find no row, write nothing, and leak.
    #[tokio::test]
    async fn a_mark_that_precedes_the_registration_still_collects() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), DEFAULT_RETENTION, false).await;

        assert_eq!(s.compactor.run_once().await.unwrap(), 1);
        assert_eq!(
            purgeable(&s.claim).await,
            vec![s.key.clone()],
            "the mark must insert the row the upload has not registered yet"
        );
        // The uploader's registrar catches up afterwards.
        assert!(!s
            .claim
            .register(&s.id, "default", "", &s.key, 1, 2)
            .await
            .unwrap());
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            s.compactor.run_retention_with(Some(Duration::ZERO)).await,
            1
        );
        assert!(!s.store.exists(&s.key).await.unwrap());
        assert!(purgeable(&s.claim).await.is_empty());
    }

    /// Segments of a dropped incarnation are quarantined into `stale/` by the
    /// drain's owner check and never commit (the quarantine itself is covered
    /// by `a_recreated_index_does_not_claim_the_dropped_incarnations_mirror`).
    /// They are therefore never marked, and reclamation produces no delete:
    /// an unattributable object is one only an operator-side lifecycle rule
    /// may collect.
    #[tokio::test]
    async fn quarantined_segments_produce_no_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), Duration::ZERO, true).await;
        let name = format!("{}.arrow", s.id);
        let quarantine = s.wal.join("stale").join("00000000-dropped");
        std::fs::create_dir_all(&quarantine).unwrap();
        std::fs::rename(s.wal.join("sealed").join(&name), quarantine.join(&name)).unwrap();

        assert_eq!(s.compactor.run_once().await.unwrap(), 0);
        assert!(
            purgeable(&s.claim).await.is_empty(),
            "a quarantined segment must never be marked committed"
        );
        assert_eq!(
            s.compactor.run_retention_with(Some(Duration::ZERO)).await,
            0
        );
        assert!(s.store.exists(&s.key).await.unwrap());
        assert!(quarantine.join(&name).exists());
    }

    /// Without the opt-in nothing changes: no mark, no reclamation, and the
    /// local sweep is ungated exactly as before.
    #[tokio::test]
    async fn reclamation_is_off_unless_it_is_attached() {
        let tmp = tempfile::tempdir().unwrap();
        let s = seed(tmp.path(), Duration::ZERO, true).await;
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse2"))
                .await
                .unwrap(),
        );
        let plain = Compactor::with_retention(&s.wal, ice, Duration::ZERO);

        assert_eq!(plain.run_once().await.unwrap(), 1);
        assert!(
            purgeable(&s.claim).await.is_empty(),
            "the row stays sealed forever, which is the limitation this closes"
        );
        assert!(
            committed_files(&s.wal).is_empty(),
            "and the ungated sweep still removes the local copy"
        );
        assert_eq!(plain.run_retention_with(Some(Duration::ZERO)).await, 0);
        assert!(s.store.exists(&s.key).await.unwrap());
    }

    #[test]
    fn the_reclaim_knob_is_off_by_default_and_reads_the_usual_spellings() {
        assert!(!mirror_ledger_reclaim_from(None));
        for off in ["", "0", "false", "no", "off", "maybe"] {
            assert!(!mirror_ledger_reclaim_from(Some(off)), "{off:?}");
        }
        for on in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(mirror_ledger_reclaim_from(Some(on)), "{on:?}");
        }
    }
}

#[cfg(test)]
mod mirror_sync_gate_tests {
    use super::mirror_sync_interval_from;

    /// `0` must mean EVERY LOOP, and only explicit words must disable.
    ///
    /// The remediation plan calls this out because overloading 0 as "off" is how
    /// a fleet-wide recovery scan gets left running by accident: at 218,569
    /// mirror objects across 16 drains, one wave is ~3.5M redundant registration
    /// attempts and 16 full prefix listings.
    #[test]
    fn only_explicit_words_disable_mirror_sync() {
        for word in ["off", "OFF", "disabled", "never", " off "] {
            assert!(
                mirror_sync_interval_from(Some(word)).is_none(),
                "{word:?} must disable mirror sync"
            );
        }
        // 0 is NOT off — it is every loop.
        assert_eq!(
            mirror_sync_interval_from(Some("0")),
            Some(std::time::Duration::ZERO),
            "0 must remain 'every loop', not 'off'"
        );
        assert_eq!(
            mirror_sync_interval_from(Some("30")),
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            mirror_sync_interval_from(None),
            Some(std::time::Duration::from_secs(60))
        );
    }
}

#[cfg(test)]
mod agg_short_repair_knob_tests {
    use super::{
        agg_short_repair_enabled_from, agg_short_repair_max_tables_from,
        agg_short_scan_interval_from,
    };
    use std::time::Duration;

    /// Unlike the mirror sweep, `0` here IS off: a census on every compactor
    /// loop would re-read every table's folded base once a second to answer a
    /// question whose answer changes only when something commits or rebuilds.
    #[test]
    fn the_census_cadence_reads_its_disable_words_and_zero() {
        for word in ["off", "OFF", "disabled", "never", "0", " off "] {
            assert!(
                agg_short_scan_interval_from(Some(word)).is_none(),
                "{word:?} must disable the census"
            );
        }
        assert_eq!(
            agg_short_scan_interval_from(Some("300")),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            agg_short_scan_interval_from(None),
            Some(Duration::from_secs(900)),
            "the default cadence"
        );
        assert_eq!(
            agg_short_scan_interval_from(Some("nonsense")),
            Some(Duration::from_secs(900)),
            "an unparseable value falls back to the default rather than to \
             every loop"
        );
    }

    /// The repair is one Tier-2 query per maintained column, so only an
    /// explicit opt-in may turn it on — anything else leaves the census
    /// reporting and the operator's rebuild the remedy.
    #[test]
    fn only_an_explicit_opt_in_enables_the_repair() {
        for word in ["1", "true", "TRUE", "yes", "on", " on "] {
            assert!(
                agg_short_repair_enabled_from(Some(word)),
                "{word:?} must enable the repair"
            );
        }
        for word in ["0", "false", "no", "off", "", "maybe"] {
            assert!(
                !agg_short_repair_enabled_from(Some(word)),
                "{word:?} must not enable the repair"
            );
        }
        assert!(!agg_short_repair_enabled_from(None), "default is off");
    }

    #[test]
    fn the_per_pass_repair_budget_is_at_least_one_table() {
        assert_eq!(agg_short_repair_max_tables_from(None), 1);
        assert_eq!(agg_short_repair_max_tables_from(Some("4")), 4);
        // 0 would make the interval gate a no-op that silently never repairs;
        // switching the repair off is what the enable knob is for.
        assert_eq!(agg_short_repair_max_tables_from(Some("0")), 1);
        assert_eq!(agg_short_repair_max_tables_from(Some("nonsense")), 1);
    }

    /// #4674's census reads the same vocabulary. Its default cadence is also
    /// what `SiglakeInlineCoverageUnproven`'s `for: 30m` is sized against —
    /// two consecutive passes — so a change here is a change to the alert.
    #[test]
    fn the_inline_coverage_cadence_reads_its_disable_words_and_zero() {
        for word in ["off", "OFF", "disabled", "never", "0", " off "] {
            assert!(
                super::inline_coverage_scan_interval_from(Some(word)).is_none(),
                "{word:?} must disable the census"
            );
        }
        assert_eq!(
            super::inline_coverage_scan_interval_from(Some("300")),
            Some(Duration::from_secs(300))
        );
        assert_eq!(
            super::inline_coverage_scan_interval_from(None),
            Some(Duration::from_secs(900)),
            "the default cadence, and half of the alert's 30-minute window"
        );
        assert_eq!(
            super::inline_coverage_scan_interval_from(Some("nonsense")),
            Some(Duration::from_secs(900))
        );
    }

    /// A dropped index must not page until the process restarts: the gauge has
    /// no delete, so the pass that stops reaching a table has to zero it. A
    /// table the pass merely could not READ stays in the seen set
    /// (`InlineCoverageOutcome::Undetermined`) and keeps its reading.
    #[test]
    fn a_table_the_census_no_longer_reaches_is_zeroed() {
        let entry = |ns: &str, t: &str| (ns.to_string(), t.to_string());
        let published = std::collections::BTreeSet::from([
            entry("siglake", "events"),
            entry("siglake", "logs-archive"),
            entry("tenant_acme", "events"),
        ]);
        let seen = std::collections::BTreeSet::from([
            entry("siglake", "events"),
            entry("tenant_acme", "events"),
        ]);
        assert_eq!(
            super::vanished_inline_coverage(&published, &seen),
            vec![entry("siglake", "logs-archive")]
        );
        // A table seen for the first time is not something to clear, and a
        // pass that reaches everything clears nothing.
        assert!(super::vanished_inline_coverage(&seen, &published).is_empty());
        assert!(super::vanished_inline_coverage(&published, &published).is_empty());
    }

    /// The liveness arm of `SiglakeInlineCoverageUnproven`. Without the series
    /// at 0, the FIRST census on a fresh compactor raises the gauge while the
    /// `increase()` beside it still reads nothing, and the alert never fires.
    #[test]
    fn the_census_pass_counter_is_preregistered() {
        assert!(
            siglake_core::metrics::COMPACTOR_ALERTED_COUNTERS
                .iter()
                .any(|c| c.name == "siglake_inline_coverage_census_total"
                    && c.series == siglake_core::metrics::UNLABELLED),
            "the compactor catalog must create the census counter at 0"
        );
    }
}

#[cfg(test)]
mod mirror_sync_cursor_tests {
    use std::collections::BTreeSet;

    use bytes::Bytes;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use opendal::services::Memory;

    use super::{
        list_mirror_sync_page, mirror_sync_cursor_id, sync_mirror_to_catalog, Operator,
        SqlSegmentClaim,
    };

    fn memory_op() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    async fn claim(tmp: &std::path::Path, owner: &str) -> SqlSegmentClaim {
        let uri = format!("sqlite://{}?mode=rwc", tmp.join("claim.db").display());
        SqlSegmentClaim::connect(&uri, owner).await.unwrap()
    }

    async fn put(store: &Operator, key: &str) {
        store.write(key, Bytes::from_static(b"x")).await.unwrap();
    }

    #[tokio::test]
    async fn crash_replays_the_uncommitted_page_without_a_gap() {
        let tmp = tempfile::tempdir().unwrap();
        let store = memory_op();
        let prefix = "wal-mirror";
        for key in ["a", "b", "c"] {
            put(&store, &format!("{prefix}/{key}.arrow")).await;
        }
        let owner_a = claim(tmp.path(), "owner-a").await;
        let owner_b = claim(tmp.path(), "owner-b").await;
        let cursor_id = mirror_sync_cursor_id(&store, prefix);

        // Model death after one registration but before page progress is
        // committed. The shared cursor must still point at the page start.
        let page = list_mirror_sync_page(&store, prefix, None, 2)
            .await
            .unwrap();
        let first = &page.objects[0];
        owner_a
            .register(
                &first.id,
                &first.tenant,
                &first.index_id,
                &first.url,
                first.bytes,
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            owner_b
                .mirror_sync_cursor(&cursor_id)
                .await
                .unwrap()
                .last_key,
            None
        );

        sync_mirror_to_catalog(&store, prefix, &owner_b, 2)
            .await
            .unwrap();
        let resumed = owner_b.mirror_sync_cursor(&cursor_id).await.unwrap();
        assert_eq!(resumed.last_key.as_deref(), Some("wal-mirror/b.arrow"));
        let claimed: BTreeSet<_> = owner_b
            .try_claim(10)
            .await
            .unwrap()
            .into_iter()
            .map(|segment| segment.id)
            .collect();
        assert!(claimed.contains("a"));
        assert!(claimed.contains("b"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wrap_repairs_inserted_and_lost_keys_behind_the_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = memory_op();
        let prefix = "wal-mirror";
        for key in ["b", "c", "d"] {
            put(&store, &format!("{prefix}/{key}.arrow")).await;
        }
        let owner_a = claim(tmp.path(), "owner-a").await;
        let owner_b = claim(tmp.path(), "owner-b").await;
        let cursor_id = mirror_sync_cursor_id(&store, prefix);

        let first_page_recorder = DebuggingRecorder::new();
        let first_page_snapshotter = first_page_recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&first_page_recorder);
            sync_mirror_to_catalog(&store, prefix, &owner_a, 2)
                .await
                .unwrap();
        }
        let first_page = owner_b.mirror_sync_cursor(&cursor_id).await.unwrap();
        assert_eq!(first_page.last_key.as_deref(), Some("wal-mirror/c.arrow"));
        assert!(first_page.rotation_started_at_ms.is_some());
        assert_eq!(first_page.rotation_objects_examined, 2);
        assert_eq!(first_page.last_completed_at_ms, None);
        let first_page_snapshot = first_page_snapshotter.snapshot().into_vec();
        let first_page_progress = first_page_snapshot
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "siglake_compactor_mirror_sync_rotation_objects_examined"
            })
            .map(|(_, _, _, value)| value)
            .expect("missing in-progress rotation object gauge");
        match first_page_progress {
            DebugValue::Gauge(examined) => assert_eq!(examined.into_inner(), 2.0),
            other => panic!("rotation progress has wrong metric type: {other:?}"),
        }

        // Both changes sort behind the durable cursor: one new object, and one
        // object whose already-registered catalog row is lost.
        put(&store, "wal-mirror/a.arrow").await;
        let initially_claimed: BTreeSet<_> = owner_a
            .try_claim(10)
            .await
            .unwrap()
            .into_iter()
            .map(|segment| segment.id)
            .collect();
        assert!(initially_claimed.contains("b"));
        owner_a.mark_committed("b").await.unwrap();
        assert_eq!(
            owner_a
                .purge_committed_ids(&["b".to_string()])
                .await
                .unwrap(),
            1
        );

        // A different owner resumes after c, reaches d, and wraps. Neither
        // behind-cursor key is visible until the next rotation begins.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            sync_mirror_to_catalog(&store, prefix, &owner_b, 2)
                .await
                .unwrap();
        }
        let wrapped = owner_a.mirror_sync_cursor(&cursor_id).await.unwrap();
        assert_eq!(wrapped.last_key, None);
        assert_eq!(wrapped.rotation, 1);
        assert_eq!(wrapped.rotation_started_at_ms, None);
        assert_eq!(wrapped.rotation_objects_examined, 3);
        assert!(wrapped.last_completed_at_ms.is_some());

        let snapshot = snapshotter.snapshot().into_vec();
        let value = |name: &str| {
            snapshot
                .iter()
                .find(|(key, _, _, _)| key.key().name() == name)
                .map(|(_, _, _, value)| value)
                .unwrap_or_else(|| panic!("missing metric {name}"))
        };
        match value("siglake_compactor_mirror_sync_rotation_objects") {
            DebugValue::Histogram(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].into_inner(), 3.0);
            }
            other => panic!("rotation objects has wrong metric type: {other:?}"),
        }
        match value("siglake_compactor_mirror_sync_rotation_duration_seconds") {
            DebugValue::Histogram(samples) => assert_eq!(samples.len(), 1),
            other => panic!("rotation duration has wrong metric type: {other:?}"),
        }
        match value("siglake_compactor_mirror_sync_rotations_completed") {
            DebugValue::Gauge(completed) => assert_eq!(completed.into_inner(), 1.0),
            other => panic!("completed rotations has wrong metric type: {other:?}"),
        }
        match value("siglake_compactor_mirror_sync_rotation_objects_examined") {
            DebugValue::Gauge(examined) => assert_eq!(examined.into_inner(), 3.0),
            other => panic!("rotation progress has wrong metric type: {other:?}"),
        }
        match value("siglake_compactor_mirror_sync_last_completed_timestamp_seconds") {
            DebugValue::Gauge(timestamp) => assert!(timestamp.into_inner() > 0.0),
            other => panic!("completion timestamp has wrong metric type: {other:?}"),
        }

        let next_rotation_recorder = DebuggingRecorder::new();
        let next_rotation_snapshotter = next_rotation_recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&next_rotation_recorder);
            sync_mirror_to_catalog(&store, prefix, &owner_b, 2)
                .await
                .unwrap();
        }
        let next_rotation_snapshot = next_rotation_snapshotter.snapshot().into_vec();
        let next_rotation_progress = next_rotation_snapshot
            .iter()
            .find(|(key, _, _, _)| {
                key.key().name() == "siglake_compactor_mirror_sync_rotation_objects_examined"
            })
            .map(|(_, _, _, value)| value)
            .expect("missing next-rotation object gauge");
        match next_rotation_progress {
            DebugValue::Gauge(examined) => assert_eq!(examined.into_inner(), 2.0),
            other => panic!("next-rotation progress has wrong metric type: {other:?}"),
        }
        let claimed: BTreeSet<_> = owner_b
            .try_claim(10)
            .await
            .unwrap()
            .into_iter()
            .map(|segment| segment.id)
            .collect();
        assert!(claimed.contains("a"), "new behind-cursor key was missed");
        assert!(
            claimed.contains("b"),
            "lost behind-cursor row was not repaired"
        );
        let next_rotation = owner_a.mirror_sync_cursor(&cursor_id).await.unwrap();
        assert_eq!(
            next_rotation.last_key.as_deref(),
            Some("wal-mirror/b.arrow")
        );
        assert_eq!(next_rotation.rotation, 1);
    }
}

#[cfg(test)]
mod claim_reclaim_tests {
    use super::{
        claim_reclaim_due_with, claim_reclaim_interval_from, claim_reclaim_max_age_from,
        ClaimReclaimMaxAgeFallback, DEFAULT_CLAIM_RECLAIM_INTERVAL_SECS,
        DEFAULT_CLAIM_RECLAIM_MAX_AGE_SECS,
    };
    use std::sync::Mutex;
    use std::time::Duration;

    const DEFAULT_MAX_AGE: Duration = Duration::from_secs(DEFAULT_CLAIM_RECLAIM_MAX_AGE_SECS);
    const DEFAULT_INTERVAL: Duration = Duration::from_secs(DEFAULT_CLAIM_RECLAIM_INTERVAL_SECS);

    #[test]
    fn claim_reclaim_max_age_defaults_when_unset() {
        assert_eq!(claim_reclaim_max_age_from(None), (DEFAULT_MAX_AGE, None));
    }

    #[test]
    fn claim_reclaim_max_age_honours_positive_seconds() {
        // Below the default is allowed: an operator who has measured short
        // appends may tighten it. Only 0 is refused.
        for (raw, secs) in [("1", 1), ("30", 30), ("900", 900), ("3600", 3600)] {
            assert_eq!(
                claim_reclaim_max_age_from(Some(raw)),
                (Duration::from_secs(secs), None),
                "{raw:?} must be honoured as-is"
            );
        }
    }

    /// `0` would make every in-flight claim "abandoned" the moment it is taken,
    /// so each sweep would yank live work from every healthy drain. It floors
    /// to the default and is reported so the env wrapper can warn once.
    #[test]
    fn claim_reclaim_max_age_zero_floors_to_the_default() {
        assert_eq!(
            claim_reclaim_max_age_from(Some("0")),
            (DEFAULT_MAX_AGE, Some(ClaimReclaimMaxAgeFallback::Zero))
        );
    }

    #[test]
    fn claim_reclaim_max_age_garbage_falls_back_to_the_default() {
        for raw in ["", "abc", "-1", "1.5", "30s"] {
            assert_eq!(
                claim_reclaim_max_age_from(Some(raw)),
                (
                    DEFAULT_MAX_AGE,
                    Some(ClaimReclaimMaxAgeFallback::Unparseable)
                ),
                "{raw:?} must fall back to the default and say why"
            );
        }
    }

    #[test]
    fn claim_reclaim_interval_defaults_when_unset_or_garbage() {
        assert_eq!(claim_reclaim_interval_from(None), DEFAULT_INTERVAL);
        for raw in ["", "abc", "-1", "1.5", "60s"] {
            assert_eq!(
                claim_reclaim_interval_from(Some(raw)),
                DEFAULT_INTERVAL,
                "{raw:?} must fall back to the default"
            );
        }
    }

    /// `0` is NOT off: it is every cycle, the same rule as the mirror sync
    /// interval, and there is no disable word because dead-worker recovery has
    /// no legitimate "off".
    #[test]
    fn claim_reclaim_interval_honours_seconds_and_zero_is_every_cycle() {
        assert_eq!(
            claim_reclaim_interval_from(Some("5")),
            Duration::from_secs(5)
        );
        assert_eq!(claim_reclaim_interval_from(Some("0")), Duration::ZERO);
    }

    #[test]
    fn claim_reclaim_due_with_zero_interval_fires_every_cycle() {
        let last = Mutex::new(None);
        for cycle in 0..3 {
            assert!(
                claim_reclaim_due_with(&last, Duration::ZERO),
                "cycle {cycle} must be due under a zero interval"
            );
        }
    }

    #[test]
    fn claim_reclaim_due_with_long_interval_fires_once_and_stamps() {
        let last = Mutex::new(None);
        let hour = Duration::from_secs(3600);
        assert!(
            claim_reclaim_due_with(&last, hour),
            "the first check is always due"
        );
        assert!(
            last.lock().unwrap().is_some(),
            "a due check stamps the sweep time"
        );
        assert!(
            !claim_reclaim_due_with(&last, hour),
            "a second check inside the interval is not due"
        );
    }
}

#[cfg(test)]
mod idle_backoff_tests {
    use super::*;

    #[test]
    fn base_is_clamped_to_the_poll_interval() {
        // A backoff longer than the configured cadence would make the drain
        // SLOWER to notice work than before the change.
        assert_eq!(
            idle_backoff_base_from(Some(5000), Duration::from_millis(200)),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn base_defaults_below_a_one_second_interval() {
        let b = idle_backoff_base_from(None, Duration::from_secs(1));
        assert_eq!(b, Duration::from_millis(50));
        assert!(
            b < Duration::from_secs(1),
            "a race-loser must retry sooner than a full interval"
        );
    }

    #[test]
    fn zero_is_floored_so_the_loop_cannot_busy_spin() {
        assert_eq!(
            idle_backoff_base_from(Some(0), Duration::from_secs(1)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn doubling_saturates_at_the_poll_interval() {
        let interval = Duration::from_secs(1);
        let mut b = idle_backoff_base_from(None, interval);
        for _ in 0..12 {
            b = (b * 2).min(interval);
        }
        assert_eq!(
            b, interval,
            "an idle drain must settle at the configured cadence"
        );
    }
}

#[cfg(test)]
mod role_tests {
    use super::*;

    #[test]
    fn drain_role_does_no_maintenance() {
        let r = CompactorRole::Drain;
        assert!(r.drains());
        assert!(
            !r.maintains(),
            "a drain node running whole-warehouse sweeps is the thing this type exists to prevent"
        );
    }

    #[test]
    fn maintenance_role_does_not_claim_wal_work() {
        let r = CompactorRole::Maintenance;
        assert!(
            !r.drains(),
            "a maintenance node must not compete for WAL claims"
        );
        assert!(r.maintains());
    }

    #[test]
    fn default_is_combined_so_single_process_deploys_are_unchanged() {
        let r = CompactorRole::default();
        assert_eq!(r, CompactorRole::Combined);
        assert!(r.drains() && r.maintains());
    }

    #[test]
    fn every_role_is_covered_by_exactly_one_half_or_both() {
        for r in [
            CompactorRole::Drain,
            CompactorRole::Maintenance,
            CompactorRole::Combined,
        ] {
            assert!(
                r.drains() || r.maintains(),
                "{r:?} would run nothing at all -- a silently idle process"
            );
        }
    }

    #[test]
    fn unknown_role_is_an_error_not_a_silent_combined() {
        assert_eq!(CompactorRole::parse("drain"), Ok(CompactorRole::Drain));
        assert_eq!(
            CompactorRole::parse("  MAINTENANCE "),
            Ok(CompactorRole::Maintenance)
        );
        // A typo that silently kept doing maintenance on every drain is exactly
        // the regression this guards.
        assert!(CompactorRole::parse("drian").is_err());
        assert!(CompactorRole::parse("").is_err());
    }
}

#[cfg(test)]
mod maintenance_lease_tests {
    use super::*;

    #[test]
    fn ttl_floors_at_a_minute() {
        // A lease shorter than a sweep lets a second node start the same work
        // mid-flight, which is worse than no election at all.
        assert_eq!(
            maintenance_lease_ttl().max(Duration::from_secs(60)),
            maintenance_lease_ttl()
        );
        assert!(maintenance_lease_ttl() >= Duration::from_secs(60));
    }

    #[tokio::test]
    async fn no_catalog_means_maintenance_still_runs() {
        // Single-process and filesystem deployments have nobody to contend with.
        // Failing CLOSED there would silently stop expiring snapshots forever --
        // a far worse outcome than duplicating a sweep.
        let tmp = tempfile::tempdir().unwrap();
        let ice = std::sync::Arc::new(
            siglake_storage::iceberg::IcebergContext::open(&tmp.path().join("wh"))
                .await
                .unwrap(),
        );
        let c = Compactor::new(tmp.path().join("wal"), ice);
        assert!(c.catalog.is_none());
        assert!(
            c.maintenance_lease("expire").await,
            "maintenance must fail OPEN with no catalog"
        );
    }

    /// Regression for the stale-list race between mirror sync and committed
    /// retention. The sync has already listed `stale.arrow`; retention must not
    /// be able to delete its object and committed row before sync attempts the
    /// idempotent registration for that saved entry.
    #[tokio::test]
    async fn stale_mirror_listing_cannot_recreate_a_purged_row() {
        use bytes::Bytes;
        use futures::StreamExt;
        use opendal::services::Memory;

        let tmp = tempfile::tempdir().unwrap();
        let uri = format!(
            "sqlite://{}?mode=rwc",
            tmp.path().join("claim.db").display()
        );
        let sync_claim = SqlSegmentClaim::connect(&uri, "sync-worker").await.unwrap();
        let retention_claim = SqlSegmentClaim::connect(&uri, "retention-worker")
            .await
            .unwrap();
        let store = Operator::new(Memory::default()).unwrap().finish();
        let key = "wal-mirror/stale.arrow";
        store
            .write(key, Bytes::from_static(b"listed before retention"))
            .await
            .unwrap();

        sync_claim
            .register("stale", "default", "", key, 23, 1)
            .await
            .unwrap();
        let claimed = sync_claim.try_claim(1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        sync_claim.mark_committed("stale").await.unwrap();

        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let sync = Compactor::new(tmp.path().join("wal-sync"), ice.clone()).with_catalog_claim(
            CatalogClaimConfig {
                claim: sync_claim.clone(),
                store: store.clone(),
                prefix: "wal-mirror".to_string(),
                batch_size: 1,
                last_mirror_sync: Default::default(),
                last_reclaim: Default::default(),
            },
        );
        let retention = Compactor::new(tmp.path().join("wal-retention"), ice).with_catalog_claim(
            CatalogClaimConfig {
                claim: retention_claim.clone(),
                store: store.clone(),
                prefix: "wal-mirror".to_string(),
                batch_size: 1,
                last_mirror_sync: Default::default(),
                last_reclaim: Default::default(),
            },
        );

        assert!(sync.acquire_mirror_reconciliation_lease().await);
        let mut listed = store
            .lister_with("wal-mirror/")
            .recursive(true)
            .await
            .unwrap();
        let stale_entry = listed.next().await.unwrap().unwrap();
        assert_eq!(stale_entry.path(), key);

        // This is the dangerous interleaving point: without the shared lease,
        // retention deletes object + row here and the saved entry below inserts
        // a fresh `sealed` row for an object that no longer exists.
        assert!(
            !retention.acquire_mirror_reconciliation_lease().await,
            "retention must be excluded after mirror sync has listed the object"
        );
        assert!(
            !sync_claim
                .register("stale", "default", "", stale_entry.path(), 23, 0)
                .await
                .unwrap(),
            "registration before retention must see the existing committed row"
        );
        sync.release_mirror_reconciliation_lease().await;

        assert!(retention.acquire_mirror_reconciliation_lease().await);
        store.delete(key).await.unwrap();
        assert_eq!(
            retention_claim
                .purge_committed_ids(&["stale".to_string()])
                .await
                .unwrap(),
            1
        );
        retention.release_mirror_reconciliation_lease().await;

        assert!(
            sync_claim.try_claim(1).await.unwrap().is_empty(),
            "the serialized list/register/delete/purge order must leave no sealed poison row"
        );
    }
}

/// The signal for delete tasks left non-terminal (#2114's design, #2245).
///
/// Every test here is deterministic and none sleeps on wall time: the
/// classifier and the planner take `now` as an argument, and the one
/// end-to-end test ages its claim object by setting the file's mtime.
///
/// What the signal is allowed to say is as load-bearing as when it fires. The
/// claim's age bounds execution duration from ABOVE and is not a running
/// duration; a stalled report proves neither that the executor died nor that
/// the rewrite failed to commit; and an observation that could not see the
/// whole namespace is `unknown`, never a healthy zero.
#[cfg(test)]
mod delete_task_stall_tests {
    use std::sync::Arc;

    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-09-08T12:00:00Z".parse().unwrap()
    }

    /// A non-terminal task whose claim was written `claim_age` ago.
    fn claimed(state: DeleteTaskState, claim_age: ChronoDuration) -> NonTerminalDeleteTask {
        NonTerminalDeleteTask {
            task_id: uuid::Uuid::now_v7(),
            index_id: "logs".to_string(),
            state,
            created_at: now() - ChronoDuration::seconds(8_830),
            claim: ObservedDeleteTaskClaim::Present {
                last_modified: Some(now() - claim_age),
            },
        }
    }

    fn unclaimed(state: DeleteTaskState) -> NonTerminalDeleteTask {
        NonTerminalDeleteTask {
            claim: ObservedDeleteTaskClaim::Absent,
            ..claimed(state, ChronoDuration::zero())
        }
    }

    fn complete(tasks: Vec<NonTerminalDeleteTask>) -> NonTerminalDeleteTaskObservation {
        NonTerminalDeleteTaskObservation {
            tasks,
            complete: true,
            uninspected: 0,
        }
    }

    fn plan(
        observations: &DeleteTaskObservations,
        logged: &mut HashMap<uuid::Uuid, DateTime<Utc>>,
    ) -> DeleteTaskObservationReport {
        plan_delete_task_observation(
            now(),
            delete_task_stall_bound_from(None),
            observations,
            logged,
        )
    }

    fn one_namespace(tasks: Vec<NonTerminalDeleteTask>) -> DeleteTaskObservationReport {
        plan(
            &[("acme".to_string(), Ok(complete(tasks)))],
            &mut HashMap::new(),
        )
    }

    fn gauge_of(report: &DeleteTaskObservationReport, state: &str) -> Option<u64> {
        report.gauge.as_ref().map(|g| g[state])
    }

    // --- the bound -------------------------------------------------------

    /// 2x the resolved drain-watchdog ceiling, from the ceiling's own resolver:
    /// no new knob, one parse site, and the pure function is what the tests
    /// drive (never `set_var`).
    #[test]
    fn the_bound_is_twice_the_resolved_watchdog_ceiling() {
        assert_eq!(
            delete_task_stall_bound_from(None),
            Some(Duration::from_secs(1_200)),
            "the 600s default ceiling gives a 20-minute bound"
        );
        assert_eq!(
            delete_task_stall_bound_from(None),
            watchdog_ceiling_from(None, DRAIN_WATCHDOG_DEFAULT_SECS).map(|c| c * 2),
            "the bound tracks the drain ceiling's own default, not a second copy of it"
        );
        assert_eq!(
            delete_task_stall_bound_from(Some("900")),
            Some(Duration::from_secs(1_800))
        );
        // The ceiling's own floor applies first: a 5s ceiling is clamped to 60.
        assert_eq!(
            delete_task_stall_bound_from(Some("5")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            delete_task_stall_bound_from(Some("nonsense")),
            Some(Duration::from_secs(1_200)),
            "an unparseable knob falls back to the default, as the ceiling does"
        );
        assert_eq!(
            delete_task_stall_bound_from(Some("0")),
            None,
            "a disabled watchdog has no bound at all"
        );
    }

    // --- the classifier --------------------------------------------------

    #[test]
    fn a_claim_older_than_the_bound_is_stalled_and_a_fresh_one_is_not() {
        let bound = delete_task_stall_bound_from(None);
        for state in [DeleteTaskState::Running, DeleteTaskState::Pending] {
            assert_eq!(
                classify_nonterminal_delete_task(
                    now(),
                    &claimed(state, ChronoDuration::seconds(4_210)),
                    bound
                ),
                DeleteTaskStall::Stalled
            );
            assert_eq!(
                classify_nonterminal_delete_task(
                    now(),
                    &claimed(state, ChronoDuration::seconds(1_200)),
                    bound
                ),
                DeleteTaskStall::Healthy,
                "the bound itself is not past the bound"
            );
            assert_eq!(
                classify_nonterminal_delete_task(
                    now(),
                    &claimed(state, ChronoDuration::seconds(30)),
                    bound
                ),
                DeleteTaskStall::Healthy,
                "a fresh claim is an execution in progress, not a stall"
            );
        }
    }

    /// Three ways there is no age, and none of them is `Healthy`: an unknown is
    /// not an all-clear. The negative case is real — the claim's
    /// `last_modified` is the STORE's clock and the observation is this
    /// process's, so skew can put the claim in the future.
    #[test]
    fn a_missing_or_backwards_age_is_unknown_never_healthy() {
        let bound = delete_task_stall_bound_from(None);
        assert_eq!(
            classify_nonterminal_delete_task(now(), &unclaimed(DeleteTaskState::Running), bound),
            DeleteTaskStall::Unknown
        );
        let no_mtime = NonTerminalDeleteTask {
            claim: ObservedDeleteTaskClaim::Present {
                last_modified: None,
            },
            ..claimed(DeleteTaskState::Running, ChronoDuration::zero())
        };
        assert_eq!(
            classify_nonterminal_delete_task(now(), &no_mtime, bound),
            DeleteTaskStall::Unknown,
            "a store that reports no last_modified gives an unknown age, not zero"
        );
        assert_eq!(
            classify_nonterminal_delete_task(
                now(),
                &claimed(DeleteTaskState::Running, ChronoDuration::seconds(-60)),
                bound
            ),
            DeleteTaskStall::Unknown,
            "a claim from the future is clock skew, reported as unknown rather than 0"
        );
    }

    /// A pod whose operator deliberately removed the watchdog ceiling may
    /// legitimately run a stage for hours. Nothing is ever stalled there, and
    /// the counter never moves — the false page this design refuses to ship.
    #[test]
    fn a_disabled_watchdog_never_stalls_anything() {
        let ancient = claimed(DeleteTaskState::Running, ChronoDuration::days(30));
        assert_eq!(
            classify_nonterminal_delete_task(
                now(),
                &ancient,
                delete_task_stall_bound_from(Some("0"))
            ),
            DeleteTaskStall::Unknown
        );
        let report = plan_delete_task_observation(
            now(),
            delete_task_stall_bound_from(Some("0")),
            &[("acme".to_string(), Ok(complete(vec![ancient])))],
            &mut HashMap::new(),
        );
        assert!(report.lines.is_empty(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert_eq!(
            gauge_of(&report, DELETE_TASK_STATE_RUNNING),
            Some(1),
            "the counts still publish with no bound; only the stall verdict is withheld"
        );
    }

    // --- the series ------------------------------------------------------

    #[test]
    fn only_running_and_claimed_pending_tasks_are_covered() {
        assert_eq!(
            delete_task_series(&claimed(DeleteTaskState::Running, ChronoDuration::zero())),
            Some(DELETE_TASK_STATE_RUNNING)
        );
        assert_eq!(
            delete_task_series(&unclaimed(DeleteTaskState::Running)),
            Some(DELETE_TASK_STATE_RUNNING),
            "a running record is running whether or not its claim is observable"
        );
        assert_eq!(
            delete_task_series(&claimed(DeleteTaskState::Pending, ChronoDuration::zero())),
            Some(DELETE_TASK_STATE_PENDING_CLAIMED)
        );
        assert_eq!(
            delete_task_series(&unclaimed(DeleteTaskState::Pending)),
            None,
            "an unclaimed pending task is indistinguishable from one waiting for the next \
             sweep, so it is not covered at all"
        );
    }

    #[test]
    fn the_preregistered_series_are_the_only_two_the_code_can_emit() {
        let registered = siglake_core::metrics::COMPACTOR_ALERTED_COUNTERS
            .iter()
            .find(|c| c.name == "siglake_compactor_delete_tasks_stalled_total")
            .expect("compactor catalog lists the stalled delete-task counter");
        for state in [DELETE_TASK_STATE_RUNNING, DELETE_TASK_STATE_PENDING_CLAIMED] {
            let series: &[(&str, &str)] = &[("state", state)];
            assert!(
                registered.series.contains(&series),
                "{state} is emitted at the call site but not pre-registered: {registered:?}"
            );
        }
        assert_eq!(
            registered.series.len(),
            2,
            "the catalog pre-registers a series the emitter cannot produce: {registered:?}"
        );
    }

    // --- the report ------------------------------------------------------

    /// The crash and the watchdog cancellation: a `running` record whose claim
    /// predates the bound is reported once, counted once, and shows in the
    /// gauge.
    #[test]
    fn a_stranded_running_task_is_reported_counted_and_gauged() {
        let task = claimed(DeleteTaskState::Running, ChronoDuration::seconds(4_210));
        let report = one_namespace(vec![task.clone()]);
        assert_eq!(report.lines.len(), 1, "{report:?}");
        let line = &report.lines[0];
        assert_eq!(line.tenant, "acme");
        assert_eq!(line.index_id, "logs");
        assert_eq!(line.task_id, task.task_id);
        assert_eq!(line.state, DELETE_TASK_STATE_RUNNING);
        assert_eq!(line.claim_age_seconds, 4_210);
        assert_eq!(line.submitted_age_seconds, 8_830);
        assert_eq!(line.bound_seconds, 1_200);
        assert_eq!(report.increments[DELETE_TASK_STATE_RUNNING], 1);
        assert_eq!(gauge_of(&report, DELETE_TASK_STATE_RUNNING), Some(1));
        assert_eq!(
            gauge_of(&report, DELETE_TASK_STATE_PENDING_CLAIMED),
            Some(0)
        );
    }

    /// The claimed-`pending` stranding: the process died between the claim and
    /// the `Running` write. Same observation, same bound, its own series.
    #[test]
    fn a_stranded_claimed_pending_task_is_reported_under_its_own_series() {
        let report = one_namespace(vec![claimed(
            DeleteTaskState::Pending,
            ChronoDuration::seconds(4_210),
        )]);
        assert_eq!(report.lines.len(), 1, "{report:?}");
        assert_eq!(report.lines[0].state, DELETE_TASK_STATE_PENDING_CLAIMED);
        assert_eq!(report.increments[DELETE_TASK_STATE_PENDING_CLAIMED], 1);
        assert_eq!(
            gauge_of(&report, DELETE_TASK_STATE_PENDING_CLAIMED),
            Some(1)
        );
        assert_eq!(gauge_of(&report, DELETE_TASK_STATE_RUNNING), Some(0));
    }

    /// Healthy concurrent execution — the shape the two-executor race produces
    /// with both executors alive. Nothing is reported, nothing is counted, and
    /// the gauge says one task is non-terminal, because it is.
    #[test]
    fn a_healthy_execution_in_progress_is_gauged_but_not_reported() {
        let report = one_namespace(vec![claimed(
            DeleteTaskState::Running,
            ChronoDuration::seconds(30),
        )]);
        assert!(report.lines.is_empty(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert_eq!(gauge_of(&report, DELETE_TASK_STATE_RUNNING), Some(1));
    }

    /// The claimed-`pending` window is where the two-executor race actually
    /// lives: `two_executors_racing_one_pending_task_produce_exactly_one_execution`
    /// leaves the loser's view of a `pending` record under the winner's fresh
    /// claim, microseconds before the `Running` write. That shape is contention
    /// and must never be reported — only the bound separates it from the
    /// stranding above, which is why this signal has no threshold of its own.
    #[test]
    fn a_fresh_claim_on_a_pending_task_is_contention_not_a_stall() {
        let report = one_namespace(vec![claimed(
            DeleteTaskState::Pending,
            ChronoDuration::seconds(2),
        )]);
        assert!(report.lines.is_empty(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert_eq!(
            gauge_of(&report, DELETE_TASK_STATE_PENDING_CLAIMED),
            Some(1),
            "the task IS non-terminal and claimed, so the dashboard shows it"
        );
        assert_eq!(report.summaries[0].stalled, 0);
        assert_eq!(report.summaries[0].unknown, 0);
    }

    /// A claimed `pending` task whose age is unavailable — a store that reports
    /// no `last_modified` — belongs to the series and is `unknown`, not a
    /// healthy zero and not a stall: neither the counter nor the WARN may move
    /// on an age nothing established.
    #[test]
    fn a_claimed_pending_task_with_no_age_is_unknown_not_stalled() {
        let ageless = NonTerminalDeleteTask {
            claim: ObservedDeleteTaskClaim::Present {
                last_modified: None,
            },
            ..claimed(DeleteTaskState::Pending, ChronoDuration::zero())
        };
        let report = one_namespace(vec![ageless]);
        assert!(report.lines.is_empty(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert_eq!(
            gauge_of(&report, DELETE_TASK_STATE_PENDING_CLAIMED),
            Some(1)
        );
        assert_eq!(report.summaries[0].unknown, 1);
        assert_eq!(report.summaries[0].stalled, 0);
    }

    /// The task reached `done`: it is terminal, so it leaves the observation
    /// (storage filters it out) and the gauge returns to 0 on the next sweep.
    #[test]
    fn a_task_that_reaches_a_terminal_state_returns_the_gauge_to_zero() {
        let mut logged = HashMap::new();
        let task = claimed(DeleteTaskState::Running, ChronoDuration::seconds(4_210));
        let first = plan(
            &[("acme".to_string(), Ok(complete(vec![task.clone()])))],
            &mut logged,
        );
        assert_eq!(gauge_of(&first, DELETE_TASK_STATE_RUNNING), Some(1));

        let second = plan(&[("acme".to_string(), Ok(complete(vec![])))], &mut logged);
        assert!(second.lines.is_empty(), "{second:?}");
        assert!(second.increments.is_empty(), "{second:?}");
        assert_eq!(gauge_of(&second, DELETE_TASK_STATE_RUNNING), Some(0));
        assert!(
            logged.is_empty(),
            "a complete pass must drop the rate-limit entry of a task that is gone"
        );
    }

    /// An incomplete observation publishes NOTHING — no gauge, no counter — and
    /// says the coverage is unknown. A lowered gauge would be
    /// indistinguishable from recovery.
    #[test]
    fn an_incomplete_observation_publishes_no_gauge_and_no_counter() {
        let truncated = NonTerminalDeleteTaskObservation {
            tasks: vec![claimed(
                DeleteTaskState::Running,
                ChronoDuration::seconds(4_210),
            )],
            complete: false,
            uninspected: 7,
        };
        let report = plan(&[("acme".to_string(), Ok(truncated))], &mut HashMap::new());
        assert!(report.gauge.is_none(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert!(report.lines.is_empty(), "{report:?}");
        assert_eq!(report.unknown_coverage.len(), 1, "{report:?}");
        assert_eq!(report.unknown_coverage[0].tenant, "acme");
        assert_eq!(report.unknown_coverage[0].uninspected, 7);
    }

    /// A listing or `stat` failure is an error, not an empty namespace.
    #[test]
    fn a_failed_observation_is_unknown_coverage_not_an_empty_namespace() {
        let report = plan(
            &[(
                "acme".to_string(),
                Err("list _siglake/config: denied".into()),
            )],
            &mut HashMap::new(),
        );
        assert!(report.gauge.is_none(), "{report:?}");
        assert!(report.increments.is_empty(), "{report:?}");
        assert_eq!(report.unknown_coverage.len(), 1, "{report:?}");
        assert!(
            report.unknown_coverage[0].reason.contains("denied"),
            "{report:?}"
        );
    }

    /// The gauge carries no namespace label, so it is one process-wide number.
    /// Two tenants must SUM into it, in either order — setting it per namespace
    /// would let a healthy tenant overwrite the other's count with zero.
    #[test]
    fn two_namespaces_aggregate_into_the_process_wide_gauge_in_either_order() {
        let stalled = vec![claimed(
            DeleteTaskState::Running,
            ChronoDuration::seconds(4_210),
        )];
        let healthy = vec![claimed(
            DeleteTaskState::Running,
            ChronoDuration::seconds(30),
        )];
        let forwards = plan(
            &[
                ("acme".to_string(), Ok(complete(stalled.clone()))),
                ("globex".to_string(), Ok(complete(healthy.clone()))),
            ],
            &mut HashMap::new(),
        );
        let backwards = plan(
            &[
                ("globex".to_string(), Ok(complete(healthy))),
                ("acme".to_string(), Ok(complete(stalled))),
            ],
            &mut HashMap::new(),
        );
        for report in [&forwards, &backwards] {
            assert_eq!(
                gauge_of(report, DELETE_TASK_STATE_RUNNING),
                Some(2),
                "the healthy tenant must not overwrite the stranded tenant's count: {report:?}"
            );
            assert_eq!(
                report.increments[DELETE_TASK_STATE_RUNNING], 1,
                "{report:?}"
            );
            assert_eq!(report.lines.len(), 1, "{report:?}");
            assert_eq!(report.lines[0].tenant, "acme");
        }
    }

    /// One namespace short of complete withholds the whole gauge — there is no
    /// honest total to publish — while the namespaces that WERE covered keep
    /// their counter increments and their task lines. Coverage is per
    /// namespace for the counter and the log, process-wide for the gauge.
    #[test]
    fn an_incomplete_later_namespace_withholds_the_gauge_but_not_the_counter() {
        let report = plan(
            &[
                (
                    "acme".to_string(),
                    Ok(complete(vec![claimed(
                        DeleteTaskState::Running,
                        ChronoDuration::seconds(4_210),
                    )])),
                ),
                ("globex".to_string(), Err("stat timed out".into())),
            ],
            &mut HashMap::new(),
        );
        assert!(
            report.gauge.is_none(),
            "a partial pass must retain the previous gauge: {report:?}"
        );
        assert_eq!(
            report.increments[DELETE_TASK_STATE_RUNNING], 1,
            "{report:?}"
        );
        assert_eq!(report.lines.len(), 1, "{report:?}");
        assert_eq!(report.unknown_coverage.len(), 1, "{report:?}");
    }

    /// The rate limit is on the LOG, not on the count: at the default 1s poll
    /// interval a stalled task would otherwise print 1,200 lines per bound
    /// interval. The counter still moves on every complete observation, since
    /// an operator watching `increase()` must not see the stall stop.
    #[test]
    fn two_observations_inside_one_bound_interval_log_once_and_count_twice() {
        let mut logged = HashMap::new();
        let task = claimed(DeleteTaskState::Running, ChronoDuration::seconds(4_210));
        let observation = [("acme".to_string(), Ok(complete(vec![task.clone()])))];

        let bound = delete_task_stall_bound_from(None);
        let first = plan_delete_task_observation(now(), bound, &observation, &mut logged);
        let second = plan_delete_task_observation(
            now() + ChronoDuration::seconds(1),
            bound,
            &observation,
            &mut logged,
        );
        assert_eq!(first.lines.len(), 1, "{first:?}");
        assert!(second.lines.is_empty(), "{second:?}");
        assert_eq!(second.summaries[0].rate_limited, 1, "{second:?}");
        assert_eq!(first.increments[DELETE_TASK_STATE_RUNNING], 1);
        assert_eq!(second.increments[DELETE_TASK_STATE_RUNNING], 1);

        // Past the interval it is reported again, so a stall that persists is
        // not silent forever.
        let third = plan_delete_task_observation(
            now() + ChronoDuration::seconds(1_201),
            bound,
            &observation,
            &mut logged,
        );
        assert_eq!(third.lines.len(), 1, "{third:?}");
        assert_eq!(third.lines[0].task_id, task.task_id);
    }

    #[test]
    fn due_is_true_when_never_done_when_the_interval_has_passed_and_on_a_backwards_clock() {
        let every = Duration::from_secs(1_200);
        assert!(due(None, now(), every));
        assert!(!due(Some(now()), now(), every));
        assert!(!due(
            Some(now() - ChronoDuration::seconds(1_199)),
            now(),
            every
        ));
        assert!(due(
            Some(now() - ChronoDuration::seconds(1_200)),
            now(),
            every
        ));
        assert!(
            due(Some(now() + ChronoDuration::hours(1)), now(), every),
            "a clock stepping backwards must not silence the log until it catches up"
        );
    }

    /// The log is bounded: ten task lines and a summary, however many tasks are
    /// stranded. Every one of them is still counted, and the lines the cap held
    /// back are not marked as logged, so the next sweep prints them.
    #[test]
    fn the_log_is_capped_at_ten_lines_per_namespace_and_still_counts_all_of_them() {
        let mut logged = HashMap::new();
        let tasks: Vec<_> = (0..12)
            .map(|_| claimed(DeleteTaskState::Running, ChronoDuration::seconds(4_210)))
            .collect();
        let report = plan(&[("acme".to_string(), Ok(complete(tasks)))], &mut logged);
        assert_eq!(report.lines.len(), DELETE_TASK_STALL_LINE_CAP, "{report:?}");
        assert_eq!(report.summaries[0].stalled, 12);
        assert_eq!(report.summaries[0].capped, 2);
        assert_eq!(report.increments[DELETE_TASK_STATE_RUNNING], 12);
        assert_eq!(
            logged.len(),
            DELETE_TASK_STALL_LINE_CAP,
            "a deferred line must not be recorded as logged"
        );
    }

    /// The request text never reaches a log line or a metric label: the
    /// observation type has no `predicate_sql` field to print, and the line
    /// type has no claimant. #2128's route is where those live.
    #[test]
    fn no_emitted_field_can_carry_the_predicate_or_the_claimant() {
        let report = one_namespace(vec![claimed(
            DeleteTaskState::Running,
            ChronoDuration::seconds(4_210),
        )]);
        let rendered = format!("{report:?}");
        for forbidden in ["predicate", "claimant", "claimed_at"] {
            assert!(
                !rendered.contains(forbidden),
                "'{forbidden}' must not be reachable from a delete-task stall report: {rendered}"
            );
        }
    }

    // --- the wiring ------------------------------------------------------

    /// `(metric, state) -> value` from ONE snapshot: `Snapshotter::snapshot()`
    /// drains gauges as well as counters, so a second call reads every series
    /// back as 0.
    fn series(snapshotter: &metrics_util::debugging::Snapshotter) -> Vec<(String, String, f64)> {
        let mut out: Vec<(String, String, f64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| {
                key.key()
                    .name()
                    .starts_with("siglake_compactor_delete_tasks_")
            })
            .map(|(key, _, _, value)| {
                let state = key
                    .key()
                    .labels()
                    .find(|label| label.key() == "state")
                    .map(|label| label.value().to_string())
                    .unwrap_or_default();
                let value = match value {
                    DebugValue::Counter(v) => v as f64,
                    DebugValue::Gauge(v) => v.into_inner(),
                    other => panic!("unexpected value {other:?}"),
                };
                (key.key().name().to_string(), state, value)
            })
            .collect();
        out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        out
    }

    /// The default WAL directory is the DEFAULT NAMESPACE. `run_delete_tasks_once`
    /// pushes the default context and then one per WAL tenant dir, and
    /// `ice_for_tenant("default")` maps straight back to the default context —
    /// so a deployment with a `<wal>/default/` subdir has the same namespace
    /// twice. Observing per context instead of per namespace would double
    /// every increment and every WARN for one stranded task.
    #[tokio::test]
    async fn a_default_wal_directory_does_not_double_report_one_namespace() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        // A tenant dir is one with a `sealed/`; this is the layout the
        // backpressure router leaves behind for header-less traffic.
        std::fs::create_dir_all(wal.join("default/sealed")).unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let mut config = IndexConfig::builtin_events();
        config.index_id = "logs".to_string();
        ice.create_index(&config).await.unwrap();

        let mut task = ice
            .create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();
        task.state = DeleteTaskState::Running;
        ice.write_delete_task_record_for_test(&task).await.unwrap();
        // Stand in for the dead executor: its claim object, aged past the
        // bound by its mtime rather than by waiting.
        let claim = warehouse
            .join("_siglake/config/delete_tasks")
            .join(ice.namespace().to_string())
            .join(format!("{}.claim", task.task_id));
        std::fs::create_dir_all(claim.parent().unwrap()).unwrap();
        std::fs::write(&claim, b"{}").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&claim)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(std::time::SystemTime::now() - Duration::from_secs(4_210)),
            )
            .unwrap();

        let compactor = Compactor::new(&wal, ice.clone()).with_delete_tasks(true);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            compactor.run_delete_tasks_once().await.unwrap();
        }
        assert_eq!(
            series(&snapshotter),
            vec![
                (
                    "siglake_compactor_delete_tasks_nonterminal".to_string(),
                    "pending_claimed".to_string(),
                    0.0
                ),
                (
                    "siglake_compactor_delete_tasks_nonterminal".to_string(),
                    "running".to_string(),
                    1.0
                ),
                (
                    "siglake_compactor_delete_tasks_stalled_total".to_string(),
                    "running".to_string(),
                    1.0
                ),
            ],
            "one stranded task in one namespace is one increment and a count of one, \
             however many WAL directories map to that namespace"
        );
        assert_eq!(
            ice.get_delete_task(task.task_id).await.unwrap().unwrap(),
            task,
            "the sweep's observation must not move the record it reports"
        );
    }

    /// #2164's own case, end to end: the executor died between taking the
    /// create-only claim and the `Running` write (`iceberg.rs`
    /// `claim_delete_task` / `write_delete_task_record`), so the record is
    /// `pending` under a claim nothing will ever release. One real sweep must
    /// report it under `state="pending_claimed"` — the same observation, the
    /// same bound, the same metric family as the `running` case — and must
    /// still refuse to execute it: the execution half's own skip stays a skip,
    /// the claim object is untouched, and the record is left exactly as it was.
    ///
    /// The claim body here is `b"{}"`, the shape a partially written claim
    /// leaves behind. An age read from the body would vanish on exactly this
    /// record; the age is the claim OBJECT's `last_modified`, so it does not.
    #[tokio::test]
    async fn a_stranded_claimed_pending_task_is_reported_by_the_sweep_and_still_not_executed() {
        let tmp = tempfile::tempdir().unwrap();
        let wal = tmp.path().join("wal");
        std::fs::create_dir_all(&wal).unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let mut config = IndexConfig::builtin_events();
        config.index_id = "logs".to_string();
        ice.create_index(&config).await.unwrap();

        // Left `pending`: the dead executor never got to the `Running` write.
        let task = ice
            .create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();
        assert_eq!(task.state, DeleteTaskState::Pending);
        let claim = warehouse
            .join("_siglake/config/delete_tasks")
            .join(ice.namespace().to_string())
            .join(format!("{}.claim", task.task_id));
        std::fs::create_dir_all(claim.parent().unwrap()).unwrap();
        std::fs::write(&claim, b"{}").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&claim)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(std::time::SystemTime::now() - Duration::from_secs(4_210)),
            )
            .unwrap();
        let claim_before = std::fs::metadata(&claim).unwrap().modified().unwrap();

        let compactor = Compactor::new(&wal, ice.clone()).with_delete_tasks(true);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        {
            let _guard = metrics::set_default_local_recorder(&recorder);
            compactor.run_delete_tasks_once().await.unwrap();
        }
        assert_eq!(
            series(&snapshotter),
            vec![
                (
                    "siglake_compactor_delete_tasks_nonterminal".to_string(),
                    "pending_claimed".to_string(),
                    1.0
                ),
                (
                    "siglake_compactor_delete_tasks_nonterminal".to_string(),
                    "running".to_string(),
                    0.0
                ),
                (
                    "siglake_compactor_delete_tasks_stalled_total".to_string(),
                    "pending_claimed".to_string(),
                    1.0
                ),
            ],
            "a pending task under a claim older than the bound is stalled under its own state"
        );
        assert_eq!(
            ice.get_delete_task(task.task_id).await.unwrap().unwrap(),
            task,
            "neither the observation nor the skipped execution may move the record: \
             no state reset, no retry, and recovery is still an explicit resubmission"
        );
        assert_eq!(
            std::fs::read(&claim).unwrap(),
            b"{}",
            "the claim is permanent: the sweep must not rewrite or take it over"
        );
        assert_eq!(
            std::fs::metadata(&claim).unwrap().modified().unwrap(),
            claim_before,
            "a create-only claim write that loses must not refresh the claim's age"
        );
    }
}
