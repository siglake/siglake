//! Async batch-job submission.
//!
//! When a query request carries `priority: "batch"` the handler:
//!
//! 1. Allocates a [`JobId`].
//! 2. Persists a "pending" job row.
//! 3. Spawns the query on a **dedicated** [`tokio::runtime::Runtime`]
//!    so a flood of batch jobs doesn't starve the interactive runtime.
//! 4. Returns `202 Accepted` immediately with the job ID + status /
//!    result URLs.
//!
//! Clients poll `GET /api/v1/jobs/<id>` for status and
//! `GET /api/v1/jobs/<id>/result` for the records body.
//! `DELETE /api/v1/jobs/<id>` transitions to `cancelled`.
//! It also aborts the execution future; the future owns both its admission
//! reservation and storage-scan cancellation guard, so the abort — not the
//! status write — is what actually frees resources.
//!
//! The abort handle is process-local by construction, and with a shared store
//! the replica that serves the `DELETE` is usually **not** the replica
//! executing the job. The status write is therefore only half of a
//! cancellation; the executing replica observes it on
//! [`JobStore::propagate_cancellations`], a sweep over the jobs *this* process
//! holds abort handles for, and aborts the ones the table says are cancelled.
//! The sweep runs on `--jobs-cancel-poll-secs`
//! ([`JobRecoveryPolicy::cancel_poll`]), which is the bound on how long a
//! cancelled job can keep burning CPU and holding admission after `202`.
//!
//! Both directions of the terminal race are resolved by the store, not by the
//! executor: every terminal write is conditional on
//! `status IN ('pending','running')`, so whichever of cancellation and
//! completion lands first wins, and the loser is told it lost — the finishers
//! return `false` and the batch path in `sql.rs` gates its `succeeded`
//! metric + audit row on that answer.
//!
//! Two storage backends share one API:
//!
//! - **In-memory** ([`Backend::InMemory`]): default. Pod restart loses
//!   queued + running jobs (they'll surface as 404 to subsequent
//!   pollers, which is fine for short-lived workloads).
//! - **Postgres** ([`Backend::Postgres`]): durable, and **shared by every
//!   query replica** (the chart points them all at the catalog database).
//!   Each row therefore records the *execution owner* that submitted it —
//!   the process incarnation whose batch runtime holds the future. Owners
//!   heartbeat into `siglake_query_job_owners`; a non-terminal row is only
//!   re-classified as `failed` once its owner's lease has expired, i.e.
//!   once there is evidence the executor is gone. A starting replica no
//!   longer fails a live sibling's jobs.
//!
//! Execution ownership is deliberately **not** an authorization key:
//! `JobInfo::tenant` decides who may read a job, `owner` decides who is
//! executing it, and neither is derived from the other.
//!
//! Ownership-scoped recovery has one blind spot, and
//! [`JobStore::reconcile_finished_jobs`] is what covers it: a row this process
//! owns is never condemned by recovery (a stalled heartbeat is not evidence
//! that we stopped executing), so a run whose execution ENDED without a
//! persisted terminal state — the store was down for every attempt the run
//! could afford — leaves a `running` row that no sweep will ever resolve while
//! this replica lives. The executor is the only party with positive evidence
//! that such a run finished, so it keeps a bounded, body-free record of those
//! ids ([`JobStore::park_finished_unpersisted`]) and retries the terminal write
//! on its own cadence, after the run has returned and released its admission
//! reservation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cost::CostReport;
use crate::format::RecordsResponse;
use crate::limits::Priority;

/// A job-store operation failed before it could return an authoritative
/// lifecycle answer.
#[derive(Debug)]
pub struct JobStoreError {
    source: anyhow::Error,
    retryable: bool,
}

impl JobStoreError {
    fn backend(source: anyhow::Error) -> Self {
        let retryable = source.chain().any(|cause| {
            cause
                .downcast_ref::<sqlx::Error>()
                .is_some_and(sqlx_error_is_retryable)
        });
        Self { source, retryable }
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl std::fmt::Display for JobStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.source)
    }
}

impl std::error::Error for JobStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn sqlx_error_is_retryable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(error) => error.code().is_some_and(|code| {
            code.starts_with("08")
                || matches!(
                    code.as_ref(),
                    "40001" | "40P01" | "53300" | "57P01" | "57P02" | "57P03"
                )
        }),
        _ => false,
    }
}

/// Unique identifier for a batch job. UUIDv7 so IDs sort by creation
/// time when listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Timeout,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Timeout
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "timeout" => Some(Self::Timeout),
            _ => None,
        }
    }
}

/// Public snapshot of job state (omits the result body — fetch that
/// separately via `/result`).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct JobInfo {
    /// Tenant that submitted the job, as [`AppState::job_owner`] computed it.
    /// `None` means the default context -- which is also what pre-existing rows
    /// read as, so an upgrade does not orphan them.
    ///
    /// Not serialized: this is an access-control key, not something a client
    /// asked for, and echoing it would leak the tenant naming of a deployment
    /// to any caller.
    #[serde(skip)]
    pub tenant: Option<String>,
    pub job_id: String,
    pub status: JobStatus,
    pub priority: Priority,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub query: String,
    pub cost: Option<CostReport>,
    pub error: Option<String>,
}

/// Ownership-scoped recovery policy for the Postgres backend, plus the one
/// cadence that has nothing to do with recovery but everything to do with the
/// same shared table: how often an executing replica looks for cancellations
/// another replica persisted.
///
/// The two recovery numbers are deliberately generous: a false "the executor is
/// gone" verdict destroys a live job's result, which is the failure that
/// mechanism exists to prevent. [`Self::cancel_poll`] pulls the other way — it
/// is a latency bound on releasing resources a client has already asked us to
/// stop spending, and reading a handful of primary keys is cheap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobRecoveryPolicy {
    /// How stale an owner's heartbeat may get before that owner counts as
    /// dead and its non-terminal jobs are recoverable.
    pub owner_lease: Duration,
    /// How old a row with **no** recorded owner must be before recovery
    /// touches it. Rows like this were written by a build that predates
    /// execution ownership; we cannot tell whether their executor is alive,
    /// so the conservative policy is to leave them alone until they are old
    /// enough that no executor could still be working on them (the batch
    /// wall-clock ceiling is an hour; the default here is a day).
    pub ownerless_grace: Duration,
    /// How often an executing replica re-reads the status of the jobs it is
    /// running, so a cancellation persisted by a *different* replica reaches
    /// the future holding the admission reservation and the storage-scan
    /// cancel guard. This is the bound the `202` from
    /// `DELETE /api/v1/jobs/<id>` promises: after it, the work has stopped.
    ///
    /// Not folded into the heartbeat cadence on purpose. A lease is sized so a
    /// live replica is never mistaken for a dead one (40 s of heartbeat by
    /// default); a cancellation bound is sized so a client's "stop" is not
    /// theatre.
    pub cancel_poll: Duration,
}

impl Default for JobRecoveryPolicy {
    fn default() -> Self {
        Self {
            owner_lease: Duration::from_secs(120),
            ownerless_grace: Duration::from_secs(86_400),
            cancel_poll: Duration::from_secs(2),
        }
    }
}

impl JobRecoveryPolicy {
    /// Heartbeat cadence: a third of the lease, so three consecutive writes
    /// must be lost before a live owner looks gone.
    pub fn heartbeat_interval(&self) -> Duration {
        (self.owner_lease / 3).max(Duration::from_secs(1))
    }

    /// How long a dead owner's registration row is kept. Long enough that
    /// "owner not registered at all" is never how a recently-live replica's
    /// jobs get recovered.
    fn owner_retention(&self) -> Duration {
        (self.owner_lease * 20).max(Duration::from_secs(3_600))
    }
}

/// Why a non-terminal row is left alone by recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    /// Ours: this process is executing it.
    SelfOwned,
    /// Another replica holds it and is still heartbeating.
    OwnerAlive,
    /// Pre-ownership row, still inside [`JobRecoveryPolicy::ownerless_grace`].
    OwnerlessWithinGrace,
}

/// The evidence that let recovery declare a row's executor gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanReason {
    /// The owner is registered but its heartbeat is older than the lease.
    OwnerLeaseExpired,
    /// The owner is not in `siglake_query_job_owners` at all — either it
    /// never registered, or its registration was pruned long after death.
    OwnerUnregistered,
    /// Pre-ownership row, past [`JobRecoveryPolicy::ownerless_grace`].
    OwnerlessBeyondGrace,
}

impl OrphanReason {
    /// Client-visible `error` text. Distinct per reason so an operator can
    /// tell "the pod running your job died" from "this row predates
    /// ownership and aged out".
    fn error_text(&self) -> &'static str {
        match self {
            Self::OwnerLeaseExpired => {
                "batch executor gone: the replica running this job stopped heartbeating"
            }
            Self::OwnerUnregistered => {
                "batch executor gone: the replica running this job is no longer registered"
            }
            Self::OwnerlessBeyondGrace => {
                "batch executor unknown: this job predates execution ownership and exceeded the ownerless grace period"
            }
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::OwnerLeaseExpired => "owner_lease_expired",
            Self::OwnerUnregistered => "owner_unregistered",
            Self::OwnerlessBeyondGrace => "ownerless_beyond_grace",
        }
    }

    fn recovery_sql(&self) -> &'static str {
        match self {
            Self::OwnerLeaseExpired => RECOVER_OWNER_LEASE_EXPIRED_SQL,
            Self::OwnerUnregistered => RECOVER_OWNER_UNREGISTERED_SQL,
            Self::OwnerlessBeyondGrace => RECOVER_OWNERLESS_BEYOND_GRACE_SQL,
        }
    }

    fn guard_duration(&self, policy: &JobRecoveryPolicy) -> Option<Duration> {
        match self {
            Self::OwnerLeaseExpired => Some(policy.owner_lease),
            Self::OwnerUnregistered => None,
            Self::OwnerlessBeyondGrace => Some(policy.ownerless_grace),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryDecision {
    Keep(KeepReason),
    Recover(OrphanReason),
}

/// One non-terminal row's ownership facts, as read from Postgres. The unit
/// of the recovery decision; kept free of SQL types so the decision itself
/// is a pure function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOwnership {
    pub job_id: String,
    /// Recorded execution owner. `None` for rows written before the column
    /// existed.
    pub owner: Option<String>,
    /// Age of the owner's last heartbeat. `None` when the owner has no
    /// registration row (including when `owner` itself is `None`).
    pub owner_heartbeat_age: Option<Duration>,
    /// `NOW() - submitted_at`.
    pub age: Duration,
}

/// The whole recovery rule, as a pure function of one row's ownership facts.
///
/// This is the specification *and* the implementation: [`JobStore`] reads
/// candidate rows, calls this per row, and only then writes. Nothing in the
/// SQL re-states the policy. The reason-specific predicate on the condemning
/// write is only a liveness re-check: it can cancel this decision when the
/// evidence changed after the read, but can never add a row to it.
pub fn recovery_decision(
    row: &JobOwnership,
    self_owner: &str,
    policy: &JobRecoveryPolicy,
) -> RecoveryDecision {
    match row.owner.as_deref() {
        // Never recover our own work, whatever the heartbeat says: the
        // future is in this process's batch runtime, and a stalled
        // heartbeat writer is not evidence that we stopped executing.
        Some(owner) if owner == self_owner => RecoveryDecision::Keep(KeepReason::SelfOwned),
        Some(_) => match row.owner_heartbeat_age {
            None => RecoveryDecision::Recover(OrphanReason::OwnerUnregistered),
            // Strictly greater: a heartbeat exactly at the lease boundary is
            // still a heartbeat.
            Some(age) if age > policy.owner_lease => {
                RecoveryDecision::Recover(OrphanReason::OwnerLeaseExpired)
            }
            Some(_) => RecoveryDecision::Keep(KeepReason::OwnerAlive),
        },
        None if row.age > policy.ownerless_grace => {
            RecoveryDecision::Recover(OrphanReason::OwnerlessBeyondGrace)
        }
        None => RecoveryDecision::Keep(KeepReason::OwnerlessWithinGrace),
    }
}

/// What one recovery pass did. Returned for logging and for tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryOutcome {
    /// Rows failed because their executor was gone.
    pub recovered: usize,
    /// Rows left alone because this process owns them.
    pub kept_self: usize,
    /// Rows left alone because a live replica owns them.
    pub kept_live_owner: usize,
    /// Pre-ownership rows left alone inside the grace period.
    pub kept_ownerless: usize,
}

/// Bounded, owner-local record of runs whose execution ENDED without a
/// persisted terminal state.
///
/// One [`JobStatus`] per stranded id and nothing else: no result body, no
/// error text, no query. Retaining bodies for the duration of a database
/// outage is how a bounded bug becomes an unbounded one, so a lost *success*
/// is reconciled as an explicit failure telling the client to resubmit rather
/// than as a success with no answer behind it (see [`reconciled_terminal`]).
#[derive(Debug, Default)]
struct FinishedUnpersisted {
    /// Job id → the verdict its run computed.
    computed: HashMap<JobId, JobStatus>,
}

/// Cap on [`FinishedUnpersisted`]. Reached only when a store outage outlives
/// this many finished runs on one replica; past it the ids are refused
/// **loudly** (`siglake_query_jobs_unreconciled_dropped_total` and a
/// `tracing::error!`) rather than silently dropped or silently evicting an
/// older id whose client is waiting longer. A refused id degrades exactly to
/// the pre-reconciliation behaviour: the row stays non-terminal until this
/// incarnation exits, and is then recovered by a peer on lease expiry.
const MAX_FINISHED_UNPERSISTED: usize = 1024;

/// How often [`JobStore::reconcile_finished_jobs`] runs. Frequent because the
/// pass is a no-op with nothing parked, and every pass a client waits through
/// is a job that finished but still reads `running`.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// Whether an id that finished without a persisted verdict is being tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkOutcome {
    /// Tracked; [`JobStore::reconcile_finished_jobs`] will keep trying.
    Tracked,
    /// Bookkeeping is full ([`MAX_FINISHED_UNPERSISTED`]) and this id is NOT
    /// tracked. The caller must say so: the row stays non-terminal until this
    /// replica exits.
    Full,
}

/// The terminal state owner-local reconciliation installs for a run whose own
/// terminal write never landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconciledTerminal {
    pub status: JobStatus,
    /// Client-visible `error`. `&'static str` on purpose: the run's own error
    /// text is not retained across an outage, so what a reconciled row says is
    /// drawn from a fixed set.
    pub error: &'static str,
}

/// What reconciliation installs for a given computed verdict — the whole of
/// the rule, as a pure function, because it is where "never rerun the SQL and
/// never claim an answer we do not have" is decided.
///
/// A lost *success* is the interesting one: the body was handed to
/// `finish_succeeded` and discarded with the failed write, and re-executing
/// the query is exactly what a batch job must not do behind a client's back.
/// So the row becomes an explicit `failed` that names the reason and asks for
/// a resubmit. A lost failure or timeout is installed as ITSELF — the status is
/// the whole of the answer there — with text saying the specific error was
/// lost with the write.
pub fn reconciled_terminal(computed: JobStatus) -> ReconciledTerminal {
    match computed {
        JobStatus::Succeeded => ReconciledTerminal {
            status: JobStatus::Failed,
            error: RECONCILED_LOST_RESULT_ERROR,
        },
        JobStatus::Timeout => ReconciledTerminal {
            status: JobStatus::Timeout,
            error: "query exceeded the timeout (recorded by its executor after the job store became reachable again)",
        },
        // `Failed` and — defensively — the states a run cannot compute for
        // itself. Any of them means "this run ended without an answer", which
        // is what `failed` says.
        _ => ReconciledTerminal {
            status: JobStatus::Failed,
            error: RECONCILED_LOST_ERROR,
        },
    }
}

const RECONCILED_LOST_RESULT_ERROR: &str = "this job finished but its result could not be stored (the job store was unavailable); the result was discarded, so resubmit the query";

const RECONCILED_LOST_ERROR: &str = "this job failed and its error could not be stored (the job store was unavailable); resubmit the query to see why";

/// What one [`JobStore::reconcile_finished_jobs`] pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Terminal states this pass installed.
    pub installed: usize,
    /// Rows that already carried a terminal state — an ambiguously
    /// acknowledged write that did land, a client cancellation, recovery —
    /// which is preserved untouched.
    pub preserved: usize,
    /// Rows the TTL sweep had already deleted.
    pub gone: usize,
    /// Ids still parked because the store still is not answering.
    pub retained: usize,
}

#[derive(Debug)]
struct InMemoryRow {
    info: JobInfo,
    owner: String,
    result: Option<RecordsResponse>,
    expires_at: Option<Instant>,
}

/// In-process job registry + batch-runtime owner.
pub struct JobStore {
    backend: Backend,
    batch_runtime: Arc<BatchRuntime>,
    aborts: Arc<Mutex<HashMap<JobId, futures::future::AbortHandle>>>,
    /// The heartbeat/recovery loop is the one background task that must be
    /// stopped and joined before a graceful shutdown removes `owner` from the
    /// shared registration table. Otherwise a late heartbeat can recreate the
    /// row after it was deleted and strand this incarnation's jobs for a full
    /// lease again.
    owner_upkeep: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// This process incarnation's execution-owner id. Stamped on every row
    /// this store submits and the one value recovery must never act on.
    /// Unique per process, so a restarted pod does not inherit ownership of
    /// its predecessor's rows.
    owner: String,
    recovery: JobRecoveryPolicy,
    /// Runs of ours that ENDED without a persisted terminal state. Process-
    /// local like the abort map, and for the same reason: only this process
    /// knows that its own future returned.
    finished_unpersisted: Arc<Mutex<FinishedUnpersisted>>,
    /// Injected job-store write failures, so the paths that only exist because
    /// a shared store can fail are reachable without one.
    #[cfg(test)]
    faults: Arc<Mutex<WriteFaults>>,
    #[cfg(test)]
    read_faults: Arc<Mutex<ReadFaults>>,
    pub ttl: Duration,
}

/// Create job-store gauges whose absence is observably different from zero.
///
/// Call after the process metrics recorder is installed. `JobStore` is built
/// before that recorder during normal query-server startup, so constructors
/// cannot provide this baseline.
pub fn initialize_metrics() {
    metrics::gauge!("siglake_query_jobs_unreconciled").set(0.0);
}

/// Pending injected write failures, newest last. Test-only.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct WriteFaults {
    next: std::collections::VecDeque<WriteFault>,
}

/// How one injected terminal-write failure behaves.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteFault {
    /// The write never reached the store: it errors and changes nothing.
    Refused,
    /// The write LANDED and the acknowledgement was lost: the row is updated
    /// and the caller still sees an error. This is the case a retry has to
    /// recognise as its own predecessor rather than as a race it lost.
    Ambiguous,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReadOperation {
    Info,
    Result,
    Cancel,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadFault {
    Retryable,
    Permanent,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ReadFaults(HashMap<ReadOperation, std::collections::VecDeque<ReadFault>>);

/// Execution-owner id for this process incarnation, from the environment.
fn new_owner_id() -> String {
    owner_id_from(std::env::var("HOSTNAME").ok().as_deref())
}

/// Pure twin of [`new_owner_id`], so tests never touch `HOSTNAME`: pod name
/// (so an operator can find the replica that owns a job) plus a UUIDv7 (so a
/// restart is a *different* owner than the process it replaced — otherwise a
/// restarted pod would inherit ownership of its predecessor's rows and never
/// recover them).
fn owner_id_from(host: Option<&str>) -> String {
    let host = host.filter(|h| !h.is_empty()).unwrap_or("unknown-host");
    format!("{host}/{}", Uuid::now_v7())
}

/// A wedged store, from [`JobStore::wedge_for_test`]. Dropping it lets the
/// store answer again.
#[cfg(test)]
pub(crate) struct StoreWedge(
    // Held, never read: dropping the guard is the whole of unwedging.
    #[allow(dead_code)] tokio::sync::OwnedRwLockWriteGuard<HashMap<JobId, InMemoryRow>>,
);

/// Both variants are handles (an `Arc`, a connection pool), so cloning one
/// shares the store rather than copying it — which is what a second replica's
/// view of the same table is.
#[derive(Clone)]
enum Backend {
    InMemory(Arc<RwLock<HashMap<JobId, InMemoryRow>>>),
    Postgres(PgPool),
}

/// The cancellation sweep's own view of the store: the shared table plus this
/// process's abort map. Split out of [`JobStore`] so the watch loop can be
/// spawned onto the store's own batch runtime without holding the store (which
/// owns that runtime) alive.
struct CancelWatch {
    backend: Backend,
    aborts: Arc<Mutex<HashMap<JobId, futures::future::AbortHandle>>>,
    owner: String,
}

impl CancelWatch {
    async fn sweep(&self) -> Result<Vec<JobId>> {
        // Only the jobs this process is actually executing are candidates: the
        // abort map IS the set of futures we hold, so the query is bounded by
        // this pod's in-flight batch work, not by the size of the table.
        let running: Vec<JobId> = self.aborts.lock().unwrap().keys().copied().collect();
        if running.is_empty() {
            return Ok(Vec::new());
        }
        let cancelled = self.cancelled_among(&running).await?;
        let mut aborted = Vec::new();
        {
            let mut aborts = self.aborts.lock().unwrap();
            for id in cancelled {
                // `remove` before `abort` so a concurrent local `cancel()`
                // cannot abort the same handle twice, and so the future's own
                // `clear_abort` on the way out is a no-op.
                if let Some(abort) = aborts.remove(&id) {
                    abort.abort();
                    aborted.push(id);
                }
            }
        }
        if !aborted.is_empty() {
            metrics::counter!("siglake_query_jobs_cancel_propagated_total")
                .increment(aborted.len() as u64);
            tracing::info!(
                count = aborted.len(),
                owner = %self.owner,
                "aborted batch jobs cancelled by another replica"
            );
        }
        Ok(aborted)
    }

    /// Which of `ids` the shared table says are cancelled.
    async fn cancelled_among(&self, ids: &[JobId]) -> Result<Vec<JobId>> {
        match &self.backend {
            Backend::InMemory(map) => {
                let map = map.read().await;
                Ok(ids
                    .iter()
                    .copied()
                    .filter(|id| {
                        map.get(id)
                            .is_some_and(|row| row.info.status == JobStatus::Cancelled)
                    })
                    .collect())
            }
            Backend::Postgres(pool) => {
                let keys: Vec<String> = ids.iter().map(JobId::as_str).collect();
                let rows = sqlx::query(CANCELLED_AMONG_SQL)
                    .bind(&keys)
                    .fetch_all(pool)
                    .await
                    .context("read cancelled job ids")?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let raw: String = row.try_get("job_id")?;
                    match JobId::parse(&raw) {
                        Ok(id) => out.push(id),
                        // Cannot happen for a row we submitted; ignoring it is
                        // still better than failing the whole sweep, which
                        // would strand every other cancellation in the batch.
                        Err(e) => tracing::warn!(job_id = %raw, error = %e, "unparseable job id"),
                    }
                }
                Ok(out)
            }
        }
    }
}

/// Wrapper that ensures the inner [`tokio::runtime::Runtime`] is shut
/// down via [`tokio::runtime::Runtime::shutdown_background`] — required
/// because the JobStore is typically dropped from inside an async
/// context (axum's worker), and a direct Runtime drop in that context
/// panics with "blocking is not allowed".
pub struct BatchRuntime {
    inner: Option<tokio::runtime::Runtime>,
}

impl BatchRuntime {
    fn new(threads: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.max(1))
            .enable_all()
            .thread_name("siglake-batch")
            .build()
            .expect("build batch runtime");
        Self { inner: Some(rt) }
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.inner
            .as_ref()
            .expect("batch runtime taken before drop")
            .spawn(future)
    }
}

impl Drop for BatchRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.inner.take() {
            rt.shutdown_background();
        }
    }
}

impl std::fmt::Debug for JobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let backend = match &self.backend {
            Backend::InMemory(_) => "in_memory",
            Backend::Postgres(_) => "postgres",
        };
        f.debug_struct("JobStore")
            .field("backend", &backend)
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

impl JobStore {
    /// Build an in-memory JobStore.
    pub fn new(batch_runtime_threads: usize, ttl: Duration) -> Self {
        let store = Self {
            backend: Backend::InMemory(Arc::new(RwLock::new(HashMap::new()))),
            batch_runtime: Arc::new(BatchRuntime::new(batch_runtime_threads)),
            aborts: Arc::new(Mutex::new(HashMap::new())),
            owner_upkeep: Mutex::new(None),
            owner: new_owner_id(),
            recovery: JobRecoveryPolicy::default(),
            finished_unpersisted: Arc::new(Mutex::new(FinishedUnpersisted::default())),
            #[cfg(test)]
            faults: Arc::new(Mutex::new(WriteFaults::default())),
            #[cfg(test)]
            read_faults: Arc::new(Mutex::new(ReadFaults::default())),
            ttl,
        };
        store.spawn_gc();
        store.spawn_reconcile();
        store
    }

    /// Build a Postgres-backed JobStore. Connects to `uri`, runs the
    /// idempotent schema migration, registers this process as an execution
    /// owner, and recovers rows whose owner is demonstrably gone.
    ///
    /// It does **not** touch rows owned by a replica that is still
    /// heartbeating: the store is shared by every query replica, so a
    /// starting or restarting pod must leave its siblings' in-flight jobs —
    /// and their eventual results — alone. Recovery repeats on the
    /// heartbeat cadence, so a peer that dies while this pod stays up is
    /// still resolved to a terminal state within one lease period.
    pub async fn new_postgres(
        uri: &str,
        batch_runtime_threads: usize,
        ttl: Duration,
        recovery: JobRecoveryPolicy,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(uri)
            .await
            .with_context(|| format!("connect Postgres {uri}"))?;

        // sqlx::query() sends one prepared statement per call;
        // Postgres rejects multi-statement payloads. Split the DDL
        // into one execute per `CREATE`.
        for stmt in SCHEMA_SQL_STATEMENTS {
            sqlx::query(stmt).execute(&pool).await.with_context(|| {
                format!("schema migration: {}", stmt.lines().next().unwrap_or(""))
            })?;
        }

        let owner = new_owner_id();
        // Register before the first recovery pass and before any row can be
        // submitted: an owner that has jobs but no registration reads as
        // dead, so the ordering here is what keeps our own rows safe.
        write_owner_heartbeat(&pool, &owner)
            .await
            .context("register execution owner")?;

        let store = Self {
            backend: Backend::Postgres(pool),
            batch_runtime: Arc::new(BatchRuntime::new(batch_runtime_threads)),
            aborts: Arc::new(Mutex::new(HashMap::new())),
            owner_upkeep: Mutex::new(None),
            owner,
            recovery,
            finished_unpersisted: Arc::new(Mutex::new(FinishedUnpersisted::default())),
            #[cfg(test)]
            faults: Arc::new(Mutex::new(WriteFaults::default())),
            #[cfg(test)]
            read_faults: Arc::new(Mutex::new(ReadFaults::default())),
            ttl,
        };

        match store.recover_orphaned_jobs().await {
            Ok(outcome) => log_recovery(&outcome),
            // A recovery failure must not stop the replica from serving:
            // the sweep runs again on the heartbeat cadence.
            Err(e) => tracing::warn!(error = %e, "startup job recovery failed"),
        }
        store.spawn_owner_upkeep();
        store.spawn_cancel_watch();
        store.spawn_gc();
        store.spawn_reconcile();
        Ok(store)
    }

    /// A second store over another store's **in-memory** backend: its own
    /// execution-owner id and its own process-local abort map, sharing one job
    /// table. `None` for the Postgres backend, where a second replica is
    /// [`Self::new_postgres`] and must run its own schema migration, owner
    /// registration and upkeep.
    ///
    /// This exists because the cross-replica cancellation path is otherwise
    /// untestable without a live database: what makes the path hard is that the
    /// store is shared while the abort map is not, and that is exactly the
    /// shape this builds. The Postgres twin of the same assertions lives in
    /// `tests/jobs_postgres_ownership.rs` behind `#[ignore]`.
    pub fn new_peer_of(
        other: &JobStore,
        batch_runtime_threads: usize,
        ttl: Duration,
    ) -> Option<Self> {
        let Backend::InMemory(map) = &other.backend else {
            return None;
        };
        let store = Self {
            backend: Backend::InMemory(map.clone()),
            batch_runtime: Arc::new(BatchRuntime::new(batch_runtime_threads)),
            aborts: Arc::new(Mutex::new(HashMap::new())),
            owner_upkeep: Mutex::new(None),
            owner: new_owner_id(),
            recovery: other.recovery,
            finished_unpersisted: Arc::new(Mutex::new(FinishedUnpersisted::default())),
            #[cfg(test)]
            faults: Arc::new(Mutex::new(WriteFaults::default())),
            #[cfg(test)]
            read_faults: Arc::new(Mutex::new(ReadFaults::default())),
            ttl,
        };
        store.spawn_gc();
        store.spawn_reconcile();
        Some(store)
    }

    /// This process incarnation's execution-owner id.
    pub fn owner_id(&self) -> &str {
        &self.owner
    }

    /// Heartbeat this owner's lease, prune long-dead owners and re-run
    /// recovery, forever. Runs on the batch runtime — not the interactive
    /// one — and dies with it when the store is dropped.
    fn spawn_owner_upkeep(&self) {
        let Backend::Postgres(pool) = &self.backend else {
            return;
        };
        let pool = pool.clone();
        let owner = self.owner.clone();
        let policy = self.recovery;
        let ttl = self.ttl;
        let interval = policy.heartbeat_interval();
        let upkeep = self.batch_runtime.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = write_owner_heartbeat(&pool, &owner).await {
                    // Losing a beat is survivable (the lease is three of
                    // them); losing the database is the query path's
                    // problem too, and it reports separately.
                    tracing::warn!(error = %e, "job owner heartbeat failed");
                    continue;
                }
                match recover_orphans(&pool, &owner, &policy, ttl).await {
                    Ok(outcome) => {
                        if outcome.recovered > 0 {
                            log_recovery(&outcome);
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "periodic job recovery failed"),
                }
                if let Err(e) = prune_dead_owners(&pool, &policy).await {
                    tracing::warn!(error = %e, "pruning dead job owners failed");
                }
            }
        });
        *self.owner_upkeep.lock().unwrap() = Some(upkeep);
    }

    /// Retire this process incarnation before the query server exits.
    ///
    /// The caller must first stop accepting requests and drain in-flight HTTP
    /// handlers, so no new batch future can be registered while this runs.
    /// Local batch futures are abandoned first, then the heartbeat loop is
    /// stopped and joined, and only then is the Postgres owner registration
    /// deleted. If the final DELETE fails, upkeep remains stopped: the last
    /// heartbeat ages out and preserves the ordinary bounded crash fallback.
    pub async fn shutdown(&self) -> Result<()> {
        let aborts: Vec<futures::future::AbortHandle> = self
            .aborts
            .lock()
            .unwrap()
            .drain()
            .map(|(_, abort)| abort)
            .collect();
        for abort in &aborts {
            abort.abort();
        }
        if !aborts.is_empty() {
            tracing::info!(
                count = aborts.len(),
                owner = %self.owner,
                "abandoned local batch jobs during shutdown"
            );
        }

        let upkeep = self.owner_upkeep.lock().unwrap().take();
        if let Some(upkeep) = upkeep {
            upkeep.abort();
            match upkeep.await {
                Ok(()) => {}
                Err(e) if e.is_cancelled() => {}
                Err(e) => tracing::warn!(error = %e, "joining job owner upkeep failed"),
            }
        }

        let Backend::Postgres(pool) = &self.backend else {
            return Ok(());
        };
        release_owner_registration(pool, &self.owner).await?;
        tracing::info!(owner = %self.owner, "released batch-job owner registration");
        Ok(())
    }

    /// Poll the shared table for cancellations of the jobs this process is
    /// executing, forever. Postgres only: with the in-memory backend the
    /// replica that persists a cancellation is by definition the one holding
    /// the abort handle, so [`Self::cancel`] has already aborted it and this
    /// loop would be pure overhead.
    ///
    /// Runs on the batch runtime alongside the owner upkeep, so it dies with
    /// the store.
    fn spawn_cancel_watch(&self) {
        if !matches!(self.backend, Backend::Postgres(_)) {
            return;
        }
        let interval = self.recovery.cancel_poll;
        // Clone the handles the sweep needs rather than the store: the store
        // owns the runtime this task runs on.
        let watch = self.cancel_watch();
        self.batch_runtime.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = watch.sweep().await {
                    // Losing a sweep delays a cancellation by one interval; it
                    // never resurrects a cancelled row, because the status
                    // write already landed on the replica that served DELETE.
                    tracing::warn!(error = %e, "batch cancellation sweep failed");
                }
            }
        });
    }

    /// Sweep expired terminal rows on the batch runtime, independently of
    /// owner upkeep. In particular, a slow `DELETE` must not delay the owner
    /// heartbeat that keeps live work from being condemned by another pod.
    ///
    /// The task clones only the backend handle, so dropping the store shuts
    /// down its runtime and the sweep with it.
    fn spawn_gc(&self) {
        let backend = self.backend.clone();
        let interval = gc_interval(self.ttl);
        self.batch_runtime.spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = gc_backend(&backend).await {
                    // A failed pass retains rows until the next pass; it does
                    // not affect their terminal state or query execution.
                    tracing::warn!(error = %e, "job TTL sweep failed");
                }
            }
        });
    }

    /// One cross-replica cancellation sweep: abort every future this process is
    /// executing whose row now reads `cancelled`, whichever replica wrote it.
    ///
    /// Aborting is the point. The status write releases nothing — the admission
    /// reservation and the storage-scan cancel guard are both owned by the
    /// spawned future, so until it is dropped a cancelled job still holds a
    /// quarter of the pod's admission budget and still pumps scans.
    ///
    /// Returns the ids aborted. Exposed (and not merely spawned) so a test can
    /// drive the sweep deterministically instead of racing an interval.
    pub async fn propagate_cancellations(&self) -> Result<Vec<JobId>> {
        self.cancel_watch().sweep().await
    }

    fn cancel_watch(&self) -> CancelWatch {
        CancelWatch {
            backend: self.backend.clone(),
            aborts: self.aborts.clone(),
            owner: self.owner.clone(),
        }
    }

    /// Fail every non-terminal row whose executor is demonstrably gone,
    /// leaving live owners' rows untouched. No-op for the in-memory backend,
    /// which cannot outlive its executor.
    pub async fn recover_orphaned_jobs(&self) -> Result<RecoveryOutcome> {
        match &self.backend {
            Backend::InMemory(_) => Ok(RecoveryOutcome::default()),
            Backend::Postgres(pool) => {
                recover_orphans(pool, &self.owner, &self.recovery, self.ttl).await
            }
        }
    }

    /// Test seam for pinning the read/write recovery interleaving against a
    /// real shared Postgres store. Production recovery always calls both
    /// halves through [`Self::recover_orphaned_jobs`].
    #[doc(hidden)]
    pub async fn ownership_candidates_for_test(&self) -> Result<Vec<JobOwnership>> {
        match &self.backend {
            Backend::Postgres(pool) => read_ownership_candidates(pool).await,
            Backend::InMemory(_) => Ok(Vec::new()),
        }
    }

    /// Condemn a previously selected set with the same guarded write used by
    /// production recovery. Exists only to put a heartbeat deterministically
    /// between selection and write in the live-Postgres regression.
    #[doc(hidden)]
    pub async fn condemn_selected_for_test(
        &self,
        reason: OrphanReason,
        ids: &[String],
    ) -> Result<u64> {
        match &self.backend {
            Backend::Postgres(pool) => {
                apply_recovery_update(pool, reason, ids, &self.recovery, self.ttl).await
            }
            Backend::InMemory(_) => Ok(0),
        }
    }

    /// Wedge this store: every read and write against it is *pending* until the
    /// returned guard is dropped.
    ///
    /// The blocked `UPDATE` a Postgres that has stopped answering serves is
    /// otherwise unreachable from a test, and it is the reason the batch
    /// lifecycle publications are bounded by the run's deadline — a publication
    /// that never returns holds admitted work (a quarter of the pod's admission
    /// budget, a batch-runtime thread) for as long as the store takes.
    #[cfg(test)]
    pub(crate) async fn wedge_for_test(&self) -> StoreWedge {
        match &self.backend {
            Backend::InMemory(map) => StoreWedge(Arc::clone(map).write_owned().await),
            Backend::Postgres(_) => unreachable!("wedge_for_test is an in-memory-backend seam"),
        }
    }

    /// Reference to the dedicated batch runtime.
    pub fn batch_runtime(&self) -> &Arc<BatchRuntime> {
        &self.batch_runtime
    }

    /// Register the handle that drops a spawned batch future on client
    /// cancellation. Kept outside the persistence backend because it is
    /// necessarily process-local and is discarded on restart with the runtime.
    ///
    /// Public alongside [`Self::batch_runtime`]: whoever spawns the future is
    /// the only one who can hand over the handle that drops it, and the set of
    /// registered handles is what
    /// [`Self::propagate_cancellations`] reads as "the jobs this process is
    /// executing".
    pub fn register_abort(&self, id: JobId, abort: futures::future::AbortHandle) {
        self.aborts.lock().unwrap().insert(id, abort);
    }

    /// Remove a completed job's process-local cancellation handle. A no-op
    /// when a cancellation sweep already took it.
    pub fn clear_abort(&self, id: JobId) {
        self.aborts.lock().unwrap().remove(&id);
    }

    /// Submit a new job. Returns the assigned `JobId`. The caller is
    /// responsible for spawning the actual work onto
    /// [`JobStore::batch_runtime`] and calling [`JobStore::set_running`]
    /// before it starts, [`JobStore::record_cost`] once planning has run, and
    /// [`JobStore::finish_succeeded`] / [`JobStore::finish_failed`] when it
    /// ends.
    pub async fn submit(
        &self,
        query: String,
        priority: Priority,
        tenant: Option<String>,
    ) -> Result<JobId> {
        let id = JobId::new();
        let info = JobInfo {
            tenant,
            job_id: id.to_string(),
            status: JobStatus::Pending,
            priority,
            submitted_at: Utc::now(),
            started_at: None,
            ended_at: None,
            query,
            cost: None,
            error: None,
        };
        match &self.backend {
            Backend::InMemory(map) => {
                map.write().await.insert(
                    id,
                    InMemoryRow {
                        info,
                        owner: self.owner.clone(),
                        result: None,
                        expires_at: None,
                    },
                );
            }
            Backend::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO siglake_query_jobs
                       (job_id, status, priority, submitted_at, query, tenant, owner)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(info.job_id)
                .bind(JobStatus::Pending.label())
                .bind(priority.label())
                .bind(info.submitted_at)
                .bind(info.query)
                .bind(info.tenant)
                .bind(&self.owner)
                .execute(pool)
                .await
                .context("INSERT job row")?;
            }
        }
        Ok(id)
    }

    /// Publish `running`, before the work this job describes is executed.
    ///
    /// `started_at` is only ever SET, never moved: it is the moment execution
    /// began, and a republication must not rewrite it. Conditional on
    /// `status IN ('pending', 'running')` like every terminal write, so
    /// publishing progress can never revive a job a client cancelled or
    /// recovery condemned — the loser is told which state beat it, exactly as
    /// [`Self::finish_succeeded`] is.
    pub async fn set_running(&self, id: JobId) -> Result<CompletionOutcome> {
        match &self.backend {
            Backend::InMemory(map) => {
                let mut map = map.write().await;
                let Some(row) = map.get_mut(&id) else {
                    return Ok(CompletionOutcome::Vanished);
                };
                if row.info.status.is_terminal() {
                    return Ok(CompletionOutcome::Superseded {
                        status: row.info.status,
                        owned_by_us: row.owner == self.owner,
                        recovered: false,
                    });
                }
                row.info.status = JobStatus::Running;
                row.info.started_at.get_or_insert_with(Utc::now);
                Ok(CompletionOutcome::Applied {
                    status: JobStatus::Running,
                })
            }
            Backend::Postgres(pool) => {
                let res = sqlx::query(SET_RUNNING_SQL)
                    .bind(id.to_string())
                    .execute(pool)
                    .await
                    .context("UPDATE → running")?;
                if res.rows_affected() > 0 {
                    return Ok(CompletionOutcome::Applied {
                        status: JobStatus::Running,
                    });
                }
                lost_conditional_write(pool, id, &self.owner, false).await
            }
        }
    }

    /// Attach the cost estimate to a job that is still executing.
    ///
    /// Separate from [`Self::set_running`] because the two facts become true at
    /// different moments: execution starts before anything is planned, and the
    /// estimate exists as soon as planning has run — which is when
    /// `GET /api/v1/jobs/{id}` promises to serve it, long before the answer it
    /// describes. Touches neither status nor `started_at`, and carries the same
    /// `status IN ('pending', 'running')` guard, so a late estimate cannot
    /// resurrect a terminal row.
    pub async fn record_cost(&self, id: JobId, cost: CostReport) -> Result<CompletionOutcome> {
        match &self.backend {
            Backend::InMemory(map) => {
                let mut map = map.write().await;
                let Some(row) = map.get_mut(&id) else {
                    return Ok(CompletionOutcome::Vanished);
                };
                if row.info.status.is_terminal() {
                    return Ok(CompletionOutcome::Superseded {
                        status: row.info.status,
                        owned_by_us: row.owner == self.owner,
                        recovered: false,
                    });
                }
                row.info.cost = Some(cost);
                Ok(CompletionOutcome::Applied {
                    status: row.info.status,
                })
            }
            Backend::Postgres(pool) => {
                let cost_json = serde_json::to_value(&cost)?;
                // `RETURNING status` rather than an assumed `running`: the row
                // this landed on is the one the client reads, and reporting a
                // status the store did not install is the same lie a terminal
                // write is gated against.
                let row = sqlx::query(RECORD_COST_SQL)
                    .bind(cost_json)
                    .bind(id.to_string())
                    .fetch_optional(pool)
                    .await
                    .context("UPDATE → cost")?;
                let Some(row) = row else {
                    return lost_conditional_write(pool, id, &self.owner, false).await;
                };
                let raw: String = row.try_get("status")?;
                match JobStatus::parse(&raw) {
                    Some(status) => Ok(CompletionOutcome::Applied { status }),
                    None => {
                        tracing::warn!(job_id = %id, status = %raw, "unknown job status");
                        Ok(CompletionOutcome::Vanished)
                    }
                }
            }
        }
    }

    /// Record success, and report what the store **installed** — see
    /// [`CompletionOutcome`].
    ///
    /// Anything but `Applied { status: Succeeded }` means this result is
    /// discarded: a cancellation (possibly persisted by another replica) or
    /// recovery got there first, or the body was too large for the row and
    /// went in as `failed`. The caller must not then claim the job succeeded.
    pub async fn finish_succeeded(
        &self,
        id: JobId,
        result: RecordsResponse,
    ) -> Result<CompletionOutcome> {
        #[cfg(test)]
        if let Some(fault) = self.take_write_fault() {
            return injected_fault(
                fault,
                install_succeeded(&self.backend, &self.owner, self.ttl, id, result),
            )
            .await;
        }
        install_succeeded(&self.backend, &self.owner, self.ttl, id, result).await
    }

    /// Record failure or timeout. Same contract as
    /// [`Self::finish_succeeded`]: anything but `Applied` means a terminal
    /// state was already installed and this verdict lost.
    pub async fn finish_failed(
        &self,
        id: JobId,
        error: String,
        is_timeout: bool,
    ) -> Result<CompletionOutcome> {
        #[cfg(test)]
        if let Some(fault) = self.take_write_fault() {
            return injected_fault(
                fault,
                install_failed(&self.backend, &self.owner, self.ttl, id, error, is_timeout),
            )
            .await;
        }
        install_failed(&self.backend, &self.owner, self.ttl, id, error, is_timeout).await
    }

    /// Queue `count` injected failures of `fault` for the next terminal
    /// writes, so the run path's bounded-retry and ambiguous-acknowledgement
    /// branches are reachable without a store that can actually fail.
    #[cfg(test)]
    pub(crate) fn fail_next_writes(&self, fault: WriteFault, count: usize) {
        let mut faults = self.faults.lock().unwrap();
        for _ in 0..count {
            faults.next.push_back(fault);
        }
    }

    #[cfg(test)]
    fn take_write_fault(&self) -> Option<WriteFault> {
        self.faults.lock().unwrap().next.pop_front()
    }

    /// Record that a run's execution ENDED without a terminal state being
    /// installed, so [`Self::reconcile_finished_jobs`] keeps trying once the
    /// run has returned and released its admission reservation.
    ///
    /// `computed` is the verdict the run reached, and the only thing kept: the
    /// result body is not retained (see [`FinishedUnpersisted`]).
    ///
    /// This is owner-local *positive evidence*: recovery keeps a self-owned
    /// row precisely because a live executor might still be working on it, and
    /// the caller of this function is the executor saying it is not. It is
    /// also why the ids are not persisted — a process that exits stops being
    /// the row's owner, and lease-expiry recovery takes over from there.
    pub fn park_finished_unpersisted(&self, id: JobId, computed: JobStatus) -> ParkOutcome {
        let mut parked = self.finished_unpersisted.lock().unwrap();
        if !parked.computed.contains_key(&id) && parked.computed.len() >= MAX_FINISHED_UNPERSISTED {
            metrics::counter!("siglake_query_jobs_unreconciled_dropped_total").increment(1);
            tracing::error!(
                job_id = %id,
                owner = %self.owner,
                computed = computed.label(),
                tracked = parked.computed.len(),
                "job-store outage outlasted this replica's reconciliation bookkeeping; \
                 this row stays non-terminal until the replica exits"
            );
            return ParkOutcome::Full;
        }
        parked.computed.insert(id, computed);
        metrics::gauge!("siglake_query_jobs_unreconciled").set(parked.computed.len() as f64);
        ParkOutcome::Tracked
    }

    /// One owner-local reconciliation pass: for every id this process parked,
    /// install the terminal state its run computed — or leave the terminal
    /// state somebody else already installed exactly as it is.
    ///
    /// The write is [`Self::finish_failed`]'s, so it is conditional on
    /// `status IN ('pending','running')` like every other terminal write: this
    /// pass can resolve a `running` row, never reopen or overwrite a terminal
    /// one. Nothing is re-executed and no result body is republished — a lost
    /// success becomes an explicit "resubmit" failure ([`reconciled_terminal`]).
    ///
    /// Exposed (and not merely spawned) so a test can drive one pass instead of
    /// racing [`RECONCILE_INTERVAL`], exactly as
    /// [`Self::propagate_cancellations`] is.
    pub async fn reconcile_finished_jobs(&self) -> ReconcileOutcome {
        self.reconciler().pass().await
    }

    fn reconciler(&self) -> Reconciler {
        Reconciler {
            backend: self.backend.clone(),
            owner: self.owner.clone(),
            ttl: self.ttl,
            finished_unpersisted: self.finished_unpersisted.clone(),
        }
    }

    /// Retry parked terminal writes forever, on [`RECONCILE_INTERVAL`]. Clones
    /// only what one pass needs, so dropping the store shuts its runtime — and
    /// this loop — down.
    fn spawn_reconcile(&self) {
        let reconciler = self.reconciler();
        self.batch_runtime.spawn(async move {
            loop {
                tokio::time::sleep(RECONCILE_INTERVAL).await;
                let outcome = reconciler.pass().await;
                if outcome.installed > 0 || outcome.preserved > 0 || outcome.gone > 0 {
                    tracing::info!(
                        installed = outcome.installed,
                        preserved = outcome.preserved,
                        gone = outcome.gone,
                        retained = outcome.retained,
                        "reconciled finished batch jobs whose terminal write had failed"
                    );
                }
            }
        });
    }

    pub async fn info(&self, id: JobId) -> std::result::Result<Option<JobInfo>, JobStoreError> {
        #[cfg(test)]
        if let Some(fault) = self.take_read_fault(ReadOperation::Info) {
            return Err(injected_read_fault(fault));
        }
        match &self.backend {
            Backend::InMemory(map) => {
                let map = map.read().await;
                Ok(map.get(&id).map(|r| r.info.clone()))
            }
            Backend::Postgres(pool) => read_info(pool, id)
                .await
                .map_err(|error| JobStoreError::backend(error.context("read job info"))),
        }
    }

    pub async fn result(&self, id: JobId) -> std::result::Result<Option<JobResult>, JobStoreError> {
        #[cfg(test)]
        if let Some(fault) = self.take_read_fault(ReadOperation::Result) {
            return Err(injected_read_fault(fault));
        }
        match &self.backend {
            Backend::InMemory(map) => {
                let map = map.read().await;
                let Some(row) = map.get(&id) else {
                    return Ok(None);
                };
                Ok(Some(JobResult {
                    status: row.info.status,
                    body: row.result.clone(),
                }))
            }
            Backend::Postgres(pool) => read_result(pool, id)
                .await
                .map_err(|error| JobStoreError::backend(error.context("read job result"))),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_read(&self, operation: ReadOperation, fault: ReadFault) {
        self.read_faults
            .lock()
            .unwrap()
            .0
            .entry(operation)
            .or_default()
            .push_back(fault);
    }

    #[cfg(test)]
    fn take_read_fault(&self, operation: ReadOperation) -> Option<ReadFault> {
        self.read_faults
            .lock()
            .unwrap()
            .0
            .get_mut(&operation)
            .and_then(std::collections::VecDeque::pop_front)
    }

    /// Persist a cancellation and stop the work, in that order.
    ///
    /// The status write is the authoritative part: it is conditional on
    /// `status IN ('pending','running')`, so it either wins the race against a
    /// completion or reports `false`, and it is visible to every replica.
    /// The local abort below is only the *fast path* for the case where this
    /// process happens to be the executor. When it is not — the usual case with
    /// a shared store and more than one query replica — the abort map has no
    /// handle for this id and the executing replica picks the cancellation up
    /// on its next [`Self::propagate_cancellations`] sweep.
    pub async fn cancel(&self, id: JobId) -> std::result::Result<bool, JobStoreError> {
        #[cfg(test)]
        if let Some(fault) = self.take_read_fault(ReadOperation::Cancel) {
            return Err(injected_read_fault(fault));
        }
        let cancelled = match &self.backend {
            Backend::InMemory(map) => {
                let mut map = map.write().await;
                let row = match map.get_mut(&id) {
                    Some(r) => r,
                    None => return Ok(false),
                };
                if row.info.status.is_terminal() {
                    return Ok(false);
                }
                row.info.status = JobStatus::Cancelled;
                row.info.ended_at = Some(Utc::now());
                row.info.error = Some("cancelled by client".into());
                row.expires_at = Some(Instant::now() + self.ttl);
                true
            }
            Backend::Postgres(pool) => {
                let expires_at =
                    Utc::now() + chrono::Duration::from_std(self.ttl).unwrap_or_default();
                sqlx::query(
                    "UPDATE siglake_query_jobs
                     SET status = 'cancelled', ended_at = NOW(),
                         error  = COALESCE(error, 'cancelled by client'),
                         expires_at = $1
                     WHERE job_id = $2 AND status IN ('pending', 'running')",
                )
                .bind(expires_at)
                .bind(id.to_string())
                .execute(pool)
                .await
                .map_err(|error| {
                    JobStoreError::backend(anyhow::Error::new(error).context("cancel job"))
                })?
                .rows_affected()
                    > 0
            }
        };
        if cancelled {
            if let Some(abort) = self.aborts.lock().unwrap().remove(&id) {
                abort.abort();
            }
        }
        Ok(cancelled)
    }

    /// Sweep entries past their TTL. Returns the number evicted.
    pub async fn gc(&self) -> usize {
        match gc_backend(&self.backend).await {
            Ok(evicted) => evicted,
            Err(e) => {
                tracing::warn!(error = %e, "job TTL sweep failed");
                0
            }
        }
    }

    pub async fn active_count(&self) -> usize {
        match &self.backend {
            Backend::InMemory(map) => {
                let map = map.read().await;
                map.values()
                    .filter(|r| !r.info.status.is_terminal())
                    .count()
            }
            Backend::Postgres(pool) => {
                let res: Result<i64, _> = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM siglake_query_jobs WHERE status IN ('pending', 'running')",
                )
                .fetch_one(pool)
                .await;
                res.unwrap_or(0) as usize
            }
        }
    }
}

/// The success write itself, as a free function over the three things it
/// needs, so [`JobStore::finish_succeeded`], the injected-fault seam and the
/// reconciliation pass all write through exactly one implementation.
async fn install_succeeded(
    backend: &Backend,
    self_owner: &str,
    ttl: Duration,
    id: JobId,
    result: RecordsResponse,
) -> Result<CompletionOutcome> {
    let expires_at = Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
    match backend {
        Backend::InMemory(map) => {
            let mut map = map.write().await;
            let Some(row) = map.get_mut(&id) else {
                return Ok(CompletionOutcome::Vanished);
            };
            if row.info.status.is_terminal() {
                return Ok(CompletionOutcome::Superseded {
                    status: row.info.status,
                    owned_by_us: row.owner == self_owner,
                    recovered: false,
                });
            }
            row.info.status = JobStatus::Succeeded;
            row.info.ended_at = Some(Utc::now());
            row.result = Some(result);
            row.expires_at = Some(Instant::now() + ttl);
            Ok(CompletionOutcome::Applied {
                status: JobStatus::Succeeded,
            })
        }
        Backend::Postgres(pool) => {
            let result_json = serde_json::to_value(&result)?;
            if let Some(reason) = result_cap_breach(&result_json) {
                return install_failed(backend, self_owner, ttl, id, reason, false).await;
            }
            let res = sqlx::query(
                "UPDATE siglake_query_jobs
                     SET status = 'succeeded',
                         ended_at = NOW(),
                         result_json = $1,
                         expires_at = $2
                     WHERE job_id = $3 AND status IN ('pending', 'running')",
            )
            .bind(result_json)
            .bind(expires_at)
            .bind(id.to_string())
            .execute(pool)
            .await
            .context("UPDATE → succeeded")?;
            if res.rows_affected() > 0 {
                return Ok(CompletionOutcome::Applied {
                    status: JobStatus::Succeeded,
                });
            }
            lost_conditional_write(pool, id, self_owner, true).await
        }
    }
}

/// The failure/timeout write itself. Conditional on
/// `status IN ('pending','running')`, which is what lets the reconciliation
/// pass reuse it: it resolves a row that is still non-terminal and reports the
/// winner for one that is not.
async fn install_failed(
    backend: &Backend,
    self_owner: &str,
    ttl: Duration,
    id: JobId,
    error: String,
    is_timeout: bool,
) -> Result<CompletionOutcome> {
    let status = if is_timeout {
        JobStatus::Timeout
    } else {
        JobStatus::Failed
    };
    let expires_at = Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default();
    match backend {
        Backend::InMemory(map) => {
            let mut map = map.write().await;
            let Some(row) = map.get_mut(&id) else {
                return Ok(CompletionOutcome::Vanished);
            };
            if row.info.status.is_terminal() {
                return Ok(CompletionOutcome::Superseded {
                    status: row.info.status,
                    owned_by_us: row.owner == self_owner,
                    recovered: false,
                });
            }
            row.info.status = status;
            row.info.ended_at = Some(Utc::now());
            row.info.error = Some(error);
            row.expires_at = Some(Instant::now() + ttl);
            Ok(CompletionOutcome::Applied { status })
        }
        Backend::Postgres(pool) => {
            let res = sqlx::query(
                "UPDATE siglake_query_jobs
                     SET status = $1, ended_at = NOW(), error = $2, expires_at = $3
                     WHERE job_id = $4 AND status IN ('pending', 'running')",
            )
            .bind(status.label())
            .bind(error)
            .bind(expires_at)
            .bind(id.to_string())
            .execute(pool)
            .await
            .context("UPDATE → failed/timeout")?;
            if res.rows_affected() > 0 {
                return Ok(CompletionOutcome::Applied { status });
            }
            lost_conditional_write(pool, id, self_owner, true).await
        }
    }
}

/// Run a terminal write under an injected fault: [`WriteFault::Ambiguous`]
/// performs the write and then reports the error a lost acknowledgement would
/// produce, [`WriteFault::Refused`] never touches the store.
#[cfg(test)]
async fn injected_fault(
    fault: WriteFault,
    write: impl std::future::Future<Output = Result<CompletionOutcome>>,
) -> Result<CompletionOutcome> {
    if fault == WriteFault::Ambiguous {
        write.await?;
    }
    Err(anyhow::anyhow!(
        "injected job-store write failure ({fault:?})"
    ))
}

#[cfg(test)]
fn injected_read_fault(fault: ReadFault) -> JobStoreError {
    let error = match fault {
        ReadFault::Retryable => sqlx::Error::PoolClosed,
        ReadFault::Permanent => sqlx::Error::ColumnNotFound("injected".into()),
    };
    JobStoreError::backend(anyhow::Error::new(error).context("injected job-store read failure"))
}

/// One reconciliation pass's view of the store: the shared table plus this
/// process's parked ids. Split out of [`JobStore`] like [`CancelWatch`] so the
/// loop can run on the store's own batch runtime without holding the store
/// (which owns that runtime) alive.
struct Reconciler {
    backend: Backend,
    owner: String,
    ttl: Duration,
    finished_unpersisted: Arc<Mutex<FinishedUnpersisted>>,
}

/// Per-write bound inside a reconciliation pass. A store that accepts the
/// connection and never answers must not wedge the loop: one id's stalled
/// write would otherwise strand every other parked id behind it. A cut write
/// is indistinguishable from a failed one here — the id stays parked and the
/// next pass tries again.
const RECONCILE_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

impl Reconciler {
    async fn pass(&self) -> ReconcileOutcome {
        // A snapshot, so the lock is never held across a write, and so an id
        // parked while this pass runs is simply picked up by the next one.
        let parked: Vec<(JobId, JobStatus)> = {
            let parked = self.finished_unpersisted.lock().unwrap();
            parked.computed.iter().map(|(id, s)| (*id, *s)).collect()
        };
        let mut outcome = ReconcileOutcome::default();
        for (id, computed) in parked {
            let terminal = reconciled_terminal(computed);
            let write = install_failed(
                &self.backend,
                &self.owner,
                self.ttl,
                id,
                terminal.error.to_string(),
                terminal.status == JobStatus::Timeout,
            );
            let settled = match tokio::time::timeout(RECONCILE_WRITE_TIMEOUT, write).await {
                Ok(Ok(installed)) => Some(installed),
                Ok(Err(e)) => {
                    tracing::warn!(
                        job_id = %id,
                        owner = %self.owner,
                        computed = computed.label(),
                        error = %e,
                        "reconciling a finished batch job's terminal state failed; will retry"
                    );
                    None
                }
                Err(_) => {
                    tracing::warn!(
                        job_id = %id,
                        owner = %self.owner,
                        computed = computed.label(),
                        timeout_secs = RECONCILE_WRITE_TIMEOUT.as_secs(),
                        "reconciling a finished batch job's terminal state timed out; will retry"
                    );
                    None
                }
            };
            let Some(installed) = settled else {
                outcome.retained += 1;
                continue;
            };
            // Settled either way: the row is terminal now, and nothing this
            // process knows will change it.
            self.settle(id);
            match installed {
                CompletionOutcome::Applied { status } => {
                    outcome.installed += 1;
                    // The executor DID install this terminal state, just later
                    // than its run did; `siglake_query_jobs_total` counts what
                    // this executor installed, so it is counted here.
                    metrics::counter!(
                        "siglake_query_jobs_total",
                        "priority" => Priority::Batch.label(),
                        "outcome" => status.label()
                    )
                    .increment(1);
                    reconcile_counter(computed, status.label());
                    tracing::warn!(
                        job_id = %id,
                        owner = %self.owner,
                        computed = computed.label(),
                        installed = status.label(),
                        "resolved a finished batch job whose terminal write had failed"
                    );
                }
                // An ambiguously acknowledged write that DID land, a client
                // cancellation, or recovery. Preserved untouched: the
                // conditional write matched nothing.
                CompletionOutcome::Superseded { status, .. } => {
                    outcome.preserved += 1;
                    reconcile_counter(computed, status.label());
                }
                CompletionOutcome::Vanished => {
                    outcome.gone += 1;
                    reconcile_counter(computed, "gone");
                }
            }
        }
        outcome
    }

    fn settle(&self, id: JobId) {
        let mut parked = self.finished_unpersisted.lock().unwrap();
        parked.computed.remove(&id);
        metrics::gauge!("siglake_query_jobs_unreconciled").set(parked.computed.len() as f64);
    }
}

/// `computed` is what the run decided, `outcome` what the row reads once
/// reconciliation is done with it — both bounded label sets.
fn reconcile_counter(computed: JobStatus, outcome: &'static str) {
    metrics::counter!(
        "siglake_query_jobs_reconciled_total",
        "computed" => computed.label(),
        "outcome" => outcome
    )
    .increment(1);
}

const MIN_GC_INTERVAL: Duration = Duration::from_millis(10);
const MAX_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Avoid polling more often than rows can normally expire, while bounding
/// retention drift for the production one-day TTL. The floor prevents a zero
/// TTL from turning the batch runtime into a busy loop.
fn gc_interval(ttl: Duration) -> Duration {
    ttl.clamp(MIN_GC_INTERVAL, MAX_GC_INTERVAL)
}

async fn gc_backend(backend: &Backend) -> Result<usize> {
    let evicted = match backend {
        Backend::InMemory(map) => {
            let now = Instant::now();
            let mut map = map.write().await;
            let before = map.len();
            map.retain(|_, row| row.expires_at.is_none_or(|expires_at| expires_at > now));
            before - map.len()
        }
        Backend::Postgres(pool) => sqlx::query(GC_EXPIRED_SQL)
            .execute(pool)
            .await
            .context("delete expired job rows")?
            .rows_affected() as usize,
    };
    if evicted > 0 {
        tracing::debug!(evicted, "job_store: ttl gc");
    }
    Ok(evicted)
}

/// Upsert this owner's registration row and stamp its heartbeat.
async fn write_owner_heartbeat(pool: &PgPool, owner: &str) -> Result<()> {
    sqlx::query(OWNER_HEARTBEAT_SQL)
        .bind(owner)
        .execute(pool)
        .await
        .context("owner heartbeat upsert")?;
    Ok(())
}

/// Delete exactly this process incarnation's registration. Job rows retain
/// their owner id, so a peer's next recovery pass classifies them as
/// `owner_unregistered` and rechecks the missing registration in its guarded
/// terminal write.
async fn release_owner_registration(pool: &PgPool, owner: &str) -> Result<()> {
    sqlx::query(RELEASE_OWNER_SQL)
        .bind(owner)
        .execute(pool)
        .await
        .context("release owner registration")?;
    Ok(())
}

/// Drop registrations for owners that have been silent far longer than the
/// lease and hold no non-terminal rows. The `NOT EXISTS` guard is what keeps
/// [`OrphanReason::OwnerUnregistered`] from becoming the ordinary path:
/// a dead owner's jobs are always recovered by lease expiry first.
async fn prune_dead_owners(pool: &PgPool, policy: &JobRecoveryPolicy) -> Result<u64> {
    let res = sqlx::query(PRUNE_DEAD_OWNERS_SQL)
        .bind(policy.owner_retention().as_secs().to_string())
        .execute(pool)
        .await
        .context("prune dead owners")?;
    Ok(res.rows_affected())
}

/// Every non-terminal row plus the ownership facts the decision needs.
/// Ages are computed by Postgres so one clock decides, not N pod clocks.
async fn read_ownership_candidates(pool: &PgPool) -> Result<Vec<JobOwnership>> {
    let rows = sqlx::query(OWNERSHIP_CANDIDATES_SQL)
        .fetch_all(pool)
        .await
        .context("read ownership candidates")?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let age_secs: Option<f64> = row.try_get("age_secs")?;
        let hb_secs: Option<f64> = row.try_get("owner_heartbeat_age_secs")?;
        out.push(JobOwnership {
            job_id: row.try_get("job_id")?,
            owner: row.try_get("owner")?,
            // Clock skew can make a heartbeat look like it is in the
            // future; clamping at zero reads that as "alive", which is the
            // conservative direction.
            owner_heartbeat_age: hb_secs.map(|s| Duration::from_secs_f64(s.max(0.0))),
            age: Duration::from_secs_f64(age_secs.unwrap_or(0.0).max(0.0)),
        });
    }
    Ok(out)
}

/// One recovery pass: read the candidates, decide each with
/// [`recovery_decision`], then fail only the rows the decision condemned.
async fn recover_orphans(
    pool: &PgPool,
    owner: &str,
    policy: &JobRecoveryPolicy,
    ttl: Duration,
) -> Result<RecoveryOutcome> {
    let candidates = read_ownership_candidates(pool).await?;
    let mut outcome = RecoveryOutcome::default();
    let mut condemned: HashMap<&'static str, (OrphanReason, Vec<String>)> = HashMap::new();
    for row in &candidates {
        match recovery_decision(row, owner, policy) {
            RecoveryDecision::Keep(KeepReason::SelfOwned) => outcome.kept_self += 1,
            RecoveryDecision::Keep(KeepReason::OwnerAlive) => outcome.kept_live_owner += 1,
            RecoveryDecision::Keep(KeepReason::OwnerlessWithinGrace) => outcome.kept_ownerless += 1,
            RecoveryDecision::Recover(reason) => condemned
                .entry(reason.label())
                .or_insert_with(|| (reason, Vec::new()))
                .1
                .push(row.job_id.clone()),
        }
    }
    for (reason, ids) in condemned.into_values() {
        let condemned = ids.len();
        let rows_affected = apply_recovery_update(pool, reason, &ids, policy, ttl).await?;
        if rows_affected as usize != condemned {
            tracing::debug!(
                reason = reason.label(),
                condemned,
                rows_affected,
                "recovery condemnation cancelled by a fresh status or liveness check"
            );
        }
        if rows_affected > 0 {
            tracing::warn!(
                reason = reason.label(),
                count = rows_affected,
                "recovered orphaned batch jobs"
            );
        }
        outcome.recovered += rows_affected as usize;
    }
    Ok(outcome)
}

async fn apply_recovery_update(
    pool: &PgPool,
    reason: OrphanReason,
    ids: &[String],
    policy: &JobRecoveryPolicy,
    ttl: Duration,
) -> Result<u64> {
    // The status and reason-specific liveness guards are deliberately checked
    // again here. A completion or renewed heartbeat between candidate read and
    // this write cancels condemnation; the fixed `ids` set means this guard
    // cannot condemn anything the Rust decision kept.
    let mut query = sqlx::query(reason.recovery_sql())
        .bind(reason.error_text())
        .bind(ttl.as_secs().to_string())
        .bind(reason.label())
        .bind(ids);
    if let Some(duration) = reason.guard_duration(policy) {
        query = query.bind(duration.as_secs().to_string());
    }
    let res = query
        .execute(pool)
        .await
        .context("ownership-scoped recovery UPDATE")?;
    Ok(res.rows_affected())
}

fn log_recovery(outcome: &RecoveryOutcome) {
    tracing::info!(
        recovered = outcome.recovered,
        kept_self = outcome.kept_self,
        kept_live_owner = outcome.kept_live_owner,
        kept_ownerless = outcome.kept_ownerless,
        "job recovery pass"
    );
}

/// Why a conditional terminal write matched no row: someone installed a
/// terminal state first, or the row is gone. Only ever runs on the losing
/// path, so the extra primary-key read costs a completion that already has
/// nothing to deliver — and it is the difference between reporting
/// `actual="cancelled"` and guessing.
async fn lost_conditional_write(
    pool: &PgPool,
    id: JobId,
    owner: &str,
    amend_recovered_error: bool,
) -> Result<CompletionOutcome> {
    let row = sqlx::query(TERMINAL_LOSER_STATUS_SQL)
        .bind(id.to_string())
        .bind(owner)
        .fetch_optional(pool)
        .await
        .context("read the status that won a terminal write")?;
    let Some(row) = row else {
        return Ok(CompletionOutcome::Vanished);
    };
    let raw: String = row.try_get("status")?;
    let owned_by_us: bool = row.try_get("owned_by_us")?;
    let recovered: bool = row.try_get("recovered")?;
    // A status this build does not know is not worth failing the caller's
    // reporting over; `Vanished`'s `gone` label is the honest "cannot say".
    match JobStatus::parse(&raw) {
        Some(status) => {
            if amend_recovered_error && status == JobStatus::Failed && owned_by_us && recovered {
                sqlx::query(AMEND_RECOVERED_ERROR_SQL)
                    .bind(id.to_string())
                    .bind(owner)
                    .bind(RECOVERED_COMPLETION_ERROR)
                    .execute(pool)
                    .await
                    .context("append discarded completion to recovered job error")?;
            }
            Ok(CompletionOutcome::Superseded {
                status,
                owned_by_us,
                recovered,
            })
        }
        None => {
            tracing::warn!(job_id = %id, status = %raw, "unknown job status");
            Ok(CompletionOutcome::Vanished)
        }
    }
}

async fn read_info(pool: &PgPool, id: JobId) -> Result<Option<JobInfo>> {
    let row = sqlx::query(
        "SELECT job_id, status, priority, submitted_at, started_at, ended_at,
                query, cost_json, error, tenant
         FROM siglake_query_jobs WHERE job_id = $1",
    )
    .bind(id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else { return Ok(None) };
    let status_str: String = row.try_get("status")?;
    let priority_str: String = row.try_get("priority")?;
    let cost_json: Option<serde_json::Value> = row.try_get("cost_json")?;
    let cost = cost_json
        .map(serde_json::from_value::<CostReport>)
        .transpose()
        .unwrap_or(None);
    Ok(Some(JobInfo {
        tenant: row.try_get("tenant")?,
        job_id: row.try_get("job_id")?,
        status: JobStatus::parse(&status_str).unwrap_or(JobStatus::Failed),
        priority: match priority_str.as_str() {
            "batch" => Priority::Batch,
            _ => Priority::Interactive,
        },
        submitted_at: row.try_get("submitted_at")?,
        started_at: row.try_get("started_at")?,
        ended_at: row.try_get("ended_at")?,
        query: row.try_get("query")?,
        cost,
        error: row.try_get("error")?,
    }))
}

async fn read_result(pool: &PgPool, id: JobId) -> Result<Option<JobResult>> {
    let row = sqlx::query("SELECT status, result_json FROM siglake_query_jobs WHERE job_id = $1")
        .bind(id.to_string())
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else { return Ok(None) };
    let status_str: String = row.try_get("status")?;
    let body_json: Option<serde_json::Value> = row.try_get("result_json")?;
    let body = body_json
        .map(serde_json::from_value::<RecordsResponse>)
        .transpose()
        .unwrap_or(None);
    Ok(Some(JobResult {
        status: JobStatus::parse(&status_str).unwrap_or(JobStatus::Failed),
        body,
    }))
}

/// What the store did with one run's terminal verdict.
///
/// Every terminal write is conditional on `status IN ('pending', 'running')`,
/// so a run's verdict can lose — to a cancellation a client persisted through
/// another replica, or to recovery condemning the row. The caller must report
/// what the store INSTALLED, never what the run computed: a `succeeded` metric
/// and audit row over a job whose row reads `cancelled` is the fleet
/// disagreeing with itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionOutcome {
    /// The conditional write landed, and the row now reads `status`.
    ///
    /// That is not always what was attempted: a success body over
    /// [`MAX_RESULT_BYTES`] is installed as `failed`, so a run whose query
    /// finished can still have `failed` recorded for it.
    Applied { status: JobStatus },
    /// Someone else got there first and `status` is what they installed. This
    /// run's result is discarded and it must claim no outcome of its own.
    Superseded {
        status: JobStatus,
        /// The row still belongs to this process incarnation.
        owned_by_us: bool,
        /// Recovery, rather than a client or another completion, installed
        /// the terminal state.
        recovered: bool,
    },
    /// The row is gone — the TTL sweep reached it, or it never existed.
    Vanished,
}

impl CompletionOutcome {
    /// The status the store installed for this run, or `None` when this run
    /// installed nothing.
    pub fn applied(self) -> Option<JobStatus> {
        match self {
            Self::Applied { status } => Some(status),
            Self::Superseded { .. } | Self::Vanished => None,
        }
    }

    /// What the job row reads now, as a bounded metric label. `gone` is the
    /// TTL sweep having deleted the row before the run finished.
    pub fn actual_label(self) -> &'static str {
        match self {
            Self::Applied { status } | Self::Superseded { status, .. } => status.label(),
            Self::Vanished => "gone",
        }
    }
}

/// Returned by [`JobStore::result`]. The body is `Some` only when the
/// job has succeeded; for in-flight or failed jobs the caller gets
/// just the status so they know whether to keep polling.
#[derive(Debug, Clone)]
pub struct JobResult {
    pub status: JobStatus,
    pub body: Option<RecordsResponse>,
}

impl Default for JobStore {
    fn default() -> Self {
        Self::new(2, Duration::from_secs(86_400))
    }
}

/// Per-Postgres-row result-body inline cap. Anything larger gets
/// rejected with a synthetic `failed` status so we don't blow up
/// the Postgres TOAST tablespace with huge JSON. Phase 4.8 may
/// overflow to S3 for larger results.
const MAX_RESULT_BYTES: usize = 1_048_576; // 1 MiB

/// The inline-cap verdict for one success body: `None` to store it, `Some`
/// with the failure text to install instead.
///
/// Split out of [`install_succeeded`] so the decision is testable without a
/// database, and measured through [`crate::format::serialized_json_len`] so
/// asking how big the body is does not build a second copy of it. The count is
/// byte-identical to the `Value::to_string().len()` this replaced (pinned by
/// `format::tests::serialized_json_len_matches_*`), so the cap moves for
/// nobody.
///
/// A body that cannot be measured is refused rather than bound blind: a
/// `Value` always serializes, so this is unreachable from `install_succeeded`,
/// but the alternative — treating "unknown size" as "under the cap" — is the
/// one that puts an unbounded body in a Postgres row.
fn result_cap_breach<T: Serialize + ?Sized>(body: &T) -> Option<String> {
    match crate::format::serialized_json_len(body) {
        Some(sz) if sz <= MAX_RESULT_BYTES => None,
        Some(sz) => Some(format!(
            "result size {sz} bytes exceeds Postgres cap {MAX_RESULT_BYTES}"
        )),
        None => Some(format!(
            "result size could not be measured against Postgres cap {MAX_RESULT_BYTES}"
        )),
    }
}

/// Ownership statements, hoisted to consts so
/// `ownership_tests::every_ownership_statement_parses_as_postgres` can gate
/// their syntax. Nothing else in the workspace exercises the Postgres
/// backend without a live database.
const OWNER_HEARTBEAT_SQL: &str =
    "INSERT INTO siglake_query_job_owners (owner_id, started_at, heartbeat_at)
     VALUES ($1, NOW(), NOW())
     ON CONFLICT (owner_id) DO UPDATE SET heartbeat_at = NOW()";

const RELEASE_OWNER_SQL: &str = "DELETE FROM siglake_query_job_owners WHERE owner_id = $1";

const PRUNE_DEAD_OWNERS_SQL: &str = "DELETE FROM siglake_query_job_owners o
     WHERE o.heartbeat_at < NOW() - ($1 || ' seconds')::INTERVAL
       AND NOT EXISTS (
             SELECT 1 FROM siglake_query_jobs j
             WHERE j.owner = o.owner_id
               AND j.status IN ('pending', 'running'))";

const GC_EXPIRED_SQL: &str = "DELETE FROM siglake_query_jobs
     WHERE expires_at IS NOT NULL AND expires_at < NOW()";

const OWNERSHIP_CANDIDATES_SQL: &str = "SELECT j.job_id,
            j.owner,
            EXTRACT(EPOCH FROM (NOW() - j.submitted_at))::DOUBLE PRECISION AS age_secs,
            EXTRACT(EPOCH FROM (NOW() - o.heartbeat_at))::DOUBLE PRECISION
                AS owner_heartbeat_age_secs
     FROM siglake_query_jobs j
     LEFT JOIN siglake_query_job_owners o ON o.owner_id = j.owner
     WHERE j.status IN ('pending', 'running')";

/// The cross-replica cancellation read. Keyed by primary key and restricted to
/// the ids this process holds abort handles for, so its cost tracks this pod's
/// in-flight batch jobs rather than the shared table.
const CANCELLED_AMONG_SQL: &str = "SELECT job_id FROM siglake_query_jobs
     WHERE job_id = ANY($1) AND status = 'cancelled'";

/// Publish `running` before the work runs. `COALESCE` because `started_at` is
/// the moment execution began: a republication must not move it forward.
const SET_RUNNING_SQL: &str = "UPDATE siglake_query_jobs
     SET status = 'running', started_at = COALESCE(started_at, NOW())
     WHERE job_id = $1 AND status IN ('pending', 'running')";

/// Attach the estimate to a job that is still executing, without touching its
/// status or `started_at`. `RETURNING status` so the caller reports the row the
/// write landed on rather than the state it assumed.
const RECORD_COST_SQL: &str = "UPDATE siglake_query_jobs
     SET cost_json = $1
     WHERE job_id = $2 AND status IN ('pending', 'running')
     RETURNING status";

/// Read on the losing side of a conditional terminal write, to report the
/// status that beat it instead of guessing at one.
const TERMINAL_LOSER_STATUS_SQL: &str = "SELECT status,
            COALESCE(owner = $2, FALSE) AS owned_by_us,
            recovered_at IS NOT NULL AS recovered
     FROM siglake_query_jobs WHERE job_id = $1";

const RECOVER_OWNER_LEASE_EXPIRED_SQL: &str = "UPDATE siglake_query_jobs j
     SET status = 'failed', ended_at = NOW(), error = COALESCE(error, $1),
         expires_at = NOW() + ($2 || ' seconds')::INTERVAL,
         recovered_at = NOW(), recovery_reason = $3
     WHERE j.job_id = ANY($4) AND j.status IN ('pending', 'running')
       AND EXISTS (
           SELECT 1 FROM siglake_query_job_owners o
           WHERE o.owner_id = j.owner
             AND o.heartbeat_at < NOW() - ($5 || ' seconds')::INTERVAL)";

const RECOVER_OWNER_UNREGISTERED_SQL: &str = "UPDATE siglake_query_jobs j
     SET status = 'failed', ended_at = NOW(), error = COALESCE(error, $1),
         expires_at = NOW() + ($2 || ' seconds')::INTERVAL,
         recovered_at = NOW(), recovery_reason = $3
     WHERE j.job_id = ANY($4) AND j.status IN ('pending', 'running')
       AND j.owner IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM siglake_query_job_owners o WHERE o.owner_id = j.owner)";

const RECOVER_OWNERLESS_BEYOND_GRACE_SQL: &str = "UPDATE siglake_query_jobs j
     SET status = 'failed', ended_at = NOW(), error = COALESCE(error, $1),
         expires_at = NOW() + ($2 || ' seconds')::INTERVAL,
         recovered_at = NOW(), recovery_reason = $3
     WHERE j.job_id = ANY($4) AND j.status IN ('pending', 'running')
       AND j.owner IS NULL
       AND j.submitted_at < NOW() - ($5 || ' seconds')::INTERVAL";

const RECOVERED_COMPLETION_ERROR: &str = "a result for this job was computed after recovery declared its executor gone; it was discarded, so resubmit the query";

const AMEND_RECOVERED_ERROR_SQL: &str = "UPDATE siglake_query_jobs
     SET error = COALESCE(error || ' | ', '') || $3
     WHERE job_id = $1 AND owner = $2 AND status = 'failed'
       AND recovered_at IS NOT NULL
       AND (error IS NULL OR POSITION($3 IN error) = 0)";

/// Per-statement DDL for `JobStore::new_postgres`. One `sqlx::query()`
/// call per element; Postgres rejects multi-statement payloads when
/// sent as a prepared statement.
const SCHEMA_SQL_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS siglake_query_jobs (
        job_id       TEXT PRIMARY KEY,
        status       TEXT NOT NULL,
        priority     TEXT NOT NULL,
        submitted_at TIMESTAMPTZ NOT NULL,
        started_at   TIMESTAMPTZ,
        ended_at     TIMESTAMPTZ,
        query        TEXT NOT NULL,
        cost_json    JSONB,
        error        TEXT,
        result_json  JSONB,
        expires_at   TIMESTAMPTZ,
        tenant       TEXT,
        owner        TEXT,
        recovered_at TIMESTAMPTZ,
        recovery_reason TEXT
    )",
    // Existing installs: the column post-dates the table. NULL is the default
    // context, which is what rows written before this change belonged to.
    "ALTER TABLE siglake_query_jobs ADD COLUMN IF NOT EXISTS tenant TEXT",
    // Execution ownership. NULL on rows written before ownership existed;
    // `JobRecoveryPolicy::ownerless_grace` says what recovery does with those.
    // This is not an authorization key -- `tenant` is.
    "ALTER TABLE siglake_query_jobs ADD COLUMN IF NOT EXISTS owner TEXT",
    "ALTER TABLE siglake_query_jobs ADD COLUMN IF NOT EXISTS recovered_at TIMESTAMPTZ",
    "ALTER TABLE siglake_query_jobs ADD COLUMN IF NOT EXISTS recovery_reason TEXT",
    // One row per query-server process incarnation, heartbeated while it
    // lives. Absence-or-staleness of a row here is the only evidence that
    // licenses recovery of another owner's job.
    "CREATE TABLE IF NOT EXISTS siglake_query_job_owners (
        owner_id     TEXT PRIMARY KEY,
        started_at   TIMESTAMPTZ NOT NULL,
        heartbeat_at TIMESTAMPTZ NOT NULL
    )",
    "CREATE INDEX IF NOT EXISTS idx_siglake_query_jobs_status
        ON siglake_query_jobs(status)",
    "CREATE INDEX IF NOT EXISTS idx_siglake_query_jobs_expires_at
        ON siglake_query_jobs(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_siglake_query_jobs_owner
        ON siglake_query_jobs(owner)",
];

#[cfg(test)]
mod metrics_tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn startup_registers_the_unreconciled_gauge_at_zero() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, super::initialize_metrics);

        let value = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(key, _, _, value)| {
                (key.key().name() == "siglake_query_jobs_unreconciled").then_some(value)
            });
        assert!(
            matches!(value, Some(DebugValue::Gauge(value)) if value.into_inner() == 0.0),
            "siglake_query_jobs_unreconciled must exist at 0 on a fresh query server"
        );
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    const SELF: &str = "pod-a/0197c0de-0000-7000-8000-000000000001";
    const PEER: &str = "pod-b/0197c0de-0000-7000-8000-000000000002";

    fn policy() -> JobRecoveryPolicy {
        JobRecoveryPolicy {
            owner_lease: Duration::from_secs(120),
            ownerless_grace: Duration::from_secs(86_400),
            cancel_poll: Duration::from_secs(2),
        }
    }

    fn row(owner: Option<&str>, heartbeat_age: Option<u64>, age: u64) -> JobOwnership {
        JobOwnership {
            job_id: "job".into(),
            owner: owner.map(str::to_string),
            owner_heartbeat_age: heartbeat_age.map(Duration::from_secs),
            age: Duration::from_secs(age),
        }
    }

    fn decide(row: &JobOwnership) -> RecoveryDecision {
        recovery_decision(row, SELF, &policy())
    }

    #[test]
    fn live_peer_owner_is_left_alone() {
        // The bug this task fixes: replica B starting up must not touch a
        // job replica A is running.
        assert_eq!(
            decide(&row(Some(PEER), Some(5), 30)),
            RecoveryDecision::Keep(KeepReason::OwnerAlive)
        );
    }

    #[test]
    fn own_rows_are_never_recovered_even_with_a_stalled_heartbeat() {
        // A stalled heartbeat writer is not evidence that this process
        // stopped executing; the future is in our own runtime.
        assert_eq!(
            decide(&row(Some(SELF), Some(10_000), 10_000)),
            RecoveryDecision::Keep(KeepReason::SelfOwned)
        );
        assert_eq!(
            decide(&row(Some(SELF), None, 10_000)),
            RecoveryDecision::Keep(KeepReason::SelfOwned)
        );
    }

    #[test]
    fn heartbeat_exactly_at_the_lease_boundary_still_counts_as_alive() {
        assert_eq!(
            decide(&row(Some(PEER), Some(120), 200)),
            RecoveryDecision::Keep(KeepReason::OwnerAlive)
        );
        assert_eq!(
            decide(&row(Some(PEER), Some(121), 200)),
            RecoveryDecision::Recover(OrphanReason::OwnerLeaseExpired)
        );
    }

    #[test]
    fn unregistered_owner_is_an_orphan() {
        assert_eq!(
            decide(&row(Some(PEER), None, 30)),
            RecoveryDecision::Recover(OrphanReason::OwnerUnregistered)
        );
    }

    #[test]
    fn ownerless_legacy_rows_are_kept_until_the_grace_period_passes() {
        assert_eq!(
            decide(&row(None, None, 60)),
            RecoveryDecision::Keep(KeepReason::OwnerlessWithinGrace)
        );
        assert_eq!(
            decide(&row(None, None, 86_400)),
            RecoveryDecision::Keep(KeepReason::OwnerlessWithinGrace)
        );
        assert_eq!(
            decide(&row(None, None, 86_401)),
            RecoveryDecision::Recover(OrphanReason::OwnerlessBeyondGrace)
        );
    }

    #[test]
    fn ownerless_grace_does_not_leak_into_owned_rows() {
        // An owned row's age is irrelevant: only its owner's liveness
        // decides. A four-day-old job whose owner still heartbeats is a
        // long-running job, not an orphan.
        assert_eq!(
            decide(&row(Some(PEER), Some(1), 4 * 86_400)),
            RecoveryDecision::Keep(KeepReason::OwnerAlive)
        );
    }

    #[test]
    fn heartbeat_interval_is_a_third_of_the_lease_with_a_one_second_floor() {
        assert_eq!(policy().heartbeat_interval(), Duration::from_secs(40));
        let tight = JobRecoveryPolicy {
            owner_lease: Duration::from_secs(1),
            ..policy()
        };
        assert_eq!(tight.heartbeat_interval(), Duration::from_secs(1));
    }

    #[test]
    fn owner_retention_outlives_the_lease_by_far() {
        assert!(policy().owner_retention() >= policy().owner_lease * 20);
        assert!(policy().owner_retention() >= Duration::from_secs(3_600));
    }

    #[test]
    fn owner_ids_are_unique_per_incarnation() {
        // A restarted pod keeps its HOSTNAME, so the id must carry more
        // than the hostname or the new process would inherit ownership of
        // the dead one's rows and never recover them.
        let a = owner_id_from(Some("siglake-query-0"));
        let b = owner_id_from(Some("siglake-query-0"));
        assert_ne!(a, b);
        assert!(a.starts_with("siglake-query-0/"), "{a}");
        // No HOSTNAME (bare process, some CRIs) still yields a usable id.
        for missing in [None, Some("")] {
            let id = owner_id_from(missing);
            assert!(id.starts_with("unknown-host/"), "{id}");
        }
    }

    #[test]
    fn every_orphan_reason_has_a_distinct_client_error() {
        let reasons = [
            OrphanReason::OwnerLeaseExpired,
            OrphanReason::OwnerUnregistered,
            OrphanReason::OwnerlessBeyondGrace,
        ];
        let texts: Vec<&str> = reasons.iter().map(|r| r.error_text()).collect();
        let labels: Vec<&str> = reasons.iter().map(|r| r.label()).collect();
        for set in [&texts, &labels] {
            let mut uniq = set.clone();
            uniq.sort_unstable();
            uniq.dedup();
            assert_eq!(uniq.len(), set.len(), "reason strings must be distinct");
        }
    }

    #[test]
    fn every_orphan_reason_has_one_matching_write_guard() {
        let policy = policy();
        let cases = [
            (
                OrphanReason::OwnerLeaseExpired,
                RECOVER_OWNER_LEASE_EXPIRED_SQL,
                Some(policy.owner_lease),
            ),
            (
                OrphanReason::OwnerUnregistered,
                RECOVER_OWNER_UNREGISTERED_SQL,
                None,
            ),
            (
                OrphanReason::OwnerlessBeyondGrace,
                RECOVER_OWNERLESS_BEYOND_GRACE_SQL,
                Some(policy.ownerless_grace),
            ),
        ];
        for (reason, sql, duration) in cases {
            assert_eq!(reason.recovery_sql(), sql);
            assert_eq!(reason.guard_duration(&policy), duration);
            assert!(sql.contains("status IN ('pending', 'running')"));
        }
        assert!(
            RECOVER_OWNER_LEASE_EXPIRED_SQL.contains("heartbeat_at < NOW()"),
            "the SQL must keep the pure decision's strict lease boundary"
        );
    }

    /// The Postgres backend has no coverage without a live database (the
    /// two-store regression in `tests/jobs_postgres_ownership.rs` is
    /// `#[ignore]`d), and a syntax error in it is a silent runtime failure
    /// on the exact path that used to destroy results. Parse every
    /// statement under the Postgres dialect so a typo is a compile-time-ish
    /// failure instead.
    #[test]
    fn every_ownership_statement_parses_as_postgres() {
        use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let mut statements: Vec<&str> = SCHEMA_SQL_STATEMENTS.to_vec();
        statements.extend([
            OWNER_HEARTBEAT_SQL,
            RELEASE_OWNER_SQL,
            PRUNE_DEAD_OWNERS_SQL,
            GC_EXPIRED_SQL,
            OWNERSHIP_CANDIDATES_SQL,
            RECOVER_OWNER_LEASE_EXPIRED_SQL,
            RECOVER_OWNER_UNREGISTERED_SQL,
            RECOVER_OWNERLESS_BEYOND_GRACE_SQL,
            AMEND_RECOVERED_ERROR_SQL,
            CANCELLED_AMONG_SQL,
            TERMINAL_LOSER_STATUS_SQL,
            SET_RUNNING_SQL,
            RECORD_COST_SQL,
        ]);
        for sql in statements {
            let parsed = Parser::parse_sql(&PostgreSqlDialect {}, sql)
                .unwrap_or_else(|e| panic!("does not parse as Postgres: {e}\n{sql}"));
            assert_eq!(parsed.len(), 1, "one statement per execute(): {sql}");
        }
    }

    /// The lifecycle publications are conditional writes like every terminal
    /// one, and `started_at` is set once. Without the guard, publishing
    /// progress from a run that has already been cancelled or condemned puts a
    /// terminal row back into `running`; without the `COALESCE`, a
    /// republication reports a start time later than the work's own.
    #[test]
    fn the_lifecycle_writes_are_guarded_and_do_not_move_the_start() {
        for sql in [SET_RUNNING_SQL, RECORD_COST_SQL] {
            assert!(
                sql.contains("status IN ('pending', 'running')"),
                "a lifecycle write may not revive a terminal job: {sql}"
            );
        }
        assert!(SET_RUNNING_SQL.contains("started_at = COALESCE(started_at, NOW())"));
        assert!(
            !RECORD_COST_SQL.contains("started_at") && !RECORD_COST_SQL.contains("status ="),
            "the estimate must not restate the job's lifecycle: {RECORD_COST_SQL}"
        );
    }

    #[tokio::test]
    async fn in_memory_recovery_is_a_no_op() {
        let store = JobStore::new(1, Duration::from_secs(60));
        assert_eq!(
            store.recover_orphaned_jobs().await.unwrap(),
            RecoveryOutcome::default()
        );
    }

    #[tokio::test]
    async fn shutdown_abandons_registered_batch_futures() {
        struct Dropped(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                if let Some(done) = self.0.take() {
                    let _ = done.send(());
                }
            }
        }

        let store = JobStore::new(1, Duration::from_secs(60));
        let id = JobId::new();
        let (abort, registration) = futures::future::AbortHandle::new_pair();
        store.register_abort(id, abort);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        store.batch_runtime().spawn(async move {
            let _dropped = Dropped(Some(done_tx));
            let _ = started_tx.send(());
            let _ =
                futures::future::Abortable::new(std::future::pending::<()>(), registration).await;
        });
        started_rx.await.expect("batch future started");

        store.shutdown().await.expect("shutdown store");
        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("abandoned batch future was dropped")
            .expect("batch future reports drop");
        // The lifecycle operation is idempotent, which keeps error paths from
        // turning a second cleanup attempt into a re-registration.
        store.shutdown().await.expect("repeat shutdown");
    }
}

#[cfg(test)]
mod gc_tests {
    use super::*;

    async fn eventually(mut condition: impl FnMut() -> bool, what: &str) {
        for _ in 0..400 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{what}");
    }

    #[test]
    fn sweep_cadence_tracks_short_ttls_and_caps_long_ones() {
        assert_eq!(gc_interval(Duration::ZERO), MIN_GC_INTERVAL);
        assert_eq!(
            gc_interval(Duration::from_millis(250)),
            Duration::from_millis(250)
        );
        assert_eq!(gc_interval(Duration::from_secs(86_400)), MAX_GC_INTERVAL);
    }

    #[tokio::test]
    async fn scheduled_sweep_expires_only_terminal_rows_past_their_ttl() {
        let ttl = Duration::from_millis(80);
        let store = JobStore::new(1, ttl);
        let expired = store
            .submit("SELECT 1".into(), Priority::Batch, None)
            .await
            .unwrap();
        store
            .finish_failed(expired, "done".into(), false)
            .await
            .unwrap();
        let active = store
            .submit("SELECT pg_sleep(1)".into(), Priority::Batch, None)
            .await
            .unwrap();

        assert_eq!(
            store.gc().await,
            0,
            "an unexpired terminal row survives a sweep"
        );
        assert!(store.info(expired).await.unwrap().is_some());

        eventually(
            || {
                let Backend::InMemory(map) = &store.backend else {
                    unreachable!()
                };
                map.try_read()
                    .is_ok_and(|rows| !rows.contains_key(&expired))
            },
            "the scheduled sweep did not evict the expired terminal row",
        )
        .await;
        assert!(
            store.info(active).await.unwrap().is_some(),
            "a non-terminal row has no expiry and must survive every sweep"
        );
    }

    #[tokio::test]
    async fn dropping_the_store_stops_its_sweep_and_releases_the_backend() {
        let store = JobStore::new(1, Duration::from_secs(60));
        let backend = match &store.backend {
            Backend::InMemory(map) => Arc::downgrade(map),
            Backend::Postgres(_) => unreachable!(),
        };
        drop(store);

        eventually(
            || backend.upgrade().is_none(),
            "the GC task retained the backend after its owning store was dropped",
        )
        .await;
    }
}

/// Cross-replica cancellation: two stores over one job table, each with its
/// own abort map — the shape that makes this hard, without a database.
#[cfg(test)]
mod cancellation_tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(600);

    fn cost() -> CostReport {
        CostReport {
            files_to_scan: Some(1),
            files_considered: Some(1),
            estimated_bytes_scanned: 1,
            estimated_rows_processed: 1,
            estimated_runtime_seconds: 0.1,
            complexity_class: crate::cost::ComplexityClass::Small,
            warnings: vec![],
            exact: true,
        }
    }

    fn records() -> RecordsResponse {
        RecordsResponse {
            columns: vec!["n".into()],
            row_count: 1,
            rows: serde_json::json!([{ "n": 1 }]),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        }
    }

    /// Two stores over one table: A executes, B serves the `DELETE`.
    async fn two_replicas() -> (JobStore, JobStore, JobId) {
        let a = JobStore::new(1, TTL);
        let b = JobStore::new_peer_of(&a, 1, TTL).expect("in-memory peer");
        assert_ne!(a.owner_id(), b.owner_id(), "owners are per incarnation");
        let id = a
            .submit("SELECT 1".into(), Priority::Batch, None)
            .await
            .expect("submit on A");
        a.set_running(id).await.expect("A -> running");
        a.record_cost(id, cost()).await.expect("A publishes cost");
        (a, b, id)
    }

    /// Wait for a condition the abort has to be *polled* to produce. The
    /// number is a generous ceiling on scheduling, not a timing assertion:
    /// what is being pinned is that the sweep is sufficient, not how fast the
    /// worker gets round to it.
    async fn eventually(mut done: impl FnMut() -> bool, what: &str) {
        for _ in 0..2_000 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{what}");
    }

    /// THE DEFECT THIS GUARDS. `cancel()` wrote `status = 'cancelled'` and
    /// aborted a handle in its own process's map. With the shared store, the
    /// replica serving `DELETE /api/v1/jobs/<id>` is usually not the one
    /// executing the job, so its abort map was empty and the `202` was a
    /// promise nobody kept: the executor kept scanning, kept its admission
    /// reservation, and only the row changed.
    #[tokio::test]
    async fn a_cancellation_persisted_by_a_peer_stops_the_owner() {
        let (a, b, id) = two_replicas().await;

        // Stand in for the batch future exactly where it matters: it owns a
        // `CancelOnDrop` (the storage-scan kill switch `run_batch_query` puts
        // in the session config) and an admission-reservation-shaped guard.
        // Dropping the future is what releases both -- the status write
        // releases neither.
        let cancel = siglake_storage::QueryCancel::new();
        let guard = siglake_storage::CancelOnDrop(cancel.clone());
        let (abort, registration) = futures::future::AbortHandle::new_pair();
        a.register_abort(id, abort);
        a.batch_runtime().spawn(async move {
            let _scan_guard = guard;
            let _ =
                futures::future::Abortable::new(std::future::pending::<()>(), registration).await;
        });

        assert!(b.cancel(id).await.unwrap(), "B persists the cancellation");
        assert_eq!(
            b.info(id)
                .await
                .expect("job store")
                .expect("row is visible from B")
                .status,
            JobStatus::Cancelled
        );
        assert!(
            !cancel.is_cancelled(),
            "the status write alone must not be mistaken for the work stopping"
        );

        assert_eq!(
            a.propagate_cancellations().await.expect("A's sweep"),
            vec![id],
            "A must observe the peer's cancellation for the job it is executing"
        );
        eventually(
            || cancel.is_cancelled(),
            "A's future was not dropped, so its storage scans were never cancelled",
        )
        .await;

        // Idempotent: the handle is gone, so a second sweep finds nothing and
        // a late `clear_abort` from the future's own exit path is a no-op.
        assert!(a
            .propagate_cancellations()
            .await
            .expect("second sweep")
            .is_empty());
        a.clear_abort(id);
        assert_eq!(
            a.info(id).await.unwrap().unwrap().status,
            JobStatus::Cancelled
        );
    }

    /// The sweep is scoped to what this process executes: it reads only the
    /// ids it holds abort handles for, so a peer's in-flight job is neither
    /// aborted nor even queried.
    #[tokio::test]
    async fn the_sweep_ignores_jobs_this_process_does_not_execute() {
        let (a, b, id) = two_replicas().await;
        let peer_job = b
            .submit("SELECT 2".into(), Priority::Batch, None)
            .await
            .unwrap();
        let (abort, _registration) = futures::future::AbortHandle::new_pair();
        b.register_abort(peer_job, abort);
        assert!(b.cancel(peer_job).await.unwrap());

        assert!(
            a.propagate_cancellations().await.unwrap().is_empty(),
            "A swept a job it is not executing"
        );
        assert_eq!(
            a.info(id).await.unwrap().unwrap().status,
            JobStatus::Running
        );
    }

    /// A completion that loses the race to a cancellation is refused, and is
    /// TOLD it was refused. Without the second half the executor reports
    /// `succeeded` over a row that reads `cancelled` -- the fleet
    /// disagreeing with itself -- which is what `sql.rs` now gates on.
    #[tokio::test]
    async fn a_completion_that_loses_to_a_cancellation_is_refused() {
        let (a, b, id) = two_replicas().await;
        assert!(b.cancel(id).await.unwrap());

        // Each loser is told WHICH state beat it, so the executor can report
        // the store's verdict without guessing at one.
        let superseded = CompletionOutcome::Superseded {
            status: JobStatus::Cancelled,
            owned_by_us: true,
            recovered: false,
        };
        assert_eq!(
            a.finish_succeeded(id, records()).await.unwrap(),
            superseded,
            "a cancelled row must refuse a late success"
        );
        assert_eq!(
            a.finish_failed(id, "boom".into(), false).await.unwrap(),
            superseded,
            "a cancelled row must refuse a late failure"
        );
        assert_eq!(
            a.finish_failed(id, "slow".into(), true).await.unwrap(),
            superseded,
            "a cancelled row must refuse a late timeout"
        );
        assert_eq!(superseded.applied(), None);
        assert_eq!(superseded.actual_label(), "cancelled");
        assert_eq!(
            a.set_running(id).await.unwrap(),
            superseded,
            "the running transition reports the same terminal winner"
        );
        assert_eq!(
            a.record_cost(id, cost()).await.unwrap(),
            superseded,
            "a late estimate must not resurrect a cancelled job either"
        );
        assert_eq!(
            b.finish_succeeded(id, records()).await.unwrap(),
            CompletionOutcome::Superseded {
                status: JobStatus::Cancelled,
                owned_by_us: false,
                recovered: false,
            },
            "a different incarnation must not claim ownership of the row"
        );
        assert_eq!(
            a.finish_succeeded(JobId::new(), records()).await.unwrap(),
            CompletionOutcome::Vanished
        );

        let row = a.info(id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Cancelled);
        assert_eq!(row.error.as_deref(), Some("cancelled by client"));
        let result = a.result(id).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Cancelled);
        assert!(
            result.body.is_none(),
            "the discarded result must not be readable"
        );
    }

    /// `started_at` is the moment execution began, and the running transition
    /// is now published before the work rather than after it — so a second
    /// publication must leave the first timestamp alone, and the estimate
    /// published mid-run must not disturb it at all.
    #[tokio::test]
    async fn republishing_the_lifecycle_keeps_the_first_start_timestamp() {
        let (a, _b, id) = two_replicas().await;
        let first = a.info(id).await.unwrap().unwrap();
        assert_eq!(first.status, JobStatus::Running);
        let started_at = first.started_at.expect("running row records its start");
        assert!(first.cost.is_some(), "planning published its estimate");

        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(
            a.set_running(id).await.unwrap(),
            CompletionOutcome::Applied {
                status: JobStatus::Running
            }
        );
        assert_eq!(
            a.record_cost(id, cost()).await.unwrap(),
            CompletionOutcome::Applied {
                status: JobStatus::Running
            },
            "the estimate lands on the row it is executing for"
        );
        let again = a.info(id).await.unwrap().unwrap();
        assert_eq!(again.started_at, Some(started_at), "the start moved");
        assert_eq!(again.ended_at, None);
    }

    /// The other direction of the same race: a completion that lands first
    /// wins, and the `DELETE` that follows it answers 409 rather than
    /// overwriting a delivered result.
    #[tokio::test]
    async fn a_completion_that_wins_the_race_keeps_its_result() {
        let (a, b, id) = two_replicas().await;
        assert_eq!(
            a.finish_succeeded(id, records()).await.unwrap(),
            CompletionOutcome::Applied {
                status: JobStatus::Succeeded
            }
        );

        assert!(
            !b.cancel(id).await.unwrap(),
            "B must not cancel a job that already succeeded"
        );
        let result = b.result(id).await.unwrap().unwrap();
        assert_eq!(result.status, JobStatus::Succeeded);
        assert_eq!(
            result.body.expect("result body").rows,
            serde_json::json!([{ "n": 1 }])
        );
    }
}

/// Owner-local reconciliation of runs that finished without a persisted
/// terminal state.
///
/// THE DEFECT THESE GUARD. Ownership-scoped recovery never condemns a row this
/// process owns — a stalled heartbeat is not evidence that we stopped
/// executing — and the TTL sweep only deletes rows with an `expires_at`, which
/// only a terminal write sets. So a run whose execution ENDED while the store
/// was unreachable left a `running` row that no sweep would ever resolve while
/// this replica lived, and a client polling it forever.
#[cfg(test)]
mod reconciliation_tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(600);

    async fn running_job(store: &JobStore) -> JobId {
        let id = store
            .submit("SELECT 1".into(), Priority::Batch, None)
            .await
            .expect("submit");
        store.set_running(id).await.expect("-> running");
        id
    }

    /// The rule, as a pure function: a lost SUCCESS cannot be reinstalled —
    /// the body went with the failed write and re-running the query behind the
    /// client's back is not an option — so it becomes an explicit failure that
    /// asks for a resubmit. A lost failure or timeout is installed as itself.
    #[test]
    fn a_lost_success_is_reconciled_as_a_resubmit_not_as_a_success() {
        let lost = reconciled_terminal(JobStatus::Succeeded);
        assert_eq!(lost.status, JobStatus::Failed);
        assert!(
            lost.error.contains("resubmit"),
            "a client whose result was discarded must be told to resubmit: {}",
            lost.error
        );
        assert_eq!(
            reconciled_terminal(JobStatus::Timeout).status,
            JobStatus::Timeout
        );
        assert_eq!(
            reconciled_terminal(JobStatus::Failed).status,
            JobStatus::Failed
        );
        // Defensive: nothing a run can compute, and still not a success.
        assert_eq!(
            reconciled_terminal(JobStatus::Cancelled).status,
            JobStatus::Failed
        );
    }

    /// The whole point: a parked id reaches a terminal state, with no result
    /// body behind it, and stops being parked.
    #[tokio::test]
    async fn a_parked_finished_job_reaches_a_terminal_state() {
        let store = JobStore::new(1, TTL);
        let id = running_job(&store).await;
        assert_eq!(
            store.park_finished_unpersisted(id, JobStatus::Succeeded),
            ParkOutcome::Tracked
        );

        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome {
                installed: 1,
                ..Default::default()
            }
        );
        let row = store.info(id).await.expect("job store").expect("row");
        assert_eq!(row.status, JobStatus::Failed);
        assert!(row.ended_at.is_some(), "a reconciled row records its end");
        assert!(row
            .error
            .expect("a reconciled row says why")
            .contains("resubmit"));
        assert!(
            store
                .result(id)
                .await
                .expect("job store")
                .expect("row")
                .body
                .is_none(),
            "reconciliation served a result body it never had"
        );

        // Settled, so the next pass has nothing to do at all.
        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome::default()
        );
    }

    /// The ambiguous-acknowledgement case as the store sees it: a terminal
    /// state is already installed, so the conditional write matches nothing and
    /// reconciliation leaves the row — and its error text — exactly as it
    /// found them.
    #[tokio::test]
    async fn reconciliation_preserves_a_terminal_state_that_already_landed() {
        let store = JobStore::new(1, TTL);
        let id = running_job(&store).await;
        store
            .finish_failed(id, "the run's own error".into(), false)
            .await
            .expect("terminal write");
        assert_eq!(
            store.park_finished_unpersisted(id, JobStatus::Failed),
            ParkOutcome::Tracked
        );

        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome {
                preserved: 1,
                ..Default::default()
            }
        );
        let row = store.info(id).await.expect("job store").expect("row");
        assert_eq!(row.status, JobStatus::Failed);
        assert_eq!(
            row.error.as_deref(),
            Some("the run's own error"),
            "reconciliation overwrote a terminal row it should have preserved"
        );
    }

    /// A cancellation is a terminal state like any other: reconciliation must
    /// not turn a job the client cancelled into a failure of ours.
    #[tokio::test]
    async fn reconciliation_does_not_reopen_a_cancelled_job() {
        let store = JobStore::new(1, TTL);
        let id = running_job(&store).await;
        assert!(store.cancel(id).await.unwrap());
        store.park_finished_unpersisted(id, JobStatus::Succeeded);

        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome {
                preserved: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            store
                .info(id)
                .await
                .expect("job store")
                .expect("row")
                .status,
            JobStatus::Cancelled
        );
    }

    /// Reconciliation is keyed by what THIS process ran, not by what the table
    /// says is non-terminal: a sibling replica's live job is not touched, and
    /// not even considered.
    #[tokio::test]
    async fn a_live_siblings_running_job_is_untouched() {
        let a = JobStore::new(1, TTL);
        let b = JobStore::new_peer_of(&a, 1, TTL).expect("in-memory peer");
        let ours = running_job(&a).await;
        let theirs = running_job(&b).await;
        a.park_finished_unpersisted(ours, JobStatus::Succeeded);

        // B knows nothing about A's finished run, so its own pass is a no-op —
        // this is what keeps a healthy replica from resolving another's work.
        assert_eq!(
            b.reconcile_finished_jobs().await,
            ReconcileOutcome::default()
        );
        assert_eq!(
            a.info(ours).await.expect("job store").expect("row").status,
            JobStatus::Running
        );

        assert_eq!(
            a.reconcile_finished_jobs().await,
            ReconcileOutcome {
                installed: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            a.info(ours).await.expect("job store").expect("row").status,
            JobStatus::Failed
        );
        assert_eq!(
            b.info(theirs)
                .await
                .expect("job store")
                .expect("row")
                .status,
            JobStatus::Running,
            "reconciliation condemned a live sibling's job"
        );
    }

    /// The TTL sweep having deleted the row (or a row that never existed)
    /// settles the id rather than retrying it forever.
    #[tokio::test]
    async fn a_row_that_is_gone_settles() {
        let store = JobStore::new(1, TTL);
        store.park_finished_unpersisted(JobId::new(), JobStatus::Failed);
        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome {
                gone: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            store.reconcile_finished_jobs().await,
            ReconcileOutcome::default(),
            "a vanished row was retried after it had been settled"
        );
    }

    /// Bookkeeping is bounded, and the bound is LOUD: the id it cannot take is
    /// refused to its caller (which logs and counts it) rather than dropped
    /// silently or traded for an older id whose client has waited longer.
    #[tokio::test]
    async fn the_bookkeeping_cap_refuses_rather_than_forgets() {
        let store = JobStore::new(1, TTL);
        let ids: Vec<JobId> = (0..MAX_FINISHED_UNPERSISTED)
            .map(|_| JobId::new())
            .collect();
        for id in &ids {
            assert_eq!(
                store.park_finished_unpersisted(*id, JobStatus::Failed),
                ParkOutcome::Tracked
            );
        }
        assert_eq!(
            store.park_finished_unpersisted(JobId::new(), JobStatus::Failed),
            ParkOutcome::Full,
            "the cap must be reported, not absorbed"
        );
        // An id already tracked is still accepted at the cap: re-parking is
        // how a reconciliation attempt that fails again stays tracked.
        assert_eq!(
            store.park_finished_unpersisted(ids[0], JobStatus::Succeeded),
            ParkOutcome::Tracked
        );

        // Every tracked id is still there, and a pass drains all of them.
        let outcome = store.reconcile_finished_jobs().await;
        assert_eq!(outcome.gone, MAX_FINISHED_UNPERSISTED);
        assert_eq!(
            store.park_finished_unpersisted(JobId::new(), JobStatus::Failed),
            ParkOutcome::Tracked,
            "the cap did not free up after a pass settled every id"
        );
    }
}

/// The Postgres inline-cap decision, which the live path can only exercise
/// against a database. The verdict is a pure function of the body, so the
/// boundary and the fail-closed branch are pinned here instead.
#[cfg(test)]
mod result_cap_tests {
    use super::*;
    use serde::Serializer;
    use serde_json::json;

    /// A body whose `Serialize` impl fails — the only way to reach the
    /// unmeasurable branch, which a `serde_json::Value` never does.
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: Serializer>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("this body does not serialize"))
        }
    }

    /// A `Value` that serializes to exactly `bytes`: a JSON string is its
    /// contents plus the two quotes.
    fn body_of_exactly(bytes: usize) -> serde_json::Value {
        let value = serde_json::Value::String("x".repeat(bytes - 2));
        assert_eq!(value.to_string().len(), bytes, "fixture is the wrong size");
        value
    }

    #[test]
    fn a_body_under_the_cap_is_stored() {
        assert_eq!(result_cap_breach(&json!({"columns": [], "rows": []})), None);
    }

    #[test]
    fn a_body_exactly_at_the_cap_is_stored() {
        // The cap is strict `>`; keep it that way.
        assert_eq!(result_cap_breach(&body_of_exactly(MAX_RESULT_BYTES)), None);
    }

    #[test]
    fn one_byte_over_the_cap_is_refused() {
        let reason = result_cap_breach(&body_of_exactly(MAX_RESULT_BYTES + 1))
            .expect("a body over the cap must be refused");
        assert_eq!(
            reason,
            format!(
                "result size {} bytes exceeds Postgres cap {MAX_RESULT_BYTES}",
                MAX_RESULT_BYTES + 1
            ),
            "the failure text a client reads changed"
        );
    }

    #[test]
    fn a_body_that_cannot_be_measured_is_refused_rather_than_bound_blind() {
        let reason =
            result_cap_breach(&Unserializable).expect("an unmeasurable body must not be stored");
        assert!(
            reason.contains("could not be measured"),
            "unhelpful refusal text: {reason}"
        );
    }

    #[test]
    fn the_verdict_reads_the_number_rendering_the_body_would_have() {
        // What `install_succeeded` measures is the `Value` it binds, and the
        // count has to equal the `Value::to_string().len()` it replaced —
        // including for payloads whose JSON encoding is longer than their
        // UTF-8 (escapes, control characters, non-BMP).
        let result = RecordsResponse {
            columns: vec!["message".into(), "level".into()],
            row_count: 2,
            rows: json!([
                {"message": "a\"quote\\ and \u{1}\u{7f} and \u{1f600}", "level": "warn"},
                {"message": "plain", "level": null},
            ]),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        };
        let result_json = serde_json::to_value(&result).unwrap();
        let rendered = result_json.to_string().len();

        assert_eq!(
            crate::format::serialized_json_len(&result_json),
            Some(rendered)
        );
        assert_eq!(result_cap_breach(&result_json), None);
    }
}
