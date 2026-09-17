//! Mpsc-fed per-tenant writer with bounded backpressure.
//!
//! The default OTLP/bulk ingest path serializes writes to each tenant's WAL
//! by holding `Arc<Mutex<WalWriter>>` across an `.append_events`
//! call. That's correct under any single-replica load (the mutex is
//! held for one fsync), but two things bite at scale:
//!
//! - **Tail latency couples handler-to-fsync.** A slow disk PUT
//!   stalls the HTTP handler holding the mutex; every queued request
//!   blocks behind it.
//! - **No backpressure surface.** Once a flood of requests piles up
//!   behind the mutex, the limit is process memory (axum's request
//!   queue + the awaited futures). There's no clean way to push a
//!   client to slow down before things explode.
//!
//! The [`BackpressureRouter`] addresses both: each tenant gets a
//! bounded mpsc channel + a dedicated writer task that drains it in
//! order. HTTP handlers `try_send` into the channel and immediately
//! return — they never block on disk. When the channel is full the
//! handler gets [`SubmitOutcome::Backpressure`], which the ingest
//! handlers surface as `503 Service Unavailable` + `Retry-After`.
//! The writer task batches whatever's queued in one
//! `WalWriter::append_events` call, amortizing the fsync cost.
//!
//! **Opt-in.** [`AppState::with_backpressure`] wires this on. With it
//! `None` (the default), every handler keeps using the
//! `Arc<Mutex<WalWriter>>` path, byte-for-byte compatible with
//! pre-4.12.14 behavior. Phase 4.12.14 ships the primitive; rolling
//! it out by default is a follow-up after smoke-testing under real
//! traffic on AWS.
//!
//! # Shared rate budget (Redis)
//!
//! [`RateBudget`] abstracts the per-key token-bucket check. The
//! existing in-memory [`crate::rate_limit::RateLimiter`] implements
//! it; a future Redis-backed implementation would share a single
//! budget across multiple ingester replicas in the same deployment.
//! Today's rate limiter is per-replica, so a 2-replica deployment
//! effectively grants 2× the configured budget. A shared budget
//! cleans that up; the trait keeps the call sites identical.

use crate::{index_label, tenant_label};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use siglake_core::{events_to_record_batch, Event};
use siglake_wal::WalWriter;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::{append_batches_with_commit, CommitMode, CommitReceipt, EVENTS_INDEX_ID};

// Re-export so existing callers of
// `siglake_ingest::backpressure::{RateBudget, RateOutcome}` keep
// working. The actual trait + impls live in `rate_limit` now —
// they're about rate-limiting, not WAL backpressure.
pub use crate::rate_limit::{RateBudget, RateOutcome};

/// One per-tenant write command sent into the bounded mpsc.
///
/// The currency is a ready-built [`RecordBatch`] (the WAL's native unit), not
/// `Vec<Event>`: the handler builds the batch straight from the borrowed
/// request parse, so no per-event owned strings cross the channel, and the
/// Arrow build happens on the (parallel) handler tasks instead of serializing
/// on this lane's writer task.
struct WriteCommand {
    batch: RecordBatch,
    commit_mode: CommitMode,
    enqueued_at: Instant,
    reply: oneshot::Sender<Result<CommitReceipt>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LaneKey {
    tenant: String,
    index: String,
}

enum LaneCommand {
    Write(WriteCommand),
    Tick,
    /// #2693: rebind the lane's writer to a (possibly new) Iceberg table. Sent
    /// by [`BackpressureRouter::refresh_table_identities`]; the writer seals
    /// the open segment under the previous identity before adopting it.
    Rebind(Option<uuid::Uuid>),
    /// #5055: flush the lane's active segment and report where it is, so the
    /// active-segment mirror can upload it. The writer is OWNED by the lane
    /// task, so this is the only way to reach it; the object-store PUT happens
    /// in the mirror loop, off this task.
    MirrorActive(oneshot::Sender<Option<siglake_wal::mirror::ActiveSnapshot>>),
}

/// Apply a rebind to a lane's writer, logging what it sealed under the
/// previous table. Failures leave the current binding in place — an
/// unstamped-or-stale segment is held by the drain, never mis-attributed.
fn rebind_writer(writer: &mut WalWriter, uuid: Option<uuid::Uuid>, tenant: &str, index: &str) {
    match writer.bind_table_uuid(uuid) {
        Ok(Some(seg)) => tracing::info!(
            tenant,
            index,
            rows = seg.rows,
            "sealed the open WAL segment under the previous table before rebinding"
        ),
        Ok(None) => {}
        Err(e) => tracing::warn!(tenant, index, error = %e, "WAL identity rebind failed"),
    }
}

/// Per-tenant lane: a bounded channel + the JoinHandle of the writer
/// task that drains it.
struct Lane {
    tx: mpsc::Sender<LaneCommand>,
    handle: tokio::task::JoinHandle<()>,
    pending_commands: Arc<AtomicUsize>,
    pending_events: Arc<AtomicUsize>,
}

impl Lane {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        tenant: String,
        index: String,
        tenant_dir: PathBuf,
        mirror_subdir: String,
        ingester_id: String,
        max_events: usize,
        max_age: Duration,
        capacity: usize,
        group_commit_ms: u64,
        mirror_handle: Option<siglake_wal::mirror::WalMirrorHandle>,
        table_uuid: Option<uuid::Uuid>,
    ) -> Result<Self> {
        // Durable creation (#3048): the lane acknowledges rows into this
        // directory, so its entry has to reach the device with them.
        siglake_wal::create_wal_dir(&tenant_dir)
            .with_context(|| format!("creating tenant WAL dir {}", tenant_dir.display()))?;
        let mut writer = WalWriter::with_thresholds(&tenant_dir, ingester_id, max_events, max_age)
            .with_context(|| format!("opening tenant WAL at {}", tenant_dir.display()))?;
        writer.set_mirror_subdir(Some(mirror_subdir));
        if let Some(h) = mirror_handle {
            writer.set_mirror_handle(Some(h));
        }
        // #2693: bound before the writer moves into the task, so the lane's
        // first segment already names the table its rows are destined for.
        if table_uuid.is_some() {
            writer.bind_table_uuid(table_uuid)?;
        }
        let (tx, mut rx) = mpsc::channel::<LaneCommand>(capacity);
        let pending_commands = Arc::new(AtomicUsize::new(0));
        let pending_events = Arc::new(AtomicUsize::new(0));
        let task_pending_commands = pending_commands.clone();
        let task_pending_events = pending_events.clone();
        metrics::gauge!(
            "siglake_ingest_backpressure_queue_commands",
            "tenant" => tenant_label(&tenant),
            "index" => index_label(&index)
        )
        .set(0.0);
        metrics::gauge!(
            "siglake_ingest_backpressure_queue_events",
            "tenant" => tenant_label(&tenant),
            "index" => index_label(&index)
        )
        .set(0.0);

        let handle = tokio::spawn(async move {
            // Each loop iteration: pull one command, then opportunistically
            // batch any further commands already waiting (`try_recv`).
            // This amortizes the WalWriter append + mirror cost when
            // bursts arrive faster than the writer drains.
            while let Some(first) = rx.recv().await {
                let first = match first {
                    LaneCommand::Write(first) => first,
                    LaneCommand::Tick => {
                        if let Err(e) = writer.tick() {
                            tracing::warn!(error = %e, tenant = tenant, "BackpressureRouter writer tick failed");
                        }
                        continue;
                    }
                    LaneCommand::Rebind(uuid) => {
                        rebind_writer(&mut writer, uuid, &tenant, &index);
                        continue;
                    }
                    LaneCommand::MirrorActive(reply) => {
                        let _ = reply.send(siglake_wal::mirror::snapshot_active(&mut writer));
                        continue;
                    }
                };
                let mut drained_commands = 1usize;
                let mut total_rows = first.batch.num_rows();
                let mut batches = vec![first.batch];
                let mut strongest_commit_mode = first.commit_mode;
                let mut batch_wait_secs = vec![first.enqueued_at.elapsed().as_secs_f64()];
                let mut replies = vec![first.reply];
                let mut tick_requested = false;
                let mut pending_rebind: Option<Option<uuid::Uuid>> = None;
                // Answered AFTER the batch is appended, like `tick_requested`:
                // the snapshot an active-mirror tick uploads should carry the
                // rows this lane has already accepted, not stop short of them.
                let mut mirror_replies: Vec<
                    oneshot::Sender<Option<siglake_wal::mirror::ActiveSnapshot>>,
                > = Vec::new();
                // Phase 4.13k group-commit mode: when group_commit_ms > 0
                // we deliberately *wait* up to that many ms for more
                // commands to accumulate before flushing. Trades a few
                // ms of ack latency for amortized fsync cost. With
                // group_commit_ms == 0 we fall back to the original
                // try_recv() drain (no extra latency).
                if group_commit_ms > 0 {
                    let deadline = Instant::now() + Duration::from_millis(group_commit_ms);
                    loop {
                        if total_rows >= 50_000 {
                            break;
                        }
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        tokio::select! {
                            biased;
                            cmd = rx.recv() => {
                                match cmd {
                                    Some(LaneCommand::Write(next)) => {
                                        strongest_commit_mode = strongest_commit_mode.max(next.commit_mode);
                                        total_rows += next.batch.num_rows();
                                        batches.push(next.batch);
                                        batch_wait_secs.push(next.enqueued_at.elapsed().as_secs_f64());
                                        replies.push(next.reply);
                                        drained_commands += 1;
                                    }
                                    Some(LaneCommand::Tick) => {
                                        tick_requested = true;
                                    }
                                    Some(LaneCommand::MirrorActive(reply)) => {
                                        mirror_replies.push(reply);
                                    }
                                    // Not mid-batch: the rows already drained
                                    // into `batches` were accepted for the
                                    // CURRENT table and must be written under
                                    // it. Re-queued for the next iteration.
                                    Some(LaneCommand::Rebind(uuid)) => {
                                        pending_rebind = Some(uuid);
                                        break;
                                    }
                                    None => break,
                                }
                            }
                            _ = tokio::time::sleep_until(deadline.into()) => break,
                        }
                    }
                } else {
                    // Drain everything we can without awaiting.
                    while let Ok(next) = rx.try_recv() {
                        match next {
                            LaneCommand::Write(next) => {
                                strongest_commit_mode = strongest_commit_mode.max(next.commit_mode);
                                total_rows += next.batch.num_rows();
                                batches.push(next.batch);
                                batch_wait_secs.push(next.enqueued_at.elapsed().as_secs_f64());
                                replies.push(next.reply);
                                drained_commands += 1;
                                // Cap batch size to avoid a huge single fsync.
                                if total_rows >= 50_000 {
                                    break;
                                }
                            }
                            LaneCommand::Tick => {
                                tick_requested = true;
                            }
                            LaneCommand::MirrorActive(reply) => {
                                mirror_replies.push(reply);
                            }
                            LaneCommand::Rebind(uuid) => {
                                pending_rebind = Some(uuid);
                                break;
                            }
                        }
                    }
                }
                let n = total_rows;
                task_pending_commands.fetch_sub(drained_commands, Ordering::Relaxed);
                task_pending_events.fetch_sub(n, Ordering::Relaxed);
                metrics::gauge!(
                    "siglake_ingest_backpressure_queue_commands",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index)
                )
                .set(task_pending_commands.load(Ordering::Relaxed) as f64);
                metrics::gauge!(
                    "siglake_ingest_backpressure_queue_events",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index)
                )
                .set(task_pending_events.load(Ordering::Relaxed) as f64);
                for wait in batch_wait_secs {
                    metrics::histogram!(
                        "siglake_ingest_backpressure_queue_wait_seconds",
                        "tenant" => tenant_label(&tenant),
                        "index" => index_label(&index)
                    )
                    .record(wait);
                }
                metrics::histogram!(
                    "siglake_ingest_backpressure_batch_commands",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index)
                )
                .record(drained_commands as f64);
                metrics::histogram!(
                    "siglake_ingest_backpressure_batch_events",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index)
                )
                .record(n as f64);
                let append_start = Instant::now();
                // Append every drained batch; `append_batch` flushes each to
                // the OS page cache and auto-rolls on threshold. Commit
                // semantics key off the last append's seal: any earlier batch
                // was fsynced by that seal, so only the tail can still need
                // `sync_active`.
                let res = append_batches_with_commit(&mut writer, &batches, strongest_commit_mode);
                let append_elapsed = append_start.elapsed().as_secs_f64();
                metrics::histogram!(
                    "siglake_ingest_backpressure_append_duration_seconds",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index)
                )
                .record(append_elapsed);
                metrics::counter!(
                    "siglake_ingest_backpressure_batches_total",
                    "tenant" => tenant_label(&tenant),
                    "index" => index_label(&index),
                    "outcome" => if res.is_ok() { "ok" } else { "error" }
                )
                .increment(1);
                // Every contributor to the batch gets the same Ok/Err —
                // they're committed atomically.
                for reply in replies {
                    let _ = reply.send(match &res {
                        Ok(receipt) => Ok(receipt.clone()),
                        Err(e) => Err(anyhow::anyhow!("{e}")),
                    });
                }
                // Answer the active-mirror requests this batch overtook, now
                // that the rows they should carry are on disk.
                for reply in mirror_replies.drain(..) {
                    let _ = reply.send(siglake_wal::mirror::snapshot_active(&mut writer));
                }
                // AFTER the batch is written and acked: the rows in it were
                // accepted for the identity the writer still holds.
                if let Some(uuid) = pending_rebind.take() {
                    rebind_writer(&mut writer, uuid, &tenant, &index);
                }
                if tick_requested {
                    if let Err(e) = writer.tick() {
                        tracing::warn!(error = %e, tenant = tenant, "BackpressureRouter writer tick failed");
                    }
                }
            }
            // Channel closed (shutdown): force-seal the active segment so any
            // buffered-but-unsealed events land in `sealed/` for the compactor
            // — `tick()` only age-rolls, which would strand a young segment on
            // scale-down. Critical for safe ingester scale-down.
            match writer.seal() {
                Ok(Some(seg)) => tracing::info!(
                    tenant = tenant,
                    rows = seg.rows,
                    "BackpressureRouter sealed active WAL segment on shutdown"
                ),
                Ok(None) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    tenant = tenant,
                    "BackpressureRouter seal-on-shutdown failed"
                ),
            }
        });

        Ok(Self {
            tx,
            handle,
            pending_commands,
            pending_events,
        })
    }
}

/// Per-tenant submission outcome. Maps to the ingest handler's HTTP
/// status code.
#[derive(Debug)]
pub enum SubmitOutcome {
    /// Events were queued and the writer task replied with success
    /// after persisting them and satisfying the requested commit mode.
    Accepted(CommitReceipt),
    /// Lane is full. Handler should return 503 with the suggested
    /// `Retry-After` header.
    Backpressure { retry_after_secs: u64 },
    /// Writer task replied with an error. Handler should return 500.
    Failed(anyhow::Error),
}

/// Mpsc-fed router. One [`Lane`] per active tenant. Created with
/// `new()` and used through `submit()` from the ingest handlers.
/// Group of shard lanes for one tenant. Round-robin across shards
/// via the `next` counter.
struct LaneGroup {
    lanes: Vec<Lane>,
    next: AtomicUsize,
}

pub struct BackpressureRouter {
    root: PathBuf,
    ingester_id: String,
    max_events: usize,
    max_age: Duration,
    capacity: usize,
    /// Phase 4.13k: when > 0, the writer task waits this many ms for
    /// more commands to accumulate before flushing (group commit).
    group_commit_ms: u64,
    /// Phase 4.13k: per-tenant write parallelism. Each shard owns its
    /// own WalWriter + segment files (distinguished by a `-s{i}`
    /// suffix on the ingester_id). 1 (default) preserves the
    /// single-writer-per-tenant behavior.
    shards_per_tenant: usize,
    lanes: Mutex<HashMap<LaneKey, LaneGroup>>,
    /// Most distinct `(tenant, index)` lanes to create. `0` = unbounded.
    ///
    /// BOTH HALVES OF THE KEY ARE CLIENT HEADERS — `X-Scope-OrgID` and
    /// `x-siglake-index` — and nothing capped or evicted this map: the only
    /// removal was `shutdown`. Each novel pair spawned `shards_per_tenant`
    /// tasks, each holding an OPEN FILE, plus a `create_dir_all` on the shared
    /// WAL mount. Two headers, a cross product, no bound: file-descriptor and
    /// inode exhaustion from a client that simply varies a header, in the
    /// default configuration, because the rate limiter is off by default.
    max_lanes: usize,
    mirror_handle: Mutex<Option<siglake_wal::mirror::WalMirrorHandle>>,
    /// #2693: resolves the Iceberg table behind each lane. See
    /// [`crate::WalTableIdentity`].
    identity: Mutex<Option<Arc<dyn crate::WalTableIdentity>>>,
    /// When each lane's identity was last resolved.
    identity_checked: Mutex<HashMap<LaneKey, Instant>>,
}

impl BackpressureRouter {
    /// Cap the number of distinct `(tenant, index)` lanes. `0` = unbounded.
    pub fn with_max_lanes(mut self, max_lanes: usize) -> Self {
        self.max_lanes = max_lanes;
        self
    }

    /// Build a router. `capacity` is the per-tenant bounded mpsc
    /// channel depth — when a tenant's lane has this many commands
    /// outstanding, the next `submit` call returns
    /// [`SubmitOutcome::Backpressure`].
    pub fn new(
        root: impl AsRef<Path>,
        ingester_id: impl Into<String>,
        max_events: usize,
        max_age: Duration,
        capacity: usize,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            ingester_id: ingester_id.into(),
            max_events,
            max_age,
            capacity: capacity.max(1),
            group_commit_ms: 0,
            shards_per_tenant: 1,
            lanes: Mutex::new(HashMap::new()),
            // Unbounded by default so existing deployments are unchanged;
            // `with_max_lanes` is how an operator opts into the bound.
            max_lanes: 0,
            mirror_handle: Mutex::new(None),
            identity: Mutex::new(None),
            identity_checked: Mutex::new(HashMap::new()),
        }
    }

    /// Install the table-identity resolver (#2693). Called once at startup,
    /// before the listener binds, so the first lane spawned already has it.
    pub async fn set_identity_resolver(&self, resolver: Option<Arc<dyn crate::WalTableIdentity>>) {
        *self.identity.lock().await = resolver;
        self.identity_checked.lock().await.clear();
        self.refresh_table_identities().await;
    }

    /// Re-resolve every open lane's table and rebind the writers whose table
    /// changed — the `DELETE`+`POST` case. Best-effort per lane: a full channel
    /// or an unreachable catalog is retried on the next tick, and until then
    /// the lane keeps the identity it has.
    ///
    /// The window between a recreation and the next refresh is the one thing
    /// this does not close: writes accepted in it are stamped with the DROPPED
    /// table and are held under `stale/` by the drain rather than committed.
    /// They are acked — visibly quarantined, not silently dropped — and
    /// `SIGLAKE_WAL_IDENTITY_REFRESH_SECS` is how short an operator makes it.
    pub async fn refresh_table_identities(&self) {
        let Some(resolver) = self.identity.lock().await.clone() else {
            return;
        };
        let ttl = crate::identity_refresh();
        let keys: Vec<LaneKey> = self.lanes.lock().await.keys().cloned().collect();
        for key in keys {
            {
                let checked = self.identity_checked.lock().await;
                if let Some(at) = checked.get(&key) {
                    if ttl.is_zero() || at.elapsed() < ttl {
                        continue;
                    }
                }
            }
            let uuid = match resolver.table_uuid(&key.tenant, &key.index).await {
                Ok(u) => u,
                Err(e) => {
                    metrics::counter!("siglake_wal_identity_resolve_failures_total").increment(1);
                    tracing::warn!(tenant = %key.tenant, index = %key.index, error = %e,
                        "could not resolve this lane's table identity; keeping the current binding");
                    continue;
                }
            };
            let mut delivered = true;
            {
                let lanes = self.lanes.lock().await;
                if let Some(group) = lanes.get(&key) {
                    for lane in &group.lanes {
                        if lane.tx.try_send(LaneCommand::Rebind(uuid)).is_err() {
                            delivered = false;
                        }
                    }
                }
            }
            if delivered {
                self.identity_checked
                    .lock()
                    .await
                    .insert(key, Instant::now());
            }
        }
    }

    /// Resolve `(tenant, index)`'s table for a lane about to be spawned.
    async fn resolve_identity(&self, tenant: &str, index: &str) -> Option<uuid::Uuid> {
        let resolver = self.identity.lock().await.clone()?;
        match resolver.table_uuid(tenant, index).await {
            Ok(uuid) => uuid,
            Err(e) => {
                metrics::counter!("siglake_wal_identity_resolve_failures_total").increment(1);
                tracing::warn!(tenant, index, error = %e,
                    "could not resolve this lane's table identity; its segments carry none \
                     until the next refresh");
                None
            }
        }
    }

    /// Builder: enable group commit. The per-tenant writer task waits
    /// up to `ms` milliseconds for more commands to accumulate before
    /// each flush. 0 (default) → no extra wait.
    pub fn with_group_commit_ms(mut self, ms: u64) -> Self {
        self.group_commit_ms = ms;
        self
    }

    /// Builder: per-tenant write parallelism. `n >= 1`. `lane_for`
    /// round-robins submissions across the tenant's `n` shards; each
    /// shard owns its own WalWriter writing to distinct segment
    /// filenames (the WalWriter's ingester_id gets a `-s{i}` suffix).
    pub fn with_shards_per_tenant(mut self, n: usize) -> Self {
        self.shards_per_tenant = n.max(1);
        self
    }

    /// Attach (or remove) the active-segment mirror handle. Applied
    /// to every new tenant lane.
    pub async fn set_mirror_handle(&self, handle: Option<siglake_wal::mirror::WalMirrorHandle>) {
        *self.mirror_handle.lock().await = handle;
        // Already-running lanes can't have their handle swapped at
        // runtime (the WalWriter is owned by a task); the next tenant
        // lane spawned picks up the new handle. v0 tradeoff —
        // production deployments set the mirror handle once at
        // startup before traffic arrives anyway.
    }

    fn pick_shard(group: &LaneGroup) -> &Lane {
        let idx = group.next.fetch_add(1, Ordering::Relaxed) % group.lanes.len();
        &group.lanes[idx]
    }

    async fn lane_for(
        &self,
        tenant: &str,
        index: &str,
    ) -> Result<(
        mpsc::Sender<LaneCommand>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    )> {
        let key = LaneKey {
            tenant: tenant.to_string(),
            index: index.to_string(),
        };
        // Fast path under read-mostly lookup.
        {
            let lanes = self.lanes.lock().await;
            if let Some(g) = lanes.get(&key) {
                let l = Self::pick_shard(g);
                return Ok((
                    l.tx.clone(),
                    l.pending_commands.clone(),
                    l.pending_events.clone(),
                ));
            }
        }
        // Resolved BEFORE the write lock: this is a catalog metadata read, and
        // holding the lanes map across it would serialize every other lane's
        // creation behind one round trip.
        let table_uuid = self.resolve_identity(tenant, index).await;
        let mut lanes = self.lanes.lock().await;
        if let Some(g) = lanes.get(&key) {
            let l = Self::pick_shard(g);
            return Ok((
                l.tx.clone(),
                l.pending_commands.clone(),
                l.pending_events.clone(),
            ));
        }
        // REFUSED BEFORE ANYTHING IS CREATED. Past the cap nothing is spawned,
        // no directory is made and no writer opens a file — the caller gets an
        // error and the process keeps the descriptors it has.
        if self.max_lanes > 0 && lanes.len() >= self.max_lanes {
            metrics::counter!("siglake_ingest_lane_refused_total").increment(1);
            anyhow::bail!(
                "lane limit reached ({} lanes): refusing a new (tenant `{tenant}`, index \
                 `{index}`) lane. Both are client headers; raise --ingest-max-lanes or bound \
                 them with --allowed-tenants.",
                self.max_lanes
            );
        }
        let tenant_root = self.root.join(tenant);
        let dir = if index == EVENTS_INDEX_ID {
            tenant_root.clone()
        } else {
            siglake_wal::create_wal_dir(&tenant_root.join(siglake_wal::SEALED_DIR)).with_context(
                || format!("creating tenant discovery dir {}", tenant_root.display()),
            )?;
            tenant_root.join(index)
        };
        let mirror_subdir = if index == EVENTS_INDEX_ID {
            tenant.to_string()
        } else {
            format!("{tenant}/{index}")
        };
        let mirror = self.mirror_handle.lock().await.clone();
        let mut shard_lanes = Vec::with_capacity(self.shards_per_tenant);
        for i in 0..self.shards_per_tenant {
            let shard_ingester_id = if self.shards_per_tenant == 1 {
                self.ingester_id.clone()
            } else {
                format!("{}-s{}", self.ingester_id, i)
            };
            let lane = Lane::spawn(
                tenant.to_string(),
                index.to_string(),
                dir.clone(),
                mirror_subdir.clone(),
                shard_ingester_id,
                self.max_events,
                self.max_age,
                self.capacity,
                self.group_commit_ms,
                mirror.clone(),
                table_uuid,
            )?;
            shard_lanes.push(lane);
        }
        let first = &shard_lanes[0];
        let tx = first.tx.clone();
        let pending_commands = first.pending_commands.clone();
        let pending_events = first.pending_events.clone();
        let group = LaneGroup {
            lanes: shard_lanes,
            next: AtomicUsize::new(1), // first request returns shard 0; next round-robins to 1
        };
        if table_uuid.is_some() {
            self.identity_checked
                .lock()
                .await
                .insert(key.clone(), Instant::now());
        }
        lanes.insert(key, group);
        metrics::gauge!("siglake_ingest_backpressure_lanes")
            .set(lanes.values().map(|g| g.lanes.len()).sum::<usize>() as f64);
        Ok((tx, pending_commands, pending_events))
    }

    /// Submit a batch for `tenant`. Returns immediately if the lane
    /// is full ([`SubmitOutcome::Backpressure`]); otherwise awaits the
    /// writer task's reply (which is the WAL append's actual
    /// completion).
    pub async fn submit(&self, tenant: &str, events: Vec<Event>) -> SubmitOutcome {
        let batch = match events_to_record_batch(&events) {
            Ok(batch) => batch,
            Err(e) => return SubmitOutcome::Failed(e.into()),
        };
        self.submit_for_index_with_commit(tenant, EVENTS_INDEX_ID, batch, CommitMode::Auto)
            .await
    }

    pub async fn submit_for_index(
        &self,
        tenant: &str,
        index: &str,
        events: Vec<Event>,
    ) -> SubmitOutcome {
        let batch = match events_to_record_batch(&events) {
            Ok(batch) => batch,
            Err(e) => return SubmitOutcome::Failed(e.into()),
        };
        self.submit_for_index_with_commit(tenant, index, batch, CommitMode::Auto)
            .await
    }

    pub(crate) async fn submit_for_index_with_commit(
        &self,
        tenant: &str,
        index: &str,
        batch: RecordBatch,
        commit_mode: CommitMode,
    ) -> SubmitOutcome {
        let event_count = batch.num_rows();
        let (tx, pending_commands, pending_events) = match self.lane_for(tenant, index).await {
            Ok(lane) => lane,
            Err(e) => return SubmitOutcome::Failed(e),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = WriteCommand {
            batch,
            commit_mode,
            enqueued_at: Instant::now(),
            reply: reply_tx,
        };
        match tx.try_send(LaneCommand::Write(cmd)) {
            Ok(()) => {
                let commands = pending_commands.fetch_add(1, Ordering::Relaxed) + 1;
                let queued_events =
                    pending_events.fetch_add(event_count, Ordering::Relaxed) + event_count;
                metrics::gauge!(
                    "siglake_ingest_backpressure_queue_commands",
                    "tenant" => tenant_label(tenant),
                    "index" => index_label(index)
                )
                .set(commands as f64);
                metrics::gauge!(
                    "siglake_ingest_backpressure_queue_events",
                    "tenant" => tenant_label(tenant),
                    "index" => index_label(index)
                )
                .set(queued_events as f64);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                return SubmitOutcome::Backpressure {
                    // A second is a reasonable client-visible
                    // breathing room. The lane drains in roughly
                    // single-event-per-fsync time, so by next second
                    // the lane is likely available again.
                    retry_after_secs: 1,
                };
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return SubmitOutcome::Failed(anyhow::anyhow!(
                    "BackpressureRouter lane for tenant `{tenant}` index `{index}` closed (writer task died)"
                ));
            }
        }
        match reply_rx.await {
            Ok(Ok(receipt)) => SubmitOutcome::Accepted(receipt),
            Ok(Err(e)) => SubmitOutcome::Failed(e),
            Err(_) => SubmitOutcome::Failed(anyhow::anyhow!(
                "writer task dropped the reply channel for tenant `{tenant}` index `{index}`"
            )),
        }
    }

    /// Tick every tenant's writer once. The tick is best-effort: if a
    /// lane is full we skip it and let the next periodic tick retry.
    pub async fn tick_all(&self) {
        let lanes = self.lanes.lock().await;
        for group in lanes.values() {
            for lane in &group.lanes {
                let _ = lane.tx.try_send(LaneCommand::Tick);
            }
        }
    }

    /// Flush every lane's active segment for the active-segment mirror, and
    /// report the ones with something to upload.
    ///
    /// One request per shard lane, because one WalWriter per shard lane is what
    /// holds the rows. A lane whose channel is full is skipped and picked up on
    /// the next tick — the same best-effort contract as [`Self::tick_all`],
    /// which is what keeps a saturated lane from stalling the mirror loop.
    async fn flush_active_segments(&self) -> Vec<siglake_wal::mirror::ActiveSnapshot> {
        let mut waiters = Vec::new();
        {
            let lanes = self.lanes.lock().await;
            for group in lanes.values() {
                for lane in &group.lanes {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    if lane
                        .tx
                        .try_send(LaneCommand::MirrorActive(reply_tx))
                        .is_ok()
                    {
                        waiters.push(reply_rx);
                    }
                }
            }
        }
        // The lanes map is released before any await: a `submit` that needs a
        // new lane must not queue behind a mirror tick.
        let mut out = Vec::with_capacity(waiters.len());
        for reply_rx in waiters {
            // `Err` is a writer task that went away between the send and the
            // reply — shutdown. Nothing to mirror either way.
            if let Ok(Some(snapshot)) = reply_rx.await {
                out.push(snapshot);
            }
        }
        out
    }

    /// Gracefully drop every tenant's mpsc sender, then join the
    /// writer tasks. Used by `siglake-cli`'s shutdown path so
    /// in-flight writes finish before the process exits.
    ///
    /// Takes `&self` so the caller can call this through an
    /// `Arc<BackpressureRouter>` — required because the same router
    /// is also held by the AppState that axum::serve owns. After
    /// shutdown returns, the lanes map is empty and any further
    /// `submit` calls will get fresh lanes (which the caller
    /// shouldn't make — by contract this is called once, on the way
    /// out).
    pub async fn shutdown(&self) {
        let mut lanes = self.lanes.lock().await;
        let taken: Vec<(LaneKey, LaneGroup)> = lanes.drain().collect();
        // Release the lock before the await-on-join, so concurrent
        // `submit` callers don't deadlock on a `lanes.lock()`.
        drop(lanes);
        for (_, group) in taken {
            for lane in group.lanes {
                // Drop sender — writer task observes channel closed
                // and ticks itself, then the loop exits + the
                // JoinHandle resolves.
                drop(lane.tx);
                let _ = lane.handle.await;
            }
        }
        metrics::gauge!("siglake_ingest_backpressure_lanes").set(0.0);
    }
}

/// Active mirroring covers the lanes' writers, which on the default ingest path
/// are the only writers that receive rows (#5055).
#[async_trait::async_trait]
impl siglake_wal::mirror::ActiveMirrorSource for BackpressureRouter {
    async fn flush_active(&self) -> Vec<siglake_wal::mirror::ActiveSnapshot> {
        self.flush_active_segments().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use siglake_wal::{list_sealed, read_segment};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::tempdir;

    fn ev(host: &str, raw: &str) -> Event {
        Event {
            timestamp: Utc::now(),
            host: host.into(),
            source: "src".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: raw.into(),
            attributes: None,
        }
    }

    #[tokio::test]
    async fn accepts_and_persists_a_batch() {
        let tmp = tempdir().unwrap();
        let router =
            BackpressureRouter::new(tmp.path(), "test-ingester", 10, Duration::from_secs(60), 16);
        let outcome = router
            .submit("default", vec![ev("h1", "r1"), ev("h1", "r2")])
            .await;
        match outcome {
            SubmitOutcome::Accepted(_) => {}
            other => panic!("expected Accepted, got {other:?}"),
        }
        // Per-tenant directory created.
        assert!(tmp.path().join("default").exists());
    }

    #[tokio::test]
    async fn full_lane_returns_backpressure() {
        // Capacity 1, slow events. Fill the lane, then submit again
        // before the writer drains.
        let tmp = tempdir().unwrap();
        let router = Arc::new(BackpressureRouter::new(
            tmp.path(),
            "test-ingester",
            // Threshold of 1 forces a seal-per-write; the writer task
            // does real work and won't drain instantly.
            1,
            Duration::from_secs(60),
            1,
        ));

        // Spawn many submits in parallel; the first to land takes the
        // slot, additional ones face the bounded mpsc. We expect at
        // least one to hit Backpressure.
        let mut handles = Vec::new();
        for i in 0..32 {
            let r = router.clone();
            handles.push(tokio::spawn(async move {
                r.submit("tenant", vec![ev("h", &format!("r{i}"))]).await
            }));
        }

        let mut saw_backpressure = false;
        let mut saw_accepted = false;
        for h in handles {
            match h.await.unwrap() {
                SubmitOutcome::Accepted(_) => saw_accepted = true,
                SubmitOutcome::Backpressure { retry_after_secs } => {
                    assert!(retry_after_secs >= 1);
                    saw_backpressure = true;
                }
                SubmitOutcome::Failed(e) => panic!("unexpected Failed: {e:#}"),
            }
        }
        assert!(saw_accepted, "at least one submission should land");
        assert!(
            saw_backpressure,
            "concurrent flood should hit the bounded channel"
        );
    }

    #[tokio::test]
    async fn per_tenant_lanes_are_independent() {
        let tmp = tempdir().unwrap();
        let router =
            BackpressureRouter::new(tmp.path(), "test-ingester", 10, Duration::from_secs(60), 8);
        let a = router.submit("acme", vec![ev("ha", "a")]).await;
        let b = router.submit("widgets", vec![ev("hw", "w")]).await;
        assert!(matches!(a, SubmitOutcome::Accepted(_)));
        assert!(matches!(b, SubmitOutcome::Accepted(_)));
        assert!(tmp.path().join("acme").exists());
        assert!(tmp.path().join("widgets").exists());
    }

    #[tokio::test]
    async fn per_index_lanes_are_independent() {
        let tmp = tempdir().unwrap();
        let router =
            BackpressureRouter::new(tmp.path(), "test-ingester", 10, Duration::from_secs(60), 8);
        let events = router
            .submit_for_index("acme", EVENTS_INDEX_ID, vec![ev("ha", "a")])
            .await;
        let custom = router
            .submit_for_index("acme", "app1", vec![ev("hb", "b")])
            .await;
        assert!(matches!(events, SubmitOutcome::Accepted(_)));
        assert!(matches!(custom, SubmitOutcome::Accepted(_)));
        assert!(tmp.path().join("acme").exists());
        assert!(tmp.path().join("acme").join("app1").exists());
    }

    // The trait-level integration test moved to `rate_limit::tests`
    // where the trait lives.

    /// Phase 4.13d: `shutdown` must drain every lane's writer task
    /// before returning. Concretely: every `Ok(_)` reply the
    /// router has handed out before shutdown ran must be backed
    /// by a sealed WAL segment on disk by the time shutdown
    /// returns.
    ///
    /// Test plan: submit a burst, await every reply, then call
    /// shutdown. The lanes map must be empty after shutdown
    /// (it was non-empty during the burst), and the writer
    /// tasks must have terminated (we can re-create a lane
    /// with the same tenant + observe a fresh tx).
    #[tokio::test]
    async fn shutdown_drains_all_lanes() {
        let tmp = tempdir().unwrap();
        let router =
            BackpressureRouter::new(tmp.path(), "test-ingester", 10, Duration::from_secs(60), 16);
        for i in 0..5 {
            let outcome = router.submit("acme", vec![ev("h", &format!("r{i}"))]).await;
            assert!(matches!(outcome, SubmitOutcome::Accepted(_)));
        }
        for i in 0..3 {
            let outcome = router
                .submit("widgets", vec![ev("h", &format!("w{i}"))])
                .await;
            assert!(matches!(outcome, SubmitOutcome::Accepted(_)));
        }
        // Both lanes are live before shutdown.
        {
            let lanes = router.lanes.lock().await;
            assert_eq!(lanes.len(), 2);
        }
        router.shutdown().await;
        // Lanes map drained; writer tasks have terminated.
        {
            let lanes = router.lanes.lock().await;
            assert!(lanes.is_empty(), "shutdown must drain the lanes map");
        }
        // Scale-down safety: the active segments were younger than the
        // size (10) + age (60s) thresholds, so only a force-seal on
        // shutdown saves their buffered events. Both tenants' events must
        // now be in `sealed/` for the compactor — not stranded in `active/`.
        let acme_rows: usize = list_sealed(&tmp.path().join("acme"))
            .unwrap()
            .iter()
            .map(|s| {
                read_segment(s)
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(
            acme_rows, 5,
            "acme's 5 buffered events must be sealed on shutdown"
        );
        let widgets_rows: usize = list_sealed(&tmp.path().join("widgets"))
            .unwrap()
            .iter()
            .map(|s| {
                read_segment(s)
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
            })
            .sum();
        assert_eq!(
            widgets_rows, 3,
            "widgets' 3 buffered events must be sealed on shutdown"
        );
    }

    #[tokio::test]
    async fn tick_all_seals_idle_active_segments() {
        let tmp = tempdir().unwrap();
        let router = BackpressureRouter::new(
            tmp.path(),
            "test-ingester",
            10_000,
            Duration::from_millis(20),
            16,
        );
        let outcome = router.submit("acme", vec![ev("h", "r1")]).await;
        assert!(matches!(outcome, SubmitOutcome::Accepted(_)));
        tokio::time::sleep(Duration::from_millis(40)).await;
        router.tick_all().await;
        tokio::time::sleep(Duration::from_millis(40)).await;
        let sealed = list_sealed(&tmp.path().join("acme")).unwrap();
        assert_eq!(sealed.len(), 1, "tick_all should age-roll the idle segment");
    }
}
