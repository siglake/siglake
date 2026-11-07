//! Internal lifecycle service. No route, environment, startup or audit wiring.
use super::*;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Notify, task::JoinHandle, time::Instant};

pub const WRITE_BUDGET: Duration = Duration::from_millis(100);
const RETRY_AGE: Duration = Duration::from_secs(30);
const QUEUE_ENTRIES: usize = 1000;
const QUEUE_BYTES: usize = 8 * 1024 * 1024;
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(40);
const PURGE_INTERVAL: Duration = Duration::from_secs(60);

/// Process-local signal counters, NOT exact lost-execution counts across crashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum LossReason {
    NoReceipt,
    StoreUnavailable,
    Timeout,
    QueueFull,
    QueueContended,
    RetryExpired,
    Conflict,
    Expired,
    Shutdown,
}
const LOSS_REASONS: usize = 9;

#[derive(Clone, Debug)]
pub struct HistoryHealth {
    /// Always true: crashes and unobserved requests cannot be counted exactly.
    pub partial_coverage: bool,
    /// Independent of SQL availability. No public route uses this service yet.
    pub history_ready: bool,
    pub startup_caught_up: bool,
    pub maintenance_ok: bool,
    pub writer_ok: bool,
    pub queued_entries: usize,
    pub queued_payload_bytes: usize,
    pub oldest_overdue_ms: Option<i64>,
    pub cleanup_due: bool,
    pub heartbeat_stale: bool,
    pub last_cleanup_success: Option<Instant>,
    pub last_heartbeat_success: Option<Instant>,
    pub recovered_indeterminate: u64,
    pub loss_signals: [u64; LOSS_REASONS],
    pub closed: bool,
}
impl HistoryHealth {
    pub fn loss_signals(&self, reason: LossReason) -> u64 {
        self.loss_signals[reason as usize]
    }
}

/// An acknowledged write, volatile queued retry, disabled observation, or a
/// repeated identical logical terminal. Queued NEVER means durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Persisted,
    Queued,
    Unobserved,
    AlreadyFinalized,
}

#[derive(Clone, Serialize)]
struct Observation {
    event: PendingExecution,
    admitted: bool,
    terminal: Option<TerminalExecution>,
    abandoned: bool,
}
#[derive(Clone, Serialize)]
enum Payload {
    Observation(Box<Observation>),
    LossPoint {
        id: Uuid,
        tenant: String,
        queue_full: bool,
    },
}
impl Payload {
    fn id(&self) -> Uuid {
        match self {
            Self::Observation(o) => o.event.receipt.execution_id,
            Self::LossPoint { id, .. } => *id,
        }
    }
    fn rank(&self) -> u8 {
        match self {
            Self::Observation(o) if o.terminal.is_some() => 3,
            Self::Observation(o) if o.abandoned => 2,
            Self::Observation(o) if o.admitted => 1,
            _ => 0,
        }
    }
    fn bytes(&self) -> usize {
        // Only bounded, safe, owned DTOs; never requests, SQL, results or errors.
        serde_json::to_vec(self).map_or(QUEUE_BYTES + 1, |p| p.len())
    }
    async fn persist(&self, writer: &HistoryWriter) -> Result<(), HistoryError> {
        match self {
            Self::Observation(o) => {
                writer.persist_pending(&o.event).await?;
                if let Some(terminal) = &o.terminal {
                    writer.finish(&o.event.receipt, terminal).await?;
                } else {
                    if o.admitted {
                        writer.admit(&o.event.receipt).await?;
                    }
                    if o.abandoned {
                        writer.abandon_pending(&o.event.receipt).await?;
                    }
                }
            }
            Self::LossPoint {
                id,
                tenant,
                queue_full,
            } => {
                writer
                    .record_loss_observation(*id, tenant, *queue_full)
                    .await?;
            }
        }
        Ok(())
    }
}
struct Entry {
    payload: Payload,
    first_attempt: Instant,
    bytes: usize,
    store_failure_observed: bool,
}
impl Entry {
    fn new(payload: Payload) -> Self {
        let bytes = payload.bytes();
        Self {
            payload,
            first_attempt: Instant::now(),
            bytes,
            store_failure_observed: false,
        }
    }
    fn expired(&self) -> bool {
        Instant::now().duration_since(self.first_attempt) >= RETRY_AGE
    }
}
#[derive(Default)]
struct RetryQueue {
    entries: VecDeque<Entry>,
    // Reservations INCLUDE the single in-flight retry until its future ends.
    bytes: usize,
    count: usize,
}
impl RetryQueue {
    fn push(&mut self, mut entry: Entry) -> bool {
        let existing = self
            .entries
            .iter()
            .position(|e| e.payload.id() == entry.payload.id());
        if let Some(index) = existing {
            let old = &mut self.entries[index];
            if old.payload.rank() > entry.payload.rank() {
                old.store_failure_observed |= entry.store_failure_observed;
                old.first_attempt = old.first_attempt.min(entry.first_attempt);
                return true;
            }
            entry.first_attempt = entry.first_attempt.min(old.first_attempt);
            entry.store_failure_observed |= old.store_failure_observed;
            if self.bytes - old.bytes + entry.bytes > QUEUE_BYTES {
                return false;
            }
            self.bytes -= old.bytes;
            self.entries.remove(index);
            self.count -= 1;
        } else if self.count >= QUEUE_ENTRIES || self.bytes + entry.bytes > QUEUE_BYTES {
            return false;
        }
        self.bytes += entry.bytes;
        self.count += 1;
        self.entries.push_back(entry);
        true
    }
    fn take(&mut self) -> Option<Entry> {
        self.entries.pop_front()
    }
    fn expire(&mut self) -> Vec<Entry> {
        let mut expired = Vec::new();
        let mut live = VecDeque::new();
        while let Some(entry) = self.entries.pop_front() {
            if entry.expired() {
                self.release(&entry);
                expired.push(entry);
            } else {
                live.push_back(entry);
            }
        }
        self.entries = live;
        expired
    }
    fn release(&mut self, entry: &Entry) {
        self.bytes -= entry.bytes;
        self.count -= 1;
    }
}
struct LocalState {
    queue: RetryQueue,
    health: HistoryHealth,
}
struct Inner {
    writer: HistoryWriter,
    owner: Uuid,
    closed: AtomicBool,
    signals: [AtomicU64; LOSS_REASONS],
    local: Mutex<LocalState>,
    wake: Notify,
}
impl Inner {
    fn signal(&self, reason: LossReason) {
        self.signals[reason as usize].fetch_add(1, Ordering::Relaxed);
    }
    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    fn queue(&self, payload: Payload) -> Delivery {
        self.queue_evidence(payload, false)
    }
    fn queue_evidence(&self, payload: Payload, store_failure_observed: bool) -> Delivery {
        // Also called by Drop: never wait for another thread's state lock.
        let Ok(mut local) = self.local.try_lock() else {
            self.signal(LossReason::QueueContended);
            return Delivery::Unobserved;
        };
        let tenant = match &payload {
            Payload::Observation(o) => Some(o.event.principal.tenant.clone()),
            _ => None,
        };
        let mut entry = Entry::new(payload);
        entry.store_failure_observed = store_failure_observed;
        let reason = if self.closed() {
            Some(LossReason::Shutdown)
        } else if !local.queue.push(entry) {
            // A small point may fit when a large observation hit the byte cap.
            // If the entry cap is full, even the loss signal can be lost.
            if let Some(tenant) = tenant {
                if local.queue.push(Entry::new(Payload::LossPoint {
                    id: Uuid::new_v4(),
                    tenant,
                    queue_full: true,
                })) {
                    self.wake.notify_one();
                }
            }
            Some(LossReason::QueueFull)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.signal(reason);
            Delivery::Unobserved
        } else {
            self.wake.notify_one();
            Delivery::Queued
        }
    }
    fn loss_point(&self, tenant: String, queue_full: bool) {
        self.queue(Payload::LossPoint {
            id: Uuid::new_v4(),
            tenant,
            queue_full,
        });
    }
    fn write_result(&self, result: &Result<(), AttemptError>) {
        self.local
            .lock()
            .expect("history local state poisoned")
            .health
            .writer_ok = matches!(
            result,
            Ok(())
                | Err(AttemptError::Store(
                    HistoryError::Conflict
                        | HistoryError::ExpiredOrAbsent
                        | HistoryError::InvalidValue
                ))
        );
        if let Err(error) = result {
            self.signal(error.reason());
        }
    }
    async fn deliver(&self, payload: Payload) -> Delivery {
        if self.closed() {
            self.signal(LossReason::Shutdown);
            return Delivery::Unobserved;
        }
        let result = bounded(payload.persist(&self.writer)).await;
        self.write_result(&result);
        match result {
            Ok(()) => Delivery::Persisted,
            Err(error) if error.retryable() => self.queue_evidence(payload, true),
            Err(_) => Delivery::Unobserved,
        }
    }
}
#[derive(Clone, Copy, Debug)]
enum AttemptError {
    Timeout,
    Store(HistoryError),
}
impl AttemptError {
    fn retryable(self) -> bool {
        matches!(self, Self::Timeout | Self::Store(HistoryError::Unavailable))
    }
    fn reason(self) -> LossReason {
        match self {
            Self::Timeout => LossReason::Timeout,
            Self::Store(HistoryError::Conflict) => LossReason::Conflict,
            Self::Store(HistoryError::ExpiredOrAbsent) => LossReason::Expired,
            _ => LossReason::StoreUnavailable,
        }
    }
}
async fn bounded<T>(
    future: impl std::future::Future<Output = Result<T, HistoryError>>,
) -> Result<T, AttemptError> {
    bounded_before(future, Instant::now() + WRITE_BUDGET).await
}
async fn bounded_before<T>(
    future: impl std::future::Future<Output = Result<T, HistoryError>>,
    deadline: Instant,
) -> Result<T, AttemptError> {
    tokio::time::timeout_at(deadline.min(Instant::now() + WRITE_BUDGET), future)
        .await
        .map_err(|_| AttemptError::Timeout)?
        .map_err(AttemptError::Store)
}

/// Sole owner of ONE maintenance/retry task. Construction requires an explicitly
/// supplied, preflighted writer; nothing consumes env or migrates a database.
/// Dropping aborts that task; `shutdown` attempts a bounded volatile drain first.
pub struct HistoryService {
    inner: Arc<Inner>,
    task: Option<JoinHandle<()>>,
}
impl HistoryService {
    pub fn new(writer: HistoryWriter) -> Self {
        let inner = Arc::new(Inner {
            writer,
            owner: Uuid::new_v4(),
            closed: AtomicBool::new(false),
            signals: std::array::from_fn(|_| AtomicU64::new(0)),
            local: Mutex::new(LocalState {
                queue: RetryQueue::default(),
                health: HistoryHealth {
                    partial_coverage: true,
                    history_ready: false,
                    startup_caught_up: false,
                    maintenance_ok: false,
                    writer_ok: false,
                    queued_entries: 0,
                    queued_payload_bytes: 0,
                    oldest_overdue_ms: None,
                    cleanup_due: true,
                    heartbeat_stale: true,
                    last_cleanup_success: None,
                    last_heartbeat_success: None,
                    recovered_indeterminate: 0,
                    loss_signals: [0; LOSS_REASONS],
                    closed: false,
                },
            }),
            wake: Notify::new(),
        });
        let task = tokio::spawn(run(inner.clone()));
        Self {
            inner,
            task: Some(task),
        }
    }
    pub fn owner_uuid(&self) -> Uuid {
        self.inner.owner
    }
    pub fn health(&self) -> HistoryHealth {
        let local = self
            .inner
            .local
            .lock()
            .expect("history local state poisoned");
        let mut health = local.health.clone();
        health.closed = self.inner.closed();
        health.loss_signals =
            std::array::from_fn(|index| self.inner.signals[index].load(Ordering::Relaxed));
        health.queued_entries = local.queue.count;
        health.queued_payload_bytes = local.queue.bytes;
        health.cleanup_due = health
            .last_cleanup_success
            .is_none_or(|last| last.elapsed() > PURGE_INTERVAL + RETRY_INTERVAL);
        health.heartbeat_stale = health
            .last_heartbeat_success
            .is_none_or(|last| last.elapsed() >= HEARTBEAT_INTERVAL * 3);
        health.history_ready = !health.closed
            && self.task.as_ref().is_some_and(|task| !task.is_finished())
            && health.startup_caught_up
            && health.maintenance_ok
            && health.writer_ok
            && !health.cleanup_due
            && !health.heartbeat_stale
            && health.oldest_overdue_ms.is_none()
            && health.queued_entries == 0;
        health
    }
    /// One 100ms budget for BOTH receipt and initial persistence. A missing DB
    /// receipt yields a disabled handle, never a local-time/recovery receipt.
    /// Failure does not reject or delay SQL beyond this budget. Caller must not
    /// turn history delivery/readiness into SQL admission policy.
    pub async fn begin(
        &self,
        principal: Principal,
        request: RequestMetadata,
        sql: RedactedSql,
    ) -> ObservationHandle {
        if self.inner.closed() {
            self.inner.signal(LossReason::Shutdown);
            return ObservationHandle::disabled(self.inner.clone());
        }
        let mut observation = None;
        let result = bounded(async {
            let receipt = self.inner.writer.receipt().await?;
            let event =
                PendingExecution::new(receipt, principal.clone(), self.inner.owner, request, sql)?;
            observation = Some(Observation {
                event,
                admitted: false,
                terminal: None,
                abandoned: false,
            });
            self.inner
                .writer
                .persist_pending(&observation.as_ref().expect("receipt assigned").event)
                .await?;
            Ok(())
        })
        .await;
        self.inner.write_result(&result);
        match observation {
            Some(observation) => {
                let delivery = match result {
                    Ok(()) => Delivery::Persisted,
                    Err(error) if error.retryable() => self
                        .inner
                        .queue_evidence(Payload::Observation(Box::new(observation.clone())), true),
                    Err(_) => Delivery::Unobserved,
                };
                ObservationHandle {
                    inner: self.inner.clone(),
                    observation: Mutex::new(Some(observation)),
                    initial_delivery: delivery,
                }
            }
            None => {
                self.inner.signal(LossReason::NoReceipt);
                self.inner.loss_point(principal.tenant, false);
                ObservationHandle::disabled(self.inner.clone())
            }
        }
    }
    /// One 100ms asynchronous waiting budget, not a real-time scheduler bound.
    /// Failure preserves uncertainty, not synthetic terminal success or durable
    /// delivery. On timeout abort is requested; termination may not yet have
    /// been observed. The task stays owned until its completion is consumed,
    /// including if this future is cancelled during either wait.
    pub async fn shutdown(&mut self) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.wake.notify_one();
        let deadline = Instant::now() + WRITE_BUDGET;
        let Some(task) = self.task.as_mut() else {
            return self.health().queued_entries == 0;
        };
        match tokio::time::timeout_at(deadline, &mut *task).await {
            Ok(result) => {
                self.task.take();
                if result.is_err() {
                    self.inner.signal(LossReason::Shutdown);
                    false
                } else {
                    self.health().queued_entries == 0
                }
            }
            Err(_) => {
                self.inner.signal(LossReason::Shutdown);
                task.abort();
                // No new budget or unbounded abort-and-join wait. Keep ownership
                // if completion cannot be observed before the same deadline.
                if tokio::time::timeout_at(deadline, &mut *task).await.is_ok() {
                    self.task.take();
                }
                false
            }
        }
    }
}
impl Drop for HistoryService {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Transfer this owner (or an Arc of it) into the final records/stream/batch
/// wrapper; do not create another parent. No public routes currently do so.
/// Dropping without terminal evidence queues uncertainty, not execution failure
/// or proof of client disconnection/cancellation. Drop never awaits or spawns.
pub struct ObservationHandle {
    inner: Arc<Inner>,
    observation: Mutex<Option<Observation>>,
    initial_delivery: Delivery,
}
impl ObservationHandle {
    fn disabled(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            observation: Mutex::new(None),
            initial_delivery: Delivery::Unobserved,
        }
    }
    pub fn receipt(&self) -> Option<Receipt> {
        self.observation
            .lock()
            .expect("history observation poisoned")
            .as_ref()
            .map(|o| o.event.receipt.clone())
    }
    pub fn initial_delivery(&self) -> Delivery {
        self.initial_delivery
    }
    /// Trusted backend admission only. Volatile on queue loss; terminal evidence
    /// can still reconcile an absent admission write with the original receipt.
    pub fn admit(&self) -> Result<Delivery, HistoryError> {
        let payload = {
            let mut state = self
                .observation
                .lock()
                .expect("history observation poisoned");
            let Some(o) = state.as_mut() else {
                return Ok(Delivery::Unobserved);
            };
            if o.terminal.is_some() {
                return Err(HistoryError::Conflict);
            }
            o.admitted = true;
            Payload::Observation(Box::new(o.clone()))
        };
        Ok(self.inner.queue(payload))
    }
    /// Caller supplies genuine outcome and separately measured monotonic
    /// duration. Persistence timestamps are never substituted for event times.
    /// Exactly one local terminal choice; identical repeats are no-ops. Once
    /// selected, cancellation/timeout cannot choose a different outcome.
    pub async fn finish(&self, terminal: TerminalExecution) -> Result<Delivery, HistoryError> {
        let payload = {
            let mut state = self
                .observation
                .lock()
                .expect("history observation poisoned");
            let Some(o) = state.as_mut() else {
                return Ok(Delivery::Unobserved);
            };
            if let Some(existing) = &o.terminal {
                return if serde_json::to_value(existing).ok()
                    == serde_json::to_value(&terminal).ok()
                {
                    Ok(Delivery::AlreadyFinalized)
                } else {
                    Err(HistoryError::Conflict)
                };
            }
            if o.admitted && terminal.admission_evidence != AdmissionEvidence::Admitted {
                return Err(HistoryError::Conflict);
            }
            o.terminal = Some(terminal);
            Payload::Observation(Box::new(o.clone()))
        };
        // Queue BEFORE awaiting: cancellation of this future must not lose the
        // only terminal evidence. Identical replay tolerates ambiguous commits.
        let queued = self.inner.queue(payload.clone());
        if queued == Delivery::Unobserved {
            return Ok(queued);
        }
        Ok(self.inner.deliver(payload).await)
    }
}
impl Drop for ObservationHandle {
    fn drop(&mut self) {
        let state = self
            .observation
            .get_mut()
            .expect("history observation poisoned");
        if let Some(o) = state.as_mut() {
            if o.terminal.is_none() {
                o.abandoned = true;
                self.inner.queue(Payload::Observation(Box::new(o.clone())));
            }
        }
    }
}

async fn maintain(inner: &Inner, heartbeat: bool, purge: bool) -> bool {
    let result = bounded(async {
        if heartbeat {
            inner.writer.heartbeat(inner.owner).await?;
            inner
                .local
                .lock()
                .expect("history local state poisoned")
                .health
                .last_heartbeat_success = Some(Instant::now());
            let recovered = inner.writer.recover_stale(inner.owner).await?;
            let mut local = inner.local.lock().expect("history local state poisoned");
            local.health.recovered_indeterminate = local
                .health
                .recovered_indeterminate
                .saturating_add(recovered);
        }
        if purge {
            inner.writer.purge_expired(1000).await?;
            let backlog = inner.writer.cleanup_backlog().await?;
            let mut local = inner.local.lock().expect("history local state poisoned");
            local.health.oldest_overdue_ms = backlog.oldest_overdue_ms;
            local.health.last_cleanup_success = Some(Instant::now());
            if backlog.oldest_overdue_ms.is_none() {
                local.health.startup_caught_up = true;
            }
        }
        Ok(())
    })
    .await;
    if let Err(error) = result {
        inner.signal(error.reason());
    }
    let ok = result.is_ok();
    let mut local = inner.local.lock().expect("history local state poisoned");
    local.health.maintenance_ok = ok;
    if ok {
        local.health.writer_ok = true;
    }
    ok
}
async fn retry(inner: &Inner) -> bool {
    let entry = inner
        .local
        .lock()
        .expect("history local state poisoned")
        .queue
        .take();
    let Some(mut entry) = entry else {
        return false;
    };
    if entry.expired() {
        inner
            .local
            .lock()
            .expect("history local state poisoned")
            .queue
            .release(&entry);
        inner.signal(LossReason::RetryExpired);
        if entry.store_failure_observed {
            if let Payload::Observation(o) = entry.payload {
                inner.loss_point(o.event.principal.tenant, false);
            }
        }
        return true;
    }
    let result = bounded_before(
        entry.payload.persist(&inner.writer),
        entry.first_attempt + RETRY_AGE,
    )
    .await;
    inner.write_result(&result);
    let retryable = result.as_ref().err().is_some_and(|e| e.retryable());
    entry.store_failure_observed |= retryable;
    let mut local = inner.local.lock().expect("history local state poisoned");
    local.queue.release(&entry);
    if retryable && !entry.expired() {
        // No wake on failed retry: wait for the next bounded retry interval.
        if !local.queue.push(entry) {
            inner.signal(LossReason::QueueFull);
        }
    } else if retryable {
        drop(local);
        inner.signal(LossReason::RetryExpired);
        if entry.store_failure_observed {
            if let Payload::Observation(o) = entry.payload {
                inner.loss_point(o.event.principal.tenant, false);
            }
        }
    }
    !retryable
}
async fn run(inner: Arc<Inner>) {
    let mut tick = tokio::time::interval(RETRY_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat_due = Instant::now();
    let mut purge_due = Instant::now();
    loop {
        tokio::select! { _ = tick.tick() => {}, _ = inner.wake.notified() => {} }
        // Scan at most 1000 entries, including those behind an unavailable head.
        // Expired payloads are never sent to PostgreSQL on a later retry.
        let expired = inner
            .local
            .lock()
            .expect("history local state poisoned")
            .queue
            .expire();
        for entry in expired {
            inner.signal(LossReason::RetryExpired);
            if entry.store_failure_observed {
                if let Payload::Observation(o) = entry.payload {
                    inner.loss_point(o.event.principal.tenant, false);
                }
            }
        }
        if !inner.closed() {
            let now = Instant::now();
            let heartbeat = now >= heartbeat_due;
            let purge = now >= purge_due;
            if heartbeat || purge {
                let ok = maintain(&inner, heartbeat, purge).await;
                // Failed startup/catch-up is retried, never marked ready.
                if heartbeat {
                    heartbeat_due = Instant::now()
                        + if ok {
                            HEARTBEAT_INTERVAL
                        } else {
                            RETRY_INTERVAL
                        };
                }
                if purge {
                    let local = inner.local.lock().expect("history local state poisoned");
                    purge_due = Instant::now()
                        + if ok
                            && local.health.startup_caught_up
                            && local.health.oldest_overdue_ms.is_none()
                        {
                            PURGE_INTERVAL
                        } else {
                            RETRY_INTERVAL
                        };
                }
            }
        }
        for _ in 0..32 {
            if !retry(&inner).await {
                break;
            }
        }
        let pending = inner
            .local
            .lock()
            .expect("history local state poisoned")
            .queue
            .count;
        if inner.closed() && pending == 0 {
            break;
        }
        // At most 32 entries per wake; failures stop the batch. The fixed tick
        // bounds outage pressure without per-entry tasks or a hot retry loop.
    }
}

#[cfg(test)]
mod tests;
