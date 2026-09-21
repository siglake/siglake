//! Bounded, best-effort append-only audit log for completed queries.
//!
//! One row per query — interactive and batch alike — captured at
//! terminal state with the resolved cost report, the caller identity
//! (subject + email from OIDC, "bearer" / "anonymous" otherwise), and
//! the final outcome. Rows are buffered in an mpsc channel and a
//! background task flushes them in batches to the
//! `siglake.query_audit` Iceberg table.
//!
//! Sized for log-analytics traffic: at 1k QPS and a 5 s flush
//! interval, each commit is ~5 k rows. Failures are logged but never
//! propagate to the query path — audit is best-effort. Each append is
//! bounded by [`DEFAULT_AUDIT_APPEND_DEADLINE`], so a storage append that
//! stops answering costs its own batch and not the service: the rows are
//! dropped whole, their retained budget is released, and the worker takes
//! the next ones.

use std::mem::size_of;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc::{self, Receiver, Sender};
#[cfg(test)]
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use siglake_core::audit_schema::query_audit_schema;
use siglake_storage::iceberg::IcebergContext;

use crate::cost::ComplexityClass;
use crate::limits::Priority;

/// Default process-local ceilings for audit rows retained by the submit
/// channel, flush buffer, and Arrow conversion working set.
pub const DEFAULT_AUDIT_LIMITS: AuditLimits = AuditLimits {
    max_rows: 10_000,
    max_bytes: 64 * 1024 * 1024,
};

/// Ceiling on ONE append, from submit to the catalog commit's answer. A
/// storage append that never returns would otherwise hold the whole retained
/// budget — the flush buffer keeps its rows charged for as long as the worker
/// awaits — and the audit service would stop serving rows for the life of the
/// process. 30 s is the same outer bound the batch path gives its terminal
/// write; a 5 k-row audit commit is far inside it.
pub const DEFAULT_AUDIT_APPEND_DEADLINE: Duration = Duration::from_secs(30);

/// Conservative per-row allowance for the bounded-channel slot, flush-vector
/// slot, Arrow offsets/validity buffers, array objects, and RecordBatch fields.
/// Variable-width Arrow data is charged separately from this allowance.
const ARROW_AND_QUEUE_OVERHEAD_BYTES: usize = 256;
/// Arrow buffers grow geometrically. Charging twice the copied string lengths
/// covers the live data plus growth slack when a builder exceeds its initial
/// per-row reservation.
const ARROW_STRING_GROWTH_MULTIPLIER: usize = 2;

/// Process-local audit retention ceilings. Values below one are normalized to
/// one because Tokio bounded channels require positive capacity.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AuditLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for AuditLimits {
    fn default() -> Self {
        DEFAULT_AUDIT_LIMITS
    }
}

impl AuditLimits {
    fn normalized(self) -> Self {
        Self {
            max_rows: self.max_rows.max(1),
            max_bytes: self.max_bytes.max(1),
        }
    }
}

/// Current charged audit retention, including the estimated Arrow conversion
/// copy for every accepted row.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AuditUsage {
    pub rows: usize,
    pub bytes: usize,
}

/// Single audit-row payload. Builders fill this in at query terminal
/// state and pass it to [`AuditWriter::submit`].
#[derive(Debug, Clone)]
pub struct AuditRow {
    pub timestamp: DateTime<Utc>,
    pub subject: String,
    pub email: Option<String>,
    pub endpoint: &'static str,
    pub query: String,
    pub format: Option<&'static str>,
    pub priority: Priority,
    pub duration_ms: i64,
    pub status: AuditStatus,
    pub complexity: Option<ComplexityClass>,
    pub estimated_bytes_scanned: Option<u64>,
    pub estimated_rows_processed: Option<u64>,
    pub truncated: bool,
    pub error: Option<String>,
}

impl AuditRow {
    /// Charge both the owned row and the overlapping Arrow conversion. String
    /// capacities cover every owned allocation; lengths cover the copied Arrow
    /// values, including labels held as static strings or enums in the row.
    fn retained_charge(&self) -> usize {
        let owned_strings = self.subject.capacity()
            + self.email.as_ref().map_or(0, String::capacity)
            + self.query.capacity()
            + self.error.as_ref().map_or(0, String::capacity);
        let arrow_strings = self.subject.len()
            + self.email.as_ref().map_or(0, String::len)
            + self.endpoint.len()
            + self.query.len()
            + self.format.map_or(0, str::len)
            + self.priority.label().len()
            + self.status.label().len()
            + self.complexity.map_or(0, |value| value.label().len())
            + self.error.as_ref().map_or(0, String::len);
        size_of::<Self>()
            .saturating_add(owned_strings)
            .saturating_add(arrow_strings.saturating_mul(ARROW_STRING_GROWTH_MULTIPLIER))
            .saturating_add(ARROW_AND_QUEUE_OVERHEAD_BYTES)
    }
}

/// Final state captured for the audit row. Matches the HTTP-status
/// categories the handlers can emit.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuditStatus {
    /// 2xx with a complete result.
    Succeeded,
    /// 413 — row cap tripped, result truncated.
    Truncated,
    /// 504 — wall-clock timeout breaker.
    Timeout,
    /// 400 — pre-flight rejection or query parse/plan failure; 429 and 503 —
    /// the server declined to run a sound query (admission budget, memory pool).
    Rejected,
    /// 500 — internal error.
    Failed,
    /// Batch only — `DELETE /api/v1/jobs/<id>` while in-flight.
    Cancelled,
}

impl AuditStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Truncated => "truncated",
            Self::Timeout => "timeout",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Submit handle. Cloneable; cheap to hold across requests.
#[derive(Clone, Debug)]
pub struct AuditWriter {
    tx: AuditSender,
    accounting: Arc<AuditAccounting>,
}

#[derive(Clone, Debug)]
enum AuditSender {
    Bounded(Sender<AccountedAuditRow>),
    #[cfg(test)]
    UnboundedTest(UnboundedSender<AuditRow>),
}

#[derive(Debug)]
struct AuditAccounting {
    limits: AuditLimits,
    rows: AtomicUsize,
    bytes: AtomicUsize,
}

#[derive(Debug)]
struct AccountedAuditRow {
    row: AuditRow,
    charge: usize,
    accounting: Arc<AuditAccounting>,
}

impl Deref for AccountedAuditRow {
    type Target = AuditRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl Drop for AccountedAuditRow {
    fn drop(&mut self) {
        self.accounting.rows.fetch_sub(1, Ordering::AcqRel);
        self.accounting
            .bytes
            .fetch_sub(self.charge, Ordering::AcqRel);
        metrics::gauge!("siglake_query_audit_retained_rows").decrement(1.0);
        metrics::gauge!("siglake_query_audit_retained_bytes").decrement(self.charge as f64);
    }
}

#[derive(Debug, Copy, Clone)]
enum Refusal {
    Oversized,
    RowLimit,
    ByteLimit,
}

impl Refusal {
    fn label(self) -> &'static str {
        match self {
            Self::Oversized => "oversized",
            Self::RowLimit => "row_limit",
            Self::ByteLimit => "byte_limit",
        }
    }
}

impl AuditAccounting {
    fn new(limits: AuditLimits) -> Self {
        Self {
            limits: limits.normalized(),
            rows: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        }
    }

    fn try_reserve(self: &Arc<Self>, row: AuditRow) -> Result<AccountedAuditRow, Refusal> {
        let charge = row.retained_charge();
        if charge > self.limits.max_bytes {
            return Err(Refusal::Oversized);
        }
        self.rows
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |rows| {
                (rows < self.limits.max_rows).then_some(rows + 1)
            })
            .map_err(|_| Refusal::RowLimit)?;
        if self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |bytes| {
                bytes
                    .checked_add(charge)
                    .filter(|next| *next <= self.limits.max_bytes)
            })
            .is_err()
        {
            self.rows.fetch_sub(1, Ordering::AcqRel);
            return Err(Refusal::ByteLimit);
        }
        metrics::gauge!("siglake_query_audit_retained_rows").increment(1.0);
        metrics::gauge!("siglake_query_audit_retained_bytes").increment(charge as f64);
        Ok(AccountedAuditRow {
            row,
            charge,
            accounting: self.clone(),
        })
    }

    fn usage(&self) -> AuditUsage {
        AuditUsage {
            rows: self.rows.load(Ordering::Acquire),
            bytes: self.bytes.load(Ordering::Acquire),
        }
    }
}

fn record_drop(reason: &'static str) {
    record_drops(reason, 1);
}

fn record_drops(reason: &'static str, rows: usize) {
    metrics::counter!("siglake_query_audit_dropped_total", "reason" => reason)
        .increment(rows as u64);
}

/// Ceiling on one audit append, from `SIGLAKE_QUERY_AUDIT_APPEND_DEADLINE_SECS`.
fn audit_append_deadline() -> Duration {
    audit_append_deadline_from(
        std::env::var("SIGLAKE_QUERY_AUDIT_APPEND_DEADLINE_SECS")
            .ok()
            .as_deref(),
    )
}

/// Resolve the append deadline from its raw environment value. `0` restores the
/// unbounded await; an absent or unparseable value takes
/// [`DEFAULT_AUDIT_APPEND_DEADLINE`].
///
/// Pure so the default, the disable value and an invalid one can be tested
/// without mutating process-global state observed by parallel tests in this
/// binary.
fn audit_append_deadline_from(configured: Option<&str>) -> Duration {
    configured
        .and_then(|raw| raw.parse::<u64>().ok())
        .map_or(DEFAULT_AUDIT_APPEND_DEADLINE, Duration::from_secs)
}

impl AuditWriter {
    /// Non-blocking, whole-row submit. Rows that exceed either retention
    /// ceiling, find the bounded channel full, or arrive after worker shutdown
    /// are dropped with a reason-labelled metric.
    pub fn submit(&self, row: AuditRow) {
        match &self.tx {
            AuditSender::Bounded(tx) => {
                let row = match self.accounting.try_reserve(row) {
                    Ok(row) => row,
                    Err(reason) => {
                        record_drop(reason.label());
                        return;
                    }
                };
                match tx.try_send(row) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_row)) => record_drop("channel_full"),
                    Err(mpsc::error::TrySendError::Closed(_row)) => {
                        record_drop("worker_shutdown");
                    }
                }
            }
            #[cfg(test)]
            AuditSender::UnboundedTest(tx) => {
                let _ = tx.send(row);
            }
        }
    }

    /// Current charged rows and bytes, for operational checks and tests.
    pub fn retained_usage(&self) -> AuditUsage {
        self.accounting.usage()
    }

    pub fn limits(&self) -> AuditLimits {
        self.accounting.limits
    }

    /// Test-only: a writer whose rows the test reads straight off the channel.
    ///
    /// The rows a caller submits are the rows the flush worker appends, so a
    /// test that is asking WHAT a code path audits has no business standing up
    /// a warehouse and an `AuditService` to read them back out of Iceberg.
    #[cfg(test)]
    pub(crate) fn for_test() -> (Self, UnboundedReceiver<AuditRow>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx: AuditSender::UnboundedTest(tx),
                accounting: Arc::new(AuditAccounting::new(DEFAULT_AUDIT_LIMITS)),
            },
            rx,
        )
    }
}

/// Destination used by the audit worker. The trait keeps append behavior
/// independently testable while production uses [`IcebergContext`].
#[async_trait]
pub trait AuditAppender: Send + Sync {
    async fn append_query_audit(&self, batch: RecordBatch) -> Result<usize>;
}

#[async_trait]
impl AuditAppender for IcebergContext {
    async fn append_query_audit(&self, batch: RecordBatch) -> Result<usize> {
        IcebergContext::append_query_audit(self, batch).await
    }
}

/// Background drain + flush worker. Spawn one of these per
/// query-server process and hold the returned [`AuditWriter`] in
/// `AppState`.
pub struct AuditService {
    rx: Receiver<AccountedAuditRow>,
    appender: Arc<dyn AuditAppender>,
    buffer: Vec<AccountedAuditRow>,
    flush_every: usize,
    flush_interval: Duration,
    append_deadline: Duration,
}

impl AuditService {
    /// Build the service + the matching submit handle. Spawn the
    /// returned future onto a tokio runtime.
    pub fn new(
        ice: Arc<IcebergContext>,
        flush_every: usize,
        flush_interval: Duration,
    ) -> (Self, AuditWriter) {
        Self::with_limits(ice, flush_every, flush_interval, DEFAULT_AUDIT_LIMITS)
    }

    pub fn with_limits(
        appender: Arc<dyn AuditAppender>,
        flush_every: usize,
        flush_interval: Duration,
        limits: AuditLimits,
    ) -> (Self, AuditWriter) {
        let limits = limits.normalized();
        let (tx, rx) = mpsc::channel(limits.max_rows);
        let accounting = Arc::new(AuditAccounting::new(limits));
        (
            Self {
                rx,
                appender,
                buffer: Vec::with_capacity(flush_every.max(64).min(limits.max_rows)),
                flush_every: flush_every.max(1),
                flush_interval,
                append_deadline: audit_append_deadline(),
            },
            AuditWriter {
                tx: AuditSender::Bounded(tx),
                accounting,
            },
        )
    }

    /// Defaults: 100 rows or 5 s, whichever comes first.
    pub fn with_defaults(ice: Arc<IcebergContext>) -> (Self, AuditWriter) {
        Self::new(ice, 100, Duration::from_secs(5))
    }

    /// Override the per-append service deadline resolved from the environment.
    /// [`Duration::ZERO`] awaits the append with no bound.
    pub fn with_append_deadline(mut self, deadline: Duration) -> Self {
        self.append_deadline = deadline;
        self
    }

    pub async fn run(mut self) {
        let mut tick = tokio::time::interval(self.flush_interval);
        // Skip the immediate first tick so a queued row doesn't get
        // flushed before any submitter has had a chance to push.
        tick.tick().await;
        loop {
            tokio::select! {
                row = self.rx.recv() => match row {
                    Some(r) => {
                        self.buffer.push(r);
                        if self.buffer.len() >= self.flush_every {
                            self.flush().await;
                        }
                    }
                    None => {
                        // Sender side dropped — flush + exit.
                        self.flush().await;
                        return;
                    }
                },
                _ = tick.tick() => {
                    if !self.buffer.is_empty() {
                        self.flush().await;
                    }
                }
            }
        }
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let count = self.buffer.len();
        let batch = match build_audit_batch(&self.buffer) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, rows = count, "audit: build batch failed");
                self.buffer.clear();
                metrics::counter!("siglake_query_audit_failures_total",
                    "reason" => "build")
                .increment(1);
                return;
            }
        };
        match self.append(batch).await {
            Some(Ok(n)) => {
                tracing::debug!(rows = n, "audit: appended");
                metrics::counter!("siglake_query_audit_rows_total").increment(n as u64);
            }
            Some(Err(e)) => {
                tracing::warn!(error = %e, rows = count, "audit: iceberg append failed");
                metrics::counter!("siglake_query_audit_failures_total",
                    "reason" => "append")
                .increment(1);
                record_drops("append", count);
            }
            None => {
                tracing::warn!(
                    rows = count,
                    deadline_secs = self.append_deadline.as_secs_f64(),
                    "audit: append exceeded its service deadline; the batch is abandoned and \
                     will not be retried, because the commit may have landed"
                );
                metrics::counter!("siglake_query_audit_failures_total",
                    "reason" => "append_deadline")
                .increment(1);
                record_drops("append_deadline", count);
            }
        }
        self.buffer.clear();
    }

    /// Await one append under the service deadline; `None` is the deadline
    /// firing.
    ///
    /// What the deadline owns is THIS AWAIT, not the append's effects. Dropping
    /// the future stops the append at whatever it was awaiting — data files it
    /// already uploaded stay in the warehouse for orphan GC, and a commit whose
    /// catalog write had already gone out can still land, unseen by this
    /// worker. That is why the abandoned batch is dropped rather than retried:
    /// re-appending rows whose commit may have succeeded would duplicate them.
    /// The one piece of work the drop does not stop is the CPU-bound aggregate
    /// pass the append hands to `spawn_blocking`, which finishes on its own
    /// over a batch this worker sized. Nothing detached is created here.
    async fn append(&self, batch: RecordBatch) -> Option<Result<usize>> {
        let append = self.appender.append_query_audit(batch);
        if self.append_deadline.is_zero() {
            return Some(append.await);
        }
        tokio::time::timeout(self.append_deadline, append)
            .await
            .ok()
    }
}

fn build_audit_batch(rows: &[AccountedAuditRow]) -> Result<RecordBatch> {
    let n = rows.len();
    let mut ts = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("+00:00");
    let mut subject = StringBuilder::with_capacity(n, n * 16);
    let mut email = StringBuilder::with_capacity(n, n * 16);
    let mut endpoint = StringBuilder::with_capacity(n, n * 4);
    let mut query = StringBuilder::with_capacity(n, n * 64);
    let mut format = StringBuilder::with_capacity(n, n * 8);
    let mut priority = StringBuilder::with_capacity(n, n * 12);
    let mut duration_ms = Int64Builder::with_capacity(n);
    let mut status = StringBuilder::with_capacity(n, n * 12);
    let mut complexity = StringBuilder::with_capacity(n, n * 8);
    let mut est_bytes = Int64Builder::with_capacity(n);
    let mut est_rows = Int64Builder::with_capacity(n);
    let mut truncated = BooleanBuilder::with_capacity(n);
    let mut error = StringBuilder::with_capacity(n, n * 64);

    for r in rows {
        ts.append_value(r.timestamp.timestamp_micros());
        subject.append_value(&r.subject);
        match &r.email {
            Some(e) => email.append_value(e),
            None => email.append_null(),
        }
        endpoint.append_value(r.endpoint);
        query.append_value(&r.query);
        match r.format {
            Some(f) => format.append_value(f),
            None => format.append_null(),
        }
        priority.append_value(r.priority.label());
        duration_ms.append_value(r.duration_ms);
        status.append_value(r.status.label());
        match r.complexity {
            Some(c) => complexity.append_value(c.label()),
            None => complexity.append_null(),
        }
        match r.estimated_bytes_scanned {
            Some(b) => est_bytes.append_value(b as i64),
            None => est_bytes.append_null(),
        }
        match r.estimated_rows_processed {
            Some(n) => est_rows.append_value(n as i64),
            None => est_rows.append_null(),
        }
        truncated.append_value(r.truncated);
        match &r.error {
            Some(e) => error.append_value(e),
            None => error.append_null(),
        }
    }

    let schema = query_audit_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts.finish()),
            Arc::new(subject.finish()),
            Arc::new(email.finish()),
            Arc::new(endpoint.finish()),
            Arc::new(query.finish()),
            Arc::new(format.finish()),
            Arc::new(priority.finish()),
            Arc::new(duration_ms.finish()),
            Arc::new(status.finish()),
            Arc::new(complexity.finish()),
            Arc::new(est_bytes.finish()),
            Arc::new(est_rows.finish()),
            Arc::new(truncated.finish()),
            Arc::new(error.finish()),
        ],
    )
    .context("assemble audit RecordBatch")
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Mutex;

    use arrow_array::{Array, StringArray};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::sync::Notify;
    use tokio::time::Instant;

    use super::*;

    fn row() -> AuditRow {
        AuditRow {
            timestamp: Utc::now(),
            subject: String::new(),
            email: None,
            endpoint: "sql",
            query: String::new(),
            format: None,
            priority: Priority::Interactive,
            duration_ms: 0,
            status: AuditStatus::Rejected,
            complexity: None,
            estimated_bytes_scanned: None,
            estimated_rows_processed: None,
            truncated: false,
            error: None,
        }
    }

    fn row_from(subject: &str) -> AuditRow {
        AuditRow {
            subject: subject.to_string(),
            ..row()
        }
    }

    /// Appender whose FIRST call never resolves, and whose later calls record
    /// the subjects they persisted. The held call is the pending append the
    /// deadline has to cut; the recorded subjects are how a test tells a
    /// retried batch from a fresh one.
    #[derive(Default)]
    struct PendingThenAppends {
        calls: AtomicUsize,
        first_started: Notify,
        appended: Mutex<Vec<String>>,
    }

    impl PendingThenAppends {
        fn appended(&self) -> Vec<String> {
            self.appended.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AuditAppender for PendingThenAppends {
        async fn append_query_audit(&self, batch: RecordBatch) -> Result<usize> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                self.first_started.notify_one();
                pending::<()>().await;
                unreachable!("the held append never resolves");
            }
            let subjects = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("subject column")
                .iter()
                .map(|value| value.unwrap_or_default().to_string())
                .collect::<Vec<_>>();
            let rows = subjects.len();
            self.appended.lock().unwrap().extend(subjects);
            Ok(rows)
        }
    }

    #[derive(Default)]
    struct FailingAppender {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl AuditAppender for FailingAppender {
        async fn append_query_audit(&self, _batch: RecordBatch) -> Result<usize> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            anyhow::bail!("fixture append failure")
        }
    }

    fn service_for(
        appender: Arc<dyn AuditAppender>,
        flush_every: usize,
    ) -> (AuditService, AuditWriter) {
        AuditService::with_limits(
            appender,
            flush_every,
            // Long enough that the interval tick never flushes on its own:
            // every flush in these tests is the one the test asked for.
            Duration::from_secs(3600),
            AuditLimits {
                max_rows: 4,
                max_bytes: 1024 * 1024,
            },
        )
    }

    #[tokio::test]
    async fn a_failed_append_counts_every_abandoned_row_as_dropped() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        let appender = Arc::new(FailingAppender::default());
        let (service, writer) = service_for(appender.clone(), 3);
        let worker = tokio::spawn(service.run());

        writer.submit(row());
        writer.submit(row());
        writer.submit(row());
        for _ in 0..1000 {
            if appender.calls.load(Ordering::Acquire) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(appender.calls.load(Ordering::Acquire), 1);

        drop(writer);
        worker.await.unwrap();

        let mut dropped = None;
        let mut failures = None;
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if !key
                .key()
                .labels()
                .any(|label| label.key() == "reason" && label.value() == "append")
            {
                continue;
            }
            let DebugValue::Counter(value) = value else {
                continue;
            };
            match key.key().name() {
                "siglake_query_audit_dropped_total" => dropped = Some(value),
                "siglake_query_audit_failures_total" => failures = Some(value),
                _ => {}
            }
        }
        assert_eq!(dropped, Some(3));
        assert_eq!(failures, Some(1));
    }

    /// Wait, without advancing the clock, for the worker to record an append.
    async fn await_appended(appender: &PendingThenAppends, min: usize) -> Vec<String> {
        for _ in 0..1000 {
            let appended = appender.appended();
            if appended.len() >= min {
                return appended;
            }
            tokio::task::yield_now().await;
        }
        panic!("the worker never appended {min} row(s)");
    }

    #[test]
    fn the_append_deadline_resolves_its_default_disable_and_invalid_values() {
        assert_eq!(
            audit_append_deadline_from(None),
            DEFAULT_AUDIT_APPEND_DEADLINE
        );
        assert_eq!(
            audit_append_deadline_from(Some("")),
            DEFAULT_AUDIT_APPEND_DEADLINE
        );
        assert_eq!(
            audit_append_deadline_from(Some("half a minute")),
            DEFAULT_AUDIT_APPEND_DEADLINE
        );
        assert_eq!(
            audit_append_deadline_from(Some("-1")),
            DEFAULT_AUDIT_APPEND_DEADLINE
        );
        assert_eq!(
            audit_append_deadline_from(Some("45")),
            Duration::from_secs(45)
        );
        assert!(audit_append_deadline_from(Some("0")).is_zero());
    }

    /// An append that never returns is abandoned at the deadline: its retained
    /// rows are released, the worker serves the next row, and the abandoned
    /// batch is never resubmitted — its commit may have landed.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_append_is_cut_at_its_deadline_and_the_worker_keeps_serving() {
        let appender = Arc::new(PendingThenAppends::default());
        let deadline = Duration::from_secs(10);
        let (service, writer) = service_for(appender.clone(), 1);
        let worker = tokio::spawn(service.with_append_deadline(deadline).run());

        writer.submit(row_from("stalled"));
        appender.first_started.notified().await;
        // Still charged: the flush buffer holds the row for as long as the
        // append is awaited. Asserted before the next await, so the
        // auto-advancing clock cannot cross the deadline first.
        assert_eq!(writer.retained_usage().rows, 1);

        tokio::time::sleep(deadline + Duration::from_secs(1)).await;
        assert_eq!(writer.retained_usage(), AuditUsage { rows: 0, bytes: 0 });

        writer.submit(row_from("after"));
        assert_eq!(await_appended(&appender, 1).await, ["after"]);
        assert_eq!(
            appender.calls.load(Ordering::Acquire),
            2,
            "the abandoned batch was retried"
        );

        drop(writer);
        worker.await.unwrap();
    }

    /// The pre-deadline behaviour, as the negative control for the test above:
    /// with the deadline disabled the same stalled append holds its row for as
    /// long as the process runs.
    #[tokio::test(start_paused = true)]
    async fn the_disable_value_leaves_a_stalled_append_holding_its_rows() {
        let appender = Arc::new(PendingThenAppends::default());
        let (service, writer) = service_for(appender.clone(), 1);
        let worker = tokio::spawn(service.with_append_deadline(Duration::ZERO).run());

        writer.submit(row_from("stalled"));
        appender.first_started.notified().await;
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert_eq!(writer.retained_usage().rows, 1);

        worker.abort();
        let _ = worker.await;
    }

    /// A finite queue whose last append stalls does not hold shutdown open: the
    /// closing flush is bounded by the same deadline, and the worker's
    /// accounting is released when it exits.
    #[tokio::test(start_paused = true)]
    async fn shutdown_of_a_finite_queue_is_bounded_by_the_append_deadline() {
        let appender = Arc::new(PendingThenAppends::default());
        let deadline = Duration::from_secs(10);
        // Batch larger than the queue, so the only flush is the closing one.
        let (service, writer) = service_for(appender.clone(), 8);
        let worker = tokio::spawn(service.with_append_deadline(deadline).run());

        writer.submit(row_from("queued"));
        let accounting = writer.accounting.clone();
        let started = Instant::now();
        drop(writer);

        tokio::time::timeout(Duration::from_secs(600), worker)
            .await
            .expect("the worker never exited")
            .unwrap();
        let waited = started.elapsed();
        assert!(
            waited >= deadline && waited < deadline + Duration::from_secs(1),
            "shutdown took {waited:?}, not one deadline"
        );
        assert_eq!(accounting.usage(), AuditUsage { rows: 0, bytes: 0 });
        assert!(appender.appended().is_empty());
    }

    #[test]
    fn retained_charge_counts_every_owned_string_capacity() {
        let baseline = row().retained_charge();

        let mut subject = row();
        subject.subject = String::with_capacity(1024);
        assert!(subject.retained_charge() >= baseline + 1024);

        let mut email = row();
        email.email = Some(String::with_capacity(1024));
        assert!(email.retained_charge() >= baseline + 1024);

        let mut query = row();
        query.query = String::with_capacity(1024);
        assert!(query.retained_charge() >= baseline + 1024);

        let mut error = row();
        error.error = Some(String::with_capacity(1024));
        assert!(error.retained_charge() >= baseline + 1024);
    }

    #[test]
    fn combined_rows_stop_at_the_byte_ceiling_and_release_after_drop() {
        let charge = row().retained_charge();
        let accounting = Arc::new(AuditAccounting::new(AuditLimits {
            max_rows: 10,
            max_bytes: charge + 1,
        }));
        let retained = accounting.try_reserve(row()).unwrap();
        assert_eq!(accounting.usage().rows, 1);
        assert!(matches!(
            accounting.try_reserve(row()),
            Err(Refusal::ByteLimit)
        ));
        drop(retained);
        assert_eq!(accounting.usage(), AuditUsage { rows: 0, bytes: 0 });
    }

    #[test]
    fn every_audit_drop_reason_is_preregistered() {
        let mut reasons: Vec<&str> = siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
            .iter()
            .filter(|counter| counter.name == "siglake_query_audit_dropped_total")
            .flat_map(|counter| counter.series)
            .map(|series| {
                assert_eq!(series.len(), 1);
                assert_eq!(series[0].0, "reason");
                series[0].1
            })
            .collect();
        reasons.sort_unstable();
        assert_eq!(
            reasons,
            [
                "append",
                "append_deadline",
                "byte_limit",
                "channel_full",
                "oversized",
                "row_limit",
                "worker_shutdown",
            ]
        );
    }
}
