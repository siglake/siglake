//! External route ownership and fixed safe evidence. Never inspect bodies,
//! SQL syntax, error messages, provider headers, or arbitrary context JSON.
use super::*;
use crate::{cost::CostReport, error::ApiError, sql::SqlRequest, AppState};
use axum::{
    http::{HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use std::{cell::RefCell, future::Future, time::Instant};

pub(crate) const CORRELATION_HEADER: &str = "x-siglake-history-id";
tokio::task_local! {
    static EVIDENCE: RefCell<Evidence>;
}
struct Evidence {
    observation: Option<std::sync::Arc<ObservationHandle>>,
    started: Option<Instant>,
    transferred: bool,
    effective_metadata: bool,
    admitted: bool,
    completed: bool,
    scan_attribution_complete: bool,
    phase: ErrorCode,
    measurements: ExecutionMeasurements,
}
impl Default for Evidence {
    fn default() -> Self {
        Self {
            observation: None,
            started: None,
            transferred: false,
            effective_metadata: false,
            admitted: false,
            completed: false,
            scan_attribution_complete: true,
            phase: ErrorCode::Validation,
            measurements: ExecutionMeasurements {
                counters: Counters::default(),
                duration_ms: None,
                cache_state: CacheState::Unknown,
                truncated: None,
                job_id: None,
            },
        }
    }
}
fn update(f: impl FnOnce(&mut Evidence)) {
    let _ = EVIDENCE.try_with(|e| f(&mut e.borrow_mut()));
}
pub(crate) fn phase(phase: ErrorCode) {
    update(|e| e.phase = phase);
}
pub(crate) fn admit() {
    update(|e| {
        if !e.admitted {
            if let Some(handle) = &e.observation {
                let _ = handle.admit();
            }
        }
        e.admitted = true;
        e.phase = ErrorCode::Execution;
    });
}
pub(crate) fn cost(cost: &CostReport) {
    update(|e| {
        // Field existence and exactness do not establish availability. A
        // successful heuristic is available; failed/partial estimates are not.
        if cost.estimate_complete {
            e.measurements.counters.estimated_bytes_scanned =
                Counter::new(cost.estimated_bytes_scanned).ok();
            e.measurements.counters.estimated_rows_processed =
                Counter::new(cost.estimated_rows_processed).ok();
        } else {
            e.measurements.counters.estimated_bytes_scanned = None;
            e.measurements.counters.estimated_rows_processed = None;
        }
    });
}
pub(crate) fn cache(cache_state: CacheState) {
    update(|e| e.measurements.cache_state = cache_state);
}
pub(crate) fn incomplete_scan_attribution() {
    update(|e| e.scan_attribution_complete = false);
}
pub(crate) fn records(
    returned_rows: usize,
    truncated: bool,
    measured_rows_scanned: Option<u64>,
    measured_bytes_scanned: Option<u64>,
) {
    update(|e| {
        e.completed = true;
        e.measurements.counters.returned_rows = u64::try_from(returned_rows)
            .ok()
            .and_then(|v| Counter::new(v).ok());
        e.measurements.truncated = Some(truncated);
        // Cached stats describe the original run, NOT this request's reads.
        if e.measurements.cache_state != CacheState::Hit && e.scan_attribution_complete {
            e.measurements.counters.measured_rows_scanned =
                measured_rows_scanned.and_then(|v| Counter::new(v).ok());
            e.measurements.counters.measured_bytes_scanned =
                measured_bytes_scanned.and_then(|v| Counter::new(v).ok());
        }
    });
}
pub(crate) fn planned() {
    update(|e| e.completed = true);
}
/// Typed chosen branch, never inferred from the eventual HTTP response.
pub(crate) fn effective(format: OutputFormat, dry_run: bool) {
    update(|e| {
        if e.effective_metadata {
            if let Some(handle) = &e.observation {
                let _ = handle.select_effective(EffectiveExecution { format, dry_run });
            }
        }
    });
}

/// One external parent. Metadata I/O is outside the pre-existing execution
/// timeout. Duration starts AFTER receipt work and ends BEFORE terminal I/O:
/// buffered handler elapsed, including planning, not pure data execution time.
pub(crate) async fn observe<F, W>(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    operation: Operation,
    eligible: bool,
    work: W,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
    W: FnOnce(AppState, CallerIdentity, SqlRequest) -> F,
{
    let metadata = (
        sql_metadata(&req, operation),
        RedactedSql::parse(&req.query),
    );
    observe_request(state, identity, req, metadata, eligible, false, work).await
}

pub(crate) async fn observe_configured<F, W>(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    work: W,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
    W: FnOnce(AppState, CallerIdentity, SqlRequest) -> F,
{
    let metadata = (
        sql_metadata(&req, Operation::Sql),
        RedactedSql::parse(&req.query),
    );
    // Every extractor-accepted configured SQL request gets its original
    // parent, including existing body-shard and validation-rejection paths.
    observe_request(state, identity, req, metadata, true, true, work).await
}

fn sql_metadata(req: &SqlRequest, operation: Operation) -> RequestMetadata {
    RequestMetadata {
        operation,
        format: if operation != Operation::Explain
            && req.priority != crate::limits::Priority::Batch
            && !req.dry_run
            && req.format == Some(crate::format::QueryFormat::Ndjson)
        {
            OutputFormat::Ndjson
        } else {
            OutputFormat::Records
        },
        priority: match req.priority {
            crate::limits::Priority::Interactive => Priority::Interactive,
            crate::limits::Priority::Batch => Priority::Batch,
        },
        dry_run: req.dry_run,
    }
}

pub(crate) async fn observe_shard<F, W>(
    state: AppState,
    identity: CallerIdentity,
    req: crate::sql::ShardQueryRequest,
    work: W,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
    W: FnOnce(AppState, CallerIdentity, crate::sql::ShardQueryRequest) -> F,
{
    let metadata = RequestMetadata {
        operation: Operation::Shard,
        format: OutputFormat::Arrow,
        priority: match req.priority {
            crate::limits::Priority::Interactive => Priority::Interactive,
            crate::limits::Priority::Batch => Priority::Batch,
        },
        dry_run: false,
    };
    let metadata = (metadata, RedactedSql::parse(&req.query));
    observe_request(state, identity, req, metadata, true, true, work).await
}

async fn observe_request<R, F, W>(
    state: AppState,
    identity: CallerIdentity,
    req: R,
    metadata: (RequestMetadata, RedactedSql),
    eligible: bool,
    effective_metadata: bool,
    work: W,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
    W: FnOnce(AppState, CallerIdentity, R) -> F,
{
    let Some(observer) = state
        .history
        .as_ref()
        .filter(|_| eligible && !identity.coordinator)
    else {
        return work(state, identity, req).await;
    };
    let Some(principal) = state
        .execution_tenant(&identity)
        .and_then(|tenant| Principal::from_caller(tenant, &identity).ok())
    else {
        return work(state, identity, req).await;
    };
    let handle = observer.begin(principal, metadata.0, metadata.1).await;
    let handle = std::sync::Arc::new(handle);
    let started = Instant::now();
    let initial = Evidence {
        observation: Some(handle.clone()),
        started: Some(started),
        effective_metadata,
        ..Evidence::default()
    };
    let (mut result, mut evidence) = EVIDENCE
        .scope(RefCell::new(initial), async {
            let result = work(state, identity, req).await;
            let evidence = EVIDENCE.with(|e| e.take());
            (result, evidence)
        })
        .await;
    evidence.measurements.duration_ms = u64::try_from(started.elapsed().as_millis())
        .ok()
        .and_then(|v| Counter::new(v).ok());
    let terminal = match &result {
        Ok(_) if evidence.transferred => None,
        Ok(_) if evidence.completed && evidence.admitted => Some((Outcome::Success, None)),
        // No status-code inference of successful admission/completion.
        Ok(_) => None,
        Err(err) if err.status == StatusCode::GATEWAY_TIMEOUT => {
            Some((Outcome::TimedOut, Some(ErrorCode::Timeout)))
        }
        Err(err) if !evidence.admitted && err.status == StatusCode::TOO_MANY_REQUESTS => {
            Some((Outcome::Rejected, Some(ErrorCode::Admission)))
        }
        Err(err) if !evidence.admitted && err.status == StatusCode::BAD_REQUEST => {
            Some((Outcome::Rejected, Some(evidence.phase)))
        }
        Err(_) => Some((Outcome::Failed, Some(ErrorCode::Execution))),
    };
    if let Some((outcome, code)) = terminal {
        if let Ok(terminal) = TerminalExecution::new(
            if evidence.admitted {
                AdmissionEvidence::Admitted
            } else {
                AdmissionEvidence::NotAdmitted
            },
            outcome,
            code,
            evidence.measurements,
        ) {
            let _ = handle.finish(terminal).await;
        }
    }
    // A genuine receipt is correlation, not a durability acknowledgement.
    if let Some(receipt) = handle.receipt() {
        if let Ok(value) = HeaderValue::from_str(&receipt.execution_id().to_string()) {
            let name = HeaderName::from_static(CORRELATION_HEADER);
            match &mut result {
                Ok(response) => {
                    response.headers_mut().insert(name, value);
                }
                Err(error) => error.headers.push((name, value)),
            }
        }
    }
    result
}

pub(crate) fn eligible(req: &SqlRequest) -> bool {
    req.shard.is_none()
        && !(req.priority == crate::limits::Priority::Batch
            && req.dry_run
            && req.format.unwrap_or_default() != crate::format::QueryFormat::Records)
}

pub(crate) fn batch_execution_id() -> Option<Uuid> {
    EVIDENCE
        .try_with(|e| {
            e.borrow()
                .observation
                .as_ref()
                .and_then(|h| h.receipt())
                .map(|r| r.execution_id())
        })
        .ok()
        .flatten()
}

pub(crate) fn transfer_batch(job: Uuid) -> Option<std::sync::Arc<super::batch::BatchObservation>> {
    EVIDENCE
        .try_with(|e| {
            let mut e = e.borrow_mut();
            if e.transferred {
                return None;
            }
            let handle = e.observation.take()?;
            e.transferred = true;
            Some(std::sync::Arc::new(super::batch::BatchObservation::new(
                handle,
                e.started,
                job,
                Some(false),
            )))
        })
        .ok()
        .flatten()
}

/// Move the sole lifecycle owner into the source/body, not a task-local clone
/// that would disappear at HTTP-handler return. Only typed admission permits it.
pub(crate) fn transfer_stream() -> Option<super::streaming::StreamObservation> {
    EVIDENCE
        .try_with(|e| {
            let mut e = e.borrow_mut();
            if !e.admitted || e.transferred {
                return None;
            }
            let handle = e.observation.take()?;
            let started = e.started?;
            e.transferred = true;
            Some(super::streaming::StreamObservation::new(
                handle,
                started,
                e.measurements.clone(),
            ))
        })
        .ok()
        .flatten()
}

#[cfg(test)]
pub(crate) async fn capture_test_measurements(
    work: impl Future<Output = ()>,
) -> ExecutionMeasurements {
    EVIDENCE
        .scope(RefCell::new(Evidence::default()), async {
            work.await;
            EVIDENCE.with(|e| e.take().measurements)
        })
        .await
}
