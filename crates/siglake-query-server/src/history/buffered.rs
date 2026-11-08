//! Buffered-only route ownership and fixed safe evidence. Never inspect bodies,
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
    let Some(observer) = state.history.as_ref().filter(|_| eligible) else {
        return work(state, identity, req).await;
    };
    let Some(principal) = state
        .execution_tenant(&identity)
        .and_then(|tenant| Principal::from_caller(tenant, &identity).ok())
    else {
        return work(state, identity, req).await;
    };
    let handle = observer
        .begin(
            principal,
            RequestMetadata {
                operation,
                format: OutputFormat::Records,
                priority: match req.priority {
                    crate::limits::Priority::Interactive => Priority::Interactive,
                    crate::limits::Priority::Batch => Priority::Batch,
                },
                dry_run: req.dry_run,
            },
            RedactedSql::parse(&req.query),
        )
        .await;
    let handle = std::sync::Arc::new(handle);
    let started = Instant::now();
    let initial = Evidence {
        observation: Some(handle.clone()),
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
    req.format.unwrap_or_default() == crate::format::QueryFormat::Records
        && (req.priority != crate::limits::Priority::Batch || req.dry_run)
        && req.shard.is_none()
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
