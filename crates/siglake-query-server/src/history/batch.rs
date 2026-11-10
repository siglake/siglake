//! Internal job lifecycle ownership. No request DTO or caller-supplied context.
use super::*;
use crate::jobs::JobStatus;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

pub(crate) struct BatchObservation {
    handle: Arc<ObservationHandle>,
    state: Mutex<State>,
}
struct State {
    admitted: Option<bool>,
    finished: bool,
    started: Option<Instant>,
    measurements: ExecutionMeasurements,
}
impl BatchObservation {
    pub(crate) fn new(
        handle: Arc<ObservationHandle>,
        started: Option<Instant>,
        job: Uuid,
        admitted: Option<bool>,
    ) -> Self {
        Self {
            handle,
            state: Mutex::new(State {
                admitted,
                finished: false,
                started,
                measurements: ExecutionMeasurements {
                    counters: Counters::default(),
                    duration_ms: None,
                    cache_state: CacheState::Bypass,
                    truncated: None,
                    job_id: Some(job),
                },
            }),
        }
    }
    pub(crate) fn execution_id(&self) -> Option<Uuid> {
        self.handle.receipt().map(|r| r.execution_id())
    }
    /// Called only when the actual run is first polled, not at reservation or 202.
    pub(crate) fn admit(&self) {
        let mut state = self.state.lock().expect("batch history poisoned");
        if !state.finished {
            state.admitted = Some(true);
            let _ = self.handle.admit();
        }
    }
    pub(crate) fn cost(&self, cost: &crate::cost::CostReport) {
        if cost.estimate_complete {
            let mut state = self.state.lock().expect("batch history poisoned");
            state.measurements.counters.estimated_bytes_scanned =
                Counter::new(cost.estimated_bytes_scanned).ok();
            state.measurements.counters.estimated_rows_processed =
                Counter::new(cost.estimated_rows_processed).ok();
        }
    }
    pub(crate) fn records(&self, rows: usize, truncated: bool) {
        let mut state = self.state.lock().expect("batch history poisoned");
        state.measurements.counters.returned_rows =
            u64::try_from(rows).ok().and_then(|v| Counter::new(v).ok());
        state.measurements.truncated = Some(truncated);
        // Batch collection has no complete settled instrumentation snapshot.
        // Never copy default response stats or infer measured scan zeros.
    }
    /// Only AFTER the authoritative job transition. Recovery is uncertainty,
    /// not genuine execution failure, even when the job API calls it failed.
    pub(crate) fn finish(&self, status: JobStatus, recovered: bool) {
        if recovered || !status.is_terminal() {
            return;
        }
        let mut state = self.state.lock().expect("batch history poisoned");
        if state.finished {
            return;
        }
        let Some(admitted) = state.admitted else {
            return;
        };
        let (outcome, error) = match status {
            JobStatus::Succeeded => (Outcome::Success, None),
            JobStatus::Failed => (Outcome::Failed, Some(ErrorCode::Execution)),
            JobStatus::Timeout => (Outcome::TimedOut, Some(ErrorCode::Timeout)),
            JobStatus::Cancelled => (Outcome::Cancelled, Some(ErrorCode::Cancelled)),
            _ => return,
        };
        let mut measurements = state.measurements.clone();
        measurements.duration_ms = state
            .started
            .and_then(|start| u64::try_from(start.elapsed().as_millis()).ok())
            .and_then(|v| Counter::new(v).ok());
        if status != JobStatus::Succeeded {
            measurements.counters.returned_rows = None;
            measurements.truncated = None;
        }
        if let Ok(terminal) = TerminalExecution::new(
            if admitted {
                AdmissionEvidence::Admitted
            } else {
                AdmissionEvidence::NotAdmitted
            },
            outcome,
            error,
            measurements,
        ) {
            state.finished = true;
            let _ = self.handle.finish_queued(terminal);
        }
    }
}
