//! Typed NDJSON source evidence. No response parsing, request/result ownership,
//! async writes, or spawned tasks. Drop observes delivery abandonment only:
//! it does not prove cancellation, a TCP disconnect, or client receipt.
use super::*;
#[cfg(test)]
mod tests;
use std::{sync::Arc, time::Instant};

pub(crate) struct StreamObservation {
    handle: Arc<ObservationHandle>,
    started: Instant,
    measurements: ExecutionMeasurements,
    selected: bool,
    delivery: Option<DeliveryObservation>,
}
impl StreamObservation {
    pub(super) fn new(
        handle: Arc<ObservationHandle>,
        started: Instant,
        measurements: ExecutionMeasurements,
    ) -> Self {
        Self {
            handle,
            started,
            measurements,
            selected: false,
            delivery: None,
        }
    }
    pub(crate) fn defer_delivery(&mut self) -> DeliveryObservation {
        let delivery = DeliveryObservation(Arc::new(std::sync::Mutex::new(DeliveryState {
            handle: self.handle.clone(),
            started: self.started,
            measurements: self.measurements.clone(),
            pending: None,
            selected: false,
        })));
        self.delivery = Some(delivery.clone());
        delivery
    }
    pub(crate) fn ready(&mut self) {
        self.select(Outcome::Success, None);
    }
    pub(crate) fn complete(
        &mut self,
        rows: usize,
        truncated: bool,
        measured_rows_scanned: Option<u64>,
        measured_bytes_scanned: Option<u64>,
    ) {
        self.measurements.counters.returned_rows =
            u64::try_from(rows).ok().and_then(|n| Counter::new(n).ok());
        self.measurements.truncated = Some(truncated);
        // The source supplies only positively instrumented, settled numeric
        // evidence. Early stops cannot establish complete scan measurements.
        if !truncated {
            if let Some(rows) = measured_rows_scanned {
                self.measurements.counters.measured_rows_scanned = Counter::new(rows).ok();
            }
            if let Some(bytes) = measured_bytes_scanned {
                self.measurements.counters.measured_bytes_scanned = Counter::new(bytes).ok();
            }
        }
        self.select(Outcome::Success, None);
    }
    pub(crate) fn failed(&mut self) {
        self.select(Outcome::Failed, Some(ErrorCode::Execution));
    }
    fn select(&mut self, outcome: Outcome, code: Option<ErrorCode>) {
        if self.selected {
            return;
        }
        self.selected = true;
        self.measurements.duration_ms = u64::try_from(self.started.elapsed().as_millis())
            .ok()
            .and_then(|n| Counter::new(n).ok());
        if let Ok(terminal) = TerminalExecution::new(
            AdmissionEvidence::Admitted,
            outcome,
            code,
            self.measurements.clone(),
        ) {
            if let Some(delivery) = &self.delivery {
                // Controlled source success AND execution-error markers are
                // candidates, not terminal authority. The producer/body arbiter
                // decides delivery/timeout/abandonment at the original deadline.
                // A delivered error marker records query Failed, not Success.
                delivery.0.lock().unwrap().pending = Some(terminal);
            } else {
                let _ = self.handle.finish_queued(terminal);
            }
        }
    }
}
impl Drop for StreamObservation {
    fn drop(&mut self) {
        if !self.selected && self.delivery.is_none() {
            // No complete measurement or engine terminal is implied by drop.
            self.measurements.counters.measured_rows_scanned = None;
            self.measurements.counters.measured_bytes_scanned = None;
            self.measurements.counters.returned_rows = None;
            self.measurements.truncated = None;
            self.select(Outcome::Disconnected, Some(ErrorCode::Disconnected));
        }
    }
}

/// Numeric evidence from selecting a real metadata result. The source owns
/// its existing payload separately; history never receives result bytes.
pub(crate) fn rendered_observation(no_data_scan: bool) -> Option<StreamObservation> {
    super::buffered::admit();
    let mut observation = super::buffered::transfer_stream();
    if no_data_scan {
        if let Some(o) = &mut observation {
            o.measurements.counters.measured_rows_scanned = Counter::new(0).ok();
            o.measurements.counters.measured_bytes_scanned = Counter::new(0).ok();
        }
    }
    observation
}

/// Shared typed delivery gate. Source EOF/error-marker production records only
/// a bounded typed candidate, never a terminal. The body arbiter selects its
/// delivery/timeout/abandonment; no query-controlled bytes are inspected.
#[derive(Clone)]
pub(crate) struct DeliveryObservation(Arc<std::sync::Mutex<DeliveryState>>);
struct DeliveryState {
    handle: Arc<ObservationHandle>,
    started: Instant,
    measurements: ExecutionMeasurements,
    pending: Option<TerminalExecution>,
    selected: bool,
}
impl DeliveryState {
    fn submit(&mut self, terminal: TerminalExecution) {
        if !self.selected {
            self.selected = true;
            let _ = self.handle.finish_queued(terminal);
        }
    }
}
impl DeliveryObservation {
    pub(crate) fn delivered(&self) {
        let mut state = self.0.lock().unwrap();
        if let Some(terminal) = state.pending.take() {
            state.submit(terminal);
        }
    }
    fn end(&self, outcome: Outcome, code: ErrorCode) {
        let mut state = self.0.lock().unwrap();
        let mut measurements = state.measurements.clone();
        measurements.counters.returned_rows = None;
        measurements.counters.measured_rows_scanned = None;
        measurements.counters.measured_bytes_scanned = None;
        measurements.truncated = None;
        measurements.duration_ms = u64::try_from(state.started.elapsed().as_millis())
            .ok()
            .and_then(|v| Counter::new(v).ok());
        if let Ok(terminal) = TerminalExecution::new(
            AdmissionEvidence::Admitted,
            outcome,
            Some(code),
            measurements,
        ) {
            state.submit(terminal);
        }
    }
    pub(crate) fn timed_out(&self) {
        self.end(Outcome::TimedOut, ErrorCode::Timeout);
    }
    pub(crate) fn failed(&self) {
        self.end(Outcome::Failed, ErrorCode::Execution);
    }
    pub(crate) fn abandoned(&self) {
        self.end(Outcome::Disconnected, ErrorCode::Disconnected);
    }
}

impl Drop for DeliveryState {
    fn drop(&mut self) {
        if !self.selected {
            let mut measurements = self.measurements.clone();
            measurements.counters.returned_rows = None;
            measurements.counters.measured_rows_scanned = None;
            measurements.counters.measured_bytes_scanned = None;
            measurements.truncated = None;
            if let Ok(terminal) = TerminalExecution::new(
                AdmissionEvidence::Admitted,
                Outcome::Disconnected,
                Some(ErrorCode::Disconnected),
                measurements,
            ) {
                let _ = self.handle.finish_queued(terminal);
            }
        }
    }
}
