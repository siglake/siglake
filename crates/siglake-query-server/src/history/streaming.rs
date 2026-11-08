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
        }
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
            let _ = self.handle.finish_queued(terminal);
        }
    }
}
impl Drop for StreamObservation {
    fn drop(&mut self) {
        if !self.selected {
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
