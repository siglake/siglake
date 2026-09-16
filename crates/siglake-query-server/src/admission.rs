//! Query admission control.
//!
//! Admission bounds how many heavy queries can run concurrently on one pod;
//! the storage scan's decoded-bytes reader budget bounds what one admitted
//! query can hold in flight. Neither replaces the other.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::cost::CostReport;

/// Floor for any one query's reservation, and the figure
/// [`AdmissionController::unestimated_reservation_bytes`] hands an interactive
/// request that never produces a cost report. Public because the Jaeger read
/// ceilings are derived from that reservation and need its value as their
/// fallback when admission is disabled (`crate::jaeger_limits`).
pub const MIN_RESERVATION_BYTES: u64 = 16 * 1024 * 1024;

/// Batch jobs are admitted before their asynchronous planning pass can produce
/// a cost report, so reserve a fixed share of the pod budget for each one.
/// Keeping the share below the whole budget prevents one job from excluding
/// every interactive request while still bounding an unauthenticated flood of
/// `priority: "batch"` submissions.
const BATCH_SHARE_DIVISOR: u64 = 4;

/// Largest share of the budget ONE query may reserve, as a divisor.
///
/// Was hardcoded at 2, which made admission a hard TWO-concurrent-query gate for
/// anything heavy: a full scan's heuristic vastly exceeds the budget, so it
/// clamped to `budget / 2` and a third query queued behind them. Measured
/// under 32-way load: ~30 of 32 in-flight queries waiting on a 2-slot gate, each
/// waiting the full timeout and then 429ing — and with a 60s request timeout a
/// deep queue turns into 504s instead.
///
/// Four rather than two because the pool, not admission, is what actually
/// bounds memory now; admission's remaining job is to stop a thundering herd of
/// heavy scans, not to serialise them. Override with
/// `SIGLAKE_QUERY_ADMISSION_MAX_SHARE_DIVISOR`.
fn per_query_share_divisor() -> u64 {
    std::env::var("SIGLAKE_QUERY_ADMISSION_MAX_SHARE_DIVISOR")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(4)
}

#[derive(Debug)]
struct AdmissionState {
    budget_bytes: u64,
    reserved_bytes: u64,
    waiting: usize,
}

#[derive(Debug)]
struct AdmissionInner {
    state: Mutex<AdmissionState>,
    notify: Notify,
    decompression_factor: u64,
    wait_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct AdmissionController {
    inner: Arc<AdmissionInner>,
}

#[derive(Debug)]
pub struct AdmissionGuard {
    inner: Arc<AdmissionInner>,
    reserved_bytes: u64,
}

#[derive(Debug)]
struct WaitingGuard {
    inner: Arc<AdmissionInner>,
}

#[derive(Debug)]
pub struct AdmissionFailure {
    pub retry_after_secs: u64,
}

impl AdmissionController {
    pub fn new(budget_bytes: u64, decompression_factor: u64, wait_timeout: Duration) -> Self {
        let controller = Self {
            inner: Arc::new(AdmissionInner {
                state: Mutex::new(AdmissionState {
                    budget_bytes,
                    reserved_bytes: 0,
                    waiting: 0,
                }),
                notify: Notify::new(),
                decompression_factor: decompression_factor.max(1),
                wait_timeout,
            }),
        };
        controller.update_gauges();
        controller
    }

    pub fn reservation_bytes(&self, cost: &CostReport) -> u64 {
        let budget = self.inner.state.lock().unwrap().budget_bytes.max(1);
        let heuristic = cost
            .estimated_bytes_scanned
            .saturating_mul(self.inner.decompression_factor);
        heuristic
            .max(MIN_RESERVATION_BYTES)
            .min((budget / per_query_share_divisor()).max(1))
            .max(1)
    }

    /// Reservation acquired synchronously by a batch submission.
    ///
    /// Batch planning deliberately stays on the dedicated runtime, so no cost
    /// report exists while the HTTP request is being admitted. A fixed share
    /// keeps submission cheap and makes batch consume the same finite budget as
    /// interactive work. Zero preserves the controller's disabled semantics.
    pub fn batch_reservation_bytes(&self) -> u64 {
        let budget = self.inner.state.lock().unwrap().budget_bytes;
        if budget == 0 {
            0
        } else {
            (budget / BATCH_SHARE_DIVISOR).max(1)
        }
    }

    /// Reservation for an INTERACTIVE request that never produces a cost
    /// report.
    ///
    /// The Jaeger read routes (#2096) are the case: they plan and collect two
    /// fixed query shapes and deliberately do not run `estimate()`, yet they
    /// share this pod's memory pool with `/api/v1/sql`, so they have to consume
    /// the same finite budget rather than a private one. Without an estimate
    /// the honest price is the floor [`Self::reservation_bytes`] gives a query
    /// estimated at nothing — still clamped by the per-query share so an
    /// unestimated request cannot crowd out the estimated ones. Zero preserves
    /// the controller's disabled semantics, as in
    /// [`Self::batch_reservation_bytes`].
    pub fn unestimated_reservation_bytes(&self) -> u64 {
        let budget = self.inner.state.lock().unwrap().budget_bytes;
        if budget == 0 {
            0
        } else {
            MIN_RESERVATION_BYTES
                .min((budget / per_query_share_divisor()).max(1))
                .max(1)
        }
    }

    pub async fn acquire(&self, reserved_bytes: u64) -> Result<AdmissionGuard, AdmissionFailure> {
        // Budget 0 = admission disabled, matching
        // `SIGLAKE_QUERY_MEMORY_POOL_BYTES` where 0 means unbounded. It used to
        // mean "no budget at all", so setting it to zero bricked interactive
        // query entirely — every request waited the full timeout and 429'd
        // while Tier-1 metadata shapes kept answering, which reads exactly like
        // the intermittent degradation already under investigation.
        if self.inner.state.lock().unwrap().budget_bytes == 0 {
            return Ok(AdmissionGuard {
                inner: Arc::clone(&self.inner),
                reserved_bytes: 0,
            });
        }
        let deadline = Instant::now() + self.inner.wait_timeout;
        loop {
            // REGISTER BEFORE CHECKING. `notify_waiters()` only wakes futures
            // already registered, and `Notify::notified()` does not register
            // until first polled — so a release landing between the budget check
            // and the await was LOST, and the waiter slept to its deadline and
            // returned 429 against a budget that was entirely free. Enabling the
            // future first closes that window: a wake arriving from here on is
            // delivered when we do await.
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            {
                let mut state = self.inner.state.lock().unwrap();
                if state.reserved_bytes.saturating_add(reserved_bytes) <= state.budget_bytes {
                    state.reserved_bytes = state.reserved_bytes.saturating_add(reserved_bytes);
                    drop(state);
                    self.update_gauges();
                    return Ok(AdmissionGuard {
                        inner: Arc::clone(&self.inner),
                        reserved_bytes,
                    });
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(AdmissionFailure {
                    retry_after_secs: self.inner.wait_timeout.as_secs().max(1),
                });
            }

            let wait_guard = WaitingGuard::new(Arc::clone(&self.inner));
            let sleep = tokio::time::sleep(deadline.saturating_duration_since(now));
            tokio::pin!(sleep);
            tokio::select! {
                _ = notified => {}
                _ = &mut sleep => {
                    drop(wait_guard);
                    return Err(AdmissionFailure {
                        retry_after_secs: self.inner.wait_timeout.as_secs().max(1),
                    });
                }
            }
            drop(wait_guard);
        }
    }

    fn update_gauges(&self) {
        let state = self.inner.state.lock().unwrap();
        metrics::gauge!("siglake_query_admission_reserved_bytes").set(state.reserved_bytes as f64);
        metrics::gauge!("siglake_query_admission_waiting").set(state.waiting as f64);
    }
}

impl AdmissionGuard {
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
            metrics::gauge!("siglake_query_admission_reserved_bytes")
                .set(state.reserved_bytes as f64);
        }
        self.inner.notify.notify_waiters();
    }
}

impl WaitingGuard {
    fn new(inner: Arc<AdmissionInner>) -> Self {
        {
            let mut state = inner.state.lock().unwrap();
            state.waiting = state.waiting.saturating_add(1);
            metrics::gauge!("siglake_query_admission_waiting").set(state.waiting as f64);
        }
        Self { inner }
    }
}

impl Drop for WaitingGuard {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.waiting = state.waiting.saturating_sub(1);
        metrics::gauge!("siglake_query_admission_waiting").set(state.waiting as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A release must wake a waiter even when it lands in the window between
    /// the budget check and the await.
    ///
    /// THE DEFECT. `notify_waiters()` wakes only futures already registered, and
    /// `Notify::notified()` does not register until first polled — so a release
    /// arriving after the check and before the await was LOST, and the waiter
    /// slept to its deadline and returned 429 against a budget that was
    /// entirely free. Registering (`enable()`) before re-checking closes it.
    ///
    /// PROBABILISTIC, and deliberately so: the window is a few instructions
    /// wide and cannot be hit deterministically without a hook in the loop. The
    /// test runs many trials with the release racing the waiter's first check
    /// and asserts NONE are spuriously rejected; with the wakeup lost, a
    /// rejected trial takes the full `wait_timeout`, so a failure is loud rather
    /// than flaky-looking. If this ever fails intermittently, it is finding the
    /// bug, not being unreliable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_release_racing_the_check_is_not_lost() {
        const TRIALS: usize = 300;
        const UNIT: u64 = 8 * 1024 * 1024;
        for trial in 0..TRIALS {
            let controller = AdmissionController::new(UNIT, 4, Duration::from_millis(250));
            let held = controller.acquire(UNIT).await.unwrap();
            let c = controller.clone();
            let waiter = tokio::spawn(async move { c.acquire(UNIT).await.is_ok() });
            // No sleep: the release races the waiter's first check, which is the
            // window. Yield so the waiter is scheduled and usually mid-loop.
            tokio::task::yield_now().await;
            drop(held);
            let admitted = waiter.await.expect("waiter panicked");
            assert!(
                admitted,
                "trial {trial}: rejected against a budget freed before the wait \
                 expired — the wakeup was lost"
            );
        }
    }

    /// A budget of 0 disables admission rather than rejecting everything.
    ///
    /// It used to mean "no budget", so every interactive query waited the full
    /// timeout and 429'd while Tier-1 metadata shapes kept answering — which
    /// reads exactly like the intermittent degradation already under
    /// investigation. Zero means unbounded in the adjacent memory knob
    /// (`SIGLAKE_QUERY_MEMORY_POOL_BYTES`); it means the same here now.
    #[tokio::test]
    async fn a_zero_budget_disables_admission_instead_of_bricking_it() {
        let controller = AdmissionController::new(0, 4, Duration::from_millis(50));
        for _ in 0..8 {
            controller
                .acquire(1024 * 1024 * 1024)
                .await
                .expect("a zero budget must admit, not reject");
        }
    }

    /// One query must not be able to reserve half the budget, which made
    /// admission a two-concurrent-query gate for anything heavy.
    ///
    /// Measured under 32-way load: ~30 of 32 in-flight queries queued on a
    /// 2-slot gate, each waiting the full timeout then 429ing — and with a 60s
    /// request timeout a deep queue becomes 504s instead.
    #[test]
    fn one_heavy_query_cannot_reserve_half_the_budget() {
        let budget = 1024 * 1024 * 1024;
        let controller = AdmissionController::new(budget, 4, Duration::from_millis(10));
        // A cost far beyond the budget: the clamp is what decides the answer.
        let cost = crate::cost::CostReport {
            files_to_scan: None,
            files_considered: None,
            estimated_bytes_scanned: u64::MAX / 8,
            estimated_rows_processed: 0,
            estimated_runtime_seconds: 0.0,
            complexity_class: crate::cost::ComplexityClass::Huge,
            warnings: vec![],
            exact: false,
        };
        let reserved = controller.reservation_bytes(&cost);
        assert!(
            reserved <= budget / 4,
            "one query reserved {reserved} of a {budget} budget — at most a \
             quarter, or heavy queries serialise"
        );
        assert!(reserved > 0);
    }

    #[tokio::test]
    async fn reservation_waiter_proceeds_after_release() {
        let controller = AdmissionController::new(32 * 1024 * 1024, 4, Duration::from_millis(500));
        let first = controller.acquire(16 * 1024 * 1024).await.unwrap();
        let controller_waiter = controller.clone();
        let waiter = tokio::spawn(async move {
            let guard = controller_waiter.acquire(16 * 1024 * 1024).await.unwrap();
            guard.reserved_bytes()
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(first);
        assert_eq!(waiter.await.unwrap(), 16 * 1024 * 1024);
    }

    #[test]
    fn reservation_uses_floor_and_cap() {
        let controller =
            AdmissionController::new(4 * 1024 * 1024 * 1024, 4, Duration::from_secs(2));
        assert_eq!(
            controller.reservation_bytes(&CostReport {
                files_to_scan: Some(1),
                files_considered: None,
                estimated_bytes_scanned: 1,
                estimated_rows_processed: 1,
                estimated_runtime_seconds: 0.0,
                complexity_class: crate::cost::ComplexityClass::Small,
                warnings: Vec::new(),
                exact: true,
            }),
            MIN_RESERVATION_BYTES
        );
        assert_eq!(
            controller.reservation_bytes(&CostReport {
                files_to_scan: Some(32),
                files_considered: None,
                estimated_bytes_scanned: 512 * 1024 * 1024,
                estimated_rows_processed: 1,
                estimated_runtime_seconds: 0.0,
                complexity_class: crate::cost::ComplexityClass::Medium,
                warnings: Vec::new(),
                exact: true,
            }),
            // The CAP, expressed as the policy rather than a baked constant:
            // was `budget / 2`, which made two heavy queries a full house.
            (4u64 * 1024 * 1024 * 1024) / per_query_share_divisor()
        );
    }

    #[test]
    fn whole_table_estimate_reserves_the_cap() {
        let controller =
            AdmissionController::new(4 * 1024 * 1024 * 1024, 4, Duration::from_secs(2));
        assert_eq!(
            controller.reservation_bytes(&CostReport {
                files_to_scan: Some(64),
                files_considered: None,
                estimated_bytes_scanned: 2 * 1024 * 1024 * 1024,
                estimated_rows_processed: 1,
                estimated_runtime_seconds: 0.0,
                complexity_class: crate::cost::ComplexityClass::Large,
                warnings: Vec::new(),
                exact: true,
            }),
            // The CAP, as the policy rather than a baked constant: it was
            // `budget / 2`, which made two heavy queries a full house and put
            // ~30 of 32 concurrent queries into a 2-slot queue.
            (4u64 * 1024 * 1024 * 1024) / per_query_share_divisor()
        );
    }

    /// An uncosted interactive reservation (#2096, the Jaeger routes) is the
    /// same floor a cost-estimated tiny query gets, never the whole budget —
    /// and it is still clamped by the per-query share on a small pod.
    #[test]
    fn an_uncosted_interactive_request_reserves_the_floor() {
        let budget = 4 * 1024 * 1024 * 1024;
        let controller = AdmissionController::new(budget, 4, Duration::from_secs(2));
        assert_eq!(
            controller.unestimated_reservation_bytes(),
            MIN_RESERVATION_BYTES
        );

        // A pod whose whole budget is under the floor: the share clamp decides,
        // so an unestimated request cannot occupy the entire budget by itself
        // and 429 every estimated query behind it.
        let small = AdmissionController::new(8 * 1024 * 1024, 4, Duration::from_secs(2));
        assert_eq!(
            small.unestimated_reservation_bytes(),
            (8 * 1024 * 1024) / per_query_share_divisor()
        );

        let disabled = AdmissionController::new(0, 4, Duration::from_secs(2));
        assert_eq!(disabled.unestimated_reservation_bytes(), 0);
    }

    #[test]
    fn batch_reserves_a_bounded_fixed_share() {
        let budget = 64 * 1024 * 1024;
        let controller = AdmissionController::new(budget, 4, Duration::from_secs(2));
        assert_eq!(controller.batch_reservation_bytes(), budget / 4);

        let disabled = AdmissionController::new(0, 4, Duration::from_secs(2));
        assert_eq!(disabled.batch_reservation_bytes(), 0);
    }
}
