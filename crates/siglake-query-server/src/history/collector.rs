//! Standalone owner: bounded preflight, then the unchanged single runtime owner.
use super::{
    HistoryConfig, HistoryError, HistoryObserver, HistoryService, HistoryWriter, WRITE_BUDGET,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, timeout, timeout_at, Instant},
};

const ATTEMPTS: u8 = 8;
const CONNECT_BUDGET: Duration = Duration::from_secs(2);

/// Aggregate operational status only. Never SQL liveness/readiness authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectorStatus {
    Disabled,
    Starting,
    Unavailable,
    Rejected,
    /// Startup budget exhausted; restart required, not a healthy disablement.
    ExhaustedUnavailable,
    ExhaustedRejected,
    Active,
    Degraded,
    Closed,
}
struct State {
    status: CollectorStatus,
    observer: Option<HistoryObserver>,
}
/// Non-owning managed facade. An unavailable collector has NO runtime observer.
#[derive(Clone)]
pub struct HistoryObserverSource(Source);
#[derive(Clone)]
enum Source {
    Fixed(HistoryObserver),
    Managed(Arc<Mutex<State>>),
}
impl From<HistoryObserver> for HistoryObserverSource {
    fn from(observer: HistoryObserver) -> Self {
        Self(Source::Fixed(observer))
    }
}
impl HistoryObserverSource {
    pub(crate) fn current(&self) -> Option<HistoryObserver> {
        match &self.0 {
            Source::Fixed(observer) => Some(observer.clone()),
            Source::Managed(state) => state.lock().unwrap().observer.clone(),
        }
    }
}

/// Sole owner of the startup task and, after preflight, its HistoryService.
/// Observers never own tasks. Drop aborts; shutdown retains the join handle
/// across cancellation/deadlines until completion is actually consumed.
pub struct HistoryCollector {
    state: Arc<Mutex<State>>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<bool>>,
}
impl HistoryCollector {
    pub fn start(config: Option<HistoryConfig>) -> Self {
        let state = Arc::new(Mutex::new(State {
            status: if config.is_some() {
                CollectorStatus::Starting
            } else {
                CollectorStatus::Disabled
            },
            observer: None,
        }));
        let (stop, receiver) = watch::channel(false);
        let task = config.map(|config| tokio::spawn(run(config, state.clone(), receiver)));
        Self { state, stop, task }
    }
    pub fn observer_source(&self) -> HistoryObserverSource {
        HistoryObserverSource(Source::Managed(self.state.clone()))
    }
    pub fn status(&self) -> CollectorStatus {
        self.state.lock().unwrap().status
    }
    pub async fn shutdown(&mut self) -> bool {
        self.close();
        let deadline = Instant::now() + WRITE_BUDGET;
        let Some(task) = self.task.as_mut() else {
            return true;
        };
        match timeout_at(deadline, &mut *task).await {
            Ok(result) => {
                self.task.take();
                result.unwrap_or(false)
            }
            Err(_) => {
                task.abort();
                if timeout_at(deadline, &mut *task).await.is_ok() {
                    self.task.take();
                }
                false
            }
        }
    }
    fn close(&self) {
        self.stop.send_replace(true);
        let mut state = self.state.lock().unwrap();
        state.observer = None;
        state.status = CollectorStatus::Closed;
    }
}
impl Drop for HistoryCollector {
    fn drop(&mut self) {
        self.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
// Also clear a published observer when the owned task is aborted or panics.
struct ClearObserver(Arc<Mutex<State>>);
impl Drop for ClearObserver {
    fn drop(&mut self) {
        let mut state = self.0.lock().unwrap();
        state.observer = None;
        if !matches!(
            state.status,
            CollectorStatus::Closed
                | CollectorStatus::ExhaustedRejected
                | CollectorStatus::ExhaustedUnavailable
        ) {
            state.status = CollectorStatus::Unavailable;
        }
    }
}
async fn run(
    config: HistoryConfig,
    state: Arc<Mutex<State>>,
    mut stop: watch::Receiver<bool>,
) -> bool {
    let _clear = ClearObserver(state.clone());
    for attempt in 0..ATTEMPTS {
        let result = tokio::select! {
            biased;
            _ = stop.changed() => return true,
            result = timeout(CONNECT_BUDGET, HistoryWriter::connect_collector(config.connect_options())) => result,
        };
        match result {
            Ok(Ok(writer)) => {
                let mut service = HistoryService::new(writer);
                let published = {
                    let mut shared = state.lock().unwrap();
                    // Closure and publication use the same short lock.
                    if shared.status == CollectorStatus::Closed || *stop.borrow() {
                        false
                    } else {
                        shared.observer = Some(service.observer());
                        shared.status = CollectorStatus::Degraded;
                        true
                    }
                };
                if !published {
                    return service.shutdown().await;
                }
                loop {
                    let status = if service.health().history_ready {
                        CollectorStatus::Active
                    } else {
                        CollectorStatus::Degraded
                    };
                    set_status(&state, status);
                    tokio::select! {
                        biased;
                        _ = stop.changed() => return service.shutdown().await,
                        _ = sleep(Duration::from_secs(1)) => {},
                    }
                }
            }
            failure => {
                let rejected = matches!(failure, Ok(Err(HistoryError::Privilege)));
                let status = match (attempt + 1 == ATTEMPTS, rejected) {
                    (true, true) => CollectorStatus::ExhaustedRejected,
                    (true, false) => CollectorStatus::ExhaustedUnavailable,
                    (false, true) => CollectorStatus::Rejected,
                    (false, false) => CollectorStatus::Unavailable,
                };
                set_status(&state, status);
                if attempt + 1 == ATTEMPTS {
                    return true;
                }
                tokio::select! {
                    biased;
                    _ = stop.changed() => return true,
                    _ = sleep(backoff(attempt)) => {},
                }
            }
        }
    }
    true
}
fn backoff(attempt: u8) -> Duration {
    Duration::from_secs(1 << attempt.min(3))
}
fn set_status(state: &Mutex<State>, status: CollectorStatus) {
    let changed = {
        let mut shared = state.lock().unwrap();
        if shared.status != CollectorStatus::Closed && shared.status != status {
            shared.status = status;
            true
        } else {
            false
        }
    };
    // Never hold the request facade's lock across a potentially blocking sink.
    if changed {
        tracing::info!(
            ?status,
            "query history collector status (independent of SQL availability)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn absent_config_has_no_task_pool_or_observer_and_shuts_down() {
        let mut collector = HistoryCollector::start(None);
        assert_eq!(collector.status(), CollectorStatus::Disabled);
        assert!(collector.task.is_none());
        assert!(collector.observer_source().current().is_none());
        assert!(collector.shutdown().await);
        assert_eq!(collector.status(), CollectorStatus::Closed);
    }
    #[test]
    fn startup_attempts_and_backoff_are_finite() {
        assert_eq!(ATTEMPTS, 8);
        assert_eq!(CONNECT_BUDGET, Duration::from_secs(2));
        assert_eq!(
            (0..ATTEMPTS - 1).map(|a| backoff(a).as_secs()).sum::<u64>(),
            39
        );
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    fn pending_owner(dropped: Arc<AtomicBool>) -> HistoryCollector {
        let state = Arc::new(Mutex::new(State {
            status: CollectorStatus::Starting,
            observer: None,
        }));
        let (stop, _) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _dropped = Dropped(dropped);
            std::future::pending::<()>().await;
            true
        });
        HistoryCollector {
            state,
            stop,
            task: Some(task),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn cancelled_shutdown_retains_handle_and_drop_aborts_owned_startup() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut collector = pending_owner(dropped.clone());
        tokio::task::yield_now().await;
        {
            let future = collector.shutdown();
            tokio::pin!(future);
            tokio::select! { biased; _ = &mut future => panic!("unexpected completion"), _ = tokio::task::yield_now() => {} }
        }
        assert!(collector.task.is_some());
        assert_eq!(collector.status(), CollectorStatus::Closed);
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(!collector.shutdown().await);
        if collector.task.is_some() {
            assert!(!collector.shutdown().await);
        }
        assert!(collector.task.is_none());
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn normal_shutdown_observes_completion_and_consumes_handle() {
        let mut collector = HistoryCollector::start(None);
        let mut stop = collector.stop.subscribe();
        collector.task = Some(tokio::spawn(async move {
            stop.changed().await.unwrap();
            true
        }));
        assert!(collector.shutdown().await);
        assert!(collector.task.is_none());
    }
    #[tokio::test]
    async fn drop_aborts_the_sole_pending_startup_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let collector = pending_owner(dropped.clone());
        let source = collector.observer_source();
        tokio::task::yield_now().await;
        drop(collector);
        for _ in 0..10 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert!(source.current().is_none());
    }
}
