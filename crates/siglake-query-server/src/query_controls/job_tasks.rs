//! Retained bounded lifecycle ownership on the existing server runtime.
//! Shutdown is cancellation-safe and polls joins in place. Drop only requests
//! abort: it does not prove task/resource termination.
use crate::error::ApiError;
use futures::task::ArcWake;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Context,
};
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};
#[derive(Default)]
pub(crate) struct JobTasks {
    state: Mutex<State>,
    wake: Arc<Wake>,
}
#[derive(Default)]
struct State {
    closed: bool,
    reserved: usize,
    tasks: Vec<(JoinHandle<()>, watch::Sender<bool>)>,
}
#[derive(Default)]
struct Wake(Notify);
impl ArcWake for Wake {
    fn wake_by_ref(w: &Arc<Self>) {
        w.0.notify_waiters();
    }
}
impl JobTasks {
    fn reap(&self, state: &mut State) {
        let waker = futures::task::waker_ref(&self.wake);
        let mut cx = Context::from_waker(&waker);
        let mut i = 0;
        while i < state.tasks.len() {
            if Pin::new(&mut state.tasks[i].0).poll(&mut cx).is_ready() {
                drop(state.tasks.swap_remove(i));
            } else {
                i += 1;
            }
        }
    }
    pub(crate) fn reserve(&self) -> Result<Slot<'_>, ApiError> {
        let mut s = self.state.lock().unwrap();
        self.reap(&mut s);
        if s.closed || s.tasks.len() + s.reserved >= 128 {
            return Err(ApiError::service_unavailable("query_controls_busy"));
        }
        s.reserved += 1;
        Ok(Slot { owner: self })
    }
    pub(crate) async fn shutdown(&self) {
        {
            let mut s = self.state.lock().unwrap();
            s.closed = true;
            for (_, stop) in &s.tasks {
                let _ = stop.send(true);
            }
        }
        loop {
            let notified = self.wake.0.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut s = self.state.lock().unwrap();
                self.reap(&mut s);
                if s.tasks.is_empty() && s.reserved == 0 {
                    return;
                }
            }
            notified.await;
        }
    }
}
pub(crate) struct Slot<'a> {
    owner: &'a JobTasks,
}
impl Slot<'_> {
    /// No await between durable acceptance and this ownership transfer.
    pub(crate) fn start<F, W>(self, work: W)
    where
        F: Future<Output = ()> + Send + 'static,
        W: FnOnce(watch::Receiver<bool>) -> F,
    {
        self.start_with(work, tokio::spawn);
    }
    pub(crate) fn start_on<F, W>(self, runtime: &crate::jobs::BatchRuntime, work: W)
    where
        F: Future<Output = ()> + Send + 'static,
        W: FnOnce(watch::Receiver<bool>) -> F,
    {
        self.start_with(work, |future| runtime.spawn(future));
    }
    fn start_with<F, W, S>(self, work: W, spawn: S)
    where
        F: Future<Output = ()> + Send + 'static,
        W: FnOnce(watch::Receiver<bool>) -> F,
        S: FnOnce(F) -> JoinHandle<()>,
    {
        let (stop, rx) = watch::channel(false);
        let mut s = self.owner.state.lock().unwrap();
        if s.closed {
            let _ = stop.send(true);
        }
        s.tasks.push((spawn(work(rx)), stop));
        drop(s);
        // Drop releases only the reservation; retained handles own the run.
    }
}
impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.owner.state.lock().unwrap().reserved -= 1;
        self.owner.wake.0.notify_waiters();
    }
}
impl Drop for JobTasks {
    fn drop(&mut self) {
        for (task, stop) in &self.state.get_mut().unwrap().tasks {
            let _ = stop.send(true);
            task.abort();
        }
    }
}
/// If a server lifecycle task panics/is finally aborted, abort the batch child.
/// Ordinary timeout/shutdown aborts AND observes its join before returning.
pub(crate) struct Child(pub(crate) JoinHandle<()>);
impl Drop for Child {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn job_owner_shutdown_retains_join_after_cancelled_wait_and_closes_generation() {
        let tasks = Arc::new(JobTasks::default());
        let (begun, ready) = tokio::sync::oneshot::channel();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let held = gate.clone();
        tasks.reserve().unwrap().start(move |mut stop| async move {
            let _ = begun.send(());
            let _ = stop.changed().await;
            let _permit = held.acquire().await.unwrap();
        });
        ready.await.unwrap();
        let owned = tasks.clone();
        let waiter = tokio::spawn(async move {
            owned.shutdown().await;
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        assert!(tasks.reserve().is_err());
        assert_eq!(tasks.state.lock().unwrap().tasks.len(), 1);
        gate.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(1), tasks.shutdown())
            .await
            .unwrap();
        assert!(tasks.state.lock().unwrap().tasks.is_empty());
    }
    #[tokio::test]
    async fn job_owner_is_bounded_and_reserved_slot_survives_shutdown_race() {
        let tasks = Arc::new(JobTasks::default());
        let mut slots = Vec::new();
        for _ in 0..128 {
            slots.push(tasks.reserve().unwrap());
        }
        assert!(tasks.reserve().is_err());
        let owner = tasks.clone();
        let shutdown = tokio::spawn(async move {
            owner.shutdown().await;
        });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        let slot = slots.pop().unwrap();
        slot.start(|mut stop| async move {
            assert!(stop.changed().await.is_ok());
            assert!(*stop.borrow());
        });
        drop(slots);
        tokio::time::timeout(std::time::Duration::from_secs(1), shutdown)
            .await
            .unwrap()
            .unwrap();
        tasks.shutdown().await;
    }
}
