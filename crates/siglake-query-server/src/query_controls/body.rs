//! Authority-owned cooperative body production. No detached reaper: handles stay
//! in the live registry until their join is observed. Explicit shutdown is the
//! confirmation boundary; final synchronous Drop only requests cancellation.
use crate::{error::ApiError, history::streaming::DeliveryObservation};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinHandle,
    time::Instant,
};

pub(crate) const CHUNK_BYTES: usize = 16 * 1024;
const CHANNEL_ITEMS: usize = 2;
// Technical authority-global bound, not per-agent admission policy.
const TASKS: usize = 128;
const LIVE: u8 = 0;
const DELIVERED: u8 = 1;
const ABANDONED: u8 = 2;
const TIMEOUT: u8 = 3;
const FAILED: u8 = 4;
tokio::task_local! { static PRODUCTION: Arc<Life>; }
pub(crate) fn check_cancel() -> io::Result<()> {
    if PRODUCTION
        .try_with(|life| life.outcome.load(Ordering::Acquire) != LIVE)
        .unwrap_or(false)
    {
        Err(io::Error::other("query body cancelled"))
    } else {
        Ok(())
    }
}
#[derive(Default)]
pub(crate) struct BodyTasks {
    state: Mutex<Registry>,
    joined: Arc<JoinWake>,
}
/// JoinHandles always see this registry-owned waker, never a shutdown waiter's
/// transient task waker. It owns only a Notify, not the registry/tasks (no cycle).
#[derive(Default)]
struct JoinWake(Notify);
impl futures::task::ArcWake for JoinWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        wake.0.notify_waiters();
    }
}
#[derive(Default)]
struct Registry {
    closed: bool,
    reserved: usize,
    tasks: Vec<(JoinHandle<()>, Arc<Life>)>,
}
struct Life {
    outcome: AtomicU8,
    changed: Notify,
    history: Option<DeliveryObservation>,
    deadline: Instant,
}
impl Life {
    /// Single terminal arbiter for producer, body and typed history. Deadline
    /// precedence is checked at commit, including after late-ready native Err.
    /// This is a cooperative checkpoint, not atomic wall-clock preemption.
    fn finish(&self, requested: u8) -> u8 {
        let outcome = if Instant::now() >= self.deadline {
            TIMEOUT
        } else {
            requested
        };
        if self
            .outcome
            .compare_exchange(LIVE, outcome, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            if let Some(h) = &self.history {
                match outcome {
                    DELIVERED => h.delivered(),
                    TIMEOUT => h.timed_out(),
                    FAILED => h.failed(),
                    _ => h.abandoned(),
                }
            }
            self.changed.notify_waiters();
        }
        self.outcome.load(Ordering::Acquire)
    }
    fn checkpoint(&self) -> u8 {
        if Instant::now() >= self.deadline {
            self.finish(TIMEOUT)
        } else {
            self.outcome.load(Ordering::Acquire)
        }
    }
}
impl BodyTasks {
    fn reap(&self, registry: &mut Registry) {
        let waker = futures::task::waker_ref(&self.joined);
        let mut cx = Context::from_waker(&waker);
        let mut i = 0;
        while i < registry.tasks.len() {
            if Pin::new(&mut registry.tasks[i].0).poll(&mut cx).is_ready() {
                // Join (including cancellation/panic) was OBSERVED before removal.
                drop(registry.tasks.swap_remove(i));
                self.joined.0.notify_waiters();
            } else {
                i += 1;
            }
        }
    }
    pub(crate) fn reserve(&self) -> Result<Slot<'_>, ApiError> {
        let mut registry = self.state.lock().unwrap();
        if registry.closed {
            return Err(ApiError::service_unavailable("query_controls_busy"));
        }
        self.reap(&mut registry);
        if registry.closed || registry.tasks.len() + registry.reserved >= TASKS {
            return Err(ApiError::service_unavailable("query_controls_busy"));
        }
        registry.reserved += 1;
        Ok(Slot {
            owner: self,
            armed: true,
        })
    }
    fn start(
        &self,
        response: Response,
        controls: super::runtime::RequestControls,
        history: Option<DeliveryObservation>,
    ) -> Result<Response, ApiError> {
        let deadline = controls.deadline;
        let mut registry = self.state.lock().unwrap();
        registry.reserved -= 1; // atomic reservation -> registered task transfer
        if Instant::now() >= deadline {
            if let Some(history) = history {
                history.timed_out();
            }
            return Err(super::runtime::expired());
        }
        if registry.closed {
            if let Some(history) = history {
                history.failed();
            }
            return Err(ApiError::service_unavailable("query_controls_busy"));
        }
        let ndjson = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .is_some_and(|v| v == "application/x-ndjson");
        let (parts, source) = response.into_parts();
        let (tx, rx) = mpsc::channel(CHANNEL_ITEMS);
        let life = Arc::new(Life {
            outcome: AtomicU8::new(LIVE),
            changed: Notify::new(),
            history,
            deadline,
        });
        let owned = life.clone();
        // Lock spans only reservation/spawn/insertion, never an await. A closed
        // generation cannot race a late spawn or reinsertion.
        let task = tokio::spawn(super::runtime::body_scope(
            controls,
            PRODUCTION.scope(life.clone(), async move {
                let result = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(deadline) => TIMEOUT,
                    _ = cancelled(&owned) => owned.outcome.load(Ordering::Acquire),
                    result = pump(source, tx, deadline, &owned) => result,
                };
                owned.finish(result);
            }),
        ));
        registry.tasks.push((task, life.clone()));
        Ok(Response::from_parts(
            parts,
            Body::from_stream(ControlledBody {
                rx,
                life,
                ndjson,
                at_boundary: true,
                ended: false,
                #[cfg(test)]
                after_dequeue: None,
            }),
        ))
    }
    pub(crate) async fn shutdown(&self) {
        {
            let mut registry = self.state.lock().unwrap();
            registry.closed = true;
            for (_, life) in &registry.tasks {
                life.finish(ABANDONED);
            }
        }
        // Poll joins in place: cancelling this waiter leaves every handle in
        // the closed registry, available to a later shutdown await.
        loop {
            // Register BEFORE polling joins. Completion either notifies every
            // enabled waiter or is seen by reap; cancelling any waiter cannot
            // replace the stable join waker or steal another waiter's wakeup.
            let joined = self.joined.0.notified();
            tokio::pin!(joined);
            joined.as_mut().enable();
            {
                let mut registry = self.state.lock().unwrap();
                self.reap(&mut registry);
                if registry.tasks.is_empty() {
                    return;
                }
            }
            joined.await;
        }
    }
}
impl Drop for BodyTasks {
    fn drop(&mut self) {
        let registry = self.state.get_mut().unwrap();
        registry.closed = true;
        for (task, life) in &registry.tasks {
            life.finish(ABANDONED);
            task.abort();
        }
        // Final teardown fallback only: abort is NOT join/termination proof.
        // A live service must retain this owner and await shutdown instead.
    }
}
async fn cancelled(life: &Life) {
    loop {
        let changed = life.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if life.outcome.load(Ordering::Acquire) != LIVE {
            return;
        }
        changed.await;
    }
}
enum Item {
    Data(Bytes),
    End,
}
async fn pump(source: Body, tx: mpsc::Sender<Item>, deadline: Instant, life: &Life) -> u8 {
    let mut source = source.into_data_stream();
    while let Some(chunk) = source.next().await {
        let Ok(chunk) = chunk else {
            return FAILED;
        };
        // Copy only bounded logical chunks: queued slices must not retain an
        // entire upstream payload allocation. Source/native backing is separate.
        for part in chunk.chunks(CHUNK_BYTES) {
            if Instant::now() >= deadline {
                return TIMEOUT;
            }
            if tx
                .send(Item::Data(Bytes::copy_from_slice(part)))
                .await
                .is_err()
            {
                return ABANDONED;
            }
            tokio::task::yield_now().await;
        }
    }
    if Instant::now() >= deadline {
        return TIMEOUT;
    }
    if tx.send(Item::End).await.is_err() {
        return ABANDONED;
    }
    // Retain source (and its admission guard) through OUTER consumption, not
    // early source EOF. Tiny never-polled bodies still expire independently.
    cancelled(life).await;
    drop(source);
    life.outcome.load(Ordering::Acquire)
}
struct ControlledBody {
    rx: mpsc::Receiver<Item>,
    life: Arc<Life>,
    ndjson: bool,
    at_boundary: bool,
    ended: bool,
    #[cfg(test)]
    after_dequeue: Option<Box<dyn FnOnce() + Send>>,
}
impl ControlledBody {
    fn terminal(&mut self, outcome: u8) -> Poll<Option<io::Result<Bytes>>> {
        self.ended = true;
        self.rx.close();
        if outcome == DELIVERED {
            return Poll::Ready(None);
        }
        // No marker may be appended to a partially emitted record.
        if outcome == TIMEOUT && self.ndjson && self.at_boundary {
            return Poll::Ready(Some(Ok(Bytes::from_static(b"{\"_meta\":\"timeout\"}\n"))));
        }
        Poll::Ready(Some(Err(io::Error::other("query body terminated"))))
    }
}
impl Stream for ControlledBody {
    type Item = Result<Bytes, io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let outcome = self.life.checkpoint();
        if outcome != LIVE {
            return self.terminal(outcome);
        }
        let item = self.rx.poll_recv(cx);
        #[cfg(test)]
        if let Some(hook) = self.after_dequeue.take() {
            hook();
        }
        match item {
            Poll::Ready(Some(Item::Data(bytes))) => {
                // A producer terminal can win while the channel is dequeued.
                // Discard queued data rather than contradicting that winner.
                let outcome = self.life.checkpoint();
                if outcome != LIVE {
                    return self.terminal(outcome);
                }
                self.at_boundary = bytes.last() == Some(&b'\n');
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Item::End)) => {
                let winner = self.life.finish(DELIVERED);
                // Clean EOF only if delivery actually won at the original
                // deadline checkpoint, never merely because End was queued.
                self.terminal(winner)
            }
            Poll::Ready(None) => {
                let winner = self.life.finish(FAILED);
                self.terminal(winner)
            }
            Poll::Pending => {
                let outcome = self.life.checkpoint();
                if outcome != LIVE {
                    self.terminal(outcome)
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
impl Drop for ControlledBody {
    fn drop(&mut self) {
        self.life.finish(ABANDONED);
    }
}

#[cfg(test)]
mod tests;

pub(crate) struct Slot<'a> {
    owner: &'a BodyTasks,
    armed: bool,
}
impl Slot<'_> {
    pub(crate) fn start(
        mut self,
        response: Response,
        controls: super::runtime::RequestControls,
        history: Option<DeliveryObservation>,
    ) -> Result<Response, ApiError> {
        self.armed = false;
        self.owner.start(response, controls, history)
    }
}
impl Drop for Slot<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.owner.state.lock().unwrap().reserved -= 1;
        }
    }
}
