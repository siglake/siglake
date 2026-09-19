//! Bounded, task-ordered draining for readers that advertise file order.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use futures::Stream;
use futures::future::BoxFuture;

use crate::error::Result;
use crate::scan::ArrowRecordBatchStream;
use crate::{Error, ErrorKind};

const DEFAULT_ORDERED_DRAIN_BUFFER_BYTES: u64 = 256 * 1024 * 1024;

fn ordered_drain_buffer_bytes_from(configured: Option<&str>) -> u64 {
    configured
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ORDERED_DRAIN_BUFFER_BYTES)
}

fn ordered_drain_buffer_bytes() -> u64 {
    ordered_drain_buffer_bytes_from(
        std::env::var("SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES")
            .ok()
            .as_deref(),
    )
}

#[derive(Debug)]
struct BufferedBatch {
    batch: RecordBatch,
    bytes: u64,
}

enum TaskState {
    Opening(BoxFuture<'static, Result<ArrowRecordBatchStream>>),
    Streaming(ArrowRecordBatchStream),
    Failed(Option<Error>),
    Done,
}

struct DrainTask {
    state: TaskState,
    buffered: VecDeque<BufferedBatch>,
    waiting_on_budget: bool,
}

impl DrainTask {
    fn new(open: BoxFuture<'static, Result<ArrowRecordBatchStream>>) -> Self {
        Self {
            state: TaskState::Opening(open),
            buffered: VecDeque::new(),
            waiting_on_budget: false,
        }
    }
}

pub(super) struct OrderedRecordBatchDrain<S> {
    tasks: S,
    active: VecDeque<DrainTask>,
    tasks_exhausted: bool,
    concurrency_limit: usize,
    budget_bytes: u64,
    buffered_bytes: u64,
}

impl<S> OrderedRecordBatchDrain<S>
where
    S: Stream<Item = BoxFuture<'static, Result<ArrowRecordBatchStream>>> + Unpin,
{
    pub(super) fn new(tasks: S, concurrency_limit: usize) -> Self {
        let drain = Self {
            tasks,
            active: VecDeque::new(),
            tasks_exhausted: false,
            concurrency_limit: concurrency_limit.max(1),
            budget_bytes: ordered_drain_buffer_bytes(),
            buffered_bytes: 0,
        };
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => "iceberg_reader"
        )
        .set(0.0);
        drain
    }

    fn set_buffered_gauge(&self) {
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => "iceberg_reader"
        )
        .set(self.buffered_bytes as f64);
    }

    fn release_buffered_bytes(&mut self, bytes: u64) {
        self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes);
        self.set_buffered_gauge();
    }

    fn front_ready_batch_or_advance(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            let front = self.active.front_mut()?;
            if let Some(buffered) = front.buffered.pop_front() {
                let clear_wait = front.waiting_on_budget;
                let bytes = buffered.bytes;
                let batch = buffered.batch;
                let _ = front;
                self.release_buffered_bytes(bytes);
                if clear_wait
                    && self.buffered_bytes < self.budget_bytes
                    && let Some(front) = self.active.front_mut()
                {
                    front.waiting_on_budget = false;
                }
                return Some(Ok(batch));
            }
            match &mut front.state {
                TaskState::Failed(error) => {
                    let error = error.take().expect("ordered drain stored error");
                    self.active.pop_front();
                    return Some(Err(error));
                }
                TaskState::Done => {
                    self.active.pop_front();
                }
                TaskState::Opening(_) | TaskState::Streaming(_) => return None,
            }
        }
    }

    fn poll_front_live(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let Some(front) = self.active.front_mut() else {
            return if self.tasks_exhausted {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        };
        loop {
            match &mut front.state {
                TaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => front.state = TaskState::Streaming(stream),
                    Poll::Ready(Err(error)) => {
                        front.state = TaskState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                TaskState::Streaming(stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(error))) => {
                        front.state = TaskState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Ready(None) => {
                        front.state = TaskState::Done;
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                TaskState::Failed(error) => {
                    return Poll::Ready(Some(Err(error
                        .take()
                        .expect("ordered drain stored error"))));
                }
                TaskState::Done => return Poll::Pending,
            }
        }
    }

    fn fill_active(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while !self.tasks_exhausted && self.active.len() < self.concurrency_limit {
            match Pin::new(&mut self.tasks).poll_next(cx) {
                Poll::Ready(Some(open)) => {
                    self.active.push_back(DrainTask::new(open));
                    progressed = true;
                }
                Poll::Ready(None) => self.tasks_exhausted = true,
                Poll::Pending => break,
            }
        }
        progressed
    }

    fn poll_non_front_task(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let Some(task) = self.active.get_mut(index) else {
            return false;
        };
        if !task.buffered.is_empty() {
            return false;
        }
        loop {
            match &mut task.state {
                TaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => task.state = TaskState::Streaming(stream),
                    Poll::Ready(Err(error)) => {
                        task.state = TaskState::Failed(Some(error));
                        return true;
                    }
                    Poll::Pending => return false,
                },
                TaskState::Streaming(stream) => {
                    if self.buffered_bytes >= self.budget_bytes {
                        Self::mark_backpressured(task);
                        return false;
                    }
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let bytes = batch.get_array_memory_size() as u64;
                            if self.buffered_bytes.saturating_add(bytes) > self.budget_bytes {
                                Self::mark_backpressured(task);
                            } else {
                                task.waiting_on_budget = false;
                            }
                            task.buffered.push_back(BufferedBatch { batch, bytes });
                            self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
                            self.set_buffered_gauge();
                            return true;
                        }
                        Poll::Ready(Some(Err(error))) => {
                            task.state = TaskState::Failed(Some(error));
                            return true;
                        }
                        Poll::Ready(None) => {
                            task.state = TaskState::Done;
                            return true;
                        }
                        Poll::Pending => return false,
                    }
                }
                TaskState::Failed(_) | TaskState::Done => return false,
            }
        }
    }

    fn mark_backpressured(task: &mut DrainTask) {
        if !task.waiting_on_budget {
            metrics::counter!(
                "siglake_query_ordered_drain_backpressure_total",
                "path" => "iceberg_reader"
            )
            .increment(1);
            task.waiting_on_budget = true;
        }
    }
}

impl<S> Stream for OrderedRecordBatchDrain<S>
where
    S: Stream<Item = BoxFuture<'static, Result<ArrowRecordBatchStream>>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let mut progressed = this.fill_active(cx);
            let front_len = this.active.len();
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.active.len() != front_len {
                continue;
            }
            match this.poll_front_live(cx) {
                Poll::Ready(batch) => return Poll::Ready(batch),
                Poll::Pending => {}
            }
            let front_len = this.active.len();
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.active.len() != front_len {
                continue;
            }
            for index in 1..this.active.len() {
                progressed |= this.poll_non_front_task(index, cx);
                if this.buffered_bytes >= this.budget_bytes {
                    break;
                }
            }
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.tasks_exhausted && this.active.is_empty() {
                return Poll::Ready(None);
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

impl<S> Drop for OrderedRecordBatchDrain<S> {
    fn drop(&mut self) {
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => "iceberg_reader"
        )
        .set(0.0);
    }
}

pub(super) fn task_stream_error(error: Error) -> Error {
    Error::new(ErrorKind::Unexpected, "file scan task generate failed").with_source(error)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_ORDERED_DRAIN_BUFFER_BYTES, ordered_drain_buffer_bytes_from};

    #[test]
    fn ordered_drain_budget_resolver_preserves_default_and_zero() {
        assert_eq!(
            ordered_drain_buffer_bytes_from(None),
            DEFAULT_ORDERED_DRAIN_BUFFER_BYTES
        );
        assert_eq!(
            ordered_drain_buffer_bytes_from(Some("invalid")),
            DEFAULT_ORDERED_DRAIN_BUFFER_BYTES
        );
        assert_eq!(ordered_drain_buffer_bytes_from(Some("0")), 0);
        assert_eq!(ordered_drain_buffer_bytes_from(Some("4096")), 4096);
    }
}
