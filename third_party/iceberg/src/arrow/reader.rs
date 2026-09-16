// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Parquet file data reader

use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_arith::boolean::{and, and_kleene, is_not_null, is_null, not, or, or_kleene};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Datum as ArrowDatum, RecordBatch, Scalar, UInt32Array,
};
use arrow_cast::cast::cast;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow_schema::{
    ArrowError, DataType, FieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use arrow_string::like::starts_with;
use bytes::Bytes;
use fnv::FnvHashSet;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::schema::types::{SchemaDescriptor, Type as ParquetType};
use typed_builder::TypedBuilder;
use arrow_select::take::take;

use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::arrow::int96::coerce_int96_timestamps;
use crate::arrow::record_batch_transformer::RecordBatchTransformerBuilder;
use crate::arrow::{arrow_schema_to_schema, get_arrow_datum};
use crate::delete_vector::DeleteVector;
use crate::error::Result;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::visitors::page_index_evaluator::PageIndexEvaluator;
use crate::expr::visitors::row_group_metrics_evaluator::RowGroupMetricsEvaluator;
use crate::expr::{BoundPredicate, BoundReference};
use crate::io::{FileIO, FileMetadata, FileRead};
use crate::io::read_observability::{
    ObjectStoreReadPhase, ReadDebouncer, record_object_store_reads,
};
use crate::metadata_columns::{RESERVED_FIELD_ID_FILE, is_metadata_field};
use crate::puffin::PuffinReader;
use crate::scan::{ArrowRecordBatchStream, FileScanTask, FileScanTaskStream};
use crate::spec::{Datum, NameMapping, NestedField, PrimitiveType, Schema, Type};
use crate::utils::available_parallelism;
use crate::{Error, ErrorKind};

/// Default gap between byte ranges below which they are coalesced into a
/// single request. Matches object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
const DEFAULT_RANGE_COALESCE_BYTES: u64 = 1024 * 1024;

/// Default maximum number of coalesced byte ranges fetched concurrently.
/// Matches object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
const DEFAULT_RANGE_FETCH_CONCURRENCY: usize = 10;

/// Default number of bytes to prefetch when parsing Parquet footer metadata.
/// Matches DataFusion's default `ParquetOptions::metadata_size_hint`.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;
const DEFAULT_ORDERED_DRAIN_BUFFER_BYTES: u64 = 256 * 1024 * 1024;

fn ordered_drain_buffer_bytes() -> u64 {
    std::env::var("SIGLAKE_ORDERED_DRAIN_BUFFER_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ORDERED_DRAIN_BUFFER_BYTES)
}

#[derive(Debug)]
struct OrderedDrainBufferedBatch {
    batch: RecordBatch,
    bytes: u64,
}

enum OrderedDrainTaskState {
    Opening(BoxFuture<'static, Result<ArrowRecordBatchStream>>),
    Streaming(ArrowRecordBatchStream),
    Failed(Option<Error>),
    Done,
}

struct OrderedDrainTask {
    state: OrderedDrainTaskState,
    buffered: VecDeque<OrderedDrainBufferedBatch>,
    waiting_on_budget: bool,
}

impl OrderedDrainTask {
    fn new(open: BoxFuture<'static, Result<ArrowRecordBatchStream>>) -> Self {
        Self {
            state: OrderedDrainTaskState::Opening(open),
            buffered: VecDeque::new(),
            waiting_on_budget: false,
        }
    }
}

struct OrderedRecordBatchDrain<S> {
    tasks: S,
    active: VecDeque<OrderedDrainTask>,
    tasks_exhausted: bool,
    concurrency_limit: usize,
    budget_bytes: u64,
    buffered_bytes: u64,
    metrics_path: &'static str,
}

impl<S> OrderedRecordBatchDrain<S>
where
    S: futures::Stream<Item = BoxFuture<'static, Result<ArrowRecordBatchStream>>> + Unpin,
{
    fn new(tasks: S, concurrency_limit: usize, metrics_path: &'static str) -> Self {
        let drain = Self {
            tasks,
            active: VecDeque::new(),
            tasks_exhausted: false,
            concurrency_limit: concurrency_limit.max(1),
            budget_bytes: ordered_drain_buffer_bytes(),
            buffered_bytes: 0,
            metrics_path,
        };
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => metrics_path
        )
        .set(0.0);
        drain
    }

    fn set_buffered_gauge(&self) {
        metrics::gauge!(
            "siglake_query_ordered_drain_buffered_bytes",
            "path" => self.metrics_path
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
                let should_clear_wait = front.waiting_on_budget;
                let bytes = buffered.bytes;
                let batch = buffered.batch;
                let _ = front;
                self.release_buffered_bytes(bytes);
                if should_clear_wait {
                    if let Some(front) = self.active.front_mut() {
                        if self.buffered_bytes < self.budget_bytes {
                            front.waiting_on_budget = false;
                        }
                    }
                }
                return Some(Ok(batch));
            }
            match &mut front.state {
                OrderedDrainTaskState::Failed(err) => {
                    return Some(Err(err.take().expect("ordered drain stored error")));
                }
                OrderedDrainTaskState::Done => {
                    self.active.pop_front();
                }
                OrderedDrainTaskState::Opening(_) | OrderedDrainTaskState::Streaming(_) => {
                    return None;
                }
            }
        }
    }

    fn poll_front_live(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let Some(front) = self.active.front_mut() else {
            return if self.tasks_exhausted {
                Poll::Ready(None)
            } else {
                Poll::Pending
            };
        };
        loop {
            match &mut front.state {
                OrderedDrainTaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        front.state = OrderedDrainTaskState::Streaming(stream);
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                    Poll::Pending => return Poll::Pending,
                },
                OrderedDrainTaskState::Streaming(stream) => match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    Poll::Ready(None) => {
                        front.state = OrderedDrainTaskState::Done;
                        return Poll::Pending;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                OrderedDrainTaskState::Failed(err) => {
                    return Poll::Ready(Some(Err(err.take().expect("ordered drain stored error"))))
                }
                OrderedDrainTaskState::Done => return Poll::Pending,
            }
        }
    }

    fn fill_active(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while !self.tasks_exhausted && self.active.len() < self.concurrency_limit {
            match Pin::new(&mut self.tasks).poll_next(cx) {
                Poll::Ready(Some(open)) => {
                    self.active.push_back(OrderedDrainTask::new(open));
                    progressed = true;
                }
                Poll::Ready(None) => {
                    self.tasks_exhausted = true;
                }
                Poll::Pending => break,
            }
        }
        progressed
    }

    fn poll_non_front_task(&mut self, index: usize, cx: &mut Context<'_>) -> bool {
        let metrics_path = self.metrics_path;
        let Some(task) = self.active.get_mut(index) else {
            return false;
        };
        if !task.buffered.is_empty() {
            return false;
        }
        loop {
            match &mut task.state {
                OrderedDrainTaskState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        task.state = OrderedDrainTaskState::Streaming(stream);
                    }
                    Poll::Ready(Err(err)) => {
                        task.state = OrderedDrainTaskState::Failed(Some(err));
                        return true;
                    }
                    Poll::Pending => return false,
                },
                OrderedDrainTaskState::Streaming(stream) => {
                    if self.buffered_bytes >= self.budget_bytes {
                        if !task.waiting_on_budget {
                            metrics::counter!(
                                "siglake_query_ordered_drain_backpressure_total",
                                "path" => metrics_path
                            )
                            .increment(1);
                            task.waiting_on_budget = true;
                        }
                        return false;
                    }
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let bytes = batch.get_array_memory_size() as u64;
                            if self.buffered_bytes.saturating_add(bytes) > self.budget_bytes
                                && !task.waiting_on_budget
                            {
                                metrics::counter!(
                                    "siglake_query_ordered_drain_backpressure_total",
                                    "path" => metrics_path
                                )
                                .increment(1);
                                task.waiting_on_budget = true;
                            } else if self.buffered_bytes.saturating_add(bytes) <= self.budget_bytes {
                                task.waiting_on_budget = false;
                            }
                            task.buffered.push_back(OrderedDrainBufferedBatch { batch, bytes });
                            self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
                            self.set_buffered_gauge();
                            return true;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            task.state = OrderedDrainTaskState::Failed(Some(err));
                            return true;
                        }
                        Poll::Ready(None) => {
                            task.state = OrderedDrainTaskState::Done;
                            return true;
                        }
                        Poll::Pending => return false,
                    }
                }
                OrderedDrainTaskState::Failed(_) | OrderedDrainTaskState::Done => return false,
            }
        }
    }
}

impl<S> futures::Stream for OrderedRecordBatchDrain<S>
where
    S: futures::Stream<Item = BoxFuture<'static, Result<ArrowRecordBatchStream>>> + Unpin,
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
                progressed = true;
                continue;
            }
            match this.poll_front_live(cx) {
                Poll::Ready(Some(Ok(batch))) => return Poll::Ready(Some(Ok(batch))),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => {}
            }
            let front_len = this.active.len();
            if let Some(batch) = this.front_ready_batch_or_advance() {
                return Poll::Ready(Some(batch));
            }
            if this.active.len() != front_len {
                progressed = true;
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
            "path" => self.metrics_path
        )
        .set(0.0);
    }
}

/// Reorder a `RowSelection`'s per-row-group segments to match a REVERSED
/// row-group read order. The selection was built walking the selected groups
/// in ascending order (`group_rows[i]` = row count of the i-th selected
/// group); parquet applies it to groups AS READ, so a reversed read needs the
/// segments re-concatenated in reversed group order. A selection may end
/// early (trailing skips omitted) — the tail is materialized as an explicit
/// skip before splitting so every segment is complete.
pub fn reverse_row_selection(
    selection: parquet::arrow::arrow_reader::RowSelection,
    group_rows: &[usize],
) -> parquet::arrow::arrow_reader::RowSelection {
    let mut segments = split_selection_by_groups(selection, group_rows);
    segments.reverse();
    segments.into_iter().flatten().collect::<Vec<_>>().into()
}

/// Split a flattened `RowSelection` at row-group boundaries — one complete
/// run list per group, ascending group order (selectors may straddle
/// boundaries; a truncated trailing skip is materialized first).
pub fn split_selection_by_groups(
    selection: parquet::arrow::arrow_reader::RowSelection,
    group_rows: &[usize],
) -> Vec<Vec<parquet::arrow::arrow_reader::RowSelector>> {
    use parquet::arrow::arrow_reader::RowSelector;
    let total: usize = group_rows.iter().sum();
    let mut selectors: Vec<RowSelector> = selection.iter().copied().collect();
    let covered: usize = selectors.iter().map(|s| s.row_count).sum();
    if covered < total {
        selectors.push(RowSelector::skip(total - covered));
    }
    let mut segments: Vec<Vec<RowSelector>> = Vec::with_capacity(group_rows.len());
    let mut iter = selectors.into_iter();
    let mut carry: Option<RowSelector> = None;
    for &rows in group_rows {
        let mut segment = Vec::new();
        let mut remaining = rows;
        while remaining > 0 {
            let sel = match carry.take() {
                Some(sel) => sel,
                None => match iter.next() {
                    Some(sel) => sel,
                    None => RowSelector::skip(remaining),
                },
            };
            if sel.row_count <= remaining {
                remaining -= sel.row_count;
                segment.push(sel);
            } else {
                let mut head = sel;
                head.row_count = remaining;
                let mut tail = sel;
                tail.row_count = sel.row_count - remaining;
                segment.push(head);
                carry = Some(tail);
                remaining = 0;
            }
        }
        segments.push(segment);
    }
    segments
}

/// Reversed tail-chunk size in SELECTED rows (`SIGLAKE_REVERSED_CHUNK_ROWS`,
/// default 32768 ≈ 4 batches; `0` disables chunking — whole-group reversal).
fn reversed_chunk_rows() -> usize {
    std::env::var("SIGLAKE_REVERSED_CHUNK_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32_768)
}

/// Decoded reversed-tail-chunk cache cap (`SIGLAKE_REVERSED_CHUNK_CACHE_MB`,
/// default 256; `0` disables). Entries are a pure function of IMMUTABLE
/// parquet content — key = (file path, row group, selection, batch size,
/// projected field ids) — so no invalidation is ever needed; the LRU only
/// bounds memory. Filter-free chunks only (a decode-time row filter makes
/// the output predicate-dependent). This is the concurrent-browse decode
/// share: N simultaneous match_all-class browses of the newest tail chunk
/// decode it ONCE (single-flight) instead of N times.
fn reversed_chunk_cache_max_bytes() -> usize {
    std::env::var("SIGLAKE_REVERSED_CHUNK_CACHE_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(256)
        .saturating_mul(1024 * 1024)
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ReversedChunkKey {
    path: String,
    group: usize,
    /// (skip, row_count) runs of the chunk's in-group selection.
    selectors: Vec<(bool, usize)>,
    batch_size: Option<usize>,
    field_ids: Vec<i32>,
}

#[derive(Default)]
struct ReversedChunkCache {
    entries: HashMap<ReversedChunkKey, (Vec<RecordBatch>, usize)>,
    order: VecDeque<ReversedChunkKey>,
    bytes: usize,
    inflight: HashMap<ReversedChunkKey, Arc<tokio::sync::Notify>>,
}

impl ReversedChunkCache {
    fn insert(&mut self, key: ReversedChunkKey, batches: Vec<RecordBatch>) {
        let max = reversed_chunk_cache_max_bytes();
        let size: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        if size > max {
            return;
        }
        if let Some((_, prev)) = self.entries.insert(key.clone(), (batches, size)) {
            self.bytes = self.bytes.saturating_sub(prev);
        }
        self.bytes = self.bytes.saturating_add(size);
        self.order.push_back(key);
        while self.bytes > max {
            let Some(oldest) = self.order.pop_front() else { break };
            let still_queued = self.order.iter().any(|k| *k == oldest);
            if !still_queued {
                if let Some((_, evicted)) = self.entries.remove(&oldest) {
                    self.bytes = self.bytes.saturating_sub(evicted);
                }
            }
        }
    }
}

/// Holds the reversed-chunk single-flight marker and releases it on EVERY exit.
///
/// THE DEFECT THIS CLOSES. The leader inserted a marker into `inflight` and
/// removed it in a `finish` closure called on the Ok and Err paths. Both of the
/// awaits between those points — opening the chunk and collecting its stream —
/// are cancellation points, and a dropped future runs NO code, so a cancelled
/// browse left the marker behind. Every later reader of that chunk then parked
/// on a `Notify` nobody would fire, forever, cured only by a restart.
///
/// This is the `ORDER BY ... DESC` browse path, which is the shape the open
/// release blocker names, and the wait happens inside the reader — so it is
/// invisible to both `siglake_query_exec_pool_in_flight` and
/// `siglake_query_in_flight`, which is why the arithmetic that found the
/// 2026-08-17 slot leak found nothing here.
///
/// Twin of the same defect in the query server's SQL result cache, fixed the
/// same day.
struct ReversedChunkLeader(Option<ReversedChunkKey>);

impl Drop for ReversedChunkLeader {
    fn drop(&mut self) {
        let Some(key) = self.0.take() else { return };
        if let Ok(mut cache) = reversed_chunk_cache().lock() {
            if let Some(notify) = cache.inflight.remove(&key) {
                notify.notify_waiters();
            }
        }
    }
}

fn reversed_chunk_cache() -> &'static std::sync::Mutex<ReversedChunkCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<ReversedChunkCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(ReversedChunkCache::default()))
}

/// Tail-first CHUNKS of one group's in-group selection: walk the run list
/// backward accumulating up to `chunk_rows` SELECTED rows per chunk; each
/// chunk is a complete in-group selection (leading skip + the chunk's runs).
/// Returned newest-last-rows-first — the reversed emission order. Every
/// selected row appears in exactly one chunk.
pub fn tail_chunks_of_group_selection(
    segment: &[parquet::arrow::arrow_reader::RowSelector],
    first_chunk_rows: usize,
    max_chunk_rows: usize,
) -> Vec<(Vec<parquet::arrow::arrow_reader::RowSelector>, usize)> {
    use parquet::arrow::arrow_reader::RowSelector;
    // PROGRESSIVE sizing: the newest chunk is what a merge/SPM first-poll
    // pays for, so it stays one batch; deeper chunks grow 4x (capped) to
    // amortize per-chunk reopen overhead on deep drains.
    let max_chunk_rows = max_chunk_rows.max(1);
    let mut chunk_rows = first_chunk_rows.clamp(1, max_chunk_rows);
    // Positions of selected runs in ascending order: (start_row, len).
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut pos = 0usize;
    for sel in segment {
        if !sel.skip && sel.row_count > 0 {
            runs.push((pos, sel.row_count));
        }
        pos += sel.row_count;
    }
    let mut chunks: Vec<(Vec<RowSelector>, usize)> = Vec::new();
    let mut current: Vec<(usize, usize)> = Vec::new(); // ascending within chunk
    let mut current_rows = 0usize;
    let flush = |current: &mut Vec<(usize, usize)>,
                 current_rows: &mut usize,
                 chunks: &mut Vec<(Vec<RowSelector>, usize)>| {
        if current.is_empty() {
            return;
        }
        let mut sel: Vec<RowSelector> = Vec::with_capacity(current.len() * 2);
        let mut at = 0usize;
        for &(start, len) in current.iter() {
            if start > at {
                sel.push(RowSelector::skip(start - at));
            }
            sel.push(RowSelector::select(len));
            at = start + len;
        }
        chunks.push((sel, *current_rows));
        current.clear();
        *current_rows = 0;
    };
    // Walk runs backward, splitting oversized runs so chunks stay bounded.
    for &(start, len) in runs.iter().rev() {
        let mut remaining = len;
        while remaining > 0 {
            let space = chunk_rows - current_rows;
            let take = remaining.min(space);
            let piece_start = start + remaining - take;
            current.insert(0, (piece_start, take));
            current_rows += take;
            remaining -= take;
            if current_rows == chunk_rows {
                flush(&mut current, &mut current_rows, &mut chunks);
                chunk_rows = (chunk_rows.saturating_mul(4)).min(max_chunk_rows);
            }
        }
    }
    flush(&mut current, &mut current_rows, &mut chunks);
    chunks
}

/// Per-group SELECTED row counts, ascending group order — how many rows the
/// reader will actually emit from each group under `selection`. Groups the
/// selection skips entirely come back as 0.
pub fn selection_rows_per_group(
    selection: &parquet::arrow::arrow_reader::RowSelection,
    group_rows: &[usize],
) -> Vec<usize> {
    let mut out = Vec::with_capacity(group_rows.len());
    let mut selectors = selection.iter().copied();
    let mut carry: Option<parquet::arrow::arrow_reader::RowSelector> = None;
    for &rows in group_rows {
        let mut selected = 0usize;
        let mut remaining = rows;
        while remaining > 0 {
            let sel = match carry.take() {
                Some(sel) => sel,
                None => match selectors.next() {
                    Some(sel) => sel,
                    // Trailing skips may be omitted from a selection.
                    None => parquet::arrow::arrow_reader::RowSelector::skip(remaining),
                },
            };
            let used = sel.row_count.min(remaining);
            if !sel.skip {
                selected += used;
            }
            if sel.row_count > remaining {
                let mut tail = sel;
                tail.row_count = sel.row_count - remaining;
                carry = Some(tail);
            }
            remaining -= used;
        }
        out.push(selected);
    }
    out
}

/// Reversed CHUNK flush: buffer one bounded chunk's batches (the chunk is a
/// single tail-first slice of one row group — see the reversed chunk plan)
/// and, when the inner stream ends, emit them in reverse batch order with
/// each batch's rows reversed. No expected-row bookkeeping: the chunk's own
/// stream end is the flush signal, which stays correct when a decode-time
/// row filter drops rows after the RowSelection was computed (the 2026-07-17
/// finding that killed the count-based flush: Exact pushdowns elide the
/// engine's FilterExec, so the filter must stay attached and counts can't be
/// trusted).
struct ReversedGroupBatches<S> {
    inner: S,
    buffered: Vec<RecordBatch>,
    flush: std::collections::VecDeque<RecordBatch>,
    done: bool,
}

impl<S> ReversedGroupBatches<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            buffered: Vec::new(),
            flush: std::collections::VecDeque::new(),
            done: false,
        }
    }
}

impl<S> futures::Stream for ReversedGroupBatches<S>
where
    S: futures::Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RecordBatch>>> {
        use std::task::Poll;
        loop {
            if let Some(batch) = self.flush.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if self.done {
                return Poll::Ready(None);
            }
            match std::pin::Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    let buffered = std::mem::take(&mut self.buffered);
                    for batch in buffered.into_iter().rev() {
                        match reverse_record_batch(batch) {
                            Ok(rb) => self.flush.push_back(rb),
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(batch))) => {
                    self.buffered.push(batch);
                }
            }
        }
    }
}

fn reverse_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch);
    }
    let indices = UInt32Array::from_iter_values((0..batch.num_rows() as u32).rev());
    let columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|err| {
                Error::new(ErrorKind::Unexpected, "Failed to reverse record batch rows")
                    .with_source(err)
            })
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns?).map_err(|err| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to rebuild reversed record batch",
        )
        .with_source(err)
    })
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DebouncedRangeKey {
    path: String,
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ParquetMetadataKey {
    range: DebouncedRangeKey,
    preload_column_index: bool,
    preload_offset_index: bool,
    preload_page_index: bool,
}

fn parquet_footer_debouncer() -> &'static ReadDebouncer<ParquetMetadataKey, Arc<ParquetMetaData>> {
    static DEBOUNCER: std::sync::OnceLock<
        ReadDebouncer<ParquetMetadataKey, Arc<ParquetMetaData>>,
    > = std::sync::OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

/// Conservative raw-text prune hints derived from query filters. These never
/// change correctness: the scan still re-evaluates the original predicate/UDF
/// above the reader, and this spec only lets the reader skip files/row groups/
/// rows it can prove cannot match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawPruneSpec {
    /// Column the prune hints apply to.
    pub column: String,
    /// Exact normalized tokens that must all be present somewhere in the file.
    pub all_terms: Vec<String>,
    /// Exact normalized tokens where at least one may be present.
    pub any_terms: Vec<String>,
    /// Substrings/prefixes answered conservatively by the trigram blooms.
    pub substrings: Vec<String>,
    /// Substrings eligible for inverted-index row-selection supersets.
    pub index_substrings: Vec<String>,
    /// `true` when this spec originated from WI-2 FTS UDF extraction.
    pub fts_udf: bool,
    /// Whether the reader may load a per-file inverted index and turn its
    /// postings into a row selection. File and row-group bloom pruning stay
    /// active when this is false.
    pub inverted_index_row_selection: bool,
}

impl Default for RawPruneSpec {
    fn default() -> Self {
        Self {
            column: "raw".to_string(),
            all_terms: Vec::new(),
            any_terms: Vec::new(),
            substrings: Vec::new(),
            index_substrings: Vec::new(),
            fts_udf: false,
            inverted_index_row_selection: true,
        }
    }
}

impl RawPruneSpec {
    /// Back-compat constructor for the legacy single-substring prune channel.
    pub fn from_raw_substring_filter(substr: Option<String>) -> Option<Self> {
        substr.map(|substr| Self {
            column: "raw".to_string(),
            all_terms: Vec::new(),
            any_terms: Vec::new(),
            substrings: vec![substr.clone()],
            index_substrings: vec![substr],
            fts_udf: false,
            inverted_index_row_selection: true,
        })
    }

    /// Whether the spec carries no usable prune hints.
    pub fn is_empty(&self) -> bool {
        self.all_terms.is_empty()
            && self.any_terms.is_empty()
            && self.substrings.is_empty()
            && self.index_substrings.is_empty()
    }
}

/// WS-7 promoted-column prune hint: `attr_get(attributes, key) = value`
/// (or `IN` list) where `key` is promoted to a typed Utf8 `column`. Sound to
/// prune from the COLUMN's statistics only for files that carry the column
/// (post-promotion files, where the column IS the extraction of the key);
/// files lacking the column are never pruned — their `attributes` JSON may
/// still contain the key. Like [`RawPruneSpec`], hints only: the engine
/// re-applies the original predicate above the scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotedPruneSpec {
    /// Promoted Utf8 column name (top-level primitive).
    pub column: String,
    /// Equality candidates: a row group survives when ANY value can be
    /// present per the column's min/max statistics.
    pub values: Vec<String>,
}

/// Per-scan pruning + IO counters, shared across every file task of one scan
/// stream. Increments sit on per-file / per-row-group control paths (never
/// per row), so relaxed atomics are free; the engine above snapshots the
/// totals when the scan finishes to attribute WHAT the scan skipped — the
/// per-request pruning-effectiveness view the global Prometheus counters
/// can't give.
#[derive(Debug, Default)]
pub struct ScanCounters {
    /// File tasks whose Parquet footer was opened.
    pub files_read: std::sync::atomic::AtomicU64,
    /// Whole files skipped by the raw trigram/token bloom before any data read.
    pub files_pruned_bloom: std::sync::atomic::AtomicU64,
    /// Row groups in scope after the task's byte-range split.
    pub row_groups_considered: std::sync::atomic::AtomicU64,
    /// Row groups dropped by per-row-group raw blooms.
    pub row_groups_pruned_bloom: std::sync::atomic::AtomicU64,
    /// Row groups dropped by predicate min/max statistics.
    pub row_groups_pruned_stats: std::sync::atomic::AtomicU64,
    /// Row groups actually handed to the Parquet reader.
    pub row_groups_read: std::sync::atomic::AtomicU64,
    /// Rows inside the read row groups skipped before decode by row
    /// selections (page index, positional deletes, inverted index).
    pub rows_pruned_selection: std::sync::atomic::AtomicU64,
    /// Object-store read requests issued (footer + index + data ranges).
    pub object_store_reads: std::sync::atomic::AtomicU64,
    /// Fetched bytes split by WHAT the bytes were, not which object they came
    /// from (cross-review F-5). A single `fetched_bytes` total cannot say
    /// whether a cold aggregate is footer-bound or column-bound, which is the
    /// difference between "the footers are too fat" and "the scan is too wide"
    /// — opposite fixes. The global `siglake_object_store_read_bytes_total`
    /// already carries the same split; these make it answerable PER REQUEST,
    /// which is what `stats.scan` needs to be self-explaining.
    pub bytes_footer: std::sync::atomic::AtomicU64,
    /// Page/offset/column-index and siglake's own index blobs.
    pub bytes_index: std::sync::atomic::AtomicU64,
    /// Column-chunk data pages — the bytes a projection actually asked for.
    pub bytes_data: std::sync::atomic::AtomicU64,
    /// Anything not classified above (manifests, stray reads).
    pub bytes_other: std::sync::atomic::AtomicU64,
}

impl ScanCounters {
    /// Add `bytes` to the class matching `phase` (cross-review F-5).
    ///
    /// Split out of the recording site so the mapping is unit-testable: as an
    /// inline `match` it was only reachable through a live scan, and a test
    /// that went through the GLOBAL phase metrics passed even with the arms
    /// deliberately swapped — the classic vacuous test.
    pub fn add_phase_bytes(&self, phase: ObjectStoreReadPhase, bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let slot = match phase {
            ObjectStoreReadPhase::Footer => &self.bytes_footer,
            ObjectStoreReadPhase::Index => &self.bytes_index,
            ObjectStoreReadPhase::Data => &self.bytes_data,
            ObjectStoreReadPhase::Manifest | ObjectStoreReadPhase::Other => &self.bytes_other,
        };
        slot.fetch_add(bytes, Relaxed);
    }
}


/// Options for tuning Parquet file I/O.
#[derive(Clone, Copy, Debug, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub(crate) struct ParquetReadOptions {
    /// Number of bytes to prefetch for parsing the Parquet metadata.
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    ///
    /// Defaults to 512 KiB, matching DataFusion's default `ParquetOptions::metadata_size_hint`.
    #[builder(default = Some(DEFAULT_METADATA_SIZE_HINT))]
    pub(crate) metadata_size_hint: Option<usize>,
    /// Gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
    #[builder(default = DEFAULT_RANGE_COALESCE_BYTES)]
    pub(crate) range_coalesce_bytes: u64,
    /// Maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
    #[builder(default = DEFAULT_RANGE_FETCH_CONCURRENCY)]
    pub(crate) range_fetch_concurrency: usize,
    /// Whether to preload the column index when reading Parquet metadata.
    #[builder(default = true)]
    pub(crate) preload_column_index: bool,
    /// Whether to preload the offset index when reading Parquet metadata.
    #[builder(default = true)]
    pub(crate) preload_offset_index: bool,
    /// Whether to preload the page index when reading Parquet metadata.
    #[builder(default = false)]
    pub(crate) preload_page_index: bool,
}

impl ParquetReadOptions {
    pub(crate) fn metadata_size_hint(&self) -> Option<usize> {
        self.metadata_size_hint
    }

    pub(crate) fn range_coalesce_bytes(&self) -> u64 {
        self.range_coalesce_bytes
    }

    pub(crate) fn range_fetch_concurrency(&self) -> usize {
        self.range_fetch_concurrency
    }

    pub(crate) fn preload_column_index(&self) -> bool {
        self.preload_column_index
    }

    pub(crate) fn preload_offset_index(&self) -> bool {
        self.preload_offset_index
    }

    pub(crate) fn preload_page_index(&self) -> bool {
        self.preload_page_index
    }
}

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    concurrency_limit_data_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    /// Optional per-scan counter for actual object-store bytes read; lets a
    /// caller attribute fetched bytes to one scan without the process-global
    /// counter (see also `object_store_bytes_read`).
    byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Optional per-scan pruning/IO counters (see [`ScanCounters`]).
    scan_counters: Option<Arc<ScanCounters>>,
    /// Optional conservative prune spec for the indexed `raw` column.
    raw_prune_spec: Option<RawPruneSpec>,
    /// WS-7 promoted-column prune hints (conjunctive; may be empty).
    promoted_prune: Vec<PromotedPruneSpec>,
    /// WS-3: emit batches strictly in task order (ordered prefetch instead of
    /// unordered flattening), so a stream of time-disjoint `timestamp ASC`
    /// files yields a globally sorted partition stream.
    output_order_preserved: bool,
    /// Reverse the physical scan within each file: row groups back-to-front,
    /// then rows within each emitted batch back-to-front.
    reverse: bool,
    /// Explicit reversed-tail chunk size; `None` reads the environment.
    reversed_chunk_rows: Option<usize>,
    /// Skip the process-global immutable footer/Puffin caches for this reader.
    cache_bypass: bool,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO) -> Self {
        let num_cpus = available_parallelism().get();

        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            concurrency_limit_data_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
            parquet_read_options: ParquetReadOptions::builder().build(),
            byte_counter: None,
            scan_counters: None,
            raw_prune_spec: None,
            promoted_prune: Vec::new(),
            output_order_preserved: false,
            reverse: false,
            reversed_chunk_rows: None,
            cache_bypass: false,
        }
    }

    /// Attach a per-scan counter that accumulates actual object-store bytes
    /// read by this reader (footer + data), in addition to the global counter.
    pub fn with_byte_counter(mut self, counter: Arc<std::sync::atomic::AtomicU64>) -> Self {
        self.byte_counter = Some(counter);
        self
    }

    /// Attach per-scan pruning/IO counters (see [`ScanCounters`]).
    pub fn with_scan_counters(mut self, counters: Option<Arc<ScanCounters>>) -> Self {
        self.scan_counters = counters;
        self
    }

    /// Back-compat shim for the old single-substring pruning path.
    pub fn with_raw_substring_filter(mut self, term: Option<String>) -> Self {
        self.raw_prune_spec = RawPruneSpec::from_raw_substring_filter(term);
        self
    }

    /// Set a conservative prune spec for the indexed `raw` column.
    pub fn with_raw_prune_spec(mut self, spec: Option<RawPruneSpec>) -> Self {
        self.raw_prune_spec = spec.filter(|spec| !spec.is_empty());
        self
    }

    /// Set WS-7 promoted-column prune hints (see [`PromotedPruneSpec`]).
    pub fn with_promoted_prune(mut self, specs: Vec<PromotedPruneSpec>) -> Self {
        self.promoted_prune = specs;
        self
    }

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.concurrency_limit_data_files = val;
        self
    }

    /// WS-3: when `true`, batches are emitted strictly in task order — files
    /// still open/plan concurrently (up to the data-file concurrency limit),
    /// but their batch streams are drained sequentially. Required when the
    /// caller advertises the concatenated stream as sorted (a run of
    /// time-disjoint `timestamp ASC` files); the default unordered flattening
    /// interleaves batches across files.
    pub fn with_output_order_preserved(mut self, preserved: bool) -> Self {
        self.output_order_preserved = preserved;
        self
    }

    /// Reverse the physical scan within each file: row groups back-to-front,
    /// then rows within each emitted batch back-to-front.
    pub fn with_reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }

    /// Pin the reversed-tail chunk size for this reader.
    pub fn with_reversed_chunk_rows(mut self, rows: usize) -> Self {
        self.reversed_chunk_rows = Some(rows);
        self
    }

    /// Bypass the process-global immutable reader caches for this scan.
    pub fn with_cache_bypass(mut self, bypass: bool) -> Self {
        self.cache_bypass = bypass;
        self
    }

    /// Sets the desired size of batches in the response
    /// to something other than the default
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Determines whether to enable row group filtering.
    pub fn with_row_group_filtering_enabled(mut self, row_group_filtering_enabled: bool) -> Self {
        self.row_group_filtering_enabled = row_group_filtering_enabled;
        self
    }

    /// Determines whether to enable row selection.
    pub fn with_row_selection_enabled(mut self, row_selection_enabled: bool) -> Self {
        self.row_selection_enabled = row_selection_enabled;
        self
    }

    /// Provide a hint as to the number of bytes to prefetch for parsing the Parquet metadata
    ///
    /// This hint can help reduce the number of fetch requests. For more details see the
    /// [ParquetMetaDataReader documentation](https://docs.rs/parquet/latest/parquet/file/metadata/struct.ParquetMetaDataReader.html#method.with_prefetch_hint).
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.parquet_read_options.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Sets the gap threshold for merging nearby byte ranges into a single request.
    /// Ranges with gaps smaller than this value will be coalesced.
    ///
    /// Defaults to 1 MiB, matching object_store's OBJECT_STORE_COALESCE_DEFAULT.
    pub fn with_range_coalesce_bytes(mut self, range_coalesce_bytes: u64) -> Self {
        self.parquet_read_options.range_coalesce_bytes = range_coalesce_bytes;
        self
    }

    /// Sets the maximum number of merged byte ranges to fetch concurrently.
    ///
    /// Defaults to 10, matching object_store's OBJECT_STORE_COALESCE_PARALLEL.
    pub fn with_range_fetch_concurrency(mut self, range_fetch_concurrency: usize) -> Self {
        self.parquet_read_options.range_fetch_concurrency = range_fetch_concurrency;
        self
    }

    /// Build the ArrowReader.
    pub fn build(self) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io.clone(),
            delete_file_loader: CachingDeleteFileLoader::new(
                self.file_io.clone(),
                self.concurrency_limit_data_files,
            ),
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            parquet_read_options: self.parquet_read_options,
            byte_counter: self.byte_counter,
            scan_counters: self.scan_counters,
            raw_prune_spec: self.raw_prune_spec,
            promoted_prune: self.promoted_prune,
            output_order_preserved: self.output_order_preserved,
            reverse: self.reverse,
            reversed_chunk_rows: self.reversed_chunk_rows,
            cache_bypass: self.cache_bypass,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    delete_file_loader: CachingDeleteFileLoader,

    /// the maximum number of data files that can be fetched at the same time
    concurrency_limit_data_files: usize,

    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
    scan_counters: Option<Arc<ScanCounters>>,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    output_order_preserved: bool,
    reverse: bool,
    reversed_chunk_rows: Option<usize>,
    cache_bypass: bool,
}

impl ArrowReader {
    /// Take a stream of FileScanTasks and reads all the files.
    /// Returns a stream of Arrow RecordBatches containing the data from the files
    pub fn read(self, tasks: FileScanTaskStream) -> Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        let concurrency_limit_data_files = self.concurrency_limit_data_files;
        let row_group_filtering_enabled = self.row_group_filtering_enabled;
        let row_selection_enabled = self.row_selection_enabled;
        let parquet_read_options = self.parquet_read_options;
        let byte_counter = self.byte_counter.clone();
        let scan_counters = self.scan_counters.clone();
        let raw_prune_spec = self.raw_prune_spec.clone();
        let promoted_prune = self.promoted_prune.clone();
        let reverse = self.reverse;
        let reversed_chunk_rows = self.reversed_chunk_rows;
        let cache_bypass = self.cache_bypass;

        // Fast-path for single concurrency to avoid overhead of try_flatten_unordered
        let stream: ArrowRecordBatchStream = if concurrency_limit_data_files == 1 {
            Box::pin(
                tasks
                    .and_then(move |task| {
                        let file_io = file_io.clone();
                        let byte_counter = byte_counter.clone();
                        let scan_counters = scan_counters.clone();
                        let raw_prune_spec = raw_prune_spec.clone();
                        let promoted_prune = promoted_prune.clone();

                        Self::process_file_scan_task(
                            task,
                            batch_size,
                            file_io,
                            self.delete_file_loader.clone(),
                            row_group_filtering_enabled,
                            row_selection_enabled,
                            parquet_read_options,
                            byte_counter,
                            scan_counters,
                            raw_prune_spec,
                            promoted_prune,
                            reverse,
                            reversed_chunk_rows,
                            cache_bypass,
                        )
                    })
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_flatten(),
            )
        } else if self.output_order_preserved {
            // WS-3 ordered mode: open/plan up to `concurrency_limit` files
            // concurrently, but drain their batch streams strictly in task
            // order, so a run of time-disjoint sorted files concatenates into
            // a sorted partition stream. Non-front tasks may prefetch ahead,
            // but their buffered batches are byte-budgeted so an inexact
            // filter above the scan can't force the ordered drain to retain
            // entire files waiting for earlier tasks.
            Box::pin(
                OrderedRecordBatchDrain::new(
                    tasks.map(move |task| {
                        let file_io = file_io.clone();
                        let byte_counter = byte_counter.clone();
                        let scan_counters = scan_counters.clone();
                        let delete_file_loader = self.delete_file_loader.clone();
                        let raw_prune_spec = raw_prune_spec.clone();
                        let promoted_prune = promoted_prune.clone();

                        async move {
                            let task = task.map_err(|err| {
                                Error::new(
                                    ErrorKind::Unexpected,
                                    "file scan task generate failed",
                                )
                                .with_source(err)
                            })?;
                            Self::process_file_scan_task(
                                task,
                                batch_size,
                                file_io,
                                delete_file_loader,
                                row_group_filtering_enabled,
                                row_selection_enabled,
                                parquet_read_options,
                                byte_counter,
                                scan_counters,
                                raw_prune_spec,
                                promoted_prune,
                                reverse,
                                reversed_chunk_rows,
                                cache_bypass,
                            )
                            .await
                        }
                        .boxed()
                    }),
                    concurrency_limit_data_files,
                    "iceberg_reader",
                ),
            )
        } else {
            Box::pin(
                tasks
                    .map_ok(move |task| {
                        let file_io = file_io.clone();
                        let byte_counter = byte_counter.clone();
                        let scan_counters = scan_counters.clone();
                        let raw_prune_spec = raw_prune_spec.clone();
                        let promoted_prune = promoted_prune.clone();

                        Self::process_file_scan_task(
                            task,
                            batch_size,
                            file_io,
                            self.delete_file_loader.clone(),
                            row_group_filtering_enabled,
                            row_selection_enabled,
                            parquet_read_options,
                            byte_counter,
                            scan_counters,
                            raw_prune_spec,
                            promoted_prune,
                            reverse,
                            reversed_chunk_rows,
                            cache_bypass,
                        )
                    })
                    .map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "file scan task generate failed")
                            .with_source(err)
                    })
                    .try_buffer_unordered(concurrency_limit_data_files)
                    .try_flatten_unordered(concurrency_limit_data_files),
            )
        };

        Ok(stream)
    }

    async fn process_file_scan_task(
        task: FileScanTask,
        batch_size: Option<usize>,
        file_io: FileIO,
        delete_file_loader: CachingDeleteFileLoader,
        row_group_filtering_enabled: bool,
        row_selection_enabled: bool,
        parquet_read_options: ParquetReadOptions,
        byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
        scan_counters: Option<Arc<ScanCounters>>,
        raw_prune_spec: Option<RawPruneSpec>,
        promoted_prune: Vec<PromotedPruneSpec>,
        reverse: bool,
        reversed_chunk_rows_override: Option<usize>,
        cache_bypass: bool,
    ) -> Result<ArrowRecordBatchStream> {
        let should_load_page_index =
            (row_selection_enabled && task.predicate.is_some()) || !task.deletes.is_empty();
        let mut parquet_read_options = parquet_read_options;
        parquet_read_options.preload_page_index = should_load_page_index;

        let delete_filter_rx =
            delete_file_loader.load_deletes(&task.deletes, Arc::clone(&task.schema));

        // Open the Parquet file once, loading its metadata
        let byte_counter_for_chunks = byte_counter.clone();
        let (parquet_file_reader, arrow_metadata) = Self::open_parquet_file(
            &task.data_file_path,
            &file_io,
            task.file_size_in_bytes,
            parquet_read_options,
            byte_counter,
            scan_counters.clone(),
            cache_bypass,
        )
        .await?;
        if let Some(counters) = scan_counters.as_ref() {
            counters
                .files_read
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Raw trigram-bloom file skip: when a `raw LIKE '%substr%'` filter is
        // set and this file's stored trigram bloom proves some trigram of
        // `substr` absent, the file cannot contain the substring — skip it
        // without reading data. Blooms never false-negate, so a matching row
        // is never dropped. Files with only the older token bloom (different
        // KV key) are simply not skipped here — correctly scanned.
        if let Some(spec) = raw_prune_spec.as_ref() {
            if !Self::file_might_match_prune_spec(&file_io, &task, arrow_metadata.metadata(), spec)
                .await?
            {
                if let Some(counters) = scan_counters.as_ref() {
                    counters
                        .files_pruned_bloom
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                return Ok(futures::stream::empty::<Result<RecordBatch>>().boxed());
            }
        }

        // (row-group bloom survivor computation lives in `rowgroup_bloom_survivors`)

        // Check if Parquet file has embedded field IDs
        // Corresponds to Java's ParquetSchemaUtil.hasIds()
        // Reference: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:118
        let missing_field_ids = arrow_metadata
            .schema()
            .fields()
            .iter()
            .next()
            .is_some_and(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none());

        // Three-branch schema resolution strategy matching Java's ReadConf constructor
        //
        // Per Iceberg spec Column Projection rules:
        // "Columns in Iceberg data files are selected by field id. The table schema's column
        //  names and order may change after a data file is written, and projection must be done
        //  using field ids."
        // https://iceberg.apache.org/spec/#column-projection
        //
        // When Parquet files lack field IDs (e.g., Hive/Spark migrations via add_files),
        // we must assign field IDs BEFORE reading data to enable correct projection.
        //
        // Java's ReadConf determines field ID strategy:
        // - Branch 1: hasIds(fileSchema) → trust embedded field IDs, use pruneColumns()
        // - Branch 2: nameMapping present → applyNameMapping(), then pruneColumns()
        // - Branch 3: fallback → addFallbackIds(), then pruneColumnsFallback()
        let arrow_metadata = if missing_field_ids {
            // Parquet file lacks field IDs - must assign them before reading
            let arrow_schema = if let Some(name_mapping) = &task.name_mapping {
                // Branch 2: Apply name mapping to assign correct Iceberg field IDs
                // Per spec rule #2: "Use schema.name-mapping.default metadata to map field id
                // to columns without field id"
                // Corresponds to Java's ParquetSchemaUtil.applyNameMapping()
                apply_name_mapping_to_arrow_schema(
                    Arc::clone(arrow_metadata.schema()),
                    name_mapping,
                )?
            } else {
                // Branch 3: No name mapping - use position-based fallback IDs
                // Corresponds to Java's ParquetSchemaUtil.addFallbackIds()
                add_fallback_field_ids_to_arrow_schema(arrow_metadata.schema())
            };

            let options = ArrowReaderOptions::new().with_schema(arrow_schema);
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "Failed to create ArrowReaderMetadata with field ID schema",
                    )
                    .with_source(e)
                },
            )?
        } else {
            // Branch 1: File has embedded field IDs - trust them
            arrow_metadata
        };

        // Coerce INT96 timestamp columns to the resolution specified by the Iceberg schema.
        // This must happen before building the stream reader to avoid i64 overflow in arrow-rs.
        let arrow_metadata = if let Some(coerced_schema) =
            coerce_int96_timestamps(arrow_metadata.schema(), &task.schema)
        {
            let options = ArrowReaderOptions::new().with_schema(Arc::clone(&coerced_schema));
            ArrowReaderMetadata::try_new(Arc::clone(arrow_metadata.metadata()), options).map_err(
                |e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Failed to create ArrowReaderMetadata with INT96-coerced schema: {coerced_schema}"
                        ),
                    )
                    .with_source(e)
                },
            )?
        } else {
            arrow_metadata
        };

        // Build the stream reader, reusing the already-opened file reader.
        // The RESOLVED metadata (field-id + INT96 fixes applied) is kept for
        // the reversed tail-chunk path, whose per-chunk builders reuse it.
        let resolved_metadata_for_chunks = arrow_metadata.clone();
        let mut record_batch_stream_builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(parquet_file_reader, arrow_metadata);

        let delete_filter = delete_filter_rx.await.unwrap()?;
        let delete_predicate = delete_filter.build_equality_delete_predicate(&task).await?;

        // In addition to the optional predicate supplied in the `FileScanTask`,
        // we also have an optional predicate resulting from equality delete files.
        // If both are present, we logical-AND them together to form a single filter
        // predicate that we can pass to the `RecordBatchStreamBuilder`.
        let final_predicate = match (&task.predicate, delete_predicate) {
            (None, None) => None,
            (Some(predicate), None) => Some(predicate.clone()),
            (None, Some(ref predicate)) => Some(predicate.clone()),
            (Some(filter_predicate), Some(delete_predicate)) => {
                Some(filter_predicate.clone().and(delete_predicate))
            }
        };
        // Kept for the reversed tail-chunk path: each chunk's builder attaches
        // its own decode-time row filter (Exact pushdowns elide the engine's
        // FilterExec above the scan, so this filter is load-bearing).
        let predicate_for_chunks = final_predicate.clone();

        // Filter out metadata fields for Parquet projection (they don't exist in files).
        // When the output projection is empty (e.g. count(*)), still project the
        // filter columns needed by predicate pushdown so we don't read unrelated
        // physical columns such as `raw`.
        let mut parquet_project_field_ids: Vec<i32> = task
            .project_field_ids
            .iter()
            .filter(|&&id| !is_metadata_field(id))
            .copied()
            .collect();
        if let Some(predicate) = final_predicate.as_ref() {
            let mut predicate_field_ids =
                Self::collect_field_ids(predicate)?.into_iter().collect::<Vec<_>>();
            predicate_field_ids.sort_unstable();
            for field_id in predicate_field_ids {
                if !parquet_project_field_ids.contains(&field_id) {
                    parquet_project_field_ids.push(field_id);
                }
            }
        }

        // Create projection mask based on field IDs
        // - If file has embedded IDs: field-ID-based projection (missing_field_ids=false)
        // - If name mapping applied: field-ID-based projection (missing_field_ids=true but IDs now match)
        // - If fallback IDs: position-based projection (missing_field_ids=true)
        let projection_mask = Self::get_arrow_projection_mask(
            &parquet_project_field_ids,
            &task.schema,
            record_batch_stream_builder.parquet_schema(),
            record_batch_stream_builder.schema(),
            missing_field_ids, // Whether to use position-based (true) or field-ID-based (false) projection
        )?;

        record_batch_stream_builder =
            record_batch_stream_builder.with_projection(projection_mask.clone());

        // RecordBatchTransformer performs any transformations required on the RecordBatches
        // that come back from the file, such as type promotion, default column insertion,
        // column re-ordering, partition constants, and virtual field addition (like _file)
        let mut record_batch_transformer_builder =
            RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());

        // Add the _file metadata column if it's in the projected fields
        if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
            let file_datum = Datum::string(task.data_file_path.clone());
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_constant(RESERVED_FIELD_ID_FILE, file_datum);
        }

        if let (Some(partition_spec), Some(partition_data)) =
            (task.partition_spec.clone(), task.partition.clone())
        {
            record_batch_transformer_builder =
                record_batch_transformer_builder.with_partition(partition_spec, partition_data)?;
        }

        let mut record_batch_transformer = record_batch_transformer_builder.build();

        if let Some(batch_size) = batch_size {
            record_batch_stream_builder = record_batch_stream_builder.with_batch_size(batch_size);
        }

        // There are three possible sources for potential lists of selected RowGroup indices,
        // and two for `RowSelection`s.
        // Selected RowGroup index lists can come from three sources:
        //   * When task.start and task.length specify a byte range (file splitting);
        //   * When there are equality delete files that are applicable;
        //   * When there is a scan predicate and row_group_filtering_enabled = true.
        // `RowSelection`s can be created in either or both of the following cases:
        //   * When there are positional delete files that are applicable;
        //   * When there is a scan predicate and row_selection_enabled = true
        // Note that row group filtering from predicates only happens when
        // there is a scan predicate AND row_group_filtering_enabled = true,
        // but we perform row selection filtering if there are applicable
        // equality delete files OR (there is a scan predicate AND row_selection_enabled),
        // since the only implemented method of applying positional deletes is
        // by using a `RowSelection`.
        let mut selected_row_group_indices = None;
        let mut row_selection = None;

        // Filter row groups based on byte range from task.start and task.length.
        // If both start and length are 0, read the entire file (backwards compatibility).
        if task.start != 0 || task.length != 0 {
            let byte_range_filtered_row_groups = Self::filter_row_groups_by_byte_range(
                record_batch_stream_builder.metadata(),
                task.start,
                task.length,
            )?;
            selected_row_group_indices = Some(byte_range_filtered_row_groups);
        }

        // Row groups in scope for this task (after any byte-range split) — the
        // baseline the per-scan pruning counters measure against.
        let scan_rg_total = record_batch_stream_builder.metadata().num_row_groups();
        let rg_in_scope = selected_row_group_indices
            .as_ref()
            .map_or(scan_rg_total, Vec::len);

        // Raw token-bloom row-group skip: drop row groups whose per-row-group
        // token bloom proves the term absent. Applied here — before the row
        // selection is computed below — so the selection stays aligned to the
        // surviving row groups. Like the file-level skip, it never false-negates.
        if let Some(spec) = raw_prune_spec.as_ref() {
            if let Some(survivors) =
                Self::rowgroup_bloom_survivors_for_spec(record_batch_stream_builder.metadata(), spec)
            {
                selected_row_group_indices = Some(match selected_row_group_indices {
                    Some(existing) => existing
                        .into_iter()
                        .filter(|idx| survivors.contains(idx))
                        .collect(),
                    None => survivors,
                });
            }
        }

        let rg_after_bloom = selected_row_group_indices
            .as_ref()
            .map_or(scan_rg_total, Vec::len);

        // WS-7 promoted-column prune: drop row groups whose promoted-column
        // min/max statistics prove `attr_get(attributes, key) = value` cannot
        // match. Only files that CARRY the column prune (the column is the
        // extraction of the key there); pre-promotion files pass through
        // untouched. Counted in the per-scan `row_groups_pruned_stats`
        // (applied after the bloom capture above, before the final tally).
        if !promoted_prune.is_empty() {
            let metadata = record_batch_stream_builder.metadata();
            let candidates: Vec<usize> = match &selected_row_group_indices {
                Some(idxs) => idxs.clone(),
                None => (0..metadata.num_row_groups()).collect(),
            };
            let survivors: Vec<usize> = candidates
                .into_iter()
                .filter(|&i| {
                    let rg = metadata.row_group(i);
                    promoted_prune
                        .iter()
                        .all(|spec| Self::row_group_might_match_promoted(rg, spec))
                })
                .collect();
            selected_row_group_indices = Some(survivors);
        }

        if let Some(predicate) = final_predicate {
            let (iceberg_field_ids, field_id_map) = Self::build_field_id_set_and_map(
                record_batch_stream_builder.parquet_schema(),
                &predicate,
            )?;

            let row_filter = Self::get_row_filter(
                &predicate,
                record_batch_stream_builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            record_batch_stream_builder = record_batch_stream_builder.with_row_filter(row_filter);

            if row_group_filtering_enabled {
                let predicate_filtered_row_groups = Self::get_selected_row_group_indices(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &field_id_map,
                    &task.schema,
                )?;

                // Merge predicate-based filtering with byte range filtering (if present)
                // by taking the intersection of both filters
                selected_row_group_indices = match selected_row_group_indices {
                    Some(byte_range_filtered) => {
                        // Keep only row groups that are in both filters
                        let intersection: Vec<usize> = byte_range_filtered
                            .into_iter()
                            .filter(|idx| predicate_filtered_row_groups.contains(idx))
                            .collect();
                        Some(intersection)
                    }
                    None => Some(predicate_filtered_row_groups),
                };
            }

            if row_selection_enabled {
                row_selection = Some(Self::get_row_selection_for_filter_predicate(
                    &predicate,
                    record_batch_stream_builder.metadata(),
                    &selected_row_group_indices,
                    &field_id_map,
                    &task.schema,
                )?);
            }
        }

        let positional_delete_indexes = delete_filter.get_delete_vector(&task);

        if let Some(positional_delete_indexes) = positional_delete_indexes {
            let delete_row_selection = {
                let positional_delete_indexes = positional_delete_indexes.lock().unwrap();

                Self::build_deletes_row_selection(
                    record_batch_stream_builder.metadata().row_groups(),
                    &selected_row_group_indices,
                    &positional_delete_indexes,
                )
            }?;

            // merge the row selection from the delete files with the row selection
            // from the filter predicate, if there is one from the filter predicate
            row_selection = match row_selection {
                None => Some(delete_row_selection),
                Some(filter_row_selection) => {
                    Some(filter_row_selection.intersection(&delete_row_selection))
                }
            };
        }

        // WS-5: row-level prune via the per-file inverted index. For a
        // normalizable `raw LIKE '%substr%'` filter, intersect in a row selection
        // of the rows whose index postings contain the substring — a *superset*
        // of the true matches (the query engine re-applies the exact `LIKE`
        // above the scan), so non-matching rows are skipped before decode. No-op
        // when the file carries no index blob or the substring isn't answerable.
        if let Some(spec) = raw_prune_spec
            .as_ref()
            .filter(|spec| spec.inverted_index_row_selection)
        {
            if let Some(index_selection) = Self::inverted_index_row_selection(
                &file_io,
                &task,
                record_batch_stream_builder.metadata(),
                &selected_row_group_indices,
                spec,
                cache_bypass,
            )
            .await?
            {
                row_selection = match row_selection {
                    None => Some(index_selection),
                    Some(existing) => Some(existing.intersection(&index_selection)),
                };
            }
        }

        // Per-scan pruning accounting: attribute this task's row-group drops
        // to their mechanism, and count the rows the final row selection skips
        // inside the surviving groups. Counts, not decisions — the filtering
        // above is unchanged.
        if let Some(counters) = scan_counters.as_ref() {
            use std::sync::atomic::Ordering::Relaxed;
            let rg_final = selected_row_group_indices
                .as_ref()
                .map_or(scan_rg_total, Vec::len);
            counters
                .row_groups_considered
                .fetch_add(rg_in_scope as u64, Relaxed);
            counters
                .row_groups_pruned_bloom
                .fetch_add(rg_in_scope.saturating_sub(rg_after_bloom) as u64, Relaxed);
            counters
                .row_groups_pruned_stats
                .fetch_add(rg_after_bloom.saturating_sub(rg_final) as u64, Relaxed);
            counters.row_groups_read.fetch_add(rg_final as u64, Relaxed);
            if let Some(selection) = row_selection.as_ref() {
                let metadata = record_batch_stream_builder.metadata();
                let rows_in_read_groups: u64 = match selected_row_group_indices.as_ref() {
                    Some(idxs) => idxs
                        .iter()
                        .map(|&i| metadata.row_group(i).num_rows() as u64)
                        .sum(),
                    None => metadata
                        .row_groups()
                        .iter()
                        .map(|rg| rg.num_rows() as u64)
                        .sum(),
                };
                counters.rows_pruned_selection.fetch_add(
                    rows_in_read_groups.saturating_sub(selection.row_count() as u64),
                    Relaxed,
                );
            }
        }

        if reverse {
            // REVERSED emission (2026-07-16 correctness finding + 07-17 perf
            // rework): parquet streams a row group's batches FRONT to BACK, so
            // per-batch row reversal alone leaves a multi-batch group in
            // ascending order. Every reversed read goes through the tail-chunk
            // plan: the selected rows of each group (ascending selection split
            // per group) are chunked TAIL-FIRST, and each chunk is read by its
            // own bounded builder — buffered fully, flushed in reverse batch
            // order. Group boundaries are inherent (one chunk never spans
            // groups), the decode-time row filter stays attached per chunk
            // (Exact pushdowns elide the engine's FilterExec, so it is
            // load-bearing), and an early-stopping LIMIT decodes only the
            // newest chunk instead of a whole converged giant group (the
            // 07-17 board's 277–330ms browse p50 / ~2.5M rows per LIMIT 100).
            let ascending: Vec<usize> = selected_row_group_indices
                .clone()
                .unwrap_or_else(|| (0..record_batch_stream_builder.metadata().num_row_groups()).collect());
            let group_rows: Vec<usize> = ascending
                .iter()
                .map(|&i| record_batch_stream_builder.metadata().row_group(i).num_rows() as usize)
                .collect();
            let per_group_selectors: Vec<Vec<RowSelector>> = match row_selection.take() {
                Some(selection) => split_selection_by_groups(selection, &group_rows),
                None => group_rows
                    .iter()
                    .map(|&r| vec![RowSelector::select(r)])
                    .collect(),
            };
            // `0` disables chunking: one chunk per group (whole-group flush).
            let max_chunk = match reversed_chunk_rows_override.unwrap_or_else(reversed_chunk_rows) {
                0 => usize::MAX,
                n => n,
            };
            let first_chunk = batch_size.unwrap_or(8192).min(max_chunk);
            let mut chunk_plan: Vec<(usize, Vec<RowSelector>, usize)> = Vec::new();
            for (pos, &g) in ascending.iter().enumerate().rev() {
                for (sel, rows) in tail_chunks_of_group_selection(
                    &per_group_selectors[pos],
                    first_chunk,
                    max_chunk,
                ) {
                    chunk_plan.push((g, sel, rows));
                }
            }
            return Ok(Self::reversed_chunked_stream(
                Arc::new(task),
                file_io.clone(),
                parquet_read_options,
                byte_counter_for_chunks,
                scan_counters.clone(),
                resolved_metadata_for_chunks,
                projection_mask,
                predicate_for_chunks,
                batch_size,
                chunk_plan,
            ));
        }

        if let Some(row_selection) = row_selection {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_selection(row_selection);
        }

        if let Some(selected_row_group_indices) = selected_row_group_indices {
            record_batch_stream_builder =
                record_batch_stream_builder.with_row_groups(selected_row_group_indices);
        }

        // Build the batch stream and send all the RecordBatches that it generates
        // to the requester.
        let record_batch_stream = record_batch_stream_builder.build()?.map(move |batch| {
            match batch {
                Ok(batch) => {
                    // Process the record batch (type promotion, column reordering, virtual fields, etc.)
                    record_batch_transformer.process_record_batch(batch)
                }
                Err(err) => Err(err.into()),
            }
        });

        Ok(Box::pin(record_batch_stream) as ArrowRecordBatchStream)
    }

    /// Reversed tail-chunk emission: one bounded builder per chunk, issued
    /// LAZILY in reversed order (`.then(...).flatten()` drains a chunk fully
    /// before opening the next), so an early-stopping `LIMIT` decodes only
    /// the newest chunk instead of a whole giant row group. Re-opening the
    /// file per chunk is metadata-free (footer/page-index cache) and each
    /// chunk's batches flush through [`ReversedGroupBatches`] with an exact
    /// expected count (reversed reads never attach the decode-time row
    /// filter, so selection rows == emitted rows).
    #[allow(clippy::too_many_arguments)]
    fn reversed_chunked_stream(
        task: Arc<FileScanTask>,
        file_io: FileIO,
        parquet_read_options: ParquetReadOptions,
        byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
        scan_counters: Option<Arc<ScanCounters>>,
        resolved_metadata: ArrowReaderMetadata,
        projection_mask: ProjectionMask,
        predicate: Option<BoundPredicate>,
        batch_size: Option<usize>,
        chunk_plan: Vec<(usize, Vec<RowSelector>, usize)>,
    ) -> ArrowRecordBatchStream {
        let stream = futures::stream::iter(chunk_plan)
            .then(move |(group_idx, selectors, expected_rows)| {
                let task = task.clone();
                let file_io = file_io.clone();
                let byte_counter = byte_counter.clone();
                let scan_counters = scan_counters.clone();
                let resolved_metadata = resolved_metadata.clone();
                let projection_mask = projection_mask.clone();
                let predicate = predicate.clone();
                async move {
                    // Filter-free chunks are a pure function of immutable
                    // file content — serve/decode-once via the shared cache.
                    let cache_key = (predicate.is_none()
                        && reversed_chunk_cache_max_bytes() > 0)
                        .then(|| ReversedChunkKey {
                            path: task.data_file_path.clone(),
                            group: group_idx,
                            selectors: selectors
                                .iter()
                                .map(|r| (r.skip, r.row_count))
                                .collect(),
                            batch_size,
                            field_ids: task.project_field_ids().to_vec(),
                        });
                    if let Some(key) = cache_key.clone() {
                        loop {
                            let waiter = {
                                let mut cache = reversed_chunk_cache().lock().unwrap();
                                if let Some((batches, _)) = cache.entries.get(&key) {
                                    let batches = batches.clone();
                                    return Box::pin(futures::stream::iter(
                                        batches.into_iter().map(Ok),
                                    ))
                                        as ArrowRecordBatchStream;
                                }
                                match cache.inflight.get(&key) {
                                    Some(notify) => notify.clone(),
                                    None => {
                                        cache.inflight.insert(
                                            key.clone(),
                                            Arc::new(tokio::sync::Notify::new()),
                                        );
                                        break;
                                    }
                                }
                            };
                            // BOUNDED. Unbounded, any stranded marker is a
                            // permanent hang for that chunk; bounded, the worst
                            // case is one wasted wait and then a normal decode.
                            if tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                waiter.notified(),
                            )
                            .await
                            .is_err()
                            {
                                break;
                            }
                        }
                    }
                    let opened = Self::open_reversed_chunk(
                        task,
                        file_io,
                        parquet_read_options,
                        byte_counter,
                        scan_counters,
                        resolved_metadata,
                        projection_mask,
                        predicate,
                        batch_size,
                        group_idx,
                        selectors,
                        expected_rows,
                    )
                    .await;
                    // Leaders collect the (bounded — ≤ chunk rows) stream so
                    // the decode result is shareable; waiters were released
                    // above on cache hit or retry the leader race.
                    // Armed BEFORE the awaits below, so a cancelled browse releases
                    // the marker on unwind instead of stranding it. `finish` now
                    // only publishes the batches; the removal and the wake belong
                    // to the guard, which runs on paths no closure is called on.
                    let _leader = ReversedChunkLeader(cache_key.clone());
                    let finish = |key: Option<ReversedChunkKey>,
                                  batches: Option<Vec<RecordBatch>>| {
                        if let Some(key) = key {
                            if let Some(batches) = batches {
                                reversed_chunk_cache().lock().unwrap().insert(key, batches);
                            }
                        }
                    };
                    match opened {
                        Ok(stream) => {
                            if cache_key.is_none() {
                                return stream;
                            }
                            match stream.try_collect::<Vec<RecordBatch>>().await {
                                Ok(batches) => {
                                    finish(cache_key, Some(batches.clone()));
                                    Box::pin(futures::stream::iter(
                                        batches.into_iter().map(Ok),
                                    ))
                                        as ArrowRecordBatchStream
                                }
                                Err(e) => {
                                    finish(cache_key, None);
                                    Box::pin(futures::stream::once(async move { Err(e) }))
                                        as ArrowRecordBatchStream
                                }
                            }
                        }
                        Err(e) => {
                            finish(cache_key, None);
                            Box::pin(futures::stream::once(async move { Err(e) }))
                                as ArrowRecordBatchStream
                        }
                    }
                }
            })
            .flatten();
        Box::pin(stream) as ArrowRecordBatchStream
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_reversed_chunk(
        task: Arc<FileScanTask>,
        file_io: FileIO,
        parquet_read_options: ParquetReadOptions,
        byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
        scan_counters: Option<Arc<ScanCounters>>,
        resolved_metadata: ArrowReaderMetadata,
        projection_mask: ProjectionMask,
        predicate: Option<BoundPredicate>,
        batch_size: Option<usize>,
        group_idx: usize,
        selectors: Vec<RowSelector>,
        _expected_rows: usize,
    ) -> Result<ArrowRecordBatchStream> {
        let (reader, _) = Self::open_parquet_file(
            &task.data_file_path,
            &file_io,
            task.file_size_in_bytes,
            parquet_read_options,
            byte_counter,
            scan_counters,
            false,
        )
        .await?;
        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, resolved_metadata);
        builder = builder.with_projection(projection_mask);
        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(predicate) = predicate.as_ref() {
            // Decode-time row filter, same as the forward path — load-bearing
            // for Exact pushdowns (no FilterExec above re-applies those).
            let (iceberg_field_ids, field_id_map) =
                Self::build_field_id_set_and_map(builder.parquet_schema(), predicate)?;
            let row_filter = Self::get_row_filter(
                predicate,
                builder.parquet_schema(),
                &iceberg_field_ids,
                &field_id_map,
            )?;
            builder = builder.with_row_filter(row_filter);
        }
        builder = builder.with_row_groups(vec![group_idx]);
        builder = builder.with_row_selection(selectors.into());
        // Fresh transformer per chunk — it is stateful per stream.
        let mut transformer_builder =
            RecordBatchTransformerBuilder::new(task.schema_ref(), task.project_field_ids());
        if task.project_field_ids().contains(&RESERVED_FIELD_ID_FILE) {
            transformer_builder = transformer_builder
                .with_constant(RESERVED_FIELD_ID_FILE, Datum::string(task.data_file_path.clone()));
        }
        if let (Some(partition_spec), Some(partition_data)) =
            (task.partition_spec.clone(), task.partition.clone())
        {
            transformer_builder = transformer_builder.with_partition(partition_spec, partition_data)?;
        }
        let mut transformer = transformer_builder.build();
        let stream = builder.build()?.map(move |batch| match batch {
            Ok(batch) => transformer.process_record_batch(batch),
            Err(err) => Err(err.into()),
        });
        Ok(Box::pin(ReversedGroupBatches::new(Box::pin(stream))) as ArrowRecordBatchStream)
    }

    /// Opens a Parquet file and loads its metadata, returning both the reader and metadata.
    /// The reader can be reused to build a `ParquetRecordBatchStreamBuilder` without
    /// reopening the file.
    pub(crate) async fn open_parquet_file(
        data_file_path: &str,
        file_io: &FileIO,
        file_size_in_bytes: u64,
        parquet_read_options: ParquetReadOptions,
        byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
        scan_counters: Option<Arc<ScanCounters>>,
        cache_bypass: bool,
    ) -> Result<(ArrowFileReader, ArrowReaderMetadata)> {
        let parquet_file = file_io.new_input(data_file_path)?;
        let parquet_reader = parquet_file.reader().await?;
        let reader = ArrowFileReader::new(
            FileMetadata {
                size: file_size_in_bytes,
            },
            parquet_reader,
        )
        .with_parquet_read_options(parquet_read_options)
        .with_byte_counter(byte_counter)
        .with_scan_counters(scan_counters);

        // Q5: reuse cached footer/page-index metadata for immutable data files,
        // skipping the footer + page-index object-store fetch on warm queries.
        if !cache_bypass && let Some(cached) = footer_cache_get(data_file_path) {
            let arrow_metadata =
                ArrowReaderMetadata::try_new(cached, Default::default()).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "Failed to build cached Parquet metadata")
                        .with_source(e)
                })?;
            return Ok((reader, arrow_metadata));
        }

        let prefetch = parquet_read_options
            .metadata_size_hint()
            .unwrap_or(8)
            .max(8) as u64;
        let metadata_key = ParquetMetadataKey {
            range: DebouncedRangeKey {
                path: data_file_path.to_string(),
                start: file_size_in_bytes.saturating_sub(prefetch),
                end: file_size_in_bytes,
            },
            preload_column_index: parquet_read_options.preload_column_index(),
            preload_offset_index: parquet_read_options.preload_offset_index(),
            preload_page_index: parquet_read_options.preload_page_index(),
        };
        let file_io = file_io.clone();
        let data_file_path = data_file_path.to_string();
        let cache_path = data_file_path.clone();
        let metadata = parquet_footer_debouncer()
            .run(metadata_key, "footer", move || async move {
                let parquet_file = file_io.new_input(&data_file_path)?;
                let parquet_reader = parquet_file.reader().await?;
                let mut metadata_reader = ArrowFileReader::new(
                    FileMetadata {
                        size: file_size_in_bytes,
                    },
                    parquet_reader,
                )
                .with_parquet_read_options(parquet_read_options)
                .with_byte_counter(None);
                metadata_reader.load_parquet_metadata().await
            })
            .await?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), Default::default()).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet metadata").with_source(e)
            })?;
        if !cache_bypass {
            footer_cache_put(&cache_path, metadata);
        }

        Ok((reader, arrow_metadata))
    }

    /// computes a `RowSelection` from positional delete indices.
    ///
    /// Using the Parquet page index, we build a `RowSelection` that rejects rows that are indicated
    /// as having been deleted by a positional delete, taking into account any row groups that have
    /// been skipped entirely by the filter predicate
    fn build_deletes_row_selection(
        row_group_metadata_list: &[RowGroupMetaData],
        selected_row_groups: &Option<Vec<usize>>,
        positional_deletes: &DeleteVector,
    ) -> Result<RowSelection> {
        let mut results: Vec<RowSelector> = Vec::new();
        let mut selected_row_groups_idx = 0;
        let mut current_row_group_base_idx: u64 = 0;
        let mut delete_vector_iter = positional_deletes.iter();
        let mut next_deleted_row_idx_opt = delete_vector_iter.next();

        for (idx, row_group_metadata) in row_group_metadata_list.iter().enumerate() {
            let row_group_num_rows = row_group_metadata.num_rows() as u64;
            let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

            // if row group selection is enabled,
            if let Some(selected_row_groups) = selected_row_groups {
                // if we've consumed all the selected row groups, we're done
                if selected_row_groups_idx == selected_row_groups.len() {
                    break;
                }

                if idx == selected_row_groups[selected_row_groups_idx] {
                    // we're in a selected row group. Increment selected_row_groups_idx
                    // so that next time around the for loop we're looking for the next
                    // selected row group
                    selected_row_groups_idx += 1;
                } else {
                    // Advance iterator past all deletes in the skipped row group.
                    // advance_to() positions the iterator to the first delete >= next_row_group_base_idx.
                    // However, if our cached next_deleted_row_idx_opt is in the skipped range,
                    // we need to call next() to update the cache with the newly positioned value.
                    delete_vector_iter.advance_to(next_row_group_base_idx);
                    // Only update the cache if the cached value is stale (in the skipped range)
                    if let Some(cached_idx) = next_deleted_row_idx_opt
                        && cached_idx < next_row_group_base_idx
                    {
                        next_deleted_row_idx_opt = delete_vector_iter.next();
                    }

                    // still increment the current page base index but then skip to the next row group
                    // in the file
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            }

            let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
                Some(next_deleted_row_idx) => {
                    // if the index of the next deleted row is beyond this row group, add a selection for
                    // the remainder of this row group and skip to the next row group
                    if next_deleted_row_idx >= next_row_group_base_idx {
                        results.push(RowSelector::select(row_group_num_rows as usize));
                        current_row_group_base_idx += row_group_num_rows;
                        continue;
                    }

                    next_deleted_row_idx
                }

                // If there are no more pos deletes, add a selector for the entirety of this row group.
                _ => {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
            };

            let mut current_idx = current_row_group_base_idx;
            'chunks: while next_deleted_row_idx < next_row_group_base_idx {
                // `select` all rows that precede the next delete index
                if current_idx < next_deleted_row_idx {
                    let run_length = next_deleted_row_idx - current_idx;
                    results.push(RowSelector::select(run_length as usize));
                    current_idx += run_length;
                }

                // `skip` all consecutive deleted rows in the current row group
                let mut run_length = 0;
                while next_deleted_row_idx == current_idx
                    && next_deleted_row_idx < next_row_group_base_idx
                {
                    run_length += 1;
                    current_idx += 1;

                    next_deleted_row_idx_opt = delete_vector_iter.next();
                    next_deleted_row_idx = match next_deleted_row_idx_opt {
                        Some(next_deleted_row_idx) => next_deleted_row_idx,
                        _ => {
                            // We've processed the final positional delete.
                            // Conclude the skip and then break so that we select the remaining
                            // rows in the row group and move on to the next row group
                            results.push(RowSelector::skip(run_length));
                            break 'chunks;
                        }
                    };
                }
                if run_length > 0 {
                    results.push(RowSelector::skip(run_length));
                }
            }

            if current_idx < next_row_group_base_idx {
                results.push(RowSelector::select(
                    (next_row_group_base_idx - current_idx) as usize,
                ));
            }

            current_row_group_base_idx += row_group_num_rows;
        }

        Ok(results.into())
    }

    /// WS-5: build a `RowSelection` keeping the rows whose per-file inverted
    /// index (footer KV [`siglake_index::INVERTED_INDEX_KV_KEY`]) match
    /// `raw LIKE '%substr%'`. Returns `Ok(None)` when the file has no index blob
    /// or the substring isn't index-answerable (delimiter-bearing / too short) —
    /// the caller then leaves decoding to the existing path.
    ///
    fn prune_source_label(spec: &RawPruneSpec) -> &'static str {
        if spec.fts_udf {
            "fts_udf"
        } else {
            "like_substring"
        }
    }

    fn file_trigram_bloom(metadata: &ParquetMetaData) -> Option<siglake_bloom::TokenBloom> {
        if metadata.file_metadata().key_value_metadata().is_none() {
            return None;
        }
        let hex = metadata.file_metadata().key_value_metadata().and_then(|kv| {
            kv.iter()
                .find(|e| e.key == siglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY)
                .and_then(|e| e.value.as_deref())
        })?;
        siglake_bloom::TokenBloom::from_hex(hex)
    }

    /// The hex blob `column`'s footer-KV index is stored as, undecoded — the
    /// presence check is what lets the caller take a load permit only when
    /// there is something to decode.
    fn footer_inverted_index_hex<'a>(
        metadata: &'a ParquetMetaData,
        column: &str,
    ) -> Option<&'a str> {
        let key = siglake_index::inverted_index_kv_key(column);
        metadata.file_metadata().key_value_metadata().and_then(|kv| {
            kv.iter()
                .find(|e| e.key == key.as_ref())
                .and_then(|e| e.value.as_deref())
        })
    }

    fn stamped_row_group_size_matches(metadata: &ParquetMetaData, stamped: usize) -> bool {
        let row_groups = metadata.row_groups();
        if row_groups.is_empty() {
            return false;
        }
        for (idx, row_group) in row_groups.iter().enumerate() {
            let rows = row_group.num_rows() as usize;
            if idx + 1 == row_groups.len() {
                if rows == 0 || rows > stamped {
                    return false;
                }
            } else if rows != stamped {
                return false;
            }
        }
        true
    }

    async fn puffin_blob_metadata(
        file_io: &FileIO,
        statistics_path: &str,
        column: &str,
        data_file_path: &str,
        cache_bypass: bool,
    ) -> Result<Option<crate::puffin::BlobMetadata>> {
        let input = file_io.new_input(statistics_path)?;
        let reader = PuffinReader::new(input);
        let file_metadata =
            puffin_file_metadata_cached(statistics_path, &reader, cache_bypass).await?;
        Ok(file_metadata
            .blobs()
            .iter()
            .find(|blob| {
                blob.blob_type() == "siglake-inverted-v1"
                    && blob
                        .properties()
                        .get("data_file")
                        .is_some_and(|path| path == data_file_path)
                    && blob
                        .properties()
                        .get("column")
                        .is_some_and(|candidate| candidate == column)
            })
            .cloned())
    }

    async fn puffin_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<Arc<siglake_index::InvertedIndex>>> {
        for stats_blob in &task.statistics_blobs {
            if stats_blob.blob_type != "siglake-inverted-v1" {
                continue;
            }
            if stats_blob
                .properties
                .get("column")
                .map(|candidate| candidate != column)
                .unwrap_or(true)
            {
                continue;
            }
            let Some(row_group_size) = stats_blob
                .properties
                .get("row_group_size")
                .and_then(|value| value.parse::<usize>().ok())
            else {
                continue;
            };
            if !Self::stamped_row_group_size_matches(metadata, row_group_size) {
                metrics::counter!("siglake_index_stamp_mismatch_total").increment(1);
                return Ok(None);
            }
            let Some(blob_metadata) = Self::puffin_blob_metadata(
                file_io,
                &stats_blob.statistics_path,
                column,
                task.data_file_path(),
                cache_bypass,
            )
            .await?
            else {
                continue;
            };
            let path = stats_blob.statistics_path.as_str();
            let offset = blob_metadata.offset();
            let key = ParsedIndexKey::puffin(path, offset);
            // A warm parsed index is handed out as-is: no deserialization, no
            // copy of the postings, and — deliberately — no load permit, so a
            // plan whose files are all warm does not serialize into waves of
            // four (#3896).
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Some(index));
            }
            // Bound concurrent whole-index deserialization: each decode can
            // transiently materialize hundreds of MB for a large file, and the
            // scan fans out across many files (the round-76 OOM note on
            // `file_might_match_prune_spec`). The permit covers the blob fetch
            // and the decode, nothing else.
            let waited = std::time::Instant::now();
            let _permit = Self::index_load_semaphore()
                .acquire()
                .await
                .expect("index-load semaphore is never closed");
            record_text_index_stage(
                TEXT_INDEX_STAGE_PERMIT_WAIT,
                TEXT_INDEX_STORAGE_PUFFIN,
                waited.elapsed(),
            );
            // Another task may have decoded this blob while we waited.
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Some(index));
            }
            if !cache_bypass && let Some(bytes) = puffin_blob_cache_get(path, offset) {
                return Ok(Self::decode_and_cache_index(key, bytes.as_ref(), cache_bypass));
            }
            let fetched = std::time::Instant::now();
            let input = file_io.new_input(path)?;
            let reader = PuffinReader::new(input);
            let blob = reader.blob(&blob_metadata).await?;
            record_text_index_stage(
                TEXT_INDEX_STAGE_BLOB_FETCH,
                TEXT_INDEX_STORAGE_PUFFIN,
                fetched.elapsed(),
            );
            if !cache_bypass {
                puffin_blob_cache_put(path, offset, blob.data());
            }
            return Ok(Self::decode_and_cache_index(key, blob.data(), cache_bypass));
        }
        Ok(None)
    }

    /// Deserialize an index blob and, unless the caller bypasses caches, keep
    /// the parsed form under the write-once identity it came from — for a
    /// Puffin blob, the same one the blob-bytes cache uses.
    fn decode_and_cache_index(
        key: ParsedIndexKey,
        bytes: &[u8],
        cache_bypass: bool,
    ) -> Option<Arc<siglake_index::InvertedIndex>> {
        INVERTED_INDEX_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        record_parsed_index_lookup(PARSED_INDEX_CACHE_MISS, key.storage());
        let decoded = std::time::Instant::now();
        let index = Arc::new(siglake_index::InvertedIndex::from_bytes(bytes)?);
        record_text_index_stage(TEXT_INDEX_STAGE_DECODE, key.storage(), decoded.elapsed());
        if !cache_bypass {
            parsed_index_cache_put(key, Arc::clone(&index));
        }
        Some(index)
    }

    /// Global bound on concurrent per-file inverted-index loads (see the
    /// round-76 OOM note on [`Self::file_might_match_prune_spec`]).
    /// `SIGLAKE_INDEX_LOAD_CONCURRENCY` overrides the default of 4.
    fn index_load_semaphore() -> &'static tokio::sync::Semaphore {
        static SEM: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
        SEM.get_or_init(|| {
            let permits = std::env::var("SIGLAKE_INDEX_LOAD_CONCURRENCY")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(4);
            tokio::sync::Semaphore::new(permits)
        })
    }

    async fn file_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<(Arc<siglake_index::InvertedIndex>, &'static str)>> {
        if let Some(hex) = Self::footer_inverted_index_hex(metadata, column) {
            // A data file is written once, so `(data file, column)` names this
            // blob for the file's life and the parsed form is reusable across
            // queries exactly as the Puffin one is (#3965). Warm lookups happen
            // before the permit, so a plan whose files are all warm does not
            // serialize into waves of four.
            let key = ParsedIndexKey::footer_kv(task.data_file_path(), column);
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Some((index, "footer_kv")));
            }
            // Same bound as the Puffin decode below — a footer-KV index is
            // parsed from memory, but into the same hundreds of MB.
            let waited = std::time::Instant::now();
            let _permit = Self::index_load_semaphore()
                .acquire()
                .await
                .expect("index-load semaphore is never closed");
            record_text_index_stage(
                TEXT_INDEX_STAGE_PERMIT_WAIT,
                TEXT_INDEX_STORAGE_FOOTER_KV,
                waited.elapsed(),
            );
            // Another task may have decoded this footer while we waited.
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Some((index, "footer_kv")));
            }
            // No blob-fetch stage here: the footer bytes came with the Parquet
            // metadata this scan already read, which is why only the Puffin
            // path can be slow at `blob_fetch`.
            let decoded = std::time::Instant::now();
            if let Some(index) = siglake_index::InvertedIndex::from_hex(hex) {
                record_text_index_stage(
                    TEXT_INDEX_STAGE_DECODE,
                    TEXT_INDEX_STORAGE_FOOTER_KV,
                    decoded.elapsed(),
                );
                INVERTED_INDEX_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                record_parsed_index_lookup(PARSED_INDEX_CACHE_MISS, TEXT_INDEX_STORAGE_FOOTER_KV);
                let index = Arc::new(index);
                if !cache_bypass {
                    parsed_index_cache_put(key, Arc::clone(&index));
                }
                return Ok(Some((index, "footer_kv")));
            }
        }
        Ok(Self::puffin_inverted_index(file_io, task, metadata, column, cache_bypass)
            .await?
            .map(|index| (index, "puffin")))
    }

    fn union_sorted_u32(a: &[u32], b: &[u32]) -> Vec<u32> {
        let mut out = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0usize, 0usize);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => {
                    out.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    out.push(b[j]);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    out.push(a[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        out.extend_from_slice(&a[i..]);
        out.extend_from_slice(&b[j..]);
        out
    }

    fn intersect_sorted_u32(a: &[u32], b: &[u32]) -> Vec<u32> {
        let mut out = Vec::with_capacity(a.len().min(b.len()));
        let (mut i, mut j) = (0usize, 0usize);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    out.push(a[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        out
    }

    fn bloom_might_contain_substring(
        bloom: &siglake_bloom::TokenBloom,
        substr: &str,
    ) -> Option<bool> {
        let grams = siglake_bloom::query_trigrams(substr)?;
        Some(grams.iter().all(|gram| bloom.maybe_contains(gram)))
    }

    /// File-level gate: BLOOM-ONLY by design. The per-file inverted index is
    /// deliberately NOT consulted here — deserializing a whole index (term
    /// dict + all postings) costs hundreds of MB for a large file, and the
    /// scan opens up to `aggregate_reader_budget` files concurrently, so an
    /// index-loading gate OOM'd a 12 GiB worker pod on a whole-table needle
    /// query (round-76 finding). The trigram bloom is a few KB, never
    /// false-negates, and prunes the same needle files; survivors load the
    /// index exactly once in [`Self::inverted_index_row_selection`] (bounded
    /// by [`index_load_semaphore`]), where its row pruning pays for itself.
    /// Non-`raw` columns have no bloom: the gate passes them through.
    async fn file_might_match_prune_spec(
        _file_io: &FileIO,
        _task: &FileScanTask,
        metadata: &ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> Result<bool> {
        let bloom = (spec.column == "raw")
            .then(|| Self::file_trigram_bloom(metadata))
            .flatten();
        let source = Self::prune_source_label(spec);

        let term_might_match = |term: &str| {
            bloom
                .as_ref()
                .and_then(|bloom| Self::bloom_might_contain_substring(bloom, term))
                .unwrap_or(true)
        };
        let substr_might_match = |substr: &str| {
            bloom
                .as_ref()
                .and_then(|bloom| Self::bloom_might_contain_substring(bloom, substr))
                .unwrap_or(true)
        };

        if spec.all_terms.iter().any(|term| !term_might_match(term)) {
            metrics::counter!(
                "siglake_iceberg_raw_bloom_skip_total",
                "outcome" => "skip",
                "source" => source
            )
            .increment(1);
            return Ok(false);
        }
        if !spec.any_terms.is_empty() && spec.any_terms.iter().all(|term| !term_might_match(term)) {
            metrics::counter!(
                "siglake_iceberg_raw_bloom_skip_total",
                "outcome" => "skip",
                "source" => source
            )
            .increment(1);
            return Ok(false);
        }
        if spec.substrings.iter().any(|substr| !substr_might_match(substr)) {
            metrics::counter!(
                "siglake_iceberg_raw_bloom_skip_total",
                "outcome" => "skip",
                "source" => source
            )
            .increment(1);
            return Ok(false);
        }
        metrics::counter!(
            "siglake_iceberg_raw_bloom_skip_total",
            "outcome" => "read",
            "source" => source
        )
        .increment(1);
        Ok(true)
    }

    /// The matched ordinals are file-physical row positions (the index is built
    /// in the same order the file is written, `timestamp ASC`), so the
    /// selection is built from the matches themselves — one run per matching
    /// stretch within each selected row group
    /// ([`index_matches_row_selection`](Self::index_matches_row_selection)).
    /// It agrees row for row with handing the complement to
    /// [`build_deletes_row_selection`](Self::build_deletes_row_selection), which
    /// is what this did until #3896, at a cost in the matches rather than in
    /// the file's rows. The selection is a superset of the true `LIKE` /
    /// FTS-UDF matches (exact row evaluation runs above the scan), which is
    /// safe.
    async fn inverted_index_row_selection(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        selected_row_groups: &Option<Vec<usize>>,
        spec: &RawPruneSpec,
        cache_bypass: bool,
    ) -> Result<Option<RowSelection>> {
        // The load bounds its own concurrency (`index_load_semaphore`), around
        // the blob fetch and decode only; a warm parsed index takes no permit.
        let index_and_storage =
            Self::file_inverted_index(file_io, task, metadata, &spec.column, cache_bypass).await?;
        let Some((index, storage)) = index_and_storage else {
            return Ok(None);
        };
        // Everything below is the `selection` stage: the term lookups plus the
        // run construction, timed together because the round reads them as one
        // post-load cost (#3969). It is charged whether the index was decoded
        // for this query or handed over warm, which is what makes the stage
        // histogram's `selection` count the number of index-pruned files while
        // `decode` counts only the cold ones.
        let selected = std::time::Instant::now();
        let source = Self::prune_source_label(spec);
        let mut matching: Option<Vec<u32>> = None;

        if !spec.all_terms.is_empty() {
            let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
            matching = Some(index.matching_rows_all(&terms));
        }

        if !spec.any_terms.is_empty() {
            let mut any_matching = Vec::new();
            for term in &spec.any_terms {
                if let Some(rows) = index.postings(term) {
                    any_matching = Self::union_sorted_u32(&any_matching, rows);
                }
            }
            matching = Some(match matching {
                Some(existing) => Self::intersect_sorted_u32(&existing, &any_matching),
                None => any_matching,
            });
        }

        for substr in &spec.index_substrings {
            let Some(rows) = index.rows_containing(substr) else {
                return Ok(None);
            };
            matching = Some(match matching {
                Some(existing) => Self::intersect_sorted_u32(&existing, &rows),
                None => rows,
            });
        }

        let Some(matching) = matching else {
            return Ok(None);
        };
        let total_rows: u64 = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as u64)
            .sum();
        // Observability: the index fired for this file, selecting `matching` of
        // `total_rows` rows for decode (the rest are skipped before decode).
        metrics::counter!(
            "siglake_iceberg_inverted_index_used_total",
            "source" => source,
            "storage" => storage
        )
        .increment(1);
        metrics::histogram!(
            "siglake_iceberg_inverted_index_selected_rows",
            "source" => source,
            "storage" => storage
        )
        .record(matching.len() as f64);
        metrics::histogram!(
            "siglake_iceberg_inverted_index_file_rows",
            "source" => source,
            "storage" => storage
        )
        .record(total_rows as f64);
        let selection = Self::index_matches_row_selection(
            metadata.row_groups(),
            selected_row_groups,
            &matching,
        );
        record_text_index_stage(TEXT_INDEX_STAGE_SELECTION, storage, selected.elapsed());
        Ok(Some(selection))
    }

    /// Select exactly `matching` (ascending, file-physical row ordinals) across
    /// the selected row groups, in the same shape
    /// [`build_deletes_row_selection`](Self::build_deletes_row_selection)
    /// produces for the complement as a delete vector: selectors cover the
    /// selected row groups only, concatenated in file order, with no
    /// zero-length runs and no run spanning a row-group boundary.
    ///
    /// The cost is in the matches and the row groups, not in the file's rows:
    /// [`siglake_index::row_selection_runs`] walks one row group's matches
    /// once, where the complement form enumerated every rejected row (#3896).
    fn index_matches_row_selection(
        row_group_metadata_list: &[RowGroupMetaData],
        selected_row_groups: &Option<Vec<usize>>,
        matching: &[u32],
    ) -> RowSelection {
        let mut selectors: Vec<RowSelector> = Vec::new();
        let mut base: u64 = 0;
        let mut cursor = 0usize; // next unconsumed entry of `matching`
        let mut selected_idx = 0usize;

        for (idx, row_group_metadata) in row_group_metadata_list.iter().enumerate() {
            let num_rows = row_group_metadata.num_rows() as u64;
            let end = base + num_rows;
            let mut skip_group = false;
            if let Some(selected) = selected_row_groups {
                if selected_idx == selected.len() {
                    break;
                }
                if idx == selected[selected_idx] {
                    selected_idx += 1;
                } else {
                    skip_group = true;
                }
            }
            // Consume this row group's matches either way: they are ignored
            // for a skipped group and rebased for a selected one.
            while cursor < matching.len() && (matching[cursor] as u64) < base {
                cursor += 1; // defensive: ordinals before the group (can't happen)
            }
            let start = cursor;
            while cursor < matching.len() && (matching[cursor] as u64) < end {
                cursor += 1;
            }
            if !skip_group {
                let rebased: Vec<u32> = matching[start..cursor]
                    .iter()
                    .map(|ordinal| (*ordinal as u64 - base) as u32)
                    .collect();
                for (selected, len) in siglake_index::row_selection_runs(&rebased, num_rows as u32)
                {
                    selectors.push(if selected {
                        RowSelector::select(len as usize)
                    } else {
                        RowSelector::skip(len as usize)
                    });
                }
            }
            base = end;
        }
        selectors.into()
    }

    fn build_field_id_set_and_map(
        parquet_schema: &SchemaDescriptor,
        predicate: &BoundPredicate,
    ) -> Result<(HashSet<i32>, HashMap<i32, usize>)> {
        let iceberg_field_ids = Self::collect_field_ids(predicate)?;

        // Without embedded field IDs, we fall back to position-based mapping for compatibility
        let field_id_map = match build_field_id_map(parquet_schema)? {
            Some(map) => map,
            None => build_fallback_field_id_map(parquet_schema),
        };

        Ok((iceberg_field_ids, field_id_map))
    }

    fn collect_field_ids(predicate: &BoundPredicate) -> Result<HashSet<i32>> {
        let mut collector = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut collector, predicate)?;
        Ok(collector.field_ids())
    }

    /// Recursively extract leaf field IDs because Parquet projection works at the leaf column level.
    /// Nested types (struct/list/map) are flattened in Parquet's columnar format.
    fn include_leaf_field_id(field: &NestedField, field_ids: &mut Vec<i32>) {
        match field.field_type.as_ref() {
            Type::Primitive(_) => {
                field_ids.push(field.id);
            }
            Type::Struct(struct_type) => {
                for nested_field in struct_type.fields() {
                    Self::include_leaf_field_id(nested_field, field_ids);
                }
            }
            Type::List(list_type) => {
                Self::include_leaf_field_id(&list_type.element_field, field_ids);
            }
            Type::Map(map_type) => {
                Self::include_leaf_field_id(&map_type.key_field, field_ids);
                Self::include_leaf_field_id(&map_type.value_field, field_ids);
            }
        }
    }

    fn get_arrow_projection_mask(
        field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        use_fallback: bool, // Whether file lacks embedded field IDs (e.g., migrated from Hive/Spark)
    ) -> Result<ProjectionMask> {
        fn type_promotion_is_valid(
            file_type: Option<&PrimitiveType>,
            projected_type: Option<&PrimitiveType>,
        ) -> bool {
            match (file_type, projected_type) {
                (Some(lhs), Some(rhs)) if lhs == rhs => true,
                (Some(PrimitiveType::Int), Some(PrimitiveType::Long)) => true,
                (Some(PrimitiveType::Float), Some(PrimitiveType::Double)) => true,
                (
                    Some(PrimitiveType::Decimal {
                        precision: file_precision,
                        scale: file_scale,
                    }),
                    Some(PrimitiveType::Decimal {
                        precision: requested_precision,
                        scale: requested_scale,
                    }),
                ) if requested_precision >= file_precision && file_scale == requested_scale => true,
                // Uuid will be store as Fixed(16) in parquet file, so the read back type will be Fixed(16).
                (Some(PrimitiveType::Fixed(16)), Some(PrimitiveType::Uuid)) => true,
                _ => false,
            }
        }

        if field_ids.is_empty() {
            return Ok(ProjectionMask::all());
        }

        if use_fallback {
            // Position-based projection necessary because file lacks embedded field IDs
            Self::get_arrow_projection_mask_fallback(field_ids, parquet_schema)
        } else {
            // Field-ID-based projection using embedded field IDs from Parquet metadata

            // Parquet's columnar format requires leaf-level (not top-level struct/list/map) projection
            let mut leaf_field_ids = vec![];
            for field_id in field_ids {
                let field = iceberg_schema_of_task.field_by_id(*field_id);
                if let Some(field) = field {
                    Self::include_leaf_field_id(field, &mut leaf_field_ids);
                }
            }

            Self::get_arrow_projection_mask_with_field_ids(
                &leaf_field_ids,
                iceberg_schema_of_task,
                parquet_schema,
                arrow_schema,
                type_promotion_is_valid,
            )
        }
    }

    /// Standard projection using embedded field IDs from Parquet metadata.
    /// For iceberg-java compatibility with ParquetSchemaUtil.pruneColumns().
    fn get_arrow_projection_mask_with_field_ids(
        leaf_field_ids: &[i32],
        iceberg_schema_of_task: &Schema,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &ArrowSchemaRef,
        type_promotion_is_valid: fn(Option<&PrimitiveType>, Option<&PrimitiveType>) -> bool,
    ) -> Result<ProjectionMask> {
        let mut column_map = HashMap::new();
        let fields = arrow_schema.fields();

        // Pre-project only the fields that have been selected, possibly avoiding converting
        // some Arrow types that are not yet supported.
        let mut projected_fields: HashMap<FieldRef, i32> = HashMap::new();
        let projected_arrow_schema = ArrowSchema::new_with_metadata(
            fields.filter_leaves(|_, f| {
                f.metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .and_then(|field_id| i32::from_str(field_id).ok())
                    .is_some_and(|field_id| {
                        projected_fields.insert((*f).clone(), field_id);
                        leaf_field_ids.contains(&field_id)
                    })
            }),
            arrow_schema.metadata().clone(),
        );
        let iceberg_schema = arrow_schema_to_schema(&projected_arrow_schema)?;

        fields.filter_leaves(|idx, field| {
            let Some(field_id) = projected_fields.get(field).cloned() else {
                return false;
            };

            let iceberg_field = iceberg_schema_of_task.field_by_id(field_id);
            let parquet_iceberg_field = iceberg_schema.field_by_id(field_id);

            if iceberg_field.is_none() || parquet_iceberg_field.is_none() {
                return false;
            }

            if !type_promotion_is_valid(
                parquet_iceberg_field
                    .unwrap()
                    .field_type
                    .as_primitive_type(),
                iceberg_field.unwrap().field_type.as_primitive_type(),
            ) {
                return false;
            }

            column_map.insert(field_id, idx);
            true
        });

        // Schema evolution: New columns may not exist in old Parquet files.
        // We only project existing columns; RecordBatchTransformer adds default/NULL values.
        let mut indices = vec![];
        for field_id in leaf_field_ids {
            if let Some(col_idx) = column_map.get(field_id) {
                indices.push(*col_idx);
            }
        }

        if indices.is_empty() {
            // Edge case: All requested columns are new (don't exist in file).
            // Project all columns so RecordBatchTransformer has a batch to transform.
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::leaves(parquet_schema, indices))
        }
    }

    /// Fallback projection for Parquet files without field IDs.
    /// Uses position-based matching: field ID N → column position N-1.
    /// Projects entire top-level columns (including nested content) for iceberg-java compatibility.
    fn get_arrow_projection_mask_fallback(
        field_ids: &[i32],
        parquet_schema: &SchemaDescriptor,
    ) -> Result<ProjectionMask> {
        // Position-based: field_id N → column N-1 (field IDs are 1-indexed)
        let parquet_root_fields = parquet_schema.root_schema().get_fields();
        let mut root_indices = vec![];

        for field_id in field_ids.iter() {
            let parquet_pos = (*field_id - 1) as usize;

            if parquet_pos < parquet_root_fields.len() {
                root_indices.push(parquet_pos);
            }
            // RecordBatchTransformer adds missing columns with NULL values
        }

        if root_indices.is_empty() {
            Ok(ProjectionMask::all())
        } else {
            Ok(ProjectionMask::roots(parquet_schema, root_indices))
        }
    }

    fn get_row_filter(
        predicates: &BoundPredicate,
        parquet_schema: &SchemaDescriptor,
        iceberg_field_ids: &HashSet<i32>,
        field_id_map: &HashMap<i32, usize>,
    ) -> Result<RowFilter> {
        // Collect Parquet column indices from field ids.
        // If the field id is not found in Parquet schema, it will be ignored due to schema evolution.
        let mut column_indices = iceberg_field_ids
            .iter()
            .filter_map(|field_id| field_id_map.get(field_id).cloned())
            .collect::<Vec<_>>();
        column_indices.sort();

        // The converter that converts `BoundPredicates` to `ArrowPredicates`
        let mut converter = PredicateConverter {
            parquet_schema,
            column_map: field_id_map,
            column_indices: &column_indices,
        };

        // After collecting required leaf column indices used in the predicate,
        // creates the projection mask for the Arrow predicates.
        let projection_mask = ProjectionMask::leaves(parquet_schema, column_indices.clone());
        let predicate_func = visit(&mut converter, predicates)?;
        let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate_func);
        Ok(RowFilter::new(vec![Box::new(arrow_predicate)]))
    }

    /// WS-7: can this row group contain a row where the promoted column holds
    /// ANY of `spec.values`? `true` (keep) whenever we can't prove otherwise:
    /// the column absent from the file (pre-promotion — its `attributes` JSON
    /// may still carry the key), no statistics, or a value inside [min, max].
    /// Parquet min/max truncation is directionally safe (min truncates down,
    /// max up), so the stored range contains the actual range.
    fn row_group_might_match_promoted(
        rg: &parquet::file::metadata::RowGroupMetaData,
        spec: &PromotedPruneSpec,
    ) -> bool {
        let Some(col) = rg
            .columns()
            .iter()
            .find(|c| c.column_descr().path().string() == spec.column)
        else {
            return true;
        };
        let Some(stats) = col.statistics() else {
            return true;
        };
        // All-null group: the key was absent from every row's JSON when this
        // (post-promotion) file was written, so no equality can match.
        if stats.null_count_opt() == Some(rg.num_rows() as u64) {
            return false;
        }
        let parquet::file::statistics::Statistics::ByteArray(bytes) = stats else {
            return true;
        };
        let (Some(min), Some(max)) = (bytes.min_bytes_opt(), bytes.max_bytes_opt()) else {
            return true;
        };
        spec.values
            .iter()
            .any(|v| v.as_bytes() >= min && v.as_bytes() <= max)
    }

    /// Returns the row-group ordinals that survive the per-row-group trigram
    /// blooms for `spec`, or `None` when this file has no usable row-group
    /// bloom metadata — in which case no row-group pruning is applied.
    fn rowgroup_bloom_survivors_for_spec(
        metadata: &parquet::file::metadata::ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> Option<Vec<usize>> {
        if spec.column != "raw" {
            return None;
        }
        let kv = metadata.file_metadata().key_value_metadata()?;
        let hex = kv
            .iter()
            .find(|e| e.key == siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY)
            .and_then(|e| e.value.as_deref())?;
        let blooms = siglake_bloom::rowgroup_blooms_from_hex(hex)?;
        // Defensive: if the list doesn't match the file's row groups, don't prune.
        if blooms.len() != metadata.row_groups().len() {
            return None;
        }
        let source = Self::prune_source_label(spec);
        let mut survivors = Vec::with_capacity(blooms.len());
        for (idx, bloom) in blooms.iter().enumerate() {
            let required_ok = spec
                .all_terms
                .iter()
                .chain(spec.substrings.iter())
                .all(|substr| {
                    Self::bloom_might_contain_substring(bloom, substr).unwrap_or(true)
                });
            let any_ok = spec.any_terms.is_empty()
                || spec
                    .any_terms
                    .iter()
                    .any(|term| Self::bloom_might_contain_substring(bloom, term).unwrap_or(true));
            if required_ok && any_ok {
                survivors.push(idx);
                metrics::counter!(
                    "siglake_iceberg_raw_rowgroup_bloom_skip_total",
                    "outcome" => "read",
                    "source" => source
                )
                .increment(1);
            } else {
                metrics::counter!(
                    "siglake_iceberg_raw_rowgroup_bloom_skip_total",
                    "outcome" => "skip",
                    "source" => source
                )
                .increment(1);
            }
        }
        Some(survivors)
    }

    fn get_selected_row_group_indices(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<Vec<usize>> {
        let row_groups_metadata = parquet_metadata.row_groups();
        let mut results = Vec::with_capacity(row_groups_metadata.len());

        for (idx, row_group_metadata) in row_groups_metadata.iter().enumerate() {
            if RowGroupMetricsEvaluator::eval(
                predicate,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )? {
                results.push(idx);
            }
        }

        Ok(results)
    }

    fn get_row_selection_for_filter_predicate(
        predicate: &BoundPredicate,
        parquet_metadata: &Arc<ParquetMetaData>,
        selected_row_groups: &Option<Vec<usize>>,
        field_id_map: &HashMap<i32, usize>,
        snapshot_schema: &Schema,
    ) -> Result<RowSelection> {
        let Some(column_index) = parquet_metadata.column_index() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Parquet file metadata does not contain a column index",
            ));
        };

        let Some(offset_index) = parquet_metadata.offset_index() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Parquet file metadata does not contain an offset index",
            ));
        };

        // If all row groups were filtered out, return an empty RowSelection (select no rows)
        if let Some(selected_row_groups) = selected_row_groups
            && selected_row_groups.is_empty()
        {
            return Ok(RowSelection::from(Vec::new()));
        }

        let mut selected_row_groups_idx = 0;

        let page_index = column_index
            .iter()
            .enumerate()
            .zip(offset_index)
            .zip(parquet_metadata.row_groups());

        let mut results = Vec::new();
        for (((idx, column_index), offset_index), row_group_metadata) in page_index {
            if let Some(selected_row_groups) = selected_row_groups {
                // skip row groups that aren't present in selected_row_groups
                if idx == selected_row_groups[selected_row_groups_idx] {
                    selected_row_groups_idx += 1;
                } else {
                    continue;
                }
            }

            let selections_for_page = PageIndexEvaluator::eval(
                predicate,
                column_index,
                offset_index,
                row_group_metadata,
                field_id_map,
                snapshot_schema,
            )?;

            results.push(selections_for_page);

            if let Some(selected_row_groups) = selected_row_groups
                && selected_row_groups_idx == selected_row_groups.len()
            {
                break;
            }
        }

        Ok(results.into_iter().flatten().collect::<Vec<_>>().into())
    }

    /// Filters row groups by byte range to support Iceberg's file splitting.
    ///
    /// Iceberg splits large files at row group boundaries, so we only read row groups
    /// whose byte ranges overlap with [start, start+length).
    fn filter_row_groups_by_byte_range(
        parquet_metadata: &Arc<ParquetMetaData>,
        start: u64,
        length: u64,
    ) -> Result<Vec<usize>> {
        let row_groups = parquet_metadata.row_groups();
        let mut selected = Vec::new();
        let end = start + length;

        // Row groups are stored sequentially after the 4-byte magic header.
        let mut current_byte_offset = 4u64;

        for (idx, row_group) in row_groups.iter().enumerate() {
            let row_group_size = row_group.compressed_size() as u64;
            let row_group_end = current_byte_offset + row_group_size;

            if current_byte_offset < end && start < row_group_end {
                selected.push(idx);
            }

            current_byte_offset = row_group_end;
        }

        Ok(selected)
    }
}

/// Build the map of parquet field id to Parquet column index in the schema.
/// Returns None if the Parquet file doesn't have field IDs embedded (e.g., migrated tables).
fn build_field_id_map(parquet_schema: &SchemaDescriptor) -> Result<Option<HashMap<i32, usize>>> {
    let mut column_map = HashMap::new();

    for (idx, field) in parquet_schema.columns().iter().enumerate() {
        let field_type = field.self_type();
        match field_type {
            ParquetType::PrimitiveType { basic_info, .. } => {
                if !basic_info.has_id() {
                    return Ok(None);
                }
                column_map.insert(basic_info.id(), idx);
            }
            ParquetType::GroupType { .. } => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leaf column in schema should be primitive type but got {field_type:?}"
                    ),
                ));
            }
        };
    }

    Ok(Some(column_map))
}

/// Build a fallback field ID map for Parquet files without embedded field IDs.
///
/// Returns the number of primitive (leaf) columns in a Parquet type, recursing into groups.
fn leaf_count(ty: &parquet::schema::types::Type) -> usize {
    if ty.is_primitive() {
        1
    } else {
        ty.get_fields().iter().map(|f| leaf_count(f)).sum()
    }
}

/// Builds a mapping from fallback field IDs to leaf column indices for Parquet files
/// without embedded field IDs. Returns entries only for primitive top-level fields.
///
/// Must use top-level field positions (not leaf column positions) to stay consistent
/// with `add_fallback_field_ids_to_arrow_schema`, which assigns ordinal IDs to
/// top-level Arrow fields. Using leaf positions instead would produce wrong indices
/// when nested types (struct/list/map) expand into multiple leaf columns.
///
/// Mirrors iceberg-java's ParquetSchemaUtil.addFallbackIds() which iterates
/// fileSchema.getFields() assigning ordinal IDs to top-level fields.
fn build_fallback_field_id_map(parquet_schema: &SchemaDescriptor) -> HashMap<i32, usize> {
    let mut column_map = HashMap::new();
    let mut leaf_idx = 0;

    for (top_pos, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
        let field_id = (top_pos + 1) as i32;
        if field.is_primitive() {
            column_map.insert(field_id, leaf_idx);
        }
        leaf_idx += leaf_count(field);
    }

    column_map
}

/// Apply name mapping to Arrow schema for Parquet files lacking field IDs.
///
/// Assigns Iceberg field IDs based on column names using the name mapping,
/// enabling correct projection on migrated files (e.g., from Hive/Spark via add_files).
///
/// Per Iceberg spec Column Projection rule #2:
/// "Use schema.name-mapping.default metadata to map field id to columns without field id"
/// https://iceberg.apache.org/spec/#column-projection
///
/// Corresponds to Java's ParquetSchemaUtil.applyNameMapping() and ApplyNameMapping visitor.
/// The key difference is Java operates on Parquet MessageType, while we operate on Arrow Schema.
///
/// # Arguments
/// * `arrow_schema` - Arrow schema from Parquet file (without field IDs)
/// * `name_mapping` - Name mapping from table metadata (TableProperties.DEFAULT_NAME_MAPPING)
///
/// # Returns
/// Arrow schema with field IDs assigned based on name mapping
fn apply_name_mapping_to_arrow_schema(
    arrow_schema: ArrowSchemaRef,
    name_mapping: &NameMapping,
) -> Result<Arc<ArrowSchema>> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs - name mapping should not be applied"
    );

    use arrow_schema::Field;

    let fields_with_mapped_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            // Look up this column name in name mapping to get the Iceberg field ID.
            // Corresponds to Java's ApplyNameMapping visitor which calls
            // nameMapping.find(currentPath()) and returns field.withId() if found.
            //
            // If the field isn't in the mapping, leave it WITHOUT assigning an ID
            // (matching Java's behavior of returning the field unchanged).
            // Later, during projection, fields without IDs are filtered out.
            let mapped_field_opt = name_mapping
                .fields()
                .iter()
                .find(|f| f.names().contains(&field.name().to_string()));

            let mut metadata = field.metadata().clone();

            if let Some(mapped_field) = mapped_field_opt
                && let Some(field_id) = mapped_field.field_id()
            {
                // Field found in mapping with a field_id → assign it
                metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
            }
            // If field_id is None, leave the field without an ID (will be filtered by projection)

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields_with_mapped_ids,
        arrow_schema.metadata().clone(),
    )))
}

/// Add position-based fallback field IDs to Arrow schema for Parquet files lacking them.
/// Enables projection on migrated files (e.g., from Hive/Spark).
///
/// Why at schema level (not per-batch): Efficiency - avoids repeated schema modification.
/// Why only top-level: Nested projection uses leaf column indices, not parent struct IDs.
/// Why 1-indexed: Compatibility with iceberg-java's ParquetSchemaUtil.addFallbackIds().
fn add_fallback_field_ids_to_arrow_schema(arrow_schema: &ArrowSchemaRef) -> Arc<ArrowSchema> {
    debug_assert!(
        arrow_schema
            .fields()
            .iter()
            .next()
            .is_none_or(|f| f.metadata().get(PARQUET_FIELD_ID_META_KEY).is_none()),
        "Schema already has field IDs"
    );

    use arrow_schema::Field;

    let fields_with_fallback_ids: Vec<_> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(pos, field)| {
            let mut metadata = field.metadata().clone();
            let field_id = (pos + 1) as i32; // 1-indexed for Java compatibility
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

            Field::new(field.name(), field.data_type().clone(), field.is_nullable())
                .with_metadata(metadata)
        })
        .collect();

    Arc::new(ArrowSchema::new_with_metadata(
        fields_with_fallback_ids,
        arrow_schema.metadata().clone(),
    ))
}

/// A visitor to collect field ids from bound predicates.
struct CollectFieldIdVisitor {
    field_ids: HashSet<i32>,
}

impl CollectFieldIdVisitor {
    fn field_ids(self) -> HashSet<i32> {
        self.field_ids
    }
}

impl BoundPredicateVisitor for CollectFieldIdVisitor {
    type T = ();

    fn always_true(&mut self) -> Result<()> {
        Ok(())
    }

    fn always_false(&mut self) -> Result<()> {
        Ok(())
    }

    fn and(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn or(&mut self, _lhs: (), _rhs: ()) -> Result<()> {
        Ok(())
    }

    fn not(&mut self, _inner: ()) -> Result<()> {
        Ok(())
    }

    fn is_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_null(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn is_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_nan(&mut self, reference: &BoundReference, _predicate: &BoundPredicate) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<()> {
        self.field_ids.insert(reference.field().id);
        Ok(())
    }
}

/// A visitor to convert Iceberg bound predicates to Arrow predicates.
struct PredicateConverter<'a> {
    /// The Parquet schema descriptor.
    pub parquet_schema: &'a SchemaDescriptor,
    /// The map between field id and leaf column index in Parquet schema.
    pub column_map: &'a HashMap<i32, usize>,
    /// The required column indices in Parquet schema for the predicates.
    pub column_indices: &'a Vec<usize>,
}

impl PredicateConverter<'_> {
    /// When visiting a bound reference, we return index of the leaf column in the
    /// required column indices which is used to project the column in the record batch.
    /// Return None if the field id is not found in the column map, which is possible
    /// due to schema evolution.
    fn bound_reference(&mut self, reference: &BoundReference) -> Result<Option<usize>> {
        // The leaf column's index in Parquet schema.
        if let Some(column_idx) = self.column_map.get(&reference.field().id) {
            if self.parquet_schema.get_column_root(*column_idx).is_group() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Leaf column `{}` in predicates isn't a root column in Parquet schema.",
                        reference.field().name
                    ),
                ));
            }

            // The leaf column's index in the required column indices.
            let index = self
                .column_indices
                .iter()
                .position(|&idx| idx == *column_idx)
                .ok_or(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                "Leaf column `{}` in predicates cannot be found in the required column indices.",
                reference.field().name
            ),
                ))?;

            Ok(Some(index))
        } else {
            Ok(None)
        }
    }

    /// Build an Arrow predicate that always returns true.
    fn build_always_true(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        }))
    }

    /// Build an Arrow predicate that always returns false.
    fn build_always_false(&self) -> Result<Box<PredicateResult>> {
        Ok(Box::new(|batch| {
            Ok(BooleanArray::from(vec![false; batch.num_rows()]))
        }))
    }
}

/// Gets the leaf column from the record batch for the required column index. Only
/// supports top-level columns for now.
fn project_column(
    batch: &RecordBatch,
    column_idx: usize,
) -> std::result::Result<ArrayRef, ArrowError> {
    let column = batch.column(column_idx);

    match column.data_type() {
        DataType::Struct(_) => Err(ArrowError::SchemaError(
            "Does not support struct column yet.".to_string(),
        )),
        _ => Ok(column.clone()),
    }
}

type PredicateResult =
    dyn FnMut(RecordBatch) -> std::result::Result<BooleanArray, ArrowError> + Send + 'static;

impl BoundPredicateVisitor for PredicateConverter<'_> {
    type T = Box<PredicateResult>;

    fn always_true(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_true()
    }

    fn always_false(&mut self) -> Result<Box<PredicateResult>> {
        self.build_always_false()
    }

    fn and(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            and_kleene(&left, &right)
        }))
    }

    fn or(
        &mut self,
        mut lhs: Box<PredicateResult>,
        mut rhs: Box<PredicateResult>,
    ) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let left = lhs(batch.clone())?;
            let right = rhs(batch)?;
            or_kleene(&left, &right)
        }))
    }

    fn not(&mut self, mut inner: Box<PredicateResult>) -> Result<Box<PredicateResult>> {
        Ok(Box::new(move |batch| {
            let pred_ret = inner(batch)?;
            not(&pred_ret)
        }))
    }

    fn is_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn not_null(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            Ok(Box::new(move |batch| {
                let column = project_column(&batch, idx)?;
                is_not_null(&column)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn is_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_true()
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_nan(
        &mut self,
        reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if self.bound_reference(reference)?.is_some() {
            self.build_always_false()
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn less_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                lt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn greater_than(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn greater_than_or_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                gt_eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                eq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                neq(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                starts_with(&left, literal.as_ref())
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_starts_with(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literal = get_arrow_datum(literal)?;

            Ok(Box::new(move |batch| {
                let left = project_column(&batch, idx)?;
                let literal = try_cast_literal(&literal, left.data_type())?;
                // update here if arrow ever adds a native not_starts_with
                not(&starts_with(&left, literal.as_ref())?)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native is_in kernel
                let left = project_column(&batch, idx)?;

                let mut acc = BooleanArray::from(vec![false; batch.num_rows()]);
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = or(&acc, &eq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_false()
        }
    }

    fn not_in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Box<PredicateResult>> {
        if let Some(idx) = self.bound_reference(reference)? {
            let literals: Vec<_> = literals
                .iter()
                .map(|lit| get_arrow_datum(lit).unwrap())
                .collect();

            Ok(Box::new(move |batch| {
                // update this if arrow ever adds a native not_in kernel
                let left = project_column(&batch, idx)?;
                let mut acc = BooleanArray::from(vec![true; batch.num_rows()]);
                for literal in &literals {
                    let literal = try_cast_literal(literal, left.data_type())?;
                    acc = and(&acc, &neq(&left, literal.as_ref())?)?
                }

                Ok(acc)
            }))
        } else {
            // A missing column, treating it as null.
            self.build_always_true()
        }
    }
}

/// SPIKE (read-path ownership): actual bytes fetched from object storage by the
/// Parquet reader (footer + data). This is impossible to observe through stock
/// iceberg-rust 0.9 (the reader is a closed box); it is trivial now that we own
/// the read path in-tree. Global counter for the spike — the production form
/// would thread a per-scan handle so a single query's read amplification can be
/// compared directly against its planned bytes.
static OBJECT_STORE_BYTES_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Total object-store bytes read by Parquet scans since process start (or the
/// last [`reset_object_store_bytes_read`]).
pub fn object_store_bytes_read() -> u64 {
    OBJECT_STORE_BYTES_READ.load(std::sync::atomic::Ordering::Relaxed)
}

/// Reset the object-store read-bytes counter, returning its prior value.
pub fn reset_object_store_bytes_read() -> u64 {
    OBJECT_STORE_BYTES_READ.swap(0, std::sync::atomic::Ordering::Relaxed)
}

#[inline]
fn record_object_store_bytes(n: u64) {
    OBJECT_STORE_BYTES_READ.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    // Also surface it as a Prometheus counter so a deployed query pod can
    // report ACTUAL fetched bytes against planned/decoded.
    metrics::counter!("siglake_iceberg_object_store_bytes_read_total").increment(n);
}

// --- Q5: cross-query Parquet footer / page-index cache ---------------------
// Round 53 measured that a narrow-window scan still fetches ~142 MB of footer +
// page-index across ~100 files even when it decodes ~20 MB — because the reader
// re-reads every file's metadata on every query. Iceberg data files are
// immutable, so caching parsed `ParquetMetaData` by file path lets warm /
// overlapping queries skip that metadata IO entirely. Bounded FIFO; default-on
// at a conservative entry count, tunable via
// `SIGLAKE_ICEBERG_FOOTER_CACHE_MAX_ENTRIES` (0 = disabled, raise it for
// dashboard workloads that re-scan a large hot file set). The cache is keyed by
// the immutable data-file path, so a hit is always correct; the only cost of a
// larger bound is memory (one parsed footer + page index per entry — small for
// siglake's few-column event files).
struct FooterCacheInner {
    order: std::collections::VecDeque<String>,
    map: std::collections::HashMap<String, Arc<ParquetMetaData>>,
}

static FOOTER_CACHE: std::sync::OnceLock<std::sync::Mutex<FooterCacheInner>> =
    std::sync::OnceLock::new();

/// Default-on bound. Conservative: ~hundreds of small few-column footers is a
/// modest memory footprint, and dashboard/overlapping-window queries — the
/// common siglake read shape — benefit immediately. Set the env var to override
/// (0 disables; raise it for very large hot file sets).
const DEFAULT_FOOTER_CACHE_MAX_ENTRIES: usize = 512;

fn footer_cache_max_entries() -> usize {
    std::env::var("SIGLAKE_ICEBERG_FOOTER_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_FOOTER_CACHE_MAX_ENTRIES)
}

fn footer_cache() -> &'static std::sync::Mutex<FooterCacheInner> {
    FOOTER_CACHE.get_or_init(|| {
        std::sync::Mutex::new(FooterCacheInner {
            order: std::collections::VecDeque::new(),
            map: std::collections::HashMap::new(),
        })
    })
}

fn footer_cache_get(path: &str) -> Option<Arc<ParquetMetaData>> {
    if footer_cache_max_entries() == 0 {
        return None;
    }
    let hit = footer_cache().lock().unwrap().map.get(path).cloned();
    metrics::counter!(
        "siglake_iceberg_footer_cache_total",
        "outcome" => if hit.is_some() { "hit" } else { "miss" }
    )
    .increment(1);
    hit
}

fn footer_cache_put(path: &str, meta: Arc<ParquetMetaData>) {
    let max = footer_cache_max_entries();
    if max == 0 {
        return;
    }
    let mut cache = footer_cache().lock().unwrap();
    if cache.map.contains_key(path) {
        return;
    }
    cache.map.insert(path.to_string(), meta);
    cache.order.push_back(path.to_string());
    while cache.order.len() > max {
        if let Some(evicted) = cache.order.pop_front() {
            cache.map.remove(&evicted);
        }
    }
}

struct PuffinFooterCacheInner {
    order: std::collections::VecDeque<String>,
    map: std::collections::HashMap<String, Arc<crate::puffin::FileMetadata>>,
}

static PUFFIN_FOOTER_CACHE: std::sync::OnceLock<std::sync::Mutex<PuffinFooterCacheInner>> =
    std::sync::OnceLock::new();
const DEFAULT_PUFFIN_FOOTER_CACHE_MAX_ENTRIES: usize = 128;

fn puffin_footer_cache_max_entries() -> usize {
    std::env::var("SIGLAKE_PUFFIN_FOOTER_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_PUFFIN_FOOTER_CACHE_MAX_ENTRIES)
}

fn puffin_footer_cache() -> &'static std::sync::Mutex<PuffinFooterCacheInner> {
    PUFFIN_FOOTER_CACHE.get_or_init(|| {
        std::sync::Mutex::new(PuffinFooterCacheInner {
            order: std::collections::VecDeque::new(),
            map: std::collections::HashMap::new(),
        })
    })
}

async fn puffin_file_metadata_cached(
    path: &str,
    reader: &PuffinReader,
    cache_bypass: bool,
) -> Result<Arc<crate::puffin::FileMetadata>> {
    let max = puffin_footer_cache_max_entries();
    if !cache_bypass
        && max > 0
        && let Some(hit) = puffin_footer_cache().lock().unwrap().map.get(path).cloned()
    {
        return Ok(hit);
    }
    let metadata = Arc::new(reader.file_metadata().await?.clone());
    if cache_bypass || max == 0 {
        return Ok(metadata);
    }
    let mut cache = puffin_footer_cache().lock().unwrap();
    if !cache.map.contains_key(path) {
        cache.map.insert(path.to_string(), metadata.clone());
        cache.order.push_back(path.to_string());
        while cache.order.len() > max {
            if let Some(evicted) = cache.order.pop_front() {
                cache.map.remove(&evicted);
            }
        }
    }
    Ok(metadata)
}

/// Serialized index blobs, keyed by `(statistics_path, blob offset)` — the same
/// immutable identity the parsed-index cache uses.
///
/// Since #3896 a warm text query is served by the parsed cache and never looks
/// here, so what this holds is a re-parse source: when a parsed index is
/// evicted, the blob is already local and the next query pays the
/// deserialization (242 ms for a 7.3M-row file) without the object-store fetch
/// that precedes it (81 MB for the same file). That is only worth the memory if
/// the two caches cover the same files, which is what the byte bound below is
/// sized for.
#[derive(Default)]
struct PuffinBlobCacheInner {
    order: std::collections::VecDeque<(String, u64)>,
    map: std::collections::HashMap<(String, u64), Arc<[u8]>>,
    bytes: usize,
}

impl PuffinBlobCacheInner {
    fn get(&self, key: &(String, u64)) -> Option<Arc<[u8]>> {
        self.map.get(key).cloned()
    }

    /// Insert under both bounds. Eviction is first-in-first-out: a blob's value
    /// does not grow with use the way a parsed index's does, and re-reading one
    /// costs a fetch, not a decode.
    fn put(&mut self, key: (String, u64), blob: Arc<[u8]>, max_bytes: usize, max_entries: usize) {
        let size = blob.len();
        // A blob larger than the whole budget would evict everything else and
        // then be evicted itself: leave it to be fetched per decode.
        if size > max_bytes || self.map.contains_key(&key) {
            return;
        }
        self.map.insert(key.clone(), blob);
        self.order.push_back(key);
        self.bytes += size;
        while self.order.len() > max_entries || self.bytes > max_bytes {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some(blob) = self.map.remove(&evicted) {
                self.bytes -= blob.len();
            }
        }
    }
}

static PUFFIN_BLOB_CACHE: std::sync::OnceLock<std::sync::Mutex<PuffinBlobCacheInner>> =
    std::sync::OnceLock::new();
const DEFAULT_PUFFIN_BLOB_CACHE_MAX_ENTRIES: usize = 128;

/// Blob bytes are about a quarter of the parsed form they decode to (#3896
/// measured 81 MB decoding to 294 MB for a 7.34M-row file), so this covers
/// about the same three or four large compacted files as the 1 GiB parsed
/// budget — which is the condition for the re-parse source above to be worth
/// anything, since a blob whose parsed twin was evicted is only useful while
/// the blob itself survives. For the ordinary case of small per-file indexes
/// the entry bound binds first and this is a ceiling, not the governor: 128
/// entries at the measured size would otherwise retain ~10.4 GB, more than
/// twice the whole 4Gi query limit the chart packages.
const DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Byte budgets the host process resolved for the two text-index caches, or
/// [`TEXT_INDEX_CACHE_UNCONFIGURED`] while nothing has set them.
///
/// WHY A SETTER. Every sibling read cache sizes itself from the pod's memory
/// limit; these two were flat constants, so on the packaged 4Gi query pod their
/// defaults claimed the whole 1.25Gi the budget leaves for the process, and the
/// query memory pool — which subtracts the caches it knows about — had never
/// heard of them. The cgroup read lives in `siglake_storage`, which depends on
/// this crate, so the limit cannot be read from here. It arrives instead the way
/// the byte-range object cache's budget does: the host process resolves it once
/// at startup (`siglake_storage::resolve_text_index_cache_config`, or
/// `resolve_role_cache_config` for the `siglake` binary's roles, where a
/// maintenance process budgets zero because it reads no text index) and calls
/// [`set_text_index_cache_max_bytes`] before the warehouse opens.
static CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(TEXT_INDEX_CACHE_UNCONFIGURED);
static CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(TEXT_INDEX_CACHE_UNCONFIGURED);

/// "No host process has resolved this budget." A real budget of `u64::MAX` is
/// not representable, which costs nothing: it exceeds any machine's RAM.
const TEXT_INDEX_CACHE_UNCONFIGURED: u64 = u64::MAX;

/// Set the byte budgets of the parsed-index and Puffin blob caches (bytes; `0`
/// disables that cache alone). Process-global; takes effect for subsequent
/// queries, and wins over `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES` and
/// `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES` — the caller resolves those itself, so
/// an override reaches the caches through this call rather than around it.
pub fn set_text_index_cache_max_bytes(parsed_index_bytes: u64, puffin_blob_bytes: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES.store(parsed_index_bytes, Relaxed);
    CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES.store(puffin_blob_bytes, Relaxed);
}

/// Hand both budgets back to the environment and the constants, as if nothing
/// had configured them. For a test that pushes a budget and must not leave it
/// in force for the rest of the process — including the
/// `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES=0` A/B, which a leftover configured
/// budget would silently win over.
pub fn clear_text_index_cache_max_bytes() {
    set_text_index_cache_max_bytes(TEXT_INDEX_CACHE_UNCONFIGURED, TEXT_INDEX_CACHE_UNCONFIGURED);
}

fn configured_text_index_cache_max_bytes(budget: &std::sync::atomic::AtomicU64) -> Option<usize> {
    match budget.load(std::sync::atomic::Ordering::Relaxed) {
        TEXT_INDEX_CACHE_UNCONFIGURED => None,
        bytes => Some(bytes.min(usize::MAX as u64) as usize),
    }
}

/// The byte budgets these two caches are ACTUALLY enforcing, as
/// `(parsed index, Puffin blob)`.
///
/// Effective, not nominal: a zero entry bound disables both caches, so both
/// budgets are then 0. The query pool subtracts this rather than re-deriving
/// the numbers, because a process that publishes one budget and enforces
/// another is the shape of every over-commitment this system has had.
pub fn text_index_cache_max_bytes_in_force() -> (u64, u64) {
    if puffin_blob_cache_max_entries() == 0 {
        return (0, 0);
    }
    (
        parsed_index_cache_max_bytes() as u64,
        puffin_blob_cache_max_bytes() as u64,
    )
}

/// Entry bound on the blob cache. `0` disables both it and the parsed-index
/// cache, so every text query fetches and deserializes as it did before #3896.
fn puffin_blob_cache_max_entries() -> usize {
    puffin_blob_cache_max_entries_from(
        std::env::var("SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES")
            .ok()
            .as_deref(),
    )
}

fn puffin_blob_cache_max_entries_from(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_PUFFIN_BLOB_CACHE_MAX_ENTRIES)
}

/// Byte bound on the blob cache. `0` disables the blob cache alone: text
/// queries still reuse parsed indexes, and a parsed miss re-fetches the blob.
///
/// A budget resolved from the pod's memory limit
/// ([`set_text_index_cache_max_bytes`]) is used when there is one; the constant
/// below is what a process that never configures the caches keeps.
fn puffin_blob_cache_max_bytes() -> usize {
    if let Some(configured) =
        configured_text_index_cache_max_bytes(&CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES)
    {
        return configured;
    }
    puffin_blob_cache_max_bytes_from(
        std::env::var("SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES")
            .ok()
            .as_deref(),
    )
}

fn puffin_blob_cache_max_bytes_from(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES)
}

fn puffin_blob_cache() -> &'static std::sync::Mutex<PuffinBlobCacheInner> {
    PUFFIN_BLOB_CACHE.get_or_init(|| std::sync::Mutex::new(PuffinBlobCacheInner::default()))
}

/// How many blobs the cache holds for statistics files whose path contains
/// `path_substring`, their summed length, and the running byte total the cache
/// enforces its bound against.
///
/// The third is process-global where the first two are per-warehouse: a test
/// that wants to know its own blobs are counted asserts that the total covers
/// them, since other warehouses in the same process contribute too. Reading
/// this is not a lookup and does not renew an entry.
pub fn puffin_blob_cache_stats(path_substring: &str) -> (usize, usize, usize) {
    let cache = puffin_blob_cache().lock().unwrap();
    let (entries, bytes) = cache
        .map
        .iter()
        .filter(|((path, _), _)| path.contains(path_substring))
        .fold((0, 0), |(entries, bytes), (_, blob)| {
            (entries + 1, bytes + blob.len())
        });
    (entries, bytes, cache.bytes)
}

fn puffin_blob_cache_get(path: &str, offset: u64) -> Option<Arc<[u8]>> {
    if puffin_blob_cache_max_entries() == 0 || puffin_blob_cache_max_bytes() == 0 {
        return None;
    }
    puffin_blob_cache()
        .lock()
        .unwrap()
        .get(&(path.to_string(), offset))
}

fn puffin_blob_cache_put(path: &str, offset: u64, bytes: &[u8]) {
    let max_entries = puffin_blob_cache_max_entries();
    let max_bytes = puffin_blob_cache_max_bytes();
    // Decide before copying: a rejected blob must not cost an 81 MB allocation
    // on the way to being dropped.
    if max_entries == 0 || max_bytes == 0 || bytes.len() > max_bytes {
        return;
    }
    puffin_blob_cache().lock().unwrap().put(
        (path.to_string(), offset),
        Arc::<[u8]>::from(bytes),
        max_bytes,
        max_entries,
    );
}

/// Which write-once identity a parsed index is held under. Both storage shapes
/// share this cache, its byte budget and its LRU; the variants keep their key
/// spaces apart, so a data file and a statistics file of the same name — or the
/// same file's `raw` and `body` indexes — never collide.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ParsedIndexKey {
    /// A Puffin blob: the statistics file and the blob's offset within it. The
    /// identity the blob-bytes cache uses.
    Puffin { path: String, offset: u64 },
    /// A Parquet footer-KV index (`siglake.inverted_index.v1`): the data file
    /// and the column it indexes. An Iceberg data file is written once, so the
    /// pair names one blob for the file's life (#3965).
    FooterKv { path: String, column: String },
}

impl ParsedIndexKey {
    fn puffin(path: &str, offset: u64) -> Self {
        Self::Puffin {
            path: path.to_string(),
            offset,
        }
    }

    fn footer_kv(path: &str, column: &str) -> Self {
        Self::FooterKv {
            path: path.to_string(),
            column: column.to_string(),
        }
    }

    /// The file the entry came from — a statistics file or a data file.
    fn path(&self) -> &str {
        match self {
            Self::Puffin { path, .. } | Self::FooterKv { path, .. } => path,
        }
    }

    /// The `storage` label the index metrics carry, so a cache outcome can be
    /// read against the stage timings of the same form.
    fn storage(&self) -> &'static str {
        match self {
            Self::Puffin { .. } => TEXT_INDEX_STORAGE_PUFFIN,
            Self::FooterKv { .. } => TEXT_INDEX_STORAGE_FOOTER_KV,
        }
    }
}

pub(crate) const TEXT_INDEX_STORAGE_PUFFIN: &str = "puffin";
pub(crate) const TEXT_INDEX_STORAGE_FOOTER_KV: &str = "footer_kv";

/// Both `storage` values every index metric carries: a Puffin blob beside the
/// data file, or the index inlined in the Parquet footer.
pub const TEXT_INDEX_STORAGE_FORMS: &[&str] =
    &[TEXT_INDEX_STORAGE_PUFFIN, TEXT_INDEX_STORAGE_FOOTER_KV];

/// The `stage` values of `siglake_iceberg_text_index_startup_seconds` — the
/// four sections a text query's per-file startup splits into, which run #73
/// could not tell apart from the round's artifacts (#3969). A regression is
/// queueing (`permit_wait`), object storage (`blob_fetch`), deserialization
/// (`decode`) or the postings-and-runs work above the index (`selection`), and
/// no single total can say which. The vocabulary is closed: these four and
/// `storage` ("puffin" / "footer_kv") are every label value emitted, eight
/// series at most.
const TEXT_INDEX_STAGE_PERMIT_WAIT: &str = "permit_wait";
const TEXT_INDEX_STAGE_BLOB_FETCH: &str = "blob_fetch";
const TEXT_INDEX_STAGE_DECODE: &str = "decode";
const TEXT_INDEX_STAGE_SELECTION: &str = "selection";

/// Every `stage` value the reader records, for the tests that hold the
/// dashboard's grouping to this vocabulary.
pub const TEXT_INDEX_STARTUP_STAGES: &[&str] = &[
    TEXT_INDEX_STAGE_PERMIT_WAIT,
    TEXT_INDEX_STAGE_BLOB_FETCH,
    TEXT_INDEX_STAGE_DECODE,
    TEXT_INDEX_STAGE_SELECTION,
];

/// One section of a text query's per-file index startup. `_seconds`, so
/// siglake-core's exporter gives it real histogram buckets and a round can
/// take a fleet-wide quantile per stage.
fn record_text_index_stage(stage: &'static str, storage: &'static str, took: std::time::Duration) {
    metrics::histogram!(
        "siglake_iceberg_text_index_startup_seconds",
        "stage" => stage,
        "storage" => storage
    )
    .record(took.as_secs_f64());
}

const PARSED_INDEX_CACHE_HIT: &str = "hit";
const PARSED_INDEX_CACHE_MISS: &str = "miss";

/// Both `outcome` values of `siglake_iceberg_parsed_index_cache_lookups_total`.
pub const PARSED_INDEX_CACHE_OUTCOMES: &[&str] = &[PARSED_INDEX_CACHE_HIT, PARSED_INDEX_CACHE_MISS];

const PARSED_INDEX_DROP_BYTE_BOUND: &str = "byte_bound";
const PARSED_INDEX_DROP_ENTRY_BOUND: &str = "entry_bound";
const PARSED_INDEX_DROP_OVERSIZED: &str = "oversized";

/// Every `reason` value of
/// `siglake_iceberg_parsed_index_cache_evictions_total`.
pub const PARSED_INDEX_CACHE_DROP_REASONS: &[&str] = &[
    PARSED_INDEX_DROP_BYTE_BOUND,
    PARSED_INDEX_DROP_ENTRY_BOUND,
    PARSED_INDEX_DROP_OVERSIZED,
];

/// Whether a query found the parsed index already decoded. Exactly one outcome
/// is recorded per acquisition that finds an index at all: the repeated
/// lookups a single cold load makes (before the permit, after it, and against
/// the blob-bytes cache) are one `miss`, recorded where the decode happens, so
/// the two arms sum to acquisitions and the hit ratio reads directly.
fn record_parsed_index_lookup(outcome: &'static str, storage: &'static str) {
    metrics::counter!(
        "siglake_iceberg_parsed_index_cache_lookups_total",
        "outcome" => outcome,
        "storage" => storage
    )
    .increment(1);
}

/// Parsed per-file inverted indexes, keyed by the write-once identity of the
/// blob they were decoded from ([`ParsedIndexKey`]). Neither a Puffin blob at an
/// offset nor a data file's footer is ever rewritten, so an entry can never go
/// stale — it is dropped only by the bounds below.
///
/// Why it exists: deserializing a blob costs time proportional to the file's
/// rows (242 ms for a 7.3M-row file, measured in #3896), and every text query
/// over that file used to pay it again. Entries are shared as `Arc`s, so a hit
/// copies no postings.
struct ParsedIndexEntry {
    index: Arc<siglake_index::InvertedIndex>,
    size: usize,
    /// Query-time lookups this entry has served since it was decoded. Tests
    /// read it through [`parsed_inverted_index_cache_hits`] to tell a warm
    /// lookup from a repeat decode.
    hits: u64,
}

#[derive(Default)]
struct ParsedIndexCacheInner {
    order: std::collections::VecDeque<ParsedIndexKey>,
    map: std::collections::HashMap<ParsedIndexKey, ParsedIndexEntry>,
    bytes: usize,
    /// Entries dropped by the byte or entry bound. A text plan whose parsed
    /// working set exceeds the budget shows up here and nowhere else: the hit
    /// counter only says a lookup found nothing, not that the entry it wanted
    /// had been decoded and thrown away. Diagnostics, like [`ParsedIndexEntry::hits`].
    evictions: u64,
    /// Indexes never admitted because one of them alone exceeds the whole
    /// budget. Those files re-decode on every query and the cache is inert for
    /// them, which reads identically to a cold cache unless it is counted.
    oversized_skips: u64,
}

impl ParsedIndexCacheInner {
    fn get(&mut self, key: &ParsedIndexKey) -> Option<Arc<siglake_index::InvertedIndex>> {
        let hit = self.map.get_mut(key).map(|entry| {
            entry.hits += 1;
            Arc::clone(&entry.index)
        })?;
        // Least-recently-used, so that a working set larger than the budget
        // keeps the files being queried rather than the ones read first.
        if let Some(position) = self.order.iter().position(|entry| entry == key) {
            let key = self.order.remove(position).expect("position is in range");
            self.order.push_back(key);
        }
        Some(hit)
    }

    fn put(
        &mut self,
        key: ParsedIndexKey,
        index: Arc<siglake_index::InvertedIndex>,
        max_bytes: usize,
        max_entries: usize,
    ) {
        let size = index.heap_size_bytes();
        // One index larger than the whole budget would evict everything and
        // then be evicted itself: leave it to decode per query.
        if size > max_bytes {
            self.oversized_skips += 1;
            record_parsed_index_dropped(PARSED_INDEX_DROP_OVERSIZED);
            return;
        }
        if self.map.contains_key(&key) {
            return;
        }
        self.map.insert(key.clone(), ParsedIndexEntry {
            index,
            size,
            hits: 0,
        });
        self.order.push_back(key);
        self.bytes += size;
        while self.order.len() > max_entries || self.bytes > max_bytes {
            // Which bound bit, so a round can tell a working set over the byte
            // budget (raise `SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES`) from a plan
            // over the entry bound (raise
            // `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES`).
            let reason = if self.order.len() > max_entries {
                PARSED_INDEX_DROP_ENTRY_BOUND
            } else {
                PARSED_INDEX_DROP_BYTE_BOUND
            };
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.map.remove(&evicted) {
                self.bytes -= entry.size;
                self.evictions += 1;
                record_parsed_index_dropped(reason);
            }
        }
        // Resident against budget, published wherever the bounds are enforced.
        // Hit and miss rates alone cannot say whether a miss is a first read or
        // an entry this cache decoded and threw away (#3969).
        metrics::gauge!("siglake_iceberg_parsed_index_cache_bytes").set(self.bytes as f64);
        metrics::gauge!("siglake_iceberg_parsed_index_cache_max_bytes").set(max_bytes as f64);
    }
}

/// A parsed index the cache would not keep: evicted by one of the two bounds,
/// or never admitted because it alone exceeds the byte budget. `reason` is one
/// of three literals — the dashboard groups by it rather than matching on it.
fn record_parsed_index_dropped(reason: &'static str) {
    metrics::counter!(
        "siglake_iceberg_parsed_index_cache_evictions_total",
        "reason" => reason
    )
    .increment(1);
}

static PARSED_INDEX_CACHE: std::sync::OnceLock<std::sync::Mutex<ParsedIndexCacheInner>> =
    std::sync::OnceLock::new();
/// A parsed index costs about 40 bytes per indexed row (measured in #3896: a
/// 7.34M-row file parses to 294 MB from an 81 MB blob), so this holds the
/// indexes of three or four large compacted files. A plan that keeps more than
/// that warm wants the knob raised; beyond the budget the oldest entries are
/// dropped and those files decode again, which is what every query did before.
const DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES: usize = 1024 * 1024 * 1024;

/// Byte bound on the parsed-index cache, covering Puffin and footer-KV entries
/// together. `0` disables it (every query then deserializes, as before #3896
/// and #3965). Its entry bound is the blob cache's
/// `SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES`: the blob cache holds the Puffin
/// blobs this one holds parsed, so one entry count governs both — a footer-KV
/// entry, which has no serialized twin to retain, counts against the same bound.
///
/// As with the blob cache, a budget resolved from the pod's memory limit
/// ([`set_text_index_cache_max_bytes`]) wins over both the environment and the
/// constant.
fn parsed_index_cache_max_bytes() -> usize {
    if let Some(configured) =
        configured_text_index_cache_max_bytes(&CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES)
    {
        return configured;
    }
    parsed_index_cache_max_bytes_from(
        std::env::var("SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES")
            .ok()
            .as_deref(),
    )
}

fn parsed_index_cache_max_bytes_from(raw: Option<&str>) -> usize {
    raw.and_then(|v| v.trim().parse().ok())
        .unwrap_or(DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES)
}

fn parsed_index_cache() -> &'static std::sync::Mutex<ParsedIndexCacheInner> {
    PARSED_INDEX_CACHE.get_or_init(|| std::sync::Mutex::new(ParsedIndexCacheInner::default()))
}

static INVERTED_INDEX_DECODES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static INVERTED_INDEX_CACHE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many whole per-file inverted indexes this process has deserialized, and
/// how many query-time lookups were served from the parsed-index cache instead.
/// Diagnostics for tests and local measurement — not a metric, and not
/// exported.
pub fn inverted_index_decode_counts() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        INVERTED_INDEX_DECODES.load(Relaxed),
        INVERTED_INDEX_CACHE_HITS.load(Relaxed),
    )
}

/// What the parsed-index cache is holding and what it has thrown away.
///
/// Unlike [`parsed_inverted_index_cache_stats`] these are whole-cache numbers:
/// the bounds are enforced across every warehouse in the process, so an
/// eviction cannot be attributed to one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParsedIndexCacheFootprint {
    /// Entries resident now.
    pub entries: usize,
    /// Summed [`siglake_index::InvertedIndex::heap_size_bytes`] of those
    /// entries — the parsed working set the budget is spent on.
    pub bytes: usize,
    /// Entries dropped by the byte or entry bound since the process started.
    pub evictions: u64,
    /// Indexes never admitted because one exceeded the whole budget.
    pub oversized_skips: u64,
}

/// A snapshot of [`ParsedIndexCacheFootprint`]. Diagnostics for tests and local
/// measurement — not a metric, and not exported.
pub fn parsed_inverted_index_cache_footprint() -> ParsedIndexCacheFootprint {
    let cache = parsed_index_cache().lock().unwrap();
    ParsedIndexCacheFootprint {
        entries: cache.map.len(),
        bytes: cache.bytes,
        evictions: cache.evictions,
        oversized_skips: cache.oversized_skips,
    }
}

/// How many parsed indexes are cached for files whose path contains
/// `path_substring` — statistics files for Puffin entries, data files for
/// footer-KV ones — and how many query-time lookups they have served since
/// they were deserialized. Reading this is not itself a lookup: it neither
/// counts as a hit nor renews an entry.
///
/// A per-warehouse diagnostic for tests: a cached entry plus a lookup it
/// served is what distinguishes a warm query from one that deserialized the
/// blob again, and global counters cannot tell one warehouse from another
/// while other tests query in the same process.
pub fn parsed_inverted_index_cache_stats(path_substring: &str) -> (usize, u64) {
    parsed_index_cache()
        .lock()
        .unwrap()
        .map
        .iter()
        .filter(|(key, _)| key.path().contains(path_substring))
        .fold((0, 0), |(entries, hits), (_, entry)| {
            (entries + 1, hits + entry.hits)
        })
}

fn parsed_index_cache_get(key: &ParsedIndexKey) -> Option<Arc<siglake_index::InvertedIndex>> {
    if parsed_index_cache_max_bytes() == 0 || puffin_blob_cache_max_entries() == 0 {
        return None;
    }
    let hit = parsed_index_cache().lock().unwrap().get(key);
    if hit.is_some() {
        INVERTED_INDEX_CACHE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        record_parsed_index_lookup(PARSED_INDEX_CACHE_HIT, key.storage());
    }
    hit
}

fn parsed_index_cache_put(key: ParsedIndexKey, index: Arc<siglake_index::InvertedIndex>) {
    let max_bytes = parsed_index_cache_max_bytes();
    let max_entries = puffin_blob_cache_max_entries();
    if max_bytes == 0 || max_entries == 0 {
        return;
    }
    parsed_index_cache()
        .lock()
        .unwrap()
        .put(key, index, max_bytes, max_entries);
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
pub struct ArrowFileReader {
    meta: FileMetadata,
    parquet_read_options: ParquetReadOptions,
    r: Box<dyn FileRead>,
    byte_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
    scan_counters: Option<Arc<ScanCounters>>,
    current_phase: ObjectStoreReadPhase,
}

impl ArrowFileReader {
    /// Create a new ArrowFileReader
    pub fn new(meta: FileMetadata, r: Box<dyn FileRead>) -> Self {
        Self {
            meta,
            parquet_read_options: ParquetReadOptions::builder().build(),
            r,
            byte_counter: None,
            scan_counters: None,
            current_phase: ObjectStoreReadPhase::Data,
        }
    }

    /// Configure all Parquet read options.
    pub(crate) fn with_parquet_read_options(mut self, options: ParquetReadOptions) -> Self {
        self.parquet_read_options = options;
        self
    }

    /// Attach an optional per-scan object-store byte counter.
    pub(crate) fn with_byte_counter(
        mut self,
        counter: Option<Arc<std::sync::atomic::AtomicU64>>,
    ) -> Self {
        self.byte_counter = counter;
        self
    }

    /// Attach optional per-scan pruning/IO counters (read-request counting).
    pub(crate) fn with_scan_counters(mut self, counters: Option<Arc<ScanCounters>>) -> Self {
        self.scan_counters = counters;
        self
    }

    /// Record `n` fetched bytes against the global counter/metric and, if set,
    /// the per-scan counter.
    #[inline]
    fn record_phase_reads(&self, bytes: u64, read_count: usize) {
        record_object_store_bytes(bytes);
        record_object_store_reads(self.current_phase, read_count, bytes);
        if let Some(counter) = &self.byte_counter {
            counter.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(counters) = &self.scan_counters {
            counters
                .object_store_reads
                .fetch_add(read_count as u64, std::sync::atomic::Ordering::Relaxed);
            // F-5: same phase the global metric is already labelled with, so
            // the per-request split can never disagree with the global one.
            counters.add_phase_bytes(self.current_phase, bytes);
        }
    }

    async fn load_parquet_metadata(&mut self) -> Result<Arc<ParquetMetaData>> {
        let file_size = self.meta.size;
        let mut reader = ParquetMetaDataReader::new()
            .with_prefetch_hint(self.parquet_read_options.metadata_size_hint())
            .with_page_index_policy(PageIndexPolicy::Skip)
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(PageIndexPolicy::Skip);

        self.current_phase = ObjectStoreReadPhase::Footer;
        reader
            .try_load(&mut *self, file_size)
            .await
            .map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet footer metadata")
                    .with_source(e)
            })?;

        if self.parquet_read_options.preload_page_index()
            || self.parquet_read_options.preload_column_index()
            || self.parquet_read_options.preload_offset_index()
        {
            self.current_phase = ObjectStoreReadPhase::Index;
            reader = reader
                .with_page_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_page_index(),
                ))
                .with_column_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_column_index(),
                ))
                .with_offset_index_policy(PageIndexPolicy::from(
                    self.parquet_read_options.preload_offset_index(),
                ));
            reader.load_page_index(&mut *self).await.map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Failed to load Parquet page indexes")
                    .with_source(e)
            })?;
        }

        self.current_phase = ObjectStoreReadPhase::Data;
        reader.finish().map(Arc::new).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "Failed to finish Parquet metadata load")
                .with_source(e)
        })
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.record_phase_reads(range.end.saturating_sub(range.start), 1);
        Box::pin(
            self.r
                .read(range.start..range.end)
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    /// Override the default `get_byte_ranges` which calls `get_bytes` sequentially.
    /// The parquet reader calls this to fetch column chunks for a row group, so
    /// without this override each column chunk is a serial round-trip to object storage.
    /// Adapted from object_store's `coalesce_ranges` in `util.rs`.
    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let coalesce_bytes = self.parquet_read_options.range_coalesce_bytes();
        let concurrency = self.parquet_read_options.range_fetch_concurrency().max(1);

        async move {
            // Merge nearby ranges to reduce the number of object store requests.
            let fetch_ranges = merge_ranges(&ranges, coalesce_bytes);
            self.record_phase_reads(
                fetch_ranges
                    .iter()
                    .map(|r| r.end.saturating_sub(r.start))
                    .sum(),
                fetch_ranges.len(),
            );
            let r = &self.r;

            // Fetch merged ranges concurrently.
            let fetched: Vec<Bytes> = futures::stream::iter(fetch_ranges.iter().cloned())
                .map(|range| async move {
                    r.read(range)
                        .await
                        .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
                })
                .buffered(concurrency)
                .try_collect()
                .await?;

            // Slice the fetched data back into the originally requested ranges.
            Ok(ranges
                .iter()
                .map(|range| {
                    let idx = fetch_ranges.partition_point(|v| v.start <= range.start) - 1;
                    let fetch_range = &fetch_ranges[idx];
                    let fetch_bytes = &fetched[idx];
                    let start = (range.start - fetch_range.start) as usize;
                    let end = (range.end - fetch_range.start) as usize;
                    fetch_bytes.slice(start..end.min(fetch_bytes.len()))
                })
                .collect())
        }
        .boxed()
    }

    // TODO: currently we don't respect `ArrowReaderOptions` cause it don't expose any method to access the option field
    // we will fix it after `v55.1.0` is released in https://github.com/apache/arrow-rs/issues/7393
    fn get_metadata(
        &mut self,
        _options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            self.load_parquet_metadata().await.map_err(|e| {
                parquet::errors::ParquetError::External(Box::new(e))
            })
        }
        .boxed()
    }
}

/// Merge overlapping or nearby byte ranges, combining ranges with gaps <= `coalesce` bytes.
/// Adapted from object_store's `merge_ranges` in `util.rs`.
fn merge_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|r| r.start);

    let mut merged = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;

    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;

        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }

        merged.push(ranges[start_idx].start..range_end);
        start_idx = end_idx;
        end_idx += 1;
    }

    merged
}

/// The Arrow type of an array that the Parquet reader reads may not match the exact Arrow type
/// that Iceberg uses for literals - but they are effectively the same logical type,
/// i.e. LargeUtf8 and Utf8 or Utf8View and Utf8 or Utf8View and LargeUtf8.
///
/// The Arrow compute kernels that we use must match the type exactly, so first cast the literal
/// into the type of the batch we read from Parquet before sending it to the compute kernel.
fn try_cast_literal(
    literal: &Arc<dyn ArrowDatum + Send + Sync>,
    column_type: &DataType,
) -> std::result::Result<Arc<dyn ArrowDatum + Send + Sync>, ArrowError> {
    let literal_array = literal.get().0;

    // No cast required
    if literal_array.data_type() == column_type {
        return Ok(Arc::clone(literal));
    }

    let literal_array = cast(literal_array, column_type)?;
    Ok(Arc::new(Scalar::new(literal_array)))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use arrow_array::cast::AsArray;
    use arrow_array::{ArrayRef, LargeStringArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use parquet::arrow::{ArrowWriter, ProjectionMask};
    use parquet::basic::Compression;
    use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor};
    use roaring::RoaringTreemap;
    use tempfile::TempDir;

    use crate::ErrorKind;

    use crate::arrow::reader::{
        CollectFieldIdVisitor, PARQUET_FIELD_ID_META_KEY, ParquetReadOptions,
    };
    use crate::arrow::{ArrowReader, ArrowReaderBuilder};
    use crate::delete_vector::DeleteVector;
    use crate::expr::visitors::bound_predicate_visitor::visit;
    use crate::expr::{Bind, Predicate, Reference};
    use crate::io::{
        FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    };
    use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
    use crate::spec::{
        DataContentType, DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type,
    };

    #[derive(Debug, Default)]
    struct CountingStorageState {
        data: std::sync::Mutex<Bytes>,
        reads: AtomicUsize,
        block_first_read: AtomicBool,
        gate: tokio::sync::Notify,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CountingStorageFactory {
        #[serde(skip, default = "default_counting_storage_state")]
        state: Arc<CountingStorageState>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct CountingStorage {
        #[serde(skip, default = "default_counting_storage_state")]
        state: Arc<CountingStorageState>,
    }

    fn default_counting_storage_state() -> Arc<CountingStorageState> {
        Arc::new(CountingStorageState::default())
    }

    #[typetag::serde]
    impl StorageFactory for CountingStorageFactory {
        fn build(&self, _config: &StorageConfig) -> crate::Result<Arc<dyn Storage>> {
            Ok(Arc::new(CountingStorage {
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[async_trait]
    impl FileRead for CountingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let current = self.state.reads.fetch_add(1, Ordering::SeqCst) + 1;
            if current == 1 && self.state.block_first_read.load(Ordering::SeqCst) {
                self.state.gate.notified().await;
            }
            let data = self.state.data.lock().unwrap();
            Ok(data.slice(range.start as usize..range.end as usize))
        }
    }

    #[derive(Debug)]
    struct CountingFileRead {
        state: Arc<CountingStorageState>,
    }

    #[derive(Debug)]
    struct NoopFileWrite;

    #[async_trait]
    impl FileWrite for NoopFileWrite {
        async fn write(&mut self, _bs: Bytes) -> crate::Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for CountingStorage {
        async fn exists(&self, _path: &str) -> crate::Result<bool> {
            Ok(true)
        }

        async fn metadata(&self, _path: &str) -> crate::Result<FileMetadata> {
            Ok(FileMetadata {
                size: self.state.data.lock().unwrap().len() as u64,
            })
        }

        async fn read(&self, _path: &str) -> crate::Result<Bytes> {
            Ok(self.state.data.lock().unwrap().clone())
        }

        async fn reader(&self, _path: &str) -> crate::Result<Box<dyn FileRead>> {
            Ok(Box::new(CountingFileRead {
                state: Arc::clone(&self.state),
            }))
        }

        async fn write(&self, _path: &str, _bs: Bytes) -> crate::Result<()> {
            Ok(())
        }

        async fn writer(&self, _path: &str) -> crate::Result<Box<dyn FileWrite>> {
            Ok(Box::new(NoopFileWrite))
        }

        async fn delete(&self, _path: &str) -> crate::Result<()> {
            Ok(())
        }

        async fn delete_prefix(&self, _path: &str) -> crate::Result<()> {
            Ok(())
        }

        fn new_input(&self, path: &str) -> crate::Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }

        fn new_output(&self, path: &str) -> crate::Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }

    fn parquet_bytes_for_footer_debounce_test() -> Bytes {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("raw", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from_iter_values([
                "alpha", "beta", "gamma", "delta",
            ])) as ArrayRef],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buf)
    }

    fn footer_debounce_file_io(state: &Arc<CountingStorageState>) -> FileIO {
        FileIOBuilder::new(Arc::new(CountingStorageFactory {
            state: Arc::clone(state),
        }))
        .build()
    }

    async fn load_footer_once(path: &str, file_io: &FileIO, file_size: u64) {
        ArrowReader::open_parquet_file(
            path,
            file_io,
            file_size,
            ParquetReadOptions::builder().build(),
            None,
            None,
            false,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn concurrent_footer_reads_are_debounced() {
        let bytes = parquet_bytes_for_footer_debounce_test();
        let file_size = bytes.len() as u64;

        // Baseline: what one uncontended footer load costs in underlying
        // reads. The metadata path fetches the footer and then the index
        // ranges it points at, so the claim under test is that eight callers
        // cost what one costs — not that either costs a single read. The two
        // phases use different paths because the footer cache is
        // process-wide and keyed by path.
        let solo = Arc::new(CountingStorageState::default());
        *solo.data.lock().unwrap() = bytes.clone();
        let solo_io = footer_debounce_file_io(&solo);
        load_footer_once("memory://footer-debounce-solo.parquet", &solo_io, file_size).await;
        let baseline_reads = solo.reads.load(Ordering::SeqCst);
        assert!(baseline_reads > 0, "the baseline load should read the file");

        let state = Arc::new(CountingStorageState::default());
        *state.data.lock().unwrap() = bytes;
        state.block_first_read.store(true, Ordering::SeqCst);
        let file_io = footer_debounce_file_io(&state);

        let tasks = (0..8).map(|_| {
            let file_io = file_io.clone();
            async move {
                load_footer_once(
                    "memory://footer-debounce-contended.parquet",
                    &file_io,
                    file_size,
                )
                .await;
            }
        });
        let mut join = std::pin::pin!(futures::future::join_all(tasks));
        // Drive every caller to its park point instead of waiting on a clock.
        // `join_all` polls each child in turn, so one pass takes the first
        // caller into the gated underlying read and the other seven onto the
        // shared in-flight footer load. The gate is still shut, so no caller
        // can finish and the count below cannot move under us. Repeat passes
        // are harmless — a parked caller polls to `Pending` again without
        // touching storage — and cover a cooperative-budget yield inside the
        // metadata read path.
        for _ in 0..4 {
            assert!(
                futures::poll!(join.as_mut()).is_pending(),
                "no caller can complete while the first underlying read is gated"
            );
        }
        assert_eq!(
            state.reads.load(Ordering::SeqCst),
            1,
            "eight concurrent footer loads should enter the storage layer once"
        );

        state.gate.notify_waiters();
        join.await;
        assert_eq!(
            state.reads.load(Ordering::SeqCst),
            baseline_reads,
            "eight concurrent footer loads should cost what one costs"
        );
    }

    fn table_schema_simple() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![2])
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                    NestedField::optional(4, "qux", Type::Primitive(PrimitiveType::Float)).into(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_collect_field_id() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux").is_null();
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_and() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .and(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_collect_field_id_with_or() {
        let schema = table_schema_simple();
        let expr = Reference::new("qux")
            .is_null()
            .or(Reference::new("baz").is_null());
        let bound_expr = expr.bind(schema, true).unwrap();

        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::default(),
        };
        visit(&mut visitor, &bound_expr).unwrap();

        let mut expected = HashSet::default();
        expected.insert(4_i32);
        expected.insert(3);

        assert_eq!(visitor.field_ids, expected);
    }

    #[test]
    fn test_arrow_projection_mask() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_identifier_field_ids(vec![1])
                .with_fields(vec![
                    NestedField::required(1, "c1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(2, "c2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(
                        3,
                        "c3",
                        Type::Primitive(PrimitiveType::Decimal {
                            precision: 38,
                            scale: 3,
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("c1", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            // Type not supported
            Field::new("c2", DataType::Duration(TimeUnit::Microsecond), true).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            ),
            // Precision is beyond the supported range
            Field::new("c3", DataType::Decimal128(39, 3), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]));

        let message_type = "
message schema {
  required binary c1 (STRING) = 1;
  optional int32 c2 (INTEGER(8,true)) = 2;
  optional fixed_len_byte_array(17) c3 (DECIMAL(39,3)) = 3;
}
    ";
        let parquet_type = parse_message_type(message_type).expect("should parse schema");
        let parquet_schema = SchemaDescriptor::new(Arc::new(parquet_type));

        // Try projecting the fields c2 and c3 with the unsupported data types
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 2, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Unsupported Arrow data type: Duration(µs)".to_string()
        );

        // Omitting field c2, we still get an error due to c3 being selected
        let err = ArrowReader::get_arrow_projection_mask(
            &[1, 3],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert_eq!(
            err.to_string(),
            "DataInvalid => Failed to create decimal type, source: DataInvalid => Decimals with precision larger than 38 are not supported: 39".to_string()
        );

        // Finally avoid selecting fields with unsupported data types
        let mask = ArrowReader::get_arrow_projection_mask(
            &[1],
            &schema,
            &parquet_schema,
            &arrow_schema,
            false,
        )
        .expect("Some ProjectionMask");
        assert_eq!(mask, ProjectionMask::leaves(&parquet_schema, vec![0]));
    }

    #[tokio::test]
    async fn test_kleene_logic_or_behaviour() {
        // a IS NULL OR a = 'foo'
        let predicate = Reference::new("a")
            .is_null()
            .or(Reference::new("a").equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: [NULL, "foo"].
        let expected = vec![None, Some("foo".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_kleene_logic_and_behaviour() {
        // a IS NOT NULL AND a != 'foo'
        let predicate = Reference::new("a")
            .is_not_null()
            .and(Reference::new("a").not_equal_to(Datum::string("foo")));

        // Table data: [NULL, "foo", "bar"]
        let data_for_col_a = vec![None, Some("foo".to_string()), Some("bar".to_string())];

        // Expected: ["bar"].
        let expected = vec![Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::Utf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        let result_data = test_perform_read(predicate, schema, table_location, reader).await;

        assert_eq!(result_data, expected);
    }

    #[tokio::test]
    async fn test_predicate_cast_literal() {
        let predicates = vec![
            // a == 'foo'
            (Reference::new("a").equal_to(Datum::string("foo")), vec![
                Some("foo".to_string()),
            ]),
            // a != 'foo'
            (
                Reference::new("a").not_equal_to(Datum::string("foo")),
                vec![Some("bar".to_string())],
            ),
            // STARTS_WITH(a, 'foo')
            (Reference::new("a").starts_with(Datum::string("f")), vec![
                Some("foo".to_string()),
            ]),
            // NOT STARTS_WITH(a, 'foo')
            (
                Reference::new("a").not_starts_with(Datum::string("f")),
                vec![Some("bar".to_string())],
            ),
            // a < 'foo'
            (Reference::new("a").less_than(Datum::string("foo")), vec![
                Some("bar".to_string()),
            ]),
            // a <= 'foo'
            (
                Reference::new("a").less_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string()), Some("bar".to_string())],
            ),
            // a > 'foo'
            (
                Reference::new("a").greater_than(Datum::string("bar")),
                vec![Some("foo".to_string())],
            ),
            // a >= 'foo'
            (
                Reference::new("a").greater_than_or_equal_to(Datum::string("foo")),
                vec![Some("foo".to_string())],
            ),
            // a IN ('foo', 'bar')
            (
                Reference::new("a").is_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("foo".to_string())],
            ),
            // a NOT IN ('foo', 'bar')
            (
                Reference::new("a").is_not_in([Datum::string("foo"), Datum::string("baz")]),
                vec![Some("bar".to_string())],
            ),
        ];

        // Table data: ["foo", "bar"]
        let data_for_col_a = vec![Some("foo".to_string()), Some("bar".to_string())];

        let (file_io, schema, table_location, _temp_dir) =
            setup_kleene_logic(data_for_col_a, DataType::LargeUtf8);
        let reader = ArrowReaderBuilder::new(file_io).build();

        for (predicate, expected) in predicates {
            println!("testing predicate {predicate}");
            let result_data = test_perform_read(
                predicate.clone(),
                schema.clone(),
                table_location.clone(),
                reader.clone(),
            )
            .await;

            assert_eq!(result_data, expected, "predicate={predicate}");
        }
    }

    async fn test_perform_read(
        predicate: Predicate,
        schema: SchemaRef,
        table_location: String,
        reader: ArrowReader,
    ) -> Vec<Option<String>> {
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1],
                predicate: Some(predicate.bind(schema, true).unwrap()),
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        result[0].columns()[0]
            .as_string_opt::<i32>()
            .unwrap()
            .iter()
            .map(|v| v.map(ToOwned::to_owned))
            .collect::<Vec<_>>()
    }

    fn setup_kleene_logic(
        data_for_col_a: Vec<Option<String>>,
        col_a_type: DataType,
    ) -> (FileIO, SchemaRef, String, TempDir) {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "a", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", col_a_type.clone(), true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        let file_io = FileIO::new_with_fs();

        let col = match col_a_type {
            DataType::Utf8 => Arc::new(StringArray::from(data_for_col_a)) as ArrayRef,
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data_for_col_a)) as ArrayRef,
            _ => panic!("unexpected col_a_type"),
        };

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![col]).unwrap();

        // Write the Parquet files
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();

        writer.write(&to_write).expect("Writing batch");

        // writer must be closed to write footer
        writer.close().unwrap();

        (file_io, schema, table_location, tmp_dir)
    }

    #[test]
    fn test_build_deletes_row_selection() {
        let schema_descr = get_test_schema_descr();

        let mut columns = vec![];
        for ptr in schema_descr.columns() {
            let column = ColumnChunkMetaData::builder(ptr.clone()).build().unwrap();
            columns.push(column);
        }

        let row_groups_metadata = vec![
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 0),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 1),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 2),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 1000, 3),
            build_test_row_group_meta(schema_descr.clone(), columns.clone(), 500, 4),
        ];

        let selected_row_groups = Some(vec![1, 3]);

        /* cases to cover:
           * {skip|select} {first|intermediate|last} {one row|multiple rows} in
             {first|intermediate|last} {skipped|selected} row group
           * row group selection disabled
        */

        let positional_deletes = RoaringTreemap::from_iter(&[
            1, // in skipped rg 0, should be ignored
            3, // run of three consecutive items in skipped rg0
            4, 5, 998, // two consecutive items at end of skipped rg0
            999, 1000, // solitary row at start of selected rg1 (1, 9)
            1010, // run of 3 rows in selected rg1
            1011, 1012, // (3, 485)
            1498, // run of two items at end of selected rg1
            1499, 1500, // run of two items at start of skipped rg2
            1501, 1600, // should ignore, in skipped rg2
            1999, // single row at end of skipped rg2
            2000, // run of two items at start of selected rg3
            2001, // (4, 98)
            2100, // single row in selected row group 3 (1, 99)
            2200, // run of 3 consecutive rows in selected row group 3
            2201, 2202, // (3, 796)
            2999, // single item at end of selected rg3 (1)
            3000, // single item at start of skipped rg4
        ]);

        let positional_deletes = DeleteVector::new(positional_deletes);

        // using selected row groups 1 and 3
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &selected_row_groups,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::skip(1),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(1),
        ]);

        assert_eq!(result, expected);

        // selecting all row groups
        let result = ArrowReader::build_deletes_row_selection(
            &row_groups_metadata,
            &None,
            &positional_deletes,
        )
        .unwrap();

        let expected = RowSelection::from(vec![
            RowSelector::select(1),
            RowSelector::skip(1),
            RowSelector::select(1),
            RowSelector::skip(3),
            RowSelector::select(992),
            RowSelector::skip(3),
            RowSelector::select(9),
            RowSelector::skip(3),
            RowSelector::select(485),
            RowSelector::skip(4),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(398),
            RowSelector::skip(3),
            RowSelector::select(98),
            RowSelector::skip(1),
            RowSelector::select(99),
            RowSelector::skip(3),
            RowSelector::select(796),
            RowSelector::skip(2),
            RowSelector::select(499),
        ]);

        assert_eq!(result, expected);
    }

    #[test]
    fn parsed_index_cache_max_bytes_resolves() {
        use crate::arrow::reader::{
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES, parsed_index_cache_max_bytes_from,
        };
        assert_eq!(
            parsed_index_cache_max_bytes_from(None),
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES
        );
        assert_eq!(parsed_index_cache_max_bytes_from(Some(" 4096 ")), 4096);
        assert_eq!(parsed_index_cache_max_bytes_from(Some("0")), 0);
        assert_eq!(
            parsed_index_cache_max_bytes_from(Some("all of it")),
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES
        );
    }

    /// The parsed-index cache holds hundreds of MB per large file, so its
    /// bounds are the thing to get right: bytes and entries both cap it, and
    /// what survives eviction is what was used most recently.
    #[test]
    fn parsed_index_cache_honours_both_bounds_and_keeps_the_used_entry() {
        use crate::arrow::reader::{ParsedIndexCacheInner, ParsedIndexKey};

        let index = Arc::new(siglake_index::InvertedIndex::from_rows([
            "database timeout",
            "request complete",
        ]));
        let size = index.heap_size_bytes();
        let key = |n: u64| ParsedIndexKey::puffin(&format!("s3://bucket/stats-{n}.puffin"), n);

        // Byte bound: room for two, third evicts the first.
        let mut cache = ParsedIndexCacheInner::default();
        for n in 0..3 {
            cache.put(key(n), Arc::clone(&index), size * 2, 128);
        }
        assert!(cache.get(&key(0)).is_none(), "oldest entry evicted");
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_some());

        // Entry bound, independent of bytes.
        let mut cache = ParsedIndexCacheInner::default();
        for n in 0..3 {
            cache.put(key(n), Arc::clone(&index), usize::MAX, 2);
        }
        assert!(cache.get(&key(0)).is_none());
        assert!(cache.get(&key(1)).is_some());

        // A hit renews an entry: entry 0 survives the insert that evicts 1.
        let mut cache = ParsedIndexCacheInner::default();
        cache.put(key(0), Arc::clone(&index), usize::MAX, 2);
        cache.put(key(1), Arc::clone(&index), usize::MAX, 2);
        assert!(cache.get(&key(0)).is_some());
        cache.put(key(2), Arc::clone(&index), usize::MAX, 2);
        assert!(cache.get(&key(0)).is_some(), "used entry survives");
        assert!(cache.get(&key(1)).is_none(), "unused entry evicted");

        // An index bigger than the whole budget is not cached, and a repeat
        // insert neither duplicates nor double-counts.
        let mut cache = ParsedIndexCacheInner::default();
        cache.put(key(0), Arc::clone(&index), size - 1, 128);
        assert!(cache.get(&key(0)).is_none());
        cache.put(key(0), Arc::clone(&index), usize::MAX, 128);
        cache.put(key(0), Arc::clone(&index), usize::MAX, 128);
        assert!(cache.get(&key(0)).is_some());
        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.bytes, size);

        // Handing out a hit shares the postings rather than copying them, and
        // the entry counts what it served.
        let hit = cache.get(&key(0)).unwrap();
        assert!(Arc::ptr_eq(&hit, &index));
        assert_eq!(cache.map[&key(0)].hits, 2);
    }

    /// #3965: Puffin and footer-KV entries share this cache, so their keys have
    /// to stay apart. A statistics file and a data file can carry the same
    /// path, and one data file carries one footer index per indexed column.
    #[test]
    fn parsed_index_cache_keys_separate_storage_shapes_files_and_columns() {
        use crate::arrow::reader::{ParsedIndexCacheInner, ParsedIndexKey};

        let raw = Arc::new(siglake_index::InvertedIndex::from_rows(["database timeout"]));
        let body = Arc::new(siglake_index::InvertedIndex::from_rows(["request complete"]));

        let mut cache = ParsedIndexCacheInner::default();
        let path = "s3://bucket/data/00000.parquet";
        cache.put(
            ParsedIndexKey::footer_kv(path, "raw"),
            Arc::clone(&raw),
            usize::MAX,
            128,
        );
        cache.put(
            ParsedIndexKey::footer_kv(path, "body"),
            Arc::clone(&body),
            usize::MAX,
            128,
        );
        assert!(Arc::ptr_eq(
            &cache.get(&ParsedIndexKey::footer_kv(path, "raw")).unwrap(),
            &raw,
        ));
        assert!(Arc::ptr_eq(
            &cache.get(&ParsedIndexKey::footer_kv(path, "body")).unwrap(),
            &body,
        ));
        assert!(
            cache
                .get(&ParsedIndexKey::footer_kv(
                    "s3://bucket/data/00001.parquet",
                    "raw"
                ))
                .is_none(),
            "another data file's raw index is a different entry"
        );
        assert!(
            cache.get(&ParsedIndexKey::puffin(path, 0)).is_none(),
            "a Puffin blob at this path is a different entry"
        );
    }

    #[test]
    fn puffin_blob_cache_bounds_resolve() {
        use crate::arrow::reader::{
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES, DEFAULT_PUFFIN_BLOB_CACHE_MAX_ENTRIES,
            puffin_blob_cache_max_bytes_from, puffin_blob_cache_max_entries_from,
        };
        assert_eq!(
            puffin_blob_cache_max_bytes_from(None),
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES
        );
        assert_eq!(puffin_blob_cache_max_bytes_from(Some(" 4096 ")), 4096);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some("0")), 0);
        assert_eq!(
            puffin_blob_cache_max_bytes_from(Some("as much as it takes")),
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES
        );

        assert_eq!(
            puffin_blob_cache_max_entries_from(None),
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_ENTRIES
        );
        assert_eq!(puffin_blob_cache_max_entries_from(Some(" 8 ")), 8);
        assert_eq!(puffin_blob_cache_max_entries_from(Some("0")), 0);
        assert_eq!(
            puffin_blob_cache_max_entries_from(Some("many")),
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_ENTRIES
        );
    }

    /// A budget resolved from the pod's memory limit must beat both the
    /// environment and the constants, and the answer the query pool subtracts
    /// must be the one the caches enforce.
    ///
    /// WHY (#4056). These were the last read caches sized by a flat constant:
    /// 1 GiB of parsed indexes plus 256 MiB of blobs on every pod, which is the
    /// whole of what the packaged 4Gi query pod leaves outside its pool and its
    /// other caches. The cgroup limit is read in siglake-storage, which depends
    /// on this crate, so the budget has to arrive through this setter.
    #[test]
    fn configured_text_index_budgets_beat_the_constants() {
        use crate::arrow::reader::{
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES, DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES,
            clear_text_index_cache_max_bytes, parsed_index_cache_max_bytes,
            puffin_blob_cache_max_bytes, set_text_index_cache_max_bytes,
            text_index_cache_max_bytes_in_force,
        };

        clear_text_index_cache_max_bytes();
        assert_eq!(
            parsed_index_cache_max_bytes(),
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES,
            "unconfigured, the constant stands"
        );
        assert_eq!(
            puffin_blob_cache_max_bytes(),
            DEFAULT_PUFFIN_BLOB_CACHE_MAX_BYTES
        );

        // What a 4Gi pod derives.
        set_text_index_cache_max_bytes(256 * 1024 * 1024, 64 * 1024 * 1024);
        assert_eq!(parsed_index_cache_max_bytes(), 256 * 1024 * 1024);
        assert_eq!(puffin_blob_cache_max_bytes(), 64 * 1024 * 1024);
        assert_eq!(
            text_index_cache_max_bytes_in_force(),
            (256 * 1024 * 1024, 64 * 1024 * 1024),
            "the pool must subtract what the caches are enforcing"
        );

        // `0` keeps its meaning through the setter: the serialized copy is
        // dropped and reserves nothing, while parsed indexes stay cached.
        set_text_index_cache_max_bytes(256 * 1024 * 1024, 0);
        assert_eq!(
            text_index_cache_max_bytes_in_force(),
            (256 * 1024 * 1024, 0)
        );

        clear_text_index_cache_max_bytes();
        assert_eq!(
            parsed_index_cache_max_bytes(),
            DEFAULT_PARSED_INDEX_CACHE_MAX_BYTES,
            "clearing must hand the budgets back"
        );
    }

    /// An index blob is data-sized — 81 MB for a 7.3M-row file — so the entry
    /// count alone never bounded what this holds. Bytes and entries both cap
    /// it, and the byte total tracks insertions and evictions exactly.
    #[test]
    fn puffin_blob_cache_honours_both_bounds() {
        use crate::arrow::reader::PuffinBlobCacheInner;

        let blob = |size: usize| Arc::<[u8]>::from(vec![7u8; size]);
        let key = |n: u64| (format!("s3://bucket/stats-{n}.puffin"), n);

        // Byte bound: room for two 100-byte blobs, the third evicts the first.
        let mut cache = PuffinBlobCacheInner::default();
        for n in 0..3 {
            cache.put(key(n), blob(100), 200, 128);
        }
        assert!(cache.get(&key(0)).is_none(), "oldest entry evicted");
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_some());
        assert_eq!(cache.bytes, 200);

        // One oversized blob does not evict the entries that fit.
        cache.put(key(3), blob(201), 200, 128);
        assert!(cache.get(&key(3)).is_none(), "oversized blob refused");
        assert!(cache.get(&key(1)).is_some(), "survivors kept");
        assert_eq!(cache.bytes, 200);

        // Entry bound, independent of bytes.
        let mut cache = PuffinBlobCacheInner::default();
        for n in 0..3 {
            cache.put(key(n), blob(100), usize::MAX, 2);
        }
        assert!(cache.get(&key(0)).is_none());
        assert!(cache.get(&key(1)).is_some());
        assert_eq!(cache.bytes, 200);

        // A repeat insert neither duplicates nor double-counts, and eviction
        // returns the bytes it took.
        let mut cache = PuffinBlobCacheInner::default();
        cache.put(key(0), blob(100), usize::MAX, 2);
        cache.put(key(0), blob(100), usize::MAX, 2);
        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.bytes, 100);
        cache.put(key(1), blob(50), usize::MAX, 2);
        cache.put(key(2), blob(50), usize::MAX, 2);
        assert_eq!(cache.bytes, 100, "evicting the 100-byte entry frees 100");
        assert!(cache.get(&key(0)).is_none());
    }

    /// #3896 replaced "complement of the matches as a delete vector" with a
    /// selection built from the matches. The two must agree exactly, including
    /// over a partial row-group selection, on runs that touch the first and
    /// last row of a group, and on the empty and full match sets.
    #[test]
    fn index_matches_row_selection_agrees_with_the_complement_form() {
        let row_groups = vec![1000u32, 500, 500, 1000, 500];
        let total_rows: u64 = row_groups.iter().map(|rows| *rows as u64).sum();
        let row_groups_metadata = row_groups_of_sizes(&row_groups);

        let all: Vec<u32> = (0..total_rows as u32).collect();
        let shapes: Vec<(&str, Vec<u32>)> = vec![
            ("empty", vec![]),
            ("all", all),
            ("first row only", vec![0]),
            ("last row only", vec![3499]),
            ("row-group boundaries", vec![999, 1000, 1499, 1500, 1999, 2000]),
            ("runs and singletons", vec![
                1, 3, 4, 5, 998, 999, 1010, 1011, 1012, 2100, 2200, 2201, 2999, 3000,
            ]),
            ("one whole row group", (1000..1500).collect()),
            ("sparse", (0..total_rows as u32).step_by(97).collect()),
        ];
        let selections = [
            None,
            Some(vec![1, 3]),
            Some(vec![0]),
            Some(vec![4]),
            Some(vec![0, 1, 2, 3, 4]),
            Some(vec![0, 2, 4]),
        ];

        for (label, matching) in &shapes {
            for selected_row_groups in &selections {
                let mut complement = RoaringTreemap::new();
                complement.insert_range(0..total_rows);
                for ordinal in matching {
                    complement.remove(*ordinal as u64);
                }
                let expected = ArrowReader::build_deletes_row_selection(
                    &row_groups_metadata,
                    selected_row_groups,
                    &DeleteVector::new(complement),
                )
                .unwrap();
                let actual = ArrowReader::index_matches_row_selection(
                    &row_groups_metadata,
                    selected_row_groups,
                    matching,
                );
                assert_eq!(
                    actual, expected,
                    "shape {label} over row groups {selected_row_groups:?}"
                );
            }
        }
    }

    /// A corpus-shaped index over `n_rows` rows: ten tokens a row drawn from a
    /// few thousand terms, plus one rare term on every thousandth row. The
    /// posting count per row is what the serialized blob's size tracks, so this
    /// stands in for a compacted events file of the same row count.
    fn synthetic_raw_index(n_rows: u32) -> siglake_index::InvertedIndex {
        let mut builder = siglake_index::IndexBuilder::new();
        for row in 0..n_rows {
            let rare = if row.is_multiple_of(1000) {
                " rareneedle"
            } else {
                ""
            };
            builder.push_row(&format!(
                "service-{} status {} region reg{} bucket{:02} latency {}ms request complete{}",
                row % 20,
                200 + row % 5,
                row % 8,
                row % 20,
                row % 250,
                rare,
            ));
        }
        builder.build()
    }

    fn row_groups_of_sizes(sizes: &[u32]) -> Vec<RowGroupMetaData> {
        let schema_descr = get_test_schema_descr();
        let columns: Vec<ColumnChunkMetaData> = schema_descr
            .columns()
            .iter()
            .map(|ptr| ColumnChunkMetaData::builder(ptr.clone()).build().unwrap())
            .collect();
        sizes
            .iter()
            .enumerate()
            .map(|(ordinal, rows)| {
                build_test_row_group_meta(
                    schema_descr.clone(),
                    columns.clone(),
                    *rows as i64,
                    ordinal as i16,
                )
            })
            .collect()
    }

    fn row_groups_of(n_rows: u32, rows_per_group: u32) -> Vec<RowGroupMetaData> {
        let mut sizes = Vec::new();
        let mut remaining = n_rows;
        while remaining > 0 {
            let rows = remaining.min(rows_per_group);
            sizes.push(rows);
            remaining -= rows;
        }
        row_groups_of_sizes(&sizes)
    }

    fn median_duration(runs: usize, mut f: impl FnMut() -> std::time::Duration) -> f64 {
        let mut samples: Vec<std::time::Duration> = (0..runs).map(|_| f()).collect();
        samples.sort();
        samples[runs / 2].as_secs_f64() * 1000.0
    }

    /// Measurement for task #3896: what one text query pays per planned indexed
    /// file before its first batch, split into whole-index deserialization
    /// (`InvertedIndex::from_bytes`) and row-selection construction. Semaphore
    /// wait is not measured here — it is a multiple of the decode time, four
    /// files at a time (`index_load_semaphore`).
    #[test]
    #[ignore = "measurement; run with --ignored --nocapture"]
    fn report_inverted_index_startup_cost() {
        for n_rows in [1_000_000u32, 7_341_274] {
            let index = synthetic_raw_index(n_rows);
            let bytes = index.to_bytes();
            let row_groups = row_groups_of(n_rows, 131_072);
            let total_rows = n_rows as u64;

            let decode_ms = median_duration(3, || {
                let started = std::time::Instant::now();
                let parsed = siglake_index::InvertedIndex::from_bytes(&bytes).unwrap();
                let elapsed = started.elapsed();
                std::hint::black_box(parsed.n_terms());
                elapsed
            });

            // The selection shapes the round-73 profiles saw: a rare needle
            // (0.1% of rows) and a common term (5% of rows).
            for (label, matching) in [
                ("rare", index.matching_rows_all(&["rareneedle"])),
                ("common", index.matching_rows_all(&["bucket07"])),
            ] {
                assert!(!matching.is_empty(), "shape {label} must match something");
                let complement_ms = median_duration(3, || {
                    let started = std::time::Instant::now();
                    let mut complement = RoaringTreemap::new();
                    complement.insert_range(0..total_rows);
                    for m in &matching {
                        complement.remove(*m as u64);
                    }
                    let complement = DeleteVector::new(complement);
                    let selection = ArrowReader::build_deletes_row_selection(
                        &row_groups,
                        &None,
                        &complement,
                    )
                    .unwrap();
                    let elapsed = started.elapsed();
                    std::hint::black_box(selection.row_count());
                    elapsed
                });
                let runs_ms = median_duration(3, || {
                    let started = std::time::Instant::now();
                    let selection =
                        ArrowReader::index_matches_row_selection(&row_groups, &None, &matching);
                    let elapsed = started.elapsed();
                    std::hint::black_box(selection.row_count());
                    elapsed
                });
                println!(
                    "index startup: rows={n_rows} blob_bytes={} parsed_bytes={} terms={} \
                     shape={label} matches={} decode_ms={decode_ms:.2} \
                     selection_complement_ms={complement_ms:.2} selection_runs_ms={runs_ms:.2}",
                    bytes.len(),
                    index.heap_size_bytes(),
                    index.n_terms(),
                    matching.len(),
                );
            }
        }
    }

    fn build_test_row_group_meta(
        schema_descr: SchemaDescPtr,
        columns: Vec<ColumnChunkMetaData>,
        num_rows: i64,
        ordinal: i16,
    ) -> RowGroupMetaData {
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(num_rows)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .set_ordinal(ordinal)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr() -> SchemaDescPtr {
        use parquet::schema::types::Type as SchemaType;

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(
                    SchemaType::primitive_type_builder("a", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
                Arc::new(
                    SchemaType::primitive_type_builder("b", parquet::basic::Type::INT32)
                        .build()
                        .unwrap(),
                ),
            ])
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    /// Verifies that file splits respect byte ranges and only read specific row groups.
    #[tokio::test]
    async fn test_file_splits_respect_byte_ranges() {
        use arrow_array::Int32Array;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/multi_row_group.parquet");

        // Force each batch into its own row group for testing byte range filtering.
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (0..100).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (100..200).collect::<Vec<i32>>(),
        ))])
        .unwrap();
        let batch3 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(Int32Array::from(
            (200..300).collect::<Vec<i32>>(),
        ))])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(100)
            .build();

        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.write(&batch3).expect("Writing batch 3");
        writer.close().unwrap();

        // Read the file metadata to get row group byte positions
        let file = File::open(&file_path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        println!("File has {} row groups", metadata.num_row_groups());
        assert_eq!(metadata.num_row_groups(), 3, "Expected 3 row groups");

        // Get byte positions for each row group
        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);
        let row_group_2 = metadata.row_group(2);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg2_start = rg1_start + row_group_1.compressed_size() as u64;
        let file_end = rg2_start + row_group_2.compressed_size() as u64;

        println!(
            "Row group 0: {} rows, starts at byte {}, {} bytes compressed",
            row_group_0.num_rows(),
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: {} rows, starts at byte {}, {} bytes compressed",
            row_group_1.num_rows(),
            rg1_start,
            row_group_1.compressed_size()
        );
        println!(
            "Row group 2: {} rows, starts at byte {}, {} bytes compressed",
            row_group_2.num_rows(),
            rg2_start,
            row_group_2.compressed_size()
        );

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Task 1: read only the first row group
        let task1 = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&file_path).unwrap().len(),
            start: rg0_start,
            length: row_group_0.compressed_size() as u64,
            record_count: Some(100),
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        // Task 2: read the second and third row groups
        let task2 = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&file_path).unwrap().len(),
            start: rg1_start,
            length: file_end - rg1_start,
            record_count: Some(200),
            data_file_path: file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        let tasks1 = Box::pin(futures::stream::iter(vec![Ok(task1)])) as FileScanTaskStream;
        let result1 = reader
            .clone()
            .read(tasks1)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task1: usize = result1.iter().map(|b| b.num_rows()).sum();
        println!(
            "Task 1 (bytes {}-{}) returned {} rows",
            rg0_start,
            rg0_start + row_group_0.compressed_size() as u64,
            total_rows_task1
        );

        let tasks2 = Box::pin(futures::stream::iter(vec![Ok(task2)])) as FileScanTaskStream;
        let result2 = reader
            .read(tasks2)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let total_rows_task2: usize = result2.iter().map(|b| b.num_rows()).sum();
        println!("Task 2 (bytes {rg1_start}-{file_end}) returned {total_rows_task2} rows");

        assert_eq!(
            total_rows_task1, 100,
            "Task 1 should read only the first row group (100 rows), but got {total_rows_task1} rows"
        );

        assert_eq!(
            total_rows_task2, 200,
            "Task 2 should read only the second+third row groups (200 rows), but got {total_rows_task2} rows"
        );

        // Verify the actual data values are correct (not just the row count)
        if total_rows_task1 > 0 {
            let first_batch = &result1[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            let last_val = id_col.value(id_col.len() - 1);
            println!("Task 1 data range: {first_val} to {last_val}");

            assert_eq!(first_val, 0, "Task 1 should start with id=0");
            assert_eq!(last_val, 99, "Task 1 should end with id=99");
        }

        if total_rows_task2 > 0 {
            let first_batch = &result2[0];
            let id_col = first_batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let first_val = id_col.value(0);
            println!("Task 2 first value: {first_val}");

            assert_eq!(first_val, 100, "Task 2 should start with id=100, not id=0");
        }
    }

    /// Test schema evolution: reading old Parquet file (with only column 'a')
    /// using a newer table schema (with columns 'a' and 'b').
    /// This tests that:
    /// 1. get_arrow_projection_mask allows missing columns
    /// 2. RecordBatchTransformer adds missing column 'b' with NULL values
    #[tokio::test]
    async fn test_schema_evolution_add_column() {
        use arrow_array::{Array, Int32Array};

        // New table schema: columns 'a' and 'b' (b was added later, file only has 'a')
        let new_schema = Arc::new(
            Schema::builder()
                .with_schema_id(2)
                .with_fields(vec![
                    NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "b", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Create Arrow schema for old Parquet file (only has column 'a')
        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Write old Parquet file with only column 'a'
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let data_a = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_new(arrow_schema_old.clone(), vec![data_a]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{table_location}/old_file.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the old Parquet file using the NEW schema (with column 'b')
        let reader = ArrowReaderBuilder::new(file_io).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/old_file.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/old_file.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: new_schema.clone(),
                project_field_ids: vec![1, 2], // Request both columns 'a' and 'b'
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        // Should have 2 columns now
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        // Column 'a' should have the original data
        let col_a = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_a.values(), &[1, 2, 3]);

        // Column 'b' should be all NULLs (it didn't exist in the old file)
        let col_b = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(col_b.null_count(), 3);
        assert!(col_b.is_null(0));
        assert!(col_b.is_null(1));
        assert!(col_b.is_null(2));
    }

    /// Test for bug where position deletes in later row groups are not applied correctly.
    ///
    /// When a file has multiple row groups and a position delete targets a row in a later
    /// row group, the `build_deletes_row_selection` function had a bug where it would
    /// fail to increment `current_row_group_base_idx` when skipping row groups.
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 199 (last row in second row group)
    ///
    /// Expected behavior: Should return 199 rows (with id=200 deleted)
    /// Bug behavior: Returns 200 rows (delete is not applied)
    ///
    /// This bug was discovered while running Apache Spark + Apache Iceberg integration tests
    /// through DataFusion Comet. The following Iceberg Java tests failed due to this bug:
    /// - `org.apache.iceberg.spark.extensions.TestMergeOnReadDelete::testDeleteWithMultipleRowGroupsParquet`
    /// - `org.apache.iceberg.spark.extensions.TestMergeOnReadUpdate::testUpdateWithMultipleRowGroupsParquet`
    #[tokio::test]
    async fn test_position_delete_across_multiple_row_groups() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(100)
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 199 (id=200, last row in row group 1)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 199 (0-indexed, so it's the last row: id=200)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![199i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Read the data file with the delete applied
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: 0,
            length: 0,
            record_count: Some(200),
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 199 rows (not 200)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        println!("Total rows read: {total_rows}");
        println!("Expected: 199 rows (deleted row 199 which had id=200)");

        // This assertion will FAIL before the fix and PASS after the fix
        assert_eq!(
            total_rows, 199,
            "Expected 199 rows after deleting row 199, but got {total_rows} rows. \
             The bug causes position deletes in later row groups to be ignored."
        );

        // Verify the deleted row (id=200) is not present
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert!(
            !all_ids.contains(&200),
            "Row with id=200 should be deleted but was found in results"
        );

        // Verify we have all other ids (1-199)
        let expected_ids: Vec<i32> = (1..=199).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 1-199 but got different values"
        );
    }

    /// Test for bug where position deletes are lost when skipping unselected row groups.
    ///
    /// This is a variant of `test_position_delete_across_multiple_row_groups` that exercises
    /// the row group selection code path (`selected_row_groups: Some([...])`).
    ///
    /// When a file has multiple row groups and only some are selected for reading,
    /// the `build_deletes_row_selection` function must correctly skip over deletes in
    /// unselected row groups WITHOUT consuming deletes that belong to selected row groups.
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 199 (last row in second row group)
    /// - Row group selection that reads ONLY row group 1 (rows 100-199)
    ///
    /// Expected behavior: Should return 99 rows (with row 199 deleted)
    /// Bug behavior: Returns 100 rows (delete is lost when skipping row group 0)
    ///
    /// The bug occurs when processing row group 0 (unselected):
    /// ```rust
    /// delete_vector_iter.advance_to(next_row_group_base_idx); // Position at first delete >= 100
    /// next_deleted_row_idx_opt = delete_vector_iter.next(); // BUG: Consumes delete at 199!
    /// ```
    ///
    /// The fix is to NOT call `next()` after `advance_to()` when skipping unselected row groups,
    /// because `advance_to()` already positions the iterator correctly without consuming elements.
    #[tokio::test]
    async fn test_position_delete_with_row_group_selection() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(100)
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 199 (id=200, last row in row group 1)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 199 (0-indexed, so it's the last row: id=200)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![199i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Get byte ranges to read ONLY row group 1 (rows 100-199)
        // This exercises the row group selection code path where row group 0 is skipped
        let metadata_file = File::open(&data_file_path).unwrap();
        let metadata_reader = SerializedFileReader::new(metadata_file).unwrap();
        let metadata = metadata_reader.metadata();

        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg1_length = row_group_1.compressed_size() as u64;

        println!(
            "Row group 0: starts at byte {}, {} bytes compressed",
            rg0_start,
            row_group_0.compressed_size()
        );
        println!(
            "Row group 1: starts at byte {}, {} bytes compressed",
            rg1_start,
            row_group_1.compressed_size()
        );

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: rg1_start,
            length: rg1_length,
            record_count: Some(100), // Row group 1 has 100 rows
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 99 rows (not 100)
        // Row group 1 has 100 rows (ids 101-200), minus 1 delete (id=200) = 99 rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        println!("Total rows read from row group 1: {total_rows}");
        println!("Expected: 99 rows (row group 1 has 100 rows, 1 delete at position 199)");

        // This assertion will FAIL before the fix and PASS after the fix
        assert_eq!(
            total_rows, 99,
            "Expected 99 rows from row group 1 after deleting position 199, but got {total_rows} rows. \
             The bug causes position deletes to be lost when advance_to() is followed by next() \
             when skipping unselected row groups."
        );

        // Verify the deleted row (id=200) is not present
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        assert!(
            !all_ids.contains(&200),
            "Row with id=200 should be deleted but was found in results"
        );

        // Verify we have ids 101-199 (not 101-200)
        let expected_ids: Vec<i32> = (101..=199).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 101-199 but got different values"
        );
    }
    /// Test for bug where stale cached delete causes infinite loop when skipping row groups.
    ///
    /// This test exposes the inverse scenario of `test_position_delete_with_row_group_selection`:
    /// - Position delete targets a row in the SKIPPED row group (not the selected one)
    /// - After calling advance_to(), the cached delete index is stale
    /// - Without updating the cache, the code enters an infinite loop
    ///
    /// This test creates:
    /// - A data file with 200 rows split into 2 row groups (0-99, 100-199)
    /// - A position delete file that deletes row 0 (first row in SKIPPED row group 0)
    /// - Row group selection that reads ONLY row group 1 (rows 100-199)
    ///
    /// The bug occurs when skipping row group 0:
    /// ```rust
    /// let mut next_deleted_row_idx_opt = delete_vector_iter.next(); // Some(0)
    /// // ... skip to row group 1 ...
    /// delete_vector_iter.advance_to(100); // Iterator advances past delete at 0
    /// // BUG: next_deleted_row_idx_opt is still Some(0) - STALE!
    /// // When processing row group 1:
    /// //   current_idx = 100, next_deleted_row_idx = 0, next_row_group_base_idx = 200
    /// //   Loop condition: 0 < 200 (true)
    /// //   But: current_idx (100) > next_deleted_row_idx (0)
    /// //   And: current_idx (100) != next_deleted_row_idx (0)
    /// //   Neither branch executes -> INFINITE LOOP!
    /// ```
    ///
    /// Expected behavior: Should return 100 rows (delete at 0 doesn't affect row group 1)
    /// Bug behavior: Infinite loop in build_deletes_row_selection
    #[tokio::test]
    async fn test_position_delete_in_skipped_row_group() {
        use arrow_array::{Int32Array, Int64Array};
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Field IDs for positional delete schema
        const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
        const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();

        // Create table schema with a single 'id' column
        let table_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));

        // Step 1: Create data file with 200 rows in 2 row groups
        // Row group 0: rows 0-99 (ids 1-100)
        // Row group 1: rows 100-199 (ids 101-200)
        let data_file_path = format!("{table_location}/data.parquet");

        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=100),
        )])
        .unwrap();

        let batch2 = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(101..=200),
        )])
        .unwrap();

        // Force each batch into its own row group
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(100)
            .build();

        let file = File::create(&data_file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();
        writer.write(&batch1).expect("Writing batch 1");
        writer.write(&batch2).expect("Writing batch 2");
        writer.close().unwrap();

        // Verify we created 2 row groups
        let verify_file = File::open(&data_file_path).unwrap();
        let verify_reader = SerializedFileReader::new(verify_file).unwrap();
        assert_eq!(
            verify_reader.metadata().num_row_groups(),
            2,
            "Should have 2 row groups"
        );

        // Step 2: Create position delete file that deletes row 0 (id=1, first row in row group 0)
        let delete_file_path = format!("{table_location}/deletes.parquet");

        let delete_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
            )])),
            Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
            )])),
        ]));

        // Delete row at position 0 (0-indexed, so it's the first row: id=1)
        let delete_batch = RecordBatch::try_new(delete_schema.clone(), vec![
            Arc::new(StringArray::from_iter_values(vec![data_file_path.clone()])),
            Arc::new(Int64Array::from_iter_values(vec![0i64])),
        ])
        .unwrap();

        let delete_props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let delete_file = File::create(&delete_file_path).unwrap();
        let mut delete_writer =
            ArrowWriter::try_new(delete_file, delete_schema, Some(delete_props)).unwrap();
        delete_writer.write(&delete_batch).unwrap();
        delete_writer.close().unwrap();

        // Step 3: Get byte ranges to read ONLY row group 1 (rows 100-199)
        // This exercises the row group selection code path where row group 0 is skipped
        let metadata_file = File::open(&data_file_path).unwrap();
        let metadata_reader = SerializedFileReader::new(metadata_file).unwrap();
        let metadata = metadata_reader.metadata();

        let row_group_0 = metadata.row_group(0);
        let row_group_1 = metadata.row_group(1);

        let rg0_start = 4u64; // Parquet files start with 4-byte magic "PAR1"
        let rg1_start = rg0_start + row_group_0.compressed_size() as u64;
        let rg1_length = row_group_1.compressed_size() as u64;

        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        // Create FileScanTask that reads ONLY row group 1 via byte range filtering
        let task = FileScanTask {
            file_size_in_bytes: std::fs::metadata(&data_file_path).unwrap().len(),
            start: rg1_start,
            length: rg1_length,
            record_count: Some(100), // Row group 1 has 100 rows
            data_file_path: data_file_path.clone(),
            data_file_format: DataFileFormat::Parquet,
            schema: table_schema.clone(),
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![FileScanTaskDeleteFile {
                file_size_in_bytes: std::fs::metadata(&delete_file_path).unwrap().len(),
                file_path: delete_file_path,
                file_type: DataContentType::PositionDeletes,
                partition_spec_id: 0,
                equality_ids: None,
            }],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Step 4: Verify we got 100 rows (all of row group 1)
        // The delete at position 0 is in row group 0, which is skipped, so it doesn't affect us
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            total_rows, 100,
            "Expected 100 rows from row group 1 (delete at position 0 is in skipped row group 0). \
             If this hangs or fails, it indicates the cached delete index was not updated after advance_to()."
        );

        // Verify we have all ids from row group 1 (101-200)
        let all_ids: Vec<i32> = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();

        let expected_ids: Vec<i32> = (101..=200).collect();
        assert_eq!(
            all_ids, expected_ids,
            "Should have ids 101-200 (all of row group 1)"
        );
    }

    /// Test reading Parquet files without field ID metadata (e.g., migrated tables).
    /// This exercises the position-based fallback path.
    ///
    /// Corresponds to Java's ParquetSchemaUtil.addFallbackIds() + pruneColumnsFallback()
    /// in /parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
    #[tokio::test]
    async fn test_read_parquet_file_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Parquet file from a migrated table - no field ID metadata
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = vec!["Alice", "Bob", "Charlie"];
        let age_data = vec![30, 25, 35];

        use arrow_array::Int32Array;
        let name_col = Arc::new(StringArray::from(name_data.clone())) as ArrayRef;
        let age_col = Arc::new(Int32Array::from(age_data.clone())) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![name_col, age_col]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        // Verify position-based mapping: field_id 1 → position 0, field_id 2 → position 1
        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
        assert_eq!(age_array.value(2), 35);
    }

    /// Test reading Parquet files without field IDs with partial projection.
    /// Only a subset of columns are requested, verifying position-based fallback
    /// handles column selection correctly.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_partial_projection() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "col1", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "col2", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(3, "col3", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(4, "col4", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Utf8, false),
            Field::new("col4", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col1_data = Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef;
        let col2_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let col3_data = Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef;
        let col4_data = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;

        let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
            col1_data, col2_data, col3_data, col4_data,
        ])
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 3],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let col1_array = batch.column(0).as_string::<i32>();
        assert_eq!(col1_array.value(0), "a");
        assert_eq!(col1_array.value(1), "b");

        let col3_array = batch.column(1).as_string::<i32>();
        assert_eq!(col3_array.value(0), "c");
        assert_eq!(col3_array.value(1), "d");
    }

    /// Test reading Parquet files without field IDs with schema evolution.
    /// The Iceberg schema has more fields than the Parquet file, testing that
    /// missing columns are filled with NULLs.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution() {
        use arrow_array::{Array, Int32Array};

        // Schema with field 3 added after the file was written
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "age", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "city", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![name_data, age_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2, 3],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let name_array = batch.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);

        // Verify missing column filled with NULLs
        let city_array = batch.column(2).as_string::<i32>();
        assert_eq!(city_array.null_count(), 2);
        assert!(city_array.is_null(0));
        assert!(city_array.is_null(1));
    }

    /// Test reading Parquet files without field IDs that have multiple row groups.
    /// This ensures the position-based fallback works correctly across row group boundaries.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_multiple_row_groups() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "value", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Small row group size to create multiple row groups
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_write_batch_size(2)
            .set_max_row_group_size(2)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), Some(props)).unwrap();

        // Write 6 rows in 3 batches (will create 3 row groups)
        for batch_num in 0..3 {
            let name_data = Arc::new(StringArray::from(vec![
                format!("name_{}", batch_num * 2),
                format!("name_{}", batch_num * 2 + 1),
            ])) as ArrayRef;
            let value_data =
                Arc::new(Int32Array::from(vec![batch_num * 2, batch_num * 2 + 1])) as ArrayRef;

            let batch =
                RecordBatch::try_new(arrow_schema.clone(), vec![name_data, value_data]).unwrap();
            writer.write(&batch).expect("Writing batch");
        }
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert!(!result.is_empty());

        let mut all_names = Vec::new();
        let mut all_values = Vec::new();

        for batch in &result {
            let name_array = batch.column(0).as_string::<i32>();
            let value_array = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_names.push(name_array.value(i).to_string());
                all_values.push(value_array.value(i));
            }
        }

        assert_eq!(all_names.len(), 6);
        assert_eq!(all_values.len(), 6);

        for i in 0..6 {
            assert_eq!(all_names[i], format!("name_{i}"));
            assert_eq!(all_values[i], i as i32);
        }
    }

    /// Test reading Parquet files without field IDs with nested types (struct).
    /// Java's pruneColumnsFallback() projects entire top-level columns including nested content.
    /// This test verifies that a top-level struct field is projected correctly with all its nested fields.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_with_struct() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::Fields;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(
                        2,
                        "person",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::required(
                                3,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(4, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "person",
                DataType::Struct(Fields::from(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                ])),
                false,
            ),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef;
        let age_data = Arc::new(Int32Array::from(vec![30, 25])) as ArrayRef;
        let person_data = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                name_data,
            ),
            (
                Arc::new(Field::new("age", DataType::Int32, false)),
                age_data,
            ),
        ])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, person_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();

        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let id_array = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let person_array = batch.column(1).as_struct();
        assert_eq!(person_array.num_columns(), 2);

        let name_array = person_array.column(0).as_string::<i32>();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let age_array = person_array
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(age_array.value(0), 30);
        assert_eq!(age_array.value(1), 25);
    }

    /// Test reading Parquet files without field IDs with schema evolution - column added in the middle.
    /// When a new column is inserted between existing columns in the schema order,
    /// the fallback projection must correctly map field IDs to output positions.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_schema_evolution_add_column_in_middle() {
        use arrow_array::{Array, Int32Array};

        let arrow_schema_old = Arc::new(ArrowSchema::new(vec![
            Field::new("col0", DataType::Int32, true),
            Field::new("col1", DataType::Int32, true),
        ]));

        // New column added between existing columns: col0 (id=1), newCol (id=5), col1 (id=2)
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "col0", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(5, "newCol", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "col1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let col0_data = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let col1_data = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema_old.clone(), vec![col0_data, col1_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::new(file_io).build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 5, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let result_col0 = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col0.value(0), 1);
        assert_eq!(result_col0.value(1), 2);

        // New column should be NULL (doesn't exist in old file)
        let result_newcol = batch
            .column(1)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_newcol.null_count(), 2);
        assert!(result_newcol.is_null(0));
        assert!(result_newcol.is_null(1));

        let result_col1 = batch
            .column(2)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(result_col1.value(0), 10);
        assert_eq!(result_col1.value(1), 20);
    }

    /// Test reading Parquet files without field IDs with a filter that eliminates all row groups.
    /// During development of field ID mapping, we saw a panic when row_selection_enabled=true and
    /// all row groups are filtered out.
    #[tokio::test]
    async fn test_read_parquet_without_field_ids_filter_eliminates_all_rows() {
        use arrow_array::{Float64Array, Int32Array};

        // Schema with fields that will use fallback IDs 1, 2, 3
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "value", Type::Primitive(PrimitiveType::Double))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Write data where all ids are >= 10
        let id_data = Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef;
        let name_data = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
        let value_data = Arc::new(Float64Array::from(vec![100.0, 200.0, 300.0])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data, value_data])
                .unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let file = File::create(format!("{table_location}/1.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Filter that eliminates all row groups: id < 5
        let predicate = Reference::new("id").less_than(Datum::int(5));

        // Enable both row_group_filtering and row_selection - triggered the panic
        let reader = ArrowReaderBuilder::new(file_io)
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2, 3],
                predicate: Some(predicate.bind(schema, true).unwrap()),
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        // Should no longer panic
        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Should return empty results
        assert!(result.is_empty() || result.iter().all(|batch| batch.num_rows() == 0));
    }

    /// Test that concurrency=1 reads all files correctly and in deterministic order.
    /// This verifies the fast-path optimization for single concurrency.
    #[tokio::test]
    async fn test_read_with_concurrency_one() {
        use arrow_array::Int32Array;

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "file_num", Type::Primitive(PrimitiveType::Int))
                        .into(),
                ])
                .build()
                .unwrap(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("file_num", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        // Create 3 parquet files with different data
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for file_num in 0..3 {
            let id_data = Arc::new(Int32Array::from_iter_values(
                file_num * 10..(file_num + 1) * 10,
            )) as ArrayRef;
            let file_num_data = Arc::new(Int32Array::from(vec![file_num; 10])) as ArrayRef;

            let to_write =
                RecordBatch::try_new(arrow_schema.clone(), vec![id_data, file_num_data]).unwrap();

            let file = File::create(format!("{table_location}/file_{file_num}.parquet")).unwrap();
            let mut writer =
                ArrowWriter::try_new(file, to_write.schema(), Some(props.clone())).unwrap();
            writer.write(&to_write).expect("Writing batch");
            writer.close().unwrap();
        }

        // Read with concurrency=1 (fast-path)
        let reader = ArrowReaderBuilder::new(file_io)
            .with_data_file_concurrency_limit(1)
            .build();

        // Create tasks in a specific order: file_0, file_1, file_2
        let tasks = vec![
            Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/file_0.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/file_0.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            }),
            Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/file_1.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/file_1.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            }),
            Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/file_2.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/file_2.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            }),
        ];

        let tasks_stream = Box::pin(futures::stream::iter(tasks)) as FileScanTaskStream;

        let result = reader
            .read(tasks_stream)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got all 30 rows (10 from each file)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 30, "Should have 30 total rows");

        // Collect all ids and file_nums to verify data
        let mut all_ids = Vec::new();
        let mut all_file_nums = Vec::new();

        for batch in &result {
            let id_col = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            let file_num_col = batch
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>();

            for i in 0..batch.num_rows() {
                all_ids.push(id_col.value(i));
                all_file_nums.push(file_num_col.value(i));
            }
        }

        assert_eq!(all_ids.len(), 30);
        assert_eq!(all_file_nums.len(), 30);

        // With concurrency=1 and sequential processing, files should be processed in order
        // file_0: ids 0-9, file_num=0
        // file_1: ids 10-19, file_num=1
        // file_2: ids 20-29, file_num=2
        for i in 0..10 {
            assert_eq!(all_file_nums[i], 0, "First 10 rows should be from file_0");
            assert_eq!(all_ids[i], i as i32, "IDs should be 0-9");
        }
        for i in 10..20 {
            assert_eq!(all_file_nums[i], 1, "Next 10 rows should be from file_1");
            assert_eq!(all_ids[i], i as i32, "IDs should be 10-19");
        }
        for i in 20..30 {
            assert_eq!(all_file_nums[i], 2, "Last 10 rows should be from file_2");
            assert_eq!(all_ids[i], i as i32, "IDs should be 20-29");
        }
    }

    /// Test bucket partitioning reads source column from data file (not partition metadata).
    ///
    /// This is an integration test verifying the complete ArrowReader pipeline with bucket partitioning.
    /// It corresponds to TestRuntimeFiltering tests in Iceberg Java (e.g., testRenamedSourceColumnTable).
    ///
    /// # Iceberg Spec Requirements
    ///
    /// Per the Iceberg spec "Column Projection" section:
    /// > "Return the value from partition metadata if an **Identity Transform** exists for the field"
    ///
    /// This means:
    /// - Identity transforms (e.g., `identity(dept)`) use constants from partition metadata
    /// - Non-identity transforms (e.g., `bucket(4, id)`) must read source columns from data files
    /// - Partition metadata for bucket transforms stores bucket numbers (0-3), NOT source values
    ///
    /// Java's PartitionUtil.constantsMap() implements this via:
    /// ```java
    /// if (field.transform().isIdentity()) {
    ///     idToConstant.put(field.sourceId(), converted);
    /// }
    /// ```
    ///
    /// # What This Test Verifies
    ///
    /// This test ensures the full ArrowReader → RecordBatchTransformer pipeline correctly handles
    /// bucket partitioning when FileScanTask provides partition_spec and partition_data:
    ///
    /// - Parquet file has field_id=1 named "id" with actual data [1, 5, 9, 13]
    /// - FileScanTask specifies partition_spec with bucket(4, id) and partition_data with bucket=1
    /// - RecordBatchTransformer.constants_map() excludes bucket-partitioned field from constants
    /// - ArrowReader correctly reads [1, 5, 9, 13] from the data file
    /// - Values are NOT replaced with constant 1 from partition metadata
    ///
    /// # Why This Matters
    ///
    /// Without correct handling:
    /// - Runtime filtering would break (e.g., `WHERE id = 5` would fail)
    /// - Query results would be incorrect (all rows would have id=1)
    /// - Bucket partitioning would be unusable for query optimization
    ///
    /// # References
    /// - Iceberg spec: format/spec.md "Column Projection" + "Partition Transforms"
    /// - Java test: spark/src/test/java/.../TestRuntimeFiltering.java
    /// - Java impl: core/src/main/java/org/apache/iceberg/util/PartitionUtil.java
    #[tokio::test]
    async fn test_bucket_partitioning_reads_source_column_from_file() {
        use arrow_array::Int32Array;

        use crate::spec::{Literal, PartitionSpec, Struct, Transform};

        // Iceberg schema with id and name columns
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );

        // Partition spec: bucket(4, id)
        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("id", "id_bucket", Transform::Bucket(4))
                .unwrap()
                .build()
                .unwrap(),
        );

        // Partition data: bucket value is 1
        let partition_data = Struct::from_iter(vec![Some(Literal::int(1))]);

        // Create Arrow schema with field IDs for Parquet file
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Write Parquet file with data
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_io = FileIO::new_with_fs();

        let id_data = Arc::new(Int32Array::from(vec![1, 5, 9, 13])) as ArrayRef;
        let name_data =
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef;

        let to_write =
            RecordBatch::try_new(arrow_schema.clone(), vec![id_data, name_data]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(format!("{}/data.parquet", &table_location)).unwrap();
        let mut writer = ArrowWriter::try_new(file, to_write.schema(), Some(props)).unwrap();
        writer.write(&to_write).expect("Writing batch");
        writer.close().unwrap();

        // Read the Parquet file with partition spec and data
        let reader = ArrowReaderBuilder::new(file_io).build();
        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(format!("{table_location}/data.parquet"))
                    .unwrap()
                    .len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: format!("{table_location}/data.parquet"),
                data_file_format: DataFileFormat::Parquet,
                schema: schema.clone(),
                project_field_ids: vec![1, 2],
                predicate: None,
                deletes: vec![],
                partition: Some(partition_data),
                partition_spec: Some(partition_spec),
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        // Verify we got the correct data
        assert_eq!(result.len(), 1);
        let batch = &result[0];

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 4);

        // The id column MUST contain actual values from the Parquet file [1, 5, 9, 13],
        // NOT the constant partition value 1
        let id_col = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int32Type>();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 5);
        assert_eq!(id_col.value(2), 9);
        assert_eq!(id_col.value(3), 13);

        let name_col = batch.column(1).as_string::<i32>();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
        assert_eq!(name_col.value(3), "Dave");
    }

    #[test]
    fn test_merge_ranges_empty() {
        assert_eq!(super::merge_ranges(&[], 1024), Vec::<Range<u64>>::new());
    }

    #[test]
    fn test_merge_ranges_no_coalesce() {
        // Ranges far apart should not be merged
        let ranges = vec![0..100, 1_000_000..1_000_100];
        let merged = super::merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..100, 1_000_000..1_000_100]);
    }

    #[test]
    fn test_merge_ranges_coalesce() {
        // Ranges within the gap threshold should be merged
        let ranges = vec![0..100, 200..300, 500..600];
        let merged = super::merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    #[test]
    fn test_merge_ranges_overlapping() {
        let ranges = vec![0..200, 100..300];
        let merged = super::merge_ranges(&ranges, 0);
        assert_eq!(merged, vec![0..300]);
    }

    #[test]
    fn test_merge_ranges_unsorted() {
        let ranges = vec![500..600, 0..100, 200..300];
        let merged = super::merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    /// Mock FileRead backed by a flat byte buffer.
    struct MockFileRead {
        data: bytes::Bytes,
    }

    impl MockFileRead {
        fn new(size: usize) -> Self {
            // Fill with sequential byte values so slices are verifiable.
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            Self {
                data: bytes::Bytes::from(data),
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::io::FileRead for MockFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<bytes::Bytes> {
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_get_byte_ranges_no_coalesce() {
        use parquet::arrow::async_reader::AsyncFileReader;

        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 2048 }, Box::new(mock))
                .with_parquet_read_options(
                    super::ParquetReadOptions::builder()
                        .with_range_coalesce_bytes(0)
                        .build(),
                );

        let result = reader
            .get_byte_ranges(vec![0..100, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_with_coalesce() {
        use parquet::arrow::async_reader::AsyncFileReader;

        let mock = MockFileRead::new(1024);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(200..300);
        let expected_2 = mock.data.slice(500..600);

        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 1024 }, Box::new(mock))
                .with_parquet_read_options(
                    super::ParquetReadOptions::builder()
                        .with_range_coalesce_bytes(1024)
                        .build(),
                );

        // All ranges within coalesce threshold — should merge into one fetch.
        let result = reader
            .get_byte_ranges(vec![0..100, 200..300, 500..600])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
        assert_eq!(result[2], expected_2);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_empty() {
        use parquet::arrow::async_reader::AsyncFileReader;

        let mock = MockFileRead::new(1024);
        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 1024 }, Box::new(mock));

        let result = reader.get_byte_ranges(vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_get_byte_ranges_coalesce_max() {
        use parquet::arrow::async_reader::AsyncFileReader;

        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 2048 }, Box::new(mock))
                .with_parquet_read_options(
                    super::ParquetReadOptions::builder()
                        .with_range_coalesce_bytes(u64::MAX)
                        .build(),
                );

        // u64::MAX coalesce — all ranges merge into a single fetch.
        let result = reader
            .get_byte_ranges(vec![0..100, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_concurrency_zero() {
        use parquet::arrow::async_reader::AsyncFileReader;

        // concurrency=0 is clamped to 1, so this should not hang.
        let mock = MockFileRead::new(1024);
        let expected = mock.data.slice(0..100);

        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 1024 }, Box::new(mock))
                .with_parquet_read_options(
                    super::ParquetReadOptions::builder()
                        .with_range_fetch_concurrency(0)
                        .build(),
                );

        let result = reader
            .get_byte_ranges(vec![0..100, 200..300])
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], expected);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_concurrency_one() {
        use parquet::arrow::async_reader::AsyncFileReader;

        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(500..600);
        let expected_2 = mock.data.slice(1500..1600);

        let mut reader =
            super::ArrowFileReader::new(crate::io::FileMetadata { size: 2048 }, Box::new(mock))
                .with_parquet_read_options(
                    super::ParquetReadOptions::builder()
                        .with_range_coalesce_bytes(0)
                        .with_range_fetch_concurrency(1)
                        .build(),
                );

        // concurrency=1 with no coalescing — sequential fetches.
        let result = reader
            .get_byte_ranges(vec![0..100, 500..600, 1500..1600])
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], expected_0);
        assert_eq!(result[1], expected_1);
        assert_eq!(result[2], expected_2);
    }

    /// Regression for <https://github.com/apache/iceberg-rust/issues/2306>:
    /// predicate on a column after nested types in a migrated file (no field IDs).
    /// Schema has struct, list, and map columns before the predicate target (`id`),
    /// exercising the fallback field ID mapping across all nested type variants.
    #[tokio::test]
    async fn test_predicate_on_migrated_file_with_nested_types() {
        use serde::{Deserialize, Serialize};
        use serde_arrow::schema::{SchemaLike, TracingOptions};

        #[derive(Serialize, Deserialize)]
        struct Person {
            name: String,
            age: i32,
        }

        #[derive(Serialize, Deserialize)]
        struct Row {
            person: Person,
            people: Vec<Person>,
            props: std::collections::BTreeMap<String, String>,
            id: i32,
        }

        let rows = vec![
            Row {
                person: Person {
                    name: "Alice".into(),
                    age: 30,
                },
                people: vec![Person {
                    name: "Alice".into(),
                    age: 30,
                }],
                props: [("k1".into(), "v1".into())].into(),
                id: 1,
            },
            Row {
                person: Person {
                    name: "Bob".into(),
                    age: 25,
                },
                people: vec![Person {
                    name: "Bob".into(),
                    age: 25,
                }],
                props: [("k2".into(), "v2".into())].into(),
                id: 2,
            },
            Row {
                person: Person {
                    name: "Carol".into(),
                    age: 40,
                },
                people: vec![Person {
                    name: "Carol".into(),
                    age: 40,
                }],
                props: [("k3".into(), "v3".into())].into(),
                id: 3,
            },
        ];

        let tracing_options = TracingOptions::default()
            .map_as_struct(false)
            .strings_as_large_utf8(false)
            .sequence_as_large_list(false);
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<Row>(tracing_options).unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(fields.clone()));
        let batch = serde_arrow::to_record_batch(&fields, &rows).unwrap();

        // Fallback field IDs: person=1, people=2, props=3, id=4
        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "person",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::required(
                                5,
                                "name",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            NestedField::required(6, "age", Type::Primitive(PrimitiveType::Int))
                                .into(),
                        ])),
                    )
                    .into(),
                    NestedField::required(
                        2,
                        "people",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::required(
                                7,
                                "element",
                                Type::Struct(crate::spec::StructType::new(vec![
                                    NestedField::required(
                                        8,
                                        "name",
                                        Type::Primitive(PrimitiveType::String),
                                    )
                                    .into(),
                                    NestedField::required(
                                        9,
                                        "age",
                                        Type::Primitive(PrimitiveType::Int),
                                    )
                                    .into(),
                                ])),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(
                        3,
                        "props",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                10,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::required(
                                11,
                                "value",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                    NestedField::required(4, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/1.parquet");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let file = File::create(&file_path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        writer.close().unwrap();

        let predicate = Reference::new("id").greater_than(Datum::int(1));

        let reader = ArrowReaderBuilder::new(FileIO::new_with_fs())
            .with_row_group_filtering_enabled(true)
            .with_row_selection_enabled(true)
            .build();

        let tasks = Box::pin(futures::stream::iter(
            vec![Ok(FileScanTask {
                file_size_in_bytes: std::fs::metadata(&file_path).unwrap().len(),
                start: 0,
                length: 0,
                record_count: None,
                data_file_path: file_path,
                data_file_format: DataFileFormat::Parquet,
                schema: iceberg_schema.clone(),
                project_field_ids: vec![4],
                predicate: Some(predicate.bind(iceberg_schema, true).unwrap()),
                deletes: vec![],
                partition: None,
                partition_spec: None,
                name_mapping: None,
                case_sensitive: false,
                statistics_blobs: vec![],
            })]
            .into_iter(),
        )) as FileScanTaskStream;

        let result = reader
            .read(tasks)
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();

        let ids: Vec<i32> = result
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    // INT96 encoding: [nanos_low_u32, nanos_high_u32, julian_day_u32]
    // Julian day 2_440_588 = Unix epoch (1970-01-01)
    const UNIX_EPOCH_JULIAN: i64 = 2_440_588;
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    // Noon on 3333-01-01 (Julian day 2_953_529) — outside the i64 nanosecond range (~1677-2262).
    const INT96_TEST_NANOS_WITHIN_DAY: u64 = 43_200_000_000_000;
    const INT96_TEST_JULIAN_DAY: u32 = 2_953_529;

    fn make_int96_test_value() -> (parquet::data_type::Int96, i64) {
        let mut val = parquet::data_type::Int96::new();
        val.set_data(
            (INT96_TEST_NANOS_WITHIN_DAY & 0xFFFFFFFF) as u32,
            (INT96_TEST_NANOS_WITHIN_DAY >> 32) as u32,
            INT96_TEST_JULIAN_DAY,
        );
        let expected_micros = (INT96_TEST_JULIAN_DAY as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
            + (INT96_TEST_NANOS_WITHIN_DAY / 1_000) as i64;
        (val, expected_micros)
    }

    async fn read_int96_batches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
    ) -> Vec<RecordBatch> {
        let file_io = FileIO::new_with_fs();
        let reader = ArrowReaderBuilder::new(file_io).build();

        let file_size = std::fs::metadata(file_path).unwrap().len();
        let task = FileScanTask {
            file_size_in_bytes: file_size,
            start: 0,
            length: file_size,
            record_count: None,
            data_file_path: file_path.to_string(),
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids,
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
                statistics_blobs: vec![],
        };

        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        reader.read(tasks).unwrap().try_collect().await.unwrap()
    }

    // ArrowWriter cannot write INT96, so we use SerializedFileWriter directly.
    fn write_int96_parquet_file(
        table_location: &str,
        filename: &str,
        with_field_ids: bool,
    ) -> (String, Vec<i64>) {
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{Int32Type, Int96, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let file_path = format!("{table_location}/{filename}");

        let mut ts_builder = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL);
        let mut id_builder = SchemaType::primitive_type_builder("id", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED);

        if with_field_ids {
            ts_builder = ts_builder.with_id(Some(1));
            id_builder = id_builder.with_id(Some(2));
        }

        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![
                Arc::new(ts_builder.build().unwrap()),
                Arc::new(id_builder.build().unwrap()),
            ])
            .build()
            .unwrap();

        // Dates outside the i64 nanosecond range (~1677-2262) overflow without coercion.
        const NOON_NANOS: u64 = INT96_TEST_NANOS_WITHIN_DAY;
        const JULIAN_3333: u32 = INT96_TEST_JULIAN_DAY;
        const JULIAN_2100: u32 = 2_488_070;

        let test_data: Vec<(u32, u32, u32, i64)> = vec![
            // 3333-01-01 00:00:00
            (
                0,
                0,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
            // 3333-01-01 12:00:00
            (
                (NOON_NANOS & 0xFFFFFFFF) as u32,
                (NOON_NANOS >> 32) as u32,
                JULIAN_3333,
                (JULIAN_3333 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY
                    + (NOON_NANOS / 1_000) as i64,
            ),
            // 2100-01-01 00:00:00
            (
                0,
                0,
                JULIAN_2100,
                (JULIAN_2100 as i64 - UNIX_EPOCH_JULIAN) * MICROS_PER_DAY,
            ),
        ];

        let int96_values: Vec<Int96> = test_data
            .iter()
            .map(|(lo, hi, day, _)| {
                let mut v = Int96::new();
                v.set_data(*lo, *hi, *day);
                v
            })
            .collect();

        let id_values: Vec<i32> = (0..test_data.len() as i32).collect();
        let expected_micros: Vec<i64> = test_data.iter().map(|(_, _, _, m)| *m).collect();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(schema), Default::default()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        {
            // def=1: ts is OPTIONAL and present. No repetition levels (top-level columns).
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&int96_values, Some(&vec![1; test_data.len()]), None)
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int32Type>()
                .write_batch(&id_values, None, None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        (file_path, expected_micros)
    }

    async fn assert_int96_read_matches(
        file_path: &str,
        schema: SchemaRef,
        project_field_ids: Vec<i32>,
        expected_micros: &[i64],
    ) {
        use arrow_array::TimestampMicrosecondArray;

        let batches = read_int96_batches(file_path, schema, project_field_ids).await;

        assert_eq!(batches.len(), 1);
        let ts_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray");

        for (i, expected) in expected_micros.iter().enumerate() {
            assert_eq!(
                ts_array.value(i),
                *expected,
                "Row {i}: got {}, expected {expected}",
                ts_array.value(i)
            );
        }
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_with_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "with_ids.parquet", true);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_without_field_ids() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamp))
                        .into(),
                    NestedField::required(2, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let (file_path, expected_micros) =
            write_int96_parquet_file(&table_location, "no_ids.parquet", false);

        assert_int96_read_matches(&file_path, schema, vec![1, 2], &expected_micros).await;
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_struct() {
        use arrow_array::{StructArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/struct_int96.parquet");

        let ts_type = SchemaType::primitive_type_builder("ts", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let struct_type = SchemaType::group_type_builder("data")
            .with_repetition(Repetition::REQUIRED)
            .with_id(Some(1))
            .with_fields(vec![Arc::new(ts_type)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(struct_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // def=1: struct is REQUIRED so no level, ts is OPTIONAL and present (1).
        // No repetition levels needed (no repeated groups).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[1]), None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(
                        1,
                        "data",
                        Type::Struct(crate::spec::StructType::new(vec![
                            NestedField::optional(
                                2,
                                "ts",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        ])),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let struct_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Expected StructArray");
        let ts_array = struct_array
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside struct");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in struct: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_list() {
        use arrow_array::{ListArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::Int96Type;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/list_int96.parquet");

        // 3-level LIST encoding:
        //   optional group timestamps (LIST) {
        //     repeated group list {
        //       optional int96 element;
        //     }
        //   }
        let element_type = SchemaType::primitive_type_builder("element", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(2))
            .build()
            .unwrap();

        let list_group = SchemaType::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(element_type)])
            .build()
            .unwrap();

        let list_type = SchemaType::group_type_builder("timestamps")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::List))
            .with_fields(vec![Arc::new(list_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(list_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a list containing one INT96 element.
        // def=3: list present (1) + repeated group (2) + element present (3)
        // rep=0: start of a new list
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "timestamps",
                        Type::List(crate::spec::ListType {
                            element_field: NestedField::optional(
                                2,
                                "element",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let list_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Expected ListArray");
        let ts_array = list_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray inside list");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in list: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }

    #[tokio::test]
    async fn test_read_int96_timestamps_in_map() {
        use arrow_array::{MapArray, TimestampMicrosecondArray};
        use parquet::basic::{Repetition, Type as PhysicalType};
        use parquet::data_type::{ByteArrayType, Int96Type};
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as SchemaType;

        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().to_str().unwrap().to_string();
        let file_path = format!("{table_location}/map_int96.parquet");

        // MAP encoding:
        //   optional group ts_map (MAP) {
        //     repeated group key_value {
        //       required binary key (UTF8);
        //       optional int96 value;
        //     }
        //   }
        let key_type = SchemaType::primitive_type_builder("key", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_logical_type(Some(parquet::basic::LogicalType::String))
            .with_id(Some(2))
            .build()
            .unwrap();

        let value_type = SchemaType::primitive_type_builder("value", PhysicalType::INT96)
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(3))
            .build()
            .unwrap();

        let key_value_group = SchemaType::group_type_builder("key_value")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![Arc::new(key_type), Arc::new(value_type)])
            .build()
            .unwrap();

        let map_type = SchemaType::group_type_builder("ts_map")
            .with_repetition(Repetition::OPTIONAL)
            .with_id(Some(1))
            .with_logical_type(Some(parquet::basic::LogicalType::Map))
            .with_fields(vec![Arc::new(key_value_group)])
            .build()
            .unwrap();

        let parquet_schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(map_type)])
            .build()
            .unwrap();

        let (int96_val, expected_micros) = make_int96_test_value();

        let file = File::create(&file_path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, Arc::new(parquet_schema), Default::default()).unwrap();

        // Write a single row with a map containing one key-value pair.
        // rep=0 for both columns: start of a new map.
        // key def=2: map present (1) + key_value entry present (2), key is REQUIRED.
        // value def=3: map present (1) + key_value entry present (2) + value present (3).
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(
                    &[parquet::data_type::ByteArray::from("event_time")],
                    Some(&[2]),
                    Some(&[0]),
                )
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int96Type>()
                .write_batch(&[int96_val], Some(&[3]), Some(&[0]))
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();

        let iceberg_schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "ts_map",
                        Type::Map(crate::spec::MapType {
                            key_field: NestedField::required(
                                2,
                                "key",
                                Type::Primitive(PrimitiveType::String),
                            )
                            .into(),
                            value_field: NestedField::optional(
                                3,
                                "value",
                                Type::Primitive(PrimitiveType::Timestamp),
                            )
                            .into(),
                        }),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );

        let batches = read_int96_batches(&file_path, iceberg_schema, vec![1]).await;

        assert_eq!(batches.len(), 1);
        let map_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("Expected MapArray");
        let ts_array = map_array
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Expected TimestampMicrosecondArray as map values");

        assert_eq!(
            ts_array.value(0),
            expected_micros,
            "INT96 in map: got {}, expected {expected_micros}",
            ts_array.value(0)
        );
    }
}
