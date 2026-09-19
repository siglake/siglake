//! Reverse row-group traversal, progressive tail chunks, and decoded sharing.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_select::take::take;
use futures::Stream;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

use crate::error::Result;
use crate::{Error, ErrorKind};

const DEFAULT_REVERSED_CHUNK_ROWS: usize = 32_768;
const DEFAULT_REVERSED_CHUNK_CACHE_MB: usize = 256;

pub(super) fn reversed_chunk_rows_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_REVERSED_CHUNK_ROWS)
}

pub(super) fn reversed_chunk_rows() -> usize {
    reversed_chunk_rows_from(std::env::var("SIGLAKE_REVERSED_CHUNK_ROWS").ok().as_deref())
}

fn reversed_chunk_cache_bytes_from(configured_mb: Option<&str>) -> usize {
    configured_mb
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_REVERSED_CHUNK_CACHE_MB)
        .saturating_mul(1024 * 1024)
}

pub(super) fn reversed_chunk_cache_max_bytes() -> usize {
    reversed_chunk_cache_bytes_from(
        std::env::var("SIGLAKE_REVERSED_CHUNK_CACHE_MB")
            .ok()
            .as_deref(),
    )
}

/// Split a flattened selection into complete segments in row-group order.
pub(super) fn split_selection_by_groups(
    selection: RowSelection,
    group_rows: &[usize],
) -> Vec<Vec<RowSelector>> {
    let total: usize = group_rows.iter().sum();
    let mut selectors: Vec<RowSelector> = selection.iter().copied().collect();
    let covered: usize = selectors.iter().map(|selector| selector.row_count).sum();
    if covered < total {
        selectors.push(RowSelector::skip(total - covered));
    }

    let mut segments = Vec::with_capacity(group_rows.len());
    let mut selectors = selectors.into_iter();
    let mut carry = None;
    for &rows in group_rows {
        let mut segment = Vec::new();
        let mut remaining = rows;
        while remaining > 0 {
            let selector = carry
                .take()
                .or_else(|| selectors.next())
                .unwrap_or_else(|| RowSelector::skip(remaining));
            if selector.row_count <= remaining {
                remaining -= selector.row_count;
                segment.push(selector);
            } else {
                let head = RowSelector {
                    row_count: remaining,
                    skip: selector.skip,
                };
                carry = Some(RowSelector {
                    row_count: selector.row_count - remaining,
                    skip: selector.skip,
                });
                segment.push(head);
                remaining = 0;
            }
        }
        segments.push(segment);
    }
    segments
}

/// Reorder a row selection to match reverse row-group traversal.
pub fn reverse_row_selection(selection: RowSelection, group_rows: &[usize]) -> RowSelection {
    let mut segments = split_selection_by_groups(selection, group_rows);
    segments.reverse();
    segments.into_iter().flatten().collect::<Vec<_>>().into()
}

/// Build tail-first chunks of selected rows, growing deeper chunks by 4x.
pub(super) fn tail_chunks_of_group_selection(
    segment: &[RowSelector],
    first_chunk_rows: usize,
    max_chunk_rows: usize,
) -> Vec<(Vec<RowSelector>, usize)> {
    let max_chunk_rows = max_chunk_rows.max(1);
    let mut chunk_rows = first_chunk_rows.clamp(1, max_chunk_rows);
    let mut runs = Vec::new();
    let mut position = 0;
    for selector in segment {
        if !selector.skip && selector.row_count > 0 {
            runs.push((position, selector.row_count));
        }
        position += selector.row_count;
    }

    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0;
    let flush = |current: &mut Vec<(usize, usize)>,
                 current_rows: &mut usize,
                 chunks: &mut Vec<(Vec<RowSelector>, usize)>| {
        if current.is_empty() {
            return;
        }
        let mut selectors = Vec::with_capacity(current.len() * 2);
        let mut at = 0;
        for &(start, len) in current.iter() {
            if start > at {
                selectors.push(RowSelector::skip(start - at));
            }
            selectors.push(RowSelector::select(len));
            at = start + len;
        }
        chunks.push((selectors, *current_rows));
        current.clear();
        *current_rows = 0;
    };

    for &(start, len) in runs.iter().rev() {
        let mut remaining = len;
        while remaining > 0 {
            let take = remaining.min(chunk_rows - current_rows);
            current.insert(0, (start + remaining - take, take));
            current_rows += take;
            remaining -= take;
            if current_rows == chunk_rows {
                flush(&mut current, &mut current_rows, &mut chunks);
                chunk_rows = chunk_rows.saturating_mul(4).min(max_chunk_rows);
            }
        }
    }
    flush(&mut current, &mut current_rows, &mut chunks);
    chunks
}

pub(super) struct ReversedGroupBatches<S> {
    inner: S,
    buffered: Vec<RecordBatch>,
    flush: VecDeque<RecordBatch>,
    done: bool,
}

impl<S> ReversedGroupBatches<S> {
    pub(super) fn new(inner: S) -> Self {
        Self {
            inner,
            buffered: Vec::new(),
            flush: VecDeque::new(),
            done: false,
        }
    }
}

impl<S> Stream for ReversedGroupBatches<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(batch) = self.flush.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if self.done {
                return Poll::Ready(None);
            }
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    for batch in std::mem::take(&mut self.buffered).into_iter().rev() {
                        match reverse_record_batch(batch) {
                            Ok(batch) => self.flush.push_back(batch),
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        }
                    }
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(batch))) => self.buffered.push(batch),
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
            take(column.as_ref(), &indices, None).map_err(|error| {
                Error::new(ErrorKind::Unexpected, "Failed to reverse record batch rows")
                    .with_source(error)
            })
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns?).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to rebuild reversed record batch",
        )
        .with_source(error)
    })
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct ReversedChunkKey {
    pub(super) path: String,
    pub(super) group: usize,
    pub(super) selectors: Vec<(bool, usize)>,
    pub(super) batch_size: Option<usize>,
    pub(super) field_ids: Vec<i32>,
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
        let size: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        if size > max {
            return;
        }
        if let Some((_, previous)) = self.entries.insert(key.clone(), (batches, size)) {
            self.bytes = self.bytes.saturating_sub(previous);
        }
        self.bytes = self.bytes.saturating_add(size);
        self.order.push_back(key);
        while self.bytes > max {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if !self.order.contains(&oldest)
                && let Some((_, evicted)) = self.entries.remove(&oldest)
            {
                self.bytes = self.bytes.saturating_sub(evicted);
            }
        }
    }
}

fn reversed_chunk_cache() -> &'static Mutex<ReversedChunkCache> {
    static CACHE: OnceLock<Mutex<ReversedChunkCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ReversedChunkCache::default()))
}

/// Ownership guard for a single-flight marker. Dropping any in-progress
/// leader wakes waiters after success, error, timeout, or cancellation.
pub(super) struct ReversedChunkLeader {
    key: Option<ReversedChunkKey>,
}

impl ReversedChunkLeader {
    fn new(key: ReversedChunkKey) -> Self {
        Self { key: Some(key) }
    }

    pub(super) fn publish(&self, batches: Vec<RecordBatch>) {
        if let Some(key) = self.key.clone() {
            reversed_chunk_cache().lock().unwrap().insert(key, batches);
        }
    }
}

impl Drop for ReversedChunkLeader {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        if let Ok(mut cache) = reversed_chunk_cache().lock()
            && let Some(notify) = cache.inflight.remove(&key)
        {
            notify.notify_waiters();
        }
    }
}

pub(super) enum ChunkCacheLookup {
    Hit(Vec<RecordBatch>),
    Leader(ReversedChunkLeader),
    TimedOut,
}

pub(super) async fn acquire_chunk(key: ReversedChunkKey) -> ChunkCacheLookup {
    acquire_chunk_with_timeout(key, std::time::Duration::from_secs(10)).await
}

async fn acquire_chunk_with_timeout(
    key: ReversedChunkKey,
    timeout: std::time::Duration,
) -> ChunkCacheLookup {
    loop {
        let waiter = {
            let mut cache = reversed_chunk_cache().lock().unwrap();
            if let Some((batches, _)) = cache.entries.get(&key) {
                return ChunkCacheLookup::Hit(batches.clone());
            }
            match cache.inflight.get(&key) {
                Some(notify) => notify.clone(),
                None => {
                    cache
                        .inflight
                        .insert(key.clone(), Arc::new(tokio::sync::Notify::new()));
                    return ChunkCacheLookup::Leader(ReversedChunkLeader::new(key));
                }
            }
        };
        if tokio::time::timeout(timeout, waiter.notified())
            .await
            .is_err()
        {
            return ChunkCacheLookup::TimedOut;
        }
    }
}

#[cfg(test)]
mod tests {
    use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

    use super::{ChunkCacheLookup, ReversedChunkKey, acquire_chunk_with_timeout};
    use super::{
        DEFAULT_REVERSED_CHUNK_CACHE_MB, DEFAULT_REVERSED_CHUNK_ROWS,
        reversed_chunk_cache_bytes_from, reversed_chunk_rows_from, split_selection_by_groups,
        tail_chunks_of_group_selection,
    };

    #[test]
    fn reverse_resolvers_preserve_defaults_zero_and_explicit_values() {
        assert_eq!(reversed_chunk_rows_from(None), DEFAULT_REVERSED_CHUNK_ROWS);
        assert_eq!(
            reversed_chunk_rows_from(Some("bad")),
            DEFAULT_REVERSED_CHUNK_ROWS
        );
        assert_eq!(reversed_chunk_rows_from(Some("0")), 0);
        assert_eq!(
            reversed_chunk_cache_bytes_from(None),
            DEFAULT_REVERSED_CHUNK_CACHE_MB * 1024 * 1024
        );
        assert_eq!(reversed_chunk_cache_bytes_from(Some("0")), 0);
    }

    #[test]
    fn split_and_progressive_chunks_keep_sparse_selected_rows_once() {
        let selection: RowSelection = vec![
            RowSelector::skip(2),
            RowSelector::select(4),
            RowSelector::skip(3),
            RowSelector::select(7),
        ]
        .into();
        let groups = split_selection_by_groups(selection, &[5, 6, 7]);
        let chunks: Vec<usize> = groups
            .iter()
            .rev()
            .flat_map(|group| tail_chunks_of_group_selection(group, 2, 8))
            .map(|(_, rows)| rows)
            .collect();
        assert_eq!(chunks, vec![2, 3, 2, 1, 2, 1]);
        assert_eq!(chunks.iter().sum::<usize>(), 11);
    }

    fn cache_key(path: &str) -> ReversedChunkKey {
        ReversedChunkKey {
            path: path.to_string(),
            group: 0,
            selectors: vec![(false, 1)],
            batch_size: Some(1),
            field_ids: vec![1],
        }
    }

    #[tokio::test]
    async fn single_flight_owner_drains_on_timeout_and_abandonment() {
        let key = cache_key("memory://reverse-owner-timeout-and-abandonment");
        let owner = match acquire_chunk_with_timeout(key.clone(), std::time::Duration::ZERO).await {
            ChunkCacheLookup::Leader(owner) => owner,
            _ => panic!("first caller must own the chunk"),
        };
        assert!(matches!(
            acquire_chunk_with_timeout(key.clone(), std::time::Duration::ZERO).await,
            ChunkCacheLookup::TimedOut
        ));
        drop(owner);
        assert!(matches!(
            acquire_chunk_with_timeout(key, std::time::Duration::ZERO).await,
            ChunkCacheLookup::Leader(_)
        ));
    }

    #[tokio::test]
    async fn published_single_flight_result_wakes_as_a_cache_hit() {
        let key = cache_key("memory://reverse-owner-success");
        let owner = match acquire_chunk_with_timeout(key.clone(), std::time::Duration::ZERO).await {
            ChunkCacheLookup::Leader(owner) => owner,
            _ => panic!("first caller must own the chunk"),
        };
        owner.publish(Vec::new());
        drop(owner);
        assert!(matches!(
            acquire_chunk_with_timeout(key, std::time::Duration::ZERO).await,
            ChunkCacheLookup::Hit(batches) if batches.is_empty()
        ));
    }
}
