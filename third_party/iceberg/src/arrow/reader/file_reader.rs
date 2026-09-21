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

//! Async Parquet file reader that adapts an Iceberg `FileRead` to parquet's `AsyncFileReader`.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataOptions, ParquetMetaDataReader,
};

use super::ParquetReadOptions;
use crate::arrow::ScanMetrics;
use crate::io::read_observability::{
    ObjectStoreReadPhase, ReadDebouncer, record_object_store_reads,
};
use crate::io::{FileMetadata, FileRead};

struct FooterCache {
    order: VecDeque<String>,
    entries: HashMap<String, Arc<ParquetMetaData>>,
}

fn footer_cache() -> &'static Mutex<FooterCache> {
    static CACHE: OnceLock<Mutex<FooterCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(FooterCache {
            order: VecDeque::new(),
            entries: HashMap::new(),
        })
    })
}

pub(super) fn footer_debouncer() -> &'static ReadDebouncer<String, Arc<ParquetMetaData>> {
    static DEBOUNCER: OnceLock<ReadDebouncer<String, Arc<ParquetMetaData>>> = OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

fn footer_cache_max_entries() -> usize {
    std::env::var("SIGLAKE_ICEBERG_FOOTER_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(512)
}

pub(super) fn footer_cache_key(path: &str, options: ParquetReadOptions) -> String {
    format!(
        "{path}|column={}|offset={}|page={}",
        options.preload_column_index(),
        options.preload_offset_index(),
        options.preload_page_index()
    )
}

pub(crate) fn footer_cache_get(
    path: &str,
    options: ParquetReadOptions,
) -> Option<Arc<ParquetMetaData>> {
    if footer_cache_max_entries() == 0 {
        return None;
    }
    let key = footer_cache_key(path, options);
    let hit = footer_cache().lock().unwrap().entries.get(&key).cloned();
    metrics::counter!(
        "siglake_iceberg_footer_cache_total",
        "outcome" => if hit.is_some() { "hit" } else { "miss" }
    )
    .increment(1);
    hit
}

pub(crate) fn footer_cache_put(
    path: &str,
    options: ParquetReadOptions,
    metadata: Arc<ParquetMetaData>,
) {
    let max_entries = footer_cache_max_entries();
    if max_entries == 0 {
        return;
    }
    let key = footer_cache_key(path, options);
    let mut cache = footer_cache().lock().unwrap();
    if cache.entries.contains_key(&key) {
        return;
    }
    cache.entries.insert(key.clone(), metadata);
    cache.order.push_back(key);
    while cache.order.len() > max_entries {
        if let Some(evicted) = cache.order.pop_front() {
            cache.entries.remove(&evicted);
        }
    }
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
pub struct ArrowFileReader {
    meta: FileMetadata,
    parquet_read_options: ParquetReadOptions,
    r: Box<dyn FileRead>,
    scan_metrics: ScanMetrics,
    current_phase: ObjectStoreReadPhase,
}

impl ArrowFileReader {
    /// Create a new ArrowFileReader
    pub fn new(meta: FileMetadata, r: Box<dyn FileRead>) -> Self {
        Self {
            meta,
            parquet_read_options: ParquetReadOptions::builder().build(),
            r,
            scan_metrics: ScanMetrics::default(),
            current_phase: ObjectStoreReadPhase::Data,
        }
    }

    /// Configure all Parquet read options.
    pub(crate) fn with_parquet_read_options(mut self, options: ParquetReadOptions) -> Self {
        self.parquet_read_options = options;
        self
    }

    pub(crate) fn with_scan_metrics(mut self, scan_metrics: ScanMetrics) -> Self {
        self.scan_metrics = scan_metrics;
        self
    }

    fn record_read(&self, phase: ObjectStoreReadPhase, bytes: u64) {
        let counters = self.scan_metrics.counters();
        counters
            .object_store_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        counters.add_phase_bytes(phase, bytes);
        record_object_store_reads(phase, 1, bytes);
    }

    pub(super) async fn load_parquet_metadata(
        &mut self,
        decryption_properties: Option<Arc<FileDecryptionProperties>>,
        metadata_options: Option<ParquetMetaDataOptions>,
    ) -> parquet::errors::Result<Arc<ParquetMetaData>> {
        let mut reader = ParquetMetaDataReader::new()
            .with_prefetch_hint(self.parquet_read_options.metadata_size_hint())
            .with_page_index_policy(PageIndexPolicy::Skip)
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(PageIndexPolicy::Skip)
            .with_metadata_options(metadata_options)
            .with_decryption_properties(decryption_properties);

        self.current_phase = ObjectStoreReadPhase::Footer;
        let file_size = self.meta.size;
        reader.try_load(&mut *self, file_size).await?;

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
            reader.load_page_index(&mut *self).await?;
        }

        self.current_phase = ObjectStoreReadPhase::Data;
        reader.finish().map(Arc::new)
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let phase = self.current_phase;
        async move {
            let outcome = self
                .r
                .read_with_outcome(range.start..range.end)
                .await
                .map_err(|error| parquet::errors::ParquetError::External(Box::new(error)))?;
            if outcome.fetched {
                self.record_read(phase, outcome.bytes.len() as u64);
            }
            Ok(outcome.bytes)
        }
        .boxed()
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
            let r = &self.r;
            let scan_metrics = self.scan_metrics.clone();
            let phase = self.current_phase;

            // Fetch merged ranges concurrently.
            let fetched: Vec<Bytes> = futures::stream::iter(fetch_ranges.iter().cloned())
                .map(|range| {
                    let scan_metrics = scan_metrics.clone();
                    async move {
                        let outcome = r.read_with_outcome(range).await.map_err(|error| {
                            parquet::errors::ParquetError::External(Box::new(error))
                        })?;
                        if outcome.fetched {
                            let counters = scan_metrics.counters();
                            counters
                                .object_store_reads
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            counters.add_phase_bytes(phase, outcome.bytes.len() as u64);
                            record_object_store_reads(phase, 1, outcome.bytes.len() as u64);
                        }
                        Ok::<_, parquet::errors::ParquetError>(outcome.bytes)
                    }
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

    fn get_metadata(
        &mut self,
        options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let decryption_properties = options
            .and_then(|options| options.file_decryption_properties())
            .cloned();
        let metadata_options = options.map(|options| options.metadata_options().clone());
        async move {
            self.load_parquet_metadata(decryption_properties, metadata_options)
                .await
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

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use parquet::arrow::async_reader::AsyncFileReader;

    use super::{ArrowFileReader, ParquetReadOptions, merge_ranges};
    use crate::arrow::ScanMetrics;
    use crate::io::{FileMetadata, FileRead};

    #[test]
    fn test_merge_ranges_empty() {
        assert_eq!(merge_ranges(&[], 1024), Vec::<Range<u64>>::new());
    }

    #[test]
    fn test_merge_ranges_no_coalesce() {
        // Ranges far apart should not be merged
        let ranges = vec![0..100, 1_000_000..1_000_100];
        let merged = merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..100, 1_000_000..1_000_100]);
    }

    #[test]
    fn test_merge_ranges_coalesce() {
        // Ranges within the gap threshold should be merged
        let ranges = vec![0..100, 200..300, 500..600];
        let merged = merge_ranges(&ranges, 1024);
        assert_eq!(merged, vec![0..600]);
    }

    #[test]
    fn test_merge_ranges_overlapping() {
        let ranges = vec![0..200, 100..300];
        let merged = merge_ranges(&ranges, 0);
        assert_eq!(merged, vec![0..300]);
    }

    #[test]
    fn test_merge_ranges_unsorted() {
        let ranges = vec![500..600, 0..100, 200..300];
        let merged = merge_ranges(&ranges, 1024);
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
    impl FileRead for MockFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<bytes::Bytes> {
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    #[tokio::test]
    async fn test_get_byte_ranges_no_coalesce() {
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
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
    async fn coalesced_data_ranges_are_attributed_once_per_physical_fetch() {
        let metrics = ScanMetrics::default();
        let mut reader = ArrowFileReader::new(
            FileMetadata { size: 2_048 },
            Box::new(MockFileRead::new(2_048)),
        )
        .with_scan_metrics(metrics.clone())
        .with_parquet_read_options(
            ParquetReadOptions::builder()
                .with_range_coalesce_bytes(1_024)
                .build(),
        );

        let result = reader
            .get_byte_ranges(vec![0..100, 200..300])
            .await
            .unwrap();
        assert_eq!(result[0].len(), 100);
        assert_eq!(result[1].len(), 100);
        let counters = metrics.scan_counters();
        assert_eq!(
            counters
                .object_store_reads
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters
                .bytes_data
                .load(std::sync::atomic::Ordering::Relaxed),
            300
        );
        assert_eq!(metrics.bytes_read(), 300);
    }

    #[tokio::test]
    async fn test_get_byte_ranges_with_coalesce() {
        let mock = MockFileRead::new(1024);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(200..300);
        let expected_2 = mock.data.slice(500..600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
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
        let mock = MockFileRead::new(1024);
        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock));

        let result = reader.get_byte_ranges(vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_get_byte_ranges_coalesce_max() {
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
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
        // concurrency=0 is clamped to 1, so this should not hang.
        let mock = MockFileRead::new(1024);
        let expected = mock.data.slice(0..100);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 1024 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
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
        let mock = MockFileRead::new(2048);
        let expected_0 = mock.data.slice(0..100);
        let expected_1 = mock.data.slice(500..600);
        let expected_2 = mock.data.slice(1500..1600);

        let mut reader = ArrowFileReader::new(FileMetadata { size: 2048 }, Box::new(mock))
            .with_parquet_read_options(
                ParquetReadOptions::builder()
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
}
