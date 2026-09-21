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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::OnceCell;

use super::validate_puffin_compression;
use crate::{Error, ErrorKind, Result};
use crate::compression::CompressionCodec;
use crate::io::{FileRead, InputFile};
use crate::io::read_observability::{
    ObjectStoreReadPhase, ReadDebouncer, record_object_store_reads,
};
use crate::puffin::blob::Blob;
use crate::puffin::metadata::{BlobMetadata, FileMetadata};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BlobKey {
    path: String,
    offset: u64,
}

fn metadata_debouncer() -> &'static ReadDebouncer<String, Arc<FileMetadata>> {
    static DEBOUNCER: OnceLock<ReadDebouncer<String, Arc<FileMetadata>>> = OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

fn blob_debouncer() -> &'static ReadDebouncer<BlobKey, Arc<[u8]>> {
    static DEBOUNCER: OnceLock<ReadDebouncer<BlobKey, Arc<[u8]>>> = OnceLock::new();
    DEBOUNCER.get_or_init(ReadDebouncer::default)
}

struct MetadataCache {
    order: VecDeque<String>,
    entries: HashMap<String, Arc<FileMetadata>>,
}

fn metadata_cache() -> &'static Mutex<MetadataCache> {
    static CACHE: OnceLock<Mutex<MetadataCache>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(MetadataCache {
            order: VecDeque::new(),
            entries: HashMap::new(),
        })
    })
}

fn metadata_cache_max_entries() -> usize {
    std::env::var("SIGLAKE_PUFFIN_FOOTER_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(128)
}

fn metadata_cache_get(path: &str) -> Option<Arc<FileMetadata>> {
    (metadata_cache_max_entries() > 0)
        .then(|| metadata_cache().lock().unwrap().entries.get(path).cloned())
        .flatten()
}

fn metadata_cache_put(path: String, metadata: Arc<FileMetadata>) {
    let max_entries = metadata_cache_max_entries();
    if max_entries == 0 {
        return;
    }
    let mut cache = metadata_cache().lock().unwrap();
    if cache.entries.contains_key(&path) {
        return;
    }
    cache.entries.insert(path.clone(), metadata);
    cache.order.push_back(path);
    while cache.order.len() > max_entries {
        if let Some(evicted) = cache.order.pop_front() {
            cache.entries.remove(&evicted);
        }
    }
}

#[derive(Default)]
struct BlobCache {
    bytes: usize,
    order: VecDeque<BlobKey>,
    entries: HashMap<BlobKey, BlobCacheEntry>,
    admitted: u64,
}

struct BlobCacheEntry {
    bytes: Arc<[u8]>,
    active: u64,
}

impl BlobCache {
    fn put(
        &mut self,
        key: BlobKey,
        bytes: Arc<[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> bool {
        if max_entries == 0 || max_bytes == 0 || bytes.len() > max_bytes {
            return false;
        }
        if self.entries.contains_key(&key) {
            return true;
        }
        let parsed_twins = crate::arrow::parsed_index_puffin_twin_ranks();
        while self.entries.len() + 1 > max_entries || self.bytes + bytes.len() > max_bytes {
            let Some((position, reason)) = blob_cache_victim(
                &self.order,
                &self.entries,
                &parsed_twins,
                self.admitted,
            ) else {
                break;
            };
            let Some(evicted) = self.order.remove(position) else {
                break;
            };
            if let Some(entry) = self.entries.remove(&evicted) {
                self.bytes = self.bytes.saturating_sub(entry.bytes.len());
                record_blob_cache_drop(reason);
            }
        }
        self.admitted += 1;
        self.bytes += bytes.len();
        self.entries.insert(key.clone(), BlobCacheEntry {
            bytes,
            active: self.admitted,
        });
        self.order.push_back(key);
        true
    }

    fn get(&mut self, key: &BlobKey) -> Option<Arc<[u8]>> {
        let admitted = self.admitted;
        self.entries.get_mut(key).map(|entry| {
            entry.active = admitted;
            Arc::clone(&entry.bytes)
        })
    }
}

const BLOB_PROTECTION_TURNOVERS: u64 = 4;
pub(crate) const PUFFIN_BLOB_CACHE_OUTCOMES: &[&str] = &["hit", "miss"];
pub(crate) const PUFFIN_BLOB_CACHE_DROP_REASONS: &[&str] =
    &["stale", "redundant", "fifo", "oversized"];

fn blob_cache_victim(
    order: &VecDeque<BlobKey>,
    entries: &HashMap<BlobKey, BlobCacheEntry>,
    parsed_twins: &HashMap<(String, u64), usize>,
    admitted: u64,
) -> Option<(usize, &'static str)> {
    let turnover = BLOB_PROTECTION_TURNOVERS * order.len().max(1) as u64;
    let stale = order
        .iter()
        .position(|key| {
            entries
                .get(key)
                .is_some_and(|entry| admitted.saturating_sub(entry.active) >= turnover)
        })
        .map(|position| (position, "stale"));
    let redundant = || {
        order
            .iter()
            .enumerate()
            .filter_map(|(position, key)| {
                parsed_twins
                    .get(&(key.path.clone(), key.offset))
                    .map(|rank| (*rank, position))
            })
            .max()
            .map(|(_, position)| (position, "redundant"))
    };
    stale
        .or_else(redundant)
        .or_else(|| (!order.is_empty()).then_some((0, "fifo")))
}

fn record_blob_cache_drop(reason: &'static str) {
    metrics::counter!(
        "siglake_iceberg_puffin_blob_cache_evictions_total",
        "reason" => reason
    )
    .increment(1);
}

static PUFFIN_BLOB_FETCHES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PUFFIN_BLOB_CACHE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn blob_cache() -> &'static Mutex<BlobCache> {
    static CACHE: OnceLock<Mutex<BlobCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BlobCache::default()))
}

fn blob_cache_bounds() -> (usize, usize) {
    let entries = std::env::var("SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(128);
    let bytes = crate::arrow::puffin_blob_cache_max_bytes();
    (entries, bytes)
}

fn blob_cache_get(key: &BlobKey) -> Option<Arc<[u8]>> {
    let (max_entries, max_bytes) = blob_cache_bounds();
    if max_entries == 0 || max_bytes == 0 {
        return None;
    }
    let hit = blob_cache().lock().unwrap().get(key);
    if hit.is_some() {
        PUFFIN_BLOB_CACHE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    metrics::counter!(
        "siglake_iceberg_puffin_blob_cache_lookups_total",
        "outcome" => if hit.is_some() { "hit" } else { "miss" }
    )
    .increment(1);
    hit
}

fn blob_cache_peek(key: &BlobKey) -> Option<Arc<[u8]>> {
    let (max_entries, max_bytes) = blob_cache_bounds();
    if max_entries == 0 || max_bytes == 0 {
        return None;
    }
    blob_cache().lock().unwrap().get(key)
}

fn blob_cache_put(key: BlobKey, bytes: Arc<[u8]>) {
    let (max_entries, max_bytes) = blob_cache_bounds();
    if max_entries == 0 || max_bytes == 0 {
        return;
    }
    if bytes.len() > max_bytes {
        record_blob_cache_drop("oversized");
        return;
    }
    blob_cache()
        .lock()
        .unwrap()
        .put(key, bytes, max_entries, max_bytes);
}

pub(crate) fn puffin_blob_cache_stats(path_substring: &str) -> (usize, usize, usize) {
    let cache = blob_cache().lock().unwrap();
    let (entries, bytes) = cache
        .entries
        .iter()
        .filter(|(key, _)| key.path.contains(path_substring))
        .fold((0, 0), |(entries, bytes), (_, entry)| {
            (entries + 1, bytes + entry.bytes.len())
        });
    (entries, bytes, cache.bytes)
}

pub(crate) fn puffin_blob_fetch_counts() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        PUFFIN_BLOB_FETCHES.load(Relaxed),
        PUFFIN_BLOB_CACHE_HITS.load(Relaxed),
    )
}

/// Puffin reader
pub struct PuffinReader {
    input_file: InputFile,
    file_metadata: OnceCell<Arc<FileMetadata>>,
    cache_bypass: bool,
}

impl PuffinReader {
    /// Returns a new Puffin reader
    pub fn new(input_file: InputFile) -> Self {
        Self {
            input_file,
            file_metadata: OnceCell::new(),
            cache_bypass: false,
        }
    }

    /// Bypass process-global immutable metadata and blob caches.
    pub fn with_cache_bypass(mut self, cache_bypass: bool) -> Self {
        self.cache_bypass = cache_bypass;
        self
    }

    /// Returns file metadata
    pub async fn file_metadata(&self) -> Result<&FileMetadata> {
        self.file_metadata
            .get_or_try_init(|| async {
                let path = self.input_file.location().to_string();
                if !self.cache_bypass
                    && let Some(metadata) = metadata_cache_get(&path)
                {
                    tokio::task::coop::consume_budget().await;
                    return Ok(metadata);
                }
                let input_file = self.input_file.clone();
                let cache_path = path.clone();
                let cache_bypass = self.cache_bypass;
                metadata_debouncer()
                    .run(path, "puffin_metadata", move || async move {
                        if !cache_bypass && let Some(metadata) = metadata_cache_get(&cache_path) {
                            tokio::task::coop::consume_budget().await;
                            return Ok(metadata);
                        }
                        let metadata = Arc::new(FileMetadata::read(&input_file).await?);
                        if !cache_bypass {
                            metadata_cache_put(cache_path, metadata.clone());
                        }
                        Ok(metadata)
                    })
                    .await
            })
            .await
            .map(Arc::as_ref)
    }

    /// Returns the size in bytes of the Puffin footer (from footer magic to EOF).
    pub async fn footer_size_in_bytes(&self) -> Result<u64> {
        let footer_bytes = FileMetadata::footer_size_in_bytes(&self.input_file).await?;
        crate::io::read_observability::record_object_store_reads(
            crate::io::read_observability::ObjectStoreReadPhase::Footer,
            1,
            footer_bytes,
        );
        Ok(footer_bytes)
    }

    /// Returns blob
    pub async fn blob(&self, blob_metadata: &BlobMetadata) -> Result<Blob> {
        validate_puffin_compression(blob_metadata.compression_codec)?;

        let start = blob_metadata.offset;
        let end = start + blob_metadata.length;
        let key = BlobKey {
            path: self.input_file.location().to_string(),
            offset: start,
        };
        let data = if !self.cache_bypass
            && let Some(hit) = blob_cache_get(&key)
        {
            tokio::task::coop::consume_budget().await;
            hit
        } else {
            let input_file = self.input_file.clone();
            let codec = blob_metadata.compression_codec;
            let cache_key = key.clone();
            let cache_bypass = self.cache_bypass;
            blob_debouncer()
                .run(key, "puffin_blob", move || async move {
                    if !cache_bypass && let Some(hit) = blob_cache_peek(&cache_key) {
                        tokio::task::coop::consume_budget().await;
                        return Ok(hit);
                    }
                    let file_read = input_file.reader().await?;
                    let outcome = file_read.read_with_outcome(start..end).await?;
                    if outcome.fetched {
                        PUFFIN_BLOB_FETCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        metrics::counter!("siglake_iceberg_puffin_blob_fetches_total").increment(1);
                        record_object_store_reads(
                            ObjectStoreReadPhase::Index,
                            1,
                            outcome.bytes.len() as u64,
                        );
                    }
                    let data = Arc::<[u8]>::from(codec.decompress(outcome.bytes.to_vec())?);
                    if !cache_bypass {
                        blob_cache_put(cache_key, data.clone());
                    }
                    Ok(data)
                })
                .await?
        };

        Ok(Blob {
            r#type: blob_metadata.r#type.clone(),
            fields: blob_metadata.fields.clone(),
            snapshot_id: blob_metadata.snapshot_id,
            sequence_number: blob_metadata.sequence_number,
            data: data.as_ref().to_vec(),
            properties: blob_metadata.properties.clone(),
        })
    }

    /// Open an addressable reader over one uncompressed blob.
    pub async fn blob_range_reader(&self, blob_metadata: &BlobMetadata) -> Result<BlobRangeReader> {
        if blob_metadata.compression_codec() != CompressionCodec::None {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!(
                    "blob {} is {:?}-compressed and cannot be read in part",
                    blob_metadata.blob_type(),
                    blob_metadata.compression_codec()
                ),
            ));
        }
        Ok(BlobRangeReader {
            file_read: self.input_file.reader().await?,
            start: blob_metadata.offset(),
            len: blob_metadata.length(),
        })
    }
}

/// Byte-range reader bounded to one uncompressed Puffin blob.
pub struct BlobRangeReader {
    file_read: Box<dyn FileRead>,
    start: u64,
    len: u64,
}

impl BlobRangeReader {
    /// The blob's addressable length.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the blob is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read `len` bytes at `offset` within the blob.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<bytes::Bytes> {
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= self.len)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("range {offset}..+{len} is outside a {}-byte blob", self.len),
                )
            })?;
        let bytes = self
            .file_read
            .read(self.start + offset..self.start + end)
            .await?;
        record_object_store_reads(ObjectStoreReadPhase::Index, 1, bytes.len() as u64);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::ErrorKind;
    use crate::compression::CompressionCodec;
    use crate::puffin::metadata::BlobMetadata;
    use crate::puffin::reader::PuffinReader;
    use crate::puffin::test_utils::{
        blob_0, blob_1, java_uncompressed_metric_input_file,
        java_zstd_compressed_metric_input_file, uncompressed_metric_file_metadata,
        zstd_compressed_metric_file_metadata,
    };

    #[test]
    fn blob_cache_holds_both_bounds_and_rejects_oversized_entries() {
        let mut cache = super::BlobCache::default();
        let key = |offset| super::BlobKey {
            path: "memory://candidate/cache-1752.puffin".to_string(),
            offset,
        };
        assert!(cache.put(key(0), std::sync::Arc::from(&b"aaaa"[..]), 2, 6));
        assert!(cache.put(key(4), std::sync::Arc::from(&b"bbbb"[..]), 2, 6));
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.bytes, 4);
        assert!(cache.get(&key(0)).is_none(), "byte bound evicted the oldest");
        assert!(cache.get(&key(4)).is_some());
        assert!(!cache.put(key(8), std::sync::Arc::from(&b"1234567"[..]), 2, 6));
        assert!(!cache.entries.contains_key(&key(8)));
        assert_eq!(cache.bytes, 4, "an oversized refusal keeps residents");

        let mut cache = super::BlobCache::default();
        for offset in [0, 4, 8] {
            assert!(cache.put(
                key(offset),
                std::sync::Arc::from(&b"aaaa"[..]),
                2,
                usize::MAX,
            ));
        }
        assert_eq!(cache.entries.len(), 2);
        assert!(cache.get(&key(0)).is_none(), "entry bound evicted the oldest");
        assert_eq!(cache.bytes, 8);

        let mut cache = super::BlobCache::default();
        let first: std::sync::Arc<[u8]> = std::sync::Arc::from(&b"0123456789"[..]);
        assert!(cache.put(key(0), first.clone(), 2, usize::MAX));
        assert!(cache.put(key(0), first, 2, usize::MAX));
        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.bytes, 10, "a repeat insert is charged once");
        assert!(cache.put(key(4), std::sync::Arc::from(&b"aaaaa"[..]), 2, usize::MAX));
        assert!(cache.put(key(8), std::sync::Arc::from(&b"bbbbb"[..]), 2, usize::MAX));
        assert_eq!(cache.bytes, 10, "eviction returns the removed bytes");
        assert!(cache.get(&key(0)).is_none());
    }

    #[tokio::test]
    async fn test_puffin_reader_uncompressed_metric_data() {
        let input_file = java_uncompressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, uncompressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_puffin_reader_zstd_compressed_metric_data() {
        let input_file = java_zstd_compressed_metric_input_file();
        let puffin_reader = PuffinReader::new(input_file);

        let file_metadata = puffin_reader.file_metadata().await.unwrap().clone();
        assert_eq!(file_metadata, zstd_compressed_metric_file_metadata());

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.first().unwrap())
                .await
                .unwrap(),
            blob_0()
        );

        assert_eq!(
            puffin_reader
                .blob(file_metadata.blobs.get(1).unwrap())
                .await
                .unwrap(),
            blob_1(),
        )
    }

    #[tokio::test]
    async fn test_gzip_compression_rejected_on_blob_access() {
        // Use a real puffin file
        let input_file = java_uncompressed_metric_input_file();
        let reader = PuffinReader::new(input_file);

        // Create a BlobMetadata with Gzip compression
        let gzip_blob_metadata = BlobMetadata {
            r#type: "test-type".to_string(),
            fields: vec![1],
            snapshot_id: 1,
            sequence_number: 1,
            offset: 4,
            length: 10,
            compression_codec: CompressionCodec::gzip_default(),
            properties: HashMap::new(),
        };

        // Attempting to access the blob should fail
        let result = reader.blob(&gzip_blob_metadata).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.to_string().contains("gzip"));
        assert!(
            err.to_string()
                .contains("is not supported for Puffin files")
        );
    }
}
