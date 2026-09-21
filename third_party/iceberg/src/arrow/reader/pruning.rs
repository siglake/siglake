// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.

//! Siglake bloom, inverted-index, and promoted-column pruning.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use futures::StreamExt;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

use super::{ArrowReader, DEFAULT_RANGE_FETCH_CONCURRENCY, PromotedPruneSpec, RawPruneSpec};
use crate::Result;
use crate::io::FileIO;
use crate::puffin::PuffinReader;
use crate::scan::FileScanTask;

const TEXT_INDEX_STORAGE_PUFFIN: &str = "puffin";
const TEXT_INDEX_STORAGE_FOOTER_KV: &str = "footer_kv";

/// Storage forms carried by text-index cache metrics.
pub const TEXT_INDEX_STORAGE_FORMS: &[&str] =
    &[TEXT_INDEX_STORAGE_PUFFIN, TEXT_INDEX_STORAGE_FOOTER_KV];
/// Startup stages carried by text-index latency metrics.
pub const TEXT_INDEX_STARTUP_STAGES: &[&str] =
    &["permit_wait", "blob_fetch", "decode", "selection"];
/// Outcomes carried by parsed-index cache lookup metrics.
pub const PARSED_INDEX_CACHE_OUTCOMES: &[&str] = &["hit", "miss"];
/// Reasons carried by parsed-index cache eviction metrics.
pub const PARSED_INDEX_CACHE_DROP_REASONS: &[&str] =
    &["byte_bound", "entry_bound", "oversized"];

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum ParsedIndexKey {
    Puffin { path: String, offset: u64 },
    FooterKv { path: String, column: String },
}

impl ParsedIndexKey {
    fn storage(&self) -> &'static str {
        match self {
            Self::Puffin { .. } => TEXT_INDEX_STORAGE_PUFFIN,
            Self::FooterKv { .. } => TEXT_INDEX_STORAGE_FOOTER_KV,
        }
    }

    fn path(&self) -> &str {
        match self {
            Self::Puffin { path, .. } | Self::FooterKv { path, .. } => path,
        }
    }
}

struct ParsedIndexEntry {
    index: Arc<siglake_index::InvertedIndex>,
    size: usize,
    hits: u64,
}

#[derive(Default)]
struct ParsedIndexCache {
    order: VecDeque<ParsedIndexKey>,
    entries: HashMap<ParsedIndexKey, ParsedIndexEntry>,
    bytes: usize,
    evictions: u64,
    oversized_skips: u64,
}

impl ParsedIndexCache {
    fn get(&mut self, key: &ParsedIndexKey) -> Option<Arc<siglake_index::InvertedIndex>> {
        let index = self.entries.get_mut(key).map(|entry| {
            entry.hits += 1;
            Arc::clone(&entry.index)
        })?;
        if let Some(position) = self.order.iter().position(|entry| entry == key) {
            let key = self.order.remove(position).expect("cache position exists");
            self.order.push_back(key);
        }
        Some(index)
    }

    fn put(
        &mut self,
        key: ParsedIndexKey,
        index: Arc<siglake_index::InvertedIndex>,
        max_bytes: usize,
        max_entries: usize,
    ) {
        if max_bytes == 0 || max_entries == 0 || self.entries.contains_key(&key) {
            return;
        }
        let size = index.heap_size_bytes();
        if size > max_bytes {
            self.oversized_skips += 1;
            record_parsed_index_drop("oversized");
            return;
        }
        self.entries.insert(key.clone(), ParsedIndexEntry { index, size, hits: 0 });
        self.order.push_back(key);
        self.bytes += size;
        while self.order.len() > max_entries || self.bytes > max_bytes {
            let reason = if self.order.len() > max_entries {
                "entry_bound"
            } else {
                "byte_bound"
            };
            let Some(key) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&key) {
                self.bytes = self.bytes.saturating_sub(entry.size);
                self.evictions += 1;
                record_parsed_index_drop(reason);
            }
        }
        metrics::gauge!("siglake_iceberg_parsed_index_cache_bytes").set(self.bytes as f64);
        metrics::gauge!("siglake_iceberg_parsed_index_cache_max_bytes").set(max_bytes as f64);
    }
}

fn parsed_index_cache() -> &'static Mutex<ParsedIndexCache> {
    static CACHE: OnceLock<Mutex<ParsedIndexCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(ParsedIndexCache::default()))
}

static INVERTED_INDEX_DECODES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static INVERTED_INDEX_CACHE_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn record_text_index_stage(stage: &'static str, storage: &'static str, started: std::time::Instant) {
    metrics::histogram!(
        "siglake_iceberg_text_index_startup_seconds",
        "stage" => stage,
        "storage" => storage
    )
    .record(started.elapsed().as_secs_f64());
}

fn record_parsed_index_lookup(outcome: &'static str, storage: &'static str) {
    metrics::counter!(
        "siglake_iceberg_parsed_index_cache_lookups_total",
        "outcome" => outcome,
        "storage" => storage
    )
    .increment(1);
}

fn record_parsed_index_drop(reason: &'static str) {
    metrics::counter!(
        "siglake_iceberg_parsed_index_cache_evictions_total",
        "reason" => reason
    )
    .increment(1);
}

fn parsed_index_cache_get(key: &ParsedIndexKey) -> Option<Arc<siglake_index::InvertedIndex>> {
    if crate::arrow::parsed_index_cache_max_bytes() == 0
        || crate::arrow::puffin_blob_cache_max_entries() == 0
    {
        return None;
    }
    let hit = parsed_index_cache().lock().unwrap().get(key);
    if hit.is_some() {
        INVERTED_INDEX_CACHE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        record_parsed_index_lookup("hit", key.storage());
    }
    hit
}

fn parsed_index_cache_put(key: ParsedIndexKey, index: Arc<siglake_index::InvertedIndex>) {
    parsed_index_cache().lock().unwrap().put(
        key,
        index,
        crate::arrow::parsed_index_cache_max_bytes(),
        crate::arrow::puffin_blob_cache_max_entries(),
    );
}

/// Current parsed-index cache occupancy and cumulative drops.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ParsedIndexCacheFootprint {
    /// Resident entries.
    pub entries: usize,
    /// Resident heap bytes.
    pub bytes: usize,
    /// Entries removed by a bound.
    pub evictions: u64,
    /// Entries refused because one exceeded the byte budget.
    pub oversized_skips: u64,
}

/// Snapshot process-wide parsed-index cache occupancy.
pub fn parsed_inverted_index_cache_footprint() -> ParsedIndexCacheFootprint {
    let cache = parsed_index_cache().lock().unwrap();
    ParsedIndexCacheFootprint {
        entries: cache.entries.len(),
        bytes: cache.bytes,
        evictions: cache.evictions,
        oversized_skips: cache.oversized_skips,
    }
}

/// Return resident entries and served hits for keys containing `path_substring`.
pub fn parsed_inverted_index_cache_stats(path_substring: &str) -> (usize, u64) {
    parsed_index_cache()
        .lock()
        .unwrap()
        .entries
        .iter()
        .filter(|(key, _)| key.path().contains(path_substring))
        .fold((0, 0), |(entries, hits), (_, entry)| {
            (entries + 1, hits + entry.hits)
        })
}

/// Return cumulative whole-index decodes and parsed-cache hits.
pub fn inverted_index_decode_counts() -> (u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        INVERTED_INDEX_DECODES.load(Relaxed),
        INVERTED_INDEX_CACHE_HITS.load(Relaxed),
    )
}

pub(crate) fn parsed_index_puffin_twin_ranks() -> HashMap<(String, u64), usize> {
    parsed_index_cache()
        .lock()
        .unwrap()
        .order
        .iter()
        .enumerate()
        .filter_map(|(rank, key)| match key {
            ParsedIndexKey::Puffin { path, offset } => Some(((path.clone(), *offset), rank)),
            ParsedIndexKey::FooterKv { .. } => None,
        })
        .collect()
}

impl ArrowReader {
    fn index_covers_file(
        metadata: &ParquetMetaData,
        index: &siglake_index::InvertedIndex,
        storage: &'static str,
    ) -> bool {
        let file_rows = metadata
            .row_groups()
            .iter()
            .map(|group| group.num_rows() as u64)
            .sum::<u64>();
        if u64::from(index.n_rows()) == file_rows {
            return true;
        }
        metrics::counter!(
            "siglake_index_row_domain_mismatch_total",
            "storage" => storage
        )
        .increment(1);
        false
    }

    fn prune_source_label(spec: &RawPruneSpec) -> &'static str {
        if spec.fts_udf {
            "fts_udf"
        } else {
            "like_substring"
        }
    }

    fn metadata_value<'a>(metadata: &'a ParquetMetaData, key: &str) -> Option<&'a str> {
        metadata
            .file_metadata()
            .key_value_metadata()?
            .iter()
            .find(|entry| entry.key == key)
            .and_then(|entry| entry.value.as_deref())
    }

    fn file_trigram_bloom(metadata: &ParquetMetaData) -> Option<siglake_bloom::TokenBloom> {
        siglake_bloom::TokenBloom::from_hex(Self::metadata_value(
            metadata,
            siglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY,
        )?)
    }

    fn footer_inverted_index_hex<'a>(
        metadata: &'a ParquetMetaData,
        column: &str,
    ) -> Option<&'a str> {
        let key = siglake_index::inverted_index_kv_key(column);
        Self::metadata_value(metadata, key.as_ref())
    }

    fn footer_inverted_index_checksum_allows(
        metadata: &ParquetMetaData,
        column: &str,
        hex: &str,
    ) -> bool {
        let checksum_key = siglake_index::inverted_index_crc32_kv_key(column);
        let checksum_entry = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry.key == checksum_key.as_ref())
            });
        let Some(checksum_entry) = checksum_entry else {
            return true;
        };
        let Some(checksum) = checksum_entry.value.as_deref() else {
            metrics::counter!(
                "siglake_index_footer_checksum_refused_total",
                "reason" => "malformed"
            )
            .increment(1);
            return false;
        };
        let expected = if checksum.len() == 8 {
            u32::from_str_radix(checksum, 16).ok()
        } else {
            None
        };
        let Some(expected) = expected else {
            metrics::counter!(
                "siglake_index_footer_checksum_refused_total",
                "reason" => "malformed"
            )
            .increment(1);
            return false;
        };
        let Some(actual) = siglake_index::inverted_index_hex_crc32(hex) else {
            metrics::counter!(
                "siglake_index_footer_checksum_refused_total",
                "reason" => "malformed"
            )
            .increment(1);
            return false;
        };
        if actual != expected {
            metrics::counter!(
                "siglake_index_footer_checksum_refused_total",
                "reason" => "mismatch"
            )
            .increment(1);
            return false;
        }
        true
    }

    fn stamped_row_group_size_matches(metadata: &ParquetMetaData, stamped: usize) -> bool {
        let row_groups = metadata.row_groups();
        !row_groups.is_empty()
            && row_groups.iter().enumerate().all(|(index, group)| {
                let rows = group.num_rows() as usize;
                if index + 1 == row_groups.len() {
                    rows > 0 && rows <= stamped
                } else {
                    rows == stamped
                }
            })
    }

    async fn puffin_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<Arc<siglake_index::InvertedIndex>>> {
        for reference in &task.statistics_blobs {
            if reference.blob_type != "siglake-inverted-v1"
                || reference.properties.get("column").map(String::as_str) != Some(column)
            {
                continue;
            }
            let Some(row_group_size) = reference
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

            let reader = PuffinReader::new(file_io.new_input(&reference.statistics_path)?)
                .with_cache_bypass(cache_bypass);
            let blob_metadata = reader
                .file_metadata()
                .await?
                .blobs()
                .iter()
                .find(|blob| {
                    blob.blob_type() == "siglake-inverted-v1"
                        && blob.properties().get("data_file").map(String::as_str)
                            == Some(task.data_file_path())
                        && blob.properties().get("column").map(String::as_str) == Some(column)
                })
                .cloned();
            let Some(blob_metadata) = blob_metadata else {
                continue;
            };
            let key = ParsedIndexKey::Puffin {
                path: reference.statistics_path.clone(),
                offset: blob_metadata.offset(),
            };
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Self::index_covers_file(
                    metadata,
                    &index,
                    TEXT_INDEX_STORAGE_PUFFIN,
                )
                .then_some(index));
            }
            let waited = std::time::Instant::now();
            let _permit = Self::index_load_semaphore()
                .acquire()
                .await
                .expect("index-load semaphore is never closed");
            record_text_index_stage("permit_wait", TEXT_INDEX_STORAGE_PUFFIN, waited);
            if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
                return Ok(Self::index_covers_file(
                    metadata,
                    &index,
                    TEXT_INDEX_STORAGE_PUFFIN,
                )
                .then_some(index));
            }
            let fetched = std::time::Instant::now();
            let blob = reader.blob(&blob_metadata).await?;
            record_text_index_stage("blob_fetch", TEXT_INDEX_STORAGE_PUFFIN, fetched);
            let decoded = std::time::Instant::now();
            let index = siglake_index::InvertedIndex::from_bytes(blob.data()).map(Arc::new);
            record_text_index_stage("decode", TEXT_INDEX_STORAGE_PUFFIN, decoded);
            let Some(index) = index else {
                return Ok(None);
            };
            INVERTED_INDEX_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            record_parsed_index_lookup("miss", TEXT_INDEX_STORAGE_PUFFIN);
            if !Self::index_covers_file(metadata, &index, TEXT_INDEX_STORAGE_PUFFIN) {
                return Ok(None);
            }
            if !cache_bypass {
                parsed_index_cache_put(key, Arc::clone(&index));
            }
            return Ok(Some(index));
        }
        Ok(None)
    }

    async fn cached_footer_inverted_index(
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Option<Arc<siglake_index::InvertedIndex>> {
        let hex = Self::footer_inverted_index_hex(metadata, column)?;
        // Verify before a warm cache lookup so a cached parse cannot hide a
        // footer whose checksum no longer agrees with the stored blob.
        if !Self::footer_inverted_index_checksum_allows(metadata, column, hex) {
            return None;
        }
        let key = ParsedIndexKey::FooterKv {
            path: task.data_file_path().to_string(),
            column: column.to_string(),
        };
        if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
            return Self::index_covers_file(metadata, &index, TEXT_INDEX_STORAGE_FOOTER_KV)
                .then_some(index);
        }
        let waited = std::time::Instant::now();
        let _permit = Self::index_load_semaphore()
            .acquire()
            .await
            .expect("index-load semaphore is never closed");
        record_text_index_stage("permit_wait", TEXT_INDEX_STORAGE_FOOTER_KV, waited);
        if !cache_bypass && let Some(index) = parsed_index_cache_get(&key) {
            return Self::index_covers_file(metadata, &index, TEXT_INDEX_STORAGE_FOOTER_KV)
                .then_some(index);
        }
        let decoded = std::time::Instant::now();
        let index = siglake_index::InvertedIndex::from_hex(hex).map(Arc::new);
        record_text_index_stage("decode", TEXT_INDEX_STORAGE_FOOTER_KV, decoded);
        let index = index?;
        INVERTED_INDEX_DECODES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        record_parsed_index_lookup("miss", TEXT_INDEX_STORAGE_FOOTER_KV);
        if !Self::index_covers_file(metadata, &index, TEXT_INDEX_STORAGE_FOOTER_KV) {
            return None;
        }
        if !cache_bypass {
            parsed_index_cache_put(key, Arc::clone(&index));
        }
        Some(index)
    }

    async fn file_inverted_index(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        column: &str,
        cache_bypass: bool,
    ) -> Result<Option<(Arc<siglake_index::InvertedIndex>, &'static str)>> {
        let footer_key = siglake_index::inverted_index_kv_key(column);
        if Self::metadata_value(metadata, footer_key.as_ref()).is_some()
            && let Some(index) =
                Self::cached_footer_inverted_index(task, metadata, column, cache_bypass).await
        {
            return Ok(Some((index, TEXT_INDEX_STORAGE_FOOTER_KV)));
        }
        Ok(
            Self::puffin_inverted_index(file_io, task, metadata, column, cache_bypass)
                .await?
                .map(|index| (index, TEXT_INDEX_STORAGE_PUFFIN)),
        )
    }

    fn index_load_semaphore() -> &'static tokio::sync::Semaphore {
        static SEMAPHORE: std::sync::OnceLock<tokio::sync::Semaphore> = std::sync::OnceLock::new();
        SEMAPHORE.get_or_init(|| {
            tokio::sync::Semaphore::new(index_load_concurrency_from(
                std::env::var("SIGLAKE_INDEX_LOAD_CONCURRENCY")
                    .ok()
                    .as_deref(),
            ))
        })
    }

    fn bloom_might_contain_substring(
        bloom: &siglake_bloom::TokenBloom,
        substring: &str,
    ) -> Option<bool> {
        let grams = siglake_bloom::query_trigrams(substring)?;
        Some(grams.iter().all(|gram| bloom.maybe_contains(gram)))
    }

    pub(super) fn file_might_match_prune_spec(
        metadata: &ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> bool {
        let bloom = (spec.column == "raw")
            .then(|| Self::file_trigram_bloom(metadata))
            .flatten();
        let might_match = |value: &str| {
            bloom
                .as_ref()
                .and_then(|bloom| Self::bloom_might_contain_substring(bloom, value))
                .unwrap_or(true)
        };
        let matches = spec.all_terms.iter().all(|term| might_match(term))
            && (spec.any_terms.is_empty() || spec.any_terms.iter().any(|term| might_match(term)))
            && spec
                .substrings
                .iter()
                .all(|substring| might_match(substring));
        metrics::counter!(
            "siglake_iceberg_raw_bloom_skip_total",
            "outcome" => if matches { "read" } else { "skip" },
            "source" => Self::prune_source_label(spec)
        )
        .increment(1);
        matches
    }

    pub(super) fn rowgroup_bloom_survivors_for_spec(
        metadata: &ParquetMetaData,
        spec: &RawPruneSpec,
    ) -> Option<Vec<usize>> {
        if spec.column != "raw" {
            return None;
        }
        let blooms = siglake_bloom::rowgroup_blooms_from_hex(Self::metadata_value(
            metadata,
            siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY,
        )?)?;
        if blooms.len() != metadata.num_row_groups() {
            return None;
        }
        let source = Self::prune_source_label(spec);
        Some(
            blooms
                .iter()
                .enumerate()
                .filter_map(|(index, bloom)| {
                    let matches = spec.all_terms.iter().chain(&spec.substrings).all(|value| {
                        Self::bloom_might_contain_substring(bloom, value).unwrap_or(true)
                    }) && (spec.any_terms.is_empty()
                        || spec.any_terms.iter().any(|value| {
                            Self::bloom_might_contain_substring(bloom, value).unwrap_or(true)
                        }));
                    metrics::counter!(
                        "siglake_iceberg_raw_rowgroup_bloom_skip_total",
                        "outcome" => if matches { "read" } else { "skip" },
                        "source" => source
                    )
                    .increment(1);
                    matches.then_some(index)
                })
                .collect(),
        )
    }

    pub(super) fn row_group_might_match_promoted(
        group: &RowGroupMetaData,
        spec: &PromotedPruneSpec,
    ) -> bool {
        let Some(column) = group
            .columns()
            .iter()
            .find(|column| column.column_descr().path().string() == spec.column)
        else {
            return true;
        };
        let Some(statistics) = column.statistics() else {
            return true;
        };
        if statistics.null_count_opt() == Some(group.num_rows() as u64) {
            return false;
        }
        let parquet::file::statistics::Statistics::ByteArray(bytes) = statistics else {
            return true;
        };
        let (Some(min), Some(max)) = (bytes.min_bytes_opt(), bytes.max_bytes_opt()) else {
            return true;
        };
        spec.values
            .iter()
            .any(|value| value.as_bytes() >= min && value.as_bytes() <= max)
    }

    pub(super) async fn inverted_index_row_selection(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        selected_row_groups: &Option<Vec<usize>>,
        spec: &RawPruneSpec,
        cache_bypass: bool,
    ) -> Result<Option<RowSelection>> {
        if segmented_index_reads_enabled()
            && let Some(selection) = Self::segmented_index_row_selection(
                file_io,
                task,
                metadata,
                selected_row_groups,
                spec,
                cache_bypass,
            )
            .await?
        {
            return Ok(Some(selection));
        }
        if spec.segmented_clipped_limit.is_some() {
            return Ok(None);
        }
        let index =
            Self::file_inverted_index(file_io, task, metadata, &spec.column, cache_bypass).await?;
        let Some((index, storage)) = index else {
            return Ok(None);
        };
        let selected = std::time::Instant::now();
        let mut matching: Option<Vec<u32>> = None;
        if !spec.all_terms.is_empty() {
            let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
            matching = Some(index.matching_rows_all(&terms));
        }
        if !spec.any_terms.is_empty() {
            let mut union = Vec::new();
            for term in &spec.any_terms {
                if let Some(rows) = index.postings(term) {
                    union = union_sorted(&union, rows);
                }
            }
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &union),
                None => union,
            });
        }
        for substring in &spec.index_substrings {
            let Some(rows) = index.rows_containing(substring) else {
                return Ok(None);
            };
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &rows),
                None => rows,
            });
        }
        let Some(matching) = matching else {
            return Ok(None);
        };
        metrics::counter!(
            "siglake_iceberg_inverted_index_used_total",
            "source" => Self::prune_source_label(spec),
            "storage" => storage
        )
        .increment(1);
        let selection = index_matches_row_selection(
            metadata.row_groups(),
            selected_row_groups,
            &matching,
        );
        record_text_index_stage("selection", storage, selected);
        Ok(Some(selection))
    }

    async fn segmented_index_row_selection(
        file_io: &FileIO,
        task: &FileScanTask,
        metadata: &ParquetMetaData,
        selected_row_groups: &Option<Vec<usize>>,
        spec: &RawPruneSpec,
        cache_bypass: bool,
    ) -> Result<Option<RowSelection>> {
        let blob_type = siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE;
        if let Some(selected) = selected_row_groups.as_ref()
            && selected.windows(2).any(|pair| pair[0] >= pair[1])
        {
            record_segmented_decline("row_group_order");
            return Ok(None);
        }
        for reference in &task.statistics_blobs {
            if reference.blob_type != blob_type
                || reference.properties.get("column").map(String::as_str)
                    != Some(spec.column.as_str())
            {
                continue;
            }
            let reader = PuffinReader::new(file_io.new_input(&reference.statistics_path)?)
                .with_cache_bypass(cache_bypass);
            let blob_metadata = reader
                .file_metadata()
                .await?
                .blobs()
                .iter()
                .find(|blob| {
                    blob.blob_type() == blob_type
                        && blob.properties().get("data_file").map(String::as_str)
                            == Some(task.data_file_path())
                        && blob.properties().get("column").map(String::as_str)
                            == Some(spec.column.as_str())
                })
                .cloned();
            let Some(blob_metadata) = blob_metadata else {
                continue;
            };
            let row_counts: Vec<u64> = metadata
                .row_groups()
                .iter()
                .map(|row_group| row_group.num_rows() as u64)
                .collect();
            let (outcome, cost) = segmented_matching_rows(
                file_io,
                &reference.statistics_path,
                &blob_metadata,
                &row_counts,
                selected_row_groups.as_deref(),
                spec,
                cache_bypass,
            )
            .await?;
            metrics::histogram!("siglake_iceberg_segmented_index_range_reads")
                .record(cost.reads as f64);
            metrics::histogram!("siglake_iceberg_segmented_index_fetched_bytes")
                .record(cost.bytes as f64);
            // #5007: what the staged shape costs and buys. `_stages` is the
            // rounds of store waits the lookup took, against the `_range_reads`
            // a serial source would have waited for one at a time;
            // `_reader_reads` is what a stage re-decodes on its way past the
            // ranges the lookup already holds.
            metrics::histogram!("siglake_iceberg_segmented_index_stages")
                .record(cost.stages as f64);
            metrics::histogram!("siglake_iceberg_segmented_index_reader_reads")
                .record(cost.requests as f64);
            let SegmentedOutcome::Matching {
                rows,
                resident_bytes,
            } = outcome
            else {
                if let SegmentedOutcome::Declined(reason) = outcome {
                    record_segmented_decline(reason);
                }
                return Ok(None);
            };
            metrics::counter!(
                "siglake_iceberg_segmented_index_used_total",
                "source" => Self::prune_source_label(spec)
            )
            .increment(1);
            metrics::histogram!("siglake_iceberg_segmented_index_resident_bytes")
                .record(resident_bytes as f64);
            metrics::histogram!("siglake_iceberg_segmented_index_selected_rows")
                .record(rows.len() as f64);
            return Ok(Some(index_matches_row_selection(
                metadata.row_groups(),
                selected_row_groups,
                &rows,
            )));
        }
        Ok(None)
    }
}

fn segmented_index_reads_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("SIGLAKE_SEGMENTED_INDEX_READS")
                .ok()
                .as_deref()
                .map(str::trim)
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("1" | "true" | "yes" | "on")
        )
    })
}

/// siglake (#5007): how many of a stage's ranges a segmented lookup fetches at
/// once. Default [`DEFAULT_RANGE_FETCH_CONCURRENCY`], the bound the scan's own
/// merged-range reader uses;
/// `SIGLAKE_SEGMENTED_INDEX_RANGE_CONCURRENCY` overrides it.
fn segmented_range_concurrency() -> usize {
    static CONCURRENCY: OnceLock<usize> = OnceLock::new();
    *CONCURRENCY.get_or_init(|| {
        segmented_range_concurrency_from(
            std::env::var("SIGLAKE_SEGMENTED_INDEX_RANGE_CONCURRENCY")
                .ok()
                .as_deref(),
        )
    })
}

fn segmented_range_concurrency_from(raw: Option<&str>) -> usize {
    raw.map(str::trim)
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|&concurrency| concurrency > 0)
        .unwrap_or(DEFAULT_RANGE_FETCH_CONCURRENCY)
}

/// What one segmented lookup fetched — the evidence that it read a sliver of
/// the blob rather than the whole of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SegmentedReadCost {
    /// Ranges the store served — and, within a rounding of the dedupe below,
    /// the sequential round trips a source serving one range at a time would
    /// have taken. A range the reader asked for twice (two terms in one
    /// dictionary block, a later stage re-reading an earlier one) is fetched
    /// once and counted once.
    reads: u64,
    bytes: u64,
    /// Reads the reader made, across every stage. Against [`Self::reads`] this
    /// is what the staged shape re-decodes: a range read again is served from
    /// what the lookup already holds, but its block is verified and walked
    /// again.
    requests: u64,
    /// Rounds of store waits: the trailer, the directory, the dictionary
    /// blocks, the posting sections.
    stages: u64,
}

fn segmented_cost(
    source: &siglake_index::segmented::StagedSource,
    stages: u64,
) -> SegmentedReadCost {
    SegmentedReadCost {
        reads: source.fetched_reads(),
        bytes: source.fetched_bytes(),
        requests: source.requests(),
        stages,
    }
}

/// What one lookup concluded. A decline carries the reason it is recorded
/// under as a metric label; all of them mean the same thing to the caller,
/// which is that this index said nothing about the file.
#[derive(Debug)]
enum SegmentedOutcome {
    Matching {
        rows: Vec<u32>,
        resident_bytes: usize,
    },
    Declined(&'static str),
}

/// One segmented lookup over one blob, driven in **stages** (#5007): the
/// lookup itself is synchronous and runs on this task, decoding only the
/// ranges it already holds and recording the ones it does not
/// ([`siglake_index::segmented::StagedSource`]); this then fetches that
/// stage's ranges together, through [`crate::puffin::BlobRangeReader`], and
/// runs the lookup again. A store wait happens between two runs of the lookup,
/// never inside one, so it holds no thread of tokio's blocking pool — where
/// #4561 held one for the whole of a file's lookup, IO waits included, per file
/// a fanned-out scan was looking up in.
///
/// The stages are the trailer, the directory, the dictionary blocks the terms
/// name and their posting sections: four rounds of store waits for any shape,
/// whatever its read count, plus one per additional term of a conjunction
/// (whose postings are fetched rarest-first and stop as soon as the
/// intersection empties). The ranges *within* a stage go out together, bounded
/// by [`segmented_range_concurrency`].
///
/// The directory itself is held between lookups (#5006,
/// [`segmented_directory_cache_get`]), so a repeat lookup on the same blob
/// reads neither the trailer nor the directory.
async fn segmented_matching_rows(
    file_io: &FileIO,
    statistics_path: &str,
    blob_metadata: &crate::puffin::BlobMetadata,
    row_counts: &[u64],
    groups: Option<&[usize]>,
    spec: &RawPruneSpec,
    cache_bypass: bool,
) -> Result<(SegmentedOutcome, SegmentedReadCost)> {
    let puffin = PuffinReader::new(file_io.new_input(statistics_path)?);
    let range_reader = match puffin.blob_range_reader(blob_metadata).await {
        Ok(reader) => reader,
        Err(_) => {
            return Ok((
                SegmentedOutcome::Declined("compressed"),
                SegmentedReadCost::default(),
            ));
        }
    };
    let source = siglake_index::segmented::StagedSource::new(range_reader.len());
    let offset = blob_metadata.offset();
    let held = (!cache_bypass)
        .then(|| segmented_directory_cache_get(statistics_path, offset))
        .flatten();
    // Termination does not rest on this: a stage fills every range it asked
    // for, an unreadable one included, so the held set only grows and a blob
    // has finitely many ranges. It bounds a future reader that asked for
    // ranges some other way.
    let stage_budget =
        8 + 4 * (spec.all_terms.len() + spec.any_terms.len() + spec.index_substrings.len()).max(1);
    let concurrency = segmented_range_concurrency();
    let mut stages = 0u64;
    // The directory the first stage that could parse it parsed. Carried into
    // the later stages so that a replay re-decodes dictionary blocks and not
    // the whole directory (474.9 KiB on the 7.34M-row file), and held apart
    // from the round's own return so the entry still reaches the cache when the
    // answering round opened on it.
    let mut parsed_here: Option<Arc<siglake_index::segmented::SegmentedDirectory>> = None;
    let mut directory = held;
    loop {
        let (outcome, parsed) = segmented_lookup(
            source.clone(),
            row_counts,
            groups,
            spec,
            directory.as_ref().map(Arc::clone),
        );
        if let Some(parsed) = parsed {
            parsed_here = Some(Arc::clone(&parsed));
            directory = Some(parsed);
        }
        let misses = source.take_misses();
        if misses.is_empty() {
            // A directory this lookup parsed is kept whatever the outcome: the
            // parse is what the next lookup on this blob should not repeat, and
            // a decline over one file's row groups says nothing about the next
            // query's.
            if !cache_bypass && let Some(directory) = parsed_here {
                segmented_directory_cache_put(statistics_path, offset, directory);
            }
            return Ok((outcome, segmented_cost(&source, stages)));
        }
        stages += 1;
        if stages > stage_budget as u64 {
            return Ok((
                SegmentedOutcome::Declined("stages"),
                segmented_cost(&source, stages),
            ));
        }
        // This stage's ranges, together. A range the store refuses is filled as
        // unreadable, which the reader reads as the failed range read it is —
        // `Unanswerable`, never a wrong answer — and which keeps the next run
        // from asking for it again.
        let reader = &range_reader;
        futures::stream::iter(misses.into_iter().map(|(offset, len)| async move {
            (
                offset,
                len,
                reader
                    .read_at(offset, len as u64)
                    .await
                    .ok()
                    .map(|bytes| bytes.to_vec()),
            )
        }))
        .buffer_unordered(concurrency)
        .for_each(|(offset, len, bytes)| {
            match bytes {
                Some(bytes) => source.fill(offset, len, bytes),
                None => source.fill_unreadable(offset, len),
            }
            std::future::ready(())
        })
        .await;
    }
}

/// The whole per-file policy (#4561), synchronous, over whatever ranges the
/// source holds.
///
/// Called once per stage (#5007). A range the source does not hold is a
/// recorded miss and reads as a failed read, so a run that is missing one
/// declines; [`segmented_matching_rows`] fetches what the run recorded and
/// calls this again, and only a run that recorded nothing is the lookup's
/// answer.
fn segmented_lookup(
    source: siglake_index::segmented::StagedSource,
    row_counts: &[u64],
    groups: Option<&[usize]>,
    spec: &RawPruneSpec,
    held: Option<Arc<siglake_index::segmented::SegmentedDirectory>>,
) -> (
    SegmentedOutcome,
    Option<Arc<siglake_index::segmented::SegmentedDirectory>>,
) {
    use siglake_index::segmented::SegmentedReader;
    let opened = match held {
        Some(directory) => SegmentedReader::open_with_directory(source.clone(), directory)
            .map(|index| (index, None))
            .or_else(|| {
                SegmentedReader::open(source).map(|index| {
                    let parsed = Arc::clone(index.directory());
                    (index, Some(parsed))
                })
            }),
        None => SegmentedReader::open(source).map(|index| {
            let parsed = Arc::clone(index.directory());
            (index, Some(parsed))
        }),
    };
    let Some((index, parsed)) = opened else {
        return (SegmentedOutcome::Declined("open"), None);
    };
    if !index.matches_row_groups(row_counts) {
        return (SegmentedOutcome::Declined("row_domain"), parsed);
    }
    let resident_bytes = index.resident_bytes();
    let mut matching = None;
    if let Some(clip) = spec.segmented_clipped_limit {
        if !spec.index_substrings.is_empty() {
            return (
                SegmentedOutcome::Declined("clipped_estimate_unavailable"),
                parsed,
            );
        }
        let all_terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
        let any_terms: Vec<&str> = spec.any_terms.iter().map(String::as_str).collect();
        matching = match index.matching_point_rows_with_df_limit_in_groups(
            &all_terms,
            &any_terms,
            groups,
            clip as u64,
        ) {
            siglake_index::segmented::ClippedLookup::Rows(rows) => Some(rows),
            siglake_index::segmented::ClippedLookup::OverBudget => {
                return (
                    SegmentedOutcome::Declined("clipped_document_frequency"),
                    parsed,
                );
            }
            siglake_index::segmented::ClippedLookup::Unanswerable => {
                return (SegmentedOutcome::Declined("unanswerable"), parsed);
            }
        };
    } else {
        if !spec.all_terms.is_empty() {
            let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
            let Some(rows) = index.matching_rows_all_in_groups(&terms, groups) else {
                return (SegmentedOutcome::Declined("unanswerable"), parsed);
            };
            matching = Some(rows);
        }
        if !spec.any_terms.is_empty() {
            let terms: Vec<&str> = spec.any_terms.iter().map(String::as_str).collect();
            let Some(rows) = index.matching_rows_any_in_groups(&terms, groups) else {
                return (SegmentedOutcome::Declined("unanswerable"), parsed);
            };
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &rows),
                None => rows,
            });
        }
        for substring in &spec.index_substrings {
            let Some(rows) = index.rows_containing_in_groups(substring, groups) else {
                return (SegmentedOutcome::Declined("unanswerable"), parsed);
            };
            matching = Some(match matching {
                Some(existing) => intersect_sorted(&existing, &rows),
                None => rows,
            });
        }
    }
    (
        matching.map_or(SegmentedOutcome::Declined("no_hints"), |rows| {
            SegmentedOutcome::Matching {
                rows,
                resident_bytes,
            }
        }),
        parsed,
    )
}

fn record_segmented_decline(reason: &'static str) {
    metrics::counter!(
        "siglake_iceberg_segmented_index_declined_total",
        "reason" => reason
    )
    .increment(1);
}

type SegmentedDirectoryKey = (String, u64);

struct SegmentedDirectoryEntry {
    directory: Arc<siglake_index::segmented::SegmentedDirectory>,
    size: usize,
    hits: u64,
}

#[derive(Default)]
struct SegmentedDirectoryCache {
    order: VecDeque<SegmentedDirectoryKey>,
    entries: HashMap<SegmentedDirectoryKey, SegmentedDirectoryEntry>,
    bytes: usize,
    evictions: u64,
    oversized_skips: u64,
}

impl SegmentedDirectoryCache {
    fn get(
        &mut self,
        key: &SegmentedDirectoryKey,
    ) -> Option<Arc<siglake_index::segmented::SegmentedDirectory>> {
        let directory = self.entries.get_mut(key).map(|entry| {
            entry.hits += 1;
            Arc::clone(&entry.directory)
        })?;
        if let Some(position) = self.order.iter().position(|candidate| candidate == key) {
            let key = self.order.remove(position).expect("cache position exists");
            self.order.push_back(key);
        }
        Some(directory)
    }

    fn put(
        &mut self,
        key: SegmentedDirectoryKey,
        directory: Arc<siglake_index::segmented::SegmentedDirectory>,
        max_bytes: usize,
    ) {
        let size = directory.resident_bytes() + key.0.len() + std::mem::size_of_val(&key);
        if size > max_bytes {
            self.oversized_skips += 1;
            record_segmented_directory_drop("oversized");
            return;
        }
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(
            key.clone(),
            SegmentedDirectoryEntry {
                directory,
                size,
                hits: 0,
            },
        );
        self.order.push_back(key);
        self.bytes += size;
        while self.bytes > max_bytes {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&evicted) {
                self.bytes -= entry.size;
                self.evictions += 1;
                record_segmented_directory_drop("byte_bound");
            }
        }
        metrics::gauge!("siglake_iceberg_segmented_index_directory_cache_bytes")
            .set(self.bytes as f64);
        metrics::gauge!("siglake_iceberg_segmented_index_directory_cache_max_bytes")
            .set(max_bytes as f64);
    }
}

fn segmented_directory_cache() -> &'static Mutex<SegmentedDirectoryCache> {
    static CACHE: OnceLock<Mutex<SegmentedDirectoryCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(SegmentedDirectoryCache::default()))
}

fn segmented_directory_cache_max_bytes() -> usize {
    std::env::var("SIGLAKE_SEGMENTED_INDEX_DIRECTORY_CACHE_MAX_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(64 * 1024 * 1024)
}

fn segmented_directory_cache_get(
    path: &str,
    offset: u64,
) -> Option<Arc<siglake_index::segmented::SegmentedDirectory>> {
    if segmented_directory_cache_max_bytes() == 0 {
        return None;
    }
    let hit = segmented_directory_cache()
        .lock()
        .unwrap()
        .get(&(path.to_string(), offset));
    metrics::counter!(
        "siglake_iceberg_segmented_index_directory_cache_lookups_total",
        "outcome" => if hit.is_some() { "hit" } else { "miss" }
    )
    .increment(1);
    hit
}

fn segmented_directory_cache_put(
    path: &str,
    offset: u64,
    directory: Arc<siglake_index::segmented::SegmentedDirectory>,
) {
    let max_bytes = segmented_directory_cache_max_bytes();
    if max_bytes == 0 {
        return;
    }
    segmented_directory_cache().lock().unwrap().put(
        (path.to_string(), offset),
        directory,
        max_bytes,
    );
}

fn record_segmented_directory_drop(reason: &'static str) {
    metrics::counter!(
        "siglake_iceberg_segmented_index_directory_cache_evictions_total",
        "reason" => reason
    )
    .increment(1);
}

/// Both segmented-directory cache lookup outcomes.
pub const SEGMENTED_DIRECTORY_CACHE_OUTCOMES: &[&str] = &["hit", "miss"];

/// Every segmented-directory cache eviction reason.
pub const SEGMENTED_DIRECTORY_CACHE_DROP_REASONS: &[&str] = &["byte_bound", "oversized"];

/// Snapshot of the segmented-directory cache footprint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SegmentedDirectoryCacheFootprint {
    /// Resident directories.
    pub entries: usize,
    /// Resident bytes charged to the cache.
    pub bytes: usize,
    /// Lookups served from cached directories.
    pub hits: u64,
    /// Entries evicted by the byte bound.
    pub evictions: u64,
    /// Directories refused because one exceeded the bound.
    pub oversized_skips: u64,
}

/// Read the process-wide segmented-directory cache footprint.
pub fn segmented_directory_cache_footprint() -> SegmentedDirectoryCacheFootprint {
    let cache = segmented_directory_cache().lock().unwrap();
    SegmentedDirectoryCacheFootprint {
        entries: cache.entries.len(),
        bytes: cache.bytes,
        hits: cache.entries.values().map(|entry| entry.hits).sum(),
        evictions: cache.evictions,
        oversized_skips: cache.oversized_skips,
    }
}

/// Count cached directories and hits whose statistics path contains `needle`.
pub fn segmented_directory_cache_stats(needle: &str) -> (usize, u64) {
    segmented_directory_cache()
        .lock()
        .unwrap()
        .entries
        .iter()
        .filter(|((path, _), _)| path.contains(needle))
        .fold((0, 0), |(entries, hits), (_, entry)| {
            (entries + 1, hits + entry.hits)
        })
}

fn index_load_concurrency_from(configured: Option<&str>) -> usize {
    configured
        .and_then(|value| value.trim().parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4)
}

fn union_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    merge_sorted(left, right, false)
}

fn intersect_sorted(left: &[u32], right: &[u32]) -> Vec<u32> {
    merge_sorted(left, right, true)
}

fn merge_sorted(left: &[u32], right: &[u32], intersection: bool) -> Vec<u32> {
    let mut output = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => {
                if !intersection {
                    output.push(left[left_index]);
                }
                left_index += 1;
            }
            std::cmp::Ordering::Greater => {
                if !intersection {
                    output.push(right[right_index]);
                }
                right_index += 1;
            }
            std::cmp::Ordering::Equal => {
                output.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    if !intersection {
        output.extend_from_slice(&left[left_index..]);
        output.extend_from_slice(&right[right_index..]);
    }
    output
}

fn index_matches_row_selection(
    row_groups: &[RowGroupMetaData],
    selected_row_groups: &Option<Vec<usize>>,
    matching: &[u32],
) -> RowSelection {
    let mut selectors = Vec::new();
    let mut base = 0_u64;
    let mut cursor = 0;
    for (group_index, group) in row_groups.iter().enumerate() {
        let end = base + group.num_rows() as u64;
        let start = cursor;
        while cursor < matching.len() && u64::from(matching[cursor]) < end {
            cursor += 1;
        }
        let selected = selected_row_groups
            .as_ref()
            .is_none_or(|groups| groups.contains(&group_index));
        if selected {
            let rebased: Vec<u32> = matching[start..cursor]
                .iter()
                .map(|ordinal| (u64::from(*ordinal) - base) as u32)
                .collect();
            selectors.extend(
                siglake_index::row_selection_runs(&rebased, group.num_rows() as u32)
                    .into_iter()
                    .map(|(selected, length)| {
                        if selected {
                            RowSelector::select(length as usize)
                        } else {
                            RowSelector::skip(length as usize)
                        }
                    }),
            );
        }
        base = end;
    }
    selectors.into()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::StreamExt;
    use siglake_index::segmented::SEGMENTED_BLOB_TYPE;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::metadata::{
        ColumnChunkMetaData, KeyValue, ParquetMetaData, RowGroupMetaData,
    };
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::schema::types::{SchemaDescPtr, SchemaDescriptor, Type as SchemaType};
    use roaring::RoaringTreemap;
    use tempfile::TempDir;

    use super::{
        ArrowReader, DEFAULT_RANGE_FETCH_CONCURRENCY, PromotedPruneSpec, RawPruneSpec,
        SegmentedOutcome, SegmentedReadCost, index_load_concurrency_from, intersect_sorted,
        parsed_inverted_index_cache_stats, segmented_directory_cache_footprint,
        segmented_directory_cache_put, segmented_directory_cache_stats, segmented_matching_rows,
        segmented_range_concurrency_from, union_sorted,
    };
    use crate::delete_vector::DeleteVector;
    use crate::io::{
        FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
        StorageConfig, StorageFactory,
    };
    use crate::puffin::{Blob, BlobMetadata, CompressionCodec, PuffinReader, PuffinWriter};
    use crate::scan::{FileScanTask, StatisticsBlobReference};
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};

    fn bloom(values: &[&str]) -> siglake_bloom::TokenBloom {
        let grams: Vec<String> = values
            .iter()
            .flat_map(|value| siglake_bloom::trigrams(value))
            .collect();
        siglake_bloom::TokenBloom::build(grams.len(), 0.000_001, grams.iter().map(String::as_str))
    }

    fn pruning_fixture() -> (tempfile::TempDir, Arc<ParquetMetaData>, FileScanTask) {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pruning.parquet");
        let raws = [
            "database timeout",
            "healthy startup",
            "worker heartbeat",
            "database migrated",
            "api timeout",
            "healthy shutdown",
            "database retry",
            "worker complete",
        ];
        let hosts = [
            "alpha", "alpha", "beta", "beta", "gamma", "gamma", "omega", "omega",
        ];
        let rowgroup_blooms = [bloom(&raws[..4]), bloom(&raws[4..])];
        let index = siglake_index::InvertedIndex::from_rows(raws);
        let metadata = vec![
            KeyValue::new(
                siglake_bloom::RAW_TRIGRAM_BLOOM_KV_KEY.to_string(),
                Some(bloom(&raws).to_hex()),
            ),
            KeyValue::new(
                siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY.to_string(),
                Some(siglake_bloom::rowgroup_blooms_to_hex(&rowgroup_blooms)),
            ),
            KeyValue::new(
                siglake_index::inverted_index_kv_key("raw").into_owned(),
                Some(index.to_hex()),
            ),
        ];
        let field = |name: &str, id: i32| {
            Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        };
        let arrow_schema = Arc::new(ArrowSchema::new(vec![field("raw", 1), field("host", 2)]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(raws.to_vec())),
                Arc::new(StringArray::from(hosts.to_vec())),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(metadata))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            arrow_schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap()).unwrap();
        let metadata = Arc::new(reader.metadata().clone());
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    Arc::new(NestedField::optional(
                        1,
                        "raw",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "host",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&path).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_data_file_path(path.to_string_lossy().into_owned())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1, 2])
            .with_case_sensitive(true)
            .build();
        (temp, metadata, task)
    }

    #[test]
    fn sorted_set_composition_is_exact() {
        assert_eq!(union_sorted(&[1, 3, 5], &[2, 3, 6]), [1, 2, 3, 5, 6]);
        assert_eq!(intersect_sorted(&[1, 3, 5], &[2, 3, 6]), [3]);
        assert_eq!(index_load_concurrency_from(None), 4);
        assert_eq!(index_load_concurrency_from(Some(" 7 ")), 7);
        assert_eq!(index_load_concurrency_from(Some("0")), 4);
        assert_eq!(index_load_concurrency_from(Some("invalid")), 4);
    }

    #[test]
    fn parsed_index_cache_honours_both_bounds_and_keeps_the_used_entry() {
        let index = Arc::new(siglake_index::InvertedIndex::from_rows([
            "database timeout",
            "request complete",
        ]));
        let size = index.heap_size_bytes();
        let key = |n: u64| super::ParsedIndexKey::Puffin {
            path: format!("s3://bucket/stats-{n}.puffin"),
            offset: n,
        };

        let mut cache = super::ParsedIndexCache::default();
        for n in 0..3 {
            cache.put(key(n), Arc::clone(&index), size * 2, 128);
        }
        assert!(cache.get(&key(0)).is_none(), "oldest entry evicted");
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_some());

        let mut cache = super::ParsedIndexCache::default();
        for n in 0..3 {
            cache.put(key(n), Arc::clone(&index), usize::MAX, 2);
        }
        assert!(cache.get(&key(0)).is_none());
        assert!(cache.get(&key(1)).is_some());

        let mut cache = super::ParsedIndexCache::default();
        cache.put(key(0), Arc::clone(&index), usize::MAX, 2);
        cache.put(key(1), Arc::clone(&index), usize::MAX, 2);
        assert!(cache.get(&key(0)).is_some());
        cache.put(key(2), Arc::clone(&index), usize::MAX, 2);
        assert!(cache.get(&key(0)).is_some(), "used entry survives");
        assert!(cache.get(&key(1)).is_none(), "unused entry evicted");

        let mut cache = super::ParsedIndexCache::default();
        cache.put(key(0), Arc::clone(&index), size - 1, 128);
        assert!(cache.get(&key(0)).is_none());
        cache.put(key(0), Arc::clone(&index), usize::MAX, 128);
        cache.put(key(0), Arc::clone(&index), usize::MAX, 128);
        assert_eq!(cache.order.len(), 1);
        assert_eq!(cache.bytes, size);
        let hit = cache.get(&key(0)).unwrap();
        assert!(Arc::ptr_eq(&hit, &index));
        assert_eq!(cache.entries[&key(0)].hits, 1);
    }

    #[test]
    fn parsed_index_cache_keys_separate_storage_shapes_files_and_columns() {
        let raw = Arc::new(siglake_index::InvertedIndex::from_rows(["database timeout"]));
        let body = Arc::new(siglake_index::InvertedIndex::from_rows(["request complete"]));
        let footer = |path: &str, column: &str| super::ParsedIndexKey::FooterKv {
            path: path.to_string(),
            column: column.to_string(),
        };
        let puffin = |path: &str| super::ParsedIndexKey::Puffin {
            path: path.to_string(),
            offset: 0,
        };

        let mut cache = super::ParsedIndexCache::default();
        let path = "s3://bucket/data/00000.parquet";
        cache.put(footer(path, "raw"), Arc::clone(&raw), usize::MAX, 128);
        cache.put(footer(path, "body"), Arc::clone(&body), usize::MAX, 128);
        assert!(Arc::ptr_eq(
            &cache.get(&footer(path, "raw")).unwrap(),
            &raw,
        ));
        assert!(Arc::ptr_eq(
            &cache.get(&footer(path, "body")).unwrap(),
            &body,
        ));
        assert!(cache.get(&footer("s3://bucket/data/00001.parquet", "raw")).is_none());
        assert!(cache.get(&puffin(path)).is_none());
    }

    fn test_row_groups(sizes: &[u32]) -> Vec<RowGroupMetaData> {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(
                SchemaType::primitive_type_builder("raw", parquet::basic::Type::BYTE_ARRAY)
                    .build()
                    .unwrap(),
            )])
            .build()
            .unwrap();
        let schema: SchemaDescPtr = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let columns: Vec<ColumnChunkMetaData> = schema
            .columns()
            .iter()
            .map(|column| ColumnChunkMetaData::builder(column.clone()).build().unwrap())
            .collect();
        sizes
            .iter()
            .enumerate()
            .map(|(ordinal, rows)| {
                RowGroupMetaData::builder(Arc::clone(&schema))
                    .set_num_rows(i64::from(*rows))
                    .set_total_byte_size(0)
                    .set_column_metadata(columns.clone())
                    .set_ordinal(ordinal as i16)
                    .build()
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn index_matches_row_selection_agrees_with_the_complement_form() {
        let sizes = [1000, 500, 500, 1000, 500];
        let total_rows: u64 = sizes.iter().map(|rows| u64::from(*rows)).sum();
        let row_groups = test_row_groups(&sizes);
        let shapes: Vec<(&str, Vec<u32>)> = vec![
            ("empty", vec![]),
            ("all", (0..total_rows as u32).collect()),
            ("first row only", vec![0]),
            ("last row only", vec![3499]),
            ("row-group boundaries", vec![999, 1000, 1499, 1500, 1999, 2000]),
            (
                "runs and singletons",
                vec![1, 3, 4, 5, 998, 999, 1010, 1011, 1012, 2100, 2200, 2201, 2999, 3000],
            ),
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
            for selected in &selections {
                let mut complement = RoaringTreemap::new();
                complement.insert_range(0..total_rows);
                for ordinal in matching {
                    complement.remove(u64::from(*ordinal));
                }
                let expected = ArrowReader::build_deletes_row_selection(
                    &row_groups,
                    selected,
                    &DeleteVector::new(complement),
                )
                .unwrap();
                let actual = super::index_matches_row_selection(&row_groups, selected, matching);
                assert_eq!(actual, expected, "shape {label} over row groups {selected:?}");
            }
        }
    }

    #[tokio::test]
    async fn pruning_matrix_is_conservative_selective_and_composes_with_deletes() {
        let (_temp, metadata, task) = pruning_fixture();
        let database = RawPruneSpec {
            all_terms: vec!["database".to_string()],
            ..RawPruneSpec::default()
        };
        assert!(ArrowReader::file_might_match_prune_spec(
            &metadata, &database
        ));
        assert_eq!(
            ArrowReader::rowgroup_bloom_survivors_for_spec(&metadata, &database),
            Some(vec![0, 1])
        );
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &database,
            false,
        )
        .await
        .unwrap()
        .expect("inline index should answer a token query");
        assert_eq!(selection.row_count(), 3, "selective token pruning");

        let substring = RawPruneSpec {
            substrings: vec!["timeout".to_string()],
            index_substrings: vec!["timeout".to_string()],
            ..RawPruneSpec::default()
        };
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &substring,
            true,
        )
        .await
        .unwrap()
        .expect("inline index should answer a substring query when caches are bypassed");
        assert_eq!(selection.row_count(), 2, "selective substring pruning");

        let absent = RawPruneSpec {
            substrings: vec!["zzz-absent".to_string()],
            ..RawPruneSpec::default()
        };
        assert!(!ArrowReader::file_might_match_prune_spec(
            &metadata, &absent
        ));

        let any = RawPruneSpec {
            any_terms: vec!["database".to_string(), "heartbeat".to_string()],
            ..RawPruneSpec::default()
        };
        let selection = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &any,
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(selection.row_count(), 4, "OR terms use the postings union");

        let promoted = PromotedPruneSpec {
            column: "host".to_string(),
            values: vec!["alpha".to_string()],
        };
        assert!(ArrowReader::row_group_might_match_promoted(
            metadata.row_group(0),
            &promoted
        ));
        assert!(!ArrowReader::row_group_might_match_promoted(
            metadata.row_group(1),
            &promoted
        ));
        let pre_promotion = PromotedPruneSpec {
            column: "column_not_in_file".to_string(),
            values: vec!["alpha".to_string()],
        };
        assert!(ArrowReader::row_group_might_match_promoted(
            metadata.row_group(0),
            &pre_promotion
        ));
        assert!(!ArrowReader::stamped_row_group_size_matches(&metadata, 3));

        let mut deleted = roaring::RoaringTreemap::new();
        deleted.insert(0);
        let delete_selection = ArrowReader::build_deletes_row_selection(
            metadata.row_groups(),
            &None,
            &DeleteVector::new(deleted),
        )
        .unwrap();
        let indexed = ArrowReader::inverted_index_row_selection(
            &FileIO::new_with_fs(),
            &task,
            &metadata,
            &None,
            &database,
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            indexed.intersection(&delete_selection).row_count(),
            2,
            "index selection intersects positional deletes"
        );
    }

    #[tokio::test]
    async fn pipeline_results_match_with_cold_warm_and_bypassed_caches() {
        use std::sync::atomic::Ordering::Relaxed;

        use arrow_array::cast::AsArray;
        use futures::TryStreamExt;

        use crate::Runtime;
        use crate::arrow::{ArrowReaderBuilder, ScanCounters};
        use crate::scan::FileScanTaskStream;

        async fn read(
            task: FileScanTask,
            raw: Option<RawPruneSpec>,
            promoted: Vec<PromotedPruneSpec>,
            cache_bypass: bool,
        ) -> (Vec<(String, String)>, Arc<ScanCounters>) {
            let counters = Arc::new(ScanCounters::default());
            let reader = ArrowReaderBuilder::new(FileIO::new_with_fs(), Runtime::current())
                .with_raw_prune_spec(raw)
                .with_promoted_prune(promoted)
                .with_cache_bypass(cache_bypass)
                .with_scan_counters(Some(counters.clone()))
                .build();
            let tasks = Box::pin(futures::stream::iter([Ok(task)])) as FileScanTaskStream;
            let batches: Vec<RecordBatch> = reader
                .read(tasks)
                .unwrap()
                .stream()
                .try_collect()
                .await
                .unwrap();
            let rows = batches
                .iter()
                .flat_map(|batch| {
                    let raw = batch.column(0).as_string::<i32>();
                    let host = batch.column(1).as_string::<i32>();
                    (0..batch.num_rows())
                        .map(|row| (raw.value(row).to_string(), host.value(row).to_string()))
                        .collect::<Vec<_>>()
                })
                .collect();
            (rows, counters)
        }

        let (_temp, _metadata, task) = pruning_fixture();
        let (baseline, _) = read(task.clone(), None, vec![], true).await;
        assert_eq!(baseline.len(), 8);
        let database = RawPruneSpec {
            all_terms: vec!["database".to_string()],
            ..RawPruneSpec::default()
        };
        for cache_bypass in [false, false, true] {
            let (pruned, counters) =
                read(task.clone(), Some(database.clone()), vec![], cache_bypass).await;
            let exact_baseline: Vec<_> = baseline
                .iter()
                .filter(|(raw, _)| raw.contains("database"))
                .cloned()
                .collect();
            assert_eq!(pruned, exact_baseline);
            assert_eq!(counters.rows_pruned_selection.load(Relaxed), 5);
        }

        let promoted = PromotedPruneSpec {
            column: "host".to_string(),
            values: vec!["alpha".to_string()],
        };
        let (hinted, counters) = read(task, None, vec![promoted], false).await;
        let exact_hinted: Vec<_> = hinted
            .iter()
            .filter(|(_, host)| host == "alpha")
            .cloned()
            .collect();
        let exact_baseline: Vec<_> = baseline
            .iter()
            .filter(|(_, host)| host == "alpha")
            .cloned()
            .collect();
        assert_eq!(exact_hinted, exact_baseline);
        assert_eq!(counters.row_groups_pruned_stats.load(Relaxed), 1);
    }

    #[tokio::test]
    async fn puffin_index_is_identical_cold_warm_and_bypassed_and_refuses_stale_stamp() {
        let (_temp, metadata, mut task) = pruning_fixture();
        let file_io = FileIO::new_with_fs();
        let puffin_path = std::path::Path::new(task.data_file_path())
            .with_extension("puffin")
            .to_string_lossy()
            .into_owned();
        let output = file_io.new_output(&puffin_path).unwrap();
        let properties = HashMap::from([
            ("data_file".to_string(), task.data_file_path().to_string()),
            ("column".to_string(), "raw".to_string()),
        ]);
        let index = siglake_index::InvertedIndex::from_rows([
            "database timeout",
            "healthy startup",
            "worker heartbeat",
            "database migrated",
            "api timeout",
            "healthy shutdown",
            "database retry",
            "worker complete",
        ]);
        let blob = Blob::builder()
            .r#type("siglake-inverted-v1".to_string())
            .fields(vec![1])
            .snapshot_id(1)
            .sequence_number(1)
            .data(index.to_bytes())
            .properties(properties.clone())
            .build();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        writer
            .add(blob, CompressionCodec::zstd_default())
            .await
            .unwrap();
        writer.close().await.unwrap();

        task.statistics_blobs = vec![StatisticsBlobReference {
            statistics_path: puffin_path,
            blob_type: "siglake-inverted-v1".to_string(),
            properties: properties
                .into_iter()
                .chain([("row_group_size".to_string(), "4".to_string())])
                .collect(),
        }];
        for cache_bypass in [false, false, true] {
            let index =
                ArrowReader::puffin_inverted_index(&file_io, &task, &metadata, "raw", cache_bypass)
                    .await
                    .unwrap()
                    .expect("valid Puffin index");
            assert_eq!(index.matching_rows_all(&["database"]), [0, 3, 6]);
        }

        task.statistics_blobs[0]
            .properties
            .insert("row_group_size".to_string(), "3".to_string());
        assert!(
            ArrowReader::puffin_inverted_index(&file_io, &task, &metadata, "raw", false)
                .await
                .unwrap()
                .is_none(),
            "a stale row-group stamp must fall back to the exact scan"
        );
    }

    // ----------------------------------------------------------------------
    // siglake #4561: bounded partial reads of a segmented inverted-index
    // sidecar, and #5007's staged driver for them. The prototype is off by
    // default; these drive it directly.
    // ----------------------------------------------------------------------

    /// The measurement corpus in miniature: shared tokens, a 2%-density
    /// `queen`, a `checkout` every twentieth row, one token unique to each row
    /// and a `rareneedle` on one row in 997 — the generator
    /// `docs/DESIGN_segmented_inverted_index.md`'s tables were taken on.
    fn segmented_corpus(rows: usize) -> Vec<String> {
        (0..rows)
            .map(|row| {
                let queen = if row % 50 == 0 { " queen" } else { "" };
                let checkout = if row % 20 == 0 { " checkout" } else { "" };
                let rare = if row % 997 == 0 { " rareneedle" } else { "" };
                format!(
                    "service-{} status {}{queen}{checkout}{rare} row-{row:06}",
                    row % 20,
                    200 + row % 5
                )
            })
            .collect()
    }

    /// A Puffin statistics file carrying **both** sidecar formats for the same
    /// data file and column: the segmented one uncompressed (its interior has
    /// to be addressable) beside a Zstd v1 blob, which is how a table would
    /// carry a mixture while the prototype is being measured.
    async fn write_mixed_sidecar(
        dir: &TempDir,
        rows: &[String],
        group_rows: u32,
    ) -> (FileIO, String) {
        write_mixed_sidecar_for(dir, rows, group_rows, "file:///data/0.parquet").await
    }

    async fn write_mixed_sidecar_for(
        dir: &TempDir,
        rows: &[String],
        group_rows: u32,
        data_file: &str,
    ) -> (FileIO, String) {
        let file_io = FileIO::new_with_fs();
        let path = format!("{}/sidecar.puffin", dir.path().to_str().unwrap());
        let output = file_io.new_output(&path).unwrap();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        let properties = HashMap::from([
            ("data_file".to_string(), data_file.to_string()),
            ("column".to_string(), "raw".to_string()),
            ("tokenizer".to_string(), "simple".to_string()),
        ]);
        let mut segmented_properties = properties.clone();
        segmented_properties.insert("format".to_string(), "seg1".to_string());
        writer
            .add(
                Blob::builder()
                    .r#type(siglake_index::segmented::SEGMENTED_BLOB_TYPE.to_string())
                    .fields(vec![1])
                    .snapshot_id(1)
                    .sequence_number(1)
                    .data(siglake_index::segmented::encode_from_rows(
                        rows.iter().map(String::as_str),
                        group_rows,
                    ))
                    .properties(segmented_properties)
                    .build(),
                CompressionCodec::None,
            )
            .await
            .unwrap();
        let mut v1_properties = properties.clone();
        v1_properties.insert("format".to_string(), "v1".to_string());
        v1_properties.insert("row_group_size".to_string(), group_rows.to_string());
        writer
            .add(
                Blob::builder()
                    .r#type("siglake-inverted-v1".to_string())
                    .fields(vec![1])
                    .snapshot_id(1)
                    .sequence_number(1)
                    .data(
                        siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str))
                            .to_bytes(),
                    )
                    .properties(v1_properties)
                    .build(),
                CompressionCodec::zstd_default(),
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        (file_io, path)
    }

    /// The statistics file a rewrite writes today: a seg2 blob for the data
    /// file and column, uncompressed because `PuffinReader::blob_range_reader`
    /// will only address the interior of an uncompressed blob, beside the
    /// Zstd v1 blob a table still carries. Properties are the ones
    /// `siglake-storage` writes (`iceberg.rs` `write_statistics_file`),
    /// including `format` = `seg2`.
    async fn write_seg2_sidecar_for(
        dir: &TempDir,
        rows: &[String],
        group_rows: u32,
        data_file: &str,
    ) -> (FileIO, String) {
        let file_io = FileIO::new_with_fs();
        let path = format!("{}/seg2.puffin", dir.path().to_str().unwrap());
        let output = file_io.new_output(&path).unwrap();
        let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
            .await
            .unwrap();
        let properties = HashMap::from([
            ("data_file".to_string(), data_file.to_string()),
            ("column".to_string(), "raw".to_string()),
            ("tokenizer".to_string(), "default".to_string()),
        ]);
        let mut segmented = siglake_index::segmented::SegmentedWriter::new_v2(
            siglake_index::segmented::DEFAULT_TARGET_BLOCK_BYTES,
        );
        for chunk in rows.chunks(group_rows as usize) {
            segmented.push_group_rows(chunk.iter().map(String::as_str));
        }
        let mut seg2_properties = properties.clone();
        seg2_properties.insert(
            "format".to_string(),
            siglake_index::segmented::SEGMENTED_V2_FORMAT_PROPERTY.to_string(),
        );
        // The blob type is a discovery key only: the decoder dispatches on the
        // trailer's version byte and answers a seg1 payload registered under
        // the seg2 type just as readily. Pin what this fixture encodes, or the
        // port goes back to testing seg1 without failing.
        let bytes = segmented.finish();
        assert_eq!(
            siglake_index::segmented::SegmentedReader::open(
                siglake_index::segmented::SliceSource::new(bytes.clone())
            )
            .unwrap()
            .directory()
            .format_property(),
            siglake_index::segmented::SEGMENTED_V2_FORMAT_PROPERTY
        );
        writer
            .add(
                Blob::builder()
                    .r#type(siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE.to_string())
                    .fields(vec![1])
                    .snapshot_id(1)
                    .sequence_number(1)
                    .data(bytes)
                    .properties(seg2_properties)
                    .build(),
                CompressionCodec::None,
            )
            .await
            .unwrap();
        let mut v1_properties = properties;
        v1_properties.insert("format".to_string(), "v1".to_string());
        v1_properties.insert("row_group_size".to_string(), group_rows.to_string());
        writer
            .add(
                Blob::builder()
                    .r#type("siglake-inverted-v1".to_string())
                    .fields(vec![1])
                    .snapshot_id(1)
                    .sequence_number(1)
                    .data(
                        siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str))
                            .to_bytes(),
                    )
                    .properties(v1_properties)
                    .build(),
                CompressionCodec::zstd_default(),
            )
            .await
            .unwrap();
        writer.close().await.unwrap();
        (file_io, path)
    }

    async fn sidecar_blob(file_io: &FileIO, path: &str, blob_type: &str) -> BlobMetadata {
        sidecar_blob_for(file_io, path, blob_type, "file:///data/0.parquet").await
    }

    async fn sidecar_blob_for(
        file_io: &FileIO,
        path: &str,
        blob_type: &str,
        data_file: &str,
    ) -> BlobMetadata {
        PuffinReader::new(file_io.new_input(path).unwrap())
            .with_cache_bypass(true)
            .file_metadata()
            .await
            .unwrap()
            .blobs()
            .iter()
            .find(|blob| {
                blob.blob_type() == blob_type
                    && blob.properties().get("data_file").map(String::as_str) == Some(data_file)
                    && blob.properties().get("column").map(String::as_str) == Some("raw")
            })
            .cloned()
            .expect("the sidecar carries this blob type")
    }

    fn row_counts(rows: usize, group_rows: usize) -> Vec<u64> {
        let mut counts = Vec::new();
        let mut remaining = rows;
        while remaining > 0 {
            let group = remaining.min(group_rows);
            counts.push(group as u64);
            remaining -= group;
        }
        counts
    }

    fn all_terms_spec(terms: &[&str]) -> RawPruneSpec {
        RawPruneSpec {
            all_terms: terms.iter().map(|term| term.to_string()).collect(),
            ..RawPruneSpec::default()
        }
    }

    #[tokio::test]
    async fn a_segmented_sidecar_answers_a_term_from_a_sliver_of_the_blob() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(20_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 5_000).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let blob_len = blob.length();
        let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));

        let (outcome, cost) = segmented_matching_rows(
            &file_io,
            &path,
            &blob,
            &row_counts(20_000, 5_000),
            None,
            &all_terms_spec(&["rareneedle"]),
            true,
        )
        .await
        .unwrap();

        let SegmentedOutcome::Matching {
            rows: matching,
            resident_bytes,
        } = outcome
        else {
            panic!("the term is in the sidecar: {outcome:?}");
        };
        assert_eq!(matching, v1.postings("rareneedle").unwrap().to_vec());
        println!(
            "rare term over {blob_len} blob bytes: {} range reads, {} fetched, \
             {resident_bytes} resident (v1 parses to {})",
            cost.reads,
            cost.bytes,
            v1.heap_size_bytes()
        );
        assert!(
            cost.bytes * 20 < blob_len,
            "a point lookup fetched {} of {blob_len} bytes",
            cost.bytes
        );
        assert!(
            (resident_bytes as u64) * 10 < blob_len,
            "the reader kept {resident_bytes} bytes of a {blob_len}-byte blob"
        );
        assert!(
            resident_bytes * 10 < v1.heap_size_bytes(),
            "resident {resident_bytes} against a parsed v1 index of {}",
            v1.heap_size_bytes()
        );
    }

    /// A second lookup on the same `(statistics file, blob offset)` reads
    /// neither the trailer nor the directory (#5006), and the two shipped
    /// text-index caches are untouched by either lookup.
    #[tokio::test]
    async fn a_second_lookup_on_the_same_blob_reads_no_directory() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(20_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 5_000).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(20_000, 5_000);
        let spec = all_terms_spec(&["rareneedle"]);

        // What the open costs on this blob, priced the way #4561's table
        // prices it: an empty group selection reads the trailer and the
        // directory and nothing else.
        let (_, open) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, Some(&[]), &spec, true)
                .await
                .unwrap();
        assert_eq!(open.reads, 2);

        let (cold, cold_cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, false)
                .await
                .unwrap();
        let (warm, warm_cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, false)
                .await
                .unwrap();

        let (
            SegmentedOutcome::Matching {
                rows: cold_rows,
                resident_bytes: cold_resident,
            },
            SegmentedOutcome::Matching {
                rows: warm_rows,
                resident_bytes: warm_resident,
            },
        ) = (cold, warm)
        else {
            panic!("both lookups answer");
        };
        assert_eq!(cold_rows, warm_rows, "the same answer, warm");
        assert_eq!(
            warm_cost.reads,
            cold_cost.reads - open.reads,
            "the warm lookup read the term's ranges and nothing else"
        );
        assert_eq!(warm_cost.bytes, cold_cost.bytes - open.bytes);
        // Two of the cold lookup's four rounds of store waits are the open, so
        // a held directory takes two (#5007).
        assert_eq!(cold_cost.stages, 4);
        assert_eq!(warm_cost.stages, 2);
        // The resident cost does not disappear when it is paid once: both
        // lookups report it, which is what #4562's warm arm reports as memory.
        assert_eq!(warm_resident, cold_resident);
        assert!(warm_resident > 0);
        assert_eq!(segmented_directory_cache_stats(&path), (1, 1));

        // A bypassing lookup neither reads the held directory nor adds one.
        let (_, bypassed) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, true)
                .await
                .unwrap();
        assert_eq!(bypassed.reads, cold_cost.reads);
        assert_eq!(segmented_directory_cache_stats(&path), (1, 1));

        // And this path spends none of the two shipped caches' budgets: it
        // holds no parsed index and no blob bytes.
        assert_eq!(parsed_inverted_index_cache_stats(&path), (0, 0));
        let (blob_entries, blob_bytes, _) = crate::arrow::puffin_blob_cache_stats(&path);
        assert_eq!((blob_entries, blob_bytes), (0, 0));
    }

    /// Two sidecars are two entries: a held directory answers for the blob it
    /// was parsed from and no other.
    #[tokio::test]
    async fn held_directories_are_per_blob_identity() {
        let first_dir = TempDir::new().unwrap();
        let second_dir = TempDir::new().unwrap();
        let first_rows = segmented_corpus(4_000);
        let second_rows = segmented_corpus(2_000);
        let (first_io, first_path) = write_mixed_sidecar(&first_dir, &first_rows, 1_000).await;
        let (second_io, second_path) = write_mixed_sidecar(&second_dir, &second_rows, 500).await;
        let first_blob = sidecar_blob(&first_io, &first_path, SEGMENTED_BLOB_TYPE).await;
        let second_blob = sidecar_blob(&second_io, &second_path, SEGMENTED_BLOB_TYPE).await;
        let spec = all_terms_spec(&["rareneedle"]);
        let v1_first =
            siglake_index::InvertedIndex::from_rows(first_rows.iter().map(String::as_str));
        let v1_second =
            siglake_index::InvertedIndex::from_rows(second_rows.iter().map(String::as_str));

        for _ in 0..2 {
            for (io, path, blob, counts, expected) in [
                (
                    &first_io,
                    &first_path,
                    &first_blob,
                    row_counts(4_000, 1_000),
                    v1_first.postings("rareneedle").unwrap().to_vec(),
                ),
                (
                    &second_io,
                    &second_path,
                    &second_blob,
                    row_counts(2_000, 500),
                    v1_second.postings("rareneedle").unwrap().to_vec(),
                ),
            ] {
                let (outcome, _) =
                    segmented_matching_rows(io, path, blob, &counts, None, &spec, false)
                        .await
                        .unwrap();
                let SegmentedOutcome::Matching { rows, .. } = outcome else {
                    panic!("{path}: {outcome:?}");
                };
                assert_eq!(rows, expected, "{path}");
            }
        }
        assert_eq!(segmented_directory_cache_stats(&first_path), (1, 1));
        assert_eq!(segmented_directory_cache_stats(&second_path), (1, 1));

        // The row domain is re-checked against the caller's file on every
        // lookup, held directory or not: the directory describes the blob, not
        // the Parquet file it is being applied to.
        let (outcome, _) = segmented_matching_rows(
            &first_io,
            &first_path,
            &first_blob,
            &[2_000, 2_000],
            None,
            &spec,
            false,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, SegmentedOutcome::Declined("row_domain")),
            "{outcome:?}"
        );
    }

    /// A held directory that is not this blob's is not a reason to decline the
    /// file: the lookup reads the blob's own directory and answers.
    #[tokio::test]
    async fn a_directory_that_is_not_this_blobs_falls_back_to_reading_it() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(4_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 1_000).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(4_000, 1_000);
        let spec = all_terms_spec(&["rareneedle"]);
        let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));

        // A directory parsed from a different blob, planted under this blob's
        // identity — the shape a mis-keyed or stale entry would have.
        let other = siglake_index::segmented::encode_from_rows(
            segmented_corpus(2_000).iter().map(String::as_str),
            500,
        );
        let planted = siglake_index::segmented::SegmentedDirectory::read(
            &siglake_index::segmented::SliceSource::new(other),
        )
        .expect("the fixture blob parses");
        segmented_directory_cache_put(&path, blob.offset(), Arc::new(planted));

        let (outcome, cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, false)
                .await
                .unwrap();
        let SegmentedOutcome::Matching { rows: matching, .. } = outcome else {
            panic!("the blob's own directory answers: {outcome:?}");
        };
        assert_eq!(matching, v1.postings("rareneedle").unwrap().to_vec());
        assert!(
            cost.reads >= 2,
            "it read the trailer and the directory itself: {} reads",
            cost.reads
        );
    }

    #[test]
    fn the_directory_cache_evicts_by_bytes_and_skips_an_oversized_entry() {
        fn directory(
            rows: usize,
            group_rows: u32,
        ) -> Arc<siglake_index::segmented::SegmentedDirectory> {
            let corpus = segmented_corpus(rows);
            let blob = siglake_index::segmented::encode_from_rows(
                corpus.iter().map(String::as_str),
                group_rows,
            );
            Arc::new(
                siglake_index::segmented::SegmentedDirectory::read(
                    &siglake_index::segmented::SliceSource::new(blob),
                )
                .expect("the fixture blob parses"),
            )
        }

        let small = directory(1_000, 500);
        let key_size = std::mem::size_of::<super::SegmentedDirectoryKey>();
        let entry_size = small.resident_bytes() + "/sidecar-0.puffin".len() + key_size;
        let budget = entry_size * 2;
        let mut cache = super::SegmentedDirectoryCache::default();
        for index in 0..3 {
            cache.put(
                (format!("/sidecar-{index}.puffin"), 0),
                Arc::clone(&small),
                budget,
            );
        }
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.evictions, 1);
        assert!(cache.bytes <= budget);
        assert!(cache.get(&("/sidecar-0.puffin".to_string(), 0)).is_none());
        assert!(cache.get(&("/sidecar-2.puffin".to_string(), 0)).is_some());

        let before = cache.bytes;
        cache.put(
            ("/huge.puffin".to_string(), 0),
            directory(1_000, 500),
            small.resident_bytes() / 2,
        );
        assert_eq!(cache.oversized_skips, 1);
        assert_eq!(cache.bytes, before, "nothing admitted, nothing evicted");
        assert_eq!(cache.evictions, 1);

        let held = cache.bytes;
        let entries = cache.entries.len();
        cache.put(("/sidecar-2.puffin".to_string(), 0), small, budget);
        assert_eq!((cache.bytes, cache.entries.len()), (held, entries));
    }

    #[tokio::test]
    async fn a_segmented_lookup_reads_only_the_row_groups_the_scan_kept() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(20_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 5_000).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(20_000, 5_000);
        let spec = all_terms_spec(&["queen"]);

        let (whole, whole_cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, true)
                .await
                .unwrap();
        let (kept, kept_cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, Some(&[2, 3]), &spec, true)
                .await
                .unwrap();

        let (
            SegmentedOutcome::Matching { rows: whole, .. },
            SegmentedOutcome::Matching { rows: kept, .. },
        ) = (whole, kept)
        else {
            panic!("both lookups answer");
        };
        assert_eq!(
            kept,
            whole
                .iter()
                .copied()
                .filter(|row| (10_000..20_000).contains(row))
                .collect::<Vec<_>>(),
            "the restriction is exactly those groups' rows, file-physical"
        );
        // A rejected group costs no read at all: the trailer and the
        // directory, then one dictionary block and one posting section per
        // kept group.
        assert_eq!(whole_cost.reads, 2 + 4 * 2);
        assert_eq!(kept_cost.reads, 2 + 2 * 2);
        assert!(
            kept_cost.bytes < whole_cost.bytes,
            "two of four groups fetched {} bytes against {}",
            kept_cost.bytes,
            whole_cost.bytes
        );
        // An empty selection is the caller having pruned everything, not a
        // malformed one: a definitive no-match, and no read at all.
        let (empty, empty_cost) =
            segmented_matching_rows(&file_io, &path, &blob, &counts, Some(&[]), &spec, true)
                .await
                .unwrap();
        assert!(
            matches!(&empty, SegmentedOutcome::Matching { rows, .. } if rows.is_empty()),
            "{empty:?}"
        );
        assert_eq!(empty_cost.reads, 2, "the trailer and the directory only");
    }

    #[tokio::test]
    async fn and_or_and_substring_agree_with_the_whole_file_index() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(4_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 512).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(4_000, 512);
        let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));

        // Terms whose postings straddle every row-group boundary, terms
        // confined to one group, and one the file does not have.
        let specs = [
            all_terms_spec(&["queen", "checkout"]),
            all_terms_spec(&["rareneedle"]),
            all_terms_spec(&["queen", "absentterm"]),
            RawPruneSpec {
                any_terms: vec!["rareneedle".to_string(), "000513".to_string()],
                ..RawPruneSpec::default()
            },
            RawPruneSpec {
                all_terms: vec!["checkout".to_string()],
                any_terms: vec!["queen".to_string(), "absentterm".to_string()],
                ..RawPruneSpec::default()
            },
            RawPruneSpec {
                index_substrings: vec!["ueen".to_string()],
                ..RawPruneSpec::default()
            },
            RawPruneSpec {
                all_terms: vec!["checkout".to_string()],
                index_substrings: vec!["ueen".to_string()],
                ..RawPruneSpec::default()
            },
        ];

        for spec in specs {
            let mut expected: Option<Vec<u32>> = None;
            if !spec.all_terms.is_empty() {
                let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
                expected = Some(v1.matching_rows_all(&terms));
            }
            if !spec.any_terms.is_empty() {
                let any = spec
                    .any_terms
                    .iter()
                    .fold(Vec::new(), |acc: Vec<u32>, term| {
                        union_sorted(&acc, v1.postings(term).unwrap_or(&[]))
                    });
                expected = Some(match expected {
                    Some(existing) => intersect_sorted(&existing, &any),
                    None => any,
                });
            }
            for substr in &spec.index_substrings {
                let rows = v1.rows_containing(substr).unwrap();
                expected = Some(match expected {
                    Some(existing) => intersect_sorted(&existing, &rows),
                    None => rows,
                });
            }

            let (outcome, _) =
                segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, true)
                    .await
                    .unwrap();
            let SegmentedOutcome::Matching { rows: matching, .. } = outcome else {
                panic!("{spec:?} is answerable: {outcome:?}");
            };
            assert_eq!(matching, expected.unwrap(), "{spec:?}");
            assert!(
                matching.windows(2).all(|pair| pair[0] < pair[1]),
                "file-physical ordinals, strictly ascending: {spec:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_term_the_sidecar_cannot_answer_declines_the_file() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(2_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 512).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(2_000, 512);

        // A term that does not normalize is not "no rows match" — the v1
        // reader's per-term union would drop it and prune rows it might have
        // matched.
        for spec in [
            all_terms_spec(&["qu"]),
            RawPruneSpec {
                any_terms: vec!["queen".to_string(), "qu".to_string()],
                ..RawPruneSpec::default()
            },
        ] {
            let (outcome, _) =
                segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, true)
                    .await
                    .unwrap();
            assert!(
                matches!(outcome, SegmentedOutcome::Declined("unanswerable")),
                "{spec:?}: {outcome:?}"
            );
        }

        // A spec with no hints concludes nothing rather than selecting no rows.
        let (outcome, _) = segmented_matching_rows(
            &file_io,
            &path,
            &blob,
            &counts,
            None,
            &RawPruneSpec::default(),
            true,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, SegmentedOutcome::Declined("no_hints")),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_sidecar_that_is_not_about_this_files_row_groups_is_declined() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(2_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 512).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let spec = all_terms_spec(&["queen"]);

        // The file's real layout is 512, 512, 512, 464. A whole-file index
        // checked against one stamped `row_group_size` accepts the first of
        // these — every group but the last is 512 rows — and the second has
        // the file's group count with the wrong domain; the directory states
        // every group, so both are refused before anything is pruned.
        for counts in [
            vec![512, 512, 512, 512],
            vec![512, 512, 464, 512],
            vec![1_000, 1_000],
            vec![2_000],
        ] {
            let (outcome, _) =
                segmented_matching_rows(&file_io, &path, &blob, &counts, None, &spec, true)
                    .await
                    .unwrap();
            assert!(
                matches!(outcome, SegmentedOutcome::Declined("row_domain")),
                "{counts:?}: {outcome:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_compressed_sidecar_has_no_addressable_interior() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(2_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 512).await;
        // The same statistics file carries both formats for the same column,
        // each discovered by its own blob type.
        let v1_blob = sidecar_blob(&file_io, &path, "siglake-inverted-v1").await;
        let segmented = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        assert_eq!(
            v1_blob.properties().get("format").map(String::as_str),
            Some("v1")
        );
        assert_eq!(
            segmented.properties().get("format").map(String::as_str),
            Some("seg1")
        );
        assert!(segmented.properties().get("row_group_size").is_none());

        let (outcome, cost) = segmented_matching_rows(
            &file_io,
            &path,
            &v1_blob,
            &row_counts(2_000, 512),
            None,
            &all_terms_spec(&["queen"]),
            true,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, SegmentedOutcome::Declined("compressed")),
            "{outcome:?}"
        );
        assert_eq!(cost.reads, 0, "declined before reading anything");
    }

    #[tokio::test]
    async fn a_corrupt_segmented_sidecar_declines_rather_than_dropping_rows() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(2_000);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, 512).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(2_000, 512);
        let spec = all_terms_spec(&["queen"]);

        let mut bytes = std::fs::read(path.trim_start_matches("file://")).unwrap();
        // A flipped bit inside the blob's body: a dictionary block's CRC no
        // longer matches, so the lookup is unanswerable rather than a group
        // that quietly does not have the term.
        let target = (blob.offset() + blob.length() / 2) as usize;
        bytes[target] ^= 0x40;
        let corrupt = format!("{}/corrupt.puffin", dir.path().to_str().unwrap());
        std::fs::write(corrupt.trim_start_matches("file://"), &bytes).unwrap();

        let (outcome, _) =
            segmented_matching_rows(&file_io, &corrupt, &blob, &counts, None, &spec, true)
                .await
                .unwrap();
        // Whichever section the flip landed in, the one thing that must not
        // happen is a shorter answer: a partial reader that reads a corrupt
        // block as "this group does not have the term" drops rows the scan
        // then never decodes.
        if let SegmentedOutcome::Matching { rows: matching, .. } = &outcome {
            let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));
            assert_eq!(
                matching,
                &v1.postings("queen").unwrap().to_vec(),
                "a corrupt sidecar answered, and answered wrong"
            );
        }

        // And a blob whose trailer is gone does not open at all.
        let mut truncated = bytes.clone();
        let end = (blob.offset() + blob.length()) as usize;
        truncated[end - 3] ^= 0xff;
        let truncated_path = format!("{}/truncated.puffin", dir.path().to_str().unwrap());
        std::fs::write(truncated_path.trim_start_matches("file://"), &truncated).unwrap();
        let (outcome, cost) =
            segmented_matching_rows(&file_io, &truncated_path, &blob, &counts, None, &spec, true)
                .await
                .unwrap();
        assert!(
            matches!(outcome, SegmentedOutcome::Declined("open")),
            "{outcome:?}"
        );
        assert_eq!(cost.reads, 1, "the trailer, and nothing after it");
    }

    /// What a text predicate costs through the reader's segmented path, per
    /// shape: the ranges it asked the object store for, the bytes in them, and
    /// what it keeps afterwards. The codec-level tables in
    /// `docs/DESIGN_segmented_inverted_index.md` are the same quantities
    /// measured without the Puffin container; this one measures them through
    /// it, which is what #4561 integrates.
    ///
    /// Sized by `SIGLAKE_SEG_READER_ROWS` and `SIGLAKE_SEG_READER_GROUPS`.
    /// Run it from a kept fork mirror (`scripts/check-fork-tests.sh --fork
    /// iceberg --keep`):
    ///
    /// ```text
    /// cargo test --release --lib report_segmented_reader_read_cost -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "measurement, minutes"]
    async fn report_segmented_reader_read_cost() {
        fn knob(name: &str, fallback: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.trim().parse().ok())
                .unwrap_or(fallback)
        }
        let n_rows = knob("SIGLAKE_SEG_READER_ROWS", 1_000_000);
        let n_groups = knob("SIGLAKE_SEG_READER_GROUPS", 8);
        let group_rows = n_rows.div_ceil(n_groups);

        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(n_rows);
        let (file_io, path) = write_mixed_sidecar(&dir, &rows, group_rows as u32).await;
        let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
        let counts = row_counts(n_rows, group_rows);
        let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let last_quarter: Vec<usize> = (counts.len() - counts.len() / 4..counts.len()).collect();

        println!(
            "\n{n_rows} rows in {} row groups: segmented blob {} B, v1 parsed {} B",
            counts.len(),
            blob.length(),
            v1.heap_size_bytes()
        );
        // The `cold` columns below include the open; the `warm` ones are the
        // same lookup with the directory held (#5006), which is the deployed
        // cost once a file has been queried once. An empty group selection is
        // a definitive no-match, so it costs the open and nothing else —
        // which is how the open is priced here.
        let (_, open) = segmented_matching_rows(
            &file_io,
            &path,
            &blob,
            &counts,
            Some(&[]),
            &all_terms_spec(&["rareneedle"]),
            true,
        )
        .await
        .unwrap();
        println!(
            "cold open (trailer + directory): {} reads, {} B",
            open.reads, open.bytes
        );
        println!(
            "shape                 rows  cold reads  cold fetched   ÷ blob  \
             warm reads  warm fetched   ÷ blob   resident"
        );

        let shapes: Vec<(&str, RawPruneSpec, Option<&[usize]>)> = vec![
            ("rare", all_terms_spec(&["rareneedle"]), None),
            (
                "rare_last25",
                all_terms_spec(&["rareneedle"]),
                Some(last_quarter.as_slice()),
            ),
            ("keyword", all_terms_spec(&["queen"]), None),
            ("unique_token", all_terms_spec(&["000001"]), None),
            (
                "and_rare_keyword",
                all_terms_spec(&["queen", "rareneedle"]),
                None,
            ),
            (
                "or_rare_unique",
                RawPruneSpec {
                    any_terms: vec!["rareneedle".to_string(), "000001".to_string()],
                    ..RawPruneSpec::default()
                },
                None,
            ),
            (
                "substring_sweep",
                RawPruneSpec {
                    index_substrings: vec!["ueen".to_string()],
                    ..RawPruneSpec::default()
                },
                None,
            ),
        ];

        // #5007's columns, printed as a second table below: the rounds of
        // store waits against the reads a serial source would have waited for
        // one at a time, and what the replay re-decodes on the way.
        let mut staged: Vec<(&str, SegmentedReadCost, SegmentedReadCost, u128, u128)> = Vec::new();
        for (name, spec, groups) in shapes {
            let started = std::time::Instant::now();
            let (outcome, cost) =
                segmented_matching_rows(&file_io, &path, &blob, &counts, groups, &spec, true)
                    .await
                    .unwrap();
            let cold_micros = started.elapsed().as_micros();
            let SegmentedOutcome::Matching {
                rows: matching,
                resident_bytes,
            } = outcome
            else {
                panic!("{name}: {outcome:?}");
            };
            // Exact against the whole-file index, restricted to the groups the
            // shape kept, before any cost is reported.
            let mut expected: Option<Vec<u32>> = None;
            if !spec.all_terms.is_empty() {
                let terms: Vec<&str> = spec.all_terms.iter().map(String::as_str).collect();
                expected = Some(v1.matching_rows_all(&terms));
            }
            if !spec.any_terms.is_empty() {
                let any = spec
                    .any_terms
                    .iter()
                    .fold(Vec::new(), |acc: Vec<u32>, term| {
                        union_sorted(&acc, v1.postings(term).unwrap_or(&[]))
                    });
                expected = Some(any);
            }
            for substr in &spec.index_substrings {
                expected = Some(v1.rows_containing(substr).unwrap());
            }
            let mut expected = expected.unwrap();
            if let Some(groups) = groups {
                let first_row: u64 = counts[..groups[0]].iter().sum();
                expected.retain(|row| u64::from(*row) >= first_row);
            }
            assert_eq!(matching, expected, "{name}");
            // The warm arm: the first of these parses the directory and holds
            // it, the second runs on the held one. Both answer the same rows
            // as the cold arm, which is asserted before either is reported.
            let mut warm = SegmentedReadCost::default();
            let mut warm_micros = 0u128;
            for _ in 0..2 {
                let started = std::time::Instant::now();
                let (outcome, cost) =
                    segmented_matching_rows(&file_io, &path, &blob, &counts, groups, &spec, false)
                        .await
                        .unwrap();
                warm_micros = started.elapsed().as_micros();
                let SegmentedOutcome::Matching {
                    rows: warm_rows, ..
                } = outcome
                else {
                    panic!("{name} warm: {outcome:?}");
                };
                assert_eq!(warm_rows, expected, "{name} warm");
                warm = cost;
            }
            println!(
                "{name:<20} {:>8} {:>11} {:>13} {:>7.3}% {:>11} {:>13} {:>7.3}% {:>10}",
                matching.len(),
                cost.reads,
                cost.bytes,
                100.0 * cost.bytes as f64 / blob.length() as f64,
                warm.reads,
                warm.bytes,
                100.0 * warm.bytes as f64 / blob.length() as f64,
                resident_bytes
            );
            staged.push((name, cost, warm, cold_micros, warm_micros));
        }
        println!(
            "\nstaged reading (#5007): stages are the rounds of store waits; reader reads are \
             what each round re-decodes\nshape                 cold stages  cold reads  \
             cold reader reads  cold µs  warm stages  warm reads  warm reader reads  warm µs"
        );
        for (name, cold, warm, cold_micros, warm_micros) in staged {
            println!(
                "{name:<20} {:>12} {:>11} {:>18} {:>8} {:>12} {:>11} {:>18} {:>8}",
                cold.stages,
                cold.reads,
                cold.requests,
                cold_micros,
                warm.stages,
                warm.reads,
                warm.requests,
                warm_micros
            );
        }
        let footprint = segmented_directory_cache_footprint();
        println!(
            "\ndirectory cache: {} entries, {} B resident, {} hits, {} evictions, \
             {} oversized",
            footprint.entries,
            footprint.bytes,
            footprint.hits,
            footprint.evictions,
            footprint.oversized_skips
        );
    }

    // ----------------------------------------------------------------------
    // siglake #5007: the store waits happen between runs of the synchronous
    // reader, so a lookup holds no thread of tokio's blocking pool.
    // ----------------------------------------------------------------------

    /// An in-memory store whose every range read takes `delay_millis` and which
    /// records how many reads were in flight at once — the two things the
    /// staged shape has to be measured against. It serves bytes per path, so
    /// one of these stands in for a fanned-out scan's several sidecars.
    #[derive(Debug, Default)]
    struct DelayedStorageState {
        files: std::sync::Mutex<HashMap<String, Bytes>>,
        delay_millis: AtomicUsize,
        reads: AtomicUsize,
        in_flight: AtomicUsize,
        peak_in_flight: AtomicUsize,
    }

    fn default_delayed_storage_state() -> Arc<DelayedStorageState> {
        Arc::new(DelayedStorageState::default())
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DelayedStorageFactory {
        #[serde(skip, default = "default_delayed_storage_state")]
        state: Arc<DelayedStorageState>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct DelayedStorage {
        #[serde(skip, default = "default_delayed_storage_state")]
        state: Arc<DelayedStorageState>,
    }

    #[typetag::serde]
    impl StorageFactory for DelayedStorageFactory {
        fn build(&self, _config: &StorageConfig) -> crate::Result<Arc<dyn Storage>> {
            Ok(Arc::new(DelayedStorage {
                state: Arc::clone(&self.state),
            }))
        }
    }

    #[derive(Debug)]
    struct DelayedFileRead {
        state: Arc<DelayedStorageState>,
        path: String,
    }

    #[async_trait]
    impl FileRead for DelayedFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let in_flight = self.state.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.state
                .peak_in_flight
                .fetch_max(in_flight, Ordering::SeqCst);
            self.state.reads.fetch_add(1, Ordering::SeqCst);
            let delay = self.state.delay_millis.load(Ordering::SeqCst) as u64;
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            self.state.in_flight.fetch_sub(1, Ordering::SeqCst);
            let files = self.state.files.lock().unwrap();
            let data = files.get(&self.path).ok_or_else(|| {
                crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("no file {}", self.path),
                )
            })?;
            Ok(data.slice(range.start as usize..range.end as usize))
        }
    }

    #[async_trait]
    #[typetag::serde]
    impl Storage for DelayedStorage {
        async fn exists(&self, path: &str) -> crate::Result<bool> {
            Ok(self.state.files.lock().unwrap().contains_key(path))
        }

        async fn metadata(&self, path: &str) -> crate::Result<FileMetadata> {
            let files = self.state.files.lock().unwrap();
            let data = files.get(path).ok_or_else(|| {
                crate::Error::new(crate::ErrorKind::DataInvalid, format!("no file {path}"))
            })?;
            Ok(FileMetadata {
                size: data.len() as u64,
            })
        }

        async fn read(&self, path: &str) -> crate::Result<Bytes> {
            let files = self.state.files.lock().unwrap();
            files.get(path).cloned().ok_or_else(|| {
                crate::Error::new(crate::ErrorKind::DataInvalid, format!("no file {path}"))
            })
        }

        async fn reader(&self, path: &str) -> crate::Result<Box<dyn FileRead>> {
            Ok(Box::new(DelayedFileRead {
                state: Arc::clone(&self.state),
                path: path.to_string(),
            }))
        }

        async fn write(&self, path: &str, bs: Bytes) -> crate::Result<()> {
            self.state
                .files
                .lock()
                .unwrap()
                .insert(path.to_string(), bs);
            Ok(())
        }

        async fn writer(&self, _path: &str) -> crate::Result<Box<dyn FileWrite>> {
            Err(crate::Error::new(
                crate::ErrorKind::FeatureUnsupported,
                "this store is seeded, not written to",
            ))
        }

        async fn delete(&self, path: &str) -> crate::Result<()> {
            self.state.files.lock().unwrap().remove(path);
            Ok(())
        }

        async fn delete_prefix(&self, _path: &str) -> crate::Result<()> {
            Ok(())
        }

        async fn delete_stream(
            &self,
            mut paths: futures::stream::BoxStream<'static, String>,
        ) -> crate::Result<()> {
            while let Some(path) = paths.next().await {
                self.delete(&path).await?;
            }
            Ok(())
        }

        fn new_input(&self, path: &str) -> crate::Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
        }

        fn new_output(&self, path: &str) -> crate::Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
        }
    }

    /// #5007's acceptance, whole: eight files' lookups, each range read
    /// delayed, run to an answer on a runtime whose **entire** blocking pool
    /// is occupied for the duration. #4561's shape cannot: it ran each lookup
    /// on a blocking thread and served its ranges from the async side, so with
    /// the pool held it could not start, and with a pool of *n* threads it
    /// could not have more than *n* files in flight however small each read
    /// was. (Checked the other way round while #5007 was written: with the
    /// blocking-thread lookup restored, this test times out.)
    ///
    /// The answers, the three outcomes and the fetched-byte accounting are the
    /// same ones the tests above pin; what this adds is where the waits went.
    #[test]
    fn a_segmented_lookup_waits_on_the_store_without_a_blocking_thread() {
        const FILES: usize = 8;
        const DELAY_MS: usize = 10;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // The sidecar bytes, written once through the filesystem store (which
        // does use the blocking pool) before anything occupies it.
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(4_000);
        let (blob, sidecar) = runtime.block_on(async {
            let (file_io, path) = write_mixed_sidecar(&dir, &rows, 1_000).await;
            let blob = sidecar_blob(&file_io, &path, SEGMENTED_BLOB_TYPE).await;
            (blob, Bytes::from(std::fs::read(&path).unwrap()))
        });
        let state = Arc::new(DelayedStorageState::default());
        state.delay_millis.store(DELAY_MS, Ordering::SeqCst);
        let paths: Vec<String> = (0..FILES)
            .map(|file| format!("memory://seg-5007-{file}.puffin"))
            .collect();
        {
            let mut files = state.files.lock().unwrap();
            for path in &paths {
                files.insert(path.clone(), sidecar.clone());
            }
        }
        let file_io = FileIOBuilder::new(Arc::new(DelayedStorageFactory {
            state: Arc::clone(&state),
        }))
        .build();
        let counts = row_counts(4_000, 1_000);
        let spec = all_terms_spec(&["rareneedle"]);
        let expected = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str))
            .postings("rareneedle")
            .unwrap()
            .to_vec();

        // Hold the whole blocking pool, and prove it is held before the
        // lookups start rather than assuming the task was picked up.
        let (release, wait_for_release) = std::sync::mpsc::channel::<()>();
        let (held, wait_until_held) = std::sync::mpsc::channel::<()>();
        runtime.spawn_blocking(move || {
            held.send(()).unwrap();
            let _ = wait_for_release.recv();
        });
        wait_until_held
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("the one blocking thread is occupied");

        // One file first: its peak in-flight reads are this lookup's own, so
        // they say whether a stage's ranges went out together.
        let solo = runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(60),
                segmented_matching_rows(&file_io, &paths[0], &blob, &counts, None, &spec, true),
            )
            .await
            .expect("a lookup that waited on a blocking thread could not finish here")
            .unwrap()
        });
        let (solo_outcome, solo_cost) = solo;
        let SegmentedOutcome::Matching {
            rows: solo_rows, ..
        } = solo_outcome
        else {
            panic!("the term is in the sidecar: {solo_outcome:?}");
        };
        assert_eq!(solo_rows, expected);
        let solo_peak = state.peak_in_flight.load(Ordering::SeqCst);
        assert!(
            solo_peak > 1,
            "one lookup's stage should fetch its ranges together, peak {solo_peak}"
        );
        assert!(
            solo_cost.stages <= 5,
            "{} stages for {} store reads",
            solo_cost.stages,
            solo_cost.reads
        );
        assert!(solo_cost.reads > solo_cost.stages);
        assert!(solo_cost.requests >= solo_cost.reads);

        // Then the fan-out: eight files at once, still under the held pool.
        state.reads.store(0, Ordering::SeqCst);
        state.peak_in_flight.store(0, Ordering::SeqCst);
        let started = std::time::Instant::now();
        let answers = runtime.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(60),
                futures::future::join_all(paths.iter().map(|path| {
                    let file_io = file_io.clone();
                    let blob = blob.clone();
                    let counts = counts.clone();
                    let spec = spec.clone();
                    async move {
                        segmented_matching_rows(&file_io, path, &blob, &counts, None, &spec, true)
                            .await
                            .unwrap()
                    }
                })),
            )
            .await
            .expect("eight lookups that each held a blocking thread could not finish here")
        });
        let elapsed = started.elapsed();
        release.send(()).ok();

        for (path, (outcome, cost)) in paths.iter().zip(&answers) {
            let SegmentedOutcome::Matching { rows, .. } = outcome else {
                panic!("{path}: {outcome:?}");
            };
            assert_eq!(rows, &expected, "{path}");
            assert_eq!(cost.stages, solo_cost.stages, "{path}");
            assert_eq!(cost.reads, solo_cost.reads, "{path}");
            assert_eq!(cost.bytes, solo_cost.bytes, "{path}");
        }
        // The latency claim, against the shape it replaces: a source serving
        // one range at a time costs a round trip per read, and these eight
        // lookups took fewer than half of one file's worth of those.
        let serial = std::time::Duration::from_millis(solo_cost.reads * DELAY_MS as u64);
        assert!(
            elapsed * 2 < serial * FILES as u32,
            "{FILES} files took {elapsed:?}; one file's reads served one at a time is {serial:?}"
        );
        println!(
            "one lookup: {} stages, {} store reads, {} reader reads, {} bytes, peak {solo_peak} \
             in flight\n{FILES} files: {elapsed:?} at {DELAY_MS} ms a read, peak {} in flight, \
             {} store reads (serial, one file: {serial:?})",
            solo_cost.stages,
            solo_cost.reads,
            solo_cost.requests,
            solo_cost.bytes,
            state.peak_in_flight.load(Ordering::SeqCst),
            state.reads.load(Ordering::SeqCst),
        );
    }

    #[test]
    fn the_range_concurrency_is_resolved_from_its_own_knob() {
        // Unset, empty or unparseable is the scan's own merged-range bound,
        // and `0` is not "no concurrency at all" — a stage with no fetches in
        // flight never completes.
        assert_eq!(
            segmented_range_concurrency_from(None),
            DEFAULT_RANGE_FETCH_CONCURRENCY
        );
        assert_eq!(
            segmented_range_concurrency_from(Some("")),
            DEFAULT_RANGE_FETCH_CONCURRENCY
        );
        assert_eq!(
            segmented_range_concurrency_from(Some("plenty")),
            DEFAULT_RANGE_FETCH_CONCURRENCY
        );
        assert_eq!(
            segmented_range_concurrency_from(Some("-4")),
            DEFAULT_RANGE_FETCH_CONCURRENCY
        );
        assert_eq!(
            segmented_range_concurrency_from(Some("0")),
            DEFAULT_RANGE_FETCH_CONCURRENCY
        );
        assert_eq!(segmented_range_concurrency_from(Some(" 3 ")), 3);
    }

    /// The whole reader path on a real Parquet file and a real statistics
    /// file: discovery by blob type, the row-domain check against the file's
    /// Parquet metadata, and a `RowSelection` over the row groups the scan
    /// kept. Ported to seg2, the only blob type the reader discovers.
    #[tokio::test]
    async fn a_registered_segmented_sidecar_selects_the_scans_rows() {
        let dir = TempDir::new().unwrap();
        let rows = segmented_corpus(2_000);
        let data_file = format!("{}/0.parquet", dir.path().to_str().unwrap());
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("raw", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![Arc::new(StringArray::from_iter_values(rows.iter()))],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(512))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&data_file).unwrap(),
            Arc::clone(&arrow_schema),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let parquet = SerializedFileReader::new(std::fs::File::open(&data_file).unwrap()).unwrap();
        let metadata = parquet.metadata();
        assert_eq!(metadata.num_row_groups(), 4);

        let (file_io, sidecar) = write_seg2_sidecar_for(&dir, &rows, 512, &data_file).await;
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "raw", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .unwrap(),
        );
        let blob_properties = HashMap::from([
            ("data_file".to_string(), data_file.clone()),
            ("column".to_string(), "raw".to_string()),
        ]);
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file).unwrap().len())
            .with_start(0)
            .with_length(0)
            .with_record_count(Some(2_000))
            .with_data_file_path(data_file.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .with_statistics_blobs(vec![StatisticsBlobReference {
                statistics_path: sidecar.clone(),
                blob_type: siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE.to_string(),
                properties: blob_properties.clone(),
            }])
            .build();
        let spec = all_terms_spec(&["rareneedle"]);
        let v1 = siglake_index::InvertedIndex::from_rows(rows.iter().map(String::as_str));
        let matching = v1.postings("rareneedle").unwrap().to_vec();
        assert!(
            matching.len() > 1 && matching.last().unwrap() >= &1_024,
            "the term straddles row groups: {matching:?}"
        );

        let selection = ArrowReader::segmented_index_row_selection(
            &file_io, &task, metadata, &None, &spec, true,
        )
        .await
        .unwrap()
        .expect("the registered sidecar answers");
        assert_eq!(
            selection,
            super::index_matches_row_selection(metadata.row_groups(), &None, &matching),
            "the same selection the whole-file index's ordinals produce"
        );

        // Restricted to the row groups a scan kept, the selection covers those
        // groups only.
        let kept = Some(vec![2, 3]);
        let selection = ArrowReader::segmented_index_row_selection(
            &file_io, &task, metadata, &kept, &spec, true,
        )
        .await
        .unwrap()
        .expect("the registered sidecar answers");
        assert_eq!(
            selection,
            super::index_matches_row_selection(metadata.row_groups(), &kept, &matching)
        );

        // A kept-group list that is not strictly ascending is refused rather
        // than answered over a different set of groups than the selection is
        // then built for.
        assert!(
            ArrowReader::segmented_index_row_selection(
                &file_io,
                &task,
                metadata,
                &Some(vec![3, 2]),
                &spec,
                true,
            )
            .await
            .unwrap()
            .is_none()
        );

        // A file whose only registered sidecar is a v1 one is not this path's
        // to answer: it falls through to the whole-file index. The statistics
        // file really does carry that v1 blob.
        let legacy = FileScanTask {
            statistics_blobs: vec![StatisticsBlobReference {
                statistics_path: sidecar.clone(),
                blob_type: "siglake-inverted-v1".to_string(),
                properties: blob_properties.clone(),
            }],
            ..task.clone()
        };
        assert!(
            ArrowReader::segmented_index_row_selection(
                &file_io, &legacy, metadata, &None, &spec, true,
            )
            .await
            .unwrap()
            .is_none()
        );

        // Nor is a seg1 blob, however well formed: discovery is by blob type,
        // and registering one under the seg2 type finds no blob to read.
        let (seg1_io, seg1_sidecar) = write_mixed_sidecar_for(&dir, &rows, 512, &data_file).await;
        let seg1 = FileScanTask {
            statistics_blobs: vec![StatisticsBlobReference {
                statistics_path: seg1_sidecar,
                blob_type: siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE.to_string(),
                properties: blob_properties,
            }],
            ..task.clone()
        };
        assert!(
            ArrowReader::segmented_index_row_selection(
                &seg1_io, &seg1, metadata, &None, &spec, true,
            )
            .await
            .unwrap()
            .is_none()
        );

        // And a sidecar written for a different row-group layout prunes
        // nothing, however well its blob parses: 5 groups of 400 rows cover
        // the same 2,000 rows the file holds in 4.
        let other_dir = TempDir::new().unwrap();
        let (other_io, other_sidecar) =
            write_seg2_sidecar_for(&other_dir, &rows, 400, &data_file).await;
        let mismatched = FileScanTask {
            statistics_blobs: vec![StatisticsBlobReference {
                statistics_path: other_sidecar,
                blob_type: siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE.to_string(),
                properties: HashMap::from([
                    ("data_file".to_string(), data_file.clone()),
                    ("column".to_string(), "raw".to_string()),
                ]),
            }],
            ..task
        };
        assert!(
            ArrowReader::segmented_index_row_selection(
                &other_io,
                &mismatched,
                metadata,
                &None,
                &spec,
                true,
            )
            .await
            .unwrap()
            .is_none()
        );
        // …and it is the row-domain check that refuses it, not a blob the
        // reader failed to parse.
        let other_blob = sidecar_blob_for(
            &other_io,
            &mismatched.statistics_blobs[0].statistics_path,
            siglake_index::segmented::SEGMENTED_V2_BLOB_TYPE,
            &data_file,
        )
        .await;
        let (outcome, _) = segmented_matching_rows(
            &other_io,
            &mismatched.statistics_blobs[0].statistics_path,
            &other_blob,
            &row_counts(2_000, 512),
            None,
            &spec,
            true,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, SegmentedOutcome::Declined("row_domain")),
            "{outcome:?}"
        );
    }
}
