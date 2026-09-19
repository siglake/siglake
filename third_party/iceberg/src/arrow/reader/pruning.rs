// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.

//! Siglake bloom, inverted-index, and promoted-column pruning.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};

use super::{ArrowReader, PromotedPruneSpec, RawPruneSpec};
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

    fn put(&mut self, key: ParsedIndexKey, index: Arc<siglake_index::InvertedIndex>) {
        let max_bytes = crate::arrow::parsed_index_cache_max_bytes();
        let max_entries = crate::arrow::puffin_blob_cache_max_entries();
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
    parsed_index_cache().lock().unwrap().put(key, index);
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

type SegmentedRangeRequest = (u64, usize, tokio::sync::oneshot::Sender<Option<Vec<u8>>>);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SegmentedReadCost {
    reads: u64,
    bytes: u64,
}

#[derive(Default)]
struct SegmentedReadCounters {
    reads: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
}

impl SegmentedReadCounters {
    fn cost(&self) -> SegmentedReadCost {
        use std::sync::atomic::Ordering::Relaxed;
        SegmentedReadCost {
            reads: self.reads.load(Relaxed),
            bytes: self.bytes.load(Relaxed),
        }
    }
}

#[derive(Clone)]
struct PuffinRangeSource {
    len: u64,
    requests: tokio::sync::mpsc::Sender<SegmentedRangeRequest>,
    counters: Arc<SegmentedReadCounters>,
}

impl siglake_index::segmented::RangeSource for PuffinRangeSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read(&self, offset: u64, len: usize) -> Option<Vec<u8>> {
        use std::sync::atomic::Ordering::Relaxed;
        let (reply, answer) = tokio::sync::oneshot::channel();
        self.requests.blocking_send((offset, len, reply)).ok()?;
        let bytes = answer.blocking_recv().ok()??;
        self.counters.reads.fetch_add(1, Relaxed);
        self.counters.bytes.fetch_add(bytes.len() as u64, Relaxed);
        Some(bytes)
    }
}

enum SegmentedOutcome {
    Matching {
        rows: Vec<u32>,
        resident_bytes: usize,
    },
    Declined(&'static str),
}

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
    let counters = Arc::new(SegmentedReadCounters::default());
    let (requests, mut inbox) = tokio::sync::mpsc::channel::<SegmentedRangeRequest>(1);
    let source = PuffinRangeSource {
        len: range_reader.len(),
        requests,
        counters: Arc::clone(&counters),
    };
    let offset = blob_metadata.offset();
    let held = (!cache_bypass)
        .then(|| segmented_directory_cache_get(statistics_path, offset))
        .flatten();
    let row_counts = row_counts.to_vec();
    let groups = groups.map(<[usize]>::to_vec);
    let spec = spec.clone();
    let lookup = tokio::task::spawn_blocking(move || {
        segmented_lookup(source, &row_counts, groups.as_deref(), &spec, held)
    });
    while let Some((offset, len, reply)) = inbox.recv().await {
        let bytes = range_reader
            .read_at(offset, len as u64)
            .await
            .ok()
            .map(|bytes| bytes.to_vec());
        let _ = reply.send(bytes);
    }
    let (outcome, parsed) = lookup.await.map_err(|error| {
        crate::Error::new(
            crate::ErrorKind::Unexpected,
            format!("segmented index lookup: {error}"),
        )
    })?;
    if !cache_bypass
        && let Some(directory) = parsed
    {
        segmented_directory_cache_put(statistics_path, offset, directory);
    }
    Ok((outcome, counters.cost()))
}

fn segmented_lookup(
    source: PuffinRangeSource,
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
    use std::sync::Arc;

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::file::metadata::{KeyValue, ParquetMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::{
        ArrowReader, PromotedPruneSpec, RawPruneSpec, index_load_concurrency_from,
        intersect_sorted, union_sorted,
    };
    use crate::delete_vector::DeleteVector;
    use crate::io::FileIO;
    use crate::puffin::{Blob, CompressionCodec, PuffinWriter};
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
}
