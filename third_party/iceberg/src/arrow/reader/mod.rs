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

use std::sync::Arc;

use crate::arrow::ScanCounters;
use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
use crate::io::FileIO;
use crate::runtime::Runtime;
use crate::scan::FileScanTask;
use crate::util::available_parallelism;

/// Callback invoked after a data file's Parquet metadata opens successfully.
pub type FileOpenedObserver = Arc<dyn Fn(&FileScanTask) + Send + Sync>;

/// Default gap between byte ranges below which they are coalesced into a
/// single request. Matches object_store's `OBJECT_STORE_COALESCE_DEFAULT`.
const DEFAULT_RANGE_COALESCE_BYTES: u64 = 1024 * 1024;

/// Default maximum number of coalesced byte ranges fetched concurrently.
/// Matches object_store's `OBJECT_STORE_COALESCE_PARALLEL`.
const DEFAULT_RANGE_FETCH_CONCURRENCY: usize = 10;

/// Default number of bytes to prefetch when parsing Parquet footer metadata.
/// Matches DataFusion's default `ParquetOptions::metadata_size_hint`.
const DEFAULT_METADATA_SIZE_HINT: usize = 512 * 1024;

mod file_reader;
mod options;
mod ordered;
mod pipeline;
mod positional_deletes;
mod predicate_visitor;
mod projection;
mod pruning;
pub use pruning::{
    PARSED_INDEX_CACHE_DROP_REASONS, PARSED_INDEX_CACHE_OUTCOMES,
    SEGMENTED_DIRECTORY_CACHE_DROP_REASONS, SEGMENTED_DIRECTORY_CACHE_OUTCOMES,
    TEXT_INDEX_STARTUP_STAGES, TEXT_INDEX_STORAGE_FORMS, ParsedIndexCacheFootprint,
    SegmentedDirectoryCacheFootprint, clear_parsed_inverted_index_cache,
    inverted_index_decode_counts, parsed_inverted_index_cache_footprint,
    parsed_inverted_index_cache_stats,
    segmented_directory_cache_footprint, segmented_directory_cache_stats,
};
pub(crate) use pruning::parsed_index_puffin_twin_ranks;
mod reverse;
pub use reverse::reverse_row_selection;
mod row_filter;
pub use file_reader::ArrowFileReader;
pub(crate) use options::ParquetReadOptions;
use predicate_visitor::{CollectFieldIdVisitor, PredicateConverter};
use projection::{add_fallback_field_ids_to_arrow_schema, apply_name_mapping_to_arrow_schema};

/// Conservative text-pruning hints. The caller must still apply its exact
/// predicate to the rows returned by the reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawPruneSpec {
    /// Column the hints apply to.
    pub column: String,
    /// Normalized tokens that must all occur.
    pub all_terms: Vec<String>,
    /// Normalized tokens where at least one must occur.
    pub any_terms: Vec<String>,
    /// Substrings conservatively answered by trigram blooms.
    pub substrings: Vec<String>,
    /// Substrings eligible for inverted-index row selection.
    pub index_substrings: Vec<String>,
    /// Whether the hints came from the full-text UDF.
    pub fts_udf: bool,
    /// Whether a whole-file inverted index may build a row selection.
    pub inverted_index_row_selection: bool,
    /// A clipped LIMIT eligible for segmented point-lookup admission.
    pub segmented_clipped_limit: Option<usize>,
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
            segmented_clipped_limit: None,
        }
    }
}

impl RawPruneSpec {
    /// Construct the legacy single-substring pruning channel.
    pub fn from_raw_substring_filter(substr: Option<String>) -> Option<Self> {
        substr.map(|substr| Self {
            substrings: vec![substr.clone()],
            index_substrings: vec![substr],
            ..Self::default()
        })
    }

    /// Whether this spec carries no usable hints.
    pub fn is_empty(&self) -> bool {
        self.all_terms.is_empty()
            && self.any_terms.is_empty()
            && self.substrings.is_empty()
            && self.index_substrings.is_empty()
    }
}

/// A promoted Utf8 column and the equality values its min/max statistics may
/// conservatively prune.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotedPruneSpec {
    /// Promoted top-level column name.
    pub column: String,
    /// Equality candidates; a group survives if any value may be present.
    pub values: Vec<String>,
}

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
    concurrency_limit_data_files: usize,
    row_group_filtering_enabled: bool,
    row_selection_enabled: bool,
    parquet_read_options: ParquetReadOptions,
    runtime: Runtime,
    scan_counters: Option<Arc<ScanCounters>>,
    file_opened_observer: Option<FileOpenedObserver>,
    cache_bypass: bool,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    output_order_preserved: bool,
    reverse: bool,
    reversed_chunk_rows: Option<usize>,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub fn new(file_io: FileIO, runtime: Runtime) -> Self {
        let num_cpus = available_parallelism().get();

        ArrowReaderBuilder {
            batch_size: None,
            file_io,
            concurrency_limit_data_files: num_cpus,
            row_group_filtering_enabled: true,
            row_selection_enabled: false,
            parquet_read_options: ParquetReadOptions::builder().build(),
            runtime,
            scan_counters: None,
            file_opened_observer: None,
            cache_bypass: false,
            raw_prune_spec: None,
            promoted_prune: Vec::new(),
            output_order_preserved: false,
            reverse: false,
            reversed_chunk_rows: None,
        }
    }

    /// Sets the max number of in flight data files that are being fetched
    pub fn with_data_file_concurrency_limit(mut self, val: usize) -> Self {
        self.concurrency_limit_data_files = val;
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

    /// Attach counters owned by the caller. Upstream [`ScanMetrics`] reads the
    /// same counters, so bytes are recorded once.
    pub fn with_scan_counters(mut self, counters: Option<Arc<ScanCounters>>) -> Self {
        self.scan_counters = counters;
        self
    }

    /// Observe a data-file task only after its Parquet file was opened and its
    /// metadata loaded successfully.
    pub fn with_file_opened_observer(
        mut self,
        observer: Option<FileOpenedObserver>,
    ) -> Self {
        self.file_opened_observer = observer;
        self
    }

    /// Bypass parsed immutable footer caches for measurement controls.
    pub fn with_cache_bypass(mut self, cache_bypass: bool) -> Self {
        self.cache_bypass = cache_bypass;
        self
    }

    /// Set conservative raw-text pruning hints.
    pub fn with_raw_prune_spec(mut self, spec: Option<RawPruneSpec>) -> Self {
        self.raw_prune_spec = spec.filter(|spec| !spec.is_empty());
        self
    }

    /// Set the legacy single-substring pruning hint.
    pub fn with_raw_substring_filter(mut self, term: Option<String>) -> Self {
        self.raw_prune_spec = RawPruneSpec::from_raw_substring_filter(term);
        self
    }

    /// Set promoted-column min/max pruning hints.
    pub fn with_promoted_prune(mut self, specs: Vec<PromotedPruneSpec>) -> Self {
        self.promoted_prune = specs;
        self
    }

    /// Emit file-task streams in task order while opening files concurrently.
    ///
    /// Callers may advertise concatenated files as ordered only when this is
    /// enabled (or when data-file concurrency is one).
    pub fn with_output_order_preserved(mut self, preserved: bool) -> Self {
        self.output_order_preserved = preserved;
        self
    }

    /// Read row groups and rows from the physical tail toward the head.
    pub fn with_reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }

    /// Set the maximum selected rows decoded by one reversed chunk.
    ///
    /// Zero keeps one whole row group per chunk.
    pub fn with_reversed_chunk_rows(mut self, rows: usize) -> Self {
        self.reversed_chunk_rows = Some(rows);
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
                self.runtime.clone(),
            ),
            concurrency_limit_data_files: self.concurrency_limit_data_files,
            row_group_filtering_enabled: self.row_group_filtering_enabled,
            row_selection_enabled: self.row_selection_enabled,
            parquet_read_options: self.parquet_read_options,
            scan_counters: self.scan_counters,
            file_opened_observer: self.file_opened_observer,
            cache_bypass: self.cache_bypass,
            raw_prune_spec: self.raw_prune_spec,
            promoted_prune: self.promoted_prune,
            output_order_preserved: self.output_order_preserved,
            reverse: self.reverse,
            reversed_chunk_rows: self.reversed_chunk_rows,
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
    scan_counters: Option<Arc<ScanCounters>>,
    file_opened_observer: Option<FileOpenedObserver>,
    cache_bypass: bool,
    raw_prune_spec: Option<RawPruneSpec>,
    promoted_prune: Vec<PromotedPruneSpec>,
    output_order_preserved: bool,
    reverse: bool,
    reversed_chunk_rows: Option<usize>,
}
