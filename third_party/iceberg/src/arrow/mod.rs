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

//! Conversion between Iceberg and Arrow schema

mod schema;
pub use schema::*;

mod nan_val_cnt_visitor;
pub(crate) use nan_val_cnt_visitor::*;
pub(crate) mod caching_delete_file_loader;
/// Delete File loader
pub mod delete_file_loader;
pub(crate) mod delete_filter;

mod int96;
mod reader;
/// RecordBatch projection utilities
pub mod record_batch_projector;
pub(crate) mod record_batch_transformer;
mod scan_metrics;
mod value;

pub use reader::*;
pub use scan_metrics::{ScanCounters, ScanMetrics, ScanResult};
pub use value::*;
/// Outcomes carried by Puffin blob-cache lookup metrics.
pub const PUFFIN_BLOB_CACHE_OUTCOMES: &[&str] = crate::puffin::PUFFIN_BLOB_CACHE_OUTCOMES;
/// Reasons carried by Puffin blob-cache eviction metrics.
pub const PUFFIN_BLOB_CACHE_DROP_REASONS: &[&str] = crate::puffin::PUFFIN_BLOB_CACHE_DROP_REASONS;

/// Return cached Puffin entries and bytes matching a path plus total bytes.
pub fn puffin_blob_cache_stats(path_substring: &str) -> (usize, usize, usize) {
    crate::puffin::puffin_blob_cache_stats(path_substring)
}

/// Return cumulative Puffin object fetches and blob-cache hits.
pub fn puffin_blob_fetch_counts() -> (u64, u64) {
    crate::puffin::puffin_blob_fetch_counts()
}

/// Total object-store bytes read since process start or the last reset.
pub fn object_store_bytes_read() -> u64 {
    crate::io::read_observability::object_store_bytes_read()
}

/// Reset the process-wide object-store byte counter and return its prior value.
pub fn reset_object_store_bytes_read() -> u64 {
    crate::io::read_observability::reset_object_store_bytes_read()
}
/// Partition value calculator for computing partition values
pub mod partition_value_calculator;
pub use partition_value_calculator::*;

const TEXT_INDEX_CACHE_UNCONFIGURED: u64 = u64::MAX;
static CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(TEXT_INDEX_CACHE_UNCONFIGURED);
static CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(TEXT_INDEX_CACHE_UNCONFIGURED);

/// Set the process-wide parsed-index and Puffin blob cache budgets.
pub fn set_text_index_cache_max_bytes(parsed_index_bytes: u64, puffin_blob_bytes: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES.store(parsed_index_bytes, Relaxed);
    CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES.store(puffin_blob_bytes, Relaxed);
}

/// Restore both text-index cache budgets to their environment/default
/// resolution. Intended for tests that temporarily install process-wide
/// budgets.
pub fn clear_text_index_cache_max_bytes() {
    set_text_index_cache_max_bytes(TEXT_INDEX_CACHE_UNCONFIGURED, TEXT_INDEX_CACHE_UNCONFIGURED);
}

fn configured_cache_bytes(
    configured: &std::sync::atomic::AtomicU64,
    raw: Option<&str>,
    resolver: fn(Option<&str>) -> usize,
) -> usize {
    let configured = configured.load(std::sync::atomic::Ordering::Relaxed);
    configured_cache_bytes_from(configured, raw, resolver)
}

fn configured_cache_bytes_from(
    configured: u64,
    raw: Option<&str>,
    resolver: fn(Option<&str>) -> usize,
) -> usize {
    if configured != TEXT_INDEX_CACHE_UNCONFIGURED {
        return configured.min(usize::MAX as u64) as usize;
    }
    resolver(raw)
}

fn cache_max_bytes_from(raw: Option<&str>, default: u64) -> usize {
    raw.map(str::trim)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
        .min(usize::MAX as u64) as usize
}

fn puffin_blob_cache_max_bytes_from(raw: Option<&str>) -> usize {
    cache_max_bytes_from(raw, 256 * 1024 * 1024)
}

fn parsed_index_cache_max_bytes_from(raw: Option<&str>) -> usize {
    cache_max_bytes_from(raw, 1024 * 1024 * 1024)
}

fn puffin_blob_cache_max_entries_from(raw: Option<&str>) -> usize {
    raw.map(str::trim)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128)
}

pub(crate) fn puffin_blob_cache_max_bytes() -> usize {
    let raw = std::env::var("SIGLAKE_PUFFIN_BLOB_CACHE_MAX_BYTES").ok();
    configured_cache_bytes(
        &CONFIGURED_PUFFIN_BLOB_CACHE_MAX_BYTES,
        raw.as_deref(),
        puffin_blob_cache_max_bytes_from,
    )
}

pub(crate) fn parsed_index_cache_max_bytes() -> usize {
    let raw = std::env::var("SIGLAKE_PARSED_INDEX_CACHE_MAX_BYTES").ok();
    configured_cache_bytes(
        &CONFIGURED_PARSED_INDEX_CACHE_MAX_BYTES,
        raw.as_deref(),
        parsed_index_cache_max_bytes_from,
    )
}

pub(crate) fn puffin_blob_cache_max_entries() -> usize {
    puffin_blob_cache_max_entries_from(
        std::env::var("SIGLAKE_PUFFIN_BLOB_CACHE_MAX_ENTRIES")
            .ok()
            .as_deref(),
    )
}

/// Return the text-index cache byte budgets currently in force.
pub fn text_index_cache_max_bytes_in_force() -> (u64, u64) {
    let entries = puffin_blob_cache_max_entries();
    if entries == 0 {
        return (0, 0);
    }
    (
        parsed_index_cache_max_bytes() as u64,
        puffin_blob_cache_max_bytes() as u64,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        TEXT_INDEX_CACHE_UNCONFIGURED, configured_cache_bytes_from,
        parsed_index_cache_max_bytes_from, puffin_blob_cache_max_bytes_from,
        puffin_blob_cache_max_entries_from,
    };

    #[test]
    fn puffin_blob_byte_budget_is_resolved_without_environment_mutation() {
        const DEFAULT: usize = 256 * 1024 * 1024;
        assert_eq!(puffin_blob_cache_max_bytes_from(None), DEFAULT);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some("")), DEFAULT);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some("many")), DEFAULT);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some("0")), 0);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some("   ")), DEFAULT);
        assert_eq!(puffin_blob_cache_max_bytes_from(Some(" 4096 ")), 4096);
        assert_eq!(
            puffin_blob_cache_max_bytes_from(Some(&u64::MAX.to_string())),
            usize::MAX
        );
    }

    #[test]
    fn parsed_index_byte_budget_is_resolved_without_environment_mutation() {
        const DEFAULT: usize = 1024 * 1024 * 1024;
        assert_eq!(parsed_index_cache_max_bytes_from(None), DEFAULT);
        assert_eq!(parsed_index_cache_max_bytes_from(Some("")), DEFAULT);
        assert_eq!(parsed_index_cache_max_bytes_from(Some("many")), DEFAULT);
        assert_eq!(parsed_index_cache_max_bytes_from(Some("0")), 0);
        assert_eq!(parsed_index_cache_max_bytes_from(Some("   ")), DEFAULT);
        assert_eq!(parsed_index_cache_max_bytes_from(Some(" 8192 ")), 8192);
        assert_eq!(
            parsed_index_cache_max_bytes_from(Some(&u64::MAX.to_string())),
            usize::MAX
        );
    }

    #[test]
    fn puffin_blob_entry_limit_is_resolved_without_environment_mutation() {
        assert_eq!(puffin_blob_cache_max_entries_from(None), 128);
        assert_eq!(puffin_blob_cache_max_entries_from(Some("")), 128);
        assert_eq!(puffin_blob_cache_max_entries_from(Some("many")), 128);
        assert_eq!(puffin_blob_cache_max_entries_from(Some("0")), 0);
        assert_eq!(puffin_blob_cache_max_entries_from(Some("   ")), 128);
        assert_eq!(puffin_blob_cache_max_entries_from(Some(" 64 ")), 64);
        assert_eq!(
            puffin_blob_cache_max_entries_from(Some(&usize::MAX.to_string())),
            usize::MAX
        );
    }

    #[test]
    fn configured_byte_budget_takes_precedence_over_environment_resolution() {
        assert_eq!(
            configured_cache_bytes_from(17, Some("23"), parsed_index_cache_max_bytes_from),
            17
        );
        assert_eq!(
            configured_cache_bytes_from(19, Some("29"), puffin_blob_cache_max_bytes_from),
            19
        );
        assert_eq!(
            configured_cache_bytes_from(
                TEXT_INDEX_CACHE_UNCONFIGURED,
                Some("23"),
                parsed_index_cache_max_bytes_from,
            ),
            23
        );
        assert_eq!(
            configured_cache_bytes_from(
                TEXT_INDEX_CACHE_UNCONFIGURED,
                Some("29"),
                puffin_blob_cache_max_bytes_from,
            ),
            29
        );
    }
}
/// Record batch partition splitter for partitioned tables
pub mod record_batch_partition_splitter;
pub use record_batch_partition_splitter::*;
