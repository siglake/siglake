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

//! Scan metrics and I/O counting for Parquet data file reads.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::io::read_observability::ObjectStoreReadPhase;
use crate::scan::ArrowRecordBatchStream;

/// Detailed per-scan pruning and physical I/O attribution.
#[derive(Debug, Default)]
pub struct ScanCounters {
    /// File tasks whose Parquet footer was opened.
    pub files_read: AtomicU64,
    /// Whole files skipped by a bloom before data reads.
    pub files_pruned_bloom: AtomicU64,
    /// Row groups in the task's byte-range scope.
    pub row_groups_considered: AtomicU64,
    /// Row groups removed by application bloom indexes.
    pub row_groups_pruned_bloom: AtomicU64,
    /// Row groups removed by predicate statistics.
    pub row_groups_pruned_stats: AtomicU64,
    /// Row groups handed to the Parquet reader.
    pub row_groups_read: AtomicU64,
    /// Rows skipped before decode by a row selection.
    pub rows_pruned_selection: AtomicU64,
    /// Physical object-store range reads, including delete files.
    pub object_store_reads: AtomicU64,
    /// Physical Parquet and Puffin footer bytes.
    pub bytes_footer: AtomicU64,
    /// Physical page, offset, column, and application index bytes.
    pub bytes_index: AtomicU64,
    /// Physical Parquet column data bytes.
    pub bytes_data: AtomicU64,
    /// Physical bytes without a more specific class.
    pub bytes_other: AtomicU64,
}

impl ScanCounters {
    /// Attribute physical bytes to one read phase.
    pub fn add_phase_bytes(&self, phase: ObjectStoreReadPhase, bytes: u64) {
        let counter = match phase {
            ObjectStoreReadPhase::Footer => &self.bytes_footer,
            ObjectStoreReadPhase::Index => &self.bytes_index,
            ObjectStoreReadPhase::Data => &self.bytes_data,
            ObjectStoreReadPhase::Manifest | ObjectStoreReadPhase::Other => &self.bytes_other,
        };
        counter.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Total physical bytes attributed to this scan.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_footer.load(Ordering::Relaxed)
            + self.bytes_index.load(Ordering::Relaxed)
            + self.bytes_data.load(Ordering::Relaxed)
            + self.bytes_other.load(Ordering::Relaxed)
    }
}

/// Metrics collected during an Iceberg scan.
#[derive(Clone, Debug)]
pub struct ScanMetrics {
    counters: Arc<ScanCounters>,
}

impl ScanMetrics {
    pub(crate) fn new(counters: Arc<ScanCounters>) -> Self {
        Self { counters }
    }

    pub(crate) fn counters(&self) -> &Arc<ScanCounters> {
        &self.counters
    }

    /// Total bytes read from storage during this scan, including data files and delete files.
    pub fn bytes_read(&self) -> u64 {
        self.counters.bytes_read()
    }

    /// Detailed counters shared by data and delete-file reads.
    pub fn scan_counters(&self) -> &ScanCounters {
        &self.counters
    }
}

impl Default for ScanMetrics {
    fn default() -> Self {
        Self::new(Arc::new(ScanCounters::default()))
    }
}

/// Result of [`ArrowReader::read`](super::ArrowReader::read), containing the
/// record batch stream and metrics collected during the scan.
pub struct ScanResult {
    stream: ArrowRecordBatchStream,
    metrics: ScanMetrics,
}

impl ScanResult {
    pub(crate) fn new(stream: ArrowRecordBatchStream, metrics: ScanMetrics) -> Self {
        Self { stream, metrics }
    }

    /// Consumes the result, returning only the record batch stream.
    pub fn stream(self) -> ArrowRecordBatchStream {
        self.stream
    }

    /// Returns a reference to the scan metrics.
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }
}
