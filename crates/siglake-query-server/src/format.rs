//! [`RecordBatch`] → JSON / NDJSON serialization.
//!
//! Two modes:
//!
//! - **records**: collect all batches, emit a `{ rows, row_count,
//!   columns, truncated, ... }` JSON envelope. Buffered — the caller
//!   gets the envelope or nothing. Safe for small result sets;
//!   server-side `max_rows` caps prevent runaways.
//! - **NDJSON**: stream batches incrementally to the HTTP response
//!   body. Each row becomes one line. When `max_rows` is hit the
//!   stream emits a final `{ "_meta": "truncated", … }` marker line
//!   and ends. An execution failure after the 200 response has started
//!   similarly becomes a final `{ "_meta": "error", … }` line so the
//!   body remains parseable. Constant memory regardless of result size.

use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use arrow_array::RecordBatch;
use arrow_json::writer::{LineDelimitedWriter, Writer};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch as DfRecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use serde::de::Deserialize as _;

/// Output format requested by the caller.
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QueryFormat {
    /// JSON object `{ rows, row_count, columns, truncated, max_rows }`.
    /// Default.
    #[default]
    Records,
    /// `application/x-ndjson` — one record per line. Streams.
    Ndjson,
}

/// Serialize `batches` as an Arrow IPC **stream** (lossless). Empty input still
/// needs a schema, so callers pass batches that share one; an empty slice
/// yields empty bytes (the reader then produces no batches).
pub fn batches_to_arrow_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return Ok(Vec::new());
    };
    let mut buf = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)
            .context("arrow ipc writer")?;
        for b in batches {
            w.write(b).context("arrow ipc write")?;
        }
        w.finish().context("arrow ipc finish")?;
    }
    Ok(buf)
}

/// Inverse of [`batches_to_arrow_ipc`] — parse an Arrow IPC stream back into
/// batches. Empty input ⇒ no batches.
pub fn arrow_ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .context("arrow ipc reader")?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("arrow ipc decode")
}

/// Per-query scan accounting surfaced on the response for benchmarking and
/// observability. `rows_scanned`/`bytes_scanned` are the leaf rows + bytes the
/// query actually read from data files; an **all-zero** `ScanStats` on a
/// successful aggregate is the signal that a pre-aggregate fast path served the
/// query with *no* data-file scan (the pruning win). `spill_bytes` is
/// aggregation/sort spill.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq, utoipa::ToSchema)]
pub struct ScanStats {
    pub rows_scanned: u64,
    pub bytes_scanned: u64,
    pub spill_bytes: u64,
    /// #85: per-phase wall breakdown for shape-level attribution (which stage
    /// a slow query spent its time in). `None` on paths that don't measure
    /// (fast paths — they're the ~ms answers). Boxed to keep the response
    /// enum variants small.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phases: Option<Box<PhaseStats>>,
    /// Per-request scan attribution: what the plan assigned, what each pruning
    /// mechanism dropped, what the read actually cost. `None` when the plan
    /// executed no siglake data-file scan (Tier-1/metadata answers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<Box<ScanDetail>>,
    /// WHICH path produced the answer.
    ///
    /// `rows_scanned` alone cannot tell you: it counts DataFusion scan output,
    /// and the footer-sum and raw-page paths bypass it entirely, so a
    /// multi-second answer and a sub-millisecond one both report 0. That
    /// ambiguity hid the original `top_hosts` regression and then hid a 6.5s
    /// `GROUP BY host` on the 2026-08-03 1TB round — twice costing an
    /// investigation that started from the wrong premise.
    ///
    /// `tier1_*` are the warm-metadata answers, including
    /// `tier1_windowed_agg` (the per-snapshot 2D time x group rollup: warm
    /// metadata plus at most two boundary ranges). `materialized` means footers
    /// were summed per live file or raw pages decoded — correct, but priced in
    /// seconds at scale. `sketch` is the bounded approximate summary.
    ///
    /// `tier1_windowed_agg` and `materialized` used to be one label, so a
    /// windowed GROUP BY could fall back from the rollup to a footer read per
    /// file — orders of magnitude — and report the same string either way.
    /// `siglake_query_windowed_agg_fallback_total` names the reason when it
    /// happens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_by: Option<String>,
}

/// The per-request pruning-effectiveness view (counts summed across the
/// partitions of every siglake scan leaf). Files: `planned` is the assigned
/// file-set after manifest/time-bounds planning; `read` is how many were
/// actually opened (an early-stopped browse opens fewer); `pruned_bloom` were
/// skipped whole by the raw trigram/token bloom after the footer read. Row
/// groups narrow `considered` → −`pruned_bloom` → −`pruned_stats` → `read`;
/// `rows_pruned_selection` counts rows inside read row groups skipped before
/// decode (page index, positional deletes, inverted index).
impl ScanDetail {
    /// Field-wise accumulate (coordinator: sum per-shard details).
    pub fn absorb(&mut self, o: &ScanDetail) {
        self.files_planned += o.files_planned;
        self.files_read += o.files_read;
        self.files_pruned_bloom += o.files_pruned_bloom;
        self.planned_bytes += o.planned_bytes;
        self.planned_rows += o.planned_rows;
        self.row_groups_considered += o.row_groups_considered;
        self.row_groups_pruned_bloom += o.row_groups_pruned_bloom;
        self.row_groups_pruned_stats += o.row_groups_pruned_stats;
        self.row_groups_read += o.row_groups_read;
        self.rows_pruned_selection += o.rows_pruned_selection;
        self.object_store_reads += o.object_store_reads;
        self.fetched_bytes += o.fetched_bytes;
        self.decoded_bytes += o.decoded_bytes;
        self.bytes_footer += o.bytes_footer;
        self.bytes_index += o.bytes_index;
        self.bytes_data += o.bytes_data;
        self.bytes_other += o.bytes_other;
        self.file_cache_hits += o.file_cache_hits;
        self.file_cache_misses += o.file_cache_misses;
        self.unsettled_partitions += o.unsettled_partitions;
        // Shards agree in practice (one gate decision per table); keep the
        // first-seen outcome, or surface any shard's refusal over nothing.
        if self.ordering.is_none() {
            self.ordering = o.ordering.clone();
        }
    }
}

/// Serde skip predicate for the F-5 byte-class fields: omit them when zero so
/// responses from paths that do not classify bytes (and older servers) stay
/// byte-identical to before.
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq, utoipa::ToSchema)]
pub struct ScanDetail {
    pub files_planned: u64,
    pub files_read: u64,
    pub files_pruned_bloom: u64,
    pub planned_bytes: u64,
    pub planned_rows: u64,
    pub row_groups_considered: u64,
    pub row_groups_pruned_bloom: u64,
    pub row_groups_pruned_stats: u64,
    pub row_groups_read: u64,
    pub rows_pruned_selection: u64,
    pub object_store_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub bytes_footer: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub bytes_index: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub bytes_data: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub bytes_other: u64,
    pub fetched_bytes: u64,
    pub decoded_bytes: u64,
    /// File tasks served from the decoded file-batch cache. A hit returns
    /// cached Arrow batches without building a reader, so it counts in none of
    /// `files_read`, `row_groups_read`, `object_store_reads` or the byte
    /// fields: a response with leaf rows, `files_read: 0` and this non-zero
    /// was served from memory, not measured before its reads landed. Omitted
    /// when zero (cache disabled, or every task bypassed it).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub file_cache_hits: u64,
    /// File tasks that consulted the decoded file-batch cache, found nothing,
    /// and read the file (populating the cache on the way). Tasks carrying a
    /// raw or promoted prune spec bypass the cache and count in neither field.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub file_cache_misses: u64,
    /// Scan partitions still unwinding when this block was built. Their
    /// counters fold only when they finish, so a non-zero value means every
    /// count above is missing that many partitions' worth of reads. The
    /// server waits for an early-stopped scan's partitions to finish before
    /// rendering, so this is omitted (zero) in the normal case and appears
    /// only when that wait hit its deadline.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub unsettled_partitions: u64,
    /// Scan-ordering gate decision: `"advertised"` (the scan streams sorted,
    /// ordered LIMITs early-stop) or the refusal reason (`"filtered"`,
    /// `"fan_in"`, `"no_bounds"`, …). Absent on responses from servers that
    /// predate the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordering: Option<String>,
}

/// Per-query wall-clock attribution, all in microseconds. The single-pod path
/// fills the local phases; a coordinated query fills `distributed` instead of
/// `collect_micros` (its work happens on the workers).
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq, utoipa::ToSchema)]
pub struct PhaseStats {
    /// Physical planning (single-pod) or logical planning + classification
    /// (coordinator).
    pub plan_micros: u64,
    /// WAL-buffer delta/partial load (0 = no buffer configured or drained).
    pub buffer_delta_micros: u64,
    /// Plan execution to completion (single-pod only).
    pub collect_micros: u64,
    /// Result rendering (Arrow → JSON).
    pub render_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distributed: Option<DistPhaseStats>,
}

/// Coordinator-side attribution for a distributed query.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq, utoipa::ToSchema)]
pub struct DistPhaseStats {
    /// Which merge the classifier chose (scan / ordered_scan / aggregate /
    /// ordered_aggregate / local fallback).
    pub mode: String,
    /// Per-shard end-to-end wall (request → decoded partials), one entry per
    /// shard in shard order. The fan-out is concurrent, so the query's
    /// fan-out wall is the MAX, not the sum — a straggler is visible here.
    pub shard_wall_micros: Vec<u64>,
    /// Coordinator-side merge (re-aggregate / merge-sort / concat + limit).
    pub merge_micros: u64,
    /// #967: how many peers this query addressed — the shard count `N` of the
    /// membership snapshot it pinned at dispatch. Every shard `0..N` was
    /// requested exactly once, on this membership, whether or not the cluster
    /// has since grown or shrunk.
    #[serde(default)]
    pub peers: usize,
    /// #967: which published membership `peers` was captured from. Membership
    /// can move between queries, so a shard count is only interpretable
    /// alongside the generation it was captured at; two answers with different
    /// generations were partitioned differently and legitimately so.
    #[serde(default)]
    pub peer_generation: u64,
}

/// JSON envelope for `QueryFormat::Records`.
#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct RecordsResponse {
    pub columns: Vec<String>,
    pub row_count: usize,
    /// One JSON object per row, keyed by `columns`.
    #[schema(value_type = Vec<Object>)]
    pub rows: serde_json::Value,
    /// True when the row cap was hit and the result is incomplete.
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
    /// Effective row cap that produced the truncation. Only present
    /// when `truncated == true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    /// Cost report attached to every successful response so callers
    /// can track effort without a separate `/explain` round trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<crate::cost::CostReport>,
    /// Per-query leaf scan accounting (rows/bytes read, spill). Set by the
    /// execution path; `Some(ScanStats::default())` on fast-path responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<ScanStats>,
    /// Present ONLY when the answer is approximate, and then always — with the
    /// bound and the residual, never a bare flag.
    ///
    /// Absence means exact. That asymmetry is deliberate: a caller that ignores
    /// this field entirely still gets exact answers treated as exact, and the
    /// only way to be misled is to receive this field and discard it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approximation: Option<Approximation>,
}

/// What makes an approximate answer usable rather than merely fast.
// ToSchema because the struct that holds it derives it (origin/main's OpenAPI
// work): utoipa requires every referenced type to compose a schema too.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct Approximation {
    /// Why the exact answer was not served.
    pub reason: String,
    /// Every count UNDERSTATES the truth by at most this much.
    pub error_upper_bound: u64,
    /// Rows no reported group accounts for — the analogue of Elasticsearch's
    /// and Quickwit's `sum_other_doc_count`. Quickwit's `top_hosts` on the
    /// http_logs corpus reported 238,291,469 here, i.e. 96.4% of the corpus
    /// discarded, which is only visible to a caller that looks.
    pub not_counted: u64,
    /// Distinct values the summary retained.
    pub counters: usize,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Key/value slots in one `std` B-tree node: `B` is 6, so a node holds up to
/// `2B - 1` pairs and is allocated whole however few of them are occupied.
const BTREE_NODE_SLOTS: usize = 11;

/// Edges in one internal node: `2B`.
const BTREE_NODE_EDGES: usize = 12;

/// Pairs per leaf in a map grown by `insert`, MEASURED. A split leaves its two
/// halves between `B - 1` and `2B - 1` pairs, so the fill is somewhere in
/// 6..=11; ascending insertion of 12..=500 keys settles at 7.0–7.5 pairs per
/// leaf (`tests/result_cache_retained_bytes.rs`, the fitted table in its module
/// doc). Seven, i.e. the pessimistic end of what was observed.
const BTREE_LEAF_FILL: usize = 7;

/// `LeafNode<K, V>`'s header: a parent pointer, a parent index and a length,
/// padded to the node's alignment.
const BTREE_NODE_HEADER: usize = 16;

/// One `serde_json::Map` leaf — `LeafNode<String, Value>`, 632 bytes.
fn btree_leaf_bytes() -> usize {
    BTREE_NODE_HEADER
        + BTREE_NODE_SLOTS
            * (std::mem::size_of::<String>() + std::mem::size_of::<serde_json::Value>())
}

/// One internal node — a leaf plus its edge pointers, 728 bytes.
fn btree_internal_bytes() -> usize {
    btree_leaf_bytes() + BTREE_NODE_EDGES * std::mem::size_of::<usize>()
}

/// Bytes a `serde_json::Map` of `entries` pairs holds in nodes, leaves and
/// internal levels together.
///
/// MODELLED, because nothing in `std` reports a node count and only a tracking
/// global allocator could measure one. Up to [`BTREE_NODE_SLOTS`] pairs — every
/// row narrower than twelve columns, which is the shape the cache actually
/// stores — it is EXACT: one leaf, no splits, no root. Past that it is fitted
/// to the allocator and errs high; see the calibration test.
fn btree_map_bytes(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    if entries <= BTREE_NODE_SLOTS {
        return btree_leaf_bytes();
    }
    let leaves = entries.div_ceil(BTREE_LEAF_FILL);
    let mut internal = 0;
    let mut level = leaves;
    while level > 1 {
        level = level.div_ceil(BTREE_NODE_EDGES);
        internal += level;
    }
    leaves * btree_leaf_bytes() + internal * btree_internal_bytes()
}

/// Heap bytes a `serde_json::Value` tree owns, excluding the 32-byte `Value`
/// itself (its holder — a `Vec` slot, a map node — has already billed that).
fn value_heap_bytes(value: &serde_json::Value) -> usize {
    match value {
        // A `Number` is inline (i64/u64/f64 in a tagged union) unless
        // `arbitrary_precision` is on, which it is not here.
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(text) => text.capacity(),
        serde_json::Value::Array(items) => {
            items.capacity() * std::mem::size_of::<serde_json::Value>()
                + items.iter().map(value_heap_bytes).sum::<usize>()
        }
        serde_json::Value::Object(map) => {
            btree_map_bytes(map.len())
                + map
                    .iter()
                    .map(|(key, value)| key.capacity() + value_heap_bytes(value))
                    .sum::<usize>()
        }
    }
}

/// Heap bytes this response RETAINS while it is held — not the length of its
/// encoding.
///
/// THE DEFECT THIS CLOSES (#2304). The SQL result cache charged
/// `serde_json::to_vec(body).len()` against its 4 MiB budget for an encoding it
/// dropped two lines later, while the entry it kept was this whole
/// `RecordsResponse`. Measured on a one-column name list that is **29–35x**:
/// 84.2 KiB retained against 2.9 KiB charged at 128 rows, 530.5 KiB against
/// 18.0 KiB at 800 (README, "Caches"). So a
/// bound on 4 MiB of accounted bytes was a bound on 21–132 MiB of heap, none of
/// it inside the query memory pool.
///
/// The dominant term is the one an encoding cannot show: a row is a
/// `serde_json::Map`, i.e. a `BTreeMap`, and its node is allocated whole — all
/// eleven key/value slots, 632 bytes — however few of them a row fills. A
/// one-column row therefore costs ~670 bytes to hold an 8-byte name, which is
/// ~26 bytes encoded.
///
/// An ESTIMATE, deliberately: the `Vec` and `String` terms are exact
/// (`capacity`, so the allocator's slack is billed too), and the one modelled
/// term is how many nodes a `BTreeMap` holds, which `std` does not report and
/// which only a tracking global allocator could measure. It is calibrated
/// against live-heap deltas in `tests/result_cache_retained_bytes.rs`, which
/// fails if the model drifts more than 10% from what the allocator hands out on
/// any shape the cache stores.
///
/// Excludes `size_of::<RecordsResponse>()` itself: what owns the value bills
/// that, and for the cache that is [`crate::sql`]'s entry accounting.
pub fn retained_heap_bytes(response: &RecordsResponse) -> usize {
    let columns = response.columns.capacity() * std::mem::size_of::<String>()
        + response.columns.iter().map(String::capacity).sum::<usize>();
    let cost = response.cost.as_ref().map_or(0, |cost| {
        cost.warnings.capacity() * std::mem::size_of::<String>()
            + cost.warnings.iter().map(String::capacity).sum::<usize>()
    });
    let stats = response.stats.as_ref().map_or(0, |stats| {
        let phases = stats.phases.as_ref().map_or(0, |phases| {
            std::mem::size_of::<PhaseStats>()
                + phases.distributed.as_ref().map_or(0, |dist| {
                    dist.mode.capacity()
                        + dist.shard_wall_micros.capacity() * std::mem::size_of::<u64>()
                })
        });
        let scan = stats.scan.as_ref().map_or(0, |scan| {
            std::mem::size_of::<ScanDetail>() + scan.ordering.as_ref().map_or(0, String::capacity)
        });
        phases + scan + stats.served_by.as_ref().map_or(0, String::capacity)
    });
    let approximation = response
        .approximation
        .as_ref()
        .map_or(0, |approximation| approximation.reason.capacity());
    columns + value_heap_bytes(&response.rows) + cost + stats + approximation
}

/// Render `batches` as a `{ rows, row_count, columns }` JSON envelope.
///
/// If `max_rows` is `Some(n)` and the total row count exceeds `n`,
/// the envelope still serializes the first `n` rows, but `truncated`
/// is set to `true` and `max_rows` carries the effective cap.
pub fn batches_to_records(
    batches: &[RecordBatch],
    max_rows: Option<usize>,
) -> Result<RecordsResponse> {
    let columns: Vec<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    let (effective_batches, row_count, truncated) = match max_rows {
        Some(cap) if total_rows > cap => {
            let trimmed = trim_batches_to(batches, cap);
            (trimmed, cap, true)
        }
        _ => (batches.to_vec(), total_rows, false),
    };

    if row_count == 0 {
        return Ok(RecordsResponse {
            columns,
            row_count: 0,
            rows: serde_json::Value::Array(Vec::new()),
            truncated,
            max_rows: if truncated { max_rows } else { None },
            cost: None,
            stats: None,
            approximation: None,
        });
    }

    // Arrow's JSON writer only targets `Write`; its array form therefore used
    // to materialize the complete JSON result before parsing that result into
    // the final `Value`. Feed its line-delimited form into a row collector
    // instead. The collector parses complete rows as they arrive, so temporary
    // JSON storage is bounded by one encoded row rather than the whole result.
    let rows =
        serde_json::Value::Array(collect_record_rows(&effective_batches, row_count)?.finish()?);

    Ok(RecordsResponse {
        columns,
        row_count,
        rows,
        truncated,
        max_rows: if truncated { max_rows } else { None },
        cost: None,
        stats: None,
        approximation: None,
    })
}

/// A `Write` sink that turns Arrow's NDJSON output directly into JSON values.
///
/// `LineDelimitedWriter` may split a large row across writes, hence `pending`.
/// Newlines within JSON strings are escaped, so every literal newline is a row
/// boundary. Most rows arrive complete and are parsed from the input slice
/// without touching `pending` at all.
struct JsonRowCollector {
    rows: Vec<serde_json::Value>,
    pending: Vec<u8>,
    #[cfg(test)]
    max_pending_len: usize,
}

impl JsonRowCollector {
    fn with_capacity(row_count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(row_count),
            pending: Vec::new(),
            #[cfg(test)]
            max_pending_len: 0,
        }
    }

    fn push_row(&mut self, bytes: &[u8]) -> io::Result<()> {
        let row = if self.pending.is_empty() {
            serde_json::from_slice(bytes)
        } else {
            self.pending.extend_from_slice(bytes);
            let row = serde_json::from_slice(&self.pending);
            self.pending.clear();
            row
        }
        .map_err(io::Error::other)?;
        self.rows.push(row);
        Ok(())
    }

    fn finish(self) -> Result<Vec<serde_json::Value>> {
        anyhow::ensure!(
            self.pending.is_empty(),
            "arrow JSON writer left an unterminated row"
        );
        Ok(self.rows)
    }

    fn record_pending_len(&mut self) {
        #[cfg(test)]
        {
            self.max_pending_len = self.max_pending_len.max(self.pending.len());
        }
    }
}

impl Write for JsonRowCollector {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut remaining = buf;
        while let Some(newline) = remaining.iter().position(|byte| *byte == b'\n') {
            let (row_end, rest) = remaining.split_at(newline);
            self.push_row(row_end)?;
            remaining = &rest[1..];
        }
        if !remaining.is_empty() {
            self.pending.extend_from_slice(remaining);
            self.record_pending_len();
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn collect_record_rows(batches: &[RecordBatch], row_count: usize) -> Result<JsonRowCollector> {
    let mut writer = LineDelimitedWriter::new(JsonRowCollector::with_capacity(row_count));
    write_all(&mut writer, batches)?;
    writer.finish().context("finish arrow JSON writer")?;
    Ok(writer.into_inner())
}

/// A `Write` sink that keeps the length of what was written to it and nothing
/// else, so a caller that only needs the SIZE of a JSON encoding never has to
/// build one.
#[derive(Default)]
struct ByteCounter {
    len: usize,
    /// Largest single fragment the serializer handed us — the whole working set
    /// of a count taken this way. Test-only: it exists to pin the bound.
    #[cfg(test)]
    max_write_len: usize,
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.len += buf.len();
        #[cfg(test)]
        {
            self.max_write_len = self.max_write_len.max(buf.len());
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The byte length `value` would serialize to as JSON, without ever holding
/// that serialization.
///
/// `serde_json::to_vec(&body).len()` on a query result allocates a second
/// complete copy of the result to read one integer off it — the class of
/// rendering overhead that was deliberately taken out of the records path. The
/// serializer emits its output in fragments (a literal, a key, one escaped
/// string run), so counting the fragments yields exactly the same number in
/// scratch space bounded by the largest fragment rather than by the result.
///
/// `None` when the value does not serialize, which for a JSON-typed body means
/// a broken `Serialize` impl rather than anything a caller can act on.
pub(crate) fn serialized_json_len<T: Serialize + ?Sized>(value: &T) -> Option<usize> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value).ok()?;
    Some(counter.len)
}

/// Render `batches` as a single NDJSON blob — one record per line.
/// Used by tests and any non-streaming caller. Production handlers
/// should prefer [`stream_ndjson`] so memory stays bounded.
pub fn batches_to_ndjson(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut writer = LineDelimitedWriter::new(&mut buf);
    write_all(&mut writer, batches)?;
    writer.finish().context("finish arrow NDJSON writer")?;
    Ok(buf)
}

fn write_all<W, F>(writer: &mut Writer<W, F>, batches: &[RecordBatch]) -> Result<()>
where
    W: std::io::Write,
    F: arrow_json::writer::JsonFormat,
{
    for b in batches {
        writer
            .write(b)
            .with_context(|| format!("write arrow JSON batch ({} rows)", b.num_rows()))?;
    }
    Ok(())
}

fn trim_batches_to(batches: &[RecordBatch], cap: usize) -> Vec<RecordBatch> {
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut remaining = cap;
    for b in batches {
        if remaining == 0 {
            break;
        }
        if b.num_rows() <= remaining {
            out.push(b.clone());
            remaining -= b.num_rows();
        } else {
            out.push(b.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

// ---- Streaming NDJSON -----------------------------------------------------

/// Adapt a DataFusion record batch stream into a byte stream of NDJSON
/// chunks, with row-cap enforcement built in.
///
/// One axum-body chunk per source batch. Two trip conditions:
///
/// - **Rows returned** (`max_rows`): the response is clipped at the
///   row cap. Emits a `{ "_meta": "truncated", … }` trailer line.
/// - **Rows scanned** (`max_rows_scanned`): polls the underlying
///   plan's leaf `output_rows` (or falls back to the cumulative
///   emitted-row count) after every batch. On trip, emits a
///   `{ "_meta": "midflight_rows_scanned_exceeded", … }` trailer.
/// - **Execution error**: emits a `{ "_meta": "error", … }` trailer.
///   Pool refusals carry code 503 and a retry delay; other failures carry 500.
///
/// Callers can recognise either marker by its `_meta` key — no real
/// column should collide.
pub struct NdjsonStream {
    inner: SendableRecordBatchStream,
    plan: Option<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    rows_emitted: usize,
    max_rows: Option<usize>,
    max_rows_scanned: Option<usize>,
    priority: crate::limits::Priority,
    finished: bool,
    trailer: Option<Bytes>,
}

impl NdjsonStream {
    pub fn new(inner: SendableRecordBatchStream, max_rows: Option<usize>) -> Self {
        Self {
            inner,
            plan: None,
            rows_emitted: 0,
            max_rows,
            max_rows_scanned: None,
            priority: crate::limits::Priority::Interactive,
            finished: false,
            trailer: None,
        }
    }

    /// Attach the physical plan + a rows-scanned ceiling. When set,
    /// every batch boundary checks the plan's leaf `output_rows`
    /// against the limit and short-circuits with a truncation marker
    /// when exceeded. Without the plan, only the rows-returned cap
    /// (set via [`NdjsonStream::new`]'s `max_rows`) applies.
    pub fn with_rows_scanned_cap(
        mut self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        max_rows_scanned: usize,
        priority: crate::limits::Priority,
    ) -> Self {
        self.plan = Some(plan);
        self.max_rows_scanned = Some(max_rows_scanned);
        self.priority = priority;
        self
    }
}

impl Stream for NdjsonStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Emit a queued trailer (the truncation marker) if we have one.
        if let Some(trailer) = self.trailer.take() {
            self.finished = true;
            return Poll::Ready(Some(Ok(trailer)));
        }
        if self.finished {
            return Poll::Ready(None);
        }

        let next = futures::ready!(Pin::new(&mut self.inner).poll_next(cx));
        match next {
            None => {
                self.finished = true;
                Poll::Ready(None)
            }
            Some(Err(e)) => {
                self.finished = true;
                Poll::Ready(Some(Ok(make_execution_error_marker(e, self.priority))))
            }
            Some(Ok(batch)) => {
                let (effective, exhausted) = self.clip_batch(&batch);
                let bytes = match render_batch_ndjson(&effective) {
                    Ok(b) => b,
                    Err(e) => {
                        self.finished = true;
                        return Poll::Ready(Some(Err(std::io::Error::other(format!(
                            "ndjson render: {e}"
                        )))));
                    }
                };
                self.rows_emitted += effective.num_rows();
                if exhausted {
                    self.trailer = Some(make_truncation_marker(
                        self.rows_emitted,
                        self.max_rows.unwrap_or(self.rows_emitted),
                    ));
                }
                // Mid-flight rows-scanned check: poll the underlying
                // plan's leaf output_rows. When the metric isn't
                // populated by the table provider, fall back to the
                // cumulative emitted-row count so the breaker still
                // trips on raw `SELECT *` traffic.
                if self.trailer.is_none() {
                    if let (Some(plan), Some(cap)) = (&self.plan, self.max_rows_scanned) {
                        let leaf = crate::midflight::sum_leaf_output_rows(plan);
                        let scanned = (leaf as usize).max(self.rows_emitted);
                        if scanned > cap {
                            metrics::counter!(
                                "siglake_query_breaker_trips_total",
                                "breaker" => "midflight_rows_scanned_ndjson",
                                "priority" => self.priority.label(),
                            )
                            .increment(1);
                            self.trailer = Some(make_midflight_marker(scanned, cap));
                        }
                    }
                }
                Poll::Ready(Some(Ok(bytes)))
            }
        }
    }
}

impl NdjsonStream {
    /// Returns `(batch_to_emit, true_if_cap_exhausted)`. The returned
    /// batch may be a prefix of `batch` when the cap is hit mid-batch.
    fn clip_batch(&self, batch: &DfRecordBatch) -> (DfRecordBatch, bool) {
        match self.max_rows {
            None => (batch.clone(), false),
            Some(cap) => {
                let remaining = cap.saturating_sub(self.rows_emitted);
                if batch.num_rows() <= remaining {
                    (batch.clone(), false)
                } else {
                    (batch.slice(0, remaining), true)
                }
            }
        }
    }
}

fn render_batch_ndjson(batch: &DfRecordBatch) -> Result<Bytes> {
    let mut buf = Vec::with_capacity(batch.num_rows() * 64);
    {
        let mut writer = LineDelimitedWriter::new(&mut buf);
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(Bytes::from(buf))
}

fn make_truncation_marker(rows_emitted: usize, max_rows: usize) -> Bytes {
    let line = serde_json::json!({
        "_meta": "truncated",
        "row_count": rows_emitted,
        "max_rows": max_rows,
    });
    let mut s = serde_json::to_vec(&line).expect("serialize truncation marker");
    s.write_all(b"\n").expect("write newline");
    Bytes::from(s)
}

fn make_midflight_marker(rows_scanned: usize, max_rows_scanned: usize) -> Bytes {
    let line = serde_json::json!({
        "_meta": "midflight_rows_scanned_exceeded",
        "rows_scanned": rows_scanned,
        "max_rows_scanned": max_rows_scanned,
    });
    let mut s = serde_json::to_vec(&line).expect("serialize midflight marker");
    s.write_all(b"\n").expect("write newline");
    Bytes::from(s)
}

fn make_execution_error_marker(
    error: datafusion::error::DataFusionError,
    priority: crate::limits::Priority,
) -> Bytes {
    let error = anyhow::Error::new(error).context("datafusion stream");
    let (code, message, retry_after_secs) = match crate::error::pool_exhaustion(&error) {
        Some(detail) => {
            metrics::counter!(
                "siglake_query_breaker_trips_total",
                "breaker" => crate::error::POOL_EXHAUSTED_BREAKER,
                "priority" => priority.label(),
            )
            .increment(1);
            let api_error = crate::error::ApiError::pool_exhausted(detail);
            (
                api_error.status.as_u16(),
                api_error.msg,
                Some(crate::error::POOL_EXHAUSTED_RETRY_AFTER_SECS),
            )
        }
        None => (500, format!("{error:#}"), None),
    };
    let mut line = serde_json::json!({
        "_meta": "error",
        "code": code,
        "error": message,
    });
    if let Some(secs) = retry_after_secs {
        line["retry_after_secs"] = serde_json::json!(secs);
    }
    let mut s = serde_json::to_vec(&line).expect("serialize execution error marker");
    s.write_all(b"\n").expect("write newline");
    Bytes::from(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;

    #[test]
    fn records_renderer_json_scratch_is_bounded_to_one_row() {
        const ROWS: usize = 64;
        // Larger than arrow-json's internal flush threshold, so every row is
        // split across writes and exercises the collector's scratch buffer.
        let payload = "x".repeat(32 * 1024);
        let values = vec![Some(payload.as_str()); ROWS];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "message",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();

        let collector = collect_record_rows(&[batch], ROWS).unwrap();
        let max_scratch = collector.max_pending_len;
        let rows = collector.finish().unwrap();
        let one_row_bytes = serde_json::to_vec(&rows[0]).unwrap().len();
        let all_rows_bytes = serde_json::to_vec(&rows).unwrap().len();

        assert_eq!(rows.len(), ROWS);
        assert!(max_scratch > 0, "test did not exercise split-row storage");
        assert!(
            max_scratch <= one_row_bytes,
            "scratch held {max_scratch} bytes for a {one_row_bytes}-byte row"
        );
        assert!(
            all_rows_bytes > max_scratch * (ROWS - 1),
            "scratch grew with the complete {all_rows_bytes}-byte result"
        );
    }

    /// Payloads whose JSON encoding is not their UTF-8 encoding: an escape is
    /// two bytes where the source is one, `\u00XX` is six, and a non-BMP
    /// character is a surrogate pair only in the parser's eyes. A counter that
    /// measured the input rather than the output would disagree on every one.
    fn escaping_fixtures() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!(""),
            serde_json::json!("plain"),
            serde_json::json!("quote \" backslash \\ slash /"),
            serde_json::json!("tab\tnewline\ncarriage\r"),
            serde_json::json!("control \u{0}\u{1}\u{1f}"),
            serde_json::json!("naïve café — ümlaut"),
            serde_json::json!("日本語のログ行"),
            serde_json::json!("emoji 🐟🌊 and a flag 🇯🇵"),
            serde_json::json!({"key with \"quotes\"": "value\u{2028}", "n": -0.5, "b": [true, null]}),
            serde_json::json!([{"a": 1}, {"a": 2}]),
        ]
    }

    #[test]
    fn serialized_json_len_matches_serialization_of_escaped_payloads() {
        for fixture in escaping_fixtures() {
            let encoded = serde_json::to_vec(&fixture).unwrap();
            assert_eq!(
                serialized_json_len(&fixture),
                Some(encoded.len()),
                "counted length disagrees with {}",
                String::from_utf8_lossy(&encoded)
            );
            // The Postgres result cap measures a `Value` with `to_string()`;
            // the counter must agree with that form too.
            assert_eq!(
                serialized_json_len(&fixture),
                Some(fixture.to_string().len())
            );
        }
    }

    #[test]
    fn serialized_json_len_matches_serialization_of_a_records_envelope() {
        let payloads = escaping_fixtures()
            .iter()
            .map(|value| Some(value.to_string()))
            .collect::<Vec<_>>();
        let rows = payloads.len();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "message",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(payloads))],
        )
        .unwrap();
        let envelope = batches_to_records(&[batch], None).unwrap();

        assert_eq!(envelope.row_count, rows);
        assert_eq!(
            serialized_json_len(&envelope),
            Some(serde_json::to_vec(&envelope).unwrap().len())
        );
    }

    #[test]
    fn serialized_json_len_scratch_is_bounded_to_one_fragment() {
        const ROWS: usize = 64;
        let payload = "x".repeat(32 * 1024);
        let values = vec![Some(payload.as_str()); ROWS];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "message",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(values))],
        )
        .unwrap();
        let envelope = batches_to_records(&[batch], None).unwrap();
        let all_bytes = serde_json::to_vec(&envelope).unwrap().len();

        // Counted through the same sink `serialized_json_len` uses, so the
        // peak fragment it is asked to hold is observable.
        let mut counter = ByteCounter::default();
        serde_json::to_writer(&mut counter, &envelope).unwrap();
        let max_fragment = counter.max_write_len;

        assert_eq!(counter.len, all_bytes);
        assert!(max_fragment > 0, "test wrote nothing");
        assert!(
            max_fragment <= payload.len(),
            "scratch fragment of {max_fragment} bytes exceeds the {} byte row payload",
            payload.len()
        );
        assert!(
            all_bytes > max_fragment * (ROWS - 1),
            "the largest fragment grew with the complete {all_bytes}-byte result"
        );
    }

    fn error_marker(error: DataFusionError) -> serde_json::Value {
        serde_json::from_slice(&make_execution_error_marker(
            error,
            crate::limits::Priority::Interactive,
        ))
        .expect("valid JSON error marker")
    }

    #[test]
    fn execution_error_marker_keeps_non_pool_failures_parseable() {
        let marker = error_marker(DataFusionError::Execution("operator failed".into()));

        assert_eq!(marker["_meta"], "error");
        assert_eq!(marker["code"], 500);
        assert!(marker["error"]
            .as_str()
            .unwrap()
            .contains("operator failed"));
        assert!(marker.get("retry_after_secs").is_none());
    }

    #[test]
    fn execution_error_marker_marks_pool_refusals_as_retryable() {
        let marker = error_marker(DataFusionError::ResourcesExhausted(
            "MemoryConsumer[TopK] cannot grow".into(),
        ));

        assert_eq!(marker["_meta"], "error");
        assert_eq!(marker["code"], 503);
        assert_eq!(marker["retry_after_secs"], 5);
        assert!(marker["error"]
            .as_str()
            .unwrap()
            .contains("MemoryConsumer[TopK]"));
    }
}
