//! Exact value-only Int64 TopK over an already qualified immutable file set.
//! Bounds only choose work: every returned value comes from a decoded row.
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Range;
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc,
};

use anyhow::Result;
use arrow_array::{Array, Int64Array};
use arrow_schema::DataType;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use futures::StreamExt;
use iceberg::arrow::{ArrowFileReader, ScanMetrics};
use iceberg::io::{FileIO, FileRead, ReadOutcome};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetStatisticsPolicy;
use parquet::file::statistics::Statistics;

/// Reservation includes retained footers, one compressed/decompressed column
/// chunk, Arrow batches and the bounded heap. Refusal leaves the normal plan.
pub const RESERVATION_BYTES: usize = 128 * 1024 * 1024;
const MAX_FOOTER_BYTES: usize = 8 * 1024 * 1024;
// The canonical Big5 snapshot has serialized footers up to 498,994 bytes.
// Keep aggregate and decoded metadata bounds independent of this per-file cap.
const MAX_SINGLE_FOOTER_BYTES: usize = 512 * 1024;
const MAX_RETAINED_METADATA_BYTES: usize = 32 * 1024 * 1024;
const MAX_CHUNK_BYTES: i64 = 32 * 1024 * 1024;
const MAX_GROUPS: usize = 4096;

#[derive(Debug, Default)]
pub struct NumericTopK {
    pub values: Vec<i64>,
    pub files_planned: u64,
    pub files_read: u64,
    pub groups_considered: u64,
    pub groups_read: u64,
    pub rows_read: u64,
    pub decoded_bytes: u64,
    pub fetched_bytes: u64,
    pub footer_bytes: u64,
    pub data_bytes: u64,
    pub reads: u64,
}

fn refuse(reason: &'static str) -> Option<NumericTopK> {
    tracing::debug!(reason, "numeric TopK declined");
    None
}

/// What this route needs out of a footer: the schema, every row group's row
/// count and target chunk sizes, and Int64 min/max/null counts for ONE leaf.
///
/// Page-encoding and size statistics are never read here, and column statistics
/// are decoded for the target leaf alone once an earlier file in the same
/// attempt has proved its ordinal. On the canonical events shape the skipped
/// columns are where the serialized footer bytes and the decoded metadata both
/// go, so this is the per-request metadata work the route can simply not do —
/// it does not weaken any check, because a skipped statistic is `None` and
/// `None` already refuses.
fn metadata_options(narrowed_leaf: Option<usize>) -> ArrowReaderOptions {
    let options = ArrowReaderOptions::new()
        .with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll)
        .with_size_stats_policy(ParquetStatisticsPolicy::SkipAll);
    match narrowed_leaf {
        Some(leaf) => {
            options.with_column_stats_policy(ParquetStatisticsPolicy::skip_except(&[leaf]))
        }
        None => options,
    }
}

struct File {
    path: String,
    /// The manifest length this attempt already matched against the object.
    size: u64,
    metadata: ArrowReaderMetadata,
    leaf: usize,
}

/// Which column statistics a decoded footer actually carries. A narrowed entry
/// must never be served to a request that needs a different leaf's bounds: the
/// skipped statistics are absent, and absent statistics refuse.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum StatsScope {
    All,
    Leaf(usize),
}

/// One object's decoded footer, keyed by object identity and statistics scope.
#[derive(Clone)]
struct CachedFooter {
    /// Serialized footer length, so a reuse charges the aggregate serialized
    /// budget exactly as the read that admitted the entry did.
    footer_length: usize,
    metadata: ArrowReaderMetadata,
}

impl crate::iceberg::ApproxHeapBytes for CachedFooter {
    fn approx_heap_bytes(&self) -> usize {
        // The same decoded-metadata measure the per-attempt 32 MiB bound uses,
        // so one number describes both the transient and the retained cost.
        self.metadata.metadata().memory_size() + std::mem::size_of::<Self>()
    }
}

/// Decoded footers reused across requests for the numeric TopK route.
///
/// Warm repeats of this query decode nothing but the few admitted row groups,
/// and still re-parsed every file's footer: measured locally on a 32-file
/// fixture of the canonical metadata shape, the footer decode was 49 of the
/// 57 ms a warm repeat spent, with zero bytes fetched. The bytes were already
/// in the FileIO byte-range cache; the parse was not retained anywhere.
///
/// Key is `(object path, manifest length, statistics scope)`. siglake data
/// objects are immutable and UUID-named, and the length is the one the manifest
/// declared and the admitting read matched against the physical object, so an
/// entry is a pure function of its key. Nothing here is keyed by table or column
/// name, and nothing is invalidated by time: a new snapshot's new files simply
/// miss. Every per-request check — expected row counts, field id, column path,
/// Int64 type, exact min/max/null counts, chunk and group budgets — still runs
/// against the reused metadata, so reuse can only save the parse, never a check.
pub struct MetadataCache {
    entries: std::sync::Mutex<crate::iceberg::LruMap<(String, u64, StatsScope), CachedFooter>>,
}

impl MetadataCache {
    pub(crate) fn with_limits(cap: usize, max_bytes: usize) -> Self {
        Self {
            entries: std::sync::Mutex::new(crate::iceberg::LruMap::with_limits(
                cap,
                max_bytes,
                "numeric_topk_metadata",
            )),
        }
    }

    fn get(&self, key: &(String, u64, StatsScope)) -> Option<CachedFooter> {
        self.entries.lock().ok()?.get(key)
    }

    fn put(&self, key: (String, u64, StatsScope), value: CachedFooter) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.put(key, value);
        }
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.entries.lock().unwrap().bytes()
    }
}

/// Enforce a cumulative per-attempt bound below both metadata prefetch and
/// data-range coalescing. Every requested byte is charged before IO, including
/// overlapping ranges and retries at this layer. Backend-internal retries are
/// not separately observable. Exhaustion is an error, never a partial result
/// or an uncharged retry through the ordinary plan.
struct BudgetedReader {
    inner: Box<dyn FileRead>,
    remaining: Arc<AtomicU64>,
}
#[async_trait::async_trait]
impl FileRead for BudgetedReader {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<bytes::Bytes> {
        Ok(self.read_with_outcome(range).await?.bytes)
    }
    async fn read_with_outcome(&self, range: Range<u64>) -> iceberg::Result<ReadOutcome> {
        let charge = range.end.checked_sub(range.start).ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                "numeric TopK invalid byte range",
            )
        })?;
        self.remaining
            .fetch_update(Relaxed, Relaxed, |remaining| remaining.checked_sub(charge))
            .map_err(|_| {
                iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "numeric TopK cumulative byte budget exhausted",
                )
            })?;
        self.inner.read_with_outcome(range).await
    }
}

/// Files must be complete, delete-free tasks from the *same* qualified
/// snapshot. The field ID prevents a renamed/reused physical name from being
/// accepted. There is no table-name refresh anywhere in this function.
pub struct NumericTopKAttempt {
    pub result: Option<NumericTopK>,
    /// Reads spent determining eligibility, even when the ordinary plan must
    /// execute. Kept separately so fallback never claims a zero-cost probe.
    pub probe_bytes: u64,
    pub probe_reads: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn read(
    io: &FileIO,
    paths: &[(String, u64, u64)],
    column: &str,
    field_id: i32,
    k: usize,
    ascending: bool,
    pool: &Arc<dyn MemoryPool>,
    cache: &MetadataCache,
) -> Result<NumericTopKAttempt> {
    let metrics = ScanMetrics::default();
    let result = read_inner(
        io,
        paths,
        column,
        field_id,
        k,
        ascending,
        pool,
        cache,
        &metrics,
        MAX_RETAINED_METADATA_BYTES,
    )
    .await?;
    let probe_bytes = metrics.bytes_read();
    let probe_reads = metrics.scan_counters().object_store_reads.load(Relaxed);
    if result.is_none() {
        tracing::debug!(
            probe_bytes,
            probe_reads,
            "numeric TopK declined after metadata probe"
        );
    }
    Ok(NumericTopKAttempt {
        result,
        probe_bytes,
        probe_reads,
    })
}

#[allow(clippy::too_many_arguments)]
async fn read_inner(
    io: &FileIO,
    paths: &[(String, u64, u64)],
    column: &str,
    field_id: i32,
    k: usize,
    ascending: bool,
    pool: &Arc<dyn MemoryPool>,
    cache: &MetadataCache,
    metrics: &ScanMetrics,
    metadata_limit: usize,
) -> Result<Option<NumericTopK>> {
    if !(1..=1000).contains(&k) || paths.len() > 256 {
        return Ok(refuse("shape_or_file_limit"));
    }
    let reservation = MemoryConsumer::new("numeric_topk").register(pool);
    if reservation.try_grow(RESERVATION_BYTES).is_err() {
        return Ok(refuse("memory_budget"));
    }
    let byte_limit = paths.iter().try_fold(0u64, |n, (_, _, bytes)| {
        n.checked_add(bytes.checked_mul(2)?.checked_add(8)?)
    });
    let Some(byte_limit) = byte_limit else {
        return Ok(refuse("byte_budget"));
    };
    let budget = Arc::new(AtomicU64::new(byte_limit));
    let mut files = Vec::new();
    let mut groups = Vec::new();
    let mut footer_bytes = 0usize;
    let mut retained_metadata_bytes = 0usize;
    // The leaf ordinal the first file proved for this attempt. Every later file
    // decodes column statistics for that ordinal ALONE: the target is one Int64
    // leaf, while a wide events footer spends most of its bytes on the other
    // columns' min/max. The ordinal is re-verified against each file's own
    // decoded schema below, so a file that places the column elsewhere refuses
    // rather than reading a neighbour's bounds.
    let mut narrowed_leaf: Option<usize> = None;
    let mut result = NumericTopK {
        files_planned: paths.len() as u64,
        ..Default::default()
    };
    // Validate ALL metadata before decoding: a late refusal must not combine
    // partial optimized output with the normal execution result.
    for (path, expected_rows, expected_size) in paths {
        tokio::task::yield_now().await;
        // The scope an earlier file in this attempt proved, so a warm repeat
        // asks for the entry its own predecessor admitted.
        let scope = narrowed_leaf.map_or(StatsScope::All, StatsScope::Leaf);
        let key = (path.clone(), *expected_size, scope);
        let cached = match cache.get(&key) {
            Some(hit) => hit,
            None => {
                let input = io.new_input(path)?;
                let size = input.metadata().await?;
                if size.size < 12 || size.size != *expected_size {
                    return Ok(refuse("footer"));
                }
                let reader: Box<dyn FileRead> = Box::new(BudgetedReader {
                    inner: input.reader().await?,
                    remaining: budget.clone(),
                });
                let outcome = reader.read_with_outcome(size.size - 8..size.size).await?;
                if outcome.fetched {
                    metrics
                        .scan_counters()
                        .object_store_reads
                        .fetch_add(1, Relaxed);
                    metrics
                        .scan_counters()
                        .bytes_footer
                        .fetch_add(outcome.bytes.len() as u64, Relaxed);
                }
                let tail = outcome.bytes;
                if tail.len() != 8 || &tail[4..] != b"PAR1" {
                    return Ok(refuse("footer"));
                }
                let length = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
                // Still refused before the footer itself is fetched.
                if length > MAX_SINGLE_FOOTER_BYTES
                    || footer_bytes.saturating_add(length) > MAX_FOOTER_BYTES
                    || length as u64 > size.size - 12
                {
                    return Ok(refuse("footer_budget"));
                }
                let builder = ParquetRecordBatchStreamBuilder::new_with_options(
                    ArrowFileReader::new(size, reader).with_scan_metrics(metrics.clone()),
                    metadata_options(narrowed_leaf),
                )
                .await?;
                let footer = CachedFooter {
                    footer_length: length,
                    metadata: ArrowReaderMetadata::try_new(
                        builder.metadata().clone(),
                        ArrowReaderOptions::new(),
                    )?,
                };
                // Admitted only after the physical object length matched the
                // manifest length that keys the entry.
                cache.put(key, footer.clone());
                footer
            }
        };
        let metadata = cached.metadata.metadata();
        // Charged identically whether the footer was parsed now or reused.
        footer_bytes = footer_bytes.saturating_add(cached.footer_length);
        if footer_bytes > MAX_FOOTER_BYTES {
            return Ok(refuse("footer_budget"));
        }
        // Serialized footer size is NOT its retained heap cost. Account for
        // the decoded metadata of every column, not just the target leaf.
        retained_metadata_bytes = retained_metadata_bytes.saturating_add(metadata.memory_size());
        if retained_metadata_bytes > metadata_limit {
            return Ok(refuse("decoded_metadata_budget"));
        }
        if metadata.file_metadata().num_rows() < 0
            || metadata.file_metadata().num_rows() as u64 != *expected_rows
        {
            return Ok(refuse("row_count"));
        }
        let group_rows = metadata.row_groups().iter().try_fold(0u64, |sum, group| {
            sum.checked_add(u64::try_from(group.num_rows()).ok()?)
        });
        if group_rows != Some(*expected_rows) {
            return Ok(refuse("row_count"));
        }
        let schema = metadata.file_metadata().schema_descr();
        let Some(leaf) = schema
            .columns()
            .iter()
            .position(|c| c.path().parts() == [column])
        else {
            return Ok(refuse("column"));
        };
        // A later file whose target leaf sits at a different ordinal had its
        // statistics skipped by the narrowed decode, so it can only be refused.
        if narrowed_leaf.is_some_and(|narrowed| narrowed != leaf) {
            return Ok(refuse("column_position"));
        }
        narrowed_leaf = Some(leaf);
        let descriptor = schema.column(leaf);
        let info = descriptor.self_type().get_basic_info();
        if descriptor.max_rep_level() != 0
            || !info.has_id()
            || info.id() != field_id
            || cached
                .metadata
                .schema()
                .field_with_name(column)
                .ok()
                .map(|f| f.data_type())
                != Some(&DataType::Int64)
        {
            return Ok(refuse("field_identity"));
        }
        for (rg, group) in metadata.row_groups().iter().enumerate() {
            if result.groups_considered >= MAX_GROUPS as u64 {
                return Ok(refuse("group_limit"));
            }
            let chunk = group.column(leaf);
            if group.num_rows() < 0
                || chunk.uncompressed_size() < 0
                || chunk.compressed_size() < 0
                || chunk.uncompressed_size() > MAX_CHUNK_BYTES
                || chunk.compressed_size() > MAX_CHUNK_BYTES
                || group.num_rows() > MAX_CHUNK_BYTES / 8
            {
                return Ok(refuse("chunk_budget"));
            }
            let Some(Statistics::Int64(stats)) = chunk.statistics() else {
                return Ok(refuse("statistics"));
            };
            let Some(nulls) = stats.null_count_opt() else {
                return Ok(refuse("null_count"));
            };
            if nulls > group.num_rows() as u64 {
                return Ok(refuse("null_count"));
            }
            result.groups_considered += 1;
            if nulls == group.num_rows() as u64 {
                continue;
            }
            let (Some(min), Some(max)) = (stats.min_opt(), stats.max_opt()) else {
                return Ok(refuse("statistics"));
            };
            if min > max || !stats.min_is_exact() || !stats.max_is_exact() {
                return Ok(refuse("statistics"));
            }
            groups.push((if ascending { *min } else { *max }, files.len(), rg));
        }
        files.push(File {
            path: path.clone(),
            size: *expected_size,
            metadata: cached.metadata,
            leaf,
        });
    }
    groups.sort_unstable_by(|a, b| {
        if ascending {
            a.0.cmp(&b.0)
        } else {
            b.0.cmp(&a.0)
        }
    });
    // Rank uses Reverse rather than negation: i64::MIN remains representable.
    let mut heap: BinaryHeap<(i64, Reverse<i64>)> = BinaryHeap::with_capacity(k + 1);
    for (bound, file, rg) in groups {
        let rank = |v: i64| {
            if ascending {
                (v, Reverse(0))
            } else {
                (0, Reverse(v))
            }
        };
        if heap.len() == k && rank(bound) >= *heap.peek().unwrap() {
            break;
        }
        let file = &files[file];
        let input = io.new_input(&file.path)?;
        // The length this attempt already matched against both the manifest and
        // the object; a second HEAD per decoded file asks storage what we know.
        let reader = ArrowFileReader::new(
            iceberg::io::FileMetadata { size: file.size },
            Box::new(BudgetedReader {
                inner: input.reader().await?,
                remaining: budget.clone(),
            }),
        )
        .with_scan_metrics(metrics.clone());
        let mask = ProjectionMask::leaves(
            file.metadata.metadata().file_metadata().schema_descr(),
            [file.leaf],
        );
        let mut stream =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, file.metadata.clone())
                .with_projection(mask)
                .with_row_groups(vec![rg])
                .with_batch_size(8192)
                .build()?;
        result.groups_read += 1;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("qualified TopK reader changed type"))?;
            result.rows_read += array.len() as u64;
            result.decoded_bytes += array.get_array_memory_size() as u64;
            for value in array.iter().flatten() {
                heap.push(rank(value));
                if heap.len() > k {
                    heap.pop();
                }
            }
            // Synchronous decode/selection cannot starve deadline cancellation.
            tokio::task::yield_now().await;
        }
    }
    result.values = heap
        .into_iter()
        .map(|(v, Reverse(desc))| if ascending { v } else { desc })
        .collect();
    result.values.sort_unstable();
    if !ascending {
        result.values.reverse();
    }
    // Every file has an opened metadata reader, even when no data group from
    // it is decoded. Match the ordinary scan's reader-open accounting.
    result.files_read = files.len() as u64;
    let counters = metrics.scan_counters();
    result.footer_bytes += counters.bytes_footer.load(Relaxed);
    result.data_bytes = counters.bytes_data.load(Relaxed);
    result.fetched_bytes = metrics.bytes_read();
    result.reads += counters.object_store_reads.load(Relaxed);
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    fn io() -> FileIO {
        FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build()
    }
    /// A fresh cache per call keeps every existing assertion a COLD one.
    fn cache() -> MetadataCache {
        MetadataCache::with_limits(4096, 64 * 1024 * 1024)
    }
    fn pool(bytes: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(bytes))
    }
    fn write(
        dir: &std::path::Path,
        name: &str,
        rows: Vec<Option<i64>>,
        stats: bool,
    ) -> (String, u64, u64) {
        let path = dir.join(name);
        let field = Field::new("value", DataType::Int64, true)
            .with_metadata([("PARQUET:field_id".to_string(), "42".to_string())].into());
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(rows.clone())) as ArrayRef],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(if stats {
                EnabledStatistics::Chunk
            } else {
                EnabledStatistics::None
            })
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (
            path.to_string_lossy().to_string(),
            rows.len() as u64,
            std::fs::metadata(&path).unwrap().len(),
        )
    }

    fn write_sized_footer(dir: &std::path::Path, name: &str, target: usize) -> (String, u64, u64) {
        use parquet::file::metadata::KeyValue;
        let path = dir.join(name);
        let rows = vec![Some(i64::MIN), None, Some(7), Some(7), Some(i64::MAX)];
        let field = Field::new("value", DataType::Int64, true)
            .with_metadata([("PARQUET:field_id".to_string(), "42".to_string())].into());
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(rows.clone())) as ArrayRef],
        )
        .unwrap();
        let mut padding = target;
        for _ in 0..4 {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "footer-bound-regression".into(),
                    Some("x".repeat(padding)),
                )]))
                .build();
            let mut writer = ArrowWriter::try_new(
                std::fs::File::create(&path).unwrap(),
                schema.clone(),
                Some(props),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let bytes = std::fs::read(&path).unwrap();
            let actual =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                    as usize;
            if actual == target {
                return (
                    path.to_string_lossy().into_owned(),
                    rows.len() as u64,
                    bytes.len() as u64,
                );
            }
            padding = padding
                .checked_add(target)
                .unwrap()
                .checked_sub(actual)
                .unwrap();
        }
        panic!("failed to construct exact persisted footer length");
    }

    #[tokio::test]
    async fn numeric_topk_canonical_sized_footer_and_single_boundary_preserve_exact_values() {
        let dir = tempfile::tempdir().unwrap();
        // Actual frozen 32-file Big5 maximum, and the inclusive new boundary.
        // Padding models non-target metadata; all returned values still come
        // from real persisted data pages with NULLs, ties and signed extrema.
        for bytes in [498_994, 512 * 1024] {
            let path = write_sized_footer(dir.path(), &format!("{bytes}.parquet"), bytes);
            for ascending in [true, false] {
                for k in [1, 2, 20] {
                    let memory = pool(RESERVATION_BYTES);
                    let result = read(
                        &io(),
                        std::slice::from_ref(&path),
                        "value",
                        42,
                        k,
                        ascending,
                        &memory,
                        &cache(),
                    )
                    .await
                    .unwrap()
                    .result
                    .unwrap();
                    let mut expected = vec![i64::MIN, 7, 7, i64::MAX];
                    if !ascending {
                        expected.reverse();
                    }
                    expected.truncate(k);
                    assert_eq!(result.values, expected);
                    assert!(result.data_bytes > 0);
                    assert_eq!(memory.reserved(), 0);
                }
            }
        }
        let large = write_sized_footer(dir.path(), "over.parquet", 512 * 1024 + 1);
        let metrics = ScanMetrics::default();
        let memory = pool(RESERVATION_BYTES);
        assert!(read_inner(
            &io(),
            &[large],
            "value",
            42,
            1,
            true,
            &memory,
            &cache(),
            &metrics,
            MAX_RETAINED_METADATA_BYTES
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(metrics.scan_counters().bytes_data.load(Relaxed), 0);
        assert_eq!(memory.reserved(), 0);
    }

    #[tokio::test]
    async fn numeric_topk_canonical_sized_footers_still_refuse_aggregate_overflow_before_data() {
        let dir = tempfile::tempdir().unwrap();
        let paths = (0..17)
            .map(|i| write_sized_footer(dir.path(), &format!("{i}.parquet"), 498_994))
            .collect::<Vec<_>>();
        assert!((paths.len() - 1) * 498_994 < 8 * 1024 * 1024);
        assert!(paths.len() * 498_994 > 8 * 1024 * 1024);
        let memory = pool(RESERVATION_BYTES);
        let metrics = ScanMetrics::default();
        let result = read_inner(
            &io(),
            &paths,
            "value",
            42,
            1,
            true,
            &memory,
            &cache(),
            &metrics,
            MAX_RETAINED_METADATA_BYTES,
        )
        .await
        .unwrap();
        assert!(result.is_none());
        assert!(metrics.bytes_read() > 16 * 498_994);
        assert_eq!(metrics.scan_counters().bytes_data.load(Relaxed), 0);
        assert_eq!(memory.reserved(), 0);
    }

    #[tokio::test]
    async fn numeric_topk_persisted_groups_match_full_sort_with_nulls_ties_and_extrema() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            Some(i64::MIN),
            Some(i64::MIN),
            None,
            Some(-1),
            Some(0),
            Some(1),
            Some(10),
            Some(10),
            Some(11),
            None,
            None,
            None,
            Some(i64::MAX),
            Some(i64::MAX),
            Some(12),
        ];
        let path = write(dir.path(), "groups.parquet", rows.clone(), true);
        for ascending in [true, false] {
            for k in [1, 2, 3, 7, 50] {
                let result = read(
                    &io(),
                    std::slice::from_ref(&path),
                    "value",
                    42,
                    k,
                    ascending,
                    &pool(RESERVATION_BYTES),
                    &cache(),
                )
                .await
                .unwrap()
                .result
                .unwrap();
                let mut expected: Vec<_> = rows.iter().copied().flatten().collect();
                expected.sort_unstable();
                if !ascending {
                    expected.reverse();
                }
                expected.truncate(k);
                assert_eq!(result.values, expected);
                assert_eq!(result.groups_considered, 5);
                let file_bytes = std::fs::metadata(&path.0).unwrap().len();
                assert!(
                    result.fetched_bytes <= 2 * file_bytes + 8,
                    "footer prefetch must fit the preflight bound"
                );
                assert!(
                    result.fetched_bytes > 0 && result.footer_bytes > 0 && result.data_bytes > 0
                );
                if k <= 2 {
                    assert_eq!(result.groups_read, 1);
                    assert_eq!(result.rows_read, 3);
                }
            }
        }
    }

    #[tokio::test]
    async fn numeric_topk_refuses_missing_late_statistics_identity_and_memory() {
        let dir = tempfile::tempdir().unwrap();
        let good = write(dir.path(), "good.parquet", vec![Some(1), Some(2)], true);
        let missing = write(dir.path(), "missing.parquet", vec![Some(100)], false);
        let memory = pool(RESERVATION_BYTES);
        let refused = read(
            &io(),
            &[good.clone(), missing],
            "value",
            42,
            1,
            false,
            &memory,
            &cache(),
        )
        .await
        .unwrap();
        assert!(refused.result.is_none());
        assert!(
            refused.probe_bytes > 0 && refused.probe_reads >= 4,
            "late refusal must retain the cost of both metadata probes"
        );
        for (field, k) in [(41, 1), (42, 0), (42, 1001)] {
            assert!(read(
                &io(),
                std::slice::from_ref(&good),
                "value",
                field,
                k,
                false,
                &memory,
                &cache(),
            )
            .await
            .unwrap()
            .result
            .is_none());
        }
        assert!(
            read(&io(), &[good], "value", 42, 1, false, &pool(1), &cache())
                .await
                .unwrap()
                .result
                .is_none()
        );
        assert_eq!(memory.reserved(), 0);
        let nulls = write(dir.path(), "nulls.parquet", vec![None, None, None], true);
        let result = read(&io(), &[nulls], "value", 42, 1, false, &memory, &cache())
            .await
            .unwrap()
            .result
            .unwrap();
        assert!(result.values.is_empty());
        assert_eq!(result.rows_read, 0);
        assert_eq!(result.groups_read, 0);
    }

    #[tokio::test]
    async fn numeric_topk_refuses_decoded_metadata_and_actual_size_before_data() {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        let dir = tempfile::tempdir().unwrap();
        let first = write(dir.path(), "first.parquet", vec![Some(1), Some(2)], true);
        let second = write(dir.path(), "second.parquet", vec![Some(3), Some(4)], true);
        let decoded = SerializedFileReader::new(std::fs::File::open(&first.0).unwrap())
            .unwrap()
            .metadata()
            .memory_size();
        let memory = pool(RESERVATION_BYTES);
        let metrics = ScanMetrics::default();
        let refused = read_inner(
            &io(),
            &[first.clone(), second],
            "value",
            42,
            1,
            true,
            &memory,
            &cache(),
            &metrics,
            decoded,
        )
        .await
        .unwrap();
        assert!(
            refused.is_none(),
            "the second decoded footer exceeds the retained budget"
        );
        assert!(metrics.bytes_read() > 0);
        assert_eq!(
            metrics.scan_counters().bytes_data.load(Relaxed),
            0,
            "late footer refusal must precede all data reads"
        );
        assert_eq!(memory.reserved(), 0);
        let mut wrong_size = first;
        wrong_size.2 += 1;
        let refused = read(
            &io(),
            &[wrong_size],
            "value",
            42,
            1,
            true,
            &memory,
            &cache(),
        )
        .await
        .unwrap();
        assert!(refused.result.is_none());
        assert_eq!(refused.probe_bytes, 0);
    }

    #[tokio::test]
    async fn numeric_topk_cumulative_io_budget_counts_overlapping_reads() {
        let dir = tempfile::tempdir().unwrap();
        let file = write(dir.path(), "bytes.parquet", vec![Some(1)], true);
        let remaining = Arc::new(AtomicU64::new(16));
        let reader = BudgetedReader {
            inner: io().new_input(&file.0).unwrap().reader().await.unwrap(),
            remaining: remaining.clone(),
        };
        assert_eq!(reader.read(0..8).await.unwrap().len(), 8);
        assert_eq!(reader.read(0..8).await.unwrap().len(), 8);
        assert_eq!(remaining.load(Relaxed), 0);
        assert!(reader
            .read(0..1)
            .await
            .unwrap_err()
            .to_string()
            .contains("cumulative byte budget"));
        assert_eq!(remaining.load(Relaxed), 0);
    }

    /// A bounded stand-in for the canonical 32-file snapshot's METADATA shape:
    /// the same file count and a comparable serialized-footer total, with a tiny
    /// amount of actual data. It isolates per-request metadata work from decode
    /// work, which is the whole question on a warm repeat request.
    fn wide_fixture(dir: &std::path::Path, files: usize) -> Vec<(String, u64, u64)> {
        use arrow_array::StringArray;
        const COLUMNS: usize = 30;
        const GROUPS: usize = 10;
        const ROWS_PER_GROUP: usize = 8;
        // Wide string statistics are what makes the canonical footers large; an
        // all-Int64 pad understates serialized bytes by an order of magnitude.
        const STATS_WIDTH: usize = 380;
        let mut fields = vec![Field::new("value", DataType::Int64, true)
            .with_metadata([("PARQUET:field_id".to_string(), "42".to_string())].into())];
        for i in 0..COLUMNS {
            fields.push(
                Field::new(format!("pad_{i}"), DataType::Utf8, true).with_metadata(
                    [("PARQUET:field_id".to_string(), (100 + i).to_string())].into(),
                ),
            );
        }
        let schema = Arc::new(Schema::new(fields));
        let rows = GROUPS * ROWS_PER_GROUP;
        (0..files)
            .map(|f| {
                let base = (f * rows) as i64;
                let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(
                    (0..rows).map(|r| Some(base + r as i64)).collect::<Vec<_>>(),
                ))];
                for c in 0..COLUMNS {
                    columns.push(Arc::new(StringArray::from(
                        (0..rows)
                            .map(|r| {
                                Some(format!(
                                    "{:0>width$}",
                                    f * 1_000_000 + c * 1_000 + r,
                                    width = STATS_WIDTH
                                ))
                            })
                            .collect::<Vec<_>>(),
                    )));
                }
                let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
                let path = dir.join(format!("wide-{f}.parquet"));
                let properties = WriterProperties::builder()
                    .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
                    .set_statistics_enabled(EnabledStatistics::Chunk)
                    .set_statistics_truncate_length(Some(STATS_WIDTH))
                    .build();
                let mut writer = ArrowWriter::try_new(
                    std::fs::File::create(&path).unwrap(),
                    schema.clone(),
                    Some(properties),
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                (
                    path.to_string_lossy().into_owned(),
                    rows as u64,
                    std::fs::metadata(&path).unwrap().len(),
                )
            })
            .collect()
    }

    /// Serialized footer bytes and retained decoded metadata bytes for a fixture,
    /// the two numbers the canonical snapshot is described by (#7079: 8,296,285
    /// serialized / 10,985,491 decoded over 32 files).
    fn fixture_metadata_bytes(paths: &[(String, u64, u64)]) -> (u64, u64) {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        paths.iter().fold((0, 0), |(serialized, decoded), (p, ..)| {
            let bytes = std::fs::read(p).unwrap();
            let length =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
            let reader = SerializedFileReader::new(std::fs::File::open(p).unwrap()).unwrap();
            (
                serialized + u64::from(length),
                decoded + reader.metadata().memory_size() as u64,
            )
        })
    }

    #[tokio::test]
    #[ignore = "local timing measurement, not a gate; see docs/DESIGN_numeric_topk.md"]
    async fn numeric_topk_repeat_request_metadata_cost_measurement() {
        // Deliberately NOT touching the process-global FileIO byte-range cache:
        // it is shared by every test in this binary, and the question here is
        // decode cost, which the page cache already isolates on a local file.
        let dir = tempfile::tempdir().unwrap();
        let paths = wide_fixture(dir.path(), 32);
        let (serialized, decoded) = fixture_metadata_bytes(&paths);
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        println!("serialized_footer_total={serialized} decoded_metadata_total={decoded}");
        // `reuse=off` is the repeat cost this card started from: the metadata
        // work a second identical request still did. `reuse=on` is the same
        // request against a cache the first one populated. Cold and warm are
        // reported separately and neither is a field measurement.
        let shared = cache();
        for reuse in ["off", "on"] {
            for attempt in 0..6 {
                let fresh = cache();
                let measured = if reuse == "on" { &shared } else { &fresh };
                let start = std::time::Instant::now();
                let got = read(
                    &io(),
                    &paths,
                    "value",
                    42,
                    10,
                    true,
                    &pool(RESERVATION_BYTES),
                    measured,
                )
                .await
                .unwrap();
                let result = got.result.unwrap();
                println!(
                    "reuse={reuse} attempt={attempt} elapsed_ms={:.2} \
                     files_read={} groups_read={} rows_read={} footer_bytes={} \
                     fetched_bytes={} reads={} retained_bytes={}",
                    start.elapsed().as_secs_f64() * 1000.0,
                    result.files_read,
                    result.groups_read,
                    result.rows_read,
                    result.footer_bytes,
                    result.fetched_bytes,
                    result.reads,
                    shared.bytes(),
                );
            }
        }
        // Where a warm repeat actually spends its time, phase by phase.
        let budget = Arc::new(AtomicU64::new(u64::MAX));
        for label in ["open+head", "decode", "memory_size", "arrow_schema"] {
            let start = std::time::Instant::now();
            for (path, _, _) in &paths {
                let input = io().new_input(path).unwrap();
                let size = input.metadata().await.unwrap();
                if label == "open+head" {
                    continue;
                }
                let reader: Box<dyn FileRead> = Box::new(BudgetedReader {
                    inner: input.reader().await.unwrap(),
                    remaining: budget.clone(),
                });
                let builder = ParquetRecordBatchStreamBuilder::new_with_options(
                    ArrowFileReader::new(size, reader),
                    metadata_options(Some(0)),
                )
                .await
                .unwrap();
                match label {
                    "memory_size" => {
                        std::hint::black_box(builder.metadata().memory_size());
                    }
                    "arrow_schema" => {
                        std::hint::black_box(
                            ArrowReaderMetadata::try_new(
                                builder.metadata().clone(),
                                ArrowReaderOptions::new(),
                            )
                            .unwrap(),
                        );
                    }
                    _ => {}
                }
            }
            println!(
                "phase={label} cumulative_ms={:.2}",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
    }

    #[tokio::test]
    async fn numeric_topk_cancellation_drops_its_memory_reservation() {
        let memory = pool(RESERVATION_BYTES);
        let paths = vec![("never-opened.parquet".to_string(), 1, 100)];
        let io = io();
        let shared = cache();
        let mut future = Box::pin(read(&io, &paths, "value", 42, 1, true, &memory, &shared));
        assert!(futures::poll!(&mut future).is_pending());
        assert_eq!(memory.reserved(), RESERVATION_BYTES);
        drop(future);
        assert_eq!(memory.reserved(), 0);
    }

    /// Two Int64 columns with field ids and contents that differ, so a reused
    /// footer cannot answer for the wrong one by accident.
    fn write_two_columns(
        dir: &std::path::Path,
        name: &str,
        first: Vec<Option<i64>>,
        second: Vec<Option<i64>>,
        reversed: bool,
    ) -> (String, u64, u64) {
        let rows = first.len();
        let mut fields = vec![
            Field::new("value", DataType::Int64, true)
                .with_metadata([("PARQUET:field_id".to_string(), "42".to_string())].into()),
            Field::new("other", DataType::Int64, true)
                .with_metadata([("PARQUET:field_id".to_string(), "43".to_string())].into()),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(first)),
            Arc::new(Int64Array::from(second)),
        ];
        if reversed {
            fields.reverse();
            columns.reverse();
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let path = dir.join(name);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema,
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        (
            path.to_string_lossy().into_owned(),
            rows as u64,
            std::fs::metadata(&path).unwrap().len(),
        )
    }

    /// A repeat of the SAME query on the SAME objects must stop re-parsing
    /// footers, and must still answer from decoded rows — NULLs, ties and the
    /// signed extremes included — with read accounting that claims only the
    /// bytes it actually asked storage for.
    #[tokio::test]
    async fn numeric_topk_repeat_reuses_decoded_footers_and_keeps_exact_answers() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![
            Some(i64::MIN),
            None,
            Some(7),
            Some(7),
            None,
            Some(-1),
            Some(i64::MAX),
            Some(i64::MAX),
            Some(3),
        ];
        let paths = ["a", "b", "c"]
            .map(|n| write(dir.path(), &format!("{n}.parquet"), rows.clone(), true))
            .to_vec();
        let shared = cache();
        // One cold attempt, so what a repeat saves is a measured difference
        // rather than an absence.
        let cold = read(
            &io(),
            &paths,
            "value",
            42,
            1,
            true,
            &pool(RESERVATION_BYTES),
            &shared,
        )
        .await
        .unwrap();
        let cold = cold.result.unwrap();
        assert!(cold.footer_bytes > 0 && cold.reads >= 6);
        for ascending in [true, false] {
            for k in [1, 4, 9, 40] {
                let mut expected: Vec<i64> = std::iter::repeat_n(rows.iter().flatten(), 3)
                    .flatten()
                    .copied()
                    .collect();
                expected.sort_unstable();
                if !ascending {
                    expected.reverse();
                }
                expected.truncate(k);
                for _ in 0..3 {
                    let got = read(
                        &io(),
                        &paths,
                        "value",
                        42,
                        k,
                        ascending,
                        &pool(RESERVATION_BYTES),
                        &shared,
                    )
                    .await
                    .unwrap();
                    let result = got.result.unwrap();
                    assert_eq!(result.values, expected, "k={k} ascending={ascending}");
                    assert_eq!(result.files_read, 3);
                    assert_eq!(result.groups_considered, 9);
                    assert_eq!(
                        result.footer_bytes, 0,
                        "a reused footer must not claim footer bytes it never read"
                    );
                    assert!(
                        result.reads < cold.reads,
                        "a reused footer must not re-open every file"
                    );
                }
            }
        }
        assert!(shared.bytes() > 0);
    }

    /// Object identity, not path: a replaced object of a different length is a
    /// different key, and a file this snapshot has never seen is simply a miss.
    #[tokio::test]
    async fn numeric_topk_metadata_reuse_is_keyed_by_object_not_path() {
        let dir = tempfile::tempdir().unwrap();
        let shared = cache();
        let first = write(
            dir.path(),
            "same-name.parquet",
            vec![Some(1), Some(2)],
            true,
        );
        let answer = |paths: &[(String, u64, u64)]| {
            let paths = paths.to_vec();
            let shared = &shared;
            async move {
                read(
                    &io(),
                    &paths,
                    "value",
                    42,
                    1,
                    false,
                    &pool(RESERVATION_BYTES),
                    shared,
                )
                .await
                .unwrap()
                .result
                .unwrap()
            }
        };
        assert_eq!(answer(std::slice::from_ref(&first)).await.values, vec![2]);
        assert_eq!(answer(std::slice::from_ref(&first)).await.footer_bytes, 0);

        // Same path, different object: a longer row set gives a different
        // manifest length, so the stale entry can never be served.
        let replaced = write(
            dir.path(),
            "same-name.parquet",
            vec![Some(10), Some(20), Some(30), Some(40)],
            true,
        );
        assert_ne!(replaced.2, first.2, "the fixture must change the length");
        let fresh = answer(std::slice::from_ref(&replaced)).await;
        assert_eq!(fresh.values, vec![40]);
        assert!(fresh.footer_bytes > 0, "a new object must be read");

        // A later snapshot adds a file: the retained one hits, the new one does not.
        let added = write(dir.path(), "added.parquet", vec![Some(99)], true);
        let grown = answer(&[replaced.clone(), added]).await;
        assert_eq!(grown.values, vec![99]);
        assert_eq!(grown.files_read, 2);
        assert!(grown.reads > 0 && grown.reads < fresh.reads * 2);
    }

    /// A narrowed footer carries ONE leaf's statistics. Asking for the other
    /// column must decode its own entry rather than refuse on absent bounds.
    #[tokio::test]
    async fn numeric_topk_narrowed_statistics_never_answer_for_another_column() {
        let dir = tempfile::tempdir().unwrap();
        let shared = cache();
        let paths = (0..3)
            .map(|i| {
                write_two_columns(
                    dir.path(),
                    &format!("pair-{i}.parquet"),
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(70), Some(80), Some(90)],
                    false,
                )
            })
            .collect::<Vec<_>>();
        for _ in 0..2 {
            for (column, field, want) in [("value", 42, 3), ("other", 43, 90)] {
                let result = read(
                    &io(),
                    &paths,
                    column,
                    field,
                    1,
                    false,
                    &pool(RESERVATION_BYTES),
                    &shared,
                )
                .await
                .unwrap()
                .result
                .unwrap();
                assert_eq!(result.values, vec![want], "column={column}");
            }
        }
        // A file that puts the target leaf elsewhere refuses rather than reading
        // a neighbour's bounds, and refuses before any data read.
        let reversed = write_two_columns(
            dir.path(),
            "reversed.parquet",
            vec![Some(1), Some(2), Some(3)],
            vec![Some(70), Some(80), Some(90)],
            true,
        );
        let metrics = ScanMetrics::default();
        let mixed = vec![paths[0].clone(), reversed];
        assert!(read_inner(
            &io(),
            &mixed,
            "value",
            42,
            1,
            false,
            &pool(RESERVATION_BYTES),
            &shared,
            &metrics,
            MAX_RETAINED_METADATA_BYTES,
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(metrics.scan_counters().bytes_data.load(Relaxed), 0);
    }

    /// Under byte pressure the cache stops retaining and the route keeps
    /// working — same answers, footers read again — and a cached footer that
    /// lacks statistics still refuses rather than guessing.
    #[tokio::test]
    async fn numeric_topk_metadata_reuse_survives_eviction_and_still_refuses() {
        let dir = tempfile::tempdir().unwrap();
        let paths = (0..4)
            .map(|i| {
                write(
                    dir.path(),
                    &format!("evict-{i}.parquet"),
                    vec![Some(i), Some(i + 100)],
                    true,
                )
            })
            .collect::<Vec<_>>();
        for starved in [
            MetadataCache::with_limits(4096, 1),
            MetadataCache::with_limits(1, 1 << 30),
        ] {
            let mut reads = Vec::new();
            for _ in 0..3 {
                let result = read(
                    &io(),
                    &paths,
                    "value",
                    42,
                    2,
                    false,
                    &pool(RESERVATION_BYTES),
                    &starved,
                )
                .await
                .unwrap()
                .result
                .unwrap();
                assert_eq!(result.values, vec![103, 102]);
                assert!(result.footer_bytes > 0, "a starved cache must re-read");
                reads.push(result.reads);
            }
            assert_eq!(reads[0], reads[2], "no entry survives to change the cost");
        }
        // Missing statistics refuse on the repeat exactly as on the first read.
        let missing = write(dir.path(), "missing.parquet", vec![Some(5)], false);
        let shared = cache();
        for _ in 0..2 {
            let attempt = read(
                &io(),
                std::slice::from_ref(&missing),
                "value",
                42,
                1,
                true,
                &pool(RESERVATION_BYTES),
                &shared,
            )
            .await
            .unwrap();
            assert!(attempt.result.is_none());
        }
    }
}
