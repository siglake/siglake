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
use parquet::file::statistics::Statistics;

/// Reservation includes retained footers, one compressed/decompressed column
/// chunk, Arrow batches and the bounded heap. Refusal leaves the normal plan.
pub const RESERVATION_BYTES: usize = 128 * 1024 * 1024;
const MAX_FOOTER_BYTES: usize = 8 * 1024 * 1024;
const MAX_SINGLE_FOOTER_BYTES: usize = 256 * 1024;
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

struct File {
    path: String,
    metadata: ArrowReaderMetadata,
    leaf: usize,
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

pub(crate) async fn read(
    io: &FileIO,
    paths: &[(String, u64, u64)],
    column: &str,
    field_id: i32,
    k: usize,
    ascending: bool,
    pool: &Arc<dyn MemoryPool>,
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
    let mut result = NumericTopK {
        files_planned: paths.len() as u64,
        ..Default::default()
    };
    // Validate ALL metadata before decoding: a late refusal must not combine
    // partial optimized output with the normal execution result.
    for (path, expected_rows, expected_size) in paths {
        tokio::task::yield_now().await;
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
        footer_bytes = footer_bytes.saturating_add(length);
        if length > MAX_SINGLE_FOOTER_BYTES
            || footer_bytes > MAX_FOOTER_BYTES
            || length as u64 > size.size - 12
        {
            return Ok(refuse("footer_budget"));
        }
        let builder = ParquetRecordBatchStreamBuilder::new(
            ArrowFileReader::new(size, reader).with_scan_metrics(metrics.clone()),
        )
        .await?;
        // Serialized footer size is NOT its retained heap cost. Account for
        // the decoded metadata of every column, not just the target leaf.
        retained_metadata_bytes =
            retained_metadata_bytes.saturating_add(builder.metadata().memory_size());
        if retained_metadata_bytes > metadata_limit {
            return Ok(refuse("decoded_metadata_budget"));
        }
        if builder.metadata().file_metadata().num_rows() < 0
            || builder.metadata().file_metadata().num_rows() as u64 != *expected_rows
        {
            return Ok(refuse("row_count"));
        }
        let group_rows = builder
            .metadata()
            .row_groups()
            .iter()
            .try_fold(0u64, |sum, group| {
                sum.checked_add(u64::try_from(group.num_rows()).ok()?)
            });
        if group_rows != Some(*expected_rows) {
            return Ok(refuse("row_count"));
        }
        let schema = builder.parquet_schema();
        let Some(leaf) = schema
            .columns()
            .iter()
            .position(|c| c.path().parts() == [column])
        else {
            return Ok(refuse("column"));
        };
        let descriptor = schema.column(leaf);
        let info = descriptor.self_type().get_basic_info();
        if descriptor.max_rep_level() != 0
            || !info.has_id()
            || info.id() != field_id
            || builder
                .schema()
                .field_with_name(column)
                .ok()
                .map(|f| f.data_type())
                != Some(&DataType::Int64)
        {
            return Ok(refuse("field_identity"));
        }
        for (rg, group) in builder.metadata().row_groups().iter().enumerate() {
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
            metadata: ArrowReaderMetadata::try_new(
                builder.metadata().clone(),
                ArrowReaderOptions::new(),
            )?,
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
        let reader = ArrowFileReader::new(
            input.metadata().await?,
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
                &memory
            )
            .await
            .unwrap()
            .result
            .is_none());
        }
        assert!(read(&io(), &[good], "value", 42, 1, false, &pool(1))
            .await
            .unwrap()
            .result
            .is_none());
        assert_eq!(memory.reserved(), 0);
        let nulls = write(dir.path(), "nulls.parquet", vec![None, None, None], true);
        let result = read(&io(), &[nulls], "value", 42, 1, false, &memory)
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
        let refused = read(&io(), &[wrong_size], "value", 42, 1, true, &memory)
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

    #[tokio::test]
    async fn numeric_topk_cancellation_drops_its_memory_reservation() {
        let memory = pool(RESERVATION_BYTES);
        let paths = vec![("never-opened.parquet".to_string(), 1, 100)];
        let io = io();
        let mut future = Box::pin(read(&io, &paths, "value", 42, 1, true, &memory));
        assert!(futures::poll!(&mut future).is_pending());
        assert_eq!(memory.reserved(), RESERVATION_BYTES);
        drop(future);
        assert_eq!(memory.reserved(), 0);
    }
}
