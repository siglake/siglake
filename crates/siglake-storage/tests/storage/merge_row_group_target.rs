//! The merge output writer's row group follows the configured BYTE target
//! (`IcebergTuning::target_row_group_bytes` /
//! `SIGLAKE_PARQUET_TARGET_ROW_GROUP_BYTES`), on every path that writes through
//! it.
//!
//! THE DEFECT (task #4754). `build_merge_output_writer` asked for its row-group
//! size with no sample batch, and the no-sample branch returns a flat
//! 1,048,576 rows without ever reading the target it was handed. So leveled
//! compaction merges, re-clustering and the streaming delete-task survivor
//! rewrite all wrote 1 Mi-row row groups whatever the target said — and the
//! open row group is buffered DECODED whenever a row-group bloom column is set,
//! which final merge output always sets. Below one row group the buffer is the
//! whole output.
//!
//! Each test runs two arms over the same fixture, differing only in the target,
//! and reads the emitted row groups out of the output footer. Before the fix
//! both arms emit ONE row group (the fixture is well under 1 Mi rows), so the
//! arms cannot differ; after it the target decides. One arm sits at the
//! `MIN_ROW_GROUP_ROWS` clamp, which pins the derived value exactly; the other
//! is sized ABOVE the clamp, the regime where the target and not a clamp is
//! what the writer follows. Rows, order and bloom-per-row-group alignment are
//! asserted in both arms: the row-group size decides how the writer forms
//! groups, and a bloom that no longer lines up with its group prunes rows that
//! match.

use arrow_array::Array;
use chrono::{TimeZone, Utc};
use siglake_bloom::RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY;
use siglake_core::Event;
use siglake_storage::iceberg::{
    IcebergContext, IcebergTuning, ReclusterMergeOptions, BLOOM_FILTER_COLUMNS,
    GROUP_COUNTS_KV_KEY, MIN_ROW_GROUP_ROWS,
};

/// Enough rows that the clamped arm needs a second row group and the sized arm
/// can land strictly between the clamp and the whole output.
const ROWS: usize = 200_000;
/// Rows the sized arm asks for: above `MIN_ROW_GROUP_ROWS` (131,072) and below
/// `ROWS`, so its first row group is neither clamped nor the whole file.
const SIZED_ARM_ROWS: usize = 160_000;
const FILES: usize = 4;

fn events(file: usize, n: usize, base_ns: i64) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::now(format!("GET /api/v1/logs 200 in 13ms seq={i} f={file}"));
            // NANOSECOND spacing so the whole fixture lands in one day
            // partition: a merge split across partitions writes an output file
            // per partition and the row groups below would be of neither.
            e.timestamp = chrono::DateTime::from_timestamp_nanos(base_ns + (file * n + i) as i64);
            e.sourcetype = "app:json".into();
            e
        })
        .collect()
}

fn base_ns() -> i64 {
    Utc.timestamp_opt(1_767_225_600, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}

/// What one arm produced: the row counts of the output's row groups, its
/// timestamps in file order, the number of row-group blooms the footer carries,
/// and the decoded size of one of its own rows (which turns a row count into a
/// byte target without hard-coding a row size the schema can change).
struct ArmOutput {
    row_groups: Vec<usize>,
    timestamps: Vec<i64>,
    blooms: usize,
    has_group_counts: bool,
    row_bytes: usize,
}

impl ArmOutput {
    fn read(path: &std::path::Path) -> Self {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = std::fs::read(path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
        let meta = builder.metadata().clone();
        let row_groups: Vec<usize> = meta
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .collect();
        let kv = meta.file_metadata().key_value_metadata();
        let blooms = kv
            .and_then(|kv| {
                kv.iter()
                    .find(|e| e.key == RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY)
                    .and_then(|e| e.value.clone())
            })
            .map(|hex| {
                siglake_bloom::rowgroup_blooms_from_hex(&hex)
                    .expect("blooms decode")
                    .len()
            })
            .unwrap_or(0);
        let has_group_counts = kv
            .map(|kv| kv.iter().any(|e| e.key == GROUP_COUNTS_KV_KEY))
            .unwrap_or(false);

        let mut timestamps = Vec::with_capacity(ROWS);
        let mut row_bytes = 0usize;
        let reader = builder.with_batch_size(8192).build().unwrap();
        for rb in reader {
            let rb = rb.unwrap();
            if row_bytes == 0 {
                // Priced the way the writer prices its sample: the extent the
                // rows actually span, not the capacity of the buffers holding
                // them. `get_array_memory_size` reads ~25% higher here, which
                // is enough to push a target sized for one row count into
                // producing another.
                let bytes: usize = rb
                    .columns()
                    .iter()
                    .map(|c| c.to_data().get_slice_memory_size().unwrap())
                    .sum();
                row_bytes = bytes / rb.num_rows();
            }
            let idx = rb
                .schema()
                .index_of(siglake_core::nanos_source_column(
                    rb.schema().as_ref(),
                    "timestamp",
                ))
                .unwrap();
            let col = siglake_core::column_nanos(rb.column(idx)).unwrap();
            for r in 0..rb.num_rows() {
                timestamps.push(col.value(r));
            }
        }
        Self {
            row_groups,
            timestamps,
            blooms,
            has_group_counts,
            row_bytes,
        }
    }

    fn rows(&self) -> usize {
        self.row_groups.iter().sum()
    }
}

/// Run one merge arm end to end and read its single output file.
async fn merge_arm(
    target_row_group_bytes: Option<usize>,
    merge: &ReclusterMergeOptions,
) -> ArmOutput {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            target_row_group_bytes,
            ..Default::default()
        });
    let base = base_ns();
    for f in 0..FILES {
        ice.append_events(&events(f, ROWS / FILES, base))
            .await
            .unwrap();
    }

    let ident = ice.events_table_ident().clone();
    let inputs = ice.live_data_files(&ident).await.unwrap();
    assert!(inputs.len() >= FILES, "expected {FILES} input files");
    let stats = ice
        .recluster_files_with(&ident, inputs, BLOOM_FILTER_COLUMNS, merge)
        .await
        .expect("recluster");
    assert_eq!(stats.rows, ROWS, "merge conserves rows");

    let out = ice.live_data_files(&ident).await.unwrap();
    assert_eq!(out.len(), 1, "fixture must merge into ONE output file");
    let path = out[0].file_path().trim_start_matches("file://").to_string();
    let read = ArmOutput::read(std::path::Path::new(&path));
    assert_eq!(read.rows(), ROWS, "output file holds every row");
    read
}

fn slice_streaming() -> ReclusterMergeOptions {
    ReclusterMergeOptions {
        force_streaming: Some(true),
        merge_fanin: Some(64),
        force_tiered: Some(false),
        inram_max_rows: Some(1),
        inram_max_bytes: Some(1),
        ..Default::default()
    }
}

fn page_bounded() -> ReclusterMergeOptions {
    ReclusterMergeOptions {
        force_streaming: Some(true),
        // Below the file count, so the bin takes the page-bounded plan merge —
        // the path that writes zero-copy SLICES of decoded input parts, where a
        // row size read off the whole part behind a slice would size every row
        // group at the clamp whatever the target said.
        merge_fanin: Some(2),
        force_tiered: Some(false),
        merge_chunk_rows: Some(16 * 1024),
        ..Default::default()
    }
}

async fn assert_target_drives_row_groups(path: &str, merge: ReclusterMergeOptions) {
    let clamped = merge_arm(Some(1), &merge).await;
    assert_eq!(
        clamped.row_groups[0], MIN_ROW_GROUP_ROWS,
        "{path}: a 1-byte target must clamp to the floor, got {:?}",
        clamped.row_groups
    );
    assert!(
        clamped.row_groups.len() > 1,
        "{path}: {ROWS} rows at the floor must need more than one row group"
    );

    // The same fixture with a target sized for SIZED_ARM_ROWS of these rows,
    // measured off the clamped arm's own output.
    let sized = merge_arm(Some(clamped.row_bytes * SIZED_ARM_ROWS), &merge).await;
    let first = sized.row_groups[0];
    assert!(
        first > MIN_ROW_GROUP_ROWS && first < ROWS,
        "{path}: the sized arm must land between the floor and the whole fixture, got {:?}",
        sized.row_groups
    );
    // ... and near what was asked for. The writer measures the slice it is
    // handed, this test measures a decoded read of the output, so the two
    // differ by buffer slack — but not by the factor that a row-group size read
    // off the wrong sample (or off no sample) would show.
    assert!(
        first * 2 > SIZED_ARM_ROWS && first < SIZED_ARM_ROWS * 2,
        "{path}: asked for ~{SIZED_ARM_ROWS} rows per group, got {:?}",
        sized.row_groups
    );

    // Same rows, same order, in both arms.
    assert_eq!(
        clamped.timestamps, sized.timestamps,
        "{path}: the two arms must write the same rows in the same order"
    );
    let mut sorted = clamped.timestamps.clone();
    sorted.sort_unstable();
    assert_eq!(clamped.timestamps, sorted, "{path}: output is time-sorted");

    for (label, arm) in [("clamped", &clamped), ("sized", &sized)] {
        assert_eq!(
            arm.blooms,
            arm.row_groups.len(),
            "{path}/{label}: {} blooms for {} row groups — every prune after this \
             is made against the wrong row group's bloom",
            arm.blooms,
            arm.row_groups.len()
        );
        assert!(
            arm.has_group_counts,
            "{path}/{label}: merge output lost its group-count footer"
        );
    }
}

#[tokio::test]
async fn slice_streaming_merge_row_groups_follow_the_byte_target() {
    assert_target_drives_row_groups("slice_streaming", slice_streaming()).await;
}

#[tokio::test]
async fn page_bounded_merge_row_groups_follow_the_byte_target() {
    assert_target_drives_row_groups("page_bounded", page_bounded()).await;
}
