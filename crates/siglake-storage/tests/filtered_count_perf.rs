//! Own test binary: the correctness test resets and compares Iceberg's
//! process-global object-store byte counter, and the ignored test reports
//! wall-clock timings.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::arrow::{
    object_store_bytes_read, reset_object_store_bytes_read, ArrowReader, ArrowReaderBuilder,
};
use iceberg::expr::{Bind, BoundPredicate, Reference};
use iceberg::io::FileIO;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, SchemaRef, Type};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use siglake_storage::default_writer_properties;
use tempfile::TempDir;

fn bench_schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "level", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "region", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "raw", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn bench_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("level", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
        Field::new("raw", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "3".to_string(),
        )])),
    ]))
}

fn bench_predicate(schema: &SchemaRef) -> BoundPredicate {
    Reference::new("level")
        .equal_to(Datum::string("error"))
        .bind(schema.clone(), true)
        .unwrap()
}

fn write_bench_file(total_rows: usize) -> (TempDir, FileIO, String, SchemaRef, u64, usize) {
    const BATCH_ROWS: usize = 20_000;

    let tmp_dir = TempDir::new().unwrap();
    let file_path = tmp_dir
        .path()
        .join("filtered-count-bench.parquet")
        .to_string_lossy()
        .to_string();
    let schema = bench_schema();
    let arrow_schema = bench_arrow_schema();
    let file_io = FileIO::new_with_fs();
    let levels = ["info", "warn", "error", "debug"];
    let regions = ["us-east-1", "us-east-2", "eu-west-1", "ap-south-1"];
    let raw_suffix = "x".repeat(192);

    let file = File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        arrow_schema.clone(),
        Some(default_writer_properties()),
    )
    .unwrap();

    for batch_start in (0..total_rows).step_by(BATCH_ROWS) {
        let batch_end = (batch_start + BATCH_ROWS).min(total_rows);
        let mut level_values = Vec::with_capacity(batch_end - batch_start);
        let mut region_values = Vec::with_capacity(batch_end - batch_start);
        let mut raw_values = Vec::with_capacity(batch_end - batch_start);

        for row in batch_start..batch_end {
            level_values.push(levels[row % levels.len()]);
            region_values.push(regions[(row / 2) % regions.len()]);
            raw_values.push(format!(
                "raw-{row:07}-{}-{raw_suffix}",
                regions[(row / 2) % regions.len()]
            ));
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(StringArray::from(level_values)) as ArrayRef,
                Arc::new(StringArray::from(region_values)) as ArrayRef,
                Arc::new(StringArray::from(raw_values)) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    let file_size = std::fs::metadata(&file_path).unwrap().len();
    (tmp_dir, file_io, file_path, schema, file_size, total_rows)
}

async fn collect_scan_batches(
    reader: ArrowReader,
    file_path: &str,
    schema: SchemaRef,
    file_size: u64,
    project_field_ids: Vec<i32>,
    predicate: Option<BoundPredicate>,
) -> Vec<RecordBatch> {
    let tasks = Box::pin(futures::stream::iter(vec![Ok(FileScanTask {
        file_size_in_bytes: file_size,
        start: 0,
        length: 0,
        record_count: None,
        data_file_path: file_path.to_string(),
        data_file_format: DataFileFormat::Parquet,
        schema,
        project_field_ids,
        predicate,
        deletes: vec![],
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: true,
        statistics_blobs: vec![],
    })])) as FileScanTaskStream;

    reader
        .read(tasks)
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap()
}

fn measure_duration<F, T>(mut f: F) -> (Duration, T)
where
    F: FnMut() -> T,
{
    let start = Instant::now();
    let value = f();
    (start.elapsed(), value)
}

async fn measure_filtered_scan(
    file_io: FileIO,
    file_path: &str,
    schema: SchemaRef,
    file_size: u64,
    project_field_ids: Vec<i32>,
    predicate: BoundPredicate,
    row_selection_enabled: bool,
) -> (Duration, usize, u64) {
    let reader = ArrowReaderBuilder::new(file_io)
        .with_row_group_filtering_enabled(true)
        .with_row_selection_enabled(row_selection_enabled)
        .build();

    let _ = collect_scan_batches(
        reader.clone(),
        file_path,
        schema.clone(),
        file_size,
        project_field_ids.clone(),
        Some(predicate.clone()),
    )
    .await;

    reset_object_store_bytes_read();
    let start = Instant::now();
    let batches = collect_scan_batches(
        reader,
        file_path,
        schema,
        file_size,
        project_field_ids,
        Some(predicate),
    )
    .await;
    let elapsed = start.elapsed();
    let rows = batches.iter().map(RecordBatch::num_rows).sum();
    let bytes = object_store_bytes_read();
    (elapsed, rows, bytes)
}

#[tokio::test]
#[ignore = "microbenchmark for filtered count scan pathology"]
async fn filtered_count_empty_projection_microbench() {
    let (_tmp_dir, file_io, file_path, schema, file_size, total_rows) = write_bench_file(1_200_000);
    let predicate = bench_predicate(&schema);

    let level_only_batches = collect_scan_batches(
        ArrowReaderBuilder::new(file_io.clone()).build(),
        &file_path,
        schema.clone(),
        file_size,
        vec![1],
        None,
    )
    .await;
    let level_rows: usize = level_only_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(level_rows, total_rows);

    let (predicate_eval_ms, matched_from_predicate_eval) = measure_duration(|| {
        level_only_batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .filter(|value| matches!(value, Some("error")))
                    .count()
            })
            .sum::<usize>()
    });

    let (empty_projection_ms, empty_projection_rows, empty_projection_bytes) =
        measure_filtered_scan(
            file_io.clone(),
            &file_path,
            schema.clone(),
            file_size,
            vec![],
            predicate.clone(),
            true,
        )
        .await;
    let (narrow_projection_ms, narrow_projection_rows, narrow_projection_bytes) =
        measure_filtered_scan(
            file_io.clone(),
            &file_path,
            schema.clone(),
            file_size,
            vec![1],
            predicate.clone(),
            true,
        )
        .await;
    let (narrow_no_row_selection_ms, narrow_no_row_selection_rows, _) = measure_filtered_scan(
        file_io,
        &file_path,
        schema,
        file_size,
        vec![1],
        predicate,
        false,
    )
    .await;

    let expected_matches = total_rows / 4;
    assert_eq!(matched_from_predicate_eval, expected_matches);
    assert_eq!(empty_projection_rows, expected_matches);
    assert_eq!(narrow_projection_rows, expected_matches);
    assert_eq!(narrow_no_row_selection_rows, expected_matches);

    println!("filtered_count_empty_projection_microbench total_rows={total_rows}");
    println!(
        "  predicate_eval_ms={:.3} matched_rows={matched_from_predicate_eval}",
        predicate_eval_ms.as_secs_f64() * 1000.0
    );
    println!(
        "  full_scan_empty_projection_ms={:.3} bytes={empty_projection_bytes}",
        empty_projection_ms.as_secs_f64() * 1000.0
    );
    println!(
        "  full_scan_narrow_projection_ms={:.3} bytes={narrow_projection_bytes}",
        narrow_projection_ms.as_secs_f64() * 1000.0
    );
    println!(
        "  full_scan_narrow_projection_no_row_selection_ms={:.3}",
        narrow_no_row_selection_ms.as_secs_f64() * 1000.0
    );
}

#[tokio::test]
async fn filtered_count_empty_projection_reads_only_predicate_columns() {
    let (_tmp_dir, file_io, file_path, schema, file_size, total_rows) = write_bench_file(100_000);
    let predicate = bench_predicate(&schema);
    let expected_matches = total_rows / 4;

    let (_empty_projection_ms, empty_projection_rows, empty_projection_bytes) =
        measure_filtered_scan(
            file_io.clone(),
            &file_path,
            schema.clone(),
            file_size,
            vec![],
            predicate.clone(),
            true,
        )
        .await;
    let (_narrow_projection_ms, narrow_projection_rows, narrow_projection_bytes) =
        measure_filtered_scan(
            file_io,
            &file_path,
            schema,
            file_size,
            vec![1],
            predicate,
            true,
        )
        .await;

    assert_eq!(empty_projection_rows, expected_matches);
    assert_eq!(narrow_projection_rows, expected_matches);
    assert!(
        empty_projection_bytes <= narrow_projection_bytes + 4096,
        "empty projection should read only predicate columns, got empty={empty_projection_bytes} narrow={narrow_projection_bytes}"
    );
}
