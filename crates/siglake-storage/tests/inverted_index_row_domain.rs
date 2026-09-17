//! Task #4558: an inverted index the reader cannot trust must send the scan
//! back to an exact scan rather than prune with it.
//!
//! Own test binary because it installs a process-global metrics recorder and
//! reads the reader's pruning counters as deltas between arms.
//!
//! Each arm writes its own Parquet file, so the reader's parsed-index and
//! footer caches (both keyed by path) cannot carry one arm's decision into the
//! next. The rows put a match in the **final** row group and one past the
//! short index's domain, which is the case that loses answers: postings the
//! file cannot place are dropped by `row_selection_runs`, so the rows past the
//! index's end are skipped before decode and never reach the exact predicate.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::arrow::{ArrowReaderBuilder, RawPruneSpec};
use iceberg::io::FileIO;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, SchemaRef, Type};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use siglake_index::InvertedIndex;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, label: Option<(&str, &str)>) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && label.is_none_or(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|entry| entry.key() == lk && entry.value() == lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(value) => *value,
            _ => 0,
        })
        .sum()
}

const ROWS: usize = 30;
const ROW_GROUP_ROWS: usize = 10;
/// Row 29 is the last row of the last row group; row 24 is past the 20-row
/// domain the short index claims.
const NEEDLE_ROWS: [usize; 4] = [0, 13, 24, 29];

fn raw_rows() -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            if NEEDLE_ROWS.contains(&row) {
                format!("row {row:04} needle present")
            } else {
                format!("row {row:04} quiet chatter")
            }
        })
        .collect()
}

fn iceberg_schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![NestedField::required(
                1,
                "raw",
                Type::Primitive(PrimitiveType::String),
            )
            .into()])
            .build()
            .unwrap(),
    )
}

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "raw",
        DataType::Utf8,
        false,
    )
    .with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        "1".to_string(),
    )]))]))
}

/// `ROWS` rows of `raw` in `ROW_GROUP_ROWS`-row row groups, carrying
/// `index_hex` as the file's inverted-index footer KV.
fn write_data_file(dir: &Path, name: &str, index_hex: Option<&str>) -> (String, u64) {
    let path = dir.join(name);
    let schema = arrow_schema();
    let mut properties = WriterProperties::builder().set_max_row_group_size(ROW_GROUP_ROWS);
    if let Some(hex) = index_hex {
        properties = properties.set_key_value_metadata(Some(vec![KeyValue::new(
            siglake_index::INVERTED_INDEX_KV_KEY.to_string(),
            hex.to_string(),
        )]));
    }
    let rows = raw_rows();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(rows)) as ArrayRef],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema,
        Some(properties.build()),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(
        metadata.row_groups().len(),
        ROWS / ROW_GROUP_ROWS,
        "the fixture needs several row groups so a match can sit in the last one"
    );
    let size = std::fs::metadata(&path).unwrap().len();
    (path.to_string_lossy().to_string(), size)
}

/// Scan `path` with the reader's `needle` prune hint and return the `raw`
/// values it actually decoded. A rejected index leaves no row selection, so
/// the whole file comes back and the exact predicate runs above the scan.
async fn scan_raw_values(path: &str, size: u64) -> Vec<String> {
    let reader = ArrowReaderBuilder::new(FileIO::new_with_fs())
        .with_row_selection_enabled(true)
        .with_raw_prune_spec(Some(RawPruneSpec {
            column: "raw".to_string(),
            all_terms: vec!["needle".to_string()],
            ..Default::default()
        }))
        .build();
    let task = FileScanTask {
        file_size_in_bytes: size,
        start: 0,
        length: 0,
        record_count: None,
        data_file_path: path.to_string(),
        data_file_format: DataFileFormat::Parquet,
        schema: iceberg_schema(),
        project_field_ids: vec![1],
        predicate: None,
        deletes: vec![],
        partition: None,
        partition_spec: None,
        name_mapping: None,
        case_sensitive: true,
        statistics_blobs: vec![],
    };
    let batches = reader
        .read(Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream)
        .unwrap()
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..column.len())
                .map(|idx| column.value(idx).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A blob header claiming four billion postings behind two bytes of payload —
/// the shape that used to reach `Vec::with_capacity` from a serialized count.
fn hostile_count_blob() -> Vec<u8> {
    let mut out = b"KIDX\x01".to_vec();
    for value in [ROWS as u64, 1, 6] {
        write_varint(&mut out, value);
    }
    out.extend_from_slice(b"needle");
    write_varint(&mut out, u32::MAX as u64);
    out.extend_from_slice(&[1, 1]);
    out
}

fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

#[tokio::test]
async fn a_footer_index_the_decoder_or_the_row_domain_rejects_falls_back_to_an_exact_scan() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let rows = raw_rows();
    let needles: Vec<String> = NEEDLE_ROWS.iter().map(|row| rows[*row].clone()).collect();
    let valid = InvertedIndex::from_rows(rows.iter().map(String::as_str));
    assert_eq!(valid.n_rows(), ROWS as u32);

    // A whole-batch index stamped onto a file the rolling writer cut short:
    // `n_rows` covers 20 of the file's 30 rows, so the file's matches at rows
    // 24 and 29 are outside anything the postings can name.
    let short_domain = InvertedIndex::from_rows(rows[..20].iter().map(String::as_str));
    assert_eq!(short_domain.n_rows(), 20);
    // The other direction: a domain wider than the file.
    let inflated_domain = InvertedIndex::from_rows(
        rows.iter()
            .map(String::as_str)
            .chain(std::iter::repeat_n("", 5)),
    );
    assert_eq!(inflated_domain.n_rows(), ROWS as u32 + 5);
    let truncated = {
        let bytes = valid.to_bytes();
        bytes[..bytes.len() - 1].to_vec()
    };

    // arm, blob, whether the reader should prune with it, whether the
    // rejection is a row-domain one (rather than the decoder's).
    let arms: Vec<(&str, Option<String>, bool, bool)> = vec![
        ("valid", Some(hex(&valid.to_bytes())), true, false),
        ("no_index", None, false, false),
        ("truncated", Some(hex(&truncated)), false, false),
        (
            "hostile_counts",
            Some(hex(&hostile_count_blob())),
            false,
            false,
        ),
        (
            "short_domain",
            Some(hex(&short_domain.to_bytes())),
            false,
            true,
        ),
        (
            "inflated_domain",
            Some(hex(&inflated_domain.to_bytes())),
            false,
            true,
        ),
    ];

    for (arm, blob, prunes, domain_rejected) in arms {
        let (path, size) = write_data_file(tmp.path(), &format!("{arm}.parquet"), blob.as_deref());
        let decoded = scan_raw_values(&path, size).await;
        let snapshot = snapshotter.snapshot().into_vec();
        let used = counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "footer_kv")),
        );
        let mismatched = counter_sum(
            &snapshot,
            "siglake_index_row_domain_mismatch_total",
            Some(("storage", "footer_kv")),
        );

        // The answer never changes, whatever the blob says.
        for needle in &needles {
            assert!(
                decoded.contains(needle),
                "arm {arm} lost the match {needle:?} (decoded {} rows)",
                decoded.len()
            );
        }
        if prunes {
            assert_eq!(used, 1, "arm {arm} should prune with its index");
            assert_eq!(
                decoded.len(),
                NEEDLE_ROWS.len(),
                "arm {arm} should decode only the matching rows"
            );
        } else {
            assert_eq!(used, 0, "arm {arm} must not prune with its index");
            assert_eq!(
                decoded.len(),
                ROWS,
                "arm {arm} should fall back to decoding the whole file"
            );
        }
        assert_eq!(
            mismatched,
            u64::from(domain_rejected),
            "arm {arm} row-domain rejections"
        );
    }
}
