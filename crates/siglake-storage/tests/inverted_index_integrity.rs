//! #4991: what a query does when a v1 inverted-index blob is corrupt **in
//! storage**, per storage path, cold and warm.
//!
//! `siglake-index/tests/v1_integrity_measure.rs` prices the formats; this is
//! the reader's disposition, which is different on each path and is the part
//! `docs/LIMITATIONS.md` has to state:
//!
//! - **footer KV** (hex in the Parquet footer): nothing covers the stored
//!   bytes. A flip that still decodes and still matches the file's row count
//!   passes both of #4558's layers, prunes, and the query answers **short** —
//!   no error, no counter.
//! - **Puffin sidecar, as written** (`CompressionCodec::Zstd`,
//!   `include_checksum(true)`): the flip hits the Zstd frame and
//!   `PuffinReader::blob` errors. `ArrowReader` propagates it
//!   (`third_party/iceberg/src/arrow/reader.rs`), so the **query fails**. It is
//!   not a fallback to a scan, and it is not a short answer.
//! - **Puffin sidecar with `CompressionCodec::None`**: the same exposure as the
//!   footer KV. Nothing in the path covers a blob; the codec does. No siglake
//!   writer selects `None` today (`crates/siglake-storage/src/iceberg.rs`
//!   registers Zstd), so this arm is what the cover is worth, not a shipped
//!   state.
//! - **warm**: neither path re-verifies anything. A parsed index already in the
//!   cache answers from memory, so a file corrupted after a query warmed it
//!   keeps answering correctly until the entry is evicted.
//!
//! One test function, sequential arms: the metrics recorder is process-global
//! and `snapshot()` drains every counter, so concurrent tests would steal each
//! other's deltas.
//!
//! Own test binary (it installs that recorder).

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use iceberg::arrow::{ArrowReaderBuilder, RawPruneSpec};
use iceberg::io::FileIO;
use iceberg::puffin::{Blob, CompressionCodec, PuffinReader, PuffinWriter};
use iceberg::scan::{FileScanTask, FileScanTaskStream, StatisticsBlobReference};
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, SchemaRef, Type};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
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

const ROWS: usize = 400;
const ROW_GROUP_ROWS: usize = 100;
/// The term the query asks for, and the rows that carry it — including one in
/// the file's last row group, the position a wrong row selection loses.
const NEEDLE: &str = "needle";
const NEEDLE_ROWS: [usize; 4] = [0, 137, 240, 399];
const BLOB_TYPE: &str = "siglake-inverted-v1";

fn raw_rows() -> Vec<String> {
    (0..ROWS)
        .map(|row| {
            let needle = if NEEDLE_ROWS.contains(&row) {
                " needle"
            } else {
                ""
            };
            format!("service-{} status {}{needle} row-{row:06}", row % 20, 200)
        })
        .collect()
}

fn needle_texts(rows: &[String]) -> Vec<String> {
    NEEDLE_ROWS.iter().map(|row| rows[*row].clone()).collect()
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
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(raw_rows())) as ArrayRef],
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
    assert_eq!(metadata.row_groups().len(), ROWS / ROW_GROUP_ROWS);
    let size = std::fs::metadata(&path).unwrap().len();
    (path.to_string_lossy().to_string(), size)
}

/// A Puffin sidecar holding `body` for `data_file`, registered the way the
/// planner registers one: blob type, `data_file` / `column` / `row_group_size`
/// properties. Returns the reference the scan task carries and the blob's byte
/// range in the file, which is what a corruption has to land inside.
async fn write_sidecar(
    dir: &Path,
    name: &str,
    data_file: &str,
    body: Vec<u8>,
    codec: CompressionCodec,
) -> (StatisticsBlobReference, std::ops::Range<u64>) {
    let path = dir.join(name).to_string_lossy().to_string();
    let file_io = FileIO::new_with_fs();
    let properties = HashMap::from([
        ("data_file".to_string(), data_file.to_string()),
        ("column".to_string(), "raw".to_string()),
        ("row_group_size".to_string(), ROW_GROUP_ROWS.to_string()),
    ]);
    let output = file_io.new_output(&path).unwrap();
    let mut writer = PuffinWriter::new(&output, HashMap::new(), false)
        .await
        .unwrap();
    writer
        .add(
            Blob::builder()
                .r#type(BLOB_TYPE.to_string())
                .fields(vec![1])
                .snapshot_id(1)
                .sequence_number(1)
                .data(body)
                .properties(properties.clone())
                .build(),
            codec,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();

    let reader = PuffinReader::new(file_io.new_input(&path).unwrap());
    let metadata = reader.file_metadata().await.unwrap();
    let blob = metadata.blobs().first().expect("one blob").clone();
    let range = blob.offset()..blob.offset() + blob.length();
    (
        StatisticsBlobReference {
            statistics_path: path,
            blob_type: BLOB_TYPE.to_string(),
            properties,
        },
        range,
    )
}

/// Flip one bit of a file in place, inside `range`.
fn flip_stored_bit(path: &str, range: &std::ops::Range<u64>, nth: u64) {
    let mut bytes = std::fs::read(path).unwrap();
    let at = (range.start + nth) as usize;
    assert!((at as u64) < range.end, "the flip has to land in the blob");
    bytes[at] ^= 0b0001_0000;
    std::fs::write(path, bytes).unwrap();
}

/// Scan `path` with the reader's `NEEDLE` prune hint and return the `raw`
/// values it decoded — or the error the scan failed with. A rejected index
/// leaves no row selection, so the whole file comes back and the exact
/// predicate runs above the scan.
async fn scan_raw_values(
    path: &str,
    size: u64,
    statistics_blobs: Vec<StatisticsBlobReference>,
    cache_bypass: bool,
) -> Result<Vec<String>, String> {
    let reader = ArrowReaderBuilder::new(FileIO::new_with_fs())
        .with_row_selection_enabled(true)
        .with_cache_bypass(cache_bypass)
        .with_raw_prune_spec(Some(RawPruneSpec {
            column: "raw".to_string(),
            all_terms: vec![NEEDLE.to_string()],
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
        statistics_blobs,
    };
    let batches = reader
        .read(Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream)
        .map_err(|err| err.to_string())?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|err| err.to_string())?;
    Ok(batches
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
        .collect())
}

/// How many files the index pruned on `storage`, and how many blobs the two
/// #4558 layers refused, since the last call.
fn pruning_delta(snapshotter: &Snapshotter, storage: &str) -> (u64, u64) {
    let snapshot = snapshotter.snapshot().into_vec();
    (
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", storage)),
        ),
        counter_sum(
            &snapshot,
            "siglake_index_row_domain_mismatch_total",
            Some(("storage", storage)),
        ),
    )
}

/// Two stored-byte corruptions of `sound`, each surviving both of #4558's
/// layers — it decodes, and its row domain is still the file's — and each
/// costing the query rows, differently:
///
/// - the term the query asks for is gone from the dictionary, so
///   `matching_rows_all` reads a definitive "no rows match" and the **whole
///   file** is skipped;
/// - the term is still there and its postings are not the file's rows, so the
///   scan decodes rows that do not match and skips rows that do. This is the
///   class #4558's sweep counted: ascending, in-range, and wrong.
///
/// Searched rather than hard-coded so the fixture cannot rot into a blob where
/// no such corruption exists; the rates are in `siglake-index`'s
/// `report_stored_byte_corruption_by_path`.
fn silently_wrong_blobs(sound: &InvertedIndex) -> (Vec<u8>, Vec<u8>) {
    let truth = sound.postings(NEEDLE).expect("the fixture has the needle");
    let blob = sound.to_bytes();
    let mut term_gone: Option<Vec<u8>> = None;
    let mut postings_wrong: Option<Vec<u8>> = None;
    for byte in 0..blob.len() {
        for bit in 0..8u32 {
            if term_gone.is_some() && postings_wrong.is_some() {
                break;
            }
            let mut corrupt = blob.clone();
            corrupt[byte] ^= 1 << bit;
            let Some(index) = InvertedIndex::from_bytes(&corrupt) else {
                continue;
            };
            if index.n_rows() != sound.n_rows() {
                continue;
            }
            match index.postings(NEEDLE) {
                None => term_gone = term_gone.or(Some(corrupt)),
                Some(got) if truth.iter().any(|row| !got.contains(row)) => {
                    postings_wrong = postings_wrong.or(Some(corrupt));
                }
                Some(_) => {}
            }
        }
    }
    (
        term_gone.expect("some flip removes the needle from the dictionary"),
        postings_wrong.expect("some flip leaves the needle's postings wrong"),
    )
}

#[tokio::test]
async fn a_corrupt_stored_blob_is_answered_differently_on_each_storage_path() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let rows = raw_rows();
    let needles = needle_texts(&rows);
    let sound = InvertedIndex::from_rows(rows.iter().map(String::as_str));
    assert_eq!(sound.n_rows(), ROWS as u32);
    let (term_gone, postings_wrong) = silently_wrong_blobs(&sound);
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };

    // --- Control: no index at all. The answer every arm is measured against.
    let (path, size) = write_data_file(tmp.path(), "control.parquet", None);
    let decoded = scan_raw_values(&path, size, vec![], true).await.unwrap();
    assert_eq!(decoded.len(), ROWS, "no index decodes the whole file");
    for needle in &needles {
        assert!(decoded.contains(needle));
    }
    assert_eq!(pruning_delta(&snapshotter, "footer_kv"), (0, 0));

    // --- Footer KV, sound blob: prunes to the matching rows, answer intact.
    let (path, size) = write_data_file(
        tmp.path(),
        "footer_sound.parquet",
        Some(&hex(&sound.to_bytes())),
    );
    let decoded = scan_raw_values(&path, size, vec![], true).await.unwrap();
    assert_eq!(decoded.len(), NEEDLE_ROWS.len(), "pruned to the matches");
    for needle in &needles {
        assert!(decoded.contains(needle));
    }
    assert_eq!(pruning_delta(&snapshotter, "footer_kv"), (1, 0));

    // --- Footer KV, corrupt stored bytes: the exposure, in both shapes. The
    // scan succeeds, the index prunes, neither #4558 layer objects, and the
    // answer is short — by the whole file when the term itself was flipped, by
    // one row when a posting delta was.
    // `decoded` is what the scan handed up, so an arm that prunes to four rows
    // and misses three matches decoded three rows that do not match.
    for (arm, blob, decodes) in [
        ("term_gone", &term_gone, 0),
        ("postings_wrong", &postings_wrong, NEEDLE_ROWS.len()),
    ] {
        let (path, size) = write_data_file(
            tmp.path(),
            &format!("footer_{arm}.parquet"),
            Some(&hex(blob)),
        );
        let decoded = scan_raw_values(&path, size, vec![], true).await.unwrap();
        let missing: Vec<&String> = needles
            .iter()
            .filter(|needle| !decoded.contains(needle))
            .collect();
        println!(
            "footer KV, {arm}: decoded {} of {ROWS} rows, lost {} of {} needles",
            decoded.len(),
            missing.len(),
            needles.len()
        );
        assert_eq!(
            decoded.len(),
            decodes,
            "arm {arm} decoded rows the corrupt index selected"
        );
        assert!(
            !missing.is_empty(),
            "arm {arm} has to cost the query matches"
        );
        assert_eq!(
            pruning_delta(&snapshotter, "footer_kv"),
            (1, 0),
            "arm {arm} pruned, and nothing refused it"
        );
    }

    // --- Puffin sidecar, as the writer registers it (Zstd, checksummed), sound
    // blob: same pruning through the sidecar path.
    let (path, size) = write_data_file(tmp.path(), "puffin_sound.parquet", None);
    let (reference, range) = write_sidecar(
        tmp.path(),
        "puffin_sound.puffin",
        &path,
        sound.to_bytes(),
        CompressionCodec::Zstd,
    )
    .await;
    let decoded = scan_raw_values(&path, size, vec![reference.clone()], true)
        .await
        .unwrap();
    assert_eq!(decoded.len(), NEEDLE_ROWS.len(), "pruned to the matches");
    assert_eq!(pruning_delta(&snapshotter, "puffin"), (1, 0));

    // --- Puffin sidecar, one flipped stored bit inside the blob: the Zstd
    // frame refuses it and the error propagates. The query fails; it does not
    // answer short and does not fall back to a scan.
    flip_stored_bit(
        &reference.statistics_path,
        &range,
        range.end - range.start - 1,
    );
    let failure = scan_raw_values(&path, size, vec![reference.clone()], true)
        .await
        .expect_err("a corrupt Zstd frame fails the query");
    println!("Puffin sidecar, corrupt Zstd frame: {failure}");
    assert_eq!(
        pruning_delta(&snapshotter, "puffin"),
        (0, 0),
        "nothing pruned and nothing was refused as an index: {failure}"
    );

    // --- Puffin sidecar with the compression the writer does not select: the
    // frame is what covers the path, so an uncompressed blob has the footer
    // KV's exposure. Same corrupt body, same silent short answer.
    let (path, size) = write_data_file(tmp.path(), "puffin_plain.parquet", None);
    let (reference, _) = write_sidecar(
        tmp.path(),
        "puffin_plain.puffin",
        &path,
        postings_wrong.clone(),
        CompressionCodec::None,
    )
    .await;
    let decoded = scan_raw_values(&path, size, vec![reference], true)
        .await
        .unwrap();
    let missing: Vec<&String> = needles
        .iter()
        .filter(|needle| !decoded.contains(needle))
        .collect();
    assert!(
        !missing.is_empty(),
        "an uncompressed corrupt sidecar costs the query rows, like the footer KV"
    );
    println!(
        "Puffin sidecar, uncompressed corrupt blob: decoded {} of {ROWS} rows, lost {} of {} needles",
        decoded.len(),
        missing.len(),
        needles.len()
    );
    assert_eq!(pruning_delta(&snapshotter, "puffin"), (1, 0));
}

/// Warm, on both paths: the caches hold the parsed index under an identity that
/// says "this file, written once", so nothing re-reads or re-verifies the
/// stored bytes. A file that rots after a query warmed it keeps answering from
/// the parse until the entry is evicted — and the cold read of the same file
/// then behaves as the arms above.
///
/// Separate test function, but the same process-global recorder is not read
/// here: the answer is the assertion.
#[tokio::test]
async fn a_warm_parsed_index_does_not_see_a_corruption_that_lands_after_it() {
    let tmp = tempfile::tempdir().unwrap();
    let rows = raw_rows();
    let needles = needle_texts(&rows);
    let sound = InvertedIndex::from_rows(rows.iter().map(String::as_str));

    let (path, size) = write_data_file(tmp.path(), "warm.parquet", None);
    let (reference, range) = write_sidecar(
        tmp.path(),
        "warm.puffin",
        &path,
        sound.to_bytes(),
        CompressionCodec::Zstd,
    )
    .await;

    // Warm the parsed-index cache (and the blob cache) with the sound sidecar.
    let decoded = scan_raw_values(&path, size, vec![reference.clone()], false)
        .await
        .unwrap();
    assert_eq!(decoded.len(), NEEDLE_ROWS.len());

    // Corrupt the stored bytes underneath it.
    flip_stored_bit(&reference.statistics_path, &range, 0);

    // Warm: answered from the parse, no fetch, no error, right answer.
    let decoded = scan_raw_values(&path, size, vec![reference.clone()], false)
        .await
        .expect("the warm path never reads the corrupt bytes");
    assert_eq!(decoded.len(), NEEDLE_ROWS.len());
    for needle in &needles {
        assert!(decoded.contains(needle));
    }

    // Cold — the same state after an eviction or a restart: the frame refuses.
    scan_raw_values(&path, size, vec![reference], true)
        .await
        .expect_err("a cold read of the same file fails on the Zstd frame");
}
