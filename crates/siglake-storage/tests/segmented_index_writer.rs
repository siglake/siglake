//! A streaming re-cluster builds its output's segmented (`seg2`) sidecar while
//! it merges, and publishes it with the rewrite commit (#4377).
//!
//! What these hold, in the order the card asks for them:
//!
//! - one sidecar per output file, whose groups are the file's Parquet row
//!   groups and whose postings agree term for term with a whole-file v1 index
//!   built over the same rows;
//! - a rewrite whose output rolls into several data files publishes one sidecar
//!   per file, addressed to the right one;
//! - a rewrite whose commit fails leaves nothing discoverable — the Puffin
//!   object is written before the commit, and discovery is by registration;
//! - an append before the first commit attempt and one that forces a CAS retry
//!   leave the rewrite snapshot, table metadata and Puffin footer in agreement;
//! - the post-commit full-file decode does not run for a column the rewrite
//!   already indexed, and a second rebuild call is a no-op;
//! - the index state the rewrite holds is one row group's, not the file's.
//!
//! Registration against a snapshot that already has a statistics file is here
//! too, because that is where the rewrite's blobs are lost: #5228's sequential
//! case, and #5298's two concurrent registrants — one racing the transaction's
//! base refresh, one racing its CAS window.
//!
//! Its own test binary: it installs a process-global metrics recorder and a
//! global allocator, both process state, so the tests take [`WRITER_TESTS`] and
//! run one at a time.

use std::alloc::{GlobalAlloc, Layout, System};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use arrow_array::builder::BooleanBuilder;
use arrow_array::Array;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use iceberg::spec::{BlobMetadata as StatisticsBlobMetadata, DataFile, StatisticsFile};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::index_config::{DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode};
use siglake_core::Event;
use siglake_index::segmented::{SegmentedReader, SliceSource, SEGMENTED_V2_BLOB_TYPE};
use siglake_index::InvertedIndex;
use siglake_storage::iceberg::test_catalog::TestCatalog;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning, ReclusterMergeOptions};

/// Live heap bytes, counted from process start, and their peak over a
/// measurement window.
///
/// `LIVE` is maintained unconditionally so that a window has a real starting
/// live-heap figure to measure against. The earlier version of this tracker
/// (and the copy in `delete_task_size_gate.rs`) counted only while tracking was
/// on, from a reset-to-zero counter: allocations already live when the window
/// opened were invisible while their frees still decremented it, so its "peak"
/// was neither the process's live heap nor an exact delta above it. Every test
/// here holds [`WRITER_TESTS`] while it measures, so a window belongs to one arm
/// of one A/B.
struct PeakTracking;

static TRACKING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for PeakTracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size() as isize;
            let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
            if TRACKING.load(Ordering::Relaxed) {
                PEAK.fetch_max(live, Ordering::Relaxed);
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Signed, and never clamped: every allocation this process makes passes
        // through `alloc` above first, so the counter is symmetric. A clamp
        // would silently absorb the one thing that would prove it is not.
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTracking = PeakTracking;

/// An open live-heap measurement window: what was live when it opened.
#[derive(Clone, Copy, Debug)]
struct HeapWindow {
    baseline: isize,
}

/// What one window saw.
#[derive(Clone, Copy, Debug)]
struct HeapReading {
    /// Live heap when the window opened — the state the measured work started
    /// from, not zero.
    baseline: u64,
    /// The largest live heap seen inside the window.
    peak: u64,
    /// `peak - baseline`: what the measured work added over what was already
    /// live. Zero when the window only freed.
    above_baseline: u64,
}

/// Open a window over the live heap as it is now.
fn start_tracking() -> HeapWindow {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    TRACKING.store(true, Ordering::Relaxed);
    HeapWindow { baseline }
}

impl HeapWindow {
    fn finish(self) -> HeapReading {
        TRACKING.store(false, Ordering::Relaxed);
        let peak = PEAK.load(Ordering::Relaxed);
        HeapReading {
            baseline: self.baseline.max(0) as u64,
            peak: peak.max(0) as u64,
            above_baseline: (peak - self.baseline).max(0) as u64,
        }
    }
}

static WRITER_TESTS: Mutex<()> = Mutex::new(());

fn setup() -> (MutexGuard<'static, ()>, &'static Snapshotter) {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    let guard = WRITER_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let snapshotter = SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        snapshotter
    });
    // Every read of a debugging recorder drains it, so this leaves each test
    // with an empty recorder to fill.
    let _ = snapshotter.snapshot();
    (guard, snapshotter)
}

/// Runs one test with this binary's process state — the metrics recorder and
/// the allocator counters — to itself.
///
/// A `#[test]` with its own runtime rather than `#[tokio::test]`: the lock has
/// to span the whole rewrite, and a std `MutexGuard` held across an `.await`
/// inside an async test is what `clippy::await_holding_lock` exists to catch.
fn serialized<F>(body: impl FnOnce(&'static Snapshotter) -> F)
where
    F: std::future::Future<Output = ()>,
{
    let (_guard, snapshotter) = setup();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build multi-thread runtime")
        .block_on(body(snapshotter));
}

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

// ---------------------------------------------------------------- the corpus

/// One day per `rows_per_file`, a 2%-density keyword, a rare term every
/// `rare_every` rows and one token unique to the row — the shape every
/// measurement in `docs/DESIGN_segmented_inverted_index.md` was taken on, so a
/// sidecar built here has the dictionary the design describes (almost every
/// term unique to one row).
fn shaped_event(row: usize, rows_per_file: usize, base: DateTime<Utc>, rare_every: usize) -> Event {
    const DAY_US: i64 = 86_400_000_000;

    let queen = if row.is_multiple_of(50) { " queen" } else { "" };
    let rare = if rare_every > 0 && row.is_multiple_of(rare_every) {
        " rareneedle"
    } else {
        ""
    };
    let mut event = Event::now(format!(
        "service-{} status {}{queen}{rare} row-{row:08}",
        row % 20,
        200 + row % 5
    ));
    let file = (row / rows_per_file) as i64;
    let within = (row % rows_per_file) as i64;
    event.timestamp = base
        + chrono::Duration::days(file)
        + chrono::Duration::microseconds(within * (DAY_US / rows_per_file.max(1) as i64));
    event
}

/// Midnight UTC, so a file's rows fill exactly one `day_ts` partition: the
/// streaming merge refuses a bin spanning two, and a corpus starting mid-day
/// would split every file across the boundary.
fn fixture_base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2023, 11, 14, 0, 0, 0).unwrap()
}

/// A warehouse with segmented writes either on or off, and nothing else
/// different between the two. `row_group_bytes` steers the output's row-group
/// count (the writer clamps at `MIN_ROW_GROUP_ROWS`, so a small value means
/// 128 Ki-row groups).
async fn open_fixture(
    path: &std::path::Path,
    segmented: bool,
    row_group_bytes: Option<usize>,
    target_file_bytes: Option<usize>,
) -> IcebergContext {
    open_fixture_with_tuning(
        path,
        IcebergTuning {
            segmented_index_writes: Some(segmented),
            // The hermetic cases keep the post-commit v1 rebuild ON, so "the
            // rewrite already indexed this column" is what stops it rather
            // than the knob.
            index_rebuild: Some(true),
            target_row_group_bytes: row_group_bytes,
            merge_target_file_bytes: target_file_bytes,
            ..Default::default()
        },
    )
    .await
}

/// A fixture whose storage knobs are explicit. The build-cost report uses
/// this to keep append indexing and Parquet layout matched while varying only
/// the two rewrite-index settings under measurement.
async fn open_fixture_with_tuning(path: &std::path::Path, tuning: IcebergTuning) -> IcebergContext {
    IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(tuning)
}

const APPEND_CHUNK: usize = 250_000;

/// Append `rows` of the corpus. Returns the data files the append added, which
/// is the bin [`rewrite_fresh`] then merges.
async fn append_rows(
    ice: &IcebergContext,
    rows: std::ops::Range<usize>,
    rows_per_file: usize,
    rare_every: usize,
) -> Vec<DataFile> {
    let ident = ice.events_table_ident().clone();
    let before: Vec<String> = ice
        .live_data_files(&ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    let base = fixture_base();
    for chunk in rows.clone().step_by(APPEND_CHUNK) {
        let events: Vec<Event> = (chunk..(chunk + APPEND_CHUNK).min(rows.end))
            .map(|row| shaped_event(row, rows_per_file, base, rare_every))
            .collect();
        ice.append_events(&events).await.unwrap();
    }
    ice.live_data_files(&ident)
        .await
        .unwrap()
        .into_iter()
        .filter(|file| !before.contains(&file.file_path().to_string()))
        .collect()
}

/// Stream-rewrite exactly `fresh`. Returns the table's live data files after it.
async fn rewrite_fresh(ice: &IcebergContext, fresh: Vec<DataFile>) -> Vec<DataFile> {
    let ident = ice.events_table_ident().clone();
    let blooms = ice.events_bloom_columns();
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    rewrite_fresh_in(ice, &ident, fresh, &bloom_refs).await
}

/// [`rewrite_fresh`] against any table in the context — the managed-index
/// tables the multi-column cases build.
async fn rewrite_fresh_in(
    ice: &IcebergContext,
    ident: &iceberg::TableIdent,
    fresh: Vec<DataFile>,
    bloom_columns: &[&str],
) -> Vec<DataFile> {
    ice.recluster_files_with(
        ident,
        fresh,
        bloom_columns,
        &ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    ice.live_data_files(ident).await.unwrap()
}

/// Append `rows` of the corpus, then stream-rewrite exactly that day's output.
async fn append_and_rewrite(
    ice: &IcebergContext,
    rows: std::ops::Range<usize>,
    rows_per_file: usize,
    rare_every: usize,
) -> Vec<DataFile> {
    let fresh = append_rows(ice, rows, rows_per_file, rare_every).await;
    rewrite_fresh(ice, fresh).await
}

// ------------------------------------------------------- reading it back out

/// Every registered seg2 blob, as `(statistics path, data file, column)`.
fn registered_seg2_blobs(table: &Table) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for statistics in table.metadata().statistics_iter() {
        for blob in &statistics.blob_metadata {
            if blob.r#type != SEGMENTED_V2_BLOB_TYPE {
                continue;
            }
            out.push((
                statistics.statistics_path.clone(),
                blob.properties
                    .get("data_file")
                    .cloned()
                    .unwrap_or_default(),
                blob.properties.get("column").cloned().unwrap_or_default(),
            ));
        }
    }
    out.sort();
    out
}

/// Registered blob types, counted only for blobs naming one of `data_files`.
///
/// Table-wide would also count the append-time v1 sidecars of the files the
/// rewrite retired: a rewrite removes data files but statistics entries stay in
/// metadata until snapshot expiry, so the question has to be asked of the live
/// output.
fn registered_blob_types_for(table: &Table, data_files: &[DataFile]) -> BTreeMap<String, usize> {
    let live: Vec<&str> = data_files.iter().map(|file| file.file_path()).collect();
    let mut out: BTreeMap<String, usize> = BTreeMap::new();
    for statistics in table.metadata().statistics_iter() {
        for blob in &statistics.blob_metadata {
            let names_live = blob
                .properties
                .get("data_file")
                .is_some_and(|path| live.contains(&path.as_str()));
            if names_live {
                *out.entry(blob.r#type.clone()).or_default() += 1;
            }
        }
    }
    out
}

/// The bytes of one registered seg2 blob, read out of its Puffin container.
async fn seg2_blob_bytes(
    table: &Table,
    statistics_path: &str,
    data_file: &str,
    column: &str,
) -> Vec<u8> {
    let input = table.file_io().new_input(statistics_path).unwrap();
    let reader = iceberg::puffin::PuffinReader::new(input);
    let metadata = reader.file_metadata().await.unwrap();
    let blob = metadata
        .blobs()
        .iter()
        .find(|blob| {
            blob.blob_type() == SEGMENTED_V2_BLOB_TYPE
                && blob
                    .properties()
                    .get("data_file")
                    .is_some_and(|p| p == data_file)
                && blob.properties().get("column").is_some_and(|c| c == column)
        })
        .expect("registered blob present in its Puffin file")
        .clone();
    assert_eq!(
        blob.compression_codec(),
        iceberg::puffin::CompressionCodec::None,
        "a segmented blob has to be registered uncompressed or its interior is unreachable"
    );
    reader.blob(&blob).await.unwrap().data().to_vec()
}

fn local_path(data_file: &str) -> String {
    data_file.trim_start_matches("file://").to_string()
}

/// The file's row groups and its `raw` column, read straight off the local
/// object — the ground truth the sidecar is checked against.
fn parquet_rows_and_row_groups(data_file: &str) -> (Vec<u32>, Vec<String>) {
    parquet_rows_and_row_groups_of(data_file, "raw")
}

/// [`parquet_rows_and_row_groups`] for any text column, which the multi-column
/// rolled cases need.
fn parquet_rows_and_row_groups_of(data_file: &str, column: &str) -> (Vec<u32>, Vec<String>) {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(local_path(data_file)).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let row_groups: Vec<u32> = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|group| group.num_rows() as u32)
        .collect();
    let mut rows = Vec::new();
    for batch in builder.with_batch_size(8192).build().unwrap() {
        let batch = batch.unwrap();
        let idx = batch.schema().index_of(column).unwrap();
        let column = batch
            .column(idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..column.len() {
            rows.push(if column.is_null(i) {
                String::new()
            } else {
                column.value(i).to_string()
            });
        }
    }
    (row_groups, rows)
}

/// Assert one file's sidecar: its groups are the file's row groups, and its
/// answer for every term in the file is the whole-file v1 index's.
async fn assert_sidecar_describes_file(
    table: &Table,
    statistics_path: &str,
    data_file: &str,
    column: &str,
) {
    let bytes = seg2_blob_bytes(table, statistics_path, data_file, column).await;
    let (row_groups, rows) = parquet_rows_and_row_groups_of(data_file, column);
    let reader = SegmentedReader::open(SliceSource::new(bytes)).expect("sidecar opens");
    let counts: Vec<u64> = row_groups.iter().map(|rows| u64::from(*rows)).collect();
    assert!(
        reader.matches_row_groups(&counts),
        "sidecar groups {:?} must be the file's row groups {row_groups:?}",
        reader.group_rows(),
    );
    assert_eq!(
        reader.n_rows() as usize,
        rows.len(),
        "sidecar must cover every row of {data_file}"
    );

    let whole_file = InvertedIndex::from_rows(rows.iter().map(String::as_str));
    assert!(
        whole_file.n_terms() > 0,
        "the corpus has to produce terms or this proves nothing"
    );
    for (term, expected) in whole_file.terms() {
        let got = reader
            .postings(term)
            .rows()
            .unwrap_or_else(|| panic!("sidecar could not answer {term}"));
        assert_eq!(got, expected, "postings for {term} in {data_file}");
    }
    // And a term the corpus does not carry is a definitive no, not an error.
    assert_eq!(
        reader.postings("nosuchtokenanywhere").rows(),
        Some(Vec::new()),
        "an absent term is absent, not unanswerable"
    );
}

/// Assert the three copies of a seg2 blob's snapshot identity: the snapshot,
/// the table's `StatisticsFile` entry and the physical Puffin footer.
async fn assert_current_seg2_sequence_agreement(table: &Table) {
    let snapshot = table
        .metadata()
        .current_snapshot()
        .expect("the rewrite committed a snapshot");
    let statistics = table
        .metadata()
        .statistics_iter()
        .find(|statistics| statistics.snapshot_id == snapshot.snapshot_id())
        .expect("the rewrite snapshot registered statistics");
    let table_blobs: Vec<_> = statistics
        .blob_metadata
        .iter()
        .filter(|blob| blob.r#type == SEGMENTED_V2_BLOB_TYPE)
        .collect();
    assert!(!table_blobs.is_empty(), "the statistics entry carries seg2");
    for blob in table_blobs {
        assert_eq!(blob.snapshot_id, snapshot.snapshot_id());
        assert_eq!(blob.sequence_number, snapshot.sequence_number());
    }

    let input = table
        .file_io()
        .new_input(&statistics.statistics_path)
        .unwrap();
    let reader = iceberg::puffin::PuffinReader::new(input);
    let physical = reader.file_metadata().await.unwrap();
    let physical_blobs: Vec<_> = physical
        .blobs()
        .iter()
        .filter(|blob| blob.blob_type() == SEGMENTED_V2_BLOB_TYPE)
        .collect();
    assert_eq!(physical_blobs.len(), statistics.blob_metadata.len());
    for blob in physical_blobs {
        assert_eq!(blob.snapshot_id(), snapshot.snapshot_id());
        assert_eq!(blob.sequence_number(), snapshot.sequence_number());
    }
}

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

/// The query server's `match_terms` UDF cannot be a storage dependency. This
/// test stub gives DataFusion the same function name and literal shape so the
/// storage reader can extract and push the predicate into its index path.
#[derive(Debug, Eq, Hash, PartialEq)]
struct MatchTermsUdf {
    signature: Signature,
}

impl MatchTermsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    datafusion::arrow::datatypes::DataType::Utf8,
                    datafusion::arrow::datatypes::DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MatchTermsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "match_terms"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> datafusion::error::Result<datafusion::arrow::datatypes::DataType> {
        Ok(datafusion::arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let rows = args.number_rows;
        let cells = match &args.args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(rows)?,
        };
        let cells = cells
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("lhs must be Utf8".into())
            })?
            .clone();
        let query = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(query)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(query)))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(query))) => query.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "query must be a Utf8 literal".into(),
                ));
            }
        };
        let tokens: Vec<String> = query
            .split_whitespace()
            .map(|token| token.to_ascii_lowercase())
            .collect();
        let mut builder = BooleanBuilder::with_capacity(rows);
        for row in 0..rows {
            if cells.is_null(row) {
                builder.append_value(false);
                continue;
            }
            let haystack = cells.value(row).to_ascii_lowercase();
            builder.append_value(tokens.iter().all(|token| haystack.contains(token)));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

async fn text_counts(ice: &IcebergContext) -> (i64, i64, i64) {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    ice.register_with_datafusion(&ctx).await.unwrap();
    (
        count(&ctx, "SELECT count(*) FROM events").await,
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE match_terms(raw, 'rareneedle')",
        )
        .await,
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE match_terms(raw, 'queen')",
        )
        .await,
    )
}

/// Rows enough for several row groups at the writer's 128 Ki floor.
const ROWS: usize = 300_000;
const RARE_EVERY: usize = 10_000;

// ------------------------------------------------------------------- the tests

/// The single-output case: one rewrite, one output file, one sidecar, and the
/// sidecar answers what a whole-file index over the same rows answers.
///
/// The control arm is the same corpus with the knob off, and its Parquet output
/// has to be identical — the writer builds an index beside the file, it does not
/// change the file.
#[test]
fn a_streaming_rewrite_publishes_a_sidecar_that_describes_its_output() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();

        let off = open_fixture(&tmp.path().join("off"), false, Some(1), None).await;
        let off_files = append_and_rewrite(&off, 0..ROWS, ROWS, RARE_EVERY).await;
        let on = open_fixture(&tmp.path().join("on"), true, Some(1), None).await;
        let on_files = append_and_rewrite(&on, 0..ROWS, ROWS, RARE_EVERY).await;

        assert_eq!(on_files.len(), 1, "one rewrite of one day's rows, one file");
        assert_eq!(
            off_files.iter().map(|f| f.record_count()).sum::<u64>(),
            on_files.iter().map(|f| f.record_count()).sum::<u64>(),
        );

        let table = on
            .catalog()
            .load_table(on.events_table_ident())
            .await
            .unwrap();
        let blobs = registered_seg2_blobs(&table);
        assert_eq!(
            blobs.len(),
            1,
            "one output file and one indexed column: {blobs:?}"
        );
        let (statistics_path, data_file, column) = blobs[0].clone();
        assert_eq!(column, "raw");
        assert_eq!(data_file, on_files[0].file_path());
        let (row_groups, _) = parquet_rows_and_row_groups(&data_file);
        assert!(
            row_groups.len() > 1,
            "the fixture has to have several row groups or the per-group build is untested: \
         {row_groups:?}"
        );
        assert_sidecar_describes_file(&table, &statistics_path, &data_file, &column).await;

        // The off arm registered no segmented blob, and the on arm registered no
        // v1 one — the rewrite's own sidecar is what the rebuild would have built.
        let off_table = off
            .catalog()
            .load_table(off.events_table_ident())
            .await
            .unwrap();
        assert!(
            registered_seg2_blobs(&off_table).is_empty(),
            "the knob is off in the control arm"
        );

        assert_eq!(
            text_counts(&on).await,
            text_counts(&off).await,
            "the rewrite's rows must not depend on whether it built an index beside them"
        );
    });
}

/// A foreign append lands after the rewrite read its base but before the
/// transaction's first commit attempt reloads it. The first attempt therefore
/// builds directly on the newer base, without needing a failed CAS to expose
/// the stale pre-write sequence-number capture.
#[test]
fn an_append_before_the_first_commit_attempt_refreshes_seg2_sequence_metadata() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("stale-first-base");
        let mut rewrite = open_fixture(&warehouse, true, Some(1), None).await;
        let fresh = append_rows(&rewrite, 0..150_000, 150_000, RARE_EVERY).await;
        let before = rewrite
            .catalog()
            .load_table(rewrite.events_table_ident())
            .await
            .unwrap()
            .metadata()
            .current_snapshot()
            .unwrap()
            .sequence_number();

        let appender = IcebergContext::open(&warehouse).await.unwrap();
        let appended = Arc::new(AtomicBool::new(false));
        let gated = TestCatalog::new(rewrite.catalog().clone())
            .after_load_table({
                let appended = appended.clone();
                move || {
                    let appender = appender.clone();
                    let appended = appended.clone();
                    async move {
                        if !appended.swap(true, Ordering::SeqCst) {
                            appender
                                .append_events(&[Event::now("intervening-before-attempt")])
                                .await
                                .unwrap();
                        }
                    }
                }
            })
            .shared();
        rewrite = rewrite.with_catalog_for_test(gated);

        let live = rewrite_fresh(&rewrite, fresh).await;
        assert!(appended.load(Ordering::SeqCst), "the append hook fired");
        assert_eq!(
            live.iter().map(DataFile::record_count).sum::<u64>(),
            150_001,
            "the append and rewrite publish atomically without losing either file"
        );
        let table = rewrite
            .catalog()
            .load_table(rewrite.events_table_ident())
            .await
            .unwrap();
        assert_eq!(
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .sequence_number(),
            before + 2
        );
        assert_current_seg2_sequence_agreement(&table).await;
    });
}

/// A foreign append lands in the first attempt's CAS window. That attempt's
/// Puffin file stays unreferenced; the retry reuses the written Parquet output
/// and seg2 bytes, and stamps a new Puffin footer from the refreshed base.
#[test]
fn an_append_during_a_forced_cas_retry_refreshes_seg2_sequence_metadata() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("cas-retry");
        let mut rewrite = open_fixture(&warehouse, true, Some(1), None).await;
        let fresh = append_rows(&rewrite, 0..150_000, 150_000, RARE_EVERY).await;

        let appender = IcebergContext::open(&warehouse).await.unwrap();
        let gated = TestCatalog::new(rewrite.catalog().clone())
            .before_first_update_with_base(move || {
                let appender = appender.clone();
                async move {
                    appender
                        .append_events(&[Event::now("intervening-in-cas-window")])
                        .await
                        .unwrap();
                }
            })
            .shared();
        rewrite = rewrite.with_catalog_for_test(gated.clone());

        let live = rewrite_fresh(&rewrite, fresh).await;
        assert!(gated.fired(), "the forced-CAS hook fired");
        assert_eq!(
            live.iter().map(DataFile::record_count).sum::<u64>(),
            150_001,
            "the successful retry retains both the append and rewritten rows"
        );
        let table = rewrite
            .catalog()
            .load_table(rewrite.events_table_ident())
            .await
            .unwrap();
        assert_current_seg2_sequence_agreement(&table).await;
        assert_eq!(
            registered_seg2_blobs(&table).len(),
            1,
            "the failed attempt's Puffin object is not discoverable"
        );
        let indexed_file = &registered_seg2_blobs(&table)[0].1;
        assert!(
            live.iter().any(|file| file.file_path() == indexed_file),
            "the retry registers the Parquet output already written by the rewrite"
        );
    });
}

/// A rewrite whose output rolls into several data files publishes one sidecar
/// per file, each addressed to its own file and covering exactly its rows.
///
/// This is the split case the acceptance asks for. The streaming arm refuses a
/// bin spanning two partitions before it writes a byte, so a *partition* split
/// is always two rewrites; what one rewrite can produce is several files, and
/// that is what the rolling writer does here at a lowered target.
#[test]
fn a_rewrite_that_rolls_its_output_publishes_one_sidecar_per_file() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();

        // Small enough that the rolling writer opens a second and third file, and
        // still several row groups per file.
        let ice = open_fixture(&tmp.path().join("rolled"), true, Some(1), Some(1 << 20)).await;
        let files = append_and_rewrite(&ice, 0..ROWS, ROWS, RARE_EVERY).await;
        assert!(
            files.len() > 1,
            "the lowered file target has to roll the output or this proves nothing: {}",
            files.len()
        );

        let table = ice
            .catalog()
            .load_table(ice.events_table_ident())
            .await
            .unwrap();
        let blobs = registered_seg2_blobs(&table);
        assert_eq!(
            blobs.len(),
            files.len(),
            "one sidecar per output file: {blobs:?}"
        );
        let mut indexed: Vec<String> = blobs.iter().map(|(_, file, _)| file.clone()).collect();
        indexed.sort();
        let mut written: Vec<String> = files.iter().map(|f| f.file_path().to_string()).collect();
        written.sort();
        assert_eq!(indexed, written, "every output file, and only those");

        for (statistics_path, data_file, column) in blobs {
            assert_sidecar_describes_file(&table, &statistics_path, &data_file, &column).await;
        }
    });
}

/// Two day partitions, one rewrite each — what `recluster_pass` does per
/// partition, since the streaming merge refuses a mixed bin. Each rewrite's
/// sidecar is registered against its own output and neither is confused for the
/// other's.
#[test]
fn a_second_partitions_rewrite_registers_its_own_sidecar() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const PER_DAY: usize = 150_000;

        let ice = open_fixture(&tmp.path().join("days"), true, Some(1), None).await;
        append_and_rewrite(&ice, 0..PER_DAY, PER_DAY, RARE_EVERY).await;
        let files = append_and_rewrite(&ice, PER_DAY..2 * PER_DAY, PER_DAY, RARE_EVERY).await;
        assert_eq!(files.len(), 2, "one output file per day partition");

        let table = ice
            .catalog()
            .load_table(ice.events_table_ident())
            .await
            .unwrap();
        let blobs = registered_seg2_blobs(&table);
        assert_eq!(blobs.len(), 2, "one sidecar per partition's output");
        // Two transactions, so two statistics files.
        let statistics: std::collections::BTreeSet<String> =
            blobs.iter().map(|(path, _, _)| path.clone()).collect();
        assert_eq!(statistics.len(), 2, "each rewrite published its own");
        for (statistics_path, data_file, column) in blobs {
            assert_sidecar_describes_file(&table, &statistics_path, &data_file, &column).await;
        }
        assert_eq!(
            text_counts(&ice).await.0,
            (2 * PER_DAY) as i64,
            "both days' rows survive"
        );
    });
}

/// A rewrite whose commit fails leaves no discoverable index.
///
/// The Puffin object is written before the commit — the transaction has to
/// reference it — so the property rests on discovery being by registration:
/// `statistics_iter` never names the object, so no scan can find it, and orphan
/// GC reclaims it. Reached here by handing the rewrite a stale file list, which
/// `RewriteFilesAction` refuses because the files it would delete are no longer
/// live.
#[test]
fn a_failed_rewrite_commit_leaves_no_discoverable_index() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const ROWS_HERE: usize = 150_000;

        let ice = open_fixture(&tmp.path().join("failed"), true, Some(1), None).await;
        let ident = ice.events_table_ident().clone();
        let base = fixture_base();
        let events: Vec<Event> = (0..ROWS_HERE)
            .map(|row| shaped_event(row, ROWS_HERE, base, RARE_EVERY))
            .collect();
        ice.append_events(&events).await.unwrap();
        let stale = ice.live_data_files(&ident).await.unwrap();
        let blooms = ice.events_bloom_columns();
        let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
        let merge = ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        };
        // First rewrite succeeds and retires `stale`.
        ice.recluster_files_with(&ident, stale.clone(), &bloom_refs, &merge)
            .await
            .unwrap();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let after_first = registered_seg2_blobs(&table);
        assert_eq!(after_first.len(), 1);
        let before = text_counts(&ice).await;

        // Second rewrite of the same, now-retired, files: the merge runs, the
        // sidecar is written, the commit is refused.
        let failed = ice
            .recluster_files_with(&ident, stale, &bloom_refs, &merge)
            .await;
        assert!(
            failed.is_err(),
            "a rewrite deleting files that are no longer live must not commit"
        );

        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            registered_seg2_blobs(&table),
            after_first,
            "the refused rewrite must not have registered anything"
        );
        for (statistics_path, data_file, column) in &after_first {
            assert_sidecar_describes_file(&table, statistics_path, data_file, column).await;
        }
        assert_eq!(
            text_counts(&ice).await,
            before,
            "the refused rewrite must not have changed the table's rows"
        );
    });
}

/// A v1 registration against the snapshot a seg2 rewrite committed must leave
/// that rewrite's sidecars discoverable. It does not: this test fails, and
/// #5228 carries the fix.
///
/// `set_statistics` inserts by snapshot id
/// (`third_party/iceberg/src/spec/table_metadata_builder.rs:589`), so the
/// second `StatisticsFile` written against a snapshot REPLACES the first
/// rather than merging into it. The rewrite registers every output blob in one
/// entry under its reserved id S; anything that registers again under S drops
/// them all, their Puffin path leaves `reachable_files` and orphan GC deletes
/// the object.
///
/// In production the second registration can follow a mixed rewrite: with both
/// `SIGLAKE_SEGMENTED_INDEX_WRITES=1` and `SIGLAKE_INDEX_REBUILD=1`, one output
/// file whose sidecar `publish_segmented_sidecars` refuses
/// (`refused{column|file_rows|row_domain}`) is uncovered, so the post-commit
/// `rebuild_inverted_indexes_for_files` over the rewrite's output builds a v1
/// blob for it and registers it under S — silently losing the *other* output
/// files' indexes. This drives the registration boundary with the same mixed
/// coverage: appends leave one day unindexed, the rewrite publishes seg2 for
/// the other, and the v1 rebuild tries to register the uncovered day against S.
#[test]
fn a_v1_rebuild_against_the_rewrites_snapshot_keeps_its_seg2_blobs() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const ROWS_HERE: usize = 300_000;

        let ice = IcebergContext::open(&tmp.path().join("mixed"))
            .await
            .unwrap()
            .with_inverted_index(true)
            .with_table_cache_ttl(std::time::Duration::ZERO)
            .with_tuning(IcebergTuning {
                segmented_index_writes: Some(true),
                index_rebuild: Some(true),
                // No append-time index, so the day the rewrite skips stays a
                // live file the rebuild has real work to do for.
                index_at_flush: Some(false),
                target_row_group_bytes: Some(1),
                merge_target_file_bytes: Some(1 << 20),
                ..Default::default()
            });
        let ident = ice.events_table_ident().clone();

        let unindexed = append_rows(&ice, 0..ROWS_HERE, ROWS_HERE, RARE_EVERY).await;
        let fresh = append_rows(&ice, ROWS_HERE..2 * ROWS_HERE, ROWS_HERE, RARE_EVERY).await;
        rewrite_fresh(&ice, fresh).await;

        let table = ice.catalog().load_table(&ident).await.unwrap();
        let registered = registered_seg2_blobs(&table);
        assert!(
            registered.len() > 1,
            "the rolled rewrite must register several sidecars: {registered:?}"
        );

        // The second registration against the same snapshot.
        assert_eq!(
            ice.rebuild_inverted_indexes_for_files(&ident, &unindexed)
                .await
                .unwrap(),
            0,
            "the uncovered day is deferred rather than replacing the snapshot's statistics"
        );

        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            registered_seg2_blobs(&table),
            registered,
            "a v1 registration under the snapshot the rewrite committed must not drop that \
             rewrite's seg2 blobs"
        );
        for (statistics_path, data_file, column) in &registered {
            assert_sidecar_describes_file(&table, statistics_path, data_file, column).await;
        }
        let reachable = ice.reachable_files(&ident).await.unwrap();
        for (statistics_path, _, _) in &registered {
            assert!(
                reachable.contains(statistics_path),
                "the retained seg2 statistics file must remain protected from orphan GC: \
                 {statistics_path}"
            );
        }
        assert_eq!(
            text_counts(&ice).await,
            (2 * ROWS_HERE as i64, 60, 12_000),
            "the retained seg2 reader must answer exact match_terms counts across both days"
        );

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(
            counter_sum(
                &snapshot,
                "siglake_index_registration_deferred_total",
                Some(("reason", "snapshot_has_statistics"))
            ),
            1,
            "one same-snapshot registration is deferred"
        );
        assert_eq!(
            counter_sum(&snapshot, "siglake_index_rebuild_files_total", None),
            0,
            "deferred files are not reported as rebuilt"
        );
        assert_eq!(
            counter_sum(&snapshot, "siglake_index_rebuild_bytes_total", None),
            0,
            "deferred bytes are not reported as rebuilt"
        );
    });
}

// ------------------------------- two registrants, one snapshot (#5298)

/// Rows per day for the two-registrant cases: enough for a real index, small
/// enough to run two full-file decodes per test.
const REBUILD_ROWS: usize = 150_000;

/// A warehouse whose appends register no index at all — no Puffin sidecar and
/// no footer index — so each live file is one a rebuild has real work for and
/// two registrants can be aimed at disjoint halves of the same snapshot.
async fn open_two_registrant_fixture(path: &std::path::Path) -> IcebergContext {
    open_fixture_with_tuning(
        path,
        IcebergTuning {
            segmented_index_writes: Some(false),
            index_rebuild: Some(true),
            index_at_flush: Some(false),
            target_row_group_bytes: Some(1),
            ..Default::default()
        },
    )
    .await
}

/// Every registered v1 text blob, as `(statistics path, data file, column)`.
fn registered_v1_blobs(table: &Table) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for statistics in table.metadata().statistics_iter() {
        for blob in &statistics.blob_metadata {
            if blob.r#type != "siglake-inverted-v1" {
                continue;
            }
            out.push((
                statistics.statistics_path.clone(),
                blob.properties
                    .get("data_file")
                    .cloned()
                    .unwrap_or_default(),
                blob.properties.get("column").cloned().unwrap_or_default(),
            ));
        }
    }
    out.sort();
    out
}

/// Every `.puffin` object under the warehouse, registered or not.
fn puffin_objects(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "puffin") {
                out.push(format!("file://{}", path.display()));
            }
        }
    }
    out.sort();
    out
}

/// The state both #5298 regressions end in: the registration that got there
/// first is the only one in metadata, it still describes the file it was built
/// for, its Puffin object is the only reachable one, and the deferred caller's
/// object is an orphan.
async fn assert_first_registration_survived(
    warehouse: &std::path::Path,
    ident: &iceberg::TableIdent,
    winner: &DataFile,
) {
    let reader = open_two_registrant_fixture(warehouse).await;
    let table = reader.catalog().load_table(ident).await.unwrap();
    let registered = registered_v1_blobs(&table);
    assert_eq!(
        registered.len(),
        1,
        "one snapshot carries one statistics file: {registered:?}"
    );
    let (statistics_path, data_file, column) = registered[0].clone();
    assert_eq!(
        (data_file.as_str(), column.as_str()),
        (winner.file_path(), "raw"),
        "the registration that arrived first is the one metadata names"
    );

    let reachable = reader.reachable_files(ident).await.unwrap();
    assert!(
        reachable.contains(&statistics_path),
        "the winning statistics file must stay protected from orphan GC: {statistics_path}"
    );
    let objects = puffin_objects(warehouse);
    assert_eq!(
        objects.len(),
        2,
        "both registrants wrote their sidecar before deciding: {objects:?}"
    );
    let orphans: Vec<&String> = objects
        .iter()
        .filter(|path| !reachable.contains(*path))
        .collect();
    assert_eq!(
        orphans.len(),
        1,
        "the deferred caller's sidecar is left for orphan GC, and only it: {orphans:?}"
    );

    assert_eq!(
        text_counts(&reader).await,
        (2 * REBUILD_ROWS as i64, 30, 6_000),
        "both days answer exactly, indexed or not"
    );
}

/// Metric readings shared by the two #5298 regressions: the deferral is counted
/// once, and only the winner's file and bytes are reported as rebuilt.
fn assert_one_deferral_and_only_the_winner_rebuilt(snapshot: &SnapshotVec, winner: &DataFile) {
    assert_eq!(
        counter_sum(
            snapshot,
            "siglake_index_registration_deferred_total",
            Some(("reason", "snapshot_has_statistics"))
        ),
        1,
        "one deferral, whatever the attempt count: retries are the same deferral seen again"
    );
    assert_eq!(
        counter_sum(snapshot, "siglake_index_rebuild_files_total", None),
        1,
        "the winner's file only — a deferred caller reports no rebuilt file"
    );
    assert_eq!(
        counter_sum(snapshot, "siglake_index_rebuild_bytes_total", None),
        winner.file_size_in_bytes(),
        "the winner's bytes only — a deferred caller reports no rebuilt bytes"
    );
}

/// A competing registration lands after this caller loaded its table but
/// before the transaction's first commit attempt refreshed its base.
///
/// Both callers start from a snapshot with no statistics file and cover
/// disjoint files, so neither pre-write check sees the other. The check that
/// decides has to be the one inside the action, against the base the attempt
/// was re-applied to: without it the first attempt commits an unconditional
/// replacement onto the refreshed base and the other registrant's blobs leave
/// metadata (#5298).
#[test]
fn a_competing_registration_before_the_refresh_keeps_the_first_statistics_file() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("pre-refresh");
        let mut ours = open_two_registrant_fixture(&warehouse).await;
        let ident = ours.events_table_ident().clone();
        let theirs = append_rows(&ours, 0..REBUILD_ROWS, REBUILD_ROWS, RARE_EVERY).await;
        let mine = append_rows(
            &ours,
            REBUILD_ROWS..2 * REBUILD_ROWS,
            REBUILD_ROWS,
            RARE_EVERY,
        )
        .await;
        assert_eq!(
            (theirs.len(), mine.len()),
            (1, 1),
            "one file per day, and the two registrants take one each"
        );

        let rival = Arc::new(open_two_registrant_fixture(&warehouse).await);
        let raced = Arc::new(AtomicBool::new(false));
        let gated = TestCatalog::new(ours.catalog().clone())
            .after_load_table({
                let raced = raced.clone();
                let ident = ident.clone();
                let theirs = Arc::new(theirs.clone());
                move || {
                    let rival = rival.clone();
                    let raced = raced.clone();
                    let ident = ident.clone();
                    let theirs = theirs.clone();
                    async move {
                        // Once, on the load this caller computes its blobs
                        // from — before the transaction reloads the table.
                        if raced.swap(true, Ordering::SeqCst) {
                            return;
                        }
                        assert_eq!(
                            rival
                                .rebuild_inverted_indexes_for_files(&ident, &theirs)
                                .await
                                .unwrap(),
                            1,
                            "the rival reaches the statistics-free snapshot first"
                        );
                    }
                }
            })
            .shared();
        ours = ours.with_catalog_for_test(gated);

        assert_eq!(
            ours.rebuild_inverted_indexes_for_files(&ident, &mine)
                .await
                .unwrap(),
            0,
            "the second registrant defers rather than replacing the first's statistics file"
        );
        assert!(
            raced.load(Ordering::SeqCst),
            "the competing registration ran"
        );

        assert_first_registration_survived(&warehouse, &ident, &theirs[0]).await;
        let snapshot = snapshotter.snapshot().into_vec();
        assert_one_deferral_and_only_the_winner_rebuilt(&snapshot, &theirs[0]);
    });
}

/// A competing registration lands in the first attempt's CAS window: the
/// conditional catalog UPDATE finds the pointer moved, and the retry re-applies
/// the action against a base that now carries the rival's statistics file.
///
/// This is the half a pre-transaction check cannot reach at all. Both callers
/// passed their own absence check, and on the pre-#5298 path the retry
/// re-applied an unconditional replacement and won on the second attempt.
#[test]
fn a_competing_registration_in_the_cas_window_keeps_the_first_statistics_file() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("cas-window");
        let mut ours = open_two_registrant_fixture(&warehouse).await;
        let ident = ours.events_table_ident().clone();
        let theirs = append_rows(&ours, 0..REBUILD_ROWS, REBUILD_ROWS, RARE_EVERY).await;
        let mine = append_rows(
            &ours,
            REBUILD_ROWS..2 * REBUILD_ROWS,
            REBUILD_ROWS,
            RARE_EVERY,
        )
        .await;

        let rival = Arc::new(open_two_registrant_fixture(&warehouse).await);
        let gated = TestCatalog::new(ours.catalog().clone())
            .before_first_update_with_base({
                let ident = ident.clone();
                let theirs = Arc::new(theirs.clone());
                move || {
                    let rival = rival.clone();
                    let ident = ident.clone();
                    let theirs = theirs.clone();
                    async move {
                        assert_eq!(
                            rival
                                .rebuild_inverted_indexes_for_files(&ident, &theirs)
                                .await
                                .unwrap(),
                            1,
                            "the rival commits inside this attempt's CAS window"
                        );
                    }
                }
            })
            .shared();
        ours = ours.with_catalog_for_test(gated.clone());

        assert_eq!(
            ours.rebuild_inverted_indexes_for_files(&ident, &mine)
                .await
                .unwrap(),
            0,
            "the attempt that lost the CAS defers on the refreshed base instead of retrying \
             the replacement"
        );
        assert!(gated.fired(), "the forced-CAS hook fired");

        assert_first_registration_survived(&warehouse, &ident, &theirs[0]).await;
        let snapshot = snapshotter.snapshot().into_vec();
        assert_one_deferral_and_only_the_winner_rebuilt(&snapshot, &theirs[0]);
        assert_eq!(
            counter_sum(
                &snapshot,
                "siglake_catalog_cas_total",
                Some(("outcome", "conflict"))
            ),
            1,
            "exactly one lost CAS — the deferral is decided on the retry's base, not raced"
        );
    });
}

/// The post-commit full-file decode does not run for a column the rewrite
/// already indexed, and asking again rebuilds nothing.
///
/// `index_rebuild` is ON in these fixtures, so the rebuild is reached and
/// declines on coverage rather than on the knob: it finds the segmented blob
/// registered for `(file, raw)` and reads no Parquet. That is the pass this card
/// removes for the format — and it is also what makes a repeat idempotent.
#[test]
fn a_rewrite_that_built_its_index_needs_no_rebuild_and_a_repeat_is_a_no_op() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const ROWS_HERE: usize = 150_000;

        let ice = open_fixture(&tmp.path().join("idempotent"), true, Some(1), None).await;
        let files = append_and_rewrite(&ice, 0..ROWS_HERE, ROWS_HERE, RARE_EVERY).await;
        let ident = ice.events_table_ident().clone();
        let table = ice.catalog().load_table(&ident).await.unwrap();

        let types = registered_blob_types_for(&table, &files);
        assert_eq!(
            types.get(SEGMENTED_V2_BLOB_TYPE).copied(),
            Some(1),
            "the rewrite's own sidecar: {types:?}"
        );
        assert_eq!(
        types.get("siglake-inverted-v1").copied(),
        None,
        "the post-commit v1 decode must not have run for a column the rewrite indexed: {types:?}"
    );
        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(
            counter_sum(&snapshot, "siglake_index_rebuild_files_total", None),
            0,
            "no file was rebuilt"
        );
        assert_eq!(
            counter_sum(
                &snapshot,
                "siglake_iceberg_segmented_index_writes_total",
                Some(("outcome", "written"))
            ),
            1,
            "one sidecar written"
        );
        assert_eq!(
            counter_sum(
                &snapshot,
                "siglake_iceberg_segmented_index_writes_total",
                Some(("outcome", "refused"))
            ),
            0,
            "nothing refused"
        );

        // Ask again, explicitly.
        assert_eq!(
            ice.rebuild_inverted_indexes_for_files(&ident, &files)
                .await
                .unwrap(),
            0,
            "a repeated rebuild over an indexed file is a no-op"
        );
        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert_eq!(
            registered_blob_types_for(&table, &files),
            types,
            "and it registers nothing"
        );
    });
}

/// The index state a rewrite holds is one row group's, not the file's.
///
/// Measured from the writer's own per-group series
/// (`siglake_iceberg_segmented_index_group_index_bytes`, one sample per row
/// group: the live postings and dictionary it holds while encoding that group)
/// rather than from process heap, because the merge's own decoded buffers are
/// an order of magnitude larger than anything the index does and vary between
/// runs by more than the quantity under test.
///
/// Three claims, and the series is a pure function of the rows, so each is
/// exact rather than a threshold:
///
/// - four times the rows at the same row-group size leaves the largest group's
///   index where it was — what the file scales is the accumulated blob;
/// - a larger row group raises it in proportion to the group;
/// - the whole file's finished sidecar is smaller than one group's live index,
///   which is why holding the blob to the end is not the bound that matters.
#[test]
fn the_index_state_a_rewrite_holds_is_one_row_groups() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const SMALL: usize = 300_000;
        const LARGE: usize = 1_200_000;
        /// A byte target of 1 reaches the writer's `MIN_ROW_GROUP_ROWS` floor
        /// (128 Ki rows).
        const FLOOR_TARGET: usize = 1;
        /// Divided by the observed row size this is a few hundred thousand rows a
        /// group — above the floor, so the row group is the target's and not the
        /// clamp's.
        const WIDE_TARGET: usize = 64 << 20;

        let floor_small =
            group_index_arm(snapshotter, tmp.path(), "floor-small", SMALL, FLOOR_TARGET).await;
        let floor_large =
            group_index_arm(snapshotter, tmp.path(), "floor-large", LARGE, FLOOR_TARGET).await;
        let wide_large =
            group_index_arm(snapshotter, tmp.path(), "wide-large", LARGE, WIDE_TARGET).await;

        assert!(
            floor_large.row_groups >= 3 * floor_small.row_groups,
            "the large arm has to hold several times the row groups: {} against {}",
            floor_large.row_groups,
            floor_small.row_groups,
        );
        // Within 5%: the groups are the same size and only the last partial one
        // differs.
        assert!(
            floor_large
                .largest_index
                .abs_diff(floor_small.largest_index)
                * 20
                < floor_small.largest_index,
            "{} row groups must hold what {} do: {} B against {} B",
            floor_large.row_groups,
            floor_small.row_groups,
            floor_large.largest_index,
            floor_small.largest_index,
        );
        // The other direction: fewer, wider groups over the same rows hold more,
        // in proportion to the group.
        assert!(
            wide_large.row_groups < floor_large.row_groups,
            "the wide target has to produce fewer row groups: {} against {}",
            wide_large.row_groups,
            floor_large.row_groups,
        );
        assert!(
            wide_large.largest_index > floor_large.largest_index,
            "a wider row group holds a larger index: {} B over {} groups against {} B over {}",
            wide_large.largest_index,
            wide_large.row_groups,
            floor_large.largest_index,
            floor_large.row_groups,
        );
        // What the file does scale is the blob, reported apart for exactly that
        // reason — and it is still under one row group's live index.
        assert!(
            floor_large.blob > floor_small.blob,
            "the accumulated blob scales with the file: {} B against {} B",
            floor_large.blob,
            floor_small.blob,
        );
        assert!(
            floor_large.blob < floor_large.largest_index,
            "the whole file's compressed sidecar is under one row group's live index: {} B \
         against {} B",
            floor_large.blob,
            floor_large.largest_index,
        );
    });
}

/// One arm of [`the_index_state_a_rewrite_holds_is_one_row_groups`].
#[derive(Debug)]
struct GroupIndexArm {
    row_groups: usize,
    /// The largest per-group live index the writer held.
    largest_index: u64,
    /// The finished sidecar's bytes.
    blob: u64,
}

async fn group_index_arm(
    snapshotter: &Snapshotter,
    root: &std::path::Path,
    name: &str,
    rows: usize,
    row_group_target_bytes: usize,
) -> GroupIndexArm {
    let ice = open_fixture(&root.join(name), true, Some(row_group_target_bytes), None).await;
    let fresh = append_rows(&ice, 0..rows, rows, RARE_EVERY).await;
    // Drain whatever the appends recorded, so what follows is the rewrite's.
    let _ = snapshotter.snapshot();
    let live = rewrite_fresh(&ice, fresh).await;
    assert_eq!(live.len(), 1, "{name}: one output file");
    let snapshot = snapshotter.snapshot().into_vec();
    let per_group = histogram_samples(
        &snapshot,
        "siglake_iceberg_segmented_index_group_index_bytes",
    );
    let blobs = histogram_samples(&snapshot, "siglake_iceberg_segmented_index_written_bytes");
    let (row_groups, _) = parquet_rows_and_row_groups(live[0].file_path());
    assert_eq!(
        per_group.len(),
        row_groups.len(),
        "{name}: one index per row group, {per_group:?} against {row_groups:?}"
    );
    assert_eq!(blobs.len(), 1, "{name}: one finished sidecar");
    let arm = GroupIndexArm {
        row_groups: row_groups.len(),
        largest_index: per_group.iter().copied().fold(0f64, f64::max) as u64,
        blob: blobs[0] as u64,
    };
    println!("{name}: rows={rows} {arm:?} row_group_rows={row_groups:?}");
    arm
}

/// Every sample a debugging recorder holds for `name`.
fn histogram_samples(snapshot: &SnapshotVec, name: &str) -> Vec<f64> {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .flat_map(|(_, _, _, value)| match value {
            DebugValue::Histogram(samples) => {
                samples.iter().map(|sample| sample.into_inner()).collect()
            }
            _ => Vec::new(),
        })
        .collect()
}

// --------------------------------------------------------------- measurement

/// What building the sidecar during the merge costs, at the scale the design's
/// tables were taken at.
///
/// ```text
/// SIGLAKE_SEG_WRITER_FILES=14 SIGLAKE_SEG_WRITER_ROWS_PER_FILE=7340000 \
///   cargo test -p siglake-storage --release --test segmented_index_writer \
///   report_segmented_writer_build_cost -- --ignored --nocapture
/// ```
///
/// Five arms over the same corpus, one file (one day) at a time, each a fresh
/// append plus a streaming rewrite of exactly that day's output: two repeated
/// no-index/seg2-in-merge pairs, then a post-commit v1 rebuild. Reported per
/// arm: separate append and rewrite wall times, their total, peak live heap,
/// and the registered sidecar bytes for live files.
///
/// The heap figure covers **append plus every rewrite together** and holds one
/// rewrite's output at a time, so it says nothing about what a rewrite retains
/// while it rolls several outputs — that is
/// [`report_rolled_rewrite_sink_heap`] (#5299). Sized by
/// `SIGLAKE_SEG_WRITER_FILES` (1),
/// `SIGLAKE_SEG_WRITER_ROWS_PER_FILE` (300,000) and
/// `SIGLAKE_SEG_WRITER_RARE_EVERY` (100,000).
#[test]
#[ignore = "measurement; sized by SIGLAKE_SEG_WRITER_*"]
fn report_segmented_writer_build_cost() {
    serialized(|_snapshotter| async move {
        const V1_BLOB_TYPE: &str = "siglake-inverted-v1";

        fn knob(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.parse().ok())
                .unwrap_or(default)
        }
        let files = knob("SIGLAKE_SEG_WRITER_FILES", 1);
        let rows_per_file = knob("SIGLAKE_SEG_WRITER_ROWS_PER_FILE", 300_000);
        let rare_every = knob("SIGLAKE_SEG_WRITER_RARE_EVERY", 100_000);

        let tmp = tempfile::tempdir().unwrap();
        println!(
            "corpus: {files} files x {rows_per_file} rows = {} rows; append indexing=true, \
             target_row_group_bytes=1, streaming rewrite=true",
            files * rows_per_file,
        );

        let arms = [
            ("off-1", false, false, None),
            ("on-1", true, false, Some(SEGMENTED_V2_BLOB_TYPE)),
            ("off-2", false, false, None),
            ("on-2", true, false, Some(SEGMENTED_V2_BLOB_TYPE)),
            ("v1-rebuild", false, true, Some(V1_BLOB_TYPE)),
        ];
        for (label, segmented, rebuild, expected_blob_type) in arms {
            let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap();
            println!("{label}: loadavg before arm: {}", loadavg.trim());
            println!(
                "{label}: effective settings segmented_index_writes={segmented}, \
                 index_rebuild={rebuild}, index_at_flush=true"
            );
            let ice = open_fixture_with_tuning(
                &tmp.path().join(label),
                IcebergTuning {
                    segmented_index_writes: Some(segmented),
                    index_rebuild: Some(rebuild),
                    index_at_flush: Some(true),
                    target_row_group_bytes: Some(1),
                    ..Default::default()
                },
            )
            .await;
            let started = std::time::Instant::now();
            let mut append_wall = std::time::Duration::ZERO;
            let mut rewrite_wall = std::time::Duration::ZERO;
            let window = start_tracking();
            for file in 0..files {
                let append_started = std::time::Instant::now();
                let fresh = append_rows(
                    &ice,
                    file * rows_per_file..(file + 1) * rows_per_file,
                    rows_per_file,
                    rare_every,
                )
                .await;
                append_wall += append_started.elapsed();
                let rewrite_started = std::time::Instant::now();
                rewrite_fresh(&ice, fresh).await;
                rewrite_wall += rewrite_started.elapsed();
            }
            let heap = window.finish();
            let wall = started.elapsed();

            let table = ice
                .catalog()
                .load_table(ice.events_table_ident())
                .await
                .unwrap();
            let live = ice.live_data_files(ice.events_table_ident()).await.unwrap();
            let live_paths: Vec<&str> = live.iter().map(|file| file.file_path()).collect();
            let registered_types = registered_blob_types_for(&table, &live);
            assert_eq!(
                registered_types
                    .get(SEGMENTED_V2_BLOB_TYPE)
                    .copied()
                    .unwrap_or(0),
                usize::from(segmented) * live.len(),
                "{label}: unexpected seg2 coverage"
            );
            assert_eq!(
                registered_types.get(V1_BLOB_TYPE).copied().unwrap_or(0),
                usize::from(rebuild) * live.len(),
                "{label}: unexpected v1 coverage"
            );

            let mut sidecar_bytes = 0i64;
            let mut sidecars = 0usize;
            for statistics in table.metadata().statistics_iter() {
                let matching = statistics
                    .blob_metadata
                    .iter()
                    .filter(|blob| {
                        expected_blob_type == Some(blob.r#type.as_str())
                            && blob
                                .properties
                                .get("data_file")
                                .is_some_and(|path| live_paths.contains(&path.as_str()))
                    })
                    .count();
                if matching > 0 {
                    sidecar_bytes += statistics.file_size_in_bytes;
                    sidecars += matching;
                }
            }
            let data_bytes: u64 = live.iter().map(|f| f.file_size_in_bytes()).sum();
            println!(
                "{label}: append wall {:.2} s, rewrite wall {:.2} s, total wall {:.2} s, peak \
             live heap {} B ({:.1} MiB) over a {} B baseline, so {} B ({:.1} MiB) above it, \
             {sidecars} live sidecars in {sidecar_bytes} B against \
             {data_bytes} B of data ({:.2}%), {} live files",
                append_wall.as_secs_f64(),
                rewrite_wall.as_secs_f64(),
                wall.as_secs_f64(),
                heap.peak,
                heap.peak as f64 / (1024.0 * 1024.0),
                heap.baseline,
                heap.above_baseline,
                heap.above_baseline as f64 / (1024.0 * 1024.0),
                if data_bytes == 0 {
                    0.0
                } else {
                    100.0 * sidecar_bytes as f64 / data_bytes as f64
                },
                live.len(),
            );
            assert_eq!(
                text_counts(&ice).await.0,
                (files * rows_per_file) as i64,
                "{label} arm lost rows"
            );
            assert_eq!(
                sidecars,
                usize::from(expected_blob_type.is_some()) * live.len(),
                "{label}: one sidecar per live file when indexing is enabled"
            );
        }
    });
}

// ------------------ many rolled outputs in one rewrite, one partition (#5299)
//
// The cases above establish that a rolling rewrite addresses one sidecar to
// each output file. What they do not establish is what holding those sidecars
// together costs: a rewrite hands every finished blob to one shared
// `SegmentedIndexSink` (`third_party/iceberg/.../parquet_writer.rs`) and takes
// them out only after the merge is done, so the retained set grows with the
// output, while the parsed state each column's `SegmentedWriter` holds stays
// one row group's. These arms separate the two, at a fixed row-group size and
// a fixed file target, with the output volume and the indexed column count as
// the only things that vary.

/// The rolling file target the rolled arms use, far below the production
/// default (512 MiB) and below one row group's compressed bytes on this corpus,
/// so the writer closes a file at every row-group boundary. That is the stress
/// case for the shared sink: it is the most sidecars the output can carry per
/// row, since a file cannot hold fewer than one row group.
const ROLL_TARGET_BYTES: usize = 64 << 10;

/// The text columns the rolled arms index, in the order a config adds them.
/// Every one carries the same row text, so column count multiplies identical
/// index work instead of varying the corpus.
///
/// The first is `raw` on purpose: the row-group token bloom is built for a
/// column of that name (`raw_rowgroup_bloom_column_for_schema`), and it is what
/// makes the writer form row groups itself. Without it the control arm has no
/// reason to form them and rolls on the inner writer's in-progress bytes
/// instead, which at this target gives the two arms different output layouts —
/// 38 files against 3 on the first run of this measurement. With it, both arms
/// close a file at the same row-group boundary, as they do for `events`.
const ROLLED_COLUMNS: [&str; 3] = ["raw", "message", "title"];

/// A managed index with `columns` indexed text columns and a day-partitioned
/// event-time column. The `events` table's spec is fixed at one column
/// (`raw`), so a multi-column rewrite has to go through a managed index.
///
/// The event-time column has to be called `timestamp`: the streaming merge
/// resolves its merge key by that name and refuses a table without it
/// (`crates/siglake-storage/src/iceberg.rs`,
/// `declared_timestamp_merge_direction`). With no `timestamp_ns` sibling the
/// merge compares `timestamp` itself, and the corpus gives every row its own
/// microsecond, so the merge key is still a total order.
fn rolled_index_config(index_id: &str, columns: usize) -> IndexConfig {
    let mut field_mappings = vec![FieldMapping {
        name: "timestamp".to_string(),
        field_type: FieldType::Datetime,
        required: true,
    }];
    for column in &ROLLED_COLUMNS[..columns] {
        field_mappings.push(FieldMapping {
            name: (*column).to_string(),
            field_type: FieldType::Text {
                tokenizer: Some("default".to_string()),
            },
            required: false,
        });
    }
    IndexConfig {
        index_id: index_id.to_string(),
        doc_mapping: DocMapping {
            mode: MappingMode::Dynamic,
            field_mappings,
            timestamp_field: "timestamp".to_string(),
            tag_fields: Vec::new(),
            default_search_fields: ROLLED_COLUMNS[..columns]
                .iter()
                .map(|column| (*column).to_string())
                .collect(),
        },
        retention: None,
        index_at_flush: None,
    }
}

/// One append's rows, in the managed index's schema: `ts`, one copy of the
/// shaped text per indexed column, and the residual-attributes column the
/// schema always carries.
fn rolled_index_batch(
    config: &IndexConfig,
    rows: std::ops::Range<usize>,
    rows_per_day: usize,
    rare_every: usize,
) -> arrow_array::RecordBatch {
    let base = fixture_base();
    let mut timestamps = Vec::with_capacity(rows.len());
    let mut text = Vec::with_capacity(rows.len());
    for row in rows.clone() {
        let event = shaped_event(row, rows_per_day, base, rare_every);
        timestamps.push(event.timestamp.timestamp_micros());
        text.push(event.raw);
    }
    let columns = config.doc_mapping.field_mappings.len() - 1;
    let mut arrays: Vec<arrow_array::ArrayRef> = vec![Arc::new(
        arrow_array::TimestampMicrosecondArray::from(timestamps)
            .with_timezone(siglake_core::TIMESTAMP_TZ),
    )];
    for _ in 0..columns {
        arrays.push(Arc::new(arrow_array::StringArray::from(text.clone())));
    }
    arrays.push(Arc::new(arrow_array::StringArray::new_null(text.len())));
    arrow_array::RecordBatch::try_new(config.to_arrow_schema(), arrays).unwrap()
}

/// Append `rows` of the corpus to a managed index, in [`APPEND_CHUNK`]-row
/// batches. Returns the data files the appends added — the single-partition bin
/// one rewrite then merges.
async fn append_rolled_rows(
    ice: &IcebergContext,
    ident: &iceberg::TableIdent,
    config: &IndexConfig,
    rows: std::ops::Range<usize>,
    rows_per_day: usize,
    rare_every: usize,
) -> Vec<DataFile> {
    let before: Vec<String> = ice
        .live_data_files(ident)
        .await
        .unwrap()
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    for chunk in rows.clone().step_by(APPEND_CHUNK) {
        let batch = rolled_index_batch(
            config,
            chunk..(chunk + APPEND_CHUNK).min(rows.end),
            rows_per_day,
            rare_every,
        );
        ice.append_to_table(ident, batch, &[]).await.unwrap();
    }
    ice.live_data_files(ident)
        .await
        .unwrap()
        .into_iter()
        .filter(|file| !before.contains(&file.file_path().to_string()))
        .collect()
}

/// The managed index's row count and, per indexed column, its `rareneedle`
/// count — the exact answers every arm has to give, indexed or not.
async fn rolled_text_counts(ice: &IcebergContext, index_id: &str, columns: usize) -> Vec<i64> {
    let ctx = SessionContext::new();
    ctx.register_udf(ScalarUDF::from(MatchTermsUdf::new()));
    assert!(
        ice.register_index_with_datafusion(&ctx, index_id)
            .await
            .unwrap(),
        "the managed index registers with DataFusion"
    );
    let mut out = vec![count(&ctx, &format!("SELECT count(*) FROM {index_id}")).await];
    for column in &ROLLED_COLUMNS[..columns] {
        out.push(
            count(
                &ctx,
                &format!(
                    "SELECT count(*) FROM {index_id} WHERE match_terms({column}, 'rareneedle')"
                ),
            )
            .await,
        );
    }
    out
}

/// What one rewrite of one partition into several rolled files cost.
#[derive(Debug)]
struct RolledRewriteArm {
    /// Data files the one rewrite produced — the sidecars the sink held
    /// together, per column.
    outputs: usize,
    /// Row groups across those files: the number of per-group index builds.
    row_groups: usize,
    /// Registered `(file, column)` seg2 blobs after the commit.
    blobs: usize,
    heap: HeapReading,
    rewrite: std::time::Duration,
    /// Serialized sidecar bytes the sink accumulated over the rewrite: every
    /// finished blob is retained until `take()` runs after the merge.
    retained_sidecar_bytes: u64,
    /// The largest parsed per-group index the writer held live, and the sum of
    /// them — the sum is *not* held at once, and reporting both is the point.
    largest_group_index_bytes: u64,
    total_group_index_bytes: u64,
    /// Puffin statistics bytes registered for the live output.
    registered_bytes: u64,
    /// Parquet bytes of the live output, for scale.
    data_bytes: u64,
}

/// Run one arm: append `rows` into a single day partition, then rewrite that
/// whole bin in one streaming call. Everything but `segmented` and `columns` is
/// held fixed, so an on/off pair is matched and a volume pair differs only in
/// how many outputs the same rolling target produces.
#[allow(clippy::too_many_arguments)]
async fn rolled_rewrite_arm(
    snapshotter: &Snapshotter,
    root: &std::path::Path,
    label: &str,
    rows: usize,
    columns: usize,
    segmented: bool,
    target_file_bytes: usize,
    rare_every: usize,
) -> RolledRewriteArm {
    let ice = open_fixture_with_tuning(
        &root.join(label),
        IcebergTuning {
            segmented_index_writes: Some(segmented),
            // Both off in every arm: the control must differ from the seg2 arm
            // only in the rewrite's own index build, and an append-time index
            // would put its allocations inside nothing but the baseline.
            index_rebuild: Some(false),
            index_at_flush: Some(false),
            target_row_group_bytes: Some(1),
            merge_target_file_bytes: Some(target_file_bytes),
            ..Default::default()
        },
    )
    .await;
    let index_id = "rolled";
    let config = rolled_index_config(index_id, columns);
    let ident = ice.create_index(&config).await.unwrap();
    // `rows_per_day = rows` keeps the whole corpus inside one day partition, so
    // the streaming merge takes the entire bin in one call.
    let fresh = append_rolled_rows(&ice, &ident, &config, 0..rows, rows, rare_every).await;
    assert!(
        fresh.len() > 1,
        "{label}: the bin has to hold several input files"
    );

    // Drain what the appends recorded, so the histograms below are the
    // rewrite's alone.
    let _ = snapshotter.snapshot();
    let started = std::time::Instant::now();
    let window = start_tracking();
    let live = rewrite_fresh_in(&ice, &ident, fresh, &[]).await;
    let heap = window.finish();
    let rewrite = started.elapsed();
    let snapshot = snapshotter.snapshot().into_vec();

    let data_bytes: u64 = live.iter().map(|file| file.file_size_in_bytes()).sum();
    assert!(
        live.len() > 1,
        "{label}: one rewrite has to roll into several output files, not {}: \
         {data_bytes} B of output against a {target_file_bytes} B rolling target",
        live.len()
    );
    let row_groups: usize = live
        .iter()
        .map(|file| {
            parquet_rows_and_row_groups_of(file.file_path(), ROLLED_COLUMNS[0])
                .0
                .len()
        })
        .sum();

    let per_group = histogram_samples(
        &snapshot,
        "siglake_iceberg_segmented_index_group_index_bytes",
    );
    let written = histogram_samples(&snapshot, "siglake_iceberg_segmented_index_written_bytes");
    let expected_blobs = usize::from(segmented) * live.len() * columns;
    assert_eq!(
        written.len(),
        expected_blobs,
        "{label}: one finished sidecar per output file and column"
    );
    assert_eq!(
        per_group.len(),
        usize::from(segmented) * row_groups * columns,
        "{label}: one parsed index per row group and column, over {row_groups} row groups"
    );

    let table = ice.catalog().load_table(&ident).await.unwrap();
    let blobs = registered_seg2_blobs(&table);
    let live_paths: Vec<&str> = live.iter().map(|file| file.file_path()).collect();
    let mut expected: Vec<(String, String)> = Vec::new();
    for path in &live_paths {
        for column in &ROLLED_COLUMNS[..columns] {
            expected.push(((*path).to_string(), (*column).to_string()));
        }
    }
    expected.sort();
    let mut registered: Vec<(String, String)> = blobs
        .iter()
        .map(|(_, file, column)| (file.clone(), column.clone()))
        .collect();
    registered.sort();
    assert_eq!(
        registered,
        if segmented { expected } else { Vec::new() },
        "{label}: every output file and column is registered, and only those"
    );

    let mut registered_bytes = 0u64;
    for statistics in table.metadata().statistics_iter() {
        let names_live = statistics.blob_metadata.iter().any(|blob| {
            blob.properties
                .get("data_file")
                .is_some_and(|path| live_paths.contains(&path.as_str()))
        });
        if names_live {
            registered_bytes += statistics.file_size_in_bytes as u64;
        }
    }

    let counts = rolled_text_counts(&ice, index_id, columns).await;
    let rare = ((rows - 1) / rare_every + 1) as i64;
    let mut want = vec![rows as i64];
    want.extend(std::iter::repeat_n(rare, columns));
    assert_eq!(
        counts, want,
        "{label}: the rewrite's rows and every indexed column's term answer exactly"
    );

    let arm = RolledRewriteArm {
        outputs: live.len(),
        row_groups,
        blobs: blobs.len(),
        heap,
        rewrite,
        retained_sidecar_bytes: written.iter().sum::<f64>() as u64,
        largest_group_index_bytes: per_group.iter().copied().fold(0f64, f64::max) as u64,
        total_group_index_bytes: per_group.iter().sum::<f64>() as u64,
        registered_bytes,
        data_bytes,
    };
    println!(
        "{label}: rows={rows} columns={columns} segmented={segmented} \
         outputs={} row_groups={} blobs={} rewrite={:.2} s | heap: baseline {} B, peak {} B, \
         above baseline {} B ({:.1} MiB) | retained sidecars {} B ({:.1} MiB) | parsed group \
         index: largest {} B, sum {} B | registered {} B against {} B of data",
        arm.outputs,
        arm.row_groups,
        arm.blobs,
        arm.rewrite.as_secs_f64(),
        arm.heap.baseline,
        arm.heap.peak,
        arm.heap.above_baseline,
        arm.heap.above_baseline as f64 / (1024.0 * 1024.0),
        arm.retained_sidecar_bytes,
        arm.retained_sidecar_bytes as f64 / (1024.0 * 1024.0),
        arm.largest_group_index_bytes,
        arm.total_group_index_bytes,
        arm.registered_bytes,
        arm.data_bytes,
    );
    arm
}

/// One rewrite, one partition, several rolled outputs, several indexed columns:
/// every output file and column is registered, each sidecar describes its own
/// file, and the answers are exact.
///
/// The bounded hermetic half of #5299 — the numbers are
/// [`report_rolled_rewrite_sink_heap`].
#[test]
fn one_rewrite_rolling_many_outputs_registers_every_file_and_column() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        const COLUMNS: usize = 2;

        let arm = rolled_rewrite_arm(
            snapshotter,
            tmp.path(),
            "rolled-multi",
            ROWS,
            COLUMNS,
            true,
            ROLL_TARGET_BYTES,
            RARE_EVERY,
        )
        .await;
        assert_eq!(
            arm.blobs,
            arm.outputs * COLUMNS,
            "one sidecar per output file and column: {arm:?}"
        );
        assert!(
            arm.retained_sidecar_bytes > 0 && arm.largest_group_index_bytes > 0,
            "both quantities have to be measurable or the report means nothing: {arm:?}"
        );

        // The sidecars themselves, read back out of the Puffin objects the
        // rewrite registered: the arm proved they are addressed to the right
        // file and column, this proves each one answers for that file's rows.
        let ice = open_fixture_with_tuning(
            &tmp.path().join("rolled-multi"),
            IcebergTuning {
                segmented_index_writes: Some(true),
                index_rebuild: Some(false),
                index_at_flush: Some(false),
                target_row_group_bytes: Some(1),
                merge_target_file_bytes: Some(ROLL_TARGET_BYTES),
                ..Default::default()
            },
        )
        .await;
        let ident = ice.index_table_ident("rolled");
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let blobs = registered_seg2_blobs(&table);
        assert_eq!(blobs.len(), arm.blobs);
        for (statistics_path, data_file, column) in blobs {
            assert_sidecar_describes_file(&table, &statistics_path, &data_file, &column).await;
        }

        // And the matched control: the same corpus and the same rolling target
        // with the writer off registers nothing and answers the same.
        let off = rolled_rewrite_arm(
            snapshotter,
            tmp.path(),
            "rolled-multi-off",
            ROWS,
            COLUMNS,
            false,
            ROLL_TARGET_BYTES,
            RARE_EVERY,
        )
        .await;
        assert_eq!(off.blobs, 0, "the knob is off in the control arm");
        assert_eq!(
            (off.outputs, off.row_groups),
            (arm.outputs, arm.row_groups),
            "the writer builds an index beside the output, it does not change it"
        );
    });
}

/// What one rewrite retains while it rolls several outputs, against the output
/// volume and the indexed column count.
///
/// ```text
/// SIGLAKE_SEG_ROLL_ROWS=1310720,2621440 SIGLAKE_SEG_ROLL_COLUMNS=1,2 \
///   cargo test -p siglake-storage --release --test segmented_index_writer \
///   report_rolled_rewrite_sink_heap -- --ignored --nocapture
/// ```
///
/// Each arm is ONE streaming rewrite of one day partition, at a fixed row-group
/// target (the 128 Ki-row floor) and a fixed rolling file target, so growing
/// `SIGLAKE_SEG_ROLL_ROWS` grows the number of output files whose sidecars the
/// shared sink holds at once. Every `(rows, columns)` shape runs with segmented
/// writes on and off, and the two are otherwise identical.
///
/// Reported per arm, separately on purpose: the rewrite-only peak live heap
/// above the live heap the rewrite started from; the serialized sidecar bytes
/// the sink accumulated; and the parsed per-group index bytes (largest, and the
/// sum that is never live at once). Sized by `SIGLAKE_SEG_ROLL_ROWS`
/// (300,000), `SIGLAKE_SEG_ROLL_COLUMNS` (1,2),
/// `SIGLAKE_SEG_ROLL_FILE_BYTES` (64 KiB, one output file per row group) and
/// `SIGLAKE_SEG_ROLL_RARE_EVERY` (10,000).
#[test]
#[ignore = "measurement; sized by SIGLAKE_SEG_ROLL_*"]
fn report_rolled_rewrite_sink_heap() {
    serialized(|snapshotter| async move {
        fn list(name: &str, default: &[usize]) -> Vec<usize> {
            match std::env::var(name) {
                Ok(raw) => raw
                    .split(',')
                    .filter_map(|item| item.trim().parse().ok())
                    .collect(),
                Err(_) => default.to_vec(),
            }
        }
        fn knob(name: &str, default: usize) -> usize {
            std::env::var(name)
                .ok()
                .and_then(|raw| raw.parse().ok())
                .unwrap_or(default)
        }

        let volumes = list("SIGLAKE_SEG_ROLL_ROWS", &[ROWS]);
        let column_counts = list("SIGLAKE_SEG_ROLL_COLUMNS", &[1, 2]);
        let target_file_bytes = knob("SIGLAKE_SEG_ROLL_FILE_BYTES", ROLL_TARGET_BYTES);
        let rare_every = knob("SIGLAKE_SEG_ROLL_RARE_EVERY", RARE_EVERY);
        assert!(
            column_counts
                .iter()
                .all(|columns| (1..=ROLLED_COLUMNS.len()).contains(columns)),
            "SIGLAKE_SEG_ROLL_COLUMNS must name between 1 and {} columns",
            ROLLED_COLUMNS.len()
        );

        let tmp = tempfile::tempdir().unwrap();
        println!(
            "one same-partition rewrite per arm; rolling file target {target_file_bytes} B, \
             target_row_group_bytes=1 (128 Ki-row floor), append indexing off, v1 rebuild off"
        );
        for rows in &volumes {
            for columns in &column_counts {
                for segmented in [false, true] {
                    let label = format!(
                        "r{rows}-c{columns}-{}",
                        if segmented { "on" } else { "off" }
                    );
                    println!(
                        "{label}: loadavg before arm: {}",
                        std::fs::read_to_string("/proc/loadavg").unwrap().trim()
                    );
                    rolled_rewrite_arm(
                        snapshotter,
                        tmp.path(),
                        &label,
                        *rows,
                        *columns,
                        segmented,
                        target_file_bytes,
                        rare_every,
                    )
                    .await;
                }
            }
        }
    });
}

/// #5231's dashboard row reads `siglake_iceberg_segmented_index_writes_total`
/// through `increase()`, so every `(outcome, reason)` the writer can record has
/// to exist at 0 on a compactor that has not closed a sidecar yet: a `refused`
/// arm that is absent and one that is flat at zero are the same chart, and the
/// second is the reading. The label values are variables at the emitter, so
/// `scripts/check-chart.py` sees a dynamic site and cannot hold the catalog to
/// it; the fork's own exported vocabulary can.
///
/// Pure — it compares two constants and takes no global state, so it needs no
/// [`WRITER_TESTS`] turn.
#[test]
fn segmented_index_write_series_are_preregistered() {
    use std::collections::BTreeSet;

    let listed: BTreeSet<BTreeSet<(&str, &str)>> =
        siglake_core::metrics::COMPACTOR_ALERTED_COUNTERS
            .iter()
            .filter(|counter| counter.name == "siglake_iceberg_segmented_index_writes_total")
            .flat_map(|counter| counter.series.iter())
            .map(|labels| labels.iter().copied().collect())
            .collect();

    let recorded: BTreeSet<BTreeSet<(&str, &str)>> =
        iceberg::writer::file_writer::SEGMENTED_INDEX_WRITE_SERIES
            .iter()
            .map(|(outcome, reason)| BTreeSet::from([("outcome", *outcome), ("reason", *reason)]))
            .collect();

    assert_eq!(
        listed, recorded,
        "every (outcome, reason) the sidecar writer records must be created at 0: \
         update COMPACTOR_ALERTED_COUNTERS in siglake_core::metrics"
    );
}

// ------------- deferred registration: reuse at a later snapshot (#5319)

/// Read an already-written Puffin sidecar's footer back into the
/// `StatisticsFile` value its writer held, re-addressed to `snapshot_id`.
///
/// This is the whole payload a bounded reuse has to keep: the deferred caller
/// already holds this struct — `write_puffin_sidecar` returns it — so
/// reconstructing it here measures what an implementation would retain rather
/// than something it would not. The blob payload is never touched: only the
/// footer is read, and the object is left byte for byte as the deferral wrote
/// it.
async fn statistics_file_from_object(
    table: &Table,
    path: &str,
    snapshot_id: i64,
) -> StatisticsFile {
    let file_size_in_bytes = table
        .file_io()
        .new_input(path)
        .unwrap()
        .metadata()
        .await
        .unwrap()
        .size as i64;
    let reader = iceberg::puffin::PuffinReader::new(table.file_io().new_input(path).unwrap());
    let file_footer_size_in_bytes = reader.footer_size_in_bytes().await.unwrap() as i64;
    let blob_metadata = reader
        .file_metadata()
        .await
        .unwrap()
        .blobs()
        .iter()
        .map(|blob| StatisticsBlobMetadata {
            r#type: blob.blob_type().to_string(),
            // Deliberately the snapshot the blob was COMPUTED FROM, which the
            // reuse does not change (`puffin/metadata.rs`). Only the
            // `StatisticsFile` the entry hangs off is re-addressed.
            snapshot_id: blob.snapshot_id(),
            sequence_number: blob.sequence_number(),
            fields: blob.fields().to_vec(),
            properties: blob.properties().clone(),
        })
        .collect();
    StatisticsFile {
        snapshot_id,
        statistics_path: path.to_string(),
        file_size_in_bytes,
        file_footer_size_in_bytes,
        key_metadata: None,
        blob_metadata,
    }
}

fn bytes_on_disk(path: &str) -> u64 {
    std::fs::metadata(local_path(path)).unwrap().len()
}

/// Rows per day for the reuse report. The point of sizing it is that the
/// retained payload does not grow with the file while the decode it avoids
/// does, so the report is run at two widths.
fn reuse_rows_per_file() -> usize {
    std::env::var("SIGLAKE_REUSE_ROWS_PER_FILE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(REBUILD_ROWS)
}

/// #5319: what bounded reuse of a deferred Puffin registration costs, and what
/// it recovers.
///
/// Starts from the state #5298's two-registrant regressions leave behind — one
/// statistics file registered, one uploaded sidecar orphaned — and re-addresses
/// the orphan to a later statistics-free snapshot. Reports, against the
/// alternative of decoding the file again:
///
/// - the retained payload (the `StatisticsFile` the deferred caller holds),
/// - the object-store bytes the reuse moves (none: no blob is read or written),
/// - the decode the reuse avoids,
/// - and the one thing it does NOT keep in agreement — the Puffin footer's
///   `snapshot-id`, which still names the snapshot the blobs were computed
///   from while the catalog entry names the snapshot they are attached to.
///
/// ```
/// cargo test -p siglake-storage --test segmented_index_writer \
///   report_deferred_registration_reuse_cost -- --ignored --nocapture
/// ```
#[test]
#[ignore = "measurement: two full-file index builds plus three appends"]
fn report_deferred_registration_reuse_cost() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("reuse");
        let rows = reuse_rows_per_file();
        let mut ours = open_two_registrant_fixture(&warehouse).await;
        let ident = ours.events_table_ident().clone();
        let theirs = append_rows(&ours, 0..rows, rows, RARE_EVERY).await;
        let mine = append_rows(&ours, rows..2 * rows, rows, RARE_EVERY).await;
        assert!(
            !theirs.is_empty() && !mine.is_empty(),
            "each day has to produce files for the two registrants to split"
        );

        // 1. Reproduce the deferral: a rival registers against the snapshot
        //    this caller is aimed at, between its load and its commit.
        let rival = Arc::new(open_two_registrant_fixture(&warehouse).await);
        let raced = Arc::new(AtomicBool::new(false));
        let gated = TestCatalog::new(ours.catalog().clone())
            .after_load_table({
                let raced = raced.clone();
                let ident = ident.clone();
                let theirs = Arc::new(theirs.clone());
                move || {
                    let rival = rival.clone();
                    let raced = raced.clone();
                    let ident = ident.clone();
                    let theirs = theirs.clone();
                    async move {
                        if raced.swap(true, Ordering::SeqCst) {
                            return;
                        }
                        assert_eq!(
                            rival
                                .rebuild_inverted_indexes_for_files(&ident, &theirs)
                                .await
                                .unwrap(),
                            theirs.len()
                        );
                    }
                }
            })
            .shared();
        ours = ours.with_catalog_for_test(gated);
        let deferred_build = std::time::Instant::now();
        assert_eq!(
            ours.rebuild_inverted_indexes_for_files(&ident, &mine)
                .await
                .unwrap(),
            0,
            "the second registrant defers"
        );
        let deferred_build = deferred_build.elapsed();
        drop(ours);

        // 2. Name the orphan the deferral left: the object no retained
        //    snapshot references.
        let ice = open_two_registrant_fixture(&warehouse).await;
        let reachable = ice.reachable_files(&ident).await.unwrap();
        let orphan = puffin_objects(&warehouse)
            .into_iter()
            .find(|path| !reachable.contains(path))
            .expect("the deferred caller's sidecar");
        let occupied_snapshot = ice
            .catalog()
            .load_table(&ident)
            .await
            .unwrap()
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();

        // 3. The alternative this is measured against: decode a comparable
        //    file again and register it the ordinary way. Day 2's append also
        //    moves the table past the occupied snapshot.
        let day2 = append_rows(&ice, 2 * rows..3 * rows, rows, RARE_EVERY).await;
        let redecode = std::time::Instant::now();
        assert_eq!(
            ice.rebuild_inverted_indexes_for_files(&ident, &day2)
                .await
                .unwrap(),
            day2.len(),
            "day 2 registers against its own statistics-free snapshot"
        );
        let redecode = redecode.elapsed();

        // 4. A later snapshot that carries no statistics file: the only kind
        //    the reuse is eligible for.
        append_rows(&ice, 3 * rows..4 * rows, rows, RARE_EVERY).await;
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let later_snapshot = table.metadata().current_snapshot().unwrap().snapshot_id();
        assert!(
            table
                .metadata()
                .statistics_for_snapshot(later_snapshot)
                .is_none(),
            "the reuse target must be statistics-free"
        );

        // 5. Re-address the retained payload and register it. No blob is read
        //    and none is written.
        let objects_before = puffin_objects(&warehouse);
        let orphan_mtime = std::fs::metadata(local_path(&orphan))
            .unwrap()
            .modified()
            .unwrap();
        let reused = statistics_file_from_object(&table, &orphan, later_snapshot).await;
        let retained_bytes = serde_json::to_vec(&reused).unwrap().len();
        let footer_snapshot = reused.blob_metadata[0].snapshot_id;
        let reuse = std::time::Instant::now();
        let tx = Transaction::new(&table);
        let tx = tx
            .update_statistics()
            .set_statistics(reused)
            .apply(tx)
            .unwrap();
        tx.commit(ice.catalog().as_ref()).await.unwrap();
        let reuse = reuse.elapsed();

        // 6. What it bought. Coverage is back, and the object is untouched.
        assert_eq!(
            puffin_objects(&warehouse),
            objects_before,
            "the reuse writes no new sidecar"
        );
        assert_eq!(
            std::fs::metadata(local_path(&orphan))
                .unwrap()
                .modified()
                .unwrap(),
            orphan_mtime,
            "and rewrites none of the one it reuses"
        );
        let table = ice.catalog().load_table(&ident).await.unwrap();
        assert!(
            registered_v1_blobs(&table)
                .iter()
                .any(|(path, data_file, column)| path == &orphan
                    && data_file == mine[0].file_path()
                    && column == "raw"),
            "the deferred file is covered at the later snapshot"
        );
        assert!(
            ice.reachable_files(&ident).await.unwrap().contains(&orphan),
            "and the object it reuses is no longer an orphan"
        );
        let _ = snapshotter.snapshot();
        assert_eq!(
            ice.rebuild_inverted_indexes_for_files(&ident, &mine)
                .await
                .unwrap(),
            0,
            "a rebuild over the recovered file finds it covered"
        );
        let after = snapshotter.snapshot().into_vec();
        assert_eq!(
            counter_sum(
                &after,
                "siglake_index_registration_deferred_total",
                Some(("reason", "snapshot_has_statistics"))
            ),
            0,
            "covered, not deferred again: the recovered file needs no second decode"
        );
        assert_eq!(
            puffin_objects(&warehouse),
            objects_before,
            "and that rebuild decoded nothing"
        );

        let _ = snapshotter.snapshot();
        assert_eq!(
            text_counts(&ice).await,
            (
                4 * rows as i64,
                (4 * rows / RARE_EVERY) as i64,
                (4 * rows / 50) as i64
            ),
            "every day answers exactly"
        );
        let reads = snapshotter.snapshot().into_vec();
        let puffin_used = counter_sum(
            &reads,
            "siglake_iceberg_inverted_index_used_total",
            Some(("storage", "puffin")),
        );

        let data_bytes: u64 = mine
            .iter()
            .map(|file| bytes_on_disk(file.file_path()))
            .sum();
        let sidecar_bytes = bytes_on_disk(&orphan);
        println!(
            "\n=== #5319 deferred-registration reuse, {rows} rows/day in {} file(s) ===",
            mine.len()
        );
        println!("deferred data files       {data_bytes:>12} B");
        println!(
            "sidecar object            {sidecar_bytes:>12} B (already uploaded by the deferral)"
        );
        println!("retained payload          {retained_bytes:>12} B (StatisticsFile as JSON)");
        println!(
            "bytes moved by the reuse  {:>12} B (footer read only; no blob, no write)",
            0
        );
        println!(
            "deferred build+upload     {:>12.3} s (includes the rival's own rebuild, gated inside it)",
            deferred_build.as_secs_f64()
        );
        println!(
            "re-decode + register      {:>12.3} s (the alternative, comparable file)",
            redecode.as_secs_f64()
        );
        println!("reuse commit              {:>12.3} s", reuse.as_secs_f64());
        println!("puffin index used         {puffin_used:>12} times over the four days");
        println!(
            "footer snapshot-id {footer_snapshot} vs statistics entry snapshot-id {later_snapshot} \
             (computed-from vs attached-to; occupied snapshot was {occupied_snapshot})"
        );
    });
}
