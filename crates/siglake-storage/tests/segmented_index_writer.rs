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
//! - the post-commit full-file decode does not run for a column the rewrite
//!   already indexed, and a second rebuild call is a no-op;
//! - the index state the rewrite holds is one row group's, not the file's.
//!
//! Its own test binary: it installs a process-global metrics recorder and a
//! global allocator, both process state, so the tests take [`WRITER_TESTS`] and
//! run one at a time.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use arrow_array::Array;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::prelude::SessionContext;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_index::segmented::{SegmentedReader, SliceSource, SEGMENTED_V2_BLOB_TYPE};
use siglake_index::InvertedIndex;
use siglake_storage::iceberg::{IcebergContext, IcebergTuning, ReclusterMergeOptions};

/// Peak live heap bytes between [`start_tracking`] and [`peak_tracked`], the
/// same test-only wrapper `delete_task_size_gate.rs` uses. Tracking is off
/// unless a test turns it on, and every test here holds [`WRITER_TESTS`] while
/// it does, so the number belongs to one arm of one A/B.
struct PeakTracking;

static TRACKING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for PeakTracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && TRACKING.load(Ordering::Relaxed) {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACKING.load(Ordering::Relaxed) {
            // Saturating: tracking starts mid-process, so some of what is freed
            // here was allocated before LIVE existed.
            let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
                Some(live.saturating_sub(layout.size()))
            });
        }
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakTracking = PeakTracking;

fn start_tracking() {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    TRACKING.store(true, Ordering::Relaxed);
}

fn peak_tracked() -> usize {
    TRACKING.store(false, Ordering::Relaxed);
    PEAK.load(Ordering::Relaxed)
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
    IcebergContext::open(path)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_table_cache_ttl(std::time::Duration::ZERO)
        .with_tuning(IcebergTuning {
            segmented_index_writes: Some(segmented),
            // The post-commit v1 rebuild ON, so "the rewrite already indexed
            // this column" is the thing that stops it rather than the knob.
            index_rebuild: Some(true),
            target_row_group_bytes: row_group_bytes,
            merge_target_file_bytes: target_file_bytes,
            ..Default::default()
        })
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
    ice.recluster_files_with(
        &ident,
        fresh,
        &bloom_refs,
        &ReclusterMergeOptions {
            force_streaming: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    ice.live_data_files(&ident).await.unwrap()
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
        let idx = batch.schema().index_of("raw").unwrap();
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
    let (row_groups, rows) = parquet_rows_and_row_groups(data_file);
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

async fn count(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn text_counts(ice: &IcebergContext) -> (i64, i64, i64) {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    (
        count(&ctx, "SELECT count(*) FROM events").await,
        count(
            &ctx,
            "SELECT count(*) FROM events WHERE raw LIKE '%rareneedle%'",
        )
        .await,
        count(&ctx, "SELECT count(*) FROM events WHERE raw LIKE '%queen%'").await,
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
/// Two arms over the same corpus, one file (one day) at a time, each a fresh
/// append plus a streaming rewrite of exactly that day's output: the control
/// with segmented writes off, then the same with them on. Reported per arm:
/// wall time for the appends and rewrites, peak live heap, and the registered
/// sidecar bytes. Sized by `SIGLAKE_SEG_WRITER_FILES` (1),
/// `SIGLAKE_SEG_WRITER_ROWS_PER_FILE` (300,000) and
/// `SIGLAKE_SEG_WRITER_RARE_EVERY` (100,000).
#[test]
#[ignore = "measurement; sized by SIGLAKE_SEG_WRITER_*"]
fn report_segmented_writer_build_cost() {
    serialized(|_snapshotter| async move {
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
            "corpus: {files} files x {rows_per_file} rows = {} rows",
            files * rows_per_file
        );

        for (label, segmented) in [("off", false), ("on", true)] {
            let ice = open_fixture(&tmp.path().join(label), segmented, Some(1), None).await;
            let started = std::time::Instant::now();
            start_tracking();
            for file in 0..files {
                append_and_rewrite(
                    &ice,
                    file * rows_per_file..(file + 1) * rows_per_file,
                    rows_per_file,
                    rare_every,
                )
                .await;
            }
            let peak = peak_tracked();
            let wall = started.elapsed();

            let table = ice
                .catalog()
                .load_table(ice.events_table_ident())
                .await
                .unwrap();
            let mut sidecar_bytes = 0i64;
            let mut sidecars = 0usize;
            for statistics in table.metadata().statistics_iter() {
                if statistics
                    .blob_metadata
                    .iter()
                    .any(|blob| blob.r#type == SEGMENTED_V2_BLOB_TYPE)
                {
                    sidecar_bytes += statistics.file_size_in_bytes;
                    sidecars += statistics
                        .blob_metadata
                        .iter()
                        .filter(|blob| blob.r#type == SEGMENTED_V2_BLOB_TYPE)
                        .count();
                }
            }
            let live = ice.live_data_files(ice.events_table_ident()).await.unwrap();
            let data_bytes: u64 = live.iter().map(|f| f.file_size_in_bytes()).sum();
            println!(
                "{label}: wall {:.2} s, peak live heap {} B ({:.1} MiB), {sidecars} sidecars in \
             {sidecar_bytes} B against {data_bytes} B of data ({:.2}%), {} live files",
                wall.as_secs_f64(),
                peak,
                peak as f64 / (1024.0 * 1024.0),
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
            if segmented {
                assert_eq!(sidecars, live.len(), "one sidecar per live file");
            }
        }
    });
}
