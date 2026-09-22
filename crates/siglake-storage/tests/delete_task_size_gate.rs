//! A delete-task candidate is rewritten without ever holding the decoded file.
//!
//! THE DEFECT THESE GUARD (task #3028). `rewrite_delete_task_candidate` read
//! the whole compressed object, decoded every batch into a `MemTable`,
//! collected the survivors and `concat_batches`'d them — four decoded copies
//! of one data file live at once, plus the compressed bytes. Against a 256 MiB
//! cold-target file at the repo's 5x decode assumption that is several GiB on
//! a compactor packaged with `memory: 1Gi`
//! (`deploy/helm/siglake/values.yaml`). The 2026-08-28 query memory audit
//! that found it closed it as unreachable because delete-task execution was
//! default-off in three places;
//! #2951 turned all three on, so the first GDPR delete against a compacted
//! index reaches it.
//!
//! The fix is the same split `recluster_should_stream` already makes: above a
//! byte or row cap the rewrite runs on a streaming arm that decodes
//! `DELETE_REWRITE_BATCH_ROWS` rows at a time and writes survivors straight
//! through a rolling writer; at or below the caps it stays on the in-RAM arm,
//! which also builds the inline inverted-index footers and the whole-file raw
//! trigram bloom.
//!
//! Its own binary: the path discriminator is a process-wide metrics recorder,
//! and [`measure_peak_allocation_per_arm`] measures live allocation through a
//! global allocator. Both are process state, so the tests here take
//! [`SIZE_GATE_TESTS`] and run one at a time.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use chrono::{Duration as ChronoDuration, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use siglake_bloom::{RAW_TRIGRAM_BLOOM_KV_KEY, RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY};
use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_index::INVERTED_INDEX_KV_KEY;
use siglake_storage::iceberg::{
    DeleteTaskState, IcebergContext, IcebergTuning, GROUP_COUNTS_KV_KEY, MIN_ROW_GROUP_ROWS,
    TIME_BUCKETS_KV_KEY,
};

/// The `--test storage` binary's fixture clock, included rather than copied:
/// the fixtures below count files and rewrites per candidate too, so they take
/// their event timestamps from the same fixed UTC day.
#[path = "storage/fixture_clock.rs"]
mod fixture_clock;
use fixture_clock::fixture_base;

/// Live heap bytes, counted from process start, and their peak over a
/// measurement window.
///
/// `LIVE` is maintained unconditionally so that a window has a real starting
/// live-heap figure to measure against. Every test in this binary holds
/// [`SIZE_GATE_TESTS`] while it measures, so a window belongs to one arm of one
/// A/B.
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
    /// Live heap when the window opened.
    baseline: usize,
    /// The largest live heap seen inside the window.
    peak: usize,
    /// `peak - baseline`: what the measured work added over existing live heap.
    above_baseline: usize,
}

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
            baseline: self.baseline.max(0) as usize,
            peak: peak.max(0) as usize,
            above_baseline: (peak - self.baseline).max(0) as usize,
        }
    }
}

#[test]
fn heap_window_accounts_for_freeing_a_preexisting_allocation() {
    let (_guard, _snapshotter) = setup();
    let old_layout = Layout::from_size_align(1024 * 1024, 8).unwrap();
    let new_layout = Layout::from_size_align(2 * 1024 * 1024, 8).unwrap();

    // Use the global allocator directly so no unrelated allocation lands in
    // the window. The first block deliberately predates it.
    let old = unsafe { std::alloc::alloc(old_layout) };
    assert!(!old.is_null());
    let window = start_tracking();
    unsafe { std::alloc::dealloc(old, old_layout) };
    let new = unsafe { std::alloc::alloc(new_layout) };
    assert!(!new.is_null());
    let reading = window.finish();
    unsafe { std::alloc::dealloc(new, new_layout) };

    assert_eq!(reading.peak, reading.baseline + 1024 * 1024);
    assert_eq!(reading.above_baseline, 1024 * 1024);
}

/// Serialises this binary's tests: they share the metrics recorder and the
/// allocator counters above.
static SIZE_GATE_TESTS: Mutex<()> = Mutex::new(());

fn setup() -> (MutexGuard<'static, ()>, &'static Snapshotter) {
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();
    // Poisoning only means another test panicked; the recorder is still usable
    // and failing every later test would hide the original failure.
    let guard = SIZE_GATE_TESTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let snapshotter = SNAPSHOTTER.get_or_init(|| {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        recorder.install().expect("install debugging recorder");
        snapshotter
    });
    // `snapshot()` DRAINS, so clear whatever the previous test left behind and
    // let each assertion read the sweep it just ran.
    let _ = snapshotter.snapshot();
    (guard, snapshotter)
}

/// Runs one test with this binary's process state — the metrics recorder and
/// the allocator counters — to itself.
///
/// A `#[test]` with its own runtime rather than `#[tokio::test]`: the lock has
/// to span the whole sweep, and a std `MutexGuard` held across an `.await`
/// inside an async test is what `clippy::await_holding_lock` exists to catch.
fn serialized<F>(body: impl FnOnce(&'static Snapshotter) -> F)
where
    F: std::future::Future<Output = ()>,
{
    let (_guard, snapshotter) = setup();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime")
        .block_on(body(snapshotter));
}

/// How many candidate files the last sweep rewrote on `path`
/// (`"streaming"` or `"in_ram"`).
fn rewrites_on_path(snapshotter: &Snapshotter, path: &str) -> u64 {
    snapshotter
        .snapshot()
        .into_vec()
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "siglake_delete_task_rewrite_path_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "path" && label.value() == path)
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(count) => *count,
            _ => 0,
        })
        .sum()
}

fn logs_index(index_id: &str) -> IndexConfig {
    let mut config = IndexConfig::builtin_events();
    config.index_id = index_id.to_string();
    config
}

fn bloom_columns(config: &IndexConfig) -> Vec<String> {
    config
        .doc_mapping
        .tag_fields
        .iter()
        .filter_map(|name| {
            config
                .doc_mapping
                .field_mappings
                .iter()
                .find(|field| field.name == *name)
                .and_then(|field| match &field.field_type {
                    FieldType::Text { .. } | FieldType::Json => Some(name.clone()),
                    _ => None,
                })
        })
        .collect()
}

fn event(ts: chrono::DateTime<Utc>, host: &str, raw: &str, attributes: Option<&str>) -> Event {
    Event {
        timestamp: ts,
        host: host.to_string(),
        source: "/var/log/app.log".to_string(),
        sourcetype: "app:json".to_string(),
        index: "main".to_string(),
        raw: raw.to_string(),
        attributes: attributes.map(str::to_string),
    }
}

/// One `append_to_table` call ⇒ one data file, which is what makes the whole
/// fixture a single delete candidate.
async fn append_index_events(
    ice: &IcebergContext,
    config: &IndexConfig,
    events: &[Event],
) -> (usize, usize) {
    use arrow::array::Array;

    let batch = events_to_record_batch(events).unwrap();
    let decoded_bytes = batch.get_array_memory_size();
    let slice_bytes: usize = batch
        .columns()
        .iter()
        .map(|c| c.to_data().get_slice_memory_size().unwrap())
        .sum();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
    (decoded_bytes, slice_bytes)
}

async fn count_index_rows(ice: &IcebergContext, index_id: &str, where_sql: Option<&str>) -> i64 {
    let ctx = datafusion::prelude::SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .unwrap();
    let sql = match where_sql {
        Some(predicate) => format!("SELECT count(*) AS n FROM \"{index_id}\" WHERE {predicate}"),
        None => format!("SELECT count(*) AS n FROM \"{index_id}\""),
    };
    let batches = ctx
        .sql(sql.as_str())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn live_paths(ice: &IcebergContext, index_id: &str) -> Vec<String> {
    let mut paths: Vec<String> = ice
        .live_data_files(&ice.index_table_ident(index_id))
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.file_path().to_string())
        .collect();
    paths.sort();
    paths
}

/// Every Parquet object under the warehouse, committed or not — a refusal that
/// leaves a survivor fragment behind shows up here even though the commit
/// never referenced it.
fn parquet_objects(root: &Path) -> BTreeSet<PathBuf> {
    let mut found = BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                found.insert(path);
            }
        }
    }
    found
}

/// Send every candidate down the streaming arm regardless of its size.
fn force_streaming() -> IcebergTuning {
    IcebergTuning {
        delete_rewrite_inram_max_bytes: Some(0),
        delete_rewrite_inram_max_rows: Some(0),
        ..Default::default()
    }
}

/// Every Parquet footer key-value key on one committed file.
async fn footer_keys(table: &iceberg::table::Table, file: &iceberg::spec::DataFile) -> Vec<String> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let bytes = table
        .file_io()
        .new_input(file.file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|entries| entries.iter().map(|e| e.key.clone()).collect())
        .unwrap_or_default()
}

/// Whether the table's statistics files register an inverted-index blob for
/// this data file — the only shape the post-commit rebuild pass can produce.
fn puffin_indexes_file(table: &iceberg::table::Table, file: &iceberg::spec::DataFile) -> bool {
    table.metadata().statistics_iter().any(|stats| {
        stats.blob_metadata.iter().any(|blob| {
            blob.r#type == "siglake-inverted-v1"
                && blob
                    .properties
                    .get("data_file")
                    .is_some_and(|path| path == file.file_path())
        })
    })
}

/// The streaming arm must be the in-RAM arm's equal on semantics: the TRUE
/// rows go, the FALSE rows stay, and — the #2094 trap — so do the NULL-valued
/// nonmatches, which `NOT (p)` silently drops and `(p) IS NOT TRUE` keeps.
#[test]
fn the_streaming_arm_deletes_exactly_the_matching_rows() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let config = logs_index("logs");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_tuning(force_streaming());
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        append_index_events(
            &ice,
            &config,
            &[
                event(
                    now - ChronoDuration::minutes(4),
                    "web-01",
                    "true row",
                    Some(r#"{"tenant":"gone"}"#),
                ),
                event(
                    now - ChronoDuration::minutes(3),
                    "web-02",
                    "false row",
                    Some(r#"{"tenant":"stay"}"#),
                ),
                event(now - ChronoDuration::minutes(2), "web-03", "null row", None),
            ],
        )
        .await;

        let task = ice
            .create_delete_task("logs", r#"attributes = '{"tenant":"gone"}'"#, None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();

        assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
        assert_eq!(outcome.tasks_failed, 0, "{outcome:?}");
        assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
        assert_eq!(outcome.rows_deleted, 1, "{outcome:?}");
        let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
        assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");

        assert_eq!(count_index_rows(&ice, "logs", None).await, 2);
        assert_eq!(
            count_index_rows(&ice, "logs", Some(r#"attributes = '{"tenant":"stay"}'"#)).await,
            1,
            "the FALSE-valued nonmatch must survive the streamed rewrite"
        );
        assert_eq!(
            count_index_rows(&ice, "logs", Some("attributes IS NULL")).await,
            1,
            "the NULL-valued nonmatch must survive the streamed rewrite"
        );
        assert_eq!(
            rewrites_on_path(snapshotter, "streaming"),
            1,
            "the rewrite did not take the streaming arm"
        );
    });
}

/// THE BOUND. A candidate AT the row cap is rewritten in RAM; one row over it
/// streams. The byte cap is out of the way in both arms, so what is pinned
/// here is the row gate on its own — the gate that fires on a file whose
/// compressed size says nothing about what it decodes to.
#[test]
fn the_row_cap_is_the_boundary_between_the_two_arms() {
    serialized(|snapshotter| async move {
        const CAP: u64 = 8;

        for (rows, expected_path) in [(CAP, "in_ram"), (CAP + 1, "streaming")] {
            let tmp = tempfile::tempdir().unwrap();
            let warehouse = tmp.path().join("warehouse");
            let config = logs_index("logs");
            let ice = IcebergContext::open(&warehouse)
                .await
                .unwrap()
                .with_tuning(IcebergTuning {
                    delete_rewrite_inram_max_bytes: Some(u64::MAX),
                    delete_rewrite_inram_max_rows: Some(CAP),
                    ..Default::default()
                });
            ice.create_index(&config).await.unwrap();

            let now = fixture_base();
            let events: Vec<Event> = (0..rows)
                .map(|i| {
                    let host = if i == 0 { "victim" } else { "keep" };
                    event(
                        now - ChronoDuration::seconds(rows as i64 - i as i64),
                        host,
                        "row",
                        None,
                    )
                })
                .collect();
            append_index_events(&ice, &config, &events).await;
            let _ = snapshotter.snapshot();

            let outcome = ice.execute_delete_tasks("logs").await.unwrap();
            assert_eq!(outcome.tasks_completed, 0, "no task yet: {outcome:?}");
            ice.create_delete_task("logs", "host = 'victim'", None, None)
                .await
                .unwrap();
            let _ = snapshotter.snapshot();
            let outcome = ice.execute_delete_tasks("logs").await.unwrap();

            assert_eq!(outcome.tasks_completed, 1, "{rows} rows: {outcome:?}");
            assert_eq!(outcome.rows_deleted, 1, "{rows} rows: {outcome:?}");
            let snapshot = snapshotter.snapshot().into_vec();
            let taken = |path: &str| -> u64 {
                snapshot
                    .iter()
                    .filter(|(key, _, _, _)| {
                        key.key().name() == "siglake_delete_task_rewrite_path_total"
                            && key
                                .key()
                                .labels()
                                .any(|label| label.key() == "path" && label.value() == path)
                    })
                    .map(|(_, _, _, value)| match value {
                        DebugValue::Counter(count) => *count,
                        _ => 0,
                    })
                    .sum()
            };
            assert_eq!(
                taken(expected_path),
                1,
                "{rows} rows against a {CAP}-row cap must take the {expected_path} arm"
            );
            let other = if expected_path == "in_ram" {
                "streaming"
            } else {
                "in_ram"
            };
            assert_eq!(taken(other), 0, "{rows} rows also took the {other} arm");

            // Either arm, the same answer.
            assert_eq!(count_index_rows(&ice, "logs", None).await, rows as i64 - 1);
            assert_eq!(
                count_index_rows(&ice, "logs", Some("host = 'victim'")).await,
                0
            );
        }
    });
}

/// A predicate that matches the whole file deletes the file rather than
/// rewriting it, and never opens a survivor writer.
#[test]
fn a_streamed_whole_file_delete_writes_no_survivor_file() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let config = logs_index("logs");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_tuning(force_streaming());
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        append_index_events(
            &ice,
            &config,
            &[
                event(now - ChronoDuration::minutes(2), "victim", "one", None),
                event(now - ChronoDuration::minutes(1), "victim", "two", None),
            ],
        )
        .await;
        let before = parquet_objects(&warehouse);
        assert_eq!(before.len(), 1);

        ice.create_delete_task("logs", "host = 'victim'", None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();

        assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
        assert_eq!(outcome.rows_deleted, 2, "{outcome:?}");
        assert_eq!(rewrites_on_path(snapshotter, "streaming"), 1);
        assert!(
            live_paths(&ice, "logs").await.is_empty(),
            "the emptied file must be dropped, not rewritten"
        );
        assert_eq!(count_index_rows(&ice, "logs", None).await, 0);
        assert_eq!(
            parquet_objects(&warehouse),
            before,
            "a whole-file delete wrote a survivor object it then had no rows for"
        );
    });
}

/// A refusal on the streaming arm commits no partial deletion. The predicate
/// is refused at plan time — the shape `delete_task_read_only_predicate`
/// covers for the in-RAM arm — and the requirement is the same here: the task
/// fails, every row stays, and no Parquet object appears.
#[test]
fn a_refused_predicate_on_the_streaming_arm_deletes_nothing() {
    serialized(|_snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let config = logs_index("logs");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_tuning(force_streaming());
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        append_index_events(
            &ice,
            &config,
            &[
                event(now - ChronoDuration::minutes(2), "victim", "one", None),
                event(now - ChronoDuration::minutes(1), "keep", "two", None),
            ],
        )
        .await;
        let before = parquet_objects(&warehouse);

        // `no_such_column` parses and plans no further: the count pass fails
        // before a single batch is decoded.
        let task = ice
            .create_delete_task("logs", "no_such_column = 'victim'", None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();

        assert_eq!(outcome.tasks_failed, 1, "{outcome:?}");
        assert_eq!(outcome.tasks_completed, 0, "{outcome:?}");
        assert_eq!(outcome.rows_deleted, 0, "{outcome:?}");
        let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();
        assert_eq!(stored.state, DeleteTaskState::Failed, "{stored:?}");
        assert!(stored.error.is_some(), "{stored:?}");
        assert_eq!(count_index_rows(&ice, "logs", None).await, 2);
        assert_eq!(
            parquet_objects(&warehouse),
            before,
            "the refused task left a Parquet object behind"
        );
    });
}

/// A refusal that lands AFTER survivors have been written commits no partial
/// deletion.
///
/// The streaming arm counts the matches in one pass and writes the survivors
/// in a second, so a predicate that answers differently on the two passes
/// fails the conservation guard with a rolling writer already half full. The
/// arm bails before `close()`, so nothing is offered to the commit: the task
/// goes `failed`, every row is still there, and the live file set is the one
/// the sweep started from. Whatever the writer uploaded is unreferenced, which
/// is orphan GC's job — the same disposition the incarnation fence has.
///
/// `random() < 0.5` is the inducer. Over 4096 rows the two passes agree about
/// 2% of the time, so the assertions below accept either outcome and pin the
/// property that matters to both: no row is deleted that the sweep did not
/// account for.
#[test]
fn a_guard_failure_mid_write_commits_no_partial_deletion() {
    serialized(|_snapshotter| async move {
        const ROWS: usize = 4096;
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let config = logs_index("logs");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_tuning(force_streaming());
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        let events: Vec<Event> = (0..ROWS)
            .map(|i| {
                event(
                    now - ChronoDuration::seconds((ROWS - i) as i64),
                    "web-01",
                    "row",
                    None,
                )
            })
            .collect();
        append_index_events(&ice, &config, &events).await;
        let files_before = live_paths(&ice, "logs").await;

        let task = ice
            .create_delete_task("logs", "random() < 0.5", None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();
        let stored = ice.get_delete_task(task.task_id).await.unwrap().unwrap();

        if stored.state == DeleteTaskState::Failed {
            assert_eq!(outcome.rows_deleted, 0, "{outcome:?}");
            assert_eq!(
                count_index_rows(&ice, "logs", None).await,
                ROWS as i64,
                "a task that failed its conservation guard still deleted rows"
            );
            assert_eq!(
                live_paths(&ice, "logs").await,
                files_before,
                "a task that failed its conservation guard still committed a rewrite"
            );
            let error = stored.error.clone().unwrap_or_default();
            assert!(
                error.contains("row-count mismatch"),
                "the failure must name the guard that fired: {error}"
            );
        } else {
            // The 2% coincidence: both passes agreed, so the rewrite is
            // legitimate and must conserve every row it did not delete.
            assert_eq!(stored.state, DeleteTaskState::Done, "{stored:?}");
            assert_eq!(
                count_index_rows(&ice, "logs", None).await + outcome.rows_deleted as i64,
                ROWS as i64,
                "the rewrite lost rows it never claimed to delete"
            );
        }
    });
}

/// What one sweep of [`sweep_peak`] measured.
#[derive(Debug, Clone, Copy)]
struct SweepPeak {
    /// Baseline, peak live heap, and peak growth during the delete sweep.
    heap: HeapReading,
    /// `get_array_memory_size` of the appended batch — the decoded size of the
    /// one candidate the sweep rewrites.
    decoded: usize,
    /// The same batch priced the way the WRITER prices its sample: the extent
    /// the rows span, without the slack in the buffers holding them. Runs
    /// ~2x under [`Self::decoded`] here, which is the difference between a byte
    /// target that splits the output's row groups and one that does not.
    decoded_slice: usize,
    /// The committed candidate's compressed size on the store.
    file_bytes: u64,
    /// Rows the predicate did NOT match, out of [`Self::rows`] — the rows the
    /// rewrite has to write back out.
    survivors: usize,
    rows: usize,
    /// Rows in the rewrite output's first row group, and how many groups it
    /// wrote: what the byte target resolved to for this fixture's rows. The
    /// open row group is what the arm buffers decoded, so this is the number
    /// [`HeapReading::above_baseline`] is supposed to follow.
    first_row_group: usize,
    row_groups: usize,
}

impl SweepPeak {
    /// The decoded bytes of the survivors alone: what a rewrite that holds its
    /// output, and only its output, would be holding.
    fn survivor_decoded(&self) -> usize {
        self.decoded / self.rows * self.survivors
    }
}

/// Peak live heap across one delete sweep over a fixture of `rows` rows of
/// `raw_len`-byte raw text, with the candidate's decoded and compressed sizes.
/// One row in `survivor_in_n` survives the predicate; the rest are deleted.
///
/// ONE CANDIDATE AT EVERY SIZE, asserted below. Rows are one MILLISECOND apart,
/// so even the largest fixture here spans a minute and the `day(timestamp)`
/// partition delivers it as a single data file. The second-apart spacing this
/// replaces put 64 Ki rows across 18 hours and so across two day partitions
/// (#3999), which measured the split rather than the arm: half a candidate
/// costs half a peak, so the arm looked flatter than it is (#4703).
async fn sweep_peak(
    rows: usize,
    raw_len: usize,
    survivor_in_n: usize,
    tuning: IcebergTuning,
) -> SweepPeak {
    let raw: String = "x".repeat(raw_len);
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let config = logs_index("logs");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_tuning(tuning);
    ice.create_index(&config).await.unwrap();

    let now = fixture_base();
    let events: Vec<Event> = (0..rows)
        .map(|i| {
            let host = if i % survivor_in_n == 0 {
                "keep"
            } else {
                "victim"
            };
            event(
                now - ChronoDuration::milliseconds((rows - i) as i64),
                host,
                raw.as_str(),
                None,
            )
        })
        .collect();
    let (decoded, decoded_slice) = append_index_events(&ice, &config, &events).await;
    drop(events);

    let files = ice
        .live_data_files(&ice.index_table_ident("logs"))
        .await
        .unwrap();
    assert_eq!(
        files.len(),
        1,
        "{rows} rows must arrive as ONE delete candidate, or the peak below is \
         the peak of a fraction of the file"
    );
    let file_bytes = files[0].file_size_in_bytes();
    drop(files);

    ice.create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    let heap_window = start_tracking();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    let heap = heap_window.finish();

    let survivors = rows.div_ceil(survivor_in_n);
    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    assert_eq!(
        outcome.rows_deleted,
        (rows - survivors) as u64,
        "{outcome:?}"
    );
    assert_eq!(count_index_rows(&ice, "logs", None).await, survivors as i64);
    let groups = index_output_row_groups(&ice, "logs").await;
    SweepPeak {
        heap,
        decoded,
        decoded_slice,
        file_bytes,
        survivors,
        rows,
        first_row_group: groups[0],
        row_groups: groups.len(),
    }
}

/// THE DEFECT, MEASURED — by hand, because each sweep below writes and
/// rewrites a multi-megabyte fixture in a debug build (~40 s apiece, seven of
/// them):
///
/// ```text
/// cargo test -p siglake-storage --test delete_task_size_gate \
///     measure_peak_allocation_per_arm -- --ignored --nocapture
/// ```
///
/// What it records is peak live heap above the heap already live when each
/// delete sweep starts, per arm, over ONE candidate file — [`sweep_peak`]
/// asserts the fixture is one, which is the correction #4703 made. Corrected
/// 2026-09-22 after replacing the reset-to-zero counter with a continuous live
/// counter; debug build, 1 KiB of raw text per row (~1,200 B decoded), half the
/// rows deleted:
///
/// ```text
/// rows    file     decoded   survivors   in-RAM above baseline   streaming above baseline
/// 16 Ki   113 KB   19.7 MB     9.8 MB                 53.9 MB                      27.1 MB
/// 32 Ki   222 KB   39.3 MB    19.7 MB                 94.4 MB                      36.4 MB
/// 64 Ki   443 KB   78.7 MB    39.3 MB                184.4 MB                      55.0 MB
/// 64 Ki   443 KB   78.7 MB     9.8 MB                       —                      27.8 MB   (⅛ survive)
/// ```
///
/// WHAT THE STREAMING ARM HOLDS, per decoded byte: **0.94 of its OUTPUT, plus
/// a fixed ~17.8 MB**, and nothing per decoded byte of its input. The fit is
/// `peak above baseline ≈ 0.94 × survivor_decoded + 17.8 MB`. The fourth row
/// is the one that separates input from output:
/// it rewrites the SAME 78.7 MB candidate as the third but keeps an eighth of
/// the rows, and lands at 27.8 MB, within 2.7% of the 16 Ki sweep's 27.1 MB
/// over the same 9.8 MB of survivors off a candidate a quarter the size. Per
/// decoded byte of the candidate the streaming arm therefore reads at
/// 1.38 / 0.93 / 0.70 / 0.35 — a ratio that says nothing on its own, which is
/// why it is not what the assertions use.
///
/// Roughly one output's decoded bytes is one whole decoded copy, and the code
/// says where: the
/// fork's `ParquetWriter` buffers the open row group as decoded Arrow batches
/// in `pending` whenever a row-group bloom column is set, which `with_footers`
/// always sets here. The answer to #4703's question is yes: the rolling writer
/// buffers a row group proportional to the output, and every fixture above is
/// one row group. When these were taken that row group was a flat 1,048,576
/// rows — `build_merge_output_writer` asked for its size with no sample batch,
/// and that branch returns the fallback without reading the byte target it was
/// passed (#4754). The fix leaves this table standing: none of these fixtures
/// reaches even the 128 Ki-row floor, so no target could have split their
/// output. [`measure_peak_against_row_group_target`] is where the target is
/// measured, on a fixture large enough for it to bite.
///
/// WHAT THIS DOES NOT ESTABLISH. Not that a 256 MiB cold-target candidate fits
/// a compactor packaged at `memory: 1Gi` (`deploy/helm/siglake/values.yaml`).
/// The model above says the opposite is the thing to check — 1 Mi rows at this
/// fixture's 1,200 decoded bytes per row is ~1.2 GB of buffered survivors
/// before the cap engages at all — but these numbers cannot carry that
/// extrapolation: they are peak growth above each sweep's baseline, taken in a
/// debug build on fixtures three orders of magnitude smaller, over uniform
/// `xxx…` raw text that compresses 174:1 and so has no representative
/// relationship between file bytes and decoded bytes. What the gate below pins
/// is the shape.
///
/// HISTORICAL COUNTER READING. The 2026-09-16 run printed 54.2 / 95.1 /
/// 187.8 MB for the three in-RAM arms, 27.4 / 36.9 / 55.7 MB for their
/// streaming controls and 28.1 MB for the narrow-survivor arm. Those figures
/// came from a counter reset to zero at the start of each sweep: frees of heap
/// allocated before the sweep pulled it down, so they were lower bounds on
/// growth rather than live heap or exact growth above a baseline. They remain
/// the source of the historical 0.96 × survivors + 18.0 MB fit.
///
/// SUPERSEDED. #3999's reading, taken the same day, recorded 125.4 MB in-RAM
/// and 41.6 MB streaming at 64 Ki, and read the arm as near-flat (13% for a
/// doubled fixture). Its rows were one second apart, so 64 Ki spanned 18 hours
/// and the `day(timestamp)` partition delivered it as TWO candidates: half a
/// candidate, half a peak. Before that, rows placed off `Utc::now()` made the
/// hour of the run decide which sizes split (#3999). Neither reading was of
/// what it claimed to measure.
#[ignore]
#[test]
fn measure_peak_allocation_per_arm() {
    serialized(|_snapshotter| async move {
        let mut streaming = Vec::new();
        let mut in_ram = Vec::new();
        for rows in [16 * 1024usize, 32 * 1024, 64 * 1024] {
            let ram = sweep_peak(rows, 1024, 2, IcebergTuning::default()).await;
            let stream = sweep_peak(rows, 1024, 2, force_streaming()).await;
            println!(
                "rows={rows} file_bytes={} decoded={} survivor_decoded={} \
                 in_ram_baseline={} in_ram_peak_live={} in_ram_peak_above_baseline={} \
                 streaming_baseline={} streaming_peak_live={} \
                 streaming_peak_above_baseline={} streaming_per_decoded={:.2} \
                 streaming_per_survivor={:.2}",
                stream.file_bytes,
                stream.decoded,
                stream.survivor_decoded(),
                ram.heap.baseline,
                ram.heap.peak,
                ram.heap.above_baseline,
                stream.heap.baseline,
                stream.heap.peak,
                stream.heap.above_baseline,
                stream.heap.above_baseline as f64 / stream.decoded as f64,
                stream.heap.above_baseline as f64 / stream.survivor_decoded() as f64,
            );
            streaming.push(stream);
            in_ram.push(ram);
        }

        // THE SAME CANDIDATE, AN EIGHTH OF THE SURVIVORS. If the streaming
        // arm's growth term is the output it buffers rather than the input it
        // reads, this sweep reads the largest candidate above and peaks near
        // the smallest.
        let narrow = sweep_peak(64 * 1024, 1024, 8, force_streaming()).await;
        println!(
            "rows={} survivors={} decoded={} survivor_decoded={} streaming_baseline={} \
             streaming_peak_live={} streaming_peak_above_baseline={} \
             streaming_per_decoded={:.2} streaming_per_survivor={:.2}",
            narrow.rows,
            narrow.survivors,
            narrow.decoded,
            narrow.survivor_decoded(),
            narrow.heap.baseline,
            narrow.heap.peak,
            narrow.heap.above_baseline,
            narrow.heap.above_baseline as f64 / narrow.decoded as f64,
            narrow.heap.above_baseline as f64 / narrow.survivor_decoded() as f64,
        );

        // WHAT THE GATE EXISTS FOR, at every size: under 0.6 of the arm it
        // replaced, over the same candidate. Measured at 0.50 / 0.39 / 0.30 —
        // the margin is widest where it matters, on the largest candidate.
        for (stream, ram) in streaming.iter().zip(in_ram.iter()) {
            assert!(
                stream.heap.above_baseline * 5 < ram.heap.above_baseline * 3,
                "the streaming arm peaked at {} B against the in-RAM arm's {} B over the \
                 same {}-byte candidate; the gate is not buying what it exists for",
                stream.heap.above_baseline,
                ram.heap.above_baseline,
                stream.decoded,
            );
        }

        // THE GROWTH TERM IS THE OUTPUT, NOT THE INPUT. Doubling the candidate
        // at a fixed survivor fraction costs a fixed share of the added decoded
        // bytes; measured at 0.47 (see the table above), so 0.7 leaves room for
        // allocator noise while still failing if the arm starts holding the
        // whole input. The in-RAM arm fails this by construction.
        for pair in streaming.windows(2) {
            let (small, large) = (pair[0], pair[1]);
            let marginal_peak = large.heap.above_baseline - small.heap.above_baseline;
            let marginal_decoded = large.decoded - small.decoded;
            assert!(
                marginal_peak * 10 < marginal_decoded * 7,
                "the streaming arm took {marginal_peak} B of peak for {marginal_decoded} \
                 added decoded bytes ({} B over {} at {} rows, {} B over {} at {}); above \
                 half the input it is holding more than the survivors it writes",
                small.heap.above_baseline,
                small.decoded,
                small.rows,
                large.heap.above_baseline,
                large.decoded,
                large.rows,
            );
        }

        // ... and that share is the SURVIVORS. `narrow` and the 16 Ki sweep
        // write the same 9.8 MB of survivors off candidates 4x apart in size,
        // and must therefore peak at the same place: measured 2.7% apart, so
        // 15% is noise margin, not slack. This is the assertion that fails if
        // the arm ever starts holding the input.
        let same_output = streaming[0];
        assert_eq!(
            narrow.survivor_decoded(),
            same_output.survivor_decoded(),
            "the two sweeps must write the same survivor bytes to be comparable"
        );
        let (lo, hi) = (
            narrow
                .heap
                .above_baseline
                .min(same_output.heap.above_baseline),
            narrow
                .heap
                .above_baseline
                .max(same_output.heap.above_baseline),
        );
        assert!(
            hi * 100 < lo * 115,
            "the same {} survivor bytes peaked at {} B off a {}-byte candidate and {} B \
             off a {}-byte one; the arm is holding the input, not the output",
            narrow.survivor_decoded(),
            same_output.heap.above_baseline,
            same_output.decoded,
            narrow.heap.above_baseline,
            narrow.decoded,
        );
    });
}

/// THE KNOB, MEASURED (#4754) — by hand, like the sweep above and for the same
/// reason; this fixture is eight times the largest one there:
///
/// ```text
/// cargo test -p siglake-storage --test delete_task_size_gate \
///     measure_peak_against_row_group_target -- --ignored --nocapture
/// ```
///
/// The peak the sweep above measured is the open row group, and the row group
/// is now `target_row_group_bytes / observed row size` clamped to
/// [`MIN_ROW_GROUP_ROWS`]. So the target moves the peak — but ONLY on a rewrite
/// whose survivors exceed the 128 Ki-row floor. Every fixture in
/// [`measure_peak_allocation_per_arm`] writes at most 32 Ki survivors, where
/// the floor alone decides the row group and no target can lower it; that is
/// why the numbers there are unchanged by this fix and why this measurement
/// needs 256 Ki survivors of its own.
///
/// Corrected 2026-09-22 with the continuous live counter, debug build, 512 Ki
/// rows of 128 B raw text, half deleted — 262,144 survivors, 113.2 MB decoded by
/// `get_array_memory_size`, 201 B/row by extent:
///
/// ```text
/// target             row groups   peak above baseline
/// default (256 MB)   1 x 262,144                 76.0 MB
/// 26.3 MB            2 x 131,727                 54.1 MB   ratio 0.71
/// ```
///
/// Halving the row group took 21.9 MB off the peak, against a fixed term the arm
/// pays either way: ~168 B per row no longer buffered. The target bounds rows,
/// not resident bytes; the buffered batches keep their backing buffers.
///
/// HISTORICAL COUNTER READING. The 2026-09-16 run reported 80.4 MB and 56.5 MB
/// (ratio 0.70), using the reset-to-zero counter described above. Those values
/// and the old 23.8 MB / ~300 B-per-row interpretation are retained here as the
/// record that led to this gate, but they were lower bounds rather than exact
/// growth above a baseline.
///
/// The first reading of this measurement sized its target off
/// `get_array_memory_size` (432 B/row here, twice the extent), asked for a row
/// group larger than the whole output, and reported ratio 1.00 over two arms
/// that emitted one row group each. The row-group columns are in the print for
/// that reason.
#[ignore]
#[test]
fn measure_peak_against_row_group_target() {
    serialized(|_snapshotter| async move {
        // 512 Ki rows, half deleted: 256 Ki survivors, twice the floor, so a
        // target sized for the floor halves the open row group.
        const ROWS: usize = 512 * 1024;
        let whole = sweep_peak(ROWS, 128, 2, force_streaming()).await;
        // Priced off `decoded_slice`, not `decoded`: the writer measures the
        // extent its sample batch spans, and the ~2x slack in
        // `get_array_memory_size` is enough to ask for a row group larger than
        // the whole output and measure nothing (the first reading here did).
        let per_row = whole.decoded_slice / whole.rows;
        let floor_target = per_row * MIN_ROW_GROUP_ROWS;
        let floored = sweep_peak(
            ROWS,
            128,
            2,
            IcebergTuning {
                target_row_group_bytes: Some(floor_target),
                ..force_streaming()
            },
        )
        .await;
        println!(
            "survivors={} survivor_decoded={} per_row={per_row} \
             whole_baseline={} whole_peak_live={} whole_peak_above_baseline={} \
             whole_groups={}x{} floor_target={floor_target} \
             floored_baseline={} floored_peak_live={} floored_peak_above_baseline={} \
             floored_groups={}x{} ratio={:.2}",
            whole.survivors,
            whole.survivor_decoded(),
            whole.heap.baseline,
            whole.heap.peak,
            whole.heap.above_baseline,
            whole.row_groups,
            whole.first_row_group,
            floored.heap.baseline,
            floored.heap.peak,
            floored.heap.above_baseline,
            floored.row_groups,
            floored.first_row_group,
            floored.heap.above_baseline as f64 / whole.heap.above_baseline as f64,
        );
        // Half the row group, so about half of the growth term and all of the
        // fixed ~18 MB. 0.85 fails a target that never reaches the writer
        // (identical arms, ratio ~1.0) while leaving room for the fixed term.
        assert!(
            floored.heap.above_baseline * 100 < whole.heap.above_baseline * 85,
            "a target sized for the {MIN_ROW_GROUP_ROWS}-row floor peaked at {} B \
             against the default target's {} B over the same {} survivor bytes; the \
             target is not reaching the survivor writer",
            floored.heap.above_baseline,
            whole.heap.above_baseline,
            whole.survivor_decoded(),
        );
    });
}

/// Row groups of the one file the index table holds, in file order.
async fn index_output_row_groups(ice: &IcebergContext, index_id: &str) -> Vec<usize> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let files = ice
        .live_data_files(&ice.index_table_ident(index_id))
        .await
        .unwrap();
    assert_eq!(files.len(), 1, "expected ONE rewritten file, got {files:?}");
    let path = files[0]
        .file_path()
        .trim_start_matches("file://")
        .to_string();
    let bytes = std::fs::read(&path).unwrap();
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .collect()
}

/// One streamed delete sweep over a single candidate, at `target_row_group_bytes`.
/// Returns the survivor output's row groups.
async fn delete_arm_row_groups(target_row_group_bytes: Option<usize>) -> Vec<usize> {
    let tmp = tempfile::tempdir().unwrap();
    let config = logs_index("logs");
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap()
        .with_tuning(IcebergTuning {
            target_row_group_bytes,
            ..force_streaming()
        });
    ice.create_index(&config).await.unwrap();

    let now = fixture_base();
    let events: Vec<Event> = (0..DELETE_ROW_GROUP_ROWS)
        .map(|i| {
            let host = if i % 10 == 0 { "victim" } else { "keep" };
            event(
                now - ChronoDuration::milliseconds((DELETE_ROW_GROUP_ROWS - i) as i64),
                host,
                "GET /api/v1/logs 200 in 13ms",
                None,
            )
        })
        .collect();
    append_index_events(&ice, &config, &events).await;
    drop(events);

    ice.create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
    let survivors = DELETE_ROW_GROUP_ROWS - DELETE_ROW_GROUP_ROWS / 10;
    assert_eq!(
        count_index_rows(&ice, "logs", None).await,
        survivors as i64,
        "the survivors are what the row groups below have to hold"
    );
    let groups = index_output_row_groups(&ice, "logs").await;
    assert_eq!(groups.iter().sum::<usize>(), survivors, "rows conserved");
    groups
}

/// Rows in the delete fixture below. Nine in ten survive, so the survivors
/// (144,000) clear `MIN_ROW_GROUP_ROWS` — the smallest fixture that can show a
/// row-group size the byte target decided rather than the clamp.
const DELETE_ROW_GROUP_ROWS: usize = 160_000;

/// THE SURVIVOR WRITER READS THE BYTE TARGET (task #4754). The streaming arm
/// writes through `build_merge_output_writer`, which asked for its row-group
/// size with no sample batch — and that branch returns a flat 1,048,576 rows
/// without reading the target at all. Since the writer is built on its first
/// survivor batch, a 1-byte target clamps the row group to `MIN_ROW_GROUP_ROWS`
/// and the default target (256 MB against ~200 B rows here) takes the whole
/// output in one. Before the fix both arms emitted a single 144,000-row group,
/// which is what makes this an A/B and not a shape assertion.
///
/// That the row group is what the arm HOLDS is measured separately, in
/// [`measure_peak_allocation_per_arm`]: the open row group is buffered decoded
/// whenever a row-group bloom column is set, which this writer always sets.
#[test]
fn a_streamed_delete_rewrite_sizes_its_row_group_from_the_byte_target() {
    serialized(|_snapshotter| async move {
        let clamped = delete_arm_row_groups(Some(1)).await;
        println!("clamped={clamped:?}");
        assert_eq!(
            clamped.first().copied(),
            Some(MIN_ROW_GROUP_ROWS),
            "a 1-byte target must clamp the survivor row group to the floor, got \
             {clamped:?}"
        );
        assert!(
            clamped.len() > 1,
            "144,000 survivors at a 128 Ki-row floor need a second row group, got \
             {clamped:?}"
        );

        let default_target = delete_arm_row_groups(None).await;
        println!("default={default_target:?}");
        assert_eq!(
            default_target.len(),
            1,
            "the default 256 MB target holds these survivors in one row group, got \
             {default_target:?}"
        );
    });
}

/// WHAT THE README PROMISES (task #3161). The streaming arm writes through
/// `build_merge_output_writer`, which cannot build the footer inverted index
/// or the whole-file raw trigram bloom: both are computed from the whole
/// decoded batch, the thing this arm exists not to hold. What it does write is
/// the group-count, time-bucket and raw row-group-bloom footers, and the
/// post-commit rebuild pass — opted into here, off in a shipped build since
/// #4162 — adds a Puffin inverted-index sidecar, never a replacement inline
/// footer and never the whole-file bloom. The consequence is pruning, not
/// correctness, which is the half of the README's claim the query below pins.
#[test]
fn a_streamed_delete_rewrite_output_carries_no_inline_index() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let config = logs_index("logs");
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_inverted_index(true)
            .with_table_cache_ttl(std::time::Duration::ZERO)
            .with_tuning(IcebergTuning {
                index_rebuild: Some(true),
                ..force_streaming()
            });
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        append_index_events(
            &ice,
            &config,
            &[
                event(
                    now - ChronoDuration::minutes(3),
                    "web-01",
                    "database timeout for tenant gone",
                    Some(r#"{"tenant":"gone"}"#),
                ),
                event(
                    now - ChronoDuration::minutes(2),
                    "web-02",
                    "database retry for tenant stay",
                    Some(r#"{"tenant":"stay"}"#),
                ),
                event(
                    now - ChronoDuration::minutes(1),
                    "web-03",
                    "healthy",
                    Some(r#"{"tenant":"stay"}"#),
                ),
            ],
        )
        .await;

        let ident = ice.index_table_ident("logs");
        let input = ice.live_data_files(&ident).await.unwrap();
        assert_eq!(input.len(), 1, "the fixture must be one delete candidate");
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let input_keys = footer_keys(&table, &input[0]).await;
        for key in [INVERTED_INDEX_KV_KEY, RAW_TRIGRAM_BLOOM_KV_KEY] {
            assert!(
                input_keys.iter().any(|k| k == key),
                "the flushed input file must carry {key} for the comparison to mean anything: \
                 {input_keys:?}"
            );
        }

        ice.create_delete_task("logs", r#"attributes = '{"tenant":"gone"}'"#, None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();
        assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
        assert_eq!(
            rewrites_on_path(snapshotter, "streaming"),
            1,
            "the rewrite did not take the streaming arm"
        );

        let after = ice.live_data_files(&ident).await.unwrap();
        assert_eq!(after.len(), 1, "{after:?}");
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let keys = footer_keys(&table, &after[0]).await;
        for key in [INVERTED_INDEX_KV_KEY, RAW_TRIGRAM_BLOOM_KV_KEY] {
            assert!(
                !keys.iter().any(|k| k == key),
                "streamed delete output cannot carry {key}: {keys:?}"
            );
        }
        for key in [
            GROUP_COUNTS_KV_KEY,
            TIME_BUCKETS_KV_KEY,
            RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY,
        ] {
            assert!(
                keys.iter().any(|k| k == key),
                "streamed delete output must keep {key}: {keys:?}"
            );
        }
        assert!(
            puffin_indexes_file(&table, &after[0]),
            "the opted-in post-commit rebuild must register a Puffin inverted index for the \
             rewritten file"
        );

        // Pruning, not correctness: whatever the output carries, the rows are
        // exact — the deleted tenant's row is gone and the survivor that only
        // an index or a scan can find is still found.
        assert_eq!(count_index_rows(&ice, "logs", None).await, 2);
        assert_eq!(
            count_index_rows(&ice, "logs", Some("raw LIKE '%database%'")).await,
            1,
            "the survivor must still match a substring predicate"
        );
    });
}

/// The other half of the README's claim: with the rebuild opted out, nothing
/// puts an index on a streamed delete output at all, and the query path still
/// answers exactly — by scanning.
#[test]
fn a_streamed_delete_rewrite_with_rebuild_off_carries_no_index_at_all() {
    serialized(|snapshotter| async move {
        let tmp = tempfile::tempdir().unwrap();
        let config = logs_index("logs");
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_inverted_index(true)
            .with_table_cache_ttl(std::time::Duration::ZERO)
            .with_tuning(IcebergTuning {
                index_rebuild: Some(false),
                ..force_streaming()
            });
        ice.create_index(&config).await.unwrap();

        let now = fixture_base();
        append_index_events(
            &ice,
            &config,
            &[
                event(
                    now - ChronoDuration::minutes(2),
                    "web-01",
                    "database timeout for tenant gone",
                    Some(r#"{"tenant":"gone"}"#),
                ),
                event(
                    now - ChronoDuration::minutes(1),
                    "web-02",
                    "database retry for tenant stay",
                    Some(r#"{"tenant":"stay"}"#),
                ),
            ],
        )
        .await;

        ice.create_delete_task("logs", r#"attributes = '{"tenant":"gone"}'"#, None, None)
            .await
            .unwrap();
        let outcome = ice.execute_delete_tasks("logs").await.unwrap();
        assert_eq!(outcome.files_rewritten, 1, "{outcome:?}");
        assert_eq!(rewrites_on_path(snapshotter, "streaming"), 1);

        let ident = ice.index_table_ident("logs");
        let after = ice.live_data_files(&ident).await.unwrap();
        let table = ice.catalog().load_table(&ident).await.unwrap();
        for file in &after {
            let keys = footer_keys(&table, file).await;
            assert!(!keys.iter().any(|k| k == INVERTED_INDEX_KV_KEY), "{keys:?}");
            assert!(
                !puffin_indexes_file(&table, file),
                "the opt-out must leave the rewritten file with no inverted index of either shape"
            );
        }

        assert_eq!(count_index_rows(&ice, "logs", None).await, 1);
        assert_eq!(
            count_index_rows(&ice, "logs", Some("raw LIKE '%database%'")).await,
            1,
            "an unindexed file is scanned, and the scan is exact"
        );
    });
}
