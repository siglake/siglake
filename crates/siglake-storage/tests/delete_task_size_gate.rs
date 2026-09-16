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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

use chrono::{Duration as ChronoDuration, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

use siglake_bloom::{RAW_TRIGRAM_BLOOM_KV_KEY, RAW_TRIGRAM_ROWGROUP_BLOOM_KV_KEY};
use siglake_core::index_config::{FieldType, IndexConfig};
use siglake_core::{events_to_record_batch, Event};
use siglake_index::INVERTED_INDEX_KV_KEY;
use siglake_storage::iceberg::{
    DeleteTaskState, IcebergContext, IcebergTuning, GROUP_COUNTS_KV_KEY, TIME_BUCKETS_KV_KEY,
};

/// The `--test storage` binary's fixture clock, included rather than copied:
/// the fixtures below count files and rewrites per candidate too, so they take
/// their event timestamps from the same fixed UTC day.
#[path = "storage/fixture_clock.rs"]
mod fixture_clock;
use fixture_clock::fixture_base;

/// Peak live heap bytes between [`start_tracking`] and [`peak_tracked`].
///
/// A test-only wrapper around the system allocator. Tracking is off unless a
/// test turns it on, and every test in this binary holds [`SIZE_GATE_TESTS`]
/// while it does, so the number belongs to one arm of one A/B.
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
            // Saturating, not wrapping: tracking starts mid-process, so some
            // of what is freed here was allocated before LIVE existed. A
            // wrapping subtraction would make PEAK meaningless.
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
) -> usize {
    let batch = events_to_record_batch(events).unwrap();
    let decoded_bytes = batch.get_array_memory_size();
    let blooms = bloom_columns(config);
    let bloom_refs: Vec<&str> = blooms.iter().map(String::as_str).collect();
    ice.append_to_table(&ice.index_table_ident(&config.index_id), batch, &bloom_refs)
        .await
        .unwrap();
    decoded_bytes
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

/// Peak live heap across one delete sweep over a fixture of `rows` rows of
/// `raw_len`-byte raw text, plus the decoded size of that fixture.
///
/// One row per second, so the 64 Ki fixture spans 18 hours and the
/// `day(timestamp)` partition splits it into two candidate files, while 16 Ki
/// and 32 Ki fit inside one. Off `Utc::now()` which of the three split depended
/// on the hour the measurement ran; off [`fixture_base`] the split is the same
/// on every run. [`measure_peak_allocation_per_arm`] records what that costs
/// the reading.
async fn sweep_peak(rows: usize, raw_len: usize, tuning: IcebergTuning) -> (usize, usize) {
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
            let host = if i % 2 == 0 { "victim" } else { "keep" };
            event(
                now - ChronoDuration::seconds((rows - i) as i64),
                host,
                raw.as_str(),
                None,
            )
        })
        .collect();
    let decoded = append_index_events(&ice, &config, &events).await;
    drop(events);
    ice.create_delete_task("logs", "host = 'victim'", None, None)
        .await
        .unwrap();

    start_tracking();
    let outcome = ice.execute_delete_tasks("logs").await.unwrap();
    let peak = peak_tracked();

    assert_eq!(outcome.tasks_completed, 1, "{outcome:?}");
    assert_eq!(outcome.rows_deleted, rows as u64 / 2, "{outcome:?}");
    assert_eq!(count_index_rows(&ice, "logs", None).await, rows as i64 / 2);
    (peak, decoded)
}

/// THE DEFECT, MEASURED — by hand, because each sweep below writes and
/// rewrites a multi-megabyte fixture in a debug build (~40 s apiece):
///
/// ```text
/// cargo test -p siglake-storage --test delete_task_size_gate \
///     measure_peak_allocation_per_arm -- --ignored --nocapture
/// ```
///
/// What it records is net heap growth across one delete sweep, per arm, over
/// the same fixture. Measured 2026-09-16 (debug build, 1 KiB of raw text per
/// row, half the rows deleted), off the fixed [`fixture_base`] and so
/// repeatable, which the 2026-09-11 numbers it replaces were not — those were
/// taken off `Utc::now()`, where the hour of the run decided how the day
/// partition split the fixture:
///
/// ```text
/// rows    decoded    in-RAM peak   streaming peak
/// 16 Ki   19.7 MB    54.2 MB       27.4 MB
/// 32 Ki   39.3 MB    95.1 MB       36.9 MB
/// 64 Ki   78.7 MB   125.4 MB       41.6 MB
/// ```
///
/// The shape is the point, not the absolute bytes (freeing memory allocated
/// before the sweep started biases every number down): the fixture doubles
/// from 32 Ki to 64 Ki rows and the streaming arm's peak moves 13% while the
/// in-RAM arm's tracks the candidate. That near-flat line is what keeps a
/// cold-target candidate inside a 1Gi compactor.
///
/// HOW MUCH OF IT IS THE FIXTURE (#3999). [`sweep_peak`] spaces its rows one
/// second apart, so the 64 Ki fixture spans 18 hours and arrives as TWO
/// day-partitioned candidate files where 32 Ki arrives as one. Spacing the
/// same 64 Ki rows a millisecond apart to make one candidate of them, measured
/// 2026-09-16, puts the streaming arm at 55.7 MB against 36.9 MB at 32 Ki: it
/// grows with the candidate, sublinearly (2x the rows, 1.5x the peak), and the
/// first assertion below fails. So part of the near-flat line is the split,
/// and nothing here yet says what one whole cold-target candidate costs the
/// streaming arm. Left as recorded rather than redefined: what the gate should
/// measure is a design question, not a fixture-hardening one (#4703).
#[ignore]
#[test]
fn measure_peak_allocation_per_arm() {
    serialized(|_snapshotter| async move {
        let mut streaming_peaks = Vec::new();
        let mut in_ram_peaks = Vec::new();
        for rows in [16 * 1024usize, 32 * 1024, 64 * 1024] {
            let (in_ram, decoded) = sweep_peak(rows, 1024, IcebergTuning::default()).await;
            let (streaming, _) = sweep_peak(rows, 1024, force_streaming()).await;
            println!(
                "rows={rows} decoded={decoded} in_ram_peak={in_ram} streaming_peak={streaming}"
            );
            streaming_peaks.push(streaming);
            in_ram_peaks.push(in_ram);
        }
        let (small, large) = (streaming_peaks[1], streaming_peaks[2]);
        assert!(
            large * 4 < small * 5,
            "the streaming arm grew with the candidate: {small} B at 32 Ki rows, \
             {large} B at 64 Ki — it is materializing something proportional to \
             the file"
        );
        assert!(
            streaming_peaks[2] * 2 < in_ram_peaks[2],
            "at 64 Ki rows the streaming arm peaked at {} B against the in-RAM \
             arm's {} B; the gate is not buying what it exists for",
            streaming_peaks[2],
            in_ram_peaks[2]
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
