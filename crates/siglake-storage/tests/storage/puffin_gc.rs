use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage,
    OutputFile, Storage, StorageConfig, StorageFactory,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use siglake_core::Event;
use siglake_storage::iceberg::test_catalog::TestCatalog;
use siglake_storage::iceberg::{GcOptions, IcebergContext, StatisticsRetirementReport};

use crate::fixture_clock::{assert_one_partition, fixture_base};

#[derive(Clone, Debug, Default)]
struct ReadMeasurement {
    requests: usize,
    bytes: usize,
    paths: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct CountedFileIoState {
    load: AtomicUsize,
    reads: Mutex<BTreeMap<usize, ReadMeasurement>>,
}

impl CountedFileIoState {
    fn begin_load(&self) {
        self.load.fetch_add(1, Ordering::SeqCst);
    }

    fn record(&self, path: &str, bytes: usize) {
        if !path.ends_with(".avro") {
            return;
        }
        let load = self.load.load(Ordering::SeqCst);
        let mut reads = self.reads.lock().unwrap();
        let measurement = reads.entry(load).or_default();
        measurement.requests += 1;
        measurement.bytes += bytes;
        measurement.paths.insert(path.to_string());
    }

    fn non_empty_loads(&self) -> Vec<ReadMeasurement> {
        self.reads.lock().unwrap().values().cloned().collect()
    }
}

fn default_counted_state() -> Arc<CountedFileIoState> {
    Arc::new(CountedFileIoState::default())
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct CountedLocalFactory {
    #[serde(skip, default = "default_counted_state")]
    state: Arc<CountedFileIoState>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct CountedLocalStorage {
    #[serde(skip, default = "default_counted_state")]
    state: Arc<CountedFileIoState>,
    inner: LocalFsStorage,
}

struct CountedRead {
    path: String,
    state: Arc<CountedFileIoState>,
    inner: Box<dyn FileRead>,
}

#[async_trait]
impl FileRead for CountedRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        let bytes = self.inner.read(range).await?;
        self.state.record(&self.path, bytes.len());
        Ok(bytes)
    }
}

#[typetag::serde]
impl StorageFactory for CountedLocalFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(CountedLocalStorage {
            state: Arc::clone(&self.state),
            inner: LocalFsStorage::new(),
        }))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for CountedLocalStorage {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        self.inner.metadata(path).await
    }

    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        let bytes = self.inner.read(path).await?;
        self.state.record(path, bytes.len());
        Ok(bytes)
    }

    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        Ok(Box::new(CountedRead {
            path: path.to_string(),
            state: Arc::clone(&self.state),
            inner: self.inner.reader(path).await?,
        }))
    }

    async fn write(&self, path: &str, bytes: Bytes) -> iceberg::Result<()> {
        self.inner.write(path, bytes).await
    }

    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
        self.inner.writer(path).await
    }

    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        self.inner.delete_prefix(path).await
    }

    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        self.inner.delete_stream(paths).await
    }

    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

fn counted_file_io(state: &Arc<CountedFileIoState>) -> FileIO {
    FileIOBuilder::new(Arc::new(CountedLocalFactory {
        state: Arc::clone(state),
    }))
    .build()
}

fn with_counted_catalog(ice: IcebergContext, state: Arc<CountedFileIoState>) -> IcebergContext {
    let hook_state = Arc::clone(&state);
    let catalog = TestCatalog::new(ice.catalog().clone())
        .after_load_table(move || {
            let state = Arc::clone(&hook_state);
            async move { state.begin_load() }
        })
        .with_file_io(counted_file_io(&state))
        .shared();
    ice.with_catalog_for_test(catalog)
}

async fn measure_retirement(
    ice: IcebergContext,
    apply: bool,
) -> (StatisticsRetirementReport, Vec<ReadMeasurement>, Duration) {
    let state = Arc::new(CountedFileIoState::default());
    let ice = with_counted_catalog(ice, Arc::clone(&state));
    let started = Instant::now();
    let report = ice
        .retire_obsolete_statistics(ice.events_table_ident(), apply)
        .await
        .unwrap();
    (report, state.non_empty_loads(), started.elapsed())
}

/// One second apart off [`fixture_base`], so the appends a fixture then
/// re-clusters carry one `day(timestamp)` partition value whatever time of day
/// the suite runs (#5678).
fn event_at(offset_secs: i64, raw: &str) -> Event {
    Event {
        timestamp: fixture_base() + chrono::Duration::seconds(offset_secs),
        ..Event::now(raw)
    }
}

async fn rewritten_sidecars() -> (
    tempfile::TempDir,
    IcebergContext,
    iceberg::TableIdent,
    Vec<String>,
    String,
) {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[event_at(0, "database timeout one")])
        .await
        .unwrap();
    ice.append_events(&[event_at(1, "database timeout two")])
        .await
        .unwrap();
    let ident = ice.events_table_ident().clone();
    let before_rewrite = ice.catalog().load_table(&ident).await.unwrap();
    let retired_sidecars: Vec<String> = before_rewrite
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.clone())
        .collect();
    assert_eq!(retired_sidecars.len(), 2);

    let files = ice.live_data_files(&ident).await.unwrap();
    assert_one_partition(&files, "rewritten_sidecars");
    ice.recluster_files(
        &ident,
        files,
        siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
    )
    .await
    .unwrap();
    ice.append_events(&[event_at(2, "database timeout live")])
        .await
        .unwrap();

    let after_append = ice.catalog().load_table(&ident).await.unwrap();
    let live_sidecar = after_append
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.clone())
        .find(|path| !retired_sidecars.contains(path))
        .expect("the post-rewrite append registers a live sidecar");
    (tmp, ice, ident, retired_sidecars, live_sidecar)
}

#[tokio::test]
async fn registered_puffin_sidecars_are_reachable_and_unregistered_ones_are_collected() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = IcebergContext::open(&warehouse)
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[Event::now("database timeout retry")])
        .await
        .unwrap();

    let ident = ice.events_table_ident().clone();
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let stats = table
        .metadata()
        .statistics_iter()
        .next()
        .cloned()
        .expect("append should register a Puffin statistics file");
    let reachable = ice.reachable_files(&ident).await.unwrap();
    assert!(
        reachable.contains(&stats.statistics_path),
        "registered statistics files must be part of the GC reachable set"
    );

    let metadata_dir = table
        .metadata_location()
        .unwrap()
        .rsplit_once('/')
        .unwrap()
        .0;
    let orphan_path = format!("{metadata_dir}/orphan-test.puffin");
    table
        .file_io()
        .new_output(&orphan_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"orphan"))
        .await
        .unwrap();

    let dry_run = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        dry_run.orphans, 1,
        "only the unregistered Puffin should be orphaned"
    );
    assert!(
        table
            .file_io()
            .exists(&stats.statistics_path)
            .await
            .unwrap(),
        "registered Puffin should not be listed for deletion"
    );

    let applied = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(applied.deleted, 1);
    assert!(
        !table.file_io().exists(&orphan_path).await.unwrap(),
        "unregistered Puffin should be reclaimed by GC"
    );
    assert!(
        table
            .file_io()
            .exists(&stats.statistics_path)
            .await
            .unwrap(),
        "registered Puffin must remain after GC"
    );
}

#[tokio::test]
async fn expiry_retires_only_fully_obsolete_owned_statistics_entries() {
    let (_tmp, ice, ident, retired_sidecars, live_sidecar) = rewritten_sidecars().await;

    assert!(ice.expire_snapshots(&ident, 1).await.unwrap() > 0);
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let retained: Vec<&str> = table
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.statistics_path.as_str())
        .collect();
    assert_eq!(retained, vec![live_sidecar.as_str()]);
    for path in &retired_sidecars {
        assert!(
            table.file_io().exists(path).await.unwrap(),
            "metadata retirement leaves {path} for age-gated orphan GC"
        );
    }
    assert!(table.file_io().exists(&live_sidecar).await.unwrap());
}

#[tokio::test]
async fn gc_removes_obsolete_entries_then_reclaims_only_their_puffin_objects() {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    let (_tmp, ice, ident, retired_sidecars, live_sidecar) = rewritten_sidecars().await;
    // Exercise gc-orphans' own retirement sequence by applying only the fork's
    // metadata-only snapshot action first. Production's elected expiry pass
    // chains the same retirement action in its commit (covered above).
    let table = ice.catalog().load_table(&ident).await.unwrap();
    let tx = Transaction::new(&table);
    let expire = tx
        .expire_snapshots()
        .retain_last(1)
        .expire_older_than_ms(i64::MAX)
        .retain_statistics_files();
    let tx = expire.apply(tx).unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
    ice.invalidate_cached_table(&ident).await;

    let dry_run = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(dry_run.statistics_entries_eligible, 2);
    assert_eq!(dry_run.statistics_entries_removed, 0);
    assert!(dry_run.orphans >= retired_sidecars.len());
    let table = ice.catalog().load_table(&ident).await.unwrap();
    for path in &retired_sidecars {
        assert!(table.file_io().exists(path).await.unwrap());
    }

    let report = ice
        .gc_orphans(
            &ident,
            GcOptions {
                min_age: std::time::Duration::ZERO,
                apply: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(report.statistics_entries_eligible, 2);
    assert_eq!(report.statistics_entries_removed, 2);
    assert_eq!(report.statistics_entries_kept_live, 1);

    let table = ice.catalog().load_table(&ident).await.unwrap();
    for path in &retired_sidecars {
        assert!(
            !table.file_io().exists(path).await.unwrap(),
            "obsolete Puffin object survived GC: {path}"
        );
    }
    assert!(
        table.file_io().exists(&live_sidecar).await.unwrap(),
        "a sidecar with a live data-file reference must survive"
    );
}

async fn one_sidecar_fixture(label: &str) -> (tempfile::TempDir, IcebergContext) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join(label))
        .await
        .unwrap()
        .with_inverted_index(true)
        .with_tuning(siglake_storage::iceberg::IcebergTuning {
            index_footer_max_bytes: Some(1),
            ..Default::default()
        });
    ice.append_events(&[event_at(0, label)]).await.unwrap();
    (tmp, ice)
}

async fn make_statistics_unowned(ice: &IcebergContext) {
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let mut replacements = table
        .metadata()
        .statistics_iter()
        .cloned()
        .collect::<Vec<_>>();
    assert!(!replacements.is_empty());
    for statistics in &mut replacements {
        for blob in &mut statistics.blob_metadata {
            blob.r#type = "apache-datasketches-theta-v1".to_string();
        }
    }
    let tx = Transaction::new(&table);
    let mut action = tx.update_statistics();
    for statistics in replacements {
        action = action.set_statistics(statistics);
    }
    action
        .apply(tx)
        .unwrap()
        .commit(ice.catalog().as_ref())
        .await
        .unwrap();
    ice.invalidate_cached_table(ice.events_table_ident()).await;
}

async fn remove_all_statistics(ice: &IcebergContext) {
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let snapshot_ids = table
        .metadata()
        .statistics_iter()
        .map(|statistics| statistics.snapshot_id)
        .collect::<Vec<_>>();
    let tx = Transaction::new(&table);
    let mut action = tx.update_statistics();
    for snapshot_id in snapshot_ids {
        action = action.remove_statistics(snapshot_id);
    }
    action
        .apply(tx)
        .unwrap()
        .commit(ice.catalog().as_ref())
        .await
        .unwrap();
    ice.invalidate_cached_table(ice.events_table_ident()).await;
}

async fn expire_metadata_only(ice: &IcebergContext, retain_last: usize) {
    let table = ice
        .catalog()
        .load_table(ice.events_table_ident())
        .await
        .unwrap();
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshots()
        .retain_last(retain_last)
        .expire_older_than_ms(i64::MAX)
        .retain_statistics_files()
        .apply(tx)
        .unwrap();
    tx.commit(ice.catalog().as_ref()).await.unwrap();
    ice.invalidate_cached_table(ice.events_table_ident()).await;
}

fn print_measurement(
    case: &str,
    report: StatisticsRetirementReport,
    attempts: &[ReadMeasurement],
    elapsed: Duration,
) {
    eprintln!(
        "statistics-retirement case={case} report={report:?} attempts={:?} elapsed_us={}",
        attempts
            .iter()
            .map(|attempt| (attempt.requests, attempt.bytes, attempt.paths.len()))
            .collect::<Vec<_>>(),
        elapsed.as_micros()
    );
}

/// #5261 — keep the regular retirement walk's local cost visible for every
/// ownership disposition. FileIO attribution is restricted to Avro reads, so
/// catalog metadata and Puffin/data IO cannot leak into these numbers.
#[tokio::test]
async fn statistics_retirement_walk_qualification() {
    let no_stats_tmp = tempfile::tempdir().unwrap();
    let no_stats = IcebergContext::open(&no_stats_tmp.path().join("no-statistics"))
        .await
        .unwrap();
    no_stats
        .append_events(&[event_at(0, "no statistics")])
        .await
        .unwrap();
    remove_all_statistics(&no_stats).await;
    let (report, attempts, elapsed) = measure_retirement(no_stats, true).await;
    assert_eq!(report, StatisticsRetirementReport::default());
    assert!(
        attempts.is_empty(),
        "the no-statistics fast path reads no manifests"
    );
    print_measurement("no-statistics", report, &attempts, elapsed);

    let (_tmp, unowned) = one_sidecar_fixture("all-unowned").await;
    make_statistics_unowned(&unowned).await;
    let (report, attempts, elapsed) = measure_retirement(unowned, true).await;
    assert_eq!(report.skipped_unowned, 1);
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].requests, attempts[0].paths.len());
    print_measurement("all-unowned", report, &attempts, elapsed);

    let (_tmp, live) = one_sidecar_fixture("all-live").await;
    let (report, attempts, elapsed) = measure_retirement(live, true).await;
    assert_eq!(report.kept_live, 1);
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].requests, attempts[0].paths.len());
    print_measurement("all-live", report, &attempts, elapsed);

    let (_tmp, obsolete, _ident, _retired, _live) = rewritten_sidecars().await;
    expire_metadata_only(&obsolete, 1).await;
    let (report, attempts, elapsed) = measure_retirement(obsolete, true).await;
    assert_eq!(report.eligible, 2);
    assert_eq!(report.kept_live, 1);
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].requests, attempts[0].paths.len());
    print_measurement("obsolete-owned", report, &attempts, elapsed);
}

/// The shipped expiry setting retains 100 snapshots. Build 102 before the
/// transaction, then land one more append in its first CAS window: both
/// retirement attempts must read each distinct Avro object once even though
/// retained manifest lists share most manifest references.
#[tokio::test]
async fn retain_last_100_retry_counts_each_retirement_attempt() {
    let (tmp, indexed) = one_sidecar_fixture("retain-last-100").await;
    let ident = indexed.events_table_ident().clone();
    let files = indexed.live_data_files(&ident).await.unwrap();
    indexed
        .recluster_files(
            &ident,
            files,
            siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
        )
        .await
        .unwrap();

    let appender = IcebergContext::open(&tmp.path().join("retain-last-100"))
        .await
        .unwrap();
    for offset in 1..=100 {
        appender
            .append_events(&[event_at(offset, "retained history")])
            .await
            .unwrap();
    }
    assert_eq!(appender.snapshot_count_for(&ident).await.unwrap(), 102);

    let state = Arc::new(CountedFileIoState::default());
    let hook_state = Arc::clone(&state);
    let retry_appender = appender.clone();
    let catalog = TestCatalog::new(indexed.catalog().clone())
        .after_load_table(move || {
            let state = Arc::clone(&hook_state);
            async move { state.begin_load() }
        })
        .before_first_update_with_base(move || {
            let appender = retry_appender.clone();
            async move {
                appender
                    .append_events(&[event_at(101, "forced retry")])
                    .await
                    .unwrap();
            }
        })
        .with_file_io(counted_file_io(&state))
        .shared();
    let measured = indexed.with_catalog_for_test(catalog.clone());
    let started = Instant::now();
    assert!(measured.expire_snapshots(&ident, 100).await.unwrap() > 0);
    let elapsed = started.elapsed();
    assert!(catalog.fired(), "the forced-CAS hook fired");

    let attempts = state.non_empty_loads();
    assert_eq!(attempts.len(), 2, "the stale base re-runs retirement once");
    for attempt in &attempts {
        assert_eq!(
            attempt.requests,
            attempt.paths.len(),
            "shared manifests are fetched once per retirement attempt"
        );
    }
    eprintln!(
        "statistics-retirement case=retain-last-100-forced-retry attempts={:?} elapsed_us={}",
        attempts
            .iter()
            .map(|attempt| (attempt.requests, attempt.bytes, attempt.paths.len()))
            .collect::<Vec<_>>(),
        elapsed.as_micros()
    );

    // Orphan GC runs this existing reachability walk after retirement. Keep
    // its shared-manifest rereads out of the retirement attribution above.
    let reach_state = Arc::new(CountedFileIoState::default());
    let reachability = with_counted_catalog(appender, Arc::clone(&reach_state));
    let reach_started = Instant::now();
    reachability.reachable_files(&ident).await.unwrap();
    let reach_elapsed = reach_started.elapsed();
    let reach_loads = reach_state.non_empty_loads();
    assert_eq!(reach_loads.len(), 1);
    assert!(
        reach_loads[0].requests > reach_loads[0].paths.len(),
        "the reachability baseline rereads manifests shared by retained snapshots"
    );
    eprintln!(
        "statistics-retirement case=retain-last-100-reachability requests={} bytes={} distinct_paths={} elapsed_us={}",
        reach_loads[0].requests,
        reach_loads[0].bytes,
        reach_loads[0].paths.len(),
        reach_elapsed.as_micros()
    );
}
