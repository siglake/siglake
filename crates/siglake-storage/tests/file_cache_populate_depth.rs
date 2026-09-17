//! Task #4890: how deep a clipped browse decodes on the SHIPPED whole-file
//! populate path, and what the number is allowed to mean.
//!
//! #4847's disposition (REVISE, reason (1)) rests on a quantity nobody has
//! measured: a row-group-granular cache populates only when a browse decodes a
//! whole group, which at the write path's floor is 131,072 rows. #4494's export
//! counted misses and abandonments, not rows. `CachePopulateStream` now records
//! one `siglake_query_scan_file_cache_populate_rows{outcome}` observation per
//! population, in `Drop`, so a clipped read that inserts nothing still says how
//! far it got.
//!
//! These phases pin the accounting the reader depends on:
//!
//! 1. a drained scan observes `completed` at the file's whole row count, and
//!    the repeat that hits observes nothing (no population exists to measure);
//! 2. a clipped browse observes `clipped` at the rows its reader actually
//!    handed over — far below the 131,072 floor for a fixture this size, and
//!    NOT the rows the query returned;
//! 3. a predicate shape observes nothing at all and counts `bypass` instead
//!    (#4891). The per-request `file_cache_bypasses` counter is what tells that
//!    apart from a shape that decoded zero rows;
//! 4. the per-request `file_cache_populate_rows` counter equals the depth the
//!    process histogram saw for the same execution, so a round can attribute
//!    depth to a shape without isolating the process.
//!
//! Isolated in its own test binary: the query-scan tuning, the file batch cache
//! and the global metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const DEPTH_METRIC: &str = "siglake_query_scan_file_cache_populate_rows";
const REQUESTS_METRIC: &str = "siglake_query_scan_file_cache_requests_total";

/// The write path's `MIN_ROW_GROUP_ROWS`: the floor a clipped browse would have
/// to reach for row-group population to leave anything behind.
const ROW_GROUP_FLOOR_ROWS: u64 = 131_072;

/// Population depth observations of one phase, by outcome label. Every
/// observation is one population; `rows` are the rows it was handed.
#[derive(Debug, Default, PartialEq)]
struct Depth {
    completed: Vec<u64>,
    clipped: Vec<u64>,
    unpolled: Vec<u64>,
    error: Vec<u64>,
}

impl Depth {
    fn observations(&self) -> usize {
        self.completed.len() + self.clipped.len() + self.unpolled.len() + self.error.len()
    }

    fn absorb(&mut self, other: Depth) {
        self.completed.extend(other.completed);
        self.clipped.extend(other.clipped);
        self.unpolled.extend(other.unpolled);
        self.error.extend(other.error);
    }
}

/// Depth observations and the request-outcome counters of one phase.
/// `Snapshotter::snapshot()` drains the registry, so every read is a delta.
fn phase(snapshotter: &Snapshotter) -> (Depth, u64, u64, u64) {
    let snapshot = snapshotter.snapshot().into_vec();
    let label = |key: &metrics_util::CompositeKey, name: &str| -> Option<String> {
        key.key()
            .labels()
            .find(|label| label.key() == name)
            .map(|label| label.value().to_string())
    };
    let mut depth = Depth::default();
    let mut hit = 0;
    let mut miss = 0;
    let mut bypass = 0;
    for (key, _, _, value) in &snapshot {
        match (key.key().name(), value) {
            (DEPTH_METRIC, DebugValue::Histogram(samples)) => {
                let rows = samples.iter().map(|sample| sample.0 as u64);
                let outcome = label(key, "outcome").expect("depth carries an outcome label");
                match outcome.as_str() {
                    "completed" => depth.completed.extend(rows),
                    "clipped" => depth.clipped.extend(rows),
                    "unpolled" => depth.unpolled.extend(rows),
                    "error" => depth.error.extend(rows),
                    other => panic!("unexpected populate outcome {other}"),
                }
            }
            (REQUESTS_METRIC, DebugValue::Counter(count)) => {
                match label(key, "outcome").unwrap_or_default().as_str() {
                    "hit" => hit += count,
                    "miss" => miss += count,
                    "bypass" => bypass += count,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    (depth, hit, miss, bypass)
}

/// Accumulate until `settled` holds or the deadline passes: the histogram is
/// recorded from `CachePopulateStream::Drop`, and a clipped plan's streams are
/// dropped by whichever task last held them — for a multi-partition plan a
/// `CoalescePartitionsExec` worker, a scheduler tick after `collect()` returns.
async fn phase_settling(
    snapshotter: &Snapshotter,
    settled: impl Fn(&Depth, u64) -> bool,
) -> (Depth, u64, u64, u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut depth = Depth::default();
    let (mut hit, mut miss, mut bypass) = (0, 0, 0);
    loop {
        let (delta, h, m, b) = phase(snapshotter);
        depth.absorb(delta);
        hit += h;
        miss += m;
        bypass += b;
        if settled(&depth, bypass) || std::time::Instant::now() >= deadline {
            return (depth, hit, miss, bypass);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

fn events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| Event::now(format!("row-{i} checkout latency={} ms", i % 97)))
        .collect()
}

async fn rows(ctx: &SessionContext, sql: &str) -> usize {
    ctx.sql(sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

#[tokio::test]
async fn population_depth_is_recorded_for_every_population_including_the_clipped_ones() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let batch_rows = 256;
    let file_rows = 4 * batch_rows;
    siglake_storage::configure_query_scan_tuning(QueryScanTuning {
        file_cache_max_bytes: Some(8 * 1024 * 1024 * 1024),
        file_cache_max_entries: Some(16384),
        file_concurrency_limit: Some(1),
        batch_size: Some(batch_rows),
        ..Default::default()
    });

    // Phase 1: a drained scan is one `completed` population at the file's whole
    // row count, and the repeat that hits builds no population to observe.
    let drained_root = tmp.path().join("drained");
    let drained = IcebergContext::open(&drained_root).await.unwrap();
    drained.append_events(&events(file_rows)).await.unwrap();
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    drained.register_with_datafusion(&ctx).await.unwrap();
    let _ = phase(&snapshotter);

    assert_eq!(rows(&ctx, "SELECT raw FROM events").await, file_rows);
    let (depth, _, miss, _) = phase_settling(&snapshotter, |d, _| d.observations() >= 1).await;
    assert_eq!(miss, 1);
    assert_eq!(
        depth.completed,
        vec![file_rows as u64],
        "a drained population is observed once, at the rows it was handed: {depth:?}"
    );
    assert_eq!(depth.clipped.len(), 0, "{depth:?}");

    assert_eq!(rows(&ctx, "SELECT raw FROM events").await, file_rows);
    let (depth, hit, _, _) = phase(&snapshotter);
    assert_eq!(hit, 1);
    assert_eq!(
        depth.observations(),
        0,
        "a hit builds no populate stream, so it observes no depth: {depth:?}"
    );

    // Phase 2: the clipped browse the 0.2.0 decision turns on. The observation
    // is the rows the READER handed the population, which is neither the rows
    // the query returned (10) nor the whole file: the scan stops after the
    // batch that satisfies the LIMIT.
    let clipped_root = tmp.path().join("clipped");
    let clipped = IcebergContext::open(&clipped_root).await.unwrap();
    clipped.append_events(&events(file_rows)).await.unwrap();
    let clipped_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    clipped
        .register_with_datafusion(&clipped_ctx)
        .await
        .unwrap();
    let _ = phase(&snapshotter);

    assert_eq!(
        rows(&clipped_ctx, "SELECT raw FROM events LIMIT 10").await,
        10
    );
    let (depth, _, miss, _) = phase_settling(&snapshotter, |d, _| d.observations() >= 1).await;
    assert_eq!(miss, 1);
    assert_eq!(
        depth.completed.len(),
        0,
        "the file was not drained: {depth:?}"
    );
    assert_eq!(depth.clipped.len(), 1, "one clipped population: {depth:?}");
    let decoded = depth.clipped[0];
    eprintln!("CLIPPED BROWSE decode depth: {decoded} rows of {file_rows} in the file");
    assert!(
        (10..=file_rows as u64).contains(&decoded),
        "a clipped browse decodes at least the rows it returned and at most the \
         file: {decoded}"
    );
    assert!(
        decoded < ROW_GROUP_FLOOR_ROWS,
        "this fixture's browse cannot reach the row-group floor; if it ever does, \
         the phase stopped measuring what it claims: {decoded}"
    );

    // Phase 3: a converted predicate bypasses population (#4891). No depth
    // observation at all — which a reader must not confuse with a browse that
    // decoded nothing. The `bypass` counter is the distinguishing evidence.
    let label_root = tmp.path().join("label");
    let label = IcebergContext::open(&label_root).await.unwrap();
    let mut labelled = events(file_rows);
    for event in &mut labelled {
        event.host = "alpha".into();
    }
    label.append_events(&labelled).await.unwrap();
    let label_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    label.register_with_datafusion(&label_ctx).await.unwrap();
    let _ = phase(&snapshotter);

    assert_eq!(
        rows(
            &label_ctx,
            "SELECT raw FROM events WHERE host = 'alpha' LIMIT 10"
        )
        .await,
        10
    );
    let (mut depth, _, _, bypass) = phase_settling(&snapshotter, |_, b| b >= 1).await;
    // A population dropped by another worker would record a sample a scheduler
    // tick later; give one the chance to appear before claiming there is none.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    depth.absorb(phase(&snapshotter).0);
    assert_eq!(
        (depth.observations(), bypass),
        (0, 1),
        "a predicate task bypasses population: no depth sample, one bypass: {depth:?}"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
}
