//! Task #5801: `siglake_query_scan_file_cache_accounted_bytes` is the total the
//! decoded-file cache's byte budget is enforced against.
//!
//! Its sibling `siglake_query_scan_file_cache_bytes` reports COMPLETED entries
//! only, so on a pod whose scans are populating — or populating and never
//! inserting — the resident number is below the budget while the bound is being
//! enforced against something larger. The four readings below are the four
//! moments that total changes: a live charge, the end-of-stream handoff into an
//! entry, an eviction, and release.
//!
//! Isolated in its own binary: the scan tuning, the batch cache, the accounting
//! total and the metrics recorder are all process-wide.

use datafusion::prelude::SessionContext;
use futures::StreamExt;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const FILES: usize = 6;
/// Eight batches per file: enough that the first batch to reach the caller
/// cannot be the file's last, which is what makes the live-charge reading a
/// reading of a population still in flight.
const FILE_ROWS: usize = 32_768;
const BATCH_ROWS: usize = 4_096;
/// A drained scan: the shape that populates this cache at all (#4494).
const SCAN_SQL: &str = "SELECT raw FROM events";
const GIB: u64 = 1024 * 1024 * 1024;

const ACCOUNTED: &str = "siglake_query_scan_file_cache_accounted_bytes";
const RESIDENT: &str = "siglake_query_scan_file_cache_bytes";

/// Last observed value of both gauges, the way a scrape reads them.
///
/// `Snapshotter::snapshot` drains the recorder, so a gauge nothing has written
/// since the previous call is absent rather than zero. Carrying the last value
/// forward is what a Prometheus series does between writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Gauges {
    accounted: u64,
    resident: u64,
}

/// One snapshot's reading: both gauges, and the evictions counted since the
/// previous sample. A snapshot drains the recorder, so everything one phase
/// wants has to come out of the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sample {
    gauges: Gauges,
    evictions: u64,
}

fn sample(snapshotter: &Snapshotter, last: &mut Gauges) -> Sample {
    let mut evictions = 0;
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        match value {
            DebugValue::Gauge(value) => {
                let value = value.into_inner() as u64;
                match key.key().name() {
                    ACCOUNTED => last.accounted = value,
                    RESIDENT => last.resident = value,
                    _ => {}
                }
            }
            DebugValue::Counter(count) => {
                let evicted = key.key().name() == "siglake_query_scan_file_cache_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "evict");
                if evicted {
                    evictions += count;
                }
            }
            _ => {}
        }
    }
    Sample {
        gauges: *last,
        evictions,
    }
}

fn events(file: usize) -> Vec<Event> {
    (0..FILE_ROWS)
        .map(|row| {
            let mut event =
                Event::now(format!("file-{file} row-{row} status=200 region=us-east-1"));
            event.host = "alpha".into();
            event
        })
        .collect()
}

/// One partition, so populations do not race each other for the cache lock: a
/// contended population is skipped by design, which would make these counts a
/// reading of the scheduler.
fn tuning(max_bytes: u64) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: Some(max_bytes),
        file_cache_max_entries: Some(1_024),
        file_concurrency_limit: Some(1),
        batch_size: Some(BATCH_ROWS),
        ..Default::default()
    }
}

async fn drain(ctx: &SessionContext) -> usize {
    ctx.sql(SCAN_SQL)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// The populate streams charge as they finish, and the last one can be dropped
/// a scheduler tick after the rows arrive.
async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_accounted_gauge_tracks_live_charges_handoff_eviction_and_release() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");
    let mut last = Gauges::default();

    // What the query server does at startup: the series exists at 0 before any
    // scan, so a pod with the cache switched off reads an empty cache rather
    // than No data.
    siglake_storage::initialize_decoded_file_cache_metrics();
    let created: Vec<f64> = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == ACCOUNTED)
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(value) => value.into_inner(),
            other => panic!("{ACCOUNTED} is not a gauge: {other:?}"),
        })
        .collect();
    assert_eq!(
        created,
        vec![0.0],
        "startup must create the accounted gauge at 0, once"
    );

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    for file in 0..FILES {
        ice.append_events(&events(file)).await.unwrap();
    }
    let rows = FILES * FILE_ROWS;

    // Phase 0: a budget nothing can bind, to price one entry of this fixture.
    siglake_storage::configure_query_scan_tuning(tuning(8 * GIB));
    let probe_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&probe_ctx).await.unwrap();
    assert_eq!(drain(&probe_ctx).await, rows);
    settle().await;
    let probe = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(probe.entries, FILES, "the probe cached no entry: {probe:?}");
    let entry_bytes = probe.priced_bytes / probe.entries as u64;
    let probed = sample(&snapshotter, &mut last).gauges;
    assert_eq!(
        (probed.accounted, probed.resident),
        (probe.priced_bytes, probe.priced_bytes),
        "with no population in flight the two gauges are the same number: {probe:?}"
    );

    // Phase 1: a live charge. The first batch to reach the caller has already
    // been admitted, and its file is nowhere near its end, so the population is
    // in the accounted total and in no entry.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(8 * entry_bytes));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let cleared = sample(&snapshotter, &mut last).gauges;
    assert_eq!(cleared.accounted, 0, "clearing left bytes accounted for");

    let mut stream = ctx
        .sql(SCAN_SQL)
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let first = stream.next().await.expect("the scan yielded no batch");
    let mut streamed = first.unwrap().num_rows();
    let live = sample(&snapshotter, &mut last).gauges;
    let live_footprint = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(
        live_footprint.entries, 0,
        "a plan that decodes a whole file before its first batch reaches the \
         caller cannot show a live population: {live_footprint:?}"
    );
    assert!(
        live.accounted > 0 && live.resident == 0,
        "a population in flight must be in the accounted total and in no \
         entry: {live:?}"
    );

    // Phase 2: end-of-stream handoff. The charge becomes the entry; it is not
    // counted twice on the way.
    while let Some(batch) = stream.next().await {
        streamed += batch.unwrap().num_rows();
    }
    assert_eq!(streamed, rows);
    drop(stream);
    settle().await;
    let handed_off = sample(&snapshotter, &mut last).gauges;
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert_eq!(
        (handed_off.accounted, handed_off.resident),
        (footprint.priced_bytes, footprint.priced_bytes),
        "the handoff must leave the entry's bytes counted once: {footprint:?}"
    );
    assert!(
        handed_off.accounted <= 8 * entry_bytes,
        "the accounted total exceeded the budget it is enforced against: \
         {handed_off:?}"
    );

    // Phase 3: eviction. Four entries' worth of budget over six files, so the
    // total is held down by eviction rather than by the fixture fitting.
    let budget = 4 * entry_bytes;
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(budget));
    let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&ctx).await.unwrap();
    let _ = sample(&snapshotter, &mut last);
    assert_eq!(drain(&ctx).await, rows);
    settle().await;
    let evicted_pass = sample(&snapshotter, &mut last);
    let evicted = evicted_pass.gauges;
    let footprint = siglake_storage::decoded_file_cache_footprint();
    assert!(
        evicted_pass.evictions > 0,
        "six files into a four-entry budget evicted nothing"
    );
    assert_eq!(
        (evicted.accounted, evicted.resident),
        (footprint.priced_bytes, footprint.priced_bytes),
        "an eviction must come off both gauges: {footprint:?}"
    );
    assert!(
        evicted.accounted <= budget,
        "the accounted total exceeded its {budget}-byte budget: {evicted:?}"
    );

    // Phase 4: release. Dropping every entry returns the whole total.
    siglake_storage::clear_decoded_file_cache();
    let released = sample(&snapshotter, &mut last).gauges;
    assert_eq!(
        released.accounted, 0,
        "a cleared cache still accounts for bytes: {released:?}"
    );

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();

    eprintln!(
        "#5801 accounted gauge: one entry = {} KiB priced over {FILES} files x \
         {FILE_ROWS} rows; live charge {} KiB, eviction budget {} KiB",
        entry_bytes / 1024,
        live.accounted / 1024,
        budget / 1024,
    );
}
