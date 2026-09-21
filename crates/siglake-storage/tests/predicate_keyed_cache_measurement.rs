//! Task #4905: local predicate-keyed decoded-cache qualification.
//!
//! `#[ignore]`d because it writes 240k events and runs a 20-request synthetic
//! browse trace through three policies. Run it in release mode and retain its
//! printed table in `docs/DESIGN_predicate_keyed_decoded_cache.md`:
//!
//! ```text
//! cargo test --release -p siglake-storage \
//!   --test predicate_keyed_cache_measurement -- --ignored --nocapture
//! ```
//!
//! Workload assumptions are declared rather than attributed to production:
//! four files with the same timestamp extent; every predicate returns 20 rows
//! per file, so `LIMIT 100` exhausts all four tasks and admits complete entries.
//! The 20 requests are eight uses of one stable label, two uses each of three
//! varied labels, and six one-use moving 20-second windows. Each time window
//! also carries `host = 'bulk'`: when the cache is enabled that exact-capable
//! filter is declared `Inexact`, making this the non-order-preserving scan whose
//! decoded cache is under study; the host-plus-window predicate defines the
//! candidate key. That is a 50%
//! request-repeat ceiling. Host predicates share one file/projection identity
//! (four keys per identity); time windows share another (six keys per identity).
//!
//! Arms: cache disabled; shipped post-#4891 cache (predicate misses bypass);
//! and #4905's in-process-only predicate-key prototype. Both cache arms use a
//! 64 MiB / 16-entry budget, so the prototype can retain four predicates over
//! four files and has to evict the rest. Planning remains `Inexact`; the
//! residual filter is kept in every arm.

use std::collections::HashMap;

use chrono::{Duration, TimeZone, Utc};
use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const FILES: usize = 4;
const FILE_ROWS: usize = 60_000;
const MATCHES_PER_FILE: usize = 20;
const CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const CACHE_MAX_ENTRIES: usize = 16;

#[derive(Clone)]
struct Request {
    class: &'static str,
    sql: String,
}

fn workload() -> Vec<Request> {
    let label = |class, value| Request {
        class,
        sql: format!("SELECT raw FROM events WHERE host = '{value}' LIMIT 100"),
    };
    let mut out = vec![
        label("stable/cold", "alpha"),
        label("stable/repeat", "alpha"),
        label("stable/repeat", "alpha"),
        label("stable/repeat", "alpha"),
        label("varied/cold", "beta"),
        label("varied/repeat", "beta"),
        label("varied/cold", "gamma"),
        label("varied/repeat", "gamma"),
        label("varied/cold", "delta"),
        label("varied/repeat", "delta"),
        label("stable/repeat", "alpha"),
        label("stable/repeat", "alpha"),
        label("stable/repeat", "alpha"),
        label("stable/repeat", "alpha"),
    ];
    let base = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    for n in 0..6 {
        let lo = base + Duration::seconds(10_000 + n * 100);
        let hi = lo + Duration::seconds(MATCHES_PER_FILE as i64);
        out.push(Request {
            class: "moving/cold",
            sql: format!(
                "SELECT raw FROM events WHERE host = 'bulk' \
                 AND timestamp >= TIMESTAMP '{}' \
                 AND timestamp < TIMESTAMP '{}' LIMIT 100",
                lo.format("%Y-%m-%dT%H:%M:%SZ"),
                hi.format("%Y-%m-%dT%H:%M:%SZ")
            ),
        });
    }
    out
}

fn fixture(file: usize) -> Vec<Event> {
    let base = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    (0..FILE_ROWS)
        .map(|row| {
            let mut event = Event::now(format!(
                "file-{file} row-{row} checkout latency={} ms",
                row % 97
            ));
            event.timestamp = base + Duration::seconds(row as i64);
            event.host = match row {
                100..120 => "alpha",
                200..220 => "beta",
                300..320 => "gamma",
                400..420 => "delta",
                _ => "bulk",
            }
            .into();
            event
        })
        .collect()
}

fn tuning(cache: bool, prototype: bool) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: cache.then_some(CACHE_MAX_BYTES),
        file_cache_max_entries: cache.then_some(CACHE_MAX_ENTRIES),
        file_concurrency_limit: Some(1),
        batch_size: Some(8_192),
        file_cache_predicate_key_prototype: prototype,
        ..Default::default()
    }
}

async fn sorted_raws(ctx: &SessionContext, sql: &str) -> Vec<String> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("raw")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| column.value(i).to_string()));
    }
    rows.sort();
    rows
}

#[derive(Debug, Default)]
struct Counters {
    hit: u64,
    miss: u64,
    bypass: u64,
    insert: u64,
    evict: u64,
    abandoned: u64,
    completed: u64,
    fetched_bytes: u64,
    decoded_bytes: u64,
}

impl Counters {
    fn read(snapshotter: &Snapshotter) -> Self {
        let snapshot = snapshotter.snapshot().into_vec();
        let counter = |outcome: &str| {
            snapshot
                .iter()
                .filter(|(key, _, _, _)| {
                    key.key().name() == "siglake_query_scan_file_cache_requests_total"
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == outcome)
                })
                .map(|(_, _, _, value)| match value {
                    DebugValue::Counter(value) => *value,
                    _ => 0,
                })
                .sum()
        };
        let histogram = |name: &str, outcome: Option<&str>| {
            snapshot
                .iter()
                .filter(|(key, _, _, _)| {
                    key.key().name() == name
                        && outcome.is_none_or(|wanted| {
                            key.key()
                                .labels()
                                .any(|label| label.key() == "outcome" && label.value() == wanted)
                        })
                })
                .filter_map(|(_, _, _, value)| match value {
                    DebugValue::Histogram(samples) => Some(samples),
                    _ => None,
                })
                .flatten()
                .map(|sample| sample.into_inner() as u64)
                .sum::<u64>()
        };
        let histogram_count = |name: &str, outcome: &str| {
            snapshot
                .iter()
                .filter(|(key, _, _, _)| {
                    key.key().name() == name
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "outcome" && label.value() == outcome)
                })
                .filter_map(|(_, _, _, value)| match value {
                    DebugValue::Histogram(samples) => Some(samples.len() as u64),
                    _ => None,
                })
                .sum()
        };
        Self {
            hit: counter("hit"),
            miss: counter("miss"),
            bypass: counter("bypass"),
            insert: counter("insert"),
            evict: counter("evict"),
            abandoned: counter("abandoned"),
            completed: histogram_count("siglake_query_scan_file_cache_populate_rows", "completed"),
            fetched_bytes: histogram("siglake_query_scan_partition_fetched_bytes", None),
            decoded_bytes: histogram("siglake_query_scan_partition_decoded_bytes", None),
        }
    }
}

fn p50(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: 240k events, 20 requests through three cache policies"]
async fn predicate_keyed_cache_browse_trace() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let started = std::time::Instant::now();
    for file in 0..FILES {
        ice.append_events(&fixture(file)).await.unwrap();
    }
    println!(
        "fixture: {FILES} files x {FILE_ROWS} rows, written in {:.1}s; workload: 20 requests, 10 distinct predicates, 50% repeat ceiling",
        started.elapsed().as_secs_f64()
    );

    let requests = workload();
    let warm_ctx = siglake_storage::session_context_with_target_partitions(Some(1));
    ice.register_with_datafusion(&warm_ctx).await.unwrap();
    siglake_storage::configure_query_scan_tuning(tuning(false, false));
    let _ = sorted_raws(&warm_ctx, "SELECT raw, host, timestamp FROM events").await;

    // Warm every query-specific footer/page-index path before the matched arms;
    // the comparison is decoded-cache policy, not first-touch range I/O.
    let mut expected: HashMap<String, Vec<String>> = HashMap::new();
    for request in &requests {
        if !expected.contains_key(&request.sql) {
            expected.insert(
                request.sql.clone(),
                sorted_raws(&warm_ctx, &request.sql).await,
            );
        }
    }
    let _ = Counters::read(&snapshotter);
    println!(
        "\n{:<12} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>10} {:>10}",
        "arm",
        "stable_c",
        "stable_r",
        "varied_c",
        "varied_r",
        "moving_c",
        "hits",
        "misses",
        "entries",
        "retainMiB",
        "fetchMiB"
    );
    for (arm, cache, prototype) in [
        ("disabled", false, false),
        ("post4891", true, false),
        ("predicate", true, true),
    ] {
        siglake_storage::clear_decoded_file_cache();
        siglake_storage::configure_query_scan_tuning(tuning(cache, prototype));
        let ctx = siglake_storage::session_context_with_target_partitions(Some(1));
        ice.register_with_datafusion(&ctx).await.unwrap();
        let _ = Counters::read(&snapshotter);

        let mut timings: HashMap<&str, Vec<f64>> = HashMap::new();
        for request in &requests {
            let start = std::time::Instant::now();
            let rows = sorted_raws(&ctx, &request.sql).await;
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows.len(), FILES * MATCHES_PER_FILE, "{}", request.sql);
            assert_eq!(
                &rows,
                expected.get(&request.sql).unwrap(),
                "{arm}: answer diverged for {}",
                request.sql
            );
            timings.entry(request.class).or_default().push(elapsed);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let counters = Counters::read(&snapshotter);
        let footprint = siglake_storage::decoded_file_cache_footprint();
        let mut median = |class| p50(timings.get_mut(class).unwrap());
        println!(
            "{:<12} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9} {:>9} {:>9} {:>10.3} {:>10.1}",
            arm,
            median("stable/cold"),
            median("stable/repeat"),
            median("varied/cold"),
            median("varied/repeat"),
            median("moving/cold"),
            counters.hit,
            counters.miss,
            footprint.entries,
            mib(footprint.retained_bytes),
            mib(counters.fetched_bytes),
        );
        println!(
            "  bypass={} insert={} completed={} evict={} abandoned={} decoded={:.1}MiB priced={:.3}MiB extent={:.3}MiB",
            counters.bypass,
            counters.insert,
            counters.completed,
            counters.evict,
            counters.abandoned,
            mib(counters.decoded_bytes),
            mib(footprint.priced_bytes),
            mib(footprint.extent_bytes),
        );
    }

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
}
