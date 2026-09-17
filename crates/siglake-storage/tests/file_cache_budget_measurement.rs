//! Task #3053: what an explicitly configured decoded-file cache buys, and what
//! it costs, at budgets a pod can actually afford.
//!
//! `#[ignore]`d — it writes half a million events and runs five budgets over a
//! repeated drained scan, which is a measurement, not a gate. Run it and read
//! the table it prints:
//!
//! ```text
//! cargo test --release -p siglake-storage --test file_cache_budget_measurement \
//!   -- --ignored --nocapture
//! ```
//!
//! Every earlier measurement of this cache (#4494, #4846, #4847) ran at the
//! bench fleet's 8 GiB / 16,384 entries, which no packaged pod has: the chart's
//! query pod is 4Gi TOTAL. So the budget never bound anything and eviction,
//! the oversized rule and the pool subtraction were all untested at the sizes
//! an operator would set. The arms here are multiples of the MEASURED entry
//! size, so the interesting boundaries land where they land rather than where a
//! constant guessed:
//!
//! * `off`      — the packaged default, `0/0`; the control every arm is read against;
//! * `fits`     — 16 entries' worth of bytes, so the whole working set stays;
//! * `bytes`    — 4 entries' worth, so the BYTE bound evicts;
//! * `entries`  — the same 16-entry byte budget with `max_entries = 2`, so the
//!   ENTRY bound evicts while bytes are free;
//! * `oversize` — 3 entries' worth, which puts one entry over `budget / 4` and
//!   therefore caches NOTHING while still subtracting its bytes from the query
//!   memory pool.
//!
//! Reported per arm: cold and warm wall time for the repeated drained scan, the
//! cache counters, the installed footprint in the three currencies
//! (budget-priced, row extent, distinct retained allocations) and the peak
//! population memory across concurrent partitions.

use datafusion::prelude::SessionContext;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::QueryScanTuning;

const FILES: usize = 8;
const FILE_ROWS: usize = 65_536;
const BATCH_ROWS: usize = 8_192;
const REPEATS: usize = 5;
/// The drained scan: the only shape that populates this cache at all (#4494 —
/// a clipped browse is dropped before the end-of-stream insert).
const SCAN_SQL: &str = "SELECT raw FROM events";

fn fixture_events(file: usize) -> Vec<Event> {
    (0..FILE_ROWS)
        .map(|i| {
            let mut event = Event::now(format!(
                "file-{file} row-{i} checkout latency={} ms status=200 region=us-east-1",
                i % 97
            ));
            event.host = "alpha".into();
            event
        })
        .collect()
}

fn tuning(max_bytes: Option<u64>, max_entries: Option<usize>) -> QueryScanTuning {
    QueryScanTuning {
        file_cache_max_bytes: max_bytes,
        file_cache_max_entries: max_entries,
        file_concurrency_limit: Some(FILES),
        batch_size: Some(BATCH_ROWS),
        ..Default::default()
    }
}

async fn row_count(ctx: &SessionContext, sql: &str) -> usize {
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

#[derive(Debug, Default, Clone, Copy)]
struct Counters {
    hit: u64,
    miss: u64,
    insert: u64,
    evict: u64,
    skip_oversized: u64,
    contended: u64,
    abandoned: u64,
}

impl Counters {
    fn read(snapshotter: &Snapshotter) -> Self {
        let snapshot = snapshotter.snapshot().into_vec();
        let sum = |outcome: &str| {
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
                    DebugValue::Counter(count) => *count,
                    _ => 0,
                })
                .sum::<u64>()
        };
        Self {
            hit: sum("hit"),
            miss: sum("miss"),
            insert: sum("insert"),
            evict: sum("evict"),
            skip_oversized: sum("skip_oversized"),
            contended: sum("insert_skipped_contended"),
            abandoned: sum("abandoned"),
        }
    }

    fn add(&mut self, other: Self) {
        self.hit += other.hit;
        self.miss += other.miss;
        self.insert += other.insert;
        self.evict += other.evict;
        self.skip_oversized += other.skip_oversized;
        self.contended += other.contended;
        self.abandoned += other.abandoned;
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement: 512k events written, five cache budgets over a repeated drained scan"]
async fn file_cache_budget_measurement() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(tmp.path())
        .await
        .unwrap()
        .with_table_cache_ttl(std::time::Duration::ZERO);
    let build = std::time::Instant::now();
    for file in 0..FILES {
        ice.append_events(&fixture_events(file)).await.unwrap();
    }
    let rows = FILES * FILE_ROWS;
    println!(
        "fixture: {FILES} files x {FILE_ROWS} rows ({rows} total), written in {:.1}s",
        build.elapsed().as_secs_f64()
    );

    // One pass at a budget nothing can bind, to MEASURE what one entry costs.
    // Every arm below is a multiple of this number, so the byte bound, the
    // entry bound and the oversized rule each bite where the data puts them.
    siglake_storage::clear_decoded_file_cache();
    siglake_storage::configure_query_scan_tuning(tuning(
        Some(8 * 1024 * 1024 * 1024),
        Some(16_384),
    ));
    let probe_ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
    ice.register_with_datafusion(&probe_ctx).await.unwrap();
    let expected = row_count(&probe_ctx, SCAN_SQL).await;
    assert_eq!(expected, rows);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let probe = siglake_storage::decoded_file_cache_footprint();
    assert!(
        probe.entries > 0,
        "the unbounded probe pass cached nothing; the arms below have no unit"
    );
    let entry_bytes = probe.priced_bytes / probe.entries as u64;
    println!(
        "one entry (one file's decoded `raw` under this projection): priced {:.1} MiB, \
         extent {:.1} MiB, retained {:.1} MiB across {} batches; \
         {} entries priced {:.1} MiB in total",
        mib(entry_bytes),
        mib(probe.extent_bytes) / probe.entries as f64,
        mib(probe.retained_bytes) / probe.entries as f64,
        probe.batches / probe.entries,
        probe.entries,
        mib(probe.priced_bytes),
    );
    println!(
        "  parquet on disk for the same rows: {:.1} MiB; \
         the cache holds the DECODED form, which is what its budget buys",
        mib(table_bytes(tmp.path()))
    );

    let arms: [(&str, Option<u64>, Option<usize>); 5] = [
        ("off", Some(0), Some(0)),
        ("fits", Some(16 * entry_bytes), Some(64)),
        ("bytes", Some(4 * entry_bytes), Some(64)),
        ("entries", Some(16 * entry_bytes), Some(2)),
        ("oversize", Some(3 * entry_bytes), Some(64)),
    ];

    println!(
        "\n{:<9} {:>9} {:>9} {:>9} {:>9}   counters, then footprint and population peak (MiB)",
        "arm", "budget", "cold_ms", "warm_p50", "warm_max"
    );
    for (arm, max_bytes, max_entries) in arms {
        siglake_storage::clear_decoded_file_cache();
        siglake_storage::configure_query_scan_tuning(tuning(max_bytes, max_entries));
        siglake_storage::reset_decoded_file_cache_population_peaks();
        let ctx = siglake_storage::session_context_with_target_partitions(Some(FILES));
        ice.register_with_datafusion(&ctx).await.unwrap();
        let _ = Counters::read(&snapshotter);

        let mut cold = Counters::default();
        let mut warm_counters = Counters::default();
        let mut timings = Vec::new();
        for run in 0..REPEATS {
            let started = std::time::Instant::now();
            let got = row_count(&ctx, SCAN_SQL).await;
            timings.push(started.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(
                got, expected,
                "{arm} run {run} returned {got} of {expected} rows"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let delta = Counters::read(&snapshotter);
            if run == 0 {
                cold = delta;
            } else {
                warm_counters.add(delta);
            }
        }
        let footprint = siglake_storage::decoded_file_cache_footprint();
        let population = siglake_storage::decoded_file_cache_population_stats();
        let mut warm = timings[1..].to_vec();
        warm.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let warm_runs = (REPEATS - 1) as u64;

        println!(
            "{:<9} {:>9} {:>9.1} {:>9.1} {:>9.1}\n    cold  {}\n    warm  {}\n    \
             cache entries={} priced={:.1} extent={:.1} retained={:.1} | \
             population peak extent={:.1} retained={:.1} streams={}",
            arm,
            max_bytes
                .map(|bytes| format!("{:.0}MiB", mib(bytes)))
                .unwrap_or_else(|| "-".into()),
            timings[0],
            warm[warm.len() / 2],
            warm[warm.len() - 1],
            summary(&cold, 1),
            summary(&warm_counters, warm_runs),
            footprint.entries,
            mib(footprint.priced_bytes),
            mib(footprint.extent_bytes),
            mib(footprint.retained_bytes),
            mib(population.peak_extent_bytes),
            mib(population.peak_retained_bytes),
            population.peak_streams,
        );
    }

    siglake_storage::configure_query_scan_tuning(QueryScanTuning::default());
    siglake_storage::clear_decoded_file_cache();
}

fn summary(counters: &Counters, runs: u64) -> String {
    format!(
        "hit={} miss={} ins={} evict={} oversize={} cont={} aband={} (per run, {runs} run(s))",
        counters.hit / runs,
        counters.miss / runs,
        counters.insert / runs,
        counters.evict / runs,
        counters.skip_oversized / runs,
        counters.contended / runs,
        counters.abandoned / runs,
    )
}

/// Parquet bytes the fixture wrote, so the decoded entry size can be read
/// against the on-disk size it came from.
fn table_bytes(root: &std::path::Path) -> u64 {
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                *total += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
            }
        }
    }
    let mut total = 0;
    walk(root, &mut total);
    total
}
