//! Drain throughput, and WHERE the drain's time goes.
//!
//! The drain — WAL → Iceberg commit — is the binding constraint on sustainable
//! ingest, not compaction. The 2026-08-06 1TB round accepted at 141,218 rows/s
//! and drained at 72,124: accept is **1.96x** the drain, so at sustained peak
//! the WAL grows ~69K rows/s forever and the system never converges under load.
//!
//! Three things could explain that, and nothing distinguished them:
//!
//!   1. **Parquet encode is synchronous CPU that never yields**, and the drain
//!      dispatches batches with `FuturesUnordered` on ONE task. That is exactly
//!      the pattern that made bin concurrency measure 1.01x and chunk
//!      pipelining hide 0% of fetch time — twice this session.
//!   2. **S3 PUT latency**, where `FuturesUnordered` genuinely does overlap and
//!      the dispatch is correct as written.
//!   3. **Iceberg's optimistic-concurrency CAS**, which serializes the actual
//!      catalog commits however they are dispatched.
//!
//! This bench discriminates (1) from (2) and (3) **locally**, which is possible
//! because the test is about whether concurrency *scales*, not about absolute
//! rates. On a local filesystem the flush is nearly free, so encode dominates
//! by construction — and that is the point: if raising
//! `SIGLAKE_DRAIN_CONCURRENCY` does not improve throughput HERE, where the work
//! is almost pure CPU, then the dispatch is serializing it and (1) is the bug.
//! If it does scale here, the dispatch is fine and the real-world gap is (2)/(3),
//! which need S3 to measure.
//!
//! What it deliberately does NOT claim: an absolute drain rate comparable to a
//! round. Local FS understates flush exactly the way it understated `input` for
//! compaction (9.6% local vs ~30% on S3).
//!
//! ```text
//! cargo test --release -p siglake-compactor --test drain_throughput -- --ignored --nocapture
//! ```
//!
//! This remains its own test binary because it compares wall-clock throughput
//! and installs a process-global metrics recorder.
//!

use std::sync::Arc;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_compactor::Compactor;
use siglake_core::Event;
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{list_sealed, WalWriter};

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

/// Sum of a histogram's recorded samples — the stage attribution is in
/// histograms, and total time per stage is what matters here, not quantiles.
fn hist_sum(snapshot: &SnapshotVec, name: &str) -> f64 {
    snapshot
        .iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .map(|(_, _, _, v)| match v {
            DebugValue::Histogram(hs) => hs.iter().map(|h| h.into_inner()).sum::<f64>(),
            _ => 0.0,
        })
        .sum()
}

fn counter_sum(snapshot: &SnapshotVec, name: &str) -> u64 {
    snapshot
        .iter()
        .filter(|(k, _, _, _)| k.key().name() == name)
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

/// Corpus-shaped rows: a realistic `raw` plus the residual `attributes` JSON,
/// so Parquet encode does the work it does in production. A trivial fixture
/// encodes almost instantly and would hide the very cost under test.
fn synth(n: usize, seed: u64) -> Vec<Event> {
    let w = |v: u64| v.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (0..n)
        .map(|i| {
            let k = seed.wrapping_add(i as u64);
            let mut e = Event::now(format!(
                "GET /api/v1/x{} 200 in {}ms service=svc{}",
                k % 8,
                k % 4000,
                k % 10
            ));
            e.host = format!("host-{:05}", k % 2000);
            e.source = format!("/var/log/svc{}.log", k % 10);
            e.sourcetype = ["debug", "info", "warn", "error"][(k % 4) as usize].into();
            e.attributes = Some(format!(
                r#"{{"agent":{{"name":"vector","id":"agent-{:05}"}},"cloud":{{"provider":"aws","az":"us-west-2a"}},"http":{{"status_code":200,"response_time_ms":{}}},"trace":{{"id":"{:016x}{:016x}","span_id":"{:016x}"}}}}"#,
                k % 5000,
                k % 4000,
                w(k),
                w(k ^ 0x5EED),
                w(k ^ 0xBEEF),
            ));
            e
        })
        .collect()
}

struct Arm {
    concurrency: usize,
    segs_per_batch: usize,
    rows: usize,
    wall_s: f64,
    read_s: f64,
    encode_s: f64,
    flush_s: f64,
    commit_attempts: u64,
    stale_base: u64,
}

async fn run_arm(
    concurrency: usize,
    segs_per_batch: usize,
    segments: usize,
    rows_per_seg: usize,
    snap: &Snapshotter,
) -> Arm {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    {
        let mut w =
            WalWriter::with_thresholds(&wal_dir, "bench", rows_per_seg, Duration::from_secs(3600))
                .unwrap();
        for s in 0..segments {
            w.append_events(&synth(rows_per_seg, (s * rows_per_seg) as u64))
                .unwrap();
        }
    }
    assert_eq!(
        list_sealed(&wal_dir).unwrap().len(),
        segments,
        "fixture must seal every segment before the drain starts"
    );

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // Segments per COMMIT. This is the second variable: the drain's cost is
    // dominated by flush + catalog commit, so how many segments ride one commit
    // matters at least as much as how many commits run concurrently. The bench
    // deploy sets 256; forcing 1 maximizes CAS pressure and is the worst case.
    let compactor = Compactor::new(&wal_dir, ice.clone())
        .with_fs_batch_limits(segs_per_batch, 0)
        .with_drain_concurrency(concurrency);

    let before = snap.snapshot().into_vec();
    let t0 = std::time::Instant::now();
    let drained = compactor.run_once().await.unwrap();
    let wall_s = t0.elapsed().as_secs_f64();
    let after = snap.snapshot().into_vec();

    assert_eq!(drained, segments, "every segment must drain in one cycle");
    assert_eq!(list_sealed(&wal_dir).unwrap().len(), 0, "WAL must be empty");

    let d = |name: &str| hist_sum(&after, name) - hist_sum(&before, name);
    let c = |name: &str| counter_sum(&after, name).saturating_sub(counter_sum(&before, name));
    Arm {
        concurrency,
        segs_per_batch,
        rows: segments * rows_per_seg,
        wall_s,
        read_s: d("siglake_compactor_segment_read_duration_seconds"),
        encode_s: d("siglake_iceberg_parquet_encode_duration_seconds"),
        flush_s: d("siglake_iceberg_data_flush_duration_seconds"),
        commit_attempts: c("siglake_iceberg_commit_attempts_total"),
        stale_base: c("siglake_iceberg_commit_stale_base_total"),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement; run with --ignored --nocapture"]
async fn report_drain_throughput_by_concurrency() {
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    let _ = metrics::set_global_recorder(recorder);

    let segments: usize = std::env::var("BENCH_SEGMENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let rows: usize = std::env::var("BENCH_ROWS_PER_SEG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000);
    let concurrencies: Vec<usize> = std::env::var("BENCH_DRAIN_CONCURRENCIES")
        .unwrap_or_else(|_| "1,2,4,8".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    println!(
        "\ndrain: {segments} segments x {rows} rows = {} rows/arm\n",
        segments * rows
    );
    let batches: Vec<usize> = std::env::var("BENCH_SEGS_PER_BATCH")
        .unwrap_or_else(|_| "1".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    println!(
        "{:>5} {:>6} {:>11} {:>8} {:>11} {:>6} {:>8} {:>8} {:>8} {:>9} {:>6}",
        "conc",
        "batch",
        "rows",
        "wall_s",
        "rows/s",
        "vs",
        "read_s",
        "encode_s",
        "flush_s",
        "cas_tries",
        "stale"
    );

    let mut baseline: Option<f64> = None;
    for &spb in &batches {
        for &conc in &concurrencies {
            let a = run_arm(conc, spb, segments, rows, &snap).await;
            let rate = a.rows as f64 / a.wall_s.max(f64::EPSILON);
            let vs = match baseline {
                Some(b) => format!("{:.2}x", rate / b),
                None => {
                    baseline = Some(rate);
                    "-".into()
                }
            };
            println!(
                "{:>5} {:>6} {:>11} {:>8.2} {:>11.0} {:>6} {:>8.2} {:>8.2} {:>8.2} {:>9} {:>6}",
                a.concurrency,
                a.segs_per_batch,
                a.rows,
                a.wall_s,
                rate,
                vs,
                a.read_s,
                a.encode_s,
                a.flush_s,
                a.commit_attempts,
                a.stale_base,
            );
        }
    }
    println!(
        "\nstage seconds are SUMMED across concurrent batches, so they exceed wall_s when\n\
         work genuinely overlaps — summed/wall IS the achieved parallelism.\n\
         If encode_s/wall_s stays ~1.0 as concurrency rises, the dispatch is serializing CPU.\n"
    );
}
