//! `siglake-loadgen` — OTLP ingest load generator.
//!
//! Spreads a target EPS rate across N workers, each running its own
//! reqwest client with HTTP keep-alive. Per-request latency is recorded
//! into an `hdrhistogram::Histogram` and periodically dumped to stderr,
//! along with a final p50/p95/p99/p99.9 summary.
//!
//! Each request is an OTLP/HTTP logs export (`POST /v1/logs`, JSON); the
//! tenant is selected by the `X-Scope-OrgID` header (`--tenant`), which the
//! target must be configured to route on.
//!
//! Usage:
//!     siglake-loadgen --target http://localhost:8088 \
//!                    --eps 5000 --duration 60s \
//!                    --workers 4 --batch-size 50

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Parser, Debug, Clone)]
#[command(about = "Siglake OTLP ingest load generator", version)]
struct Args {
    /// Ingest base URL (without the `/v1/logs` path).
    #[arg(long, default_value = "http://localhost:8088")]
    target: String,

    /// Total events per second, summed across all workers.
    #[arg(long, default_value_t = 1000)]
    eps: u64,

    /// How long to run.
    #[arg(long, default_value = "30s")]
    duration: humantime::Duration,

    /// Concurrent worker tasks. Each owns its own HTTP client + keep-alive pool.
    #[arg(long, default_value_t = 4)]
    workers: usize,

    /// Events per HTTP request body.
    #[arg(long, default_value_t = 50)]
    batch_size: usize,

    /// Optional bearer token. When set, requests carry
    /// `Authorization: Bearer <token>`.
    #[arg(long)]
    token: Option<String>,

    /// Tenant id sent as the `X-Scope-OrgID` header. Unset ⇒ the
    /// ingester's `default` tenant. A single-tenant ingester (the default)
    /// answers `403` to any other value.
    #[arg(long)]
    tenant: Option<String>,

    /// Per-line progress output interval.
    #[arg(long, default_value = "5s")]
    progress_interval: humantime::Duration,
}

#[derive(Default)]
struct Stats {
    sent: AtomicU64,
    errors: AtomicU64,
    bytes: AtomicU64,
    /// Single shared histogram, locked. The mutex is uncontended in the
    /// steady state (each worker holds it for microseconds).
    histo: Mutex<Option<Histogram<u64>>>,
    // Status-code counters. `s2xx` is the green path; `s429`
    // surfaces rate-limit throttles; `s503` surfaces backpressure
    // rejections. Anything else lands in `s_other`. Used by the
    // Phase 4.13 backpressure baseline runbook to confirm the
    // 503 path actually fires under steady-state load (a single
    // curl flood drains too fast for the bounded mpsc to fill —
    // sustained EPS over time is what surfaces it).
    s2xx: AtomicU64,
    s429: AtomicU64,
    s503: AtomicU64,
    s_other: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let duration: Duration = args.duration.into();
    let progress_interval: Duration = args.progress_interval.into();

    let stats = Arc::new(Stats {
        sent: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        histo: Mutex::new(Some(
            // Track up to 60s with 3 sig-fig precision.
            Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
        )),
        s2xx: AtomicU64::new(0),
        s429: AtomicU64::new(0),
        s503: AtomicU64::new(0),
        s_other: AtomicU64::new(0),
    });

    let started = Instant::now();
    let deadline = started + duration;

    eprintln!(
        "siglake-loadgen → {target} eps={eps} workers={w} batch={b} duration={d:?} tenant={tenant}",
        target = args.target,
        eps = args.eps,
        w = args.workers,
        b = args.batch_size,
        d = duration,
        tenant = args.tenant.as_deref().unwrap_or("default"),
    );

    // Periodic progress reporter.
    let progress_stats = stats.clone();
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(progress_interval);
        interval.tick().await; // skip the immediate tick
        let mut last_sent: u64 = 0;
        let mut last_t = Instant::now();
        while Instant::now() < deadline {
            interval.tick().await;
            let now = Instant::now();
            let sent = progress_stats.sent.load(Ordering::Relaxed);
            let errors = progress_stats.errors.load(Ordering::Relaxed);
            let bytes = progress_stats.bytes.load(Ordering::Relaxed);
            let dt = now.saturating_duration_since(last_t).as_secs_f64();
            let cur_eps = if dt > 0.0 {
                ((sent - last_sent) as f64 / dt) as u64
            } else {
                0
            };
            let elapsed = now.saturating_duration_since(started).as_secs_f64();
            let (p50, p95, p99) = {
                let h = progress_stats.histo.lock().await;
                if let Some(h) = h.as_ref() {
                    (
                        h.value_at_quantile(0.50),
                        h.value_at_quantile(0.95),
                        h.value_at_quantile(0.99),
                    )
                } else {
                    (0, 0, 0)
                }
            };
            let s429 = progress_stats.s429.load(Ordering::Relaxed);
            let s503 = progress_stats.s503.load(Ordering::Relaxed);
            eprintln!(
                "[+{:>5.1}s] sent={} eps={} bytes={} err={} 429={} 503={} latency p50={}us p95={}us p99={}us",
                elapsed, sent, cur_eps, bytes, errors, s429, s503, p50, p95, p99,
            );
            last_sent = sent;
            last_t = now;
        }
    });

    // Worker tasks.
    let per_worker_eps = (args.eps as f64) / (args.workers as f64);
    let batches_per_sec = per_worker_eps / (args.batch_size as f64);
    let interval = if batches_per_sec > 0.0 {
        Duration::from_secs_f64(1.0 / batches_per_sec)
    } else {
        Duration::from_millis(1)
    };

    let mut handles = Vec::with_capacity(args.workers);
    for w in 0..args.workers {
        let args = args.clone();
        let stats = stats.clone();
        handles.push(tokio::spawn(async move {
            run_worker(w, args, stats, interval, deadline).await
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    let _ = progress_handle.await;

    // Final summary.
    let elapsed = started.elapsed().as_secs_f64();
    let sent = stats.sent.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    let bytes = stats.bytes.load(Ordering::Relaxed);
    let actual_eps = if elapsed > 0.0 {
        sent as f64 / elapsed
    } else {
        0.0
    };
    let (p50, p95, p99, p999, max) = {
        let h = stats.histo.lock().await;
        let h = h.as_ref().unwrap();
        (
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.95),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.max(),
        )
    };
    let s2xx = stats.s2xx.load(Ordering::Relaxed);
    let s429 = stats.s429.load(Ordering::Relaxed);
    let s503 = stats.s503.load(Ordering::Relaxed);
    let s_other = stats.s_other.load(Ordering::Relaxed);
    eprintln!();
    eprintln!("==== siglake-loadgen summary ====");
    eprintln!("  elapsed:        {:.2}s", elapsed);
    eprintln!("  events sent:    {}", sent);
    eprintln!("  achieved EPS:   {:.0}", actual_eps);
    eprintln!("  bytes sent:     {}", bytes);
    eprintln!("  HTTP 2xx:       {s2xx}");
    eprintln!("  HTTP 429:       {s429}    (rate limit)");
    eprintln!("  HTTP 503:       {s503}    (backpressure)");
    eprintln!("  HTTP other:     {s_other}");
    eprintln!("  errors:         {} (transport+other)", errors);
    eprintln!("  latency (us):   p50={p50} p95={p95} p99={p99} p999={p999} max={max}");
    Ok(())
}

async fn run_worker(
    worker_id: usize,
    args: Args,
    stats: Arc<Stats>,
    interval: Duration,
    deadline: Instant,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(8)
        .timeout(Duration::from_secs(10))
        .build()
        .context("building reqwest client")?;

    let logs_url = format!("{}/v1/logs", args.target.trim_end_matches('/'));

    let mut next_send = Instant::now();
    let mut counter: u64 = (worker_id as u64) * 1_000_000_000;
    let mut rng = SmallRng::from_entropy();

    while Instant::now() < deadline {
        // Pace.
        let now = Instant::now();
        if now < next_send {
            sleep(next_send - now).await;
        }
        next_send += interval;

        // Build an OTLP/HTTP logs export envelope for this batch.
        let n = args.batch_size.max(1);
        let body = synth_otlp_batch(n, counter, &mut rng);
        counter = counter.wrapping_add(n as u64);

        let body_len = body.len() as u64;
        let req_start = Instant::now();
        let resp = {
            let mut req = client
                .post(&logs_url)
                .header("Content-Type", "application/json");
            if let Some(token) = args.token.as_deref() {
                req = req.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(tenant) = args.tenant.as_deref() {
                req = req.header("X-Scope-OrgID", tenant);
            }
            req.body(body).send().await
        };
        let elapsed_us = req_start.elapsed().as_micros() as u64;

        let mut h = stats.histo.lock().await;
        if let Some(h) = h.as_mut() {
            h.record(elapsed_us.max(1)).ok();
        }
        drop(h);

        match resp {
            Ok(r) => {
                let status = r.status().as_u16();
                match status {
                    200..=299 => {
                        stats.s2xx.fetch_add(1, Ordering::Relaxed);
                        stats.sent.fetch_add(n as u64, Ordering::Relaxed);
                        stats.bytes.fetch_add(body_len, Ordering::Relaxed);
                    }
                    429 => {
                        // Rate-limit throttle. Don't count toward
                        // `sent` (no events were accepted) but
                        // also don't count as a transport error.
                        stats.s429.fetch_add(1, Ordering::Relaxed);
                    }
                    503 => {
                        // Backpressure rejection — same reasoning
                        // as 429 for `sent`.
                        stats.s503.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        stats.s_other.fetch_add(1, Ordering::Relaxed);
                        stats.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(_) => {
                // Transport failure (timeout, conn reset, etc).
                stats.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

const HOSTS: &[&str] = &[
    "host-0", "host-1", "host-2", "host-3", "host-4", "host-5", "host-6", "host-7",
];
const SOURCETYPES: &[&str] = &["app:json", "syslog", "nginx:access", "k8s:audit"];
const STATUSES: &[u16] = &[200, 200, 200, 200, 200, 301, 400, 404, 500, 503];

/// Build one OTLP/HTTP `ExportLogsServiceRequest` (JSON) carrying `batch`
/// log records. Each record gets its own `resourceLogs` entry so per-event
/// `host`/`source` vary (resource attributes apply to the whole resource);
/// `sourcetype`/`index` ride as record attributes and the synthetic line is
/// the record `body`. Mirrors the ingester's OTLP→column mapping
/// (`host.name`→host, `service.name`→source, body→raw).
fn synth_otlp_batch(batch: usize, base_seq: u64, rng: &mut SmallRng) -> String {
    let mut resource_logs = Vec::with_capacity(batch);
    for i in 0..batch {
        let host = HOSTS.choose(rng).unwrap();
        let st = SOURCETYPES.choose(rng).unwrap();
        let status = STATUSES.choose(rng).unwrap();
        let latency = rng.gen_range(1..500);
        let seq = base_seq + i as u64;
        let raw = format!(
            "seq={seq} status={status} latency_ms={latency} path=/api/v1/items?id={}",
            rng.gen_range(1..10000),
        );
        resource_logs.push(serde_json::json!({
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": host } },
                { "key": "service.name", "value": { "stringValue": "loadgen" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "siglake-loadgen" },
                "logRecords": [{
                    "body": { "stringValue": raw },
                    "attributes": [
                        { "key": "sourcetype", "value": { "stringValue": st } },
                        { "key": "index", "value": { "stringValue": "main" } }
                    ]
                }]
            }]
        }));
    }
    serde_json::to_string(&serde_json::json!({ "resourceLogs": resource_logs })).unwrap()
}
