//! `siglake-corpus` — generate + verify a deterministic soak-test
//! dataset for siglake.
//!
//! See `siglake_loadgen::corpus` module docs for the design.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::{Parser, Subcommand};
use serde_json::Value;
use tokio::task::JoinSet;

use siglake_loadgen::corpus::{generate, read_manifest, ExpectedResult, QueryCase};

#[derive(Parser, Debug)]
#[command(name = "siglake-corpus", about, version)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate `events.ndjson` + `queries.json` + `manifest.json`
    /// into `--out`. Same `(seed, events, start-time, window-hours)`
    /// always produces the same bytes — generate once, stash the
    /// directory anywhere, reuse forever.
    Generate {
        /// Output directory. Created if missing.
        #[arg(long)]
        out: PathBuf,
        /// Anchor for the per-event deterministic RNG.
        #[arg(long, default_value_t = 42)]
        seed: u64,
        /// Event count.
        #[arg(long)]
        events: u64,
        /// Window start (RFC3339). Events are spread uniformly across
        /// `[start, start + window-hours)`.
        #[arg(long, default_value = "2026-01-01T00:00:00Z")]
        start_time: String,
        /// Time window the events span, in whole hours.
        #[arg(long, default_value_t = 24)]
        window_hours: u32,
    },

    /// Load a corpus's `events.ndjson` into an OTLP ingester. Streams
    /// the file line-by-line, transforms each event to an OTLP log
    /// record, and batches them into `POST /v1/logs` requests at the
    /// configured rate.
    ///
    /// The exit status reflects success of the load — not of query
    /// verification. Run `verify` afterwards.
    Load {
        #[arg(long)]
        corpus: PathBuf,
        /// Ingest base URL.
        #[arg(long, default_value = "http://localhost:8088")]
        target: String,
        /// Bearer token, if the target requires auth.
        #[arg(long)]
        token: Option<String>,
        /// Tenant id sent as `X-Scope-OrgID` (unset ⇒ `default`). A
        /// single-tenant ingester answers `403` to any other value.
        #[arg(long)]
        tenant: Option<String>,
        /// Events per HTTP request.
        #[arg(long, default_value_t = 500)]
        batch_size: usize,
        /// Concurrent in-flight requests.
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },

    /// Run every query in `queries.json` against a siglake-query-server
    /// and compare to expected. Exit 0 if every assertion passes,
    /// non-zero otherwise.
    Verify {
        #[arg(long)]
        corpus: PathBuf,
        /// query-server base URL.
        #[arg(long, default_value = "http://localhost:8089")]
        query_target: String,
        /// Bearer token, if the target requires auth.
        #[arg(long)]
        token: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    match cli.cmd {
        Command::Generate {
            out,
            seed,
            events,
            start_time,
            window_hours,
        } => {
            let start = parse_rfc3339(&start_time)?;
            tracing::info!(
                out = %out.display(),
                seed,
                events,
                start_time = %start,
                window_hours,
                "generating corpus"
            );
            let started = std::time::Instant::now();
            let manifest = generate(&out, seed, events, start, window_hours)?;
            tracing::info!(
                elapsed_secs = started.elapsed().as_secs_f64(),
                bytes = manifest.events_ndjson_bytes,
                "corpus generated"
            );
            println!(
                "wrote {} events ({:.2} MB) to {}",
                manifest.events,
                manifest.events_ndjson_bytes as f64 / 1e6,
                out.display()
            );
        }
        Command::Load {
            corpus,
            target,
            token,
            tenant,
            batch_size,
            concurrency,
        } => {
            load_command(
                &corpus,
                &target,
                token.as_deref(),
                tenant.as_deref(),
                batch_size,
                concurrency,
            )
            .await?;
        }
        Command::Verify {
            corpus,
            query_target,
            token,
        } => {
            verify_command(&corpus, &query_target, token.as_deref()).await?;
        }
    }
    Ok(())
}

fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>> {
    let dt = DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("--start-time {s:?} is not valid RFC3339"))?;
    Ok(Utc.from_utc_datetime(&dt.naive_utc()))
}

async fn load_command(
    corpus: &std::path::Path,
    target: &str,
    token: Option<&str>,
    tenant: Option<&str>,
    batch_size: usize,
    concurrency: usize,
) -> Result<()> {
    let (manifest, _queries, events_path) = read_manifest(corpus)?;
    tracing::info!(
        events = manifest.events,
        bytes = manifest.events_ndjson_bytes,
        target,
        "loading corpus"
    );

    let url = format!("{}/v1/logs", target.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .timeout(Duration::from_secs(30))
        .build()?;
    let file = std::fs::File::open(&events_path)
        .with_context(|| format!("open {}", events_path.display()))?;
    let reader = BufReader::new(file);

    // Keep only `concurrency` active requests at a time and
    // retire completed work as we stream the input file.
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut active = JoinSet::new();
    let mut batch: Vec<String> = Vec::with_capacity(batch_size);
    let mut queued_events: u64 = 0;
    let mut completed_events: u64 = 0;
    let started = std::time::Instant::now();

    for line in reader.lines() {
        let line = line.context("read events.ndjson")?;
        // Transform each corpus line into an OTLP resourceLogs entry.
        batch.push(line_to_resource_log(&line)?);
        if batch.len() >= batch_size {
            let body = otlp_envelope(&batch);
            let n = batch.len();
            batch.clear();
            if active.len() >= concurrency {
                completed_events += drain_one_completed(&mut active).await?;
            }
            let sem = sem.clone();
            let client = client.clone();
            let url = url.clone();
            let token = token.map(str::to_string);
            let tenant = tenant.map(str::to_string);
            active.spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                send_batch(&client, &url, &body, token.as_deref(), tenant.as_deref()).await?;
                Ok::<u64, anyhow::Error>(n as u64)
            });
            queued_events += n as u64;
            if queued_events.is_multiple_of(100_000) {
                let elapsed = started.elapsed().as_secs_f64();
                let eps = queued_events as f64 / elapsed.max(0.001);
                tracing::info!(
                    sent = queued_events,
                    completed = completed_events,
                    elapsed_secs = elapsed,
                    eps,
                    "load progress"
                );
            }
        }
    }
    if !batch.is_empty() {
        let body = otlp_envelope(&batch);
        let n = batch.len();
        if active.len() >= concurrency {
            completed_events += drain_one_completed(&mut active).await?;
        }
        let sem = sem.clone();
        let client = client.clone();
        let url = url.clone();
        let token = token.map(str::to_string);
        let tenant = tenant.map(str::to_string);
        active.spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            send_batch(&client, &url, &body, token.as_deref(), tenant.as_deref()).await?;
            Ok::<u64, anyhow::Error>(n as u64)
        });
        queued_events += n as u64;
    }

    let mut errors = 0;
    while let Some(result) = active.join_next().await {
        match result {
            Ok(Ok(n)) => completed_events += n,
            Ok(Err(e)) => {
                errors += 1;
                tracing::error!(error = %e, "batch failed");
            }
            Err(e) => {
                errors += 1;
                tracing::error!(error = %e, "task panicked");
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let eps = queued_events as f64 / elapsed.max(0.001);
    tracing::info!(
        sent = queued_events,
        completed = completed_events,
        errors,
        elapsed_secs = elapsed,
        eps,
        "load complete"
    );
    if errors > 0 {
        anyhow::bail!("{errors} batches failed during load");
    }
    if completed_events != queued_events {
        anyhow::bail!(
            "load incomplete: queued {queued_events} events but only completed {completed_events}"
        );
    }
    Ok(())
}

async fn drain_one_completed(active: &mut JoinSet<Result<u64>>) -> Result<u64> {
    match active.join_next().await {
        Some(Ok(Ok(n))) => Ok(n),
        Some(Ok(Err(e))) => Err(e),
        Some(Err(e)) => Err(anyhow!("task panicked: {e}")),
        None => Ok(0),
    }
}

async fn send_batch(
    client: &reqwest::Client,
    url: &str,
    body: &str,
    token: Option<&str>,
    tenant: Option<&str>,
) -> Result<()> {
    let mut req = client.post(url).header("Content-Type", "application/json");
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    if let Some(t) = tenant {
        req = req.header("X-Scope-OrgID", t);
    }
    let resp = req.body(body.to_string()).send().await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("OTLP POST {status}: {body}");
    }
    Ok(())
}

/// One corpus line (`events.ndjson` is a neutral on-disk format; the wire
/// protocol is OTLP).
#[derive(serde::Deserialize)]
struct CorpusLine {
    #[serde(default)]
    time: f64,
    host: String,
    source: String,
    sourcetype: String,
    index: String,
    event: String,
}

/// Transform a corpus line into a serialized OTLP `resourceLogs` entry,
/// matching the ingester's OTLP→column mapping (`host.name`→host,
/// `service.name`→source, `event`→body, `time`→`timeUnixNano`).
fn line_to_resource_log(line: &str) -> Result<String> {
    let e: CorpusLine =
        serde_json::from_str(line).with_context(|| format!("parse corpus line: {line}"))?;
    let nanos = (e.time * 1e9) as u64;
    let v = serde_json::json!({
        "resource": { "attributes": [
            { "key": "host.name", "value": { "stringValue": e.host } },
            { "key": "service.name", "value": { "stringValue": e.source } }
        ]},
        "scopeLogs": [{
            "scope": { "name": "siglake-corpus" },
            "logRecords": [{
                "timeUnixNano": nanos.to_string(),
                "body": { "stringValue": e.event },
                "attributes": [
                    { "key": "sourcetype", "value": { "stringValue": e.sourcetype } },
                    { "key": "index", "value": { "stringValue": e.index } }
                ]
            }]
        }]
    });
    Ok(v.to_string())
}

/// Wrap pre-serialized `resourceLogs` entries into one OTLP export envelope.
fn otlp_envelope(entries: &[String]) -> String {
    format!(r#"{{"resourceLogs":[{}]}}"#, entries.join(","))
}

async fn verify_command(
    corpus: &std::path::Path,
    query_target: &str,
    token: Option<&str>,
) -> Result<()> {
    let (manifest, queries, _events_path) = read_manifest(corpus)?;
    tracing::info!(
        seed = manifest.seed,
        events = manifest.events,
        distribution_version = manifest.distribution_version,
        target = query_target,
        "verifying corpus"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()?;
    let mut failed: Vec<String> = Vec::new();
    for q in &queries {
        let outcome = run_query_case(&client, query_target, token, q).await;
        match outcome {
            Ok(()) => println!("  ok  {} — {}", q.id, q.description),
            Err(e) => {
                println!("  FAIL {} — {}: {:#}", q.id, q.description, e);
                failed.push(q.id.clone());
            }
        }
    }
    if failed.is_empty() {
        println!("\nall {} queries match expected.", queries.len());
        Ok(())
    } else {
        anyhow::bail!(
            "{} of {} queries failed: {:?}",
            failed.len(),
            queries.len(),
            failed
        );
    }
}

async fn run_query_case(
    client: &reqwest::Client,
    target: &str,
    token: Option<&str>,
    q: &QueryCase,
) -> Result<()> {
    let url = format!("{}/api/v1/sql", target.trim_end_matches('/'));
    let mut req = client.post(&url).header("Content-Type", "application/json");
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    let body = serde_json::json!({"query": q.sql, "format": "records"});
    let resp = req.json(&body).send().await.context("POST /api/v1/sql")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("query-server {status}: {body}");
    }
    let body: Value = resp.json().await.context("parse JSON response")?;
    let rows = body
        .get("rows")
        .and_then(|r| r.as_array())
        .ok_or_else(|| anyhow!("response missing `rows`: {body}"))?;
    match &q.expected {
        ExpectedResult::Count { value } => {
            let actual = rows
                .first()
                .and_then(|r| r.get("n"))
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("response row missing `n`: {body}"))?;
            if actual != *value {
                anyhow::bail!("count mismatch: expected {value}, got {actual}");
            }
        }
        ExpectedResult::GroupedCount { rows: expected } => {
            let actual: Vec<(String, u64)> = rows
                .iter()
                .map(|r| {
                    let key = r
                        .as_object()
                        .and_then(|o| o.iter().find(|(k, _)| *k != "n").map(|(_, v)| v))
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("response row missing group key: {r}"))?;
                    let count = r
                        .get("n")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| anyhow!("response row missing `n`: {r}"))?;
                    Ok::<_, anyhow::Error>((key.to_string(), count))
                })
                .collect::<Result<Vec<_>>>()?;
            if actual != *expected {
                anyhow::bail!(
                    "grouped-count mismatch:\n  expected: {expected:?}\n  got:      {actual:?}"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Clone)]
    struct TestState {
        batches: Arc<AtomicUsize>,
        events: Arc<AtomicUsize>,
    }

    async fn serve_otlp(listener: tokio::net::TcpListener, state: TestState) {
        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let state = state.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 8192];
                let header_end = loop {
                    let n = socket.read(&mut tmp).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                let header_text = String::from_utf8_lossy(&buf[..header_end]);
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        let lower = line.to_ascii_lowercase();
                        lower
                            .strip_prefix("content-length:")
                            .and_then(|v| v.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while buf.len() < header_end + content_length {
                    let n = socket.read(&mut tmp).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let body = &buf[header_end..header_end + content_length];
                // OTLP export envelope: count log records across all resourceLogs.
                let payload: Value = serde_json::from_slice(body).unwrap();
                let n_records: usize = payload["resourceLogs"]
                    .as_array()
                    .map(|rls| {
                        rls.iter()
                            .map(|rl| {
                                rl["scopeLogs"]
                                    .as_array()
                                    .map(|sls| {
                                        sls.iter()
                                            .map(|sl| {
                                                sl["logRecords"].as_array().map_or(0, |r| r.len())
                                            })
                                            .sum::<usize>()
                                    })
                                    .unwrap_or(0)
                            })
                            .sum()
                    })
                    .unwrap_or(0);
                state.batches.fetch_add(1, Ordering::Relaxed);
                state.events.fetch_add(n_records, Ordering::Relaxed);
                let response_body = br#"{"text":"Success","code":0}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.write_all(response_body).await.unwrap();
                let _ = socket.shutdown().await;
            });
        }
    }

    #[tokio::test]
    async fn load_command_completes_all_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        generate(tmp.path(), 42, 2_500, start, 24).unwrap();

        let state = TestState {
            batches: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(AtomicUsize::new(0)),
        };
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("bind loopback listener: {err}"),
        };
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_otlp(listener, state.clone()));

        load_command(tmp.path(), &format!("http://{addr}"), None, None, 500, 4)
            .await
            .unwrap();

        assert_eq!(state.events.load(Ordering::Relaxed), 2_500);
        assert_eq!(state.batches.load(Ordering::Relaxed), 5);

        server.abort();
    }
}
