//! Soak-test corpora: deterministic event datasets plus curated query suites
//! with computed expected results.
//!
//! The original `classic` corpus remains unchanged and keeps its legacy
//! `events.ndjson` + `queries.json` contract. An additive `otel-rich`
//! profile that keeps the same on-disk NDJSON artifact for compatibility while
//! also exposing an in-memory OTLP-shaped batch iterator for the benchmark
//! harness.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, SecondsFormat, TimeZone, Utc};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

pub const DISTRIBUTION_VERSION: u32 = 1;
pub const OTEL_RICH_DISTRIBUTION_VERSION: u32 = 2;
pub const OTEL_RICH_MIN_EVENTS: u64 = 111_860;
pub const OTEL_RICH_DEFAULT_WINDOW_HOURS: u32 = 24 * 14;
const OTEL_RICH_ATTRS_PER_EVENT: usize = 48;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum CorpusProfile {
    #[default]
    Classic,
    OtelRich,
}

/// Fixed host pool. 16 hosts so `group by host` returns a predictable
/// cardinality regardless of dataset size.
pub const HOSTS: &[&str] = &[
    "web-01.prod.example.com",
    "web-02.prod.example.com",
    "web-03.prod.example.com",
    "web-04.prod.example.com",
    "web-05.prod.example.com",
    "web-06.prod.example.com",
    "web-07.prod.example.com",
    "web-08.prod.example.com",
    "api-01.prod.example.com",
    "api-02.prod.example.com",
    "api-03.prod.example.com",
    "api-04.prod.example.com",
    "auth-01.prod.example.com",
    "auth-02.prod.example.com",
    "auth-03.prod.example.com",
    "auth-04.prod.example.com",
];

pub const SOURCETYPES: &[&str] = &["nginx:access", "app:api", "auth:syslog"];

/// `(sourcetype, mix_weight)`. Weights sum to 100 — that's the per-100 modulus
/// the index uses to pick a shape.
const SHAPE_MIX: &[(&str, u32)] = &[("nginx:access", 60), ("app:api", 35), ("auth:syslog", 5)];

const URL_PATHS: &[&str] = &[
    "/",
    "/index.html",
    "/api/v1/users",
    "/api/v1/items",
    "/api/v1/orders",
    "/api/v1/items/search",
    "/healthz",
    "/metrics",
    "/login",
    "/logout",
];

const API_ENDPOINTS: &[&str] = &[
    "/api/v1/items",
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/items/search",
    "/api/v1/sessions",
];

const HTTP_METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];

const AUTH_ACTIONS: &[&str] = &["login", "logout", "sudo"];

const OTEL_SERVICES: &[&str] = &[
    "frontend-gateway",
    "checkout-api",
    "inventory-api",
    "billing-worker",
    "auth-service",
    "search-api",
    "session-cache",
    "reporting-worker",
];

const OTEL_SCOPES: &[&str] = &[
    "otel-rich/bench",
    "otel-rich/vector",
    "otel-rich/fluentbit",
    "otel-rich/sdk-rust",
];

const STRING_VALUE_POOLS: &[&[&str]] = &[
    &["gold", "silver", "bronze", "shadow"],
    &["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
    &["payments", "checkout", "catalog", "auth", "prod", "staging"],
    &["k8s-prod-a", "k8s-prod-b", "k8s-stage-a", "k8s-dr-a"],
    &["web", "api", "worker", "cron"],
    &["enabled", "disabled"],
    &["linux", "darwin", "windows"],
    &["amd64", "arm64"],
];

const STRING_KEYS: [&str; 64] = [
    "service.tier",
    "cloud.region",
    "k8s.namespace.name",
    "k8s.cluster.name",
    "deployment.environment",
    "service.group",
    "host.os.name",
    "host.arch",
    "cloud.availability_zone",
    "service.version",
    "http.route",
    "db.system",
    "messaging.system",
    "feature.flag",
    "tenant.plan",
    "runtime.name",
    "attr.str.016",
    "attr.str.017",
    "attr.str.018",
    "attr.str.019",
    "attr.str.020",
    "attr.str.021",
    "attr.str.022",
    "attr.str.023",
    "attr.str.024",
    "attr.str.025",
    "attr.str.026",
    "attr.str.027",
    "attr.str.028",
    "attr.str.029",
    "attr.str.030",
    "attr.str.031",
    "attr.str.032",
    "attr.str.033",
    "attr.str.034",
    "attr.str.035",
    "attr.str.036",
    "attr.str.037",
    "attr.str.038",
    "attr.str.039",
    "attr.str.040",
    "attr.str.041",
    "attr.str.042",
    "attr.str.043",
    "attr.str.044",
    "attr.str.045",
    "attr.str.046",
    "attr.str.047",
    "attr.str.048",
    "attr.str.049",
    "attr.str.050",
    "attr.str.051",
    "attr.str.052",
    "attr.str.053",
    "attr.str.054",
    "attr.str.055",
    "attr.str.056",
    "attr.str.057",
    "attr.str.058",
    "attr.str.059",
    "attr.str.060",
    "attr.str.061",
    "attr.str.062",
    "attr.str.063",
];

const ID_KEYS: [&str; 64] = [
    "request_id",
    "user_id",
    "trace_id",
    "session_id",
    "customer_id",
    "cart_id",
    "order_id",
    "device_id",
    "pod_uid",
    "node_uid",
    "cluster_uid",
    "tenant_id",
    "attr.id.012",
    "attr.id.013",
    "attr.id.014",
    "attr.id.015",
    "attr.id.016",
    "attr.id.017",
    "attr.id.018",
    "attr.id.019",
    "attr.id.020",
    "attr.id.021",
    "attr.id.022",
    "attr.id.023",
    "attr.id.024",
    "attr.id.025",
    "attr.id.026",
    "attr.id.027",
    "attr.id.028",
    "attr.id.029",
    "attr.id.030",
    "attr.id.031",
    "attr.id.032",
    "attr.id.033",
    "attr.id.034",
    "attr.id.035",
    "attr.id.036",
    "attr.id.037",
    "attr.id.038",
    "attr.id.039",
    "attr.id.040",
    "attr.id.041",
    "attr.id.042",
    "attr.id.043",
    "attr.id.044",
    "attr.id.045",
    "attr.id.046",
    "attr.id.047",
    "attr.id.048",
    "attr.id.049",
    "attr.id.050",
    "attr.id.051",
    "attr.id.052",
    "attr.id.053",
    "attr.id.054",
    "attr.id.055",
    "attr.id.056",
    "attr.id.057",
    "attr.id.058",
    "attr.id.059",
    "attr.id.060",
    "attr.id.061",
    "attr.id.062",
    "attr.id.063",
];

const INT_KEYS: [&str; 64] = [
    "latency_ms",
    "status",
    "retry_count",
    "http.response_content_length",
    "db.rows_returned",
    "cache.entries_scanned",
    "cpu.millicores",
    "memory.rss_mib",
    "payload.bytes",
    "k8s.container.restart_count",
    "attr.int.010",
    "attr.int.011",
    "attr.int.012",
    "attr.int.013",
    "attr.int.014",
    "attr.int.015",
    "attr.int.016",
    "attr.int.017",
    "attr.int.018",
    "attr.int.019",
    "attr.int.020",
    "attr.int.021",
    "attr.int.022",
    "attr.int.023",
    "attr.int.024",
    "attr.int.025",
    "attr.int.026",
    "attr.int.027",
    "attr.int.028",
    "attr.int.029",
    "attr.int.030",
    "attr.int.031",
    "attr.int.032",
    "attr.int.033",
    "attr.int.034",
    "attr.int.035",
    "attr.int.036",
    "attr.int.037",
    "attr.int.038",
    "attr.int.039",
    "attr.int.040",
    "attr.int.041",
    "attr.int.042",
    "attr.int.043",
    "attr.int.044",
    "attr.int.045",
    "attr.int.046",
    "attr.int.047",
    "attr.int.048",
    "attr.int.049",
    "attr.int.050",
    "attr.int.051",
    "attr.int.052",
    "attr.int.053",
    "attr.int.054",
    "attr.int.055",
    "attr.int.056",
    "attr.int.057",
    "attr.int.058",
    "attr.int.059",
    "attr.int.060",
    "attr.int.061",
    "attr.int.062",
    "attr.int.063",
];

const FLOAT_KEYS: [&str; 32] = [
    "cpu.utilization",
    "memory.utilization",
    "cache.hit_ratio",
    "queue.depth_ratio",
    "db.query_cost",
    "network.rtt_ms",
    "temperature.celsius",
    "attr.float.007",
    "attr.float.008",
    "attr.float.009",
    "attr.float.010",
    "attr.float.011",
    "attr.float.012",
    "attr.float.013",
    "attr.float.014",
    "attr.float.015",
    "attr.float.016",
    "attr.float.017",
    "attr.float.018",
    "attr.float.019",
    "attr.float.020",
    "attr.float.021",
    "attr.float.022",
    "attr.float.023",
    "attr.float.024",
    "attr.float.025",
    "attr.float.026",
    "attr.float.027",
    "attr.float.028",
    "attr.float.029",
    "attr.float.030",
    "attr.float.031",
];

const BOOL_KEYS: [&str; 32] = [
    "error.flag",
    "cache.hit",
    "auth.success",
    "cold.start",
    "sampled",
    "rate_limited",
    "feature.alpha",
    "feature.beta",
    "attr.bool.008",
    "attr.bool.009",
    "attr.bool.010",
    "attr.bool.011",
    "attr.bool.012",
    "attr.bool.013",
    "attr.bool.014",
    "attr.bool.015",
    "attr.bool.016",
    "attr.bool.017",
    "attr.bool.018",
    "attr.bool.019",
    "attr.bool.020",
    "attr.bool.021",
    "attr.bool.022",
    "attr.bool.023",
    "attr.bool.024",
    "attr.bool.025",
    "attr.bool.026",
    "attr.bool.027",
    "attr.bool.028",
    "attr.bool.029",
    "attr.bool.030",
    "attr.bool.031",
];

/// Status-code distribution within `nginx:access` shape. Indices are per-100;
/// e.g. `2..=94` maps to 200.
fn nginx_status_for(per_100: u32) -> u16 {
    match per_100 {
        0..=2 => 304,
        3..=94 => 200,
        95..=96 => 404,
        97 => 400,
        98 => 503,
        _ => 500,
    }
}

/// Materialize a single corpus event for the legacy profile. Pure: same
/// `(index, seed, start_time, window_secs)` always produces the same event.
pub fn event_for(
    index: u64,
    seed: u64,
    start_time: DateTime<Utc>,
    window_secs: u64,
) -> CorpusEvent {
    event_for_profile(index, seed, start_time, window_secs, CorpusProfile::Classic)
}

pub fn event_for_profile(
    index: u64,
    seed: u64,
    start_time: DateTime<Utc>,
    window_secs: u64,
    profile: CorpusProfile,
) -> CorpusEvent {
    match profile {
        CorpusProfile::Classic => classic_event_for(index, seed, start_time, window_secs),
        CorpusProfile::OtelRich => otel_rich_event(index, seed, start_time, window_secs).corpus,
    }
}

enum Shape {
    NginxAccess,
    AppApi,
    AuthSyslog,
}

fn pick_shape(per_100: u32) -> Shape {
    let mut acc = 0u32;
    for &(name, weight) in SHAPE_MIX {
        acc += weight;
        if per_100 < acc {
            return match name {
                "nginx:access" => Shape::NginxAccess,
                "app:api" => Shape::AppApi,
                "auth:syslog" => Shape::AuthSyslog,
                _ => unreachable!(),
            };
        }
    }
    Shape::AuthSyslog
}

/// In-memory shape of one corpus event. The legacy profile maps 1:1 to a HEC
/// `/event` payload. The `otel-rich` profile reuses the same file shape for
/// compatibility and fills `attributes` with a deterministic JSON object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorpusEvent {
    #[serde(rename = "time", serialize_with = "serialize_epoch_secs")]
    pub timestamp: DateTime<Utc>,
    pub host: String,
    pub source: String,
    pub sourcetype: String,
    #[serde(rename = "index")]
    pub index_name: String,
    #[serde(rename = "event")]
    pub raw: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<Value>,
}

fn serialize_epoch_secs<S: serde::Serializer>(
    ts: &DateTime<Utc>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_f64(ts.timestamp() as f64 + (ts.timestamp_subsec_nanos() as f64) / 1e9)
}

/// Stable header at the top of the artifact directory. Carries every input the
/// generator needs to reproduce the dataset bit-for-bit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub distribution_version: u32,
    #[serde(default)]
    pub profile: CorpusProfile,
    pub seed: u64,
    pub events: u64,
    pub start_time: DateTime<Utc>,
    pub window_hours: u32,
    pub events_ndjson_bytes: u64,
}

/// One verification entry. The legacy corpus stores representative SQL / SPL
/// alongside the expected result. The benchmark harness keys off `id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCase {
    pub id: String,
    pub description: String,
    pub sql: String,
    pub spl: String,
    pub expected: ExpectedResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExpectedResult {
    Count { value: u64 },
    GroupedCount { rows: Vec<(String, u64)> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpBatch {
    pub resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpResourceLogs {
    pub resource_attributes: Vec<OtlpAttribute>,
    pub scope_name: String,
    pub log_records: Vec<OtlpLogRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpLogRecord {
    pub time_unix_nano: u64,
    pub body: String,
    pub attributes: Vec<OtlpAttribute>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OtlpAttribute {
    pub key: String,
    pub value: OtlpValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OtlpValue {
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
}

pub struct OtlpBatchIter {
    manifest: Manifest,
    batch_size: usize,
    next_index: u64,
}

impl Iterator for OtlpBatchIter {
    type Item = OtlpBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.manifest.events {
            return None;
        }
        let end = (self.next_index + self.batch_size as u64).min(self.manifest.events);
        let mut records = Vec::with_capacity((end - self.next_index) as usize);
        for index in self.next_index..end {
            records.push(otlp_resource_log_for(
                index,
                self.manifest.seed,
                self.manifest.start_time,
                self.manifest.window_hours * 3600,
                self.manifest.profile,
            ));
        }
        self.next_index = end;
        Some(OtlpBatch {
            resource_logs: records,
        })
    }
}

pub fn otlp_batches(manifest: &Manifest, batch_size: usize) -> Result<OtlpBatchIter> {
    if batch_size == 0 {
        anyhow::bail!("OTLP batch size must be > 0");
    }
    Ok(OtlpBatchIter {
        manifest: manifest.clone(),
        batch_size,
        next_index: 0,
    })
}

/// Generation entry point for the legacy profile.
pub fn generate(
    out_dir: impl AsRef<Path>,
    seed: u64,
    events: u64,
    start_time: DateTime<Utc>,
    window_hours: u32,
) -> Result<Manifest> {
    generate_with_profile(
        out_dir,
        seed,
        events,
        start_time,
        window_hours,
        CorpusProfile::Classic,
    )
}

/// Generation entry point with an explicit profile.
pub fn generate_with_profile(
    out_dir: impl AsRef<Path>,
    seed: u64,
    events: u64,
    start_time: DateTime<Utc>,
    window_hours: u32,
    profile: CorpusProfile,
) -> Result<Manifest> {
    let out_dir = out_dir.as_ref();
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("creating corpus dir {}", out_dir.display()))?;
    let events_path = out_dir.join("events.ndjson");
    let queries_path = out_dir.join("queries.json");
    let manifest_path = out_dir.join("manifest.json");

    let window_secs = (window_hours as u64) * 3600;
    if window_secs == 0 {
        anyhow::bail!("--window-hours must be > 0");
    }
    if profile == CorpusProfile::OtelRich && events < OTEL_RICH_MIN_EVENTS {
        anyhow::bail!(
            "otel-rich requires at least {OTEL_RICH_MIN_EVENTS} events so its exact-count needles fit"
        );
    }

    let mut writer = BufWriter::with_capacity(
        4 * 1024 * 1024,
        File::create(&events_path).with_context(|| format!("create {}", events_path.display()))?,
    );

    let mut classic_agg = (profile == CorpusProfile::Classic).then(ClassicAggregator::default);
    let mut otel_agg =
        (profile == CorpusProfile::OtelRich).then(|| OtelRichAggregator::new(window_hours));
    let mut total_bytes: u64 = 0;

    for i in 0..events {
        let ev = event_for_profile(i, seed, start_time, window_secs, profile);
        if let Some(agg) = classic_agg.as_mut() {
            agg.observe(&ev);
        }
        if let Some(agg) = otel_agg.as_mut() {
            agg.observe(&ev);
        }
        let line = serde_json::to_string(&ev).expect("CorpusEvent serializes");
        writer
            .write_all(line.as_bytes())
            .context("write events.ndjson")?;
        writer.write_all(b"\n").context("write newline")?;
        total_bytes += line.len() as u64 + 1;
    }
    writer.flush().context("flush events.ndjson")?;
    drop(writer);

    let queries = match (classic_agg, otel_agg) {
        (Some(agg), None) => agg.into_queries(),
        (None, Some(agg)) => agg.into_queries(start_time, window_hours),
        _ => unreachable!(),
    };
    let queries_file = File::create(&queries_path)
        .with_context(|| format!("create {}", queries_path.display()))?;
    serde_json::to_writer_pretty(BufWriter::new(queries_file), &queries)
        .context("write queries.json")?;

    let manifest = Manifest {
        distribution_version: match profile {
            CorpusProfile::Classic => DISTRIBUTION_VERSION,
            CorpusProfile::OtelRich => OTEL_RICH_DISTRIBUTION_VERSION,
        },
        profile,
        seed,
        events,
        start_time,
        window_hours,
        events_ndjson_bytes: total_bytes,
    };
    let manifest_file = File::create(&manifest_path)
        .with_context(|| format!("create {}", manifest_path.display()))?;
    serde_json::to_writer_pretty(BufWriter::new(manifest_file), &manifest)
        .context("write manifest.json")?;

    Ok(manifest)
}

/// Read a generated corpus's `manifest.json` + `queries.json`. The events file
/// is not loaded into memory.
pub fn read_manifest(corpus_dir: impl AsRef<Path>) -> Result<(Manifest, Vec<QueryCase>, PathBuf)> {
    let dir = corpus_dir.as_ref();
    let manifest: Manifest = serde_json::from_reader(
        File::open(dir.join("manifest.json"))
            .with_context(|| format!("open {}/manifest.json", dir.display()))?,
    )
    .context("parse manifest.json")?;
    let queries: Vec<QueryCase> = serde_json::from_reader(
        File::open(dir.join("queries.json"))
            .with_context(|| format!("open {}/queries.json", dir.display()))?,
    )
    .context("parse queries.json")?;
    Ok((manifest, queries, dir.join("events.ndjson")))
}

fn classic_event_for(
    index: u64,
    seed: u64,
    start_time: DateTime<Utc>,
    window_secs: u64,
) -> CorpusEvent {
    let seed_bits = seed ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut rng = SmallRng::seed_from_u64(seed_bits);

    let host = HOSTS[(index as usize) % HOSTS.len()];
    let offset_secs = rng.gen_range(0..window_secs);
    let timestamp = start_time + Duration::seconds(offset_secs as i64);
    let per_100 = (index % 100) as u32;
    let shape = pick_shape(per_100);

    let (source, sourcetype, index_name, raw) = match shape {
        Shape::NginxAccess => {
            let method = HTTP_METHODS[rng.gen_range(0..HTTP_METHODS.len())];
            let path = URL_PATHS[rng.gen_range(0..URL_PATHS.len())];
            let status = nginx_status_for(rng.gen_range(0..100));
            let response_ms = rng.gen_range(1..500);
            let ip = format!(
                "10.{}.{}.{}",
                rng.gen_range(0..255),
                rng.gen_range(0..255),
                rng.gen_range(1..255)
            );
            let raw = format!(
                "{ip} - - [{}] \"{method} {path} HTTP/1.1\" {status} response_ms={response_ms} idx={index}",
                timestamp.format("%d/%b/%Y:%H:%M:%S +0000"),
            );
            (
                "/var/log/nginx/access.log".to_string(),
                "nginx:access".to_string(),
                "web".to_string(),
                raw,
            )
        }
        Shape::AppApi => {
            let endpoint = API_ENDPOINTS[rng.gen_range(0..API_ENDPOINTS.len())];
            let latency_ms = rng.gen_range(5..2000);
            let user_id = rng.gen_range(1..10_000);
            let raw = format!(
                "{} endpoint={endpoint} latency_ms={latency_ms} user_id={user_id} idx={index}",
                timestamp.to_rfc3339(),
            );
            (
                "/var/log/app/api.log".to_string(),
                "app:api".to_string(),
                "app".to_string(),
                raw,
            )
        }
        Shape::AuthSyslog => {
            let action = AUTH_ACTIONS[rng.gen_range(0..AUTH_ACTIONS.len())];
            let success = rng.gen_range(0..100) >= 10;
            let user_id = rng.gen_range(1..1000);
            let raw = format!(
                "{} action={action} success={success} user_id={user_id} idx={index}",
                timestamp.to_rfc3339(),
            );
            (
                "/var/log/auth.log".to_string(),
                "auth:syslog".to_string(),
                "security".to_string(),
                raw,
            )
        }
    };

    CorpusEvent {
        timestamp,
        host: host.to_string(),
        source,
        sourcetype,
        index_name,
        raw,
        attributes: None,
    }
}

#[derive(Debug, Clone)]
struct RichGeneratedEvent {
    corpus: CorpusEvent,
    resource_attributes: Vec<OtlpAttribute>,
    scope_name: String,
    log_attributes: Vec<OtlpAttribute>,
}

#[derive(Debug, Clone, Copy, Default)]
struct OtelRichFlags {
    error: bool,
    phrase: bool,
    substring: bool,
}

fn otel_rich_event(
    index: u64,
    seed: u64,
    start_time: DateTime<Utc>,
    window_secs: u64,
) -> RichGeneratedEvent {
    let mix = mix64(seed ^ index.wrapping_mul(0xD6E8_FEB8_6659_FD93));
    let host = HOSTS[(index as usize) % HOSTS.len()].to_string();
    let service = OTEL_SERVICES[(mix as usize) % OTEL_SERVICES.len()].to_string();
    let scope_name = OTEL_SCOPES[((mix >> 7) as usize) % OTEL_SCOPES.len()].to_string();
    let offset_secs = mix % window_secs;
    let timestamp = start_time + Duration::seconds(offset_secs as i64);
    let flags = rich_flags_for(index);
    let attr_map = rich_attribute_map(index, seed, &host, &service, flags.error);
    let body = rich_body(index, seed, &service, &host, &attr_map, flags);
    let resource_attributes = rich_resource_attributes(index, seed, &host, &service, &attr_map);
    let log_attributes = map_to_otlp_attributes(&attr_map);
    RichGeneratedEvent {
        corpus: CorpusEvent {
            timestamp,
            host,
            source: service.clone(),
            sourcetype: "otel:logs".to_string(),
            index_name: "events".to_string(),
            raw: body,
            attributes: Some(Value::Object(attr_map.clone())),
        },
        resource_attributes,
        scope_name,
        log_attributes,
    }
}

fn rich_flags_for(index: u64) -> OtelRichFlags {
    OtelRichFlags {
        error: index.is_multiple_of(20),
        phrase: (112_000..112_500).contains(&index),
        substring: (112_500..112_750).contains(&index),
    }
}

fn rich_body(
    index: u64,
    seed: u64,
    service: &str,
    host: &str,
    attrs: &Map<String, Value>,
    flags: OtelRichFlags,
) -> String {
    let region = attrs
        .get("cloud.region")
        .and_then(Value::as_str)
        .unwrap_or("us-east-1");
    let namespace = attrs
        .get("k8s.namespace.name")
        .and_then(Value::as_str)
        .unwrap_or("payments");
    let latency_ms = attrs.get("latency_ms").and_then(Value::as_i64).unwrap_or(0);
    let status = attrs.get("status").and_then(Value::as_i64).unwrap_or(200);

    let mut parts = vec![format!(
        "svc={service} host={host} region={region} namespace={namespace} latency_ms={latency_ms} status={status} idx={index}"
    )];

    if let Some(needle) = rare_needle_for(index) {
        parts.push(needle.to_string());
    }
    if flags.error {
        parts.push("error".to_string());
    }
    if flags.phrase {
        parts.push("quantum entanglement cascade".to_string());
    } else {
        if index.is_multiple_of(353) {
            parts.push("quantum jitter".to_string());
        }
        if index.is_multiple_of(587) {
            parts.push("entanglement drift".to_string());
        }
        if index.is_multiple_of(719) {
            parts.push("cascade warning".to_string());
        }
        if index.is_multiple_of(997) {
            parts.push("quantum noise before entanglement and eventual cascade".to_string());
        }
    }
    if flags.substring {
        parts.push("midprotoxqzfragtoken".to_string());
    }
    if mix64(seed ^ index.wrapping_mul(0xA076_1D64_78BD_642F)).is_multiple_of(7) {
        parts.push("timeout retry saturation".to_string());
    }
    if mix64(seed ^ index.wrapping_mul(0xE703_7ED1_A0B4_28DB)).is_multiple_of(11) {
        parts.push("database connection healthy".to_string());
    }
    parts.join(" ")
}

fn rare_needle_for(index: u64) -> Option<&'static str> {
    match index {
        0..=9 => Some("zugzwang0"),
        10..=109 => Some("zugzwang1"),
        110..=1_109 => Some("zugzwang2"),
        1_110..=11_109 => Some("zugzwang3"),
        11_110..=111_109 => Some("zugzwang4"),
        _ => None,
    }
}

fn rich_resource_attributes(
    index: u64,
    seed: u64,
    host: &str,
    service: &str,
    attrs: &Map<String, Value>,
) -> Vec<OtlpAttribute> {
    let mut out = vec![
        OtlpAttribute {
            key: "host.name".to_string(),
            value: OtlpValue::String(host.to_string()),
        },
        OtlpAttribute {
            key: "service.name".to_string(),
            value: OtlpValue::String(service.to_string()),
        },
        OtlpAttribute {
            key: "service.instance.id".to_string(),
            value: OtlpValue::String(hex128(seed, index, 0x100)),
        },
    ];
    for key in [
        "cloud.region",
        "service.tier",
        "k8s.namespace.name",
        "k8s.cluster.name",
        "deployment.environment",
    ] {
        if let Some(value) = attrs.get(key) {
            out.push(OtlpAttribute {
                key: key.to_string(),
                value: otlp_value_from_json(value),
            });
        }
    }
    out
}

fn rich_attribute_map(
    index: u64,
    seed: u64,
    host: &str,
    service: &str,
    error_flag: bool,
) -> Map<String, Value> {
    let mut map = Map::new();
    let start = (mix64(seed ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15)) & 0xFF) as u8;
    let mut step = ((mix64(seed ^ index.wrapping_mul(0x94D0_49BB_1331_11EB)) >> 17) as u8) | 1;
    if step == 0 {
        step = 1;
    }
    let mut cursor = start;
    for _ in 0..OTEL_RICH_ATTRS_PER_EVENT {
        let key_index = cursor as usize;
        let (key, value) = rich_attribute_for(key_index, index, seed, host, service, error_flag);
        map.insert(key, value);
        cursor = cursor.wrapping_add(step);
    }
    map
}

fn rich_attribute_for(
    key_index: usize,
    index: u64,
    seed: u64,
    host: &str,
    service: &str,
    error_flag: bool,
) -> (String, Value) {
    match key_index {
        0..=63 => {
            let slot = key_index;
            (
                STRING_KEYS[slot].to_string(),
                Value::String(rich_string_value(slot, index, seed, host, service)),
            )
        }
        64..=127 => {
            let slot = key_index - 64;
            (
                ID_KEYS[slot].to_string(),
                Value::String(hex128(seed ^ 0x55AA, index, slot as u64)),
            )
        }
        128..=191 => {
            let slot = key_index - 128;
            let value = rich_int_value(slot, index, seed, error_flag);
            (
                INT_KEYS[slot].to_string(),
                Value::Number(Number::from(value)),
            )
        }
        192..=223 => {
            let slot = key_index - 192;
            let value = rich_float_value(slot, index, seed);
            (
                FLOAT_KEYS[slot].to_string(),
                Value::Number(Number::from_f64(value).expect("finite float")),
            )
        }
        224..=255 => {
            let slot = key_index - 224;
            (
                BOOL_KEYS[slot].to_string(),
                Value::Bool(rich_bool_value(slot, index, seed, error_flag)),
            )
        }
        _ => unreachable!(),
    }
}

fn rich_string_value(slot: usize, index: u64, seed: u64, host: &str, service: &str) -> String {
    match STRING_KEYS[slot] {
        "service.tier" => ["gold", "silver", "bronze", "shadow"][(index as usize) % 4].to_string(),
        "cloud.region" => ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
            [((index / 7) as usize) % 4]
            .to_string(),
        "k8s.namespace.name" => ["payments", "checkout", "catalog", "auth", "search", "ops"]
            [((index / 11) as usize) % 6]
            .to_string(),
        "k8s.cluster.name" => ["k8s-prod-a", "k8s-prod-b", "k8s-stage-a", "k8s-dr-a"]
            [((index / 13) as usize) % 4]
            .to_string(),
        "deployment.environment" => ["prod", "stage", "dev"][(index as usize) % 3].to_string(),
        "service.group" => ["web", "api", "worker", "cron"][(index as usize) % 4].to_string(),
        "host.os.name" => ["linux", "darwin", "windows"][(index as usize) % 3].to_string(),
        "host.arch" => ["amd64", "arm64"][(index as usize) % 2].to_string(),
        "cloud.availability_zone" => {
            ["use1-az1", "use1-az2", "usw2-az1", "euw1-az1"][(index as usize) % 4].to_string()
        }
        "service.version" => format!("v{}.{}.{}", 1 + (index % 3), (index / 5) % 10, index % 10),
        "http.route" => ["/search", "/checkout", "/inventory", "/login", "/healthz"]
            [(index as usize) % 5]
            .to_string(),
        "db.system" => ["postgresql", "mysql", "sqlite", "redis"][(index as usize) % 4].to_string(),
        "messaging.system" => ["kafka", "sqs", "nats", "redis"][(index as usize) % 4].to_string(),
        "feature.flag" => {
            ["payments_v2", "autoscale", "kv_cache", "ab_test"][(index as usize) % 4].to_string()
        }
        "tenant.plan" => {
            ["free", "team", "business", "enterprise"][(index as usize) % 4].to_string()
        }
        "runtime.name" => ["rust", "go", "python", "java"][(index as usize) % 4].to_string(),
        _ => {
            let pool = STRING_VALUE_POOLS[slot % STRING_VALUE_POOLS.len()];
            let pick =
                ((mix64(seed ^ index.wrapping_mul((slot as u64) + 1)) >> 9) as usize) % pool.len();
            let suffix = match slot % 4 {
                0 => host,
                1 => service,
                2 => "steady",
                _ => "burst",
            };
            format!("{}-{suffix}", pool[pick])
        }
    }
}

fn rich_int_value(slot: usize, index: u64, seed: u64, error_flag: bool) -> i64 {
    match INT_KEYS[slot] {
        "latency_ms" => 5 + (mix64(seed ^ index) % 4_000) as i64,
        "status" => {
            if error_flag {
                [500_i64, 502, 503, 504][(index as usize) % 4]
            } else {
                [200_i64, 201, 204, 304, 404][(index as usize) % 5]
            }
        }
        "retry_count" => (mix64(seed ^ index.wrapping_mul(3)) % 6) as i64,
        "http.response_content_length" => {
            256 + (mix64(seed ^ index.wrapping_mul(5)) % 65_536) as i64
        }
        "db.rows_returned" => (mix64(seed ^ index.wrapping_mul(7)) % 50_000) as i64,
        "cache.entries_scanned" => (mix64(seed ^ index.wrapping_mul(11)) % 5_000) as i64,
        "cpu.millicores" => 100 + (mix64(seed ^ index.wrapping_mul(13)) % 4_000) as i64,
        "memory.rss_mib" => 64 + (mix64(seed ^ index.wrapping_mul(17)) % 8_192) as i64,
        "payload.bytes" => 128 + (mix64(seed ^ index.wrapping_mul(19)) % 1_000_000) as i64,
        "k8s.container.restart_count" => (mix64(seed ^ index.wrapping_mul(23)) % 20) as i64,
        _ => (mix64(seed ^ index.wrapping_mul((slot as u64) + 29)) % 100_000) as i64,
    }
}

fn rich_float_value(slot: usize, index: u64, seed: u64) -> f64 {
    let raw =
        (mix64(seed ^ index.wrapping_mul((slot as u64) + 41)) % 1_000_000) as f64 / 1_000_000.0;
    match FLOAT_KEYS[slot] {
        "cpu.utilization" | "memory.utilization" | "cache.hit_ratio" | "queue.depth_ratio" => raw,
        "db.query_cost" => raw * 1_000.0,
        "network.rtt_ms" => raw * 250.0,
        "temperature.celsius" => 20.0 + raw * 70.0,
        _ => raw * 10_000.0,
    }
}

fn rich_bool_value(slot: usize, index: u64, seed: u64, error_flag: bool) -> bool {
    match BOOL_KEYS[slot] {
        "error.flag" => error_flag,
        "cache.hit" => mix64(seed ^ index.wrapping_mul(67)) % 10 < 7,
        "auth.success" => mix64(seed ^ index.wrapping_mul(71)) % 10 < 9,
        "cold.start" => index.is_multiple_of(10_000),
        "sampled" => mix64(seed ^ index.wrapping_mul(73)) % 10 < 8,
        "rate_limited" => mix64(seed ^ index.wrapping_mul(79)) % 100 < 3,
        "feature.alpha" => mix64(seed ^ index.wrapping_mul(83)).is_multiple_of(2),
        "feature.beta" => mix64(seed ^ index.wrapping_mul(89)).is_multiple_of(5),
        _ => mix64(seed ^ index.wrapping_mul((slot as u64) + 97)).is_multiple_of(2),
    }
}

fn otlp_resource_log_for(
    index: u64,
    seed: u64,
    start_time: DateTime<Utc>,
    window_secs: u32,
    profile: CorpusProfile,
) -> OtlpResourceLogs {
    match profile {
        CorpusProfile::Classic => {
            let event = classic_event_for(index, seed, start_time, window_secs as u64);
            OtlpResourceLogs {
                resource_attributes: vec![
                    OtlpAttribute {
                        key: "host.name".to_string(),
                        value: OtlpValue::String(event.host.clone()),
                    },
                    OtlpAttribute {
                        key: "service.name".to_string(),
                        value: OtlpValue::String(event.source.clone()),
                    },
                ],
                scope_name: "siglake-corpus".to_string(),
                log_records: vec![OtlpLogRecord {
                    time_unix_nano: timestamp_to_nanos(event.timestamp),
                    body: event.raw.clone(),
                    attributes: vec![
                        OtlpAttribute {
                            key: "sourcetype".to_string(),
                            value: OtlpValue::String(event.sourcetype),
                        },
                        OtlpAttribute {
                            key: "index".to_string(),
                            value: OtlpValue::String(event.index_name),
                        },
                    ],
                }],
            }
        }
        CorpusProfile::OtelRich => {
            let rich = otel_rich_event(index, seed, start_time, window_secs as u64);
            OtlpResourceLogs {
                resource_attributes: rich.resource_attributes,
                scope_name: rich.scope_name,
                log_records: vec![OtlpLogRecord {
                    time_unix_nano: timestamp_to_nanos(rich.corpus.timestamp),
                    body: rich.corpus.raw,
                    attributes: rich.log_attributes,
                }],
            }
        }
    }
}

fn timestamp_to_nanos(ts: DateTime<Utc>) -> u64 {
    ts.timestamp_nanos_opt().expect("timestamp in range") as u64
}

fn map_to_otlp_attributes(map: &Map<String, Value>) -> Vec<OtlpAttribute> {
    map.iter()
        .map(|(key, value)| OtlpAttribute {
            key: key.clone(),
            value: otlp_value_from_json(value),
        })
        .collect()
}

fn otlp_value_from_json(value: &Value) -> OtlpValue {
    match value {
        Value::String(v) => OtlpValue::String(v.clone()),
        Value::Bool(v) => OtlpValue::Bool(*v),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                OtlpValue::Int(i)
            } else {
                OtlpValue::Double(v.as_f64().expect("finite number"))
            }
        }
        _ => OtlpValue::String(value.to_string()),
    }
}

#[derive(Default)]
struct ClassicAggregator {
    total: u64,
    per_sourcetype: BTreeMap<String, u64>,
    per_host: BTreeMap<String, u64>,
    status_500: u64,
    nginx_total: u64,
    auth_failures: u64,
}

impl ClassicAggregator {
    fn observe(&mut self, ev: &CorpusEvent) {
        self.total += 1;
        *self
            .per_sourcetype
            .entry(ev.sourcetype.clone())
            .or_insert(0) += 1;
        *self.per_host.entry(ev.host.clone()).or_insert(0) += 1;
        if ev.sourcetype == "nginx:access" {
            self.nginx_total += 1;
            if ev.raw.contains(" 500 ") {
                self.status_500 += 1;
            }
        }
        if ev.sourcetype == "auth:syslog" && ev.raw.contains("success=false") {
            self.auth_failures += 1;
        }
    }

    fn into_queries(self) -> Vec<QueryCase> {
        let scope = "host LIKE '%.prod.example.com'";
        let spl_scope = "host=*.prod.example.com";
        vec![
            QueryCase {
                id: "total_count".into(),
                description: "Total event count (corpus-scoped).".into(),
                sql: format!("SELECT count(*) AS n FROM events WHERE {scope}"),
                spl: format!("search {spl_scope} | stats count"),
                expected: ExpectedResult::Count { value: self.total },
            },
            QueryCase {
                id: "nginx_count".into(),
                description: "Count of nginx:access events (corpus-scoped).".into(),
                sql: format!(
                    "SELECT count(*) AS n FROM events WHERE {scope} AND sourcetype = 'nginx:access'"
                ),
                spl: format!("search {spl_scope} sourcetype=nginx:access | stats count"),
                expected: ExpectedResult::Count {
                    value: self.nginx_total,
                },
            },
            QueryCase {
                id: "status_500_count".into(),
                description: "Count of HTTP 500 nginx events.".into(),
                sql: format!(
                    "SELECT count(*) AS n FROM events WHERE {scope} AND sourcetype = 'nginx:access' AND raw LIKE '% 500 %'"
                ),
                spl: format!(
                    "search {spl_scope} sourcetype=nginx:access \" 500 \" | stats count"
                ),
                expected: ExpectedResult::Count {
                    value: self.status_500,
                },
            },
            QueryCase {
                id: "auth_failure_count".into(),
                description: "Count of failed auth events.".into(),
                sql: format!(
                    "SELECT count(*) AS n FROM events WHERE {scope} AND sourcetype = 'auth:syslog' AND raw LIKE '%success=false%'"
                ),
                spl: format!(
                    "search {spl_scope} sourcetype=auth:syslog success=false | stats count"
                ),
                expected: ExpectedResult::Count {
                    value: self.auth_failures,
                },
            },
            QueryCase {
                id: "count_by_sourcetype".into(),
                description: "GROUP BY sourcetype (alphabetical, corpus-scoped).".into(),
                sql: format!(
                    "SELECT sourcetype, count(*) AS n FROM events WHERE {scope} GROUP BY sourcetype ORDER BY sourcetype"
                ),
                spl: format!("search {spl_scope} | stats count by sourcetype | sort sourcetype"),
                expected: ExpectedResult::GroupedCount {
                    rows: self.per_sourcetype.into_iter().collect(),
                },
            },
            QueryCase {
                id: "count_by_host".into(),
                description: "GROUP BY host (alphabetical, 16 rows, corpus-scoped).".into(),
                sql: format!(
                    "SELECT host, count(*) AS n FROM events WHERE {scope} GROUP BY host ORDER BY host"
                ),
                spl: format!("search {spl_scope} | stats count by host | sort host"),
                expected: ExpectedResult::GroupedCount {
                    rows: self.per_host.into_iter().collect(),
                },
            },
        ]
    }
}

struct OtelRichAggregator {
    total: u64,
    full_window_hours: u32,
    per_host: BTreeMap<String, u64>,
    common_error: u64,
    phrase: u64,
    substring: u64,
    needles: [u64; 5],
    hist_1h: BTreeMap<String, u64>,
    hist_24h: BTreeMap<String, u64>,
    hist_full: BTreeMap<String, u64>,
}

impl OtelRichAggregator {
    fn new(full_window_hours: u32) -> Self {
        Self {
            total: 0,
            full_window_hours,
            per_host: BTreeMap::new(),
            common_error: 0,
            phrase: 0,
            substring: 0,
            needles: [0; 5],
            hist_1h: BTreeMap::new(),
            hist_24h: BTreeMap::new(),
            hist_full: BTreeMap::new(),
        }
    }

    fn observe(&mut self, ev: &CorpusEvent) {
        self.total += 1;
        *self.per_host.entry(ev.host.clone()).or_insert(0) += 1;
        if ev.raw.split_whitespace().any(|token| token == "error") {
            self.common_error += 1;
        }
        if ev.raw.contains("quantum entanglement cascade") {
            self.phrase += 1;
        }
        if ev.raw.contains("xqzfrag") {
            self.substring += 1;
        }
        for (idx, token) in [
            "zugzwang0",
            "zugzwang1",
            "zugzwang2",
            "zugzwang3",
            "zugzwang4",
        ]
        .iter()
        .enumerate()
        {
            if ev.raw.split_whitespace().any(|part| part == *token) {
                self.needles[idx] += 1;
            }
        }
        *self.hist_1h.entry(bucket_key(ev.timestamp, 1)).or_insert(0) += 1;
        *self
            .hist_24h
            .entry(bucket_key(ev.timestamp, 24))
            .or_insert(0) += 1;
        *self
            .hist_full
            .entry(bucket_key(ev.timestamp, self.full_window_hours))
            .or_insert(0) += 1;
    }

    fn into_queries(self, start_time: DateTime<Utc>, window_hours: u32) -> Vec<QueryCase> {
        let mut top_hosts: Vec<(String, u64)> = self.per_host.into_iter().collect();
        top_hosts.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        top_hosts.truncate(10);
        let full_bucket = bucket_key(start_time, window_hours.max(1));

        vec![
            QueryCase {
                id: "count_all".into(),
                description: "Total otel-rich event count.".into(),
                sql: "SELECT count(*) AS n FROM events".into(),
                spl: String::new(),
                expected: ExpectedResult::Count { value: self.total },
            },
            QueryCase {
                id: "rare_needle_10".into(),
                description: "Bodies containing zugzwang0.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang0')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.needles[0],
                },
            },
            QueryCase {
                id: "rare_needle_100".into(),
                description: "Bodies containing zugzwang1.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang1')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.needles[1],
                },
            },
            QueryCase {
                id: "rare_needle_1000".into(),
                description: "Bodies containing zugzwang2.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang2')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.needles[2],
                },
            },
            QueryCase {
                id: "rare_needle_10000".into(),
                description: "Bodies containing zugzwang3.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang3')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.needles[3],
                },
            },
            QueryCase {
                id: "rare_needle_100000".into(),
                description: "Bodies containing zugzwang4.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zugzwang4')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.needles[4],
                },
            },
            QueryCase {
                id: "common_term".into(),
                description: "Bodies containing the common token error.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'error')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.common_error,
                },
            },
            QueryCase {
                id: "phrase".into(),
                description: "Bodies containing the exact phrase quantum entanglement cascade.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE match_phrase(raw, 'quantum entanglement cascade')".into(),
                spl: String::new(),
                expected: ExpectedResult::Count { value: self.phrase },
            },
            QueryCase {
                id: "like_substring".into(),
                description: "Bodies containing the embedded substring xqzfrag.".into(),
                sql: "SELECT count(*) AS n FROM events WHERE raw LIKE '%xqzfrag%'".into(),
                spl: String::new(),
                expected: ExpectedResult::Count {
                    value: self.substring,
                },
            },
            QueryCase {
                id: "date_histogram_1h".into(),
                description: "1h histogram over timestamp.".into(),
                sql: "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket".into(),
                spl: String::new(),
                expected: ExpectedResult::GroupedCount {
                    rows: self.hist_1h.into_iter().collect(),
                },
            },
            QueryCase {
                id: "date_histogram_24h".into(),
                description: "24h histogram over timestamp.".into(),
                sql: "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket".into(),
                spl: String::new(),
                expected: ExpectedResult::GroupedCount {
                    rows: self.hist_24h.into_iter().collect(),
                },
            },
            QueryCase {
                id: "date_histogram_full".into(),
                description: "Whole-window histogram over timestamp.".into(),
                sql: format!(
                    "SELECT TIMESTAMP '{}' AS bucket, count(*) AS n FROM events",
                    full_bucket
                ),
                spl: String::new(),
                expected: ExpectedResult::GroupedCount {
                    rows: self.hist_full.into_iter().collect(),
                },
            },
            QueryCase {
                id: "terms_top10_hosts".into(),
                description: "Top 10 hosts by count.".into(),
                sql: "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC, host ASC LIMIT 10".into(),
                spl: String::new(),
                expected: ExpectedResult::GroupedCount { rows: top_hosts },
            },
        ]
    }
}

/// Keep corpus bucket keys in the exact RFC3339/second-precision form the
/// benchmark runner normalizes observed group keys to.
pub fn normalize_bucket_key_timestamp(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn normalize_bucket_key_time_string(text: &str) -> Option<String> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|ts| normalize_bucket_key_timestamp(ts.with_timezone(&Utc)))
}

fn bucket_key(ts: DateTime<Utc>, hours: u32) -> String {
    let seconds = (hours as i64) * 3600;
    let aligned = ts.timestamp().div_euclid(seconds) * seconds;
    normalize_bucket_key_timestamp(
        Utc.timestamp_opt(aligned, 0)
            .single()
            .expect("aligned timestamp"),
    )
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn hex128(seed: u64, index: u64, salt: u64) -> String {
    let hi = mix64(seed ^ index.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ salt);
    let lo = mix64(seed.rotate_left(17) ^ index.wrapping_mul(0xD6E8_FEB8_6659_FD93) ^ !salt);
    format!("{hi:016x}{lo:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_for_is_deterministic() {
        let t = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let a = event_for(12345, 42, t, 86400);
        let b = event_for(12345, 42, t, 86400);
        assert_eq!(a.host, b.host);
        assert_eq!(a.raw, b.raw);
        assert_eq!(a.timestamp, b.timestamp);
    }

    #[test]
    fn event_for_changes_with_seed() {
        let t = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let a = event_for(12345, 42, t, 86400);
        let b = event_for(12345, 43, t, 86400);
        assert_eq!(a.host, b.host);
        assert_ne!(a.raw, b.raw);
    }

    #[test]
    fn nginx_status_distribution_matches_per_100_bins() {
        let mut counts = BTreeMap::new();
        for i in 0..1000 {
            *counts.entry(nginx_status_for(i % 100)).or_insert(0_u32) += 1;
        }
        assert_eq!(counts[&200], 920);
        assert_eq!(counts[&500], 10);
        assert_eq!(counts[&304], 30);
    }

    #[test]
    fn shape_mix_covers_all_per_100() {
        let mut nginx = 0u32;
        let mut api = 0u32;
        let mut auth = 0u32;
        for i in 0..100 {
            match pick_shape(i) {
                Shape::NginxAccess => nginx += 1,
                Shape::AppApi => api += 1,
                Shape::AuthSyslog => auth += 1,
            }
        }
        assert_eq!(nginx, 60);
        assert_eq!(api, 35);
        assert_eq!(auth, 5);
    }

    #[test]
    fn generate_small_corpus_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let manifest = generate(tmp.path(), 42, 5_000, start, 24).expect("generate");
        assert_eq!(manifest.events, 5_000);
        assert_eq!(manifest.distribution_version, DISTRIBUTION_VERSION);
        assert_eq!(manifest.profile, CorpusProfile::Classic);

        assert!(tmp.path().join("events.ndjson").exists());
        assert!(tmp.path().join("queries.json").exists());
        assert!(tmp.path().join("manifest.json").exists());

        let (m2, queries, events_path) = read_manifest(tmp.path()).unwrap();
        assert_eq!(m2.seed, 42);
        assert_eq!(m2.events, 5_000);
        assert!(events_path.exists());

        let ids: Vec<_> = queries.iter().map(|q| q.id.clone()).collect();
        assert!(ids.contains(&"total_count".to_string()));
        assert!(ids.contains(&"nginx_count".to_string()));
        assert!(ids.contains(&"status_500_count".to_string()));
        assert!(ids.contains(&"count_by_sourcetype".to_string()));
        assert!(ids.contains(&"count_by_host".to_string()));

        let total = queries.iter().find(|q| q.id == "total_count").unwrap();
        match &total.expected {
            ExpectedResult::Count { value } => assert_eq!(*value, 5_000),
            other => panic!("total_count should be Count, got {other:?}"),
        }

        let by_st = queries
            .iter()
            .find(|q| q.id == "count_by_sourcetype")
            .unwrap();
        let ExpectedResult::GroupedCount { rows } = &by_st.expected else {
            panic!("count_by_sourcetype should be GroupedCount");
        };
        let total: u64 = rows.iter().map(|(_, n)| n).sum();
        assert_eq!(total, 5_000);
        let nginx = rows.iter().find(|(s, _)| s == "nginx:access").unwrap().1;
        let api = rows.iter().find(|(s, _)| s == "app:api").unwrap().1;
        let auth = rows.iter().find(|(s, _)| s == "auth:syslog").unwrap().1;
        assert_eq!(nginx, 3_000);
        assert_eq!(api, 1_750);
        assert_eq!(auth, 250);

        let by_host = queries.iter().find(|q| q.id == "count_by_host").unwrap();
        let ExpectedResult::GroupedCount { rows } = &by_host.expected else {
            panic!("count_by_host should be GroupedCount");
        };
        assert_eq!(rows.len(), 16);
        let host_total: u64 = rows.iter().map(|(_, n)| n).sum();
        assert_eq!(host_total, 5_000);
    }

    #[test]
    fn generate_is_byte_for_byte_reproducible() {
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        generate(t1.path(), 12345, 2_000, start, 24).unwrap();
        generate(t2.path(), 12345, 2_000, start, 24).unwrap();
        let a = std::fs::read(t1.path().join("events.ndjson")).unwrap();
        let b = std::fs::read(t2.path().join("events.ndjson")).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn otel_rich_generation_records_expected_feature_counts() {
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let window_secs = OTEL_RICH_DEFAULT_WINDOW_HOURS as u64 * 3600;
        let mut aggregator = OtelRichAggregator::new(OTEL_RICH_DEFAULT_WINDOW_HOURS);
        for index in 0..200_000 {
            aggregator.observe(&event_for_profile(
                index,
                7,
                start,
                window_secs,
                CorpusProfile::OtelRich,
            ));
        }
        let queries = aggregator.into_queries(start, OTEL_RICH_DEFAULT_WINDOW_HOURS);
        let count = |id: &str| -> u64 {
            let query = queries.iter().find(|q| q.id == id).unwrap();
            match query.expected {
                ExpectedResult::Count { value } => value,
                _ => panic!("expected Count"),
            }
        };
        assert_eq!(count("rare_needle_10"), 10);
        assert_eq!(count("rare_needle_100"), 100);
        assert_eq!(count("rare_needle_1000"), 1_000);
        assert_eq!(count("rare_needle_10000"), 10_000);
        assert_eq!(count("rare_needle_100000"), 100_000);
        assert_eq!(count("phrase"), 500);
        assert_eq!(count("like_substring"), 250);
        assert_eq!(count("common_term"), 10_000);
    }

    #[test]
    fn otel_bucket_keys_match_runner_normalization_contract() {
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 1, 23, 45).unwrap();
        assert_eq!(bucket_key(ts, 1), "2026-01-01T01:00:00Z");
        assert_eq!(
            normalize_bucket_key_time_string("2026-01-01T01:00:00Z").as_deref(),
            Some("2026-01-01T01:00:00Z")
        );
        assert_eq!(
            normalize_bucket_key_time_string("2026-01-01T01:00:00+00:00").as_deref(),
            Some("2026-01-01T01:00:00Z")
        );
        assert_eq!(
            normalize_bucket_key_time_string("2026-01-01T01:00:00.000Z").as_deref(),
            Some("2026-01-01T01:00:00Z")
        );
    }

    #[test]
    fn otlp_batch_iter_emits_all_events() {
        let manifest = Manifest {
            distribution_version: OTEL_RICH_DISTRIBUTION_VERSION,
            profile: CorpusProfile::OtelRich,
            seed: 9,
            events: 1_234,
            start_time: Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap(),
            window_hours: OTEL_RICH_DEFAULT_WINDOW_HOURS,
            events_ndjson_bytes: 0,
        };
        let mut total = 0usize;
        for batch in otlp_batches(&manifest, 128).unwrap() {
            for resource_logs in batch.resource_logs {
                total += resource_logs.log_records.len();
                assert!(!resource_logs.resource_attributes.is_empty());
            }
        }
        assert_eq!(total, 1_234);
    }
}
