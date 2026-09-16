//! OTel ingest hot-path microbenchmark (perf-tuning instrument).
//!
//! Models the CPU work of `POST /v1/logs` (JSON): deserialize the OTLP envelope
//! → `otlp_logs_to_events` (core mapping + WS-7 residual-attribute capture) →
//! `events_to_record_batch` (Arrow build for the WAL). Disk/HTTP are excluded so
//! the numbers isolate the per-event CPU cost we iterate on.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use siglake_core::events_to_record_batch;
use siglake_ingest::otlp::{
    otlp_logs_to_events, otlp_logs_to_record_batch, parse_otlp_json, ExportLogsServiceRequest,
};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Build a representative OTLP/HTTP logs JSON body: `records` log records spread
/// across `records/10` resourceLogs (10 records each), every resource carrying 2
/// core + 3 residual attributes, every record 2 core + 4 residual attributes and
/// a ~110-byte body. Mirrors a typical agent batch (Otel Collector / FluentBit).
fn otlp_json(records: usize) -> Vec<u8> {
    let per_resource = 10usize;
    let n_resources = records.div_ceil(per_resource);
    let mut resource_logs = Vec::with_capacity(n_resources);
    let mut emitted = 0usize;
    for r in 0..n_resources {
        let mut log_records = Vec::new();
        for _ in 0..per_resource {
            if emitted >= records {
                break;
            }
            emitted += 1;
            log_records.push(serde_json::json!({
                "timeUnixNano": "1700000000000000000",
                "body": {"stringValue": "GET /api/v1/items?id=4821 status=200 latency_ms=37 bytes=1043 ua=curl/8.1"},
                "attributes": [
                    {"key": "sourcetype", "value": {"stringValue": "nginx:access"}},
                    {"key": "index", "value": {"stringValue": "main"}},
                    {"key": "http.method", "value": {"stringValue": "GET"}},
                    {"key": "http.status_code", "value": {"intValue": 200}},
                    {"key": "http.target", "value": {"stringValue": "/api/v1/items"}},
                    {"key": "trace.sampled", "value": {"boolValue": true}}
                ]
            }));
        }
        resource_logs.push(serde_json::json!({
            "resource": {"attributes": [
                {"key": "host.name", "value": {"stringValue": format!("web-{r:03}.prod.example.com")}},
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "k8s.namespace", "value": {"stringValue": "prod"}},
                {"key": "k8s.pod.name", "value": {"stringValue": format!("checkout-7d9f-{r:04}")}},
                {"key": "cloud.region", "value": {"stringValue": "us-east-1"}}
            ]},
            "scopeLogs": [{"scope": {"name": "otel"}, "logRecords": log_records}]
        }));
    }
    serde_json::to_vec(&serde_json::json!({"resourceLogs": resource_logs})).unwrap()
}

/// Same shape but with ONLY the core attributes (no residual) — isolates the
/// WS-7 residual-attribute build cost in `map_to_events`.
fn otlp_json_core_only(records: usize) -> Vec<u8> {
    let per_resource = 10usize;
    let n_resources = records.div_ceil(per_resource);
    let mut resource_logs = Vec::with_capacity(n_resources);
    let mut emitted = 0usize;
    for r in 0..n_resources {
        let mut log_records = Vec::new();
        for _ in 0..per_resource {
            if emitted >= records {
                break;
            }
            emitted += 1;
            log_records.push(serde_json::json!({
                "timeUnixNano": "1700000000000000000",
                "body": {"stringValue": "GET /api/v1/items?id=4821 status=200 latency_ms=37 bytes=1043 ua=curl/8.1"},
                "attributes": [
                    {"key": "sourcetype", "value": {"stringValue": "nginx:access"}},
                    {"key": "index", "value": {"stringValue": "main"}}
                ]
            }));
        }
        resource_logs.push(serde_json::json!({
            "resource": {"attributes": [
                {"key": "host.name", "value": {"stringValue": format!("web-{r:03}.prod.example.com")}},
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"scope": {"name": "otel"}, "logRecords": log_records}]
        }));
    }
    serde_json::to_vec(&serde_json::json!({"resourceLogs": resource_logs})).unwrap()
}

/// "Thin" shape: 1 record/resourceLogs, only the 2 core record attrs, and a
/// large ~500-byte body — the common unstructured-log case where the raw body,
/// not attributes, dominates. Stresses a different part of the pipeline.
fn otlp_json_thin(records: usize) -> Vec<u8> {
    let big_body = "GET /api/v1/items?id=4821 status=200 latency_ms=37 bytes=1043 ".repeat(8);
    let resource_logs: Vec<_> = (0..records)
        .map(|r| {
            serde_json::json!({
                "resource": {"attributes": [
                    {"key": "host.name", "value": {"stringValue": format!("web-{r:05}")}},
                    {"key": "service.name", "value": {"stringValue": "checkout"}}
                ]},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": {"stringValue": big_body},
                    "attributes": [
                        {"key": "sourcetype", "value": {"stringValue": "syslog"}},
                        {"key": "index", "value": {"stringValue": "main"}}
                    ]
                }]}]
            })
        })
        .collect();
    serde_json::to_vec(&serde_json::json!({"resourceLogs": resource_logs})).unwrap()
}

fn bench(c: &mut Criterion) {
    const N: usize = 1000;
    let body = otlp_json(N);

    let mut g = c.benchmark_group("otlp_ingest");
    g.throughput(Throughput::Elements(N as u64));

    // Thin payload (large body, few attrs) — validates the wins generalize.
    g.bench_function("parse_and_map_thin", |b| {
        let thin = otlp_json_thin(N);
        b.iter_batched(
            || thin.clone(),
            |mut buf| std::hint::black_box(otlp_logs_to_events(parse_otlp_json(&mut buf).unwrap())),
            BatchSize::SmallInput,
        );
    });

    // 1. Full JSON path: deserialize + map (what the handler does pre-WAL).
    g.bench_function("parse_and_map", |b| {
        b.iter(|| {
            let req: ExportLogsServiceRequest =
                serde_json::from_slice(std::hint::black_box(&body)).unwrap();
            std::hint::black_box(otlp_logs_to_events(req))
        });
    });

    // 2. JSON deserialize only (serde_json).
    g.bench_function("json_deserialize", |b| {
        b.iter(|| {
            let req: ExportLogsServiceRequest =
                serde_json::from_slice(std::hint::black_box(&body)).unwrap();
            std::hint::black_box(req);
        });
    });

    // 2b. JSON deserialize only (simd-json, in-situ over an owned buffer).
    g.bench_function("json_deserialize_simd", |b| {
        b.iter_batched(
            || body.clone(),
            |mut buf| {
                let req = parse_otlp_json(&mut buf).unwrap();
                // Return a non-borrowing value so the borrow of `buf` doesn't escape.
                std::hint::black_box(req.resource_logs.len())
            },
            BatchSize::SmallInput,
        );
    });

    // 2c. Full path with simd-json parse + map.
    g.bench_function("parse_and_map_simd", |b| {
        b.iter_batched(
            || body.clone(),
            |mut buf| std::hint::black_box(otlp_logs_to_events(parse_otlp_json(&mut buf).unwrap())),
            BatchSize::SmallInput,
        );
    });

    // 2d. Handler hot path post-change: simd parse + map straight into the
    //     Arrow RecordBatch, no owned `Vec<Event>` detour. Compare against
    //     parse_and_map_simd + events_to_batch (the old two-stage cost).
    g.bench_function("parse_map_batch_simd", |b| {
        b.iter_batched(
            || body.clone(),
            |mut buf| {
                std::hint::black_box(
                    otlp_logs_to_record_batch(parse_otlp_json(&mut buf).unwrap()).unwrap(),
                )
            },
            BatchSize::SmallInput,
        );
    });

    // 2e. Batch path on the thin shape (large body, few attrs).
    g.bench_function("parse_map_batch_thin", |b| {
        let thin = otlp_json_thin(N);
        b.iter_batched(
            || thin.clone(),
            |mut buf| {
                std::hint::black_box(
                    otlp_logs_to_record_batch(parse_otlp_json(&mut buf).unwrap()).unwrap(),
                )
            },
            BatchSize::SmallInput,
        );
    });

    // 3. Mapping only (incl. WS-7 residual-attribute capture), pre-parsed input.
    g.bench_function("map_to_events", |b| {
        let req: ExportLogsServiceRequest = serde_json::from_slice(&body).unwrap();
        b.iter_batched(
            || req.clone(),
            |req| std::hint::black_box(otlp_logs_to_events(req)),
            BatchSize::SmallInput,
        );
    });

    // 3b. Mapping with NO residual attributes — isolates the WS-7 build cost.
    g.bench_function("map_to_events_core_only", |b| {
        let core_body = otlp_json_core_only(N);
        let req: ExportLogsServiceRequest = serde_json::from_slice(&core_body).unwrap();
        b.iter_batched(
            || req.clone(),
            |req| std::hint::black_box(otlp_logs_to_events(req)),
            BatchSize::SmallInput,
        );
    });

    // 4. Arrow batch build for the WAL.
    g.bench_function("events_to_batch", |b| {
        let req: ExportLogsServiceRequest = serde_json::from_slice(&body).unwrap();
        let events = otlp_logs_to_events(req);
        b.iter(|| {
            std::hint::black_box(events_to_record_batch(std::hint::black_box(&events)).unwrap())
        });
    });

    // 5. End-to-end CPU: parse + map + WAL append (Arrow IPC serialize to a
    //    buffered segment file; no forced fsync — that happens on seal). Shows
    //    whether the JSON CPU path or the WAL serialize now dominates.
    g.bench_function("parse_map_wal_append", |b| {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            dir.path(),
            "bench",
            usize::MAX, // never seal mid-bench (isolate append cost, no fsync)
            std::time::Duration::from_secs(3600),
        )
        .unwrap();
        b.iter_batched(
            || body.clone(),
            |mut buf| {
                // The handler's actual post-change path: parse → RecordBatch →
                // WAL append (no owned Vec<Event>).
                let batch = otlp_logs_to_record_batch(parse_otlp_json(&mut buf).unwrap()).unwrap();
                writer.append_batch(&batch).unwrap();
                std::hint::black_box(())
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
