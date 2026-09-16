//! Every Jaeger read ceiling, end to end through the real router, with the
//! refusal ATTRIBUTED (#2184).
//!
//! THE CONTRACT THIS PINS. The four routes' render ceilings are derived from
//! the admission reservation a trace read already takes — there is no
//! `SIGLAKE_JAEGER_*` knob — so the only way to move them in a test is to
//! inject the admission budget they come out of, which is what every phase
//! below does. Each ceiling is exercised in the shape
//! `jaeger_pool_exhaustion_503.rs` uses: a POSITIVE CONTROL first (the same
//! request, the same corpus, one notch of headroom, answering 200 with
//! COMPLETE data), then the refusal. A ceiling that refuses everything fails
//! this file; so does one that refuses nothing.
//!
//! Its own binary because the metrics recorder is process-global: the
//! `siglake_query_breaker_trips_total{breaker="jaeger_*"}` series are the half
//! of the operator signal the statuses (#2102) cannot give — WHICH ceiling
//! refused — and they can only be read from a `DebuggingRecorder` no other
//! test's counters are landing in. Nothing here reads or writes the
//! environment.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderMap, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue,
};
use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
use opentelemetry_proto::tonic::trace::v1::{
    span, ResourceSpans as ProtoResourceSpans, ScopeSpans as ProtoScopeSpans, Span as ProtoSpan,
};
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, map_carrier_batch};
use siglake_ingest::otlp_traces::otlp_proto_traces_to_events;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::index_manager::builtin_traces_template;
use tower::util::ServiceExt;

/// The packaged pod's reservation: a 64 MiB admission budget at the default
/// per-query share divisor resolves `min(16 MiB, 64 MiB / 4)` = 16 MiB, the
/// same figure a 4Gi query pod resolves. Every positive control runs here, so
/// they are controls for the DEFAULT ceilings and not for a test-only shape.
const PACKAGED_BUDGET: u64 = 64 * 1024 * 1024;

/// A budget whose reservation resolves `span_rows` to exactly the fat trace's
/// span count (200), and one that resolves it to 199. The pair is the
/// cap/cap+1 boundary at the HTTP layer: at the ceiling the trace is returned
/// WHOLE, one row over it is refused whole.
///
/// Derived, not guessed: `span_rows = (budget / 4 - 2 MiB) / 8704`.
const AT_200_SPAN_ROWS: u64 = 15_351_808;
const AT_199_SPAN_ROWS: u64 = 15_316_992;

/// 3 MiB of reservation, 1 MiB of render budget: 120 span rows, 262,144 Arrow
/// bytes, 731 names. Small enough that the wide trace and the name corpus
/// below cross it, large enough that the small corpus does not.
const SMALL_BUDGET: u64 = 12 * 1024 * 1024;

const SMALL: &str = "traces-small";
const FAT: &str = "traces-fat";
const WIDE: &str = "traces-wide";
const NAMES: &str = "traces-names";

/// One trace of 200 spans: the row bound's corpus, on a route with no `limit`
/// at all, so the row refusal cannot be confused with the trace ceiling.
const FAT_SPANS: usize = 200;
/// Four spans of 128 KiB of attributes each: ~512 KiB of Arrow in four rows,
/// which no row ceiling here would refuse.
const WIDE_SPANS: usize = 4;
const WIDE_ATTR_BYTES: usize = 128 * 1024;
/// Distinct services, one span each: the name bound's corpus.
const DISTINCT_SERVICES: usize = 800;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && labels.iter().all(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|l| l.key() == *lk && l.value() == *lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

fn trips(snapshot: &SnapshotVec, breaker: &str) -> u64 {
    counter_sum(
        snapshot,
        "siglake_query_breaker_trips_total",
        &[("breaker", breaker)],
    )
}

fn gauge_value(snapshot: &SnapshotVec, name: &str) -> f64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == name)
        .map(|(_, _, _, value)| match value {
            DebugValue::Gauge(g) => g.into_inner(),
            _ => 0.0,
        })
        .sum()
}

fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_string(),
        value: Some(ProtoAnyValue {
            value: Some(ProtoValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// `spans` spans, `traces` traces, one service unless `service_per_span`.
fn proto_fixture(
    traces: usize,
    spans_per_trace: usize,
    attr_bytes: usize,
    service_per_span: bool,
) -> ExportTraceServiceRequest {
    let payload = "x".repeat(attr_bytes);
    let resource_spans = (0..traces)
        .map(|t| {
            let spans = (0..spans_per_trace)
                .map(|i| {
                    let k = (t * spans_per_trace + i) as u64;
                    let start = 1_700_000_000_000_000_000 + k * 700_000;
                    ProtoSpan {
                        trace_id: (t as u64).to_be_bytes().repeat(2),
                        span_id: (k ^ 0xa5a5_a5a5_a5a5_a5a5).to_be_bytes().to_vec(),
                        name: format!("op-{i}"),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: start,
                        end_time_unix_nano: start + 50_000_000,
                        attributes: vec![proto_str("payload", &payload)],
                        ..Default::default()
                    }
                })
                .collect();
            let service = if service_per_span {
                format!("checkout-{t:04}")
            } else {
                "checkout".to_string()
            };
            ProtoResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![proto_str("service.name", &service)],
                    ..Default::default()
                }),
                scope_spans: vec![ProtoScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

async fn append(
    ice: &IcebergContext,
    index: &str,
    traces: usize,
    spans_per_trace: usize,
    attr_bytes: usize,
    service_per_span: bool,
) {
    let config = IndexConfig {
        index_id: index.to_string(),
        doc_mapping: builtin_traces_template().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    let bloom_refs = config
        .doc_mapping
        .tag_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let events = otlp_proto_traces_to_events(proto_fixture(
        traces,
        spans_per_trace,
        attr_bytes,
        service_per_span,
    ));
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = map_carrier_batch(&carrier, &config).unwrap();
    ice.append_to_table(&ice.index_table_ident(index), mapped, &bloom_refs)
        .await
        .unwrap();
}

/// Everything at the interactive defaults except the admission budget the
/// ceilings are derived from. INJECTED, not set in the environment (project
/// convention: tests never `set_var`), and a short admission wait so a
/// saturated-budget arm costs milliseconds.
fn limits_with_budget(admission_budget_bytes: u64) -> ServerLimits {
    ServerLimits {
        admission_budget_bytes,
        admission_wait_timeout: std::time::Duration::from_millis(50),
        ..Default::default()
    }
}

fn app_with_budget(ice: &Arc<IcebergContext>, admission_budget_bytes: u64) -> Router {
    router(
        AppState::new(Arc::clone(ice), AuthConfig::open())
            .with_limits(limits_with_budget(admission_budget_bytes))
            // One partition, one file at a time: the ceilings are read at a
            // batch boundary, so a deterministic batch sequence keeps the
            // boundary arms arithmetic rather than a race.
            .with_query_scan(QueryScanConfig {
                target_partitions: Some(1),
                file_concurrency_limit: Some(1),
                ..Default::default()
            }),
    )
}

async fn get(app: &Router, path: &str) -> (StatusCode, HeaderMap, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(
        |_| serde_json::json!({ "raw_body": String::from_utf8_lossy(&bytes).to_string() }),
    );
    (status, headers, body)
}

/// A refusal carries no data and no retry contract: these ceilings are
/// deterministic, so a client that retries unchanged gets the same answer.
fn assert_refusal(
    label: &str,
    status: StatusCode,
    expect: StatusCode,
    headers: &HeaderMap,
    body: &serde_json::Value,
) {
    assert_eq!(status, expect, "{label}: {body}");
    assert!(
        body.get("data").is_none(),
        "{label}: a refusal returned partial data: {body}"
    );
    assert!(
        headers.get(header::RETRY_AFTER).is_none(),
        "{label}: a deterministic refusal must advertise no retry: {:?}",
        headers.get(header::RETRY_AFTER)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_jaeger_ceiling_refuses_whole_and_says_which_one_refused() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Four corpora, four indexes in one warehouse, so each ceiling is crossed
    // by the corpus that is about IT and nothing else.
    append(&ice, SMALL, 2, 2, 64, false).await;
    append(&ice, FAT, 1, FAT_SPANS, 64, false).await;
    append(&ice, WIDE, 1, WIDE_SPANS, WIDE_ATTR_BYTES, false).await;
    append(&ice, NAMES, DISTINCT_SERVICES, 1, 64, true).await;

    // `DebuggingRecorder::snapshot` DRAINS, so every snapshot below is the
    // delta since the previous one.
    snapshotter.snapshot();

    // ---- 1. Positive controls, at the packaged pod's ceilings ----
    let app = app_with_budget(&ice, PACKAGED_BUDGET);

    let (status, _, body) = get(&app, &format!("/api/v1/jaeger/{SMALL}/api/services")).await;
    assert_eq!(status, StatusCode::OK, "the service list: {body}");
    assert_eq!(body["data"], serde_json::json!(["checkout"]), "{body}");

    let (status, _, body) = get(
        &app,
        &format!("/api/v1/jaeger/{SMALL}/api/services/checkout/operations"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "one service's operations: {body}");
    assert_eq!(body["data"], serde_json::json!(["op-0", "op-1"]), "{body}");

    let (status, _, body) = get(
        &app,
        &format!("/api/v1/jaeger/{SMALL}/api/traces?service=checkout&limit=20"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the default search: {body}");
    assert_eq!(body["total"], 2, "{body}");
    let spans: usize = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|trace| trace["spans"].as_array().unwrap().len())
        .sum();
    assert_eq!(spans, 4, "the default search must be COMPLETE: {body}");

    let (status, _, body) = get(&app, &format!("/api/v1/jaeger/{FAT}/api/traces/{:032x}", 0)).await;
    assert_eq!(status, StatusCode::OK, "a {FAT_SPANS}-span trace: {body}");
    assert_eq!(
        body["data"][0]["spans"].as_array().unwrap().len(),
        FAT_SPANS,
        "a single-trace fetch must be COMPLETE under the default ceilings"
    );

    let (status, _, body) = get(
        &app,
        &format!("/api/v1/jaeger/{WIDE}/api/traces/{:032x}", 0),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a wide trace: {body}");
    assert_eq!(
        body["data"][0]["spans"].as_array().unwrap().len(),
        WIDE_SPANS,
        "512 KiB of Arrow fits the packaged 3.5 MiB render budget"
    );

    let (status, _, body) = get(&app, &format!("/api/v1/jaeger/{NAMES}/api/services")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{DISTINCT_SERVICES} services: {body}"
    );
    assert_eq!(
        body["data"].as_array().unwrap().len(),
        DISTINCT_SERVICES,
        "the name list must be COMPLETE under the packaged 10,237-name ceiling"
    );

    let snapshot = snapshotter.snapshot().into_vec();
    for breaker in [
        "jaeger_trace_limit",
        "jaeger_span_rows",
        "jaeger_render_bytes",
        "jaeger_name_rows",
    ] {
        assert_eq!(
            trips(&snapshot, breaker),
            0,
            "{breaker} refused one of the positive controls"
        );
    }

    // ---- 2. The trace ceiling: 400, before anything executes ----
    let (status, headers, body) = get(
        &app,
        &format!("/api/v1/jaeger/{SMALL}/api/traces?service=checkout&limit=844"),
    )
    .await;
    assert_refusal(
        "?limit=844",
        status,
        StatusCode::BAD_REQUEST,
        &headers,
        &body,
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("844") && msg.contains("843"),
        "the refusal names neither the value sent nor the ceiling: {msg}"
    );
    // The control is `?limit=843`, one under: same route, same corpus, 200.
    let (status, _, body) = get(
        &app,
        &format!("/api/v1/jaeger/{SMALL}/api/traces?service=checkout&limit=843"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "?limit=843 at the ceiling: {body}");
    assert_eq!(body["total"], 2, "{body}");

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(trips(&snapshot, "jaeger_trace_limit"), 1);
    assert_eq!(trips(&snapshot, "jaeger_span_rows"), 0);

    // ---- 3. The span-row ceiling, at the boundary ----
    // Exactly at it: the 200-span trace is returned WHOLE.
    let at_cap = app_with_budget(&ice, AT_200_SPAN_ROWS);
    let (status, _, body) = get(
        &at_cap,
        &format!("/api/v1/jaeger/{FAT}/api/traces/{:032x}", 0),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "exactly at the row ceiling: {body}");
    assert_eq!(
        body["data"][0]["spans"].as_array().unwrap().len(),
        FAT_SPANS,
        "a result exactly at the ceiling must be complete, not refused"
    );
    // One row over it: refused whole.
    let over_cap = app_with_budget(&ice, AT_199_SPAN_ROWS);
    let (status, headers, body) = get(
        &over_cap,
        &format!("/api/v1/jaeger/{FAT}/api/traces/{:032x}", 0),
    )
    .await;
    assert_refusal(
        "200 spans against a 199-row ceiling",
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        &headers,
        &body,
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("199 span rows") && msg.contains("rows"),
        "the row refusal does not name its unit and ceiling: {msg}"
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(trips(&snapshot, "jaeger_span_rows"), 1);
    assert_eq!(trips(&snapshot, "jaeger_render_bytes"), 0);

    // ---- 4. The render-byte ceiling: four rows, half a megabyte ----
    let small = app_with_budget(&ice, SMALL_BUDGET);
    let (status, headers, body) = get(
        &small,
        &format!("/api/v1/jaeger/{WIDE}/api/traces/{:032x}", 0),
    )
    .await;
    assert_refusal(
        "four wide spans against a 262,144-byte budget",
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        &headers,
        &body,
    );
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("bytes of Arrow data") && msg.contains("262144 bytes"),
        "a byte refusal must say it was BYTES (four rows are far under any row \
         ceiling here): {msg}"
    );

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(trips(&snapshot, "jaeger_render_bytes"), 1);
    assert_eq!(trips(&snapshot, "jaeger_span_rows"), 0);

    // ---- 5. The name ceiling, on both list routes ----
    let (status, headers, body) =
        get(&small, &format!("/api/v1/jaeger/{NAMES}/api/services")).await;
    assert_refusal(
        "800 services against a 731-name ceiling",
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        &headers,
        &body,
    );
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("731 names"),
        "the name refusal does not name its unit and ceiling: {body}"
    );
    // The operations route reads the same ceiling. Its corpus is one name per
    // service here, so the control is that it still answers.
    let (status, _, body) = get(
        &small,
        &format!("/api/v1/jaeger/{NAMES}/api/services/checkout-0000/operations"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "one service's operations: {body}");
    assert_eq!(body["data"], serde_json::json!(["op-0"]), "{body}");

    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(trips(&snapshot, "jaeger_name_rows"), 1);

    // Not once, on any of these: a policy refusal of a request that FITS the
    // pod is not the pool running out, and an operator who reads it as one
    // starts looking at pod memory instead of at `limit`.
    assert_eq!(
        trips(&snapshot, "pool_exhausted"),
        0,
        "a Jaeger ceiling was counted as pool exhaustion"
    );

    // ---- 6. The collect this surface now runs on, and what it settles ----
    //
    // Before #2184 `query_rows` called `df.collect()`: no exec-pool slot (so no
    // abort on a request that goes away), no rows-scanned breaker, and no
    // scan-attribution settle. Going through the shared mid-flight collector is
    // what fixed all three, and this is the HTTP-level evidence for it —
    // otherwise "reuse the existing collector" is a claim about the source
    // rather than about the binary.
    //
    // A refusal is the interesting case: it returns early, so a slot released
    // only on the straight-line path would leak (the 2026-08-17 defect
    // `PoolSlot` exists for). Every request above has finished, refusals
    // included, so the pool must be empty, nothing abandoned, and every plan's
    // partitions settled.
    assert!(
        counter_sum(
            &snapshot,
            "siglake_query_exec_route_total",
            &[("route", "pool")],
        ) > 0,
        "no Jaeger read went through the exec pool, so it is still collecting \
         with a bare df.collect()"
    );
    assert_eq!(
        gauge_value(&snapshot, "siglake_query_exec_pool_in_flight"),
        0.0,
        "a Jaeger read (or a refusal) leaked its exec pool slot"
    );
    assert_eq!(
        counter_sum(&snapshot, "siglake_query_exec_pool_abandoned_total", &[]),
        0,
        "a Jaeger read's collect was abandoned rather than completing"
    );
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_query_scan_attribution_incomplete_total",
            &[],
        ),
        0,
        "a Jaeger read answered with scan partitions still unwinding"
    );
}
