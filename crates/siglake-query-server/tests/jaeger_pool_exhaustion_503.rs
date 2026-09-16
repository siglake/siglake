//! The Jaeger read API gives the same answer as `/api/v1/sql` when the query
//! memory pool refuses an allocation: 503 + `Retry-After`, counted.
//!
//! THE CONTRACT THIS PINS (#2095). `TraceQueryContext::query_rows` runs on the
//! process-wide pool that `tests/pool_exhaustion_503.rs` bounds for the SQL
//! routes — the same `FairSpillPool`, the same spill cap — and Jaeger's trace
//! search is an aggregate plus a `TopK` whose size the client chooses
//! (`?limit=`). Until this test existed that route mapped a
//! `DataFusionError::ResourcesExhausted` straight to `ApiError::internal`: HTTP
//! 500, no `Retry-After`, and no
//! `siglake_query_breaker_trips_total{breaker="pool_exhausted"}` — so normal
//! memory pressure reached the Jaeger UI as "the server broke" and reached the
//! operator as nothing at all. Only capacity moves: a refusal DataFusion did
//! not raise (a read-only planner refusal, a plan error) is still a 500, which
//! `jaeger_routes`' own tests pin at the call site.
//!
//! Its own binary, on purpose, for the reason `pool_exhaustion_503.rs` gives:
//! the pool is a process-wide `OnceLock`, pinned here through
//! `preset_query_memory_pool_bytes` rather than `set_var`. Nothing here reads
//! or writes the environment.

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
use siglake_query_server::error::{POOL_EXHAUSTED_BREAKER, POOL_EXHAUSTED_RETRY_AFTER_SECS};
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig};
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::index_manager::builtin_traces_template;
use tower::util::ServiceExt;

/// A pool no real query pod would run at, chosen so BOTH halves of this test
/// are far from the boundary. MEASURED on 2026-09-08 against the fixture below,
/// single-partition (the plan is pinned to one partition and one file at a time
/// so the pool arithmetic is deterministic rather than a race between the scan
/// and the operator that follows it):
///
/// - `siglake-scan-decode` holds ~6.9 MB for the duration of every query here
///   (one file's decoded working set; it degrades rather than fails, so it just
///   occupies the pool),
/// - a sort that may spill pre-reserves DataFusion's 10 MB merge headroom, so
///   the two control queries need ~17 MB and pass with ~7 MB to spare,
/// - the refused query's `TopK` asks for 25.2 MB in one go against ~17 MB free.
///
/// Halving this makes the controls fail too (at 16 MiB all three are refused),
/// which is the failure mode a positive control exists to catch.
const POOL_BYTES: u64 = 24 * 1024 * 1024;

const INDEX: &str = "siglake-traces-default";

/// A second index holding ONE trace of 500 spans, each carrying 64 KiB of
/// attributes: ~32 MB of Arrow in a single decoded batch, more than the whole
/// pool above, so the sort that orders a trace's spans is refused the moment it
/// reserves for that batch. Spilling cannot save a single batch that does not
/// fit, which is what makes this refusal deterministic.
///
/// It exists because #2184 took the CLIENT-driven route to this refusal away,
/// and deliberately: `?limit=` is now bounded by a derived trace ceiling
/// (400, before the reservation), and that ceiling is small enough — a few
/// hundred traces — that the `TopK` it sizes can no longer outgrow any pool a
/// pod would run. What is left is a DATA-driven refusal: one trace whose spans
/// do not fit. So this file still pins #2095's contract (a pool refusal on the
/// Jaeger surface is 503 + `Retry-After`, counted), on the shape that can still
/// produce it.
const WIDE_INDEX: &str = "siglake-traces-wide";
const WIDE_SPANS: i64 = 500;
const WIDE_ATTR_BYTES: usize = 64 * 1024;
/// Spans per Parquet file, each its own trace, so the `GROUP BY trace_id` has
/// as many groups as there are rows and the `TopK` above it as many candidates.
/// Many small files rather than a few big ones: the decode reservation is per
/// FILE, and it is the term that would otherwise crowd the pool on every query
/// including the controls.
const SPANS_PER_FILE: i64 = 5_000;
const FILES: i64 = 40;

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

fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_string(),
        value: Some(ProtoAnyValue {
            value: Some(ProtoValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// The Jaeger fixture of `jaeger_routes`' own tests (one resource, hex trace
/// and span ids, an attribute per span), scaled to a table whose trace search
/// cannot fit the pool above.
fn proto_fixture(file: i64) -> ExportTraceServiceRequest {
    let spans = (0..SPANS_PER_FILE)
        .map(|i| {
            let k = (i * FILES + file) as u64;
            let start = 1_700_000_000_000_000_000 + k * 700_000;
            ProtoSpan {
                trace_id: k.to_be_bytes().repeat(2),
                span_id: (k ^ 0xa5a5_a5a5_a5a5_a5a5).to_be_bytes().to_vec(),
                name: format!("GET /api/v1/items/{}", k % 1000),
                kind: span::SpanKind::Server as i32,
                start_time_unix_nano: start,
                end_time_unix_nano: start + 50_000_000,
                attributes: vec![proto_str("http.method", "GET")],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ProtoResourceSpans {
            resource: Some(ProtoResource {
                attributes: vec![
                    proto_str("host.name", "trace-host"),
                    proto_str("service.name", "checkout"),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ProtoScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn wide_proto_fixture() -> ExportTraceServiceRequest {
    let payload = "x".repeat(WIDE_ATTR_BYTES);
    let spans = (0..WIDE_SPANS)
        .map(|i| {
            let start = 1_700_000_000_000_000_000 + (i as u64) * 700_000;
            ProtoSpan {
                trace_id: 0u64.to_be_bytes().repeat(2),
                span_id: ((i as u64) ^ 0x5a5a_5a5a_5a5a_5a5a).to_be_bytes().to_vec(),
                name: "GET /api/v1/items".to_string(),
                kind: span::SpanKind::Server as i32,
                start_time_unix_nano: start,
                end_time_unix_nano: start + 50_000_000,
                attributes: vec![proto_str("payload", &payload)],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ProtoResourceSpans {
            resource: Some(ProtoResource {
                attributes: vec![proto_str("service.name", "checkout")],
                ..Default::default()
            }),
            scope_spans: vec![ProtoScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn append_wide_trace_fixture(ice: &IcebergContext) {
    let config = IndexConfig {
        index_id: WIDE_INDEX.to_string(),
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
    let events = otlp_proto_traces_to_events(wide_proto_fixture());
    let carrier = events_to_record_batch(&events).unwrap();
    let mapped = map_carrier_batch(&carrier, &config).unwrap();
    ice.append_to_table(&ice.index_table_ident(WIDE_INDEX), mapped, &bloom_refs)
        .await
        .unwrap();
}

async fn append_trace_fixture(ice: &IcebergContext) {
    let config = IndexConfig {
        index_id: INDEX.to_string(),
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
    for file in 0..FILES {
        let events = otlp_proto_traces_to_events(proto_fixture(file));
        let carrier = events_to_record_batch(&events).unwrap();
        let mapped = map_carrier_batch(&carrier, &config).unwrap();
        ice.append_to_table(&ice.index_table_ident(INDEX), mapped, &bloom_refs)
            .await
            .unwrap();
    }
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

fn retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_allocation_on_the_jaeger_read_api_is_a_503_with_retry_after() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    assert!(
        siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES),
        "the query memory pool was already built at another size before this binary \
         could bound it at {POOL_BYTES} bytes — nothing may touch the pool before this"
    );

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    append_trace_fixture(&ice).await;
    append_wide_trace_fixture(&ice).await;
    // One partition, one file decoded at a time. Not how a query pod runs: it
    // makes the pool arithmetic above deterministic. Left at the defaults, the
    // per-partition decode reservations take whatever is free at the moment
    // they register, so which operator gets refused (and whether the controls
    // fit) moves between runs — measured, on this fixture, as a control that
    // passed and failed alternately at the same pool size.
    let app = router(
        AppState::new(ice, AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(1),
            file_concurrency_limit: Some(1),
            ..Default::default()
        }),
    );
    let base = format!("/api/v1/jaeger/{INDEX}/api");
    // `DebuggingRecorder::snapshot` drains each metric, establishing an exact
    // zero baseline for the request/status assertions below.
    snapshotter.snapshot();

    // The bound must not have made the routes useless, or the refusal below
    // would pass for the wrong reason: the service list and a default-limit
    // trace search both answer from the same pool.
    let (status, _, body) = get(&app, &format!("{base}/services")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the service list must fit a {POOL_BYTES}-byte pool: {body}"
    );
    assert_eq!(body["data"], serde_json::json!(["checkout"]));
    let (status, _, body) = get(&app, &format!("{base}/traces?service=checkout&limit=5")).await;
    assert_eq!(status, StatusCode::OK, "a 5-trace search must fit: {body}");
    assert_eq!(body["total"], 5, "{body}");

    // The other two registered Jaeger routes use the same response-layer
    // accounting. Together these four successful calls cover every route.
    let (status, _, body) = get(&app, &format!("{base}/services/checkout/operations")).await;
    assert_eq!(status, StatusCode::OK, "operation listing must fit: {body}");
    assert!(
        body["data"]
            .as_array()
            .is_some_and(|names| !names.is_empty()),
        "operation listing must return fixture names: {body}"
    );
    let trace_id = format!("{:032x}", 0);
    let (status, _, body) = get(&app, &format!("{base}/traces/{trace_id}")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "single-trace lookup must fit: {body}"
    );
    assert_eq!(body["total"], 1, "{body}");

    // Axum's query extractor rejects this before entering `find_traces`.
    let (status, _, _) = get(
        &app,
        &format!("{base}/traces?service=checkout&limit=not-a-number"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // This reaches the handler and fails its index lookup.
    let missing_base = "/api/v1/jaeger/missing-index/api";
    let (status, _, body) = get(&app, &format!("{missing_base}/services")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "missing index: {body}");

    // A hostile `?limit=` no longer reaches the pool at all: #2184's derived
    // trace ceiling refuses it with 400 BEFORE the reservation, the index
    // lookup and the planner. That is the point of that ceiling — this request
    // used to be answered by whatever the pool happened to have free at that
    // instant, and this file's own comment above records a control that
    // "passed and failed alternately at the same pool size". Asserted here so
    // the change of answer is deliberate rather than discovered later.
    let (status, headers, body) = get(
        &app,
        &format!(
            "{base}/traces?service=checkout&limit={}",
            SPANS_PER_FILE * FILES
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a hostile ?limit= must be refused before it can reach the pool: {body}"
    );
    assert!(
        headers.get(header::RETRY_AFTER).is_none(),
        "a deterministic 400 must advertise no retry: {:?}",
        headers.get(header::RETRY_AFTER)
    );

    // The refusal that IS the pool: one trace whose 500 spans carry 64 KiB of
    // attributes each, so the sort that orders them reserves ~32 MB — more
    // than this pool holds — in one go, for a batch spilling cannot break up.
    let wide_base = format!("/api/v1/jaeger/{WIDE_INDEX}/api");
    let (status, headers, body) = get(&app, &format!("{wide_base}/traces/{:032x}", 0)).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a pool refusal on the Jaeger read API must be 503, got {status} with body {body}"
    );
    assert_eq!(
        retry_after_secs(&headers),
        Some(POOL_EXHAUSTED_RETRY_AFTER_SECS),
        "a Jaeger 503 must carry the same Retry-After as the SQL routes, got {:?}",
        headers.get(header::RETRY_AFTER)
    );
    assert_eq!(body["code"], 503, "body code must match the status: {body}");
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("query memory pool exhausted"),
        "the body must say what refused, got: {msg}"
    );
    assert!(
        msg.contains("remain available for the total pool") || msg.contains("Failed to allocate"),
        "DataFusion's own account (consumer and bytes) must survive to the client: {msg}"
    );

    // Counted once, under the breaker scheme the SQL routes use: one refused
    // request, one trip. Not zero — that counter is the signal that explains
    // the 503 to an operator — and not two, this route runs one query.
    let snapshot = snapshotter.snapshot().into_vec();
    let trips = counter_sum(
        &snapshot,
        "siglake_query_breaker_trips_total",
        &[
            ("breaker", POOL_EXHAUSTED_BREAKER),
            ("priority", "interactive"),
        ],
    );
    assert_eq!(
        trips, 1,
        "one pool refusal must be exactly one pool_exhausted trip, got {trips}"
    );

    // And the hostile `?limit=` above was counted as the ceiling it hit, not
    // as capacity: an operator reading `pool_exhausted` for a request the pool
    // never saw would go looking at pod memory instead of at `limit`.
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_query_breaker_trips_total",
            &[
                ("breaker", "jaeger_trace_limit"),
                ("priority", "interactive")
            ],
        ),
        1,
        "the trace ceiling must count the refusal it made"
    );

    // Two 400s now: axum's `limit=not-a-number` rejection and the trace
    // ceiling's refusal of `?limit={SPANS_PER_FILE}*{FILES}`.
    for (status, expected) in [("200", 4), ("400", 2), ("404", 1), ("503", 1)] {
        let requests = counter_sum(
            &snapshot,
            "siglake_query_requests_total",
            &[("endpoint", "jaeger"), ("status", status)],
        );
        assert_eq!(
            requests, expected,
            "Jaeger status={status} response delta must be exactly {expected}, got {requests}"
        );
    }

    // The pool gives everything back, so the route still serves afterwards.
    let (status, _, body) = get(&app, &format!("{base}/services")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the pod must still serve Jaeger reads after refusing: {body}"
    );
    let after_recovery = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &after_recovery,
            "siglake_query_requests_total",
            &[("endpoint", "jaeger"), ("status", "200")],
        ),
        1,
        "the successful recovery response must add exactly one Jaeger 200"
    );
}
