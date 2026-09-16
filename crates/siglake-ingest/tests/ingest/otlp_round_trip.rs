//! End-to-end OTLP ingest tests against the axum router directly.

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use prost::Message;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value as ProtoValue, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use siglake_ingest::{router, AppState, AuthTokens, TenantRouting, TenantWalRouter};
use siglake_wal::{list_sealed, list_tenant_dirs, read_segment, WalWriter};

async fn build_app(tokens: Option<AuthTokens>) -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let tenants = TenantWalRouter::new(tmp.path(), "test", 5, Duration::from_secs(60));
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 5, Duration::from_secs(60)).unwrap();
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: tokens.filter(|t| !t.is_empty()).map(Arc::new),
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
    };
    (router(state), tmp)
}

async fn build_over_mem_limit_app() -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 5, Duration::from_secs(60)).unwrap();
    let guard = Arc::new(siglake_ingest::mem_guard::MemoryGuard::new(1));
    guard.observe(1024 * 1024);
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: None,
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: Some(guard),
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
    };
    (router(state), tmp)
}

fn otlp_logs(host: &str, body: &str, sourcetype: &str) -> serde_json::Value {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": host } },
                { "key": "service.name", "value": { "stringValue": "svc" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "test" },
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": body },
                    "attributes": [
                        { "key": "sourcetype", "value": { "stringValue": sourcetype } },
                        { "key": "index", "value": { "stringValue": "main" } }
                    ]
                }]
            }]
        }]
    })
}

fn otlp_traces_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "resourceSpans": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "trace-host" } },
                { "key": "service.name", "value": { "stringValue": "trace-svc" } }
            ]},
            "scopeSpans": [{
                "spans": [{
                    "traceId": "00112233445566778899aabbccddeeff",
                    "spanId": "8899aabbccddeeff",
                    "parentSpanId": "0011223344556677",
                    "name": name,
                    "kind": "SPAN_KIND_SERVER",
                    "startTimeUnixNano": "1700000000000000000",
                    "endTimeUnixNano": "1700000000000005000",
                    "attributes": [
                        { "key": "http.method", "value": { "stringValue": "GET" } }
                    ]
                }]
            }]
        }]
    })
}

fn otlp_traces_protobuf(name: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    proto_str("host.name", "trace-host"),
                    proto_str("service.name", "trace-svc"),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![
                        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
                        0xcc, 0xdd, 0xee, 0xff,
                    ],
                    span_id: vec![0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff],
                    parent_span_id: vec![0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
                    name: name.to_string(),
                    kind: opentelemetry_proto::tonic::trace::v1::span::SpanKind::Server as i32,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_000_005_000,
                    attributes: vec![proto_str("http.method", "GET")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

struct SendSpec<'a> {
    method: &'a str,
    uri: &'a str,
    tenant: Option<&'a str>,
    index: Option<&'a str>,
    auth: Option<&'a str>,
    content_type: &'a str,
    body: Vec<u8>,
}

async fn send(app: &Router, spec: SendSpec<'_>) -> axum::response::Response {
    let mut req = Request::builder()
        .method(spec.method)
        .uri(spec.uri)
        .header("content-type", spec.content_type);
    if let Some(tenant) = spec.tenant {
        req = req.header("X-Scope-OrgID", tenant);
    }
    if let Some(index) = spec.index {
        req = req.header("X-Siglake-Index", index);
    }
    if let Some(auth) = spec.auth {
        req = req.header("Authorization", auth);
    }
    app.clone()
        .oneshot(req.body(Body::from(spec.body)).unwrap())
        .await
        .unwrap()
}

async fn post_logs(
    app: &Router,
    tenant: Option<&str>,
    index: Option<&str>,
    payload: &serde_json::Value,
) -> axum::response::Response {
    send(
        app,
        SendSpec {
            method: "POST",
            uri: "/v1/logs",
            tenant,
            index,
            auth: None,
            content_type: "application/json",
            body: serde_json::to_vec(payload).unwrap(),
        },
    )
    .await
}

async fn post_traces_json(
    app: &Router,
    tenant: Option<&str>,
    index: Option<&str>,
    payload: &serde_json::Value,
) -> axum::response::Response {
    send(
        app,
        SendSpec {
            method: "POST",
            uri: "/v1/traces",
            tenant,
            index,
            auth: None,
            content_type: "application/json",
            body: serde_json::to_vec(payload).unwrap(),
        },
    )
    .await
}

async fn post_traces_protobuf(
    app: &Router,
    tenant: Option<&str>,
    index: Option<&str>,
    payload: &ExportTraceServiceRequest,
) -> axum::response::Response {
    send(
        app,
        SendSpec {
            method: "POST",
            uri: "/v1/traces",
            tenant,
            index,
            auth: None,
            content_type: "application/x-protobuf",
            body: payload.encode_to_vec(),
        },
    )
    .await
}

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn health_endpoint() {
    let (app, _tmp) = build_app(None).await;
    let resp = send(
        &app,
        SendSpec {
            method: "GET",
            uri: "/healthz",
            tenant: None,
            index: None,
            auth: None,
            content_type: "application/json",
            body: vec![],
        },
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["code"], 200);
}

#[tokio::test]
async fn otlp_logs_land_in_default_tenant_wal() {
    let (app, tmp) = build_app(None).await;
    for i in 0..6 {
        let resp = post_logs(
            &app,
            None,
            None,
            &otlp_logs("h1", &format!("event {i}"), "app:json"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let dir = tmp.path().join("default");
    let sealed = list_sealed(&dir).unwrap();
    assert!(!sealed.is_empty());
    let batches = read_segment(&sealed[0]).unwrap();
    let batch = &batches[0];
    let host = batch
        .column_by_name("host")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(host.value(0), "h1");
    let raw = batch
        .column_by_name("raw")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert!(raw.value(0).contains("event"));
}

#[tokio::test]
async fn x_scope_orgid_routes_to_separate_tenant_dirs() {
    let (app, tmp) = build_app(None).await;
    for _ in 0..6 {
        assert_eq!(
            post_logs(&app, Some("acme"), None, &otlp_logs("h", "e", "st"))
                .await
                .status(),
            StatusCode::OK
        );
    }
    for _ in 0..6 {
        assert_eq!(
            post_logs(&app, Some("globex"), None, &otlp_logs("h", "e", "st"))
                .await
                .status(),
            StatusCode::OK
        );
    }
    let tenants = list_tenant_dirs(tmp.path()).unwrap();
    assert!(tenants.iter().any(|(n, _)| n == "acme"));
    assert!(tenants.iter().any(|(n, _)| n == "globex"));
}

#[tokio::test]
async fn invalid_tenant_id_is_rejected() {
    let (app, _tmp) = build_app(None).await;
    let resp = post_logs(&app, Some("../etc"), None, &otlp_logs("h", "e", "st")).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn x_siglake_index_routes_segments_and_rejects_invalid_values() {
    let (app, tmp) = build_app(None).await;

    for i in 0..6 {
        let resp = post_logs(
            &app,
            Some("acme"),
            Some("app1"),
            &otlp_logs("h1", &format!("indexed {i}"), "app:json"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    for i in 0..6 {
        let resp = post_logs(
            &app,
            Some("acme"),
            None,
            &otlp_logs("h1", &format!("events {i}"), "app:json"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    assert!(!list_sealed(&tmp.path().join("acme").join("app1"))
        .unwrap()
        .is_empty());
    assert!(!list_sealed(&tmp.path().join("acme")).unwrap().is_empty());

    let invalid = send(
        &app,
        SendSpec {
            method: "POST",
            uri: "/v1/logs",
            tenant: Some("acme"),
            index: Some("Bad"),
            auth: None,
            content_type: "application/json",
            body: b"not-json".to_vec(),
        },
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let invalid_body = json_body(invalid).await;
    assert!(invalid_body["text"]
        .as_str()
        .unwrap_or_default()
        .contains("x-siglake-index"));

    let reserved = send(
        &app,
        SendSpec {
            method: "POST",
            uri: "/v1/logs",
            tenant: Some("acme"),
            index: Some("sealed"),
            auth: None,
            content_type: "application/json",
            body: b"not-json".to_vec(),
        },
    )
    .await;
    assert_eq!(reserved.status(), StatusCode::BAD_REQUEST);
    let reserved_body = json_body(reserved).await;
    assert!(reserved_body["text"]
        .as_str()
        .unwrap_or_default()
        .contains("x-siglake-index"));
}

#[tokio::test]
async fn bearer_token_auth_when_configured() {
    let (app, _tmp) = build_app(Some(AuthTokens::from_tokens(["sekret"]))).await;
    let payload = otlp_logs("h", "e", "st");

    assert_eq!(
        post_logs(&app, None, None, &payload).await.status(),
        StatusCode::UNAUTHORIZED
    );

    let wrong = send(
        &app,
        SendSpec {
            method: "POST",
            uri: "/v1/logs",
            tenant: None,
            index: None,
            auth: Some("Bearer nope"),
            content_type: "application/json",
            body: serde_json::to_vec(&payload).unwrap(),
        },
    )
    .await;
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

    let ok = send(
        &app,
        SendSpec {
            method: "POST",
            uri: "/v1/logs",
            tenant: None,
            index: None,
            auth: Some("Bearer sekret"),
            content_type: "application/json",
            body: serde_json::to_vec(&payload).unwrap(),
        },
    )
    .await;
    assert_eq!(ok.status(), StatusCode::OK);
}

#[tokio::test]
async fn mem_breaker_sheds_with_503_and_retry_after() {
    let (app, _tmp) = build_over_mem_limit_app().await;
    let resp = post_logs(&app, None, None, &otlp_logs("h", "e", "st")).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        resp.headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    let body = json_body(resp).await;
    assert_eq!(body["code"], 503);
}

#[tokio::test]
async fn otlp_traces_json_land_in_default_traces_wal_dir() {
    let (app, tmp) = build_app(None).await;
    for i in 0..6 {
        let resp =
            post_traces_json(&app, None, None, &otlp_traces_json(&format!("trace {i}"))).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let sealed = list_sealed(&tmp.path().join("default").join("siglake-traces-default")).unwrap();
    assert!(!sealed.is_empty());
    let batches = read_segment(&sealed[0]).unwrap();
    let raw = batches[0]
        .column_by_name("raw")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert!(raw.value(0).contains("trace"));
}

#[tokio::test]
async fn otlp_traces_protobuf_routes_to_custom_index_dir() {
    let (app, tmp) = build_app(None).await;
    for i in 0..6 {
        let resp = post_traces_protobuf(
            &app,
            Some("acme"),
            Some("siglake-traces-app"),
            &otlp_traces_protobuf(&format!("proto-trace {i}")),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    assert!(
        !list_sealed(&tmp.path().join("acme").join("siglake-traces-app"))
            .unwrap()
            .is_empty()
    );
}

fn proto_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(ProtoValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}
