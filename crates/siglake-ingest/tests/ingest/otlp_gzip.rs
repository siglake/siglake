//! `Content-Encoding` on the OTLP/HTTP routes.
//!
//! The OTLP specification says every server MUST accept `gzip` and MUST limit the
//! body size *after* decompression. The Collector's `otlphttp` exporter gzips by
//! default, so without this a stock Collector config gets `400 invalid OTLP`.
//! These tests pin both requirements, the `415` for any other coding, and the
//! rule that the auth middleware refuses a request before any byte is inflated.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::Router;
use flate2::write::GzEncoder;
use flate2::Compression;
use prost::Message;
use tower::util::ServiceExt;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use siglake_ingest::{router, AppState, AuthTokens, TenantWalRouter};
use siglake_wal::{list_sealed, read_segment, WalWriter};

const JSON: &str = "application/json";
const PROTOBUF: &str = "application/x-protobuf";

/// The ingest body cap when `SIGLAKE_INGEST_MAX_BODY_BYTES` is unset. Tests in
/// this binary run in one process, so they rely on the default rather than set
/// the variable.
const DEFAULT_BODY_CAP: usize = 16 * 1024 * 1024;

/// One event per segment, so a single accepted request leaves a sealed segment
/// to read back.
fn build_app(tokens: Option<AuthTokens>) -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let writer = WalWriter::with_thresholds(
        tmp.path().join("direct"),
        "gzip",
        1,
        Duration::from_secs(60),
    )
    .unwrap();
    let mut state = AppState::with_streaming(writer);
    state.tenants = Some(Arc::new(TenantWalRouter::new(
        tmp.path(),
        "gzip",
        1,
        Duration::from_secs(60),
    )));
    if let Some(tokens) = tokens {
        state = state.with_tokens(tokens);
    }
    (router(state), tmp)
}

fn gzip(raw: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(raw).unwrap();
    encoder.finish().unwrap()
}

/// A gzip stream of `len` spaces, which JSON would accept as whitespace. It is
/// written in chunks, so the test never holds the decompressed bytes either.
fn gzip_of_spaces(len: usize) -> Vec<u8> {
    let chunk = vec![b' '; 1024 * 1024];
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    let mut left = len;
    while left > 0 {
        let n = left.min(chunk.len());
        encoder.write_all(&chunk[..n]).unwrap();
        left -= n;
    }
    encoder.finish().unwrap()
}

async fn post(
    app: &Router,
    uri: &str,
    content_type: &str,
    encoding: Option<&str>,
    auth: Option<&str>,
    body: Vec<u8>,
) -> Response {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    if let Some(encoding) = encoding {
        req = req.header(CONTENT_ENCODING, encoding);
    }
    if let Some(auth) = auth {
        req = req.header("authorization", auth);
    }
    app.clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap()
}

async fn body_text(resp: Response) -> String {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Every `raw` value in the sealed segments under `dir`.
fn raw_values(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    for segment in list_sealed(dir).unwrap_or_default() {
        for batch in read_segment(&segment).unwrap() {
            let raw = batch
                .column_by_name("raw")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            out.extend(raw.iter().flatten().map(str::to_owned));
        }
    }
    out
}

fn logs_json(body: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": "gzip-svc" } }
            ]},
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": body }
                }]
            }]
        }]
    }))
    .unwrap()
}

fn logs_protobuf(body: &str) -> Vec<u8> {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(body.into())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn traces_json(name: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "resourceSpans": [{
            "scopeSpans": [{
                "spans": [{
                    "traceId": "00112233445566778899aabbccddeeff",
                    "spanId": "8899aabbccddeeff",
                    "name": name,
                    "kind": 2,
                    "startTimeUnixNano": "1700000000000000000",
                    "endTimeUnixNano": "1700000000000005000"
                }]
            }]
        }]
    }))
    .unwrap()
}

fn traces_protobuf(name: &str) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0x11; 16],
                    span_id: vec![0x22; 8],
                    name: name.into(),
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_000_005_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// A gzip body in either format, on either signal, lands the same rows as the
/// plain body would. `identity` is also a valid coding and passes through
/// unchanged.
#[tokio::test]
async fn gzip_bodies_land_on_both_signals_in_both_formats() {
    let (app, tmp) = build_app(None);
    let cases: [(&str, &str, Option<&str>, Vec<u8>); 5] = [
        (
            "/v1/logs",
            JSON,
            Some("gzip"),
            gzip(&logs_json("gzip-json-log")),
        ),
        (
            "/v1/logs",
            PROTOBUF,
            Some("gzip"),
            gzip(&logs_protobuf("gzip-proto-log")),
        ),
        (
            "/v1/logs",
            JSON,
            Some("identity"),
            logs_json("identity-json-log"),
        ),
        (
            "/v1/traces",
            JSON,
            Some("gzip"),
            gzip(&traces_json("gzip-json-span")),
        ),
        (
            "/v1/traces",
            PROTOBUF,
            Some("gzip"),
            gzip(&traces_protobuf("gzip-proto-span")),
        ),
    ];
    for (uri, content_type, encoding, body) in cases {
        let resp = post(&app, uri, content_type, encoding, None, body).await;
        let status = resp.status();
        assert_eq!(
            status,
            StatusCode::OK,
            "{uri} {content_type} {encoding:?}: {}",
            body_text(resp).await
        );
    }

    let logs = raw_values(&tmp.path().join("default"));
    for marker in ["gzip-json-log", "gzip-proto-log", "identity-json-log"] {
        assert!(
            logs.iter().any(|raw| raw.contains(marker)),
            "{marker} missing from the logs WAL: {logs:?}"
        );
    }
    let spans = raw_values(&tmp.path().join("default").join("siglake-traces-default"));
    for marker in ["gzip-json-span", "gzip-proto-span"] {
        assert!(
            spans.iter().any(|raw| raw.contains(marker)),
            "{marker} missing from the traces WAL: {spans:?}"
        );
    }
}

/// Any coding other than gzip is `415` and names the one that would work, rather
/// than a `400` from parsing compressed bytes. That includes codings that
/// tower-http could decode if its features were on (`zstd`, `deflate`, `br`),
/// and one it cannot (`snappy`).
#[tokio::test]
async fn other_content_codings_are_415_naming_gzip() {
    let (app, tmp) = build_app(None);
    for uri in ["/v1/logs", "/v1/traces"] {
        for encoding in ["zstd", "deflate", "br", "snappy"] {
            let resp = post(&app, uri, JSON, Some(encoding), None, logs_json("x")).await;
            assert_eq!(
                resp.status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{uri} Content-Encoding: {encoding}"
            );
            assert_eq!(
                resp.headers().get(ACCEPT_ENCODING).unwrap(),
                "gzip",
                "{uri} Content-Encoding: {encoding}"
            );
        }
    }
    assert!(raw_values(&tmp.path().join("default")).is_empty());
}

/// The spec's second requirement: the body cap counts decompressed bytes. A
/// 256 MiB stream that is a few megabytes on the wire, well under the 16 MiB
/// cap, is refused at the cap, so the server never inflates it whole. The
/// boundary is exact. A body that decodes to exactly the cap reaches the parser,
/// and all-whitespace JSON is a `400` there. One byte more is a `413`.
#[tokio::test]
async fn gzip_cap_counts_decoded_bytes() {
    let (app, _tmp) = build_app(None);

    let bomb = gzip_of_spaces(256 * 1024 * 1024);
    assert!(
        bomb.len() < DEFAULT_BODY_CAP / 4,
        "the bomb must be far under the cap on the wire, or the 413 proves nothing \
         about decoded bytes; got {} bytes",
        bomb.len()
    );
    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, bomb).await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let at_cap = gzip_of_spaces(DEFAULT_BODY_CAP);
    let resp = post(&app, "/v1/traces", JSON, Some("gzip"), None, at_cap).await;
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(
        text.contains("invalid OTLP"),
        "a body of exactly the cap must reach the parser, not stop at the cap: {text}"
    );

    let just_over = gzip_of_spaces(DEFAULT_BODY_CAP + 1);
    let resp = post(&app, "/v1/traces", JSON, Some("gzip"), None, just_over).await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

/// A stream labelled gzip that is not one is a `400`. It must not reach the
/// parser as raw bytes. The framework refuses it while reading the body, so it
/// is the plain-text rejection the OpenAPI `400` description promises, not the
/// handler's JSON envelope.
#[tokio::test]
async fn corrupt_gzip_is_400() {
    let (app, tmp) = build_app(None);
    let mut truncated = gzip(&logs_json("truncated"));
    truncated.truncate(truncated.len() / 2);
    for body in [logs_json("not-compressed"), truncated] {
        let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, body).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let text = body_text(resp).await;
        assert!(
            content_type.starts_with("text/plain") && !text.contains("invalid OTLP"),
            "expected the framework's plain-text rejection, got {content_type:?}: {text}"
        );
    }
    assert!(raw_values(&tmp.path().join("default")).is_empty());
}

/// The decoder sits inside the auth middleware. An unauthenticated caller gets
/// `401` whatever it sends, so it cannot make the server inflate anything or
/// probe the accepted codings. Once authenticated, gzip works as usual.
#[tokio::test]
async fn auth_refuses_before_any_decoding() {
    let (app, _tmp) = build_app(Some(AuthTokens::from_tokens(["sekret"])));

    let bomb = gzip_of_spaces(64 * 1024 * 1024);
    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, bomb).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = post(&app, "/v1/logs", JSON, Some("zstd"), None, logs_json("x")).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = post(
        &app,
        "/v1/logs",
        JSON,
        Some("gzip"),
        Some("Bearer sekret"),
        gzip(&logs_json("authed")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}
