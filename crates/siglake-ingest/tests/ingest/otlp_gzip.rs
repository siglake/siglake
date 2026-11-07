//! `Content-Encoding` on the OTLP/HTTP routes.
//!
//! The OTLP specification says every server MUST accept `gzip` and MUST limit the
//! body size *after* decompression. The Collector's `otlphttp` exporter gzips by
//! default, so without this a stock Collector config gets `400 invalid OTLP`.
//! These tests pin both requirements, that every member of a multi-member gzip
//! stream is decoded, the `415` for any other coding, and the rules that bound
//! what a gzip request can make the server inflate: auth and the memory breaker
//! refuse before any byte is inflated, decoded bytes in flight share one budget,
//! and a body that stalls is cut off at the body-read deadline.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, RETRY_AFTER};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::Router;
use flate2::write::GzEncoder;
use flate2::Compression;
use prost::Message;
use tokio::sync::oneshot;
use tower::util::ServiceExt;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use siglake_ingest::mem_guard::MemoryGuard;
use siglake_ingest::{
    router, AppState, AuthTokens, TenantWalRouter, DEFAULT_BODY_READ_TIMEOUT,
    DEFAULT_GZIP_DECODE_BUDGET_BYTES,
};
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
    let (state, tmp) = build_state();
    let state = match tokens {
        Some(tokens) => state.with_tokens(tokens),
        None => state,
    };
    (router(state), tmp)
}

fn build_state() -> (AppState, tempfile::TempDir) {
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
    (state, tmp)
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

/// A request body that the test controls. `prefix` is sent first. `polled`
/// fires when the server asks for more after it, or, with no prefix, when it
/// first reads the body at all, which for a gzip body is only once the request
/// has passed every check before decoding. The body then stays open until
/// `release` is sent or dropped, and ends there.
struct HeldBody {
    body: Body,
    polled: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
}

fn held_body(prefix: Vec<u8>) -> HeldBody {
    let (polled_tx, polled) = oneshot::channel();
    let (release, release_rx) = oneshot::channel::<()>();
    let stream = async_stream::stream! {
        if !prefix.is_empty() {
            yield Ok::<_, std::io::Error>(Bytes::from(prefix));
        }
        let _ = polled_tx.send(());
        let _ = release_rx.await;
    };
    HeldBody {
        body: Body::from_stream(stream),
        polled,
        release,
    }
}

/// The start of a gzip stream of `len` spaces: flushed, so all of it decodes,
/// but without the trailer, so a body that ends there is truncated.
fn gzip_prefix_of_spaces(len: usize) -> Vec<u8> {
    let chunk = vec![b' '; 1024 * 1024];
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    let mut left = len;
    while left > 0 {
        let n = left.min(chunk.len());
        encoder.write_all(&chunk[..n]).unwrap();
        left -= n;
    }
    encoder.flush().unwrap();
    encoder.get_ref().clone()
}

/// Send a request that must be answered without its held body being read. If
/// the server reads it instead, it would wait on the body forever, so a
/// regression fails here rather than hanging the test.
async fn answered_unread(app: Router, req: Request<Body>) -> Response {
    tokio::time::timeout(Duration::from_secs(10), app.oneshot(req))
        .await
        .expect("the server waited on a body it must refuse unread")
        .unwrap()
}

/// Await `fut` for at most twice the body-read deadline. The deadline tests run
/// on a paused clock, where this is instant; without it, a request the deadline
/// failed to cut off would hang the test instead of failing it.
async fn within_the_deadline<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(DEFAULT_BODY_READ_TIMEOUT * 2, fut)
        .await
        .expect("the body-read deadline did not cut the stalled body off")
}

fn held_request(
    uri: &str,
    content_type: &str,
    encoding: Option<&str>,
    body: Body,
) -> Request<Body> {
    let mut req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", content_type);
    if let Some(encoding) = encoding {
        req = req.header(CONTENT_ENCODING, encoding);
    }
    req.body(body).unwrap()
}

fn gzip_logs_request(body: Body) -> Request<Body> {
    held_request("/v1/logs", JSON, Some("gzip"), body)
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

/// RFC 1952 allows a gzip stream of several members back to back, and pigz,
/// Go's gzip writer and chunked compressors send one. Every member is decoded:
/// tower-http's request decoder stopped after the first, so the rest of the
/// batch was dropped while the request still answered `200`.
#[tokio::test]
async fn every_gzip_member_is_decoded() {
    let (app, tmp) = build_app(None);

    // Two protobuf messages, gzipped separately and concatenated. Concatenated
    // protobuf messages merge, so the decoded body is one request holding both
    // records, and both must land.
    let mut two_messages = gzip(&logs_protobuf("first-member-log"));
    two_messages.extend(gzip(&logs_protobuf("second-member-log")));
    let resp = post(&app, "/v1/logs", PROTOBUF, Some("gzip"), None, two_messages).await;
    let status = resp.status();
    assert_eq!(status, StatusCode::OK, "{}", body_text(resp).await);

    // One JSON document split across two members. The first member alone is
    // not valid JSON, so this is a `400` unless the second is decoded too.
    let doc = traces_json("split-member-span");
    let (head, tail) = doc.split_at(doc.len() / 2);
    let mut split_doc = gzip(head);
    split_doc.extend(gzip(tail));
    let resp = post(&app, "/v1/traces", JSON, Some("gzip"), None, split_doc).await;
    let status = resp.status();
    assert_eq!(status, StatusCode::OK, "{}", body_text(resp).await);

    let logs = raw_values(&tmp.path().join("default"));
    for marker in ["first-member-log", "second-member-log"] {
        assert!(
            logs.iter().any(|raw| raw.contains(marker)),
            "{marker} missing from the logs WAL: {logs:?}"
        );
    }
    let spans = raw_values(&tmp.path().join("default").join("siglake-traces-default"));
    assert!(
        spans.iter().any(|raw| raw.contains("split-member-span")),
        "split-member-span missing from the traces WAL: {spans:?}"
    );
}

/// Content codings are case-insensitive and `x-gzip` is `gzip` (RFC 9110). Two
/// `Content-Encoding` headers are stacked codings, which are refused rather than
/// decoded once and parsed still compressed.
#[tokio::test]
async fn gzip_spellings_decode_and_stacked_codings_are_415() {
    let (app, _tmp) = build_app(None);
    for encoding in ["GZIP", "x-gzip"] {
        let body = gzip(&logs_json(encoding));
        let resp = post(&app, "/v1/logs", JSON, Some(encoding), None, body).await;
        let status = resp.status();
        assert_eq!(
            status,
            StatusCode::OK,
            "{encoding}: {}",
            body_text(resp).await
        );
    }

    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", JSON)
        .header(CONTENT_ENCODING, "gzip")
        .header(CONTENT_ENCODING, "gzip")
        .body(Body::from(gzip(&gzip(&logs_json("stacked")))))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(resp.headers().get(ACCEPT_ENCODING).unwrap(), "gzip");
}

/// Decoded gzip bytes in flight share one budget per server, so a burst of
/// small gzip requests cannot each inflate to the body cap at once. Bodies that
/// have each decoded 15 MiB and stalled hold almost all of the default 128 MiB.
/// A small gzip request still fits in what is left, so many small requests are
/// not throttled by a few large ones. A large one does not fit and is `503`
/// with a short `Retry-After`. Uncompressed bodies are never charged. A
/// request's charge comes back when it finishes, and the large one then fits.
#[tokio::test]
async fn gzip_decode_budget_bounds_decoded_bytes_in_flight() {
    const HELD_BYTES: usize = 15 * 1024 * 1024;
    const LARGE_BYTES: usize = 10 * 1024 * 1024;
    let held_count = DEFAULT_GZIP_DECODE_BUDGET_BYTES / HELD_BYTES;
    assert!(
        DEFAULT_GZIP_DECODE_BUDGET_BYTES - held_count * HELD_BYTES < LARGE_BYTES,
        "the held bodies must leave less than the large request needs"
    );
    let (app, tmp) = build_app(None);

    let prefix = gzip_prefix_of_spaces(HELD_BYTES);
    let mut held = Vec::new();
    for _ in 0..held_count {
        let HeldBody {
            body,
            polled,
            release,
        } = held_body(prefix.clone());
        let task = tokio::spawn(app.clone().oneshot(gzip_logs_request(body)));
        polled
            .await
            .expect("a held gzip body must be decoded up to where it stalls");
        held.push((task, release));
    }

    let body = gzip(&logs_json("small-gzip-while-held"));
    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, body).await;
    let status = resp.status();
    assert_eq!(status, StatusCode::OK, "{}", body_text(resp).await);

    let large = gzip_of_spaces(LARGE_BYTES);
    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, large.clone()).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers().get(RETRY_AFTER).unwrap(), "1");
    let text = body_text(resp).await;
    assert!(text.contains("gzip decode budget"), "{text}");

    // The same bytes uncompressed are not charged: they reach the parser, where
    // all-whitespace JSON is a `400`.
    let resp = post(&app, "/v1/logs", JSON, None, None, vec![b' '; LARGE_BYTES]).await;
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("invalid OTLP"), "{text}");

    let (task, release) = held.pop().unwrap();
    drop(release);
    let resp = task.await.unwrap().unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "a held gzip body ends truncated"
    );

    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, large).await;
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(
        text.contains("invalid OTLP"),
        "with one held body finished, the large request must fit and reach the parser: {text}"
    );

    let logs = raw_values(&tmp.path().join("default"));
    assert!(
        logs.iter().any(|raw| raw.contains("small-gzip-while-held")),
        "small-gzip-while-held missing from the logs WAL: {logs:?}"
    );
}

/// A body that stops arriving is cut off at the body-read deadline with `408`,
/// on every ingest route that reads a body, compressed or not. The clock is
/// paused, so the 30 s deadline passes as soon as nothing else can run.
#[tokio::test(start_paused = true)]
async fn a_body_that_stalls_is_408_at_the_deadline() {
    let (app, _tmp) = build_app(None);
    let cases: [(&str, &str, Option<&str>, Vec<u8>); 3] = [
        ("/v1/logs", JSON, None, b"{\"resourceLogs\":".to_vec()),
        (
            "/v1/traces",
            JSON,
            Some("gzip"),
            gzip_prefix_of_spaces(1024 * 1024),
        ),
        (
            "/api/v1/_elastic/_bulk",
            "application/x-ndjson",
            None,
            b"{\"index\":{}}\n".to_vec(),
        ),
    ];
    for (uri, content_type, encoding, prefix) in cases {
        let HeldBody {
            body,
            polled: _polled,
            release: _release,
        } = held_body(prefix);
        let started = tokio::time::Instant::now();
        let resp = within_the_deadline(app.clone().oneshot(held_request(
            uri,
            content_type,
            encoding,
            body,
        )))
        .await
        .unwrap();
        let waited = started.elapsed();
        let status = resp.status();
        let text = body_text(resp).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{uri}: {text}");
        assert!(text.contains("Request Timeout"), "{uri}: {text}");
        assert!(
            waited >= DEFAULT_BODY_READ_TIMEOUT,
            "{uri}: refused after {waited:?}, before the deadline"
        );
    }
}

/// The deadline is what bounds how long a stalled gzip body holds its share of
/// the decode budget. Stalled bodies holding almost all of it are cut off at the
/// deadline, and their charge comes back with them.
#[tokio::test(start_paused = true)]
async fn the_deadline_returns_a_stalled_bodys_budget() {
    const HELD_BYTES: usize = 15 * 1024 * 1024;
    let (app, _tmp) = build_app(None);

    let prefix = gzip_prefix_of_spaces(HELD_BYTES);
    let mut held = Vec::new();
    for _ in 0..DEFAULT_GZIP_DECODE_BUDGET_BYTES / HELD_BYTES {
        let HeldBody {
            body,
            polled,
            release,
        } = held_body(prefix.clone());
        let task = tokio::spawn(app.clone().oneshot(gzip_logs_request(body)));
        polled.await.unwrap();
        held.push((task, release));
    }
    for (task, _release) in held {
        let resp = within_the_deadline(task).await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::REQUEST_TIMEOUT);
    }

    let large = gzip_of_spaces(10 * 1024 * 1024);
    let resp = post(&app, "/v1/logs", JSON, Some("gzip"), None, large).await;
    let status = resp.status();
    let text = body_text(resp).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(text.contains("invalid OTLP"), "{text}");
}

/// A tripped memory breaker refuses a gzip request before reading its body, so
/// an ingester that is already over its memory limit inflates nothing. The
/// handler's own breaker check runs only after the body is read.
#[tokio::test]
async fn memory_breaker_refuses_gzip_before_decoding() {
    let (state, _tmp) = build_state();
    let guard = Arc::new(MemoryGuard::new(1));
    guard.observe(1024 * 1024);
    let app = router(state.with_mem_guard(guard));

    let held = held_body(Vec::new());
    let resp = answered_unread(app, gzip_logs_request(held.body)).await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(resp.headers().contains_key(RETRY_AFTER));
    assert!(
        held.polled.await.is_err(),
        "the breaker must refuse before the body is read"
    );
}

/// Any coding other than gzip is `415` and names the one that would work, rather
/// than a `400` from parsing compressed bytes. That includes common codings
/// (`zstd`, `deflate`, `br`) and a rarer one (`snappy`).
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
