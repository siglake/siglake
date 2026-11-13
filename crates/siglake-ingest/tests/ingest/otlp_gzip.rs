//! `Content-Encoding` on the OTLP/HTTP routes.
//!
//! The OTLP specification says every server MUST accept `gzip` and MUST limit the
//! body size *after* decompression. The Collector's `otlphttp` exporter gzips by
//! default, so without this a stock Collector config gets `400 invalid OTLP`.
//! These tests pin both requirements, with the cap on the compressed bytes as
//! well as the decoded ones, that every member of a multi-member gzip stream is
//! decoded, the `415` for any other coding, the error envelope for a corrupt
//! gzip body, and that decoding runs inside auth.
//!
//! HOW THE COUNTERS ARE OBSERVED. As in `mem_breaker_accounting.rs`:
//! `set_default_local_recorder` is thread-local and `#[tokio::test]` is a
//! current-thread runtime, so a snapshot holds this test's metrics only.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::Router;
use flate2::write::GzEncoder;
use flate2::Compression;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
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
    let state = match tokens {
        Some(tokens) => state.with_tokens(tokens),
        None => state,
    };
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
        .header(CONTENT_TYPE, content_type);
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

type Labels = Vec<(String, String)>;

/// The `siglake_ingest_requests_total` series in `snap`, each as its full,
/// sorted label set and its count.
fn request_counts(snap: Snapshot) -> Vec<(Labels, u64)> {
    let mut rows: Vec<_> = snap
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| k.key().name() == "siglake_ingest_requests_total")
        .map(|(k, _, _, v)| {
            let mut labels: Labels = k
                .key()
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.sort();
            let DebugValue::Counter(count) = v else {
                panic!("siglake_ingest_requests_total is not a counter: {v:?}");
            };
            (labels, count)
        })
        .collect();
    rows.sort();
    rows
}

/// The labels the handler for `uri` counts its requests under, with `status`.
fn route_labels(uri: &str, status: &str) -> Labels {
    let mut labels = vec![("status".to_string(), status.to_string())];
    match uri {
        "/v1/logs" => labels.push(("endpoint".into(), "otlp".into())),
        "/v1/traces" => {
            labels.push(("endpoint".into(), "otlp_traces".into()));
            labels.push(("signal".into(), "traces".into()));
        }
        other => panic!("not an OTLP route: {other}"),
    }
    labels.sort();
    labels
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
/// tower-http's request decoder stops after the first, so the rest of the batch
/// would be dropped while the request still answered `200`.
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
        .header(CONTENT_TYPE, JSON)
        .header(CONTENT_ENCODING, "gzip")
        .header(CONTENT_ENCODING, "gzip")
        .body(Body::from(gzip(&gzip(&logs_json("stacked")))))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(resp.headers().get(ACCEPT_ENCODING).unwrap(), "gzip");
}

/// Any coding other than gzip is `415` and names the one that would work, rather
/// than a `400` from parsing compressed bytes. That includes common codings
/// (`zstd`, `deflate`, `br`) and a rarer one (`snappy`). It is counted under the
/// route's labels, so a client set to the wrong compression shows in the
/// request counter, as its `400` did before.
#[tokio::test]
async fn other_content_codings_are_415_naming_gzip() {
    let (app, tmp) = build_app(None);
    for uri in ["/v1/logs", "/v1/traces"] {
        for encoding in ["zstd", "deflate", "br", "snappy"] {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let guard = metrics::set_default_local_recorder(&recorder);
            let resp = post(&app, uri, JSON, Some(encoding), None, logs_json("x")).await;
            let counts = request_counts(snapshotter.snapshot());
            drop(guard);
            assert_eq!(
                counts,
                vec![(route_labels(uri, "415"), 1)],
                "{uri} Content-Encoding: {encoding}"
            );
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
/// stream that decodes to twice the cap but is tiny on the wire is refused at
/// the cap. The boundary is exact: a body that decodes to exactly the cap
/// reaches the parser, where all-whitespace JSON is a `400`, and one byte more
/// is a `413`.
#[tokio::test]
async fn gzip_cap_counts_decoded_bytes() {
    let (app, _tmp) = build_app(None);

    let bomb = gzip_of_spaces(2 * DEFAULT_BODY_CAP);
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

/// The cap counts the compressed bytes too. Empty gzip members are 20 bytes
/// that decode to nothing, so without it a body of any size that decodes to one
/// small record would be read in full and land. Over the cap, it is the same
/// `413` an uncompressed body over the cap gets. A few empty members under the
/// cap are still a valid stream and land.
#[tokio::test]
async fn the_cap_counts_compressed_bytes_too() {
    let (app, tmp) = build_app(None);
    let empty_member = gzip(b"");

    let mut small = gzip(&logs_protobuf("empty-members-under-cap"));
    for _ in 0..3 {
        small.extend_from_slice(&empty_member);
    }
    let resp = post(&app, "/v1/logs", PROTOBUF, Some("gzip"), None, small).await;
    let status = resp.status();
    assert_eq!(status, StatusCode::OK, "{}", body_text(resp).await);

    let mut padded = gzip(&logs_protobuf("empty-members-over-cap"));
    while padded.len() <= DEFAULT_BODY_CAP {
        padded.extend_from_slice(&empty_member);
    }
    let resp = post(&app, "/v1/logs", PROTOBUF, Some("gzip"), None, padded).await;
    let status = resp.status();
    let text = body_text(resp).await;

    let plain = post(
        &app,
        "/v1/logs",
        PROTOBUF,
        None,
        None,
        vec![0; DEFAULT_BODY_CAP + 1],
    )
    .await;
    assert_eq!(plain.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let plain_text = body_text(plain).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{text}");
    assert_eq!(
        text, plain_text,
        "the compressed 413 must match the framework's"
    );

    let logs = raw_values(&tmp.path().join("default"));
    assert!(
        logs.iter()
            .any(|raw| raw.contains("empty-members-under-cap")),
        "empty-members-under-cap missing from the logs WAL: {logs:?}"
    );
    assert!(
        !logs
            .iter()
            .any(|raw| raw.contains("empty-members-over-cap")),
        "a body over the cap must not land: {logs:?}"
    );
}

/// A body labelled gzip that is not a gzip stream, or is cut short, is a `400`
/// in the same JSON envelope (`SiglakeErrorBody`) as any other undecodable
/// body, and is counted as one, under the route's own endpoint label. It never
/// reaches the OTLP parser, and nothing lands.
#[tokio::test]
async fn corrupt_gzip_is_a_json_400_counted_like_any_undecodable_body() {
    let (app, tmp) = build_app(None);
    let mut truncated = gzip(&logs_json("truncated"));
    truncated.truncate(truncated.len() / 2);
    let cases = [
        ("/v1/logs", logs_json("not-compressed")),
        ("/v1/logs", truncated),
        ("/v1/traces", traces_json("not-compressed")),
    ];
    for (uri, body) in cases {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let guard = metrics::set_default_local_recorder(&recorder);
        let resp = post(&app, uri, JSON, Some("gzip"), None, body).await;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let text = body_text(resp).await;
        let counts = request_counts(snapshotter.snapshot());
        drop(guard);

        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {text}");
        assert_eq!(content_type, JSON, "{uri}: {text}");
        let body: serde_json::Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("{uri}: not the JSON envelope ({e}): {text}"));
        assert_eq!(body["code"], 400, "{uri}: {text}");
        let message = body["text"].as_str().unwrap_or_default();
        assert!(
            message.contains("gzip") && !message.contains("invalid OTLP"),
            "{uri}: the message must name the gzip stream, not the OTLP parser: {text}"
        );
        assert_eq!(
            counts,
            vec![(route_labels(uri, "400"), 1)],
            "{uri}: a corrupt gzip body must be counted once as a 400"
        );
    }
    assert!(raw_values(&tmp.path().join("default")).is_empty());
}

/// A gzip body whose connection fails mid-upload is not blamed on the gzip
/// stream: it gets the framework's answer and is not counted, as the same
/// failure on an uncompressed body is.
#[tokio::test]
async fn a_broken_upload_is_not_reported_as_invalid_gzip() {
    let (app, _tmp) = build_app(None);
    let compressed = gzip(&logs_json("broken-upload"));
    let (head, _) = compressed.split_at(compressed.len() / 2);
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> = vec![
        Ok(head.to_vec()),
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        )),
    ];
    let req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(CONTENT_TYPE, JSON)
        .header(CONTENT_ENCODING, "gzip")
        .body(Body::from_stream(futures::stream::iter(chunks)))
        .unwrap();

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let text = body_text(resp).await;
    let counts = request_counts(snapshotter.snapshot());
    drop(guard);

    assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
    assert!(
        !text.contains("invalid gzip"),
        "a connection failure is not a gzip error: {text}"
    );
    assert!(counts.is_empty(), "counted as a request: {counts:?}");
}

/// The content-coding middleware sits inside the auth middleware, so an
/// unauthenticated caller gets `401` whatever coding it names and cannot probe
/// the accepted ones: were the order reversed, `zstd` would be a `415` here.
/// Once authenticated, gzip works as usual.
#[tokio::test]
async fn auth_refuses_before_the_content_coding_is_checked() {
    let (app, _tmp) = build_app(Some(AuthTokens::from_tokens(["sekret"])));

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
