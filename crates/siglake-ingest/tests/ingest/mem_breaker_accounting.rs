//! A request the memory breaker sheds is counted once, on either transport.
//!
//! THE DEFECT THIS GUARDS. The HTTP handlers shed through
//! `mem_breaker_response`, which increments
//! `siglake_ingest_requests_total{status="503"}` and
//! `siglake_ingest_mem_breaker_rejected_total{endpoint}`. The two OTLP/gRPC
//! exporters shed through a sibling that built the same `ApiError` and
//! incremented nothing. So on a gRPC-only deployment the breaker could refuse
//! every export with both counters flat, leaving only the
//! `siglake_ingest_mem_breaker_open` gauge to say so — and the 503 counter is
//! what the chart's KEDA trigger and the operator's offered-load query read,
//! so the refused load was invisible to scaling as well (task #3876).
//!
//! WHAT EACH TEST ASSERTS. Not just "the counter moved": the whole set of
//! series emitted under those two names, with their labels and values. A
//! refusal counted twice, counted under a new label dimension, or counted with
//! the wrong endpoint fails here.
//!
//! HOW THE REFUSAL IS FORCED. `MemoryGuard::observe` publishes an RSS reading
//! directly, so the breaker trips on a stated number rather than on this
//! process's live RSS. Nothing sleeps and nothing samples `/proc`.
//!
//! HOW THE COUNTERS ARE OBSERVED. `set_default_local_recorder` is
//! thread-local, so a snapshot holds this test's metrics only even though the
//! binary runs its tests concurrently (the same reason `stream_lag.rs` uses
//! it). `#[tokio::test]` is a current-thread runtime and the shed path neither
//! spawns nor blocks, so the `counter!` calls run on the thread the guard
//! covers. The snapshot is taken once per phase: the debugging recorder drains
//! on every read.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use tokio::sync::Mutex;
use tonic::{Code, Request as TonicRequest};
use tower::util::ServiceExt;

use siglake_ingest::mem_guard::MemoryGuard;
use siglake_ingest::{
    router, AppState, OtlpGrpcLogsService, OtlpGrpcTracesService, TenantRouting, TenantWalRouter,
};
use siglake_wal::WalWriter;

const REJECTED: &str = "siglake_ingest_mem_breaker_rejected_total";
const REQUESTS: &str = "siglake_ingest_requests_total";

/// An ingester whose breaker is already tripped: the limit is 1 byte and the
/// published reading is a megabyte.
fn over_limit_state(root: &std::path::Path) -> AppState {
    let guard = Arc::new(MemoryGuard::new(1));
    guard.observe(1024 * 1024);
    AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(
            WalWriter::with_thresholds(root, "test", 5, Duration::from_secs(60)).unwrap(),
        )),
        tenants: Some(Arc::new(TenantWalRouter::new(
            root,
            "test",
            5,
            Duration::from_secs(60),
        ))),
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
    }
}

/// One counter series: its name, its labels sorted by key, and its value.
type Series = (String, Vec<(String, String)>, u64);

/// Every series in `snap` under one of the two breaker counter names, sorted
/// so the assertion is an exact comparison and not a containment check.
fn breaker_series(snap: Snapshot) -> Vec<Series> {
    let mut rows: Vec<_> = snap
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| matches!(k.key().name(), REJECTED | REQUESTS))
        .map(|(k, _, _, v)| {
            let key = k.key();
            let mut labels: Vec<(String, String)> = key
                .labels()
                .map(|l| (l.key().to_string(), l.value().to_string()))
                .collect();
            labels.sort();
            let count = match v {
                DebugValue::Counter(c) => c,
                other => panic!("{} is not a counter: {other:?}", key.name()),
            };
            (key.name().to_string(), labels, count)
        })
        .collect();
    rows.sort();
    rows
}

/// What one refused request on `endpoint` must leave behind: one 503 request
/// and one breaker rejection, and nothing else under those names.
fn one_refusal(endpoint: &str) -> Vec<Series> {
    let mut expected = vec![
        (
            REJECTED.to_string(),
            vec![("endpoint".to_string(), endpoint.to_string())],
            1,
        ),
        (
            REQUESTS.to_string(),
            vec![
                ("endpoint".to_string(), endpoint.to_string()),
                ("status".to_string(), "503".to_string()),
            ],
            1,
        ),
    ];
    expected.sort();
    expected
}

#[tokio::test]
async fn grpc_logs_export_refused_by_the_breaker_is_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let service = OtlpGrpcLogsService::new(over_limit_state(tmp.path()));

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let err = service
        .export(TonicRequest::new(ExportLogsServiceRequest::default()))
        .await
        .expect_err("the breaker is over its limit; the export must be refused");
    let series = breaker_series(snapshotter.snapshot());
    drop(guard);

    // The gRPC contract does not change: UNAVAILABLE with a retry-after.
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        series,
        one_refusal("otlp"),
        "a gRPC logs export shed by the breaker did not land in both counters \
         exactly once under endpoint=otlp"
    );
}

#[tokio::test]
async fn grpc_traces_export_refused_by_the_breaker_is_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let service = OtlpGrpcTracesService::new(over_limit_state(tmp.path()));

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let err = service
        .export(TonicRequest::new(ExportTraceServiceRequest::default()))
        .await
        .expect_err("the breaker is over its limit; the export must be refused");
    let series = breaker_series(snapshotter.snapshot());
    drop(guard);

    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata()
            .get("retry-after")
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        series,
        one_refusal("otlp_traces"),
        "a gRPC traces export shed by the breaker did not land in both counters \
         exactly once under endpoint=otlp_traces"
    );
}

/// The HTTP side already counted; this pins that moving the increments into a
/// shared helper did not make it count twice.
#[tokio::test]
async fn http_logs_export_refused_by_the_breaker_is_counted_once() {
    let tmp = tempfile::tempdir().unwrap();
    let app = router(over_limit_state(tmp.path()));

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let guard = metrics::set_default_local_recorder(&recorder);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("content-type", "application/json")
                .body(Body::from("{\"resourceLogs\":[]}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let series = breaker_series(snapshotter.snapshot());
    drop(guard);

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        series,
        one_refusal("otlp"),
        "the HTTP shed path counted a single refusal more than once, or under \
         different labels"
    );
}
