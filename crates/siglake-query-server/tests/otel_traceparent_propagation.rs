//! A distributed query is ONE trace: the worker's server span is a child of
//! the coordinator's fan-out span, over a real socket.
//!
//! WHAT THIS PINS, and why it is an end-to-end test rather than two unit ones.
//! The two halves are written against different context stores.
//! `tracing-opentelemetry` keeps a span's OTel `SpanContext` in the TRACING
//! span's extensions; `opentelemetry::Context::current()` — the obvious thing
//! to hand a propagator — is a separate thread-local that nothing in this
//! workspace ever attaches to. Injecting from it produces an invalid span
//! context, `TraceContextPropagator` then writes no header at all, and every
//! signal that the plumbing is wrong is silence: the worker's span is still
//! recorded, still exported, still looks fine on its own, and just belongs to a
//! different trace. This test captures the coordinator's tracing span context,
//! then compares the worker export against it after the real HTTP hop.
//!
//! The whole test runs with OTel ON, which is the only configuration in which
//! propagation is observable at all.

use std::sync::{Arc, Mutex};

use opentelemetry::global;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use siglake_query_server::coordinator::{HttpShardRunner, ShardRunner};
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Keeps every span the SDK exports, so the test can match a child to its
/// parent across the hop.
#[derive(Debug, Default, Clone)]
struct RecordingExporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl SpanExporter for RecordingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.spans.lock().expect("spans").extend(batch);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shard_request_carries_the_coordinator_trace() {
    let exporter = RecordingExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("siglake");
    global::set_text_map_propagator(TraceContextPropagator::new());
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    // A real worker on a real port: the header has to survive reqwest, hyper
    // and axum, which an in-process `oneshot` would skip.
    let tmp = tempfile::tempdir().expect("tempdir");
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .expect("open warehouse");
    let app = router(AppState::new(Arc::new(ice), AuthConfig::open()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind worker");
    let worker_url = format!("http://{}", listener.local_addr().expect("addr"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let runner = HttpShardRunner::new(vec![worker_url], None);
    // `SELECT 1` needs no table, so the answer depends on nothing but the hop.
    let coordinator_span = tracing::info_span!("coordinator.fanout");
    let coordinator_context = coordinator_span.context();
    let expected = coordinator_context.span().span_context().clone();
    assert!(expected.is_valid(), "coordinator span has no OTel context");

    // Own the instrumented future and drop it before `force_flush`, so the
    // exporter holds every completed span when the assertions run.
    let mut request = Box::pin(
        async {
            runner
                .run("SELECT 1 AS n", None)
                .await
                .expect("shard query")
        }
        .instrument(coordinator_span),
    );
    let rows = request.as_mut().await;
    drop(request);
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    server.abort();
    provider.force_flush().expect("flush");

    let spans = exporter.spans.lock().expect("spans").clone();
    let names: Vec<&str> = spans.iter().map(|s| s.name.as_ref()).collect();
    let server_span = spans
        .iter()
        .find(|s| s.name == "http.server")
        .unwrap_or_else(|| panic!("no worker server span; exported: {names:?}"));

    assert_eq!(
        server_span.span_context.trace_id(),
        expected.trace_id(),
        "the worker joined a different trace: the traceparent did not survive \
         the hop (exported: {names:?})"
    );
    assert_eq!(
        server_span.parent_span_id,
        expected.span_id(),
        "the worker's span is not a child of the fan-out span"
    );
    assert_eq!(
        server_span
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "http.route")
            .map(|kv| kv.value.to_string()),
        Some("/api/v1/sql/shard".to_string())
    );
}
