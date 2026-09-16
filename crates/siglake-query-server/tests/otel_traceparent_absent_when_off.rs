//! With telemetry off, a shard request carries no `traceparent`.
//!
//! Its own test binary: the text-map propagator is a process-wide global, so a
//! sibling test that installs one would decide this test's answer.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use siglake_query_server::coordinator::{HttpShardRunner, ShardRunner};

/// With no propagator installed — a process where `telemetry::init` never ran,
/// or ran with OTel off — the shard request carries no `traceparent` and the
/// query still answers. This is the "no cost when telemetry is off" half of the
/// claim that is observable from a test: no header, no error, same rows.
#[tokio::test(flavor = "multi_thread")]
async fn shard_request_without_telemetry_carries_no_traceparent() {
    let seen: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured = seen.clone();
    // A stand-in worker, so the assertion is on the bytes the coordinator put
    // on the wire rather than on what a span recorded.
    let app = axum::Router::new().route(
        "/api/v1/sql/shard",
        axum::routing::post(move |headers: axum::http::HeaderMap| {
            let captured = captured.clone();
            async move {
                captured.lock().expect("seen").push(
                    headers
                        .get("traceparent")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                );
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind worker");
    let worker_url = format!("http://{}", listener.local_addr().expect("addr"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let runner = HttpShardRunner::new(vec![worker_url], None);
    let _ = tokio::time::timeout(Duration::from_secs(10), runner.run("SELECT 1", None)).await;
    server.abort();

    let seen = seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "the worker saw exactly one shard request");
    assert_eq!(
        seen[0], None,
        "a process with telemetry off must send no traceparent"
    );
}
