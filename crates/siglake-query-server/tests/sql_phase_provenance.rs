//! `stats.phases` on a `/api/v1/sql` records response describes the execution
//! that produced the body, not the request that received it (#7471).
//!
//! A result-cache hit returns the STORED body — the same `Arc`, phases and all
//! (`crates/siglake-query-server/src/sql.rs`, the `Hit` arm of the cache
//! probe). So the phase block a reader gets back may belong to a request that
//! ran minutes ago, while this request did a catalog probe and a serialize and
//! nothing else. Any field added to that block inherits the same property: it
//! is execution provenance, and a request-timing field placed there would
//! report the first request's timing to every later one.
//!
//! The hit is proven by the cache's own `outcome` counter rather than by being
//! fast, and the replay by comparing the two bodies' phase blocks for
//! equality. No wall-clock duration is asserted.
//!
//! ONE test function in its own binary: the result cache and the metrics
//! recorder are both process-wide.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

const FILES: usize = 4;
const ROWS_PER_FILE: usize = 500;

/// Cache-outcome counts SINCE THE LAST CALL (`snapshot()` drains), keyed by the
/// `outcome` label: `hit`, `miss`, `insert`, ….
fn outcomes(snapshotter: &Snapshotter) -> HashMap<String, u64> {
    let mut by_outcome = HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if key.key().name() != "siglake_query_sql_result_cache_requests_total" {
            continue;
        }
        let Some(outcome) = key
            .key()
            .labels()
            .find(|label| label.key() == "outcome")
            .map(|label| label.value().to_string())
        else {
            continue;
        };
        if let DebugValue::Counter(count) = value {
            *by_outcome.entry(outcome).or_insert(0) += count;
        }
    }
    by_outcome.retain(|_, count| *count > 0);
    by_outcome
}

async fn needle_table(ice: &IcebergContext) {
    for file in 0..FILES {
        let events: Vec<Event> = (0..ROWS_PER_FILE)
            .map(|i| {
                let mut e = Event::now(format!("row {file}-{i} payload needle-{}", i % 7));
                e.host = format!("host-{}", (b'a' + (i % 26) as u8) as char);
                e
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }
}

async fn post_records(app: &Router, query: &str) -> serde_json::Value {
    let body = serde_json::json!({ "query": query });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{query}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cached_answer_replays_the_filling_execution_phases() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    needle_table(&ice).await;
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));

    let sql = "SELECT host, raw FROM events WHERE raw LIKE '%needle-3%' LIMIT 50";

    let executed = post_records(&app, sql).await;
    let filling = outcomes(&snapshotter);
    assert_eq!(
        filling.get("hit").copied().unwrap_or(0),
        0,
        "the first request cannot hit an empty cache: {filling:?}"
    );
    let executed_phases = executed["stats"]["phases"].clone();
    assert!(
        executed_phases.is_object(),
        "an executed records response carries its phase block: {executed}"
    );

    let replayed = post_records(&app, sql).await;
    let serving = outcomes(&snapshotter);
    assert_eq!(
        serving.get("hit").copied().unwrap_or(0),
        1,
        "the repeat must be served from the result cache: {serving:?}"
    );

    // The rows are the same answer, by design. The phases are the same NUMBERS,
    // which is a different claim: the second request planned nothing and
    // collected nothing, and still reports a plan and a collect.
    assert_eq!(replayed["rows"], executed["rows"]);
    assert_eq!(
        replayed["stats"]["phases"], executed_phases,
        "a cache hit replays the stored phase block verbatim"
    );
}
