//! Constant-predicate normalization must preserve SQL three-valued logic and
//! numeric coercion semantics in the result-cache key.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use siglake_core::Event;
use siglake_query_server::{reset_result_cache_for_test, router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

fn cache_outcomes(snapshotter: &Snapshotter) -> HashMap<String, u64> {
    let mut outcomes = HashMap::new();
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
            *outcomes.entry(outcome).or_insert(0) += count;
        }
    }
    outcomes
}

async fn post_sql(app: &Router, query: &str) -> serde_json::Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&serde_json::json!({ "query": query })).unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{query}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).unwrap()
}

fn assert_cache_activity(snapshotter: &Snapshotter, expected_outcome: &str, query: &str) {
    let outcomes = cache_outcomes(snapshotter);
    assert_eq!(
        outcomes.get(expected_outcome).copied(),
        Some(1),
        "expected one {expected_outcome} for {query}: {outcomes:?}"
    );
}

async fn cold_rows(app: &Router, snapshotter: &Snapshotter, query: &str) -> serde_json::Value {
    reset_result_cache_for_test().await;
    let _ = cache_outcomes(snapshotter);
    let body = post_sql(app, query).await;
    assert_cache_activity(snapshotter, "miss", query);
    body["rows"].clone()
}

async fn assert_fill_order(
    app: &Router,
    snapshotter: &Snapshotter,
    first: (&str, &serde_json::Value),
    second: (&str, &serde_json::Value),
) {
    reset_result_cache_for_test().await;
    let _ = cache_outcomes(snapshotter);

    let first_body = post_sql(app, first.0).await;
    assert_eq!(first_body["rows"], *first.1, "{}", first.0);
    assert_cache_activity(snapshotter, "miss", first.0);

    let second_body = post_sql(app, second.0).await;
    assert_eq!(second_body["rows"], *second.1, "{}", second.0);
    assert_cache_activity(snapshotter, "miss", second.0);

    let repeated = post_sql(app, second.0).await;
    assert_eq!(repeated["rows"], *second.1, "{}", second.0);
    assert_cache_activity(snapshotter, "hit", second.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_constant_predicates_match_uncached_sql_in_both_fill_orders() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let mut matching = Event::now("matching row");
    matching.host = "cache-semantic-host".to_string();
    let mut other = Event::now("other row");
    other.host = "other-host".to_string();
    ice.append_events(&[matching, other]).await.unwrap();
    let app = router(AppState::new(ice, AuthConfig::open()));

    let filtered = "SELECT count(*) AS n FROM events WHERE host = 'cache-semantic-host'";
    let predicates = [
        "SELECT count(*) AS n FROM events WHERE host = 'cache-semantic-host' AND NULL = NULL",
        "SELECT count(*) AS n FROM events WHERE host = 'cache-semantic-host' AND 1 != 1.0",
    ];
    let filtered_rows = cold_rows(&app, &snapshotter, filtered).await;
    assert_eq!(filtered_rows[0]["n"], 1);

    for predicate in predicates {
        let predicate_rows = cold_rows(&app, &snapshotter, predicate).await;
        assert_eq!(predicate_rows[0]["n"], 0, "{predicate}");
        assert_fill_order(
            &app,
            &snapshotter,
            (filtered, &filtered_rows),
            (predicate, &predicate_rows),
        )
        .await;
        assert_fill_order(
            &app,
            &snapshotter,
            (predicate, &predicate_rows),
            (filtered, &filtered_rows),
        )
        .await;
    }
}
