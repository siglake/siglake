//! Task #4890: the per-request half of clipped-browse decode depth.
//!
//! The process histogram `siglake_query_scan_file_cache_populate_rows` says how
//! deep each population read, but a round runs several shapes against one
//! process and cannot attribute a process histogram to one of them without
//! isolating it. `stats.scan.file_cache_populate_rows` is the same quantity
//! charged to the request, and `stats.scan.file_cache_bypasses` is the
//! denominator that says why a shape produced no depth at all: since #4891 a
//! task carrying a converted predicate declines population and reads with its
//! predicate intact, so its browses leave no samples. Zero samples from an
//! ineligible shape and zero samples from a shape that decoded nothing are
//! opposite readings, and only the bypass count separates them.
//!
//! Two requests through the real router, in its own binary because the
//! decoded-file cache and the scan tuning are process-global.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

const ROWS: usize = 4_000;
const BATCH_ROWS: usize = 256;

async fn post_sql(app: &Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
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
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_stats_separate_population_depth_from_an_ineligible_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let events: Vec<Event> = (0..ROWS)
        .map(|i| {
            let mut event = Event::now(format!("row {i} payload"));
            event.host = "alpha".into();
            event
        })
        .collect();
    ice.append_events(&events).await.unwrap();

    siglake_storage::configure_query_scan_tuning(siglake_storage::QueryScanTuning {
        file_cache_max_bytes: Some(256 << 20),
        file_cache_max_entries: Some(4096),
        batch_size: Some(BATCH_ROWS),
        ..Default::default()
    });
    let app = router(
        AppState::new(ice.clone(), AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(1),
            ..Default::default()
        }),
    );

    // A predicate-free clipped browse: the task populates, the LIMIT stops it
    // early, and the request carries the depth its population reached.
    let (status, body) = post_sql(
        &app,
        serde_json::json!({
            "query": "SELECT raw FROM events LIMIT 10",
            "default_order": false,
            "limits": { "max_rows_returned": 1_000 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let scan = &body["stats"]["scan"];
    eprintln!("CLIPPED BROWSE scan={scan}");
    let depth = scan["file_cache_populate_rows"].as_u64().unwrap_or(0);
    assert!(
        depth >= 10 && depth <= ROWS as u64,
        "the request must carry the rows its population was handed, between the \
         rows it returned and the whole file: {scan}"
    );
    assert!(
        scan.get("file_cache_bypasses").is_none(),
        "a predicate-free browse populates; nothing bypassed: {scan}"
    );
    assert_eq!(scan["file_cache_misses"].as_u64().unwrap_or(0), 1, "{scan}");

    // The same browse under a converted predicate: no population, so no depth,
    // and the bypass count is what says so. Reading the absent
    // `file_cache_populate_rows` as "decoded nothing" would be wrong.
    let (status, body) = post_sql(
        &app,
        serde_json::json!({
            "query": "SELECT raw FROM events WHERE host = 'alpha' LIMIT 10",
            "default_order": false,
            "limits": { "max_rows_returned": 1_001 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let scan = &body["stats"]["scan"];
    eprintln!("PREDICATE BROWSE scan={scan}");
    assert_eq!(
        scan["file_cache_bypasses"].as_u64().unwrap_or(0),
        1,
        "a converted predicate declines population (#4891): {scan}"
    );
    assert!(
        scan.get("file_cache_populate_rows").is_none(),
        "an ineligible shape must report no population depth at all, not zero \
         rows from a population that ran: {scan}"
    );

    siglake_storage::configure_query_scan_tuning(siglake_storage::QueryScanTuning::default());
}
