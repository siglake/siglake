//! Probe: is a reversed (DESC-on-ASC-file) read correct when a row group
//! spans MULTIPLE record batches? (All prior hermetic reversed tests used
//! single-batch files.)
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration, TimeZone, Utc};
use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use tower::util::ServiceExt;

async fn request_json(app: &Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    (status, body)
}

#[tokio::test]
async fn reversed_read_of_multi_batch_row_group_is_ordered() {
    // Small tail chunks so the 30k-row group needs several — proves both the
    // cross-chunk ordering AND that an early-stopping LIMIT decodes only the
    // newest chunk.
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // ONE file, one row group, ~30k rows = several 8192-row batches.
    let mut events = Vec::with_capacity(30_000);
    for i in 0..30_000i64 {
        let mut e = Event::now(format!("row {i}"));
        e.timestamp = base + Duration::milliseconds(i * 100);
        events.push(e);
    }
    ice.append_batch(siglake_core::events_to_record_batch(&events).unwrap())
        .await
        .unwrap();
    let mut state = AppState::new(ice, AuthConfig::open());
    state.query_scan.reversed_chunk_rows = Some(4096);
    let app = router(state);
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 5"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<String> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().to_string())
        .collect();
    let want: Vec<String> = (0..5)
        .map(|i| {
            (base + Duration::milliseconds((29_999 - i) * 100))
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
        })
        .collect();
    assert_eq!(
        got, want,
        "reversed multi-batch read must emit newest first: {body}"
    );
    let rows_scanned = body["stats"]["rows_scanned"].as_u64().unwrap();
    assert!(
        rows_scanned <= 8192,
        "tail-chunked reversed LIMIT must decode ~one chunk, not the group: {rows_scanned}"
    );

    // Decode-share cache: a repeat of the identical browse serves the same
    // rows from the decoded-chunk cache (distinct SQL text so the SQL result
    // cache doesn't mask the reader-level path).
    let (status2, body2) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 5 OFFSET 0"
        }),
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "{body2}");
    let got2: Vec<String> = body2["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        got2, want,
        "cached reversed chunk must serve identical rows: {body2}"
    );

    // Cross-chunk boundary correctness: a LIMIT deeper than one chunk must
    // stay strictly descending across the chunk seam.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\" FROM events ORDER BY \"timestamp\" DESC LIMIT 6000"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<String> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(got.len(), 6000, "{}", body["row_count"]);
    let want: Vec<String> = (0..6000)
        .map(|i| {
            (base + Duration::milliseconds((29_999 - i) * 100))
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
        })
        .collect();
    assert_eq!(got, want, "ordering must hold across the chunk seam");
}
