//! Depth-spike ordered browses (07-16 windowed-browse 413s): a cluster whose
//! overlap depth exceeds the merge fan-in cap must not forfeit the whole
//! scan's ordered advertisement — small spikes (the newest tail piling up
//! between compaction passes) eager-sort instead; oversized ones still refuse
//! but the breaker response now attributes itself.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{DateTime, Duration, TimeZone, Utc};
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
async fn depth_spike_sort_cluster_and_breaker_attribution() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();

    // History: one file, 200k rows over ~19.9h (disjoint from the tail).
    let mut history = Vec::with_capacity(200_000);
    for i in 0..200_000i64 {
        let mut e = Event::now(format!("history {i}"));
        e.timestamp = base + Duration::milliseconds(i * 358);
        history.push(e);
    }
    ice.append_batch(siglake_core::events_to_record_batch(&history).unwrap())
        .await
        .unwrap();

    // Tail: 6 fully-overlapping tiny files (depth 6 > cap 4) in the newest
    // 2 minutes — the marker-pileup shape.
    let tail_base = base + Duration::hours(20);
    for j in 0..6i64 {
        let mut events = Vec::with_capacity(200);
        for i in 0..200i64 {
            let k = i * 6 + j;
            let mut e = Event::now(format!("tail {k}"));
            e.timestamp = tail_base + Duration::milliseconds(k * 100);
            events.push(e);
        }
        ice.append_batch(siglake_core::events_to_record_batch(&events).unwrap())
            .await
            .unwrap();
    }

    // 2 partitions for 7 files forces multi-file partitions (the gate's
    // re-split path, where cluster depth matters).
    let mut state = AppState::new(ice, AuthConfig::open());
    state.query_scan.target_partitions = Some(2);
    state.query_scan.ordered_merge_max_fan_in = Some(4);
    let app = router(state.clone());

    let lo = (base + Duration::hours(10)).to_rfc3339();
    let hi = (tail_base + Duration::seconds(180)).to_rfc3339();
    let browse = format!(
        "SELECT \"timestamp\", raw FROM events WHERE \"timestamp\" >= '{lo}' AND \"timestamp\" < '{hi}' ORDER BY \"timestamp\" DESC LIMIT 100"
    );

    // Phase 1: the depth spike fits the sort budget — the advertisement must
    // survive (eager-sorted cluster), the browse early-stops, rows exact.
    let (status, body) = request_json(&app, serde_json::json!({ "query": browse })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["row_count"].as_u64(), Some(100), "{body}");
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("advertised"),
        "depth spike under the sort budget must keep the advertisement: {}",
        body["stats"]
    );
    let rows_scanned = body["stats"]["rows_scanned"].as_u64().unwrap();
    assert!(
        rows_scanned < 50_000,
        "sort-cluster browse must early-stop, not TopK the window: scanned {rows_scanned}"
    );
    // Exact newest-100, strictly descending: tail rows k = 1100..1200.
    let got: Vec<DateTime<Utc>> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().parse().unwrap())
        .collect();
    let want: Vec<DateTime<Utc>> = (0..100)
        .map(|i| tail_base + Duration::milliseconds((1199 - i) * 100))
        .collect();
    assert_eq!(
        got, want,
        "sort-cluster output must be exactly the newest 100"
    );

    // Phase 2: an OVERSIZED spike (budget 500 < 1200 tail rows) still refuses
    // — correct rows via TopK, and the response self-attributes the refusal.
    state.query_scan.ordered_sort_cluster_max_rows = Some(500);
    let low_sort_budget_app = router(state);
    // Textually distinct SQL: the snapshot-keyed result cache would otherwise
    // replay phase 1's response (same table, snapshot, and query string).
    let lo2 = (base + Duration::hours(11)).to_rfc3339();
    let browse2 = format!(
        "SELECT \"timestamp\", raw FROM events WHERE \"timestamp\" >= '{lo2}' AND \"timestamp\" < '{hi}' ORDER BY \"timestamp\" DESC LIMIT 100"
    );
    let (status, body) = request_json(
        &low_sort_budget_app,
        serde_json::json!({ "query": browse2 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<DateTime<Utc>> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["timestamp"].as_str().unwrap().parse().unwrap())
        .collect();
    assert_eq!(
        got, want,
        "refused-advertisement TopK must return the same rows"
    );
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("fan_in"),
        "oversized spike must surface the refusal reason: {}",
        body["stats"]
    );
    // Phase 3: a breaker-tripped query's 413 body carries the same scan
    // attribution a success carries (the 07-16 lesson: three live 413s were
    // undiagnosable without it).
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\", raw FROM events ORDER BY raw LIMIT 5",
            "limits": { "max_rows_scanned": 50 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    let scanned = body["stats"]["rows_scanned"].as_u64().unwrap_or(0);
    assert!(
        scanned > 50,
        "breaker body must report what was scanned: {body}"
    );
    assert!(
        body["stats"]["scan"]["ordering"].is_string(),
        "breaker body must carry the ordering-gate outcome: {body}"
    );
}
