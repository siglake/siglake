//! Selectivity-aware ordered policy (07-16 filed finding): a LOW-selectivity
//! equality browse takes the ordered early-stop drain (advertised), a
//! HIGH-selectivity one keeps the pruned TopK (filtered), and a hinted drain
//! that busts the rows cap retries through the TopK fallback instead of
//! erroring outright.

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

/// 30k rows, hosts by index: i%10<=5 → host-common (60%), otherwise
/// host-other — except 30 scattered host-rare rows (0.1%).
fn corpus(base: chrono::DateTime<Utc>) -> Vec<Event> {
    (0..30_000i64)
        .map(|i| {
            let host = if i % 1000 == 7 {
                "host-rare"
            } else if i % 10 <= 5 {
                "host-common"
            } else {
                "host-other"
            };
            let mut e = Event::now(format!("row {i}"));
            e.timestamp = base + Duration::milliseconds(i * 100);
            e.host = host.into();
            e
        })
        .collect()
}

#[tokio::test]
async fn low_selectivity_browse_advertises_and_rare_keeps_topk() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // Interleaved appends → overlapping files (the ordered path's real shape).
    let all = corpus(base);
    for j in 0..3 {
        let part: Vec<Event> = all.iter().skip(j).step_by(3).cloned().collect();
        ice.append_batch(siglake_core::events_to_record_batch(&part).unwrap())
            .await
            .unwrap();
    }
    let app = router(AppState::new(ice, AuthConfig::open()));

    // 60%-selective equality: the hint fires, the scan advertises, rows exact.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\", host FROM events WHERE host = 'host-common' \
                      ORDER BY \"timestamp\" DESC LIMIT 50"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("advertised"),
        "low-selectivity residual browse must take the ordered drain: {}",
        body["stats"]
    );
    let got: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r["timestamp"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<Utc>>()
                .unwrap()
                .timestamp_millis()
        })
        .collect();
    let want: Vec<i64> = (0..30_000i64)
        .rev()
        .filter(|i| i % 1000 != 7 && i % 10 <= 5)
        .take(50)
        .map(|i| (base + Duration::milliseconds(i * 100)).timestamp_millis())
        .collect();
    assert_eq!(
        got, want,
        "ordered residual drain must return the exact newest matches"
    );

    // 0.1%-selective equality: below the threshold — stays on the TopK path.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\", host FROM events WHERE host = 'host-rare' \
                      ORDER BY \"timestamp\" DESC LIMIT 5"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("filtered"),
        "rare-value browse must keep the pruned TopK: {}",
        body["stats"]
    );
    assert_eq!(body["row_count"].as_u64(), Some(5), "{body}");
}

#[tokio::test]
async fn hinted_drain_over_cap_falls_back_to_topk() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // Temporal skew: 12k oldest rows are host-legacy; 28k newer rows carry
    // NO matches but lexically-straddling hosts (page/group min/max covers
    // 'host-legacy') — the DESC drain must chew through every newer row
    // before its first match, busting the rows cap below.
    let mut events = Vec::with_capacity(40_000);
    for i in 0..40_000i64 {
        let host = if i < 12_000 {
            "host-legacy"
        } else if i % 2 == 0 {
            "host-aaa"
        } else {
            "host-zzz"
        };
        let mut e = Event::now(format!("row {i}"));
        e.timestamp = base + Duration::milliseconds(i * 100);
        e.host = host.into();
        events.push(e);
    }
    ice.append_batch(siglake_core::events_to_record_batch(&events[..12_000]).unwrap())
        .await
        .unwrap();
    ice.append_batch(siglake_core::events_to_record_batch(&events[12_000..26_000]).unwrap())
        .await
        .unwrap();
    ice.append_batch(siglake_core::events_to_record_batch(&events[26_000..]).unwrap())
        .await
        .unwrap();
    let app = router(AppState::new(ice, AuthConfig::open()));

    // ~30% selective → the hint fires. Even with every match BELOW 28k rows
    // of non-matching newer data, the drain answers exactly and cheaply: the
    // pushed predicate row-filters INSIDE the reader, so non-matching rows
    // never emit and the rows breaker barely engages — temporal skew is not
    // the hazard the naive cost model suggested.
    let (status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\", host FROM events WHERE host = 'host-legacy' \
                      ORDER BY \"timestamp\" DESC LIMIT 20",
            "limits": { "max_rows_scanned": 20000 },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("advertised"),
        "{}",
        body["stats"]
    );
    let got: Vec<i64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            r["timestamp"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<Utc>>()
                .unwrap()
                .timestamp_millis()
        })
        .collect();
    let want: Vec<i64> = (0..20)
        .map(|i| (base + Duration::milliseconds((11_999 - i) * 100)).timestamp_millis())
        .collect();
    assert_eq!(
        got, want,
        "skewed drain must return the exact newest matches"
    );
    let drain_rows = body["stats"]["rows_scanned"].as_u64().unwrap();
    assert!(
        drain_rows < 20_000,
        "row-filtered drain must stay cheap: {drain_rows}"
    );

    // Fallback mechanism: a cap below even the matched emission trips the
    // hinted drain; the response must then come from the TopK RETRY —
    // `ordering == "filtered"` (a drain answer says "advertised"), 413 or
    // not. Distinct SQL so the result cache doesn't replay the drain answer.
    let (_status, body) = request_json(
        &app,
        serde_json::json!({
            "query": "SELECT \"timestamp\", host FROM events WHERE host = 'host-legacy' \
                      ORDER BY \"timestamp\" DESC LIMIT 19",
            "limits": { "max_rows_scanned": 1000 },
        }),
    )
    .await;
    assert_eq!(
        body["stats"]["scan"]["ordering"].as_str(),
        Some("filtered"),
        "an over-cap hinted drain must be re-answered by the TopK fallback: {body}"
    );
}
