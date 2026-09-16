//! #4375: a text query with a literal `LIMIT` must not load a whole-file
//! inverted index, end to end through the SQL route.
//!
//! The unit tests either side of this cover the halves: `clipping_scan_limit`
//! reads the shape off the AST, `text_index_decline_reason` turns the hint into
//! a refusal. What only a request can show is that the two are connected — the
//! hint travels from the parsed statement into the session the provider plans
//! against, and the request still returns the right rows without it.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::Value;
use tower::util::ServiceExt;

use siglake_core::Event;
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;

type SnapshotVec = Vec<(
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
)>;

fn counter_sum(snapshot: &SnapshotVec, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == name
                && labels.iter().all(|(lk, lv)| {
                    key.key()
                        .labels()
                        .any(|l| l.key() == *lk && l.value() == *lv)
                })
        })
        .map(|(_, _, _, value)| match value {
            DebugValue::Counter(c) => *c,
            _ => 0,
        })
        .sum()
}

async fn query(app: &Router, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
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

/// Every counter here is process-wide and the recorder reports deltas, so each
/// phase reads its own snapshot and nothing else in this binary runs a text
/// query.
fn declines(snapshotter: &Snapshotter, reason: &str) -> u64 {
    counter_sum(
        &snapshotter.snapshot().into_vec(),
        "siglake_query_inverted_index_declined_total",
        &[("reason", reason)],
    )
}

#[tokio::test]
async fn a_clipped_text_limit_declines_the_index_and_still_returns_its_rows() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_inverted_index(true)
            .with_table_cache_ttl(std::time::Duration::ZERO),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    // `queen` on every fifth row, the benchmark corpus in miniature: enough
    // matches that a `LIMIT 5` clips, few enough that the unclipped shape
    // below can be compared row for row.
    for file in 0..3 {
        let events: Vec<Event> = (0..40)
            .map(|row| {
                let ordinal = file * 40 + row;
                let queen = if ordinal % 5 == 0 { " queen" } else { "" };
                Event {
                    timestamp: base + Duration::seconds(ordinal),
                    host: "host".into(),
                    source: "clipped-decline".into(),
                    sourcetype: "app".into(),
                    index: "main".into(),
                    raw: format!("service status{queen} row-{ordinal:04}"),
                    attributes: None,
                }
            })
            .collect();
        ice.append_events(&events).await.unwrap();
    }

    let app = router(AppState::new(ice, AuthConfig::open()));
    let _ = snapshotter.snapshot();

    // `default_order: false` is how the benchmark's text shapes run, and the
    // reason this card exists: the rounds that blew the ceilings carried
    // NEITHER ordering extension, so #3771's ordered decline never saw them.
    let (status, body) = query(
        &app,
        serde_json::json!({
            "query": "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 5",
            "default_order": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(5));
    assert!(
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["raw"].as_str().is_some_and(|raw| raw.contains("queen"))),
        "a declined index must not change which rows match: {body}"
    );
    assert_eq!(declines(&snapshotter, "clipped_limit"), 1);

    // Same predicate, no LIMIT: nothing clips the scan, so the index stays
    // eligible. This is the shape #4329 measured the index WINNING on.
    let (status, body) = query(
        &app,
        serde_json::json!({
            "query": "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen')",
            "default_order": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(24));
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_query_inverted_index_declined_total",
            &[("reason", "clipped_limit")]
        ),
        0,
        "an unclipped text scan must keep the index"
    );
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            &[("source", "fts_udf")]
        ) > 0,
        "and must actually row-select from it"
    );

    // The LIMIT clips the aggregate's one row, not the scan feeding it.
    let (status, body) = query(
        &app,
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'queen') LIMIT 5",
            "default_order": false,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(24));
    assert_eq!(declines(&snapshotter, "clipped_limit"), 0);

    // With the default newest-first rewrite the same clipped browse is the
    // ORDERED decline's: the rewrite injects an ORDER BY, and the two hints
    // are never both set, so the refusal keeps one attribution.
    let (status, body) = query(
        &app,
        serde_json::json!({
            "query": "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 5",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(5));
    let snapshot = snapshotter.snapshot().into_vec();
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_query_inverted_index_declined_total",
            &[("reason", "clipped_limit")]
        ),
        0,
    );
    assert_eq!(
        counter_sum(
            &snapshot,
            "siglake_query_inverted_index_declined_total",
            &[("reason", "ordered_limit")]
        ),
        1,
    );
}
