use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use chrono::{Duration, TimeZone, Utc};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
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

async fn request_json(app: &Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(path)
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
async fn fts_udfs_return_correct_rows_and_emit_pruning_metrics() {
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

    let chunks: [&[&str]; 3] = [
        &[
            "healthy heartbeat",
            "healthy heartbeat",
            "connection temporarily refused",
            "refused connection order",
        ],
        &[
            "error timeout retry",
            "error timeout backoff",
            "database connection refused by peer",
            "connection refused hard",
        ],
        &[
            "error only row",
            "timeout only row",
            "healthy row",
            "warning row",
        ],
    ];
    for (file_idx, chunk) in chunks.into_iter().enumerate() {
        let evs: Vec<Event> = chunk
            .iter()
            .enumerate()
            .map(|(row_idx, raw)| Event {
                timestamp: base + Duration::seconds((file_idx * 10 + row_idx) as i64),
                host: "host".into(),
                source: "fts-test".into(),
                sourcetype: "app".into(),
                index: "main".into(),
                raw: (*raw).to_string(),
                attributes: None,
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let app = router(AppState::new(ice, AuthConfig::open()));

    let (status, body) = request_json(
        &app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'error timeout')"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(2));
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            &[("source", "fts_udf")]
        ) > 0
            || counter_sum(
                &snapshot,
                "siglake_iceberg_raw_bloom_skip_total",
                &[("source", "fts_udf"), ("outcome", "skip")]
            ) > 0,
        "expected FTS pruning metrics to engage for match_terms"
    );

    let (status, body) = request_json(
        &app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE match_terms(raw, 'zzznothing')"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(0));
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_raw_bloom_skip_total",
            &[("source", "fts_udf"), ("outcome", "skip")]
        ) > 0,
        "expected absent-term match_terms query to skip at least one file"
    );

    let (status, body) = request_json(
        &app,
        "/api/v1/sql/local",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE match_phrase(raw, 'connection refused')"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(2));
    let snapshot = snapshotter.snapshot().into_vec();
    assert!(
        counter_sum(
            &snapshot,
            "siglake_iceberg_inverted_index_used_total",
            &[("source", "fts_udf")]
        ) > 0,
        "expected match_phrase to engage all-terms index pruning before row re-check"
    );
}

/// The bloom file-skip is attributed per request: a substring only present in
/// one file prunes the other three WHOLE files (trigram bloom, post-footer),
/// and the response's `stats.scan` says so.
#[tokio::test]
async fn scan_detail_attributes_bloom_file_pruning() {
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_inverted_index(true),
    );
    let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    for i in 0..4 {
        let evs: Vec<Event> = (0..25)
            .map(|j| Event {
                timestamp: base + Duration::seconds((i * 100 + j) as i64),
                host: format!("host-{}", j % 4),
                source: "scan-detail-test".into(),
                sourcetype: "app".into(),
                index: "main".into(),
                raw: format!("row {j} marker-{i} filler"),
                attributes: None,
            })
            .collect();
        ice.append_events(&evs).await.unwrap();
    }

    let app = router(AppState::new(ice, AuthConfig::open()));
    let (status, body) = request_json(
        &app,
        "/api/v1/sql",
        serde_json::json!({
            "query": "SELECT count(*) AS n FROM events WHERE raw LIKE '%marker-3%'"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["n"].as_i64(), Some(25), "{body}");
    let scan = body["stats"]["scan"]
        .as_object()
        .unwrap_or_else(|| panic!("scanning query must carry stats.scan: {body}"));
    assert_eq!(scan["files_planned"].as_u64(), Some(4), "{body}");
    assert_eq!(
        scan["files_read"].as_u64(),
        Some(4),
        "all footers open: {body}"
    );
    assert_eq!(
        scan["files_pruned_bloom"].as_u64(),
        Some(3),
        "three files provably lack 'marker-3': {body}"
    );
    assert_eq!(
        scan["row_groups_read"].as_u64(),
        Some(1),
        "only the matching file's row group is read: {body}"
    );
}
