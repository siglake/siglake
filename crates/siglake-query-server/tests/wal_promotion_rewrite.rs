//! #6982: a committed promotion proof must not rewrite unmaterialized WAL rows.
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use siglake_core::{Event, PromotedColumn, PromotedType};
use siglake_query_server::{router, AppState, AuthConfig};
use siglake_storage::iceberg::IcebergContext;
use std::sync::Arc;
use tower::ServiceExt;

async fn request(app: &Router, body: Value) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/sql")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    (
        response.status(),
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
}
async fn fixture() -> (tempfile::TempDir, Arc<IcebergContext>, Router) {
    let dir = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(dir.path())
            .await
            .unwrap()
            .with_promoted_columns(vec![PromotedColumn {
                attr_key: "metrics.size".into(),
                attr_get_path: vec![],
                carrier: Default::default(),
                name: "metrics_size".into(),
                ty: PromotedType::Int64,
            }]),
    );
    ice.ensure_promoted_columns().await.unwrap();
    // Multiple immutable files with overlapping ranges, equal boundary values,
    // NULLs, and signed extrema. NULL itself must never consume LIMIT slots.
    for values in [
        vec![Some(i64::MIN), Some(7), None],
        vec![Some(7), Some(5), Some(8)],
        vec![Some(i64::MAX), Some(i64::MAX), None],
    ] {
        let rows = values
            .into_iter()
            .map(|v| {
                Event::now("row").with_attributes(Some(json!({"metrics":{"size":v}}).to_string()))
            })
            .collect::<Vec<_>>();
        ice.append_events(&rows).await.unwrap();
    }
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));
    (dir, ice, app)
}
#[tokio::test]
async fn wal_promotion_rewrite_preserves_raw_value_on_the_serving_route() {
    let (dir, ice, _) = fixture().await;
    let wal = dir.path().join("wal");
    let mut writer = siglake_wal::WalWriter::with_thresholds(
        &wal,
        "rewrite",
        10_000,
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    writer
        .append_events(&[Event::now("buffered")
            .with_attributes(Some(r#"{"metrics":{"size":9223372036854775806}}"#.into()))])
        .unwrap();
    writer.seal().unwrap().unwrap();
    let app = router(AppState::new(ice, AuthConfig::open()).with_wal_buffer_dir(Some(wal)));
    let (status, body) = request(
        &app,
        json!({"query":"SELECT count(*) AS n FROM events","format":"records"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["rows"],
        json!([{"n":10}])
    );
    // The plain raw predicate is admitted by the existing bounded WAL filter;
    // this isolates rewrite correctness from complex-filter admission policy.
    for function in ["ATTR_GET", "attr_get"] {
        for format in ["records", "ndjson"] {
            let sql = format!("SELECT CAST({function}(attributes, 'metrics.size') AS BIGINT) AS value FROM events WHERE raw='buffered'");
            let (status, bytes) = request(&app, json!({"query":sql,"format":format})).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );
            let rows = if format == "records" {
                serde_json::from_slice::<Value>(&bytes).unwrap()["rows"].clone()
            } else {
                json!(String::from_utf8(bytes)
                    .unwrap()
                    .lines()
                    .map(|s| serde_json::from_str::<Value>(s).unwrap())
                    .filter(|v| v.get("value").is_some())
                    .collect::<Vec<_>>())
            };
            assert_eq!(rows, json!([{"value":i64::MAX-1}]), "{function}/{format}");
        }
    }
}
