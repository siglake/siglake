//! #6978: registered Big5 SQL through the real HTTP router, compared with the
//! unchanged streaming DataFusion route. No hand-built result substitutes.
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
    request_at(app, "/api/v1/sql", body).await
}
async fn request_at(app: &Router, path: &str, body: Value) -> (StatusCode, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(
            Request::post(path)
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
    fixture_with_promotion(Default::default(), "metrics_size").await
}
async fn fixture_with_promotion(
    carrier: siglake_core::PromotionCarrier,
    name: &str,
) -> (tempfile::TempDir, Arc<IcebergContext>, Router) {
    let dir = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(dir.path())
            .await
            .unwrap()
            .with_promoted_columns(vec![PromotedColumn {
                attr_key: "metrics.size".into(),
                attr_get_path: vec![],
                carrier,
                name: name.into(),
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
                Event::now(r#"{"metrics":{"size":999}}"#)
                    .with_attributes(Some(json!({"metrics":{"size":v}}).to_string()))
            })
            .collect::<Vec<_>>();
        ice.append_events(&rows).await.unwrap();
    }
    assert!(ice.ensure_promotion_backfill_property().await.unwrap());
    let app = router(AppState::new(ice.clone(), AuthConfig::open()));
    (dir, ice, app)
}
const VALUE: &str = "CAST(attr_get(attributes, 'metrics.size') AS BIGINT)";

#[tokio::test]
async fn numeric_topk_http_registered_sql_matches_generic_ndjson_and_prunes() {
    let (_dir, _ice, app) = fixture().await;
    for direction in ["ASC", "DESC"] {
        for k in [1, 2, 5, 20] {
            let sql = format!("SELECT {VALUE} AS \"value quoted\" FROM \"events\" WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} {direction} LIMIT {k}");
            let (status, bytes) = request(&app, json!({"query":sql,"format":"records"})).await;
            let records: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, StatusCode::OK, "{records}");
            assert_eq!(records["stats"]["served_by"], "numeric_topk", "{records}");
            let (status, bytes) = request(&app, json!({"query":sql,"format":"ndjson"})).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{}",
                String::from_utf8_lossy(&bytes)
            );
            let rows = String::from_utf8(bytes)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .filter(|row| row.get("value quoted").is_some())
                .collect::<Vec<_>>();
            assert_eq!(records["rows"], json!(rows));
            if k == 1 {
                assert!(
                    records["stats"]["scan"]["row_groups_read"]
                        .as_u64()
                        .unwrap()
                        < 3
                );
            }
        }
    }
}

#[tokio::test]
async fn numeric_topk_http_unsupported_shapes_and_request_limits_use_original_path() {
    let (_dir, _ice, app) = fixture().await;
    for sql in [
        format!("SELECT {VALUE} AS value FROM events ORDER BY {VALUE} DESC LIMIT 2"),
        format!("SELECT {VALUE} AS value, raw FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 2"),
        format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL AND {VALUE}>0 ORDER BY {VALUE} DESC LIMIT 2"),
        format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 2 OFFSET 1"),
        "SELECT metrics_size AS value FROM events WHERE metrics_size IS NOT NULL ORDER BY metrics_size DESC LIMIT 2".to_string(),
    ] {
        let (status,bytes)=request(&app,json!({"query":sql,"format":"records"})).await;
        let body:Value=serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status,StatusCode::OK,"{sql}: {body}");
        assert_ne!(body["stats"]["served_by"],"numeric_topk","{body}");
    }
    let sql=format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 5");
    let (status, bytes) = request(
        &app,
        json!({"query":sql,"format":"records","limits":{"max_rows_returned":1}}),
    )
    .await;
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_ne!(body["stats"]["served_by"], "numeric_topk");
    assert_eq!(body["row_count"], 1);
    for (limits, expected_status) in [
        (json!({"max_bytes_scanned":1}), StatusCode::OK),
        (json!({"max_rows_scanned":1}), StatusCode::PAYLOAD_TOO_LARGE),
        (json!({"timeout_seconds":0}), StatusCode::GATEWAY_TIMEOUT),
    ] {
        let (status, bytes) = request(
            &app,
            json!({"query":sql,"format":"records","limits":limits}),
        )
        .await;
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status, expected_status, "{limits}: {body}");
        assert_ne!(body["stats"]["served_by"], "numeric_topk");
    }
}

#[tokio::test]
async fn numeric_topk_snapshot_binding_survives_intervening_commit_and_type_refusal() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let (_dir, ice, _app) = fixture().await;
    let qualified = ice
        .qualified_attr_rewrite_for(ice.events_table_ident())
        .await
        .unwrap()
        .unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
    let old = ice
        .qualified_numeric_topk(&qualified, "metrics_size", 20, false, &pool, u64::MAX)
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap()
        .values;
    ice.append_events(&[
        Event::now("late").with_attributes(Some(r#"{"metrics":{"size":999}}"#.into()))
    ])
    .await
    .unwrap();
    assert_ne!(
        ice.current_table_generation("events")
            .await
            .unwrap()
            .snapshot_id,
        qualified.snapshot.snapshot_id
    );
    let still_old = ice
        .qualified_numeric_topk(&qualified, "metrics_size", 20, false, &pool, u64::MAX)
        .await
        .unwrap()
        .unwrap()
        .result
        .unwrap()
        .values;
    assert_eq!(
        old, still_old,
        "captured snapshot must not refresh to a newer file set"
    );
    assert!(ice
        .qualified_numeric_topk(&qualified, "raw", 1, false, &pool, u64::MAX)
        .await
        .unwrap()
        .is_none());
    assert!(ice
        .qualified_numeric_topk(&qualified, "metrics_size", 1, false, &pool, 1)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn numeric_topk_buffer_disables_candidate_and_preserves_residual_admission() {
    let (dir, ice, _) = fixture().await;
    let wal = dir.path().join("wal");
    let mut writer = siglake_wal::WalWriter::with_thresholds(
        &wal,
        "topk",
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
    let sql=format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 3");
    let (status, bytes) = request(&app, json!({"query":sql,"format":"records"})).await;
    let actual: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{actual}");
    assert_ne!(actual["stats"]["served_by"], "numeric_topk");
    let (status, bytes) = request(
        &app,
        json!({"query":sql.replace("attr_get","ATTR_GET"),"format":"records"}),
    )
    .await;
    let residual: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{residual}");
    assert_eq!(actual["rows"], residual["rows"]);
    // Existing bounded WAL admission refuses function/cast filter workspace
    // and serves committed rows. TopK must neither bypass nor silently expand
    // that policy. #6982 separately tests a WAL-admitted plain predicate and
    // proves its raw value survives the ordinary projection rewrite guard.
    assert_eq!(
        actual["rows"],
        json!([{"value":i64::MAX},{"value":i64::MAX},{"value":8}])
    );
}

#[tokio::test]
async fn numeric_topk_shard_and_distributed_local_fallback_keep_original_plan() {
    let (_dir, ice, app) = fixture().await;
    let sql=format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 2");
    let (status, bytes) = request(
        &app,
        json!({"query":sql,"format":"records","shard":{"index":0,"count":1}}),
    )
    .await;
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(body["stats"]["served_by"], "numeric_topk");
    let coordinator = router(
        AppState::new(ice, AuthConfig::open())
            .with_coordinator(vec!["http://127.0.0.1:1".into()], None),
    );
    // Small-LIMIT distributed shapes use the existing local fallback. An
    // unreachable peer proves this test introduces no hidden remote service.
    for path in ["/api/v1/sql/local", "/api/v1/sql/distributed"] {
        let (status, bytes) =
            request_at(&coordinator, path, json!({"query":sql,"format":"records"})).await;
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_ne!(body["stats"]["served_by"], "numeric_topk");
        assert_eq!(body["rows"], json!([{"value":i64::MAX},{"value":i64::MAX}]));
    }
}

#[tokio::test]
async fn numeric_topk_late_unprovenanced_file_forces_residual_fallback() {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    let (_dir, ice, app) = fixture().await;
    let batch = siglake_core::events_to_record_batch(&[Event::now("legacy")
        .with_attributes(Some(r#"{"metrics":{"size":9223372036854775806}}"#.into()))])
    .unwrap();
    let mut fields = batch.schema().fields().to_vec();
    fields.insert(
        0,
        Arc::new(Field::new("metrics_size", DataType::Int64, true)),
    );
    let mut columns = batch.columns().to_vec();
    columns.insert(0, Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef);
    let stale = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    ice.append_unprovenanced_promoted_batch_for_test(ice.events_table_ident(), stale)
        .await
        .unwrap();
    assert!(ice
        .qualified_attr_rewrite_for(ice.events_table_ident())
        .await
        .unwrap()
        .is_none());
    let sql=format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 3");
    let (status, bytes) = request(&app, json!({"query":sql,"format":"records"})).await;
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(body["stats"]["served_by"], "numeric_topk");
    assert_eq!(
        body["rows"],
        json!([{"value":i64::MAX},{"value":i64::MAX},{"value":i64::MAX-1}])
    );
}

#[tokio::test]
#[ignore = "run in a fresh process with SIGLAKE_NUMERIC_TOPK=0"]
async fn numeric_topk_disabled_process_uses_original_handler() {
    assert_eq!(std::env::var("SIGLAKE_NUMERIC_TOPK").unwrap(), "0");
    let (_dir, _, app) = fixture().await;
    let sql=format!("SELECT {VALUE} AS value FROM events WHERE {VALUE} IS NOT NULL ORDER BY {VALUE} DESC LIMIT 3");
    let (status, bytes) = request(&app, json!({"query":sql,"format":"records"})).await;
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["stats"]["served_by"], "scan");
    assert_eq!(
        body["rows"],
        json!([{"value":i64::MAX},{"value":i64::MAX},{"value":8}])
    );
}

#[tokio::test]
async fn numeric_topk_quoted_column_requires_matching_carrier_proof() {
    let (_dir, _, app) =
        fixture_with_promotion(siglake_core::PromotionCarrier::Attributes, "Size Metric").await;
    for (carrier, expected_route, expected) in [
        (
            "attributes",
            "numeric_topk",
            json!([{"value":i64::MAX},{"value":i64::MAX}]),
        ),
        ("raw", "scan", json!([{"value":999},{"value":999}])),
    ] {
        let value = format!("CAST(attr_get({carrier}, 'metrics.size') AS BIGINT)");
        let sql=format!("SELECT {value} AS value FROM events WHERE {value} IS NOT NULL ORDER BY {value} DESC LIMIT 2");
        let (status, bytes) = request(&app, json!({"query":sql,"format":"records"})).await;
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["stats"]["served_by"], expected_route, "{body}");
        assert_eq!(body["rows"], expected);
    }
}
