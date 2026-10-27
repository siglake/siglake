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

/// #7587: a literal-key nested attr_get has a source-payload allocation bound,
/// so it must not take the generic unsupported-UDF refusal. The v0.3.0 path
/// returned the committed-only plan as soon as it saw the UDF, so projecting
/// this equality was true while putting it in `WHERE` dropped the same
/// uncommitted row.
#[tokio::test]
async fn nested_attr_get_where_keeps_the_matching_wal_row() {
    const LO: i64 = 1_793_090_139_000_000_000;
    const HI: i64 = LO + 12_000_000;

    let (dir, ice, _) = fixture().await;
    let wal = dir.path().join("nested-wal");
    let mut writer = siglake_wal::WalWriter::with_thresholds(
        &wal,
        "nested-attr",
        10_000,
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    let event =
        |timestamp_ns: i64, raw: &str, sourcetype: &str, index: &str, attributes: Option<Value>| {
            siglake_core::Event {
                timestamp: chrono::DateTime::from_timestamp_nanos(timestamp_ns),
                host: "nested-host".into(),
                source: "native-regression".into(),
                sourcetype: sourcetype.into(),
                index: index.into(),
                raw: raw.into(),
                attributes: attributes.map(|value| value.to_string()),
            }
        };
    writer
        .append_events(&[
            event(
                LO,
                "match",
                "nested.test",
                "nested-events",
                Some(json!({
                    "record.id": "match",
                    "resource.labels": {"bench.resource.int": "42"}
                })),
            ),
            event(
                LO + 1,
                "missing",
                "nested.test",
                "nested-events",
                Some(json!({
                    "record.id": "missing",
                    "resource.labels": {"other": "value"}
                })),
            ),
            event(
                LO + 2,
                "null",
                "nested.test",
                "nested-events",
                Some(json!({
                    "record.id": "null",
                    "resource.labels": {"bench.resource.int": null}
                })),
            ),
            event(
                LO + 3,
                "no-attributes",
                "nested.test",
                "nested-events",
                None,
            ),
            event(
                HI,
                "outside-time",
                "nested.test",
                "nested-events",
                Some(json!({
                    "record.id": "outside-time",
                    "resource.labels": {"bench.resource.int": "42"}
                })),
            ),
            event(
                LO + 4,
                "outside-sourcetype",
                "other.test",
                "nested-events",
                Some(json!({
                    "record.id": "outside-sourcetype",
                    "resource.labels": {"bench.resource.int": "42"}
                })),
            ),
            event(
                LO + 5,
                "outside-index",
                "nested.test",
                "other-events",
                Some(json!({
                    "record.id": "outside-index",
                    "resource.labels": {"bench.resource.int": "42"}
                })),
            ),
        ])
        .unwrap();
    writer.seal().unwrap().unwrap();
    let app = router(AppState::new(ice, AuthConfig::open()).with_wal_buffer_dir(Some(wal)));
    let nested = "attr_get(attr_get(attributes, 'resource.labels'), 'bench.resource.int')";
    let scope = format!(
        "sourcetype = 'nested.test' AND \"index\" = 'nested-events' \
         AND timestamp_ns >= {LO} AND timestamp_ns < {HI}"
    );

    let projection = format!(
        "SELECT attr_get(attributes, 'record.id') AS record_id, \
         {nested} AS resource_int, {nested} = '42' AS equals_42 \
         FROM events WHERE {scope} \
         ORDER BY attr_get(attributes, 'record.id') ASC NULLS LAST LIMIT 32"
    );
    let (status, body) = request(&app, json!({"query": projection, "format": "records"})).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["row_count"], 4);
    assert_eq!(
        body["rows"][0],
        json!({"record_id": "match", "resource_int": "42", "equals_42": true})
    );

    let filtered = format!(
        "SELECT attr_get(attributes, 'record.id') AS record_id \
         FROM events WHERE {scope} AND {nested} = '42' \
         ORDER BY attr_get(attributes, 'record.id') ASC NULLS LAST LIMIT 32"
    );
    let (status, body) = request(&app, json!({"query": filtered, "format": "records"})).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["rows"], json!([{"record_id": "match"}]));
}
