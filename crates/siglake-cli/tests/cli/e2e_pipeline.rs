//! End-to-end pipeline integration: OTLP request → WAL on disk →
//! compactor → Iceberg → DataFusion query.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use datafusion::prelude::SessionContext;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use siglake_compactor::Compactor;
use siglake_ingest::{router, AppState, TenantRouting};
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::WalWriter;

fn otlp_logs(records: &[(&str, &str, &str)]) -> serde_json::Value {
    let resource_logs: Vec<_> = records
        .iter()
        .map(|(host, body, sourcetype)| {
            serde_json::json!({
                "resource": { "attributes": [
                    { "key": "host.name", "value": { "stringValue": host } },
                    { "key": "service.name", "value": { "stringValue": "s1" } }
                ]},
                "scopeLogs": [{
                    "scope": { "name": "e2e" },
                    "logRecords": [{
                        "body": { "stringValue": body },
                        "attributes": [
                            { "key": "sourcetype", "value": { "stringValue": sourcetype } },
                            { "key": "index", "value": { "stringValue": "main" } }
                        ]
                    }]
                }]
            })
        })
        .collect();
    serde_json::json!({ "resourceLogs": resource_logs })
}

fn build_app(writer: WalWriter) -> Router {
    router(AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: None,
        allowed_tenants: None,
        tenant_routing: TenantRouting::default(),
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
    })
}

async fn post_logs(app: &Router, payload: &serde_json::Value) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn otlp_batch_to_iceberg_query() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    let writer = WalWriter::with_thresholds(&wal_dir, "e2e", 4, Duration::from_secs(60)).unwrap();
    let app = build_app(writer);

    let payload = otlp_logs(&[
        ("h1", "alpha", "st"),
        ("h1", "bravo", "st"),
        ("h2", "charlie", "st"),
        ("h2", "delta", "st"),
    ]);
    let resp = post_logs(&app, &payload).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 1);

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();

    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4);

    let df = ctx
        .sql("SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
}

#[tokio::test]
async fn otlp_per_record_to_iceberg_query() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");

    let writer =
        WalWriter::with_thresholds(&wal_dir, "e2e-raw", 3, Duration::from_secs(60)).unwrap();
    let app = build_app(writer);

    for i in 0..3 {
        let body = format!("plain text event {i}");
        let payload = otlp_logs(&[("hraw", &body, "app:raw")]);
        let resp = post_logs(&app, &payload).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let n = compactor.run_once().await.unwrap();
    assert_eq!(n, 1);

    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx
        .sql("SELECT raw, host FROM events ORDER BY raw")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    let raw_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(raw_col.value(0), "plain text event 0");
    assert_eq!(raw_col.value(1), "plain text event 1");
    assert_eq!(raw_col.value(2), "plain text event 2");
    let host_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(host_col.value(0), "hraw");
}
