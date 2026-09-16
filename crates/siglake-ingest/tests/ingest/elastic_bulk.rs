use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use datafusion::prelude::SessionContext;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use siglake_compactor::Compactor;
use siglake_ingest::{router, AppState, TenantRouting, TenantWalRouter};
use siglake_storage::iceberg::IcebergContext;
use siglake_wal::{list_sealed, WalWriter};

fn build_app(tmp: &tempfile::TempDir, max_events: usize, force_timeout: Duration) -> Router {
    let tenants = TenantWalRouter::new(tmp.path(), "test", max_events, Duration::from_secs(60));
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", max_events, Duration::from_secs(60))
            .unwrap();
    router(AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        allowed_tenants: None,
        tenant_routing: TenantRouting::default(),
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: force_timeout,
        remote_wal_drain: true,
    })
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap()
}

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn otlp_logs(body: &str) -> serde_json::Value {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "force-host" } },
                { "key": "service.name", "value": { "stringValue": "force-svc" } }
            ]},
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": body },
                    "attributes": [
                        { "key": "sourcetype", "value": { "stringValue": "force:test" } }
                    ]
                }]
            }]
        }]
    })
}

async fn count_events(ice: &IcebergContext) -> i64 {
    let ctx = SessionContext::new();
    ice.register_with_datafusion(&ctx).await.unwrap();
    let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
    let batches = df.collect().await.unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn bulk_happy_path_routes_to_action_and_path_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));
    let body = br#"{"index":{"_index":"app1"}}
{"message":"alpha","host":{"name":"h1"},"service":{"name":"svc1"}}
{"create":{}}
{"message":"beta","host":"h2","source":"bulk-src"}
"#
    .to_vec();

    let resp = send(&app, "POST", "/api/v1/_elastic/pathbulk/_bulk", None, body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["errors"], false);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(body["items"][0]["index"]["_index"], "app1");
    assert_eq!(body["items"][0]["index"]["status"], 201);
    assert_eq!(body["items"][1]["create"]["_index"], "pathbulk");
    assert_eq!(body["items"][1]["create"]["status"], 201);

    assert!(!list_sealed(&tmp.path().join("default").join("app1"))
        .unwrap()
        .is_empty());
    assert!(!list_sealed(&tmp.path().join("default").join("pathbulk"))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn bulk_update_and_delete_are_per_item_400s() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));
    let body = br#"{"update":{"_index":"app1"}}
{"doc":{"message":"alpha"}}
{"delete":{"_index":"app1"}}
"#
    .to_vec();

    let resp = send(&app, "POST", "/api/v1/_elastic/_bulk", None, body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"][0]["update"]["status"], 400);
    assert_eq!(
        body["items"][0]["update"]["error"]["type"],
        "unsupported_operation_exception"
    );
    assert_eq!(body["items"][1]["delete"]["status"], 400);
}

#[tokio::test]
async fn bulk_invalid_index_is_per_item_400() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));
    let body = br#"{"index":{"_index":"Bad"}}
{"message":"alpha"}
"#
    .to_vec();

    let resp = send(&app, "POST", "/api/v1/_elastic/_bulk", None, body).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = json_body(resp).await;
    assert_eq!(body["errors"], true);
    assert_eq!(body["items"][0]["index"]["status"], 400);
    assert_eq!(
        body["items"][0]["index"]["error"]["type"],
        "illegal_argument_exception"
    );
}

#[tokio::test]
async fn bulk_malformed_ndjson_returns_es_error_body() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));
    let resp = send(
        &app,
        "POST",
        "/api/v1/_elastic/_bulk",
        None,
        br#"{"index":{"_index":"app1"}}
"#
        .to_vec(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = json_body(resp).await;
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "parse_exception");
}

#[tokio::test]
async fn elastic_probe_endpoints_and_search_pointer() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));

    let root = send(&app, "GET", "/", None, vec![]).await;
    assert_eq!(root.status(), StatusCode::OK);
    let root_body = json_body(root).await;
    assert_eq!(root_body["version"]["number"], "7.10.2");
    assert_eq!(root_body["tagline"], "You Know, for Search");

    let head = send(&app, "HEAD", "/", None, vec![]).await;
    assert_eq!(head.status(), StatusCode::OK);

    let health = send(&app, "GET", "/_cluster/health", None, vec![]).await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await["status"], "green");

    let prefixed = send(
        &app,
        "GET",
        "/api/v1/_elastic/_cluster/health",
        None,
        vec![],
    )
    .await;
    assert_eq!(prefixed.status(), StatusCode::OK);
    assert_eq!(json_body(prefixed).await["status"], "green");

    let search = send(
        &app,
        "POST",
        "/api/v1/_elastic/events/_search",
        None,
        vec![],
    )
    .await;
    assert_eq!(search.status(), StatusCode::NOT_IMPLEMENTED);
    let body = json_body(search).await;
    assert_eq!(body["status"], 501);
    assert!(body["error"]["reason"]
        .as_str()
        .unwrap()
        .contains("/api/v1/sql"));
}

/// Every ES search-family shape answers `501` on both verbs.
///
/// These paths are deliberately absent from the OpenAPI document
/// (`es_read_stubs`), so the spec's pinned route table cannot say the family is
/// still fully routed and this test is the only thing that does — including
/// `_cat`, whose axum catch-all (`{*tail}`) has to keep matching *multiple*
/// trailing segments.
#[tokio::test]
async fn elastic_search_family_is_not_implemented_on_every_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 1, Duration::from_secs(30));

    let both_verbs = [
        "/api/v1/_elastic/_search",
        "/api/v1/_elastic/events/_search",
        "/api/v1/_elastic/_msearch",
        "/api/v1/_elastic/events/_msearch",
        "/api/v1/_elastic/_search/scroll",
        "/api/v1/_elastic/_field_caps",
        "/api/v1/_elastic/events/_field_caps",
    ];
    for path in both_verbs {
        for method in ["GET", "POST"] {
            let resp = send(&app, method, path, None, vec![]).await;
            assert_eq!(
                resp.status(),
                StatusCode::NOT_IMPLEMENTED,
                "{method} {path} should be 501"
            );
            assert_eq!(json_body(resp).await["status"], 501, "{method} {path}");
        }
    }

    // `_cat` is GET-only and takes a catch-all tail: one segment and several
    // must both route to the stub rather than 404.
    for path in [
        "/api/v1/_elastic/_cat/indices",
        "/api/v1/_elastic/_cat/nodes/stats/fs",
    ] {
        let resp = send(&app, "GET", path, None, vec![]).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_IMPLEMENTED,
            "GET {path} should be 501"
        );
        assert_eq!(json_body(resp).await["status"], 501, "GET {path}");
    }
}

#[tokio::test]
async fn commit_force_waits_until_segment_is_queryable() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let warehouse = tmp.path().join("warehouse");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    // Tenant "default" commits to the DEFAULT namespace (namespace unification).
    let tenant_ice = ice.clone();
    let compactor = Compactor::new(&wal_dir, ice.clone());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_bg = stop.clone();
    let bg = tokio::spawn(async move {
        while !stop_bg.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = compactor.run_once().await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });

    let app = {
        let tenants = TenantWalRouter::new(&wal_dir, "test", 100, Duration::from_secs(60));
        let mirror_store = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let (_mirror, mirror_handle) =
            siglake_wal::mirror::WalMirror::new(mirror_store, "wal-mirror");
        tenants
            .set_mirror_handle(Some(mirror_handle))
            .await
            .unwrap();
        let writer =
            WalWriter::with_thresholds(&wal_dir, "test", 100, Duration::from_secs(60)).unwrap();
        router(AppState {
            oidc_verifier: None,
            writer: Arc::new(Mutex::new(writer)),
            tenants: Some(Arc::new(tenants)),
            allowed_tenants: None,
            tenant_routing: TenantRouting::default(),
            max_tenants: 0,
            tenant_admission: Default::default(),
            backpressure: None,
            events_tx: None,
            tokens: None,
            rate_limiter: None,
            mem_guard: None,
            commit_force_timeout: Duration::from_secs(2),
            remote_wal_drain: false,
        })
    };

    let resp = send(
        &app,
        "POST",
        "/v1/logs?commit=force",
        Some("application/json"),
        serde_json::to_vec(&otlp_logs("force-body")).unwrap(),
    )
    .await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    bg.await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(count_events(&tenant_ice).await, 1);
}

#[tokio::test]
async fn commit_force_timeout_returns_504() {
    let tmp = tempfile::tempdir().unwrap();
    let app = build_app(&tmp, 100, Duration::from_millis(50));
    let resp = send(
        &app,
        "POST",
        "/v1/logs?commit=force",
        Some("application/json"),
        serde_json::to_vec(&otlp_logs("timeout-body")).unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
    let body = json_body(resp).await;
    assert_eq!(body["code"], 504);
    assert!(body["text"]
        .as_str()
        .unwrap()
        .contains("commit=force timed out"));
}
