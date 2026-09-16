//! Who may write as which tenant, and how much a client header can mint.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use siglake_ingest::{router, AppState, TenantRouting, TenantWalRouter};
use siglake_wal::WalWriter;

fn otlp_body() -> String {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "t" },
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": "hello" },
                    "severityText": "INFO"
                }]
            }]
        }]
    })
    .to_string()
}

async fn app_with(allowed: Option<Vec<&str>>) -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let tenants = TenantWalRouter::new(tmp.path(), "test", 5, Duration::from_secs(60));
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 5, Duration::from_secs(60)).unwrap();
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
        allowed_tenants: allowed.map(|v| Arc::new(v.into_iter().map(str::to_string).collect())),
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
    };
    (router(state), tmp)
}

async fn post_as(app: &Router, tenant: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json");
    if let Some(t) = tenant {
        req = req.header("X-Scope-OrgID", t);
    }
    app.clone()
        .oneshot(req.body(Body::from(otlp_body())).unwrap())
        .await
        .unwrap()
        .status()
}

/// An unlisted tenant must be refused BEFORE anything is created for it.
///
/// THE DEFECT THIS GUARDS. `X-Scope-OrgID` is an unauthenticated client header,
/// and each novel value minted a backpressure lane holding an open file, a
/// fresh set of metric label values, and — downstream — an Iceberg namespace
/// with seven tables and their metadata objects. Nothing capped or evicted any
/// of it. One header could exhaust an ingester's file descriptors, collapse
/// Prometheus with cardinality, and grow the catalog without bound, in the
/// DEFAULT configuration, because the rate limiter is off by default.
#[tokio::test]
async fn an_unlisted_tenant_is_refused() {
    let (app, tmp) = app_with(Some(vec!["acme", "widgets"])).await;

    assert_eq!(post_as(&app, Some("acme")).await, StatusCode::OK);
    assert_eq!(
        post_as(&app, Some("intruder")).await,
        StatusCode::FORBIDDEN,
        "an unlisted tenant was accepted"
    );

    // And nothing was created for the refused tenant — the point is that the
    // refusal happens before the mint, not after.
    assert!(
        !tmp.path().join("intruder").exists(),
        "a refused tenant still had its WAL directory created"
    );
}

/// With no allowlist, behaviour is unchanged.
#[tokio::test]
async fn without_an_allowlist_any_tenant_is_accepted() {
    let (app, _tmp) = app_with(None).await;
    assert_eq!(post_as(&app, Some("anything")).await, StatusCode::OK);
    assert_eq!(post_as(&app, None).await, StatusCode::OK);
}

/// The routes that IGNORE the tenant still refuse a bad one, because the
/// middleware runs first.
///
/// This is what `docs/api/openapi-ingest.yaml` says on `GET|HEAD /` and both
/// cluster-health pings — exactly what a shipper calls before it sends
/// anything, so the claim is worth holding to. The search-family stubs and
/// `_cat` are covered here too, and only here: they are the one undocumented
/// family (`es_read_stubs`). The body is the siglake `{text, code}` envelope
/// even on the stubs, which otherwise answer in the ES envelope.
#[tokio::test]
async fn tenant_ignoring_routes_still_refuse_a_bad_tenant_header() {
    let ignoring = [
        ("GET", "/"),
        ("HEAD", "/"),
        ("GET", "/_cluster/health"),
        ("GET", "/api/v1/_elastic/_cluster/health"),
        ("POST", "/api/v1/_elastic/_search"),
        ("GET", "/api/v1/_elastic/_cat/indices"),
    ];

    async fn probe(
        app: &Router,
        method: &str,
        uri: &str,
        tenant: &str,
    ) -> axum::http::Response<Body> {
        app.clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    // 400: an unusable header value, on an ingester with no allowlist at all.
    let (open, _tmp) = app_with(None).await;
    for (method, uri) in ignoring {
        let resp = probe(&open, method, uri, "not a tenant!").await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "{method} {uri} accepted a malformed X-Scope-OrgID"
        );
        if method != "HEAD" {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(
                body["code"], 400,
                "{method} {uri}: not the siglake envelope"
            );
            assert!(
                body["text"].is_string() && body.get("error").is_none(),
                "{method} {uri}: expected `{{text, code}}`, got {body}"
            );
        }
    }

    // 403: a well-formed tenant outside the allowlist.
    let (bounded, _tmp) = app_with(Some(vec!["acme"])).await;
    for (method, uri) in ignoring {
        assert_eq!(
            probe(&bounded, method, uri, "intruder").await.status(),
            StatusCode::FORBIDDEN,
            "{method} {uri} served an unlisted tenant"
        );
        assert_eq!(
            probe(&bounded, method, uri, "acme").await.status(),
            if uri.contains("_search") || uri.contains("_cat") {
                StatusCode::NOT_IMPLEMENTED
            } else {
                StatusCode::OK
            },
            "{method} {uri} refused a listed tenant"
        );
    }
}

/// A tenant id becomes a DIRECTORY NAME under the WAL root, and the WAL layout
/// owns five of those names.
///
/// `list_layout_dirs` skips child directories called active/sealed/processing/
/// committed/consumers so the legacy flat layout is not mistaken for a set of
/// tenants. So `X-Scope-OrgID: active` used to return 200, the router created
/// `<wal>/active/`, segments were written to `<wal>/active/sealed/*.arrow`, and
/// neither the drain's tenant walk nor `catch_up_sweep` ever enumerated it:
/// accepted, durable, permanently unqueryable, no error anywhere.
///
/// The index-id validator already enforced this exact list. Against the old
/// tenant validator this test FAILS with 200 OK.
#[tokio::test]
async fn a_reserved_wal_layout_name_is_not_a_valid_tenant() {
    let (app, _tmp) = app_with(None).await;
    for reserved in siglake_core::index_config::RESERVED_WAL_LAYOUT_DIRS {
        assert_eq!(
            post_as(&app, Some(reserved)).await,
            StatusCode::BAD_REQUEST,
            "tenant `{reserved}` collides with the WAL layout and must be refused, not \
             written somewhere nothing enumerates"
        );
        // And case must not be a way around it: the filesystem may or may not
        // be case-sensitive, and the enumeration skip-list is exact.
        assert_eq!(
            post_as(&app, Some(&reserved.to_uppercase())).await,
            StatusCode::BAD_REQUEST,
            "tenant `{}` differs only in case from a reserved name",
            reserved.to_uppercase()
        );
    }
}
