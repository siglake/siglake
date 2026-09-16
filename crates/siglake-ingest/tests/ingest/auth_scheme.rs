//! `Bearer` is the only accepted authorization scheme.
//!
//! A second, vendor-specific `<Scheme> <token>` form used to be accepted
//! alongside it — left over from the HTTP event-collector compatibility
//! surface removed in 2026-06. Once the protocol was OTLP it bought nothing
//! (no OTLP client sends it) while widening the credential shapes the ingest
//! path accepts, and it was covered by no test at all: the acceptance was
//! untested, so the removal would have been too.
//!
//! The removal breaks nothing that shipped — 0.1.0 is unreleased, with no tag
//! and no published image.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use siglake_ingest::{router, AppState, AuthTokens, TenantRouting};
use siglake_wal::WalWriter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower::ServiceExt;

const TOKEN: &str = "s3cret";

fn payload() -> String {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": "hello" }
                }]
            }]
        }]
    })
    .to_string()
}

fn app(root: &std::path::Path) -> axum::Router {
    let writer = WalWriter::with_thresholds(root, "test", 64, Duration::from_secs(60)).unwrap();
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
        tokens: Some(Arc::new(AuthTokens::from_csv(TOKEN))),
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(2),
        remote_wal_drain: true,
    })
}

async fn post_with_auth(app: &axum::Router, header: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("content-type", "application/json")
                .header("authorization", header)
                .body(Body::from(payload()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn bearer_is_accepted() {
    let tmp = tempfile::tempdir().unwrap();
    let app = app(&tmp.path().join("wal"));
    assert_eq!(
        post_with_auth(&app, &format!("Bearer {TOKEN}")).await,
        StatusCode::OK
    );
}

/// `Bearer` is the whole of the accepted scheme set: a valid token under any
/// other scheme is refused. The removed second scheme is one instance of this
/// rule; the case below uses a neutral scheme name so the test states the rule
/// rather than re-litigating the one scheme that carried it.
#[tokio::test]
async fn a_non_bearer_scheme_is_refused_even_with_a_valid_token() {
    let tmp = tempfile::tempdir().unwrap();
    let app = app(&tmp.path().join("wal"));
    assert_eq!(
        post_with_auth(&app, &format!("Token {TOKEN}")).await,
        StatusCode::UNAUTHORIZED,
        "only Bearer is an accepted scheme; a valid token under any other must not authenticate"
    );
}

/// And no scheme at all is still refused, so the test above is measuring the
/// SCHEME rather than a blanket rejection of anything unusual.
#[tokio::test]
async fn a_bare_token_with_no_scheme_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let app = app(&tmp.path().join("wal"));
    assert_eq!(post_with_auth(&app, TOKEN).await, StatusCode::UNAUTHORIZED);
}
