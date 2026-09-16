//! Ingest tenancy bound to the verified identity.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use serde_json::json;
use tokio::sync::Mutex;
use tower::util::ServiceExt;

use crate::fake_idp::{issue_jwt_with_tenant, spawn_fake_idp};

use siglake_core::oidc::OidcVerifier;
use siglake_ingest::{router, AppState, TenantRouting, TenantWalRouter};
use siglake_wal::WalWriter;

fn otlp_body() -> String {
    json!({
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

async fn app_with_verifier(verifier: OidcVerifier) -> (Router, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let tenants = TenantWalRouter::new(tmp.path(), "test", 5, Duration::from_secs(60));
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 5, Duration::from_secs(60)).unwrap();
    let state = AppState {
        oidc_verifier: Some(Arc::new(verifier)),
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
    };
    (router(state), tmp)
}

async fn post(app: &Router, token: &str, header_tenant: Option<&str>) -> StatusCode {
    let mut req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"));
    if let Some(t) = header_tenant {
        req = req.header("X-Scope-OrgID", t);
    }
    app.clone()
        .oneshot(req.body(Body::from(otlp_body())).unwrap())
        .await
        .unwrap()
        .status()
}

/// With a tenant claim configured, the JWT decides the tenant — not the header.
///
/// THE DEFECT THIS GUARDS. `ingest_auth_middleware` resolved the tenant from
/// `X-Scope-OrgID` and then authenticated INDEPENDENTLY: the OIDC branch called
/// `verifier.verify(token)` and DISCARDED the returned claims. There was no
/// `--oidc-tenant-claim` on the ingest side at all, so in NO configuration was
/// the write tenant tied to a verified identity. Any holder of any accepted
/// credential could inject fabricated logs into any other tenant's namespace —
/// and the tenant selects both the WAL subtree and the Iceberg namespace the
/// drain commits to. The README's multi-tenancy section presented header
/// tenancy as isolation.
#[tokio::test]
async fn the_jwt_claim_decides_the_tenant_not_the_header() {
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-ingest";
    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let (app, tmp) = app_with_verifier(verifier).await;

    let acme = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");

    // No header: the claim supplies the tenant.
    assert_eq!(post(&app, &acme, None).await, StatusCode::OK);
    assert!(
        tmp.path().join("acme").exists(),
        "the claim's tenant was not used to route the write"
    );

    // Header agreeing with the claim: fine.
    assert_eq!(post(&app, &acme, Some("acme")).await, StatusCode::OK);

    // Header naming ANOTHER tenant: refused, and nothing created for it.
    assert_eq!(
        post(&app, &acme, Some("widgets")).await,
        StatusCode::FORBIDDEN,
        "a token for `acme` wrote into `widgets` because the header was believed"
    );
    assert!(
        !tmp.path().join("widgets").exists(),
        "a refused cross-tenant write still created the target tenant's WAL"
    );

    idp.abort();
}

/// A configured claim that the token does not carry is a refusal, not a
/// fallback to the header.
#[tokio::test]
async fn a_token_without_the_claim_is_refused() {
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-ingest";
    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        // The deployment says tenancy is authenticated...
        .with_tenant_claim("org_id");
    let (app, _tmp) = app_with_verifier(verifier).await;

    // ...but this token carries `tenant`, not `org_id`.
    let token = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    assert_eq!(
        post(&app, &token, Some("acme")).await,
        StatusCode::FORBIDDEN,
        "a token with no tenant claim fell back to trusting the header"
    );

    idp.abort();
}
