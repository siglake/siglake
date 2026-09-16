//! Per-request OIDC-routed multi-tenancy.
//!
//! Two JWTs, two `tenant` claims, two Iceberg namespaces:
//! the query each tenant runs sees only its own namespace's data.

use crate::support;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::routing::get;
use axum::{Json, Router};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;
use siglake_core::index_config::IndexConfig;
use siglake_core::{events_to_record_batch, Event};

use siglake_query_server::{router, AppState, AuthConfig, JobStore, OidcVerifier, TenantRegistry};
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::index_manager::IndexTemplate;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

// Public test-only signing key; never use for a deployed identity provider.
const TEST_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCvbZFLjSRszCva
bT/g688pdo3xxxI2xtW2AGxlhbTaM192V+y9bBi2qtFpGDfSrQdITrN2RwFetcoP
ooi+TcB1tr3mDSeDor/9L4GFQLYbSUcySdDsPcTiu+dEu9mQJchPFa5fYBifsaVl
XUPg7ho5hWGXWtkvPtfyJDYQ+WROa5OqSqZiWvjRnHugCtmwAhxyTtinHNHWQggj
188Bw0uhacRq63bfQK+pnttKehzChmGLI7RvFKokS0wUNHGXwT3epFqTp31vWg72
hprCtC/J4GlBcwJ0N0b0qb/E1s+xVRw2xwAcXpGiWDlRjqT+xs2aJU7N6Tw150c0
4z53mOc1AgMBAAECggEAGsfOfDrkAmLp1+rBK3A8PB9v3GbQQkH46pOmeJogiYX5
rwqNpr4riKlLljBfBz+oYrK3BVmiHSgB3ICqwOiENsQqucWs0FTmW7ummWXPVxuI
7aWkqgfk+FsIm27U8ANAkMglykQUhj57mh2SiPI4WSsiQpWZHbQJidra0R0NYcYb
lVo8mwrzTeHnkvJJIXsnR1iLXFxXMQUkmXoGZxIvdY8imB4ouL/vTNhRfOPISfdH
yAAAxmvioB35k48t2ZX89KWvK1bEUgrswrEROM0khnTBRTQv1EaO3sR3xfmL57/A
8Iekze2Wh2Eh5X1FxQ6URi9qzm68BnwS2u3CnG7nQQKBgQD0S14JsumMtTD6WISl
85neoHlcJ77S933rXA7tetyhtnKGlFY3z5yHNpEzTjQpkIKKBsy9GX/rkKXGXyw+
TDDftRjQHtOy1J5Gmw2akYEZoHJuLIKHENMIWCEz4MNgoGO9FwKHVVc9Bie+s1M4
JUZi9DafvyYTihcl2HLkiOXxdQKBgQC31XOnKay9JUY+am+gD8pr+ut0YDV06rhG
ZV7i60YEHXlJsRPYp4u+9NT1MqWbBq4irJAcDGOoy4chvAyiwwgFnLF12EHFMo1/
0BMRP/X30Yp+HkLHLi899QBIon4TA0MYJiFcIux6J5tlqjjcGoFI1V4JM3rXoPr8
1z0HB3ymwQKBgQDh2qQQN3aw/ftQGHJaswK4zogk6SIFDYc/B5dNe19rqq/rOE0V
wD2ozIwlcNHM86ucTHkRAvg/IzYAVpEi73HoARf1oep61ROXl1ZWZtuCg9IHheMP
WECi4EeiHNTFCsPrV9CgqgfDhWNNbaEssVmHttyhiCl9uxd3h8uA+ggM2QKBgBKO
4dYGRwHxOV4jsJEgBvdPpWViMQNUjrXMlf+icLcJoqzly3MbtufYH4eBTWaRDhNC
CGpMdeMcaM/nA/+KYMzwPJoA8uLNb6tvff1Hz7Ts2mZQ97zT1MEUcqrifIe+1I8j
ikqa2/SY+v8QaB0QL+0CXTPglo4eGjhcIjULdHIBAoGANtEiOGJmXsLZYdq7IzAf
xLtoKGslDsxkKB9ksJIaBVmWZ1P2ZXR2bc+mgmIEtfheB0B1QvR1tWl8PlqHOSgD
2zU9YEfR1X6JJsq41BOUEmyPCe0pPUKm0fpqYbobwWlULCVf1ITkPQsKT/rJPNq7
RSl5KChEgdpJF4QBxu0e4uA=
-----END PRIVATE KEY-----
";
const TEST_MODULUS_B64URL: &str = "r22RS40kbMwr2m0_4OvPKXaN8ccSNsbVtgBsZYW02jNfdlfsvWwYtqrRaRg30q0HSE6zdkcBXrXKD6KIvk3Adba95g0ng6K__S-BhUC2G0lHMknQ7D3E4rvnRLvZkCXITxWuX2AYn7GlZV1D4O4aOYVhl1rZLz7X8iQ2EPlkTmuTqkqmYlr40Zx7oArZsAIcck7YpxzR1kIII9fPAcNLoWnEaut230CvqZ7bSnocwoZhiyO0bxSqJEtMFDRxl8E93qRak6d9b1oO9oaawrQvyeBpQXMCdDdG9Km_xNbPsVUcNscAHF6Rolg5UY6k_sbNmiVOzek8NedHNOM-d5jnNQ";
const TEST_EXPONENT_B64URL: &str = "AQAB";
const TEST_KID: &str = "siglake-test-key";

#[derive(Serialize)]
struct ClaimsWithTenant {
    sub: String,
    iss: String,
    aud: String,
    exp: i64,
    iat: i64,
    tenant: String,
}

fn issue_jwt_with_tenant(issuer: &str, audience: &str, sub: &str, tenant: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = ClaimsWithTenant {
        sub: sub.into(),
        iss: issuer.into(),
        aud: audience.into(),
        exp: now + 600,
        iat: now,
        tenant: tenant.into(),
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.into());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM).unwrap(),
    )
    .unwrap()
}

/// Sign a token whose `tenant` claim is whatever the caller says — absent
/// (`None`), or any JSON value at all. The typed issuer above cannot express
/// "no claim" or "the claim is a number", and those are two of the shapes the
/// tenant check has to refuse.
fn issue_jwt_with_raw_tenant(
    issuer: &str,
    audience: &str,
    sub: &str,
    tenant: Option<serde_json::Value>,
) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut claims = json!({
        "sub": sub,
        "iss": issuer,
        "aud": audience,
        "exp": now + 600,
        "iat": now,
    });
    if let Some(tenant) = tenant {
        claims["tenant"] = tenant;
    }
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.into());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM).unwrap(),
    )
    .unwrap()
}

async fn spawn_fake_idp() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let issuer = format!("http://{addr}");
    let issuer_for_meta = issuer.clone();
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || {
                let issuer = issuer_for_meta.clone();
                async move {
                    Json(json!({
                        "issuer": issuer,
                        "jwks_uri": format!("{issuer}/jwks"),
                    }))
                }
            }),
        )
        .route(
            "/jwks",
            get(|| async {
                Json(json!({
                    "keys": [{
                        "kty": "RSA",
                        "use": "sig",
                        "alg": "RS256",
                        "kid": TEST_KID,
                        "n": TEST_MODULUS_B64URL,
                        "e": TEST_EXPONENT_B64URL,
                    }]
                }))
            }),
        );
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (issuer, handle)
}

fn ev(host: &str) -> Event {
    Event {
        timestamp: Utc::now(),
        host: host.into(),
        source: "test".into(),
        sourcetype: "test:json".into(),
        index: "main".into(),
        raw: format!("hi from {host}"),
        attributes: None,
    }
}

#[tokio::test]
async fn oidc_tenants_routes_query_to_per_tenant_namespace() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";

    // Default warehouse + catalog. Two tenants share the same on-disk
    // warehouse but land in different Iceberg namespaces.
    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let acme = default_ice.for_namespace("tenant_acme").await.unwrap();
    let widgets = default_ice.for_namespace("tenant_widgets").await.unwrap();
    // Seed each namespace with a distinct host so the test can assert isolation.
    let batch_acme = events_to_record_batch(&[ev("acme-host")]).unwrap();
    acme.append_batch(batch_acme).await.unwrap();
    let batch_widgets =
        events_to_record_batch(&[ev("widgets-host-1"), ev("widgets-host-2")]).unwrap();
    widgets.append_batch(batch_widgets).await.unwrap();

    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice.clone()));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");

    // acme tenant: sees 1 row.
    let token_acme = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 1, "acme should see its 1 row");

    // widgets tenant: sees 2 rows.
    let token_widgets = issue_jwt_with_tenant(&issuer, aud, "u2", "widgets");
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_widgets}"),
        )
        .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 2, "widgets should see its 2 rows");

    // Cross-tenant: acme cannot see widgets-only host.
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .json(&json!({"query": "SELECT count(*) AS n FROM events WHERE host = 'widgets-host-1'"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 0, "acme must not see widgets data");

    qs.abort();
    idp.abort();
}

/// Claim diagnostics are resolved only after OIDC selected the task's
/// namespace. A task id and claimant UUID are operational metadata, not a
/// capability that another tenant may probe.
#[tokio::test]
async fn delete_task_claim_diagnostics_are_tenant_scoped() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let default_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let acme = default_ice.for_namespace("tenant_acme").await.unwrap();
    default_ice.for_namespace("tenant_widgets").await.unwrap();
    let mut config = IndexConfig::builtin_events();
    config.index_id = "logs".to_string();
    acme.create_index(&config).await.unwrap();
    let task = acme
        .create_delete_task("logs", "host = 'private'", None, None)
        .await
        .unwrap();
    let claimant = uuid::Uuid::parse_str("018f1000-0000-7000-8000-000000000002").unwrap();
    let claim_dir = warehouse
        .join("_siglake/config/delete_tasks")
        .join(acme.namespace().to_string());
    std::fs::create_dir_all(&claim_dir).unwrap();
    std::fs::write(
        claim_dir.join(format!("{}.claim", task.task_id)),
        serde_json::to_vec(&json!({
            "task_id": task.task_id,
            "claimed_at": Utc::now(),
            "claimant": claimant,
        }))
        .unwrap(),
    )
    .unwrap();

    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let app = router(
        AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
            .with_tenants(TenantRegistry::new(default_ice)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/api/v1/delete-tasks/{}", task.task_id);

    let token_acme = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    let response = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["claim"]["present"], true);
    assert_eq!(body["claim"]["claimant"], claimant.to_string());

    let token_widgets = issue_jwt_with_tenant(&issuer, aud, "u2", "widgets");
    let response = client
        .get(&url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_widgets}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    let body = response.text().await.unwrap();
    assert!(
        !body.contains(&claimant.to_string()),
        "cross-tenant 404 leaked claim diagnostics: {body}"
    );

    qs.abort();
    idp.abort();
}

#[tokio::test]
async fn oidc_tenants_route_index_templates_to_per_tenant_namespaces() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";
    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let acme_token = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    let widgets_token = issue_jwt_with_tenant(&issuer, aud, "u2", "widgets");
    let template = IndexTemplate {
        template_id: "customer-logs".to_string(),
        index_id_patterns: vec!["customer-*".to_string()],
        priority: 10,
        doc_mapping: IndexConfig::builtin_events().doc_mapping,
        retention: None,
    };

    let response = client
        .put(format!(
            "{base}/api/v1/index-templates/{}",
            template.template_id
        ))
        .bearer_auth(&acme_token)
        .json(&template)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());

    let response = client
        .get(format!("{base}/api/v1/index-templates"))
        .bearer_auth(&acme_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<Vec<IndexTemplate>>().await.unwrap(),
        [template]
    );

    let response = client
        .get(format!("{base}/api/v1/index-templates"))
        .bearer_auth(&widgets_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(response
        .json::<Vec<IndexTemplate>>()
        .await
        .unwrap()
        .is_empty());

    qs.abort();
    idp.abort();
}

/// One tenant's batch job must be invisible to another.
///
/// THE DEFECT THIS GUARDS. `status`, `result` and `cancel` were the only
/// handlers in the query server that never extracted `CallerIdentity`, and no
/// backend recorded a tenant on a job, so lookup was by `JobId` alone. `result`
/// hands back another tenant's full result rows; `status` hands back their SQL
/// text, whose WHERE literals routinely carry user ids, IPs and account
/// identifiers; `cancel` terminates their job. A UUIDv7 is a timestamped,
/// loggable identifier, not a capability -- and with `query.jobs.persistent`
/// those ids live in the shared catalog that every tenant's data also transits.
///
/// The check answers 404 rather than 403: whether a job id exists is itself
/// information, so the "not yours" and "not there" cases must look identical.
#[tokio::test]
async fn batch_jobs_are_not_readable_across_tenants() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";

    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let acme = default_ice.for_namespace("tenant_acme").await.unwrap();
    default_ice.for_namespace("tenant_widgets").await.unwrap();
    acme.append_batch(events_to_record_batch(&[ev("acme-host")]).unwrap())
        .await
        .unwrap();

    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice.clone()));
    let app = router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // acme submits a batch job whose SQL carries a value it would not want read.
    let token_acme = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    let resp = client
        .post(format!("{base}/api/v1/sql"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .json(&json!({
            "query": "SELECT count(*) AS n FROM events WHERE host = 'acme-secret-subject'",
            "priority": "batch"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202, "batch submit should be accepted");
    let submitted: serde_json::Value = resp.json().await.unwrap();
    let job_id = submitted["job_id"]
        .as_str()
        .expect("submit response carries job_id")
        .to_string();

    // acme can see its own job.
    let resp = client
        .get(format!("{base}/api/v1/jobs/{job_id}"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "acme cannot see its own job");

    // widgets must not, on any of the three routes.
    let token_widgets = issue_jwt_with_tenant(&issuer, aud, "u2", "widgets");
    let auth = || {
        (
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_widgets}"),
        )
    };
    let status = client
        .get(format!("{base}/api/v1/jobs/{job_id}"))
        .header(auth().0, auth().1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        status.status(),
        404,
        "widgets read acme's job status: {}",
        status.text().await.unwrap()
    );

    let result = client
        .get(format!("{base}/api/v1/jobs/{job_id}/result"))
        .header(auth().0, auth().1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        result.status(),
        404,
        "widgets read acme's job result: {}",
        result.text().await.unwrap()
    );

    let cancel = client
        .delete(format!("{base}/api/v1/jobs/{job_id}"))
        .header(auth().0, auth().1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        cancel.status(),
        404,
        "widgets cancelled acme's job: {}",
        cancel.text().await.unwrap()
    );

    // --- and none of it weakens on a SECOND replica. Cross-replica
    // cancellation propagation means a `DELETE` is now routinely served by a
    // replica that is not executing the job, over a job table it shares. The
    // tenant check is per-replica and reads the row's `tenant` column, so it
    // has to hold on the replica that never saw the submission.
    let verifier_b = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let jobs_b = JobStore::new_peer_of(&state.jobs, 1, Duration::from_secs(600))
        .expect("in-memory peer store");
    let app_b = router(
        AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier_b))
            .with_tenants(TenantRegistry::new(default_ice.clone()))
            .with_jobs(jobs_b),
    );
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = listener_b.local_addr().unwrap();
    let qs_b = tokio::spawn(async move {
        axum::serve(listener_b, app_b).await.unwrap();
    });
    let base_b = format!("http://{addr_b}");

    for route in [
        format!("{base_b}/api/v1/jobs/{job_id}"),
        format!("{base_b}/api/v1/jobs/{job_id}/result"),
    ] {
        let response = client
            .get(&route)
            .header(auth().0, auth().1)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            404,
            "widgets read acme's job on replica B via {route}: {}",
            response.text().await.unwrap()
        );
    }
    let cancel_b = client
        .delete(format!("{base_b}/api/v1/jobs/{job_id}"))
        .header(auth().0, auth().1)
        .send()
        .await
        .unwrap();
    assert_eq!(
        cancel_b.status(),
        404,
        "widgets cancelled acme's job through replica B: {}",
        cancel_b.text().await.unwrap()
    );

    // acme, on the other hand, is admitted by B: `404` above is the tenant
    // check, not B failing to see a row it did not write. (Whether the DELETE
    // then answers 202 or 409 depends on whether the job has finished, which
    // is not what this test is about.)
    let acme_on_b = client
        .get(format!("{base_b}/api/v1/jobs/{job_id}"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        acme_on_b.status(),
        200,
        "acme cannot see its own job on replica B: {}",
        acme_on_b.text().await.unwrap()
    );

    qs_b.abort();
    qs.abort();
    idp.abort();
}

/// A shard request must run as the tenant the COORDINATOR names — and only when
/// it is really the coordinator asking.
///
/// THE DEFECT THIS GUARDS, which is two failures with one cause. A shard request
/// carries the coordinator's own service token, never the caller's, and the
/// worker re-derived tenancy from it:
///
///   - Under OIDC the opaque token cannot verify as a JWT, so every shard 401'd
///     and 401 is deliberately non-retryable. Turning on the only authentication
///     the chart supports broke every scanning query on the default topology
///     (`query.replicas: 2`, `distributed.enabled: true`).
///   - Under bearer auth, if the token was in the peer's allow-list, the worker
///     resolved `tenant = None` and read the DEFAULT namespace on behalf of a
///     tenant caller — a silent cross-tenant read.
///
/// The trust boundary is what this test is really about: the forwarded tenant is
/// believed because the request proved it came from the coordinator, and NOT
/// believed otherwise, so reaching `/shard` directly grants nothing extra.
#[tokio::test]
async fn shard_takes_its_tenant_from_the_coordinator_but_only_from_the_coordinator() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";
    let coord_token = "coordinator-service-token";

    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Distinct row counts per namespace, so the answer names the namespace read.
    let acme = default_ice.for_namespace("tenant_acme").await.unwrap();
    let widgets = default_ice.for_namespace("tenant_widgets").await.unwrap();
    acme.append_batch(events_to_record_batch(&[ev("a1")]).unwrap())
        .await
        .unwrap();
    widgets
        .append_batch(events_to_record_batch(&[ev("w1"), ev("w2"), ev("w3")]).unwrap())
        .await
        .unwrap();
    // The default namespace is seeded differently again, so "fell back to
    // default" is distinguishable from both tenants rather than aliasing one.
    default_ice
        .append_batch(events_to_record_batch(&[ev("d1"), ev("d2")]).unwrap())
        .await
        .unwrap();

    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice.clone()))
        .with_coordinator(vec![], Some(coord_token.to_string()));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();
    let shard_body = |tenant: &str| {
        json!({
            "query": "SELECT count(*) AS n FROM events",
            "shard": { "index": 0, "count": 1 },
            "tenant": tenant,
        })
    };

    // The coordinator's service token is accepted on /shard even under OIDC,
    // where it could never verify as a JWT, and the forwarded tenant decides
    // which namespace is read.
    let mut bodies = Vec::new();
    for tenant in ["widgets", "acme"] {
        let resp = client
            .post(format!("{base}/api/v1/sql/shard"))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {coord_token}"),
            )
            .json(&shard_body(tenant))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.bytes().await.unwrap();
        assert_eq!(
            status,
            200,
            "coordinator shard request for {tenant} was rejected: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert!(!bytes.is_empty(), "shard for {tenant} returned no body");
        bodies.push(bytes);
    }
    // The response is Arrow IPC, so rather than decode it: the two namespaces
    // hold 3 rows and 1 row, and the default holds 2. If the forwarded tenant
    // were ignored both requests would read the SAME namespace and return
    // byte-identical counts. Differing bodies is therefore the whole claim.
    assert_ne!(
        bodies[0], bodies[1],
        "shard returned identical results for two different tenants — the \
         forwarded tenant was ignored and both read one namespace"
    );

    // A USER who reaches /shard directly does not get to name a tenant: acme's
    // JWT with `tenant: widgets` in the body must read acme, not widgets.
    let token_acme = issue_jwt_with_tenant(&issuer, aud, "u1", "acme");
    let resp = client
        .post(format!("{base}/api/v1/sql/shard"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token_acme}"),
        )
        .json(&shard_body("widgets"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "acme's own shard request was rejected");

    // And a wrong service token is not admitted at all.
    let resp = client
        .post(format!("{base}/api/v1/sql/shard"))
        .header(
            reqwest::header::AUTHORIZATION,
            "Bearer not-the-coordinator-token",
        )
        .json(&shard_body("widgets"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "a bogus service token was admitted to /shard"
    );

    // The coordinator token is scoped to /shard and must not open anything else.
    let resp = client
        .post(format!("{base}/api/v1/sql"))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {coord_token}"),
        )
        .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "the coordinator token was accepted on /api/v1/sql, which it is not for"
    );

    // Being the coordinator buys the right to NAME a tenant, not the right to
    // name a nonsense one. The forwarded value goes through the same validator
    // as a JWT claim, so the trusted hop cannot be the way an identifier the
    // front door refuses reaches the registry.
    for bad in ["", "   ", "acme.corp", "../../etc/passwd"] {
        let resp = client
            .post(format!("{base}/api/v1/sql/shard"))
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {coord_token}"),
            )
            .json(&shard_body(bad))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            400,
            "the coordinator forwarded tenant {bad:?} and the shard accepted it"
        );
    }

    qs.abort();
    idp.abort();
}

/// A verified signature is not a tenant authorization.
///
/// THE DEFECT THIS GUARDS. With `--oidc-tenant-claim` configured, the query
/// server accepted any token the IDP signed and then routed it by whatever the
/// claim happened to contain:
///
///   - missing, blank or non-string claim ⇒ `extract_tenant` returned `None`,
///     `resolve_ice` read `None` as "not a tenanted deployment", and the query
///     ran against the DEFAULT namespace — the shared one — on the strength of
///     the signature alone.
///   - a claim outside `[A-Za-z0-9_-]` ⇒ the registry DROPPED the offending
///     characters, so `acme.corp` resolved to `tenant_acmecorp` and read a
///     different tenant's data.
///
/// Ingest already refused all of these (`ingest_auth_middleware`); the query
/// boundary is where an unusable claim was worth more, because it reads.
#[tokio::test]
async fn unusable_tenant_claims_are_refused_before_routing() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";

    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // Row counts name the namespace an answer came from. `acmecorp` is the
    // namespace `acme.corp` used to sanitize into, and it is seeded with data
    // of its own so "acme.corp was refused" cannot be confused with "acmecorp
    // happened to be empty".
    let acme = default_ice.for_namespace("tenant_acme").await.unwrap();
    let acmecorp = default_ice.for_namespace("tenant_acmecorp").await.unwrap();
    acme.append_batch(events_to_record_batch(&[ev("a1")]).unwrap())
        .await
        .unwrap();
    acmecorp
        .append_batch(events_to_record_batch(&[ev("c1"), ev("c2"), ev("c3")]).unwrap())
        .await
        .unwrap();
    default_ice
        .append_batch(events_to_record_batch(&[ev("d1"), ev("d2")]).unwrap())
        .await
        .unwrap();

    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap()
        .with_tenant_claim("tenant");
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice.clone()));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let overlong = "a".repeat(129);
    let cases: [(&str, Option<serde_json::Value>); 8] = [
        ("no tenant claim at all", None),
        ("empty claim", Some(json!(""))),
        ("blank claim", Some(json!("   "))),
        ("non-string claim (number)", Some(json!(42))),
        ("non-string claim (object)", Some(json!({"id": "acme"}))),
        ("overlong claim", Some(json!(overlong))),
        ("claim with dropped characters", Some(json!("acme.corp"))),
        ("path traversal claim", Some(json!("../../etc/passwd"))),
    ];
    for (label, tenant) in cases {
        let token = issue_jwt_with_raw_tenant(&issuer, aud, "u1", tenant);
        let resp = client
            .post(format!("{base}/api/v1/sql"))
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert_eq!(
            status, 403,
            "{label}: a signed token with an unusable tenant claim ran a query — {body}"
        );

        // The refusal is the middleware's, so it lands on every authenticated
        // route, including one that reads no tenant data at all.
        let resp = client
            .get(format!("{base}/debug/memory-pool"))
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            403,
            "{label}: the tenant refusal did not reach /debug/memory-pool"
        );
    }

    // VALID ROUTING IS UNTOUCHED, and the two namespaces the collision would
    // have merged stay distinct and separately reachable.
    for (tenant, expected) in [("acme", 1), ("acmecorp", 3)] {
        let token = issue_jwt_with_tenant(&issuer, aud, "u1", tenant);
        let resp = client
            .post(format!("{base}/api/v1/sql"))
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "tenant {tenant} was refused");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["rows"][0]["n"], expected,
            "tenant {tenant} read the wrong namespace"
        );
    }

    qs.abort();
    idp.abort();
}

/// The refusal is scoped to deployments that ASKED for claim-derived tenancy.
///
/// An OIDC verifier with no tenant claim configured, and the static-token mode,
/// both route every caller to the default context by design — that is the
/// single-tenant install, and a token that carries no tenant claim is not an
/// error there. Fail-closed must not turn into fail-always.
#[tokio::test]
async fn unscoped_auth_modes_still_admit_tokens_without_a_tenant() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let aud = "siglake-audience";

    let tmp = tempfile::tempdir().unwrap();
    let default_ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    default_ice
        .append_batch(events_to_record_batch(&[ev("d1"), ev("d2")]).unwrap())
        .await
        .unwrap();

    // OIDC, no tenant claim configured. A registry is still attached, so this
    // is not passing merely because tenant routing is off.
    let verifier = OidcVerifier::from_issuer(issuer.clone(), aud.into())
        .await
        .unwrap();
    let state = AppState::new(default_ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(default_ice.clone()));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let oidc_qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let token = issue_jwt_with_raw_tenant(&issuer, aud, "u1", None);
    let resp = client
        .post(format!("{base}/api/v1/sql"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap();
    assert_eq!(
        status, 200,
        "an OIDC deployment with no tenant claim refused a token without one — {body}"
    );
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["rows"][0]["n"], 2, "should read the default namespace");

    // Static bearer tokens: same expectation, no claims at all to speak of.
    let state = AppState::new(default_ice.clone(), AuthConfig::from_tokens(["tok"]))
        .with_tenants(TenantRegistry::new(default_ice.clone()));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bearer_qs = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let base = format!("http://{addr}");
    let resp = client
        .post(format!("{base}/api/v1/sql"))
        .header(reqwest::header::AUTHORIZATION, "Bearer tok")
        .json(&json!({"query": "SELECT count(*) AS n FROM events"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "bearer mode refused its own token");
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 2, "should read the default namespace");

    oidc_qs.abort();
    bearer_qs.abort();
    idp.abort();
}
