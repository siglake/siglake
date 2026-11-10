//! OIDC end-to-end test.
//!
//! Spins up a fake IDP (axum) serving `/.well-known/openid-configuration`
//! plus `/jwks`, then drives the siglake-query-server against it with
//! locally-signed JWTs. The RSA keypair lives entirely in this file so
//! the test has no external IDP dependency.

use crate::support;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::routing::get;
use axum::{Json, Router};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;

use siglake_query_server::{router, AppState, AuthConfig, OidcVerifier};
use siglake_storage::iceberg::IcebergContext;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

/// RSA-2048 private key generated solely for this test. Public.
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

/// Matching modulus in base64url (RFC 7518). Generated alongside the
/// private key above.
const TEST_MODULUS_B64URL: &str = "r22RS40kbMwr2m0_4OvPKXaN8ccSNsbVtgBsZYW02jNfdlfsvWwYtqrRaRg30q0HSE6zdkcBXrXKD6KIvk3Adba95g0ng6K__S-BhUC2G0lHMknQ7D3E4rvnRLvZkCXITxWuX2AYn7GlZV1D4O4aOYVhl1rZLz7X8iQ2EPlkTmuTqkqmYlr40Zx7oArZsAIcck7YpxzR1kIII9fPAcNLoWnEaut230CvqZ7bSnocwoZhiyO0bxSqJEtMFDRxl8E93qRak6d9b1oO9oaawrQvyeBpQXMCdDdG9Km_xNbPsVUcNscAHF6Rolg5UY6k_sbNmiVOzek8NedHNOM-d5jnNQ";
const TEST_EXPONENT_B64URL: &str = "AQAB";
const TEST_KID: &str = "siglake-test-key";

/// Issue an RS256-signed JWT with the embedded test key.
fn issue_jwt(issuer: &str, audience: &str, sub: &str, exp_offset_secs: i64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let claims = Claims {
        sub: sub.to_string(),
        iss: issuer.to_string(),
        aud: audience.to_string(),
        exp: now + exp_offset_secs,
        iat: now,
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM).unwrap(),
    )
    .unwrap()
}

#[derive(Serialize)]
struct Claims {
    sub: String,
    iss: String,
    aud: String,
    exp: i64,
    iat: i64,
}

/// Spin up the fake IDP on a random port; returns the issuer URL.
fn signing_jwks(kid: &str) -> serde_json::Value {
    json!({"keys":[{"kty":"RSA", "use":"sig", "alg":"RS256", "kid":kid,
        "n":TEST_MODULUS_B64URL, "e":TEST_EXPONENT_B64URL}]})
}

async fn spawn_fake_idp() -> (String, tokio::task::JoinHandle<()>) {
    spawn_fake_idp_with_keys(Arc::new(std::sync::RwLock::new(signing_jwks(TEST_KID)))).await
}

async fn spawn_fake_idp_with_keys(
    keys: Arc<std::sync::RwLock<serde_json::Value>>,
) -> (String, tokio::task::JoinHandle<()>) {
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
            get(move || {
                let keys = keys.clone();
                async move { Json(keys.read().unwrap().clone()) }
            }),
        );

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (issuer, handle)
}

/// Spin up a siglake-query-server backed by a fresh warehouse,
/// wired to the supplied issuer for OIDC verification.
async fn spawn_query_server(
    issuer: &str,
    audience: &str,
) -> (String, tempfile::TempDir, tokio::task::JoinHandle<()>) {
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let verifier = OidcVerifier::from_issuer(issuer.to_string(), audience.to_string())
        .await
        .unwrap();
    let state = AppState::new(Arc::new(ice), AuthConfig::from_oidc(verifier));
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), tmp, handle)
}

#[tokio::test]
async fn oidc_accepts_valid_jwt() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let (base, _tmp, qs) = spawn_query_server(&issuer, "siglake-audience").await;

    let token = issue_jwt(&issuer, "siglake-audience", "alice@example.com", 600);

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({"query": "SELECT 1 AS n"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    qs.abort();
    idp.abort();
}

#[tokio::test]
async fn oidc_rejects_wrong_audience() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let (base, _tmp, qs) = spawn_query_server(&issuer, "siglake-audience").await;

    let token = issue_jwt(&issuer, "different-audience", "alice", 600);

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({"query": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    qs.abort();
    idp.abort();
}

#[tokio::test]
async fn oidc_rejects_expired_jwt() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let (base, _tmp, qs) = spawn_query_server(&issuer, "siglake-audience").await;

    // Expired an hour ago; well past the 30s leeway in the verifier.
    let token = issue_jwt(&issuer, "siglake-audience", "alice", -3600);

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({"query": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    qs.abort();
    idp.abort();
}

#[tokio::test]
async fn oidc_rejects_missing_header() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let (base, _tmp, qs) = spawn_query_server(&issuer, "siglake-audience").await;

    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .json(&json!({"query": "SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    qs.abort();
    idp.abort();
}

fn access_claims(issuer: &str) -> serde_json::Value {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    json!({"iss":issuer, "sub":"synthetic-subject", "iat":now, "exp":now + 600,
        "token_use":"access", "client_id":"client-one", "custom:org_id":"acme"})
}

fn sign_claims(claims: &serde_json::Value, alg: Algorithm, kid: Option<&str>) -> String {
    let mut header = Header::new(alg);
    header.kid = kid.map(str::to_string);
    let key = if alg == Algorithm::HS256 {
        EncodingKey::from_secret(b"synthetic-not-an-rsa-key")
    } else {
        EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM).unwrap()
    };
    encode(&header, claims, &key).unwrap()
}

struct ProfileServer {
    base: String,
    _tmp: tempfile::TempDir,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for ProfileServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn_profile_server(issuer: &str, access: bool) -> ProfileServer {
    let verifier = if access {
        OidcVerifier::from_cognito_access_issuer(issuer.into(), "custom:org_id".into())
            .await
            .unwrap()
    } else {
        OidcVerifier::from_issuer(issuer.into(), "human-audience".into())
            .await
            .unwrap()
            .with_tenant_claim("custom:org_id")
    };
    spawn_auth_server(AuthConfig::from_oidc(verifier)).await
}

async fn spawn_auth_server(auth_config: AuthConfig) -> ProfileServer {
    use siglake_query_server::{auth, CallerIdentity, TenantRegistry};
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let state = AppState::new(ice.clone(), auth_config)
        .with_tenants(TenantRegistry::new(ice))
        .with_coordinator(vec![], Some("synthetic-coordinator".into()));
    // Test-only identity observation behind the SAME production auth middleware;
    // SQL/shard requests below go through the complete production router.
    let probe = Router::new().route("/__identity", get(
        |axum::Extension(id): axum::Extension<CallerIdentity>| async move {
            Json(json!({"tenant":id.tenant, "coordinator":id.coordinator,
                "principal":id.provider_principal.map(|p| json!({"issuer":p.issuer,"client_id":p.client_id}))}))
        }
    )).route_layer(axum::middleware::from_fn_with_state(state.clone(), auth::middleware));
    let app = router(state).merge(probe);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    ProfileServer {
        base,
        _tmp: tmp,
        handle,
    }
}

async fn post_sql(server: &ProfileServer, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/v1/sql", server.base))
        .bearer_auth(token)
        .header("X-Scope-OrgID", "foreign")
        .header("X-Client-Id", "forged-client")
        .header("X-Issuer", "https://untrusted.invalid")
        .json(&json!({"query":"SELECT 1 AS n", "tenant":"foreign", "client_id":"forged-client"}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn cognito_access_requires_explicit_profile_and_preserves_verified_identity() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let access = spawn_profile_server(&issuer, true).await;
    let generic = spawn_profile_server(&issuer, false).await;
    let mut claims = access_claims(&issuer);
    claims["iat"] = json!(claims["iat"].as_u64().unwrap() - 60);
    let token = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
    assert_eq!(post_sql(&access, &token).await.status(), 200);
    assert_eq!(post_sql(&generic, &token).await.status(), 401);
    let probe = |base: String, token: String| async move {
        reqwest::Client::new()
            .get(format!("{base}/__identity"))
            .bearer_auth(token)
            .header("X-Scope-OrgID", "foreign")
            .header("X-Client-Id", "forged-client")
            .header("X-Issuer", "https://untrusted.invalid")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    let original = probe(access.base.clone(), token).await;
    assert_eq!(
        original,
        json!({"tenant":"acme", "coordinator":false,
        "principal":{"issuer":issuer,"client_id":"client-one"}})
    );
    claims["iat"] = json!(claims["iat"].as_u64().unwrap() + 60);
    claims["exp"] = json!(claims["exp"].as_u64().unwrap() + 60);
    claims["sub"] = json!("renewed-subject-not-client-identity");
    assert_eq!(
        probe(
            access.base.clone(),
            sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
        )
        .await,
        original
    );
    claims["client_id"] = json!("client-two");
    claims["aud"] = json!("not-the-human-client");
    let second_token = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
    let second = probe(access.base.clone(), second_token.clone()).await;
    assert_ne!(second["principal"], original["principal"]);
    assert_eq!(second["principal"]["client_id"], "client-two");
    assert_eq!(post_sql(&access, &second_token).await.status(), 200);
    assert_eq!(post_sql(&generic, &second_token).await.status(), 401);
    claims["aud"] = json!("human-audience");
    let generic_id = probe(
        generic.base.clone(),
        sign_claims(&claims, Algorithm::RS256, Some(TEST_KID)),
    )
    .await;
    assert_eq!(generic_id["principal"], serde_json::Value::Null);
    assert_eq!(generic_id["tenant"], "acme");
    idp.abort();
}

#[tokio::test]
async fn cognito_access_rejects_invalid_signed_contract_and_time_claims() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = spawn_profile_server(&issuer, true).await;
    let valid = access_claims(&issuer);
    let now = valid["iat"].as_u64().unwrap();
    let mut cases = vec![
        ("token_use", json!("id")),
        ("token_use", json!("refresh")),
        ("token_use", json!(1)),
        ("client_id", json!(null)),
        ("client_id", json!([])),
        ("client_id", json!("")),
        ("client_id", json!(" ")),
        ("client_id", json!("x".repeat(129))),
        ("client_id", json!("é".repeat(65))),
        ("iss", json!("https://untrusted.invalid")),
        ("iss", json!([issuer])),
        ("nbf", json!("invalid")),
        ("exp", json!(now - 3600)),
        ("iat", json!(now + 3600)),
        ("iat", json!("not-a-time")),
        ("iat", json!(-1)),
        ("nbf", json!(now + 3600)),
        ("sub", json!("")),
    ];
    for field in ["token_use", "client_id", "iss", "exp", "iat", "sub"] {
        let mut claims = valid.clone();
        claims.as_object_mut().unwrap().remove(field);
        let token = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
        assert_eq!(
            post_sql(&server, &token).await.status(),
            401,
            "missing {field}"
        );
    }
    for (field, value) in cases.drain(..) {
        let mut claims = valid.clone();
        claims[field] = value;
        let token = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
        assert_eq!(
            post_sql(&server, &token).await.status(),
            401,
            "invalid {field}"
        );
    }
    // A correctly signed ID-token-shaped credential with the human audience
    // must not be confused with an access token in the explicit profile.
    let mut id_token = valid.clone();
    id_token["token_use"] = json!("id");
    id_token["aud"] = json!("human-audience");
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&id_token, Algorithm::RS256, Some(TEST_KID))
        )
        .await
        .status(),
        401
    );
    // The documented bound is bytes; exactly 128 is still accepted.
    let mut claims = valid;
    claims["client_id"] = json!("x".repeat(128));
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
        )
        .await
        .status(),
        200
    );
    idp.abort();
}

#[tokio::test]
async fn cognito_access_rejects_bad_signature_algorithm_and_key_selection() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = spawn_profile_server(&issuer, true).await;
    let claims = access_claims(&issuer);
    for (alg, kid) in [
        (Algorithm::RS512, Some(TEST_KID)),
        (Algorithm::HS256, Some(TEST_KID)),
        (Algorithm::RS256, None),
        (Algorithm::RS256, Some("unknown")),
    ] {
        let token = sign_claims(&claims, alg, kid);
        assert_eq!(post_sql(&server, &token).await.status(), 401);
    }
    let signed = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
    let (signing_input, signature) = signed.rsplit_once('.').unwrap();
    assert_eq!(
        post_sql(&server, &format!("{signing_input}."))
            .await
            .status(),
        401
    );
    let mut foreign_claims = claims.clone();
    foreign_claims["custom:org_id"] = json!("foreign");
    let foreign = sign_claims(&foreign_claims, Algorithm::RS256, Some(TEST_KID));
    let (foreign_input, _) = foreign.rsplit_once('.').unwrap();
    assert_eq!(
        post_sql(&server, &format!("{foreign_input}.{signature}"))
            .await
            .status(),
        401
    );
    let mut token = signed.into_bytes();
    let start = token.iter().rposition(|&b| b == b'.').unwrap() + 1;
    token[start] = if token[start] == b'A' { b'B' } else { b'A' };
    assert_eq!(
        post_sql(&server, &String::from_utf8(token).unwrap())
            .await
            .status(),
        401
    );
    assert_eq!(post_sql(&server, "unsigned.claims.").await.status(), 401);
    idp.abort();
}

#[tokio::test]
async fn cognito_access_requires_valid_signed_tenant_not_headers_or_body() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = spawn_profile_server(&issuer, true).await;
    let mut claims = access_claims(&issuer);
    for value in [
        json!(null),
        json!(0),
        json!(""),
        json!(" "),
        json!("acme.corp"),
        json!("x".repeat(129)),
    ] {
        claims["custom:org_id"] = value;
        assert_eq!(
            post_sql(
                &server,
                &sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
            )
            .await
            .status(),
            403
        );
    }
    claims.as_object_mut().unwrap().remove("custom:org_id");
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
        )
        .await
        .status(),
        403
    );
    // Query's existing contract ignores header/body tenancy; neither can
    // override the signed tenant (rather than inventing new header refusals).
    claims["custom:org_id"] = json!("  acme  ");
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
        )
        .await
        .status(),
        200
    );
    idp.abort();
}

#[tokio::test]
async fn oidc_rejects_wrong_issuer_even_with_valid_signature_and_audience() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let (base, _tmp, qs) = spawn_query_server(&issuer, "siglake-audience").await;
    let token = issue_jwt(
        "https://untrusted.invalid",
        "siglake-audience",
        "alice",
        600,
    );
    let response = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .bearer_auth(token)
        .json(&json!({"query":"SELECT 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    qs.abort();
    idp.abort();
}

#[tokio::test]
async fn cognito_access_refreshes_rotated_kid_and_rejects_removed_or_ambiguous_keys() {
    require_loopback!();
    let keys = Arc::new(std::sync::RwLock::new(signing_jwks(TEST_KID)));
    let (issuer, idp) = spawn_fake_idp_with_keys(keys.clone()).await;
    let server = spawn_profile_server(&issuer, true).await;
    let claims = access_claims(&issuer);
    let original = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
    assert_eq!(post_sql(&server, &original).await.status(), 200);
    *keys.write().unwrap() = signing_jwks("rotated");
    let rotated = sign_claims(&claims, Algorithm::RS256, Some("rotated"));
    assert_eq!(post_sql(&server, &rotated).await.status(), 200);
    assert_eq!(post_sql(&server, &original).await.status(), 401);
    let mut ambiguous = signing_jwks("ambiguous");
    let duplicate = ambiguous["keys"][0].clone();
    ambiguous["keys"].as_array_mut().unwrap().push(duplicate);
    *keys.write().unwrap() = ambiguous;
    let token = sign_claims(&claims, Algorithm::RS256, Some("ambiguous"));
    assert_eq!(post_sql(&server, &token).await.status(), 401);
    idp.abort();
}

#[tokio::test]
async fn anonymous_and_static_middleware_ignore_provider_and_tenant_headers() {
    require_loopback!();
    for auth in [
        AuthConfig::open(),
        AuthConfig::from_tokens(["synthetic-static"]),
    ] {
        let server = spawn_auth_server(auth).await;
        let response = reqwest::Client::new()
            .get(format!("{}/__identity", server.base))
            .bearer_auth("synthetic-static")
            .header("X-Client-Id", "forged-client")
            .header("X-Issuer", "https://untrusted.invalid")
            .header("X-Scope-OrgID", "foreign")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap(),
            json!({"tenant":null,"coordinator":false,"principal":null})
        );
    }
}

#[tokio::test]
async fn coordinator_middleware_is_internal_and_cannot_fabricate_provider_principal() {
    require_loopback!();
    use siglake_query_server::{auth, CallerIdentity};
    let tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .unwrap();
    let state = AppState::new(Arc::new(ice), AuthConfig::from_tokens(["synthetic-static"]))
        .with_coordinator(vec![], Some("synthetic-coordinator".into()));
    // Observe the authenticated identity on the coordinator's exact allowed
    // path, using the real middleware (without replacing any production route).
    let app = Router::new()
        .route(
            "/api/v1/sql/shard",
            get(
                |axum::Extension(id): axum::Extension<CallerIdentity>| async move {
                    assert!(id.coordinator);
                    assert!(id.provider_principal.is_none());
                    assert!(id.tenant.is_none());
                    Json(json!({"internal":true}))
                },
            ),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state,
            auth::middleware,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/api/v1/sql/shard", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let response = reqwest::Client::new()
        .get(url)
        .bearer_auth("synthetic-coordinator")
        .header("X-Client-Id", "forged-client")
        .header("X-Issuer", "https://untrusted.invalid")
        .header("X-Scope-OrgID", "foreign")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap(),
        json!({"internal":true})
    );
    handle.abort();
}

#[tokio::test]
async fn generic_oidc_requires_issuer_audience_and_expiry_claims() {
    require_loopback!();
    let (issuer, idp) = spawn_fake_idp().await;
    let server = spawn_profile_server(&issuer, false).await;
    let mut valid = access_claims(&issuer);
    valid["aud"] = json!("human-audience");
    for missing in ["iss", "aud", "exp"] {
        let mut claims = valid.clone();
        claims.as_object_mut().unwrap().remove(missing);
        let token = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
        if missing != "exp" {
            // Minimal reproduction of the old verifier's optional-claim
            // behavior: same real signature, exact old Validation settings.
            let key = jsonwebtoken::DecodingKey::from_rsa_components(
                TEST_MODULUS_B64URL,
                TEST_EXPONENT_B64URL,
            )
            .unwrap();
            let mut old = jsonwebtoken::Validation::new(Algorithm::RS256);
            old.set_issuer(&[&issuer]);
            old.set_audience(&["human-audience"]);
            old.validate_exp = true;
            old.leeway = 30;
            assert!(jsonwebtoken::decode::<siglake_core::oidc::Claims>(&token, &key, &old).is_ok());
        }
        assert_eq!(
            post_sql(&server, &token).await.status(),
            401,
            "missing {missing}"
        );
    }
    assert_eq!(
        post_sql(
            &server,
            &sign_claims(&valid, Algorithm::RS256, Some(TEST_KID))
        )
        .await
        .status(),
        200
    );
    idp.abort();
}

#[tokio::test]
async fn cognito_access_external_shard_body_cannot_override_signed_tenant() {
    external_shard_body_cannot_override_signed_tenant(false).await;
}

#[tokio::test]
async fn cognito_mixed_external_shard_body_cannot_override_signed_tenant() {
    external_shard_body_cannot_override_signed_tenant(true).await;
}

async fn external_shard_body_cannot_override_signed_tenant(mixed: bool) {
    require_loopback!();
    use siglake_core::{events_to_record_batch, Event};
    let (issuer, idp) = spawn_fake_idp().await;
    let server = if mixed {
        let verifier = OidcVerifier::from_cognito_mixed_issuer(
            issuer.clone(),
            "human-audience".into(),
            "custom:org_id".into(),
        )
        .await
        .unwrap();
        spawn_auth_server(AuthConfig::from_oidc(verifier)).await
    } else {
        spawn_profile_server(&issuer, true).await
    };
    let ice = IcebergContext::open(&server._tmp.path().join("warehouse"))
        .await
        .unwrap();
    for (tenant, count) in [("acme", 1), ("foreign", 2)] {
        let tenant_ice = ice
            .for_namespace(&format!("tenant_{tenant}"))
            .await
            .unwrap();
        let events: Vec<_> = (0..count)
            .map(|_| Event {
                timestamp: chrono::Utc::now(),
                host: "synthetic".into(),
                source: "test".into(),
                sourcetype: "test:json".into(),
                index: "main".into(),
                raw: "synthetic".into(),
                attributes: None,
            })
            .collect();
        tenant_ice
            .append_batch(events_to_record_batch(&events).unwrap())
            .await
            .unwrap();
    }
    let shard = |token: String, tenant: &'static str| {
        let base = server.base.clone();
        async move {
            let response = reqwest::Client::new().post(format!("{base}/api/v1/sql/shard"))
                .bearer_auth(token).header("X-Scope-OrgID", "foreign")
                .header("X-Client-Id", "forged-client")
                .json(&json!({"query":"SELECT count(*) AS n FROM events", "shard":{"index":0,"count":1},"tenant":tenant}))
                .send().await.unwrap();
            assert_eq!(response.status(), 200);
            response.bytes().await.unwrap()
        }
    };
    for use_ in if mixed {
        vec!["access", "id"]
    } else {
        vec!["access"]
    } {
        let mut claims = access_claims(&issuer);
        claims["token_use"] = json!(use_);
        claims["aud"] = json!("human-audience");
        claims["provider_principal"] = json!({"issuer":issuer,"client_id":"forged"});

        let acme = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
        let own = shard(acme.clone(), "acme").await;
        assert_eq!(shard(acme, "foreign").await, own);
        claims["custom:org_id"] = json!("foreign");
        let foreign = shard(
            sign_claims(&claims, Algorithm::RS256, Some(TEST_KID)),
            "foreign",
        )
        .await;
        assert_ne!(own, foreign);
        assert_eq!(
            shard("synthetic-coordinator".into(), "foreign").await,
            foreign
        );
        assert_eq!(
            post_sql(&server, "synthetic-coordinator").await.status(),
            401
        );
    }
    idp.abort();
}

#[path = "buffered_controls.rs"]
mod buffered_controls;
#[path = "buffered_history.rs"]
mod buffered_history;

#[path = "startup_history.rs"]
mod startup_history;

#[path = "cognito_mixed.rs"]
mod cognito_mixed;

#[path = "selected_paths_controls.rs"]
mod selected_paths_controls;

#[path = "runtime_surface.rs"]
mod runtime_surface;
