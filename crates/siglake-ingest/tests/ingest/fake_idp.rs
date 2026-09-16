//! A fake OIDC issuer, for the tests that have to hold a token the ingester
//! will actually verify.
//!
//! Shared between the HTTP tenant-identity tests and the transport-parity
//! tests: the same JWKS and the same signing key, so the two cannot end up
//! proving different things about the same verifier.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::routing::get;
use axum::{Json, Router};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use serde_json::json;

// Public test-only signing key; never use for a deployed identity provider.
pub const TEST_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
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
pub const TEST_MODULUS_B64URL: &str = "r22RS40kbMwr2m0_4OvPKXaN8ccSNsbVtgBsZYW02jNfdlfsvWwYtqrRaRg30q0HSE6zdkcBXrXKD6KIvk3Adba95g0ng6K__S-BhUC2G0lHMknQ7D3E4rvnRLvZkCXITxWuX2AYn7GlZV1D4O4aOYVhl1rZLz7X8iQ2EPlkTmuTqkqmYlr40Zx7oArZsAIcck7YpxzR1kIII9fPAcNLoWnEaut230CvqZ7bSnocwoZhiyO0bxSqJEtMFDRxl8E93qRak6d9b1oO9oaawrQvyeBpQXMCdDdG9Km_xNbPsVUcNscAHF6Rolg5UY6k_sbNmiVOzek8NedHNOM-d5jnNQ";
pub const TEST_EXPONENT_B64URL: &str = "AQAB";
pub const TEST_KID: &str = "siglake-test-key";

#[derive(Serialize)]
struct ClaimsWithTenant {
    sub: String,
    iss: String,
    aud: String,
    exp: i64,
    iat: i64,
    tenant: String,
}

pub fn issue_jwt_with_tenant(issuer: &str, audience: &str, sub: &str, tenant: &str) -> String {
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

pub async fn spawn_fake_idp() -> (String, tokio::task::JoinHandle<()>) {
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
