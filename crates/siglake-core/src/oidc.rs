//! OIDC JWT verification for the query API.
//!
//! Pulls the issuer's `.well-known/openid-configuration` document
//! once at startup, then fetches the JWKS lazily and caches it for
//! [`JWKS_CACHE_TTL`]. Each request verifies the bearer JWT against
//! the cached keys (matched by `kid`); on cache miss or expiry the
//! verifier refreshes the JWKS once and retries.
//!
//! Algorithms: RS256/RS384/RS512 + ES256/ES384 (whatever the IDP
//! declares in the JWKS). HMAC algorithms are intentionally not
//! accepted — they'd require a shared secret with the IDP which
//! doesn't fit the BYOC story.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet};
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::RwLock;

/// How long a successfully-fetched JWKS is cached before forcing a
/// refresh. Bound is short enough that key rotation lands quickly,
/// long enough that we don't hammer the IDP.
pub const JWKS_CACHE_TTL: Duration = Duration::from_secs(900); // 15 min

/// Verifier state shared across requests.
///
/// Cheap to clone — wraps an [`Arc`] over the actual cache. Hand a
/// clone to each request handler.
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    jwks_uri: String,
    http: reqwest::Client,
    cache: Arc<RwLock<JwksCache>>,
    /// Name of the JWT claim that carries the tenant identifier when
    /// per-request multi-tenancy is enabled. `None` ⇒ single-tenant
    /// (the v0 behavior; every caller routes to the default
    /// IcebergContext namespace).
    tenant_claim: Option<String>,
}

struct JwksCache {
    keys: HashMap<String, DecodingKey>,
    expires_at: Instant,
}

impl OidcVerifier {
    /// Discover the JWKS URI by GET'ing `{issuer}/.well-known/openid-configuration`,
    /// then prime an empty cache. The first `verify` call lazily
    /// populates it.
    pub async fn from_issuer(issuer: String, audience: String) -> Result<Self, OidcError> {
        let issuer = issuer.trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| OidcError::Discover(e.to_string()))?;

        let meta_url = format!("{issuer}/.well-known/openid-configuration");
        let meta: OidcMetadata = http
            .get(&meta_url)
            .send()
            .await
            .map_err(|e| OidcError::Discover(format!("GET {meta_url}: {e}")))?
            .error_for_status()
            .map_err(|e| OidcError::Discover(format!("GET {meta_url}: {e}")))?
            .json()
            .await
            .map_err(|e| OidcError::Discover(format!("parse {meta_url}: {e}")))?;

        Ok(Self::with_jwks_uri(issuer, audience, meta.jwks_uri, http))
    }

    /// Lower-level constructor that skips discovery. Used by tests
    /// and any deployment that hardcodes the JWKS URI.
    pub fn with_jwks_uri(
        issuer: String,
        audience: String,
        jwks_uri: String,
        http: reqwest::Client,
    ) -> Self {
        Self {
            issuer,
            audience,
            jwks_uri,
            http,
            cache: Arc::new(RwLock::new(JwksCache {
                keys: HashMap::new(),
                // Past Instant ⇒ first verify forces a refresh.
                expires_at: Instant::now(),
            })),
            tenant_claim: None,
        }
    }

    /// Enable per-request tenant routing. `claim_name` is the name of
    /// the JWT claim that carries the tenant identifier. Callers
    /// without that claim (or with an empty value) route to the
    /// default IcebergContext.
    pub fn with_tenant_claim(mut self, claim_name: impl Into<String>) -> Self {
        let name = claim_name.into();
        self.tenant_claim = if name.is_empty() { None } else { Some(name) };
        self
    }

    /// Was this verifier configured to derive the tenant from a claim?
    ///
    /// The distinction matters at the ingest boundary: with a claim configured
    /// a token that does not carry one is REFUSED, because "the operator said
    /// tenancy is authenticated" and an unauthenticated tenant is not a
    /// fallback. Without one, the header is the authority by design.
    pub fn has_tenant_claim(&self) -> bool {
        self.tenant_claim.is_some()
    }

    /// Return the tenant identifier carried by the verified JWT, if
    /// the verifier was configured with a tenant claim. Empty / absent
    /// values produce `None`.
    pub fn extract_tenant(&self, claims: &Claims) -> Option<String> {
        let name = self.tenant_claim.as_deref()?;
        claims
            .string_claim(name)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    }

    /// Verify a bearer JWT. Returns the verified [`Claims`] on
    /// success; otherwise the specific failure reason.
    pub async fn verify(&self, token: &str) -> Result<Claims, OidcError> {
        let header = decode_header(token).map_err(|e| OidcError::Malformed(e.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| OidcError::Malformed("JWT header missing `kid`".into()))?;
        let alg = header.alg;

        let key = self.get_or_refresh(&kid).await?;

        let mut validation = Validation::new(alg);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.validate_exp = true;
        validation.leeway = 30;

        decode::<Claims>(token, &key, &validation)
            .map(|d| d.claims)
            .map_err(|e| OidcError::Verify(e.to_string()))
    }

    async fn get_or_refresh(&self, kid: &str) -> Result<DecodingKey, OidcError> {
        // Fast path: cache hit + not expired.
        {
            let cache = self.cache.read().await;
            if cache.expires_at > Instant::now() {
                if let Some(k) = cache.keys.get(kid) {
                    return Ok(k.clone());
                }
            }
        }
        // Slow path: refresh once, then try again.
        self.refresh().await?;
        let cache = self.cache.read().await;
        cache
            .keys
            .get(kid)
            .cloned()
            .ok_or_else(|| OidcError::UnknownKid(kid.to_string()))
    }

    async fn refresh(&self) -> Result<(), OidcError> {
        let jwks: JwkSet = self
            .http
            .get(&self.jwks_uri)
            .send()
            .await
            .map_err(|e| OidcError::Jwks(format!("GET {}: {e}", self.jwks_uri)))?
            .error_for_status()
            .map_err(|e| OidcError::Jwks(format!("GET {}: {e}", self.jwks_uri)))?
            .json()
            .await
            .map_err(|e| OidcError::Jwks(format!("parse JWKS: {e}")))?;

        let keys = jwks_to_keys(&jwks)?;
        let mut cache = self.cache.write().await;
        cache.keys = keys;
        cache.expires_at = Instant::now() + JWKS_CACHE_TTL;
        Ok(())
    }
}

fn jwks_to_keys(jwks: &JwkSet) -> Result<HashMap<String, DecodingKey>, OidcError> {
    let mut out = HashMap::new();
    for jwk in &jwks.keys {
        // Skip keys that aren't usable for verification or don't carry a kid.
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        match &jwk.algorithm {
            AlgorithmParameters::RSA(_) | AlgorithmParameters::EllipticCurve(_) => {
                let key =
                    DecodingKey::from_jwk(jwk).map_err(|e| OidcError::KeyConv(e.to_string()))?;
                out.insert(kid, key);
            }
            // Skip HMAC / OctetKeyPair — we don't accept those for OIDC.
            _ => continue,
        }
    }
    if out.is_empty() {
        return Err(OidcError::Jwks(
            "JWKS contains no usable RSA/EC keys with `kid`".into(),
        ));
    }
    Ok(out)
}

/// Claims captured from a verified JWT. We intentionally only pull
/// the fields the audit pipeline cares about by name; the rest are
/// captured into `extra` so per-deployment claim mappings (e.g. a
/// custom tenant claim name) can pull them out by string key.
#[derive(Debug, Deserialize)]
pub struct Claims {
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl Claims {
    /// Pull a string-valued claim out of either the named fields or
    /// the `extra` map. Used to look up the configured "tenant claim"
    /// (`tenant`, `tid`, `org_id`, …) on a per-deployment basis.
    pub fn string_claim(&self, name: &str) -> Option<&str> {
        match name {
            "sub" => Some(self.sub.as_str()),
            "email" => self.email.as_deref(),
            "scope" => self.scope.as_deref(),
            other => self.extra.get(other).and_then(|v| v.as_str()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OidcMetadata {
    jwks_uri: String,
}

#[derive(Debug, Error)]
pub enum OidcError {
    #[error("malformed JWT: {0}")]
    Malformed(String),
    #[error("token verification failed: {0}")]
    Verify(String),
    #[error("issuer discovery failed: {0}")]
    Discover(String),
    #[error("JWKS fetch failed: {0}")]
    Jwks(String),
    #[error("unknown signing key (kid={0})")]
    UnknownKid(String),
    #[error("JWK conversion failed: {0}")]
    KeyConv(String),
}
