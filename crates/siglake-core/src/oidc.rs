//! OIDC JWT verification for the query API.
//!
//! Pulls the issuer's `.well-known/openid-configuration` document
//! once at startup, then fetches the JWKS lazily and caches it for
//! [`JWKS_CACHE_TTL`]. Each request verifies the bearer JWT against
//! the cached keys (matched by `kid`); on cache miss or expiry the
//! verifier refreshes the JWKS once and retries.
//!
//! Generic algorithms: RS256/RS384/RS512 + ES256/ES384. The explicit
//! Cognito access-token profile pins RS256. HMAC algorithms are intentionally not
//! accepted — they'd require a shared secret with the IDP which
//! doesn't fit the BYOC story.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
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
    cognito_access: bool,
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
            cognito_access: false,
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

    /// Explicit Cognito access-token contract. Discovery uses only the
    /// operator's trusted issuer, never a submitted token's claims.
    /// A tenant claim is mandatory; request boundaries still validate its value.
    pub async fn from_cognito_access_issuer(
        issuer: String,
        tenant_claim: String,
    ) -> Result<Self, OidcError> {
        if issuer.trim().is_empty() || tenant_claim.trim().is_empty() {
            return Err(OidcError::Config(
                "Cognito access requires an issuer and tenant claim".into(),
            ));
        }
        let mut verifier = Self::from_issuer(issuer, String::new()).await?;
        verifier.cognito_access = true;
        Ok(verifier.with_tenant_claim(tenant_claim))
    }

    /// Explicit access-token constructor for an operator-pinned JWKS endpoint.
    pub fn with_cognito_access_jwks_uri(
        issuer: String,
        jwks_uri: String,
        http: reqwest::Client,
        tenant_claim: String,
    ) -> Result<Self, OidcError> {
        if issuer.trim().is_empty() || tenant_claim.trim().is_empty() {
            return Err(OidcError::Config(
                "Cognito access requires an issuer and tenant claim".into(),
            ));
        }
        let mut verifier = Self::with_jwks_uri(issuer, String::new(), jwks_uri, http);
        verifier.cognito_access = true;
        Ok(verifier.with_tenant_claim(tenant_claim))
    }

    /// Enable per-request tenant routing. `claim_name` is the name of
    /// the JWT claim that carries the tenant identifier. Callers
    /// without a usable claim must be refused by request boundaries; they
    /// must never fall back to a default tenant.
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
        if self.cognito_access && (alg != Algorithm::RS256 || !self.has_tenant_claim()) {
            return Err(OidcError::Verify(
                "Cognito access requires RS256 and tenant routing".into(),
            ));
        }

        let key = self.get_or_refresh(&kid).await?;

        let mut validation = Validation::new(alg);
        validation.set_issuer(&[&self.issuer]);
        if self.cognito_access {
            // Cognito access tokens identify clients via signed client_id, not
            // the human client's audience (nor an optional resource audience).
            validation.validate_aud = false;
            validation.set_required_spec_claims(&["iss", "exp", "sub"]);
            validation.validate_nbf = true;
        } else {
            validation.set_audience(&[&self.audience]);
            // set_issuer/set_audience alone only check claims if present.
            validation.set_required_spec_claims(&["iss", "aud", "exp"]);
        }
        validation.validate_exp = true;
        validation.leeway = 30;

        let mut claims = decode::<Claims>(token, &key, &validation)
            .map_err(|e| OidcError::Verify(e.to_string()))?
            .claims;
        if self.cognito_access {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| OidcError::Verify("system clock before epoch".into()))?
                .as_secs();
            // jsonwebtoken does not enforce iat via required_spec_claims.
            let iat = claims.extra.get("iat").and_then(serde_json::Value::as_u64);
            if iat.is_none_or(|iat| iat > now.saturating_add(30)) || claims.sub.is_empty() {
                return Err(OidcError::Verify(
                    "invalid access token issue time or subject".into(),
                ));
            }
            if claims.string_claim("iss") != Some(self.issuer.as_str()) {
                return Err(OidcError::Verify(
                    "expected the configured issuer string".into(),
                ));
            }
            if claims
                .extra
                .get("nbf")
                .is_some_and(|nbf| nbf.as_u64().is_none())
            {
                return Err(OidcError::Verify(
                    "invalid access token not-before time".into(),
                ));
            }
            if claims.string_claim("token_use") != Some("access") {
                return Err(OidcError::Verify("expected an access token".into()));
            }
            let client_id = claims
                .string_claim("client_id")
                .filter(|id| !id.trim().is_empty() && id.len() <= 128)
                .ok_or_else(|| OidcError::Verify("invalid access token client_id".into()))?;
            // Keep tenant refusals at the existing request boundary (403),
            // but never expose provider attribution for an unusable tenant.
            if self
                .extract_tenant(&claims)
                .and_then(|tenant| crate::tenant::sanitize_tenant(&tenant))
                .is_some()
            {
                claims.provider_principal = Some(VerifiedProviderPrincipal {
                    issuer: self.issuer.clone(),
                    client_id: client_id.to_string(),
                });
            }
        }
        Ok(claims)
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

        let keys = jwks_to_keys(&jwks, self.cognito_access)?;
        let mut cache = self.cache.write().await;
        cache.keys = keys;
        cache.expires_at = Instant::now() + JWKS_CACHE_TTL;
        Ok(())
    }
}

fn jwks_to_keys(
    jwks: &JwkSet,
    cognito_access: bool,
) -> Result<HashMap<String, DecodingKey>, OidcError> {
    let mut out = HashMap::new();
    for jwk in &jwks.keys {
        // Skip keys that aren't usable for verification or don't carry a kid.
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        if cognito_access {
            if !matches!(jwk.algorithm, AlgorithmParameters::RSA(_))
                || jwk
                    .common
                    .key_algorithm
                    .is_some_and(|alg| alg != KeyAlgorithm::RS256)
                || jwk
                    .common
                    .public_key_use
                    .as_ref()
                    .is_some_and(|usage| *usage != PublicKeyUse::Signature)
                || jwk
                    .common
                    .key_operations
                    .as_ref()
                    .is_some_and(|ops| !ops.contains(&KeyOperations::Verify))
                || kid.is_empty()
            {
                continue;
            }
            if out.contains_key(&kid) {
                return Err(OidcError::Jwks("ambiguous signing kid".into()));
            }
        }
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
    // Never populated by deserialization, including a signed lookalike claim.
    #[serde(skip)]
    provider_principal: Option<VerifiedProviderPrincipal>,
}

/// Provider identity established by the explicit access-token contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedProviderPrincipal {
    pub issuer: String,
    pub client_id: String,
}

impl Claims {
    pub fn provider_principal(&self) -> Option<&VerifiedProviderPrincipal> {
        self.provider_principal.as_ref()
    }

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
    #[error("invalid OIDC configuration: {0}")]
    Config(String),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rsa_jwk() -> serde_json::Value {
        json!({"kty":"RSA", "kid":"test", "alg":"RS256", "use":"sig", "n":"AQAB", "e":"AQAB"})
    }

    #[test]
    fn cognito_key_selection_rejects_incompatible_and_ambiguous_keys() {
        let parse = |keys| serde_json::from_value::<JwkSet>(json!({"keys": keys})).unwrap();
        assert!(jwks_to_keys(&parse(vec![rsa_jwk()]), true).is_ok());
        for (field, value) in [
            ("alg", json!("RS512")),
            ("use", json!("enc")),
            ("key_ops", json!(["sign"])),
            ("kid", json!("")),
        ] {
            let mut key = rsa_jwk();
            key[field] = value;
            assert!(jwks_to_keys(&parse(vec![key.clone()]), true).is_err());
            // Legacy constructors retain their existing generic key behavior.
            assert!(jwks_to_keys(&parse(vec![key]), false).is_ok());
        }
        assert!(jwks_to_keys(&parse(vec![rsa_jwk(), rsa_jwk()]), true).is_err());
        let mut key = rsa_jwk();
        key["key_ops"] = json!(["verify"]);
        assert!(jwks_to_keys(&parse(vec![key]), true).is_ok());
    }

    #[test]
    fn deserialized_claims_cannot_fabricate_a_verified_principal() {
        let claims: Claims = serde_json::from_value(json!({
            "sub":"test", "client_id":"forged", "iss":"https://untrusted.invalid",
            "provider_principal":{"issuer":"forged", "client_id":"forged"}
        }))
        .unwrap();
        assert!(claims.provider_principal().is_none());
    }

    #[test]
    fn access_constructor_requires_issuer_and_tenant_configuration() {
        assert!(OidcVerifier::with_cognito_access_jwks_uri(
            "https://issuer.invalid".into(),
            "https://issuer.invalid/jwks".into(),
            reqwest::Client::new(),
            " ".into()
        )
        .is_err());
        assert!(OidcVerifier::with_cognito_access_jwks_uri(
            " ".into(),
            "https://issuer.invalid/jwks".into(),
            reqwest::Client::new(),
            "org".into()
        )
        .is_err());
    }
}
