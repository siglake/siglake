//! Bearer-token / OIDC middleware.
//!
//! Three modes, picked at startup based on which env vars / CLI flags
//! the operator supplied:
//!
//! - [`AuthConfig::Open`] — every request passes. Logged with a warning
//!   at startup; only safe inside a trusted network.
//! - [`AuthConfig::Tokens`] — static bearer-token allow-list. Same
//!   shape as 4.5. Useful for dev / CI.
//! - [`AuthConfig::Oidc`] — verifies the request's bearer JWT against
//!   the configured issuer's JWKS. Customer SSO/IDP path.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::Extensions;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::oidc::{Claims, OidcVerifier};
use crate::AppState;

/// Authentication configuration. Cheap to clone — internal state
/// behind [`Arc`]s.
#[derive(Clone)]
pub enum AuthConfig {
    Open,
    Tokens(Arc<HashSet<String>>),
    Oidc(Arc<OidcVerifier>),
}

impl AuthConfig {
    /// No authentication required.
    pub fn open() -> Self {
        Self::Open
    }

    /// Static bearer-token allow-list. Empty/whitespace entries are
    /// dropped; an entirely-empty result collapses to [`AuthConfig::Open`].
    pub fn from_tokens<I, S>(tokens: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let tokens: HashSet<String> = tokens
            .into_iter()
            .map(Into::into)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if tokens.is_empty() {
            Self::Open
        } else {
            Self::Tokens(Arc::new(tokens))
        }
    }

    /// Wrap an [`OidcVerifier`].
    pub fn from_oidc(verifier: OidcVerifier) -> Self {
        Self::Oidc(Arc::new(verifier))
    }

    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }

    /// Human-readable label for startup logs + metrics.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Tokens(_) => "bearer",
            Self::Oidc(_) => "oidc",
        }
    }
}

/// Caller identity attached to a request after successful auth.
/// Pulled out of the request's [`Extensions`] by handlers that want
/// it (e.g. audit logging).
#[derive(Debug, Clone, Default)]
pub struct CallerIdentity {
    /// `sub` claim from OIDC, the token string for bearer mode, or
    /// `anonymous` for open mode.
    pub subject: String,
    /// `email` claim if the IDP supplied one.
    pub email: Option<String>,
    /// Tenant identifier carried by the JWT (e.g. the configured
    /// `tenant`/`tid`/`org_id` claim) when per-request multi-tenancy
    /// is enabled on the OIDC verifier. `None` ⇒ route to the
    /// default tenant on the [`AppState`].
    ///
    /// `None` therefore means "this deployment is not tenant-scoped" — open
    /// auth, bearer auth, an OIDC verifier with no configured tenant claim, or
    /// the coordinator's service account. It never means "a tenant-scoped
    /// deployment could not work out who this is": that is a `403` in
    /// [`resolve_claim_tenant`] and no identity is built at all. When it is
    /// `Some`, the value has already passed [`crate::tenants::validate`].
    pub tenant: Option<String>,
    /// This request is our own coordinator fanning a query out to us, proven by
    /// presenting this node's `coordinator_token`.
    ///
    /// The single place that decides it, so the shard handler does not have to
    /// re-derive the same fact from the same header. That matters: a shard
    /// request carries the COORDINATOR's credential, so `tenant` above describes
    /// a service account rather than the person who asked, and the handler needs
    /// to know it may take the tenant from the request body instead.
    pub coordinator: bool,
}

impl CallerIdentity {
    fn from_token(_token: &str) -> Self {
        Self {
            subject: "bearer".into(),
            email: None,
            tenant: None,
            coordinator: false,
        }
    }

    fn from_claims(claims: Claims, tenant: Option<String>) -> Self {
        Self {
            subject: claims.sub,
            email: claims.email,
            tenant,
            coordinator: false,
        }
    }

    /// The coordinator itself, fanning a query out to this node.
    ///
    /// `tenant: None` deliberately: the service account belongs to no tenant,
    /// and the real one arrives in the shard request body.
    fn coordinator() -> Self {
        Self {
            subject: "coordinator".into(),
            email: None,
            tenant: None,
            coordinator: true,
        }
    }

    fn anonymous() -> Self {
        Self {
            subject: "anonymous".into(),
            email: None,
            tenant: None,
            coordinator: false,
        }
    }
}

/// axum middleware that enforces the configured [`AuthConfig`].
pub async fn middleware(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    // INTERNAL FAN-OUT PRINCIPAL.
    //
    // The coordinator authenticates to peers with a shared service token, which
    // is not a user credential and — under OIDC — cannot possibly be one: an
    // opaque token will never verify as a JWT, so every shard request 401'd and
    // 401 is deliberately non-retryable. Turning on the only authentication the
    // chart supports therefore broke every scanning query on the default
    // topology.
    //
    // Scoped to `/api/v1/sql/shard`, the only thing this token is for. It is a
    // cluster-internal secret, but that is a reason to keep its reach narrow
    // rather than to widen it: nothing else should be reachable by presenting
    // it.
    if let Some(expected) = state.coordinator_token.as_deref() {
        if request.uri().path() == "/api/v1/sql/shard"
            && extract_bearer(request.extensions(), request.headers()) == Some(expected)
        {
            request
                .extensions_mut()
                .insert(CallerIdentity::coordinator());
            return next.run(request).await;
        }
    }
    let identity = match state.auth.as_ref() {
        AuthConfig::Open => CallerIdentity::anonymous(),
        AuthConfig::Tokens(tokens) => match extract_bearer(request.extensions(), request.headers())
        {
            Some(t) if tokens.contains(t) => CallerIdentity::from_token(t),
            Some(_) => return ApiError::unauthorized("invalid bearer token").into_response(),
            None => {
                return ApiError::unauthorized(
                    "missing Authorization header (expected `Bearer <token>`)",
                )
                .into_response();
            }
        },
        AuthConfig::Oidc(verifier) => {
            match extract_bearer(request.extensions(), request.headers()) {
                Some(t) => match verifier.verify(t).await {
                    Ok(claims) => match resolve_claim_tenant(verifier, &claims) {
                        Ok(tenant) => CallerIdentity::from_claims(claims, tenant),
                        Err(refusal) => return refusal.into_response(),
                    },
                    Err(e) => {
                        tracing::debug!(error = %e, "oidc verify failed");
                        return ApiError::unauthorized(format!("oidc verify failed: {e}"))
                            .into_response();
                    }
                },
                None => {
                    return ApiError::unauthorized(
                        "missing Authorization header (expected `Bearer <jwt>`)",
                    )
                    .into_response();
                }
            }
        }
    };
    request.extensions_mut().insert(identity);
    next.run(request).await
}

/// Decide the tenant a verified token is scoped to, or the refusal that stops
/// the request before it can be routed anywhere.
///
/// TOKEN VALIDITY IS NOT TENANT AUTHORIZATION. When the operator configured a
/// tenant claim they said tenancy is authenticated; a token that carries no
/// usable one is therefore refused, exactly as ingest refuses it
/// (`ingest_auth_middleware`). It used to be accepted: `extract_tenant`
/// returned `None` for a missing, blank or non-string claim, `resolve_ice` read
/// `None` as "no tenancy configured for this caller", and the request ran
/// against the DEFAULT namespace — the shared one — on a valid signature alone.
/// A claim outside `[A-Za-z0-9_-]` was worse than that: the registry dropped
/// the offending characters, so `acme.corp` reached `acmecorp`'s namespace.
///
/// `None` here means "this deployment does not derive tenancy from a claim",
/// which is a different fact from "this token has no tenant" and is the only
/// case that may legitimately route to the default context.
fn resolve_claim_tenant(
    verifier: &OidcVerifier,
    claims: &Claims,
) -> Result<Option<String>, ApiError> {
    if !verifier.has_tenant_claim() {
        return Ok(None);
    }
    let Some(claimed) = verifier.extract_tenant(claims) else {
        // Missing, blank, or not a JSON string — `extract_tenant` folds all
        // three into `None`, and all three mean the same thing here.
        metrics::counter!("siglake_query_tenant_denied_total", "reason" => "claim_missing")
            .increment(1);
        return Err(ApiError::forbidden(
            "this query server derives the tenant from a JWT claim, and the token carries none",
        ));
    };
    let Some(tenant) = crate::tenants::validate(&claimed) else {
        // Deliberately does not echo the claim back: it is attacker-controlled
        // text that would land in a client's logs and error surfaces.
        metrics::counter!("siglake_query_tenant_denied_total", "reason" => "claim_invalid")
            .increment(1);
        return Err(ApiError::forbidden(format!(
            "tenant claim is unusable: {}",
            siglake_core::tenant::TENANT_ID_RULE
        )));
    };
    Ok(Some(tenant))
}

fn extract_bearer<'a>(_ext: &Extensions, headers: &'a axum::http::HeaderMap) -> Option<&'a str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_token_iterator_collapses_to_open() {
        let cfg = AuthConfig::from_tokens(Vec::<String>::new());
        assert!(cfg.is_open());
    }

    #[test]
    fn whitespace_and_empty_entries_are_filtered() {
        let cfg = AuthConfig::from_tokens(["", "  ", "tok1", " tok2 "]);
        match &cfg {
            AuthConfig::Tokens(set) => {
                assert!(set.contains("tok1"));
                assert!(set.contains("tok2"));
                assert!(!set.contains(""));
                assert_eq!(set.len(), 2);
            }
            other => panic!("expected Tokens, got {}", other.label()),
        }
    }

    #[test]
    fn labels() {
        assert_eq!(AuthConfig::open().label(), "open");
        assert_eq!(AuthConfig::from_tokens(["a"]).label(), "bearer");
    }
}
