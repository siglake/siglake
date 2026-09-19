//! Per-request tenant routing.
//!
//! When the OIDC verifier is configured with a tenant claim, each
//! caller's verified JWT carries a tenant identifier that the
//! handlers use to scope the query to that tenant's Iceberg
//! namespace. This module owns the cache of per-tenant
//! [`IcebergContext`]s (one process serves many tenants without
//! reopening the catalog connection for every request).
//!
//! Namespace mapping: the tenant claim is VALIDATED against
//! [`siglake_core::tenant::sanitize_tenant`] — the same rule ingest applies to
//! `X-Scope-OrgID` — then prefixed by a configurable `namespace_prefix`
//! (default `tenant_`) and used verbatim as the Iceberg namespace name.
//!
//! IT USED TO SANITIZE INSTEAD, dropping every character outside
//! `[A-Za-z0-9_-]`, and that is a tenancy hole rather than a cosmetic one:
//!
//!   - `acme.corp` and `acmecorp` both resolved to `tenant_acmecorp`, so one
//!     tenant read the other's data by holding a token for a claim that merely
//!     *looked* different.
//!   - a claim made entirely of dropped characters sanitized to the empty
//!     string, which routed to the DEFAULT namespace — the shared one — rather
//!     than to anything belonging to the caller.
//!
//! Both are now refused. A tenant claim is either usable as given or the
//! request does not run; nothing here repairs one.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::RwLock;

use siglake_storage::iceberg::IcebergContext;

/// Default prefix prepended to the tenant claim to derive the Iceberg
/// namespace name. Picked to avoid colliding with the reserved `siglake`
/// namespace.
pub const DEFAULT_NAMESPACE_PREFIX: &str = "tenant_";

/// Per-process registry of tenant-scoped [`IcebergContext`]s.
///
/// Cheap to clone — wraps an [`Arc`] over the actual cache. The
/// `default` context is what callers without a tenant claim (or
/// from non-OIDC auth modes) route to.
#[derive(Clone)]
pub struct TenantRegistry {
    default: Arc<IcebergContext>,
    namespace_prefix: String,
    cache: Arc<RwLock<HashMap<String, Arc<IcebergContext>>>>,
}

impl TenantRegistry {
    pub fn new(default: Arc<IcebergContext>) -> Self {
        Self::with_prefix(default, DEFAULT_NAMESPACE_PREFIX.to_string())
    }

    pub fn with_prefix(default: Arc<IcebergContext>, namespace_prefix: String) -> Self {
        Self {
            default,
            namespace_prefix,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn default_ice(&self) -> Arc<IcebergContext> {
        self.default.clone()
    }

    /// Number of retained tenant contexts. Public only for integration tests
    /// that prove admission refusals happen before registry resolution.
    #[doc(hidden)]
    pub async fn cached_tenant_count(&self) -> usize {
        self.cache.read().await.len()
    }

    /// Resolve the per-tenant context for `tenant`, opening the
    /// namespace lazily on first use.
    ///
    /// Errors when the identifier is not a usable tenant id, and when the
    /// namespace cannot be opened (catalog unreachable, etc.). The first case
    /// should be unreachable — [`crate::auth::middleware`] refuses an unusable
    /// claim with a `403` before routing, and `/api/v1/sql/shard` refuses an
    /// unusable forwarded tenant with a `400` — so it is a backstop, not the
    /// enforcement point. It is here so that no future caller can reintroduce
    /// the fallback by handing this an identifier it did not check.
    pub async fn resolve(&self, tenant: &str) -> Result<Arc<IcebergContext>> {
        let tenant = validate(tenant).ok_or_else(|| {
            anyhow!(
                "unusable tenant identifier ({})",
                siglake_core::tenant::TENANT_ID_RULE
            )
        })?;
        // Fast path: cache hit.
        {
            let guard = self.cache.read().await;
            if let Some(ctx) = guard.get(&tenant) {
                return Ok(ctx.clone());
            }
        }
        // Slow path: open the namespace once and insert. We hold the
        // write lock for the full open so a concurrent request for
        // the same tenant waits on us rather than racing the catalog.
        let mut guard = self.cache.write().await;
        if let Some(ctx) = guard.get(&tenant) {
            return Ok(ctx.clone());
        }
        let namespace = format!("{}{}", self.namespace_prefix, tenant);
        let ctx = self
            .default
            .for_namespace(&namespace)
            .await
            .with_context(|| format!("open tenant namespace `{namespace}`"))?;
        let arc = Arc::new(ctx);
        guard.insert(tenant, arc.clone());
        Ok(arc)
    }
}

/// Is `tenant` usable as an Iceberg namespace suffix, a WAL path component and
/// a metric label value? Returns the trimmed identifier, or `None`.
///
/// One line so the rule cannot drift from ingest's: both read
/// [`siglake_core::tenant::sanitize_tenant`].
pub fn validate(tenant: &str) -> Option<String> {
    siglake_core::tenant::sanitize_tenant(tenant)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The identifier collision the old sanitizer permitted: `acme.corp` no
    /// longer becomes `acmecorp`, it becomes a refusal.
    #[test]
    fn collision_and_traversal_are_refused_not_rewritten() {
        assert_eq!(validate("acme").as_deref(), Some("acme"));
        assert_eq!(validate("Acme_42-Inc").as_deref(), Some("Acme_42-Inc"));
        assert_eq!(validate("acme.corp"), None);
        assert_eq!(validate("acme/../escape"), None);
        assert_eq!(validate("../../etc/passwd"), None);
    }

    /// An empty (or all-invalid) claim used to route to the DEFAULT namespace.
    #[test]
    fn empty_no_longer_means_default() {
        assert_eq!(validate(""), None);
        assert_eq!(validate("   "), None);
        assert_eq!(validate("...."), None);
    }

    #[test]
    fn overlong_is_refused() {
        assert!(validate(&"a".repeat(128)).is_some());
        assert_eq!(validate(&"a".repeat(129)), None);
    }
}
