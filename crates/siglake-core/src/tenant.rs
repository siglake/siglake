//! The one definition of what a tenant identifier may be.
//!
//! A tenant id becomes a WAL subdirectory (`<wal>/<tenant>/`), an Iceberg
//! namespace (`tenant_<id>`) and a metric label value, so it is restricted to a
//! safe identifier — ASCII alphanumerics plus `-`/`_`, 1..=[`MAX_TENANT_ID_LEN`]
//! chars, and not one of the reserved WAL layout directory names.
//!
//! Both boundaries that accept a tenant from outside read this: ingest's
//! `X-Scope-OrgID` / JWT-claim resolution and the query server's JWT-claim
//! resolution. It lives here rather than in either crate because the two must
//! agree on the answer — a value one accepts and the other rewrites is a
//! namespace the writer and the reader disagree about.
//!
//! VALIDATING, NOT SANITIZING. The rejected value is refused, never repaired:
//! dropping the offending characters maps distinct identifiers onto one
//! namespace (`acme.corp` and `acmecorp` both landing on `tenant_acmecorp`) and
//! maps an all-invalid identifier onto the empty string, which reads as "no
//! tenant" and routes to the default. Either is a cross-tenant read.

/// Longest tenant id accepted. Bounds the WAL path component, the Iceberg
/// namespace name and the metric label value in one number.
pub const MAX_TENANT_ID_LEN: usize = 128;

/// Validate a client-supplied tenant id, returning the trimmed value when it is
/// usable and `None` when it is not.
///
/// The name is historical (it was a sanitizer once); it validates. See the
/// module docs for why it must stay that way.
pub fn sanitize_tenant(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.is_empty()
        || t.len() > MAX_TENANT_ID_LEN
        || !t
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return None;
    }
    // A tenant id becomes a DIRECTORY NAME under the WAL root, and the WAL
    // layout owns five of them. `list_layout_dirs` skips child directories with
    // those names so the legacy flat layout is not mistaken for tenants — so
    // `X-Scope-OrgID: active` used to return 200, create `<wal>/active/`, write
    // segments to `<wal>/active/sealed/*.arrow`, and then never be enumerated
    // by the drain's tenant walk or by `catch_up_sweep`. Accepted, durable,
    // permanently unqueryable, zero errors.
    //
    // The sibling validator for INDEX ids already enforced exactly this list.
    if crate::index_config::RESERVED_WAL_LAYOUT_DIRS
        .iter()
        .any(|r| r.eq_ignore_ascii_case(t))
    {
        return None;
    }
    Some(t.to_string())
}

/// Human-readable statement of the rule, for the message an API refusal
/// carries. Both boundaries word their refusal the same way.
pub const TENANT_ID_RULE: &str = "use [A-Za-z0-9_-], 1..=128 chars";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_well_formed_ids_and_trims() {
        assert_eq!(sanitize_tenant("acme").as_deref(), Some("acme"));
        assert_eq!(sanitize_tenant("  acme  ").as_deref(), Some("acme"));
        assert_eq!(
            sanitize_tenant("Acme_42-Inc").as_deref(),
            Some("Acme_42-Inc")
        );
    }

    /// The collision the old query-side sanitizer created: two distinct claims
    /// mapping to one namespace. Both are refused now, so neither can be the
    /// other.
    #[test]
    fn refuses_rather_than_repairs() {
        assert_eq!(sanitize_tenant("acme.corp"), None);
        assert_eq!(sanitize_tenant("acmecorp").as_deref(), Some("acmecorp"));
        assert_eq!(sanitize_tenant("acme/../escape"), None);
        assert_eq!(sanitize_tenant("../../etc/passwd"), None);
    }

    #[test]
    fn refuses_empty_blank_and_overlong() {
        assert_eq!(sanitize_tenant(""), None);
        assert_eq!(sanitize_tenant("   "), None);
        assert!(sanitize_tenant(&"a".repeat(MAX_TENANT_ID_LEN)).is_some());
        assert_eq!(sanitize_tenant(&"a".repeat(MAX_TENANT_ID_LEN + 1)), None);
    }

    #[test]
    fn refuses_reserved_wal_layout_dirs() {
        for dir in crate::index_config::RESERVED_WAL_LAYOUT_DIRS {
            assert_eq!(sanitize_tenant(dir), None, "{dir} must not be a tenant id");
            assert_eq!(
                sanitize_tenant(&dir.to_uppercase()),
                None,
                "{dir} must not be a tenant id in any casing"
            );
        }
    }
}
