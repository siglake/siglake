//! Management-only edit ledger. Constructors validate shape, NOT authentication.
use super::{bounded, PolicyError, SavedSettings, VerifiedHumanActor};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

pub const MAX_AUDIT_PAGE_LIMIT: u16 = 100;
pub const AUDIT_RETENTION_HOURS: i64 = 720;

/// The next Console server supplies this only after current human session and
/// same-org DB authorization. Neither a role name, GUC nor subject proves that.
/// Not deserializable; never construct from a browser/agent bearer body.
pub struct AuthorizedHumanManagementContext {
    pub(super) org: Uuid,
    _actor: VerifiedHumanActor,
}
impl std::fmt::Debug for AuthorizedHumanManagementContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthorizedHumanManagementContext([redacted])")
    }
}
impl AuthorizedHumanManagementContext {
    pub fn from_trusted_metadata(
        org: Uuid,
        actor: VerifiedHumanActor,
    ) -> Result<Self, PolicyError> {
        if org.is_nil() {
            return Err(PolicyError::Invalid);
        }
        Ok(Self { org, _actor: actor })
    }
}

/// Descending exclusive revision cursor, scoped by the separately authorized
/// binding on EVERY page. No caller clock/cutoff, offset or arbitrary SQL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuditPageRequest {
    pub(super) limit: u16,
    pub(super) before_revision: Option<i64>,
}
impl AuditPageRequest {
    pub fn new(limit: u16, before_revision: Option<i64>) -> Result<Self, PolicyError> {
        if !(1..=MAX_AUDIT_PAGE_LIMIT).contains(&limit)
            || before_revision.is_some_and(|revision| revision <= 0)
        {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            limit,
            before_revision,
        })
    }
}

/// Only original policy edit facts, never Activity/query usage or free text.
/// Actor is a verified human subject, not an email/display name. Treat the
/// entire page as private/no-store and do not log it (Debug is redacted).
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct PolicyEdit {
    pub schema_version: i16,
    pub revision: i64,
    pub actor_sub: String,
    pub edited_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub old_settings: SavedSettings,
    pub new_settings: SavedSettings,
}
impl std::fmt::Debug for PolicyEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PolicyEdit([redacted])")
    }
}
impl PolicyEdit {
    pub(super) fn validate(&self) -> Result<(), PolicyError> {
        if self.schema_version != 1
            || self.revision <= 0
            || !bounded(&self.actor_sub, 256)
            || self.expires_at.signed_duration_since(self.edited_at)
                != chrono::Duration::hours(AUDIT_RETENTION_HOURS)
        {
            return Err(PolicyError::Unavailable);
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct AuditPage {
    pub edits: Vec<PolicyEdit>,
    /// Present for a full page; the next page may be empty (including expiry).
    /// New writes are above this cursor and cannot shift/duplicate later pages.
    pub next_before_revision: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audit_page_limits_cursor_and_human_context_are_checked() {
        for limit in [0, 101, u16::MAX] {
            assert_eq!(
                AuditPageRequest::new(limit, None),
                Err(PolicyError::Invalid)
            );
        }
        for cursor in [Some(0), Some(-1)] {
            assert_eq!(AuditPageRequest::new(1, cursor), Err(PolicyError::Invalid));
        }
        assert!(AuditPageRequest::new(100, Some(i64::MAX)).is_ok());
        assert!(AuthorizedHumanManagementContext::from_trusted_metadata(
            Uuid::nil(),
            VerifiedHumanActor::from_trusted_metadata("human").unwrap()
        )
        .is_err());
        let c = AuthorizedHumanManagementContext::from_trusted_metadata(
            Uuid::new_v4(),
            VerifiedHumanActor::from_trusted_metadata("private-subject").unwrap(),
        )
        .unwrap();
        assert!(!format!("{c:?}").contains("private-subject"));
    }
}
