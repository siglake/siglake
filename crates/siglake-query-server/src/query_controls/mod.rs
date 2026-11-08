//! Policy store and injection-only buffered request authority. No env/main
//! activation or public enforcement metadata. Supplied metadata constructors
//! are NOT authentication proof; runtime derives keys from verified identity.
mod config;
mod dto;
mod owned;
mod role;
pub(crate) mod runtime;
mod store;
pub use config::{ConnectionBounds, PolicyConfig};
pub use dto::*;
pub use runtime::ControlsAuthority;
pub use store::{PolicyManager, PolicyReader};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("invalid explicit policy configuration")]
    Configuration,
    #[error("invalid policy input")]
    Invalid,
    #[error("policy privilege or schema contract rejected")]
    Unsafe,
    #[error("policy unavailable; save outcome may be unknown")]
    Unavailable,
    #[error("policy revision conflict")]
    Conflict,
    #[error("policy binding rejected")]
    Binding,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    Reader,
    Manager,
}
impl Role {
    fn name(self) -> &'static str {
        match self {
            Self::Reader => "qp_reader",
            Self::Manager => "qp_manager",
        }
    }
}

/// Exact validated tenant + verified issuer/client. Never deserialize principal
/// authority from request JSON/headers, sub, JWT bytes, or history records.
/// Future runtime callers must derive this from verified CallerIdentity.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeKey {
    tenant: String,
    issuer: String,
    client: String,
}
impl std::fmt::Debug for RuntimeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RuntimeKey([redacted])")
    }
}
fn bounded(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
impl RuntimeKey {
    pub fn from_trusted_metadata(
        tenant: &str,
        verified_issuer: &str,
        verified_client: &str,
    ) -> Result<Self, PolicyError> {
        if !bounded(tenant, 128)
            || !tenant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            || ["active", "sealed", "processing", "committed", "consumers"]
                .contains(&tenant.to_ascii_lowercase().as_str())
            || !bounded(verified_issuer, 2048)
            || !bounded(verified_client, 128)
        {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            tenant: tenant.into(),
            issuer: verified_issuer.into(),
            client: verified_client.into(),
        })
    }
}
/// Management metadata from an already authorized exact org-owned ready credential.
/// Org/session/Origin/ready-state authorization is deliberately not implemented here.
#[derive(Clone)]
pub struct ManagementBinding {
    key: RuntimeKey,
    org: Uuid,
    credential: Uuid,
}
impl std::fmt::Debug for ManagementBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManagementBinding([redacted])")
    }
}
impl ManagementBinding {
    pub fn from_trusted_metadata(
        key: RuntimeKey,
        org: Uuid,
        credential: Uuid,
    ) -> Result<Self, PolicyError> {
        if org.is_nil() || credential.is_nil() {
            return Err(PolicyError::Invalid);
        }
        Ok(Self {
            key,
            org,
            credential,
        })
    }
    pub fn runtime_key(&self) -> &RuntimeKey {
        &self.key
    }
}
/// Verified human actor supplied by a future session-authorized server, never a
/// browser field. The 256-byte cap is an audit-storage bound, not identity proof.
pub struct VerifiedHumanActor(String);
impl std::fmt::Debug for VerifiedHumanActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VerifiedHumanActor([redacted])")
    }
}
impl VerifiedHumanActor {
    pub fn from_trusted_metadata(subject: &str) -> Result<Self, PolicyError> {
        if !bounded(subject, 256) {
            return Err(PolicyError::Invalid);
        }
        Ok(Self(subject.into()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SavedPolicy {
    revision: i64,
    settings: SavedSettings,
}
impl SavedPolicy {
    pub fn revision(self) -> i64 {
        self.revision
    }
    pub fn settings(self) -> SavedSettings {
        self.settings
    }
}
/// Successful primary lookups distinguish explicit inherit rows from absence.
/// Errors NEVER establish inheritance or a known revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyLookup {
    Found(SavedPolicy),
    Absent,
}
impl PolicyLookup {
    /// Configuration presence, NOT effective enforcement: warning-only is applied;
    /// an all-NULL explicit-inherit row is inherited with its persisted revision.
    pub fn state(self) -> PolicyState {
        match self {
            Self::Found(p)
                if p.settings.timeout_seconds().is_some()
                    || p.settings.scan_bytes_warning().is_some() =>
            {
                PolicyState::Applied
            }
            _ => PolicyState::Inherited,
        }
    }
    pub fn revision(self) -> Option<i64> {
        match self {
            Self::Found(p) => Some(p.revision),
            Self::Absent => None,
        }
    }
}

#[cfg(test)]
mod postgres_tests;
#[cfg(test)]
mod tests;
