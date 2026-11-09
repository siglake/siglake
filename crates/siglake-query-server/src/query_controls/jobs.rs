//! Private stable job binding is never part of the public explanatory snapshot.
//! These strict versioned values are store inputs, not HTTP-supplied authority.
use super::{
    runtime::{ControlsAuthority, RequestControls},
    PolicyError, PolicyLookup, RuntimeKey,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

fn required_option<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(d)
}

/// Immutable authority ingredients only: retaining this must not retain the
/// task registry through an Arc<ControlsAuthority> self-cycle.
pub(crate) struct DequeueAuthority {
    reader: std::sync::Arc<super::PolicyReader>,
    ceiling: Duration,
    generation: i64,
}
impl ControlsAuthority {
    pub(crate) fn dequeue_authority(&self) -> DequeueAuthority {
        DequeueAuthority {
            reader: self.reader.clone(),
            ceiling: self.batch.ceiling_timeout,
            generation: self.generation,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobSnapshot {
    pub version: u8,
    #[serde(deserialize_with = "required_option")]
    pub policy_revision: Option<i64>,
    pub policy_schema_version: u8,
    pub mandatory_timeout: bool,
    pub operator_generation: i64,
    pub effective_timeout_ms: i64,
    #[serde(deserialize_with = "required_option")]
    pub scan_bytes_warning: Option<i64>,
    pub timeout_opt_out_ignored: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobControls {
    pub accepted_at: DateTime<Utc>,
    pub original_expires_at: DateTime<Utc>,
    pub accepted: JobSnapshot,
    #[serde(deserialize_with = "required_option")]
    pub execution: Option<JobSnapshot>,
}
impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JobContext([redacted])")
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JobContext {
    version: u8,
    pub(crate) owner: String,
    #[serde(deserialize_with = "required_option")]
    binding: Option<Binding>,
    pub(crate) controls: JobControls,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    tenant: String,
    issuer: String,
    client: String,
}
impl JobSnapshot {
    pub(crate) fn validate(&self) -> Result<(), PolicyError> {
        if self.version != 1
            || self.policy_schema_version != 1
            || !self.mandatory_timeout
            || self.policy_revision.is_some_and(|r| r <= 0)
            || self.operator_generation <= 0
            || self.effective_timeout_ms < 0
            || self.scan_bytes_warning.is_some_and(|n| n <= 0)
        {
            return Err(PolicyError::Invalid);
        }
        Ok(())
    }
}
impl JobContext {
    pub(crate) fn new(
        owner: &str,
        identity: &crate::auth::CallerIdentity,
        c: RequestControls,
    ) -> Result<Self, PolicyError> {
        let key = super::runtime::runtime_key(identity)?;
        let accepted_at = Utc::now();
        let ms = i64::try_from(c.timeout.as_millis()).map_err(|_| PolicyError::Invalid)?;
        let original_expires_at = accepted_at
            .checked_add_signed(chrono::Duration::try_milliseconds(ms).ok_or(PolicyError::Invalid)?)
            .ok_or(PolicyError::Invalid)?;
        let accepted = JobSnapshot {
            version: 1,
            policy_revision: c.revision,
            policy_schema_version: 1,
            mandatory_timeout: true,
            operator_generation: c.generation,
            effective_timeout_ms: ms,
            scan_bytes_warning: c.scan_warning,
            timeout_opt_out_ignored: c.timeout_opt_out_ignored,
        };
        let context = Self {
            version: 1,
            owner: owner.into(),
            binding: key.map(|k| Binding {
                tenant: k.tenant,
                issuer: k.issuer,
                client: k.client,
            }),
            controls: JobControls {
                accepted_at,
                original_expires_at,
                accepted,
                execution: None,
            },
        };
        context.validate(owner)?;
        Ok(context)
    }
    pub(crate) fn accept_now(&mut self) -> Result<(), PolicyError> {
        self.controls.accepted_at = Utc::now();
        self.controls.original_expires_at = self
            .controls
            .accepted_at
            .checked_add_signed(
                chrono::Duration::try_milliseconds(self.controls.accepted.effective_timeout_ms)
                    .ok_or(PolicyError::Invalid)?,
            )
            .ok_or(PolicyError::Invalid)?;
        Ok(())
    }
    pub(crate) fn validate(&self, owner: &str) -> Result<(), PolicyError> {
        self.controls.accepted.validate()?;
        if self.version != 1
            || self.owner != owner
            || owner.is_empty()
            || owner.len() > 512
            || owner.chars().any(char::is_control)
            || self.controls.execution.is_some()
            || self.controls.accepted_at.checked_add_signed(
                chrono::Duration::try_milliseconds(self.controls.accepted.effective_timeout_ms)
                    .ok_or(PolicyError::Invalid)?,
            ) != Some(self.controls.original_expires_at)
        {
            return Err(PolicyError::Invalid);
        }
        if let Some(b) = &self.binding {
            RuntimeKey::from_trusted_metadata(&b.tenant, &b.issuer, &b.client)?;
        }
        Ok(())
    }
    pub(crate) fn deadline(
        &self,
        anchor: tokio::time::Instant,
    ) -> Result<tokio::time::Instant, PolicyError> {
        let budget = Duration::from_millis(
            u64::try_from(self.controls.accepted.effective_timeout_ms)
                .map_err(|_| PolicyError::Invalid)?,
        );
        let absolute_remaining = (self.controls.original_expires_at - Utc::now())
            .to_std()
            .unwrap_or_default();
        Ok(anchor.checked_add(budget).ok_or(PolicyError::Invalid)?.min(
            tokio::time::Instant::now()
                .checked_add(absolute_remaining)
                .ok_or(PolicyError::Invalid)?,
        ))
    }
    /// The read is primary/uncached and charged to the original lifetime by caller.
    pub(crate) async fn revalidate(
        &self,
        authority: &DequeueAuthority,
    ) -> Result<(JobSnapshot, Duration), PolicyError> {
        self.validate(&self.owner)?;
        let policy = if let Some(b) = &self.binding {
            let key = RuntimeKey::from_trusted_metadata(&b.tenant, &b.issuer, &b.client)?;
            authority.reader.lookup(&key).await?
        } else {
            authority.reader.preflight().await?;
            PolicyLookup::Absent
        };
        let accepted = &self.controls.accepted;
        let policy_timeout = match policy {
            PolicyLookup::Found(p) => p.settings().timeout(),
            PolicyLookup::Absent => None,
        };
        let timeout = Duration::from_millis(accepted.effective_timeout_ms as u64)
            .min(authority.ceiling)
            .min(policy_timeout.unwrap_or(authority.ceiling));
        let snapshot = JobSnapshot {
            version: 1,
            policy_revision: policy.revision(),
            policy_schema_version: 1,
            mandatory_timeout: true,
            operator_generation: authority.generation,
            effective_timeout_ms: i64::try_from(timeout.as_millis())
                .map_err(|_| PolicyError::Invalid)?,
            scan_bytes_warning: match policy {
                PolicyLookup::Found(p) => p.settings().scan_bytes_warning(),
                PolicyLookup::Absent => None,
            },
            timeout_opt_out_ignored: accepted.timeout_opt_out_ignored,
        };
        Ok((snapshot, timeout))
    }
    pub(crate) fn execution_controls(
        &self,
        snapshot: &JobSnapshot,
        deadline: tokio::time::Instant,
        started: Instant,
    ) -> RequestControls {
        RequestControls {
            priority: crate::Priority::Batch,
            started,
            deadline,
            timeout: Duration::from_millis(snapshot.effective_timeout_ms as u64),
            state: super::PolicyState::Inherited,
            revision: snapshot.policy_revision,
            generation: snapshot.operator_generation,
            scan_warning: snapshot.scan_bytes_warning,
            timeout_opt_out_ignored: snapshot.timeout_opt_out_ignored,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn job_context_strict_version_binding_bounds_expiry_and_public_projection() {
        let c = super::super::runtime::test_controls(Duration::from_secs(1));
        let identity = crate::auth::CallerIdentity {
            tenant: Some("acme".into()),
            provider_principal: Some(crate::oidc::VerifiedProviderPrincipal {
                issuer: "https://private.invalid/".into(),
                client_id: "private-client".into(),
            }),
            ..Default::default()
        };
        let context = JobContext::new("unique-owner", &identity, c).unwrap();
        let public = serde_json::to_string(&context.controls).unwrap();
        for private in [
            "acme",
            "private.invalid",
            "private-client",
            "unique-owner",
            "issuer",
            "tenant",
            "client",
            "sub",
        ] {
            assert!(!public.contains(private));
        }
        assert!(public.len() < 4096);
        let raw = serde_json::to_value(&context).unwrap();
        for field in ["version", "owner", "controls", "binding"] {
            let mut v = raw.clone();
            v.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<JobContext>(v).is_err());
        }
        for version in [0, 2] {
            let mut v = raw.clone();
            v["version"] = version.into();
            assert!(serde_json::from_value::<JobContext>(v)
                .unwrap()
                .validate("unique-owner")
                .is_err());
        }
        let mut v = raw.clone();
        v["extra"] = true.into();
        assert!(serde_json::from_value::<JobContext>(v).is_err());
        let mut v = raw.clone();
        v["controls"]["accepted"]["effective_timeout_ms"] = (-1).into();
        assert!(serde_json::from_value::<JobContext>(v)
            .unwrap()
            .validate("unique-owner")
            .is_err());
        assert!(context.validate("foreign-owner").is_err());
        let mut broken = context.clone();
        broken.controls.original_expires_at += chrono::Duration::milliseconds(1);
        assert!(broken.validate("unique-owner").is_err());
        let mut broken = context.clone();
        broken.binding.as_mut().unwrap().tenant = "bad.tenant".into();
        assert!(broken.validate("unique-owner").is_err());
    }
    #[tokio::test]
    async fn job_deadline_never_restarts_on_late_conversion() {
        let c = super::super::runtime::test_controls(Duration::from_millis(20));
        let context = JobContext::new("owner", &crate::auth::CallerIdentity::default(), c).unwrap();
        let anchor = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(context.deadline(anchor).unwrap() <= tokio::time::Instant::now());
    }
}
