//! Inert history foundations. No routes, startup tasks, audit cutover or runtime DDL.
pub mod redaction;
mod store;
pub use store::{HistoryReader, HistoryWriter, StoredExecution, WriteResult};

use crate::auth::CallerIdentity;
use chrono::{DateTime, Utc};
use redaction::RedactedSql;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HistoryError {
    #[error("invalid history value")]
    InvalidValue,
    #[error("history store unavailable")]
    Unavailable,
    #[error("history privilege contract rejected")]
    Privilege,
    #[error("history observation conflict")]
    Conflict,
    #[error("history observation expired or absent")]
    ExpiredOrAbsent,
}

macro_rules! fixed_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
        pub enum $name { $(#[serde(rename = $value)] $variant),+ }
        impl $name { pub(crate) fn parse(value: &str) -> Result<Self, HistoryError> { match value { $($value => Ok(Self::$variant)),+, _ => Err(HistoryError::Unavailable) } } }
    };
}
fixed_enum!(Operation { Sql => "sql", Explain => "explain", Shard => "shard" });
fixed_enum!(OutputFormat { Records => "records", Ndjson => "ndjson", Csv => "csv", Arrow => "arrow", Parquet => "parquet" });
fixed_enum!(Priority { Interactive => "interactive", Batch => "batch" });
fixed_enum!(PrincipalKind { Provider => "provider", Unknown => "unknown", Internal => "internal" });
fixed_enum!(CacheState { Hit => "hit", Miss => "miss", Bypass => "bypass", Unknown => "unknown" });
fixed_enum!(Outcome { Success => "success", Rejected => "rejected", Failed => "failed", TimedOut => "timed_out", Cancelled => "cancelled", Disconnected => "disconnected" });
/// Evidence from the trusted backend lifecycle, never an HTTP/client flag.
/// Unknown admission cannot construct a terminal event; retain uncertainty.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionEvidence {
    Admitted,
    NotAdmitted,
    Unknown,
}
fixed_enum!(ObservationState { Pending => "pending", Terminal => "terminal", Indeterminate => "indeterminate" });
fixed_enum!(ErrorCode { InvalidSql => "invalid_sql", Validation => "validation", Planning => "planning", Admission => "admission", Execution => "execution", Timeout => "timeout", Cancelled => "cancelled", Disconnected => "disconnected" });

/// Fields are deliberately private; never copy subject/email/token into history.
#[derive(Clone, Serialize)]
pub struct Principal {
    tenant: String,
    issuer: Option<String>,
    client_id: Option<String>,
    principal_kind: PrincipalKind,
}
impl Principal {
    /// Call only after authentication and resolution of the effective tenant.
    pub fn from_caller(tenant: &str, caller: &CallerIdentity) -> Result<Self, HistoryError> {
        let tenant =
            siglake_core::tenant::sanitize_tenant(tenant).ok_or(HistoryError::InvalidValue)?;
        if caller.tenant.as_ref().is_some_and(|claim| claim != &tenant) {
            return Err(HistoryError::InvalidValue);
        }
        let (issuer, client_id, principal_kind) = if caller.coordinator {
            (None, None, PrincipalKind::Internal)
        } else if let Some(provider) = &caller.provider_principal {
            if provider.issuer.is_empty()
                || provider.issuer.len() > 2048
                || provider.client_id.is_empty()
                || provider.client_id.len() > 128
                || provider.issuer.contains('\0')
                || provider.client_id.contains('\0')
            {
                return Err(HistoryError::InvalidValue);
            }
            (
                Some(provider.issuer.clone()),
                Some(provider.client_id.clone()),
                PrincipalKind::Provider,
            )
        } else {
            (None, None, PrincipalKind::Unknown)
        };
        Ok(Self {
            tenant,
            issuer,
            client_id,
            principal_kind,
        })
    }
}

#[derive(Clone, Serialize)]
pub struct RequestMetadata {
    pub operation: Operation,
    pub format: OutputFormat,
    pub priority: Priority,
    pub dry_run: bool,
}

/// Issued by Postgres, not deserializable. Keep this exact receipt across retries.
#[derive(Clone, Serialize)]
pub struct Receipt {
    execution_id: Uuid,
    received_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}
impl Receipt {
    pub fn execution_id(&self) -> Uuid {
        self.execution_id
    }
    pub fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
}

#[derive(Clone, Serialize)]
pub struct PendingExecution {
    #[serde(flatten)]
    receipt: Receipt,
    #[serde(flatten)]
    principal: Principal,
    owner_uuid: Uuid,
    #[serde(flatten)]
    request: RequestMetadata,
    redaction: RedactedSql,
}
impl PendingExecution {
    pub fn new(
        receipt: Receipt,
        principal: Principal,
        owner_uuid: Uuid,
        request: RequestMetadata,
        sql: RedactedSql,
    ) -> Result<Self, HistoryError> {
        if owner_uuid.is_nil() {
            return Err(HistoryError::InvalidValue);
        }
        let event = Self {
            receipt,
            principal,
            owner_uuid,
            request,
            redaction: sql,
        };
        if serde_json::to_vec(&event)
            .map_err(|_| HistoryError::InvalidValue)?
            .len()
            > 8192
        {
            return Err(HistoryError::InvalidValue);
        }
        Ok(event)
    }
    pub fn receipt(&self) -> &Receipt {
        &self.receipt
    }
}

/// Counts and elapsed time are nullable, never fabricated from estimates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Counter(i64);
impl Counter {
    pub fn new(value: u64) -> Result<Self, HistoryError> {
        i64::try_from(value)
            .map(Self)
            .map_err(|_| HistoryError::InvalidValue)
    }
}
#[derive(Clone, Default, Serialize)]
pub struct Counters {
    pub estimated_bytes_scanned: Option<Counter>,
    pub estimated_rows_processed: Option<Counter>,
    pub measured_bytes_scanned: Option<Counter>,
    pub measured_rows_scanned: Option<Counter>,
    pub returned_rows: Option<Counter>,
}
/// Measured values stay separate from estimates and persistence timestamps.
#[derive(Clone, Serialize)]
pub struct ExecutionMeasurements {
    #[serde(flatten)]
    pub counters: Counters,
    pub duration_ms: Option<Counter>,
    pub cache_state: CacheState,
    pub truncated: Option<bool>,
    pub job_id: Option<Uuid>,
}
/// Immutable terminal evidence. No arbitrary error strings/labels or Deserialize.
#[derive(Clone, Serialize)]
pub struct TerminalExecution {
    admission_evidence: AdmissionEvidence,
    outcome: Outcome,
    error_code: Option<ErrorCode>,
    #[serde(flatten)]
    measurements: ExecutionMeasurements,
}
impl TerminalExecution {
    pub fn new(
        admission_evidence: AdmissionEvidence,
        outcome: Outcome,
        error_code: Option<ErrorCode>,
        measurements: ExecutionMeasurements,
    ) -> Result<Self, HistoryError> {
        if admission_evidence == AdmissionEvidence::Unknown
            || (outcome == Outcome::Success && admission_evidence != AdmissionEvidence::Admitted)
            || (outcome == Outcome::Rejected
                && admission_evidence != AdmissionEvidence::NotAdmitted)
            || measurements.job_id.is_some_and(|id| id.is_nil())
            || (outcome != Outcome::Success && measurements.truncated == Some(true))
        {
            return Err(HistoryError::InvalidValue);
        }
        let valid = match outcome {
            Outcome::Success => error_code.is_none(),
            Outcome::Rejected => matches!(
                error_code,
                Some(
                    ErrorCode::InvalidSql
                        | ErrorCode::Validation
                        | ErrorCode::Planning
                        | ErrorCode::Admission
                )
            ),
            Outcome::Failed => error_code == Some(ErrorCode::Execution),
            Outcome::TimedOut => error_code == Some(ErrorCode::Timeout),
            Outcome::Cancelled => error_code == Some(ErrorCode::Cancelled),
            Outcome::Disconnected => error_code == Some(ErrorCode::Disconnected),
        };
        if !valid {
            return Err(HistoryError::InvalidValue);
        }
        Ok(Self {
            admission_evidence,
            outcome,
            error_code,
            measurements,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dto_rejects_bad_bounds_and_does_not_copy_caller_secrets() {
        let caller = CallerIdentity {
            subject: "SECRET_TOKEN".into(),
            email: Some("SECRET_EMAIL".into()),
            ..Default::default()
        };
        let principal = Principal::from_caller("tenant-a", &caller).unwrap();
        assert!(!serde_json::to_string(&principal)
            .unwrap()
            .contains("SECRET"));
        assert!(Principal::from_caller("active", &caller).is_err());
        assert!(Principal::from_caller("bad.tenant", &caller).is_err());
        assert!(Counter::new(u64::MAX).is_err());
        assert!(Counter::new(i64::MAX as u64).is_ok());
        let mut attributed = caller.clone();
        attributed.provider_principal = Some(crate::oidc::VerifiedProviderPrincipal {
            issuer: "i".repeat(2048),
            client_id: "c".repeat(128),
        });
        assert!(Principal::from_caller("tenant-a", &attributed).is_ok());
        attributed
            .provider_principal
            .as_mut()
            .unwrap()
            .issuer
            .push('x');
        assert!(Principal::from_caller("tenant-a", &attributed).is_err());
        attributed.provider_principal.as_mut().unwrap().issuer = "https://issuer.invalid".into();
        attributed
            .provider_principal
            .as_mut()
            .unwrap()
            .client_id
            .push('x');
        assert!(Principal::from_caller("tenant-a", &attributed).is_err());
        attributed.tenant = Some("tenant-b".into());
        assert!(Principal::from_caller("tenant-a", &attributed).is_err());
        let measurements = ExecutionMeasurements {
            counters: Counters::default(),
            duration_ms: None,
            cache_state: CacheState::Unknown,
            truncated: Some(true),
            job_id: None,
        };
        assert!(TerminalExecution::new(
            AdmissionEvidence::Admitted,
            Outcome::Failed,
            Some(ErrorCode::Execution),
            measurements.clone()
        )
        .is_err());
        assert!(TerminalExecution::new(
            AdmissionEvidence::NotAdmitted,
            Outcome::Success,
            None,
            measurements.clone()
        )
        .is_err());
        assert!(TerminalExecution::new(
            AdmissionEvidence::Unknown,
            Outcome::Success,
            None,
            measurements
        )
        .is_err());
        for error in [
            HistoryError::InvalidValue,
            HistoryError::Unavailable,
            HistoryError::Privilege,
            HistoryError::Conflict,
            HistoryError::ExpiredOrAbsent,
        ] {
            assert!(!format!("{error:?}: {error}").contains("SECRET"));
        }
    }
}

#[cfg(test)]
mod postgres_tests;
