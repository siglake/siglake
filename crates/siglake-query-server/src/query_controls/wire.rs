//! Coordinator-authenticated authority, NOT JWT provenance. Injection only.
//! Clock contract: during a request, relative wall-clock error is bounded by
//! the explicitly configured S and both clocks remain stable relative to their
//! monotonic clocks. This is an operator assumption, not a measured guarantee.
use super::{runtime::*, PolicyError, PolicyState, RuntimeKey};
use crate::{auth::CallerIdentity, error::ApiError, limits::Priority};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(crate) const PATH: &str = "/api/v2/internal/sql/shard";
const MAX_CONTEXT_BYTES: usize = 4096;

/// Explicit maximum relative clock error. No production default or activation.
#[derive(Clone, Copy)]
pub struct DistributedControlsConfig {
    skew_ms: i64,
}
impl DistributedControlsConfig {
    pub fn new(max_relative_clock_skew: Duration) -> Result<Self, PolicyError> {
        let n = max_relative_clock_skew.as_millis();
        if n > i64::MAX as u128 || Duration::from_millis(n as u64) != max_relative_clock_skew {
            return Err(PolicyError::Configuration);
        }
        Ok(Self { skew_ms: n as i64 })
    }
}
fn required_nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::deserialize(d)
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
enum Version {
    V2,
}
impl TryFrom<u8> for Version {
    type Error = &'static str;
    fn try_from(n: u8) -> Result<Self, Self::Error> {
        if n == 2 {
            Ok(Self::V2)
        } else {
            Err("unsupported controls version")
        }
    }
}
impl From<Version> for u8 {
    fn from(_: Version) -> Self {
        2
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "snake_case")]
enum Binding {
    Operator {
        #[serde(deserialize_with = "required_nullable")]
        tenant: Option<String>,
    },
    Provider {
        tenant: String,
        issuer: String,
        client: String,
    },
}
impl Binding {
    fn verified(identity: &CallerIdentity) -> Result<Self, ApiError> {
        match runtime_key(identity).map_err(|_| unsupported())? {
            Some(key) => Ok(Self::Provider {
                tenant: key.tenant,
                issuer: key.issuer,
                client: key.client,
            }),
            None => Ok(Self::Operator {
                tenant: identity.tenant.clone(),
            }),
        }
    }
    fn validate(&self) -> Result<(), ApiError> {
        if let Self::Provider {
            tenant,
            issuer,
            client,
        } = self
        {
            RuntimeKey::from_trusted_metadata(tenant, issuer, client).map_err(|_| unsupported())?;
        }
        if self
            .tenant()
            .is_some_and(|t| crate::tenants::validate(t).is_none())
        {
            return Err(unsupported());
        }
        Ok(())
    }
    fn tenant(&self) -> Option<&str> {
        match self {
            Self::Operator { tenant } => tenant.as_deref(),
            Self::Provider { tenant, .. } => Some(tenant),
        }
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Applied,
    Inherited,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Tier {
    Interactive,
    Batch,
}
impl From<Priority> for Tier {
    fn from(p: Priority) -> Self {
        match p {
            Priority::Interactive => Self::Interactive,
            Priority::Batch => Self::Batch,
        }
    }
}
/// Private strict bounded wire. Never attach this to public metadata or Debug.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Context {
    version: Version,
    schema: u8,
    #[serde(deserialize_with = "required_nullable")]
    revision: Option<i64>,
    state: State,
    generation: i64,
    priority: Tier,
    timeout_ms: i64,
    #[serde(deserialize_with = "required_nullable")]
    scan_warning: Option<i64>,
    timeout_opt_out_ignored: bool,
    binding: Binding,
    original_expires_unix_ms: i64,
    remaining_ms: i64,
    clock_skew_ms: i64,
}
#[derive(Clone)]
pub(crate) struct Dispatch {
    context: Context,
    deadline: tokio::time::Instant,
}
tokio::task_local! { static DISPATCH: Option<Dispatch>; }
impl Dispatch {
    pub(super) fn admitted(
        config: DistributedControlsConfig,
        identity: &CallerIdentity,
        c: RequestControls,
        priority: Priority,
        ingress_unix_ms: i64,
    ) -> Result<Self, ApiError> {
        let timeout_ms = i64::try_from(c.timeout.as_millis()).map_err(|_| unsupported())?;
        let context = Context {
            version: Version::V2,
            schema: 1,
            revision: c.revision,
            state: match c.state {
                PolicyState::Applied => State::Applied,
                PolicyState::Inherited => State::Inherited,
                _ => return Err(unsupported()),
            },
            generation: c.generation,
            priority: priority.into(),
            timeout_ms,
            scan_warning: c.scan_warning,
            timeout_opt_out_ignored: c.timeout_opt_out_ignored,
            binding: Binding::verified(identity)?,
            original_expires_unix_ms: ingress_unix_ms
                .checked_add(timeout_ms)
                .ok_or_else(unsupported)?,
            remaining_ms: timeout_ms,
            clock_skew_ms: config.skew_ms,
        };
        context.validate(config)?;
        Ok(Self {
            context,
            deadline: c.deadline,
        })
    }
    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }
    pub(crate) fn context(&self) -> Result<Context, ApiError> {
        let mut c = self.context.clone();
        // Floor, never round up; every dispatch/retry uses the SAME anchor.
        c.remaining_ms = i64::try_from(
            self.deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis(),
        )
        .map_err(|_| unsupported())?;
        if c.remaining_ms <= 0 {
            return Err(expired());
        }
        Ok(c)
    }
}
pub(crate) fn current_dispatch() -> Option<Dispatch> {
    DISPATCH.try_with(Clone::clone).ok().flatten()
}
pub(super) async fn dispatch_scope<F: Future>(dispatch: Option<Dispatch>, future: F) -> F::Output {
    DISPATCH.scope(dispatch, future).await
}
impl Context {
    fn validate(&self, config: DistributedControlsConfig) -> Result<(), ApiError> {
        if self.schema != 1
            || self.generation <= 0
            || self.timeout_ms <= 0
            || self.remaining_ms <= 0
            || self.remaining_ms > self.timeout_ms
            || self.original_expires_unix_ms <= 0
            || self.clock_skew_ms != config.skew_ms
            || self.revision.is_some_and(|n| n <= 0)
            || self.scan_warning.is_some_and(|n| n <= 0)
            || (matches!(self.state, State::Applied) && self.revision.is_none())
            || serde_json::to_vec(self).map_err(|_| unsupported())?.len() > MAX_CONTEXT_BYTES
        {
            return Err(unsupported());
        }
        if matches!(self.binding, Binding::Operator { .. })
            && (self.revision.is_some()
                || matches!(self.state, State::Applied)
                || self.scan_warning.is_some())
        {
            return Err(unsupported());
        }
        self.binding.validate()
    }
    pub(crate) fn tenant(&self) -> Option<String> {
        self.binding.tenant().map(str::to_owned)
    }
    pub(crate) fn priority(&self) -> Priority {
        match self.priority {
            Tier::Interactive => Priority::Interactive,
            Tier::Batch => Priority::Batch,
        }
    }
}
pub(super) fn unix_ms_floor(time: SystemTime) -> Option<i64> {
    i64::try_from(time.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()
}
fn unix_ms_ceil(time: SystemTime) -> Option<i64> {
    let nanos = time.duration_since(UNIX_EPOCH).ok()?.as_nanos();
    i64::try_from(nanos.checked_add(999_999)? / 1_000_000).ok()
}
/// Let E be coordinator expiry, W worker wall time, and |W-C| <= S.
/// E-(W+S) <= E-C, including a worker behind by S. Transit has already
/// advanced W. Taking min with floored remaining and local ceiling cannot
/// extend lifetime. Checked subtraction/ceil refuses uncertainty/overflow.
fn remaining_ms(
    expiry: i64,
    worker_now: i64,
    skew: i64,
    sent_remaining: i64,
    ceiling: i64,
) -> Option<i64> {
    if expiry <= 0 || worker_now < 0 || skew < 0 || sent_remaining <= 0 || ceiling <= 0 {
        return None;
    }
    let n = expiry
        .checked_sub(worker_now.checked_add(skew)?)?
        .min(sent_remaining)
        .min(ceiling);
    (n > 0).then_some(n)
}
pub(crate) async fn run_forwarded<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: &CallerIdentity,
    context: &Context,
    future: F,
) -> Result<axum::response::Response, ApiError>
where
    F: Future<Output = Result<axum::response::Response, ApiError>>,
{
    if !identity.coordinator {
        return Err(ApiError::forbidden("coordinator authentication required"));
    }
    let authority = authority.ok_or_else(unsupported)?;
    let config = authority.distributed.ok_or_else(unsupported)?;
    context.validate(config)?;
    // Sample wall AFTER monotonic: any sampling overhead shortens the budget.
    let started = std::time::Instant::now();
    let wall = unix_ms_ceil(SystemTime::now()).ok_or_else(unsupported)?;
    let ceiling = i64::try_from(
        authority
            .tier(context.priority())
            .ceiling_timeout
            .as_millis(),
    )
    .map_err(|_| unsupported())?;
    let remaining = remaining_ms(
        context.original_expires_unix_ms,
        wall,
        config.skew_ms,
        context.remaining_ms,
        ceiling,
    )
    .ok_or_else(expired)?;
    let budget = Duration::from_millis(remaining as u64);
    let controls = RequestControls {
        started,
        deadline: tokio::time::Instant::from_std(
            started.checked_add(budget).ok_or_else(unsupported)?,
        ),
        timeout: Duration::from_millis(context.timeout_ms.min(ceiling) as u64),
        state: match context.state {
            State::Applied => PolicyState::Applied,
            State::Inherited => PolicyState::Inherited,
        },
        revision: context.revision,
        generation: authority.generation,
        scan_warning: context.scan_warning,
        timeout_opt_out_ignored: context.timeout_opt_out_ignored,
    };
    // Preserve role/schema readiness checks, NOT a replacement policy lookup.
    execute(
        authority.clone(),
        controls,
        NativeResponse::Shard,
        context.priority(),
        async {
            phase(async {
                authority
                    .reader
                    .preflight()
                    .await
                    .map_err(|_| ApiError::service_unavailable("query_policy_unavailable"))
            })
            .await?;
            future.await
        },
    )
    .await
}

#[cfg(test)]
mod owned_tests;
#[cfg(test)]
mod tests;
