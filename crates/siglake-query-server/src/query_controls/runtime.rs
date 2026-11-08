//! Injection-only request and durable-job authority. No env/main activation.
//! Unsupported lifetimes refuse before unprotected work.
use super::{PolicyError, PolicyLookup, PolicyReader, PolicyState, RuntimeKey};
use crate::{auth::CallerIdentity, error::ApiError, limits::*, ServerLimits};
use axum::response::Response;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

/// A read-only authority with validated, immutable operator timeout settings.
/// Configure AppState limits BEFORE injecting this authority. Neither request
/// preferences nor history/cache metadata can construct or replace it.
pub struct ControlsAuthority {
    pub(super) reader: Arc<PolicyReader>,
    interactive: TierLimits,
    pub(super) batch: TierLimits,
    pub(super) generation: i64,
    bodies: super::body::BodyTasks,
    pub(crate) jobs: super::job_tasks::JobTasks,
}
impl ControlsAuthority {
    pub(crate) fn new(
        reader: Arc<PolicyReader>,
        limits: &ServerLimits,
        generation: i64,
    ) -> Result<Self, PolicyError> {
        validate_tier(&limits.interactive)?;
        validate_tier(&limits.batch)?;
        if generation <= 0 {
            return Err(PolicyError::Configuration);
        }
        Ok(Self {
            reader,
            interactive: limits.interactive.clone(),
            batch: limits.batch.clone(),
            generation,
            bodies: super::body::BodyTasks::default(),
            jobs: super::job_tasks::JobTasks::default(),
        })
    }
    /// Close job/body generations and observe all retained joins. Cancelling
    /// this wait preserves handles; await it again before service teardown.
    pub async fn shutdown(&self) {
        self.jobs.shutdown().await;
        self.bodies.shutdown().await;
    }
    fn tier(&self, priority: Priority) -> &TierLimits {
        match priority {
            Priority::Interactive => &self.interactive,
            Priority::Batch => &self.batch,
        }
    }
}
fn validate_tier(tier: &TierLimits) -> Result<(), PolicyError> {
    for duration in [tier.default_timeout, tier.ceiling_timeout] {
        if duration.as_millis() == 0
            || duration.as_millis() > i64::MAX as u128
            || Instant::now().checked_add(duration).is_none()
        {
            return Err(PolicyError::Configuration);
        }
    }
    if tier.default_timeout > tier.ceiling_timeout {
        return Err(PolicyError::Configuration);
    }
    Ok(())
}
fn timeout(tier: &TierLimits, caller: &RequestLimits, policy: PolicyLookup) -> Duration {
    let policy_timeout = match policy {
        PolicyLookup::Found(p) => p
            .settings()
            .timeout_seconds()
            .map(|s| Duration::from_secs(s as u64)),
        PolicyLookup::Absent => None,
    };
    tier.ceiling_timeout
        .min(policy_timeout.unwrap_or(tier.ceiling_timeout))
        .min(
            caller
                .timeout_seconds
                .map(Duration::from_secs)
                .unwrap_or(tier.default_timeout),
        )
}

/// One admitted snapshot; private fields and no Deserialize. The monotonic
/// anchor is original ingress, not completion of policy/history/classification.
#[derive(Clone, Copy)]
pub(crate) struct RequestControls {
    pub(crate) started: Instant,
    pub(crate) deadline: tokio::time::Instant,
    pub(super) timeout: Duration,
    pub(super) state: PolicyState,
    pub(super) revision: Option<i64>,
    pub(super) generation: i64,
    pub(super) scan_warning: Option<i64>,
    pub(super) timeout_opt_out_ignored: bool,
}
tokio::task_local! {
    static INGRESS: Instant;
    static CONTROLS: RequestControls;
    static BODY_HISTORY: std::cell::RefCell<Option<crate::history::streaming::DeliveryObservation>>;
}
pub(crate) async fn ingress<F: Future>(started: Instant, future: F) -> F::Output {
    INGRESS.scope(started, future).await
}
pub(crate) fn current() -> Option<RequestControls> {
    CONTROLS.try_with(|c| *c).ok()
}
/// Transfer observation delivery ownership without making history an authority.
pub(crate) fn defer_history(observation: &mut crate::history::streaming::StreamObservation) {
    let _ = BODY_HISTORY.try_with(|slot| *slot.borrow_mut() = Some(observation.defer_delivery()));
}
/// Checked from synchronous Arrow/serde sinks as well as async row boundaries.
/// A single native conversion/escape step is NOT preempted or size bounded.
pub(crate) fn render_check() -> std::io::Result<()> {
    super::body::check_cancel()?;
    if current().is_some_and(|c| tokio::time::Instant::now() >= c.deadline) {
        Err(std::io::Error::other("query deadline exceeded"))
    } else {
        Ok(())
    }
}
fn unavailable() -> ApiError {
    ApiError::service_unavailable("query_policy_unavailable")
}
pub(crate) fn unsupported() -> ApiError {
    ApiError::service_unavailable("query_controls_path_unavailable")
}
pub(crate) fn expired() -> ApiError {
    ApiError {
        status: axum::http::StatusCode::GATEWAY_TIMEOUT,
        msg: "query deadline exceeded".into(),
        context: None,
        headers: vec![],
    }
}
async fn until<F, T>(deadline: tokio::time::Instant, future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(expired());
    }
    let result = tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| expired())?;
    // A ready future (including synchronous parser/JSON serialization) can run
    // beyond its budget before Tokio polls its timer. Never deliver late success.
    if tokio::time::Instant::now() >= deadline {
        return Err(expired());
    }
    result
}

/// Scope includes observational history waits and fully buffered response
/// rendering and independently owned body production. Native CPU work remains
/// cooperative, not OS-preempted. Fanout/shards/Jaeger remain gated.
pub(crate) async fn run<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    future: F,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    run_inner(authority, identity, caller, priority, false, future).await
}
pub(crate) async fn run_handoff<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    future: F,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    run_inner(authority, identity, caller, priority, true, future).await
}
async fn run_inner<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    handoff: bool,
    future: F,
) -> Result<Response, ApiError>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    let Some(authority) = authority else {
        return future.await;
    };
    // A missing authenticated ingress scope is an infrastructure failure, not
    // permission to mint a fresh clock from supplied identity metadata.
    let started = INGRESS.try_with(|s| *s).map_err(|_| unavailable())?;
    let tier = authority.tier(priority);
    let initial_timeout = if handoff {
        tier.default_timeout
    } else {
        timeout(tier, &caller, PolicyLookup::Absent)
    };
    let initial_deadline = tokio::time::Instant::from_std(
        started
            .checked_add(initial_timeout)
            .ok_or_else(unavailable)?,
    );
    let policy = until(
        initial_deadline,
        Box::pin(async {
            match runtime_key(&identity).map_err(|_| unavailable())? {
                Some(key) => authority
                    .reader
                    .lookup(&key)
                    .await
                    .map_err(|_| unavailable()),
                None => {
                    authority
                        .reader
                        .preflight()
                        .await
                        .map_err(|_| unavailable())?;
                    Ok(PolicyLookup::Absent)
                }
            }
        }),
    )
    .await?;
    let timeout = timeout(tier, &caller, policy);
    let controls = RequestControls {
        started,
        deadline: tokio::time::Instant::from_std(
            started
                .checked_add(if handoff {
                    tier.default_timeout
                } else {
                    timeout
                })
                .ok_or_else(unavailable)?,
        ),
        timeout,
        state: policy.state(),
        revision: policy.revision(),
        generation: authority.generation,
        scan_warning: match policy {
            PolicyLookup::Found(p) => p.settings().scan_bytes_warning(),
            PolicyLookup::Absent => None,
        },
        timeout_opt_out_ignored: caller.circuit_breakers == Some(false),
    };
    if tokio::time::Instant::now() >= controls.deadline {
        return Err(expired());
    }
    let body_slot = authority.bodies.reserve()?;
    let (result, history) = BODY_HISTORY
        .scope(std::cell::RefCell::new(None), async {
            let result = CONTROLS
                .scope(controls, until(controls.deadline, future))
                .await;
            (result, BODY_HISTORY.with(|h| h.take()))
        })
        .await;
    match result {
        Ok(response) => body_slot.start(response, controls, history),
        Err(error) => {
            if let Some(h) = history {
                if tokio::time::Instant::now() >= controls.deadline {
                    h.timed_out();
                } else {
                    h.failed();
                }
            }
            Err(error)
        }
    }
}
pub(super) fn runtime_key(identity: &CallerIdentity) -> Result<Option<RuntimeKey>, PolicyError> {
    if identity.coordinator {
        return Err(PolicyError::Invalid);
    }
    match (&identity.tenant, &identity.provider_principal) {
        (Some(tenant), Some(principal)) => {
            RuntimeKey::from_trusted_metadata(tenant, &principal.issuer, &principal.client_id)
                .map(Some)
        }
        // Generic/static/open callers inherit bounded operator controls, without
        // a named revision. sub/audience/body/header hints never become a key.
        (_, None) => Ok(None),
        (None, Some(_)) => Err(PolicyError::Invalid),
    }
}
/// Check before the history observer considers a selected work result complete.
pub(crate) fn phase<F, T>(future: F) -> impl Future<Output = Result<T, ApiError>>
where
    F: Future<Output = Result<T, ApiError>>,
{
    // Box BEFORE constructing the async wrapper: selected SQL futures are
    // large, and embedding them through nested timer enums copies them on the
    // executor stack. This does not change stack limits or query budgets.
    let future = Box::pin(future);
    async move {
        match current() {
            Some(c) => until(c.deadline, future).await,
            None => future.await,
        }
    }
}
pub(crate) fn resolve(
    caller: &RequestLimits,
    priority: Priority,
    tier: &TierLimits,
) -> ResolvedLimits {
    let mut limits = ResolvedLimits::resolve(caller, priority, tier);
    if let Some(c) = current() {
        limits.timeout = c.timeout;
    }
    limits
}
/// Append only fixed/numeric trusted control values. The existing key retains
/// tenant/snapshot/schema/query scoping. No controls or correlation headers are
/// stored in a cached payload, and every hit/wait has already read primary.
pub(crate) fn cache_discriminator(limits: &ResolvedLimits) -> Option<String> {
    current().map(|c| {
        format!(
            "controls=1,{:?},{:?},{},{},{},{},{},{},{},{},{}",
            c.state,
            c.revision,
            c.generation,
            c.timeout.as_nanos(),
            c.scan_warning.unwrap_or(0),
            u8::from(c.timeout_opt_out_ignored),
            limits.priority.label(),
            limits.max_rows_returned,
            limits.max_bytes_scanned,
            limits.max_rows_scanned,
            u8::from(limits.circuit_breakers)
        )
    })
}

#[cfg(test)]
mod tests;

pub(crate) async fn body_scope<F: Future>(controls: RequestControls, future: F) -> F::Output {
    CONTROLS.scope(controls, future).await
}

#[cfg(test)]
pub(crate) fn test_controls(budget: Duration) -> RequestControls {
    RequestControls {
        started: Instant::now(),
        deadline: tokio::time::Instant::now() + budget,
        timeout: budget,
        state: PolicyState::Inherited,
        revision: None,
        generation: 1,
        scan_warning: None,
        timeout_opt_out_ignored: false,
    }
}
