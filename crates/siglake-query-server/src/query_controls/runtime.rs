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
    pub(crate) distributed: Option<super::wire::DistributedControlsConfig>,
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
            distributed: None,
            interactive: limits.interactive.clone(),
            batch: limits.batch.clone(),
            generation,
            bodies: super::body::BodyTasks::default(),
            jobs: super::job_tasks::JobTasks::default(),
        })
    }
    #[cfg(test)]
    pub(super) fn owned_body_counts(&self) -> (usize, usize, usize) {
        self.bodies.owned_counts()
    }
    /// Close job/body generations and observe all retained joins. Cancelling
    /// this wait preserves handles; await it again before service teardown.
    pub async fn shutdown(&self) {
        self.jobs.shutdown().await;
        self.bodies.shutdown().await;
    }
    pub(super) fn tier(&self, priority: Priority) -> &TierLimits {
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
    static INGRESS_UNIX_MS: Option<i64>;
    static CONTROLS: RequestControls;
    // Only actual local durable selection may transfer the active lifetime.
    // The snapshot/run budget above is immutable; this is an ingress-anchored
    // handoff deadline, not a new admission or a second policy lookup.
    static LOCAL_HANDOFF: std::cell::Cell<Option<tokio::time::Instant>>;
    static BODY_HISTORY: std::cell::RefCell<Option<crate::history::streaming::DeliveryObservation>>;
}
pub(crate) async fn ingress<F: Future>(started: Instant, future: F) -> F::Output {
    // Wall sampled before elapsed subtraction: conversion errs earlier, never
    // later. Stable relative clocks are an explicit distributed prerequisite.
    let wall = std::time::SystemTime::now();
    let unix_ms = wall
        .checked_sub(started.elapsed())
        .and_then(super::wire::unix_ms_floor);
    INGRESS_UNIX_MS
        .scope(unix_ms, INGRESS.scope(started, future))
        .await
}
pub(crate) fn current() -> Option<RequestControls> {
    CONTROLS
        .try_with(|c| {
            let mut c = *c;
            if let Ok(Some(deadline)) = LOCAL_HANDOFF.try_with(std::cell::Cell::get) {
                c.deadline = deadline;
            }
            c
        })
        .ok()
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

/// Called only at the explicit router's proven local-durable branches, before
/// local preparation/admission. Classification still spends REQUEST time. A
/// late-ready selection cannot erase expiry, and the handoff uses ORIGINAL
/// ingress plus the existing operator budget, never the short run budget.
pub(crate) fn select_local_durable(authority: Option<&ControlsAuthority>) -> Result<(), ApiError> {
    let Some(authority) = authority else {
        return Ok(());
    };
    transfer_local_durable(authority.batch.default_timeout)
}
fn transfer_local_durable(handoff_budget: Duration) -> Result<(), ApiError> {
    let controls = current().ok_or_else(unavailable)?;
    if tokio::time::Instant::now() >= controls.deadline {
        return Err(expired());
    }
    let deadline = tokio::time::Instant::from_std(
        controls
            .started
            .checked_add(handoff_budget)
            .ok_or_else(unavailable)?,
    );
    LOCAL_HANDOFF
        .try_with(|phase| {
            if phase.get().is_some() {
                return Err(unavailable());
            }
            // Actual selection is this synchronous phase transfer. Check BOTH
            // original request and handoff immediately before changing phase.
            let selected_at = tokio::time::Instant::now();
            if selected_at >= controls.deadline || selected_at >= deadline {
                return Err(expired());
            }
            phase.set(Some(deadline));
            Ok(())
        })
        .map_err(|_| unavailable())?
}

/// Poll a single owned continuation under the currently selected deadline.
/// Unlike a fixed outer timeout, every enclosing phase observes the transfer
/// during the SAME poll. Checks on both sides refuse late native/ready work;
/// the timer still independently wakes a pending continuation.
async fn until_selected<F, T>(future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    use std::task::Poll;
    let mut future = Box::pin(future);
    let Some(controls) = current() else {
        return future.await;
    };
    let mut timer = Box::pin(tokio::time::sleep_until(controls.deadline));
    std::future::poll_fn(|cx| {
        if tokio::time::Instant::now() >= current().unwrap().deadline {
            return Poll::Ready(Err(expired()));
        }
        let result = future.as_mut().poll(cx);
        let deadline = current().unwrap().deadline;
        if tokio::time::Instant::now() >= deadline {
            return Poll::Ready(Err(expired()));
        }
        if result.is_ready() {
            return result;
        }
        timer.as_mut().reset(deadline);
        if timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(expired()))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// Scope includes observational history waits and fully buffered response
/// rendering and independently owned body production. Native CPU work remains
/// cooperative, not OS-preempted. Fanout requires explicit wirev2 configuration.
pub(crate) fn run<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    run_inner(
        authority,
        identity,
        caller,
        priority,
        false,
        NativeResponse::Sql,
        future,
    )
}
pub(crate) fn run_handoff<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    run_inner(
        authority,
        identity,
        caller,
        priority,
        true,
        NativeResponse::Sql,
        future,
    )
}
/// Jaeger keeps native JSON, with fixed typed response-only metadata.
pub(crate) fn run_jaeger<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    run_inner(
        authority,
        identity,
        RequestLimits::default(),
        Priority::Interactive,
        false,
        NativeResponse::Jaeger,
        future,
    )
}

/// Direct external v1 shards resolve their own identity. Controlled coordinator
/// forwarding uses the distinct authenticated v2 route, never permissive v1.
pub(crate) fn run_shard<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    priority: Priority,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    let unsupported_coordinator = authority.is_some() && identity.coordinator;
    let future = run_inner(
        authority,
        identity,
        RequestLimits::default(),
        priority,
        false,
        NativeResponse::Shard,
        future,
    );
    async move {
        if unsupported_coordinator {
            return Err(unsupported());
        }
        future.await
    }
}

#[derive(Clone, Copy)]
pub(super) enum NativeResponse {
    Sql,
    Jaeger,
    Shard,
}
impl NativeResponse {
    fn timeout(
        self,
        error: ApiError,
        budget: Duration,
        started: Instant,
        priority: Priority,
    ) -> ApiError {
        if matches!(self, Self::Shard)
            && error.status == axum::http::StatusCode::GATEWAY_TIMEOUT
            && error.msg == "query deadline exceeded"
            && error.context.is_none()
        {
            let mut native = crate::sql::shard_timeout_error(budget, priority);
            native.headers = error.headers;
            native
        } else {
            jaeger_timeout(error, matches!(self, Self::Jaeger), budget, started)
        }
    }
}

fn run_inner<F>(
    authority: Option<Arc<ControlsAuthority>>,
    identity: CallerIdentity,
    caller: RequestLimits,
    priority: Priority,
    handoff: bool,
    native: NativeResponse,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    // The legacy/disabled branch must not retain a second inline copy of a
    // large native work future either. Box BEFORE constructing this frame.
    let future = Box::pin(future);
    async move {
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
        .await
        .map_err(|error| native.timeout(error, initial_timeout, started, priority))?;
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
        let dispatch = match authority.distributed {
            Some(config) if !handoff && tokio::time::Instant::now() < controls.deadline => {
                Some(super::wire::Dispatch::admitted(
                    config,
                    &identity,
                    controls,
                    priority,
                    INGRESS_UNIX_MS
                        .try_with(|n| *n)
                        .ok()
                        .flatten()
                        .ok_or_else(unavailable)?,
                )?)
            }
            _ => None,
        };
        super::wire::dispatch_scope(
            dispatch,
            execute(authority, controls, native, priority, future),
        )
        .await
    }
}

pub(super) fn execute<F>(
    authority: Arc<ControlsAuthority>,
    controls: RequestControls,
    native: NativeResponse,
    priority: Priority,
    future: F,
) -> impl Future<Output = Result<Response, ApiError>>
where
    F: Future<Output = Result<Response, ApiError>>,
{
    // Box before constructing the shared body/dispatch wrappers. Jaeger and
    // selected SQL futures are large; copying them through another async frame
    // can exhaust a native executor stack even before its first await.
    let future = Box::pin(future);
    async move {
        let timeout = controls.timeout;
        let started = controls.started;
        let headers = match native {
            NativeResponse::Sql => None,
            NativeResponse::Jaeger => Some(jaeger_headers(controls)?),
            NativeResponse::Shard => Some(unestimated_headers(controls, priority)?),
        };
        if tokio::time::Instant::now() >= controls.deadline {
            return Err(with_headers(
                native.timeout(expired(), timeout, started, priority),
                &headers,
            ));
        }
        let body_slot = authority
            .bodies
            .reserve()
            .map_err(|e| with_headers(e, &headers))?;
        let (result, history, controls) = LOCAL_HANDOFF
            .scope(
                std::cell::Cell::new(None),
                BODY_HISTORY.scope(
                    std::cell::RefCell::new(None),
                    CONTROLS.scope(controls, async {
                        let result = until_selected(future).await;
                        (result, BODY_HISTORY.with(|h| h.take()), current().unwrap())
                    }),
                ),
            )
            .await;
        let result = match result {
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
        };
        match headers {
            Some(headers) => match result {
                Ok(mut response) => {
                    for (name, value) in headers {
                        response.headers_mut().insert(name, value);
                    }
                    Ok(response)
                }
                Err(error) => {
                    let mut error = native.timeout(error, timeout, started, priority);
                    error.headers.extend(headers);
                    Err(error)
                }
            },
            None => result,
        }
    }
}
/// Pre-header Jaeger wire compatibility only. Keep the original anchor and
/// initial/resolved budget, and reuse TraceRequest's existing unestimated cost
/// stand-in, not a new estimate. SQL/jobs, policy failures and already-native
/// errors retain their existing construction. Sent bodies are never adapted.
fn jaeger_timeout(
    mut error: ApiError,
    jaeger: bool,
    timeout: Duration,
    started: Instant,
) -> ApiError {
    if jaeger
        && error.status == axum::http::StatusCode::GATEWAY_TIMEOUT
        && error.msg == "query deadline exceeded"
        && error.context.is_none()
    {
        let native = ApiError::timeout(
            &crate::cost::CostReport::unknown_after_timeout(),
            started.elapsed().as_secs_f64(),
            timeout.as_secs(),
        );
        error.msg = native.msg;
        error.context = native.context;
    }
    error
}
fn with_headers(
    mut error: ApiError,
    headers: &Option<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>>,
) -> ApiError {
    if let Some(headers) = headers {
        error.headers.extend(headers.iter().cloned());
    }
    error
}
fn jaeger_headers(
    c: RequestControls,
) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, ApiError> {
    unestimated_headers(c, Priority::Interactive)
}
fn unestimated_headers(
    c: RequestControls,
    priority: Priority,
) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, ApiError> {
    use super::dto::*;
    let invalid = || ApiError::service_unavailable("query_controls_metadata_unavailable");
    let mut warnings = WarningSet::default();
    if c.timeout_opt_out_ignored {
        warnings.insert(WarningCode::TimeoutOptOutIgnored);
    }
    warnings.insert(WarningCode::ScanEstimateUnavailable);
    if c.scan_warning.is_some() {
        warnings.insert(WarningCode::ScanThresholdNotEvaluated);
    }
    let dto = EffectiveQueryControls::new(ControlsInput {
        state: c.state,
        policy_revision: c.revision,
        operator_generation: c.generation,
        priority: match priority {
            Priority::Interactive => ControlPriority::Interactive,
            Priority::Batch => ControlPriority::Batch,
        },
        mode: SelectedMode::Request,
        scope: ControlScope::Request,
        mandatory_timeout: true,
        effective_timeout_ms: Some(i64::try_from(c.timeout.as_millis()).map_err(|_| invalid())?),
        effective_output_rows: None,
        output_cap_enforced: false,
        scan_bytes_warning: c.scan_warning,
        estimate_status: EstimateStatus::Unavailable,
        estimate_exact: None,
        estimated_bytes: None,
        warnings,
    })
    .map_err(|_| invalid())?;
    dto.json().map_err(|_| invalid())?;
    let encoded = dto.headers().map_err(|_| invalid())?;
    let mut headers = vec![(
        axum::http::HeaderName::from_static("x-siglake-query-controls"),
        axum::http::HeaderValue::from_str(&encoded.controls).map_err(|_| invalid())?,
    )];
    if let Some(value) = encoded.warnings {
        headers.push((
            axum::http::HeaderName::from_static("x-siglake-query-warnings"),
            axum::http::HeaderValue::from_str(&value).map_err(|_| invalid())?,
        ));
    }
    Ok(headers)
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
            Some(_) => until_selected(future).await,
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
