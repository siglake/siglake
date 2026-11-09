//! Caller-only diagnostic: no principal selector, catalog scan, or future-overlay preview.
use super::{runtime, PolicyLookup, PolicyState};
use crate::{
    auth::CallerIdentity,
    limits::{Priority, RequestLimits},
    ApiError, AppState,
};
use axum::{
    extract::{RawQuery, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Availability {
    Enabled,
    Disabled,
    Unavailable,
}
#[derive(Serialize)]
struct OperatorTier {
    default_timeout_ms: i64,
    ceiling_timeout_ms: i64,
}
#[derive(Serialize)]
struct TierSummary {
    operator: OperatorTier,
    state: PolicyState,
    policy_revision: Option<i64>,
    default_request_timeout_ms: i64,
    scan_bytes_warning: Option<i64>,
}
#[derive(Serialize)]
struct Summary {
    version: u8,
    availability: Availability,
    // Fixed scope: values apply only to defaults on this node/configuration;
    // caller overlays, worker clamps and future policy edits are not previewed.
    scope: Scope,
    operator_generation: Option<i64>,
    interactive: Option<TierSummary>,
    batch: Option<TierSummary>,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Scope {
    LocalNodeDefaultRequests,
}
fn encode(body: Summary, status: StatusCode) -> Result<Response, ApiError> {
    let bytes = serde_json::to_vec(&body).map_err(|_| super::presentation::invalid())?;
    if bytes.len() > 4096 {
        return Err(super::presentation::invalid());
    }
    Ok((
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "private, no-store"),
        ],
        bytes,
    )
        .into_response())
}
#[utoipa::path(get, path="/api/v1/query-controls/effective", tag="sql", responses(
    (status=200, description="This verified caller's local default-request controls for both priorities; disabled is explicit."),
    (status=400, description="Query parameters are not supported.", body=crate::openapi_dto::ApiErrorBody),
    (status=401, description="Missing or invalid bearer credential.", body=crate::openapi_dto::ApiErrorBody),
    (status=403, description="The verified tenant claim is missing, unusable or refused by this server.", body=crate::openapi_dto::ApiErrorBody),
    (status=503, description="Configured policy authority unavailable."),
    (status=504, description="Original operator budget expired.")
))]
pub(crate) async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    RawQuery(query): RawQuery,
) -> Response {
    let mut response = resolve_defaults(state, identity, query)
        .await
        .unwrap_or_else(IntoResponse::into_response);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("private, no-store"),
    );
    response
}
async fn resolve_defaults(
    state: AppState,
    identity: CallerIdentity,
    query: Option<String>,
) -> Result<Response, ApiError> {
    if query.is_some() {
        return Err(ApiError::bad_request("query parameters are not supported"));
    }
    let mut summary = Summary {
        version: 1,
        availability: Availability::Disabled,
        scope: Scope::LocalNodeDefaultRequests,
        operator_generation: None,
        interactive: None,
        batch: None,
    };
    let Some(authority) = &state.query_controls else {
        return encode(summary, StatusCode::OK);
    };
    if !authority.is_admitting() {
        summary.availability = Availability::Unavailable;
        return encode(summary, StatusCode::SERVICE_UNAVAILABLE);
    }
    let started = runtime::ingress_started()?;
    let budget = authority
        .tier(Priority::Interactive)
        .default_timeout
        .min(authority.tier(Priority::Batch).default_timeout);
    let deadline = tokio::time::Instant::from_std(
        started
            .checked_add(budget)
            .ok_or_else(super::presentation::invalid)?,
    );
    let policy = runtime::until(deadline, async {
        match runtime::runtime_key(&identity)
            .map_err(|_| ApiError::service_unavailable("query_policy_unavailable"))?
        {
            Some(key) => authority
                .reader
                .lookup(&key)
                .await
                .map_err(|_| ApiError::service_unavailable("query_policy_unavailable")),
            None => {
                authority
                    .reader
                    .preflight()
                    .await
                    .map_err(|_| ApiError::service_unavailable("query_policy_unavailable"))?;
                Ok(PolicyLookup::Absent)
            }
        }
    })
    .await;
    let policy = match policy {
        Ok(policy) => policy,
        Err(error) => {
            summary.availability = Availability::Unavailable;
            return encode(summary, error.status);
        }
    };
    let tier = |priority| -> Result<TierSummary, ApiError> {
        let t = authority.tier(priority);
        let ms = |d: std::time::Duration| {
            i64::try_from(d.as_millis()).map_err(|_| super::presentation::invalid())
        };
        Ok(TierSummary {
            operator: OperatorTier {
                default_timeout_ms: ms(t.default_timeout)?,
                ceiling_timeout_ms: ms(t.ceiling_timeout)?,
            },
            state: policy.state(),
            policy_revision: policy.revision(),
            default_request_timeout_ms: ms(runtime::timeout(t, &RequestLimits::default(), policy))?,
            scan_bytes_warning: match policy {
                PolicyLookup::Found(p) => p.settings().scan_bytes_warning(),
                PolicyLookup::Absent => None,
            },
        })
    };
    summary.availability = Availability::Enabled;
    summary.operator_generation = Some(authority.generation);
    summary.interactive = Some(tier(Priority::Interactive)?);
    summary.batch = Some(tier(Priority::Batch)?);
    if tokio::time::Instant::now() >= deadline {
        return Err(runtime::expired());
    }
    let response = encode(summary, StatusCode::OK)?;
    if tokio::time::Instant::now() >= deadline {
        return Err(runtime::expired());
    }
    Ok(response)
}
