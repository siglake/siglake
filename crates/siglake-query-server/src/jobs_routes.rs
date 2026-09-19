//! `GET /api/v1/jobs/<id>` / `GET /api/v1/jobs/<id>/result`
//! / `DELETE /api/v1/jobs/<id>`.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::auth::CallerIdentity;
use crate::error::ApiError;
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::format::RecordsResponse;
use crate::jobs::{JobId, JobInfo, JobStatus, JobStoreError};
#[allow(unused_imports)]
use crate::openapi_dto::{ApiErrorBody, JobCancelResponse};
use crate::AppState;

fn job_store_error(error: JobStoreError) -> ApiError {
    tracing::warn!(error = %error, retryable = error.is_retryable(), "job store request failed");
    if error.is_retryable() {
        ApiError::job_store_unavailable()
    } else {
        ApiError::internal(error)
    }
}

/// Resolve a job the caller is entitled to see, or 404.
///
/// THE DEFECT THIS GUARDS. `status`, `result` and `cancel` were the only
/// handlers in the query server that never extracted `CallerIdentity`, and jobs
/// were looked up by id alone with no tenant recorded anywhere. `result`
/// returns another tenant's full result rows, `status` returns their SQL text
/// -- whose WHERE literals routinely carry user ids, IPs and account
/// identifiers -- and `cancel` terminates their job. A UUIDv7 is a timestamped,
/// loggable identifier, not a capability, and with the default job store the
/// ids live in the shared catalog every tenant's data transits.
///
/// 404 rather than 403 on a mismatch: whether a job id exists is itself
/// information, and the two cases must be indistinguishable.
async fn owned_job(
    state: &AppState,
    identity: &CallerIdentity,
    id: JobId,
) -> Result<JobInfo, ApiError> {
    let not_found = || ApiError::new_status(StatusCode::NOT_FOUND, format!("job {id} not found"));
    let info = state
        .jobs
        .info(id)
        .await
        .map_err(job_store_error)?
        .ok_or_else(not_found)?;
    if info.tenant != state.job_owner(identity) {
        metrics::counter!("siglake_query_job_access_denied_total").increment(1);
        return Err(not_found());
    }
    Ok(info)
}

/// Poll a batch job's status.
///
/// Batch jobs are created by `POST /api/v1/sql` with `priority: "batch"`.
/// Any replica answers for any job: the store is the catalog database by
/// default (`query.jobs.persistent`), so the id is not tied to the pod that
/// took the submission. With that turned off each pod keeps its own in-memory
/// jobs and a poll routed elsewhere answers `404`.
#[utoipa::path(
    get,
    path = "/api/v1/jobs/{id}",
    tag = "jobs",
    params(("id" = String, Path, description = "Job id (UUIDv7).")),
    responses(
        (status = 200, description = "Job state. `cost` is populated once planning \
            has run.", body = JobInfo),
        (status = 400, description = "Malformed job id.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such job.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The job store is temporarily unavailable; retry the request.",
            body = ApiErrorBody,
            headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
    ),
)]
pub async fn status(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = JobId::parse(&id).map_err(|_| ApiError::bad_request("invalid job_id"))?;
    let info = owned_job(&state, &identity, id).await?;
    Ok((StatusCode::OK, Json(info)).into_response())
}

/// Fetch a finished batch job's rows.
///
/// Only valid once the job has succeeded: a still-running job answers `404` and
/// a job that ended in any non-success state answers `409` — poll
/// `GET /api/v1/jobs/{id}` to tell those apart.
#[utoipa::path(
    get,
    path = "/api/v1/jobs/{id}/result",
    tag = "jobs",
    params(("id" = String, Path, description = "Job id (UUIDv7).")),
    responses(
        (status = 200, description = "Result rows.", body = RecordsResponse),
        (status = 400, description = "Malformed job id.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such job, or it is still pending or running.",
         body = ApiErrorBody),
        (status = 409, description = "The job ended in a non-success state \
            (failed, cancelled or timed out).", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The job store is temporarily unavailable; retry the request.",
            body = ApiErrorBody,
            headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
    ),
)]
pub async fn result(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = JobId::parse(&id).map_err(|_| ApiError::bad_request("invalid job_id"))?;
    owned_job(&state, &identity, id).await?;
    let res = state
        .jobs
        .result(id)
        .await
        .map_err(job_store_error)?
        .ok_or_else(|| {
            ApiError::new_status(StatusCode::NOT_FOUND, format!("job {id} not found"))
        })?;
    match res.status {
        JobStatus::Pending | JobStatus::Running => Err(ApiError::new_status(
            StatusCode::NOT_FOUND,
            format!("job {id} is {:?} — try again later", res.status),
        )),
        JobStatus::Cancelled | JobStatus::Failed | JobStatus::Timeout => Err(ApiError::new_status(
            StatusCode::CONFLICT,
            format!(
                "job {id} ended in {:?}; fetch /api/v1/jobs/{id} for details",
                res.status
            ),
        )),
        JobStatus::Succeeded => {
            let body = res
                .body
                .ok_or_else(|| ApiError::internal("succeeded job has no result body"))?;
            Ok((StatusCode::OK, Json(body)).into_response())
        }
    }
}

/// Cancel a batch job.
///
/// The `202` means the cancellation is *persisted* and terminal: the job row
/// reads `cancelled` from every replica, and a completion the executor writes
/// afterwards is refused. Pending or running execution is then aborted,
/// releasing its query-admission reservation and cancelling its storage scans.
///
/// When the executing replica is this one, that abort is immediate. When it is
/// another replica — the ordinary case, since the store is shared by default
/// and the chart runs two query pods — the abort handle lives in that process,
/// and it observes the persisted cancellation within `--jobs-cancel-poll-secs`
/// (default 2 s).
/// So `202` bounds when the work stops rather than promising it has already
/// stopped. A job that already reached a terminal state answers `409`.
#[utoipa::path(
    delete,
    path = "/api/v1/jobs/{id}",
    tag = "jobs",
    params(("id" = String, Path, description = "Job id (UUIDv7).")),
    responses(
        (status = 202, description = "Cancellation persisted. The row is \
            `cancelled` for every replica and a later completion is refused; \
            the executing replica aborts the work within \
            `--jobs-cancel-poll-secs` (default 2 s) when it is not the replica \
            that served this request.", body = JobCancelResponse),
        (status = 400, description = "Malformed job id.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such job.", body = ApiErrorBody),
        (status = 409, description = "The job already reached a terminal state.",
            body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The job store is temporarily unavailable; retry the request.",
            body = ApiErrorBody,
            headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
    ),
)]
pub async fn cancel(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let id = JobId::parse(&id).map_err(|_| ApiError::bad_request("invalid job_id"))?;
    owned_job(&state, &identity, id).await?;
    if state.jobs.cancel(id).await.map_err(job_store_error)? {
        Ok((
            StatusCode::ACCEPTED,
            Json(crate::openapi_dto::JobCancelResponse {
                cancelled: id.to_string(),
            }),
        )
            .into_response())
    } else {
        Err(ApiError::new_status(
            StatusCode::CONFLICT,
            format!("job {id} already reached a terminal state"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::http::header;
    use siglake_storage::iceberg::IcebergContext;

    use super::*;
    use crate::auth::AuthConfig;
    use crate::jobs::{JobStore, ReadFault, ReadOperation};
    use crate::limits::Priority;

    async fn state_with_pending_job() -> (AppState, JobId, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let jobs = JobStore::new(1, Duration::from_secs(600));
        let id = jobs
            .submit("SELECT 1".into(), Priority::Batch, None)
            .await
            .unwrap();
        (
            AppState::new(ice, AuthConfig::open()).with_jobs(jobs),
            id,
            tmp,
        )
    }

    async fn fail_route(operation: ReadOperation, fault: ReadFault) -> ApiError {
        let (state, id, _tmp) = state_with_pending_job().await;
        state.jobs.fail_next_read(operation, fault);
        let identity = CallerIdentity::default();
        let id_path = id.to_string();
        let error = match operation {
            ReadOperation::Info => status(State(state.clone()), Extension(identity), Path(id_path))
                .await
                .expect_err("injected info failure reached the route"),
            ReadOperation::Result => {
                result(State(state.clone()), Extension(identity), Path(id_path))
                    .await
                    .expect_err("injected result failure reached the route")
            }
            ReadOperation::Cancel => {
                let (abort, registration) = futures::future::AbortHandle::new_pair();
                state.jobs.register_abort(id, abort);
                let error = cancel(State(state.clone()), Extension(identity), Path(id_path))
                    .await
                    .expect_err("injected cancel failure reached the route");
                assert_eq!(
                    state
                        .jobs
                        .info(id)
                        .await
                        .expect("job store")
                        .expect("job")
                        .status,
                    JobStatus::Pending,
                    "a failed persistence attempt must not abort or claim cancellation"
                );
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(10),
                        futures::future::Abortable::new(std::future::pending::<()>(), registration)
                    )
                    .await
                    .is_err(),
                    "a failed persistence attempt aborted the executing job"
                );
                state.jobs.clear_abort(id);
                error
            }
        };
        error
    }

    #[tokio::test]
    async fn transient_store_failures_are_retryable_503s_on_every_job_route() {
        for operation in [
            ReadOperation::Info,
            ReadOperation::Result,
            ReadOperation::Cancel,
        ] {
            let error = fail_route(operation, ReadFault::Retryable).await;
            assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                error
                    .headers
                    .iter()
                    .find(|(name, _)| name == header::RETRY_AFTER)
                    .map(|(_, value)| value.to_str().unwrap()),
                Some("5")
            );
        }
    }

    #[tokio::test]
    async fn non_transient_store_failures_are_500s_on_every_job_route() {
        for operation in [
            ReadOperation::Info,
            ReadOperation::Result,
            ReadOperation::Cancel,
        ] {
            let error = fail_route(operation, ReadFault::Permanent).await;
            assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(error
                .headers
                .iter()
                .all(|(name, _)| name != header::RETRY_AFTER));
        }
    }
}
