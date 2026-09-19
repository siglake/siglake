//! `POST /api/v1/delete-tasks`, `GET /api/v1/delete-tasks`, and
//! `GET /api/v1/delete-tasks/{id}`.

use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    visit_expressions, Expr as SqlAstExpr, Query as SqlQuery, SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::CallerIdentity;
use crate::error::ApiError;
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::openapi_dto::ApiErrorBody;
use crate::AppState;
#[allow(unused_imports)]
use siglake_storage::iceberg::{DeleteTask, DeleteTaskClaimRead};

/// Read-only observation of a pending delete task's permanent claim object.
///
/// `present` says only that the object was observed during this read. It does
/// not establish claimant liveness, abandonment, execution, commit outcome,
/// or whether takeover would be safe. When a present claim body is malformed
/// or names another task, its diagnostic fields are null rather than treating
/// the claim as absent.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DeleteTaskClaimObservation {
    pub present: bool,
    pub observed_at: DateTime<Utc>,
    pub claimant: Option<Uuid>,
    pub claimed_at: Option<DateTime<Utc>>,
    /// Whole seconds from claim creation to `observed_at`, floored at zero for
    /// clock skew. This is not execution duration or proof of abandonment.
    pub age_seconds: Option<u64>,
}

/// GET-only representation of a delete task. POST and collection-list
/// responses remain the persisted [`DeleteTask`] contract.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct DeleteTaskGetResponse {
    #[serde(flatten)]
    pub task: DeleteTask,
    /// Present only while the persisted task state is `pending`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim: Option<DeleteTaskClaimObservation>,
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct CreateDeleteTaskRequest {
    /// Index whose rows should be deleted.
    pub index_id: String,
    /// SQL boolean expression selecting the rows to delete, as it would appear
    /// after `WHERE`.
    #[schema(example = "host = 'web-01'")]
    pub predicate_sql: String,
    /// Optional inclusive lower time bound, narrowing the files the executor
    /// has to rewrite.
    #[serde(default)]
    pub start_ts: Option<DateTime<Utc>>,
    /// Optional exclusive upper time bound.
    #[serde(default)]
    pub end_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListDeleteTasksQuery {
    /// Return only tasks for this index. Omit to list every index's tasks.
    pub index_id: Option<String>,
}

/// Submit a delete task (GDPR / targeted deletion).
///
/// Asynchronous: this records the request and returns immediately with the task
/// in `pending`. A background executor rewrites the affected data files without
/// their matching rows. Rows removed from the live snapshot stay visible to
/// snapshot time-travel until a later snapshot-expiry plus orphan-GC sweep drops
/// the old files, so completion here is not the same as unrecoverable erasure.
///
/// THE TASK BELONGS TO ONE INDEX INCARNATION. The record carries the
/// `table_uuid` this server resolved `index_id` to at submission. An index id
/// is reusable — deleting an index and creating it again under the same id
/// gives a different table — and the executor refuses a task whose `index_id`
/// no longer resolves to that table, so a deletion queued against the dropped
/// index never touches the replacement's rows. The refusal is a `failed` task
/// whose `error` names both tables; recover it the same way as any other
/// failure, by resubmitting against the index that exists. Tasks recorded
/// before this binding existed carry a null `table_uuid` and are refused for
/// the same reason: nothing infers an incarnation from the reused name.
///
/// RECOVERING A `failed` TASK. The lifecycle is one-way: the executor takes
/// `pending` tasks only, and no surface returns a terminal task to `pending`.
/// A task that reached `failed` therefore never runs again, even though the
/// deletion request was acknowledged with a 201 — and neither does one left in
/// `running` by a crash or by a status write that failed after its rewrite
/// committed. The recovery is the same for both: remedy the failure cause —
/// named in the task's `error`, where it has one — make sure the executor runs
/// the fixed build, and then POST the original `index_id`, `predicate_sql`,
/// `start_ts` and `end_ts` here again under the same tenant identity — request
/// fields only. The response carries a NEW `task_id` in `pending`; the failed
/// record and its `error` are left exactly as they were, so record both ids in
/// your audit trail. Then confirm delete-task execution was not turned off for
/// this deployment — the compactor's maintenance sweep runs it by default,
/// `SIGLAKE_DELETE_TASKS=0` disables it — and poll
/// `GET /api/v1/delete-tasks/{id}` on the new id until it is `done` or
/// `failed`.
///
/// This is a resubmission, not a retry: it is not idempotent at the HTTP layer
/// (every POST creates another task) and it promises nothing about
/// exactly-once execution. Each task reports only what its own run rewrote,
/// never the request's cumulative effect. Over an unchanged dataset with the
/// same predicate and the same fixed bounds a repeat finishes with
/// `rows_deleted: 0`, `files_rewritten: 0` and no new snapshot — but a
/// predicate that is time-dependent, or new rows that have arrived since,
/// makes the repeat delete a different set.
#[utoipa::path(
    post,
    path = "/api/v1/delete-tasks",
    tag = "delete-tasks",
    request_body = CreateDeleteTaskRequest,
    responses(
        (status = 201, description = "Task recorded in `pending`, bound to the \
            index incarnation named in `table_uuid`.", body = DeleteTask),
        (status = 400, description = "Empty or unparseable `predicate_sql`, or \
            `start_ts >= end_ts`.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Json(req): Json<CreateDeleteTaskRequest>,
) -> Result<Response, ApiError> {
    if req.predicate_sql.trim().is_empty() {
        return Err(ApiError::bad_request("predicate_sql must not be empty"));
    }
    if matches!((req.start_ts, req.end_ts), (Some(start), Some(end)) if start >= end) {
        return Err(ApiError::bad_request(
            "start_ts and end_ts must satisfy start_ts < end_ts",
        ));
    }
    validate_delete_predicate_fragment(req.predicate_sql.as_str())?;

    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    if ice
        .get_index(&req.index_id)
        .await
        .map_err(ApiError::from_index_manager)?
        .is_none()
    {
        return Err(ApiError::new_status(
            StatusCode::NOT_FOUND,
            format!("index {} not found", req.index_id),
        ));
    }
    plan_delete_predicate(&ice, req.index_id.as_str(), req.predicate_sql.as_str()).await?;

    let task = ice
        .create_delete_task(
            req.index_id.as_str(),
            req.predicate_sql.as_str(),
            req.start_ts,
            req.end_ts,
        )
        .await
        .map_err(ApiError::from_index_manager)?;
    Ok((StatusCode::CREATED, Json(task)).into_response())
}

/// List delete tasks and their progress.
#[utoipa::path(
    get,
    path = "/api/v1/delete-tasks",
    tag = "delete-tasks",
    params(ListDeleteTasksQuery),
    responses(
        (status = 200, description = "Matching delete tasks.", body = Vec<DeleteTask>),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Query(query): Query<ListDeleteTasksQuery>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let tasks = ice
        .list_delete_tasks(query.index_id.as_deref())
        .await
        .map_err(ApiError::internal)?;
    Ok((StatusCode::OK, Json(tasks)).into_response())
}

/// Fetch one delete task, including rows deleted and files rewritten.
#[utoipa::path(
    get,
    path = "/api/v1/delete-tasks/{id}",
    tag = "delete-tasks",
    params(("id" = Uuid, Path, description = "Task id.")),
    responses(
        (status = 200, description = "The delete task. Pending tasks include a read-only claim observation.", body = DeleteTaskGetResponse),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such task.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let task = ice
        .get_delete_task(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::new_status(StatusCode::NOT_FOUND, format!("delete task {id} not found"))
        })?;
    let claim = if task.state == siglake_storage::iceberg::DeleteTaskState::Pending {
        let read = ice
            .read_delete_task_claim(id)
            .await
            .map_err(ApiError::internal)?;
        let observed_at = Utc::now();
        let (present, claimed_at, claimant) = match read {
            DeleteTaskClaimRead::Absent => (false, None, None),
            DeleteTaskClaimRead::Present(None) => (true, None, None),
            DeleteTaskClaimRead::Present(Some(metadata)) => {
                (true, Some(metadata.claimed_at), Some(metadata.claimant))
            }
        };
        let age_seconds = claimed_at.map(|claimed_at| {
            observed_at
                .signed_duration_since(claimed_at)
                .num_seconds()
                .max(0) as u64
        });
        Some(DeleteTaskClaimObservation {
            present,
            observed_at,
            claimant,
            claimed_at,
            age_seconds,
        })
    } else {
        None
    };
    Ok((StatusCode::OK, Json(DeleteTaskGetResponse { task, claim })).into_response())
}

fn validate_delete_predicate_fragment(predicate_sql: &str) -> Result<(), ApiError> {
    let wrapper = format!("SELECT count(*) FROM candidate_file WHERE {predicate_sql}");
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, wrapper.as_str())
        .map_err(|err| ApiError::bad_request(format!("invalid predicate_sql: {err}")))?;
    if stmts.len() != 1 {
        return Err(ApiError::bad_request(
            "predicate_sql must parse as a single WHERE fragment",
        ));
    }
    let Statement::Query(query) = stmts.remove(0) else {
        return Err(ApiError::bad_request(
            "predicate_sql must parse as a SELECT WHERE fragment",
        ));
    };
    let selection = select_where_expr(&query).ok_or_else(|| {
        ApiError::bad_request("predicate_sql must parse as a SELECT WHERE fragment")
    })?;
    use core::ops::ControlFlow;
    let contains_subquery = visit_expressions(selection, |expr: &SqlAstExpr| match expr {
        SqlAstExpr::Subquery(_) | SqlAstExpr::Exists { .. } | SqlAstExpr::InSubquery { .. } => {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break();
    if contains_subquery {
        return Err(ApiError::bad_request(
            "predicate_sql must not contain subqueries",
        ));
    }
    Ok(())
}

fn select_where_expr(query: &SqlQuery) -> Option<&SqlAstExpr> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    select.selection.as_ref()
}

async fn plan_delete_predicate(
    ice: &siglake_storage::iceberg::IcebergContext,
    index_id: &str,
    predicate_sql: &str,
) -> Result<(), ApiError> {
    let ctx = SessionContext::new();
    ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(index_id), index_id)
        .await
        .map_err(ApiError::internal)?;
    let query = format!(
        "SELECT count(*) FROM {} WHERE {}",
        quote_sql_ident(index_id),
        predicate_sql,
    );
    // Read-only, like every other place a client's SQL text reaches the
    // planner (see `sql::read_only_sql`). `validate_delete_predicate_fragment`
    // already requires the wrapped text to be ONE `Statement::Query`, so this
    // is defence in depth rather than the load-bearing check — but the
    // permissive default is what made `COPY … TO` a filesystem write, and no
    // request-derived string should be planned under it.
    crate::sql::plan_client_sql(&ctx, query.as_str())
        .await
        .map_err(|err| ApiError::bad_request(format!("invalid predicate_sql: {err}")))?;
    Ok(())
}

fn quote_sql_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
