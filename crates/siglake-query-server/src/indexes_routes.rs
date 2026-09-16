//! `POST/GET/PUT/DELETE /api/v1/indexes` and
//! `GET/PUT/DELETE /api/v1/index-templates`.
//!
//! Audit-log emission is intentionally out of scope here: `query_audit`
//! is query-shaped, so index-management ops will need a follow-up path.
//! Template creation is `PUT`-only upsert for now; there is no separate
//! `POST /api/v1/index-templates`.

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use siglake_core::index_config::IndexConfig;
use siglake_storage::index_manager::IndexTemplate;

use crate::auth::CallerIdentity;
use crate::error::ApiError;
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::openapi_dto::ApiErrorBody;
use crate::AppState;

/// Create an index.
///
/// The body is the full index configuration: its document mapping (typed
/// columns and their tokenizers), the required timestamp field, and optional
/// retention. Attributes not declared here are handled per `doc_mapping.mode`.
#[utoipa::path(
    post,
    path = "/api/v1/indexes",
    tag = "indexes",
    request_body = IndexConfig,
    responses(
        (status = 201, description = "Created. Body is the stored configuration as \
            the catalog normalized it.", body = IndexConfig),
        (status = 400, description = "Invalid configuration.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 409, description = "An index with this id already exists.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn create(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Json(config): Json<IndexConfig>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    ice.create_index(&config)
        .await
        .map_err(ApiError::from_index_manager)?;
    let stored = ice
        .get_index(&config.index_id)
        .await
        .map_err(ApiError::from_index_manager)?
        .ok_or_else(|| ApiError::internal("created index missing from catalog"))?;
    Ok((StatusCode::CREATED, Json(stored)).into_response())
}

/// List every index in the caller's tenant.
#[utoipa::path(
    get,
    path = "/api/v1/indexes",
    tag = "indexes",
    responses(
        (status = 200, description = "All index configurations.", body = Vec<IndexConfig>),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn list(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let indexes = ice
        .list_indexes()
        .await
        .map_err(ApiError::from_index_manager)?;
    Ok((StatusCode::OK, Json(indexes)).into_response())
}

/// Fetch one index configuration.
#[utoipa::path(
    get,
    path = "/api/v1/indexes/{id}",
    tag = "indexes",
    params(("id" = String, Path, description = "Index id.")),
    responses(
        (status = 200, description = "The index configuration.", body = IndexConfig),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn get(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let config = ice
        .get_index(&id)
        .await
        .map_err(ApiError::from_index_manager)?
        .ok_or_else(|| {
            ApiError::new_status(StatusCode::NOT_FOUND, format!("index {id} not found"))
        })?;
    Ok((StatusCode::OK, Json(config)).into_response())
}

/// Replace an index configuration.
///
/// Schema changes are additive only — the catalog rejects an update that would
/// drop or retype an existing column. The body's `index_id` must equal the path
/// id.
#[utoipa::path(
    put,
    path = "/api/v1/indexes/{id}",
    tag = "indexes",
    params(("id" = String, Path, description = "Index id. Must match the body's `index_id`.")),
    request_body = IndexConfig,
    responses(
        (status = 200, description = "Updated. Body is the stored configuration.",
         body = IndexConfig),
        (status = 400, description = "Invalid configuration, or the path id does not \
            match the body's `index_id`.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn update(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
    Json(config): Json<IndexConfig>,
) -> Result<Response, ApiError> {
    if id != config.index_id {
        return Err(ApiError::bad_request(format!(
            "path id `{id}` does not match body index_id `{}`",
            config.index_id
        )));
    }

    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    ice.update_index(&config)
        .await
        .map_err(ApiError::from_index_manager)?;
    let stored = ice
        .get_index(&id)
        .await
        .map_err(ApiError::from_index_manager)?
        .ok_or_else(|| ApiError::internal("updated index missing from catalog"))?;
    Ok((StatusCode::OK, Json(stored)).into_response())
}

/// Delete an index.
///
/// Removes the catalog entry only. Committed data files remain at the table
/// location; the current retention and orphan-GC sweeps cannot reclaim them
/// after the catalog entry is gone.
#[utoipa::path(
    delete,
    path = "/api/v1/indexes/{id}",
    tag = "indexes",
    params(("id" = String, Path, description = "Index id.")),
    responses(
        (status = 204, description = "Deleted. Empty body."),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn delete(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let deleted = ice
        .delete_index(&id)
        .await
        .map_err(ApiError::from_index_manager)?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new_status(
            StatusCode::NOT_FOUND,
            format!("index {id} not found"),
        ))
    }
}

/// List every index template in the caller's tenant namespace.
///
/// Templates apply a document mapping to indexes created later whose id matches
/// one of their glob patterns; the highest `priority` match wins. Templates are
/// never shared across tenant namespaces.
#[utoipa::path(
    get,
    path = "/api/v1/index-templates",
    tag = "index-templates",
    responses(
        (status = 200, description = "All index templates.", body = Vec<IndexTemplate>),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn list_templates(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
) -> Result<Response, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let templates = ice
        .list_index_templates()
        .await
        .map_err(ApiError::from_index_manager)?;
    Ok((StatusCode::OK, Json(templates)).into_response())
}

/// Create or replace an index template.
///
/// Upsert: there is no separate `POST`. The body's `template_id` must equal the
/// path id. Templates are applied at index-creation time, so editing one does
/// not retroactively change existing indexes. The template is stored only in
/// the caller's tenant namespace.
#[utoipa::path(
    put,
    path = "/api/v1/index-templates/{id}",
    tag = "index-templates",
    params(("id" = String, Path,
            description = "Template id. Must match the body's `template_id`.")),
    request_body = IndexTemplate,
    responses(
        (status = 200, description = "Stored. Body echoes the template.", body = IndexTemplate),
        (status = 400, description = "Invalid template, or the path id does not match \
            the body's `template_id`.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn put_template(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
    Json(template): Json<IndexTemplate>,
) -> Result<Response, ApiError> {
    if id != template.template_id {
        return Err(ApiError::bad_request(format!(
            "path id `{id}` does not match body template_id `{}`",
            template.template_id
        )));
    }

    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    ice.put_index_template(&template)
        .await
        .map_err(ApiError::from_index_manager)?;
    Ok((StatusCode::OK, Json(template)).into_response())
}

/// Delete an index template from the caller's tenant namespace.
#[utoipa::path(
    delete,
    path = "/api/v1/index-templates/{id}",
    tag = "index-templates",
    params(("id" = String, Path, description = "Template id.")),
    responses(
        (status = 204, description = "Deleted. Empty body."),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such template.", body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn delete_template(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let deleted = ice
        .delete_index_template(&id)
        .await
        .map_err(ApiError::from_index_manager)?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::new_status(
            StatusCode::NOT_FOUND,
            format!("index template {id} not found"),
        ))
    }
}
