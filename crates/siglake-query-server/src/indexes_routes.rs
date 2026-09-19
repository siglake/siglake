//! `POST/GET/PUT/DELETE /api/v1/indexes` and
//! `GET/PUT/DELETE /api/v1/index-templates`.
//!
//! Audit-log emission is intentionally out of scope here: `query_audit`
//! is query-shaped, so index-management ops will need a follow-up path.
//! Template creation is `PUT`-only upsert for now; there is no separate
//! `POST /api/v1/index-templates`.

use axum::extract::{Extension, Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use siglake_core::index_config::IndexConfig;
use siglake_storage::index_manager::{IndexConfigEntity, IndexConfigPrecondition, IndexTemplate};

use crate::auth::CallerIdentity;
use crate::error::ApiError;
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::openapi_dto::{ApiErrorBody, ManagedIndexPreconditionErrorBody};
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
        (status = 200, description = "The index configuration.", body = IndexConfig,
         headers(("ETag" = String, description = "Strong validator for this table incarnation's full index configuration."))),
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
    let entity = ice
        .get_index_entity(&id)
        .await
        .map_err(ApiError::from_index_manager)?
        .ok_or_else(|| {
            ApiError::new_status(StatusCode::NOT_FOUND, format!("index {id} not found"))
        })?;
    index_entity_response(StatusCode::OK, entity)
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
    params(
        ("id" = String, Path, description = "Index id. Must match the body's `index_id`."),
        ("If-Match" = Option<String>, Header, description = "Optional RFC 9110 strong entity-tag condition. `*` matches any existing index.")
    ),
    request_body = IndexConfig,
    responses(
        (status = 200, description = "Updated. Body is the stored configuration.",
         body = IndexConfig,
         headers(("ETag" = String, description = "Strong validator for the returned configuration."))),
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
        (status = 412, description = "The supplied If-Match condition is false. `current` and ETag come from the exact rejecting commit base; another writer may replace them before this response arrives.",
         body = ManagedIndexPreconditionErrorBody,
         headers(("ETag" = String, description = "Strong validator matching `current`."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn update(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(id): Path<String>,
    headers: HeaderMap,
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
    match parse_if_match(&headers)? {
        Some(precondition) => ice
            .update_index_if_match(&config, precondition)
            .await
            .map_err(ApiError::from_index_manager)?,
        None => ice
            .update_index(&config)
            .await
            .map_err(ApiError::from_index_manager)?,
    }
    let entity = ice
        .get_index_entity(&id)
        .await
        .map_err(ApiError::from_index_manager)?
        .ok_or_else(|| ApiError::internal("updated index missing from catalog"))?;
    index_entity_response(StatusCode::OK, entity)
}

fn index_entity_response(
    status: StatusCode,
    entity: IndexConfigEntity,
) -> Result<Response, ApiError> {
    let etag = HeaderValue::from_str(&entity.etag)
        .map_err(|err| ApiError::internal(format!("invalid managed-index ETag: {err}")))?;
    let mut response = (status, Json(entity.config)).into_response();
    response.headers_mut().insert(header::ETAG, etag);
    Ok(response)
}

fn parse_if_match(headers: &HeaderMap) -> Result<Option<IndexConfigPrecondition>, ApiError> {
    let values: Vec<&HeaderValue> = headers.get_all(header::IF_MATCH).iter().collect();
    if values.is_empty() {
        return Ok(None);
    }

    let mut wildcard = false;
    let mut strong = Vec::new();
    for value in values {
        match parse_if_match_value(value.as_bytes()) {
            Ok(ParsedIfMatch::Any) => wildcard = true,
            Ok(ParsedIfMatch::Tags(tags)) => strong.extend(tags),
            Err(()) => return Err(ApiError::bad_request("malformed If-Match header")),
        }
    }
    if wildcard {
        if !strong.is_empty() || headers.get_all(header::IF_MATCH).iter().count() != 1 {
            return Err(ApiError::bad_request("malformed If-Match header"));
        }
        Ok(Some(IndexConfigPrecondition::Any))
    } else {
        Ok(Some(IndexConfigPrecondition::StrongEtags(strong)))
    }
}

enum ParsedIfMatch {
    Any,
    Tags(Vec<String>),
}

fn parse_if_match_value(input: &[u8]) -> Result<ParsedIfMatch, ()> {
    let mut pos = skip_ows(input, 0);
    if input.get(pos) == Some(&b'*') {
        pos = skip_ows(input, pos + 1);
        return (pos == input.len()).then_some(ParsedIfMatch::Any).ok_or(());
    }

    let mut strong = Vec::new();
    loop {
        let weak = input.get(pos..pos + 2) == Some(b"W/");
        if weak {
            pos += 2;
        }
        if input.get(pos) != Some(&b'\"') {
            return Err(());
        }
        let start = pos;
        pos += 1;
        while let Some(&byte) = input.get(pos) {
            if byte == b'\"' {
                break;
            }
            if !(byte == 0x21 || (0x23..=0x7e).contains(&byte) || byte >= 0x80) {
                return Err(());
            }
            pos += 1;
        }
        if input.get(pos) != Some(&b'\"') {
            return Err(());
        }
        pos += 1;
        if !weak {
            if let Ok(tag) = std::str::from_utf8(&input[start..pos]) {
                strong.push(tag.to_string());
            }
        }
        pos = skip_ows(input, pos);
        if pos == input.len() {
            return Ok(ParsedIfMatch::Tags(strong));
        }
        if input.get(pos) != Some(&b',') {
            return Err(());
        }
        pos = skip_ows(input, pos + 1);
        if pos == input.len() {
            return Err(());
        }
    }
}

fn skip_ows(input: &[u8], mut pos: usize) -> usize {
    while matches!(input.get(pos), Some(b' ' | b'\t')) {
        pos += 1;
    }
    pos
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn if_match_parser_uses_strong_comparison_and_accepts_lists() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_MATCH,
            HeaderValue::from_static("W/\"weak\", \"first,tag\", \"second\""),
        );
        assert_eq!(
            parse_if_match(&headers).unwrap(),
            Some(IndexConfigPrecondition::StrongEtags(vec![
                "\"first,tag\"".to_string(),
                "\"second\"".to_string(),
            ]))
        );
    }

    #[test]
    fn if_match_parser_rejects_malformed_or_mixed_wildcards() {
        for value in ["", "tag", "\"unterminated", "\"tag\",", "*, \"tag\""] {
            let mut headers = HeaderMap::new();
            headers.insert(header::IF_MATCH, HeaderValue::from_str(value).unwrap());
            assert!(parse_if_match(&headers).is_err(), "accepted {value:?}");
        }
    }
}
