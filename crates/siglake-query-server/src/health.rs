//! Liveness and readiness probes.
//!
//! `/healthz` is always 200 — k8s uses it to decide whether to restart
//! the pod. `/readyz` exercises the catalog by listing namespaces; if
//! that round-trip fails (RDS down, IRSA broken, etc.) the pod gets
//! pulled out of Service rotation but isn't restarted.

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

use crate::openapi_dto::{HealthResponse, ReadyResponse};
use crate::AppState;

/// Liveness probe.
///
/// Mounted outside the auth layer, so it needs no credentials. Answers `200` as
/// long as the process is serving; it says nothing about catalog reachability —
/// use `/readyz` for that.
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    security(()),
    responses((status = 200, description = "The process is serving.", body = HealthResponse)),
)]
pub async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

/// Readiness probe: verifies the Iceberg catalog is reachable.
///
/// Mounted outside the auth layer, so it needs no credentials.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    security(()),
    responses(
        (status = 200, description = "Catalog reachable; ready to serve queries.",
         body = ReadyResponse),
        (status = 503, description = "Catalog unreachable. `error` carries the cause.",
         body = ReadyResponse),
    ),
)]
pub async fn readyz(State(state): State<AppState>) -> (StatusCode, Json<ReadyResponse>) {
    if let Some(authority) = &state.query_controls {
        if authority.ready().await.is_err() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyResponse {
                    status: "not_ready".to_string(),
                    error: Some("query_policy_unavailable".to_string()),
                }),
            );
        }
    }
    match state.ice.catalog().list_namespaces(None).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ReadyResponse {
                status: "ready".to_string(),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                status: "not_ready".to_string(),
                error: Some(if state.query_controls.is_some() {
                    "catalog_unavailable".to_string()
                } else {
                    format!("{e:#}")
                }),
            }),
        ),
    }
}
