//! Authenticated process diagnostics.

use axum::Json;

use crate::openapi_dto::MemoryPoolDebugResponse;

const TOP_CONSUMERS: usize = 10;

/// Snapshot the process-wide DataFusion memory pool.
///
/// `top_consumers` is DataFusion's own report text, ordered by current
/// reservation. The byte fields are `null` for an unbounded pool, and the
/// report is `null` when consumer tracking is disabled or the pool is
/// unbounded.
#[utoipa::path(
    get,
    path = "/debug/memory-pool",
    tag = "debug",
    responses(
        (status = 200, description = "Current query memory-pool occupancy and its ten \
            largest tracked consumers.", body = MemoryPoolDebugResponse),
        (status = 401, description = "Missing or invalid credentials.",
         body = crate::openapi_dto::ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it — including this one, which reads no tenant data.",
         body = crate::openapi_dto::ApiErrorBody),
    ),
)]
pub async fn memory_pool() -> Json<MemoryPoolDebugResponse> {
    let usage = siglake_storage::query_memory_pool_usage();
    let top_consumers = siglake_storage::query_memory_pool_top_consumers(TOP_CONSUMERS);
    Json(MemoryPoolDebugResponse::new(usage, top_consumers))
}

#[cfg(test)]
mod tests {
    use super::MemoryPoolDebugResponse;

    #[test]
    fn snapshot_preserves_usage_and_datafusion_report() {
        let response = MemoryPoolDebugResponse::new(
            Some((12, 64)),
            Some("  SortExec#1(can spill: true) consumed 12.0 B, peak 16.0 B.".into()),
        );

        assert_eq!(response.reserved_bytes, Some(12));
        assert_eq!(response.limit_bytes, Some(64));
        assert!(response.top_consumers.unwrap().contains("SortExec#1"));
    }

    #[test]
    fn unbounded_snapshot_keeps_fields_explicitly_unavailable() {
        let response = MemoryPoolDebugResponse::new(None, None);

        assert_eq!(response.reserved_bytes, None);
        assert_eq!(response.limit_bytes, None);
        assert_eq!(response.top_consumers, None);
    }
}
