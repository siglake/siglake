//! Response-body types for the query server's OpenAPI document.
//!
//! Two kinds live here, and the distinction matters when changing a handler:
//!
//! * **Adopted** — the handler returns the type directly ([`HealthResponse`],
//!   [`ReadyResponse`], [`BatchSubmitResponse`], [`JobCancelResponse`]). These
//!   cannot drift from the spec, because the spec is generated from the same
//!   type the handler serializes.
//! * **Spec-only** — the body is a foreign contract still built as an ad-hoc
//!   `serde_json::json!` value ([`ApiErrorBody`] and the Jaeger shapes). These
//!   are documentation, kept honest by the integration tests that assert on the
//!   real bodies.

use serde::Serialize;
use utoipa::ToSchema;

use crate::cost::CostReport;
use crate::format::RecordsResponse;
use siglake_core::index_config::IndexConfig;

/// `GET /healthz` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    #[schema(example = "ok")]
    pub status: String,
}

/// `GET /readyz` response body. `status` is `ready` on 200 and `not_ready` on
/// 503, in which case `error` carries the catalog failure.
#[derive(Debug, Serialize, ToSchema)]
pub struct ReadyResponse {
    #[schema(example = "ready")]
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `GET /debug/memory-pool` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct MemoryPoolDebugResponse {
    /// Bytes currently reserved by DataFusion operators. `null` when the pool
    /// is unbounded.
    pub reserved_bytes: Option<u64>,
    /// Process-wide DataFusion pool limit in bytes. `null` when the pool is
    /// unbounded.
    pub limit_bytes: Option<u64>,
    /// DataFusion's report for the ten consumers with the largest current
    /// reservations. `null` when tracking is disabled or the pool is
    /// unbounded.
    pub top_consumers: Option<String>,
}

impl MemoryPoolDebugResponse {
    pub(crate) fn new(usage: Option<(u64, u64)>, top_consumers: Option<String>) -> Self {
        let (reserved_bytes, limit_bytes) = usage
            .map(|(reserved, limit)| (Some(reserved), Some(limit)))
            .unwrap_or((None, None));
        Self {
            reserved_bytes,
            limit_bytes,
            top_consumers,
        }
    }
}

/// `202 Accepted` body returned by `POST /api/v1/sql` when the request carries
/// `priority: "batch"`: after admission, the query is queued rather than
/// executed inline and the caller polls the returned URLs. A full shared query
/// admission budget returns `429` instead of creating a job.
#[derive(Debug, Serialize, ToSchema)]
pub struct BatchSubmitResponse {
    /// Job identifier (UUIDv7, so ids sort by submission time).
    pub job_id: String,
    /// Path to poll for status — `GET /api/v1/jobs/{id}`.
    pub status_url: String,
    /// Path to fetch the result once the job succeeds —
    /// `GET /api/v1/jobs/{id}/result`.
    pub result_url: String,
    /// Always `batch`.
    #[schema(example = "batch")]
    pub priority: String,
}

/// `202 Accepted` body of `DELETE /api/v1/jobs/{id}`.
#[derive(Debug, Serialize, ToSchema)]
pub struct JobCancelResponse {
    /// Id of the job that was cancelled.
    pub cancelled: String,
}

/// Error body used by every endpoint on this server.
///
/// Spec-only. Some errors additionally flatten diagnostic keys into the top
/// level — `cost` on a rejected or timed-out query, and `cost` plus `stats` on
/// the mid-flight rows breaker — so this schema deliberately allows additional
/// properties rather than pinning an exact shape.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorBody {
    /// Human-readable failure description.
    pub error: String,
    /// Echo of the HTTP status code.
    pub code: u16,
    /// Present when the failure was cost- or time-related.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<CostReport>,
}

/// A failed managed-index `If-Match` condition.
#[derive(Debug, Serialize, ToSchema)]
pub struct ManagedIndexPreconditionErrorBody {
    /// Human-readable failure description.
    pub error: String,
    /// Always 412.
    pub code: u16,
    /// Configuration from the exact commit base that rejected the condition.
    pub current: IndexConfig,
}

// utoipa keys responses by status code, so separate 200 entries would collide;
// this wrapper preserves both success shapes as a `oneOf`.
/// The two success bodies `POST /api/v1/sql` can return at `200`: a dry run
/// answers with only a cost report, while a real query answers with result rows.
#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum SqlSuccess {
    /// Result rows. The normal case.
    Records(RecordsResponse),
    /// Cost estimate only, returned when the request set `dry_run: true`.
    Cost(CostReport),
}

/// Jaeger `{ data, total }` envelope for the service- and operation-name lists.
#[derive(Debug, Serialize, ToSchema)]
pub struct JaegerNamesResponse {
    pub data: Vec<String>,
    /// Number of entries in `data`.
    pub total: usize,
}

/// Jaeger `{ data, total }` envelope for trace lookups.
#[derive(Debug, Serialize, ToSchema)]
pub struct JaegerTracesResponse {
    pub data: Vec<JaegerTrace>,
    /// Number of traces in `data`.
    pub total: usize,
}

/// One trace in Jaeger's UI wire format.
///
/// Spec-only, and deliberately loose: the shape is Jaeger's contract, not
/// siglake's, and is reproduced closely enough for the Jaeger UI to render.
#[derive(Debug, Serialize, ToSchema)]
pub struct JaegerTrace {
    #[serde(rename = "traceID")]
    #[schema(rename = "traceID")]
    pub trace_id: String,
    pub spans: Vec<JaegerSpan>,
    /// Map of process id (`p1`, `p2`, …) to `{ serviceName, tags }`. Spans
    /// reference these through `processID`.
    #[schema(value_type = Object)]
    pub processes: serde_json::Value,
}

/// One span in a trace response, in Jaeger's UI wire format.
#[derive(Debug, Serialize, ToSchema)]
pub struct JaegerSpan {
    #[serde(rename = "traceID")]
    #[schema(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "spanID")]
    #[schema(rename = "spanID")]
    pub span_id: String,
    #[serde(rename = "operationName")]
    #[schema(rename = "operationName")]
    pub operation_name: String,
    /// Parent links. Jaeger models a parent as a `CHILD_OF` reference.
    #[schema(value_type = Vec<Object>)]
    pub references: serde_json::Value,
    /// Span start, in microseconds since the epoch.
    #[serde(rename = "startTime")]
    #[schema(rename = "startTime")]
    pub start_time: i64,
    /// Span duration in microseconds.
    pub duration: i64,
    /// Span attributes as Jaeger key/value/type triples.
    #[schema(value_type = Vec<Object>)]
    pub tags: serde_json::Value,
    /// Span events as Jaeger timestamped log entries.
    #[schema(value_type = Vec<Object>)]
    pub logs: serde_json::Value,
    #[serde(rename = "processID")]
    #[schema(rename = "processID")]
    pub process_id: String,
}
