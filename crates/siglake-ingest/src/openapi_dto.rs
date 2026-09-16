//! Response-body types for the ingest server's OpenAPI document.
//!
//! Two kinds live here, and the distinction matters when changing a handler:
//!
//! * **Adopted** — the handler returns the type directly ([`HealthBody`],
//!   [`EsRootInfo`], [`EsClusterHealth`]). These cannot drift from the spec,
//!   because the spec is generated from the same type the handler serializes.
//! * **Spec-only** — the handler still builds an ad-hoc `serde_json::json!`
//!   value (the ES bulk envelope, both error shapes) or the body comes from a
//!   foreign crate we cannot derive on (the OTLP export responses from
//!   `opentelemetry-proto`). These are documentation, kept honest by the
//!   integration tests that assert on the real bodies.
//!
//! Nothing here reads the environment: `SIGLAKE_ES_COMPAT_VERSION` and
//! `SIGLAKE_INGEST_MAX_BODY_BYTES` are resolved per request, and baking either
//! into an `example`/`description` would make the generated spec — and so the
//! CI freshness gate — machine-dependent.

use serde::Serialize;
use utoipa::ToSchema;

/// `GET /healthz` response body.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthBody {
    /// Human-readable status line.
    #[schema(example = "ingest is healthy")]
    pub text: String,
    /// Echo of the HTTP status code.
    #[schema(example = 200)]
    pub code: u16,
}

/// `GET /` response body — an Elasticsearch-shaped fingerprint. Some
/// ES-compatible shippers probe `GET /` and refuse to send to an endpoint that
/// does not look like Elasticsearch at all.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsRootInfo {
    #[schema(example = "siglake")]
    pub name: String,
    #[schema(example = "siglake")]
    pub cluster_name: String,
    pub version: EsVersion,
    #[schema(example = "You Know, for Search")]
    pub tagline: String,
}

/// The `version` object in the Elasticsearch-compatible `GET /` response.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsVersion {
    /// Advertised Elasticsearch wire version. Env-tunable via
    /// `SIGLAKE_ES_COMPAT_VERSION`; the default is an ES 7.10-series version.
    pub number: String,
}

/// `GET /_cluster/health` (and its `/api/v1/_elastic` alias) response body.
/// Always reports `green` — siglake has no ES-style shard allocation to report
/// on, and shippers only use this as a readiness gate.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsClusterHealth {
    #[schema(example = "green")]
    pub status: String,
}

/// `POST /api/v1/_elastic/_bulk` response body.
///
/// Elasticsearch semantics: **HTTP 200 even when `errors` is `true`**. Per-item
/// failures are reported inside `items`, so a client that only checks the HTTP
/// status will silently lose the rejected documents.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsBulkResponse {
    /// Server-side wall time in milliseconds.
    pub took: u64,
    /// True when at least one item in `items` failed.
    pub errors: bool,
    /// One entry per action line in the request, in request order.
    pub items: Vec<EsBulkItem>,
}

/// One `items` entry: a single-key object whose key is the action from the
/// request's action line. Exactly one field is ever present.
///
/// `update` and `delete` are parsed but always rejected per-item with
/// `unsupported_operation_exception` — siglake is append-only.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsBulkItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<EsBulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create: Option<EsBulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<EsBulkItemResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<EsBulkItemResult>,
}

/// Per-item outcome in an Elasticsearch bulk response.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsBulkItemResult {
    /// `201` when the document was accepted, `400` when it was rejected.
    #[schema(example = 201)]
    pub status: u16,
    /// Resolved target index, absent when the action line failed to parse.
    #[serde(rename = "_index", skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    /// Present only on rejection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<EsErrorDetail>,
}

/// Elasticsearch-shaped error detail.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsErrorDetail {
    /// ES exception name, e.g. `parse_exception`,
    /// `unsupported_operation_exception`.
    #[serde(rename = "type")]
    #[schema(rename = "type", example = "parse_exception")]
    pub error_type: String,
    /// Human-readable explanation.
    pub reason: String,
}

/// Error body returned by the **Elasticsearch-compat** endpoints (`_bulk`
/// whole-request failures and every 501 stub).
///
/// The ingest server has two distinct error shapes. Native endpoints use a
/// separate `{ text, code, cost? }` response.
#[derive(Debug, Serialize, ToSchema)]
pub struct EsErrorBody {
    pub error: EsErrorDetail,
    /// Echo of the HTTP status code.
    pub status: u16,
}

/// Error body returned by the **native** endpoints (`/v1/logs`, `/v1/traces`,
/// `/api/v1/stream`).
#[derive(Debug, Serialize, ToSchema)]
pub struct SiglakeErrorBody {
    /// Human-readable failure description.
    pub text: String,
    /// Echo of the HTTP status code.
    pub code: u16,
}

/// `429 Too Many Requests` body, emitted by the rate-limit middleware ahead of
/// authentication. Accompanied by a `Retry-After` header carrying the same
/// number of seconds.
#[derive(Debug, Serialize, ToSchema)]
pub struct RateLimitBody {
    #[schema(example = "Too Many Requests")]
    pub text: String,
    #[schema(example = 429)]
    pub code: u16,
    pub retry_after_secs: u64,
}

/// OTLP `ExportLogsServiceResponse` / `ExportTraceServiceResponse` as rendered
/// on the JSON path (proto3 JSON mapping, so `camelCase`).
///
/// Spec-only: the real value comes from `opentelemetry-proto`. `partialSuccess`
/// is populated only when the request contained no records — a fully accepted
/// request serializes it as `null`.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OtlpExportResponse {
    pub partial_success: Option<OtlpPartialSuccess>,
}

/// The `partialSuccess` object in a successful OTLP export response.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct OtlpPartialSuccess {
    /// Present on the logs endpoint only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected_log_records: Option<i64>,
    /// Present on the traces endpoint only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected_spans: Option<i64>,
    #[schema(example = "request contained no log records")]
    pub error_message: String,
}
