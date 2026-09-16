use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use axum::extract::{Extension, Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use datafusion::prelude::{DataFrame, SessionContext};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use siglake_core::index_config::{FieldType, IndexConfig};

use crate::auth::CallerIdentity;
use crate::cost::CostReport;
use crate::error::ApiError;
use crate::format::batches_to_records;
use crate::jaeger_limits::{ceilings_from, JaegerCeilings};
use crate::limits::{Priority, RequestLimits, ResolvedLimits};
use crate::midflight::{AccumulatedBound, AccumulationBound, CollectOutcome};
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::openapi_dto::{ApiErrorBody, JaegerNamesResponse, JaegerTracesResponse};
use crate::AppState;

const TRACE_TABLE_NAME: &str = "traces";
const REQUIRED_TRACE_COLUMNS: &[(&str, ExpectedFieldType)] = &[
    ("timestamp", ExpectedFieldType::Datetime),
    ("trace_id", ExpectedFieldType::Text),
    ("span_id", ExpectedFieldType::Text),
    ("parent_span_id", ExpectedFieldType::Text),
    ("service", ExpectedFieldType::Text),
    ("name", ExpectedFieldType::Text),
    ("kind", ExpectedFieldType::Text),
    ("status_code", ExpectedFieldType::Text),
    ("duration_nanos", ExpectedFieldType::Long),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExpectedFieldType {
    Text,
    Long,
    Datetime,
}

/// One of the two name lists the Jaeger UI polls, and everything that tells
/// two of those polls apart.
///
/// A type rather than two SQL strings at the call sites because the SQL is no
/// longer the whole identity of the request: it is also the result-cache key,
/// and `service` has to appear in both.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NameList {
    /// Every service in the index.
    Services,
    /// Every operation of ONE service.
    Operations { service: String },
}

impl NameList {
    /// All history, deliberately: a service or operation that has stopped
    /// emitting stays listed, which is what makes these queries a pure
    /// function of the snapshot and what #2185 settled must not change.
    fn sql(&self) -> String {
        match self {
            NameList::Services => "SELECT DISTINCT service FROM traces \
                 WHERE service IS NOT NULL AND service != '' ORDER BY service"
                .to_string(),
            NameList::Operations { service } => format!(
                "SELECT DISTINCT name FROM traces \
                 WHERE service = {} AND name IS NOT NULL AND name != '' \
                 ORDER BY name",
                sql_string_literal(service)
            ),
        }
    }

    /// The part of the cache key that separates the two lists, and one
    /// service's operations from another's.
    fn key_fragment(&self) -> String {
        match self {
            NameList::Services => "kind=services".to_string(),
            NameList::Operations { service } => {
                format!("kind=operations|service={}", key_field(service))
            }
        }
    }
}

/// A cache-key field carrying a value this server does not choose: an index id
/// or a Jaeger service name. Length-prefixed, so no value can spell the
/// separators the key is built out of and pose as another request's key.
fn key_field(value: &str) -> String {
    format!("{}:{value}", value.len())
}

/// Query parameters of the Jaeger `find traces` API. Names and semantics follow
/// Jaeger's own HTTP API so the Jaeger UI can point at siglake unchanged, which
/// is why several are camelCase.
#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct FindTracesParams {
    /// Service name to search within (`service.name`).
    #[serde(default)]
    service: Option<String>,
    /// Span/operation name to match.
    #[serde(default, rename = "operation")]
    operation: Option<String>,
    /// Inclusive start of the search window, in microseconds since the epoch.
    #[serde(default)]
    start: Option<i64>,
    /// Exclusive end of the search window, in microseconds since the epoch.
    #[serde(default)]
    end: Option<i64>,
    /// Maximum number of traces to return.
    #[serde(default)]
    limit: Option<usize>,
    /// Minimum span duration as a Go-style duration string, e.g. `100ms`, `2s`.
    #[serde(default, rename = "minDuration")]
    min_duration: Option<String>,
    /// Maximum span duration as a Go-style duration string.
    #[serde(default, rename = "maxDuration")]
    max_duration: Option<String>,
    /// Tag filter. Accepts either Jaeger's comma-separated `key:value` form
    /// (`http.status_code:500,region:eu`) or a JSON object of tag names to
    /// values. No schema can express that dual form, hence the prose.
    #[serde(default)]
    tags: Option<String>,
}

/// Jaeger-compatible service-name list.
///
/// Part of a read API shaped so the Jaeger UI can be pointed at siglake
/// unchanged. Backed by the same stored spans SQL sees.
#[utoipa::path(
    get,
    path = "/api/v1/jaeger/{index}/api/services",
    tag = "jaeger",
    params(("index" = String, Path, description = "Index holding the spans.")),
    responses(
        (status = 200, description = "Distinct service names.", body = JaegerNamesResponse),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 413, description = "The render exceeded one of this request's \
            ceilings. Two units apply: distinct names and accumulated Arrow bytes \
            (a span can carry megabytes of attributes, so no row count bounds the \
            render). Both are DERIVED from the admission reservation this request \
            already holds — `min(16 MiB, budget / share)` — so they move with \
            `SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES` and the pod's memory and there is \
            no Jaeger-specific knob; the resolved interactive `max_rows_returned` \
            still applies where it is tighter. Counted across the WHOLE request: for \
            a trace search, the spans of every selected trace rather than each trace \
            separately, and trace selection and span retrieval spend ONE budget. \
            Read MID-FLIGHT, at a batch boundary — and the PRODUCER is bounded \
            too, so the refusal no longer costs a whole materialized result: the \
            plan carries a fetch of one row past the row bound, which turns the \
            span query's blocking sort into a bounded `TopK`. The figure in the \
            message is what was held when the bound was crossed — one row past the \
            row bound, and for the byte bound whatever those rows weigh, since one \
            span can carry megabytes — not the size of the whole result. NOTHING is returned: `/api/v1/sql` answers 413 with a \
            truncated body and says so in its envelope (`truncated`, `max_rows`), \
            but the Jaeger response carries only `data` and `total`, and a trace \
            silently missing spans is worse than no trace. No `Retry-After` — the \
            refusal is deterministic, not load-dependent. Narrow the request: \
            service, operation, time window, tags, or a smaller `limit`. Counted as \
            `siglake_query_breaker_trips_total{breaker=\"jaeger_name_rows\"}` or \
            `{breaker=\"jaeger_render_bytes\"}`.", body = ApiErrorBody),
        (status = 429, description = "The shared interactive admission budget stayed \
            full through the admission wait. A trace read reserves from the SAME \
            per-pod budget as `/api/v1/sql` — one reservation per request, held \
            across both query phases — so it sheds the same way. Nothing is wrong \
            with the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The query memory pool (or the spill directory's \
            byte cap) refused an allocation for this trace query — the same pool the \
            SQL routes run on, and the same answer they give. Nothing is wrong with \
            the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The request exceeded the server's interactive \
            wall-clock budget. ONE budget starts before tenant resolution and covers \
            the index lookup, table registration and every query phase (for a trace \
            search, trace-id selection AND span retrieval), so a request that spends \
            it all preparing is refused rather than handed a fresh budget to scan \
            with. The refusal releases the admission reservation and cancels any scan \
            already running for the request.", body = ApiErrorBody),
    ),
)]
pub async fn services(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(index): Path<String>,
) -> Result<Response, ApiError> {
    let request = TraceRequest::begin(&state).await?;
    let traces = request.open(&state, &identity, &index).await?;
    let services = request.name_list(&traces, NameList::Services).await?;
    Ok((
        axum::http::StatusCode::OK,
        Json(json!({
            "data": services,
            "total": services.len(),
        })),
    )
        .into_response())
}

/// Jaeger-compatible operation-name list for one service.
#[utoipa::path(
    get,
    path = "/api/v1/jaeger/{index}/api/services/{service}/operations",
    tag = "jaeger",
    params(
        ("index" = String, Path, description = "Index holding the spans."),
        ("service" = String, Path, description = "Service name to list operations for."),
    ),
    responses(
        (status = 200, description = "Distinct operation names.", body = JaegerNamesResponse),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 413, description = "The render exceeded one of this request's \
            ceilings. Two units apply: distinct names and accumulated Arrow bytes \
            (a span can carry megabytes of attributes, so no row count bounds the \
            render). Both are DERIVED from the admission reservation this request \
            already holds — `min(16 MiB, budget / share)` — so they move with \
            `SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES` and the pod's memory and there is \
            no Jaeger-specific knob; the resolved interactive `max_rows_returned` \
            still applies where it is tighter. Counted across the WHOLE request: for \
            a trace search, the spans of every selected trace rather than each trace \
            separately, and trace selection and span retrieval spend ONE budget. \
            Read MID-FLIGHT, at a batch boundary — and the PRODUCER is bounded \
            too, so the refusal no longer costs a whole materialized result: the \
            plan carries a fetch of one row past the row bound, which turns the \
            span query's blocking sort into a bounded `TopK`. The figure in the \
            message is what was held when the bound was crossed — one row past the \
            row bound, and for the byte bound whatever those rows weigh, since one \
            span can carry megabytes — not the size of the whole result. NOTHING is returned: `/api/v1/sql` answers 413 with a \
            truncated body and says so in its envelope (`truncated`, `max_rows`), \
            but the Jaeger response carries only `data` and `total`, and a trace \
            silently missing spans is worse than no trace. No `Retry-After` — the \
            refusal is deterministic, not load-dependent. Narrow the request: \
            service, operation, time window, tags, or a smaller `limit`. Counted as \
            `siglake_query_breaker_trips_total{breaker=\"jaeger_name_rows\"}` or \
            `{breaker=\"jaeger_render_bytes\"}`.", body = ApiErrorBody),
        (status = 429, description = "The shared interactive admission budget stayed \
            full through the admission wait. A trace read reserves from the SAME \
            per-pod budget as `/api/v1/sql` — one reservation per request, held \
            across both query phases — so it sheds the same way. Nothing is wrong \
            with the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The query memory pool (or the spill directory's \
            byte cap) refused an allocation for this trace query — the same pool the \
            SQL routes run on, and the same answer they give. Nothing is wrong with \
            the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The request exceeded the server's interactive \
            wall-clock budget. ONE budget starts before tenant resolution and covers \
            the index lookup, table registration and every query phase (for a trace \
            search, trace-id selection AND span retrieval), so a request that spends \
            it all preparing is refused rather than handed a fresh budget to scan \
            with. The refusal releases the admission reservation and cancels any scan \
            already running for the request.", body = ApiErrorBody),
    ),
)]
pub async fn operations(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path((index, service)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let request = TraceRequest::begin(&state).await?;
    let traces = request.open(&state, &identity, &index).await?;
    let operations = request
        .name_list(&traces, NameList::Operations { service })
        .await?;
    Ok((
        axum::http::StatusCode::OK,
        Json(json!({
            "data": operations,
            "total": operations.len(),
        })),
    )
        .into_response())
}

/// Jaeger-compatible trace search.
#[utoipa::path(
    get,
    path = "/api/v1/jaeger/{index}/api/traces",
    tag = "jaeger",
    params(
        ("index" = String, Path, description = "Index holding the spans."),
        FindTracesParams,
    ),
    responses(
        (status = 200, description = "Matching traces. `data` is empty rather than \
            absent when nothing matched.", body = JaegerTracesResponse),
        (status = 400, description = "Unparseable duration or tag filter, or a \
            `limit` above this server's derived Jaeger trace ceiling. The ceiling \
            comes out of the same admission reservation as the render ceilings \
            below (no Jaeger-specific knob), and `limit` is checked BEFORE the \
            reservation is taken, before the index lookup and before the planner — \
            so a flood of hostile `?limit=` cannot occupy the pod's admission \
            budget on its way to being refused. The message names both the ceiling \
            and the value sent. 400 rather than 413 because the request is refused \
            on its face: the answer does not depend on what is stored or on load, so \
            it carries no retry semantics, and 413 here already means \"your result \
            was too big\" (narrow the search) when the fix is to lower `limit`. \
            Counted as \
            `siglake_query_breaker_trips_total{breaker=\"jaeger_trace_limit\"}`.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 413, description = "The render exceeded one of this request's \
            ceilings. Two units apply: span rows and accumulated Arrow bytes \
            (a span can carry megabytes of attributes, so no row count bounds the \
            render). Both are DERIVED from the admission reservation this request \
            already holds — `min(16 MiB, budget / share)` — so they move with \
            `SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES` and the pod's memory and there is \
            no Jaeger-specific knob; the resolved interactive `max_rows_returned` \
            still applies where it is tighter. Counted across the WHOLE request: for \
            a trace search, the spans of every selected trace rather than each trace \
            separately, and trace selection and span retrieval spend ONE budget. \
            Read MID-FLIGHT, at a batch boundary — and the PRODUCER is bounded \
            too, so the refusal no longer costs a whole materialized result: the \
            plan carries a fetch of one row past the row bound, which turns the \
            span query's blocking sort into a bounded `TopK`. The figure in the \
            message is what was held when the bound was crossed — one row past the \
            row bound, and for the byte bound whatever those rows weigh, since one \
            span can carry megabytes — not the size of the whole result. NOTHING is returned: `/api/v1/sql` answers 413 with a \
            truncated body and says so in its envelope (`truncated`, `max_rows`), \
            but the Jaeger response carries only `data` and `total`, and a trace \
            silently missing spans is worse than no trace. No `Retry-After` — the \
            refusal is deterministic, not load-dependent. Narrow the request: \
            service, operation, time window, tags, or a smaller `limit`. Counted as \
            `siglake_query_breaker_trips_total{breaker=\"jaeger_span_rows\"}` or \
            `{breaker=\"jaeger_render_bytes\"}`.", body = ApiErrorBody),
        (status = 429, description = "The shared interactive admission budget stayed \
            full through the admission wait. A trace read reserves from the SAME \
            per-pod budget as `/api/v1/sql` — one reservation per request, held \
            across both query phases — so it sheds the same way. Nothing is wrong \
            with the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The query memory pool (or the spill directory's \
            byte cap) refused an allocation for this trace query — the same pool the \
            SQL routes run on, and the same answer they give. Nothing is wrong with \
            the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The request exceeded the server's interactive \
            wall-clock budget. ONE budget starts before tenant resolution and covers \
            the index lookup, table registration and every query phase (for a trace \
            search, trace-id selection AND span retrieval), so a request that spends \
            it all preparing is refused rather than handed a fresh budget to scan \
            with. The refusal releases the admission reservation and cancels any scan \
            already running for the request.", body = ApiErrorBody),
    ),
)]
pub async fn find_traces(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path(index): Path<String>,
    Query(params): Query<FindTracesParams>,
) -> Result<Response, ApiError> {
    // BEFORE `begin`, deliberately: before the admission reservation, the index
    // lookup and the planner (#2184). `?limit=` is the only multiplier a client
    // controls, and this is the only ceiling on this surface that can be
    // enforced for free — so a flood of hostile `?limit=` requests cannot
    // occupy the pod's admission budget on its way to being refused.
    refuse_over_trace_ceiling(params.limit, &state)?;
    let request = TraceRequest::begin(&state).await?;
    let traces = request.open(&state, &identity, &index).await?;
    let trace_ids = request.phase(traces.find_trace_ids(&params)).await?;
    if trace_ids.is_empty() {
        // Immediately ready, and still refused when the clock is gone: the
        // selection scan can consume the whole budget and come back empty.
        request.check_deadline()?;
        return Ok((
            axum::http::StatusCode::OK,
            Json(json!({"data": [], "total": 0})),
        )
            .into_response());
    }
    let rows = request
        .phase(traces.fetch_spans_for_trace_ids(&trace_ids))
        .await?;
    let data = build_jaeger_traces(rows, &trace_ids)?;
    Ok((
        axum::http::StatusCode::OK,
        Json(json!({
            "data": data,
            "total": trace_ids.len(),
        })),
    )
        .into_response())
}

/// Jaeger-compatible single-trace lookup.
#[utoipa::path(
    get,
    path = "/api/v1/jaeger/{index}/api/traces/{trace_id}",
    tag = "jaeger",
    params(
        ("index" = String, Path, description = "Index holding the spans."),
        ("trace_id" = String, Path, description = "Hex-encoded trace id."),
    ),
    responses(
        (status = 200, description = "The trace, as a single-element `data` array.",
         body = JaegerTracesResponse),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars). Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 404, description = "No such index.", body = ApiErrorBody),
        (status = 413, description = "The render exceeded one of this request's \
            ceilings. Two units apply: span rows and accumulated Arrow bytes \
            (a span can carry megabytes of attributes, so no row count bounds the \
            render). Both are DERIVED from the admission reservation this request \
            already holds — `min(16 MiB, budget / share)` — so they move with \
            `SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES` and the pod's memory and there is \
            no Jaeger-specific knob; the resolved interactive `max_rows_returned` \
            still applies where it is tighter. Counted across the WHOLE request: for \
            a trace search, the spans of every selected trace rather than each trace \
            separately, and trace selection and span retrieval spend ONE budget. \
            Read MID-FLIGHT, at a batch boundary — and the PRODUCER is bounded \
            too, so the refusal no longer costs a whole materialized result: the \
            plan carries a fetch of one row past the row bound, which turns the \
            span query's blocking sort into a bounded `TopK`. The figure in the \
            message is what was held when the bound was crossed — one row past the \
            row bound, and for the byte bound whatever those rows weigh, since one \
            span can carry megabytes — not the size of the whole result. NOTHING is returned: `/api/v1/sql` answers 413 with a \
            truncated body and says so in its envelope (`truncated`, `max_rows`), \
            but the Jaeger response carries only `data` and `total`, and a trace \
            silently missing spans is worse than no trace. No `Retry-After` — the \
            refusal is deterministic, not load-dependent. Narrow the request: \
            service, operation, time window, tags, or a smaller `limit`. Counted as \
            `siglake_query_breaker_trips_total{breaker=\"jaeger_span_rows\"}` or \
            `{breaker=\"jaeger_render_bytes\"}`.", body = ApiErrorBody),
        (status = 429, description = "The shared interactive admission budget stayed \
            full through the admission wait. A trace read reserves from the SAME \
            per-pod budget as `/api/v1/sql` — one reservation per request, held \
            across both query phases — so it sheds the same way. Nothing is wrong \
            with the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The query memory pool (or the spill directory's \
            byte cap) refused an allocation for this trace query — the same pool the \
            SQL routes run on, and the same answer they give. Nothing is wrong with \
            the request: wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The request exceeded the server's interactive \
            wall-clock budget. ONE budget starts before tenant resolution and covers \
            the index lookup, table registration and every query phase (for a trace \
            search, trace-id selection AND span retrieval), so a request that spends \
            it all preparing is refused rather than handed a fresh budget to scan \
            with. The refusal releases the admission reservation and cancels any scan \
            already running for the request.", body = ApiErrorBody),
    ),
)]
pub async fn get_trace(
    State(state): State<AppState>,
    Extension(identity): Extension<CallerIdentity>,
    Path((index, trace_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let request = TraceRequest::begin(&state).await?;
    let traces = request.open(&state, &identity, &index).await?;
    let rows = request
        .phase(traces.fetch_spans_for_trace_ids(std::slice::from_ref(&trace_id)))
        .await?;
    let data = build_jaeger_traces(rows, std::slice::from_ref(&trace_id))?;
    Ok((
        axum::http::StatusCode::OK,
        Json(json!({
            "data": data,
            "total": data.len(),
        })),
    )
        .into_response())
}

/// One interactive request lifecycle for the four Jaeger read routes (#2096).
///
/// These handlers used to enter [`TraceQueryContext`] directly and collect:
/// no admission reservation, no wall-clock budget, no cancellation. A trace
/// search is an aggregate plus a client-sized `TopK` over the same tables, the
/// same session contexts and the same process-wide memory pool `/api/v1/sql`
/// runs on, so trace traffic evaded every request-level control SQL and batch
/// work are subject to; the pool refusal (#2095) was the only backpressure
/// left, and it answers after the memory is already gone.
///
/// Deliberately NOT a private budget. The reservation comes out of the shared
/// interactive [`crate::admission::AdmissionController`] and the wall clock is
/// the interactive tier's own `default_timeout`, so trace reads compete with
/// SQL rather than beside it. The Jaeger API has no `limits` object to tighten
/// them with — its shape is fixed by the Jaeger UI — so the server ceilings ARE
/// the resolved limits here.
///
/// ONE guard and ONE deadline per REQUEST, not per query phase. `find_traces`
/// selects trace ids and then retrieves their spans: two reservations would let
/// the second one 429 after the first phase's scan was already paid for, and
/// two budgets would let a request that spent its whole clock selecting ids
/// start a fresh one to retrieve spans with — a 60s client deadline turning
/// into 120s of scan work on the pod.
struct TraceRequest {
    resolved: ResolvedLimits,
    started: Instant,
    deadline: tokio::time::Instant,
    /// No cost report exists on this surface: these are two fixed query shapes
    /// and `estimate()` is deliberately not run for them, while the 504 body
    /// carries a cost. The same stand-in the SQL handler's own preparation
    /// phases use, and it says so in its warnings.
    cost: CostReport,
    cancel: siglake_storage::QueryCancel,
    /// Fields drop in declaration order, so the flag that ends the source
    /// streams the spawned partition pumps drain from fires BEFORE the
    /// reservation is handed to the next waiter — a request that gave its
    /// budget back while still scanning would be admission telling the truth
    /// about bytes nobody has released.
    _cancel_guard: siglake_storage::CancelOnDrop,
    _admission: crate::admission::AdmissionGuard,
}

impl TraceRequest {
    /// Start the clock, then take the reservation.
    ///
    /// In that order, and before tenant resolution: the index lookup and the
    /// DataFusion table registration are catalog and object-store work that a
    /// slow backend can stretch past any request budget, and admission's own
    /// bounded wait (`SIGLAKE_QUERY_ADMISSION_WAIT_MS`) is time this request
    /// has SPENT, not time it gets for free.
    ///
    /// `/api/v1/sql` admits later — after planning, because its reservation is
    /// priced from the cost estimate. Nothing prices this one, so there is
    /// nothing to wait for, and holding it from the start is what keeps a flood
    /// of trace reads from all registering tables at once.
    async fn begin(state: &AppState) -> Result<Self, ApiError> {
        let tier = state.limits.tier(Priority::Interactive);
        let resolved =
            ResolvedLimits::resolve(&RequestLimits::default(), Priority::Interactive, &tier);
        let started = Instant::now();
        let deadline = tokio::time::Instant::from_std(started + resolved.timeout);
        let admission = crate::sql::acquire_admission_reservation(
            state,
            Priority::Interactive,
            state.admission.unestimated_reservation_bytes(),
            None,
        )
        .await?;
        let cancel = siglake_storage::QueryCancel::new();
        Ok(Self {
            resolved,
            started,
            deadline,
            cost: CostReport::unknown_after_timeout(),
            cancel: cancel.clone(),
            _cancel_guard: siglake_storage::CancelOnDrop(cancel),
            _admission: admission,
        })
    }

    /// Run one phase on what is LEFT of this request's budget.
    async fn phase<F, T>(&self, fut: F) -> Result<T, ApiError>
    where
        F: std::future::Future<Output = Result<T, ApiError>>,
    {
        crate::sql::apply_timeout(fut, &self.resolved, &self.cost, self.deadline, self.started)
            .await
    }

    /// The same refusal for a response that needs no `await` at all.
    ///
    /// `phase` checks before it polls because `tokio::time::timeout_at` polls
    /// the inner future first even on an elapsed deadline; a branch that is
    /// simply ready (an empty trace-id set) has no future to wrap, and must
    /// not answer 200 to a request whose wall clock is already gone.
    fn check_deadline(&self) -> Result<(), ApiError> {
        if crate::sql::deadline_expired(&self.resolved, self.deadline) {
            return Err(crate::sql::request_timeout_error(
                &self.resolved,
                &self.cost,
                self.started,
            ));
        }
        Ok(())
    }

    /// One name-list poll, on this request's budget and through the result
    /// cache (#2268).
    ///
    /// The cache probe runs INSIDE the phase, so a single-flight follower's
    /// wait is spent out of this request's wall clock like every other await
    /// here, and a request whose clock runs out while parked is refused rather
    /// than handed the leader's answer late. The admission reservation is held
    /// across it either way — it was taken in `begin`, before the index lookup
    /// — so a hit is served under the same backpressure a miss is.
    async fn name_list(
        &self,
        traces: &TraceQueryContext,
        list: NameList,
    ) -> Result<Vec<String>, ApiError> {
        self.phase(traces.list_names(&list, &self.resolved, self.deadline))
            .await
    }

    /// Tenant resolution, the index lookup and table registration, on this
    /// request's budget and bound to its cancellation flag.
    async fn open(
        &self,
        state: &AppState,
        identity: &CallerIdentity,
        index_id: &str,
    ) -> Result<TraceQueryContext, ApiError> {
        self.phase(TraceQueryContext::new(
            state,
            identity,
            index_id,
            &self.cancel,
            TraceRenderBudget {
                max_rows_returned: self.resolved.max_rows_returned,
                max_rows_scanned: self.resolved.max_rows_scanned,
                circuit_breakers: self.resolved.circuit_breakers,
                ceilings: ceilings_from(state.admission.unestimated_reservation_bytes()),
            },
        ))
        .await
    }
}

/// The ceilings one Jaeger request renders against, resolved once in
/// [`TraceRequest::open`] and spent by both phases of a trace search.
#[derive(Debug, Clone, Copy)]
struct TraceRenderBudget {
    /// This REQUEST's already-resolved interactive `max_rows_returned` (#2119).
    /// Still enforced, per query, because an operator who pins it below the
    /// derived ceiling means it.
    max_rows_returned: usize,
    /// The interactive `max_rows_scanned` this surface never read before
    /// #2184: it collected with `df.collect()`, so the mid-flight breaker every
    /// `/api/v1/sql` browse runs under did not exist here.
    max_rows_scanned: usize,
    /// `false` (from `RequestLimits`, which this surface cannot set) would
    /// bypass the wall clock; it never lifts a render ceiling. Kept so the
    /// rows-SCANNED breaker reads the same switch the SQL path reads.
    circuit_breakers: bool,
    /// Derived from the admission reservation this request holds (#2184).
    ceilings: JaegerCeilings,
}

/// What the mid-flight collector had accumulated when it crossed a bound, and
/// what it crossed. One struct because these five travel together from the
/// collector's outcome into the refusal message and nowhere else.
#[derive(Clone, Copy, Debug)]
struct BoundCrossing {
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    crossed: AccumulatedBound,
}

/// Which ceiling produced a row refusal — the message has to say, because the
/// two are moved by different things: `max_rows_returned` by the interactive
/// tier's limits, the derived one by the pod's admission budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowCeiling {
    Interactive,
    Render,
}

/// What a query on this surface renders, and therefore which derived ceiling
/// and which breaker label bound it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenderKind {
    /// Trace-id selection and span retrieval, in span rows.
    Spans,
    /// The two list routes, in distinct names.
    Names,
}

struct TraceQueryContext {
    ctx: SessionContext,
    budget: TraceRenderBudget,
    /// The index's REAL table identity — `namespace.index_id` — and the
    /// generation the registered provider serves. Both exist for the name-list
    /// cache key (#2268) and neither can be recovered from the query text:
    /// every index is registered under the one `traces` alias, so two indexes
    /// in two namespaces issue byte-identical SQL.
    table: String,
    generation: siglake_storage::iceberg::TableGeneration,
    /// `SIGLAKE_QUERY_RESULT_CACHE`, as this tenant's context resolves it. Off
    /// means the poll executes, exactly as a `/api/v1/sql` repeat does.
    result_caches: bool,
    /// Rows and Arrow bytes this REQUEST has already accumulated. A trace
    /// search renders twice — trace ids, then their spans — out of one budget,
    /// so the second phase runs on what the first one left. Atomics rather
    /// than a `Cell` only because the handler future must stay `Send`.
    spent_rows: AtomicUsize,
    spent_bytes: AtomicUsize,
}

impl TraceQueryContext {
    async fn new(
        state: &AppState,
        identity: &CallerIdentity,
        index_id: &str,
        cancel: &siglake_storage::QueryCancel,
        budget: TraceRenderBudget,
    ) -> Result<Self, ApiError> {
        let ice = state
            .resolve_ice(identity)
            .await
            .map_err(ApiError::internal)?;
        let config = ice
            .get_index(index_id)
            .await
            .map_err(ApiError::from_index_manager)?
            .ok_or_else(|| {
                ApiError::new_status(
                    axum::http::StatusCode::NOT_FOUND,
                    format!("index {index_id} not found"),
                )
            })?;
        validate_trace_mapping(&config)?;

        // Per-request cancellation, carried by the session config exactly as
        // `/api/v1/sql` carries it: dropping the request future cancels the
        // future, but the plan's spawned partition pumps keep scanning until
        // this flag ends the source stream they drain from.
        let ctx = {
            let mut hinted = state.query_scan.session_context().state();
            hinted
                .config_mut()
                .set_extension(std::sync::Arc::new(cancel.clone()));
            SessionContext::new_with_state(hinted)
        };
        let table = ice.index_table_ident(index_id);
        // The generation comes back FROM the registration rather than from a
        // second lookup afterwards: the table cache is refreshed on commit and
        // in the background, so a separate `current_table_generation` can
        // answer with a generation newer than the provider just registered, and
        // the name-list cache would file this generation's answer under the
        // next one's key.
        let generation = ice
            .register_table_returning_generation(&ctx, &table, TRACE_TABLE_NAME)
            .await
            .map_err(ApiError::internal)?;
        crate::udfs::register_udfs(&ctx);
        Ok(Self {
            ctx,
            budget,
            table: table.to_string(),
            generation,
            result_caches: ice.result_caches_enabled(),
            spent_rows: AtomicUsize::new(0),
            spent_bytes: AtomicUsize::new(0),
        })
    }

    /// What this phase may still accumulate, and which row ceiling governs it.
    ///
    /// The derived ceilings are per REQUEST and the interactive row cap is per
    /// QUERY (#2119's contract, unchanged), so the bound handed to the
    /// collector is the tighter of "what the interactive cap allows this query"
    /// and "what the request's render budget has left".
    fn accumulation_bound(&self, kind: RenderKind) -> (AccumulationBound, RowCeiling) {
        let ceiling_rows = match kind {
            RenderKind::Spans => self.budget.ceilings.span_rows,
            RenderKind::Names => self.budget.ceilings.names,
        };
        let remaining_rows = ceiling_rows.saturating_sub(self.spent_rows.load(Ordering::Relaxed));
        let remaining_bytes = self
            .budget
            .ceilings
            .render_bytes
            .saturating_sub(self.spent_bytes.load(Ordering::Relaxed));
        let (max_rows, governed_by) = if self.budget.max_rows_returned < remaining_rows {
            (self.budget.max_rows_returned, RowCeiling::Interactive)
        } else {
            (remaining_rows, RowCeiling::Render)
        };
        (
            AccumulationBound::Refuse {
                max_rows,
                max_bytes: remaining_bytes,
            },
            governed_by,
        )
    }

    /// Cap what the plan may PRODUCE at one row past the bound the collector
    /// will refuse over (#2231).
    ///
    /// #2184 bounds accumulation at a batch boundary, which is the last place a
    /// render can be refused — but not the first place the rows exist. The
    /// span-fetch query is `ORDER BY timestamp, span_id` with no `LIMIT`, so
    /// its `SortExec` is BLOCKING: it buffers every matching row before it
    /// emits the first batch, and the collector cannot inspect a bound until
    /// then. Measured on the wide corpus (200 traces x 50 spans x 16 KiB of
    /// attributes), a refused `?limit=200` peaked at 322 MiB against 162.90 MiB
    /// of Arrow for the same rows: the sort's buffer plus the 8,192-row batch
    /// it hands over, neither of which the ceiling could reach.
    ///
    /// A `Limit` on the logical plan is what reaches them. `push_down_limit`
    /// folds it into the `Sort` as a fetch, so the blocking sort becomes a
    /// bounded `TopK` and the producer stops at `max_rows + 1` rows. `+ 1`
    /// because [`AccumulationBound::Refuse`] refuses only STRICTLY over the
    /// bound: exactly at it the result may be complete, and one row past it is
    /// all the evidence a refusal needs. The refusal DECISION is therefore
    /// unchanged — every result this truncates is a result the collector
    /// refuses whole — and a complete result (`rows <= max_rows`) is never
    /// touched, which is why the positive controls do not move.
    ///
    /// Where the query already carries a `LIMIT` (`find_trace_ids` splices the
    /// client's `?limit=`, itself refused above the trace ceiling), the
    /// optimizer keeps the tighter of the two.
    ///
    /// NOT a byte bound. It bounds produced ROWS, so one oversized row — a span
    /// carrying megabytes of attributes — still arrives whole, and the byte
    /// ceiling remains the thing that refuses it. It also says nothing about
    /// how many batches are buffered concurrently: partitions, the `TopK`'s
    /// retained input batches and any `CoalesceBatchesExec` all hold their own,
    /// so this shrinks the overshoot rather than capping it.
    fn bound_the_producer(
        &self,
        df: DataFrame,
        bound: AccumulationBound,
    ) -> Result<DataFrame, ApiError> {
        let AccumulationBound::Refuse { max_rows, .. } = bound else {
            // This surface only ever refuses; a truncating bound here would
            // mean a partial trace, which #2119 settled it must never return.
            return Ok(df);
        };
        // A budget that bounds nothing gets no fetch. Two reasons, and the
        // second is the load-bearing one: a `LIMIT` is an `i64` in the logical
        // plan, so `usize::MAX + 1` is not expressible; and `SortExec` with a
        // fetch is a `TopK`, which holds its rows in memory, while the
        // `ExternalSorter` it replaces can SPILL. That trade is right when the
        // fetch is small — a render that large is refused either way, and
        // spilling 158 MiB to disk to then refuse it is the worst of both —
        // and wrong when the fetch bounds nothing.
        let Some(fetch) = max_rows
            .checked_add(1)
            .filter(|fetch| i64::try_from(*fetch).is_ok())
        else {
            return Ok(df);
        };
        df.limit(0, Some(fetch))
            .map_err(|e| ApiError::from_execution(anyhow::Error::new(e), Priority::Interactive))
    }

    /// 413 for a render this request's budget cannot hold, with the number that
    /// refused it and the number it was refused against (#2184).
    ///
    /// `rows`/`bytes` are what the collector had accumulated when the bound was
    /// crossed, not the size of the whole result, which is never accumulated.
    /// Since #2231 the producer stops at `max_rows + 1`, so `rows` is exactly
    /// one past the row bound; `bytes` is whatever those rows WEIGH, which one
    /// wide span can put far over the byte bound. The message says "at least"
    /// rather than implying an exact total either way.
    fn refuse_over_render_ceiling(
        &self,
        kind: RenderKind,
        governed_by: RowCeiling,
        crossing: BoundCrossing,
    ) -> ApiError {
        let BoundCrossing {
            rows,
            bytes,
            max_rows,
            max_bytes,
            crossed,
        } = crossing;
        let budget = self.budget.ceilings.render_budget_bytes;
        let msg = match (crossed, governed_by) {
            (AccumulatedBound::Bytes, _) => {
                count_breaker(JaegerBreaker::RenderBytes);
                format!(
                    "trace query rendered at least {bytes} bytes of Arrow data, over this \
                     request's remaining render budget of {max_bytes} bytes (derived from the \
                     {budget}-byte admission reservation a trace read holds: one span can carry \
                     megabytes of attributes, so no row count bounds this). The Jaeger API \
                     cannot express a partial trace, so nothing is returned — narrow the \
                     request (service, operation, time window, tags, a smaller `limit`) or \
                     raise the pod's query admission budget"
                )
            }
            (AccumulatedBound::Rows, RowCeiling::Interactive) => {
                count_breaker(kind.row_breaker());
                format!(
                    "trace query matched at least {rows} rows, exceeding this server's \
                     interactive row cap of {max_rows}; the Jaeger API cannot express a partial \
                     trace, so nothing is returned — narrow the search (service, operation, \
                     time window, tags) or lower `limit`"
                )
            }
            (AccumulatedBound::Rows, RowCeiling::Render) => {
                count_breaker(kind.row_breaker());
                let unit = kind.row_unit();
                let whole = match kind {
                    RenderKind::Spans => self.budget.ceilings.span_rows,
                    RenderKind::Names => self.budget.ceilings.names,
                };
                format!(
                    "trace query rendered at least {rows} rows, over this request's remaining \
                     render budget of {max_rows} {unit} ({whole} per request, shared by trace \
                     selection and span retrieval, derived from the {budget}-byte admission \
                     reservation a trace read holds). The Jaeger API cannot express a partial \
                     trace, so nothing is returned — narrow the request (service, operation, \
                     time window, tags, a smaller `limit`) or raise the pod's query admission \
                     budget"
                )
            }
        };
        ApiError::new_status(axum::http::StatusCode::PAYLOAD_TOO_LARGE, msg)
    }

    /// Every trace query goes through here, and every one of them is a template
    /// with request text spliced into it (`find_trace_ids`,
    /// `fetch_spans_for_trace_ids`). So it plans READ-ONLY, like `/api/v1/sql`
    /// (#1523) and the compactor's delete-task predicate (#1543): the escaping
    /// in `sql_string_literal` is the first line of defence, not the last one.
    ///
    /// Planning and execution failures go through the shared capacity mapper
    /// (#2095), not straight to `internal`: these queries run on the same
    /// process-wide memory pool as `/api/v1/sql`, so a `ResourcesExhausted`
    /// here means exactly what it means there — wait and retry (503 +
    /// `Retry-After`), counted under
    /// `siglake_query_breaker_trips_total{breaker="pool_exhausted"}` so the
    /// operator signal that explains the refusal is not missing on this
    /// surface. Everything else, including the read-only planner's refusals,
    /// is still 500. Interactive priority because these routes serve the
    /// Jaeger UI: there is no batch path here to retry on.
    ///
    /// The render ceilings are enforced MID-FLIGHT, at a batch boundary, before
    /// any JSON is built (#2184).
    ///
    /// This was `df.collect()` plus a row count afterwards (#2119): the whole
    /// result was already in memory when the one ceiling this surface read was
    /// checked, and that ceiling was in the wrong unit — measured 2026-09-08, a
    /// `?limit=200` search over traces carrying 16 KiB of attributes per span
    /// is 10,000 span rows, 0.1% of the 10,000,000-row interactive cap, and
    /// peaks at 643 MiB. So the collect goes through the same mid-flight
    /// collector `/api/v1/sql` uses, with a bound in BOTH units and
    /// [`AccumulationBound::Refuse`] semantics rather than the SQL path's
    /// truncation: over either bound nothing is returned.
    ///
    /// Going through that collector is also what brings this surface the
    /// interactive `ceiling_rows_scanned` breaker, the exec pool's slot
    /// accounting and cancellation, and the scan-attribution settle — three
    /// things a bare `df.collect()` had none of.
    async fn query_records(
        &self,
        sql: &str,
        kind: RenderKind,
    ) -> Result<crate::format::RecordsResponse, ApiError> {
        let df = crate::sql::plan_client_sql(&self.ctx, sql)
            .await
            .map_err(|e| ApiError::from_execution(anyhow::Error::new(e), Priority::Interactive))?;
        let (bound, governed_by) = self.accumulation_bound(kind);
        let df = self.bound_the_producer(df, bound)?;
        let max_rows_scanned = if self.budget.circuit_breakers {
            self.budget.max_rows_scanned
        } else {
            usize::MAX
        };
        let outcome = crate::midflight::collect_with_bounds(
            df,
            max_rows_scanned,
            bound,
            Priority::Interactive,
        )
        .await
        .map_err(|e| ApiError::from_execution(e, Priority::Interactive))?;
        let batches = match outcome {
            CollectOutcome::Ok(batches) => batches,
            // The interactive mid-flight breaker, counted by the collector
            // under `midflight_rows_scanned` exactly as it is for SQL. Refused
            // whole, like every other ceiling here.
            CollectOutcome::RowsScannedExceeded {
                rows_scanned,
                limit,
                ..
            } => {
                return Err(ApiError::new_status(
                    axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                    format!(
                        "trace query scanned {rows_scanned} rows, exceeding this server's \
                         interactive mid-flight limit of {limit}; the Jaeger API cannot express \
                         a partial trace, so nothing is returned — narrow the search (service, \
                         operation, time window, tags) or lower `limit`"
                    ),
                ))
            }
            CollectOutcome::AccumulatedBoundExceeded {
                rows,
                bytes,
                max_rows,
                max_bytes,
                bound,
            } => {
                return Err(self.refuse_over_render_ceiling(
                    kind,
                    governed_by,
                    BoundCrossing {
                        rows,
                        bytes,
                        max_rows,
                        max_bytes,
                        crossed: bound,
                    },
                ))
            }
        };
        // Spend what this phase rendered, so a trace search's span retrieval
        // runs on what its trace selection LEFT of one request's budget.
        self.spent_rows.fetch_add(
            batches.iter().map(|batch| batch.num_rows()).sum(),
            Ordering::Relaxed,
        );
        self.spent_bytes.fetch_add(
            batches
                .iter()
                .map(|batch| batch.get_array_memory_size())
                .sum(),
            Ordering::Relaxed,
        );
        // `Some(cap)` cannot truncate after the refusals above; it is passed so
        // the render carries the same ceiling the SQL surface passes it, and a
        // future caller of this helper inherits the bound rather than `None`.
        batches_to_records(&batches, Some(self.budget.max_rows_returned))
            .map_err(ApiError::internal)
    }

    async fn query_rows(
        &self,
        sql: &str,
        kind: RenderKind,
    ) -> Result<Vec<Map<String, Value>>, ApiError> {
        rows_of(&self.query_records(sql, kind).await?)
    }

    /// This context's key for `list`, or `None` when this poll has no business
    /// in the cache: result caches are off, or the index has no snapshot yet
    /// and there is nothing for an entry to be a pure function OF (the SQL
    /// path skips a generation with no snapshot id the same way).
    fn name_cache_key(&self, list: &NameList) -> Option<String> {
        if !self.result_caches {
            return None;
        }
        Some(name_cache_key(
            &self.table,
            self.generation.snapshot_id?,
            self.generation.schema_id,
            &self.budget,
            list,
        ))
    }

    /// One name list, from the result cache when the snapshot has already
    /// answered this question and from the aggregate otherwise (#2268).
    ///
    /// Nothing about the refusals moves. The bounds are read from the same
    /// `accumulation_bound` on the miss path, and a hit is a body that already
    /// passed them under the same ceilings (they are in the key), so it is a
    /// complete answer this request would have been given anyway. A refusal or
    /// an error returns from `query_records` WITHOUT reaching the insert: a
    /// 413, a 504 and a dropped client all leave the leader's context to its
    /// `Drop`, which removes the single-flight marker and wakes the waiters, so
    /// the next poll pays a miss rather than parking on a leader that is gone.
    /// A partial list is never cached because a partial list is never returned.
    async fn list_names(
        &self,
        list: &NameList,
        resolved: &ResolvedLimits,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<String>, ApiError> {
        let sql = list.sql();
        let Some(key) = self.name_cache_key(list) else {
            return names_of(&self.query_records(&sql, RenderKind::Names).await?);
        };
        let leader = match crate::sql::prepare_name_list_cache(key, resolved, deadline).await {
            // The stored arena, split back into names outside the cache mutex
            // (#2302). This is the extracted OUTPUT of `names_of`, so order,
            // filtering and content are what this request would have rendered
            // by construction rather than by replay.
            crate::sql::SqlResultCacheDecision::Hit(body) => return Ok(body.expect_names()),
            crate::sql::SqlResultCacheDecision::Skip => None,
            crate::sql::SqlResultCacheDecision::Leader(ctx) => Some(ctx),
        };
        let records = self.query_records(&sql, RenderKind::Names).await?;
        let names = names_of(&records)?;
        drop(records);
        if let Some(leader) = leader {
            // Stored as an ARENA under this caller's own byte allowance
            // (#2302), not as the render under SQL's 128-row gate: the render
            // holds ~670 bytes per name (a `serde_json::Map` row is a
            // `BTreeMap` node allocated whole), and a row count prices a
            // one-column list of short strings wrong by two orders of
            // magnitude. Rows are bounded upstream instead, by
            // `ceilings.names`, which is in the key. A list past the byte
            // allowance is still simply not stored — never truncated to fit,
            // and never a reason to raise the caps that bound the SQL cache
            // too.
            crate::sql::finish_result_cache(
                leader,
                crate::sql::CacheEligibility::NAME_LIST,
                crate::sql::CachedBody::names(&names),
            )
            .await;
        }
        Ok(names)
    }

    async fn find_trace_ids(&self, params: &FindTracesParams) -> Result<Vec<String>, ApiError> {
        let limit = params.limit.unwrap_or(20).max(1);
        let mut filters = vec![
            "trace_id IS NOT NULL".to_string(),
            "trace_id != ''".to_string(),
        ];
        if let Some(service) = params.service.as_deref().filter(|value| !value.is_empty()) {
            filters.push(format!("service = {}", sql_string_literal(service)));
        }
        if let Some(operation) = params
            .operation
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            filters.push(format!("name = {}", sql_string_literal(operation)));
        }
        if let Some(start) = params.start {
            filters.push(format!("timestamp >= to_timestamp_micros({start})"));
        }
        if let Some(end) = params.end {
            filters.push(format!("timestamp <= to_timestamp_micros({end})"));
        }
        if let Some(min_duration) = params
            .min_duration
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            filters.push(format!(
                "duration_nanos >= {}",
                parse_duration_nanos(min_duration)?
            ));
        }
        if let Some(max_duration) = params
            .max_duration
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            filters.push(format!(
                "duration_nanos <= {}",
                parse_duration_nanos(max_duration)?
            ));
        }
        if let Some(tags) = params.tags.as_deref().filter(|value| !value.is_empty()) {
            for (key, value) in parse_tags(tags)? {
                filters.push(format!(
                    "attr_get(attributes, {}) = {}",
                    sql_string_literal(&key),
                    sql_string_literal(&value)
                ));
            }
        }

        let sql = format!(
            "SELECT trace_id, MAX(timestamp) AS newest \
             FROM traces \
             WHERE {} \
             GROUP BY trace_id \
             ORDER BY newest DESC \
             LIMIT {}",
            filters.join(" AND "),
            limit
        );
        let rows = self.query_rows(&sql, RenderKind::Spans).await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                row.get("trace_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect())
    }

    async fn fetch_spans_for_trace_ids(
        &self,
        trace_ids: &[String],
    ) -> Result<Vec<Map<String, Value>>, ApiError> {
        if trace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let in_list = trace_ids
            .iter()
            .map(|trace_id| sql_string_literal(trace_id))
            .collect::<Vec<_>>()
            .join(", ");
        self.query_rows(
            format!(
                "SELECT timestamp, trace_id, span_id, parent_span_id, service, name, kind, \
                 status_code, duration_nanos, attributes \
                 FROM traces \
                 WHERE trace_id IN ({in_list}) \
                 ORDER BY timestamp ASC, span_id ASC"
            )
            .as_str(),
            RenderKind::Spans,
        )
        .await
    }
}

/// The result-cache key of one name-list poll (#2268).
///
/// What is in it, and why each part has to be:
///
///   - the RESOLVED table (`namespace.index_id`), never the `traces` alias —
///     the alias is the same string for every index of every tenant, so keying
///     on the SQL text would serve one index's services as another's;
///   - the generation the registered provider serves — its snapshot, which is
///     what makes the entry snapshot-keyed and commit-invalidated rather than
///     aged out (standing invariant: result caches are never TTL-expired), and
///     its schema id, because an additive `migrate-schema` moves the served
///     schema WITHOUT committing a data snapshot (#2494). A name list reads one
///     column, so a widen does not change the answer it would give today; keyed
///     anyway, for the same reason the ceilings are — the SQL twin's identity is
///     a whole `TableGeneration` and a name key that agreed with it only by
///     accident is a key that stops agreeing when the render changes;
///   - the list, and for operations the exact service, since an index has one
///     service list and one operation list PER service;
///   - the ceilings the answer was rendered under, so an entry produced when
///     more was allowed can never be replayed to a request that would have been
///     refused. They are process-wide today (derived from the admission budget,
///     plus the interactive row cap); keyed anyway, because "they happen not to
///     vary" is not something a cache key should rest on.
///
/// It cannot collide with a `/api/v1/sql` key. Those are
/// `{namespace}|snapshot={id}|…`; the second field here is the literal `v1`,
/// which no snapshot id spells.
fn name_cache_key(
    table: &str,
    snapshot_id: i64,
    schema_id: i32,
    budget: &TraceRenderBudget,
    list: &NameList,
) -> String {
    format!(
        "jaeger-names|v1|table={}|snapshot={}|schema={}|rows={}|names={}|render_bytes={}|{}",
        key_field(table),
        snapshot_id,
        schema_id,
        budget.max_rows_returned,
        budget.ceilings.names,
        budget.ceilings.render_bytes,
        list.key_fragment(),
    )
}

/// The rendered rows of a trace query, as JSON objects.
fn rows_of(records: &crate::format::RecordsResponse) -> Result<Vec<Map<String, Value>>, ApiError> {
    records
        .rows
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or_else(|| ApiError::internal("internal traces query returned non-object row"))
        })
        .collect()
}

/// The names of a one-column `SELECT DISTINCT` render, in the order the query
/// produced them.
///
/// Reads the RENDERED body rather than the Arrow batches, so a name list
/// replayed from the cache is built by the same code that built it the first
/// time and cannot answer with a differently ordered or differently filtered
/// list.
fn names_of(records: &crate::format::RecordsResponse) -> Result<Vec<String>, ApiError> {
    Ok(rows_of(records)?
        .into_iter()
        .filter_map(|row| row.into_iter().next().map(|(_, value)| value))
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect())
}

/// Which Jaeger ceiling refused a request, as
/// `siglake_query_breaker_trips_total{breaker=…}` (#2184).
///
/// Response STATUSES are already counted by #2102; this is the other half an
/// operator needs — WHICH ceiling, so "the trace UI is refusing" turns into
/// "lower `limit`" or "the pod's admission budget is too small" without reading
/// a client's error text. Deliberately not `pool_exhausted`: these are policy
/// refusals of a request that fits the pod, not the pool running out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JaegerBreaker {
    TraceLimit,
    SpanRows,
    RenderBytes,
    NameRows,
}

/// One literal label set per breaker, because `scripts/check-chart.py` reads
/// these call sites to verify every series is pre-registered in
/// `siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS` (a series that only
/// appears at its first increment is invisible on a fresh pod). `interactive`
/// is a constant here, not `priority.label()`: this surface has no batch tier.
fn count_breaker(breaker: JaegerBreaker) {
    match breaker {
        JaegerBreaker::TraceLimit => metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "jaeger_trace_limit",
            "priority" => "interactive",
        )
        .increment(1),
        JaegerBreaker::SpanRows => metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "jaeger_span_rows",
            "priority" => "interactive",
        )
        .increment(1),
        JaegerBreaker::RenderBytes => metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "jaeger_render_bytes",
            "priority" => "interactive",
        )
        .increment(1),
        JaegerBreaker::NameRows => metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "jaeger_name_rows",
            "priority" => "interactive",
        )
        .increment(1),
    }
}

impl RenderKind {
    fn row_breaker(self) -> JaegerBreaker {
        match self {
            RenderKind::Spans => JaegerBreaker::SpanRows,
            RenderKind::Names => JaegerBreaker::NameRows,
        }
    }

    fn row_unit(self) -> &'static str {
        match self {
            RenderKind::Spans => "span rows",
            RenderKind::Names => "names",
        }
    }
}

/// Refuse a `?limit=` above this pod's derived trace ceiling with 400, before
/// anything executes (#2184).
///
/// 400, not 413. The request is refused on its face: the answer does not depend
/// on what is stored or on what the pod is busy with, so it must not carry
/// retry semantics. 413 on this surface already means "your RESULT was too
/// big" and tells the client to narrow the search — the wrong advice when the
/// fix is to lower `limit`. axum already answers 400 for `limit=not-a-number`,
/// so "an unusable `limit`" keeps one status.
///
/// Takes the whole [`AppState`] rather than a resolved ceiling so the call site
/// cannot accidentally be moved after [`TraceRequest::begin`]: the ceiling is
/// read from admission WITHOUT reserving anything.
fn refuse_over_trace_ceiling(limit: Option<usize>, state: &AppState) -> Result<(), ApiError> {
    let Some(requested) = limit else {
        return Ok(());
    };
    let ceilings = ceilings_from(state.admission.unestimated_reservation_bytes());
    if requested <= ceilings.traces {
        return Ok(());
    }
    count_breaker(JaegerBreaker::TraceLimit);
    Err(ApiError::bad_request(format!(
        "`limit={requested}` exceeds this server's Jaeger trace ceiling of {} traces, \
         derived from the {}-byte admission reservation a trace read holds (a trace costs \
         at least a selection row and a span of this request's render budget). Refused \
         before the search runs: lower `limit` — this API's default is 20 — or raise the \
         pod's query admission budget",
        ceilings.traces, ceilings.render_budget_bytes
    )))
}

fn validate_trace_mapping(config: &IndexConfig) -> Result<(), ApiError> {
    let fields: HashMap<&str, &FieldType> = config
        .doc_mapping
        .field_mappings
        .iter()
        .map(|field| (field.name.as_str(), &field.field_type))
        .collect();
    let mut missing = Vec::new();
    for (name, expected) in REQUIRED_TRACE_COLUMNS {
        let Some(actual) = fields.get(name) else {
            missing.push((*name).to_string());
            continue;
        };
        if !field_type_matches(actual, *expected) {
            missing.push((*name).to_string());
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "index `{}` is not a traces index; missing or incompatible columns: {}",
            config.index_id,
            missing.join(", ")
        )))
    }
}

fn field_type_matches(actual: &FieldType, expected: ExpectedFieldType) -> bool {
    matches!(
        (actual, expected),
        (FieldType::Text { .. }, ExpectedFieldType::Text)
            | (FieldType::Long, ExpectedFieldType::Long)
            | (FieldType::Datetime, ExpectedFieldType::Datetime)
    )
}

fn parse_tags(raw: &str) -> Result<Vec<(String, String)>, ApiError> {
    let value: Value = serde_json::from_str(raw)
        .map_err(|e| ApiError::bad_request(format!("invalid tags JSON: {e}")))?;
    let object = value.as_object().ok_or_else(|| {
        ApiError::bad_request("invalid tags JSON: expected an object of key/value filters")
    })?;
    Ok(object
        .iter()
        .map(|(key, value)| (key.clone(), json_scalar_to_filter_string(value)))
        .collect())
}

fn json_scalar_to_filter_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn parse_duration_nanos(raw: &str) -> Result<i64, ApiError> {
    let raw = raw.trim();
    let (number, multiplier) = if let Some(value) = raw.strip_suffix("ns") {
        (value, 1_f64)
    } else if let Some(value) = raw.strip_suffix("us") {
        (value, 1_000_f64)
    } else if let Some(value) = raw.strip_suffix("µs").or_else(|| raw.strip_suffix("μs")) {
        (value, 1_000_f64)
    } else if let Some(value) = raw.strip_suffix("ms") {
        (value, 1_000_000_f64)
    } else if let Some(value) = raw.strip_suffix('s') {
        (value, 1_000_000_000_f64)
    } else if let Some(value) = raw.strip_suffix('m') {
        (value, 60_f64 * 1_000_000_000_f64)
    } else if let Some(value) = raw.strip_suffix('h') {
        (value, 60_f64 * 60_f64 * 1_000_000_000_f64)
    } else {
        (raw, 1_000_f64)
    };
    let value = number
        .parse::<f64>()
        .map_err(|e| ApiError::bad_request(format!("invalid duration `{raw}`: {e}")))?;
    let nanos = value * multiplier;
    if !nanos.is_finite() || nanos < 0_f64 || nanos > i64::MAX as f64 {
        return Err(ApiError::bad_request(format!("invalid duration `{raw}`")));
    }
    Ok(nanos.round() as i64)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn build_jaeger_traces(
    rows: Vec<Map<String, Value>>,
    trace_order: &[String],
) -> Result<Vec<Value>, ApiError> {
    let mut grouped: BTreeMap<String, Vec<Map<String, Value>>> = BTreeMap::new();
    for row in rows {
        let trace_id = row
            .get("trace_id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::internal("trace row missing trace_id"))?
            .to_string();
        grouped.entry(trace_id).or_default().push(row);
    }

    let mut traces = Vec::new();
    for trace_id in trace_order {
        let Some(rows) = grouped.remove(trace_id) else {
            continue;
        };
        traces.push(build_jaeger_trace(trace_id, rows)?);
    }
    Ok(traces)
}

fn build_jaeger_trace(trace_id: &str, rows: Vec<Map<String, Value>>) -> Result<Value, ApiError> {
    let mut process_ids = HashMap::new();
    let mut processes = Map::new();
    let mut spans = Vec::with_capacity(rows.len());

    for row in rows {
        let service = row
            .get("service")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown")
            .to_string();
        let next_process_id = process_ids.len() + 1;
        let process_id = process_ids
            .entry(service.clone())
            .or_insert_with(|| format!("p{next_process_id}"))
            .clone();
        processes.entry(process_id.clone()).or_insert_with(|| {
            json!({
                "serviceName": service,
                "tags": [],
            })
        });
        spans.push(build_jaeger_span(trace_id, &process_id, &row)?);
    }

    Ok(json!({
        "traceID": trace_id,
        "spans": spans,
        "processes": Value::Object(processes),
    }))
}

fn build_jaeger_span(
    trace_id: &str,
    process_id: &str,
    row: &Map<String, Value>,
) -> Result<Value, ApiError> {
    let span_id = required_string(row, "span_id")?;
    let parent_span_id = row
        .get("parent_span_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let references = if parent_span_id.is_empty() {
        Vec::new()
    } else {
        vec![json!({
            "refType": "CHILD_OF",
            "traceID": trace_id,
            "spanID": parent_span_id,
        })]
    };
    let attributes = row
        .get("attributes")
        .and_then(Value::as_str)
        .map(parse_attributes_object)
        .transpose()?
        .unwrap_or_default();
    Ok(json!({
        "traceID": trace_id,
        "spanID": span_id,
        "operationName": required_string(row, "name")?,
        "references": references,
        "startTime": timestamp_micros(required_string(row, "timestamp")?)?,
        "duration": row.get("duration_nanos").and_then(Value::as_i64).unwrap_or_default() / 1_000,
        "processID": process_id,
        "tags": residual_tags(&attributes),
        "logs": event_logs(&attributes),
    }))
}

fn required_string<'a>(row: &'a Map<String, Value>, key: &str) -> Result<&'a str, ApiError> {
    row.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::internal(format!("trace row missing `{key}`")))
}

fn parse_attributes_object(raw: &str) -> Result<Map<String, Value>, ApiError> {
    serde_json::from_str(raw)
        .map_err(|e| ApiError::internal(format!("parse trace attributes: {e}")))
}

fn residual_tags(attributes: &Map<String, Value>) -> Vec<Value> {
    let mut tags = attributes
        .iter()
        .filter(|(key, _)| *key != "events" && *key != "links")
        .filter_map(|(key, value)| {
            value.as_str().map(|value| {
                json!({
                    "key": key,
                    "type": "string",
                    "value": value,
                })
            })
        })
        .collect::<Vec<_>>();
    tags.sort_by(|left, right| left["key"].as_str().cmp(&right["key"].as_str()));
    tags
}

fn event_logs(attributes: &Map<String, Value>) -> Vec<Value> {
    let Some(events) = attributes.get("events").and_then(Value::as_array) else {
        return Vec::new();
    };
    events.iter().filter_map(event_log_entry).collect()
}

fn event_log_entry(event: &Value) -> Option<Value> {
    let object = event.as_object()?;
    let timestamp = otlp_event_time_micros(object.get("timeUnixNano")?)?;
    let mut fields = Vec::new();
    if let Some(name) = object.get("name").and_then(Value::as_str) {
        fields.push(json!({
            "key": "event",
            "type": "string",
            "value": name,
        }));
    }
    if let Some(attributes) = object.get("attributes").and_then(Value::as_array) {
        for attribute in attributes {
            let Some(attr) = attribute.as_object() else {
                continue;
            };
            let Some(key) = attr.get("key").and_then(Value::as_str) else {
                continue;
            };
            let Some(value) = attr.get("value").and_then(proto_json_value_to_string) else {
                continue;
            };
            fields.push(json!({
                "key": key,
                "type": "string",
                "value": value,
            }));
        }
    }
    Some(json!({
        "timestamp": timestamp,
        "fields": fields,
    }))
}

fn proto_json_value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Object(object) => {
            if let Some(value) = object.get("stringValue").and_then(Value::as_str) {
                return Some(value.to_string());
            }
            if let Some(value) = object.get("intValue") {
                return proto_json_value_to_string(value);
            }
            if let Some(value) = object.get("doubleValue") {
                return proto_json_value_to_string(value);
            }
            if let Some(value) = object.get("boolValue") {
                return proto_json_value_to_string(value);
            }
            if let Some(value) = object.get("bytesValue") {
                return proto_json_value_to_string(value);
            }
            None
        }
        Value::Array(_) | Value::Null => None,
    }
}

fn otlp_event_time_micros(value: &Value) -> Option<i64> {
    match value {
        Value::String(value) => value.parse::<i64>().ok().map(|value| value / 1_000),
        Value::Number(value) => value.as_i64().map(|value| value / 1_000),
        _ => None,
    }
}

fn timestamp_micros(raw: &str) -> Result<i64, ApiError> {
    let timestamp = DateTime::parse_from_rfc3339(raw)
        .map_err(|e| ApiError::internal(format!("parse trace timestamp `{raw}`: {e}")))?;
    Ok(timestamp.with_timezone(&Utc).timestamp_micros())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request, StatusCode};
    use opentelemetry_proto::tonic::common::v1::{
        any_value::Value as ProtoValue, AnyValue as ProtoAnyValue, KeyValue as ProtoKeyValue,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
    use opentelemetry_proto::tonic::trace::v1::{
        span, ResourceSpans as ProtoResourceSpans, ScopeSpans as ProtoScopeSpans, Span as ProtoSpan,
    };
    use siglake_core::index_config::IndexConfig;
    use siglake_core::{events_to_record_batch, map_carrier_batch};
    use siglake_ingest::otlp_traces::otlp_proto_traces_to_events;
    use siglake_storage::iceberg::IcebergContext;
    use siglake_storage::index_manager::builtin_traces_template;
    use tower::util::ServiceExt;

    use super::*;
    use crate::ServerLimits;

    /// Like [`request_json`] but tolerant of a non-JSON body: axum's own
    /// `Query`/`Path` rejections are plain text, and the parameter-typing arms
    /// below assert on the status, not the shape.
    async fn request_text(app: &axum::Router, path: &str) -> (StatusCode, String) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&body).to_string())
    }

    async fn request_json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    async fn build_state() -> (crate::AppState, Arc<IcebergContext>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let state = crate::AppState::new(ice.clone(), crate::AuthConfig::open());
        (state, ice, tmp)
    }

    async fn build_server() -> (axum::Router, Arc<IcebergContext>, tempfile::TempDir) {
        let (state, ice, tmp) = build_state().await;
        (crate::router(state), ice, tmp)
    }

    async fn append_trace_fixture(ice: &IcebergContext, index_id: &str) {
        let config = IndexConfig {
            index_id: index_id.to_string(),
            doc_mapping: builtin_traces_template().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        ice.create_index(&config).await.unwrap();

        let events = otlp_proto_traces_to_events(proto_fixture());
        let carrier = events_to_record_batch(&events).unwrap();
        let mapped = map_carrier_batch(&carrier, &config).unwrap();
        let bloom_refs = config
            .doc_mapping
            .tag_fields
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        ice.append_to_table(&ice.index_table_ident(index_id), mapped, &bloom_refs)
            .await
            .unwrap();
    }

    fn proto_fixture() -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest
    {
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
            resource_spans: vec![ProtoResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![
                        proto_str("host.name", "trace-host"),
                        proto_str("service.name", "checkout"),
                        proto_str("deployment.environment", "prod"),
                    ],
                    ..Default::default()
                }),
                scope_spans: vec![ProtoScopeSpans {
                    spans: vec![
                        ProtoSpan {
                            trace_id: vec![
                                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa,
                                0xbb, 0xcc, 0xdd, 0xee, 0xff,
                            ],
                            span_id: vec![0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17],
                            name: "checkout".to_string(),
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: 1_700_000_000_000_000_000,
                            end_time_unix_nano: 1_700_000_000_050_000_000,
                            attributes: vec![proto_str("http.method", "GET")],
                            events: vec![span::Event {
                                time_unix_nano: 1_700_000_000_010_000_000,
                                name: "exception".to_string(),
                                attributes: vec![proto_str("exception.type", "IOError")],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ProtoSpan {
                            trace_id: vec![
                                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa,
                                0xbb, 0xcc, 0xdd, 0xee, 0xff,
                            ],
                            span_id: vec![0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27],
                            parent_span_id: vec![0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17],
                            name: "db".to_string(),
                            kind: span::SpanKind::Client as i32,
                            start_time_unix_nano: 1_700_000_000_020_000_000,
                            end_time_unix_nano: 1_700_000_000_030_000_000,
                            attributes: vec![proto_str("db.system", "postgres")],
                            ..Default::default()
                        },
                        ProtoSpan {
                            trace_id: vec![
                                0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44,
                                0x55, 0x66, 0x77, 0x88, 0x99,
                            ],
                            span_id: vec![0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37],
                            name: "inventory".to_string(),
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: 1_700_000_100_000_000_000,
                            end_time_unix_nano: 1_700_000_100_005_000_000,
                            attributes: vec![proto_str("http.method", "POST")],
                            links: vec![span::Link {
                                trace_id: vec![
                                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
                                    0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
                                ],
                                span_id: vec![0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27],
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
        ProtoKeyValue {
            key: key.to_string(),
            value: Some(ProtoAnyValue {
                value: Some(ProtoValue::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn jaeger_endpoints_round_trip_services_operations_find_and_get() {
        let (app, ice, _tmp) = build_server().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;

        let (status, body) =
            request_json(&app, "/api/v1/jaeger/siglake-traces-default/api/services").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], json!(["checkout"]));
        assert_eq!(body["total"], 1);

        let (status, body) = request_json(
            &app,
            "/api/v1/jaeger/siglake-traces-default/api/services/checkout/operations",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], json!(["checkout", "db", "inventory"]));

        let (status, body) = request_json(
            &app,
            "/api/v1/jaeger/siglake-traces-default/api/traces?service=checkout&tags=%7B%22http.method%22%3A%22GET%22%7D&minDuration=40ms",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 1);
        assert_eq!(
            body["data"][0]["traceID"],
            "00112233445566778899aabbccddeeff"
        );
        assert_eq!(body["data"][0]["spans"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["data"][0]["spans"][0]["logs"][0]["fields"][0]["value"],
            "exception"
        );

        let (status, body) = request_json(
            &app,
            "/api/v1/jaeger/siglake-traces-default/api/traces/00112233445566778899aabbccddeeff",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 1);
        assert_eq!(
            body["data"][0]["spans"][1]["references"][0]["spanID"],
            "1011121314151617"
        );
    }

    /// Every regular file under `dir`, recursively — the same witness
    /// `tests/sql_read_only_boundary.rs` uses.
    fn files_under(dir: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(files_under(&path));
            } else {
                out.push(path.display().to_string());
            }
        }
        out.sort();
        out
    }

    /// `TraceQueryContext::query_rows` plans READ-ONLY (task #1693).
    ///
    /// The Jaeger routes take no SQL from the caller, but every string they
    /// plan is a template with request text spliced into it, and they were the
    /// last such site left on DataFusion's permissive default after #1523
    /// (`/api/v1/sql`) and #1543 (the compactor's delete-task predicate). No
    /// escape through `find_trace_ids`'s `WHERE` list has been demonstrated:
    /// `sql_string_literal` doubles quotes and DataFusion refuses more than one
    /// statement per `sql()` call, so a fragment there cannot start a statement.
    /// This pins the planner as the boundary instead of the escaper.
    ///
    /// A/B, so the options are shown to do work rather than merely be spelled:
    /// the first arm drives the same `COPY` through plain `ctx.sql` on the very
    /// same context and asserts it DOES write the file. Both halves fail if
    /// either `with_allow_*` is dropped from `sql::read_only_sql`.
    #[tokio::test]
    async fn jaeger_trace_sql_is_planned_read_only() {
        let (state, ice, tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            // Out of the way: this arm is about the planner, not the ceilings.
            unbounded_budget(),
        )
        .await
        .unwrap();

        // Outside the warehouse, and it must stay empty for the read-only half.
        // `readable/` exists so the CREATE EXTERNAL TABLE case names a path that
        // WOULD have worked: the refusal has to come from the policy.
        let target = tmp.path().join("outside");
        let readable = target.join("readable");
        std::fs::create_dir_all(&readable).unwrap();
        let dest = |name: &str| target.join(name).display().to_string();

        // A: the permissive default this call site used to run under really does
        // write the filesystem from a `traces` query.
        let ab_path = target.join("ab.parquet");
        traces
            .ctx
            .sql(&format!(
                "COPY (SELECT trace_id FROM traces) TO '{}'",
                ab_path.display()
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(
            ab_path.exists(),
            "the A arm wrote nothing, so the B arm below proves nothing"
        );
        std::fs::remove_file(&ab_path).unwrap();
        assert_eq!(files_under(&target), Vec::<String>::new());

        // B: through `query_rows`, every one of those shapes is refused and
        // nothing lands. Only spellings BOTH sqlparser and DataFusion accept,
        // for the reason recorded in `tests/sql_read_only_boundary.rs`.
        let hostile: Vec<(&str, String)> = vec![
            (
                "copy_traces_to_parquet",
                format!(
                    "COPY (SELECT trace_id FROM traces) TO '{}'",
                    dest("spans.parquet")
                ),
            ),
            (
                "copy_literal_to_csv",
                format!("COPY (SELECT 1 AS a) TO '{}'", dest("literal.csv")),
            ),
            (
                "explain_analyze_copy",
                format!(
                    "EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{}'",
                    dest("analyze.parquet")
                ),
            ),
            (
                "create_external_table",
                format!(
                    "CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '{}'",
                    readable.display()
                ),
            ),
            (
                "insert_into_traces",
                "INSERT INTO traces SELECT * FROM traces".to_string(),
            ),
            (
                "create_table_as",
                "CREATE TABLE t AS SELECT 1 AS a".to_string(),
            ),
            ("drop_table", "DROP TABLE traces".to_string()),
            (
                "set_variable",
                "SET datafusion.execution.batch_size = 2".to_string(),
            ),
        ];
        for (name, sql) in &hostile {
            let err = traces
                .query_rows(sql, RenderKind::Spans)
                .await
                .err()
                .unwrap_or_else(|| panic!("{name} was not refused"));
            assert!(
                err.msg.contains("not supported"),
                "{name} failed for the wrong reason: {}",
                err.msg
            );
            // #2095 routed this call site through the capacity mapper. A
            // planner refusal is not capacity: it stays a 500 with no retry
            // contract, or a caller would read "the pool is busy, try again"
            // off a statement this server will never run.
            assert_eq!(
                err.status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{name} must stay a server-side refusal: {} {}",
                err.status,
                err.msg
            );
            assert!(
                err.headers.is_empty(),
                "{name} must carry no retry contract: {:?}",
                err.headers
            );
            assert_eq!(
                files_under(&target),
                Vec::<String>::new(),
                "{name} touched the filesystem"
            );
        }

        // A green run must not be one where everything is refused.
        let rows = traces
            .query_rows("SELECT count(*) AS n FROM traces", RenderKind::Spans)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "a plain SELECT was refused: {rows:?}");
    }

    /// Quoted request text stays a literal, on every parameter that reaches the
    /// `WHERE` list, and the typed ones cannot carry SQL at all (task #1693).
    ///
    /// The read-only planner is the boundary, but it is not a licence to stop
    /// escaping: a spliced fragment that parses would still change WHICH rows a
    /// tenant reads. So: an injection-shaped `service` matches nothing rather
    /// than everything, a quote in a tag key or value is a literal quote, and
    /// `start`/`end`/`minDuration` — interpolated with no escaping at all — are
    /// refused by their own parsers before any SQL is built.
    ///
    /// MEASURED, so nobody re-derives it: this arm passes against the pre-#1693
    /// `ctx.sql` call site as well, because the escaping and the typed params
    /// were already there. It is a characterisation of the behaviour the
    /// read-only switch must not change, not a second proof of the switch —
    /// [`jaeger_trace_sql_is_planned_read_only`] is the discriminating one.
    #[tokio::test]
    async fn jaeger_trace_params_with_quotes_stay_literal() {
        let (app, ice, _tmp) = build_server().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let base = "/api/v1/jaeger/siglake-traces-default/api";

        // Baseline: the honest query this index answers with both fixture
        // traces (every span carries `service.name=checkout`).
        let (status, body) = request_json(&app, &format!("{base}/traces?service=checkout")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total"], 2, "{body}");

        // Tautology-shaped values: escaped, so they match no service and no
        // operation. `total: 0`, NOT the whole index.
        for (label, query) in [
            (
                "service_or_tautology",
                "service=checkout%27%20OR%20%271%27%3D%271",
            ),
            ("service_comment", "service=checkout%27--"),
            (
                "service_union",
                "service=x%27%20UNION%20SELECT%20trace_id%20FROM%20traces%20WHERE%20%271%27%3D%271",
            ),
            (
                "operation_or_tautology",
                "operation=checkout%27%20OR%20%271%27%3D%271",
            ),
            // A quote in the tag KEY and in the tag VALUE. `attr_get` gets both
            // as literals, so neither matches the fixture's `http.method=GET`.
            (
                "tag_key_quote",
                "tags=%7B%22http.method%27%22%3A%22GET%22%7D",
            ),
            (
                "tag_value_tautology",
                "tags=%7B%22http.method%22%3A%22GET%27%20OR%20%271%27%3D%271%22%7D",
            ),
        ] {
            let (status, body) = request_json(&app, &format!("{base}/traces?{query}")).await;
            assert_eq!(status, StatusCode::OK, "{label}: {body}");
            assert_eq!(body["total"], 0, "{label} matched rows: {body}");
            assert_eq!(body["data"], json!([]), "{label}: {body}");
        }

        // A service name that legitimately contains a quote round-trips as
        // itself: the doubling is an escape, not a filter.
        let (status, body) = request_json(&app, &format!("{base}/traces?service=it%27s")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total"], 0, "{body}");

        // The numeric knobs are interpolated raw (`{start}`, `{end}`,
        // `duration_nanos >= {n}`), so they must never be strings by the time
        // the template sees them.
        for (label, query) in [
            ("start_not_a_number", "start=0%20OR%201%3D1"),
            ("end_not_a_number", "end=%271%27"),
            ("limit_not_a_number", "limit=1%3B%20DROP%20TABLE%20traces"),
            (
                "min_duration_not_a_duration",
                "minDuration=1%27%20OR%20%271",
            ),
            (
                "max_duration_not_a_duration",
                "maxDuration=%27%29%20OR%20TRUE%20--",
            ),
        ] {
            let (status, body) = request_text(&app, &format!("{base}/traces?{query}")).await;
            assert!(
                status.is_client_error(),
                "{label} was not refused: {status} {body}"
            );
        }

        // The single-trace route splices the path segment into an `IN (...)`
        // list through the same escaper.
        let (status, body) = request_json(
            &app,
            &format!("{base}/traces/00112233445566778899aabbccddeeff%27%20OR%20%271%27%3D%271"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total"], 0, "hostile trace id matched rows: {body}");

        // So does the operations route, from its `{service}` path segment.
        let (status, body) = request_json(
            &app,
            &format!("{base}/services/checkout%27%20OR%20%271%27%3D%271/operations"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["data"],
            json!([]),
            "hostile service matched rows: {body}"
        );
    }

    /// #2095: only a capacity refusal changes status at this call site.
    ///
    /// `query_rows` classifies its DataFusion failures through
    /// `ApiError::from_execution`, which turns a `ResourcesExhausted` into 503
    /// + `Retry-After` (proved end-to-end, at a pinned pool size, by
    /// `tests/jaeger_pool_exhaustion_503.rs` — the pool is process-wide, so
    /// that half needs its own binary). Everything else must still page: a
    /// plan error and a mid-execution Arrow failure are 500 with no
    /// `Retry-After`, exactly as before the mapper went in.
    #[tokio::test]
    async fn jaeger_non_capacity_query_failures_stay_500() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            // Out of the way: these failures happen before any render.
            unbounded_budget(),
        )
        .await
        .unwrap();

        for (name, sql) in [
            // Planning: no such column.
            ("plan_error", "SELECT nope FROM traces"),
            // Execution: the plan is fine and the division fails per row.
            (
                "execution_error",
                "SELECT trace_id, 1 / (length(trace_id) - 32) AS boom FROM traces",
            ),
        ] {
            let err = traces
                .query_rows(sql, RenderKind::Spans)
                .await
                .err()
                .unwrap_or_else(|| panic!("{name} did not fail"));
            assert_eq!(
                err.status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{name} must stay a server fault: {} {}",
                err.status,
                err.msg
            );
            assert!(
                err.headers.is_empty(),
                "{name} must advertise no retry: {:?}",
                err.headers
            );
            assert!(
                !err.msg.contains("memory pool"),
                "{name} must not be reported as capacity: {}",
                err.msg
            );
        }
    }

    #[tokio::test]
    async fn jaeger_returns_clear_errors_for_unknown_or_non_trace_indexes() {
        let (app, ice, _tmp) = build_server().await;
        ice.create_index(&IndexConfig {
            index_id: "logs".to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        })
        .await
        .unwrap();

        let (status, body) = request_json(&app, "/api/v1/jaeger/missing/api/services").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("index missing not found"));

        let (status, body) = request_json(&app, "/api/v1/jaeger/logs/api/services").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"]
            .as_str()
            .unwrap()
            .contains("is not a traces index"));
    }

    // ---- #2096: one interactive request lifecycle on the Jaeger routes ----

    /// Server limits with the knobs these tests move, INJECTED rather than
    /// set in the environment (project convention: tests never `set_var`).
    /// Everything else stays at the interactive defaults, because the point is
    /// that these routes read the same ceilings SQL does.
    fn tuned_limits(timeout: std::time::Duration, admission_budget_bytes: u64) -> ServerLimits {
        let mut limits = ServerLimits::default();
        limits.interactive.default_timeout = timeout;
        limits.interactive.ceiling_timeout = limits.interactive.ceiling_timeout.max(timeout);
        limits.admission_budget_bytes = admission_budget_bytes;
        // Short, so a saturated-budget assertion costs milliseconds. It is also
        // the figure the 429's `Retry-After` is derived from (rounded up to 1s).
        limits.admission_wait_timeout = std::time::Duration::from_millis(50);
        limits
    }

    /// The same, with the interactive row cap pinned to `max_rows` (#2119).
    ///
    /// Both the default AND the ceiling: the resolver clamps the default
    /// against the ceiling, and the Jaeger surface has no request `limits`
    /// object, so what these routes resolve is `default_rows_returned`.
    fn row_capped_limits(max_rows: usize) -> ServerLimits {
        let mut limits = tuned_limits(std::time::Duration::from_secs(60), 64 * 1024 * 1024);
        limits.interactive.default_rows_returned = max_rows;
        limits.interactive.ceiling_rows_returned = max_rows;
        limits
    }

    /// The four routes, one URL each, against the fixture index.
    fn every_jaeger_route() -> [&'static str; 4] {
        [
            "/api/v1/jaeger/siglake-traces-default/api/services",
            "/api/v1/jaeger/siglake-traces-default/api/services/checkout/operations",
            "/api/v1/jaeger/siglake-traces-default/api/traces?service=checkout",
            "/api/v1/jaeger/siglake-traces-default/api/traces/00112233445566778899aabbccddeeff",
        ]
    }

    async fn request_parts(app: &axum::Router, path: &str) -> (StatusCode, axum::http::HeaderMap) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        (response.status(), response.headers().clone())
    }

    /// An exhausted wall clock refuses every Jaeger route with 504, before it
    /// scans, and gives the reservation back (#2096).
    ///
    /// A zero budget is the sharpest form of the defect these handlers had:
    /// they awaited tenant resolution, table registration and a collect with no
    /// deadline anywhere, so the wall clock could not refuse them at all. It
    /// also pins the immediately-ready case the acceptance calls out —
    /// `tokio::time::timeout_at` polls its inner future BEFORE its timer, so a
    /// route whose work happens to be ready would answer 200 on a budget that
    /// is already gone without the pre-poll check `phase` makes.
    #[tokio::test]
    async fn an_exhausted_budget_refuses_every_jaeger_route_before_it_scans() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(tuned_limits(std::time::Duration::ZERO, 64 * 1024 * 1024));
        let admission = state.admission.clone();
        let app = crate::router(state);

        for path in every_jaeger_route() {
            let (status, body) = request_json(&app, path).await;
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{path}: {body}");
            assert!(
                body["error"].as_str().unwrap_or_default().contains("0s"),
                "{path} did not report the budget it exceeded: {body}"
            );
        }

        // Every one of those requests handed its reservation back on the way
        // out: the whole budget is available to one acquire.
        assert!(
            admission.acquire(64 * 1024 * 1024).await.is_ok(),
            "a timed-out trace read kept its admission reservation"
        );
    }

    /// Saturation sheds trace reads with 429 + `Retry-After`, from the SAME
    /// budget as `/api/v1/sql` — and the refusal is capacity, not a broken
    /// route: it succeeds the moment a reservation is released (#2096).
    ///
    /// Contention is built out of trace-sized reservations rather than one
    /// synthetic acquire of the whole budget, so the arithmetic is the one a
    /// pod actually runs: the per-query share clamp means several concurrent
    /// requests are needed to fill it, which is exactly the flood admission
    /// exists to bound.
    #[tokio::test]
    async fn a_saturated_admission_budget_sheds_trace_reads_with_retry_after() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(tuned_limits(
            std::time::Duration::from_secs(60),
            64 * 1024 * 1024,
        ));
        let admission = state.admission.clone();
        let app = crate::router(state);

        // Fill the budget with reservations the size a trace read takes.
        let unit = admission.unestimated_reservation_bytes();
        assert!(unit > 0, "the injected budget disabled admission");
        let mut held = Vec::new();
        while let Ok(guard) = admission.acquire(unit).await {
            held.push(guard);
            assert!(
                held.len() < 64,
                "{unit} bytes never filled the 64 MiB budget"
            );
        }
        assert!(
            !held.is_empty(),
            "the budget was full before this test reserved anything"
        );

        for path in every_jaeger_route() {
            let (status, headers) = request_parts(&app, path).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{path}");
            let retry_after = headers
                .get("retry-after")
                .unwrap_or_else(|| panic!("{path}: 429 without Retry-After"));
            assert!(
                retry_after.to_str().unwrap().parse::<u64>().unwrap() >= 1,
                "{path}: unusable Retry-After {retry_after:?}"
            );
        }

        // Positive control: with one reservation released the same request is
        // answered, so the 429s above were the budget and not the handler.
        held.pop();
        let (status, body) =
            request_json(&app, "/api/v1/jaeger/siglake-traces-default/api/services").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"], json!(["checkout"]));

        // And that answered request holds nothing afterwards.
        drop(held);
        assert!(
            admission.acquire(64 * 1024 * 1024).await.is_ok(),
            "a completed trace read kept its admission reservation"
        );
    }

    /// The SECOND query phase runs on what the first one LEFT, not a fresh
    /// budget (#2096).
    ///
    /// `find_traces` runs two queries — trace-id selection, then span
    /// retrieval — and before this card neither had a budget at all. The
    /// failure pinned here is the one a naive fix introduces: a per-phase
    /// timeout, under which a request that spent its whole clock selecting ids
    /// gets a whole second clock to retrieve spans with, and a client's 60s
    /// deadline becomes 120s of scan work on the pod.
    ///
    /// Paused clock and a `pending()` second phase, so this is arithmetic
    /// rather than a race: the first phase consumes 45 of 60 seconds through a
    /// timer, the second can only ever end at the deadline, and the 504 must
    /// land ONE budget after the request started.
    ///
    /// Paused with `pause()` rather than `start_paused`: the warehouse's SQL
    /// catalog opens a real connection pool, and under an auto-advancing clock
    /// its acquire timeout fires before the connection is established.
    #[tokio::test]
    async fn the_second_phase_spends_the_remainder_of_one_budget() {
        let (state, _ice, _tmp) = build_state().await;
        let state = state.with_limits(tuned_limits(
            std::time::Duration::from_secs(60),
            64 * 1024 * 1024,
        ));
        tokio::time::pause();
        let request = TraceRequest::begin(&state).await.unwrap();
        let started = tokio::time::Instant::now();

        request
            .phase(async {
                tokio::time::sleep(std::time::Duration::from_secs(45)).await;
                Ok(())
            })
            .await
            .expect("the first phase did not fit in the budget");

        let err = request
            .phase(std::future::pending::<Result<(), ApiError>>())
            .await
            .expect_err("the second phase was never cut off");
        assert_eq!(err.status, StatusCode::GATEWAY_TIMEOUT, "{}", err.msg);
        let elapsed = started.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_secs(60),
            "the request was refused before its budget ran out: {elapsed:?}"
        );
        // A per-phase budget would have taken 45s + 60s. One second of slack
        // because the request's zero point is the real clock while this
        // measurement is the paused one.
        assert!(
            elapsed < std::time::Duration::from_secs(61),
            "the second phase got its own 60s budget: {elapsed:?}"
        );
    }

    /// The request's cancel flag reaches the session context its queries plan
    /// against, and firing it is what stops work the request no longer wants
    /// (#2096).
    ///
    /// Dropping a request future cancels the future; the plan's spawned
    /// partition pumps keep scanning until this flag ends the source stream
    /// they drain from. So a timeout or a client hang-up has to both release
    /// the reservation AND set the flag — a reservation released while the scan
    /// runs on is admission lying about the pod's bytes.
    #[tokio::test]
    async fn a_dropped_request_cancels_its_scans_and_releases_its_reservation() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(tuned_limits(
            std::time::Duration::from_secs(60),
            16 * 1024 * 1024,
        ));
        let admission = state.admission.clone();

        let request = TraceRequest::begin(&state).await.unwrap();
        let traces = request
            .open(&state, &CallerIdentity::default(), "siglake-traces-default")
            .await
            .unwrap();
        let planned_cancel = traces
            .ctx
            .state()
            .config()
            .get_extension::<siglake_storage::QueryCancel>()
            .expect("the trace session context omitted QueryCancel");
        assert!(!planned_cancel.is_cancelled());
        // The reservation is held for the whole request, not per query.
        assert!(
            admission.acquire(16 * 1024 * 1024).await.is_err(),
            "an in-flight trace read reserved nothing"
        );

        drop(request);
        assert!(
            planned_cancel.is_cancelled(),
            "dropping the request left its scans running"
        );
        assert!(
            admission.acquire(16 * 1024 * 1024).await.is_ok(),
            "dropping the request left its reservation held"
        );
    }

    // ---- #2119: the resolved interactive row cap bounds the render ----

    /// A single trace larger than the resolved cap is refused WHOLE, with 413
    /// and no partial data (#2119).
    ///
    /// The proxy decision this card carries: `/api/v1/sql` truncates and signals
    /// it through the records envelope (`truncated`, `max_rows`), but Jaeger
    /// discards that envelope — its response is `{data, total}` — so a
    /// truncated render would hand the UI a trace missing spans that nothing in
    /// the response says is missing. Hence a refusal, and hence the assertion
    /// that the body carries no `data` at all.
    ///
    /// The positive control at cap 2 is what makes the refusal a cap and not a
    /// broken route: the same request, one row of headroom, answers 200 with
    /// both spans.
    #[tokio::test]
    async fn a_trace_over_the_row_cap_is_refused_rather_than_truncated() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        // The fixture's first trace has exactly two spans, so a cap of one is
        // cap + 1 and a cap of two is exactly at the cap.
        let state = state.with_limits(row_capped_limits(1));
        let admission = state.admission.clone();
        let app = crate::router(state.clone());
        let path =
            "/api/v1/jaeger/siglake-traces-default/api/traces/00112233445566778899aabbccddeeff";

        let (status, body) = request_json(&app, path).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(
            body.get("data").is_none(),
            "the refusal returned partial trace data: {body}"
        );
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("2 rows"),
            "the refusal does not name what it refused: {body}"
        );

        // A refused request holds nothing: the whole budget is available again.
        assert!(
            admission.acquire(64 * 1024 * 1024).await.is_ok(),
            "a row-capped refusal kept its admission reservation"
        );

        // Exactly at the cap is a success, whole.
        let at_cap = crate::router(state.with_limits(row_capped_limits(2)));
        let (status, body) = request_json(&at_cap, path).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"][0]["spans"].as_array().unwrap().len(), 2);
    }

    /// A trace SEARCH is capped across every selected trace's spans, not per
    /// trace, and an empty search is answered under any cap (#2119).
    ///
    /// `find_traces` runs two queries on one budget. At cap 2 the first phase
    /// (two trace ids) is exactly at the cap and passes; the second retrieves
    /// three spans across those two traces and is refused. A per-trace cap
    /// would have let it through — two spans then one — which is the reading
    /// this pins against.
    #[tokio::test]
    async fn a_trace_search_caps_spans_across_all_selected_traces() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(row_capped_limits(2));
        let admission = state.admission.clone();
        let app = crate::router(state);
        let base = "/api/v1/jaeger/siglake-traces-default/api";

        let (status, body) = request_json(&app, &format!("{base}/traces?service=checkout")).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(
            body.get("data").is_none(),
            "the refusal returned partial search data: {body}"
        );
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("3 rows"),
            "the refusal does not name the spans it refused: {body}"
        );
        assert!(
            admission.acquire(64 * 1024 * 1024).await.is_ok(),
            "a row-capped refusal kept its admission reservation"
        );

        // Nothing matched: an empty result is under every cap, and still 200.
        let (status, body) = request_json(&app, &format!("{base}/traces?service=nope")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"], json!([]), "{body}");
        assert_eq!(body["total"], 0, "{body}");
    }

    // ---- #2184: the derived trace, span-row and render-byte ceilings ----

    /// A render budget that bounds nothing, for the arms that are about the
    /// planner or the error mapper rather than the ceilings.
    fn unbounded_budget() -> TraceRenderBudget {
        TraceRenderBudget {
            max_rows_returned: usize::MAX,
            max_rows_scanned: usize::MAX,
            circuit_breakers: true,
            ceilings: JaegerCeilings {
                traces: usize::MAX,
                span_rows: usize::MAX,
                render_bytes: usize::MAX,
                names: usize::MAX,
                render_budget_bytes: u64::MAX,
            },
        }
    }

    /// Nothing that changes the ANSWER may leave the name-list cache key
    /// unchanged (#2268).
    ///
    /// THE DEFECT THIS FORECLOSES. The two list routes issue byte-identical
    /// SQL for every index of every tenant — `jaeger_routes.rs` registers each
    /// index under the shared `traces` alias — so a key derived from the query
    /// text, the way `/api/v1/sql`'s is, would hand one tenant's service list
    /// to another tenant, and one index's to another index. The key is the
    /// route's own, and this pins each ingredient by varying exactly one thing
    /// at a time from a common base.
    ///
    /// A pure function of its arguments, tested as one: the values it composes
    /// (the resolved table, the generation the provider serves, the request's
    /// ceilings) are established by `TraceQueryContext::new` and asserted
    /// end-to-end in `tests/jaeger_name_cache.rs`.
    #[test]
    fn every_ingredient_of_a_name_list_key_separates_two_answers() {
        let budget = unbounded_budget();
        let table = "siglake.traces";
        let snapshot = 7_i64;
        let schema = 3_i32;
        let base = name_cache_key(table, snapshot, schema, &budget, &NameList::Services);

        let operations = |service: &str| NameList::Operations {
            service: service.to_string(),
        };
        let mut tighter_rows = budget;
        tighter_rows.max_rows_returned = 10;
        let mut tighter_names = budget;
        tighter_names.ceilings.names = 10;
        let mut tighter_bytes = budget;
        tighter_bytes.ceilings.render_bytes = 10;

        for (what, key) in [
            // Another tenant's namespace, and another index in the same one.
            (
                "namespace",
                name_cache_key(
                    "other.traces",
                    snapshot,
                    schema,
                    &budget,
                    &NameList::Services,
                ),
            ),
            (
                "index",
                name_cache_key(
                    "siglake.spans",
                    snapshot,
                    schema,
                    &budget,
                    &NameList::Services,
                ),
            ),
            // A commit.
            (
                "snapshot",
                name_cache_key(table, 8, schema, &budget, &NameList::Services),
            ),
            // An additive `migrate-schema`, which moves the schema id and NOT
            // the snapshot (#2494).
            (
                "schema",
                name_cache_key(table, snapshot, 4, &budget, &NameList::Services),
            ),
            // The other list, and another service's operations.
            (
                "list",
                name_cache_key(table, snapshot, schema, &budget, &operations("checkout")),
            ),
            (
                "service",
                name_cache_key(table, snapshot, schema, &budget, &operations("payments")),
            ),
            // A ceiling that would have refused this answer.
            (
                "max_rows_returned",
                name_cache_key(table, snapshot, schema, &tighter_rows, &NameList::Services),
            ),
            (
                "names ceiling",
                name_cache_key(table, snapshot, schema, &tighter_names, &NameList::Services),
            ),
            (
                "render_bytes ceiling",
                name_cache_key(table, snapshot, schema, &tighter_bytes, &NameList::Services),
            ),
        ] {
            assert_ne!(base, key, "{what} does not separate two name-list answers");
        }

        // And the same question twice is the same key, or nothing is ever
        // served from the cache at all.
        assert_eq!(
            base,
            name_cache_key(table, snapshot, schema, &budget, &NameList::Services)
        );
        assert_eq!(
            name_cache_key(table, snapshot, schema, &budget, &operations("checkout")),
            name_cache_key(table, snapshot, schema, &budget, &operations("checkout"))
        );

        // A service name carrying the key's own separators cannot pose as
        // another key: every client-supplied field is length-prefixed.
        assert_ne!(
            name_cache_key(
                table,
                snapshot,
                schema,
                &budget,
                &operations("a|kind=services")
            ),
            name_cache_key(table, snapshot, schema, &budget, &operations("a")),
        );
        assert_ne!(
            name_cache_key(
                "siglake.a|snapshot=7",
                snapshot,
                schema,
                &budget,
                &NameList::Services
            ),
            name_cache_key("siglake.a", snapshot, schema, &budget, &NameList::Services),
        );
    }

    /// The 64 MiB admission budget these arms inject resolves a 16 MiB
    /// reservation — the packaged pod's — so the ceilings under test are the
    /// documented ones rather than a test-only shape.
    const PACKAGED_TRACE_CEILING: usize = 843;

    /// A hostile `?limit=` is refused BEFORE the reservation, the index lookup
    /// and the planner (#2184).
    ///
    /// "Before" is proved twice, because an admission budget that is acquirable
    /// afterwards only proves the request RELEASED what it took:
    ///
    ///  1. against a FULL admission budget, where a request that admitted would
    ///     have waited out `admission_wait_timeout` and answered 429. It
    ///     answers 400 instead, so it never asked. The control is the same
    ///     request one trace under the ceiling: it does ask, and it 429s.
    ///  2. against a MISSING index, where a request that reached
    ///     `TraceQueryContext::new` would have answered 404 (the control does).
    ///     It answers 400 instead, so neither the index lookup nor the planner
    ///     ran — and no result was rendered from the rejected input.
    #[tokio::test]
    async fn a_hostile_limit_is_refused_before_admission_the_index_lookup_and_the_planner() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(tuned_limits(
            std::time::Duration::from_secs(60),
            64 * 1024 * 1024,
        ));
        let ceiling =
            crate::jaeger_limits::ceilings_from(state.admission.unestimated_reservation_bytes())
                .traces;
        assert_eq!(
            ceiling, PACKAGED_TRACE_CEILING,
            "this arm is calibrated against the packaged pod's reservation"
        );
        let admission = state.admission.clone();
        let app = crate::router(state);
        let base = "/api/v1/jaeger/siglake-traces-default/api";

        // Fill the budget with reservations the size a trace read takes, so
        // anything that admits from here on waits and then sheds.
        let unit = admission.unestimated_reservation_bytes();
        let mut held = Vec::new();
        while let Ok(guard) = admission.acquire(unit).await {
            held.push(guard);
            assert!(held.len() < 64, "{unit} bytes never filled the budget");
        }
        assert!(!held.is_empty(), "the budget was full before this arm ran");

        let (status, body) = request_json(
            &app,
            &format!("{base}/traces?service=checkout&limit={}", ceiling + 1),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a hostile limit reached admission (429) or the pool: {body}"
        );
        let msg = body["error"].as_str().unwrap_or_default();
        assert!(
            msg.contains(&(ceiling + 1).to_string()) && msg.contains(&ceiling.to_string()),
            "the refusal names neither the value sent nor the ceiling: {body}"
        );
        assert!(
            body.get("data").is_none(),
            "the refusal rendered the rejected request: {body}"
        );

        // Control: one trace UNDER the ceiling does reach admission, and sheds.
        let (status, _) = request_parts(
            &app,
            &format!("{base}/traces?service=checkout&limit={ceiling}"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "a legal limit must still be admitted (and shed on a full budget), \
             or the 400 above proves nothing about admission"
        );

        // And the refusal held nothing of its own: releasing the contention
        // leaves the whole budget acquirable.
        drop(held);
        assert!(
            admission.acquire(64 * 1024 * 1024).await.is_ok(),
            "a trace-ceiling refusal kept an admission reservation"
        );

        // 2. No index lookup either: this index does not exist.
        let missing = "/api/v1/jaeger/nope/api/traces";
        let (status, body) = request_json(&app, &format!("{missing}?limit={}", ceiling + 1)).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a hostile limit reached the index lookup: {body}"
        );
        let (status, body) = request_json(&app, &format!("{missing}?limit={ceiling}")).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "a legal limit must reach the index lookup, or the 400 above proves \
             nothing about it: {body}"
        );
    }

    /// The default client keeps working on every budget a usable pod resolves,
    /// including a DISABLED admission controller (budget 0, reservation 0),
    /// which must fall back to the packaged ceilings rather than resolving them
    /// to zero and refusing the surface outright.
    ///
    /// The exception is deliberate and asserted below: a reservation at or
    /// under the ~2 MiB per-request floor leaves no render budget at all, so
    /// even the default search is refused — with a 413 that says so, not a
    /// silent empty result. That is a 8 MiB admission budget at the default
    /// share divisor, i.e. a pod that cannot render a trace.
    #[tokio::test]
    async fn the_trace_ceiling_admits_the_default_client_on_every_usable_budget() {
        for budget in [
            0u64,
            12 * 1024 * 1024,
            64 * 1024 * 1024,
            1024 * 1024 * 1024,
            u64::MAX,
        ] {
            let (state, ice, _tmp) = build_state().await;
            append_trace_fixture(&ice, "siglake-traces-default").await;
            let state = state.with_limits(tuned_limits(std::time::Duration::from_secs(60), budget));
            let app = crate::router(state);
            let base = "/api/v1/jaeger/siglake-traces-default/api";

            // No `limit` at all (the surface's own default of 20), and an
            // explicit 20: Grafana's default search.
            for query in ["service=checkout", "service=checkout&limit=20"] {
                let (status, body) = request_json(&app, &format!("{base}/traces?{query}")).await;
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "budget {budget}, `{query}`: the default client was refused: {body}"
                );
                assert_eq!(body["total"], 2, "budget {budget}, `{query}`: {body}");
            }
        }

        // Below the per-request floor there is nothing left to render with, and
        // the surface says so rather than pretending.
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let state = state.with_limits(tuned_limits(
            std::time::Duration::from_secs(60),
            8 * 1024 * 1024,
        ));
        let app = crate::router(state);
        let (status, body) = request_json(
            &app,
            "/api/v1/jaeger/siglake-traces-default/api/traces?service=checkout",
        )
        .await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "a 2 MiB reservation has no render budget; it must refuse, not render: {body}"
        );
        assert!(
            body.get("data").is_none(),
            "the refusal returned partial data: {body}"
        );
    }

    /// The render budget is spent by BOTH phases of a search, not reset between
    /// them (#2184).
    ///
    /// Driven through the context directly so the budget is the assertion
    /// rather than an arithmetic consequence of some admission budget: at four
    /// span rows, phase A (two trace ids) and phase B (three spans) each FIT on
    /// their own — a per-phase bound answers 200 twice — and together they do
    /// not. The refusal must therefore come from the second phase, naming the
    /// two rows the first one already spent.
    #[tokio::test]
    async fn the_render_budget_is_spent_across_both_phases_of_a_search() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let mut budget = unbounded_budget();
        budget.ceilings.span_rows = 4;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            budget,
        )
        .await
        .unwrap();

        let params = FindTracesParams {
            service: Some("checkout".to_string()),
            operation: None,
            start: None,
            end: None,
            limit: None,
            min_duration: None,
            max_duration: None,
            tags: None,
        };
        let trace_ids = traces
            .find_trace_ids(&params)
            .await
            .expect("phase A fits four rows on its own");
        assert_eq!(trace_ids.len(), 2, "{trace_ids:?}");
        assert_eq!(traces.spent_rows.load(Ordering::Relaxed), 2);

        let err = traces
            .fetch_spans_for_trace_ids(&trace_ids)
            .await
            .expect_err("three spans on the two rows left of a four-row budget");
        assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", err.msg);
        assert!(
            err.msg.contains("2 span rows") && err.msg.contains("4 per request"),
            "the refusal does not say what the first phase left: {}",
            err.msg
        );
        assert!(
            err.headers.is_empty(),
            "413 must advertise no retry: {:?}",
            err.headers
        );

        // Positive control, same fixture and the same two phases: five rows is
        // the whole request, so both phases complete and nothing is refused.
        let mut budget = unbounded_budget();
        budget.ceilings.span_rows = 5;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            budget,
        )
        .await
        .unwrap();
        let trace_ids = traces.find_trace_ids(&params).await.unwrap();
        let rows = traces.fetch_spans_for_trace_ids(&trace_ids).await.unwrap();
        assert_eq!(rows.len(), 3, "a five-row budget must render both phases");
    }

    /// The byte bound refuses what no row count can: a handful of rows carrying
    /// megabytes of attributes (#2184). Rows are far under their ceiling, and
    /// the message says which unit refused.
    #[tokio::test]
    async fn the_byte_bound_refuses_wide_rows_a_row_count_admits() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;
        let mut budget = unbounded_budget();
        budget.ceilings.render_bytes = 512;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            budget,
        )
        .await
        .unwrap();

        let err = traces
            .fetch_spans_for_trace_ids(std::slice::from_ref(
                &"00112233445566778899aabbccddeeff".to_string(),
            ))
            .await
            .expect_err("two wide spans against a 512-byte render budget");
        assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", err.msg);
        assert!(
            err.msg.contains("bytes of Arrow data") && err.msg.contains("512 bytes"),
            "the refusal does not name the unit that refused: {}",
            err.msg
        );

        // Positive control: the same two spans under a budget that fits them.
        let mut budget = unbounded_budget();
        budget.ceilings.render_bytes = 64 * 1024 * 1024;
        let traces = TraceQueryContext::new(
            &state,
            &CallerIdentity::default(),
            "siglake-traces-default",
            &siglake_storage::QueryCancel::new(),
            budget,
        )
        .await
        .unwrap();
        let rows = traces
            .fetch_spans_for_trace_ids(std::slice::from_ref(
                &"00112233445566778899aabbccddeeff".to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "the control must render both spans");
    }

    /// Both list routes are bounded in names, and by the same request-wide
    /// budget — with the fixture's own names as the positive control that the
    /// bound is a bound and not a broken route.
    #[tokio::test]
    async fn the_name_routes_are_bounded_in_names() {
        let (state, ice, _tmp) = build_state().await;
        append_trace_fixture(&ice, "siglake-traces-default").await;

        let tier = state.limits.tier(Priority::Interactive);
        let resolved =
            ResolvedLimits::resolve(&RequestLimits::default(), Priority::Interactive, &tier);
        for (label, list, names, ceiling, expect_ok) in [
            // The fixture has one service and three operations.
            ("services_fit", NameList::Services, 1, 1usize, true),
            (
                "operations_fit",
                NameList::Operations {
                    service: "checkout".to_string(),
                },
                3,
                3,
                true,
            ),
            (
                "operations_over",
                NameList::Operations {
                    service: "checkout".to_string(),
                },
                3,
                2,
                false,
            ),
        ] {
            let mut budget = unbounded_budget();
            budget.ceilings.names = ceiling;
            let traces = TraceQueryContext::new(
                &state,
                &CallerIdentity::default(),
                "siglake-traces-default",
                &siglake_storage::QueryCancel::new(),
                budget,
            )
            .await
            .unwrap();
            let result = traces
                .list_names(
                    &list,
                    &resolved,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(3_600),
                )
                .await;
            match (expect_ok, result) {
                (true, Ok(found)) => assert_eq!(found.len(), names, "{label}"),
                (true, Err(err)) => panic!("{label} was refused at its own size: {}", err.msg),
                (false, Ok(names)) => panic!("{label} was not refused: {names:?}"),
                (false, Err(err)) => {
                    assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", err.msg);
                    assert!(
                        err.msg.contains("names"),
                        "{label} does not name its unit: {}",
                        err.msg
                    );
                }
            }
        }
    }

    // ---- #2231: the PRODUCER stops one row past the bound ----

    /// One trace of `spans` spans in ONE file, so a plan that materializes its
    /// input materializes all of them before anything can look at a bound.
    async fn append_one_wide_trace(ice: &IcebergContext, index_id: &str, spans: u64) -> String {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

        let config = IndexConfig {
            index_id: index_id.to_string(),
            doc_mapping: builtin_traces_template().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        ice.create_index(&config).await.unwrap();
        let trace_id = vec![0x7fu8; 16];
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ProtoResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![proto_str("service.name", "checkout")],
                    ..Default::default()
                }),
                scope_spans: vec![ProtoScopeSpans {
                    spans: (0..spans)
                        .map(|i| ProtoSpan {
                            trace_id: trace_id.clone(),
                            span_id: i.to_be_bytes().to_vec(),
                            name: "checkout".to_string(),
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: 1_700_000_000_000_000_000 + i * 1_000_000,
                            end_time_unix_nano: 1_700_000_000_000_500_000 + i * 1_000_000,
                            attributes: vec![proto_str("http.method", "GET")],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let events = otlp_proto_traces_to_events(request);
        let carrier = events_to_record_batch(&events).unwrap();
        let mapped = map_carrier_batch(&carrier, &config).unwrap();
        let bloom_refs = config
            .doc_mapping
            .tag_fields
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        ice.append_to_table(&ice.index_table_ident(index_id), mapped, &bloom_refs)
            .await
            .unwrap();
        "7f".repeat(trace_id.len())
    }

    /// A refusal costs ONE row past the bound, not one whole produced batch
    /// (#2231).
    ///
    /// #2184 reads its bound at a batch boundary, which is the earliest a
    /// refusal can happen but not the earliest the rows exist: the span-fetch
    /// query is `ORDER BY timestamp, span_id` with no `LIMIT`, so its
    /// `SortExec` buffered EVERY matching row and then handed over an
    /// 8,192-row batch. Measured on the wide corpus, a refused `?limit=200`
    /// peaked at 322 MiB against 162.90 MiB of Arrow for the same rows.
    ///
    /// This asserts the bound, not the status: 300 spans against a 10-row
    /// budget must report 11 rows — `max_rows + 1`, the whole evidence a
    /// refusal needs — and not 300. Against a `SortExec` with no fetch it
    /// reports 300, which is what the fix removes.
    #[tokio::test]
    async fn a_refusal_costs_one_row_past_the_bound_not_one_whole_batch() {
        let (state, ice, _tmp) = build_state().await;
        let trace_id = append_one_wide_trace(&ice, "siglake-traces-default", 300).await;
        let ids = std::slice::from_ref(&trace_id);

        let refused = |span_rows: usize| {
            let state = state.clone();
            let ids = ids.to_vec();
            async move {
                let mut budget = unbounded_budget();
                budget.ceilings.span_rows = span_rows;
                let traces = TraceQueryContext::new(
                    &state,
                    &CallerIdentity::default(),
                    "siglake-traces-default",
                    &siglake_storage::QueryCancel::new(),
                    budget,
                )
                .await
                .unwrap();
                traces.fetch_spans_for_trace_ids(&ids).await
            }
        };

        let err = refused(10)
            .await
            .expect_err("300 spans against a 10-row render budget");
        assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", err.msg);
        assert!(
            err.msg.contains("at least 11 rows"),
            "the refusal reports more than one row past the bound, so the producer was not \
             bounded: {}",
            err.msg
        );

        // One row under the whole trace: the producer stops at 300, which is
        // still strictly over 299, so the refusal is unchanged in kind.
        let err = refused(299).await.expect_err("300 spans against 299 rows");
        assert!(
            err.msg.contains("at least 300 rows"),
            "the boundary case does not report the row that crossed it: {}",
            err.msg
        );

        // EXACTLY at the bound is a completion, whole — the fetch is
        // `max_rows + 1`, so a complete result is never the one truncated.
        let rows = refused(300)
            .await
            .expect("300 spans against a 300-row budget");
        assert_eq!(rows.len(), 300, "the trace came back short");
    }
}
