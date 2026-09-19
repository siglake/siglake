//! Shared HTTP error type. Follows the same shape as `siglake-ingest`'s
//! `ApiError`: a status code + free-text message, rendered as
//! `{"error": "...", "code": <status>}`.

use anyhow::Error as AnyhowError;
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use datafusion::error::DataFusionError;
use serde_json::{json, Value};
use siglake_core::index_config::IndexConfigError;
use siglake_storage::index_manager::IndexManagerError;

use crate::cost::CostReport;
use crate::limits::Priority;

/// `Retry-After` for a query the memory pool refused.
///
/// The pool frees as the queries holding it finish, and an interactive query
/// that is not pathological finishes in well under this. Not a knob: nothing
/// measured yet says what the right figure is, and a wrong constant costs a
/// client one extra retry while a wrong env var costs a resolver, a test and a
/// chart line.
pub const POOL_EXHAUSTED_RETRY_AFTER_SECS: u64 = 5;

/// Backoff advertised when the persistent batch-job store cannot currently
/// answer. The next request may use a healthy connection or replica.
pub const JOB_STORE_RETRY_AFTER_SECS: u64 = 5;

/// The counter arm a pool refusal is counted under, in
/// `siglake_query_breaker_trips_total{breaker=...}` alongside `admission`,
/// `timeout` and `shard_timeout`.
pub const POOL_EXHAUSTED_BREAKER: &str = "pool_exhausted";

/// `Retry-After` for a shard whose pinned snapshot this worker cannot resolve.
///
/// The condition clears on its own: either this pod's metadata cache converges
/// on the coordinator's snapshot, or the coordinator's own cache moves off a
/// snapshot the catalog no longer retains and it pins the next attempt to a
/// live one. Both are bounded by the metadata cache TTL
/// (`SIGLAKE_ICEBERG_METADATA_CACHE_TTL_SECS`, 5s by default), which is what
/// this figure tracks.
///
/// Deliberately the same number as [`POOL_EXHAUSTED_RETRY_AFTER_SECS`]:
/// [`ApiError::from_query`] re-attaches *the coordinator's* constant to any
/// forwarded 503 (a `ShardError` carries the worker's status and body, not its
/// headers), so a different value here would never reach the client anyway.
pub const SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS: u64 = POOL_EXHAUSTED_RETRY_AFTER_SECS;

/// The `reason` discriminator in the error body of a shard-pin refusal, so a
/// caller (or a coordinator log reader) can tell it from the other 503 — a
/// memory-pool refusal — without parsing prose.
pub const SHARD_PIN_UNRESOLVED_REASON: &str = "shard_pin_unresolved";

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub msg: String,
    /// Optional structured context attached to the response body so
    /// callers can pretty-print the failure (e.g. cost-rejection prints
    /// the cost report).
    pub context: Option<Value>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, msg)
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, msg)
    }

    /// The credential verified, and still does not authorize this request.
    ///
    /// Today the one producer is the tenant check in `auth::middleware`: a
    /// token whose configured tenant claim is missing or unusable. 403 rather
    /// than 401 because re-authenticating cannot help — the token is fine, the
    /// tenancy it asserts is not — and a 401 would send a client into a token
    /// refresh loop over a claim its IDP is not configured to mint.
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, msg)
    }

    pub fn service_unavailable(msg: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, msg)
    }

    pub fn job_store_unavailable() -> Self {
        let mut err = Self::service_unavailable(format!(
            "job store temporarily unavailable; retry after {JOB_STORE_RETRY_AFTER_SECS}s"
        ));
        err.headers.push(retry_after(JOB_STORE_RETRY_AFTER_SECS));
        err
    }

    pub fn internal(e: impl std::fmt::Display) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
    }

    pub fn too_many_requests(
        msg: impl Into<String>,
        retry_after_secs: u64,
        cost: Option<&CostReport>,
    ) -> Self {
        let mut err = Self::new(StatusCode::TOO_MANY_REQUESTS, msg);
        err.headers.push(retry_after(retry_after_secs));
        if let Some(cost) = cost {
            err.context = Some(json!({ "cost": cost }));
        }
        err
    }

    /// The process-wide query memory pool — or the spill directory's byte cap,
    /// which DataFusion reports the same way — refused an allocation.
    ///
    /// 503 with `Retry-After`, not 500 and not 429. Not 500 because nothing is
    /// broken: the pool did exactly what it is for, and a client that backs off
    /// on 5xx-with-Retry-After should back off here, while one that pages on
    /// 500 should not page. Not 429 because the client did nothing wrong — 429
    /// is admission's answer to a caller who exceeded a budget priced against
    /// THEIR request; the pool refuses on the aggregate of everyone's, and the
    /// same query will fit the moment the others finish. `detail` is
    /// DataFusion's own message, which names the consumer and the bytes.
    pub fn pool_exhausted(detail: impl std::fmt::Display) -> Self {
        let mut err = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "query memory pool exhausted; retry after {POOL_EXHAUSTED_RETRY_AFTER_SECS}s: \
                 {detail}"
            ),
        );
        err.headers
            .push(retry_after(POOL_EXHAUSTED_RETRY_AFTER_SECS));
        err
    }

    /// #1525: this worker cannot serve the snapshot the coordinator pinned the
    /// fan-out to, so it refuses the fragment instead of answering from its own
    /// current snapshot.
    ///
    /// The pin exists because file-shard completeness depends on every worker
    /// enumerating the SAME file generation: shard `i` takes files `i, i+N,
    /// …` of the snapshot's live file list. Answering a pinned request from a
    /// different generation — after a compaction rewrite, or across a commit —
    /// makes the shards a partition of nothing in particular, and the merged
    /// count is silently wrong in either direction. A degraded answer is worse
    /// than no answer here, because nothing downstream can tell it happened.
    ///
    /// 503 + `Retry-After`, matching the pool refusal's contract: the request
    /// is well-formed (not 400), nothing is broken (not 500), and the same
    /// query succeeds once the caches converge. The body carries
    /// `reason: "shard_pin_unresolved"` plus the pin, so this is
    /// distinguishable from a pool refusal without reading the message.
    pub fn shard_pin_unresolved(
        table: &str,
        snapshot_id: i64,
        schema_id: Option<i32>,
        table_uuid: Option<&str>,
    ) -> Self {
        let mut err = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "snapshot {snapshot_id} of `{table}` is not available on this worker; \
                 refusing the shard rather than answering from a different snapshot; \
                 retry after {SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS}s"
            ),
        );
        err.context = Some(json!({
            "reason": SHARD_PIN_UNRESOLVED_REASON,
            "pin": {
                "table": table,
                "snapshot_id": snapshot_id,
                "schema_id": schema_id,
                "table_uuid": table_uuid,
            },
        }));
        err.headers
            .push(retry_after(SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS));
        err
    }

    /// #2554: the pinned SNAPSHOT resolves on this worker but the pinned schema
    /// generation does not, so it refuses the fragment.
    ///
    /// Same contract as [`Self::shard_pin_unresolved`] — 503, `Retry-After`,
    /// `reason: "shard_pin_unresolved"` — because a caller reacts to both the
    /// same way. The message differs because the operator does not: a snapshot
    /// refusal points at snapshot retention against a stale coordinator, and
    /// this one points at a schema-only `migrate-schema` that has not reached
    /// every replica's metadata cache. An additive migration commits no data
    /// snapshot, so the snapshot id alone cannot tell them apart.
    ///
    /// Reachable only when the pinned schema id is in NO generation this
    /// worker's metadata retains, even after a refresh — a worker that is
    /// merely ahead serves the pinned historical schema instead.
    pub fn shard_pin_schema_unresolved(
        table: &str,
        snapshot_id: Option<i64>,
        schema_id: i32,
        table_uuid: Option<&str>,
    ) -> Self {
        let mut err = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "schema {schema_id} of `{table}` is not available on this worker; \
                 refusing the shard rather than answering from a different schema; \
                 retry after {SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS}s"
            ),
        );
        err.context = Some(json!({
            "reason": SHARD_PIN_UNRESOLVED_REASON,
            "pin": {
                "table": table,
                "snapshot_id": snapshot_id,
                "schema_id": schema_id,
                "table_uuid": table_uuid,
            },
        }));
        err.headers
            .push(retry_after(SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS));
        err
    }

    /// #2921: the table name now resolves to a different Iceberg incarnation
    /// than the one the coordinator pinned. This uses the same retryable shard
    /// refusal contract as snapshot and schema misses.
    pub fn shard_pin_incarnation_unresolved(
        table: &str,
        snapshot_id: Option<i64>,
        schema_id: Option<i32>,
        table_uuid: &str,
    ) -> Self {
        let mut err = Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "table incarnation {table_uuid} of `{table}` is not available on this worker; \
                 refusing the shard rather than answering from a replacement table; \
                 retry after {SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS}s"
            ),
        );
        err.context = Some(json!({
            "reason": SHARD_PIN_UNRESOLVED_REASON,
            "pin": {
                "table": table,
                "snapshot_id": snapshot_id,
                "schema_id": schema_id,
                "table_uuid": table_uuid,
            },
        }));
        err.headers
            .push(retry_after(SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS));
        err
    }

    /// Pre-flight rejection: the resolved limits say "no" before
    /// execution starts. 400 + the cost report so the caller can fix
    /// the limit (or the query) and retry.
    pub fn cost_rejected(reason: impl Into<String>, cost: &CostReport) -> Self {
        let mut err = Self::new(StatusCode::BAD_REQUEST, reason);
        err.context = Some(json!({ "cost": cost }));
        err
    }

    /// Wall-clock timeout. 504; attaches the cost so the caller can
    /// decide whether to retry with `priority: "batch"`.
    pub fn timeout(cost: &CostReport, elapsed_secs: f64, timeout_secs: u64) -> Self {
        let mut err = Self::new(
            StatusCode::GATEWAY_TIMEOUT,
            format!(
                "query exceeded {timeout_secs}s timeout (elapsed: {elapsed_secs:.2}s); \
                 retry with `priority: \"batch\"` for long-running queries"
            ),
        );
        err.context = Some(json!({ "cost": cost }));
        err
    }

    fn new(status: StatusCode, msg: impl Into<String>) -> Self {
        Self {
            status,
            msg: msg.into(),
            context: None,
            headers: Vec::new(),
        }
    }

    pub fn new_status(status: StatusCode, msg: impl Into<String>) -> Self {
        Self::new(status, msg)
    }

    /// Map a failure from EXECUTING a plan on this pod into the shared API
    /// error body.
    ///
    /// A `DataFusionError::ResourcesExhausted` anywhere in the chain — the
    /// memory pool refusing a sort, aggregate or join, or the spill directory
    /// hitting its byte cap — is a capacity answer, not a fault: it becomes
    /// [`Self::pool_exhausted`] (503 + `Retry-After`) and is counted under
    /// `siglake_query_breaker_trips_total{breaker="pool_exhausted"}`, so an
    /// operator can tell memory pressure from a defect. Everything else is
    /// still 500. The count lives here rather than at the call sites because
    /// there are several of them and a refusal that is not counted is
    /// invisible in exactly the situation the counter exists for.
    pub fn from_execution(err: AnyhowError, priority: Priority) -> Self {
        match pool_exhaustion(&err) {
            Some(detail) => {
                metrics::counter!(
                    "siglake_query_breaker_trips_total",
                    "breaker" => POOL_EXHAUSTED_BREAKER,
                    "priority" => priority.label()
                )
                .increment(1);
                Self::pool_exhausted(detail)
            }
            None => Self::internal(err),
        }
    }

    /// Map a query failure into the shared API error body, preserving a shard
    /// worker's verdict about the QUERY.
    ///
    /// A distributed query's real answer can come from a worker: the row-ceiling
    /// breaker (413), a rejected query (400), a budget refusal (429), a pool
    /// refusal (503), a timeout (504). Those are the same answers a single-node
    /// deployment gives, and a client acts on them differently than on a server
    /// fault — 413 means "narrow the query", 503 means "wait and retry", 500
    /// means "retry or page someone". Collapsing them all to 500 (which is what
    /// `internal` does) loses that distinction, so forward the worker's status
    /// for the refusals and keep 500 for everything else, including a worker's
    /// own 500 (that IS a server fault).
    ///
    /// A forwarded 503 gets this pod's `Retry-After`: `ShardError` carries the
    /// worker's status and body, not its headers, and the figure is the same
    /// constant on every pod. The worker already counted the refusal, so the
    /// coordinator does not count it again; a refusal in the coordinator's OWN
    /// execution (no `ShardError` in the chain) goes through
    /// [`Self::from_execution`] and is counted here.
    pub fn from_query(err: AnyhowError, priority: Priority) -> Self {
        let Some(shard) = chain_downcast_ref::<crate::coordinator::ShardError>(&err) else {
            return Self::from_execution(err, priority);
        };
        let forwarded = matches!(
            shard.status.as_u16(),
            400 | 413 | 429 | 503 | 504 | 401 | 403 | 404 | 422
        );
        if !forwarded {
            return Self::internal(err);
        }
        // reqwest and axum both re-export http's StatusCode but the versions can
        // differ; go through the wire number, which is the contract anyway.
        match StatusCode::from_u16(shard.status.as_u16()) {
            Ok(status) => {
                let mut out = Self::new(status, format!("{err:#}"));
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    out.headers
                        .push(retry_after(POOL_EXHAUSTED_RETRY_AFTER_SECS));
                }
                out
            }
            Err(_) => Self::internal(err),
        }
    }

    /// Map index-management/storage failures into the shared API error body.
    pub fn from_index_manager(err: AnyhowError) -> Self {
        if let Some(stale @ IndexManagerError::StaleIndexConfig { current, etag, .. }) =
            chain_downcast_ref::<IndexManagerError>(&err)
        {
            let mut out = Self::new(StatusCode::PRECONDITION_FAILED, stale.to_string());
            out.context = Some(json!({ "current": current }));
            if let Ok(value) = HeaderValue::from_str(etag) {
                out.headers.push((header::ETAG, value));
            }
            return out;
        }
        let status = if let Some(err) = chain_downcast_ref::<IndexManagerError>(&err) {
            match err {
                IndexManagerError::IndexAlreadyExists(_) => StatusCode::CONFLICT,
                IndexManagerError::IndexNotFound(_) => StatusCode::NOT_FOUND,
                IndexManagerError::StaleIndexConfig { .. } => StatusCode::PRECONDITION_FAILED,
                IndexManagerError::NotAnIndex(_)
                | IndexManagerError::ReservedSystemIndexId(_)
                | IndexManagerError::FieldMappingsPrefixMismatch { .. }
                | IndexManagerError::FieldTypeChanged { .. }
                | IndexManagerError::FieldRequiredChanged { .. }
                | IndexManagerError::AppendedFieldMustBeNullable { .. }
                | IndexManagerError::TimestampFieldChanged { .. }
                | IndexManagerError::StoredConfigIndexIdMismatch { .. }
                // A mapping the live schema contradicts: either the body is
                // wrong or it was computed from a state a concurrent update
                // has replaced. Same answer as the other mapping refusals —
                // re-read the index and send an update that fits it.
                | IndexManagerError::IndexSchemaFieldConflict { .. }
                | IndexManagerError::EmptyTemplatePatterns(_) => StatusCode::BAD_REQUEST,
                // Stored-state corruption, not anything the caller sent: a
                // template record whose key and body disagree. Falls through to
                // `Self::internal`, which logs it and keeps it out of the body.
                IndexManagerError::TemplateRecordIdMismatch { .. } => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        } else if chain_downcast_ref::<IndexConfigError>(&err).is_some() {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };

        if status == StatusCode::INTERNAL_SERVER_ERROR {
            Self::internal(err)
        } else {
            Self::new(status, format!("{err:#}"))
        }
    }
}

fn chain_downcast_ref<E: std::error::Error + 'static>(err: &AnyhowError) -> Option<&E> {
    err.chain().find_map(|cause| cause.downcast_ref::<E>())
}

fn retry_after(secs: u64) -> (HeaderName, HeaderValue) {
    (
        header::RETRY_AFTER,
        HeaderValue::from_str(&secs.max(1).to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("1")),
    )
}

/// The `ResourcesExhausted` message at the root of `err`, if that is what
/// failed — from wherever in the chain it sits.
///
/// DataFusion wraps a pool refusal several layers deep by the time a stream
/// yields it: `Context` around `ArrowError::ExternalError` around the
/// `ResourcesExhausted`, sometimes behind an `Arc` when a shared stream
/// reports it. `DataFusionError::find_root` unwinds that nesting (it is what
/// DataFusion's own tests match on); the anyhow chain walk on top of it covers
/// the `.context("stream item")` layers siglake adds. `Arc<DataFusionError>`
/// is matched explicitly because the `Arc`'s `source()` delegates but its
/// `downcast_ref::<DataFusionError>` does not.
pub fn pool_exhaustion(err: &AnyhowError) -> Option<String> {
    err.chain().find_map(|cause| {
        let df = cause.downcast_ref::<DataFusionError>().or_else(|| {
            cause
                .downcast_ref::<std::sync::Arc<DataFusionError>>()
                .map(|e| e.as_ref())
        })?;
        match df.find_root() {
            DataFusionError::ResourcesExhausted(detail) => Some(detail.clone()),
            _ => None,
        }
    })
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({"error": self.msg, "code": self.status.as_u16()});
        if let Some(Value::Object(ctx_map)) = self.context {
            if let Value::Object(body_map) = &mut body {
                for (k, v) in ctx_map {
                    body_map.insert(k, v);
                }
            }
        }
        let mut response = (self.status, Json(body)).into_response();
        for (name, value) in self.headers {
            response.headers_mut().insert(name, value);
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::ShardError;

    fn shard_err(code: u16) -> AnyhowError {
        anyhow::Error::new(ShardError {
            status: reqwest::StatusCode::from_u16(code).unwrap(),
            url: "http://worker:8089/api/v1/sql/shard".into(),
            body: r#"{"code":413,"error":"scanned 3075003 rows, ceiling 3075002"}"#.into(),
        })
        .context("coordinate")
    }

    /// A mapping refusal raised against the transaction's commit base arrives
    /// wrapped in the context `update_index` adds, which is where
    /// `from_index_manager` has to find it (#2552).
    #[test]
    fn a_mapping_refusal_under_context_answers_400() {
        let err = AnyhowError::new(IndexManagerError::IndexSchemaFieldConflict {
            index_id: "logs".to_string(),
            field: "level".to_string(),
            declared: "long".to_string(),
            actual: "string".to_string(),
        })
        .context("index `logs` changed while this update was in flight");
        let api = ApiError::from_index_manager(err);
        assert_eq!(api.status, StatusCode::BAD_REQUEST);
        assert!(
            api.msg.contains("live schema of index `logs`"),
            "the refusal reason must reach the caller: {}",
            api.msg
        );
    }

    fn retry_after_secs(err: &ApiError) -> Option<u64> {
        err.headers
            .iter()
            .find(|(name, _)| *name == header::RETRY_AFTER)
            .and_then(|(_, value)| value.to_str().ok()?.parse().ok())
    }

    /// What a pool refusal looks like by the time a collect loop sees it: the
    /// `ResourcesExhausted` sits under an `ArrowError::ExternalError` (the
    /// stream's item type is an Arrow result), under a DataFusion `Context`,
    /// under the anyhow `.context("stream item")` siglake adds.
    fn pool_refusal_as_streamed() -> AnyhowError {
        let root = DataFusionError::ResourcesExhausted(
            "Failed to allocate additional 6.2 MB for TopK[0] with 0 bytes already \
             allocated for this reservation - 1.9 MB remain available for the total pool"
                .to_string(),
        );
        let arrow = datafusion::arrow::error::ArrowError::ExternalError(Box::new(root));
        let df = DataFusionError::Context(
            "execute".to_string(),
            Box::new(DataFusionError::ArrowError(Box::new(arrow), None)),
        );
        AnyhowError::from(df).context("stream item")
    }

    #[test]
    fn forwards_a_shard_breaker_413() {
        // The breaker is a verdict about the query: a client must be able to
        // tell "narrow it" from "the server broke".
        let err = ApiError::from_query(shard_err(413), Priority::Interactive);
        assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            err.msg.contains("ceiling"),
            "the worker's own explanation must survive the hop: {}",
            err.msg
        );
    }

    #[test]
    fn forwards_other_query_refusals() {
        for code in [400u16, 429, 503, 504, 401, 403, 404, 422] {
            assert_eq!(
                ApiError::from_query(shard_err(code), Priority::Interactive)
                    .status
                    .as_u16(),
                code,
                "status {code} should be forwarded verbatim"
            );
        }
    }

    /// A worker whose pool refused answers 503; the coordinator used to turn
    /// that into a 500, so the one status the client could act on (wait, then
    /// retry) reached them as "the server broke".
    #[test]
    fn a_workers_pool_refusal_is_forwarded_with_retry_after() {
        let err = ApiError::from_query(shard_err(503), Priority::Interactive);
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_after_secs(&err),
            Some(POOL_EXHAUSTED_RETRY_AFTER_SECS),
            "a forwarded 503 must carry Retry-After: {:?}",
            err.headers
        );
    }

    #[test]
    fn a_worker_fault_stays_a_server_fault() {
        // A worker 500 or 502 really is our problem — don't launder it into a
        // status that tells the caller their query was at fault, or that
        // waiting will help.
        for code in [500u16, 502] {
            assert_eq!(
                ApiError::from_query(shard_err(code), Priority::Interactive).status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "status {code} must not be forwarded"
            );
        }
    }

    #[test]
    fn a_non_shard_failure_is_still_internal() {
        let err = ApiError::from_query(
            anyhow::anyhow!("object store unreachable"),
            Priority::Interactive,
        );
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(retry_after_secs(&err), None);
    }

    /// The pool doing its job is not a server fault. Before this, a
    /// `ResourcesExhausted` was formatted through `internal` like any other
    /// error: 500, no `Retry-After`, and a page for a capacity signal.
    #[test]
    fn a_pool_refusal_during_execution_is_a_503_with_retry_after() {
        let err = ApiError::from_execution(pool_refusal_as_streamed(), Priority::Interactive);
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_after_secs(&err),
            Some(POOL_EXHAUSTED_RETRY_AFTER_SECS)
        );
        assert!(
            err.msg.contains("TopK[0]") && err.msg.contains("1.9 MB remain"),
            "DataFusion's own account of the refusal must reach the client: {}",
            err.msg
        );
    }

    /// The coordinator's own execution (merge, buffer partial) can hit the
    /// pool with no shard involved; `from_query` must classify that too.
    #[test]
    fn from_query_classifies_the_coordinators_own_pool_refusal() {
        let err = ApiError::from_query(pool_refusal_as_streamed(), Priority::Interactive);
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_after_secs(&err),
            Some(POOL_EXHAUSTED_RETRY_AFTER_SECS)
        );
    }

    /// A bare `ResourcesExhausted` (no wrapping at all) and one behind the
    /// `Arc` a shared stream reports through are the two ends of the range.
    #[test]
    fn pool_exhaustion_is_found_at_either_end_of_the_chain() {
        let bare = AnyhowError::from(DataFusionError::ResourcesExhausted("bare".into()));
        assert_eq!(pool_exhaustion(&bare).as_deref(), Some("bare"));

        let shared = AnyhowError::from(DataFusionError::Shared(std::sync::Arc::new(
            DataFusionError::ResourcesExhausted("shared".into()),
        )))
        .context("stream item");
        assert_eq!(pool_exhaustion(&shared).as_deref(), Some("shared"));
    }

    /// #1525: an unresolvable shard pin is a retryable refusal that a client
    /// can tell apart from a pool refusal without reading the message — the two
    /// share a status and need different operator responses (converge the
    /// caches / lengthen snapshot retention vs. add memory).
    #[test]
    fn an_unresolved_shard_pin_is_a_typed_retryable_503() {
        let err = ApiError::shard_pin_unresolved(
            "events",
            8123456789012345678,
            Some(3),
            Some("b217f039-6577-4f01-a74b-329f641b686f"),
        );
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_after_secs(&err),
            Some(SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS)
        );
        let body = err.context.clone().expect("typed context");
        assert_eq!(body["reason"], SHARD_PIN_UNRESOLVED_REASON);
        assert_eq!(body["pin"]["table"], "events");
        assert_eq!(body["pin"]["snapshot_id"], 8123456789012345678i64);
        assert_eq!(body["pin"]["schema_id"], 3);
        assert_eq!(
            body["pin"]["table_uuid"],
            "b217f039-6577-4f01-a74b-329f641b686f"
        );
        // The rendered body carries both the discriminator and the status.
        let rendered = ApiError::shard_pin_unresolved("events", 7, None, None).into_response();
        assert_eq!(rendered.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(rendered.headers().contains_key(header::RETRY_AFTER));
    }

    /// #2554: the schema half of a pin refuses under the same contract — same
    /// status, same `Retry-After`, same `reason` — so a client needs no new
    /// handling, while the message and the echoed pin say which generation the
    /// worker could not honour.
    #[test]
    fn an_unresolved_pinned_schema_refuses_like_an_unresolved_snapshot() {
        let err = ApiError::shard_pin_schema_unresolved("events", Some(41), 7, None);
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            retry_after_secs(&err),
            Some(SHARD_PIN_UNRESOLVED_RETRY_AFTER_SECS)
        );
        let body = err.context.clone().expect("typed context");
        assert_eq!(body["reason"], SHARD_PIN_UNRESOLVED_REASON);
        assert_eq!(body["pin"]["snapshot_id"], 41);
        assert_eq!(body["pin"]["schema_id"], 7);
        assert!(
            err.msg.contains("schema 7 of `events`"),
            "the refusal must name the schema, not a snapshot: {}",
            err.msg
        );

        // An empty-table pin has no snapshot; the echoed pin says so rather
        // than inventing one.
        let empty = ApiError::shard_pin_schema_unresolved("events", None, 7, None);
        let body = empty.context.expect("typed context");
        assert!(body["pin"]["snapshot_id"].is_null(), "{body}");
    }

    /// Only a refusal is a refusal: the classifier must not read every
    /// DataFusion error as capacity, or a real fault would stop paging.
    #[test]
    fn other_datafusion_errors_stay_internal() {
        for df in [
            DataFusionError::Internal("bug".into()),
            DataFusionError::Execution("bad batch".into()),
            DataFusionError::Plan("no such column".into()),
        ] {
            let err = ApiError::from_execution(
                AnyhowError::from(df).context("stream item"),
                Priority::Interactive,
            );
            assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(retry_after_secs(&err), None);
        }
    }
}
