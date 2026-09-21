//! Distributed query coordinator (#7 part 2).
//!
//! Part 1 ([`siglake_storage::ScanShard`]) lets a worker scan a disjoint slice
//! of a table's files. This module is the coordinator that fans a query out
//! across `count` workers (shards `0..count`) and **merges** their partial
//! results back into the answer a single-pod query would have produced.
//!
//! ## Correctness
//!
//! Merging partial results is only valid for query shapes where the global
//! answer is a deterministic function of the per-shard answers. The classifier
//! ([`classify`]) is deliberately **conservative** — it distributes only:
//!
//! * **Two-phase aggregates** — `count` / `sum` / `min` / `max` (non-`DISTINCT`),
//!   with or without `GROUP BY`, and no `HAVING` / `ORDER BY` / `LIMIT` /
//!   `DISTINCT` / join / window. Each worker runs the original query on its
//!   shard; the coordinator re-aggregates the partials (`count`/`sum` → `sum`,
//!   `min` → `min`, `max` → `max`). Aggregate output columns must pass through
//!   unaliased/unwrapped so the partial schema matches.
//! * **Pure scans** — projection + filter (+ optional `LIMIT`), no aggregate /
//!   sort / join / distinct / window. Each worker returns its rows; the
//!   coordinator concatenates and re-applies the `LIMIT`.
//! * **Ordered aggregates** (#82) — a mergeable two-phase aggregate under a
//!   top-level `ORDER BY … [LIMIT n]` whose sort keys are output columns.
//!   Workers must NOT run the original query: a per-shard top-N over partial
//!   group values is not a superset of the global top-N (a group's total can
//!   dominate while every per-shard slice of it is small). Instead the
//!   classifier unparses the aggregate WITHOUT the sort/limit back to SQL
//!   ([`DistPlan::OrderedAggregate::worker_sql`]); each worker returns its
//!   complete per-shard groups, and the coordinator re-aggregates, then
//!   applies the `ORDER BY`/`LIMIT` once, over global totals.
//!
//! Everything else falls back to a single whole-table run ([`DistPlan::Local`])
//! — always correct, just not distributed. The exhaustive differential tests
//! assert the coordinated result equals the unsharded result for every
//! supported shape; the fallback covers the rest.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use siglake_storage::iceberg::TableGeneration;
use siglake_storage::ScanShard;

/// Hard bound for worker scan attribution carried in `x-siglake-scan`.
pub const SHARD_SCAN_HEADER_MAX_BYTES: usize = 16 * 1024;

fn decode_shard_scan_header(bytes: Option<&[u8]>) -> Result<Option<crate::format::ScanDetail>> {
    let bytes = bytes.context("shard response missing x-siglake-scan")?;
    anyhow::ensure!(
        bytes.len() <= SHARD_SCAN_HEADER_MAX_BYTES,
        "shard x-siglake-scan exceeds {} bytes",
        SHARD_SCAN_HEADER_MAX_BYTES
    );
    let mut scan: Option<crate::format::ScanDetail> =
        serde_json::from_slice(bytes).context("parse shard x-siglake-scan")?;
    if let Some(attribution) = scan
        .as_mut()
        .and_then(|detail| detail.file_attribution.take())
    {
        let mut normalized = crate::format::FileAttribution {
            files: Vec::new(),
            files_omitted: 0,
            identity_complete: true,
        };
        normalized.absorb(&attribution);
        scan.as_mut().expect("scan was Some").file_attribution = Some(normalized);
    }
    Ok(scan)
}

/// Runs a query against one shard of the file set, returning its result
/// batches. Implemented in-process ([`LocalShardRunner`]) for tests + the
/// single-pod fallback, and over HTTP for cross-pod fan-out (part 2b).
#[async_trait]
pub trait ShardRunner: Send + Sync {
    /// Execute `sql` scanning only `shard`'s files. `shard == None` scans the
    /// whole table (used for the non-distributable fallback).
    async fn run(&self, sql: &str, shard: Option<ScanShard>) -> Result<Vec<RecordBatch>>;

    /// Like [`Self::run`] but also returns the shard's scan attribution when
    /// the transport carries it (the HTTP runner reads the worker's
    /// `x-siglake-scan` header). Default: batches with no detail — in-process
    /// and test runners don't need to implement it.
    async fn run_detailed(&self, sql: &str, shard: Option<ScanShard>) -> Result<ShardRun> {
        Ok(ShardRun {
            batches: self.run(sql, shard).await?,
            scan: None,
        })
    }
}

/// One shard's result: its record batches plus (when the transport carries
/// it) the worker-side scan attribution for the response's `stats.scan`.
pub struct ShardRun {
    pub batches: Vec<RecordBatch>,
    pub scan: Option<crate::format::ScanDetail>,
}

/// Cross-pod [`ShardRunner`]: POSTs each shard's query to a peer worker's
/// `/api/v1/sql/shard` endpoint and decodes the Arrow-IPC response. `peers[i]`
/// serves shard `i`, so `peers.len()` is the shard count.
pub struct HttpShardRunner {
    client: reqwest::Client,
    peers: Vec<String>,
    /// Bearer token presented to peers (when they enforce auth).
    auth: Option<String>,
    /// #89/#1655/#2554/#2921: (table, serving generation) every shard request
    /// is pinned to. A `TableGeneration` with no snapshot id names the initial
    /// empty generation.
    pin: Option<(String, TableGeneration)>,
    /// The ORIGINAL caller's tenant, forwarded to every shard.
    ///
    /// Shard requests authenticate with the coordinator's service token, so
    /// without this the worker re-derives tenancy from a credential that
    /// belongs to no tenant. The worker accepts it only from a request bearing
    /// its own coordinator token.
    tenant: Option<String>,
}

impl HttpShardRunner {
    pub fn new(peers: Vec<String>, auth: Option<String>) -> Self {
        Self {
            // #94 follow-up: a DEAD peer must fail fast so the failover
            // retry costs ~ms, not the OS SYN timeout (live test measured
            // 2.4s). Healthy intra-VPC connects are sub-ms; 250ms is
            // generous. The overall request timeout stays unbounded — heavy
            // shards legitimately run long.
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_millis(250))
                .build()
                .expect("reqwest client"),
            peers,
            auth,
            pin: None,
            tenant: None,
        }
    }

    /// Forward `tenant` (the original caller's) on every shard request.
    pub fn with_tenant(mut self, tenant: Option<String>) -> Self {
        self.tenant = tenant;
        self
    }

    /// #89/#1655: pin every shard request to one captured table generation.
    /// A `generation` with no `snapshot_id` is the table's initial,
    /// snapshotless generation, not an omitted pin; workers serve an empty
    /// provider even if the first commit lands before dispatch.
    ///
    /// #2554: `generation` carries the schema id as well, and both halves must
    /// come from ONE capture — a schema id read separately could describe a
    /// generation the pinned snapshot never belonged to.
    pub fn with_pin(mut self, table: String, generation: TableGeneration) -> Self {
        self.pin = Some((table, generation));
        self
    }
    pub fn shard_count(&self) -> usize {
        self.peers.len()
    }
}

/// Is this shard failure worth retrying on the coordinator?
///
/// ONLY when the worker did not give an answer. The failover exists for a DEAD
/// worker (#94: dead worker -> 200 exact); it was written as `if run.is_err()`,
/// so it also fired on answers the worker meant.
///
/// Retrying a deliberate answer is worse than useless in every case:
///
/// - **429** is admission SHEDDING. The peer is saying it has no capacity, and
///   the coordinator responds by doing that shard's work itself — at a moment
///   when correlated load means its own pool is tightest. Load shedding becomes
///   load AMPLIFICATION, Nx on an N-peer fan-out, which is the shape of a
///   metastable failure: the more the cluster sheds, the more the coordinators
///   take on.
/// - **503** is the same signal by another name — and since #1525 it is also
///   how a worker says it cannot resolve the pinned snapshot. Re-running that
///   fragment is the one retry that could corrupt an answer rather than merely
///   waste work: the fallback would have to produce the shard from SOME
///   snapshot, and a fan-out whose fragments come from different file
///   generations is not a partition of the table. Refusing the query is the
///   only correct move.
/// - **413** is the mid-flight row breaker: a deterministic refusal of this
///   exact shard against limits the coordinator shares. Retrying pays a full
///   scan to reach the identical verdict.
/// - **504** is the worker's wall clock. Retrying spends another full timeout
///   to reach the same place, and doubles the work still running.
/// - **400** and other 4xx are the request's fault and will not improve.
///
/// A transport error — connection refused, DNS, a dropped socket — is not an
/// answer, and neither is a 500. Those are the failover's actual purpose.
fn shard_failure_is_retryable(err: &anyhow::Error) -> bool {
    match err.downcast_ref::<ShardError>() {
        // No status: the worker never answered. This is the dead-worker case.
        None => true,
        Some(e) => e.status == reqwest::StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// A shard worker answered with a non-success status. Carries that status so the
/// coordinator can forward the worker's verdict instead of flattening it — see
/// [`crate::error::ApiError::from_query`] for which statuses are forwarded.
#[derive(Debug)]
pub struct ShardError {
    pub status: reqwest::StatusCode,
    pub url: String,
    pub body: String,
}

impl std::fmt::Display for ShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The worker's own message is the useful part; keep it verbatim so a
        // breaker's "scanned N rows, ceiling M" survives the hop.
        let detail = self.body.trim();
        if detail.is_empty() {
            write!(f, "shard worker {} returned {}", self.url, self.status)
        } else {
            write!(
                f,
                "shard worker {} returned {}: {detail}",
                self.url, self.status
            )
        }
    }
}

impl std::error::Error for ShardError {}

#[async_trait]
impl ShardRunner for HttpShardRunner {
    async fn run(&self, sql: &str, shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
        Ok(self.run_detailed(sql, shard).await?.batches)
    }

    async fn run_detailed(&self, sql: &str, shard: Option<ScanShard>) -> Result<ShardRun> {
        // No shard ⇒ whole-table fallback, sent to peer 0 as shard {0,1}.
        let (index, count) = shard.map_or((0, 1), |s| (s.index, s.count));
        let peer = self
            .peers
            .get(index)
            .with_context(|| format!("shard {index} has no peer (have {})", self.peers.len()))?;
        let url = format!("{}/api/v1/sql/shard", peer.trim_end_matches('/'));
        let mut body = serde_json::json!({
            "query": sql,
            "shard": { "index": index, "count": count },
        });
        if let Some((table, generation)) = &self.pin {
            // `schema_id` and `table_uuid` ride on both arms. A worker built
            // before either field existed ignores it, which preserves the
            // documented mixed-version contract at that worker's older level.
            body["pin"] = match generation.snapshot_id {
                Some(snapshot_id) => serde_json::json!({
                    "table": table,
                    "snapshot_id": snapshot_id,
                    "schema_id": generation.schema_id,
                    "table_uuid": generation.table_uuid,
                }),
                None => serde_json::json!({
                    "table": table,
                    "empty": true,
                    "schema_id": generation.schema_id,
                    "table_uuid": generation.table_uuid,
                }),
            };
        }
        if let Some(tenant) = &self.tenant {
            body["tenant"] = serde_json::json!(tenant);
        }
        let mut rb = self.client.post(&url).json(&body);
        if let Some(t) = &self.auth {
            rb = rb.bearer_auth(t);
        }
        // W3C trace-context injection: propagate the current OTel context (the
        // coordinator's request span) as `traceparent` so the worker's shard
        // span becomes a child. No-op when OTel is off (no propagator set).
        let mut trace_headers = reqwest::header::HeaderMap::new();
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        let current_context = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|p| {
            p.inject_context(
                &current_context,
                &mut opentelemetry_http::HeaderInjector(&mut trace_headers),
            );
        });
        if !trace_headers.is_empty() {
            rb = rb.headers(trace_headers);
        }
        let resp = rb.send().await.with_context(|| format!("POST {url}"))?;
        // A shard worker's own refusal (413 row-ceiling breaker, 400 bad query,
        // 429 budget, 503 memory pool) is a statement ABOUT THE QUERY or its
        // moment, not a server fault. Carry
        // its status so the coordinator can hand the caller the same answer a
        // single-node deployment would — `error_for_status()` erases it, which
        // surfaced every breaker 413 to clients as an opaque 500.
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ShardError { status, url, body }.into());
        }
        let scan = decode_shard_scan_header(
            resp.headers()
                .get("x-siglake-scan")
                .map(|header| header.as_bytes()),
        )?;
        let bytes = resp.bytes().await.context("read shard response")?;
        Ok(ShardRun {
            batches: crate::format::arrow_ipc_to_batches(&bytes)?,
            scan,
        })
    }
}

/// How a partial aggregate column is re-aggregated on the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeFn {
    Sum,
    Min,
    Max,
}

impl MergeFn {
    fn from_func(name: &str) -> Option<Self> {
        match name {
            // count + sum both re-aggregate by summing the partial counts/sums.
            "count" | "sum" => Some(MergeFn::Sum),
            "min" => Some(MergeFn::Min),
            "max" => Some(MergeFn::Max),
            _ => None,
        }
    }
    fn sql(self) -> &'static str {
        match self {
            MergeFn::Sum => "sum",
            MergeFn::Min => "min",
            MergeFn::Max => "max",
        }
    }
}

/// The coordinator's plan for a query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistPlan {
    /// Two-phase aggregate: run the original query per shard, then merge.
    /// `key_names` are the `GROUP BY` output columns; `agg_fns` maps each
    /// aggregate output column to its merge function.
    Aggregate {
        key_names: HashSet<String>,
        agg_fns: HashMap<String, MergeFn>,
    },
    /// Pure scan: run per shard, concatenate, re-apply `limit`.
    Scan { limit: Option<usize> },
    /// Ordered scan (`ORDER BY … [LIMIT n]` over a pure scan): each shard
    /// sorts + limits its slice, the coordinator merge-sorts the partials and
    /// re-applies the limit. `order_by` is the rendered `ORDER BY` clause over
    /// output columns; `limit` is the effective top-N (None ⇒ full sort).
    OrderedScan {
        order_by: String,
        limit: Option<usize>,
    },
    /// #82: a mergeable aggregate under a top-level `ORDER BY … [LIMIT n]`.
    /// Workers run `worker_sql` — the aggregate with the sort/limit STRIPPED
    /// (unparsed from the plan), so every shard returns its complete groups;
    /// the coordinator re-aggregates the partials and applies the rendered
    /// `order_by` + `limit` once, over global totals. Shipping the original
    /// query instead would be wrong: a per-shard top-N over partial group
    /// values is not a superset of the global top-N.
    OrderedAggregate {
        key_names: HashSet<String>,
        agg_fns: HashMap<String, MergeFn>,
        order_by: String,
        limit: Option<usize>,
        worker_sql: String,
    },
    /// Not safely mergeable — run the whole table on one worker.
    Local,
}

/// Classify a (already-planned) query into a [`DistPlan`].
///
/// STRUCTURAL check only (#86): any query over exactly ONE relation is a
/// fan-out candidate — the shape decides mergeability. The caller owns the
/// safety gate on WHICH table may distribute: `distributed_inner` fans out
/// only `events` or a managed user index, so hot-cache table functions
/// (`last_values()`), detection tables, and joins never reach a worker (a
/// UDTF isn't registered there; a no-table `SELECT 1` would be multiplied by
/// the union — `scanned_table` returns `None` for both).
pub fn classify(plan: &LogicalPlan) -> DistPlan {
    if scanned_table(plan).is_none() {
        return DistPlan::Local;
    }
    if let Some(dist) = classify_aggregate(plan) {
        return dist;
    }
    if let Some(dist) = ordered_aggregate(plan) {
        return dist;
    }
    if let Some(dist) = ordered_scan(plan) {
        return dist;
    }
    match scan_limit(plan) {
        Some(limit) => DistPlan::Scan { limit },
        None => DistPlan::Local,
    }
}

/// The single table `plan` reads, if it reads exactly one distinct relation
/// (at least one scan, every scan the same name). `None` for joins across
/// different relations or no-table queries.
fn scanned_table(plan: &LogicalPlan) -> Option<String> {
    fn collect<'a>(plan: &'a LogicalPlan, out: &mut Vec<&'a str>) {
        if let LogicalPlan::TableScan(ts) = plan {
            out.push(ts.table_name.table());
        }
        for input in plan.inputs() {
            collect(input, out);
        }
    }
    let mut tables = Vec::new();
    collect(plan, &mut tables);
    let first = *tables.first()?;
    tables
        .iter()
        .all(|t| *t == first)
        .then(|| first.to_string())
}

/// `true` if `plan` is a pure scan: projection / filter / table-scan only.
fn is_pure_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => true,
        LogicalPlan::Projection(p) => is_pure_scan(&p.input),
        LogicalPlan::Filter(f) => is_pure_scan(&f.input),
        LogicalPlan::SubqueryAlias(s) => is_pure_scan(&s.input),
        _ => false,
    }
}

/// `Some(OrderedScan)` if `plan` is `ORDER BY … [LIMIT n]` over a pure scan
/// **and every sort key is a column present in the output** (so the per-shard
/// partials carry the key for the merge sort). `None` otherwise — the caller
/// then tries the plain-scan path. Sort keys that are expressions, or columns
/// not projected into the output, fall through to the single-pod path.
fn ordered_scan(plan: &LogicalPlan) -> Option<DistPlan> {
    let (order_by, limit, input) = ordered_root(plan)?;
    if !is_pure_scan(input) {
        return None;
    }
    Some(DistPlan::OrderedScan { order_by, limit })
}

/// Unwrap a top-level `[LIMIT] → Sort` root whose every sort key is a column
/// present in the plan's output (so per-shard partials carry it for the
/// merge). Returns the rendered `ORDER BY` clause, the effective top-N
/// (tightest of the outer `LIMIT` and the Sort's fetch), and the Sort's input.
fn ordered_root(plan: &LogicalPlan) -> Option<(String, Option<usize>, &LogicalPlan)> {
    // Output columns the workers will return (the merge can only sort by these).
    let out: HashSet<&str> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // Unwrap an optional top-level LIMIT (skip 0) wrapping the Sort.
    let (outer_fetch, inner) = match plan {
        LogicalPlan::Limit(l) => {
            let skip_zero = l.skip.as_deref().is_none()
                || matches!(l.skip.as_deref(), Some(Expr::Literal(s, _)) if is_zero_lit(s));
            if !skip_zero {
                return None;
            }
            let fetch = match l.fetch.as_deref() {
                Some(Expr::Literal(s, _)) => lit_usize(s),
                None => None,
                _ => return None,
            };
            (fetch, l.input.as_ref())
        }
        other => (None, other),
    };

    let LogicalPlan::Sort(sort) = inner else {
        return None;
    };

    let mut terms = Vec::with_capacity(sort.expr.len());
    for s in &sort.expr {
        let Expr::Column(c) = &s.expr else {
            return None; // expression sort keys aren't merge-safe here
        };
        if !out.contains(c.name.as_str()) {
            return None; // sort key isn't in the partials' columns
        }
        terms.push(format!(
            "{} {} NULLS {}",
            quote_ident(&c.name),
            if s.asc { "ASC" } else { "DESC" },
            if s.nulls_first { "FIRST" } else { "LAST" },
        ));
    }
    if terms.is_empty() {
        return None;
    }
    // Effective top-N is the tightest of the outer LIMIT and the Sort's fetch.
    let limit = match (outer_fetch, sort.fetch) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    Some((terms.join(", "), limit, sort.input.as_ref()))
}

/// #82: `Some(OrderedAggregate)` for `ORDER BY … [LIMIT n]` over a mergeable
/// aggregate. `Some(Local)` when the sorted input IS an aggregate but not
/// mergeable (the whole query must stay single-pod). `None` when the root
/// isn't an ordered aggregate at all (caller tries the scan paths). Requires
/// the sort/limit-stripped aggregate to unparse back to worker SQL — an
/// unparse failure conservatively falls back to `Local`.
fn ordered_aggregate(plan: &LogicalPlan) -> Option<DistPlan> {
    let (order_by, limit, input) = ordered_root(plan)?;
    match classify_aggregate(input)? {
        DistPlan::Aggregate { key_names, agg_fns } => {
            let worker_sql = match datafusion::sql::unparser::plan_to_sql(input) {
                Ok(stmt) => stmt.to_string(),
                Err(e) => {
                    tracing::debug!(error = %e,
                        "ordered aggregate: unparse failed; falling back to single-pod");
                    return Some(DistPlan::Local);
                }
            };
            Some(DistPlan::OrderedAggregate {
                key_names,
                agg_fns,
                order_by,
                limit,
                worker_sql,
            })
        }
        other => Some(other), // Local: aggregate but not mergeable
    }
}

/// The merge role of one aggregate-output column.
enum AggRole {
    Key,
    Agg(MergeFn),
}

/// Classify an aggregate-rooted plan. Returns `None` if `plan` is not an
/// aggregate (or aggregate-under-a-pass-through-projection) — the caller then
/// tries the scan path. Returns `Some(DistPlan::Local)` when it *is* an
/// aggregate but is not shard-mergeable (e.g. `DISTINCT` / `avg`), and
/// `Some(DistPlan::Aggregate{..})` when it is.
///
/// DataFusion always wraps an aggregate in a projection that aliases the
/// internal aggregate column (`count(Int64(1))`) to its display name
/// (`count(*)`), so the common case is `Projection(pass-through) → Aggregate`,
/// not a bare `Aggregate`. We accept a projection whose every expression is a
/// column or an aliased column (rename/reorder, but no computation) and key the
/// merge functions by the **output** column names — exactly what each worker
/// returns when it runs the original SQL.
fn classify_aggregate(plan: &LogicalPlan) -> Option<DistPlan> {
    use datafusion::logical_expr::Aggregate;
    let (agg, proj): (&Aggregate, Option<&datafusion::logical_expr::Projection>) = match plan {
        LogicalPlan::Aggregate(a) => (a, None),
        LogicalPlan::Projection(p) => match p.input.as_ref() {
            LogicalPlan::Aggregate(a) => (a, Some(p)),
            _ => return None,
        },
        _ => return None,
    };

    // THE AGGREGATE'S INPUT MUST BE A PURE SCAN.
    //
    // Merging partials by summing counts and maxing maxima is only valid when
    // each worker computed its partial over a DISJOINT SLICE OF ROWS. That
    // holds when the input is a scan the coordinator sharded by file; it does
    // not hold when the input is itself a subquery, because the subquery is
    // then evaluated per-shard and merged as though it had been evaluated
    // globally.
    //
    // `count(*) FROM (SELECT DISTINCT host ...)` sums per-shard distinct counts
    // and over-counts every host appearing in more than one shard -- and it is
    // the rewrite a user reaches for precisely because `count(DISTINCT host)` is
    // correctly refused just below. `count(*) FROM (... LIMIT 10)` returns ten
    // times the peer count.
    //
    // Both sibling classifiers, `ordered_scan` and `scan_limit`, already gate on
    // `is_pure_scan`; this one was the exception.
    if !is_pure_scan(&agg.input) {
        return Some(DistPlan::Local);
    }

    // Role of each aggregate-internal output column (group keys come first in
    // the aggregate's own schema, then the aggregates in `aggr_expr` order).
    let n_keys = agg.group_expr.len();
    let agg_fields = agg.schema.fields();
    let mut role: HashMap<String, AggRole> = HashMap::new();
    for f in agg_fields.iter().take(n_keys) {
        role.insert(f.name().to_string(), AggRole::Key);
    }
    for (j, aggr) in agg.aggr_expr.iter().enumerate() {
        let Expr::AggregateFunction(AggregateFunction { func, params }) = aggr else {
            return Some(DistPlan::Local);
        };
        if params.distinct {
            return Some(DistPlan::Local);
        }
        let Some(mf) = MergeFn::from_func(func.name()) else {
            return Some(DistPlan::Local);
        };
        role.insert(agg_fields[n_keys + j].name().to_string(), AggRole::Agg(mf));
    }

    // Resolve to OUTPUT column names — what the workers' partials are named.
    let mut key_names = HashSet::new();
    let mut agg_fns = HashMap::new();
    let mut record = |out_name: &str, internal: &str| -> bool {
        match role.get(internal) {
            Some(AggRole::Key) => {
                key_names.insert(out_name.to_string());
                true
            }
            Some(AggRole::Agg(mf)) => {
                agg_fns.insert(out_name.to_string(), *mf);
                true
            }
            None => false,
        }
    };
    match proj {
        // Bare aggregate: output names are the aggregate-schema names directly.
        None => {
            for f in agg_fields.iter() {
                if !record(f.name(), f.name()) {
                    return Some(DistPlan::Local);
                }
            }
        }
        // Pass-through projection: each expr must be a (possibly aliased) column
        // referencing an aggregate output; the output field name is what ships.
        Some(p) => {
            for (field, expr) in p.schema.fields().iter().zip(p.expr.iter()) {
                // Peel any nesting of aliases — DataFusion can double-alias
                // (`count(Int64(1)) AS count(*) AS n`). A pass-through projection
                // bottoms out at a plain column reference; anything computed
                // (`count(*) * 2`) is not mergeable.
                let mut inner = expr;
                while let Expr::Alias(a) = inner {
                    inner = a.expr.as_ref();
                }
                let Expr::Column(c) = inner else {
                    return Some(DistPlan::Local);
                };
                if !record(field.name(), &c.name) {
                    return Some(DistPlan::Local);
                }
            }
        }
    }
    Some(DistPlan::Aggregate { key_names, agg_fns })
}

/// `Some(limit)` if `plan` is a pure scan (projection/filter/table-scan, with
/// an optional top-level `LIMIT` and no `OFFSET`); `None` otherwise.
fn scan_limit(plan: &LogicalPlan) -> Option<Option<usize>> {
    match plan {
        LogicalPlan::Limit(l) => {
            // A non-zero offset can't be re-applied after a cross-shard union.
            let skip_zero = l.skip.as_deref().is_none()
                || matches!(l.skip.as_deref(), Some(Expr::Literal(s, _)) if is_zero_lit(s));
            if !skip_zero || !is_pure_scan(&l.input) {
                return None;
            }
            let fetch = match l.fetch.as_deref() {
                Some(Expr::Literal(s, _)) => lit_usize(s),
                None => None,
                _ => return None,
            };
            Some(fetch)
        }
        _ if is_pure_scan(plan) => Some(None),
        _ => None,
    }
}

fn is_zero_lit(s: &datafusion::scalar::ScalarValue) -> bool {
    lit_usize(s) == Some(0)
}

fn lit_usize(s: &datafusion::scalar::ScalarValue) -> Option<usize> {
    use datafusion::scalar::ScalarValue::*;
    match s {
        Int64(Some(v)) if *v >= 0 => Some(*v as usize),
        UInt64(Some(v)) => Some(*v as usize),
        Int32(Some(v)) if *v >= 0 => Some(*v as usize),
        UInt32(Some(v)) => Some(*v as usize),
        _ => None,
    }
}

/// Coordinate `sql` across `count` shards using `runner` for each shard.
///
/// `planning_ctx` must have the query's tables registered (it is used only to
/// build the logical plan for classification, not to scan). Returns the merged
/// result batches — identical to what a single-pod run would produce for every
/// distributable shape; for non-distributable shapes it runs the whole table.
pub async fn coordinate(
    planning_ctx: &SessionContext,
    runner: &dyn ShardRunner,
    sql: &str,
    count: usize,
) -> Result<Vec<RecordBatch>> {
    coordinate_with_extra(planning_ctx, runner, sql, count, Vec::new()).await
}

/// Like [`coordinate`], but folds `extra_partials` into the merge alongside the
/// per-shard worker partials. WS-6 uses this to inject a WAL-buffer partial
/// (the un-committed `events` rows, computed once on the coordinator over a
/// buffer-only table) so a distributed query sees just-ingested data. The extra
/// partials MUST be in the same shape as a worker's per-shard partial — i.e.
/// the result of running `sql` over a disjoint slice of `events` — so the
/// existing per-shape merge (aggregate re-merge, ordered merge, scan concat)
/// stays correct. Callers pass these only for distributable plans (the
/// `Local` / `count <= 1` whole-table paths can't merge an extra partial).
pub async fn coordinate_with_extra(
    planning_ctx: &SessionContext,
    runner: &dyn ShardRunner,
    sql: &str,
    count: usize,
    extra_partials: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    coordinate_with_extra_stats(planning_ctx, runner, sql, count, extra_partials)
        .await
        .map(|(batches, _)| batches)
}

/// Coordinator-side attribution for one coordinated query (#85): which merge
/// mode ran, each shard's end-to-end wall, and the merge cost. Surfaced on the
/// response as [`crate::format::DistPhaseStats`].
#[derive(Debug, Clone, Default)]
pub struct CoordStats {
    pub mode: &'static str,
    pub plan_micros: u64,
    pub shard_wall_micros: Vec<u64>,
    pub merge_micros: u64,
    /// Summed worker-side scan attribution (None when no shard reported it —
    /// in-process runners, Tier-1, or pre-upgrade workers).
    pub scan: Option<crate::format::ScanDetail>,
}

/// [`coordinate_with_extra`] plus per-query coordinator stats.
pub async fn coordinate_with_extra_stats(
    planning_ctx: &SessionContext,
    runner: &dyn ShardRunner,
    sql: &str,
    count: usize,
    extra_partials: Vec<RecordBatch>,
) -> Result<(Vec<RecordBatch>, CoordStats)> {
    coordinate_with_failover(planning_ctx, runner, None, sql, count, extra_partials).await
}

/// #94: [`coordinate_with_extra_stats`] with per-shard FAILOVER. When a
/// shard's worker fails (dead peer, timeout, 5xx), the shard is retried on
/// `fallback` — the coordinator's own in-process runner, which holds the full
/// file set and can execute any shard's slice — so one dead worker degrades
/// that shard's latency instead of failing the whole query. Failovers are
/// observable (`siglake_query_coordinator_failover_total`) and visible in the
/// per-shard walls.
pub async fn coordinate_with_failover(
    planning_ctx: &SessionContext,
    runner: &dyn ShardRunner,
    fallback: Option<&dyn ShardRunner>,
    sql: &str,
    count: usize,
    extra_partials: Vec<RecordBatch>,
) -> Result<(Vec<RecordBatch>, CoordStats)> {
    let plan_started = std::time::Instant::now();
    let plan = planning_ctx
        .state()
        .create_logical_plan(sql)
        .await
        .context("coordinator: plan query")?;
    let dist = classify(&plan);
    let plan_micros = plan_started.elapsed().as_micros() as u64;

    // count <= 1, or a non-distributable plan ⇒ single whole-table run. The
    // caller guarantees `extra_partials` is empty in these cases (a whole-table
    // run already covers everything the workers would, and an extra partial
    // can't be merged into it generically); assert that contract in debug.
    let shards = match (count, &dist) {
        (0..=1, _) | (_, DistPlan::Local) => {
            debug_assert!(
                extra_partials.is_empty(),
                "extra_partials passed for a non-distributable plan"
            );
            metrics::counter!("siglake_query_coordinator_total", "mode" => "local").increment(1);
            let started = std::time::Instant::now();
            let run = runner.run_detailed(sql, None).await?;
            let stats = CoordStats {
                mode: "local",
                plan_micros,
                shard_wall_micros: vec![started.elapsed().as_micros() as u64],
                merge_micros: 0,
                scan: run.scan,
            };
            return Ok((run.batches, stats));
        }
        (n, _) => n,
    };

    // Fan out CONCURRENTLY: run the shard query on each shard — the original
    // for most shapes; an ordered aggregate ships the sort/limit-stripped
    // rewrite so every shard returns its COMPLETE groups (see
    // `DistPlan::OrderedAggregate`). Concurrency matters: the sequential loop
    // this replaces made every multi-shard query pay the SUM of worker walls
    // instead of the max.
    let shard_sql: &str = match &dist {
        DistPlan::OrderedAggregate { worker_sql, .. } => worker_sql,
        _ => sql,
    };
    let shard_runs = futures::future::join_all((0..shards).map(|index| async move {
        let started = std::time::Instant::now();
        let shard = ScanShard::new(index, shards);
        let mut run = runner.run_detailed(shard_sql, shard).await;
        if let Some(err) = run.as_ref().err() {
            let retryable = shard_failure_is_retryable(err);
            if !retryable {
                // The worker ANSWERED. Forward its verdict rather than doing its
                // work: retrying a 429 or 503 turns shedding into amplification,
                // and retrying a 413 or 504 buys the same answer at full price.
                metrics::counter!("siglake_query_coordinator_failover_skipped_total").increment(1);
                tracing::debug!(shard = index, error = ?err,
                    "shard worker answered with a verdict; forwarding it rather \
                     than retrying on the coordinator");
            } else if let Some(fallback) = fallback {
                metrics::counter!("siglake_query_coordinator_failover_total").increment(1);
                tracing::warn!(shard = index, error = ?err,
                    "shard worker did not answer; retrying the shard on the coordinator");
                run = fallback.run_detailed(shard_sql, shard).await;
            }
        }
        (run, started.elapsed().as_micros() as u64)
    }))
    .await;
    let mut partials: Vec<RecordBatch> = Vec::new();
    let mut shard_wall_micros = Vec::with_capacity(shards);
    let mut scan: Option<crate::format::ScanDetail> = None;
    let mut shard_missing_file_attribution = false;
    for (run, wall) in shard_runs {
        let mut run = run?;
        partials.append(&mut run.batches);
        shard_wall_micros.push(wall);
        if let Some(s) = run.scan {
            shard_missing_file_attribution |= s.file_attribution.is_none();
            scan.get_or_insert_with(Default::default).absorb(&s);
        }
    }
    if shard_missing_file_attribution {
        if let Some(attribution) = scan
            .as_mut()
            .and_then(|detail| detail.file_attribution.as_mut())
        {
            attribution.identity_complete = false;
        }
    }
    // Fold in any caller-supplied partials (WS-6 WAL buffer) before the merge.
    partials.extend(extra_partials);

    let merge_started = std::time::Instant::now();
    let (mode, merged) = match dist {
        DistPlan::Scan { limit } => {
            metrics::counter!("siglake_query_coordinator_total", "mode" => "scan").increment(1);
            ("scan", apply_limit(partials, limit))
        }
        DistPlan::OrderedScan { order_by, limit } => {
            metrics::counter!("siglake_query_coordinator_total", "mode" => "ordered_scan")
                .increment(1);
            (
                "ordered_scan",
                merge_ordered(partials, &order_by, limit).await?,
            )
        }
        DistPlan::Aggregate { key_names, agg_fns } => {
            metrics::counter!("siglake_query_coordinator_total", "mode" => "aggregate")
                .increment(1);
            (
                "aggregate",
                merge_aggregate(partials, &key_names, &agg_fns, None).await?,
            )
        }
        DistPlan::OrderedAggregate {
            key_names,
            agg_fns,
            order_by,
            limit,
            ..
        } => {
            metrics::counter!("siglake_query_coordinator_total", "mode" => "ordered_aggregate")
                .increment(1);
            (
                "ordered_aggregate",
                merge_aggregate(partials, &key_names, &agg_fns, Some((&order_by, limit))).await?,
            )
        }
        DistPlan::Local => unreachable!("handled above"),
    };
    let stats = CoordStats {
        mode,
        plan_micros,
        shard_wall_micros,
        merge_micros: merge_started.elapsed().as_micros() as u64,
        scan,
    };
    Ok((merged, stats))
}

/// Concatenate scan partials, truncating to `limit` rows if set.
fn apply_limit(partials: Vec<RecordBatch>, limit: Option<usize>) -> Vec<RecordBatch> {
    let Some(limit) = limit else {
        return partials;
    };
    let mut out = Vec::new();
    let mut taken = 0usize;
    for b in partials {
        if taken >= limit {
            break;
        }
        let take = (limit - taken).min(b.num_rows());
        if take == b.num_rows() {
            taken += b.num_rows();
            out.push(b);
        } else {
            out.push(b.slice(0, take));
            break;
        }
    }
    out
}

/// Unify per-shard partial schemas before registering them as one `MemTable`.
///
/// Shards running the same SQL can disagree on field **nullability** for the
/// same output column: a shard whose scan answers `min`/`max`(`count`) from
/// exact file statistics emits non-nullable literal fields, while a shard that
/// really aggregates emits DataFusion's nullable aggregate fields. Which shards
/// take which path depends on the file→shard split, so a mixed batch set is
/// timing/layout-dependent — this surfaced as the `coordinated_aggregates_
/// match_unsharded` flake ("Mismatch between schema and batches") and would
/// equally 500 the production HTTP coordinator. Relaxing every field to
/// nullable is always safe and makes the partials registrable as one table; a
/// genuine type mismatch still fails, now with a clear context.
///
/// Returns `None` when there are no partials at all (empty result).
fn normalize_partials(
    partials: Vec<RecordBatch>,
) -> Result<Option<(arrow::datatypes::SchemaRef, Vec<RecordBatch>)>> {
    use arrow::datatypes::Schema;
    let Some(first) = partials.first() else {
        return Ok(None);
    };
    let schema = Arc::new(Schema::new_with_metadata(
        first
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
        first.schema().metadata().clone(),
    ));
    let batches = partials
        .into_iter()
        .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("coordinator: normalize partial schemas")?;
    Ok(Some((schema, batches)))
}

/// Re-aggregate per-shard partials by registering them as an in-memory table
/// and running a generated merge query (DataFusion does the actual
/// aggregation — we only choose the per-column merge function). `order`
/// (#82 ordered aggregates) appends `ORDER BY <clause> [LIMIT n]` to the
/// merge, applied over the GLOBAL totals — the only point where sorting
/// grouped results is exact.
async fn merge_aggregate(
    partials: Vec<RecordBatch>,
    key_names: &HashSet<String>,
    agg_fns: &HashMap<String, MergeFn>,
    order: Option<(&str, Option<usize>)>,
) -> Result<Vec<RecordBatch>> {
    let Some((schema, partials)) = normalize_partials(partials)? else {
        // No partials at all (every shard returned nothing) — empty result.
        return Ok(Vec::new());
    };

    // Build the merge SELECT in the partial schema's column order so the
    // coordinated result schema matches the unsharded one.
    let mut select = Vec::new();
    let mut group = Vec::new();
    for field in schema.fields() {
        let name = field.name();
        let q = quote_ident(name);
        if let Some(mf) = agg_fns.get(name) {
            select.push(format!("{}({}) AS {}", mf.sql(), q, q));
        } else if key_names.contains(name) {
            select.push(q.clone());
            group.push(q);
        } else {
            // A column that is neither a known key nor a known aggregate means
            // the classifier and schema disagree — refuse rather than guess.
            anyhow::bail!("coordinator merge: unclassified column `{name}`");
        }
    }
    let mut merge_sql = format!("SELECT {} FROM \"__shard_partials\"", select.join(", "));
    if !group.is_empty() {
        merge_sql.push_str(" GROUP BY ");
        merge_sql.push_str(&group.join(", "));
    }
    if let Some((order_by, limit)) = order {
        merge_sql.push_str(" ORDER BY ");
        merge_sql.push_str(order_by);
        if let Some(n) = limit {
            merge_sql.push_str(&format!(" LIMIT {n}"));
        }
    }

    let ctx = siglake_storage::session_context_with(None, None);
    let table =
        MemTable::try_new(schema, vec![partials]).context("coordinator: partials MemTable")?;
    ctx.register_table("__shard_partials", Arc::new(table))
        .context("coordinator: register partials")?;
    let df = ctx
        .sql(&merge_sql)
        .await
        .with_context(|| format!("coordinator: merge query `{merge_sql}`"))?;
    df.collect().await.context("coordinator: collect merge")
}

/// Merge per-shard ordered partials. Each shard already returned its slice
/// sorted (+ limited) by `order_by`; the coordinator concatenates them, re-sorts
/// by the same keys, and re-applies the top-N `limit`. Distributed top-N is
/// exact because each shard's local top-N is a superset of its contribution to
/// the global top-N.
async fn merge_ordered(
    partials: Vec<RecordBatch>,
    order_by: &str,
    limit: Option<usize>,
) -> Result<Vec<RecordBatch>> {
    let Some((schema, partials)) = normalize_partials(partials)? else {
        return Ok(Vec::new());
    };
    let mut merge_sql = format!("SELECT * FROM \"__shard_partials\" ORDER BY {order_by}");
    if let Some(n) = limit {
        merge_sql.push_str(&format!(" LIMIT {n}"));
    }
    let ctx = siglake_storage::session_context_with(None, None);
    let table =
        MemTable::try_new(schema, vec![partials]).context("coordinator: partials MemTable")?;
    ctx.register_table("__shard_partials", Arc::new(table))
        .context("coordinator: register partials")?;
    let df = ctx
        .sql(&merge_sql)
        .await
        .with_context(|| format!("coordinator: ordered merge `{merge_sql}`"))?;
    df.collect()
        .await
        .context("coordinator: collect ordered merge")
}

/// Double-quote a SQL identifier, escaping embedded quotes — agg output names
/// like `count(*)` / `sum(events.bytes)` must be quoted to be referenced.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    /// A worker's ANSWER must not be retried on the coordinator.
    ///
    /// THE DEFECT. The failover was `if run.is_err()`, so it fired on every
    /// failure — including the ones the worker meant. A peer returning 429
    /// (admission shedding) had its shard re-run by the coordinator, at the
    /// moment correlated load makes the coordinator's own pool tightest. Load
    /// shedding became load AMPLIFICATION, Nx on an N-peer fan-out: the more
    /// the cluster sheds, the more work the coordinators take on. That is the
    /// shape of a metastable failure — it does not recover when load drops,
    /// because the amplification is self-sustaining.
    ///
    /// 413 and 504 are the same mistake more cheaply: both are deterministic
    /// against limits the coordinator shares, so the retry buys the identical
    /// verdict at the price of a full scan or a full timeout.
    #[test]
    fn a_workers_verdict_is_forwarded_not_retried() {
        let verdicts = [
            (reqwest::StatusCode::TOO_MANY_REQUESTS, "admission shedding"),
            (reqwest::StatusCode::SERVICE_UNAVAILABLE, "overloaded"),
            (reqwest::StatusCode::PAYLOAD_TOO_LARGE, "row breaker"),
            (reqwest::StatusCode::GATEWAY_TIMEOUT, "wall clock"),
            (reqwest::StatusCode::BAD_REQUEST, "bad request"),
        ];
        for (status, why) in verdicts {
            let err = anyhow::Error::from(ShardError {
                status,
                url: "http://peer/api/v1/sql/shard".to_string(),
                body: String::new(),
            });
            assert!(
                !shard_failure_is_retryable(&err),
                "{status} ({why}) would be retried on the coordinator"
            );
        }
    }

    /// But a worker that did NOT answer must still fail over — that is what the
    /// failover is for, and it was proven live against a dead worker (#94).
    #[test]
    fn a_worker_that_never_answered_still_fails_over() {
        // No `ShardError` in the chain: a transport failure, not a verdict.
        let transport = anyhow::anyhow!("connection refused");
        assert!(
            shard_failure_is_retryable(&transport),
            "a dead worker must still fail over, or #94's guarantee is lost"
        );
        let internal = anyhow::Error::from(ShardError {
            status: reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            url: "http://peer/api/v1/sql/shard".to_string(),
            body: String::new(),
        });
        assert!(
            shard_failure_is_retryable(&internal),
            "a worker that broke internally should still be retried"
        );
    }

    #[test]
    fn shard_scan_transport_refuses_malformed_and_oversized_attribution() {
        let missing = decode_shard_scan_header(None).unwrap_err();
        assert!(missing.to_string().contains("missing x-siglake-scan"));

        let malformed = decode_shard_scan_header(Some(b"{not-json}")).unwrap_err();
        assert!(malformed.to_string().contains("parse shard x-siglake-scan"));

        let oversized = vec![b'x'; SHARD_SCAN_HEADER_MAX_BYTES + 1];
        let error = decode_shard_scan_header(Some(&oversized)).unwrap_err();
        assert!(error.to_string().contains("exceeds"));

        assert!(decode_shard_scan_header(Some(b"null")).unwrap().is_none());

        let contradicted = serde_json::to_vec(&crate::format::ScanDetail {
            file_attribution: Some(crate::format::FileAttribution {
                files: Vec::new(),
                files_omitted: 1,
                identity_complete: true,
            }),
            ..Default::default()
        })
        .unwrap();
        let decoded = decode_shard_scan_header(Some(&contradicted))
            .unwrap()
            .unwrap();
        assert!(!decoded.file_attribution.unwrap().identity_complete);
    }

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Mutex;

    /// A `SessionContext` with a tiny in-memory `events` table, so we can build
    /// the *real* DataFusion logical plans the classifier must handle.
    async fn events_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("raw", DataType::Utf8, false),
            Field::new("bytes", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(StringArray::from(vec!["x status=500", "y", "z status=500"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        ctx
    }

    async fn classify_sql(ctx: &SessionContext, sql: &str) -> DistPlan {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        classify(&plan)
    }

    /// An aggregate whose INPUT is not a pure scan must not be distributed.
    ///
    /// THE DEFECT THIS GUARDS. `classify_aggregate` reasoned only about the root
    /// aggregate -- `group_expr`, `aggr_expr`, `distinct`, `MergeFn::from_func`
    /// -- and never looked at `agg.input`. Its two sibling classifiers,
    /// `ordered_scan` and `scan_limit`, both call `is_pure_scan`; this one did
    /// not, so any aggregate over a SUBQUERY was distributed and the partials
    /// summed as if the subquery had been evaluated globally.
    ///
    /// The shapes below all returned 200 with no marker of approximation:
    ///   - `count(*) FROM (SELECT DISTINCT host ...)` sums per-shard distinct
    ///     counts, over-counting every host present in more than one shard.
    ///     This one is the trap: `count(DISTINCT host)` is correctly refused as
    ///     non-mergeable, so it is the rewrite a user reaches for next.
    ///   - `count(*) FROM (SELECT * ... LIMIT 10)` returns 10 x the peer count.
    ///   - `max(c) FROM (... GROUP BY host)` takes a max over per-shard partials
    ///     rather than global totals.
    ///
    /// Reachable on the packaged default, which is a 2-replica query
    /// StatefulSet with distribution on.
    #[tokio::test]
    async fn classify_refuses_to_distribute_an_aggregate_over_a_subquery() {
        let ctx = events_ctx().await;
        for sql in [
            "SELECT count(*) AS n FROM (SELECT DISTINCT host FROM events)",
            "SELECT count(*) AS n FROM (SELECT * FROM events LIMIT 10)",
            "SELECT max(c) AS m FROM (SELECT host, count(*) AS c FROM events GROUP BY host)",
            "SELECT count(*) AS n FROM (SELECT host FROM events GROUP BY host)",
        ] {
            match classify_sql(&ctx, sql).await {
                DistPlan::Local => {}
                other => panic!("{sql} -> {other:?}, expected Local (not shard-decomposable)"),
            }
        }
    }

    /// ...while the ordinary aggregates it exists to distribute still do.
    #[tokio::test]
    async fn classify_still_distributes_an_aggregate_over_a_plain_scan() {
        let ctx = events_ctx().await;
        for sql in [
            "SELECT count(*) AS n FROM events",
            "SELECT count(*) AS n FROM events WHERE host = 'a'",
            "SELECT host, count(*) AS c FROM events GROUP BY host",
        ] {
            match classify_sql(&ctx, sql).await {
                DistPlan::Aggregate { .. } => {}
                other => panic!("{sql} -> {other:?}, expected Aggregate"),
            }
        }
    }

    /// Regression for the bug AWS smoke round 62 exposed: DataFusion wraps every
    /// aggregate in a projection that aliases the internal aggregate column
    /// (`count(Int64(1)) AS count(*)`), so the old "bare Column projection only"
    /// matcher classified *every* aggregate as `Local` — distribution silently
    /// never happened (the whole-table fallback just returned correct numbers).
    #[tokio::test]
    async fn classify_distributes_aggregates_under_datafusions_aliasing_projection() {
        let ctx = events_ctx().await;

        match classify_sql(&ctx, "SELECT count(*) FROM events").await {
            DistPlan::Aggregate { key_names, agg_fns } => {
                assert!(key_names.is_empty());
                assert_eq!(agg_fns.get("count(*)"), Some(&MergeFn::Sum));
            }
            other => panic!("count(*) -> {other:?}, expected Aggregate"),
        }
        match classify_sql(&ctx, "SELECT count(*) AS n FROM events").await {
            DistPlan::Aggregate { agg_fns, .. } => {
                assert_eq!(agg_fns.get("n"), Some(&MergeFn::Sum));
            }
            other => panic!("count(*) AS n -> {other:?}"),
        }
        match classify_sql(&ctx, "SELECT host, count(*) AS c FROM events GROUP BY host").await {
            DistPlan::Aggregate { key_names, agg_fns } => {
                assert!(key_names.contains("host"));
                assert_eq!(agg_fns.get("c"), Some(&MergeFn::Sum));
            }
            other => panic!("group-by -> {other:?}"),
        }
        match classify_sql(
            &ctx,
            "SELECT min(bytes) AS lo, max(bytes) AS hi FROM events",
        )
        .await
        {
            DistPlan::Aggregate { agg_fns, .. } => {
                assert_eq!(agg_fns.get("lo"), Some(&MergeFn::Min));
                assert_eq!(agg_fns.get("hi"), Some(&MergeFn::Max));
            }
            other => panic!("min/max -> {other:?}"),
        }
    }

    #[tokio::test]
    async fn classify_falls_back_for_non_mergeable_shapes() {
        let ctx = events_ctx().await;
        // avg has no single-column merge function → Local.
        assert!(matches!(
            classify_sql(&ctx, "SELECT avg(bytes) FROM events").await,
            DistPlan::Local
        ));
        // DISTINCT aggregate can't be merged from per-shard partials → Local.
        assert!(matches!(
            classify_sql(&ctx, "SELECT count(DISTINCT host) FROM events").await,
            DistPlan::Local
        ));
        // A computed projection over an aggregate is not pass-through → Local.
        assert!(matches!(
            classify_sql(&ctx, "SELECT count(*) * 2 FROM events").await,
            DistPlan::Local
        ));
        // A pure scan is a Scan, not an aggregate.
        assert!(matches!(
            classify_sql(
                &ctx,
                "SELECT host FROM events WHERE raw LIKE '%status=500%'"
            )
            .await,
            DistPlan::Scan { .. }
        ));
    }

    #[tokio::test]
    async fn classify_orders_scans_when_sort_key_is_in_output() {
        let ctx = events_ctx().await;
        // ORDER BY ... LIMIT over a scan, sort key in output (SELECT *).
        match classify_sql(&ctx, "SELECT * FROM events ORDER BY bytes DESC LIMIT 5").await {
            DistPlan::OrderedScan { order_by, limit } => {
                assert!(order_by.contains("\"bytes\" DESC"), "got {order_by}");
                assert_eq!(limit, Some(5));
            }
            other => panic!("ordered scan -> {other:?}"),
        }
        // No LIMIT → full ordered scan.
        match classify_sql(&ctx, "SELECT host, bytes FROM events ORDER BY bytes ASC").await {
            DistPlan::OrderedScan { order_by, limit } => {
                assert!(order_by.contains("\"bytes\" ASC"));
                assert_eq!(limit, None);
            }
            other => panic!("ordered scan no-limit -> {other:?}"),
        }
        // Sort key NOT projected into the output → partials lack it → Local.
        assert!(matches!(
            classify_sql(&ctx, "SELECT host FROM events ORDER BY bytes DESC").await,
            DistPlan::Local
        ));
        // ORDER BY over a mergeable aggregate → OrderedAggregate (#82): the
        // worker SQL is the sort/limit-stripped aggregate, the merge orders
        // global totals.
        match classify_sql(
            &ctx,
            "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 10",
        )
        .await
        {
            DistPlan::OrderedAggregate {
                key_names,
                agg_fns,
                order_by,
                limit,
                worker_sql,
            } => {
                assert!(key_names.contains("host"));
                assert_eq!(agg_fns.get("n"), Some(&MergeFn::Sum));
                assert!(order_by.contains("\"n\" DESC"), "got {order_by}");
                assert_eq!(limit, Some(10));
                let lowered = worker_sql.to_lowercase();
                assert!(
                    !lowered.contains("order by") && !lowered.contains("limit"),
                    "worker SQL must strip sort/limit: {worker_sql}"
                );
            }
            other => panic!("ordered aggregate -> {other:?}"),
        }
        // Ordered but UNMERGEABLE aggregate (avg) → still Local.
        assert!(matches!(
            classify_sql(
                &ctx,
                "SELECT host, avg(bytes) AS a FROM events GROUP BY host ORDER BY a"
            )
            .await,
            DistPlan::Local
        ));
        // Sort key not in the aggregate output → Local.
        assert!(matches!(
            classify_sql(
                &ctx,
                "SELECT count(*) AS n FROM events GROUP BY host ORDER BY host"
            )
            .await,
            DistPlan::Local
        ));
    }

    /// #85: the fan-out must be CONCURRENT — a multi-shard query's wall is the
    /// slowest worker, not the sum (the sequential loop this guards against
    /// tripled 3-shard latencies). Four 100ms workers must finish in well
    /// under the 400ms a sequential fan-out would take, and the per-shard
    /// walls must be captured for attribution.
    #[tokio::test]
    async fn fan_out_runs_shards_concurrently_and_captures_walls() {
        struct SleepyRunner {
            schema: SchemaRef,
        }
        #[async_trait]
        impl ShardRunner for SleepyRunner {
            async fn run(&self, _sql: &str, _shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                Ok(vec![RecordBatch::try_new(
                    self.schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![1i64]))],
                )?])
            }
        }
        let ctx = events_ctx().await;
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "count(*)",
            DataType::Int64,
            true,
        )]));
        let runner = SleepyRunner { schema };
        let started = std::time::Instant::now();
        let (batches, stats) = coordinate_with_extra_stats(
            &ctx,
            &runner,
            "SELECT count(*) FROM events",
            4,
            Vec::new(),
        )
        .await
        .unwrap();
        let wall = started.elapsed();
        assert!(
            wall < std::time::Duration::from_millis(300),
            "4x100ms shards must fan out concurrently, took {wall:?}"
        );
        assert_eq!(stats.mode, "aggregate");
        assert_eq!(stats.shard_wall_micros.len(), 4, "one wall per shard");
        assert!(stats.shard_wall_micros.iter().all(|&w| w >= 100_000));
        let total: i64 = batches
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .sum();
        assert_eq!(total, 4, "merged sum of per-shard partials");
    }

    #[tokio::test]
    async fn distributed_file_attribution_deduplicates_and_ors_shard_outcomes() {
        struct AttributingRunner {
            schema: SchemaRef,
        }

        #[async_trait]
        impl ShardRunner for AttributingRunner {
            async fn run(&self, _sql: &str, _shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
                unreachable!("coordinate uses run_detailed")
            }

            async fn run_detailed(&self, _sql: &str, shard: Option<ScanShard>) -> Result<ShardRun> {
                let index = shard.unwrap().index;
                let file = crate::format::FileAttributionEntry {
                    table: "ns.events".to_string(),
                    object_key: "data/shared.parquet".to_string(),
                    start: 0,
                    length: 100,
                    cache_candidate: index == 1,
                    reader_opened: index == 0,
                    cache_hit: index == 1,
                };
                Ok(ShardRun {
                    batches: vec![RecordBatch::try_new(
                        self.schema.clone(),
                        vec![Arc::new(Int64Array::from(vec![1_i64]))],
                    )?],
                    scan: Some(crate::format::ScanDetail {
                        file_attribution: Some(crate::format::FileAttribution {
                            files: vec![file],
                            files_omitted: 0,
                            identity_complete: true,
                        }),
                        ..Default::default()
                    }),
                })
            }
        }

        let ctx = events_ctx().await;
        let runner = AttributingRunner {
            schema: Arc::new(Schema::new(vec![Field::new(
                "count(*)",
                DataType::Int64,
                false,
            )])),
        };
        let (_, stats) = coordinate_with_extra_stats(
            &ctx,
            &runner,
            "SELECT count(*) FROM events",
            2,
            Vec::new(),
        )
        .await
        .unwrap();

        let attribution = stats.scan.unwrap().file_attribution.unwrap();
        assert_eq!(attribution.files.len(), 1);
        let file = &attribution.files[0];
        assert!(file.cache_candidate && file.reader_opened && file.cache_hit);
        assert_eq!(attribution.files_omitted, 0);
        assert!(attribution.identity_complete);
    }

    /// Records every shard it is asked to run, and returns a fixed partial
    /// count of 1 per shard so the merge is checkable.
    struct CountingRunner {
        calls: Mutex<Vec<Option<ScanShard>>>,
        schema: SchemaRef,
    }

    #[async_trait]
    impl ShardRunner for CountingRunner {
        async fn run(&self, _sql: &str, shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
            self.calls.lock().unwrap().push(shard);
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1i64]))],
            )?;
            Ok(vec![batch])
        }
    }

    /// `coordinate` must actually *fan out* an aggregate to all N shards and sum
    /// the partials — not silently take the single whole-table fallback.
    #[tokio::test]
    async fn coordinate_fans_out_aggregate_to_all_shards() {
        let ctx = events_ctx().await;
        let runner = CountingRunner {
            calls: Mutex::new(Vec::new()),
            schema: Arc::new(Schema::new(vec![Field::new(
                "count(*)",
                DataType::Int64,
                false,
            )])),
        };
        let merged = coordinate(&ctx, &runner, "SELECT count(*) FROM events", 3)
            .await
            .unwrap();

        // Fanned out to 3 distinct shards (not 1× the None fallback).
        let calls = runner.calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            3,
            "expected 3 shard calls, got {}",
            calls.len()
        );
        assert!(
            calls.iter().all(|s| s.is_some()),
            "fallback path taken: {calls:?}"
        );
        assert_eq!(
            calls
                .iter()
                .filter_map(|s| s.map(|s| s.index))
                .collect::<HashSet<_>>()
                .len(),
            3,
            "shards must be distinct"
        );
        // Merge summed the three partials (1 each) → 3.
        let col = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);
    }

    /// #1525: a worker that refused because it could not resolve the pinned
    /// snapshot must NOT have its fragment re-run on the coordinator.
    ///
    /// The failover runner is the coordinator's own `/shard` endpoint. Running
    /// the fragment there produces it from whatever snapshot THAT pod resolves,
    /// which is the mixed-snapshot fan-out the pin exists to prevent — only now
    /// with a 200 on it. The query must fail instead, and the fallback must be
    /// untouched.
    #[tokio::test]
    async fn a_pin_refusal_is_not_re_run_on_the_coordinator() {
        struct RefusingRunner;
        #[async_trait]
        impl ShardRunner for RefusingRunner {
            async fn run(&self, _sql: &str, _shard: Option<ScanShard>) -> Result<Vec<RecordBatch>> {
                Err(ShardError {
                    status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    url: "http://peer/api/v1/sql/shard".to_string(),
                    body: format!(
                        r#"{{"code":503,"reason":"{}"}}"#,
                        crate::error::SHARD_PIN_UNRESOLVED_REASON
                    ),
                }
                .into())
            }
        }

        let ctx = events_ctx().await;
        let fallback = CountingRunner {
            calls: Mutex::new(Vec::new()),
            schema: Arc::new(Schema::new(vec![Field::new(
                "count(*)",
                DataType::Int64,
                false,
            )])),
        };
        let err = coordinate_with_failover(
            &ctx,
            &RefusingRunner,
            Some(&fallback),
            "SELECT count(*) FROM events",
            2,
            Vec::new(),
        )
        .await
        .expect_err("a pin refusal must fail the query, not be papered over");
        assert!(
            fallback.calls.lock().unwrap().is_empty(),
            "the refused fragment was re-run on the coordinator: {:?}",
            fallback.calls.lock().unwrap()
        );
        assert!(
            crate::error::ApiError::from_query(err, crate::limits::Priority::Interactive).status
                == axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "the worker's 503 must reach the caller as a 503"
        );
    }

    /// WS-6: an extra partial (the WAL buffer's count) is folded into the
    /// aggregate merge alongside the per-shard worker partials.
    #[tokio::test]
    async fn coordinate_with_extra_folds_buffer_partial_into_aggregate() {
        let ctx = events_ctx().await;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count(*)",
            DataType::Int64,
            false,
        )]));
        let runner = CountingRunner {
            calls: Mutex::new(Vec::new()),
            schema: schema.clone(),
        };
        // Buffer partial: 5 un-committed rows ⇒ a partial count of 5.
        let buffer_partial =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![5i64]))])
                .unwrap();
        let merged = coordinate_with_extra(
            &ctx,
            &runner,
            "SELECT count(*) FROM events",
            3,
            vec![buffer_partial],
        )
        .await
        .unwrap();
        // 3 shards × 1 (Iceberg) + 5 (buffer) = 8.
        let col = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 8, "3 Iceberg-shard rows + 5 buffer rows");
    }

    /// Regression for the `coordinated_aggregates_match_unsharded` flake: per-
    /// shard partials for the SAME query can disagree on field nullability — a
    /// shard whose min/max was answered from exact file statistics emits
    /// non-nullable literal fields, a really-aggregating shard emits nullable
    /// ones. The merge must accept the mix, not die on
    /// "Mismatch between schema and batches".
    #[tokio::test]
    async fn merge_aggregate_accepts_mixed_nullability_partials() {
        use arrow::array::TimestampNanosecondArray;
        let mk = |nullable: bool, lo: i64, hi: i64| {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "min(events.timestamp)",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    nullable,
                ),
                Field::new(
                    "max(events.timestamp)",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    nullable,
                ),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![lo])),
                    Arc::new(TimestampNanosecondArray::from(vec![hi])),
                ],
            )
            .unwrap()
        };
        // Stats-path shard (non-nullable) + real-aggregate shard (nullable).
        let partials = vec![mk(false, 100, 900), mk(true, 50, 700)];
        let agg_fns = HashMap::from([
            ("min(events.timestamp)".to_string(), MergeFn::Min),
            ("max(events.timestamp)".to_string(), MergeFn::Max),
        ]);
        let merged = merge_aggregate(partials, &HashSet::new(), &agg_fns, None)
            .await
            .unwrap();
        let lo = merged[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        let hi = merged[0]
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        assert_eq!((lo, hi), (50, 900), "min of mins / max of maxes");
    }

    /// Same hazard on the ordered-scan merge path (it builds the partials
    /// MemTable the same way).
    #[tokio::test]
    async fn merge_ordered_accepts_mixed_nullability_partials() {
        let mk = |nullable: bool, vals: Vec<i64>| {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Int64,
                nullable,
            )]));
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
        };
        let partials = vec![mk(false, vec![3, 1]), mk(true, vec![2])];
        let merged = merge_ordered(partials, "\"ts\" ASC NULLS LAST", Some(2))
            .await
            .unwrap();
        let rows: Vec<i64> = merged
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            rows,
            vec![1, 2],
            "merge-sorted top-2 across mixed-nullability partials"
        );
    }
}
