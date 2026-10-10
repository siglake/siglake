//! `POST /api/v1/sql` — run a DataFusion SQL query against the five
//! Iceberg-backed siglake tables: the built-in `events` and `query_audit`,
//! plus every managed user index.
//!
//! Also exports `POST /api/v1/sql/explain` for cost-only requests.
//!
//! Client SQL is READ-ONLY on every entry point here — see [`read_only_sql`]
//! for what that refuses and why the default was not safe.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow_schema::DataType;
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, Limit, LogicalPlan, Operator, Projection, Sort,
};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SQLOptions, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    visit_expressions, visit_expressions_mut, BinaryOperator as SqlBinaryOperator, Cte,
    Expr as SqlAstExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    LimitClause, OrderBy, OrderByExpr, OrderByKind, OrderByOptions, Query as SqlQuery, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, Visit,
    VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use serde::Deserialize;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Mutex, Notify};

use crate::audit::{AuditRow, AuditStatus, AuditWriter};
use crate::auth::CallerIdentity;
use crate::cost::{estimate, extract_exact_time_bounds_from_expr, CostReport};
use crate::error::ApiError;
#[allow(unused_imports)] // referenced by the `#[utoipa::path]` response bodies
use crate::format::RecordsResponse;
use crate::format::{batches_to_records, NdjsonStream, QueryFormat};
use crate::jobs::{CompletionOutcome, JobStatus};
use crate::limits::{Priority, RequestLimits, ResolvedLimits};
#[allow(unused_imports)]
use crate::openapi_dto::{ApiErrorBody, BatchSubmitResponse, SqlSuccess};
use crate::AppState;

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct SqlRequest {
    pub query: String,
    #[serde(default)]
    pub format: Option<QueryFormat>,
    #[serde(default)]
    pub priority: Priority,
    /// Order a bare interactive `SELECT` newest-first (default true).
    ///
    /// A query that names ONE timestamp-bearing table and asks for no ordering
    /// of its own is given `ORDER BY timestamp DESC` — what a log reader means
    /// by `SELECT timestamp, raw FROM t LIMIT 100`, and what puts the query on
    /// the ordered early-stop path instead of a file-order scan. Eligible
    /// tables are `events`, `query_audit`, and any managed index whose doc
    /// mapping declares `timestamp` as its `timestamp_field`; an index that
    /// names another event-time field is left as written, because a column
    /// called `timestamp` there need not be a time order.
    ///
    /// An explicit `ORDER BY`, a `GROUP BY`, an aggregate, a CTE, a join,
    /// `DISTINCT`, `EXPLAIN` and the batch tier are never rewritten. Neither
    /// is a projection that gives another column the output name `timestamp`
    /// (`SELECT raw AS timestamp FROM t LIMIT 2`): SQL resolves the injected
    /// bare identifier to that output name, and the source column cannot be
    /// named around the alias. Identifier case follows SQL's rules here: a
    /// mapping may declare both `timestamp` and `Timestamp`, and the quoted
    /// `"Timestamp"` is the second of them, so `SELECT "Timestamp" AS
    /// timestamp FROM t LIMIT 2` is a shadowing projection too. An explicit
    /// `LIMIT` is preserved; without one the rewrite adds
    /// `max_rows_returned + 1`, so the truncation signal still fires.
    ///
    /// Set false to get the query exactly as written.
    #[serde(default = "default_true")]
    pub default_order: bool,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub limits: RequestLimits,
    /// Distributed-query shard (#7): when set, this worker scans only its
    /// `index`-of-`count` slice of the table's files. A coordinator fans the
    /// query out to `count` workers (shards `0..count`) and merges the partial
    /// results. Absent ⇒ a normal whole-table scan.
    #[serde(default)]
    pub shard: Option<ShardParam>,
    /// Refuse an approximate answer and take the exact path however slow it is.
    ///
    /// Default false, i.e. approximation is ALLOWED. That is the deliberate
    /// choice: a high-cardinality `GROUP BY` that silently costs seconds is a
    /// worse surprise than a labelled approximation, and the response always
    /// says which it gave you. Set this when a caller would rather wait —
    /// billing, reconciliation, anything where the number is the deliverable.
    #[serde(default)]
    pub exact: bool,
}

fn default_true() -> bool {
    true
}

/// Whether approximate group counts may be served at all
/// (`SIGLAKE_APPROXIMATE_GROUP_COUNTS`, default on).
///
/// The per-request `exact` flag is the user's control; this is the operator's,
/// for a deployment that wants exactness enforced regardless of what callers
/// ask for.
/// Whether a group-count answer may be an approximation.
///
/// A NEWTYPE rather than a `bool`, because three call sites took a bool and one
/// of them passed a literal `true`: the coordinator's Tier-1 battery, which with
/// `query.distributed.enabled` on by default is the path every user query takes.
/// So `SqlRequest::exact` -- "the user's control ... for billing, reconciliation,
/// anything where the number is the deliverable" -- and
/// `SIGLAKE_APPROXIMATE_GROUP_COUNTS`, the operator's kill switch, were both
/// inert exactly where they mattered, and an operator who set the switch had no
/// way to learn it did nothing.
///
/// There is deliberately no `From<bool>`: the only ways to make one are from a
/// request or from an explicit, named decision, so a future call site cannot
/// quietly hardcode the permissive answer the way this one did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllowApproximate(bool);

impl AllowApproximate {
    /// The rule: the caller did not demand exactness AND the operator has not
    /// disabled approximation.
    fn for_request(req: &SqlRequest) -> Self {
        Self(!req.exact && approximate_group_counts_enabled())
    }

    #[cfg(test)]
    fn always() -> Self {
        Self(true)
    }

    fn get(self) -> bool {
        self.0
    }
}

fn approximate_group_counts_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("SIGLAKE_APPROXIMATE_GROUP_COUNTS")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "0" | "false" | "no"
        )
    })
}

/// A `{index, count}` scan-shard selector on a query request.
#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, utoipa::ToSchema)]
pub struct ShardParam {
    pub index: usize,
    pub count: usize,
}

impl ShardParam {
    pub fn resolve(self) -> Option<siglake_storage::ScanShard> {
        siglake_storage::ScanShard::new(self.index, self.count)
    }
}

const SQL_RESULT_CACHE_MAX_ENTRIES: usize = 256;
const SQL_RESULT_CACHE_MAX_BYTES: usize = 4 * 1024 * 1024;
const SQL_RESULT_CACHE_MAX_ROWS: usize = 128;

/// What ONE Jaeger name-list entry may retain: an eighth of the shared byte
/// budget (#2302).
///
/// Chosen against the measurement, not rounded to taste: the sweep is
/// `what_a_name_list_over_the_entry_cap_costs` in
/// `tests/jaeger_render_cost_measurement.rs`, and README, "Caches" carries the
/// figures. An 800-name list is 8.6–9.4 KiB as an arena — 1.8% of this — and
/// 512 KiB admits the whole 10,237-name render ceiling whenever names average
/// under 47 characters, which is where `JaegerCeilings::names` already stops
/// the surface. Above that the pre-#2302 bypass stays: the list is executed
/// and returned WHOLE, never truncated to fit.
///
/// It is an eighth rather than the whole budget because the store is shared
/// with `/api/v1/sql`, and because an arena's accounted bytes ARE its retained
/// bytes (two allocations of known length), the eighth is honest rather than
/// the 29–35x understatement a rendered entry used to carry (#2304).
///
/// No knob, like the three constants above it. The row bound this replaces for
/// the name path is already a function of
/// `SIGLAKE_QUERY_ADMISSION_BUDGET_BYTES` through `JaegerCeilings::names`.
const NAME_LIST_CACHE_MAX_BYTES: usize = 512 * 1024;

/// One wall-clock budget carried across handler handoffs.
///
/// The transparent endpoint resolves and rewrites before deciding whether a
/// query runs locally or fans out. Passing the original clock downstream keeps
/// that preparation from being followed by a fresh full execution budget.
#[derive(Clone, Copy)]
struct RequestClock {
    started: Instant,
    deadline: tokio::time::Instant,
}

impl RequestClock {
    fn new(timeout: std::time::Duration) -> Self {
        let started = Instant::now();
        Self {
            deadline: tokio::time::Instant::from_std(started + timeout),
            started,
        }
    }
}

/// One stored answer: what it costs the budget, and the body itself behind an
/// `Arc`.
///
/// THE DEFECT THIS CLOSES (#2304), in both fields.
///
///   - `bytes` charged `serde_json::to_vec(body).len()` — the length of an
///     encoding `finish_result_cache` then DROPPED — while the entry retained
///     the whole `RecordsResponse`. Measured 29–35x apart on a one-column list
///     (84.2 KiB retained against 2.9 KiB charged at 128 rows, 530.5 KiB
///     against 18.0 KiB at 800), so [`SQL_RESULT_CACHE_MAX_BYTES`] bounded
///     4 MiB of accounted bytes over 21–132 MiB of heap, none of it inside the
///     query memory pool. It is now [`crate::format::retained_heap_bytes`],
///     which is what the gauge and the cap were always read as meaning.
///   - `body` was cloned on every hit — `entries.get(key).cloned()`, a deep
///     copy of a `Value` tree, 17–25 µs at 128 rows and 115–139 µs at 800,
///     inside the process-wide cache mutex every SQL and Jaeger probe on the
///     pod queues behind. Behind an `Arc` a hit clones a pointer: A/B'd on the
///     same bodies in release, 12.45 µs → 0.009 µs at 128 rows and 253.6 µs →
///     0.011 µs at 800.
///
/// Evidence and the sizing method are documented in README, "Caches".
#[derive(Clone)]
struct CachedSqlResult {
    /// The BODY's retained heap. The key this entry is stored under is charged
    /// separately, by the store, because whether it is a new allocation depends
    /// on what the store already holds — see [`SqlResultCache::insert`] and
    /// [`key_heap_bytes`].
    bytes: usize,
    body: CachedBody,
}

/// What one entry holds. Two shapes, one store (#2302).
///
/// The name-list shape exists because the rendered one prices a one-column list
/// of short strings at ~670 bytes per name: a row is a `serde_json::Map`, i.e.
/// a `BTreeMap` whose node is allocated whole. Measured, an 800-name list
/// retains 530.5 KiB as [`Self::Records`] and 9.4 KiB as [`Self::Names`], so
/// under a 512 KiB caller allowance ([`NAME_LIST_CACHE_MAX_BYTES`]) the arena
/// is what makes the entry admissible at all.
///
/// Both variants clone as refcount bumps, which is what a hit does inside the
/// process-wide mutex (#2304).
#[derive(Clone)]
pub(crate) enum CachedBody {
    /// A rendered SQL result, exactly as before.
    Records(Arc<crate::format::RecordsResponse>),
    /// A one-column name list: every name concatenated into one buffer, plus
    /// the END offset of each. Two allocations however long the list, and
    /// `text.len() + 4 * offsets.len()` is its whole heap — so what the byte
    /// budget charges a name entry is what the pod pays for it.
    Names { text: Arc<str>, offsets: Arc<[u32]> },
}

impl CachedBody {
    /// The names as an arena, or `None` when the list cannot be one.
    ///
    /// `None` only on a total text length past `u32::MAX`, which the render
    /// ceilings make unreachable — it is refused entry rather than truncated or
    /// widened to `u64`, because 4 GiB of names is not an entry any budget here
    /// would admit anyway.
    pub(crate) fn names(names: &[String]) -> Option<Self> {
        // Both buffers pre-sized: growing geometrically would leave up to 2x of
        // capacity slack, and the charge below bills capacity, so the store
        // would pay for the allocator's doubling.
        let mut text = String::with_capacity(names.iter().map(String::len).sum());
        let mut offsets: Vec<u32> = Vec::with_capacity(names.len());
        for name in names {
            text.push_str(name);
            offsets.push(u32::try_from(text.len()).ok()?);
        }
        Some(Self::Names {
            text: Arc::from(text.as_str()),
            offsets: Arc::from(offsets.as_slice()),
        })
    }

    /// Rows this body holds, for a policy that bounds them.
    fn row_count(&self) -> usize {
        match self {
            Self::Records(body) => body.row_count,
            Self::Names { offsets, .. } => offsets.len(),
        }
    }

    /// Heap this body retains, which is what the budget charges it.
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Records(body) => crate::format::retained_heap_bytes(body),
            Self::Names { text, offsets } => {
                text.len() + offsets.len() * std::mem::size_of::<u32>()
            }
        }
    }

    /// A SQL hit's body.
    ///
    /// The two key spaces are disjoint by construction and this ASSERTS that
    /// rather than falling back: a `/api/v1/sql` key is
    /// `{namespace}|snapshot={id}|…` and a name key's second field is the
    /// literal `v1` (`jaeger_routes::name_cache_key`), which no snapshot id
    /// spells. A SQL lookup reaching a name entry is a key-space bug, not a
    /// case to serve something else from.
    pub(crate) fn expect_records(self) -> Arc<crate::format::RecordsResponse> {
        match self {
            Self::Records(body) => body,
            Self::Names { .. } => unreachable!(
                "a /api/v1/sql cache key resolved to a Jaeger name-list entry: \
                 the two key spaces are disjoint (a name key's second field is \
                 `v1`), so this is a key-space bug"
            ),
        }
    }

    /// A name-list hit's names, rebuilt from the arena.
    ///
    /// Runs OUTSIDE the cache mutex — the probe hands out two `Arc`s under the
    /// lock and this splits them afterwards. Asserts the key spaces the same
    /// way [`Self::expect_records`] does, in the other direction.
    pub(crate) fn expect_names(&self) -> Vec<String> {
        match self {
            Self::Names { text, offsets } => {
                let mut names = Vec::with_capacity(offsets.len());
                let mut start = 0usize;
                for end in offsets.iter() {
                    let end = *end as usize;
                    names.push(text[start..end].to_string());
                    start = end;
                }
                names
            }
            Self::Records(_) => unreachable!(
                "a Jaeger name-list cache key resolved to a /api/v1/sql entry: \
                 the two key spaces are disjoint (a name key's second field is \
                 `v1`), so this is a key-space bug"
            ),
        }
    }
}

/// What a caller will let into the shared store (#2302).
///
/// THE DEFECT THIS CLOSES. `finish_result_cache` read
/// [`SQL_RESULT_CACHE_MAX_ROWS`] and [`SQL_RESULT_CACHE_MAX_BYTES`] itself, so
/// every caller was bounded by `/api/v1/sql`'s 128 rows — and a name list is a
/// shape a row count prices wrong by two orders of magnitude. Measured, the
/// 800-name list that gate refused is 0.4% of the byte budget it was refused
/// by, while every poll of it re-ran a 17–41 ms unpredicated aggregate on a
/// snapshot that never moved. Eligibility is now the caller's; the store, the
/// LRU, the key, the counter and single-flight are untouched, and the SHARED
/// budgets ([`SQL_RESULT_CACHE_MAX_BYTES`], [`SQL_RESULT_CACHE_MAX_ENTRIES`])
/// still bound the store as a whole.
#[derive(Clone, Copy)]
pub(crate) struct CacheEligibility {
    /// Rows this caller will store, or `None` where the caller's OWN upstream
    /// bound is what limits them. `None` is not "unbounded": see
    /// [`Self::NAME_LIST`].
    max_rows: Option<usize>,
    /// Heap ONE of this caller's entries may retain.
    max_bytes: usize,
}

impl CacheEligibility {
    /// `/api/v1/sql`, byte-identical to what the gate read before #2302.
    pub(crate) const SQL: Self = Self {
        max_rows: Some(SQL_RESULT_CACHE_MAX_ROWS),
        max_bytes: SQL_RESULT_CACHE_MAX_BYTES,
    };

    /// The two Jaeger name lists.
    ///
    /// `max_rows: None` is safe HERE and would not be for SQL: the list is
    /// already row-bounded upstream by `JaegerCeilings::names` (10,237 on a
    /// packaged pod), enforced mid-flight at a batch boundary and present in
    /// the cache key, so a list over it is refused 413 and never reaches the
    /// insert. The name path has a row bound; it is not 128, and the operator's
    /// memory configuration is what moves it.
    pub(crate) const NAME_LIST: Self = Self {
        max_rows: None,
        max_bytes: NAME_LIST_CACHE_MAX_BYTES,
    };

    /// Whether this body may be stored under this key, and what the store will
    /// bill for the BODY.
    ///
    /// The gate prices the whole entry — body plus key — while the number it
    /// returns is the body's alone, because the key allocation is the store's
    /// to bill: only [`SqlResultCache`] knows whether that key is already
    /// retained (#2493).
    ///
    /// The key is priced at all because it is the one part of an entry the
    /// caller sizes: it carries the normalized SQL, so a 64 KiB query with a
    /// 100-byte answer retains 64 KiB. A body-only gate let such an entry in
    /// as if it were 100 bytes, which is how the store came to hold key
    /// material outside the budget it publishes.
    fn admits(&self, key: &str, body: &CachedBody) -> Option<usize> {
        if self.max_rows.is_some_and(|rows| body.row_count() > rows) {
            return None;
        }
        let body_bytes = body.retained_bytes();
        let retained = body_bytes.saturating_add(key_heap_bytes(key));
        (retained <= self.max_bytes).then_some(body_bytes)
    }
}

/// Heap ONE retained key costs: its text, plus the strong and weak counts the
/// `Arc<str>` carries in front of it.
///
/// Charged once per key however many places hold it. The map entry and every
/// recency marker are clones of one `Arc`, so the text exists once (#2493); it
/// used to exist once per holder, uncharged.
fn key_heap_bytes(key: &str) -> usize {
    key.len() + 2 * std::mem::size_of::<usize>()
}

/// What one recency marker costs the store: a fat pointer in the queue's ring
/// buffer. The key text behind it is charged with the entry that owns it.
const RECENCY_MARKER_BYTES: usize = std::mem::size_of::<Arc<str>>();

#[derive(Default)]
struct SqlResultCache {
    /// Heap the stored entries RETAIN, summed: each body and each key. The
    /// recency queue is added on top by [`Self::retained_bytes`], which is the
    /// quantity [`SQL_RESULT_CACHE_MAX_BYTES`] bounds and
    /// `siglake_query_sql_result_cache_bytes` publishes. See
    /// [`CachedSqlResult`] for what it used to be.
    entry_bytes: usize,
    /// Recency markers, newest last. `Arc<str>` rather than `String` so a
    /// marker is a refcount bump on the key the map already holds: the queue
    /// used to own a second copy of every key and one more per hit (#2493).
    order: VecDeque<Arc<str>>,
    entries: HashMap<Arc<str>, CachedSqlResult>,
}

/// Single-flight markers, in a SYNCHRONOUS mutex of their own.
///
/// They used to live in `SqlResultCache` behind a `tokio::sync::Mutex`, which
/// meant cleanup could only happen in an `async fn` — so it was done by hand at
/// eleven call sites, and any path that returned without one leaked the marker
/// permanently. Out here, `Drop` can remove it.
/// Cap on how long a single-flight waiter will park before giving up and
/// running the query itself. NOT the whole bound: a request whose own budget is
/// shorter than this waits only what it has left (`cache_wait_budget`). The cap
/// was written assuming it was "shorter than any request timeout", which held
/// for the tier defaults and not for a caller-set `timeout_seconds`.
const SQL_RESULT_CACHE_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// What a single-flight follower may spend parked: the cap, or the remainder of
/// this request's wall-clock budget, whichever is smaller. With breakers off
/// there is no request deadline to respect, so only the cap applies.
fn cache_wait_budget(
    resolved: &ResolvedLimits,
    deadline: tokio::time::Instant,
) -> std::time::Duration {
    if !resolved.circuit_breakers {
        return SQL_RESULT_CACHE_WAIT;
    }
    deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .min(SQL_RESULT_CACHE_WAIT)
}

/// A deadline far enough out that a test exercising cache MECHANICS never trips
/// the wall-clock guard. The deadline's own behaviour is covered separately.
#[cfg(test)]
fn far_future_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3600)
}

fn sql_result_inflight() -> &'static std::sync::Mutex<HashMap<String, Arc<Notify>>> {
    static INFLIGHT: OnceLock<std::sync::Mutex<HashMap<String, Arc<Notify>>>> = OnceLock::new();
    INFLIGHT.get_or_init(Default::default)
}

enum CacheProbe {
    Leader(Arc<Notify>),
    Wait(Pin<Box<OwnedNotified>>),
}

/// Register for this flight's completion before giving up protected membership
/// in the in-flight map. `notify_waiters` retains no permit for a future made
/// afterwards, while an enabled `OwnedNotified` remains ready even when the
/// leader completes before the caller first awaits it.
fn registered_completion(notify: Arc<Notify>) -> Pin<Box<OwnedNotified>> {
    let mut completion = Box::pin(notify.notified_owned());
    let _ = completion.as_mut().enable();
    completion
}

fn sql_result_cache() -> &'static Mutex<SqlResultCache> {
    static CACHE: OnceLock<Mutex<SqlResultCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(SqlResultCache::default()))
}

impl SqlResultCache {
    /// A hit, as an `Arc` clone, promotes the key to most recently used. The
    /// whole call runs inside
    /// `sql_result_cache().lock()`, so what this does is what every other
    /// probe on the pod waits for: a hash lookup, a refcount bump and one key
    /// clone, not a scan of the recency queue or the `RecordsResponse` deep
    /// copy it used to be (#2304).
    fn get(&mut self, key: &str) -> Option<CachedBody> {
        let (stored, entry) = self.entries.get_key_value(key)?;
        let (body, marker) = (entry.body.clone(), Arc::clone(stored));
        self.record_use(marker);
        Some(body)
    }

    /// Heap the store retains: every entry's body and key, plus one slot per
    /// recency marker. What [`SQL_RESULT_CACHE_MAX_BYTES`] bounds and what the
    /// gauge publishes.
    ///
    /// Derived rather than accumulated for the queue's part, so the marker
    /// bookkeeping cannot drift from the queue it describes. The two things it
    /// does NOT charge are the hash table's own spare capacity and the ring
    /// buffer's, both bounded by the entry cap and by the compaction rule
    /// below, and both small beside a key: 256 entries of slack is single-digit
    /// KiB against a 4 MiB budget.
    fn retained_bytes(&self) -> usize {
        self.entry_bytes
            .saturating_add(self.order.len().saturating_mul(RECENCY_MARKER_BYTES))
    }

    /// The charge recomputed from what the store actually holds, for tests that
    /// assert the running total did not drift.
    #[cfg(test)]
    fn audited_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|(key, entry)| entry.bytes + key_heap_bytes(key))
            .sum::<usize>()
            + self.order.len() * RECENCY_MARKER_BYTES
    }

    /// Drop everything the store holds and everything it bills for it. One
    /// method rather than three statements at the reset seam, so a future
    /// charged field is released where it is cleared.
    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.entry_bytes = 0;
    }

    /// A non-promoting presence probe for tests that assert store state.
    #[cfg(test)]
    fn contains(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Append a recency marker in O(1), leaving stale markers for eviction to
    /// skip. Compact only after they outnumber live entries, which bounds the
    /// queue while keeping hit promotion O(1) amortized.
    ///
    /// The marker is a clone of the key the map holds, so a hit adds
    /// [`RECENCY_MARKER_BYTES`] and no text. That is what bounds the queue's
    /// contribution to the budget at twice the live entries — about 8 KiB at
    /// the entry cap — however long the keys are and however often they are
    /// hit.
    fn record_use(&mut self, key: Arc<str>) {
        self.order.push_back(key);
        self.compact_order_if_needed();
    }

    fn compact_order_if_needed(&mut self) {
        if self.order.len() <= self.entries.len().saturating_mul(2) {
            return;
        }

        let mut seen: std::collections::HashSet<Arc<str>> =
            std::collections::HashSet::with_capacity(self.entries.len());
        let mut compacted = VecDeque::with_capacity(self.entries.len());
        while let Some(key) = self.order.pop_back() {
            if self.entries.contains_key(&key) && seen.insert(Arc::clone(&key)) {
                compacted.push_front(key);
            }
        }
        self.order = compacted;
    }

    fn insert(&mut self, key: String, entry: CachedSqlResult) {
        // One allocation per retained key (#2493). A key already in the store
        // is re-used, so a re-insert leaves the map and the queue sharing the
        // text they already share rather than holding a second copy of it.
        let key: Arc<str> = match self.entries.get_key_value(key.as_str()) {
            Some((stored, _)) => Arc::clone(stored),
            None => Arc::from(key),
        };
        // Charged before the move, rather than by cloning the entry to read it
        // back afterwards: that clone was a second deep copy of the body per
        // insert (#2304).
        let charge = entry.bytes;
        match self.entries.insert(Arc::clone(&key), entry) {
            // A replacement re-uses the key allocation already billed; only the
            // body's charge changes hands.
            Some(prev) => self.entry_bytes = self.entry_bytes.saturating_sub(prev.bytes),
            None => self.entry_bytes = self.entry_bytes.saturating_add(key_heap_bytes(&key)),
        }
        self.entry_bytes = self.entry_bytes.saturating_add(charge);
        self.record_use(key);
        while (!self.entries.is_empty())
            && (self.entries.len() > SQL_RESULT_CACHE_MAX_ENTRIES
                || self.retained_bytes() > SQL_RESULT_CACHE_MAX_BYTES)
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            let should_remove = self.entries.contains_key(&oldest)
                && self.order.iter().all(|candidate| *candidate != oldest);
            if should_remove {
                if let Some(evicted) = self.entries.remove(&oldest) {
                    // Body and key together: the key allocation dies with the
                    // last holder, and by here the queue holds no marker for it.
                    self.entry_bytes = self
                        .entry_bytes
                        .saturating_sub(evicted.bytes.saturating_add(key_heap_bytes(&oldest)));
                    metrics::counter!(
                        "siglake_query_sql_result_cache_requests_total",
                        "outcome" => "evict"
                    )
                    .increment(1);
                }
            }
        }
        self.compact_order_if_needed();
        metrics::gauge!("siglake_query_sql_result_cache_bytes").set(self.retained_bytes() as f64);
        metrics::gauge!("siglake_query_sql_result_cache_entries").set(self.entries.len() as f64);
    }
}

/// Empty the process-wide result store. Test seam for measurements whose
/// phases must each start from a STATED store state (#2398).
///
/// The store is a process-wide static, so an integration test that wants to
/// know how many of its own entries survived a phase has no way to tell them
/// from whatever an earlier phase left behind. Nothing here touches the
/// single-flight map (no key is in flight while a measurement is between
/// phases) and nothing counts an `outcome`: a reset is not an eviction, and
/// billing it as one would put the number this seam exists to measure into the
/// counter that measures it. The two gauges are re-published because they are
/// levels, not deltas.
#[doc(hidden)]
pub async fn reset_result_cache_for_test() {
    let mut cache = sql_result_cache().lock().await;
    cache.clear();
    metrics::gauge!("siglake_query_sql_result_cache_bytes").set(0.0);
    metrics::gauge!("siglake_query_sql_result_cache_entries").set(0.0);
}

#[derive(Clone)]
pub(crate) struct SqlResultCacheCtx {
    key: String,
    notify: Arc<Notify>,
    /// The WAL listing this key's eligibility was established over. Re-taken in
    /// `finish_result_cache`, because the execution between the two lists the
    /// directory again — see [`BufferProof`].
    buffer_proof: BufferProof,
}

/// THE DEFECT THIS CLOSES. Becoming the single-flight leader registered a marker
/// that only an explicit `finish_result_cache` call removed, and the pre-flight
/// byte rejection returned without one: the marker stayed forever, and every
/// later identical query became a waiter parked on a `Notify` nobody would ever
/// fire. Permanent for that query shape, cured only by a restart.
///
/// Deterministically reachable: send one query that trips `preflight_bytes`, get
/// a 400, then send it again and it hangs until the request timeout. The
/// 2026-08-25 round measured 6,413 preflight_bytes refusals in under three
/// hours, each one poisoning its own key.
///
/// It matches much of the still-open release blocker's signature — specific
/// browse shapes returning 504 at exactly the timeout while metadata shapes stay
/// fast, accumulating with failed queries, and cured by a restart. It does NOT
/// explain that blocker's 140% idle CPU, so this is one cause, not proof of the
/// whole thing.
///
/// A Drop impl rather than a twelfth careful call site, because the paths that
/// leaked are the ones nobody enumerated: `?` returns, cost rejections, and a
/// client disconnect dropping the whole handler future, where no code runs at
/// all.
impl Drop for SqlResultCacheCtx {
    fn drop(&mut self) {
        if let Ok(mut inflight) = sql_result_inflight().lock() {
            // A handler may retain one clone after another clone finishes the
            // flight. A new leader can claim the same key before that older
            // guard is dropped, so removal must be specific to this flight.
            let owns_marker = inflight
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &self.notify));
            if owns_marker {
                inflight.remove(&self.key);
            }
        }
        // Wake waiters even if the lock was poisoned: a waiter that is never
        // woken is the failure being fixed.
        self.notify.notify_waiters();
    }
}

pub(crate) enum SqlResultCacheDecision {
    Skip,
    /// A stored body, SHARED with the cache and with every other request being
    /// served the same entry — never a copy of it (#2304). Callers read it or
    /// serialize it by reference; nothing mutates a served body. Which shape it
    /// is follows from the key the caller probed with
    /// ([`CachedBody::expect_records`], [`CachedBody::expect_names`]).
    Hit(CachedBody),
    Leader(SqlResultCacheCtx),
}

/// The serving mode a cached body belongs to.
///
/// THE DEFECT THIS CLOSES. The key was `(namespace, snapshot, limits, SQL)` and
/// nothing else, but two requests with all four identical are not owed the same
/// answer:
///
///   - **Exactness.** `exact: true` is the caller's opt-out from an approximate
///     top-K ("for billing, reconciliation, anything where the number is the
///     deliverable" — see `AllowApproximate`). The guard sits in the group-count
///     fast path, which a cache hit never reaches: one default request warming a
///     sketch answer made every later `exact: true` request replay it, complete
///     with its error bound, until the snapshot moved.
///   - **Shard scope.** `shard` is a public request field on `/api/v1/sql` and
///     `/api/v1/sql/local`, and it selects a SUBSET of the file set. Sharing one
///     entry with the whole-table request meant whichever arrived first defined
///     the answer for all of them — a whole-table count served to a shard, or a
///     shard's partial served as the whole table.
///
/// Keyed rather than bypassed: a sharded or exact request is still a pure
/// function of `(table, snapshot, query)` *within its mode*, so separating the
/// modes keeps every same-mode hit and costs at most one extra entry per mode
/// actually used. Keyed on the RESOLVED values — `AllowApproximate` (which folds
/// in the operator kill switch, so with approximation disabled deployment-wide
/// `exact: true` and the default share one entry) and `ScanShard::new` (which
/// maps a no-op selector like `{index: 0, count: 1}` to the whole table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResultCacheScope {
    allow_approximate: AllowApproximate,
    shard: Option<siglake_storage::ScanShard>,
}

impl ResultCacheScope {
    fn key_fragment(self) -> String {
        let shard = match self.shard {
            Some(shard) => format!("{}/{}", shard.index, shard.count),
            None => "whole".to_string(),
        };
        format!(
            "approx={}|shard={}",
            u8::from(self.allow_approximate.get()),
            shard
        )
    }
}

/// The key of one `/api/v1/sql` result.
///
/// The table identity in it is a whole [`TableGeneration`] — snapshot AND schema
/// id — not a snapshot alone (#2494). An additive `migrate-schema` commits a
/// schema update and no data snapshot, so the served column set widens while the
/// snapshot stands still, and a snapshot-only key replayed the pre-migration
/// body (a filtered `SELECT *` short one column) until the next data commit. The
/// `schema=` field separates those generations; nothing about the invalidation
/// changes, because a schema id advances on the commit exactly as a snapshot id
/// does and the superseded key is simply never asked for again.
fn result_cache_key(
    namespace: &str,
    snapshot_id: i64,
    schema_id: i32,
    query: &str,
    resolved: &ResolvedLimits,
    scope: ResultCacheScope,
) -> String {
    let normalized_query = normalize_query_for_cache(query);
    format!(
        "{}|snapshot={}|schema={}|rows={}|bytes={}|{}|query={}",
        namespace,
        snapshot_id,
        schema_id,
        resolved.max_rows_returned,
        resolved.max_bytes_scanned,
        scope.key_fragment(),
        normalized_query
    )
}

/// The planner's function names, split by what the cache may conclude from
/// them: `pure` is every name that resolves to an IMMUTABLE function, `known`
/// is every name the registry can classify at all, whatever its volatility.
///
/// Both are needed because the two impure verdicts are different operational
/// facts. A `known` name that is not `pure` is expected traffic -- a dashboard
/// calling `now()` -- while a name in neither set is a name the registry cannot
/// classify, which is the signal that this conservatism is costing a deployment
/// its cache. They share one build so the sets cannot disagree about what the
/// session holds.
struct SnapshotFunctionRegistry {
    pure: std::collections::HashSet<String>,
    known: std::collections::HashSet<String>,
}

/// The function names the planner would resolve, by volatility class.
///
/// Read out of the same registry the query session uses -- DataFusion's default
/// scalar/aggregate/window functions plus `crate::udfs::register_udfs` -- rather
/// than kept as a hand-written list, so a future volatile UDF is classified by
/// the volatility its author declared instead of by whether anyone remembered to
/// add it here. Aliases are included because a caller may spell a function by
/// any of them (`current_timestamp` is an alias of `now`).
///
/// Built once. Constructing a `SessionContext` costs a few milliseconds and this
/// runs at most once per process, on the first cacheable-shaped request.
fn snapshot_function_registry() -> &'static SnapshotFunctionRegistry {
    static REGISTRY: OnceLock<SnapshotFunctionRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        use datafusion::logical_expr::Volatility;
        // The planner's own session, not a fresh `SessionContext::new()`: the
        // query template is what decides which functions exist, and the two
        // must not be able to drift apart.
        let ctx = siglake_storage::session_context_with_order(None, None, None);
        crate::udfs::register_udfs(&ctx);
        let state = ctx.state();
        let mut pure = std::collections::HashSet::new();
        let mut known = std::collections::HashSet::new();
        // Every name goes into `known`; only an IMMUTABLE one also goes into
        // `pure`. One pass per registry so a name cannot land in `pure` without
        // landing in `known`.
        let mut record = |name: &str, aliases: &[String], immutable: bool| {
            for name in std::iter::once(name.to_ascii_lowercase())
                .chain(aliases.iter().map(|a| a.to_ascii_lowercase()))
            {
                if immutable {
                    pure.insert(name.clone());
                }
                known.insert(name);
            }
        };
        let immutable = |volatility| volatility == Volatility::Immutable;
        for udf in state.scalar_functions().values() {
            record(
                udf.name(),
                udf.aliases(),
                immutable(udf.signature().volatility),
            );
        }
        for udaf in state.aggregate_functions().values() {
            record(
                udaf.name(),
                udaf.aliases(),
                immutable(udaf.signature().volatility),
            );
        }
        for udwf in state.window_functions().values() {
            record(
                udwf.name(),
                udwf.aliases(),
                immutable(udwf.signature().volatility),
            );
        }
        SnapshotFunctionRegistry { pure, known }
    })
}

/// The one parse every result-cache classification shares: shape
/// ([`statements_have_cacheable_shape`]), purity
/// ([`statements_snapshot_impurity`]) and table resolution
/// ([`referenced_tables_in`]) all read the same AST. `None` means the
/// statement is not classifiable at all — unparseable, or no statement — and
/// nothing unclassifiable is cacheable.
fn parse_for_cache_classification(query: &str) -> Option<Vec<Statement>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).ok()?;
    (!stmts.is_empty()).then_some(stmts)
}

/// Why a statement's answer is something other than a pure function of
/// `(table, snapshot, query)`. Produced by [`statements_snapshot_impurity`].
///
/// THE INVARIANT THIS ENFORCES. The result cache is snapshot-keyed and, by
/// standing rule, never TTL-expired -- an entry "must be a pure function of
/// (table, snapshot, query)". `now()` is resolved server-side against a fresh
/// `Utc::now()` per request but survives into the cache key as the literal text
/// `now()`, so a query containing it is not such a function and nothing checked
/// the premise.
///
/// The consequence is a frozen answer, not a stale-by-seconds one: on a table
/// whose snapshot is not advancing -- an idle tenant, a paused source, an index
/// that only receives periodic batches -- a dashboard's
/// `WHERE timestamp >= now() - interval '15 minutes'` is served the value
/// computed at first miss, indefinitely, with no metric distinguishing it from a
/// legitimate hit.
///
/// WHY THIS IS NOT A SUBSTRING SCAN ANY MORE. The previous check matched literal
/// spellings (`now(`, `current_date`, ...) on the lowered SQL. Two holes, both
/// silently cacheable:
///
///   - **Spelling.** SQL is not sensitive to whitespace or comments between a
///     function name and its argument list, so `now ()` and `now /* gap */ ()`
///     plan exactly like `now()` and matched no needle. The rewrite path
///     preserves the caller's original spelling in the string the cache keys on
///     (`apply_default_order_if_needed` returns `query.to_string()` unchanged
///     whenever it does not rewrite), so whatever the caller typed is what was
///     scanned.
///   - **Volatility.** Clock functions are only one kind of non-pure expression.
///     `random()` and `uuid()` are declared VOLATILE and are re-evaluated per
///     row, per execution; caching one pins the first draw forever.
///
/// So classification is by AST node and declared volatility: parse the SQL, walk
/// every expression, and demand that each function call resolve to an IMMUTABLE
/// entry of the planner's own registry. Only `Volatility::Immutable` qualifies —
/// `now`/`current_date` are STABLE (fixed within one execution, different across
/// executions), which is exactly the frozen-answer defect.
///
/// Everything not classifiable is treated as impure: an unparseable statement,
/// and a function name the registry does not know. A name the registry does not
/// know does not plan either, so the cost of that conservatism is a cache skip
/// on a query that is about to 400; the cost of the other direction is a wrong
/// answer served forever. The caller reaches that verdict for an unparseable
/// statement through [`parse_for_cache_classification`], which hands back
/// `None` and skips the cache outright.
///
/// WHY THE VERDICT IS A REASON AND NOT A BOOL (#1481). The two impure cases are
/// different operational facts and one `skip_time_dependent` counter could not
/// tell them apart: a registered non-immutable function is a dashboard doing
/// what dashboards do, while an unclassifiable name is this conservatism costing
/// a deployment its cache — the query may well plan and succeed anyway, since
/// this classifier's parse is `sqlparser`'s and the planner's is DataFusion's.
/// The caller counts them under separate `outcome` values.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CacheImpurity {
    /// A name the registry resolves, declared STABLE or VOLATILE:
    /// `now`, `current_date`, `random`, `uuid`.
    NonImmutableFunction(String),
    /// A name the registry cannot classify at all. `None` is a name that is not
    /// a plain identifier (an `ObjectNamePart::Function`).
    UnclassifiableFunction(Option<String>),
}

impl CacheImpurity {
    /// The function name for a log line, however it was spelled.
    fn function(&self) -> &str {
        match self {
            Self::NonImmutableFunction(name) => name,
            Self::UnclassifiableFunction(name) => name.as_deref().unwrap_or("<unnamed>"),
        }
    }
}

/// The reason this statement is not a pure function of `(table, snapshot,
/// query)`, or `None` when every function call in it resolves to an IMMUTABLE
/// registry entry. See [`CacheImpurity`].
fn statements_snapshot_impurity(stmts: &[Statement]) -> Option<CacheImpurity> {
    use core::ops::ControlFlow;
    let registry = snapshot_function_registry();
    stmts.iter().find_map(|stmt| {
        visit_expressions(stmt, |expr: &SqlAstExpr| {
            let SqlAstExpr::Function(func) = expr else {
                return ControlFlow::Continue(());
            };
            // The tail identifier: `datafusion.now()` is `now()`. A part that is
            // not a plain identifier (an ObjectNamePart::Function) is not a name
            // we can classify, so it falls through to impure.
            let name = func
                .name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.to_ascii_lowercase());
            match name {
                Some(name) if registry.pure.contains(&name) => ControlFlow::Continue(()),
                Some(name) if registry.known.contains(&name) => {
                    ControlFlow::Break(CacheImpurity::NonImmutableFunction(name))
                }
                other => ControlFlow::Break(CacheImpurity::UnclassifiableFunction(other)),
            }
        })
        .break_value()
    })
}

fn normalize_query_for_cache(query: &str) -> String {
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, query) {
        Ok(stmts) if !stmts.is_empty() => stmts
            .into_iter()
            .map(normalize_statement_for_cache)
            .map(|stmt| stmt.to_string())
            .collect::<Vec<_>>()
            .join("; "),
        _ => query.split_whitespace().collect::<Vec<_>>().join(" "),
    }
}

fn normalize_statement_for_cache(mut stmt: Statement) -> Statement {
    normalize_statement_ast_for_cache(&mut stmt);
    stmt
}

fn normalize_statement_ast_for_cache(stmt: &mut Statement) {
    match stmt {
        Statement::Query(query) => normalize_query_ast_for_cache(query),
        Statement::Explain { statement, .. } => normalize_statement_ast_for_cache(statement),
        Statement::Insert(insert) => {
            if let Some(source) = &mut insert.source {
                normalize_query_ast_for_cache(source);
            }
        }
        _ => {}
    }
}

fn normalize_query_ast_for_cache(query: &mut SqlQuery) {
    if let Some(with) = &mut query.with {
        for cte in &mut with.cte_tables {
            normalize_query_ast_for_cache(&mut cte.query);
        }
    }
    normalize_set_expr_ast_for_cache(query.body.as_mut());
}

fn normalize_set_expr_ast_for_cache(expr: &mut SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            if let Some(selection) = select.selection.take() {
                select.selection = simplify_cache_sql_predicate(selection);
            }
        }
        SetExpr::Query(query) => normalize_query_ast_for_cache(query),
        SetExpr::SetOperation { left, right, .. } => {
            normalize_set_expr_ast_for_cache(left);
            normalize_set_expr_ast_for_cache(right);
        }
        _ => {}
    }
}

fn simplify_cache_sql_predicate(expr: SqlAstExpr) -> Option<SqlAstExpr> {
    match expr {
        SqlAstExpr::BinaryOp {
            left,
            op: SqlBinaryOperator::And,
            right,
        } => {
            let left = simplify_cache_sql_predicate(*left);
            let right = simplify_cache_sql_predicate(*right);
            match (left, right) {
                (Some(left), Some(right)) => Some(SqlAstExpr::BinaryOp {
                    left: Box::new(left),
                    op: SqlBinaryOperator::And,
                    right: Box::new(right),
                }),
                (Some(left), None) => Some(left),
                (None, Some(right)) => Some(right),
                (None, None) => None,
            }
        }
        SqlAstExpr::Nested(expr) => {
            simplify_cache_sql_predicate(*expr).map(|inner| SqlAstExpr::Nested(Box::new(inner)))
        }
        other if sql_ast_expr_is_constant_true(&other) => None,
        other => Some(other),
    }
}

fn sql_ast_expr_is_constant_true(expr: &SqlAstExpr) -> bool {
    match expr {
        SqlAstExpr::Value(value) => matches!(&value.value, SqlValue::Boolean(true)),
        SqlAstExpr::Nested(expr) => sql_ast_expr_is_constant_true(expr),
        SqlAstExpr::BinaryOp { left, op, right } => {
            let Some(left) = sql_ast_literal(left) else {
                return false;
            };
            let Some(right) = sql_ast_literal(right) else {
                return false;
            };
            match op {
                SqlBinaryOperator::Eq => {
                    !matches!(left, SqlValue::Null)
                        && std::mem::discriminant(left) == std::mem::discriminant(right)
                        && left == right
                }
                SqlBinaryOperator::NotEq => match (left, right) {
                    (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left != right,
                    (SqlValue::SingleQuotedString(left), SqlValue::SingleQuotedString(right)) => {
                        left != right
                    }
                    _ => false,
                },
                _ => false,
            }
        }
        _ => false,
    }
}

fn sql_ast_literal(expr: &SqlAstExpr) -> Option<&SqlValue> {
    match expr {
        SqlAstExpr::Value(value) if !matches!(value.value, SqlValue::Placeholder(_)) => {
            Some(&value.value)
        }
        SqlAstExpr::Nested(expr) => sql_ast_literal(expr),
        _ => None,
    }
}

const SEARCH_GUIDANCE: &str =
    "search() requires an index with default_search_fields; qualify with match_terms(column, ...)";

fn sql_ast_expr_contains_search(expr: &SqlAstExpr) -> bool {
    use core::ops::ControlFlow;
    visit_expressions(expr, |e: &SqlAstExpr| {
        if let SqlAstExpr::Function(func) = e {
            if func
                .name
                .0
                .last()
                .is_some_and(|part| part.to_string().eq_ignore_ascii_case("search"))
            {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

fn statement_contains_search(stmt: &Statement) -> bool {
    use core::ops::ControlFlow;
    visit_expressions(stmt, |expr: &SqlAstExpr| {
        if sql_ast_expr_contains_search(expr) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
}

fn query_contains_subquery(query: &SqlQuery) -> bool {
    use core::ops::ControlFlow;
    visit_expressions(query, |expr: &SqlAstExpr| match expr {
        SqlAstExpr::Subquery(_) | SqlAstExpr::Exists { .. } | SqlAstExpr::InSubquery { .. } => {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn search_string_literal(func: &datafusion::sql::sqlparser::ast::Function) -> Option<String> {
    let FunctionArguments::List(args) = &func.args else {
        return None;
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() || args.args.len() != 1 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlAstExpr::Value(value))) = &args.args[0]
    else {
        return None;
    };
    match &value.value {
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

fn match_terms_call(field: &str, query: &str) -> SqlAstExpr {
    SqlAstExpr::Function(datafusion::sql::sqlparser::ast::Function {
        name: vec!["match_terms".into()].into(),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(datafusion::sql::sqlparser::ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlAstExpr::Identifier(field.into()))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlAstExpr::Value(
                    SqlValue::SingleQuotedString(query.to_string()).into(),
                ))),
            ],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

async fn rewrite_search_if_needed(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
) -> Result<String, ApiError> {
    let dialect = GenericDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, query).map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
    let has_search = stmts.iter().any(statement_contains_search);
    if !has_search {
        return Ok(query.to_string());
    }
    if stmts.len() != 1 {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    }
    let Statement::Query(sql_query) = &mut stmts[0] else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    if sql_query.with.is_some() || query_contains_subquery(sql_query) {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    }
    let SetExpr::Select(select) = sql_query.body.as_mut() else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    let table_name = name.to_string();
    let Some(index_id) = object_name_tail(&table_name) else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    let Some(config) = ice
        .cached_index_config(index_id)
        .await
        .map_err(ApiError::internal)?
    else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    if config.doc_mapping.default_search_fields.is_empty() {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    }
    let Some(selection) = select.selection.as_mut() else {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    };
    let fields = config.doc_mapping.default_search_fields.clone();
    let mut invalid = None::<String>;
    let mut rewrote = false;
    let _ = visit_expressions_mut(selection, |expr: &mut SqlAstExpr| {
        if let SqlAstExpr::Function(func) = expr {
            if func
                .name
                .0
                .last()
                .is_some_and(|part| part.to_string().eq_ignore_ascii_case("search"))
            {
                let Some(query) = search_string_literal(func) else {
                    invalid =
                        Some("search() requires a single string literal argument".to_string());
                    return core::ops::ControlFlow::Break(());
                };
                let mut expanded = fields.iter().map(|field| match_terms_call(field, &query));
                let Some(mut replacement) = expanded.next() else {
                    invalid = Some(SEARCH_GUIDANCE.to_string());
                    return core::ops::ControlFlow::Break(());
                };
                for clause in expanded {
                    replacement = SqlAstExpr::BinaryOp {
                        left: Box::new(replacement),
                        op: SqlBinaryOperator::Or,
                        right: Box::new(clause),
                    };
                }
                *expr = SqlAstExpr::Nested(Box::new(replacement));
                rewrote = true;
            }
        }
        core::ops::ControlFlow::Continue(())
    });
    if let Some(msg) = invalid {
        return Err(ApiError::bad_request(msg));
    }
    if !rewrote {
        return Err(ApiError::bad_request(SEARCH_GUIDANCE));
    }
    Ok(Statement::Query(sql_query.clone()).to_string())
}

async fn apply_query_rewrites(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
    default_order: bool,
    priority: Priority,
    max_rows: usize,
) -> Result<RewrittenQuery, ApiError> {
    let search_rewritten = rewrite_search_if_needed(ice, query).await?;
    // WS-7: `attr_get(attributes, 'key')` → promoted column, gated on the
    // backfill-complete property (unsound before it: pre-promotion files
    // read the column as NULL while their JSON still holds the key). The
    // substituted text flows everywhere downstream — the Tier-1 group-count
    // battery sees a plain `GROUP BY <column>` (~3ms metadata answer), the
    // provider prunes natively, shard fan-out ships the rewritten SQL.
    let search_rewritten = if search_rewritten.contains("attr_get") {
        // The gate is table-local: a promoted column is sound only after that
        // table's backfill property matches its promotion list. Load every
        // referenced table's independently gated map, then let the AST scope
        // resolver apply a map only where the column owner is proven.
        let mut mappings = HashMap::new();
        for name in referenced_tables(&search_rewritten).unwrap_or_default() {
            if name == "query_audit" {
                continue;
            }
            let ident = if name == "events" {
                ice.events_table_ident().clone()
            } else {
                ice.index_table_ident(&name)
            };
            if let Ok(Some(mapping)) = ice.attr_rewrite_map_for(&ident).await {
                mappings.insert(name, mapping);
            }
        }
        rewrite_attr_get_to_columns(&search_rewritten, &mappings).unwrap_or(search_rewritten)
    } else {
        search_rewritten
    };
    // The table whose event-time field this query's order contract turns on.
    // The implicit rewrite needs it to inject an order; a query that ALREADY
    // orders by a non-canonical column needs it to decide whether that column
    // is the index's event time and may therefore carry a scan hint.
    let ordered_index =
        match resolve_default_order_index(ice, &search_rewritten, default_order, priority).await {
            Some(resolved) => Some(resolved),
            None => resolve_explicit_order_index(ice, &search_rewritten).await,
        };
    Ok(apply_default_order_if_needed(
        &search_rewritten,
        default_order,
        priority,
        max_rows,
        ordered_index
            .as_ref()
            .map(|(table, field)| (table.as_str(), field.as_str())),
    ))
}

/// The managed index and event-time field behind a query that already carries
/// its own `ORDER BY <field>` over a non-canonical column. `None` — and the
/// canonical column — for every other shape; see
/// [`resolve_query_event_time_field`] for what that costs.
async fn resolve_explicit_order_index(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
) -> Option<(String, String)> {
    let field = resolve_query_event_time_field(ice, query).await?;
    let table = single_scan_table(&sole_query_statement(query)?)?;
    Some((table, field))
}

/// The one `SELECT` a request is, seen through an `EXPLAIN` when it is one.
fn sole_query_statement(sql: &str) -> Option<SqlQuery> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    match stmts.pop()? {
        Statement::Query(query) => Some(*query),
        // #91: EXPLAIN inherits the inner query's hint, so it inherits the
        // field that hint is about.
        Statement::Explain { statement, .. } => match *statement {
            Statement::Query(inner) => Some(*inner),
            _ => None,
        },
        _ => None,
    }
}

/// The managed index a bare SELECT may be ordered by, resolved against the
/// CALLER'S tenant context.
///
/// The implicit newest-first rewrite was gated on a hard-coded name list, so
/// it fired for `events` and for nothing else — a WI-1 index, which registers
/// dynamically and carries the same `timestamp` column, browsed in file order
/// and could never reach the ordered scan path (`PreferredScanOrder` /
/// `OrderedScanLimit`) without the caller spelling `ORDER BY` out (#4038).
///
/// `None` — no index-driven rewrite — when the query's shape disqualifies it
/// anyway, when the table is one this module already knows, or when the name is
/// not a managed index of this tenant. A mapping that names its own
/// `timestamp_field` is eligible like any other and comes back WITH that field,
/// which the rewrite quotes and the scan hint carries (#6020). Eligibility
/// comes from the bounded-staleness table cache, so the warm path adds a
/// catalog row lookup and no S3 metadata read; a shape that would not be
/// rewritten never asks at all.
async fn resolve_default_order_index(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
    default_order: bool,
    priority: Priority,
) -> Option<(String, String)> {
    if !default_order || priority == Priority::Batch {
        return None;
    }
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).ok()?;
    let [Statement::Query(sql_query)] = stmts.as_slice() else {
        return None;
    };
    // The alias-shadow half of the shape test is deliberately left to the
    // rewrite: it compares against the very field this lookup resolves.
    let table = default_order_shape_table(sql_query)?;
    if query_table_has_timestamp(&table) {
        return None;
    }
    index_event_time_field(ice, &table)
        .await
        .map(|field| (table, field))
}

/// One managed index's event-time field, or `None` when the name is not a
/// managed index of this tenant.
///
/// A catalog hiccup answers `None`: the order contract is an optimisation of
/// presentation, never a correctness gate, so a failed lookup leaves the query
/// unordered rather than failing it, and planning reports a missing table.
async fn index_event_time_field(
    ice: &siglake_storage::iceberg::IcebergContext,
    table: &str,
) -> Option<String> {
    match ice.index_event_time_field(table).await {
        Ok(field) => field,
        Err(err) => {
            tracing::debug!(table = %table, error = %err, "event-time field lookup failed");
            None
        }
    }
}

/// The event-time field to judge a query's order contract by, when it is not
/// the canonical `timestamp`.
///
/// `None` — meaning `timestamp`, today's behavior exactly — for a query that
/// names no single table, one that carries no `ORDER BY` of its own (the
/// implicit rewrite resolves its own field, and a query with neither has no
/// order contract to judge), one whose lead `ORDER BY` is already the canonical
/// column or is not a column at all, one over a table this module knows, and
/// one whose table is not a managed index. Only the remaining shape pays the
/// (cached-metadata) mapping lookup.
async fn resolve_query_event_time_field(
    ice: &siglake_storage::iceberg::IcebergContext,
    sql: &str,
) -> Option<String> {
    let query = sole_query_statement(sql)?;
    let order_by = query.order_by.as_ref()?;
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return None;
    };
    let lead = exprs.first()?;
    if sql_order_expr_is_field(&lead.expr, CANONICAL_EVENT_TIME) || sql_ident(&lead.expr).is_none()
    {
        return None;
    }
    let table = single_scan_table(&query)?;
    if query_table_has_timestamp(&table) {
        return None;
    }
    index_event_time_field(ice, &table)
        .await
        .filter(|field| field != CANONICAL_EVENT_TIME)
}

/// The event-time field the order contract for `query` is about: the resolved
/// index field when this query reads that index, the canonical column
/// otherwise.
fn query_order_field<'a>(query: &SqlQuery, ordered_index: Option<(&'a str, &'a str)>) -> &'a str {
    match (ordered_index, single_scan_table(query)) {
        (Some((index, field)), Some(table)) if index == table => field,
        _ => CANONICAL_EVENT_TIME,
    }
}

/// The event-time field of the canonical `events` table, and the fallback for
/// every table whose mapping does not name another one.
const CANONICAL_EVENT_TIME: &str = siglake_storage::CANONICAL_EVENT_TIME_FIELD;

type AttrRewriteMap = HashMap<String, (String, siglake_core::PromotedType)>;

/// WS-7 rewrite mechanics: replace every `attr_get(attributes, 'key')` whose
/// column owner is proven and whose owning table's `mapping` contains the key.
/// `None` when the SQL doesn't parse as a single statement or nothing matched.
fn rewrite_attr_get_to_columns(
    sql: &str,
    mappings: &HashMap<String, AttrRewriteMap>,
) -> Option<String> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let mut changed = false;
    // PASS 1 — typed comparison shapes. Must run as its own walk: the visitor
    // is POST-order (children before parents), so a single-pass version saw
    // the bare `attr_get` first and CAST-substituted it before the BinaryOp
    // could claim the typed rewrite. In this pass Function nodes are left
    // untouched, so when the parent BinaryOp is visited its children are
    // intact: `attr_get(a,'k') <op> '<lit>'` where the key promoted to a
    // numeric/bool column and the literal parses as that type →
    // `col <op> <typed literal>` — native row-group stats pruning on ranges.
    struct Rewriter<'a> {
        mappings: &'a HashMap<String, AttrRewriteMap>,
        scopes: Vec<AttrRewriteScope>,
        pass: AttrRewritePass,
        changed: &'a mut bool,
    }
    impl VisitorMut for Rewriter<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut SqlQuery) -> core::ops::ControlFlow<()> {
            let inherited_ctes = self
                .scopes
                .last()
                .map(|scope| scope.visible_ctes.clone())
                .unwrap_or_default();
            self.scopes
                .push(AttrRewriteScope::for_query(query, inherited_ctes));
            core::ops::ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &mut SqlQuery) -> core::ops::ControlFlow<()> {
            self.scopes.pop();
            core::ops::ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &mut SqlAstExpr) -> core::ops::ControlFlow<()> {
            match self.pass {
                AttrRewritePass::TypedComparisons => {
                    rewrite_typed_attr_comparison(expr, self.mappings, &self.scopes, self.changed)
                }
                AttrRewritePass::Remaining => {
                    rewrite_remaining_attr_get(expr, self.mappings, &self.scopes, self.changed)
                }
            }
            core::ops::ControlFlow::Continue(())
        }
    }

    let _ = VisitMut::visit(
        &mut stmts[0],
        &mut Rewriter {
            mappings,
            scopes: Vec::new(),
            pass: AttrRewritePass::TypedComparisons,
            changed: &mut changed,
        },
    );
    let _ = VisitMut::visit(
        &mut stmts[0],
        &mut Rewriter {
            mappings,
            scopes: Vec::new(),
            pass: AttrRewritePass::Remaining,
            changed: &mut changed,
        },
    );
    changed.then(|| stmts[0].to_string())
}

#[derive(Clone, Copy)]
enum AttrRewritePass {
    TypedComparisons,
    Remaining,
}

#[derive(Default)]
struct AttrRewriteScope {
    /// SQL qualifier (table name or alias) -> physical table. `None` is a
    /// known CTE/derived owner and deliberately shadows an outer qualifier.
    qualifiers: HashMap<String, Option<String>>,
    unqualified_owner: Option<String>,
    visible_ctes: Vec<String>,
    blocks_outer_qualifiers: bool,
}

impl AttrRewriteScope {
    fn for_query(query: &SqlQuery, mut visible_ctes: Vec<String>) -> Self {
        if let Some(with) = &query.with {
            visible_ctes.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| cte.alias.name.value.to_ascii_lowercase()),
            );
        }
        let mut scope = Self {
            visible_ctes,
            ..Self::default()
        };
        let branch_owners = scope.add_set_expr(&query.body);
        if let Some(Some(owner)) = branch_owners.first() {
            if branch_owners
                .iter()
                .all(|candidate| candidate.as_ref() == Some(owner))
            {
                scope.unqualified_owner = Some(owner.clone());
            }
        }
        scope
    }

    /// Collect one unqualified owner per SELECT branch. Set-operation branches
    /// share a query visitor scope, so they are rewriteable only when every
    /// branch independently proves the same lone owner. That preserves the
    /// former single-table UNION behavior without treating JOIN inputs as one.
    fn add_set_expr(&mut self, expr: &SetExpr) -> Vec<Option<String>> {
        match expr {
            SetExpr::Select(select) => vec![self.add_select(select)],
            SetExpr::SetOperation { left, right, .. } => {
                let mut owners = self.add_set_expr(left);
                owners.extend(self.add_set_expr(right));
                owners
            }
            // A parenthesized Query receives its own visitor scope.
            _ => vec![None],
        }
    }

    fn add_select(&mut self, select: &Select) -> Option<String> {
        let mut owners = Vec::new();
        for from in &select.from {
            self.add_table_factor(&from.relation, &mut owners);
            for join in &from.joins {
                self.add_table_factor(&join.relation, &mut owners);
            }
        }
        if let [Some(owner)] = owners.as_slice() {
            Some(owner.clone())
        } else {
            None
        }
    }

    fn add_table_factor(&mut self, factor: &TableFactor, owners: &mut Vec<Option<String>>) {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let rendered = name.to_string();
                let Some(tail) = object_name_tail(&rendered) else {
                    owners.push(None);
                    return;
                };
                let owner = (!self
                    .visible_ctes
                    .iter()
                    .any(|cte| cte.eq_ignore_ascii_case(tail)))
                .then(|| canonical_query_table(tail).unwrap_or(tail).to_string());
                owners.push(owner.clone());
                if let Some(alias) = alias {
                    self.bind_qualifier(&alias.name.value, owner);
                } else {
                    self.bind_qualifier(tail, owner.clone());
                    self.bind_qualifier(&rendered.replace(['"', '`'], ""), owner);
                }
            }
            TableFactor::Derived { alias, .. } => {
                owners.push(None);
                if let Some(alias) = alias {
                    self.bind_qualifier(&alias.name.value, None);
                }
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                if let Some(alias) = alias {
                    owners.push(None);
                    self.bind_qualifier(&alias.name.value, None);
                } else {
                    self.add_table_factor(&table_with_joins.relation, owners);
                    for join in &table_with_joins.joins {
                        self.add_table_factor(&join.relation, owners);
                    }
                }
            }
            _ => {
                owners.push(None);
                // We do not enumerate every dialect-specific alias-bearing
                // table factor. Conservatively prevent a qualifier from
                // resolving through this scope to an outer table.
                self.blocks_outer_qualifiers = true;
            }
        }
    }

    fn bind_qualifier(&mut self, qualifier: &str, owner: Option<String>) {
        use std::collections::hash_map::Entry;
        let qualifier = qualifier.to_ascii_lowercase();
        match self.qualifiers.entry(qualifier) {
            Entry::Vacant(entry) => {
                entry.insert(owner);
            }
            Entry::Occupied(mut entry) if entry.get() != &owner => {
                entry.insert(None);
            }
            Entry::Occupied(_) => {}
        }
    }
}

fn rewrite_typed_attr_comparison(
    expr: &mut SqlAstExpr,
    mappings: &HashMap<String, AttrRewriteMap>,
    scopes: &[AttrRewriteScope],
    changed: &mut bool,
) {
    use datafusion::sql::sqlparser::ast::BinaryOperator as Op;
    use siglake_core::PromotedType;
    if let SqlAstExpr::BinaryOp { left, op, right } = expr {
        if matches!(
            op,
            Op::Eq | Op::NotEq | Op::Lt | Op::LtEq | Op::Gt | Op::GtEq
        ) {
            // Two-phase: decide immutably, then apply (the borrow checker
            // rejects assigning through sides still borrowed by a probe).
            let mut decision: Option<(bool, SqlAstExpr, SqlValue)> = None;
            for (attr_on_left, probe, other) in [
                (true, left.as_ref(), right.as_ref()),
                (false, right.as_ref(), left.as_ref()),
            ] {
                let target = match probe {
                    SqlAstExpr::Function(f) => attr_get_rewrite_target(f, mappings, scopes),
                    _ => None,
                };
                let (column, ty) = match target {
                    Some(t) if t.1 != PromotedType::Utf8 => t,
                    _ => continue,
                };
                let Some(lit) = (match other {
                    SqlAstExpr::Value(v) => match &v.value {
                        SqlValue::SingleQuotedString(l) | SqlValue::DoubleQuotedString(l) => {
                            Some(l.clone())
                        }
                        SqlValue::Number(l, _) => Some(l.clone()),
                        _ => None,
                    },
                    _ => None,
                }) else {
                    continue;
                };
                let typed_value = match ty {
                    PromotedType::Int64 if lit.parse::<i64>().is_ok() => {
                        SqlValue::Number(lit, false)
                    }
                    PromotedType::Float64 if lit.parse::<f64>().is_ok() => {
                        SqlValue::Number(lit, false)
                    }
                    PromotedType::Boolean => match lit.to_ascii_lowercase().as_str() {
                        "true" => SqlValue::Boolean(true),
                        "false" => SqlValue::Boolean(false),
                        _ => continue,
                    },
                    _ => continue,
                };
                decision = Some((attr_on_left, column, typed_value));
                break;
            }
            if let Some((attr_on_left, column, typed_value)) = decision {
                let lit_expr = SqlAstExpr::Value(typed_value.into());
                if attr_on_left {
                    **left = column;
                    **right = lit_expr;
                } else {
                    **right = column;
                    **left = lit_expr;
                }
                *changed = true;
            }
        }
    }
}

fn rewrite_remaining_attr_get(
    expr: &mut SqlAstExpr,
    mappings: &HashMap<String, AttrRewriteMap>,
    scopes: &[AttrRewriteScope],
    changed: &mut bool,
) {
    use datafusion::sql::sqlparser::ast::{CastKind, DataType as SqlDataType};
    use siglake_core::PromotedType;
    // PASS 2 — every remaining `attr_get` occurrence (projections, GROUP BYs,
    // non-comparison predicates).
    if let SqlAstExpr::Function(func) = expr {
        let is_attr_get = func
            .name
            .0
            .last()
            .is_some_and(|part| part.to_string().eq_ignore_ascii_case("attr_get"));
        if is_attr_get {
            if let Some((column, ty)) = attr_get_rewrite_target(func, mappings, scopes) {
                // Utf8: type-preserving direct substitution. Typed: CAST
                // back to VARCHAR so projections/GROUP BYs keep attr_get's
                // string output type (JSON scalars stringify canonically,
                // matching the CAST rendering).
                *expr = if ty == PromotedType::Utf8 {
                    column
                } else {
                    SqlAstExpr::Cast {
                        kind: CastKind::Cast,
                        expr: Box::new(column),
                        data_type: SqlDataType::Varchar(None),
                        array: false,
                        format: None,
                    }
                };
                *changed = true;
            }
        }
    }
}

/// The promoted column for one `attr_get` call — `Some` only for the exact
/// shape `attr_get(attributes, '<literal key>')` with a mapped key.
fn attr_get_rewrite_target(
    func: &datafusion::sql::sqlparser::ast::Function,
    mappings: &HashMap<String, AttrRewriteMap>,
    scopes: &[AttrRewriteScope],
) -> Option<(SqlAstExpr, siglake_core::PromotedType)> {
    let FunctionArguments::List(args) = &func.args else {
        return None;
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() || args.args.len() != 2 {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(first)) = &args.args[0] else {
        return None;
    };
    let (owner, qualifier) = match first {
        SqlAstExpr::Identifier(id) if id.value.eq_ignore_ascii_case("attributes") => {
            (scopes.last()?.unqualified_owner.as_deref()?, None)
        }
        SqlAstExpr::CompoundIdentifier(parts)
            if parts
                .last()
                .is_some_and(|id| id.value.eq_ignore_ascii_case("attributes")) =>
        {
            let qualifier = parts[..parts.len() - 1]
                .iter()
                .map(|id| id.value.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(".");
            let mut owner = None;
            for scope in scopes.iter().rev() {
                if let Some(found) = scope.qualifiers.get(&qualifier) {
                    owner = found.as_deref();
                    break;
                }
                if scope.blocks_outer_qualifiers {
                    break;
                }
            }
            (owner?, Some(parts[..parts.len() - 1].to_vec()))
        }
        _ => return None,
    };
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlAstExpr::Value(value))) = &args.args[1]
    else {
        return None;
    };
    let key = match &value.value {
        SqlValue::SingleQuotedString(k) | SqlValue::DoubleQuotedString(k) => k,
        _ => return None,
    };
    let mapping = mappings.get(owner)?;
    let (column, ty) = mapping.get(key)?.clone();
    let column = match qualifier {
        None => SqlAstExpr::Identifier(column.as_str().into()),
        Some(mut parts) => {
            parts.push(column.as_str().into());
            SqlAstExpr::CompoundIdentifier(parts)
        }
    };
    Some((column, ty))
}

#[derive(Clone)]
struct RewrittenQuery {
    sql: String,
    preferred_scan_order: Option<siglake_storage::PreferredScanOrder>,
    /// Literal `LIMIT n` of a timestamp-ordered query — rides into the
    /// session as `OrderedScanLimit` so the gate can coalesce partitions.
    ordered_limit: Option<usize>,
    /// Literal `LIMIT n` of an UNordered query whose limit clips the scan row
    /// for row (see [`clipping_scan_limit`]) — rides into the session as
    /// `ClippedScanLimit`. Disjoint from `ordered_limit` by construction: the
    /// shape test refuses a query carrying an ORDER BY, including the one the
    /// newest-first rewrite injects.
    clipped_limit: Option<usize>,
}

/// Inject the implicit newest-first ordering into a qualifying bare SELECT.
///
/// `ordered_index` is the managed index the caller already resolved, paired
/// with its event-time field (see [`resolve_default_order_index`]); `events`
/// and `query_audit` are known to carry `timestamp` without it. Every order
/// judgement below — the injected `ORDER BY`, the alias-shadow guard, the scan
/// hint — is made against that one field.
fn apply_default_order_if_needed(
    query: &str,
    enabled: bool,
    priority: Priority,
    max_rows: usize,
    ordered_index: Option<(&str, &str)>,
) -> RewrittenQuery {
    let dialect = GenericDialect {};
    let mut stmts = match Parser::parse_sql(&dialect, query) {
        Ok(stmts) if stmts.len() == 1 => stmts,
        _ => {
            return RewrittenQuery {
                sql: query.to_string(),
                preferred_scan_order: None,
                ordered_limit: None,
                clipped_limit: None,
            };
        }
    };
    // #91: EXPLAIN derives the scan-order hint from its INNER statement (but
    // never rewrites — the plan shown should be the query's own). Without
    // this, EXPLAIN of an ordered browse planned WITHOUT PreferredScanOrder
    // and showed a blocking TopK even when the real query early-stops —
    // misleading enough to cost a diagnosis detour (2026-07-12).
    if let Statement::Explain { statement, .. } = &stmts[0] {
        if let Statement::Query(inner) = statement.as_ref() {
            return RewrittenQuery {
                sql: query.to_string(),
                preferred_scan_order: preferred_scan_order_from_query(
                    inner,
                    query_order_field(inner, ordered_index),
                ),
                // The hints an EXPLAIN carries are the ones that decide the
                // PLAN the caller asked to see. `clipping_scan_limit` decides
                // the text-index path and prints in the scan's display; a
                // missing hint here would show an index the real query
                // declines (#91's argument for the scan-order hint).
                ordered_limit: None,
                clipped_limit: clipping_scan_limit(inner),
            };
        }
        return RewrittenQuery {
            sql: query.to_string(),
            preferred_scan_order: None,
            ordered_limit: None,
            clipped_limit: None,
        };
    }
    let Statement::Query(sql_query) = &mut stmts[0] else {
        return RewrittenQuery {
            sql: query.to_string(),
            preferred_scan_order: None,
            ordered_limit: None,
            clipped_limit: None,
        };
    };
    // The event-time field of a table this rewrite may order, or `None` when
    // it may not order that table at all.
    let table_order_field = |table: &str| -> Option<&str> {
        if query_table_has_timestamp(table) {
            return Some(CANONICAL_EVENT_TIME);
        }
        // Index ids are catalog table names: matched exactly, unlike the
        // case-folded system names.
        match ordered_index {
            Some((index, field)) if index == table => Some(field),
            _ => None,
        }
    };
    if enabled
        && priority != Priority::Batch
        && apply_default_order_to_query(sql_query, max_rows, table_order_field).is_some()
    {
        let preferred_scan_order =
            preferred_scan_order_from_query(sql_query, query_order_field(sql_query, ordered_index));
        let sql = Statement::Query(sql_query.clone()).to_string();
        if preferred_scan_order.is_some() {
            metrics::counter!("siglake_query_default_order_applied_total").increment(1);
        }
        let ordered_limit = preferred_scan_order
            .as_ref()
            .and_then(|_| explicit_limit_value(sql_query));
        return RewrittenQuery {
            sql,
            preferred_scan_order,
            ordered_limit,
            // The rewrite injected an ORDER BY, so the shape test refuses:
            // this browse is the ordered decline's business, not the clipped
            // one's.
            clipped_limit: clipping_scan_limit(sql_query),
        };
    }
    let preferred_scan_order =
        preferred_scan_order_from_query(sql_query, query_order_field(sql_query, ordered_index));
    RewrittenQuery {
        ordered_limit: preferred_scan_order
            .as_ref()
            .and_then(|_| explicit_limit_value(sql_query)),
        sql: query.to_string(),
        preferred_scan_order,
        clipped_limit: clipping_scan_limit(sql_query),
    }
}

#[cfg(test)]
fn rewrite_query_with_default_order(
    query: &str,
    max_rows: usize,
    table_order_field: impl Fn(&str) -> Option<&'static str>,
) -> Option<String> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, query).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let Statement::Query(sql_query) = &mut stmts[0] else {
        return None;
    };
    apply_default_order_to_query(sql_query, max_rows, table_order_field)?;
    Some(Statement::Query(sql_query.clone()).to_string())
}

/// Rewrite `query` in place to browse its table newest-first. `Some(field)` —
/// the event-time field it ordered by — when it rewrote.
fn apply_default_order_to_query<'a>(
    query: &mut SqlQuery,
    max_rows: usize,
    table_order_field: impl Fn(&str) -> Option<&'a str>,
) -> Option<&'a str> {
    let table = default_order_shape_table(query)?;
    let field = table_order_field(&table)?;
    // The alias-shadow guard, now that the field is known: `SELECT raw AS "ts"`
    // over a `ts`-mapped index captures the injected order exactly as an alias
    // named `timestamp` captures the canonical one.
    default_order_target_table(query, field)?;

    query.order_by = Some(OrderBy {
        kind: OrderByKind::Expressions(vec![OrderByExpr {
            expr: SqlAstExpr::Identifier(order_by_ident(field)),
            options: OrderByOptions {
                asc: Some(false),
                nulls_first: None,
            },
            with_fill: None,
        }]),
        interpolate: None,
    });
    if !query_has_explicit_limit(query) {
        // LIMIT ceiling+1, NOT ceiling: the format layer truncates to the
        // ceiling and signals 413/`truncated` only when it observes an
        // overflow row. An exact-ceiling LIMIT would return precisely
        // ceiling rows and silently erase that contract (caught by
        // tests/query_server/limits.rs::request_can_tighten_max_rows_returned). The +1
        // still bounds the ordered scan's early-stop at k = ceiling+1.
        let limit_rows = max_rows.saturating_add(1);
        let limit = Some(SqlAstExpr::Value(
            SqlValue::Number(limit_rows.to_string(), false).into(),
        ));
        query.limit_clause = Some(match query.limit_clause.take() {
            Some(LimitClause::LimitOffset {
                offset, limit_by, ..
            }) => LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            },
            _ => LimitClause::LimitOffset {
                limit,
                offset: None,
                limit_by: Vec::new(),
            },
        });
    }
    Some(field)
}

/// The single table a default-order rewrite would act on once its event-time
/// field is known: [`default_order_shape_table`] plus the alias-shadow guard,
/// which can only be judged against that field.
fn default_order_target_table(query: &SqlQuery, order_field: &str) -> Option<String> {
    let table = default_order_shape_table(query)?;
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    (!projection_shadows_order_field(select, order_field)).then_some(table)
}

/// The single table a default-order rewrite would act on, or `None` when the
/// query's SHAPE disqualifies it (a CTE, an explicit ORDER BY, an aggregate, a
/// join, …). Says nothing about whether that table is eligible — the caller
/// decides that, and for a managed index deciding it needs `await`, which is
/// why the shape test is separate from the rewrite.
///
/// The alias-shadow guard is NOT here: it compares against the table's
/// event-time field, which the caller resolves from this table name. See
/// [`default_order_target_table`].
fn default_order_shape_table(query: &SqlQuery) -> Option<String> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.distinct.is_some()
        || select.having.is_some()
        || !matches!(select.group_by, datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(ref exprs, _) if exprs.is_empty())
        || select.from.len() != 1
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || select.value_table_mode.is_some()
    {
        return None;
    }
    if select
        .projection
        .iter()
        .filter_map(select_item_expr)
        .any(sql_expr_looks_aggregate)
    {
        return None;
    }
    single_scan_table(query)
}

/// The one table a query reads, when it reads exactly one plain table with no
/// join. Says nothing about the rest of the query's shape — the order contract
/// needs the table name to resolve its event-time field before any judgement
/// about ordering can be made.
fn single_scan_table(query: &SqlQuery) -> Option<String> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return None;
    };
    Some(object_name_tail(&name.to_string())?.to_string())
}

/// True when the projection binds the OUTPUT name `timestamp` to something
/// other than the source timestamp column (`SELECT raw AS timestamp …`).
///
/// SQL resolves a bare ORDER BY identifier against the output names first, so
/// such a projection captures the rewrite's injected `ORDER BY timestamp DESC`
/// and the browse silently comes back ordered by that other column — a
/// different top-N, not a different rendering of the same rows (#4090). The
/// sort cannot be re-bound to the source column either: DataFusion refuses
/// `ORDER BY "tbl".timestamp` under this projection as an ambiguous reference
/// (`DFSchema::check_names` sees a qualified `tbl.timestamp` beside the
/// unqualified alias), so a qualified injection would turn a 200 into a 400.
/// The shape therefore disqualifies the rewrite, exactly as `default_order:
/// false` would: file order, and no scan-order hint.
fn projection_shadows_order_field(select: &Select, field: &str) -> bool {
    select.projection.iter().any(|item| match item {
        SelectItem::ExprWithAlias { expr, alias } => {
            ident_is_field(alias, field) && !sql_order_expr_is_field(expr, field)
        }
        _ => false,
    })
}

/// A bare event-time identifier in ORDER BY — the only form a projection
/// alias can capture. A qualified `t.timestamp` names the source column and
/// cannot be shadowed (it is either unambiguous or rejected outright).
fn sql_order_expr_is_bare_field(expr: &SqlAstExpr, field: &str) -> bool {
    match expr {
        SqlAstExpr::Identifier(ident) => ident_is_field(ident, field),
        SqlAstExpr::Nested(expr) => sql_order_expr_is_bare_field(expr, field),
        _ => false,
    }
}

fn select_item_expr(item: &SelectItem) -> Option<&SqlAstExpr> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(expr),
        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

/// Selectivity-aware ordered policy (07-16 filed): the shape of an
/// ordered-LIMIT browse with exactly ONE residual equality/IN/negation term
/// on a string column (plus any pure time-range terms). For such shapes the
/// server measures the term's selectivity from the group-count side
/// aggregates and, when LOW-selectivity, lets the scan advertise ordering —
/// an early-stop drain needs ~LIMIT/frac rows where the blanket
/// filtered-refusal forces a full-window TopK (a 1/3-selective attribute
/// browse full-scanned into the rows breaker on the 200G board).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResidualBrowseShape {
    table: String,
    column: String,
    values: Vec<String>,
    negated: bool,
}

fn flatten_and_conjuncts<'a>(expr: &'a SqlAstExpr, out: &mut Vec<&'a SqlAstExpr>) {
    match expr {
        SqlAstExpr::BinaryOp {
            left,
            op: datafusion::sql::sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            flatten_and_conjuncts(left, out);
            flatten_and_conjuncts(right, out);
        }
        SqlAstExpr::Nested(inner) => flatten_and_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// The identifier an expression names, if it names one. Hands back the `Ident`
/// rather than its written value so callers can weigh the quote style with
/// `ident_is_timestamp` — an unquoted `TIMESTAMP` is the canonical column,
/// a quoted `"Timestamp"` is a different one (#4217).
fn sql_ident(expr: &SqlAstExpr) -> Option<&Ident> {
    match expr {
        SqlAstExpr::Identifier(ident) => Some(ident),
        SqlAstExpr::CompoundIdentifier(parts) => parts.last(),
        SqlAstExpr::Nested(inner) => sql_ident(inner),
        _ => None,
    }
}

fn sql_string_literal(expr: &SqlAstExpr) -> Option<String> {
    match expr {
        SqlAstExpr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        },
        SqlAstExpr::Nested(inner) => sql_string_literal(inner),
        _ => None,
    }
}

/// The exact schema column DataFusion's default SQL planner binds this
/// identifier to: unquoted names are ASCII-lowercased and quoted names retain
/// their written case. Group-count aggregates use exact column keys, so the
/// browse hints must carry this resolved name rather than the parser spelling.
fn normalized_sql_ident(ident: &Ident) -> String {
    IdentNormalizer::default().normalize(ident.clone())
}

/// A pure time-range conjunct: `<event time> <op> <anything>` (or flipped) for
/// a range operator. The value side is deliberately loose — casts, literals,
/// and function calls all count; the point is only that the term constrains
/// no dimension column.
fn sql_expr_is_time_range(expr: &SqlAstExpr, time_field: &str) -> bool {
    use datafusion::sql::sqlparser::ast::BinaryOperator as Op;
    let SqlAstExpr::BinaryOp { left, op, right } = expr else {
        return false;
    };
    if !matches!(op, Op::Gt | Op::GtEq | Op::Lt | Op::LtEq) {
        return false;
    }
    [left, right]
        .iter()
        .any(|side| sql_ident(side).is_some_and(|i| ident_is_field(i, time_field)))
}

/// One dimensional term: `col = 'v'`, `col <> 'v'`, `col IN (…)`,
/// `col NOT IN (…)` — string literals only (the group-count battery covers
/// string dimensions). Returns `(column, values, negated)`.
fn sql_expr_dimensional_term(
    expr: &SqlAstExpr,
    time_field: &str,
) -> Option<(String, Vec<String>, bool)> {
    use datafusion::sql::sqlparser::ast::BinaryOperator as Op;
    match expr {
        SqlAstExpr::BinaryOp { left, op, right } if matches!(op, Op::Eq | Op::NotEq) => {
            let (col, value) = match (sql_ident(left), sql_string_literal(right)) {
                (Some(c), Some(v)) => (c, v),
                _ => (sql_ident(right)?, sql_string_literal(left)?),
            };
            if ident_is_field(col, time_field) {
                return None;
            }
            Some((
                normalized_sql_ident(col),
                vec![value],
                matches!(op, Op::NotEq),
            ))
        }
        SqlAstExpr::InList {
            expr,
            list,
            negated,
        } => {
            let col = sql_ident(expr)?;
            if ident_is_field(col, time_field) {
                return None;
            }
            let values: Option<Vec<String>> = list.iter().map(sql_string_literal).collect();
            Some((normalized_sql_ident(col), values?, *negated))
        }
        SqlAstExpr::Nested(inner) => sql_expr_dimensional_term(inner, time_field),
        _ => None,
    }
}

fn detect_ordered_residual_browse(sql: &str, time_field: &str) -> Option<ResidualBrowseShape> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let [Statement::Query(query)] = &stmts[..] else {
        return None;
    };
    // Ordered by the table's event time (sole sort key) with an explicit LIMIT.
    let order_by = query.order_by.as_ref()?;
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return None;
    };
    let [lead] = &exprs[..] else {
        return None;
    };
    if !sql_order_expr_is_field(&lead.expr, time_field) || !query_has_explicit_limit(query) {
        return None;
    }
    dim_browse_shape(query, time_field)
}

/// The same dimensional shape WITHOUT requiring an `ORDER BY`: a plain
/// `WHERE <col> = 'v' [AND <time range>] LIMIT n` browse. The distribution gate
/// needs this because its question is how much the scan must sift, which does
/// not depend on how the rows come back ordered.
fn detect_dim_browse(sql: &str, time_field: &str) -> Option<ResidualBrowseShape> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let [Statement::Query(query)] = &stmts[..] else {
        return None;
    };
    if !query_has_explicit_limit(query) {
        return None;
    }
    dim_browse_shape(query, time_field)
}

/// The single dimensional term of a filtered browse: table, column, values and
/// whether it is negated. Conservative by construction -- pure time ranges plus
/// EXACTLY one dimensional term; an OR, a LIKE, a function or a second
/// dimension all disqualify.
fn dim_browse_shape(
    query: &datafusion::sql::sqlparser::ast::Query,
    time_field: &str,
) -> Option<ResidualBrowseShape> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || select.distinct.is_some()
        || select.having.is_some()
        || !matches!(select.group_by,
            datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(ref e, _) if e.is_empty())
    {
        return None;
    }
    if select
        .projection
        .iter()
        .filter_map(select_item_expr)
        .any(sql_expr_looks_aggregate)
    {
        return None;
    }
    let TableFactor::Table { name, .. } = &select.from[0].relation else {
        return None;
    };
    let table = object_name_tail(&name.to_string())?.to_string();
    // WHERE: pure time ranges + exactly one dimensional term; anything else
    // (OR, LIKE, functions, a second dimension) disqualifies — conservative.
    let selection = select.selection.as_ref()?;
    let mut conjuncts = Vec::new();
    flatten_and_conjuncts(selection, &mut conjuncts);
    let mut dim: Option<(String, Vec<String>, bool)> = None;
    for conjunct in conjuncts {
        if sql_expr_is_time_range(conjunct, time_field) {
            continue;
        }
        let term = sql_expr_dimensional_term(conjunct, time_field)?;
        if dim.is_some() {
            return None;
        }
        dim = Some(term);
    }
    let (column, values, negated) = dim?;
    Some(ResidualBrowseShape {
        table,
        column,
        values,
        negated,
    })
}

/// Rows ceiling for the inline-execution priority lane
/// (`SIGLAKE_QUERY_INLINE_EXEC_MAX_ROWS`, default 200000; `0` disables —
/// everything routes through the exec pool as before).
fn inline_exec_max_rows() -> usize {
    std::env::var("SIGLAKE_QUERY_INLINE_EXEC_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200_000)
}

/// Bytes ceiling for the inline lane (`SIGLAKE_QUERY_INLINE_EXEC_MAX_BYTES`,
/// default 64 MiB) — the decode-cost bound for inexact estimates.
fn inline_exec_max_bytes() -> u64 {
    std::env::var("SIGLAKE_QUERY_INLINE_EXEC_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(64 * 1024 * 1024)
}

/// The inline-lane eligibility predicate: a POSITIVE estimate within both
/// caps. Zero `estimated_rows_processed` means the estimator punted (ordered
/// browses) — never inline those.
fn inline_exec_eligible(c: &CostReport) -> bool {
    let max_rows = inline_exec_max_rows();
    max_rows > 0
        && c.estimated_rows_processed > 0
        && c.estimated_rows_processed <= max_rows as u64
        && c.estimated_bytes_scanned <= inline_exec_max_bytes()
}

/// Rows a browse must SIFT to fill its LIMIT, from the exact group-count
/// footers: `limit / matching_fraction`, capped at the table.
///
/// A small LIMIT is not a small scan, and conflating the two is what the
/// distribution gate got wrong. `WHERE region='us-east-2' LIMIT 100` matches
/// 12.5% of the corpus, early-stops after roughly 800 rows, and is 15ms on one
/// node -- distributing it would add coordination for nothing. `WHERE
/// region='probe' LIMIT 100` matches 75 rows in 394M and must sift the WHOLE
/// table just to discover it cannot fill the limit: measured 54.7s and a
/// breaker trip on a single node. Both have `LIMIT 100`, so a gate keyed on the
/// limit sends both to the same place.
///
/// `None` whenever the answer is not known exactly (no group-count coverage,
/// not a single-dimension browse) -- the caller then keeps today's behaviour.
async fn browse_expected_scan_rows(
    ice: &siglake_storage::iceberg::IcebergContext,
    sql: &str,
    limit: usize,
) -> Option<u64> {
    // The distribution gate asks how much a browse must sift, which turns on
    // the same event-time field: a `ts` range bound is a window, not a
    // dimensional term (#6020).
    let time_field = resolve_query_event_time_field(ice, sql).await;
    let shape = detect_dim_browse(sql, time_field.as_deref().unwrap_or(CANONICAL_EVENT_TIME))?;
    let rows = ice
        .grouped_counts_with_summary(&shape.table, &shape.column, None, None)
        .await
        .ok()??;
    let total: u64 = rows.iter().map(|(_, c)| c).sum();
    if total == 0 {
        return None;
    }
    let matching: u64 = rows
        .iter()
        .filter(|(value, _)| value.is_some_and(|v| shape.values.iter().any(|want| want == v)))
        .map(|(_, c)| c)
        .sum();
    let matching = if shape.negated {
        total.saturating_sub(matching)
    } else {
        matching
    };
    Some(expected_sift_rows(total, matching, limit))
}

/// `limit / (matching/total)`, capped at the table. Pure so it can be tested
/// directly -- an earlier version of these tests re-implemented this formula in
/// the test body, which proves only that I can copy an expression twice.
fn expected_sift_rows(total: u64, matching: u64, limit: usize) -> u64 {
    // Nothing matches: the browse reads the entire table and returns nothing.
    // That is the worst case for a single node and the best case for fan-out.
    if matching == 0 {
        return total;
    }
    let frac = matching as f64 / total as f64;
    ((limit as f64 / frac).min(total as f64)) as u64
}

/// Sift threshold above which a small-LIMIT browse is worth distributing
/// (`SIGLAKE_DIST_BROWSE_MIN_SCAN_ROWS`). **Default 0 = OFF.**
///
/// OFF BY DEFAULT, DELIBERATELY. The policy is sound and measured once at a
/// 291x win (a `region='probe'` browse over 2.016B rows: 60s timeout on one
/// node, 206ms across five). But validating it at 1TB found that the shape it
/// routes -- a low-selectivity browse -- does not reliably survive the
/// distributed path:
///
///   depth 13, converged, 2,016,590,762 rows
///     1 node : 413 after 49.6s   <- bounded: the mid-flight breaker fires
///     5 nodes: no response at 120s, peers received ZERO shard requests
///
/// The coordinator scans the whole timestamp column before dispatch (measured
/// 17.2s and 10.4s in its own logs) and the request never reaches a worker. So
/// turning this on trades a BOUNDED failure for an UNBOUNDED one, which is
/// worse even though the bounded one is also a failure.
///
/// Set it to a row count (5,000,000 is a reasonable starting point) once the
/// distributed browse path dispatches reliably. The gate itself, and its tests,
/// are kept precisely so that flipping this default is the only change needed.
fn dist_browse_min_scan_rows() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SIGLAKE_DIST_BROWSE_MIN_SCAN_ROWS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    })
}

/// Minimum matching fraction for the ordered-residual hint
/// (`SIGLAKE_ORDERED_RESIDUAL_MIN_FRAC`, default 0.01 — expected drain
/// ≈ LIMIT/frac ≈ 10K rows at LIMIT 100). `0` disables the policy.
fn ordered_residual_min_frac() -> Option<f64> {
    let frac = std::env::var("SIGLAKE_ORDERED_RESIDUAL_MIN_FRAC")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(0.01);
    (frac > 0.0).then_some(frac.min(1.0))
}

/// Measure the shape's selectivity from the group-count side aggregates.
/// `false` (stay on the TopK path) when the column has no aggregate coverage
/// or the matching fraction is below the threshold. The battery is
/// cardinality-capped: a capped column undercounts `matching`, which is
/// conservative for equality (stays TopK) — and a negation of missing values
/// is ~the whole table either way.
async fn residual_browse_low_selectivity(
    ice: &siglake_storage::iceberg::IcebergContext,
    shape: &ResidualBrowseShape,
) -> bool {
    let Some(min_frac) = ordered_residual_min_frac() else {
        return false;
    };
    residual_browse_low_selectivity_at(ice, shape, min_frac).await
}

async fn residual_browse_low_selectivity_at(
    ice: &siglake_storage::iceberg::IcebergContext,
    shape: &ResidualBrowseShape,
    min_frac: f64,
) -> bool {
    let rows = match ice
        .grouped_counts_with_summary(&shape.table, &shape.column, None, None)
        .await
    {
        Ok(Some(rows)) => rows,
        _ => return false,
    };
    let total: u64 = rows.iter().map(|(_, c)| c).sum();
    if total == 0 {
        return false;
    }
    let matching: u64 = rows
        .iter()
        .filter(|(value, _)| value.is_some_and(|v| shape.values.iter().any(|want| want == v)))
        .map(|(_, c)| c)
        .sum();
    let matching = if shape.negated {
        total.saturating_sub(matching)
    } else {
        matching
    };
    let frac = matching as f64 / total as f64;
    let allow = frac >= min_frac;
    tracing::debug!(
        table = %shape.table,
        column = %shape.column,
        frac,
        allow,
        "ordered-residual selectivity"
    );
    if allow {
        metrics::counter!("siglake_query_ordered_residual_hint_total").increment(1);
    }
    allow
}

/// The scan-order hint for a query ordered by `time_field`, the event-time
/// field of the table it reads.
///
/// The hint carries the field as well as the direction: storage accepts it
/// only after proving that field IS the scanned table's identity sort lead
/// (#6020), so naming it here is what lets a `ts`-mapped index early-stop
/// instead of falling to a blocking sort.
fn preferred_scan_order_from_query(
    query: &SqlQuery,
    time_field: &str,
) -> Option<siglake_storage::PreferredScanOrder> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Table { .. } = &select.from[0].relation else {
        return None;
    };
    let order_by = query.order_by.as_ref()?;
    let OrderByKind::Expressions(exprs) = &order_by.kind else {
        return None;
    };
    let lead = exprs.first()?;
    if !sql_order_expr_is_field(&lead.expr, time_field) {
        return None;
    }
    // A client's own `ORDER BY timestamp` over `SELECT <col> AS timestamp`
    // sorts the aliased column, not the source one (#4090). Keeping the hint
    // would ask the scan to produce an ordering nothing in the plan consumes.
    if sql_order_expr_is_bare_field(&lead.expr, time_field)
        && projection_shadows_order_field(select, time_field)
    {
        return None;
    }
    Some(siglake_storage::PreferredScanOrder::new(
        time_field,
        lead.options.asc == Some(false),
    ))
}

fn preferred_scan_order_from_sql(
    query: &str,
    time_field: &str,
) -> Option<siglake_storage::PreferredScanOrder> {
    let dialect = GenericDialect {};
    let mut stmts = Parser::parse_sql(&dialect, query).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    match stmts.pop()? {
        Statement::Query(sql_query) => preferred_scan_order_from_query(&sql_query, time_field),
        // #91: EXPLAIN inherits the inner query's hint.
        Statement::Explain { statement, .. } => match *statement {
            Statement::Query(inner) => preferred_scan_order_from_query(&inner, time_field),
            _ => None,
        },
        _ => None,
    }
}

/// The same hint, for a caller that holds the tenant context and only the SQL
/// text: resolves the event-time field of the single table the query reads
/// before deriving the order. Used by the paths that plan a query the rewrite
/// has already been through (`EXPLAIN`, the shard worker), so they hint the
/// same field the coordinator did.
async fn preferred_scan_order_for_sql(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
) -> Option<siglake_storage::PreferredScanOrder> {
    let field = resolve_query_event_time_field(ice, query).await;
    preferred_scan_order_from_sql(query, field.as_deref().unwrap_or(CANONICAL_EVENT_TIME))
}

fn sql_order_expr_is_field(expr: &SqlAstExpr, field: &str) -> bool {
    match expr {
        SqlAstExpr::Identifier(ident) => ident_is_field(ident, field),
        SqlAstExpr::CompoundIdentifier(idents) => {
            idents.last().is_some_and(|i| ident_is_field(i, field))
        }
        SqlAstExpr::Nested(expr) => sql_order_expr_is_field(expr, field),
        _ => false,
    }
}

/// Does this identifier name the column `field`, under SQL's own case rules?
/// An unquoted identifier is case-normalized to lower case (`TIMESTAMP` and
/// `timestamp` are the same column, as DataFusion's ident normalizer resolves
/// them, and an unquoted `Ts` can only bind a column named `ts`); a quoted one
/// keeps exactly the case it was written with.
///
/// The distinction is not academic here: a managed mapping may declare both
/// `timestamp` and `Timestamp`, because `IndexConfig::validate` preserves case
/// and rejects duplicates by exact name (`siglake_core::index_config`). Reading
/// `"Timestamp"` as the canonical column let `SELECT "Timestamp" AS timestamp`
/// pass the #4090 shadowing guard, and the implicit browse then came back
/// ordered by the text column (#4214). The same rule decides a mapping's own
/// event-time field, which may be spelled in any case (#6020).
fn ident_is_field(ident: &Ident, field: &str) -> bool {
    match ident.quote_style {
        Some(_) => ident.value == field,
        None => ident.value.to_ascii_lowercase() == field,
    }
}

/// The identifier the implicit newest-first rewrite injects for `field`.
///
/// The canonical column is written bare, exactly as the rewrite always wrote
/// it. Any other event-time field is QUOTED: mapping field names keep their
/// case and may collide with SQL keywords, and an unquoted injection would
/// case-fold to a different column or fail to parse (#6020).
fn order_by_ident(field: &str) -> Ident {
    if field == siglake_storage::CANONICAL_EVENT_TIME_FIELD {
        Ident::new(field)
    } else {
        Ident::with_quote('"', field)
    }
}

/// Names of every aggregate DataFusion registers by default (plus aliases).
/// Built from the engine's own registry so new aggregate functions are
/// covered automatically — a hardcoded list here once missed
/// `approx_distinct` et al., and injecting `ORDER BY timestamp` into an
/// implicit-aggregate SELECT is a query-breaking rewrite.
static DEFAULT_AGGREGATE_NAMES: std::sync::LazyLock<std::collections::HashSet<String>> =
    std::sync::LazyLock::new(|| {
        datafusion::functions_aggregate::all_default_aggregate_functions()
            .iter()
            .flat_map(|f| {
                std::iter::once(f.name().to_string())
                    .chain(f.aliases().iter().map(|a| a.to_string()))
            })
            .map(|name| name.to_ascii_lowercase())
            .collect()
    });

/// True when the expression (recursively) contains an aggregate function
/// call or any windowed (`OVER`) function — either disqualifies the
/// newest-first rewrite. Conservative: an unknown function name does NOT
/// disqualify (scalar UDFs like `attr_get`/`match_terms` are legitimate in
/// bare SELECTs); only names the engine itself registers as aggregates do.
fn sql_expr_looks_aggregate(expr: &SqlAstExpr) -> bool {
    use core::ops::ControlFlow;
    use datafusion::sql::sqlparser::ast::visit_expressions;
    visit_expressions(expr, |e: &SqlAstExpr| {
        if let SqlAstExpr::Function(func) = e {
            if func.over.is_some() {
                return ControlFlow::Break(());
            }
            let name = func
                .name
                .0
                .last()
                .map(|part| part.to_string().to_ascii_lowercase())
                .unwrap_or_default();
            if DEFAULT_AGGREGATE_NAMES.contains(&name) {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .is_break()
}

/// Does any query in these statements have a shape the result cache is for?
///
/// The cacheable shapes are filtered scans (`WHERE`), aggregates (`GROUP BY`)
/// and ordered `LIMIT` browses (match_all / deep pagination — a browse result
/// is as snapshot-pure as an aggregate, and its warm cost was the newest
/// file's S3 tail fetch + decode on every repeat). Bare unordered unfiltered
/// scans stay uncached: unbounded, and cheap enough not to want a copy of.
///
/// READ OFF THE AST, NOT THE TEXT. This test used to be
/// `lowered.contains(" where ")` / `" group by "` / `" order by "`+`" limit "`
/// on the raw SQL, which is the same whitespace fragility #1444 removed from
/// the volatility check one gate below. A client that pretty-prints —
/// `SELECT count(*)\nFROM events\nWHERE host='h'` — matched no needle and
/// never cached, while the byte-identical single-line query did; the failure
/// was a silent halving of the cache's reach for every such client, indexed on
/// nothing but the caller's formatter. It also read `ORDER BY` out of a window
/// function's `OVER (...)` and `WHERE` out of a column alias.
///
/// ANY query node, not just the outermost one, so that a filter pushed into a
/// derived table or a CTE still counts — that is what the substring scan did,
/// and a query with a predicate anywhere is not the bare scan this gate
/// excludes. `ORDER BY` and `LIMIT` must sit on the SAME query to count as a
/// browse.
fn statements_have_cacheable_shape(stmts: &[Statement]) -> bool {
    struct ShapeWalker;
    impl Visitor for ShapeWalker {
        type Break = ();
        fn pre_visit_query(&mut self, query: &SqlQuery) -> core::ops::ControlFlow<()> {
            if query_shape_is_cacheable(query) {
                core::ops::ControlFlow::Break(())
            } else {
                core::ops::ControlFlow::Continue(())
            }
        }
    }
    stmts
        .iter()
        .any(|stmt| stmt.visit(&mut ShapeWalker).is_break())
}

fn query_shape_is_cacheable(query: &SqlQuery) -> bool {
    if query.order_by.is_some() && query_has_explicit_limit(query) {
        return true;
    }
    set_expr_shape_is_cacheable(&query.body)
}

/// The SELECTs reachable without crossing into a nested `Query` node — those
/// are visited in their own right by [`statements_have_cacheable_shape`].
fn set_expr_shape_is_cacheable(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => select.selection.is_some() || select_has_group_by(select),
        SetExpr::Query(query) => query_shape_is_cacheable(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_shape_is_cacheable(left) || set_expr_shape_is_cacheable(right)
        }
        _ => false,
    }
}

fn select_has_group_by(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(exprs, modifiers) => !exprs.is_empty() || !modifiers.is_empty(),
    }
}

fn query_has_explicit_limit(query: &SqlQuery) -> bool {
    match &query.limit_clause {
        Some(LimitClause::LimitOffset { limit, .. }) => limit.is_some(),
        Some(LimitClause::OffsetCommaLimit { .. }) => true,
        None => false,
    }
}

/// The literal rows needed by `LIMIT n [OFFSET m]`, when both are plain
/// numbers. Drives the single-partition coalesce for ordered browses (see
/// `OrderedScanLimit`); non-literal or absent limits simply don't set the hint.
fn explicit_limit_value(query: &SqlQuery) -> Option<usize> {
    let literal = |expr: &SqlAstExpr| match expr {
        SqlAstExpr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => n.parse::<usize>().ok(),
            _ => None,
        },
        _ => None,
    };
    let (limit, offset) = match &query.limit_clause {
        // `LIMIT n BY expr` is n rows PER GROUP, so `n` is not the row count
        // the query needs and a hint built from it would be wrong in the
        // direction that truncates. Nothing in siglake's SQL surface documents
        // the form; a dialect that parses it gets no hint.
        Some(LimitClause::LimitOffset {
            limit: Some(limit),
            offset,
            limit_by,
        }) if limit_by.is_empty() => (
            literal(limit)?,
            match offset {
                Some(offset) => literal(&offset.value)?,
                None => 0,
            },
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            (literal(limit)?, literal(offset)?)
        }
        _ => return None,
    };
    Some(limit.saturating_add(offset))
}

/// The literal `LIMIT n [OFFSET m]` of a query whose limit clips its SCAN row
/// for row, so the scan only ever owes `n + m` rows. Rides into the session as
/// [`siglake_storage::ClippedScanLimit`], which is what lets the provider
/// decline a whole-file inverted index for a shape that reads a sliver of the
/// first file (#4375; the ordered form of the same decline travels as
/// `OrderedScanLimit`).
///
/// `None` unless every operator between the scan and the limit passes rows
/// through unchanged. [`default_order_target_table`] already spells that shape
/// out — one plain table, no CTE/join/GROUP BY/DISTINCT/aggregate/window — for
/// the newest-first rewrite, and the two want the same thing for the same
/// reason. It also requires no ORDER BY, which is what keeps this disjoint
/// from the ordered path rather than double-counting it.
///
/// Subqueries disqualify the whole statement: the hint is per SESSION, and an
/// inner scan feeding an aggregate is not clipped by the outer limit at all.
/// `SELECT count(*) … LIMIT 100` is the shape that would otherwise lose its
/// index for no reason, and it is excluded by the aggregate test above.
fn clipping_scan_limit(query: &SqlQuery) -> Option<usize> {
    if query_contains_subquery(query) {
        return None;
    }
    // Clipping has nothing to do with the order contract, so the shape test
    // judges its alias guard against the canonical column, exactly as it did
    // before per-index event times existed.
    default_order_target_table(query, CANONICAL_EVENT_TIME)?;
    explicit_limit_value(query)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_result_cache(
    ice: &siglake_storage::iceberg::IcebergContext,
    query: &str,
    dry_run: bool,
    format: QueryFormat,
    resolved: &ResolvedLimits,
    buffer_delta: BufferDeltaState,
    buffer_proof: BufferProof,
    scope: ResultCacheScope,
    deadline: tokio::time::Instant,
) -> Result<SqlResultCacheDecision, ApiError> {
    if format != QueryFormat::Records || dry_run {
        return Ok(SqlResultCacheDecision::Skip);
    }
    // Measurement knob: with result caches off a repeated query re-executes
    // instead of replaying a memoized answer. See
    // `siglake_storage::iceberg::result_caches_enabled`.
    if !siglake_storage::iceberg::result_caches_enabled() {
        return Ok(SqlResultCacheDecision::Skip);
    }
    // Correctness gate: a snapshot-keyed entry is only valid while the WAL
    // buffer is KNOWN to contribute NOTHING to this query's tables — buffered
    // rows change results WITHOUT a snapshot change, so with rows in flight
    // both serving a hit (misses newer buffered rows) and inserting (freezes
    // the in-flight set) would be stale. A delta whose load was DECLINED or
    // FAILED cannot prove otherwise. The drained steady state dominates, so the
    // cache still pays; while draining, every request recomputes fresh.
    if buffer_delta.record_result_cache_skip() {
        return Ok(SqlResultCacheDecision::Skip);
    }
    // ONE parse for all three classifications below. They used to run three:
    // a substring shape filter first (cheap, and wrong on any client that
    // formats SQL across lines), then a parse for purity, then another for the
    // table list. The shape test reads off the AST now, so the order is kept
    // for its metric semantics only — see the skip counter below.
    //
    // An unparseable statement is not classifiable and skips here, above the
    // shape gate — nothing can be said about the shape of a statement that did
    // not parse. It gets its own outcome (#1481) and is expected at ~zero: this
    // parse is `sqlparser`'s, the planner's is DataFusion's, so a rising
    // `skip_unparseable` is either a client sending SQL that is about to 400 or
    // the two dialects having drifted apart, which costs cache coverage on
    // queries that succeed. Uncacheable SHAPES stay uncounted below: those are
    // expected traffic, not a signal.
    let Some(stmts) = parse_for_cache_classification(query) else {
        metrics::counter!(
            "siglake_query_sql_result_cache_requests_total",
            "outcome" => "skip_unparseable"
        )
        .increment(1);
        return Ok(SqlResultCacheDecision::Skip);
    };
    // Cacheable shapes: filtered scans, aggregates (GROUP BY), and ordered
    // LIMIT browses. Bare unordered unfiltered scans stay uncached
    // (unbounded/cheap). See `statements_have_cacheable_shape`.
    if !statements_have_cacheable_shape(&stmts) {
        return Ok(SqlResultCacheDecision::Skip);
    }
    // Not a pure function of (table, snapshot, query): see
    // `statements_snapshot_impurity`. Caching one freezes its answer for as long
    // as the snapshot stands still.
    //
    // ONE outcome per reason (#1481). `skip_time_dependent` keeps its meaning —
    // a registered function the planner declares STABLE or VOLATILE, i.e.
    // expected traffic — and an unclassifiable name gets `skip_unclassifiable`,
    // the series that says this conservatism is costing a deployment its cache.
    // The label values stay literal at the call site so `check-chart.py` can
    // read the vocabulary out of the source (as with `record_result_cache_skip`).
    //
    // Below the shape filter, not above it: a shape that cannot be cached
    // anyway must not be counted as a time-dependent skip.
    if let Some(reason) = statements_snapshot_impurity(&stmts) {
        match &reason {
            CacheImpurity::NonImmutableFunction(_) => {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "skip_time_dependent"
                )
                .increment(1);
            }
            CacheImpurity::UnclassifiableFunction(_) => {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "skip_unclassifiable"
                )
                .increment(1);
            }
        }
        tracing::debug!(
            function = reason.function(),
            reason = ?reason,
            "sql result cache skipped: function is not immutable"
        );
        return Ok(SqlResultCacheDecision::Skip);
    }
    let Some(tables) = referenced_tables_in(&stmts) else {
        return Ok(SqlResultCacheDecision::Skip);
    };
    // Single-table only: the key carries ONE table generation.
    let [table] = &tables[..] else {
        return Ok(SqlResultCacheDecision::Skip);
    };
    // Snapshot AND schema id, from one metadata generation (#2494). Two
    // independent reads could straddle a refresh and produce a key that
    // describes no generation at all; see `TableGeneration`.
    let generation = ice
        .current_table_generation(table)
        .await
        .map_err(ApiError::internal)?;
    let Some(snapshot_id) = generation.snapshot_id else {
        return Ok(SqlResultCacheDecision::Skip);
    };
    let key = result_cache_key(
        &ice.namespace().to_string(),
        snapshot_id,
        generation.schema_id,
        query,
        resolved,
        scope,
    );
    Ok(probe_result_cache(key, resolved, deadline, buffer_proof).await)
}

/// The lookup and single-flight half of [`prepare_result_cache`], over a key
/// whose ELIGIBILITY the caller has already established.
///
/// Split out for the Jaeger name-list routes (#2268), which reach the same
/// cache through [`prepare_name_list_cache`]: everything above this point is
/// the SQL handler's own eligibility — its format, its `dry_run`, its parsed
/// statement, its WAL delta — and none of it describes a route that serves two
/// fixed `SELECT DISTINCT`s. What is shared is the part that must not be
/// written twice: one process-wide store, one byte and entry budget, one
/// single-flight map, and one `outcome` counter.
async fn probe_result_cache(
    key: String,
    resolved: &ResolvedLimits,
    deadline: tokio::time::Instant,
    buffer_proof: BufferProof,
) -> SqlResultCacheDecision {
    loop {
        let probe = {
            let mut cache = sql_result_cache().lock().await;
            if let Some(hit) = cache.get(&key) {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "hit"
                )
                .increment(1);
                return SqlResultCacheDecision::Hit(hit);
            }
            let mut inflight = sql_result_inflight()
                .lock()
                .expect("sql result inflight mutex");
            if let Some(wait) = inflight.get(&key) {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "wait"
                )
                .increment(1);
                // `notify_waiters` does not save a permit. Enable the owned
                // observation while protected by the same mutex a leader's
                // Drop needs in order to remove this marker.
                CacheProbe::Wait(registered_completion(Arc::clone(wait)))
            } else {
                let notify = Arc::new(Notify::new());
                inflight.insert(key.clone(), notify.clone());
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "miss"
                )
                .increment(1);
                CacheProbe::Leader(notify)
            }
        };
        match probe {
            CacheProbe::Leader(notify) => {
                return SqlResultCacheDecision::Leader(SqlResultCacheCtx {
                    key,
                    notify,
                    buffer_proof,
                });
            }
            CacheProbe::Wait(completion) => {
                // BOUNDED. An unbounded `notified()` turns any leaked marker
                // into a permanent hang for that query shape; with a bound the
                // worst case is one wasted wait and then a normal miss. Defence
                // in depth behind the Drop guard, not a substitute for it.
                //
                // The bound is the SMALLER of that cap and what is left of THIS
                // request's wall-clock budget. The flat cap alone parked a
                // two-second request for ten seconds behind somebody else's
                // leader — a wait the caller had already said it would not pay.
                let wait = cache_wait_budget(resolved, deadline);
                if wait.is_zero() {
                    // Nothing left to wait with. Hand the decision back rather
                    // than burn a timer; the caller's next deadline check turns
                    // this into the 504 it has already earned.
                    return SqlResultCacheDecision::Skip;
                }
                if tokio::time::timeout(wait, completion).await.is_err() {
                    metrics::counter!(
                        "siglake_query_sql_result_cache_requests_total",
                        "outcome" => "wait_timeout"
                    )
                    .increment(1);
                    return SqlResultCacheDecision::Skip;
                }
            }
        }
    }
}

/// The Jaeger name-list routes' way into this cache (#2268).
///
/// `/api/v1/jaeger/{index}/api/services` and `.../operations` plan and collect
/// directly — they never enter the SQL handler — so the repeated UI poll paid
/// the whole unpredicated `SELECT DISTINCT` every time (measured 2026-09-08 on
/// 2,000,000 spans: services 53.7–66.1 ms, operations 89.8–105.5 ms on every
/// repeat; through this cache the same repeats are 1.3–1.7 ms). The
/// measurement is the `#[ignore]`d
/// `what_repeated_jaeger_name_polls_cost_on_a_large_snapshot` in
/// `tests/jaeger_render_cost_measurement.rs`, which prints the cache outcome
/// of every poll.
///
/// The SQL handler's eligibility gates above do not apply, and the caller
/// discharges what they stand for:
///
///   - **Shape and purity.** Two fixed `SELECT DISTINCT … ORDER BY`s over one
///     table, built by the route rather than by a client. Nothing in them is
///     time-dependent, so there is no statement to parse and classify.
///   - **The WAL buffer.** The vacuous [`BufferProof`], and it is vacuous by
///     construction rather than by luck: `TraceQueryContext` registers the
///     Iceberg provider ALONE and never unions the buffer the way
///     [`override_indexes_with_wal_buffer`] does for SQL. A name list is
///     already committed-data-only, so an entry keyed by the snapshot cannot
///     be made stale by rows in flight.
///   - **Identity.** The key is the caller's, because the query text cannot be
///     it: every index is registered under the same `traces` alias, so two
///     indexes issue byte-identical SQL. See `jaeger_routes::name_cache_key`.
///
/// `SIGLAKE_QUERY_RESULT_CACHE` still governs, and a name list past what one
/// entry may retain ([`CacheEligibility::NAME_LIST`], 512 KiB of arena) is
/// still refused entry rather than truncated to fit.
pub(crate) async fn prepare_name_list_cache(
    key: String,
    resolved: &ResolvedLimits,
    deadline: tokio::time::Instant,
) -> SqlResultCacheDecision {
    if !siglake_storage::iceberg::result_caches_enabled() {
        return SqlResultCacheDecision::Skip;
    }
    probe_result_cache(key, resolved, deadline, BufferProof::default()).await
}

/// Store this leader's answer, or nothing, and release its single-flight
/// marker.
///
/// Takes the body by `Arc` rather than by reference so the caller's response
/// and the stored entry are ONE allocation (#2304). Every producer already
/// holds its body until the response is serialized, so a `&` here meant a deep
/// copy of the `Value` tree per insert on top of the encoding this used to
/// take, and several callers then cloned it a second time to hand it to
/// `Json`. A stored body is never mutated afterwards, which is what makes the
/// sharing sound.
///
/// `eligibility` is the CALLER's, not this function's (#2302): what may be
/// stored depends on the shape the caller produces and the bounds it already
/// enforces upstream. See [`CacheEligibility`].
pub(crate) async fn finish_result_cache(
    cache_ctx: SqlResultCacheCtx,
    eligibility: CacheEligibility,
    body: Option<CachedBody>,
) {
    // Sized BEFORE the lock and before the WAL re-proof: a body that cannot be
    // stored anyway must not pay a directory listing, and the listing must not
    // be paid inside the process-wide cache mutex.
    //
    // Sized, not encoded. Until #2304 this gate serialized the body to measure
    // it and dropped the bytes on the next line — the whole cost of an encoding
    // for a number that then described the wrong object by 29–35x. What is
    // measured now is what will be RETAINED, by walking the body.
    let insertable = body.and_then(|body| {
        eligibility
            .admits(&cache_ctx.key, &body)
            .map(|bytes| CachedSqlResult { bytes, body })
    });
    // #1562: the key asserts the WAL contributed nothing to this body, but that
    // was proven at the top of the request and the scan listed the directory
    // again. Re-take the proof here; a segment that sealed in the interval is in
    // the body and makes the entry a lie about its own key.
    let insertable = match insertable {
        Some(entry) if cache_ctx.buffer_proof.still_holds() => Some(entry),
        Some(_) => {
            metrics::counter!(
                "siglake_query_sql_result_cache_requests_total",
                "outcome" => "skip_buffer_moved"
            )
            .increment(1);
            None
        }
        None => None,
    };
    let mut cache = sql_result_cache().lock().await;
    if let Some(entry) = insertable {
        cache.insert(cache_ctx.key.clone(), entry);
        metrics::counter!(
            "siglake_query_sql_result_cache_requests_total",
            "outcome" => "insert"
        )
        .increment(1);
    }
    // Removal and wake happen in `SqlResultCacheCtx::drop`, which runs here when
    // `cache_ctx` falls out of scope AND on every path that used to skip this
    // call. Doing it by hand is what left markers behind.
    drop(cache);
    drop(cache_ctx);
}

#[cfg(test)]
mod result_cache_singleflight_tests {
    use super::*;
    use tokio::sync::Barrier;

    fn is_inflight(key: &str) -> bool {
        sql_result_inflight().lock().unwrap().contains_key(key)
    }

    /// A leader that goes away without calling `finish_result_cache` must not
    /// leave its marker behind.
    ///
    /// THE DEFECT. Cleanup was an explicit call at eleven sites, and the
    /// pre-flight byte rejection (`sql.rs`, "estimated bytes scanned exceeds
    /// the limit") returned without one. The marker stayed in the map forever
    /// and every later identical query parked on a `Notify` nobody would fire —
    /// permanent for that shape, cured only by a restart. The 2026-08-25 round
    /// counted 6,413 preflight_bytes refusals in under three hours.
    ///
    /// This is the general case, not that one path: a `?` return, a panic, or a
    /// client disconnect dropping the handler future all skip an explicit call,
    /// and the last runs no code at all. Only Drop covers them.
    #[test]
    fn a_leader_that_never_finishes_does_not_poison_its_key() {
        let notify = Arc::new(Notify::new());
        sql_result_inflight()
            .lock()
            .unwrap()
            .insert("poison-me".to_string(), notify.clone());
        assert!(is_inflight("poison-me"), "marker was not registered");

        {
            let _ctx = SqlResultCacheCtx {
                key: "poison-me".to_string(),
                notify,
                buffer_proof: BufferProof::default(),
            };
            // Falls out of scope WITHOUT `finish_result_cache` — the shape of
            // every path that used to leak.
        }

        assert!(
            !is_inflight("poison-me"),
            "the marker outlived its leader; every later identical query would \
             wait on a Notify nobody will fire"
        );
    }

    /// A completion after protected registration but before the wait future's
    /// first await must be retained. `Notify::notify_waiters` does not retain a
    /// permit, so constructing the future after this point loses the wake.
    #[tokio::test]
    async fn completion_before_wait_is_polled_is_observed() {
        let notify = Arc::new(Notify::new());
        sql_result_inflight()
            .lock()
            .unwrap()
            .insert("wake-me".to_string(), notify.clone());
        let ctx = SqlResultCacheCtx {
            key: "wake-me".to_string(),
            notify: notify.clone(),
            buffer_proof: BufferProof::default(),
        };

        let completion = {
            let inflight = sql_result_inflight().lock().unwrap();
            registered_completion(Arc::clone(inflight.get("wake-me").unwrap()))
        };
        let completed = Arc::new(Barrier::new(2));
        let leader_completed = Arc::clone(&completed);
        let leader = tokio::spawn(async move {
            drop(ctx);
            leader_completed.wait().await;
        });

        // The barrier proves the leader's Drop and notification happened before
        // `completion` is first awaited. No scheduler timing arranges the race.
        completed.wait().await;

        tokio::time::timeout(std::time::Duration::from_secs(5), completion)
            .await
            .expect("completion before the first await was lost");
        leader.await.expect("leader task panicked");
    }

    /// Finishing through one clone can be followed immediately by a new flight
    /// while the handler still holds the original clone. The old guard must not
    /// remove the new flight when the handler returns.
    #[tokio::test]
    async fn old_guard_does_not_remove_replacement_after_non_inserting_completion() {
        let key = "replacement-after-empty-completion";
        let old_notify = Arc::new(Notify::new());
        sql_result_inflight()
            .lock()
            .unwrap()
            .insert(key.to_string(), Arc::clone(&old_notify));
        let old_guard = SqlResultCacheCtx {
            key: key.to_string(),
            notify: old_notify,
            buffer_proof: BufferProof::default(),
        };

        let completion_observed = Arc::new(Barrier::new(2));
        let completed = Arc::clone(&completion_observed);
        let finishing_guard = old_guard.clone();
        let finishing = tokio::spawn(async move {
            finish_result_cache(finishing_guard, CacheEligibility::SQL, None).await;
            completed.wait().await;
        });
        completion_observed.wait().await;
        assert!(!is_inflight(key), "the completed flight was not removed");

        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let SqlResultCacheDecision::Leader(replacement) = probe_result_cache(
            key.to_string(),
            &resolved,
            far_future_deadline(),
            BufferProof::default(),
        )
        .await
        else {
            panic!("the completed key was not available to a replacement leader");
        };
        let replacement_notify = Arc::clone(&replacement.notify);

        // Park the stale handler guard until the replacement owns the key, then
        // release it into the precise ordering that used to delete the new
        // marker.
        let replacement_installed = Arc::new(Barrier::new(2));
        let installed = Arc::clone(&replacement_installed);
        let stale_drop = tokio::spawn(async move {
            installed.wait().await;
            drop(old_guard);
        });
        replacement_installed.wait().await;
        stale_drop.await.expect("stale-guard task panicked");
        finishing.await.expect("finishing task panicked");
        let inflight = sql_result_inflight().lock().unwrap();
        assert!(
            inflight
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, &replacement_notify)),
            "an old guard removed the replacement flight"
        );
        drop(inflight);
        drop(replacement);
        assert!(!is_inflight(key), "the replacement did not clean up");
    }

    /// Cancelling the handler future drops its leader guard, removes the marker,
    /// and wakes a follower that registered before the cancellation.
    #[tokio::test]
    async fn cancellation_cleans_up_owned_flight_and_wakes_waiter() {
        let key = "cancel-owned-flight";
        let notify = Arc::new(Notify::new());
        sql_result_inflight()
            .lock()
            .unwrap()
            .insert(key.to_string(), Arc::clone(&notify));
        let ctx = SqlResultCacheCtx {
            key: key.to_string(),
            notify,
            buffer_proof: BufferProof::default(),
        };
        let completion = {
            let inflight = sql_result_inflight().lock().unwrap();
            registered_completion(Arc::clone(inflight.get(key).unwrap()))
        };
        let parked = Arc::new(Barrier::new(2));
        let task_parked = Arc::clone(&parked);
        let handler = tokio::spawn(async move {
            task_parked.wait().await;
            std::future::pending::<()>().await;
            drop(ctx);
        });

        parked.wait().await;
        handler.abort();
        assert!(
            handler
                .await
                .expect_err("cancelled handler completed")
                .is_cancelled(),
            "handler failed for a reason other than cancellation"
        );
        assert!(!is_inflight(key), "cancellation left the marker behind");
        tokio::time::timeout(std::time::Duration::from_secs(5), completion)
            .await
            .expect("cancellation did not wake the registered follower");
    }
}

/// What an entry COSTS the store and what a hit COPIES (#2304).
///
/// Over a local [`SqlResultCache`], not the process-global one: these are
/// assertions about the store's own arithmetic, and the global store is shared
/// with every other test in this binary.
#[cfg(test)]
mod result_cache_accounting_tests {
    use super::*;

    /// A one-column `SELECT DISTINCT` list — the shape on which README,
    /// "Caches", records the measured 29–35x discrepancy.
    fn name_list(rows: usize) -> crate::format::RecordsResponse {
        crate::format::RecordsResponse {
            columns: vec!["service".to_string()],
            row_count: rows,
            rows: serde_json::Value::Array(
                (0..rows)
                    .map(|i| serde_json::json!({ "service": format!("service-{i:04}") }))
                    .collect(),
            ),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        }
    }

    fn entry(body: crate::format::RecordsResponse) -> CachedSqlResult {
        let body = CachedBody::Records(Arc::new(body));
        CachedSqlResult {
            bytes: body.retained_bytes(),
            body,
        }
    }

    /// THE UNDER-ACCOUNTING. The charge is the heap the entry holds, and on
    /// this shape that is an order of magnitude over the encoding the store
    /// used to bill — so the 4 MiB budget now bounds 4 MiB of heap rather than
    /// tens of MiB of it.
    #[test]
    fn an_entry_is_charged_what_it_retains_not_what_it_encodes() {
        let body = name_list(SQL_RESULT_CACHE_MAX_ROWS);
        let encoded = serde_json::to_vec(&body).unwrap().len();
        let mut cache = SqlResultCache::default();
        cache.insert("names".to_string(), entry(body));

        assert!(
            cache.retained_bytes() >= 20 * encoded,
            "a 128-row one-column list was measured at 29–35x its encoding; \
             this store charges {} B against {encoded} B encoded",
            cache.retained_bytes()
        );
        // Every row is one whole 11-slot `BTreeMap` node whatever it holds.
        let per_row = cache.retained_bytes() / SQL_RESULT_CACHE_MAX_ROWS;
        assert!(
            (600..1_200).contains(&per_row),
            "expected ~670 B retained per one-column row, got {per_row} B"
        );
    }

    /// THE DEEP COPY. `get` hands back the stored body itself, so what runs
    /// inside the process-wide mutex is a refcount bump.
    #[test]
    fn a_hit_shares_the_stored_body_rather_than_copying_it() {
        let stored = Arc::new(name_list(SQL_RESULT_CACHE_MAX_ROWS));
        let mut cache = SqlResultCache::default();
        cache.insert(
            "shared".to_string(),
            CachedSqlResult {
                bytes: crate::format::retained_heap_bytes(&stored),
                body: CachedBody::Records(Arc::clone(&stored)),
            },
        );

        let first = cache
            .get("shared")
            .expect("the entry was just inserted")
            .expect_records();
        let second = cache
            .get("shared")
            .expect("a second hit on the same entry")
            .expect_records();
        assert!(
            Arc::ptr_eq(&first, &stored) && Arc::ptr_eq(&first, &second),
            "a hit copied the body instead of sharing it"
        );
    }

    /// THE FIFO DEFECT (#2452). A hit used to leave the key at its insertion
    /// position, so the next insert evicted a hot key before an older un-hit
    /// one. A hit now promotes the key while keeping stale recency markers
    /// bounded.
    #[test]
    fn a_hit_promotes_the_key_past_an_unhit_key() {
        let mut cache = SqlResultCache::default();
        for i in 0..SQL_RESULT_CACHE_MAX_ENTRIES {
            cache.insert(format!("k{i}"), entry(name_list(8)));
        }

        assert!(cache.get("k0").is_some(), "the hot key was not warmed");
        for _ in 0..(SQL_RESULT_CACHE_MAX_ENTRIES * 3) {
            assert!(cache.get("k0").is_some(), "the hot key disappeared");
        }
        assert!(
            cache.order.len() <= cache.entries.len() * 2,
            "stale recency markers grew without bound: {} markers for {} entries",
            cache.order.len(),
            cache.entries.len()
        );

        cache.insert("displacer".to_string(), entry(name_list(8)));

        assert!(cache.contains("k0"), "the re-hit key was evicted as FIFO");
        assert!(
            !cache.contains("k1"),
            "the oldest un-hit key survived the entry-cap eviction"
        );
    }

    /// Charge and discharge stay symmetric: a re-insert under the same key
    /// replaces the charge rather than adding to it, and an eviction gives
    /// back exactly what it billed. Otherwise the budget drifts and either
    /// evicts everything or nothing.
    #[test]
    fn overwrites_and_evictions_return_the_bytes_they_billed() {
        let mut cache = SqlResultCache::default();
        let one = entry(name_list(8));
        let charge = one.bytes;
        cache.insert("k".to_string(), one);
        cache.insert("k".to_string(), entry(name_list(8)));
        assert_eq!(
            cache.retained_bytes(),
            // One body, one key, and the two markers the two inserts appended
            // (compaction trims at twice the live entries, so both stand).
            charge + key_heap_bytes("k") + 2 * RECENCY_MARKER_BYTES,
            "re-inserting one key billed the store twice"
        );

        // Past the entry cap, so the oldest key is evicted and its charge is
        // returned; only the survivors are billed.
        for i in 0..SQL_RESULT_CACHE_MAX_ENTRIES {
            cache.insert(format!("k{i}"), entry(name_list(8)));
        }
        assert_eq!(cache.entries.len(), SQL_RESULT_CACHE_MAX_ENTRIES);
        assert_eq!(
            cache.retained_bytes(),
            cache.audited_bytes(),
            "the running total drifted from the entries it is supposed to describe"
        );
    }

    /// The byte budget is enforced on the retained figure, so a store full of
    /// row-heavy entries stops at 4 MiB of HEAP.
    #[test]
    fn the_byte_budget_bounds_retained_heap() {
        let mut cache = SqlResultCache::default();
        for i in 0..SQL_RESULT_CACHE_MAX_ENTRIES {
            cache.insert(format!("k{i}"), entry(name_list(SQL_RESULT_CACHE_MAX_ROWS)));
        }
        assert!(
            cache.retained_bytes() <= SQL_RESULT_CACHE_MAX_BYTES,
            "the store retains {} B under a {SQL_RESULT_CACHE_MAX_BYTES} B budget",
            cache.retained_bytes()
        );
        // And the budget, not the entry cap, is what refused them: 4 MiB of
        // 84 KiB entries is ~48, far short of 256.
        assert!(
            cache.entries.len() < SQL_RESULT_CACHE_MAX_ENTRIES,
            "the byte budget never bound: {} entries fit",
            cache.entries.len()
        );
    }

    /// A 64 KiB key over a 100-byte answer: the shape the body-only charge
    /// priced at ~100 bytes (#2493). The key is caller-controlled — it carries
    /// the normalized SQL — so what the store retains for it has to be inside
    /// the budget the store publishes.
    fn long_key(tag: &str) -> String {
        format!("{tag}{}", "x".repeat(64 * 1024))
    }

    /// A body small enough that anything the store charges above ~1 KiB is key
    /// material.
    fn small_body() -> CachedSqlResult {
        entry(name_list(1))
    }

    /// What makes the charge honest: a recency marker POINTS AT the key the map
    /// holds instead of carrying a copy of its text. Charging one key per entry
    /// is only true while this is. Stale markers — the entry behind them is
    /// gone — are nobody's second copy.
    fn markers_share_the_stored_keys(cache: &SqlResultCache) -> bool {
        cache
            .order
            .iter()
            .all(|marker| match cache.entries.get_key_value(&**marker) {
                Some((stored, _)) => Arc::ptr_eq(stored, marker),
                None => true,
            })
    }

    /// INSERTION. The key is billed with the entry, exactly once, and the total
    /// is the entry, the key and the one recency marker the insert appended.
    #[test]
    fn a_long_key_is_charged_against_the_shared_budget() {
        let key = long_key("k");
        let body = small_body();
        let charge = body.bytes;
        let mut cache = SqlResultCache::default();
        cache.insert(key.clone(), body);

        assert_eq!(
            cache.retained_bytes(),
            charge + key_heap_bytes(&key) + RECENCY_MARKER_BYTES,
            "the store's total is not its entry, its key and its marker"
        );
        assert!(
            cache.retained_bytes() > 64 * 1024,
            "a 64 KiB key was charged as if it were the {charge} B body it \
             points at: {} B",
            cache.retained_bytes()
        );
        assert_eq!(cache.retained_bytes(), cache.audited_bytes());
    }

    /// REPEATED HITS. Every hit used to push another copy of the key into the
    /// recency queue — 64 KiB a hit, charged nowhere, until compaction. A hit
    /// now adds one pointer-sized marker over a key the map already holds, and
    /// compaction bounds even those.
    #[test]
    fn repeated_hits_on_a_long_key_do_not_retain_more_key_material() {
        let key = long_key("k");
        let mut cache = SqlResultCache::default();
        cache.insert(key.clone(), small_body());
        let after_insert = cache.retained_bytes();

        for _ in 0..10_000 {
            assert!(cache.get(&key).is_some(), "the entry disappeared");
        }

        assert!(
            cache.retained_bytes() <= after_insert + 2 * RECENCY_MARKER_BYTES,
            "10,000 hits grew the store from {after_insert} B to {} B; a hit \
             must not retain key material",
            cache.retained_bytes()
        );
        assert_eq!(cache.retained_bytes(), cache.audited_bytes());
        assert!(
            markers_share_the_stored_keys(&cache),
            "a hit put a second copy of the key text in the recency queue"
        );
        // One live entry, so compaction holds the queue at two markers however
        // many hits arrive.
        assert!(
            cache.order.len() <= 2,
            "{} recency markers for one entry",
            cache.order.len()
        );
    }

    /// REPLACEMENT. Storing the same long key again — a new snapshot of the
    /// same query text under the same key — bills one key, not two.
    #[test]
    fn replacing_a_long_key_bills_one_key() {
        let key = long_key("k");
        let mut cache = SqlResultCache::default();
        cache.insert(key.clone(), small_body());
        let after_first = cache.retained_bytes();
        cache.get(&key);
        cache.insert(key.clone(), small_body());

        assert_eq!(
            cache.retained_bytes(),
            after_first,
            "a re-insert under the same key billed a second copy of it"
        );
        assert_eq!(cache.retained_bytes(), cache.audited_bytes());
        assert_eq!(cache.entries.len(), 1);
        assert!(
            markers_share_the_stored_keys(&cache),
            "the replacement left the map and the queue on two allocations"
        );
    }

    /// EVICTION AND COMPACTION. Long keys with tiny bodies now bind on bytes
    /// like anything else, the store gives back exactly what it billed, and the
    /// figure returns to zero when the last entry goes.
    #[test]
    fn long_keys_evict_on_bytes_and_return_what_they_billed() {
        let mut cache = SqlResultCache::default();
        // 64 KiB of key each: the entry cap would allow 256 of them (16 MiB of
        // keys), the byte budget must not.
        for i in 0..SQL_RESULT_CACHE_MAX_ENTRIES {
            cache.insert(long_key(&format!("k{i}|")), small_body());
        }

        assert!(
            cache.retained_bytes() <= SQL_RESULT_CACHE_MAX_BYTES,
            "256 tiny answers under 64 KiB keys retain {} B against a \
             {SQL_RESULT_CACHE_MAX_BYTES} B budget",
            cache.retained_bytes()
        );
        assert!(
            cache.entries.len() < SQL_RESULT_CACHE_MAX_ENTRIES,
            "the byte budget never bound: {} entries of 64 KiB of key fit",
            cache.entries.len()
        );
        assert_eq!(
            cache.retained_bytes(),
            cache.audited_bytes(),
            "eviction returned a different number of bytes than it billed"
        );
        assert!(
            markers_share_the_stored_keys(&cache),
            "eviction left a marker holding its own copy of a key"
        );

        // Drain the survivors through the entry cap, on keys whose own charge
        // is negligible, and the long-key bookkeeping has to unwind to nothing.
        for i in 0..(SQL_RESULT_CACHE_MAX_ENTRIES * 2) {
            cache.insert(format!("short{i}"), small_body());
        }
        assert!(
            cache.entries.keys().all(|key| key.len() < 64 * 1024),
            "a long key survived twice the entry cap of later inserts"
        );
        assert_eq!(cache.retained_bytes(), cache.audited_bytes());
        assert!(
            cache.retained_bytes() < SQL_RESULT_CACHE_MAX_ENTRIES * 4 * 1024,
            "the store still bills {} B for {} short-key entries",
            cache.retained_bytes(),
            cache.entries.len()
        );

        // And the reset seam (`reset_result_cache_for_test`, which publishes a
        // zero gauge) has to leave the same nothing behind it.
        cache.clear();
        assert_eq!(cache.retained_bytes(), 0);
        assert_eq!(cache.audited_bytes(), 0);
    }

    /// The gauge publishes the whole retained figure, keys included: the
    /// operator reading `siglake_query_sql_result_cache_bytes` against the
    /// 4 MiB cap must be reading the same quantity the cap enforces.
    #[test]
    fn the_published_figure_is_the_one_the_budget_enforces() {
        let key = long_key("k");
        let mut cache = SqlResultCache::default();
        cache.insert(key.clone(), small_body());
        assert_eq!(cache.retained_bytes(), cache.audited_bytes());
        assert!(
            cache.retained_bytes() > cache.entries.values().map(|e| e.bytes).sum::<usize>(),
            "the published figure is still the bodies alone"
        );
    }
}

/// Who may store what, and what a name list costs once stored (#2302).
#[cfg(test)]
mod cache_eligibility_tests {
    use super::*;

    fn names(count: usize, len: usize) -> Vec<String> {
        (0..count)
            .map(|i| format!("{i:0width$}", width = len))
            .collect()
    }

    fn arena(count: usize, len: usize) -> CachedBody {
        CachedBody::names(&names(count, len)).expect("an arena of this list")
    }

    /// The round trip is the correctness of the whole representation: what a
    /// hit hands the route has to be what the miss extracted, name for name.
    #[test]
    fn an_arena_returns_the_list_it_was_built_from() {
        for list in [
            Vec::new(),
            vec![String::new()],
            vec!["a".to_string(), String::new(), "bb".to_string()],
            // Multi-byte, because the offsets are BYTE offsets and slicing off
            // a char boundary would panic rather than mis-answer.
            vec!["frontend-café".to_string(), "übergang".to_string()],
            names(800, 8),
        ] {
            let arena = CachedBody::names(&list).expect("an arena of this list");
            assert_eq!(arena.expect_names(), list);
            assert_eq!(arena.row_count(), list.len());
        }
    }

    /// What the byte budget charges a name entry is its whole heap: one buffer
    /// and one offset per name, and nothing else. This is the property that
    /// makes a 512 KiB allowance mean 512 KiB — the rendered form retained
    /// 29–35x what it accounted (#2304 fixed the accounting; the arena removes
    /// the overhead being accounted).
    #[test]
    fn a_name_entry_is_charged_its_whole_heap_and_nothing_more() {
        let list = names(800, 8);
        let arena = CachedBody::names(&list).expect("an arena");
        let text: usize = list.iter().map(String::len).sum();
        assert_eq!(arena.retained_bytes(), text + 4 * list.len());
        // …and it is an order of magnitude under the same list rendered, which
        // is why the caller-scoped allowance admits anything at all.
        let rendered = CachedBody::Records(Arc::new(crate::format::RecordsResponse {
            columns: vec!["service".to_string()],
            row_count: list.len(),
            rows: serde_json::Value::Array(
                list.iter()
                    .map(|name| serde_json::json!({ "service": name }))
                    .collect(),
            ),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        }));
        assert!(
            rendered.retained_bytes() > 20 * arena.retained_bytes(),
            "the render retained {} B against the arena's {} B; the measurement \
             this design rests on is 530.5 KiB against 9.4 KiB at 800 names",
            rendered.retained_bytes(),
            arena.retained_bytes()
        );
    }

    /// `/api/v1/sql` eligibility is EXACTLY what it was before #2302: 128 rows
    /// and 4 MiB, and the row gate is what refuses a 129-row result. The name
    /// path's `max_rows: None` must not have leaked into it.
    #[test]
    fn sql_still_refuses_one_row_past_its_row_cap() {
        let body = |rows: usize| {
            CachedBody::Records(Arc::new(crate::format::RecordsResponse {
                columns: vec!["service".to_string()],
                row_count: rows,
                rows: serde_json::Value::Array(
                    (0..rows)
                        .map(|i| serde_json::json!({ "service": format!("service-{i:04}") }))
                        .collect(),
                ),
                truncated: false,
                max_rows: None,
                cost: None,
                stats: None,
                approximation: None,
            }))
        };
        let at_cap = body(SQL_RESULT_CACHE_MAX_ROWS);
        assert_eq!(
            CacheEligibility::SQL.admits("ns|snapshot=1|query=select 1", &at_cap),
            Some(at_cap.retained_bytes()),
            "a result AT the row cap must still be stored, charged its heap"
        );
        let over = body(SQL_RESULT_CACHE_MAX_ROWS + 1);
        assert!(
            over.retained_bytes() < SQL_RESULT_CACHE_MAX_BYTES,
            "fixture assumption: the 129-row body is far inside the byte budget, \
             so the ROW gate is what this test reads"
        );
        assert!(
            CacheEligibility::SQL
                .admits("ns|snapshot=1|query=select 1", &over)
                .is_none(),
            "a 129-row /api/v1/sql result was admitted: SQL's row cap moved"
        );
    }

    /// The name list's own policy: rows are somebody else's problem
    /// (`ceilings.names`, mid-flight and in the key), bytes are this one's.
    #[test]
    fn a_name_list_is_bounded_by_bytes_and_not_by_rows() {
        // The 800-name list #2268 refused on rows, and the 129 that first
        // crossed the row cap: both far inside 512 KiB.
        let key = "ns.idx|v1|services";
        for count in [129usize, 800] {
            let list = arena(count, 8);
            assert_eq!(
                CacheEligibility::NAME_LIST.admits(key, &list),
                Some(list.retained_bytes()),
                "a {count}-name list was refused entry"
            );
            assert!(
                CacheEligibility::SQL.admits(key, &list).is_none(),
                "fixture assumption: SQL's row cap is what used to refuse a \
                 {count}-name list"
            );
        }

        // The packaged pod's render ceiling of 10,237 names, at the length the
        // design says fits (under 47 characters) and at one that does not.
        let ceiling = crate::jaeger_limits::ceilings_from(crate::admission::MIN_RESERVATION_BYTES);
        let fits = arena(ceiling.names, 24);
        assert_eq!(
            CacheEligibility::NAME_LIST.admits(key, &fits),
            Some(fits.retained_bytes()),
            "the whole render ceiling at 24-character names must fit 512 KiB"
        );
        let too_wide = arena(ceiling.names, 128);
        assert!(
            CacheEligibility::NAME_LIST.admits(key, &too_wide).is_none(),
            "10,237 names of 128 characters is 1.3 MiB and must keep the bypass"
        );
    }

    /// And the allowance is a fraction of the SHARED budget, not a second
    /// budget beside it: one name entry can displace at most an eighth of the
    /// store, and the store's own caps still evict.
    #[test]
    fn one_name_entry_cannot_displace_the_shared_store() {
        assert_eq!(NAME_LIST_CACHE_MAX_BYTES * 8, SQL_RESULT_CACHE_MAX_BYTES);
        let mut cache = SqlResultCache::default();
        for i in 0..16 {
            let key = format!("jaeger-names|v1|{i}");
            let body = arena(10_000, 24);
            let bytes = CacheEligibility::NAME_LIST
                .admits(&key, &body)
                .expect("inside the allowance");
            cache.insert(key, CachedSqlResult { bytes, body });
        }
        assert!(
            cache.retained_bytes() <= SQL_RESULT_CACHE_MAX_BYTES,
            "sixteen ceiling-sized name entries retain {} B against a \
             {SQL_RESULT_CACHE_MAX_BYTES} B store",
            cache.retained_bytes()
        );
    }
}

async fn acquire_admission(
    state: &AppState,
    resolved: &ResolvedLimits,
    cost: &CostReport,
) -> Result<Option<crate::admission::AdmissionGuard>, ApiError> {
    if resolved.priority != Priority::Interactive {
        return Ok(None);
    }
    let reserved_bytes = state.admission.reservation_bytes(cost);
    acquire_admission_reservation(state, resolved.priority, reserved_bytes, Some(cost))
        .await
        .map(Some)
}

pub(crate) async fn acquire_admission_reservation(
    state: &AppState,
    priority: Priority,
    reserved_bytes: u64,
    cost: Option<&CostReport>,
) -> Result<crate::admission::AdmissionGuard, ApiError> {
    match state.admission.acquire(reserved_bytes).await {
        Ok(guard) => Ok(guard),
        Err(failure) => {
            metrics::counter!(
                "siglake_query_breaker_trips_total",
                "breaker" => "admission",
                "priority" => priority.label()
            )
            .increment(1);
            Err(ApiError::too_many_requests(
                format!(
                    "query admission budget is saturated; retry after {}s",
                    failure.retry_after_secs
                ),
                failure.retry_after_secs,
                cost,
            ))
        }
    }
}

/// Plan CLIENT SQL. Read-only, enforced — never `ctx.sql()` on a string that
/// came from a request body.
///
/// `SessionContext::sql` is `sql_with_options(sql, SQLOptions::new())`, and
/// that default allows DDL, DML and session statements. Worse, the DDL is run
/// EAGERLY inside that call — `execute_logical_plan` dispatches
/// `LogicalPlan::Ddl` before the caller ever sees a `DataFrame` — so it lands
/// before this handler's cost estimate, before admission, and before the
/// `dry_run` early return. Measured on 2026-09-06 (task #1523), against every
/// SQL entry point including `dry_run: true` and `priority: "batch"`:
///
///   * `COPY (SELECT raw FROM events) TO '/abs/path.csv'` wrote tenant rows to
///     an arbitrary path on the query pod, 200 OK. Same for `.parquet`,
///     `.json`, and `COPY events TO ...`, and for `EXPLAIN ANALYZE COPY`.
///   * `CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '<any dir>'`
///     returned 200 having listed and schema-read a path outside the
///     warehouse, on `dry_run: true` as well.
///
/// The only thing standing between the rest of the DataFusion write surface
/// and these handlers was an ACCIDENT: `rewrite_search_if_needed` pre-parses
/// with plain sqlparser, which rejects the DataFusion-only `STORED AS CSV` /
/// `COPY ... STORED AS` spellings. Every shape sqlparser happens to accept
/// went straight through. That is not a policy, so this is.
///
/// `sql_with_options` verifies the plan BEFORE `execute_logical_plan`, so a
/// refusal costs the DDL nothing. Every string in this workspace that is built
/// from request input now plans through [`plan_client_sql`] or its storage twin
/// `siglake_storage::plan_read_only_sql`: this handler (#1523), the compactor's
/// persisted delete-task predicate (#1543,
/// `delete_tasks_routes::plan_delete_predicate` and the compactor's sweep), and the
/// Jaeger trace routes' assembled `WHERE` clauses (#1693,
/// `jaeger_routes::TraceQueryContext::query_rows`). What still keeps `ctx.sql`
/// is SQL with no request text in it — the coordinator's merge and the warm
/// probe.
///
/// Regressions: `tests/sql_read_only_boundary.rs`,
/// `siglake-storage/tests/storage/delete_task_read_only_predicate.rs`,
/// `jaeger_routes::tests::jaeger_trace_sql_is_planned_read_only`.
fn read_only_sql() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

/// [`read_only_sql`] applied to one context. A refusal is the planner's own
/// `Error during planning: DDL not supported: …`, which every caller here
/// already maps to a 400.
pub(crate) async fn plan_client_sql(
    ctx: &SessionContext,
    sql: &str,
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    ctx.sql_with_options(sql, read_only_sql()).await
}

// The single-pod fallback is implemented by `handle_local`.
/// `POST /api/v1/sql` entry point. Transparently coordinates a distributed
/// query when worker peers are configured and this is an ordinary interactive
/// request; otherwise (no peers, a worker sub-request with `shard`, a batch
/// job, or a dry-run) it runs the single-pod path, which is also always
/// reachable at `POST /api/v1/sql/local`.
#[utoipa::path(
    post,
    path = "/api/v1/sql",
    tag = "sql",
    request_body = SqlRequest,
    responses(
        (status = 200, description = "Success. Normally result rows \
            (`RecordsResponse`); a `dry_run` request returns only the `CostReport` \
            instead. When the request asks for `format: \"ndjson\"` the body is \
            `application/x-ndjson` — one JSON object per line, streamed, with a \
            trailing `{\"_meta\": ...}` line if the result was cut short or \
            execution failed after the 200 response started. An error trailer \
            carries `code` and `error`; a pool-refusal trailer also carries \
            `retry_after_secs`.",
         content(
             (SqlSuccess = "application/json"),
             (String = "application/x-ndjson"),
         )),
        (status = 202, description = "Admitted and queued on the dedicated runtime \
            when the request set `priority: \"batch\"`. Poll the returned URLs. \
            Batch submissions share the query admission budget and return 429 \
            instead when it remains full through the admission wait.",
         body = BatchSubmitResponse),
        (status = 400, description = "Empty or unparseable SQL, a statement that is \
            not a read (DDL, DML and session statements — `CREATE`, `COPY … TO`, \
            `INSERT`, `SET` — are refused before they can take effect), or a \
            rejected cost estimate. `cost` carries the estimate that was refused.",
         body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 413, description = "Two distinct cases. Either the row cap was hit \
            while rendering — the body is then a `RecordsResponse` with \
            `truncated: true` and the rows that did fit — or the mid-flight \
            rows-scanned breaker tripped, in which case the body is an error \
            carrying `cost` and `stats`. Distinguish by whether `rows` is present.",
         content(
             (RecordsResponse = "application/json"),
             (ApiErrorBody = "application/json"),
         )),
        (status = 422, description = "Unprocessable request body: a key inside \
            `limits` is not a known per-request limit. The commonest case is \
            `max_rows`, which is the RESPONSE envelope's name for the applied cap; \
            the request field is `max_rows_returned`. Refused by the JSON extractor \
            before planning, execution or batch enqueue, so no work is done and no \
            job is created — a dropped cap is never applied silently. The body is \
            the extractor's plain-text message naming the unknown field, not an \
            `ApiErrorBody`. Unknown keys at the TOP level of the request are still \
            ignored."),
        (status = 429, description = "Admission control rejected the interactive query \
            or batch submission; the server is at its in-flight ceiling.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "Retryable refusal, from this server or from a \
            worker whose answer is forwarded: the query memory pool (or the spill \
            directory's byte cap) refused an allocation, or a fanned-out shard could \
            not resolve the generation — table incarnation, snapshot or schema — the coordinator pinned \
            it to (`reason: \"shard_pin_unresolved\"`), which is refused rather than \
            answered from a mix of generations. Nothing is wrong with the query: wait \
            `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The query exceeded its wall-clock budget.",
         body = ApiErrorBody),
    ),
)]
pub async fn handle(
    State(state): State<AppState>,
    axum::extract::Extension(identity): axum::extract::Extension<CallerIdentity>,
    Json(req): Json<SqlRequest>,
) -> Result<Response, ApiError> {
    if req.query.trim().is_empty() {
        return handle_local(State(state), axum::extract::Extension(identity), Json(req)).await;
    }
    let tier = state.limits.tier(req.priority);
    let resolved = ResolvedLimits::resolve(&req.limits, req.priority, &tier);

    // A batch request's timeout governs the queued RUN, not the HTTP handoff.
    // Keep validation, tenant resolution, admission and the 202 outside that
    // caller-set budget; `handle_local_inner` owns this exceptional path.
    if req.priority == Priority::Batch && !req.dry_run {
        return handle_local_inner(state, identity, req, None, None, None).await;
    }

    let clock = RequestClock::new(resolved.timeout);
    let unknown_cost = CostReport::unknown_after_timeout();
    let ice = apply_timeout(
        async {
            state
                .resolve_ice(&identity)
                .await
                .map_err(ApiError::internal)
        },
        &resolved,
        &unknown_cost,
        clock.deadline,
        clock.started,
    )
    .await?;
    let effective_query = apply_timeout(
        apply_query_rewrites(
            &ice,
            &req.query,
            req.default_order,
            req.priority,
            resolved.max_rows_returned,
        ),
        &resolved,
        &unknown_cost,
        clock.deadline,
        clock.started,
    )
    .await?;
    // #967: capture the membership ONCE, here, and hand it to the coordinator
    // path. Under SRV discovery this is `None` until the first usable answer —
    // that pod runs locally, which is correct and merely slower.
    let captured = state
        .peers
        .as_ref()
        .and_then(|source| source.capture())
        .filter(|_| req.shard.is_none() && !req.dry_run && req.priority != Priority::Batch);
    if let Some(peers) = captured {
        // The distributed path had NO timeout. `execute_with_limits` -- the only
        // place applying one -- is reached solely from the single-pod path, so a
        // coordinator query could run forever: measured at 1TB on 2026-08-19
        // hanging past 130s with no response, where the same query on one node
        // returned a bounded 413 at 49.6s.
        //
        // Wrapped HERE rather than around the fan-out itself because the hang was
        // in the coordinator's PRE-DISPATCH work -- cost estimation and the
        // Tier-1 battery, which scan -- and the workers were never reached. Only
        // a wrapper around the whole path covers that.
        //
        // Trading an unbounded hang for a bounded 504 is the same call as
        // defaulting the selectivity gate off: a caller can retry a 504, and can
        // do nothing at all with a request that never returns.
        return apply_timeout(
            distributed_inner(
                state,
                identity,
                req,
                Some(ice),
                Some(effective_query),
                peers,
            ),
            &resolved,
            &unknown_cost,
            clock.deadline,
            clock.started,
        )
        .await;
    }
    handle_local_inner(
        state,
        identity,
        req,
        Some(ice),
        Some(effective_query),
        Some(clock),
    )
    .await
}

/// Run a SQL query on this pod only.
///
/// Identical to `POST /api/v1/sql` except that it never fans out to worker
/// peers, whatever the deployment. Useful for isolating a coordinator from its
/// workers when debugging.
#[utoipa::path(
    post,
    path = "/api/v1/sql/local",
    tag = "sql",
    request_body = SqlRequest,
    responses(
        (status = 200, description = "Success. Result rows, the `CostReport` for a \
            `dry_run`, or an `application/x-ndjson` stream for `format: \"ndjson\"`. \
            If execution fails after that stream's 200 response starts, its final \
            line is an `_meta: \"error\"` trailer with `code` and `error`; a pool \
            refusal also carries `retry_after_secs`.",
         content(
             (SqlSuccess = "application/json"),
             (String = "application/x-ndjson"),
         )),
        (status = 202, description = "Admitted and queued on the dedicated runtime \
            when `priority: \"batch\"`; returns 429 instead if the shared admission \
            budget remains full through the admission wait.",
         body = BatchSubmitResponse),
        (status = 400, description = "Empty or unparseable SQL, a statement that is \
            not a read (DDL, DML and session statements — `CREATE`, `COPY … TO`, \
            `INSERT`, `SET` — are refused before they can take effect), or a \
            rejected cost estimate.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 413, description = "Row cap hit (body is a truncated \
            `RecordsResponse`) or the mid-flight breaker tripped (body is an error).",
         content(
             (RecordsResponse = "application/json"),
             (ApiErrorBody = "application/json"),
         )),
        (status = 422, description = "Unprocessable request body: a key inside \
            `limits` is not a known per-request limit. The commonest case is \
            `max_rows`, which is the RESPONSE envelope's name for the applied cap; \
            the request field is `max_rows_returned`. Refused by the JSON extractor \
            before planning, execution or batch enqueue, so no work is done and no \
            job is created — a dropped cap is never applied silently. The body is \
            the extractor's plain-text message naming the unknown field, not an \
            `ApiErrorBody`. Unknown keys at the TOP level of the request are still \
            ignored."),
        (status = 429, description = "Admission control rejected the interactive query \
            or batch submission.",
         body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "The query memory pool (or the spill directory's \
            byte cap) refused an allocation. Nothing is wrong with the query: wait \
            `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The query exceeded its wall-clock budget.",
         body = ApiErrorBody),
    ),
)]
pub async fn handle_local(
    State(state): State<AppState>,
    axum::extract::Extension(identity): axum::extract::Extension<CallerIdentity>,
    Json(req): Json<SqlRequest>,
) -> Result<Response, ApiError> {
    handle_local_inner(state, identity, req, None, None, None).await
}

async fn handle_local_inner(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    pre_resolved_ice: Option<Arc<siglake_storage::iceberg::IcebergContext>>,
    pre_rewritten_query: Option<RewrittenQuery>,
    request_clock: Option<RequestClock>,
) -> Result<Response, ApiError> {
    let start = request_clock
        .map(|clock| clock.started)
        .unwrap_or_else(Instant::now);
    let format = req.format.unwrap_or_default();
    if req.query.trim().is_empty() {
        emit_audit(
            state.audit.as_ref(),
            &identity,
            "sql",
            &req,
            start.elapsed().as_millis() as i64,
            AuditStatus::Rejected,
            None,
            false,
            Some("query is empty".into()),
        );
        return Err(ApiError::bad_request("query is empty"));
    }
    let tier = state.limits.tier(req.priority);
    let resolved = ResolvedLimits::resolve(&req.limits, req.priority, &tier);
    let clock = request_clock.unwrap_or(RequestClock {
        started: start,
        deadline: tokio::time::Instant::from_std(start + resolved.timeout),
    });
    // One wall-clock budget covers planning and execution. Work before the
    // timeout wrapper below consumes this same deadline rather than giving the
    // render/collect phase a fresh budget.
    let deadline = clock.deadline;
    // Approximation is allowed unless the CALLER opts out, or the operator has
    // disabled it deployment-wide. Default-on because the alternative surprise
    // is worse: a high-cardinality GROUP BY that silently costs seconds looks
    // like the system is broken, while an approximate answer says so in the
    // response and carries its own error bound.
    let allow_approximate = AllowApproximate::for_request(&req);
    let unknown_cost = CostReport::unknown_after_timeout();
    let batch_submission = req.priority == Priority::Batch && !req.dry_run;
    let resolve_started = Instant::now();
    let resolve = async {
        match pre_resolved_ice {
            Some(ice) => Ok(ice),
            None => state
                .resolve_ice(&identity)
                .await
                .map_err(ApiError::internal),
        }
    };
    let ice = if batch_submission {
        resolve.await?
    } else {
        apply_timeout(resolve, &resolved, &unknown_cost, deadline, start).await?
    };
    let resolve_elapsed = resolve_started.elapsed();
    record_phase_metric("sql", "resolve_ice", resolve_elapsed);
    let rewrite = async {
        match pre_rewritten_query {
            Some(query) => Ok(query),
            None => {
                apply_query_rewrites(
                    &ice,
                    &req.query,
                    req.default_order,
                    req.priority,
                    resolved.max_rows_returned,
                )
                .await
            }
        }
    };
    let effective_query = if batch_submission {
        rewrite.await?
    } else {
        apply_timeout(rewrite, &resolved, &unknown_cost, deadline, start).await?
    };

    // Batch path: submit + return 202. Hand the query off to the
    // dedicated batch runtime so it doesn't steal interactive cores. Above the
    // deadline machinery below on purpose: a batch SUBMISSION is not the batch
    // RUN, and its 202 should not be refused because the caller asked the run
    // itself for a short budget.
    if batch_submission {
        return submit_batch(state, identity, req, resolved, ice, effective_query).await;
    }

    // Every preparation phase from here down draws on the SAME budget as
    // execution: WAL delta load, result-cache probe, table registration,
    // logical planning. They used to run unbounded, so a request could spend
    // its whole wall clock before reaching the first wrapper and then be given
    // a fresh one for the collect.
    macro_rules! under_request_deadline {
        ($future:expr) => {
            apply_timeout($future, &resolved, &unknown_cost, deadline, start).await?
        };
    }

    // The count / group-count / date-histogram fast paths read committed
    // Iceberg state directly, bypassing the WS-6 WAL buffer the session ctx
    // is overridden with below. When buffering is active and this query's
    // tables hold uncommitted rows, the committed-only fast paths can't see
    // them. #61 hybrid serving: load those rows ONCE (bounded — a few
    // seal-age segments) and fold them into each fast path as a delta,
    // keeping ~ms metadata answers exact while rows are in flight. A delta
    // LOAD error skips the fast paths (exactness first); the union plan
    // still serves. Loaded BEFORE the result-cache probe: cache hits and
    // inserts are only valid while the delta is PROVEN EMPTY (buffered rows
    // change results without a snapshot change, and a failed load proves
    // nothing either way) — see `BufferDeltaState`.
    let buffer_delta_started = Instant::now();
    // Under the request deadline: a segment scan over a deep buffer is real
    // work, and it is work this caller asked to be bounded. Cutting it costs
    // nothing beyond the request — the delta cache is populated by the load,
    // not by us, so a cancelled load simply leaves the next request to redo it.
    let (buffer_delta_state, buffer_load) = under_request_deadline!(async {
        Ok::<_, ApiError>(
            resolve_buffer_delta(
                &ice,
                state.wal_buffer_dir.as_deref(),
                &state.buffer_delta_cache,
                &identity,
                &effective_query.sql,
            )
            .await,
        )
    });
    let fast_path_safe = buffer_delta_state.fast_paths_safe();
    // The listing the state above was classified from. It travels with the
    // leader ctx so the INSERT can re-prove it: the execution below lists the
    // WAL again, and a segment sealing in that interval lands in the body
    // (#1562).
    let buffer_proof = buffer_proof_of(buffer_load.as_ref());
    let buffer_delta_micros = buffer_delta_started.elapsed().as_micros() as u64;

    // Resolved ABOVE the cache probe, not below it: the probe decides what this
    // request may be served, and both the shard scope and the exactness mode
    // change that (see `ResultCacheScope`). While shard resolution sat after the
    // probe, a shard request could be handed the whole-table body — the probe
    // had no idea a shard had been asked for.
    let shard = req.shard.and_then(ShardParam::resolve);
    let cache_scope = ResultCacheScope {
        allow_approximate,
        shard,
    };

    // The probe itself resolves a snapshot id against the catalog and may park
    // behind another caller's in-flight leader, so it runs under the deadline
    // too. Cancelling it cannot orphan a marker: nothing awaits between the
    // insert into the in-flight map and the `Leader` return, and every other
    // exit is covered by `SqlResultCacheCtx::drop`.
    let cache_ctx = match under_request_deadline!(prepare_result_cache(
        &ice,
        &effective_query.sql,
        req.dry_run,
        format,
        &resolved,
        buffer_delta_state,
        buffer_proof,
        cache_scope,
        deadline,
    )) {
        SqlResultCacheDecision::Skip => None,
        SqlResultCacheDecision::Leader(ctx) => Some(ctx),
        SqlResultCacheDecision::Hit(body) => {
            // The stored body, shared rather than copied (#2304): only the
            // cost report is cloned out of it, for the audit and the metrics,
            // and the response serializes it by reference below. A SQL key
            // cannot resolve to a name-list entry (#2302) — see
            // `CachedBody::expect_records`.
            let body = body.expect_records();
            let cost = body.cost.clone();
            // A hit is a response to THIS request, and this request's clock may
            // already have run out — the probe can be handed a warm entry with
            // no await in sight, which is exactly the shape `apply_timeout`'s
            // pre-poll check exists for. Serving it would report success for a
            // query the caller was told had a wall-clock limit.
            if deadline_expired(&resolved, deadline) {
                let cost = cost.unwrap_or_else(CostReport::unknown_after_timeout);
                let result_meta: Result<Response, ApiError> =
                    Err(request_timeout_error(&resolved, &cost, start));
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            let status = if body.truncated {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::OK
            };
            let result_meta: Result<Response, ApiError> =
                Ok((status, Json(body.as_ref())).into_response());
            if let Some(cost) = cost.as_ref() {
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    cost,
                );
                record_metrics("sql", start, &result_meta, cost);
            }
            return result_meta;
        }
    };

    // Selectivity-aware ordered policy: measured BEFORE the session is built
    // so the provider sees the hint at physical-plan time. Local whole-table
    // requests only — shard scans are the coordinator's business.
    let residual_ordered_hint = under_request_deadline!(async {
        Ok::<_, ApiError>(
            !req.dry_run
                && shard.is_none()
                && match effective_query
                    .preferred_scan_order
                    .as_ref()
                    .and_then(|order| {
                        // The classifier judges the same event-time field the hint
                        // is about: a pure `ts` range is a window, not a residual
                        // dimension term (#6020).
                        detect_ordered_residual_browse(&effective_query.sql, &order.field)
                    }) {
                    Some(shape) => residual_browse_low_selectivity(&ice, &shape).await,
                    None => false,
                },
        )
    });
    let ctx = state
        .query_scan
        .session_context_sharded_with_order(shard, effective_query.preferred_scan_order.clone());
    // Ordered-scan session hints: the residual-selectivity override and the
    // explicit LIMIT (drives the gate's single-partition coalesce — an
    // early-stopping browse gains nothing from scan parallelism and pays an
    // SPM first-poll per partition).
    let ordered_limit_hint = if shard.is_none() {
        effective_query.ordered_limit
    } else {
        None
    };
    // No shard guard, unlike the ordered hint above: that one drives the
    // coordinator's partition coalesce, while this one only says how many rows
    // THIS scan owes — and a shard executing `… LIMIT n` is as clipped as the
    // whole-table request that fanned it out.
    let clipped_limit_hint = effective_query.clipped_limit;
    // Per-request cancellation. The guard lives to the end of this function, so
    // it fires on EVERY exit -- including the one that leaked: the request future
    // being dropped by the 60s timeout or a client hanging up. Dropping that
    // future cancels the future, but the plan's spawned partition pumps keep
    // scanning; setting this flag ends the source stream they drain from, and
    // they unwind. See QueryCancel for the measurement that forced this.
    let cancel = siglake_storage::QueryCancel::new();
    // This execution's identity, minted beside the cancellation flag because
    // the two have the same scope: one request, one plan, one set of scans.
    // The scan stamps it on its tuning and partition profile events, and the
    // terminal `sql query profile` line below carries it too — that is the
    // whole join. It has to be an event FIELD rather than a span: the scan's
    // partition events are emitted from DataFusion's spawned pumps, which
    // carry no request span, and on the NDJSON path they are emitted after
    // this handler has already logged its terminal line.
    let execution_id = siglake_storage::QueryExecutionId::next();
    log_query_execution_start("sql", execution_id, Some(&effective_query.sql), None);
    // ARMED ONLY FOR BUFFERED RESPONSES. An NDJSON response STREAMS its body
    // after this function returns, so a handler-scoped guard would cancel the
    // scan out from under a perfectly healthy request -- caught by three e2e
    // tests that went from 3 rows to 0. Records is the buffered format: the
    // query completes inside this function, so the guard's Drop coincides with
    // the request ending, including the 60s-timeout drop that leaked.
    //
    // GAP, stated rather than hidden: a cancelled NDJSON query still leaks its
    // scan. Closing that means attaching the guard to the response body's
    // lifetime, which is a bigger change than this one.
    // Armed for EVERY format. It covers planning, cost estimation and the
    // Tier-1 battery here, then moves into execute_with_limits below -- which
    // hands it to the NDJSON body when the response streams. The earlier
    // format-conditional version left streaming queries entirely uncancellable.
    let mut cancel_guard = Some(siglake_storage::CancelOnDrop(cancel.clone()));
    let ctx = {
        let mut hinted = ctx.state();
        hinted
            .config_mut()
            .set_extension(std::sync::Arc::new(cancel.clone()));
        hinted
            .config_mut()
            .set_extension(std::sync::Arc::new(execution_id));
        if residual_ordered_hint {
            hinted.config_mut().set_extension(std::sync::Arc::new(
                siglake_storage::OrderedResidualHint { allow: true },
            ));
        }
        if let Some(limit) = ordered_limit_hint {
            hinted.config_mut().set_extension(std::sync::Arc::new(
                siglake_storage::OrderedScanLimit { limit },
            ));
        }
        if let Some(limit) = clipped_limit_hint {
            hinted.config_mut().set_extension(std::sync::Arc::new(
                siglake_storage::ClippedScanLimit { limit },
            ));
        }
        datafusion::prelude::SessionContext::new_with_state(hinted)
    };
    let register_started = Instant::now();
    // Registration reads table metadata (and, with buffering on, lists WAL
    // segments) — catalog and object-store work that a slow backend can stretch
    // well past the request budget. It ends on the deadline like everything
    // else; the half-registered session context dies with the request.
    let registered_tables = under_request_deadline!(async {
        let registered = register_tables_for_query(&ice, &ctx, &effective_query.sql).await?;
        // WS-6: if real-time buffering is on and this query touches `events`,
        // replace the plain Iceberg provider with a union over the un-committed
        // WAL segments so just-ingested rows are visible before the compaction
        // commit.
        if let Some(buffer_root) = state.wal_buffer_dir.as_deref() {
            override_events_with_wal_buffer(&ice, &ctx, buffer_root, identity.tenant.as_deref())
                .await?;
            // #61: same for every USER INDEX the query references.
            override_indexes_with_wal_buffer(
                &ice,
                &ctx,
                buffer_root,
                identity.tenant.as_deref(),
                &effective_query.sql,
            )
            .await?;
        }
        // WS-6: bind the last_values()/distinct_values() hot-cache UDTFs to
        // this query's tenant cache (no-op when hot caches are disabled).
        if let Some(reg) = state.hot_caches.as_ref() {
            reg.register_udtfs(&ctx, identity.tenant.as_deref().unwrap_or("default"));
        }
        Ok::<_, ApiError>(registered)
    });
    let register_elapsed = register_started.elapsed();
    record_phase_metric("sql", "register_tables", register_elapsed);

    let logical_started = Instant::now();
    // Planning is analysis, but it is analysis that touches providers (schema
    // resolution, and for the residual twin a second full plan). Both plans sit
    // inside one wrapper so the twin cannot spend budget the first plan already
    // exhausted.
    let (df, residual_fallback) = under_request_deadline!(async {
        let df = plan_client_sql(&ctx, &effective_query.sql)
            .await
            .map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
        // Temporal-skew safety net for the ordered-residual hint: a twin plan
        // with the hint FORCED OFF (state clones share the registered catalog,
        // buffer overrides included), rendered only if the hinted drain trips the
        // rows breaker — matches that sit deep in the scan direction degrade to
        // today's pruned TopK instead of a 413.
        let residual_fallback = if residual_ordered_hint {
            let mut unhinted = ctx.state();
            unhinted.config_mut().set_extension(std::sync::Arc::new(
                siglake_storage::OrderedResidualHint { allow: false },
            ));
            plan_client_sql(
                &datafusion::prelude::SessionContext::new_with_state(unhinted),
                &effective_query.sql,
            )
            .await
            .ok()
        } else {
            None
        };
        Ok::<_, ApiError>((df, residual_fallback))
    });
    let logical_elapsed = logical_started.elapsed();
    record_phase_metric("sql", "logical_plan", logical_elapsed);
    let estimate_started = Instant::now();
    let cost =
        under_request_deadline!(async { estimate(&df, &ice).await.map_err(ApiError::internal) });
    let estimate_elapsed = estimate_started.elapsed();
    record_phase_metric("sql", "estimate", estimate_elapsed);

    let delta = buffer_load.as_ref().and_then(BufferDeltaLoad::delta);

    let battery_started = Instant::now();
    let fast_path_plan_micros =
        (register_elapsed + logical_elapsed + estimate_elapsed).as_micros() as u64;
    if !req.dry_run && fast_path_safe {
        if format == QueryFormat::Records {
            if let Some(mut body) = try_count_fast_path_records(
                &df,
                shard,
                cost.clone(),
                delta,
                buffer_load.as_ref(),
                "local_records_fast_path",
            ) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                // One allocation from here on: the same body answers this
                // request and fills the cache entry. It used to be copied
                // twice — once into the store, once into the response (#2304).
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let status = if body.truncated {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::OK
                };
                let result_meta: Result<Response, ApiError> =
                    Ok((status, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_windowed_count_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_negation_count_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_count_distinct_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_grouped_numeric_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_group_count_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta,
                allow_approximate,
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
            if let Some(mut body) = under_request_deadline!(try_date_histogram_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                delta
            )) {
                attach_fast_path_phases(
                    &mut body,
                    fast_path_plan_micros,
                    buffer_delta_micros,
                    battery_started,
                    None,
                );
                let body = Arc::new(body);
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(
                        cache_ctx,
                        CacheEligibility::SQL,
                        Some(CachedBody::Records(Arc::clone(&body))),
                    )
                    .await;
                }
                let result_meta: Result<Response, ApiError> =
                    Ok((StatusCode::OK, Json(body.as_ref())).into_response());
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return result_meta;
            }
        } else {
            if let Some(resp) = try_count_fast_path_response(
                &df,
                shard,
                format,
                cost.clone(),
                delta,
                buffer_load.as_ref(),
                "local_ndjson_fast_path",
            )? {
                let result_meta: Result<Response, ApiError> = Ok(Response::builder()
                    .status(resp.status())
                    .body(Body::empty())
                    .map_err(ApiError::internal)?);
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return Ok(resp);
            }
            if let Some(resp) = under_request_deadline!(try_group_count_fast_path_response(
                &df,
                &ice,
                shard,
                format,
                cost.clone(),
                delta,
                allow_approximate,
            )) {
                let result_meta: Result<Response, ApiError> = Ok(Response::builder()
                    .status(resp.status())
                    .body(Body::empty())
                    .map_err(ApiError::internal)?);
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return Ok(resp);
            }
            if let Some(resp) = under_request_deadline!(try_date_histogram_fast_path_response(
                &df,
                &ice,
                shard,
                format,
                cost.clone(),
                delta,
            )) {
                let result_meta: Result<Response, ApiError> = Ok(Response::builder()
                    .status(resp.status())
                    .body(Body::empty())
                    .map_err(ApiError::internal)?);
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result_meta,
                    &cost,
                );
                record_metrics("sql", start, &result_meta, &cost);
                return Ok(resp);
            }
        }
    }

    // PRE-FLIGHT REJECTION GOES *AFTER* THE FAST-PATH BATTERY.
    //
    // It used to run before it, so a query was refused for the bytes a naive
    // full scan WOULD read -- even when the engine answers it from metadata
    // and scans nothing. `SELECT count(*)` over a 1.4B-row index prices at
    // 119 GB and is rejected, while Tier-1 returns it in 35ms having read no
    // data at all.
    //
    // This was invisible until 2026-08-24 because the cost estimator did not
    // recognise managed indexes and priced every user-index query at ZERO, so
    // the gate never fired on the tables anyone actually queries. Making the
    // estimate honest turned on a refusal that had never run, and it refused
    // free queries -- caught by a validation round, not by tests.
    //
    // A query that never scans must not be rejected for bytes it never reads.
    // The battery is metadata-only and cheap, so trying it first costs nothing
    // and a genuinely expensive scan still gets refused here, before it runs.
    // A ROW-LIMITED query is governed by the MID-FLIGHT breaker, not by this
    // one. `estimated_bytes_scanned` is an upper bound, and a `LIMIT` makes it
    // meaningless in BOTH directions -- see `cost::is_row_limited`. Refusing on
    // it took `WHERE region=... LIMIT 100`, which answers in 88ms over 1.0B
    // rows, and turned it into a 400 (measured 2026-08-25: 6,413 refusals in
    // under three hours). The row breaker stops the pathological case at an
    // exact, measured row count instead of a hypothetical byte count.
    let row_limited = crate::cost::is_row_limited(df.logical_plan(), resolved.max_rows_returned);
    if row_limited && cost.estimated_bytes_scanned > resolved.max_bytes_scanned {
        metrics::counter!("siglake_query_preflight_bytes_skipped_limited_total").increment(1);
    }
    if !row_limited && cost.estimated_bytes_scanned > resolved.max_bytes_scanned {
        metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "preflight_bytes",
            "priority" => resolved.priority.label()
        )
        .increment(1);
        let msg = format!(
            "estimated bytes scanned ({}) exceeds the {} limit ({})",
            cost.estimated_bytes_scanned,
            resolved.priority.label(),
            resolved.max_bytes_scanned,
        );
        emit_audit(
            state.audit.as_ref(),
            &identity,
            "sql",
            &req,
            start.elapsed().as_millis() as i64,
            AuditStatus::Rejected,
            Some(&cost),
            false,
            Some(msg.clone()),
        );
        return Err(ApiError::cost_rejected(msg, &cost));
    }

    let _admission = if req.dry_run {
        None
    } else {
        match acquire_admission(&state, &resolved, &cost).await {
            Ok(guard) => guard,
            Err(err) => {
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(cache_ctx, CacheEligibility::SQL, None).await;
                }
                let result = Err(err);
                emit_terminal_audit(
                    state.audit.as_ref(),
                    &identity,
                    "sql",
                    &req,
                    start,
                    &result,
                    &cost,
                );
                record_metrics("sql", start, &result, &cost);
                return result;
            }
        }
    };

    let result = if req.dry_run {
        Ok((StatusCode::OK, Json(cost.clone())).into_response())
    } else {
        let execute_started = Instant::now();
        let result = execute_with_limits(
            df,
            format,
            &resolved,
            cost.clone(),
            cache_ctx.clone(),
            _admission,
            buffer_delta_micros,
            residual_fallback,
            execution_id,
            // Ownership moves out of this scope: for NDJSON it travels into the
            // response body, for Records it is held across the collect. Taking
            // it here is what stops the handler's own guard cancelling a stream
            // that has only just started.
            cancel_guard.take(),
            deadline,
            start,
        )
        .await;
        record_phase_metric("sql", "execute", execute_started.elapsed());
        result
    };
    if let Some(cache_ctx) = cache_ctx {
        if result.is_err() {
            finish_result_cache(cache_ctx, CacheEligibility::SQL, None).await;
        }
    }

    tracing::info!(
        endpoint = "sql",
        // The join key for this execution's scan events. NOT a boundary: on
        // the NDJSON path the body streams after this line, so partition
        // profiles carrying this id still arrive below it.
        query_execution_id = execution_id.0,
        format = match format {
            QueryFormat::Records => "records",
            QueryFormat::Ndjson => "ndjson",
        },
        dry_run = req.dry_run,
        priority = resolved.priority.label(),
        status = response_status_label(&result),
        resolve_ms = resolve_elapsed.as_secs_f64() * 1000.0,
        register_ms = register_elapsed.as_secs_f64() * 1000.0,
        registered_tables,
        logical_plan_ms = logical_elapsed.as_secs_f64() * 1000.0,
        estimate_ms = estimate_elapsed.as_secs_f64() * 1000.0,
        total_ms = start.elapsed().as_secs_f64() * 1000.0,
        query = %compact_query(&effective_query.sql),
        "sql query profile"
    );

    emit_terminal_audit(
        state.audit.as_ref(),
        &identity,
        "sql",
        &req,
        start,
        &result,
        &cost,
    );
    record_metrics("sql", start, &result, &cost);
    result
}

/// One line naming a query execution's id and the SQL it will run.
///
/// Emitted at the START of the execution, not at its end, because start is the
/// only point every path reaches. `/api/v1/sql` ends with `sql query profile`,
/// but the coordinator fan-out logs no terminal line at all, the worker shard
/// endpoint refuses on a dozen paths that return before one could be written,
/// and an NDJSON body outlives the handler that would write it. With this
/// line, every `query_execution_id` stamped on a scan event has exactly one
/// place in the log that says what it was — so a partition profile is
/// attributable without reading the lines around it.
///
/// An EMPTY `query` field means the execution runs more than one statement
/// under one id (the Jaeger render is the only such caller): its scan events
/// are attributable to the request but not to one of its statements, which
/// `scan_id` separates.
pub(crate) fn log_query_execution_start(
    endpoint: &'static str,
    execution_id: siglake_storage::QueryExecutionId,
    query: Option<&str>,
    shard: Option<(usize, usize)>,
) {
    tracing::info!(
        endpoint,
        query_execution_id = execution_id.0,
        shard_index = shard.map(|(index, _)| index).unwrap_or_default(),
        shard_count = shard.map(|(_, count)| count).unwrap_or_default(),
        query = %query.map(compact_query).unwrap_or_default(),
        "query execution start"
    );
}

fn emit_terminal_audit(
    audit: Option<&AuditWriter>,
    identity: &CallerIdentity,
    endpoint: &'static str,
    req: &SqlRequest,
    start: Instant,
    result: &Result<Response, ApiError>,
    cost: &CostReport,
) {
    let duration_ms = start.elapsed().as_millis() as i64;
    let (status, error, truncated) = match result {
        Ok(resp) => match resp.status().as_u16() {
            200 | 202 => (AuditStatus::Succeeded, None, false),
            413 => (AuditStatus::Truncated, None, true),
            _ => (AuditStatus::Succeeded, None, false),
        },
        Err(e) => match e.status.as_u16() {
            400 => (AuditStatus::Rejected, Some(e.msg.clone()), false),
            429 => (AuditStatus::Rejected, Some(e.msg.clone()), false),
            // The pool declined to run it, as admission declines at 429: the
            // query was sound and nothing broke, so it is not `Failed`.
            503 => (AuditStatus::Rejected, Some(e.msg.clone()), false),
            504 => (AuditStatus::Timeout, Some(e.msg.clone()), false),
            500 => (AuditStatus::Failed, Some(e.msg.clone()), false),
            _ => (AuditStatus::Failed, Some(e.msg.clone()), false),
        },
    };
    emit_audit(
        audit,
        identity,
        endpoint,
        req,
        duration_ms,
        status,
        Some(cost),
        truncated,
        error,
    );
}

#[allow(clippy::too_many_arguments)]
fn emit_audit(
    audit: Option<&AuditWriter>,
    identity: &CallerIdentity,
    endpoint: &'static str,
    req: &SqlRequest,
    duration_ms: i64,
    status: AuditStatus,
    cost: Option<&CostReport>,
    truncated: bool,
    error: Option<String>,
) {
    let Some(audit) = audit else {
        return;
    };
    let format = req.format.map(|f| match f {
        QueryFormat::Records => "records",
        QueryFormat::Ndjson => "ndjson",
    });
    audit.submit(AuditRow {
        timestamp: chrono::Utc::now(),
        subject: identity.subject.clone(),
        email: identity.email.clone(),
        endpoint,
        query: req.query.clone(),
        format,
        priority: req.priority,
        duration_ms,
        status,
        complexity: cost.map(|c| c.complexity_class),
        estimated_bytes_scanned: cost.map(|c| c.estimated_bytes_scanned),
        estimated_rows_processed: cost.map(|c| c.estimated_rows_processed),
        truncated,
        error,
    });
}

async fn submit_batch(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    resolved: ResolvedLimits,
    ice: Arc<siglake_storage::iceberg::IcebergContext>,
    effective_query: RewrittenQuery,
) -> Result<Response, ApiError> {
    // Batch planning happens on the dedicated runtime, so there is no cost
    // report yet. Reserve the batch tier's fixed share before creating the job
    // row; a saturated pod therefore answers 429 + Retry-After rather than
    // accepting work that bypasses the interactive admission budget.
    let admission = acquire_admission_reservation(
        &state,
        Priority::Batch,
        state.admission.batch_reservation_bytes(),
        None,
    )
    .await?;
    let id = state
        .jobs
        .submit(
            effective_query.sql.clone(),
            Priority::Batch,
            state.job_owner(&identity),
        )
        .await
        .map_err(ApiError::internal)?;
    let audit = state.audit.clone();
    let identity = identity.clone();
    // Passed WHOLE. `max_rows_scanned` had never been copied across, so
    // `batch_defaults().ceiling_rows_scanned` was dead code and the batch tier
    // had no mid-flight breaker at all.
    let batch_limits = resolved.clone();
    // Resolved BEFORE the job is spawned: `req` does not outlive this scope, and
    // it was being dropped entirely (`_req`), which is how the batch tier came
    // to ignore the caller's exactness request.
    let batch_allow_approximate = AllowApproximate::for_request(&req);
    let jobs = state.jobs.clone();
    let query = effective_query;
    let query_scan = state.query_scan;

    let (abort, abort_registration) = futures::future::AbortHandle::new_pair();
    jobs.register_abort(id, abort);

    state.jobs.batch_runtime().spawn(async move {
        // The reservation lives in the spawned future, not in the short HTTP
        // request. Dropping this future on DELETE also drops the reservation.
        let _admission = admission;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let query_sql = query.sql.clone();
        // ONE budget for everything this run does, started HERE — inside the
        // spawned future, so the wait for a batch-runtime thread is not charged
        // to the caller's execution budget, and before the first lifecycle
        // publication, so a publication cannot outlive it either.
        let deadline = BatchDeadline::for_run(&batch_limits);
        let execution = drive_batch_job(
            BatchJobReporting {
                lifecycle: &lifecycle,
                audit: audit.as_ref(),
                identity: &identity,
                query_sql: &query_sql,
                deadline,
            },
            run_batch_query(
                ice,
                query,
                query_scan,
                batch_limits,
                batch_allow_approximate,
                Some(&lifecycle),
                deadline,
            ),
        );
        let _ = futures::future::Abortable::new(execution, abort_registration).await;
        jobs.clear_abort(id);
    });

    let body = crate::openapi_dto::BatchSubmitResponse {
        job_id: id.to_string(),
        status_url: format!("/api/v1/jobs/{id}"),
        result_url: format!("/api/v1/jobs/{id}/result"),
        priority: "batch".to_string(),
    };
    Ok((StatusCode::ACCEPTED, Json(body)).into_response())
}

/// What a spawned batch run needs to report on itself, apart from the
/// execution it is reporting on.
struct BatchJobReporting<'a> {
    lifecycle: &'a JobLifecycle,
    audit: Option<&'a AuditWriter>,
    identity: &'a CallerIdentity,
    query_sql: &'a str,
    /// The run's one budget — the SAME value the execution runs under, so the
    /// job's start publication, query, and audit duration cannot draw on
    /// different clocks.
    deadline: BatchDeadline,
}

/// A batch job's lifecycle row, as the run executing it publishes to it.
///
/// THE DEFECT THIS GUARDS. `set_running` used to be called from the *completed*
/// branches below, after `run_batch_query` had returned: a job that executed
/// for an hour read `pending` for that hour, its `started_at` recorded the
/// moment execution ENDED, and the cost estimate — which
/// `GET /api/v1/jobs/{id}` documents as populated "once planning has run" —
/// arrived with the answer it was supposed to precede, or never, for a run that
/// failed after planning. The two facts become true at different moments, so
/// they are two writes made at those moments.
///
/// Both writes are conditional on `status IN ('pending', 'running')`, so
/// publishing progress cannot resurrect a job a client cancelled or recovery
/// condemned. A refused start publication stops the run before execution; a
/// refused cost publication is logged and the already-started run carries on
/// to its terminal write, which reports the store's verdict as before.
#[derive(Clone)]
struct JobLifecycle {
    jobs: Arc<crate::jobs::JobStore>,
    id: crate::jobs::JobId,
}

impl JobLifecycle {
    /// Publish `running` before any work is done, so a job that is executing
    /// is observable as executing, with the timestamp it really started at.
    async fn publish_running(&self) -> anyhow::Result<CompletionOutcome> {
        report_lifecycle_transition(
            "running",
            self.id,
            self.jobs.owner_id(),
            self.jobs.set_running(self.id).await,
        )
    }

    /// Publish the cost estimate the moment `estimate()` produced it, rather
    /// than at the end of a run whose whole reason to exist is that it is long.
    async fn publish_cost(&self, cost: &CostReport) {
        let _ = report_lifecycle_transition(
            "cost",
            self.id,
            self.jobs.owner_id(),
            self.jobs.record_cost(self.id, cost.clone()).await,
        );
    }
}

/// Publish the job's start, run it, and record the verdict the store accepted.
///
/// The execution is a parameter so a test can hold a run open at a point of its
/// choosing and observe the row while it is held — the lifecycle is otherwise
/// only visible in the gap between two writes made by the same future.
async fn drive_batch_job(
    reporting: BatchJobReporting<'_>,
    run: impl std::future::Future<Output = BatchOutcome>,
) {
    let publication = reporting
        .deadline
        .run(reporting.lifecycle.publish_running())
        .await;
    drive_batch_job_after_running_publication(reporting, run, publication).await;
}

/// Continue a batch run after its first lifecycle write answered.
///
/// Kept as a seam so cancellation, recovery, disappearance, and store-error
/// answers can be driven without racing a real shared store. In production the
/// answer always comes from `JobLifecycle::publish_running` above.
async fn drive_batch_job_after_running_publication(
    reporting: BatchJobReporting<'_>,
    run: impl std::future::Future<Output = BatchOutcome>,
    publication: Result<anyhow::Result<CompletionOutcome>, BudgetSpent>,
) {
    let BatchJobReporting {
        lifecycle,
        audit,
        identity,
        query_sql,
        deadline,
    } = reporting;
    let jobs = &lifecycle.jobs;
    let id = lifecycle.id;
    // BEFORE the work, not after it — and UNDER the run's budget, because a
    // publication is a write to a store shared by every replica and a shared
    // store can block. A wedged Postgres holds this `UPDATE` for as long as its
    // connection waits, and until it returns the job is admitted work: it holds
    // a quarter of the pod's admission budget and a batch-runtime thread while
    // consuming no execution budget at all. The budget the caller asked for
    // bounds the run, not the part of the run that happens to be query work.
    //
    // A spent budget here is the ordinary `timeout` verdict, reached without
    // executing anything: `run` is dropped UNPOLLED, so no tables are
    // registered, nothing is planned and no scan is started — so there is no
    // estimate to report, and the verdict says so with a null rather than with
    // a zero. The terminal write
    // below is bounded too, on its own clock — see `persist_terminal_state`. It
    // used to be deliberately unbounded, on the grounds that a completion that
    // is never persisted is what recovery exists to clean up; that is only true
    // of a row whose OWNER is gone. Recovery keeps a self-owned row by design,
    // so an unbounded terminal write against a dead store held admitted work
    // for the length of the outage and then left a `running` row that no sweep
    // would ever resolve.
    let outcome = match publication {
        Ok(Ok(CompletionOutcome::Applied {
            status: JobStatus::Running,
        }))
        // A persistence error is not evidence that another actor made the row
        // terminal. Preserve the existing availability choice: execute and
        // let the terminal write discover the authoritative state.
        | Ok(Err(_)) => run.await,
        Ok(Ok(refused)) => {
            drop(run);
            report_refused_batch_start(
                audit,
                identity,
                query_sql,
                deadline.elapsed().as_millis() as i64,
                refused,
            );
            return;
        }
        Err(BudgetSpent) => {
            drop(run);
            BatchOutcome::Timeout(None)
        }
    };
    let duration_ms = deadline.elapsed().as_millis() as i64;
    let audit_row_template = AuditRow {
        timestamp: chrono::Utc::now(),
        subject: identity.subject.clone(),
        email: identity.email.clone(),
        endpoint: "sql",
        query: query_sql.to_string(),
        format: Some("records"),
        priority: Priority::Batch,
        duration_ms,
        status: AuditStatus::Failed, // overridden per outcome
        complexity: None,
        estimated_bytes_scanned: None,
        estimated_rows_processed: None,
        truncated: false,
        error: None,
    };
    // ONE place decides this run's verdict, ONE place writes it, and
    // the report is gated on the write being ACCEPTED. Every terminal
    // write is conditional on `status IN ('pending','running')`, so a
    // cancellation that landed first — including one persisted by
    // another replica and observed here a moment too late — wins, and
    // this run must not then claim the outcome it computed. Reporting
    // `succeeded` over a row that reads `cancelled` is the fleet
    // disagreeing with itself: the metric says the job produced an
    // answer, the audit trail says a query ran to completion, and
    // `GET /api/v1/jobs/<id>` says it was cancelled.
    let (terminal, persisted) = match outcome {
        BatchOutcome::Ok(body, cost) => {
            let dropped_rows = body.row_count;
            // COUNTED, not built: this number is read only by the two `warn!`s
            // below, on the paths where the store refused the completion, but
            // it is computed on EVERY successful batch job — including the
            // ordinary one whose body the store then accepts. Serializing the
            // result into a throwaway `Vec` to call `.len()` on it doubled the
            // peak footprint of every batch success for a diagnostic that is
            // usually never printed. `serialized_json_len` reports the same
            // byte count with scratch bounded by one serialized fragment.
            let dropped_bytes = crate::format::serialized_json_len(&body).unwrap_or(0);
            // Cloned per attempt: `finish_succeeded` takes the body by value
            // (it either stores it or discards it), and a body that has been
            // moved into a write that failed cannot be offered to the next
            // attempt. The path only exists when a write has already failed.
            let persisted = persist_terminal_state(
                jobs,
                id,
                JobStatus::Succeeded,
                deadline.circuit_breakers,
                || jobs.finish_succeeded(id, body.clone()),
            )
            .await;
            (
                BatchTerminal {
                    computed: JobStatus::Succeeded,
                    error: None,
                    cost: Some(cost),
                    dropped_rows,
                    dropped_bytes,
                },
                persisted,
            )
        }
        BatchOutcome::Timeout(cost) => {
            metrics::counter!(
                "siglake_query_breaker_trips_total",
                "breaker" => "timeout",
                "priority" => Priority::Batch.label()
            )
            .increment(1);
            let persisted = persist_terminal_state(
                jobs,
                id,
                JobStatus::Timeout,
                deadline.circuit_breakers,
                || jobs.finish_failed(id, "query exceeded the timeout".into(), true),
            )
            .await;
            (
                BatchTerminal {
                    computed: JobStatus::Timeout,
                    error: Some("query exceeded the timeout".into()),
                    // Whatever the run had estimated before its budget ran
                    // out, on the same terms as a failure: null before
                    // `estimate` returned, the estimate afterwards.
                    cost,
                    dropped_rows: 0,
                    dropped_bytes: 0,
                },
                persisted,
            )
        }
        BatchOutcome::Failed(msg, cost) => {
            let persisted = persist_terminal_state(
                jobs,
                id,
                JobStatus::Failed,
                deadline.circuit_breakers,
                || jobs.finish_failed(id, msg.clone(), false),
            )
            .await;
            (
                BatchTerminal {
                    computed: JobStatus::Failed,
                    error: Some(msg),
                    // Whatever the run had estimated before it failed, so the
                    // audit row agrees with the cost `publish_cost` already
                    // wrote to the job row. Null only before `estimate` ran.
                    cost,
                    dropped_rows: 0,
                    dropped_bytes: 0,
                },
                persisted,
            )
        }
    };
    let attempted = terminal.computed.label();
    match persisted {
        TerminalPersisted::Installed(_) => {}
        // The client's job is NOT resolved yet, and the run cannot resolve it
        // without holding its admission reservation through the outage. Its
        // executor owns the row and knows the execution ended, so it keeps
        // retrying on its own cadence.
        TerminalPersisted::Deferred => tracing::warn!(
            job_id = %id,
            owner = jobs.owner_id(),
            attempted,
            "batch terminal state could not be persisted; deferred to owner-local reconciliation"
        ),
        TerminalPersisted::Abandoned => tracing::error!(
            job_id = %id,
            owner = jobs.owner_id(),
            attempted,
            "batch terminal state could not be persisted and could not be tracked; \
             this row stays non-terminal until its executor exits"
        ),
    }
    let report = terminal_report(attempted, persisted);

    if let Some(outcome) = report.outcome {
        metrics::counter!(
            "siglake_query_jobs_total",
            "priority" => "batch",
            "outcome" => outcome
        )
        .increment(1);
    }
    if let Some(actual) = report.conflict_actual {
        metrics::counter!(
            "siglake_query_job_terminal_conflict_total",
            "attempted" => attempted,
            "actual" => actual,
            "cause" => report.conflict_cause.unwrap_or("other")
        )
        .increment(1);
        if report.conflict_cause == Some("gone") {
            tracing::debug!(
                job_id = %id,
                owner = jobs.owner_id(),
                attempted,
                dropped_rows = terminal.dropped_rows,
                dropped_bytes = terminal.dropped_bytes,
                "batch row vanished before its completion could be stored"
            );
        } else if report.superseded {
            tracing::warn!(
                job_id = %id,
                owner = jobs.owner_id(),
                attempted,
                superseding_status = actual,
                cause = report.conflict_cause.unwrap_or("other"),
                dropped_rows = terminal.dropped_rows,
                dropped_bytes = terminal.dropped_bytes,
                "batch completion was refused; computed output was discarded"
            );
        } else {
            tracing::info!(
                job_id = %id,
                attempted,
                actual,
                "batch completion did not install the outcome it computed"
            );
        }
    }
    if let Some(a) = &audit {
        // The cost is this run's own measurement and stands whatever
        // the store did with the verdict; the status and the error
        // text are the STORE's, never what this run wished for.
        let keep_result = report.outcome == Some(attempted);
        a.submit(AuditRow {
            status: report.audit,
            complexity: terminal.cost.as_ref().map(|c| c.complexity_class),
            estimated_bytes_scanned: terminal.cost.as_ref().map(|c| c.estimated_bytes_scanned),
            estimated_rows_processed: terminal.cost.as_ref().map(|c| c.estimated_rows_processed),
            error: if keep_result {
                terminal.error
            } else {
                Some(report.error(attempted))
            },
            ..audit_row_template
        });
    }
}

/// What a finished batch run *wants* the job's terminal state to be, separated
/// from whether the store accepted it. The result body is not here on purpose:
/// it is handed to `finish_succeeded` and is either stored or discarded by the
/// same conditional write, so there is nothing left to report about it.
struct BatchTerminal {
    /// The verdict this run reached. Its label is the `attempted` one in
    /// `siglake_query_jobs_total` and the conflict series, and the value is
    /// what owner-local reconciliation is told to install if the terminal
    /// write never lands.
    computed: JobStatus,
    error: Option<String>,
    /// `None` only when `estimate` never returned — a registration, plan or
    /// estimation failure. Every later verdict, success or failure, carries the
    /// estimate `JobLifecycle::publish_cost` put on the job row, so the audit
    /// row and `GET /api/v1/jobs/<id>` cannot disagree about the same run.
    cost: Option<CostReport>,
    dropped_rows: usize,
    dropped_bytes: usize,
}

/// Log what the store did with one of the two lifecycle publications
/// (`phase`), which — unlike a terminal write — has nothing to report to a
/// client and nothing to discard. A refused start is returned to the driver,
/// which stops before execution; a refused cost means a terminal state was
/// installed while the run was executing, and its terminal write will meet
/// the same winner and report it.
fn report_lifecycle_transition(
    phase: &'static str,
    id: crate::jobs::JobId,
    owner: &str,
    outcome: anyhow::Result<CompletionOutcome>,
) -> anyhow::Result<CompletionOutcome> {
    match &outcome {
        Ok(CompletionOutcome::Applied {
            status: JobStatus::Running,
        }) => {}
        Ok(CompletionOutcome::Superseded {
            status,
            owned_by_us,
            recovered,
        }) => tracing::warn!(
            job_id = %id,
            owner,
            phase,
            superseding_status = status.label(),
            owned_by_us,
            recovered,
            "batch lifecycle transition was refused"
        ),
        Ok(CompletionOutcome::Vanished) => {
            tracing::debug!(job_id = %id, owner, phase, "batch row vanished before its lifecycle transition")
        }
        Ok(CompletionOutcome::Applied { status }) => tracing::warn!(
            job_id = %id,
            owner,
            phase,
            installed = status.label(),
            "batch lifecycle transition installed an unexpected status"
        ),
        Err(e) => {
            tracing::warn!(job_id = %id, owner, phase, error = %e, "persisting batch lifecycle state failed")
        }
    }
    outcome
}

/// Report a start publication that found an authoritative disposition already
/// in the store. This is not a completion: no query future was polled, no
/// terminal write is attempted, and no job outcome is manufactured.
fn report_refused_batch_start(
    audit: Option<&AuditWriter>,
    identity: &CallerIdentity,
    query_sql: &str,
    duration_ms: i64,
    disposition: CompletionOutcome,
) {
    let actual = disposition.actual_label();
    let cause = completion_conflict_cause(disposition);
    metrics::counter!(
        "siglake_query_job_terminal_conflict_total",
        "attempted" => "running",
        "actual" => actual,
        "cause" => cause
    )
    .increment(1);

    if let Some(a) = audit {
        a.submit(AuditRow {
            timestamp: chrono::Utc::now(),
            subject: identity.subject.clone(),
            email: identity.email.clone(),
            endpoint: "sql",
            query: query_sql.to_string(),
            format: Some("records"),
            priority: Priority::Batch,
            duration_ms,
            status: audit_status_for_disposition(disposition),
            complexity: None,
            estimated_bytes_scanned: None,
            estimated_rows_processed: None,
            truncated: false,
            error: Some(format!(
                "batch execution not started: running publication refused; job already {actual}"
            )),
        });
    }
}

/// How many times a finished run tries to install its terminal state before
/// handing the job to owner-local reconciliation. More than one because a
/// shared store's transient failures (a failover, a dropped connection) are
/// resolved in seconds, and a run that installs its own verdict keeps the
/// error text and — for a success — the RESULT, which reconciliation cannot.
const TERMINAL_PERSIST_ATTEMPTS: usize = 3;

/// Total wall clock those attempts get. The run still holds its admission
/// reservation and a batch-runtime thread while it retries, so this is the
/// price of keeping a real result across a blip, and the reason it is not
/// unbounded: past it the job is handed off and the reservation released.
const TERMINAL_PERSIST_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Spacing between attempts, so three attempts are not three round trips into
/// the same instant of a failover.
const TERMINAL_PERSIST_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

/// What happened to one finished run's terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalPersisted {
    /// The store answered, and this is what it did.
    Installed(CompletionOutcome),
    /// No attempt landed, and the job is parked for owner-local
    /// reconciliation. The row is still non-terminal for now.
    Deferred,
    /// No attempt landed and the id could NOT be parked (reconciliation
    /// bookkeeping is full). The row stays non-terminal until its executor
    /// exits, when lease-expiry recovery resolves it.
    Abandoned,
}

/// Install one finished run's terminal state: bounded attempts, under a
/// bounded wall clock, and a hand-off to owner-local reconciliation if the
/// store never answers.
///
/// THE DEFECT THIS FIXES. One attempt was made and a failure was logged with
/// `actual=unknown`. The row stayed `running` forever: recovery preserves rows
/// owned by a live replica (a stalled heartbeat is not evidence that we stopped
/// executing), and the TTL sweep only deletes rows with an `expires_at`, which
/// only a terminal write sets. The client's job was permanently running with
/// nobody left to finish it.
///
/// The bound is a second clock, started when execution ended — not the run's
/// execution budget, which a timed-out run has by definition already spent and
/// which still has to record that timeout. The wall-clock opt-out
/// (`circuit_breakers: false`) lifts this bound as it lifts every other one.
async fn persist_terminal_state<F, Fut>(
    jobs: &crate::jobs::JobStore,
    id: crate::jobs::JobId,
    computed: JobStatus,
    circuit_breakers: bool,
    mut write: F,
) -> TerminalPersisted
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<CompletionOutcome>>,
{
    let budget = BatchDeadline::starting_now(TERMINAL_PERSIST_BUDGET, circuit_breakers);
    let attempted = computed.label();
    let mut retried_after_error = false;
    for attempt in 1..=TERMINAL_PERSIST_ATTEMPTS {
        match budget.run(write()).await {
            Ok(Ok(outcome)) => {
                return TerminalPersisted::Installed(fold_ambiguous_ack(
                    computed,
                    retried_after_error,
                    outcome,
                ))
            }
            Ok(Err(e)) => {
                metrics::counter!(
                    "siglake_query_job_terminal_write_failed_total",
                    "attempted" => attempted
                )
                .increment(1);
                tracing::warn!(
                    job_id = %id,
                    owner = jobs.owner_id(),
                    attempted,
                    attempt,
                    error = %e,
                    "persisting a batch job's terminal state failed"
                );
                retried_after_error = true;
                if attempt == TERMINAL_PERSIST_ATTEMPTS
                    || budget
                        .run(tokio::time::sleep(TERMINAL_PERSIST_BACKOFF))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            Err(BudgetSpent) => {
                metrics::counter!(
                    "siglake_query_job_terminal_write_failed_total",
                    "attempted" => attempted
                )
                .increment(1);
                tracing::warn!(
                    job_id = %id,
                    owner = jobs.owner_id(),
                    attempted,
                    attempt,
                    budget_secs = TERMINAL_PERSIST_BUDGET.as_secs(),
                    "persisting a batch job's terminal state ran out of time"
                );
                break;
            }
        }
    }
    match jobs.park_finished_unpersisted(id, computed) {
        crate::jobs::ParkOutcome::Tracked => TerminalPersisted::Deferred,
        crate::jobs::ParkOutcome::Full => TerminalPersisted::Abandoned,
    }
}

/// Fold a retried terminal write's answer back into what this run installed.
///
/// A write can fail AFTER the store applied it — the acknowledgement is lost,
/// the connection drops mid-commit — and the retry then meets its own
/// predecessor and is told it lost. That row is not a race with anybody: a
/// client cancellation installs `cancelled`, recovery stamps `recovered_at`,
/// another replica does not execute this job, and this run computes exactly
/// one verdict. So a row that still belongs to us, was not recovered, and
/// reads precisely the status we are trying to install, after an attempt that
/// errored, is our own write — and reporting it as a conflict would count a
/// phantom `siglake_query_job_terminal_conflict_total` and audit an outcome
/// nobody else caused.
///
/// Pure, because it is the whole of that judgement.
fn fold_ambiguous_ack(
    computed: JobStatus,
    retried_after_error: bool,
    outcome: CompletionOutcome,
) -> CompletionOutcome {
    match outcome {
        CompletionOutcome::Superseded {
            status,
            owned_by_us: true,
            recovered: false,
        } if retried_after_error && status == computed => CompletionOutcome::Applied { status },
        other => other,
    }
}

/// What a finished batch run may report, given what it computed and what the
/// store installed.
#[derive(Debug, PartialEq, Eq)]
struct TerminalReport {
    /// `siglake_query_jobs_total{outcome}` — `None` when this run installed
    /// nothing, so it counts no outcome at all. When the store installed a
    /// DIFFERENT status than the run attempted (an oversize result body goes
    /// in as `failed`), this is the installed one.
    outcome: Option<&'static str>,
    /// `siglake_query_job_terminal_conflict_total{attempted,actual}`'s
    /// `actual`, whenever the store did not install what was attempted.
    conflict_actual: Option<&'static str>,
    /// Bounded reason for the conflict, used to page only on recovery
    /// discarding a completion (not expected cancellation or TTL expiry).
    conflict_cause: Option<&'static str>,
    /// Whether another terminal write won, as opposed to this write installing
    /// a different status (oversize body) or erroring.
    superseded: bool,
    /// Audit status — the store's verdict, never the run's wish.
    audit: AuditStatus,
}

impl TerminalReport {
    /// Audit `error` text for a run whose verdict was not installed as
    /// computed. The job row's own `error` carries the authoritative reason;
    /// this is the audit trail saying which run tried what.
    fn error(&self, attempted: &str) -> String {
        let actual = self.conflict_actual.unwrap_or("gone");
        match (self.outcome, self.conflict_cause) {
            (Some(installed), _) => {
                format!("batch completion ({attempted}) recorded as {installed}")
            }
            // Not a race: the store never answered. Says who resolves it,
            // because "refused: job already unknown" reads as a verdict and
            // this is a pending one.
            (None, Some(CAUSE_WRITE_DEFERRED)) => format!(
                "batch completion ({attempted}) could not be persisted; \
                 its executor is reconciling the job row"
            ),
            (None, Some(CAUSE_WRITE_ABANDONED)) => format!(
                "batch completion ({attempted}) could not be persisted and could not be \
                 tracked for reconciliation"
            ),
            (None, _) => format!("batch completion ({attempted}) refused: job already {actual}"),
        }
    }
}

/// No attempt landed and the run's executor will keep trying.
const CAUSE_WRITE_DEFERRED: &str = "write_deferred";
/// No attempt landed and nothing is tracking the job: it is resolved when its
/// executor exits and lease-expiry recovery reaches the row.
const CAUSE_WRITE_ABANDONED: &str = "write_abandoned";

/// Decide what a finished batch run may report. Pure, because it is the whole
/// of the decision and getting it wrong is a metric and an audit row that
/// contradict the job row a client reads.
///
/// A run whose write never landed claims no outcome: nothing is known to have
/// been installed. The two `unknown` causes differ in who resolves the row —
/// its executor's reconciliation pass, or nobody until that executor exits.
fn terminal_report(attempted: &'static str, persisted: TerminalPersisted) -> TerminalReport {
    let installed = match persisted {
        TerminalPersisted::Installed(installed) => installed,
        TerminalPersisted::Deferred => {
            return TerminalReport {
                outcome: None,
                conflict_actual: Some("unknown"),
                conflict_cause: Some(CAUSE_WRITE_DEFERRED),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        }
        TerminalPersisted::Abandoned => {
            return TerminalReport {
                outcome: None,
                conflict_actual: Some("unknown"),
                conflict_cause: Some(CAUSE_WRITE_ABANDONED),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        }
    };
    let actual = installed.actual_label();
    let audit = audit_status_for_disposition(installed);
    match installed {
        // The ordinary case: the store installed exactly this verdict.
        CompletionOutcome::Applied { .. } if actual == attempted => TerminalReport {
            outcome: Some(actual),
            conflict_actual: None,
            conflict_cause: None,
            superseded: false,
            audit,
        },
        // Accepted, but as something else. Count what the row reads, and say
        // so — reporting `succeeded` for a row that reads `failed` is the
        // same lie as reporting it over a cancellation.
        CompletionOutcome::Applied { .. } => TerminalReport {
            outcome: Some(actual),
            conflict_actual: Some(actual),
            conflict_cause: Some("result_too_large"),
            superseded: false,
            audit,
        },
        // A cancellation (possibly persisted by another replica), recovery,
        // or the TTL sweep got there first. This run's result is discarded.
        CompletionOutcome::Superseded { .. } => TerminalReport {
            outcome: None,
            conflict_actual: Some(actual),
            conflict_cause: Some(completion_conflict_cause(installed)),
            superseded: true,
            audit,
        },
        CompletionOutcome::Vanished => TerminalReport {
            outcome: None,
            conflict_actual: Some(actual),
            conflict_cause: Some(completion_conflict_cause(installed)),
            superseded: false,
            audit,
        },
    }
}

/// Map the store disposition to the existing audit status vocabulary.
fn audit_status_for_disposition(disposition: CompletionOutcome) -> AuditStatus {
    match disposition {
        CompletionOutcome::Applied { status } | CompletionOutcome::Superseded { status, .. } => {
            match status {
                JobStatus::Succeeded => AuditStatus::Succeeded,
                JobStatus::Timeout => AuditStatus::Timeout,
                JobStatus::Cancelled => AuditStatus::Cancelled,
                // `failed`, and the two non-terminal states, which a
                // conditional terminal write cannot leave behind.
                _ => AuditStatus::Failed,
            }
        }
        CompletionOutcome::Vanished => AuditStatus::Failed,
    }
}

/// Existing bounded cause classification, shared by refused starts and
/// refused completions. `Applied` is only reachable here for an unexpected
/// running-publication answer and is deliberately not `result_too_large`:
/// there was no result.
fn completion_conflict_cause(disposition: CompletionOutcome) -> &'static str {
    match disposition {
        CompletionOutcome::Superseded {
            recovered: true, ..
        } => "recovery",
        CompletionOutcome::Superseded {
            status: JobStatus::Cancelled,
            ..
        } => "cancellation",
        CompletionOutcome::Superseded { .. } | CompletionOutcome::Applied { .. } => "other",
        CompletionOutcome::Vanished => "gone",
    }
}

// One short-lived value per finished batch job; the variant spread is fine.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum BatchOutcome {
    Ok(crate::format::RecordsResponse, CostReport),
    /// The estimate this run had ALREADY made when its budget ran out — `None`
    /// only before `estimate()` returned, exactly as for `Failed` below.
    ///
    /// It used to be a `CostReport::unknown_after_timeout()` sentinel on every
    /// path, so a timeout taken during table registration, planning or
    /// estimation itself audited `estimated_bytes_scanned = 0`,
    /// `estimated_rows_processed = 0` and `complexity = huge` while the job row
    /// carried null — a number no estimate produced, and `query_audit` has no
    /// `warnings` column to carry the sentinel's "cost was not estimated"
    /// caveat. The sentinel remains the INTERACTIVE path's HTTP-body value,
    /// where the caveat is readable; the batch tier says "not estimated" the
    /// way the rest of the tier says it, with a null.
    Timeout(Option<CostReport>),
    /// The message a client reads, and the estimate this run had ALREADY made
    /// when it failed — `None` only before `estimate()` returned.
    ///
    /// THE DEFECT THIS CARRIES. The variant used to be the message alone, so
    /// every failure audited a null cost while the job row published by
    /// `JobLifecycle::publish_cost` carried the estimate: a job refused by the
    /// pre-flight bytes ceiling — refused *by* that very number — left an audit
    /// row that could not say what it cost to refuse, and
    /// `GET /api/v1/jobs/<id>` and `siglake.query_audit` disagreed about the
    /// same run. A post-estimate failure has a cost; it is the run's own
    /// measurement, so the run reports it.
    Failed(String, Option<CostReport>),
}

fn cancellable_batch_context(
    query_scan: crate::QueryScanConfig,
    preferred_scan_order: Option<siglake_storage::PreferredScanOrder>,
    execution_id: siglake_storage::QueryExecutionId,
) -> (
    datafusion::prelude::SessionContext,
    siglake_storage::CancelOnDrop,
) {
    let cancel = siglake_storage::QueryCancel::new();
    let cancel_guard = siglake_storage::CancelOnDrop(cancel.clone());
    let ctx = query_scan.session_context_with_order(preferred_scan_order);
    let ctx = {
        let mut state = ctx.state();
        state
            .config_mut()
            .set_extension(std::sync::Arc::new(cancel.clone()));
        state
            .config_mut()
            .set_extension(std::sync::Arc::new(execution_id));
        datafusion::prelude::SessionContext::new_with_state(state)
    };
    (ctx, cancel_guard)
}

/// The budget ran out: before the phase was polled, or during it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BudgetSpent;

/// ONE wall-clock budget for a batch RUN — publishing `running`, table
/// registration, planning, estimation, publishing the estimate, the fast-path
/// battery, collection and rendering all draw on it.
///
/// It exists because the batch tier is the asynchronous twin of the interactive
/// deadline, and needs the same contract with a different refusal: a batch job
/// has no HTTP response to carry `ApiError::timeout`, so its verdict is
/// `BatchOutcome::Timeout` and the job row's `timeout` status. Only the collect
/// used to be wrapped, so preparation ran unbounded and then handed a FRESH
/// budget to the collect — on the tier whose jobs hold an admission
/// reservation for their whole life.
///
/// The clock starts when the run starts, not at submission: queue time on the
/// batch runtime is not the caller's execution budget, and the 202 handoff is
/// deliberately outside it (see the batch submission comment above).
///
/// It is `Copy` and passed DOWN from the spawned run rather than created where
/// the query work begins: the run's first act is a lifecycle publication, and a
/// budget created after it would be a second clock that the publication — the
/// one phase that talks to a shared store — is measured by nobody.
#[derive(Debug, Clone, Copy)]
struct BatchDeadline {
    started: tokio::time::Instant,
    deadline: tokio::time::Instant,
    circuit_breakers: bool,
}

impl BatchDeadline {
    /// The budget one batch run gets, from the limits that admitted it. One
    /// place derives it, so the run's phases cannot disagree about the clock.
    fn for_run(limits: &crate::limits::ResolvedLimits) -> Self {
        Self::starting_now(limits.timeout, limits.circuit_breakers)
    }

    fn starting_now(timeout: std::time::Duration, circuit_breakers: bool) -> Self {
        let started = tokio::time::Instant::now();
        Self {
            started,
            deadline: started + timeout,
            circuit_breakers,
        }
    }

    /// Runtime spent since the same instant from which the execution budget
    /// is derived. In particular, this excludes time waiting for this run's
    /// future to be polled on the dedicated batch runtime.
    fn elapsed(&self) -> std::time::Duration {
        self.started.elapsed()
    }

    /// Same question `deadline_expired` asks for a request, for the phases that
    /// are not a wrapped future.
    fn expired(&self) -> bool {
        self.circuit_breakers && tokio::time::Instant::now() >= self.deadline
    }

    /// Run one phase against what is LEFT of the budget.
    ///
    /// The pre-poll check is the whole point, and the same one `apply_timeout`
    /// makes: Tokio polls the inner future before its timer, so without it an
    /// immediately-ready metadata fast path answered a job whose budget had
    /// already gone — and a zero-second batch budget never refused anything the
    /// footers could serve in one poll.
    async fn run<F, T>(&self, fut: F) -> Result<T, BudgetSpent>
    where
        F: std::future::Future<Output = T>,
    {
        // The explicit opt-out lifts the wall clock and NOTHING else: every
        // other limit below is still applied by the phase itself.
        if !self.circuit_breakers {
            return Ok(fut.await);
        }
        if self.expired() {
            return Err(BudgetSpent);
        }
        tokio::time::timeout_at(self.deadline, fut)
            .await
            .map_err(|_| BudgetSpent)
    }
}

async fn run_batch_query(
    ice: std::sync::Arc<siglake_storage::iceberg::IcebergContext>,
    query: RewrittenQuery,
    query_scan: crate::QueryScanConfig,
    // The whole resolved set rather than a widening list of scalars: adding the
    // mid-flight rows-scanned cap made this the eighth parameter, and the reason
    // that cap was missing in the first place is that the call site simply did
    // not copy one more field out.
    limits: crate::limits::ResolvedLimits,
    // Carried explicitly because the batch tier had no way to learn it: the
    // submit path took the `SqlRequest` as `_req` and dropped it, so a batch job
    // that asked for `exact` got an approximate answer regardless -- on the tier
    // whose whole reason to exist is the query too big to answer interactively,
    // where the number is most likely to be the deliverable.
    allow_approximate: AllowApproximate,
    // The job row this run publishes its progress to, when there is one. `None`
    // in the unit tests that only want the outcome.
    lifecycle: Option<&JobLifecycle>,
    // The run's budget, started by the caller before it published `running`.
    deadline: BatchDeadline,
) -> BatchOutcome {
    // Dropping the batch future (timeout or DELETE) must stop the scan pumps,
    // not just their parent future. The scan captures this extension while it
    // is planned; CancelOnDrop flips it on every return path.
    let execution_id = siglake_storage::QueryExecutionId::next();
    log_query_execution_start("sql_batch", execution_id, Some(&query.sql), None);
    let (ctx, cancel_guard) =
        cancellable_batch_context(query_scan, query.preferred_scan_order.clone(), execution_id);
    run_batch_query_in(
        ice,
        query,
        ctx,
        cancel_guard,
        limits,
        allow_approximate,
        lifecycle,
        deadline,
    )
    .await
}

/// The run itself, given the context its scans will be cancelled through.
///
/// Split from [`run_batch_query`] so a test can hold that context's
/// `QueryCancel` and assert what a timeout releases; the guard is moved in
/// here, so every return path from this function still drops it.
#[allow(clippy::too_many_arguments)]
async fn run_batch_query_in(
    ice: std::sync::Arc<siglake_storage::iceberg::IcebergContext>,
    query: RewrittenQuery,
    ctx: datafusion::prelude::SessionContext,
    _cancel_guard: siglake_storage::CancelOnDrop,
    limits: crate::limits::ResolvedLimits,
    allow_approximate: AllowApproximate,
    lifecycle: Option<&JobLifecycle>,
    deadline: BatchDeadline,
) -> BatchOutcome {
    let max_rows = limits.max_rows_returned;
    let max_bytes_scanned = limits.max_bytes_scanned;
    let max_rows_scanned = limits.max_rows_scanned;
    use anyhow::Context;

    // Every phase from here down draws on the same budget — the one the caller
    // started before it published `running`, not a fresh one — and every
    // timeout returns from this function, which drops `_cancel_guard` (stopping
    // the scan pumps) and, one frame up, the job's admission reservation.
    // Before `estimate` runs there is no cost to report, and the verdict says
    // so with `None` — the same thing `BatchOutcome::Failed` says about the
    // same phases. The `$cost` each phase hands the macro is therefore what the
    // run knows at that point, not a sentinel.
    macro_rules! under_run_deadline {
        ($cost:expr, $future:expr) => {
            match deadline.run($future).await {
                Ok(value) => value,
                Err(BudgetSpent) => return BatchOutcome::Timeout($cost),
            }
        };
    }

    if let Err(e) = under_run_deadline!(
        None,
        register_tables_for_query_batch(&ice, &ctx, &query.sql)
    ) {
        return BatchOutcome::Failed(format!("register tables: {e:#}"), None);
    }
    crate::udfs::register_udfs(&ctx);

    let df = match under_run_deadline!(None, plan_client_sql(&ctx, &query.sql)) {
        Ok(df) => df,
        Err(e) => return BatchOutcome::Failed(format!("plan SQL: {e:#}"), None),
    };
    let cost = match under_run_deadline!(None, estimate(&df, &ice)) {
        Ok(c) => c,
        // Estimation itself failed, so there is no estimate to carry — the one
        // remaining case where a batch failure audits a null cost.
        Err(e) => return BatchOutcome::Failed(format!("cost estimate: {e:#}"), None),
    };
    // Planning has run, so the estimate is available — which is what
    // `GET /api/v1/jobs/{id}` promises. It is published HERE rather than with
    // the terminal write, and before the ceiling below can refuse the job, so
    // the client can see the number that refused it.
    //
    // Under the budget like every other phase: this is a write to the shared
    // job store, and a store that has stopped answering must not hold an
    // admitted job past its execution budget. The estimate is already computed,
    // so a run cut here reports the cost it never managed to publish.
    if let Some(lifecycle) = lifecycle {
        under_run_deadline!(Some(cost.clone()), lifecycle.publish_cost(&cost));
    }
    if cost.estimated_bytes_scanned > max_bytes_scanned {
        // The estimate rides along: it is the number that refused the job, and
        // the audit row is where an operator asks what the refusal was about.
        return BatchOutcome::Failed(
            format!(
                "estimated bytes scanned ({}) exceeds batch limit ({})",
                cost.estimated_bytes_scanned, max_bytes_scanned
            ),
            Some(cost),
        );
    }

    // The fast paths answer from metadata, so they can complete in a single
    // poll — which is precisely why they go through the deadline rather than
    // around it: a spent budget must refuse a footer-served answer too.
    if let Some(body) = under_run_deadline!(
        Some(cost.clone()),
        std::future::ready(try_count_fast_path_records(
            &df,
            None,
            cost.clone(),
            None,
            None,
            "batch_fast_path"
        ))
    ) {
        return BatchOutcome::Ok(body, cost);
    }
    match under_run_deadline!(
        Some(cost.clone()),
        try_windowed_count_fast_path_records(&df, &ice, None, cost.clone(), None)
    ) {
        Ok(Some(body)) => return BatchOutcome::Ok(body, cost),
        Ok(None) => {}
        Err(err) => {
            return BatchOutcome::Failed(format!("windowed count fast path: {err:#?}"), Some(cost))
        }
    }
    match under_run_deadline!(
        Some(cost.clone()),
        try_negation_count_fast_path_records(&df, &ice, None, cost.clone(), None)
    ) {
        Ok(Some(body)) => return BatchOutcome::Ok(body, cost),
        Ok(None) => {}
        Err(err) => {
            return BatchOutcome::Failed(format!("negation count fast path: {err:#?}"), Some(cost))
        }
    }
    match under_run_deadline!(
        Some(cost.clone()),
        try_count_distinct_fast_path_records(&df, &ice, None, cost.clone(), None)
    ) {
        Ok(Some(body)) => return BatchOutcome::Ok(body, cost),
        Ok(None) => {}
        Err(err) => {
            return BatchOutcome::Failed(format!("count distinct fast path: {err:#?}"), Some(cost))
        }
    }
    match under_run_deadline!(
        Some(cost.clone()),
        try_grouped_numeric_fast_path_records(&df, &ice, None, cost.clone(), None)
    ) {
        Ok(Some(body)) => return BatchOutcome::Ok(body, cost),
        Ok(None) => {}
        Err(err) => {
            return BatchOutcome::Failed(format!("grouped numeric fast path: {err:#?}"), Some(cost))
        }
    }
    match under_run_deadline!(
        Some(cost.clone()),
        try_group_count_fast_path_records(&df, &ice, None, cost.clone(), None, allow_approximate)
    ) {
        Ok(Some(body)) => return BatchOutcome::Ok(body, cost),
        Ok(None) => {}
        Err(err) => {
            return BatchOutcome::Failed(format!("group count fast path: {err:#?}"), Some(cost))
        }
    }

    let collect_fut = async {
        // BOUNDED, both ways. This was `df.collect()`, which accumulates the
        // entire result into an unaccounted `Vec<RecordBatch>` — outside the
        // memory pool, so nothing could see it — and only truncated afterwards
        // in `batches_to_records`. Combined with the default-order `LIMIT`
        // rewrite being skipped for Batch (see `rewrite_query`) and
        // `max_rows_scanned` never being passed down, ONE request
        // (`{"priority":"batch","query":"SELECT raw FROM events"}`) collected a
        // two-billion-row table until the pod was OOM-killed. No concurrency
        // needed, and `priority` is an unauthenticated body field.
        //
        // The returned-rows bound is not observable: the rows it declines to
        // hold are exactly the ones `batches_to_records` was about to discard.
        let batches = match crate::midflight::collect_with_row_caps(
            df,
            max_rows_scanned,
            max_rows,
            Priority::Batch,
        )
        .await
        .context("collect")?
        {
            crate::midflight::CollectOutcome::Ok(b) => b,
            crate::midflight::CollectOutcome::RowsScannedExceeded {
                rows_scanned,
                limit,
                ..
            } => {
                anyhow::bail!(
                    "query scanned {rows_scanned} rows, exceeding the batch \
                     mid-flight limit of {limit}"
                )
            }
            // Unreachable: this call site asks for `StopAtRows`, which
            // truncates. The refusing bound belongs to the Jaeger routes,
            // whose `{data, total}` shape cannot express a partial result
            // (#2184). Reported rather than rendered as an empty success.
            crate::midflight::CollectOutcome::AccumulatedBoundExceeded { .. } => {
                anyhow::bail!("batch collect reported a refusing accumulation bound")
            }
        };
        let mut body = crate::format::batches_to_records(&batches, Some(max_rows))
            .context("render records")?;
        body.cost = Some(cost.clone());
        Ok::<_, anyhow::Error>(body)
    };

    // What is LEFT of the budget, not a fresh one: the preparation above spent
    // from the same clock.
    match deadline.run(collect_fut).await {
        Ok(Ok(body)) => BatchOutcome::Ok(body, cost),
        // A collect error and a mid-flight row cap are both post-estimate, so
        // both report what the run thought the query would cost.
        Ok(Err(e)) => batch_failed(&e, cost),
        Err(BudgetSpent) => BatchOutcome::Timeout(Some(cost)),
    }
}

/// A batch job's failure has no HTTP status to carry the pool's verdict, but
/// the refusal must still be COUNTED: batch queries are the heavy ones, and a
/// pool refusal there that shows up only as a failed job with a long message is
/// the same "memory pressure or defect?" blind spot the interactive counter
/// closes.
fn batch_failed(err: &anyhow::Error, cost: CostReport) -> BatchOutcome {
    if crate::error::pool_exhaustion(err).is_some() {
        metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => crate::error::POOL_EXHAUSTED_BREAKER,
            "priority" => Priority::Batch.label()
        )
        .increment(1);
    }
    BatchOutcome::Failed(format!("{err:#}"), Some(cost))
}

/// Estimate a query's cost without running it.
///
/// Returns the same `CostReport` that a successful query attaches to its
/// response, so a client can gate on effort before committing to the read.
#[utoipa::path(
    post,
    path = "/api/v1/sql/explain",
    tag = "sql",
    request_body = SqlRequest,
    responses(
        (status = 200, description = "Cost estimate. `exact` is false when the \
            estimate fell back to a heuristic.", body = CostReport),
        (status = 400, description = "Empty or unparseable SQL, or a statement that \
            is not a read (DDL, DML and session statements — `CREATE`, \
            `COPY … TO`, `INSERT`, `SET` — are refused before they can take \
            effect).", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 422, description = "Unprocessable request body: a key inside \
            `limits` is not a known per-request limit. The commonest case is \
            `max_rows`, which is the RESPONSE envelope's name for the applied cap; \
            the request field is `max_rows_returned`. Refused by the JSON extractor \
            before planning, execution or batch enqueue, so no work is done and no \
            job is created — a dropped cap is never applied silently. The body is \
            the extractor's plain-text message naming the unknown field, not an \
            `ApiErrorBody`. Unknown keys at the TOP level of the request are still \
            ignored."),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
    ),
)]
pub async fn explain(
    State(state): State<AppState>,
    axum::extract::Extension(identity): axum::extract::Extension<CallerIdentity>,
    Json(req): Json<SqlRequest>,
) -> Result<Response, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::bad_request("query is empty"));
    }
    let ice = state
        .resolve_ice(&identity)
        .await
        .map_err(ApiError::internal)?;
    let query = rewrite_search_if_needed(&ice, &req.query).await?;
    let ctx = state
        .query_scan
        .session_context_with_order(preferred_scan_order_for_sql(&ice, &query).await);
    register_tables_for_query(&ice, &ctx, &query).await?;
    let df = plan_client_sql(&ctx, &query)
        .await
        .map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
    let cost = estimate(&df, &ice).await.map_err(ApiError::internal)?;
    Ok((StatusCode::OK, Json(cost)).into_response())
}

#[derive(Debug, Clone)]
struct CountFastPath {
    table_name: String,
    output_column: String,
}

/// Which output column a group-count `ORDER BY` targets. Both are servable
/// from the footers battery — the aggregate output is tiny (cardinality-
/// capped), so the re-sort is in-memory over ≤4096 rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupCountSort {
    Count { ascending: bool },
    Group { ascending: bool },
}

#[derive(Debug, Clone)]
struct GroupCountFastPath {
    table_name: String,
    group_column: String,
    group_output_column: String,
    count_output_column: String,
    sort: Option<GroupCountSort>,
    limit: Option<usize>,
    /// `Some(bounds)` for a windowed `WHERE timestamp ∈ [lo,hi)` aggregation
    /// (Phase 1) — a *pure* time range. `None` for the unfiltered fast path.
    time_bounds: Option<siglake_storage::iceberg::TimeBounds>,
    /// How the group value must be RENDERED and ORDERED, taken from the plan's
    /// own output schema.
    ///
    /// Group-count footers store keys as the cast-to-Utf8 rendering of the
    /// column, and this path used to hand those strings straight out and sort
    /// them as strings. For a typed column that is wrong twice over: the JSON
    /// changes type depending on which path served (`"404"` here, `404` from the
    /// planner's Arrow writer), and `ORDER BY <group> LIMIT 5` returns the
    /// LEXICOGRAPHICALLY smallest values — 100, 200, 404, 500, 99 — rather than
    /// the numerically smallest.
    ///
    /// Read from the plan schema rather than the table schema on purpose: the
    /// detector strips a `CAST(col AS Utf8)` to find the column, so it cannot
    /// otherwise tell `GROUP BY CAST(status AS Utf8)` (whose output really is a
    /// string) from a bare `GROUP BY status`. The plan's output type is exactly
    /// what the planner would have produced, which is the thing this path has to
    /// match.
    group_kind: GroupKeyKind,
}

#[derive(Debug, Clone)]
struct GroupedNumericFastPath {
    table_name: String,
    group_column: String,
    value_column: String,
    group_output_column: String,
    avg_output_column: String,
    count_output_column: String,
    limit: Option<usize>,
    group_kind: GroupKeyKind,
}

/// The JSON shape and ordering of a group key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKeyKind {
    Str,
    Int,
    Float,
    Bool,
}

impl GroupKeyKind {
    /// `None` for a type this path cannot render faithfully, which refuses the
    /// fast path rather than guessing.
    fn from_data_type(dt: &datafusion::arrow::datatypes::DataType) -> Option<Self> {
        use datafusion::arrow::datatypes::DataType as D;
        match dt {
            D::Utf8 | D::LargeUtf8 | D::Utf8View => Some(Self::Str),
            D::Int8 | D::Int16 | D::Int32 | D::Int64 => Some(Self::Int),
            D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => Some(Self::Int),
            D::Float16 | D::Float32 | D::Float64 => Some(Self::Float),
            D::Boolean => Some(Self::Bool),
            _ => None,
        }
    }

    /// The footer key as JSON of the planner's type. A key that does not parse
    /// falls back to the string — footers are written by our own writer, so
    /// this should not happen, and a wrong-typed value is better than a panic.
    fn render(self, key: &str) -> serde_json::Value {
        match self {
            Self::Str => serde_json::Value::String(key.to_string()),
            Self::Int => key
                .parse::<i64>()
                .map(|v| serde_json::json!(v))
                .unwrap_or_else(|_| serde_json::Value::String(key.to_string())),
            Self::Float => key
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(key.to_string())),
            Self::Bool => match key {
                "true" => serde_json::Value::Bool(true),
                "false" => serde_json::Value::Bool(false),
                other => serde_json::Value::String(other.to_string()),
            },
        }
    }

    /// Order two group keys as their TYPE orders them, not as text.
    fn cmp_keys(self, a: &str, b: &str) -> std::cmp::Ordering {
        match self {
            Self::Str => a.cmp(b),
            Self::Int => match (a.parse::<i64>(), b.parse::<i64>()) {
                (Ok(l), Ok(r)) => l.cmp(&r),
                _ => a.cmp(b),
            },
            Self::Float => match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(l), Ok(r)) => l.total_cmp(&r),
                _ => a.cmp(b),
            },
            Self::Bool => a.cmp(b),
        }
    }
}

/// #85 follow-through: Tier-1 fast-path bodies get the same wall attribution
/// as executed plans — `stats.phases` was `None` on every metadata-served
/// answer, so a bench couldn't see where a 2ms (or a regressed 9ms) aggregate
/// spends. `plan` covers register + logical plan + estimate (the pre-battery
/// pipeline); `collect` is the battery time through the serving fast path.
fn attach_fast_path_phases(
    body: &mut crate::format::RecordsResponse,
    plan_micros: u64,
    buffer_delta_micros: u64,
    battery_started: Instant,
    distributed: Option<crate::format::DistPhaseStats>,
) {
    if let Some(stats) = body.stats.as_mut() {
        stats.phases = Some(Box::new(crate::format::PhaseStats {
            plan_micros,
            buffer_delta_micros,
            collect_micros: battery_started.elapsed().as_micros() as u64,
            render_micros: 0,
            distributed,
        }));
    }
}

/// The per-request scan-attribution block, from the executed plan's siglake
/// scan-node counters. `None` when the plan had no siglake scan leaf (pure
/// metadata answers) so Tier-1 responses stay visibly scan-free.
fn scan_detail_from_runtime(
    runtime: &crate::midflight::PlanRuntimeStats,
) -> Option<Box<crate::format::ScanDetail>> {
    if runtime.files_planned == 0 && runtime.files_read == 0 {
        return None;
    }
    let mut file_attribution: Option<crate::format::FileAttribution> = None;
    for snapshot in &runtime.file_attributions {
        let next = crate::format::FileAttribution {
            files: snapshot
                .files
                .iter()
                .map(|entry| crate::format::FileAttributionEntry {
                    table: entry.identity.table.clone(),
                    object_key: entry.identity.object_key.clone(),
                    start: entry.identity.start,
                    length: entry.identity.length,
                    cache_candidate: entry.cache_candidate,
                    reader_opened: entry.reader_opened,
                    cache_hit: entry.cache_hit,
                })
                .collect(),
            files_omitted: snapshot.files_omitted,
            identity_complete: snapshot.identity_complete && runtime.unsettled_partitions == 0,
        };
        match &mut file_attribution {
            Some(current) => current.absorb(&next),
            None => file_attribution = Some(next),
        }
    }
    Some(Box::new(crate::format::ScanDetail {
        files_planned: runtime.files_planned,
        files_read: runtime.files_read,
        files_pruned_bloom: runtime.files_pruned_bloom,
        planned_bytes: runtime.planned_bytes,
        planned_rows: runtime.planned_rows,
        row_groups_considered: runtime.row_groups_considered,
        row_groups_pruned_bloom: runtime.row_groups_pruned_bloom,
        row_groups_pruned_stats: runtime.row_groups_pruned_stats,
        row_groups_read: runtime.row_groups_read,
        rows_pruned_selection: runtime.rows_pruned_selection,
        object_store_reads: runtime.object_store_reads,
        bytes_footer: runtime.bytes_footer,
        bytes_index: runtime.bytes_index,
        bytes_data: runtime.bytes_data,
        bytes_data_requested: runtime.bytes_data_requested,
        bytes_other: runtime.bytes_other,
        fetched_bytes: runtime.bytes_scanned,
        decoded_bytes: runtime.decoded_bytes,
        file_cache_hits: runtime.file_cache_hits,
        file_cache_misses: runtime.file_cache_misses,
        file_cache_bypasses: runtime.file_cache_bypasses,
        file_cache_populate_rows: runtime.file_cache_populate_rows,
        unsettled_partitions: runtime.unsettled_partitions,
        ordering: runtime.ordering_outcome.map(str::to_string),
        file_attribution,
    }))
}

fn try_count_fast_path_response(
    df: &DataFrame,
    shard: Option<siglake_storage::ScanShard>,
    format: QueryFormat,
    mut cost: CostReport,
    delta: Option<&BufferDelta>,
    load: Option<&BufferDeltaLoad>,
    response_path: &'static str,
) -> Result<Option<Response>, ApiError> {
    // The manifest total is for the complete table, not the request's file
    // slice. Let the sharded plan compute its partial so shard results remain
    // disjoint and sum to the whole-table count.
    if shard.is_some() || !cost.exact {
        return Ok(None);
    }
    let Some(fp) = detect_count_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let observation =
        resolve_whole_table_count(&mut cost, &fp.table_name, delta, load, response_path);
    let count = i64::try_from(observation.count)
        .map_err(|_| ApiError::internal(anyhow::anyhow!("count result does not fit into i64")))?;
    let row = serde_json::json!({ fp.output_column.clone(): count });
    match format {
        QueryFormat::Records => {
            let body = crate::format::RecordsResponse {
                columns: vec![fp.output_column],
                row_count: 1,
                rows: serde_json::Value::Array(vec![row]),
                truncated: false,
                max_rows: None,
                cost: Some(cost),
                // Fast path: served from pre-aggregates, zero data-file scan.
                stats: Some(crate::format::ScanStats::default()),
                approximation: None,
            };
            Ok(Some((StatusCode::OK, Json(body)).into_response()))
        }
        QueryFormat::Ndjson => {
            let line = serde_json::to_vec(&row).map_err(ApiError::internal)?;
            let mut resp = Response::new(Body::from([line, b"\n".to_vec()].concat()));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/x-ndjson"),
            );
            *resp.status_mut() = StatusCode::OK;
            Ok(Some(resp))
        }
    }
}

fn try_count_fast_path_records(
    df: &DataFrame,
    shard: Option<siglake_storage::ScanShard>,
    mut cost: CostReport,
    delta: Option<&BufferDelta>,
    load: Option<&BufferDeltaLoad>,
    response_path: &'static str,
) -> Option<crate::format::RecordsResponse> {
    // The manifest total is for the complete table, not the request's file
    // slice. Let the sharded plan compute its partial so shard results remain
    // disjoint and sum to the whole-table count.
    if shard.is_some() || !cost.exact {
        return None;
    }
    let fp = detect_count_fast_path(df.logical_plan())?;
    // #61 hybrid: committed count (manifest-exact) + buffered rows. No time
    // bounds to pass: this path now only serves whole-table counts, so every
    // buffered row is in range by construction.
    let observation =
        resolve_whole_table_count(&mut cost, &fp.table_name, delta, load, response_path);
    let count = i64::try_from(observation.count).ok()?;
    Some(crate::format::RecordsResponse {
        columns: vec![fp.output_column.clone()],
        row_count: 1,
        rows: serde_json::Value::Array(vec![serde_json::json!({ fp.output_column: count })]),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        // Fast path: served from pre-aggregates, zero data-file scan.
        stats: Some(crate::format::ScanStats::default()),
        approximation: None,
    })
}

/// Form a whole-table count from one table-cache generation. Before #1262 the
/// delta was loaded against snapshot A, then `estimate()` could refresh to B;
/// if B was the drain commit for one of A's buffered segments, the response
/// briefly counted that segment twice and the next coherent response fell by
/// exactly the drain-batch size.
fn resolve_whole_table_count(
    cost: &mut CostReport,
    table: &str,
    delta: Option<&BufferDelta>,
    load: Option<&BufferDeltaLoad>,
    response_path: &'static str,
) -> WholeTableCountObservation {
    let state = load.and_then(|load| load.counts.get(table));
    let manifest_rows = state
        .and_then(|state| state.manifest_rows)
        .unwrap_or(cost.estimated_rows_processed);
    let included_buffer_rows = state.map_or_else(
        || delta.map_or(0, |delta| delta.rows(table, None)),
        |state| state.included_buffer_rows,
    );
    // The cost attached to a metadata count should describe the committed base
    // used for that answer, not a newer snapshot observed later in the request.
    cost.estimated_rows_processed = manifest_rows;
    let observation = WholeTableCountObservation {
        snapshot_id: state.and_then(|state| state.snapshot_id),
        manifest_rows,
        buffer_within_budget: state.map(|state| state.buffer_within_budget),
        buffer_bytes: state.map(|state| state.buffer_bytes),
        included_buffer_rows,
        response_path,
        count: manifest_rows.saturating_add(included_buffer_rows),
    };
    tracing::info!(
        table,
        snapshot_id = observation.snapshot_id,
        manifest_rows = observation.manifest_rows,
        buffer_within_budget = observation.buffer_within_budget,
        buffer_bytes = observation.buffer_bytes,
        included_buffer_rows = observation.included_buffer_rows,
        response_path = observation.response_path,
        serving_mode = if observation.included_buffer_rows == 0 {
            "committed_only"
        } else {
            "buffer_delta"
        },
        count = observation.count,
        "whole-table count observation"
    );
    observation
}

#[derive(Debug, Clone)]
struct NegationCountFastPath {
    table_name: String,
    column: String,
    predicate: DimCountPredicate,
    output_column: String,
}

/// The dimensional-count predicate, evaluated against group-count footer
/// keys. Footer keys are the cast-to-Utf8 rendering of the column (typed
/// columns tally through arrow's cast kernel), so string/int/bool literals
/// all resolve into the same key space.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DimCountPredicate {
    /// `col = v` / `col IN (…)` (`negated: false` — the answer is
    /// Σ counts(values)) or `col != v` / `col NOT IN (…)` (`negated: true` —
    /// total − Σ counts(values)). NULL-group rows drop out for free since
    /// only non-null matching groups are summed (SQL `=`/`!=`/`IN`/`NOT IN`
    /// all evaluate non-true for a NULL column).
    Set { values: Vec<String>, negated: bool },
    /// A conjunction of integer comparisons on ONE column (`status >= 500
    /// AND status <= 599`, one- or two-sided), normalized to INCLUSIVE
    /// bounds. Served by summing footer keys that parse into the range —
    /// added when the http_logs round found typed-column counts full-scanning
    /// 247M rows with the answer sitting in the same footers as the 1.3ms
    /// zero-scan GROUP BY.
    IntRange { lo: Option<i64>, hi: Option<i64> },
}

impl DimCountPredicate {
    /// Whether a (non-NULL) footer key satisfies the predicate.
    fn matches(&self, key: &str) -> bool {
        match self {
            Self::Set { values, negated } => values.iter().any(|v| v == key) != *negated,
            Self::IntRange { lo, hi } => key
                .parse::<i64>()
                .map(|v| lo.is_none_or(|l| v >= l) && hi.is_none_or(|h| v <= h))
                .unwrap_or(false),
        }
    }
}

/// Detect `SELECT count(*) FROM <t> WHERE <col> != <lit>` (also `<>` and
/// `<col> NOT IN (<lit>, …)`) over a string column. A negation/inequality can't
/// be bloom-pruned (absence is unprovable), so without this it full-scans — at
/// 1TB it timed out. But the answer is `total − count(excluded)`, both available
/// from the per-file group-count footers → ZERO data-file scan. Only a PURE
/// inequality predicate matches; a compound `WHERE` falls through to the planner.
/// Fix 4 — `SELECT count(DISTINCT col) FROM t` (no WHERE / GROUP BY): the
/// exact distinct count is the number of non-NULL keys in the same guarded
/// per-value counts the group-count fast path serves — Tier-1 side object
/// (cap 4096 values/column) in ~ms, per-file footers / raw-page RLE below it,
/// always exact (every tier enumerates real values; the caps only decide WHICH
/// tier answers). Inverted-index engines refuse this shape outright; siglake previously
/// paid a full column scan+hash (~650ms warm / 3.6s cold at 200G).
struct CountDistinctFastPath {
    table_name: String,
    column: String,
    output_column: String,
}

fn detect_count_distinct_fast_path(plan: &LogicalPlan) -> Option<CountDistinctFastPath> {
    let output_column = single_output_name(plan.schema().as_ref())?;
    let plan = match plan {
        LogicalPlan::Projection(Projection { expr, input, .. })
            if projection_is_passthrough(expr) =>
        {
            input.as_ref()
        }
        other => other,
    };
    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return None;
    };
    if !group_expr.is_empty() || aggr_expr.len() != 1 {
        return None;
    }
    let column = count_distinct_column(&aggr_expr[0])?;
    // timestamp: ns-granular ⇒ per-value counts are as big as the table;
    // attributes: a JSON blob column with no footer aggregation. Both would
    // land in the slowest tier for no benefit over the planner.
    if matches!(column.as_str(), "timestamp" | "attributes") {
        return None;
    }
    let LogicalPlan::TableScan(scan) = input.as_ref() else {
        return None;
    };
    if !is_count_fast_path_table(scan.table_name.table()) {
        return None;
    }
    Some(CountDistinctFastPath {
        table_name: scan.table_name.table().to_string(),
        column,
        output_column,
    })
}

/// Match `count(DISTINCT <column>)` (no FILTER / ORDER BY) and return the
/// column name.
fn count_distinct_column(expr: &Expr) -> Option<String> {
    let mut e = expr;
    while let Expr::Alias(alias) = e {
        e = alias.expr.as_ref();
    }
    let Expr::AggregateFunction(agg) = e else {
        return None;
    };
    if !agg.func.name().eq_ignore_ascii_case("count")
        || !agg.params.distinct
        || agg.params.filter.is_some()
        || !agg.params.order_by.is_empty()
        || agg.params.args.len() != 1
    {
        return None;
    }
    unwrap_alias_to_column(&agg.params.args[0]).map(|c| c.name.clone())
}

/// Serve `count(DISTINCT col)` from the guarded per-value counts. `None`
/// (fall through to the planner) when the column has no aggregation path.
async fn try_count_distinct_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    // Same reasoning as the negation path: exactness comes from the per-value
    // counts' own `total == record_count` guards, not from cost-exactness.
    if !group_count_fast_path_enabled() || shard.is_some() {
        return Ok(None);
    }
    let Some(fp) = detect_count_distinct_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let Some(rows) = ice
        .grouped_counts_with_summary(&fp.table_name, &fp.column, shard, None)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    // COUNT(DISTINCT col) counts distinct non-NULL values. The merged rows'
    // keys are already distinct, so the drained steady state is a plain
    // non-null scan — no per-key set materialization (1.1M-key columns paid
    // ~250ms of string clones here). A live buffer unions via a BORROWED set
    // (#61 hybrid).
    let distinct = match delta {
        None => rows.iter().filter(|(key, _)| key.is_some()).count(),
        Some(delta) => {
            let committed: std::collections::HashSet<&str> =
                rows.iter().filter_map(|(key, _)| key).collect();
            let extra: std::collections::HashSet<String> = delta
                .group_counts(&fp.table_name, &fp.column, None)
                .into_keys()
                .flatten()
                .filter(|k| !committed.contains(k.as_str()))
                .collect();
            committed.len() + extra.len()
        }
    };
    let count = i64::try_from(distinct)
        .map_err(|_| ApiError::internal(anyhow::anyhow!("count result does not fit into i64")))?;
    metrics::counter!("siglake_query_count_distinct_fast_path_total").increment(1);
    Ok(Some(crate::format::RecordsResponse {
        columns: vec![fp.output_column.clone()],
        row_count: 1,
        rows: serde_json::Value::Array(vec![serde_json::json!({ fp.output_column: count })]),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        // Fast path: served from the per-value counts, zero data-file scan.
        stats: Some(crate::format::ScanStats::default()),
        approximation: None,
    }))
}

fn detect_negation_count_fast_path(plan: &LogicalPlan) -> Option<NegationCountFastPath> {
    let output_column = single_output_name(plan.schema().as_ref())?;
    let plan = match plan {
        LogicalPlan::Projection(Projection { expr, input, .. })
            if projection_is_passthrough(expr) =>
        {
            input.as_ref()
        }
        other => other,
    };
    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return None;
    };
    if !group_expr.is_empty() || aggr_expr.len() != 1 || !is_exact_count_star(&aggr_expr[0]) {
        return None;
    }
    let LogicalPlan::Filter(filter) = input.as_ref() else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    if !is_count_fast_path_table(scan.table_name.table()) {
        return None;
    }
    let (column, predicate) = extract_dimensional_predicate(&filter.predicate)?;
    if matches!(column.as_str(), "timestamp" | "attributes") {
        return None;
    }
    // THE COLUMN'S TYPE, not just its name.
    //
    // This path answers by comparing the literal against group-count footer
    // KEYS, which are the cast-to-Utf8 rendering of the column. That is only
    // sound when the rendering is canonical AND agrees with how the literal was
    // written -- true for strings, integers and booleans, false for floating
    // point, whose keys read "1.5" and "2.0".
    //
    // Unchecked, `count(*) WHERE latency_ms > 100` on a `type: "double"` field
    // returned 0: `IntRange::matches` does `key.parse::<i64>()`, no float key
    // parses, so every group was excluded -- zero-scan, no approximation
    // marker, and the `cost.exact` gate is deliberately absent on this path.
    // `ratio != 1` had the mirror form, counting rows whose value is 1.0
    // because the key "1.0" is not the literal key "1".
    //
    // An allowlist rather than a float denylist: the failure is silent and
    // wrong, so a type nobody considered should fall through to the planner and
    // be slow, not answer confidently.
    let field = filter
        .input
        .schema()
        .field_with_unqualified_name(&column)
        .ok()?;
    if !dim_count_key_rendering_is_exact(field.data_type()) {
        return None;
    }
    Some(NegationCountFastPath {
        table_name: scan.table_name.table().to_string(),
        column,
        predicate,
        output_column,
    })
}

/// Pull `(column, predicate)` out of a pure dimensional predicate:
/// `col = v` / `col != v` / `col <> v` (either operand order), `col IN (…)`
/// / `col NOT IN (…)` — string, integer, or boolean literals — and integer
/// comparisons `col >= a [AND col <= b]` (also `>`, `<`, `BETWEEN`).
/// Anything else (OR, mixed columns, float literals — whose footer-key
/// rendering isn't reproducible from the SQL text) returns `None` so the
/// caller falls through to the planner.
fn extract_dimensional_predicate(pred: &Expr) -> Option<(String, DimCountPredicate)> {
    match pred {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(op, Operator::NotEq | Operator::Eq) =>
        {
            let negated = *op == Operator::NotEq;
            if let (Some(col), Some(v)) = (unwrap_alias_to_column(left), footer_key_literal(right))
            {
                return Some((
                    col.name.clone(),
                    DimCountPredicate::Set {
                        values: vec![v],
                        negated,
                    },
                ));
            }
            if let (Some(v), Some(col)) = (footer_key_literal(left), unwrap_alias_to_column(right))
            {
                return Some((
                    col.name.clone(),
                    DimCountPredicate::Set {
                        values: vec![v],
                        negated,
                    },
                ));
            }
            None
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            ) =>
        {
            let (col, lo, hi) = int_range_term(left, *op, right)?;
            Some((col, DimCountPredicate::IntRange { lo, hi }))
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            // Conjunction of integer comparisons on ONE column → merged range.
            let mut conjuncts = Vec::new();
            flatten_and_exprs(left, &mut conjuncts);
            flatten_and_exprs(right, &mut conjuncts);
            let mut column: Option<String> = None;
            let (mut lo, mut hi): (Option<i64>, Option<i64>) = (None, None);
            for c in conjuncts {
                let Expr::BinaryExpr(BinaryExpr { left, op, right }) = c else {
                    return None;
                };
                if !matches!(
                    op,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
                ) {
                    return None;
                }
                let (col, c_lo, c_hi) = int_range_term(left, *op, right)?;
                match &column {
                    Some(existing) if *existing != col => return None,
                    Some(_) => {}
                    None => column = Some(col),
                }
                lo = match (lo, c_lo) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (a, b) => a.or(b),
                };
                hi = match (hi, c_hi) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
            Some((column?, DimCountPredicate::IntRange { lo, hi }))
        }
        Expr::Between(between) if !between.negated => {
            let col = unwrap_alias_to_column(&between.expr)?;
            let lo = int_literal(&between.low)?;
            let hi = int_literal(&between.high)?;
            Some((
                col.name.clone(),
                DimCountPredicate::IntRange {
                    lo: Some(lo),
                    hi: Some(hi),
                },
            ))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let col = unwrap_alias_to_column(expr)?;
            let values = list
                .iter()
                .map(footer_key_literal)
                .collect::<Option<Vec<_>>>()?;
            (!values.is_empty()).then(|| {
                (
                    col.name.clone(),
                    DimCountPredicate::Set {
                        values,
                        negated: *negated,
                    },
                )
            })
        }
        _ => None,
    }
}

fn flatten_and_exprs<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            flatten_and_exprs(left, out);
            flatten_and_exprs(right, out);
        }
        other => out.push(other),
    }
}

/// One integer comparison, either operand order, normalized to INCLUSIVE
/// `(column, lo, hi)` bounds. `col > a` ⇒ lo = a+1; `a > col` ⇒ hi = a−1;
/// overflow at the i64 edge rejects (falls back to the planner).
fn int_range_term(
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<(String, Option<i64>, Option<i64>)> {
    let (col, value, flipped) = match (unwrap_alias_to_column(left), int_literal(right)) {
        (Some(c), Some(v)) => (c, v, false),
        _ => match (int_literal(left), unwrap_alias_to_column(right)) {
            (Some(v), Some(c)) => (c, v, true),
            _ => return None,
        },
    };
    // Flip the operator when the literal is on the left (`500 <= col`).
    let op = if flipped {
        match op {
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            other => other,
        }
    } else {
        op
    };
    let (lo, hi) = match op {
        Operator::GtEq => (Some(value), None),
        Operator::Gt => (Some(value.checked_add(1)?), None),
        Operator::LtEq => (None, Some(value)),
        Operator::Lt => (None, Some(value.checked_sub(1)?)),
        _ => return None,
    };
    Some((col.name.clone(), lo, hi))
}

fn string_literal(expr: &Expr) -> Option<String> {
    use datafusion::scalar::ScalarValue;
    let mut e = expr;
    while let Expr::Alias(alias) = e {
        e = alias.expr.as_ref();
    }
    match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

/// Render a string/integer/boolean literal to its group-count footer key —
/// the cast-to-Utf8 rendering the writer's tally produces (decimal for
/// ints, `true`/`false` for bools). Floats are deliberately absent: their
/// cast rendering isn't reliably reproducible from the SQL literal.
/// Can this column's values be compared as group-count footer keys?
///
/// Footer keys are the cast-to-Utf8 rendering of the column, and the literals
/// this path can express are strings, integers and booleans (see
/// [`footer_key_literal`]). The set below is exactly the types whose rendering
/// round-trips against those literals.
fn dim_count_key_rendering_is_exact(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType as D;
    matches!(
        dt,
        D::Utf8
            | D::LargeUtf8
            | D::Utf8View
            | D::Boolean
            | D::Int8
            | D::Int16
            | D::Int32
            | D::Int64
            | D::UInt8
            | D::UInt16
            | D::UInt32
            | D::UInt64
    )
}

fn footer_key_literal(expr: &Expr) -> Option<String> {
    use datafusion::scalar::ScalarValue;
    if let Some(s) = string_literal(expr) {
        return Some(s);
    }
    if let Some(i) = int_literal(expr) {
        return Some(i.to_string());
    }
    let mut e = expr;
    while let Expr::Alias(alias) = e {
        e = alias.expr.as_ref();
    }
    match e {
        Expr::Literal(ScalarValue::Boolean(Some(b)), _) => Some(b.to_string()),
        _ => None,
    }
}

fn int_literal(expr: &Expr) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    let mut e = expr;
    while let Expr::Alias(alias) = e {
        e = alias.expr.as_ref();
    }
    if let Expr::Negative(inner) = e {
        return int_literal(inner).and_then(i64::checked_neg);
    }
    match e {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Some(i64::from(*v)),
        Expr::Literal(ScalarValue::Int16(Some(v)), _) => Some(i64::from(*v)),
        Expr::Literal(ScalarValue::Int8(Some(v)), _) => Some(i64::from(*v)),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => i64::try_from(*v).ok(),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(i64::from(*v)),
        _ => None,
    }
}

/// Execute the negation/inequality count from the group-count footers:
/// `total − Σ excluded`, with NULL groups excluded. Coordinator-local only
/// (mirrors the group-count fast path's shard guard). Returns `None` (fall
/// through) when the column isn't footer-aggregated.
async fn try_negation_count_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    // NB: do NOT gate on `cost.exact` — a dimensional `!=`/`NOT IN` predicate is
    // never cost-exact (the cost estimate can't prune it), so requiring exactness
    // would make this path never fire (it 504'd at 1TB). Exactness here comes from
    // the group-count aggregate, validated by its own `total == record_count` guard
    // inside `grouped_counts_with_summary`.
    if !group_count_fast_path_enabled() || shard.is_some() {
        return Ok(None);
    }
    let Some(fp) = detect_negation_count_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let Some(rows) = ice
        .grouped_counts_with_summary(&fp.table_name, &fp.column, shard, None)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    // Negation: rows whose (non-NULL) value is NOT in the set. Equality/IN:
    // rows whose value IS in the set. Ranges: keys parsing into the bounds.
    // All from the same per-value counts.
    let mut total: u64 = rows
        .iter()
        .filter_map(|(key, count)| key.map(|k| (k, count)))
        .filter(|(k, _)| fp.predicate.matches(k))
        .map(|(_, count)| count)
        .sum();
    // #61 hybrid: buffered rows count too (same predicate, NULLs excluded).
    if let Some(delta) = delta {
        total += delta
            .group_counts(&fp.table_name, &fp.column, None)
            .into_iter()
            .filter_map(|(k, v)| k.map(|k| (k, v)))
            .filter(|(k, _)| fp.predicate.matches(k.as_str()))
            .map(|(_, v)| v)
            .sum::<u64>();
    }
    let count = i64::try_from(total)
        .map_err(|_| ApiError::internal(anyhow::anyhow!("count result does not fit into i64")))?;
    Ok(Some(crate::format::RecordsResponse {
        columns: vec![fp.output_column.clone()],
        row_count: 1,
        rows: serde_json::Value::Array(vec![serde_json::json!({ fp.output_column: count })]),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        // Fast path: served from group-count footers, zero data-file scan.
        stats: Some(crate::format::ScanStats::default()),
        approximation: None,
    }))
}

#[derive(Debug, Clone)]
struct WindowedCountFastPath {
    table_name: String,
    output_column: String,
    time_bounds: siglake_storage::iceberg::TimeBounds,
}

/// Detect `SELECT count(*) FROM <t> WHERE timestamp ∈ [lo,hi)` — a count over a
/// PURE time range. The plain count fast path can't serve it (the cost estimate
/// over-counts boundary files, so `cost.exact` is false → full scan); this routes
/// it to the per-snapshot time-bucket aggregate (`IcebergContext::windowed_count`).
fn detect_windowed_count_fast_path(plan: &LogicalPlan) -> Option<WindowedCountFastPath> {
    let output_column = single_output_name(plan.schema().as_ref())?;
    let plan = match plan {
        LogicalPlan::Projection(Projection { expr, input, .. })
            if projection_is_passthrough(expr) =>
        {
            input.as_ref()
        }
        other => other,
    };
    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return None;
    };
    if !group_expr.is_empty() || aggr_expr.len() != 1 || !is_exact_count_star(&aggr_expr[0]) {
        return None;
    }
    let LogicalPlan::Filter(filter) = input.as_ref() else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    if !is_count_fast_path_table(scan.table_name.table()) {
        return None;
    }
    let time_bounds =
        extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema())?;
    Some(WindowedCountFastPath {
        table_name: scan.table_name.table().to_string(),
        output_column,
        time_bounds,
    })
}

/// Serve a windowed `count(*)` from the per-snapshot time-bucket aggregate (core
/// from warm metadata + ≤2 small boundary ranges). Coordinator-local only.
/// Returns `None` (fall through to the planner/scan) for an open-ended window or
/// a missing/stale aggregate.
async fn try_windowed_count_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    if !group_count_fast_path_enabled() || shard.is_some() {
        return Ok(None);
    }
    let Some(fp) = detect_windowed_count_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let buffered = delta.map_or(0, |d| d.rows(&fp.table_name, Some(&fp.time_bounds)));
    let Some(count) = ice
        .windowed_count(&fp.table_name, fp.time_bounds, shard)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    let count = i64::try_from(count + buffered)
        .map_err(|_| ApiError::internal(anyhow::anyhow!("count result does not fit into i64")))?;
    Ok(Some(crate::format::RecordsResponse {
        columns: vec![fp.output_column.clone()],
        row_count: 1,
        rows: serde_json::Value::Array(vec![serde_json::json!({ fp.output_column: count })]),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        // Core from the snapshot aggregate; only ≤2 small boundary ranges touch files.
        stats: Some(crate::format::ScanStats::default()),
        approximation: None,
    }))
}

async fn try_grouped_numeric_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    if !grouped_numeric_fast_path_enabled() || shard.is_some() || delta.is_some() {
        return Ok(None);
    }
    let Some(fp) = detect_grouped_numeric_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let Some(mut rows) = ice
        .grouped_numeric_avg(&fp.table_name, &fp.group_column, &fp.value_column)
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    rows.sort_by(|left, right| {
        right.rows.cmp(&left.rows).then_with(|| {
            match (left.group.as_deref(), right.group.as_deref()) {
                (Some(left), Some(right)) => fp.group_kind.cmp_keys(left, right),
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            }
        })
    });
    if let Some(limit) = fp.limit {
        rows.truncate(limit);
    }
    let json_rows = rows
        .into_iter()
        .map(|row| {
            serde_json::json!({
                fp.group_output_column.clone(): row.group.as_deref().map_or(serde_json::Value::Null, |key| fp.group_kind.render(key)),
                fp.avg_output_column.clone(): row.avg,
                fp.count_output_column.clone(): row.rows,
            })
        })
        .collect::<Vec<_>>();
    Ok(Some(crate::format::RecordsResponse {
        columns: vec![
            fp.group_output_column,
            fp.avg_output_column,
            fp.count_output_column,
        ],
        row_count: json_rows.len(),
        rows: serde_json::Value::Array(json_rows),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        stats: Some(crate::format::ScanStats {
            served_by: Some("grouped_numeric_footer".to_string()),
            ..Default::default()
        }),
        approximation: None,
    }))
}

fn grouped_numeric_fast_path_from(value: Option<&str>) -> bool {
    !value.is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false" | "no"
        )
    })
}

fn grouped_numeric_fast_path_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        grouped_numeric_fast_path_from(
            std::env::var("SIGLAKE_GROUPED_NUMERIC_FAST_PATH")
                .ok()
                .as_deref(),
        )
    })
}

async fn try_group_count_fast_path_response(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    format: QueryFormat,
    cost: CostReport,
    delta: Option<&BufferDelta>,
    allow_approximate: AllowApproximate,
) -> Result<Option<Response>, ApiError> {
    let Some(body) =
        try_group_count_fast_path_records(df, ice, shard, cost, delta, allow_approximate).await?
    else {
        return Ok(None);
    };
    match format {
        QueryFormat::Records => Ok(Some((StatusCode::OK, Json(body)).into_response())),
        QueryFormat::Ndjson => {
            let rows = body.rows.as_array().ok_or_else(|| {
                ApiError::internal(anyhow::anyhow!("group fast path rows must be an array"))
            })?;
            let mut payload = Vec::new();
            for row in rows {
                let line = serde_json::to_vec(row).map_err(ApiError::internal)?;
                payload.extend_from_slice(&line);
                payload.push(b'\n');
            }
            let mut resp = Response::new(Body::from(payload));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/x-ndjson"),
            );
            *resp.status_mut() = StatusCode::OK;
            Ok(Some(resp))
        }
    }
}

/// The GROUP BY COUNT(*) fast path is now backed by the precomputed per-file
/// group-count summary stamped into each data file's Parquet footer at write
/// time (with a raw-page RLE scan fallback for any file the summary doesn't
/// cover — high-cardinality column, pre-feature/rolled file, or deletes). Because
/// it sums footers instead of run-length-decoding pages, the win no longer
/// depends on dim-clustered RLE compressibility, so it is **default-on**. Set
/// `SIGLAKE_GROUP_COUNT_FAST_PATH=0` to disable (fall back to the DataFusion
/// aggregate). The result is differential-tested byte-equal to DataFusion.
fn group_count_fast_path_enabled() -> bool {
    group_count_fast_path_enabled_from(
        std::env::var("SIGLAKE_GROUP_COUNT_FAST_PATH")
            .ok()
            .as_deref(),
    )
}

/// Resolve the group-count fast-path switch from its raw environment value.
///
/// Pure so the default and explicit opt-out can be tested without mutating
/// process-global state observed by parallel tests in this binary.
fn group_count_fast_path_enabled_from(configured: Option<&str>) -> bool {
    configured != Some("0")
}

/// Render an approximate top-K, carrying the bound and the residual with it.
///
/// The `approximation` field is set here and nowhere else, so there is exactly
/// one place where a response becomes approximate — an answer cannot acquire
/// approximation by accident, and cannot lose the label by taking a different
/// return path.
fn approximate_group_count_response(
    fp: GroupCountFastPath,
    approx: siglake_storage::iceberg::ApproximateGroupCounts,
    cost: CostReport,
) -> crate::format::RecordsResponse {
    let rows: Vec<serde_json::Value> = approx
        .rows
        .iter()
        .map(|(value, count)| {
            serde_json::json!({
                fp.group_output_column.clone(): value,
                fp.count_output_column.clone(): *count as i64,
            })
        })
        .collect();
    metrics::counter!(
        "siglake_query_approximate_group_counts_total",
        "column" => fp.group_column.clone()
    )
    .increment(1);
    crate::format::RecordsResponse {
        columns: vec![fp.group_output_column, fp.count_output_column],
        row_count: rows.len(),
        rows: serde_json::Value::Array(rows),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        stats: Some(crate::format::ScanStats {
            served_by: Some("sketch".to_string()),
            ..Default::default()
        }),
        approximation: Some(crate::format::Approximation {
            reason: format!(
                "`{}` exceeds the exact group-count cardinality cap; served from a \
                 bounded heavy-hitter summary",
                fp.group_column
            ),
            error_upper_bound: approx.error_upper_bound,
            not_counted: approx.not_counted,
            counters: approx.counters,
        }),
    }
}

/// Orders group entries for the fast path's sort and its bounded top-K.
///
/// A `Copy` struct rather than the `fn` pointer this used to be, because the
/// ordering depends on the group column's TYPE and a function pointer cannot
/// carry it. Sorting footer keys as text answered `ORDER BY status LIMIT 5` on
/// an integer column with 100, 200, 404, 500, 99 — the lexicographically
/// smallest values, not the numerically smallest.
#[derive(Debug, Clone, Copy)]
struct GroupEntryCmp {
    sort: GroupCountSort,
    kind: GroupKeyKind,
}

type GroupEntry<'k> = (Option<&'k str>, u64);

impl GroupEntryCmp {
    /// NULL order is the SQL default (ASC ⇒ NULLS LAST, DESC ⇒ NULLS FIRST —
    /// detection rejects explicit non-defaults).
    fn compare(&self, l: &GroupEntry<'_>, r: &GroupEntry<'_>) -> std::cmp::Ordering {
        let keys = |a: &GroupEntry<'_>, b: &GroupEntry<'_>| match (a.0, b.0) {
            (Some(x), Some(y)) => self.kind.cmp_keys(x, y),
            (None, None) => std::cmp::Ordering::Equal,
            // Mirrors the old `(is_none(), key)` tuple ordering: present keys
            // sort before absent ones, and the caller flips it for DESC.
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
        };
        match self.sort {
            GroupCountSort::Count { ascending: true } => l.1.cmp(&r.1).then_with(|| keys(l, r)),
            GroupCountSort::Count { ascending: false } => r.1.cmp(&l.1).then_with(|| keys(l, r)),
            GroupCountSort::Group { ascending: true } => keys(l, r),
            GroupCountSort::Group { ascending: false } => keys(r, l),
        }
    }
}

/// Append one group entry, collapsing back to the best `k` whenever the buffer
/// reaches `2k`. After a partition, `cutoff` is the worst of the retained K:
/// a candidate no better than it cannot enter the final top K. Between
/// partitions this cutoff may be stale, but only in the conservative direction
/// (the true Kth entry can improve, never worsen). Skip those candidates before
/// writing or partitioning them. Unbounded (`bound` is `None`) is a plain push.
#[inline]
fn push_bounded<'k>(
    buf: &mut Vec<(Option<&'k str>, u64)>,
    entry: (Option<&'k str>, u64),
    bound: Option<(usize, GroupEntryCmp)>,
    cutoff: &mut Option<GroupEntry<'k>>,
) {
    if let (Some((_, cmp)), Some(worst)) = (bound, *cutoff) {
        if !cmp.compare(&entry, &worst).is_lt() {
            return;
        }
    }
    buf.push(entry);
    if let Some((k, cmp)) = bound {
        if buf.len() >= k.saturating_mul(2) {
            buf.select_nth_unstable_by(k - 1, |a, b| cmp.compare(a, b));
            buf.truncate(k);
            // select_nth leaves the partition unsorted, with its Kth entry at
            // k - 1; it is exactly the worst retained entry under this comparator.
            *cutoff = Some(buf[k - 1]);
        }
    }
}

async fn try_group_count_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
    allow_approximate: AllowApproximate,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    if !group_count_fast_path_enabled() {
        return Ok(None);
    }
    // Coordinator-local only. On a distributed /shard worker (`shard` set) this
    // path would sort + apply `LIMIT` to its OWN slice before the coordinator
    // merges, corrupting a global `ORDER BY count … LIMIT n`. So a shard worker
    // returns `None` and runs the full SQL plan; the coordinator merges those
    // partials. The fast path still fires on the single-node and the
    // coordinator-local (e.g. ORDER BY/LIMIT-rooted aggregate, which classifies
    // as single-pod) paths, where it reads the whole file set and finalizes the
    // sort/limit correctly. Mirrors the date_histogram fast path.
    if shard.is_some() {
        return Ok(None);
    }
    let Some(fp) = detect_group_count_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    // The sketch preempts a Tier-1 MISS, not merely a refusal.
    //
    // The distinction decides whether any of this is reachable. On the 08-01
    // round `host` (1.15M distinct) missed Tier-1 and fell to the raw-page
    // tier, which SUCCEEDED — exactly, in ~3.1s. A sketch that only replaces a
    // refusal is therefore never consulted, and `top_hosts` was no faster than
    // before the feature existed. A cheap exact answer always wins; an
    // expensive one does not, unless the caller asked for it.
    let tier1_only = shard.is_none()
        && fp.time_bounds.is_none()
        && allow_approximate.get()
        && matches!(fp.sort, Some(GroupCountSort::Count { ascending: false }))
        && fp.limit.is_some();
    let mut exact = if tier1_only {
        ice.tier1_group_counts(&fp.table_name, &fp.group_column)
            .await
            .map_err(ApiError::internal)?
    } else {
        ice.grouped_counts_with_summary(&fp.table_name, &fp.group_column, shard, fp.time_bounds)
            .await
            .map_err(ApiError::internal)?
    };
    if exact.is_none() && tier1_only {
        // Only a whole-table top-K by descending count reaches here: a
        // Misra-Gries summary cannot answer a windowed or sharded question (it
        // is whole-table), an ascending order (the tail is exactly what it
        // discards), or an unbounded one.
        let limit = fp.limit.unwrap_or_default();
        let buffered = delta
            .map(|d| d.group_counts(&fp.table_name, &fp.group_column, fp.time_bounds.as_ref()));
        if let Some(approx) = ice
            .approximate_top_group_counts(
                &fp.table_name,
                &fp.group_column,
                limit,
                buffered.as_ref(),
            )
            .await
            .map_err(ApiError::internal)?
        {
            return Ok(Some(approximate_group_count_response(fp, approx, cost)));
        }
        // No summary either — take the full path after all, so an over-cap
        // column without a sketch is no worse off than before any of this
        // existed.
        exact = ice
            .grouped_counts_with_summary(&fp.table_name, &fp.group_column, shard, fp.time_bounds)
            .await
            .map_err(ApiError::internal)?;
    }
    let Some(rows) = exact else {
        return Ok(None);
    };
    let served_by = rows.source_label();

    // #61 hybrid: fold the buffered rows' per-value counts before sort/limit.
    // The delta is TINY (seal-age segments) while the committed rows can be
    // millions of keys, so the fold adds delta counts into the BORROWED view
    // — no re-materialization. (The first version rebuilt an owned map of
    // every committed key per query: with 20 buffered markers on a 1.1M-key
    // column, top_hosts paid ~700ms of clones+sort per request, live-found
    // in the http_logs validation round. Real deployments always have rows
    // in flight, so the delta path must be as cheap as the drained one.)
    let delta_owned: std::collections::HashMap<Option<String>, u64> = delta
        .map(|delta| delta.group_counts(&fp.table_name, &fp.group_column, fp.time_bounds.as_ref()))
        .unwrap_or_default();
    // Split NULL out so the rest keys on `&str`: the committed side now yields
    // `Option<&str>`, and `HashMap<Option<String>, _>` cannot be probed with one
    // without allocating a `String` per key — which is the cost this whole path
    // exists to avoid.
    let mut delta_nulls = 0u64;
    let mut delta_by_key: std::collections::HashMap<&str, u64> =
        std::collections::HashMap::with_capacity(delta_owned.len());
    for (k, v) in &delta_owned {
        match k {
            Some(key) => {
                delta_by_key.insert(key.as_str(), *v);
            }
            None => delta_nulls += *v,
        }
    }
    let mut matched: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(delta_by_key.len());
    let mut saw_null = false;
    // Clone-free comparators over borrowed keys; NULL order is the SQL
    // default (ASC ⇒ NULLS LAST, DESC ⇒ NULLS FIRST — detection rejects
    // explicit non-defaults).
    let cmp: Option<GroupEntryCmp> = fp.sort.map(|sort| GroupEntryCmp {
        sort,
        kind: fp.group_kind,
    });
    // Bounded top-K: with a sort and a small LIMIT the answer only ever needs
    // K entries in hand, so the full view never has to exist. Entries stream
    // into a buffer of 2K that collapses back to K by the SAME comparator the
    // unbounded path uses. `cmp` is a strict total order on distinct entries
    // (count-then-key, or key alone — and keys are distinct by construction,
    // with at most one NULL), so the bounded path returns the same K rows in
    // the same order as sorting everything; there is no tie for the unstable
    // partition to break differently. At 1.15M hosts a LIMIT-100 leaderboard
    // stops allocating and writing a ~28MB Vec per request.
    let bound = match (cmp, fp.limit) {
        (Some(cmp), Some(limit))
            if limit > 0 && limit.saturating_mul(4) < rows.len() + delta_by_key.len() =>
        {
            Some((limit, cmp))
        }
        _ => None,
    };
    let mut view: Vec<(Option<&str>, u64)> = Vec::with_capacity(match bound {
        Some((k, _)) => k.saturating_mul(2),
        None => rows.len() + delta_by_key.len() + 1,
    });
    let mut cutoff = None;
    for (k, c) in rows.iter() {
        let entry = match k {
            Some(key) => match delta_by_key.get(key) {
                Some(dv) => {
                    matched.insert(key);
                    (Some(key), c + *dv)
                }
                None => (Some(key), c),
            },
            None => {
                saw_null = true;
                (None, c + delta_nulls)
            }
        };
        push_bounded(&mut view, entry, bound, &mut cutoff);
    }
    // Buffered-only groups (not yet committed at all) join the view.
    for (k, v) in delta_by_key.iter().filter(|(k, _)| !matched.contains(*k)) {
        push_bounded(&mut view, (Some(*k), *v), bound, &mut cutoff);
    }
    if !saw_null && delta_nulls > 0 {
        push_bounded(&mut view, (None, delta_nulls), bound, &mut cutoff);
    }
    if let Some(cmp) = cmp {
        match fp.limit {
            // Top-K: O(n) partition to the K boundary, then sort only the
            // prefix — a LIMIT-100 leaderboard never sorts the 1.1M tail.
            Some(limit) if limit > 0 && limit < view.len() => {
                view.select_nth_unstable_by(limit - 1, |a, b| cmp.compare(a, b));
                view.truncate(limit);
                view.sort_unstable_by(|a, b| cmp.compare(a, b));
            }
            _ => view.sort_unstable_by(|a, b| cmp.compare(a, b)),
        }
    }
    if let Some(limit) = fp.limit {
        view.truncate(limit);
    }

    let mut json_rows = Vec::with_capacity(view.len());
    for (key, count) in view {
        let count = i64::try_from(count).map_err(|_| {
            ApiError::internal(anyhow::anyhow!("count result does not fit into i64"))
        })?;
        // Rendered as the planner's TYPE, not as the footer's string. A client
        // must not be able to tell which path served its query, and
        // `indexes.rs` already asserts `as_i64() == 200` on the planner path,
        // so the two demonstrably disagreed.
        let key = match key {
            Some(k) => fp.group_kind.render(k),
            None => serde_json::Value::Null,
        };
        json_rows.push(serde_json::json!({
            fp.group_output_column.clone(): key,
            fp.count_output_column.clone(): count,
        }));
    }

    Ok(Some(crate::format::RecordsResponse {
        columns: vec![fp.group_output_column, fp.count_output_column],
        row_count: json_rows.len(),
        rows: serde_json::Value::Array(json_rows),
        truncated: false,
        max_rows: None,
        cost: Some(cost),
        // Zero data-file scan either way, but NOT the same cost: `served_by`
        // separates the warm-metadata answer from a footer sum or raw-page
        // decode, which `rows_scanned` cannot.
        stats: Some(crate::format::ScanStats {
            served_by: Some(served_by.to_string()),
            ..Default::default()
        }),
        approximation: None,
    }))
}

/// Build the date-histogram result as an Arrow batch matching the plan's exact
/// output schema, then serialize it through the normal `batches_to_records` path
/// so the JSON (timestamp formatting, column names, order) is byte-identical to
/// the non-fast-path result. Returns `None` when the plan isn't the histogram
/// shape (caller falls through to the planner).
async fn try_date_histogram_fast_path_records(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<crate::format::RecordsResponse>, ApiError> {
    let Some(fp) = detect_date_histogram_fast_path(df.logical_plan()) else {
        return Ok(None);
    };
    let Some(mut rows) = ice
        .date_histogram_counts(
            &fp.table_name,
            fp.interval_ns,
            fp.origin_ns,
            shard,
            fp.time_bounds,
        )
        .await
        .map_err(ApiError::internal)?
    else {
        return Ok(None);
    };
    // #61 hybrid: fold the buffered rows' buckets before ordering/limit.
    if let Some(delta) = delta {
        let (lo, hi) = BufferDelta::bounds_ns(fp.time_bounds.as_ref());
        let folded = delta.histogram(&fp.table_name, fp.interval_ns, fp.origin_ns, lo, hi);
        if !folded.is_empty() {
            let mut map: std::collections::BTreeMap<i64, i64> = rows.into_iter().collect();
            for (b, c) in folded {
                *map.entry(b).or_default() += c;
            }
            rows = map.into_iter().collect();
        }
    }
    // Storage returns buckets ascending; honor an ORDER BY bucket DESC.
    if fp.bucket_sort_ascending == Some(false) {
        rows.reverse();
    }
    // Apply the LIMIT after ordering (only present alongside ORDER BY bucket, so
    // this is the deterministic first-n / last-n of the ordered buckets).
    if let Some(limit) = fp.limit {
        rows.truncate(limit);
    }

    // The plan's output schema is exactly [bucket, n] (a Sort doesn't change it),
    // so reuse it verbatim — including the bucket column's timestamp type +
    // timezone — to guarantee identical serialization.
    let arrow_schema = std::sync::Arc::new(df.schema().as_arrow().clone());
    // The bucket column follows the `timestamp` column's precision, which is
    // MICROSECONDS since the 2026-09-06 contract (it was nanoseconds before, and
    // an index whose declared time field is coarser gives coarser buckets), so
    // the storage layer's nanosecond bucket starts are scaled to match.
    let (bucket_unit, bucket_tz) = match arrow_schema.field(0).data_type() {
        arrow_schema::DataType::Timestamp(unit, tz) => (*unit, tz.clone()),
        other => {
            return Err(ApiError::internal(anyhow::anyhow!(
                "date_histogram bucket column has unexpected type {other:?}"
            )))
        }
    };
    let per_value = siglake_core::nanos_per_value(arrow_schema.field(0).data_type())
        .unwrap_or(1)
        .max(1);
    let buckets: Vec<i64> = rows
        .iter()
        .map(|(bucket, _)| bucket.div_euclid(per_value))
        .collect();
    let counts: Vec<i64> = rows.iter().map(|(_, count)| *count).collect();
    let bucket_array: arrow_array::ArrayRef = match bucket_unit {
        arrow_schema::TimeUnit::Second => std::sync::Arc::new(
            arrow_array::TimestampSecondArray::from(buckets).with_timezone_opt(bucket_tz),
        ),
        arrow_schema::TimeUnit::Millisecond => std::sync::Arc::new(
            arrow_array::TimestampMillisecondArray::from(buckets).with_timezone_opt(bucket_tz),
        ),
        arrow_schema::TimeUnit::Microsecond => std::sync::Arc::new(
            arrow_array::TimestampMicrosecondArray::from(buckets).with_timezone_opt(bucket_tz),
        ),
        arrow_schema::TimeUnit::Nanosecond => std::sync::Arc::new(
            arrow_array::TimestampNanosecondArray::from(buckets).with_timezone_opt(bucket_tz),
        ),
    };
    let count_array = arrow_array::Int64Array::from(counts);
    let batch = arrow_array::RecordBatch::try_new(
        arrow_schema,
        vec![bucket_array, std::sync::Arc::new(count_array)],
    )
    .map_err(|e| ApiError::internal(anyhow::anyhow!("build date_histogram batch: {e}")))?;

    let mut body = crate::format::batches_to_records(&[batch], None).map_err(ApiError::internal)?;
    body.cost = Some(cost);
    Ok(Some(body))
}

async fn try_date_histogram_fast_path_response(
    df: &DataFrame,
    ice: &siglake_storage::iceberg::IcebergContext,
    shard: Option<siglake_storage::ScanShard>,
    format: QueryFormat,
    cost: CostReport,
    delta: Option<&BufferDelta>,
) -> Result<Option<Response>, ApiError> {
    let Some(body) = try_date_histogram_fast_path_records(df, ice, shard, cost, delta).await?
    else {
        return Ok(None);
    };
    match format {
        QueryFormat::Records => Ok(Some((StatusCode::OK, Json(body)).into_response())),
        QueryFormat::Ndjson => {
            let rows = body.rows.as_array().ok_or_else(|| {
                ApiError::internal(anyhow::anyhow!(
                    "date_histogram fast path rows must be an array"
                ))
            })?;
            let mut payload = Vec::new();
            for row in rows {
                let line = serde_json::to_vec(row).map_err(ApiError::internal)?;
                payload.extend_from_slice(&line);
                payload.push(b'\n');
            }
            let mut resp = Response::new(Body::from(payload));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/x-ndjson"),
            );
            *resp.status_mut() = StatusCode::OK;
            Ok(Some(resp))
        }
    }
}

fn detect_count_fast_path(plan: &LogicalPlan) -> Option<CountFastPath> {
    let output_column = single_output_name(plan.schema().as_ref())?;
    let plan = match plan {
        LogicalPlan::Projection(Projection { expr, input, .. })
            if projection_is_passthrough(expr) =>
        {
            input.as_ref()
        }
        other => other,
    };

    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return None;
    };
    if !group_expr.is_empty() || aggr_expr.len() != 1 || !is_exact_count_star(&aggr_expr[0]) {
        return None;
    }

    // WHOLE FILES ONLY.
    //
    // The caller answers with `cost.estimated_rows_processed`, which is the sum
    // of the full `record_count()` of every file the scan would OPEN. That is
    // the row count only when every one of those files is counted in its
    // entirety -- i.e. when nothing narrows the scan. Any predicate, including a
    // pure time range, makes it an upper bound instead, and `cost.exact` does
    // not say otherwise: it reports whether the FILE LIST is exact, a different
    // property that stays true precisely when the estimator is confident about
    // which files a narrowed scan must open.
    //
    // A windowed count is not refused, it is redirected: every call site tries
    // this path first and falls through to `try_windowed_count_fast_path_
    // records`, which answers from the time-bucket rollups plus a bounded
    // boundary scan and is correct at the edges. So this returns None and the
    // right implementation runs.
    match input.as_ref() {
        LogicalPlan::TableScan(scan)
            // `filters` are predicates pushed INTO the scan, where they narrow
            // the same way a Filter node would while leaving no Filter node to
            // notice. Without this the arm below could be sidestepped entirely.
            if scan.filters.is_empty() && is_count_fast_path_table(scan.table_name.table()) =>
        {
            Some(CountFastPath {
                table_name: scan.table_name.table().to_string(),
                output_column,
            })
        }
        _ => None,
    }
}

fn detect_grouped_numeric_fast_path(plan: &LogicalPlan) -> Option<GroupedNumericFastPath> {
    let output_schema = plan.schema().clone();
    let mut plan = plan;
    let mut limit = None;
    if let LogicalPlan::Limit(Limit {
        skip, fetch, input, ..
    }) = plan
    {
        let skip_zero = skip.as_deref().is_none()
            || matches!(skip.as_deref(), Some(Expr::Literal(value, _)) if literal_nonnegative_usize(value) == Some(0));
        if !skip_zero {
            return None;
        }
        limit = match fetch.as_deref() {
            Some(Expr::Literal(value, _)) => literal_nonnegative_usize(value),
            None => None,
            _ => return None,
        };
        plan = input.as_ref();
    }
    let LogicalPlan::Sort(Sort {
        expr, input, fetch, ..
    }) = plan
    else {
        return None;
    };
    if expr.len() != 1 || expr[0].asc || !expr[0].nulls_first {
        return None;
    }
    let Expr::Column(sort_column) = &expr[0].expr else {
        return None;
    };
    if let Some(fetch) = fetch {
        limit = Some(limit.map_or(*fetch, |existing| existing.min(*fetch)));
    }
    plan = input.as_ref();
    let (aggregate, projection) = match plan {
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => (aggregate, Some(projection)),
            _ => return None,
        },
        LogicalPlan::Aggregate(aggregate) => (aggregate, None),
        _ => return None,
    };
    if aggregate.group_expr.len() != 1 || aggregate.aggr_expr.len() != 2 {
        return None;
    }
    let group_column = unwrap_alias_to_column(&aggregate.group_expr[0])?;
    let mut value_column = None;
    let mut avg_index = None;
    let mut count_index = None;
    for (index, expr) in aggregate.aggr_expr.iter().enumerate() {
        if is_exact_count_star(expr) {
            count_index = Some(index + 1);
        } else if let Some(column) = exact_avg_column(expr) {
            value_column = Some(column);
            avg_index = Some(index + 1);
        } else {
            return None;
        }
    }
    let (value_column, avg_index, count_index) = (value_column?, avg_index?, count_index?);
    let LogicalPlan::TableScan(scan) = aggregate.input.as_ref() else {
        return None;
    };
    if !scan.filters.is_empty() || !is_count_fast_path_table(scan.table_name.table()) {
        return None;
    }
    let internal = aggregate.schema.fields();
    if internal.len() != 3 {
        return None;
    }
    let output_names = if let Some(projection) = projection {
        if projection.expr.len() != 3 {
            return None;
        }
        for (expr, field) in projection.expr.iter().zip(internal) {
            if passthrough_column_name(expr)?.as_str() != field.name() {
                return None;
            }
        }
        projection
            .schema
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>()
    } else {
        internal
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>()
    };
    let count_output_column = output_names[count_index].clone();
    if sort_column.name != count_output_column {
        return None;
    }
    let group_output_column = output_names[0].clone();
    let group_kind = GroupKeyKind::from_data_type(
        output_schema
            .field_with_unqualified_name(&group_output_column)
            .ok()?
            .data_type(),
    )?;
    Some(GroupedNumericFastPath {
        table_name: scan.table_name.table().to_string(),
        group_column: group_column.name.clone(),
        value_column,
        group_output_column,
        avg_output_column: output_names[avg_index].clone(),
        count_output_column,
        limit,
        group_kind,
    })
}

fn exact_avg_column(expr: &Expr) -> Option<String> {
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    let Expr::AggregateFunction(aggregate) = expr else {
        return None;
    };
    if !aggregate.func.name().eq_ignore_ascii_case("avg")
        || aggregate.params.distinct
        || aggregate.params.filter.is_some()
        || !aggregate.params.order_by.is_empty()
        || aggregate.params.args.len() != 1
    {
        return None;
    }
    unwrap_alias_to_column(&aggregate.params.args[0]).map(|column| column.name.clone())
}

fn detect_group_count_fast_path(plan: &LogicalPlan) -> Option<GroupCountFastPath> {
    // Captured before the root is peeled away: this is the schema the CALLER
    // sees, so it carries the types the planner would have emitted.
    let plan_schema = plan.schema().clone();
    let mut limit = None;
    let mut sort = None;
    let mut plan = plan;

    if let LogicalPlan::Limit(Limit {
        skip, fetch, input, ..
    }) = plan
    {
        let skip_zero = skip.as_deref().is_none()
            || matches!(skip.as_deref(), Some(Expr::Literal(value, _)) if literal_nonnegative_usize(value) == Some(0));
        if !skip_zero {
            return None;
        }
        limit = match fetch.as_deref() {
            Some(Expr::Literal(value, _)) => literal_nonnegative_usize(value),
            None => None,
            _ => return None,
        };
        plan = input.as_ref();
    }

    if let LogicalPlan::Sort(Sort {
        expr, input, fetch, ..
    }) = plan
    {
        if expr.len() != 1 {
            return None;
        }
        let Expr::Column(sort_column) = &expr[0].expr else {
            return None;
        };
        // The in-memory re-sort below reproduces SQL's DEFAULT null order
        // only (ASC ⇒ NULLS LAST, DESC ⇒ NULLS FIRST); an explicit
        // non-default null order falls back to the planner.
        if expr[0].nulls_first == expr[0].asc {
            return None;
        }
        sort = Some((sort_column.name.clone(), expr[0].asc));
        if let Some(fetch) = fetch {
            limit = Some(limit.map_or(*fetch, |existing| existing.min(*fetch)));
        }
        plan = input.as_ref();
    }

    if limit.is_some() && sort.is_none() {
        return None;
    }

    let (aggregate, projection) = match plan {
        LogicalPlan::Aggregate(aggregate) => (aggregate, None),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => (aggregate, Some(projection)),
            _ => return None,
        },
        _ => return None,
    };

    if aggregate.group_expr.len() != 1
        || aggregate.aggr_expr.len() != 1
        || !is_exact_count_star(&aggregate.aggr_expr[0])
    {
        return None;
    }

    let group_column = unwrap_alias_to_column(&aggregate.group_expr[0])?;
    if matches!(group_column.name.as_str(), "timestamp" | "attributes") {
        return None;
    }
    let group_field = aggregate
        .input
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == group_column.name.as_str())?;
    // Utf8 dims and TYPED promoted columns (footers tally the latter through
    // the cast-to-Utf8 kernel, so their keys are the CAST-rendered strings
    // the query's own cast produces).
    if !matches!(
        group_field.data_type(),
        DataType::Utf8 | DataType::Int64 | DataType::Float64 | DataType::Boolean
    ) {
        return None;
    }

    // The aggregate input is either a bare `TableScan` (unfiltered fast path) or a
    // `Filter{predicate, input: TableScan}` whose predicate is a PURE time range
    // (Phase 1 windowed fast path). `extract_exact_time_bounds_from_expr` returns
    // `None` for a mixed time+dimensional filter → falls back to the planner.
    let (table_name, time_bounds) = match aggregate.input.as_ref() {
        LogicalPlan::TableScan(scan) if is_count_fast_path_table(scan.table_name.table()) => {
            (scan.table_name.table().to_string(), None)
        }
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::TableScan(scan) if is_count_fast_path_table(scan.table_name.table()) => {
                let bounds =
                    extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema())?;
                (scan.table_name.table().to_string(), Some(bounds))
            }
            _ => return None,
        },
        _ => return None,
    };

    let aggregate_fields = aggregate.schema.fields();
    if aggregate_fields.len() != 2 {
        return None;
    }
    let group_internal = aggregate_fields[0].name().to_string();
    let count_internal = aggregate_fields[1].name().to_string();

    let (group_output_column, count_output_column) = match projection {
        None => (group_internal.clone(), count_internal.clone()),
        Some(projection) => {
            if projection.expr.len() != 2 {
                return None;
            }
            let projected_group = passthrough_column_name(&projection.expr[0])?;
            let projected_count = passthrough_column_name(&projection.expr[1])?;
            if projected_group != group_internal || projected_count != count_internal {
                return None;
            }
            (
                projection.schema.fields()[0].name().to_string(),
                projection.schema.fields()[1].name().to_string(),
            )
        }
    };

    let sort = match sort {
        None => None,
        Some((column, ascending)) if column == count_output_column => {
            Some(GroupCountSort::Count { ascending })
        }
        // The leaderboard's sibling: `GROUP BY x ORDER BY x` — hit first by
        // the WS-7 attr_get-rewrite e2e, which fell to a full scan for want
        // of this arm.
        Some((column, ascending)) if column == group_output_column => {
            Some(GroupCountSort::Group { ascending })
        }
        Some(_) => return None,
    };

    // The planner's own output type for the group column, which is what this
    // path has to reproduce byte-for-byte.
    let group_kind = GroupKeyKind::from_data_type(
        plan_schema
            .field_with_unqualified_name(&group_output_column)
            .ok()?
            .data_type(),
    )?;

    Some(GroupCountFastPath {
        table_name,
        group_column: group_column.name.clone(),
        group_output_column,
        count_output_column,
        sort,
        limit,
        time_bounds,
        group_kind,
    })
}

#[derive(Debug, Clone)]
struct DateHistogramFastPath {
    table_name: String,
    interval_ns: i64,
    origin_ns: i64,
    /// `Some(ascending)` when an `ORDER BY bucket` is present, else `None`. The
    /// output column names/types come from the plan's own schema at serialization
    /// time, so they aren't carried here.
    bucket_sort_ascending: Option<bool>,
    /// `Some(n)` for a top-level `LIMIT n` (only honored alongside an
    /// `ORDER BY bucket`, so the truncation is deterministic); applied to the
    /// ordered buckets after the stats path produces them.
    limit: Option<usize>,
    /// `Some(bounds)` for a windowed `WHERE timestamp ∈ [lo,hi)` histogram
    /// (Phase 1) — a *pure* time range. `None` for the unfiltered fast path.
    time_bounds: Option<siglake_storage::iceberg::TimeBounds>,
}

/// Detect `SELECT date_bin(<interval>, timestamp, <origin>) AS bucket,
/// count(*) AS n FROM <table> GROUP BY bucket [ORDER BY bucket [ASC|DESC]]` —
/// the date-histogram shape. v1 requires a bare table scan (no `WHERE`); a
/// filtered histogram falls back to the planner. The stride must be a pure
/// time interval (no month component — `date_bin` rejects months anyway).
fn detect_date_histogram_fast_path(plan: &LogicalPlan) -> Option<DateHistogramFastPath> {
    let mut limit: Option<usize> = None;
    let mut sort: Option<(String, bool)> = None;
    let mut plan = plan;

    // The canonical dashboard shape is `... ORDER BY bucket LIMIT n` — unwrap an
    // optional top-level `LIMIT` (skip must be zero; a non-zero offset can't be
    // re-applied after the stats path) just like the group-count fast path.
    if let LogicalPlan::Limit(Limit {
        skip, fetch, input, ..
    }) = plan
    {
        let skip_zero = skip.as_deref().is_none()
            || matches!(skip.as_deref(), Some(Expr::Literal(value, _)) if literal_nonnegative_usize(value) == Some(0));
        if !skip_zero {
            return None;
        }
        limit = match fetch.as_deref() {
            Some(Expr::Literal(value, _)) => literal_nonnegative_usize(value),
            None => None,
            _ => return None,
        };
        plan = input.as_ref();
    }

    if let LogicalPlan::Sort(Sort {
        expr, input, fetch, ..
    }) = plan
    {
        if expr.len() != 1 {
            return None;
        }
        let Expr::Column(sort_column) = &expr[0].expr else {
            return None;
        };
        sort = Some((sort_column.name.clone(), expr[0].asc));
        // DataFusion can fuse the LIMIT into the Sort as `fetch`; take the tighter.
        if let Some(fetch) = fetch {
            limit = Some(limit.map_or(*fetch, |existing| existing.min(*fetch)));
        }
        plan = input.as_ref();
    }

    // A LIMIT without an ORDER BY truncates to arbitrary buckets in DataFusion;
    // the stats path yields them in bucket order, which wouldn't match. Fall back.
    if limit.is_some() && sort.is_none() {
        return None;
    }

    let (aggregate, projection) = match plan {
        LogicalPlan::Aggregate(aggregate) => (aggregate, None),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate) => (aggregate, Some(projection)),
            _ => return None,
        },
        _ => return None,
    };

    if aggregate.group_expr.len() != 1
        || aggregate.aggr_expr.len() != 1
        || !is_exact_count_star(&aggregate.aggr_expr[0])
    {
        return None;
    }
    let (interval_ns, origin_ns) = parse_date_bin_over_timestamp(&aggregate.group_expr[0])
        .or_else(|| parse_date_trunc_over_timestamp(&aggregate.group_expr[0]))?;

    // The aggregate input is either a bare `TableScan` (unfiltered) or a
    // `Filter{predicate, input: TableScan}` whose predicate is a PURE time range
    // (Phase 1 windowed histogram). A mixed time+dimensional filter →
    // `extract_exact_time_bounds_from_expr` returns `None` → falls back.
    let (table_name, time_bounds) = match aggregate.input.as_ref() {
        LogicalPlan::TableScan(scan) if is_count_fast_path_table(scan.table_name.table()) => {
            (scan.table_name.table().to_string(), None)
        }
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::TableScan(scan) if is_count_fast_path_table(scan.table_name.table()) => {
                let bounds =
                    extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema())?;
                (scan.table_name.table().to_string(), Some(bounds))
            }
            _ => return None,
        },
        _ => return None,
    };

    let aggregate_fields = aggregate.schema.fields();
    if aggregate_fields.len() != 2 {
        return None;
    }
    let group_internal = aggregate_fields[0].name().to_string();
    let count_internal = aggregate_fields[1].name().to_string();

    let (bucket_output_column, count_output_column) = match projection {
        None => (group_internal.clone(), count_internal.clone()),
        Some(projection) => {
            if projection.expr.len() != 2 {
                return None;
            }
            let projected_group = passthrough_column_name(&projection.expr[0])?;
            let projected_count = passthrough_column_name(&projection.expr[1])?;
            if projected_group != group_internal || projected_count != count_internal {
                return None;
            }
            (
                projection.schema.fields()[0].name().to_string(),
                projection.schema.fields()[1].name().to_string(),
            )
        }
    };

    // An ORDER BY must target the bucket column (the only sort the merge-free
    // stats path can satisfy by reversing the already-ascending output).
    let bucket_sort_ascending = match sort {
        Some((column, ascending)) if column == bucket_output_column => Some(ascending),
        Some(_) => return None,
        None => None,
    };
    // `count_output_column` was validated above (projection passthrough); the
    // output schema is taken from the plan at serialization time.
    let _ = count_output_column;

    Some(DateHistogramFastPath {
        table_name,
        interval_ns,
        origin_ns,
        bucket_sort_ascending,
        limit,
        time_bounds,
    })
}

/// Parse `date_bin(<interval>, timestamp, [<origin>])` → `(interval_ns,
/// origin_ns)`. Returns `None` unless arg 2 is the bare `timestamp` column, the
/// stride is a month-free time interval, and the origin (default epoch) is a
/// timestamp literal.
fn parse_date_bin_over_timestamp(expr: &Expr) -> Option<(i64, i64)> {
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if call.func.name() != "date_bin" {
        return None;
    }
    if call.args.len() != 2 && call.args.len() != 3 {
        return None;
    }
    let interval_ns = interval_literal_ns(&call.args[0])?;
    if !expr_is_timestamp_column(&call.args[1]) {
        return None;
    }
    let origin_ns = match call.args.get(2) {
        Some(origin) => timestamp_literal_ns(origin)?,
        None => 0, // date_bin's default origin is the Unix epoch (1970-01-01Z).
    };
    Some((interval_ns, origin_ns))
}

/// Parse `date_trunc(<unit>, timestamp)` → `(interval_ns, origin_ns = 0)` for the
/// FIXED-width, epoch-aligned units. `second`/`minute`/`hour`/`day` truncate to
/// multiples of their width from the Unix epoch (top-of-hour / midnight-UTC
/// aligned), so each is exactly `date_bin(width, ts, epoch)` and the existing
/// bucket machinery (`origin = 0`) is correct. `week` (Monday-aligned, not an
/// epoch multiple) and the calendar units (`month`/`quarter`/`year`, variable
/// width) are NOT fixed buckets → `None`, so they fall back to full SQL.
fn parse_date_trunc_over_timestamp(expr: &Expr) -> Option<(i64, i64)> {
    use datafusion::scalar::ScalarValue;
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    if call.func.name() != "date_trunc" || call.args.len() != 2 {
        return None;
    }
    let mut unit_expr = &call.args[0];
    while let Expr::Alias(alias) = unit_expr {
        unit_expr = alias.expr.as_ref();
    }
    let Expr::Literal(unit, _) = unit_expr else {
        return None;
    };
    let unit = match unit {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.to_ascii_lowercase(),
        _ => return None,
    };
    if !expr_is_timestamp_column(&call.args[1]) {
        return None;
    }
    let interval_ns: i64 = match unit.as_str() {
        "second" => 1_000_000_000,
        "minute" => 60_000_000_000,
        "hour" => 3_600_000_000_000,
        "day" => 86_400_000_000_000,
        // `week`/`month`/`quarter`/`year`: not a fixed epoch-aligned width.
        _ => return None,
    };
    Some((interval_ns, 0))
}

fn expr_is_timestamp_column(expr: &Expr) -> bool {
    let mut expr = expr;
    loop {
        match expr {
            Expr::Alias(alias) => expr = alias.expr.as_ref(),
            Expr::Cast(cast) => expr = cast.expr.as_ref(),
            Expr::Column(column) => return column.name == "timestamp",
            _ => return false,
        }
    }
}

/// Total nanoseconds of a month-free interval literal; `None` if it carries a
/// month component (`date_bin` rejects calendar months) or isn't an interval.
fn interval_literal_ns(expr: &Expr) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    const NS_PER_DAY: i64 = 86_400_000_000_000;
    let mut expr = expr;
    while let Expr::Alias(alias) = expr {
        expr = alias.expr.as_ref();
    }
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            if v.months != 0 {
                return None;
            }
            (v.days as i64)
                .checked_mul(NS_PER_DAY)?
                .checked_add(v.nanoseconds)
        }
        ScalarValue::IntervalDayTime(Some(v)) => (v.days as i64)
            .checked_mul(NS_PER_DAY)?
            .checked_add((v.milliseconds as i64).checked_mul(1_000_000)?),
        _ => None,
    }
}

/// Nanoseconds of a timestamp literal (any precision), scaled to ns.
fn timestamp_literal_ns(expr: &Expr) -> Option<i64> {
    use datafusion::scalar::ScalarValue;
    let mut expr = expr;
    loop {
        match expr {
            Expr::Alias(alias) => expr = alias.expr.as_ref(),
            Expr::Cast(cast) => expr = cast.expr.as_ref(),
            Expr::Literal(value, _) => {
                return match value {
                    ScalarValue::TimestampNanosecond(Some(v), _) => Some(*v),
                    ScalarValue::TimestampMicrosecond(Some(v), _) => v.checked_mul(1_000),
                    ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000_000),
                    ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000_000),
                    // date_bin accepts a string origin (e.g. '1970-01-01T00:00:00Z')
                    // and coerces it; parse RFC3339 to nanoseconds ourselves.
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .and_then(|dt| dt.timestamp_nanos_opt()),
                    _ => None,
                };
            }
            _ => return None,
        }
    }
}

fn single_output_name(schema: &DFSchema) -> Option<String> {
    let fields = schema.fields();
    if fields.len() != 1 {
        return None;
    }
    Some(fields[0].name().to_string())
}

fn projection_is_passthrough(exprs: &[Expr]) -> bool {
    if exprs.len() != 1 {
        return false;
    }
    expr_is_passthrough_column(&exprs[0])
}

fn passthrough_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Alias(alias) => passthrough_column_name(&alias.expr),
        // WS-7 typed promotion: `attr_get` over a typed column rewrites to
        // `CAST(col AS VARCHAR)`, and the group-count footers tally typed
        // columns through the SAME cast kernel — the rendered strings are the
        // footer keys, so the cast is transparent to the footer battery.
        Expr::Cast(cast)
            if matches!(
                cast.data_type,
                arrow_schema::DataType::Utf8
                    | arrow_schema::DataType::Utf8View
                    | arrow_schema::DataType::LargeUtf8
            ) =>
        {
            passthrough_column_name(&cast.expr)
        }
        _ => None,
    }
}

fn unwrap_alias_to_column(expr: &Expr) -> Option<&datafusion::common::Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => unwrap_alias_to_column(&alias.expr),
        Expr::Cast(cast)
            if matches!(
                cast.data_type,
                arrow_schema::DataType::Utf8
                    | arrow_schema::DataType::Utf8View
                    | arrow_schema::DataType::LargeUtf8
            ) =>
        {
            unwrap_alias_to_column(&cast.expr)
        }
        _ => None,
    }
}

fn expr_is_passthrough_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias(alias) => expr_is_passthrough_column(&alias.expr),
        Expr::Cast(cast)
            if matches!(
                cast.data_type,
                arrow_schema::DataType::Utf8
                    | arrow_schema::DataType::Utf8View
                    | arrow_schema::DataType::LargeUtf8
            ) =>
        {
            expr_is_passthrough_column(&cast.expr)
        }
        _ => false,
    }
}

fn is_exact_count_star(expr: &Expr) -> bool {
    let Expr::AggregateFunction(agg) = expr else {
        return false;
    };
    agg.func.name().eq_ignore_ascii_case("count")
        && !agg.params.distinct
        && agg.params.filter.is_none()
        && agg.params.order_by.is_empty()
        && agg.params.args.len() == 1
        && aggregate_arg_is_total_row_count(&agg.params.args[0])
}

/// The count / group-count / date-histogram fast paths apply to any real
/// Iceberg table scan. They were originally gated to the built-in tables, but
/// every siglake table — user-managed indexes included — is produced by the
/// same write path (group-count footer summaries, the snapshot group-count
/// aggregate, a `timestamp` sort column), so the name gate needlessly excluded
/// user indexes, which are the primary analytics target (e.g. `logs-bench`).
/// A table that happens to lack the structure a given fast path needs is
/// handled gracefully downstream: the executor returns `None` and the full SQL
/// plan runs. The WAL-buffer correctness guard for uncommitted `events` rows
/// lives at the call site (`handle_local_inner`), not here.
fn is_count_fast_path_table(name: &str) -> bool {
    !name.is_empty()
}

fn aggregate_arg_is_total_row_count(expr: &Expr) -> bool {
    match expr {
        #[allow(deprecated)]
        Expr::Wildcard { .. } => true,
        Expr::Literal(value, _) => !value.is_null(),
        _ => false,
    }
}

fn literal_nonnegative_usize(value: &datafusion::scalar::ScalarValue) -> Option<usize> {
    use datafusion::scalar::ScalarValue::*;
    match value {
        Int64(Some(value)) if *value >= 0 => Some(*value as usize),
        UInt64(Some(value)) => Some(*value as usize),
        Int32(Some(value)) if *value >= 0 => Some(*value as usize),
        UInt32(Some(value)) => Some(*value as usize),
        _ => None,
    }
}

async fn register_all_tables(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
) -> Result<usize, ApiError> {
    ice.register_with_datafusion(ctx)
        .await
        .map_err(ApiError::internal)?;
    ice.register_query_audit_with_datafusion(ctx)
        .await
        .map_err(ApiError::internal)?;
    // WI-1 managed indexes (`events` is excluded internally — it is already
    // registered above through the canonical WS-6-aware path).
    let indexes = ice
        .register_indexes_with_datafusion(ctx)
        .await
        .map_err(ApiError::internal)?;
    crate::udfs::register_udfs(ctx);
    Ok(6 + indexes)
}

/// Marker error: a referenced table is neither a system table nor a managed
/// index. Surfaces as a 400 (user error), not a 500 — same contract as the
/// planner's own "table not found".
#[derive(Debug)]
struct UnknownQueryTable(String);

impl std::fmt::Display for UnknownQueryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown query table `{}` (not a managed index)", self.0)
    }
}

impl std::error::Error for UnknownQueryTable {}

fn registration_error_to_api(err: anyhow::Error) -> ApiError {
    if err.downcast_ref::<UnknownQueryTable>().is_some() {
        ApiError::bad_request(err.to_string())
    } else {
        ApiError::internal(err)
    }
}

async fn register_tables_for_query(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
    query: &str,
) -> Result<usize, ApiError> {
    let Some(tables) = referenced_tables(query) else {
        return register_all_tables(ice, ctx).await;
    };
    register_selected_tables(ice, ctx, &tables)
        .await
        .map_err(registration_error_to_api)
}

async fn register_tables_for_query_batch(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
    query: &str,
) -> anyhow::Result<usize> {
    let Some(tables) = referenced_tables(query) else {
        return register_all_tables(ice, ctx)
            .await
            .map_err(|e| anyhow::anyhow!(e.msg));
    };
    register_selected_tables(ice, ctx, &tables).await
}

async fn register_selected_tables(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
    tables: &[String],
) -> anyhow::Result<usize> {
    for table in tables {
        match table.as_str() {
            "events" => ice.register_with_datafusion(ctx).await?,
            "query_audit" => ice.register_query_audit_with_datafusion(ctx).await?,
            // Anything else may be a WI-1 managed index — register it
            // dynamically; a name that is neither errors here (clearer than
            // the planner's "table not found").
            other => {
                if !ice.register_index_with_datafusion(ctx, other).await? {
                    return Err(UnknownQueryTable(other.to_string()).into());
                }
            }
        }
    }
    crate::udfs::register_udfs(ctx);
    metrics::histogram!("siglake_query_registered_tables").record(tables.len() as f64);
    Ok(tables.len())
}

/// WS-6: replace the registered `events` Iceberg provider with a
/// [`UnionEventsProvider`] over the un-committed WAL segments, so a query sees
/// just-ingested rows before the compaction commit. No-op when `events` isn't
/// registered for this query. Best-effort — a buffer build error is logged and
/// the query falls back to the Iceberg-only view rather than failing.
async fn override_events_with_wal_buffer(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
    buffer_root: &std::path::Path,
    tenant: Option<&str>,
) -> Result<(), ApiError> {
    if !ctx.table_exist("events").unwrap_or(false) {
        return Ok(());
    }
    let (base, consumed) = ice
        .events_provider_with_consumed()
        .await
        .map_err(ApiError::internal)?;
    let wal_dir =
        crate::wal_buffer::resolve_tenant_wal_dir(buffer_root, tenant.unwrap_or("default"));
    match crate::wal_buffer::UnionEventsProvider::try_new(base, &wal_dir, &consumed) {
        Ok(provider) => {
            // Replace the plain Iceberg `events` registration with the union; the
            // provider reads the WAL lazily in `scan` (filter-aware time pruning),
            // emitting `siglake_query_wal_buffer_used_total` when it unions rows.
            ctx.deregister_table("events")
                .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
            ctx.register_table("events", std::sync::Arc::new(provider))
                .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
        }
        Err(e) => {
            metrics::counter!("siglake_query_wal_buffer_build_errors_total").increment(1);
            tracing::warn!(error = %e, dir = %wal_dir.display(),
                "WAL buffer union build failed; serving Iceberg-only view");
        }
    }
    Ok(())
}

/// #61 hybrid serving: the buffered (sealed-but-uncommitted) rows of every
/// table a query references, loaded ONCE per request and folded into the
/// metadata-served fast paths as a DELTA. The original design DISABLED the
/// fast paths whenever the buffer held rows — but the union plan a whole-table
/// aggregate then takes is a full committed-file scan, so a single 5-row
/// marker segment degraded ~1ms answers into breaker-tripping scans (the
/// Phase-4 rounds measured exactly that). Buffered rows are bounded (a few
/// seal-age segments), so folding them in memory keeps the fast paths exact
/// AND fast while rows are in flight.
struct BufferDelta {
    /// table → buffered batches, aligned to the table schema (carrier-mapped
    /// for user indexes) with already-committed segments excluded.
    batches: std::collections::HashMap<String, Vec<arrow_array::RecordBatch>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CountBufferObservation {
    snapshot_id: Option<i64>,
    manifest_rows: Option<u64>,
    buffer_within_budget: bool,
    buffer_bytes: u64,
    included_buffer_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WholeTableCountObservation {
    snapshot_id: Option<i64>,
    manifest_rows: u64,
    buffer_within_budget: Option<bool>,
    buffer_bytes: Option<u64>,
    included_buffer_rows: u64,
    response_path: &'static str,
    count: u64,
}

struct BufferDeltaLoad {
    delta: Option<BufferDelta>,
    counts: std::collections::HashMap<String, CountBufferObservation>,
    /// The WAL listing every classification below was drawn from — see
    /// [`BufferProof`].
    proof: BufferProof,
}

impl BufferDeltaLoad {
    fn delta(&self) -> Option<&BufferDelta> {
        self.delta.as_ref()
    }

    fn is_empty(&self) -> bool {
        self.delta.is_none()
    }

    /// Whether the load DECLINED to read some table's buffer because it was
    /// past the decode budget. Such a load carries no batches for that table,
    /// which is indistinguishable from a drained buffer by `is_empty` alone —
    /// and it is the opposite of proof that nothing is in flight.
    fn declined_a_buffer(&self) -> bool {
        self.counts
            .values()
            .any(|state| !state.buffer_within_budget)
    }
}

/// What a request managed to establish about the WAL buffer's contribution to
/// its tables.
///
/// THE DEFECT THIS CLOSES. A failed delta load produced `(false, None)`, and
/// the result-cache gate read that `None` through `is_none_or` as "no buffered
/// rows". Skipping the fast paths was right; staying cache-eligible was not.
/// An UNKNOWN delta is not a proven-empty one: a hit could serve an answer that
/// hides buffered rows, and an insert could freeze a body whose snapshot purity
/// was never established, outliving the failure that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferDeltaState {
    /// No buffer configured, or a load that proved the delta empty. The only
    /// state in which a snapshot-keyed result-cache entry is sound.
    Empty,
    /// A load that found buffered rows for this query's tables.
    NonEmpty,
    /// The buffer was past the decode budget, so the load did not read it.
    /// Bounded-staleness degradation for the fast paths (committed-only,
    /// exactly as designed), but NOT a proof of emptiness — see below.
    OverBudget,
    /// The load failed. Nothing is known about what is in flight.
    Unknown,
}

impl BufferDeltaState {
    /// The committed-only fast paths need a delta they can fold in; `Empty`
    /// and `NonEmpty` supply one, and `OverBudget` deliberately folds in
    /// nothing (the backlog guard: rows appear as the drain commits).
    /// `Unknown` cannot be exact, so the union plan serves the request instead.
    fn fast_paths_safe(self) -> bool {
        !matches!(self, Self::Unknown)
    }

    /// Result-cache lookups AND insertions both require a proven-empty delta.
    ///
    /// `OverBudget` is NOT that proof, though it produces the same empty
    /// `BufferDeltaLoad` as a drained buffer. The two buffer gates price
    /// different things: `load_buffer_delta` sizes the WHOLE buffer
    /// (`time_bound: None`), while `UnionEventsProvider::scan` sizes only the
    /// segments the query's `timestamp` window keeps. During backlog recovery a
    /// windowed query is therefore routinely served a union INCLUDING buffered
    /// rows while this gate believed the WAL contributed nothing — freezing
    /// those rows into a `(table, snapshot, query)` entry that later identical
    /// requests keep hitting (the buffer is still over budget, so the delta
    /// keeps "proving" empty) until the drain commit moves the snapshot. The
    /// backlog window is exactly when the buffer changes fastest.
    ///
    /// Record and return whether this state bypasses the result cache. Keep the
    /// match exhaustive and the label values literal: a new buffer state must
    /// not silently fold into `unknown`, disappear from cache metrics, or hide
    /// its series vocabulary from the alert pre-registration check.
    fn record_result_cache_skip(self) -> bool {
        match self {
            Self::Empty => false,
            Self::NonEmpty => {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "skip_buffer_nonempty"
                )
                .increment(1);
                true
            }
            Self::OverBudget => {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "skip_buffer_over_budget"
                )
                .increment(1);
                true
            }
            Self::Unknown => {
                metrics::counter!(
                    "siglake_query_sql_result_cache_requests_total",
                    "outcome" => "skip_buffer_unknown"
                )
                .increment(1);
                true
            }
        }
    }
}

/// The WAL state a request's buffer classification was actually taken over:
/// for every table whose buffer the load listed, the directory, the
/// committed-segment exclusion, and the surviving segment basenames.
///
/// THE DEFECT THIS CLOSES (#1562). `BufferDeltaState::Empty` is a statement
/// about a listing taken at the TOP of the request, but
/// `UnionEventsProvider::scan` lists the same directory AFRESH at execution
/// time — by design, since that is how a `timestamp` predicate prunes segments
/// by header. A segment that seals in between is unioned into the body, and
/// `finish_result_cache` then froze that body under a key whose eligibility
/// rests on the WAL contributing nothing. Nothing re-checked at insert time, so
/// the entry contradicted itself on its face; only the drain commit moving the
/// snapshot ever removed it.
///
/// An empty witness list is the vacuous proof and holds by construction: no
/// buffer configured, or a table that is not an ingest target (a detection or
/// audit table has no WAL directory), so nothing can seal into it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BufferProof {
    tables: Vec<WitnessedBuffer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WitnessedBuffer {
    dir: std::path::PathBuf,
    /// The consumed (already-committed) segment names the listing excluded. A
    /// drain that commits between the proof and the insert grows this set AND
    /// shrinks the listing, so either half moving is enough to fail the re-check.
    exclude: Arc<std::collections::HashSet<String>>,
    names: Vec<String>,
    /// #2661: the table uuid the listing was taken for, so the re-check applies
    /// the same owner gate. A directory refused as a dropped incarnation's is
    /// still witnessed — with an empty listing — because the drain can
    /// quarantine and re-stamp it at any moment, after which it starts
    /// contributing again and this proof must stop holding.
    owner: Option<String>,
}

impl BufferProof {
    /// Re-list every witnessed directory: does this proof still describe the
    /// WAL?
    ///
    /// An unreadable directory is NOT a match, for the same reason
    /// `BufferDeltaState::Unknown` is ineligible — a listing that failed is not
    /// a listing that agreed. One `read_dir` per witnessed table (no segment is
    /// opened, no header read), on a directory this request has already listed
    /// once, and only on a cacheable-shape MISS whose delta was proven empty: a
    /// deep backlog classifies `OverBudget` and never gets here.
    fn still_holds(&self) -> bool {
        self.tables.iter().all(|witness| {
            crate::wal_buffer::owned_uncommitted_segment_names(
                &witness.dir,
                &witness.exclude,
                witness.owner.as_deref(),
            )
            .is_ok_and(|names| names == witness.names)
        })
    }
}

/// The proof a delta load carries. A load that never happened (`None` — no
/// buffer configured, or a failed load whose state is `Unknown` and therefore
/// ineligible anyway) carries the vacuous one.
fn buffer_proof_of(load: Option<&BufferDeltaLoad>) -> BufferProof {
    load.map(|load| load.proof.clone()).unwrap_or_default()
}

/// Load this request's buffer delta and classify what the load proved. `None`
/// for `buffer_root` is the unbuffered deployment: nothing can be in flight, so
/// the delta is empty by construction.
async fn resolve_buffer_delta(
    ice: &siglake_storage::iceberg::IcebergContext,
    buffer_root: Option<&std::path::Path>,
    cache: &Arc<crate::wal_buffer::BufferDeltaCache>,
    identity: &CallerIdentity,
    query: &str,
) -> (BufferDeltaState, Option<BufferDeltaLoad>) {
    resolve_buffer_delta_with_max(ice, buffer_root, cache, identity, query, None).await
}

/// [`resolve_buffer_delta`] with the decode ceiling injected, so the backlog
/// classification is testable without a 2 GiB fixture (the sibling of
/// [`load_buffer_delta_with_max`]).
async fn resolve_buffer_delta_with_max(
    ice: &siglake_storage::iceberg::IcebergContext,
    buffer_root: Option<&std::path::Path>,
    cache: &Arc<crate::wal_buffer::BufferDeltaCache>,
    identity: &CallerIdentity,
    query: &str,
    max_bytes: Option<u64>,
) -> (BufferDeltaState, Option<BufferDeltaLoad>) {
    let Some(buffer_root) = buffer_root else {
        return (BufferDeltaState::Empty, None);
    };
    match load_buffer_delta_with_max(ice, buffer_root, cache, identity, query, max_bytes).await {
        // Checked BEFORE `is_empty`: a refused buffer yields no batches, so the
        // emptiness test cannot tell the two apart, and only one of them is a
        // proof. A load that refused ONE table's buffer and read another's is
        // still unproven for the query as a whole.
        Ok(load) if load.declined_a_buffer() => (BufferDeltaState::OverBudget, Some(load)),
        Ok(load) if load.is_empty() => (BufferDeltaState::Empty, Some(load)),
        Ok(load) => (BufferDeltaState::NonEmpty, Some(load)),
        Err(err) => {
            tracing::warn!(error = ?err,
                "buffer delta load failed; skipping fast paths and result caching this request");
            (BufferDeltaState::Unknown, None)
        }
    }
}

impl BufferDelta {
    fn table(&self, table: &str) -> &[arrow_array::RecordBatch] {
        self.batches.get(table).map(Vec::as_slice).unwrap_or(&[])
    }

    fn bounds_ns(
        bounds: Option<&siglake_storage::iceberg::TimeBounds>,
    ) -> (Option<i64>, Option<i64>) {
        match bounds {
            None => (None, None),
            Some(b) => (
                b.start.and_then(|t| t.timestamp_nanos_opt()),
                b.end.and_then(|t| t.timestamp_nanos_opt()),
            ),
        }
    }

    fn ts_in(ts: i64, lo: Option<i64>, hi: Option<i64>) -> bool {
        lo.is_none_or(|lo| ts >= lo) && hi.is_none_or(|hi| ts < hi)
    }

    /// Buffered row count for `table` within optional time bounds.
    fn rows(&self, table: &str, bounds: Option<&siglake_storage::iceberg::TimeBounds>) -> u64 {
        use arrow_array::Array;
        let (lo, hi) = Self::bounds_ns(bounds);
        let mut n = 0u64;
        for b in self.table(table) {
            if lo.is_none() && hi.is_none() {
                n += b.num_rows() as u64;
                continue;
            }
            let Ok(idx) = b.schema().index_of(siglake_core::nanos_source_column(
                b.schema().as_ref(),
                "timestamp",
            )) else {
                continue;
            };
            let Some(ts) = siglake_core::column_nanos(b.column(idx)) else {
                continue;
            };
            for i in 0..ts.len() {
                if ts.is_valid(i) && Self::ts_in(ts.value(i), lo, hi) {
                    n += 1;
                }
            }
        }
        n
    }

    /// Buffered per-value counts of `column` for `table` within bounds
    /// (`None` key = NULL group), foldable into `GroupCountRows`.
    fn group_counts(
        &self,
        table: &str,
        column: &str,
        bounds: Option<&siglake_storage::iceberg::TimeBounds>,
    ) -> std::collections::HashMap<Option<String>, u64> {
        use arrow_array::Array;
        let (lo, hi) = Self::bounds_ns(bounds);
        let mut out: std::collections::HashMap<Option<String>, u64> =
            std::collections::HashMap::new();
        for b in self.table(table) {
            let Ok(cidx) = b.schema().index_of(column) else {
                continue;
            };
            // Typed dimension columns (Int64/Float64/Boolean promoted or
            // index-mapped) render into the same key space as the footer
            // tally via the identical cast kernel; string columns pass
            // through untouched.
            let cast_storage;
            let raw = b.column(cidx);
            let col = match raw.as_any().downcast_ref::<arrow_array::StringArray>() {
                Some(s) => s,
                None => {
                    let Ok(cast) =
                        datafusion::arrow::compute::cast(raw, &arrow_schema::DataType::Utf8)
                    else {
                        continue;
                    };
                    cast_storage = cast;
                    let Some(s) = cast_storage
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                    else {
                        continue;
                    };
                    s
                }
            };
            let ts = b
                .schema()
                .index_of(siglake_core::nanos_source_column(
                    b.schema().as_ref(),
                    "timestamp",
                ))
                .ok()
                .and_then(|i| siglake_core::column_nanos(b.column(i)));
            for i in 0..col.len() {
                if lo.is_some() || hi.is_some() {
                    let Some(ts) = ts.as_ref() else { continue };
                    if !ts.is_valid(i) || !Self::ts_in(ts.value(i), lo, hi) {
                        continue;
                    }
                }
                let key = col.is_valid(i).then(|| col.value(i).to_string());
                *out.entry(key).or_default() += 1;
            }
        }
        out
    }

    /// Buffered `date_bin` bucket counts for `table` within `[lo, hi)`.
    fn histogram(
        &self,
        table: &str,
        interval_ns: i64,
        origin_ns: i64,
        lo: Option<i64>,
        hi: Option<i64>,
    ) -> std::collections::BTreeMap<i64, i64> {
        use arrow_array::Array;
        let mut out = std::collections::BTreeMap::new();
        if interval_ns <= 0 {
            return out;
        }
        for b in self.table(table) {
            let Ok(idx) = b.schema().index_of(siglake_core::nanos_source_column(
                b.schema().as_ref(),
                "timestamp",
            )) else {
                continue;
            };
            let Some(ts) = siglake_core::column_nanos(b.column(idx)) else {
                continue;
            };
            for i in 0..ts.len() {
                if !ts.is_valid(i) {
                    continue;
                }
                let t = ts.value(i);
                if !Self::ts_in(t, lo, hi) {
                    continue;
                }
                let bucket = origin_ns + (t - origin_ns).div_euclid(interval_ns) * interval_ns;
                *out.entry(bucket).or_default() += 1;
            }
        }
        out
    }
}

/// Load the buffered rows for every table `query` references. The returned
/// count observations retain the snapshot and budget decision even when no
/// delta is included, so whole-table counts can use one coherent base/buffer
/// generation and explain committed-only fallbacks.
async fn load_buffer_delta(
    ice: &siglake_storage::iceberg::IcebergContext,
    buffer_root: &std::path::Path,
    cache: &Arc<crate::wal_buffer::BufferDeltaCache>,
    identity: &CallerIdentity,
    query: &str,
) -> Result<BufferDeltaLoad, ApiError> {
    load_buffer_delta_with_max(ice, buffer_root, cache, identity, query, None).await
}

/// [`load_buffer_delta`] with an injected byte ceiling for hermetic backlog
/// regressions. Production passes `None` and reads the configured ceiling.
async fn load_buffer_delta_with_max(
    ice: &siglake_storage::iceberg::IcebergContext,
    buffer_root: &std::path::Path,
    cache: &Arc<crate::wal_buffer::BufferDeltaCache>,
    identity: &CallerIdentity,
    query: &str,
    max_bytes: Option<u64>,
) -> Result<BufferDeltaLoad, ApiError> {
    let Some(tables) = referenced_tables(query) else {
        // Unparseable table list: no delta, and the caller's detect fns will
        // simply not match anything unusual — conservative but non-fatal.
        return Ok(BufferDeltaLoad {
            delta: None,
            counts: Default::default(),
            proof: BufferProof::default(),
        });
    };
    let tenant = identity.tenant.as_deref().unwrap_or("default");
    let mut batches: std::collections::HashMap<String, Vec<arrow_array::RecordBatch>> =
        std::collections::HashMap::new();
    let mut counts = std::collections::HashMap::new();
    let mut proof = BufferProof::default();
    for name in tables {
        let (dir, snapshot, carrier, owner) = if name == "events" {
            let snapshot = ice
                .events_provider_with_consumed_snapshot()
                .await
                .map_err(ApiError::internal)?;
            (
                crate::wal_buffer::resolve_tenant_wal_dir(buffer_root, tenant),
                snapshot,
                None,
                None,
            )
        } else {
            match ice
                .index_provider_with_consumed_snapshot(&name)
                .await
                .map_err(ApiError::internal)?
            {
                Some((snapshot, config)) => {
                    let owner = snapshot.table_uuid.clone();
                    (
                        crate::wal_buffer::resolve_index_wal_dir(buffer_root, tenant, &name),
                        snapshot,
                        Some(config),
                        Some(owner),
                    )
                }
                None => continue, // not a managed index — never buffered
            }
        };
        // #2661: a directory whose owner marker names a table other than this
        // one holds a DROPPED incarnation's segments. It contributes nothing,
        // but it is still witnessed (with the empty listing the gate produces)
        // so a later quarantine + re-stamp invalidates this proof.
        if owner
            .as_deref()
            .is_some_and(|uuid| !crate::wal_buffer::wal_dir_serves_table(&dir, uuid))
        {
            proof.tables.push(WitnessedBuffer {
                dir,
                exclude: snapshot.consumed.clone(),
                names: Vec::new(),
                owner,
            });
            counts.insert(
                name,
                CountBufferObservation {
                    snapshot_id: snapshot.snapshot_id,
                    manifest_rows: snapshot.manifest_rows,
                    buffer_within_budget: true,
                    buffer_bytes: 0,
                    included_buffer_rows: 0,
                },
            );
            continue;
        }
        let schema = snapshot.provider.schema();
        // Backlog guard: past the decode budget (outage recovery) the fast
        // paths serve the committed-only view — same degradation as the union
        // provider, and the only bounded-memory answer with a corpus-sized
        // buffer. Rows appear as the drain commits.
        // `None`: this site has only the raw SQL string, not a parsed window, so
        // it cannot price per-query the way the scan provider now does. The
        // windowed fast paths would benefit from the same time-aware gate —
        // it needs the bound plumbed down here, which is a larger change.
        let (within_budget, buffer_bytes) = match max_bytes {
            Some(max) => crate::wal_buffer::buffer_within_budget_with_max(
                &dir,
                &snapshot.consumed,
                None,
                max,
            ),
            None => crate::wal_buffer::buffer_within_budget(&dir, &snapshot.consumed, None),
        };
        let mut observation = CountBufferObservation {
            snapshot_id: snapshot.snapshot_id,
            manifest_rows: snapshot.manifest_rows,
            buffer_within_budget: within_budget,
            buffer_bytes,
            included_buffer_rows: 0,
        };
        if !within_budget {
            cache.evict(&dir);
            counts.insert(name, observation);
            continue;
        }
        // #78/#652: cache the decoded delta by its immutable segment set. The
        // distributed buffer partial consults the same cache later in this
        // request instead of decoding the WAL again.
        let (loaded, read_names) = tokio::task::spawn_blocking({
            let cache = Arc::clone(cache);
            let dir = dir.clone();
            let schema = schema.clone();
            let carrier = carrier.clone();
            let consumed = snapshot.consumed.clone();
            let owner = owner.clone();
            move || {
                cache.get_or_load_for_query_keyed(
                    &dir,
                    &schema,
                    &consumed,
                    None,
                    carrier.as_ref(),
                    owner.as_deref(),
                )
            }
        })
        .await
        .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?
        .map_err(ApiError::internal)?;
        // #1562: the listing this classification was drawn from, so an insert
        // keyed on "the WAL contributes nothing" can re-prove it. Taken from the
        // read itself — a second listing here could already disagree.
        proof.tables.push(WitnessedBuffer {
            dir: dir.clone(),
            exclude: snapshot.consumed.clone(),
            names: read_names,
            owner,
        });
        observation.included_buffer_rows = loaded.iter().map(|batch| batch.num_rows() as u64).sum();
        counts.insert(name.clone(), observation);
        if !loaded.is_empty() {
            batches.insert(name, loaded);
        }
    }
    let delta = if batches.is_empty() {
        None
    } else {
        metrics::counter!("siglake_query_fast_path_buffer_delta_total").increment(1);
        Some(BufferDelta { batches })
    };
    Ok(BufferDeltaLoad {
        delta,
        counts,
        proof,
    })
}

/// #61: buffer each USER INDEX the query references, the per-index sibling of
/// [`override_events_with_wal_buffer`]. Built-in tables (events, detection
/// tables, query_audit) are skipped — events has its own override, the rest
/// aren't ingest targets. Best-effort per index: a build error logs and the
/// index serves the Iceberg-only view.
async fn override_indexes_with_wal_buffer(
    ice: &siglake_storage::iceberg::IcebergContext,
    ctx: &SessionContext,
    buffer_root: &std::path::Path,
    tenant: Option<&str>,
    query: &str,
) -> Result<(), ApiError> {
    // The tables siglake itself owns. Everything else is a managed index and
    // takes the index path below -- including a consumer's own output tables,
    // which used to be listed here by name.
    const BUILT_IN: [&str; 2] = ["events", "query_audit"];
    let Some(tables) = referenced_tables(query) else {
        return Ok(());
    };
    for name in tables {
        if BUILT_IN.contains(&name.as_str()) || !ctx.table_exist(&name).unwrap_or(false) {
            continue;
        }
        let Some((snapshot, config)) = ice
            .index_provider_with_consumed_snapshot(&name)
            .await
            .map_err(ApiError::internal)?
        else {
            continue;
        };
        // #2661: a directory left behind by a dropped incarnation of this id
        // serves nothing — the index reads its committed view only.
        let Some(wal_dir) = crate::wal_buffer::resolve_owned_index_wal_dir(
            buffer_root,
            tenant.unwrap_or("default"),
            &name,
            &snapshot.table_uuid,
        ) else {
            continue;
        };
        match crate::wal_buffer::UnionEventsProvider::try_new_for_index(
            snapshot.provider,
            &wal_dir,
            &snapshot.consumed,
            config,
            Some(&snapshot.table_uuid),
        ) {
            Ok(provider) => {
                ctx.deregister_table(&name)
                    .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
                ctx.register_table(&name, std::sync::Arc::new(provider))
                    .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
            }
            Err(e) => {
                metrics::counter!("siglake_query_wal_buffer_build_errors_total").increment(1);
                tracing::warn!(error = %e, index = %name, dir = %wal_dir.display(),
                    "index WAL buffer union build failed; serving Iceberg-only view");
            }
        }
    }
    Ok(())
}

/// WS-6 distributed path: compute the WAL-buffer partial for `query` — the
/// result of running `query` over the un-committed rows only (a disjoint
/// slice from the workers' Iceberg shards), to be folded into the coordinator
/// merge. #86: covers `events` AND managed user indexes (carrier-mapped, same
/// as the local #61 path). Returns an empty vec (no buffer contribution) when:
/// - the query doesn't read exactly one bufferable table (joins/system
///   tables have no buffer to fold),
/// - the plan is `Local`/non-distributable (no per-shape merge to fold into),
/// - or there are no un-committed segments.
struct BufferedShardRead {
    table: String,
    snapshot: siglake_storage::iceberg::BufferedTableSnapshot,
    carrier: Option<siglake_core::index_config::IndexConfig>,
}

/// Capture every input that defines the coordinator's committed/buffer split.
/// The returned generation is also the worker pin; no later cache read may
/// choose a different snapshot after a drain commit.
async fn capture_buffered_shard_read(
    ice: &siglake_storage::iceberg::IcebergContext,
    table: String,
) -> Result<BufferedShardRead, ApiError> {
    let (snapshot, carrier) = if table == "events" {
        (
            ice.events_provider_with_consumed_snapshot()
                .await
                .map_err(ApiError::internal)?,
            None,
        )
    } else {
        let Some((snapshot, config)) = ice
            .index_provider_with_consumed_snapshot(&table)
            .await
            .map_err(ApiError::internal)?
        else {
            return Err(ApiError::internal(anyhow::anyhow!(
                "managed index `{table}` disappeared while preparing distributed query"
            )));
        };
        (snapshot, Some(config))
    };
    Ok(BufferedShardRead {
        table,
        snapshot,
        carrier,
    })
}

async fn compute_buffer_partials(
    read: &BufferedShardRead,
    buffer_root: &std::path::Path,
    cache: &crate::wal_buffer::BufferDeltaCache,
    tenant: Option<&str>,
    query: &str,
    plan: &datafusion::logical_expr::LogicalPlan,
    priority: Priority,
) -> Result<Vec<arrow_array::RecordBatch>, ApiError> {
    let table = match referenced_tables(query).as_deref() {
        Some([t]) => t.clone(),
        _ => return Ok(Vec::new()),
    };
    if table != read.table {
        return Err(ApiError::internal(anyhow::anyhow!(
            "buffered shard read captured `{}`, query references `{table}`",
            read.table
        )));
    }
    let dist = crate::coordinator::classify(plan);
    if matches!(dist, crate::coordinator::DistPlan::Local) {
        return Ok(Vec::new());
    }
    // #82: an ordered aggregate's partials must be the sort/limit-stripped
    // aggregate (same rewrite the workers run) — sorting/limiting the buffer
    // slice would drop groups from the merge.
    let query: &str = match &dist {
        crate::coordinator::DistPlan::OrderedAggregate { worker_sql, .. } => worker_sql,
        _ => query,
    };
    let tenant_label = tenant.unwrap_or("default");
    let (wal_dir, carrier) = if table == "events" {
        (
            crate::wal_buffer::resolve_tenant_wal_dir(buffer_root, tenant_label),
            None,
        )
    } else {
        // #2661: same gate as the local path — a dropped incarnation's
        // directory folds nothing into the fan-out.
        let Some(dir) = crate::wal_buffer::resolve_owned_index_wal_dir(
            buffer_root,
            tenant_label,
            &table,
            &read.snapshot.table_uuid,
        ) else {
            return Ok(Vec::new());
        };
        (dir, read.carrier.as_ref())
    };
    let base_schema = read.snapshot.provider.schema();
    let consumed = &read.snapshot.consumed;
    // WS-8 time-range prune: skip buffer segments outside the query's timestamp
    // window (extracted from the plan's predicates, same as the cost model).
    // Computed BEFORE the backlog guard so the guard prices only what this
    // query would decode — see `buffer_within_budget`.
    let time_bound = crate::cost::extract_time_bounds(plan).and_then(|b| {
        let lo = b
            .start
            .and_then(|d| d.timestamp_nanos_opt())
            .unwrap_or(i64::MIN);
        let hi = b
            .end
            .and_then(|d| d.timestamp_nanos_opt())
            .unwrap_or(i64::MAX);
        (lo < hi).then_some((lo, hi))
    });
    // Backlog guard: a corpus-sized buffer (outage recovery) can't be
    // materialized into a partial — serve committed-only until the drain
    // catches up.
    if !crate::wal_buffer::buffer_within_budget(&wal_dir, consumed, time_bound).0 {
        return Ok(Vec::new());
    }
    let batches = cache
        .get_or_load_for_query(
            &wal_dir,
            &base_schema,
            consumed,
            time_bound,
            carrier,
            (table != "events").then_some(read.snapshot.table_uuid.as_str()),
        )
        .map_err(ApiError::internal)?;
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    // Run the shard query over a buffer-only table to produce a partial in the
    // same shape the workers return for their Iceberg shards. A pool refusal
    // here is the same capacity answer as one in the coordinator's own scan
    // (503 + Retry-After, counted), not a fault.
    let ctx = buffer_partial_context();
    let partials = run_buffer_partial(&ctx, &table, base_schema, batches, query)
        .await
        .map_err(|e| ApiError::from_execution(anyhow::anyhow!(e), priority))?;
    metrics::counter!("siglake_query_wal_buffer_distributed_used_total").increment(1);
    Ok(partials)
}

/// The `SessionContext` the buffer partial executes on: a clone of the
/// process-wide template state (bounded `FairSpillPool`, spill target), with
/// siglake's UDFs registered so the shard SQL plans exactly as it does on a
/// worker.
///
/// This was `SessionContext::new()` until #549 — a fresh `RuntimeEnv` with
/// DataFusion's UNBOUNDED default pool, so the hash aggregate over the decoded
/// WAL buffer was the one execution site in the query pod whose working set
/// neither spilled at the limit nor showed up in
/// `siglake_query_memory_pool_reserved_bytes` (audit 2026-08-28, items 33 and
/// 36). Its two siblings in `coordinator.rs` (the MemTable-over-partials
/// merges) already took the template; this brings the third into line.
fn buffer_partial_context() -> SessionContext {
    let ctx = siglake_storage::session_context_with(None, None);
    crate::udfs::register_udfs(&ctx);
    ctx
}

/// Execute `query` over a buffer-only `table` holding `batches`, on `ctx`.
///
/// Split from [`compute_buffer_partials`] so the execution can be driven on a
/// context with a KNOWN pool: the production caller hands it
/// [`buffer_partial_context`]; the test hands it a one-byte pool and asserts
/// the aggregate is refused rather than completing outside the pool.
async fn run_buffer_partial(
    ctx: &SessionContext,
    table: &str,
    schema: arrow_schema::SchemaRef,
    batches: Vec<arrow_array::RecordBatch>,
    query: &str,
) -> datafusion::error::Result<Vec<arrow_array::RecordBatch>> {
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table, std::sync::Arc::new(mem))?;
    crate::sql::plan_client_sql(ctx, query)
        .await?
        .collect()
        .await
}

/// #86 fan-out floor for (ordered) scans: at or under this LIMIT the
/// coordinator runs the browse itself — single-node early-stop reads a few
/// pages, while fan-out costs per-worker drains + a transfer + a merge.
/// `SIGLAKE_DIST_SCAN_LOCAL_MAX_LIMIT` overrides (0 = always fan out).
fn dist_scan_local_max_limit() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("SIGLAKE_DIST_SCAN_LOCAL_MAX_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(100_000)
    })
}

fn referenced_tables(query: &str) -> Option<Vec<String>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, query).ok()?;
    referenced_tables_in(&stmts)
}

fn referenced_tables_in(stmts: &[Statement]) -> Option<Vec<String>> {
    let mut seen = std::collections::BTreeSet::<String>::new();
    for stmt in stmts {
        collect_tables_from_statement(stmt, &mut seen);
    }
    if seen.is_empty() {
        return None;
    }
    // Known system tables normalize to their canonical names; anything else
    // passes through as-parsed — WI-1 index ids are valid table names and
    // register dynamically (round-76 finding: the old canonical-only filter
    // silently dropped custom indexes, so they never registered and every
    // query against them failed planning with "table not found").
    let out = seen
        .into_iter()
        .map(|name| match canonical_query_table(&name) {
            Some(known) => known.to_string(),
            None => name,
        })
        .collect::<Vec<_>>();
    Some(out)
}

fn collect_tables_from_statement(stmt: &Statement, out: &mut std::collections::BTreeSet<String>) {
    match stmt {
        Statement::Query(query) => collect_tables_from_query(query, out),
        Statement::Explain { statement, .. } => collect_tables_from_statement(statement, out),
        Statement::Insert(insert) => {
            if let Some(name) = object_name_tail(&insert.table.to_string()) {
                out.insert(name.to_string());
            }
            if let Some(source) = &insert.source {
                collect_tables_from_query(source, out);
            }
        }
        _ => {}
    }
}

fn collect_tables_from_query(query: &SqlQuery, out: &mut std::collections::BTreeSet<String>) {
    // Keep each query's discoveries local until its WITH aliases have been
    // removed. A nested WITH may reuse a physical table's name without
    // hiding that table from the enclosing query.
    let mut local = std::collections::BTreeSet::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            collect_tables_from_cte(cte, &mut local);
        }
    }
    collect_tables_from_set_expr(&query.body, &mut local);
    // The tail clauses can carry scalar subqueries of their own
    // (`ORDER BY (SELECT ...)`, `LIMIT (SELECT ...)`). They sit outside
    // `body`, but inside this query's WITH scope, so they go into `local`.
    collect_tables_from_subqueries(&query.order_by, &mut local);
    collect_tables_from_subqueries(&query.limit_clause, &mut local);
    collect_tables_from_subqueries(&query.fetch, &mut local);
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            local.retain(|name| !name.eq_ignore_ascii_case(&cte.alias.name.value));
        }
    }
    out.extend(local);
}

fn collect_tables_from_cte(cte: &Cte, out: &mut std::collections::BTreeSet<String>) {
    collect_tables_from_query(&cte.query, out);
}

fn collect_tables_from_set_expr(expr: &SetExpr, out: &mut std::collections::BTreeSet<String>) {
    match expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                collect_tables_from_table_with_joins(twj, out);
            }
            // Everything else the SELECT carries — projection expressions,
            // WHERE/HAVING/QUALIFY, JOIN conditions, GROUP BY, DISTINCT ON —
            // reaches its subqueries here rather than through a hand-written
            // match per clause. The FROM relations are walked twice (the
            // visitor also finds derived subqueries); collection is a set, so
            // the second pass adds nothing.
            collect_tables_from_subqueries(select.as_ref(), out);
        }
        SetExpr::Query(query) => collect_tables_from_query(query, out),
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_from_set_expr(left, out);
            collect_tables_from_set_expr(right, out);
        }
        // VALUES rows, INSERT/UPDATE/DELETE bodies and TABLE t: no relations of
        // their own to name here, but they can nest a query.
        other => collect_tables_from_subqueries(other, out),
    }
}

/// Feed every subquery `node` carries back to [`collect_tables_from_query`].
///
/// Only the OUTERMOST subqueries are handed back: that recursion already
/// covers everything nested inside one, and re-entering an inner query here
/// would apply it without the WITH scope its parent established — the
/// shadowing `collect_tables_from_query` exists to enforce.
fn collect_tables_from_subqueries<N: Visit>(
    node: &N,
    out: &mut std::collections::BTreeSet<String>,
) {
    struct Walker<'a> {
        depth: usize,
        out: &'a mut std::collections::BTreeSet<String>,
    }
    impl Visitor for Walker<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, query: &SqlQuery) -> core::ops::ControlFlow<()> {
            if self.depth == 0 {
                collect_tables_from_query(query, self.out);
            }
            self.depth += 1;
            core::ops::ControlFlow::Continue(())
        }
        fn post_visit_query(&mut self, _query: &SqlQuery) -> core::ops::ControlFlow<()> {
            self.depth -= 1;
            core::ops::ControlFlow::Continue(())
        }
    }
    let _ = node.visit(&mut Walker { depth: 0, out });
}

fn collect_tables_from_table_with_joins(
    twj: &TableWithJoins,
    out: &mut std::collections::BTreeSet<String>,
) {
    collect_tables_from_table_factor(&twj.relation, out);
    for join in &twj.joins {
        collect_tables_from_table_factor(&join.relation, out);
    }
}

fn collect_tables_from_table_factor(
    factor: &TableFactor,
    out: &mut std::collections::BTreeSet<String>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            if let Some(last) = object_name_tail(&name.to_string()) {
                out.insert(last.to_string());
            }
        }
        TableFactor::Derived { subquery, .. } => collect_tables_from_query(subquery, out),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_tables_from_table_with_joins(table_with_joins, out),
        _ => {}
    }
}

fn canonical_query_table(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "events" => Some("events"),
        "query_audit" => Some("query_audit"),
        _ => None,
    }
}

fn query_table_has_timestamp(name: &str) -> bool {
    canonical_query_table(name).is_some()
}

fn object_name_tail(name: &str) -> Option<&str> {
    name.rsplit('.')
        .next()
        .map(|part| part.trim_matches('"').trim_matches('`'))
        .filter(|part| !part.is_empty())
}

#[allow(clippy::too_many_arguments)]
async fn execute_with_limits(
    df: DataFrame,
    format: QueryFormat,
    resolved: &ResolvedLimits,
    cost: CostReport,
    cache_ctx: Option<SqlResultCacheCtx>,
    admission: Option<crate::admission::AdmissionGuard>,
    buffer_delta_micros: u64,
    residual_fallback: Option<DataFrame>,
    execution_id: siglake_storage::QueryExecutionId,
    cancel: Option<siglake_storage::CancelOnDrop>,
    deadline: tokio::time::Instant,
    started: Instant,
) -> Result<Response, ApiError> {
    let max_rows = resolved.max_rows_returned;
    let cost_for_timeout = cost.clone();
    let resolved_inner = resolved.clone();
    let _ = max_rows;
    let inner = async move {
        match format {
            QueryFormat::Records => {
                render_records(
                    df,
                    &resolved_inner,
                    Some(cost),
                    cache_ctx,
                    buffer_delta_micros,
                    residual_fallback,
                    execution_id,
                )
                .await
            }
            // Ndjson hands the guard to the body; Records keeps it in this
            // scope, where it covers the collect and fires on the timeout.
            QueryFormat::Ndjson => render_ndjson(df, &resolved_inner, admission, cancel).await,
        }
    };
    apply_timeout(inner, resolved, &cost_for_timeout, deadline, started).await
}

struct GuardedStream<S> {
    _guard: Option<crate::admission::AdmissionGuard>,
    /// Cancels the query's scans when this BODY is dropped. It has to live here
    /// rather than in the handler: an NDJSON response streams after the handler
    /// returns, so a handler-scoped guard cancelled healthy requests (3 e2e
    /// tests went 3 rows -> 0). Here it fires on exactly the right event --
    /// the body finishing, or the client hanging up mid-stream.
    _cancel: Option<siglake_storage::CancelOnDrop>,
    inner: S,
}

impl<S> futures::Stream for GuardedStream<S>
where
    S: futures::Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

pub(crate) async fn apply_timeout<F, T>(
    fut: F,
    resolved: &ResolvedLimits,
    cost: &CostReport,
    deadline: tokio::time::Instant,
    started: Instant,
) -> Result<T, ApiError>
where
    F: std::future::Future<Output = Result<T, ApiError>>,
{
    if !resolved.circuit_breakers {
        return fut.await;
    }
    // Tokio polls the inner future before its timer, even when `deadline` has
    // already elapsed. That is useful for best-effort timeouts, but these are
    // wall-clock request limits: an immediately-ready metadata/cache path must
    // not turn a zero-second (or planning-exhausted) deadline into a success.
    if deadline_expired(resolved, deadline) {
        return Err(request_timeout_error(resolved, cost, started));
    }
    match tokio::time::timeout_at(deadline, fut).await {
        Ok(r) => r,
        Err(_) => Err(request_timeout_error(resolved, cost, started)),
    }
}

/// Has this request's wall-clock budget already run out?
///
/// The guard `apply_timeout` applies before it polls, hoisted so the phases that
/// are NOT a wrapped future can ask the same question. A result-cache hit is
/// the one that mattered: it is an immediately-available response produced
/// without awaiting anything the deadline could cut, so without this check a
/// warmed entry answered 200 to a request whose budget had already gone.
pub(crate) fn deadline_expired(resolved: &ResolvedLimits, deadline: tokio::time::Instant) -> bool {
    resolved.circuit_breakers && tokio::time::Instant::now() >= deadline
}

pub(crate) fn request_timeout_error(
    resolved: &ResolvedLimits,
    cost: &CostReport,
    started: Instant,
) -> ApiError {
    metrics::counter!(
        "siglake_query_breaker_trips_total",
        "breaker" => "timeout",
        "priority" => resolved.priority.label()
    )
    .increment(1);
    ApiError::timeout(
        cost,
        started.elapsed().as_secs_f64(),
        resolved.timeout.as_secs(),
    )
}

/// The shard endpoint's wall-clock refusal, with the `shard_timeout` accounting
/// that used to live inline at the collect.
///
/// Separate from [`request_timeout_error`] on purpose: the shard answers a bare
/// 504 rather than the cost-carrying `ApiError::timeout` body, because it has no
/// `CostReport` (`estimate()` is deliberately not run per shard) and its caller
/// is the coordinator, which reads the status, not the body.
fn shard_timeout_error(resolved: &ResolvedLimits) -> ApiError {
    metrics::counter!(
        "siglake_query_breaker_trips_total",
        "breaker" => "shard_timeout",
        "priority" => resolved.priority.label()
    )
    .increment(1);
    ApiError::new_status(
        axum::http::StatusCode::GATEWAY_TIMEOUT,
        format!(
            "shard exceeded its wall-clock budget of {}s",
            resolved.timeout.as_secs()
        ),
    )
}

async fn render_records(
    df: DataFrame,
    resolved: &ResolvedLimits,
    cost: Option<CostReport>,
    cache_ctx: Option<SqlResultCacheCtx>,
    buffer_delta_micros: u64,
    residual_fallback: Option<DataFrame>,
    execution_id: siglake_storage::QueryExecutionId,
) -> Result<Response, ApiError> {
    let plan_started = Instant::now();
    // Before `create_physical_plan`, which consumes the DataFrame: execution
    // runs with the session's own config (#2251), on the shared bounded pool.
    let task_ctx = crate::midflight::task_ctx_for(&df);
    let plan = df
        .create_physical_plan()
        .await
        .map_err(ApiError::internal)?;
    let plan_elapsed = plan_started.elapsed();
    record_phase_metric("sql", "physical_plan", plan_elapsed);
    // Diagnosis aid (`SIGLAKE_LOG_EXEC_PLANS=1`): log the EXECUTED physical
    // plan. EXPLAIN through the API is misleading for ordered shapes (the
    // scan-order hint isn't derived for EXPLAIN statements), so live plan
    // attribution needs the real thing.
    if std::env::var("SIGLAKE_LOG_EXEC_PLANS").as_deref() == Ok("1") {
        tracing::info!(plan = %displayable(plan.as_ref()).indent(true), "executed physical plan");
    }

    let collect_started = Instant::now();
    // Priority lane (see midflight). Gate tuned 2026-07-22: the original
    // `cost.exact` requirement excluded every filtered shape (their estimates
    // are heuristic) — the lane recorded ZERO routes in the census round.
    // Eligibility is now a POSITIVE small estimate: rows AND bytes bounds
    // hold regardless of exactness, while zero-estimates (the estimator
    // punting — exactly the ordered browses that must stay pooled) and
    // large candidate sets keep pool routing. Worst-case mis-estimate is
    // bounded by both caps + the mid-flight breaker.
    let inline_exec = cost.as_ref().map(inline_exec_eligible).unwrap_or(false);
    let batches = if resolved.circuit_breakers {
        match crate::midflight::collect_plan_with_rows_scanned_cap_routed(
            plan.clone(),
            task_ctx,
            resolved.max_rows_scanned,
            resolved.priority,
            inline_exec,
        )
        .await
        {
            Ok(crate::midflight::CollectOutcome::Ok(b)) => b,
            Ok(crate::midflight::CollectOutcome::RowsScannedExceeded {
                rows_scanned,
                limit,
                ..
            }) => {
                // Ordered-residual temporal-skew fallback: the hinted drain
                // found too few matches in scan order — degrade to the pruned
                // TopK plan instead of erroring (today's behavior for this
                // shape, minus the wasted drain).
                if let Some(fallback) = residual_fallback {
                    metrics::counter!("siglake_query_ordered_residual_fallback_total").increment(1);
                    tracing::info!(
                        rows_scanned,
                        limit,
                        "ordered-residual drain hit the rows cap; retrying as pruned TopK"
                    );
                    return Box::pin(render_records(
                        fallback,
                        resolved,
                        cost,
                        cache_ctx,
                        buffer_delta_micros,
                        None,
                        execution_id,
                    ))
                    .await;
                }
                let mut err = ApiError::new_status(
                    axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                    format!(
                        "mid-flight breaker: scanned {} rows, exceeded {} limit",
                        rows_scanned, limit
                    ),
                );
                // A tripped browse must self-attribute (07-16: three windowed-
                // browse 413s were undiagnosable — pod-level ordering-outcome
                // counters can't isolate one request): attach the same scan
                // accounting a success response carries.
                let runtime = crate::midflight::summarize_plan_runtime(&plan);
                err.context = Some(serde_json::json!({
                    "cost": cost,
                    "stats": {
                        "rows_scanned": runtime.leaf_output_rows,
                        "scan": scan_detail_from_runtime(&runtime),
                    },
                }));
                if let Some(cache_ctx) = cache_ctx.clone() {
                    finish_result_cache(cache_ctx, CacheEligibility::SQL, None).await;
                }
                return Err(err);
            }
            // Unreachable: this call site passes `AccumulationBound::None`.
            // The refusing bound belongs to the Jaeger routes, whose
            // `{data, total}` shape cannot express a partial result (#2184).
            Ok(crate::midflight::CollectOutcome::AccumulatedBoundExceeded { .. }) => {
                return Err(ApiError::internal(
                    "sql collect reported a refusing accumulation bound",
                ))
            }
            // A pool refusal is a 503 with Retry-After, not a 500: the pool
            // bounding memory is the design, and the client can act on "wait".
            Err(e) => return Err(ApiError::from_execution(e, resolved.priority)),
        }
    } else {
        crate::midflight::collect_plan(plan.clone(), task_ctx)
            .await
            .map_err(|e| ApiError::from_execution(e, resolved.priority))?
    };
    let collect_elapsed = collect_started.elapsed();
    record_phase_metric("sql", "collect", collect_elapsed);
    let runtime = crate::midflight::summarize_plan_runtime(&plan);
    record_runtime_metrics("sql", "records", &runtime);

    let render_started = Instant::now();
    let mut body = batches_to_records(&batches, Some(resolved.max_rows_returned))
        .map_err(ApiError::internal)?;
    let render_elapsed = render_started.elapsed();
    record_phase_metric("sql", "render_records", render_elapsed);
    body.cost = cost;
    // Surface the leaf scan accounting so the bench (and any client) can see
    // how much the query actually read — the pruning-effectiveness metric.
    body.stats = Some(crate::format::ScanStats {
        rows_scanned: runtime.leaf_output_rows,
        bytes_scanned: runtime.bytes_scanned,
        spill_bytes: runtime.spilled_bytes,
        // #85: single-pod wall attribution (plan → execute → render, plus the
        // buffer-delta load that preceded execution).
        phases: Some(Box::new(crate::format::PhaseStats {
            plan_micros: plan_elapsed.as_micros() as u64,
            buffer_delta_micros,
            collect_micros: collect_elapsed.as_micros() as u64,
            render_micros: render_elapsed.as_micros() as u64,
            distributed: None,
        })),
        scan: scan_detail_from_runtime(&runtime),
        // The full SQL plan ran: whatever this cost, it is not a fast path.
        served_by: Some("scan".to_string()),
    });
    let status = if body.truncated {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::OK
    };
    // One allocation from here on: the same body answers this request and
    // fills the cache entry, rather than being deep-copied into it (#2304).
    let body = Arc::new(body);
    if let Some(cache_ctx) = cache_ctx {
        if body.truncated {
            finish_result_cache(cache_ctx, CacheEligibility::SQL, None).await;
        } else {
            finish_result_cache(
                cache_ctx,
                CacheEligibility::SQL,
                Some(CachedBody::Records(Arc::clone(&body))),
            )
            .await;
        }
    }
    tracing::info!(
        endpoint = "sql",
        query_execution_id = execution_id.0,
        format = "records",
        root_plan = %displayable(plan.as_ref()).one_line(),
        plan_nodes = runtime.nodes,
        leaf_rows_scanned = runtime.leaf_output_rows,
        df_output_rows = runtime.output_rows,
        df_output_batches = runtime.output_batches,
        df_elapsed_compute_ms = runtime.elapsed_compute_nanos as f64 / 1_000_000.0,
        df_bytes_scanned = runtime.bytes_scanned,
        spill_count = runtime.spill_count,
        spilled_bytes = runtime.spilled_bytes,
        physical_plan_ms = plan_elapsed.as_secs_f64() * 1000.0,
        collect_ms = collect_elapsed.as_secs_f64() * 1000.0,
        render_ms = render_elapsed.as_secs_f64() * 1000.0,
        "sql execution profile"
    );
    Ok((status, Json(body.as_ref())).into_response())
}

async fn render_ndjson(
    df: DataFrame,
    resolved: &ResolvedLimits,
    admission: Option<crate::admission::AdmissionGuard>,
    cancel: Option<siglake_storage::CancelOnDrop>,
) -> Result<Response, ApiError> {
    // Build the physical plan ourselves so we can hand it to
    // NdjsonStream for mid-flight rows-scanned polling.
    let task_ctx = crate::midflight::task_ctx_for(&df);
    let plan = df
        .create_physical_plan()
        .await
        .map_err(ApiError::internal)?;
    let stream = datafusion::physical_plan::execute_stream(plan.clone(), task_ctx)
        .map_err(ApiError::internal)?;
    let mut ndjson = NdjsonStream::new(stream, Some(resolved.max_rows_returned));
    if resolved.circuit_breakers {
        ndjson = ndjson.with_rows_scanned_cap(plan, resolved.max_rows_scanned, resolved.priority);
    }
    let body = Body::from_stream(GuardedStream {
        _guard: admission,
        _cancel: cancel,
        inner: ndjson,
    });
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/x-ndjson"),
    );
    *resp.status_mut() = StatusCode::OK;
    Ok(resp)
}

fn record_metrics(
    endpoint: &'static str,
    start: Instant,
    result: &Result<Response, ApiError>,
    cost: &CostReport,
) {
    let elapsed = start.elapsed().as_secs_f64();
    let status_label = match result {
        Ok(_) => "200",
        Err(e) => match e.status.as_u16() {
            400 => "400",
            429 => "429",
            413 => "413",
            500 => "500",
            503 => "503",
            504 => "504",
            _ => "other",
        },
    };
    metrics::counter!("siglake_query_requests_total",
        "endpoint" => endpoint, "status" => status_label)
    .increment(1);
    metrics::histogram!("siglake_query_request_duration_seconds",
        "endpoint" => endpoint)
    .record(elapsed);
    metrics::histogram!(
        "siglake_query_cost_bytes_estimated",
        "endpoint" => endpoint,
        "complexity" => match cost.complexity_class {
            crate::cost::ComplexityClass::Small => "small",
            crate::cost::ComplexityClass::Medium => "medium",
            crate::cost::ComplexityClass::Large => "large",
            crate::cost::ComplexityClass::Huge => "huge",
        }
    )
    .record(cost.estimated_bytes_scanned as f64);
}

fn record_phase_metric(endpoint: &'static str, phase: &'static str, elapsed: std::time::Duration) {
    metrics::histogram!(
        "siglake_query_phase_duration_seconds",
        "endpoint" => endpoint,
        "phase" => phase,
    )
    .record(elapsed.as_secs_f64());
}

fn record_runtime_metrics(
    endpoint: &'static str,
    format: &'static str,
    runtime: &crate::midflight::PlanRuntimeStats,
) {
    metrics::histogram!(
        "siglake_query_runtime_leaf_rows_scanned",
        "endpoint" => endpoint,
        "format" => format,
    )
    .record(runtime.leaf_output_rows as f64);
    metrics::histogram!(
        "siglake_query_runtime_df_elapsed_compute_seconds",
        "endpoint" => endpoint,
        "format" => format,
    )
    .record(runtime.elapsed_compute_nanos as f64 / 1_000_000_000.0);
    metrics::histogram!(
        "siglake_query_runtime_df_bytes_scanned",
        "endpoint" => endpoint,
        "format" => format,
    )
    .record(runtime.bytes_scanned as f64);
}

fn response_status_label(result: &Result<Response, ApiError>) -> &'static str {
    match result {
        Ok(resp) => match resp.status().as_u16() {
            200 => "200",
            202 => "202",
            413 => "413",
            other if other >= 500 => "500",
            _ => "other",
        },
        Err(e) => match e.status.as_u16() {
            400 => "400",
            429 => "429",
            413 => "413",
            500 => "500",
            503 => "503",
            504 => "504",
            _ => "other",
        },
    }
}

fn compact_query(query: &str) -> String {
    let compact = query.split_whitespace().collect::<Vec<_>>().join(" ");
    const LIMIT: usize = 160;
    if compact.len() <= LIMIT {
        compact
    } else {
        format!("{}...", &compact[..LIMIT])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};

    #[test]
    fn grouped_numeric_fast_path_switch_defaults_on_and_parses_off_values() {
        assert!(grouped_numeric_fast_path_from(None));
        assert!(grouped_numeric_fast_path_from(Some("on")));
        for value in ["off", "0", "false", "NO"] {
            assert!(!grouped_numeric_fast_path_from(Some(value)), "{value}");
        }
    }

    #[tokio::test]
    async fn grouped_integer_avg_separates_arithmetic_and_renderer_differentials() {
        use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;

        // Drive the real grouped DataFusion accumulator with two row orders
        // that have the same exact integer (sum, count). This is deliberately
        // separate from the status-304 receipt below: its much smaller retained
        // sum does not establish that its inputs crossed this precision bound.
        let high = 1_i64 << 53;
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Int64, false),
            Field::new("size", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])),
                Arc::new(Int64Array::from(vec![high, 1, -high, high, -high, 1])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("httplogs", batch).unwrap();
        let batches = ctx
            .sql(
                "SELECT status, avg(size) AS avg_size, count(*) AS n \
                 FROM httplogs GROUP BY status ORDER BY status",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let averages = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let raw_bits = [averages.value(0).to_bits(), averages.value(1).to_bits()];
        assert_eq!(raw_bits, [0.0_f64.to_bits(), (1.0_f64 / 3.0).to_bits()]);

        let scan = crate::format::batches_to_records(&batches, None).unwrap();
        let scan_rows = scan.rows.as_array().unwrap();
        let scan_bits = [
            scan_rows[0]["avg_size"].as_f64().unwrap().to_bits(),
            scan_rows[1]["avg_size"].as_f64().unwrap().to_bits(),
        ];
        assert_eq!(scan_bits, raw_bits, "Arrow JSON changed DataFusion bits");

        // The footer path constructs a Value directly. Its final serde_json
        // serialization and parse preserve the same exact quotient bits.
        let exact_avg = 1.0_f64 / 3.0;
        let fast = serde_json::json!({ "status": 1, "avg_size": exact_avg, "n": 3 });
        let fast: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&fast).unwrap()).unwrap();
        assert_eq!(
            fast["avg_size"].as_f64().unwrap().to_bits(),
            exact_avg.to_bits()
        );

        // The AWS receipt's exact footer quotient is one binary64 value above
        // the scan response of 2026-10-10. Arrow JSON emits the round-trippable
        // decimal, and serde_json built WITH `float_roundtrip` (#6659) parses
        // it back to the same bits while collecting scan rows; before that
        // feature it read the adjacent lower value and the two renderers
        // disagreed. The footer path constructs its Value directly. Both now
        // carry the quotient's bits, which is independent of the genuine
        // accumulation-order example above: that one survives the renderers.
        let footer_avg = 1_168_295_731_f64 / 37_137_326_f64;
        let reference_avg = 31.45880053399644_f64;
        assert_eq!(footer_avg.to_bits(), reference_avg.to_bits() + 1);
        let footer_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "avg_size",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(Float64Array::from(vec![footer_avg]))],
        )
        .unwrap();
        let encoded_scan =
            crate::format::batches_to_ndjson(std::slice::from_ref(&footer_batch)).unwrap();
        assert_eq!(
            encoded_scan,
            br#"{"avg_size":31.458800533996442}
"#
        );
        let scan = crate::format::batches_to_records(&[footer_batch], None).unwrap();
        assert_eq!(
            scan.rows[0]["avg_size"].as_f64().unwrap().to_bits(),
            footer_avg.to_bits(),
            "the records renderer lost the quotient's bits"
        );
        // ... and the response the client reads carries them too.
        let served: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&scan.rows).unwrap()).unwrap();
        assert_eq!(
            served[0]["avg_size"].as_f64().unwrap().to_bits(),
            footer_avg.to_bits()
        );
        let fast = serde_json::json!({ "avg_size": footer_avg });
        let encoded_fast = serde_json::to_vec(&fast).unwrap();
        assert_eq!(encoded_fast, br#"{"avg_size":31.458800533996442}"#);
        assert_eq!(
            fast["avg_size"].as_f64().unwrap().to_bits(),
            footer_avg.to_bits()
        );
    }

    fn strip_table_metadata_for_test(warehouse: &std::path::Path, index_id: &str) -> usize {
        let mut removed = 0;
        for namespace in std::fs::read_dir(warehouse).unwrap() {
            let metadata = namespace.unwrap().path().join(index_id).join("metadata");
            let Ok(entries) = std::fs::read_dir(metadata) else {
                continue;
            };
            for entry in entries {
                let path = entry.unwrap().path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                {
                    std::fs::remove_file(path).unwrap();
                    removed += 1;
                }
            }
        }
        removed
    }

    /// Result-cache counts SINCE THE LAST CALL (`snapshot()` drains), by
    /// `outcome`, omitting the outcomes that did not move. Read the whole map
    /// when a test needs to assert that ONE outcome moved and its siblings did
    /// not: a second `snapshot()` would see the drained remainder, not the same
    /// phase. A drained series stays registered and reports a 0 delta, which is
    /// "did not move" and so is not an entry.
    fn result_cache_outcomes(snapshotter: &Snapshotter) -> std::collections::HashMap<String, u64> {
        let mut by_outcome = std::collections::HashMap::new();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if key.key().name() != "siglake_query_sql_result_cache_requests_total" {
                continue;
            }
            let Some(outcome) = key
                .key()
                .labels()
                .find(|label| label.key() == "outcome")
                .map(|label| label.value().to_string())
            else {
                continue;
            };
            let count = match value {
                DebugValue::Counter(count) => count,
                other => panic!("result-cache outcome was not a counter: {other:?}"),
            };
            *by_outcome.entry(outcome).or_insert(0) += count;
        }
        by_outcome.retain(|_, count| *count > 0);
        by_outcome
    }

    fn result_cache_outcome(snapshotter: &Snapshotter, expected: &str) -> u64 {
        result_cache_outcomes(snapshotter)
            .get(expected)
            .copied()
            .unwrap_or(0)
    }

    /// Compare the previous partition-every-2K implementation with the cutoff
    /// over 1.15M distinct groups. Reports collector-only medians, not SQL or
    /// HTTP latency; the real benchmark still needs an end-to-end round.
    /// `cargo test -p siglake-query-server --lib report_bounded_top_k -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement, not a gate"]
    fn report_bounded_top_k_vs_full_view() {
        let cmp = GroupEntryCmp {
            sort: GroupCountSort::Count { ascending: false },
            kind: GroupKeyKind::Str,
        };
        let keys: Vec<String> = (0..1_150_000).map(|i| format!("host-{i:07}")).collect();
        let k = 100usize;
        for distribution in ["uniform", "long_tail", "ties", "improving"] {
            let counts: Vec<u64> = (0..keys.len())
                .map(|i| match distribution {
                    "uniform" => ((i * 2_654_435_761) % 1_000_000) as u64,
                    "long_tail" => 1_000_000 / (1 + ((i * 2_654_435_761) % 1_000_000)) as u64,
                    "ties" => 1,
                    _ => i as u64,
                })
                .collect();
            let mut reference: Vec<_> = keys
                .iter()
                .zip(&counts)
                .map(|(key, count)| (Some(key.as_str()), *count))
                .collect();
            reference.sort_unstable_by(|a, b| cmp.compare(a, b));
            reference.truncate(k);
            let mut before = Vec::new();
            let mut after = Vec::new();
            // Alternate order so one arm does not always follow the other.
            for round in 0..12 {
                for arm in [round % 2 == 0, round % 2 != 0] {
                    let started = std::time::Instant::now();
                    let mut bounded = Vec::with_capacity(k * 2);
                    let mut cutoff = None;
                    for (key, count) in keys.iter().zip(&counts) {
                        let entry = (Some(key.as_str()), *count);
                        if arm {
                            push_bounded(&mut bounded, entry, Some((k, cmp)), &mut cutoff);
                        } else {
                            bounded.push(entry);
                            if bounded.len() >= k * 2 {
                                bounded.select_nth_unstable_by(k - 1, |a, b| cmp.compare(a, b));
                                bounded.truncate(k);
                            }
                        }
                    }
                    bounded.sort_unstable_by(|a, b| cmp.compare(a, b));
                    bounded.truncate(k);
                    let elapsed = started.elapsed().as_nanos();
                    assert_eq!(bounded, reference, "{distribution}");
                    std::hint::black_box(&bounded);
                    if round > 0 {
                        if arm {
                            after.push(elapsed);
                        } else {
                            before.push(elapsed);
                        }
                    }
                }
            }
            before.sort_unstable();
            after.sort_unstable();
            let before = before[before.len() / 2] as f64 / 1_000_000.0;
            let after = after[after.len() / 2] as f64 / 1_000_000.0;
            println!("{distribution}: top-{k} over {} groups: previous {before:.3}ms, cutoff {after:.3}ms = {:.2}x", keys.len(), before / after);
        }
    }

    const BUFFER_PARTIAL_SQL: &str = "SELECT host, count(*) AS n FROM events GROUP BY host";
    const BUFFER_PARTIAL_ROWS: usize = 40_000;
    const BUFFER_PARTIAL_GROUPS: usize = 4_000;

    /// Five "segments" of 8,000 rows over 4,000 distinct hosts: enough
    /// aggregate state that no pool small enough to refuse it could be
    /// mistaken for a fluke, small enough for the unit-test binary.
    fn buffer_partial_fixture() -> (arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>) {
        use arrow_schema::{Field, Schema};
        let schema: arrow_schema::SchemaRef = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("host", DataType::Utf8, false),
        ]));
        let per_batch = 8_000;
        let batches = (0..BUFFER_PARTIAL_ROWS / per_batch)
            .map(|b| {
                let rows = (b * per_batch)..((b + 1) * per_batch);
                arrow_array::RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(arrow_array::Int64Array::from_iter_values(
                            rows.clone().map(|i| i as i64),
                        )),
                        Arc::new(arrow_array::StringArray::from_iter_values(
                            rows.map(|i| format!("host-{:04}", i % BUFFER_PARTIAL_GROUPS)),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();
        (schema, batches)
    }

    /// #549: the buffer partial executes inside whatever pool its context
    /// carries. Against a one-byte greedy pool the aggregate's first
    /// reservation must be refused (`ResourcesExhausted`) — the outcome that
    /// could NOT happen on the `SessionContext::new()` this replaced, where the
    /// same aggregate completed on an unbounded pool no gauge ever saw.
    #[tokio::test]
    async fn buffer_partial_is_refused_by_a_tiny_pool() {
        use datafusion::error::DataFusionError;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion::prelude::SessionConfig;

        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(1)))
            .build_arc()
            .unwrap();
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let (schema, batches) = buffer_partial_fixture();
        let err = run_buffer_partial(&ctx, "events", schema, batches, BUFFER_PARTIAL_SQL)
            .await
            .expect_err("a GROUP BY over 4,000 groups cannot fit a 1-byte pool");
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "expected a pool refusal, got: {err}"
        );
    }

    /// The other half of the contract: the production context IS the shared
    /// `RuntimeEnv` (so its reservations are the ones
    /// `siglake_query_memory_pool_reserved_bytes` reports), keeps the UDFs the
    /// shard SQL may reference, and the ordinary case still gets the right
    /// answer.
    #[tokio::test]
    async fn buffer_partial_context_shares_the_query_pool() {
        use arrow_array::Array;
        use datafusion::execution::FunctionRegistry;

        let ctx = buffer_partial_context();
        assert!(
            Arc::ptr_eq(
                &ctx.runtime_env(),
                &siglake_storage::shared_query_runtime_env()
            ),
            "the buffer partial must execute on the process-wide RuntimeEnv"
        );
        for udf in [crate::udfs::KV_EXTRACT_FN, crate::udfs::MATCH_ALIAS_FN] {
            assert!(ctx.udf(udf).is_ok(), "UDF {udf} must stay registered");
        }

        let (schema, batches) = buffer_partial_fixture();
        let out = run_buffer_partial(&ctx, "events", schema, batches, BUFFER_PARTIAL_SQL)
            .await
            .unwrap();
        let groups: usize = out.iter().map(|b| b.num_rows()).sum();
        let total: i64 = out
            .iter()
            .map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .sum::<i64>()
            })
            .sum();
        assert_eq!(groups, BUFFER_PARTIAL_GROUPS);
        assert_eq!(total as usize, BUFFER_PARTIAL_ROWS);
    }

    /// The buffer helper re-plans the client's SQL on a fresh context, so it
    /// must enforce the read-only boundary itself rather than rely on its
    /// current caller having planned the same string first (task #1765).
    #[tokio::test]
    async fn buffer_partial_refuses_copy() {
        let ctx = buffer_partial_context();
        let (schema, batches) = buffer_partial_fixture();
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("buffer-copy.parquet");
        let query = format!("COPY (SELECT host FROM events) TO '{}'", target.display());

        let err = run_buffer_partial(&ctx, "events", schema, batches, &query)
            .await
            .expect_err("COPY through the buffer helper must be refused");
        assert!(
            err.to_string().contains("not supported"),
            "COPY failed for the wrong reason: {err}"
        );
        assert!(!target.exists(), "refused COPY wrote to the filesystem");
    }

    /// The bounded top-K collector must return byte-identical rows to sorting
    /// the whole set — the point of it is memory, and a leaderboard that shifts
    /// under a memory optimization is a correctness regression, not a tradeoff.
    ///
    /// Adversarial by design: a third of the counts are drawn from a tiny range,
    /// so the K boundary lands in the middle of a mass of equal counts and the
    /// key tiebreak is what decides membership. That is the case an unstable
    /// partition would get wrong if `cmp` were not a total order.
    #[test]
    fn bounded_top_k_matches_the_full_sort() {
        let mk = |sort| GroupEntryCmp {
            sort,
            kind: GroupKeyKind::Str,
        };
        let descending = mk(GroupCountSort::Count { ascending: false });
        let ascending = mk(GroupCountSort::Count { ascending: true });
        let by_group = mk(GroupCountSort::Group { ascending: true });
        let by_group_desc = mk(GroupCountSort::Group { ascending: false });

        let keys: Vec<String> = (0..5_000).map(|i| format!("host-{i:05}")).collect();
        let entries: Vec<(Option<&str>, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let count = if i % 3 == 0 {
                    // Dense ties across the boundary.
                    (i % 7) as u64
                } else {
                    ((i * 2_654_435_761) % 100_000) as u64
                };
                (Some(k.as_str()), count)
            })
            .chain(std::iter::once((None, 42u64)))
            .collect();

        for cmp in [descending, ascending, by_group, by_group_desc] {
            for k in [1usize, 2, 10, 100, 999, 5_001] {
                let mut reference: Vec<(Option<&str>, u64)> = entries.clone();
                reference.sort_unstable_by(|a, b| cmp.compare(a, b));
                reference.truncate(k);

                let mut bounded: Vec<(Option<&str>, u64)> = Vec::new();
                let mut cutoff = None;
                for e in &entries {
                    push_bounded(&mut bounded, *e, Some((k, cmp)), &mut cutoff);
                }
                bounded.sort_unstable_by(|a, b| cmp.compare(a, b));
                bounded.truncate(k);

                assert_eq!(bounded, reference, "k={k}");
                // The whole point: the buffer never grew past the bound.
                assert!(
                    bounded.len() <= k,
                    "bounded path held {} entries for k={k}",
                    bounded.len()
                );
            }
        }
    }
    /// Exercise a moving cutoff in both input directions, numeric key ordering,
    /// ties, NULL, and an eventual winner arriving last (as a buffer-only group
    /// does). Compare with a full sort rather than a second cutoff algorithm.
    #[test]
    fn bounded_top_k_cutoff_matches_full_sort_for_typed_and_late_groups() {
        let keys: Vec<_> = (-250..250).map(|i| i.to_string()).collect();
        for kind in [GroupKeyKind::Str, GroupKeyKind::Int, GroupKeyKind::Float] {
            for sort in [
                GroupCountSort::Count { ascending: true },
                GroupCountSort::Count { ascending: false },
                GroupCountSort::Group { ascending: true },
                GroupCountSort::Group { ascending: false },
            ] {
                let cmp = GroupEntryCmp { sort, kind };
                for reverse in [false, true] {
                    for ties in [false, true] {
                        let mut entries: Vec<_> = keys
                            .iter()
                            .enumerate()
                            .map(|(i, key)| (Some(key.as_str()), if ties { 1 } else { i as u64 }))
                            .collect();
                        if reverse {
                            entries.reverse();
                        }
                        entries.push((None, 600));
                        entries.push((Some("1000"), 1000));
                        for k in [1, 2, 7, 100, 251, 502, 503] {
                            let mut reference = entries.clone();
                            reference.sort_unstable_by(|a, b| cmp.compare(a, b));
                            reference.truncate(k);
                            let mut bounded = Vec::new();
                            let mut cutoff = None;
                            for entry in &entries {
                                push_bounded(&mut bounded, *entry, Some((k, cmp)), &mut cutoff);
                                assert!(bounded.len() < k * 2);
                            }
                            bounded.sort_unstable_by(|a, b| cmp.compare(a, b));
                            bounded.truncate(k);
                            assert_eq!(
                                bounded, reference,
                                "{kind:?} {sort:?} reverse={reverse} ties={ties} k={k}"
                            );
                        }
                    }
                }
            }
        }
        let mut unbounded = Vec::new();
        let mut cutoff = None;
        for key in &keys {
            push_bounded(&mut unbounded, (Some(key), 1), None, &mut cutoff);
        }
        assert_eq!(unbounded.len(), keys.len());
        assert!(cutoff.is_none());
    }

    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::displayable;
    use siglake_core::index_config::{
        DocMapping, FieldMapping, FieldType, IndexConfig, MappingMode,
    };
    use siglake_core::Event;
    use siglake_storage::iceberg::IcebergContext;

    fn field(name: &str, field_type: FieldType, required: bool) -> FieldMapping {
        FieldMapping {
            name: name.to_string(),
            field_type,
            required,
        }
    }

    fn logs_config(default_search_fields: Vec<&str>) -> IndexConfig {
        IndexConfig {
            index_id: "logs".to_string(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    field("ts", FieldType::Datetime, true),
                    field(
                        "message",
                        FieldType::Text {
                            tokenizer: Some("default".to_string()),
                        },
                        false,
                    ),
                    field(
                        "title",
                        FieldType::Text {
                            tokenizer: Some("stem".to_string()),
                        },
                        false,
                    ),
                ],
                timestamp_field: "ts".to_string(),
                tag_fields: Vec::new(),
                default_search_fields: default_search_fields
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
            },
            retention: None,
            index_at_flush: None,
        }
    }

    /// The distributed-vs-local gate keys on `referenced_tables`: only an
    /// `events`-only query distributes; a hot-cache table function, another
    /// table, a join, or no-table query is `None`/non-`["events"]` ⇒ single-pod.
    #[test]
    fn distributed_gate_keys_on_referenced_tables() {
        let events_only =
            |q: &str| referenced_tables(q).as_deref() == Some(&["events".to_string()]);
        assert!(events_only("SELECT count(*) FROM events"));
        assert!(events_only(
            "SELECT host, count(*) FROM events GROUP BY host"
        ));
        // Hot-cache UDTFs are not the events table ⇒ not distributable.
        assert!(!events_only("SELECT value FROM distinct_values('host')"));
        assert!(!events_only("SELECT * FROM last_values()"));
        // No-table and other-table queries likewise.
        assert!(!events_only("SELECT 1"));
        assert!(!events_only("SELECT count(*) FROM candidates"));
    }

    #[test]
    fn default_order_rewrite_applies_to_plain_select() {
        let rewritten = rewrite_query_with_default_order(
            "SELECT host FROM events WHERE host = 'host-1'",
            25,
            |table| (table == "events").then_some("timestamp"),
        )
        .expect("rewrite");
        assert!(rewritten.contains("ORDER BY timestamp DESC"));
        // Ceiling+1 so the 413/`truncated` overflow signal survives.
        assert!(rewritten.contains("LIMIT 26"));
    }

    #[test]
    fn default_order_rewrite_preserves_existing_limit() {
        let rewritten =
            rewrite_query_with_default_order("SELECT host FROM events LIMIT 7", 25, |table| {
                (table == "events").then_some("timestamp")
            })
            .expect("rewrite");
        assert!(rewritten.contains("ORDER BY timestamp DESC"));
        assert!(rewritten.contains("LIMIT 7"));
        assert!(!rewritten.contains("LIMIT 25"));
    }

    #[test]
    fn default_order_rewrite_skips_disqualifiers() {
        for sql in [
            "SELECT host FROM events ORDER BY timestamp DESC",
            "SELECT DISTINCT host FROM events",
            "SELECT host, count(*) FROM events GROUP BY host",
            "SELECT count(*) FROM events",
            "SELECT host FROM events JOIN candidates ON events.host = candidates.host",
            "SELECT host FROM events UNION ALL SELECT host FROM events",
            "SELECT * FROM (SELECT * FROM events) t",
            // Aggregates beyond the obvious five — caught via the engine's
            // own aggregate registry, not a hardcoded name list.
            "SELECT approx_distinct(host) FROM events",
            "SELECT median(timestamp) FROM events",
            "SELECT array_agg(host) FROM events",
            "SELECT 1 + count(*) FROM events",
            // Windowed functions disqualify too.
            "SELECT row_number() OVER (ORDER BY timestamp) FROM events",
        ] {
            assert!(
                rewrite_query_with_default_order(sql, 25, |table| (table == "events")
                    .then_some("timestamp"))
                .is_none(),
                "expected no rewrite for `{sql}`"
            );
        }
    }

    /// #4090: a projection that names another column `timestamp` captures the
    /// injected bare identifier, so the rewrite would order the browse by that
    /// column. The shape disqualifies it; re-binding to the source column is
    /// not available (DataFusion calls a qualified reference here ambiguous,
    /// asserted end-to-end in tests/query_server/indexes.rs).
    #[test]
    fn default_order_rewrite_skips_a_projection_that_shadows_timestamp() {
        for sql in [
            "SELECT raw AS timestamp FROM events LIMIT 2",
            "SELECT raw AS TimeStamp FROM events LIMIT 2",
            "SELECT t.raw AS timestamp FROM events AS t LIMIT 2",
            "SELECT host, raw AS timestamp FROM events",
            "SELECT date_trunc('hour', timestamp) AS timestamp FROM events",
        ] {
            assert!(
                rewrite_query_with_default_order(sql, 25, |table| (table == "events")
                    .then_some("timestamp"))
                .is_none(),
                "expected no rewrite for `{sql}`"
            );
        }
        // Not shadowing: the output name is bound to the source column itself,
        // so the injected sort still means event time. Nor is an alias under
        // any other name.
        for sql in [
            "SELECT timestamp AS timestamp FROM events LIMIT 2",
            "SELECT t.timestamp AS timestamp FROM events AS t LIMIT 2",
            "SELECT raw AS ts FROM events LIMIT 2",
        ] {
            let rewritten = rewrite_query_with_default_order(sql, 25, |table| {
                (table == "events").then_some("timestamp")
            })
            .unwrap_or_else(|| panic!("expected a rewrite for `{sql}`"));
            assert!(rewritten.contains("ORDER BY timestamp DESC"), "{rewritten}");
        }
    }

    /// #4090: the same shadowing under the client's OWN `ORDER BY timestamp`.
    /// The sort is on the aliased column, so hinting the scan to produce
    /// timestamp order buys the plan nothing.
    #[test]
    fn preferred_scan_order_declines_a_shadowed_timestamp_alias() {
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT raw AS timestamp FROM events ORDER BY timestamp DESC LIMIT 5",
                "timestamp",
            ),
            None
        );
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT raw AS ts FROM events ORDER BY timestamp DESC LIMIT 5",
                "timestamp",
            ),
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT timestamp AS timestamp FROM events ORDER BY timestamp DESC LIMIT 5",
                "timestamp",
            ),
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );
    }

    /// #4214: a mapping may declare `timestamp` and `Timestamp` side by side,
    /// and a quoted identifier keeps its case. `"Timestamp"` is therefore a
    /// different column, and a projection binding it to the output name
    /// `timestamp` shadows the canonical one exactly as `raw AS timestamp`
    /// does. Unquoted `TIMESTAMP` still normalizes; quoted `"timestamp"` is
    /// still the canonical column.
    #[test]
    fn timestamp_recognition_respects_quoted_identifier_case() {
        for sql in [
            "SELECT \"Timestamp\" AS timestamp FROM events LIMIT 2",
            "SELECT t.\"Timestamp\" AS timestamp FROM events AS t LIMIT 2",
            "SELECT \"Timestamp\" AS TIMESTAMP FROM events LIMIT 2",
        ] {
            assert!(
                rewrite_query_with_default_order(sql, 25, |table| (table == "events")
                    .then_some("timestamp"))
                .is_none(),
                "expected no rewrite for `{sql}`"
            );
        }
        for sql in [
            // Unquoted, so case-normalized: still the canonical column, and
            // the alias is still the canonical output name.
            "SELECT TIMESTAMP AS timestamp FROM events LIMIT 2",
            "SELECT timestamp AS TimeStamp FROM events LIMIT 2",
            "SELECT \"timestamp\" AS timestamp FROM events LIMIT 2",
            "SELECT t.\"timestamp\" AS timestamp FROM events AS t LIMIT 2",
            // A quoted alias that is not the canonical output name cannot be
            // captured by the injected bare identifier.
            "SELECT raw AS \"Timestamp\" FROM events LIMIT 2",
        ] {
            let rewritten = rewrite_query_with_default_order(sql, 25, |table| {
                (table == "events").then_some("timestamp")
            })
            .unwrap_or_else(|| panic!("expected a rewrite for `{sql}`"));
            assert!(rewritten.contains("ORDER BY timestamp DESC"), "{rewritten}");
        }
    }

    /// #4214: an explicit sort on the quoted text column keeps its requested
    /// semantics and gets no canonical-timestamp scan hint — the scan cannot
    /// produce that ordering.
    #[test]
    fn preferred_scan_order_declines_a_quoted_mixed_case_timestamp() {
        for sql in [
            "SELECT raw FROM events ORDER BY \"Timestamp\" DESC LIMIT 5",
            "SELECT raw FROM events ORDER BY events.\"Timestamp\" DESC LIMIT 5",
            "SELECT \"Timestamp\" AS timestamp FROM events ORDER BY timestamp DESC LIMIT 5",
        ] {
            assert_eq!(
                preferred_scan_order_from_sql(sql, "timestamp"),
                None,
                "{sql}"
            );
        }
        for sql in [
            "SELECT raw FROM events ORDER BY \"timestamp\" DESC LIMIT 5",
            "SELECT raw FROM events ORDER BY events.\"timestamp\" DESC LIMIT 5",
            "SELECT raw FROM events ORDER BY TIMESTAMP DESC LIMIT 5",
        ] {
            assert_eq!(
                preferred_scan_order_from_sql(sql, "timestamp"),
                Some(siglake_storage::PreferredScanOrder::timestamp(true)),
                "{sql}"
            );
        }
    }

    /// #4375. The four shapes the 50G text gate runs are all clipped, and the
    /// two rare-term shapes #4329 added are not — which is the whole boundary
    /// the storage-side decline is built on, stated here against the AST.
    #[test]
    fn clipping_scan_limit_reads_the_shapes_the_text_gate_runs() {
        let limit_of = |sql: &str| {
            let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
            let Statement::Query(query) = &stmts[0] else {
                panic!("not a query: {sql}");
            };
            clipping_scan_limit(query)
        };

        for sql in [
            // keyword
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') LIMIT 100",
            // substring_scan
            "SELECT timestamp, raw FROM events WHERE raw LIKE '%checkout%' LIMIT 100",
            // keyword_last25 / keyword_last5: the time window sits in the same
            // WHERE and changes nothing about who clips whom.
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'queen') \
             AND timestamp >= TIMESTAMP '2026-09-01T00:00:00Z' LIMIT 100",
        ] {
            assert_eq!(limit_of(sql), Some(100), "{sql}");
        }
        // OFFSET is rows the scan still has to produce.
        assert_eq!(
            limit_of("SELECT raw FROM events WHERE raw LIKE '%x%' LIMIT 10 OFFSET 40"),
            Some(50)
        );

        for sql in [
            // rare_scan / rare_scan_last25: no LIMIT at all.
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'rareneedle')",
            "SELECT timestamp, raw FROM events WHERE match_terms(raw, 'rareneedle') \
             AND timestamp >= TIMESTAMP '2026-09-01T00:00:00Z'",
            // The limit clips the aggregate's ONE row, not the scan under it.
            "SELECT count(*) FROM events WHERE match_terms(raw, 'rareneedle') LIMIT 100",
            "SELECT host, count(*) FROM events WHERE raw LIKE '%x%' GROUP BY host LIMIT 100",
            "SELECT DISTINCT host FROM events WHERE raw LIKE '%x%' LIMIT 100",
            // A sort consumes the whole scan before the limit applies.
            "SELECT raw FROM events WHERE raw LIKE '%x%' ORDER BY host LIMIT 100",
            "SELECT raw FROM events WHERE raw LIKE '%x%' ORDER BY timestamp DESC LIMIT 100",
            // The inner scan is not the one being clipped.
            "SELECT raw FROM events WHERE raw LIKE '%x%' AND host IN \
             (SELECT host FROM events WHERE raw LIKE '%y%') LIMIT 100",
            "WITH m AS (SELECT raw FROM events WHERE raw LIKE '%x%') SELECT * FROM m LIMIT 100",
            // A join makes the scan's row count and the output's unrelated.
            "SELECT a.raw FROM events a JOIN events b ON a.host = b.host LIMIT 100",
            // Not a literal: nothing to carry.
            "SELECT raw FROM events WHERE raw LIKE '%x%' LIMIT ALL",
            // `LIMIT n BY expr` is n rows per group, so n is not the row count
            // the scan owes. `explicit_limit_value` refuses it for both hints.
            "SELECT raw FROM events WHERE raw LIKE '%x%' LIMIT 5 BY host",
        ] {
            assert_eq!(limit_of(sql), None, "{sql}");
        }
    }

    #[test]
    fn default_order_rewrite_respects_opt_out_and_batch() {
        let sql = "SELECT host FROM events";
        assert_eq!(
            apply_default_order_if_needed(sql, false, Priority::Interactive, 25, None).sql,
            sql
        );
        assert_eq!(
            apply_default_order_if_needed(sql, true, Priority::Batch, 25, None).sql,
            sql
        );
    }

    #[test]
    fn default_order_rewrite_covers_a_resolved_managed_index() {
        let sql = "SELECT timestamp, raw FROM \"logs-bench\" WHERE raw LIKE '%err%' LIMIT 100";
        // Without the resolved index the query is left exactly as written —
        // the events-only behaviour #4038 replaced.
        let unresolved = apply_default_order_if_needed(sql, true, Priority::Interactive, 25, None);
        assert_eq!(unresolved.sql, sql);
        assert_eq!(unresolved.preferred_scan_order, None);
        assert_eq!(unresolved.ordered_limit, None);
        // Un-rewritten it is a bare `LIMIT 100` over a text predicate: the
        // clipped decline is what keeps it off the whole-file index (#4375).
        assert_eq!(unresolved.clipped_limit, Some(100));

        let resolved = apply_default_order_if_needed(
            sql,
            true,
            Priority::Interactive,
            25,
            Some(("logs-bench", "timestamp")),
        );
        assert!(
            resolved.sql.contains("ORDER BY timestamp DESC"),
            "{}",
            resolved.sql
        );
        // The caller's explicit LIMIT survives — no ceiling+1 substitution.
        assert!(resolved.sql.ends_with("LIMIT 100"), "{}", resolved.sql);
        assert_eq!(
            resolved.preferred_scan_order,
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );
        assert_eq!(resolved.ordered_limit, Some(100));
        // Once the implicit newest-first ordering is injected the SAME query
        // is the ordered decline's, not the clipped one's — the two hints are
        // never both set, so the refusal has one attribution.
        assert_eq!(resolved.clipped_limit, None);

        // A different index in the same request is not the resolved one.
        assert_eq!(
            apply_default_order_if_needed(
                "SELECT timestamp FROM \"other-idx\" LIMIT 5",
                true,
                Priority::Interactive,
                25,
                Some(("logs-bench", "timestamp")),
            )
            .sql,
            "SELECT timestamp FROM \"other-idx\" LIMIT 5"
        );
    }

    #[test]
    fn ordered_scan_limit_includes_the_offset() {
        for (sql, expected) in [
            (
                "SELECT raw FROM events ORDER BY timestamp DESC LIMIT 100 OFFSET 25",
                Some(125),
            ),
            (
                "SELECT raw FROM events ORDER BY timestamp DESC LIMIT 25, 100",
                Some(125),
            ),
            (
                "SELECT raw FROM events ORDER BY timestamp DESC LIMIT 100 OFFSET dynamic_offset",
                None,
            ),
        ] {
            assert_eq!(
                apply_default_order_if_needed(sql, true, Priority::Interactive, 1_000, None)
                    .ordered_limit,
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn default_order_target_table_reports_the_shape_qualifying_table() {
        let table = |sql: &str| {
            let dialect = GenericDialect {};
            let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
            match stmts.pop().unwrap() {
                Statement::Query(query) => default_order_target_table(&query, "timestamp"),
                _ => None,
            }
        };
        assert_eq!(
            table("SELECT timestamp, raw FROM \"logs-bench\" LIMIT 10"),
            Some("logs-bench".to_string())
        );
        assert_eq!(table("SELECT count(*) FROM \"logs-bench\""), None);
        assert_eq!(
            table("SELECT timestamp FROM \"logs-bench\" ORDER BY raw LIMIT 10"),
            None
        );
    }

    #[test]
    fn default_order_rewrite_requires_timestamp_table() {
        assert!(
            rewrite_query_with_default_order("SELECT host FROM events", 25, |_| None).is_none()
        );
    }

    #[test]
    fn preferred_scan_order_only_applies_to_single_table_timestamp_order() {
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT timestamp FROM events ORDER BY timestamp DESC LIMIT 5",
                "timestamp",
            ),
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT timestamp FROM events ORDER BY events.timestamp ASC",
                "timestamp",
            ),
            Some(siglake_storage::PreferredScanOrder::timestamp(false))
        );
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT timestamp FROM events ORDER BY host DESC",
                "timestamp"
            ),
            None
        );
        assert_eq!(
            preferred_scan_order_from_sql(
                "SELECT timestamp FROM events JOIN candidates ON true ORDER BY timestamp DESC",
                "timestamp",
            ),
            None
        );
    }

    #[test]
    fn inline_lane_admits_positive_small_estimates_and_rejects_punts() {
        let base = CostReport {
            files_to_scan: Some(2),
            files_considered: None,
            estimated_bytes_scanned: 1024,
            estimated_rows_processed: 500,
            estimated_runtime_seconds: 0.0,
            complexity_class: crate::cost::ComplexityClass::Small,
            warnings: Vec::new(),
            exact: false,
        };
        // Inexact-but-small: eligible (the 2026-07-22 census fix — the old
        // exact-only gate recorded zero routes).
        assert!(inline_exec_eligible(&base));
        // Estimator punt (zero rows = the ordered browses): never inline.
        assert!(!inline_exec_eligible(&CostReport {
            estimated_rows_processed: 0,
            ..base.clone()
        }));
        // Over either cap: pool.
        assert!(!inline_exec_eligible(&CostReport {
            estimated_rows_processed: 300_000,
            ..base.clone()
        }));
        assert!(!inline_exec_eligible(&CostReport {
            estimated_bytes_scanned: 1 << 30,
            ..base
        }));
    }

    #[test]
    fn detect_ordered_residual_browse_matches_single_dim_ordered_limits() {
        let shape = |sql: &str| detect_ordered_residual_browse(sql, "timestamp");
        // Equality with a time window (the attr_filter_provider_rows shape).
        assert_eq!(
            shape(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND timestamp >= '2024-01-01T00:00:00Z' ORDER BY timestamp DESC LIMIT 100"
            ),
            Some(ResidualBrowseShape {
                table: "logs-bench".into(),
                column: "cloud_provider".into(),
                values: vec!["gcp".into()],
                negated: false,
            })
        );
        // IN + negation polarities.
        assert_eq!(
            shape("SELECT raw FROM events WHERE host IN ('a', 'b') ORDER BY timestamp ASC LIMIT 5")
                .map(|s| (s.values, s.negated)),
            Some((vec!["a".into(), "b".into()], false))
        );
        assert_eq!(
            shape("SELECT raw FROM events WHERE level <> 'debug' ORDER BY timestamp DESC LIMIT 10")
                .map(|s| (s.values, s.negated)),
            Some((vec!["debug".into()], true))
        );
        // Disqualifiers: two dimension terms, OR, LIKE, no LIMIT, wrong sort
        // key, aggregates.
        assert_eq!(
            shape(
                "SELECT raw FROM events WHERE host = 'a' AND level = 'b' \
                 ORDER BY timestamp DESC LIMIT 5"
            ),
            None
        );
        assert_eq!(
            shape(
                "SELECT raw FROM events WHERE host = 'a' OR host = 'b' \
                 ORDER BY timestamp DESC LIMIT 5"
            ),
            None
        );
        assert_eq!(
            shape(
                "SELECT raw FROM events WHERE raw LIKE '%x%' AND host = 'a' \
                 ORDER BY timestamp DESC LIMIT 5"
            ),
            None
        );
        assert_eq!(
            shape("SELECT raw FROM events WHERE host = 'a' ORDER BY timestamp DESC"),
            None
        );
        assert_eq!(
            shape("SELECT raw FROM events WHERE host = 'a' ORDER BY host DESC LIMIT 5"),
            None
        );
        assert_eq!(
            shape("SELECT count(*) FROM events WHERE host = 'a' ORDER BY timestamp DESC LIMIT 5"),
            None
        );
    }

    /// #4217: both detectors read the WHERE clause's identifiers under SQL's
    /// case rules. An unquoted `TIMESTAMP` range is the same pure time range as
    /// `timestamp` — before this, the term fell through to the dimensional arm
    /// and either displaced the real dimension or (with one already present)
    /// lost the shape entirely. A quoted `"Timestamp"` stays a different column.
    #[test]
    fn browse_detectors_read_an_unquoted_timestamp_as_the_canonical_column() {
        let ordered = |sql: &str| detect_ordered_residual_browse(sql, "timestamp");
        let plain = |sql: &str| detect_dim_browse(sql, "timestamp");
        let expected = Some(ResidualBrowseShape {
            table: "logs-bench".into(),
            column: "cloud_provider".into(),
            values: vec!["gcp".into()],
            negated: false,
        });

        // One dimensional predicate + a time range + LIMIT: the unquoted range
        // classifies exactly as the lowercase one, ordered and plain alike.
        assert_eq!(
            ordered(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND TIMESTAMP >= '2024-01-01T00:00:00Z' ORDER BY timestamp DESC LIMIT 100"
            ),
            expected
        );
        assert_eq!(
            ordered(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND timestamp >= '2024-01-01T00:00:00Z' ORDER BY timestamp DESC LIMIT 100"
            ),
            expected
        );
        assert_eq!(
            plain(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND TIMESTAMP >= '2024-01-01T00:00:00Z' LIMIT 100"
            ),
            expected
        );
        assert_eq!(
            plain(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND timestamp >= '2024-01-01T00:00:00Z' LIMIT 100"
            ),
            expected
        );
        // A compound `t.TIMESTAMP` qualifier reads the same way.
        assert_eq!(
            plain(
                "SELECT timestamp, raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND \"logs-bench\".TIMESTAMP >= '2024-01-01T00:00:00Z' LIMIT 100"
            ),
            expected
        );

        // The equality and IN exclusions apply to the unquoted spelling too:
        // an equality on the event-time column is no dimension, so the shape is
        // refused rather than reported with `TIMESTAMP` as its dimension.
        for sql in [
            "SELECT raw FROM \"logs-bench\" WHERE TIMESTAMP = '2024-01-01T00:00:00Z' LIMIT 5",
            "SELECT raw FROM \"logs-bench\" WHERE TIMESTAMP <> '2024-01-01T00:00:00Z' LIMIT 5",
            "SELECT raw FROM \"logs-bench\" WHERE TIMESTAMP IN ('2024-01-01T00:00:00Z') LIMIT 5",
        ] {
            assert_eq!(plain(sql), None, "should refuse: {sql}");
            let with_order = sql.replace("LIMIT 5", "ORDER BY timestamp DESC LIMIT 5");
            assert_eq!(ordered(&with_order), None, "should refuse: {with_order}");
        }

        // Quoted `"Timestamp"` is an ordinary column: its equality IS the
        // dimensional term, and it does not stand in for the time range.
        assert_eq!(
            plain(
                "SELECT raw FROM \"logs-bench\" WHERE \"Timestamp\" = 'gcp' \
                 AND timestamp >= '2024-01-01T00:00:00Z' LIMIT 100"
            ),
            Some(ResidualBrowseShape {
                table: "logs-bench".into(),
                column: "Timestamp".into(),
                values: vec!["gcp".into()],
                negated: false,
            })
        );
        // ... so a range on it is neither a time range nor a dimension, and the
        // shape is refused.
        assert_eq!(
            plain(
                "SELECT raw FROM \"logs-bench\" WHERE cloud_provider = 'gcp' \
                 AND \"Timestamp\" >= '2024-01-01T00:00:00Z' LIMIT 100"
            ),
            None
        );
    }

    fn case_distinct_browse_config() -> IndexConfig {
        IndexConfig {
            index_id: "case-distinct".to_string(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    field("timestamp", FieldType::Datetime, true),
                    field(
                        "region",
                        FieldType::Text {
                            tokenizer: Some("raw".to_string()),
                        },
                        true,
                    ),
                    field(
                        "Region",
                        FieldType::Text {
                            tokenizer: Some("raw".to_string()),
                        },
                        true,
                    ),
                ],
                timestamp_field: "timestamp".to_string(),
                tag_fields: vec!["region".to_string(), "Region".to_string()],
                default_search_fields: Vec::new(),
            },
            retention: None,
            index_at_flush: None,
        }
    }

    async fn case_distinct_browse_fixture() -> (tempfile::TempDir, IcebergContext) {
        use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};

        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let config = case_distinct_browse_config();
        ice.create_index(&config).await.unwrap();

        // `region='probe'` is below the ordered-hint threshold (1/200), while
        // the distinct quoted `Region='probe'` is common (100/200).
        let region: Vec<&str> = (0..200)
            .map(|row| if row == 0 { "probe" } else { "other" })
            .collect();
        let title_region: Vec<&str> = (0..200)
            .map(|row| if row < 100 { "probe" } else { "other" })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(0..200)
                    .with_timezone(siglake_core::TIMESTAMP_TZ),
            ),
            Arc::new(StringArray::from(region)),
            Arc::new(StringArray::from(title_region)),
            Arc::new(StringArray::from(vec![None::<&str>; 200])),
        ];
        let batch = RecordBatch::try_new(config.to_arrow_schema(), arrays).unwrap();
        ice.append_to_table(
            &ice.index_table_ident("case-distinct"),
            batch,
            &["region", "Region"],
        )
        .await
        .unwrap();
        (tmp, ice)
    }

    /// #5024: both browse-policy consumers use the same quote-aware column
    /// name as DataFusion. Exact aggregate keys remain case-sensitive, so the
    /// two mapped columns below deliberately produce different answers.
    #[tokio::test]
    async fn browse_policies_resolve_dimension_case_like_datafusion() {
        let (_tmp, ice) = case_distinct_browse_fixture().await;

        let expected_scans = [
            // Equality and IN: unquoted names bind to lowercase `region`.
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE region = 'probe' LIMIT 4",
                Some(200),
            ),
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE REGION = 'probe' LIMIT 4",
                Some(200),
            ),
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE REGION IN ('probe') LIMIT 4",
                Some(200),
            ),
            // Quoting selects the distinct mixed-case aggregate.
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE \"Region\" = 'probe' LIMIT 4",
                Some(8),
            ),
            // Both negated forms retain the same distinction.
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE REGION <> 'other' LIMIT 4",
                Some(200),
            ),
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE REGION NOT IN ('other') LIMIT 4",
                Some(200),
            ),
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE \"Region\" <> 'other' LIMIT 4",
                Some(8),
            ),
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE \"Region\" NOT IN ('other') LIMIT 4",
                Some(8),
            ),
            // No exact schema/aggregate key: preserve the conservative fallback.
            (
                "SELECT timestamp FROM \"case-distinct\" WHERE \"REGION\" = 'probe' LIMIT 4",
                None,
            ),
        ];
        for (sql, expected) in expected_scans {
            assert_eq!(
                browse_expected_scan_rows(&ice, sql, 4).await,
                expected,
                "distribution estimate for {sql}"
            );
        }

        let ordered_cases = [
            ("region = 'probe'", false),
            ("REGION = 'probe'", false),
            ("REGION IN ('probe')", false),
            ("REGION <> 'other'", false),
            ("REGION NOT IN ('other')", false),
            ("\"Region\" = 'probe'", true),
            ("\"Region\" IN ('probe')", true),
            ("\"Region\" <> 'other'", true),
            ("\"Region\" NOT IN ('other')", true),
            ("\"REGION\" = 'probe'", false),
        ];
        for (predicate, expected) in ordered_cases {
            let sql = format!(
                "SELECT timestamp FROM \"case-distinct\" WHERE {predicate} \
                 ORDER BY timestamp DESC LIMIT 4"
            );
            let shape =
                detect_ordered_residual_browse(&sql, "timestamp").expect("ordered browse shape");
            assert_eq!(
                residual_browse_low_selectivity_at(&ice, &shape, 0.01).await,
                expected,
                "ordered hint for {sql}"
            );
        }
    }

    /// `logs_config`'s twin with the canonical event-time column, i.e. what
    /// every shipped index template and every bulk-created index declares.
    fn timestamped_index_config(index_id: &str) -> IndexConfig {
        let mut config = logs_config(vec![]);
        config.index_id = index_id.to_string();
        config.doc_mapping.field_mappings[0] = field("timestamp", FieldType::Datetime, true);
        config.doc_mapping.timestamp_field = "timestamp".to_string();
        config
    }

    #[tokio::test]
    async fn default_order_resolves_a_managed_index_and_its_event_time_field() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&timestamped_index_config("logs-bench"))
            .await
            .unwrap();
        // `logs` declares `ts` as its event-time field: a column called
        // `timestamp` there (if any) is not a time order, and `ts` is.
        ice.create_index(&logs_config(vec![])).await.unwrap();

        let resolve = |sql: &'static str, enabled: bool, priority: Priority| {
            let ice = ice.clone();
            async move { resolve_default_order_index(&ice, sql, enabled, priority).await }
        };

        assert_eq!(
            resolve(
                "SELECT timestamp, raw FROM \"logs-bench\" LIMIT 100",
                true,
                Priority::Interactive
            )
            .await,
            Some(("logs-bench".to_string(), "timestamp".to_string()))
        );
        // A mapping's own event-time field resolves as itself (#6020) — the
        // rewrite and the scan hint then name `ts`, not `timestamp`.
        assert_eq!(
            resolve("SELECT ts FROM logs LIMIT 10", true, Priority::Interactive).await,
            Some(("logs".to_string(), "ts".to_string()))
        );
        // Negative coverage: an unknown name, the statically known tables, the
        // opt-out and batch.
        for (sql, enabled, priority) in [
            (
                "SELECT timestamp FROM \"no-such-index\" LIMIT 10",
                true,
                Priority::Interactive,
            ),
            (
                "SELECT timestamp FROM events LIMIT 10",
                true,
                Priority::Interactive,
            ),
            (
                "SELECT timestamp FROM \"logs-bench\" ORDER BY raw LIMIT 10",
                true,
                Priority::Interactive,
            ),
            (
                "SELECT timestamp FROM \"logs-bench\" LIMIT 10",
                false,
                Priority::Interactive,
            ),
            (
                "SELECT timestamp FROM \"logs-bench\" LIMIT 10",
                true,
                Priority::Batch,
            ),
        ] {
            assert_eq!(
                resolve(sql, enabled, priority).await,
                None,
                "expected no index-driven default order for `{sql}`"
            );
        }
    }

    #[tokio::test]
    async fn query_rewrites_order_a_bare_index_select_newest_first() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&timestamped_index_config("logs-bench"))
            .await
            .unwrap();
        ice.create_index(&logs_config(vec![])).await.unwrap();

        let rewritten = apply_query_rewrites(
            &ice,
            "SELECT timestamp, raw FROM \"logs-bench\" WHERE raw LIKE '%err%' LIMIT 100",
            true,
            Priority::Interactive,
            25,
        )
        .await
        .unwrap();
        assert!(
            rewritten.sql.contains("ORDER BY timestamp DESC"),
            "{}",
            rewritten.sql
        );
        assert_eq!(
            rewritten.preferred_scan_order,
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );
        assert_eq!(rewritten.ordered_limit, Some(100));

        // An index whose event-time field is `ts` is ordered by `ts`, QUOTED
        // so the injected identifier binds that exact column whatever its case
        // (#6020). The hint names the field as well as the direction.
        let custom = apply_query_rewrites(
            &ice,
            "SELECT ts, message FROM logs LIMIT 100",
            true,
            Priority::Interactive,
            25,
        )
        .await
        .unwrap();
        assert_eq!(
            custom.sql,
            "SELECT ts, message FROM logs ORDER BY \"ts\" DESC LIMIT 100"
        );
        assert_eq!(
            custom.preferred_scan_order,
            Some(siglake_storage::PreferredScanOrder::new("ts", true))
        );
        assert_eq!(custom.ordered_limit, Some(100));

        // The canonical column of a `ts`-mapped index is NOT its event time.
        // The query stands as written and the hint names the column it asked
        // for, `timestamp` — which the scan refuses against a `ts` sort lead,
        // so the browse keeps its blocking sort exactly as it does today
        // (`query_provider::tests::custom_event_time_sort_is_advertised_for_its_own_field`).
        // Deciding that here instead would cost a mapping lookup on every
        // `ORDER BY timestamp` over an index, to reach the same plan.
        let unrelated = apply_query_rewrites(
            &ice,
            "SELECT ts, message FROM logs ORDER BY timestamp DESC LIMIT 100",
            true,
            Priority::Interactive,
            25,
        )
        .await
        .unwrap();
        assert_eq!(
            unrelated.sql,
            "SELECT ts, message FROM logs ORDER BY timestamp DESC LIMIT 100"
        );
        assert_eq!(
            unrelated.preferred_scan_order,
            Some(siglake_storage::PreferredScanOrder::timestamp(true))
        );

        // An explicit order on the mapping's own field keeps the query as
        // written and DOES carry the hint.
        let explicit = apply_query_rewrites(
            &ice,
            "SELECT ts, message FROM logs ORDER BY ts DESC LIMIT 5",
            true,
            Priority::Interactive,
            25,
        )
        .await
        .unwrap();
        assert_eq!(
            explicit.sql,
            "SELECT ts, message FROM logs ORDER BY ts DESC LIMIT 5"
        );
        assert_eq!(
            explicit.preferred_scan_order,
            Some(siglake_storage::PreferredScanOrder::new("ts", true))
        );
        assert_eq!(explicit.ordered_limit, Some(5));

        // `default_order: false` and a batch request keep file order and no
        // hint, exactly as they do for a canonical index.
        for (default_order, priority) in [(false, Priority::Interactive), (true, Priority::Batch)] {
            let opted_out = apply_query_rewrites(
                &ice,
                "SELECT ts, message FROM logs LIMIT 100",
                default_order,
                priority,
                25,
            )
            .await
            .unwrap();
            assert_eq!(opted_out.sql, "SELECT ts, message FROM logs LIMIT 100");
            assert_eq!(opted_out.preferred_scan_order, None);
        }

        // Alias shadowing, on the mapping's field: `SELECT raw AS ts` captures
        // the injected `ORDER BY "ts"`, so the rewrite declines (#4090's guard,
        // per-index).
        let shadowed = apply_query_rewrites(
            &ice,
            "SELECT message AS ts FROM logs LIMIT 100",
            true,
            Priority::Interactive,
            25,
        )
        .await
        .unwrap();
        assert_eq!(shadowed.sql, "SELECT message AS ts FROM logs LIMIT 100");
        assert_eq!(shadowed.preferred_scan_order, None);
    }

    #[tokio::test]
    async fn search_rewrite_expands_default_search_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&logs_config(vec!["message", "title"]))
            .await
            .unwrap();
        let rewritten = rewrite_search_if_needed(
            &ice,
            "SELECT * FROM logs WHERE search('timeout') AND ts IS NOT NULL",
        )
        .await
        .unwrap();
        assert!(rewritten.contains("match_terms(message, 'timeout')"));
        assert!(rewritten.contains("OR match_terms(title, 'timeout')"));
    }

    #[tokio::test]
    async fn warm_search_rewrite_does_not_reload_table_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let ice = IcebergContext::open(&warehouse)
            .await
            .unwrap()
            .with_table_cache_ttl(std::time::Duration::from_secs(3600))
            .with_tuning(siglake_storage::iceberg::IcebergTuning {
                result_caches: Some(false),
                ..Default::default()
            });
        ice.create_index(&logs_config(vec!["message", "title"]))
            .await
            .unwrap();
        let query = "SELECT * FROM logs WHERE search('timeout')";
        let warm = rewrite_search_if_needed(&ice, query).await.unwrap();

        let removed = strip_table_metadata_for_test(&warehouse, "logs");
        assert!(removed > 0, "the fixture removed no metadata.json files");
        assert!(
            ice.get_index("logs").await.is_err(),
            "the uncached control did not reach metadata storage"
        );

        assert_eq!(
            rewrite_search_if_needed(&ice, query).await.unwrap(),
            warm,
            "the warm rewrite must use the cached mapping"
        );
    }

    #[tokio::test]
    async fn search_rewrite_rejects_unsupported_shapes_and_missing_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&logs_config(vec![])).await.unwrap();

        for sql in [
            "SELECT * FROM events e JOIN candidates c ON e.host = c.host WHERE search('x')",
            "SELECT * FROM (SELECT * FROM events) t WHERE search('x')",
        ] {
            let err = rewrite_search_if_needed(&ice, sql).await.unwrap_err();
            assert!(err.msg.contains(SEARCH_GUIDANCE), "{sql} -> {}", err.msg);
        }

        let err = rewrite_search_if_needed(&ice, "SELECT * FROM logs WHERE search('x')")
            .await
            .unwrap_err();
        assert!(err.msg.contains(SEARCH_GUIDANCE));
    }

    #[tokio::test]
    async fn search_rewrite_rejects_unknown_tables() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let err = rewrite_search_if_needed(&ice, "SELECT * FROM unknown WHERE search('x')")
            .await
            .unwrap_err();
        assert!(err.msg.contains(SEARCH_GUIDANCE));
    }

    fn attr_rewrite_maps(entries: &[(&str, &str, &str)]) -> HashMap<String, AttrRewriteMap> {
        let mut maps = HashMap::<String, AttrRewriteMap>::new();
        for (table, key, column) in entries {
            maps.entry((*table).into()).or_default().insert(
                (*key).into(),
                ((*column).into(), siglake_core::PromotedType::Utf8),
            );
        }
        maps
    }

    /// THE DEFECT THIS GUARDS: a predicate-only second table used to make the
    /// query-wide `tables.len() == 1` gate skip an otherwise sound events
    /// rewrite. Ownership is lexical: the outer SELECT still has one source.
    #[test]
    fn attr_rewrite_uses_the_owning_select_with_a_predicate_subquery() {
        let sql = "SELECT attr_get(attributes, 'k') AS value FROM events \
                   WHERE EXISTS (SELECT 1 FROM idx)";
        let rewritten =
            rewrite_attr_get_to_columns(sql, &attr_rewrite_maps(&[("events", "k", "event_k")]))
                .expect("outer events attr_get rewrites");
        assert!(rewritten.contains("SELECT event_k AS value FROM events"));
        assert!(rewritten.contains("EXISTS (SELECT 1 FROM idx)"));
    }

    #[tokio::test]
    async fn query_rewrites_load_the_promoted_owner_map_in_a_two_table_query() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap()
            .with_promoted_columns(vec![siglake_core::PromotedColumn {
                attr_key: "k".into(),
                name: "event_k".into(),
                ty: siglake_core::PromotedType::Utf8,
            }]);
        ice.ensure_promoted_columns().await.unwrap();
        assert!(ice.ensure_promotion_backfill_property().await.unwrap());
        ice.create_index(&logs_config(vec![])).await.unwrap();

        let rewritten = apply_query_rewrites(
            &ice,
            "SELECT attr_get(attributes, 'k') AS value FROM events \
             WHERE EXISTS (SELECT 1 FROM logs)",
            false,
            Priority::Interactive,
            100,
        )
        .await
        .unwrap();
        assert!(
            rewritten
                .sql
                .contains("SELECT event_k AS value FROM events"),
            "{}",
            rewritten.sql
        );
    }

    #[test]
    fn attr_rewrite_preserves_qualifiers_and_each_tables_gate() {
        let sql = "SELECT attr_get(e.attributes, 'k'), attr_get(i.attributes, 'k') \
                   FROM events AS e JOIN idx AS i ON true";
        let rewritten =
            rewrite_attr_get_to_columns(sql, &attr_rewrite_maps(&[("events", "k", "event_k")]))
                .expect("the gated events table rewrites");
        assert!(rewritten.contains("e.event_k"), "{rewritten}");
        assert!(
            rewritten.contains("attr_get(i.attributes, 'k')"),
            "ungated idx expression changed: {rewritten}"
        );

        assert!(
            rewrite_attr_get_to_columns(
                "SELECT attr_get(attributes, 'k') FROM events JOIN idx ON true",
                &attr_rewrite_maps(&[("events", "k", "event_k"), ("idx", "k", "idx_k")]),
            )
            .is_none(),
            "an unqualified two-source column owner is ambiguous"
        );
    }

    #[test]
    fn attr_rewrite_honors_alias_and_cte_shadowing() {
        let maps = attr_rewrite_maps(&[("events", "k", "event_k"), ("idx", "k", "idx_k")]);
        let aliased = rewrite_attr_get_to_columns(
            "SELECT attr_get(events.attributes, 'k') FROM idx AS events",
            &maps,
        )
        .expect("the alias proves idx owns the qualified column");
        assert!(aliased.contains("events.idx_k"), "{aliased}");
        assert!(!aliased.contains("event_k"), "alias shadow lost: {aliased}");

        assert!(
            rewrite_attr_get_to_columns(
                "WITH events AS (SELECT * FROM idx) \
                 SELECT attr_get(attributes, 'k') FROM events",
                &maps,
            )
            .is_none(),
            "a CTE named events must not acquire the physical events map"
        );
    }

    #[test]
    fn attr_rewrite_keeps_single_table_set_operations() {
        let rewritten = rewrite_attr_get_to_columns(
            "SELECT attr_get(attributes, 'k') FROM events \
             UNION ALL SELECT attr_get(attributes, 'k') FROM events",
            &attr_rewrite_maps(&[("events", "k", "event_k")]),
        )
        .expect("both independently single-table branches rewrite");
        assert_eq!(rewritten.matches("event_k").count(), 2, "{rewritten}");
    }

    #[tokio::test]
    async fn predicate_subquery_attr_rewrite_preserves_results() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let ctx = SessionContext::new();
        crate::udfs::register_udfs(&ctx);
        let events_schema = Arc::new(Schema::new(vec![
            Field::new("attributes", DataType::Utf8, true),
            Field::new("event_k", DataType::Utf8, true),
        ]));
        let events = RecordBatch::try_new(
            events_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![r#"{"k":"b"}"#, r#"{"k":"a"}"#])),
                Arc::new(StringArray::from(vec!["b", "a"])),
            ],
        )
        .unwrap();
        ctx.register_batch("events", events).unwrap();
        let idx_schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let idx =
            RecordBatch::try_new(idx_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        ctx.register_batch("idx", idx).unwrap();

        let original = "SELECT attr_get(attributes, 'k') AS value FROM events \
                        WHERE EXISTS (SELECT 1 FROM idx) ORDER BY value";
        let rewritten = rewrite_attr_get_to_columns(
            original,
            &attr_rewrite_maps(&[("events", "k", "event_k")]),
        )
        .unwrap();
        let before = ctx.sql(original).await.unwrap().collect().await.unwrap();
        let after = ctx.sql(&rewritten).await.unwrap().collect().await.unwrap();
        assert_eq!(
            crate::format::batches_to_records(&before, None)
                .unwrap()
                .rows,
            crate::format::batches_to_records(&after, None)
                .unwrap()
                .rows
        );
    }

    async fn plan_for(query: &str) -> DataFrame {
        let events: Vec<Event> = (0..4).map(smoke_event).collect();
        let (_tmp, _ice, _ctx, df) = dataframe_for(query, events).await;
        df
    }

    async fn collect_records(query: &str) -> crate::format::RecordsResponse {
        let events: Vec<Event> = (0..4).map(smoke_event).collect();
        let (_tmp, _ice, _ctx, df) = dataframe_for(query, events).await;
        let batches = df.collect().await.unwrap();
        crate::format::batches_to_records(&batches, None).unwrap()
    }

    async fn physical_plan_string(query: &str) -> String {
        let events: Vec<Event> = (0..4).map(smoke_event).collect();
        let (_tmp, _ice, _ctx, df) = dataframe_for(query, events).await;
        let plan = df.create_physical_plan().await.unwrap();
        let rendered = displayable(plan.as_ref()).indent(true).to_string();
        rendered
    }

    fn smoke_event(i: usize) -> Event {
        Event {
            timestamp: Utc::now(),
            host: format!("host-{i}"),
            source: "smoke".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        }
    }

    #[tokio::test]
    async fn result_cache_decisions_cover_aggregates_and_the_buffer_guard() {
        let (_tmp, ice, _ctx, _df) =
            dataframe_for("SELECT 1", (0..2).map(smoke_event).collect()).await;
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let decide = |sql: &'static str, delta: BufferDeltaState| {
            let ice = ice.clone();
            let resolved = resolved.clone();
            async move {
                prepare_result_cache(
                    &ice,
                    sql,
                    false,
                    QueryFormat::Records,
                    &resolved,
                    delta,
                    BufferProof::default(),
                    ResultCacheScope {
                        allow_approximate: AllowApproximate::always(),
                        shard: None,
                    },
                    far_future_deadline(),
                )
                .await
                .unwrap()
            }
        };
        // GROUP BY aggregate with no WHERE is now cacheable (the http_logs
        // avg_size_by_status class).
        assert!(matches!(
            decide(
                "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 5",
                BufferDeltaState::Empty
            )
            .await,
            SqlResultCacheDecision::Leader(_)
        ));
        // A NON-empty buffer delta skips the cache entirely — buffered rows
        // change results without a snapshot change (hit AND insert unsafe).
        assert!(matches!(
            decide(
                "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 5",
                BufferDeltaState::NonEmpty
            )
            .await,
            SqlResultCacheDecision::Skip
        ));
        // An UNKNOWN delta (the load failed) is treated as the unsafe case, not
        // as the empty one: nothing proved the buffer contributes nothing.
        assert!(matches!(
            decide(
                "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 5",
                BufferDeltaState::Unknown
            )
            .await,
            SqlResultCacheDecision::Skip
        ));
        // Bare unfiltered scans stay uncached.
        assert!(matches!(
            decide(
                "SELECT timestamp, raw FROM events LIMIT 10",
                BufferDeltaState::Empty
            )
            .await,
            SqlResultCacheDecision::Skip
        ));
        // Ordered LIMIT browses (match_all / deep pagination) are cacheable —
        // the http_logs board's last red cells were repeat browses re-paying
        // the newest file's S3 tail fetch per request.
        assert!(matches!(
            decide(
                "SELECT timestamp, raw FROM events ORDER BY timestamp DESC LIMIT 100",
                BufferDeltaState::Empty
            )
            .await,
            SqlResultCacheDecision::Leader(_)
        ));
        // Filtered events queries keep their existing cacheability.
        assert!(matches!(
            decide(
                "SELECT count(*) AS n FROM events WHERE host = 'h-1'",
                BufferDeltaState::Empty
            )
            .await,
            SqlResultCacheDecision::Leader(_)
        ));
        // ...and so does the byte-identical query a client PRETTY-PRINTED. The
        // superseded shape filter matched " where " / " group by " on the raw
        // text, so a newline before the clause silently cost that client its
        // cache while the single-line spelling above kept it.
        for sql in [
            "SELECT count(*) AS n\nFROM events\nWHERE host = 'h-1'",
            "SELECT host, count(*) AS n\n  FROM events\n GROUP BY host",
            "SELECT\n  host,\n  count(*) AS n\nFROM events\nWHERE host <> 'h-0'\nGROUP BY host\nORDER BY n DESC\nLIMIT 5",
            "SELECT timestamp, raw\nFROM events\nORDER BY timestamp DESC\nLIMIT 100",
            // WHERE that only exists inside a derived table: the substring scan
            // counted it, and a query with a predicate anywhere is not a bare
            // scan.
            "SELECT count(*) AS n FROM (\n  SELECT host FROM events\n  WHERE host = 'h-1'\n) t",
        ] {
            assert!(
                matches!(
                    decide(sql, BufferDeltaState::Empty).await,
                    SqlResultCacheDecision::Leader(_)
                ),
                "a multi-line cacheable shape was skipped: {sql}"
            );
        }
        // Bare scans stay uncached however they are formatted, and an ORDER BY
        // that only appears in a window function's OVER(...) is not a browse.
        for sql in [
            "SELECT timestamp, raw\nFROM events\nLIMIT 10",
            "SELECT\n  timestamp,\n  raw\nFROM events",
            "SELECT row_number() OVER (ORDER BY timestamp) AS n, raw FROM events",
        ] {
            assert!(
                matches!(
                    decide(sql, BufferDeltaState::Empty).await,
                    SqlResultCacheDecision::Skip
                ),
                "a bare scan entered the cache: {sql}"
            );
        }
        // Cacheable SHAPES that are not snapshot-pure: every one of these has a
        // WHERE and a single known table, so shape alone would admit them.
        // `Skip` means neither a lookup nor an insertion — no leader ctx exists
        // to carry a body into the cache. The three spellings are the ones the
        // superseded substring scan missed.
        for sql in [
            "SELECT count(*) AS n FROM events WHERE timestamp >= now() - interval '15 minutes'",
            "SELECT count(*) AS n FROM events WHERE timestamp >= now () - interval '15 minutes'",
            "SELECT count(*) AS n FROM events WHERE timestamp >= now /* gap */ () - interval '15 minutes'",
            "SELECT count(*) AS n FROM events WHERE random() < 0.1",
            "SELECT count(*) AS n FROM events WHERE host = 'h-1' AND uuid() IS NOT NULL",
        ] {
            assert!(
                matches!(decide(sql, BufferDeltaState::Empty).await, SqlResultCacheDecision::Skip),
                "a non-snapshot-pure query entered the cache: {sql}"
            );
        }
    }

    /// The single-flight wait is bounded by the SMALLER of the flat cap and
    /// what is left of the request's own budget.
    #[test]
    fn a_follower_waits_no_longer_than_its_own_budget() {
        let limits = RequestLimits {
            timeout_seconds: Some(2),
            ..Default::default()
        };
        let resolved = ResolvedLimits::resolve(
            &limits,
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let now = tokio::time::Instant::now();
        let short = cache_wait_budget(&resolved, now + std::time::Duration::from_secs(2));
        assert!(
            short < SQL_RESULT_CACHE_WAIT,
            "a two-second request was allowed to park for the ten-second cap: {short:?}"
        );
        // Past the deadline there is nothing left to spend.
        assert!(cache_wait_budget(&resolved, now - std::time::Duration::from_secs(1)).is_zero());
        // A budget longer than the cap is still capped.
        assert_eq!(
            cache_wait_budget(&resolved, now + std::time::Duration::from_secs(600)),
            SQL_RESULT_CACHE_WAIT
        );

        // Breakers off means no request deadline to respect: only the cap.
        let no_breakers = RequestLimits {
            circuit_breakers: Some(false),
            ..Default::default()
        };
        let unbounded = ResolvedLimits::resolve(
            &no_breakers,
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        assert_eq!(
            cache_wait_budget(&unbounded, now - std::time::Duration::from_secs(1)),
            SQL_RESULT_CACHE_WAIT
        );
    }

    /// And the bound is real end to end: a follower behind a live leader gives
    /// up when ITS budget runs out, not ten seconds later.
    #[tokio::test]
    async fn a_short_budget_follower_gives_up_before_the_flat_cap() {
        let (_tmp, ice, _ctx, _df) =
            dataframe_for("SELECT 1", (0..2).map(smoke_event).collect()).await;
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        // Unique shape: the in-flight map is process-global.
        let sql = "SELECT count(*) AS n FROM events WHERE host = 'short-budget-follower'";
        let probe = |deadline| {
            prepare_result_cache(
                &ice,
                sql,
                false,
                QueryFormat::Records,
                &resolved,
                BufferDeltaState::Empty,
                BufferProof::default(),
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
                deadline,
            )
        };
        let SqlResultCacheDecision::Leader(leader) = probe(far_future_deadline()).await.unwrap()
        else {
            panic!("the first probe must lead");
        };

        // The leader is still working (it is held here), so this one parks.
        let budget = std::time::Duration::from_millis(300);
        let started = Instant::now();
        let follower = probe(tokio::time::Instant::now() + budget).await.unwrap();
        let waited = started.elapsed();
        assert!(
            matches!(follower, SqlResultCacheDecision::Skip),
            "a follower that ran out of budget must hand the decision back"
        );
        assert!(
            waited < SQL_RESULT_CACHE_WAIT,
            "the follower parked for the flat cap ({waited:?}) instead of its own budget"
        );

        // Cancelling that follower must not have disturbed the leader's marker.
        drop(leader);
        // ... and dropping the leader — the shape of every `?` return from a
        // preparation timeout — releases the key for the next request.
        assert!(matches!(
            probe(far_future_deadline()).await.unwrap(),
            SqlResultCacheDecision::Leader(_)
        ));
    }

    /// Every ineligible buffer state must neither be served from the result
    /// cache nor allowed to populate it. Seed a real entry through the
    /// production insert path, then prove that each state gets `Skip` (no hit),
    /// no leader ctx (nothing can be inserted), and its own metric outcome,
    /// while the `Empty` state that seeded it still hits.
    #[tokio::test]
    async fn ineligible_buffer_deltas_are_counted_and_neither_hit_nor_fill() {
        let (_tmp, ice, _ctx, _df) =
            dataframe_for("SELECT 1", (0..2).map(smoke_event).collect()).await;
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        // Unique shape: the result cache is process-global, so a shared SQL
        // string would let sibling tests decide this one's outcome.
        let sql = "SELECT count(*) AS n FROM events WHERE host = 'unknown-delta-guard'";
        let decide = |delta: BufferDeltaState| {
            let ice = ice.clone();
            let resolved = resolved.clone();
            async move {
                prepare_result_cache(
                    &ice,
                    sql,
                    false,
                    QueryFormat::Records,
                    &resolved,
                    delta,
                    BufferProof::default(),
                    ResultCacheScope {
                        allow_approximate: AllowApproximate::always(),
                        shard: None,
                    },
                    far_future_deadline(),
                )
                .await
                .unwrap()
            }
        };

        let SqlResultCacheDecision::Leader(ctx) = decide(BufferDeltaState::Empty).await else {
            panic!("a proven-empty delta must lead the first execution");
        };
        let key = ctx.key.clone();
        finish_result_cache(
            ctx,
            CacheEligibility::SQL,
            Some(CachedBody::Records(Arc::new(
                crate::format::RecordsResponse {
                    columns: vec!["n".into()],
                    row_count: 1,
                    rows: serde_json::json!([{ "n": 2 }]),
                    truncated: false,
                    max_rows: None,
                    cost: None,
                    stats: None,
                    approximation: None,
                },
            ))),
        )
        .await;
        assert!(
            sql_result_cache().lock().await.contains(&key),
            "fixture failed to seed the entry the guard must not serve"
        );

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        // The failure path: no hit, and no leader ctx, so `finish_result_cache`
        // is never reached and the entry count cannot grow. The request falls
        // through to ordinary uncached execution (or its error).
        assert!(matches!(
            decide(BufferDeltaState::Unknown).await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcome(&snapshotter, "skip_buffer_unknown"),
            1,
            "a failed buffer load must be visible separately from a cold cache"
        );
        // A `Skip` also leaves no single-flight marker behind, so a later
        // healthy request is not parked on a leader that will never finish.
        assert!(
            !sql_result_inflight()
                .lock()
                .expect("sql result inflight mutex")
                .contains_key(&key),
            "the skipped request registered a single-flight marker"
        );
        assert!(matches!(
            decide(BufferDeltaState::NonEmpty).await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcome(&snapshotter, "skip_buffer_nonempty"),
            1,
            "rows in flight must have their own cache-bypass outcome"
        );
        assert!(matches!(
            decide(BufferDeltaState::OverBudget).await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcome(&snapshotter, "skip_buffer_over_budget"),
            1,
            "a declined decode must not be folded into a failed load"
        );
        // Same query, same snapshot, delta proven empty again: the seeded entry
        // is still there, which is what makes the two skips above meaningful.
        assert!(matches!(
            decide(BufferDeltaState::Empty).await,
            SqlResultCacheDecision::Hit(_)
        ));
    }

    /// THE ACCEPTANCE CRITERION FOR #1481. `skip_time_dependent` used to fire
    /// for three different situations, and only one of them is expected traffic.
    /// A query calling `now()` is a dashboard doing what dashboards do; a query
    /// calling a function the registry cannot classify is #1444's conservatism
    /// costing this deployment its cache (that query may still plan and succeed
    /// — the classifier parses with `sqlparser`, the planner with DataFusion);
    /// an unparseable statement should be ~zero. Each gets its own `outcome`.
    ///
    /// Asserted on the whole outcome map per phase, not just the expected
    /// value: the point of the split is that the OTHER series stay still.
    #[tokio::test]
    async fn each_cache_skip_reason_gets_its_own_outcome_label() {
        let (_tmp, ice, _ctx, _df) =
            dataframe_for("SELECT 1", (0..2).map(smoke_event).collect()).await;
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let decide = |sql: &'static str| {
            let ice = ice.clone();
            let resolved = resolved.clone();
            async move {
                prepare_result_cache(
                    &ice,
                    sql,
                    false,
                    QueryFormat::Records,
                    &resolved,
                    BufferDeltaState::Empty,
                    BufferProof::default(),
                    ResultCacheScope {
                        allow_approximate: AllowApproximate::always(),
                        shard: None,
                    },
                    far_future_deadline(),
                )
                .await
                .unwrap()
            }
        };

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);

        // A registered STABLE function: the case the existing series keeps.
        // Cacheable SHAPE (aggregate + filter), or the shape gate above would
        // skip it uncounted and this test would pass on the wrong reason.
        assert!(matches!(
            decide("SELECT count(*) AS n FROM events WHERE timestamp >= now()").await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcomes(&snapshotter),
            std::collections::HashMap::from([("skip_time_dependent".to_string(), 1)]),
            "a clock function must count as, and only as, a time-dependent skip"
        );

        // A name the registry cannot classify at all.
        assert!(matches!(
            decide("SELECT count(*) AS n FROM events WHERE no_such_udf(host) = 'h'").await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcomes(&snapshotter),
            std::collections::HashMap::from([("skip_unclassifiable".to_string(), 1)]),
            "an unregistered function must be distinguishable from now()"
        );

        // Unparseable: counted above the shape gate, expected at ~zero.
        assert!(matches!(
            decide("SELECT count(*) AS n FROM events WHERE").await,
            SqlResultCacheDecision::Skip
        ));
        assert_eq!(
            result_cache_outcomes(&snapshotter),
            std::collections::HashMap::from([("skip_unparseable".to_string(), 1)]),
            "an unparseable statement must not be folded into either function skip"
        );

        // A pure query of the same shape still reaches the cache, so the three
        // skips above are refusals of these queries and not of every query.
        assert!(matches!(
            decide("SELECT count(*) AS n FROM events WHERE host = 'skip-reason-guard'").await,
            SqlResultCacheDecision::Leader(_)
        ));
        assert_eq!(
            result_cache_outcomes(&snapshotter),
            std::collections::HashMap::from([("miss".to_string(), 1)]),
            "an immutable query of a cacheable shape must be a plain miss"
        );
    }

    /// The mapping the cache gate depends on: no buffer and a successful empty
    /// load are `Empty`, a successful load with rows is `NonEmpty`, and a load
    /// ERROR is `Unknown` rather than the `None` that used to read as empty.
    #[tokio::test]
    async fn resolve_buffer_delta_separates_an_empty_delta_from_a_failed_load() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.append_events(&[Event::now("committed")]).await.unwrap();
        let identity = CallerIdentity::default();
        let query = "SELECT count(*) AS n FROM events WHERE host = 'h'";
        let resolve = |root: Option<std::path::PathBuf>| {
            let ice = &ice;
            let identity = &identity;
            async move {
                let cache = Arc::new(crate::wal_buffer::BufferDeltaCache::default());
                resolve_buffer_delta(ice, root.as_deref(), &cache, identity, query).await
            }
        };

        // No buffer configured: nothing can be in flight.
        let (state, load) = resolve(None).await;
        assert_eq!(state, BufferDeltaState::Empty);
        assert!(load.is_none());

        // Configured but drained: the load ran and proved the delta empty.
        let empty_root = tmp.path().join("wal-empty");
        std::fs::create_dir_all(empty_root.join("sealed")).unwrap();
        let (state, load) = resolve(Some(empty_root)).await;
        assert_eq!(state, BufferDeltaState::Empty);
        assert!(load.is_some_and(|l| l.is_empty()));

        // Rows in flight.
        let rows_root = tmp.path().join("wal-rows");
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            &rows_root,
            "ing",
            10_000,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        writer.append_events(&[Event::now("buffered")]).unwrap();
        writer.seal().unwrap().expect("sealed segment");
        let (state, load) = resolve(Some(rows_root)).await;
        assert_eq!(state, BufferDeltaState::NonEmpty);
        assert_eq!(
            load.as_ref()
                .and_then(BufferDeltaLoad::delta)
                .map(|d| d.rows("events", None)),
            Some(1)
        );

        // A segment that decodes but cannot be aligned to the events schema
        // fails the load itself (a merely unreadable segment is skipped, which
        // is a different, best-effort path).
        let broken_root = tmp.path().join("wal-broken");
        std::fs::create_dir_all(broken_root.join("sealed")).unwrap();
        write_unalignable_segment(&broken_root.join("sealed").join("broken.arrow"));
        let (state, load) = resolve(Some(broken_root)).await;
        assert_eq!(state, BufferDeltaState::Unknown);
        assert!(load.is_none(), "a failed load must carry no delta");
    }

    /// A legacy raw-IPC WAL segment whose single `timestamp` column is a string:
    /// `read_segment` accepts it, and aligning it to the events schema fails —
    /// which is how the tests above make a delta load return an error.
    fn write_unalignable_segment(path: &std::path::Path) {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["not a ts"]))],
        )
        .unwrap();
        // A raw IPC STREAM, matching what `siglake_wal::read_segment` decodes
        // for a legacy (unframed, no CRC sidecar) segment.
        let file = std::fs::File::create(path).unwrap();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }

    // ---------------------------------------------------------------------
    // #1524: does the cached body, the snapshot key, and the empty-WAL proof
    // describe ONE read state? The three are established independently — the
    // WAL proof in `resolve_buffer_delta`, the key in `prepare_result_cache`,
    // the provider in `register_tables_for_query` + the WAL-buffer override —
    // so each pair has an interval a barrier can open.
    // ---------------------------------------------------------------------

    /// 2026-01-01T00:00:00Z / 2027-01-01T00:00:00Z, the half-open window every
    /// #1524 query asks for.
    const WINDOW_LO_SECS: i64 = 1_767_225_600;
    const WINDOW_HI_SECS: i64 = 1_798_761_600;
    /// Well before the window, so a segment holding only these is pruned by the
    /// WS-8 header check — but still sized by the unwindowed budget gate.
    const BACKLOG_SECS: i64 = 1_577_836_800; // 2020-01-01T00:00:00Z

    fn event_at(secs: i64, raw: impl Into<String>) -> Event {
        Event {
            timestamp: Utc.timestamp_opt(secs, 0).single().expect("valid instant"),
            host: "h".into(),
            source: "backlog".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: raw.into(),
            attributes: None,
        }
    }

    /// A committed table plus a WAL buffer holding an out-of-window BACKLOG
    /// segment and a small in-window one, sized so the WHOLE buffer busts the
    /// decode ceiling while the window's share fits under it.
    ///
    /// That is the outage-recovery shape, and it is where the two buffer gates
    /// part company: `load_buffer_delta` prices the whole buffer
    /// (`time_bound: None`) and refuses, while `UnionEventsProvider::scan`
    /// prices only the segments the query's window keeps and unions them.
    struct BacklogFixture {
        _tmp: tempfile::TempDir,
        ice: Arc<IcebergContext>,
        wal_root: std::path::PathBuf,
        wal_dir: std::path::PathBuf,
        writer: siglake_wal::WalWriter,
        /// The injected decode ceiling: between the whole buffer and the
        /// window's share for `new`, and `0` (guard off) for `drained`.
        max_bytes: u64,
        committed_in_window: usize,
        buffered_in_window: usize,
    }

    impl BacklogFixture {
        async fn new() -> Self {
            Self::build(true).await
        }

        /// The same committed table with a DRAINED buffer and the decode
        /// ceiling switched off (`0`), so the WAL proof is a genuine `Empty` and
        /// the scan unions whatever sealed after it was taken. This is the
        /// fixture for the proof→execution interval (#1562); `new` is the one
        /// for the two buffer gates disagreeing (#1524).
        async fn drained() -> Self {
            Self::build(false).await
        }

        async fn build(seed_backlog: bool) -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let ice = Arc::new(
                IcebergContext::open(&tmp.path().join("warehouse"))
                    .await
                    .unwrap(),
            );
            let committed: Vec<Event> = (0..4)
                .map(|i| event_at(WINDOW_LO_SECS + 60 + i, format!("committed {i}")))
                .collect();
            ice.append_events(&committed).await.unwrap();

            let wal_root = tmp.path().join("wal");
            let mut writer = siglake_wal::WalWriter::with_thresholds(
                &wal_root,
                "ing",
                10_000,
                std::time::Duration::from_secs(60),
            )
            .unwrap();
            let mut buffered_in_window = 0;
            let max_bytes = if seed_backlog {
                // The backlog: uncommitted, outside the window, and big enough
                // that the unwindowed gate refuses the buffer.
                let backlog: Vec<Event> = (0..512)
                    .map(|i| event_at(BACKLOG_SECS + i, format!("backlog {i} {}", "x".repeat(64))))
                    .collect();
                writer.append_events(&backlog).unwrap();
                let backlog_segment = writer.seal().unwrap().expect("sealed backlog segment");
                // The window's share.
                let buffered: Vec<Event> = (0..2)
                    .map(|i| event_at(WINDOW_LO_SECS + 600 + i, format!("buffered {i}")))
                    .collect();
                writer.append_events(&buffered).unwrap();
                let in_window = writer.seal().unwrap().expect("sealed in-window segment");
                buffered_in_window = buffered.len();
                // The ceiling sits between the two: the whole buffer is over it,
                // the window's share (with room for the barrier's segment) is not.
                let max_bytes = std::fs::metadata(&backlog_segment.path).unwrap().len();
                assert!(
                    std::fs::metadata(&in_window.path).unwrap().len() * 4 < max_bytes,
                    "fixture: the in-window share must stay well under the ceiling"
                );
                max_bytes
            } else {
                // Nothing sealed, and `0` disables the decode ceiling
                // (`buffer_delta_max_bytes`): the drained-buffer fixture must
                // not have a budget refusal standing in for a proof.
                0
            };

            let wal_dir = crate::wal_buffer::resolve_tenant_wal_dir(&wal_root, "default");
            Self {
                _tmp: tmp,
                ice,
                wal_root,
                wal_dir,
                writer,
                max_bytes,
                committed_in_window: committed.len(),
                buffered_in_window,
            }
        }

        /// A records shape NO Tier-1 fast path serves, so the buffer can only
        /// reach it through `UnionEventsProvider::scan`. `tag` keeps the
        /// process-global result cache from letting sibling tests decide this
        /// one's outcome.
        fn query(&self, tag: &str) -> String {
            format!(
                "SELECT raw FROM events WHERE source <> '{tag}' \
                 AND timestamp >= to_timestamp({WINDOW_LO_SECS}) \
                 AND timestamp < to_timestamp({WINDOW_HI_SECS})"
            )
        }

        async fn resolve_delta(&self, query: &str) -> (BufferDeltaState, Option<BufferDeltaLoad>) {
            let cache = Arc::new(crate::wal_buffer::BufferDeltaCache::default());
            resolve_buffer_delta_with_max(
                &self.ice,
                Some(&self.wal_root),
                &cache,
                &CallerIdentity::default(),
                query,
                Some(self.max_bytes),
            )
            .await
        }

        /// What this request would actually be SERVED: the registered Iceberg
        /// provider with the WS-6 union override on top, at the same injected
        /// ceiling the delta gate used.
        async fn execute_union(&self, query: &str) -> crate::format::RecordsResponse {
            let ctx = SessionContext::new();
            register_all_tables(&self.ice, &ctx).await.unwrap();
            let (base, consumed) = self.ice.events_provider_with_consumed().await.unwrap();
            let union = crate::wal_buffer::UnionEventsProvider::try_new_with_max_bytes(
                base,
                &self.wal_dir,
                &consumed,
                self.max_bytes,
            )
            .unwrap();
            ctx.deregister_table("events").unwrap();
            ctx.register_table("events", Arc::new(union)).unwrap();
            let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();
            crate::format::batches_to_records(&batches, None).unwrap()
        }

        /// Seal one more in-window segment: the barrier between a request's
        /// empty-WAL proof and the scan that reads the directory afresh.
        fn seal_another_in_window_segment(&mut self, rows: usize) {
            let more: Vec<Event> = (0..rows)
                .map(|i| event_at(WINDOW_LO_SECS + 900 + i as i64, format!("late {i}")))
                .collect();
            self.writer.append_events(&more).unwrap();
            self.writer.seal().unwrap().expect("sealed late segment");
        }

        fn resolved_limits(&self) -> ResolvedLimits {
            ResolvedLimits::resolve(
                &RequestLimits::default(),
                Priority::Interactive,
                &crate::limits::TierLimits::interactive_defaults(),
            )
        }

        /// The proof is a separate argument because most of these tests only
        /// need the CLASSIFICATION; the interval test below is the one that
        /// hands over the listing its `Empty` was taken from.
        async fn decide_cache(
            &self,
            query: &str,
            state: BufferDeltaState,
            proof: BufferProof,
        ) -> SqlResultCacheDecision {
            prepare_result_cache(
                &self.ice,
                query,
                false,
                QueryFormat::Records,
                &self.resolved_limits(),
                state,
                proof,
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
                far_future_deadline(),
            )
            .await
            .unwrap()
        }
    }

    /// A buffer past the decode budget is not a drained one. Both hand back a
    /// `BufferDeltaLoad` with no batches, and `is_empty()` cannot tell them
    /// apart — which is exactly how a REFUSAL to read the WAL came to be read
    /// as a PROOF that the WAL held nothing.
    #[tokio::test]
    async fn an_over_budget_buffer_is_not_a_proven_empty_delta() {
        let fx = BacklogFixture::new().await;
        let query = fx.query("over-budget-classification");
        let (state, load) = fx.resolve_delta(&query).await;

        // Indistinguishable from a drained buffer by the delta alone.
        let load = load.expect("an over-budget load still reports its observation");
        assert!(
            load.is_empty(),
            "fixture assumption: a refused buffer carries no batches"
        );
        assert!(!load.counts["events"].buffer_within_budget);
        assert!(load.counts["events"].buffer_bytes > fx.max_bytes);

        // But the gate the SCAN applies prices only the window, and that fits:
        // the refusal above says nothing about what the union will carry.
        let consumed = std::collections::HashSet::new();
        let window = (
            WINDOW_LO_SECS * 1_000_000_000,
            WINDOW_HI_SECS * 1_000_000_000,
        );
        assert!(
            !crate::wal_buffer::buffer_within_budget_with_max(
                &fx.wal_dir,
                &consumed,
                None,
                fx.max_bytes
            )
            .0,
            "fixture assumption: the whole buffer busts the ceiling"
        );
        assert!(
            crate::wal_buffer::buffer_within_budget_with_max(
                &fx.wal_dir,
                &consumed,
                Some(window),
                fx.max_bytes
            )
            .0,
            "fixture assumption: the window's share fits, so the scan unions it"
        );

        assert_eq!(state, BufferDeltaState::OverBudget);
        assert!(
            !matches!(state, BufferDeltaState::Empty),
            "a refused buffer is not the proven-empty delta a snapshot-keyed \
             entry requires"
        );
        assert!(
            state.fast_paths_safe(),
            "the backlog guard must keep serving committed-only ms answers; \
             degrading to the union plan under backlog is what it exists to prevent"
        );
    }

    /// The contamination itself, inspected at the SUBSEQUENT hit rather than
    /// the first response.
    ///
    /// Under a backlogged buffer the request's three stages disagree: the WAL
    /// proof refuses the buffer and reports nothing in flight, the key is
    /// `(table, snapshot, query)`, and the scan re-prices the SAME buffer
    /// against the query's window and unions its rows. Freezing that body under
    /// that key is durable — the snapshot cannot move until the drain commits,
    /// and until then every identical request keeps "proving" the WAL empty,
    /// so the frozen answer keeps being served while the buffer grows.
    #[tokio::test]
    async fn a_backlogged_buffer_does_not_freeze_windowed_rows_into_the_result_cache() {
        let mut fx = BacklogFixture::new().await;
        let query = fx.query("backlog-cache-contamination");

        // The shape IS cacheable: with a genuinely drained buffer this query
        // leads its own execution. Without this, the assertions below could
        // pass because the shape gate rejected the query for some other reason.
        let SqlResultCacheDecision::Leader(shape_probe) = fx
            .decide_cache(&query, BufferDeltaState::Empty, BufferProof::default())
            .await
        else {
            panic!("fixture assumption: a windowed records query is cacheable by shape");
        };
        drop(shape_probe); // release the single-flight marker without inserting

        // REQUEST 1, in the handler's order: prove the delta, key the cache,
        // then register the provider and execute.
        let (state, _) = fx.resolve_delta(&query).await;
        let decision = fx.decide_cache(&query, state, BufferProof::default()).await;
        let body = fx.execute_union(&query).await;
        assert_eq!(
            body.row_count,
            fx.committed_in_window + fx.buffered_in_window,
            "fixture assumption: the window fits the ceiling, so the served \
             body carries buffered rows the delta gate refused to look at"
        );
        if let SqlResultCacheDecision::Leader(ctx) = decision {
            finish_result_cache(
                ctx,
                CacheEligibility::SQL,
                Some(CachedBody::Records(Arc::new(body.clone()))),
            )
            .await;
        }

        // BARRIER: another in-window segment seals. No commit, so the snapshot
        // — the whole key — is unchanged.
        fx.seal_another_in_window_segment(3);

        // REQUEST 2. A fresh read now returns more rows; the delta still
        // refuses the (still over-budget) buffer.
        let (state, _) = fx.resolve_delta(&query).await;
        assert_eq!(state, BufferDeltaState::OverBudget);
        let fresh = fx.execute_union(&query).await;
        assert_eq!(
            fresh.row_count,
            body.row_count + 3,
            "fixture assumption: the barrier's rows are visible to a fresh read"
        );
        if let SqlResultCacheDecision::Hit(hit) =
            fx.decide_cache(&query, state, BufferProof::default()).await
        {
            panic!(
                "the result cache served a frozen {}-row body for a read state that \
                 now has {} rows: its empty-WAL proof was a REFUSED buffer, not a \
                 drained one, and nothing will invalidate the entry until the drain \
                 commit moves the snapshot",
                hit.expect_records().row_count,
                fresh.row_count
            );
        }
    }

    /// The THIRD interval, and the one #1562 closes: the empty-WAL proof is
    /// taken at the top of the request, and `UnionEventsProvider::scan` lists
    /// the WAL directory AFRESH at execution time (by design — that is how a
    /// `timestamp` predicate prunes segments by header). A segment that seals in
    /// between is unioned into the body, and the insert used to freeze that body
    /// under a key whose eligibility says the WAL contributed nothing.
    ///
    /// Unlike the backlog case above, the buffer here is genuinely drained at
    /// proof time — `Empty`, not `OverBudget` — so nothing but the re-proof in
    /// `finish_result_cache` stands between the sealed segment and a
    /// self-contradicting entry.
    #[tokio::test]
    async fn a_segment_sealing_before_the_execution_blocks_the_result_cache_insert() {
        let mut fx = BacklogFixture::drained().await;

        // CONTROL. With the WAL still drained when the insert runs, the entry
        // lands: without this the assertion below could pass because inserts
        // never happen in this fixture at all.
        let control = fx.query("proof-execution-control");
        let (state, load) = fx.resolve_delta(&control).await;
        assert_eq!(
            state,
            BufferDeltaState::Empty,
            "fixture assumption: a drained buffer proves Empty"
        );
        let SqlResultCacheDecision::Leader(ctx) = fx
            .decide_cache(&control, state, buffer_proof_of(load.as_ref()))
            .await
        else {
            panic!("fixture assumption: a windowed records query leads its own execution");
        };
        let control_key = ctx.key.clone();
        let body = fx.execute_union(&control).await;
        assert_eq!(body.row_count, fx.committed_in_window);
        finish_result_cache(
            ctx,
            CacheEligibility::SQL,
            Some(CachedBody::Records(Arc::new(body.clone()))),
        )
        .await;
        assert!(
            sql_result_cache().lock().await.contains(&control_key),
            "an undisturbed request must still cache its body"
        );

        // THE INTERVAL. Prove the WAL empty, key the cache, and only then seal.
        let query = fx.query("proof-execution-interval");
        let (state, load) = fx.resolve_delta(&query).await;
        assert_eq!(state, BufferDeltaState::Empty);
        let SqlResultCacheDecision::Leader(ctx) = fx
            .decide_cache(&query, state, buffer_proof_of(load.as_ref()))
            .await
        else {
            panic!("the interval request must lead its own execution");
        };
        let key = ctx.key.clone();

        // BARRIER: a segment seals after the proof. No commit, so the snapshot —
        // the whole key — is unchanged.
        fx.seal_another_in_window_segment(3);

        let body = fx.execute_union(&query).await;
        assert_eq!(
            body.row_count,
            fx.committed_in_window + 3,
            "fixture assumption: the scan re-lists the WAL, so the sealed \
             segment is in the body this request is about to cache"
        );
        finish_result_cache(
            ctx,
            CacheEligibility::SQL,
            Some(CachedBody::Records(Arc::new(body.clone()))),
        )
        .await;

        assert!(
            !sql_result_cache().lock().await.contains(&key),
            "a body carrying rows from a segment that sealed AFTER the \
             empty-WAL proof was inserted under a key asserting an empty WAL; \
             nothing invalidates it until the drain commit moves the snapshot"
        );
    }

    /// `BufferProof` on its own, away from the query path: the vacuous proof
    /// holds, a listing that still agrees holds, and a listing that moved — or
    /// one that cannot be taken at all — does not.
    #[test]
    fn a_buffer_proof_holds_only_while_its_segment_set_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("wal-dir");
        let sealed = dir.join("sealed");
        std::fs::create_dir_all(&sealed).unwrap();
        // Never decoded — `uncommitted_segment_names` only lists basenames.
        std::fs::write(sealed.join("a.arrow"), b"segment").unwrap();

        assert!(
            BufferProof::default().still_holds(),
            "no witnessed table means nothing can seal into one"
        );

        let owned = |owner: Option<&str>, exclude: &[&str], names: &[&str]| BufferProof {
            tables: vec![WitnessedBuffer {
                dir: dir.clone(),
                exclude: Arc::new(exclude.iter().map(|n| n.to_string()).collect()),
                names: names.iter().map(|n| n.to_string()).collect(),
                owner: owner.map(String::from),
            }],
        };
        let witness = |exclude: &[&str], names: &[&str]| owned(None, exclude, names);
        assert!(witness(&[], &["a.arrow"]).still_holds());

        std::fs::write(sealed.join("b.arrow"), b"a late seal").unwrap();
        assert!(
            !witness(&[], &["a.arrow"]).still_holds(),
            "a segment sealed after the proof was taken invalidates it"
        );
        assert!(witness(&[], &["a.arrow", "b.arrow"]).still_holds());
        assert!(
            witness(&["b.arrow"], &["a.arrow"]).still_holds(),
            "the re-listing applies the SAME consumed-set exclusion the proof \
             was taken under; a set that has since grown is a different proof"
        );

        // #2661: under an owner marker naming a DIFFERENT table the directory
        // contributes nothing, so the proof that describes it is the empty
        // listing — and that proof must stop holding the moment a drain
        // quarantines and re-stamps the directory for the live table.
        siglake_wal::stamp_wal_owner(&dir, "dropped-uuid").unwrap();
        assert!(owned(Some("live-uuid"), &[], &[]).still_holds());
        assert!(!owned(Some("live-uuid"), &[], &["a.arrow", "b.arrow"]).still_holds());
        assert!(
            owned(Some("dropped-uuid"), &[], &["a.arrow", "b.arrow"]).still_holds(),
            "the marker names the witnessed table: the gate is open and the \
             listing is the ordinary one"
        );
        siglake_wal::stamp_wal_owner(&dir, "live-uuid").unwrap();
        assert!(
            !owned(Some("live-uuid"), &[], &[]).still_holds(),
            "a re-stamped directory serves its segments again"
        );

        std::fs::remove_dir_all(&dir).unwrap();
        assert!(
            !witness(&[], &["a.arrow", "b.arrow"]).still_holds(),
            "a listing that disagrees — including one that could not be taken \
             — is not the proof the key rests on"
        );
    }

    /// The other end of the same request: the whole-table count path folds in
    /// NOTHING under the same backlogged buffer (committed-only — the designed
    /// bounded-staleness degradation), so its body IS a pure function of
    /// `(table, snapshot, query)`. The divergence #1524 hunts is on the scan
    /// side, not here. And the shape gate keeps a bare count out of the cache
    /// regardless of the buffer state.
    #[tokio::test]
    async fn the_whole_table_count_path_stays_snapshot_pure_under_the_same_backlog() {
        let fx = BacklogFixture::new().await;
        let query = "SELECT count(*) AS n FROM events";
        let cache = Arc::new(crate::wal_buffer::BufferDeltaCache::default());
        let load = load_buffer_delta_with_max(
            &fx.ice,
            &fx.wal_root,
            &cache,
            &CallerIdentity::default(),
            query,
            Some(fx.max_bytes),
        )
        .await
        .unwrap();

        let mut cost = count_cost_for_test(&fx.ice, query).await;
        let observation = resolve_whole_table_count(
            &mut cost,
            "events",
            load.delta(),
            Some(&load),
            "test_backlog",
        );
        assert_eq!(observation.buffer_within_budget, Some(false));
        assert_eq!(observation.included_buffer_rows, 0);
        assert_eq!(
            observation.count, fx.committed_in_window as u64,
            "the count path serves the committed base alone while the buffer is \
             past the decode budget"
        );

        // Not a cacheable shape in the first place: no WHERE, no GROUP BY, no
        // ordered LIMIT.
        assert!(matches!(
            fx.decide_cache(query, BufferDeltaState::Empty, BufferProof::default())
                .await,
            SqlResultCacheDecision::Skip
        ));
    }

    /// The second interval: the snapshot key is read in `prepare_result_cache`,
    /// the provider several stages later in `register_tables_for_query`. A
    /// commit in between produces an entry whose body is NEWER than its key.
    ///
    /// Establishes that this one is not exploitable today: the key advances
    /// with the same table-cache generation the provider came from, so the
    /// mis-keyed entry is unreachable — no later request can ask for the
    /// superseded snapshot. It is only harmless while the table cache moves
    /// forward.
    #[tokio::test]
    async fn a_commit_between_the_snapshot_key_and_the_provider_leaves_no_reachable_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let first: Vec<Event> = (0..4)
            .map(|i| event_at(WINDOW_LO_SECS + 60 + i, format!("first {i}")))
            .collect();
        ice.append_events(&first).await.unwrap();
        let query = format!(
            "SELECT raw FROM events WHERE source <> 'generation-barrier' \
             AND timestamp >= to_timestamp({WINDOW_LO_SECS}) \
             AND timestamp < to_timestamp({WINDOW_HI_SECS})"
        );
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let decide = |ice: Arc<IcebergContext>, query: String, resolved: ResolvedLimits| async move {
            prepare_result_cache(
                &ice,
                &query,
                false,
                QueryFormat::Records,
                &resolved,
                // No buffer is configured in this fixture, so the WAL proof is
                // `Empty` by construction and the only moving part is the
                // table-cache generation.
                BufferDeltaState::Empty,
                BufferProof::default(),
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
                far_future_deadline(),
            )
            .await
            .unwrap()
        };

        // The key is taken here, against snapshot A.
        let SqlResultCacheDecision::Leader(ctx) =
            decide(ice.clone(), query.clone(), resolved.clone()).await
        else {
            panic!("first request must lead its own execution");
        };
        let key_at_a = ctx.key.clone();

        // BARRIER: a commit lands before this request registers its provider.
        let second: Vec<Event> = (0..3)
            .map(|i| event_at(WINDOW_LO_SECS + 120 + i, format!("second {i}")))
            .collect();
        ice.append_events(&second).await.unwrap();

        let dfctx = SessionContext::new();
        register_all_tables(&ice, &dfctx).await.unwrap();
        let batches = dfctx.sql(&query).await.unwrap().collect().await.unwrap();
        let body = crate::format::batches_to_records(&batches, None).unwrap();
        assert_eq!(
            body.row_count, 7,
            "the interval is real: registration reads a newer generation than the key"
        );
        finish_result_cache(
            ctx,
            CacheEligibility::SQL,
            Some(CachedBody::Records(Arc::new(body.clone()))),
        )
        .await;
        assert!(
            sql_result_cache().lock().await.contains(&key_at_a),
            "the mis-keyed entry is what makes the reachability check below \
             meaningful"
        );

        // The next request keys off the CURRENT snapshot, so it cannot ask for
        // the superseded one: the seven-row body is never served as snapshot A.
        match decide(ice.clone(), query.clone(), resolved.clone()).await {
            SqlResultCacheDecision::Leader(next) => assert_ne!(
                next.key, key_at_a,
                "a post-commit request re-derived the pre-commit key"
            ),
            SqlResultCacheDecision::Hit(_) => {
                panic!("a body built from a newer snapshot was served under the older key")
            }
            SqlResultCacheDecision::Skip => panic!("unexpected skip"),
        }
    }

    /// Two requests that will be SERVED differently must not share a cache
    /// entry: the key separates exactness and shard scope, and separates
    /// nothing else (a shard selector that resolves to the whole file set is
    /// the whole-table mode, not a fourth one).
    #[test]
    fn the_result_cache_key_separates_serving_modes() {
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let sql = "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 5";
        let key = |allow_approximate: AllowApproximate, shard: Option<(usize, usize)>| {
            result_cache_key(
                "default",
                42,
                3,
                sql,
                &resolved,
                ResultCacheScope {
                    allow_approximate,
                    shard: shard.and_then(|(i, n)| siglake_storage::ScanShard::new(i, n)),
                },
            )
        };
        let approx = AllowApproximate::always();
        let exact = AllowApproximate::for_request(&SqlRequest {
            query: sql.into(),
            format: None,
            priority: Priority::Interactive,
            default_order: true,
            dry_run: false,
            limits: RequestLimits::default(),
            shard: None,
            exact: true,
        });
        assert!(!exact.get(), "`exact: true` must forbid approximation");
        let modes = [
            ("approximate whole table", key(approx, None)),
            ("exact whole table", key(exact, None)),
            ("approximate shard 0/2", key(approx, Some((0, 2)))),
            ("approximate shard 1/2", key(approx, Some((1, 2)))),
            ("exact shard 0/2", key(exact, Some((0, 2)))),
            ("approximate shard 0/3", key(approx, Some((0, 3)))),
        ];
        for (i, (name, k)) in modes.iter().enumerate() {
            for (other_name, other) in modes.iter().skip(i + 1) {
                assert_ne!(k, other, "{name} shares a cache entry with {other_name}");
            }
        }
        // Same mode ⇒ same entry, so an immutable repeat still hits.
        assert_eq!(key(approx, Some((1, 2))), key(approx, Some((1, 2))));
        // `ScanShard::new` maps a no-op selector to the whole file set, and the
        // key follows it: `{index: 0, count: 1}` scans everything, so it is the
        // whole-table mode and shares its entry.
        assert_eq!(key(approx, Some((0, 1))), key(approx, None));
    }

    /// The schema id is part of the table identity, and an unchanged one keeps
    /// its entry (#2494). The behavioural half — a real additive migration on a
    /// standing snapshot — is
    /// `tests/result_cache_schema_generation.rs`.
    #[test]
    fn the_result_cache_key_separates_schema_generations() {
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let sql = "SELECT * FROM events WHERE host = 'h'";
        let key = |snapshot: i64, schema: i32| {
            result_cache_key(
                "default",
                snapshot,
                schema,
                sql,
                &resolved,
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
            )
        };
        assert_ne!(
            key(42, 3),
            key(42, 4),
            "an additive migration moves the schema id and NOT the snapshot; \
             sharing the entry replays the pre-migration column set"
        );
        assert_eq!(key(42, 3), key(42, 3), "an unchanged generation still hits");
        assert_ne!(key(42, 3), key(43, 3), "a commit still separates entries");
    }

    async fn dataframe_for(
        query: &str,
        events: Vec<Event>,
    ) -> (
        tempfile::TempDir,
        Arc<IcebergContext>,
        SessionContext,
        DataFrame,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.append_events(&events).await.unwrap();
        let ctx = SessionContext::new();
        let ice = Arc::new(ice);
        register_all_tables(&ice, &ctx).await.unwrap();
        let df = ctx.sql(query).await.unwrap();
        (tmp, ice, ctx, df)
    }

    #[tokio::test]
    async fn detect_count_fast_path_matches_plain_count_star() {
        let df = plan_for("SELECT count(*) AS n FROM events").await;
        let fp = detect_count_fast_path(df.logical_plan()).expect("count fast path");
        assert_eq!(fp.output_column, "n");
    }

    #[tokio::test]
    async fn detect_count_fast_path_rejects_non_time_filter() {
        let df = plan_for("SELECT count(*) AS n FROM events WHERE host = 'host-1'").await;
        assert!(detect_count_fast_path(df.logical_plan()).is_none());
    }

    /// A time filter must NOT take this path, tautology or not.
    ///
    /// This test asserted the opposite until 2026-08-29, and the behaviour it
    /// pinned was a silent 4.46x over-count in the field: the path answers with
    /// the summed `record_count()` of every file the window TOUCHES, which is
    /// the row count only for a whole-table count. The windowed path answers
    /// this shape correctly, and every call site falls through to it.
    /// A clock-dependent or volatile query must not be cached under a snapshot
    /// key, however it is spelled.
    ///
    /// THE DEFECT THIS GUARDS. The SQL result cache keys on
    /// `namespace|snapshot|rows|bytes|<normalized SQL>` and is never
    /// TTL-expired, by standing invariant. `now()` resolves server-side per
    /// request but survives into that key as the literal text `now()`, so on a
    /// table whose snapshot stops advancing -- an idle tenant, a paused source,
    /// an index fed in periodic batches -- a dashboard's
    /// `WHERE timestamp >= now() - interval '15 minutes'` is served the value
    /// computed at first miss, forever, and nothing distinguishes that from a
    /// legitimate hit.
    ///
    /// The whitespace, comment and volatile-function cases are the holes the
    /// superseded substring scan left: `now ()` plans identically to `now()` and
    /// matched no needle, and `random()` was never on any list.
    #[test]
    fn clock_dependent_and_volatile_queries_are_not_cacheable() {
        // The production path parses once for all three cache classifications;
        // an unparseable statement never reaches this one (it is skipped at the
        // parse), so the test's own wrapper treats it as impure to keep the
        // classifier's "not classifiable is not cacheable" reading whole.
        let query_is_snapshot_impure = |sql: &str| match parse_for_cache_classification(sql) {
            Some(stmts) => statements_snapshot_impurity(&stmts).is_some(),
            None => true,
        };
        for sql in [
            "SELECT count(*) FROM events WHERE timestamp >= now() - interval '15 minutes'",
            "SELECT count(*) FROM events WHERE timestamp >= NOW() - interval '1 hour'",
            "SELECT count(*) FROM events WHERE timestamp > current_timestamp - interval '5 minutes'",
            "SELECT count(*) FROM events WHERE ts::date = current_date",
            "SELECT count(*) FROM events WHERE timestamp >= today()",
            // Spellings the substring scan missed.
            "SELECT count(*) FROM events WHERE timestamp >= now () - interval '15 minutes'",
            "SELECT count(*) FROM events WHERE timestamp >= now /* gap */ () - interval '15 minutes'",
            "SELECT count(*) FROM events WHERE timestamp >= now\n() - interval '15 minutes'",
            "SELECT count(*) FROM events WHERE timestamp >= current_date /* gap */ ()",
            // Volatile functions: re-drawn per row, per execution.
            "SELECT count(*) FROM events WHERE random() < 0.1",
            "SELECT uuid() AS id, host FROM events WHERE host = 'h1'",
            // Nested: subquery, function argument, CTE, and behind EXPLAIN —
            // all four reach the cache probe, `referenced_tables` resolves the
            // table through them, so all four must be walked.
            "SELECT count(*) FROM events WHERE host IN (SELECT host FROM events WHERE ts > now())",
            "SELECT count(*) FROM events WHERE date_trunc('hour', now()) = ts",
            "WITH recent AS (SELECT * FROM events WHERE ts > now()) SELECT count(*) FROM recent WHERE host = 'h1'",
            "EXPLAIN SELECT count(*) FROM events WHERE ts > now()",
            // A function the planner does not know is not classifiable, so it
            // is not cacheable either. It does not plan, so this costs nothing.
            "SELECT snow(x) FROM events WHERE known_at > 3",
        ] {
            assert!(
                query_is_snapshot_impure(sql),
                "a non-snapshot-pure query would have been cached: {sql}"
            );
        }
        // ...and ordinary immutable queries still are cacheable. `known_at` and
        // the `now`-prefixed alias exist here because the superseded check was
        // textual: a careless substring match on `now(` rejects both and
        // silently costs every deployment its cache.
        for sql in [
            "SELECT count(*) FROM events WHERE timestamp >= TIMESTAMP '2024-01-01T00:00:00Z'",
            "SELECT host, count(*) FROM events GROUP BY host",
            "SELECT count(*) FROM events WHERE known_at > 3",
            "SELECT date_trunc('hour', timestamp) AS h, count(*) FROM events GROUP BY h",
            "SELECT nownership FROM events WHERE nownership IS NOT NULL",
            "SELECT lower(host) AS h FROM events WHERE host LIKE 'h%' ORDER BY h LIMIT 10",
            "SELECT row_number() OVER (ORDER BY timestamp) AS n FROM events WHERE host = 'h1'",
        ] {
            assert!(
                !query_is_snapshot_impure(sql),
                "a snapshot-pure query was treated as impure: {sql}"
            );
        }
    }

    /// The reason, not just the verdict: a registry name declared STABLE or
    /// VOLATILE is `NonImmutableFunction`, a name the registry does not hold at
    /// all is `UnclassifiableFunction`. Both skip the cache; they are separate
    /// operational facts and the caller counts them separately (#1481).
    #[test]
    fn the_impurity_reason_separates_a_non_immutable_function_from_an_unknown_one() {
        let reason = |sql: &str| {
            let stmts =
                parse_for_cache_classification(sql).unwrap_or_else(|| panic!("parses: {sql}"));
            statements_snapshot_impurity(&stmts)
        };
        for (sql, name) in [
            ("SELECT count(*) FROM events WHERE ts > now()", "now"),
            ("SELECT count(*) FROM events WHERE ts > NOW()", "now"),
            // Spelled through a qualifier, and through an alias.
            (
                "SELECT count(*) FROM events WHERE ts > datafusion.now()",
                "now",
            ),
            ("SELECT count(*) FROM events WHERE ts >= today()", "today"),
            ("SELECT count(*) FROM events WHERE random() < 0.1", "random"),
        ] {
            assert_eq!(
                reason(sql),
                Some(CacheImpurity::NonImmutableFunction(name.to_string())),
                "{sql}"
            );
        }
        for (sql, name) in [
            ("SELECT snow(x) FROM events WHERE known_at > 3", "snow"),
            (
                "SELECT count(*) FROM events WHERE no_such_udf(host) = 'h'",
                "no_such_udf",
            ),
        ] {
            assert_eq!(
                reason(sql),
                Some(CacheImpurity::UnclassifiableFunction(Some(
                    name.to_string()
                ))),
                "{sql}"
            );
        }
        assert_eq!(
            reason("SELECT count(*) FROM events WHERE host = 'h1'"),
            None,
            "an immutable query must carry no impurity reason"
        );
    }

    /// The shape gate is a property of the parsed statement, not of the
    /// caller's whitespace. Pinned here as well as through
    /// `prepare_result_cache` because the decision test can only observe the
    /// gate through a live catalog, and the interesting cases are cheap.
    #[test]
    fn the_cache_shape_gate_reads_the_ast_not_the_whitespace() {
        let shape = |sql: &str| {
            let stmts =
                parse_for_cache_classification(sql).unwrap_or_else(|| panic!("parses: {sql}"));
            statements_have_cacheable_shape(&stmts)
        };
        for sql in [
            "SELECT count(*) FROM events WHERE host = 'h1'",
            "SELECT count(*)\nFROM events\nWHERE host='h'",
            "SELECT host, count(*)\nFROM events\nGROUP BY host",
            "SELECT host, count(*) FROM events GROUP BY ALL",
            "SELECT timestamp, raw\nFROM events\nORDER BY timestamp DESC\nLIMIT 100",
            "SELECT count(*) FROM (SELECT * FROM events WHERE host='h') t",
            "WITH f AS (SELECT * FROM events WHERE host='h') SELECT count(*) FROM f",
            "EXPLAIN SELECT count(*)\nFROM events\nWHERE host='h'",
            "SELECT host FROM events WHERE host='h' UNION ALL SELECT host FROM events",
        ] {
            assert!(shape(sql), "a cacheable shape was rejected: {sql}");
        }
        for sql in [
            "SELECT timestamp, raw FROM events",
            "SELECT timestamp, raw\nFROM events\nLIMIT 10",
            // ORDER BY without a LIMIT is not a browse, and neither is a LIMIT
            // whose ORDER BY belongs to a window function or to a subquery.
            "SELECT timestamp FROM events ORDER BY timestamp",
            "SELECT row_number() OVER (ORDER BY timestamp) AS n FROM events",
            "SELECT * FROM (SELECT raw FROM events ORDER BY timestamp) t LIMIT 10",
        ] {
            assert!(!shape(sql), "a bare scan was treated as cacheable: {sql}");
        }
    }

    /// The registry snapshot the classifier reads must contain what a log query
    /// actually calls, or every deployment silently loses its result cache.
    ///
    /// Not a restatement of the test above: that one asserts on whole queries,
    /// this one pins the SOURCE of the answer. The classifier treats an unknown
    /// name as impure, so an empty or half-built registry set would make it
    /// return `true` for everything and both the "still cacheable" assertions
    /// and the cache itself would fail silently rather than loudly.
    #[test]
    fn the_volatility_registry_classifies_datafusion_and_siglake_functions() {
        let pure = &snapshot_function_registry().pure;
        // Every function name this repository's own SQL uses (bench queries,
        // docs, tests), plus the shapes the caches exist for.
        for name in [
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "median",
            "length",
            "lower",
            "to_timestamp",
            "date_trunc",
            "row_number",
            // siglake's own UDFs, all declared Immutable.
            "kv_extract",
            "attr_get",
            "match_terms",
            "match_phrase",
        ] {
            assert!(pure.contains(name), "{name} should be classified immutable");
        }
        for name in ["now", "current_date", "current_timestamp", "random", "uuid"] {
            assert!(
                !pure.contains(name),
                "{name} is not immutable and must not be in the pure set"
            );
            // …but the registry does KNOW them, which is what makes the two
            // skip reasons separable: an impure call the planner resolves is
            // not the same fact as a name it cannot classify (#1481).
            assert!(
                snapshot_function_registry().known.contains(name),
                "{name} must still be a known function name"
            );
        }
        for name in ["snow", "no_such_udf"] {
            assert!(
                !snapshot_function_registry().known.contains(name),
                "{name} is not a real function and must not be classifiable"
            );
        }
    }

    /// `exact: true` must switch approximation off, on every path.
    ///
    /// THE DEFECT THIS GUARDS. Three call sites took a `bool` and the
    /// coordinator's Tier-1 battery passed a literal `true`, so on the default
    /// topology -- where the coordinator serves every user query -- neither
    /// `SqlRequest::exact` nor the operator's `SIGLAKE_APPROXIMATE_GROUP_COUNTS`
    /// kill switch had any effect. The batch tier was worse: `submit_batch` took
    /// the request as `_req` and dropped it, so a batch job could not express
    /// exactness at all -- on the tier that exists for the queries where the
    /// number is most likely to be the deliverable.
    ///
    /// The type is the real guard: `AllowApproximate` has no `From<bool>` and no
    /// public constructor, so a future call site cannot hardcode the permissive
    /// answer. This pins the rule the constructor encodes.
    #[test]
    fn exact_requests_never_allow_an_approximate_answer() {
        let req = |exact: bool| SqlRequest {
            query: "SELECT host, count(*) FROM events GROUP BY host".into(),
            format: None,
            priority: Priority::Interactive,
            default_order: true,
            dry_run: false,
            limits: RequestLimits::default(),
            shard: None,
            exact,
        };
        let exact = req(true);
        assert!(
            !AllowApproximate::for_request(&exact).get(),
            "a request that asked for exactness was told approximation is allowed"
        );
        // The permissive direction depends on the operator switch, so assert
        // only what is unconditional: it is never MORE permissive than the
        // switch itself.
        let loose = req(false);
        assert_eq!(
            AllowApproximate::for_request(&loose).get(),
            approximate_group_counts_enabled(),
            "a request that did not ask for exactness disagreed with the operator setting"
        );
    }

    #[tokio::test]
    async fn detect_count_fast_path_rejects_exact_time_filter_with_tautology() {
        let df = plan_for(
            "SELECT count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
               AND 7 = 7",
        )
        .await;
        assert!(
            detect_count_fast_path(df.logical_plan()).is_none(),
            "a windowed count took the whole-file path"
        );
    }

    /// The whole-table count must still take it — the fix redirects windowed
    /// counts, it does not switch the fast path off.
    #[tokio::test]
    async fn detect_count_fast_path_still_accepts_the_whole_table_count() {
        let df = plan_for("SELECT count(*) AS n FROM events").await;
        let fp = detect_count_fast_path(df.logical_plan()).expect("whole-table count fast path");
        assert_eq!(fp.output_column, "n");
    }

    #[tokio::test]
    async fn detect_count_fast_path_rejects_mixed_time_and_dimensional_filter() {
        let df = plan_for(
            "SELECT count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
               AND host = 'host-1'",
        )
        .await;
        assert!(detect_count_fast_path(df.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn detect_negation_count_matches_neq_and_ne_and_not_in() {
        for (sql, expected) in [
            (
                "SELECT count(*) AS n FROM events WHERE host != 'host-1'",
                vec!["host-1"],
            ),
            (
                "SELECT count(*) AS n FROM events WHERE host <> 'host-1'",
                vec!["host-1"],
            ),
            (
                "SELECT count(*) AS n FROM events WHERE host NOT IN ('host-1', 'host-2')",
                vec!["host-1", "host-2"],
            ),
        ] {
            let df = plan_for(sql).await;
            let fp = detect_negation_count_fast_path(df.logical_plan())
                .unwrap_or_else(|| panic!("negation fast path for: {sql}"));
            assert_eq!(fp.table_name, "events");
            assert_eq!(fp.column, "host");
            assert_eq!(fp.output_column, "n");
            let expected: Vec<String> = expected.into_iter().map(String::from).collect();
            assert_eq!(
                fp.predicate,
                DimCountPredicate::Set {
                    values: expected,
                    negated: true
                },
                "sql: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn detect_negation_count_rejects_plain_and_compound_and_equality() {
        // plain count (no filter) is the count fast path, not negation
        assert!(detect_negation_count_fast_path(
            plan_for("SELECT count(*) AS n FROM events")
                .await
                .logical_plan()
        )
        .is_none());
        // equality is the dimensional-count path's POSITIVE polarity now
        // (07-16 finding: low-cardinality equality counts full-scanned with
        // the answer sitting in the same footers).
        assert!(matches!(
            detect_negation_count_fast_path(
                plan_for("SELECT count(*) AS n FROM events WHERE host = 'host-1'")
                    .await
                    .logical_plan()
            ),
            Some(fp) if matches!(fp.predicate, DimCountPredicate::Set { negated: false, .. })
        ));
        // compound predicate (negation AND time) must fall through to the planner
        assert!(detect_negation_count_fast_path(
            plan_for(
                "SELECT count(*) AS n FROM events \
                 WHERE host != 'host-1' AND timestamp >= to_timestamp(1767225600)"
            )
            .await
            .logical_plan()
        )
        .is_none());
    }

    #[tokio::test]
    async fn detect_windowed_count_matches_pure_time_range() {
        let df = plan_for(
            "SELECT count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) AND timestamp < to_timestamp(1767312000)",
        )
        .await;
        let fp = detect_windowed_count_fast_path(df.logical_plan()).expect("windowed count");
        assert_eq!(fp.table_name, "events");
        assert_eq!(fp.output_column, "n");
        assert!(fp.time_bounds.start.is_some() && fp.time_bounds.end.is_some());
        // No time filter → not a windowed count.
        assert!(detect_windowed_count_fast_path(
            plan_for("SELECT count(*) AS n FROM events")
                .await
                .logical_plan()
        )
        .is_none());
        // Dimensional filter mixed in → not a PURE time range.
        assert!(detect_windowed_count_fast_path(
            plan_for(
                "SELECT count(*) AS n FROM events \
                 WHERE timestamp >= to_timestamp(1767225600) AND host = 'h1'"
            )
            .await
            .logical_plan()
        )
        .is_none());
    }

    #[tokio::test]
    async fn detect_dimensional_count_matches_equality_and_in() {
        let df = plan_for("SELECT count(*) AS n FROM events WHERE host = 'h-1'").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("equality count");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["h-1".into()],
                negated: false
            }
        );

        let df = plan_for("SELECT count(*) AS n FROM events WHERE host IN ('a', 'b')").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("IN count");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["a".into(), "b".into()],
                negated: false
            }
        );
    }

    /// Typed-column dimensional counts (http_logs gap class): integer/bool
    /// literals and integer ranges detect against an Int64-typed column.
    /// Planned against a MemTable schema mirroring an httplogs-style index.
    async fn plan_for_typed(query: &str) -> DataFrame {
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("host", DataType::Utf8, true),
            Field::new("status", DataType::Int64, true),
            Field::new("cache_hit", DataType::Boolean, true),
            Field::new("latency_ms", DataType::Float64, true),
        ]));
        let ctx = datafusion::prelude::SessionContext::new();
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("httplogs", Arc::new(table)).unwrap();
        ctx.sql(query).await.unwrap()
    }

    /// The group key's rendering follows the PLAN's output type, which is the
    /// only thing that can tell a cast apart from the column it casts.
    ///
    /// `unwrap_alias_to_column` strips `CAST(col AS Utf8)` to find the
    /// underlying column -- that is what makes the WS-7 `attr_get` rewrite
    /// work -- so the detector cannot distinguish `GROUP BY CAST(status AS
    /// Utf8)`, whose output really IS a string, from a bare `GROUP BY status`,
    /// whose output is a number. Reading the type off the table would get the
    /// cast case wrong in the opposite direction.
    #[tokio::test]
    async fn group_key_kind_follows_the_plan_output_type() {
        let cases = [
            (
                "SELECT status, count(*) AS n FROM \"httplogs\" GROUP BY status",
                GroupKeyKind::Int,
            ),
            (
                "SELECT CAST(status AS VARCHAR) AS status, count(*) AS n \
                 FROM \"httplogs\" GROUP BY CAST(status AS VARCHAR)",
                GroupKeyKind::Str,
            ),
            (
                "SELECT host, count(*) AS n FROM \"httplogs\" GROUP BY host",
                GroupKeyKind::Str,
            ),
            (
                "SELECT cache_hit, count(*) AS n FROM \"httplogs\" GROUP BY cache_hit",
                GroupKeyKind::Bool,
            ),
            (
                "SELECT latency_ms, count(*) AS n FROM \"httplogs\" GROUP BY latency_ms",
                GroupKeyKind::Float,
            ),
        ];
        for (sql, want) in cases {
            let df = plan_for_typed(sql).await;
            let fp = detect_group_count_fast_path(df.logical_plan())
                .unwrap_or_else(|| panic!("no group-count fast path for {sql}"));
            assert_eq!(fp.group_kind, want, "wrong group key kind for {sql}");
        }
    }

    /// Typed keys order as their type, not as text.
    #[test]
    fn group_keys_order_by_type_not_by_text() {
        let mut keys = ["100", "200", "404", "500", "99"];
        keys.sort_by(|a, b| GroupKeyKind::Int.cmp_keys(a, b));
        assert_eq!(
            keys,
            ["99", "100", "200", "404", "500"],
            "integer group keys sorted lexicographically"
        );
        assert_eq!(GroupKeyKind::Int.render("404"), serde_json::json!(404));
        assert_eq!(GroupKeyKind::Str.render("404"), serde_json::json!("404"));
        assert_eq!(GroupKeyKind::Bool.render("true"), serde_json::json!(true));
        assert_eq!(GroupKeyKind::Float.render("1.5"), serde_json::json!(1.5));
    }

    /// A float column must not take the dimensional-count path.
    ///
    /// THE DEFECT THIS GUARDS. The detector consulted the column's NAME
    /// (excluding only `timestamp` and `attributes`) and never its type, while
    /// the path answers by comparing literals against group-count footer keys --
    /// the cast-to-Utf8 rendering of the column. A Float64 column renders "1.5",
    /// "2.0", and `IntRange::matches` does `key.parse::<i64>()`, which none of
    /// those satisfy. So `count(*) WHERE latency_ms > 100` on a `type: "double"`
    /// field returned 0: zero-scan, no approximation marker, and `cost.exact` is
    /// deliberately not consulted on this path. `ratio != 1` had the mirror
    /// form, counting rows whose value is 1.0 because "1.0" is not "1".
    ///
    /// Falling through to the planner is the correct outcome: slower, right.
    #[tokio::test]
    async fn detect_dimensional_count_refuses_a_float_column() {
        for sql in [
            "SELECT count(*) AS n FROM \"httplogs\" WHERE latency_ms > 100",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE latency_ms >= 10 AND latency_ms <= 20",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE latency_ms = 1",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE latency_ms != 1",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE latency_ms BETWEEN 1 AND 9",
        ] {
            let df = plan_for_typed(sql).await;
            assert!(
                detect_negation_count_fast_path(df.logical_plan()).is_none(),
                "a float column took the footer-key path, which cannot render it: {sql}"
            );
        }
        // The typed columns this path exists for must still take it, or the
        // guard would "pass" by disabling the feature.
        for sql in [
            "SELECT count(*) AS n FROM \"httplogs\" WHERE status > 100",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE status = 404",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE cache_hit = true",
            "SELECT count(*) AS n FROM \"httplogs\" WHERE host = 'h1'",
        ] {
            let df = plan_for_typed(sql).await;
            assert!(
                detect_negation_count_fast_path(df.logical_plan()).is_some(),
                "a renderable column stopped taking the fast path: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn detect_dimensional_count_matches_typed_literals_and_ranges() {
        // Int equality → footer key "404".
        let df = plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE status = 404").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("int equality");
        assert_eq!(fp.column, "status");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["404".into()],
                negated: false
            }
        );

        // Int negation.
        let df = plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE status != 200").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("int negation");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["200".into()],
                negated: true
            }
        );

        // Two-sided range, inclusive normalization.
        let df = plan_for_typed(
            "SELECT count(*) AS n FROM \"httplogs\" WHERE status >= 500 AND status <= 599",
        )
        .await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("range");
        assert_eq!(fp.column, "status");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::IntRange {
                lo: Some(500),
                hi: Some(599)
            }
        );

        // Strict bounds normalize (+1/−1); literal-on-left flips.
        let df = plan_for_typed(
            "SELECT count(*) AS n FROM \"httplogs\" WHERE status > 399 AND 500 > status",
        )
        .await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("strict range");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::IntRange {
                lo: Some(400),
                hi: Some(499)
            }
        );

        // One-sided.
        let df = plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE status >= 500").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("one-sided");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::IntRange {
                lo: Some(500),
                hi: None
            }
        );

        // BETWEEN.
        let df = plan_for_typed(
            "SELECT count(*) AS n FROM \"httplogs\" WHERE status BETWEEN 500 AND 599",
        )
        .await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("between");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::IntRange {
                lo: Some(500),
                hi: Some(599)
            }
        );

        // Bool equality → footer key "true".
        let df =
            plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE cache_hit = true").await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("bool equality");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["true".into()],
                negated: false
            }
        );

        // Int IN list.
        let df =
            plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE status IN (301, 302)")
                .await;
        let fp = detect_negation_count_fast_path(df.logical_plan()).expect("int IN");
        assert_eq!(
            fp.predicate,
            DimCountPredicate::Set {
                values: vec!["301".into(), "302".into()],
                negated: false
            }
        );

        // Range conjuncts on DIFFERENT columns must fall through.
        assert!(detect_negation_count_fast_path(
            plan_for_typed(
                "SELECT count(*) AS n FROM \"httplogs\" WHERE status >= 500 AND cache_hit = true"
            )
            .await
            .logical_plan()
        )
        .is_none());

        // Float literals must fall through (footer-key rendering not
        // reproducible from the SQL literal).
        assert!(detect_negation_count_fast_path(
            plan_for_typed("SELECT count(*) AS n FROM \"httplogs\" WHERE status = 404.0")
                .await
                .logical_plan()
        )
        .is_none());
    }

    #[test]
    fn dim_count_predicate_range_matches_numeric_keys() {
        let p = DimCountPredicate::IntRange {
            lo: Some(500),
            hi: Some(599),
        };
        assert!(p.matches("500") && p.matches("599") && p.matches("550"));
        assert!(!p.matches("499") && !p.matches("600"));
        assert!(!p.matches("50a") && !p.matches("") && !p.matches("5.5"));
        let open = DimCountPredicate::IntRange {
            lo: None,
            hi: Some(-1),
        };
        assert!(open.matches("-2") && !open.matches("0"));
    }

    #[tokio::test]
    async fn detect_group_count_fast_path_matches_order_by_group_column() {
        let df =
            plan_for("SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY host LIMIT 3")
                .await;
        let fp = detect_group_count_fast_path(df.logical_plan()).expect("group-sorted fast path");
        assert_eq!(fp.sort, Some(GroupCountSort::Group { ascending: true }));
        assert_eq!(fp.limit, Some(3));
    }

    #[tokio::test]
    async fn detect_group_count_fast_path_matches_group_order_limit() {
        let df = plan_for(
            "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 2",
        )
        .await;
        let fp = detect_group_count_fast_path(df.logical_plan()).expect("group count fast path");
        assert_eq!(fp.table_name, "events");
        assert_eq!(fp.group_column, "host");
        assert_eq!(fp.group_output_column, "host");
        assert_eq!(fp.count_output_column, "n");
        assert_eq!(fp.sort, Some(GroupCountSort::Count { ascending: false }));
        assert_eq!(fp.limit, Some(2));
    }

    #[tokio::test]
    async fn detect_group_count_fast_path_rejects_dimensional_where_clause() {
        // A pure dimensional filter has no time bounds → falls back.
        let df =
            plan_for("SELECT host, count(*) AS n FROM events WHERE source = 'smoke' GROUP BY host")
                .await;
        assert!(detect_group_count_fast_path(df.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn detect_group_count_fast_path_accepts_pure_time_window() {
        // Phase 1: a pure `timestamp ∈ [lo,hi)` filter is accepted and carries
        // the extracted time bounds.
        let df = plan_for(
            "SELECT host, count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
             GROUP BY host",
        )
        .await;
        let fp = detect_group_count_fast_path(df.logical_plan())
            .expect("windowed group count fast path");
        assert_eq!(fp.group_column, "host");
        let bounds = fp.time_bounds.expect("time bounds carried");
        assert!(bounds.start.is_some() && bounds.end.is_some());
    }

    #[tokio::test]
    async fn detect_group_count_fast_path_rejects_mixed_time_and_dimensional() {
        // A mixed time+dimensional filter is NOT a pure time range → falls back.
        let df = plan_for(
            "SELECT host, count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
               AND source = 'smoke' \
             GROUP BY host",
        )
        .await;
        assert!(detect_group_count_fast_path(df.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_accepts_pure_time_window() {
        let df = plan_for(
            "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
             GROUP BY bucket ORDER BY bucket",
        )
        .await;
        let fp = detect_date_histogram_fast_path(df.logical_plan())
            .expect("windowed histogram fast path");
        assert_eq!(fp.interval_ns, 3_600_000_000_000);
        let bounds = fp.time_bounds.expect("time bounds carried");
        assert!(bounds.start.is_some() && bounds.end.is_some());
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_rejects_mixed_time_and_dimensional() {
        let df = plan_for(
            "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events \
             WHERE timestamp >= to_timestamp(1767225600) \
               AND timestamp < to_timestamp(1767312000) \
               AND host = 'host-1' \
             GROUP BY bucket",
        )
        .await;
        assert!(detect_date_histogram_fast_path(df.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_matches_bench_shape() {
        let df = plan_for(
            "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket",
        )
        .await;
        let fp = detect_date_histogram_fast_path(df.logical_plan()).expect("histogram fast path");
        assert_eq!(fp.table_name, "events");
        assert_eq!(fp.interval_ns, 3_600_000_000_000);
        assert_eq!(fp.origin_ns, 0);
        assert_eq!(fp.bucket_sort_ascending, Some(true));
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_handles_desc_and_no_order() {
        let desc = plan_for(
            "SELECT date_bin(INTERVAL '24 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events GROUP BY bucket ORDER BY bucket DESC",
        )
        .await;
        let fp = detect_date_histogram_fast_path(desc.logical_plan()).expect("desc histogram");
        assert_eq!(fp.interval_ns, 86_400_000_000_000);
        assert_eq!(fp.bucket_sort_ascending, Some(false));

        let unordered = plan_for(
            "SELECT date_bin(INTERVAL '15 minute', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events GROUP BY bucket",
        )
        .await;
        let fp =
            detect_date_histogram_fast_path(unordered.logical_plan()).expect("unordered histogram");
        assert_eq!(fp.interval_ns, 900_000_000_000);
        assert_eq!(fp.bucket_sort_ascending, None);
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_rejects_filter_and_non_timestamp() {
        // A dimensional WHERE clause falls back (only a PURE time range is a
        // Phase 1 fast path; a `host = …` filter is not).
        let filtered = plan_for(
            "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, count(*) AS n FROM events WHERE host = 'host-1' GROUP BY bucket",
        )
        .await;
        assert!(detect_date_histogram_fast_path(filtered.logical_plan()).is_none());

        // A non-count aggregate falls back.
        let summed = plan_for(
            "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
             AS bucket, sum(length(raw)) AS n FROM events GROUP BY bucket",
        )
        .await;
        assert!(detect_date_histogram_fast_path(summed.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_handles_date_trunc() {
        // date_trunc('hour'/'day'/...) over timestamp = epoch-aligned fixed buckets.
        for (unit, interval_ns) in [
            ("second", 1_000_000_000i64),
            ("minute", 60_000_000_000),
            ("hour", 3_600_000_000_000),
            ("day", 86_400_000_000_000),
        ] {
            let df = plan_for(&format!(
                "SELECT date_trunc('{unit}', timestamp) AS h, count(*) AS n \
                 FROM events GROUP BY h ORDER BY h"
            ))
            .await;
            let fp = detect_date_histogram_fast_path(df.logical_plan())
                .unwrap_or_else(|| panic!("date_trunc('{unit}') histogram fast path"));
            assert_eq!(fp.interval_ns, interval_ns, "unit {unit}");
            assert_eq!(fp.origin_ns, 0);
            assert_eq!(fp.bucket_sort_ascending, Some(true));
        }
        // Variable-width / non-epoch-aligned units must NOT match (would mis-bucket).
        for unit in ["week", "month", "quarter", "year"] {
            let df = plan_for(&format!(
                "SELECT date_trunc('{unit}', timestamp) AS h, count(*) AS n FROM events GROUP BY h"
            ))
            .await;
            assert!(
                detect_date_histogram_fast_path(df.logical_plan()).is_none(),
                "date_trunc('{unit}') must fall back to full SQL"
            );
        }
    }

    #[test]
    fn group_count_fast_path_defaults_on_with_explicit_opt_out() {
        assert!(group_count_fast_path_enabled_from(None));
        assert!(group_count_fast_path_enabled_from(Some("1")));
        assert!(group_count_fast_path_enabled_from(Some("on")));
        assert!(!group_count_fast_path_enabled_from(Some("0")));
    }

    #[tokio::test]
    async fn group_count_fast_path_records_match_datafusion() {
        let events = vec![
            Event {
                timestamp: Utc::now(),
                host: "alpha".into(),
                source: "svc-a".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-0".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "alpha".into(),
                source: "svc-b".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-1".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "alpha".into(),
                source: "svc-c".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-2".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "bravo".into(),
                source: "svc-d".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-3".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "bravo".into(),
                source: "svc-e".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-4".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "charlie".into(),
                source: "svc-f".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "event-5".into(),
                attributes: None,
            },
        ];
        let query = "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 2";
        let (_tmp, ice, _ctx, df) = dataframe_for(query, events).await;
        let cost = estimate(&df, &ice).await.unwrap();
        let fast = try_group_count_fast_path_records(
            &df,
            &ice,
            None,
            cost.clone(),
            None,
            AllowApproximate::always(),
        )
        .await
        .unwrap()
        .expect("group count fast path");
        let batches = df.collect().await.unwrap();
        let mut expected = crate::format::batches_to_records(&batches, None).unwrap();
        expected.cost = Some(cost);
        assert_eq!(fast.columns, expected.columns);
        assert_eq!(fast.row_count, expected.row_count);
        assert_eq!(fast.rows, expected.rows);
    }

    #[tokio::test]
    async fn group_count_cutoff_merges_buffer_before_selecting_winners() {
        // More than four times K committed groups engages the bounded collector. One
        // existing group becomes the winner only after its delta is added;
        // another winner exists only in the buffer and arrives after the
        // committed iteration established the cutoff.
        let mut committed = Vec::new();
        for host in 0..100 {
            committed.extend(std::iter::repeat_n(smoke_event(host), host + 1));
        }
        let mut buffered = vec![smoke_event(0); 130];
        buffered.extend(std::iter::repeat_n(smoke_event(100), 120));
        let query = "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 3";
        let (_tmp, ice, _ctx, df) = dataframe_for(query, committed.clone()).await;
        let cost = estimate(&df, &ice).await.unwrap();
        let delta = BufferDelta {
            batches: std::collections::HashMap::from([(
                "events".to_string(),
                vec![siglake_core::events_to_record_batch(&buffered).unwrap()],
            )]),
        };
        let fast = try_group_count_fast_path_records(
            &df,
            &ice,
            None,
            cost,
            Some(&delta),
            AllowApproximate(false),
        )
        .await
        .unwrap()
        .expect("exact group count fast path");
        committed.extend(buffered);
        let reference = SessionContext::new();
        reference
            .register_batch(
                "events",
                siglake_core::events_to_record_batch(&committed).unwrap(),
            )
            .unwrap();
        let batches = reference.sql(query).await.unwrap().collect().await.unwrap();
        let expected = crate::format::batches_to_records(&batches, None).unwrap();
        assert_eq!(fast.rows, expected.rows);
        assert!(fast.approximation.is_none());
    }

    #[tokio::test]
    async fn group_count_fast_path_is_coordinator_local_only() {
        // Distributed safety: a /shard worker (shard set) must NOT use the fast
        // path — it would sort+limit its own slice before the coordinator merges.
        // It returns None (runs full SQL); the coordinator-local call fires.
        let events = vec![
            Event {
                timestamp: Utc::now(),
                host: "alpha".into(),
                source: "svc-a".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "e0".into(),
                attributes: None,
            },
            Event {
                timestamp: Utc::now(),
                host: "bravo".into(),
                source: "svc-b".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: "e1".into(),
                attributes: None,
            },
        ];
        let query = "SELECT host, count(*) AS n FROM events GROUP BY host ORDER BY n DESC LIMIT 5";
        let (_tmp, ice, _ctx, df) = dataframe_for(query, events).await;
        let cost = estimate(&df, &ice).await.unwrap();

        let shard = Some(siglake_storage::ScanShard { index: 0, count: 2 });
        assert!(
            try_group_count_fast_path_records(
                &df,
                &ice,
                shard,
                cost.clone(),
                None,
                AllowApproximate::always()
            )
            .await
            .unwrap()
            .is_none(),
            "shard worker must decline the fast path and run full SQL"
        );
        assert!(
            try_group_count_fast_path_records(
                &df,
                &ice,
                None,
                cost,
                None,
                AllowApproximate::always()
            )
            .await
            .unwrap()
            .is_some(),
            "coordinator-local must use the fast path"
        );
    }

    #[tokio::test]
    async fn date_histogram_fast_path_records_match_datafusion() {
        // Events spread across several hour buckets; some hours empty so the
        // output is non-contiguous (only non-empty buckets emitted) — must match.
        let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let offsets_secs = [10i64, 20, 90, 3700, 3800, 7300, 7400, 7500, 180000];
        let events: Vec<Event> = offsets_secs
            .iter()
            .enumerate()
            .map(|(i, s)| Event {
                timestamp: base + chrono::Duration::seconds(*s),
                host: format!("host-{i}"),
                source: "smoke".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        for (asc_sql, descending) in [("ORDER BY bucket", false), ("ORDER BY bucket DESC", true)] {
            let query = format!(
                "SELECT date_bin(INTERVAL '1 hour', timestamp, TIMESTAMP '1970-01-01T00:00:00Z') \
                 AS bucket, count(*) AS n FROM events GROUP BY bucket {asc_sql}"
            );
            let (_tmp, ice, _ctx, df) = dataframe_for(&query, events.clone()).await;
            let cost = estimate(&df, &ice).await.unwrap();
            let fast = try_date_histogram_fast_path_records(&df, &ice, None, cost.clone(), None)
                .await
                .unwrap()
                .expect("date_histogram fast path");
            let batches = df.collect().await.unwrap();
            let expected = crate::format::batches_to_records(&batches, None).unwrap();
            assert_eq!(fast.columns, expected.columns, "columns ({asc_sql})");
            assert_eq!(fast.row_count, expected.row_count, "row_count ({asc_sql})");
            assert_eq!(
                fast.rows, expected.rows,
                "rows must be byte-identical to DataFusion ({asc_sql}); descending={descending}"
            );
        }
    }

    #[tokio::test]
    async fn date_trunc_histogram_fast_path_matches_datafusion() {
        // date_trunc('hour'/'day') must produce byte-identical buckets+counts to
        // DataFusion (the fast path maps them to epoch-aligned fixed widths).
        let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let offsets_secs = [10i64, 20, 90, 3700, 3800, 7300, 90000, 180000, 180001];
        let events: Vec<Event> = offsets_secs
            .iter()
            .enumerate()
            .map(|(i, s)| Event {
                timestamp: base + chrono::Duration::seconds(*s),
                host: format!("host-{i}"),
                source: "smoke".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        for unit in ["hour", "day"] {
            let query = format!(
                "SELECT date_trunc('{unit}', timestamp) AS h, count(*) AS n \
                 FROM events GROUP BY h ORDER BY h"
            );
            let (_tmp, ice, _ctx, df) = dataframe_for(&query, events.clone()).await;
            let cost = estimate(&df, &ice).await.unwrap();
            let fast = try_date_histogram_fast_path_records(&df, &ice, None, cost.clone(), None)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("date_trunc('{unit}') fast path"));
            let batches = df.collect().await.unwrap();
            let expected = crate::format::batches_to_records(&batches, None).unwrap();
            assert_eq!(
                fast.rows, expected.rows,
                "date_trunc('{unit}') rows vs DataFusion"
            );
        }
    }

    #[tokio::test]
    async fn detect_date_histogram_fast_path_handles_order_by_limit() {
        // The canonical QW dashboard shape: `... ORDER BY h LIMIT n`. A prior gap
        // left the `Limit` root unmatched, so the fast path never engaged for the
        // real bench query (it full-scanned ~98M rows).
        let df = plan_for(
            "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
             FROM events GROUP BY h ORDER BY h LIMIT 10000",
        )
        .await;
        let fp = detect_date_histogram_fast_path(df.logical_plan())
            .expect("histogram fast path must match ORDER BY ... LIMIT");
        assert_eq!(fp.interval_ns, 3_600_000_000_000);
        assert_eq!(fp.bucket_sort_ascending, Some(true));
        assert_eq!(fp.limit, Some(10000));

        // DESC + a tight limit carries the direction and the limit.
        let desc = plan_for(
            "SELECT date_trunc('day', timestamp) AS h, count(*) AS n \
             FROM events GROUP BY h ORDER BY h DESC LIMIT 3",
        )
        .await;
        let fp = detect_date_histogram_fast_path(desc.logical_plan()).expect("desc limit");
        assert_eq!(fp.bucket_sort_ascending, Some(false));
        assert_eq!(fp.limit, Some(3));

        // No LIMIT ⇒ no limit carried (still matches).
        let no_limit = plan_for(
            "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
             FROM events GROUP BY h ORDER BY h",
        )
        .await;
        assert_eq!(
            detect_date_histogram_fast_path(no_limit.logical_plan())
                .unwrap()
                .limit,
            None
        );

        // A LIMIT without an ORDER BY truncates arbitrary buckets in DataFusion;
        // the stats path can't reproduce that, so it must fall back.
        let limit_no_order = plan_for(
            "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
             FROM events GROUP BY h LIMIT 5",
        )
        .await;
        assert!(detect_date_histogram_fast_path(limit_no_order.logical_plan()).is_none());
    }

    #[tokio::test]
    async fn date_trunc_histogram_fast_path_honors_limit() {
        // End-to-end: the limit must truncate the ordered buckets, byte-identical
        // to DataFusion's own `ORDER BY h LIMIT n`.
        let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        // Rows in 5 distinct hour buckets.
        let offsets_secs = [10i64, 3700, 7300, 10900, 14500];
        let events: Vec<Event> = offsets_secs
            .iter()
            .enumerate()
            .map(|(i, s)| Event {
                timestamp: base + chrono::Duration::seconds(*s),
                host: format!("host-{i}"),
                source: "smoke".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        for order in ["ORDER BY h LIMIT 2", "ORDER BY h DESC LIMIT 2"] {
            let query = format!(
                "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
                 FROM events GROUP BY h {order}"
            );
            let (_tmp, ice, _ctx, df) = dataframe_for(&query, events.clone()).await;
            let cost = estimate(&df, &ice).await.unwrap();
            let fast = try_date_histogram_fast_path_records(&df, &ice, None, cost, None)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("histogram fast path ({order})"));
            let batches = df.collect().await.unwrap();
            let expected = crate::format::batches_to_records(&batches, None).unwrap();
            assert_eq!(fast.row_count, 2, "limit applied ({order})");
            assert_eq!(fast.rows, expected.rows, "rows vs DataFusion ({order})");
        }
    }

    /// A user-managed index whose mapping mirrors the QW `logs-bench` shape:
    /// a `timestamp` event-time column plus a low-cardinality `level` tag.
    fn bench_like_config() -> IndexConfig {
        IndexConfig {
            index_id: "logs-bench".to_string(),
            doc_mapping: DocMapping {
                mode: MappingMode::Dynamic,
                field_mappings: vec![
                    field("timestamp", FieldType::Datetime, true),
                    field(
                        "level",
                        FieldType::Text {
                            tokenizer: Some("raw".to_string()),
                        },
                        false,
                    ),
                ],
                timestamp_field: "timestamp".to_string(),
                tag_fields: vec!["level".to_string()],
                default_search_fields: Vec::new(),
            },
            retention: None,
            index_at_flush: None,
        }
    }

    #[tokio::test]
    async fn fast_paths_apply_to_user_index_not_just_builtin_tables() {
        // Regression: the fast-path table gate used to whitelist only the built-in
        // tables, so the histogram + group-count fast paths never engaged for a
        // user index (the primary analytics target, e.g. `logs-bench`) — they
        // full-scanned instead. Both must now detect over the user index.
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&bench_like_config()).await.unwrap();
        let ctx = SessionContext::new();
        let ice = Arc::new(ice);
        register_all_tables(&ice, &ctx).await.unwrap();

        // The exact QW histogram_hourly shape, over the user index.
        let hist = ctx
            .sql(
                "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
                 FROM \"logs-bench\" GROUP BY h ORDER BY h LIMIT 10000",
            )
            .await
            .unwrap();
        let fp = detect_date_histogram_fast_path(hist.logical_plan())
            .expect("histogram fast path must engage on a user index");
        assert_eq!(fp.table_name, "logs-bench");
        assert_eq!(fp.interval_ns, 3_600_000_000_000);
        assert_eq!(fp.limit, Some(10000));

        // The exact QW count_by_level shape, over the user index.
        let grp = ctx
            .sql(
                "SELECT level, count(*) AS n FROM \"logs-bench\" \
                 GROUP BY level ORDER BY n DESC LIMIT 10",
            )
            .await
            .unwrap();
        let gp = detect_group_count_fast_path(grp.logical_plan())
            .expect("group-count fast path must engage on a user index");
        assert_eq!(gp.table_name, "logs-bench");
        assert_eq!(gp.group_column, "level");
        assert_eq!(gp.limit, Some(10));

        // And a plain whole-index count.
        let cnt = ctx
            .sql("SELECT count(*) AS n FROM \"logs-bench\"")
            .await
            .unwrap();
        assert!(
            detect_count_fast_path(cnt.logical_plan()).is_some(),
            "count fast path must engage on a user index"
        );
    }

    #[tokio::test]
    async fn user_index_fast_paths_match_datafusion_end_to_end() {
        // End-to-end correctness (not just detection): with real rows in a USER
        // index, the histogram + group-count fast paths must be byte-identical to
        // DataFusion. Guards against a footer/schema difference between `events`
        // and a managed index that detection alone wouldn't catch.
        use siglake_core::events_to_record_batch;

        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let mut config = IndexConfig::builtin_events();
        config.index_id = "logs-bench".to_string();
        ice.create_index(&config).await.unwrap();

        let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        // Rows across several hour buckets, with a low-cardinality `sourcetype`.
        let offsets_secs = [10i64, 20, 3700, 3800, 7300, 7400, 7500, 90000, 180000];
        let events: Vec<Event> = offsets_secs
            .iter()
            .enumerate()
            .map(|(i, s)| Event {
                timestamp: base + chrono::Duration::seconds(*s),
                host: format!("host-{}", i % 3),
                source: "smoke".into(),
                sourcetype: if i % 2 == 0 { "app:json" } else { "syslog" }.into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        let batch = events_to_record_batch(&events).unwrap();
        ice.append_to_table(&ice.index_table_ident("logs-bench"), batch, &[])
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let ice = Arc::new(ice);
        register_all_tables(&ice, &ctx).await.unwrap();
        // Histogram (the QW shape) over the user index, with ORDER BY + LIMIT.
        let hist_sql = "SELECT date_trunc('hour', timestamp) AS h, count(*) AS n \
             FROM \"logs-bench\" GROUP BY h ORDER BY h LIMIT 10000";
        let df = ctx.sql(hist_sql).await.unwrap();
        let cost = estimate(&df, &ice).await.unwrap();
        let fast = try_date_histogram_fast_path_records(&df, &ice, None, cost, None)
            .await
            .unwrap()
            .expect("histogram fast path engages on the user index");
        let expected =
            crate::format::batches_to_records(&df.clone().collect().await.unwrap(), None).unwrap();
        assert_eq!(
            fast.rows, expected.rows,
            "user-index histogram vs DataFusion"
        );

        // count_by_level (the QW shape) over the user index — group by sourcetype.
        let grp_sql = "SELECT sourcetype, count(*) AS n FROM \"logs-bench\" \
             GROUP BY sourcetype ORDER BY n DESC LIMIT 10";
        let df = ctx.sql(grp_sql).await.unwrap();
        let cost = estimate(&df, &ice).await.unwrap();
        let fast = try_group_count_fast_path_records(
            &df,
            &ice,
            None,
            cost,
            None,
            AllowApproximate::always(),
        )
        .await
        .unwrap()
        .expect("group-count fast path engages on the user index");
        let expected =
            crate::format::batches_to_records(&df.clone().collect().await.unwrap(), None).unwrap();
        assert_eq!(
            fast.rows, expected.rows,
            "user-index group-count vs DataFusion"
        );
    }

    #[test]
    fn referenced_tables_tracks_simple_event_query() {
        let tables =
            referenced_tables("SELECT count(*) AS n FROM events WHERE host = 'h1'").unwrap();
        assert_eq!(tables, vec!["events"]);
    }

    #[test]
    fn referenced_tables_tracks_joined_tables() {
        let tables =
            referenced_tables("SELECT * FROM events e JOIN candidates c ON e.host = c.host")
                .unwrap();
        assert_eq!(tables, vec!["candidates", "events"]);
    }

    #[test]
    fn referenced_tables_excludes_cte_aliases() {
        let tables = referenced_tables(
            "WITH errs AS (SELECT * FROM events WHERE raw LIKE '%ERROR%') \
             SELECT host, count(*) FROM errs GROUP BY host",
        )
        .unwrap();
        assert_eq!(tables, vec!["events"]);
    }

    #[test]
    fn referenced_tables_excludes_nested_cte_aliases() {
        let tables = referenced_tables(
            "WITH outer_rows AS (\
                 WITH inner_rows AS (SELECT * FROM events) \
                 SELECT * FROM inner_rows\
             ) SELECT * FROM outer_rows",
        )
        .unwrap();
        assert_eq!(tables, vec!["events"]);
    }

    #[test]
    fn nested_cte_alias_does_not_hide_enclosing_physical_table() {
        let tables = referenced_tables(
            "SELECT * FROM events \
             JOIN (WITH events AS (SELECT * FROM candidates) SELECT * FROM events) nested \
             ON true",
        )
        .unwrap();
        assert_eq!(tables, vec!["candidates", "events"]);
    }

    /// THE DEFECT THIS GUARDS. Discovery examined FROM relations and bare
    /// projection subqueries only, so a second table reachable only through a
    /// predicate (`EXISTS`, `IN`), a JOIN condition, HAVING, or an expression
    /// wrapped around a subquery was never registered — and the query failed
    /// planning with "table not found" before it ever executed, though
    /// DataFusion supports every shape below.
    #[test]
    fn referenced_tables_finds_tables_in_predicates_and_nested_expressions() {
        for sql in [
            // The acceptance shape.
            "SELECT count(*) FROM events WHERE EXISTS (SELECT 1 FROM other_index)",
            "SELECT count(*) FROM events WHERE NOT EXISTS (SELECT 1 FROM other_index)",
            "SELECT count(*) FROM events WHERE host IN (SELECT host FROM other_index)",
            "SELECT count(*) FROM events WHERE host NOT IN (SELECT host FROM other_index)",
            "SELECT count(*) FROM events WHERE n > (SELECT max(n) FROM other_index)",
            "SELECT count(*) FROM events WHERE n > ANY (SELECT n FROM other_index)",
            // A subquery nested inside a larger expression, not the whole one.
            "SELECT count(*) FROM events WHERE n > (SELECT max(n) FROM other_index) + 1",
            "SELECT count(*) FROM events WHERE \
             CASE WHEN host = 'h1' THEN n ELSE (SELECT max(n) FROM other_index) END > 3",
            "SELECT count(*) FROM events WHERE abs(n - (SELECT max(n) FROM other_index)) < 5",
            // Projection: wrapped, aliased, and as a function argument.
            "SELECT (SELECT max(n) FROM other_index) + 1 AS m FROM events",
            "SELECT coalesce((SELECT max(n) FROM other_index), 0) FROM events",
            // The remaining clauses.
            "SELECT host, count(*) FROM events GROUP BY host \
             HAVING count(*) > (SELECT max(n) FROM other_index)",
            "SELECT * FROM events e JOIN candidates c \
             ON e.host = c.host AND e.n IN (SELECT n FROM other_index)",
            "SELECT host FROM events ORDER BY (SELECT max(n) FROM other_index)",
            "SELECT host FROM events GROUP BY host, (SELECT max(n) FROM other_index)",
            // Behind a set operation and an EXPLAIN.
            "SELECT count(*) FROM events WHERE EXISTS (SELECT 1 FROM other_index) \
             UNION ALL SELECT count(*) FROM events",
            "EXPLAIN SELECT count(*) FROM events WHERE EXISTS (SELECT 1 FROM other_index)",
        ] {
            let tables = referenced_tables(sql).unwrap_or_else(|| panic!("parses: {sql}"));
            assert!(
                tables.contains(&"other_index".to_string()),
                "predicate-only table was not discovered, so it never registers: {sql} \
                 (found {tables:?})"
            );
            assert!(tables.contains(&"events".to_string()), "lost events: {sql}");
        }
    }

    /// Scope survives the wider walk: a WITH alias declared INSIDE a predicate
    /// subquery still shadows there, and only there.
    #[test]
    fn predicate_subquery_cte_alias_is_not_a_table() {
        let tables = referenced_tables(
            "SELECT count(*) FROM events WHERE EXISTS (\
                 WITH other_index AS (SELECT * FROM candidates) SELECT 1 FROM other_index\
             )",
        )
        .unwrap();
        assert_eq!(tables, vec!["candidates", "events"]);

        // ... and the enclosing query's alias shadows inside the predicate too.
        let tables = referenced_tables(
            "WITH scratch AS (SELECT * FROM candidates) \
             SELECT count(*) FROM events WHERE host IN (SELECT host FROM scratch)",
        )
        .unwrap();
        assert_eq!(tables, vec!["candidates", "events"]);
    }

    /// Wider discovery must not widen what the result cache accepts: a query
    /// whose second table is reachable only through a predicate subquery is a
    /// MULTI-table query, and the cache key carries exactly one snapshot id,
    /// so it stays out. The two tables commit on their own clocks — an entry
    /// keyed on `events`' snapshot alone would survive a commit to the index
    /// that changes the answer, and result caches are never TTL-expired.
    #[tokio::test]
    async fn a_predicate_subquery_table_keeps_a_query_out_of_the_result_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.create_index(&bench_like_config()).await.unwrap();
        // Both tables need a snapshot: the key is `(namespace, snapshot, query)`,
        // and a table that has never committed has no id to key on.
        let events: Vec<Event> = (0..2).map(smoke_event).collect();
        ice.append_events(&events).await.unwrap();
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let decide = |sql: &'static str| {
            prepare_result_cache(
                &ice,
                sql,
                false,
                QueryFormat::Records,
                &resolved,
                BufferDeltaState::Empty,
                BufferProof::default(),
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
                far_future_deadline(),
            )
        };

        for sql in [
            "SELECT count(*) AS n FROM events \
             WHERE EXISTS (SELECT 1 FROM \"logs-bench\") AND host = 'pred-sub-1'",
            "SELECT count(*) AS n FROM events \
             WHERE host IN (SELECT level FROM \"logs-bench\") AND host = 'pred-sub-2'",
            "SELECT count(*) AS n FROM events WHERE host = 'pred-sub-3' \
             GROUP BY host HAVING count(*) > (SELECT count(*) FROM \"logs-bench\")",
        ] {
            assert!(
                matches!(decide(sql).await.unwrap(), SqlResultCacheDecision::Skip),
                "a two-table query was admitted to the single-snapshot cache: {sql}"
            );
        }

        // The control: the same shape without the subquery IS cacheable, so the
        // skips above are the multi-table rule and not some other refusal.
        let single = "SELECT count(*) AS n FROM events WHERE host = 'pred-sub-control'";
        match decide(single).await.unwrap() {
            SqlResultCacheDecision::Leader(ctx) => {
                finish_result_cache(ctx, CacheEligibility::SQL, None).await
            }
            SqlResultCacheDecision::Hit(_) => panic!("nothing was ever inserted"),
            SqlResultCacheDecision::Skip => {
                panic!("single-table control was not cacheable, so the skips prove nothing")
            }
        }
    }

    #[test]
    fn normalize_query_for_cache_ignores_comments_and_spacing() {
        let left = "SELECT count(*) AS n FROM events WHERE host = 'h1'";
        let right = "  SELECT  count(*) AS n FROM events /* warm */ WHERE host = 'h1'  ";
        assert_eq!(
            normalize_query_for_cache(left),
            normalize_query_for_cache(right)
        );
    }

    #[test]
    fn normalize_query_for_cache_strips_constant_true_and_terms() {
        let left =
            "SELECT count(*) AS n FROM events WHERE timestamp >= to_timestamp(0) AND timestamp < to_timestamp(60)";
        let right = "SELECT count(*) AS n FROM events WHERE timestamp >= to_timestamp(0) AND timestamp < to_timestamp(60) AND 7 = 7";
        assert_eq!(
            normalize_query_for_cache(left),
            normalize_query_for_cache(right)
        );
    }

    #[test]
    fn result_cache_keys_preserve_null_and_numeric_predicates() {
        let resolved = ResolvedLimits::resolve(
            &RequestLimits::default(),
            Priority::Interactive,
            &crate::limits::TierLimits::interactive_defaults(),
        );
        let key = |sql: &str| {
            result_cache_key(
                "default",
                42,
                3,
                sql,
                &resolved,
                ResultCacheScope {
                    allow_approximate: AllowApproximate::always(),
                    shard: None,
                },
            )
        };
        let filtered = "SELECT count(*) AS n FROM events WHERE host = 'cache-key-host'";

        assert_eq!(
            key(filtered),
            key("SELECT count(*) AS n FROM events WHERE host = 'cache-key-host' AND 7 = 7"),
            "a proven numeric equality should still normalize away"
        );
        for predicate in ["TRUE", "'left' != 'right'"] {
            assert_eq!(
                key(filtered),
                key(&format!("{filtered} AND {predicate}")),
                "a proven true predicate should still normalize away: {predicate}"
            );
        }
        assert_ne!(
            key(filtered),
            key("SELECT count(*) AS n FROM events WHERE host = 'cache-key-host' AND NULL = NULL"),
            "NULL = NULL is UNKNOWN, not true"
        );
        assert_ne!(
            key(filtered),
            key("SELECT count(*) AS n FROM events WHERE host = 'cache-key-host' AND 1 != 1.0"),
            "numeric literals with different spellings may compare equal after coercion"
        );
    }

    #[tokio::test]
    async fn runtime_timestamp_query_executes_now_interval_predicate() {
        let body = collect_records(
            "SELECT count(host) AS n FROM events WHERE timestamp >= now() - interval '5 minutes'",
        )
        .await;
        assert_eq!(body.row_count, 1);
        assert_eq!(body.rows.as_array().unwrap()[0]["n"], 4);
    }

    #[tokio::test]
    async fn runtime_timestamp_query_executes_to_timestamp_predicate() {
        let body = collect_records(
            "SELECT count(host) AS n FROM events WHERE timestamp >= to_timestamp(0)",
        )
        .await;
        assert_eq!(body.row_count, 1);
        assert_eq!(body.rows.as_array().unwrap()[0]["n"], 4);
    }

    #[tokio::test]
    async fn runtime_timestamp_query_pushes_down_open_ended_now_predicate() {
        let plan = physical_plan_string(
            "SELECT count(host) AS n FROM events WHERE timestamp >= now() - interval '5 minutes'",
        )
        .await;
        assert!(
            plan.contains("SiglakeIcebergTableScan") && plan.contains("predicate:[timestamp >="),
            "expected predicate pushdown in physical plan, got:\n{plan}"
        );
    }

    #[tokio::test]
    async fn runtime_timestamp_query_pushes_down_between_predicate() {
        let plan = physical_plan_string(
            "SELECT count(host) AS n FROM events WHERE timestamp BETWEEN to_timestamp(0) AND to_timestamp(4102444800)",
        )
        .await;
        assert!(
            plan.contains("SiglakeIcebergTableScan")
                && plan.contains("predicate:[")
                && plan.contains("timestamp >=")
                && plan.contains("timestamp <="),
            "expected between pushdown in physical plan, got:\n{plan}"
        );
    }

    /// WS-6: with a WAL buffer dir, an `events` query sees both the committed
    /// Iceberg rows and the un-committed (sealed) WAL rows.
    #[tokio::test]
    async fn wal_buffer_override_unions_committed_and_uncommitted() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        // 4 committed rows.
        let committed: Vec<Event> = (0..4)
            .map(|i| Event::now(format!("committed {i}")))
            .collect();
        ice.append_events(&committed).await.unwrap();
        let ice = Arc::new(ice);

        // 3 un-committed rows in a sealed WAL segment under a separate dir.
        let wal_root = tmp.path().join("wal");
        let mut w = siglake_wal::WalWriter::with_thresholds(
            &wal_root,
            "ing",
            10_000,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let buffered: Vec<Event> = (0..3)
            .map(|i| Event::now(format!("buffered {i}")))
            .collect();
        w.append_events(&buffered).unwrap();
        w.seal().unwrap().expect("sealed segment");

        let ctx = SessionContext::new();
        register_all_tables(&ice, &ctx).await.unwrap();

        // Without override: Iceberg-only ⇒ 4.
        let n_before = count_events(&ctx).await;
        assert_eq!(n_before, 4);

        // With override: union ⇒ 7.
        override_events_with_wal_buffer(&ice, &ctx, &wal_root, None)
            .await
            .unwrap();
        let n_after = count_events(&ctx).await;
        assert_eq!(n_after, 7, "4 committed + 3 un-committed WAL rows");
    }

    /// #1262 / benchmark run #34: the delta load and manifest estimate used to
    /// be two independent table generations. A drain commit in between made one
    /// response count the drained segment twice; the next coherent response
    /// then fell by exactly that segment's rows. Exercise the same interval with
    /// a competing recluster commit and a tiny injected backlog budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn whole_table_count_stays_snapshot_consistent_during_wal_drain_and_recluster() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        // Pinned event data. The rewrite below hands two of the committed files
        // to `recluster_files`, which takes one `day(timestamp)` partition per
        // call; on `Event::now()` a run that crossed UTC midnight between the
        // three appends split them across two days and the rewrite came back an
        // error (#5678). Midnight of a fixed day plus a second per row, the same
        // base the fixtures above use.
        let base = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        for commit in 0..3i64 {
            let events: Vec<Event> = (0..4i64)
                .map(|row| Event {
                    timestamp: base + chrono::Duration::seconds(commit * 4 + row),
                    ..Event::now(format!("base {commit}/{row}"))
                })
                .collect();
            ice.append_events(&events).await.unwrap();
        }

        let wal_root = tmp.path().join("wal");
        let buffered: Vec<Event> = (0..5i64)
            .map(|row| Event {
                timestamp: base + chrono::Duration::seconds(100 + row),
                ..Event::now(format!("buffered {row}"))
            })
            .collect();
        let mut writer = siglake_wal::WalWriter::with_thresholds(
            &wal_root,
            "ing",
            10_000,
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        writer.append_events(&buffered).unwrap();
        let segment = writer.seal().unwrap().expect("sealed segment");
        let segment_name = segment
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let mut segment_batches = siglake_wal::read_segment(&segment.path).unwrap();
        assert_eq!(segment_batches.len(), 1, "fixture writes one WAL batch");
        let segment_batch = segment_batches.pop().unwrap();

        let identity = CallerIdentity::default();
        let cache = Arc::new(crate::wal_buffer::BufferDeltaCache::default());
        let query = "SELECT count(*) AS n FROM events";

        // The over-budget path intentionally serves committed-only. Capture all
        // fields emitted by the production count observation.
        let over = load_buffer_delta_with_max(&ice, &wal_root, &cache, &identity, query, Some(1))
            .await
            .unwrap();
        let mut over_cost = count_cost_for_test(&ice, query).await;
        let over_count = resolve_whole_table_count(
            &mut over_cost,
            "events",
            over.delta(),
            Some(&over),
            "test_committed_only",
        );
        assert_eq!(over_count.manifest_rows, 12);
        assert_eq!(over_count.buffer_within_budget, Some(false));
        assert_eq!(over_count.included_buffer_rows, 0);
        assert_eq!(over_count.response_path, "test_committed_only");
        assert_eq!(over_count.count, 12);

        // Capture snapshot A + its five-row delta, then commit that same segment
        // while a recluster is also trying to publish a row-preserving rewrite.
        let before =
            load_buffer_delta_with_max(&ice, &wal_root, &cache, &identity, query, Some(u64::MAX))
                .await
                .unwrap();
        let before_state = &before.counts["events"];
        assert!(before_state.snapshot_id.is_some());
        assert_eq!(before_state.manifest_rows, Some(12));
        assert!(before_state.buffer_within_budget);
        assert_eq!(before_state.included_buffer_rows, 5);

        let ident = ice.events_table_ident().clone();
        let rewrite_files: Vec<_> = ice
            .live_data_files(&ident)
            .await
            .unwrap()
            .into_iter()
            .take(2)
            .collect();
        // The bin is one partition by construction of `base` above; stating it
        // here puts a lost pin on the seeding rather than on the rewrite.
        assert!(
            rewrite_files
                .iter()
                .all(|file| file.partition() == rewrite_files[0].partition()),
            "the rewrite bin must hold one partition value: {:?}",
            rewrite_files
                .iter()
                .map(|file| format!("{:?}", file.partition()))
                .collect::<std::collections::BTreeSet<_>>()
        );
        let consumed = vec![segment_name];
        let (drain, rewrite) = tokio::join!(
            ice.append_batch_with_consumed(segment_batch.clone(), &consumed),
            ice.recluster_files(
                &ident,
                rewrite_files.clone(),
                siglake_storage::iceberg::BLOOM_FILTER_COLUMNS
            ),
        );
        if drain.is_err() {
            ice.append_batch_with_consumed(segment_batch, &consumed)
                .await
                .unwrap();
        }
        if rewrite.is_err() {
            let retry_files: Vec<_> = ice
                .live_data_files(&ident)
                .await
                .unwrap()
                .into_iter()
                .take(2)
                .collect();
            ice.recluster_files(
                &ident,
                retry_files,
                siglake_storage::iceberg::BLOOM_FILTER_COLUMNS,
            )
            .await
            .unwrap();
        }

        // This is the old answer: snapshot B's 17 committed rows plus snapshot
        // A's five-row delta. The following coherent answer fell by five.
        let post_cost = count_cost_for_test(&ice, query).await;
        assert_eq!(post_cost.estimated_rows_processed, 17);
        let old_racy_count =
            post_cost.estimated_rows_processed + before.delta().unwrap().rows("events", None);
        assert_eq!(old_racy_count, 22, "reproduces the transient overcount");

        // The fixed resolver keeps A's base and A's exclusion set together, so
        // even a later estimate of B cannot alter this response.
        let mut raced_cost = post_cost.clone();
        let raced = resolve_whole_table_count(
            &mut raced_cost,
            "events",
            before.delta(),
            Some(&before),
            "test_buffer_delta",
        );
        assert_eq!(raced.snapshot_id, before_state.snapshot_id);
        assert_eq!(raced.manifest_rows, 12);
        assert_eq!(raced.buffer_within_budget, Some(true));
        assert_eq!(raced.included_buffer_rows, 5);
        assert_eq!(raced.response_path, "test_buffer_delta");
        assert_eq!(raced.count, 17);

        let after =
            load_buffer_delta_with_max(&ice, &wal_root, &cache, &identity, query, Some(u64::MAX))
                .await
                .unwrap();
        let mut after_cost = count_cost_for_test(&ice, query).await;
        let after_count = resolve_whole_table_count(
            &mut after_cost,
            "events",
            after.delta(),
            Some(&after),
            "test_committed_only_after_drain",
        );
        assert_eq!(after_count.manifest_rows, 17);
        assert_eq!(after_count.included_buffer_rows, 0);
        assert_eq!(after_count.count, 17);
        assert_eq!(old_racy_count - after_count.count, 5);
        assert!(over_count.count <= raced.count && raced.count <= after_count.count);

        // Every published Iceberg snapshot is itself monotonic. The recluster
        // publishes the same total; only the old cross-snapshot composition can
        // manufacture the falling response.
        let table = ice.catalog().load_table(&ident).await.unwrap();
        let mut history: Vec<_> = table
            .metadata()
            .snapshots()
            .filter_map(|snapshot| {
                snapshot
                    .summary()
                    .additional_properties
                    .get("total-records")
                    .and_then(|rows| rows.parse::<u64>().ok())
                    .map(|rows| (snapshot.sequence_number(), rows))
            })
            .collect();
        history.sort_unstable_by_key(|(sequence, _)| *sequence);
        assert!(
            history.windows(2).all(|pair| pair[0].1 <= pair[1].1),
            "a rewrite/refresh published fewer rows: {history:?}"
        );
    }

    async fn count_cost_for_test(ice: &Arc<IcebergContext>, query: &str) -> CostReport {
        let ctx = SessionContext::new();
        register_all_tables(ice, &ctx).await.unwrap();
        let df = ctx.sql(query).await.unwrap();
        estimate(&df, ice).await.unwrap()
    }

    async fn count_events(ctx: &SessionContext) -> i64 {
        let df = ctx.sql("SELECT count(*) AS n FROM events").await.unwrap();
        let batches = df.collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    }

    /// Phase 5 — measure the per-query overhead FLOOR that sits under every
    /// `/api/v1/sql` request (`sql.rs:1005-1036`), one phase at a time, on a
    /// multi-file events table. Reports, for `count(*)` and a `GROUP BY` query:
    /// (a) `session_context_with_order` build, (b) register
    /// (`register_tables_for_query`: cached_table_entry + register_table),
    /// (c) `ctx.sql()` logical plan, (d) `estimate()`/`scan_cost()`.
    ///
    /// Locally `scan_cost` is cheap (no S3 manifest-walk latency); the
    /// context-build + logical-plan numbers are real and motivate Step 3. Run:
    /// `cargo test -p siglake-query-server --lib --release \
    ///    bench_per_query_overhead_floor -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn bench_per_query_overhead_floor() {
        use std::time::Instant;

        let tmp = tempfile::tempdir().unwrap();
        let ice = IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        // Many small commits => many live data files (so the manifest walk +
        // scan_cost iterate has real work, like the production events table).
        let day0 = 1_700_000_000i64;
        let commits = 40usize;
        let per = 500usize;
        for c in 0..commits {
            let batch: Vec<Event> = (0..per)
                .map(|j| {
                    let secs = day0 + (c * per + j) as i64;
                    Event {
                        timestamp: Utc.timestamp_opt(secs, 0).single().unwrap(),
                        host: format!("host-{:03}", j % 50),
                        source: "/var/log/app.log".into(),
                        sourcetype: "app:json".into(),
                        index: "main".into(),
                        raw: format!("level=info req_id={} status=200", c * per + j),
                        attributes: None,
                    }
                })
                .collect();
            ice.append_events(&batch).await.unwrap();
        }
        let ice = Arc::new(ice);
        let n_files = ice
            .live_data_files(ice.events_table_ident())
            .await
            .unwrap()
            .len();

        let queries = [
            ("count(*)", "SELECT count(*) FROM events"),
            (
                "group_by",
                "SELECT host, count(*) FROM events GROUP BY host",
            ),
        ];
        let iters = 50u32;

        eprintln!("BENCH per-query floor: {n_files} live files, {iters} iters/phase");
        for (label, sql) in queries {
            // Warm the table-metadata cache (cached_table_entry / footer) once so
            // we measure the steady-state floor, not the first cold load.
            {
                let ctx = siglake_storage::session_context_with_order(None, None, None);
                let _ = register_tables_for_query(&ice, &ctx, sql).await.unwrap();
                let df = ctx.sql(sql).await.unwrap();
                let _ = crate::cost::estimate(&df, &ice).await.unwrap();
            }

            let mut build_ns = 0u128;
            let mut register_ns = 0u128;
            let mut logical_ns = 0u128;
            let mut estimate_ns = 0u128;
            for _ in 0..iters {
                let t = Instant::now();
                let ctx = siglake_storage::session_context_with_order(None, None, None);
                build_ns += t.elapsed().as_nanos();

                let t = Instant::now();
                let _ = register_tables_for_query(&ice, &ctx, sql).await.unwrap();
                register_ns += t.elapsed().as_nanos();

                let t = Instant::now();
                let df = ctx.sql(sql).await.unwrap();
                logical_ns += t.elapsed().as_nanos();

                let t = Instant::now();
                let _ = crate::cost::estimate(&df, &ice).await.unwrap();
                estimate_ns += t.elapsed().as_nanos();
            }
            let us = |ns: u128| (ns as f64) / (iters as f64) / 1000.0;
            let total = us(build_ns) + us(register_ns) + us(logical_ns) + us(estimate_ns);
            eprintln!(
                "  [{label:8}] build={:7.1}us register={:7.1}us logical={:7.1}us estimate={:7.1}us | total={:7.1}us",
                us(build_ns),
                us(register_ns),
                us(logical_ns),
                us(estimate_ns),
                total
            );
        }
    }
}

// ===== Distributed query (#7 part 2b) ============================================

#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ShardQueryRequest {
    pub query: String,
    #[serde(default)]
    pub shard: Option<ShardParam>,
    /// Original query priority, forwarded by the coordinator so the worker
    /// applies the matching tier's mid-flight breaker. Defaults to interactive
    /// (the only priority that is ever coordinated).
    #[serde(default)]
    pub priority: Priority,
    /// #89/#1655: the exact table generation used to construct the
    /// coordinator's WAL partial — a snapshot by id (or the initial empty
    /// generation) and, since #2554, the schema id it planned against. The
    /// worker reads THAT generation so a fan-out is consistent cluster-wide: a
    /// worker whose cache runs ahead would otherwise double-serve rows the
    /// coordinator still counts in its buffer partial, or answer with a wider
    /// column set than the merge was planned for.
    ///
    /// A worker that cannot resolve the pinned generation (a snapshot expired,
    /// a schema unknown, or either still not visible after one metadata
    /// refresh) answers **503** rather than serving its own current one: file
    /// shards only partition the table when every worker enumerates the same
    /// file generation, and a two-phase merge only type-checks when every
    /// partial carries the schema the coordinator planned. Omit `pin` to read
    /// the worker's current generation deliberately.
    #[serde(default)]
    pub pin: Option<ShardPin>,
    /// The tenant the ORIGINAL caller resolved to, forwarded by the coordinator.
    ///
    /// A shard request carries the coordinator's own credential, not the
    /// caller's, so without this the worker re-derives tenancy from a token that
    /// belongs to no tenant and reads the DEFAULT namespace for a tenant
    /// caller -- a silent cross-tenant read on the packaged 2-replica default.
    ///
    /// Honoured ONLY when the request authenticated with this node's own
    /// `coordinator_token` -- `CallerIdentity::coordinator`, decided once in the
    /// auth middleware. Otherwise a user who can reach `/shard` directly could
    /// name any tenant they liked.
    #[serde(default)]
    pub tenant: Option<String>,
}

/// #89/#1655: one table generation a shard read is pinned to.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ShardPin {
    pub table: String,
    /// #2921: the Iceberg table incarnation the coordinator captured.
    /// Snapshot and schema identifiers can both repeat when a snapshotless
    /// managed index is dropped and recreated under the same name.
    ///
    /// A worker enforces this against the same cached table handle it uses to
    /// resolve the schema or snapshot. A mismatch refreshes once, then refuses
    /// the shard if the named incarnation is no longer available.
    ///
    /// Omitted means the pre-#2921 contract: snapshot and schema pins are
    /// enforced as supplied, without incarnation identity. This permits an
    /// older coordinator to address a newer worker. An older worker ignores a
    /// UUID sent by a newer coordinator, retaining the old exposure until all
    /// workers have been upgraded.
    #[serde(default)]
    pub table_uuid: Option<String>,
    /// The exact committed snapshot to read. Mutually exclusive with `empty`.
    pub snapshot_id: Option<i64>,
    /// Pin the table's initial, snapshotless generation. This differs from
    /// omitting `pin`, which deliberately asks for the worker's current
    /// generation.
    #[serde(default)]
    pub empty: bool,
    /// #2554: the Iceberg schema id the coordinator PLANNED against, from the
    /// same metadata generation as `snapshot_id`.
    ///
    /// A snapshot is not the whole identity of what a query is served from. An
    /// additive `migrate-schema` commits an `UpdateSchemaAction` and no data
    /// snapshot, so the schema id moves while the snapshot id stands still. A
    /// worker whose metadata cache predates that migration therefore RECOGNISES
    /// the pinned snapshot and answers from its narrow schema, and a fragment
    /// referencing the new column fails on that peer as invalid SQL while every
    /// other peer succeeds.
    ///
    /// A worker enforces this alongside the snapshot: it refreshes its metadata
    /// once, serves the pinned schema — including a historical one, so a worker
    /// AHEAD of the coordinator is not refused — and answers **503**
    /// (`reason: "shard_pin_unresolved"`) when the generation cannot be
    /// honoured at all.
    ///
    /// Omitted means the pre-#2554 contract: the snapshot alone is enforced and
    /// the worker plans against its own current schema. That is what an older
    /// COORDINATOR sends. In the other direction — a coordinator that sends it
    /// to an older WORKER — the field is ignored, so a mixed-version cluster
    /// keeps the old exposure until every replica is upgraded.
    #[serde(default)]
    pub schema_id: Option<i32>,
}

/// Worker endpoint: run the (sharded) query locally and return the result as an
/// Arrow IPC stream (lossless transport). Not intended for human clients.
#[utoipa::path(
    post,
    path = "/api/v1/sql/shard",
    tag = "sql",
    request_body = ShardQueryRequest,
    responses(
        (status = 200, description = "Partial result for this shard, as an Arrow IPC \
            **stream** (lossless — the coordinator merges typed batches, not JSON).",
         content_type = "application/vnd.apache.arrow.stream", body = Vec<u8>),
        (status = 400, description = "Empty or unparseable SQL, an invalid generation pin, \
            a statement that is \
            not a read (DDL, DML and session statements — `CREATE`, `COPY … TO`, \
            `INSERT`, `SET` — are refused before they can take effect), or an \
            invalid shard selector.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 413, description = "The mid-flight rows-scanned breaker tripped.",
         body = ApiErrorBody),
        (status = 500, description = "Internal error.", body = ApiErrorBody),
        (status = 503, description = "Retryable refusal. Either this worker's query \
            memory pool (or spill directory byte cap) refused an allocation, or the \
            request carried a `pin` whose generation this worker cannot resolve even \
            after a metadata refresh (`reason: \"shard_pin_unresolved\"` in the body, \
            with the pin) — a pinned shard is never answered from a different \
            table incarnation or snapshot, because file shards only partition the \
            table when every worker reads the same file generation, nor from a \
            different schema, because a fan-out plans once on the coordinator. The \
            coordinator forwards either to \
            the caller rather than re-running the shard on itself.",
         body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The shard exceeded its wall-clock budget. \
            The budget starts when the request arrives and covers preparation \
            (tenant resolution, table registration, pin resolution, planning) as \
            well as execution, so a shard that spends it all preparing is \
            refused without starting its scan.",
         body = ApiErrorBody),
    ),
)]
pub async fn shard(
    State(state): State<AppState>,
    axum::extract::Extension(identity): axum::extract::Extension<CallerIdentity>,
    Json(req): Json<ShardQueryRequest>,
) -> Result<Response, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::bad_request("query is empty"));
    }
    // Per-pod governance parity (#7): each worker enforces the mid-flight
    // rows-scanned breaker on *its* shard, so no single pod runs unbounded. The
    // cluster's aggregate ceiling is therefore N × the per-pod limit — which is
    // the point of distribution. The coordinator's pre-flight bytes breaker
    // governs total cost. Workers only serve interactive shards (the transparent
    // path skips coordination for Batch), so the interactive tier is correct.
    let tier = state.limits.tier(req.priority);
    let resolved = ResolvedLimits::resolve(&RequestLimits::default(), req.priority, &tier);
    // WALL-CLOCK BUDGET, ANCHORED AT REQUEST ARRIVAL.
    //
    // The 504 this endpoint's OpenAPI list documents used to be armed only
    // around the collect, with `timeout(resolved.timeout, …)` — a FRESH full
    // budget handed out after tenancy resolution, `SEARCH` rewriting, table
    // registration, pin resolution and both planning steps had already run
    // unbounded. A shard that spent 40s preparing then got another 60s to
    // collect, outliving the coordinator waiting on it and holding pool bytes
    // the whole way. Same defect as #1526 on the local path, different endpoint.
    //
    // One instant, taken here, covers every phase below. Unconditional: the
    // shard path resolves from `RequestLimits::default()`, so `circuit_breakers`
    // is always on and the collect timeout was unconditional too.
    let deadline = tokio::time::Instant::now() + resolved.timeout;
    macro_rules! under_shard_deadline {
        ($future:expr) => {{
            // Pre-poll check, like `apply_timeout`: `timeout_at` polls once
            // before consulting the clock, so an already-ready metadata path
            // would otherwise turn an exhausted budget into a success.
            if tokio::time::Instant::now() >= deadline {
                return Err(shard_timeout_error(&resolved));
            }
            match tokio::time::timeout_at(deadline, $future).await {
                Ok(res) => res?,
                Err(_) => return Err(shard_timeout_error(&resolved)),
            }
        }};
    }
    // TENANCY COMES FROM THE COORDINATOR, NOT FROM THIS HOP'S CREDENTIAL.
    //
    // The coordinator authenticates to peers with its own service token, so the
    // identity this handler was handed describes the COORDINATOR, not the user
    // who asked. Re-deriving the tenant from it sent a tenant caller's fan-out
    // to the default namespace -- reading data that is not theirs, silently,
    // on the chart's default topology.
    //
    // The forwarded value is trusted only because the request proved it came
    // from the coordinator by presenting this node's own coordinator token. A
    // request that did not is left with the identity it actually authenticated
    // as, so reaching `/shard` directly grants nothing extra.
    //
    // The forwarded value is still checked against the same rule the auth
    // middleware applies to a JWT claim, so the ONE definition of a usable
    // tenant id holds on every path into `resolve_ice`. A coordinator only
    // forwards a tenant it already resolved, so this refuses nothing real; it
    // is here so the trusted hop cannot become the way an unusable identifier
    // reaches the registry, and it answers 400 (the coordinator sent a bad
    // request) rather than the 500 the registry's backstop would raise.
    let identity = match identity.coordinator {
        true => {
            if let Some(t) = req.tenant.as_deref() {
                if crate::tenants::validate(t).is_none() {
                    return Err(ApiError::bad_request(format!(
                        "shard request names an unusable tenant ({})",
                        siglake_core::tenant::TENANT_ID_RULE
                    )));
                }
            }
            CallerIdentity {
                tenant: req.tenant.clone(),
                ..identity
            }
        }
        false => identity,
    };
    // A worker may be more restrictive than the coordinator during a rolling
    // configuration change. Enforce its own policy against the authenticated,
    // forwarded tenant before `resolve_ice` can create a namespace or context;
    // the coordinator preserves this deliberate 403 for the caller.
    state.authorize_tenant(&identity)?;
    // Resolving the tenant context can create and open a catalog; the `SEARCH`
    // rewrite reads the index config behind it. Catalog work, on this request's
    // clock like everything else.
    let ice = under_shard_deadline!(async {
        state
            .resolve_ice(&identity)
            .await
            .map_err(ApiError::internal)
    });
    let query = under_shard_deadline!(rewrite_search_if_needed(&ice, &req.query));
    let preferred_scan_order = preferred_scan_order_for_sql(&ice, &query).await;
    // PER-REQUEST CANCELLATION, which this path never had. On the default chart
    // config (`query.replicas: 2`, distributed on) every user query executes
    // here, so every coordinator timeout or client disconnect left a worker
    // scanning to completion with nothing able to stop it: dropping the request
    // future does not abort the plan's spawned partition pumps, and the flag
    // this sets is what ends the source stream they drain from.
    //
    // Now more load-bearing, not less: shards no longer take an admission slot,
    // so nothing else bounds how many abandoned scans a pod can accumulate.
    //
    // Safe to scope to this handler because the shard response is BUFFERED
    // (Arrow IPC, collected before the response is built). The main path has to
    // hand its guard to the NDJSON body instead, because that streams after the
    // handler returns.
    let cancel = siglake_storage::QueryCancel::new();
    let _cancel_guard = siglake_storage::CancelOnDrop(cancel.clone());
    // A worker's shard is its own execution, with its own id in ITS pod's log.
    // The coordinator's id is not carried across the hop: the shard body is a
    // versioned wire contract and a pod-local counter would not be comparable
    // anyway. What joins the two halves is the W3C trace context the fan-out
    // already propagates (see `HttpShardRunner::run_detailed`).
    let execution_id = siglake_storage::QueryExecutionId::next();
    let resolved_shard = req.shard.and_then(ShardParam::resolve);
    log_query_execution_start(
        "sql_shard",
        execution_id,
        Some(&query),
        resolved_shard.map(|s| (s.index, s.count)),
    );
    let ctx = state
        .query_scan
        .session_context_sharded_with_order(resolved_shard, preferred_scan_order);
    // Injected BEFORE planning: the scan node captures the flag at PLANNING
    // time, because the collect path builds its own `TaskContext` and session
    // extensions do not survive to `execute()`.
    let ctx = {
        let mut hinted = ctx.state();
        hinted
            .config_mut()
            .set_extension(std::sync::Arc::new(cancel.clone()));
        hinted
            .config_mut()
            .set_extension(std::sync::Arc::new(execution_id));
        datafusion::prelude::SessionContext::new_with_state(hinted)
    };
    // Registration reads table metadata from the catalog and object store — the
    // phase a slow backend stretches furthest past the budget. The
    // half-registered session context dies with the request.
    under_shard_deadline!(register_tables_for_query(&ice, &ctx, &query));
    // #89: pin the shard read to the coordinator's serving snapshot.
    //
    // #1525: a pin that is STILL unresolvable after `pinned_table_provider`'s
    // one refresh is a refusal (503 + `Retry-After`), not a fallback to this
    // worker's current snapshot.
    //
    // The fallback was explicit and explicitly tested, and it was wrong: a
    // file shard is `files[i], files[i+N], …` of ONE snapshot's live file
    // list, so shards only partition the table when every worker enumerates
    // the same generation. Serving this fragment from a newer snapshot — the
    // exact situation the pin exists to prevent, and the likely one, since a
    // pin misses precisely when the metadata moved — yields a 200 whose count
    // is wrong in either direction (rows double-served across a commit
    // boundary, rows dropped after a compaction rewrite renumbers the list).
    // The miss window is narrow and the answer is unmarked, so nothing
    // downstream can tell a mixed-snapshot fan-out from a good one. Failing
    // the fragment is recoverable; a silently wrong exact count is not.
    //
    // The coordinator does not re-run this fragment: 503 is a verdict, and
    // `coordinator::shard_failure_is_retryable` refuses to fail over on a
    // verdict. (Its failover runner carries the same pin anyway.)
    if let Some(pin) = req.pin.as_ref() {
        match (pin.snapshot_id, pin.empty) {
            (None, true) => {
                // #2554: an explicit empty pin still has a SCHEMA generation.
                // Building the `EmptyTable` from this worker's current schema
                // was the same defect as the snapshot arm's — a fan-out over a
                // table that a migration widened before its first append
                // planned wide on the coordinator and narrow on a stale peer.
                //
                // Without a pinned schema id (an older coordinator) the
                // worker's own registered schema is what it always was.
                let schema = match pin.schema_id {
                    None => ctx
                        .table_provider(&pin.table)
                        .await
                        .map_err(|e| {
                            ApiError::internal(anyhow::anyhow!(
                                "resolving schema for empty pin on `{}`: {e}",
                                pin.table
                            ))
                        })?
                        .schema(),
                    Some(schema_id) => {
                        match under_shard_deadline!(async {
                            ice.pinned_empty_schema(
                                &pin.table,
                                schema_id,
                                pin.table_uuid.as_deref(),
                            )
                            .await
                            .map_err(ApiError::internal)
                        }) {
                            siglake_storage::iceberg::PinnedEmptySchema::Resolved(schema) => schema,
                            siglake_storage::iceberg::PinnedEmptySchema::SchemaMissing => {
                                metrics::counter!(
                                    "siglake_query_shard_pin_total",
                                    "outcome" => "miss"
                                )
                                .increment(1);
                                tracing::warn!(table = %pin.table, schema_id,
                                    "pinned schema generation not resolvable after refresh; \
                                     refusing the empty-table shard rather than serving a \
                                     different schema");
                                return Err(ApiError::shard_pin_schema_unresolved(
                                    &pin.table,
                                    None,
                                    schema_id,
                                    pin.table_uuid.as_deref(),
                                ));
                            }
                            siglake_storage::iceberg::PinnedEmptySchema::IncarnationMissing => {
                                metrics::counter!(
                                    "siglake_query_shard_pin_total",
                                    "outcome" => "miss"
                                )
                                .increment(1);
                                let table_uuid = pin.table_uuid.as_deref().unwrap_or_default();
                                tracing::warn!(table = %pin.table, table_uuid,
                                    "pinned table incarnation not resolvable after refresh; \
                                     refusing the empty-table shard");
                                return Err(ApiError::shard_pin_incarnation_unresolved(
                                    &pin.table,
                                    None,
                                    pin.schema_id,
                                    table_uuid,
                                ));
                            }
                        }
                    }
                };
                let empty = datafusion::datasource::empty::EmptyTable::new(schema);
                let _ = ctx.deregister_table(&*pin.table);
                ctx.register_table(&*pin.table, std::sync::Arc::new(empty))
                    .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
                metrics::counter!("siglake_query_shard_pin_total", "outcome" => "pinned")
                    .increment(1);
            }
            (Some(snapshot_id), false) => {
                // A pin miss triggers a metadata refresh inside `pinned_table_provider`,
                // so this is another catalog round trip on the budget.
                match under_shard_deadline!(async {
                    ice.pinned_table_provider(
                        &pin.table,
                        snapshot_id,
                        pin.schema_id,
                        pin.table_uuid.as_deref(),
                    )
                    .await
                    .map_err(ApiError::internal)
                }) {
                    siglake_storage::iceberg::PinnedGeneration::Resolved(provider) => {
                        let _ = ctx.deregister_table(&*pin.table);
                        ctx.register_table(&*pin.table, provider)
                            .map_err(|e| ApiError::internal(anyhow::anyhow!(e)))?;
                        metrics::counter!("siglake_query_shard_pin_total", "outcome" => "pinned")
                            .increment(1);
                    }
                    // The counter arm keeps its name: `outcome="miss"` is what the
                    // bench and kind harnesses already read. What changed is what
                    // a miss now costs — a refused shard, not a quiet wrong answer.
                    // #2554 counts a schema miss the same way: both are a pinned
                    // generation this worker cannot reproduce.
                    siglake_storage::iceberg::PinnedGeneration::SnapshotMissing => {
                        metrics::counter!("siglake_query_shard_pin_total", "outcome" => "miss")
                            .increment(1);
                        tracing::warn!(table = %pin.table, snapshot_id,
                    "shard pin not resolvable after refresh; refusing the shard \
                     rather than serving a different snapshot");
                        return Err(ApiError::shard_pin_unresolved(
                            &pin.table,
                            snapshot_id,
                            pin.schema_id,
                            pin.table_uuid.as_deref(),
                        ));
                    }
                    siglake_storage::iceberg::PinnedGeneration::SchemaMissing => {
                        metrics::counter!("siglake_query_shard_pin_total", "outcome" => "miss")
                            .increment(1);
                        // Only reachable with a pinned schema id: with none,
                        // the snapshot decides the outcome by itself.
                        let schema_id = pin.schema_id.unwrap_or_default();
                        tracing::warn!(table = %pin.table, snapshot_id, schema_id,
                            "pinned schema generation not resolvable after refresh; \
                             refusing the shard rather than serving a different schema");
                        return Err(ApiError::shard_pin_schema_unresolved(
                            &pin.table,
                            Some(snapshot_id),
                            schema_id,
                            pin.table_uuid.as_deref(),
                        ));
                    }
                    siglake_storage::iceberg::PinnedGeneration::IncarnationMissing => {
                        metrics::counter!("siglake_query_shard_pin_total", "outcome" => "miss")
                            .increment(1);
                        let table_uuid = pin.table_uuid.as_deref().unwrap_or_default();
                        tracing::warn!(table = %pin.table, snapshot_id, table_uuid,
                            "pinned table incarnation not resolvable after refresh; \
                             refusing the shard");
                        return Err(ApiError::shard_pin_incarnation_unresolved(
                            &pin.table,
                            Some(snapshot_id),
                            pin.schema_id,
                            table_uuid,
                        ));
                    }
                }
            }
            _ => {
                return Err(ApiError::bad_request(
                    "shard pin must name exactly one of `snapshot_id` or `empty: true`",
                ));
            }
        }
    }
    crate::udfs::register_udfs(&ctx);
    let df = under_shard_deadline!(async {
        plan_client_sql(&ctx, &query)
            .await
            .map_err(|e| ApiError::bad_request(format!("{e:#}")))
    });
    // No `estimate()` here any more: its only consumer on this path was the
    // admission reservation removed below, and the pre-flight bytes breaker runs
    // on the COORDINATOR. Keeping the call would be a cost estimation per shard
    // request — on the hot path of every distributed query — feeding nothing.

    // NO ADMISSION HERE, deliberately. A shard is a fragment of a query its
    // coordinator has already admitted, so admitting it again double-counts —
    // which the coordinator's own call site acknowledged — and on a pod acting
    // as both coordinator and worker it DEADLOCKS: the coordinator holds a
    // reservation, then blocks on a self-shard request that must acquire from
    // the same controller. It also capped the cluster: every distributed query
    // needed a slot on every pod, so adding replicas could not raise concurrent
    // distributed queries at all.
    //
    // The pod is still governed, by the two things that actually bound it: the
    // process-wide memory pool, and the mid-flight rows-scanned breaker this
    // path already enforces per shard.

    // One plan handle for both collect paths so the shard's scan attribution
    // (node counters → `x-siglake-scan` response header → coordinator
    // `stats.scan`) survives the Arrow-IPC transport, which has no body slot.
    // Taken before the DataFrame is consumed: the shard executes with the
    // session it planned under (its shard pin, its target partitions and any
    // `datafusion.execution.*` option), on the shared bounded pool (#2251).
    let task_ctx = crate::midflight::task_ctx_for(&df);
    let plan = under_shard_deadline!(async {
        df.create_physical_plan().await.map_err(ApiError::internal)
    });
    // The row breaker below bounds ROWS, which is a different quantity from the
    // wall clock: a scan can be slow without being large (a deep unconverged
    // layout, a stalled S3 fetch), and the row cap never fires for it.
    let collect = async {
        let batches = if resolved.circuit_breakers {
            match crate::midflight::collect_plan_with_rows_scanned_cap(
                plan.clone(),
                task_ctx,
                resolved.max_rows_scanned,
                resolved.priority,
            )
            .await
            // A pool refusal on a shard is a 503 the coordinator forwards (see
            // `ApiError::from_query`) and does NOT fail over: re-running the
            // shard on itself at the moment memory is scarcest is amplification.
            .map_err(|e| ApiError::from_execution(e, resolved.priority))?
            {
                crate::midflight::CollectOutcome::Ok(b) => b,
                crate::midflight::CollectOutcome::RowsScannedExceeded {
                    rows_scanned,
                    limit,
                    ..
                } => {
                    metrics::counter!(
                        "siglake_query_breaker_trips_total",
                        "breaker" => "midflight_rows_shard",
                        "priority" => resolved.priority.label()
                    )
                    .increment(1);
                    let mut err = ApiError::new_status(
                        axum::http::StatusCode::PAYLOAD_TOO_LARGE,
                        format!(
                            "shard mid-flight breaker: scanned {} rows, exceeded {} limit",
                            rows_scanned, limit
                        ),
                    );
                    let runtime = crate::midflight::summarize_plan_runtime(&plan);
                    err.context = Some(serde_json::json!({
                        "stats": {
                            "rows_scanned": runtime.leaf_output_rows,
                            "scan": scan_detail_from_runtime(&runtime),
                        },
                    }));
                    return Err(err);
                }
                // Unreachable: this call site passes `AccumulationBound::None`.
                // The refusing bound belongs to the Jaeger routes (#2184).
                crate::midflight::CollectOutcome::AccumulatedBoundExceeded { .. } => {
                    return Err(ApiError::internal(
                        "shard collect reported a refusing accumulation bound",
                    ))
                }
            }
        } else {
            crate::midflight::collect_plan(plan.clone(), task_ctx)
                .await
                .map_err(|e| ApiError::from_execution(e, resolved.priority))?
        };
        Ok::<_, ApiError>(batches)
    };
    // What is left of the budget after preparation, not a second copy of it.
    // `_cancel_guard` fires as the macro's `return` unwinds this frame, so the
    // scan stops rather than running on unobserved — which is the whole point of
    // arming it: a timeout that only stops WAITING leaves the work running.
    let batches = under_shard_deadline!(collect);
    let bytes = crate::format::batches_to_arrow_ipc(&batches).map_err(ApiError::internal)?;
    let mut resp = Response::new(Body::from(bytes));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/vnd.apache.arrow.stream"),
    );
    let runtime = crate::midflight::summarize_plan_runtime(&plan);
    let scan = scan_detail_from_runtime(&runtime);
    let encoded_scan = serde_json::to_vec(&scan).map_err(ApiError::internal)?;
    if encoded_scan.len() > crate::coordinator::SHARD_SCAN_HEADER_MAX_BYTES {
        return Err(ApiError::internal(anyhow::anyhow!(
            "shard scan attribution is {} bytes, exceeds {}-byte transport limit",
            encoded_scan.len(),
            crate::coordinator::SHARD_SCAN_HEADER_MAX_BYTES
        )));
    }
    let scan_header = axum::http::HeaderValue::from_bytes(&encoded_scan)
        .map_err(|error| ApiError::internal(anyhow::Error::new(error)))?;
    resp.headers_mut().insert("x-siglake-scan", scan_header);
    Ok(resp)
}

// Fan-out and merging are implemented in `crate::coordinator`.
/// Coordinator endpoint: fan `req.query` across the configured worker peers and
/// merge into the single-pod answer. 400 when no peers are configured. Renders
/// `records` (default) or `ndjson`.
#[utoipa::path(
    post,
    path = "/api/v1/sql/distributed",
    tag = "sql",
    request_body = SqlRequest,
    responses(
        (status = 200, description = "Merged result rows, or NDJSON when the request \
            asked for `format: \"ndjson\"`.",
         content(
             (RecordsResponse = "application/json"),
             (String = "application/x-ndjson"),
         )),
        (status = 400, description = "Empty or unparseable SQL, a statement that is \
            not a read (DDL, DML and session statements — `CREATE`, `COPY … TO`, \
            `INSERT`, `SET` — are refused before they can take effect), or no \
            worker peers are configured on this server.", body = ApiErrorBody),
        (status = 401, description = "Missing or invalid credentials.", body = ApiErrorBody),
        (status = 403, description = "Tenant refused before the handler runs. This \
            server derives the tenant from a verified JWT claim when one is \
            configured (`--oidc-tenant-claim`), and this token carries none, or \
            carries one that is not a usable tenant id (`[A-Za-z0-9_-]`, 1..=128 \
            chars), or is absent from the configured `--allowed-tenants` \
            set. Answered by the auth middleware, so it can reach every \
            operation behind it.", body = ApiErrorBody),
        (status = 413, description = "Row cap hit, or the mid-flight rows-scanned \
            breaker tripped.",
         content(
             (RecordsResponse = "application/json"),
             (ApiErrorBody = "application/json"),
         )),
        (status = 422, description = "Unprocessable request body: a key inside \
            `limits` is not a known per-request limit. The commonest case is \
            `max_rows`, which is the RESPONSE envelope's name for the applied cap; \
            the request field is `max_rows_returned`. Refused by the JSON extractor \
            before planning, execution or batch enqueue, so no work is done and no \
            job is created — a dropped cap is never applied silently. The body is \
            the extractor's plain-text message naming the unknown field, not an \
            `ApiErrorBody`. Unknown keys at the TOP level of the request are still \
            ignored."),
        (status = 429, description = "Admission control rejected the query.",
         body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 500, description = "Internal error, including a failed shard with no \
            surviving replica.", body = ApiErrorBody),
        (status = 503, description = "Retryable refusal, from this coordinator or from \
            a worker whose answer is forwarded: the query memory pool refused an \
            allocation, or a shard could not resolve the generation — table \
            incarnation, snapshot or schema — this coordinator pinned the fan-out to \
            (`reason: \"shard_pin_unresolved\"`). A pinned shard is never answered from \
            a different generation, so the whole query is refused rather than merged \
            across them. Wait `Retry-After` seconds and retry.", body = ApiErrorBody,
         headers(("retry-after" = u64, description = "Seconds to wait before retrying."))),
        (status = 504, description = "The query exceeded its wall-clock budget.",
         body = ApiErrorBody),
    ),
)]
pub async fn distributed(
    State(state): State<AppState>,
    axum::extract::Extension(identity): axum::extract::Extension<CallerIdentity>,
    Json(req): Json<SqlRequest>,
) -> Result<Response, ApiError> {
    // #967: the EXPLICIT endpoint reports that no membership is available
    // rather than silently running single-pod — a caller who asked for the
    // distributed path wants to know it did not happen. The transparent
    // `/api/v1/sql` falls back to local instead (see `handle`).
    let Some(peers) = state.peers.as_ref().and_then(|source| source.capture()) else {
        return Err(ApiError::bad_request(
            "distributed query not configured, or peer discovery has not published a \
             membership yet (set --query-peers, or --query-peer-discovery-srv and wait \
             for the first answer)",
        ));
    };
    distributed_inner(state, identity, req, None, None, peers).await
}

#[tracing::instrument(skip_all, fields(otel.kind = "internal", query = %req.query))]
async fn distributed_inner(
    state: AppState,
    identity: CallerIdentity,
    req: SqlRequest,
    pre_resolved_ice: Option<Arc<siglake_storage::iceberg::IcebergContext>>,
    pre_rewritten_query: Option<RewrittenQuery>,
    // #967: the ONE membership this query uses — captured by the caller,
    // immutable for the request. Every read below (`N`, the shard→peer map,
    // the WAL-partial branch, failover, attribution) comes from here, so a
    // pod that joins or leaves mid-query cannot duplicate or omit a shard.
    peers: Arc<crate::discovery::PeerSnapshot>,
) -> Result<Response, ApiError> {
    if req.query.trim().is_empty() {
        return Err(ApiError::bad_request("query is empty"));
    }
    // #86: a query over exactly ONE distributable table — `events` or a
    // managed user index (workers scan its Iceberg file shards; the /shard
    // endpoint registers tables generically) — fans out. Anything else — a
    // hot-cache table function (last_values()/distinct_values()), a detection
    // or system table, a join, or no table — runs single-pod on this
    // coordinator, which has the full UDF/UDTF set registered. Keyed on the
    // SQL's table names, which (unlike a UDTF's logical-plan scan name) is an
    // unambiguous discriminator.
    let ice = match pre_resolved_ice {
        Some(ice) => ice,
        None => state
            .resolve_ice(&identity)
            .await
            .map_err(ApiError::internal)?,
    };
    let distributable = match referenced_tables(&req.query).as_deref() {
        Some([t]) if t == "events" => true,
        // #4074: the same question `get_index` used to answer here, read from
        // the bounded-staleness table cache. `get_index` does an uncached
        // `load_table` — the per-query metadata.json read that
        // `register_index_with_datafusion` was changed to avoid — and this gate
        // runs before anything is dispatched, so keeping it here just moved
        // that read one step earlier in the same request.
        Some([t]) => ice.is_managed_index(t).await.unwrap_or(false),
        _ => false,
    };
    if !distributable {
        return handle_local_inner(state, identity, req, Some(ice), pre_rewritten_query, None)
            .await;
    }
    let start = Instant::now();
    let format = req.format.unwrap_or_default();
    let tier = state.limits.tier(req.priority);
    let resolved = ResolvedLimits::resolve(&req.limits, req.priority, &tier);
    let effective_query = match pre_rewritten_query {
        Some(query) => query,
        None => {
            apply_query_rewrites(
                &ice,
                &req.query,
                req.default_order,
                req.priority,
                resolved.max_rows_returned,
            )
            .await?
        }
    };
    // Planning context: tables registered so the coordinator can classify the
    // query + estimate cost. It is not used to scan (the workers do that).
    let plan_started = Instant::now();
    // Same per-request cancellation as the local path. The coordinator does real
    // scanning here before it dispatches -- cost estimation and the Tier-1
    // battery -- and that work must stop when the request goes away.
    let cancel = siglake_storage::QueryCancel::new();
    let _cancel_guard = siglake_storage::CancelOnDrop(cancel.clone());
    // The coordinator's PRE-DISPATCH work is its own execution: it plans, it
    // cost-estimates and it runs the Tier-1 battery, all of which scan. Its
    // scan events are a different execution from the workers' shards and from
    // anything this pod serves locally, and this id is what says so.
    let planning_execution_id = siglake_storage::QueryExecutionId::next();
    log_query_execution_start(
        "sql_coordinator",
        planning_execution_id,
        Some(&effective_query.sql),
        None,
    );
    let planning = {
        let mut st = state
            .query_scan
            .session_context_sharded_with_order(None, effective_query.preferred_scan_order.clone())
            .state();
        st.config_mut()
            .set_extension(std::sync::Arc::new(cancel.clone()));
        st.config_mut()
            .set_extension(std::sync::Arc::new(planning_execution_id));
        datafusion::prelude::SessionContext::new_with_state(st)
    };
    register_tables_for_query(&ice, &planning, &effective_query.sql).await?;
    crate::udfs::register_udfs(&planning);
    // WS-6: a last_values()/distinct_values() query classifies as Local and runs
    // on this coordinator ctx, so its UDTFs must be registered here too.
    if let Some(reg) = state.hot_caches.as_ref() {
        reg.register_udtfs(&planning, identity.tenant.as_deref().unwrap_or("default"));
    }

    // Same pre-flight bytes breaker as the single-pod path — the estimate is on
    // the whole-table plan (total work), which is the right ceiling to enforce.
    let df = plan_client_sql(&planning, &effective_query.sql)
        .await
        .map_err(|e| ApiError::bad_request(format!("{e:#}")))?;
    let cost = estimate(&df, &ice).await.map_err(ApiError::internal)?;
    let tier1_plan_micros = plan_started.elapsed().as_micros() as u64;
    // A ROW-LIMITED query is governed by the MID-FLIGHT breaker, not by this
    // one. `estimated_bytes_scanned` is an upper bound, and a `LIMIT` makes it
    // meaningless in BOTH directions -- see `cost::is_row_limited`. Refusing on
    // it took `WHERE region=... LIMIT 100`, which answers in 88ms over 1.0B
    // rows, and turned it into a 400 (measured 2026-08-25: 6,413 refusals in
    // under three hours). The row breaker stops the pathological case at an
    // exact, measured row count instead of a hypothetical byte count.
    let row_limited = crate::cost::is_row_limited(df.logical_plan(), resolved.max_rows_returned);
    if row_limited && cost.estimated_bytes_scanned > resolved.max_bytes_scanned {
        metrics::counter!("siglake_query_preflight_bytes_skipped_limited_total").increment(1);
    }
    if !row_limited && cost.estimated_bytes_scanned > resolved.max_bytes_scanned {
        metrics::counter!(
            "siglake_query_breaker_trips_total",
            "breaker" => "preflight_bytes",
            "priority" => resolved.priority.label()
        )
        .increment(1);
        let msg = format!(
            "estimated bytes scanned ({}) exceeds the {} limit ({})",
            cost.estimated_bytes_scanned,
            resolved.priority.label(),
            resolved.max_bytes_scanned,
        );
        let result = Err(ApiError::cost_rejected(msg, &cost));
        emit_terminal_audit(
            state.audit.as_ref(),
            &identity,
            "sql",
            &req,
            start,
            &result,
            &cost,
        );
        return result;
    }

    // Distributed Tier-1 short-circuit: the coordinator holds the whole-table
    // manifest and side aggregates (total-records, group counts, time buckets),
    // so a whole-table metadata answer is served HERE (zero file reads, ~ms)
    // instead of fanning a hash-aggregate out to every shard. The battery below
    // is the FULL local one, tried in this order; the first `Some` is the answer
    // and each arm returns `None` when the plan is not its shape:
    //   1. whole-table `count(*)`            manifest total-records
    //   2. windowed `count(*)`               time-bucket aggregate + ≤2 boundary
    //                                        ranges
    //   3. negation / inequality count       total − Σ excluded group counts
    //   4. `count(DISTINCT col)`             guarded per-value counts
    //   5. `GROUP BY col count(*)`           group-count side aggregate
    //   6. date_histogram                    `date_bin`/`date_trunc(timestamp)`
    //                                        + `count(*)`, bare table or a PURE
    //                                        time window: snapshot time-bucket
    //                                        aggregate, else per-file footers,
    //                                        else a timestamp scan of only the
    //                                        bucket-straddling files
    // date_histogram IS short-circuited (arm 6). An earlier version of this
    // comment said it was not, and that misdirected the #550 fan-out
    // measurement: a records-format histogram over `events` (bare or with a pure
    // time window) never reaches a worker, so it proves nothing about fan-out.
    //
    // Gating. Two conditions, both on the `if` below:
    // - Records format only. An ndjson request skips the battery entirely and
    //   takes the classify()/fan-out path (or `handle_local_inner`, which has
    //   the ndjson fast-path variants, when it classifies Local).
    // - The buffered delta loaded (#61 hybrid). With no WAL buffer configured
    //   there is no delta and Tier-1 is always safe. With one, `load_buffer_delta`
    //   reads the uncommitted rows once (`None` when nothing is buffered) and
    //   every arm folds them into its committed answer, so a Tier-1 answer never
    //   misses just-ingested rows. If that load FAILS, Tier-1 is skipped and the
    //   query fans out, where `compute_buffer_partials` (extra_partials below)
    //   does the same fold on the merge. (The old static buffer-off gate 413'd
    //   every distributed aggregate whenever buffering was merely configured;
    //   the interim dynamic guard still fell to worker full scans while any
    //   segment was in flight.)
    //
    // What still fans out: every ndjson aggregate, and any records-format shape
    // no arm accepts -- for date_histogram that is a dimensional (or mixed)
    // filter, an ORDER BY on the count column, a LIMIT without ORDER BY, or a
    // non-`count(*)` aggregate. Those classify as (Ordered)Aggregate and run as
    // per-shard hash-aggregates on the workers' /shard, which has no fast-path
    // battery, and merge here.
    let tier1_delta_started = Instant::now();
    let (tier1_safe, tier1_load): (bool, Option<BufferDeltaLoad>) =
        match state.wal_buffer_dir.as_deref() {
            None => (true, None),
            Some(buffer_root) => {
                match load_buffer_delta(
                    &ice,
                    buffer_root,
                    &state.buffer_delta_cache,
                    &identity,
                    &effective_query.sql,
                )
                .await
                {
                    Ok(load) => (true, Some(load)),
                    Err(err) => {
                        tracing::warn!(error = ?err,
                            "coordinator buffer delta load failed; skipping Tier-1");
                        (false, None)
                    }
                }
            }
        };
    let tier1_delta_micros = tier1_delta_started.elapsed().as_micros() as u64;
    if tier1_safe && format == QueryFormat::Records {
        // #86 follow-through: the FULL local fast-path battery, not just
        // count/group-count. Windowed counts, negation, count-distinct, and
        // date histograms are whole-table metadata/rollup answers (with the
        // buffered delta folded in) — fanning them out ships full scans to
        // workers that have no fast paths on /shard, which is exactly the
        // six 500s of the first round-2 board.
        let battery_started = Instant::now();
        let delta = tier1_load.as_ref().and_then(BufferDeltaLoad::delta);
        let served = if let Some(body) = try_count_fast_path_records(
            &df,
            None,
            cost.clone(),
            delta,
            tier1_load.as_ref(),
            "coordinator_tier1_fast_path",
        ) {
            Some(body)
        } else if let Some(body) =
            try_windowed_count_fast_path_records(&df, &ice, None, cost.clone(), delta).await?
        {
            Some(body)
        } else if let Some(body) =
            try_negation_count_fast_path_records(&df, &ice, None, cost.clone(), delta).await?
        {
            Some(body)
        } else if let Some(body) =
            try_count_distinct_fast_path_records(&df, &ice, None, cost.clone(), delta).await?
        {
            Some(body)
        } else if let Some(body) =
            try_grouped_numeric_fast_path_records(&df, &ice, None, cost.clone(), delta).await?
        {
            Some(body)
        } else if let Some(body) = try_group_count_fast_path_records(
            &df,
            &ice,
            None,
            cost.clone(),
            delta,
            // The caller's `exact` and the operator's kill switch, NOT a
            // literal `true`. Both controls are documented as deliberate,
            // and with distribution on by default the coordinator is the
            // path every user query takes -- so hardcoding here made both
            // of them inert wherever they actually mattered.
            AllowApproximate::for_request(&req),
        )
        .await?
        {
            Some(body)
        } else {
            try_date_histogram_fast_path_records(&df, &ice, None, cost.clone(), delta).await?
        };
        if let Some(mut body) = served {
            attach_fast_path_phases(
                &mut body,
                tier1_plan_micros,
                tier1_delta_micros,
                battery_started,
                Some(crate::format::DistPhaseStats {
                    mode: "tier1_local".to_string(),
                    ..Default::default()
                }),
            );
            metrics::counter!("siglake_query_coordinator_total", "mode" => "tier1_local")
                .increment(1);
            let result: Result<Response, ApiError> =
                Ok((StatusCode::OK, Json(body)).into_response());
            emit_terminal_audit(
                state.audit.as_ref(),
                &identity,
                "sql",
                &req,
                start,
                &result,
                &cost,
            );
            record_metrics("sql", start, &result, &cost);
            return result;
        }
    }

    // #86: shapes that don't benefit from fan-out run on THIS coordinator —
    // with its full fast-path battery, UDFs, and buffer union — instead of
    // being shipped whole to a worker (coordinate's Local fallback posts to
    // peers[0]'s /shard, which has none of that; first round-2 board):
    // - classify() == Local (non-mergeable shapes), and
    // - small-LIMIT (ordered) scans: a single node early-stops after a few
    //   pages, while fan-out forces per-worker drains + transfer + merge
    //   (match_all measured 769ms distributed vs ~50ms local).
    let dist_preview = crate::coordinator::classify(df.logical_plan());
    let scan_local = match &dist_preview {
        crate::coordinator::DistPlan::Local => true,
        crate::coordinator::DistPlan::OrderedScan { limit: Some(n), .. }
        | crate::coordinator::DistPlan::Scan { limit: Some(n) } => {
            if *n > dist_scan_local_max_limit() {
                false
            } else {
                // A small LIMIT usually early-stops, which is why these run
                // local. But a LOW-SELECTIVITY predicate makes a small LIMIT the
                // most expensive shape there is -- it sifts the whole table to
                // fill 100 rows -- and that is precisely the browse that timed
                // out on one node. Distribute those, and only those: without
                // exact footer evidence this falls through to today's behaviour.
                let min = dist_browse_min_scan_rows();
                let sift = if min == 0 {
                    None
                } else {
                    browse_expected_scan_rows(&ice, &effective_query.sql, *n).await
                };
                match sift {
                    Some(rows) if rows >= min => {
                        metrics::counter!("siglake_query_coordinator_total",
                            "mode" => "browse_distributed")
                        .increment(1);
                        tracing::debug!(
                            expected_sift_rows = rows,
                            limit = *n,
                            "distributing a low-selectivity browse"
                        );
                        false
                    }
                    _ => true,
                }
            }
        }
        _ => false,
    };
    if scan_local {
        metrics::counter!("siglake_query_coordinator_total", "mode" => "scan_local").increment(1);
        return handle_local_inner(state, identity, req, Some(ice), Some(effective_query), None)
            .await;
    }

    let pin_table = referenced_tables(&effective_query.sql)
        .and_then(|t| t.into_iter().next())
        .unwrap_or_else(|| "events".to_string());
    let buffered_read = capture_buffered_shard_read(&ice, pin_table.clone()).await?;

    // WS-6: compute the WAL-buffer partial once on the coordinator (the workers
    // scan only their Iceberg file shards). Folded into the merge so a
    // distributed query sees just-ingested rows. Empty unless the buffer is
    // configured, the plan is distributable, and the query reads exactly one
    // bufferable table (events or a managed index).
    let buffer_started = Instant::now();
    let extra_partials = if peers.len() <= 1 {
        Vec::new()
    } else if let Some(buffer_root) = state.wal_buffer_dir.as_deref() {
        compute_buffer_partials(
            &buffered_read,
            buffer_root,
            &state.buffer_delta_cache,
            identity.tenant.as_deref(),
            &effective_query.sql,
            df.logical_plan(),
            resolved.priority,
        )
        .await?
    } else {
        Vec::new()
    };
    let buffer_delta_micros = buffer_started.elapsed().as_micros() as u64;

    #[cfg(test)]
    if let Some(hook) = state.distributed_dispatch_hook.as_ref() {
        hook.captured.notify_one();
        hook.resume.notified().await;
    }

    // #89/#1655: workers scan the exact generation whose consumed-segment set
    // formed the WAL partial. In particular, a `snapshot_id` of `None` is an
    // explicit empty-table pin, not permission to read a first commit that
    // landed after capture.
    //
    // #2554: the generation includes the SCHEMA id, and it comes out of the
    // same capture — `buffered_read` — that produced the snapshot and the
    // consumed set. Asking the metadata cache for a schema id separately later
    // could pair this snapshot with a generation it never belonged to, which
    // is the mistake #2494 closed on the result-cache key.
    let pin_generation = buffered_read.snapshot.generation();
    let runner = crate::coordinator::HttpShardRunner::new(
        peers.peers.clone(),
        state.coordinator_token.clone(),
    )
    .with_tenant(identity.tenant.clone())
    .with_pin(pin_table.clone(), pin_generation.clone());
    // The ONLY admission reservation a distributed query takes: one share on
    // this coordinator, priced from the whole-query estimate and held through
    // fan-out and merge. Workers admit nothing (see `shard`: admitting a
    // fragment of an already-admitted query double-counted, deadlocked a pod
    // against its own ordinal shard, and capped the cluster). Measured
    // 2026-09-05 on a coordinator-plus-peer pair with a four-slot budget:
    // `/api/v1/sql` and `/api/v1/sql/local` both admit four and refuse the
    // fifth, and the peer trips admission zero times.
    let _admission = match acquire_admission(&state, &resolved, &cost).await {
        Ok(guard) => guard,
        Err(err) => {
            let result = Err(err);
            emit_terminal_audit(
                state.audit.as_ref(),
                &identity,
                "sql",
                &req,
                start,
                &result,
                &cost,
            );
            record_metrics("sql", start, &result, &cost);
            return result;
        }
    };
    // #94: the coordinator itself is the failover target — it holds the full
    // file set, so a dead worker degrades one shard's latency, not the query.
    // The fallback posts to the coordinator's OWN /shard endpoint, so a
    // failed-over shard runs the exact worker path — pruning, breakers, pin —
    // with zero duplicated semantics (a hand-rolled local runner missed the
    // pruning parity and tripped the rows breaker on the 07-13 live test).
    //
    // #967: the URL is the captured snapshot's EXPLICIT self member, not
    // `peers[0]`. Under the static list every pod rendered the same list, so
    // `peers[0]` was this node only on ordinal zero — every other pod failed a
    // shard over to ordinal zero. SRV ordering makes that assumption worse
    // still. The retry target is fixed for the request's lifetime: a peer that
    // joined after capture cannot receive a failed-over shard.
    let fallback = crate::coordinator::HttpShardRunner::new(
        vec![peers.self_url.clone(); peers.len()],
        state.coordinator_token.clone(),
    )
    .with_tenant(identity.tenant.clone())
    .with_pin(pin_table.clone(), pin_generation);
    let result: Result<Response, ApiError> = async {
        let (batches, coord_stats) = crate::coordinator::coordinate_with_failover(
            &planning,
            &runner,
            Some(&fallback),
            &effective_query.sql,
            peers.len(),
            extra_partials,
        )
        .await
        .map_err(|e| ApiError::from_query(e, resolved.priority))?;
        match format {
            QueryFormat::Ndjson => {
                let body =
                    crate::format::batches_to_ndjson(&batches).map_err(ApiError::internal)?;
                let mut resp = Response::new(Body::from(body));
                resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    axum::http::HeaderValue::from_static("application/x-ndjson"),
                );
                Ok(resp)
            }
            QueryFormat::Records => {
                let render_started = Instant::now();
                let mut body = batches_to_records(&batches, Some(resolved.max_rows_returned))
                    .map_err(ApiError::internal)?;
                body.cost = Some(cost.clone());
                // #85: coordinator-side attribution — plan/classify, the
                // buffer partial, each shard's wall (fan-out is concurrent,
                // so the max is the fan-out cost), and the merge.
                body.stats = Some(crate::format::ScanStats {
                    phases: Some(Box::new(crate::format::PhaseStats {
                        plan_micros: coord_stats.plan_micros,
                        buffer_delta_micros,
                        collect_micros: 0,
                        render_micros: render_started.elapsed().as_micros() as u64,
                        distributed: Some(crate::format::DistPhaseStats {
                            mode: coord_stats.mode.to_string(),
                            shard_wall_micros: coord_stats.shard_wall_micros,
                            merge_micros: coord_stats.merge_micros,
                            peers: peers.len(),
                            peer_generation: peers.generation,
                        }),
                    })),
                    // Summed worker-side scan attribution (x-siglake-scan
                    // shard headers) — fan-out queries get the same
                    // pruning-effectiveness view as single-pod ones.
                    scan: coord_stats.scan.map(Box::new),
                    ..Default::default()
                });
                Ok((StatusCode::OK, Json(body)).into_response())
            }
        }
    }
    .await;

    emit_terminal_audit(
        state.audit.as_ref(),
        &identity,
        "sql",
        &req,
        start,
        &result,
        &cost,
    );
    record_metrics("sql", start, &result, &cost);
    result
}

#[cfg(test)]
mod distributed_buffer_generation_tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};
    use siglake_core::{index_config::IndexConfig, Event};
    use siglake_storage::iceberg::{IcebergContext, CONSUMED_SEGMENTS_PROP};
    use std::sync::Arc;

    async fn serve(state: AppState) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback test server");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::router(state);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, task)
    }

    fn race_events() -> Vec<Event> {
        let base = Utc.with_ymd_and_hms(2026, 9, 6, 0, 0, 0).unwrap();
        (0..9)
            .map(|i| {
                let mut event = Event::now(format!("generation-race row {i}"));
                event.host = format!("host-{}", i % 3);
                event.timestamp = base + Duration::seconds(i);
                event
            })
            .collect()
    }

    async fn run_case(managed_index: bool) {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let wal_root = tmp.path().join("wal");
        let table = if managed_index {
            "race-index"
        } else {
            "events"
        };
        let config = managed_index.then(|| IndexConfig {
            index_id: table.to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        });

        // The writer owns catalog mutations; coordinator and peer have
        // independent metadata caches, as they do in the two-replica chart.
        let writer = IcebergContext::open(&warehouse).await.unwrap();
        if let Some(config) = config.as_ref() {
            writer.create_index(config).await.unwrap();
        }
        let coordinator_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let peer_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());

        // Start with a snapshotless table and one sealed segment. Capturing
        // this state must mean "empty Iceberg + these WAL rows" even after the
        // segment is drained into the table.
        assert_eq!(
            coordinator_ice.table_snapshot_id(table).await.unwrap(),
            None,
            "fixture must exercise the captured-empty generation"
        );
        let wal_dir = if managed_index {
            wal_root.join("default").join(table)
        } else {
            wal_root.join("default")
        };
        std::fs::create_dir_all(&wal_dir).unwrap();
        let events = race_events();
        let mut wal = siglake_wal::WalWriter::with_thresholds(
            &wal_dir,
            format!("race-{table}"),
            10_000,
            std::time::Duration::from_secs(600),
        )
        .unwrap();
        wal.append_events(&events).unwrap();
        let sealed = wal.seal().unwrap().expect("sealed race segment");
        drop(wal);
        let basename = sealed
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let (peer_url, peer_task) = serve(AppState::new(peer_ice, crate::AuthConfig::open())).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind coordinator");
        let coordinator_url = format!("http://{}", listener.local_addr().unwrap());
        let hook = Arc::new(crate::DistributedDispatchHook::default());
        let mut state = AppState::new(coordinator_ice.clone(), crate::AuthConfig::open())
            .with_coordinator(vec![coordinator_url.clone(), peer_url], None)
            .with_wal_buffer_dir(Some(wal_root));
        state.distributed_dispatch_hook = Some(hook.clone());
        let app = crate::router(state);
        let coordinator_task =
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // The residual predicate forces fan-out (Tier-1 cannot answer it), and
        // GROUP BY makes a duplicated drain visible in the merged per-key sum.
        let quoted = if managed_index {
            format!("\"{table}\"")
        } else {
            table.to_string()
        };
        let sql = format!(
            "SELECT host, count(*) AS n FROM {quoted} \
             WHERE raw LIKE '%generation-race%' GROUP BY host"
        );
        let request = {
            let sql = sql.clone();
            let coordinator_url = coordinator_url.clone();
            tokio::spawn(async move {
                reqwest::Client::new()
                    .post(format!("{coordinator_url}/api/v1/sql/distributed"))
                    .json(&serde_json::json!({ "query": sql }))
                    .send()
                    .await
                    .unwrap()
            })
        };

        // The hook is after WAL partial construction and before either shard
        // request. Drain the exact segment and force the coordinator's next
        // metadata access to observe the new snapshot. The old implementation
        // re-read here and pinned the workers to that snapshot, counting 18.
        tokio::time::timeout(std::time::Duration::from_secs(10), hook.captured.notified())
            .await
            .expect("query reached the pre-dispatch boundary");
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let batch = match config.as_ref() {
            Some(config) => siglake_core::map_carrier_batch(&batch, config).unwrap(),
            None => batch,
        };
        let ident = if managed_index {
            writer.index_table_ident(table)
        } else {
            writer.events_table_ident().clone()
        };
        let mut props = std::collections::HashMap::new();
        props.insert(CONSUMED_SEGMENTS_PROP.to_string(), basename);
        writer
            .append_to_table_with_props(&ident, batch, &[], props)
            .await
            .unwrap();
        coordinator_ice.invalidate_cached_table(&ident).await;
        assert!(
            coordinator_ice
                .table_snapshot_id(table)
                .await
                .unwrap()
                .is_some(),
            "the coordinator refresh between partial and dispatch must see the drain"
        );
        hook.resume.notify_one();

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), request)
            .await
            .expect("distributed request completed")
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let distributed_total: i64 = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["n"].as_i64().unwrap())
            .sum();
        assert_eq!(distributed_total, events.len() as i64, "{body}");
        assert_eq!(
            body["stats"]["phases"]["distributed"]["shard_wall_micros"]
                .as_array()
                .map(Vec::len),
            Some(2),
            "fixture must genuinely fan out to two peers: {body}"
        );

        // Fresh context, no result cache and no WAL union: the committed table
        // is the uncached reference for both the total and every group.
        let reference_ice = IcebergContext::open(&warehouse).await.unwrap();
        let reference = siglake_storage::session_context_with(None, None);
        if managed_index {
            assert!(reference_ice
                .register_index_with_datafusion(&reference, table)
                .await
                .unwrap());
        } else {
            reference_ice
                .register_with_datafusion(&reference)
                .await
                .unwrap();
        }
        let expected = reference.sql(&sql).await.unwrap().collect().await.unwrap();
        let expected = batches_to_records(&expected, None).unwrap().rows;
        let normalize = |rows: &serde_json::Value| {
            let mut rows: Vec<_> = rows
                .as_array()
                .unwrap()
                .iter()
                .map(|row| serde_json::to_string(row).unwrap())
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(normalize(&body["rows"]), normalize(&expected));

        coordinator_task.abort();
        peer_task.abort();
    }

    #[tokio::test]
    async fn drain_refresh_cannot_move_wal_partial_worker_generation() {
        run_case(false).await;
        run_case(true).await;
    }

    /// #2921: a coordinator captures empty incarnation A and its WAL partial;
    /// before dispatch, the catalog name is replaced by empty incarnation B.
    /// Both schemas have id 0, so the UUID is the only pin component that can
    /// make the B worker refuse the captured-A fan-out.
    #[tokio::test]
    async fn recreation_between_wal_capture_and_fan_out_refuses_the_old_incarnation() {
        const TABLE: &str = "incarnation_race_idx";
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let wal_root = tmp.path().join("wal");
        let writer = IcebergContext::open(&warehouse).await.unwrap();

        let first_config = IndexConfig {
            index_id: TABLE.to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        writer.create_index(&first_config).await.unwrap();
        let coordinator_ice = Arc::new(
            IcebergContext::open(&warehouse)
                .await
                .unwrap()
                .with_table_cache_ttl(std::time::Duration::from_secs(3600)),
        );
        let peer_ice = Arc::new(
            IcebergContext::open(&warehouse)
                .await
                .unwrap()
                .with_table_cache_ttl(std::time::Duration::from_secs(3600)),
        );
        let first = coordinator_ice
            .current_table_generation(TABLE)
            .await
            .unwrap();
        assert!(
            first.snapshot_id.is_none(),
            "fixture must be empty: {first:?}"
        );
        assert_eq!(
            peer_ice.current_table_generation(TABLE).await.unwrap(),
            first,
            "both servers must begin on A"
        );

        // The only rows are in the coordinator's WAL buffer. This makes the
        // generation capture and the extra partial one atomic input to the
        // fan-out under test.
        let wal_dir = wal_root.join("default").join(TABLE);
        std::fs::create_dir_all(&wal_dir).unwrap();
        let mut wal = siglake_wal::WalWriter::with_thresholds(
            &wal_dir,
            "incarnation-race",
            10_000,
            std::time::Duration::from_secs(600),
        )
        .unwrap();
        wal.append_events(&race_events()).unwrap();
        wal.seal()
            .unwrap()
            .expect("sealed incarnation-race segment");
        drop(wal);

        let (peer_url, peer_task) =
            serve(AppState::new(peer_ice.clone(), crate::AuthConfig::open())).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind coordinator");
        let coordinator_url = format!("http://{}", listener.local_addr().unwrap());
        let hook = Arc::new(crate::DistributedDispatchHook::default());
        let mut state = AppState::new(coordinator_ice, crate::AuthConfig::open())
            .with_coordinator(vec![coordinator_url.clone(), peer_url], None)
            .with_wal_buffer_dir(Some(wal_root));
        state.distributed_dispatch_hook = Some(hook.clone());
        let coordinator_task =
            tokio::spawn(async move { axum::serve(listener, crate::router(state)).await.unwrap() });

        let request = {
            let url = coordinator_url.clone();
            tokio::spawn(async move {
                reqwest::Client::new()
                    .post(format!("{url}/api/v1/sql/distributed"))
                    .json(&serde_json::json!({
                        "query": format!(
                            "SELECT host, count(*) AS n FROM {TABLE} \
                             WHERE raw LIKE '%generation-race%' GROUP BY host"
                        )
                    }))
                    .send()
                    .await
                    .unwrap()
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), hook.captured.notified())
            .await
            .expect("query reached the post-WAL-capture boundary");

        // Replace A with a different mapping whose initial schema id is still
        // 0, then deterministically put the peer on B before dispatch resumes.
        assert!(writer.delete_index(TABLE).await.unwrap());
        let mut replacement_config = first_config;
        replacement_config.doc_mapping.field_mappings.push(
            siglake_core::index_config::FieldMapping {
                name: "replacement_only".to_string(),
                field_type: siglake_core::index_config::FieldType::Long,
                required: false,
            },
        );
        writer.create_index(&replacement_config).await.unwrap();
        let ident = writer.index_table_ident(TABLE);
        peer_ice.invalidate_cached_table(&ident).await;
        let replacement = peer_ice.current_table_generation(TABLE).await.unwrap();
        assert!(replacement.snapshot_id.is_none(), "B must be empty");
        assert_eq!(replacement.schema_id, first.schema_id, "both ids are 0");
        assert_ne!(replacement.table_uuid, first.table_uuid, "A and B differ");
        hook.resume.notify_one();

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), request)
            .await
            .expect("fan-out completed")
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "the B worker must refuse A rather than merge its schema with A's WAL partial"
        );
        let body: serde_json::Value = response.json().await.unwrap();
        let error = body["error"].as_str().unwrap_or_default();
        assert!(
            error.contains(crate::error::SHARD_PIN_UNRESOLVED_REASON),
            "the forwarded worker refusal must retain its typed reason: {body}"
        );
        assert!(
            error.contains(&first.table_uuid),
            "the forwarded worker refusal must name the captured A UUID: {body}"
        );

        coordinator_task.abort();
        peer_task.abort();
    }

    /// The managed index the widen-race fixture below moves. Underscored so
    /// the SQL needs no quoting.
    const WIDEN_INDEX: &str = "widen_race_idx";
    /// The column `update_index` appends mid-request.
    const WIDEN_COLUMN: &str = "severity_number";

    fn widen_index_config() -> IndexConfig {
        IndexConfig {
            index_id: WIDEN_INDEX.to_string(),
            doc_mapping: IndexConfig::builtin_events().doc_mapping,
            retention: None,
            index_at_flush: None,
        }
    }

    /// The same mapping with one appended nullable field — the only widen
    /// `validate_additive_update` accepts.
    fn widened_index_config() -> IndexConfig {
        let mut config = widen_index_config();
        config
            .doc_mapping
            .field_mappings
            .push(siglake_core::index_config::FieldMapping {
                name: WIDEN_COLUMN.to_string(),
                field_type: siglake_core::index_config::FieldType::Long,
                required: false,
            });
        config
    }

    fn widen_events(n: usize, host_of: impl Fn(usize) -> String) -> Vec<Event> {
        let base = Utc.with_ymd_and_hms(2026, 9, 8, 0, 0, 0).unwrap();
        (0..n)
            .map(|i| {
                let mut event = Event::now(format!("widen-race row {i}"));
                event.host = host_of(i);
                event.timestamp = base + Duration::seconds(i as i64);
                event
            })
            .collect()
    }

    /// Per-key counts of a `GROUP BY host` answer, sorted, plus their sum.
    fn per_host(body: &serde_json::Value) -> (Vec<(String, i64)>, i64) {
        let mut rows: Vec<(String, i64)> = body["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("rows array: {body}"))
            .iter()
            .map(|row| {
                (
                    row["host"].as_str().unwrap().to_string(),
                    row["n"].as_i64().unwrap(),
                )
            })
            .collect();
        rows.sort();
        let total = rows.iter().map(|(_, n)| *n).sum();
        (rows, total)
    }

    /// #2591: an `update_index` mapping widen that commits BETWEEN the
    /// coordinator's buffer capture and its fan-out.
    ///
    /// [`a_widened_managed_index_folds_its_buffer_into_the_pinned_fan_out`]
    /// (tests/query_server/distributed_e2e.rs) widens before the request
    /// starts, so capture and dispatch see one mapping. The window this drives
    /// is the other one: `capture_buffered_shard_read` has already fixed
    /// (snapshot, schema), `compute_buffer_partials` has already built the
    /// partial from that captured schema, and the widen lands before
    /// `pin_generation` is read. The contract is that the whole in-flight
    /// request serves the pre-widen mapping and the NEXT one serves the new
    /// mapping.
    ///
    /// Equal row counts cannot tell those apart — the widen adds no rows. The
    /// discriminator is peer B: it is frozen on the narrow generation, and
    /// `pinned_table_provider` refreshes it only when the pin names a
    /// generation its cache cannot reproduce. So B still narrow after the
    /// in-flight request means the pin carried the captured schema id, and B
    /// wide after the follow-up means the second request moved on.
    ///
    /// This is preventive coverage; it is not a reproduction of a shipped
    /// defect.
    #[tokio::test]
    async fn a_mapping_widen_between_capture_and_dispatch_serves_the_captured_generation() {
        const COMMITTED: usize = 12;
        const BUFFERED: usize = 3;

        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let wal_root = tmp.path().join("wal");

        // The writer owns catalog mutations; neither server's metadata cache
        // moves as a side effect of the create, the append or the widen.
        let writer = IcebergContext::open(&warehouse).await.unwrap();
        let narrow_config = widen_index_config();
        writer.create_index(&narrow_config).await.unwrap();
        let ident = writer.index_table_ident(WIDEN_INDEX);
        let committed = widen_events(COMMITTED, |i| format!("host-{}", i % 3));
        let batch = siglake_core::events_to_record_batch(&committed).unwrap();
        let mapped = siglake_core::map_carrier_batch(&batch, &narrow_config).unwrap();
        writer.append_to_table(&ident, mapped, &[]).await.unwrap();

        // A sealed, un-consumed segment: carrier-mapped through the NARROW
        // mapping, so the coordinator's partial is the half that was built
        // before the widen existed.
        let index_wal = wal_root.join("default").join(WIDEN_INDEX);
        std::fs::create_dir_all(&index_wal).unwrap();
        let mut wal = siglake_wal::WalWriter::with_thresholds(
            &index_wal,
            "widen-race",
            10_000,
            std::time::Duration::from_secs(600),
        )
        .unwrap();
        wal.append_events(&widen_events(BUFFERED, |_| "host-buffered".to_string()))
            .unwrap();
        wal.seal().unwrap().expect("sealed widen-race segment");
        drop(wal);

        let coordinator_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        // B never refreshes on its own, so every move of its generation is one
        // a pin made.
        let peer_ice = Arc::new(
            IcebergContext::open(&warehouse)
                .await
                .unwrap()
                .with_table_cache_ttl(std::time::Duration::from_secs(3600)),
        );
        let narrow = peer_ice
            .current_table_generation(WIDEN_INDEX)
            .await
            .unwrap();
        assert!(
            narrow.snapshot_id.is_some(),
            "the fixture needs committed rows: {narrow:?}"
        );

        let (peer_url, peer_task) =
            serve(AppState::new(peer_ice.clone(), crate::AuthConfig::open())).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind coordinator");
        let coordinator_url = format!("http://{}", listener.local_addr().unwrap());
        let hook = Arc::new(crate::DistributedDispatchHook::default());
        let mut state = AppState::new(coordinator_ice.clone(), crate::AuthConfig::open())
            .with_coordinator(vec![coordinator_url.clone(), peer_url], None)
            .with_wal_buffer_dir(Some(wal_root));
        state.distributed_dispatch_hook = Some(hook.clone());
        let app = crate::router(state);
        let coordinator_task =
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // The residual predicate forces fan-out (Tier-1 cannot answer it) and
        // GROUP BY makes the cross-shard merge visible per key. It names only
        // narrow columns — the widened one does not exist when this is planned.
        let client = reqwest::Client::new();
        let narrow_sql = format!(
            "SELECT host, count(*) AS n FROM {WIDEN_INDEX} \
             WHERE raw LIKE '%widen-race%' GROUP BY host"
        );
        let request = {
            let sql = narrow_sql.clone();
            let url = coordinator_url.clone();
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .post(format!("{url}/api/v1/sql"))
                    .json(&serde_json::json!({ "query": sql }))
                    .send()
                    .await
                    .unwrap()
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(10), hook.captured.notified())
            .await
            .expect("query reached the pre-dispatch boundary");

        // THE RACE: the mapping widens after the capture, and the coordinator's
        // serving metadata is refreshed onto it before the fan-out resumes.
        writer.update_index(&widened_index_config()).await.unwrap();
        coordinator_ice.invalidate_cached_table(&ident).await;
        let wide = coordinator_ice
            .current_table_generation(WIDEN_INDEX)
            .await
            .unwrap();
        assert_eq!(
            wide.snapshot_id, narrow.snapshot_id,
            "a mapping widen must be metadata-only; if `update_index` starts \
             committing data this fixture is no longer about the pin"
        );
        assert_ne!(
            wide.schema_id, narrow.schema_id,
            "the mid-request widen must move the schema id"
        );
        hook.resume.notify_one();

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), request)
            .await
            .expect("in-flight request completed")
            .unwrap();
        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(status, reqwest::StatusCode::OK, "{body}");
        assert_eq!(
            body["stats"]["phases"]["distributed"]["shard_wall_micros"]
                .as_array()
                .map(Vec::len),
            Some(2),
            "fixture must genuinely fan out to two peers: {body}"
        );
        let (rows, total) = per_host(&body);
        assert_eq!(
            rows,
            vec![
                ("host-0".to_string(), 4),
                ("host-1".to_string(), 4),
                ("host-2".to_string(), 4),
                ("host-buffered".to_string(), BUFFERED as i64),
            ],
            "committed shards plus the coordinator's buffer partial, per key: {body}"
        );
        assert_eq!(
            total,
            (COMMITTED + BUFFERED) as i64,
            "the per-key counts must sum to the whole row count: {body}"
        );

        // The evidence that the answer above is the CAPTURED generation: B was
        // frozen narrow, and a pin it can serve from its own cache never
        // refreshes it. Had the coordinator pinned the post-widen schema id, B
        // would have had to reload to resolve it — with the same row counts.
        assert_eq!(
            peer_ice
                .current_table_generation(WIDEN_INDEX)
                .await
                .unwrap(),
            narrow,
            "the in-flight fan-out must have pinned the pre-widen generation"
        );

        // The next request is the one that sees the widen: the column resolves,
        // the narrow-written buffered rows read null through it, and B refreshes
        // onto the wide pin.
        let wide_sql = format!(
            "SELECT host, count(*) AS n FROM {WIDEN_INDEX} \
             WHERE raw LIKE '%widen-race%' AND {WIDEN_COLUMN} IS NULL GROUP BY host"
        );
        // The hook is still installed and fires on every distributed request.
        // `notify_one` leaves a permit, so this one walks straight through it.
        hook.resume.notify_one();
        let response = client
            .post(format!("{coordinator_url}/api/v1/sql"))
            .json(&serde_json::json!({ "query": wide_sql }))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: serde_json::Value = response.json().await.unwrap();
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "the follow-up must plan against the widened mapping: {body}"
        );
        assert_eq!(
            body["stats"]["phases"]["distributed"]["shard_wall_micros"]
                .as_array()
                .map(Vec::len),
            Some(2),
            "the follow-up must fan out too: {body}"
        );
        let (wide_rows, wide_total) = per_host(&body);
        assert_eq!(
            rows, wide_rows,
            "every row is null through the widen: {body}"
        );
        assert_eq!(wide_total, (COMMITTED + BUFFERED) as i64, "{body}");
        assert_eq!(
            peer_ice
                .current_table_generation(WIDEN_INDEX)
                .await
                .unwrap(),
            wide,
            "the follow-up must have pinned the post-widen generation"
        );

        coordinator_task.abort();
        peer_task.abort();
    }
}

/// #967: membership CHURN under a running query.
///
/// The pinned-snapshot contract only means anything if a publish that lands
/// mid-query cannot reach the query already in flight. This drives exactly
/// that: a query captures `N=2`, the directory publishes a three-member
/// membership before either shard response completes, and the answer must
/// still be the two-shard partition of the file set — no shard omitted, none
/// served twice. The next query then observes the three-member snapshot, which
/// is the whole point of discovery (a KEDA replica becomes eligible without a
/// rollout).
#[cfg(test)]
mod peer_snapshot_churn_tests {
    use super::*;
    use crate::discovery::{PeerDirectory, PeerSource};
    use chrono::{Duration, TimeZone, Utc};
    use siglake_core::Event;
    use siglake_storage::iceberg::IcebergContext;
    use std::sync::Arc;

    async fn serve_peer(ice: Arc<IcebergContext>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback peer");
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = crate::router(AppState::new(ice, crate::AuthConfig::open()));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, task)
    }

    /// `GROUP BY` over a residual predicate: Tier-1 cannot answer it, so it
    /// genuinely fans out, and both a duplicated and an omitted shard show up
    /// in the per-key sum.
    const SQL: &str = "SELECT host, count(*) AS n FROM events \
                       WHERE raw LIKE '%churn%' GROUP BY host";

    fn churn_events() -> Vec<Event> {
        let base = Utc.with_ymd_and_hms(2026, 9, 7, 0, 0, 0).unwrap();
        (0..24)
            .map(|i| {
                let mut event = Event::now(format!("churn row {i}"));
                event.host = format!("host-{}", i % 4);
                event.timestamp = base + Duration::seconds(i);
                event
            })
            .collect()
    }

    async fn total(client: &reqwest::Client, url: &str) -> (i64, serde_json::Value) {
        let response = client
            .post(format!("{url}/api/v1/sql"))
            .json(&serde_json::json!({ "query": SQL }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = response.json().await.unwrap();
        let sum = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["n"].as_i64().unwrap())
            .sum();
        (sum, body)
    }

    #[tokio::test]
    async fn a_peer_joining_mid_query_changes_neither_the_shard_count_nor_the_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = tmp.path().join("warehouse");
        let writer = IcebergContext::open(&warehouse).await.unwrap();
        let events = churn_events();
        // Several commits ⇒ several files, so a two-way and a three-way split
        // are genuinely different partitions.
        for chunk in events.chunks(6) {
            writer.append_events(chunk).await.unwrap();
        }

        let coordinator_ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
        let (peer_one, peer_one_task) =
            serve_peer(Arc::new(IcebergContext::open(&warehouse).await.unwrap())).await;
        let (peer_two, peer_two_task) =
            serve_peer(Arc::new(IcebergContext::open(&warehouse).await.unwrap())).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind coordinator");
        let coordinator_url = format!("http://{}", listener.local_addr().unwrap());
        // Loopback peers all share the `127.0.0.1` host, so the SRV label
        // match cannot pick self out; publish the membership explicitly. The
        // normalization and self-match paths have their own unit gates.
        let directory = Arc::new(PeerDirectory::new("coordinator"));
        directory.publish(
            vec![coordinator_url.clone(), peer_one.clone()],
            coordinator_url.clone(),
        );
        let hook = Arc::new(crate::DistributedDispatchHook::default());
        let mut state = AppState::new(coordinator_ice, crate::AuthConfig::open())
            .with_peer_source(Some(PeerSource::from_directory(directory.clone())), None);
        state.distributed_dispatch_hook = Some(hook.clone());
        let app = crate::router(state);
        let coordinator_task =
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = reqwest::Client::new();
        let in_flight = {
            let client = client.clone();
            let url = coordinator_url.clone();
            tokio::spawn(async move { total(&client, &url).await })
        };

        // The hook sits after the WAL partial and before either shard request.
        tokio::time::timeout(std::time::Duration::from_secs(10), hook.captured.notified())
            .await
            .expect("query reached the pre-dispatch boundary");
        directory.publish(
            vec![coordinator_url.clone(), peer_one.clone(), peer_two.clone()],
            coordinator_url.clone(),
        );
        hook.resume.notify_one();

        let (sum, body) = tokio::time::timeout(std::time::Duration::from_secs(20), in_flight)
            .await
            .expect("in-flight query completed")
            .unwrap();
        let dist = &body["stats"]["phases"]["distributed"];
        assert_eq!(
            dist["shard_wall_micros"].as_array().map(Vec::len),
            Some(2),
            "the captured membership had two members, so exactly two shards \
             must have been dispatched: {body}"
        );
        assert_eq!(dist["peers"].as_u64(), Some(2), "{body}");
        assert_eq!(dist["peer_generation"].as_u64(), Some(1), "{body}");
        assert_eq!(
            sum,
            events.len() as i64,
            "a mid-query join must not duplicate or omit a shard: {body}"
        );

        // The NEXT query sees the larger membership — the reason discovery
        // exists. Same exact answer over three shards instead of two. Arm the
        // rendezvous first: this query passes the same pre-dispatch hook and
        // would otherwise block there until its wall clock expired.
        hook.resume.notify_one();
        let (next_sum, next_body) = total(&client, &coordinator_url).await;
        let next_dist = &next_body["stats"]["phases"]["distributed"];
        assert_eq!(next_dist["peers"].as_u64(), Some(3), "{next_body}");
        assert_eq!(
            next_dist["peer_generation"].as_u64(),
            Some(2),
            "{next_body}"
        );
        assert_eq!(
            next_dist["shard_wall_micros"].as_array().map(Vec::len),
            Some(3),
            "{next_body}"
        );
        assert_eq!(next_sum, events.len() as i64, "{next_body}");

        coordinator_task.abort();
        peer_one_task.abort();
        peer_two_task.abort();
    }
}

#[cfg(test)]
mod dist_browse_gate_tests {
    use super::*;

    /// The plain filtered browse -- no ORDER BY -- must be recognised. The
    /// ordered detector requires ORDER BY timestamp and so never saw it, which
    /// is why the distribution gate had nothing but the LIMIT to go on.
    #[test]
    fn detects_a_browse_without_an_order_by() {
        let shape = detect_dim_browse(
            "SELECT timestamp, raw FROM \"logs-bench\" WHERE region = 'probe' LIMIT 100",
            "timestamp",
        )
        .expect("plain filtered browse is a dimensional browse");
        assert_eq!(shape.table, "logs-bench");
        assert_eq!(shape.column, "region");
        assert_eq!(shape.values, vec!["probe".to_string()]);
        assert!(!shape.negated);
    }

    #[test]
    fn a_browse_without_a_limit_is_not_a_browse() {
        assert!(detect_dim_browse(
            "SELECT raw FROM \"logs-bench\" WHERE region = 'probe'",
            "timestamp"
        )
        .is_none());
    }

    /// Two dimensions, an OR, or a LIKE must NOT be treated as a known
    /// selectivity -- the gate then falls back to today's behaviour rather than
    /// distributing on a guess.
    #[test]
    fn ambiguous_predicates_are_refused() {
        for sql in [
            "SELECT raw FROM \"logs-bench\" WHERE region = 'a' AND host = 'b' LIMIT 100",
            "SELECT raw FROM \"logs-bench\" WHERE region = 'a' OR region = 'b' LIMIT 100",
            "SELECT raw FROM \"logs-bench\" WHERE raw LIKE '%x%' LIMIT 100",
        ] {
            assert!(
                detect_dim_browse(sql, "timestamp").is_none(),
                "should refuse: {sql}"
            );
        }
    }

    /// The arithmetic the gate turns on, on the two shapes that motivated it.
    /// 394M rows: region='us-east-2' matches 12.5% and sifts ~800 rows (15ms
    /// local); region='probe' matches 75 rows and sifts the whole table (54.7s
    /// and a breaker trip on one node).
    #[test]
    fn sift_estimate_separates_the_two_real_shapes() {
        let total = 393_866_085u64;
        let selective = expected_sift_rows(total, total / 8, 100); // 12.5%
        let rare = expected_sift_rows(total, 75, 100);
        assert!(
            selective < 1_000,
            "high-selectivity browse sifts ~800: {selective}"
        );
        assert!(
            rare > 100_000_000,
            "rare-value browse sifts most of the table: {rare}"
        );
        let min = 5_000_000u64;
        assert!(selective < min, "the fast browse must STAY LOCAL");
        assert!(rare >= min, "the pathological browse must DISTRIBUTE");
    }

    /// A predicate matching nothing is the worst case for one node: it reads
    /// everything and returns nothing.
    #[test]
    fn a_predicate_matching_nothing_sifts_the_whole_table() {
        assert_eq!(expected_sift_rows(1_000_000, 0, 100), 1_000_000);
    }

    /// The cap matters: without it a one-in-a-billion value would "estimate" a
    /// sift larger than the table, which is nonsense and would read as a
    /// stronger signal than it is.
    #[test]
    fn the_sift_estimate_is_capped_at_the_table() {
        assert_eq!(expected_sift_rows(1_000, 1, 10_000), 1_000);
    }

    /// The default is OFF until the distributed browse path dispatches
    /// reliably -- see the doc comment on dist_browse_min_scan_rows for the
    /// 1TB measurement that decided this.
    #[test]
    fn the_policy_is_off_by_default() {
        assert_eq!(
            dist_browse_min_scan_rows(),
            0,
            "default must stay OFF until distributed browses stop hanging"
        );
    }
}

#[cfg(test)]
mod ndjson_cancel_guard_tests {
    use super::*;

    /// The NDJSON body must carry the cancel guard, so the scan stops when the
    /// body is dropped -- a client hanging up mid-stream. The first version of
    /// the cancellation fix armed the guard in the HANDLER, which returns before
    /// a streaming body has sent anything, so it cancelled healthy requests
    /// (three e2e tests went 3 rows -> 0) and streaming queries ended up with no
    /// cancellation at all.
    #[test]
    fn dropping_the_ndjson_body_cancels_the_query() {
        let cancel = siglake_storage::QueryCancel::new();
        let stream = GuardedStream {
            _guard: None,
            _cancel: Some(siglake_storage::CancelOnDrop(cancel.clone())),
            inner: futures::stream::empty::<Result<Vec<u8>, std::io::Error>>(),
        };
        assert!(
            !cancel.is_cancelled(),
            "live while the body is still streaming"
        );
        drop(stream);
        assert!(
            cancel.is_cancelled(),
            "dropping the response body must cancel the query's scans"
        );
    }

    /// And a body with no guard must not panic or cancel anything -- the paths
    /// that pass `None` (dry runs, internal renders) keep working.
    #[test]
    fn a_body_without_a_guard_is_harmless() {
        let cancel = siglake_storage::QueryCancel::new();
        let stream = GuardedStream {
            _guard: None,
            _cancel: None,
            inner: futures::stream::empty::<Result<Vec<u8>, std::io::Error>>(),
        };
        drop(stream);
        assert!(!cancel.is_cancelled());
    }
}

#[cfg(test)]
mod batch_cancel_guard_tests {
    use super::*;

    #[test]
    fn batch_context_arms_query_cancel_until_execution_is_dropped() {
        let (ctx, guard) = cancellable_batch_context(
            crate::QueryScanConfig::default(),
            None,
            siglake_storage::QueryExecutionId::next(),
        );
        let planned_cancel = ctx
            .state()
            .config()
            .get_extension::<siglake_storage::QueryCancel>()
            .expect("batch context omitted QueryCancel");
        assert!(!planned_cancel.is_cancelled());

        drop(guard);
        assert!(planned_cancel.is_cancelled());
    }
}

/// One wall-clock budget for the whole batch RUN.
///
/// The tier's asynchronous timeout used to wrap only the collect, so table
/// registration, planning, estimation and the metadata fast paths ran unbounded
/// — and then the collect started on a FRESH budget — while the job held its
/// admission reservation throughout.
#[cfg(test)]
mod batch_run_deadline_tests {
    use super::*;

    use std::time::Duration;

    use crate::TierLimits;

    use chrono::Utc;
    use siglake_core::Event;

    async fn warehouse(
        events: usize,
    ) -> (
        tempfile::TempDir,
        Arc<siglake_storage::iceberg::IcebergContext>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = siglake_storage::iceberg::IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        let rows: Vec<Event> = (0..events)
            .map(|i| Event {
                timestamp: Utc::now(),
                host: format!("host-{i}"),
                source: "batch-deadline".into(),
                sourcetype: "app:json".into(),
                index: "main".into(),
                raw: format!("event {i}"),
                attributes: None,
            })
            .collect();
        ice.append_events(&rows).await.unwrap();
        (tmp, Arc::new(ice))
    }

    fn batch_limits(request: crate::limits::RequestLimits) -> crate::limits::ResolvedLimits {
        crate::limits::ResolvedLimits::resolve(
            &request,
            Priority::Batch,
            &TierLimits::batch_defaults(),
        )
    }

    fn count_query() -> RewrittenQuery {
        RewrittenQuery {
            sql: "SELECT count(*) AS n FROM events".to_string(),
            preferred_scan_order: None,
            ordered_limit: None,
            clipped_limit: None,
        }
    }

    /// Run the batch query while holding the cancel flag its scans read, so the
    /// test can see what the outcome released.
    async fn run_watching_cancel(
        ice: Arc<siglake_storage::iceberg::IcebergContext>,
        limits: crate::limits::ResolvedLimits,
    ) -> (BatchOutcome, siglake_storage::QueryCancel) {
        let (ctx, guard) = cancellable_batch_context(
            crate::QueryScanConfig::default(),
            None,
            siglake_storage::QueryExecutionId::next(),
        );
        let cancel = siglake_storage::QueryCancel::clone(
            &ctx.state()
                .config()
                .get_extension::<siglake_storage::QueryCancel>()
                .expect("batch context omitted QueryCancel"),
        );
        let deadline = BatchDeadline::for_run(&limits);
        let outcome = run_batch_query_in(
            ice,
            count_query(),
            ctx,
            guard,
            limits,
            AllowApproximate::always(),
            None,
            deadline,
        )
        .await;
        (outcome, cancel)
    }

    /// Preparation spends from the same clock as the collect. Virtual time, so
    /// the split between the two phases is exact rather than raced.
    #[tokio::test(start_paused = true)]
    async fn preparation_and_collection_draw_on_one_budget() {
        let deadline = BatchDeadline::starting_now(Duration::from_secs(10), true);

        // "Preparation": most of the run's budget, and it completes.
        assert_eq!(
            deadline
                .run(tokio::time::sleep(Duration::from_secs(9)))
                .await,
            Ok(())
        );
        // The collect gets what is LEFT — one second, not another ten.
        assert_eq!(
            deadline
                .run(tokio::time::sleep(Duration::from_secs(5)))
                .await,
            Err(BudgetSpent)
        );
        assert_eq!(
            tokio::time::Instant::now().duration_since(deadline.deadline),
            Duration::ZERO,
            "the collect ran past the run's deadline"
        );

        // And a spent budget refuses the next phase WITHOUT polling it: the
        // metadata fast paths are immediately ready, and Tokio polls an inner
        // future before its timer.
        let polled = std::cell::Cell::new(false);
        let outcome = deadline
            .run(async {
                polled.set(true);
            })
            .await;
        assert_eq!(outcome, Err(BudgetSpent));
        assert!(!polled.get(), "a spent budget still polled the next phase");
    }

    /// `circuit_breakers: false` lifts the wall clock and nothing else — the
    /// other ceilings are enforced by the phases themselves (see
    /// `the_opt_out_keeps_the_bytes_ceiling` below).
    #[tokio::test(start_paused = true)]
    async fn the_opt_out_lifts_the_wall_clock() {
        let deadline = BatchDeadline::starting_now(Duration::ZERO, false);
        assert!(!deadline.expired());
        assert_eq!(
            deadline
                .run(tokio::time::sleep(Duration::from_secs(6 * 3600)))
                .await,
            Ok(())
        );
    }

    /// A zero-budget job whose answer comes from the footers, in ONE poll,
    /// without ever awaiting anything the old collect-only wrapper could cut.
    /// The absent cost is the evidence that the refusal landed in preparation:
    /// `estimate()` never ran, so the verdict has nothing to report.
    #[tokio::test]
    async fn a_zero_budget_fast_path_job_times_out_and_releases_its_scan() {
        let (_tmp, ice) = warehouse(4).await;
        let (outcome, cancel) = run_watching_cancel(
            ice,
            batch_limits(crate::limits::RequestLimits {
                timeout_seconds: Some(0),
                ..Default::default()
            }),
        )
        .await;
        match outcome {
            BatchOutcome::Timeout(cost) => assert!(
                cost.is_none(),
                "a spent budget still reached estimation: {cost:?}"
            ),
            other => panic!("zero-budget batch run did not time out: {other:?}"),
        }
        assert!(
            cancel.is_cancelled(),
            "the timed-out run left its scan pumps armed"
        );
    }

    /// Control for the two tests above: with a budget, the same job answers.
    #[tokio::test]
    async fn a_budgeted_fast_path_job_still_answers() {
        let (_tmp, ice) = warehouse(4).await;
        let (outcome, cancel) = run_watching_cancel(ice, batch_limits(Default::default())).await;
        match outcome {
            BatchOutcome::Ok(body, _) => assert_eq!(body.row_count, 1),
            other => panic!("budgeted batch run did not answer: {other:?}"),
        }
        assert!(
            cancel.is_cancelled(),
            "the finished run kept its scan armed"
        );
    }

    /// The explicit opt-out is a wall-clock opt-out. With the timeout waived,
    /// the pre-flight bytes ceiling must still refuse the job.
    #[tokio::test]
    async fn the_opt_out_keeps_the_bytes_ceiling() {
        let (_tmp, ice) = warehouse(4).await;
        let (outcome, _) = run_watching_cancel(
            ice,
            batch_limits(crate::limits::RequestLimits {
                timeout_seconds: Some(0),
                circuit_breakers: Some(false),
                max_bytes_scanned: Some(1),
                ..Default::default()
            }),
        )
        .await;
        match outcome {
            BatchOutcome::Failed(msg, cost) => {
                assert!(
                    msg.contains("exceeds batch limit"),
                    "opt-out job failed for the wrong reason: {msg}"
                );
                let cost = cost.expect("a refusal that read the estimate carries it");
                assert!(
                    cost.estimated_bytes_scanned > 1,
                    "the refusing estimate is not the one reported: {cost:?}"
                );
            }
            other => panic!("the bytes ceiling was lifted with the clock: {other:?}"),
        }
    }
}

/// What a batch job's row says about it WHILE it runs.
///
/// THE DEFECT THESE GUARD. The lifecycle was published from the completed
/// branches of the spawned future: a job that executed for an hour read
/// `pending` for that hour, `started_at` recorded the moment execution *ended*,
/// and a run that failed after planning never published the estimate the jobs
/// API promises "once planning has run" at all.
///
/// The interesting states only exist while a run is in flight, so the
/// execution here is a future the test holds open: no sleeps, no polling for a
/// window to appear, and the assertions run at a point the test chose.
#[cfg(test)]
mod batch_lifecycle_tests {
    use super::*;

    use std::time::Duration;

    use crate::jobs::JobStore;

    fn identity() -> CallerIdentity {
        CallerIdentity {
            subject: "lifecycle-test".into(),
            email: None,
            tenant: None,
            coordinator: false,
        }
    }

    fn cost() -> CostReport {
        CostReport {
            files_to_scan: Some(1),
            files_considered: Some(1),
            estimated_bytes_scanned: 4096,
            estimated_rows_processed: 10,
            estimated_runtime_seconds: 0.1,
            complexity_class: crate::cost::ComplexityClass::Small,
            warnings: vec![],
            exact: true,
        }
    }

    fn records() -> crate::format::RecordsResponse {
        crate::format::RecordsResponse {
            columns: vec!["n".to_string()],
            row_count: 1,
            rows: serde_json::json!([{ "n": 1 }]),
            truncated: false,
            max_rows: None,
            cost: None,
            stats: None,
            approximation: None,
        }
    }

    async fn submitted_job() -> (Arc<JobStore>, crate::jobs::JobId) {
        let jobs = Arc::new(JobStore::new(1, Duration::from_secs(60)));
        let id = jobs
            .submit(
                "SELECT count(*) AS n FROM events".into(),
                Priority::Batch,
                None,
            )
            .await
            .expect("submit");
        (jobs, id)
    }

    fn reporting<'a>(
        lifecycle: &'a JobLifecycle,
        identity: &'a CallerIdentity,
    ) -> BatchJobReporting<'a> {
        BatchJobReporting {
            lifecycle,
            audit: None,
            identity,
            query_sql: "SELECT count(*) AS n FROM events",
            // A budget these tests never spend: they are about what the row
            // says, not about the clock (see `batch_publication_deadline_tests`
            // for the clock).
            deadline: BatchDeadline::for_run(&crate::limits::ResolvedLimits::resolve(
                &Default::default(),
                Priority::Batch,
                &crate::TierLimits::batch_defaults(),
            )),
        }
    }

    /// Queue time belongs to the submission-to-start interval exposed by the
    /// jobs API, not to the execution interval bounded by `BatchDeadline` and
    /// reported in `query_audit.duration_ms`. The future stays unpolled for an
    /// exact virtual-time interval, just as it does while queued on the batch
    /// runtime; its deadline and audit clock must both begin on its first poll.
    #[tokio::test(start_paused = true)]
    async fn a_queued_job_audits_only_the_run_interval() {
        const QUEUE_TIME: Duration = Duration::from_secs(30);
        const RUN_TIME: Duration = Duration::from_secs(7);

        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let submitted = tokio::time::Instant::now();

        // Constructing an async block does not poll it. This is the batch
        // runtime's queue: the run-start clock below does not exist yet.
        let queued_run = async {
            let deadline = BatchDeadline::starting_now(Duration::from_secs(60), true);
            drive_batch_job(
                BatchJobReporting {
                    lifecycle: &lifecycle,
                    audit: Some(&audit),
                    identity: &identity,
                    query_sql: "SELECT count(*) AS n FROM events",
                    deadline,
                },
                async {
                    tokio::time::sleep(RUN_TIME).await;
                    BatchOutcome::Ok(records(), cost())
                },
            )
            .await;
        };

        tokio::time::sleep(QUEUE_TIME).await;
        queued_run.await;

        assert_eq!(submitted.elapsed(), QUEUE_TIME + RUN_TIME);
        let audit_row = audited.recv().await.expect("the completed run was audited");
        assert_eq!(audit_row.status, AuditStatus::Succeeded);
        assert_eq!(
            audit_row.duration_ms,
            RUN_TIME.as_millis() as i64,
            "the audit duration included time before the run was first polled"
        );
    }

    /// A run held open mid-execution: the row must say `running`, with the time
    /// the work started, and the completion must not move that start backwards
    /// or forwards.
    #[tokio::test]
    async fn a_held_run_is_observable_as_running_with_the_time_it_started() {
        let (jobs, id) = submitted_job().await;
        let pending = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(pending.status, JobStatus::Pending);
        assert!(
            pending.started_at.is_none(),
            "a submitted job has not started"
        );

        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (executing_tx, executing_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let run = async move {
            executing_tx.send(()).expect("signal execution");
            release_rx.await.expect("release");
            BatchOutcome::Ok(records(), cost())
        };

        let observe = async {
            executing_rx.await.expect("execution started");
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(
                row.status,
                JobStatus::Running,
                "a job whose query is executing must not read pending"
            );
            let started_at = row.started_at.expect("a running job records its start");
            assert!(
                started_at >= row.submitted_at,
                "start {started_at} precedes submission {}",
                row.submitted_at
            );
            assert_eq!(row.ended_at, None, "a running job has not ended");
            release_tx.send(()).expect("release the run");
            started_at
        };

        let (_, started_at) = tokio::join!(
            drive_batch_job(reporting(&lifecycle, &identity), run),
            observe
        );

        let done = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(done.status, JobStatus::Succeeded);
        assert_eq!(
            done.started_at,
            Some(started_at),
            "the completion rewrote the start timestamp"
        );
        assert!(done.ended_at.expect("ended_at") >= started_at);
    }

    /// One event, so `estimate()` has real footers to read and the bytes
    /// ceiling below has something above 1 byte to refuse.
    async fn one_event_warehouse() -> (
        tempfile::TempDir,
        Arc<siglake_storage::iceberg::IcebergContext>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = siglake_storage::iceberg::IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap();
        ice.append_events(&[siglake_core::Event {
            timestamp: chrono::Utc::now(),
            host: "host-0".into(),
            source: "lifecycle".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "event".into(),
            attributes: None,
        }])
        .await
        .unwrap();
        (tmp, Arc::new(ice))
    }

    /// The bytes ceiling refuses this: `max_bytes_scanned` of 1 is below any
    /// real estimate, and the refusal happens after planning published it.
    fn refusing_limits() -> crate::limits::ResolvedLimits {
        crate::limits::ResolvedLimits::resolve(
            &crate::limits::RequestLimits {
                max_bytes_scanned: Some(1),
                ..Default::default()
            },
            Priority::Batch,
            &crate::TierLimits::batch_defaults(),
        )
    }

    const LIFECYCLE_SQL: &str = "SELECT count(*) AS n FROM events";

    fn lifecycle_query() -> RewrittenQuery {
        RewrittenQuery {
            sql: LIFECYCLE_SQL.to_string(),
            preferred_scan_order: None,
            ordered_limit: None,
            clipped_limit: None,
        }
    }

    /// The estimate is published where planning happens, so a job refused by
    /// the pre-flight bytes ceiling — which reads that very estimate — carries
    /// the number that refused it. Before the fix the row's `cost` stayed null
    /// for every failure, including this one.
    #[tokio::test]
    async fn planning_publishes_the_estimate_that_refused_the_job() {
        let (_tmp, ice) = one_event_warehouse().await;
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let _ = lifecycle.publish_running().await;

        let limits = refusing_limits();
        let deadline = BatchDeadline::for_run(&limits);
        let outcome = run_batch_query(
            ice,
            lifecycle_query(),
            crate::QueryScanConfig::default(),
            limits,
            AllowApproximate::always(),
            Some(&lifecycle),
            deadline,
        )
        .await;
        match outcome {
            BatchOutcome::Failed(msg, cost) => {
                assert!(msg.contains("exceeds batch limit"), "wrong refusal: {msg}");
                assert!(
                    cost.is_some(),
                    "the refusal read the estimate, so its verdict must carry it"
                );
            }
            other => panic!("the bytes ceiling did not refuse the job: {other:?}"),
        }
        let row = jobs.info(id).await.expect("job store").expect("row");
        assert!(
            row.cost.is_some(),
            "planning ran, so the estimate must be readable: {row:?}"
        );
        assert!(row.started_at.is_some(), "the run started");
    }

    /// THE DEFECT THIS GUARDS. The estimate reached the job row (above) but not
    /// the audit row: `BatchOutcome::Failed` carried only a message, so every
    /// post-estimate refusal wrote `query_audit.estimated_bytes_scanned` null
    /// while `GET /api/v1/jobs/<id>` served the number for the same run. The
    /// two answers about one run must be the same answer, so the audit row is
    /// asserted against the job row's own cost rather than a literal.
    #[tokio::test]
    async fn a_refusal_after_planning_audits_the_cost_the_job_row_carries() {
        let (_tmp, ice) = one_event_warehouse().await;
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let limits = refusing_limits();
        let deadline = BatchDeadline::for_run(&limits);

        drive_batch_job(
            BatchJobReporting {
                lifecycle: &lifecycle,
                audit: Some(&audit),
                identity: &identity,
                query_sql: LIFECYCLE_SQL,
                deadline,
            },
            run_batch_query(
                ice,
                lifecycle_query(),
                crate::QueryScanConfig::default(),
                limits,
                AllowApproximate::always(),
                Some(&lifecycle),
                deadline,
            ),
        )
        .await;

        let row = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(row.status, JobStatus::Failed);
        let cost = row
            .cost
            .expect("planning ran, so the job row carries the estimate");

        let audit_row = audited.recv().await.expect("the refusal was audited");
        assert_eq!(audit_row.status, AuditStatus::Failed);
        assert_eq!(audit_row.priority, Priority::Batch);
        assert!(
            audit_row
                .error
                .as_deref()
                .is_some_and(|e| e.contains("exceeds batch limit")),
            "audited the wrong refusal: {:?}",
            audit_row.error
        );
        assert_eq!(
            audit_row.estimated_bytes_scanned,
            Some(cost.estimated_bytes_scanned),
            "the audit row disagrees with the job row about the same run"
        );
        assert_eq!(
            audit_row.estimated_rows_processed,
            Some(cost.estimated_rows_processed)
        );
        assert_eq!(audit_row.complexity, Some(cost.complexity_class));
    }

    /// The control, and the one case that keeps a null cost: a run that failed
    /// before `estimate()` returned has nothing to report and must not invent
    /// a number — the job row is null there too.
    #[tokio::test]
    async fn a_failure_before_estimation_audits_no_cost() {
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();

        drive_batch_job(
            BatchJobReporting {
                audit: Some(&audit),
                ..reporting(&lifecycle, &identity)
            },
            async { BatchOutcome::Failed("plan SQL: no such table".into(), None) },
        )
        .await;

        assert!(
            jobs.info(id)
                .await
                .expect("job store")
                .expect("row")
                .cost
                .is_none(),
            "a plan failure never estimated anything"
        );
        let audit_row = audited.recv().await.expect("the failure was audited");
        assert_eq!(audit_row.status, AuditStatus::Failed);
        assert_eq!(audit_row.complexity, None);
        assert_eq!(audit_row.estimated_bytes_scanned, None);
        assert_eq!(audit_row.estimated_rows_processed, None);
    }

    /// THE SAME DEFECT, THE OTHER WAY ROUND. A timeout taken before
    /// `estimate()` returned used to audit
    /// `CostReport::unknown_after_timeout()` — zero bytes, zero rows,
    /// complexity `huge` — while the job row's cost stayed null, because
    /// nothing had called `record_cost`. The audit reader saw a number no
    /// estimate produced, and `query_audit` has no `warnings` column to carry
    /// the sentinel's "cost was not estimated" caveat. A pre-estimate timeout
    /// now says what a pre-estimate failure says, with a null.
    ///
    /// The budget is spent by the RUN, not by the driver: the run is given a
    /// zero-budget deadline so its cut is exact (`register_tables` is refused
    /// before it is polled), while the driver's own deadline is healthy, so the
    /// start publication and the terminal write both land and the job row this
    /// is compared against is a real one. The post-estimate half is
    /// `deadline::a_wedged_estimate_publication_times_the_job_out_before_the_fast_paths`,
    /// which asserts the audit row still carries the estimate.
    #[tokio::test]
    async fn a_timeout_before_estimation_audits_no_cost() {
        let (_tmp, ice) = one_event_warehouse().await;
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let spent = BatchDeadline::starting_now(Duration::ZERO, true);

        drive_batch_job(
            BatchJobReporting {
                audit: Some(&audit),
                ..reporting(&lifecycle, &identity)
            },
            run_batch_query(
                ice,
                lifecycle_query(),
                crate::QueryScanConfig::default(),
                refusing_limits(),
                AllowApproximate::always(),
                Some(&lifecycle),
                spent,
            ),
        )
        .await;

        let row = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(row.status, JobStatus::Timeout);
        assert!(
            row.cost.is_none(),
            "the run was cut before `estimate` returned, so the row has no cost: {:?}",
            row.cost
        );

        let audit_row = audited.recv().await.expect("the timeout was audited");
        assert_eq!(audit_row.status, AuditStatus::Timeout);
        assert_eq!(audit_row.priority, Priority::Batch);
        // The three columns `GET /api/v1/jobs/<id>` answers null for.
        assert_eq!(audit_row.complexity, None);
        assert_eq!(audit_row.estimated_bytes_scanned, None);
        assert_eq!(audit_row.estimated_rows_processed, None);
    }

    /// A cancellation that lands before the run publishes anything stays the
    /// job's verdict: the lifecycle writes carry the same
    /// `status IN ('pending', 'running')` guard as the terminal ones, so
    /// neither the start nor the estimate can reopen a terminal row.
    #[tokio::test]
    async fn a_cancelled_job_is_not_revived_by_the_run_that_was_executing_it() {
        let (jobs, id) = submitted_job().await;
        assert!(jobs.cancel(id).await.unwrap(), "cancel the pending job");

        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let admission =
            crate::admission::AdmissionController::new(1 << 20, 1, Duration::from_millis(1));
        let reservation = admission
            .acquire(admission.batch_reservation_bytes())
            .await
            .expect("reserve this run's batch share");
        let polled = std::cell::Cell::new(false);
        {
            let _reservation = reservation;
            drive_batch_job(
                BatchJobReporting {
                    audit: Some(&audit),
                    ..reporting(&lifecycle, &identity)
                },
                async {
                    polled.set(true);
                    BatchOutcome::Ok(records(), cost())
                },
            )
            .await;
        }
        assert!(
            !polled.get(),
            "a cancelled job still polled its query future"
        );
        assert!(
            admission.acquire(1 << 20).await.is_ok(),
            "a refused start kept its admission reservation"
        );
        lifecycle.publish_cost(&cost()).await;

        let row = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(row.status, JobStatus::Cancelled);
        assert_eq!(row.started_at, None, "a cancelled job never started");
        assert!(row.cost.is_none(), "a cancelled job took no estimate");
        assert!(
            jobs.result(id)
                .await
                .expect("job store")
                .expect("row")
                .body
                .is_none(),
            "the discarded result must not be readable"
        );
        let audit_row = audited.recv().await.expect("the refused start was audited");
        assert_eq!(audit_row.status, AuditStatus::Cancelled);
        assert_eq!(audit_row.complexity, None);
        assert_eq!(audit_row.estimated_bytes_scanned, None);
        assert_eq!(audit_row.estimated_rows_processed, None);
        assert!(
            audit_row
                .error
                .as_deref()
                .is_some_and(|e| e.contains("execution not started")),
            "the audit did not distinguish a refused start: {audit_row:?}"
        );
    }

    /// Recovery can condemn the row between submission and the batch worker's
    /// first poll. That confirmed disposition stops execution without
    /// manufacturing a failed completion.
    #[tokio::test]
    async fn recovery_refusing_the_start_does_not_poll_the_query() {
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let polled = std::cell::Cell::new(false);
        let admission =
            crate::admission::AdmissionController::new(1 << 20, 1, Duration::from_millis(1));
        let reservation = admission
            .acquire(admission.batch_reservation_bytes())
            .await
            .expect("reserve this run's batch share");

        {
            let _reservation = reservation;
            drive_batch_job_after_running_publication(
                BatchJobReporting {
                    audit: Some(&audit),
                    ..reporting(&lifecycle, &identity)
                },
                async {
                    polled.set(true);
                    BatchOutcome::Failed("must not exist".into(), None)
                },
                Ok(Ok(CompletionOutcome::Superseded {
                    status: JobStatus::Failed,
                    owned_by_us: true,
                    recovered: true,
                })),
            )
            .await;
        }

        assert!(!polled.get(), "a recovery-refused start polled the query");
        assert!(
            admission.acquire(1 << 20).await.is_ok(),
            "a recovery-refused start kept its admission reservation"
        );
        assert_eq!(
            jobs.info(id).await.expect("job store").expect("row").status,
            JobStatus::Pending,
            "the refused start manufactured a terminal write"
        );
        let audit_row = audited.recv().await.expect("the refused start was audited");
        assert_eq!(audit_row.status, AuditStatus::Failed);
        assert_eq!(audit_row.estimated_bytes_scanned, None);
        assert!(audit_row
            .error
            .as_deref()
            .is_some_and(|e| e.contains("execution not started")));
    }

    /// A row gone before the worker starts has no terminal state to write. It
    /// still gets the existing failed audit mapping, with no invented cost.
    #[tokio::test]
    async fn disappearance_refusing_the_start_does_not_poll_the_query() {
        let jobs = Arc::new(JobStore::new(1, Duration::from_secs(60)));
        let lifecycle = JobLifecycle {
            jobs,
            id: crate::jobs::JobId::new(),
        };
        let identity = identity();
        let (audit, mut audited) = AuditWriter::for_test();
        let polled = std::cell::Cell::new(false);
        let admission =
            crate::admission::AdmissionController::new(1 << 20, 1, Duration::from_millis(1));
        let reservation = admission
            .acquire(admission.batch_reservation_bytes())
            .await
            .expect("reserve this run's batch share");

        {
            let _reservation = reservation;
            drive_batch_job(
                BatchJobReporting {
                    audit: Some(&audit),
                    ..reporting(&lifecycle, &identity)
                },
                async {
                    polled.set(true);
                    BatchOutcome::Ok(records(), cost())
                },
            )
            .await;
        }

        assert!(!polled.get(), "a vanished row still polled the query");
        assert!(
            admission.acquire(1 << 20).await.is_ok(),
            "a vanished start kept its admission reservation"
        );
        let audit_row = audited
            .recv()
            .await
            .expect("the vanished start was audited");
        assert_eq!(audit_row.status, AuditStatus::Failed);
        assert_eq!(audit_row.complexity, None);
        assert!(audit_row
            .error
            .as_deref()
            .is_some_and(|e| e.ends_with("job already gone")));
    }

    /// A failed publication is not a store verdict. Abandoning on an error
    /// could strand a live pending row, so the query and terminal write remain
    /// the availability-preserving control.
    #[tokio::test]
    async fn a_running_publication_error_still_executes_and_finishes() {
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        let polled = std::cell::Cell::new(false);

        drive_batch_job_after_running_publication(
            reporting(&lifecycle, &identity),
            async {
                polled.set(true);
                BatchOutcome::Ok(records(), cost())
            },
            Ok(Err(anyhow::anyhow!("store unavailable"))),
        )
        .await;

        assert!(
            polled.get(),
            "a persistence error was treated as supersession"
        );
        assert_eq!(
            jobs.info(id).await.expect("job store").expect("row").status,
            JobStatus::Succeeded
        );
    }

    /// Defensive control: an unexpected applied status is a refused start,
    /// not the oversized-result branch (there was no result to size).
    #[test]
    fn an_unexpected_applied_start_uses_the_generic_conflict_cause() {
        assert_eq!(
            completion_conflict_cause(CompletionOutcome::Applied {
                status: JobStatus::Failed,
            }),
            "other"
        );
    }

    /// One refused start emits one conflict sample and one audit row. It must
    /// not fall through into terminal reporting, which would duplicate both.
    #[test]
    fn a_recovery_refused_start_reports_counter_and_audit_exactly_once() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let (audit, mut audited) = AuditWriter::for_test();
        let identity = identity();
        metrics::with_local_recorder(&recorder, || {
            report_refused_batch_start(
                Some(&audit),
                &identity,
                LIFECYCLE_SQL,
                7,
                CompletionOutcome::Superseded {
                    status: JobStatus::Failed,
                    owned_by_us: true,
                    recovered: true,
                },
            );
        });

        let count = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, value)| {
                key.key().name() == "siglake_query_job_terminal_conflict_total"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "attempted" && l.value() == "running")
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "actual" && l.value() == "failed")
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "cause" && l.value() == "recovery")
                    && matches!(value, DebugValue::Counter(1))
            })
            .count();
        assert_eq!(count, 1, "the refused start counter was not exact");
        let audit_row = audited.try_recv().expect("one audit row");
        assert_eq!(audit_row.status, AuditStatus::Failed);
        assert_eq!(audit_row.duration_ms, 7);
        assert_eq!(audit_row.estimated_bytes_scanned, None);
        assert!(
            audited.try_recv().is_err(),
            "the refused start audited twice"
        );
    }

    /// A preparation failure is terminal at the moment it is written, and the
    /// run that wrote it publishes nothing afterwards — a late start or
    /// estimate would put a finished job back into `running`.
    #[tokio::test]
    async fn a_failed_job_stays_failed_when_the_run_publishes_late() {
        let (jobs, id) = submitted_job().await;
        let lifecycle = JobLifecycle {
            jobs: jobs.clone(),
            id,
        };
        let identity = identity();
        drive_batch_job(reporting(&lifecycle, &identity), async {
            BatchOutcome::Failed("plan SQL: no such table".into(), None)
        })
        .await;
        let failed = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(failed.status, JobStatus::Failed);
        assert!(
            failed.started_at.is_some(),
            "the job executed before it failed"
        );

        let _ = lifecycle.publish_running().await;
        lifecycle.publish_cost(&cost()).await;
        let after = jobs.info(id).await.expect("job store").expect("row");
        assert_eq!(after.status, JobStatus::Failed);
        assert_eq!(after.started_at, failed.started_at);
        assert!(after.cost.is_none(), "a terminal row took a late estimate");
    }

    /// A store that has stopped answering must not hold admitted work past its
    /// execution budget.
    ///
    /// THE DEFECT THESE GUARD. The lifecycle publications #1848 added are
    /// writes to the job store, shared by every query replica and, with
    /// `query.jobs.persistent`, the catalog Postgres. They were both OUTSIDE
    /// the run's deadline: `publish_running` preceded the budget's creation
    /// entirely, and `publish_cost` was awaited between two wrapped phases. A
    /// blocked `UPDATE` therefore held a job that had been admitted — a quarter
    /// of the pod's admission budget and a batch-runtime thread — for as long
    /// as the connection waited, no matter what execution budget the caller
    /// asked for. The budget bounds the RUN, not the part of it that happens to
    /// be query work.
    ///
    /// The wedge is the store's own lock, held by the test: a publication that
    /// never returns, at a point the test chose, with no sleeps and no polling
    /// for a window to appear. Virtual time, so the cut is exact.
    mod deadline {
        use super::*;

        use crate::admission::AdmissionController;

        /// The run's budget in these tests. Nothing here spends real time; the
        /// value only has to be distinguishable from the release below.
        const RUN_BUDGET: Duration = Duration::from_secs(30);

        /// Long after the budget is spent, so the store starts answering again
        /// only once the deadline has had its say. Under virtual time this is
        /// the second of exactly two events.
        const UNWEDGE_AFTER: Duration = Duration::from_secs(300);

        fn ordinary_limits() -> crate::limits::ResolvedLimits {
            crate::limits::ResolvedLimits::resolve(
                &Default::default(),
                Priority::Batch,
                &crate::TierLimits::batch_defaults(),
            )
        }

        /// A pod-sized admission budget, with this job's fixed batch share
        /// already reserved — the reservation the spawned run holds for its
        /// whole life.
        async fn admitted() -> (AdmissionController, crate::admission::AdmissionGuard) {
            let admission = AdmissionController::new(1 << 20, 1, Duration::from_millis(1));
            let guard = admission
                .acquire(admission.batch_reservation_bytes())
                .await
                .expect("an empty budget admits the first batch job");
            (admission, guard)
        }

        /// The whole budget is free again, so the run released its reservation
        /// by returning rather than waiting on the store.
        async fn assert_admission_released(admission: &AdmissionController) {
            assert!(
                admission.acquire(1 << 20).await.is_ok(),
                "the timed-out run is still holding its admission reservation"
            );
        }

        /// The start publication is the run's FIRST act, so a store that never
        /// answers it must cut the run before any query work is done at all —
        /// and with nothing planned there is no estimate for the audit row to
        /// carry, which is the driver's own half of
        /// `a_timeout_before_estimation_audits_no_cost`.
        #[tokio::test(start_paused = true)]
        async fn a_wedged_start_publication_times_the_job_out_before_it_executes() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (audit, mut audited) = AuditWriter::for_test();
            let (admission, reservation) = admitted().await;
            let wedged = jobs.wedge_for_test().await;

            let executed = std::cell::Cell::new(false);
            let run = async {
                executed.set(true);
                BatchOutcome::Ok(records(), cost())
            };
            let drive = async {
                // Held exactly as the spawned future holds it.
                let _reservation = reservation;
                drive_batch_job(
                    BatchJobReporting {
                        audit: Some(&audit),
                        deadline: BatchDeadline::starting_now(RUN_BUDGET, true),
                        ..reporting(&lifecycle, &identity)
                    },
                    run,
                )
                .await;
            };
            let unwedge = async {
                tokio::time::sleep(UNWEDGE_AFTER).await;
                drop(wedged);
            };
            tokio::join!(drive, unwedge);

            assert!(
                !executed.get(),
                "the run executed its query after its budget was already spent"
            );
            // The audited STATUS is `failed`, not `timeout`: this wedge also
            // outlasts the terminal write, so the row's verdict is the
            // deferral's, not the one the run computed (`terminal_report`). The
            // cost is the run's own measurement either way — and there is none.
            let audit_row = audited.recv().await.expect("the cut run was audited");
            assert_eq!(
                (
                    audit_row.complexity,
                    audit_row.estimated_bytes_scanned,
                    audit_row.estimated_rows_processed
                ),
                (None, None, None),
                "a run cut before it registered a table audited an estimate"
            );
            // The terminal write is bounded by its own clock now (#1942), and
            // this wedge outlasts it: the run hands the verdict to its
            // executor's reconciliation pass and RETURNS, which is what
            // releases the reservation. The row is still non-terminal here.
            assert_admission_released(&admission).await;
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert!(
                !row.status.is_terminal(),
                "the wedged terminal write somehow landed: {:?}",
                row.status
            );
            assert_eq!(
                row.started_at, None,
                "the publication was not cut, only delayed"
            );

            // The store answers again (the wedge above was dropped), so the
            // executor's own pass resolves the job it knows finished.
            let outcome = jobs.reconcile_finished_jobs().await;
            assert_eq!(
                outcome,
                crate::jobs::ReconcileOutcome {
                    installed: 1,
                    ..Default::default()
                }
            );
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(
                row.status,
                JobStatus::Timeout,
                "a run cut in its start publication is a timeout like any other"
            );
            assert!(row.ended_at.is_some(), "the timeout was not persisted");
            assert!(
                row.error
                    .expect("a reconciled row says why")
                    .contains("timeout"),
                "the reconciled row must name the verdict its run computed"
            );
        }

        /// The estimate publication sits between planning and the fast paths,
        /// so a store that never answers it must cut the run there — with the
        /// estimate the run already computed, and without answering from the
        /// footers a moment later.
        ///
        /// REAL time, unlike its neighbours: this run needs a real warehouse,
        /// and a virtual clock jumps the moment the catalog's connection pool is
        /// awaited, which times the run out in preparation before it can reach
        /// the publication at all. The budget is therefore measured rather than
        /// guessed — the control below runs the same query against the same
        /// warehouse with the store answering, and the wedged run gets five
        /// times what that took. What the run is cut in is not left to the
        /// margin either: the audit row carries the estimate, which only exists
        /// if planning finished.
        #[tokio::test]
        async fn a_wedged_estimate_publication_times_the_job_out_before_the_fast_paths() {
            let (_tmp, ice) = one_event_warehouse().await;
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (audit, mut audited) = AuditWriter::for_test();
            let (admission, reservation) = admitted().await;
            // The start lands while the store still answers: this test is about
            // the SECOND publication.
            let _ = lifecycle.publish_running().await;

            let control_started = Instant::now();
            let control = run_batch_query(
                ice.clone(),
                lifecycle_query(),
                crate::QueryScanConfig::default(),
                ordinary_limits(),
                AllowApproximate::always(),
                None,
                BatchDeadline::starting_now(RUN_BUDGET, true),
            )
            .await;
            assert!(
                matches!(control, BatchOutcome::Ok(..)),
                "the control run did not answer: {control:?}"
            );
            let budget = (control_started.elapsed() * 5).max(Duration::from_millis(100));

            let deadline = BatchDeadline::starting_now(budget, true);
            // Wedged from the run's FIRST POLL, so the start publication the
            // driver makes before it lands unblocked and this test is about the
            // estimate alone — and unwedged when the run returns, so the
            // terminal write below is a healthy one.
            let run = async {
                let _wedged = jobs.wedge_for_test().await;
                run_batch_query(
                    ice,
                    lifecycle_query(),
                    crate::QueryScanConfig::default(),
                    ordinary_limits(),
                    AllowApproximate::always(),
                    Some(&lifecycle),
                    deadline,
                )
                .await
            };
            {
                let _reservation = reservation;
                // An unbounded publication against this wedge never returns, and
                // the wedge is released by the run's own frame: without the
                // deadline the join below is a deadlock, so it is given an
                // outer bound that says which one it was.
                tokio::time::timeout(
                    budget * 20,
                    drive_batch_job(
                        BatchJobReporting {
                            audit: Some(&audit),
                            deadline,
                            ..reporting(&lifecycle, &identity)
                        },
                        run,
                    ),
                )
                .await
                .expect("the wedged estimate publication was never cut by the run's deadline");
            }

            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(
                row.status,
                JobStatus::Timeout,
                "the count fast path answered a job whose budget was spent"
            );
            assert!(
                row.cost.is_none(),
                "the publication was not cut, only delayed: {:?}",
                row.cost
            );
            assert!(
                jobs.result(id)
                    .await
                    .expect("job store")
                    .expect("row")
                    .body
                    .is_none(),
                "a timed-out job served a result"
            );
            let audit_row = audited.recv().await.expect("the timeout was audited");
            assert_eq!(audit_row.status, AuditStatus::Timeout);
            assert!(
                audit_row.estimated_bytes_scanned.is_some_and(|b| b > 0),
                "the run was cut before planning, not at the publication: {audit_row:?}"
            );
            assert_admission_released(&admission).await;
        }

        /// The wall-clock opt-out is a WALL-CLOCK opt-out: a job that asked for
        /// no circuit breakers waits for its publications, as it waits for
        /// everything else, and then runs.
        #[tokio::test(start_paused = true)]
        async fn the_opt_out_waits_for_a_wedged_publication() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let wedged = jobs.wedge_for_test().await;

            let executed = std::cell::Cell::new(false);
            let run = async {
                executed.set(true);
                BatchOutcome::Ok(records(), cost())
            };
            let drive = drive_batch_job(
                BatchJobReporting {
                    deadline: BatchDeadline::starting_now(RUN_BUDGET, false),
                    ..reporting(&lifecycle, &identity)
                },
                run,
            );
            let unwedge = async {
                tokio::time::sleep(UNWEDGE_AFTER).await;
                drop(wedged);
            };
            tokio::join!(drive, unwedge);

            assert!(executed.get(), "the opt-out job was cut by the wall clock");
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(row.status, JobStatus::Succeeded);
            assert!(
                row.started_at.is_some(),
                "the publication the opt-out waited for never landed"
            );
        }

        /// A store that stops answering the TERMINAL write must not hold
        /// admitted work either.
        ///
        /// THE DEFECT THIS GUARDS. That write was deliberately unbounded, on
        /// the grounds that a completion nobody persisted is what recovery
        /// cleans up. Recovery preserves a row owned by a LIVE replica, so the
        /// unbounded write held this run's admission reservation and a
        /// batch-runtime thread for the length of the outage and then left a
        /// `running` row that no sweep would ever resolve. The write is
        /// bounded now and the verdict is handed to the executor's own
        /// reconciliation pass, which is what resolves the client's job.
        #[tokio::test(start_paused = true)]
        async fn a_wedged_terminal_write_releases_admission_and_defers_the_verdict() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (admission, reservation) = admitted().await;

            // The wedge is taken by the RUN, so the start publication lands
            // while the store still answers and this test is about the write
            // that ends the job. It leaves the run's frame through the channel,
            // so it outlives the run exactly as an outage would.
            let (wedge_tx, wedge_rx) = tokio::sync::oneshot::channel();
            let wedging = jobs.clone();
            let run = async move {
                wedge_tx.send(wedging.wedge_for_test().await).ok();
                BatchOutcome::Ok(records(), cost())
            };
            {
                let _reservation = reservation;
                drive_batch_job(
                    BatchJobReporting {
                        deadline: BatchDeadline::starting_now(RUN_BUDGET, true),
                        ..reporting(&lifecycle, &identity)
                    },
                    run,
                )
                .await;
            }
            assert_admission_released(&admission).await;

            // Still wedged: a pass cannot resolve anything, and must not wedge
            // the loop waiting for one row either.
            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                crate::jobs::ReconcileOutcome {
                    retained: 1,
                    ..Default::default()
                },
                "a stalled store must leave the id parked, not settled"
            );

            let wedged = wedge_rx.await.expect("the run took the wedge");
            drop(wedged);
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert!(
                !row.status.is_terminal(),
                "the wedged terminal write somehow landed: {:?}",
                row.status
            );
            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                crate::jobs::ReconcileOutcome {
                    installed: 1,
                    ..Default::default()
                },
                "the executor never resolved the job it knows finished"
            );
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(row.status, JobStatus::Failed);
            assert!(row
                .error
                .expect("a reconciled row says why")
                .contains("resubmit"));
            assert!(
                jobs.result(id)
                    .await
                    .expect("job store")
                    .expect("row")
                    .body
                    .is_none(),
                "the discarded result was served by reconciliation"
            );
        }
    }

    /// A finished run whose terminal write FAILS rather than stalling, and
    /// what its executor does about it afterwards.
    mod reconciliation {
        use super::*;

        use crate::jobs::{ReconcileOutcome, WriteFault};

        /// Well past [`TERMINAL_PERSIST_BUDGET`], so a bound would have had
        /// its say long before the store answers again.
        const UNWEDGE_AFTER: Duration = Duration::from_secs(300);

        /// Every attempt failing is the case the whole mechanism exists for:
        /// the run says what it could not do, and the job is resolved later by
        /// the one process that knows its execution ended.
        #[tokio::test(start_paused = true)]
        async fn every_attempt_failing_defers_the_verdict_and_says_so() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (audit, mut audited) = AuditWriter::for_test();
            jobs.fail_next_writes(WriteFault::Refused, TERMINAL_PERSIST_ATTEMPTS);

            drive_batch_job(
                BatchJobReporting {
                    audit: Some(&audit),
                    ..reporting(&lifecycle, &identity)
                },
                async { BatchOutcome::Ok(records(), cost()) },
            )
            .await;

            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(
                row.status,
                JobStatus::Running,
                "nothing installed a terminal state, so the row still reads running"
            );
            let audit_row = audited.recv().await.expect("the run audited itself");
            assert_eq!(audit_row.status, AuditStatus::Failed);
            assert!(
                audit_row
                    .error
                    .as_deref()
                    .is_some_and(|e| e.contains("could not be persisted")),
                "the audit row must not claim an outcome the store never took: {audit_row:?}"
            );

            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                ReconcileOutcome {
                    installed: 1,
                    ..Default::default()
                }
            );
            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(row.status, JobStatus::Failed);
            assert!(row
                .error
                .expect("a reconciled row says why")
                .contains("resubmit"));
            assert!(
                jobs.result(id)
                    .await
                    .expect("job store")
                    .expect("row")
                    .body
                    .is_none(),
                "reconciliation served a result body it never had"
            );
        }

        /// Why the attempts are bounded rather than one: a failover blip costs
        /// a retry, and the retry keeps the RESULT — which reconciliation,
        /// holding no bodies, cannot.
        #[tokio::test(start_paused = true)]
        async fn a_retry_that_lands_keeps_the_run_s_own_result() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            jobs.fail_next_writes(WriteFault::Refused, 1);

            drive_batch_job(reporting(&lifecycle, &identity), async {
                BatchOutcome::Ok(records(), cost())
            })
            .await;

            assert_eq!(
                jobs.info(id).await.expect("job store").expect("row").status,
                JobStatus::Succeeded
            );
            assert_eq!(
                jobs.result(id)
                    .await
                    .expect("job store")
                    .expect("row")
                    .body
                    .expect("the retry stored the run's result")
                    .rows,
                serde_json::json!([{ "n": 1 }])
            );
            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                ReconcileOutcome::default(),
                "a job whose write landed must not be parked for reconciliation"
            );
        }

        /// A write can fail AFTER the store applied it — the acknowledgement is
        /// lost — and the retry then meets its own predecessor. That is not a
        /// race with anybody: the run must report the verdict it computed, with
        /// its own error text, and count no terminal conflict.
        #[tokio::test(start_paused = true)]
        async fn an_ambiguously_acknowledged_write_is_not_a_conflict() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (audit, mut audited) = AuditWriter::for_test();
            jobs.fail_next_writes(WriteFault::Ambiguous, 1);

            drive_batch_job(
                BatchJobReporting {
                    audit: Some(&audit),
                    ..reporting(&lifecycle, &identity)
                },
                async { BatchOutcome::Failed("boom".into(), Some(cost())) },
            )
            .await;

            let row = jobs.info(id).await.expect("job store").expect("row");
            assert_eq!(row.status, JobStatus::Failed);
            assert_eq!(
                row.error.as_deref(),
                Some("boom"),
                "the write that landed carried the run's own error"
            );
            let audit_row = audited.recv().await.expect("the run audited itself");
            assert_eq!(audit_row.status, AuditStatus::Failed);
            assert_eq!(
                audit_row.error.as_deref(),
                Some("boom"),
                "the retry reported its own write as a lost race: {audit_row:?}"
            );
            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                ReconcileOutcome::default(),
                "a job whose write did land must not be parked"
            );
        }

        /// The wall-clock opt-out lifts the persistence bound as it lifts every
        /// other bound: a job that asked for no circuit breakers waits for its
        /// terminal write, and keeps its result.
        #[tokio::test(start_paused = true)]
        async fn the_opt_out_waits_for_the_terminal_write_too() {
            let (jobs, id) = submitted_job().await;
            let lifecycle = JobLifecycle {
                jobs: jobs.clone(),
                id,
            };
            let identity = identity();
            let (wedge_tx, wedge_rx) = tokio::sync::oneshot::channel();
            let wedging = jobs.clone();
            let run = async move {
                wedge_tx.send(wedging.wedge_for_test().await).ok();
                BatchOutcome::Ok(records(), cost())
            };
            let drive = drive_batch_job(
                BatchJobReporting {
                    deadline: BatchDeadline::starting_now(UNWEDGE_AFTER, false),
                    ..reporting(&lifecycle, &identity)
                },
                run,
            );
            let unwedge = async {
                let wedged = wedge_rx.await.expect("the run took the wedge");
                tokio::time::sleep(UNWEDGE_AFTER).await;
                drop(wedged);
            };
            tokio::join!(drive, unwedge);

            assert_eq!(
                jobs.info(id).await.expect("job store").expect("row").status,
                JobStatus::Succeeded,
                "the opt-out job was cut by the persistence bound"
            );
            assert_eq!(
                jobs.reconcile_finished_jobs().await,
                ReconcileOutcome::default()
            );
        }
    }
}

/// What a finished batch run is allowed to say about itself.
///
/// THE DEFECT THIS GUARDS. The batch path used to report the outcome it
/// COMPUTED: `let _ = jobs.finish_succeeded(..)` and then a
/// `siglake_query_jobs_total{outcome="succeeded"}` increment and a
/// `Succeeded` audit row. Every terminal write is conditional, so a
/// cancellation persisted through another replica (or recovery, or the
/// oversize-result path) leaves the row reading something else entirely and
/// the fleet disagreeing with itself: the metric says the job produced an
/// answer, the audit trail says a query ran to completion, and
/// `GET /api/v1/jobs/<id>` says it was cancelled.
#[cfg(test)]
mod terminal_report_tests {
    use super::*;

    /// The ordinary path: the store installed exactly this verdict, so the run
    /// counts it and audits it, with no conflict series at all.
    #[test]
    fn an_installed_verdict_is_reported_as_computed() {
        for (attempted, status, audit) in [
            ("succeeded", JobStatus::Succeeded, AuditStatus::Succeeded),
            ("failed", JobStatus::Failed, AuditStatus::Failed),
            ("timeout", JobStatus::Timeout, AuditStatus::Timeout),
        ] {
            assert_eq!(
                terminal_report(
                    attempted,
                    TerminalPersisted::Installed(CompletionOutcome::Applied { status })
                ),
                TerminalReport {
                    outcome: Some(attempted),
                    conflict_actual: None,
                    conflict_cause: None,
                    superseded: false,
                    audit,
                },
                "{attempted}"
            );
        }
    }

    /// A completion that lost to a cancellation — the cross-replica case —
    /// counts NO job outcome and audits the store's verdict, not its own.
    #[test]
    fn a_superseded_completion_claims_no_outcome() {
        let report = terminal_report(
            "succeeded",
            TerminalPersisted::Installed(CompletionOutcome::Superseded {
                status: JobStatus::Cancelled,
                owned_by_us: true,
                recovered: false,
            }),
        );
        assert_eq!(
            report,
            TerminalReport {
                outcome: None,
                conflict_actual: Some("cancelled"),
                conflict_cause: Some("cancellation"),
                superseded: true,
                audit: AuditStatus::Cancelled,
            }
        );
        assert_eq!(
            report.error("succeeded"),
            "batch completion (succeeded) refused: job already cancelled"
        );
    }

    /// Recovery condemning the row while the query ran is the same shape with
    /// a different `actual`, and it must not become a `failed` outcome count
    /// either: this run installed nothing.
    #[test]
    fn a_completion_superseded_by_recovery_claims_no_outcome() {
        assert_eq!(
            terminal_report(
                "succeeded",
                TerminalPersisted::Installed(CompletionOutcome::Superseded {
                    status: JobStatus::Failed,
                    owned_by_us: true,
                    recovered: true,
                })
            ),
            TerminalReport {
                outcome: None,
                conflict_actual: Some("failed"),
                conflict_cause: Some("recovery"),
                superseded: true,
                audit: AuditStatus::Failed,
            }
        );
    }

    /// The TTL sweep deleted the row before the query finished. Nothing to
    /// report about, and `gone` says which of the two it was.
    #[test]
    fn a_vanished_row_claims_no_outcome() {
        let report = terminal_report(
            "succeeded",
            TerminalPersisted::Installed(CompletionOutcome::Vanished),
        );
        assert_eq!(
            report,
            TerminalReport {
                outcome: None,
                conflict_actual: Some("gone"),
                conflict_cause: Some("gone"),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        );
        assert_eq!(
            report.error("succeeded"),
            "batch completion (succeeded) refused: job already gone"
        );
    }

    /// Accepted, but as something ELSE: a success body over the row's inline
    /// cap is installed as `failed` by `finish_succeeded` itself. The run
    /// finished, so *something* is countable — but it is `failed`, which is
    /// what the client will read, and never `succeeded`.
    #[test]
    fn an_oversize_result_counts_the_failure_it_installed() {
        let report = terminal_report(
            "succeeded",
            TerminalPersisted::Installed(CompletionOutcome::Applied {
                status: JobStatus::Failed,
            }),
        );
        assert_eq!(
            report,
            TerminalReport {
                outcome: Some("failed"),
                conflict_actual: Some("failed"),
                conflict_cause: Some("result_too_large"),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        );
        assert_eq!(
            report.error("succeeded"),
            "batch completion (succeeded) recorded as failed"
        );
    }

    /// Every attempt at the write failed. Nothing is known to have been
    /// installed, so no outcome is claimed — least of all a success — and
    /// `unknown` keeps a broken store distinguishable from a lost race. The
    /// cause names who resolves the row, because with the store unreachable
    /// the client's job is not resolved by this report at all.
    #[test]
    fn a_deferred_terminal_write_claims_no_outcome_and_names_its_reconciler() {
        let report = terminal_report("succeeded", TerminalPersisted::Deferred);
        assert_eq!(
            report,
            TerminalReport {
                outcome: None,
                conflict_actual: Some("unknown"),
                conflict_cause: Some("write_deferred"),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        );
        assert_eq!(
            report.error("succeeded"),
            "batch completion (succeeded) could not be persisted; \
             its executor is reconciling the job row"
        );
    }

    /// The same, minus the promise: reconciliation bookkeeping was full, so
    /// nothing on this replica is tracking the job and the audit row must not
    /// claim otherwise.
    #[test]
    fn an_abandoned_terminal_write_promises_no_reconciliation() {
        let report = terminal_report("timeout", TerminalPersisted::Abandoned);
        assert_eq!(
            report,
            TerminalReport {
                outcome: None,
                conflict_actual: Some("unknown"),
                conflict_cause: Some("write_abandoned"),
                superseded: false,
                audit: AuditStatus::Failed,
            }
        );
        assert_eq!(
            report.error("timeout"),
            "batch completion (timeout) could not be persisted and could not be \
             tracked for reconciliation"
        );
    }

    /// #1951: `SiglakeBatchRowStrandedNonTerminal` reads this counter through
    /// `increase()`, so every series it selects must exist at 0 before the
    /// first abandonment — and that first increment is the one that matters,
    /// because a store outage deep enough to fill the 1024-id bookkeeping
    /// usually strands a row once and then the pod is restarted.
    ///
    /// `check-chart.py` cannot hold this pair together: the increment site
    /// records `attempted`/`actual`/`cause` from variables, so its "every
    /// literal label set the code records is pre-registered" rule sees nothing
    /// here. This test is that rule for the abandoned series.
    #[test]
    fn every_abandoned_write_series_the_run_records_is_preregistered() {
        let baseline: Vec<&[(&str, &str)]> = siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
            .iter()
            .filter(|c| c.name == "siglake_query_job_terminal_conflict_total")
            .flat_map(|c| c.series.iter().copied())
            .collect();
        // The verdicts a batch run can compute for itself. `cancelled` is
        // installed by the DELETE path, never by a run's own terminal write,
        // so it is not a series this counter can carry with these causes.
        for computed in [JobStatus::Succeeded, JobStatus::Failed, JobStatus::Timeout] {
            let attempted = computed.label();
            let report = terminal_report(attempted, TerminalPersisted::Abandoned);
            let recorded: &[(&str, &str)] = &[
                ("attempted", attempted),
                (
                    "actual",
                    report
                        .conflict_actual
                        .expect("an abandoned write conflicts"),
                ),
                (
                    "cause",
                    report.conflict_cause.expect("an abandoned write names why"),
                ),
            ];
            assert!(
                baseline.contains(&recorded),
                "siglake_query_job_terminal_conflict_total{recorded:?} is recorded by a run \
                 whose terminal write was abandoned, but QUERY_SERVER_ALERTED_COUNTERS does not \
                 create it at 0, so SiglakeBatchRowStrandedNonTerminal would miss its first \
                 increment on a fresh pod"
            );
        }
    }

    /// The recovery alert reads this series through `increase()`. A recovery
    /// that wins before execution increments it once on a fresh pod, so it
    /// needs the same zero baseline as refused completions.
    #[test]
    fn the_recovery_refused_start_series_is_preregistered() {
        let recorded: &[(&str, &str)] = &[
            ("attempted", "running"),
            ("actual", "failed"),
            ("cause", "recovery"),
        ];
        assert!(
            siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
                .iter()
                .filter(|c| c.name == "siglake_query_job_terminal_conflict_total")
                .flat_map(|c| c.series.iter().copied())
                .any(|series| series == recorded),
            "the recovery-refused start must exist at 0 before its first increment"
        );
    }

    /// The other half of that alert, and the one a rename would silently
    /// blank: the counter the bookkeeping increments when it refuses an id.
    #[test]
    fn the_reconciliation_overflow_counter_is_preregistered() {
        assert!(
            siglake_core::metrics::QUERY_SERVER_ALERTED_COUNTERS
                .iter()
                .any(
                    |c| c.name == "siglake_query_jobs_unreconciled_dropped_total"
                        && c.series == siglake_core::metrics::UNLABELLED
                ),
            "siglake_query_jobs_unreconciled_dropped_total must exist at 0 on a fresh query \
             pod: the cap it counts is usually reached once per outage, and \
             SiglakeBatchRowStrandedNonTerminal reads it through increase()"
        );
    }
}
