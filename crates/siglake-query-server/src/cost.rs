//! Query cost estimation.
//!
//! For a parsed SQL statement over `events` or a managed index, compute
//! a conservative upper bound on the work the query will do —
//! bytes/rows scanned, files touched, rough runtime — and bucket it
//! into a complexity class. Used by:
//!
//! - `POST /api/v1/sql/explain` — dedicated cost endpoint.
//! - `POST /api/v1/sql` with `dry_run: true` — cost without
//!   execution.
//! - Every successful execution — cost is attached to the response so
//!   dashboards can track per-query effort.
//! - The circuit-breaker pre-flight (chunk 3) — reject upfront if the
//!   estimate exceeds the limit.
//!
//! The numbers come from DataFusion's physical-plan statistics, which
//! in turn come from Iceberg manifest stats. No data-file reads, no S3
//! fetches: `/explain` is cheap and side-effect-free.
//!
//! ## Where the numbers come from
//!
//! `iceberg-datafusion 0.9` doesn't surface manifest byte/row
//! statistics through `ExecutionPlan::partition_statistics()` (the
//! call returns `Precision::Absent`). We therefore bypass DataFusion
//! and read manifests directly via [`IcebergContext::scan_cost`],
//! which walks `TableScan::plan_files()` and sums
//! `file_size_in_bytes` plus `record_count` per surviving
//! FileScanTask. The scan builder pushes the time-range predicate
//! (when present) down to manifest-level pruning, so a tight `WHERE
//! timestamp BETWEEN ...` clause shrinks the estimate without
//! reading any data files.

use std::sync::Arc;

use arrow_array::types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType};
use arrow_schema::{DataType, TimeUnit};
use chrono::{DateTime, Days, Months, TimeDelta, TimeZone, Utc};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{scalar::ScalarValue, DFSchema};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{ExprSchemable, JoinType, LogicalPlan, Operator};
use datafusion::prelude::Expr;
use serde::Serialize;

use siglake_storage::iceberg::{IcebergContext, ScanCost, TimeBounds};

/// Bytes-per-second-per-core that we assume zstd-compressed Parquet
/// decode achieves. Conservative; real numbers depend on column types
/// and CPU. Used only for the runtime estimate, never for breaker
/// enforcement (which uses actual bytes/rows).
pub const TARGET_DECODE_BPS: f64 = 50.0 * 1024.0 * 1024.0;

/// Cost report returned by `/explain` and attached to every successful
/// query response.
#[derive(Debug, Serialize, serde::Deserialize, Clone, utoipa::ToSchema)]
pub struct CostReport {
    pub files_to_scan: Option<usize>,
    /// Live files considered BEFORE the manifest time-bounds prune —
    /// `files_considered - files_to_scan` is planning-time pruning's win.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_considered: Option<usize>,
    pub estimated_bytes_scanned: u64,
    pub estimated_rows_processed: u64,
    pub estimated_runtime_seconds: f64,
    pub complexity_class: ComplexityClass,
    pub warnings: Vec<String>,
    /// `true` when the underlying statistics are exact; `false` when
    /// they fell back to a heuristic.
    pub exact: bool,
}

impl CostReport {
    /// The report for a query that timed out before its cost was ever
    /// estimated. Deliberately NOT a `Default`: the zero value would say
    /// "small, exact, no bytes", which is the opposite of what is known about a
    /// query that ran until the breaker fired.
    ///
    /// The INTERACTIVE path's value, where it is carried in an HTTP body whose
    /// `warnings` say so. The batch tier does not use it: `query_audit` has no
    /// `warnings` column, so a batch timeout taken before `estimate()` returned
    /// reports no cost at all (`BatchOutcome::Timeout(None)`), as its failures
    /// do, rather than a zero the audit reader would read as a measurement.
    pub fn unknown_after_timeout() -> Self {
        Self {
            files_to_scan: None,
            files_considered: None,
            estimated_bytes_scanned: 0,
            estimated_rows_processed: 0,
            estimated_runtime_seconds: 0.0,
            complexity_class: ComplexityClass::Huge,
            warnings: vec!["cost was not estimated: the query timed out".to_string()],
            exact: false,
        }
    }
}

#[derive(Debug, Serialize, serde::Deserialize, Copy, Clone, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ComplexityClass {
    Small,
    Medium,
    Large,
    Huge,
}

impl ComplexityClass {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::Huge => "huge",
        }
    }

    fn classify(bytes: u64, rows: u64) -> Self {
        const MB: u64 = 1024 * 1024;
        const GB: u64 = 1024 * MB;
        if bytes <= 100 * MB && rows <= 1_000_000 {
            Self::Small
        } else if bytes <= 10 * GB && rows <= 100_000_000 {
            Self::Medium
        } else if bytes <= 100 * GB && rows <= 1_000_000_000 {
            Self::Large
        } else {
            Self::Huge
        }
    }
}

/// Estimate the cost of running `df` without executing it.
///
/// For each Iceberg-backed table the logical plan touches, walk the
/// manifests (via [`IcebergContext::scan_cost`]) with the extracted
/// time-range predicate applied, and sum the byte/row counts. Annotate
/// with anti-pattern warnings (no time predicate, cross joins).
pub async fn estimate(df: &DataFrame, ice: &Arc<IcebergContext>) -> anyhow::Result<CostReport> {
    let logical = df.logical_plan().clone();

    let scanned = scanned_tables(&logical);
    let time_bounds = extract_time_bounds(&logical);
    let warnings = collect_warnings(&logical);

    let mut total = ScanCost {
        exact: true,
        ..Default::default()
    };
    // MANAGED INDEXES ARE SIGLAKE TABLES TOO. `is_siglake_table` lists only the
    // built-in `events` table, so a query against a user index matched nothing,
    // `had_known_scan` stayed false, and the whole report came back
    // `files_to_scan: 0, estimated_bytes_scanned: 0, complexity_class: small`
    // -- for scans that really read hundreds of gigabytes.
    //
    // Measured 2026-08-24: a full-table keyword search over 2B rows in
    // `logs-bench` timed out at 60s having been priced "small" with 0 bytes.
    // Everything downstream of the estimate is therefore inert on user indexes:
    // pre-flight cost rejection cannot fire, admission reserves nothing, and the
    // 504 body misreports what the query was doing. Every bench suite queries a
    // user index, so this was the measured path in every round.
    //
    // Same shape as the 2026-07-11 finding where the distribution gate knew
    // `events` and not user indexes.
    //
    // Resolved LAZILY: a query touching only built-ins pays nothing for this.
    let mut index_time_columns: Option<std::collections::HashMap<String, String>> = None;
    let mut had_known_scan = false;
    for table in &scanned {
        let time_col = if is_siglake_table(table) {
            Some("timestamp".to_string())
        } else {
            match &index_time_columns {
                Some(columns) => columns.get(table).cloned(),
                None => {
                    let columns: std::collections::HashMap<String, String> =
                        match ice.list_indexes().await {
                            Ok(cfgs) => cfgs
                                .into_iter()
                                .map(|c| (c.index_id, c.doc_mapping.timestamp_field))
                                .collect(),
                            Err(e) => {
                                tracing::debug!(error = %e, "cost: list_indexes failed");
                                Default::default()
                            }
                        };
                    let time_col = columns.get(table).cloned();
                    index_time_columns = Some(columns);
                    time_col
                }
            }
        };
        let Some(time_col) = time_col else { continue };
        had_known_scan = true;
        match ice.scan_cost(table, &time_col, time_bounds).await {
            Ok(c) => total = total.add(&c),
            Err(e) => {
                tracing::debug!(table, error = %e, "scan_cost failed; estimate may be incomplete");
                total.exact = false;
            }
        }
    }
    if !had_known_scan {
        total.exact = false;
    }

    let runtime = total.bytes as f64 / TARGET_DECODE_BPS;
    let complexity = ComplexityClass::classify(total.bytes, total.rows);

    Ok(CostReport {
        files_to_scan: Some(total.files),
        files_considered: had_known_scan.then_some(total.files_considered),
        estimated_bytes_scanned: total.bytes,
        estimated_rows_processed: total.rows,
        estimated_runtime_seconds: runtime,
        complexity_class: complexity,
        warnings,
        exact: total.exact,
    })
}

/// Does the plan carry a row-limiting `LIMIT`?
///
/// This decides WHICH bound governs the query, and the distinction is the whole
/// reason the byte gate misfires.
///
/// `estimated_bytes_scanned` is an UPPER BOUND: what a naive full scan would
/// read. For a `LIMIT` query that bound is frequently absurd — measured
/// 2026-08-25, `WHERE region='us-east-2' LIMIT 100` answered in **88ms** over
/// 1.0B rows while priced at 122 GB, because it early-stops. Refusing it
/// pre-flight refuses a query that works.
///
/// And a `LIMIT` cannot be priced DOWNWARD either: 2026-08-19 measured
/// `region='probe' LIMIT 100` matching 130 rows in 2B and sifting the entire
/// table. Selectivity is not knowable at planning time, so a `LIMIT` query's
/// real cost is somewhere between "88ms" and "everything" and no pre-flight
/// number can tell which.
///
/// So do not guess: let the MID-FLIGHT row breaker bound it. That one is exact,
/// already exists (`ceiling_rows_scanned` -> 413), and stops the query at a
/// measured row count rather than a hypothetical byte count. Unbounded queries
/// — aggregations over everything, exports — keep the pre-flight byte gate,
/// which is where an upper bound is the right instrument.
pub fn row_limit_of(plan: &LogicalPlan) -> Option<usize> {
    let mut found: Option<usize> = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Limit(limit) = node {
            if let Some(datafusion::prelude::Expr::Literal(sv, _)) = limit.fetch.as_deref() {
                if let Ok(n) = sv.to_string().parse::<usize>() {
                    found = Some(match found {
                        Some(prev) => prev.min(n),
                        None => n,
                    });
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

/// Is the plan bounded by a limit that actually CONSTRAINS it?
///
/// CRITICAL SUBTLETY, and the reason this is not simply "has a Limit node":
/// the server injects `ORDER BY timestamp DESC LIMIT max_rows` into every
/// query on a timestamped table (the newest-first default). So every plan has
/// a Limit, and treating any Limit as a bound would exempt EVERY query from the
/// pre-flight gate — caught by `an_unbounded_scan_is_still_refused`.
///
/// A limit at or above the server's own row ceiling adds nothing the ceiling
/// did not already impose. Only a limit BELOW it is a user asking for a small
/// slice, which is the shape that early-stops.
pub fn is_row_limited(plan: &LogicalPlan, row_ceiling: usize) -> bool {
    row_limit_of(plan).is_some_and(|n| n < row_ceiling)
}

/// Which built-in siglake tables the cost estimator knows how to walk.
/// Other names are priced only when they resolve to managed indexes;
/// unrelated tables such as `information_schema` views are skipped.
fn is_siglake_table(name: &str) -> bool {
    name == "events"
}

/// Names of all tables referenced by `TableScan` nodes in the plan.
fn scanned_tables(plan: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            out.push(scan.table_name.table().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    out.sort();
    out.dedup();
    out
}

/// Best-effort extraction of a `(start, end)` time window from
/// `WHERE timestamp >= X AND timestamp < Y` (or equivalent) clauses
/// in the plan. Open ranges + non-AND combinations are skipped.
pub(crate) fn extract_time_bounds(plan: &LogicalPlan) -> Option<TimeBounds> {
    let now = Utc::now();
    let mut bounds = TimeBounds::default();
    plan.apply(|node| {
        if let LogicalPlan::Filter(filter) = node {
            walk_predicate(&filter.predicate, filter.input.schema(), now, &mut bounds);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    match (bounds.start, bounds.end) {
        (Some(s), Some(e)) if s >= e => None,
        (Some(_), _) | (_, Some(_)) => Some(bounds),
        _ => None,
    }
}

pub(crate) fn extract_exact_time_bounds_from_expr(
    expr: &Expr,
    schema: &DFSchema,
) -> Option<TimeBounds> {
    let now = Utc::now();
    let mut bounds = TimeBounds::default();
    if !walk_exact_time_predicate(expr, schema, now, &mut bounds) {
        return None;
    }
    match (bounds.start, bounds.end) {
        (Some(s), Some(e)) if s >= e => None,
        (Some(_), _) | (_, Some(_)) => Some(bounds),
        _ => None,
    }
}

fn walk_exact_time_predicate(
    expr: &Expr,
    schema: &DFSchema,
    now: DateTime<Utc>,
    bounds: &mut TimeBounds,
) -> bool {
    match expr {
        Expr::BinaryExpr(b) => match b.op {
            Operator::And => {
                walk_exact_time_predicate(&b.left, schema, now, bounds)
                    && walk_exact_time_predicate(&b.right, schema, now, bounds)
            }
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                let (col, step_ns, op, lit) =
                    match timestamp_comparison(&b.left, &b.right, schema, now) {
                        Some((c, step, t)) => (c, step, b.op, t),
                        None => match timestamp_comparison(&b.right, &b.left, schema, now) {
                            Some((c, step, t)) => (c, step, flip_op(b.op), t),
                            None => return constant_true_expr(expr).unwrap_or(false),
                        },
                    };
                if !is_time_column(&col) {
                    return false;
                }
                match op {
                    Operator::Gt => update_start(bounds, quantize_bound(lit, step_ns, false)),
                    Operator::GtEq => update_start(bounds, quantize_bound(lit, step_ns, true)),
                    Operator::Lt => update_end(bounds, quantize_bound(lit, step_ns, true)),
                    Operator::LtEq => update_end(bounds, quantize_bound(lit, step_ns, false)),
                    _ => {}
                }
                true
            }
            _ => constant_true_expr(expr).unwrap_or(false),
        },
        Expr::Between(between) if !between.negated => {
            let (col, step_ns, low) =
                match timestamp_comparison(&between.expr, &between.low, schema, now) {
                    Some(value) if is_time_column(&value.0) => value,
                    _ => return false,
                };
            let Some((high_col, high_step_ns, high)) =
                timestamp_comparison(&between.expr, &between.high, schema, now)
            else {
                return false;
            };
            if high_col != col || high_step_ns != step_ns {
                return false;
            }
            update_start(bounds, quantize_bound(low, step_ns, true));
            update_end(bounds, quantize_bound(high, step_ns, false));
            true
        }
        _ => constant_true_expr(expr).unwrap_or(false),
    }
}

fn constant_true_expr(expr: &Expr) -> Option<bool> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(v), _) => Some(v.unwrap_or(false)),
        Expr::Cast(cast) => constant_true_expr(&cast.expr),
        Expr::Alias(alias) => constant_true_expr(&alias.expr),
        Expr::BinaryExpr(b) => {
            let left = literal_scalar_value(&b.left)?;
            let right = literal_scalar_value(&b.right)?;
            match b.op {
                Operator::Eq => Some(left == right),
                Operator::NotEq => Some(left != right),
                _ => None,
            }
        }
        _ => None,
    }
}

fn literal_scalar_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => literal_scalar_value(&cast.expr),
        Expr::Alias(alias) => literal_scalar_value(&alias.expr),
        _ => None,
    }
}

fn walk_predicate(expr: &Expr, schema: &DFSchema, now: DateTime<Utc>, bounds: &mut TimeBounds) {
    match expr {
        Expr::BinaryExpr(b) => match b.op {
            Operator::And => {
                walk_predicate(&b.left, schema, now, bounds);
                walk_predicate(&b.right, schema, now, bounds);
            }
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
                let (col, step_ns, op, lit) =
                    match timestamp_comparison(&b.left, &b.right, schema, now) {
                        Some((c, step, t)) => (c, step, b.op, t),
                        None => match timestamp_comparison(&b.right, &b.left, schema, now) {
                            Some((c, step, t)) => (c, step, flip_op(b.op), t),
                            None => return,
                        },
                    };
                if !is_time_column(&col) {
                    return;
                }
                match op {
                    Operator::Gt => update_start(bounds, quantize_bound(lit, step_ns, false)),
                    Operator::GtEq => update_start(bounds, quantize_bound(lit, step_ns, true)),
                    Operator::Lt => update_end(bounds, quantize_bound(lit, step_ns, true)),
                    Operator::LtEq => update_end(bounds, quantize_bound(lit, step_ns, false)),
                    _ => {}
                }
            }
            _ => {}
        },
        Expr::Between(between) if !between.negated => {
            let (col, step_ns, low) =
                match timestamp_comparison(&between.expr, &between.low, schema, now) {
                    Some(value) if is_time_column(&value.0) => value,
                    _ => return,
                };
            let Some((high_col, high_step_ns, high)) =
                timestamp_comparison(&between.expr, &between.high, schema, now)
            else {
                return;
            };
            if high_col != col || high_step_ns != step_ns {
                return;
            }
            update_start(bounds, quantize_bound(low, step_ns, true));
            update_end(bounds, quantize_bound(high, step_ns, false));
        }
        _ => {}
    }
}

fn is_time_column(col: &str) -> bool {
    const TIME_COLS: &[&str] = &[
        "timestamp",
        "started_at",
        "ended_at",
        "bucket_start",
        "added_at",
    ];
    TIME_COLS.iter().any(|t| t.eq_ignore_ascii_case(col))
}

fn update_start(bounds: &mut TimeBounds, candidate: Option<DateTime<Utc>>) {
    if let Some(candidate) = candidate {
        bounds.start = Some(match bounds.start {
            Some(current) => current.max(candidate),
            None => candidate,
        });
    }
}

fn update_end(bounds: &mut TimeBounds, candidate: Option<DateTime<Utc>>) {
    if let Some(candidate) = candidate {
        bounds.end = Some(match bounds.end {
            Some(current) => current.min(candidate),
            None => candidate,
        });
    }
}

/// Map a comparison against a discrete timestamp column onto a half-open
/// nanosecond interval. `round_up` selects the first representable value at or
/// above `ts`; otherwise this returns the first representable value above it.
/// Euclidean division is essential here: truncation toward zero is wrong for
/// pre-epoch subsecond values.
fn quantize_bound(ts: DateTime<Utc>, step_ns: i64, round_up: bool) -> Option<DateTime<Utc>> {
    let nanos = ts.timestamp_nanos_opt()?;
    let floor = nanos.div_euclid(step_ns).checked_mul(step_ns)?;
    let boundary = if round_up && nanos.rem_euclid(step_ns) == 0 {
        floor
    } else {
        floor.checked_add(step_ns)?
    };
    Some(Utc.timestamp_nanos(boundary))
}

fn timestamp_step(data_type: &DataType) -> Option<i64> {
    match data_type {
        DataType::Timestamp(TimeUnit::Second, _) => Some(1_000_000_000),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(1_000_000),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(1_000),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(1),
        _ => None,
    }
}

/// Return the source column and its actual representable timestamp step.
/// Lossless casts to an equal or finer timestamp unit preserve that step.
/// Lossy casts and non-timestamp casts are declined: replacing those predicates
/// by a raw `timestamp_ns` interval would not be demonstrably equivalent.
fn timestamp_column(expr: &Expr, schema: &DFSchema) -> Option<(String, i64)> {
    match expr {
        Expr::Column(c) => Some((
            c.name.clone(),
            timestamp_step(&expr.get_type(schema).ok()?)?,
        )),
        Expr::Alias(alias) => timestamp_column(&alias.expr, schema),
        Expr::Cast(cast) => {
            let (name, source_step) = timestamp_column(&cast.expr, schema)?;
            let target_step = timestamp_step(&cast.data_type)?;
            (target_step <= source_step).then_some((name, source_step))
        }
        _ => None,
    }
}

/// Resolve the coercion DataFusion applies to a timestamp comparison. The
/// literal must be evaluated in that coerced type before the half-open bounds
/// are derived (`timestamp_us <= timestamp_ns(999)` compares against `0us`).
/// A coercion that makes the column coarser is deliberately refused: Arrow's
/// pre-epoch downcast buckets are not representable by one ordinary ns range.
fn timestamp_comparison(
    column: &Expr,
    literal: &Expr,
    schema: &DFSchema,
    now: DateTime<Utc>,
) -> Option<(String, i64, DateTime<Utc>)> {
    let (name, source_step) = timestamp_column(column, schema)?;
    let column_type = column.get_type(schema).ok()?;
    let literal_type = literal.get_type(schema).ok()?;
    let comparison_type = comparison_coercion(&column_type, &literal_type)?;
    let comparison_step = timestamp_step(&comparison_type)?;
    if comparison_step > source_step {
        return None;
    }
    let value = extract_timestamp_at(literal, now)?;
    let nanos = value.timestamp_nanos_opt()?;
    let coerced = ScalarValue::TimestampNanosecond(Some(nanos), None)
        .cast_to(&comparison_type)
        .ok()
        .and_then(|value| literal_timestamp(&value))?;
    Some((name, source_step, coerced))
}

fn extract_timestamp_at(expr: &Expr, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match expr {
        Expr::Literal(v, _) => literal_timestamp(v),
        Expr::Cast(cast) => {
            // Evaluate the cast instead of peeling it off. In particular, a
            // nanosecond literal cast to microseconds is not the same instant;
            // DataFusion applies the cast before the comparison.
            if let Expr::Literal(value, _) = cast.expr.as_ref() {
                return value
                    .cast_to(&cast.data_type)
                    .ok()
                    .and_then(|value| literal_timestamp(&value));
            }
            let nanos = extract_timestamp_at(&cast.expr, now)?.timestamp_nanos_opt()?;
            ScalarValue::TimestampNanosecond(Some(nanos), None)
                .cast_to(&cast.data_type)
                .ok()
                .and_then(|value| literal_timestamp(&value))
        }
        Expr::Alias(alias) => extract_timestamp_at(&alias.expr, now),
        Expr::ScalarFunction(func)
            if is_current_timestamp_function(func.func.name(), &func.args) =>
        {
            Some(now)
        }
        Expr::ScalarFunction(func) => extract_function_timestamp(func.func.name(), &func.args, now),
        Expr::BinaryExpr(b) => match b.op {
            Operator::Plus => {
                if let (Some(ts), Some(interval)) = (
                    extract_timestamp_at(&b.left, now),
                    extract_interval_value(&b.right),
                ) {
                    return apply_interval(ts, interval, false);
                }
                if let (Some(interval), Some(ts)) = (
                    extract_interval_value(&b.left),
                    extract_timestamp_at(&b.right, now),
                ) {
                    return apply_interval(ts, interval, false);
                }
                None
            }
            Operator::Minus => {
                if let (Some(ts), Some(interval)) = (
                    extract_timestamp_at(&b.left, now),
                    extract_interval_value(&b.right),
                ) {
                    return apply_interval(ts, interval, true);
                }
                None
            }
            _ => None,
        },
        _ => None,
    }
}

fn literal_timestamp(scalar: &ScalarValue) -> Option<DateTime<Utc>> {
    use chrono::{NaiveDate, NaiveDateTime, TimeZone};
    match scalar {
        ScalarValue::TimestampNanosecond(Some(n), _) => Utc.timestamp_nanos(*n).into(),
        ScalarValue::TimestampMicrosecond(Some(n), _) => Utc.timestamp_micros(*n).single(),
        ScalarValue::TimestampMillisecond(Some(n), _) => Utc.timestamp_millis_opt(*n).single(),
        ScalarValue::TimestampSecond(Some(n), _) => Utc.timestamp_opt(*n, 0).single(),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s
            .parse::<DateTime<Utc>>()
            .ok()
            .or_else(|| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc))
            })
            .or_else(|| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            })
            .or_else(|| {
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            }),
        _ => None,
    }
}

fn literal_epoch_seconds(scalar: &ScalarValue) -> Option<DateTime<Utc>> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_opt(*v, 0).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_opt(*v as i64, 0).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v)
            .ok()
            .and_then(|secs| Utc.timestamp_opt(secs, 0).single()),
        _ => None,
    }
}

fn literal_epoch_millis(scalar: &ScalarValue) -> Option<DateTime<Utc>> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_millis_opt(*v).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_millis_opt(*v as i64).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v)
            .ok()
            .and_then(|millis| Utc.timestamp_millis_opt(millis).single()),
        _ => None,
    }
}

fn literal_epoch_micros(scalar: &ScalarValue) -> Option<DateTime<Utc>> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::Int16(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::Int32(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::Int64(Some(v)) => Utc.timestamp_micros(*v).single(),
        ScalarValue::UInt8(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::UInt16(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::UInt32(Some(v)) => Utc.timestamp_micros(*v as i64).single(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v)
            .ok()
            .and_then(|micros| Utc.timestamp_micros(micros).single()),
        _ => None,
    }
}

fn literal_epoch_nanos(scalar: &ScalarValue) -> Option<DateTime<Utc>> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::Int16(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::Int32(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::Int64(Some(v)) => Some(Utc.timestamp_nanos(*v)),
        ScalarValue::UInt8(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::UInt16(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::UInt32(Some(v)) => Some(Utc.timestamp_nanos(*v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v)
            .ok()
            .map(|nanos| Utc.timestamp_nanos(nanos)),
        _ => None,
    }
}

fn is_current_timestamp_function(name: &str, args: &[Expr]) -> bool {
    args.is_empty()
        && matches!(
            name.to_ascii_lowercase().as_str(),
            "now" | "current_timestamp" | "current_timestamp()"
        )
}

fn extract_function_timestamp(
    name: &str,
    args: &[Expr],
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let lower = name.to_ascii_lowercase();
    if is_current_timestamp_function(&lower, args) {
        return Some(now);
    }
    match lower.as_str() {
        "to_timestamp" => {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                Expr::Literal(value, _) => {
                    literal_timestamp(value).or_else(|| literal_epoch_seconds(value))
                }
                Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
                _ => None,
            }
        }
        "to_timestamp_millis" => {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                Expr::Literal(value, _) => {
                    literal_timestamp(value).or_else(|| literal_epoch_millis(value))
                }
                Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
                _ => None,
            }
        }
        "to_timestamp_micros" => {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                Expr::Literal(value, _) => {
                    literal_timestamp(value).or_else(|| literal_epoch_micros(value))
                }
                Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
                _ => None,
            }
        }
        "to_timestamp_nanos" => {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                Expr::Literal(value, _) => {
                    literal_timestamp(value).or_else(|| literal_epoch_nanos(value))
                }
                Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
                _ => None,
            }
        }
        "to_timestamp_seconds" => {
            if args.len() != 1 {
                return None;
            }
            match &args[0] {
                Expr::Literal(value, _) => {
                    literal_timestamp(value).or_else(|| literal_epoch_seconds(value))
                }
                Expr::Cast(cast) => extract_timestamp_at(&cast.expr, now),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
struct IntervalValue {
    months: i32,
    days: i32,
    nanos: i64,
}

fn extract_interval_value(expr: &Expr) -> Option<IntervalValue> {
    let scalar = match expr {
        Expr::Literal(v, _) => v,
        Expr::Cast(cast) => return extract_interval_value(&cast.expr),
        Expr::Alias(alias) => return extract_interval_value(&alias.expr),
        _ => return None,
    };
    match scalar {
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
            Some(IntervalValue {
                months,
                days,
                nanos,
            })
        }
        ScalarValue::IntervalDayTime(Some(v)) => {
            let (days, millis) = IntervalDayTimeType::to_parts(*v);
            Some(IntervalValue {
                months: 0,
                days,
                nanos: millis as i64 * 1_000_000,
            })
        }
        ScalarValue::IntervalYearMonth(Some(v)) => {
            let months = IntervalYearMonthType::to_months(*v);
            Some(IntervalValue {
                months,
                days: 0,
                nanos: 0,
            })
        }
        _ => None,
    }
}

fn apply_interval(
    ts: DateTime<Utc>,
    interval: IntervalValue,
    subtract: bool,
) -> Option<DateTime<Utc>> {
    let mut out = ts;
    if interval.months != 0 {
        let months = Months::new(interval.months.unsigned_abs());
        out = if subtract == (interval.months > 0) {
            out.checked_sub_months(months)?
        } else {
            out.checked_add_months(months)?
        };
    }
    if interval.days != 0 {
        let days = Days::new(interval.days.unsigned_abs() as u64);
        out = if subtract == (interval.days > 0) {
            out.checked_sub_days(days)?
        } else {
            out.checked_add_days(days)?
        };
    }
    if interval.nanos != 0 {
        let delta = TimeDelta::nanoseconds(interval.nanos.abs());
        out = if subtract == (interval.nanos > 0) {
            out.checked_sub_signed(delta)?
        } else {
            out.checked_add_signed(delta)?
        };
    }
    Some(out)
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

/// Walk the logical plan looking for anti-patterns we want to surface
/// to the caller. Best-effort — additions to this list are cheap.
fn collect_warnings(plan: &LogicalPlan) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut has_table_scan = false;
    let mut has_time_predicate = false;
    let mut has_cross_join = false;

    plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(_) => has_table_scan = true,
            LogicalPlan::Filter(filter) if filter_touches_time_column(&filter.predicate) => {
                has_time_predicate = true;
            }
            // No equi-keys + no residual predicate = cartesian product.
            LogicalPlan::Join(join)
                if join.on.is_empty()
                    && join.filter.is_none()
                    && matches!(
                        join.join_type,
                        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                    ) =>
            {
                has_cross_join = true;
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();

    if has_table_scan && !has_time_predicate {
        warnings
            .push("query has no predicate on a timestamp column — full table scan likely".into());
    }
    if has_cross_join {
        warnings.push("query contains a cross join — cardinality may blow up".into());
    }
    warnings
}

fn filter_touches_time_column(expr: &Expr) -> bool {
    let mut found = false;
    expr.apply(|e| {
        if let Expr::Column(col) = e {
            // The known time columns across siglake tables. Cheap
            // pattern check — if you rename these, update both.
            const TIME_COLS: &[&str] = &[
                "timestamp",
                "started_at",
                "ended_at",
                "bucket_start",
                "added_at",
            ];
            if TIME_COLS.iter().any(|t| col.name.eq_ignore_ascii_case(t)) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        if let Expr::BinaryExpr(b) = e {
            // Only equality / range comparisons count as time predicates.
            if !matches!(
                b.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
            ) {
                return Ok(TreeNodeRecursion::Continue);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::Duration;
    use datafusion::prelude::SessionContext;
    use siglake_core::Event;
    use siglake_storage::iceberg::IcebergContext;

    async fn plan_with_events(query: &str, events: Vec<Event>) -> (Arc<IcebergContext>, DataFrame) {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        if !events.is_empty() {
            ice.append_events(&events).await.unwrap();
        }
        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let df = ctx.sql(query).await.unwrap();
        (ice, df)
    }

    fn find_filter(plan: &LogicalPlan) -> Option<&datafusion::logical_expr::Filter> {
        match plan {
            LogicalPlan::Filter(filter) => Some(filter),
            LogicalPlan::Projection(projection) => find_filter(projection.input.as_ref()),
            LogicalPlan::Aggregate(aggregate) => find_filter(aggregate.input.as_ref()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn extract_time_bounds_understands_now_minus_interval() {
        let (_, df) = plan_with_events(
            "SELECT * FROM events \
             WHERE timestamp >= now() - interval '5 minutes' \
               AND timestamp < now()",
            Vec::new(),
        )
        .await;
        let bounds = extract_time_bounds(df.logical_plan()).expect("time bounds");
        let start = bounds.start.expect("start");
        let end = bounds.end.expect("end");
        let secs = (end - start).num_seconds();
        assert!(
            (299..=301).contains(&secs),
            "expected ~5 minute window, got {secs}s ({start}..{end})"
        );
    }

    #[tokio::test]
    async fn extract_time_bounds_understands_numeric_to_timestamp_seconds() {
        let (_, df) = plan_with_events(
            "SELECT * FROM events \
             WHERE timestamp >= to_timestamp(1767312000) \
               AND timestamp < to_timestamp(1767398400)",
            Vec::new(),
        )
        .await;
        let bounds = extract_time_bounds(df.logical_plan()).expect("time bounds");
        let start = bounds.start.expect("start");
        let end = bounds.end.expect("end");
        assert_eq!(start.timestamp(), 1_767_312_000);
        assert_eq!(end.timestamp(), 1_767_398_400);
    }

    #[tokio::test]
    async fn extract_time_bounds_supports_open_lower_bound() {
        let (_, df) = plan_with_events(
            "SELECT * FROM events \
             WHERE timestamp >= now() - interval '1 hour'",
            Vec::new(),
        )
        .await;
        let bounds = extract_time_bounds(df.logical_plan()).expect("time bounds");
        assert!(bounds.start.is_some(), "missing lower bound: {bounds:?}");
        assert!(bounds.end.is_none(), "unexpected upper bound: {bounds:?}");
    }

    #[tokio::test]
    async fn extract_time_bounds_supports_between() {
        let (_, df) = plan_with_events(
            "SELECT * FROM events \
             WHERE timestamp BETWEEN to_timestamp(1767312000) AND to_timestamp(1767484799)",
            Vec::new(),
        )
        .await;
        let bounds = extract_time_bounds(df.logical_plan()).expect("time bounds");
        assert_eq!(bounds.start.expect("start").timestamp(), 1_767_312_000);
        assert_eq!(bounds.end.expect("end").timestamp(), 1_767_484_799);
    }

    #[tokio::test]
    async fn extract_exact_time_bounds_accepts_tautology_and_time_window() {
        let (_, df) = plan_with_events(
            "SELECT count(*) FROM events \
             WHERE timestamp >= to_timestamp(1767312000) \
               AND timestamp < to_timestamp(1767398400) \
               AND 7 = 7",
            Vec::new(),
        )
        .await;
        let filter = find_filter(df.logical_plan()).expect("filter plan");
        let bounds = extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema())
            .expect("exact time bounds");
        assert_eq!(bounds.start.expect("start").timestamp(), 1_767_312_000);
        assert_eq!(bounds.end.expect("end").timestamp(), 1_767_398_400);
    }

    #[tokio::test]
    async fn extract_exact_time_bounds_rejects_dimensional_filter() {
        let (_, df) = plan_with_events(
            "SELECT count(*) FROM events \
             WHERE timestamp >= to_timestamp(1767312000) \
               AND timestamp < to_timestamp(1767398400) \
               AND host = 'host-1'",
            Vec::new(),
        )
        .await;
        let filter = find_filter(df.logical_plan()).expect("filter plan");
        assert!(
            extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema()).is_none()
        );
    }

    #[tokio::test]
    async fn exact_bounds_use_the_timestamp_columns_microsecond_step() {
        let cases = [
            (
                "timestamp > to_timestamp_micros(0) AND timestamp < to_timestamp_micros(2)",
                1_000,
                2_000,
            ),
            (
                "timestamp >= to_timestamp_micros(0) AND timestamp <= to_timestamp_micros(0)",
                0,
                1_000,
            ),
            (
                "timestamp BETWEEN to_timestamp_micros(0) AND to_timestamp_micros(0)",
                0,
                1_000,
            ),
            (
                "timestamp >= to_timestamp_nanos(123) AND timestamp <= to_timestamp_nanos(999)",
                0,
                1_000,
            ),
        ];
        for (predicate, expected_lo, expected_hi) in cases {
            let (_, df) = plan_with_events(
                &format!("SELECT count(*) FROM events WHERE {predicate}"),
                Vec::new(),
            )
            .await;
            let filter = find_filter(df.logical_plan()).expect("filter plan");
            let bounds =
                extract_exact_time_bounds_from_expr(&filter.predicate, filter.input.schema())
                    .unwrap_or_else(|| panic!("bounds for {predicate}: {:?}", filter.predicate));
            assert_eq!(
                bounds.start.and_then(|v| v.timestamp_nanos_opt()),
                Some(expected_lo),
                "lower bound for {predicate}"
            );
            assert_eq!(
                bounds.end.and_then(|v| v.timestamp_nanos_opt()),
                Some(expected_hi),
                "upper bound for {predicate}"
            );
        }
    }

    #[test]
    fn quantized_bounds_round_pre_epoch_values_with_euclidean_division() {
        let value = Utc.timestamp_nanos(-1_877);
        assert_eq!(
            quantize_bound(value, 1_000, true).and_then(|v| v.timestamp_nanos_opt()),
            Some(-1_000)
        );
        assert_eq!(
            quantize_bound(value, 1_000, false).and_then(|v| v.timestamp_nanos_opt()),
            Some(-1_000)
        );
        let exact = Utc.timestamp_nanos(-2_000);
        assert_eq!(
            quantize_bound(exact, 1_000, true).and_then(|v| v.timestamp_nanos_opt()),
            Some(-2_000)
        );
        assert_eq!(
            quantize_bound(exact, 1_000, false).and_then(|v| v.timestamp_nanos_opt()),
            Some(-1_000)
        );
    }

    #[tokio::test]
    async fn estimate_prunes_dynamic_now_interval() {
        let now = Utc::now();
        let old = Event {
            timestamp: now - Duration::days(2),
            host: "old-host".into(),
            source: "smoke".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "old".into(),
            attributes: None,
        };
        let recent = Event {
            timestamp: now - Duration::minutes(5),
            host: "recent-host".into(),
            source: "smoke".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "recent".into(),
            attributes: None,
        };
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.append_events(&[old, recent]).await.unwrap();
        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let all_df = ctx.sql("SELECT count(*) FROM events").await.unwrap();
        let filtered_df = ctx
            .sql(
                "SELECT count(*) FROM events \
                 WHERE timestamp >= now() - interval '1 day' \
                   AND timestamp < now() + interval '1 day'",
            )
            .await
            .unwrap();

        let all = estimate(&all_df, &ice).await.unwrap();
        let filtered = estimate(&filtered_df, &ice).await.unwrap();
        assert!(
            filtered.estimated_rows_processed < all.estimated_rows_processed,
            "dynamic time predicate failed to prune rows: all={all:?} filtered={filtered:?}"
        );
        assert!(
            filtered.files_to_scan.unwrap_or(0) < all.files_to_scan.unwrap_or(0),
            "dynamic time predicate failed to prune files: all={all:?} filtered={filtered:?}"
        );
    }

    #[tokio::test]
    async fn estimate_prunes_open_ended_future_interval() {
        let old = Event {
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            host: "old-host".into(),
            source: "smoke".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: "old".into(),
            attributes: None,
        };
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.append_events(&[old]).await.unwrap();
        let ctx = SessionContext::new();
        ice.register_with_datafusion(&ctx).await.unwrap();
        let all_df = ctx.sql("SELECT count(*) FROM events").await.unwrap();
        let filtered_df = ctx
            .sql(
                "SELECT count(*) FROM events \
                 WHERE timestamp >= now() - interval '1 hour'",
            )
            .await
            .unwrap();

        let all = estimate(&all_df, &ice).await.unwrap();
        let filtered = estimate(&filtered_df, &ice).await.unwrap();
        assert_eq!(
            filtered.files_to_scan,
            Some(0),
            "open-ended lower bound failed to prune all files: {filtered:?} all={all:?}"
        );
        assert_eq!(
            filtered.estimated_rows_processed, 0,
            "open-ended lower bound failed to prune rows: {filtered:?} all={all:?}"
        );
    }
}

#[cfg(test)]
mod user_index_cost_tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use siglake_core::index_config::IndexConfig;
    use siglake_core::Event;

    /// A query against a MANAGED INDEX must be priced like one against
    /// `events`. `is_siglake_table` listed only built-ins, so a user
    /// index matched nothing and the whole report came back
    /// `files_to_scan: 0, estimated_bytes_scanned: 0, complexity_class: small`
    /// — for scans that really read hundreds of gigabytes.
    ///
    /// Everything downstream is inert while that is true: pre-flight cost
    /// rejection cannot fire, admission reserves nothing, and a 504 reports a
    /// full-table scan as "small". Every bench suite queries a user index, so
    /// this was the measured path in every round.
    #[tokio::test]
    async fn a_managed_index_named_candidates_is_not_priced_at_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let config = IndexConfig {
            index_id: "candidates".into(),
            ..IndexConfig::builtin_events()
        };
        assert_eq!(config.doc_mapping.timestamp_field, "timestamp");
        ice.create_index(&config).await.unwrap();
        let ident = ice.index_table_ident("candidates");
        let events: Vec<Event> = (0..500)
            .map(|i| Event::now(format!("row {i} queen")))
            .collect();
        let batch = siglake_core::events_to_record_batch(&events).unwrap();
        let mapped = siglake_core::mapping::map_carrier_batch(&batch, &config).unwrap();
        ice.append_to_table(&ident, mapped, &[]).await.unwrap();

        let ctx = SessionContext::new();
        let (provider, _, _) = ice
            .index_provider_with_consumed("candidates")
            .await
            .unwrap()
            .expect("index provider");
        ctx.register_table("candidates", provider).unwrap();

        let df = ctx.sql("SELECT count(*) FROM candidates").await.unwrap();
        let cost = estimate(&df, &ice).await.unwrap();
        let future_df = ctx
            .sql(
                "SELECT count(*) FROM candidates \
                 WHERE timestamp >= now() + interval '1 hour'",
            )
            .await
            .unwrap();
        let future_cost = estimate(&future_df, &ice).await.unwrap();

        assert!(
            cost.estimated_bytes_scanned > 0,
            "a user index priced at ZERO bytes — pre-flight rejection and \
             admission are both inert on this path: {cost:?}"
        );
        assert!(
            cost.estimated_rows_processed > 0,
            "a user index priced at ZERO rows: {cost:?}"
        );
        assert!(
            cost.files_to_scan.unwrap_or(0) > 0,
            "a user index reported no files to scan: {cost:?}"
        );
        assert_eq!(
            future_cost.files_to_scan,
            Some(0),
            "the managed index timestamp field did not prune a future interval: {future_cost:?}"
        );
        assert_eq!(
            future_cost.estimated_rows_processed, 0,
            "the managed index timestamp field did not prune future rows: {future_cost:?}"
        );
    }

    /// The lazy index lookup must not make an unknown table look known — a
    /// bogus name should still contribute nothing and mark the estimate
    /// inexact, rather than erroring or silently inflating the cost.
    #[tokio::test]
    async fn an_unknown_table_is_still_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.append_events(&[Event::now("hello".to_string())])
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE not_siglake AS SELECT 1 AS x")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM not_siglake").await.unwrap();
        let cost = estimate(&df, &ice).await.unwrap();
        assert_eq!(cost.estimated_bytes_scanned, 0);
        assert!(
            !cost.exact,
            "an unknown table must mark the estimate inexact"
        );
    }
}
