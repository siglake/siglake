//! What one Jaeger read response COSTS, measured — the sizing evidence behind
//! the Jaeger read-limit ceilings designed under task #2103.
//!
//! `#[ignore]`d on purpose: it is a measurement, not an assertion. It appends
//! real spans through the ingest mapper, drives the real Jaeger routes through
//! the real router, and reports, per request, the three quantities the design
//! has to tell apart:
//!
//! - **traces** — what the client asks for (`?limit=`), the only one the
//!   caller controls,
//! - **span rows** — what the second query materializes (`limit` x spans per
//!   trace), which is what `max_rows_returned` counts,
//! - **bytes** — what the render and the response body actually occupy, which
//!   no row count bounds because one span carries an arbitrarily wide
//!   `attributes` JSON string.
//!
//! Peak PROCESS allocation is the number that matters, not the response body
//! length: `query_rows` holds the collected Arrow batches, a
//! `serde_json::Value` tree over all of them, a `Vec<Map<String, Value>>` of
//! the same rows again, then the built Jaeger tree, and axum's response buffer
//! — none of it inside the query memory pool, all of it out of the ~1.25Gi the
//! chart's 4Gi query pod leaves after its caches and pool. A tracking global
//! allocator is the only way to see that from inside the process.
//!
//! `--test-threads=1` IS REQUIRED. The allocator is process-wide, so two tests
//! from this binary running at once interleave their marks and the peak column
//! becomes noise (observed: a peak of `0.00 MiB` for a request that allocates
//! 24 MiB). Run it as:
//!
//! ```text
//! cargo test --release -p siglake-query-server \
//!     --test jaeger_render_cost_measurement -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--release` for the timings only: the peaks are data-driven and reproduced
//! to within 0.1% between the debug and release profiles on 2026-09-08.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
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
use siglake_query_server::{router, AppState, AuthConfig, QueryScanConfig};
use siglake_storage::iceberg::IcebergContext;
use siglake_storage::index_manager::builtin_traces_template;
use tower::util::ServiceExt;

/// What planning the second phase's `trace_id IN (...)` costs, by list length.
///
/// Separated from the corpus sweep because it is the part of a large `?limit=`
/// that NO row or byte bound can reach: `fetch_spans_for_trace_ids` splices one
/// quoted literal per selected trace into the SQL text, and the parse plus
/// `InList` construction happen before a single row is read. It also runs
/// inside one synchronous `sql()` call, so the request deadline (#2096) — which
/// can only fire at an await point — cannot cut it.
///
/// The table is EMPTY and in memory: whatever this costs is planner cost, not
/// scan cost. `ctx.sql` rather than the server's read-only planner, which
/// differs only by three `with_allow_*` flags on the same parse and analysis.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn what_a_large_trace_id_in_list_costs_to_plan() {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::SessionContext;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("service", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("status_code", DataType::Utf8, true),
        Field::new("duration_nanos", DataType::Int64, true),
        Field::new("attributes", DataType::Utf8, true),
    ]));
    let ctx = SessionContext::new();
    ctx.register_batch("traces", RecordBatch::new_empty(schema))
        .unwrap();

    println!(
        "\n=== planning `trace_id IN (n)` over an EMPTY in-memory table ===\n\
         {:>10}  {:>12}  {:>12}  {:>12}",
        "n", "sql KiB", "plan ms", "collect ms",
    );
    for n in [1usize, 20, 200, 2_000, 20_000, 200_000] {
        let in_list = (0..n)
            .map(|i| format!("'{i:032x}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT timestamp, trace_id, span_id, parent_span_id, service, name, kind, \
             status_code, duration_nanos, attributes \
             FROM traces WHERE trace_id IN ({in_list}) \
             ORDER BY timestamp ASC, span_id ASC"
        );
        let started = std::time::Instant::now();
        let df = ctx.sql(&sql).await.unwrap();
        let planned = started.elapsed();
        let started = std::time::Instant::now();
        let batches = df.collect().await.unwrap();
        let collected = started.elapsed();
        assert!(batches.iter().all(|b| b.num_rows() == 0));
        println!(
            "{:>10}  {:>12.1}  {:>12.0}  {:>12.0}",
            n,
            sql.len() as f64 / 1024.0,
            planned.as_secs_f64() * 1000.0,
            collected.as_secs_f64() * 1000.0,
        );
    }
}

/// Live and peak bytes handed out by the allocator. `Relaxed` throughout: the
/// peak is a high-water mark across every thread including the exec pool's, and
/// a few bytes of skew between two threads' updates does not change a
/// megabyte-scale reading.
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Tracking;

impl Tracking {
    fn add(bytes: usize) {
        let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            Self::add(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        if !out.is_null() {
            if new_size >= layout.size() {
                Self::add(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        out
    }
}

#[global_allocator]
static ALLOC: Tracking = Tracking;

/// Live bytes now, and the peak since the last [`reset_peak`].
fn live_bytes() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn reset_peak() -> usize {
    let live = live_bytes();
    PEAK.store(live, Ordering::Relaxed);
    live
}

fn peak_bytes() -> usize {
    PEAK.load(Ordering::Relaxed)
}

/// Generous enough that the pool is never what refuses here — this measures the
/// render, which sits OUTSIDE the pool, so a pool refusal would only hide the
/// number. Reported in the output so the reading says what it was taken under.
const POOL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

const INDEX: &str = "siglake-traces-default";

fn proto_str(key: &str, value: &str) -> ProtoKeyValue {
    ProtoKeyValue {
        key: key.to_string(),
        value: Some(ProtoAnyValue {
            value: Some(ProtoValue::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One corpus shape. `unique_names` is the cardinality axis the two list routes
/// live on: with it every span gets its own `service` and `name`, so
/// `/api/services` and `/api/services/{service}/operations` render one row per
/// SPAN instead of one row per fixture constant.
#[derive(Clone, Copy)]
struct Shape {
    traces: u64,
    spans_per_trace: u64,
    attr_bytes: usize,
    unique_names: bool,
}

/// The service every non-`unique_names` span reports, and the one the trace
/// search filters on.
const SERVICE: &str = "checkout";

/// `traces` traces of `spans_per_trace` spans each, every span carrying
/// `attr_bytes` of attribute value. One resource per span so `service.name` can
/// vary per span when `unique_names` is set; the trace search filters on
/// `service`, so a unique-service corpus is deliberately NOT searched.
fn proto_fixture(first_trace: u64, traces: u64, shape: Shape) -> ExportTraceServiceRequest {
    let filler = "x".repeat(shape.attr_bytes);
    let resource_spans = (0..traces)
        .flat_map(|t| {
            let trace = first_trace + t;
            let filler = filler.clone();
            (0..shape.spans_per_trace).map(move |s| {
                let k = trace * shape.spans_per_trace + s;
                let start = 1_700_000_000_000_000_000 + k * 700_000;
                let (service, name) = if shape.unique_names {
                    (
                        format!("checkout-shard-{k}"),
                        format!("GET /api/v1/items/{k}"),
                    )
                } else {
                    (SERVICE.to_string(), format!("GET /api/v1/items/{}", s % 16))
                };
                ProtoResourceSpans {
                    resource: Some(ProtoResource {
                        attributes: vec![
                            proto_str("host.name", "trace-host"),
                            proto_str("service.name", &service),
                        ],
                        ..Default::default()
                    }),
                    scope_spans: vec![ProtoScopeSpans {
                        spans: vec![ProtoSpan {
                            trace_id: trace.to_be_bytes().repeat(2),
                            span_id: k.to_be_bytes().to_vec(),
                            name,
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: start,
                            end_time_unix_nano: start + 50_000_000,
                            attributes: vec![
                                proto_str("http.method", "GET"),
                                proto_str("pad", &filler),
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            })
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

/// Append `traces` traces in files of at most `traces_per_file` traces.
async fn append(ice: &IcebergContext, config: &IndexConfig, traces_per_file: u64, shape: Shape) {
    let bloom_refs = config
        .doc_mapping
        .tag_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut done = 0;
    while done < shape.traces {
        let batch_traces = traces_per_file.min(shape.traces - done);
        let events = otlp_proto_traces_to_events(proto_fixture(done, batch_traces, shape));
        let carrier = events_to_record_batch(&events).unwrap();
        let mapped = map_carrier_batch(&carrier, config).unwrap();
        ice.append_to_table(&ice.index_table_ident(INDEX), mapped, &bloom_refs)
            .await
            .unwrap();
        done += batch_traces;
    }
}

async fn get(app: &Router, path: &str) -> (StatusCode, usize, usize, std::time::Duration) {
    // Settle whatever the previous request left behind before the mark, so the
    // peak below belongs to THIS request.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let before = reset_peak();
    let started = std::time::Instant::now();
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
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let elapsed = started.elapsed();
    let peak = peak_bytes().saturating_sub(before);
    (status, bytes.len(), peak, elapsed)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Result-cache outcomes SINCE THE LAST CALL (`snapshot()` drains), keyed by
/// the `outcome` label. One call per request below, so each map describes one
/// poll (#2268).
fn cache_outcomes(snapshotter: &Snapshotter) -> std::collections::HashMap<String, u64> {
    let mut by_outcome = std::collections::HashMap::new();
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        if key.key().name() != "siglake_query_sql_result_cache_requests_total" {
            continue;
        }
        let Some(outcome) = key
            .key()
            .labels()
            .find(|l| l.key() == "outcome")
            .map(|l| l.value().to_string())
        else {
            continue;
        };
        if let DebugValue::Counter(c) = value {
            *by_outcome.entry(outcome).or_insert(0) += c;
        }
    }
    by_outcome
}

/// The one word that describes a poll: `hit`, or `miss` (and what followed it),
/// or `-` when the poll never consulted the cache at all.
fn outcome_label(outcomes: &std::collections::HashMap<String, u64>) -> String {
    let mut labels: Vec<&str> = ["hit", "miss", "insert", "wait", "wait_timeout"]
        .into_iter()
        .filter(|o| outcomes.get(*o).copied().unwrap_or(0) > 0)
        .collect();
    if labels.is_empty() {
        labels.push("-");
    }
    labels.join("+")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn what_a_jaeger_read_costs() {
    siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES);

    // One dimension at a time. Every row of the output holds the others fixed
    // so the moving one's effect is attributable.
    let corpora: &[Shape] = &[
        // Trace count, one span each: the `?limit=` axis by itself.
        Shape {
            traces: 20_000,
            spans_per_trace: 1,
            attr_bytes: 64,
            unique_names: false,
        },
        // Span rows: 20 traces of 50 spans is already 1_000 rows.
        Shape {
            traces: 2_000,
            spans_per_trace: 50,
            attr_bytes: 64,
            unique_names: false,
        },
        // Encoded bytes: the same row counts, 256x the attribute payload.
        Shape {
            traces: 200,
            spans_per_trace: 50,
            attr_bytes: 16_384,
            unique_names: false,
        },
        // Name cardinality: 20_000 distinct services and operations, which is
        // what the two list routes actually render.
        Shape {
            traces: 1_000,
            spans_per_trace: 20,
            attr_bytes: 64,
            unique_names: true,
        },
    ];

    for &shape in corpora {
        let Shape {
            traces,
            spans_per_trace,
            attr_bytes,
            unique_names,
        } = shape;
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        let config = IndexConfig {
            index_id: INDEX.to_string(),
            doc_mapping: builtin_traces_template().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        ice.create_index(&config).await.unwrap();
        let traces_per_file = (5_000 / spans_per_trace).max(1);
        append(&ice, &config, traces_per_file, shape).await;
        let ice_for_arrow = Arc::clone(&ice);
        let app = router(
            AppState::new(ice, AuthConfig::open()).with_query_scan(QueryScanConfig {
                target_partitions: Some(1),
                file_concurrency_limit: Some(1),
                ..Default::default()
            }),
        );
        let base = format!("/api/v1/jaeger/{INDEX}/api");

        println!(
            "\n=== corpus: {traces} traces x {spans_per_trace} spans, {attr_bytes} B attr payload, \
             {} names ({} span rows total, pool {} MiB) ===",
            if unique_names { "unique" } else { "shared" },
            traces * spans_per_trace,
            POOL_BYTES / (1024 * 1024),
        );
        println!(
            "{:>28}  {:>6}  {:>10}  {:>10}  {:>12}  {:>10}  {:>9}",
            "request", "status", "spanrows", "body MiB", "peak MiB", "B/spanrow", "ms",
        );

        // A unique-service corpus has one span per service, so a trace search
        // filtered on one service says nothing; only the list routes below do.
        let mut limits = if unique_names {
            Vec::new()
        } else {
            vec![1u64, 20, 200, 2_000, 20_000]
        };
        limits.retain(|limit| *limit <= traces);
        for limit in limits {
            let (status, body, peak, elapsed) = get(
                &app,
                &format!("{base}/traces?service={SERVICE}&limit={limit}"),
            )
            .await;
            let rows = limit * spans_per_trace;
            println!(
                "{:>28}  {:>6}  {:>10}  {:>10.2}  {:>12.2}  {:>10.0}  {:>9.0}",
                format!("traces?limit={limit}"),
                status.as_u16(),
                rows,
                mib(body),
                mib(peak),
                peak as f64 / rows as f64,
                elapsed.as_secs_f64() * 1000.0,
            );
        }

        // The single-trace route: no `limit` exists on it at all, so its whole
        // cost is one trace's span count.
        let trace_id = format!("{:032x}", 0u64);
        let (status, body, peak, elapsed) = get(&app, &format!("{base}/traces/{trace_id}")).await;
        println!(
            "{:>28}  {:>6}  {:>10}  {:>10.2}  {:>12.2}  {:>10.0}  {:>9.0}",
            "traces/{one}",
            status.as_u16(),
            spans_per_trace,
            mib(body),
            mib(peak),
            peak as f64 / spans_per_trace as f64,
            elapsed.as_secs_f64() * 1000.0,
        );

        // The unit a mid-flight bound can actually read: ARROW bytes of the
        // span columns as they arrive, which is what
        // `collect_plan_with_caps_stream` already sees per batch on the SQL
        // path. Everything above is a PEAK; this is the number a ceiling would
        // be expressed in, and the ratio between them is what makes such a
        // ceiling sizeable. Same ten columns `fetch_spans_for_trace_ids`
        // selects, no `IN` list (the list length is measured separately).
        {
            let ctx = datafusion::prelude::SessionContext::new();
            ice_for_arrow
                .register_table_with_datafusion(
                    &ctx,
                    &ice_for_arrow.index_table_ident(INDEX),
                    "traces",
                )
                .await
                .unwrap();
            for rows in [1_000u64, 10_000] {
                if rows > traces * spans_per_trace {
                    continue;
                }
                let batches = ctx
                    .sql(&format!(
                        "SELECT timestamp, trace_id, span_id, parent_span_id, service, name, \
                         kind, status_code, duration_nanos, attributes FROM traces LIMIT {rows}"
                    ))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let arrow: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
                let got: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!(
                    "{:>28}  {:>6}  {:>10}  {:>10.2}  {:>12}  {:>10.0}  {:>9}",
                    format!("[arrow] {rows} span rows"),
                    "-",
                    got,
                    mib(arrow),
                    "-",
                    arrow as f64 / got as f64,
                    "-",
                );
            }
        }

        // The two list routes: no time window, no limit, one aggregate over the
        // whole table each, and a render proportional to distinct-name
        // cardinality — which is what `unique_names` moves.
        let one_service = if unique_names {
            "checkout-shard-0".to_string()
        } else {
            SERVICE.to_string()
        };
        for (label, path) in [
            ("services", format!("{base}/services")),
            (
                "one service's operations",
                format!("{base}/services/{one_service}/operations"),
            ),
        ] {
            let (status, body, peak, elapsed) = get(&app, &path).await;
            println!(
                "{:>28}  {:>6}  {:>10}  {:>10.2}  {:>12.2}  {:>10}  {:>9.0}",
                label,
                status.as_u16(),
                "-",
                mib(body),
                mib(peak),
                "-",
                elapsed.as_secs_f64() * 1000.0,
            );
        }
    }
}

/// What the two name-list polls cost when their OUTPUT is tiny but their INPUT
/// is a bench-scale snapshot.
///
/// This is deliberately separate from [`what_a_jaeger_read_costs`]. That sweep
/// sizes the render ceilings by moving output cardinality over fixtures of at
/// most 100,000 spans; this one holds output at one service / sixteen operations
/// and moves the scan to two million spans in one hundred files. It runs each
/// route repeatedly on the same snapshot, then commits one more file and runs
/// both again. That separates the first read, lower-level warm data-cache
/// effects, and the first poll after a snapshot advance.
///
/// `SIGLAKE_QUERY_RESULT_CACHE` is intentionally left at its default. Before
/// #2268 the line printed below confirmed the opposite of what it says now:
/// result-level caching was enabled and these real Jaeger requests still
/// executed every poll, because the routes enter `TraceQueryContext` directly
/// and never reach the SQL HTTP wrapper that owns the cache. Since #2268 they
/// reach the same store through `sql::prepare_name_list_cache`, so the
/// `outcome` column below is the remeasurement: `miss` on the first poll of a
/// snapshot, `hit` on every repeat of it, and `miss` again on the first poll
/// after a commit, which is the new key.
///
/// The two assertions at the end are the ones the card asks for — a repeat must
/// not re-execute the aggregate, and a commit must produce a new key — and they
/// hold whatever the timings turn out to be, so this stays a measurement whose
/// numbers are read by a human and whose CONTRACT is checked by the machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn what_repeated_jaeger_name_polls_cost_on_a_large_snapshot() {
    siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    const TRACES: u64 = 125_000;
    const SPANS_PER_TRACE: u64 = 16;
    const TRACES_PER_FILE: u64 = 1_250;
    const ROWS: u64 = TRACES * SPANS_PER_TRACE;
    const REPEATS: usize = 5;
    let shape = Shape {
        traces: TRACES,
        spans_per_trace: SPANS_PER_TRACE,
        attr_bytes: 64,
        unique_names: false,
    };
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let config = IndexConfig {
        index_id: INDEX.to_string(),
        doc_mapping: builtin_traces_template().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();

    let ingest_started = std::time::Instant::now();
    append(&ice, &config, TRACES_PER_FILE, shape).await;
    let ingest_elapsed = ingest_started.elapsed();
    let app = router(
        AppState::new(Arc::clone(&ice), AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(1),
            file_concurrency_limit: Some(1),
            ..Default::default()
        }),
    );
    let base = format!("/api/v1/jaeger/{INDEX}/api");
    let routes = [
        ("services", format!("{base}/services")),
        (
            "operations",
            format!("{base}/services/{SERVICE}/operations"),
        ),
    ];

    println!(
        "\n=== Jaeger name polls: {ROWS} span rows, {} files, one service, sixteen operations ===\n\
         fixture build: {:.2}s; SQL result cache enabled: {}\n\
         {:>12}  {:>7}  {:>6}  {:>10}  {:>12}  {:>9}  {:>9}",
        TRACES / TRACES_PER_FILE,
        ingest_elapsed.as_secs_f64(),
        siglake_storage::iceberg::result_caches_enabled(),
        "route",
        "poll",
        "status",
        "body KiB",
        "peak MiB",
        "ms",
        "outcome",
    );
    // `snapshot()` drains, so each read below is the delta for one request.
    let _ = cache_outcomes(&snapshotter);
    let mut repeats_that_executed = 0usize;
    for (label, path) in &routes {
        for poll in 1..=REPEATS {
            let (status, body, peak, elapsed) = get(&app, path).await;
            let outcomes = cache_outcomes(&snapshotter);
            println!(
                "{:>12}  {:>7}  {:>6}  {:>10.2}  {:>12.2}  {:>9.1}  {:>9}",
                label,
                poll,
                status.as_u16(),
                body as f64 / 1024.0,
                mib(peak),
                elapsed.as_secs_f64() * 1000.0,
                outcome_label(&outcomes),
            );
            assert_eq!(status, StatusCode::OK);
            if poll > 1 && outcomes.get("miss").copied().unwrap_or(0) > 0 {
                repeats_that_executed += 1;
            }
        }
    }
    assert_eq!(
        repeats_that_executed, 0,
        "a repeat poll on a standing snapshot re-executed the aggregate — the \
         cost this task exists to remove"
    );

    // A commit invalidates the IcebergContext's table and snapshot-keyed
    // data-file-list caches. The values are unchanged, so response semantics
    // and render cardinality stay fixed while the snapshot and scan input
    // advance.
    append(
        &ice,
        &config,
        TRACES_PER_FILE,
        Shape {
            traces: TRACES_PER_FILE,
            ..shape
        },
    )
    .await;
    for (label, path) in &routes {
        let (status, body, peak, elapsed) = get(&app, path).await;
        let outcomes = cache_outcomes(&snapshotter);
        println!(
            "{:>12}  {:>7}  {:>6}  {:>10.2}  {:>12.2}  {:>9.1}  {:>9}",
            label,
            "new-snap",
            status.as_u16(),
            body as f64 / 1024.0,
            mib(peak),
            elapsed.as_secs_f64() * 1000.0,
            outcome_label(&outcomes),
        );
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            outcomes.get("miss").copied().unwrap_or(0),
            1,
            "{label}: the first poll after a commit was not a new key — the \
             entry is not snapshot-keyed: {outcomes:?}"
        );
    }
}

/// What the PRODUCER hands the collector, batch by batch — the other half of
/// the overshoot #2231 asked about.
///
/// #2184 bounds ACCUMULATION at a batch boundary, so a refusal costs whatever
/// the plan had already built when the collector first got to look. On the wide
/// corpus that was 322 MiB against 162.90 MiB of Arrow for the same rows, and
/// the reason is in the plan rather than in the batch size: the span-fetch
/// query is `ORDER BY timestamp, span_id` with no `LIMIT`, so its `SortExec`
/// is BLOCKING — it buffers every matching row before emitting anything.
///
/// This prints, for the same SQL with and without the render fetch
/// (`max_rows + 1`, what `bound_the_producer` now adds), the physical plan, the
/// rows and Arrow bytes of every batch produced, and the peak allocation of the
/// whole stream.
///
/// The arms keep executing through `bounded_task_context()` — a DEFAULT
/// `SessionConfig` — because that is what the mid-flight collector executed
/// with when the table in the design doc was measured. Since #2251 the
/// collector carries the planning session's config (`midflight::task_ctx_for`),
/// so the last arm is what the collector now does with a tuned session and the
/// others are the historical baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn what_the_producer_hands_the_collector() {
    use datafusion::physical_plan::{displayable, execute_stream};
    use futures::StreamExt;

    siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES);

    // The corpus the 322 MiB reading came from: 200 traces x 50 spans, 16 KiB
    // of attributes per span, 10,000 span rows, 162.90 MiB of Arrow.
    let shape = Shape {
        traces: 200,
        spans_per_trace: 50,
        attr_bytes: 16_384,
        unique_names: false,
    };
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let config = IndexConfig {
        index_id: INDEX.to_string(),
        doc_mapping: builtin_traces_template().doc_mapping,
        retention: None,
        index_at_flush: None,
    };
    ice.create_index(&config).await.unwrap();
    append(&ice, &config, 100, shape).await;

    // The same session shape the routes get, and the same one-partition,
    // one-file-at-a-time configuration the sweep above measures under.
    let scan = QueryScanConfig {
        target_partitions: Some(1),
        file_concurrency_limit: Some(1),
        ..Default::default()
    };

    // `fetch_spans_for_trace_ids`' shape without the `IN` list (measured
    // separately): every span row, ordered by the two keys it orders by.
    const SQL: &str = "SELECT timestamp, trace_id, span_id, parent_span_id, service, name, \
                       kind, status_code, duration_nanos, attributes FROM traces \
                       WHERE trace_id IS NOT NULL ORDER BY timestamp ASC, span_id ASC";
    // The packaged pod's span-row ceiling (1,686) plus the one row a refusal
    // needs — `bound_the_producer`'s fetch on an unspent budget.
    const RENDER_FETCH: usize = 1_687;

    // Six arms, because #2231 filed the batch size as the HYPOTHESIS and the
    // fetch as the alternative, and only a matched pair separates them.
    //
    // "The batch size" is three different things here, and the arms take them
    // apart: the PARQUET READER's, which is process-global scan tuning
    // (`effective_reader_tuning`, set the way the query server's CLI sets it
    // and restored afterwards); the session's
    // `datafusion.execution.batch_size`, which is what operators ABOVE the
    // scan size their output by; and whether that session setting is even
    // DELIVERED to execution — when this was measured the mid-flight collector
    // executed with `bounded_task_context()`, i.e. `TaskContext::default()`
    // plus the shared runtime, which carries a DEFAULT `SessionConfig` and
    // therefore discarded whatever the plan was built with (#2251, fixed since:
    // the collector now takes the session's own config onto the shared
    // runtime). The last arm executes the same plan through the session's own
    // `TaskContext` to show which of those it is.
    for (fetch, reader_batch_size, session_batch_size, session_task_ctx) in [
        (None, None, None, false),
        (Some(RENDER_FETCH), None, None, false),
        (None, Some(1_024usize), None, false),
        (None, None, Some(1_024usize), false),
        (Some(RENDER_FETCH), None, Some(1_024usize), false),
        (None, None, Some(1_024usize), true),
    ] {
        siglake_storage::configure_query_scan_tuning(siglake_storage::QueryScanTuning {
            batch_size: reader_batch_size,
            ..Default::default()
        });
        let ctx = match session_batch_size {
            None => scan.session_context(),
            Some(rows) => {
                let mut state = scan.session_context().state();
                state.config_mut().options_mut().execution.batch_size = rows;
                datafusion::prelude::SessionContext::new_with_state(state)
            }
        };
        ice.register_table_with_datafusion(&ctx, &ice.index_table_ident(INDEX), "traces")
            .await
            .unwrap();
        let df = ctx.sql(SQL).await.unwrap();
        let df = match fetch {
            Some(fetch) => df.limit(0, Some(fetch)).unwrap(),
            None => df,
        };
        let plan = df.create_physical_plan().await.unwrap();
        let shown = |size: Option<usize>| {
            size.map(|b| b.to_string())
                .unwrap_or_else(|| "default (8192)".to_string())
        };
        println!(
            "\n=== producer, fetch = {}, reader batch_size = {}, session batch_size = {}, \
             task ctx = {} ===\n{}",
            fetch.map(|f| f.to_string()).unwrap_or("none".into()),
            shown(reader_batch_size),
            shown(session_batch_size),
            if session_task_ctx {
                "session"
            } else {
                "bounded_task_context()"
            },
            displayable(plan.as_ref()).indent(false),
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let before = reset_peak();
        let started = std::time::Instant::now();
        // The DEFAULT-context arm is the measurement's control: it exists to
        // show what a session-less context costs, so it calls the helper on
        // purpose.
        #[allow(clippy::disallowed_methods)]
        let task_ctx = if session_task_ctx {
            ctx.task_ctx()
        } else {
            siglake_storage::bounded_task_context()
        };
        let mut stream = execute_stream(plan, task_ctx).unwrap();
        let mut n = 0usize;
        let mut rows_total = 0usize;
        let mut arrow_total = 0usize;
        let mut largest = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            let arrow = batch.get_array_memory_size();
            n += 1;
            rows_total += batch.num_rows();
            arrow_total += arrow;
            largest = largest.max(arrow);
            if n <= 3 {
                println!(
                    "  batch {n}: {} rows, {:.2} MiB of Arrow",
                    batch.num_rows(),
                    mib(arrow)
                );
            }
            // What the collector does with a refusing bound: it holds nothing
            // past the crossing. One batch is all it needs to refuse here
            // (1,687 rows > 1,686, or 28.8 MiB > the 3.5 MiB byte ceiling).
            drop(batch);
        }
        let elapsed = started.elapsed();
        let peak = peak_bytes().saturating_sub(before);
        println!(
            "  {n} batches, {rows_total} rows, {:.2} MiB of Arrow in total, largest batch \
             {:.2} MiB, stream peak {:.2} MiB, {:.0} ms",
            mib(arrow_total),
            mib(largest),
            mib(peak),
            elapsed.as_secs_f64() * 1000.0,
        );
    }
    siglake_storage::configure_query_scan_tuning(siglake_storage::QueryScanTuning::default());
}

// ---------------------------------------------------------------------------
// #2282: what a name list ABOVE the 128-row entry cap costs, and what storing
// one would cost the shared SQL result cache.
// ---------------------------------------------------------------------------

/// A corpus whose two name lists have an EXACT cardinality.
///
/// [`Shape`]'s `unique_names` moves cardinality and row count together — one
/// service and one operation per span — which cannot land on 127, 128 and 129
/// names over a fixture large enough for the aggregate to cost anything. This
/// pins `names` distinct services AND `names` distinct operations of one
/// service over an arbitrary row count instead:
///
///   - even spans carry service `svc_name(i)` with the single operation
///     `op-0000`, and `svc_name(0)` IS [`SERVICE`], so the distinct services
///     are `{checkout} ∪ {svc-0001..svc-{names-1}}` — exactly `names`;
///   - odd spans carry [`SERVICE`] with operation `op-{i:04}` over the whole
///     range including `op-0000`, so `SERVICE`'s distinct operations are
///     exactly `names` too.
///
/// Both lists therefore have the same cardinality on one fixture, which is what
/// makes the two routes' rows in the table below comparable. Requires at least
/// `2 * names` span rows so every residue is present; asserted by the sweep,
/// which reads the rendered length back.
///
/// `pad` right-pads every generated service name to that many bytes (0 leaves
/// them at their natural eight), which is the only way to make a name list
/// WIDE without going over a packaged pod's 10,237-name render ceiling — the
/// axis #2398's byte-cap arm moves. [`SERVICE`] is never padded: it is the
/// service the operations route is asked for by name.
fn cardinal_fixture(
    first_trace: u64,
    traces: u64,
    spans_per_trace: u64,
    attr_bytes: usize,
    names: u64,
    pad: usize,
) -> ExportTraceServiceRequest {
    let filler = "x".repeat(attr_bytes);
    let resource_spans = (0..traces)
        .flat_map(|t| {
            let trace = first_trace + t;
            let filler = filler.clone();
            (0..spans_per_trace).map(move |s| {
                let k = trace * spans_per_trace + s;
                let i = (k / 2) % names;
                let (service, name) = if k.is_multiple_of(2) {
                    (svc_name(i, pad), "op-0000".to_string())
                } else {
                    (SERVICE.to_string(), format!("op-{i:04}"))
                };
                ProtoResourceSpans {
                    resource: Some(ProtoResource {
                        attributes: vec![
                            proto_str("host.name", "trace-host"),
                            proto_str("service.name", &service),
                        ],
                        ..Default::default()
                    }),
                    scope_spans: vec![ProtoScopeSpans {
                        spans: vec![ProtoSpan {
                            trace_id: trace.to_be_bytes().repeat(2),
                            span_id: k.to_be_bytes().to_vec(),
                            name,
                            kind: span::SpanKind::Server as i32,
                            start_time_unix_nano: 1_700_000_000_000_000_000 + k * 700_000,
                            end_time_unix_nano: 1_700_000_000_000_000_000
                                + k * 700_000
                                + 50_000_000,
                            attributes: vec![
                                proto_str("http.method", "GET"),
                                proto_str("pad", &filler),
                            ],
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }
            })
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

/// Service `i` of a [`cardinal_fixture`]. Index 0 is [`SERVICE`] so the service
/// list's cardinality is `names` rather than `names + 1`, and so the operations
/// route has a service it can be asked for by a name the caller knows.
///
/// `pad` right-pads to that many ASCII bytes, so the arena an entry of these
/// names weighs is `names * pad + 4 * names` by construction.
fn svc_name(i: u64, pad: usize) -> String {
    if i == 0 {
        SERVICE.to_string()
    } else {
        format!("{:x<width$}", format!("svc-{i:04}"), width = pad)
    }
}

#[allow(clippy::too_many_arguments)]
async fn append_cardinal(
    ice: &IcebergContext,
    config: &IndexConfig,
    index: &str,
    traces: u64,
    spans_per_trace: u64,
    traces_per_file: u64,
    names: u64,
    pad: usize,
) {
    let bloom_refs = config
        .doc_mapping
        .tag_fields
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut done = 0;
    while done < traces {
        let batch_traces = traces_per_file.min(traces - done);
        let events = otlp_proto_traces_to_events(cardinal_fixture(
            done,
            batch_traces,
            spans_per_trace,
            64,
            names,
            pad,
        ));
        let carrier = events_to_record_batch(&events).unwrap();
        let mapped = map_carrier_batch(&carrier, config).unwrap();
        ice.append_to_table(&ice.index_table_ident(index), mapped, &bloom_refs)
            .await
            .unwrap();
        done += batch_traces;
    }
}

/// Like [`get`], but hands back the body so the sweep can read the rendered
/// names and size what an entry holding them would weigh.
async fn get_body(app: &Router, path: &str) -> (StatusCode, Vec<u8>, usize, std::time::Duration) {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let before = reset_peak();
    let started = std::time::Instant::now();
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
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let elapsed = started.elapsed();
    let peak = peak_bytes().saturating_sub(before);
    (status, bytes.to_vec(), peak, elapsed)
}

/// The `data` array of a Jaeger name-list response.
fn names_of_body(body: &[u8]) -> Vec<String> {
    let value: serde_json::Value = serde_json::from_slice(body).unwrap();
    value
        .get("data")
        .and_then(|d| d.as_array())
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// EXACTLY the body `finish_result_cache` would encode and then RETAIN for this
/// list: the `RecordsResponse` a one-column `SELECT DISTINCT` renders —
/// `columns`, `row_count`, and one JSON OBJECT per row keyed by the column, with
/// `cost`, `stats`, `truncated` and `approximation` all absent, which is what
/// `batches_to_records(.., Some(cap))` leaves on this path.
fn rendered_body(column: &str, names: &[String]) -> siglake_query_server::format::RecordsResponse {
    siglake_query_server::format::RecordsResponse {
        columns: vec![column.to_string()],
        row_count: names.len(),
        rows: serde_json::Value::Array(
            names
                .iter()
                .map(|name| {
                    let mut row = serde_json::Map::new();
                    row.insert(column.to_string(), serde_json::Value::String(name.clone()));
                    serde_json::Value::Object(row)
                })
                .collect(),
        ),
        truncated: false,
        max_rows: None,
        cost: None,
        stats: None,
        approximation: None,
    }
}

/// Heap bytes STILL LIVE while the built value is held — the retained cost of a
/// representation, not the traffic building it generated.
///
/// The tracking allocator's `LIVE` counter, read either side of the build with
/// the result alive across the second read and dropped after. Measured rather
/// than counted by hand because the interesting figure is
/// `serde_json::Map`'s, and it is a `BTreeMap` whose one-entry node is a whole
/// 11-slot leaf: any hand estimate of it is wrong by an order of magnitude in
/// one direction or the other.
fn retained_bytes<T>(build: impl FnOnce() -> T) -> usize {
    let before = live_bytes();
    let held = build();
    let retained = live_bytes().saturating_sub(before);
    drop(held);
    retained
}

/// Mean nanoseconds to CLONE one representation, over `ROUNDS` clones.
///
/// A hit is a clone: `SqlResultCache::get` returns `self.entries.get(key)
/// .cloned()`, deep-copying the stored body while the process-wide cache mutex
/// is held. So this is the per-hit cost the two representations differ by, and
/// it is paid under the lock every SQL and Jaeger hit queues behind.
fn clone_nanos<T: Clone>(value: &T) -> f64 {
    const ROUNDS: u32 = 64;
    // `black_box` on every clone: LLVM removes a dead malloc/free pair, and an
    // arena clone is two of them plus a memcpy — without this it optimizes to
    // nothing and reports 0.0. One clone outside the timing so the allocator's
    // first touch of a new size class is not charged to the mean.
    drop(std::hint::black_box(value.clone()));
    let started = std::time::Instant::now();
    for _ in 0..ROUNDS {
        drop(std::hint::black_box(value.clone()));
    }
    started.elapsed().as_nanos() as f64 / f64::from(ROUNDS)
}

/// The three ways to hold the same list, in bytes RETAINED:
///
///   - `rendered` — today's entry: `CachedSqlResult` keeps the whole
///     `RecordsResponse`, i.e. a `serde_json::Value` tree with one
///     `Map<String, Value>` per name. What `SqlResultCache::bytes` charges it,
///     though, is the length of the encoding, which is then DROPPED.
///   - `vec_string` — a `Vec<String>` of the names.
///   - `arena` — one concatenated `String` plus a `Vec<u32>` of offsets: two
///     allocations for the whole list however many names it holds.
fn representation_bytes(column: &str, names: &[String]) -> (usize, usize, usize) {
    let rendered = retained_bytes(|| rendered_body(column, names));
    let vec_string = retained_bytes(|| names.to_vec());
    // Both buffers PRE-SIZED. Growing them geometrically instead leaves up to
    // 2x of capacity slack, which would report the allocator's doubling rather
    // than the representation's footprint (observed: 1,064 KiB for 640 KiB of
    // names). A real implementation knows the total length from the Arrow
    // array's value offsets, or calls `shrink_to_fit` once.
    let arena = retained_bytes(|| {
        let mut text = String::with_capacity(names.iter().map(String::len).sum());
        let mut offsets: Vec<u32> = Vec::with_capacity(names.len());
        for name in names {
            offsets.push(text.len() as u32);
            text.push_str(name);
        }
        (text, offsets)
    });
    (rendered, vec_string, arena)
}

/// What a name list over the 128-row entry cap costs on every poll, and what
/// storing one would cost the SHARED SQL result cache (#2282).
///
/// #2268 routed the two Jaeger name lists into that cache and left the shared
/// entry caps alone, so a COMPLETE list over `SQL_RESULT_CACHE_MAX_ROWS` = 128
/// rows was executed and returned whole on every poll — never truncated, never
/// stored. The card that filed this asked for the cost of the bypass at high
/// cardinality, which the #2268 measurement could not give: its fixture renders
/// one service and sixteen operations, i.e. it measures the SCAN with the list
/// held tiny.
///
/// SINCE #2302 this sweep is the before/after rather than the bypass: name-list
/// eligibility is 512 KiB of arena, not 128 rows, so all four cardinalities
/// here read `miss+insert` then `hit` and the `outcome2` column is where that
/// is read. The `entry KiB` / `render KiB` / `vec KiB` / `arena KiB` columns
/// still price the three representations, which is what chose the entry shape;
/// `entry KiB` remains the pre-#2304 encoded figure, kept so the 29–35x
/// under-accounting the design argued from stays legible.
///
/// Four cardinalities — 127 (the last that fits), 128 (the cap itself), 129
/// (one over) and 800 (the bypass arm of `tests/jaeger_name_cache.rs`) — over
/// ONE fixture size, so the only thing moving between rows is the number of
/// distinct names. Both routes on each, since operations were the more
/// expensive of the two at low cardinality. Per row:
///
///   - `ms` for poll 1 and poll 2 of the same snapshot, and the cache
///     `outcome` of each. Before #2302: `miss+insert` then `hit` below the
///     128-row cap, `miss` then `miss` above it — the bypass costing a full
///     aggregate forever. After it: `hit` on every row.
///   - `peak MiB` of the whole request, from the tracking allocator.
///   - `entry KiB` — what `finish_result_cache` would ENCODE for this list,
///     i.e. what `SqlResultCache::bytes` would charge it against the 4 MiB
///     budget — and the same list's retained cost in the three
///     representations, which is what a "store the names, not the render"
///     design would actually save.
///
/// `--test-threads=1`, like every measurement in this file: the allocator, the
/// result cache and the metrics recorder are all process-wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn what_a_name_list_over_the_entry_cap_costs() {
    siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // Held FIXED across the sweep so cardinality is the only moving axis. 50
    // files of 8,000 rows: enough scan that a full aggregate is milliseconds
    // rather than microseconds, and small enough that four fixtures build in
    // one run.
    const TRACES: u64 = 25_000;
    const SPANS_PER_TRACE: u64 = 16;
    const TRACES_PER_FILE: u64 = 500;
    const ROWS: u64 = TRACES * SPANS_PER_TRACE;
    const CARDINALITIES: &[u64] = &[127, 128, 129, 800];

    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    // One index per cardinality in one warehouse: cardinality is a property of
    // the DATA, so each arm needs its own table, and they must not share a
    // snapshot or a cache key.
    let mut indexes = Vec::new();
    let build_started = std::time::Instant::now();
    for &names in CARDINALITIES {
        let index = format!("traces-card-{names}");
        let config = IndexConfig {
            index_id: index.clone(),
            doc_mapping: builtin_traces_template().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        ice.create_index(&config).await.unwrap();
        append_cardinal(
            &ice,
            &config,
            &index,
            TRACES,
            SPANS_PER_TRACE,
            TRACES_PER_FILE,
            names,
            0,
        )
        .await;
        indexes.push((names, index));
    }
    let build_elapsed = build_started.elapsed();

    let app = router(
        AppState::new(Arc::clone(&ice), AuthConfig::open()).with_query_scan(QueryScanConfig {
            target_partitions: Some(1),
            file_concurrency_limit: Some(1),
            ..Default::default()
        }),
    );

    println!(
        "\n=== #2282: name lists across the 128-row entry cap ===\n\
         {ROWS} span rows x {} files per index, {} indexes; fixtures built in {:.1}s\n\
         SQL result cache enabled: {}; name-list entry allowance: 512 KiB of arena \
         (was 128 rows), store: 4 MiB / 256 entries\n\
         {:>10}  {:>6}  {:>9}  {:>9}  {:>10}  {:>9}  {:>9}  {:>9}  {:>10}  {:>10}  {:>8}",
        TRACES / TRACES_PER_FILE,
        CARDINALITIES.len(),
        build_elapsed.as_secs_f64(),
        siglake_storage::iceberg::result_caches_enabled(),
        "list",
        "names",
        "poll1 ms",
        "poll2 ms",
        "outcome1",
        "outcome2",
        "peak MiB",
        "entry KiB",
        "render KiB",
        "vec KiB",
        "arena KiB",
    );
    let _ = cache_outcomes(&snapshotter);
    let mut sizes: Vec<(u64, &str, usize, usize, f64, f64)> = Vec::new();
    for (names, index) in &indexes {
        let base = format!("/api/v1/jaeger/{index}/api");
        for (label, path, column) in [
            ("services", format!("{base}/services"), "service"),
            (
                "operations",
                format!("{base}/services/{SERVICE}/operations"),
                "name",
            ),
        ] {
            let (status, body, peak, first) = get_body(&app, &path).await;
            let outcome1 = outcome_label(&cache_outcomes(&snapshotter));
            assert_eq!(status, StatusCode::OK, "{label} at {names} names");
            let rendered_names = names_of_body(&body);
            assert_eq!(
                rendered_names.len() as u64,
                *names,
                "{label}: the fixture rendered {} names, not {names} — the corpus \
                 does not cover every residue",
                rendered_names.len()
            );
            let (_, _, _, second) = get_body(&app, &path).await;
            let outcome2 = outcome_label(&cache_outcomes(&snapshotter));
            let entry = serde_json::to_vec(&rendered_body(column, &rendered_names))
                .unwrap()
                .len();
            let (render, vec_string, arena) = representation_bytes(column, &rendered_names);
            println!(
                "{:>10}  {:>6}  {:>9.1}  {:>9.1}  {:>10}  {:>9}  {:>9.2}  {:>9.1}  {:>10.1}  \
                 {:>10.1}  {:>8.1}",
                label,
                names,
                first.as_secs_f64() * 1000.0,
                second.as_secs_f64() * 1000.0,
                outcome1,
                outcome2,
                mib(peak),
                entry as f64 / 1024.0,
                render as f64 / 1024.0,
                vec_string as f64 / 1024.0,
                arena as f64 / 1024.0,
            );
            // What a HIT copies, for the two candidate entry shapes.
            let clone_render = clone_nanos(&rendered_body(column, &rendered_names));
            let text: String = rendered_names.concat();
            let offsets: Vec<u32> = rendered_names
                .iter()
                .scan(0u32, |at, name| {
                    let start = *at;
                    *at += name.len() as u32;
                    Some(start)
                })
                .collect();
            let clone_arena = clone_nanos(&(text, offsets));
            sizes.push((*names, label, entry, render, clone_render, clone_arena));
        }
    }

    // DISPLACEMENT, from the sizes just measured. `SqlResultCache` evicts on
    // whichever of 256 entries and 4 MiB of ACCOUNTED bytes binds first, and
    // what it accounts is `encoded.len()` — an encoding it then drops — while
    // what it retains is the `RecordsResponse`. So the two columns below are
    // the same store measured in the unit it enforces and the unit the pod
    // pays: `fit` name entries of this size fill the byte budget, and holding
    // that many retains `retained MiB`.
    println!(
        "\n--- displacement: filling the 4 MiB accounted budget with name entries ---\n\
         {:>10}  {:>6}  {:>9}  {:>10}  {:>10}  {:>8}  {:>12}  {:>12}  {:>11}",
        "list",
        "names",
        "entry KiB",
        "render KiB",
        "accounted x",
        "fit",
        "retained MiB",
        "hit us:rend",
        "hit us:aren",
    );
    for (names, label, entry, render, clone_render, clone_arena) in &sizes {
        let fit = (4 * 1024 * 1024 / entry.max(&1)).min(256);
        println!(
            "{:>10}  {:>6}  {:>9.1}  {:>10.1}  {:>10.1}  {:>8}  {:>12.1}  {:>12.2}  {:>11.3}",
            label,
            names,
            *entry as f64 / 1024.0,
            *render as f64 / 1024.0,
            *render as f64 / *entry as f64,
            fit,
            mib(fit * render),
            clone_render / 1000.0,
            clone_arena / 1000.0,
        );
    }

    // What the SHARED store would be charged if the row gate simply moved to
    // the render ceiling. Projected rather than measured, because no local
    // fixture can carry 10,237 distinct 128-character service names cheaply,
    // and the projection is the number the design has to answer: one entry's
    // share of a 4 MiB budget that 256 SQL entries also live in.
    let ceilings = siglake_query_server::jaeger_limits::ceilings_from(
        siglake_query_server::admission::MIN_RESERVATION_BYTES,
    );
    println!(
        "\n--- projection: ONE entry at the render ceiling ({} names) against the \
         4 MiB shared byte budget ---\n{:>12}  {:>12}  {:>12}  {:>12}  {:>12}  {:>12}",
        ceilings.names, "name len", "entry KiB", "% of 4 MiB", "render KiB", "vec KiB", "arena KiB",
    );
    for name_len in [8usize, 24, 64, 128] {
        let projected: Vec<String> = (0..ceilings.names)
            .map(|i| format!("{i:0width$}", width = name_len))
            .collect();
        let entry = serde_json::to_vec(&rendered_body("service", &projected))
            .unwrap()
            .len();
        let (render, vec_string, arena) = representation_bytes("service", &projected);
        println!(
            "{:>12}  {:>12.1}  {:>12.1}  {:>12.1}  {:>12.1}  {:>12.1}",
            name_len,
            entry as f64 / 1024.0,
            100.0 * entry as f64 / (4.0 * 1024.0 * 1024.0),
            render as f64 / 1024.0,
            vec_string as f64 / 1024.0,
            arena as f64 / 1024.0,
        );
    }
}

// ---------------------------------------------------------------------------
// #2398: whether Jaeger name entries displace `/api/v1/sql` entries in the ONE
// shared result store.
// ---------------------------------------------------------------------------

/// Rows one measured `/api/v1/sql` browse returns. Small on purpose: 64 such
/// entries have to fit the 4 MiB budget with room left, or the control arm
/// would already be evicting itself and there would be no baseline to compare
/// a name-polled arm against.
const BROWSE_ROWS: usize = 20;
/// `/api/v1/sql` keys per index. `max_rows_returned` is IN the key
/// (`sql::result_cache_key`), and a SQL comment is not — the key re-serialises
/// the parsed statement — so this is how one query shape gets several keys.
const BROWSE_KEYS_PER_INDEX: usize = 8;
/// Indexes one pod serves in every arm below.
const DISPLACEMENT_INDEXES: u64 = 8;
/// Each arm is run this many times end to end, from a reset store.
const DISPLACEMENT_REPEATS: usize = 2;

/// The packaged pod's admission budget: 64 MiB at the default share divisor is
/// a 16 MiB reservation, i.e. the 10,237-name render ceiling every name list
/// below is inside. Same constant as `tests/jaeger_name_cache.rs`.
const DISPLACEMENT_BUDGET: u64 = 64 * 1024 * 1024;

/// One phase's view of the shared store: the `outcome` counters it moved
/// (drained, so they are this phase's deltas) and the two gauges as LEVELS at
/// the end of it.
///
/// One `snapshot()` per phase, because a snapshot DRAINS — and it drains the
/// GAUGES too: `Snapshotter::snapshot` reads every handle with
/// `swap(0, SeqCst)`, counters and gauges alike (metrics-util 0.20,
/// `src/debugging.rs`). So a gauge reads as its real level in the phase that
/// last set it and as a literal zero in every phase after that, which is not
/// what a level means. The store publishes both gauges from
/// `SqlResultCache::insert` and nowhere else, so a phase with no `insert` left
/// the store untouched and its level is the previous phase's — that is the rule
/// below, and it is exact rather than a guess at which zeros are real.
struct StoreReading {
    outcomes: std::collections::HashMap<String, u64>,
    bytes: f64,
    entries: f64,
}

impl StoreReading {
    fn count(&self, outcome: &str) -> u64 {
        self.outcomes.get(outcome).copied().unwrap_or(0)
    }

    /// The levels to carry into the next phase's reading.
    fn levels(&self) -> (f64, f64) {
        (self.bytes, self.entries)
    }
}

fn store_reading(snapshotter: &Snapshotter, carried: (f64, f64)) -> StoreReading {
    let mut outcomes = std::collections::HashMap::new();
    let (mut bytes, mut entries) = (0.0, 0.0);
    for (key, _, _, value) in snapshotter.snapshot().into_vec() {
        match (key.key().name(), value) {
            ("siglake_query_sql_result_cache_requests_total", DebugValue::Counter(count)) => {
                if let Some(outcome) = key.key().labels().find(|l| l.key() == "outcome") {
                    *outcomes.entry(outcome.value().to_string()).or_insert(0) += count;
                }
            }
            ("siglake_query_sql_result_cache_bytes", DebugValue::Gauge(g)) => {
                bytes = g.into_inner()
            }
            ("siglake_query_sql_result_cache_entries", DebugValue::Gauge(g)) => {
                entries = g.into_inner()
            }
            _ => {}
        }
    }
    if outcomes.get("insert").copied().unwrap_or(0) == 0 {
        (bytes, entries) = carried;
    }
    StoreReading {
        outcomes,
        bytes,
        entries,
    }
}

/// One `/api/v1/sql/local` request: status, rows rendered, wall clock.
///
/// `/local` rather than the transparent endpoint because this measures ONE
/// pod's store; a fan-out would put a second replica's cache in the reading.
async fn post_sql(
    app: &Router,
    body: &serde_json::Value,
) -> (StatusCode, usize, std::time::Duration) {
    let started = std::time::Instant::now();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/sql/local")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let elapsed = started.elapsed();
    let rows = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|body| body.get("row_count").and_then(serde_json::Value::as_u64))
        .unwrap_or(0) as usize;
    (status, rows, elapsed)
}

/// A GET with no allocator mark and no settle sleep — this sweep reads the
/// cache counters, not the peak, and an arm polls hundreds of name keys.
async fn get_timed(app: &Router, path: &str) -> (StatusCode, std::time::Duration) {
    let started = std::time::Instant::now();
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
    let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, started.elapsed())
}

/// The `/api/v1/sql` body for browse `variant` of `index`: one shape, one
/// filter, `BROWSE_ROWS` rows, and a `max_rows_returned` that differs per
/// variant so each is its own cache key without changing what is executed.
fn browse_request(index: &str, variant: usize) -> serde_json::Value {
    serde_json::json!({
        "query": format!(
            "SELECT service, name, span_id, attributes FROM {index} \
             WHERE service = '{SERVICE}' ORDER BY span_id ASC LIMIT {BROWSE_ROWS}"
        ),
        "limits": { "max_rows_returned": BROWSE_ROWS + variant },
    })
}

/// What ONE arm polls between warming the SQL keys and re-polling them.
#[derive(Clone, Copy)]
struct NamePoll {
    /// `/api/services` on every index: one entry per index.
    services: bool,
    /// `/api/services/{service}/operations` for this many DISTINCT services on
    /// every index. Each is its own key, and on a `cardinal_fixture` every
    /// service but [`SERVICE`] has exactly one operation, so these are the
    /// smallest name entries there are — the arm that drives the 256-ENTRY cap
    /// without touching the byte budget.
    operations_per_index: usize,
    /// Re-poll the SQL keys in the OPPOSITE order to the one they were warmed
    /// in. The store evicts the least-recently-used key and promotes hits, but
    /// a forward re-poll starts with the oldest, potentially displaced keys;
    /// each miss can still evict the next unpolled survivor. A backward pass
    /// promotes the newest survivors first, so the pair measures that
    /// order-sensitive behavior under LRU.
    reverse_repoll: bool,
}

/// One arm's reading, in the units the card asks for.
struct ArmReading {
    warm_entries: f64,
    warm_bytes: f64,
    name_entries: f64,
    name_bytes: f64,
    name_inserts: u64,
    /// SQL entries STILL STORED when the name polls are done: the entry count
    /// minus what the name polls inserted. The direct displacement figure, and
    /// deliberately separate from the hit rate below — a store can hold an
    /// entry and still miss on it one pass later (see the LRU promotion and
    /// ordered re-poll note on
    /// [`whether_jaeger_name_entries_displace_sql_entries`]).
    sql_survivors: f64,
    evict_names: u64,
    evict_repoll: u64,
    /// Hits on the first re-poll of every SQL key, and on a second one right
    /// after it — the difference between "this sweep lost its hits" and "this
    /// pod is in steady churn".
    hits_first: u64,
    hits_second: u64,
    warm_ms: f64,
    repoll_ms: f64,
}

/// Warm `indexes x BROWSE_KEYS_PER_INDEX` SQL keys, poll `names` name keys,
/// then re-poll every SQL key and count how many survived.
///
/// The store is reset first, so `warm_entries` and the eviction count are this
/// arm's own and not a previous arm's residue. Snapshots are taken once per
/// phase.
async fn displacement_arm(
    app: &Router,
    snapshotter: &Snapshotter,
    indexes: &[String],
    services: &[String],
    names: NamePoll,
) -> ArmReading {
    siglake_query_server::reset_result_cache_for_test().await;
    // The reset zeroed both levels and published them; drain whatever the
    // previous arm left in the recorder.
    let _ = store_reading(snapshotter, (0.0, 0.0));

    let keys: Vec<(&String, usize)> = indexes
        .iter()
        .flat_map(|index| (0..BROWSE_KEYS_PER_INDEX).map(move |variant| (index, variant)))
        .collect();
    let mut warm_ms = 0.0;
    for (index, variant) in &keys {
        let (status, rows, elapsed) = post_sql(app, &browse_request(index, *variant)).await;
        assert_eq!(status, StatusCode::OK, "warm {index} variant {variant}");
        assert_eq!(
            rows, BROWSE_ROWS,
            "{index}: the browse must render {BROWSE_ROWS} rows or the entries \
             this arm displaces are not the ones it thinks it stored"
        );
        warm_ms += elapsed.as_secs_f64() * 1000.0;
    }
    let warm = store_reading(snapshotter, (0.0, 0.0));

    for index in indexes {
        let base = format!("/api/v1/jaeger/{index}/api");
        if names.services {
            let (status, _) = get_timed(app, &format!("{base}/services")).await;
            assert_eq!(status, StatusCode::OK, "services on {index}");
        }
        for service in services.iter().take(names.operations_per_index) {
            let (status, _) =
                get_timed(app, &format!("{base}/services/{service}/operations")).await;
            assert_eq!(status, StatusCode::OK, "operations of {service} on {index}");
        }
    }
    let polled = store_reading(snapshotter, warm.levels());

    // Two full re-poll passes in the SAME order the warm phase used, which is
    // the order the Jaeger UI and a dashboard both poll in.
    let mut repoll_ms = 0.0;
    let mut passes = Vec::new();
    let mut order: Vec<&(&String, usize)> = keys.iter().collect();
    if names.reverse_repoll {
        order.reverse();
    }
    for pass in 0..2 {
        for (index, variant) in &order {
            let (status, _, elapsed) = post_sql(app, &browse_request(index, *variant)).await;
            assert_eq!(status, StatusCode::OK, "re-poll {index} variant {variant}");
            if pass == 0 {
                repoll_ms += elapsed.as_secs_f64() * 1000.0;
            }
        }
        let carried = passes
            .last()
            .map_or_else(|| polled.levels(), StoreReading::levels);
        passes.push(store_reading(snapshotter, carried));
    }

    let sql_keys = keys.len() as f64;
    ArmReading {
        warm_entries: warm.entries,
        warm_bytes: warm.bytes,
        name_entries: polled.entries,
        name_bytes: polled.bytes,
        name_inserts: polled.count("insert"),
        // LRU promotes hits, but names are inserted AFTER every SQL key with no
        // intervening SQL hits, so each name is more recent than every SQL
        // entry. Eviction therefore cannot remove a name while an older SQL
        // entry survives: below the entry cap this difference IS the surviving
        // SQL count, and at or above it every SQL entry is gone.
        sql_survivors: (polled.entries - polled.count("insert") as f64).max(0.0),
        evict_names: warm.count("evict") + polled.count("evict"),
        evict_repoll: passes.iter().map(|p| p.count("evict")).sum(),
        hits_first: passes[0].count("hit"),
        hits_second: passes[1].count("hit"),
        warm_ms: warm_ms / sql_keys,
        repoll_ms: repoll_ms / sql_keys,
    }
}

/// Build `DISPLACEMENT_INDEXES` trace indexes of one cardinality in one
/// warehouse, and return their ids.
async fn many_index_warehouse(
    ice: &IcebergContext,
    prefix: &str,
    traces: u64,
    traces_per_file: u64,
    names: u64,
    pad: usize,
) -> Vec<String> {
    let mut indexes = Vec::new();
    for i in 0..DISPLACEMENT_INDEXES {
        // Underscores: the index id is the SQL table name, and an unquoted
        // identifier is what the browse above spells.
        let index = format!("{prefix}_{i:02}");
        let config = IndexConfig {
            index_id: index.clone(),
            doc_mapping: builtin_traces_template().doc_mapping,
            retention: None,
            index_at_flush: None,
        };
        ice.create_index(&config).await.unwrap();
        append_cardinal(
            ice,
            &config,
            &index,
            traces,
            16,
            traces_per_file,
            names,
            pad,
        )
        .await;
        indexes.push(index);
    }
    indexes
}

/// Do Jaeger name entries cost `/api/v1/sql` its hit rate on a many-index pod
/// (#2398)?
///
/// #2302 priced ONE name entry — 512 KiB, an eighth of the shared 4 MiB byte
/// budget — and left the AGGREGATE unpriced: a pod serving N indexes holds up
/// to `2N + (services per index)` name keys, one per (index, list, service,
/// ceilings). The store is shared with `/api/v1/sql` and evicts on whichever of
/// 256 entries and 4 MiB binds first, so the question is whether name polls
/// turn `outcome=evict` into churn that costs SQL its hits.
///
/// Five arms over two warehouses of eight indexes each, every arm from a RESET
/// store so its entry count and eviction count are its own:
///
///   - `control` — 64 SQL keys warmed and re-polled, no name traffic. The
///     baseline: 64 hits, no evictions.
///   - `2 lists/index` — what a Jaeger UI actually polls: the services list and
///     one service's operations on every index, 16 short-name entries.
///   - `24 ops/index`, `39 ops/index` — service-scoped operations keys, the
///     smallest entries there are (one name each), driving the 256-ENTRY cap
///     with the byte budget nowhere near.
///   - `wide services` — the byte cap instead: 5,000 services of 96 bytes per
///     index is ~488 KiB of arena per entry, so eight of them are the whole
///     4 MiB budget.
///   - `… reverse` — the same two displacements with the re-poll walking the
///     SQL keys backwards. Same store, same evicted entries, and the hit rate
///     is what moves.
///
/// `ev name` and `ev sql` split
/// `siglake_query_sql_result_cache_requests_total{outcome="evict"}` by the phase
/// that caused it, and `name MiB` is `siglake_query_sql_result_cache_bytes` —
/// the two series the card asks for, isolated per arm here because the shared
/// panel (**SQL result cache by outcome**,
/// `deploy/grafana/siglake-overview.json`) cannot attribute them and no public
/// label was added to make it.
///
/// TWO COLUMNS, NOT ONE, because displacement and hit rate are different
/// quantities here. `surv` is how many SQL entries are still stored when the
/// name polls are done; `hit 1` and `hit 2` are how many of the 64 keys the two
/// re-poll passes actually got served. They can come apart under LRU even though
/// `SqlResultCache::get` promotes a hit: a forward re-poll starts with the
/// least-recently-used, potentially displaced keys, and each miss's insert can
/// evict the next unpolled survivor. A reverse pass starts with the newest
/// survivors and promotes them before it reaches the missing keys. LRU does
/// not by itself establish whether ordered re-poll thrash disappears. Read
/// `surv` for what the name polls cost the store and `hit 1` for what that cost
/// the caller on this workload.
///
/// `--test-threads=1`, like every measurement in this file: the store, the
/// metrics recorder and the allocator are process-wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
async fn whether_jaeger_name_entries_displace_sql_entries() {
    siglake_storage::preset_query_memory_pool_bytes(POOL_BYTES);
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("install debugging recorder");

    // Narrow: 100 short service names per index, so a name entry is ~1 KiB and
    // the 256-entry cap is the only one that can bind.
    const NARROW_TRACES: u64 = 1_250;
    const NARROW_PER_FILE: u64 = 250;
    const NARROW_NAMES: u64 = 100;
    // Wide: 5,000 services of 96 bytes, inside a packaged pod's 10,237-name
    // render ceiling and inside the 512 KiB one entry may retain, so the entry
    // is admitted and the BYTE budget is what binds.
    const WIDE_TRACES: u64 = 800;
    const WIDE_PER_FILE: u64 = 200;
    const WIDE_NAMES: u64 = 5_000;
    const WIDE_PAD: usize = 96;

    let narrow_tmp = tempfile::tempdir().unwrap();
    let narrow_ice = Arc::new(
        IcebergContext::open(&narrow_tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let wide_tmp = tempfile::tempdir().unwrap();
    let wide_ice = Arc::new(
        IcebergContext::open(&wide_tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let build_started = std::time::Instant::now();
    let narrow_indexes = many_index_warehouse(
        &narrow_ice,
        "traces_narrow",
        NARROW_TRACES,
        NARROW_PER_FILE,
        NARROW_NAMES,
        0,
    )
    .await;
    let wide_indexes = many_index_warehouse(
        &wide_ice,
        "traces_wide",
        WIDE_TRACES,
        WIDE_PER_FILE,
        WIDE_NAMES,
        WIDE_PAD,
    )
    .await;
    let build_elapsed = build_started.elapsed();

    let app_for = |ice: &Arc<IcebergContext>| {
        router(
            AppState::new(Arc::clone(ice), AuthConfig::open())
                .with_limits(siglake_query_server::ServerLimits {
                    admission_budget_bytes: DISPLACEMENT_BUDGET,
                    admission_wait_timeout: std::time::Duration::from_millis(500),
                    ..Default::default()
                })
                .with_query_scan(QueryScanConfig {
                    target_partitions: Some(1),
                    file_concurrency_limit: Some(1),
                    ..Default::default()
                }),
        )
    };
    let narrow_app = app_for(&narrow_ice);
    let wide_app = app_for(&wide_ice);
    let narrow_services: Vec<String> = (0..NARROW_NAMES).map(|i| svc_name(i, 0)).collect();
    let wide_services: Vec<String> = (0..WIDE_NAMES).map(|i| svc_name(i, WIDE_PAD)).collect();

    println!(
        "\n=== #2398: do Jaeger name entries displace /api/v1/sql entries? ===\n\
         {} indexes per warehouse, {} SQL keys per arm ({BROWSE_KEYS_PER_INDEX} per index, \
         {BROWSE_ROWS} rows each); narrow = {NARROW_NAMES} names of 8 B over {} span rows, \
         wide = {WIDE_NAMES} names of {WIDE_PAD} B over {} span rows; fixtures built in {:.1}s\n\
         store: 256 entries / 4 MiB shared; one name entry may retain 512 KiB; \
         result caches enabled: {}\n\
         {:>15}  {:>3}  {:>7}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>7}  {:>7}  {:>6}  \
         {:>6}  {:>6}  {:>7}  {:>9}",
        DISPLACEMENT_INDEXES,
        DISPLACEMENT_INDEXES as usize * BROWSE_KEYS_PER_INDEX,
        NARROW_TRACES * 16,
        WIDE_TRACES * 16,
        build_elapsed.as_secs_f64(),
        siglake_storage::iceberg::result_caches_enabled(),
        "arm",
        "rep",
        "sqlkeys",
        "warm ent",
        "warm MiB",
        "name ins",
        "name ent",
        "name MiB",
        "ev name",
        "ev sql",
        "surv",
        "hit 1",
        "hit 2",
        "warm ms",
        "repoll ms",
    );

    let arms: &[(&str, bool, NamePoll)] = &[
        (
            "control",
            false,
            NamePoll {
                services: false,
                operations_per_index: 0,
                reverse_repoll: false,
            },
        ),
        (
            "2 lists/index",
            false,
            NamePoll {
                services: true,
                operations_per_index: 1,
                reverse_repoll: false,
            },
        ),
        (
            "24 ops/index",
            false,
            NamePoll {
                services: true,
                operations_per_index: 24,
                reverse_repoll: false,
            },
        ),
        // The same displacement read backwards: identical store, identical
        // eight evicted entries, only the re-poll order differs.
        (
            "24 ops, reverse",
            false,
            NamePoll {
                services: true,
                operations_per_index: 24,
                reverse_repoll: true,
            },
        ),
        (
            "39 ops/index",
            false,
            NamePoll {
                services: true,
                operations_per_index: 39,
                reverse_repoll: false,
            },
        ),
        (
            "wide services",
            true,
            NamePoll {
                services: true,
                operations_per_index: 0,
                reverse_repoll: false,
            },
        ),
        (
            "wide, reverse",
            true,
            NamePoll {
                services: true,
                operations_per_index: 0,
                reverse_repoll: true,
            },
        ),
    ];

    for (label, wide, names) in arms {
        for rep in 1..=DISPLACEMENT_REPEATS {
            let (app, indexes, services) = if *wide {
                (&wide_app, &wide_indexes, &wide_services)
            } else {
                (&narrow_app, &narrow_indexes, &narrow_services)
            };
            let reading = displacement_arm(app, &snapshotter, indexes, services, *names).await;
            println!(
                "{:>15}  {:>3}  {:>7}  {:>8.0}  {:>8.2}  {:>8}  {:>8.0}  {:>8.2}  {:>7}  \
                 {:>7}  {:>6.0}  {:>6}  {:>6}  {:>7.1}  {:>9.1}",
                label,
                rep,
                indexes.len() * BROWSE_KEYS_PER_INDEX,
                reading.warm_entries,
                reading.warm_bytes / (1024.0 * 1024.0),
                reading.name_inserts,
                reading.name_entries,
                reading.name_bytes / (1024.0 * 1024.0),
                reading.evict_names,
                reading.evict_repoll,
                reading.sql_survivors,
                reading.hits_first,
                reading.hits_second,
                reading.warm_ms,
                reading.repoll_ms,
            );
        }
    }
}
