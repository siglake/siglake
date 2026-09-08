//! On-demand process profiling, mounted on the `--metrics-bind` server.
//!
//! Compiled only under the crate's off-by-default `profiling` feature, and
//! even then every route answers `404` unless `SIGLAKE_PPROF_ENABLED=1`. The
//! two gates are deliberate and not redundant: the feature keeps the profiler
//! out of a shipped binary, and the env var keeps a *deliberately* profiling
//! image inert until an operator turns it on. Neither alone is enough — a
//! feature flag is too easy to ship by accident, and an env var cannot remove
//! the code.
//!
//! Why this server and not the query server's authenticated `/debug/*` routes:
//! this one is the only HTTP surface every role shares ([`crate::metrics::init`]
//! is called by `siglake-cli`, `siglake-query-server` and `siglake-operator`),
//! so one mount point profiles the ingester, the compactor and the query tier.
//! Its exposure model is the same as `/metrics`: node-local. The AWS bench
//! stack opens only 8088, 8089 and 22, so 9100/9105 are reachable from the node
//! and its peers and nowhere else. **Do not mount these routes on the public
//! API port** — a CPU profile is a stack-trace oracle and the heap route names
//! allocation sites.
//!
//! ## What each route is for
//!
//! - `/debug/pprof/profile?seconds=N` — on-CPU samples at 99 Hz, returned as
//!   gzipped pprof protobuf. Symbolized **in-process**, so the response is
//!   self-contained: it carries function names (and file:line, when the binary
//!   kept its DWARF) without the caller needing the binary. That matters here
//!   because the bench node and its bucket are destroyed at the end of a round.
//! - `/debug/pprof/heap` — a jemalloc heap profile in `jeprof` text format.
//!   Needs the binary to symbolize, which is why a profiling round records its
//!   image digest. The process must be started with
//!   `_RJEM_MALLOC_CONF=prof:true,prof_active:true` — **prefixed**, because
//!   `tikv-jemalloc-sys` builds jemalloc with a prefixed symbol namespace and
//!   ignores the plain `MALLOC_CONF` entirely. Get that wrong and this route
//!   answers 412 while the other two look perfectly healthy, which is exactly
//!   how it went unnoticed until the image was run locally (2026-09-08).
//! - `/debug/pprof/runtime` — tokio runtime counters as JSON. A CPU profile
//!   samples only threads that are *running*, so an await-blocked query looks
//!   free in it; the derived `worker_idle_duration_ns` / `busy_ratio` here are
//!   that missing half.
//!
//! ## Ordering constraint
//!
//! Never take a CPU window and a heap dump concurrently: the heap dump walks
//! allocator state while the CPU profiler's `SIGPROF` handler is interrupting
//! threads. `CPU_PROFILE_IN_FLIGHT` makes concurrent CPU requests fail fast
//! rather than interleave, and the harness helper
//! (`quickwit-testing/bench/lib/profile_capture.sh`) sequences heap *after* the
//! CPU window closes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::Query;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

/// Env var that arms the routes. Read once, at router-construction time.
pub const PPROF_ENABLED_ENV: &str = "SIGLAKE_PPROF_ENABLED";

/// Sampling frequency for the CPU profiler, in Hz.
///
/// 99 rather than 100 to avoid phase-locking with anything on a 10 ms period
/// (timer ticks, the metrics scrape, a 100 Hz poll loop), which would
/// systematically over-sample whatever runs on that boundary.
const SAMPLE_HZ: i32 = 99;

/// Bounds on `?seconds=`. The lower bound keeps a window from being too short
/// to hold samples; the upper bound keeps a stuck client from pinning the
/// profiler (and its `SIGPROF` overhead) on indefinitely.
const MIN_SECONDS: u64 = 1;
const MAX_SECONDS: u64 = 600;
const DEFAULT_SECONDS: u64 = 30;

/// One CPU profile at a time. Two overlapping `pprof` guards in one process
/// produce a corrupt profile rather than an error, so the second request is
/// refused instead.
static CPU_PROFILE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

/// Pure resolver twin for [`PPROF_ENABLED_ENV`]: tests drive this, never the
/// environment (the harness runs a binary's tests on parallel threads, so env
/// mutation races — see `scripts/check-set-var.py`).
///
/// Deliberately strict. Only `1` and `true` arm the profiler; anything else,
/// including `yes`, `on` and a stray empty assignment, leaves it off. An
/// operator who misspells the value gets a disarmed profiler and a round that
/// refuses at the readback gate, which is a far cheaper failure than a round
/// that silently captures nothing.
pub fn pprof_enabled_from(raw: Option<&str>) -> bool {
    matches!(
        raw.map(str::trim)
            .unwrap_or("")
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true"
    )
}

/// Clamp a requested window to `MIN_SECONDS..=MAX_SECONDS` (1..=600 seconds).
///
/// Clamps rather than rejects: a profiling round asking for a longer window
/// than the ceiling should still get a profile. The response carries the
/// window actually used in `X-Profile-Seconds` so the caller is never guessing.
pub fn clamp_seconds(requested: Option<u64>) -> u64 {
    requested
        .unwrap_or(DEFAULT_SECONDS)
        .clamp(MIN_SECONDS, MAX_SECONDS)
}

#[derive(Debug, Deserialize)]
pub struct ProfileParams {
    seconds: Option<u64>,
}

/// The `/debug/pprof/*` routes, or an empty router when the env gate is off.
///
/// Returning an empty router (rather than routes that answer 403) is what makes
/// the harness gate meaningful: a `404` from this path means "this build or this
/// process cannot profile", which is exactly the condition the round must refuse
/// on. See the `PPROF_OK` gate in `quickwit-testing/bench/siglake_aws_run.sh`.
pub fn routes() -> Router {
    if !pprof_enabled_from(std::env::var(PPROF_ENABLED_ENV).ok().as_deref()) {
        tracing::debug!(
            env = PPROF_ENABLED_ENV,
            "profiling compiled in but not armed; /debug/pprof/* not mounted"
        );
        return Router::new();
    }
    tracing::warn!(
        "profiling routes ARMED on the metrics port: /debug/pprof/{{profile,heap,runtime}}"
    );
    Router::new()
        .route("/debug/pprof/profile", get(cpu_profile))
        .route("/debug/pprof/heap", get(heap_profile))
        .route("/debug/pprof/runtime", get(runtime_stats))
}

/// Collect on-CPU samples for the requested window and return gzipped pprof
/// protobuf.
///
/// Holds the request open for the whole window — that is the contract the
/// harness relies on, since it backgrounds this call, runs a benchmark stage,
/// and then waits for the response.
async fn cpu_profile(Query(params): Query<ProfileParams>) -> Response {
    let seconds = clamp_seconds(params.seconds);

    if CPU_PROFILE_IN_FLIGHT.swap(true, Ordering::SeqCst) {
        return (
            StatusCode::CONFLICT,
            "a CPU profile is already in flight; concurrent profiles would corrupt both\n",
        )
            .into_response();
    }
    // Everything below must clear the flag, including the error paths.
    let result = collect_cpu_profile(seconds).await;
    CPU_PROFILE_IN_FLIGHT.store(false, Ordering::SeqCst);

    match result {
        Ok(body) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/octet-stream"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"profile.pb.gz\"",
                ),
            ],
            [("x-profile-seconds", seconds.to_string())],
            Body::from(body),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, seconds, "CPU profile failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("CPU profile failed: {e}\n"),
            )
                .into_response()
        }
    }
}

async fn collect_cpu_profile(seconds: u64) -> anyhow::Result<Vec<u8>> {
    use pprof::protos::Message;

    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(SAMPLE_HZ)
        // Sampling the profiler's own signal plumbing and libc's allocator
        // internals adds frames that are never actionable.
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()?;

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    // `report().pprof()` is CPU-bound symbolization over the whole sample set,
    // so it goes to a blocking thread rather than stalling a runtime worker
    // that the profiled process still needs.
    let report = guard.report().build()?;
    drop(guard);
    let body = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let profile = report.pprof()?;
        let mut encoded = Vec::new();
        profile.write_to_vec(&mut encoded)?;
        gzip(&encoded)
    })
    .await??;
    Ok(body)
}

/// gzip the pprof payload, matching what `pprof`/`go tool pprof` expect from a
/// `?seconds=` endpoint.
fn gzip(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    use std::io::Write;
    // `flate2` arrives transitively via `inferno`-free `pprof`'s protobuf
    // codec; going through `GzEncoder` keeps the framing identical to upstream
    // pprof HTTP endpoints.
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(raw)?;
    Ok(encoder.finish()?)
}

/// Dump a jemalloc heap profile and return it as `jeprof` text.
///
/// Requires BOTH that the binary linked a `tikv-jemallocator` built with
/// `--enable-prof` (its `profiling` feature) and that the process started with
/// `_RJEM_MALLOC_CONF=prof:true,prof_active:true` — jemalloc reads that once, at
/// startup, so profiling cannot be turned on later, and only allocations made
/// while sampling was active appear in a dump. Either missing and this answers
/// 412 rather than a misleading near-empty profile.
async fn heap_profile() -> Response {
    match tokio::task::spawn_blocking(collect_heap_profile).await {
        Ok(Ok(body)) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
                (
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"heap.jeprof\"",
                ),
            ],
            Body::from(body),
        )
            .into_response(),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "heap profile failed");
            (
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "heap profile unavailable: {e}\n\
                     Needs a binary built with `--features profiling` (so \
                     tikv-jemallocator gets --enable-prof) AND \
                     _RJEM_MALLOC_CONF=prof:true,prof_active:true at process start.\n"
                ),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("heap profile task failed: {e}\n"),
        )
            .into_response(),
    }
}

fn collect_heap_profile() -> anyhow::Result<Vec<u8>> {
    use std::ffi::CString;

    use tikv_jemalloc_ctl::raw;

    // `opt.prof` reflects the _RJEM_MALLOC_CONF the process actually started with.
    // Checking it first turns "profiling was never enabled" into a precise
    // message instead of an opaque mallctl failure further down.
    let prof_built: bool = unsafe { raw::read(b"opt.prof\0") }
        .map_err(|e| anyhow::anyhow!("read opt.prof (jemalloc built without profiling?): {e}"))?;
    anyhow::ensure!(
        prof_built,
        "jemalloc reports opt.prof=false (_RJEM_MALLOC_CONF is missing prof:true)"
    );

    // Sampling must ALREADY have been running. A jemalloc heap profile is a
    // snapshot of currently-live *sampled* allocations, and only allocations
    // made while `prof.active` was true are ever sampled — so arming it here
    // and dumping immediately would report an almost-empty heap and, worse,
    // read as "this stage allocated nothing".
    //
    // That is why the round starts the process with `prof_active:true` and this
    // function neither activates nor deactivates: toggling it off after a dump
    // would silently blank every later stage's profile.
    let active: bool = unsafe { raw::read(b"prof.active\0") }
        .map_err(|e| anyhow::anyhow!("read prof.active: {e}"))?;
    anyhow::ensure!(
        active,
        "jemalloc sampling is inactive (prof.active=false), so a dump would be empty; \
         start the process with _RJEM_MALLOC_CONF=prof:true,prof_active:true"
    );

    let dump = tempfile::Builder::new()
        .prefix("siglake-heap-")
        .suffix(".jeprof")
        .tempfile()?;
    let path = CString::new(dump.path().as_os_str().as_encoded_bytes())?;
    unsafe { raw::write(b"prof.dump\0", path.as_ptr()) }
        .map_err(|e| anyhow::anyhow!("prof.dump: {e}"))?;

    Ok(std::fs::read(dump.path())?)
}

/// Worker-idle time and busy ratio, derived rather than read.
///
/// tokio-metrics 0.5 exposes no idle-duration field, so compute it: total
/// worker capacity over the interval is `elapsed * workers`, and whatever is
/// not `total_busy_duration` is workers parked with nothing runnable. This is
/// the number an on-CPU profile structurally cannot show — a stage that is
/// 90% idle is waiting on S3 or the catalog, and no amount of staring at the
/// CPU profile's hot frames will say so.
///
/// Returns `(idle_nanos, busy_ratio)`. `busy_ratio` is `None` when capacity is
/// zero (a zero-length interval, or a runtime reporting no workers), because a
/// ratio over zero capacity is undefined rather than 0.0.
pub fn worker_idle(elapsed: Duration, workers: usize, total_busy: Duration) -> (u128, Option<f64>) {
    let capacity = elapsed.as_nanos() * workers as u128;
    let busy = total_busy.as_nanos();
    // Saturating: busy can exceed a naive capacity estimate when workers are
    // added mid-interval, and an underflow panic here would take down the
    // profiling endpoint during a round.
    let idle = capacity.saturating_sub(busy);
    let ratio = if capacity == 0 {
        None
    } else {
        Some(busy as f64 / capacity as f64)
    };
    (idle, ratio)
}

/// Tokio runtime counters for the current runtime.
///
/// Aggregate per-runtime, not per-task: this says *that* tasks stalled and for
/// how long, not which one. Per-task attribution needs a `console-subscriber`
/// build and a live `tokio-console` session, which an unattended round cannot
/// capture.
///
/// Most of the interesting counters (poll counts and durations, steal counts,
/// queue depths) are `#[cfg(tokio_unstable)]` in tokio-metrics, so a build
/// without `--cfg tokio_unstable` gets only the always-available set. Rather
/// than fail, the response reports which set it is via `tokio_unstable`, so a
/// reader is never left guessing whether a missing key means "zero" or "not
/// compiled in".
async fn runtime_stats() -> Response {
    let handle = tokio::runtime::Handle::current();
    let monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    // `intervals()` yields cumulative-since-last-poll; the first item covers
    // since-runtime-start, which is what a per-stage delta wants.
    let Some(m) = monitor.intervals().next() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "runtime monitor produced no sample\n",
        )
            .into_response();
    };

    let (idle_ns, busy_ratio) = worker_idle(m.elapsed, m.workers_count, m.total_busy_duration);
    // `mut` is used only by the `cfg(tokio_unstable)` block below, which is
    // compiled out of a build without the flag.
    #[allow(unused_mut)]
    let mut out = serde_json::json!({
        "tokio_unstable": cfg!(tokio_unstable),
        "elapsed_ns": m.elapsed.as_nanos(),
        "workers_count": m.workers_count,
        "live_tasks_count": m.live_tasks_count,
        "global_queue_depth": m.global_queue_depth,
        "total_park_count": m.total_park_count,
        "total_busy_duration_ns": m.total_busy_duration.as_nanos(),
        "max_busy_duration_ns": m.max_busy_duration.as_nanos(),
        "min_busy_duration_ns": m.min_busy_duration.as_nanos(),
        // Derived; see `worker_idle`.
        "worker_idle_duration_ns": idle_ns,
        "busy_ratio": busy_ratio,
    });

    // `serde_json::Value` has no `From<u128>`, so durations go through `json!`
    // (which serializes u128 fine) rather than `.into()`.
    #[cfg(tokio_unstable)]
    {
        let obj = out.as_object_mut().expect("json object");
        for (k, v) in [
            ("total_polls_count", serde_json::json!(m.total_polls_count)),
            (
                "mean_poll_duration_ns",
                serde_json::json!(m.mean_poll_duration.as_nanos()),
            ),
            ("total_steal_count", serde_json::json!(m.total_steal_count)),
            ("total_noop_count", serde_json::json!(m.total_noop_count)),
            (
                "total_overflow_count",
                serde_json::json!(m.total_overflow_count),
            ),
            (
                "total_local_queue_depth",
                serde_json::json!(m.total_local_queue_depth),
            ),
            (
                "blocking_queue_depth",
                serde_json::json!(m.blocking_queue_depth),
            ),
            (
                "blocking_threads_count",
                serde_json::json!(m.blocking_threads_count),
            ),
            (
                "idle_blocking_threads_count",
                serde_json::json!(m.idle_blocking_threads_count),
            ),
            (
                "budget_forced_yield_count",
                serde_json::json!(m.budget_forced_yield_count),
            ),
            (
                "io_driver_ready_count",
                serde_json::json!(m.io_driver_ready_count),
            ),
        ] {
            obj.insert(k.to_string(), v);
        }
    }

    Json(out).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_one_and_true_arm_the_profiler() {
        assert!(pprof_enabled_from(Some("1")));
        assert!(pprof_enabled_from(Some("true")));
        assert!(pprof_enabled_from(Some("TRUE")));
        assert!(pprof_enabled_from(Some("  true  ")));
    }

    /// The default has to be off, and a misspelling has to read as off rather
    /// than as on: a round that thinks it is profiling but is not wastes a
    /// full round's spend.
    #[test]
    fn anything_else_leaves_it_disarmed() {
        assert!(!pprof_enabled_from(None));
        assert!(!pprof_enabled_from(Some("")));
        assert!(!pprof_enabled_from(Some("0")));
        assert!(!pprof_enabled_from(Some("false")));
        assert!(!pprof_enabled_from(Some("yes")));
        assert!(!pprof_enabled_from(Some("on")));
        assert!(!pprof_enabled_from(Some("2")));
    }

    #[test]
    fn seconds_defaults_and_clamps() {
        assert_eq!(clamp_seconds(None), DEFAULT_SECONDS);
        assert_eq!(clamp_seconds(Some(120)), 120);
        assert_eq!(clamp_seconds(Some(0)), MIN_SECONDS);
        assert_eq!(clamp_seconds(Some(MAX_SECONDS + 1)), MAX_SECONDS);
    }

    /// A gzip payload has to be what `pprof` will accept, so assert the magic
    /// bytes rather than just a non-empty buffer.
    #[test]
    fn gzip_emits_a_gzip_stream() {
        let out = gzip(b"some pprof protobuf").expect("gzip");
        assert_eq!(&out[..2], &[0x1f, 0x8b], "gzip magic");
    }

    /// Idle is capacity minus busy, where capacity counts every worker.
    /// Getting the worker multiplier wrong is the easy mistake here, and it
    /// would make a saturated 8-worker runtime look 87% idle.
    #[test]
    fn idle_is_capacity_across_all_workers_minus_busy() {
        let (idle, ratio) = worker_idle(Duration::from_secs(10), 4, Duration::from_secs(10));
        assert_eq!(idle, Duration::from_secs(30).as_nanos());
        assert_eq!(ratio, Some(0.25));
    }

    #[test]
    fn fully_busy_runtime_reports_no_idle() {
        let (idle, ratio) = worker_idle(Duration::from_secs(5), 2, Duration::from_secs(10));
        assert_eq!(idle, 0);
        assert_eq!(ratio, Some(1.0));
    }

    /// A ratio over zero capacity is undefined, not 0.0 — reporting 0.0 would
    /// read as "completely idle" for an interval that measured nothing.
    #[test]
    fn zero_capacity_has_no_ratio() {
        assert_eq!(worker_idle(Duration::ZERO, 8, Duration::ZERO), (0, None));
        assert_eq!(
            worker_idle(Duration::from_secs(10), 0, Duration::ZERO),
            (0, None)
        );
    }

    /// Workers can join mid-interval, so busy may exceed the capacity implied
    /// by the end-of-interval worker count. That must clamp, not panic: this
    /// endpoint is polled during a paid round.
    #[test]
    fn busy_above_capacity_saturates_instead_of_panicking() {
        let (idle, ratio) = worker_idle(Duration::from_secs(1), 1, Duration::from_secs(5));
        assert_eq!(idle, 0);
        assert_eq!(ratio, Some(5.0));
    }
}
