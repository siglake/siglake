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
//! threads. [`ProfileAdmission`] is one process-wide ticket covering BOTH
//! routes, so any second capture — CPU beside CPU, heap beside CPU, either way
//! round — is refused with `409` rather than interleaved. The harness helper
//! that sequences heap *after* the CPU window closes lives in the benchmark
//! repository (`siglake-benchmarks`, its own pull request), so in a healthy
//! round the ticket is never contended; it is there for the round that is not.
//!
//! The ticket is released on drop, which is what makes it correct under
//! cancellation: a harness that hangs up mid-window, or a `curl` killed at its
//! own timeout, drops the request future and the profiler is usable again. An
//! explicit "clear the flag after the await" would be skipped on exactly that
//! path and leave the endpoint permanently busy for the rest of the round.

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

/// One profile at a time, of either kind. Two overlapping `pprof` guards in one
/// process produce a corrupt profile rather than an error, and a heap dump
/// walking allocator state while `SIGPROF` interrupts threads corrupts both, so
/// the second request is refused instead.
static PROFILE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

/// What a refused capture says. One message for both routes, because the caller
/// cannot tell from its own request which kind of capture is holding the
/// ticket.
const IN_FLIGHT_BODY: &str =
    "a profile is already in flight; concurrent CPU and heap captures would corrupt both\n";

/// Exclusive admission to the profiler, released on drop.
///
/// Drop rather than an explicit release is the whole point: both handlers do
/// their work across an `.await`, so a dropped request future (client hangs up,
/// harness times out, the runtime shuts down) must not leave the endpoint busy.
/// A `store(false)` placed after the await is unreachable on precisely that
/// path, which is how the endpoint could wedge for the rest of a round.
///
/// Not `Clone`, not constructible except through [`Self::try_acquire`], so the
/// only way to hold it is to have won it.
struct ProfileAdmission;

impl ProfileAdmission {
    /// The ticket, or `None` when a capture is already in flight.
    fn try_acquire() -> Option<Self> {
        if PROFILE_IN_FLIGHT.swap(true, Ordering::SeqCst) {
            None
        } else {
            Some(Self)
        }
    }
}

impl Drop for ProfileAdmission {
    fn drop(&mut self) {
        PROFILE_IN_FLIGHT.store(false, Ordering::SeqCst);
    }
}

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

/// Bounds on the runtime route's `?seconds=`. Far smaller than the CPU
/// profiler's: this is a metrics delta, not a sample set, and the harness takes
/// it at the END of a stage window, so a long wait here only delays the round.
const RUNTIME_MIN_SECONDS: u64 = 1;
const RUNTIME_MAX_SECONDS: u64 = 60;
const RUNTIME_DEFAULT_SECONDS: u64 = 2;

// The runtime window must stay well under the CPU profiler's, since the
// harness reads it at the END of a stage window and a long wait here only
// delays the round. Compile-time, so the two ceilings cannot drift together.
const _: () = assert!(RUNTIME_MAX_SECONDS < MAX_SECONDS);
const _: () = assert!(RUNTIME_MIN_SECONDS >= 1);

#[derive(Debug, Deserialize)]
pub struct ProfileParams {
    seconds: Option<u64>,
}

/// Clamp the runtime sampling window to `1..=60` seconds, defaulting to 2.
///
/// Separate from [`clamp_seconds`] on purpose: sharing the CPU profiler's
/// 600-second ceiling would let one `?seconds=` value mean two very different
/// things depending on the route.
pub fn clamp_runtime_window(requested: Option<u64>) -> u64 {
    requested
        .unwrap_or(RUNTIME_DEFAULT_SECONDS)
        .clamp(RUNTIME_MIN_SECONDS, RUNTIME_MAX_SECONDS)
}

/// The `/debug/pprof/*` routes, or an empty router when the env gate is off.
///
/// Returning an empty router (rather than routes that answer 403) is what makes
/// a harness readback gate meaningful: a `404` from this path means "this build
/// or this process cannot profile", which is exactly the condition a round must
/// refuse on rather than run and deliver nothing. The gate itself is the
/// benchmark repository's (`siglake-benchmarks`, its own pull request).
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

    let Some(_admission) = ProfileAdmission::try_acquire() else {
        return (StatusCode::CONFLICT, IN_FLIGHT_BODY).into_response();
    };
    // Held for the whole operation and released on drop, so the error paths and
    // a request future dropped mid-window both free the profiler.
    let result = collect_cpu_profile(seconds).await;

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
///
/// Shares [`ProfileAdmission`] with the CPU route: `prof.dump` walks allocator
/// state, which is not safe beside the CPU profiler's `SIGPROF` handler.
async fn heap_profile() -> Response {
    let Some(admission) = ProfileAdmission::try_acquire() else {
        return (StatusCode::CONFLICT, IN_FLIGHT_BODY).into_response();
    };

    // The ticket moves INTO the blocking closure instead of being held by this
    // future. `spawn_blocking` work is not cancelled when the future awaiting
    // it is dropped, so a ticket held out here would be released while
    // `prof.dump` was still running — and a CPU capture could then start on top
    // of a live allocator walk, which is the exact overlap the ticket exists to
    // prevent. Released when the dump returns, cancelled request or not.
    let dumped = tokio::task::spawn_blocking(move || {
        let _admission = admission;
        collect_heap_profile()
    })
    .await;

    match dumped {
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
///
/// Blocks for `?seconds=` (default 2, clamped to 1..=60) because everything
/// here except the point-in-time gauges is a DELTA over a sampling interval;
/// see the comment in the body.
async fn runtime_stats(Query(params): Query<ProfileParams>) -> Response {
    let seconds = clamp_runtime_window(params.seconds);
    let handle = tokio::runtime::Handle::current();
    let monitor = tokio_metrics::RuntimeMonitor::new(&handle);
    let mut intervals = monitor.intervals();

    // MEASURE OVER A REAL WINDOW. Every duration and count in `RuntimeMetrics`
    // is a DELTA over the sampling interval, and `intervals()` starts that
    // interval when it is called -- so taking the first item immediately
    // yields a window of a few microseconds in which nothing happened. The
    // 2026-09-08 profiling round shipped exactly that: `elapsed_ns: 5527`,
    // every counter 0, and `busy_ratio: 0.0` on an ingester that was running
    // flat out. It reads as "completely idle", which is worse than useless.
    //
    // So: burn the first item to establish the baseline, sleep, then take a
    // genuine `seconds`-long interval.
    let _ = intervals.next();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let Some(m) = intervals.next() else {
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

    /// The admission ticket is process-global and `cargo test` runs a binary's
    /// tests on parallel threads, so the tests that touch it have to take turns:
    /// otherwise one test reads another's legitimately-held ticket as the
    /// refusal it was asserting, or as the leak it was asserting the absence of.
    static PROFILE_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Poisoning is ignored on purpose. The mutex guards nothing but test
    /// ordering, so a panicking test must not turn every later one red and hide
    /// the one real failure.
    fn serialized() -> std::sync::MutexGuard<'static, ()> {
        PROFILE_TEST_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn multi_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime")
    }

    #[test]
    fn admission_is_exclusive_and_releases_on_drop() {
        let _serial = serialized();

        let held = ProfileAdmission::try_acquire().expect("uncontended");
        assert!(
            ProfileAdmission::try_acquire().is_none(),
            "a second capture must be refused while the first holds the ticket"
        );
        drop(held);
        assert!(
            ProfileAdmission::try_acquire().is_some(),
            "the ticket must be free again once the holder drops"
        );
    }

    /// THE CANCELLATION REGRESSION.
    ///
    /// The first version cleared its flag only after awaiting the collector, so
    /// a request future dropped inside the window — a harness that hangs up, a
    /// `curl` killed at its own timeout — never ran the clear, and every later
    /// capture for the rest of the round got a `409`. A paid round would then
    /// deliver one profile and six refusals.
    #[test]
    fn a_dropped_cpu_request_leaves_the_endpoint_usable() {
        let _serial = serialized();
        let runtime = multi_thread_runtime();

        runtime.block_on(async {
            // The longest window the route allows, so nothing but the drop can
            // end it: if this ever completes, the test is measuring the wrong
            // thing and the assertion below says so.
            let capturing = cpu_profile(Query(ProfileParams {
                seconds: Some(MAX_SECONDS),
            }));
            let outcome = tokio::time::timeout(Duration::from_millis(150), capturing).await;
            assert!(
                outcome.is_err(),
                "a {MAX_SECONDS}s window cannot have finished in 150ms"
            );

            // `timeout` drops the request future before it returns, so by here
            // the ticket must already be free.
            assert!(
                !PROFILE_IN_FLIGHT.load(Ordering::SeqCst),
                "the dropped request left the profiler marked busy"
            );
            let next = heap_profile().await;
            assert_ne!(
                next.status(),
                StatusCode::CONFLICT,
                "a capture after a dropped one must not be refused as overlapping"
            );
        });
    }

    /// THE OVERLAP REGRESSION, both directions.
    ///
    /// `heap_profile` had no exclusion at all: it went straight to
    /// `spawn_blocking(collect_heap_profile)`, so a heap dump could walk
    /// allocator state while the CPU profiler's `SIGPROF` handler was
    /// interrupting threads — the one ordering the module's header forbids.
    ///
    /// Stands in for an in-flight capture by holding the ticket directly rather
    /// than starting a real one, so neither half needs `pprof` to install a
    /// signal handler or jemalloc to have been built with `--enable-prof`.
    #[test]
    fn a_capture_during_another_capture_is_refused() {
        let _serial = serialized();
        let runtime = multi_thread_runtime();

        runtime.block_on(async {
            let cpu_in_flight = ProfileAdmission::try_acquire().expect("uncontended");
            let heap = heap_profile().await;
            assert_eq!(
                heap.status(),
                StatusCode::CONFLICT,
                "a heap dump during a CPU capture must be refused"
            );
            drop(cpu_in_flight);

            let heap_in_flight = ProfileAdmission::try_acquire().expect("ticket released");
            let cpu = cpu_profile(Query(ProfileParams { seconds: Some(1) })).await;
            assert_eq!(
                cpu.status(),
                StatusCode::CONFLICT,
                "a CPU capture during a heap dump must be refused"
            );
            drop(heap_in_flight);
        });
    }

    /// The heap route's ticket moves into the blocking closure, so a bug there
    /// leaks it: the closure would hold a ticket nothing ever drops and the
    /// endpoint would wedge after the first dump, 412 or not.
    #[test]
    fn a_completed_heap_request_releases_admission() {
        let _serial = serialized();
        let runtime = multi_thread_runtime();

        runtime.block_on(async {
            // 412 here (the test binary's allocator is not a prof-enabled
            // jemalloc) and 200 in a profiling image are both fine; what is
            // asserted is that the ticket came back either way.
            let response = heap_profile().await;
            assert_ne!(response.status(), StatusCode::CONFLICT, "nothing else held");
            assert!(
                !PROFILE_IN_FLIGHT.load(Ordering::SeqCst),
                "the heap route kept the ticket after its dump returned"
            );
        });
    }

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

    #[test]
    fn runtime_window_defaults_and_clamps() {
        assert_eq!(clamp_runtime_window(None), RUNTIME_DEFAULT_SECONDS);
        assert_eq!(clamp_runtime_window(Some(10)), 10);
        assert_eq!(clamp_runtime_window(Some(0)), RUNTIME_MIN_SECONDS);
        assert_eq!(
            clamp_runtime_window(Some(RUNTIME_MAX_SECONDS + 1)),
            RUNTIME_MAX_SECONDS
        );
    }

    /// The runtime window must never be zero. A zero-length interval is what
    /// produced the 2026-09-08 round's useless snapshots: every delta counter
    /// 0 and busy_ratio 0.0 on a fully loaded ingester.
    #[test]
    fn runtime_window_is_never_zero() {
        for requested in [None, Some(0), Some(1), Some(u64::MAX)] {
            assert!(clamp_runtime_window(requested) >= 1, "{requested:?}");
        }
    }

    /// The two routes' windows are bounded independently: one `?seconds=600`
    /// is a valid CPU profile and an absurd metrics delta.
    #[test]
    fn the_two_windows_have_separate_ceilings() {
        assert_eq!(clamp_seconds(Some(600)), 600);
        assert_eq!(clamp_runtime_window(Some(600)), RUNTIME_MAX_SECONDS);
    }

    /// THE REGRESSION THIS FILE EXISTS TO PREVENT.
    ///
    /// Every duration and count in `RuntimeMetrics` is a delta over the
    /// sampling interval, and `intervals()` starts that interval when it is
    /// called. Taking the first item immediately therefore measures a window
    /// of microseconds in which nothing happened — which is what the
    /// 2026-09-08 profiling round shipped for all seven stages:
    /// `elapsed_ns: 5527`, every counter 0, `busy_ratio: 0.0` on an ingester
    /// running flat out.
    ///
    /// Asserts the shape of the fix rather than the handler (which needs an
    /// HTTP server): burn the first interval, do real work, then take a
    /// second interval and require it to have observed that work.
    #[test]
    fn a_real_window_observes_work_where_an_immediate_sample_sees_none() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        let monitor = tokio_metrics::RuntimeMonitor::new(runtime.handle());
        let mut intervals = monitor.intervals();

        // The old behaviour: sample immediately, before anything can run.
        let immediate = intervals.next().expect("first interval");

        // Now do work the runtime must account for.
        runtime.block_on(async {
            let mut handles = Vec::new();
            for _ in 0..8 {
                handles.push(tokio::spawn(async {
                    let mut acc = 0u64;
                    for i in 0..2_000_000u64 {
                        acc = acc.wrapping_add(i);
                    }
                    acc
                }));
            }
            for handle in handles {
                let _ = handle.await;
            }
        });

        let measured = intervals.next().expect("second interval");

        assert!(
            measured.elapsed > immediate.elapsed,
            "the measured window must be longer than the microsecond one \
             (immediate={:?}, measured={:?})",
            immediate.elapsed,
            measured.elapsed
        );
        assert!(
            measured.total_busy_duration > Duration::ZERO,
            "a window containing 8 CPU-bound tasks must report busy time, got {:?}",
            measured.total_busy_duration
        );

        // And the derived ratio must now be a real number rather than the
        // 0.0-over-nothing the round reported.
        let (idle, ratio) = worker_idle(
            measured.elapsed,
            measured.workers_count,
            measured.total_busy_duration,
        );
        let ratio = ratio.expect("a non-empty window has a defined busy ratio");
        assert!(ratio > 0.0, "busy_ratio must be above zero, got {ratio}");
        assert!(
            idle > 0 || ratio >= 1.0,
            "idle and ratio must be consistent (idle={idle}, ratio={ratio})"
        );
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
