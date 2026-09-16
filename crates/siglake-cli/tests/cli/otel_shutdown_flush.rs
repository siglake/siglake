//! A subcommand that FAILS still flushes its telemetry before the process
//! ends — and a process with telemetry off sends nothing at all.
//!
//! WHAT THIS PINS. The OTel providers live in a `OnceLock` that never drops,
//! and the batch processors hold records until someone shuts them down. As
//! written on PR #7 that shutdown sat on the one return the branch instrumented
//! (the ingest server's graceful stop), so every other way out of the process —
//! a startup error, a bad path, a refused flag — dropped whatever was buffered.
//! `main` now initializes telemetry and wraps a `run()`, which is the claim
//! this test checks from outside the process: run the real binary against a
//! stand-in collector, on a path that returns `Err`, and require an OTLP export
//! to arrive before the exit status does.

use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::Router;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

/// A stand-in OTLP/HTTP collector: counts request bodies and answers 200. The
/// exporter's own decode of that empty answer is not what is under test.
async fn collector() -> (SocketAddr, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let exports = Arc::new(AtomicUsize::new(0));
    let counter = exports.clone();
    let app = Router::new().fallback(axum::routing::any(move |_body: Bytes| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            StatusCode::OK
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind collector");
    let addr = listener.local_addr().expect("collector addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve collector");
    });
    (addr, exports, handle)
}

fn server_addr_from_log(line: &str) -> Option<String> {
    let mut plain = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.next_if_eq(&'[').is_some() {
            for escape_ch in chars.by_ref() {
                if ('@'..='~').contains(&escape_ch) {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
        .split_whitespace()
        .find_map(|field| field.strip_prefix("addr=").map(str::to_string))
}

#[test]
fn server_address_parser_ignores_tracing_ansi_codes() {
    let colored = "siglake-ingest HTTP server listening \
        \x1b[3maddr\x1b[0m\x1b[2m=\x1b[0m127.0.0.1:43123\x1b[0m";
    assert_eq!(
        server_addr_from_log(colored).as_deref(),
        Some("127.0.0.1:43123")
    );
}

/// Run the real binary, either on a path that returns `Err` (`wal-recover`
/// with an unparseable `--from`, which touches no storage and returns in
/// milliseconds) or on one that returns `Ok` (`gen`, which prints events).
async fn run_subcommand(endpoint: Option<&str>, failing: bool) -> (bool, String) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_siglake"));
    cmd.arg("--data-dir").arg(tmp.path());
    if failing {
        cmd.arg("wal-recover")
            .arg("--from")
            .arg("not-a-url")
            .arg("--to")
            .arg(tmp.path().join("wal"));
    } else {
        cmd.arg("gen").arg("--n").arg("1");
    }
    // The mirror URL is also an env var on that flag; clear it so the caller's
    // environment cannot supply one.
    cmd.env_remove("SIGLAKE_WAL_MIRROR_URL")
        .env_remove("SIGLAKE_OTEL_DISABLED")
        .env("RUST_LOG", "info");
    match endpoint {
        Some(e) => cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", e),
        None => cmd.env_remove("OTEL_EXPORTER_OTLP_ENDPOINT"),
    };
    let out = tokio::time::timeout(Duration::from_secs(60), cmd.output())
        .await
        .expect("binary finished")
        .expect("run binary");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_subcommand_flushes_before_it_exits() {
    let (addr, exports, server) = collector().await;

    let (ok, stderr) = run_subcommand(Some(&format!("http://{addr}")), true).await;
    assert!(!ok, "wal-recover with a bad URL must fail: {stderr}");
    assert!(
        stderr.contains("opentelemetry emission enabled"),
        "telemetry never came up, so the flush proves nothing: {stderr}"
    );

    server.abort();
    let n = exports.load(Ordering::SeqCst);
    assert!(
        n > 0,
        "the process exited on an error path without flushing: the collector \
         saw {n} exports (stderr: {stderr})"
    );
}

/// The negative control, and the off-by-default claim: with no endpoint, the
/// same run sends nothing anywhere. Without this the test above would pass on a
/// build that exported unconditionally.
#[tokio::test(flavor = "multi_thread")]
async fn telemetry_off_sends_nothing() {
    let (_addr, exports, server) = collector().await;

    let (ok, stderr) = run_subcommand(None, true).await;
    assert!(!ok, "wal-recover with a bad URL must fail: {stderr}");
    assert!(
        !stderr.contains("opentelemetry emission enabled"),
        "emission must stay off without an endpoint: {stderr}"
    );

    server.abort();
    assert_eq!(
        exports.load(Ordering::SeqCst),
        0,
        "a process with no OTLP endpoint exported something"
    );
}

/// The other exit: a subcommand that SUCCEEDS. `main` flushes after `run`
/// returns either way, so this is the same claim on the ordinary path.
#[tokio::test(flavor = "multi_thread")]
async fn a_succeeding_subcommand_flushes_before_it_exits() {
    let (addr, exports, server) = collector().await;

    let (ok, stderr) = run_subcommand(Some(&format!("http://{addr}")), false).await;
    assert!(ok, "gen must succeed: {stderr}");

    server.abort();
    let n = exports.load(Ordering::SeqCst);
    assert!(
        n > 0,
        "the process returned normally without flushing: the collector saw {n} \
         exports (stderr: {stderr})"
    );
}

/// The long-running exit: SIGTERM resolves the ingest server's graceful
/// shutdown future, `run()` drains its writers, and `main` flushes telemetry.
/// This covers the deployed server path separately from the one-shot returns
/// above.
#[tokio::test(flavor = "multi_thread")]
async fn a_graceful_server_shutdown_flushes_before_it_exits() {
    let (addr, exports, collector) = collector().await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_siglake"))
        .arg("--data-dir")
        .arg(tmp.path())
        .arg("ingest-server")
        .arg("--bind")
        .arg("127.0.0.1:0")
        .arg("--metrics-bind")
        .arg("127.0.0.1:0")
        .arg("--disable-otlp-grpc")
        .env_remove("SIGLAKE_OTEL_DISABLED")
        .env_remove("SIGLAKE_WAREHOUSE_URL")
        .env_remove("SIGLAKE_CATALOG_URI")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", format!("http://{addr}"))
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ingest server");
    let pid = child.id().expect("child pid");
    let mut stderr = BufReader::new(child.stderr.take().expect("child stderr"));
    let mut captured = String::new();
    let mut server_addr = None;

    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let mut line = String::new();
            assert_ne!(
                stderr.read_line(&mut line).await.expect("read stderr"),
                0,
                "ingest server exited before listening: {captured}"
            );
            captured.push_str(&line);
            if line.contains("siglake-ingest HTTP server listening") {
                server_addr = server_addr_from_log(&line);
                break;
            }
        }
    })
    .await
    .expect("ingest server started");
    let health_url = format!(
        "http://{}/healthz",
        server_addr.expect("server address in log")
    );
    let response = tokio::time::timeout(Duration::from_secs(10), reqwest::get(health_url))
        .await
        .expect("health request finished")
        .expect("health request succeeded");
    assert!(response.status().is_success(), "ingest server became ready");

    // SAFETY: `pid` came from this live child, and SIGTERM neither dereferences
    // memory nor outlives the process handle retained below.
    let sent = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "send SIGTERM to ingest server");
    let status = tokio::time::timeout(Duration::from_secs(60), child.wait())
        .await
        .expect("ingest server stopped")
        .expect("wait for ingest server");
    stderr
        .read_to_string(&mut captured)
        .await
        .expect("read remaining stderr");
    assert!(status.success(), "graceful shutdown failed: {captured}");
    assert!(
        captured.contains("shutdown signal received"),
        "server did not take the graceful signal path: {captured}"
    );

    collector.abort();
    let n = exports.load(Ordering::SeqCst);
    assert!(
        n > 0,
        "the graceful server return did not flush: the collector saw {n} \
         exports (stderr: {captured})"
    );
}
