//! `commit=force` must not claim a wrong answer where it cannot observe the
//! truth.
//!
//! It proves commit by watching the segment leave the LOCAL `sealed/` and
//! `processing/` directories. A local drain still supplies that proof when the
//! WAL is mirrored. A remote drain commits the segment out of the mirror and
//! never moves the ingester's local file, so that topology must refuse the mode
//! before waiting.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use siglake_ingest::{router, AppState, TenantRouting};
use siglake_wal::WalWriter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn payload() -> String {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": "hello" }
                }]
            }]
        }]
    })
    .to_string()
}

fn app_with_mirror(root: &std::path::Path, mirrored: bool, remote_wal_drain: bool) -> axum::Router {
    let mut writer = WalWriter::with_thresholds(root, "test", 1, Duration::from_secs(60)).unwrap();
    if mirrored {
        let op = opendal::Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let (_mirror, handle) = siglake_wal::mirror::WalMirror::new(op, "wal-mirror");
        writer.set_mirror_handle(Some(handle));
    }
    router(AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: None,
        allowed_tenants: None,
        tenant_routing: TenantRouting::default(),
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        // Short, so a regression that reinstates the wait fails fast rather
        // than hanging the suite for 30s.
        commit_force_timeout: Duration::from_secs(2),
        remote_wal_drain,
    })
}

async fn post_force(app: &axum::Router) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs?commit=force")
                .header("content-type", "application/json")
                .body(Body::from(payload()))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

/// Against the old code this test waits the full timeout and returns 504.
#[tokio::test]
async fn commit_force_is_refused_when_a_remote_drain_consumes_the_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let status = post_force(&app_with_mirror(&tmp.path().join("wal"), true, true)).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a mode that cannot possibly observe the commit must refuse, not time out"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "and it must refuse UP FRONT, not after pinning the connection for the full \
         timeout — took {:?}",
        started.elapsed()
    );
}

/// Unmirrored, the local file IS the proof, so the mode still works — it times
/// out here only because no drain is running in this test.
#[tokio::test]
async fn commit_force_still_runs_without_a_mirror() {
    let tmp = tempfile::tempdir().unwrap();
    let status = post_force(&app_with_mirror(&tmp.path().join("wal"), false, true)).await;
    assert_ne!(
        status,
        StatusCode::BAD_REQUEST,
        "without a mirror this ingester can observe its own commit, so the mode is valid"
    );
}

#[tokio::test]
async fn commit_force_runs_with_a_mirror_and_local_drain() {
    let tmp = tempfile::tempdir().unwrap();
    let status = post_force(&app_with_mirror(&tmp.path().join("wal"), true, false)).await;
    assert_ne!(status, StatusCode::BAD_REQUEST);
}
