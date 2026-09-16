//! The ingester must be able to say it is NOT ready.
//!
//! It used to wire the constant-200 `/healthz` as both liveness and readiness,
//! so it had no readiness gate at all: an ingester whose WAL volume was full or
//! remounted read-only stayed in Service rotation, accepting traffic it could
//! not durably ack. An ack here means "the rows are in the WAL", so WAL
//! writability is not an aspect of readiness — it is readiness.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn state_at(root: &std::path::Path) -> siglake_ingest::AppState {
    let tenants = siglake_ingest::TenantWalRouter::new(
        root,
        "test-ingester",
        5,
        std::time::Duration::from_secs(60),
    );
    let writer = siglake_wal::WalWriter::with_thresholds(
        root,
        "test-ingester",
        5,
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    siglake_ingest::AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        backpressure: None,
        events_tx: None,
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: std::time::Duration::from_secs(30),
        remote_wal_drain: true,
        allowed_tenants: None,
        tenant_routing: siglake_ingest::TenantRouting::default(),
        max_tenants: 0,
        tenant_admission: Default::default(),
    }
}

async fn probe(state: siglake_ingest::AppState, path: &str) -> StatusCode {
    siglake_ingest::router(state)
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn a_writable_wal_is_ready() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("wal");
    assert_eq!(probe(state_at(&root), "/readyz").await, StatusCode::OK);
}

/// Against the old wiring this test FAILS: readiness was a string literal, so
/// it returned 200 whatever the volume was doing.
#[tokio::test]
async fn an_unwritable_wal_is_not_ready() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("wal");
    let state = state_at(&root);
    // Ready to begin with...
    assert_eq!(probe(state.clone(), "/readyz").await, StatusCode::OK);

    // ...then the volume goes read-only under us.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).unwrap();
    let status = probe(state.clone(), "/readyz").await;
    // Restore before asserting so a failure does not leave an undeletable dir.
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "an ingester that cannot write its WAL must leave Service rotation"
    );

    // And it recovers on its own once the volume does.
    assert_eq!(probe(state, "/readyz").await, StatusCode::OK);
}

/// Liveness stays a liveness probe: a full volume is not a reason to kill and
/// restart the process, only a reason to stop sending it traffic.
#[tokio::test]
async fn liveness_does_not_follow_readiness() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("wal");
    let state = state_at(&root);
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o555)).unwrap();
    let status = probe(state, "/healthz").await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(status, StatusCode::OK);
}
