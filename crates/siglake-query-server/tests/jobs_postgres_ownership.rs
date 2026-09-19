//! Two-store regression for the shared Postgres job store.
//!
//! The chart points every query replica at one database
//! (`deploy/helm/siglake/templates/statefulset-query-server.yaml`), and the
//! startup sweep used to fail *every* pending/running row. Scaling out or
//! rolling a replica therefore destroyed a sibling's in-flight job and its
//! eventual result. This test opens two stores against one database and
//! pins the ownership rules end to end:
//!
//! 1. Opening store B leaves store A's running job alone, and A's result
//!    still lands and is readable through B.
//! 2. Once A is gone and its owner lease has expired, B's recovery fails
//!    A's orphan — and does not touch B's own running job in the same pass.
//! 3. A legacy row with no owner is kept inside the grace period and failed
//!    outside it.
//! 4. Owner-local reconciliation resolves only this incarnation's parked
//!    jobs, gives them a TTL without marking them recovered, and preserves a
//!    cancellation that already won.
//! 5. A second replica's HTTP status and result routes serve the exact rows
//!    computed by the first replica through their shared Postgres table.
//!
//! Another case covers the other half of the same shared-store problem:
//! a cancellation persisted by store B reaches the future store A is
//! executing, over the real backend and on the real timer — not a hand-driven
//! sweep. The status write alone releases nothing, because the admission
//! reservation and the storage-scan cancel guard both live in A's future.
//!
//! Gated with `#[ignore]` so `cargo test` stays hermetic (the same pattern as
//! `crates/siglake-ingest/tests/ingest/redis_rate_budget.rs`). Run against a
//! **scratch** database — recovery is fleet-wide by design, and the test
//! deletes the `siglake_query_jobs` / `siglake_query_job_owners` rows it
//! created:
//!
//! ```text
//! SIGLAKE_TEST_JOBS_POSTGRES_URI=postgres://siglake:siglake@127.0.0.1:5432/siglake_jobs_test \
//!   cargo test -p siglake-query-server --test jobs_postgres_ownership -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use chrono::Utc;
use siglake_core::Event;
use siglake_query_server::cost::{ComplexityClass, CostReport};
use siglake_query_server::format::RecordsResponse;
use siglake_query_server::jobs::{
    recovery_decision, CompletionOutcome, JobId, JobRecoveryPolicy, JobStatus, JobStore,
    OrphanReason, ParkOutcome, ReconcileOutcome, RecoveryDecision,
};
use siglake_query_server::limits::Priority;
use siglake_query_server::{router, AppState, AuthConfig, ServerLimits};
use siglake_storage::iceberg::IcebergContext;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tower::util::ServiceExt;

const TTL: Duration = Duration::from_secs(600);

/// Default-shaped policy. The lease is long enough that a live store's
/// upkeep loop (which beats at a third of it) cannot refresh a heartbeat we
/// have backdated during the few seconds this test takes.
fn policy() -> JobRecoveryPolicy {
    JobRecoveryPolicy {
        owner_lease: Duration::from_secs(120),
        ownerless_grace: Duration::from_secs(86_400),
        // This test cancels nothing, and an hour is long enough that its
        // stores' watch loops never wake to read a table it is mutating.
        cancel_poll: Duration::from_secs(3_600),
    }
}

/// Same recovery numbers, but a cancellation poll short enough that the
/// store's own watch loop is observable inside a test. The point of using the
/// timer rather than calling the sweep by hand is that it proves the loop is
/// actually spawned and actually reaches the database.
fn cancel_policy() -> JobRecoveryPolicy {
    JobRecoveryPolicy {
        cancel_poll: Duration::from_millis(300),
        ..policy()
    }
}

fn cost() -> CostReport {
    CostReport {
        files_to_scan: Some(1),
        files_considered: Some(1),
        estimated_bytes_scanned: 1,
        estimated_rows_processed: 1,
        estimated_runtime_seconds: 0.1,
        complexity_class: ComplexityClass::Small,
        warnings: vec![],
        exact: true,
    }
}

fn records(marker: &str) -> RecordsResponse {
    RecordsResponse {
        columns: vec!["marker".to_string()],
        row_count: 1,
        rows: serde_json::json!([{ "marker": marker }]),
        truncated: false,
        max_rows: None,
        cost: None,
        stats: None,
        approximation: None,
    }
}

async fn send_http(
    app: &Router,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(path);
    let body = match body {
        Some(json) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).expect("encode request"))
        }
        None => Body::empty(),
    };
    app.clone()
        .oneshot(builder.body(body).expect("build request"))
        .await
        .expect("route request")
}

async fn http_json(response: axum::response::Response) -> serde_json::Value {
    serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body"),
    )
    .expect("decode JSON response")
}

async fn http_warehouse() -> (tempfile::TempDir, Arc<IcebergContext>) {
    let tmp = tempfile::tempdir().expect("temporary warehouse");
    let ice = IcebergContext::open(&tmp.path().join("warehouse"))
        .await
        .expect("open warehouse");
    let events: Vec<Event> = (0..4)
        .map(|i| Event {
            timestamp: Utc::now(),
            host: format!("host-{i}"),
            source: "postgres-peer-http".into(),
            sourcetype: "app:json".into(),
            index: "main".into(),
            raw: format!("event {i}"),
            attributes: None,
        })
        .collect();
    ice.append_events(&events).await.expect("append events");
    (tmp, Arc::new(ice))
}

fn http_state(ice: Arc<IcebergContext>, jobs: JobStore) -> AppState {
    AppState::new(ice, AuthConfig::open())
        .with_limits(ServerLimits::default())
        .with_jobs(jobs)
}

#[tokio::test]
#[ignore]
async fn a_peer_replica_serves_http_status_and_result_from_postgres() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI").expect(
        "SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres, e.g. \
         postgres://siglake:siglake@localhost:5433/siglake from scripts/up.sh",
    );
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect cleanup pool");
    let (_tmp, ice) = http_warehouse().await;

    let jobs_a = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store A");
    let state_a = http_state(ice.clone(), jobs_a);
    let app_a = router(state_a.clone());

    let submitted = send_http(
        &app_a,
        Method::POST,
        "/api/v1/sql",
        Some(serde_json::json!({
            "query": "SELECT count(*) AS n FROM events",
            "priority": "batch"
        })),
    )
    .await;
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    let job_id = http_json(submitted).await["job_id"]
        .as_str()
        .expect("submit response carries job_id")
        .to_string();

    // B has an independently opened store and router. Open authentication
    // gives both requests the same anonymous caller identity.
    let jobs_b = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store B");
    let state_b = http_state(ice, jobs_b);
    let app_b = router(state_b.clone());

    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let response =
                send_http(&app_b, Method::GET, &format!("/api/v1/jobs/{job_id}"), None).await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "replica B lost replica A's job"
            );
            let body = http_json(response).await;
            if matches!(
                body["status"].as_str(),
                Some("succeeded" | "failed" | "cancelled" | "timeout")
            ) {
                break body;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("batch job never finished");
    assert_eq!(status["status"], "succeeded", "executor failed: {status}");

    let result = send_http(
        &app_b,
        Method::GET,
        &format!("/api/v1/jobs/{job_id}/result"),
        None,
    )
    .await;
    assert_eq!(
        result.status(),
        StatusCode::OK,
        "replica B did not serve replica A's result"
    );
    let result = http_json(result).await;
    assert_eq!(result["columns"], serde_json::json!(["n"]));
    assert_eq!(result["rows"], serde_json::json!([{ "n": 4 }]));

    sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
        .bind(&job_id)
        .execute(&pool)
        .await
        .expect("cleanup job row");
    state_a.jobs.shutdown().await.expect("shutdown store A");
    state_b.jobs.shutdown().await.expect("shutdown store B");
}

async fn status_of(pool: &PgPool, id: JobId) -> String {
    sqlx::query_scalar("SELECT status FROM siglake_query_jobs WHERE job_id = $1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn error_of(pool: &PgPool, id: JobId) -> Option<String> {
    sqlx::query_scalar("SELECT error FROM siglake_query_jobs WHERE job_id = $1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await
        .expect("read error")
}

async fn owner_of(pool: &PgPool, id: JobId) -> Option<String> {
    sqlx::query_scalar("SELECT owner FROM siglake_query_jobs WHERE job_id = $1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await
        .expect("read owner")
}

async fn recovery_reason_of(pool: &PgPool, id: JobId) -> Option<String> {
    sqlx::query_scalar("SELECT recovery_reason FROM siglake_query_jobs WHERE job_id = $1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await
        .expect("read recovery reason")
}

async fn has_expiry_and_is_unrecovered(pool: &PgPool, id: JobId) -> (bool, bool) {
    sqlx::query_as(
        "SELECT expires_at IS NOT NULL, recovered_at IS NULL
         FROM siglake_query_jobs WHERE job_id = $1",
    )
    .bind(id.to_string())
    .fetch_one(pool)
    .await
    .expect("read expiry and recovery marker")
}

#[tokio::test]
#[ignore]
async fn opening_a_second_store_leaves_the_first_stores_jobs_and_results_intact() {
    // Panic rather than skip: the test only runs when someone asked for it by
    // name with `--ignored`, and a green that verified nothing is worse than a
    // red that says which variable is missing.
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI").expect(
        "SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres, e.g. \
         postgres://siglake:siglake@localhost:5433/siglake from scripts/up.sh",
    );
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");

    // --- store A: submit + start a job, as a replica executing it would.
    let a = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store A");
    let a_job = a
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit on A");
    a.set_running(a_job).await.expect("A -> running");
    a.record_cost(a_job, cost())
        .await
        .expect("A publishes cost");
    assert_eq!(
        owner_of(&pool, a_job).await.as_deref(),
        Some(a.owner_id()),
        "the row must record A as its execution owner"
    );

    // --- store B opens against the same database. This is the scale-out /
    // rolling-restart moment that used to fail A's job.
    let b = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store B");
    assert_ne!(a.owner_id(), b.owner_id(), "owners are per incarnation");
    assert_eq!(
        status_of(&pool, a_job).await,
        "running",
        "B's startup must not fail A's running job"
    );

    // A finishes afterwards and the result is readable through either store.
    a.finish_succeeded(a_job, records("from-a"))
        .await
        .expect("A -> succeeded");
    let via_b = b
        .result(a_job)
        .await
        .expect("job store")
        .expect("B reads A's job");
    assert_eq!(via_b.status, JobStatus::Succeeded);
    assert_eq!(
        via_b.body.expect("result body").rows,
        serde_json::json!([{ "marker": "from-a" }]),
        "A's result survived B's startup"
    );

    // --- A submits a second job. Backdating its heartbeat creates the same
    // recovery evidence as a stopped upkeep loop, while retaining A lets the
    // true owner deliver a deterministic late completion below.
    let orphan = a
        .submit("SELECT 2".into(), Priority::Batch, None)
        .await
        .expect("submit orphan on A");
    a.set_running(orphan).await.expect("A -> running");
    let dead_owner = a.owner_id().to_string();

    // B's own running job, to prove one pass can recover an orphan while
    // leaving the recovering replica's work untouched.
    let b_job = b
        .submit("SELECT 3".into(), Priority::Batch, None)
        .await
        .expect("submit on B");
    b.set_running(b_job).await.expect("B -> running");

    // Recovery with A's heartbeat still fresh: nothing is condemned.
    let before = b.recover_orphaned_jobs().await.expect("recovery pass");
    assert_eq!(
        before.recovered, 0,
        "a fresh lease is not evidence that the executor is gone"
    );
    assert_eq!(status_of(&pool, orphan).await, "running");

    // Evidence of death: A stopped heartbeating longer ago than the lease.
    sqlx::query(
        "UPDATE siglake_query_job_owners
         SET heartbeat_at = NOW() - INTERVAL '10 minutes'
         WHERE owner_id = $1",
    )
    .bind(&dead_owner)
    .execute(&pool)
    .await
    .expect("backdate A's heartbeat");

    let after = b.recover_orphaned_jobs().await.expect("recovery pass");
    assert_eq!(after.recovered, 1, "exactly A's orphan is recovered");
    assert_eq!(after.kept_self, 1, "B's own running job is kept");
    assert_eq!(status_of(&pool, orphan).await, "failed");
    assert_eq!(
        recovery_reason_of(&pool, orphan).await.as_deref(),
        Some("owner_lease_expired")
    );
    assert!(
        error_of(&pool, orphan)
            .await
            .unwrap_or_default()
            .contains("stopped heartbeating"),
        "the failure names the evidence"
    );
    assert_eq!(
        b.finish_succeeded(orphan, records("wrong-incarnation"))
            .await
            .expect("non-owner completion"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: false,
            recovered: true,
        }
    );
    assert!(
        !error_of(&pool, orphan)
            .await
            .unwrap_or_default()
            .contains("computed after recovery"),
        "a restarted or different incarnation must not amend its predecessor's row"
    );
    assert_eq!(
        a.finish_succeeded(orphan, records("discarded-after-recovery"))
            .await
            .expect("late A completion"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: true,
            recovered: true,
        }
    );
    let amended = error_of(&pool, orphan).await.unwrap_or_default();
    assert!(amended.contains("stopped heartbeating"));
    assert!(amended.contains("computed after recovery"));
    assert_eq!(
        amended.matches("computed after recovery").count(),
        1,
        "the client-visible disposition is appended exactly once"
    );
    assert_eq!(
        a.finish_succeeded(orphan, records("discarded-again"))
            .await
            .expect("repeated late A completion"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: true,
            recovered: true,
        }
    );
    assert_eq!(
        a.finish_failed(orphan, "query exceeded the timeout".into(), true)
            .await
            .expect("late timeout"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: true,
            recovered: true,
        },
        "a timeout verdict refused by recovery has the same disposition"
    );
    assert_eq!(
        error_of(&pool, orphan)
            .await
            .unwrap_or_default()
            .matches("computed after recovery")
            .count(),
        1,
        "the diagnostic sentence must be idempotent"
    );
    assert_eq!(
        status_of(&pool, b_job).await,
        "running",
        "the live owner's job is untouched by the same pass"
    );

    // --- legacy row: no owner at all.
    let legacy = JobId::new();
    sqlx::query(
        "INSERT INTO siglake_query_jobs
           (job_id, status, priority, submitted_at, query, owner)
         VALUES ($1, 'running', 'batch', NOW(), 'SELECT 4', NULL)",
    )
    .bind(legacy.to_string())
    .execute(&pool)
    .await
    .expect("insert legacy row");

    let kept = b.recover_orphaned_jobs().await.expect("recovery pass");
    assert_eq!(
        kept.kept_ownerless, 1,
        "an ownerless row inside the grace period is left alone"
    );
    assert_eq!(status_of(&pool, legacy).await, "running");

    sqlx::query(
        "UPDATE siglake_query_jobs SET submitted_at = NOW() - INTERVAL '2 days' WHERE job_id = $1",
    )
    .bind(legacy.to_string())
    .execute(&pool)
    .await
    .expect("age the legacy row");
    let aged = b.recover_orphaned_jobs().await.expect("recovery pass");
    assert_eq!(aged.recovered, 1, "past the grace period it is failed");
    assert_eq!(status_of(&pool, legacy).await, "failed");
    assert_eq!(
        recovery_reason_of(&pool, legacy).await.as_deref(),
        Some("ownerless_beyond_grace")
    );
    assert!(
        error_of(&pool, legacy)
            .await
            .unwrap_or_default()
            .contains("predates execution ownership"),
        "the failure says why an ownerless row was condemned"
    );
    assert_eq!(
        b.finish_succeeded(legacy, records("legacy-discarded"))
            .await
            .expect("late ownerless completion"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: false,
            recovered: true,
        }
    );
    assert!(
        !error_of(&pool, legacy)
            .await
            .unwrap_or_default()
            .contains("computed after recovery"),
        "an incarnation that did not own the row must not amend its error"
    );

    // --- cleanup: only the rows this test wrote.
    let b_owner = b.owner_id().to_string();
    for id in [a_job, orphan, b_job, legacy] {
        sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
            .bind(id.to_string())
            .execute(&pool)
            .await
            .expect("cleanup job row");
    }
    drop(a);
    drop(b);
    for owner in [dead_owner, b_owner] {
        sqlx::query("DELETE FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(owner)
            .execute(&pool)
            .await
            .expect("cleanup owner row");
    }
}

/// A heartbeat renewed after candidate selection invalidates the evidence at
/// the condemning write. This is the stale-observation window the liveness
/// guard closes; parsing the SQL cannot prove it.
#[tokio::test]
#[ignore]
async fn a_heartbeat_between_recovery_selection_and_write_cancels_condemnation() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI")
        .expect("SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");
    let a = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store A");
    let b = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store B");
    let job = a
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit on A");
    a.set_running(job).await.expect("A -> running");
    a.record_cost(job, cost()).await.expect("A publishes cost");

    sqlx::query(
        "UPDATE siglake_query_job_owners SET heartbeat_at = NOW() - INTERVAL '10 minutes' WHERE owner_id = $1",
    )
    .bind(a.owner_id())
    .execute(&pool)
    .await
    .expect("backdate heartbeat");
    let selected = b
        .ownership_candidates_for_test()
        .await
        .expect("read candidates")
        .into_iter()
        .find(|candidate| candidate.job_id == job.to_string())
        .expect("job is a candidate");
    assert_eq!(
        recovery_decision(&selected, b.owner_id(), &policy()),
        RecoveryDecision::Recover(OrphanReason::OwnerLeaseExpired)
    );

    sqlx::query("UPDATE siglake_query_job_owners SET heartbeat_at = NOW() WHERE owner_id = $1")
        .bind(a.owner_id())
        .execute(&pool)
        .await
        .expect("renew heartbeat between read and write");
    assert_eq!(
        b.condemn_selected_for_test(OrphanReason::OwnerLeaseExpired, &[job.to_string()])
            .await
            .expect("guarded condemnation"),
        0
    );
    assert_eq!(status_of(&pool, job).await, "running");
    assert_eq!(
        a.finish_succeeded(job, records("survived"))
            .await
            .expect("A completion"),
        CompletionOutcome::Applied {
            status: JobStatus::Succeeded
        }
    );

    let owners = [a.owner_id().to_string(), b.owner_id().to_string()];
    sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
        .bind(job.to_string())
        .execute(&pool)
        .await
        .expect("cleanup job");
    drop(a);
    drop(b);
    for owner in owners {
        sqlx::query("DELETE FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(owner)
            .execute(&pool)
            .await
            .expect("cleanup owner");
    }
}

/// Planned shutdown is stronger evidence than waiting for a lease: after the
/// executing store retires its local work, it removes its incarnation row and
/// cannot heartbeat it back into existence. A peer can therefore fail the
/// abandoned row on its very next pass.
#[tokio::test]
#[ignore]
async fn graceful_shutdown_releases_the_owner_for_the_next_recovery_pass() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI")
        .expect("SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");
    let short_lease = JobRecoveryPolicy {
        owner_lease: Duration::from_secs(1),
        ..policy()
    };
    let a = JobStore::new_postgres(&uri, 1, TTL, short_lease)
        .await
        .expect("open store A");
    let b = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store B");
    let job = a
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit on A");
    a.set_running(job).await.expect("A -> running");
    let a_owner = a.owner_id().to_string();

    a.shutdown().await.expect("graceful shutdown A");
    let registrations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(&a_owner)
            .fetch_one(&pool)
            .await
            .expect("count A registrations");
    assert_eq!(registrations, 0, "shutdown removes A's live registration");

    let recovered = b.recover_orphaned_jobs().await.expect("first peer sweep");
    assert_eq!(
        recovered.recovered, 1,
        "the first peer sweep recovers A's job"
    );
    assert_eq!(status_of(&pool, job).await, "failed");
    assert_eq!(
        recovery_reason_of(&pool, job).await.as_deref(),
        Some("owner_unregistered")
    );

    // Wait past the cadence A's upkeep used. A stale loop would upsert the
    // registration again here even though the first DELETE appeared to work.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let registrations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(&a_owner)
            .fetch_one(&pool)
            .await
            .expect("recount A registrations");
    assert_eq!(registrations, 0, "owner upkeep must stay stopped");

    sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
        .bind(job.to_string())
        .execute(&pool)
        .await
        .expect("cleanup job row");
    b.shutdown().await.expect("graceful shutdown B");
}

/// Owner-local reconciliation is deliberately distinct from fleet recovery:
/// it may resolve only ids parked by this incarnation, and it must not stamp
/// `recovered_at`. The repeated failure write below is retry classification
/// evidence, not a simulation of a connection dropping after COMMIT.
#[tokio::test]
#[ignore]
async fn owner_local_reconciliation_preserves_peer_work_and_terminal_rows() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI")
        .expect("SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");
    let a = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store A");
    let b = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store B");

    let ours = a
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit parked job on A");
    a.set_running(ours).await.expect("parked job -> running");
    assert_eq!(owner_of(&pool, ours).await.as_deref(), Some(a.owner_id()));
    assert_eq!(
        a.park_finished_unpersisted(ours, JobStatus::Succeeded),
        ParkOutcome::Tracked
    );

    let peer = b
        .submit("SELECT 2".into(), Priority::Batch, None)
        .await
        .expect("submit peer job on B");
    b.set_running(peer).await.expect("peer job -> running");
    assert_eq!(owner_of(&pool, peer).await.as_deref(), Some(b.owner_id()));
    let peer_is_heartbeating: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM siglake_query_job_owners
             WHERE owner_id = $1
               AND heartbeat_at > NOW() - INTERVAL '2 minutes'
         )",
    )
    .bind(b.owner_id())
    .fetch_one(&pool)
    .await
    .expect("read peer heartbeat");
    assert!(peer_is_heartbeating, "store B has a live owner lease");

    let cancelled = a
        .submit("SELECT 3".into(), Priority::Batch, None)
        .await
        .expect("submit cancelled job on A");
    a.set_running(cancelled)
        .await
        .expect("cancelled job -> running");
    assert!(a.cancel(cancelled).await.expect("cancel job"));
    assert_eq!(
        a.park_finished_unpersisted(cancelled, JobStatus::Succeeded),
        ParkOutcome::Tracked
    );

    assert_eq!(
        a.reconcile_finished_jobs().await,
        ReconcileOutcome {
            installed: 1,
            preserved: 1,
            ..Default::default()
        }
    );
    assert_eq!(status_of(&pool, ours).await, "failed");
    assert_eq!(
        has_expiry_and_is_unrecovered(&pool, ours).await,
        (true, true),
        "local reconciliation must make the row sweepable without calling it recovery"
    );
    assert_eq!(
        status_of(&pool, peer).await,
        "running",
        "A's owner-local pass must not consider B's live work"
    );
    assert_eq!(status_of(&pool, cancelled).await, "cancelled");
    assert_eq!(
        error_of(&pool, cancelled).await.as_deref(),
        Some("cancelled by client"),
        "reconciliation must preserve the terminal row that won"
    );

    let retried = a
        .submit("SELECT 4".into(), Priority::Batch, None)
        .await
        .expect("submit retry-classification job on A");
    a.set_running(retried)
        .await
        .expect("retry-classification job -> running");
    assert_eq!(
        a.finish_failed(retried, "first write".into(), false)
            .await
            .expect("first failure write"),
        CompletionOutcome::Applied {
            status: JobStatus::Failed
        }
    );
    assert_eq!(
        a.finish_failed(retried, "retry".into(), false)
            .await
            .expect("repeated failure write"),
        CompletionOutcome::Superseded {
            status: JobStatus::Failed,
            owned_by_us: true,
            recovered: false,
        },
        "a retry sees its owner's unrecovered predecessor through Postgres"
    );
    assert_eq!(
        error_of(&pool, retried).await.as_deref(),
        Some("first write"),
        "the retry must not overwrite its predecessor"
    );

    let owners = [a.owner_id().to_string(), b.owner_id().to_string()];
    for id in [ours, peer, cancelled, retried] {
        sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
            .bind(id.to_string())
            .execute(&pool)
            .await
            .expect("cleanup job row");
    }
    drop(a);
    drop(b);
    for owner in owners {
        sqlx::query("DELETE FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(owner)
            .execute(&pool)
            .await
            .expect("cleanup owner row");
    }
}

/// The TTL column is an enforced retention boundary, not bookkeeping. One
/// sweep removes an expired terminal row while leaving both a future-expiry
/// result and a non-terminal job intact.
#[tokio::test]
#[ignore]
async fn one_gc_sweep_deletes_only_rows_past_expires_at() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI")
        .expect("SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");
    let store = JobStore::new_postgres(&uri, 1, TTL, policy())
        .await
        .expect("open store");

    let expired = store
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit expired row");
    store
        .finish_failed(expired, "expired".into(), false)
        .await
        .expect("finish expired row");
    sqlx::query(
        "UPDATE siglake_query_jobs SET expires_at = NOW() - INTERVAL '1 second' WHERE job_id = $1",
    )
    .bind(expired.to_string())
    .execute(&pool)
    .await
    .expect("backdate expiry");

    let unexpired = store
        .submit("SELECT 2".into(), Priority::Batch, None)
        .await
        .expect("submit unexpired row");
    store
        .finish_failed(unexpired, "retained".into(), false)
        .await
        .expect("finish unexpired row");
    let active = store
        .submit("SELECT 3".into(), Priority::Batch, None)
        .await
        .expect("submit active row");

    assert_eq!(store.gc().await, 1, "exactly the expired row is swept");
    assert!(store.info(expired).await.expect("job store").is_none());
    assert_eq!(status_of(&pool, unexpired).await, "failed");
    assert_eq!(status_of(&pool, active).await, "pending");

    let owner = store.owner_id().to_string();
    for id in [unexpired, active] {
        sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
            .bind(id.to_string())
            .execute(&pool)
            .await
            .expect("cleanup job row");
    }
    drop(store);
    sqlx::query("DELETE FROM siglake_query_job_owners WHERE owner_id = $1")
        .bind(owner)
        .execute(&pool)
        .await
        .expect("cleanup owner row");
}

/// THE DEFECT THIS GUARDS. `cancel()` wrote the row and aborted a handle in
/// its *own* process's map. With one shared store the replica serving
/// `DELETE /api/v1/jobs/<id>` is usually not the executor, so the `202` said
/// "reservation released, scans cancelled" while the other pod kept both.
/// Here store B cancels a job store A is executing and A's own watch loop —
/// not a hand-driven sweep — has to stop it.
#[tokio::test]
#[ignore]
async fn a_cancellation_persisted_by_one_replica_stops_the_executor_on_another() {
    let uri = std::env::var("SIGLAKE_TEST_JOBS_POSTGRES_URI").expect(
        "SIGLAKE_TEST_JOBS_POSTGRES_URI must name a scratch Postgres, e.g. \
         postgres://siglake:siglake@localhost:5433/siglake from scripts/up.sh",
    );
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&uri)
        .await
        .expect("connect probe pool");

    let a = JobStore::new_postgres(&uri, 1, TTL, cancel_policy())
        .await
        .expect("open store A");
    let b = JobStore::new_postgres(&uri, 1, TTL, cancel_policy())
        .await
        .expect("open store B");
    assert_ne!(a.owner_id(), b.owner_id());

    let job = a
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .expect("submit on A");
    a.set_running(job).await.expect("A -> running");
    a.record_cost(job, cost()).await.expect("A publishes cost");

    // Stand in for the batch future where it matters: it owns the
    // storage-scan kill switch `run_batch_query` puts in its session config,
    // and only dropping the future flips it.
    let cancel = siglake_storage::QueryCancel::new();
    let guard = siglake_storage::CancelOnDrop(cancel.clone());
    let (abort, registration) = futures::future::AbortHandle::new_pair();
    a.register_abort(job, abort);
    a.batch_runtime().spawn(async move {
        let _scan_guard = guard;
        let _ = futures::future::Abortable::new(std::future::pending::<()>(), registration).await;
    });

    // B cancels. B holds no abort handle for this job, so at this instant the
    // row is terminal and A is still executing.
    assert!(
        b.cancel(job).await.expect("job store"),
        "B persists the cancellation"
    );
    assert_eq!(status_of(&pool, job).await, "cancelled");

    // A's watch loop reads the shared table and aborts. The ceiling is a
    // generous multiple of the 300 ms poll, not a latency assertion.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !cancel.is_cancelled() {
        assert!(
            std::time::Instant::now() < deadline,
            "A never observed B's cancellation, so its storage scans were never stopped"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The handle is consumed, so a further sweep is a no-op.
    assert!(a
        .propagate_cancellations()
        .await
        .expect("explicit sweep")
        .is_empty());

    // And the losing half of the race: a completion arriving after the
    // cancellation is refused and told so, and the row does not move.
    // The `Superseded` status is read back from the shared row, which is the
    // half no in-memory store exercises: it comes out of Postgres.
    assert_eq!(
        a.finish_succeeded(job, records("too-late"))
            .await
            .expect("late success"),
        CompletionOutcome::Superseded {
            status: JobStatus::Cancelled,
            owned_by_us: true,
            recovered: false,
        },
        "a cancelled row must refuse a late completion, and say what beat it"
    );
    assert_eq!(status_of(&pool, job).await, "cancelled");
    assert_eq!(
        error_of(&pool, job).await.as_deref(),
        Some("cancelled by client")
    );
    assert_eq!(
        owner_of(&pool, job).await.as_deref(),
        Some(a.owner_id()),
        "cancellation does not reassign execution ownership"
    );

    // --- cleanup: only the rows this test wrote.
    let owners = [a.owner_id().to_string(), b.owner_id().to_string()];
    sqlx::query("DELETE FROM siglake_query_jobs WHERE job_id = $1")
        .bind(job.to_string())
        .execute(&pool)
        .await
        .expect("cleanup job row");
    drop(a);
    drop(b);
    for owner in owners {
        sqlx::query("DELETE FROM siglake_query_job_owners WHERE owner_id = $1")
            .bind(owner)
            .execute(&pool)
            .await
            .expect("cleanup owner row");
    }
}
