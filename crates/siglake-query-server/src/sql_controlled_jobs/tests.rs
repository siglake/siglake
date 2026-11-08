//! Owned socket-only PostgreSQL fixture, never an installed/shared database.
use super::*;
use crate::query_controls::*;
use sqlx::{ConnectOptions, Row};
use std::time::Duration;

async fn fixture() -> (AppState, sqlx::PgPool, PolicyManager, tempfile::TempDir) {
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use owned fixture runner"),
    );
    assert!(
        socket.starts_with("/tmp")
            && socket
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("siglake-policy-")
    );
    assert_eq!(
        std::fs::read_to_string(socket.join("fixture-marker")).unwrap(),
        "new-owned-policy-fixture-v1\n"
    );
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let uri = |role| format!("postgres://{role}:@{encoded}:5432/qh_fixture?sslmode=disable");
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            uri("fixture_admin")
                .parse::<sqlx::postgres::PgConnectOptions>()
                .unwrap()
                .disable_statement_logging(),
        )
        .await
        .unwrap();
    let jobs = Arc::new(
        crate::jobs::JobStore::new_postgres(
            &uri("fixture_admin"),
            1,
            Duration::from_secs(60),
            Default::default(),
        )
        .await
        .unwrap(),
    );
    assert!(jobs.controlled_ready().await.is_err());
    sqlx::raw_sql(include_str!("../query_controls/jobs.sql"))
        .execute(&admin)
        .await
        .unwrap();
    let bounds = ConnectionBounds {
        max_connections: 1,
        operation_timeout: Duration::from_secs(2),
    };
    let reader = Arc::new(
        PolicyReader::connect(PolicyConfig::reader(&uri("qp_reader"), bounds).unwrap())
            .await
            .unwrap(),
    );
    let manager =
        PolicyManager::connect(PolicyConfig::manager(&uri("qp_manager"), bounds).unwrap())
            .await
            .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(tmp.path())
            .await
            .unwrap(),
    );
    let mut state = AppState::new(ice, crate::AuthConfig::open());
    state.jobs = jobs;
    Arc::make_mut(&mut state.limits).batch.default_timeout = Duration::from_secs(4);
    Arc::make_mut(&mut state.limits).batch.ceiling_timeout = Duration::from_secs(8);
    (
        state.with_query_controls(reader, 1).unwrap(),
        admin,
        manager,
        tmp,
    )
}
async fn submit(
    state: &AppState,
    identity: CallerIdentity,
    seconds: u64,
    breakers: bool,
) -> (crate::jobs::JobId, serde_json::Value) {
    let req:SqlRequest=serde_json::from_value(serde_json::json!({"query":"SELECT 1 AS value","priority":"batch","limits":{"timeout_seconds":seconds,"circuit_breakers":breakers}})).unwrap();
    let response = runtime::ingress(
        Instant::now(),
        crate::sql::handle_local(State(state.clone()), axum::Extension(identity), Json(req)),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), 202);
    let body: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 16384)
            .await
            .unwrap(),
    )
    .unwrap();
    (
        crate::jobs::JobId::parse(body["job_id"].as_str().unwrap()).unwrap(),
        body,
    )
}
async fn terminal(state: &AppState, id: crate::jobs::JobId) -> crate::jobs::JobInfo {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let info = state.jobs.info(id).await.unwrap().unwrap();
            if info.status.is_terminal() {
                return info;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
// Block only the existing batch thread. Owner/timer/persistence remain on the
// server runtime. Release before shutdown to acknowledge cooperative aborts.
async fn hold_batch(
    state: &AppState,
) -> (std::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (tx, rx) = std::sync::mpsc::channel();
    let task = state.jobs.batch_runtime().spawn(async move {
        let _ = ready_tx.send(());
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    ready_rx.await.unwrap();
    (tx, task)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned local PostgreSQL policy+job store fixture"]
async fn durable_jobs_real_pg_submission_queue_dequeue_policy_fence_and_recovery() {
    let (state, admin, manager, _tmp) = fixture().await;
    let identity = CallerIdentity {
        tenant: Some("durable".into()),
        provider_principal: Some(crate::oidc::VerifiedProviderPrincipal {
            issuer: "https://synthetic.invalid/exact/".into(),
            client_id: "durable-client".into(),
        }),
        ..Default::default()
    };
    let key = RuntimeKey::from_trusted_metadata(
        "durable",
        "https://synthetic.invalid/exact/",
        "durable-client",
    )
    .unwrap();
    let binding =
        ManagementBinding::from_trusted_metadata(key, uuid::Uuid::new_v4(), uuid::Uuid::new_v4())
            .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    // A zero queue+run budget still durably hands off and false cannot disable it.
    let (release, held) = hold_batch(&state).await;
    let (id, body) = submit(&state, identity.clone(), 0, false).await;
    assert_eq!(body["controls"]["accepted"]["effective_timeout_ms"], 0);
    let info = terminal(&state, id).await;
    assert_eq!(info.status, JobStatus::Timeout);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    release.send(()).unwrap();
    held.await.unwrap();
    // EXACT unchanged old-worker SQL must be fenced even on a repeat start.
    assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
        .bind(id.to_string())
        .execute(&admin)
        .await
        .is_ok()); // terminal CAS no-op
    let (release, held) = hold_batch(&state).await;
    let (queued, accepted) = submit(&state, identity.clone(), 4, false).await;
    assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
        .bind(queued.to_string())
        .execute(&admin)
        .await
        .is_err());
    assert!(
        sqlx::query("UPDATE siglake_query_jobs SET controls_version=NULL WHERE job_id=$1")
            .bind(queued.to_string())
            .execute(&admin)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM siglake_query_job_controls WHERE job_id=$1")
            .bind(queued.to_string())
            .execute(&admin)
            .await
            .is_err()
    );
    let saved = manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(0, SavedSettings::new(Some(1), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    release.send(()).unwrap();
    held.await.unwrap();
    let info = terminal(&state, queued).await;
    assert_eq!(info.status, JobStatus::Timeout);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    assert_eq!(
        info.controls.unwrap().original_expires_at,
        accepted["controls"]["original_expires_at"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap()
    );
    // Loosening at dequeue cannot extend the accepted one-second budget.
    let (release, held) = hold_batch(&state).await;
    let (queued, accepted) = submit(&state, identity.clone(), 4, true).await;
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(saved.revision(), SavedSettings::new(Some(4), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(terminal(&state, queued).await.status, JobStatus::Timeout);
    assert_eq!(
        accepted["controls"]["accepted"]["effective_timeout_ms"],
        1000
    );
    release.send(()).unwrap();
    held.await.unwrap();
    // Normal success with overflow caller clamped by the operator ceiling.
    let (success, body) = submit(&state, identity.clone(), u64::MAX, false).await;
    assert_eq!(body["controls"]["accepted"]["effective_timeout_ms"], 4000);
    let info = terminal(&state, success).await;
    assert_eq!(
        info.status,
        JobStatus::Succeeded,
        "private failure: {:?}",
        info.error
    );
    assert!(info.controls.unwrap().execution.is_some());
    let response = crate::jobs_routes::result(
        State(state.clone()),
        axum::Extension(identity.clone()),
        axum::extract::Path(success.to_string()),
    )
    .await
    .unwrap();
    let result: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), 16384)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(result["controls"]["accepted"], body["controls"]["accepted"]);
    assert!(result["controls"]["execution"].is_object());
    assert_eq!(result["rows"][0]["value"], 1);

    // Transaction-local guard cannot leak to a subsequent legacy start.
    assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
        .bind(success.to_string())
        .execute(&admin)
        .await
        .is_ok()); // terminal no-op
    let legacy = state
        .jobs
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .unwrap();
    assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
        .bind(legacy.to_string())
        .execute(&admin)
        .await
        .is_ok());
    // Primary unavailable at actual dequeue: no RUNNING/planning, safe reason.
    let (release, held) = hold_batch(&state).await;
    let (unavailable, _) = submit(&state, identity.clone(), 4, false).await;
    sqlx::query("ALTER TABLE query_policy.agent_limits RENAME TO agent_limits_unavailable")
        .execute(&admin)
        .await
        .unwrap();
    release.send(()).unwrap();
    held.await.unwrap();
    let info = terminal(&state, unavailable).await;
    assert_eq!(info.status, JobStatus::Failed);
    assert!(info.started_at.is_none());
    assert_eq!(info.error.as_deref(), Some("query_policy_unavailable"));
    sqlx::query("ALTER TABLE query_policy.agent_limits_unavailable RENAME TO agent_limits")
        .execute(&admin)
        .await
        .unwrap();
    // Local cancellation remains authoritative for queued work.
    let (release, held) = hold_batch(&state).await;
    let (cancelled, _) = submit(&state, identity, 4, false).await;
    state.jobs.cancel(cancelled).await.unwrap();
    release.send(()).unwrap();
    held.await.unwrap();
    assert_eq!(
        terminal(&state, cancelled).await.status,
        JobStatus::Cancelled
    );

    // Remote DELETE persists the same CAS; the submitting owner alone aborts
    // the process-local future. The shared peer never claims or replays SQL.
    let socket = std::env::var("SIGLAKE_POLICY_FIXTURE_DIR")
        .unwrap()
        .replace('/', "%2F");
    let remote = crate::jobs::JobStore::new_postgres(
        &format!("postgres://fixture_admin:@{socket}:5432/qh_fixture?sslmode=disable"),
        1,
        Duration::from_secs(60),
        Default::default(),
    )
    .await
    .unwrap();
    let (release, held) = hold_batch(&state).await;
    let (remote_id, _) = submit(&state, CallerIdentity::default(), 4, false).await;
    assert!(remote.cancel(remote_id).await.unwrap());
    assert!(state
        .jobs
        .propagate_cancellations()
        .await
        .unwrap()
        .contains(&remote_id));
    release.send(()).unwrap();
    held.await.unwrap();
    assert_eq!(
        terminal(&state, remote_id).await.status,
        JobStatus::Cancelled
    );
    remote.shutdown().await.unwrap();
    // A blocked start association is within queue+run and never plans late.
    let (release, held) = hold_batch(&state).await;
    let (blocked, _) = submit(&state, CallerIdentity::default(), 1, false).await;
    let mut locked = admin.begin().await.unwrap();
    sqlx::query("SELECT job_id FROM siglake_query_job_controls WHERE job_id=$1 FOR UPDATE")
        .bind(blocked.to_string())
        .fetch_one(&mut *locked)
        .await
        .unwrap();
    release.send(()).unwrap();
    held.await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    locked.rollback().await.unwrap();
    let info = terminal(&state, blocked).await;
    assert_eq!(info.status, JobStatus::Timeout);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    // A missing or malformed durable context cannot downgrade to legacy work.
    for malformed in [false, true] {
        let (release, held) = hold_batch(&state).await;
        let (id, _) = submit(&state, CallerIdentity::default(), 4, false).await;
        let context = state.jobs.controlled_context(id).await.unwrap();
        sqlx::query("ALTER TABLE siglake_query_job_controls DISABLE TRIGGER siglake_controlled_context_immutable").execute(&admin).await.unwrap();
        if malformed {
            sqlx::query("UPDATE siglake_query_job_controls SET context_json=jsonb_set(context_json,'{version}','2') WHERE job_id=$1").bind(id.to_string()).execute(&admin).await.unwrap();
        } else {
            sqlx::query("DELETE FROM siglake_query_job_controls WHERE job_id=$1")
                .bind(id.to_string())
                .execute(&admin)
                .await
                .unwrap();
        }
        sqlx::query("ALTER TABLE siglake_query_job_controls ENABLE TRIGGER siglake_controlled_context_immutable").execute(&admin).await.unwrap();
        assert!(state.jobs.controlled_context(id).await.is_err());
        assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
            .bind(id.to_string())
            .execute(&admin)
            .await
            .is_err());
        release.send(()).unwrap();
        held.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let status: String =
                    sqlx::query_scalar("SELECT status FROM siglake_query_jobs WHERE job_id=$1")
                        .bind(id.to_string())
                        .fetch_one(&admin)
                        .await
                        .unwrap();
                if status == "failed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        // Only this fixture's superuser simulates/repairs corruption. Runtime
        // has neither automatic schema upgrade nor metadata fabrication.
        sqlx::query("ALTER TABLE siglake_query_job_controls DISABLE TRIGGER siglake_controlled_context_immutable").execute(&admin).await.unwrap();
        sqlx::query("INSERT INTO siglake_query_job_controls(job_id,context_json) VALUES ($1,$2) ON CONFLICT(job_id) DO UPDATE SET context_json=EXCLUDED.context_json").bind(id.to_string()).bind(serde_json::to_value(context).unwrap()).execute(&admin).await.unwrap();
        sqlx::query("ALTER TABLE siglake_query_job_controls ENABLE TRIGGER siglake_controlled_context_immutable").execute(&admin).await.unwrap();
        let info = terminal(&state, id).await;
        assert!(info.started_at.is_none());
        assert!(info.cost.is_none());
    }
    // A start transaction's local GUC is absent on the SAME pool after commit.
    // Hold the batch queue so the test can exercise a repeat legacy start before
    // any real execution. A later DELETE still wins the terminal CAS.
    let (release, held) = hold_batch(&state).await;
    let (guarded, _) = submit(&state, CallerIdentity::default(), 4, false).await;
    let context = state.jobs.controlled_context(guarded).await.unwrap();
    let snapshot = context.controls.accepted.clone();
    state
        .jobs
        .start_controlled(
            guarded,
            &context,
            &snapshot,
            context.controls.original_expires_at,
        )
        .await
        .unwrap();
    let pool = state.jobs.controlled_pool_for_test();
    let guard: Option<String> =
        sqlx::query_scalar("SELECT current_setting('siglake.controlled_start',true)")
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(guard.as_deref().unwrap_or("").is_empty());
    assert!(sqlx::query(crate::jobs::SET_RUNNING_SQL)
        .bind(guarded.to_string())
        .execute(pool)
        .await
        .is_err());
    state.jobs.cancel(guarded).await.unwrap();
    release.send(()).unwrap();
    held.await.unwrap();
    assert_eq!(terminal(&state, guarded).await.status, JobStatus::Cancelled);
    // Block all handoff store reads. The original ingress/operator handoff
    // bound acts even with zero run budget and caller false breakers.
    let mut lock = admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE siglake_query_jobs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let req=serde_json::from_value(serde_json::json!({"query":"SELECT 1","priority":"batch","limits":{"timeout_seconds":0,"circuit_breakers":false}})).unwrap();
    let started = Instant::now();
    let error = runtime::ingress(
        started,
        crate::sql::handle_local(
            State(state.clone()),
            axum::Extension(CallerIdentity::default()),
            Json(req),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(error.status, 504);
    assert!(started.elapsed() < Duration::from_secs(6));
    lock.rollback().await.unwrap();

    // INSERT lock wait belongs to handoff, not the short positive run budget.
    let mut insert_lock = admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE siglake_query_jobs IN SHARE MODE")
        .execute(&mut *insert_lock)
        .await
        .unwrap();
    let unlock = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1200)).await;
        insert_lock.rollback().await.unwrap();
    });
    let before = chrono::Utc::now();
    let (after_wait, accepted) = submit(&state, CallerIdentity::default(), 1, false).await;
    unlock.await.unwrap();
    let accepted_at = accepted["controls"]["accepted_at"]
        .as_str()
        .unwrap()
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap();
    assert!((accepted_at - before).num_milliseconds() >= 1100);
    assert_eq!(
        terminal(&state, after_wait).await.status,
        JobStatus::Succeeded
    );
    // ACK hooks pause the REAL commit path in this disposable store. No HTTP
    // or job SQL/INSERT/COMMIT is replayed by the executor.
    for (after, fail, drop_http) in [
        (false, true, true),
        (true, true, false),
        (true, false, true),
    ] {
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        state
            .jobs
            .controlled_commit_hook(crate::jobs::controlled::CommitHook {
                after,
                reached: reached_tx,
                release: release_rx,
            });
        let owned = state.clone();
        let request = tokio::spawn(async move {
            let req=serde_json::from_value(serde_json::json!({"query":"SELECT 1 AS value","priority":"batch","limits":{"timeout_seconds":1,"circuit_breakers":false}})).unwrap();
            runtime::ingress(
                Instant::now(),
                crate::sql::handle_local(
                    State(owned),
                    axum::Extension(CallerIdentity::default()),
                    Json(req),
                ),
            )
            .await
        });
        let id = reached_rx.await.unwrap();
        if drop_http {
            request.abort();
        }
        release_tx.send(fail).unwrap();
        let response = request.await;
        if !drop_http {
            assert_eq!(response.unwrap().unwrap_err().status, 503);
        }
        if fail {
            // A pre-commit failure may remain absent indefinitely; it must NOT
            // vanish from the uncertain ledger on those zero-row passes.
            for _ in 0..4 {
                state.jobs.reconcile_finished_jobs().await;
            }
            if after {
                let info = terminal(&state, id).await;
                assert_eq!(info.status, JobStatus::Failed);
                assert!(info.started_at.is_none());
                assert!(info.cost.is_none());
            } else {
                assert!(state.jobs.info(id).await.unwrap().is_none());
            }
        } else {
            assert_eq!(terminal(&state, id).await.status, JobStatus::Succeeded);
        }
    }

    // DELETE wins even when the commit response is ambiguous: reconciliation
    // never overwrites an authoritative terminal state for better telemetry.
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    state
        .jobs
        .controlled_commit_hook(crate::jobs::controlled::CommitHook {
            after: true,
            reached: reached_tx,
            release: release_rx,
        });
    let owned = state.clone();
    let request = tokio::spawn(async move {
        let req =
            serde_json::from_value(serde_json::json!({"query":"SELECT 1","priority":"batch"}))
                .unwrap();
        runtime::ingress(
            Instant::now(),
            crate::sql::handle_local(
                State(owned),
                axum::Extension(CallerIdentity::default()),
                Json(req),
            ),
        )
        .await
    });
    let cancelled_commit = reached_rx.await.unwrap();
    assert!(state.jobs.cancel(cancelled_commit).await.unwrap());
    release_tx.send(true).unwrap();
    assert_eq!(request.await.unwrap().unwrap_err().status, 503);
    state.jobs.reconcile_finished_jobs().await;
    assert_eq!(
        terminal(&state, cancelled_commit).await.status,
        JobStatus::Cancelled
    );
    // Timeout after a real COMMIT but before ACK suppresses execution, and
    // retained reconciliation installs a safe failure, never a replay.
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let (_release, release_rx) = tokio::sync::oneshot::channel();
    state
        .jobs
        .controlled_commit_hook(crate::jobs::controlled::CommitHook {
            after: true,
            reached: reached_tx,
            release: release_rx,
        });
    let owned = state.clone();
    let request = tokio::spawn(async move {
        let req=serde_json::from_value(serde_json::json!({"query":"SELECT 1","priority":"batch","limits":{"timeout_seconds":1}})).unwrap();
        runtime::ingress(
            Instant::now(),
            crate::sql::handle_local(
                State(owned),
                axum::Extension(CallerIdentity::default()),
                Json(req),
            ),
        )
        .await
    });
    let unknown = reached_rx.await.unwrap();
    assert_eq!(request.await.unwrap().unwrap_err().status, 504);
    // Allow owner timeout branch to publish its uncertainty, not fake-abort proof.
    tokio::time::sleep(Duration::from_millis(20)).await;
    state.jobs.reconcile_finished_jobs().await;
    let info = terminal(&state, unknown).await;
    assert_eq!(info.status, JobStatus::Failed);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    // First absent, then late COMMIT visibility beyond the fast attempts.
    let reservation = state.jobs.reserve_controlled_commit().unwrap();
    let late = reservation.id();
    reservation.writing();
    drop(reservation);
    let context = crate::query_controls::jobs::JobContext::new(
        state.jobs.owner_id(),
        &CallerIdentity::default(),
        runtime::test_controls(Duration::from_secs(1)),
    )
    .unwrap();
    let mut delayed = admin.begin().await.unwrap();
    sqlx::query("INSERT INTO siglake_query_jobs(job_id,status,priority,submitted_at,query,owner,controls_version) VALUES ($1,'pending','batch',$2,'SELECT 1',$3,1)").bind(late.to_string()).bind(context.controls.accepted_at).bind(state.jobs.owner_id()).execute(&mut *delayed).await.unwrap();
    sqlx::query("INSERT INTO siglake_query_job_controls(job_id,context_json) VALUES ($1,$2)")
        .bind(late.to_string())
        .bind(serde_json::to_value(&context).unwrap())
        .execute(&mut *delayed)
        .await
        .unwrap();
    for _ in 0..4 {
        assert!(state.jobs.reconcile_finished_jobs().await.retained >= 1);
    }
    delayed.commit().await.unwrap();
    assert_eq!(state.jobs.reconcile_finished_jobs().await.installed, 1);
    let info = terminal(&state, late).await;
    assert_eq!(info.status, JobStatus::Failed);
    assert!(info.started_at.is_none());
    // Full uncertainty capacity refuses BEFORE write, preserving all old IDs.
    let mut slots = Vec::new();
    while let Ok(slot) = state.jobs.reserve_controlled_commit() {
        slots.push(slot);
    }
    let count_before: i64 = sqlx::query_scalar("SELECT count(*) FROM siglake_query_jobs")
        .fetch_one(&admin)
        .await
        .unwrap();
    let req =
        serde_json::from_value(serde_json::json!({"query":"SELECT 1","priority":"batch"})).unwrap();
    let refusal = runtime::ingress(
        Instant::now(),
        crate::sql::handle_local(
            State(state.clone()),
            axum::Extension(CallerIdentity::default()),
            Json(req),
        ),
    )
    .await
    .unwrap_err();
    assert_eq!(refusal.status, 503);
    let count_after: i64 = sqlx::query_scalar("SELECT count(*) FROM siglake_query_jobs")
        .fetch_one(&admin)
        .await
        .unwrap();
    assert_eq!(count_before, count_after);
    drop(slots);

    // Original history receipt + a real blocked association consume the SAME
    // accepted lifetime. Queue most of that second before first batch poll.
    let socket = std::env::var("SIGLAKE_POLICY_FIXTURE_DIR")
        .unwrap()
        .replace('/', "%2F");
    let options = format!("postgres://qh_writer:@{socket}:5432/qh_fixture?sslmode=disable")
        .parse::<sqlx::postgres::PgConnectOptions>()
        .unwrap()
        .disable_statement_logging();
    let writer = crate::history::HistoryWriter::connect(options)
        .await
        .unwrap();
    let mut history = crate::history::HistoryService::new(writer);
    let observed = state
        .clone()
        .with_tenants(crate::TenantRegistry::new(state.ice.clone()))
        .with_history(history.observer());
    sqlx::raw_sql("CREATE FUNCTION public.durable_history_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.job_id IS NULL AND NEW.job_id IS NOT NULL THEN PERFORM pg_advisory_xact_lock(682307); END IF; RETURN NEW; END $$; REVOKE ALL ON FUNCTION public.durable_history_gate() FROM PUBLIC; CREATE TRIGGER durable_history_gate BEFORE UPDATE ON query_history.executions FOR EACH ROW EXECUTE FUNCTION public.durable_history_gate();").execute(&admin).await.unwrap();
    let mut advisory = admin.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(682307)")
        .execute(&mut *advisory)
        .await
        .unwrap();
    let (release, held) = hold_batch(&state).await;
    let caller = CallerIdentity {
        tenant: Some("durable".into()),
        provider_principal: Some(crate::oidc::VerifiedProviderPrincipal {
            issuer: "https://synthetic.invalid/exact/".into(),
            client_id: "durable-client".into(),
        }),
        ..Default::default()
    };
    let (bound, _) = submit(&observed, caller, 1, false).await;
    let receipt: uuid::Uuid =
        sqlx::query_scalar("SELECT history_execution_id FROM siglake_query_jobs WHERE job_id=$1")
            .bind(bound.to_string())
            .fetch_one(&admin)
            .await
            .unwrap();
    tokio::time::sleep(Duration::from_millis(950)).await;
    release.send(()).unwrap();
    held.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1),async{loop{let n:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE '%UPDATE query_history.executions SET job_id%'").fetch_one(&admin).await.unwrap();if n>0{break;}tokio::task::yield_now().await;}}).await.unwrap();
    let info = terminal(&state, bound).await;
    assert_eq!(info.status, JobStatus::Timeout);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    sqlx::query("SELECT pg_advisory_unlock(682307)")
        .execute(&mut *advisory)
        .await
        .unwrap();
    drop(advisory);
    sqlx::raw_sql("DROP TRIGGER durable_history_gate ON query_history.executions; DROP FUNCTION public.durable_history_gate();").execute(&admin).await.unwrap();
    let receipt_row=sqlx::query("SELECT (expires_at-received_at)=interval '720 hours' AS original_expiry, redacted_sql IS NULL AS no_sql FROM query_history.executions WHERE execution_id=$1").bind(receipt).fetch_one(&admin).await.unwrap();
    assert!(receipt_row.get::<bool, _>("original_expiry"));
    assert!(receipt_row.get::<bool, _>("no_sql"));
    let _ = history.shutdown().await;
    // Explicit shutdown cancels/joins retained ownership; pending durable work
    // is then failed by owner recovery, NEVER replayed.
    let (release, held) = hold_batch(&state).await;
    let (orphan, _) = submit(&state, CallerIdentity::default(), 4, false).await;
    let owned = state.clone();
    let shutdown = tokio::spawn(async move {
        owned.shutdown_query_controls().await;
    });
    tokio::task::yield_now().await;
    assert!(state
        .jobs
        .info(orphan)
        .await
        .unwrap()
        .unwrap()
        .started_at
        .is_none());
    release.send(()).unwrap();
    held.await.unwrap();
    shutdown.await.unwrap();
    state.shutdown_query_controls().await;
    state.jobs.shutdown().await.unwrap();
    let socket = std::env::var("SIGLAKE_POLICY_FIXTURE_DIR")
        .unwrap()
        .replace('/', "%2F");
    let peer = crate::jobs::JobStore::new_postgres(
        &format!("postgres://fixture_admin:@{socket}:5432/qh_fixture?sslmode=disable"),
        1,
        Duration::from_secs(60),
        Default::default(),
    )
    .await
    .unwrap();
    let info = peer.info(orphan).await.unwrap().unwrap();
    assert_eq!(info.status, JobStatus::Failed);
    assert!(info.started_at.is_none());
    assert!(info.cost.is_none());
    peer.shutdown().await.unwrap();
    // Parent deletion still cascades immutable context at terminal TTL cleanup.
    sqlx::query("DELETE FROM siglake_query_jobs")
        .execute(&admin)
        .await
        .unwrap();
    let count: i64 = sqlx::query("SELECT count(*) AS n FROM siglake_query_job_controls")
        .fetch_one(&admin)
        .await
        .unwrap()
        .get("n");
    assert_eq!(count, 0);
    sqlx::raw_sql("DROP TABLE siglake_query_job_controls; DROP TRIGGER siglake_controlled_job_version ON siglake_query_jobs; DROP TRIGGER siglake_controlled_job_start ON siglake_query_jobs; ALTER TABLE siglake_query_jobs DROP COLUMN controls_version; DROP FUNCTION siglake_guard_controlled_job(); DROP FUNCTION siglake_guard_job_context();").execute(&admin).await.unwrap();
}

#[tokio::test]
async fn controlled_running_publication_error_never_polls_planning_unlike_legacy_driver() {
    let jobs = Arc::new(crate::jobs::JobStore::new(1, Duration::from_secs(60)));
    let id = jobs
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .unwrap();
    let lifecycle = JobLifecycle {
        jobs: jobs.clone(),
        id,
    };
    let identity = CallerIdentity::default();
    let plans = std::sync::atomic::AtomicUsize::new(0);
    drive_controlled_batch_job(
        BatchJobReporting {
            lifecycle: &lifecycle,
            audit: None,
            identity: &identity,
            query_sql: "SELECT 1",
            deadline: BatchDeadline::starting_now(Duration::from_secs(1), true),
        },
        async {
            plans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            BatchOutcome::Failed("must never execute".into(), None)
        },
        Err(anyhow::anyhow!("injected running publication store error")),
        None,
    )
    .await;
    assert_eq!(plans.load(std::sync::atomic::Ordering::SeqCst), 0);
    let info = jobs.info(id).await.unwrap().unwrap();
    assert_eq!(info.status, JobStatus::Failed);
    assert!(info.cost.is_none());
    assert!(info.started_at.is_none());
    assert_eq!(
        info.error.as_deref(),
        Some("query_job_controls_unavailable")
    );
}

#[tokio::test]
async fn controlled_late_ready_execution_cannot_persist_success() {
    let jobs = Arc::new(crate::jobs::JobStore::new(1, Duration::from_secs(60)));
    let id = jobs
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .unwrap();
    let lifecycle = JobLifecycle {
        jobs: jobs.clone(),
        id,
    };
    let identity = CallerIdentity::default();
    drive_controlled_batch_job(
        BatchJobReporting {
            lifecycle: &lifecycle,
            audit: None,
            identity: &identity,
            query_sql: "SELECT 1",
            deadline: BatchDeadline::starting_now(Duration::from_millis(10), true),
        },
        async {
            std::thread::sleep(Duration::from_millis(30));
            let body =
                serde_json::from_value(serde_json::json!({"columns":[],"rows":[],"row_count":0}))
                    .unwrap();
            BatchOutcome::Ok(body, CostReport::unknown_after_timeout())
        },
        Ok(CompletionOutcome::Applied {
            status: JobStatus::Running,
        }),
        None,
    )
    .await;
    assert_eq!(
        jobs.info(id).await.unwrap().unwrap().status,
        JobStatus::Timeout
    );
    jobs.shutdown().await.unwrap();
}
#[tokio::test]
async fn controlled_terminal_cleanup_has_its_own_mandatory_budget_after_execution_ends() {
    let jobs = Arc::new(crate::jobs::JobStore::new(1, Duration::from_secs(60)));
    let id = jobs
        .submit("SELECT 1".into(), Priority::Batch, None)
        .await
        .unwrap();
    let blocked = jobs.wedge_for_test().await;
    let (phase, mut changed) = tokio::sync::watch::channel(Some(
        tokio::time::Instant::now() + Duration::from_millis(100),
    ));
    let owned = jobs.clone();
    let driver = tokio::spawn(async move {
        let lifecycle = JobLifecycle { jobs: owned, id };
        let identity = CallerIdentity::default();
        drive_controlled_batch_job(
            BatchJobReporting {
                lifecycle: &lifecycle,
                audit: None,
                identity: &identity,
                query_sql: "SELECT 1",
                deadline: BatchDeadline::starting_now(Duration::from_millis(100), true),
            },
            async {
                let body = serde_json::from_value(
                    serde_json::json!({"columns":[],"rows":[],"row_count":0}),
                )
                .unwrap();
                BatchOutcome::Ok(body, CostReport::unknown_after_timeout())
            },
            Ok(CompletionOutcome::Applied {
                status: JobStatus::Running,
            }),
            Some(&phase),
        )
        .await;
    });
    changed.changed().await.unwrap();
    assert!(changed.borrow().is_none());
    tokio::time::sleep(Duration::from_millis(130)).await;
    assert!(!driver.is_finished());
    drop(blocked);
    driver.await.unwrap();
    assert_eq!(
        jobs.info(id).await.unwrap().unwrap().status,
        JobStatus::Succeeded
    );
    jobs.shutdown().await.unwrap();
}
