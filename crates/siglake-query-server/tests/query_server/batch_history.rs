//! Actual local batch router + engine + signed local issuer + owned PostgreSQL.
//! Gates are private fixture backpressure, never production execution knobs.
use super::*;
use futures::FutureExt;
use siglake_query_server::jobs::{JobId, JobRecoveryPolicy, JobStore};

fn jobs_uri(socket: &std::path::Path) -> String {
    format!(
        "postgresql://qh_jobs@localhost/qh_fixture?host={}&sslmode=disable",
        socket.display()
    )
}
async fn jobs(socket: &std::path::Path) -> JobStore {
    JobStore::new_postgres(
        &jobs_uri(socket),
        1,
        Duration::from_secs(600),
        JobRecoveryPolicy::default(),
    )
    .await
    .unwrap()
}
pub(super) struct QueueGate(Option<std::sync::mpsc::Sender<()>>);
impl QueueGate {
    pub(super) async fn hold(jobs: &JobStore) -> Self {
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        jobs.batch_runtime().spawn(async move {
            let _ = ready.send(());
            let _ = held.recv();
        });
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        Self(Some(release))
    }
    pub(super) fn release(mut self) {
        let _ = self.0.take().unwrap().send(());
    }
}
impl Drop for QueueGate {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
async fn submit(s: &Server, token: &str, query: &str, extra: Value) -> (Uuid, JobId) {
    let mut body = json!({"query":query,"priority":"batch","history_execution_id":Uuid::nil(),"history_id":Uuid::nil()});
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let response = post(s, token, "/api/v1/sql/local", body).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let execution = correlation(&response);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(
        body.as_object().unwrap().len(),
        4,
        "unchanged public handoff DTO"
    );
    (
        execution,
        JobId::parse(body["job_id"].as_str().unwrap()).unwrap(),
    )
}
async fn job_response(s: &Server, token: &str, job: JobId, cancel: bool) -> reqwest::Response {
    let url = format!("{}/api/v1/jobs/{job}", s.base);
    let client = reqwest::Client::new();
    let request = if cancel {
        client.delete(url)
    } else {
        client.get(url)
    };
    request
        .bearer_auth(token)
        .header(CORRELATION, Uuid::nil().to_string())
        .send()
        .await
        .unwrap()
}
async fn persisted(db: &PgPool, job: JobId, status: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let found: String =
                sqlx::query_scalar("SELECT status FROM siglake_query_jobs WHERE job_id=$1")
                    .bind(job.to_string())
                    .fetch_one(db)
                    .await
                    .unwrap();
            if found == status {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("authoritative job transition");
}
async fn pending(db: &PgPool, execution: Uuid, job: JobId) -> Value {
    pending_association(db, execution, job, true).await
}
async fn queued_pending(db: &PgPool, execution: Uuid, job: JobId) -> Value {
    // QueueGate holds the worker before the owned task's first poll. The job
    // pointer is durable already; 202 does not promise a history-side bind yet.
    pending_association(db, execution, job, false).await
}
async fn pending_association(db: &PgPool, execution: Uuid, job: JobId, bound: bool) -> Value {
    let row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
    )
    .bind(execution)
    .fetch_one(db)
    .await
    .unwrap();
    assert_eq!(row["observation_state"], "pending");
    assert!(row["outcome"].is_null());
    assert!(
        row["execution_admitted"].is_null(),
        "202 and reservation are NOT execution admission"
    );
    if bound {
        assert_eq!(row["job_id"], job.to_string());
    } else {
        assert!(
            row["job_id"].is_null(),
            "unpolled task cannot have associated"
        );
    }
    let link: Option<Uuid> =
        sqlx::query_scalar("SELECT history_execution_id FROM siglake_query_jobs WHERE job_id=$1")
            .bind(job.to_string())
            .fetch_one(db)
            .await
            .unwrap();
    assert_eq!(link, Some(execution));
    row
}
async fn wait_query(db: &PgPool, pattern: &str, wait: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let found: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND datname=current_database() AND query LIKE $1 AND wait_event=$2)").bind(pattern).bind(wait).fetch_one(db).await.unwrap();
            if found { return; }
            tokio::task::yield_now().await;
        }
    }).await.expect("private PG gate was not reached");
}
async fn install_cost_gate(db: &PgPool) {
    // Only cost publication takes the advisory lock. The row then supplies real
    // backpressure and PostgreSQL's lock queue orders cancellation/completion.
    sqlx::raw_sql("CREATE OR REPLACE FUNCTION public.batch_history_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.cost_json IS NOT NULL AND OLD.cost_json IS NULL THEN PERFORM pg_advisory_xact_lock(682305); END IF; RETURN NEW; END $$; REVOKE ALL ON FUNCTION public.batch_history_gate() FROM PUBLIC; DROP TRIGGER IF EXISTS batch_history_gate ON siglake_query_jobs; CREATE TRIGGER batch_history_gate BEFORE UPDATE ON siglake_query_jobs FOR EACH ROW EXECUTE FUNCTION public.batch_history_gate();").execute(db).await.unwrap();
}
async fn remove_cost_gate(db: &PgPool) {
    sqlx::raw_sql("DROP TRIGGER batch_history_gate ON siglake_query_jobs; DROP FUNCTION public.batch_history_gate();").execute(db).await.unwrap();
}

#[tokio::test]
#[ignore = "owned PG fixture; scripts/check-query-history-batch.sh"]
async fn real_router_batch_queue_execution_cancellation_identity_and_privacy() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("PRIVATE_BATCH_RESULT")])
        .await
        .unwrap();
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_jobs(jobs(&f.socket).await)
        .with_history(service.observer());
    let s = server(state.clone()).await;
    let original = token(&issuer, "acme", "original-client", 0);
    let renewed = token(&issuer, "acme", "original-client", 90);
    let canceller = token(&issuer, "acme", "different-client", 0);
    let foreign = token(&issuer, "foreign", "foreign-client", 0);

    let gate = QueueGate::hold(&state.jobs).await;
    let (execution, job) = submit(&s, &original, "SELECT raw FROM events", json!({})).await;
    let initial = queued_pending(&f.admin, execution, job).await;
    assert_eq!(
        job_response(&s, &foreign, job, false).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        job_response(&s, &foreign, job, true).await.status(),
        StatusCode::NOT_FOUND
    );
    let info = job_response(&s, &renewed, job, false)
        .await
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(info["status"], "pending");
    assert!(info.get("history_execution_id").is_none());
    assert!(info.get("tenant").is_none());
    gate.release();
    let success = row(&f.admin, execution).await;
    persisted(&f.admin, job, "succeeded").await;
    assert_eq!(success["outcome"], "success");
    assert_eq!(success["execution_admitted"], true);
    assert_eq!(success["returned_rows"], 1);
    assert_eq!(success["job_id"], job.to_string());
    assert_eq!(success["received_at"], initial["received_at"]);
    assert_eq!(success["expires_at"], initial["expires_at"]);
    assert_eq!(
        job_response(&s, &renewed, job, true).await.status(),
        StatusCode::CONFLICT
    );

    let gate = QueueGate::hold(&state.jobs).await;
    let (execution, job) = submit(&s, &original, "SELECT raw FROM events", json!({})).await;
    let initial = queued_pending(&f.admin, execution, job).await;
    assert_eq!(
        job_response(&s, &canceller, job, true).await.status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        job_response(&s, &canceller, job, true).await.status(),
        StatusCode::CONFLICT
    );
    // No fabricated terminal before the queued execution owner acknowledges
    // dropping the future. Freeing its worker does not execute cancelled SQL.
    gate.release();
    let cancelled = row(&f.admin, execution).await;
    persisted(&f.admin, job, "cancelled").await;
    assert_eq!(cancelled["outcome"], "cancelled");
    assert_eq!(cancelled["execution_admitted"], false);
    assert_eq!(cancelled["job_id"], job.to_string());
    assert_eq!(cancelled["received_at"], initial["received_at"]);
    assert_eq!(cancelled["expires_at"], initial["expires_at"]);
    assert_eq!(
        cancelled["client_id"], "original-client",
        "not the canceller"
    );
    let info = state.jobs.info(job).await.unwrap().unwrap();
    assert!(info.started_at.is_none() && info.cost.is_none());

    for (query, extra, expected, admitted) in [
        ("SELECT missing FROM events", json!({}), "failed", true),
        (
            "SELECT raw FROM events",
            json!({"limits":{"timeout_seconds":0}}),
            "timed_out",
            false,
        ),
    ] {
        let (execution, job) = submit(&s, &original, query, extra).await;
        let terminal = row(&f.admin, execution).await;
        assert_eq!(terminal["outcome"], expected);
        assert_eq!(terminal["execution_admitted"], admitted);
        persisted(
            &f.admin,
            job,
            if expected == "timed_out" {
                "timeout"
            } else {
                "failed"
            },
        )
        .await;
    }
    let before = state.jobs.active_count().await;
    let rejected = post(
        &s,
        &original,
        "/api/v1/sql/local",
        json!({"query":"SELECT (","priority":"batch"}),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let rejection = row(&f.admin, correlation(&rejected)).await;
    assert_eq!(rejection["outcome"], "rejected");
    assert_eq!(rejection["execution_admitted"], false);
    assert!(rejection["job_id"].is_null());
    assert_eq!(state.jobs.active_count().await, before);
    let _all = state
        .admission
        .acquire(state.limits.admission_budget_bytes)
        .await
        .unwrap();
    let refused = post(
        &s,
        &original,
        "/api/v1/sql/local",
        json!({"query":"SELECT 1","priority":"batch"}),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
    let refused = row(&f.admin, correlation(&refused)).await;
    assert_eq!(refused["error_code"], "admission");
    assert!(refused["job_id"].is_null());
    drop(_all);

    // Configured local batch now retains one parent; no-membership explicit
    // rejection is not admitted. Default/auth failures remain unobserved.
    let configured = server(
        state
            .clone()
            .with_coordinator(vec!["http://127.0.0.1:1".into()], None),
    )
    .await;
    let before = count(&f.admin).await;
    let response = post(
        &configured,
        &original,
        "/api/v1/sql",
        json!({"query":"SELECT 1","priority":"batch"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        row(&f.admin, correlation(&response)).await["outcome"],
        "success"
    );
    assert_eq!(count(&f.admin).await, before + 1);
    observed(
        &s,
        &original,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1","priority":"batch"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    unobserved(
        &s,
        "invalid-synthetic-token",
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT 1","priority":"batch"}),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let mut shared = state.clone();
    shared.tenants = None;
    let shared = server(shared).await;
    unobserved(
        &shared,
        &original,
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT 1","priority":"batch"}),
        StatusCode::ACCEPTED,
    )
    .await;

    let rows: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM query_history.executions e")
        .fetch_all(&f.admin)
        .await
        .unwrap();
    for r in rows {
        assert!(r["redacted_sql"].is_null());
        assert!(r["measured_rows_scanned"].is_null());
        assert!(r["measured_bytes_scanned"].is_null());
        assert!(!r.to_string().contains("PRIVATE_BATCH_RESULT"));
        assert_eq!(r["client_id"], "original-client");
        assert!(r["admitted_at"].is_null() && r["finished_at"].is_null());
    }
    state.jobs.shutdown().await.unwrap();
    let _ = service.shutdown().await;
    issuer_task.abort();
}

#[tokio::test]
#[ignore = "owned PG fixture; scripts/check-query-history-batch.sh"]
async fn real_router_batch_running_cancel_completion_race_timeout_and_history_outage() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("PRIVATE_BATCH_RESULT")])
        .await
        .unwrap();
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_jobs(jobs(&f.socket).await)
        .with_history(service.observer());
    let s = server(state.clone()).await;
    let t = token(&issuer, "acme", "original-client", 0);
    install_cost_gate(&f.admin).await;
    let mut lock = f.admin.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(682305)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let (execution, job) = submit(&s, &t, "SELECT raw FROM events", json!({})).await;
    wait_query(&f.admin, "%SET cost_json%", "advisory").await;
    persisted(&f.admin, job, "running").await;
    let cancel = job_response(&s, &t, job, true);
    tokio::pin!(cancel);
    // Drive the real HTTP DELETE until PostgreSQL confirms it is queued on the
    // execution's row, then release the fixture gate. No arbitrary sleeps.
    tokio::select! {
        result = &mut cancel => panic!("cancel escaped held row: {}", result.status()),
        () = wait_query(&f.admin, "%SET status = 'cancelled'%", "transactionid") => {}
    }
    sqlx::query("SELECT pg_advisory_unlock(682305)")
        .execute(&mut *lock)
        .await
        .unwrap();
    assert_eq!(cancel.await.status(), StatusCode::ACCEPTED);
    persisted(&f.admin, job, "cancelled").await;
    let terminal = row(&f.admin, execution).await;
    assert_eq!(terminal["outcome"], "cancelled");
    assert_eq!(terminal["execution_admitted"], true);
    assert_eq!(
        job_response(&s, &t, job, true).await.status(),
        StatusCode::CONFLICT
    );

    sqlx::query("SELECT pg_advisory_lock(682305)")
        .execute(&mut *lock)
        .await
        .unwrap();
    let (execution, job) = submit(
        &s,
        &t,
        "SELECT raw FROM events",
        json!({"limits":{"timeout_seconds":1}}),
    )
    .await;
    wait_query(&f.admin, "%SET cost_json%", "advisory").await;
    // The actual execution deadline expires while the original cost publication
    // is blocked. Its terminal UPDATE then queues behind that same row.
    wait_query(&f.admin, "%SET status = $1%", "transactionid").await;
    sqlx::query("SELECT pg_advisory_unlock(682305)")
        .execute(&mut *lock)
        .await
        .unwrap();
    persisted(&f.admin, job, "timeout").await;
    let timeout = row(&f.admin, execution).await;
    assert_eq!(timeout["outcome"], "timed_out");
    assert_eq!(timeout["execution_admitted"], true);
    remove_cost_gate(&f.admin).await;

    // Real table-lock history outage never changes 202/job execution. Original
    // receipt survives an unavailable initial/association/terminal write.
    let mut history_lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *history_lock)
        .await
        .unwrap();
    let started = Instant::now();
    let (execution, job) = submit(&s, &t, "SELECT raw FROM events", json!({})).await;
    assert!(started.elapsed() < Duration::from_secs(2));
    persisted(&f.admin, job, "succeeded").await;
    history_lock.rollback().await.unwrap();
    assert_eq!(row(&f.admin, execution).await["outcome"], "success");
    state.jobs.shutdown().await.unwrap();
    let _ = service.shutdown().await;
    issuer_task.abort();
}

// Own resources outside the scenario future so its cooperative cancellation
// cannot detach the issuer or strand a job owner's upkeep/execution tasks.
#[derive(Default)]
struct RecoveryScenarioResources {
    stores: Vec<Arc<JobStore>>,
    issuer: Option<tokio::task::JoinHandle<()>>,
    cost_gate_installed: bool,
    association_gate_installed: bool,
}
impl RecoveryScenarioResources {
    async fn shutdown(&mut self, db: &PgPool) {
        if let Some(issuer) = &self.issuer {
            issuer.abort();
        }
        // Poll every shutdown, rather than leaving other stores executing behind
        // one blocked database operation. No detached cleanup task is spawned.
        let results =
            futures::future::join_all(self.stores.iter().map(|store| store.shutdown())).await;
        assert!(
            results.iter().all(Result::is_ok),
            "batch recovery fixture store cleanup failed"
        );
        if self.cost_gate_installed {
            sqlx::raw_sql("DROP TRIGGER IF EXISTS batch_history_gate ON siglake_query_jobs; DROP FUNCTION IF EXISTS public.batch_history_gate();")
                .execute(db).await.unwrap();
            self.cost_gate_installed = false;
        }
        if self.association_gate_installed {
            sqlx::raw_sql("DROP TRIGGER IF EXISTS batch_history_association_gate ON query_history.executions; DROP FUNCTION IF EXISTS public.batch_history_association_gate();")
                .execute(db).await.unwrap();
            self.association_gate_installed = false;
        }
        if let Some(issuer) = self.issuer.as_mut() {
            let _ = issuer.await;
        }
        self.issuer.take();
    }
}
impl Drop for RecoveryScenarioResources {
    fn drop(&mut self) {
        if let Some(issuer) = &self.issuer {
            issuer.abort();
        }
    }
}

async fn history_state(db: &PgPool, execution: Uuid, state: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row: Value = sqlx::query_scalar(
                "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
            )
            .bind(execution)
            .fetch_one(db)
            .await
            .unwrap();
            if row["observation_state"] == state {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("history state transition")
}

#[tokio::test]
#[ignore = "owned PG fixture; scripts/check-query-history-batch.sh"]
async fn real_router_batch_original_uuid_recreation_recovery_legacy_and_late_terminal() {
    // Acquire suite serialization BEFORE starting this scenario's own deadline.
    let f = fixture().await;
    let mut resources = RecoveryScenarioResources::default();
    let scenario = async {
        let mut original_service = HistoryService::new(f.writer);
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.for_namespace("tenant_acme")
            .await
            .unwrap()
            .append_events(&[siglake_core::Event::now("PRIVATE_BATCH_RESULT")])
            .await
            .unwrap();
        let (issuer, issuer_task) = spawn_fake_idp().await;
        resources.issuer = Some(issuer_task);
        let verifier =
            OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
                .await
                .unwrap();
        let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
            .with_tenants(TenantRegistry::new(ice))
            .with_jobs(jobs(&f.socket).await)
            .with_history(original_service.observer());
        resources.stores.push(state.jobs.clone());
        let s = server(state.clone()).await;
        let t = token(&issuer, "acme", "original-client", 0);
        let other = token(&issuer, "acme", "new-canceller", 60);

        // Construct the peer before the fixture holds a job-table transaction; the
        // existing job constructor performs owner migrations requiring table locks.
        let replica = state.clone().with_jobs(jobs(&f.socket).await);
        resources.stores.push(replica.jobs.clone());

        // A live job executor survives loss of its ORIGINAL history service owner.
        // Recovery of that stale history owner is uncertainty, not failed SQL.
        resources.cost_gate_installed = true;
        install_cost_gate(&f.admin).await;
        let mut lock = f.admin.acquire().await.unwrap();
        // A cancelled scenario must close this dedicated session, never return a
        // still-advisory-locked connection to the shared fixture pool.
        lock.close_on_drop();
        sqlx::query("SELECT pg_advisory_lock(682305)")
            .execute(&mut *lock)
            .await
            .unwrap();
        let (execution, job) = submit(&s, &t, "SELECT raw FROM events", json!({})).await;
        wait_query(&f.admin, "%SET cost_json%", "advisory").await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let admitted: Option<bool> = sqlx::query_scalar(
                    "SELECT execution_admitted FROM query_history.executions WHERE execution_id=$1",
                )
                .bind(execution)
                .fetch_one(&f.admin)
                .await
                .unwrap();
                if admitted == Some(true) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before: Value = sqlx::query_scalar(
            "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
        )
        .bind(execution)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        let old_owner = original_service.owner_uuid();
        let _ = original_service.shutdown().await;
        sqlx::query("UPDATE query_history.owners SET heartbeat_at=clock_timestamp()-interval '121 seconds' WHERE owner_uuid=$1").bind(old_owner).execute(&f.admin).await.unwrap();
        let mut replacement = HistoryService::new(
            siglake_query_server::history::HistoryWriter::connect(options(&f.socket, "qh_writer"))
                .await
                .unwrap(),
        );
        let uncertain = history_state(&f.admin, execution, "indeterminate").await;
        assert!(uncertain["outcome"].is_null());
        assert_eq!(uncertain["owner_uuid"], before["owner_uuid"]);
        let replica = replica.with_history(replacement.observer());
        let peer = server(replica.clone()).await;
        let cancel = job_response(&peer, &other, job, true);
        tokio::pin!(cancel);
        tokio::select! {
            response = &mut cancel => panic!("cancel escaped cost gate: {}", response.status()),
            () = wait_query(&f.admin, "%SET status = 'cancelled'%", "transactionid") => {}
        }
        sqlx::query("SELECT pg_advisory_unlock(682305)")
            .execute(&mut *lock)
            .await
            .unwrap();
        assert_eq!(cancel.await.status(), StatusCode::ACCEPTED);
        let late = row(&f.admin, execution).await;
        assert_eq!(late["outcome"], "cancelled");
        assert_eq!(late["client_id"], "original-client");
        assert_eq!(late["received_at"], before["received_at"]);
        assert_eq!(late["expires_at"], before["expires_at"]);
        assert_eq!(late["owner_uuid"], before["owner_uuid"]);
        assert!(
            late["duration_ms"].is_null(),
            "no recovery-time monotonic clock"
        );
        let heartbeat: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT heartbeat_at FROM query_history.owners WHERE owner_uuid=$1")
                .bind(old_owner)
                .fetch_one(&f.admin)
                .await
                .unwrap();
        assert!(
            heartbeat < chrono::Utc::now() - chrono::Duration::seconds(120),
            "replacement must not impersonate old heartbeat"
        );
        state.jobs.propagate_cancellations().await.unwrap();
        remove_cost_gate(&f.admin).await;
        resources.cost_gate_installed = false;

        // Controller/store recreation and lease recovery, NOT SIGKILL/process proof.
        // The job API's existing orphan recovery is failed; history stays unknown.
        let mut service = HistoryService::new(
            siglake_query_server::history::HistoryWriter::connect(options(&f.socket, "qh_writer"))
                .await
                .unwrap(),
        );
        let live = state
            .clone()
            .with_jobs(jobs(&f.socket).await)
            .with_history(service.observer());
        resources.stores.push(live.jobs.clone());
        let live_server = server(live.clone()).await;
        let gate = QueueGate::hold(&live.jobs).await;
        let (execution, job) = submit(&live_server, &t, "SELECT raw FROM events", json!({})).await;
        let before = queued_pending(&f.admin, execution, job).await;
        let legacy = live
            .jobs
            .submit(
                "SELECT 17".into(),
                siglake_query_server::limits::Priority::Batch,
                Some("acme".into()),
            )
            .await
            .unwrap();
        let count_before = count(&f.admin).await;
        let _ = service.shutdown().await;
        // Retire this job owner explicitly, then recover through an independent
        // existing controller. No execution is replayed or falsely reconstructed.
        let retire = live.jobs.shutdown();
        tokio::pin!(retire);
        assert!(futures::poll!(&mut retire).is_pending());
        gate.release();
        retire.await.unwrap();
        let outcome = replica.jobs.recover_orphaned_jobs().await.unwrap();
        assert!(outcome.recovered >= 2);
        persisted(&f.admin, job, "failed").await;
        persisted(&f.admin, legacy, "failed").await;
        let uncertain = history_state(&f.admin, execution, "indeterminate").await;
        assert!(uncertain["outcome"].is_null());
        assert_eq!(uncertain["execution_id"], before["execution_id"]);
        assert_eq!(uncertain["tenant"], before["tenant"]);
        assert_eq!(uncertain["client_id"], before["client_id"]);
        assert_eq!(uncertain["received_at"], before["received_at"]);
        assert_eq!(uncertain["expires_at"], before["expires_at"]);
        assert_eq!(
            count(&f.admin).await,
            count_before,
            "no recovery receipt or legacy backfill"
        );
        let link: Option<Uuid> = sqlx::query_scalar(
            "SELECT history_execution_id FROM siglake_query_jobs WHERE job_id=$1",
        )
        .bind(legacy.to_string())
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(link.is_none());
        replica.jobs.shutdown().await.unwrap();
        state.jobs.shutdown().await.unwrap();
        let _ = replacement.shutdown().await;
    };
    let scenario = tokio::time::timeout(
        Duration::from_secs(30),
        std::panic::AssertUnwindSafe(scenario).catch_unwind(),
    )
    .await;
    // Drop of the timed scenario first releases QueueGate and closes its
    // dedicated advisory-lock session. Cleanup remains owned and bounded;
    // Tokio's timeout is cooperative, not an OS-level blocking guarantee.
    let cleanup = tokio::time::timeout(Duration::from_secs(5), resources.shutdown(&f.admin)).await;
    assert!(
        cleanup.is_ok(),
        "batch recovery fixture cleanup exceeded 5s cooperative deadline"
    );
    match scenario {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!(
            "batch original UUID recovery scenario exceeded 30s cooperative fixture deadline"
        ),
    }
}

#[tokio::test]
#[ignore = "owned PG fixture; scripts/check-query-history-batch.sh"]
async fn real_router_batch_v1_v2_owner_migration_binding_and_store_fault_compatibility() {
    let f = fixture().await;
    let db_options = options(&f.socket, "fixture_admin").database("qh_batch_v1");
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect_with(db_options)
        .await
        .unwrap();
    let writer = siglake_query_server::history::HistoryWriter::connect(
        options(&f.socket, "qh_writer").database("qh_batch_v1"),
    )
    .await
    .unwrap();
    let mut service = HistoryService::new(writer);
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let uri = jobs_uri(&f.socket).replace("qh_fixture", "qh_batch_v1");
    let jobs = JobStore::new_postgres(
        &uri,
        1,
        Duration::from_secs(600),
        JobRecoveryPolicy::default(),
    )
    .await
    .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_jobs(jobs)
        .with_history(service.observer());
    let s = server(state.clone()).await;
    let t = token(&issuer, "acme", "original-client", 0);
    let gate = QueueGate::hold(&state.jobs).await;
    let (execution, job) = submit(&s, &t, "SELECT 1 AS n", json!({})).await;
    let queued = queued_pending(&db, execution, job).await;
    // The owned task has not polled, so its missing binding alone is not a v1
    // refusal. Exercise the association against this actual router receipt with
    // its original valid job UUID, and identify the pending-evidence CHECK.
    let pending_constraint: String = sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint WHERE conrelid='query_history.executions'::regclass AND contype='c' AND convalidated AND pg_get_constraintdef(oid) LIKE '%observation_state%' AND pg_get_constraintdef(oid) LIKE '%job_id IS NULL%'",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    let refusal =
        sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
            .bind(execution)
            .bind(job.0)
            .execute(&db)
            .await
            .expect_err("v1 must refuse pending association with the original valid job UUID");
    let refusal = refusal
        .as_database_error()
        .expect("PostgreSQL CHECK refusal");
    assert_eq!(refusal.code().as_deref(), Some("23514"));
    assert_eq!(refusal.constraint(), Some(pending_constraint.as_str()));
    assert_eq!(queued_pending(&db, execution, job).await, queued);
    gate.release();
    let old = row(&db, execution).await;
    assert_eq!(
        old["outcome"], "success",
        "missing v2 never changes job execution"
    );
    persisted(&db, job, "succeeded").await;
    let gate = QueueGate::hold(&state.jobs).await;
    let (first, first_job) = submit(&s, &t, "SELECT 2 AS n", json!({})).await;
    let (second, second_job) = submit(&s, &t, "SELECT 3 AS n", json!({})).await;
    sqlx::raw_sql(include_str!("../../src/history/schema-v2-batch.sql"))
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        row(&db, execution).await,
        old,
        "no migration backfill or relabeling"
    );
    let mut migration = db.acquire().await.unwrap();
    assert!(
        sqlx::raw_sql(include_str!("../../src/history/schema-v2-batch.sql"))
            .execute(&mut *migration)
            .await
            .is_err(),
        "incompatible/repeated migration fails atomically"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *migration)
        .await
        .unwrap();
    drop(migration);

    sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
        .bind(first)
        .bind(first_job.0)
        .execute(&db)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
            .bind(second)
            .bind(first_job.0)
            .execute(&db)
            .await
            .is_err(),
        "one original parent per job"
    );
    sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
        .bind(second)
        .bind(second_job.0)
        .execute(&db)
        .await
        .unwrap();
    let original = pending(&db, first, first_job).await;
    pending(&db, second, second_job).await;
    sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
        .bind(first)
        .bind(first_job.0)
        .execute(&db)
        .await
        .unwrap();
    for target in [Some(second_job.0), None] {
        assert!(
            sqlx::query("UPDATE query_history.executions SET job_id=$2 WHERE execution_id=$1")
                .bind(first)
                .bind(target)
                .execute(&db)
                .await
                .is_err(),
            "association cannot rebind or clear"
        );
    }
    assert!(
        sqlx::query("UPDATE query_history.executions SET duration_ms=1 WHERE execution_id=$1")
            .bind(first)
            .execute(&db)
            .await
            .is_err(),
        "all other nonterminal evidence invariants remain"
    );
    assert!(
        sqlx::query("UPDATE query_history.executions SET tenant='foreign' WHERE execution_id=$1")
            .bind(first)
            .execute(&db)
            .await
            .is_err(),
        "association never changes tenant authority"
    );
    assert_eq!(pending(&db, first, first_job).await, original);
    gate.release();
    let first = row(&db, first).await;
    assert_eq!(first["outcome"], "success");
    let second = row(&db, second).await;
    assert_eq!(second["outcome"], "success");

    // Real job-store privilege refusal, with and without history. This is not
    // a transport outage or commit-acknowledgement fault; HTTP policy is equal.
    let mut plain = state.clone();
    plain.history = None;
    let unobserved = server(plain).await;
    sqlx::query("REVOKE INSERT ON siglake_query_jobs FROM qh_jobs")
        .execute(&db)
        .await
        .unwrap();
    let a = post(
        &s,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 4","priority":"batch"}),
    )
    .await;
    let b = post(
        &unobserved,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 4","priority":"batch"}),
    )
    .await;
    assert_eq!(a.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(a.status(), b.status());
    assert!(!b.headers().contains_key(CORRELATION));
    let failure = row(&db, correlation(&a)).await;
    assert_eq!(failure["execution_admitted"], false);
    assert!(failure["job_id"].is_null());
    sqlx::query("GRANT INSERT ON siglake_query_jobs TO qh_jobs")
        .execute(&db)
        .await
        .unwrap();
    let before = count(&db).await;
    // Disable ONLY this fixture's history login and terminate its fixture DB
    // sessions, never a real target. No receipt means no UUID/header promised.
    sqlx::query("ALTER ROLE qh_writer NOLOGIN")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='qh_writer' AND datname='qh_batch_v1'").execute(&db).await.unwrap();
    let response = post(
        &s,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 5","priority":"batch"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(!response.headers().contains_key(CORRELATION));
    let body = response.json::<Value>().await.unwrap();
    let job = JobId::parse(body["job_id"].as_str().unwrap()).unwrap();
    persisted(&db, job, "succeeded").await;
    assert_eq!(count(&db).await, before);
    sqlx::query("ALTER ROLE qh_writer LOGIN")
        .execute(&db)
        .await
        .unwrap();
    state.jobs.shutdown().await.unwrap();
    let _ = service.shutdown().await;
    issuer_task.abort();
}

#[tokio::test]
#[ignore = "owned PG fixture; scripts/check-query-history-batch.sh"]
async fn real_router_batch_dropped_submit_during_association_owned_execution_and_delete() {
    use axum::{body::Body, http::Request};
    use siglake_query_server::admission::AdmissionController;
    use tower::ServiceExt;

    let f = fixture().await;
    let mut resources = RecoveryScenarioResources::default();
    let scenario = async {
        let mut service = HistoryService::new(f.writer);
        let tmp = tempfile::tempdir().unwrap();
        let ice = Arc::new(
            IcebergContext::open(&tmp.path().join("warehouse"))
                .await
                .unwrap(),
        );
        ice.for_namespace("tenant_acme")
            .await
            .unwrap()
            .append_events(&[siglake_core::Event::now("PRIVATE_BATCH_RESULT")])
            .await
            .unwrap();
        let (issuer, issuer_task) = spawn_fake_idp().await;
        resources.issuer = Some(issuer_task);
        let verifier =
            OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
                .await
                .unwrap();
        let mut state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
            .with_tenants(TenantRegistry::new(ice))
            .with_jobs(jobs(&f.socket).await)
            .with_history(service.observer());
        // Three fixture-owned shares leave exactly the batch reservation free.
        state.admission = Arc::new(AdmissionController::new(4, 1, Duration::from_secs(2)));
        resources.stores.push(state.jobs.clone());
        let s = server(state.clone()).await;
        let original = token(&issuer, "acme", "original-client", 0);
        let canceller = token(&issuer, "acme", "different-client", 0);
        resources.association_gate_installed = true;
        sqlx::raw_sql("CREATE FUNCTION public.batch_history_association_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.job_id IS NULL AND NEW.job_id IS NOT NULL THEN PERFORM pg_advisory_xact_lock(682306); END IF; RETURN NEW; END $$; REVOKE ALL ON FUNCTION public.batch_history_association_gate() FROM PUBLIC; CREATE TRIGGER batch_history_association_gate BEFORE UPDATE ON query_history.executions FOR EACH ROW EXECUTE FUNCTION public.batch_history_association_gate();")
            .execute(&f.admin).await.unwrap();

        for (cancel, outage) in [(false, false), (true, false), (false, true)] {
            let shares = state.admission.acquire(3).await.unwrap();
            let before = count(&f.admin).await;
            let mut lock = f.admin.acquire().await.unwrap();
            lock.close_on_drop();
            sqlx::query("SELECT pg_advisory_lock(682306)")
                .execute(&mut *lock)
                .await
                .unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/api/v1/sql/local")
                .header("authorization", format!("Bearer {original}"))
                .header("content-type", "application/json")
                .header(CORRELATION, Uuid::nil().to_string())
                .body(Body::from(
                    json!({"query":"SELECT raw FROM events", "priority":"batch"}).to_string(),
                ))
                .unwrap();
            // Poll the actual router/submit future, not a lifecycle helper. On the
            // broken source it is still inside association; on fixed source the
            // 202 may already exist, but execution must remain independently owned.
            let mut request = Box::pin(router(state.clone()).oneshot(request));
            let gate = wait_query(
                &f.admin,
                "%UPDATE query_history.executions SET job_id%",
                "advisory",
            );
            tokio::pin!(gate);
            let response = tokio::select! {
                response = &mut request => {
                    let response = response.unwrap();
                    assert_eq!(response.status(), StatusCode::ACCEPTED);
                    gate.await;
                    Some(response)
                }
                () = &mut gate => None,
            };
            let (job_text, execution): (String, Uuid) = sqlx::query_as("SELECT j.job_id, j.history_execution_id FROM siglake_query_jobs j JOIN query_history.executions e ON e.execution_id=j.history_execution_id WHERE j.status='pending' AND e.job_id IS NULL")
                .fetch_one(&f.admin).await.unwrap();
            let job = JobId::parse(&job_text).unwrap();
            let initial: Value = sqlx::query_scalar(
                "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
            )
            .bind(execution)
            .fetch_one(&f.admin)
            .await
            .unwrap();
            assert_eq!(initial["observation_state"], "pending");
            assert!(initial["execution_admitted"].is_null());
            assert!(initial["job_id"].is_null());
            assert_eq!(initial["client_id"], "original-client");
            if let Some(response) = &response {
                assert_eq!(response.headers()[CORRELATION], execution.to_string());
            }
            drop(request);
            drop(response);
            let mut probe = Box::pin(state.admission.acquire(1));
            assert!(
                futures::poll!(&mut probe).is_pending(),
                "owned batch must retain admission after HTTP future/response drop"
            );
            drop(probe);

            let started = Instant::now();
            if cancel {
                assert_eq!(
                    job_response(&s, &canceller, job, true).await.status(),
                    StatusCode::ACCEPTED
                );
                persisted(&f.admin, job, "cancelled").await;
                assert!(
                    started.elapsed() < Duration::from_secs(2),
                    "DELETE is independent of history association lock"
                );
            }
            if outage {
                // Keep association locked THROUGH its bounded timeout. Engine and
                // authoritative job completion remain available during the outage.
                persisted(&f.admin, job, "succeeded").await;
                assert!(started.elapsed() < Duration::from_secs(2));
                assert!(
                    service
                        .health()
                        .loss_signals(siglake_query_server::history::LossReason::Timeout)
                        >= 1
                );
            }
            sqlx::query("SELECT pg_advisory_unlock(682306)")
                .execute(&mut *lock)
                .await
                .unwrap();
            let expected = if cancel { "cancelled" } else { "succeeded" };
            persisted(&f.admin, job, expected).await;
            eprintln!("owned association terminal wait: cancel={cancel}, outage={outage}");
            let terminal = row(&f.admin, execution).await;
            assert_eq!(
                terminal["outcome"],
                if cancel { "cancelled" } else { "success" }
            );
            assert_eq!(terminal["execution_admitted"], !cancel);
            assert_eq!(terminal["job_id"], job.to_string());
            assert_eq!(terminal["received_at"], initial["received_at"]);
            assert_eq!(terminal["expires_at"], initial["expires_at"]);
            assert_eq!(terminal["client_id"], "original-client");
            assert_eq!(
                count(&f.admin).await,
                before + 1,
                "no replacement receipt/backfill"
            );
            if cancel {
                let info = state.jobs.info(job).await.unwrap().unwrap();
                assert!(info.started_at.is_none() && info.cost.is_none());
                assert_eq!(
                    job_response(&s, &canceller, job, true).await.status(),
                    StatusCode::CONFLICT
                );
            } else {
                assert_eq!(terminal["returned_rows"], 1);
            }
            // The owned run must release its share after terminal observation too.
            let free = tokio::time::timeout(Duration::from_secs(2), state.admission.acquire(1))
                .await
                .unwrap()
                .unwrap();
            drop(free);
            drop(shares);
        }
        state.jobs.shutdown().await.unwrap();
        let _ = service.shutdown().await;
    };
    let scenario = tokio::time::timeout(
        Duration::from_secs(30),
        std::panic::AssertUnwindSafe(scenario).catch_unwind(),
    )
    .await;
    let cleanup = tokio::time::timeout(Duration::from_secs(5), resources.shutdown(&f.admin)).await;
    assert!(
        cleanup.is_ok(),
        "association fixture cleanup exceeded 5s cooperative deadline"
    );
    match scenario {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("batch association scenario exceeded 30s cooperative fixture deadline"),
    }
}
