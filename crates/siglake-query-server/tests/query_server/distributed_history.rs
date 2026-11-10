//! Production coordinator + worker routers, real synthetic engine warehouses,
//! signed local access issuer and owned PostgreSQL. No fake peer success JSON.
use super::*;
use axum::{body::Body, http::Request};
use futures::StreamExt;
use siglake_query_server::jobs::{JobId, JobStore};
use tower::ServiceExt;

const SERVICE_TOKEN: &str = "synthetic-owned-coordinator-credential";
const FANOUT: &str = "SELECT host, sum(length(raw)) AS n FROM events WHERE raw LIKE '%owned-distributed%' GROUP BY host";

async fn state(
    warehouse: &std::path::Path,
    issuer: &str,
    observer: &siglake_query_server::history::HistoryObserver,
) -> AppState {
    let ice = Arc::new(IcebergContext::open(warehouse).await.unwrap());
    let verifier =
        OidcVerifier::from_cognito_access_issuer(issuer.to_owned(), "custom:org_id".into())
            .await
            .unwrap();
    AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_history(observer.clone())
        .with_coordinator(vec![], Some(SERVICE_TOKEN.into()))
}
async fn cluster(coordinator: AppState, worker: AppState) -> (Server, Server, AppState) {
    let worker = server(worker).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let coordinator = coordinator.with_coordinator(
        vec![base.clone(), worker.base.clone()],
        Some(SERVICE_TOKEN.into()),
    );
    let app = router(coordinator.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (Server { base, task }, worker, coordinator)
}
async fn seed(ice: &IcebergContext) {
    for tenant in ["acme", "other"] {
        let tenant_ice = ice
            .for_namespace(&format!("tenant_{tenant}"))
            .await
            .unwrap();
        for file in 0..2 {
            let mut event = siglake_core::Event::now("owned-distributed PRIVATE_RESULT");
            event.host = format!("{tenant}-host-{file}");
            tenant_ice.append_events(&[event]).await.unwrap();
        }
    }
}
fn safe(row: &Value, tenant: &str, client: &str) {
    assert_eq!(row["tenant"], tenant);
    assert_eq!(row["client_id"], client);
    assert_eq!(row["principal_kind"], "provider");
    assert!(row["redacted_sql"].is_null());
    assert!(row["admitted_at"].is_null());
    assert!(row["finished_at"].is_null());
}
fn executed(row: &Value, format: &str) {
    assert_eq!(row["outcome"], "success");
    assert_eq!(row["execution_admitted"], true);
    assert_eq!(row["effective_format"], format);
    assert_eq!(row["effective_dry_run"], false);
}

#[tokio::test]
#[ignore = "owned PG; scripts/check-query-history-batch.sh"]
async fn real_coordinator_worker_parent_identity_formats_and_effective_variants() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let observer = service.observer();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let coordinator = state(&warehouse, &issuer, &observer).await;
    seed(&coordinator.ice).await;
    let worker = state(&warehouse, &issuer, &observer).await;
    let (s, worker, configured) = cluster(coordinator, worker).await;
    let t = token(&issuer, "acme", "client-one", 0);
    let renewed = token(&issuer, "acme", "client-one", 60);
    for (credential, tenant, client) in [
        (&t, "acme", "client-one"),
        (&renewed, "acme", "client-one"),
        (
            &token(&issuer, "acme", "client-two", 0),
            "acme",
            "client-two",
        ),
        (
            &token(&issuer, "other", "client-two", 0),
            "other",
            "client-two",
        ),
    ] {
        for route in ["/api/v1/sql", "/api/v1/sql/distributed"] {
            let row = observed(&s, credential, &f.admin, route,
                json!({"query":FANOUT,"tenant":"other","parent_execution_id":Uuid::nil(),"coordinator":true}), StatusCode::OK).await;
            safe(&row, tenant, client);
            executed(&row, "records");
            assert_eq!(row["returned_rows"], 2);
            assert!(row["measured_rows_scanned"].is_null());
            assert!(row["measured_bytes_scanned"].is_null());
            assert!(row["job_id"].is_null());
        }
    }
    // Existing API discrepancy: explicit distributable dry_run executes; the
    // immutable hint is NOT a guarantee. Effective v3 fields tell the truth.
    let dry = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":FANOUT,"dry_run":true}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(dry["dry_run"], true);
    executed(&dry, "records");
    let planning = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1","dry_run":true,"format":"ndjson"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(planning["effective_dry_run"], true);
    assert_eq!(planning["effective_format"], "records");
    assert!(planning["returned_rows"].is_null());
    // Batch here is a tier, not a job. NDJSON is actually returned.
    let before = count(&f.admin).await;
    let response = post(
        &s,
        &t,
        "/api/v1/sql/distributed",
        json!({"query":FANOUT,"priority":"batch","format":"ndjson"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/x-ndjson");
    let id = correlation(&response);
    let _ = response.bytes().await.unwrap();
    let batch_tier = row(&f.admin, id).await;
    executed(&batch_tier, "ndjson");
    assert_eq!(batch_tier["priority"], "batch");
    assert!(batch_tier["job_id"].is_null());
    assert_eq!(count(&f.admin).await, before + 1);
    // Direct externally authenticated shards ignore unsigned forwarded tenant,
    // parent/coordinator indicators and use the existing signed claim.
    for (credential, tenant, priority) in [
        (&t, "acme", "interactive"),
        (&renewed, "acme", "batch"),
        (
            &token(&issuer, "other", "client-one", 0),
            "other",
            "interactive",
        ),
    ] {
        let before = count(&f.admin).await;
        let response = reqwest::Client::new().post(format!("{}/api/v1/sql/shard", worker.base))
            .bearer_auth(credential).header("x-siglake-coordinator", "true")
            .header("x-siglake-parent-id", Uuid::nil().to_string())
            .header("x-scope-orgid", "forged")
            .json(&json!({"query":"SELECT host FROM events","priority":priority,"tenant":"forged","coordinator":true,"parent_execution_id":Uuid::nil()}))
            .send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/vnd.apache.arrow.stream"
        );
        let id = correlation(&response);
        let bytes = response.bytes().await.unwrap();
        let batches = siglake_query_server::format::arrow_ipc_to_batches(&bytes).unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        for batch in &batches {
            let hosts = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for host in hosts.iter().flatten() {
                assert!(host.starts_with(tenant));
            }
        }
        let direct = row(&f.admin, id).await;
        safe(&direct, tenant, "client-one");
        executed(&direct, "arrow");
        assert_eq!(direct["operation"], "shard");
        assert_eq!(direct["format"], "arrow");
        assert_eq!(count(&f.admin).await, before + 1);
    }
    // Only an authenticated service credential marks an internal hop; it has
    // no named provider principal and creates no second public execution.
    unobserved(
        &worker,
        SERVICE_TOKEN,
        &f.admin,
        "/api/v1/sql/shard",
        json!({"query":"SELECT 1","tenant":"acme"}),
        StatusCode::OK,
    )
    .await;
    let early = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT missing FROM events"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(early["execution_admitted"], false);
    assert!(early["effective_format"].is_null());
    assert!(early["effective_dry_run"].is_null());
    // Actual typed metadata result and real configured local cache fallback.
    executed(
        &observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":"SELECT count(*) FROM events"}),
            StatusCode::OK,
        )
        .await,
        "records",
    );
    let mut cache = Value::Null;
    for _ in 0..2 {
        cache = observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":"SELECT raw FROM events LIMIT 1"}),
            StatusCode::OK,
        )
        .await;
        executed(&cache, "records");
    }
    assert_eq!(cache["cache_state"], "hit");
    let directory = Arc::new(siglake_query_server::discovery::PeerDirectory::new(
        "owned-no-membership",
    ));
    let discovery = server(configured.clone().with_peer_source(
        Some(siglake_query_server::discovery::PeerSource::from_directory(
            directory,
        )),
        Some(SERVICE_TOKEN.into()),
    ))
    .await;
    executed(
        &observed(
            &discovery,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":"SELECT 3"}),
            StatusCode::OK,
        )
        .await,
        "records",
    );
    let missing = observed(
        &discovery,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":FANOUT,"format":"ndjson","priority":"batch","dry_run":true}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(missing["execution_admitted"], false);
    assert!(missing["effective_format"].is_null());
    executed(
        &observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":"SELECT raw FROM events","shard":{"index":0,"count":2}}),
            StatusCode::OK,
        )
        .await,
        "records",
    );
    unobserved(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1","format":"arrow"}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    let planning = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1","priority":"batch","dry_run":true,"format":"ndjson"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(planning["execution_admitted"], true);
    assert_eq!(planning["effective_format"], "records");
    assert_eq!(planning["effective_dry_run"], true);
    assert!(planning["returned_rows"].is_null());
    assert!(planning["job_id"].is_null());
    let mine = f.reader.list("acme", 100).await.unwrap();
    assert!(!mine.is_empty());
    for item in mine {
        assert_eq!(serde_json::to_value(item).unwrap()["tenant"], "acme");
    }
    // Buffered coordinator NDJSON owns its already-rendered payload until EOF.
    // Body-drop evidence is in-process abandonment, NOT listening TCP proof.
    for poll_payload in [false, true] {
        let before = count(&f.admin).await;
        let response = router(configured.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sql/distributed")
                    .header("authorization", format!("Bearer {t}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"query":FANOUT,"format":"ndjson"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let id = Uuid::parse_str(response.headers()[CORRELATION].to_str().unwrap()).unwrap();
        let pending: Value = sqlx::query_scalar(
            "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
        )
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert!(pending["outcome"].is_null());
        let mut body = response.into_body().into_data_stream();
        if poll_payload {
            assert!(body.next().await.is_some());
        }
        drop(body);
        assert_eq!(row(&f.admin, id).await["outcome"], "disconnected");
        assert_eq!(count(&f.admin).await, before + 1);
    }
    let before = count(&f.admin).await;
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let began = Instant::now();
    let response = post(&s, &t, "/api/v1/sql/distributed", json!({"query":FANOUT})).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        began.elapsed() < Duration::from_secs(2),
        "history outage cannot block real work indefinitely"
    );
    let original = correlation(&response);
    let _ = response.bytes().await.unwrap();
    lock.rollback().await.unwrap();
    let recovered = row(&f.admin, original).await;
    executed(&recovered, "records");
    safe(&recovered, "acme", "client-one");
    assert_eq!(count(&f.admin).await, before + 1);
    assert!(service.shutdown().await);
    issuer_task.abort();
}

#[tokio::test]
#[ignore = "owned PG; scripts/check-query-history-batch.sh"]
async fn effective_v3_manual_migration_legacy_null_coherent_immutable_and_reader_compatibility() {
    use siglake_query_server::history::{HistoryReader, HistoryWriter};
    use sqlx::postgres::PgPoolOptions;
    const MIGRATION: &str = include_str!("../../src/history/schema-v3-effective.sql");
    let f = fixture().await;
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let t = token(&issuer, "acme", "client-one", 0);
    for version in ["v1", "v2"] {
        let database = format!("qh_effective_{version}");
        let db = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(options(&f.socket, "fixture_admin").database(&database))
            .await
            .unwrap();
        let writer = HistoryWriter::connect(options(&f.socket, "qh_writer").database(&database))
            .await
            .unwrap();
        let reader = HistoryReader::connect(options(&f.socket, "qh_reader").database(&database))
            .await
            .unwrap();
        let mut service = HistoryService::new(writer);
        let state = state(&warehouse, &issuer, &service.observer()).await;
        let local = server(state.clone()).await;
        let old = observed(
            &local,
            &t,
            &db,
            "/api/v1/sql/local",
            json!({"query":"SELECT 1"}),
            StatusCode::OK,
        )
        .await;
        let original = Uuid::parse_str(old["execution_id"].as_str().unwrap()).unwrap();
        assert!(
            serde_json::to_value(reader.detail("acme", original).await.unwrap().unwrap()).unwrap()
                ["effective_format"]
                .is_null()
        );
        let missing_v3 = server(
            state
                .clone()
                .with_coordinator(vec!["http://127.0.0.1:1".into()], None),
        )
        .await;
        let response = post(&missing_v3, &t, "/api/v1/sql", json!({"query":"SELECT 9"})).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "missing metadata migration cannot change SQL availability"
        );
        let pending_id = correlation(&response);
        let _ = response.bytes().await.unwrap();
        let pending: bool = sqlx::query_scalar(
            "SELECT outcome IS NULL FROM query_history.executions WHERE execution_id=$1",
        )
        .bind(pending_id)
        .fetch_one(&db)
        .await
        .unwrap();
        assert!(
            pending,
            "missing-v3 cannot certify effective terminal metadata"
        );
        let constraints: String = sqlx::query_scalar("SELECT string_agg(pg_get_constraintdef(oid),';' ORDER BY conname) FROM pg_constraint WHERE conrelid='query_history.executions'::regclass").fetch_one(&db).await.unwrap();
        sqlx::raw_sql(MIGRATION).execute(&db).await.unwrap();
        // Manual schema changes require reader/service recreation: PostgreSQL
        // cached SELECT * plans cannot change result shape across ADD COLUMN.
        drop(reader);
        let reader = HistoryReader::connect(options(&f.socket, "qh_reader").database(&database))
            .await
            .unwrap();
        let preserved: String = sqlx::query_scalar("SELECT string_agg(pg_get_constraintdef(oid),';' ORDER BY conname) FROM pg_constraint WHERE conrelid='query_history.executions'::regclass AND conname<>'executions_effective_v3'").fetch_one(&db).await.unwrap();
        assert_eq!(
            constraints, preserved,
            "v1/v2 constraints preserved exactly"
        );
        let legacy = row(&db, original).await;
        for name in [
            "execution_id",
            "tenant",
            "issuer",
            "client_id",
            "received_at",
            "expires_at",
            "format",
            "dry_run",
            "outcome",
        ] {
            assert_eq!(old[name], legacy[name]);
        }
        assert!(legacy["effective_format"].is_null());
        assert!(legacy["effective_dry_run"].is_null());
        executed(&row(&db, pending_id).await, "records");
        // This SQL connection is fixture administrator ONLY. It demonstrates
        // owner migration compatibility without a runtime DDL path.
        let mut connection = db.acquire().await.unwrap();
        assert!(sqlx::raw_sql(MIGRATION)
            .execute(&mut *connection)
            .await
            .is_err());
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        // Schema-only primitive observation (not an executed SQL request):
        // prove the validated pair check independently of immutability.
        let probe = service
            .begin(
                siglake_query_server::history::Principal::from_caller(
                    "acme",
                    &siglake_query_server::CallerIdentity::default(),
                )
                .unwrap(),
                siglake_query_server::history::RequestMetadata {
                    operation: siglake_query_server::history::Operation::Sql,
                    format: siglake_query_server::history::OutputFormat::Records,
                    priority: siglake_query_server::history::Priority::Interactive,
                    dry_run: false,
                },
                siglake_query_server::history::redaction::RedactedSql::parse(""),
            )
            .await;
        let probe_id = probe.receipt().unwrap().execution_id();
        for sql in [
            "UPDATE query_history.executions SET effective_format='records' WHERE execution_id=$1",
            "UPDATE query_history.executions SET effective_dry_run=false WHERE execution_id=$1",
            "UPDATE query_history.executions SET effective_format='ndjson',effective_dry_run=true WHERE execution_id=$1",
            "UPDATE query_history.executions SET effective_format='arrow',effective_dry_run=false WHERE execution_id=$1",
        ] { assert!(sqlx::query(sql).bind(probe_id).execute(&db).await.is_err()); }
        drop(probe);
        let configured = state.with_coordinator(vec!["http://127.0.0.1:1".into()], None);
        let response = router(configured)
            .oneshot(
                Request::post("/api/v1/sql")
                    .header("authorization", format!("Bearer {t}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"query":"SELECT 2","format":"ndjson"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let id = Uuid::parse_str(response.headers()[CORRELATION].to_str().unwrap()).unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let selected: bool = sqlx::query_scalar("SELECT effective_format='ndjson' AND effective_dry_run IS FALSE FROM query_history.executions WHERE execution_id=$1").bind(id).fetch_one(&db).await.unwrap_or(false);
                if selected { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(sqlx::query("UPDATE query_history.executions SET effective_format='ndjson',effective_dry_run=false WHERE execution_id=$1").bind(id).execute(&db).await.is_ok());
        for sql in [
            "UPDATE query_history.executions SET effective_dry_run=true WHERE execution_id=$1",
            "UPDATE query_history.executions SET effective_format='records' WHERE execution_id=$1",
            "UPDATE query_history.executions SET effective_format=NULL,effective_dry_run=NULL WHERE execution_id=$1",
            "UPDATE query_history.executions SET tenant='other' WHERE execution_id=$1",
        ] { assert!(sqlx::query(sql).bind(id).execute(&db).await.is_err()); }
        drop(response);
        assert_eq!(row(&db, id).await["outcome"], "disconnected");
        let projected =
            serde_json::to_value(reader.detail("acme", id).await.unwrap().unwrap()).unwrap();
        assert_eq!(projected["effective_format"], "ndjson");
        assert_eq!(projected["effective_dry_run"], false);
        assert!(reader.detail("other", id).await.unwrap().is_none());
        assert!(service.shutdown().await);
        db.close().await;
    }
    issuer_task.abort();
}

#[tokio::test]
#[ignore = "owned PG; scripts/check-query-history-batch.sh"]
async fn configured_peer_failure_fallback_refusal_and_delegated_jobs() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let observer = service.observer();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let mut coordinator = state(&warehouse, &issuer, &observer).await;
    seed(&coordinator.ice).await;
    coordinator.jobs = Arc::new(JobStore::new(1, Duration::from_secs(600)));
    let worker_state = state(&warehouse, &issuer, &observer).await;
    let (s, worker, configured) = cluster(coordinator, worker_state).await;
    let t = token(&issuer, "acme", "client-one", 0);
    // A real successful owned peer, then its real listener is shut down.
    executed(
        &observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":FANOUT}),
            StatusCode::OK,
        )
        .await,
        "records",
    );
    worker.task.abort();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !worker.task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Connection refusal is an intentionally induced transport fault, not a
    // claim of a real network outage. The existing self-shard fallback runs.
    executed(
        &observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":FANOUT}),
            StatusCode::OK,
        )
        .await,
        "records",
    );
    // Real production worker policy refusal: accepted parent failed, no retry
    // of its deliberate 403, no internal public row. Unlike a sampled scan
    // breaker over a tiny fixture, this boundary is deterministic.
    let refusing = state(&warehouse, &issuer, &observer)
        .await
        .with_allowed_tenants(["other"]);
    let (refused, _worker, _) = cluster(configured.clone(), refusing).await;
    let failure = observed(
        &refused,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":FANOUT}),
        StatusCode::FORBIDDEN,
    )
    .await;
    assert_eq!(failure["execution_admitted"], true);
    assert_eq!(failure["outcome"], "failed");
    let failed = observed(&s, &t, &f.admin, "/api/v1/sql/distributed", json!({"query":"SELECT sum(CAST(raw AS BIGINT)) FROM events WHERE raw LIKE '%owned-distributed%'"}), StatusCode::INTERNAL_SERVER_ERROR).await;
    assert_eq!(failed["execution_admitted"], true);
    assert_eq!(failed["outcome"], "failed");
    let direct_response = post(
        &s,
        &t,
        "/api/v1/sql/shard",
        json!({"query":"SELECT missing FROM events","tenant":"other"}),
    )
    .await;
    assert_eq!(direct_response.status(), StatusCode::BAD_REQUEST);
    let direct_rejected = row(&f.admin, correlation(&direct_response)).await;
    assert_eq!(direct_rejected["execution_admitted"], false);
    assert_eq!(direct_rejected["tenant"], "acme");
    let timed = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":FANOUT,"limits":{"timeout_seconds":0}}),
        StatusCode::GATEWAY_TIMEOUT,
    )
    .await;
    assert_eq!(timed["execution_admitted"], false);
    assert_eq!(timed["outcome"], "timed_out");
    // Existing non-distributable explicit batch delegates to a real job.
    // Memory store is the baseline; durable association/recovery is separately
    // covered by the retained PostgreSQL batch suite.
    for route in ["/api/v1/sql", "/api/v1/sql/distributed"] {
        let before = count(&f.admin).await;
        let response = post(
            &s,
            &t,
            route,
            json!({"query":"SELECT 1 AS n","priority":"batch","format":"ndjson"}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let id = correlation(&response);
        let response = response.json::<Value>().await.unwrap();
        let job = JobId::parse(response["job_id"].as_str().unwrap()).unwrap();
        let done = row(&f.admin, id).await;
        executed(&done, "records");
        assert_eq!(done["job_id"], job.to_string());
        assert_eq!(count(&f.admin).await, before + 1);
    }
    let gate = batch_history::QueueGate::hold(&configured.jobs).await;
    let before = count(&f.admin).await;
    let response = post(
        &s,
        &t,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 4","priority":"batch","format":"ndjson"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = correlation(&response);
    let response = response.json::<Value>().await.unwrap();
    let job = response["job_id"].as_str().unwrap();
    let pending: Value = sqlx::query_scalar(
        "SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1",
    )
    .bind(id)
    .fetch_one(&f.admin)
    .await
    .unwrap();
    assert!(pending["outcome"].is_null());
    assert!(pending["execution_admitted"].is_null());
    let cancel = reqwest::Client::new()
        .delete(format!("{}/api/v1/jobs/{job}", s.base))
        .bearer_auth(&t)
        .send()
        .await
        .unwrap();
    assert!(cancel.status().is_success());
    gate.release();
    let cancelled = row(&f.admin, id).await;
    assert_eq!(cancelled["outcome"], "cancelled");
    assert_eq!(cancelled["execution_admitted"], false);
    assert_eq!(cancelled["effective_format"], "records");
    assert_eq!(count(&f.admin).await, before + 1);
    assert!(service.shutdown().await);
    issuer_task.abort();
}
