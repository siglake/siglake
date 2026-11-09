//! Signed actual selected-path matrix. Local PG and two loopback routers only.
//! Receipt/effective fields below are observations, never routing/clock inputs.
//! Existing receipts normalize format to records for batch/dry-run; they are NOT
//! a lossless raw-format preference. HTTP inputs are asserted separately.
//! Normal-service history checks do not promise terminal durability in outages;
//! the independent accepted owner/outage oracles remain unmodified.
use super::buffered_controls::fanout_controls::{engine, limits, Mode, Trace, SQL};
use super::buffered_controls::{fixture, policy_config, post, serve, token, Server};
use super::spawn_fake_idp;
use serde_json::{json, Value};
use siglake_query_server::{
    history::{HistoryService, HistoryWriter},
    jobs::{JobId, JobStatus, JobStore},
};
use siglake_query_server::{query_controls::*, AppState, AuthConfig, OidcVerifier, TenantRegistry};
use siglake_storage::iceberg::IcebergContext;
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode},
    ConnectOptions, PgPool,
};
use std::time::Instant;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use uuid::Uuid;
const ROUTE: &str = "/api/v1/sql/distributed";
const CORRELATION: &str = "x-siglake-history-id";
async fn jobs_count(admin: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM siglake_query_jobs")
        .fetch_one(admin)
        .await
        .unwrap()
}
async fn history_row(admin: &PgPool, id: Uuid) -> Value {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let row: Option<Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1 AND observation_state='terminal'").bind(id).fetch_optional(admin).await.unwrap();
            if let Some(row) = row { return row; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("normal-service selected history observation")
}
async fn terminal(state: &AppState, id: JobId) -> JobStatus {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let info = state.jobs.info(id).await.unwrap().unwrap();
            if info.status.is_terminal() {
                return info.status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
fn receipt_id(response: &reqwest::Response) -> Uuid {
    Uuid::parse_str(response.headers()[CORRELATION].to_str().unwrap()).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed two-node PG fixture; check-selected-paths.sh"]
async fn signed_selected_paths_priority_format_dry_run_history_and_original_clock_matrix() {
    let (socket, admin) = fixture().await;
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let reader_b = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let (issuer, task) = spawn_fake_idp().await;
    let idp = Server {
        base: issuer.clone(),
        task,
        state: None,
    };
    let jwt = token(&issuer, "acme", "selected-client");
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "selected-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(0, SavedSettings::new(Some(1), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("wire-selected-row")])
        .await
        .unwrap();
    let ice_b = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let urls = vec![
        format!("http://{}", listener_a.local_addr().unwrap()),
        format!("http://{}", listener_b.local_addr().unwrap()),
    ];
    let auth = AuthConfig::from_oidc(
        OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
            .await
            .unwrap(),
    );
    let options = PgConnectOptions::new_without_pgpass()
        .host(socket.to_str().unwrap())
        .socket(&socket)
        .port(5432)
        .database("qh_fixture")
        .username("qh_writer")
        .password("")
        .ssl_mode(PgSslMode::Disable)
        .disable_statement_logging();
    let mut service = HistoryService::new(HistoryWriter::connect(options).await.unwrap());
    let observer = service.observer();
    let uri = format!(
        "postgres://fixture_admin:@{}:5432/qh_fixture?sslmode=disable",
        socket.to_str().unwrap().replace('/', "%2F")
    );
    let jobs = JobStore::new_postgres(&uri, 1, Duration::from_secs(60), Default::default())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../src/query_controls/jobs.sql"))
        .execute(&admin)
        .await
        .unwrap();
    let config = DistributedControlsConfig::new(Duration::from_millis(20)).unwrap();
    let mut operator = limits(3000);
    operator.admission_wait_timeout = Duration::from_secs(2);
    let state_a = AppState::new(ice.clone(), auth.clone())
        .with_tenants(TenantRegistry::new(ice))
        .with_limits(operator.clone())
        .with_history(observer.clone())
        .with_jobs(jobs)
        .with_coordinator(urls, Some("synthetic-coordinator".into()))
        .with_distributed_query_controls(reader.clone(), 81, config)
        .unwrap();
    let state_b = AppState::new(ice_b.clone(), auth)
        .with_tenants(TenantRegistry::new(ice_b))
        .with_limits(operator)
        .with_history(observer)
        .with_coordinator(vec![], Some("synthetic-coordinator".into()))
        .with_distributed_query_controls(reader_b.clone(), 82, config)
        .unwrap();
    let ta = Arc::new(Trace::default());
    let tb = Arc::new(Trace::default());
    let a = engine(listener_a, state_a.clone(), ta.clone()).await;
    let b = engine(listener_b, state_b.clone(), tb.clone()).await;
    for (shape, query, delegated) in [
        ("tier1", "SELECT count(*) AS n FROM events", false),
        ("fanout", SQL, false),
        ("nondistributable", "SELECT 1 AS n", true),
        ("scan_local", "SELECT raw FROM events LIMIT 1", true),
    ] {
        for priority in ["interactive", "batch"] {
            for format in ["records", "ndjson"] {
                for dry in [false, true] {
                    let before = jobs_count(&admin).await;
                    let receipts_before: i64 =
                        sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
                            .fetch_one(&admin)
                            .await
                            .unwrap();
                    let wire_before = tb.requests.lock().unwrap().len();
                    let response=post(&a.base,&jwt,ROUTE,json!({"query":query,"priority":priority,"format":format,"dry_run":dry,"default_order":false,"limits":{"timeout_seconds":2,"circuit_breakers":false}})).await;
                    let durable = delegated && priority == "batch" && !dry;
                    assert_eq!(
                        response.status(),
                        if durable { 202 } else { 200 },
                        "{shape}/{priority}/{format}/{dry}"
                    );
                    let id = receipt_id(&response);
                    let selected_format = if delegated && (dry || durable) {
                        "records"
                    } else {
                        format
                    };
                    let bytes = response.bytes().await.unwrap();
                    if durable {
                        let body: Value = serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(body["controls"]["accepted"]["effective_timeout_ms"], 1000);
                        let job = JobId::parse(body["job_id"].as_str().unwrap()).unwrap();
                        assert_eq!(terminal(&state_a, job).await, JobStatus::Succeeded);
                    } else if delegated && dry {
                        let body: Value = serde_json::from_slice(&bytes).unwrap();
                        assert!(body.get("estimated_bytes_scanned").is_some());
                        assert!(body.get("job_id").is_none());
                    } else if selected_format == "ndjson" {
                        let lines: Vec<Value> = bytes
                            .split(|b| *b == b'\n')
                            .filter(|b| !b.is_empty())
                            .map(|b| serde_json::from_slice(b).unwrap())
                            .collect();
                        assert!(!lines.is_empty());
                    } else {
                        let body: Value = serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(body["row_count"], 1);
                        assert!(body.get("job_id").is_none());
                        if shape == "tier1" {
                            assert_eq!(
                                body["stats"]["phases"]["distributed"]["mode"],
                                "tier1_local"
                            );
                        }
                    }
                    let row = history_row(&admin, id).await;
                    let receipts_after: i64 =
                        sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
                            .fetch_one(&admin)
                            .await
                            .unwrap();
                    assert_eq!(
                        receipts_after,
                        receipts_before + 1,
                        "exactly one original parent receipt across delegation/fanout"
                    );
                    assert_eq!(
                        row["format"],
                        if priority == "batch" || dry {
                            "records"
                        } else {
                            format
                        }
                    );
                    assert_eq!(row["dry_run"], dry);
                    assert_eq!(row["priority"], priority);
                    assert_eq!(row["effective_format"], selected_format);
                    assert_eq!(row["effective_dry_run"], delegated && dry);
                    assert_eq!(row["job_id"].is_string(), durable);
                    assert_eq!(row["tenant"], "acme");
                    assert_eq!(row["issuer"], issuer);
                    assert_eq!(row["client_id"], "selected-client");
                    let expiry:bool=sqlx::query_scalar("SELECT expires_at=received_at+INTERVAL '720 hours' FROM query_history.executions WHERE execution_id=$1").bind(id).fetch_one(&admin).await.unwrap();
                    assert!(expiry);
                    assert_eq!(
                        jobs_count(&admin).await,
                        before + i64::from(durable),
                        "synchronous batch must not create durable jobs"
                    );
                    let dispatches = tb.requests.lock().unwrap().len() - wire_before;
                    if shape == "fanout" || (shape == "tier1" && format == "ndjson") {
                        assert!(dispatches > 0, "actual fanout proof");
                    } else {
                        assert_eq!(dispatches, 0, "actual local selection proof");
                    }
                    eprintln!("selected {shape}/{priority}/{format}/dry={dry}: status={} selected={selected_format}/dry={} jobs_delta={} wire={dispatches}",if durable{202}else{200},delegated&&dry,i64::from(durable));
                }
            }
        }
    }
    // Warm selected local cache is still classified and snapshot-gated.
    for attempt in 0..2 {
        let response=post(&a.base,&jwt,ROUTE,json!({"query":"SELECT raw FROM events WHERE raw LIKE '%wire%' LIMIT 1","default_order":false})).await;
        assert_eq!(response.status(), 200);
        let id = receipt_id(&response);
        response.bytes().await.unwrap();
        let row = history_row(&admin, id).await;
        assert_eq!(row["effective_format"], "records");
        if attempt == 1 {
            assert_eq!(row["cache_state"], "hit");
        }
    }
    // Every explicit branch still needing classification refuses zero BEFORE
    // work, regardless of requested priority/dry-run/format or false breakers.
    let before = jobs_count(&admin).await;
    for query in [
        "SELECT count(*) FROM events",
        SQL,
        "SELECT 1",
        "SELECT raw FROM events LIMIT 1",
        "SELECT raw FROM events WHERE false",
    ] {
        for priority in ["interactive", "batch"] {
            for format in ["records", "ndjson"] {
                for dry in [false, true] {
                    let response=post(&a.base,&jwt,ROUTE,json!({"query":query,"priority":priority,"format":format,"dry_run":dry,"limits":{"timeout_seconds":0,"circuit_breakers":false}})).await;
                    assert_eq!(response.status(), 504);
                    assert!(!response.headers().contains_key(CORRELATION));
                }
            }
        }
    }
    assert_eq!(jobs_count(&admin).await, before);
    // Actual nondistributable AND scan_local durable selections precede request
    // expiry; a real INSERT wait exceeds the short run/request budget but not
    // the ingress-anchored operator handoff. Queue/run starts at final staging.
    for query in ["SELECT 1 AS n", "SELECT raw FROM events LIMIT 1"] {
        let mut lock = admin.begin().await.unwrap();
        sqlx::query("LOCK siglake_query_jobs IN SHARE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let base = a.base.clone();
        let credential = jwt.clone();
        let before = chrono::Utc::now();
        let started = Instant::now();
        let request = tokio::spawn(async move {
            post(&base,&credential,ROUTE,json!({"query":query,"priority":"batch","default_order":false,"limits":{"timeout_seconds":1,"circuit_breakers":false}})).await
        });
        tokio::time::timeout(Duration::from_millis(800),async {
            loop {let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'INSERT INTO siglake_query_jobs%')").fetch_one(&admin).await.unwrap(); if waiting {break;} tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.expect("actual durable INSERT selected before request expiry");
        // No re-resolution at phase transfer: lock primary AFTER actual local
        // selection, keep it locked through 202, then allow the required dequeue read.
        let mut primary = admin.begin().await.unwrap();
        sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *primary)
            .await
            .unwrap();
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            started + Duration::from_millis(1200),
        ))
        .await;
        lock.rollback().await.unwrap();
        let response = request.await.unwrap();
        assert_eq!(response.status(), 202);
        primary.rollback().await.unwrap();
        eprintln!("selected durable {query}: observed INSERT before 800ms, 202 after {:?}, primary locked through response",started.elapsed());
        let accepted: Value = response.json().await.unwrap();
        let at = accepted["controls"]["accepted_at"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        assert!((at - before).num_milliseconds() >= 1100);
        assert_eq!(
            accepted["controls"]["accepted"]["effective_timeout_ms"],
            1000
        );
        assert_eq!(
            terminal(
                &state_a,
                JobId::parse(accepted["job_id"].as_str().unwrap()).unwrap()
            )
            .await,
            JobStatus::Succeeded
        );
    }
    // Primary waits before classification may not guess handoff, even when the
    // eventual branch WOULD be durable. Policy tightens original ingress.
    for query in [
        "SELECT 1",
        SQL,
        "SELECT count(*) FROM events",
        "SELECT raw FROM events LIMIT 1",
    ] {
        let before = jobs_count(&admin).await;
        let mut lock = admin.begin().await.unwrap();
        sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1100)).await;
            lock.rollback().await.unwrap();
        });
        let response=post(&a.base,&jwt,ROUTE,json!({"query":query,"priority":"batch","default_order":false,"limits":{"timeout_seconds":2,"circuit_breakers":false}})).await;
        assert_eq!(response.status(), 504);
        release.await.unwrap();
        assert_eq!(jobs_count(&admin).await, before);
    }
    // Admission snapshots cannot tighten/loosen in flight or come from history.
    // The worker trace is the actual signed-parent v2 dispatch, not a receipt.
    for (revision, next_timeout, expected_ms) in [(1, 2, 1000), (2, 1, 2000)] {
        *tb.mode.lock().unwrap() = Mode::Transit(300);
        let seen = tb.requests.lock().unwrap().len();
        let base = a.base.clone();
        let credential = jwt.clone();
        let request = tokio::spawn(async move {
            post(&base,&credential,ROUTE,json!({"query":SQL,"priority":"batch","dry_run":true,"limits":{"timeout_seconds":3,"circuit_breakers":false}})).await
        });
        tokio::time::timeout(Duration::from_millis(800), async {
            while tb.requests.lock().unwrap().len() == seen {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let admitted = tb.requests.lock().unwrap()[seen].clone();
        assert_eq!(admitted["controls"]["revision"], revision);
        assert_eq!(admitted["controls"]["timeout_ms"], expected_ms);
        manager
            .save(
                &binding,
                &actor,
                SaveRequest::new(
                    revision,
                    SavedSettings::new(Some(next_timeout), None).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let response = request.await.unwrap();
        assert_eq!(response.status(), 200);
        response.bytes().await.unwrap();
    }
    let before = jobs_count(&admin).await;
    // Coordinated batch remains REQUEST work even with dry-run/NDJSON hints.
    // Exercise policy AND caller minima with optional breakers disabled.
    for policy_seconds in [1, 2] {
        if policy_seconds == 2 {
            manager
                .save(
                    &binding,
                    &actor,
                    SaveRequest::new(3, SavedSettings::new(Some(2), None).unwrap()).unwrap(),
                )
                .await
                .unwrap();
        }
        for format in ["records", "ndjson"] {
            for dry in [false, true] {
                *tb.mode.lock().unwrap() = Mode::Transit(1100);
                let response=post(&a.base,&jwt,ROUTE,json!({"query":SQL,"priority":"batch","format":format,"dry_run":dry,"limits":{"timeout_seconds":if policy_seconds==1{2}else{1},"circuit_breakers":false}})).await;
                assert_eq!(response.status(), 504);
                assert_eq!(jobs_count(&admin).await, before);
            }
        }
    }
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(4, SavedSettings::new(Some(1), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    for status in [404, 503] {
        *tb.mode.lock().unwrap() = Mode::Refuse(status);
        ta.requests.lock().unwrap().clear();
        let response = post(
            &a.base,
            &jwt,
            ROUTE,
            json!({"query":SQL,"priority":"batch"}),
        )
        .await;
        assert!(!response.status().is_success());
        assert!(
            !ta.requests
                .lock()
                .unwrap()
                .iter()
                .any(|v| v["shard"]["index"] == 1),
            "incompatible/refusing worker became self-fallback success"
        );
        assert_eq!(jobs_count(&admin).await, before);
    }
    *tb.mode.lock().unwrap() = Mode::StallBody;
    let response=post(&a.base,&jwt,ROUTE,json!({"query":SQL,"priority":"batch","format":"ndjson","limits":{"circuit_breakers":false}})).await;
    assert_eq!(response.status(), 504);
    tokio::time::timeout(Duration::from_secs(1), async {
        while tb.stalled_sources.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    *tb.mode.lock().unwrap() = Mode::Normal;
    // Primary + history initial-persistence waits share REQUEST time until an
    // actual local durable branch is selected. History is still fail-open, not
    // an admission authority; the outer request timer, not history, refuses.
    let mut history_lock = admin.begin().await.unwrap();
    sqlx::query("LOCK query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *history_lock)
        .await
        .unwrap();
    let mut primary_lock = admin.begin().await.unwrap();
    sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *primary_lock)
        .await
        .unwrap();
    let base = a.base.clone();
    let credential = jwt.clone();
    let request = tokio::spawn(async move {
        post(&base,&credential,ROUTE,json!({"query":"SELECT 1","priority":"batch","limits":{"timeout_seconds":2,"circuit_breakers":false}})).await
    });
    tokio::time::sleep(Duration::from_millis(940)).await;
    primary_lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_millis(100),async {
        loop {let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%INSERT INTO query_history.executions%')").fetch_one(&admin).await.unwrap();if waiting {break;}tokio::time::sleep(Duration::from_millis(2)).await;}
    }).await.expect("actual history wait before request expiry");
    let response = request.await.unwrap();
    assert_eq!(response.status(), 504);
    history_lock.rollback().await.unwrap();
    assert_eq!(jobs_count(&admin).await, before);
    // Pin actual discovery membership BEFORE primary/history waits. Churn
    // cannot turn a synchronous coordinated request into guessed local handoff,
    // nor turn original missing membership into a late ready local/job route.
    for initially_configured in [true, false] {
        use siglake_query_server::discovery::{PeerDirectory, PeerSource};
        let directory = Arc::new(PeerDirectory::new("synthetic-self"));
        if initially_configured {
            directory.publish(vec![a.base.clone(), b.base.clone()], a.base.clone());
        }
        let dynamic = serve(
            state_a
                .clone()
                .with_peer_source(
                    Some(PeerSource::from_directory(directory.clone())),
                    Some("synthetic-coordinator".into()),
                )
                .with_distributed_query_controls(reader.clone(), 83, config)
                .unwrap(),
        )
        .await;
        let mut lock = admin.begin().await.unwrap();
        sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let base = dynamic.base.clone();
        let credential = jwt.clone();
        let request = tokio::spawn(async move {
            post(&base,&credential,ROUTE,json!({"query":if initially_configured{SQL}else{"SELECT 1"},"priority":"batch","limits":{"timeout_seconds":2,"circuit_breakers":false}})).await
        });
        // Policy leases verify role/schema before SELECT. Match the owned
        // reader's actual lock on THIS policy relation, not a potentially
        // truncated SQL substring in pg_stat_activity.
        tokio::time::timeout(Duration::from_millis(500),async {
            loop {let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.usename='qp_reader' AND a.wait_event_type='Lock' AND NOT l.granted AND l.relation='query_policy.agent_limits'::regclass)").fetch_one(&admin).await.unwrap();if waiting {break;}assert!(!request.is_finished(), "request finished before observed policy wait");tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.expect("owned policy reader blocked before classification");
        if initially_configured {
            directory.publish(
                vec!["http://127.0.0.1:9".into()],
                "http://127.0.0.1:9".into(),
            );
        } else {
            directory.publish(vec![a.base.clone(), b.base.clone()], a.base.clone());
        }
        lock.rollback().await.unwrap();
        let response = request.await.unwrap();
        assert_eq!(
            response.status(),
            if initially_configured { 200 } else { 400 }
        );
        response.bytes().await.unwrap();
        eprintln!("pinned membership initially_configured={initially_configured}: expected/proven status={}",if initially_configured{200}else{400});
        assert_eq!(jobs_count(&admin).await, before);
        dynamic.shutdown().await;
    }
    // A proven no-peer main/local durable route, unlike explicit classification,
    // may accept zero run time and expire without ever starting execution.
    let local = serve(
        state_a
            .clone()
            .with_coordinator(vec![], Some("synthetic-coordinator".into())),
    )
    .await;
    for path in ["/api/v1/sql", "/api/v1/sql/local"] {
        for seconds in [0, 1] {
            let response=post(&local.base,&jwt,path,json!({"query":"SELECT 1","priority":"batch","limits":{"timeout_seconds":seconds,"circuit_breakers":false}})).await;
            assert_eq!(response.status(), 202);
            let body: Value = response.json().await.unwrap();
            let id = JobId::parse(body["job_id"].as_str().unwrap()).unwrap();
            assert_eq!(
                terminal(&state_a, id).await,
                if seconds == 0 {
                    JobStatus::Timeout
                } else {
                    JobStatus::Succeeded
                }
            );
            if seconds == 0 {
                assert!(state_a
                    .jobs
                    .info(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .started_at
                    .is_none());
            }
        }
    }
    // Explicit no membership remains its existing refusal, never guessed local.
    let response = post(
        &local.base,
        &jwt,
        ROUTE,
        json!({"query":"SELECT 1","priority":"batch"}),
    )
    .await;
    assert_eq!(response.status(), 400);
    for state in [&state_a, &state_b] {
        tokio::time::timeout(Duration::from_secs(1), state.admission.acquire(1))
            .await
            .unwrap()
            .unwrap();
    }
    state_a.jobs.shutdown().await.unwrap();
    // These states share an authority: finish all traffic before closing it.
    for server in [local, a, b, idp] {
        server.shutdown().await;
    }
    let _ = service.shutdown().await;
    manager.close();
    reader.close();
    reader_b.close();
    admin.close().await;
}
