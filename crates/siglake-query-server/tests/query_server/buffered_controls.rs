//! Synthetic signed local JWT + owned primary PostgreSQL only. No provider,
//! real credentials, public controls metadata or runtime activation configuration.
use super::{access_claims, sign_claims, spawn_fake_idp, TEST_KID};
use axum::http::StatusCode;
use jsonwebtoken::Algorithm;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde_json::{json, Value};
use siglake_query_server::{
    query_controls::*, router, AppState, AuthConfig, OidcVerifier, ServerLimits, TenantRegistry,
};
use siglake_storage::iceberg::IcebergContext;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    ConnectOptions, PgPool,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;

struct Server {
    base: String,
    task: tokio::task::JoinHandle<()>,
    state: Option<AppState>,
}
impl Server {
    async fn shutdown(mut self) {
        if let Some(state) = self.state.take() {
            state.shutdown_query_controls().await;
        }
        self.task.abort();
        let _ = (&mut self.task).await;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(state: AppState) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let service_state = state.clone();
    let task =
        tokio::spawn(async move { axum::serve(listener, router(service_state)).await.unwrap() });
    Server {
        base,
        task,
        state: Some(state),
    }
}
fn token(issuer: &str, tenant: &str, client: &str) -> String {
    let mut claims = access_claims(issuer);
    claims["custom:org_id"] = json!(tenant);
    claims["client_id"] = json!(client);
    sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
}
async fn post(base: &str, token: &str, route: &str, body: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{base}{route}"))
        .bearer_auth(token)
        .header("x-client-id", "forged-client")
        .header("x-scope-orgid", "foreign")
        .header("x-siglake-query-controls", "v=1;timeout_ms=999999")
        .json(&body)
        .send()
        .await
        .unwrap()
}
async fn status(base: &str, token: &str, route: &str, body: Value, expected: StatusCode) -> Value {
    let response = post(base, token, route, body).await;
    assert_eq!(response.status(), expected);
    assert!(!response.headers().contains_key("x-siglake-query-controls"));
    assert!(!response.headers().contains_key("x-siglake-query-warnings"));
    let value: Value = response.json().await.unwrap();
    assert!(value.get("controls").is_none());
    value
}
fn cache_counts(s: &Snapshotter) -> std::collections::HashMap<String, u64> {
    let mut counts = std::collections::HashMap::new();
    for (key, _, _, value) in s.snapshot().into_vec() {
        if key.key().name() == "siglake_query_sql_result_cache_requests_total" {
            if let (Some(label), DebugValue::Counter(n)) =
                (key.key().labels().find(|l| l.key() == "outcome"), value)
            {
                counts.insert(label.value().to_string(), n);
            }
        }
    }
    counts
}
async fn wait_cache(s: &Snapshotter, outcome: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if cache_counts(s).get(outcome).copied().unwrap_or(0) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected bounded cache phase not reached");
}
fn policy_config(socket: &std::path::Path, reader: bool) -> PolicyConfig {
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let role = if reader { "qp_reader" } else { "qp_manager" };
    let uri = format!("postgres://{role}:@{encoded}:5432/qh_fixture?sslmode=disable");
    let bounds = ConnectionBounds {
        max_connections: 2,
        operation_timeout: Duration::from_secs(2),
    };
    if reader {
        PolicyConfig::reader(&uri, bounds)
    } else {
        PolicyConfig::manager(&uri, bounds)
    }
    .unwrap()
}
async fn fixture() -> (PathBuf, PgPool) {
    let socket = PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("use check-query-policy.sh"),
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
    assert_eq!(std::env::var_os("PGHOST").unwrap(), socket.as_os_str());
    assert!(std::env::var_os("DATABASE_URL").is_none());
    assert_eq!(
        std::fs::metadata(socket.join("home/.pgpass"))
            .unwrap()
            .len(),
        0
    );
    let options = PgConnectOptions::new_without_pgpass()
        .host(socket.to_str().unwrap())
        .socket(&socket)
        .port(5432)
        .database("qh_fixture")
        .username("fixture_admin")
        .password("")
        .ssl_mode(PgSslMode::Disable)
        .disable_statement_logging();
    let admin = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("TRUNCATE query_policy.agent_limits,query_policy.edits")
        .execute(&admin)
        .await
        .unwrap();
    (socket, admin)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned synthetic primary/JWT fixture; scripts/check-query-policy.sh"]
async fn real_signed_primary_buffered_cache_planning_delegation_and_coverage_guards() {
    let recorder = DebuggingRecorder::new();
    let metrics = recorder.snapshotter();
    metrics::set_global_recorder(recorder).unwrap();
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let issuer_owner = Server {
        base: issuer.clone(),
        task: issuer_task,
        state: None,
    };
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let token = token(&issuer, "acme", "controlled-client");
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "controlled-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(0, SavedSettings::new(Some(1), Some(1)).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("synthetic-row")])
        .await
        .unwrap();
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(2);
    limits.interactive.ceiling_timeout = Duration::from_secs(3);
    limits.batch.default_timeout = Duration::from_secs(2);
    limits.batch.ceiling_timeout = Duration::from_secs(3);
    limits.admission_wait_timeout = Duration::from_secs(2);
    let baseline = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_limits(limits);
    let enabled = baseline
        .clone()
        .with_query_controls(reader.clone(), 7)
        .unwrap();
    let s = serve(enabled.clone()).await;
    let query = json!({"query":"SELECT raw FROM events WHERE host = 'localhost' LIMIT 10", "default_order":false, "tenant":"foreign", "issuer":"forged", "client_id":"forged", "controls":{"timeout_seconds":999999}});
    cache_counts(&metrics);
    let first = status(
        &s.base,
        &token,
        "/api/v1/sql",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(first["row_count"], 1);
    assert_eq!(cache_counts(&metrics).get("miss"), Some(&1));
    let second = status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second["row_count"], 1);
    assert_eq!(cache_counts(&metrics).get("hit"), Some(&1));
    // A primary lookup that completes inside the operator budget but AFTER the
    // tighter saved budget must tighten the original anchor, not start a timer.
    let mut delayed = admin.begin().await.unwrap();
    sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *delayed)
        .await
        .unwrap();
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1100)).await;
        delayed.rollback().await.unwrap();
    });
    status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::GATEWAY_TIMEOUT,
    )
    .await;
    release.await.unwrap();
    assert!(cache_counts(&metrics).values().all(|count| *count == 0));
    // Fresh primary precedes an otherwise immediately-ready cache hit. A locked
    // primary consumes the initial operator/caller clock, not a new post-read one.
    let mut lock = admin.begin().await.unwrap();
    sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let timed = status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::GATEWAY_TIMEOUT,
    )
    .await;
    assert_eq!(timed["error"], "query deadline exceeded");
    assert!(cache_counts(&metrics).values().all(|count| *count == 0));
    lock.rollback().await.unwrap();
    // Warning-only policy is not a hard scan tariff/cap; retained NULL reset is
    // known inheritance, and revisions separate old payloads from new admissions.
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(1, SavedSettings::new(None, Some(1)).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cache_counts(&metrics).get("miss"), Some(&1));
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(2, SavedSettings::new(None, None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(cache_counts(&metrics).get("miss"), Some(&1));
    for (route, body) in [
        (
            "/api/v1/sql",
            json!({"query":"SELECT raw FROM events", "dry_run":true, "format":"ndjson"}),
        ),
        (
            "/api/v1/sql/local",
            json!({"query":"SELECT raw FROM events", "dry_run":true, "priority":"batch"}),
        ),
        (
            "/api/v1/sql/explain",
            json!({"query":"SELECT raw FROM events", "format":"ndjson"}),
        ),
    ] {
        let body = status(&s.base, &token, route, body, StatusCode::OK).await;
        assert!(body.get("estimated_bytes_scanned").is_some());
    }
    for route in ["/api/v1/sql", "/api/v1/sql/local", "/api/v1/sql/explain"] {
        status(
            &s.base,
            &token,
            route,
            json!({"query":"SELECT 1", "limits":{"timeout_seconds":0,"circuit_breakers":false}}),
            StatusCode::GATEWAY_TIMEOUT,
        )
        .await;
    }
    // Successful absence is bounded inheritance only for the exact verified
    // tuple. Forged hints cannot select a different stored policy.
    let unknown = self::token(&issuer, "acme", "unknown-client");
    status(
        &s.base,
        &unknown,
        "/api/v1/sql/local",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    // Cache follower: leader owns its marker before admission, follower owns a
    // fresh primary read and its own original deadline. No marker survives drop.
    let wait_query = json!({"query":"SELECT raw FROM events WHERE source = 'wait-probe' LIMIT 7", "default_order":false, "limits":{"timeout_seconds":1,"circuit_breakers":false}});
    let held = enabled
        .admission
        .acquire(enabled.limits.admission_budget_bytes)
        .await
        .unwrap();
    cache_counts(&metrics);
    let a = tokio::spawn({
        let base = s.base.clone();
        let token = token.clone();
        let query = wait_query.clone();
        async move {
            post(&base, &token, "/api/v1/sql/local", query)
                .await
                .status()
        }
    });
    wait_cache(&metrics, "miss").await;
    let b = tokio::spawn({
        let base = s.base.clone();
        let token = token.clone();
        let query = wait_query.clone();
        async move {
            post(&base, &token, "/api/v1/sql/local", query)
                .await
                .status()
        }
    });
    wait_cache(&metrics, "wait").await;
    assert_eq!(a.await.unwrap(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(b.await.unwrap(), StatusCode::GATEWAY_TIMEOUT);
    drop(held);
    status(
        &s.base,
        &token,
        "/api/v1/sql/local",
        wait_query,
        StatusCode::OK,
    )
    .await;
    // Durable job refusal before INSERT, not after association/admission. The
    // existing job schema is used unchanged in this disposable DB only.
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let jobs = siglake_query_server::jobs::JobStore::new_postgres(
        &format!("postgres://fixture_admin:@{encoded}:5432/qh_fixture?sslmode=disable"),
        1,
        Duration::from_secs(60),
        Default::default(),
    )
    .await
    .unwrap();
    let job_state = enabled.clone().with_jobs(jobs);
    let job_server = serve(job_state.clone()).await;
    let held = enabled
        .admission
        .acquire(enabled.limits.admission_budget_bytes)
        .await
        .unwrap();
    status(
        &job_server.base,
        &token,
        "/api/v1/sql/local",
        json!({"query":"SELECT raw FROM events", "priority":"batch"}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    for path in [
        "/api/v1/jaeger/events/api/services",
        "/api/v1/jaeger/events/api/services/service/operations",
        "/api/v1/jaeger/events/api/traces",
        "/api/v1/jaeger/events/api/traces/00000000000000000000000000000001",
    ] {
        let response = reqwest::Client::new()
            .get(format!("{}{path}", s.base))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        assert!(response.headers()["x-siglake-query-controls"]
            .to_str()
            .unwrap()
            .contains("state=inherited;rev=3;gen=7;"));
    }
    drop(held);
    // Signed actual local NDJSON is now covered, not a format-hint bypass.
    // The client timeout is deliberately longer than server policy.
    for route in ["/api/v1/sql", "/api/v1/sql/local"] {
        let response = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap()
            .post(format!("{}{route}", s.base)).bearer_auth(&token)
            .json(&json!({"query":"SELECT raw FROM events WHERE host = 'localhost' LIMIT 10", "default_order":false,"format":"ndjson", "limits":{"circuit_breakers":false}}))
            .send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload = response.bytes().await.unwrap();
        let rows: Vec<Value> = payload
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["raw"], "synthetic-row");
    }
    // Real authenticated Axum route + PG primary, with a body the transport
    // NEVER polls. The primary lock spends 700ms of the ORIGINAL 1s budget.
    // Only source/guard cleanup is observed: no client receipt/ACK claim.
    {
        use tower::ServiceExt;
        let mut blocked = admin.begin().await.unwrap();
        sqlx::query("LOCK query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocked)
            .await
            .unwrap();
        let started = tokio::time::Instant::now();
        let route_task = tokio::spawn({
            let service = router(enabled.clone());
            let request = axum::http::Request::builder().method("POST").uri("/api/v1/sql/local")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(serde_json::to_vec(&json!({"query":"SELECT raw FROM events WHERE host = 'localhost' LIMIT 11", "default_order":false,"format":"ndjson", "limits":{"circuit_breakers":false,"timeout_seconds":1}})).unwrap())).unwrap();
            async move { service.oneshot(request).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(700)).await;
        blocked.rollback().await.unwrap();
        let response = route_task.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let reserved = |snapshot: &Snapshotter| {
            snapshot
                .snapshot()
                .into_vec()
                .into_iter()
                .filter_map(|(key, _, _, v)| match v {
                    DebugValue::Gauge(g)
                        if key.key().name() == "siglake_query_admission_reserved_bytes" =>
                    {
                        Some(g.into_inner())
                    }
                    _ => None,
                })
                .sum::<f64>()
        };
        assert!(
            reserved(&metrics) > 0.0,
            "admission remains held through tiny source EOF until outer EOF/deadline"
        );
        // DebuggingRecorder snapshots RESET gauges; do not mistake a drained
        // gauge for cleanup. Reacquiring the entire REAL admission budget is
        // positive proof that the retained stream guard actually released it.
        let all = tokio::time::timeout_at(
            started + Duration::from_millis(1500),
            enabled
                .admission
                .acquire(enabled.limits.admission_budget_bytes),
        )
        .await
        .expect("original server timer must free admission before a reset body timer could")
        .unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "source EOF must not release admission early"
        );
        drop(all);
        assert_eq!(siglake_query_server::queries_in_flight(), 0);
        let payload = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&payload[..], b"{\"_meta\":\"timeout\"}\n");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM siglake_query_jobs")
            .fetch_one(&admin)
            .await
            .unwrap(),
        0
    );
    // An actual fanout is refused, but distributed local fallback and synchronous
    // tier-1 batch/dry-run selection are preserved (not mislabeled as jobs).
    let dispatches = Arc::new(AtomicUsize::new(0));
    let count = dispatches.clone();
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_url = format!("http://{}", peer.local_addr().unwrap());
    let peer_task = tokio::spawn(async move {
        axum::serve(
            peer,
            axum::Router::new().fallback(move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }),
        )
        .await
        .unwrap();
    });
    let peer_owner = Server {
        base: peer_url.clone(),
        task: peer_task,
        state: None,
    };
    let dist = serve(
        enabled
            .clone()
            .with_coordinator(vec![peer_url], Some("synthetic-coordinator".into())),
    )
    .await;
    status(
        &dist.base,
        &token,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    let tier1 = status(
        &dist.base,
        &token,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT count(*) FROM events", "priority":"batch", "dry_run":true}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(tier1["row_count"], 1);
    for route in ["/api/v1/sql", "/api/v1/sql/distributed"] {
        status(&dist.base, &token, route, json!({"query":"SELECT sum(length(raw)) AS total FROM events", "dry_run":route.ends_with("distributed")}), StatusCode::SERVICE_UNAVAILABLE).await;
    }
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    // Controlled coordinator shards still refuse before work; ordinary direct
    // shards now resolve their own identity (covered by shard_controls).
    status(
        &dist.base,
        "synthetic-coordinator",
        "/api/v1/sql/shard",
        json!({"query":"SELECT raw FROM events", "tenant":"foreign"}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    // Unsafe checkout must deny before cache/execution for both named and open
    // callers. No stale health latch, role bypass or policy-negative caching.
    let generic = serve(
        AppState::new(ice.clone(), AuthConfig::open())
            .with_query_controls(reader.clone(), 8)
            .unwrap(),
    )
    .await;
    status(
        &generic.base,
        "",
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    sqlx::query("GRANT fixture_other TO qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    for (base, credential) in [(&s.base, token.as_str()), (&generic.base, "")] {
        let body = status(
            base,
            credential,
            "/api/v1/sql",
            json!({"query":"SELECT 1"}),
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await;
        assert_eq!(body["error"], "query_policy_unavailable");
    }
    sqlx::query("REVOKE fixture_other FROM qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    status(
        &generic.base,
        "",
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    // Operational connection failure is not a successful absence either.
    sqlx::query("REVOKE CONNECT ON DATABASE qh_fixture FROM qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='qp_reader' AND datname=current_database()")
        .execute(&admin).await.unwrap();
    for (base, credential) in [(&s.base, token.as_str()), (&generic.base, "")] {
        let body = status(
            base,
            credential,
            "/api/v1/sql",
            json!({"query":"SELECT 1"}),
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await;
        assert_eq!(body["error"], "query_policy_unavailable");
    }
    sqlx::query("GRANT CONNECT ON DATABASE qh_fixture TO qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    status(
        &generic.base,
        "",
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    reader.close();
    for (base, credential) in [(&s.base, token.as_str()), (&generic.base, "")] {
        status(
            base,
            credential,
            "/api/v1/sql",
            json!({"query":"SELECT 1"}),
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await;
        status(
            base,
            credential,
            "/api/v1/sql",
            json!({"query":"SELECT 1", "limits":{"timeout_seconds":0}}),
            StatusCode::GATEWAY_TIMEOUT,
        )
        .await;
    }
    let registry = enabled.tenants.as_ref().unwrap();
    let retained = registry.cached_tenant_count().await;
    let unopened = self::token(&issuer, "not_admitted", "unknown-client");
    status(
        &s.base,
        &unopened,
        "/api/v1/sql",
        json!({"query":"SELECT raw FROM events"}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    assert_eq!(
        registry.cached_tenant_count().await,
        retained,
        "policy failure must precede tenant/catalog resolution"
    );
    // Completely absent authority remains baseline-compatible, even with the
    // same closed PolicyReader elsewhere. It never gets partial-mode guards.
    let old = serve(baseline).await;
    status(
        &old.base,
        &token,
        "/api/v1/sql/local",
        json!({"query":"SELECT 1", "limits":{"timeout_seconds":0,"circuit_breakers":false}}),
        StatusCode::OK,
    )
    .await;
    let response = post(
        &old.base,
        &token,
        "/api/v1/sql/local",
        json!({"query":"SELECT 1", "format":"ndjson"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.bytes().await.unwrap();
    job_state.jobs.shutdown().await.unwrap();
    for server in [job_server, dist, generic, old, s, peer_owner, issuer_owner] {
        server.shutdown().await;
    }
    enabled.shutdown_query_controls().await;
    manager.close();
    admin.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned synthetic primary/JWT fixture; scripts/check-query-policy.sh"]
async fn real_signed_explicit_distributed_batch_refuses_before_work_not_handoff() {
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let issuer_owner = Server {
        base: issuer.clone(),
        task: issuer_task,
        state: None,
    };
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let credential = token(&issuer, "acme", "distributed-clock-client");
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "distributed-clock-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    manager
        .save(
            &binding,
            &VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap(),
            SaveRequest::new(0, SavedSettings::new(Some(1), None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("synthetic-row")])
        .await
        .unwrap();
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(2);
    limits.interactive.ceiling_timeout = Duration::from_secs(3);
    limits.batch.default_timeout = Duration::from_secs(2);
    limits.batch.ceiling_timeout = Duration::from_secs(3);
    // Configured membership, but these metadata/local-only requests must never
    // dispatch. No peer service is started or contacted by this fixture.
    let baseline = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_limits(limits)
        .with_coordinator(
            vec!["http://127.0.0.1:9".into()],
            Some("synthetic-coordinator".into()),
        );
    let enabled = baseline
        .clone()
        .with_query_controls(reader.clone(), 9)
        .unwrap();
    let controlled = serve(enabled.clone()).await;
    let legacy = serve(baseline.clone()).await;
    let route = "/api/v1/sql/distributed";
    for query in ["SELECT count(*) FROM events", "SELECT 1"] {
        for seconds in [0, 1, 2] {
            let body = status(&controlled.base, &credential, route,
                json!({"query":query,"priority":"batch","limits":{"timeout_seconds":seconds,"circuit_breakers":false}}),
                StatusCode::SERVICE_UNAVAILABLE).await;
            assert_eq!(body["error"], "query_controls_path_unavailable");
        }
    }
    assert_eq!(enabled.jobs.active_count().await, 0);
    // Dry-run and interactive Tier1 keep request-clock semantics, not the
    // unsupported batch-execution branch and not the operator handoff budget.
    for extra in [json!({}), json!({"priority":"batch","dry_run":true})] {
        let mut body = json!({"query":"SELECT count(*) FROM events","limits":{"timeout_seconds":0,"circuit_breakers":false}});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        status(
            &controlled.base,
            &credential,
            route,
            body.clone(),
            StatusCode::GATEWAY_TIMEOUT,
        )
        .await;
        body["limits"]["timeout_seconds"] = json!(2);
        let success = status(
            &controlled.base,
            &credential,
            route,
            body.clone(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(success["row_count"], 1);
        // Charge a delayed primary read to ORIGINAL ingress: the one-second
        // policy has expired, although the two-second operator clock has not.
        let mut lock = admin.begin().await.unwrap();
        sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1100)).await;
            lock.rollback().await.unwrap();
        });
        status(
            &controlled.base,
            &credential,
            route,
            body,
            StatusCode::GATEWAY_TIMEOUT,
        )
        .await;
        release.await.unwrap();
    }
    // No authority retains baseline synchronous Tier1 batch behavior, including
    // its existing optional timeout opt-out at zero; no job is fabricated.
    let old = status(&legacy.base, &credential, route,
        json!({"query":"SELECT count(*) FROM events","priority":"batch","limits":{"timeout_seconds":0,"circuit_breakers":false}}),
        StatusCode::OK).await;
    assert_eq!(old["row_count"], 1);
    assert!(old.get("job_id").is_none());
    // Actual main/local durable selection must STILL hand off zero/short runs.
    for path in ["/api/v1/sql", "/api/v1/sql/local"] {
        for seconds in [0, 1] {
            let response = post(&controlled.base, &credential, path,
                json!({"query":"SELECT 1","priority":"batch","limits":{"timeout_seconds":seconds,"circuit_breakers":false}})).await;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let accepted: Value = response.json().await.unwrap();
            assert_eq!(
                accepted["controls"]["accepted"]["effective_timeout_ms"],
                seconds * 1000
            );
            let id = siglake_query_server::jobs::JobId::parse(accepted["job_id"].as_str().unwrap())
                .unwrap();
            let terminal = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let info = enabled.jobs.info(id).await.unwrap().unwrap();
                    if info.status.is_terminal() {
                        break info.status;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                terminal,
                if seconds == 0 {
                    siglake_query_server::jobs::JobStatus::Timeout
                } else {
                    siglake_query_server::jobs::JobStatus::Succeeded
                }
            );
        }
    }
    // Closed primary + invalid SQL still produce the declared early branch
    // refusal, not a primary-read/planning error: work must not be polled.
    reader.close();
    let refused = status(
        &controlled.base,
        &credential,
        route,
        json!({"query":"not valid SQL","priority":"batch","limits":{"timeout_seconds":1}}),
        StatusCode::SERVICE_UNAVAILABLE,
    )
    .await;
    assert_eq!(refused["error"], "query_controls_path_unavailable");
    controlled.shutdown().await;
    legacy.shutdown().await;
    enabled.jobs.shutdown().await.unwrap();
    issuer_owner.shutdown().await;
}

#[path = "fanout_controls.rs"]
mod fanout_controls;
#[path = "jaeger_controls.rs"]
mod jaeger_controls;
#[path = "shard_controls.rs"]
mod shard_controls;
