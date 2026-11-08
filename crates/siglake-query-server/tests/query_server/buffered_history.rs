//! Real HTTP execution against a disposable engine warehouse and PostgreSQL.
//! Reuse the public local RS256/JWKS fixture, never a fake query response.
use super::{access_claims, sign_claims, spawn_fake_idp, TEST_KID};
#[path = "history_fixture.rs"]
mod history_fixture;
use axum::http::StatusCode;
use history_fixture::{fixture, options};
use jsonwebtoken::Algorithm;
use serde_json::{json, Value};
use siglake_query_server::{
    history::HistoryService, router, AppState, AuthConfig, OidcVerifier, ServerLimits,
    TenantRegistry,
};
use siglake_storage::iceberg::IcebergContext;
use sqlx::{PgPool, Row};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;
const CORRELATION: &str = "x-siglake-history-id";

struct Server {
    base: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(state: AppState) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router(state)).await.unwrap() });
    Server { base, task }
}
fn token(issuer: &str, tenant: &str, client: &str, renewal: u64) -> String {
    let mut claims = access_claims(issuer);
    claims["custom:org_id"] = json!(tenant);
    claims["client_id"] = json!(client);
    claims["sub"] = json!("PRIVATE_SUBJECT");
    claims["exp"] = json!(claims["exp"].as_u64().unwrap() + renewal);
    sign_claims(&claims, Algorithm::RS256, Some(TEST_KID))
}
async fn post(s: &Server, token: &str, route: &str, body: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{route}", s.base))
        .bearer_auth(token)
        .header("x-scope-orgid", "forged-tenant")
        .header("x-client-id", "forged-client")
        .header(CORRELATION, Uuid::nil().to_string())
        .json(&body)
        .send()
        .await
        .unwrap()
}
async fn row(db: &PgPool, id: Uuid) -> Value {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let found: Option<Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1 AND observation_state='terminal'")
            .bind(id).fetch_optional(db).await.unwrap();
        if let Some(found) = found {
            return found;
        }
        assert!(
            Instant::now() < deadline,
            "terminal observation did not persist"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn correlation(response: &reqwest::Response) -> Uuid {
    let id = Uuid::parse_str(
        response
            .headers()
            .get(CORRELATION)
            .expect("real receipt correlation")
            .to_str()
            .unwrap(),
    )
    .unwrap();
    assert!(!id.is_nil());
    id
}
async fn count(db: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(db)
        .await
        .unwrap()
}
async fn observed(
    s: &Server,
    token: &str,
    db: &PgPool,
    route: &str,
    body: Value,
    status: StatusCode,
) -> Value {
    let before = count(db).await;
    let response = post(s, token, route, body).await;
    assert_eq!(response.status(), status);
    let id = correlation(&response);
    let body = response.json::<Value>().await.unwrap();
    let row = row(db, id).await;
    if let Some(returned_rows) = row["returned_rows"].as_u64() {
        assert_eq!(body["row_count"].as_u64(), Some(returned_rows));
    }
    assert_eq!(count(db).await, before + 1, "one UUID per external parent");
    row
}
async fn unobserved(
    s: &Server,
    token: &str,
    db: &PgPool,
    route: &str,
    body: Value,
    status: StatusCode,
) {
    let before = count(db).await;
    let response = post(s, token, route, body).await;
    assert_eq!(response.status(), status);
    assert!(!response.headers().contains_key(CORRELATION));
    // Consume deferred streams/jobs as HTTP responses only; no false success row.
    let _ = response.bytes().await.unwrap();
    assert_eq!(count(db).await, before);
}

#[tokio::test]
#[ignore = "real disposable PostgreSQL; scripts/check-query-history-buffered.sh"]
async fn real_router_buffered_history_identity_lifecycle_privacy_and_outage() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let observer = service.observer();
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = tmp.path().join("warehouse");
    let ice = Arc::new(IcebergContext::open(&warehouse).await.unwrap());
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_history(observer.clone());
    // Real synthetic engine data, NOT seeded history events. A snapshot is
    // required by the production result-cache key; empty namespaces skip cache.
    let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
    tenant_ice
        .append_events(&[siglake_core::Event::now("PRIVATE_RESULT_NOT_A_NUMBER")])
        .await
        .unwrap();
    let s = server(state.clone()).await;
    let t = token(&issuer, "acme", "client-one", 0);
    let renewed = token(&issuer, "acme", "client-one", 60);
    assert_ne!(t, renewed);
    let query = json!({"query":"SELECT raw FROM events WHERE host = 'localhost' LIMIT 10", "tenant":"forged-tenant", "client_id":"forged-client"});
    let miss = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        query.clone(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(miss["outcome"], "success");
    assert_eq!(miss["cache_state"], "miss");
    assert_eq!(miss["execution_admitted"], true);
    assert_eq!(miss["returned_rows"], 1);
    assert!(miss["measured_rows_scanned"].as_i64().is_some());
    let hit = observed(
        &s,
        &renewed,
        &f.admin,
        "/api/v1/sql/local",
        query,
        StatusCode::OK,
    )
    .await;
    assert_eq!(hit["cache_state"], "hit");
    assert_eq!(hit["returned_rows"], 1);
    assert!(hit["measured_rows_scanned"].is_null());
    assert!(hit["measured_bytes_scanned"].is_null());
    for r in [&miss, &hit] {
        assert_eq!(r["tenant"], "acme");
        assert_eq!(r["issuer"], issuer);
        assert_eq!(r["client_id"], "client-one");
        assert_eq!(r["principal_kind"], "provider");
    }
    assert_ne!(miss["execution_id"], hit["execution_id"]);
    let other = token(&issuer, "foreign", "client-two", 0);
    let second = observed(
        &s,
        &other,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 2 AS n"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(second["tenant"], "foreign");
    assert_eq!(second["client_id"], "client-two");
    let id = Uuid::parse_str(second["execution_id"].as_str().unwrap()).unwrap();
    assert!(f.reader.detail("acme", id).await.unwrap().is_none());
    assert!(f.reader.detail("foreign", id).await.unwrap().is_some());
    for (route, dry_run) in [("/api/v1/sql", true), ("/api/v1/sql/explain", false)] {
        let planning = observed(
            &s,
            &t,
            &f.admin,
            route,
            json!({"query":"SELECT raw FROM events", "dry_run":dry_run}),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            planning["operation"],
            if dry_run { "sql" } else { "explain" }
        );
        assert_eq!(planning["dry_run"], dry_run);
        assert_eq!(planning["execution_admitted"], true);
        for field in [
            "returned_rows",
            "measured_rows_scanned",
            "measured_bytes_scanned",
        ] {
            assert!(planning[field].is_null());
        }
        assert!(planning["estimated_bytes_scanned"].as_i64().is_some());
    }
    for (sql, code) in [
        ("", "validation"),
        ("SELECT (", "invalid_sql"),
        ("SELECT missing_column FROM events", "planning"),
    ] {
        let rejected = observed(
            &s,
            &t,
            &f.admin,
            "/api/v1/sql",
            json!({"query":sql}),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(rejected["outcome"], "rejected");
        assert_eq!(rejected["execution_admitted"], false);
        assert_eq!(rejected["error_code"], code);
        assert!(rejected["measured_rows_scanned"].is_null());
        assert!(rejected["estimated_bytes_scanned"].is_null());
    }
    let failed = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT CAST(raw AS BIGINT) FROM events"}),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(failed["outcome"], "failed");
    assert_eq!(failed["error_code"], "execution");
    assert_eq!(failed["execution_admitted"], true);
    let not_configured = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(not_configured["outcome"], "rejected");
    assert_eq!(not_configured["execution_admitted"], false);
    // A real catalog-open failure AFTER selecting a named routing tenant.
    std::fs::write(warehouse.join("tenant_broken"), b"fixture obstruction").unwrap();
    let broken = token(&issuer, "broken", "client-one", 0);
    let catalog_failure = observed(
        &s,
        &broken,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::INTERNAL_SERVER_ERROR,
    )
    .await;
    assert_eq!(catalog_failure["tenant"], "broken");
    assert_eq!(catalog_failure["execution_admitted"], false);
    assert_eq!(catalog_failure["error_code"], "execution");
    // Admission rejection at the real reservation boundary (no HTTP200 inference).
    let limits = ServerLimits {
        admission_budget_bytes: 1024,
        admission_wait_timeout: Duration::from_millis(20),
        ..Default::default()
    };
    let admission_state = state.clone().with_limits(limits);
    let held = admission_state.admission.acquire(1024).await.unwrap();
    let admission_server = server(admission_state).await;
    let rejected = observed(
        &admission_server,
        &t,
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT 7 AS admission_probe"}),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await;
    assert_eq!(rejected["error_code"], "admission");
    assert_eq!(rejected["execution_admitted"], false);
    drop(held);
    let mut timeout_limits = ServerLimits::default();
    timeout_limits.interactive.default_timeout = Duration::from_nanos(1);
    let timeout_server = server(state.clone().with_limits(timeout_limits)).await;
    let timed_out = observed(
        &timeout_server,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::GATEWAY_TIMEOUT,
    )
    .await;
    assert_eq!(timed_out["outcome"], "timed_out");
    assert_eq!(timed_out["error_code"], "timeout");
    assert_eq!(timed_out["execution_admitted"], false);
    assert!(timed_out["estimated_bytes_scanned"].is_null());
    let fast_path = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT count(*) AS n FROM events"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(fast_path["outcome"], "success");
    assert_eq!(fast_path["returned_rows"], 1);
    assert_eq!(fast_path["execution_admitted"], true);
    let explain_error = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/explain",
        json!({"query":"SELECT missing_column FROM events"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(explain_error["operation"], "explain");
    assert_eq!(explain_error["outcome"], "rejected");
    assert_eq!(explain_error["error_code"], "planning");
    // Distinct clients cannot overwrite each other's provider attribution,
    // even when reading the same tenant with forged provider/body hints.
    let same_tenant_other_client = token(&issuer, "acme", "client-two", 0);
    let other_client = observed(
        &s,
        &same_tenant_other_client,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 2", "client_id":"client-one"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(other_client["tenant"], "acme");
    assert_eq!(other_client["client_id"], "client-two");
    // Auth and JSON extractor failures never enter the observation wrapper.
    unobserved(
        &s,
        "invalid-synthetic-token",
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::UNAUTHORIZED,
    )
    .await;
    let invalid_tenant = token(&issuer, "../bad", "client-one", 0);
    unobserved(
        &s,
        &invalid_tenant,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::FORBIDDEN,
    )
    .await;
    unobserved(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1", "limits":{"max_rows":1}}),
        StatusCode::UNPROCESSABLE_ENTITY,
    )
    .await;
    unobserved(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1", "format":"ndjson"}),
        StatusCode::OK,
    )
    .await;
    unobserved(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1", "priority":"batch"}),
        StatusCode::ACCEPTED,
    )
    .await;
    // Configured coordinator fallback is explicitly deferred, even if it would
    // execute locally. No nested wrapper may create a phantom parent.
    let coordinator = server(
        state
            .clone()
            .with_coordinator(vec!["http://127.0.0.1:1".into()], None),
    )
    .await;
    unobserved(
        &coordinator,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    unobserved(
        &coordinator,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    // A signed claim WITHOUT the registry still executes against shared default
    // storage: do not incorrectly attribute that data to the unused claim.
    let mut default_state = state.clone();
    default_state.tenants = None;
    let default_server = server(default_state).await;
    unobserved(
        &default_server,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    for auth in [
        AuthConfig::open(),
        AuthConfig::from_tokens(["synthetic-static".to_owned()]),
    ] {
        let open = server(
            AppState::new(ice.clone(), auth)
                .with_tenants(TenantRegistry::new(ice.clone()))
                .with_history(observer.clone()),
        )
        .await;
        unobserved(
            &open,
            "synthetic-static",
            &f.admin,
            "/api/v1/sql",
            json!({"query":"SELECT 1"}),
            StatusCode::OK,
        )
        .await;
    }
    // Generic verified identities retain tenant rows but not provider attribution.
    let generic = OidcVerifier::from_issuer(issuer.clone(), "human-audience".into())
        .await
        .unwrap()
        .with_tenant_claim("custom:org_id");
    let generic_server = server(
        AppState::new(ice.clone(), AuthConfig::from_oidc(generic))
            .with_tenants(TenantRegistry::new(ice.clone()))
            .with_history(observer.clone()),
    )
    .await;
    let mut generic_claims = access_claims(&issuer);
    generic_claims["aud"] = json!("human-audience");
    let generic_token = sign_claims(&generic_claims, Algorithm::RS256, Some(TEST_KID));
    let unknown = observed(
        &generic_server,
        &generic_token,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(unknown["principal_kind"], "unknown");
    assert!(unknown["client_id"].is_null());
    // Admin inspection of EVERY collected execution, without dumping row data.
    let facts = sqlx::query("SELECT count(*) AS n, count(DISTINCT execution_id) AS ids, count(DISTINCT owner_uuid) AS owners, bool_and(redacted_sql IS NULL AND redaction_status='omitted' AND expires_at-received_at=interval '720 hours' AND admitted_at IS NULL AND finished_at IS NULL AND finished_observed_at IS NOT NULL AND duration_ms IS NOT NULL AND observation_state='terminal') AS safe FROM query_history.executions").fetch_one(&f.admin).await.unwrap();
    let n: i64 = facts.get("n");
    assert_eq!(n, 17);
    assert_eq!(n, facts.get::<i64, _>("ids"));
    assert_eq!(facts.get::<i64, _>("owners"), 1);
    assert!(facts.get::<bool, _>("safe"));
    let serialized: String = sqlx::query_scalar(
        "SELECT coalesce(jsonb_agg(to_jsonb(e))::text,'[]') FROM query_history.executions e",
    )
    .fetch_one(&f.admin)
    .await
    .unwrap();
    for secret in [
        "PRIVATE_RESULT",
        "PRIVATE_SUBJECT",
        "forged-client",
        "forged-tenant",
        "SELECT",
        &t,
        &renewed,
    ] {
        assert!(!serialized.contains(secret), "history privacy invariant");
    }
    // History terminal writes blocked by a real PG lock stay bounded and do
    // not replace the successful query response. Recovery keeps the SAME UUID.
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let started = Instant::now();
    let response = post(
        &s,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 42 AS outage_probe"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let retry_id = correlation(&response);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        response.json::<Value>().await.unwrap()["rows"][0]["outage_probe"],
        42
    );
    lock.rollback().await.unwrap();
    let recovered = row(&f.admin, retry_id).await;
    assert_eq!(recovered["outcome"], "success");
    assert_eq!(count(&f.admin).await, n + 1);
    // Receipt/initial-write delay must not consume or reset the existing
    // execution budget. This is a real lock/deadline, not paused test time.
    let mut latency_limits = ServerLimits::default();
    latency_limits.interactive.default_timeout = Duration::from_millis(80);
    let bounded_server = server(state.clone().with_limits(latency_limits)).await;
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let response = post(
        &bounded_server,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 44 AS bounded_probe"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let bounded_id = correlation(&response);
    lock.rollback().await.unwrap();
    let bounded_row = row(&f.admin, bounded_id).await;
    assert_eq!(bounded_row["outcome"], "success");
    assert!(bounded_row["duration_ms"].as_i64().unwrap() < 80);
    assert_eq!(count(&f.admin).await, n + 2);
    // No-receipt failure: an explicitly preflighted fixture writer whose pool
    // loses connectivity. No implicit URL/env resolver or fabricated header.
    let outage_writer =
        siglake_query_server::history::HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap();
    let mut outage_service = HistoryService::new(outage_writer);
    let outage_server = server(state.clone().with_history(outage_service.observer())).await;
    // Terminate only this fixture role's sessions then revoke fixture CONNECT.
    // Existing production/environment targets are impossible in this helper.
    sqlx::query("REVOKE CONNECT ON DATABASE qh_fixture FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='qh_writer' AND pid<>pg_backend_pid()").execute(&f.admin).await.unwrap();
    let started = Instant::now();
    let response = post(
        &outage_server,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT 43 AS no_receipt_probe"}),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(CORRELATION));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(
        response.json::<Value>().await.unwrap()["rows"][0]["no_receipt_probe"],
        43
    );
    sqlx::query("GRANT CONNECT ON DATABASE qh_fixture TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert_eq!(count(&f.admin).await, n + 2);
    let _ = outage_service.shutdown().await;
    let _ = service.shutdown().await;
    // Observer clones do not keep the sole service owner/task alive.
    drop(service);
    let closed = server(state.clone().with_history(observer)).await;
    unobserved(
        &closed,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    let drop_writer =
        siglake_query_server::history::HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .unwrap();
    let drop_service = HistoryService::new(drop_writer);
    let surviving_observer = drop_service.observer();
    drop(drop_service); // No shutdown: a clone still must NOT own the task.
    let dropped_owner = server(state.with_history(surviving_observer)).await;
    unobserved(
        &dropped_owner,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1"}),
        StatusCode::OK,
    )
    .await;
    issuer_task.abort();
    eprintln!("Buffered real-router proof: 19 unique durable execution rows; one owner; safe NULL SQL/30-day expiry; renewed RS256 client; typed planning/cache/errors/admission; real PG lock/retry/no-receipt faults; auth/extractor/deferred/default routes add zero rows.");
}

#[tokio::test]
#[ignore = "real disposable PostgreSQL; scripts/check-query-history-buffered.sh"]
async fn routed_boundary_count_reads_files_without_claiming_measured_zero() {
    use chrono::{TimeZone, Utc};
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    let tenant_ice = ice.for_namespace("tenant_acme").await.unwrap();
    // One minute bucket, three distinct timestamps. Neither the whole-file
    // manifest count nor its minute aggregate can answer the partial window.
    let events = [1, 30, 59].map(|second| {
        let mut event = siglake_core::Event::now("fixture");
        event.timestamp = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, second).unwrap();
        event
    });
    tenant_ice.append_events(&events).await.unwrap();
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_history(service.observer());
    let s = server(state).await;
    let t = token(&issuer, "acme", "client-one", 0);
    let query = |lo, hi| {
        json!({"query":format!(
        "SELECT count(*) AS n FROM events WHERE timestamp >= to_timestamp('2026-01-01T00:00:{lo:02}Z') AND timestamp < to_timestamp('2026-01-01T00:00:{hi:02}Z')")})
    };
    let response = post(&s, &t, "/api/v1/sql/local", query(20, 40)).await;
    assert_eq!(response.status(), StatusCode::OK);
    let id = correlation(&response);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 1);
    // Normal physical-plan execution is explicitly served_by=scan. This
    // successful response instead came from the windowed-count battery branch,
    // whose legacy public ScanStats defaults must NOT become history counters.
    assert!(body["stats"]["served_by"].is_null());
    assert_eq!(body["stats"]["rows_scanned"], 0);
    assert!(body["stats"]["phases"].is_object());
    let stored = row(&f.admin, id).await;
    assert_eq!(stored["outcome"], "success");
    assert_eq!(stored["returned_rows"], 1);
    assert_eq!(stored["truncated"], false);
    assert!(stored["measured_rows_scanned"].is_null());
    assert!(stored["measured_bytes_scanned"].is_null());
    assert_eq!(stored["estimated_rows_processed"], 3);
    assert!(stored["estimated_bytes_scanned"].as_i64().unwrap() > 0);
    assert_eq!(body["cost"]["exact"], false, "valid heuristic retained");
    // Establish a real dependency on boundary data-file reads, not merely a
    // detector/helper test: the same snapshot/aggregate with an uncached window
    // fails when this fixture's one Parquet file is absent. Restore it at once.
    fn parquet_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                parquet_files(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                out.push(path);
            }
        }
    }
    let mut paths = vec![];
    parquet_files(&tmp.path().join("warehouse/tenant_acme"), &mut paths);
    assert_eq!(paths.len(), 1);
    let path = &paths[0];
    let absent = path.with_extension("fixture-absent");
    std::fs::rename(path, &absent).unwrap();
    let response = post(&s, &t, "/api/v1/sql/local", query(21, 41)).await;
    std::fs::rename(&absent, path).unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let failed = row(&f.admin, correlation(&response)).await;
    assert_eq!(failed["outcome"], "failed");
    assert!(failed["returned_rows"].is_null());
    // Whole-table metadata count has independent positive no-data-scan proof.
    let whole = observed(
        &s,
        &t,
        &f.admin,
        "/api/v1/sql/local",
        json!({"query":"SELECT count(*) AS n FROM events"}),
        StatusCode::OK,
    )
    .await;
    assert_eq!(whole["measured_rows_scanned"], 0);
    assert_eq!(whole["measured_bytes_scanned"], 0);
    assert_eq!(count(&f.admin).await, 3);
    service.shutdown().await;
    issuer_task.abort();
}
