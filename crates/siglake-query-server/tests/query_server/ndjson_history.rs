//! Local signed-issuer + real router/engine/PG. Body drop is in-process proof,
//! NOT a controlled TCP disconnect, cancellation acknowledgement or delivery.
use super::*;
use axum::{body::Body, http::Request};
use futures::StreamExt;
use tower::ServiceExt;

async fn in_process(state: AppState, token: &str, body: Value) -> axum::response::Response {
    router(state)
        .oneshot(
            Request::post("/api/v1/sql/local")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .header(CORRELATION, Uuid::nil().to_string())
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}
fn body_id(response: &axum::response::Response) -> Uuid {
    Uuid::parse_str(
        response
            .headers()
            .get(CORRELATION)
            .unwrap()
            .to_str()
            .unwrap(),
    )
    .unwrap()
}
async fn not_terminal(db: &PgPool, id: Uuid) {
    let pending: bool = sqlx::query_scalar("SELECT outcome IS NULL AND finished_observed_at IS NULL FROM query_history.executions WHERE execution_id=$1")
        .bind(id).fetch_one(db).await.unwrap();
    assert!(
        pending,
        "HTTP200/stream construction is not terminal success"
    );
}
async fn consume(s: &Server, t: &str, db: &PgPool, query: Value) -> (Value, Vec<u8>) {
    let response = post(s, t, "/api/v1/sql/local", query).await;
    assert_eq!(response.status(), StatusCode::OK);
    let id = correlation(&response);
    let body = response.bytes().await.unwrap().to_vec();
    (row(db, id).await, body)
}
#[tokio::test]
#[ignore = "own disposable PG; scripts/check-query-history-ndjson.sh"]
async fn real_router_ndjson_eof_error_truncation_drop_identity_and_outage() {
    let f = fixture().await;
    let mut service = HistoryService::new(f.writer);
    let observer = service.observer();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        IcebergContext::open(&tmp.path().join("warehouse"))
            .await
            .unwrap(),
    );
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[
            siglake_core::Event::now("PRIVATE_STREAM_RESULT"),
            siglake_core::Event::now("PRIVATE_STREAM_RESULT"),
        ])
        .await
        .unwrap();
    let (issuer, issuer_task) = spawn_fake_idp().await;
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let state = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_history(observer.clone());
    let s = server(state.clone()).await;
    let t = token(&issuer, "acme", "stream-client", 0);
    let renewed = token(&issuer, "acme", "stream-client", 60);
    let query = json!({"query":"SELECT raw FROM events", "format":"ndjson"});
    // The real engine stream outlives the handler. Nothing has polled the body.
    let response = in_process(state.clone(), &t, query.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let id = body_id(&response);
    assert!(!id.is_nil());
    not_terminal(&f.admin, id).await;
    let body = axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap();
    assert_eq!(
        body.split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        2
    );
    let success = row(&f.admin, id).await;
    assert_eq!(success["outcome"], "success");
    assert_eq!(success["format"], "ndjson");
    assert_eq!(success["returned_rows"], 2);
    assert_eq!(success["truncated"], false);
    assert_eq!(success["execution_admitted"], true);
    assert!(success["duration_ms"].is_number());
    // Renewed verified identity, transparent no-peer route and spoof boundaries.
    let response = post(&s, &renewed, "/api/v1/sql", query.clone()).await;
    let renewed_id = correlation(&response);
    let _ = response.bytes().await.unwrap();
    let renewed_row = row(&f.admin, renewed_id).await;
    for r in [&success, &renewed_row] {
        assert_eq!(r["tenant"], "acme");
        assert_eq!(r["issuer"], issuer);
        assert_eq!(r["client_id"], "stream-client");
        assert_eq!(r["cache_state"], "bypass");
    }
    let foreign_token = token(&issuer, "foreign", "other-client", 0);
    let (foreign, _) = consume(
        &s,
        &foreign_token,
        &f.admin,
        json!({"query":"SELECT 7 AS n", "format":"ndjson"}),
    )
    .await;
    let foreign_id = Uuid::parse_str(foreign["execution_id"].as_str().unwrap()).unwrap();
    assert_eq!(foreign["tenant"], "foreign");
    assert_eq!(foreign["client_id"], "other-client");
    assert!(f.reader.detail("acme", foreign_id).await.unwrap().is_none());
    assert!(f
        .reader
        .detail("foreign", foreign_id)
        .await
        .unwrap()
        .is_some());
    // A real expression fails during polling, AFTER 200, and is encoded as
    // Ok(JSON marker bytes) by the existing source. History must use the typed
    // error branch, never Rust Err/status/EOF or marker parsing.
    let (failed, body) = consume(
        &s,
        &t,
        &f.admin,
        json!({"query":"SELECT CAST(raw AS BIGINT) AS n FROM events", "format":"ndjson"}),
    )
    .await;
    assert!(body
        .windows(b"\"_meta\":\"error\"".len())
        .any(|w| w == b"\"_meta\":\"error\""));
    assert_eq!(failed["outcome"], "failed");
    assert_eq!(failed["error_code"], "execution");
    assert_eq!(failed["execution_admitted"], true);
    assert!(failed["returned_rows"].is_null());
    assert!(failed["measured_rows_scanned"].is_null());
    let (truncated, _) = consume(&s, &t, &f.admin, json!({"query":"SELECT raw FROM events", "format":"ndjson", "limits":{"max_rows_returned":1}})).await;
    assert_eq!(truncated["outcome"], "success");
    assert_eq!(truncated["returned_rows"], 1);
    assert_eq!(truncated["truncated"], true);
    assert!(truncated["measured_rows_scanned"].is_null());
    assert!(truncated["measured_bytes_scanned"].is_null());
    // Metadata count is typed no-data-scan; the other materialized fast paths
    // must not infer scans from legacy default-zero response stats.
    let (metadata, _) = consume(
        &s,
        &t,
        &f.admin,
        json!({"query":"SELECT count(*) AS n FROM events", "format":"ndjson"}),
    )
    .await;
    assert_eq!(metadata["returned_rows"], 1);
    assert_eq!(metadata["measured_rows_scanned"], 0);
    assert_eq!(metadata["measured_bytes_scanned"], 0);
    // Existing timeout is before body construction; do NOT add a new body
    // deadline or claim that this proves a mid-body wall-clock timeout.
    let mut timeout_limits = ServerLimits::default();
    timeout_limits.interactive.default_timeout = Duration::from_nanos(1);
    let timeout_server = server(state.clone().with_limits(timeout_limits)).await;
    let timed = post(
        &timeout_server,
        &t,
        "/api/v1/sql/local",
        json!({"query":"SELECT raw FROM events", "format":"ndjson"}),
    )
    .await;
    assert_eq!(timed.status(), StatusCode::GATEWAY_TIMEOUT);
    let timeout_row = row(&f.admin, correlation(&timed)).await;
    assert_eq!(timeout_row["outcome"], "timed_out");
    assert_eq!(timeout_row["error_code"], "timeout");
    // Actual in-process Body abandonment with a ready real engine stream.
    let response = in_process(state.clone(), &t, query.clone()).await;
    let dropped_id = body_id(&response);
    not_terminal(&f.admin, dropped_id).await;
    let start = Instant::now();
    drop(response);
    assert!(start.elapsed() < Duration::from_millis(100));
    let dropped = row(&f.admin, dropped_id).await;
    assert_eq!(dropped["outcome"], "disconnected");
    assert_eq!(dropped["error_code"], "disconnected");
    for field in [
        "returned_rows",
        "measured_rows_scanned",
        "measured_bytes_scanned",
        "truncated",
    ] {
        assert!(dropped[field].is_null());
    }
    let response = in_process(state.clone(), &t, query.clone()).await;
    let partial_id = body_id(&response);
    let mut body = response.into_body().into_data_stream();
    assert!(body.next().await.unwrap().is_ok());
    not_terminal(&f.admin, partial_id).await;
    drop(body); // Real body drop AFTER a data chunk, before source EOF.
    assert_eq!(row(&f.admin, partial_id).await["outcome"], "disconnected");
    // A locked store cannot block start beyond the existing bounded budget,
    // stream production, EOF/drop, or create a different recovery UUID.
    let mut lock = f.admin.begin().await.unwrap();
    sqlx::query("LOCK query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let start = Instant::now();
    let eof = in_process(state.clone(), &t, query.clone()).await;
    let eof_id = body_id(&eof);
    assert!(start.elapsed() < Duration::from_secs(2));
    let bytes = axum::body::to_bytes(eof.into_body(), 1_000_000)
        .await
        .unwrap();
    assert!(!bytes.is_empty());
    let abandoned = in_process(state.clone(), &t, query.clone()).await;
    let abandoned_id = body_id(&abandoned);
    let start = Instant::now();
    drop(abandoned);
    assert!(start.elapsed() < Duration::from_millis(100));
    lock.rollback().await.unwrap();
    assert_eq!(row(&f.admin, eof_id).await["outcome"], "success");
    assert_eq!(row(&f.admin, abandoned_id).await["outcome"], "disconnected");
    // Configured transparent fallback and explicit distributed NDJSON remain
    // deferred; local is deliberately independent of configured peers.
    let coordinator_state = state
        .clone()
        .with_coordinator(vec!["http://127.0.0.1:1".into()], None);
    let coordinator = server(coordinator_state.clone()).await;
    for route in ["/api/v1/sql", "/api/v1/sql/distributed"] {
        unobserved(
            &coordinator,
            &t,
            &f.admin,
            route,
            json!({"query":"SELECT 1", "format":"ndjson"}),
            StatusCode::OK,
        )
        .await;
    }
    let local = in_process(coordinator_state, &t, query.clone()).await;
    let local_id = body_id(&local);
    let _ = axum::body::to_bytes(local.into_body(), 1_000_000)
        .await
        .unwrap();
    assert_eq!(row(&f.admin, local_id).await["outcome"], "success");
    let mut default_state = state.clone();
    default_state.tenants = None;
    let default = server(default_state).await;
    unobserved(
        &default,
        &t,
        &f.admin,
        "/api/v1/sql",
        json!({"query":"SELECT 1", "format":"ndjson"}),
        StatusCode::OK,
    )
    .await;
    let explicit = server(state.clone()).await;
    unobserved(
        &explicit,
        &t,
        &f.admin,
        "/api/v1/sql/distributed",
        json!({"query":"SELECT 1", "format":"ndjson"}),
        StatusCode::BAD_REQUEST,
    )
    .await;
    // No receipt: no fabricated header, no interference with source results.
    sqlx::query("REVOKE CONNECT ON DATABASE qh_fixture FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename='qh_writer' AND pid<>pg_backend_pid()").execute(&f.admin).await.unwrap();
    let response = in_process(state.clone(), &t, query.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(CORRELATION));
    assert!(!axum::body::to_bytes(response.into_body(), 1_000_000)
        .await
        .unwrap()
        .is_empty());
    sqlx::query("GRANT CONNECT ON DATABASE qh_fixture TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    // Close the sole task while the actual body still owns its observation.
    // Late drop cannot fabricate durable success or keep the service alive.
    let response = in_process(state.clone(), &t, query).await;
    let shutdown_id = body_id(&response);
    not_terminal(&f.admin, shutdown_id).await;
    let _ = service.shutdown().await;
    let losses = service
        .health()
        .loss_signals(siglake_query_server::history::LossReason::Shutdown);
    drop(response);
    assert!(
        service
            .health()
            .loss_signals(siglake_query_server::history::LossReason::Shutdown)
            > losses
    );
    not_terminal(&f.admin, shutdown_id).await;
    let safe: bool = sqlx::query_scalar("SELECT bool_and(redacted_sql IS NULL AND expires_at-received_at=interval '720 hours' AND admitted_at IS NULL AND finished_at IS NULL) FROM query_history.executions").fetch_one(&f.admin).await.unwrap();
    assert!(safe);
    let serialized: String =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(e))::text FROM query_history.executions e")
            .fetch_one(&f.admin)
            .await
            .unwrap();
    for secret in [
        "PRIVATE_STREAM_RESULT",
        "PRIVATE_SUBJECT",
        "forged-client",
        "forged-tenant",
        "SELECT",
        &t,
        &renewed,
    ] {
        assert!(!serialized.contains(secret), "history privacy invariant");
    }
    issuer_task.abort();
}
