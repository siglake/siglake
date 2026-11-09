//! Source-level runtime surface: signed synthetic HTTP, owned primary, actual STDIO.
use super::buffered_controls::{fixture, policy_config, post, serve, token, Server};
use super::spawn_fake_idp;
use axum::http::StatusCode;
use serde_json::{json, Value};
use siglake_query_server::{
    query_controls::*, AppState, AuthConfig, OidcVerifier, ServerLimits, TenantRegistry,
};
use siglake_storage::iceberg::IcebergContext;
use std::{path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

async fn effective(base: &str, token: &str, suffix: &str) -> (StatusCode, Value) {
    let response = reqwest::Client::new()
        .get(format!("{base}/api/v1/query-controls/effective{suffix}"))
        .bearer_auth(token)
        .header("x-client-id", "forged")
        .header("x-scope-orgid", "foreign")
        .send()
        .await
        .unwrap();
    let status = response.status();
    if status.is_success() || status == StatusCode::SERVICE_UNAVAILABLE {
        assert_eq!(response.headers()["cache-control"], "private, no-store");
    }
    let bytes = response.bytes().await.unwrap();
    assert!(bytes.len() <= 4096);
    let text = std::str::from_utf8(&bytes).unwrap();
    for secret in [
        "controlled-client",
        "forged",
        "foreign",
        "custom:org_id",
        "synthetic-human",
        "Bearer",
        "SELECT",
    ] {
        assert!(!text.contains(secret));
    }
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned synthetic primary/JWT/STDIO fixture; check-runtime-surface.sh"]
async fn caller_effective_formats_primary_readiness_and_actual_stdio() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
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
    let credential = token(&issuer, "acme", "controlled-client");
    let absent = token(&issuer, "acme", "absent-client");
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(3);
    limits.interactive.ceiling_timeout = Duration::from_secs(4);
    limits.batch.default_timeout = Duration::from_secs(5);
    limits.batch.ceiling_timeout = Duration::from_secs(6);
    let baseline = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_allowed_tenants(["acme"])
        .with_limits(limits)
        .with_coordinator(vec![], Some("synthetic-coordinator".into()));
    let registry = baseline.tenants.clone().unwrap();
    let enabled = baseline
        .clone()
        .with_query_controls(reader.clone(), 9_007_199_254_740_993)
        .unwrap();
    let s = serve(enabled).await;
    let before = registry.cached_tenant_count().await;
    let (status, body) = effective(&s.base, &absent, "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["availability"], "enabled");
    assert_eq!(body["scope"], "local_node_default_requests");
    assert_eq!(body["interactive"]["state"], "inherited");
    assert!(body["interactive"]["policy_revision"].is_null());
    assert_eq!(body["interactive"]["default_request_timeout_ms"], 3000);
    assert_eq!(body["batch"]["default_request_timeout_ms"], 5000);
    assert_eq!(before, registry.cached_tenant_count().await);
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
            SaveRequest::new(0, SavedSettings::new(None, None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let (_, body) = effective(&s.base, &credential, "").await;
    for priority in ["interactive", "batch"] {
        assert_eq!(body[priority]["state"], "inherited");
        assert_eq!(body[priority]["policy_revision"], 1);
    }
    manager
        .save(
            &binding,
            &actor,
            SaveRequest::new(1, SavedSettings::new(Some(2), Some(1)).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let (_, body) = effective(&s.base, &credential, "").await;
    for priority in ["interactive", "batch"] {
        assert_eq!(body[priority]["state"], "applied");
        assert_eq!(body[priority]["policy_revision"], 2);
        assert_eq!(body[priority]["default_request_timeout_ms"], 2000);
    }
    for suffix in [
        "?principal=x",
        "?org=x",
        "?credential=x",
        "?timeout_seconds=0",
        "?",
    ] {
        assert_eq!(
            effective(&s.base, &credential, suffix).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        effective(&s.base, &token(&issuer, "foreign", "controlled-client"), "")
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        effective(&s.base, "synthetic-coordinator", "").await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        effective(&s.base, "invalid", "").await.0,
        StatusCode::UNAUTHORIZED
    );
    for (route, extra) in [
        ("/api/v1/sql", json!({})),
        ("/api/v1/sql/local", json!({})),
        ("/api/v1/sql/local", json!({"dry_run":true})),
        ("/api/v1/sql/explain", json!({})),
    ] {
        let mut query = json!({"query":"SELECT 1 AS n", "default_order":false, "limits":{"circuit_breakers":false}});
        query
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let response = post(&s.base, &credential, route, query).await;
        assert_eq!(response.status(), StatusCode::OK);
        let h = response.headers()["x-siglake-query-controls"].clone();
        assert!(h.as_bytes().is_ascii() && h.as_bytes().len() <= 1024);
        let body: Value = response.json().await.unwrap();
        let c = &body["controls"];
        assert_eq!(c["policy_revision"], 2);
        assert_eq!(c["effective_timeout_ms"], 2000);
        assert_eq!(c["estimate_status"], "complete");
        assert_eq!(c["estimated_bytes"], 0);
        assert!(c["warnings"]
            .as_array()
            .unwrap()
            .contains(&json!("timeout_opt_out_ignored")));
        assert!(!c["warnings"]
            .as_array()
            .unwrap()
            .contains(&json!("scan_warning_exceeded")));
        if route.ends_with("explain") || body.get("rows").is_none() {
            assert_eq!(c["mode"], "planning");
        }
    }
    for route in ["/api/v1/sql", "/api/v1/sql/local"] {
        let response = post(
            &s.base,
            &credential,
            route,
            json!({"query":"SELECT 1 AS n", "format":"ndjson", "default_order":false}),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let text = response.text().await.unwrap();
        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines[0]["_meta"], "controls");
        assert_eq!(lines[0]["controls"]["policy_revision"], 2);
        assert!(text.lines().next().unwrap().len() <= 4096);
        assert_eq!(lines[1]["n"], 1);
        assert_eq!(lines.last().unwrap()["_meta"], "complete");
    }
    // Same tenant/result controls, distinct verified binding: a shared payload
    // hit still carries the NEW caller's freshly read primary snapshot.
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("cache-caller-row")])
        .await
        .unwrap();
    siglake_query_server::reset_result_cache_for_test().await;
    let cache_query = json!({"query":"SELECT raw FROM events WHERE host = 'localhost' LIMIT 10", "default_order":false});
    let response = post(
        &s.base,
        &credential,
        "/api/v1/sql/local",
        cache_query.clone(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let first: Value = response.json().await.unwrap();
    let next_binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "new-cache-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    manager
        .save(
            &next_binding,
            &actor,
            SaveRequest::new(0, SavedSettings::new(None, None).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    manager
        .save(
            &next_binding,
            &actor,
            SaveRequest::new(1, SavedSettings::new(Some(2), Some(1)).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let next_token = token(&issuer, "acme", "new-cache-client");
    let response = post(&s.base, &next_token, "/api/v1/sql/local", cache_query).await;
    assert_eq!(response.status(), StatusCode::OK);
    let second: Value = response.json().await.unwrap();
    assert_eq!(first["rows"], second["rows"]);
    assert_eq!(second["controls"]["policy_revision"], 2);
    assert_eq!(second["controls"]["effective_timeout_ms"], 2000);
    assert!(
        metrics
            .snapshot()
            .into_vec()
            .into_iter()
            .any(|(key, _, _, value)| {
                key.key().name() == "siglake_query_sql_result_cache_requests_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "outcome" && label.value() == "hit")
                    && matches!(value, metrics_util::debugging::DebugValue::Counter(n) if n > 0)
            }),
        "expected independently observed fresh-caller cache hit"
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let python =
        std::env::var("SIGLAKE_SYNTHETIC_MCP_PYTHON").expect("explicit cached MCP Python required");
    let output = tokio::process::Command::new(python)
        .arg(root.join("integrations/mcp/tests/engine_round_trip.py"))
        .env("PYTHONPATH", root.join("integrations/mcp/src"))
        .env("SIGLAKE_SYNTHETIC_ENGINE", &s.base)
        .env("SIGLAKE_SYNTHETIC_TOKEN", &credential)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "STDIO proof: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
    sqlx::query("GRANT UPDATE ON query_policy.agent_limits TO qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    let (status, body) = effective(&s.base, &credential, "").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["availability"], "unavailable");
    assert!(body["interactive"].is_null());
    let ready = reqwest::get(format!("{}/readyz", s.base)).await.unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        ready.json::<Value>().await.unwrap()["error"],
        "query_policy_unavailable"
    );
    sqlx::query("REVOKE UPDATE ON query_policy.agent_limits FROM qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(effective(&s.base, &credential, "").await.0, StatusCode::OK);
    let disabled = serve(baseline).await;
    assert_eq!(
        effective(&disabled.base, &credential, "").await.1["availability"],
        "disabled"
    );
    let generic = serve(
        AppState::new(ice, AuthConfig::from_tokens(["static"]))
            .with_query_controls(reader.clone(), 8)
            .unwrap(),
    )
    .await;
    let (_, body) = effective(&generic.base, "static", "").await;
    assert_eq!(body["interactive"]["state"], "inherited");
    assert!(body["interactive"]["policy_revision"].is_null());
    reader.close();
    assert_eq!(
        effective(&s.base, &credential, "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        effective(&generic.base, "static", "").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    for server in [s, disabled, generic, issuer_owner] {
        server.shutdown().await;
    }
    manager.close();
    admin.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned primary and explicit built binary; check-runtime-surface.sh"]
async fn optional_binary_startup_valid_and_fail_closed_secret_canaries() {
    let (socket, admin) = fixture().await;
    let binary = std::env::var("SIGLAKE_SYNTHETIC_QUERY_BINARY").expect("explicit built binary");
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("must-not-exist");
    let encoded = socket.to_str().unwrap().replace('/', "%2F");
    let dsn = format!("postgres://qp_reader:@{encoded}:5432/qh_fixture?sslmode=disable");
    let base = [
        "--audit-disabled",
        "--query-controls-operation-timeout-ms",
        "2000",
        "--query-controls-max-connections",
        "2",
        "--query-controls-operator-generation",
        "1",
    ];
    for (uri, extra) in [
        (
            "postgres://qp_reader:DSN_SECRET_CANARY@invalid".to_owned(),
            vec![],
        ),
        (dsn.replace("qp_reader", "qp_manager"), vec![]),
        (
            dsn.clone(),
            vec!["--query-controls-interactive-default-seconds", "0"],
        ),
        (
            dsn.clone(),
            vec!["--query-controls-interactive-ceiling-seconds", "1"],
        ),
        (dsn.clone(), vec!["--oidc-profile", "cognito-mixed"]),
        (dsn.clone(), vec!["--query-peers", "http://127.0.0.1:1"]),
        (
            dsn.clone(),
            vec![
                "--query-peers",
                "http://PEER_SECRET_CANARY@127.0.0.1:1",
                "--query-controls-clock-skew-ms",
                "20",
                "--query-coordinator-token",
                "synthetic",
            ],
        ),
    ] {
        let output = tokio::process::Command::new(&binary)
            .args(base)
            .args(extra)
            .arg("--query-controls-database-url")
            .arg(uri)
            .arg("--data-dir")
            .arg(&data)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!text.contains("SECRET_CANARY"));
        assert!(
            !data.exists(),
            "invalid static config performed unrelated catalog work"
        );
    }
    let help = tokio::process::Command::new(&binary)
        .arg("--help")
        .env(
            "SIGLAKE_QUERY_CONTROLS_DATABASE_URL",
            "postgres://qp_reader:DSN_SECRET_CANARY@invalid",
        )
        .output()
        .await
        .unwrap();
    assert!(help.status.success());
    assert!(!String::from_utf8_lossy(&help.stdout).contains("DSN_SECRET_CANARY"));
    assert!(!String::from_utf8_lossy(&help.stderr).contains("DSN_SECRET_CANARY"));
    sqlx::query("GRANT UPDATE ON query_policy.agent_limits TO qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    let output = tokio::process::Command::new(&binary)
        .args(base)
        .arg("--query-controls-database-url")
        .arg(&dsn)
        .arg("--data-dir")
        .arg(&data)
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(!data.exists());
    sqlx::query("REVOKE UPDATE ON query_policy.agent_limits FROM qp_reader")
        .execute(&admin)
        .await
        .unwrap();
    // Owned fixture-only schema fault, not an application migration. Startup
    // must fail before creating the unrelated catalog/data directory.
    sqlx::query("ALTER SCHEMA query_policy RENAME TO hidden_query_policy")
        .execute(&admin)
        .await
        .unwrap();
    let output = tokio::process::Command::new(&binary)
        .args(base)
        .arg("--query-controls-database-url")
        .arg(&dsn)
        .arg("--data-dir")
        .arg(&data)
        .output()
        .await
        .unwrap();
    sqlx::query("ALTER SCHEMA hidden_query_policy RENAME TO query_policy")
        .execute(&admin)
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(!data.exists());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = listener.local_addr().unwrap();
    drop(listener);
    let mut child = tokio::process::Command::new(&binary)
        .args(base)
        .arg("--query-controls-database-url")
        .arg(&dsn)
        .arg("--data-dir")
        .arg(tmp.path().join("positive"))
        .arg("--bind")
        .arg(bind.to_string())
        .args(["--metrics-bind", "127.0.0.1:0", "--tokens", "static"])
        .env("SIGLAKE_QUERY_PREWARM", "0")
        .env("SIGLAKE_QUERY_WARM_INTERVAL_SECS", "0")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let base_url = format!("http://{bind}");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "positive configured binary exited"
            );
            if reqwest::get(format!("{base_url}/healthz")).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let (status, body) = effective(&base_url, "static", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["availability"], "enabled");
    assert_eq!(body["interactive"]["default_request_timeout_ms"], 60_000);
    assert_eq!(body["batch"]["default_request_timeout_ms"], 3_600_000);
    assert!(
        tokio::process::Command::new("/run/current-system/sw/bin/kill")
            .arg("-TERM")
            .arg(child.id().unwrap().to_string())
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    admin.close().await;
}
