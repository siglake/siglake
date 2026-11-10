//! Signed local HTTP + owned primary. Direct shards only, not controlled fanout.
use super::*;
use arrow::array::StringArray;
const ROUTE: &str = "/api/v1/sql/shard";
fn metadata(
    r: &reqwest::Response,
    state: &str,
    revision: &str,
    ms: u64,
    warning: bool,
    priority: &str,
) {
    let h = r.headers();
    assert_eq!(h.get_all("x-siglake-query-controls").iter().count(), 1);
    assert_eq!(h.get_all("x-siglake-query-warnings").iter().count(), 1);
    let c = h["x-siglake-query-controls"].to_str().unwrap();
    let w = h["x-siglake-query-warnings"].to_str().unwrap();
    assert_eq!(c, format!("v=1;state={state};rev={revision};gen=41;priority={priority};mode=request;scope=request;mandatory=1;timeout_ms={ms};output_rows=na;output_enforced=0;scan_warning={};estimate=unavailable;exact=na;estimated_bytes=na", if warning {"1"} else {"na"}));
    assert_eq!(
        w,
        if warning {
            "scan_estimate_unavailable,scan_threshold_not_evaluated"
        } else {
            "scan_estimate_unavailable"
        }
    );
    assert!(c.is_ascii() && c.len() <= 1024 && w.is_ascii() && w.len() <= 512);
    assert!(
        c.len() + w.len() + "x-siglake-query-controls: \r\nx-siglake-query-warnings: \r\n".len()
            <= 2048
    );
    for secret in [
        "acme",
        "shard-client",
        "forged",
        "http://",
        "SELECT",
        "\r",
        "\n",
    ] {
        assert!(!c.contains(secret) && !w.contains(secret));
    }
}
fn scan(r: &reqwest::Response) -> Vec<u8> {
    let h = r.headers();
    assert_eq!(h["content-type"], "application/vnd.apache.arrow.stream");
    assert_eq!(h.get_all("x-siglake-scan").iter().count(), 1);
    let bytes = h["x-siglake-scan"].as_bytes().to_vec();
    assert!(bytes.len() <= siglake_query_server::coordinator::SHARD_SCAN_HEADER_MAX_BYTES);
    let _: siglake_query_server::format::ScanDetail = serde_json::from_slice(&bytes).unwrap();
    bytes
}
fn native_timeout(body: &Value, seconds: u64) {
    assert_eq!(
        *body,
        json!({"error":format!("shard exceeded its wall-clock budget of {seconds}s"), "code":504})
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed primary fixture; check-shard-policy.sh"]
async fn signed_direct_shard_fresh_primary_arrow_bytes_schema_values_scan_and_safe_headers() {
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, task) = spawn_fake_idp().await;
    let idp = Server {
        base: issuer.clone(),
        task,
        state: None,
    };
    let jwt = token(&issuer, "acme", "shard-client");
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    ice.for_namespace("tenant_acme")
        .await
        .unwrap()
        .append_events(&[siglake_core::Event::now("direct-shard-value")])
        .await
        .unwrap();
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(3);
    limits.interactive.ceiling_timeout = Duration::from_secs(3);
    limits.batch.default_timeout = Duration::from_secs(3);
    limits.batch.ceiling_timeout = Duration::from_secs(3);
    let base = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice.clone()))
        .with_allowed_tenants(["acme"])
        .with_limits(limits)
        .with_coordinator(vec![], Some("synthetic-coordinator".into()));
    let disabled = serve(base.clone()).await;
    let enabled_state = base
        .clone()
        .with_query_controls(reader.clone(), 41)
        .unwrap();
    let enabled = serve(enabled_state.clone()).await;
    let query = json!({"query":"SELECT raw FROM events", "tenant":"foreign", "controls":{"revision":999,"timeout_ms":999999}, "issuer":"forged", "client_id":"forged"});
    // Warm only the data/metadata caches, never policy. Compare scan attribution
    // independently of added headers, not as an estimate or threshold verdict.
    post(&disabled.base, &jwt, ROUTE, query.clone())
        .await
        .bytes()
        .await
        .unwrap();
    let r = post(&disabled.base, &jwt, ROUTE, query.clone()).await;
    assert_eq!(r.status(), StatusCode::OK);
    assert!(!r.headers().contains_key("x-siglake-query-controls"));
    let native_scan = scan(&r);
    assert!(
        serde_json::from_slice::<Value>(&native_scan).unwrap()["planned_bytes"]
            .as_u64()
            .unwrap()
            > 1
    );
    let native_bytes = r.bytes().await.unwrap();
    let decoded = siglake_query_server::format::arrow_ipc_to_batches(&native_bytes).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].schema().field(0).name(), "raw");
    assert_eq!(
        decoded[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "direct-shard-value"
    );
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "shard-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    // Entire admission controller is occupied. A shard must not reserve again,
    // including a direct externally authenticated call (self-shard deadlock).
    let held = enabled_state
        .admission
        .acquire(enabled_state.limits.admission_budget_bytes)
        .await
        .unwrap();
    for (state, revision, settings) in [
        ("inherited", 0, None),
        (
            "inherited",
            1,
            Some(SavedSettings::new(None, None).unwrap()),
        ),
        (
            "applied",
            2,
            Some(SavedSettings::new(Some(1), Some(1)).unwrap()),
        ),
        (
            "inherited",
            3,
            Some(SavedSettings::new(None, None).unwrap()),
        ),
    ] {
        if let Some(settings) = settings {
            manager
                .save(
                    &binding,
                    &actor,
                    SaveRequest::new(revision - 1, settings).unwrap(),
                )
                .await
                .unwrap();
        }
        for priority in ["interactive", "batch"] {
            let mut body = query.clone();
            body["priority"] = json!(priority);
            let r = post(&enabled.base, &jwt, ROUTE, body).await;
            assert_eq!(r.status(), StatusCode::OK);
            metadata(
                &r,
                state,
                &if revision == 0 {
                    "na".into()
                } else {
                    revision.to_string()
                },
                if state == "applied" { 1000 } else { 3000 },
                state == "applied",
                priority,
            );
            assert_eq!(
                scan(&r),
                native_scan,
                "scan wire changed independently of controls"
            );
            let bytes = r.bytes().await.unwrap();
            assert_eq!(bytes, native_bytes, "IPC bytes/batching changed");
            assert_eq!(
                siglake_query_server::format::arrow_ipc_to_batches(&bytes).unwrap(),
                decoded
            );
        }
        if state == "applied" {
            let mut lock = admin.begin().await.unwrap();
            sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *lock)
                .await
                .unwrap();
            let started = std::time::Instant::now();
            let (r, ()) = tokio::join!(
                post(
                    &enabled.base,
                    &jwt,
                    ROUTE,
                    json!({"query":"invalid SQL before any catalog work"})
                ),
                async {
                    tokio::time::sleep(Duration::from_millis(1100)).await;
                    lock.commit().await.unwrap();
                }
            );
            assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
            metadata(&r, "applied", "2", 1000, true, "interactive");
            native_timeout(&r.json().await.unwrap(), 1);
            assert!(started.elapsed() < Duration::from_secs(3));
        }
    }
    drop(held);
    // Existing native SQL/pin errors and empty IPC results stay byte exact.
    for body in [
        json!({"query":"not valid SQL"}),
        json!({"query":"SELECT raw FROM events WHERE false"}),
        json!({"query":"SELECT raw FROM events", "pin":{"table":"events","snapshot_id":null,"empty":false}}),
    ] {
        let old = post(&disabled.base, &jwt, ROUTE, body.clone()).await;
        let new = post(&enabled.base, &jwt, ROUTE, body).await;
        assert_eq!(old.status(), new.status());
        metadata(&new, "inherited", "3", 3000, false, "interactive");
        assert_eq!(old.bytes().await.unwrap(), new.bytes().await.unwrap());
    }
    let other = token(&issuer, "acme", "other-client");
    let r = post(&enabled.base, &other, ROUTE, query.clone()).await;
    metadata(&r, "inherited", "na", 3000, false, "interactive");
    assert_eq!(r.bytes().await.unwrap(), native_bytes);
    // Coordinator token remains its own trust boundary. Controlled forwarding
    // is unavailable in 4A, before even invalid forwarded tenancy/catalog work.
    for tenant in ["acme", "foreign", "bad.tenant"] {
        let r = post(
            &enabled.base,
            "synthetic-coordinator",
            ROUTE,
            json!({"query":"SELECT 1", "tenant":tenant, "controls":{"v":2}}),
        )
        .await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!r.headers().contains_key("x-siglake-query-controls"));
        assert_eq!(
            r.json::<Value>().await.unwrap()["error"],
            "query_controls_path_unavailable"
        );
    }
    let r = post(
        &disabled.base,
        "synthetic-coordinator",
        ROUTE,
        json!({"query":"SELECT raw FROM events", "tenant":"acme"}),
    )
    .await;
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.bytes().await.unwrap(), native_bytes);
    // Store failure must precede query/catalog work, never inheritance; auth
    // refusals continue to win before that failed store is considered.
    reader.close();
    for jwt in [&jwt, &other] {
        let r = post(&enabled.base, jwt, ROUTE, json!({"query":"SELECT 1"})).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!r.headers().contains_key("x-siglake-query-controls"));
        assert_eq!(
            r.json::<Value>().await.unwrap()["error"],
            "query_policy_unavailable"
        );
    }
    for (jwt, expected) in [
        (
            token(&issuer, "foreign", "shard-client"),
            StatusCode::FORBIDDEN,
        ),
        (
            token("https://wrong-issuer.invalid", "acme", "shard-client"),
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let r = post(&enabled.base, &jwt, ROUTE, query.clone()).await;
        assert_eq!(r.status(), expected);
        assert!(!r.headers().contains_key("x-siglake-query-controls"));
    }
    enabled.shutdown().await;
    disabled.shutdown().await;
    idp.shutdown().await;
    manager.close();
    admin.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed primary fixture; check-shard-policy.sh"]
async fn signed_direct_shard_native_timeout_schema_initial_and_resolved_original_budget() {
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, task) = spawn_fake_idp().await;
    let idp = Server {
        base: issuer.clone(),
        task,
        state: None,
    };
    let jwt = token(&issuer, "acme", "shard-timeout-client");
    let verifier = OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_millis(1);
    limits.interactive.ceiling_timeout = Duration::from_millis(1);
    let base = AppState::new(ice.clone(), AuthConfig::from_oidc(verifier))
        .with_tenants(TenantRegistry::new(ice))
        .with_limits(limits);
    let disabled = serve(base.clone()).await;
    let enabled = serve(
        base.clone()
            .with_query_controls(reader.clone(), 41)
            .unwrap(),
    )
    .await;
    // The existing shard wire has no caller limits. Forged limits/controls do
    // not create forwarded authority or disable its unconditional native time.
    let slow = json!({"query":"SELECT sum(v) FROM generate_series(1, 100000000) AS t(v)", "limits":{"timeout_seconds":999999,"circuit_breakers":false}, "controls":{"timeout_ms":999999}});
    let r = post(&disabled.base, &jwt, ROUTE, slow.clone()).await;
    assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
    let native: Value = r.json().await.unwrap();
    native_timeout(&native, 0);
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "shard-timeout-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    let actor = VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap();
    for revision in [0, 1] {
        if revision == 1 {
            manager
                .save(
                    &binding,
                    &actor,
                    SaveRequest::new(0, SavedSettings::new(None, None).unwrap()).unwrap(),
                )
                .await
                .unwrap();
        }
        let r = post(&enabled.base, &jwt, ROUTE, slow.clone()).await;
        assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
        // Resolution can itself spend the tiny initial budget; only a proven
        // resolved snapshot is allowed to add effective headers.
        if r.headers().contains_key("x-siglake-query-controls") {
            metadata(
                &r,
                "inherited",
                if revision == 0 { "na" } else { "1" },
                1,
                false,
                "interactive",
            );
        }
        assert_eq!(r.json::<Value>().await.unwrap(), native);
    }
    let mut lock = admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let r = post(&enabled.base, &jwt, ROUTE, json!({"query":"SELECT 1"})).await;
    assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(!r.headers().contains_key("x-siglake-query-controls"));
    assert_eq!(r.json::<Value>().await.unwrap(), native);
    lock.rollback().await.unwrap();
    enabled.shutdown().await;
    disabled.shutdown().await;
    idp.shutdown().await;
    reader.close();
    manager.close();
    admin.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed primary fixture; check-shard-policy.sh"]
async fn direct_shard_open_static_and_signed_human_id_preflight_without_agent_attribution() {
    let (socket, admin) = fixture().await;
    let manager = PolicyManager::connect(policy_config(&socket, false))
        .await
        .unwrap();
    let reader = Arc::new(
        PolicyReader::connect(policy_config(&socket, true))
            .await
            .unwrap(),
    );
    let (issuer, task) = spawn_fake_idp().await;
    let idp = Server {
        base: issuer.clone(),
        task,
        state: None,
    };
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "human-same-client").unwrap(),
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
    let verifier = OidcVerifier::from_cognito_mixed_issuer(
        issuer.clone(),
        "human-api".into(),
        "custom:org_id".into(),
    )
    .await
    .unwrap();
    let mut claims = access_claims(&issuer);
    claims["token_use"] = json!("id");
    claims["aud"] = json!("human-api");
    claims["custom:org_id"] = json!("acme");
    claims["client_id"] = json!("human-same-client");
    claims["sub"] = json!("human-same-client");
    let human = sign_claims(&claims, Algorithm::RS256, Some(TEST_KID));
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_secs(3);
    limits.interactive.ceiling_timeout = Duration::from_secs(3);
    for (auth, jwt) in [
        (AuthConfig::open(), "".to_string()),
        (
            AuthConfig::from_tokens(["shard-static"]),
            "shard-static".into(),
        ),
        (AuthConfig::from_oidc(verifier), human),
    ] {
        let base = AppState::new(ice.clone(), auth)
            .with_tenants(TenantRegistry::new(ice.clone()))
            .with_limits(limits.clone());
        let enabled = serve(base.with_query_controls(reader.clone(), 41).unwrap()).await;
        let r = post(&enabled.base,&jwt,ROUTE,json!({"query":"SELECT 1", "tenant":"acme", "provider_principal":{"issuer":issuer,"client_id":"human-same-client"}})).await;
        assert_eq!(r.status(), StatusCode::OK);
        metadata(&r, "inherited", "na", 3000, false, "interactive");
        r.bytes().await.unwrap();
        // Unsafe checkout fails the no-agent preflight, too; do not inherit a
        // fabricated successful agent lookup or reuse a healthy preflight.
        let mut tx = admin.begin().await.unwrap();
        sqlx::query("GRANT SELECT ON app_catalog.secrets TO qp_reader")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let r = post(&enabled.base, &jwt, ROUTE, json!({"query":"SELECT 1"})).await;
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!r.headers().contains_key("x-siglake-query-controls"));
        assert_eq!(
            r.json::<Value>().await.unwrap()["error"],
            "query_policy_unavailable"
        );
        sqlx::query("REVOKE SELECT ON app_catalog.secrets FROM qp_reader")
            .execute(&admin)
            .await
            .unwrap();
        enabled.shutdown().await;
    }
    idp.shutdown().await;
    reader.close();
    manager.close();
    admin.close().await;
}
