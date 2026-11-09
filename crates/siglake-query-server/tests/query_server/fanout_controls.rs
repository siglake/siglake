//! Two owned loopback engine routers, owned primary, signed synthetic caller.
//! Same-host transport proof only; clock theorem tests are separate pure math.
use super::*;
use axum::{
    body::Body,
    extract::Request,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use std::sync::Mutex;
const V2: &str = "/api/v2/internal/sql/shard";
pub(in super::super) const SQL: &str =
    "SELECT host, count(*) AS n FROM events WHERE raw LIKE '%wire%' GROUP BY host";
#[derive(Clone, Copy)]
pub(in super::super) enum Mode {
    Normal,
    Refuse(u16),
    Transit(u64),
    StallBody,
}
pub(in super::super) struct Trace {
    pub(in super::super) mode: Mutex<Mode>,
    pub(in super::super) requests: Mutex<Vec<Value>>,
    pub(in super::super) completed: Mutex<Vec<(u16, String)>>,
    pub(in super::super) stalled_sources: Arc<AtomicUsize>,
}
impl Default for Trace {
    fn default() -> Self {
        Self {
            mode: Mutex::new(Mode::Normal),
            requests: Mutex::new(vec![]),
            completed: Mutex::new(vec![]),
            stalled_sources: Arc::new(AtomicUsize::new(0)),
        }
    }
}
struct PendingSource(Arc<AtomicUsize>);
impl Drop for PendingSource {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
async fn trace(trace: Arc<Trace>, request: Request, next: Next) -> Response {
    if request.uri().path() != V2 {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = axum::body::to_bytes(body, 32 * 1024).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    trace.requests.lock().unwrap().push(value);
    let mode = *trace.mode.lock().unwrap();
    if let Mode::Refuse(status) = mode {
        return StatusCode::from_u16(status).unwrap().into_response();
    }
    if let Mode::Transit(ms) = mode {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    let mut response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    trace.completed.lock().unwrap().push((
        response.status().as_u16(),
        response
            .headers()
            .get("x-siglake-query-controls")
            .map(|h| h.to_str().unwrap().to_owned())
            .unwrap_or_default(),
    ));
    if matches!(mode, Mode::StallBody) && response.status().is_success() {
        trace.stalled_sources.fetch_add(1, Ordering::SeqCst);
        let guard = PendingSource(trace.stalled_sources.clone());
        *response.body_mut() = Body::from_stream(async_stream::stream! {
            let _guard=guard;
            std::future::pending::<()>().await;
            yield Ok::<_,std::io::Error>(bytes::Bytes::new());
        });
    }
    response
}
pub(in super::super) async fn engine(
    listener: tokio::net::TcpListener,
    state: AppState,
    trace_state: Arc<Trace>,
) -> Server {
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = router(state.clone()).layer(middleware::from_fn(move |r, n| {
        trace(trace_state.clone(), r, n)
    }));
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server {
        base,
        task,
        state: Some(state),
    }
}
pub(in super::super) fn limits(ms: u64) -> ServerLimits {
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_millis(ms);
    limits.interactive.ceiling_timeout = Duration::from_millis(ms);
    limits.batch.default_timeout = Duration::from_millis(ms);
    limits.batch.ceiling_timeout = Duration::from_millis(ms);
    limits.admission_budget_bytes = 1; // Coordinator occupies ALL; self-shard cannot reserve.
    limits.admission_wait_timeout = Duration::from_millis(100);
    limits
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned signed two-node primary fixture; check-fanout-policy.sh"]
async fn signed_two_node_wire_fanout_clamps_refusals_retry_and_http_pump_deadline() {
    let (socket, admin) = fixture().await;
    let reader_a = Arc::new(
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
    let jwt = token(&issuer, "acme", "wire-client");
    let binding = ManagementBinding::from_trusted_metadata(
        RuntimeKey::from_trusted_metadata("acme", &issuer, "wire-client").unwrap(),
        Uuid::new_v4(),
        Uuid::new_v4(),
    )
    .unwrap();
    manager
        .save(
            &binding,
            &VerifiedHumanActor::from_trusted_metadata("synthetic-human").unwrap(),
            SaveRequest::new(0, SavedSettings::new(Some(2), Some(1)).unwrap()).unwrap(),
        )
        .await
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let ice_a = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    for _ in 0..3 {
        ice_a
            .for_namespace("tenant_acme")
            .await
            .unwrap()
            .append_events(&[siglake_core::Event::now("wire-row")])
            .await
            .unwrap();
    }
    let ice_b = Arc::new(IcebergContext::open(tmp.path()).await.unwrap());
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url_a = format!("http://{}", listener_a.local_addr().unwrap());
    let url_b = format!("http://{}", listener_b.local_addr().unwrap());
    let config = DistributedControlsConfig::new(Duration::from_millis(20)).unwrap();
    let auth = AuthConfig::from_oidc(
        OidcVerifier::from_cognito_access_issuer(issuer.clone(), "custom:org_id".into())
            .await
            .unwrap(),
    );
    let state_a = AppState::new(ice_a.clone(), auth.clone())
        .with_tenants(TenantRegistry::new(ice_a))
        .with_allowed_tenants(["acme"])
        .with_limits(limits(3000))
        .with_coordinator(vec![url_a, url_b], Some("synthetic-coordinator".into()))
        .with_distributed_query_controls(reader_a.clone(), 71, config)
        .unwrap();
    let state_b = AppState::new(ice_b.clone(), auth)
        .with_tenants(TenantRegistry::new(ice_b))
        .with_allowed_tenants(["acme"])
        .with_limits(limits(900))
        .with_coordinator(vec![], Some("synthetic-coordinator".into()))
        .with_distributed_query_controls(reader_b.clone(), 72, config)
        .unwrap();
    let ta = Arc::new(Trace::default());
    let tb = Arc::new(Trace::default());
    let a = engine(listener_a, state_a.clone(), ta.clone()).await;
    let b = engine(listener_b, state_b.clone(), tb.clone()).await;
    for path in ["/api/v1/sql", "/api/v1/sql/distributed"] {
        let response = post(
            &a.base,
            &jwt,
            path,
            json!({"query":SQL,"default_order":false,"tenant":"foreign","issuer":"forged","client_id":"forged","controls":{"version":2,"revision":9999,"timeout_ms":999999}}),
        )
        .await;
        assert_eq!(response.status(), 200);
        // Fragment metadata remains worker-local, never the parent snapshot.
        assert!(!response
            .headers()
            .get("x-siglake-query-controls")
            .is_some_and(|h| h.to_str().unwrap().contains("gen=72;")));
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["rows"][0]["n"], 3);
        assert_eq!(
            body["stats"]["phases"]["distributed"]["shard_wall_micros"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    assert!(tb
        .completed
        .lock()
        .unwrap()
        .iter()
        .any(|(s, c)| *s == 200 && c.contains("gen=72;") && c.contains("timeout_ms=900;")));
    let ndjson = post(
        &a.base,
        &jwt,
        "/api/v1/sql/distributed",
        json!({"query":SQL,"format":"ndjson"}),
    )
    .await;
    assert_eq!(ndjson.status(), 200);
    assert_eq!(ndjson.headers()["content-type"], "application/x-ndjson");
    assert!(ndjson.text().await.unwrap().contains("\"n\":3"));
    let captured = tb.requests.lock().unwrap().last().unwrap().clone();
    assert_eq!(captured["controls"]["binding"]["tenant"], "acme");
    assert_eq!(captured["controls"]["binding"]["issuer"], issuer);
    assert_eq!(captured["controls"]["binding"]["client"], "wire-client");
    assert_eq!(captured["controls"]["revision"], 1);
    assert_eq!(captured["controls"]["timeout_ms"], 2000);
    let disabled = serve(
        state_b
            .clone()
            .with_query_controls(reader_b.clone(), 72)
            .unwrap(),
    )
    .await;
    let mut invalid = captured.clone();
    invalid["query"] = json!("not SQL");
    assert_eq!(
        post(&disabled.base, "synthetic-coordinator", V2, invalid)
            .await
            .status(),
        503
    );
    disabled.shutdown().await;
    assert!(captured["controls"]["remaining_ms"].as_i64().unwrap() < 2000);
    // External valid signed caller cannot manufacture forwarded authority.
    let response = post(&b.base, &jwt, V2, captured.clone()).await;
    assert_eq!(response.status(), 403);
    for (field, value, expected) in [
        ("version", json!(1), 422),
        ("schema", json!(2), 503),
        ("remaining_ms", json!(0), 503),
        ("remaining_ms", json!(-1), 503),
        ("original_expires_unix_ms", json!(1), 504),
        ("clock_skew_ms", json!(21), 503),
        ("timeout_ms", json!(9223372036854775808u64), 422),
    ] {
        let mut body = captured.clone();
        body["query"] = json!("not SQL");
        body["controls"][field] = value;
        assert_eq!(
            post(&b.base, "synthetic-coordinator", V2, body)
                .await
                .status()
                .as_u16(),
            expected,
            "{field}"
        );
    }
    let mut reflected = captured.clone();
    reflected["controls"]["binding"]["kind"] = json!("private-token-value");
    let refusal = post(&b.base, "synthetic-coordinator", V2, reflected).await;
    assert_eq!(refusal.status(), 422);
    assert_eq!(
        refusal.json::<Value>().await.unwrap()["error"],
        "controlled shard context invalid"
    );
    let mut missing = captured.clone();
    missing.as_object_mut().unwrap().remove("controls");
    assert_eq!(
        post(&b.base, "synthetic-coordinator", V2, missing)
            .await
            .status(),
        422
    );
    let mut foreign = captured.clone();
    foreign["controls"]["binding"]["tenant"] = json!("foreign");
    assert_eq!(
        post(&b.base, "synthetic-coordinator", V2, foreign)
            .await
            .status(),
        403
    );
    // Real 500 peer answer selects self-shard retry, preserving original expiry.
    ta.requests.lock().unwrap().clear();
    tb.requests.lock().unwrap().clear();
    *tb.mode.lock().unwrap() = Mode::Refuse(500);
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL})
        )
        .await
        .status(),
        200
    );
    let remote = tb.requests.lock().unwrap()[0].clone();
    let retry = ta
        .requests
        .lock()
        .unwrap()
        .iter()
        .find(|v| v["shard"]["index"] == 1)
        .unwrap()
        .clone();
    assert_eq!(
        retry["controls"]["original_expires_unix_ms"],
        remote["controls"]["original_expires_unix_ms"]
    );
    assert!(
        retry["controls"]["remaining_ms"].as_i64().unwrap()
            <= remote["controls"]["remaining_ms"].as_i64().unwrap()
    );
    // Old-route (404)/disabled-config (503) refusals are verdicts, not retries
    // or partial successful merges, even if the sibling already completed.
    for status in [404, 503] {
        ta.requests.lock().unwrap().clear();
        *tb.mode.lock().unwrap() = Mode::Refuse(status);
        assert!(!post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL})
        )
        .await
        .status()
        .is_success());
        assert!(!ta
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["shard"]["index"] == 1));
    }
    *tb.mode.lock().unwrap() = Mode::Transit(1000);
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL,"limits":{"timeout_seconds":1}})
        )
        .await
        .status(),
        504
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    *tb.mode.lock().unwrap() = Mode::StallBody;
    let started = std::time::Instant::now();
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL,"limits":{"timeout_seconds":1}})
        )
        .await
        .status(),
        504
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    tokio::time::timeout(Duration::from_secs(1), async {
        while tb.stalled_sources.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    *tb.mode.lock().unwrap() = Mode::Normal;
    let before = tb.requests.lock().unwrap().len();
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL,"limits":{"timeout_seconds":0}})
        )
        .await
        .status(),
        504
    );
    assert_eq!(
        tb.requests.lock().unwrap().len(),
        before,
        "zero ingress budget dispatched remote work"
    );
    // Selected nondistributable local delegation and coordinated batch/dry-run
    // remain synchronous with explicit wire authority. The separate named
    // no-distributed-configuration refusal regression remains unchanged.
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":"SELECT 1"})
        )
        .await
        .status(),
        200
    );
    assert_eq!(
        post(
            &a.base,
            &jwt,
            "/api/v1/sql/distributed",
            json!({"query":SQL,"priority":"batch","dry_run":true})
        )
        .await
        .status(),
        200
    );
    let batch = post(
        &a.base,
        &jwt,
        "/api/v1/sql/distributed",
        json!({"query":SQL,"priority":"batch"}),
    )
    .await;
    assert_eq!(batch.status(), 200);
    let body: Value = batch.json().await.unwrap();
    assert_eq!(body["rows"][0]["n"], 3);
    assert!(body.get("job_id").is_none());
    assert_eq!(state_a.jobs.active_count().await, 0);
    for state in [&state_a, &state_b] {
        tokio::time::timeout(Duration::from_secs(1), state.admission.acquire(1))
            .await
            .unwrap()
            .unwrap();
    }
    a.shutdown().await;
    b.shutdown().await;
    idp.shutdown().await;
    reader_a.close();
    reader_b.close();
    manager.close();
    admin.close().await;
    eprintln!("two-node: real v2 fanout/self-shard, worker clamp, signed external denial, strict context refusal, unchanged retry expiry, old-route/config verdicts, original HTTP/body-pump timeout; local-only clocks");
}
