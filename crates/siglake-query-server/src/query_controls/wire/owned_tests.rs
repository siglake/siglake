//! Owned loopback worker lifecycle observations, complementary to the signed
//! two-engine integration fixture. No real cross-host clock qualification.
use super::*;
use crate::{
    coordinator::{HttpShardRunner, ShardRunner},
    query_controls::{ConnectionBounds, PolicyConfig, PolicyReader},
    AppState, AuthConfig, ServerLimits,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned primary fixture; check-fanout-policy.sh"]
async fn dropped_coordinator_worker_own_expiry_and_unpolled_arrow_owner_join() {
    let socket = std::path::PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR").expect("owned fixture"),
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
    let reader = Arc::new(
        PolicyReader::connect(
            PolicyConfig::reader(
                &format!(
                    "postgres://qp_reader:@{}:5432/qh_fixture?sslmode=disable",
                    socket.to_str().unwrap().replace('/', "%2F")
                ),
                ConnectionBounds {
                    max_connections: 2,
                    operation_timeout: Duration::from_secs(2),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap(),
    );
    let tmp = tempfile::tempdir().unwrap();
    let ice = Arc::new(
        siglake_storage::iceberg::IcebergContext::open(tmp.path())
            .await
            .unwrap(),
    );
    ice.append_events(&[siglake_core::Event::now("x".repeat(256 * 1024))])
        .await
        .unwrap();
    let mut limits = ServerLimits::default();
    limits.interactive.default_timeout = Duration::from_millis(120);
    limits.interactive.ceiling_timeout = Duration::from_millis(120);
    let config = DistributedControlsConfig::new(Duration::from_millis(20)).unwrap();
    let state = AppState::new(ice, AuthConfig::from_tokens(["external"]))
        .with_limits(limits)
        .with_coordinator(vec![], Some("synthetic-coordinator".into()))
        .with_distributed_query_controls(reader.clone(), 5, config)
        .unwrap();
    let authority = state.query_controls.as_ref().unwrap().clone();
    let cancel = Arc::new(Mutex::new(None::<siglake_storage::QueryCancel>));
    let delay = Arc::new(AtomicBool::new(true));
    let ready = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(Mutex::new(Vec::new()));
    let app = crate::router(state.clone()).layer(axum::middleware::from_fn({
        let cancel = cancel.clone();
        let delay = delay.clone();
        let ready = ready.clone();
        let release = release.clone();
        let completed = completed.clone();
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let cancel = cancel.clone();
            let ready = ready.clone();
            let release = release.clone();
            let completed = completed.clone();
            let delayed = delay.load(Ordering::SeqCst);
            async move {
                let started = std::time::Instant::now();
                let response = if delayed {
                    crate::sql::shard_controls_tests::capture(cancel, next.run(request)).await
                } else {
                    next.run(request).await
                };
                completed
                    .lock()
                    .unwrap()
                    .push((response.status(), started.elapsed()));
                if !delayed {
                    ready.notify_one();
                    release.notified().await;
                }
                response
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dispatch = || {
        let mut context = super::tests::context();
        context.binding = Binding::Operator { tenant: None };
        context.state = State::Inherited;
        context.revision = None;
        context.scan_warning = None;
        context.timeout_ms = 2000;
        context.remaining_ms = 2000;
        context.clock_skew_ms = 20;
        context.original_expires_unix_ms = unix_ms_floor(SystemTime::now()).unwrap() + 2000;
        Dispatch {
            context,
            deadline: tokio::time::Instant::now() + Duration::from_secs(2),
        }
    };
    let runner = HttpShardRunner::new(vec![url.clone()], Some("synthetic-coordinator".into()))
        .with_controls(Some(dispatch()));
    let coordinator =
        tokio::spawn(async move { runner.run_detailed("SELECT raw FROM events", None).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while cancel.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(!cancel.lock().unwrap().as_ref().unwrap().is_cancelled());
    coordinator.abort();
    let _ = coordinator.await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !cancel.lock().unwrap().as_ref().unwrap().is_cancelled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    eprintln!("normal HTTP coordinator disconnect: actual worker QueryCancel observed; this alone is NOT expiry proof");
    *cancel.lock().unwrap() = None;
    completed.lock().unwrap().clear();
    // Keep the orphan transport in the FIXTURE while dropping the coordinator
    // response waiter. This prevents disconnect cancellation from masquerading
    // as proof of the worker's independent expiry backstop. No detached task.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut orphan = tokio::net::TcpStream::connect(url.trim_start_matches("http://"))
        .await
        .unwrap();
    let body = serde_json::to_vec(&serde_json::json!({"query":"SELECT raw FROM events", "controls": dispatch().context().unwrap()})).unwrap();
    let head = format!("POST {PATH} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer synthetic-coordinator\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", body.len());
    orphan.write_all(head.as_bytes()).await.unwrap();
    orphan.write_all(&body).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while cancel.lock().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let mut coordinator_waiter = Box::pin(orphan.read_u8());
    assert!(matches!(
        futures::poll!(coordinator_waiter.as_mut()),
        std::task::Poll::Pending
    ));
    drop(coordinator_waiter);
    tokio::time::timeout(Duration::from_secs(1), async {
        while completed.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let (status, elapsed) = completed.lock().unwrap()[0];
    assert_eq!(status, 504);
    assert!(elapsed >= Duration::from_millis(100) && elapsed < Duration::from_millis(500));
    assert!(cancel.lock().unwrap().as_ref().unwrap().is_cancelled());
    assert_eq!(authority.owned_body_counts(), (0, 0, 0));
    drop(orphan);
    eprintln!("dropped coordinator waiter with fixture-held orphan transport: worker completed by OWN local clamp at {elapsed:?}; real QueryCancel set; no body task retained");

    delay.store(false, Ordering::SeqCst);
    let runner = HttpShardRunner::new(vec![url], Some("synthetic-coordinator".into()))
        .with_controls(Some(dispatch()));
    let coordinator =
        tokio::spawn(async move { runner.run_detailed("SELECT raw FROM events", None).await });
    tokio::time::timeout(Duration::from_secs(1), ready.notified())
        .await
        .unwrap();
    let before = authority.owned_body_counts();
    assert_eq!(
        before,
        (0, 1, 1),
        "unpolled large Arrow source must be owned/live"
    );
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(
        authority.owned_body_counts(),
        (0, 0, 0),
        "expiry must end producer and OBSERVE retained join without client polling"
    );
    release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(1), coordinator)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    state.shutdown_query_controls().await;
    server.abort();
    let _ = server.await;
    reader.close();
    eprintln!("unpolled actual worker Arrow body: owned live producer {before:?} -> (0,0,0), retained join observed before releasing response");
}
