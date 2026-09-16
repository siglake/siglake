//! The live tail must not cross tenants.
//!
//! THE DEFECT THIS GUARDS. `GET /api/v1/stream` subscribed to `events_tx`, a
//! single process-wide `broadcast::Sender` that every ingest lane feeds, and
//! yielded everything it received. `Event` carries `raw` -- the full log line --
//! so any subscriber got every tenant's log content in real time, and since
//! `Event` has no tenant field could not even tell whose it was. In the default
//! open configuration (`--auth-tokens` unset) that needs no credential at all,
//! and streaming cannot be turned off: the CLI always sets `events_tx`.
//!
//! There was NO test of this endpoint before this file, which is how it shipped.
//!
//! Note what this does and does not establish. It proves a subscriber receives
//! only the tenant it asked for. It does not prove the caller is entitled to ask
//! for that tenant -- on the ingest path the header is the sole authority for
//! tenancy, by design and separately recorded. This filter is what makes binding
//! tenancy to identity meaningful for the stream.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};

use siglake_ingest::{router, AppState, StreamedEvent, TenantRouting, TenantWalRouter};
use siglake_wal::WalWriter;

fn otlp_logs(body: &str) -> serde_json::Value {
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "host.name", "value": { "stringValue": "h1" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "test" },
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "body": { "stringValue": body },
                    "severityText": "INFO"
                }]
            }]
        }]
    })
}

async fn build_streaming_app() -> (Router, tempfile::TempDir, broadcast::Sender<StreamedEvent>) {
    let tmp = tempfile::tempdir().unwrap();
    let tenants = TenantWalRouter::new(tmp.path(), "test", 5, Duration::from_secs(60));
    let writer =
        WalWriter::with_thresholds(tmp.path(), "test", 5, Duration::from_secs(60)).unwrap();
    let (tx, _) = broadcast::channel(siglake_ingest::STREAM_BROADCAST_CAPACITY);
    let state = AppState {
        oidc_verifier: None,
        writer: Arc::new(Mutex::new(writer)),
        tenants: Some(Arc::new(tenants)),
        allowed_tenants: None,
        tenant_routing: TenantRouting::TrustHeader,
        max_tenants: 0,
        tenant_admission: Default::default(),
        backpressure: None,
        events_tx: Some(tx.clone()),
        tokens: None,
        rate_limiter: None,
        mem_guard: None,
        commit_force_timeout: Duration::from_secs(30),
        remote_wal_drain: true,
    };
    (router(state), tmp, tx)
}

#[tokio::test]
async fn stream_delivers_only_the_subscribers_own_tenant() {
    let (app, _tmp, tx) = build_streaming_app().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Subscribe as tenant-a over a raw socket: SSE never completes, so no
    // buffering client can read it.
    let mut sock = TcpStream::connect(addr).await.unwrap();
    sock.write_all(
        b"GET /api/v1/stream HTTP/1.1\r\nHost: t\r\nX-Scope-OrgID: tenant-a\r\nAccept: text/event-stream\r\n\r\n",
    )
    .await
    .unwrap();

    // The tee skips entirely while no one is listening, so publishing before the
    // subscriber registers would make this test pass against any code at all.
    let subscribed = tokio::time::timeout(Duration::from_secs(10), async {
        while tx.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(subscribed.is_ok(), "subscriber never registered");

    // tenant-b's secret goes first, so a filter that merely lags would still fail.
    let client = reqwest::Client::new();
    for (tenant, body) in [
        ("tenant-b", "BRAVO-SECRET-must-not-leak"),
        ("tenant-a", "ALFA-own-line"),
    ] {
        let resp = client
            .post(format!("http://{addr}/v1/logs"))
            .header("X-Scope-OrgID", tenant)
            .json(&otlp_logs(body))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "ingest for {tenant} failed");
    }

    // Read until tenant-a's own line arrives, or time out. Reaching the line we
    // expect means everything tenant-b published has already been through the
    // filter ahead of it, so its absence is decisive rather than a race.
    let mut seen = String::new();
    let mut buf = [0u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let n = sock.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            seen.push_str(&String::from_utf8_lossy(&buf[..n]));
            if seen.contains("ALFA-own-line") {
                break;
            }
        }
    })
    .await;
    assert!(
        read.is_ok() && seen.contains("ALFA-own-line"),
        "tenant-a never received its own event; got:\n{seen}"
    );
    assert!(
        !seen.contains("BRAVO-SECRET-must-not-leak"),
        "tenant-a received tenant-b's log line:\n{seen}"
    );
}
