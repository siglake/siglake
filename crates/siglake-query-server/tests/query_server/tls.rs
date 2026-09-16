//! In-binary TLS test: spin up the query-server over rustls with a
//! self-signed cert and verify a reqwest HTTPS client accepts it.

use crate::support;

use std::sync::Arc;

use siglake_query_server::{serve_tls, AppState, AuthConfig, TlsConfig};
use siglake_storage::iceberg::IcebergContext;

macro_rules! require_loopback {
    () => {
        if !support::loopback_available() {
            return;
        }
    };
}

/// Generate a self-signed cert + key for `localhost` into a fresh
/// tempdir. Returns the dir handle (kept alive for the test) plus
/// the cert/key paths and the cert's PEM bytes (for the reqwest
/// client to trust explicitly).
fn issue_cert() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    Vec<u8>,
) {
    let dir = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    let cert_pem = cert.cert.pem().into_bytes();
    (dir, cert_path, key_path, cert_pem)
}

#[tokio::test]
async fn tls_serve_round_trip() {
    require_loopback!();
    let warehouse_tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_tmp.path().join("warehouse"))
        .await
        .unwrap();
    let state = AppState::new(Arc::new(ice), AuthConfig::open());

    let (_cert_dir, cert_path, key_path, cert_pem) = issue_cert();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: std::net::SocketAddr = listener.local_addr().unwrap();
    drop(listener); // axum-server will rebind via SocketAddr

    let tls = TlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
    };
    let server_handle = tokio::spawn(serve_tls(addr, state, tls));

    // Wait for the server to be listening — poll with a TLS-trusting
    // client until /healthz responds.
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&cert_pem).unwrap())
        .build()
        .unwrap();
    let url = format!("https://localhost:{}/healthz", addr.port());
    let mut ok = false;
    for _ in 0..40 {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status() == 200 {
                let body: serde_json::Value = resp.json().await.unwrap();
                assert_eq!(body["status"], "ok");
                ok = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    server_handle.abort();
    assert!(ok, "TLS /healthz never responded 200");
}

#[tokio::test]
async fn tls_rejects_http_request() {
    require_loopback!();
    let warehouse_tmp = tempfile::tempdir().unwrap();
    let ice = IcebergContext::open(&warehouse_tmp.path().join("warehouse"))
        .await
        .unwrap();
    let state = AppState::new(Arc::new(ice), AuthConfig::open());

    let (_cert_dir, cert_path, key_path, _cert_pem) = issue_cert();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr: std::net::SocketAddr = listener.local_addr().unwrap();
    drop(listener);

    let tls = TlsConfig {
        cert_path,
        key_path,
    };
    let server_handle = tokio::spawn(serve_tls(addr, state, tls));

    // Give the server a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Plain HTTP against a TLS port should not produce a 200.
    let plain = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let url = format!("http://localhost:{}/healthz", addr.port());
    let resp = plain.get(&url).send().await;
    server_handle.abort();
    // Either a connection error or a non-200 — never a successful
    // round-trip.
    if let Ok(r) = resp {
        assert_ne!(r.status(), 200);
    }
}
