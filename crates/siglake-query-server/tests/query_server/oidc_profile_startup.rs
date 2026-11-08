//! Shared executable-level flag/env/startup proof, not a second config parser.
//! Loopback issuer only; owned local data directory; no ambient provider config.
use axum::{routing::get, Json, Router};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::process::Command;

fn command(binary: &str, ingest: bool, data: &std::path::Path, bind: &str) -> Command {
    let mut command = Command::new(binary);
    if ingest {
        command.arg("ingest-server");
    }
    command
        .arg("--data-dir")
        .arg(data)
        .args(["--bind", bind, "--metrics-bind", "127.0.0.1:0"]);
    if ingest {
        command.args(["--disable-otlp-grpc", "--ingest-backpressure-capacity", "0"]);
    } else {
        command.args([
            "--audit-disabled",
            "--query-object-cache-max-bytes",
            "0",
            "--query-parsed-index-cache-max-bytes",
            "0",
        ]);
    }
    for name in [
        "SIGLAKE_QUERY_OIDC_PROFILE",
        "SIGLAKE_INGEST_OIDC_PROFILE",
        "SIGLAKE_OIDC_ISSUER",
        "SIGLAKE_OIDC_AUDIENCE",
        "SIGLAKE_OIDC_TENANT_CLAIM",
    ] {
        command.env_remove(name);
    }
    command.kill_on_drop(true);
    command
}

pub async fn invalid_config_before_io(binary: &str, ingest: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let seen = count.clone();
    let provider = tokio::spawn(async move {
        while let Ok((_stream, _)) = listener.accept().await {
            seen.fetch_add(1, Ordering::SeqCst);
        }
    });
    let profile_env = if ingest {
        "SIGLAKE_INGEST_OIDC_PROFILE"
    } else {
        "SIGLAKE_QUERY_OIDC_PROFILE"
    };
    for via_env in [false, true] {
        for missing in 0..3 {
            for blank in [None, Some(""), Some(" "), Some("\t\n")] {
                let tmp = tempfile::tempdir().unwrap();
                let data = tmp.path().join("must-not-exist");
                let mut command = command(binary, ingest, &data, "127.0.0.1:0");
                command.env("OTEL_EXPORTER_OTLP_ENDPOINT", &issuer);
                if via_env {
                    command.env(profile_env, "cognito-mixed");
                } else {
                    command.args(["--oidc-profile", "cognito-mixed"]);
                }
                let values = [issuer.as_str(), "human-api", "tenant"];
                for (index, (flag, env)) in [
                    ("--oidc-issuer", "SIGLAKE_OIDC_ISSUER"),
                    ("--oidc-audience", "SIGLAKE_OIDC_AUDIENCE"),
                    ("--oidc-tenant-claim", "SIGLAKE_OIDC_TENANT_CLAIM"),
                ]
                .into_iter()
                .enumerate()
                {
                    let value = if index == missing {
                        blank
                    } else {
                        Some(values[index])
                    };
                    if let Some(value) = value {
                        if via_env {
                            command.env(env, value);
                        } else {
                            command.args([flag, value]);
                        }
                    }
                }
                let output = tokio::time::timeout(Duration::from_secs(10), command.output())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!output.status.success());
                assert!(String::from_utf8_lossy(&output.stderr)
                    .contains("--oidc-profile cognito-mixed requires"));
                assert!(!data.exists(), "invalid mixed created storage");
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    0,
                    "invalid mixed contacted discovery"
                );
            }
        }
    }
    for via_env in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let mut command = command(binary, ingest, tmp.path(), "127.0.0.1:0");
        if via_env {
            command.env(profile_env, "unknown");
        } else {
            command.args(["--oidc-profile", "unknown"]);
        }
        assert!(!command.output().await.unwrap().status.success());
    }
    let tmp = tempfile::tempdir().unwrap();
    let output = command(binary, ingest, tmp.path(), "127.0.0.1:0")
        .arg("--help")
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(
        help.contains("cognito-mixed")
            && help.contains(profile_env)
            && help.contains("human API audience")
    );
    provider.abort();
}

pub async fn valid_selection_installs_oidc_not_open_or_static(binary: &str, ingest: bool) {
    let signer = TransientSigner::new();
    let jwks = json!({"keys":[signer.jwk("startup-key")]});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let metadata_issuer = issuer.clone();
    let discoveries = Arc::new(AtomicUsize::new(0));
    let seen = discoveries.clone();
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || {
                seen.fetch_add(1, Ordering::SeqCst);
                let issuer = metadata_issuer.clone();
                async move { Json(json!({"jwks_uri":format!("{issuer}/jwks")})) }
            }),
        )
        .route(
            "/jwks",
            get(move || {
                let jwks = jwks.clone();
                async move { Json(jwks) }
            }),
        );
    let provider = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for via_env in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        drop(socket);
        let mut rebuilt = command(binary, ingest, tmp.path(), &addr.to_string());
        rebuilt
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        rebuilt.args([
            if ingest { "--auth-tokens" } else { "--tokens" },
            "synthetic-static",
        ]);
        if via_env {
            rebuilt
                .env(
                    if ingest {
                        "SIGLAKE_INGEST_OIDC_PROFILE"
                    } else {
                        "SIGLAKE_QUERY_OIDC_PROFILE"
                    },
                    "cognito-mixed",
                )
                .env("SIGLAKE_OIDC_ISSUER", &issuer)
                .env("SIGLAKE_OIDC_AUDIENCE", "human-api")
                .env("SIGLAKE_OIDC_TENANT_CLAIM", "tenant");
        } else {
            rebuilt.args([
                "--oidc-profile",
                "cognito-mixed",
                "--oidc-issuer",
                &issuer,
                "--oidc-audience",
                "human-api",
                "--oidc-tenant-claim",
                "tenant",
            ]);
        }
        let mut child = rebuilt.spawn().unwrap();
        let base = format!("http://{addr}");
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if reqwest::get(format!("{base}/healthz")).await.is_ok() {
                    break;
                }
                assert!(child.try_wait().unwrap().is_none(), "mixed startup exited");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let response = reqwest::Client::new()
            .post(format!(
                "{base}{}",
                if ingest { "/v1/logs" } else { "/api/v1/sql" }
            ))
            .bearer_auth("synthetic-static")
            .json(&json!({"query":"SELECT 1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            401,
            "explicit mixed fell back to static/open auth"
        );
        for use_ in ["id", "access"] {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut claims = json!({"iss":issuer,"sub":"synthetic-subject","exp":now+600,"iat":now,"token_use":use_,"tenant":"acme"});
            if use_ == "id" {
                claims["aud"] = json!("human-api");
            } else {
                claims["client_id"] = json!("synthetic-client");
            }
            let response = reqwest::Client::new()
                .post(format!(
                    "{base}{}",
                    if ingest { "/v1/logs" } else { "/api/v1/sql" }
                ))
                .bearer_auth(signer.sign(&claims, "startup-key"))
                .json(&if ingest {
                    json!({"resourceLogs":[]})
                } else {
                    json!({"query":"SELECT 1"})
                })
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                200,
                "real CLI selected wrong token contract"
            );
            // Consume the synthetic response without recording query contents.
            let _ = response.bytes().await.unwrap();
        }
        child.kill().await.unwrap();
        child.wait().await.unwrap();
    }
    assert_eq!(discoveries.load(Ordering::SeqCst), 2);
    provider.abort();
}

// A second real signing key where tests need distinct ID/access keys. Keys live
// only in owned 0700/0600 transient files and are removed on Drop, never logged.
pub(super) struct TransientSigner {
    _root: tempfile::TempDir,
    key: tempfile::NamedTempFile,
}
impl TransientSigner {
    pub(super) fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let key = tempfile::NamedTempFile::new_in(root.path()).unwrap();
        assert!(std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out"
            ])
            .arg(key.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success());
        assert_eq!(
            key.as_file().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
        Self { _root: root, key }
    }
    pub(super) fn jwk(&self, kid: &str) -> serde_json::Value {
        let output = std::process::Command::new("openssl")
            .args(["rsa", "-in"])
            .arg(self.key.path())
            .args(["-noout", "-modulus"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        let hex = text.trim().strip_prefix("Modulus=").unwrap();
        let bytes: Vec<_> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        json!({"kty":"RSA","kid":kid,"use":"sig","alg":"RS256","n":base64url(&bytes),"e":"AQAB"})
    }
    pub(super) fn sign(&self, claims: &serde_json::Value, kid: &str) -> String {
        use std::io::Write;
        let input = format!(
            "{}.{}",
            base64url(json!({"alg":"RS256","kid":kid}).to_string().as_bytes()),
            base64url(claims.to_string().as_bytes())
        );
        let mut child = std::process::Command::new("openssl")
            .args(["dgst", "-sha256", "-sign"])
            .arg(self.key.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success());
        format!("{input}.{}", base64url(&output.stdout))
    }
}
fn base64url(bytes: &[u8]) -> String {
    use std::io::Write;
    let mut child = std::process::Command::new("openssl")
        .args(["base64", "-A"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_")
}
