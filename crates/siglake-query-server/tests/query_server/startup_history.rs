//! Actual executable smoke, separate from injected AppState/constructor proofs.
//! Only the owned socket fixture and explicitly trusted loopback issuer are used.
use super::buffered_history::history_fixture::fixture;
use super::{spawn_fake_idp, TEST_KID, TEST_PRIVATE_KEY_PEM};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::json;
use siglake_query_server::history::{CollectorStatus, HistoryCollector, HistoryConfig};
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    task::JoinHandle,
    time::{sleep, timeout, Instant},
};

const SECRET: &str = "synthetic-history-secret-sentinel";
struct Binary {
    child: Child,
    log: Arc<Mutex<Vec<u8>>>,
    reader: Option<JoinHandle<()>>,
    base: String,
    _data: tempfile::TempDir,
}
impl Binary {
    async fn start(issuer: &str, uri: Option<&str>) -> Self {
        let data = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        drop(socket);
        let mut command = Command::new(env!("CARGO_BIN_EXE_siglake-query-server"));
        command
            .args([
                "--bind",
                &addr.to_string(),
                "--metrics-bind",
                "127.0.0.1:0",
                "--data-dir",
            ])
            .arg(data.path())
            .args([
                "--audit-disabled",
                "--oidc-issuer",
                issuer,
                "--oidc-profile",
                "cognito-access",
                "--oidc-tenant-claim",
                "custom:org_id",
                "--query-object-cache-max-bytes",
                "0",
                "--query-parsed-index-cache-max-bytes",
                "0",
            ])
            .env_remove("SIGLAKE_QUERY_HISTORY_DATABASE_URL")
            .env("RUST_LOG", "info,sqlx=trace")
            .env("NO_COLOR", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(uri) = uri {
            command.env("SIGLAKE_QUERY_HISTORY_DATABASE_URL", uri);
        }
        let mut child = command.spawn().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let capture = log.clone();
        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                let n = stderr.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                let mut output = capture.lock().unwrap();
                assert!(
                    output.len() + n <= 2 * 1024 * 1024,
                    "bounded in-memory binary trace exceeded"
                );
                output.extend_from_slice(&buf[..n]);
            }
        });
        let mut binary = Self {
            child,
            log,
            reader: Some(reader),
            base: format!("http://{addr}"),
            _data: data,
        };
        let listening = timeout(Duration::from_secs(15), async {
            loop {
                if reqwest::get(format!("{}/healthz", binary.base))
                    .await
                    .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if listening.is_err() {
            // Retain only fixed diagnostic classes, not raw engine/SQLx traces.
            let exited = binary.child.try_wait().unwrap().is_some();
            let categories: Vec<_> = [
                "unexpected argument",
                "oidc discovery",
                "database is locked",
                "No such file",
                "panicked",
                "cache",
                "listening",
                "Error:",
            ]
            .into_iter()
            .filter(|text| binary.has_log(text))
            .collect();
            panic!("actual executable did not listen: exited={exited}, fixed_categories={categories:?}");
        }
        binary
    }
    fn has_log(&self, text: &str) -> bool {
        String::from_utf8_lossy(&self.log.lock().unwrap()).contains(text)
    }
    fn log_offset(&self) -> usize {
        self.log.lock().unwrap().len()
    }
    async fn status(&self, status: &str) {
        self.status_after(status, 0).await;
    }
    async fn status_after(&self, status: &str, offset: usize) {
        let observed = timeout(Duration::from_secs(5), async {
            while !String::from_utf8_lossy(&self.log.lock().unwrap()[offset..]).contains(status) {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            observed.is_ok(),
            "fixed collector status not observed: {status}; history_message={}, bytes={}",
            self.has_log("query history collector"),
            self.log.lock().unwrap().len()
        );
    }
    async fn sql(&self, token: &str) -> Option<String> {
        let response = reqwest::Client::new()
            .post(format!("{}/api/v1/sql", self.base))
            .bearer_auth(token)
            .json(&json!({"query":"SELECT 1 AS n"}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "history must not change SQL availability"
        );
        let id = response
            .headers()
            .get("x-siglake-history-id")
            .map(|v| v.to_str().unwrap().to_owned());
        // Consume without copying/retaining raw results in evidence.
        let _ = response.bytes().await.unwrap();
        id
    }
    async fn stop(&mut self, graceful: bool) {
        if graceful {
            assert!(Command::new("/run/current-system/sw/bin/kill")
                .args(["-TERM", &self.child.id().unwrap().to_string()])
                .status()
                .await
                .unwrap()
                .success());
        } else {
            self.child.start_kill().unwrap();
        }
        let status = timeout(Duration::from_secs(10), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        if graceful {
            assert!(
                status.success(),
                "actual executable graceful shutdown failed"
            );
        }
        self.reader.take().unwrap().await.unwrap();
        let log = self.log.lock().unwrap();
        assert!(
            !String::from_utf8_lossy(&log).contains(SECRET),
            "secret reflected in binary output"
        );
        assert!(
            !String::from_utf8_lossy(&log).contains("qh_missing_sentinel"),
            "upstream database detail reflected"
        );
        // Captured bytes stay memory-only; no tokens/results/raw traces persisted.
    }
}
impl Drop for Binary {
    fn drop(&mut self) {
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}
fn token(issuer: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.into());
    encode(
        &header,
        &json!({"iss":issuer,"sub":"synthetic-sub-sentinel","email":"synthetic-email-sentinel",
        "client_id":"startup-client-sentinel","token_use":"access","custom:org_id":"startup-tenant",
        "iat":now,"exp":now+600}),
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM).unwrap(),
    )
    .unwrap()
}
fn uri(socket: &std::path::Path, database: &str) -> String {
    let host = socket.to_str().unwrap().replace('/', "%2F");
    format!("postgresql://qh_writer:@{host}:5432/{database}?sslmode=disable")
}

#[tokio::test]
#[ignore = "owned PG fixture only: scripts/check-query-history-startup.sh"]
async fn actual_binary_default_off_safe_collection_rejections_outage_and_shutdown() {
    let fixture = fixture().await;
    // This proof uses the executable's writer, not injected helper handles.
    drop(fixture.writer);
    drop(fixture.reader);
    let (issuer, idp) = spawn_fake_idp().await;
    let jwt = token(&issuer);
    let configured = uri(&fixture.socket, "qh_fixture");
    let mut off = Binary::start(&issuer, None).await;
    off.status("status=Disabled").await;
    assert!(off.sql(&jwt).await.is_none());
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name='siglake-query-history'",
    )
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(count, 0, "default-off binary opened history connections");
    off.stop(true).await;

    let mut safe = Binary::start(&issuer, Some(&configured)).await;
    safe.status("status=Active").await;
    let execution = safe
        .sql(&jwt)
        .await
        .expect("safe role should collect signed named tenant");
    // Receipt/header is not durability acknowledgement; terminal may be queued.
    let terminal = timeout(Duration::from_secs(5), async {
        loop {
            let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM query_history.executions WHERE execution_id=$1 AND tenant='startup-tenant' AND client_id='startup-client-sentinel' AND redacted_sql IS NULL AND expires_at=received_at+interval '720 hours' AND effective_format IS NULL AND effective_dry_run IS NULL AND observation_state='terminal' AND outcome='success')")
                .bind(uuid::Uuid::parse_str(&execution).unwrap()).fetch_one(&fixture.admin).await.unwrap();
            if valid { break; }
            sleep(Duration::from_millis(20)).await;
        }
    }).await;
    if terminal.is_err() {
        let fixed: Option<Vec<bool>> = sqlx::query_scalar("SELECT ARRAY[observation_state='terminal',observation_state='pending',observation_state='indeterminate',COALESCE(outcome='success',false),effective_format IS NULL AND effective_dry_run IS NULL,COALESCE(execution_admitted,false)] FROM query_history.executions WHERE execution_id=$1")
            .bind(uuid::Uuid::parse_str(&execution).unwrap()).fetch_optional(&fixture.admin).await.unwrap();
        panic!("binary original receipt terminal diagnostic (terminal/pending/indeterminate/success/unresolved/admitted booleans): {fixed:?}; connection_rejected={}", safe.has_log("history connection rejected"));
    }
    // Fresh transitions only: an old startup Degraded/Active cannot satisfy
    // outage/recovery assertions. Same five-second bounds as before.
    let outage_offset = safe.log_offset();
    // Active runtime retains the same owner/pool and bounded fail-open retry.
    let mut lock = fixture.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_history.executions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let start = Instant::now();
    let delayed = safe
        .sql(&jwt)
        .await
        .expect("receipt still obtained before blocked initial write");
    assert!(start.elapsed() < Duration::from_secs(2));
    safe.status_after("status=Degraded", outage_offset).await;
    let recovery_offset = safe.log_offset();
    lock.rollback().await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            let terminal: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM query_history.executions WHERE execution_id=$1 AND observation_state='terminal' AND outcome='success')")
                .bind(uuid::Uuid::parse_str(&delayed).unwrap()).fetch_one(&fixture.admin).await.unwrap();
            if terminal { break; } sleep(Duration::from_millis(20)).await;
        }
    }).await.expect("original receipt did not recover");
    safe.status_after("status=Active", recovery_offset).await;
    // A fresh connection cannot bypass v3 after activation. This is a local
    // fixture-only schema fault, not a production migration operation.
    sqlx::query("ALTER TABLE query_history.executions DISABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='siglake-query-history'").execute(&fixture.admin).await.unwrap();
    for _ in 0..5 {
        assert!(safe.sql(&jwt).await.is_none());
        if safe.has_log("history connection rejected") {
            break;
        }
    }
    assert!(
        safe.has_log("history connection rejected"),
        "fresh connection schema refusal not exercised"
    );
    sqlx::query("ALTER TABLE query_history.executions ENABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();
    safe.stop(true).await;
    assert!(safe.has_log("query history collector shutdown"));

    sqlx::query("ALTER ROLE qh_writer SUPERUSER")
        .execute(&fixture.admin)
        .await
        .unwrap();
    let mut unsafe_role = Binary::start(&issuer, Some(&configured)).await;
    unsafe_role.status("status=Rejected").await;
    assert!(unsafe_role.sql(&jwt).await.is_none());
    unsafe_role.stop(true).await;
    sqlx::query("ALTER ROLE qh_writer NOSUPERUSER")
        .execute(&fixture.admin)
        .await
        .unwrap();

    sqlx::query("ALTER TABLE query_history.executions DISABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();
    let mut unsafe_schema = Binary::start(&issuer, Some(&configured)).await;
    unsafe_schema.status("status=Rejected").await;
    assert!(unsafe_schema.sql(&jwt).await.is_none());
    unsafe_schema.stop(true).await;
    sqlx::query("ALTER TABLE query_history.executions ENABLE TRIGGER immutable_effective_v3")
        .execute(&fixture.admin)
        .await
        .unwrap();

    let mut unavailable =
        Binary::start(&issuer, Some(&uri(&fixture.socket, "qh_missing_sentinel"))).await;
    unavailable.status("status=Unavailable").await;
    assert!(unavailable.sql(&jwt).await.is_none());
    // Real process cancellation while preflight/backoff is pending, not a
    // constructor-only simulation. SIGKILL cannot promise volatile drain.
    unavailable.stop(false).await;
    // Valid explicit TCP config includes a synthetic password; connection
    // refusal stays aggregate-only, with no host/password/upstream reflection.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let tcp =
        format!("postgresql://qh_writer:{SECRET}@127.0.0.1:{port}/qh_fixture?sslmode=verify-full");
    let mut secret = Binary::start(&issuer, Some(&tcp)).await;
    secret.status("status=Unavailable").await;
    assert!(secret.sql(&jwt).await.is_none());
    secret.stop(true).await;
    idp.abort();
    let _ = idp.await;
}

#[tokio::test]
#[ignore = "owned PG fixture only: scripts/check-query-history-startup.sh"]
async fn managed_constructor_finite_exhaustion_and_no_observer_before_preflight() {
    let fixture = fixture().await;
    let config =
        HistoryConfig::explicit(&uri(&fixture.socket, "qh_missing_sentinel"), true).unwrap();
    let mut collector = HistoryCollector::start(Some(config));
    timeout(Duration::from_secs(58), async {
        loop {
            assert!(!matches!(
                collector.status(),
                CollectorStatus::Active | CollectorStatus::Degraded
            ));
            if collector.status() == CollectorStatus::ExhaustedUnavailable {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("finite preflight attempt budget did not exhaust");
    assert!(collector.shutdown().await);
    assert_eq!(collector.status(), CollectorStatus::Closed);
}

#[test]
fn actual_binary_help_and_invalid_config_never_reflect_explicit_secrets() {
    for (help, audit) in [(true, false), (false, false), (false, true)] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_siglake-query-server"));
        command.env(
            "SIGLAKE_QUERY_HISTORY_DATABASE_URL",
            format!("invalid:{SECRET}"),
        );
        if help {
            command.arg("--help");
        }
        if audit {
            command.arg("--audit-disabled");
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.success(), help);
        assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
    }
}
