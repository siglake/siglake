//! Real PostgreSQL only: scripts/check-query-history.sh creates the fixture.
//! No DATABASE_URL, production URI, external services or ambient PG settings.
use super::*;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool, Row,
};
use std::{path::PathBuf, time::Duration};
use tokio::sync::{Mutex, MutexGuard};
static DB_LOCK: Mutex<()> = Mutex::const_new(());
pub(super) struct Fixture {
    _guard: MutexGuard<'static, ()>,
    pub(super) admin: PgPool,
    pub(super) writer: HistoryWriter,
    pub(super) reader: HistoryReader,
    pub(super) socket: PathBuf,
}
fn assert_fixture_isolation(socket: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    // Inspect ONLY files created by our disposable runner, never real HOME or
    // passfiles. new_without_pgpass still imports PG environment defaults.
    for (name, _) in std::env::vars_os() {
        assert!(
            !name.to_string_lossy().starts_with("PG")
                || matches!(
                    name.to_str(),
                    Some(
                        "PGPASSFILE"
                            | "PGHOST"
                            | "PGPORT"
                            | "PGUSER"
                            | "PGDATABASE"
                            | "PGPASSWORD"
                            | "PGSSLMODE"
                            | "PGAPPNAME"
                    )
                ),
            "unexpected PostgreSQL environment variable"
        );
    }
    assert!(
        std::env::var_os("PGHOST").is_some_and(|actual| actual.as_os_str() == socket.as_os_str()),
        "PGHOST is not the fixture socket"
    );
    for (name, expected) in [
        ("PGPORT", "5432"),
        ("PGUSER", "fixture_admin"),
        ("PGDATABASE", "qh_fixture"),
        ("PGPASSWORD", ""),
        ("PGSSLMODE", "disable"),
        ("PGAPPNAME", "siglake-history-fixture"),
    ] {
        assert!(
            std::env::var(name).is_ok_and(|value| value == expected),
            "non-fixture PostgreSQL default"
        );
    }
    assert!(std::env::var_os("DATABASE_URL").is_none());
    for (name, relative) in [
        ("HOME", "home"),
        ("XDG_CONFIG_HOME", "xdg-config"),
        ("XDG_CACHE_HOME", "xdg-cache"),
        ("XDG_DATA_HOME", "xdg-data"),
        ("XDG_STATE_HOME", "xdg-state"),
    ] {
        assert!(
            std::env::var_os(name)
                .is_some_and(|actual| actual.as_os_str() == socket.join(relative).as_os_str()),
            "non-fixture isolation path"
        );
        assert_eq!(
            std::fs::metadata(socket.join(relative))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
    for (name, relative) in [
        ("PGPASSFILE", "empty.pgpass"),
        ("AWS_CONFIG_FILE", "providers/aws-config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "providers/aws-credentials"),
        ("BOTO_CONFIG", "providers/boto-config"),
        (
            "GOOGLE_APPLICATION_CREDENTIALS",
            "providers/google-credentials",
        ),
        ("KUBECONFIG", "providers/kubeconfig"),
    ] {
        assert!(
            std::env::var_os(name)
                .is_some_and(|actual| actual.as_os_str() == socket.join(relative).as_os_str()),
            "non-fixture isolation path"
        );
        let metadata = std::fs::metadata(socket.join(relative)).unwrap();
        assert!(metadata.is_file() && metadata.len() == 0);
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
    let fallback = std::fs::metadata(socket.join("home/.pgpass")).unwrap();
    assert!(fallback.is_file() && fallback.len() == 0);
    assert_eq!(fallback.permissions().mode() & 0o777, 0o600);
    assert_eq!(std::env::var("AWS_EC2_METADATA_DISABLED").unwrap(), "true");
    assert_eq!(
        std::env::var("AWS_EC2_METADATA_V1_DISABLED").unwrap(),
        "true"
    );
    assert_eq!(std::env::var("CARGO_NET_OFFLINE").unwrap(), "true");
}
pub(super) fn options(socket: &std::path::Path, role: &str) -> PgConnectOptions {
    assert_fixture_isolation(socket);
    let options = PgConnectOptions::new_without_pgpass()
        .host(socket.to_str().unwrap())
        .socket(socket)
        .port(5432)
        .database("qh_fixture")
        .username(role)
        .password("") // Synthetic trust-auth fixture only; no passfile lookup.
        .application_name("siglake-history-fixture")
        .ssl_mode(PgSslMode::Disable);
    assert_eq!(options.get_socket().unwrap(), socket);
    assert_eq!(options.get_options(), None);
    options
}
pub(super) async fn fixture() -> Fixture {
    let guard = DB_LOCK.lock().await;
    let socket = PathBuf::from(
        std::env::var("SIGLAKE_HISTORY_FIXTURE_DIR").expect("use scripts/check-query-history.sh"),
    );
    assert!(
        socket.starts_with("/tmp")
            && socket
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("siglake-history-")
    );
    assert_eq!(
        std::fs::read_to_string(socket.join("fixture-marker")).unwrap(),
        "isolated-history-fixture-v1\n"
    );
    let admin = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options(&socket, "fixture_admin"))
        .await
        .unwrap();
    sqlx::query("TRUNCATE query_history.executions,query_history.coverage,query_history.owners")
        .execute(&admin)
        .await
        .unwrap();
    let writer = HistoryWriter::connect(options(&socket, "qh_writer"))
        .await
        .unwrap();
    let reader = HistoryReader::connect(options(&socket, "qh_reader"))
        .await
        .unwrap();
    Fixture {
        _guard: guard,
        admin,
        writer,
        reader,
        socket,
    }
}
async fn event(f: &Fixture, tenant: &str, sql: &str) -> PendingExecution {
    let caller = CallerIdentity {
        subject: "SECRET_BEARER".into(),
        email: Some("SECRET_EMAIL".into()),
        ..Default::default()
    };
    PendingExecution::new(
        f.writer.receipt().await.unwrap(),
        Principal::from_caller(tenant, &caller).unwrap(),
        Uuid::new_v4(),
        RequestMetadata {
            operation: Operation::Sql,
            format: OutputFormat::Records,
            priority: Priority::Interactive,
            dry_run: false,
        },
        RedactedSql::parse(sql),
    )
    .unwrap()
}
pub(super) fn terminal(outcome: Outcome) -> TerminalExecution {
    let code = match outcome {
        Outcome::Success => None,
        Outcome::Rejected => Some(ErrorCode::Planning),
        Outcome::Failed => Some(ErrorCode::Execution),
        Outcome::TimedOut => Some(ErrorCode::Timeout),
        Outcome::Cancelled => Some(ErrorCode::Cancelled),
        Outcome::Disconnected => Some(ErrorCode::Disconnected),
    };
    TerminalExecution::new(
        if outcome == Outcome::Rejected {
            AdmissionEvidence::NotAdmitted
        } else {
            AdmissionEvidence::Admitted
        },
        outcome,
        code,
        ExecutionMeasurements {
            counters: Counters {
                estimated_bytes_scanned: Some(Counter::new(4096).unwrap()),
                ..Default::default()
            },
            duration_ms: Some(Counter::new(7).unwrap()),
            cache_state: CacheState::Unknown,
            truncated: None,
            job_id: None,
        },
    )
    .unwrap()
}
pub(super) async fn json_row(f: &Fixture, id: Uuid) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(e) FROM query_history.executions e WHERE execution_id=$1")
        .bind(id)
        .fetch_one(&f.admin)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_durable_idempotency_and_privacy() {
    let f = fixture().await;
    for original in ["SELECT \"SECRET_IDENTIFIER\",'SECRET_LITERAL',999888777,$$SECRET_DOLLAR$$ /*SECRET_COMMENT*/", "WITH SECRET_CTE AS (SELECT 'SECRET_LITERAL') SELECT * FROM SECRET_CTE", "EXPLAIN SELECT SECRET_FUNCTION('SECRET_LITERAL')", "SELECT 'SECRET_LITERAL' UNION SELECT 'SECRET_UNION'", "SELECT 'SECRET_MALFORMED"] {
        let event = event(&f,"tenant-a",original).await;
        assert!(!serde_json::to_string(&event).unwrap().contains("SECRET"));
        let receipt = event.receipt();
        assert_eq!((receipt.expires_at()-receipt.received_at()).num_seconds(),30*24*60*60);
        f.writer.persist_pending(&event).await.unwrap();
        f.writer.persist_pending(&event).await.unwrap(); // ambiguous initial acknowledgement retry
        f.writer.admit(receipt).await.unwrap();
        f.writer.admit(receipt).await.unwrap();
        let before = json_row(&f,receipt.execution_id()).await;
        f.writer.finish(receipt,&terminal(Outcome::Success)).await.unwrap();
        let persisted = json_row(&f,receipt.execution_id()).await;
        assert_eq!(persisted["expires_at"],before["expires_at"]);
        assert_eq!(persisted["measured_bytes_scanned"],serde_json::Value::Null);
        assert_eq!(persisted["estimated_bytes_scanned"],4096);
        assert_eq!(persisted["redacted_sql"], serde_json::Value::Null);
        assert_eq!(persisted["redaction_status"], "omitted");
        assert!(!persisted.to_string().contains("SECRET"));
        assert!(!persisted.to_string().contains("999888777"));
        f.writer.finish(receipt,&terminal(Outcome::Success)).await.unwrap();
        f.writer.persist_pending(&event).await.unwrap(); // never reverts terminal
        assert_eq!(json_row(&f,receipt.execution_id()).await,persisted);
        assert_eq!(f.writer.finish(receipt,&terminal(Outcome::Cancelled)).await.err(),Some(HistoryError::Conflict));
        let restarted = HistoryReader::connect(options(&f.socket,"qh_reader")).await.unwrap();
        let detail = restarted.detail("tenant-a",receipt.execution_id()).await.unwrap().unwrap();
        assert!(!serde_json::to_string(&detail).unwrap().contains("SECRET"));
        assert!(f.reader.detail("tenant-b",receipt.execution_id()).await.unwrap().is_none());
    }
    // Rejection recovery can insert a missing initial observation with its exact receipt.
    let missing = event(&f, "tenant-a", "SELECT 'SECRET_MISSING'").await;
    f.writer
        .finish_pending(&missing, &terminal(Outcome::Rejected))
        .await
        .unwrap();
    assert_eq!(
        json_row(&f, missing.receipt.execution_id()).await["execution_admitted"],
        false
    );
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_concurrent_terminal_and_uncertain_recovery() {
    let f = fixture().await;
    let event = event(&f, "tenant-a", "SELECT 42").await;
    f.writer.persist_pending(&event).await.unwrap();
    f.writer.heartbeat(event.owner_uuid).await.unwrap();
    assert_eq!(
        f.writer.mark_indeterminate(event.owner_uuid).await.unwrap(),
        1
    );
    let uncertain = json_row(&f, event.receipt.execution_id()).await;
    assert_eq!(uncertain["observation_state"], "indeterminate");
    assert_eq!(uncertain["outcome"], serde_json::Value::Null);
    assert_eq!(uncertain["execution_admitted"], serde_json::Value::Null);
    f.writer.admit(&event.receipt).await.unwrap();
    let success = terminal(Outcome::Success);
    let cancelled = terminal(Outcome::Cancelled);
    let (a, b) = tokio::join!(
        f.writer.finish(&event.receipt, &success),
        f.writer.finish(&event.receipt, &cancelled)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(
        if a.is_err() { a.err() } else { b.err() },
        Some(HistoryError::Conflict)
    );
    assert_eq!(
        f.writer.mark_indeterminate(event.owner_uuid).await.unwrap(),
        0
    );
    let mut conflicting = event.clone();
    conflicting.request.dry_run = true;
    assert_eq!(
        f.writer.persist_pending(&conflicting).await.err(),
        Some(HistoryError::Conflict)
    );
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_rls_concurrency_missing_where_and_cancel_cleanup() {
    let f = fixture().await;
    for tenant in ["tenant-a", "tenant-b"] {
        let event = event(&f, tenant, "SELECT 'SECRET'").await;
        f.writer.persist_pending(&event).await.unwrap();
    }
    let forced: bool = sqlx::query_scalar("SELECT bool_and(relrowsecurity AND relforcerowsecurity) FROM pg_class WHERE oid IN ('query_history.executions'::regclass,'query_history.coverage'::regclass)").fetch_one(&f.admin).await.unwrap();
    assert!(forced);
    let raw = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options(&f.socket, "qh_reader"))
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(count, 0); // absent context denies even with missing WHERE
    for tenant in ["tenant-a", "tenant-b"] {
        let mut tx = raw.begin().await.unwrap();
        sqlx::query("SELECT set_config('query_history.tenant',$1,true)")
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .unwrap();
        let tenants: Vec<String> =
            sqlx::query_scalar("SELECT tenant FROM query_history.executions")
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        assert_eq!(tenants, vec![tenant]);
        tx.rollback().await.unwrap();
    }
    let interleaved = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options(&f.socket, "qh_reader"))
        .await
        .unwrap();
    async fn missing_where(
        pool: &PgPool,
        tenant: &str,
        barrier: &tokio::sync::Barrier,
    ) -> Vec<String> {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT set_config('query_history.tenant',$1,true)")
            .bind(tenant)
            .execute(&mut *tx)
            .await
            .unwrap();
        barrier.wait().await; // both distinct pooled transactions have their own context
        let rows = sqlx::query_scalar("SELECT tenant FROM query_history.executions")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        rows
    }
    for _ in 0..10 {
        let barrier = tokio::sync::Barrier::new(2);
        let (a, b) = tokio::join!(
            missing_where(&interleaved, "tenant-a", &barrier),
            missing_where(&interleaved, "tenant-b", &barrier)
        );
        assert_eq!(a, vec!["tenant-a"]);
        assert_eq!(b, vec!["tenant-b"]);
    }
    for _ in 0..20 {
        let (a, b) = tokio::join!(
            f.reader.list("tenant-a", 100),
            f.reader.list("tenant-b", 100)
        );
        for (rows, expected) in [(a.unwrap(), "tenant-a"), (b.unwrap(), "tenant-b")] {
            assert_eq!(rows.len(), 1);
            assert_eq!(serde_json::to_value(&rows[0]).unwrap()["tenant"], expected);
        }
    }
    let cancelled = tokio::time::timeout(Duration::from_millis(50), async {
        let mut tx = raw.begin().await.unwrap();
        sqlx::query("SELECT set_config('query_history.tenant',$1,true)")
            .bind("tenant-a")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT pg_sleep(0.25)")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    })
    .await;
    assert!(cancelled.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(count, 0); // cancelled transaction was rolled back before reuse
    assert!(sqlx::query("SELECT * FROM query_history.owners")
        .fetch_all(&raw)
        .await
        .is_err());
    assert!(sqlx::query("SELECT * FROM app_catalog.secrets")
        .fetch_all(&raw)
        .await
        .is_err());
    assert!(sqlx::query("DELETE FROM query_history.executions")
        .execute(&raw)
        .await
        .is_err());
    assert!(sqlx::query("CREATE TABLE public.reader_owned(x int)")
        .execute(&raw)
        .await
        .is_err());
    assert!(sqlx::query("SET ROLE qh_owner")
        .execute(&raw)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_rejects_privileged_or_assumable_pool_roles() {
    let f = fixture().await;
    assert!(HistoryReader::connect(options(&f.socket, "fixture_admin"))
        .await
        .is_err());
    assert!(HistoryReader::connect(options(&f.socket, "qh_writer"))
        .await
        .is_err());
    assert!(HistoryWriter::connect(options(&f.socket, "fixture_admin"))
        .await
        .is_err());
    for (grant, revoke) in [
        (
            "ALTER ROLE qh_reader BYPASSRLS",
            "ALTER ROLE qh_reader NOBYPASSRLS",
        ),
        (
            "ALTER ROLE qh_reader SUPERUSER",
            "ALTER ROLE qh_reader NOSUPERUSER",
        ),
        (
            "GRANT qh_owner TO qh_reader WITH INHERIT FALSE",
            "REVOKE qh_owner FROM qh_reader",
        ),
        (
            "GRANT SELECT ON app_catalog.secrets TO qh_reader",
            "REVOKE SELECT ON app_catalog.secrets FROM qh_reader",
        ),
        (
            "GRANT UPDATE ON query_history.executions TO qh_reader",
            "REVOKE UPDATE ON query_history.executions FROM qh_reader",
        ),
        (
            "GRANT TEMP ON DATABASE qh_fixture TO qh_reader",
            "REVOKE TEMP ON DATABASE qh_fixture FROM qh_reader",
        ),
        (
            "ALTER TABLE app_catalog.secrets OWNER TO qh_reader",
            "ALTER TABLE app_catalog.secrets OWNER TO fixture_admin",
        ),
        (
            "ALTER TABLE query_history.executions NO FORCE ROW LEVEL SECURITY",
            "ALTER TABLE query_history.executions FORCE ROW LEVEL SECURITY",
        ),
    ] {
        sqlx::query(grant).execute(&f.admin).await.unwrap();
        assert!(HistoryReader::connect(options(&f.socket, "qh_reader"))
            .await
            .is_err());
        assert_eq!(
            f.reader.list("tenant-a", 1).await.err(),
            Some(HistoryError::Privilege)
        );
        sqlx::query(revoke).execute(&f.admin).await.unwrap();
    }
    sqlx::query("CREATE ROLE fixture_bypass BYPASSRLS")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("GRANT fixture_bypass TO qh_reader WITH INHERIT FALSE")
        .execute(&f.admin)
        .await
        .unwrap();
    assert!(HistoryReader::connect(options(&f.socket, "qh_reader"))
        .await
        .is_err());
    sqlx::query("REVOKE fixture_bypass FROM qh_reader")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("DROP ROLE fixture_bypass")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("CREATE FUNCTION app_catalog.fixture_lookup() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'").execute(&f.admin).await.unwrap();
    assert!(HistoryReader::connect(options(&f.socket, "qh_reader"))
        .await
        .is_err());
    sqlx::query("DROP FUNCTION app_catalog.fixture_lookup()")
        .execute(&f.admin)
        .await
        .unwrap();
    assert!(HistoryReader::connect(options(&f.socket, "qh_reader"))
        .await
        .is_ok());
}

/// Both new-connection preflight and the existing pool's transaction recheck
/// must reject late privilege drift, before any tenant rows are returned.
async fn reader_privilege_rejected(f: &Fixture) {
    assert_eq!(
        HistoryReader::connect(options(&f.socket, "qh_reader"))
            .await
            .err(),
        Some(HistoryError::Privilege)
    );
    assert_eq!(
        f.reader.list("tenant-a", 1).await.err(),
        Some(HistoryError::Privilege)
    );
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_rejects_column_only_privileges_and_late_grants() {
    let f = fixture().await;
    sqlx::query("GRANT USAGE ON SCHEMA app_catalog TO qh_reader,qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    // Fixed fixture-only statements. SELECT on owners/secrets is genuinely
    // usable; no table ACL exists to make the original preflight reject it.
    for (grant, revoke, relation, right) in [
        (
            "GRANT SELECT(owner_uuid) ON query_history.owners TO qh_reader",
            "REVOKE SELECT(owner_uuid) ON query_history.owners FROM qh_reader",
            "query_history.owners",
            "SELECT",
        ),
        (
            "GRANT SELECT(value) ON app_catalog.secrets TO qh_reader",
            "REVOKE SELECT(value) ON app_catalog.secrets FROM qh_reader",
            "app_catalog.secrets",
            "SELECT",
        ),
        (
            "GRANT INSERT(tenant) ON query_history.executions TO qh_reader",
            "REVOKE INSERT(tenant) ON query_history.executions FROM qh_reader",
            "query_history.executions",
            "INSERT",
        ),
        (
            "GRANT UPDATE(tenant) ON query_history.executions TO qh_reader",
            "REVOKE UPDATE(tenant) ON query_history.executions FROM qh_reader",
            "query_history.executions",
            "UPDATE",
        ),
        (
            "GRANT REFERENCES(tenant) ON query_history.coverage TO qh_reader",
            "REVOKE REFERENCES(tenant) ON query_history.coverage FROM qh_reader",
            "query_history.coverage",
            "REFERENCES",
        ),
    ] {
        sqlx::query(grant).execute(&f.admin).await.unwrap();
        let column_only: bool = sqlx::query_scalar("SELECT NOT has_table_privilege('qh_reader',$1,$2) AND has_any_column_privilege('qh_reader',$1,$2)")
            .bind(relation).bind(right).fetch_one(&f.admin).await.unwrap();
        assert!(column_only);
        reader_privilege_rejected(&f).await;
        sqlx::query(revoke).execute(&f.admin).await.unwrap();
        assert!(f.reader.list("tenant-a", 1).await.is_ok());
    }
    // Forbidden application relations must reject every column-grantable right
    // for the writer too, not only SELECT or the reader's write permissions.
    for (grant, revoke) in [
        (
            "GRANT SELECT(value) ON app_catalog.secrets TO qh_writer",
            "REVOKE SELECT(value) ON app_catalog.secrets FROM qh_writer",
        ),
        (
            "GRANT INSERT(value) ON app_catalog.secrets TO qh_writer",
            "REVOKE INSERT(value) ON app_catalog.secrets FROM qh_writer",
        ),
        (
            "GRANT UPDATE(value) ON app_catalog.secrets TO qh_writer",
            "REVOKE UPDATE(value) ON app_catalog.secrets FROM qh_writer",
        ),
        (
            "GRANT REFERENCES(value) ON app_catalog.secrets TO qh_writer",
            "REVOKE REFERENCES(value) ON app_catalog.secrets FROM qh_writer",
        ),
    ] {
        sqlx::query(grant).execute(&f.admin).await.unwrap();
        assert_eq!(
            HistoryWriter::connect(options(&f.socket, "qh_writer"))
                .await
                .err(),
            Some(HistoryError::Privilege)
        );
        sqlx::query(revoke).execute(&f.admin).await.unwrap();
    }
    sqlx::query("REVOKE USAGE ON SCHEMA app_catalog FROM qh_reader,qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert!(HistoryWriter::connect(options(&f.socket, "qh_writer"))
        .await
        .is_ok());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_audits_pgapp_schema_and_late_privileges() {
    let f = fixture().await;
    sqlx::query("CREATE SCHEMA pgapp")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE pgapp.secrets(value text)")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("GRANT USAGE ON SCHEMA pgapp TO qh_reader,qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    // pgapp is an ordinary application schema, not the literal pg_ prefix.
    // Each grant is tested independently, so no other failing condition masks it.
    for (grant, revoke) in [
        (
            "GRANT CREATE ON SCHEMA pgapp TO qh_reader",
            "REVOKE CREATE ON SCHEMA pgapp FROM qh_reader",
        ),
        (
            "GRANT SELECT ON pgapp.secrets TO qh_reader",
            "REVOKE SELECT ON pgapp.secrets FROM qh_reader",
        ),
        (
            "GRANT SELECT(value) ON pgapp.secrets TO qh_reader",
            "REVOKE SELECT(value) ON pgapp.secrets FROM qh_reader",
        ),
    ] {
        sqlx::query(grant).execute(&f.admin).await.unwrap();
        reader_privilege_rejected(&f).await;
        sqlx::query(revoke).execute(&f.admin).await.unwrap();
        assert!(f.reader.list("tenant-a", 1).await.is_ok());
    }
    for (grant, revoke) in [
        (
            "GRANT CREATE ON SCHEMA pgapp TO qh_writer",
            "REVOKE CREATE ON SCHEMA pgapp FROM qh_writer",
        ),
        (
            "GRANT SELECT ON pgapp.secrets TO qh_writer",
            "REVOKE SELECT ON pgapp.secrets FROM qh_writer",
        ),
    ] {
        sqlx::query(grant).execute(&f.admin).await.unwrap();
        assert_eq!(
            HistoryWriter::connect(options(&f.socket, "qh_writer"))
                .await
                .err(),
            Some(HistoryError::Privilege)
        );
        sqlx::query(revoke).execute(&f.admin).await.unwrap();
    }
    // Default PUBLIC EXECUTE on an admin-owned SECURITY DEFINER is sufficient
    // to reach protected application data. Prove executability with a raw reader
    // connection, then prove the history API rejects it, including its old pool.
    sqlx::query("CREATE FUNCTION pgapp.fixture_lookup() RETURNS bigint LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS 'SELECT count(*) FROM app_catalog.secrets'")
        .execute(&f.admin).await.unwrap();
    let raw = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options(&f.socket, "qh_reader"))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT pgapp.fixture_lookup()")
            .fetch_one(&raw)
            .await
            .unwrap(),
        0
    );
    reader_privilege_rejected(&f).await;
    assert_eq!(
        HistoryWriter::connect(options(&f.socket, "qh_writer"))
            .await
            .err(),
        Some(HistoryError::Privilege)
    );
    sqlx::query("REVOKE EXECUTE ON FUNCTION pgapp.fixture_lookup() FROM PUBLIC")
        .execute(&f.admin)
        .await
        .unwrap();
    assert!(f.reader.list("tenant-a", 1).await.is_ok());
    sqlx::query("GRANT EXECUTE ON FUNCTION pgapp.fixture_lookup() TO qh_reader")
        .execute(&f.admin)
        .await
        .unwrap();
    reader_privilege_rejected(&f).await;
    sqlx::query("REVOKE EXECUTE ON FUNCTION pgapp.fixture_lookup() FROM qh_reader")
        .execute(&f.admin)
        .await
        .unwrap();
    raw.close().await;
    sqlx::query("DROP SCHEMA pgapp CASCADE")
        .execute(&f.admin)
        .await
        .unwrap();
    assert!(f.reader.list("tenant-a", 1).await.is_ok());
    assert!(HistoryWriter::connect(options(&f.socket, "qh_writer"))
        .await
        .is_ok());
}

/// Backdate ONLY synthetic fixture rows, with privileged TEST inspection. The
/// production writer has neither ALTER TABLE nor a controllable clock API.
pub(super) async fn expire(f: &Fixture, event: &mut PendingExecution) {
    sqlx::query("ALTER TABLE query_history.executions DISABLE TRIGGER immutable_receipt")
        .execute(&f.admin)
        .await
        .unwrap();
    let row = sqlx::query("WITH t AS MATERIALIZED (SELECT clock_timestamp()-interval '1 second' AS expiry) UPDATE query_history.executions SET received_at=t.expiry-interval '720 hours', expires_at=t.expiry,admission_observed_at=CASE WHEN execution_admitted THEN t.expiry-interval '719 hours' ELSE NULL END,finished_observed_at=CASE WHEN observation_state='terminal' THEN t.expiry-interval '718 hours' ELSE NULL END FROM t WHERE execution_id=$1 RETURNING received_at,expires_at")
        .bind(event.receipt.execution_id()).fetch_one(&f.admin).await.unwrap();
    event.receipt.received_at = row.get("received_at");
    event.receipt.expires_at = row.get("expires_at");
    sqlx::query("ALTER TABLE query_history.executions ENABLE TRIGGER immutable_receipt")
        .execute(&f.admin)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_expiry_purge_failure_restart_and_late_retry() {
    let f = fixture().await;
    let mut pending = event(&f, "tenant-a", "SELECT 'SECRET'").await;
    let mut finished = event(&f, "tenant-a", "SELECT 42").await;
    for event in [&pending, &finished] {
        f.writer.persist_pending(event).await.unwrap();
    }
    f.writer.admit(&finished.receipt).await.unwrap();
    f.writer
        .finish(&finished.receipt, &terminal(Outcome::Success))
        .await
        .unwrap();
    expire(&f, &mut pending).await;
    expire(&f, &mut finished).await;
    let mut plan_tx = f.admin.begin().await.unwrap();
    sqlx::query("SET LOCAL timezone='America/New_York'")
        .execute(&mut *plan_tx)
        .await
        .unwrap();
    let absolute: bool=sqlx::query_scalar("SELECT extract(epoch FROM (('2026-03-01 00:00:00-05'::timestamptz+interval '720 hours')-'2026-03-01 00:00:00-05'::timestamptz))=2592000").fetch_one(&mut *plan_tx).await.unwrap();
    assert!(absolute);
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *plan_tx)
        .await
        .unwrap();
    let plan: serde_json::Value=sqlx::query_scalar("EXPLAIN (FORMAT JSON) SELECT execution_id FROM query_history.executions WHERE expires_at<=statement_timestamp() ORDER BY expires_at,execution_id LIMIT 1 FOR UPDATE SKIP LOCKED").fetch_one(&mut *plan_tx).await.unwrap();
    assert!(plan.to_string().contains("executions_expiry"));
    plan_tx.rollback().await.unwrap();

    assert!(f.reader.list("tenant-a", 100).await.unwrap().is_empty());
    assert!(f
        .reader
        .detail("tenant-a", finished.receipt.execution_id())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        f.writer.persist_pending(&pending).await.err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
    assert_eq!(
        f.writer
            .finish(&finished.receipt, &terminal(Outcome::Success))
            .await
            .err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 2); // hidden != physically purged
    for item in [&pending, &finished] {
        f.writer.heartbeat(item.owner_uuid).await.unwrap();
    }
    sqlx::query(
        "UPDATE query_history.owners SET heartbeat_at=clock_timestamp()-interval '744 hours'",
    )
    .execute(&f.admin)
    .await
    .unwrap();
    let live_owner = Uuid::new_v4();
    f.writer.heartbeat(live_owner).await.unwrap();
    sqlx::query("WITH t AS MATERIALIZED (SELECT clock_timestamp()-interval '1 second' AS expiry) INSERT INTO query_history.coverage(gap_id,tenant,received_at,expires_at,started_at,reason) SELECT $1,'tenant-a',expiry-interval '720 hours',expiry,expiry-interval '720 hours','unknown_crash' FROM t").bind(Uuid::new_v4()).execute(&f.admin).await.unwrap();
    // Failure AFTER a primary delete must roll the whole sweep transaction back.
    assert!(sqlx::query(
        "UPDATE query_history.coverage SET expires_at=expires_at+interval '1 second'"
    )
    .execute(&f.admin)
    .await
    .is_err());
    sqlx::query("REVOKE DELETE ON query_history.coverage FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert_eq!(
        f.writer.purge_expired(1).await.err(),
        Some(HistoryError::Unavailable)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM query_history.executions")
            .fetch_one(&f.admin)
            .await
            .unwrap(),
        2
    );
    sqlx::query("GRANT DELETE ON query_history.coverage TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    sqlx::query("REVOKE DELETE ON query_history.executions FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert_eq!(
        f.writer.purge_expired(1).await.err(),
        Some(HistoryError::Unavailable)
    );
    sqlx::query("GRANT DELETE ON query_history.executions TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let restarted = HistoryWriter::connect(options(&f.socket, "qh_writer"))
        .await
        .unwrap();
    let (a, b) = tokio::join!(restarted.purge_expired(1), f.writer.purge_expired(1));
    let deleted = a.unwrap() + b.unwrap();
    assert!((1..=2).contains(&deleted));
    assert_eq!(restarted.purge_expired(1000).await.unwrap(), 2 - deleted);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_history.executions")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 0); // privileged TEST primary inspection, not read filtering
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM query_history.coverage")
            .fetch_one(&f.admin)
            .await
            .unwrap(),
        0
    );
    let owners: Vec<Uuid> = sqlx::query_scalar("SELECT owner_uuid FROM query_history.owners")
        .fetch_all(&f.admin)
        .await
        .unwrap();
    assert_eq!(owners, vec![live_owner]);
    assert_eq!(
        restarted
            .finish_pending(&pending, &terminal(Outcome::Rejected))
            .await
            .err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
    assert_eq!(
        restarted.persist_pending(&finished).await.err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM query_history.executions")
            .fetch_one(&f.admin)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_schema_rejects_invalid_or_oversized_values() {
    let f = fixture().await;
    let event = event(&f, "tenant-a", "SELECT 1").await;
    f.writer.persist_pending(&event).await.unwrap();
    for change in [
        "estimated_bytes_scanned=-1",
        "measured_rows_scanned=-1",
        "duration_ms=-1",
        "returned_rows=9223372036854775808",
        "issuer=repeat('x',2049)",
        "client_id=repeat('x',129)",
        "redacted_sql=repeat('x',2049)",
        "tenant='active'",
        "expires_at=expires_at+interval '1 second'",
        "outcome='success'",
        "observation_state='terminal'",
        "truncated=true",
        "error_code='arbitrary upstream text'",
    ] {
        // Only constant test-controlled SQL, never application/user input.
        assert!(
            sqlx::query(&format!("UPDATE query_history.executions SET {change}"))
                .execute(&f.admin)
                .await
                .is_err()
        );
    }
    let oversized = sqlx::query("UPDATE query_history.executions SET issuer=repeat(chr(1),2048),client_id='fixture-client',principal_kind='provider'").execute(&f.admin).await.unwrap_err();
    assert!(
        matches!(oversized, sqlx::Error::Database(error) if error.message()=="history payload limit")
    );
    let row = json_row(&f, event.receipt.execution_id()).await;
    assert_eq!(row["observation_state"], "pending");
    assert_eq!(row["outcome"], serde_json::Value::Null);
    assert!(HistoryWriter::connect(options(&f.socket, "qh_owner"))
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "requires own disposable Postgres runner"]
async fn postgres_success_recovery_after_persistence_outage_keeps_original_receipt() {
    let f = fixture().await;
    // Trusted receipt exists before the outage; no invented recovery-time receipt.
    let pending = event(&f, "tenant-a", "SELECT 'SECRET_RECOVERY'").await;
    let original_id = pending.receipt.execution_id();
    let original_expiry = pending.receipt.expires_at();
    sqlx::query("REVOKE INSERT,UPDATE ON query_history.executions FROM qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    assert_eq!(
        f.writer.persist_pending(&pending).await.err(),
        Some(HistoryError::Unavailable)
    );
    assert_eq!(
        f.writer.admit(&pending.receipt).await.err(),
        Some(HistoryError::Unavailable)
    );
    sqlx::query("GRANT INSERT,UPDATE ON query_history.executions TO qh_writer")
        .execute(&f.admin)
        .await
        .unwrap();
    let success = terminal(Outcome::Success); // explicitly AdmissionEvidence::Admitted
    f.writer.finish_pending(&pending, &success).await.unwrap();
    let row = json_row(&f, original_id).await;
    assert_eq!(row["execution_admitted"], true);
    assert_eq!(row["admitted_at"], serde_json::Value::Null);
    assert_eq!(row["finished_at"], serde_json::Value::Null);
    assert!(row["admission_observed_at"].is_string());
    assert!(row["finished_observed_at"].is_string());
    let persisted_expiry: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT expires_at FROM query_history.executions WHERE execution_id=$1")
            .bind(original_id)
            .fetch_one(&f.admin)
            .await
            .unwrap();
    assert_eq!(persisted_expiry, original_expiry);
    f.writer.finish_pending(&pending, &success).await.unwrap();
    assert_eq!(json_row(&f, original_id).await, row);
    assert_eq!(
        f.writer
            .finish_pending(&pending, &terminal(Outcome::Cancelled))
            .await
            .err(),
        Some(HistoryError::Conflict)
    );

    let mut expires_during_outage = event(&f, "tenant-a", "SELECT 42").await;
    f.writer
        .persist_pending(&expires_during_outage)
        .await
        .unwrap();
    expire(&f, &mut expires_during_outage).await;
    assert_eq!(
        f.writer
            .finish_pending(&expires_during_outage, &success)
            .await
            .err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
    f.writer.purge_expired(1000).await.unwrap();
    assert_eq!(
        f.writer
            .finish_pending(&expires_during_outage, &success)
            .await
            .err(),
        Some(HistoryError::ExpiredOrAbsent)
    );
}
