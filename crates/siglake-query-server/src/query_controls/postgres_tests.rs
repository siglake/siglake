//! ONLY scripts/check-query-policy.sh's newly owned marked disposable cluster.
use super::*;
use sqlx::{postgres::PgPoolOptions, ConnectOptions, Connection, PgConnection, PgPool, Row};
use std::{path::PathBuf, time::Duration};
use tokio::{
    sync::{Mutex, MutexGuard},
    time::timeout,
};
static LOCK: Mutex<()> = Mutex::const_new(());
struct Fixture {
    _guard: MutexGuard<'static, ()>,
    socket: PathBuf,
    admin: PgPool,
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
    for relative in [
        "home/.aws/config",
        "home/.aws/credentials",
        "home/.boto",
        "home/.config/gcloud/application_default_credentials.json",
        "home/.azure/accessTokens.json",
        "home/.azure/azureProfile.json",
        "home/.kube/config",
    ] {
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

async fn fixture() -> Fixture {
    let guard = LOCK.lock().await;
    let socket = PathBuf::from(
        std::env::var("SIGLAKE_POLICY_FIXTURE_DIR")
            .expect("use scripts/check-query-policy.sh; missing setup is a FAILURE"),
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
    assert_fixture_isolation(&socket);
    let admin = PgPoolOptions::new()
        .max_connections(3)
        .connect_with(options(&socket, "fixture_admin"))
        .await
        .unwrap();
    sqlx::query("TRUNCATE query_policy.agent_limits,query_policy.edits")
        .execute(&admin)
        .await
        .unwrap();
    Fixture {
        _guard: guard,
        socket,
        admin,
    }
}
fn options(socket: &std::path::Path, role: &str) -> sqlx::postgres::PgConnectOptions {
    assert_fixture_isolation(socket);
    sqlx::postgres::PgConnectOptions::new_without_pgpass()
        .host(socket.to_str().unwrap())
        .socket(socket)
        .port(5432)
        .username(role)
        .password("")
        .database("qh_fixture")
        .ssl_mode(sqlx::postgres::PgSslMode::Disable)
        .application_name("siglake-query-policy")
        .disable_statement_logging()
}
fn config(f: &Fixture, role: Role) -> PolicyConfig {
    let uri = format!(
        "postgresql://{}:@{}:5432/qh_fixture?sslmode=disable",
        role.name(),
        f.socket.to_str().unwrap().replace('/', "%2F")
    );
    match role {
        Role::Reader => PolicyConfig::reader(&uri, bounds()).unwrap(),
        Role::Manager => PolicyConfig::manager(&uri, bounds()).unwrap(),
    }
}
fn bounds() -> ConnectionBounds {
    ConnectionBounds {
        max_connections: 2,
        operation_timeout: Duration::from_secs(2),
    }
}
fn key(tenant: &str, issuer: &str, client: &str) -> RuntimeKey {
    RuntimeKey::from_trusted_metadata(tenant, issuer, client).unwrap()
}
fn binding(key: RuntimeKey) -> ManagementBinding {
    ManagementBinding::from_trusted_metadata(key, Uuid::new_v4(), Uuid::new_v4()).unwrap()
}
fn actor() -> VerifiedHumanActor {
    VerifiedHumanActor::from_trusted_metadata("synthetic-verified-human").unwrap()
}
fn save(expected: i64, seconds: Option<i64>, warning: Option<i64>) -> SaveRequest {
    SaveRequest::new(expected, SavedSettings::new(seconds, warning).unwrap()).unwrap()
}
async fn sql(f: &Fixture, text: &str) {
    sqlx::raw_sql(text).execute(&f.admin).await.unwrap();
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn primary_no_cache_cas_reset_exact_identity_binding_and_safe_audit() {
    let f = fixture().await;
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("tenant_a", "https://issuer.invalid/", "same-client"));
    assert_eq!(reader.lookup(&b.key).await.unwrap(), PolicyLookup::Absent);
    assert_eq!(reader.lookup(&b.key).await.unwrap(), PolicyLookup::Absent);
    let one = manager
        .save(&b, &actor(), save(0, Some(12), Some(200)))
        .await
        .unwrap();
    assert_eq!(one.revision(), 1);
    assert_eq!(
        reader.lookup(&b.key).await.unwrap(),
        PolicyLookup::Found(one)
    );
    let mut committed = reader.pool.acquire().await.unwrap();
    assert!(tenant(&mut committed).await.is_none_or(|s| s.is_empty()));
    committed.complete().await.unwrap();
    drop(committed);
    assert_eq!(
        manager.save(&b, &actor(), save(0, Some(13), None)).await,
        Err(PolicyError::Conflict)
    );
    let two = manager
        .save(&b, &actor(), save(1, None, Some(250)))
        .await
        .unwrap();
    assert_eq!(two.revision(), 2);
    let warning_only = reader.lookup(&b.key).await.unwrap();
    assert_eq!(warning_only, PolicyLookup::Found(two));
    assert_eq!(warning_only.state(), PolicyState::Applied);
    assert_eq!(warning_only.revision(), Some(2));
    let reset = manager
        .save(&b, &actor(), save(2, None, None))
        .await
        .unwrap();
    assert_eq!(reset.revision(), 3);
    assert_eq!(
        manager.lookup(&b).await.unwrap(),
        PolicyLookup::Found(reset)
    );
    let reset_lookup = reader.lookup(&b.key).await.unwrap();
    assert_eq!(reset_lookup, PolicyLookup::Found(reset));
    assert_eq!(reset_lookup.state(), PolicyState::Inherited);
    assert_eq!(reset_lookup.revision(), Some(3));
    // A synthetic finite operator value tests serialization correspondence only;
    // this does not introduce a deployment default or an effective resolver.
    let encoded = EffectiveQueryControls::new(ControlsInput {
        state: reset_lookup.state(),
        policy_revision: reset_lookup.revision(),
        operator_generation: 1,
        priority: ControlPriority::Interactive,
        mode: SelectedMode::Unknown,
        scope: ControlScope::Unknown,
        mandatory_timeout: true,
        effective_timeout_ms: Some(1000),
        effective_output_rows: None,
        output_cap_enforced: false,
        scan_bytes_warning: reset.settings().scan_bytes_warning(),
        estimate_status: EstimateStatus::Unavailable,
        estimate_exact: None,
        estimated_bytes: None,
        warnings: WarningSet::default(),
    })
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&encoded.json().unwrap()).unwrap();
    assert_eq!(json["state"], "inherited");
    assert_eq!(json["policy_revision"], 3);
    assert!(encoded
        .headers()
        .unwrap()
        .controls
        .contains("state=inherited;rev=3;"));
    let audit=sqlx::query("SELECT revision,actor_sub,old_timeout_seconds,old_scan_bytes_warning,new_timeout_seconds,new_scan_bytes_warning FROM query_policy.edits ORDER BY revision").fetch_all(&f.admin).await.unwrap();
    assert_eq!(audit.len(), 3);
    assert_eq!(audit[0].get::<Option<i64>, _>("old_timeout_seconds"), None);
    assert_eq!(
        audit[0].get::<Option<i64>, _>("new_timeout_seconds"),
        Some(12)
    );
    assert_eq!(
        audit[1].get::<Option<i64>, _>("old_scan_bytes_warning"),
        Some(200)
    );
    assert_eq!(audit[2].get::<Option<i64>, _>("old_timeout_seconds"), None);
    assert_eq!(
        audit[2].get::<Option<i64>, _>("old_scan_bytes_warning"),
        Some(250)
    );
    assert_eq!(audit[2].get::<Option<i64>, _>("new_timeout_seconds"), None);
    assert_eq!(
        audit[2].get::<&str, _>("actor_sub"),
        "synthetic-verified-human"
    );
    let other = binding(b.key.clone());
    assert_eq!(
        manager.save(&other, &actor(), save(3, Some(1), None)).await,
        Err(PolicyError::Binding)
    );
    assert_eq!(manager.lookup(&other).await, Err(PolicyError::Binding));
    for k in [
        key("tenant_b", "https://issuer.invalid/", "same-client"),
        key("tenant_a", "https://issuer.invalid", "same-client"),
        key("tenant_a", "https://issuer.invalid/", "other-client"),
    ] {
        assert_eq!(reader.lookup(&k).await.unwrap(), PolicyLookup::Absent);
        let remap =
            ManagementBinding::from_trusted_metadata(k.clone(), b.org, b.credential).unwrap();
        assert_eq!(
            manager.save(&remap, &actor(), save(0, None, None)).await,
            Err(PolicyError::Binding)
        );
        let independent = binding(k);
        assert_eq!(
            manager
                .save(&independent, &actor(), save(0, None, None))
                .await
                .unwrap()
                .revision(),
            1
        );
    }
    // Primary errors and unsafe schema NEVER become inherited, even after a miss.
    let missing = key("missing", "issuer", "client");
    assert_eq!(reader.lookup(&missing).await.unwrap(), PolicyLookup::Absent);
    sql(
        &f,
        "ALTER TABLE query_policy.agent_limits NO FORCE ROW LEVEL SECURITY",
    )
    .await;
    assert_eq!(reader.lookup(&missing).await, Err(PolicyError::Unsafe));
    sql(
        &f,
        "ALTER TABLE query_policy.agent_limits FORCE ROW LEVEL SECURITY",
    )
    .await;
    assert_eq!(reader.lookup(&missing).await.unwrap(), PolicyLookup::Absent);
    sql(
        &f,
        "ALTER TABLE query_policy.agent_limits RENAME TO unavailable_limits",
    )
    .await;
    assert!(matches!(
        reader.lookup(&missing).await,
        Err(PolicyError::Unavailable)
    ));
    sql(
        &f,
        "ALTER TABLE query_policy.unavailable_limits RENAME TO agent_limits",
    )
    .await;
    reader.close();
    assert_eq!(reader.lookup(&missing).await, Err(PolicyError::Unavailable));
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn concurrent_create_update_and_all_null_reset_are_single_winner_cas() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("tenant", "issuer", "client"));
    let a = actor();
    for expected in 0..3 {
        let request = save(expected, if expected == 2 { None } else { Some(10) }, None);
        let (left, right) =
            tokio::join!(manager.save(&b, &a, request), manager.save(&b, &a, request));
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(left == Err(PolicyError::Conflict) || right == Err(PolicyError::Conflict));
        let PolicyLookup::Found(saved) = manager.lookup(&b).await.unwrap() else {
            panic!("committed policy absent")
        };
        assert_eq!(saved.revision(), expected + 1);
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_policy.edits")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn reader_manager_history_grants_and_transaction_local_exact_tenant_predicates() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let a = binding(key("a", "issuer", "client"));
    let b = binding(key("b", "issuer", "client"));
    manager
        .save(&a, &actor(), save(0, None, None))
        .await
        .unwrap();
    manager
        .save(&b, &actor(), save(0, None, None))
        .await
        .unwrap();
    for role in ["qp_reader", "qp_manager", "qh_reader", "qh_writer"] {
        let mut c = PgConnection::connect_with(&options(&f.socket, role))
            .await
            .unwrap();
        for statement in [
            "SELECT * FROM query_policy.edits",
            "UPDATE query_policy.agent_limits SET timeout_seconds=10",
            "INSERT INTO query_policy.edits DEFAULT VALUES",
            "DELETE FROM query_policy.agent_limits",
        ] {
            assert!(
                sqlx::query(statement).execute(&mut c).await.is_err(),
                "unexpected direct policy/audit authority"
            );
        }
        if role != "qp_manager" {
            assert!(sqlx::query("SELECT query_policy.save('a','issuer','client',gen_random_uuid(),gen_random_uuid(),'human',0,NULL,NULL)").execute(&mut c).await.is_err());
        }
        if role.starts_with("qh_") {
            assert!(sqlx::query("SELECT * FROM query_policy.agent_limits")
                .execute(&mut c)
                .await
                .is_err());
            continue;
        }
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_policy.agent_limits")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(n, 0);
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
            .execute(&mut c)
            .await
            .unwrap();
        let n:i64=sqlx::query_scalar("SELECT count(*) FROM query_policy.agent_limits WHERE tenant='b' AND verified_issuer='issuer' AND verified_client_id='client'").fetch_one(&mut c).await.unwrap();
        assert_eq!(n, 0);
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_policy.agent_limits")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(n, 1);
        sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
        let tenant: Option<String> =
            sqlx::query_scalar("SELECT current_setting('query_policy.tenant',true)")
                .fetch_one(&mut c)
                .await
                .unwrap();
        assert!(tenant.is_none_or(|s| s.is_empty()));
    }
    let mut c = PgConnection::connect_with(&options(&f.socket, "qp_manager"))
        .await
        .unwrap();
    // Choosing the GUC is not authenticated tenant authority; definer still checks
    // supplied binding against that exact transaction tenant, even as owner.
    sqlx::query("BEGIN").execute(&mut c).await.unwrap();
    sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
        .execute(&mut c)
        .await
        .unwrap();
    let e =
        sqlx::query("SELECT query_policy.save('b','issuer','client',$1,$2,'human',1,NULL,NULL)")
            .bind(b.org)
            .bind(b.credential)
            .execute(&mut c)
            .await
            .unwrap_err();
    assert_eq!(
        e.as_database_error().unwrap().code().as_deref(),
        Some("QP003")
    );
    sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
    // Owner trigger prevents out-of-band rebinding (fixture admin is trusted).
    assert!(sqlx::query(
        "UPDATE query_policy.agent_limits SET org_uuid=gen_random_uuid(),revision=revision+1"
    )
    .execute(&f.admin)
    .await
    .is_err());
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn checkout_rejects_role_schema_table_column_function_ownership_membership_and_tamper() {
    let f = fixture().await;
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("tenant", "issuer", "client"));
    sql(&f,"CREATE SCHEMA \"pgXfixture\"; REVOKE ALL ON SCHEMA \"pgXfixture\" FROM PUBLIC; CREATE TABLE \"pgXfixture\".secrets(value text); CREATE FUNCTION app_catalog.fn() RETURNS integer LANGUAGE sql AS 'SELECT 1'; REVOKE ALL ON FUNCTION app_catalog.fn() FROM PUBLIC").await;
    let cases=[
      ("ALTER ROLE qp_reader BYPASSRLS","ALTER ROLE qp_reader NOBYPASSRLS"),
      ("ALTER ROLE qp_reader SUPERUSER","ALTER ROLE qp_reader NOSUPERUSER"),
      ("ALTER ROLE qp_reader INHERIT","ALTER ROLE qp_reader NOINHERIT"),
      ("GRANT fixture_other TO qp_reader","REVOKE fixture_other FROM qp_reader"),
      ("GRANT qp_reader TO fixture_other","REVOKE qp_reader FROM fixture_other"),
      ("GRANT qp_owner TO fixture_other","REVOKE qp_owner FROM fixture_other"),
      ("GRANT USAGE ON SCHEMA query_policy TO qh_reader","REVOKE USAGE ON SCHEMA query_policy FROM qh_reader"),
      ("GRANT CREATE ON SCHEMA query_policy TO PUBLIC","REVOKE CREATE ON SCHEMA query_policy FROM PUBLIC"),
      ("GRANT SELECT(actor_sub) ON query_policy.edits TO qp_manager","REVOKE SELECT(actor_sub) ON query_policy.edits FROM qp_manager"),
      ("GRANT qp_owner TO qp_manager","REVOKE qp_owner FROM qp_manager"),
      ("ALTER ROLE qp_owner LOGIN","ALTER ROLE qp_owner NOLOGIN"),
      ("ALTER ROLE qp_owner BYPASSRLS","ALTER ROLE qp_owner NOBYPASSRLS"),
      ("GRANT fixture_other TO qp_owner","REVOKE fixture_other FROM qp_owner"),
      ("GRANT TEMP ON DATABASE qh_fixture TO qp_reader","REVOKE TEMP ON DATABASE qh_fixture FROM qp_reader"),
      ("GRANT CREATE ON SCHEMA app_catalog TO qp_reader","REVOKE CREATE ON SCHEMA app_catalog FROM qp_reader"),
      ("GRANT SELECT ON app_catalog.secrets TO qp_reader","REVOKE SELECT ON app_catalog.secrets FROM qp_reader"),
      ("GRANT SELECT(value) ON app_catalog.secrets TO qp_reader","REVOKE SELECT(value) ON app_catalog.secrets FROM qp_reader"),
      ("GRANT INSERT(value) ON app_catalog.secrets TO qp_manager","REVOKE INSERT(value) ON app_catalog.secrets FROM qp_manager"),
      ("GRANT SELECT ON \"pgXfixture\".secrets TO qp_reader","REVOKE SELECT ON \"pgXfixture\".secrets FROM qp_reader"),
      ("GRANT EXECUTE ON FUNCTION app_catalog.fn() TO qp_reader","REVOKE EXECUTE ON FUNCTION app_catalog.fn() FROM qp_reader"),
      ("ALTER TABLE app_catalog.secrets OWNER TO qp_reader","ALTER TABLE app_catalog.secrets OWNER TO fixture_admin"),
      ("GRANT UPDATE ON query_policy.agent_limits TO qp_manager","REVOKE UPDATE ON query_policy.agent_limits FROM qp_manager"),
      ("GRANT UPDATE(timeout_seconds) ON query_policy.agent_limits TO qp_manager","REVOKE UPDATE(timeout_seconds) ON query_policy.agent_limits FROM qp_manager"),
      ("GRANT SELECT(actor_sub) ON query_policy.edits TO qp_reader","REVOKE SELECT(actor_sub) ON query_policy.edits FROM qp_reader"),
      ("GRANT EXECUTE ON FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) TO qp_reader","REVOKE EXECUTE ON FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) FROM qp_reader"),
      ("GRANT EXECUTE ON FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) TO PUBLIC","REVOKE EXECUTE ON FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) FROM PUBLIC"),
      ("GRANT SELECT ON query_policy.agent_limits TO qh_reader","REVOKE SELECT ON query_policy.agent_limits FROM qh_reader"),
      ("GRANT UPDATE(timeout_seconds) ON query_policy.agent_limits TO qh_writer","REVOKE UPDATE(timeout_seconds) ON query_policy.agent_limits FROM qh_writer"),
      ("ALTER TABLE query_policy.agent_limits NO FORCE ROW LEVEL SECURITY","ALTER TABLE query_policy.agent_limits FORCE ROW LEVEL SECURITY"),
      ("ALTER TABLE query_policy.edits NO FORCE ROW LEVEL SECURITY","ALTER TABLE query_policy.edits FORCE ROW LEVEL SECURITY"),
      ("ALTER TABLE query_policy.agent_limits DISABLE ROW LEVEL SECURITY","ALTER TABLE query_policy.agent_limits ENABLE ROW LEVEL SECURITY"),
      ("ALTER FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) SET search_path=public","ALTER FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) SET search_path=pg_catalog"),
      ("ALTER FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) SECURITY INVOKER","ALTER FUNCTION query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint) SECURITY DEFINER"),
      ("ALTER TABLE query_policy.agent_limits DISABLE TRIGGER immutable_binding","ALTER TABLE query_policy.agent_limits ENABLE TRIGGER immutable_binding"),
      ("ALTER POLICY tenant_scope ON query_policy.edits USING (true)","ALTER POLICY tenant_scope ON query_policy.edits USING (tenant=current_setting('query_policy.tenant',true))"),
      ("ALTER TABLE query_policy.agent_limits ADD CONSTRAINT tamper CHECK (revision<100)","ALTER TABLE query_policy.agent_limits DROP CONSTRAINT tamper"),
      ("ALTER TABLE query_policy.agent_limits ALTER COLUMN timeout_seconds TYPE numeric","ALTER TABLE query_policy.agent_limits DROP CONSTRAINT agent_limits_timeout_seconds_check; ALTER TABLE query_policy.agent_limits ALTER COLUMN timeout_seconds TYPE bigint; ALTER TABLE query_policy.agent_limits ADD CONSTRAINT agent_limits_timeout_seconds_check CHECK (timeout_seconds > 0 AND timeout_seconds <= 9223372036854775)"),
      ("ALTER TABLE query_policy.edits OWNER TO qp_manager","ALTER TABLE query_policy.edits OWNER TO qp_owner"),
      ("GRANT EXECUTE ON FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) TO qp_reader","REVOKE EXECUTE ON FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) FROM qp_reader"),
      ("GRANT EXECUTE ON FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) TO PUBLIC","REVOKE EXECUTE ON FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) FROM PUBLIC"),
      ("GRANT EXECUTE ON FUNCTION query_policy.purge_expired_edits(text,integer) TO qp_manager","REVOKE EXECUTE ON FUNCTION query_policy.purge_expired_edits(text,integer) FROM qp_manager"),
      ("GRANT EXECUTE ON FUNCTION query_policy.purge_expired_edits(text,integer) TO PUBLIC","REVOKE EXECUTE ON FUNCTION query_policy.purge_expired_edits(text,integer) FROM PUBLIC"),
      ("GRANT UPDATE(expires_at) ON query_policy.edits TO qp_manager","REVOKE UPDATE(expires_at) ON query_policy.edits FROM qp_manager"),
      ("ALTER TABLE query_policy.edits DISABLE TRIGGER guard_edit","ALTER TABLE query_policy.edits ENABLE TRIGGER guard_edit"),
      ("ALTER FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) SET search_path=public","ALTER FUNCTION query_policy.read_edits(text,text,text,uuid,uuid,bigint,integer) SET search_path=pg_catalog"),
      ("ALTER FUNCTION query_policy.purge_expired_edits(text,integer) SECURITY DEFINER","ALTER FUNCTION query_policy.purge_expired_edits(text,integer) SECURITY INVOKER"),
      ("ALTER TABLE query_policy.edits DROP CONSTRAINT edits_retention_check","ALTER TABLE query_policy.edits ADD CONSTRAINT edits_retention_check CHECK (expires_at-edited_at=interval '720 hours')"),
      ("CREATE FUNCTION query_policy.unexpected() RETURNS integer LANGUAGE sql AS 'SELECT 1'; REVOKE ALL ON FUNCTION query_policy.unexpected() FROM PUBLIC","DROP FUNCTION query_policy.unexpected()"),
    ];
    for (change, undo) in cases {
        sql(&f, change).await;
        let r = reader.lookup(&b.key).await;
        let m = manager.lookup(&b).await;
        // Require rejection by the affected login, not merely its peer.
        let affected = if change.contains("qp_manager") { m } else { r };
        assert_eq!(
            affected,
            Err(PolicyError::Unsafe),
            "unsafe contract accepted: {change}"
        );
        sql(&f, undo).await;
        assert_eq!(
            reader.lookup(&b.key).await,
            Ok(PolicyLookup::Absent),
            "reader after undo: {change}"
        );
        assert_eq!(
            manager.lookup(&b).await,
            Ok(PolicyLookup::Absent),
            "manager after undo: {change}"
        );
    }
    let original:String=sqlx::query_scalar("SELECT pg_get_functiondef('query_policy.save(text,text,text,uuid,uuid,text,bigint,bigint,bigint)'::regprocedure)").fetch_one(&f.admin).await.unwrap();
    sql(&f,"CREATE OR REPLACE FUNCTION query_policy.save(p_tenant text,p_issuer text,p_client text,p_org uuid,p_credential uuid,p_actor text,p_expected bigint,p_timeout bigint,p_warning bigint) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS 'BEGIN RETURN 99; END'").await;
    assert_eq!(reader.lookup(&b.key).await, Err(PolicyError::Unsafe));
    assert_eq!(manager.lookup(&b).await, Err(PolicyError::Unsafe));
    sql(&f, &original).await;
    assert_eq!(reader.lookup(&b.key).await.unwrap(), PolicyLookup::Absent);
    sql(
        &f,
        "DROP SCHEMA \"pgXfixture\" CASCADE; DROP FUNCTION app_catalog.fn()",
    )
    .await;
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn audit_failure_rolls_back_first_and_update_policy_atomically() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let b = binding(key("tenant", "issuer", "client"));
    // Deliberately preoccupy the audit revision; policy schema remains canonical.
    // A real INSERT failure inside save must undo its earlier policy INSERT.
    sqlx::query("INSERT INTO query_policy.edits (tenant,org_uuid,credential_uuid,schema_version,revision,actor_sub) VALUES('tenant',$1,$2,1,1,'fixture-human')").bind(b.org).bind(b.credential).execute(&f.admin).await.unwrap();
    assert_eq!(
        manager
            .save(&b, &actor(), save(0, Some(10), Some(20)))
            .await,
        Err(PolicyError::Unavailable)
    );
    assert_eq!(reader.lookup(&b.key).await.unwrap(), PolicyLookup::Absent);
    sql(&f, "TRUNCATE query_policy.edits").await;
    let one = manager
        .save(&b, &actor(), save(0, Some(10), Some(20)))
        .await
        .unwrap();
    sqlx::query("INSERT INTO query_policy.edits (tenant,org_uuid,credential_uuid,schema_version,revision,actor_sub) VALUES('tenant',$1,$2,1,2,'fixture-human')").bind(b.org).bind(b.credential).execute(&f.admin).await.unwrap();
    assert_eq!(
        manager.save(&b, &actor(), save(1, None, None)).await,
        Err(PolicyError::Unavailable)
    );
    assert_eq!(
        reader.lookup(&b.key).await.unwrap(),
        PolicyLookup::Found(one)
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_policy.edits")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 2);
}
async fn pid(c: &mut PgConnection) -> i32 {
    sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(c)
        .await
        .unwrap()
}
async fn tenant(c: &mut PgConnection) -> Option<String> {
    sqlx::query_scalar("SELECT current_setting('query_policy.tenant',true)")
        .fetch_one(c)
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn owned_capacity_reuse_cancel_unfinished_transactions_tenant_cleanup_and_close() {
    let f = fixture().await;
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let pool = &reader.pool;
    let mut lease = pool.acquire().await.unwrap();
    let original = pid(&mut lease).await;
    let mut tx = lease.begin().await.unwrap();
    sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(tenant(&mut tx).await.as_deref(), Some("a"));
    tx.rollback().await.unwrap();
    lease.complete().await.unwrap();
    drop(lease);
    let mut lease = pool.acquire().await.unwrap();
    assert_eq!(pid(&mut lease).await, original);
    assert!(tenant(&mut lease).await.is_none_or(|s| s.is_empty()));
    let second = pool.acquire().await.unwrap();
    assert!(timeout(Duration::from_millis(30), pool.acquire())
        .await
        .is_err());
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='siglake-query-policy' AND usename='qp_reader'").fetch_one(&f.admin).await.unwrap();
    assert_eq!(count, 2);
    // Even a deliberately forgotten borrowed transaction cannot be reusable.
    // The library itself always uses tracked begin_with, never raw SQL BEGIN.
    let mut unfinished = lease.begin().await.unwrap();
    sqlx::query("SELECT set_config('query_policy.tenant','leaked',true)")
        .execute(&mut *unfinished)
        .await
        .unwrap();
    std::mem::forget(unfinished);
    assert_eq!(lease.complete().await, Err(PolicyError::Unavailable));
    drop(lease);
    drop(second);
    assert_eq!(pool.idle_count(), 0);
    let mut lease = pool.acquire().await.unwrap();
    assert_ne!(pid(&mut lease).await, original);
    assert!(tenant(&mut lease).await.is_none_or(|s| s.is_empty()));
    let mut tx = lease.begin().await.unwrap();
    sqlx::query("SELECT set_config('statement_timeout','100ms',true),set_config('query_policy.tenant','cancelled',true)").execute(&mut *tx).await.unwrap();
    assert!(timeout(
        Duration::from_millis(20),
        sqlx::query("SELECT pg_sleep(1)").execute(&mut *tx)
    )
    .await
    .is_err());
    drop(tx); // queued rollback is discarded with the owned raw connection
    drop(lease);
    assert_eq!(pool.idle_count(), 0);
    let mut clean = pool.acquire().await.unwrap();
    assert!(tenant(&mut clean).await.is_none_or(|s| s.is_empty()));
    clean.complete().await.unwrap();
    drop(clean);
    assert_eq!(pool.idle_count(), 1);
    // Current user alone is insufficient; session role also remains checked.
    let mut admin = PgConnection::connect_with(&options(&f.socket, "fixture_admin"))
        .await
        .unwrap();
    sqlx::query("SET ROLE qp_reader")
        .execute(&mut admin)
        .await
        .unwrap();
    assert_eq!(
        super::role::verify(&mut admin, Role::Reader).await,
        Err(PolicyError::Unsafe)
    );
    let held = pool.acquire().await.unwrap();
    reader.close();
    assert!(pool.acquire().await.is_err());
    drop(held);
    assert_eq!(pool.idle_count(), 0);
    assert!(PolicyReader::connect(config(&f, Role::Manager))
        .await
        .is_err());
    assert!(PolicyManager::connect(config(&f, Role::Reader))
        .await
        .is_err());
    timeout(Duration::from_secs(3), async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE application_name='siglake-query-policy' AND usename='qp_reader'").fetch_one(&f.admin).await.unwrap();
            if count == 0 { break; }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("owned sockets did not close");
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn read_and_save_cancellation_fail_closed_without_tenant_reuse() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let b = binding(key("tenant", "issuer", "client"));
    let one = manager
        .save(&b, &actor(), save(0, Some(10), None))
        .await
        .unwrap();
    // Block data I/O AFTER checkout; cancelling the caller must discard its
    // transaction rather than return a connection with a tenant GUC or rollback.
    let mut guard = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *guard)
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(100), reader.lookup(&b.key))
        .await
        .is_err());
    assert!(timeout(
        Duration::from_millis(100),
        manager.save(&b, &actor(), save(1, None, None))
    )
    .await
    .is_err());
    guard.rollback().await.unwrap();
    assert_eq!(reader.pool.idle_count(), 0);
    assert_eq!(manager.pool.idle_count(), 0);
    assert_eq!(
        reader.lookup(&b.key).await.unwrap(),
        PolicyLookup::Found(one)
    );
    let absent = key("other", "issuer", "client");
    assert_eq!(reader.lookup(&absent).await.unwrap(), PolicyLookup::Absent);
    // Typed transport/read unavailability, no silent inherited result.
    let mut guard = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_policy.agent_limits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *guard)
        .await
        .unwrap();
    assert_eq!(reader.lookup(&absent).await, Err(PolicyError::Unavailable));
    guard.rollback().await.unwrap();
}

/// Synthetic wire relay: withholds COMMIT acknowledgement, not a store mock.
/// No packets are logged/retained. Both network directions belong to this future.
async fn ambiguous_commit_relay(
    listener: tokio::net::UnixListener,
    upstream: PathBuf,
    forward: bool,
) {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (mut client, _) = listener.accept().await.unwrap();
    let mut server = tokio::net::UnixStream::connect(upstream).await.unwrap();
    let mut length = [0; 4];
    client.read_exact(&mut length).await.unwrap();
    let size = u32::from_be_bytes(length) as usize;
    assert!((8..=65536).contains(&size));
    let mut startup = vec![0; size - 4];
    client.read_exact(&mut startup).await.unwrap();
    server.write_all(&length).await.unwrap();
    server.write_all(&startup).await.unwrap();
    let (mut cr, mut cw) = client.split();
    let (mut sr, mut sw) = server.split();
    let committing = Arc::new(AtomicBool::new(false));
    let observed = committing.clone();
    let outbound = async move {
        let mut commit_statement: Option<Vec<u8>> = None;
        loop {
            let mut tag = [0; 1];
            if cr.read_exact(&mut tag).await.is_err() {
                break;
            }
            let mut length = [0; 4];
            cr.read_exact(&mut length).await.unwrap();
            let size = u32::from_be_bytes(length) as usize;
            assert!((4..=65536).contains(&size));
            let mut payload = vec![0; size - 4];
            cr.read_exact(&mut payload).await.unwrap();
            let query = if tag[0] == b'P' {
                payload.split(|b| *b == 0).nth(1)
            } else if tag[0] == b'Q' {
                payload.split(|b| *b == 0).next()
            } else {
                None
            };
            if query == Some(b"COMMIT".as_slice()) {
                if !forward {
                    break;
                }
                if tag[0] == b'Q' {
                    committing.store(true, Ordering::SeqCst);
                } else {
                    // SQLx prepares before Bind/Execute. Let its preparation
                    // acknowledgement through; suppress only real execution.
                    commit_statement = payload.split(|b| *b == 0).next().map(<[u8]>::to_vec);
                }
            }
            if tag[0] == b'B'
                && commit_statement
                    .as_deref()
                    .is_some_and(|name| payload.split(|b| *b == 0).nth(1) == Some(name))
            {
                committing.store(true, Ordering::SeqCst);
            }
            sw.write_all(&tag).await.unwrap();
            sw.write_all(&length).await.unwrap();
            sw.write_all(&payload).await.unwrap();
        }
    };
    let inbound = async move {
        loop {
            let mut tag = [0; 1];
            if sr.read_exact(&mut tag).await.is_err() {
                break;
            }
            let mut length = [0; 4];
            sr.read_exact(&mut length).await.unwrap();
            let size = u32::from_be_bytes(length) as usize;
            assert!((4..=65536).contains(&size));
            let mut payload = vec![0; size - 4];
            sr.read_exact(&mut payload).await.unwrap();
            if observed.load(Ordering::SeqCst) {
                if tag[0] == b'Z' {
                    assert_eq!(payload, [b'I']);
                    break;
                }
                continue;
            }
            cw.write_all(&tag).await.unwrap();
            cw.write_all(&length).await.unwrap();
            cw.write_all(&payload).await.unwrap();
        }
    };
    tokio::select! {_=outbound=>{},_=inbound=>{}}
}
#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn lost_commit_acknowledgement_is_unknown_not_success_or_asserted_rollback() {
    let f = fixture().await;
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    for forward in [false, true] {
        let proxy = f.socket.join(if forward {
            "after-commit"
        } else {
            "before-commit"
        });
        std::fs::create_dir(&proxy).unwrap();
        let listener = tokio::net::UnixListener::bind(proxy.join(".s.PGSQL.5432")).unwrap();
        let upstream = f.socket.join(".s.PGSQL.5432");
        let task = tokio::spawn(ambiguous_commit_relay(listener, upstream, forward));
        struct Abort(tokio::task::AbortHandle);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _abort = Abort(task.abort_handle());
        let uri = format!(
            "postgresql://qp_manager:@{}:5432/qh_fixture?sslmode=disable",
            proxy.to_str().unwrap().replace('/', "%2F")
        );
        let manager = PolicyManager::connect(PolicyConfig::manager(&uri, bounds()).unwrap())
            .await
            .unwrap();
        let b = binding(key(
            "tenant",
            "issuer",
            if forward { "after" } else { "before" },
        ));
        let result = manager
            .save(&b, &actor(), save(0, Some(10), Some(20)))
            .await;
        timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, Err(PolicyError::Unavailable));
        let lookup = reader.lookup(&b.key).await.unwrap();
        if forward {
            let PolicyLookup::Found(p) = lookup else {
                panic!("server committed despite lost acknowledgement")
            };
            assert_eq!(p.revision(), 1);
        } else {
            assert_eq!(lookup, PolicyLookup::Absent);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM query_policy.edits WHERE org_uuid=$1 AND credential_uuid=$2",
        )
        .bind(b.org)
        .bind(b.credential)
        .fetch_one(&f.admin)
        .await
        .unwrap();
        assert_eq!(count, i64::from(forward));
        assert_eq!(manager.pool.idle_count(), 0);
    }
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn definer_rejects_invalid_values_even_without_browser_dto() {
    let f = fixture().await;
    let mut c = PgConnection::connect_with(&options(&f.socket, "qp_manager"))
        .await
        .unwrap();
    let org = Uuid::new_v4();
    let credential = Uuid::new_v4();
    let cases = [
        (
            org,
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            -1,
            None,
            None,
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            i64::MAX,
            None,
            None,
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            0,
            Some(0),
            None,
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            0,
            Some(MAX_TIMEOUT_SECONDS + 1),
            None,
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            0,
            None,
            Some(-1),
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "".to_owned(),
            0,
            None,
            None,
        ),
        (
            org,
            credential,
            "issuer".to_owned(),
            "x".repeat(257),
            0,
            None,
            None,
        ),
        (
            org,
            credential,
            "".to_owned(),
            "human".to_owned(),
            0,
            None,
            None,
        ),
        (
            Uuid::nil(),
            credential,
            "issuer".to_owned(),
            "human".to_owned(),
            0,
            None,
            None,
        ),
        (
            org,
            Uuid::nil(),
            "issuer".to_owned(),
            "human".to_owned(),
            0,
            None,
            None,
        ),
    ];
    for (org, credential, issuer, actor, expected, seconds, warning) in cases {
        sqlx::query("BEGIN").execute(&mut c).await.unwrap();
        sqlx::query("SELECT set_config('query_policy.tenant','tenant',true)")
            .execute(&mut c)
            .await
            .unwrap();
        let e = sqlx::query("SELECT query_policy.save('tenant',$1,'client',$2,$3,$4,$5,$6,$7)")
            .bind(issuer)
            .bind(org)
            .bind(credential)
            .bind(actor)
            .bind(expected)
            .bind(seconds)
            .bind(warning)
            .execute(&mut c)
            .await
            .unwrap_err();
        assert_eq!(
            e.as_database_error().unwrap().code().as_deref(),
            Some("QP003")
        );
        sqlx::query("ROLLBACK").execute(&mut c).await.unwrap();
    }
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("tenant", "issuer", "client"));
    let p = manager
        .save(
            &b,
            &actor(),
            save(0, Some(MAX_TIMEOUT_SECONDS), Some(i64::MAX)),
        )
        .await
        .unwrap();
    assert_eq!(p.settings().timeout_seconds(), Some(MAX_TIMEOUT_SECONDS));
    assert_eq!(p.settings().scan_bytes_warning(), Some(i64::MAX));
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn owned_late_error_cancellation_and_close_do_not_log_upstream_error_body() {
    use std::sync::{Arc, Mutex as StdMutex};
    let f = fixture().await;
    let captured = Arc::new(StdMutex::new(Vec::<u8>::new()));
    struct Sink(Arc<StdMutex<Vec<u8>>>);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut output = self.0.lock().unwrap();
            assert!(
                output.len() + bytes.len() <= 2 * 1024 * 1024,
                "bounded capture exceeded"
            );
            output.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let output = captured.clone();
    // Global in this selected fixture process, so an unexpected detached SQLx
    // return/close task cannot evade capture by switching runtime threads.
    tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || Sink(output.clone()))
        .try_init()
        .expect("isolated policy logger");
    tracing::warn!("fixed policy tracing probe");
    log::warn!("fixed policy logging probe");
    const SENTINEL: &str = "synthetic_policy_private_upstream_error";
    const DELAYED:&str="DO $$ BEGIN PERFORM pg_advisory_xact_lock(724918220196); RAISE EXCEPTION 'synthetic_policy_private_upstream_error'; END $$";
    async fn waiting(admin: &PgPool, pid: i32) {
        timeout(Duration::from_secs(3),async {loop {let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted AND locktype='advisory')").bind(pid).fetch_one(admin).await.unwrap();if blocked{break;}tokio::time::sleep(Duration::from_millis(1)).await;}}).await.expect("backend did not reach synthetic gate");
    }
    let mut gate = f.admin.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(724918220196)")
        .execute(&mut *gate)
        .await
        .unwrap();
    // Control proves a real late ErrorResponse exists, inspected only in memory.
    let mut control = PgConnection::connect_with(&options(&f.socket, "qp_reader"))
        .await
        .unwrap();
    let id = pid(&mut control).await;
    let (timed, ()) = tokio::join!(
        timeout(
            Duration::from_millis(100),
            sqlx::query(DELAYED).execute(&mut control)
        ),
        waiting(&f.admin, id)
    );
    assert!(timed.is_err());
    sqlx::query("SELECT pg_advisory_unlock(724918220196)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let late = timeout(Duration::from_secs(3), control.flush())
        .await
        .unwrap()
        .expect_err("late error expected");
    assert!(late
        .as_database_error()
        .is_some_and(|e| e.message() == SENTINEL));
    assert_eq!(super::store::save_error(&late), PolicyError::Unavailable);
    drop(late);
    drop(control);
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let mut lease = reader.pool.acquire().await.unwrap();
    let id = pid(&mut lease).await;
    sqlx::query("SELECT pg_advisory_lock(724918220196)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let (timed, ()) = tokio::join!(
        timeout(
            Duration::from_millis(100),
            sqlx::query(DELAYED).execute(&mut *lease)
        ),
        waiting(&f.admin, id)
    );
    assert!(timed.is_err());
    drop(lease);
    reader.close();
    sqlx::query("SELECT pg_advisory_unlock(724918220196)")
        .execute(&mut *gate)
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let gone: bool = sqlx::query_scalar(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
            )
            .bind(id)
            .fetch_one(&f.admin)
            .await
            .unwrap();
            if gone {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("cancelled socket backend did not close");
    let bytes = captured.lock().unwrap();
    let output = String::from_utf8_lossy(&bytes);
    assert!(
        output.contains("fixed policy tracing probe")
            && output.contains("fixed policy logging probe")
    );
    assert!(!output.contains(SENTINEL));
    // This is a late ERROR/release regression, NOT a NOTICE/INFO guarantee.
}

fn human_context(b: &ManagementBinding) -> AuthorizedHumanManagementContext {
    AuthorizedHumanManagementContext::from_trusted_metadata(b.org, actor()).unwrap()
}
async fn page(
    manager: &PolicyManager,
    b: &ManagementBinding,
    limit: u16,
    before: Option<i64>,
) -> AuditPage {
    manager
        .audit_page(
            b,
            &human_context(b),
            AuditPageRequest::new(limit, before).unwrap(),
        )
        .await
        .unwrap()
}
/// Fixture-only time travel, NOT deployed owner mutation authority. Keep the
/// canonical trigger enabled again BEFORE any role-limited call/checkout.
async fn age_edits(f: &Fixture, b: &ManagementBinding, through: i64) {
    let mut tx = f.admin.begin().await.unwrap();
    sqlx::query("ALTER TABLE query_policy.edits DISABLE TRIGGER guard_edit")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE query_policy.edits SET edited_at=statement_timestamp()-interval '721 hours', expires_at=statement_timestamp()-interval '1 hour' WHERE org_uuid=$1 AND credential_uuid=$2 AND revision <= $3")
        .bind(b.org).bind(b.credential).bind(through).execute(&mut *tx).await.unwrap();
    sqlx::query("ALTER TABLE query_policy.edits ENABLE TRIGGER guard_edit")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
/// Fixture admin switches to the existing NOLOGIN/non-bypass owner, rather than
/// presenting an admin DELETE as proof of FORCE-RLS-respecting purge authority.
async fn owner_purge(f: &Fixture, tenant: &str, limit: i32) -> i32 {
    let mut tx = f.admin.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE qp_owner")
        .execute(&mut *tx)
        .await
        .unwrap();
    let safe: bool = sqlx::query_scalar("SELECT current_user='qp_owner' AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcanlogin FROM pg_roles WHERE rolname=current_user").fetch_one(&mut *tx).await.unwrap();
    assert!(safe);
    sqlx::query("SELECT set_config('query_policy.tenant',$1,true)")
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .unwrap();
    let foreign: i64 =
        sqlx::query_scalar("SELECT count(*) FROM query_policy.edits WHERE tenant <> $1")
            .bind(tenant)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(foreign, 0, "NOLOGIN owner must respect FORCE RLS");
    let count = sqlx::query_scalar("SELECT query_policy.purge_expired_edits($1,$2)")
        .bind(tenant)
        .bind(limit)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    count
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn audit_management_exact_binding_bounded_stable_pages_and_original_720h_expiry() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("tenant", "https://issuer.invalid/", "reused-client"));
    assert!(page(&manager, &b, 2, None).await.edits.is_empty());
    for rev in 0..5 {
        manager
            .save(&b, &actor(), save(rev, Some(10 + rev), Some(100 + rev)))
            .await
            .unwrap();
    }
    let first = page(&manager, &b, 2, None).await;
    assert_eq!(
        first.edits.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![5, 4]
    );
    assert_eq!(first.next_before_revision, Some(4));
    for e in &first.edits {
        assert_eq!(e.expires_at - e.edited_at, chrono::Duration::hours(720));
        assert_eq!(e.actor_sub, "synthetic-verified-human");
        assert_eq!(e.schema_version, 1);
        assert_eq!(e.new_settings.timeout_seconds(), Some(9 + e.revision));
        assert_eq!(e.old_settings.timeout_seconds(), Some(8 + e.revision));
        assert!(!format!("{e:?}").contains("synthetic-verified-human"));
    }
    let db_stamped: bool = sqlx::query_scalar("SELECT bool_and(edited_at <= clock_timestamp() AND edited_at > clock_timestamp()-interval '1 minute' AND expires_at-edited_at=interval '720 hours') FROM query_policy.edits").fetch_one(&f.admin).await.unwrap();
    assert!(db_stamped);
    // Successful reset is a NEW edit, not renewal of any original edit.
    manager
        .save(&b, &actor(), save(5, None, None))
        .await
        .unwrap();
    assert_eq!(
        manager.save(&b, &actor(), save(5, Some(1), None)).await,
        Err(PolicyError::Conflict)
    );
    let second = page(&manager, &b, 2, first.next_before_revision).await;
    assert_eq!(
        second.edits.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![3, 2]
    );
    let third = page(&manager, &b, 2, second.next_before_revision).await;
    assert_eq!(
        third.edits.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(third.next_before_revision, None);
    assert_eq!(page(&manager, &b, 2, None).await.edits[1], first.edits[0]);
    let foreign_context =
        AuthorizedHumanManagementContext::from_trusted_metadata(Uuid::new_v4(), actor()).unwrap();
    assert_eq!(
        manager
            .audit_page(
                &b,
                &foreign_context,
                AuditPageRequest::new(2, None).unwrap()
            )
            .await,
        Err(PolicyError::Binding)
    );
    for wrong in [
        ManagementBinding::from_trusted_metadata(b.key.clone(), Uuid::new_v4(), b.credential)
            .unwrap(),
        ManagementBinding::from_trusted_metadata(b.key.clone(), b.org, Uuid::new_v4()).unwrap(),
    ] {
        assert_eq!(
            manager
                .audit_page(
                    &wrong,
                    &human_context(&wrong),
                    AuditPageRequest::new(2, None).unwrap()
                )
                .await,
            Err(PolicyError::Binding)
        );
    }
    for k in [
        key("other", "https://issuer.invalid/", "reused-client"),
        key("tenant", "https://issuer.invalid", "reused-client"),
        key("tenant", "https://issuer.invalid/", "other-client"),
    ] {
        let remap =
            ManagementBinding::from_trusted_metadata(k.clone(), b.org, b.credential).unwrap();
        assert!(page(&manager, &remap, 100, None).await.edits.is_empty());
        let independent = binding(k);
        manager
            .save(&independent, &actor(), save(0, Some(99), None))
            .await
            .unwrap();
        assert_eq!(page(&manager, &independent, 100, None).await.edits.len(), 1);
        assert_eq!(
            manager
                .audit_page(
                    &remap,
                    &human_context(&remap),
                    AuditPageRequest::new(1, None).unwrap()
                )
                .await,
            Err(PolicyError::Binding)
        );
    }
    // Reads do not renew the original timestamps. The database, not the DTO or
    // application clock, excludes expired rows while the policy remains known.
    age_edits(&f, &b, 4).await;
    let live = page(&manager, &b, 100, None).await;
    assert_eq!(
        live.edits.iter().map(|e| e.revision).collect::<Vec<_>>(),
        vec![6, 5]
    );
    assert_eq!(live.edits[1], first.edits[0]);
    assert!(page(&manager, &b, 100, Some(5)).await.edits.is_empty());
    let mut lease = manager.pool.acquire().await.unwrap();
    assert!(tenant(&mut lease).await.is_none_or(|v| v.is_empty()));
    lease.complete().await.unwrap();
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn audit_owner_expired_purge_is_bounded_force_rls_and_policy_survives_all_null() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let reader = PolicyReader::connect(config(&f, Role::Reader))
        .await
        .unwrap();
    let a = binding(key("a", "issuer", "same-client"));
    let b = binding(key("b", "issuer", "same-client"));
    for binding in [&a, &b] {
        manager
            .save(binding, &actor(), save(0, Some(12), Some(100)))
            .await
            .unwrap();
        manager
            .save(binding, &actor(), save(1, None, None))
            .await
            .unwrap();
        manager
            .save(binding, &actor(), save(2, None, None))
            .await
            .unwrap();
        age_edits(&f, binding, 2).await;
    }
    // Expired audit rows cannot be mutated, even by ordinary owner DML.
    for statement in [
        "UPDATE query_policy.edits SET expires_at=expires_at-interval '1 hour'",
        "UPDATE query_policy.edits SET edited_at=edited_at-interval '1 hour', expires_at=expires_at-interval '1 hour'",
        "UPDATE query_policy.edits SET revision=revision+10",
        "UPDATE query_policy.edits SET tenant='b'",
        "UPDATE query_policy.edits SET actor_sub='another-human'",
        "DELETE FROM query_policy.edits WHERE revision=3",
    ] {
        let mut tx=f.admin.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE qp_owner").execute(&mut *tx).await.unwrap();
        sqlx::query("SELECT set_config('query_policy.tenant','a',true)").execute(&mut *tx).await.unwrap();
        let e=sqlx::query(statement).execute(&mut *tx).await.unwrap_err();
        assert_eq!(e.as_database_error().unwrap().code().as_deref(),Some("QP002"));
        tx.rollback().await.unwrap();
    }
    assert_eq!(owner_purge(&f, "a", 1).await, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM query_policy.edits WHERE tenant='b'")
        .fetch_one(&f.admin)
        .await
        .unwrap();
    assert_eq!(count, 3);
    assert_eq!(owner_purge(&f, "a", 1000).await, 1);
    assert_eq!(owner_purge(&f, "a", 1000).await, 0);
    let live = page(&manager, &a, 100, None).await;
    assert_eq!(live.edits.len(), 1);
    assert_eq!(live.edits[0].revision, 3);
    let unchanged = live.edits[0].clone();
    assert_eq!(page(&manager, &a, 100, None).await.edits[0], unchanged);
    assert_eq!(owner_purge(&f, "b", 1000).await, 2);
    age_edits(&f, &a, 3).await;
    assert!(page(&manager, &a, 100, None).await.edits.is_empty());
    assert_eq!(owner_purge(&f, "a", 1000).await, 1);
    let lookup = reader.lookup(&a.key).await.unwrap();
    assert_eq!(lookup.state(), PolicyState::Inherited);
    assert_eq!(lookup.revision(), Some(3));
    assert_eq!(manager.lookup(&a).await.unwrap(), lookup);
    assert_eq!(
        manager.save(&a, &actor(), save(0, None, None)).await,
        Err(PolicyError::Conflict)
    );
    let next = manager
        .save(&a, &actor(), save(3, Some(7), Some(90)))
        .await
        .unwrap();
    assert_eq!(next.revision(), 4);
    age_edits(&f, &a, 4).await;
    assert_eq!(owner_purge(&f, "a", 1000).await, 1);
    assert_eq!(
        reader.lookup(&a.key).await.unwrap(),
        PolicyLookup::Found(next)
    );
    assert_eq!(next.settings().timeout_seconds(), Some(7));
    assert!(page(&manager, &a, 100, None).await.edits.is_empty());
    // Never gain cross-tenant authority via a purge argument or invalid bound.
    for (requested, limit) in [("b", 1), ("a", 0), ("a", 1001), ("a", -1)] {
        let mut tx = f.admin.begin().await.unwrap();
        sqlx::query("SET LOCAL ROLE qp_owner")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let e = sqlx::query("SELECT query_policy.purge_expired_edits($1,$2)")
            .bind(requested)
            .bind(limit)
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert_eq!(
            e.as_database_error().unwrap().code().as_deref(),
            Some("QP003")
        );
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn audit_role_limited_interfaces_deny_writes_expiry_purge_and_unbounded_reads() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let b = binding(key("a", "issuer", "client"));
    manager
        .save(&b, &actor(), save(0, None, None))
        .await
        .unwrap();
    // Even trusted owner INSERT cannot supply a renewable/backdated clock; this
    // synthetic row is rolled back, not a deployed direct-write workflow.
    let mut stamp_tx = f.admin.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE qp_owner")
        .execute(&mut *stamp_tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
        .execute(&mut *stamp_tx)
        .await
        .unwrap();
    let db_stamp: bool=sqlx::query_scalar("INSERT INTO query_policy.edits (tenant,org_uuid,credential_uuid,schema_version,revision,actor_sub,edited_at,expires_at) VALUES ('a',$1,$2,1,2,'fixture-human','2000-01-01','2099-01-01') RETURNING edited_at >= statement_timestamp() AND edited_at <= clock_timestamp() AND expires_at-edited_at=interval '720 hours'").bind(b.org).bind(b.credential).fetch_one(&mut *stamp_tx).await.unwrap();
    assert!(db_stamp);
    stamp_tx.rollback().await.unwrap();
    for role in ["qp_reader", "qp_manager", "qh_reader", "qh_writer"] {
        let mut c = PgConnection::connect_with(&options(&f.socket, role))
            .await
            .unwrap();
        for statement in [
            "SELECT query_policy.purge_expired_edits('a',1)",
            "DELETE FROM query_policy.edits",
            "UPDATE query_policy.edits SET expires_at=clock_timestamp()",
            "ALTER TABLE query_policy.edits DISABLE TRIGGER guard_edit",
            "SELECT query_policy.guard_edit()",
            "SELECT query_policy.immutable_binding()",
        ] {
            assert!(
                sqlx::query(statement).execute(&mut c).await.is_err(),
                "unexpected authority: {role} {statement}"
            );
        }
        if role != "qp_manager" {
            assert!(sqlx::query(
                "SELECT * FROM query_policy.read_edits('a','issuer','client',$1,$2,NULL,1)"
            )
            .bind(b.org)
            .bind(b.credential)
            .execute(&mut c)
            .await
            .is_err());
        }
    }
    let mut c = PgConnection::connect_with(&options(&f.socket, "qp_manager"))
        .await
        .unwrap();
    for (tenant_arg, limit, cursor) in [
        ("a", 0, None),
        ("a", 101, None),
        ("a", -1, None),
        ("a", 1, Some(0_i64)),
        ("a", 1, Some(-1)),
        ("b", 1, None),
    ] {
        let mut tx = c.begin().await.unwrap();
        sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
            .execute(&mut *tx)
            .await
            .unwrap();
        let e =
            sqlx::query("SELECT * FROM query_policy.read_edits($1,'issuer','client',$2,$3,$4,$5)")
                .bind(tenant_arg)
                .bind(b.org)
                .bind(b.credential)
                .bind(cursor)
                .bind(limit)
                .execute(&mut *tx)
                .await
                .unwrap_err();
        assert_eq!(
            e.as_database_error().unwrap().code().as_deref(),
            Some("QP003")
        );
        tx.rollback().await.unwrap();
        assert!(tenant(&mut c).await.is_none_or(|v| v.is_empty()));
    }
    // GUC on a different connection cannot supply the function's context.
    let mut other = PgConnection::connect_with(&options(&f.socket, "qp_manager"))
        .await
        .unwrap();
    let mut tx = c.begin().await.unwrap();
    sqlx::query("SELECT set_config('query_policy.tenant','a',true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(sqlx::query(
        "SELECT * FROM query_policy.read_edits('a','issuer','client',$1,$2,NULL,1)"
    )
    .bind(b.org)
    .bind(b.credential)
    .execute(&mut other)
    .await
    .is_err());
    tx.rollback().await.unwrap();
    assert_eq!(page(&manager, &b, 1, None).await.edits.len(), 1);
    let mut lease = manager.pool.acquire().await.unwrap();
    assert!(tenant(&mut lease).await.is_none_or(|v| v.is_empty()));
    lease.complete().await.unwrap();
}

#[tokio::test]
#[ignore = "new owned disposable PG only: scripts/check-query-policy.sh"]
async fn audit_read_cancellation_rollback_and_reused_manager_connection_have_no_tenant_leak() {
    let f = fixture().await;
    let manager = PolicyManager::connect(config(&f, Role::Manager))
        .await
        .unwrap();
    let a = binding(key("a", "issuer", "client"));
    let b = binding(key("b", "issuer", "client"));
    for binding in [&a, &b] {
        manager
            .save(binding, &actor(), save(0, Some(10), None))
            .await
            .unwrap();
    }
    let context = human_context(&a);
    let request = AuditPageRequest::new(1, None).unwrap();
    let mut guard = f.admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE query_policy.edits IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *guard)
        .await
        .unwrap();
    assert!(timeout(
        Duration::from_millis(100),
        manager.audit_page(&a, &context, request)
    )
    .await
    .is_err());
    assert_eq!(manager.pool.idle_count(), 0);
    guard.rollback().await.unwrap();
    assert_eq!(page(&manager, &b, 1, None).await.edits.len(), 1);
    let mut lease = manager.pool.acquire().await.unwrap();
    let original = pid(&mut lease).await;
    assert!(tenant(&mut lease).await.is_none_or(|s| s.is_empty()));
    lease.complete().await.unwrap();
    drop(lease);
    assert_eq!(page(&manager, &a, 1, None).await.edits.len(), 1);
    let mut lease = manager.pool.acquire().await.unwrap();
    assert_eq!(pid(&mut lease).await, original);
    assert!(tenant(&mut lease).await.is_none_or(|s| s.is_empty()));
    lease.complete().await.unwrap();
}
