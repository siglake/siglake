//! Real PostgreSQL only: scripts/check-query-history.sh creates the fixture.
//! No DATABASE_URL, production URI, external services or ambient PG settings.
use siglake_query_server::history::{HistoryReader, HistoryWriter};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool,
};
use std::path::PathBuf;
use tokio::sync::{Mutex, MutexGuard};
static DB_LOCK: Mutex<()> = Mutex::const_new(());
pub(crate) struct Fixture {
    _guard: MutexGuard<'static, ()>,
    pub(crate) admin: PgPool,
    pub(crate) writer: HistoryWriter,
    pub(crate) reader: HistoryReader,
    pub(crate) socket: PathBuf,
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
pub(crate) fn options(socket: &std::path::Path, role: &str) -> PgConnectOptions {
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
pub(crate) async fn fixture() -> Fixture {
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
