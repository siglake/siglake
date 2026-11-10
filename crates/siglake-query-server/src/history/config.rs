//! Deliberately restricted explicit writer configuration. Never SQLx FromStr:
//! that path imports PG defaults, reads passfiles and logs unknown URI values.
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode},
    ConnectOptions,
};

/// Dedicated history configuration; never consult catalog/jobs/PG credentials.
/// The URI is intentionally absent from Debug, errors and clap help.
pub struct HistoryConfig {
    options: PgConnectOptions,
}
impl std::fmt::Debug for HistoryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HistoryConfig([redacted])")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HistoryConfigError {
    #[error("invalid explicit history configuration")]
    Invalid,
    #[error("history requires explicit legacy audit disablement")]
    LegacyAuditEnabled,
}
impl HistoryConfig {
    /// Absent means disabled, including when unrelated database/PG config exists.
    pub fn from_env(audit_disabled: bool) -> Result<Option<Self>, HistoryConfigError> {
        match std::env::var("SIGLAKE_QUERY_HISTORY_DATABASE_URL") {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(HistoryConfigError::Invalid),
            Ok(uri) => {
                let selector = std::env::var("SIGLAKE_QUERY_HISTORY_CA_PROFILE")
                    .map(Some)
                    .or_else(|e| match e {
                        std::env::VarError::NotPresent => Ok(None),
                        _ => Err(HistoryConfigError::Invalid),
                    })?;
                Self::explicit_with_ca_profile(&uri, audit_disabled, selector.as_deref()).map(Some)
            }
        }
    }
    pub fn explicit(uri: &str, audit_disabled: bool) -> Result<Self, HistoryConfigError> {
        Self::explicit_with_ca_profile(uri, audit_disabled, None)
    }
    pub fn explicit_with_ca_profile(
        uri: &str,
        audit_disabled: bool,
        ca_profile: Option<&str>,
    ) -> Result<Self, HistoryConfigError> {
        if !audit_disabled {
            return Err(HistoryConfigError::LegacyAuditEnabled);
        }
        Ok(Self {
            options: crate::postgres_trust::apply_selector(
                options(uri).ok_or(HistoryConfigError::Invalid)?,
                ca_profile,
            )
            .ok_or(HistoryConfigError::Invalid)?,
        })
    }
    pub(super) fn connect_options(&self) -> PgConnectOptions {
        self.options.clone()
    }
}

fn decode(value: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut input = value.bytes();
    while let Some(byte) = input.next() {
        bytes.push(if byte == b'%' {
            (char::from(input.next()?).to_digit(16)? * 16
                + char::from(input.next()?).to_digit(16)?) as u8
        } else {
            byte
        });
    }
    String::from_utf8(bytes).ok()
}

fn options(uri: &str) -> Option<PgConnectOptions> {
    // SQLx has no public unsetters for these inherited fields. Reject presence
    // BEFORE construction, including empty/non-Unicode values, without file IO.
    if ["PGOPTIONS", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"]
        .iter()
        .any(|key| std::env::var_os(key).is_some())
    {
        return None;
    }
    if uri.is_empty() || uri.len() > 8192 {
        return None;
    }
    let url = reqwest::Url::parse(uri).ok()?;
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.fragment().is_some()
        || url.username() != "qh_writer"
    {
        return None;
    }
    // URL normalizers may erase an empty password: require its explicit colon in
    // the original authority, then accept empty ONLY for local Unix sockets.
    let authority = uri.split_once("://")?.1.split('/').next()?;
    if !authority.rsplit_once('@')?.0.starts_with("qh_writer:") {
        return None;
    }
    let password = decode(url.password().unwrap_or(""))?;
    if password.len() > 4096 || password.chars().any(char::is_control) {
        return None;
    }
    let host = decode(url.host_str()?)?;
    if host.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return None;
    }
    let port = url.port()?;
    if port == 0 {
        return None;
    }
    let database = decode(url.path().strip_prefix('/')?)?;
    if database.is_empty()
        || database.len() > 128
        || !database
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return None;
    }
    let pairs: Vec<_> = url.query_pairs().collect();
    if pairs.len() != 1 || pairs[0].0 != "sslmode" {
        return None;
    }
    let socket = host.starts_with('/');
    let ssl = if socket && password.is_empty() && pairs[0].1 == "disable" {
        if host.len() > 100
            || host
                .split('/')
                .skip(1)
                .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return None;
        }
        PgSslMode::Disable
    } else if !socket && !password.is_empty() && pairs[0].1 == "verify-full" {
        if host.is_empty()
            || host
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || c == '/' || c == '%')
        {
            return None;
        }
        PgSslMode::VerifyFull
    } else {
        return None;
    };
    // Audited SQLx0.8.6 constructor: PGHOSTADDR/HOST share `host`; socket
    // starts None. All inherited effective fields are overwritten here except
    // options/TLS file paths, whose presence was rejected above. No apply_pgpass.
    Some(
        PgConnectOptions::new_without_pgpass()
            .host(&host)
            .port(port)
            .username("qh_writer")
            .password(&password)
            .database(&database)
            .ssl_mode(ssl)
            .application_name("siglake-query-history")
            .statement_cache_capacity(100)
            .extra_float_digits(3)
            .disable_statement_logging(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::{ffi::OsStringExt, fs::PermissionsExt},
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    #[tokio::test]
    #[ignore = "owned synthetic Postgres TLS fixture; explicit pinned tools required"]
    async fn rds_tls_writer_ca_host_reconnect_and_no_downgrade() {
        crate::postgres_trust_tests::verify_connector("qh_writer", |uri, selector| {
            let config = HistoryConfig::explicit_with_ca_profile(uri, true, selector).unwrap();
            assert_eq!(format!("{config:?}"), "HistoryConfig([redacted])");
            config.connect_options()
        })
        .await;
    }
    #[test]
    fn named_ca_profile_preserves_strict_writer_config() {
        let uri =
            "postgresql://qh_writer:synthetic@explicit.invalid:5432/fixture?sslmode=verify-full";
        for selector in [
            Some(""),
            Some("unknown"),
            Some("aws-rds-us-east-1-rsa2048-g1 "),
        ] {
            let error = HistoryConfig::explicit_with_ca_profile(uri, true, selector).unwrap_err();
            assert_eq!(error, HistoryConfigError::Invalid);
            assert_eq!(error.to_string(), "invalid explicit history configuration");
        }
        assert!(HistoryConfig::explicit_with_ca_profile(uri, true, None).is_ok());
        assert!(HistoryConfig::explicit_with_ca_profile(
            "postgresql://qh_writer:@%2Ftmp%2Fsynthetic:5432/fixture?sslmode=disable",
            true,
            Some("aws-rds-us-east-1-rsa2048-g1")
        )
        .is_err());
    }
    #[test]
    fn explicit_config_environment_isolation() {
        if let Some(case) = std::env::var_os("QH_SYNTHETIC_CONFIG_CASE") {
            let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
            struct Writer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
            impl std::io::Write for Writer {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    self.0.lock().unwrap().extend_from_slice(bytes);
                    Ok(bytes.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            let output = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::TRACE)
                .with_writer(move || Writer(output.clone()))
                .finish();
            let _guard = tracing::subscriber::set_default(subscriber);
            let uri="postgresql://qh_writer:synthetic%40password@explicit.example.invalid:6543/history_fixture?sslmode=verify-full";
            let configured = options(uri);
            // Selectors are ignored without a URI (default-off), rejected once configured.
            std::env::set_var("SIGLAKE_QUERY_HISTORY_CA_PROFILE", "unknown");
            assert!(HistoryConfig::from_env(true).unwrap().is_none());
            std::env::set_var("SIGLAKE_QUERY_HISTORY_DATABASE_URL", uri);
            assert_eq!(
                HistoryConfig::from_env(true).unwrap_err(),
                HistoryConfigError::Invalid
            );
            std::env::set_var(
                "SIGLAKE_QUERY_HISTORY_CA_PROFILE",
                std::ffi::OsString::from_vec(vec![255]),
            );
            assert_eq!(
                HistoryConfig::from_env(true).unwrap_err(),
                HistoryConfigError::Invalid
            );
            std::env::remove_var("SIGLAKE_QUERY_HISTORY_DATABASE_URL");
            std::env::remove_var("SIGLAKE_QUERY_HISTORY_CA_PROFILE");

            assert!(HistoryConfig::from_env(true).unwrap().is_none());
            assert_eq!(
                HistoryConfig::explicit(uri, false).unwrap_err(),
                HistoryConfigError::LegacyAuditEnabled
            );
            if case == "supported" {
                assert_eq!(
                    format!("{:?}", HistoryConfig::explicit(uri, true).unwrap()),
                    "HistoryConfig([redacted])"
                );
            }
            if case != "supported" {
                assert!(configured.is_none());
                assert!(captured.lock().unwrap().is_empty());
                return;
            }
            let o = configured.expect("explicit config");
            assert!(
                o.get_host() == "explicit.example.invalid"
                    && o.get_port() == 6543
                    && o.get_username() == "qh_writer"
                    && o.get_database() == Some("history_fixture")
                    && o.get_socket().is_none()
                    && matches!(o.get_ssl_mode(), PgSslMode::VerifyFull)
                    && o.get_application_name() == Some("siglake-query-history")
                    && o.get_options().is_none()
            );
            assert!(o.to_url_lossy().password() == Some("synthetic%40password"));
            for invalid in [
                "postgresql:///history_fixture", // no target/role/auth
                "postgresql://qh_writer@host:5432/db?sslmode=verify-full", // no password
                "postgresql://other:synthetic@host:5432/db?sslmode=verify-full",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=require",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=verify-full&options=synthetic",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=verify-full&sslrootcert=/synthetic",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=verify-full&host=other",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=verify-full&unexpected=synthetic-do-not-log",
                "postgresql://qh_writer:synthetic@host:5432/db?sslmode=verify-full&sslmode=disable",
                "postgresql://qh_writer:synthetic@host/db?sslmode=verify-full", // no explicit port
            ] {assert!(options(invalid).is_none());}
            assert!(options(
                "postgresql://qh_writer:@%2Ftmp%2Fsynthetic-socket:5432/db?sslmode=disable"
            )
            .is_some());
            assert!(captured.lock().unwrap().is_empty());
            return;
        }
        let root =
            std::env::temp_dir().join(format!("siglake-history-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        // Even empty explicit passfiles may fall back to HOME/.pgpass in the
        // ordinary SQLx parser. This root is private, and its default is empty.
        std::fs::write(root.join(".pgpass"), []).unwrap();
        std::fs::set_permissions(root.join(".pgpass"), std::fs::Permissions::from_mode(0o600))
            .unwrap();
        let passfile = root.join("synthetic-passfile-fifo");
        assert!(Command::new("mkfifo")
            .arg("-m")
            .arg("600")
            .arg(&passfile)
            .status()
            .unwrap()
            .success());
        let cases = std::iter::once(("supported", None)).chain(
            ["PGOPTIONS", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"]
                .into_iter()
                .flat_map(|key| {
                    [
                        (
                            key,
                            Some((
                                key,
                                std::ffi::OsString::from("/nonexistent-synthetic-config"),
                            )),
                        ),
                        (key, Some((key, std::ffi::OsString::from("")))),
                        (key, Some((key, std::ffi::OsString::from_vec(vec![255])))),
                    ]
                }),
        );
        for (case, unsupported) in cases {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap())
                .env("HOME", &root)
                .env("QH_SYNTHETIC_CONFIG_CASE", case)
                .env("PGPASSFILE", &passfile)
                .env("PGHOSTADDR", "192.0.2.1")
                .env("PGHOST", "/nonexistent-synthetic-env-socket")
                .env("PGPORT", "9")
                .env("PGUSER", "synthetic_wrong_user")
                .env("PGPASSWORD", "synthetic_wrong_password")
                .env("PGDATABASE", "synthetic_wrong_database")
                .env("PGSSLMODE", "disable")
                .env("PGAPPNAME", "synthetic_wrong_application")
                .args([
                    "--exact",
                    "history::config::tests::explicit_config_environment_isolation",
                    "--nocapture",
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Some((key, value)) = unsupported {
                command.env(key, value);
            }
            let mut child = command.spawn().unwrap();
            let start = Instant::now();
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success(), "synthetic config subprocess failed");
                    break;
                }
                if start.elapsed() > Duration::from_secs(3) {
                    child.kill().unwrap();
                    let _ = child.wait();
                    panic!("configuration blocked on synthetic passfile");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        std::fs::remove_file(passfile).unwrap();
        std::fs::remove_file(root.join(".pgpass")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }
}
