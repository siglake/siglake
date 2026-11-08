//! Explicit role-specific configuration, never SQLx URI parsing or PG/passfile fallback.
use super::{PolicyError, Role};
use sqlx::{
    postgres::{PgConnectOptions, PgSslMode},
    ConnectOptions,
};
use std::time::Duration;

/// Technical socket capacity and total checkout/operation budget, not agent limits.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionBounds {
    pub max_connections: u8,
    pub operation_timeout: Duration,
}
impl ConnectionBounds {
    pub(super) fn validate(self) -> Result<Self, PolicyError> {
        if !(1..=16).contains(&self.max_connections)
            || self.operation_timeout.is_zero()
            || self.operation_timeout > Duration::from_secs(30)
        {
            return Err(PolicyError::Configuration);
        }
        Ok(self)
    }
}
/// No environment activation API: constructing this does not enable any route.
pub struct PolicyConfig {
    pub(super) options: PgConnectOptions,
    pub(super) role: Role,
    pub(super) bounds: ConnectionBounds,
}
impl std::fmt::Debug for PolicyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PolicyConfig([redacted])")
    }
}
impl PolicyConfig {
    pub fn reader(uri: &str, bounds: ConnectionBounds) -> Result<Self, PolicyError> {
        Self::reader_with_ca_profile(uri, bounds, None)
    }
    pub fn manager(uri: &str, bounds: ConnectionBounds) -> Result<Self, PolicyError> {
        Self::manager_with_ca_profile(uri, bounds, None)
    }
    pub fn reader_with_ca_profile(
        uri: &str,
        bounds: ConnectionBounds,
        ca_profile: Option<&str>,
    ) -> Result<Self, PolicyError> {
        Self::explicit(uri, Role::Reader, bounds, ca_profile)
    }
    pub fn manager_with_ca_profile(
        uri: &str,
        bounds: ConnectionBounds,
        ca_profile: Option<&str>,
    ) -> Result<Self, PolicyError> {
        Self::explicit(uri, Role::Manager, bounds, ca_profile)
    }
    fn explicit(
        uri: &str,
        role: Role,
        bounds: ConnectionBounds,
        ca_profile: Option<&str>,
    ) -> Result<Self, PolicyError> {
        Ok(Self {
            options: crate::postgres_trust::apply_selector(
                options(uri, role).ok_or(PolicyError::Configuration)?,
                ca_profile,
            )
            .ok_or(PolicyError::Configuration)?,
            role,
            bounds: bounds.validate()?,
        })
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

fn options(uri: &str, role: Role) -> Option<PgConnectOptions> {
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
        || url.username() != role.name()
    {
        return None;
    }
    // URL normalizers may erase an empty password: require its explicit colon in
    // the original authority, then accept empty ONLY for local Unix sockets.
    let authority = uri.split_once("://")?.1.split('/').next()?;
    if !authority
        .rsplit_once('@')?
        .0
        .starts_with(&format!("{}:", role.name()))
    {
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
            .username(role.name())
            .password(&password)
            .database(&database)
            .ssl_mode(ssl)
            .application_name("siglake-query-policy")
            .statement_cache_capacity(100)
            .extra_float_digits(3)
            .disable_statement_logging(),
    )
}

#[cfg(test)]
mod trust_tests {
    use super::*;
    #[tokio::test]
    #[ignore = "owned synthetic Postgres TLS fixture; explicit pinned tools required"]
    async fn rds_tls_policy_reader_and_manager_ca_host_reconnect_and_no_downgrade() {
        for role in [Role::Reader, Role::Manager] {
            crate::postgres_trust_tests::verify_connector(role.name(), |uri, selector| {
                let bounds = ConnectionBounds {
                    max_connections: 2,
                    operation_timeout: Duration::from_secs(2),
                };
                let config = match role {
                    Role::Reader => PolicyConfig::reader_with_ca_profile(uri, bounds, selector),
                    Role::Manager => PolicyConfig::manager_with_ca_profile(uri, bounds, selector),
                }
                .unwrap();
                assert_eq!(format!("{config:?}"), "PolicyConfig([redacted])");
                assert_eq!(config.bounds.operation_timeout, bounds.operation_timeout);
                config.options
            })
            .await;
        }
    }
    #[test]
    fn named_ca_profile_preserves_policy_roles_and_bounds() {
        let bounds = ConnectionBounds {
            max_connections: 2,
            operation_timeout: Duration::from_secs(2),
        };
        for role in [Role::Reader, Role::Manager] {
            let uri = format!(
                "postgresql://{}:synthetic@explicit.invalid:5432/fixture?sslmode=verify-full",
                role.name()
            );
            for selector in [
                Some(""),
                Some("unknown"),
                Some("aws-rds-us-east-1-rsa2048-g1 "),
            ] {
                assert!(PolicyConfig::explicit(&uri, role, bounds, selector).is_err());
            }
            let config =
                PolicyConfig::explicit(&uri, role, bounds, Some("aws-rds-us-east-1-rsa2048-g1"))
                    .unwrap();
            assert_eq!(config.options.get_username(), role.name());
            assert_eq!(config.bounds.max_connections, 2);
            let socket = format!(
                "postgresql://{}:@%2Ftmp%2Fsynthetic:5432/fixture?sslmode=disable",
                role.name()
            );
            assert!(PolicyConfig::explicit(
                &socket,
                role,
                bounds,
                Some("aws-rds-us-east-1-rsa2048-g1")
            )
            .is_err());
        }
    }
}
