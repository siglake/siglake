//! Optional source-level activation, never schema installation or credential fallback.
use super::{
    ConnectionBounds, ControlsAuthority, DistributedControlsConfig, PolicyConfig, PolicyError,
    PolicyReader,
};
use crate::{AppState, Priority, TierLimits};
use std::{sync::Arc, time::Duration};

/// Opaque input: Clap accepts the string WITHOUT invoking a URL value parser
/// (which echoes rejected values). Validation happens later with fixed errors.
#[derive(Clone)]
pub struct SecretDsn(String);
impl std::fmt::Debug for SecretDsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted]")
    }
}
impl std::str::FromStr for SecretDsn {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}
#[derive(clap::Args, Debug, Default)]
pub struct ControlsArgs {
    /// Dedicated PRIMARY qp_reader URI; never the app/job/history/manager pool.
    #[arg(
        long = "query-controls-database-url",
        env = "SIGLAKE_QUERY_CONTROLS_DATABASE_URL",
        hide_env_values = true
    )]
    database_url: Option<SecretDsn>,
    /// Explicit total reader operation/checkout budget (1..=30000 ms).
    #[arg(
        long = "query-controls-operation-timeout-ms",
        env = "SIGLAKE_QUERY_CONTROLS_OPERATION_TIMEOUT_MS"
    )]
    operation_timeout_ms: Option<u64>,
    /// Explicit dedicated reader socket capacity (1..=16).
    #[arg(
        long = "query-controls-max-connections",
        env = "SIGLAKE_QUERY_CONTROLS_MAX_CONNECTIONS"
    )]
    max_connections: Option<u8>,
    /// Positive local operator configuration generation.
    #[arg(
        long = "query-controls-operator-generation",
        env = "SIGLAKE_QUERY_CONTROLS_OPERATOR_GENERATION"
    )]
    generation: Option<i64>,
    /// Optional additive named RDS CA trust profile, not a TLS downgrade.
    #[arg(
        long = "query-controls-ca-profile",
        env = "SIGLAKE_QUERY_CONTROLS_CA_PROFILE"
    )]
    ca_profile: Option<String>,
    #[arg(
        long = "query-controls-interactive-default-seconds",
        env = "SIGLAKE_QUERY_CONTROLS_INTERACTIVE_DEFAULT_SECONDS"
    )]
    interactive_default_seconds: Option<u64>,
    #[arg(
        long = "query-controls-interactive-ceiling-seconds",
        env = "SIGLAKE_QUERY_CONTROLS_INTERACTIVE_CEILING_SECONDS"
    )]
    interactive_ceiling_seconds: Option<u64>,
    #[arg(
        long = "query-controls-batch-default-seconds",
        env = "SIGLAKE_QUERY_CONTROLS_BATCH_DEFAULT_SECONDS"
    )]
    batch_default_seconds: Option<u64>,
    #[arg(
        long = "query-controls-batch-ceiling-seconds",
        env = "SIGLAKE_QUERY_CONTROLS_BATCH_CEILING_SECONDS"
    )]
    batch_ceiling_seconds: Option<u64>,
    /// Explicit finite relative clock error bound, required for controlled fanout.
    #[arg(
        long = "query-controls-clock-skew-ms",
        env = "SIGLAKE_QUERY_CONTROLS_CLOCK_SKEW_MS"
    )]
    clock_skew_ms: Option<u64>,
}
pub struct PreparedControls {
    config: PolicyConfig,
    generation: i64,
    distributed: Option<DistributedControlsConfig>,
    interactive: TierLimits,
    batch: TierLimits,
}
impl ControlsArgs {
    /// Synchronous validation BEFORE telemetry/catalog/discovery. Existing tier
    /// defaults are retained unless explicitly overridden; no invented timeout.
    pub fn prepare(
        &self,
        fanout: bool,
        coordinator: Option<&str>,
    ) -> Result<Option<PreparedControls>, PolicyError> {
        let Some(dsn) = &self.database_url else {
            if self.operation_timeout_ms.is_some()
                || self.max_connections.is_some()
                || self.generation.is_some()
                || self.ca_profile.is_some()
                || self.interactive_default_seconds.is_some()
                || self.interactive_ceiling_seconds.is_some()
                || self.batch_default_seconds.is_some()
                || self.batch_ceiling_seconds.is_some()
                || self.clock_skew_ms.is_some()
            {
                return Err(PolicyError::Configuration);
            }
            return Ok(None);
        };
        // Do NOT construct ServerLimits here: its admission default initializes
        // the global query pool, which must remain after cache configuration.
        let mut interactive = TierLimits::interactive_defaults();
        let mut batch = TierLimits::batch_defaults();
        for (value, target) in [
            (
                self.interactive_default_seconds,
                &mut interactive.default_timeout,
            ),
            (
                self.interactive_ceiling_seconds,
                &mut interactive.ceiling_timeout,
            ),
            (self.batch_default_seconds, &mut batch.default_timeout),
            (self.batch_ceiling_seconds, &mut batch.ceiling_timeout),
        ] {
            if let Some(n) = value {
                *target = Duration::from_secs(n);
            }
        }
        super::runtime::validate_tier(&interactive)?;
        super::runtime::validate_tier(&batch)?;
        let generation = self
            .generation
            .filter(|n| *n > 0)
            .ok_or(PolicyError::Configuration)?;
        let distributed = if fanout || self.clock_skew_ms.is_some() {
            let token = coordinator.ok_or(PolicyError::Configuration)?;
            if token.is_empty()
                || token.len() > 8192
                || !token.bytes().all(|b| (33..=126).contains(&b))
            {
                return Err(PolicyError::Configuration);
            }
            Some(DistributedControlsConfig::new(Duration::from_millis(
                self.clock_skew_ms.ok_or(PolicyError::Configuration)?,
            ))?)
        } else {
            None
        };
        let config = PolicyConfig::reader_with_ca_profile(
            &dsn.0,
            ConnectionBounds {
                max_connections: self.max_connections.ok_or(PolicyError::Configuration)?,
                operation_timeout: Duration::from_millis(
                    self.operation_timeout_ms
                        .ok_or(PolicyError::Configuration)?,
                ),
            },
            self.ca_profile.as_deref(),
        )?;
        Ok(Some(PreparedControls {
            config,
            generation,
            distributed,
            interactive,
            batch,
        }))
    }
}
impl PreparedControls {
    /// Enabled startup refuses unsafe/unavailable primary role/layout. A read
    /// pool is never substituted or silently downgraded to disabled.
    pub async fn connect(self) -> Result<Arc<ControlsAuthority>, PolicyError> {
        let reader = Arc::new(PolicyReader::connect(self.config).await?);
        let mut authority =
            ControlsAuthority::from_tiers(reader, self.interactive, self.batch, self.generation)?;
        authority.distributed = self.distributed;
        Ok(Arc::new(authority))
    }
}
impl AppState {
    pub fn with_controls_authority(mut self, authority: Option<Arc<ControlsAuthority>>) -> Self {
        if let Some(authority) = &authority {
            let limits = Arc::make_mut(&mut self.limits);
            limits.interactive.default_timeout =
                authority.tier(Priority::Interactive).default_timeout;
            limits.interactive.ceiling_timeout =
                authority.tier(Priority::Interactive).ceiling_timeout;
            limits.batch.default_timeout = authority.tier(Priority::Batch).default_timeout;
            limits.batch.ceiling_timeout = authority.tier(Priority::Batch).ceiling_timeout;
        }
        self.query_controls = authority;
        self
    }
}
/// Controlled HTTP fanout accepts explicit worker origins/base paths only;
/// userinfo/query/fragment must not redirect the existing coordinator secret.
pub fn validate_peer_urls(peers: &[String]) -> Result<(), PolicyError> {
    for peer in peers {
        let url = reqwest::Url::parse(peer).map_err(|_| PolicyError::Configuration)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(PolicyError::Configuration);
        }
    }
    Ok(())
}

/// Source startup owns this dedicated reader. Keep it alive until retained
/// body/job joins have completed; cancelled waits leave ownership with caller.
pub async fn shutdown_owned(authority: &ControlsAuthority) {
    authority.shutdown().await;
    authority.reader.close();
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(flatten)]
        controls: ControlsArgs,
    }
    #[test]
    fn controlled_peer_urls_refuse_secret_userinfo_query_fragment_and_non_http() {
        assert!(validate_peer_urls(&["https://worker.invalid:8089/".into()]).is_ok());
        for uri in [
            "bad",
            "file:///tmp/worker",
            "http://SECRET_CANARY@worker.invalid",
            "http://worker.invalid?x=1",
            "http://worker.invalid#fragment",
        ] {
            assert_eq!(
                validate_peer_urls(&[uri.into()]),
                Err(PolicyError::Configuration)
            );
        }
    }
    #[test]
    fn startup_config_disabled_defaults_and_secret_canary() {
        let cli = TestCli::try_parse_from(["fixture"]).unwrap();
        assert!(cli.controls.prepare(false, None).unwrap().is_none());
        let cli = TestCli::try_parse_from([
            "fixture",
            "--query-controls-database-url",
            "postgres://qp_reader:DSN_SECRET_CANARY@invalid",
        ])
        .unwrap();
        assert!(!format!("{cli:?}").contains("DSN_SECRET_CANARY"));
        assert_eq!(
            cli.controls.prepare(false, None).err(),
            Some(PolicyError::Configuration)
        );
        let mut help = Vec::new();
        use clap::CommandFactory;
        TestCli::command().write_long_help(&mut help).unwrap();
        assert!(!String::from_utf8(help)
            .unwrap()
            .contains("DSN_SECRET_CANARY"));
    }
    #[test]
    fn startup_config_positive_bounds_skew_and_static_rejections() {
        let args = [
            "fixture",
            "--query-controls-database-url",
            "postgresql://qp_reader:synthetic@explicit.invalid:5432/fixture?sslmode=verify-full",
            "--query-controls-operation-timeout-ms",
            "2000",
            "--query-controls-max-connections",
            "2",
            "--query-controls-operator-generation",
            "1",
        ];
        let cli = TestCli::try_parse_from(args).unwrap();
        assert!(cli.controls.prepare(false, None).unwrap().is_some());
        assert!(cli.controls.prepare(true, Some("synthetic")).is_err());
        for (flag, value) in [
            ("--query-controls-interactive-default-seconds", "0"),
            ("--query-controls-interactive-ceiling-seconds", "1"),
            (
                "--query-controls-batch-default-seconds",
                "18446744073709551615",
            ),
        ] {
            let cli = TestCli::try_parse_from(args.into_iter().chain([flag, value])).unwrap();
            assert!(cli.controls.prepare(false, None).is_err());
        }
        let cli = TestCli::try_parse_from(
            args.into_iter()
                .chain(["--query-controls-clock-skew-ms", "100"]),
        )
        .unwrap();
        assert!(cli.controls.prepare(true, Some("synthetic")).is_ok());
        assert!(cli.controls.prepare(true, Some(" ")).is_err());
    }
}
