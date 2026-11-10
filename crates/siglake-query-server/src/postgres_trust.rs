//! Explicit reviewed root additions for dedicated PostgreSQL connectors only.
//! SQLx adds these bytes to bundled WebPKI roots: this is NOT exclusive pinning.
use sqlx::postgres::{PgConnectOptions, PgSslMode};

/// No arbitrary path/URL roots and no inference from the connection target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaProfile {
    AwsRdsUsEast1Rsa2048G1,
}
impl CaProfile {
    pub fn parse(selector: &str) -> Option<Self> {
        match selector {
            "aws-rds-us-east-1-rsa2048-g1" => Some(Self::AwsRdsUsEast1Rsa2048G1),
            _ => None,
        }
    }
    pub(crate) fn apply(self, options: PgConnectOptions) -> Option<PgConnectOptions> {
        // A selected root must never be ignored on a local socket/insecure mode.
        if !matches!(options.get_ssl_mode(), PgSslMode::VerifyFull)
            || options.get_host().starts_with('/')
            || options.get_socket().is_some()
        {
            return None;
        }
        Some(options.ssl_root_cert_from_pem(
            include_bytes!("../certs/aws-rds-us-east-1-rsa2048-g1.pem").to_vec(),
        ))
    }
}

pub(crate) fn apply_selector(
    options: PgConnectOptions,
    selector: Option<&str>,
) -> Option<PgConnectOptions> {
    match selector {
        None => Some(options),
        Some(selector) => CaProfile::parse(selector)?.apply(options),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_profile_rejects_unknown_empty_and_insecure_modes() {
        for invalid in [
            "",
            "AWS-RDS-US-EAST-1-RSA2048-G1",
            "aws-rds-us-east-1-rsa2048-g1 ",
            "/tmp/root.pem",
            "https://example.invalid",
            "unknown",
        ] {
            assert!(CaProfile::parse(invalid).is_none());
        }
        let profile = CaProfile::parse("aws-rds-us-east-1-rsa2048-g1").unwrap();
        for mode in [
            PgSslMode::Disable,
            PgSslMode::Allow,
            PgSslMode::Prefer,
            PgSslMode::Require,
            PgSslMode::VerifyCa,
        ] {
            assert!(profile
                .apply(
                    PgConnectOptions::new_without_pgpass()
                        .host("explicit.invalid")
                        .ssl_mode(mode)
                )
                .is_none());
        }
        assert!(profile
            .apply(
                PgConnectOptions::new_without_pgpass()
                    .host("/synthetic/socket")
                    .ssl_mode(PgSslMode::VerifyFull)
            )
            .is_none());
        let options = PgConnectOptions::new_without_pgpass()
            .host("explicit.invalid")
            .ssl_mode(PgSslMode::VerifyFull);
        assert!(apply_selector(options.clone(), None).is_some());
        assert!(profile.apply(options).is_some());
    }
}
