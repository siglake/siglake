//! AWS credential chain that fills the gap in reqsign 0.16.5: the ECS/Fargate
//! container-credentials endpoint is not part of `AwsDefaultLoader`'s chain.
//!
//! Resolution order (matches the AWS SDK default chain):
//!   1. Static keys   (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY)
//!   2. IRSA          (AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE → STS)
//!   3. ECS task role (AWS_CONTAINER_CREDENTIALS_RELATIVE_URI → 169.254.170.2)
//!   4. EC2 IMDSv2    (only when no ECS/Fargate env-vars are present)

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;

const ECS_BASE: &str = "http://169.254.170.2";
const IMDS_BASE: &str = "http://169.254.169.254";
const METADATA_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

struct AwsChainConfig {
    aws: reqsign::AwsConfig,
    ecs_url: Option<String>,
}

impl AwsChainConfig {
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let relative_uri = lookup("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        let full_uri = lookup("AWS_CONTAINER_CREDENTIALS_FULL_URI");
        Self {
            aws: reqsign::AwsConfig {
                access_key_id: lookup("AWS_ACCESS_KEY_ID"),
                secret_access_key: lookup("AWS_SECRET_ACCESS_KEY"),
                session_token: lookup("AWS_SESSION_TOKEN"),
                role_arn: lookup("AWS_ROLE_ARN"),
                web_identity_token_file: lookup("AWS_WEB_IDENTITY_TOKEN_FILE"),
                region: lookup("AWS_REGION").or_else(|| lookup("AWS_DEFAULT_REGION")),
                // We own IMDSv2 (step 4 below) so disable it here to avoid a
                // competing attempt that would race/double-timeout on Fargate.
                ec2_metadata_disabled: true,
                ..Default::default()
            },
            ecs_url: ecs_url_from(relative_uri.as_deref(), full_uri.as_deref()),
        }
    }
}

fn ecs_url_from(relative: Option<&str>, full: Option<&str>) -> Option<String> {
    relative
        .map(|relative| format!("{ECS_BASE}{relative}"))
        .or_else(|| full.map(str::to_owned))
}

/// AWS credential loader with full provider chain including ECS/Fargate task roles.
///
/// Drop-in replacement for reqsign's `AwsDefaultLoader` wherever opendal exposes
/// `customized_credential_load`. Handles the Fargate-without-IRSA case that causes
/// reqsign 0.16.5 to time out waiting for IMDSv2 at 169.254.169.254.
pub struct SiglakeAwsLoader {
    inner: reqsign::AwsDefaultLoader,
    client: reqwest::Client,
    ecs_url: Option<String>,
    ecs_cache: Mutex<Option<reqsign::AwsCredential>>,
    imds_cache: Mutex<Option<reqsign::AwsCredential>>,
}

impl SiglakeAwsLoader {
    pub fn new() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(METADATA_TIMEOUT)
            .build()
            .unwrap_or_default();
        // Populate from env vars at construction; only the file *content* of
        // AWS_WEB_IDENTITY_TOKEN_FILE rotates — reqsign reads the file fresh
        // each IRSA exchange, so path-at-startup is correct.
        let cfg = AwsChainConfig::from_lookup(lookup);
        Self {
            inner: reqsign::AwsDefaultLoader::new(client.clone(), cfg.aws),
            client,
            ecs_url: cfg.ecs_url,
            ecs_cache: Mutex::new(None),
            imds_cache: Mutex::new(None),
        }
    }

    async fn load_ecs(&self) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        let Some(url) = self.ecs_url.as_deref() else {
            return Ok(None);
        };

        {
            let lock = self.ecs_cache.lock().await;
            if let Some(c) = lock.as_ref() {
                if c.is_valid() {
                    return Ok(Some(c.clone()));
                }
            }
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Resp {
            access_key_id: String,
            secret_access_key: String,
            token: String,
            expiration: String,
        }

        let resp = self.client.get(url).send().await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let r: Resp = resp.json().await?;
        let cred = reqsign::AwsCredential {
            access_key_id: r.access_key_id,
            secret_access_key: r.secret_access_key,
            session_token: Some(r.token),
            expires_in: chrono::DateTime::parse_from_rfc3339(&r.expiration)
                .ok()
                .map(|d| d.with_timezone(&chrono::Utc)),
        };
        *self.ecs_cache.lock().await = Some(cred.clone());
        Ok(Some(cred))
    }

    async fn load_imds(&self) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        {
            let lock = self.imds_cache.lock().await;
            if let Some(c) = lock.as_ref() {
                if c.is_valid() {
                    return Ok(Some(c.clone()));
                }
            }
        }

        // IMDSv2 step 1: get session token
        let token_resp = self
            .client
            .put(format!("{IMDS_BASE}/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()
            .await?;
        if !token_resp.status().is_success() {
            return Ok(None);
        }
        let token = token_resp.text().await?;

        // Step 2: get IAM role name attached to instance
        let role_resp = self
            .client
            .get(format!(
                "{IMDS_BASE}/latest/meta-data/iam/security-credentials/"
            ))
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .await?;
        if !role_resp.status().is_success() {
            return Ok(None);
        }
        let role = role_resp.text().await?;
        let role = role.lines().next().unwrap_or("").trim().to_string();
        if role.is_empty() {
            return Ok(None);
        }

        // Step 3: get temporary credentials for that role
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct ImdsResp {
            access_key_id: String,
            secret_access_key: String,
            token: String,
            expiration: String,
        }

        let cred_resp = self
            .client
            .get(format!(
                "{IMDS_BASE}/latest/meta-data/iam/security-credentials/{role}"
            ))
            .header("X-aws-ec2-metadata-token", &token)
            .send()
            .await?;
        if !cred_resp.status().is_success() {
            return Ok(None);
        }
        let r: ImdsResp = cred_resp.json().await?;
        let cred = reqsign::AwsCredential {
            access_key_id: r.access_key_id,
            secret_access_key: r.secret_access_key,
            session_token: Some(r.token),
            expires_in: chrono::DateTime::parse_from_rfc3339(&r.expiration)
                .ok()
                .map(|d| d.with_timezone(&chrono::Utc)),
        };
        *self.imds_cache.lock().await = Some(cred.clone());
        Ok(Some(cred))
    }
}

impl Default for SiglakeAwsLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl reqsign::AwsCredentialLoad for SiglakeAwsLoader {
    async fn load_credential(
        &self,
        client: reqwest::Client,
    ) -> anyhow::Result<Option<reqsign::AwsCredential>> {
        // Steps 1+2: static keys + IRSA (inner loader has EC2 metadata disabled)
        if let Some(c) = self.inner.load_credential(client).await? {
            return Ok(Some(c));
        }
        // Step 3: ECS/Fargate task role via container credentials endpoint
        if let Some(c) = self.load_ecs().await? {
            return Ok(Some(c));
        }
        // Step 4: EC2 IMDSv2 — skipped when ECS env-vars indicate Fargate/ECS
        if self.ecs_url.is_none() {
            if let Some(c) = self.load_imds().await? {
                return Ok(Some(c));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_credentials_populate_aws_config() {
        let cfg = AwsChainConfig::from_lookup(|key| match key {
            "AWS_ACCESS_KEY_ID" => Some("access-key".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret-key".to_owned()),
            "AWS_SESSION_TOKEN" => Some("session-token".to_owned()),
            _ => None,
        });

        assert_eq!(cfg.aws.access_key_id.as_deref(), Some("access-key"));
        assert_eq!(cfg.aws.secret_access_key.as_deref(), Some("secret-key"));
        assert_eq!(cfg.aws.session_token.as_deref(), Some("session-token"));
    }

    #[test]
    fn aws_region_wins_over_default_region() {
        let cfg = AwsChainConfig::from_lookup(|key| match key {
            "AWS_REGION" => Some("preferred-region".to_owned()),
            "AWS_DEFAULT_REGION" => Some("fallback-region".to_owned()),
            _ => None,
        });

        assert_eq!(cfg.aws.region.as_deref(), Some("preferred-region"));
    }

    #[test]
    fn relative_ecs_uri_is_prefixed_and_wins_over_full_uri() {
        assert_eq!(
            ecs_url_from(Some("/v2/credentials"), Some("http://example.invalid/full")),
            Some("http://169.254.170.2/v2/credentials".to_owned())
        );
    }

    #[test]
    fn no_ecs_uri_leaves_imds_eligible() {
        let cfg = AwsChainConfig::from_lookup(|_| None);

        assert!(cfg.ecs_url.is_none());
    }

    #[test]
    fn inner_loader_always_has_ec2_metadata_disabled() {
        let cfg = AwsChainConfig::from_lookup(|_| None);

        assert!(cfg.aws.ec2_metadata_disabled);
    }
}
