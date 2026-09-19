//! Candidate reqsign 3 AWS credential adapter.
//!
//! The provider order matches the production adapter: static keys, IRSA,
//! ECS/container credentials, then IMDSv2. An explicitly configured ECS
//! endpoint suppresses IMDS rather than falling through after an ECS failure.

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use reqsign_aws_v4::{
    AssumeRoleWithWebIdentityCredentialProvider, Credential, ECSCredentialProvider,
    IMDSv2CredentialProvider, StaticCredentialProvider,
};
use reqsign_core::{Context, Error, ProvideCredential, ProvideCredentialDyn, Result};

const METADATA_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct StaticConfig {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Debug)]
struct IrsaConfig {
    role_arn: String,
    token_file: PathBuf,
    region: Option<String>,
    session_name: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum EcsConfig {
    Relative(String),
    Full(String),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Static,
    Irsa,
    Ecs,
    Imds,
}

#[derive(Debug)]
struct AwsChainConfig {
    static_keys: Option<StaticConfig>,
    irsa: Option<IrsaConfig>,
    ecs: Option<EcsConfig>,
    imds: bool,
}

impl AwsChainConfig {
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let static_keys = match (lookup("AWS_ACCESS_KEY_ID"), lookup("AWS_SECRET_ACCESS_KEY")) {
            (Some(access_key_id), Some(secret_access_key)) => Some(StaticConfig {
                access_key_id,
                secret_access_key,
                session_token: lookup("AWS_SESSION_TOKEN"),
            }),
            _ => None,
        };
        let region = lookup("AWS_REGION").or_else(|| lookup("AWS_DEFAULT_REGION"));
        let irsa = match (
            lookup("AWS_ROLE_ARN"),
            lookup("AWS_WEB_IDENTITY_TOKEN_FILE"),
        ) {
            (Some(role_arn), Some(token_file)) => Some(IrsaConfig {
                role_arn,
                token_file: token_file.into(),
                region,
                session_name: lookup("AWS_ROLE_SESSION_NAME"),
            }),
            _ => None,
        };
        let ecs = lookup("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            .map(EcsConfig::Relative)
            .or_else(|| lookup("AWS_CONTAINER_CREDENTIALS_FULL_URI").map(EcsConfig::Full));
        Self {
            static_keys,
            irsa,
            imds: ecs.is_none(),
            ecs,
        }
    }

    #[cfg(test)]
    fn provider_kinds(&self) -> Vec<ProviderKind> {
        let mut kinds = Vec::with_capacity(4);
        if self.static_keys.is_some() {
            kinds.push(ProviderKind::Static);
        }
        if self.irsa.is_some() {
            kinds.push(ProviderKind::Irsa);
        }
        if self.ecs.is_some() {
            kinds.push(ProviderKind::Ecs);
        }
        if self.imds {
            kinds.push(ProviderKind::Imds);
        }
        kinds
    }

    fn into_providers(self) -> Vec<Box<dyn ProvideCredentialDyn<Credential = Credential>>> {
        let mut providers: Vec<Box<dyn ProvideCredentialDyn<Credential = Credential>>> =
            Vec::with_capacity(4);
        if let Some(config) = self.static_keys {
            let mut provider =
                StaticCredentialProvider::new(&config.access_key_id, &config.secret_access_key);
            if let Some(token) = config.session_token.as_deref() {
                provider = provider.with_session_token(token);
            }
            providers.push(Box::new(provider));
        }
        if let Some(config) = self.irsa {
            let mut provider = AssumeRoleWithWebIdentityCredentialProvider::with_config(
                config.role_arn,
                config.token_file,
            );
            if let Some(region) = config.region {
                provider = provider.with_region(region);
            }
            if let Some(session_name) = config.session_name {
                provider = provider.with_role_session_name(session_name);
            }
            providers.push(Box::new(provider));
        }
        if let Some(config) = self.ecs {
            let provider = match config {
                EcsConfig::Relative(uri) => ECSCredentialProvider::new().with_relative_uri(uri),
                EcsConfig::Full(uri) => ECSCredentialProvider::new().with_endpoint(uri),
            };
            providers.push(Box::new(provider));
        }
        if self.imds {
            providers.push(Box::new(IMDSv2CredentialProvider::new()));
        }
        providers
    }
}

/// Full Siglake AWS provider chain for reqsign 3.
pub struct SiglakeAwsLoader {
    providers: Vec<Box<dyn ProvideCredentialDyn<Credential = Credential>>>,
    timeout: Duration,
}

impl Debug for SiglakeAwsLoader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SiglakeAwsLoader")
            .field("providers", &self.providers.len())
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Default for SiglakeAwsLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl SiglakeAwsLoader {
    pub fn new() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        Self {
            providers: AwsChainConfig::from_lookup(lookup).into_providers(),
            timeout: METADATA_TIMEOUT,
        }
    }

    #[cfg(test)]
    fn from_providers(
        providers: Vec<Box<dyn ProvideCredentialDyn<Credential = Credential>>>,
        timeout: Duration,
    ) -> Self {
        Self { providers, timeout }
    }
}

impl ProvideCredential for SiglakeAwsLoader {
    type Credential = Credential;

    async fn provide_credential(&self, ctx: &Context) -> Result<Option<Self::Credential>> {
        for provider in &self.providers {
            match tokio::time::timeout(self.timeout, provider.provide_credential_dyn(ctx)).await {
                Ok(Ok(Some(credential))) => return Ok(Some(credential)),
                Ok(Ok(None)) => {}
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(Error::unexpected("AWS credential provider timed out")
                        .with_context(format!("timeout: {}s", self.timeout.as_secs_f64()))
                        .set_retryable(true));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use reqsign_core::time::Timestamp;
    use reqsign_core::{FileRead, HttpSend, StaticEnv};

    use super::*;

    fn config(entries: &[(&str, &str)]) -> AwsChainConfig {
        AwsChainConfig::from_lookup(|key| {
            entries
                .iter()
                .find_map(|(candidate, value)| (*candidate == key).then(|| (*value).to_owned()))
        })
    }

    #[test]
    fn pure_resolver_preserves_precedence_and_ecs_uri_rules() {
        let cfg = config(&[
            ("AWS_ACCESS_KEY_ID", "static-ak"),
            ("AWS_SECRET_ACCESS_KEY", "static-sk"),
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/test"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/token"),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/relative"),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://full.invalid"),
        ]);
        assert_eq!(
            cfg.provider_kinds(),
            vec![ProviderKind::Static, ProviderKind::Irsa, ProviderKind::Ecs]
        );
        assert_eq!(cfg.ecs, Some(EcsConfig::Relative("/relative".to_owned())));
        assert!(!cfg.imds, "configured ECS must suppress IMDS fallback");

        let cfg = config(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://full.invalid/credentials",
        )]);
        assert_eq!(
            cfg.ecs,
            Some(EcsConfig::Full(
                "http://full.invalid/credentials".to_owned()
            ))
        );
        assert_eq!(cfg.provider_kinds(), vec![ProviderKind::Ecs]);

        assert_eq!(config(&[]).provider_kinds(), vec![ProviderKind::Imds]);
    }

    #[derive(Debug)]
    enum Outcome {
        None,
        Credential,
        Error,
        Pending,
    }

    #[derive(Debug)]
    struct TestProvider {
        outcome: Outcome,
        calls: Arc<AtomicUsize>,
    }

    impl ProvideCredential for TestProvider {
        type Credential = Credential;

        async fn provide_credential(&self, _: &Context) -> Result<Option<Self::Credential>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            match self.outcome {
                Outcome::None => Ok(None),
                Outcome::Credential => Ok(Some(Credential {
                    access_key_id: format!("rotating-{call}"),
                    secret_access_key: "secret".to_owned(),
                    session_token: Some("token".to_owned()),
                    expires_in: Some(Timestamp::now() + Duration::from_secs(600 + call as u64)),
                })),
                Outcome::Error => Err(Error::unexpected("provider failed")),
                Outcome::Pending => pending().await,
            }
        }
    }

    fn provider(
        outcome: Outcome,
    ) -> (
        Box<dyn ProvideCredentialDyn<Credential = Credential>>,
        Arc<AtomicUsize>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Box::new(TestProvider {
                outcome,
                calls: Arc::clone(&calls),
            }),
            calls,
        )
    }

    #[tokio::test]
    async fn chain_rotates_credentials_and_stops_at_first_success() {
        let (empty, empty_calls) = provider(Outcome::None);
        let (rotating, rotating_calls) = provider(Outcome::Credential);
        let (imds, imds_calls) = provider(Outcome::Credential);
        let loader =
            SiglakeAwsLoader::from_providers(vec![empty, rotating, imds], Duration::from_secs(1));
        let ctx = Context::new();

        let first = loader
            .provide_credential(&ctx)
            .await
            .expect("first load")
            .expect("credential");
        let second = loader
            .provide_credential(&ctx)
            .await
            .expect("second load")
            .expect("credential");
        assert_eq!(first.access_key_id, "rotating-0");
        assert_eq!(second.access_key_id, "rotating-1");
        assert_ne!(first.expires_in, second.expires_in);
        assert_eq!(empty_calls.load(Ordering::SeqCst), 2);
        assert_eq!(rotating_calls.load(Ordering::SeqCst), 2);
        assert_eq!(imds_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn provider_failures_and_timeouts_propagate_without_fallback() {
        let (failing, failing_calls) = provider(Outcome::Error);
        let (fallback, fallback_calls) = provider(Outcome::Credential);
        let loader =
            SiglakeAwsLoader::from_providers(vec![failing, fallback], Duration::from_secs(1));
        assert!(loader.provide_credential(&Context::new()).await.is_err());
        assert_eq!(failing_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_calls.load(Ordering::SeqCst), 0);

        let (hanging, hanging_calls) = provider(Outcome::Pending);
        let loader = SiglakeAwsLoader::from_providers(vec![hanging], Duration::from_millis(10));
        let error = loader
            .provide_credential(&Context::new())
            .await
            .expect_err("timeout must be visible");
        assert!(error.is_retryable());
        assert_eq!(hanging_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resolved_static_keys_are_usable_without_network_or_environment() {
        let loader = SiglakeAwsLoader::from_lookup(|key| match key {
            "AWS_ACCESS_KEY_ID" => Some("access".to_owned()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_owned()),
            "AWS_SESSION_TOKEN" => Some("session".to_owned()),
            _ => None,
        });
        let credential = loader
            .provide_credential(&Context::new())
            .await
            .expect("static provider succeeds")
            .expect("credential");
        assert_eq!(credential.access_key_id, "access");
        assert_eq!(credential.secret_access_key, "secret");
        assert_eq!(credential.session_token.as_deref(), Some("session"));
    }

    #[derive(Clone, Debug, Default)]
    struct MetadataFixture {
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl FileRead for MetadataFixture {
        async fn file_read(&self, path: &str) -> Result<Vec<u8>> {
            assert_eq!(path, "/var/run/token");
            Ok(b"web-identity-token".to_vec())
        }
    }

    impl HttpSend for MetadataFixture {
        async fn http_send(&self, request: http::Request<Bytes>) -> Result<http::Response<Bytes>> {
            let uri = request.uri().to_string();
            self.requests.lock().unwrap().push(uri.clone());
            let body = if uri.contains("sts.amazonaws.com") {
                r#"<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult><Credentials>
    <AccessKeyId>irsa-access</AccessKeyId>
    <SecretAccessKey>irsa-secret</SecretAccessKey>
    <SessionToken>irsa-token</SessionToken>
    <Expiration>2124-05-25T11:45:17Z</Expiration>
  </Credentials></AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#
            } else if uri == "http://ecs.test/credentials" {
                r#"{"AccessKeyId":"ecs-access","SecretAccessKey":"ecs-secret","Token":"ecs-token","Expiration":"2124-05-25T11:45:17Z"}"#
            } else if uri.ends_with("/latest/api/token") {
                "imds-token"
            } else if uri.ends_with("/latest/meta-data/iam/security-credentials/") {
                "test-role"
            } else if uri.ends_with("/latest/meta-data/iam/security-credentials/test-role") {
                r#"{"Code":"Success","AccessKeyId":"imds-access","SecretAccessKey":"imds-secret","Token":"imds-token","Expiration":"2124-05-25T11:45:17Z"}"#
            } else {
                return Err(Error::unexpected(format!(
                    "unexpected metadata request: {uri}"
                )));
            };
            http::Response::builder()
                .status(200)
                .body(Bytes::copy_from_slice(body.as_bytes()))
                .map_err(Error::from)
        }
    }

    fn metadata_context(fixture: MetadataFixture) -> Context {
        Context::new()
            .with_file_read(fixture.clone())
            .with_http_send(fixture)
            .with_env(StaticEnv {
                home_dir: None,
                envs: HashMap::new(),
            })
    }

    #[tokio::test]
    async fn real_irsa_ecs_and_imdsv2_providers_are_hermetic() {
        let irsa_io = MetadataFixture::default();
        let irsa = SiglakeAwsLoader::from_lookup(|key| match key {
            "AWS_ROLE_ARN" => Some("arn:aws:iam::1:role/test".to_owned()),
            "AWS_WEB_IDENTITY_TOKEN_FILE" => Some("/var/run/token".to_owned()),
            _ => None,
        })
        .provide_credential(&metadata_context(irsa_io.clone()))
        .await
        .expect("IRSA succeeds")
        .expect("IRSA credential");
        assert_eq!(irsa.access_key_id, "irsa-access");
        assert_eq!(irsa_io.requests.lock().unwrap().len(), 1);

        let ecs_io = MetadataFixture::default();
        let ecs = SiglakeAwsLoader::from_lookup(|key| {
            (key == "AWS_CONTAINER_CREDENTIALS_FULL_URI")
                .then(|| "http://ecs.test/credentials".to_owned())
        })
        .provide_credential(&metadata_context(ecs_io.clone()))
        .await
        .expect("ECS succeeds")
        .expect("ECS credential");
        assert_eq!(ecs.access_key_id, "ecs-access");
        assert_eq!(
            ecs_io.requests.lock().unwrap().as_slice(),
            ["http://ecs.test/credentials"]
        );

        let imds_io = MetadataFixture::default();
        let imds = SiglakeAwsLoader::from_lookup(|_| None)
            .provide_credential(&metadata_context(imds_io.clone()))
            .await
            .expect("IMDS succeeds")
            .expect("IMDS credential");
        assert_eq!(imds.access_key_id, "imds-access");
        let requests = imds_io.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].ends_with("/latest/api/token"));
        assert!(requests[1].ends_with("/latest/meta-data/iam/security-credentials/"));
        assert!(requests[2].ends_with("/latest/meta-data/iam/security-credentials/test-role"));
    }
}
