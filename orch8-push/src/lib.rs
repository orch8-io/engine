mod apns;
mod fcm;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("push delivery failed: {0}")]
    Delivery(String),
    #[error("invalid push token")]
    InvalidToken,
    #[error("configuration error: {0}")]
    Config(String),
}

#[async_trait]
pub trait PushProvider: Send + Sync + 'static {
    async fn send_silent_push(&self, token: &str, platform: &str) -> Result<(), PushError>;
}

pub struct NoopPushProvider;

#[async_trait]
impl PushProvider for NoopPushProvider {
    async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
        tracing::debug!("noop push provider: silent push not sent");
        Ok(())
    }
}

pub use apns::ApnsProvider;
pub use fcm::FcmProvider;

/// Secret material that is redacted from debug output and wiped on drop.
#[derive(Clone)]
pub struct PushSecret(String);

impl PushSecret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for PushSecret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for PushSecret {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl std::fmt::Debug for PushSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl Drop for PushSecret {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.0.zeroize();
    }
}

#[derive(Debug, Clone)]
pub struct ApnsConfig {
    pub key_pem: PushSecret,
    pub key_id: String,
    pub team_id: String,
    pub topic: String,
    pub sandbox: bool,
}

#[derive(Debug, Clone)]
pub struct FcmConfig {
    pub project_id: String,
    pub service_account_json: PushSecret,
}

pub(crate) fn safe_prefix(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

/// Routes each push to the provider matching the device's platform. Neither
/// `ApnsProvider` nor `FcmProvider` looks at the `platform` argument, so
/// without this dispatch a single configured provider would happily send iOS
/// tokens to FCM (or vice versa) and misclassify the resulting vendor error.
struct DispatchingPushProvider {
    apns: Option<ApnsProvider>,
    fcm: Option<FcmProvider>,
}

#[async_trait]
impl PushProvider for DispatchingPushProvider {
    async fn send_silent_push(&self, token: &str, platform: &str) -> Result<(), PushError> {
        match platform.to_ascii_lowercase().as_str() {
            "ios" => match &self.apns {
                Some(provider) => provider.send_silent_push(token, platform).await,
                None => Err(PushError::Config(
                    "no APNs provider configured for iOS device".to_string(),
                )),
            },
            "android" => match &self.fcm {
                Some(provider) => provider.send_silent_push(token, platform).await,
                None => Err(PushError::Config(
                    "no FCM provider configured for Android device".to_string(),
                )),
            },
            other => Err(PushError::Config(format!(
                "unknown device platform: {other}"
            ))),
        }
    }
}

pub fn create_provider(
    apns: Option<ApnsConfig>,
    fcm: Option<FcmConfig>,
) -> Result<Box<dyn PushProvider>, PushError> {
    let apns = apns.map(ApnsProvider::new).transpose()?;
    let fcm = fcm.map(FcmProvider::new).transpose()?;
    if apns.is_none() && fcm.is_none() {
        return Ok(Box::new(NoopPushProvider));
    }
    Ok(Box::new(DispatchingPushProvider { apns, fcm }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fcm_rejects_unexpected_token_uri() {
        let cfg = FcmConfig {
            project_id: "p".into(),
            service_account_json: r#"{
                "client_email": "x@p.iam.gserviceaccount.com",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiMLAHnoDX\n-----END RSA PRIVATE KEY-----",
                "token_uri": "https://attacker.example.com/token"
            }"#
            .into(),
        };
        let Err(err) = FcmProvider::new(cfg) else {
            panic!("unexpected token_uri must fail");
        };
        assert!(err.to_string().contains("token_uri"));
    }

    #[tokio::test]
    async fn fcm_rejects_oversized_token() {
        let cfg = FcmConfig {
            project_id: "p".into(),
            service_account_json: r#"{
                "client_email": "x@p.iam.gserviceaccount.com",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiMLAHnoDX\n-----END RSA PRIVATE KEY-----",
                "token_uri": "https://oauth2.googleapis.com/token"
            }"#
            .into(),
        };
        let provider = FcmProvider::new(cfg).unwrap();
        let long_token = "a".repeat(513);
        let err = provider
            .send_silent_push(&long_token, "android")
            .await
            .expect_err("long token must be rejected");
        assert!(matches!(err, PushError::InvalidToken));
    }

    fn fcm_test_config() -> FcmConfig {
        FcmConfig {
            project_id: "p".into(),
            service_account_json: r#"{
                "client_email": "x@p.iam.gserviceaccount.com",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiMLAHnoDX\n-----END RSA PRIVATE KEY-----",
                "token_uri": "https://oauth2.googleapis.com/token"
            }"#
            .into(),
        }
    }

    #[tokio::test]
    async fn create_provider_dispatches_on_platform() {
        // Only FCM configured: an iOS device must get a config error instead
        // of the token being misdirected to FCM.
        let provider = create_provider(None, Some(fcm_test_config())).unwrap();
        let err = provider
            .send_silent_push("tok", "ios")
            .await
            .expect_err("iOS without APNs must be a config error");
        assert!(matches!(err, PushError::Config(_)));

        // Unknown platforms are rejected rather than guessed.
        let err = provider
            .send_silent_push("tok", "windows")
            .await
            .expect_err("unknown platform must be a config error");
        assert!(matches!(err, PushError::Config(_)));
    }

    #[tokio::test]
    async fn create_provider_with_no_configs_is_noop() {
        let provider = create_provider(None, None).unwrap();
        provider.send_silent_push("tok", "ios").await.unwrap();
    }
}
