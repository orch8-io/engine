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

pub fn create_provider(
    apns: Option<ApnsConfig>,
    fcm: Option<FcmConfig>,
) -> Result<Box<dyn PushProvider>, PushError> {
    if let Some(cfg) = apns {
        return Ok(Box::new(ApnsProvider::new(cfg)?));
    }
    if let Some(cfg) = fcm {
        return Ok(Box::new(FcmProvider::new(cfg)?));
    }
    Ok(Box::new(NoopPushProvider))
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
}
