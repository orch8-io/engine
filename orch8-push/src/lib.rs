mod apns;
mod fcm;
mod governance;
mod outbox;

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum PushError {
    #[error("push delivery failed: {0}")]
    Delivery(String),
    #[error("temporary push delivery failure: {0}")]
    Retryable(String),
    #[error("permanent push delivery failure: {0}")]
    Permanent(String),
    #[error("invalid push token")]
    InvalidToken,
    #[error("configuration error: {0}")]
    Config(String),
}

#[async_trait]
pub trait PushProvider: Send + Sync + 'static {
    /// Whether this provider can contact a real push service.
    fn is_configured(&self) -> bool {
        true
    }

    async fn send_silent_push(&self, token: &str, platform: &str) -> Result<(), PushError>;

    /// Send a silent wake carrying signed, non-sensitive command metadata.
    /// Built-in APNs/FCM implementations serialize the signature envelope;
    /// third-party providers must opt in explicitly or delivery fails closed.
    async fn send_signed_wake(
        &self,
        token: &str,
        platform: &str,
        metadata: &SignedWakeMetadata,
    ) -> Result<(), PushError> {
        let _ = (token, platform, metadata);
        Err(PushError::Config(
            "push provider does not support signed wake metadata".into(),
        ))
    }
}

pub struct NoopPushProvider;

#[async_trait]
impl PushProvider for NoopPushProvider {
    fn is_configured(&self) -> bool {
        false
    }

    async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
        tracing::debug!("noop push provider: silent push not sent");
        Ok(())
    }
}

pub use apns::ApnsProvider;
pub use fcm::FcmProvider;
pub use governance::{
    CollapsibleWake, CredentialRouter, EncryptedPushCredentialSource, PushCredentialRoute,
    PushGovernanceError, SignedWakeMetadata, TokenLifecycleState, WakeNonceCache, collapse_wakes,
};
pub use outbox::{
    ClaimedWake, PushOutboxStore, PushOutboxWorker, PushTerminalReason, WakeAttemptOutcome,
};

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

impl DispatchingPushProvider {
    fn provider_for(&self, platform: &str) -> Result<&dyn PushProvider, PushError> {
        match platform.to_ascii_lowercase().as_str() {
            "ios" => self
                .apns
                .as_ref()
                .map(|provider| provider as &dyn PushProvider)
                .ok_or_else(|| {
                    PushError::Config("no APNs provider configured for iOS device".into())
                }),
            "android" => self
                .fcm
                .as_ref()
                .map(|provider| provider as &dyn PushProvider)
                .ok_or_else(|| {
                    PushError::Config("no FCM provider configured for Android device".into())
                }),
            other => Err(PushError::Config(format!(
                "unknown device platform: {other}"
            ))),
        }
    }
}

#[async_trait]
impl PushProvider for DispatchingPushProvider {
    async fn send_silent_push(&self, token: &str, platform: &str) -> Result<(), PushError> {
        self.provider_for(platform)?
            .send_silent_push(token, platform)
            .await
    }

    async fn send_signed_wake(
        &self,
        token: &str,
        platform: &str,
        metadata: &SignedWakeMetadata,
    ) -> Result<(), PushError> {
        self.provider_for(platform)?
            .send_signed_wake(token, platform, metadata)
            .await
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

/// Read APNs / FCM provider configuration from environment variables.
///
/// APNs (all required once any is set): `ORCH8_APNS_KEY_PEM` (or
/// `ORCH8_APNS_KEY_PATH`), `ORCH8_APNS_KEY_ID`, `ORCH8_APNS_TEAM_ID`,
/// `ORCH8_APNS_TOPIC`; optional `ORCH8_APNS_SANDBOX=true`.
/// FCM: `ORCH8_FCM_PROJECT_ID` plus `ORCH8_FCM_SERVICE_ACCOUNT_JSON` (or
/// `ORCH8_FCM_SERVICE_ACCOUNT_PATH`).
///
/// Nothing set → `(None, None)` (Noop provider). A partially-set provider is
/// a configuration error rather than a silent Noop.
pub fn configs_from_env() -> Result<(Option<ApnsConfig>, Option<FcmConfig>), PushError> {
    configs_from_lookup(|key| std::env::var(key).ok().filter(|v| !v.trim().is_empty()))
}

fn configs_from_lookup(
    get: impl Fn(&str) -> Option<String>,
) -> Result<(Option<ApnsConfig>, Option<FcmConfig>), PushError> {
    let secret = |inline: &str, path: &str| -> Result<Option<String>, PushError> {
        if let Some(v) = get(inline) {
            return Ok(Some(v));
        }
        get(path)
            .map(|p| {
                std::fs::read_to_string(&p)
                    .map_err(|e| PushError::Config(format!("{path}: cannot read {p}: {e}")))
            })
            .transpose()
    };
    let missing = |provider: &str, key: &str| {
        PushError::Config(format!(
            "{provider} push is partially configured: {key} is not set"
        ))
    };

    let apns_key = secret("ORCH8_APNS_KEY_PEM", "ORCH8_APNS_KEY_PATH")?;
    let apns_any = apns_key.is_some()
        || [
            "ORCH8_APNS_KEY_ID",
            "ORCH8_APNS_TEAM_ID",
            "ORCH8_APNS_TOPIC",
        ]
        .iter()
        .any(|k| get(k).is_some());
    let apns = if apns_any {
        let need = |k: &str| get(k).ok_or_else(|| missing("APNs", k));
        Some(ApnsConfig {
            key_pem: apns_key
                .ok_or_else(|| missing("APNs", "ORCH8_APNS_KEY_PEM/ORCH8_APNS_KEY_PATH"))?
                .into(),
            key_id: need("ORCH8_APNS_KEY_ID")?,
            team_id: need("ORCH8_APNS_TEAM_ID")?,
            topic: need("ORCH8_APNS_TOPIC")?,
            sandbox: get("ORCH8_APNS_SANDBOX")
                .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true")),
        })
    } else {
        None
    };

    let fcm_json = secret(
        "ORCH8_FCM_SERVICE_ACCOUNT_JSON",
        "ORCH8_FCM_SERVICE_ACCOUNT_PATH",
    )?;
    let fcm_project = get("ORCH8_FCM_PROJECT_ID");
    let fcm = match (fcm_project, fcm_json) {
        (None, None) => None,
        (Some(project_id), Some(json)) => Some(FcmConfig {
            project_id,
            service_account_json: json.into(),
        }),
        (None, Some(_)) => return Err(missing("FCM", "ORCH8_FCM_PROJECT_ID")),
        (Some(_), None) => {
            return Err(missing(
                "FCM",
                "ORCH8_FCM_SERVICE_ACCOUNT_JSON/ORCH8_FCM_SERVICE_ACCOUNT_PATH",
            ));
        }
    };
    Ok((apns, fcm))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn env_config_absent_is_noop() {
        let (apns, fcm) = configs_from_lookup(lookup(&[])).unwrap();
        assert!(apns.is_none() && fcm.is_none());
    }

    #[test]
    fn env_config_builds_apns_and_fcm() {
        let (apns, fcm) = configs_from_lookup(lookup(&[
            ("ORCH8_APNS_KEY_PEM", "pem"),
            ("ORCH8_APNS_KEY_ID", "kid"),
            ("ORCH8_APNS_TEAM_ID", "team"),
            ("ORCH8_APNS_TOPIC", "com.acme"),
            ("ORCH8_APNS_SANDBOX", "true"),
            ("ORCH8_FCM_PROJECT_ID", "proj"),
            ("ORCH8_FCM_SERVICE_ACCOUNT_JSON", "{}"),
        ]))
        .unwrap();
        let apns = apns.unwrap();
        assert_eq!(apns.topic, "com.acme");
        assert!(apns.sandbox);
        assert_eq!(fcm.unwrap().project_id, "proj");
    }

    #[test]
    fn env_config_partial_is_an_error() {
        assert!(configs_from_lookup(lookup(&[("ORCH8_APNS_KEY_ID", "kid")])).is_err());
        assert!(configs_from_lookup(lookup(&[("ORCH8_FCM_PROJECT_ID", "p")])).is_err());
    }

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
    async fn create_provider_routes_signed_wakes_to_the_configured_platform() {
        let provider = create_provider(None, Some(fcm_test_config())).unwrap();
        let now = chrono::Utc::now();
        let metadata = SignedWakeMetadata::sign(
            "tenant",
            "device",
            "command",
            "key",
            &ed25519_dalek::SigningKey::from_bytes(&[7; 32]),
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        // Local provider validation proves forwarding without contacting FCM.
        let error = provider
            .send_signed_wake(&"a".repeat(513), "ANDROID", &metadata)
            .await
            .unwrap_err();
        assert!(matches!(error, PushError::InvalidToken));
        for platform in ["ios", "unknown"] {
            let error = provider
                .send_signed_wake("token", platform, &metadata)
                .await
                .unwrap_err();
            assert!(matches!(error, PushError::Config(_)));
        }
    }

    #[tokio::test]
    async fn create_provider_with_no_configs_is_noop() {
        let provider = create_provider(None, None).unwrap();
        provider.send_silent_push("tok", "ios").await.unwrap();
    }
}
