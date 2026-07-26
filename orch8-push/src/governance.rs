//! Tenant-safe push routing, collapse, signatures, and token lifecycle.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const MAX_WAKE_TTL: Duration = Duration::minutes(15);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PushGovernanceError {
    #[error("invalid push governance input: {0}")]
    Invalid(String),
    #[error("no credential route for tenant/application/topic")]
    MissingCredential,
    #[error("wake signature is invalid")]
    InvalidSignature,
    #[error("wake is expired or exceeds the 15 minute TTL")]
    InvalidExpiry,
    #[error("wake nonce was already consumed")]
    Replay,
}

/// A route points only at an encrypted credential record. Request payloads
/// are intentionally absent, preventing secrets from being selected by data
/// controlled by a workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushCredentialRoute {
    pub tenant_id: String,
    pub application_id: String,
    pub topic: String,
    pub encrypted_credential_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct CredentialRouter {
    routes: BTreeMap<(String, String, String), String>,
}

impl CredentialRouter {
    pub fn new(
        routes: impl IntoIterator<Item = PushCredentialRoute>,
    ) -> Result<Self, PushGovernanceError> {
        let mut catalog = BTreeMap::new();
        for route in routes {
            if [
                route.tenant_id.as_str(),
                route.application_id.as_str(),
                route.topic.as_str(),
                route.encrypted_credential_id.as_str(),
            ]
            .iter()
            .any(|value| value.is_empty())
            {
                return Err(PushGovernanceError::Invalid(
                    "credential routes require exact non-empty fields".into(),
                ));
            }
            let key = (route.tenant_id, route.application_id, route.topic);
            if catalog.insert(key, route.encrypted_credential_id).is_some() {
                return Err(PushGovernanceError::Invalid(
                    "duplicate credential route".into(),
                ));
            }
        }
        Ok(Self { routes: catalog })
    }

    pub fn resolve(
        &self,
        tenant_id: &str,
        application_id: &str,
        topic: &str,
    ) -> Result<&str, PushGovernanceError> {
        self.routes
            .get(&(tenant_id.into(), application_id.into(), topic.into()))
            .map(String::as_str)
            .ok_or(PushGovernanceError::MissingCredential)
    }

    /// Resolve the route first, then ask the encrypted credential boundary to
    /// construct a provider. The boundary receives only an operator-defined
    /// credential id, never workflow/request payload data.
    pub async fn provider_for(
        &self,
        source: &dyn EncryptedPushCredentialSource,
        tenant_id: &str,
        application_id: &str,
        topic: &str,
    ) -> Result<Arc<dyn crate::PushProvider>, PushGovernanceError> {
        source
            .load_provider(self.resolve(tenant_id, application_id, topic)?)
            .await
    }
}

#[async_trait]
pub trait EncryptedPushCredentialSource: Send + Sync {
    async fn load_provider(
        &self,
        encrypted_credential_id: &str,
    ) -> Result<Arc<dyn crate::PushProvider>, PushGovernanceError>;
}

/// Non-sensitive vendor payload. It contains no sequence, state, context, or
/// command body; the device fetches the durable command after verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedWakeMetadata {
    pub schema_version: u32,
    pub tenant_id: String,
    pub device_id: String,
    pub command_id: String,
    pub nonce: Uuid,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub key_id: String,
    pub signature: String,
}

#[derive(Serialize)]
struct WakeClaims<'a> {
    schema_version: u32,
    tenant_id: &'a str,
    device_id: &'a str,
    command_id: &'a str,
    nonce: Uuid,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    key_id: &'a str,
}

impl SignedWakeMetadata {
    pub fn sign(
        tenant_id: impl Into<String>,
        device_id: impl Into<String>,
        command_id: impl Into<String>,
        key_id: impl Into<String>,
        signing_key: &SigningKey,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, PushGovernanceError> {
        let mut wake = Self {
            schema_version: 1,
            tenant_id: tenant_id.into(),
            device_id: device_id.into(),
            command_id: command_id.into(),
            nonce: Uuid::new_v4(),
            issued_at,
            expires_at,
            key_id: key_id.into(),
            signature: String::new(),
        };
        wake.validate_shape()?;
        let digest = wake.digest()?;
        wake.signature = URL_SAFE_NO_PAD.encode(signing_key.sign(&digest).to_bytes());
        Ok(wake)
    }

    pub fn verify(
        &self,
        expected_tenant: &str,
        expected_device: &str,
        now: DateTime<Utc>,
        verifying_key: &VerifyingKey,
        consumed_nonces: &mut WakeNonceCache,
    ) -> Result<(), PushGovernanceError> {
        self.validate_shape()?;
        if self.tenant_id != expected_tenant || self.device_id != expected_device {
            return Err(PushGovernanceError::InvalidSignature);
        }
        if now < self.issued_at || now >= self.expires_at {
            return Err(PushGovernanceError::InvalidExpiry);
        }
        let signature_bytes: [u8; 64] = URL_SAFE_NO_PAD
            .decode(&self.signature)
            .map_err(|_| PushGovernanceError::InvalidSignature)?
            .try_into()
            .map_err(|_| PushGovernanceError::InvalidSignature)?;
        verifying_key
            .verify(&self.digest()?, &Signature::from_bytes(&signature_bytes))
            .map_err(|_| PushGovernanceError::InvalidSignature)?;
        consumed_nonces.consume(self.nonce, self.expires_at, now)
    }

    fn validate_shape(&self) -> Result<(), PushGovernanceError> {
        if self.schema_version != 1
            || [
                self.tenant_id.as_str(),
                self.device_id.as_str(),
                self.command_id.as_str(),
                self.key_id.as_str(),
            ]
            .iter()
            .any(|value| value.is_empty() || value.len() > 256)
            || self.expires_at <= self.issued_at
            || self.expires_at - self.issued_at > MAX_WAKE_TTL
        {
            return Err(PushGovernanceError::InvalidExpiry);
        }
        Ok(())
    }

    fn digest(&self) -> Result<Vec<u8>, PushGovernanceError> {
        let claims = WakeClaims {
            schema_version: self.schema_version,
            tenant_id: &self.tenant_id,
            device_id: &self.device_id,
            command_id: &self.command_id,
            nonce: self.nonce,
            issued_at: self.issued_at,
            expires_at: self.expires_at,
            key_id: &self.key_id,
        };
        let encoded = serde_json::to_vec(&claims)
            .map_err(|error| PushGovernanceError::Invalid(error.to_string()))?;
        Ok(Sha256::digest(encoded).to_vec())
    }
}

/// Bounded replay cache. Expired nonces are discarded before every insert;
/// when full, the earliest-expiring entry is evicted deterministically.
#[derive(Debug)]
pub struct WakeNonceCache {
    entries: BTreeMap<Uuid, DateTime<Utc>>,
    max_entries: usize,
}

impl WakeNonceCache {
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            max_entries: max_entries.clamp(1, 100_000),
        }
    }

    fn consume(
        &mut self,
        nonce: Uuid,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), PushGovernanceError> {
        self.entries.retain(|_, expiry| *expiry > now);
        if self.entries.contains_key(&nonce) {
            return Err(PushGovernanceError::Replay);
        }
        if self.entries.len() >= self.max_entries
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, expiry)| **expiry)
                .map(|(nonce, _)| *nonce)
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(nonce, expires_at);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollapsibleWake {
    pub tenant_id: String,
    pub device_id: String,
    pub execution_id: String,
    pub topic: String,
    pub command_id: String,
    pub created_at: DateTime<Utc>,
}

impl CollapsibleWake {
    #[must_use]
    pub fn collapse_key(&self) -> String {
        let mut hasher = Sha256::new();
        for value in [
            &self.tenant_id,
            &self.device_id,
            &self.execution_id,
            &self.topic,
        ] {
            hasher.update(value.len().to_be_bytes());
            hasher.update(value.as_bytes());
        }
        hex::encode(hasher.finalize())
    }
}

/// Retains the newest command for the same execution/topic while preserving
/// separate executions even when they target one device.
#[must_use]
pub fn collapse_wakes(wakes: impl IntoIterator<Item = CollapsibleWake>) -> Vec<CollapsibleWake> {
    let mut newest = BTreeMap::<String, CollapsibleWake>::new();
    for wake in wakes {
        let key = wake.collapse_key();
        let replace = newest.get(&key).is_none_or(|current| {
            (wake.created_at, &wake.command_id) > (current.created_at, &current.command_id)
        });
        if replace {
            newest.insert(key, wake);
        }
    }
    newest.into_values().collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenLifecycleState {
    pub tenant_id: String,
    pub device_id: String,
    pub active: bool,
    pub quarantined_at: Option<DateTime<Utc>>,
    pub quarantine_reason: Option<String>,
}

impl TokenLifecycleState {
    pub fn quarantine_invalid_token(&mut self, now: DateTime<Utc>) {
        self.active = false;
        self.quarantined_at = Some(now);
        self.quarantine_reason = Some("provider_invalid_token".into());
    }

    pub fn reactivate_with_new_token(&mut self) {
        self.active = true;
        self.quarantined_at = None;
        self.quarantine_reason = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_routes_are_exact_and_cross_tenant_safe() {
        let router = CredentialRouter::new([PushCredentialRoute {
            tenant_id: "tenant-a".into(),
            application_id: "field".into(),
            topic: "com.acme.field".into(),
            encrypted_credential_id: "credential/apns-field".into(),
        }])
        .unwrap();
        assert_eq!(
            router
                .resolve("tenant-a", "field", "com.acme.field")
                .unwrap(),
            "credential/apns-field"
        );
        assert_eq!(
            router.resolve("tenant-b", "field", "com.acme.field"),
            Err(PushGovernanceError::MissingCredential)
        );
    }

    struct RecordingCredentialSource(std::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl EncryptedPushCredentialSource for RecordingCredentialSource {
        async fn load_provider(
            &self,
            encrypted_credential_id: &str,
        ) -> Result<Arc<dyn crate::PushProvider>, PushGovernanceError> {
            self.0.lock().unwrap().push(encrypted_credential_id.into());
            Ok(Arc::new(crate::NoopPushProvider))
        }
    }

    #[tokio::test]
    async fn provider_resolution_passes_only_encrypted_record_id() {
        let router = CredentialRouter::new([PushCredentialRoute {
            tenant_id: "tenant-a".into(),
            application_id: "field".into(),
            topic: "com.acme.field".into(),
            encrypted_credential_id: "credential/apns-field".into(),
        }])
        .unwrap();
        let source = RecordingCredentialSource(std::sync::Mutex::new(Vec::new()));
        router
            .provider_for(&source, "tenant-a", "field", "com.acme.field")
            .await
            .unwrap();
        assert_eq!(
            source.0.lock().unwrap().as_slice(),
            ["credential/apns-field"]
        );
    }

    #[test]
    fn signed_wake_binds_identity_expiry_and_nonce() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let now = Utc::now();
        let wake = SignedWakeMetadata::sign(
            "tenant-a",
            "device-a",
            "command-a",
            "wake-v1",
            &key,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let encoded = serde_json::to_string(&wake).unwrap();
        assert!(!encoded.contains("workflow") && !encoded.contains("context"));
        let mut nonces = WakeNonceCache::new(100);
        assert!(
            wake.verify(
                "tenant-a",
                "device-a",
                now,
                &key.verifying_key(),
                &mut nonces
            )
            .is_ok()
        );
        assert_eq!(
            wake.verify(
                "tenant-a",
                "device-a",
                now,
                &key.verifying_key(),
                &mut nonces
            ),
            Err(PushGovernanceError::Replay)
        );
        let mut fresh = WakeNonceCache::new(100);
        assert_eq!(
            wake.verify(
                "tenant-b",
                "device-a",
                now,
                &key.verifying_key(),
                &mut fresh
            ),
            Err(PushGovernanceError::InvalidSignature)
        );
    }

    #[test]
    fn collapse_keeps_newest_per_execution_without_cross_execution_loss() {
        let now = Utc::now();
        let make = |execution: &str, command: &str, seconds: i64| CollapsibleWake {
            tenant_id: "tenant-a".into(),
            device_id: "device-a".into(),
            execution_id: execution.into(),
            topic: "resume".into(),
            command_id: command.into(),
            created_at: now + Duration::seconds(seconds),
        };
        let collapsed = collapse_wakes([
            make("exec-a", "old", 0),
            make("exec-a", "new", 1),
            make("exec-b", "distinct", 0),
        ]);
        assert_eq!(collapsed.len(), 2);
        assert!(collapsed.iter().any(|wake| wake.command_id == "new"));
        assert!(collapsed.iter().any(|wake| wake.command_id == "distinct"));
    }

    #[test]
    fn invalid_token_quarantine_is_observable_and_recoverable() {
        let mut state = TokenLifecycleState {
            tenant_id: "tenant-a".into(),
            device_id: "device-a".into(),
            active: true,
            quarantined_at: None,
            quarantine_reason: None,
        };
        state.quarantine_invalid_token(Utc::now());
        assert!(!state.active && state.quarantined_at.is_some());
        assert_eq!(
            state.quarantine_reason.as_deref(),
            Some("provider_invalid_token")
        );
        state.reactivate_with_new_token();
        assert!(state.active && state.quarantine_reason.is_none());
    }
}
