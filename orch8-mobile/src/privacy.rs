//! Mobile protected-field boundary and leakage assertions.

use std::collections::BTreeSet;

use orch8_types::encryption::{EncryptionError, FieldEncryptor};
use serde::{Deserialize, Serialize};

const REDACTED: &str = "[PROTECTED]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisclosureSurface {
    Log,
    Trace,
    Artifact,
    Sync,
}

/// Encrypts protected workflow input and sanitizes every outward SDK surface.
/// Key rotation is supported by constructing the encryptor with an old key;
/// new writes always use the primary key.
#[derive(Clone)]
pub struct ProtectedFieldBoundary {
    encryptor: FieldEncryptor,
    protected_fields: BTreeSet<String>,
}

impl ProtectedFieldBoundary {
    pub fn new(
        encryptor: FieldEncryptor,
        protected_fields: impl IntoIterator<Item = String>,
    ) -> Result<Self, PrivacyError> {
        let protected_fields = protected_fields.into_iter().collect::<BTreeSet<_>>();
        if protected_fields.is_empty() || protected_fields.iter().any(String::is_empty) {
            return Err(PrivacyError::InvalidPolicy);
        }
        Ok(Self {
            encryptor,
            protected_fields,
        })
    }

    pub fn seal_for_handoff(
        &self,
        tenant_id: &str,
        instance_id: &str,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, PrivacyError> {
        if tenant_id.is_empty() || instance_id.is_empty() {
            return Err(PrivacyError::InvalidPolicy);
        }
        let aad = format!("mobile-handoff:{tenant_id}:{instance_id}");
        self.encryptor
            .encrypt_value_with_aad(value, aad.as_bytes())
            .map_err(PrivacyError::Encryption)
    }

    pub fn open_in_trusted_runtime(
        &self,
        tenant_id: &str,
        instance_id: &str,
        sealed: &serde_json::Value,
    ) -> Result<serde_json::Value, PrivacyError> {
        let aad = format!("mobile-handoff:{tenant_id}:{instance_id}");
        self.encryptor
            .decrypt_value_with_aad(sealed, aad.as_bytes())
            .map_err(PrivacyError::Encryption)
    }

    /// Recursively redact policy-labelled keys before a value crosses logs,
    /// traces, artifacts, or sync. Encrypted strings remain opaque.
    #[must_use]
    pub fn sanitize(
        &self,
        _surface: DisclosureSurface,
        value: &serde_json::Value,
    ) -> serde_json::Value {
        sanitize_value(value, &self.protected_fields)
    }

    /// Executable assertion used by reference workflows and host test suites.
    pub fn assert_no_raw_value(
        &self,
        raw_value: &str,
        emitted: &[(&str, serde_json::Value)],
    ) -> Result<(), PrivacyError> {
        if raw_value.is_empty() {
            return Err(PrivacyError::InvalidPolicy);
        }
        for (surface, value) in emitted {
            let encoded = serde_json::to_string(value).map_err(PrivacyError::Serialization)?;
            if encoded.contains(raw_value) {
                return Err(PrivacyError::Leak((*surface).into()));
            }
        }
        Ok(())
    }
}

fn sanitize_value(value: &serde_json::Value, protected: &BTreeSet<String>) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let value = if protected.contains(key) {
                        serde_json::Value::String(REDACTED.into())
                    } else {
                        sanitize_value(value, protected)
                    };
                    (key.clone(), value)
                })
                .collect(),
        ),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| sanitize_value(item, protected))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PrivacyError {
    #[error("protected-field policy is invalid")]
    InvalidPolicy,
    #[error("protected data leaked through {0}")]
    Leak(String),
    #[error("protected-field encryption failed: {0}")]
    Encryption(EncryptionError),
    #[error("serialize leakage evidence: {0}")]
    Serialization(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary() -> ProtectedFieldBoundary {
        ProtectedFieldBoundary::new(
            FieldEncryptor::from_bytes(&[7; 32]),
            ["ssn".to_string(), "photo".to_string()],
        )
        .unwrap()
    }

    #[test]
    fn protected_reference_workflow_never_emits_raw_field() {
        let boundary = boundary();
        let raw = "123-45-6789";
        let protected = serde_json::json!({"ssn": raw, "case": "A-1"});
        let sealed = boundary
            .seal_for_handoff("tenant-a", "instance-a", &protected)
            .unwrap();
        let opened = boundary
            .open_in_trusted_runtime("tenant-a", "instance-a", &sealed)
            .unwrap();
        assert_eq!(opened, protected);

        let derived = serde_json::json!({"eligible": true, "ssn": raw});
        let emitted = [
            ("log", boundary.sanitize(DisclosureSurface::Log, &derived)),
            (
                "trace",
                boundary.sanitize(DisclosureSurface::Trace, &derived),
            ),
            (
                "artifact",
                boundary.sanitize(DisclosureSurface::Artifact, &derived),
            ),
            ("sync", boundary.sanitize(DisclosureSurface::Sync, &derived)),
            ("capsule", sealed),
        ];
        boundary.assert_no_raw_value(raw, &emitted).unwrap();
    }

    #[test]
    fn tenant_or_instance_swap_cannot_open_ciphertext() {
        let boundary = boundary();
        let sealed = boundary
            .seal_for_handoff(
                "tenant-a",
                "instance-a",
                &serde_json::json!({"ssn": "secret"}),
            )
            .unwrap();
        assert!(
            boundary
                .open_in_trusted_runtime("tenant-b", "instance-a", &sealed)
                .is_err()
        );
        assert!(
            boundary
                .open_in_trusted_runtime("tenant-a", "instance-b", &sealed)
                .is_err()
        );
    }

    #[test]
    fn leakage_assertion_catches_unsanitized_output() {
        let boundary = boundary();
        let emitted = [("trace", serde_json::json!({"message": "secret"}))];
        assert!(matches!(
            boundary.assert_no_raw_value("secret", &emitted),
            Err(PrivacyError::Leak(_))
        ));
    }

    #[test]
    fn old_key_reads_during_rotation_but_new_key_writes() {
        let old = ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[1; 32]), ["ssn".into()])
            .unwrap();
        let sealed = old
            .seal_for_handoff(
                "tenant-a",
                "instance-a",
                &serde_json::json!({"ssn": "secret"}),
            )
            .unwrap();
        let old_hex = hex::encode([1_u8; 32]);
        let rotated = ProtectedFieldBoundary::new(
            FieldEncryptor::from_bytes(&[2; 32])
                .with_old_key(&old_hex)
                .unwrap(),
            ["ssn".into()],
        )
        .unwrap();
        assert!(
            rotated
                .open_in_trusted_runtime("tenant-a", "instance-a", &sealed)
                .is_ok()
        );
        let new_sealed = rotated
            .seal_for_handoff("tenant-a", "instance-a", &serde_json::json!({"ssn": "new"}))
            .unwrap();
        assert!(
            old.open_in_trusted_runtime("tenant-a", "instance-a", &new_sealed)
                .is_err()
        );
    }
}

#[cfg(test)]
#[path = "privacy_boundary_tests.rs"]
mod boundary_tests;
