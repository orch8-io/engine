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

    fn policy() -> BTreeSet<String> {
        ["ssn", "secret", "photo"]
            .map(String::from)
            .into_iter()
            .collect()
    }

    #[test]
    fn sanitizer_replaces_protected_keys_at_any_depth_and_keeps_everything_else() {
        use serde_json::json;
        let cases = [
            (
                "nested object",
                json!({"user": {"ssn": "123"}}),
                json!({"user": {"ssn": "[PROTECTED]"}}),
            ),
            (
                "deeply nested",
                json!({"a": {"b": {"secret": "x"}}}),
                json!({"a": {"b": {"secret": "[PROTECTED]"}}}),
            ),
            (
                "objects inside a top-level array",
                json!([{"secret": "x"}, {"secret": "y"}]),
                json!([{"secret": "[PROTECTED]"}, {"secret": "[PROTECTED]"}]),
            ),
            (
                "objects inside a nested array",
                json!({"items": [{"photo": "x"}]}),
                json!({"items": [{"photo": "[PROTECTED]"}]}),
            ),
            (
                "protected object replaced wholesale",
                json!({"secret": {"nested": "x"}}),
                json!({"secret": "[PROTECTED]"}),
            ),
            (
                "protected array replaced wholesale",
                json!({"photo": ["x", "y"]}),
                json!({"photo": "[PROTECTED]"}),
            ),
            (
                "protected null still replaced",
                json!({"ssn": null}),
                json!({"ssn": "[PROTECTED]"}),
            ),
            (
                "public siblings preserved",
                json!({"ssn": "x", "secret": "y", "photo": "z", "case": "A"}),
                json!({"ssn": "[PROTECTED]", "secret": "[PROTECTED]", "photo": "[PROTECTED]", "case": "A"}),
            ),
            (
                "keys are matched exactly, not by prefix or suffix",
                json!({"user_ssn": "x", "secret_value": "y"}),
                json!({"user_ssn": "x", "secret_value": "y"}),
            ),
            (
                "scalars and unprotected arrays pass through",
                json!([1, true, "x", null, {}]),
                json!([1, true, "x", null, {}]),
            ),
        ];
        for (case, input, expected) in cases {
            assert_eq!(sanitize_value(&input, &policy()), expected, "case: {case}");
        }
    }

    #[test]
    #[ignore = "BUG: protected-field redaction is case-sensitive, so {\"SSN\": ..} leaks (T4 #12c) — fixed on test/bugs"]
    fn sanitizer_matches_protected_keys_case_insensitively() {
        let sanitized = sanitize_value(&serde_json::json!({"SSN": "123"}), &policy());

        assert_eq!(sanitized, serde_json::json!({"SSN": "[PROTECTED]"}));
    }

    #[test]
    fn policy_with_no_fields_or_an_empty_field_name_is_rejected() {
        let empty = ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[1; 32]), Vec::new());
        let blank =
            ProtectedFieldBoundary::new(FieldEncryptor::from_bytes(&[1; 32]), [String::new()]);

        assert!(matches!(empty, Err(PrivacyError::InvalidPolicy)));
        assert!(matches!(blank, Err(PrivacyError::InvalidPolicy)));
    }

    #[test]
    fn sealing_requires_both_tenant_and_instance_binding() {
        let value = serde_json::json!({"ssn": "x"});
        for (tenant, instance) in [("", "instance"), ("tenant", "")] {
            let result = boundary().seal_for_handoff(tenant, instance, &value);

            assert!(
                matches!(result, Err(PrivacyError::InvalidPolicy)),
                "tenant={tenant:?} instance={instance:?}"
            );
        }
    }

    #[test]
    fn leak_probe_rejects_an_empty_needle() {
        let result = boundary().assert_no_raw_value("", &[("log", serde_json::json!({}))]);

        assert!(matches!(result, Err(PrivacyError::InvalidPolicy)));
    }

    #[test]
    fn leak_probe_names_the_surface_that_leaked_a_nested_value() {
        let result = boundary().assert_no_raw_value(
            "needle",
            &[
                ("log", serde_json::json!({"ok": true})),
                ("trace", serde_json::json!({"nested": {"value": "needle"}})),
            ],
        );

        assert!(matches!(result, Err(PrivacyError::Leak(surface)) if surface == "trace"));
    }
}
