//! Manifest generation and signing.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use tracing::info;

/// A signing key entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSigningKey {
    pub key_id: String,
    pub public_key: String,
}

/// A sequence entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSequence {
    pub name: String,
    pub version: i32,
    pub url: String,
    pub signing_key_id: String,
    pub sha256: String,
    pub required_handlers: Vec<String>,
    pub min_sdk_version: String,
}

/// A removed sequence entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRemoved {
    pub name: String,
    pub removed_at: DateTime<Utc>,
}

/// The manifest structure before signing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestBody {
    pub signing_keys: Vec<ManifestSigningKey>,
    pub sequences: Vec<ManifestSequence>,
    pub removed: Vec<ManifestRemoved>,
    pub manifest_version: i64,
    pub generated_at: DateTime<Utc>,
}

/// Signed manifest ready for upload.
pub struct SignedManifest {
    pub body: ManifestBody,
    pub signature_b64: String,
    pub canonical_json: String,
}

/// Generates and signs manifests.
pub struct ManifestGenerator {
    signing_key: SigningKey,
    key_id: String,
    /// Last manifest version handed out, used to keep versions strictly
    /// increasing even for two `generate` calls inside the same millisecond.
    last_version: std::sync::atomic::AtomicI64,
}

impl ManifestGenerator {
    pub fn new(signing_key: SigningKey, key_id: String) -> Self {
        Self {
            signing_key,
            key_id,
            last_version: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Generate a manifest from the given sequences and removed entries.
    pub fn generate(
        &self,
        sequences: Vec<ManifestSequence>,
        removed: Vec<ManifestRemoved>,
        other_keys: Vec<ManifestSigningKey>,
    ) -> Result<SignedManifest, ManifestError> {
        let mut seen_keys = std::collections::HashSet::new();
        for key in &other_keys {
            if !seen_keys.insert(key.key_id.as_str()) {
                return Err(ManifestError::Serialization(format!(
                    "duplicate signing key_id {}",
                    key.key_id
                )));
            }
        }
        if !seen_keys.insert(self.key_id.as_str()) {
            return Err(ManifestError::Serialization(format!(
                "duplicate signing key_id {}",
                self.key_id
            )));
        }

        let mut signing_keys = other_keys;
        signing_keys.push(ManifestSigningKey {
            key_id: self.key_id.clone(),
            public_key: BASE64.encode(self.signing_key.verifying_key().to_bytes()),
        });

        // Derive a monotonic version from the generation time (epoch millis).
        // Clients reject any manifest whose version is not strictly greater than
        // the last one applied, so a replayed older manifest — which necessarily
        // carries an earlier timestamp — is refused. Two `generate` calls within
        // the same millisecond (e.g. an immediate retry) would collide on the
        // raw timestamp, so clamp to one above the last version handed out. A
        // wall-clock regression (NTP step back) also fails safe this way: the
        // version keeps increasing from the high-water mark instead of going
        // backwards and being refused by clients.
        let generated_at = Utc::now();
        let now_millis = generated_at.timestamp_millis();
        let mut prev = self.last_version.load(std::sync::atomic::Ordering::Relaxed);
        let manifest_version = loop {
            let candidate = now_millis.max(prev.saturating_add(1));
            match self.last_version.compare_exchange_weak(
                prev,
                candidate,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break candidate,
                Err(observed) => prev = observed,
            }
        };
        let body = ManifestBody {
            signing_keys,
            sequences,
            removed,
            manifest_version,
            generated_at,
        };

        let canonical_json = canonical_json(&body)?;
        let signature = self.signing_key.sign(canonical_json.as_bytes());
        let signature_b64 = BASE64.encode(signature.to_bytes());

        info!(
            manifest_version = body.manifest_version,
            sequences = body.sequences.len(),
            "manifest generated"
        );

        Ok(SignedManifest {
            body,
            signature_b64,
            canonical_json,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("serialization failed: {0}")]
    Serialization(String),
}

/// Serialize a value to canonical JSON: sorted keys at every level, no extra whitespace.
/// Cross-language verifiers can reproduce the exact same bytes by sorting keys.
pub fn canonical_json<T: Serialize>(value: &T) -> Result<String, ManifestError> {
    let v = serde_json::to_value(value).map_err(|e| ManifestError::Serialization(e.to_string()))?;
    let sorted = sort_json_keys(v);
    serde_json::to_string(&sorted).map_err(|e| ManifestError::Serialization(e.to_string()))
}

fn sort_json_keys(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, sort_json_keys(v)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect();
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(sort_json_keys).collect())
        }
        other => other,
    }
}

/// Filter out removed entries older than 30 days.
pub fn prune_removed(removed: &mut Vec<ManifestRemoved>) {
    let cutoff = Utc::now() - chrono::Duration::days(30);
    removed.retain(|r| r.removed_at >= cutoff);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand_core::OsRng;

    fn test_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    #[test]
    fn manifest_generation_roundtrip() {
        let r#gen = ManifestGenerator::new(test_key(), "key1".to_string());
        let seq = ManifestSequence {
            name: "test".to_string(),
            version: 1,
            url: "https://cdn.example.com/seq.json".to_string(),
            signing_key_id: "key1".to_string(),
            sha256: "abcd".to_string(),
            required_handlers: vec!["echo".to_string()],
            min_sdk_version: "0.1.0".to_string(),
        };
        let signed = r#gen.generate(vec![seq], vec![], vec![]).unwrap();
        assert!(!signed.signature_b64.is_empty());
        assert_eq!(signed.body.sequences.len(), 1);
    }

    #[test]
    fn canonical_json_sorts_keys() {
        let input: serde_json::Value =
            serde_json::json!({"z": 1, "a": {"c": 3, "b": 2}, "m": [{"y": 1, "x": 2}]});
        let result = canonical_json(&input).unwrap();
        assert_eq!(result, r#"{"a":{"b":2,"c":3},"m":[{"x":2,"y":1}],"z":1}"#);
    }

    #[test]
    fn canonical_json_is_deterministic() {
        let seq = ManifestSequence {
            name: "test".to_string(),
            version: 1,
            url: "/test.json".to_string(),
            signing_key_id: "k1".to_string(),
            sha256: "abc".to_string(),
            required_handlers: vec!["echo".to_string()],
            min_sdk_version: "0.1.0".to_string(),
        };
        let a = canonical_json(&seq).unwrap();
        let b = canonical_json(&seq).unwrap();
        assert_eq!(a, b);
        assert!(a.contains(r#""min_sdk_version":"0.1.0""#));
        assert!(a.contains(r#""name":"test""#));
    }

    #[test]
    fn prune_removed_drops_old_entries() {
        let mut removed = vec![
            ManifestRemoved {
                name: "old".to_string(),
                removed_at: Utc::now() - chrono::Duration::days(31),
            },
            ManifestRemoved {
                name: "recent".to_string(),
                removed_at: Utc::now() - chrono::Duration::days(1),
            },
        ];
        prune_removed(&mut removed);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].name, "recent");
    }

    // Clients require strictly-greater versions, so two generations within
    // the same millisecond must not collide on the timestamp.
    #[test]
    fn manifest_versions_are_strictly_increasing() {
        let r#gen = ManifestGenerator::new(test_key(), "key1".to_string());
        let a = r#gen.generate(vec![], vec![], vec![]).unwrap();
        let b = r#gen.generate(vec![], vec![], vec![]).unwrap();
        assert!(
            b.body.manifest_version > a.body.manifest_version,
            "versions must be strictly increasing: {} then {}",
            a.body.manifest_version,
            b.body.manifest_version
        );
    }

    #[test]
    fn generate_rejects_generator_key_in_other_keys() {
        let signing_key = SigningKey::from_bytes(&[1u8; 32]);
        let r#gen = ManifestGenerator::new(signing_key, "primary".to_string());
        let other = ManifestSigningKey {
            key_id: "primary".to_string(),
            public_key: "abc".to_string(),
        };
        let result = r#gen.generate(vec![], vec![], vec![other]);
        let Err(err) = result else {
            panic!("expected duplicate generator key to be rejected");
        };
        assert!(err.to_string().contains("duplicate signing key_id primary"));
    }

    #[test]
    fn generate_rejects_duplicate_other_key_id() {
        let signing_key = SigningKey::from_bytes(&[2u8; 32]);
        let r#gen = ManifestGenerator::new(signing_key, "primary".to_string());
        let others = vec![
            ManifestSigningKey {
                key_id: "k1".to_string(),
                public_key: "a".to_string(),
            },
            ManifestSigningKey {
                key_id: "k1".to_string(),
                public_key: "b".to_string(),
            },
        ];
        let result = r#gen.generate(vec![], vec![], others);
        let Err(err) = result else {
            panic!("expected duplicate other key to be rejected");
        };
        assert!(err.to_string().contains("duplicate signing key_id k1"));
    }
}
