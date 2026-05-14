//! Manifest generation and signing.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
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
}

impl ManifestGenerator {
    pub fn new(signing_key: SigningKey, key_id: String) -> Self {
        Self {
            signing_key,
            key_id,
        }
    }

    /// Generate a manifest from the given sequences and removed entries.
    pub fn generate(
        &self,
        sequences: Vec<ManifestSequence>,
        removed: Vec<ManifestRemoved>,
        other_keys: Vec<ManifestSigningKey>,
    ) -> Result<SignedManifest, ManifestError> {
        let mut signing_keys = other_keys;
        signing_keys.push(ManifestSigningKey {
            key_id: self.key_id.clone(),
            public_key: BASE64.encode(self.signing_key.verifying_key().to_bytes()),
        });

        let body = ManifestBody {
            signing_keys,
            sequences,
            removed,
            manifest_version: 1,
            generated_at: Utc::now(),
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

/// Serialize a value to canonical JSON: deterministic key ordering, no extra whitespace.
fn canonical_json<T: Serialize>(value: &T) -> Result<String, ManifestError> {
    // Standard serde_json::to_string preserves field declaration order,
    // which is deterministic for the same binary. For cross-language
    // canonicalization, a full sorting serializer would be required.
    serde_json::to_string(value).map_err(|e| ManifestError::Serialization(e.to_string()))
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
    use rand::rngs::OsRng;

    fn test_key() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    #[test]
    fn manifest_generation_roundtrip() {
        let gen = ManifestGenerator::new(test_key(), "key1".to_string());
        let seq = ManifestSequence {
            name: "test".to_string(),
            version: 1,
            url: "https://cdn.example.com/seq.json".to_string(),
            signing_key_id: "key1".to_string(),
            sha256: "abcd".to_string(),
            required_handlers: vec!["echo".to_string()],
            min_sdk_version: "0.1.0".to_string(),
        };
        let signed = gen.generate(vec![seq], vec![], vec![]).unwrap();
        assert!(!signed.signature_b64.is_empty());
        assert_eq!(signed.body.sequences.len(), 1);
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
}
