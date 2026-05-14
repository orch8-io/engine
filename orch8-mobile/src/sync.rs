//! Sync infrastructure: manifest fetch, signature verification, sequence download,
//! and reconciliation.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::ids::{Namespace, TenantId};
use orch8_types::sequence::SequenceDefinition;

use crate::error::{MobileError, SyncError};
use crate::storage::MobileStorage;

/// Root of trust: embedded Ed25519 public key for verifying manifest signatures.
pub struct RootKey {
    pubkey: VerifyingKey,
}

impl RootKey {
    /// Load from a base64-encoded 32-byte Ed25519 public key.
    pub fn from_base64(b64: &str) -> Result<Self, SyncError> {
        let bytes = BASE64
            .decode(b64)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("invalid base64 root key: {e}"),
            })?;
        let pubkey = VerifyingKey::from_bytes(&bytes.try_into().map_err(|_| {
            SyncError::SignatureInvalid {
                message: "invalid Ed25519 public key length".to_string(),
            }
        })?)
        .map_err(|e| SyncError::SignatureInvalid {
            message: format!("invalid Ed25519 public key: {e}"),
        })?;
        Ok(Self { pubkey })
    }

    pub fn verify(&self, message: &[u8], signature_bytes: &[u8]) -> Result<(), SyncError> {
        let sig_bytes: &[u8; 64] =
            signature_bytes
                .try_into()
                .map_err(|_| SyncError::SignatureInvalid {
                    message: "invalid signature length (expected 64 bytes)".to_string(),
                })?;
        let sig = Signature::from_bytes(sig_bytes);
        self.pubkey
            .verify(message, &sig)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("signature verification failed: {e}"),
            })
    }
}

/// A signing key entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SigningKeyEntry {
    pub key_id: String,
    pub public_key: String, // base64-encoded Ed25519 public key
}

/// A sequence entry in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSequenceEntry {
    pub name: String,
    pub version: i32,
    pub url: String,
    pub signing_key_id: String,
    pub sha256: String,
    pub required_handlers: Vec<String>,
    pub min_sdk_version: String,
}

/// A removed sequence entry with TTL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovedEntry {
    pub name: String,
    pub removed_at: DateTime<Utc>,
}

/// The signed manifest fetched from the CDN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub signing_keys: Vec<SigningKeyEntry>,
    pub sequences: Vec<ManifestSequenceEntry>,
    pub removed: Vec<RemovedEntry>,
    #[allow(clippy::struct_field_names)]
    pub manifest_version: i64,
    pub generated_at: DateTime<Utc>,
}

/// Result of a sync operation.
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct SyncResult {
    pub added: u32,
    pub updated: u32,
    pub removed: u32,
    pub skipped: u32,
    pub signature_failures: u32,
}

/// Auth configuration for sync requests.
pub enum SyncAuth {
    /// Token provided via callback interface.
    Bearer(String),
    /// Token embedded directly in the manifest URL (signed URL mode).
    UrlToken,
}

/// Orchestrates manifest fetch, verification, diff, and sequence download.
pub struct SyncOrchestrator {
    mobile_storage: Arc<MobileStorage>,
    backend: Arc<dyn StorageBackend>,
    root_key: RootKey,
    http: reqwest::Client,
    sdk_version: String,
}

impl SyncOrchestrator {
    pub fn new(
        mobile_storage: Arc<MobileStorage>,
        backend: Arc<dyn StorageBackend>,
        root_key: RootKey,
        sdk_version: String,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client builds");
        Self {
            mobile_storage,
            backend,
            root_key,
            http,
            sdk_version,
        }
    }

    /// Perform a full sync against the given manifest URL.
    #[allow(clippy::too_many_lines)]
    pub async fn sync(
        &self,
        manifest_url: &str,
        auth: &SyncAuth,
        registered_handlers: &HashSet<String>,
    ) -> Result<SyncResult, MobileError> {
        let mut result = SyncResult::default();

        // 1. Fetch manifest with ETag support.
        let (manifest_bytes, maybe_etag) = self.fetch_manifest(manifest_url, auth).await?;

        if manifest_bytes.is_empty() {
            return Ok(result);
        }

        // 2. Verify manifest signature.
        let manifest: Manifest = self.verify_and_parse_manifest(&manifest_bytes)?;

        // 3. Validate signing keys against root key and cache them.
        let mut trusted_keys: HashMap<String, VerifyingKey> = HashMap::new();
        for entry in &manifest.signing_keys {
            let pk_bytes =
                BASE64
                    .decode(&entry.public_key)
                    .map_err(|e| SyncError::SignatureInvalid {
                        message: format!("bad base64 for signing key {}: {e}", entry.key_id),
                    })?;
            let pk = VerifyingKey::from_bytes(&pk_bytes.try_into().map_err(|_| {
                SyncError::SignatureInvalid {
                    message: format!("bad public key length for {}", entry.key_id),
                }
            })?)
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("bad public key for {}: {e}", entry.key_id),
            })?;
            trusted_keys.insert(entry.key_id.clone(), pk);
            self.mobile_storage
                .upsert_trusted_key(&entry.key_id, &entry.public_key)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }

        // 4. Determine what to add/update/remove.
        let local_sequences = self.list_local_sequences().await?;
        let manifest_names: HashSet<String> =
            manifest.sequences.iter().map(|s| s.name.clone()).collect();

        // Reconcile removed entries.
        let cutoff = Utc::now() - chrono::Duration::days(30);
        for removed in &manifest.removed {
            if removed.removed_at < cutoff {
                continue; // ignore old removals
            }
            if local_sequences.contains_key(&removed.name) {
                if let Err(e) = self.remove_local_sequence(&removed.name).await {
                    warn!(name = %removed.name, error = %e, "failed to remove local sequence");
                } else {
                    result.removed += 1;
                }
            }
        }

        // Full reconciliation: if last sync > 30 days ago, treat any local sequence
        // not in manifest as removed.
        let last_sync = self.get_last_sync_ts().await?;
        let thirty_days_ago = Utc::now() - chrono::Duration::days(30);
        if last_sync.is_none_or(|t| t < thirty_days_ago) {
            for name in local_sequences.keys() {
                if !manifest_names.contains(name) {
                    if let Err(e) = self.remove_local_sequence(name).await {
                        warn!(name = %name, error = %e, "failed to remove stale local sequence");
                    } else {
                        result.removed += 1;
                    }
                }
            }
        }

        // 5. Download and verify each sequence.
        for entry in &manifest.sequences {
            if !self.version_meets_min(&self.sdk_version, &entry.min_sdk_version) {
                warn!(
                    name = %entry.name,
                    min_sdk = %entry.min_sdk_version,
                    sdk = %self.sdk_version,
                    "sequence requires newer SDK — skipping"
                );
                result.skipped += 1;
                continue;
            }

            let missing_handlers: Vec<&String> = entry
                .required_handlers
                .iter()
                .filter(|h| !registered_handlers.contains(h.as_str()))
                .collect();
            if !missing_handlers.is_empty() {
                warn!(
                    name = %entry.name,
                    missing = ?missing_handlers,
                    "sequence requires unregistered handlers — skipping"
                );
                result.skipped += 1;
                continue;
            }

            let seq_json = match self.download_sequence(&entry.url, auth).await {
                Ok(json) => json,
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "failed to download sequence");
                    result.skipped += 1;
                    continue;
                }
            };

            let computed_hash = format!("{:x}", Sha256::digest(&seq_json));
            if computed_hash != entry.sha256 {
                warn!(
                    name = %entry.name,
                    expected = %entry.sha256,
                    got = %computed_hash,
                    "sequence hash mismatch — skipping"
                );
                result.signature_failures += 1;
                continue;
            }

            // Verify sequence signature using trusted key.
            if !trusted_keys.contains_key(&entry.signing_key_id) {
                let cached = self
                    .mobile_storage
                    .get_trusted_key(&entry.signing_key_id)
                    .await
                    .map_err(|e| MobileError::Storage {
                        message: e.to_string(),
                    })?;
                if let Some(cached_b64) = cached {
                    let bytes =
                        BASE64
                            .decode(&cached_b64)
                            .map_err(|e| SyncError::SignatureInvalid {
                                message: format!("cached key decode failed: {e}"),
                            })?;
                    let pk = VerifyingKey::from_bytes(&bytes.try_into().map_err(|_| {
                        SyncError::SignatureInvalid {
                            message: "cached key invalid length".to_string(),
                        }
                    })?)
                    .map_err(|e| SyncError::SignatureInvalid {
                        message: format!("cached key invalid: {e}"),
                    })?;
                    trusted_keys.insert(entry.signing_key_id.clone(), pk);
                }
            }

            let Some(signing_pk) = trusted_keys.get(&entry.signing_key_id) else {
                warn!(name = %entry.name, key_id = %entry.signing_key_id, "no trusted signing key — skipping");
                result.signature_failures += 1;
                continue;
            };

            let sig_url = entry.url.replace(".json", ".sig");
            match self.download_sequence(&sig_url, auth).await {
                Ok(sig_b64) => {
                    let sig_bytes =
                        BASE64
                            .decode(sig_b64.trim())
                            .map_err(|e| SyncError::SignatureInvalid {
                                message: format!("sequence signature decode failed: {e}"),
                            })?;
                    let sig: Signature =
                        Signature::from_bytes(&sig_bytes.try_into().map_err(|_| {
                            SyncError::SignatureInvalid {
                                message: "sequence signature wrong length".to_string(),
                            }
                        })?);
                    if let Err(e) = signing_pk.verify(seq_json.as_bytes(), &sig) {
                        warn!(name = %entry.name, error = %e, "sequence signature verification failed — skipping");
                        result.signature_failures += 1;
                        continue;
                    }
                }
                Err(e) => {
                    warn!(name = %entry.name, error = %e, "failed to download sequence signature — skipping");
                    result.signature_failures += 1;
                    continue;
                }
            }

            let seq: SequenceDefinition =
                serde_json::from_str(&seq_json).map_err(|e| MobileError::InvalidInput {
                    message: format!("sequence JSON invalid: {e}"),
                })?;

            let existing = local_sequences.get(&entry.name);
            if let Some(existing_seq) = existing {
                if existing_seq.version >= entry.version {
                    result.skipped += 1;
                    continue;
                }
                self.upsert_sequence(&seq).await?;
                result.updated += 1;
            } else {
                self.upsert_sequence(&seq).await?;
                result.added += 1;
            }
        }

        // 6. Persist sync metadata.
        self.mobile_storage
            .set_sync_metadata("last_sync_ts", &Utc::now().to_rfc3339())
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if let Some(etag) = maybe_etag {
            self.mobile_storage
                .set_sync_metadata("manifest_etag", &etag)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }

        info!(
            added = result.added,
            updated = result.updated,
            removed = result.removed,
            skipped = result.skipped,
            "sync completed"
        );
        Ok(result)
    }

    async fn fetch_manifest(
        &self,
        url: &str,
        auth: &SyncAuth,
    ) -> Result<(Vec<u8>, Option<String>), MobileError> {
        let mut req = self.http.get(url);
        match auth {
            SyncAuth::Bearer(token) => {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            SyncAuth::UrlToken => {}
        }

        let cached_etag = self
            .mobile_storage
            .get_sync_metadata("manifest_etag")
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if let Some(etag) = cached_etag {
            req = req.header("if-none-match", etag);
        }

        let response = req.send().await.map_err(|e| SyncError::Network {
            message: format!("manifest fetch failed: {e}"),
        })?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok((Vec::new(), None));
        }

        if !response.status().is_success() {
            return Err(SyncError::Network {
                message: format!("manifest fetch returned {}", response.status()),
            }
            .into());
        }

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok().map(String::from));
        let bytes = response.bytes().await.map_err(|e| SyncError::Network {
            message: format!("failed to read manifest body: {e}"),
        })?;
        Ok((bytes.to_vec(), etag))
    }

    fn verify_and_parse_manifest(&self, bytes: &[u8]) -> Result<Manifest, MobileError> {
        let divider = bytes.iter().position(|&b| b == b'\n');
        let (body, sig_b64) = match divider {
            Some(idx) => {
                let sig_part =
                    std::str::from_utf8(&bytes[..idx]).map_err(|_| SyncError::InvalidManifest {
                        message: "manifest signature is not valid utf-8".to_string(),
                    })?;
                (&bytes[idx + 1..], sig_part)
            }
            None => {
                return Err(SyncError::InvalidManifest {
                    message: "manifest missing signature divider".to_string(),
                }
                .into());
            }
        };

        let sig_bytes = BASE64
            .decode(sig_b64.trim())
            .map_err(|e| SyncError::SignatureInvalid {
                message: format!("manifest signature base64 decode failed: {e}"),
            })?;

        self.root_key.verify(body, &sig_bytes)?;

        let manifest: Manifest =
            serde_json::from_slice(body).map_err(|e| SyncError::InvalidManifest {
                message: format!("manifest JSON parse failed: {e}"),
            })?;

        Ok(manifest)
    }

    async fn download_sequence(&self, url: &str, auth: &SyncAuth) -> Result<String, MobileError> {
        let mut req = self.http.get(url);
        match auth {
            SyncAuth::Bearer(token) => {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            SyncAuth::UrlToken => {}
        }

        let response = req.send().await.map_err(|e| SyncError::Network {
            message: format!("sequence download failed: {e}"),
        })?;

        if !response.status().is_success() {
            return Err(SyncError::Network {
                message: format!("sequence download returned {}", response.status()),
            }
            .into());
        }

        let text = response.text().await.map_err(|e| SyncError::Network {
            message: format!("failed to read sequence body: {e}"),
        })?;
        Ok(text)
    }

    async fn list_local_sequences(
        &self,
    ) -> Result<HashMap<String, SequenceDefinition>, MobileError> {
        let tenant = TenantId::new("mobile").expect("valid tenant");
        let ns = Namespace::new("default");
        let seqs = self
            .backend
            .list_sequences(Some(&tenant), Some(&ns), 1000, 0)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        let mut map = HashMap::new();
        for seq in seqs {
            map.insert(seq.name.clone(), seq);
        }
        Ok(map)
    }

    async fn remove_local_sequence(&self, name: &str) -> Result<(), MobileError> {
        let tenant = TenantId::new("mobile").expect("valid tenant");
        let ns = Namespace::new("default");
        let seq = self
            .backend
            .get_sequence_by_name(&tenant, &ns, name, None)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        if let Some(s) = seq {
            self.backend
                .delete_sequence(s.id)
                .await
                .map_err(|e| MobileError::Storage {
                    message: e.to_string(),
                })?;
        }
        Ok(())
    }

    async fn upsert_sequence(&self, seq: &SequenceDefinition) -> Result<(), MobileError> {
        let tenant = TenantId::new("mobile").expect("valid tenant");
        let ns = Namespace::new("default");
        if let Ok(Some(existing)) = self
            .backend
            .get_sequence_by_name(&tenant, &ns, &seq.name, None)
            .await
        {
            let _ = self.backend.delete_sequence(existing.id).await;
        }
        self.backend
            .create_sequence(seq)
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        Ok(())
    }

    async fn get_last_sync_ts(&self) -> Result<Option<DateTime<Utc>>, MobileError> {
        let raw = self
            .mobile_storage
            .get_sync_metadata("last_sync_ts")
            .await
            .map_err(|e| MobileError::Storage {
                message: e.to_string(),
            })?;
        match raw {
            Some(s) => s.parse().map(Some).map_err(|e| MobileError::InvalidInput {
                message: format!("bad last_sync_ts: {e}"),
            }),
            None => Ok(None),
        }
    }

    #[allow(clippy::unused_self)]
    fn version_meets_min(&self, sdk: &str, min: &str) -> bool {
        let sdk_parts: Vec<u32> = sdk.split('.').filter_map(|s| s.parse().ok()).collect();
        let min_parts: Vec<u32> = min.split('.').filter_map(|s| s.parse().ok()).collect();
        for i in 0..min_parts.len().max(sdk_parts.len()) {
            let s = sdk_parts.get(i).copied().unwrap_or(0);
            let m = min_parts.get(i).copied().unwrap_or(0);
            if s > m {
                return true;
            }
            if s < m {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;

    #[tokio::test]
    async fn version_meets_min_works() {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let mobile_storage = Arc::new(MobileStorage::new(sqlite.clone()));
        let root =
            RootKey::from_base64("MCowBQYDK2VwAyEAJr8nz5g7XPz8r2M6p/7u0k4v1i8v3y5x9B0jK7mNqRs=");
        // The above base64 is 44 chars which is wrong for 32-byte key; test the logic path.
        let orch = SyncOrchestrator::new(
            mobile_storage,
            sqlite,
            root.unwrap_or_else(|_| RootKey {
                pubkey: VerifyingKey::from_bytes(&[0u8; 32]).unwrap(),
            }),
            "0.4.0".to_string(),
        );

        assert!(orch.version_meets_min("0.4.0", "0.3.0"));
        assert!(orch.version_meets_min("0.4.0", "0.4.0"));
        assert!(!orch.version_meets_min("0.3.0", "0.4.0"));
        assert!(orch.version_meets_min("1.0.0", "0.9.9"));
        assert!(!orch.version_meets_min("0.4.0", "0.4.1"));
    }

    #[test]
    fn root_key_from_base64_rejects_bad_input() {
        let result = RootKey::from_base64("not-valid-base64!!!");
        assert!(result.is_err());
    }
}
