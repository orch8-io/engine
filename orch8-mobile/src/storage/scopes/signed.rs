//! Verified offline ingestion for scoped catalogs. This does not authenticate a
//! network credential; the future scoped transport must establish that binding
//! before fetching private content. No public execution API uses this yet.
use super::{CatalogAcceptance, CatalogScope, validate};
use crate::{MobileError, error::SyncError, storage::MobileStorage, sync::Manifest};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signature, VerifyingKey};
use orch8_types::{catalog::CatalogManifest, ids::SequenceId};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

fn refused() -> MobileError {
    SyncError::InvalidManifest {
        message: "invalid or foreign scoped catalog document".into(),
    }
    .into()
}

fn key(encoded: &str) -> Result<VerifyingKey, MobileError> {
    let bytes: [u8; 32] = BASE64
        .decode(encoded)
        .map_err(|_| refused())?
        .try_into()
        .map_err(|_| refused())?;
    VerifyingKey::from_bytes(&bytes).map_err(|_| refused())
}

fn verify(key: &VerifyingKey, body: &[u8], signature: &[u8]) -> Result<(), MobileError> {
    let signature = Signature::from_slice(signature).map_err(|_| refused())?;
    key.verify_strict(body, &signature).map_err(|_| refused())
}

/// Construction is private: a caller cannot attach a verified label to an
/// arbitrary document or transplant acceptance to another account/catalog.
pub(crate) struct AcceptedCatalogManifest {
    storage: Arc<MobileStorage>,
    scope: CatalogScope,
    manifest: Manifest,
    digest: String,
    keys: HashMap<String, VerifyingKey>,
}

impl AcceptedCatalogManifest {
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub(crate) fn digest(&self) -> &str {
        &self.digest
    }

    /// Verify entry identity and exact bytes before owner-qualified installation.
    /// Handler/SDK compatibility and transport authorization remain caller duties.
    pub(crate) async fn install_workflow(
        &self,
        name: &str,
        original_json: &str,
        signature: &[u8],
    ) -> Result<SequenceId, MobileError> {
        if original_json.len() > crate::MAX_SEQUENCES_RESPONSE_BYTES {
            return Err(refused());
        }
        let entry = self
            .manifest
            .sequences
            .iter()
            .find(|entry| entry.name == name)
            .ok_or_else(refused)?;
        if hex::encode(Sha256::digest(original_json.as_bytes())) != entry.sha256 {
            return Err(refused());
        }
        verify(
            self.keys.get(&entry.signing_key_id).ok_or_else(refused)?,
            original_json.as_bytes(),
            signature,
        )?;
        let definition: orch8_types::sequence::SequenceDefinition =
            serde_json::from_str(original_json).map_err(|_| refused())?;
        if definition.name != entry.name || definition.version != entry.version {
            return Err(refused());
        }
        Ok(self
            .storage
            .install_catalog_workflow(
                &self.scope,
                original_json,
                self.manifest.manifest_version,
                &self.digest,
            )
            .await?)
    }
}

impl MobileStorage {
    /// Verify a complete audience-bound envelope with the catalog's immutable
    /// `ed25519:<base64 public key>` authority, then atomically accept its keys
    /// and replay fence. Legacy documents cannot be adopted into a scope.
    pub(crate) async fn accept_signed_catalog_manifest(
        self: &Arc<Self>,
        scope: &CatalogScope,
        bytes: &[u8],
    ) -> Result<AcceptedCatalogManifest, MobileError> {
        if bytes.len() > crate::MAX_SEQUENCES_RESPONSE_BYTES {
            return Err(refused());
        }
        let binding = self.catalog_binding(scope).await?;
        let authority = key(binding
            .authority
            .strip_prefix("ed25519:")
            .ok_or_else(refused)?)?;
        let divider = bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or_else(refused)?;
        let signature = BASE64.decode(&bytes[..divider]).map_err(|_| refused())?;
        let body = &bytes[divider + 1..];
        verify(&authority, body, &signature)?;
        let document: CatalogManifest<Manifest> =
            serde_json::from_slice(body).map_err(|_| refused())?;
        document.audience.validate().map_err(|_| refused())?;
        // Public access requires an explicit persisted policy; never downgrade.
        if document.audience != binding.audience {
            return Err(refused());
        }
        let manifest = document.manifest;
        if manifest.sequences.len() > 1000
            || manifest.removed.len() > 1000
            || manifest.signing_keys.len() > 1000
        {
            return Err(refused());
        }
        let mut keys = HashMap::new();
        for entry in &manifest.signing_keys {
            validate(&entry.key_id)?;
            if keys
                .insert(entry.key_id.clone(), key(&entry.public_key)?)
                .is_some()
            {
                return Err(refused());
            }
        }
        let mut names = std::collections::HashSet::new();
        for entry in &manifest.sequences {
            validate(&entry.name)?;
            if entry.version <= 0
                || !names.insert(&entry.name)
                || !keys.contains_key(&entry.signing_key_id)
                || entry.sha256.len() != 64
                || !entry
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(refused());
            }
        }
        for entry in &manifest.removed {
            validate(&entry.name)?;
            if names.contains(&entry.name) {
                return Err(refused());
            }
        }
        let digest = hex::encode(Sha256::digest(body));
        let encoded: Vec<_> = manifest
            .signing_keys
            .iter()
            .map(|entry| (entry.key_id.as_str(), entry.public_key.as_str()))
            .collect();
        let entries: Vec<_> = manifest
            .sequences
            .iter()
            .map(|entry| (entry.name.as_str(), entry.version, entry.sha256.as_str()))
            .collect();
        self.replace_catalog_keys_in(
            scope,
            &encoded,
            Some(CatalogAcceptance {
                version: manifest.manifest_version,
                digest: &digest,
                entries: &entries,
                execution: Some(&manifest.sequences),
            }),
        )
        .await?;
        Ok(AcceptedCatalogManifest {
            storage: Arc::clone(self),
            scope: scope.clone(),
            manifest,
            digest,
            keys,
        })
    }
}

#[cfg(test)]
mod tests;
