//! Canonical signing and offline verification for execution-capsule manifests.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use orch8_types::continuity::CapsuleManifest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::manifest::canonical_json;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedCapsuleManifest {
    pub manifest: CapsuleManifest,
    pub manifest_sha256: String,
    pub signature: String,
    pub public_key: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CapsuleSignatureError {
    #[error("capsule manifest serialization failed: {0}")]
    Serialization(String),
    #[error("capsule manifest hash does not match its canonical content")]
    Tampered,
    #[error("capsule public key is invalid")]
    InvalidPublicKey,
    #[error("capsule signature is invalid")]
    InvalidSignature,
    #[error("capsule signing key is not trusted")]
    UntrustedKey,
}

pub fn canonical_manifest_json(
    manifest: &CapsuleManifest,
) -> Result<String, CapsuleSignatureError> {
    canonical_json(manifest)
        .map_err(|error| CapsuleSignatureError::Serialization(error.to_string()))
}

pub fn manifest_sha256(manifest: &CapsuleManifest) -> Result<String, CapsuleSignatureError> {
    let canonical = canonical_manifest_json(manifest)?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub fn sign_capsule_manifest(
    manifest: CapsuleManifest,
    signing_key: &SigningKey,
) -> Result<SignedCapsuleManifest, CapsuleSignatureError> {
    let manifest_sha256 = manifest_sha256(&manifest)?;
    let signature = signing_key.sign(manifest_sha256.as_bytes());
    Ok(SignedCapsuleManifest {
        manifest,
        manifest_sha256,
        signature: BASE64.encode(signature.to_bytes()),
        public_key: BASE64.encode(signing_key.verifying_key().to_bytes()),
    })
}

/// Verify content integrity and proof-of-possession. Trust in the embedded key
/// is intentionally a separate decision through [`check_capsule_key_trust`].
pub fn verify_signed_capsule(signed: &SignedCapsuleManifest) -> Result<(), CapsuleSignatureError> {
    let actual = manifest_sha256(&signed.manifest)?;
    let equal: bool = actual
        .as_bytes()
        .ct_eq(signed.manifest_sha256.as_bytes())
        .into();
    if !equal {
        return Err(CapsuleSignatureError::Tampered);
    }
    let key_bytes: [u8; 32] = BASE64
        .decode(&signed.public_key)
        .map_err(|_| CapsuleSignatureError::InvalidPublicKey)?
        .try_into()
        .map_err(|_| CapsuleSignatureError::InvalidPublicKey)?;
    let key = VerifyingKey::from_bytes(&key_bytes)
        .map_err(|_| CapsuleSignatureError::InvalidPublicKey)?;
    let signature_bytes: [u8; 64] = BASE64
        .decode(&signed.signature)
        .map_err(|_| CapsuleSignatureError::InvalidSignature)?
        .try_into()
        .map_err(|_| CapsuleSignatureError::InvalidSignature)?;
    key.verify(
        signed.manifest_sha256.as_bytes(),
        &Signature::from_bytes(&signature_bytes),
    )
    .map_err(|_| CapsuleSignatureError::InvalidSignature)
}

pub fn check_capsule_key_trust(
    signed: &SignedCapsuleManifest,
    trusted_public_keys: &[String],
) -> Result<(), CapsuleSignatureError> {
    let trusted = trusted_public_keys.iter().any(|candidate| {
        let equal: bool = candidate
            .as_bytes()
            .ct_eq(signed.public_key.as_bytes())
            .into();
        equal
    });
    trusted
        .then_some(())
        .ok_or(CapsuleSignatureError::UntrustedKey)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use orch8_types::continuity::{
        ArtifactReference, CapsuleId, CapsuleManifest, CapsuleSchemaVersion, CheckpointIdentity,
        ContinuityId, ExecutionEpoch, RuntimeId, SequenceIdentity,
    };
    use orch8_types::ids::{BlockId, InstanceId, SequenceId, TenantId};
    use rand_core::OsRng;

    use super::*;

    fn manifest() -> CapsuleManifest {
        let now = Utc::now();
        CapsuleManifest {
            schema: CapsuleSchemaVersion::V1,
            capsule_id: CapsuleId::new(),
            continuity_id: ContinuityId::new(),
            source_instance_id: InstanceId::new(),
            epoch: ExecutionEpoch::initial(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            source_runtime_id: RuntimeId::new(),
            allowed_destination_runtime_id: Some(RuntimeId::new()),
            sequence: SequenceIdentity {
                id: SequenceId::new(),
                version: 1,
                content_sha256: "a".repeat(64),
            },
            checkpoint: CheckpointIdentity {
                block_id: BlockId::new("checkpoint"),
                sha256: "b".repeat(64),
            },
            requirements_sha256: "c".repeat(64),
            payload_artifact: ArtifactReference {
                key: "capsules/one".into(),
                sha256: "d".repeat(64),
                bytes: 42,
            },
            provenance_head: None,
            issued_at: now,
            expires_at: now + Duration::minutes(5),
            signing_key_id: "test-signing-key".into(),
            encryption_key_id: "test-encryption-key".into(),
        }
    }

    #[test]
    fn signature_detects_manifest_tampering_and_requires_separate_trust() {
        let key = SigningKey::generate(&mut OsRng);
        let mut signed = sign_capsule_manifest(manifest(), &key).unwrap();
        verify_signed_capsule(&signed).unwrap();
        check_capsule_key_trust(&signed, &[signed.public_key.clone()]).unwrap();
        assert_eq!(
            check_capsule_key_trust(&signed, &[]),
            Err(CapsuleSignatureError::UntrustedKey)
        );

        signed.manifest.expires_at += Duration::seconds(1);
        assert_eq!(
            verify_signed_capsule(&signed),
            Err(CapsuleSignatureError::Tampered)
        );
    }
}
