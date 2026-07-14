//! Canonical signing and verification for one-time continuation grants.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use orch8_types::continuity::ContinuationGrant;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::manifest::canonical_json;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedContinuationGrant {
    pub grant: ContinuationGrant,
    pub grant_sha256: String,
    pub signature: String,
    pub public_key: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GrantSignatureError {
    #[error("continuation grant serialization failed: {0}")]
    Serialization(String),
    #[error("continuation grant hash does not match its canonical content")]
    Tampered,
    #[error("continuation grant public key is invalid")]
    InvalidPublicKey,
    #[error("continuation grant signature is invalid")]
    InvalidSignature,
    #[error("continuation grant signing key is not trusted")]
    UntrustedKey,
}

fn grant_sha256(grant: &ContinuationGrant) -> Result<String, GrantSignatureError> {
    let canonical = canonical_json(grant)
        .map_err(|error| GrantSignatureError::Serialization(error.to_string()))?;
    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

pub fn sign_continuation_grant(
    grant: ContinuationGrant,
    signing_key: &SigningKey,
) -> Result<SignedContinuationGrant, GrantSignatureError> {
    let grant_sha256 = grant_sha256(&grant)?;
    let signature = signing_key.sign(grant_sha256.as_bytes());
    Ok(SignedContinuationGrant {
        grant,
        grant_sha256,
        signature: BASE64.encode(signature.to_bytes()),
        public_key: BASE64.encode(signing_key.verifying_key().to_bytes()),
    })
}

pub fn verify_signed_continuation_grant(
    signed: &SignedContinuationGrant,
    trusted_public_keys: &[String],
) -> Result<(), GrantSignatureError> {
    let actual = grant_sha256(&signed.grant)?;
    let intact: bool = actual
        .as_bytes()
        .ct_eq(signed.grant_sha256.as_bytes())
        .into();
    if !intact {
        return Err(GrantSignatureError::Tampered);
    }
    let trusted = trusted_public_keys.iter().any(|candidate| {
        let equal: bool = candidate
            .as_bytes()
            .ct_eq(signed.public_key.as_bytes())
            .into();
        equal
    });
    if !trusted {
        return Err(GrantSignatureError::UntrustedKey);
    }
    let key_bytes: [u8; 32] = BASE64
        .decode(&signed.public_key)
        .map_err(|_| GrantSignatureError::InvalidPublicKey)?
        .try_into()
        .map_err(|_| GrantSignatureError::InvalidPublicKey)?;
    let key =
        VerifyingKey::from_bytes(&key_bytes).map_err(|_| GrantSignatureError::InvalidPublicKey)?;
    let signature_bytes: [u8; 64] = BASE64
        .decode(&signed.signature)
        .map_err(|_| GrantSignatureError::InvalidSignature)?
        .try_into()
        .map_err(|_| GrantSignatureError::InvalidSignature)?;
    key.verify(
        signed.grant_sha256.as_bytes(),
        &Signature::from_bytes(&signature_bytes),
    )
    .map_err(|_| GrantSignatureError::InvalidSignature)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use orch8_types::continuity::{
        ContinuationGrantId, ContinuationGrantState, ContinuityId, ExecutionEpoch, GrantAction,
        RuntimeId,
    };
    use orch8_types::ids::TenantId;
    use rand_core::OsRng;

    use super::*;

    #[test]
    fn signed_grant_requires_trust_and_detects_scope_tampering() {
        let now = Utc::now();
        let key = SigningKey::generate(&mut OsRng);
        let grant = ContinuationGrant {
            id: ContinuationGrantId::new(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            continuity_id: ContinuityId::new(),
            expected_epoch: ExecutionEpoch::initial(),
            destination_runtime_id: RuntimeId::new(),
            subject: Some("device-a".into()),
            allowed_actions: vec![GrantAction::Accept],
            nonce_sha256: "a".repeat(64),
            state: ContinuationGrantState::Active,
            issued_at: now,
            expires_at: now + Duration::minutes(5),
            consumed_at: None,
            signing_key_id: "grant-v1".into(),
        };
        let mut signed = sign_continuation_grant(grant, &key).unwrap();
        let trusted = vec![signed.public_key.clone()];
        verify_signed_continuation_grant(&signed, &trusted).unwrap();
        assert_eq!(
            verify_signed_continuation_grant(&signed, &[]),
            Err(GrantSignatureError::UntrustedKey)
        );
        signed.grant.allowed_actions.push(GrantAction::Resume);
        assert_eq!(
            verify_signed_continuation_grant(&signed, &trusted),
            Err(GrantSignatureError::Tampered)
        );
    }
}
