use super::{
    DelegationInputReference, EncodedDelegationInput, InputLimits, VerifiedDelegationInput,
    bytes_sha256, invalid, valid_hash, value_budget,
};
use crate::delegation_definition::wire::{DefinitionPayloadEncoding, DefinitionReadScope};
use crate::error::StorageError;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize, de::DeserializeSeed as _};
use std::cell::Cell;
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct DelegationInputEnvelope {
    pub version: u32,
    pub scope: DefinitionReadScope,
    pub request_sha256: String,
    pub reference: DelegationInputReference,
    pub payload_encoding: DefinitionPayloadEncoding,
    pub payload_base64: String,
}
impl std::fmt::Debug for DelegationInputEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegationInputEnvelope")
            .field("version", &self.version)
            .field("scope", &self.scope)
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}
impl DelegationInputEnvelope {
    pub fn new(
        scope: DefinitionReadScope,
        request_sha256: String,
        input: &EncodedDelegationInput,
    ) -> Result<Self, StorageError> {
        scope.validate()?;
        if !valid_hash(&request_sha256) {
            return Err(invalid());
        }
        Ok(Self {
            version: 1,
            scope,
            request_sha256,
            reference: input.reference.clone(),
            payload_encoding: DefinitionPayloadEncoding::JsonUtf8Base64,
            payload_base64: STANDARD.encode(&input.payload),
        })
    }
    /// Expected values come independently from the trusted claim. Copying this
    /// response's own reference into the verifier does not establish provenance.
    pub fn verify(
        &self,
        scope: &DefinitionReadScope,
        request_sha256: &str,
        reference: &DelegationInputReference,
        limits: InputLimits,
    ) -> Result<VerifiedDelegationInput, StorageError> {
        scope.validate()?;
        reference.validate(limits)?;
        let bytes = usize::try_from(reference.payload_bytes).map_err(|_| invalid())?;
        if !valid_hash(request_sha256)
            || self.version != 1
            || &self.scope != scope
            || self.request_sha256 != request_sha256
            || &self.reference != reference
            || self.payload_encoding != DefinitionPayloadEncoding::JsonUtf8Base64
            || self.payload_base64.len() != bytes.div_ceil(3) * 4
        {
            return Err(invalid());
        }
        let (_, value) = decode_payload(reference, &self.payload_base64, limits)?;
        Ok(VerifiedDelegationInput {
            scope: scope.clone(),
            request_sha256: request_sha256.into(),
            reference: reference.clone(),
            value,
        })
    }
}

// Shared exact-byte storage and transport decoder. Neither establishes authority.
pub(super) fn decode_payload(
    reference: &DelegationInputReference,
    payload_base64: &str,
    limits: InputLimits,
) -> Result<(Vec<u8>, serde_json::Value), StorageError> {
    reference.validate(limits)?;
    let bytes = usize::try_from(reference.payload_bytes).map_err(|_| invalid())?;
    if payload_base64.len() != bytes.div_ceil(3) * 4 {
        return Err(invalid());
    }
    let payload = STANDARD.decode(payload_base64).map_err(|_| invalid())?;
    if payload.len() != bytes || bytes_sha256(&payload) != reference.payload_sha256 {
        return Err(invalid());
    }
    let expanded = usize::try_from(reference.expanded_bytes).map_err(|_| invalid())?;
    let remaining = Cell::new(expanded);
    let mut decoder = serde_json::Deserializer::from_slice(&payload);
    let value = value_budget::ValueSeed {
        remaining: &remaining,
    }
    .deserialize(&mut decoder)
    .map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    if remaining.get() != 0 {
        return Err(invalid());
    }
    Ok((payload, value))
}
