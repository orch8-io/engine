//! Atomic operator settlement and its provenance boundary.
use orch8_types::{
    continuity::{EffectReceipt, ProvenanceEntry},
    error::StorageError,
};
use sha2::{Digest, Sha256};
use std::fmt::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectResolutionCommit {
    Applied,
    /// Receipt or continuity epoch changed; the caller must reload explicitly.
    Stale,
    /// Another entry consumed the proposed predecessor; rebuild only the entry.
    ProvenanceChanged,
}

/// Digest matches the API's original receipt evidence encoding.
pub fn effect_resolution_digest(receipt: &EffectReceipt) -> Result<String, StorageError> {
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(serde_json::to_vec(receipt)?) {
        write!(&mut hash, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(hash)
}

pub(crate) fn validate(
    expected: &EffectReceipt,
    next: &EffectReceipt,
    entry: &ProvenanceEntry,
) -> Result<(), StorageError> {
    let mut permitted = expected.clone();
    permitted.state = next.state;
    permitted
        .provider_receipt_id
        .clone_from(&next.provider_receipt_id);
    permitted.updated_at = next.updated_at;
    if permitted != *next
        || !expected.state.can_transition_to(next.state)
        || next.updated_at < expected.updated_at
        || entry.tenant_id != expected.tenant_id
        || entry.continuity_id != expected.continuity_id
        || entry.kind != "effect_resolved"
        || entry.payload_sha256 != effect_resolution_digest(next)?
    {
        return Err(StorageError::Constraint(
            "invalid effect resolution evidence or scope".into(),
        ));
    }
    Ok(())
}
