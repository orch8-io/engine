//! Validate the logical key independently of a randomly allocated receipt ID.
use orch8_types::{continuity::EffectReceipt, error::StorageError};

pub(crate) fn validate(
    existing: &EffectReceipt,
    candidate: &EffectReceipt,
) -> Result<(), StorageError> {
    if existing.tenant_id != candidate.tenant_id
        || existing.continuity_id != candidate.continuity_id
        || existing.epoch != candidate.epoch
        || existing.instance_id != candidate.instance_id
        || existing.block_id != candidate.block_id
        || existing.attempt != candidate.attempt
    {
        return Err(StorageError::Conflict(
            "effect attempt binding has different ownership".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_bound(
    existing: &EffectReceipt,
    candidate: &EffectReceipt,
) -> Result<(), StorageError> {
    if existing.tenant_id != candidate.tenant_id
        || existing.continuity_id != candidate.continuity_id
        || existing.block_id != candidate.block_id
        || existing.attempt != candidate.attempt
    {
        return Err(StorageError::Conflict(
            "effect alias contains different intent evidence".into(),
        ));
    }
    Ok(())
}
