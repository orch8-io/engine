//! Keyset positions use SQL ordering columns, never timestamps inside JSON.
use orch8_types::{
    continuity::{ContinuityId, EffectId, EffectReceipt},
    error::StorageError,
    ids::TenantId,
};

/// Backend-local position in a tenant/continuity receipt ledger. Obtain it
/// from the preceding page; it is not a cross-backend or authorization token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectReceiptCursor {
    pub created_at: String,
    pub id: EffectId,
}

#[derive(Debug, Clone)]
pub struct EffectReceiptPage {
    pub receipts: Vec<EffectReceipt>,
    /// A full page may be the final page; follow its cursor until None.
    pub next: Option<EffectReceiptCursor>,
}

pub(crate) fn validate(
    receipt: &EffectReceipt,
    tenant: &TenantId,
    continuity: ContinuityId,
    id: EffectId,
) -> Result<(), StorageError> {
    if receipt.id != id || &receipt.tenant_id != tenant || receipt.continuity_id != continuity {
        return Err(StorageError::Constraint(
            "receipt record differs from indexed ownership".into(),
        ));
    }
    Ok(())
}
