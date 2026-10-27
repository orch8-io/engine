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

/// Complete ledger verdict with a bounded, deterministic witness sample.
/// Counts include every qualifying receipt; witnesses include at most 16 groups.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectAtMostOnceAudit {
    pub observed_receipts: u64,
    pub duplicate_groups: u64,
    pub witnesses: Vec<EffectDuplicateWitness>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectDuplicateWitness {
    pub destination_fingerprint: String,
    pub request_sha256: String,
}

pub(crate) fn validate_audit_candidate(
    tenant: &TenantId,
    continuity: ContinuityId,
    kind: orch8_types::continuity::EffectKind,
    candidate: Option<&EffectReceipt>,
) -> Result<(), StorageError> {
    if candidate.is_some_and(|r| {
        &r.tenant_id != tenant
            || r.continuity_id != continuity
            || r.kind != kind
            || r.state != orch8_types::continuity::EffectState::Dispatched
    }) {
        return Err(StorageError::Constraint(
            "effect audit candidate scope or state mismatch".into(),
        ));
    }
    Ok(())
}

pub(crate) fn decode_audit(
    observed: i64,
    duplicates: i64,
    witnesses: serde_json::Value,
) -> Result<EffectAtMostOnceAudit, StorageError> {
    let invalid = || StorageError::Constraint("invalid effect audit aggregation".into());
    let report = EffectAtMostOnceAudit {
        observed_receipts: u64::try_from(observed).map_err(|_| invalid())?,
        duplicate_groups: u64::try_from(duplicates).map_err(|_| invalid())?,
        witnesses: serde_json::from_value(witnesses).map_err(|_| invalid())?,
    };
    if report.duplicate_groups > report.observed_receipts / 2
        || report.witnesses.len() as u64 != report.duplicate_groups.min(16)
    {
        return Err(invalid());
    }
    Ok(report)
}

/// Complete state counts with bounded evidence for safety decisions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EffectLedgerStatus {
    pub receipt_count: u64,
    pub unresolved_count: u64,
    pub unknown_count: u64,
    pub unresolved_sample: Vec<EffectReceipt>,
    pub unknown_request_sha256: Vec<String>,
}

pub(crate) fn decode_status(
    tenant: &TenantId,
    continuity: ContinuityId,
    counts: (i64, i64, i64),
    unresolved: serde_json::Value,
    unknown: serde_json::Value,
) -> Result<EffectLedgerStatus, StorageError> {
    let invalid = || StorageError::Constraint("invalid effect ledger state summary".into());
    let status = EffectLedgerStatus {
        receipt_count: u64::try_from(counts.0).map_err(|_| invalid())?,
        unresolved_count: u64::try_from(counts.1).map_err(|_| invalid())?,
        unknown_count: u64::try_from(counts.2).map_err(|_| invalid())?,
        unresolved_sample: serde_json::from_value(unresolved).map_err(|_| invalid())?,
        unknown_request_sha256: serde_json::from_value(unknown).map_err(|_| invalid())?,
    };
    if status.unknown_count > status.unresolved_count
        || status.unresolved_count > status.receipt_count
        || status.unresolved_sample.len() as u64 != status.unresolved_count.min(16)
        || status.unknown_request_sha256.len() as u64 != status.unknown_count.min(16)
        || status.unresolved_sample.iter().any(|r| {
            &r.tenant_id != tenant || r.continuity_id != continuity || r.state.is_resolved()
        })
    {
        return Err(invalid());
    }
    Ok(status)
}

pub(crate) fn validate_known_states(invalid: i64) -> Result<(), StorageError> {
    if invalid != 0 {
        return Err(StorageError::Constraint(
            "effect ledger contains missing or unrecognized states".into(),
        ));
    }
    Ok(())
}

/// Minimal immutable planning fields. Raw receipt records are decoded one at a
/// time; actionable outcomes and all unsettled work remain plan evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompensationEffect {
    pub id: EffectId,
    pub block_id: orch8_types::ids::BlockId,
    pub state: orch8_types::continuity::EffectState,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl From<&EffectReceipt> for CompensationEffect {
    fn from(receipt: &EffectReceipt) -> Self {
        Self {
            id: receipt.id,
            block_id: receipt.block_id.clone(),
            state: receipt.state,
            created_at: receipt.created_at,
        }
    }
}

pub(crate) fn compensation_relevant(state: orch8_types::continuity::EffectState) -> bool {
    matches!(
        state,
        orch8_types::continuity::EffectState::Planned
            | orch8_types::continuity::EffectState::Prepared
            | orch8_types::continuity::EffectState::Dispatched
            | orch8_types::continuity::EffectState::Committed
            | orch8_types::continuity::EffectState::Verified
            | orch8_types::continuity::EffectState::Unknown
    )
}
