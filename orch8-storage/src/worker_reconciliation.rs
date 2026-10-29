//! One-time operator acceptance, distinct from original claim attribution.
use crate::{
    worker_claim::{WorkerClaimPrincipal, WorkerIssuedClaim},
    worker_outcome::WorkerAcceptedOutcome,
};
use orch8_types::{error::StorageError, ids::TenantId, worker::WorkerClaim};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerReconciliationApproval {
    pub(crate) approval_id: Uuid,
    pub(crate) tenant_id: TenantId,
    pub(crate) task_id: Uuid,
    pub(crate) claim: WorkerClaim,
    pub(crate) actor: WorkerClaimPrincipal,
    pub(crate) evidence_reference: String,
}
fn name(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}
fn invalid() -> StorageError {
    StorageError::Constraint("invalid worker reconciliation approval or scope".into())
}
impl WorkerReconciliationApproval {
    /// `actor` must come from verified server authentication, never request JSON.
    pub fn new(
        actor: WorkerClaimPrincipal,
        tenant_id: TenantId,
        task_id: Uuid,
        claim: WorkerClaim,
        approval_id: Uuid,
        evidence_reference: String,
    ) -> Result<Self, StorageError> {
        let value = Self {
            approval_id,
            tenant_id,
            task_id,
            claim,
            actor,
            evidence_reference,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        self.actor.validate()?;
        if self.approval_id.is_nil()
            || !name(self.tenant_id.as_str(), 128)
            || !name(&self.claim.worker_id, 128)
            || self.claim.claim_epoch > i64::MAX as u64
            || !name(&self.evidence_reference, 256)
            || !matches!(
                &self.actor,
                WorkerClaimPrincipal::Root {} | WorkerClaimPrincipal::ApiKey { .. }
            )
            || self
                .actor
                .tenant_id()
                .is_some_and(|tenant| tenant != &self.tenant_id)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn validate_outcome(
        &self,
        outcome: &WorkerAcceptedOutcome,
    ) -> Result<(), StorageError> {
        self.validate()?;
        if self.tenant_id != outcome.tenant_id
            || self.task_id != outcome.task_id
            || self.claim != outcome.claim
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ReconciledClaimHistory {
    Missing {},
    Legacy {},
    Unsecured {},
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerReconciliationDecision {
    pub(crate) version: u8,
    pub(crate) approval: WorkerReconciliationApproval,
    pub(crate) logical_output_sha256: String,
    pub(crate) history: ReconciledClaimHistory,
}
impl WorkerReconciliationDecision {
    pub(crate) fn prepare(
        approval: &WorkerReconciliationApproval,
        outcome: &WorkerAcceptedOutcome,
        issued: Option<&WorkerIssuedClaim>,
    ) -> Result<Self, StorageError> {
        approval.validate_outcome(outcome)?;
        let history = if let Some(issued) = issued {
            issued.validate()?;
            if issued.tenant_id != outcome.tenant_id
                || issued.task_id != outcome.task_id
                || issued.instance_id != outcome.instance_id
                || issued.claim != outcome.claim
            {
                return Err(invalid());
            }
            match &issued.principal {
                WorkerClaimPrincipal::Legacy {} => ReconciledClaimHistory::Legacy {},
                WorkerClaimPrincipal::Unsecured {} => ReconciledClaimHistory::Unsecured {},
                _ => {
                    return Err(StorageError::Conflict(
                        "authenticated worker claims require explicit handoff, not legacy reconciliation".into(),
                    ));
                }
            }
        } else {
            ReconciledClaimHistory::Missing {}
        };
        let value = Self {
            version: 1,
            approval: approval.clone(),
            logical_output_sha256: outcome.logical_output_sha256.clone().ok_or_else(invalid)?,
            history,
        };
        value.validate(outcome)?;
        Ok(value)
    }
    pub(crate) fn validate(&self, outcome: &WorkerAcceptedOutcome) -> Result<(), StorageError> {
        self.approval.validate_outcome(outcome)?;
        if self.version != 1
            || Some(&self.logical_output_sha256) != outcome.logical_output_sha256.as_ref()
            || self.logical_output_sha256.len() != 64
            || !self
                .logical_output_sha256
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Translate only the permanent approval-ID conflict; other storage failures
/// must still roll the whole completion transaction back and remain visible.
pub(crate) fn insert_error(error: sqlx::Error) -> StorageError {
    let rebound = error.as_database_error().is_some_and(|database| {
        database.is_unique_violation()
            && (database.constraint() == Some("uq_worker_reconciliation_approval")
                || database
                    .message()
                    .contains("uq_worker_reconciliation_approval"))
    });
    if rebound {
        StorageError::Conflict("approval_id is already bound to another worker report".into())
    } else {
        error.into()
    }
}
