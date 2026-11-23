//! Immutable failure/lease evidence independent of reusable hot tasks and native GC.
use crate::{
    worker_invocation::{WorkerDispatchEffect, WorkerInvocation},
    worker_outcome::digest,
};
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{EffectId, EffectReceipt, EffectState},
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::{
        WorkerAttemptEventKind, WorkerTaskEffectResolution, WorkerTaskResolution,
        WorkerTaskResolutionAction, WorkerTaskState,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

/// Opaque logical report metadata survives sealing without trusting caller ciphertext.
#[derive(Clone)]
pub struct WorkerResolutionEvidence {
    pub(crate) reason: Value,
    pub(crate) logical_sha256: String,
    pub(crate) stored_sha256: String,
    pub(crate) sealed: bool,
    #[cfg(feature = "postgres")]
    pub(crate) expired_lease: Option<ExpiredLeaseObservation>,
}

#[cfg(feature = "postgres")]
#[derive(Clone)]
pub(crate) struct ExpiredLeaseObservation {
    pub(crate) worker_id: Option<String>,
    pub(crate) claimed_at: Option<DateTime<Utc>>,
    pub(crate) heartbeat_at: Option<DateTime<Utc>>,
    pub(crate) checkpoint_seq: u64,
    pub(crate) default_lease: std::time::Duration,
}
impl WorkerResolutionEvidence {
    pub(crate) fn new(resolution: &WorkerTaskResolution) -> Self {
        let reason = Value::String(resolution.reason.clone());
        let hash = digest(&reason);
        Self {
            reason,
            logical_sha256: hash.clone(),
            stored_sha256: hash,
            sealed: false,
            #[cfg(feature = "postgres")]
            expired_lease: None,
        }
    }

    /// Carry the reaper's observation into the transaction that can adopt legacy work.
    #[must_use]
    pub fn expired_lease(
        resolution: &WorkerTaskResolution,
        task: &orch8_types::worker::WorkerTask,
        default_lease: std::time::Duration,
    ) -> Self {
        let evidence = Self::new(resolution);
        #[cfg(feature = "postgres")]
        {
            if task.id != resolution.task_id
                || task.instance_id != resolution.instance_id
                || task.claim_epoch != resolution.expected_claim_epoch
                || task.state != resolution.expected_state
            {
                return evidence;
            }
            Self {
                expired_lease: Some(ExpiredLeaseObservation {
                    worker_id: task.worker_id.clone(),
                    claimed_at: task.claimed_at,
                    heartbeat_at: task.heartbeat_at,
                    checkpoint_seq: task.checkpoint_seq,
                    default_lease,
                }),
                ..evidence
            }
        }
        #[cfg(not(feature = "postgres"))]
        {
            let _ = (task, default_lease);
            evidence
        }
    }
    pub(crate) fn with_stored_reason(&self, reason: Value) -> Self {
        Self {
            stored_sha256: digest(&reason),
            reason,
            logical_sha256: self.logical_sha256.clone(),
            sealed: true,
            #[cfg(feature = "postgres")]
            expired_lease: self.expired_lease.clone(),
        }
    }
    pub(crate) fn validate(&self, resolution: &WorkerTaskResolution) -> Result<(), StorageError> {
        if digest(&self.reason) != self.stored_sha256
            || digest(&Value::String(resolution.reason.clone())) != self.logical_sha256
            || (!self.sealed && self.stored_sha256 != self.logical_sha256)
            || !matches!(
                resolution.event,
                WorkerAttemptEventKind::Failed
                    | WorkerAttemptEventKind::TimedOut
                    | WorkerAttemptEventKind::Reclaimed
                    | WorkerAttemptEventKind::Cancelled
            )
        {
            return Err(invalid());
        }
        Ok(())
    }
}
impl std::fmt::Debug for WorkerResolutionEvidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerResolutionEvidence")
            .field("logical_sha256", &self.logical_sha256)
            .finish_non_exhaustive()
    }
}

/// The reported outcome is uncertainty evidence, never proof that a provider did not act.
/// A future factual success remains a separate accepted-outcome fact.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerResolutionOutcome {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub task_id: Uuid,
    pub instance_id: InstanceId,
    pub claim_epoch: u64,
    pub worker_id: Option<String>,
    pub invocation: Option<WorkerInvocation>,
    pub dispatch_effect: Option<WorkerDispatchEffect>,
    #[serde(default)]
    pub admitted: bool,
    pub event: WorkerAttemptEventKind,
    pub effect_resolution: WorkerTaskEffectResolution,
    pub effect_id: Option<EffectId>,
    pub effect_state_before: Option<EffectState>,
    pub effect_state_after: Option<EffectState>,
    pub reason: Value,
    pub logical_reason_sha256: String,
    pub stored_reason_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    pub retryable: bool,
    pub action: Value,
    pub resolved_at: DateTime<Utc>,
}
impl std::fmt::Debug for WorkerResolutionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerResolutionOutcome")
            .field("task_id", &self.task_id)
            .field("claim_epoch", &self.claim_epoch)
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}
impl WorkerResolutionOutcome {
    pub(crate) fn validate(
        &self,
        tenant: &TenantId,
        task: Uuid,
        instance: InstanceId,
        epoch: u64,
        worker: Option<&str>,
    ) -> Result<(), StorageError> {
        if self.tenant_id != *tenant
            || self.task_id != task
            || self.instance_id != instance
            || self.claim_epoch != epoch
            || self.worker_id.as_deref() != worker
            || digest(&self.reason) != self.stored_reason_sha256
        {
            return Err(invalid());
        }
        if let Some(invocation) = &self.invocation {
            invocation.validate_scope(tenant, task, instance)?;
        }
        Ok(())
    }
    pub(crate) fn validate_retained_receipt(
        &self,
        receipt: EffectReceipt,
    ) -> Result<(), StorageError> {
        let source = self.invocation.as_ref().ok_or_else(invalid)?;
        let snapshot = ResolutionSource {
            tenant: self.tenant_id.clone(),
            task_id: self.task_id,
            instance: self.instance_id,
            block: source.block_id.clone(),
            handler: source.handler_name.clone(),
            attempt: source.attempt,
            state: WorkerTaskState::Failed,
            claim_epoch: self.claim_epoch,
            worker_id: self.worker_id.clone(),
            effect_id: self.effect_id,
            invocation: Some(source.clone()),
            proof: self.dispatch_effect.clone(),
            admitted: self.admitted,
        };
        snapshot.validate()?;
        if Some(receipt.state) != self.effect_state_after || receipt.state != EffectState::Unknown {
            return Err(invalid());
        }
        snapshot.settled_receipt(WorkerTaskEffectResolution::Keep, receipt)?;
        Ok(())
    }
    pub(crate) fn is_logical_reason(&self) -> bool {
        digest(&self.reason) == self.logical_reason_sha256
    }
}

pub(crate) struct ResolutionSource {
    pub tenant: TenantId,
    pub task_id: Uuid,
    pub instance: InstanceId,
    pub block: orch8_types::ids::BlockId,
    pub handler: String,
    pub attempt: u16,
    pub state: WorkerTaskState,
    pub claim_epoch: u64,
    pub worker_id: Option<String>,
    pub effect_id: Option<EffectId>,
    pub invocation: Option<WorkerInvocation>,
    pub proof: Option<WorkerDispatchEffect>,
    pub admitted: bool,
}
impl ResolutionSource {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        if let Some(source) = &self.invocation {
            source.validate_scope(&self.tenant, self.task_id, self.instance)?;
            if source.block_id != self.block
                || source.handler_name != self.handler
                || source.attempt != self.attempt
                || source
                    .effect_id
                    .is_some_and(|id| Some(id) != self.effect_id)
            {
                return Err(invalid());
            }
        }
        if self
            .proof
            .as_ref()
            .is_some_and(|proof| Some(proof.effect_id) != self.effect_id)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn validate_disposition(
        &self,
        resolution: &WorkerTaskResolution,
    ) -> Result<(), StorageError> {
        if resolution.effect_resolution == WorkerTaskEffectResolution::Abandoned
            && (self.state != WorkerTaskState::Pending
                || self.claim_epoch != 0
                || self.worker_id.is_some())
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub(crate) fn outcome(
        &self,
        resolution: &WorkerTaskResolution,
        evidence: &WorkerResolutionEvidence,
        states: Option<(EffectState, EffectState)>,
    ) -> WorkerResolutionOutcome {
        let action = match &resolution.action {
            WorkerTaskResolutionAction::Requeue => json!({"kind":"requeue"}),
            WorkerTaskResolutionAction::Retry {
                retry_task,
                node_id,
                fire_at,
                ..
            } => {
                json!({"kind":"retry","task_id":retry_task.id,"attempt":retry_task.attempt,"node_id":node_id,"fire_at":fire_at})
            }
            WorkerTaskResolutionAction::FailNode {
                node_id, fire_at, ..
            } => {
                json!({"kind":"fail_node","node_id":node_id,"fire_at":fire_at})
            }
            WorkerTaskResolutionAction::FailInstance => json!({"kind":"fail_instance"}),
            WorkerTaskResolutionAction::FailTaskOnly => json!({"kind":"fail_task_only"}),
        };
        WorkerResolutionOutcome {
            id: Uuid::now_v7(),
            tenant_id: self.tenant.clone(),
            task_id: self.task_id,
            instance_id: self.instance,
            claim_epoch: self.claim_epoch,
            worker_id: self.worker_id.clone(),
            invocation: self.invocation.clone(),
            dispatch_effect: self.proof.clone(),
            admitted: self.admitted,
            event: resolution.event,
            effect_resolution: resolution.effect_resolution,
            effect_id: self.effect_id,
            effect_state_before: states.map(|s| s.0),
            effect_state_after: states.map(|s| s.1),
            reason: evidence.reason.clone(),
            logical_reason_sha256: evidence.logical_sha256.clone(),
            stored_reason_sha256: evidence.stored_sha256.clone(),
            error_code: resolution.error_code.clone(),
            retryable: resolution.retryable,
            action,
            resolved_at: Utc::now(),
        }
    }
    pub(crate) fn settled_receipt(
        &self,
        disposition: WorkerTaskEffectResolution,
        mut receipt: EffectReceipt,
    ) -> Result<EffectReceipt, StorageError> {
        let source = self.invocation.as_ref().ok_or_else(invalid)?;
        let owner = source.continuity.as_ref().ok_or_else(invalid)?;
        if self.effect_id != Some(receipt.id)
            || receipt.tenant_id != self.tenant
            || receipt.continuity_id != owner.continuity_id
            || receipt.epoch.get() != owner.epoch
            || receipt.instance_id != self.instance
            || receipt.block_id != self.block
            || receipt.attempt != u32::from(self.attempt)
            || self.proof.as_ref().is_some_and(|proof| {
                proof.effect_id != receipt.id
                    || proof.kind != receipt.kind
                    || proof.request_sha256 != receipt.request_sha256
                    || proof.destination_fingerprint != receipt.destination_fingerprint
            })
        {
            return Err(invalid());
        }
        match disposition {
            WorkerTaskEffectResolution::Keep => {}
            WorkerTaskEffectResolution::Unknown => match receipt.state {
                EffectState::Dispatched => {
                    receipt
                        .transition(EffectState::Unknown, Utc::now())
                        .map_err(|_| invalid())?;
                }
                EffectState::Unknown => {}
                _ => return Err(invalid()),
            },
            WorkerTaskEffectResolution::Abandoned => {
                if self.state != WorkerTaskState::Pending
                    || self.claim_epoch != 0
                    || self.worker_id.is_some()
                {
                    return Err(invalid());
                }
                if receipt.state == EffectState::Dispatched {
                    receipt
                        .transition(EffectState::Unknown, Utc::now())
                        .map_err(|_| invalid())?;
                }
                if receipt.state != EffectState::Unknown {
                    return Err(invalid());
                }
                receipt
                    .transition(EffectState::Abandoned, Utc::now())
                    .map_err(|_| invalid())?;
            }
        }
        Ok(receipt)
    }
}
fn invalid() -> StorageError {
    StorageError::Conflict("worker resolution evidence or effect requires reconciliation".into())
}

/// Keep valid UTF-8 reports lossless across JSONB, including U+0000.
/// This internal representation does not change public outcome serialization.
pub(crate) fn encode_record(outcome: &WorkerResolutionOutcome) -> Result<Value, StorageError> {
    let mut record = serde_json::to_value(outcome)?;
    record["reason_encoding"] = Value::String("json_text_v1".into());
    record["reason"] = Value::String(serde_json::to_string(&outcome.reason)?);
    Ok(record)
}
pub(crate) fn decode_record(mut record: Value) -> Result<WorkerResolutionOutcome, StorageError> {
    if let Some(encoding) = record.get("reason_encoding") {
        if encoding.as_str() != Some("json_text_v1") {
            return Err(invalid());
        }
        let reason = record
            .get("reason")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        let reason: Value = serde_json::from_str(reason)?;
        record["reason"] = reason;
        record
            .as_object_mut()
            .ok_or_else(invalid)?
            .remove("reason_encoding");
    }
    Ok(serde_json::from_value(record)?)
}

/// Text diagnostics escape the one character `PostgreSQL` text cannot represent.
/// The durable report retains the exact original string separately.
#[must_use]
pub fn diagnostic_reason(reason: &str) -> std::borrow::Cow<'_, str> {
    if reason.contains('\0') {
        std::borrow::Cow::Owned(reason.replace('\0', "\\u0000"))
    } else {
        std::borrow::Cow::Borrowed(reason)
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::*;

    #[test]
    fn expired_lease_evidence_tracks_only_the_matching_task_through_reason_sealing() {
        let task_id = Uuid::new_v4();
        let instance_id = InstanceId::new();
        let observed_at = Utc::now();
        let mut task: orch8_types::worker::WorkerTask = serde_json::from_value(json!({
            "id": task_id, "instance_id": instance_id, "block_id": "work",
            "handler_name": "remote", "params": {}, "context": {}, "attempt": 0,
            "state": "claimed", "worker_id": "holder", "claim_epoch": 0,
            "checkpoint_seq": 3, "created_at": observed_at, "heartbeat_at": observed_at,
        }))
        .unwrap();
        let resolution = WorkerTaskResolution {
            task_id,
            instance_id,
            expected_state: WorkerTaskState::Claimed,
            expected_claim_epoch: 0,
            expected_worker_id: None,
            holder_worker_id: None,
            event: WorkerAttemptEventKind::Reclaimed,
            effect_resolution: WorkerTaskEffectResolution::Unknown,
            reason: "lease expired".into(),
            error_code: None,
            retryable: true,
            action: WorkerTaskResolutionAction::Requeue,
        };
        let lease = std::time::Duration::from_secs(30);
        let evidence = WorkerResolutionEvidence::expired_lease(&resolution, &task, lease)
            .with_stored_reason(json!({"sealed":"reason"}));
        evidence.validate(&resolution).unwrap();
        let observation = evidence.expired_lease.unwrap();
        assert_eq!(observation.heartbeat_at, Some(observed_at));
        assert_eq!(observation.checkpoint_seq, 3);
        assert_eq!(observation.default_lease, lease);
        task.claim_epoch = 1;
        assert!(
            WorkerResolutionEvidence::expired_lease(&resolution, &task, lease)
                .expired_lease
                .is_none()
        );
    }
}
