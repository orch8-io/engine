//! Authority captured before an effect is prepared, rechecked at dispatch.
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{
        ContinuityExecution, EffectDispatchOutcome, EffectReceipt, EffectState, ExecutionEpoch,
        OwnershipState,
    },
    ids::{InstanceId, TenantId},
};

/// This value is not a serialized capability. Backends verify every field
/// against durable ownership and the original instance claim at admission.
#[derive(Debug, Clone, Copy)]
pub struct EffectDispatchFence {
    pub instance_id: InstanceId,
    pub epoch: ExecutionEpoch,
    pub owner_runtime_id: orch8_types::continuity::RuntimeId,
    /// Some binds the original scheduler claim. None is reserved for the
    /// explicit unclaimed public step APIs and worker recovery operations.
    pub claim_token: Option<DateTime<Utc>>,
}

pub(crate) fn owns(
    execution: &ContinuityExecution,
    tenant: &TenantId,
    next: &EffectReceipt,
    fence: &EffectDispatchFence,
) -> bool {
    execution.tenant_id == *tenant
        && next.tenant_id == *tenant
        && execution.continuity_id == next.continuity_id
        && execution.state == OwnershipState::Owned
        && execution.epoch == fence.epoch
        && execution.owner_runtime_id == fence.owner_runtime_id
        && execution.current_instance_id == fence.instance_id
        && next.instance_id == fence.instance_id
        && next.epoch == fence.epoch
}

pub(crate) fn live(state: &str, claim: Option<DateTime<Utc>>, fence: &EffectDispatchFence) -> bool {
    match fence.claim_token {
        Some(token) => state == "running" && claim == Some(token),
        None => matches!(state, "running" | "scheduled" | "waiting"),
    }
}

pub(crate) fn verdict(
    receipts: &[EffectReceipt],
    next: &EffectReceipt,
    guarded: bool,
) -> EffectDispatchOutcome {
    let Some(current) = receipts.iter().find(|receipt| receipt.id == next.id) else {
        return EffectDispatchOutcome::Stale;
    };
    if current.state != EffectState::Prepared {
        return EffectDispatchOutcome::Stale;
    }
    let mut expected = current.clone();
    expected.state = EffectState::Dispatched;
    expected.updated_at = next.updated_at;
    if &expected != next {
        return EffectDispatchOutcome::Stale;
    }
    if guarded
        && receipts.iter().any(|receipt| {
            receipt.id != next.id
                && receipt.kind == next.kind
                && receipt.destination_fingerprint == next.destination_fingerprint
                && receipt.request_sha256 == next.request_sha256
                && !matches!(
                    receipt.state,
                    EffectState::Abandoned | EffectState::Compensated
                )
                && (matches!(
                    receipt.state,
                    EffectState::Dispatched
                        | EffectState::Committed
                        | EffectState::Unknown
                        | EffectState::Verified
                ) || (receipt.created_at, receipt.id) < (next.created_at, next.id))
        })
    {
        EffectDispatchOutcome::Duplicate
    } else {
        EffectDispatchOutcome::Dispatched
    }
}
