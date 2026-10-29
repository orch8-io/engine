//! Pure ownership checks shared by atomic capsule activation transactions.
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{CapsuleManifest, ContinuityExecution, OwnershipState, RuntimeId},
    error::StorageError,
    ids::{InstanceId, TenantId},
};

pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("capsule activation identity or ownership conflict".into())
}

/// None is an exact accepted retry: it must never change scheduling or claims.
pub(crate) fn accepted_owner(
    tenant: &TenantId,
    manifest: &CapsuleManifest,
    current: &ContinuityExecution,
    runtime: RuntimeId,
    instance: InstanceId,
    now: DateTime<Utc>,
) -> Result<Option<ContinuityExecution>, StorageError> {
    if instance == manifest.source_instance_id
        || &manifest.tenant_id != tenant
        || &current.tenant_id != tenant
        || manifest.continuity_id != current.continuity_id
        || manifest
            .allowed_destination_runtime_id
            .is_some_and(|id| id != runtime)
    {
        return Err(invalid());
    }
    let epoch = manifest.epoch.checked_next().map_err(|_| invalid())?;
    if current.epoch == epoch
        && current.owner_runtime_id == runtime
        && current.current_instance_id == instance
        && matches!(
            current.state,
            OwnershipState::Owned | OwnershipState::Completed
        )
    {
        return Ok(None);
    }
    if current.state != OwnershipState::Transferring
        || current.epoch != manifest.epoch
        || current.owner_runtime_id != manifest.source_runtime_id
        || (current.current_instance_id != manifest.source_instance_id
            && current.current_instance_id != instance)
    {
        return Err(invalid());
    }
    Ok(Some(ContinuityExecution {
        current_instance_id: instance,
        owner_runtime_id: runtime,
        epoch,
        state: OwnershipState::Owned,
        updated_at: now,
        ..current.clone()
    }))
}
