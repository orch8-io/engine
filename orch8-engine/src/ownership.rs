//! Continuity ownership fencing for the local scheduler and worker leases.
//!
//! A portable execution has exactly one owner per epoch. While its capsule is
//! being exported (`transferring`) or after another runtime accepted it (the
//! instance is no longer the execution's `current_instance_id`), the local
//! instance must not advance — neither by the scheduler nor by a worker task
//! completing — or two runtimes would drive the same execution.

use orch8_storage::StorageBackend;
use orch8_types::continuity::OwnershipState;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::worker::WorkerTask;

use crate::error::EngineError;

/// Whether this runtime may advance an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOwnership {
    /// Not enrolled in continuity, or owned here: advance normally.
    Owned,
    /// A handoff export is in flight: park and re-check later.
    Transferring,
    /// Another instance/runtime owns the execution now: never advance.
    Superseded,
}

pub async fn local_ownership(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    instance_id: InstanceId,
) -> Result<LocalOwnership, EngineError> {
    let Some(execution) = storage
        .get_continuity_execution_touching_instance(tenant_id, instance_id)
        .await?
    else {
        return Ok(LocalOwnership::Owned);
    };
    if execution.current_instance_id != instance_id {
        return Ok(LocalOwnership::Superseded);
    }
    Ok(match execution.state {
        OwnershipState::Transferring => LocalOwnership::Transferring,
        OwnershipState::Owned | OwnershipState::Completed => LocalOwnership::Owned,
    })
}

/// Ownership half of the worker lease fence; see
/// [`orch8_storage::fencing::worker_task_ownership_current`].
pub async fn worker_task_ownership_current(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &WorkerTask,
) -> Result<bool, EngineError> {
    Ok(orch8_storage::fencing::worker_task_ownership_current(storage, tenant_id, task).await?)
}
