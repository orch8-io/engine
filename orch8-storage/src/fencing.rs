//! Continuity ownership fence for worker-task lease mutations.
//!
//! Lives in the storage crate so every transport (HTTP, gRPC) and the engine
//! apply the exact same rule.

use orch8_types::continuity::OwnershipState;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::worker::WorkerTask;

use crate::StorageBackend;

/// Ownership half of the worker lease fence (the claim-epoch half is the
/// storage CAS). A task dispatched under owner epoch `e` may only mutate its
/// lease while the execution is still owned by the task's instance at exactly
/// epoch `e`; a task dispatched before enrollment may not act while the
/// execution is transferring or has moved to another instance/runtime.
pub async fn worker_task_ownership_current(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &WorkerTask,
) -> Result<bool, StorageError> {
    let execution = storage
        .get_continuity_execution_touching_instance(tenant_id, task.instance_id)
        .await?;
    Ok(match (task.continuity_epoch, execution) {
        (None, None) => true,
        (Some(_), None) => false,
        (expected, Some(execution)) => {
            execution.current_instance_id == task.instance_id
                && execution.state != OwnershipState::Transferring
                && expected.is_none_or(|epoch| execution.epoch.get() == epoch)
        }
    })
}
