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
    /// Compensation has permanently stopped forward execution.
    Compensating,
    /// Another instance/runtime owns the execution now: never advance.
    Superseded,
}

pub async fn local_ownership(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    instance_id: InstanceId,
) -> Result<LocalOwnership, EngineError> {
    // A missing continuity after history GC cannot turn a physical source back
    // into unenrolled legacy work. Retirement is a permanent independent fence.
    if storage.is_physical_instance_retired(instance_id).await? {
        return Ok(LocalOwnership::Superseded);
    }
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
        OwnershipState::Compensating => LocalOwnership::Compensating,
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

#[cfg(test)]
mod retirement_tests {
    use super::*;
    use orch8_storage::testing::{backends::TestBackend, faulty::FaultStorage};

    #[tokio::test]
    async fn physical_retirement_fences_missing_continuity_and_read_errors_fail_closed() {
        for backend in TestBackend::sqlite_and_postgres().await {
            let id = InstanceId::new();
            let tenant = TenantId::new("retired-owner").unwrap();
            if backend.kind.is_postgres() {
                sqlx::query("INSERT INTO physical_instance_retirements VALUES($1,$2,$3,$4,$5)")
                    .bind(id.into_uuid())
                    .bind(tenant.as_str())
                    .bind(uuid::Uuid::new_v4())
                    .bind(uuid::Uuid::new_v4())
                    .bind(chrono::Utc::now())
                    .execute(backend.pg_pool())
                    .await
                    .unwrap();
            } else {
                sqlx::query("INSERT INTO physical_instance_retirements VALUES(?1,?2,?3,?4,?5)")
                    .bind(id.to_string())
                    .bind(tenant.as_str())
                    .bind(uuid::Uuid::new_v4().to_string())
                    .bind(uuid::Uuid::new_v4().to_string())
                    .bind(chrono::Utc::now().to_rfc3339())
                    .execute(backend.sqlite_pool())
                    .await
                    .unwrap();
            }
            for scope in [tenant.clone(), TenantId::new("foreign-owner").unwrap()] {
                assert_eq!(
                    local_ownership(backend.storage.as_ref(), &scope, id)
                        .await
                        .unwrap(),
                    LocalOwnership::Superseded
                );
            }
            assert_eq!(
                local_ownership(backend.storage.as_ref(), &tenant, InstanceId::new())
                    .await
                    .unwrap(),
                LocalOwnership::Owned
            );
            let (storage, faults) = FaultStorage::wrap(backend.storage.clone());
            faults
                .on("is_physical_instance_retired")
                .times(1)
                .fail_with(orch8_types::error::StorageError::Connection(
                    "retirement authority unavailable".into(),
                ));
            assert!(
                local_ownership(storage.as_ref(), &tenant, InstanceId::new())
                    .await
                    .is_err(),
                "missing retirement evidence must not become permission to run"
            );
        }
    }
}
