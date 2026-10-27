//! Atomic destination ownership acceptance and initial scheduling.
use super::{
    SqliteStorage,
    continuity::{decode, encode, state_name},
};
use crate::capsule_activation::{accepted_owner, invalid};
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{CapsuleId, CapsuleManifest, ContinuityExecution, ContinuityLocation, RuntimeId},
    error::StorageError,
    ids::{InstanceId, TenantId},
};
use sqlx::Row;

pub(super) async fn activate(
    store: &SqliteStorage,
    tenant: &TenantId,
    capsule: CapsuleId,
    runtime: RuntimeId,
    instance: InstanceId,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let row = sqlx::query("SELECT manifest FROM execution_capsules WHERE tenant_id=?1 AND id=?2")
        .bind(tenant.as_str())
        .bind(capsule.to_string())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(invalid)?;
    let manifest: CapsuleManifest = decode(&row.get::<String, _>("manifest"))?;
    if manifest.capsule_id != capsule {
        return Err(invalid());
    }
    // Match effect dispatch and GC: ownership precedes instance locks.
    let row = sqlx::query(
        "SELECT record FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(tenant.as_str())
    .bind(manifest.continuity_id.to_string())
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(invalid)?;
    let current: ContinuityExecution = decode(&row.get::<String, _>("record"))?;
    let proof = sqlx::query("SELECT 1 FROM capsule_effect_imports p JOIN capsule_imports i ON i.tenant_id=p.tenant_id AND i.capsule_id=p.capsule_id AND i.destination_runtime_id=p.destination_runtime_id AND i.instance_id=p.instance_id WHERE p.tenant_id=?1 AND p.capsule_id=?2 AND p.destination_runtime_id=?3 AND p.instance_id=?4 AND p.continuity_id=?5")
        .bind(tenant.as_str()).bind(capsule.to_string()).bind(runtime.to_string())
        .bind(instance.to_string()).bind(manifest.continuity_id.to_string())
        .fetch_optional(&mut *tx).await?;
    if proof.is_none() {
        return Err(invalid());
    }
    if let Some(next) = accepted_owner(tenant, &manifest, &current, runtime, instance, now)? {
        persist(&mut tx, &next).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn persist(
    conn: &mut sqlx::SqliteConnection,
    next: &ContinuityExecution,
) -> Result<(), StorageError> {
    let epoch = i64::try_from(next.epoch.get()).map_err(|_| invalid())?;
    sqlx::query("UPDATE continuity_executions SET epoch=?1,owner_runtime_id=?2,state=?3,record=?4,updated_at=?5 WHERE tenant_id=?6 AND continuity_id=?7")
        .bind(epoch).bind(next.owner_runtime_id.to_string()).bind(state_name(next.state)?)
        .bind(encode(next)?).bind(next.updated_at.to_rfc3339()).bind(next.tenant_id.as_str())
        .bind(next.continuity_id.to_string()).execute(&mut *conn).await?;
    let location = ContinuityLocation {
        tenant_id: next.tenant_id.clone(),
        continuity_id: next.continuity_id,
        epoch: next.epoch,
        runtime_id: next.owner_runtime_id,
        instance_id: next.current_instance_id,
        handoff_id: None,
        entered_at: next.updated_at,
    };
    sqlx::query("INSERT INTO continuity_locations(tenant_id,continuity_id,epoch,runtime_id,instance_id,handoff_id,record,entered_at) VALUES(?1,?2,?3,?4,?5,NULL,?6,?7)")
        .bind(next.tenant_id.as_str()).bind(next.continuity_id.to_string()).bind(epoch)
        .bind(next.owner_runtime_id.to_string()).bind(next.current_instance_id.to_string())
        .bind(encode(&location)?).bind(next.updated_at.to_rfc3339()).execute(&mut *conn).await?;
    // Last write: any failure rolls back the new owner and location as well.
    let changed = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE task_instances SET state='scheduled',next_fire_at=?1,updated_at=?1,claimed_at=NULL WHERE id=?2 AND tenant_id=?3 AND state='paused' AND claimed_at IS NULL AND ({})", super::mobile_execution::ALLOWED)))
        .bind(next.updated_at.to_rfc3339()).bind(next.current_instance_id.to_string())
        .bind(next.tenant_id.as_str()).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(invalid());
    }
    Ok(())
}
