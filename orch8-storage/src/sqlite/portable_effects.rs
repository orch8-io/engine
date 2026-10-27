//! Portable receipts retain origin identity; active aliases carry current authority.
use super::{
    SqliteStorage,
    continuity::{decode, encode, state_name},
};
use crate::portable_effects::CapsuleEffectImport;
use orch8_types::{
    continuity::{
        CapsuleEffectBinding, CapsuleId, CapsulePayload, ContinuityExecution, ContinuityId,
        ContinuityLocation, EffectId, EffectKind, EffectReceipt, OwnershipState, RuntimeId,
    },
    continuity_advanced::WorkflowInvariant,
    error::StorageError,
    ids::{BlockId, InstanceId, SequenceId, TenantId},
    instance::TaskInstance,
};
use sqlx::Row;
fn invalid() -> StorageError {
    StorageError::Constraint("portable effect scope or identity conflict".into())
}

pub(super) async fn check_namespace_on(
    conn: &mut sqlx::SqliteConnection,
    candidate: &ContinuityExecution,
) -> Result<(), StorageError> {
    let row=sqlx::query("SELECT continuity_id FROM capsule_effect_imports WHERE tenant_id=?1 AND instance_id=?2 LIMIT 1")
        .bind(candidate.tenant_id.as_str()).bind(candidate.current_instance_id.to_string()).fetch_optional(&mut *conn).await?;
    if let Some(row) = row
        && ContinuityId::from_uuid(
            row.get::<String, _>("continuity_id")
                .parse::<uuid::Uuid>()
                .map_err(|e| StorageError::Query(e.to_string()))?,
        ) != candidate.continuity_id
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) async fn attempts(
    store: &SqliteStorage,
    owner: &ContinuityExecution,
) -> Result<(Vec<CapsuleEffectBinding>, Vec<BlockId>), StorageError> {
    let epoch = i64::try_from(owner.epoch.get()).map_err(|_| invalid())?;
    let rows=sqlx::query("SELECT block_id,attempt,effect_id FROM effect_attempt_bindings WHERE tenant_id=?1 AND continuity_id=?2 AND epoch=?3 AND instance_id=?4 ORDER BY block_id,attempt LIMIT 10001")
  .bind(owner.tenant_id.as_str()).bind(owner.continuity_id.to_string()).bind(epoch).bind(owner.current_instance_id.to_string()).fetch_all(&store.pool).await?;
    if rows.len() > CapsulePayload::MAX_PENDING_ITEMS {
        return Err(invalid());
    }
    let mut bindings = Vec::with_capacity(rows.len());
    for row in rows {
        bindings.push(CapsuleEffectBinding {
            block_id: BlockId::new(row.get::<String, _>("block_id")),
            attempt: u32::try_from(row.get::<i64, _>("attempt")).map_err(|_| invalid())?,
            effect_id: EffectId::from_uuid(
                row.get::<String, _>("effect_id")
                    .parse::<uuid::Uuid>()
                    .map_err(|e| StorageError::Query(e.to_string()))?,
            ),
        });
    }
    let rows=sqlx::query("SELECT block_id FROM effect_block_resets WHERE tenant_id=?1 AND instance_id=?2 ORDER BY block_id LIMIT 10001").bind(owner.tenant_id.as_str()).bind(owner.current_instance_id.to_string()).fetch_all(&store.pool).await?;
    if rows.len() > CapsulePayload::MAX_PENDING_ITEMS {
        return Err(invalid());
    }
    Ok((
        bindings,
        rows.into_iter()
            .map(|row| BlockId::new(row.get::<String, _>("block_id")))
            .collect(),
    ))
}

pub(super) async fn guards(
    store: &SqliteStorage,
    tenant: &TenantId,
    continuity: ContinuityId,
    sequence: SequenceId,
    version: i32,
    selected: Option<EffectKind>,
) -> Result<Vec<WorkflowInvariant>, StorageError> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT record FROM workflow_invariants WHERE tenant_id=");
    query.push_bind(tenant.as_str()).push(" AND sequence_id=").push_bind(sequence.to_string()).push(" AND enabled=1 AND (sequence_version IS NULL OR sequence_version=").push_bind(version).push(") AND json_extract(record,'$.commit_guard')=1 AND json_extract(record,'$.rule.type')='effect_at_most_once'");
    if let Some(k) = selected {
        query
            .push(" AND json_extract(record,'$.rule.kind')=")
            .push_bind(state_name(k)?);
    }
    query.push(" ORDER BY created_at,id LIMIT 10001");
    let rows = query.build().fetch_all(&store.pool).await?;
    let mut global = Vec::with_capacity(rows.len());
    for row in rows {
        global.push(decode(&row.get::<String, _>("record"))?);
    }
    let mut query =
        sqlx::QueryBuilder::new("SELECT record FROM continuity_effect_guards WHERE tenant_id=");
    query
        .push_bind(tenant.as_str())
        .push(" AND continuity_id=")
        .push_bind(continuity.to_string());
    if let Some(k) = selected {
        query
            .push(" AND json_extract(record,'$.rule.kind')=")
            .push_bind(state_name(k)?);
    }
    query.push(" LIMIT 10001");
    let rows = query.build().fetch_all(&store.pool).await?;
    let mut frozen = Vec::with_capacity(rows.len());
    for row in rows {
        let guard: WorkflowInvariant = decode(&row.get::<String, _>("record"))?;
        if guard.sequence_id != sequence {
            return Err(invalid());
        }
        frozen.push(guard);
    }
    crate::portable_effects::merge_guards(global, frozen)
}

pub(super) async fn check_replay(
    store: &SqliteStorage,
    tenant: &TenantId,
    capsule: CapsuleId,
    runtime: RuntimeId,
    instance: InstanceId,
    effects: Option<&CapsuleEffectImport>,
) -> Result<(), StorageError> {
    let row=sqlx::query("SELECT instance_id,manifest_sha256 FROM capsule_effect_imports WHERE tenant_id=?1 AND capsule_id=?2 AND destination_runtime_id=?3")
  .bind(tenant.as_str()).bind(capsule.to_string()).bind(runtime.to_string()).fetch_optional(&store.pool).await?;
    if let Some(effects) = effects {
        let row = row.ok_or_else(invalid)?;
        if InstanceId::from_uuid(
            row.get::<String, _>("instance_id")
                .parse::<uuid::Uuid>()
                .map_err(|e| StorageError::Query(e.to_string()))?,
        ) != instance
            || row.get::<String, _>("manifest_sha256") != effects.manifest_sha256
        {
            return Err(invalid());
        }
    } else if row.is_some() {
        return Err(invalid());
    }

    Ok(())
}

pub(super) async fn import_on(
    conn: &mut sqlx::SqliteConnection,
    capsule: CapsuleId,
    runtime: RuntimeId,
    instance: &TaskInstance,
    effects: &CapsuleEffectImport,
) -> Result<(), StorageError> {
    effects.validate(instance.sequence_id)?;
    if effects.manifest.capsule_id != capsule
        || effects
            .manifest
            .allowed_destination_runtime_id
            .is_some_and(|target| target != runtime)
    {
        return Err(invalid());
    }

    if effects.source.tenant_id != instance.tenant_id {
        return Err(invalid());
    }
    let pending = ContinuityExecution {
        current_instance_id: instance.id,
        state: OwnershipState::Transferring,
        updated_at: instance.created_at,
        ..effects.source.clone()
    };
    let created=sqlx::query("INSERT INTO continuity_executions(continuity_id,tenant_id,epoch,owner_runtime_id,state,record,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING")
  .bind(pending.continuity_id.to_string()).bind(instance.tenant_id.as_str()).bind(i64::try_from(pending.epoch.get()).map_err(|_|invalid())?).bind(pending.owner_runtime_id.to_string()).bind(state_name(pending.state)?).bind(encode(&pending)?).bind(pending.updated_at.to_rfc3339()).execute(&mut *conn).await?.rows_affected()==1;
    let row = sqlx::query(
        "SELECT record FROM continuity_executions WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(instance.tenant_id.as_str())
    .bind(pending.continuity_id.to_string())
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(invalid)?;
    let owner: ContinuityExecution = decode(&row.get::<String, _>("record"))?;
    if !effects.accepts_owner(&owner, instance.id) {
        return Err(invalid());
    }
    let other_owner=sqlx::query("SELECT 1 FROM continuity_executions WHERE tenant_id=?1 AND json_extract(record,'$.current_instance_id')=?2 AND continuity_id<>?3 LIMIT 1")
        .bind(instance.tenant_id.as_str()).bind(instance.id.to_string()).bind(pending.continuity_id.to_string()).fetch_optional(&mut *conn).await?;
    if other_owner.is_some() {
        return Err(invalid());
    }

    if created {
        let location = ContinuityLocation {
            tenant_id: pending.tenant_id.clone(),
            continuity_id: pending.continuity_id,
            epoch: pending.epoch,
            runtime_id: pending.owner_runtime_id,
            instance_id: pending.current_instance_id,
            handoff_id: None,
            entered_at: pending.updated_at,
        };
        sqlx::query("INSERT INTO continuity_locations(tenant_id,continuity_id,epoch,runtime_id,instance_id,handoff_id,record,entered_at) VALUES(?1,?2,?3,?4,?5,NULL,?6,?7)")
   .bind(instance.tenant_id.as_str()).bind(pending.continuity_id.to_string()).bind(i64::try_from(pending.epoch.get()).map_err(|_|invalid())?).bind(pending.owner_runtime_id.to_string()).bind(instance.id.to_string()).bind(encode(&location)?).bind(pending.updated_at.to_rfc3339()).execute(&mut *conn).await?;
    }
    for incoming in &effects.state.receipts {
        let row = sqlx::query("SELECT record FROM effect_receipts WHERE id=?1")
            .bind(incoming.id.to_string())
            .fetch_optional(&mut *conn)
            .await?;
        if let Some(row) = row {
            let existing: EffectReceipt = decode(&row.get::<String, _>("record"))?;
            let merged = crate::portable_effects::merge_receipt(&existing, incoming)?;
            if merged != existing {
                sqlx::query("UPDATE effect_receipts SET state=?1,record=?2,updated_at=?3 WHERE tenant_id=?4 AND id=?5").bind(state_name(merged.state)?).bind(encode(&merged)?).bind(merged.updated_at.to_rfc3339()).bind(instance.tenant_id.as_str()).bind(merged.id.to_string()).execute(&mut *conn).await?;
            }
        } else {
            sqlx::query("INSERT INTO effect_receipts(id,continuity_id,tenant_id,instance_id,state,record,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)")
    .bind(incoming.id.to_string()).bind(incoming.continuity_id.to_string()).bind(incoming.tenant_id.as_str()).bind(incoming.instance_id.to_string()).bind(state_name(incoming.state)?).bind(encode(incoming)?).bind(incoming.created_at.to_rfc3339()).bind(incoming.updated_at.to_rfc3339()).execute(&mut *conn).await?;
        }
    }
    let epoch = i64::try_from(
        effects
            .source
            .epoch
            .checked_next()
            .map_err(|_| invalid())?
            .get(),
    )
    .map_err(|_| invalid())?;
    for binding in &effects.state.active_attempts {
        sqlx::query("INSERT INTO effect_attempt_bindings(tenant_id,continuity_id,epoch,instance_id,block_id,attempt,effect_id) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING")
   .bind(instance.tenant_id.as_str()).bind(pending.continuity_id.to_string()).bind(epoch).bind(instance.id.to_string()).bind(binding.block_id.as_str()).bind(i64::from(binding.attempt)).bind(binding.effect_id.to_string()).execute(&mut *conn).await?;
        let row=sqlx::query("SELECT effect_id FROM effect_attempt_bindings WHERE tenant_id=?1 AND continuity_id=?2 AND epoch=?3 AND instance_id=?4 AND block_id=?5 AND attempt=?6").bind(instance.tenant_id.as_str()).bind(pending.continuity_id.to_string()).bind(epoch).bind(instance.id.to_string()).bind(binding.block_id.as_str()).bind(i64::from(binding.attempt)).fetch_one(&mut *conn).await?;
        if EffectId::from_uuid(
            row.get::<String, _>("effect_id")
                .parse::<uuid::Uuid>()
                .map_err(|e| StorageError::Query(e.to_string()))?,
        ) != binding.effect_id
        {
            return Err(invalid());
        }
    }
    for block in &effects.state.reset_blocks {
        sqlx::query("INSERT INTO effect_block_resets(tenant_id,instance_id,block_id) VALUES(?1,?2,?3) ON CONFLICT DO NOTHING").bind(instance.tenant_id.as_str()).bind(instance.id.to_string()).bind(block.as_str()).execute(&mut *conn).await?;
    }
    for guard in &effects.state.commit_guards {
        freeze_guard(conn, &pending, guard).await?;
    }
    let manifest = &effects.manifest;
    sqlx::query("INSERT INTO execution_capsules(id,continuity_id,tenant_id,expires_at,manifest) VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING")
        .bind(manifest.capsule_id.to_string()).bind(manifest.continuity_id.to_string()).bind(manifest.tenant_id.as_str()).bind(manifest.expires_at.to_rfc3339()).bind(encode(manifest)?).execute(&mut *conn).await?;
    let row = sqlx::query("SELECT manifest FROM execution_capsules WHERE id=?1")
        .bind(capsule.to_string())
        .fetch_one(&mut *conn)
        .await?;
    let existing: orch8_types::continuity::CapsuleManifest =
        decode(&row.get::<String, _>("manifest"))?;
    if &existing != manifest {
        return Err(invalid());
    }
    sqlx::query("INSERT INTO capsule_effect_imports(tenant_id,capsule_id,destination_runtime_id,instance_id,continuity_id,manifest_sha256) VALUES(?1,?2,?3,?4,?5,?6)")
  .bind(instance.tenant_id.as_str()).bind(capsule.to_string()).bind(runtime.to_string()).bind(instance.id.to_string()).bind(pending.continuity_id.to_string()).bind(&effects.manifest_sha256).execute(&mut *conn).await?;
    Ok(())
}

async fn freeze_guard(
    conn: &mut sqlx::SqliteConnection,
    owner: &ContinuityExecution,
    guard: &WorkflowInvariant,
) -> Result<(), StorageError> {
    let mut stub = guard.clone();
    stub.enabled = false;
    sqlx::query("INSERT INTO workflow_invariants(id,tenant_id,sequence_id,sequence_version,enabled,record,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT DO NOTHING")
  .bind(stub.id.to_string()).bind(stub.tenant_id.as_str()).bind(stub.sequence_id.to_string()).bind(stub.sequence_version).bind(false).bind(encode(&stub)?).bind(stub.created_at.to_rfc3339()).execute(&mut *conn).await?;
    let row = sqlx::query("SELECT record FROM workflow_invariants WHERE id=?1")
        .bind(guard.id.to_string())
        .fetch_one(&mut *conn)
        .await?;
    let mut existing: WorkflowInvariant = decode(&row.get::<String, _>("record"))?;
    existing.enabled = false;
    if existing != stub {
        return Err(invalid());
    }
    sqlx::query("INSERT INTO continuity_effect_guards(tenant_id,continuity_id,invariant_id,record) VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING")
  .bind(owner.tenant_id.as_str()).bind(owner.continuity_id.to_string()).bind(guard.id.to_string()).bind(encode(guard)?).execute(&mut *conn).await?;
    let row=sqlx::query("SELECT record FROM continuity_effect_guards WHERE tenant_id=?1 AND continuity_id=?2 AND invariant_id=?3").bind(owner.tenant_id.as_str()).bind(owner.continuity_id.to_string()).bind(guard.id.to_string()).fetch_one(&mut *conn).await?;
    let existing: WorkflowInvariant = decode(&row.get::<String, _>("record"))?;
    if &existing != guard {
        return Err(invalid());
    }
    Ok(())
}
