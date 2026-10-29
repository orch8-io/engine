//! A local recovery read never changes continuity or physical execution authority.
use super::{PostgresStorage, capsule_source_snapshot::Read, continuity::decode};
use crate::{
    capsule_export::CapsuleCutRefusal,
    local_migration::{
        LocalMigrationSnapshot, LocalMigrationSnapshotCapture as Capture,
        LocalMigrationSnapshotRequest, valid_record,
    },
};
use orch8_types::{
    continuity::OwnershipState, continuity_advanced::LiveMigrationRecord, error::StorageError,
};
use sqlx::Row;

pub(super) async fn prepare(
    store: &PostgresStorage,
    request: &LocalMigrationSnapshotRequest,
) -> Result<Capture, StorageError> {
    if request.execution.state != OwnershipState::Owned || request.mobile_access.is_some() {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let mut tx = store.pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let outcome = prepare_on(&mut tx, request).await;
    match outcome {
        Ok(Capture::Ready(snapshot)) => {
            tx.commit().await?;
            Ok(Capture::Ready(snapshot))
        }
        Ok(Capture::Refused(reason)) => {
            tx.rollback().await?;
            Ok(Capture::Refused(reason))
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

async fn prepare_on(
    conn: &mut sqlx::PgConnection,
    request: &LocalMigrationSnapshotRequest,
) -> Result<Capture, StorageError> {
    // Keep transfer/compensation ordering: continuity, native instance, plan.
    let current = super::capsule_export::owner(conn, &request.execution).await?;
    if current.as_ref() != Some(&request.execution) {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let instance: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT id FROM task_instances WHERE id=$1 AND tenant_id=$2 AND updated_at=$3 FOR UPDATE",
    )
    .bind(request.execution.current_instance_id.into_uuid())
    .bind(request.execution.tenant_id.as_str())
    .bind(request.expected_instance_updated_at)
    .fetch_optional(&mut *conn)
    .await?;
    if instance.is_none() {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=$1)
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=$1 AND state IN ('captured','sealed'))
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE tenant_id=$2 AND continuity_id=$3 AND state IN ('captured','sealed') AND state='captured')")
        .bind(request.execution.current_instance_id.into_uuid()).bind(request.execution.tenant_id.as_str()).bind(request.execution.continuity_id.into_uuid()).fetch_one(&mut *conn).await?;
    if blocked {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let row = sqlx::query(
        "SELECT record,state FROM live_migrations WHERE tenant_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(request.execution.tenant_id.as_str())
    .bind(request.migration_id.into_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    };
    let record: LiveMigrationRecord = decode(row.get("record"))?;
    if !valid_record(&record, request)
        || super::continuity::state_name(record.state)? != row.get::<String, _>("state")
    {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let source = match super::capsule_source_snapshot::read(conn, &request.execution).await? {
        Read::Ready(s) => s,
        Read::Refused(r) => return Ok(Capture::Refused(r)),
    };
    let planned = record.state == orch8_types::continuity_advanced::LiveMigrationState::Planned;
    let (sequence, version) = if planned {
        (record.plan.from_sequence_id, record.plan.from_version)
    } else {
        (record.plan.to_sequence_id, record.plan.to_version)
    };
    if source.instance.sequence_id != sequence
        || source.sequence.version != version
        || (planned
            && (source.instance.state != record.source_state
                || source.checkpoint.id != record.source_checkpoint.id))
        || (!planned && source.instance.state != orch8_types::instance::InstanceState::Paused)
    {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let fingerprint = crate::local_migration::fingerprint(&record, &source)?;
    let artifact_key = format!("{}/{}", source.instance.id, record.plan.id);
    sqlx::query("INSERT INTO capsule_artifact_retirements(artifact_key,tenant_id,capsule_id,next_sweep_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING")
        .bind(&artifact_key).bind(record.plan.tenant_id.as_str()).bind(record.plan.id.into_uuid()).bind(record.rollback_expires_at).execute(&mut *conn).await?;
    let scoped:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capsule_artifact_retirements WHERE artifact_key=$1 AND tenant_id=$2 AND capsule_id=$3)")
        .bind(&artifact_key).bind(record.plan.tenant_id.as_str()).bind(record.plan.id.into_uuid()).fetch_one(&mut *conn).await?;
    if !scoped {
        return Err(StorageError::Constraint(
            "local recovery artifact cleanup identity conflict".into(),
        ));
    }
    Ok(Capture::Ready(Box::new(LocalMigrationSnapshot {
        record,
        source: *source,
        artifact_key,
        fingerprint,
    })))
}

/// Revalidate under READ COMMITTED after parent waits. A migration must never
/// overwrite a newer context just because its state label is still `paused`.
pub(super) async fn lock_transition(
    conn: &mut sqlx::PgConnection,
    t: &crate::LiveMigrationTransition<'_>,
) -> Result<bool, StorageError> {
    let owner = super::capsule_export::owner(conn, t.expected_execution).await?;
    if owner.as_ref() != Some(t.expected_execution) {
        return Ok(false);
    }
    let sequence = if t.expected_record.state
        == orch8_types::continuity_advanced::LiveMigrationState::Planned
    {
        t.expected_record.plan.from_sequence_id
    } else {
        t.expected_record.plan.to_sequence_id
    };
    let native:Option<uuid::Uuid>=sqlx::query_scalar("SELECT id FROM task_instances WHERE id=$1 AND tenant_id=$2 AND state=$3 AND sequence_id=$4 AND updated_at=$5 FOR UPDATE")
        .bind(t.expected_execution.current_instance_id.into_uuid()).bind(t.tenant_id.as_str()).bind(super::continuity::state_name(t.expected_instance_state)?)
        .bind(sequence.into_uuid()).bind(t.expected_instance_updated_at).fetch_optional(&mut *conn).await?;
    if native.is_none() {
        return Ok(false);
    }
    let target=sqlx::query_as::<_,super::rows::SequenceRow>("SELECT id,tenant_id,namespace,name,definition,version,deprecated,status,created_at FROM sequences WHERE id=$1 AND tenant_id=$2 FOR SHARE")
        .bind(t.next_sequence_id.into_uuid()).bind(t.tenant_id.as_str()).fetch_optional(&mut *conn).await?;
    let Some(target) = target else {
        return Ok(false);
    };
    let target = target.into_definition()?;
    if serde_json::to_value(&target)? != serde_json::to_value(t.expected_target_sequence)? {
        return Ok(false);
    }
    let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=$1)
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=$1 AND state IN ('captured','sealed'))
        OR EXISTS(SELECT 1 FROM worker_tasks WHERE instance_id=$1 AND state IN ('pending','claimed'))")
        .bind(t.expected_execution.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if blocked {
        return Ok(false);
    }
    let checkpoint:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM checkpoints WHERE instance_id=$1 AND id=$2 AND ($3 OR created_at=$4)
        AND id=(SELECT id FROM checkpoints WHERE instance_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1))")
        .bind(t.expected_execution.current_instance_id.into_uuid()).bind(t.expected_checkpoint_id)
        .bind(t.forbid_effects_epoch.is_none()).bind(t.expected_instance_updated_at).fetch_one(&mut *conn).await?;
    if !checkpoint {
        return Ok(false);
    }
    let row = sqlx::query(
        "SELECT record,state FROM live_migrations WHERE tenant_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(t.tenant_id.as_str())
    .bind(t.expected_record.plan.id.into_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let current: LiveMigrationRecord = decode(row.get("record"))?;
    if super::continuity::state_name(current.state)? != row.get::<String, _>("state")
        || !crate::local_migration::same_record_metadata(&current, t.expected_record)
    {
        return Ok(false);
    }
    if !super::capsule_source_snapshot::bounded(conn, t.expected_execution).await? {
        return Ok(false);
    }
    lock_recovery_rows(conn, t).await?;
    // Inserts reference the locked continuity parent. Lock unresolved rows too,
    // so their dispatch cannot race the final effect check after a parent wait.
    sqlx::query("SELECT id FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2 AND state IN ('planned','prepared') FOR UPDATE")
        .bind(t.tenant_id.as_str()).bind(t.expected_execution.continuity_id.into_uuid()).execute(&mut *conn).await?;
    if t.forbid_effects_epoch.is_none() {
        let uncertain:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2 AND state IN ('dispatched','unknown'))")
            .bind(t.tenant_id.as_str()).bind(t.expected_execution.continuity_id.into_uuid()).fetch_one(&mut *conn).await?;
        if uncertain {
            return Ok(false);
        }
    }
    let source = match super::capsule_source_snapshot::read(conn, t.expected_execution).await? {
        Read::Ready(source) => source,
        Read::Refused(_) => return Ok(false),
    };
    // Artifact upload and parent/child lock waits may outlive the recovery window.
    if current.rollback_expires_at <= chrono::Utc::now()
        || crate::local_migration::fingerprint(&current, &source)? != t.expected_source_fingerprint
    {
        return Ok(false);
    }
    Ok(true)
}

async fn lock_recovery_rows(
    conn: &mut sqlx::PgConnection,
    t: &crate::LiveMigrationTransition<'_>,
) -> Result<(), StorageError> {
    let id = t.expected_execution.current_instance_id.into_uuid();
    for query in [
        "SELECT id FROM execution_tree WHERE instance_id=$1 FOR UPDATE",
        "SELECT id FROM block_outputs WHERE instance_id=$1 FOR UPDATE",
        "SELECT ref_key FROM externalized_state WHERE instance_id=$1 FOR UPDATE",
    ] {
        sqlx::query(query).bind(id).execute(&mut *conn).await?;
    }
    sqlx::query("SELECT id FROM checkpoints WHERE id=$1 FOR UPDATE")
        .bind(t.expected_checkpoint_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "SELECT id FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2 FOR UPDATE",
    )
    .bind(t.tenant_id.as_str())
    .bind(t.expected_execution.continuity_id.into_uuid())
    .execute(&mut *conn)
    .await?;
    Ok(())
}
