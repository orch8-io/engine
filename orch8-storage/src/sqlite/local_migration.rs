//! Local recovery snapshots retain native authority and all local suspension rows.
use super::{SqliteStorage, capsule_source_snapshot::Read, continuity::decode};
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
    store: &SqliteStorage,
    request: &LocalMigrationSnapshotRequest,
) -> Result<Capture, StorageError> {
    if request.execution.state != OwnershipState::Owned {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
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
    conn: &mut sqlx::SqliteConnection,
    request: &LocalMigrationSnapshotRequest,
) -> Result<Capture, StorageError> {
    if !super::mobile_access::permits_capsule(
        conn,
        request.mobile_access.as_ref(),
        request.execution.current_instance_id,
        &request.execution.tenant_id,
    )
    .await?
    {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let current = super::capsule_export::owner(conn, &request.execution).await?;
    if current.as_ref() != Some(&request.execution) {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let instance: Option<String> = sqlx::query_scalar(
        "SELECT id FROM task_instances WHERE id=?1 AND tenant_id=?2 AND updated_at=?3",
    )
    .bind(request.execution.current_instance_id.to_string())
    .bind(request.execution.tenant_id.as_str())
    .bind(request.expected_instance_updated_at.to_rfc3339())
    .fetch_optional(&mut *conn)
    .await?;
    if instance.is_none() {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=?1)
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=?1 AND state IN ('captured','sealed'))
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE tenant_id=?2 AND continuity_id=?3 AND state IN ('captured','sealed') AND state='captured')")
        .bind(request.execution.current_instance_id.to_string()).bind(request.execution.tenant_id.as_str()).bind(request.execution.continuity_id.to_string()).fetch_one(&mut *conn).await?;
    if blocked {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    }
    let row = sqlx::query("SELECT record,state FROM live_migrations WHERE tenant_id=?1 AND id=?2")
        .bind(request.execution.tenant_id.as_str())
        .bind(request.migration_id.to_string())
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(Capture::Refused(CapsuleCutRefusal::Ownership));
    };
    let record: LiveMigrationRecord = decode(&row.get::<String, _>("record"))?;
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
    sqlx::query("INSERT OR IGNORE INTO capsule_artifact_retirements(artifact_key,tenant_id,capsule_id,next_sweep_at) VALUES(?1,?2,?3,?4)")
        .bind(&artifact_key).bind(record.plan.tenant_id.as_str()).bind(record.plan.id.to_string()).bind(record.rollback_expires_at.to_rfc3339()).execute(&mut *conn).await?;
    let scoped:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM capsule_artifact_retirements WHERE artifact_key=?1 AND tenant_id=?2 AND capsule_id=?3)")
        .bind(&artifact_key).bind(record.plan.tenant_id.as_str()).bind(record.plan.id.to_string()).fetch_one(&mut *conn).await?;
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

pub(super) async fn lock_transition(
    conn: &mut sqlx::SqliteConnection,
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
    let native:Option<String>=sqlx::query_scalar("SELECT id FROM task_instances WHERE id=?1 AND tenant_id=?2 AND state=?3 AND sequence_id=?4 AND updated_at=?5")
        .bind(t.expected_execution.current_instance_id.to_string()).bind(t.tenant_id.as_str()).bind(super::continuity::state_name(t.expected_instance_state)?)
        .bind(sequence.to_string()).bind(t.expected_instance_updated_at.to_rfc3339()).fetch_optional(&mut *conn).await?;
    if native.is_none() {
        return Ok(false);
    }
    let row = sqlx::query("SELECT * FROM sequences WHERE id=?1 AND tenant_id=?2")
        .bind(t.next_sequence_id.to_string())
        .bind(t.tenant_id.as_str())
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let target = super::helpers::row_to_sequence(&row)?;
    if serde_json::to_value(&target)? != serde_json::to_value(t.expected_target_sequence)? {
        return Ok(false);
    }
    let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=?1)
        OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=?1 AND state IN ('captured','sealed'))
        OR EXISTS(SELECT 1 FROM worker_tasks WHERE instance_id=?1 AND state IN ('pending','claimed'))")
        .bind(t.expected_execution.current_instance_id.to_string()).fetch_one(&mut *conn).await?;
    if blocked {
        return Ok(false);
    }
    let checkpoint:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM checkpoints WHERE instance_id=?1 AND id=?2 AND (?3 OR created_at=?4)
        AND id=(SELECT id FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC,id DESC LIMIT 1))")
        .bind(t.expected_execution.current_instance_id.to_string()).bind(t.expected_checkpoint_id.to_string())
        .bind(t.forbid_effects_epoch.is_none()).bind(t.expected_instance_updated_at.to_rfc3339()).fetch_one(&mut *conn).await?;
    if !checkpoint {
        return Ok(false);
    }
    let row = sqlx::query("SELECT record,state FROM live_migrations WHERE tenant_id=?1 AND id=?2")
        .bind(t.tenant_id.as_str())
        .bind(t.expected_record.plan.id.to_string())
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let current: LiveMigrationRecord = decode(&row.get::<String, _>("record"))?;
    if super::continuity::state_name(current.state)? != row.get::<String, _>("state")
        || !crate::local_migration::same_record_metadata(&current, t.expected_record)
    {
        return Ok(false);
    }
    if t.forbid_effects_epoch.is_none() {
        let uncertain:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2 AND state IN ('dispatched','unknown'))")
            .bind(t.tenant_id.as_str()).bind(t.expected_execution.continuity_id.to_string()).fetch_one(&mut *conn).await?;
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
