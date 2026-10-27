//! Bounded readers on the same transaction connection as the source fence.
use super::continuity::decode;
use crate::capsule_export::{CapsuleCutRefusal, CapsuleSourceSnapshot, CaptureBudget, too_large};
use orch8_types::{
    continuity::{CapsuleEffectState, CapsuleExecutionState, ContinuityExecution, EffectReceipt},
    error::StorageError,
    instance::InstanceState,
};
use sqlx::Row;

pub(super) enum Read {
    Ready(Box<CapsuleSourceSnapshot>),
    Refused(CapsuleCutRefusal),
}
pub(super) async fn bounded(
    conn: &mut sqlx::PgConnection,
    owner: &ContinuityExecution,
) -> Result<bool, StorageError> {
    let Some(mut total) = metadata_budget(conn, owner).await? else {
        return Ok(false);
    };
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(checkpoint_data::text)),0) AS bytes FROM checkpoints WHERE id=(SELECT id FROM checkpoints WHERE instance_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1)").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 1 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(output::text)+coalesce(octet_length(output_ref),0)+octet_length(block_id)+128),0) AS bytes FROM block_outputs WHERE instance_id=$1").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(block_id)+1024),0) AS bytes FROM execution_tree WHERE instance_id=$1").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(coalesce(octet_length(payload::text),0)+coalesce(octet_length(payload_bytes),0)+octet_length(ref_key)+256),0) AS bytes FROM externalized_state WHERE instance_id=$1").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(record::text)),0) AS bytes FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2").bind(owner.tenant_id.as_str()).bind(owner.continuity_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(block_id)+128),0) AS bytes FROM effect_attempt_bindings WHERE tenant_id=$1 AND instance_id=$2 AND continuity_id=$3 AND epoch=$4").bind(owner.tenant_id.as_str()).bind(owner.current_instance_id.into_uuid()).bind(owner.continuity_id.into_uuid()).bind(i64::try_from(owner.epoch.get()).map_err(|_| crate::capsule_export::invalid())?).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(block_id)+128),0) AS bytes FROM effect_block_resets WHERE tenant_id=$1 AND instance_id=$2").bind(owner.tenant_id.as_str()).bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(record::text)),0) AS bytes FROM workflow_invariants WHERE tenant_id=$1 AND sequence_id=(SELECT sequence_id FROM task_instances WHERE id=$2) AND enabled=TRUE AND (sequence_version IS NULL OR sequence_version=(SELECT version FROM sequences WHERE id=(SELECT sequence_id FROM task_instances WHERE id=$2))) AND record->>'commit_guard'='true' AND record->'rule'->>'type'='effect_at_most_once'").bind(owner.tenant_id.as_str()).bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(octet_length(record::text)),0) AS bytes FROM continuity_effect_guards WHERE tenant_id=$1 AND continuity_id=$2").bind(owner.tenant_id.as_str()).bind(owner.continuity_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 10000 {
        return Ok(false);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(false);
    }
    Ok(true)
}

pub(super) async fn read(
    conn: &mut sqlx::PgConnection,
    owner: &ContinuityExecution,
) -> Result<Read, StorageError> {
    if !bounded(conn, owner).await? {
        return Ok(Read::Refused(CapsuleCutRefusal::PayloadTooLarge));
    }
    let Some(instance) = super::instances::get_on(conn, owner.current_instance_id).await? else {
        return Ok(Read::Refused(CapsuleCutRefusal::Ownership));
    };
    if instance.tenant_id != owner.tenant_id {
        return Ok(Read::Refused(CapsuleCutRefusal::Ownership));
    }
    if !matches!(
        instance.state,
        InstanceState::Paused | InstanceState::Waiting
    ) {
        return Ok(Read::Refused(CapsuleCutRefusal::UnsafeBoundary));
    }
    let workers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM worker_tasks WHERE instance_id=$1 AND state IN ('pending','claimed')",
    )
    .bind(owner.current_instance_id.into_uuid())
    .fetch_one(&mut *conn)
    .await?;
    if workers > 0 {
        return Ok(Read::Refused(CapsuleCutRefusal::WorkerTasksInFlight(
            usize::try_from(workers).map_err(|_| too_large())?,
        )));
    }
    let Some(checkpoint) = super::checkpoints::get_latest_on(conn, instance.id).await? else {
        return Ok(Read::Refused(CapsuleCutRefusal::MissingCheckpoint));
    };
    let Some(sequence) = sqlx::query_as::<_,super::rows::SequenceRow>("SELECT id,tenant_id,namespace,name,definition,version,deprecated,status,created_at FROM sequences WHERE id=$1").bind(instance.sequence_id.into_uuid()).fetch_optional(&mut *conn).await? else { return Ok(Read::Refused(CapsuleCutRefusal::MissingSequence)); };
    let sequence = sequence.into_definition()?;
    let outputs = super::outputs::get_all_on(conn, instance.id).await?;
    let nodes = super::execution_tree::get_tree_on(conn, instance.id).await?;
    let rows=sqlx::query("SELECT id,instance_id,state,record FROM effect_receipts WHERE tenant_id=$1 AND continuity_id=$2 ORDER BY created_at,id LIMIT 10001").bind(owner.tenant_id.as_str()).bind(owner.continuity_id.into_uuid()).fetch_all(&mut *conn).await?;
    let mut receipts = Vec::with_capacity(rows.len());
    for row in rows {
        let receipt: EffectReceipt = decode(row.get("record"))?;
        if receipt.id.into_uuid() != row.get::<uuid::Uuid, _>("id")
            || receipt.instance_id.into_uuid() != row.get::<uuid::Uuid, _>("instance_id")
            || super::continuity::state_name(receipt.state)? != row.get::<String, _>("state")
        {
            return Ok(Read::Refused(CapsuleCutRefusal::Ownership));
        }
        receipts.push(receipt);
    }
    let (active_attempts, reset_blocks) = super::portable_effects::attempts_on(conn, owner).await?;
    let commit_guards = super::portable_effects::guards_on(
        conn,
        &owner.tenant_id,
        owner.continuity_id,
        sequence.id,
        sequence.version,
        None,
    )
    .await?;
    let effects = CapsuleEffectState {
        receipts,
        active_attempts,
        reset_blocks,
        commit_guards,
    };
    let mut budget = CaptureBudget::new();
    for size in [
        budget.add(&instance),
        budget.add(&sequence),
        budget.add(&checkpoint),
        budget.add(&outputs),
        budget.add(&nodes),
        budget.add(&effects),
    ] {
        if size.is_err() {
            return Ok(Read::Refused(CapsuleCutRefusal::PayloadTooLarge));
        }
    }
    let Some(payloads) = externalized_on(conn, instance.id, &mut budget).await? else {
        return Ok(Read::Refused(CapsuleCutRefusal::PayloadTooLarge));
    };
    let head=sqlx::query("SELECT head.entry_sha256 FROM provenance_entries head WHERE head.tenant_id=$1 AND head.continuity_id=$2 AND NOT EXISTS (SELECT 1 FROM provenance_entries child WHERE child.tenant_id=head.tenant_id AND child.continuity_id=head.continuity_id AND child.previous_sha256=head.entry_sha256) LIMIT 1").bind(owner.tenant_id.as_str()).bind(owner.continuity_id.into_uuid()).fetch_optional(&mut *conn).await?;
    let provenance_head = head.map(|row| row.get("entry_sha256"));
    let snapshot = CapsuleSourceSnapshot {
        instance,
        sequence,
        checkpoint,
        outputs,
        execution_state: CapsuleExecutionState { nodes, payloads },
        effects,
        provenance_head,
    };
    if let Err(reason) = snapshot.validate(owner) {
        return Ok(Read::Refused(reason));
    }
    Ok(Read::Ready(Box::new(snapshot)))
}

async fn metadata_budget(
    conn: &mut sqlx::PgConnection,
    owner: &ContinuityExecution,
) -> Result<Option<i64>, StorageError> {
    let mut total = 0_i64;
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(coalesce(octet_length(context::text),0)+coalesce(octet_length(metadata::text),0)+coalesce(octet_length(budget::text),0)+coalesce(octet_length(timezone),0)+coalesce(octet_length(namespace),0)),0) AS bytes FROM task_instances WHERE id=$1").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 1 {
        return Ok(None);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(None);
    }
    let row=sqlx::query("SELECT COUNT(*) AS n,COALESCE(SUM(coalesce(octet_length(definition::text),0)),0) AS bytes FROM sequences WHERE id=(SELECT sequence_id FROM task_instances WHERE id=$1)").bind(owner.current_instance_id.into_uuid()).fetch_one(&mut *conn).await?;
    if row.get::<i64, _>("n") > 1 {
        return Ok(None);
    }
    total = total
        .checked_add(row.get::<i64, _>("bytes"))
        .ok_or_else(too_large)?;
    if total > CapsuleSourceSnapshot::MAX_CAPTURE_BYTES as i64 {
        return Ok(None);
    }
    Ok(Some(total))
}

async fn externalized_on(
    conn: &mut sqlx::PgConnection,
    instance: orch8_types::ids::InstanceId,
    budget: &mut CaptureBudget,
) -> Result<Option<Vec<crate::externalizing::ExternalizedEntry>>, StorageError> {
    let mut payloads = Vec::new();
    {
        use futures::TryStreamExt;
        let mut rows=sqlx::query("SELECT ref_key,payload,payload_bytes,compression,created_at,expires_at FROM externalized_state WHERE instance_id=$1 ORDER BY ref_key LIMIT 10001").bind(instance.into_uuid()).fetch(&mut *conn);
        while let Some(row) = rows.try_next().await? {
            let codec: Option<String> = row.try_get("compression")?;
            let payload = crate::externalizing::decode_payload(
                row.try_get("payload")?,
                row.try_get("payload_bytes")?,
                codec.as_deref(),
            )?;
            if budget.add(&payload).is_err() {
                return Ok(None);
            }
            payloads.push(crate::externalizing::ExternalizedEntry {
                instance_id: instance,
                ref_key: row.try_get("ref_key")?,
                payload,
                created_at: row.try_get("created_at")?,
                expires_at: row.try_get("expires_at")?,
            });
        }
    }
    Ok(Some(payloads))
}
