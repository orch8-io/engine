//! Establish a binding for a resumable dispatch written by a pre-epoch server.
use crate::{
    worker_admission::legacy_identity,
    worker_invocation::{WorkerInvocation, WorkerInvocationTarget},
    worker_resolution::ExpiredLeaseObservation,
};
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{ContinuityExecution, EffectKind, EffectReceipt, EffectState, OwnershipState},
    error::StorageError,
    ids::{BlockId, InstanceId, TenantId},
};
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

/// The caller holds the native parent, owner, and hot task locks. The old
/// receipt is locked before any change, so either its original report or the
/// adoption wins. An unverifiable checkpoint leaves the old row untouched.
#[allow(clippy::too_many_lines)] // The lease, invocation, and receipt are bound in one transaction.
pub(super) async fn adopt_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
    lease: &ExpiredLeaseObservation,
) -> Result<Option<WorkerInvocation>, StorageError> {
    let Ok(checkpoint_seq) = i64::try_from(lease.checkpoint_seq) else {
        return Ok(None);
    };
    let Some(row) = sqlx::query(
        "SELECT t.instance_id,t.block_id,t.attempt,t.created_at,t.handler_name,t.params,
                i.tenant_id,i.context->'runtime'->>'current_step_started_at' AS started_at,
                i.context->'runtime'->>'run_id' AS run_id
         FROM worker_tasks t JOIN task_instances i ON t.instance_id=i.id
         WHERE t.id=$1 AND t.state='claimed' AND t.claim_epoch=0 AND t.worker_id=$2
           AND t.claimed_at IS NOT DISTINCT FROM $3 AND t.heartbeat_at IS NOT DISTINCT FROM $4
           AND COALESCE(t.heartbeat_at,t.claimed_at) <
               NOW()-make_interval(secs => COALESCE(t.lease_secs::double precision,$6))
           AND t.checkpoint_seq=$5 AND t.checkpoint_seq>0 AND t.resume_checkpoint IS NOT NULL
           AND (t.tenant_id IS NULL OR t.tenant_id=i.tenant_id)
           AND t.effect_id IS NULL AND t.continuity_epoch IS NULL AND NOT t.awaiting_dispatch
           AND t.heartbeat_timeout_secs IS NULL AND NOT t.wait_for_callback
           AND t.params_encoding IS NULL AND t.context_encoding IS NULL
           AND i.state='waiting' AND i.context->'runtime'->>'current_step'=t.block_id
           AND COALESCE(i.context->'runtime'->>'dry_run','false')='false'
           AND t.context->'runtime'->>'instance_id'=t.instance_id::text
           AND t.context->'runtime'->>'run_id'=i.context->'runtime'->>'run_id'
           AND NOT EXISTS(SELECT 1 FROM execution_tree WHERE instance_id=i.id)
           AND NOT EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=i.id)
           AND NOT EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=i.id AND state IN ('captured','sealed'))
           AND NOT EXISTS(SELECT 1 FROM worker_task_invocations WHERE task_id=t.id)
           AND NOT EXISTS(SELECT 1 FROM worker_task_admissions WHERE task_id=t.id)
           AND NOT EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=t.id)",
    )
    .bind(task_id)
    .bind(lease.worker_id.as_deref())
    .bind(lease.claimed_at)
    .bind(lease.heartbeat_at)
    .bind(checkpoint_seq)
    .bind(lease.default_lease.as_secs_f64())
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let Some(started) = row
        .get::<Option<String>, _>("started_at")
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|time| time.with_timezone(&Utc))
    else {
        return Ok(None);
    };
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    let instance = InstanceId::from_uuid(row.get("instance_id"));
    let block = BlockId::new(row.get::<String, _>("block_id"));
    let created: DateTime<Utc> = row.get("created_at");
    let attempt = i64::from(row.get::<i16, _>("attempt"));
    let run_id: String = row.get("run_id");
    if created < started || run_id.is_empty() || run_id.len() > 128 {
        return Ok(None);
    }
    let owner: Option<Value> = sqlx::query_scalar(
        "SELECT record FROM continuity_executions WHERE tenant_id=$1 AND record->>'current_instance_id'=$2",
    )
    .bind(tenant.as_str())
    .bind(instance.to_string())
    .fetch_optional(&mut *conn)
    .await?;
    let Some(owner) = owner
        .map(serde_json::from_value::<ContinuityExecution>)
        .transpose()?
    else {
        return Ok(None);
    };
    if owner.state != OwnershipState::Owned
        || owner.current_instance_id != instance
        || owner.tenant_id != tenant
    {
        return Ok(None);
    }
    let epoch = i64::try_from(owner.epoch.get())
        .map_err(|_| StorageError::Constraint("legacy worker owner epoch out of range".into()))?;
    let receipts = sqlx::query(
        "SELECT id,state,record FROM effect_receipts
         WHERE tenant_id=$1 AND instance_id=$2 AND continuity_id=$3
           AND record->>'block_id'=$4 AND record->>'attempt'=$5
           AND state IN ('dispatched','unknown') LIMIT 2 FOR UPDATE",
    )
    .bind(tenant.as_str())
    .bind(instance.into_uuid())
    .bind(owner.continuity_id.into_uuid())
    .bind(block.as_str())
    .bind(attempt.to_string())
    .fetch_all(&mut *conn)
    .await?;
    let [stored] = receipts.as_slice() else {
        return Ok(None);
    };
    let receipt: EffectReceipt = serde_json::from_value(stored.get("record"))?;
    let hash = crate::worker_outcome::digest(&json!({
        "handler": row.get::<String, _>("handler_name"),
        "params": row.get::<Value, _>("params"),
    }));
    if receipt.id != legacy_identity(&receipt)
        || receipt.id.into_uuid() != stored.get::<Uuid, _>("id")
        || receipt.tenant_id != tenant
        || receipt.instance_id != instance
        || receipt.continuity_id != owner.continuity_id
        || receipt.epoch != owner.epoch
        || receipt.block_id != block
        || i64::from(receipt.attempt) != attempt
        || receipt.kind != EffectKind::Worker
        || !matches!(
            receipt.state,
            EffectState::Dispatched | EffectState::Unknown
        )
        || super::continuity::state_name(receipt.state)? != stored.get::<String, _>("state")
        || receipt.request_sha256 != hash
        || receipt.created_at < started
        || receipt.created_at > created
    {
        return Ok(None);
    }

    let effect = receipt.id.into_uuid();
    let inserted = sqlx::query(
        "INSERT INTO effect_attempt_bindings(tenant_id,continuity_id,epoch,instance_id,block_id,attempt,effect_id)
         VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
    )
    .bind(tenant.as_str())
    .bind(owner.continuity_id.into_uuid())
    .bind(epoch)
    .bind(instance.into_uuid())
    .bind(block.as_str())
    .bind(attempt)
    .bind(effect)
    .execute(&mut *conn)
    .await?;
    if inserted.rows_affected() != 1 {
        return Ok(None);
    }
    // All failures from here must roll back the newly inserted binding.
    let changed = sqlx::query(
        "UPDATE worker_tasks SET continuity_epoch=$2,effect_id=$3,tenant_id=$4 WHERE id=$1",
    )
    .bind(task_id)
    .bind(epoch)
    .bind(effect)
    .bind(tenant.as_str())
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if changed != 1 {
        return Err(adoption_changed());
    }
    let record = super::worker_invocations::read_on(conn, task_id, None).await?;
    if record.target
        != (WorkerInvocationTarget::Flat {
            step_started_at: started,
        })
        || record.continuity.is_none()
        || record.run_id.as_deref() != Some(&run_id)
    {
        return Err(adoption_changed());
    }
    super::worker_invocations::insert_on(conn, &record).await?;
    sqlx::query("INSERT INTO legacy_worker_checkpoint_adoptions(task_id,effect_id) VALUES($1,$2)")
        .bind(task_id)
        .bind(effect)
        .execute(&mut *conn)
        .await?;
    Ok(Some(record))
}

pub(super) fn adoption_changed() -> StorageError {
    StorageError::Conflict("legacy worker checkpoint adoption changed during resolution".into())
}
