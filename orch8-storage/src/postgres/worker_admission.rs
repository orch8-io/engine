//! One transaction admits the effect, prepared payload, invocation and queue visibility.
use super::PostgresStorage;
use crate::worker_admission::{
    WorkerDispatchAdmission, WorkerDispatchIntent, WorkerDispatchPublication as Publication,
};
use orch8_types::{
    continuity::{EffectDispatchOutcome, EffectReceipt, EffectState},
    error::StorageError,
    ids::TenantId,
    worker::WorkerTask,
};
use sqlx::Row;
use uuid::Uuid;

fn invalid() -> StorageError {
    StorageError::Constraint("worker admission evidence mismatch".into())
}

pub(super) async fn create(
    store: &PostgresStorage,
    intent: &WorkerDispatchIntent,
) -> Result<Publication, StorageError> {
    i16::try_from(intent.task.attempt).map_err(|_| invalid())?;
    intent.validate_stored()?;
    let mut tx = store.pool.begin().await?;
    match publish_on(&mut tx, intent).await {
        Ok(result @ Publication::Published { .. }) => {
            tx.commit().await?;
            Ok(result)
        }
        Ok(result) => {
            tx.rollback().await?;
            Ok(result)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant: &TenantId,
    task_id: Uuid,
) -> Result<Option<WorkerDispatchAdmission>, StorageError> {
    let mut conn = store.pool.acquire().await?;
    get_on(&mut conn, tenant, task_id).await
}

pub(super) async fn get_on(
    conn: &mut sqlx::PgConnection,
    tenant: &TenantId,
    task_id: Uuid,
) -> Result<Option<WorkerDispatchAdmission>, StorageError> {
    let row=sqlx::query("SELECT invocation_key,instance_id,record FROM worker_task_admissions WHERE task_id=$1 AND tenant_id=$2")
        .bind(task_id).bind(tenant.as_str()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else { return Ok(None) };
    let admission: WorkerDispatchAdmission = serde_json::from_value(row.get("record"))?;
    admission.invocation.validate_scope(
        tenant,
        task_id,
        orch8_types::ids::InstanceId::from_uuid(row.get("instance_id")),
    )?;
    if admission.invocation_key != row.get::<String, _>("invocation_key")
        || admission.effect.as_ref().map(|effect| effect.effect_id)
            != admission.invocation.effect_id
    {
        return Err(invalid());
    }
    Ok(Some(admission))
}

async fn by_key_on(
    conn: &mut sqlx::PgConnection,
    intent: &WorkerDispatchIntent,
) -> Result<Option<WorkerDispatchAdmission>, StorageError> {
    let row = sqlx::query("SELECT task_id FROM worker_task_admissions WHERE invocation_key=$1")
        .bind(intent.invocation_key())
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else { return Ok(None) };
    let task_id = row.get::<Uuid, _>("task_id");
    get_on(conn, &intent.fence.tenant_id, task_id)
        .await?
        .map_or_else(|| Err(invalid()), |admission| Ok(Some(admission)))
}

async fn receipt_on(
    conn: &mut sqlx::PgConnection,
    admission: &WorkerDispatchAdmission,
) -> Result<Option<Box<EffectReceipt>>, StorageError> {
    let Some(id) = admission.invocation.effect_id else {
        return Ok(None);
    };
    let value: serde_json::Value =
        sqlx::query_scalar("SELECT record FROM effect_receipts WHERE tenant_id=$1 AND id=$2")
            .bind(admission.invocation.tenant_id.as_str())
            .bind(id.into_uuid())
            .fetch_one(&mut *conn)
            .await?;
    Ok(Some(Box::new(super::continuity::decode(value)?)))
}

/// Permit only canonical epoch-zero enrollment, never a later owner or historical namespace.
async fn initial_owner_on(
    conn: &mut sqlx::PgConnection,
    intent: &WorkerDispatchIntent,
) -> Result<bool, StorageError> {
    let Some(owner) = &intent.initial_owner else {
        return Ok(true);
    };
    ensure_initial_owner_on(conn, owner).await
}
pub(super) async fn ensure_initial_owner_on(
    conn: &mut sqlx::PgConnection,
    owner: &orch8_types::continuity::ContinuityExecution,
) -> Result<bool, StorageError> {
    use orch8_types::continuity::{ContinuityExecution, ContinuityLocation, OwnershipState};
    super::portable_effects::lock_instance_on(conn, &owner.tenant_id, owner.current_instance_id)
        .await?;
    let existing:Option<serde_json::Value>=sqlx::query_scalar("SELECT record FROM continuity_executions WHERE tenant_id=$1 AND record->>'current_instance_id'=$2 FOR UPDATE")
        .bind(owner.tenant_id.as_str()).bind(owner.current_instance_id.to_string()).fetch_optional(&mut *conn).await?;
    if let Some(value) = existing {
        let actual: ContinuityExecution = serde_json::from_value(value)?;
        return Ok(actual.continuity_id == owner.continuity_id
            && actual.tenant_id == owner.tenant_id
            && actual.current_instance_id == owner.current_instance_id
            && actual.owner_runtime_id == owner.owner_runtime_id
            && actual.epoch == owner.epoch
            && actual.state == OwnershipState::Owned);
    }
    let history: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuity_locations WHERE tenant_id=$1 AND instance_id=$2)",
    )
    .bind(owner.tenant_id.as_str())
    .bind(owner.current_instance_id.into_uuid())
    .fetch_one(&mut *conn)
    .await?;
    if history {
        return Ok(false);
    }
    super::portable_effects::check_namespace_on(conn, owner).await?;
    let inserted=sqlx::query("INSERT INTO continuity_executions(continuity_id,tenant_id,epoch,owner_runtime_id,state,record,updated_at) VALUES($1,$2,0,$3,'owned',$4,$5) ON CONFLICT DO NOTHING")
        .bind(owner.continuity_id.into_uuid()).bind(owner.tenant_id.as_str()).bind(owner.owner_runtime_id.into_uuid())
        .bind(super::continuity::encode(owner)?).bind(owner.updated_at).execute(&mut *conn).await?.rows_affected();
    if inserted != 1 {
        return Ok(false);
    }
    let location = ContinuityLocation {
        continuity_id: owner.continuity_id,
        tenant_id: owner.tenant_id.clone(),
        epoch: owner.epoch,
        runtime_id: owner.owner_runtime_id,
        instance_id: owner.current_instance_id,
        handoff_id: None,
        entered_at: owner.updated_at,
    };
    sqlx::query("INSERT INTO continuity_locations(tenant_id,continuity_id,epoch,runtime_id,instance_id,handoff_id,record,entered_at) VALUES($1,$2,0,$3,$4,NULL,$5,$6)")
        .bind(owner.tenant_id.as_str()).bind(owner.continuity_id.into_uuid()).bind(owner.owner_runtime_id.into_uuid()).bind(owner.current_instance_id.into_uuid())
        .bind(super::continuity::encode(&location)?).bind(owner.updated_at).execute(&mut *conn).await?;
    Ok(true)
}

async fn publish_on(
    conn: &mut sqlx::PgConnection,
    intent: &WorkerDispatchIntent,
) -> Result<Publication, StorageError> {
    let task = &intent.task;
    let fence = &intent.fence;
    if !initial_owner_on(conn, intent).await? {
        return Ok(Publication::Stale);
    }
    if !super::worker_dispatch::suspend_native_on(conn, task, fence).await? {
        return Ok(Publication::Stale);
    }
    if let Some(admission) = by_key_on(conn, intent).await? {
        if !fence.matches(&admission.invocation) || admission.invocation.attempt != task.attempt {
            return Err(invalid());
        }
        return Ok(Publication::Published {
            task_id: admission.invocation.task_id,
            receipt: receipt_on(conn, &admission).await?,
        });
    }
    let row=sqlx::query("SELECT id,state,attempt,awaiting_dispatch FROM worker_tasks WHERE instance_id=$1 AND block_id=$2 FOR UPDATE")
        .bind(intent.task.instance_id.into_uuid()).bind(task.block_id.as_str()).fetch_optional(&mut *conn).await?;
    let mut actual = task.clone();
    let reserved = row.is_some();
    if let Some(row) = row {
        let task_id = row.get::<Uuid, _>("id");
        if row.get::<String, _>("state") != "pending"
            || !row.get::<bool, _>("awaiting_dispatch")
            || u16::try_from(row.get::<i16, _>("attempt")).map_err(|_| invalid())? != task.attempt
        {
            return Ok(Publication::Stale);
        }
        let Some(binding) =
            super::worker_invocations::get_on(conn, &fence.tenant_id, task_id).await?
        else {
            return Ok(Publication::Stale);
        };
        if !fence.matches(&binding) || binding.attempt != task.attempt {
            return Ok(Publication::Stale);
        }
        actual.id = task_id;
    }
    let receipt = match admit_effect_on(conn, intent).await? {
        EffectSelection::Ready(receipt) => receipt,
        EffectSelection::Refused(result) => return Ok(result),
    };
    actual.effect_id = receipt.as_ref().map(|receipt| receipt.id);
    if !store_payload_on(conn, &actual, reserved).await? {
        return Ok(Publication::Stale);
    }
    super::worker_invocations::capture(conn, actual.id, Some(fence.expected_target())).await?;
    let admission = intent.admission(actual.id, receipt.as_ref());
    sqlx::query("INSERT INTO worker_task_admissions(task_id,invocation_key,tenant_id,instance_id,record) VALUES($1,$2,$3,$4,$5)")
        .bind(actual.id).bind(&admission.invocation_key).bind(fence.tenant_id.as_str()).bind(actual.instance_id.into_uuid()).bind(serde_json::to_value(&admission)?).execute(&mut *conn).await?;
    Ok(Publication::Published {
        task_id: actual.id,
        receipt: receipt.map(Box::new),
    })
}

enum EffectSelection {
    Ready(Option<EffectReceipt>),
    Refused(Publication),
}

async fn admit_effect_on(
    conn: &mut sqlx::PgConnection,
    intent: &WorkerDispatchIntent,
) -> Result<EffectSelection, StorageError> {
    let Some(plan) = &intent.effect else {
        return Ok(EffectSelection::Ready(None));
    };
    let scope = intent.fence.continuity.as_ref().ok_or_else(invalid)?;
    let epoch = i64::try_from(scope.epoch).map_err(|_| invalid())?;
    let mut receipt = super::effect_attempt::on(conn, &plan.receipt, plan.legacy_id, epoch).await?;
    if receipt.state == EffectState::Planned {
        receipt
            .transition(EffectState::Prepared, chrono::Utc::now())
            .map_err(|_| invalid())?;
        let changed=sqlx::query("UPDATE effect_receipts SET state='prepared',record=$1,updated_at=$2 WHERE tenant_id=$3 AND id=$4 AND state='planned'")
            .bind(super::continuity::encode(&receipt)?).bind(receipt.updated_at).bind(receipt.tenant_id.as_str()).bind(receipt.id.into_uuid()).execute(&mut *conn).await?.rows_affected();
        if changed != 1 {
            return Ok(EffectSelection::Refused(Publication::Stale));
        }
    }
    if receipt.state != EffectState::Prepared {
        return Ok(EffectSelection::Refused(Publication::EffectBlocked(
            Box::new(receipt),
        )));
    }
    receipt
        .transition(EffectState::Dispatched, chrono::Utc::now())
        .map_err(|_| invalid())?;
    let fence = crate::EffectDispatchFence {
        instance_id: intent.fence.instance_id,
        epoch: plan.receipt.epoch,
        owner_runtime_id: scope.owner_runtime_id,
        claim_token: Some(intent.fence.claim_token),
    };
    let result = super::effect_dispatch::on(
        conn,
        &intent.fence.tenant_id,
        &receipt,
        &fence,
        plan.guarded,
    )
    .await?;
    Ok(match result {
        EffectDispatchOutcome::Dispatched => EffectSelection::Ready(Some(receipt)),
        EffectDispatchOutcome::Duplicate => {
            EffectSelection::Refused(Publication::Duplicate(Box::new(receipt)))
        }
        EffectDispatchOutcome::Stale => EffectSelection::Refused(Publication::Stale),
    })
}

async fn store_payload_on(
    conn: &mut sqlx::PgConnection,
    task: &WorkerTask,
    reserved: bool,
) -> Result<bool, StorageError> {
    if reserved {
        let params = super::json_encoding::encode(&task.params)?;
        let context = super::json_encoding::encode(&task.context)?;
        Ok(sqlx::query("UPDATE worker_tasks SET tenant_id=(SELECT native.tenant_id FROM task_instances native WHERE native.id=worker_tasks.instance_id),handler_name=$2,queue_name=$3,requirements=$4,params=$5,context=$6,timeout_ms=$7,carries_credentials=$8,effect_id=$9,continuity_epoch=$10,created_at=$11,awaiting_dispatch=FALSE,params_encoding=$13,context_encoding=$14 WHERE id=$1 AND state='pending' AND awaiting_dispatch AND attempt=$12")
            .bind(task.id).bind(&task.handler_name).bind(&task.queue_name).bind(serde_json::to_value(&task.requirements)?).bind(params.value).bind(context.value)
            .bind(task.timeout_ms).bind(task.carries_credentials).bind(task.effect_id.map(orch8_types::continuity::EffectId::into_uuid)).bind(task.continuity_epoch.map(i64::try_from).transpose().map_err(|_|invalid())?).bind(task.created_at).bind(i16::try_from(task.attempt).map_err(|_| invalid())?)
            .bind(params.encoding).bind(context.encoding)
            .execute(&mut *conn).await?.rows_affected()==1)
    } else {
        let Some(row) = super::workers::insert_task_query(task, false)?
            .fetch_optional(&mut *conn)
            .await?
        else {
            return Ok(false);
        };
        Ok(row.get::<Uuid, _>("id") == task.id)
    }
}

pub(super) async fn matches_hot_on(
    conn: &mut sqlx::PgConnection,
    admission: &WorkerDispatchAdmission,
) -> Result<bool, StorageError> {
    let task_id = admission.invocation.task_id;
    let row=sqlx::query("SELECT params_encoding,context_encoding,jsonb_build_object('handler',handler_name,'queue',queue_name,'requirements',requirements,'params',params,'context',context,'attempt',attempt,'timeout_ms',timeout_ms,'carries_credentials',carries_credentials) AS spec FROM worker_tasks WHERE id=$1")
        .bind(task_id).fetch_optional(&mut *conn).await?;
    let Some(row) = row else { return Ok(false) };
    let mut value: serde_json::Value = row.get("spec");
    value["params"] = super::json_encoding::decode(
        value["params"].take(),
        row.get::<Option<String>, _>("params_encoding").as_deref(),
    )?;
    value["context"] = super::json_encoding::decode(
        value["context"].take(),
        row.get::<Option<String>, _>("context_encoding").as_deref(),
    )?;
    Ok(crate::worker_admission::digest(&value) == admission.stored_spec_sha256)
}
