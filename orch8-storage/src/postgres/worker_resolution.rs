//! Resolution settlement and immutable report evidence share the native resolution transaction.
use super::PostgresStorage;
use crate::worker_resolution::{
    ResolutionSource, WorkerResolutionEvidence, WorkerResolutionOutcome,
};
use orch8_types::{
    error::StorageError,
    ids::{BlockId, InstanceId, TenantId},
    worker::{WorkerClaim, WorkerTaskResolution},
};
use sqlx::Row;
use uuid::Uuid;
fn invalid() -> StorageError {
    StorageError::Conflict("worker resolution scope or effect requires reconciliation".into())
}

pub(super) async fn capture_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
) -> Result<ResolutionSource, StorageError> {
    let row=sqlx::query("SELECT i.tenant_id,t.instance_id,t.block_id,t.handler_name,t.attempt,t.state,t.claim_epoch,t.worker_id,t.effect_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=$1")
        .bind(task_id).fetch_one(&mut *conn).await?;
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    let admission = super::worker_admission::get_on(conn, &tenant, task_id).await?;
    let invocation = match &admission {
        Some(a) => Some(a.invocation.clone()),
        None => super::worker_invocations::get_on(conn, &tenant, task_id).await?,
    };
    let admitted = admission.is_some();
    let source = ResolutionSource {
        tenant,
        task_id,
        instance: InstanceId::from_uuid(row.get("instance_id")),
        block: BlockId::new(row.get::<String, _>("block_id")),
        handler: row.get("handler_name"),
        attempt: u16::try_from(row.get::<i16, _>("attempt")).map_err(|_| invalid())?,
        state: row
            .get::<String, _>("state")
            .parse()
            .map_err(|_| invalid())?,
        claim_epoch: u64::try_from(row.get::<i64, _>("claim_epoch")).map_err(|_| invalid())?,
        worker_id: row.get("worker_id"),
        effect_id: (row.get::<Option<Uuid>, _>("effect_id"))
            .map(orch8_types::continuity::EffectId::from_uuid),
        invocation,
        proof: admission.and_then(|a| a.effect),
        admitted,
    };
    source.validate()?;
    if source.effect_id.is_none() {
        let alias:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings WHERE tenant_id=$1 AND instance_id=$2 AND block_id=$3 AND attempt=$4)")
            .bind(source.tenant.as_str()).bind(source.instance.into_uuid()).bind(source.block.as_str()).bind(i64::from(source.attempt))
            .fetch_one(&mut *conn).await?;
        if alias {
            return Err(invalid());
        }
    }
    Ok(source)
}

pub(super) async fn record_on(
    conn: &mut sqlx::PgConnection,
    source: &ResolutionSource,
    resolution: &WorkerTaskResolution,
    evidence: &WorkerResolutionEvidence,
) -> Result<(), StorageError> {
    source.validate_disposition(resolution)?;
    let states = if let Some(effect_id) = source.effect_id {
        let row = sqlx::query(
            "SELECT state,record FROM effect_receipts WHERE tenant_id=$1 AND id=$2 FOR UPDATE",
        )
        .bind(source.tenant.as_str())
        .bind(effect_id.into_uuid())
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(invalid)?;
        let receipt: orch8_types::continuity::EffectReceipt =
            serde_json::from_value(row.get("record"))?;
        let before = receipt.state;
        let old = super::continuity::state_name(before)?;
        if old != row.get::<String, _>("state") {
            return Err(invalid());
        }
        let settled = source.settled_receipt(resolution.effect_resolution, receipt)?;
        if settled.state != before {
            let changed=sqlx::query("UPDATE effect_receipts SET state=$1,record=$2,updated_at=$3 WHERE tenant_id=$4 AND id=$5 AND state=$6")
                .bind(super::continuity::state_name(settled.state)?).bind(serde_json::to_value(&settled)?)
                .bind(settled.updated_at)
                .bind(source.tenant.as_str()).bind(effect_id.into_uuid()).bind(old).execute(&mut *conn).await?.rows_affected();
            if changed != 1 {
                return Err(invalid());
            }
        }
        Some((before, settled.state))
    } else {
        None
    };
    let outcome = source.outcome(resolution, evidence, states);
    sqlx::query("INSERT INTO worker_resolution_outcomes(id,task_id,tenant_id,instance_id,claim_epoch,worker_id,kind,record,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(outcome.id).bind(outcome.task_id).bind(outcome.tenant_id.as_str()).bind(outcome.instance_id.into_uuid())
        .bind(i64::try_from(outcome.claim_epoch).map_err(|_|invalid())?).bind(&outcome.worker_id).bind(outcome.event.as_str())
        .bind(crate::worker_resolution::encode_record(&outcome)?).bind(outcome.resolved_at)
        .execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: Option<&WorkerClaim>,
) -> Result<Option<WorkerResolutionOutcome>, StorageError> {
    let Ok(epoch) = claim.map(|c| i64::try_from(c.claim_epoch)).transpose() else {
        return Ok(None);
    };
    let row=sqlx::query("SELECT id,task_id,tenant_id,instance_id,claim_epoch,worker_id,kind,record FROM worker_resolution_outcomes WHERE task_id=$1 AND ($2::text IS NULL OR tenant_id=$2) AND ($3::bigint IS NULL OR (claim_epoch=$3 AND worker_id=$4)) ORDER BY created_at DESC,id DESC LIMIT 1")
        .bind(task_id).bind(tenant.map(TenantId::as_str)).bind(epoch).bind(claim.map(|c|c.worker_id.as_str()))
        .fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let outcome = crate::worker_resolution::decode_record(row.get("record"))?;
    outcome.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(row.get("instance_id")),
        u64::try_from(row.get::<i64, _>("claim_epoch")).map_err(|_| invalid())?,
        row.get::<Option<String>, _>("worker_id").as_deref(),
    )?;
    if outcome.id != row.get::<Uuid, _>("id")
        || outcome.event.as_str() != row.get::<String, _>("kind")
    {
        return Err(invalid());
    }
    Ok(Some(outcome))
}
