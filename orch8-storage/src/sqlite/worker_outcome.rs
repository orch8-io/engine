//! Accepted output and hot completion share a transaction; the journal has no hot FK.
use super::SqliteStorage;
use crate::worker_outcome::WorkerAcceptedOutcome;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::WorkerClaim,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn get(
    store: &SqliteStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerAcceptedOutcome>, StorageError> {
    let row=sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=?1 AND (?2 IS NULL OR tenant_id=?2)")
        .bind(task_id.to_string()).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let outcome = crate::worker_outcome::decode_record(serde_json::from_str(
        &row.get::<String, _>("record"),
    )?)?;
    outcome.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(
            Uuid::parse_str(&row.get::<String, _>("instance_id"))
                .map_err(|e| StorageError::Constraint(e.to_string()))?,
        ),
    )?;
    Ok((outcome.claim == *claim).then_some(outcome))
}
pub(super) async fn insert_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    let row = sqlx::query("SELECT i.tenant_id,t.instance_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=?1")
        .bind(task_id.to_string())
        .fetch_one(&mut *conn)
        .await?;
    let raw: Option<String>=sqlx::query_scalar("SELECT COALESCE((SELECT json_extract(record,'$.invocation') FROM worker_task_admissions WHERE task_id=?1),(SELECT record FROM worker_task_invocations WHERE task_id=?1))")
        .bind(task_id.to_string()).fetch_one(&mut *conn).await?;
    let invocation = raw.map(|value| serde_json::from_str(&value)).transpose()?;
    let outcome = WorkerAcceptedOutcome::new(
        TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(
            Uuid::parse_str(&row.get::<String, _>("instance_id"))
                .map_err(|e| StorageError::Constraint(e.to_string()))?,
        ),
        claim.clone(),
        invocation,
        completion,
    );
    outcome.validate(&outcome.tenant_id, task_id, outcome.instance_id)?;
    sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES(?1,?2,?3,?4)")
        .bind(task_id.to_string()).bind(outcome.tenant_id.as_str()).bind(outcome.instance_id.to_string()).bind(serde_json::to_string(&crate::worker_outcome::encode_record(&outcome)?)?)
        .execute(&mut *conn).await?;
    Ok(())
}

/// Runs only after the exact lease CAS has won, inside that same transaction.
pub(super) async fn settle_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    completion: &crate::worker_outcome::WorkerCompletion,
) -> Result<(), StorageError> {
    use orch8_types::continuity::EffectReceipt;
    let invalid = || {
        StorageError::Conflict("worker completion effect evidence requires reconciliation".into())
    };
    let row=sqlx::query("SELECT t.effect_id,t.block_id,t.attempt,i.tenant_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=?1")
        .bind(task_id.to_string()).fetch_one(&mut *conn).await?;
    let tenant = TenantId::unchecked(row.get::<String, _>("tenant_id"));
    let effect_id = row
        .get::<Option<String>, _>("effect_id")
        .map(|id| Uuid::parse_str(&id).map_err(|e| StorageError::Constraint(e.to_string())))
        .transpose()?;
    let Some(effect_id) = effect_id else {
        // An old unbound task cannot certify that an existing effect is pure.
        let has_effect_binding: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt WHERE t.id=?1 AND b.tenant_id=?2)")
            .bind(task_id.to_string()).bind(tenant.as_str()).fetch_one(&mut *conn).await?;
        return if has_effect_binding {
            Err(invalid())
        } else {
            Ok(())
        };
    };
    let admission = super::worker_admission::get_on(conn, &tenant, task_id).await?;
    let invocation = match &admission {
        Some(admission) => admission.invocation.clone(),
        None => super::worker_invocations::get_on(conn, &tenant, task_id)
            .await?
            .ok_or_else(invalid)?,
    };
    if invocation
        .effect_id
        .map(orch8_types::continuity::EffectId::into_uuid)
        != Some(effect_id)
        || invocation.block_id.as_str() != row.get::<String, _>("block_id")
        || i64::from(invocation.attempt) != row.get::<i64, _>("attempt")
    {
        return Err(invalid());
    }
    let scope = invocation.continuity.as_ref().ok_or_else(invalid)?;
    let epoch = i64::try_from(scope.epoch).map_err(|_| invalid())?;
    let bound:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN worker_tasks t ON b.instance_id=t.instance_id AND b.block_id=t.block_id AND b.attempt=t.attempt AND b.effect_id=t.effect_id WHERE t.id=?1 AND b.tenant_id=?2 AND b.continuity_id=?3 AND b.epoch=?4)")
        .bind(task_id.to_string()).bind(tenant.as_str()).bind(scope.continuity_id.to_string()).bind(epoch).fetch_one(&mut *conn).await?;
    if !bound {
        return Err(invalid());
    }
    let row = sqlx::query("SELECT state,record FROM effect_receipts WHERE tenant_id=?1 AND id=?2")
        .bind(tenant.as_str())
        .bind(effect_id.to_string())
        .fetch_optional(&mut *conn)
        .await?
        .ok_or_else(invalid)?;
    let receipt: EffectReceipt = serde_json::from_str(&row.get::<String, _>("record"))?;
    let previous = super::continuity::state_name(receipt.state)?;
    if previous != row.get::<String, _>("state") {
        return Err(invalid());
    }
    let next = crate::worker_outcome::settled_receipt(
        &invocation,
        admission.as_ref().and_then(|a| a.effect.as_ref()),
        receipt,
        completion,
    )?;
    let changed=sqlx::query("UPDATE effect_receipts SET state='committed',record=?1,updated_at=?2 WHERE tenant_id=?3 AND id=?4 AND state=?5")
        .bind(serde_json::to_string(&next)?).bind(next.updated_at.to_rfc3339()).bind(tenant.as_str()).bind(effect_id.to_string()).bind(previous).execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(invalid());
    }
    Ok(())
}
