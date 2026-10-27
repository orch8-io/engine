//! Accepted output and hot completion share a transaction; the journal has no hot FK.
use super::PostgresStorage;
use crate::worker_outcome::WorkerAcceptedOutcome;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::WorkerClaim,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn get(
    store: &PostgresStorage,
    tenant: Option<&TenantId>,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerAcceptedOutcome>, StorageError> {
    let row=sqlx::query("SELECT tenant_id,instance_id,record FROM worker_accepted_outcomes WHERE task_id=$1 AND ($2 IS NULL OR tenant_id=$2)")
        .bind(task_id).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let outcome: WorkerAcceptedOutcome = serde_json::from_value(row.get("record"))?;
    outcome.validate(
        &TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(row.get("instance_id")),
    )?;
    Ok((outcome.claim == *claim).then_some(outcome))
}
pub(super) async fn insert_on(
    conn: &mut sqlx::PgConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
    output: &serde_json::Value,
) -> Result<(), StorageError> {
    let row = sqlx::query("SELECT i.tenant_id,t.instance_id FROM worker_tasks t JOIN task_instances i ON i.id=t.instance_id WHERE t.id=$1")
        .bind(task_id)
        .fetch_one(&mut *conn)
        .await?;
    let raw: Option<serde_json::Value>=sqlx::query_scalar("SELECT COALESCE((SELECT record->'invocation' FROM worker_task_admissions WHERE task_id=$1),(SELECT record FROM worker_task_invocations WHERE task_id=$1))")
        .bind(task_id).fetch_one(&mut *conn).await?;
    let invocation = raw.map(serde_json::from_value).transpose()?;
    let outcome = WorkerAcceptedOutcome::new(
        TenantId::unchecked(row.get::<String, _>("tenant_id")),
        task_id,
        InstanceId::from_uuid(row.get("instance_id")),
        claim.clone(),
        invocation,
        output.clone(),
    );
    outcome.validate(&outcome.tenant_id, task_id, outcome.instance_id)?;
    sqlx::query("INSERT INTO worker_accepted_outcomes(task_id,tenant_id,instance_id,record) VALUES($1,$2,$3,$4)")
        .bind(task_id).bind(outcome.tenant_id.as_str()).bind(outcome.instance_id.into_uuid()).bind(serde_json::to_value(&outcome)?)
        .execute(&mut *conn).await?;
    Ok(())
}
