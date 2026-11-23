use crate::worker_callback::WorkerCallbackBinding;
use orch8_types::{error::StorageError, ids::TenantId, worker::WorkerClaim};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn insert_on(
    conn: &mut sqlx::SqliteConnection,
    binding: &WorkerCallbackBinding,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO worker_callback_tokens (task_id,claim_epoch,tenant_id,worker_id,token,expires_at) VALUES (?1,?2,?3,?4,?5,?6)")
        .bind(binding.task_id.to_string())
        .bind(crate::worker_claim::sql_counter(binding.claim.claim_epoch)?)
        .bind(binding.tenant_id.as_str())
        .bind(&binding.claim.worker_id)
        .bind(&binding.token)
        .bind(super::helpers::ts(binding.expires_at))
        .execute(&mut *conn).await?;
    Ok(())
}

pub(super) async fn get_for_claim_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<WorkerCallbackBinding>, StorageError> {
    let row = sqlx::query("SELECT * FROM worker_callback_tokens WHERE task_id=?1 AND claim_epoch=?2 AND worker_id=?3 AND julianday(expires_at)>julianday('now')")
        .bind(task_id.to_string())
        .bind(crate::worker_claim::sql_counter(claim.claim_epoch)?)
        .bind(&claim.worker_id)
        .fetch_optional(&mut *conn).await?;
    row.as_ref().map(parse).transpose()
}

pub(super) async fn lookup(
    store: &super::SqliteStorage,
    tenant: Option<&TenantId>,
    token: &str,
) -> Result<Option<WorkerCallbackBinding>, StorageError> {
    if !crate::worker_callback::valid_token(token) {
        return Ok(None);
    }
    let row = sqlx::query("SELECT * FROM worker_callback_tokens WHERE token=?1 AND (?2 IS NULL OR tenant_id=?2) AND julianday(expires_at)>julianday('now')")
        .bind(token).bind(tenant.map(TenantId::as_str)).fetch_optional(&store.pool).await?;
    row.as_ref().map(parse).transpose()
}

/// Recheck callback authority under the same transaction as the worker outcome.
pub(super) async fn authorized_on(
    conn: &mut sqlx::SqliteConnection,
    task_id: Uuid,
    claim: &WorkerClaim,
    proof: Option<&WorkerCallbackBinding>,
) -> Result<bool, StorageError> {
    let callback: Option<bool> =
        sqlx::query_scalar("SELECT wait_for_callback FROM worker_tasks WHERE id=?1")
            .bind(task_id.to_string())
            .fetch_optional(&mut *conn)
            .await?;
    match (callback, proof) {
        (Some(false), None) => Ok(true),
        (Some(true), Some(proof)) if proof.task_id == task_id && &proof.claim == claim => {
            Ok(get_for_claim_on(conn, task_id, claim)
                .await?
                .is_some_and(|issued| {
                    issued.token == proof.token
                        && issued.tenant_id == proof.tenant_id
                        && issued.expires_at == proof.expires_at
                }))
        }
        _ => Ok(false),
    }
}

fn parse(row: &sqlx::sqlite::SqliteRow) -> Result<WorkerCallbackBinding, StorageError> {
    let epoch = u64::try_from(row.try_get::<i64, _>("claim_epoch")?)
        .map_err(|_| StorageError::Constraint("invalid callback claim epoch".into()))?;
    Ok(WorkerCallbackBinding {
        task_id: Uuid::parse_str(row.try_get::<&str, _>("task_id")?)
            .map_err(|_| StorageError::Constraint("invalid callback task id".into()))?,
        claim: WorkerClaim::new(row.try_get::<String, _>("worker_id")?, epoch),
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        token: row.try_get("token")?,
        expires_at: super::helpers::parse_ts(row.try_get::<&str, _>("expires_at")?)?,
    })
}
