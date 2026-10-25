//! The legacy mobile tenant remains explicit; catalog persistence is SQLite-only.
use super::{PostgresStorage, rows::InstanceRow};
use crate::MobileInstanceAccess;
use orch8_types::{
    error::StorageError,
    ids::InstanceId,
    instance::{InstanceState, TaskInstance},
};

fn require_legacy(access: &MobileInstanceAccess) -> Result<(), StorageError> {
    match access {
        MobileInstanceAccess::Legacy => Ok(()),
        MobileInstanceAccess::Catalog { .. } => Err(StorageError::Unsupported(
            "mobile catalog persistence requires SQLite".into(),
        )),
    }
}
pub(super) async fn get(
    storage: &PostgresStorage,
    access: &MobileInstanceAccess,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    require_legacy(access)?;
    let row = sqlx::query_as::<_, InstanceRow>(
        "SELECT i.* FROM task_instances i WHERE i.id=$1 AND i.tenant_id='mobile'",
    )
    .bind(id.into_uuid())
    .fetch_optional(&storage.pool)
    .await?;
    row.map(InstanceRow::into_instance).transpose()
}
pub(super) async fn cancel(
    storage: &PostgresStorage,
    access: &MobileInstanceAccess,
    id: InstanceId,
) -> Result<Option<InstanceState>, StorageError> {
    require_legacy(access)?;
    let mut transaction = storage.pool.begin().await?;
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM task_instances WHERE id=$1 AND tenant_id='mobile' FOR UPDATE",
    )
    .bind(id.into_uuid())
    .fetch_optional(&mut *transaction)
    .await?;
    let Some(state) = state else { return Ok(None) };
    let state = state
        .parse::<InstanceState>()
        .map_err(StorageError::Query)?;
    if state.is_terminal() {
        transaction.commit().await?;
        return Ok(Some(state));
    }
    sqlx::query("UPDATE task_instances SET state='cancelled',claimed_at=NULL,next_fire_at=NULL,updated_at=$2 WHERE id=$1")
        .bind(id.into_uuid()).bind(chrono::Utc::now()).execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(Some(InstanceState::Cancelled))
}
