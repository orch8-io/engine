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

pub(super) async fn complete(
    storage: &PostgresStorage,
    access: &MobileInstanceAccess,
    expected_updated_at: chrono::DateTime<chrono::Utc>,
    context: &orch8_types::context::ExecutionContext,
    signal: &orch8_types::signal::Signal,
) -> Result<bool, StorageError> {
    require_legacy(access)?;
    let step = crate::mobile_access::completion_step(signal)?;
    let context_json = serde_json::to_value(context)?;
    let mut transaction = storage.pool.begin().await?;
    let target: Option<uuid::Uuid> = sqlx::query_scalar(
        "SELECT i.id FROM task_instances i WHERE i.id=$1 AND i.tenant_id='mobile' AND i.state='waiting'
         AND i.updated_at=$2 AND (
           (NOT EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=i.id) AND i.context->'runtime'->>'current_step'=$3)
           OR EXISTS(SELECT 1 FROM execution_tree n WHERE n.instance_id=i.id AND n.block_type='step' AND n.state='waiting' AND n.block_id=$3)
         ) FOR UPDATE",
    )
    .bind(signal.instance_id.into_uuid())
    .bind(expected_updated_at)
    .bind(step)
    .fetch_optional(&mut *transaction)
    .await?;
    if target.is_none() {
        return Ok(false);
    }
    let now = chrono::Utc::now();
    sqlx::query("UPDATE task_instances SET context=$2,state='scheduled',claimed_at=NULL,next_fire_at=$3,updated_at=$3 WHERE id=$1")
        .bind(signal.instance_id.into_uuid()).bind(context_json).bind(now)
        .execute(&mut *transaction).await?;
    let signal_type = signal.signal_type.to_string();
    super::signals::bind_signal_insert(
        sqlx::query(super::signals::SIGNAL_INSERT_SQL),
        signal,
        &signal_type,
    )
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(true)
}
