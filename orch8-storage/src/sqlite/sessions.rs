use chrono::Utc;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::TaskInstance;
use orch8_types::session::{Session, SessionState};

use super::helpers::{row_to_instance, row_to_session, ts};
use super::SqliteStorage;

pub(super) async fn create(storage: &SqliteStorage, s: &Session) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO sessions (id,tenant_id,session_key,data,state,created_at,updated_at,expires_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)")
        .bind(s.id.to_string())
        .bind(&s.tenant_id.0)
        .bind(&s.session_key)
        .bind(serde_json::to_string(&s.data)?)
        .bind(s.state.to_string())
        .bind(ts(s.created_at))
        .bind(ts(s.updated_at))
        .bind(s.expires_at.map(ts))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<Session>, StorageError> {
    let row = sqlx::query("SELECT * FROM sessions WHERE id=?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_session).transpose()
}

pub(super) async fn get_by_key(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    session_key: &str,
) -> Result<Option<Session>, StorageError> {
    let row = sqlx::query("SELECT * FROM sessions WHERE tenant_id=?1 AND session_key=?2")
        .bind(&tenant_id.0)
        .bind(session_key)
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_session).transpose()
}

pub(super) async fn update_data(
    storage: &SqliteStorage,
    id: Uuid,
    data: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET data=?2, updated_at=?3 WHERE id=?1")
        .bind(id.to_string())
        .bind(serde_json::to_string(data)?)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn update_state(
    storage: &SqliteStorage,
    id: Uuid,
    state: SessionState,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET state=?2, updated_at=?3 WHERE id=?1")
        .bind(id.to_string())
        .bind(state.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn list_instances(
    storage: &SqliteStorage,
    session_id: Uuid,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows = sqlx::query("SELECT * FROM task_instances WHERE session_id=?1")
        .bind(session_id.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_instance).collect()
}
