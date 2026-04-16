use chrono::Utc;
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::ts;
use super::SqliteStorage;

pub(super) async fn save(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("INSERT OR REPLACE INTO externalized_state (ref_key,instance_id,payload,created_at) VALUES (?1,?2,?3,?4)")
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(payload)?)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row = sqlx::query("SELECT payload FROM externalized_state WHERE ref_key=?1")
        .bind(ref_key)
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.map(|r| serde_json::from_str(r.get::<&str, _>("payload")))
        .transpose()?)
}

pub(super) async fn delete(storage: &SqliteStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key=?1")
        .bind(ref_key)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
