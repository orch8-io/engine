use chrono::Utc;
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::ts;
use super::SqliteStorage;
use crate::compression::{compress, decompress, COMPRESSION_THRESHOLD_BYTES};

pub(super) async fn save(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
    let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

    if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
        let compressed = compress(payload)?;
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, NULL, ?3, 'zstd', ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(compressed)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    } else {
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(payload).map_err(StorageError::Serialization)?)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row = sqlx::query(
        "SELECT payload, payload_bytes, compression \
         FROM externalized_state WHERE ref_key = ?1",
    )
    .bind(ref_key)
    .fetch_optional(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let compression: Option<String> = row.try_get("compression").unwrap_or(None);
    match compression.as_deref() {
        Some("zstd") => {
            let bytes: Vec<u8> = row
                .try_get("payload_bytes")
                .map_err(|e| StorageError::Query(e.to_string()))?;
            Ok(Some(decompress(&bytes)?))
        }
        None => {
            let payload: Option<String> = row.try_get("payload").unwrap_or(None);
            match payload {
                Some(s) => Ok(Some(
                    serde_json::from_str(&s).map_err(StorageError::Serialization)?,
                )),
                None => Err(StorageError::Query(
                    "externalized_state row has compression=NULL and payload=NULL".into(),
                )),
            }
        }
        Some(other) => Err(StorageError::Query(format!(
            "unknown externalized_state compression codec: {other}"
        ))),
    }
}

pub(super) async fn delete(storage: &SqliteStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key=?1")
        .bind(ref_key)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
