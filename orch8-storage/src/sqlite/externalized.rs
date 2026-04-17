use std::collections::HashMap;

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

/// Atomic batched save. Wraps N inserts in a single transaction so partial
/// failures never leave `task_instances.context` pointing at ref-keys that
/// don't exist. Per-row compression branching mirrors [`save`].
pub(super) async fn batch_save(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    entries: &[(String, serde_json::Value)],
) -> Result<(), StorageError> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    for (ref_key, payload) in entries {
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
            .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        }
    }

    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Batched variant of [`get`]. SQLite has no native array binding, so we build
/// an `IN (?,?,...)` clause dynamically. Missing keys are absent from the
/// returned map — callers treat that as "nothing to hydrate".
///
/// SQLite caps a single statement at `SQLITE_MAX_VARIABLE_NUMBER` bound params
/// (default 32766 in modern builds). The scheduler preload path only batches
/// the fields of *one* dispatched step, so in practice `ref_keys` is tiny —
/// no chunking needed here.
pub(super) async fn batch_get(
    storage: &SqliteStorage,
    ref_keys: &[String],
) -> Result<HashMap<String, serde_json::Value>, StorageError> {
    if ref_keys.is_empty() {
        return Ok(HashMap::new());
    }

    // Build placeholder list: "?1,?2,?3" — 1-based for clarity, though sqlx
    // accepts plain "?" too. Named indices keep behavior unambiguous.
    let placeholders = (1..=ref_keys.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT ref_key, payload, payload_bytes, compression \
         FROM externalized_state WHERE ref_key IN ({placeholders})"
    );

    let mut query = sqlx::query(&sql);
    for key in ref_keys {
        query = query.bind(key);
    }
    let rows = query
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let ref_key: String = row
            .try_get("ref_key")
            .map_err(|e| StorageError::Query(e.to_string()))?;
        let compression: Option<String> = row.try_get("compression").unwrap_or(None);
        let value = match compression.as_deref() {
            Some("zstd") => {
                let bytes: Vec<u8> = row
                    .try_get("payload_bytes")
                    .map_err(|e| StorageError::Query(e.to_string()))?;
                decompress(&bytes)?
            }
            None => {
                let payload: Option<String> = row.try_get("payload").unwrap_or(None);
                match payload {
                    Some(s) => serde_json::from_str(&s).map_err(StorageError::Serialization)?,
                    None => {
                        return Err(StorageError::Query(
                            "externalized_state row has compression=NULL and payload=NULL".into(),
                        ));
                    }
                }
            }
            Some(other) => {
                return Err(StorageError::Query(format!(
                    "unknown externalized_state compression codec: {other}"
                )));
            }
        };
        out.insert(ref_key, value);
    }
    Ok(out)
}

pub(super) async fn delete(storage: &SqliteStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key=?1")
        .bind(ref_key)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Delete up to `limit` rows whose `expires_at` has elapsed. `expires_at` is
/// stored as ISO-8601 text in SQLite. Both sides of the comparison are
/// normalized through `datetime(...)` so RFC 3339 strings (with a 'T'
/// separator and timezone offset, e.g. `2026-04-16T10:00:00+00:00`) are
/// compared as real timestamps rather than by lexicographic byte order,
/// which would otherwise disagree with `datetime('now')`'s space-separated
/// form and silently skip expired rows.
pub(super) async fn delete_expired(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM externalized_state
          WHERE ref_key IN (
              SELECT ref_key FROM externalized_state
              WHERE expires_at IS NOT NULL AND datetime(expires_at) <= datetime('now')
              LIMIT ?1
          )",
    )
    .bind(i64::from(limit))
    .execute(&storage.pool)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_types::ids::InstanceId;
    use serde_json::json;

    use super::*;

    async fn mk_store() -> SqliteStorage {
        SqliteStorage::in_memory().await.unwrap()
    }

    async fn set_expires_at(
        store: &SqliteStorage,
        ref_key: &str,
        expires: Option<&str>,
    ) {
        sqlx::query("UPDATE externalized_state SET expires_at = ?1 WHERE ref_key = ?2")
            .bind(expires)
            .bind(ref_key)
            .execute(&store.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn delete_expired_removes_past_but_keeps_future_and_null() {
        let store = mk_store().await;
        let inst = InstanceId::new();

        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();

        for k in ["a:past", "b:future", "c:null"] {
            save(&store, inst, k, &json!({"v": k})).await.unwrap();
        }
        set_expires_at(&store, "a:past", Some(&past)).await;
        set_expires_at(&store, "b:future", Some(&future)).await;
        // c:null keeps expires_at = NULL (never expires)

        let deleted = delete_expired(&store, 100).await.unwrap();
        assert_eq!(deleted, 1, "only the past-expired row should be deleted");

        assert!(get(&store, "a:past").await.unwrap().is_none());
        assert!(get(&store, "b:future").await.unwrap().is_some());
        assert!(get(&store, "c:null").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_expired_respects_limit() {
        let store = mk_store().await;
        let inst = InstanceId::new();
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();

        for i in 0..5 {
            let k = format!("k:{i}");
            save(&store, inst, &k, &json!({"i": i})).await.unwrap();
            set_expires_at(&store, &k, Some(&past)).await;
        }

        // Sweep in two bounded passes — confirms LIMIT is honored.
        let first = delete_expired(&store, 2).await.unwrap();
        assert_eq!(first, 2);
        let second = delete_expired(&store, 100).await.unwrap();
        assert_eq!(second, 3);
        let third = delete_expired(&store, 100).await.unwrap();
        assert_eq!(third, 0, "idempotent once backlog is drained");
    }

    #[tokio::test]
    async fn delete_expired_on_empty_store_is_zero() {
        let store = mk_store().await;
        let deleted = delete_expired(&store, 100).await.unwrap();
        assert_eq!(deleted, 0);
    }
}
