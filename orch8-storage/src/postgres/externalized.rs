use std::collections::HashMap;

use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;
use crate::compression::{compress, decompress, COMPRESSION_THRESHOLD_BYTES};

pub(super) async fn save(
    store: &PostgresStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
    let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

    if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
        let compressed = compress(payload)?;
        sqlx::query(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, NULL, $4, 'zstd', $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = NULL,
                    payload_bytes = EXCLUDED.payload_bytes,
                    compression = 'zstd',
                    size_bytes = EXCLUDED.size_bytes",
        )
        .bind(Uuid::now_v7())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(&compressed)
        .bind(raw_size)
        .execute(&store.pool)
        .await?;
    } else {
        sqlx::query(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, $4, NULL, NULL, $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = EXCLUDED.payload,
                    payload_bytes = NULL,
                    compression = NULL,
                    size_bytes = EXCLUDED.size_bytes",
        )
        .bind(Uuid::now_v7())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(payload)
        .bind(raw_size)
        .execute(&store.pool)
        .await?;
    }
    Ok(())
}

type ExternalizedRow = (Option<serde_json::Value>, Option<Vec<u8>>, Option<String>);

pub(super) async fn get(
    store: &PostgresStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row: Option<ExternalizedRow> = sqlx::query_as(
        "SELECT payload, payload_bytes, compression
             FROM externalized_state WHERE ref_key = $1",
    )
    .bind(ref_key)
    .fetch_optional(&store.pool)
    .await?;

    let Some((payload, payload_bytes, compression)) = row else {
        return Ok(None);
    };

    match compression.as_deref() {
        Some("zstd") => {
            let bytes = payload_bytes.ok_or_else(|| {
                StorageError::Query(
                    "externalized_state row has compression='zstd' but payload_bytes is NULL"
                        .into(),
                )
            })?;
            Ok(Some(decompress(&bytes)?))
        }
        None => Ok(payload),
        Some(other) => Err(StorageError::Query(format!(
            "unknown externalized_state compression codec: {other}"
        ))),
    }
}

/// Atomic batched save. Opens a transaction, groups entries by storage
/// shape (compressed vs inline), and issues one bulk `INSERT` per group
/// using `QueryBuilder::push_values`. Each bulk statement carries the same
/// `ON CONFLICT DO UPDATE` semantics as [`save`].
///
/// Atomicity contract: if any single row fails to insert the transaction is
/// rolled back so the caller's `task_instances.context` never references a
/// ref-key that isn't persisted.
pub(super) async fn batch_save(
    store: &PostgresStorage,
    instance_id: InstanceId,
    entries: &[(String, serde_json::Value)],
) -> Result<(), StorageError> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut compressed: Vec<(String, Vec<u8>, i64)> = Vec::with_capacity(entries.len());
    let mut inline: Vec<(String, serde_json::Value, i64)> = Vec::with_capacity(entries.len());

    for (ref_key, payload) in entries {
        let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
        let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

        if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
            let c = compress(payload)?;
            compressed.push((ref_key.clone(), c, raw_size));
        } else {
            inline.push((ref_key.clone(), payload.clone(), raw_size));
        }
    }

    let mut tx = store.pool.begin().await?;

    for chunk in compressed.chunks(500) {
        let mut qb = sqlx::QueryBuilder::new(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ",
        );
        qb.push_values(chunk, |mut b, (ref_key, payload_bytes, size_bytes)| {
            b.push_bind(Uuid::now_v7())
                .push_bind(instance_id.0)
                .push_bind(ref_key)
                .push_bind(None::<serde_json::Value>)
                .push_bind(payload_bytes)
                .push_bind("zstd")
                .push_bind(size_bytes)
                .push_bind(chrono::Utc::now());
        });
        qb.push(
            " ON CONFLICT (ref_key) DO UPDATE
                SET payload = NULL,
                    payload_bytes = EXCLUDED.payload_bytes,
                    compression = 'zstd',
                    size_bytes = EXCLUDED.size_bytes",
        );
        qb.build().execute(&mut *tx).await?;
    }

    for chunk in inline.chunks(500) {
        let mut qb = sqlx::QueryBuilder::new(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ",
        );
        qb.push_values(chunk, |mut b, (ref_key, payload, size_bytes)| {
            b.push_bind(Uuid::now_v7())
                .push_bind(instance_id.0)
                .push_bind(ref_key)
                .push_bind(payload)
                .push_bind(None::<Vec<u8>>)
                .push_bind(None::<String>)
                .push_bind(size_bytes)
                .push_bind(chrono::Utc::now());
        });
        qb.push(
            " ON CONFLICT (ref_key) DO UPDATE
                SET payload = EXCLUDED.payload,
                    payload_bytes = NULL,
                    compression = NULL,
                    size_bytes = EXCLUDED.size_bytes",
        );
        qb.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

type BatchRow = (
    String,
    Option<serde_json::Value>,
    Option<Vec<u8>>,
    Option<String>,
);

/// Batched variant of [`get`]. Executes a single `SELECT ... WHERE ref_key = ANY($1)`
/// and decompresses rows as needed. Missing keys simply won't appear in the
/// result map — callers treat absent entries as "nothing to hydrate".
pub(super) async fn batch_get(
    store: &PostgresStorage,
    ref_keys: &[String],
) -> Result<HashMap<String, serde_json::Value>, StorageError> {
    if ref_keys.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<BatchRow> = sqlx::query_as(
        "SELECT ref_key, payload, payload_bytes, compression
             FROM externalized_state WHERE ref_key = ANY($1)",
    )
    .bind(ref_keys)
    .fetch_all(&store.pool)
    .await?;

    let mut out = HashMap::with_capacity(rows.len());
    for (ref_key, payload, payload_bytes, compression) in rows {
        let value = match compression.as_deref() {
            Some("zstd") => {
                let bytes = payload_bytes.ok_or_else(|| {
                    StorageError::Query(
                        "externalized_state row has compression='zstd' but payload_bytes is NULL"
                            .into(),
                    )
                })?;
                decompress(&bytes)?
            }
            None => match payload {
                Some(v) => v,
                None => {
                    return Err(StorageError::Query(
                        "externalized_state row has compression=NULL and payload=NULL".into(),
                    ));
                }
            },
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

pub(super) async fn delete(store: &PostgresStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key = $1")
        .bind(ref_key)
        .execute(&store.pool)
        .await?;
    Ok(())
}

/// Delete up to `limit` rows whose `expires_at` has elapsed. Returns the
/// affected row count.
///
/// Postgres `DELETE` does not accept a `LIMIT` clause directly, so we pre-select
/// the `ref_key` values to bound sweep size, then delete the matching rows.
///
/// Multi-node safety: several engine nodes run the GC loop concurrently.
/// `FOR UPDATE SKIP LOCKED` makes each node's sub-query claim its own slice of
/// expired rows without blocking the others (two sweepers no longer fight over
/// the same rows and neither waits on the other's transaction). `ORDER BY
/// expires_at` makes the selection deterministic and biases toward the oldest
/// debt first — important when a backlog builds up and we want bounded tail
/// latency on cleanup.
pub(super) async fn delete_expired(
    store: &PostgresStorage,
    limit: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        r"DELETE FROM externalized_state
          WHERE ref_key IN (
              SELECT ref_key FROM externalized_state
              WHERE expires_at IS NOT NULL AND expires_at <= NOW()
              ORDER BY expires_at ASC
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )",
    )
    .bind(i64::from(limit))
    .execute(&store.pool)
    .await?;
    Ok(result.rows_affected())
}
