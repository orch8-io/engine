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
        .bind(Uuid::new_v4())
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
        .bind(Uuid::new_v4())
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

pub(super) async fn delete(store: &PostgresStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key = $1")
        .bind(ref_key)
        .execute(&store.pool)
        .await?;
    Ok(())
}
