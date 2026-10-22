//! Logical externalized payload snapshots, scoped to an instance.
use super::PostgresStorage;
use crate::externalizing::{ExternalizedEntry, decode_payload};
use orch8_types::{error::StorageError, ids::InstanceId};
use sqlx::Row;

pub(super) async fn list(
    store: &PostgresStorage,
    instance_id: InstanceId,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<ExternalizedEntry>, StorageError> {
    let rows = sqlx::query("SELECT ref_key, payload, payload_bytes, compression, created_at, expires_at FROM externalized_state WHERE instance_id=$1 AND ($2::text IS NULL OR ref_key>$2) ORDER BY ref_key LIMIT $3")
        .bind(instance_id.into_uuid()).bind(after).bind(i64::from(limit.min(1000)))
        .fetch_all(&store.pool).await?;
    rows.into_iter()
        .map(|row| {
            let payload = row.try_get("payload")?;
            let codec: Option<String> = row.try_get("compression")?;
            Ok(ExternalizedEntry {
                instance_id,
                ref_key: row.try_get("ref_key")?,
                payload: decode_payload(payload, row.try_get("payload_bytes")?, codec.as_deref())?,
                created_at: row.try_get("created_at")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .collect()
}

pub(super) async fn restore(
    store: &PostgresStorage,
    entry: &ExternalizedEntry,
) -> Result<(), StorageError> {
    let mut tx = store.pool.begin().await?;
    super::externalized::save_on(&mut tx, entry.instance_id, &entry.ref_key, &entry.payload)
        .await?;
    let changed = sqlx::query("UPDATE externalized_state SET created_at=$1, expires_at=$2 WHERE ref_key=$3 AND instance_id=$4")
        .bind(entry.created_at).bind(entry.expires_at).bind(&entry.ref_key).bind(entry.instance_id.into_uuid())
        .execute(&mut *tx).await?.rows_affected();
    if changed != 1 {
        tx.rollback().await?;
        return Err(StorageError::Query(
            "externalized reference belongs to another instance".into(),
        ));
    }
    tx.commit().await?;
    Ok(())
}
