//! Logical externalized payload snapshots, scoped to an instance.
use super::SqliteStorage;
use crate::externalizing::{ExternalizedEntry, decode_payload};
use orch8_types::{error::StorageError, ids::InstanceId};
use sqlx::Row;

pub(super) async fn list(
    store: &SqliteStorage,
    instance_id: InstanceId,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<ExternalizedEntry>, StorageError> {
    let rows = sqlx::query("SELECT ref_key, payload, payload_bytes, compression, created_at, expires_at FROM externalized_state WHERE instance_id=?1 AND (?2 IS NULL OR ref_key>?2) ORDER BY ref_key LIMIT ?3")
        .bind(instance_id.to_string()).bind(after).bind(i64::from(limit.min(1000)))
        .fetch_all(&store.pool).await?;
    rows.into_iter()
        .map(|row| {
            let payload = row
                .try_get::<Option<String>, _>("payload")?
                .map(|value| serde_json::from_str(&value))
                .transpose()?;
            let codec: Option<String> = row.try_get("compression")?;
            Ok(ExternalizedEntry {
                instance_id,
                ref_key: row.try_get("ref_key")?,
                payload: decode_payload(payload, row.try_get("payload_bytes")?, codec.as_deref())?,
                created_at: super::helpers::parse_ts(&row.try_get::<String, _>("created_at")?)?,
                expires_at: super::helpers::parse_ts_opt(row.try_get("expires_at")?)?,
            })
        })
        .collect()
}

pub(super) async fn restore(
    store: &SqliteStorage,
    entry: &ExternalizedEntry,
) -> Result<(), StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    if let Err(error) = restore_on(&mut tx, entry).await {
        tx.rollback().await?;
        return Err(error);
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn restore_on(
    conn: &mut sqlx::SqliteConnection,
    entry: &ExternalizedEntry,
) -> Result<(), StorageError> {
    super::externalized::save_on(
        &mut *conn,
        entry.instance_id,
        &entry.ref_key,
        &entry.payload,
    )
    .await?;
    let changed = sqlx::query("UPDATE externalized_state SET created_at=?1, expires_at=?2 WHERE ref_key=?3 AND instance_id=?4")
        .bind(super::helpers::ts(entry.created_at)).bind(entry.expires_at.map(super::helpers::ts)).bind(&entry.ref_key).bind(entry.instance_id.to_string())
        .execute(&mut *conn).await?.rows_affected();
    if changed != 1 {
        return Err(StorageError::Query(
            "externalized reference belongs to another instance".into(),
        ));
    }
    Ok(())
}
