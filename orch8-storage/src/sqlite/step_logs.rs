use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::step_log::{StepLog, StepLogEntry};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

pub(super) async fn append(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    block_id: &BlockId,
    entries: &[StepLogEntry],
) -> Result<(), StorageError> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut tx = storage.pool.begin().await?;
    for e in entries {
        sqlx::query(
            "INSERT INTO step_logs (id, instance_id, block_id, ts, level, message) VALUES (?1,?2,?3,?4,?5,?6)",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(instance_id.to_string())
        .bind(block_id.as_str())
        .bind(ts(e.ts))
        .bind(&e.level)
        .bind(&e.message)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<Vec<StepLog>, StorageError> {
    let rows = sqlx::query(
        "SELECT block_id, ts, level, message FROM step_logs WHERE instance_id = ?1 ORDER BY ts ASC",
    )
    .bind(instance_id.to_string())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(StepLog {
                block_id: row.get("block_id"),
                ts: parse_ts(row.get::<&str, _>("ts"))?,
                level: row.get("level"),
                message: row.get("message"),
            })
        })
        .collect()
}
