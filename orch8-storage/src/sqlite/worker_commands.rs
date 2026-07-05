use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerCommand;

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn row_to_command(row: &sqlx::sqlite::SqliteRow) -> Result<WorkerCommand, StorageError> {
    let payload_str: String = row.get("payload");
    let command_str: String = row.get("command");
    Ok(WorkerCommand {
        id: Uuid::parse_str(row.get::<&str, _>("id"))
            .map_err(|e| StorageError::Query(e.to_string()))?,
        worker_id: row.get("worker_id"),
        command: command_str
            .parse()
            .map_err(|e: String| StorageError::Query(e))?,
        payload: serde_json::from_str(&payload_str)?,
        created_at: parse_ts(row.get::<&str, _>("created_at"))?,
    })
}

pub(super) async fn enqueue(
    storage: &SqliteStorage,
    cmd: &WorkerCommand,
) -> Result<(), StorageError> {
    let payload = serde_json::to_string(&cmd.payload)?;
    sqlx::query(
        "INSERT INTO worker_commands (id, worker_id, command, payload, created_at) VALUES (?1,?2,?3,?4,?5)",
    )
    .bind(cmd.id.to_string())
    .bind(&cmd.worker_id)
    .bind(cmd.command.to_string())
    .bind(&payload)
    .bind(ts(cmd.created_at))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    worker_id: &str,
) -> Result<Vec<WorkerCommand>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, worker_id, command, payload, created_at FROM worker_commands WHERE worker_id = ?1 ORDER BY created_at ASC",
    )
    .bind(worker_id)
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_command).collect()
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_commands WHERE id = ?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}
