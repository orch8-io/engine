use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::worker::WorkerCommand;

use super::PostgresStorage;

fn row_to_command(row: &sqlx::postgres::PgRow) -> Result<WorkerCommand, StorageError> {
    let command_str: String = row.get("command");
    Ok(WorkerCommand {
        id: row.get("id"),
        worker_id: row.get("worker_id"),
        command: command_str
            .parse()
            .map_err(|e: String| StorageError::Query(e))?,
        payload: row.get("payload"),
        created_at: row.get("created_at"),
    })
}

pub(super) async fn enqueue(
    store: &PostgresStorage,
    cmd: &WorkerCommand,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO worker_commands (id, worker_id, command, payload, created_at)
          VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(cmd.id)
    .bind(&cmd.worker_id)
    .bind(cmd.command.to_string())
    .bind(&cmd.payload)
    .bind(cmd.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
    worker_id: &str,
) -> Result<Vec<WorkerCommand>, StorageError> {
    let rows = sqlx::query(
        r"SELECT id, worker_id, command, payload, created_at
          FROM worker_commands WHERE worker_id = $1 ORDER BY created_at ASC",
    )
    .bind(worker_id)
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_command).collect()
}

pub(super) async fn delete(store: &PostgresStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_commands WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}
