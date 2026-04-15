use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

pub(super) async fn save(
    store: &PostgresStorage,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO externalized_state (id, instance_id, ref_key, payload, created_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (ref_key) DO UPDATE SET payload = $4",
    )
    .bind(Uuid::new_v4())
    .bind(instance_id.0)
    .bind(ref_key)
    .bind(payload)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    ref_key: &str,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT payload FROM externalized_state WHERE ref_key = $1")
            .bind(ref_key)
            .fetch_optional(&store.pool)
            .await?;
    Ok(row.map(|r| r.0))
}

pub(super) async fn delete(store: &PostgresStorage, ref_key: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM externalized_state WHERE ref_key = $1")
        .bind(ref_key)
        .execute(&store.pool)
        .await?;
    Ok(())
}
