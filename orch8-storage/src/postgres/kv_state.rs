use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

impl PostgresStorage {
    pub(crate) async fn set_instance_kv_impl(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO instance_kv_state (instance_id, key, value, updated_at)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (instance_id, key) DO UPDATE SET value = $3, updated_at = NOW()",
        )
        .bind(instance_id.0)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    pub(crate) async fn get_instance_kv_impl(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT value FROM instance_kv_state WHERE instance_id = $1 AND key = $2",
        )
        .bind(instance_id.0)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(|(v,)| v))
    }

    pub(crate) async fn get_all_instance_kv_impl(
        &self,
        instance_id: InstanceId,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        let rows: Vec<(String, serde_json::Value)> =
            sqlx::query_as("SELECT key, value FROM instance_kv_state WHERE instance_id = $1")
                .bind(instance_id.0)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.into_iter().collect())
    }

    pub(crate) async fn delete_instance_kv_impl(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM instance_kv_state WHERE instance_id = $1 AND key = $2")
            .bind(instance_id.0)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }
}
