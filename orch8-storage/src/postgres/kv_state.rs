use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

impl PostgresStorage {
    pub(crate) async fn set_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO shared_agent_knowledge (tenant_id, namespace, key, value, updated_at)
             VALUES ($1, $2, $3, $4, NOW())
             ON CONFLICT (tenant_id, namespace, key)
             DO UPDATE SET value = $4, updated_at = NOW()",
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn list_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        limit: u32,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT key, value FROM shared_agent_knowledge
             WHERE tenant_id = $1 AND namespace = $2
             ORDER BY updated_at DESC, key ASC LIMIT $3",
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(rows.into_iter().collect())
    }

    pub(crate) async fn delete_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "DELETE FROM shared_agent_knowledge
             WHERE tenant_id = $1 AND namespace = $2 AND key = $3",
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(key)
        .execute(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

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
        .bind(instance_id.into_uuid())
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
        .bind(instance_id.into_uuid())
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
                .bind(instance_id.into_uuid())
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
            .bind(instance_id.into_uuid())
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }
}
