use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::SqliteStorage;

const KV_DELETE_CHUNK_SIZE: usize = 500;

impl SqliteStorage {
    pub(crate) async fn set_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let value =
            serde_json::to_string(value).map_err(|error| StorageError::Query(error.to_string()))?;
        sqlx::query(
            "INSERT INTO shared_agent_knowledge (tenant_id, namespace, key, value, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))
             ON CONFLICT (tenant_id, namespace, key)
             DO UPDATE SET value = ?4, updated_at = datetime('now')",
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
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key, value FROM shared_agent_knowledge
             WHERE tenant_id = ?1 AND namespace = ?2
             ORDER BY updated_at DESC, key ASC LIMIT ?3",
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        rows.into_iter()
            .map(|(key, value)| {
                serde_json::from_str(&value)
                    .map(|value| (key, value))
                    .map_err(|error| StorageError::Query(error.to_string()))
            })
            .collect()
    }

    pub(crate) async fn get_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM shared_agent_knowledge WHERE tenant_id = ?1 AND namespace = ?2 AND key = ?3",
        )
        .bind(tenant_id)
        .bind(namespace)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| StorageError::Query(error.to_string()))?;
        row.map(|(value,)| {
            serde_json::from_str(&value).map_err(|error| StorageError::Query(error.to_string()))
        })
        .transpose()
    }

    pub(crate) async fn delete_shared_knowledge_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "DELETE FROM shared_agent_knowledge
             WHERE tenant_id = ?1 AND namespace = ?2 AND key = ?3",
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
        let id_str = instance_id.into_uuid().to_string();
        let val_str =
            serde_json::to_string(value).map_err(|e| StorageError::Query(e.to_string()))?;
        sqlx::query(
            "INSERT INTO instance_kv_state (instance_id, key, value, updated_at)
             VALUES (?1, ?2, ?3, datetime('now'))
             ON CONFLICT (instance_id, key) DO UPDATE SET value = ?3, updated_at = datetime('now')",
        )
        .bind(&id_str)
        .bind(key)
        .bind(&val_str)
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
        let id_str = instance_id.into_uuid().to_string();
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM instance_kv_state WHERE instance_id = ?1 AND key = ?2",
        )
        .bind(&id_str)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        match row {
            Some((val_str,)) => {
                let v: serde_json::Value = serde_json::from_str(&val_str)
                    .map_err(|e| StorageError::Query(e.to_string()))?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn get_all_instance_kv_impl(
        &self,
        instance_id: InstanceId,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        let id_str = instance_id.into_uuid().to_string();
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key, value FROM instance_kv_state WHERE instance_id = ?1")
                .bind(&id_str)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Query(e.to_string()))?;
        let mut map = std::collections::HashMap::with_capacity(rows.len());
        for (k, v_str) in rows {
            let v: serde_json::Value =
                serde_json::from_str(&v_str).map_err(|e| StorageError::Query(e.to_string()))?;
            map.insert(k, v);
        }
        Ok(map)
    }

    pub(crate) async fn delete_instance_kv_impl(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError> {
        let id_str = instance_id.into_uuid().to_string();
        sqlx::query("DELETE FROM instance_kv_state WHERE instance_id = ?1 AND key = ?2")
            .bind(&id_str)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    pub(crate) async fn delete_instance_kv_batch_impl(
        &self,
        instance_id: InstanceId,
        keys: &[String],
    ) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }
        let instance_id = instance_id.into_uuid().to_string();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        for chunk in keys.chunks(KV_DELETE_CHUNK_SIZE) {
            let mut query =
                sqlx::QueryBuilder::new("DELETE FROM instance_kv_state WHERE instance_id = ");
            query.push_bind(&instance_id);
            query.push(" AND key IN (");
            let mut separated = query.separated(", ");
            for key in chunk {
                separated.push_bind(key);
            }
            separated.push_unseparated(")");
            query
                .build()
                .execute(&mut *tx)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }

    pub(crate) async fn delete_shared_knowledge_batch_impl(
        &self,
        tenant_id: &str,
        namespace: &str,
        keys: &[String],
    ) -> Result<(), StorageError> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        for chunk in keys.chunks(KV_DELETE_CHUNK_SIZE) {
            let mut query =
                sqlx::QueryBuilder::new("DELETE FROM shared_agent_knowledge WHERE tenant_id = ");
            query.push_bind(tenant_id);
            query.push(" AND namespace = ").push_bind(namespace);
            query.push(" AND key IN (");
            let mut separated = query.separated(", ");
            for key in chunk {
                separated.push_bind(key);
            }
            separated.push_unseparated(")");
            query
                .build()
                .execute(&mut *tx)
                .await
                .map_err(|error| StorageError::Query(error.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|error| StorageError::Query(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn oversized_key_list() -> Vec<String> {
        (0..32_768).map(|index| format!("key-{index}")).collect()
    }

    #[tokio::test]
    async fn instance_kv_batch_delete_handles_more_than_sqlite_variable_limit() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let target = InstanceId::new();
        let other = InstanceId::new();
        let keys = oversized_key_list();
        for key in [&keys[0], &keys[32_767]] {
            storage
                .set_instance_kv_impl(target, key, &json!(true))
                .await
                .unwrap();
            storage
                .set_instance_kv_impl(other, key, &json!(true))
                .await
                .unwrap();
        }

        storage
            .delete_instance_kv_batch_impl(target, &keys)
            .await
            .unwrap();

        for key in [&keys[0], &keys[32_767]] {
            assert_eq!(
                storage.get_instance_kv_impl(target, key).await.unwrap(),
                None
            );
            assert_eq!(
                storage.get_instance_kv_impl(other, key).await.unwrap(),
                Some(json!(true))
            );
        }
    }

    #[tokio::test]
    async fn shared_knowledge_batch_delete_handles_more_than_sqlite_variable_limit() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let keys = oversized_key_list();
        for key in [&keys[0], &keys[32_767]] {
            storage
                .set_shared_knowledge_impl("target", "ns", key, &json!(true))
                .await
                .unwrap();
            storage
                .set_shared_knowledge_impl("other", "ns", key, &json!(true))
                .await
                .unwrap();
        }

        storage
            .delete_shared_knowledge_batch_impl("target", "ns", &keys)
            .await
            .unwrap();

        for key in [&keys[0], &keys[32_767]] {
            assert_eq!(
                storage
                    .get_shared_knowledge_impl("target", "ns", key)
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                storage
                    .get_shared_knowledge_impl("other", "ns", key)
                    .await
                    .unwrap(),
                Some(json!(true))
            );
        }
    }

    #[tokio::test]
    async fn instance_kv_batch_delete_rolls_back_prior_chunks_on_error() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let instance_id = InstanceId::new();
        let keys: Vec<String> = (0..=KV_DELETE_CHUNK_SIZE)
            .map(|index| format!("key-{index}"))
            .collect();
        for key in [&keys[0], &keys[KV_DELETE_CHUNK_SIZE]] {
            storage
                .set_instance_kv_impl(instance_id, key, &json!(true))
                .await
                .unwrap();
        }
        sqlx::query(
            "CREATE TRIGGER fail_late_delete BEFORE DELETE ON instance_kv_state
             WHEN OLD.key = 'key-500' BEGIN SELECT RAISE(ABORT, 'late failure'); END",
        )
        .execute(&storage.pool)
        .await
        .unwrap();

        assert!(
            storage
                .delete_instance_kv_batch_impl(instance_id, &keys)
                .await
                .is_err()
        );
        assert_eq!(
            storage
                .get_instance_kv_impl(instance_id, &keys[0])
                .await
                .unwrap(),
            Some(json!(true))
        );
    }
}
