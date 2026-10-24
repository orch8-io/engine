//! Atomic admission for the legacy, single mobile-tenant API. Scoped requests
//! have a separate preparation store and must not enter this path.
use super::MobileStorage;
use crate::error::MobileError;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::instance::TaskInstance;

impl MobileStorage {
    pub(crate) async fn active_dedup_instance(
        &self,
        key: &str,
    ) -> Result<Option<String>, MobileError> {
        Ok(sqlx::query_scalar(
            "SELECT d.instance_id FROM mobile_dedup d JOIN task_instances i ON i.id = d.instance_id
             WHERE d.dedup_key = ?1 AND i.tenant_id = ?2
             AND i.state IN ('scheduled','running','waiting','paused')",
        )
        .bind(key)
        .bind(crate::mobile_tenant_id().as_str())
        .fetch_optional(self.pool())
        .await
        .map_err(orch8_types::error::StorageError::from)?)
    }

    pub(crate) async fn start_legacy_instance(
        &self,
        mut instance: TaskInstance,
        key: Option<&str>,
        max_active: u32,
    ) -> Result<String, MobileError> {
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(orch8_types::error::StorageError::from)?;
        // Recheck after acquiring the writer: another engine or pool may have
        // admitted this key while this caller validated its workflow/input.
        if let Some(key) = key {
            let existing: Option<String> = sqlx::query_scalar(
                "SELECT d.instance_id FROM mobile_dedup d JOIN task_instances i ON i.id = d.instance_id
                 WHERE d.dedup_key = ?1 AND i.tenant_id = ?2
                 AND i.state IN ('scheduled','running','waiting','paused')",
            )
            .bind(key).bind(instance.tenant_id.as_str())
            .fetch_optional(&mut *tx).await.map_err(orch8_types::error::StorageError::from)?;
            if let Some(id) = existing {
                tx.commit()
                    .await
                    .map_err(orch8_types::error::StorageError::from)?;
                return Ok(id);
            }
            // Recover an active native instance whose mapping was lost by an
            // older client. Terminal instances keep their historical native
            // key; a fresh run gets a private storage key below.
            if !key.is_empty() {
                let previous: Option<(String, String)> = sqlx::query_as(
                    "SELECT id, state FROM task_instances WHERE tenant_id = ?1 AND idempotency_key = ?2",
                )
                .bind(instance.tenant_id.as_str()).bind(key)
                .fetch_optional(&mut *tx).await.map_err(orch8_types::error::StorageError::from)?;
                if let Some((id, state)) = previous {
                    if matches!(
                        state.as_str(),
                        "scheduled" | "running" | "waiting" | "paused"
                    ) {
                        set_mapping(&mut tx, key, &id).await?;
                        tx.commit()
                            .await
                            .map_err(orch8_types::error::StorageError::from)?;
                        return Ok(id);
                    }
                    if !matches!(state.as_str(), "completed" | "failed" | "cancelled") {
                        return Err(MobileError::Storage {
                            message: "invalid previous mobile instance state".into(),
                        });
                    }
                    instance.idempotency_key =
                        Some(format!("{key}#rerun-{}", uuid::Uuid::now_v7()));
                }
            }
        }
        // Keep the legacy database-wide ceiling and include paused runs:
        // resuming one must not create capacity that was already consumed.
        let active: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_instances WHERE state IN ('scheduled','running','waiting','paused')",
        ).fetch_one(&mut *tx).await.map_err(orch8_types::error::StorageError::from)?;
        if u64::try_from(active).unwrap_or(u64::MAX) >= u64::from(max_active) {
            return Err(MobileError::ResourceLimit {
                message: format!("max concurrent instances ({max_active}) reached"),
            });
        }
        SqliteStorage::create_instance_in_transaction(&mut tx, &instance).await?;
        let id = instance.id.to_string();
        if let Some(key) = key {
            set_mapping(&mut tx, key, &id).await?;
        }
        tx.commit()
            .await
            .map_err(orch8_types::error::StorageError::from)?;
        Ok(id)
    }
}

async fn set_mapping(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    key: &str,
    id: &str,
) -> Result<(), MobileError> {
    sqlx::query(
        "INSERT INTO mobile_dedup(dedup_key, instance_id, created_at) VALUES (?1,?2,?3)
         ON CONFLICT(dedup_key) DO UPDATE SET instance_id = excluded.instance_id, created_at = excluded.created_at",
    ).bind(key).bind(id).bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut **tx).await.map_err(orch8_types::error::StorageError::from)?;
    Ok(())
}
