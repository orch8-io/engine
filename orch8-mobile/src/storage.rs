//! Mobile-specific storage operations layered on top of `SqliteStorage`.
//!
//! These tables are created by the bundled schema in `orch8-storage` and are
//! idempotent (`IF NOT EXISTS`). Only the mobile SDK reads/writes them.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::error::StorageError;

/// Mobile-specific storage wrapper.
pub struct MobileStorage {
    inner: Arc<SqliteStorage>,
}

#[allow(dead_code)]
impl MobileStorage {
    pub fn new(inner: Arc<SqliteStorage>) -> Self {
        Self { inner }
    }

    fn pool(&self) -> &sqlx::SqlitePool {
        self.inner.pool()
    }

    // ── Telemetry ──

    /// Append a telemetry event to the local buffer.
    pub async fn append_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
        )
        .bind(event_type)
        .bind(payload)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Read up to `limit` telemetry events, oldest first.
    pub async fn read_telemetry_events(
        &self,
        limit: u32,
    ) -> Result<Vec<TelemetryEvent>, StorageError> {
        let rows = sqlx::query_as::<_, TelemetryEventRow>(
            "SELECT id, event_type, payload, created_at FROM telemetry_events ORDER BY id ASC LIMIT ?1",
        )
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Delete telemetry events by their row IDs.
    pub async fn delete_telemetry_events(&self, ids: &[i64]) -> Result<u64, StorageError> {
        if ids.is_empty() {
            return Ok(0);
        }
        // SQLite doesn't support binding arrays directly; build an IN clause safely using QueryBuilder.
        let mut qb = sqlx::QueryBuilder::new("DELETE FROM telemetry_events WHERE id IN (");
        let mut separated = qb.separated(",");
        for id in ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let result = qb.build().execute(self.pool()).await?;
        Ok(result.rows_affected())
    }

    /// Count telemetry events in the buffer.
    pub async fn count_telemetry_events(&self) -> Result<u64, StorageError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(self.pool())
            .await?;
        #[allow(clippy::cast_sign_loss)]
        Ok(count as u64)
    }

    // ── Sync Metadata ──

    pub async fn get_sync_metadata(&self, key: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM sync_metadata WHERE key = ?1")
                .bind(key)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_sync_metadata(&self, key: &str, value: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO sync_metadata (key, value, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        )
        .bind(key)
        .bind(value)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    // ── Trusted Keys ──

    pub async fn get_trusted_key(&self, key_id: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT public_key FROM trusted_keys WHERE key_id = ?1")
                .bind(key_id)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn list_trusted_keys(&self) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT key_id, public_key FROM trusted_keys")
                .fetch_all(self.pool())
                .await?;
        Ok(rows)
    }

    pub async fn upsert_trusted_key(
        &self,
        key_id: &str,
        public_key: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO trusted_keys (key_id, public_key, trusted_since) VALUES (?1, ?2, ?3) \
             ON CONFLICT(key_id) DO UPDATE SET public_key = excluded.public_key, trusted_since = excluded.trusted_since",
        )
        .bind(key_id)
        .bind(public_key)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn delete_trusted_key(&self, key_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM trusted_keys WHERE key_id = ?1")
            .bind(key_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    // ── Mobile Dedup (persistent) ──

    pub async fn get_dedup_instance(&self, key: &str) -> Result<Option<String>, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT instance_id FROM mobile_dedup WHERE dedup_key = ?1")
                .bind(key)
                .fetch_optional(self.pool())
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_dedup(&self, key: &str, instance_id: &str) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_dedup (dedup_key, instance_id, created_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(dedup_key) DO UPDATE SET instance_id = excluded.instance_id",
        )
        .bind(key)
        .bind(instance_id)
        .bind(Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    pub async fn remove_dedup(&self, instance_id: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM mobile_dedup WHERE instance_id = ?1")
            .bind(instance_id)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// List all dedup entries for hydration on engine startup.
    pub async fn list_all_dedup(&self) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT dedup_key, instance_id FROM mobile_dedup")
                .fetch_all(self.pool())
                .await?;
        Ok(rows)
    }
}

/// A telemetry event stored in the local `SQLite` buffer.
#[derive(Debug, Clone)]
pub struct TelemetryEvent {
    pub id: i64,
    pub event_type: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct TelemetryEventRow {
    id: i64,
    event_type: String,
    payload: String,
    created_at: String,
}

impl From<TelemetryEventRow> for TelemetryEvent {
    fn from(row: TelemetryEventRow) -> Self {
        Self {
            id: row.id,
            event_type: row.event_type,
            payload: row.payload,
            created_at: row.created_at.parse().unwrap_or_else(|_| Utc::now()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (MobileStorage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = SqliteStorage::file_mobile(&path).await.unwrap();
        (MobileStorage::new(Arc::new(sqlite)), dir)
    }

    #[tokio::test]
    async fn telemetry_roundtrip() {
        let (storage, _dir) = setup().await;

        storage
            .append_telemetry_event("SyncCompleted", r#"{"version":1}"#)
            .await
            .unwrap();

        let events = storage.read_telemetry_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "SyncCompleted");
        assert_eq!(events[0].payload, r#"{"version":1}"#);

        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 1);

        storage
            .delete_telemetry_events(&[events[0].id])
            .await
            .unwrap();
        let count = storage.count_telemetry_events().await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sync_metadata_roundtrip() {
        let (storage, _dir) = setup().await;

        assert!(storage
            .get_sync_metadata("last_sync_ts")
            .await
            .unwrap()
            .is_none());

        storage
            .set_sync_metadata("last_sync_ts", "2026-01-01T00:00:00Z")
            .await
            .unwrap();

        let val = storage.get_sync_metadata("last_sync_ts").await.unwrap();
        assert_eq!(val, Some("2026-01-01T00:00:00Z".to_string()));
    }

    #[tokio::test]
    async fn trusted_keys_roundtrip() {
        let (storage, _dir) = setup().await;

        storage.upsert_trusted_key("key1", "pubkey1").await.unwrap();

        let key = storage.get_trusted_key("key1").await.unwrap();
        assert_eq!(key, Some("pubkey1".to_string()));

        let keys = storage.list_trusted_keys().await.unwrap();
        assert_eq!(keys.len(), 1);

        storage.delete_trusted_key("key1").await.unwrap();
        let keys = storage.list_trusted_keys().await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn dedup_roundtrip() {
        let (storage, _dir) = setup().await;

        assert!(storage.get_dedup_instance("dk1").await.unwrap().is_none());

        storage.set_dedup("dk1", "inst-1").await.unwrap();
        let id = storage.get_dedup_instance("dk1").await.unwrap();
        assert_eq!(id, Some("inst-1".to_string()));

        storage.remove_dedup("inst-1").await.unwrap();
        assert!(storage.get_dedup_instance("dk1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dedup_list_all_returns_all_entries() {
        let (storage, _dir) = setup().await;

        storage.set_dedup("dk1", "inst-1").await.unwrap();
        storage.set_dedup("dk2", "inst-2").await.unwrap();

        let all = storage.list_all_dedup().await.unwrap();
        assert_eq!(all.len(), 2);
        let map: std::collections::HashMap<_, _> = all.into_iter().collect();
        assert_eq!(map.get("dk1"), Some(&"inst-1".to_string()));
        assert_eq!(map.get("dk2"), Some(&"inst-2".to_string()));
    }

    #[tokio::test]
    async fn telemetry_delete_empty_is_noop() {
        let (storage, _dir) = setup().await;
        let deleted = storage.delete_telemetry_events(&[]).await.unwrap();
        assert_eq!(deleted, 0);
    }

    #[tokio::test]
    async fn telemetry_oldest_first_ordering() {
        let (storage, _dir) = setup().await;

        storage.append_telemetry_event("A", "1").await.unwrap();
        storage.append_telemetry_event("B", "2").await.unwrap();
        storage.append_telemetry_event("C", "3").await.unwrap();

        let events = storage.read_telemetry_events(2).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "A");
        assert_eq!(events[1].event_type, "B");
    }
}
