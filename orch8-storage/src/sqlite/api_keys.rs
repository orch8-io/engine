use chrono::{DateTime, Utc};
use orch8_types::api_key::ApiKeyRecord;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::SqliteStorage;
use super::helpers::parse_ts;

pub(super) async fn create(store: &SqliteStorage, key: &ApiKeyRecord) -> Result<(), StorageError> {
    if key.capabilities.is_empty() {
        return Err(StorageError::Constraint(
            "api key must carry at least one capability".into(),
        ));
    }
    let created = key.created_at.to_rfc3339();
    let last_used = key.last_used_at.map(|t| t.to_rfc3339());
    let expires = key.expires_at.map(|t| t.to_rfc3339());
    sqlx::query(
        r"INSERT INTO api_keys (id, tenant_id, name, capabilities_json, key_hash, created_at, last_used_at, expires_at, revoked)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .bind(&key.id)
    .bind(&key.tenant_id)
    .bind(&key.name)
    .bind(serde_json::to_string(&key.capabilities)?)
    .bind(&key.key_hash)
    .bind(&created)
    .bind(&last_used)
    .bind(&expires)
    .bind(key.revoked)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn lookup_by_hash(
    store: &SqliteStorage,
    key_hash: &str,
) -> Result<Option<ApiKeyRecord>, StorageError> {
    let row: Option<ApiKeyRow> = sqlx::query_as(
        r"SELECT id, tenant_id, name, capabilities_json, key_hash, created_at, last_used_at, expires_at, revoked
          FROM api_keys WHERE key_hash = ?1",
    )
    .bind(key_hash)
    .fetch_optional(&store.pool)
    .await?;
    row.map(ApiKeyRow::into_record).transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: &TenantId,
) -> Result<Vec<ApiKeyRecord>, StorageError> {
    let rows: Vec<ApiKeyRow> = sqlx::query_as(
        r"SELECT id, tenant_id, name, capabilities_json, key_hash, created_at, last_used_at, expires_at, revoked
          FROM api_keys WHERE tenant_id = ?1 ORDER BY created_at DESC LIMIT 1000",
    )
    .bind(tenant_id.as_str())
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(ApiKeyRow::into_record)
        .collect::<Result<Vec<_>, _>>()
}

pub(super) async fn touch(
    store: &SqliteStorage,
    id: &str,
    at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE api_keys SET last_used_at = ?1 WHERE id = ?2")
        .bind(at.to_rfc3339())
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn revoke(store: &SqliteStorage, id: &str) -> Result<bool, StorageError> {
    // `RETURNING` hands back the hash so we can evict the auth cache atomically
    // with the write — a revoked key then stops authenticating immediately
    // rather than lingering for the cache TTL.
    let key_hash: Option<String> =
        sqlx::query_scalar("UPDATE api_keys SET revoked = 1 WHERE id = ?1 RETURNING key_hash")
            .bind(id)
            .fetch_optional(&store.pool)
            .await?;
    if let Some(ref hash) = key_hash {
        crate::api_key_cache::invalidate(hash).await;
    }
    Ok(key_hash.is_some())
}

#[derive(sqlx::FromRow)]
struct ApiKeyRow {
    id: String,
    tenant_id: String,
    name: String,
    capabilities_json: String,
    key_hash: String,
    created_at: String,
    last_used_at: Option<String>,
    expires_at: Option<String>,
    revoked: bool,
}

impl ApiKeyRow {
    fn into_record(self) -> Result<ApiKeyRecord, StorageError> {
        // An empty stored list means *no* capabilities (fail closed); it is
        // never widened to the full set on read.
        let capabilities: Vec<orch8_types::api_key::ApiCapability> =
            serde_json::from_str(&self.capabilities_json)?;
        Ok(ApiKeyRecord {
            id: self.id,
            tenant_id: self.tenant_id,
            name: self.name,
            capabilities,
            key_hash: self.key_hash,
            created_at: parse_ts(&self.created_at)?,
            last_used_at: self.last_used_at.as_deref().map(parse_ts).transpose()?,
            expires_at: self.expires_at.as_deref().map(parse_ts).transpose()?,
            revoked: self.revoked,
        })
    }
}

#[cfg(test)]
mod tests {
    use orch8_types::api_key::{ApiCapability, mint_scoped};
    use orch8_types::error::StorageError;

    use super::SqliteStorage;
    use crate::AdminStore as _;

    async fn insert_raw_row(store: &SqliteStorage, key_hash: &str, capabilities_json: &str) {
        sqlx::query(
            "INSERT INTO api_keys (id, tenant_id, name, capabilities_json, key_hash, created_at)
             VALUES (?1, 'acme', 'raw', ?2, ?3, '2026-01-01T00:00:00+00:00')",
        )
        .bind(format!("ak_{key_hash}"))
        .bind(capabilities_json)
        .bind(key_hash)
        .execute(store.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stored_empty_capability_list_reads_back_as_no_capabilities() {
        let store = SqliteStorage::in_memory().await.unwrap();
        let hash = uuid::Uuid::new_v4().simple().to_string();
        insert_raw_row(&store, &hash, "[]").await;

        let record = store.lookup_api_key_by_hash(&hash).await.unwrap().unwrap();

        assert_eq!(record.capabilities, Vec::<ApiCapability>::new());
    }

    #[tokio::test]
    async fn create_api_key_rejects_an_empty_capability_grant() {
        let store = SqliteStorage::in_memory().await.unwrap();
        let minted = mint_scoped("acme", "empty", None, vec![]);

        let err = store.create_api_key(&minted.record).await.unwrap_err();

        assert!(matches!(err, StorageError::Constraint(_)), "{err:?}");
    }

    #[tokio::test]
    async fn legacy_key_without_capabilities_column_gains_all_capabilities_on_upgrade() {
        let path = std::env::temp_dir().join(format!(
            "orch8-legacy-api-keys-{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        {
            let opts = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true);
            let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
            sqlx::query(
                "CREATE TABLE api_keys (
                    id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
                    key_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL, last_used_at TEXT,
                    expires_at TEXT, revoked INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO api_keys (id, tenant_id, key_hash, created_at)
                 VALUES ('ak_legacy', 'acme', 'legacy-hash', '2026-01-01T00:00:00+00:00')",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }

        let store = SqliteStorage::file(path.to_str().unwrap()).await.unwrap();
        let record = store
            .lookup_api_key_by_hash("legacy-hash")
            .await
            .unwrap()
            .unwrap();
        drop(store);
        let _ = std::fs::remove_file(&path);

        assert_eq!(record.capabilities, ApiCapability::all());
    }
}
