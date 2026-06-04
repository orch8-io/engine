use chrono::{DateTime, Utc};
use orch8_types::api_key::ApiKeyRecord;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    key: &ApiKeyRecord,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO api_keys (id, tenant_id, name, key_hash, created_at, last_used_at, expires_at, revoked)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(&key.id)
    .bind(&key.tenant_id)
    .bind(&key.name)
    .bind(&key.key_hash)
    .bind(key.created_at)
    .bind(key.last_used_at)
    .bind(key.expires_at)
    .bind(key.revoked)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn lookup_by_hash(
    store: &PostgresStorage,
    key_hash: &str,
) -> Result<Option<ApiKeyRecord>, StorageError> {
    let row = sqlx::query_as::<_, ApiKeyRow>(
        r"SELECT id, tenant_id, name, key_hash, created_at, last_used_at, expires_at, revoked
          FROM api_keys WHERE key_hash = $1",
    )
    .bind(key_hash)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(ApiKeyRow::into_record))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: &TenantId,
) -> Result<Vec<ApiKeyRecord>, StorageError> {
    let rows = sqlx::query_as::<_, ApiKeyRow>(
        r"SELECT id, tenant_id, name, key_hash, created_at, last_used_at, expires_at, revoked
          FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1000",
    )
    .bind(tenant_id.as_str())
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(ApiKeyRow::into_record).collect())
}

pub(super) async fn touch(
    store: &PostgresStorage,
    id: &str,
    at: DateTime<Utc>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE api_keys SET last_used_at = $1 WHERE id = $2")
        .bind(at)
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn revoke(store: &PostgresStorage, id: &str) -> Result<bool, StorageError> {
    // `RETURNING` hands back the hash so we can evict the auth cache atomically
    // with the write — a revoked key then stops authenticating immediately
    // rather than lingering for the cache TTL.
    let key_hash: Option<String> =
        sqlx::query_scalar("UPDATE api_keys SET revoked = TRUE WHERE id = $1 RETURNING key_hash")
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
    key_hash: String,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    revoked: bool,
}

impl ApiKeyRow {
    fn into_record(self) -> ApiKeyRecord {
        ApiKeyRecord {
            id: self.id,
            tenant_id: self.tenant_id,
            name: self.name,
            key_hash: self.key_hash,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            expires_at: self.expires_at,
            revoked: self.revoked,
        }
    }
}
