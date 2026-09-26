use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::progress_share::ProgressShare;

use super::PostgresStorage;

const COLUMNS: &str =
    "id, token_hash, tenant_id, instance_id, allowed_fields, created_at, expires_at, revoked_at";

fn row_to_share(row: &sqlx::postgres::PgRow) -> Result<ProgressShare, StorageError> {
    let fields: serde_json::Value = row.try_get("allowed_fields")?;
    Ok(ProgressShare {
        id: row.try_get("id")?,
        token_hash: row.try_get("token_hash")?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        instance_id: InstanceId::from_uuid(row.try_get("instance_id")?),
        allowed_fields: serde_json::from_value(fields).map_err(StorageError::Serialization)?,
        created_at: row.try_get("created_at")?,
        expires_at: row.try_get("expires_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

pub(super) async fn create(store: &PostgresStorage, s: &ProgressShare) -> Result<(), StorageError> {
    sqlx::query(&format!(
        "INSERT INTO progress_shares ({COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    ))
    .bind(s.id)
    .bind(&s.token_hash)
    .bind(s.tenant_id.as_str())
    .bind(s.instance_id.into_uuid())
    .bind(serde_json::to_value(&s.allowed_fields)?)
    .bind(s.created_at)
    .bind(s.expires_at)
    .bind(s.revoked_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_by_hash(
    store: &PostgresStorage,
    token_hash: &str,
) -> Result<Option<ProgressShare>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM progress_shares WHERE token_hash = $1"
    ))
    .bind(token_hash)
    .fetch_optional(&store.pool)
    .await?;
    row.as_ref().map(row_to_share).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    instance_id: InstanceId,
) -> Result<Vec<ProgressShare>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM progress_shares WHERE tenant_id = $1 AND instance_id = $2
         ORDER BY created_at DESC LIMIT 100"
    ))
    .bind(tenant_id.as_str())
    .bind(instance_id.into_uuid())
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_share).collect()
}

pub(super) async fn revoke(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    instance_id: InstanceId,
    share_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let res = sqlx::query(
        r"UPDATE progress_shares SET revoked_at = $4
          WHERE id = $1 AND tenant_id = $2 AND instance_id = $3 AND revoked_at IS NULL",
    )
    .bind(share_id)
    .bind(tenant_id.as_str())
    .bind(instance_id.into_uuid())
    .bind(now)
    .execute(&store.pool)
    .await?;
    Ok(res.rows_affected() == 1)
}
