use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::progress_share::ProgressShare;

use super::SqliteStorage;
use super::approval_tokens::sortable_ts;
use super::helpers::{parse_ts, parse_ts_opt};

fn parse_uuid(s: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(s).map_err(|e| StorageError::Query(format!("invalid UUID '{s}': {e}")))
}

fn row_to_share(row: &sqlx::sqlite::SqliteRow) -> Result<ProgressShare, StorageError> {
    Ok(ProgressShare {
        id: parse_uuid(&row.try_get::<String, _>("id")?)?,
        token_hash: row.try_get("token_hash")?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        instance_id: InstanceId::from_uuid(parse_uuid(&row.try_get::<String, _>("instance_id")?)?),
        allowed_fields: serde_json::from_str(&row.try_get::<String, _>("allowed_fields")?)
            .map_err(StorageError::Serialization)?,
        created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        expires_at: parse_ts(&row.try_get::<String, _>("expires_at")?)?,
        revoked_at: parse_ts_opt(row.try_get("revoked_at")?)?,
    })
}

pub(super) async fn create(store: &SqliteStorage, s: &ProgressShare) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO progress_shares
          (id, token_hash, tenant_id, instance_id, allowed_fields, created_at, expires_at, revoked_at)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
    )
    .bind(s.id.to_string())
    .bind(&s.token_hash)
    .bind(s.tenant_id.as_str())
    .bind(s.instance_id.into_uuid().to_string())
    .bind(serde_json::to_string(&s.allowed_fields)?)
    .bind(sortable_ts(s.created_at))
    .bind(sortable_ts(s.expires_at))
    .bind(s.revoked_at.map(sortable_ts))
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_by_hash(
    store: &SqliteStorage,
    token_hash: &str,
) -> Result<Option<ProgressShare>, StorageError> {
    let row = sqlx::query("SELECT * FROM progress_shares WHERE token_hash = ?1")
        .bind(token_hash)
        .fetch_optional(&store.pool)
        .await?;
    row.as_ref().map(row_to_share).transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: &TenantId,
    instance_id: InstanceId,
) -> Result<Vec<ProgressShare>, StorageError> {
    let rows = sqlx::query(
        r"SELECT * FROM progress_shares WHERE tenant_id = ?1 AND instance_id = ?2
          ORDER BY created_at DESC LIMIT 100",
    )
    .bind(tenant_id.as_str())
    .bind(instance_id.into_uuid().to_string())
    .fetch_all(&store.pool)
    .await?;
    rows.iter().map(row_to_share).collect()
}

pub(super) async fn revoke(
    store: &SqliteStorage,
    tenant_id: &TenantId,
    instance_id: InstanceId,
    share_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let res = sqlx::query(
        r"UPDATE progress_shares SET revoked_at = ?4
          WHERE id = ?1 AND tenant_id = ?2 AND instance_id = ?3 AND revoked_at IS NULL",
    )
    .bind(share_id.to_string())
    .bind(tenant_id.as_str())
    .bind(instance_id.into_uuid().to_string())
    .bind(sortable_ts(now))
    .execute(&store.pool)
    .await?;
    Ok(res.rows_affected() == 1)
}
