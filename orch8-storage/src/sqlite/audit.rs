use orch8_types::audit::{AuditLogEntry, ChangeCursor};
use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::SqliteStorage;
use super::helpers::{row_to_audit, ts};

pub(super) async fn append(
    storage: &SqliteStorage,
    entry: &AuditLogEntry,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO audit_log (id,instance_id,tenant_id,event_type,from_state,to_state,block_id,details,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)")
        .bind(entry.id.to_string())
        .bind(entry.instance_id.into_uuid().to_string())
        .bind(entry.tenant_id.as_str())
        .bind(&entry.event_type)
        .bind(&entry.from_state)
        .bind(&entry.to_state)
        .bind(&entry.block_id)
        .bind(serde_json::to_string(&entry.details)?)
        .bind(ts(entry.created_at))
        .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn list_by_instance(
    storage: &SqliteStorage,
    instance_id: InstanceId,
    limit: u32,
) -> Result<Vec<AuditLogEntry>, StorageError> {
    let rows = sqlx::query(
        "SELECT * FROM audit_log WHERE instance_id=?1 ORDER BY created_at DESC LIMIT ?2",
    )
    .bind(instance_id.into_uuid().to_string())
    .bind(limit as i64)
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_audit).collect()
}

pub(super) async fn list_by_tenant(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    limit: u32,
) -> Result<Vec<AuditLogEntry>, StorageError> {
    let rows =
        sqlx::query("SELECT * FROM audit_log WHERE tenant_id=?1 ORDER BY created_at DESC LIMIT ?2")
            .bind(tenant_id.as_str())
            .bind(limit as i64)
            .fetch_all(&storage.pool)
            .await?;
    rows.iter().map(row_to_audit).collect()
}

pub(super) async fn list_changes(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    after: Option<ChangeCursor>,
    limit: u32,
) -> Result<Vec<AuditLogEntry>, StorageError> {
    let rows = if let Some(cursor) = after {
        sqlx::query(
            "SELECT * FROM audit_log WHERE tenant_id=?1 AND (created_at>?2 OR (created_at=?2 AND id>?3)) ORDER BY created_at ASC, id ASC LIMIT ?4",
        )
        .bind(tenant_id.as_str())
        .bind(ts(cursor.created_at))
        .bind(cursor.id.to_string())
        .bind(i64::from(limit))
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query(
            "SELECT * FROM audit_log WHERE tenant_id=?1 ORDER BY created_at ASC, id ASC LIMIT ?2",
        )
        .bind(tenant_id.as_str())
        .bind(i64::from(limit))
        .fetch_all(&storage.pool)
        .await?
    };
    rows.iter().map(row_to_audit).collect()
}
