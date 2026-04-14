use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, TenantId};

use super::rows::AuditLogRow;
use super::PostgresStorage;

pub(super) async fn append(
    store: &PostgresStorage,
    entry: &orch8_types::audit::AuditLogEntry,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO audit_log (id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(entry.id)
    .bind(entry.instance_id.0)
    .bind(&entry.tenant_id.0)
    .bind(&entry.event_type)
    .bind(&entry.from_state)
    .bind(&entry.to_state)
    .bind(&entry.block_id)
    .bind(&entry.details)
    .bind(entry.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_by_instance(
    store: &PostgresStorage,
    instance_id: InstanceId,
    limit: u32,
) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
    let rows = sqlx::query_as::<_, AuditLogRow>(
        r"SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
          FROM audit_log WHERE instance_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(instance_id.0)
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(AuditLogRow::into_entry).collect())
}

pub(super) async fn list_by_tenant(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    limit: u32,
) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
    let rows = sqlx::query_as::<_, AuditLogRow>(
        r"SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
          FROM audit_log WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(&tenant_id.0)
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(AuditLogRow::into_entry).collect())
}
