use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::helpers::{row_to_sequence, ts};
use super::SqliteStorage;

pub(super) async fn create(
    storage: &SqliteStorage,
    seq: &orch8_types::sequence::SequenceDefinition,
) -> Result<(), StorageError> {
    let blocks = serde_json::to_string(&seq.blocks)?;
    let interceptors = seq
        .interceptors
        .as_ref()
        .map(|i| serde_json::to_string(i).unwrap_or_default());
    sqlx::query(
        "INSERT INTO sequences (id, tenant_id, namespace, name, version, deprecated, blocks, interceptors, created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
    )
    .bind(seq.id.0.to_string())
    .bind(&seq.tenant_id.0)
    .bind(&seq.namespace.0)
    .bind(&seq.name)
    .bind(seq.version)
    .bind(seq.deprecated as i32)
    .bind(&blocks)
    .bind(&interceptors)
    .bind(ts(seq.created_at))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: SequenceId,
) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let row = sqlx::query("SELECT * FROM sequences WHERE id = ?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.map(|r| row_to_sequence(&r)).transpose()
}

pub(super) async fn get_by_name(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    namespace: &Namespace,
    name: &str,
    version: Option<i32>,
) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let row = if let Some(v) = version {
        sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND version=?4")
            .bind(&tenant_id.0).bind(&namespace.0).bind(name).bind(v)
            .fetch_optional(&storage.pool).await
    } else {
        sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND deprecated=0 ORDER BY version DESC LIMIT 1")
            .bind(&tenant_id.0).bind(&namespace.0).bind(name)
            .fetch_optional(&storage.pool).await
    }
    .map_err(|e| StorageError::Query(e.to_string()))?;
    row.map(|r| row_to_sequence(&r)).transpose()
}

pub(super) async fn list_versions(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    namespace: &Namespace,
    name: &str,
) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let rows = sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 ORDER BY version DESC")
        .bind(&tenant_id.0).bind(&namespace.0).bind(name)
        .fetch_all(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_sequence).collect()
}

pub(super) async fn deprecate(storage: &SqliteStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET deprecated=1 WHERE id=?1")
        .bind(id.0.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
