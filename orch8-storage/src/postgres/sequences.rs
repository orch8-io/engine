use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::sequence::SequenceDefinition;

use super::rows::SequenceRow;
use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    seq: &SequenceDefinition,
) -> Result<(), StorageError> {
    let definition = serde_json::json!({
        "blocks": seq.blocks,
        "interceptors": seq.interceptors,
    });
    sqlx::query(
        r"
        INSERT INTO sequences (id, tenant_id, namespace, name, definition, version, deprecated, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ",
    )
    .bind(seq.id.0)
    .bind(&seq.tenant_id.0)
    .bind(&seq.namespace.0)
    .bind(&seq.name)
    .bind(&definition)
    .bind(seq.version)
    .bind(seq.deprecated)
    .bind(seq.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: SequenceId,
) -> Result<Option<SequenceDefinition>, StorageError> {
    let row = sqlx::query_as::<_, SequenceRow>(
        "SELECT id, tenant_id, namespace, name, definition, version, deprecated, created_at FROM sequences WHERE id = $1",
    )
    .bind(id.0)
    .fetch_optional(&store.pool)
    .await?;
    row.map(SequenceRow::into_definition).transpose()
}

pub(super) async fn get_by_name(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    namespace: &Namespace,
    name: &str,
    version: Option<i32>,
) -> Result<Option<SequenceDefinition>, StorageError> {
    let row = if let Some(v) = version {
        sqlx::query_as::<_, SequenceRow>(
            r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, created_at
               FROM sequences
               WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4",
        )
        .bind(&tenant_id.0)
        .bind(&namespace.0)
        .bind(name)
        .bind(v)
        .fetch_optional(&store.pool)
        .await?
    } else {
        sqlx::query_as::<_, SequenceRow>(
            r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, created_at
               FROM sequences
               WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND deprecated = false
               ORDER BY version DESC
               LIMIT 1",
        )
        .bind(&tenant_id.0)
        .bind(&namespace.0)
        .bind(name)
        .fetch_optional(&store.pool)
        .await?
    };
    row.map(SequenceRow::into_definition).transpose()
}

pub(super) async fn list_versions(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    namespace: &Namespace,
    name: &str,
) -> Result<Vec<SequenceDefinition>, StorageError> {
    let rows = sqlx::query_as::<_, SequenceRow>(
        r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, created_at
          FROM sequences
          WHERE tenant_id = $1 AND namespace = $2 AND name = $3
          ORDER BY version DESC",
    )
    .bind(&tenant_id.0)
    .bind(&namespace.0)
    .bind(name)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(SequenceRow::into_definition).collect()
}

pub(super) async fn deprecate(store: &PostgresStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET deprecated = TRUE WHERE id = $1")
        .bind(id.0)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM sequences WHERE id = $1")
        .bind(id.0)
        .execute(&store.pool)
        .await?;
    Ok(())
}
