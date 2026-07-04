use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::sequence::SequenceDefinition;

use super::PostgresStorage;
use super::rows::SequenceRow;

pub(super) async fn create(
    store: &PostgresStorage,
    seq: &SequenceDefinition,
) -> Result<(), StorageError> {
    let definition = serde_json::json!({
        "blocks": seq.blocks,
        "interceptors": seq.interceptors,
        "input_schema": seq.input_schema,
        "sla": seq.sla,
        "on_failure": seq.on_failure,
        "on_cancel": seq.on_cancel,
    });
    sqlx::query(
        r"
        INSERT INTO sequences (id, tenant_id, namespace, name, definition, version, deprecated, status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ",
    )
    .bind(seq.id.into_uuid())
    .bind(seq.tenant_id.as_str())
    .bind(seq.namespace.as_str())
    .bind(&seq.name)
    .bind(&definition)
    .bind(seq.version)
    .bind(seq.deprecated)
    .bind(seq.status.to_string())
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
        "SELECT id, tenant_id, namespace, name, definition, version, deprecated, status, created_at FROM sequences WHERE id = $1",
    )
    .bind(id.into_uuid())
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
            r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, status, created_at
               FROM sequences
               WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4",
        )
        .bind(tenant_id.as_str())
        .bind(namespace.as_str())
        .bind(name)
        .bind(v)
        .fetch_optional(&store.pool)
        .await?
    } else {
        sqlx::query_as::<_, SequenceRow>(
            r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, status, created_at
               FROM sequences
               WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND deprecated = false
               ORDER BY version DESC
               LIMIT 1",
        )
        .bind(tenant_id.as_str())
        .bind(namespace.as_str())
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
        r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, status, created_at
          FROM sequences
          WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND deprecated = false
          ORDER BY version DESC",
    )
    .bind(tenant_id.as_str())
    .bind(namespace.as_str())
    .bind(name)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(SequenceRow::into_definition).collect()
}

pub(super) async fn list_all(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    namespace: Option<&Namespace>,
    limit: u32,
    offset: u32,
) -> Result<Vec<SequenceDefinition>, StorageError> {
    // NULL-safe filters: when a filter is None we use "true" so the row always
    // matches. Keeps a single prepared statement rather than six SQL variants.
    let tenant = tenant_id.map(orch8_types::TenantId::as_str);
    let ns = namespace.map(orch8_types::Namespace::as_str);

    let rows = sqlx::query_as::<_, SequenceRow>(
        r"SELECT id, tenant_id, namespace, name, definition, version, deprecated, status, created_at
          FROM sequences
          WHERE ($1::text IS NULL OR tenant_id = $1)
            AND ($2::text IS NULL OR namespace = $2)
          ORDER BY tenant_id, namespace, name, version DESC
          LIMIT $3 OFFSET $4",
    )
    .bind(tenant)
    .bind(ns)
    .bind(i64::from(limit))
    .bind(i64::from(offset))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(SequenceRow::into_definition).collect()
}

pub(super) async fn deprecate(store: &PostgresStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET deprecated = TRUE WHERE id = $1")
        .bind(id.into_uuid())
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn update_status(
    store: &PostgresStorage,
    id: SequenceId,
    status: &str,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET status = $2 WHERE id = $1")
        .bind(id.into_uuid())
        .bind(status)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, id: SequenceId) -> Result<(), StorageError> {
    // Cascade: remove terminal instances (and their FK dependents) before
    // deleting the sequence. Only terminal instances should remain at this
    // point — the API handler rejects deletes if active instances exist.
    let mut tx = store.pool.begin().await?;

    // Gather instance IDs referencing this sequence.
    let instance_ids: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT id FROM task_instances WHERE sequence_id = $1")
            .bind(id.into_uuid())
            .fetch_all(&mut *tx)
            .await?;

    if !instance_ids.is_empty() {
        // Delete child tables that lack ON DELETE CASCADE.
        for table in &[
            "block_outputs",
            "execution_tree",
            "signal_inbox",
            "worker_tasks",
            "externalized_state",
        ] {
            let sql = match *table {
                "block_outputs" => "DELETE FROM block_outputs WHERE instance_id = ANY($1)",
                "execution_tree" => "DELETE FROM execution_tree WHERE instance_id = ANY($1)",
                "signal_inbox" => "DELETE FROM signal_inbox WHERE instance_id = ANY($1)",
                "worker_tasks" => "DELETE FROM worker_tasks WHERE instance_id = ANY($1)",
                "externalized_state" => {
                    "DELETE FROM externalized_state WHERE instance_id = ANY($1)"
                }
                _ => continue,
            };
            sqlx::query(sql)
                .bind(&instance_ids)
                .execute(&mut *tx)
                .await?;
        }

        sqlx::query("DELETE FROM task_instances WHERE sequence_id = $1")
            .bind(id.into_uuid())
            .execute(&mut *tx)
            .await?;
    }

    // Also remove cron schedules referencing this sequence.
    sqlx::query("DELETE FROM cron_schedules WHERE sequence_id = $1")
        .bind(id.into_uuid())
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM sequences WHERE id = $1")
        .bind(id.into_uuid())
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}
