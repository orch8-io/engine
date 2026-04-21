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
        .map(serde_json::to_string)
        .transpose()?;
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
    .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: SequenceId,
) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let row = sqlx::query("SELECT * FROM sequences WHERE id = ?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await?;
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
    }?;
    row.map(|r| row_to_sequence(&r)).transpose()
}

pub(super) async fn list_versions(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    namespace: &Namespace,
    name: &str,
) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let rows = sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND deprecated=0 ORDER BY version DESC")
        .bind(&tenant_id.0).bind(&namespace.0).bind(name)
        .fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_sequence).collect()
}

pub(super) async fn list_all(
    storage: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    namespace: Option<&Namespace>,
    limit: u32,
    offset: u32,
) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
    // Dynamic SQL assembly: bindings are positional so a string placeholder is
    // safe and keeps the query plan stable.
    let mut sql = String::from("SELECT * FROM sequences");
    let mut conds: Vec<&'static str> = Vec::new();
    if tenant_id.is_some() {
        conds.push("tenant_id = ?");
    }
    if namespace.is_some() {
        conds.push("namespace = ?");
    }
    if !conds.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conds.join(" AND "));
    }
    sql.push_str(" ORDER BY tenant_id, namespace, name, version DESC LIMIT ? OFFSET ?");

    let mut q = sqlx::query(&sql);
    if let Some(t) = tenant_id {
        q = q.bind(&t.0);
    }
    if let Some(n) = namespace {
        q = q.bind(&n.0);
    }
    q = q.bind(limit as i64).bind(offset as i64);

    let rows = q.fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_sequence).collect()
}

pub(super) async fn deprecate(storage: &SqliteStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET deprecated=1 WHERE id=?1")
        .bind(id.0.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn delete(storage: &SqliteStorage, id: SequenceId) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;

    let id_str = id.0.to_string();

    // Gather instance IDs referencing this sequence.
    let instance_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM task_instances WHERE sequence_id = ?1")
            .bind(&id_str)
            .fetch_all(&mut *tx)
            .await?;

    if !instance_ids.is_empty() {
        // Delete child tables that lack ON DELETE CASCADE. Previously this
        // issued `instance_ids.len() * 5` round-trips — on a sequence with 10k
        // instances that is 50k queries, all under an IMMEDIATE txn holding
        // the write lock. Now we issue exactly one `DELETE ... WHERE
        // instance_id IN (...)` per child table (5 total) regardless of
        // instance count, drastically reducing lock-hold time.
        for table in &[
            "block_outputs",
            "execution_tree",
            "signal_inbox",
            "worker_tasks",
            "externalized_state",
        ] {
            let mut qb =
                sqlx::QueryBuilder::new(format!("DELETE FROM {table} WHERE instance_id IN ("));
            let mut sep = qb.separated(", ");
            for iid in &instance_ids {
                sep.push_bind(iid);
            }
            sep.push_unseparated(")");
            qb.build().execute(&mut *tx).await?;
        }

        sqlx::query("DELETE FROM task_instances WHERE sequence_id = ?1")
            .bind(&id_str)
            .execute(&mut *tx)
            .await?;
    }

    sqlx::query("DELETE FROM cron_schedules WHERE sequence_id = ?1")
        .bind(&id_str)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM sequences WHERE id = ?1")
        .bind(&id_str)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}
