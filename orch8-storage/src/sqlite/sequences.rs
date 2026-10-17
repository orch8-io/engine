use std::collections::HashSet;

use orch8_types::error::StorageError;
use orch8_types::ids::*;

use super::SqliteStorage;
use super::helpers::{row_to_sequence, ts};

/// Insert a sequence row using an already-open connection/transaction. Shared
/// by [`create`] (its own transaction) and [`replace`] (same transaction as
/// the preceding delete, for atomicity).
async fn create_on(
    conn: &mut sqlx::SqliteConnection,
    seq: &orch8_types::sequence::SequenceDefinition,
) -> Result<(), StorageError> {
    let blocks = serde_json::to_string(&seq.blocks)?;
    let interceptors = seq
        .interceptors
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let input_schema = seq
        .input_schema
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let sla = seq.sla.as_ref().map(serde_json::to_string).transpose()?;
    let on_failure = seq
        .on_failure
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let on_cancel = seq
        .on_cancel
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let embed = seq.embed.as_ref().map(serde_json::to_string).transpose()?;
    let placement = seq
        .placement
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    sqlx::query(
        "INSERT INTO sequences (id, tenant_id, namespace, name, version, deprecated, status, blocks, interceptors, input_schema, sla, on_failure, on_cancel, created_at, sub_tenant, embed, placement) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
    )
    .bind(seq.id.into_uuid().to_string())
    .bind(seq.tenant_id.as_str())
    .bind(seq.namespace.as_str())
    .bind(&seq.name)
    .bind(seq.version)
    .bind(seq.deprecated as i32)
    .bind(seq.status.to_string())
    .bind(&blocks)
    .bind(&interceptors)
    .bind(&input_schema)
    .bind(&sla)
    .bind(&on_failure)
    .bind(&on_cancel)
    .bind(ts(seq.created_at))
    .bind(seq.sub_tenant.as_deref())
    .bind(&embed)
    .bind(&placement)
    .execute(conn).await?;
    Ok(())
}

pub(super) async fn create(
    storage: &SqliteStorage,
    seq: &orch8_types::sequence::SequenceDefinition,
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    create_on(&mut tx, seq).await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: SequenceId,
) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let row = sqlx::query("SELECT * FROM sequences WHERE id = ?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await?;
    row.map(|r| row_to_sequence(&r)).transpose()
}

pub(super) async fn get_many(
    storage: &SqliteStorage,
    ids: &[SequenceId],
) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = HashSet::new();
    let unique_ids = ids
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    let mut tx = storage.pool.begin().await?;
    for chunk in unique_ids.chunks(500) {
        let mut qb = sqlx::QueryBuilder::new("SELECT * FROM sequences WHERE id IN (");
        let mut separated = qb.separated(",");
        for id in chunk {
            separated.push_bind(id.to_string());
        }
        separated.push_unseparated(")");
        let rows = qb.build().fetch_all(&mut *tx).await?;
        result.extend(
            rows.iter()
                .map(row_to_sequence)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    tx.commit().await?;
    Ok(result)
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
            .bind(tenant_id.as_str()).bind(namespace.as_str()).bind(name).bind(v)
            .fetch_optional(&storage.pool).await
    } else {
        sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND deprecated=0 ORDER BY version DESC LIMIT 1")
            .bind(tenant_id.as_str()).bind(namespace.as_str()).bind(name)
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
        .bind(tenant_id.as_str()).bind(namespace.as_str()).bind(name)
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
    let mut qb = sqlx::QueryBuilder::new("SELECT * FROM sequences WHERE 1=1");
    if let Some(t) = tenant_id {
        qb.push(" AND tenant_id = ");
        qb.push_bind(t.as_str().to_owned());
    }
    if let Some(n) = namespace {
        qb.push(" AND namespace = ");
        qb.push_bind(n.as_str().to_owned());
    }
    qb.push(" ORDER BY tenant_id, namespace, name, version DESC LIMIT ");
    qb.push_bind(limit as i64);
    qb.push(" OFFSET ");
    qb.push_bind(offset as i64);

    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_sequence).collect()
}

/// Latest non-deprecated version per `(namespace, name)` visible to an embed
/// sub-tenant (EM-02), filtered in SQL.
pub(super) async fn list_embed_visible(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    sub_tenant: &str,
    tenant_level: &crate::EmbedTenantLevel,
    limit: u32,
) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("SELECT s.* FROM sequences s WHERE s.tenant_id = ");
    qb.push_bind(tenant_id.as_str().to_owned());
    qb.push(" AND s.deprecated = 0 AND (s.sub_tenant = ");
    qb.push_bind(sub_tenant.to_owned());
    qb.push(" OR (s.sub_tenant IS NULL AND ((s.namespace = ");
    qb.push_bind(orch8_types::sub_tenant::GALLERY_NAMESPACE);
    qb.push(" AND json_extract(s.embed, '$.gallery') = 1)");
    match tenant_level {
        crate::EmbedTenantLevel::None => {}
        crate::EmbedTenantLevel::All => {
            qb.push(" OR 1=1");
        }
        crate::EmbedTenantLevel::Names(pairs) => {
            for (ns, name) in pairs {
                qb.push(" OR (s.namespace = ");
                qb.push_bind(ns.clone());
                qb.push(" AND s.name = ");
                qb.push_bind(name.clone());
                qb.push(")");
            }
        }
    }
    qb.push("))) AND s.version = (SELECT MAX(v.version) FROM sequences v WHERE v.tenant_id = s.tenant_id \
             AND v.namespace = s.namespace AND v.name = s.name AND v.deprecated = 0 \
             AND (v.sub_tenant IS s.sub_tenant)) \
             ORDER BY s.namespace, s.name LIMIT ");
    qb.push_bind(i64::from(limit));
    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_sequence).collect()
}

pub(super) async fn count_sub_tenant_names(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    sub_tenant: &str,
) -> Result<u64, StorageError> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (SELECT DISTINCT namespace, name FROM sequences \
         WHERE tenant_id = ?1 AND sub_tenant = ?2)",
    )
    .bind(tenant_id.as_str())
    .bind(sub_tenant)
    .fetch_one(&storage.pool)
    .await?;
    Ok(u64::try_from(n).unwrap_or(0))
}

pub(super) async fn deprecate(storage: &SqliteStorage, id: SequenceId) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET deprecated=1 WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn update_status(
    storage: &SqliteStorage,
    id: SequenceId,
    status: &str,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sequences SET status=?2 WHERE id=?1")
        .bind(id.to_string())
        .bind(status)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

/// Delete a sequence row (and its dependent instances/cron schedules) using
/// an already-open connection/transaction. Shared by [`delete`] (its own
/// transaction) and [`replace`] (same transaction as the following insert,
/// for atomicity).
async fn delete_on(conn: &mut sqlx::SqliteConnection, id: SequenceId) -> Result<(), StorageError> {
    let id_str = id.to_string();

    // Refuse while any non-terminal instance still runs this sequence
    // (L3-03): a caller that skipped the API's active-instance check must
    // never cascade live work away. Only terminal instances are removed below.
    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_instances \
         WHERE sequence_id = ?1 AND state NOT IN ('completed', 'failed', 'cancelled')",
    )
    .bind(&id_str)
    .fetch_one(&mut *conn)
    .await?;
    if active > 0 {
        return Err(StorageError::Conflict(format!(
            "sequence {id} still has {active} non-terminal instance(s)"
        )));
    }

    // Select dependent IDs inside SQLite instead of expanding one bind per
    // instance. The query stays within the bind limit regardless of run count.
    // (Every remaining instance of the sequence is terminal, per the check.)
    for sql in [
        "DELETE FROM block_outputs WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM execution_tree WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM signal_inbox WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM worker_tasks WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM externalized_state WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM instance_kv_state WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM injected_blocks WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        // No FK/cascade on SQLite for these; same set as
        // `delete_terminal_instances`.
        "DELETE FROM step_logs WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM audit_log WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM usage_events WHERE instance_id IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
        "DELETE FROM emit_event_dedupe WHERE scope_kind = 'parent' AND scope_value IN (SELECT id FROM task_instances WHERE sequence_id = ?1)",
    ] {
        sqlx::query(sql).bind(&id_str).execute(&mut *conn).await?;
    }

    sqlx::query("DELETE FROM task_instances WHERE sequence_id = ?1")
        .bind(&id_str)
        .execute(&mut *conn)
        .await?;

    sqlx::query("DELETE FROM cron_schedules WHERE sequence_id = ?1")
        .bind(&id_str)
        .execute(&mut *conn)
        .await?;

    sqlx::query("DELETE FROM sequences WHERE id = ?1")
        .bind(&id_str)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

pub(super) async fn delete(storage: &SqliteStorage, id: SequenceId) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    delete_on(&mut tx, id).await?;
    tx.commit().await?;
    Ok(())
}

/// Atomically replace a sequence: delete `old_id` and insert `new` as a
/// single transaction. Used when upserting a same-name sequence with a new
/// definition (e.g. mobile sync's `upsert_sequence`) so a mid-operation
/// failure can't leave the old row deleted with the new one never created —
/// see M-18 in `docs/DEEP_REVIEW_2026-07.md`.
pub(super) async fn replace(
    storage: &SqliteStorage,
    old_id: SequenceId,
    new: &orch8_types::sequence::SequenceDefinition,
) -> Result<(), StorageError> {
    let mut tx = storage.pool.begin().await?;
    delete_on(&mut tx, old_id).await?;
    create_on(&mut tx, new).await?;
    tx.commit().await?;
    Ok(())
}
