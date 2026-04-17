use chrono::{DateTime, Utc};
use sqlx::Row;
use tracing::instrument;

use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, TaskInstance};

use super::helpers::{apply_filter_sql, row_to_instance, ts};
use super::SqliteStorage;

#[instrument(skip(storage, i), fields(instance_id = %i.id, tenant = %i.tenant_id))]
pub(super) async fn create(storage: &SqliteStorage, i: &TaskInstance) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
    )
    .bind(i.id.0.to_string())
    .bind(i.sequence_id.0.to_string())
    .bind(&i.tenant_id.0)
    .bind(&i.namespace.0)
    .bind(i.state.to_string())
    .bind(i.next_fire_at.map(ts))
    .bind(i.priority as i16)
    .bind(&i.timezone)
    .bind(serde_json::to_string(&i.metadata)?)
    .bind(serde_json::to_string(&i.context)?)
    .bind(&i.concurrency_key)
    .bind(i.max_concurrency)
    .bind(&i.idempotency_key)
    .bind(i.session_id.map(|u| u.to_string()))
    .bind(i.parent_instance_id.map(|u| u.0.to_string()))
    .bind(ts(i.created_at))
    .bind(ts(i.updated_at))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn create_batch(
    storage: &SqliteStorage,
    instances: &[TaskInstance],
) -> Result<u64, StorageError> {
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    for i in instances {
        sqlx::query(
            "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
        )
        .bind(i.id.0.to_string())
        .bind(i.sequence_id.0.to_string())
        .bind(&i.tenant_id.0)
        .bind(&i.namespace.0)
        .bind(i.state.to_string())
        .bind(i.next_fire_at.map(ts))
        .bind(i.priority as i16)
        .bind(&i.timezone)
        .bind(serde_json::to_string(&i.metadata)?)
        .bind(serde_json::to_string(&i.context)?)
        .bind(&i.concurrency_key)
        .bind(i.max_concurrency)
        .bind(&i.idempotency_key)
        .bind(i.session_id.map(|u| u.to_string()))
        .bind(i.parent_instance_id.map(|u| u.0.to_string()))
        .bind(ts(i.created_at))
        .bind(ts(i.updated_at))
        .execute(&mut *tx).await.map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(instances.len() as u64)
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query("SELECT * FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_instance).transpose()
}

#[instrument(skip(storage), fields(limit, max_per_tenant))]
pub(super) async fn claim_due(
    storage: &SqliteStorage,
    now: DateTime<Utc>,
    limit: u32,
    max_per_tenant: u32,
) -> Result<Vec<TaskInstance>, StorageError> {
    let now_s = ts(now);
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    let rows = if max_per_tenant > 0 {
        // Noisy-neighbor protection: cap instances per tenant using ROW_NUMBER().
        sqlx::query(
            "SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY priority DESC, next_fire_at ASC) AS rn
                FROM task_instances
                WHERE state='scheduled' AND (next_fire_at IS NULL OR next_fire_at <= ?1)
            ) ranked
            WHERE rn <= ?3
            ORDER BY priority DESC, next_fire_at ASC
            LIMIT ?2"
        )
        .bind(&now_s)
        .bind(limit as i64)
        .bind(max_per_tenant as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?
    } else {
        // No per-tenant cap — original fast path.
        sqlx::query(
            "SELECT * FROM task_instances WHERE state='scheduled' AND (next_fire_at IS NULL OR next_fire_at <= ?1) ORDER BY priority DESC, next_fire_at ASC LIMIT ?2"
        )
        .bind(&now_s)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?
    };

    let instances: Vec<TaskInstance> = rows
        .iter()
        .map(row_to_instance)
        .collect::<Result<Vec<_>, _>>()?;
    if !instances.is_empty() {
        let ids: Vec<String> = instances.iter().map(|i| i.id.0.to_string()).collect();
        let placeholders: String = ids
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE task_instances SET state='running', updated_at=?1 WHERE id IN ({placeholders})"
        );
        let mut q = sqlx::query(&sql).bind(&now_s);
        for id in &ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(instances)
}

pub(super) async fn update_state(
    storage: &SqliteStorage,
    id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET state=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(new_state.to_string())
        .bind(next_fire_at.map(ts))
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn update_context(
    storage: &SqliteStorage,
    id: InstanceId,
    context: &ExecutionContext,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(serde_json::to_string(context)?)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Transactional variant: externalize oversized `data.*` fields and commit
/// the payload rows + context UPDATE atomically. See the Postgres twin in
/// [`super::super::postgres::instances::update_context_externalized`] for the
/// contract; SQLite differs only in wire syntax and bind form.
pub(super) async fn update_context_externalized(
    storage: &SqliteStorage,
    id: InstanceId,
    context: &ExecutionContext,
    threshold_bytes: u32,
) -> Result<(), StorageError> {
    let mut ctx_clone = context.clone();
    let refs = crate::externalizing::externalize_fields(
        &mut ctx_clone.data,
        &id.0.to_string(),
        threshold_bytes,
    );
    let ctx_str = serde_json::to_string(&ctx_clone)?;

    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, id, ref_key, payload).await?;
    }

    sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(ctx_str)
        .bind(ts(Utc::now()))
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Transactional single-instance create with externalization. SQLite twin of
/// [`super::super::postgres::instances::create_externalized`].
pub(super) async fn create_externalized(
    storage: &SqliteStorage,
    instance: &TaskInstance,
    threshold_bytes: u32,
) -> Result<(), StorageError> {
    let mut inst_clone = instance.clone();
    let refs = crate::externalizing::externalize_fields(
        &mut inst_clone.context.data,
        &instance.id.0.to_string(),
        threshold_bytes,
    );

    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, instance.id, ref_key, payload).await?;
    }

    sqlx::query(
        "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
    )
    .bind(inst_clone.id.0.to_string())
    .bind(inst_clone.sequence_id.0.to_string())
    .bind(&inst_clone.tenant_id.0)
    .bind(&inst_clone.namespace.0)
    .bind(inst_clone.state.to_string())
    .bind(inst_clone.next_fire_at.map(ts))
    .bind(inst_clone.priority as i16)
    .bind(&inst_clone.timezone)
    .bind(serde_json::to_string(&inst_clone.metadata)?)
    .bind(serde_json::to_string(&inst_clone.context)?)
    .bind(&inst_clone.concurrency_key)
    .bind(inst_clone.max_concurrency)
    .bind(&inst_clone.idempotency_key)
    .bind(inst_clone.session_id.map(|u| u.to_string()))
    .bind(inst_clone.parent_instance_id.map(|u| u.0.to_string()))
    .bind(ts(inst_clone.created_at))
    .bind(ts(inst_clone.updated_at))
    .execute(&mut *tx)
    .await
    .map_err(|e| StorageError::Query(e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

/// Transactional batched create with externalization. Each instance's
/// `context.data` is externalized independently, then every externalized
/// row plus every `task_instances` row commits in a single transaction.
pub(super) async fn create_batch_externalized(
    storage: &SqliteStorage,
    instances: &[TaskInstance],
    threshold_bytes: u32,
) -> Result<u64, StorageError> {
    if instances.is_empty() {
        return Ok(0);
    }

    let mut prepared: Vec<(TaskInstance, Vec<(String, serde_json::Value)>)> =
        Vec::with_capacity(instances.len());
    for inst in instances {
        let mut c = inst.clone();
        let refs = crate::externalizing::externalize_fields(
            &mut c.context.data,
            &inst.id.0.to_string(),
            threshold_bytes,
        );
        prepared.push((c, refs));
    }

    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    // Step 1: externalized rows (per-instance keyed).
    for (inst, refs) in &prepared {
        for (ref_key, payload) in refs {
            insert_externalized_row(&mut tx, inst.id, ref_key, payload).await?;
        }
    }

    // Step 2: insert marker-swapped task_instances rows.
    for (inst, _) in &prepared {
        sqlx::query(
            "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
        )
        .bind(inst.id.0.to_string())
        .bind(inst.sequence_id.0.to_string())
        .bind(&inst.tenant_id.0)
        .bind(&inst.namespace.0)
        .bind(inst.state.to_string())
        .bind(inst.next_fire_at.map(ts))
        .bind(inst.priority as i16)
        .bind(&inst.timezone)
        .bind(serde_json::to_string(&inst.metadata)?)
        .bind(serde_json::to_string(&inst.context)?)
        .bind(&inst.concurrency_key)
        .bind(inst.max_concurrency)
        .bind(&inst.idempotency_key)
        .bind(inst.session_id.map(|u| u.to_string()))
        .bind(inst.parent_instance_id.map(|u| u.0.to_string()))
        .bind(ts(inst.created_at))
        .bind(ts(inst.updated_at))
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    }

    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(instances.len() as u64)
}

/// Insert (or upsert via INSERT OR REPLACE) one externalized_state row
/// inside an existing transaction. Picks compressed or inline representation
/// based on raw payload size.
async fn insert_externalized_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    instance_id: InstanceId,
    ref_key: &str,
    payload: &serde_json::Value,
) -> Result<(), StorageError> {
    use crate::compression::{compress, COMPRESSION_THRESHOLD_BYTES};
    let raw = serde_json::to_vec(payload).map_err(StorageError::Serialization)?;
    let raw_size = i64::try_from(raw.len()).unwrap_or(i64::MAX);

    if raw.len() >= COMPRESSION_THRESHOLD_BYTES {
        let compressed = compress(payload)?;
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, NULL, ?3, 'zstd', ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(compressed)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    } else {
        sqlx::query(
            "INSERT OR REPLACE INTO externalized_state \
             (ref_key, instance_id, payload, payload_bytes, compression, size_bytes, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5)",
        )
        .bind(ref_key)
        .bind(instance_id.0.to_string())
        .bind(serde_json::to_string(payload).map_err(StorageError::Serialization)?)
        .bind(raw_size)
        .bind(ts(Utc::now()))
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    Ok(())
}

pub(super) async fn update_sequence(
    storage: &SqliteStorage,
    id: InstanceId,
    new_sequence_id: SequenceId,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET sequence_id=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(new_sequence_id.0.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn merge_context_data(
    storage: &SqliteStorage,
    id: InstanceId,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), StorageError> {
    // Read-modify-write in a transaction for SQLite (no JSONB).
    let mut tx = storage
        .pool
        .begin()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    let row = sqlx::query("SELECT context FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    if let Some(row) = row {
        let ctx_str: String = row.get("context");
        let mut ctx: ExecutionContext =
            serde_json::from_str(&ctx_str).map_err(StorageError::Serialization)?;
        if let Some(obj) = ctx.data.as_object_mut() {
            obj.insert(key.to_string(), value.clone());
        }
        let ctx_json = serde_json::to_string(&ctx).map_err(StorageError::Serialization)?;
        sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(ctx_json)
            .bind(ts(Utc::now()))
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
    }
    tx.commit()
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut sql = String::from("SELECT * FROM task_instances WHERE 1=1");
    let mut args: Vec<String> = Vec::new();
    apply_filter_sql(&mut sql, filter, &mut args);
    let limit = pagination.limit.min(1000);
    args.push(limit.to_string());
    sql.push_str(&format!(" ORDER BY created_at DESC LIMIT ?{}", args.len()));
    args.push(pagination.offset.to_string());
    sql.push_str(&format!(" OFFSET ?{}", args.len()));
    let mut q = sqlx::query(&sql);
    for arg in &args {
        q = q.bind(arg);
    }
    let rows = q
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_instance).collect()
}

pub(super) async fn count(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
) -> Result<u64, StorageError> {
    let mut sql = String::from("SELECT COUNT(*) as cnt FROM task_instances WHERE 1=1");
    let mut args: Vec<String> = Vec::new();
    apply_filter_sql(&mut sql, filter, &mut args);
    let mut q = sqlx::query(&sql);
    for arg in &args {
        q = q.bind(arg);
    }
    let row = q
        .fetch_one(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(row.get::<i64, _>("cnt") as u64)
}

pub(super) async fn bulk_update_state(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    new_state: InstanceState,
) -> Result<u64, StorageError> {
    let mut args: Vec<String> = vec![new_state.to_string(), ts(Utc::now())];
    let mut sql = String::from("UPDATE task_instances SET state=?1, updated_at=?2 WHERE 1=1");
    apply_filter_sql(&mut sql, filter, &mut args);
    let mut q = sqlx::query(&sql);
    for arg in &args {
        q = q.bind(arg);
    }
    let result = q
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}

pub(super) async fn bulk_reschedule(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    offset_secs: i64,
) -> Result<u64, StorageError> {
    let mut args: Vec<String> = vec![format!("+{offset_secs} seconds"), ts(Utc::now())];
    let mut sql = String::from(
        "UPDATE task_instances SET next_fire_at=datetime(next_fire_at, ?1), updated_at=?2 WHERE state='scheduled'"
    );
    apply_filter_sql(&mut sql, filter, &mut args);
    let mut q = sqlx::query(&sql);
    for arg in &args {
        q = q.bind(arg);
    }
    let result = q
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(result.rows_affected())
}
