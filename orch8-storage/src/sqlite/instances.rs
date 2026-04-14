use std::collections::HashMap;

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

/// SQL for a full `task_instances` insert. Kept as a single canonical string so
/// that adding a column requires editing exactly one place per backend. All
/// insert sites (`create`, `create_batch`, `create_externalized`,
/// `create_batch_externalized`, `create_instance_with_dedupe`) bind against
/// this string via [`bind_instance_insert`].
pub(super) const INSTANCE_INSERT_SQL: &str = "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)";

/// Bind a `TaskInstance` to an already-prepared query in the canonical column
/// order used by [`INSTANCE_INSERT_SQL`]. Kept out of `sqlx::query(...)` so
/// that callers can `.execute(...)` against either a pool or a transaction
/// without duplicating the bind sequence.
pub(super) fn bind_instance_insert<'q>(
    q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    i: &'q TaskInstance,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>, StorageError> {
    Ok(q.bind(i.id.0.to_string())
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
        .bind(ts(i.updated_at)))
}

#[instrument(skip(storage, i), fields(instance_id = %i.id, tenant = %i.tenant_id))]
pub(super) async fn create(storage: &SqliteStorage, i: &TaskInstance) -> Result<(), StorageError> {
    bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), i)?
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn create_batch(
    storage: &SqliteStorage,
    instances: &[TaskInstance],
) -> Result<u64, StorageError> {
    let mut tx = storage.pool.begin().await?;
    for i in instances {
        bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), i)?
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(instances.len() as u64)
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query("SELECT * FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&storage.pool)
        .await?;
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
    let mut tx = storage.pool.begin().await?;

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
        ?
    } else {
        // No per-tenant cap — original fast path.
        sqlx::query(
            "SELECT * FROM task_instances WHERE state='scheduled' AND (next_fire_at IS NULL OR next_fire_at <= ?1) ORDER BY priority DESC, next_fire_at ASC LIMIT ?2"
        )
        .bind(&now_s)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        ?
    };

    let all_candidates: Vec<TaskInstance> = rows
        .iter()
        .map(row_to_instance)
        .collect::<Result<Vec<_>, _>>()?;

    // Enforce concurrency limits WITHIN the transaction so instances that
    // would exceed max_concurrency are never set to Running. This prevents
    // a window where the test (or any observer) could see more Running
    // instances than allowed.
    let instances = filter_by_concurrency(&mut tx, &all_candidates).await?;

    if !instances.is_empty() {
        let mut qb =
            sqlx::QueryBuilder::new("UPDATE task_instances SET state='running', updated_at=");
        qb.push_bind(&now_s);
        qb.push(" WHERE id IN (");
        let mut separated = qb.separated(",");
        for i in &instances {
            separated.push_bind(i.id.0.to_string());
        }
        separated.push_unseparated(")");

        qb.build().execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(instances)
}

/// Within a transaction, filter candidates by concurrency_key / max_concurrency.
/// For each distinct key, count already-Running instances and only allow enough
/// candidates through to fill the remaining slots.
async fn filter_by_concurrency(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    candidates: &[TaskInstance],
) -> Result<Vec<TaskInstance>, StorageError> {
    // Group candidates by concurrency_key.
    let mut keyed: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, inst) in candidates.iter().enumerate() {
        if let (Some(ref key), Some(_)) = (&inst.concurrency_key, inst.max_concurrency) {
            keyed.entry(key.clone()).or_default().push(idx);
        }
    }

    if keyed.is_empty() {
        return Ok(candidates.to_vec());
    }

    let mut excluded = std::collections::HashSet::new();
    for (key, indices) in &keyed {
        let max = candidates[indices[0]].max_concurrency.unwrap_or(i32::MAX);

        // Count already-Running instances for this key (within the same transaction).
        let row = sqlx::query(
            "SELECT COUNT(*) as cnt FROM task_instances WHERE concurrency_key=?1 AND state='running'",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await
        ?;
        let already_running: i64 = row.get("cnt");

        let slots = (i64::from(max) - already_running).max(0) as usize;
        if slots < indices.len() {
            for &idx in &indices[slots..] {
                excluded.insert(idx);
            }
        }
    }

    Ok(candidates
        .iter()
        .enumerate()
        .filter(|(idx, _)| !excluded.contains(idx))
        .map(|(_, inst)| inst.clone())
        .collect())
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
        .await?;
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
        .await?;
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

    let mut tx = storage.pool.begin().await?;

    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, id, ref_key, payload).await?;
    }

    sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
        .bind(id.0.to_string())
        .bind(ctx_str)
        .bind(ts(Utc::now()))
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
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

    let mut tx = storage.pool.begin().await?;

    // Parent row must exist before children so the FK
    // (externalized_state.instance_id -> task_instances.id) is satisfied.
    bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), &inst_clone)?
        .execute(&mut *tx)
        .await?;

    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, instance.id, ref_key, payload).await?;
    }

    tx.commit().await?;
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

    let mut tx = storage.pool.begin().await?;

    // Step 1: insert marker-swapped task_instances rows first so the FK
    // on externalized_state.instance_id is satisfied when children land.
    for (inst, _) in &prepared {
        bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), inst)?
            .execute(&mut *tx)
            .await?;
    }

    // Step 2: externalized rows (per-instance keyed).
    for (inst, refs) in &prepared {
        for (ref_key, payload) in refs {
            insert_externalized_row(&mut tx, inst.id, ref_key, payload).await?;
        }
    }

    tx.commit().await?;
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
        .await?;
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
        .await?;
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
        .await?;
    Ok(())
}

pub(super) async fn merge_context_data(
    storage: &SqliteStorage,
    id: InstanceId,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), StorageError> {
    // Read-modify-write in a transaction for SQLite (no JSONB).
    let mut tx = storage.pool.begin().await?;
    let row = sqlx::query("SELECT context FROM task_instances WHERE id=?1")
        .bind(id.0.to_string())
        .fetch_optional(&mut *tx)
        .await?;
    if let Some(row) = row {
        let ctx_str: String = row.get("context");
        let mut ctx: ExecutionContext =
            serde_json::from_str(&ctx_str).map_err(StorageError::Serialization)?;
        // Instances are created with `data: null` by default. Promote to an
        // object so the key-insert below persists (parity with the Postgres
        // jsonb_set path which guards on `jsonb_typeof`).
        if !ctx.data.is_object() {
            ctx.data = serde_json::Value::Object(serde_json::Map::new());
        }
        if let Some(obj) = ctx.data.as_object_mut() {
            obj.insert(key.to_string(), value.clone());
        }
        let ctx_json = serde_json::to_string(&ctx).map_err(StorageError::Serialization)?;
        sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(ctx_json)
            .bind(ts(Utc::now()))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(super) async fn list(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("SELECT * FROM task_instances WHERE 1=1");
    apply_filter_sql(&mut qb, filter);

    qb.push(" ORDER BY updated_at DESC LIMIT ");
    qb.push_bind(i64::from(pagination.limit.min(1000)));
    qb.push(" OFFSET ");
    qb.push_bind(pagination.offset as i64);

    let rows = qb.build().fetch_all(&storage.pool).await?;
    rows.iter().map(row_to_instance).collect()
}

pub(super) async fn list_waiting_with_trees(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<(TaskInstance, Vec<orch8_types::execution::ExecutionNode>)>, StorageError> {
    use std::collections::HashMap;

    // 1. Filtered Waiting instances.
    let waiting_filter = InstanceFilter {
        states: Some(vec![InstanceState::Waiting]),
        tenant_id: filter.tenant_id.clone(),
        namespace: filter.namespace.clone(),
        ..InstanceFilter::default()
    };
    let instances = list(storage, &waiting_filter, pagination).await?;
    if instances.is_empty() {
        return Ok(Vec::new());
    }

    // 2. One batched SELECT for all execution trees (mirrors the Postgres
    // backend's `ANY($1)` pattern, but SQLite requires an explicit `IN (...)`
    // list). Replaces the previous per-instance N+1 loop which issued one
    // round-trip per row.
    let mut qb = sqlx::QueryBuilder::new("SELECT * FROM execution_tree WHERE instance_id IN (");
    let mut sep = qb.separated(", ");
    for inst in &instances {
        sep.push_bind(inst.id.0.to_string());
    }
    sep.push_unseparated(") ORDER BY id");

    let rows = qb.build().fetch_all(&storage.pool).await?;
    let mut trees: HashMap<InstanceId, Vec<orch8_types::execution::ExecutionNode>> =
        instances.iter().map(|i| (i.id, Vec::new())).collect();
    for row in &rows {
        let node = super::helpers::row_to_node(row)?;
        trees.entry(node.instance_id).or_default().push(node);
    }

    Ok(instances
        .into_iter()
        .map(|inst| {
            let tree = trees.remove(&inst.id).unwrap_or_default();
            (inst, tree)
        })
        .collect())
}

pub(super) async fn count(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
) -> Result<u64, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("SELECT COUNT(*) as cnt FROM task_instances WHERE 1=1");
    apply_filter_sql(&mut qb, filter);

    let row = qb.build().fetch_one(&storage.pool).await?;
    Ok(row.get::<i64, _>("cnt") as u64)
}

pub(super) async fn bulk_update_state(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    new_state: InstanceState,
) -> Result<u64, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("UPDATE task_instances SET state=");
    qb.push_bind(new_state.to_string());
    qb.push(", updated_at=");
    qb.push_bind(ts(Utc::now()));
    qb.push(" WHERE 1=1");
    apply_filter_sql(&mut qb, filter);

    let result = qb.build().execute(&storage.pool).await?;
    Ok(result.rows_affected())
}

pub(super) async fn bulk_reschedule(
    storage: &SqliteStorage,
    filter: &InstanceFilter,
    offset_secs: i64,
) -> Result<u64, StorageError> {
    let mut qb =
        sqlx::QueryBuilder::new("UPDATE task_instances SET next_fire_at=datetime(next_fire_at, ");
    qb.push_bind(format!("+{offset_secs} seconds"));
    qb.push("), updated_at=");
    qb.push_bind(ts(Utc::now()));
    qb.push(" WHERE state='scheduled'");
    apply_filter_sql(&mut qb, filter);

    let result = qb.build().execute(&storage.pool).await?;
    Ok(result.rows_affected())
}
