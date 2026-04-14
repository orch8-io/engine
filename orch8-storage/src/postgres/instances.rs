use std::collections::HashMap;

use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::{InstanceState, TaskInstance};

use super::rows::InstanceRow;
use super::PostgresStorage;

/// Canonical `task_instances` INSERT statement. Kept as a single string so
/// that adding a column requires editing exactly one place per backend. All
/// per-row insert sites (`create`, `create_externalized`,
/// `create_instance_with_dedupe`) bind via [`bind_instance_insert`].
pub(super) const INSTANCE_INSERT_SQL: &str = r"
    INSERT INTO task_instances
        (id, sequence_id, tenant_id, namespace, state, next_fire_at,
         priority, timezone, metadata, context,
         concurrency_key, max_concurrency, idempotency_key,
         session_id, parent_instance_id,
         created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
";

/// Bind a `TaskInstance` to an already-prepared query in the canonical column
/// order used by [`INSTANCE_INSERT_SQL`]. Serializes `context` up front so the
/// returned `Query` is `'q`-bound to owned data.
pub(super) fn bind_instance_insert<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    inst: &'q TaskInstance,
    context_json: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q.bind(inst.id.0)
        .bind(inst.sequence_id.0)
        .bind(&inst.tenant_id.0)
        .bind(&inst.namespace.0)
        .bind(inst.state.to_string())
        .bind(inst.next_fire_at)
        .bind(inst.priority as i16)
        .bind(&inst.timezone)
        .bind(&inst.metadata)
        .bind(context_json)
        .bind(&inst.concurrency_key)
        .bind(inst.max_concurrency)
        .bind(&inst.idempotency_key)
        .bind(inst.session_id)
        .bind(inst.parent_instance_id.map(|id| id.0))
        .bind(inst.created_at)
        .bind(inst.updated_at)
}

pub(super) async fn create(
    store: &PostgresStorage,
    inst: &TaskInstance,
) -> Result<(), StorageError> {
    let context = serde_json::to_value(&inst.context)?;
    bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), inst, &context)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn create_batch(
    store: &PostgresStorage,
    instances: &[TaskInstance],
) -> Result<u64, StorageError> {
    if instances.is_empty() {
        return Ok(0);
    }

    let mut tx = store.pool.begin().await?;
    let mut count = 0u64;

    for chunk in instances.chunks(500) {
        let mut qb = sqlx::QueryBuilder::new(
            r"INSERT INTO task_instances
                (id, sequence_id, tenant_id, namespace, state, next_fire_at,
                 priority, timezone, metadata, context,
                 concurrency_key, max_concurrency, idempotency_key,
                 session_id, parent_instance_id,
                 created_at, updated_at) ",
        );
        qb.push_values(chunk, |mut b, inst| {
            let context = serde_json::to_value(&inst.context)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::default()));
            b.push_bind(inst.id.0)
                .push_bind(inst.sequence_id.0)
                .push_bind(&inst.tenant_id.0)
                .push_bind(&inst.namespace.0)
                .push_bind(inst.state.to_string())
                .push_bind(inst.next_fire_at)
                .push_bind(inst.priority as i16)
                .push_bind(&inst.timezone)
                .push_bind(&inst.metadata)
                .push_bind(context)
                .push_bind(&inst.concurrency_key)
                .push_bind(inst.max_concurrency)
                .push_bind(&inst.idempotency_key)
                .push_bind(inst.session_id)
                .push_bind(inst.parent_instance_id.map(|id| id.0))
                .push_bind(inst.created_at)
                .push_bind(inst.updated_at);
        });
        let result = qb.build().execute(&mut *tx).await?;
        count += result.rows_affected();
    }

    tx.commit().await?;
    Ok(count)
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: InstanceId,
) -> Result<Option<TaskInstance>, StorageError> {
    let row = sqlx::query_as::<_, InstanceRow>(
        r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                  priority, timezone, metadata, context,
                  concurrency_key, max_concurrency, idempotency_key,
                  session_id, parent_instance_id, created_at, updated_at
           FROM task_instances WHERE id = $1",
    )
    .bind(id.0)
    .fetch_optional(&store.pool)
    .await?;
    row.map(InstanceRow::into_instance).transpose()
}

pub(super) async fn claim_due(
    store: &PostgresStorage,
    now: DateTime<Utc>,
    limit: u32,
    max_per_tenant: u32,
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut tx = store.pool.begin().await?;

    // Step 1: SELECT candidates with FOR UPDATE SKIP LOCKED (locks the rows
    // but doesn't change state yet).
    //
    // Two nuances this query must handle that the previous shape got wrong:
    //
    //   (a) `next_fire_at IS NULL` — scheduled rows with no explicit fire time
    //       are due *now*. SQLite's `claim_due` already treats them this way;
    //       without the NULL branch here, null-fire rows starve forever on PG.
    //   (b) `FOR UPDATE SKIP LOCKED` cannot legally live inside a subselect
    //       that also contains a window function (`ROW_NUMBER() OVER ...`) —
    //       PostgreSQL rejects locking clauses in contexts where "returned
    //       rows cannot be clearly identified with individual table rows",
    //       which includes WINDOW. The old combined shape was undefined
    //       behaviour at best. We now lock base rows in a CTE and rank them
    //       in the outer query. We over-select (`limit * max_per_tenant`) so
    //       that per-tenant capping doesn't starve the caller when the lock
    //       pool is monopolised by one tenant's backlog.
    let candidate_rows = if max_per_tenant > 0 {
        // Over-select cap: we need enough locked rows to survive `rn <= $3`
        // trimming and still return up to `$2` distinct-tenant rows. Saturating
        // multiply protects against u32 overflow on pathological limits.
        let overselect = i64::from(limit).saturating_mul(i64::from(max_per_tenant));
        sqlx::query_as::<_, InstanceRow>(
            r"
            WITH locked AS (
                SELECT *
                FROM task_instances
                WHERE (next_fire_at IS NULL OR next_fire_at <= $1)
                  AND state = 'scheduled'
                ORDER BY priority DESC, next_fire_at ASC NULLS FIRST
                LIMIT $4
                FOR UPDATE SKIP LOCKED
            ), ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY tenant_id
                           ORDER BY priority DESC, next_fire_at ASC NULLS FIRST
                       ) AS rn
                FROM locked
            )
            SELECT *
            FROM ranked
            WHERE rn <= $3
            ORDER BY priority DESC, next_fire_at ASC NULLS FIRST
            LIMIT $2
            ",
        )
        .bind(now)
        .bind(i64::from(limit))
        .bind(i64::from(max_per_tenant))
        .bind(overselect)
        .fetch_all(&mut *tx)
        .await?
    } else {
        sqlx::query_as::<_, InstanceRow>(
            r"
            SELECT * FROM task_instances
            WHERE (next_fire_at IS NULL OR next_fire_at <= $1)
              AND state = 'scheduled'
            ORDER BY priority DESC, next_fire_at ASC NULLS FIRST
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            ",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&mut *tx)
        .await?
    };

    let all_candidates: Vec<TaskInstance> = candidate_rows
        .into_iter()
        .map(InstanceRow::into_instance)
        .collect::<Result<Vec<_>, _>>()?;

    if all_candidates.is_empty() {
        tx.commit().await?;
        return Ok(Vec::new());
    }

    // Step 2: Filter by concurrency limits within the transaction.
    let instances = filter_by_concurrency_pg(&mut tx, &all_candidates).await?;

    if !instances.is_empty() {
        // Step 3: Only update the filtered instances to Running.
        let ids: Vec<uuid::Uuid> = instances.iter().map(|i| i.id.0).collect();
        sqlx::query(
            r"UPDATE task_instances SET state = 'running', updated_at = $1 WHERE id = ANY($2)",
        )
        .bind(now)
        .bind(&ids)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    // Return instances with state updated to Running.
    Ok(instances
        .into_iter()
        .map(|mut i| {
            i.state = InstanceState::Running;
            i
        })
        .collect())
}

/// Filter candidates by `concurrency_key` / `max_concurrency` within a transaction.
async fn filter_by_concurrency_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    candidates: &[TaskInstance],
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut keyed: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, inst) in candidates.iter().enumerate() {
        if let (Some(ref key), Some(_)) = (&inst.concurrency_key, inst.max_concurrency) {
            keyed.entry(key.clone()).or_default().push(idx);
        }
    }

    if keyed.is_empty() {
        return Ok(candidates.to_vec());
    }

    // Batch-fetch running counts for all concurrency keys in a single query
    // instead of N separate COUNT queries (one per key).
    let keys: Vec<&str> = keyed.keys().map(String::as_str).collect();
    let rows = sqlx::query(
        "SELECT concurrency_key, COUNT(*) as cnt FROM task_instances \
         WHERE concurrency_key = ANY($1) AND state = 'running' \
         GROUP BY concurrency_key",
    )
    .bind(&keys)
    .fetch_all(&mut **tx)
    .await?;

    let running_counts: HashMap<String, i64> = rows
        .into_iter()
        .map(|r| {
            (
                r.get::<String, _>("concurrency_key"),
                r.get::<i64, _>("cnt"),
            )
        })
        .collect();

    let mut excluded = std::collections::HashSet::new();
    for (key, indices) in &keyed {
        let max = candidates[indices[0]].max_concurrency.unwrap_or(i32::MAX);
        let already_running = running_counts.get(key).copied().unwrap_or(0);

        #[allow(clippy::cast_possible_truncation)]
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
    store: &PostgresStorage,
    id: InstanceId,
    new_state: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    sqlx::query(
        r"
        UPDATE task_instances
        SET state = $2, next_fire_at = $3, updated_at = NOW()
        WHERE id = $1
        ",
    )
    .bind(id.0)
    .bind(new_state.to_string())
    .bind(next_fire_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn update_context(
    store: &PostgresStorage,
    id: InstanceId,
    context: &orch8_types::context::ExecutionContext,
) -> Result<(), StorageError> {
    let ctx_json = serde_json::to_value(context)?;
    sqlx::query("UPDATE task_instances SET context = $2, updated_at = NOW() WHERE id = $1")
        .bind(id.0)
        .bind(&ctx_json)
        .execute(&store.pool)
        .await?;
    Ok(())
}

/// Update a task instance's context with externalization, atomic:
/// externalized payloads + marker-swapped context commit or rollback together.
///
/// Contract:
/// 1. Clone + mutate the context in-memory so any `data.*` field whose
///    serialized size meets `threshold_bytes` is swapped for a marker.
/// 2. Inside a single transaction, `INSERT ... ON CONFLICT DO UPDATE` each
///    externalized payload into `externalized_state`, then `UPDATE
///    task_instances` to the marker-swapped context.
/// 3. `threshold_bytes == 0` short-circuits to the plain update — no
///    externalization work is performed.
pub(super) async fn update_context_externalized(
    store: &PostgresStorage,
    id: InstanceId,
    context: &orch8_types::context::ExecutionContext,
    threshold_bytes: u32,
) -> Result<(), StorageError> {
    // Clone once; any externalization mutation happens on the clone so the
    // caller's value is never silently rewritten.
    let mut ctx_clone = context.clone();
    let refs = crate::externalizing::externalize_fields(
        &mut ctx_clone.data,
        &id.0.to_string(),
        threshold_bytes,
    );
    let ctx_json = serde_json::to_value(&ctx_clone)?;

    let mut tx = store.pool.begin().await?;

    // Step 1: persist each externalized payload inside the transaction.
    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, id, ref_key, payload).await?;
    }

    // Step 2: flip the instance context to the marker-swapped form.
    sqlx::query("UPDATE task_instances SET context = $2, updated_at = NOW() WHERE id = $1")
        .bind(id.0)
        .bind(&ctx_json)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(())
}

/// Transactional single-instance create with externalization.
///
/// Any `context.data` field that meets `threshold_bytes` is replaced with
/// an externalization marker; the payload lands in `externalized_state` and
/// the marker-swapped `task_instances` row is inserted in the same tx.
pub(super) async fn create_externalized(
    store: &PostgresStorage,
    instance: &TaskInstance,
    threshold_bytes: u32,
) -> Result<(), StorageError> {
    let mut inst_clone = instance.clone();
    let refs = crate::externalizing::externalize_fields(
        &mut inst_clone.context.data,
        &instance.id.0.to_string(),
        threshold_bytes,
    );
    let context_json = serde_json::to_value(&inst_clone.context)?;

    let mut tx = store.pool.begin().await?;

    // Parent first: the FK on externalized_state.instance_id is IMMEDIATE in
    // Postgres, so the task_instances row must exist before its children.
    bind_instance_insert(sqlx::query(INSTANCE_INSERT_SQL), &inst_clone, &context_json)
        .execute(&mut *tx)
        .await?;

    for (ref_key, payload) in &refs {
        insert_externalized_row(&mut tx, instance.id, ref_key, payload).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Transactional batched create with externalization.
///
/// Per-instance `context.data` externalization (each instance's refs are
/// keyed by its own `instance_id`), then all externalized rows and all
/// `task_instances` rows commit in a single transaction. Returns the
/// number of instances inserted.
pub(super) async fn create_batch_externalized(
    store: &PostgresStorage,
    instances: &[TaskInstance],
    threshold_bytes: u32,
) -> Result<u64, StorageError> {
    if instances.is_empty() {
        return Ok(0);
    }

    // Materialise the marker-swapped clones + per-instance refs up front so
    // the transaction body is linear and allocation-free mid-flight.
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

    let mut tx = store.pool.begin().await?;
    let mut count = 0u64;

    // Step 1: bulk-insert task_instances first so the FK on
    // externalized_state.instance_id is satisfied before children land.
    for chunk in prepared.chunks(500) {
        let mut qb = sqlx::QueryBuilder::new(
            r"INSERT INTO task_instances
                (id, sequence_id, tenant_id, namespace, state, next_fire_at,
                 priority, timezone, metadata, context,
                 concurrency_key, max_concurrency, idempotency_key,
                 session_id, parent_instance_id,
                 created_at, updated_at) ",
        );
        qb.push_values(chunk, |mut b, (inst, _)| {
            let context = serde_json::to_value(&inst.context)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::default()));
            b.push_bind(inst.id.0)
                .push_bind(inst.sequence_id.0)
                .push_bind(&inst.tenant_id.0)
                .push_bind(&inst.namespace.0)
                .push_bind(inst.state.to_string())
                .push_bind(inst.next_fire_at)
                .push_bind(inst.priority as i16)
                .push_bind(&inst.timezone)
                .push_bind(&inst.metadata)
                .push_bind(context)
                .push_bind(&inst.concurrency_key)
                .push_bind(inst.max_concurrency)
                .push_bind(&inst.idempotency_key)
                .push_bind(inst.session_id)
                .push_bind(inst.parent_instance_id.map(|id| id.0))
                .push_bind(inst.created_at)
                .push_bind(inst.updated_at);
        });
        let result = qb.build().execute(&mut *tx).await?;
        count += result.rows_affected();
    }

    // Step 2: persist every externalized payload across every instance.
    for (inst, refs) in &prepared {
        for (ref_key, payload) in refs {
            insert_externalized_row(&mut tx, inst.id, ref_key, payload).await?;
        }
    }

    tx.commit().await?;
    Ok(count)
}

/// Insert (or upsert) one `externalized_state` row inside an existing
/// transaction, choosing the compressed or inline representation based on
/// raw payload size.
async fn insert_externalized_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
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
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, NULL, $4, 'zstd', $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = NULL,
                    payload_bytes = EXCLUDED.payload_bytes,
                    compression = 'zstd',
                    size_bytes = EXCLUDED.size_bytes",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(&compressed)
        .bind(raw_size)
        .execute(&mut **tx)
        .await?;
    } else {
        sqlx::query(
            r"INSERT INTO externalized_state
                  (id, instance_id, ref_key, payload, payload_bytes, compression, size_bytes, created_at)
              VALUES ($1, $2, $3, $4, NULL, NULL, $5, NOW())
              ON CONFLICT (ref_key) DO UPDATE
                SET payload = EXCLUDED.payload,
                    payload_bytes = NULL,
                    compression = NULL,
                    size_bytes = EXCLUDED.size_bytes",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(payload)
        .bind(raw_size)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn update_sequence(
    store: &PostgresStorage,
    id: InstanceId,
    new_sequence_id: SequenceId,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE task_instances SET sequence_id = $2, updated_at = NOW() WHERE id = $1")
        .bind(id.0)
        .bind(new_sequence_id.0)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn merge_context_data(
    store: &PostgresStorage,
    id: InstanceId,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), StorageError> {
    // `jsonb_set` with `create_missing=true` creates a missing *leaf* key but
    // cannot descend into a null/non-object parent. Instances are created
    // with `context.data = null` by default, so calling `jsonb_set(ctx,
    // '{data,foo}', …, true)` on that silently no-ops. Guard by ensuring
    // `data` is an object first, then set the key inside it.
    sqlx::query(
        r"UPDATE task_instances
          SET context = jsonb_set(
                CASE
                    WHEN jsonb_typeof(context->'data') = 'object' THEN context
                    ELSE jsonb_set(context, ARRAY['data'], '{}'::jsonb, true)
                END,
                ARRAY['data', $2],
                $3,
                true
              ),
              updated_at = NOW()
          WHERE id = $1",
    )
    .bind(id.0)
    .bind(key)
    .bind(value)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list(
    store: &PostgresStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<TaskInstance>, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                  priority, timezone, metadata, context,
                  concurrency_key, max_concurrency, idempotency_key,
                  session_id, parent_instance_id, created_at, updated_at
           FROM task_instances WHERE 1=1",
    );
    apply_instance_filter(&mut qb, filter);
    qb.push(" ORDER BY updated_at DESC");
    qb.push(" LIMIT ")
        .push_bind(i64::from(pagination.limit.min(1000)));
    qb.push(" OFFSET ")
        .push_bind(i64::try_from(pagination.offset).unwrap_or(i64::MAX));

    let rows = qb
        .build_query_as::<InstanceRow>()
        .fetch_all(&store.pool)
        .await?;

    rows.into_iter()
        .map(InstanceRow::into_instance)
        .collect::<Result<Vec<_>, _>>()
}

pub(super) async fn list_waiting_with_trees(
    store: &PostgresStorage,
    filter: &InstanceFilter,
    pagination: &Pagination,
) -> Result<Vec<(TaskInstance, Vec<orch8_types::execution::ExecutionNode>)>, StorageError> {
    // 1. Filtered Waiting instances.
    let waiting_filter = InstanceFilter {
        states: Some(vec![InstanceState::Waiting]),
        tenant_id: filter.tenant_id.clone(),
        namespace: filter.namespace.clone(),
        ..InstanceFilter::default()
    };
    let instances = list(store, &waiting_filter, pagination).await?;

    if instances.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Batch-fetch all execution trees in a single query instead of N+1.
    let instance_ids: Vec<uuid::Uuid> = instances.iter().map(|i| i.id.0).collect();
    let tree_rows = sqlx::query_as::<_, super::rows::ExecutionNodeRow>(
        r"SELECT id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at
           FROM execution_tree WHERE instance_id = ANY($1) ORDER BY id",
    )
    .bind(&instance_ids)
    .fetch_all(&store.pool)
    .await?;

    // Group nodes by instance_id (grab the raw UUID before into_node consumes the row).
    let mut trees: HashMap<uuid::Uuid, Vec<orch8_types::execution::ExecutionNode>> = HashMap::new();
    for row in tree_rows {
        let iid = row.instance_id; // Copy the Uuid before consuming the row.
        trees.entry(iid).or_default().push(row.into_node());
    }

    let out = instances
        .into_iter()
        .map(|inst| {
            let tree = trees.remove(&inst.id.0).unwrap_or_default();
            (inst, tree)
        })
        .collect();
    Ok(out)
}

pub(super) async fn count(
    store: &PostgresStorage,
    filter: &InstanceFilter,
) -> Result<u64, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("SELECT COUNT(*) as count FROM task_instances WHERE 1=1");
    apply_instance_filter(&mut qb, filter);

    let row: (i64,) = qb.build_query_as().fetch_one(&store.pool).await?;
    #[allow(clippy::cast_sign_loss)]
    Ok(row.0.max(0) as u64)
}

pub(super) async fn bulk_update_state(
    store: &PostgresStorage,
    filter: &InstanceFilter,
    new_state: InstanceState,
) -> Result<u64, StorageError> {
    let mut qb = sqlx::QueryBuilder::new("UPDATE task_instances SET state = ");
    qb.push_bind(new_state.to_string());
    qb.push(", updated_at = NOW() WHERE 1=1");
    apply_instance_filter(&mut qb, filter);

    let result = qb.build().execute(&store.pool).await?;
    Ok(result.rows_affected())
}

pub(super) async fn bulk_reschedule(
    store: &PostgresStorage,
    filter: &InstanceFilter,
    offset_secs: i64,
) -> Result<u64, StorageError> {
    let mut qb = sqlx::QueryBuilder::new(
        "UPDATE task_instances SET next_fire_at = next_fire_at + make_interval(secs => ",
    );
    #[allow(clippy::cast_precision_loss)] // offset_secs is bounded to practical ranges
    qb.push_bind(offset_secs as f64);
    qb.push("), updated_at = NOW() WHERE state = 'scheduled'");
    apply_instance_filter(&mut qb, filter);

    let result = qb.build().execute(&store.pool).await?;
    Ok(result.rows_affected())
}

/// Apply `InstanceFilter` conditions to a query builder.
fn apply_instance_filter<'a>(
    qb: &mut sqlx::QueryBuilder<'a, sqlx::Postgres>,
    filter: &'a InstanceFilter,
) {
    if let Some(ref tid) = filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(&tid.0);
    }
    if let Some(ref ns) = filter.namespace {
        qb.push(" AND namespace = ").push_bind(&ns.0);
    }
    if let Some(ref sid) = filter.sequence_id {
        qb.push(" AND sequence_id = ").push_bind(sid.0);
    }
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let state_strings: Vec<String> = states.iter().map(ToString::to_string).collect();
            qb.push(" AND state = ANY(")
                .push_bind(state_strings)
                .push(")");
        }
    }
    if let Some(ref meta) = filter.metadata_filter {
        qb.push(" AND metadata @> ").push_bind(meta);
    }
    if let Some(ref p) = filter.priority {
        qb.push(" AND priority = ").push_bind(*p as i16);
    }
}
