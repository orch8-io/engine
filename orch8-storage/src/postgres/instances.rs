use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::{InstanceState, TaskInstance};

use super::rows::InstanceRow;
use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    inst: &TaskInstance,
) -> Result<(), StorageError> {
    let context = serde_json::to_value(&inst.context)?;
    sqlx::query(
        r"
        INSERT INTO task_instances
            (id, sequence_id, tenant_id, namespace, state, next_fire_at,
             priority, timezone, metadata, context,
             concurrency_key, max_concurrency, idempotency_key,
             session_id, parent_instance_id,
             created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ",
    )
    .bind(inst.id.0)
    .bind(inst.sequence_id.0)
    .bind(&inst.tenant_id.0)
    .bind(&inst.namespace.0)
    .bind(inst.state.to_string())
    .bind(inst.next_fire_at)
    .bind(inst.priority as i16)
    .bind(&inst.timezone)
    .bind(&inst.metadata)
    .bind(&context)
    .bind(&inst.concurrency_key)
    .bind(inst.max_concurrency)
    .bind(&inst.idempotency_key)
    .bind(inst.session_id)
    .bind(inst.parent_instance_id.map(|id| id.0))
    .bind(inst.created_at)
    .bind(inst.updated_at)
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
            let context = serde_json::to_value(&inst.context).unwrap_or(serde_json::Value::Object(serde_json::Map::default()));
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
    let rows = if max_per_tenant > 0 {
        // Noisy-neighbor protection: cap instances per tenant using ROW_NUMBER().
        sqlx::query_as::<_, InstanceRow>(
            r"
            UPDATE task_instances
            SET state = 'running', updated_at = $1
            WHERE id IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY priority DESC, next_fire_at ASC) AS rn
                    FROM task_instances
                    WHERE next_fire_at <= $1 AND state = 'scheduled'
                    FOR UPDATE SKIP LOCKED
                ) ranked
                WHERE rn <= $3
                ORDER BY rn, id
                LIMIT $2
            )
            RETURNING id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      session_id, parent_instance_id, created_at, updated_at
            ",
        )
        .bind(now)
        .bind(i64::from(limit))
        .bind(i64::from(max_per_tenant))
        .fetch_all(&store.pool)
        .await?
    } else {
        // No per-tenant cap — original fast path.
        sqlx::query_as::<_, InstanceRow>(
            r"
            UPDATE task_instances
            SET state = 'running', updated_at = $1
            WHERE id IN (
                SELECT id FROM task_instances
                WHERE next_fire_at <= $1 AND state = 'scheduled'
                ORDER BY priority DESC, next_fire_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      session_id, parent_instance_id, created_at, updated_at
            ",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&store.pool)
        .await?
    };

    rows.into_iter()
        .map(InstanceRow::into_instance)
        .collect::<Result<Vec<_>, _>>()
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
    sqlx::query(
        r"UPDATE task_instances
          SET context = jsonb_set(context, ARRAY['data', $2], $3, true),
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
    qb.push(" ORDER BY created_at DESC");
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
fn apply_instance_filter(qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>, filter: &InstanceFilter) {
    if let Some(ref tid) = filter.tenant_id {
        qb.push(" AND tenant_id = ").push_bind(tid.0.clone());
    }
    if let Some(ref ns) = filter.namespace {
        qb.push(" AND namespace = ").push_bind(ns.0.clone());
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
        qb.push(" AND metadata @> ").push_bind(meta.clone());
    }
    if let Some(ref p) = filter.priority {
        qb.push(" AND priority = ").push_bind(*p as i16);
    }
}
