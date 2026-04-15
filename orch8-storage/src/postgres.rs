use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;

use crate::StorageBackend;

pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn new(database_url: &str, max_connections: u32) -> Result<Self, StorageError> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StorageError::Migration(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for PostgresStorage {
    // === Sequences ===

    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError> {
        let definition = serde_json::to_value(&seq.blocks)?;
        sqlx::query(
            r"
            INSERT INTO sequences (id, tenant_id, namespace, name, definition, version, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(seq.id.0)
        .bind(&seq.tenant_id.0)
        .bind(&seq.namespace.0)
        .bind(&seq.name)
        .bind(&definition)
        .bind(seq.version)
        .bind(seq.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        let row = sqlx::query_as::<_, SequenceRow>(
            "SELECT id, tenant_id, namespace, name, definition, version, created_at FROM sequences WHERE id = $1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?;
        row.map(SequenceRow::into_definition).transpose()
    }

    async fn get_sequence_by_name(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        let row = if let Some(v) = version {
            sqlx::query_as::<_, SequenceRow>(
                r"SELECT id, tenant_id, namespace, name, definition, version, created_at
                   FROM sequences
                   WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4",
            )
            .bind(&tenant_id.0)
            .bind(&namespace.0)
            .bind(name)
            .bind(v)
            .fetch_optional(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, SequenceRow>(
                r"SELECT id, tenant_id, namespace, name, definition, version, created_at
                   FROM sequences
                   WHERE tenant_id = $1 AND namespace = $2 AND name = $3
                   ORDER BY version DESC
                   LIMIT 1",
            )
            .bind(&tenant_id.0)
            .bind(&namespace.0)
            .bind(name)
            .fetch_optional(&self.pool)
            .await?
        };
        row.map(SequenceRow::into_definition).transpose()
    }

    // === Task Instances ===

    async fn create_instance(&self, inst: &TaskInstance) -> Result<(), StorageError> {
        let context = serde_json::to_value(&inst.context)?;
        sqlx::query(
            r"
            INSERT INTO task_instances
                (id, sequence_id, tenant_id, namespace, state, next_fire_at,
                 priority, timezone, metadata, context,
                 concurrency_key, max_concurrency, idempotency_key,
                 created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
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
        .bind(inst.created_at)
        .bind(inst.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        if instances.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut count = 0u64;

        for chunk in instances.chunks(500) {
            let mut qb = sqlx::QueryBuilder::new(
                r"INSERT INTO task_instances
                    (id, sequence_id, tenant_id, namespace, state, next_fire_at,
                     priority, timezone, metadata, context,
                     concurrency_key, max_concurrency, idempotency_key,
                     created_at, updated_at) ",
            );
            qb.push_values(chunk, |mut b, inst| {
                let context = serde_json::to_value(&inst.context).unwrap_or_default();
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
                    .push_bind(inst.created_at)
                    .push_bind(inst.updated_at);
            });
            let result = qb.build().execute(&mut *tx).await?;
            count += result.rows_affected();
        }

        tx.commit().await?;
        Ok(count)
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        let row = sqlx::query_as::<_, InstanceRow>(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      created_at, updated_at
               FROM task_instances WHERE id = $1",
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?;
        row.map(InstanceRow::into_instance).transpose()
    }

    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let rows = sqlx::query_as::<_, InstanceRow>(
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
                      created_at, updated_at
            ",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(InstanceRow::into_instance)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn update_instance_state(
        &self,
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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        let ctx_json = serde_json::to_value(context)?;
        sqlx::query("UPDATE task_instances SET context = $2, updated_at = NOW() WHERE id = $1")
            .bind(id.0)
            .bind(&ctx_json)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut qb = sqlx::QueryBuilder::new(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      created_at, updated_at
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
            .fetch_all(&self.pool)
            .await?;

        rows.into_iter()
            .map(InstanceRow::into_instance)
            .collect::<Result<Vec<_>, _>>()
    }

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError> {
        let mut qb =
            sqlx::QueryBuilder::new("SELECT COUNT(*) as count FROM task_instances WHERE 1=1");
        apply_instance_filter(&mut qb, filter);

        let row: (i64,) = qb.build_query_as().fetch_one(&self.pool).await?;
        #[allow(clippy::cast_sign_loss)]
        Ok(row.0.max(0) as u64)
    }

    async fn bulk_update_state(
        &self,
        filter: &InstanceFilter,
        new_state: InstanceState,
    ) -> Result<u64, StorageError> {
        let mut qb = sqlx::QueryBuilder::new("UPDATE task_instances SET state = ");
        qb.push_bind(new_state.to_string());
        qb.push(", updated_at = NOW() WHERE 1=1");
        apply_instance_filter(&mut qb, filter);

        let result = qb.build().execute(&self.pool).await?;
        Ok(result.rows_affected())
    }

    // === Execution Tree ===

    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError> {
        sqlx::query(
            r"
            INSERT INTO execution_tree
                (id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ",
        )
        .bind(node.id.0)
        .bind(node.instance_id.0)
        .bind(&node.block_id.0)
        .bind(node.parent_id.map(|p| p.0))
        .bind(node.block_type.to_string())
        .bind(node.branch_index)
        .bind(node.state.to_string())
        .bind(node.started_at)
        .bind(node.completed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError> {
        if nodes.is_empty() {
            return Ok(());
        }
        let mut qb = sqlx::QueryBuilder::new(
            "INSERT INTO execution_tree (id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at) ",
        );
        qb.push_values(nodes, |mut b, node| {
            b.push_bind(node.id.0)
                .push_bind(node.instance_id.0)
                .push_bind(&node.block_id.0)
                .push_bind(node.parent_id.map(|p| p.0))
                .push_bind(node.block_type.to_string())
                .push_bind(node.branch_index)
                .push_bind(node.state.to_string())
                .push_bind(node.started_at)
                .push_bind(node.completed_at);
        });
        qb.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        let rows = sqlx::query_as::<_, ExecutionNodeRow>(
            r"SELECT id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at
               FROM execution_tree WHERE instance_id = $1 ORDER BY id",
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ExecutionNodeRow::into_node).collect())
    }

    async fn update_node_state(
        &self,
        node_id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), StorageError> {
        let completed_at = if matches!(
            state,
            NodeState::Completed | NodeState::Failed | NodeState::Cancelled
        ) {
            Some(Utc::now())
        } else {
            None
        };
        sqlx::query(
            "UPDATE execution_tree SET state = $2, completed_at = COALESCE($3, completed_at) WHERE id = $1",
        )
        .bind(node_id.0)
        .bind(state.to_string())
        .bind(completed_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_children(
        &self,
        parent_id: ExecutionNodeId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        let rows = sqlx::query_as::<_, ExecutionNodeRow>(
            r"SELECT id, instance_id, block_id, parent_id, block_type, branch_index, state, started_at, completed_at
               FROM execution_tree WHERE parent_id = $1 ORDER BY branch_index, id",
        )
        .bind(parent_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ExecutionNodeRow::into_node).collect())
    }

    // === Block Outputs ===

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError> {
        sqlx::query(
            r"
            INSERT INTO block_outputs (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (instance_id, block_id) DO UPDATE
            SET output = $4, output_ref = $5, output_size = $6, attempt = $7
            ",
        )
        .bind(output.id)
        .bind(output.instance_id.0)
        .bind(&output.block_id.0)
        .bind(&output.output)
        .bind(&output.output_ref)
        .bind(output.output_size)
        .bind(output.attempt)
        .bind(output.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError> {
        let row = sqlx::query_as::<_, BlockOutputRow>(
            r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
               FROM block_outputs WHERE instance_id = $1 AND block_id = $2",
        )
        .bind(instance_id.0)
        .bind(&block_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(BlockOutputRow::into_output))
    }

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        let rows = sqlx::query_as::<_, BlockOutputRow>(
            r"SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
               FROM block_outputs WHERE instance_id = $1 ORDER BY created_at",
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(BlockOutputRow::into_output).collect())
    }

    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockId>, StorageError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT block_id FROM block_outputs WHERE instance_id = $1",
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| BlockId(id)).collect())
    }

    // === Rate Limits ===

    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError> {
        // Atomic: read the rate limit, check window, increment or reset.
        let row = sqlx::query_as::<_, RateLimitRow>(
            "SELECT id, tenant_id, resource_key, max_count, window_seconds, current_count, window_start FROM rate_limits WHERE tenant_id = $1 AND resource_key = $2",
        )
        .bind(&tenant_id.0)
        .bind(&resource_key.0)
        .fetch_optional(&self.pool)
        .await?;

        let Some(rl) = row else {
            // No rate limit configured for this resource — allowed.
            return Ok(RateLimitCheck::Allowed);
        };

        let window_end = rl.window_start + chrono::Duration::seconds(i64::from(rl.window_seconds));

        if now >= window_end {
            // Window expired — reset and allow.
            sqlx::query(
                "UPDATE rate_limits SET current_count = 1, window_start = $2 WHERE id = $1",
            )
            .bind(rl.id)
            .bind(now)
            .execute(&self.pool)
            .await?;
            return Ok(RateLimitCheck::Allowed);
        }

        if rl.current_count < rl.max_count {
            // Within window and under limit — increment and allow.
            sqlx::query("UPDATE rate_limits SET current_count = current_count + 1 WHERE id = $1")
                .bind(rl.id)
                .execute(&self.pool)
                .await?;
            return Ok(RateLimitCheck::Allowed);
        }

        // Exceeded — return retry_after as window end.
        Ok(RateLimitCheck::Exceeded {
            retry_after: window_end,
        })
    }

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError> {
        sqlx::query(
            r"
            INSERT INTO rate_limits (id, tenant_id, resource_key, max_count, window_seconds, current_count, window_start)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, resource_key) DO UPDATE
            SET max_count = $4, window_seconds = $5
            ",
        )
        .bind(limit.id)
        .bind(&limit.tenant_id.0)
        .bind(&limit.resource_key.0)
        .bind(limit.max_count)
        .bind(limit.window_seconds)
        .bind(limit.current_count)
        .bind(limit.window_start)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // === Signals ===

    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError> {
        let signal_type_str = signal.signal_type.to_string();
        sqlx::query(
            r"
            INSERT INTO signal_inbox (id, instance_id, signal_type, payload, delivered, created_at, delivered_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(signal.id)
        .bind(signal.instance_id.0)
        .bind(&signal_type_str)
        .bind(&signal.payload)
        .bind(signal.delivered)
        .bind(signal.created_at)
        .bind(signal.delivered_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError> {
        let rows = sqlx::query_as::<_, SignalRow>(
            r"SELECT id, instance_id, signal_type, payload, delivered, created_at, delivered_at
               FROM signal_inbox WHERE instance_id = $1 AND delivered = FALSE
               ORDER BY created_at",
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(SignalRow::into_signal).collect())
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE signal_inbox SET delivered = TRUE, delivered_at = NOW() WHERE id = $1")
            .bind(signal_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // === Idempotency ===

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        let row = sqlx::query_as::<_, InstanceRow>(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      created_at, updated_at
               FROM task_instances
               WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(&tenant_id.0)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(InstanceRow::into_instance).transpose()
    }

    // === Concurrency ===

    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM task_instances WHERE concurrency_key = $1 AND state = 'running'",
        )
        .bind(concurrency_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        let row: (i64,) = sqlx::query_as(
            r"SELECT COUNT(*) FROM task_instances
              WHERE concurrency_key = $1 AND state = 'running' AND id <= $2",
        )
        .bind(concurrency_key)
        .bind(instance_id.0)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    // === Recovery ===

    async fn recover_stale_instances(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        let threshold_secs = stale_threshold.as_secs_f64();
        let result = sqlx::query(
            r"
            UPDATE task_instances
            SET state = 'scheduled', next_fire_at = NOW(), updated_at = NOW()
            WHERE state = 'running'
              AND updated_at < NOW() - make_interval(secs => $1::double precision)
            ",
        )
        .bind(threshold_secs)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    // === Cron Schedules ===

    async fn create_cron_schedule(&self, s: &CronSchedule) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO cron_schedules
                (id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                 metadata, last_triggered_at, next_fire_at, created_at, updated_at)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
        )
        .bind(s.id)
        .bind(&s.tenant_id.0)
        .bind(&s.namespace.0)
        .bind(s.sequence_id.0)
        .bind(&s.cron_expr)
        .bind(&s.timezone)
        .bind(s.enabled)
        .bind(&s.metadata)
        .bind(s.last_triggered_at)
        .bind(s.next_fire_at)
        .bind(s.created_at)
        .bind(s.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError> {
        let row = sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(CronRow::into_schedule))
    }

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query_as::<_, CronRow>(
                r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                         metadata, last_triggered_at, next_fire_at, created_at, updated_at
                  FROM cron_schedules WHERE tenant_id = $1 ORDER BY created_at",
            )
            .bind(&tid.0)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, CronRow>(
                r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                         metadata, last_triggered_at, next_fire_at, created_at, updated_at
                  FROM cron_schedules ORDER BY created_at",
            )
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows.into_iter().map(CronRow::into_schedule).collect())
    }

    async fn update_cron_schedule(&self, s: &CronSchedule) -> Result<(), StorageError> {
        sqlx::query(
            r"UPDATE cron_schedules
              SET cron_expr=$2, timezone=$3, enabled=$4, metadata=$5, next_fire_at=$6, updated_at=NOW()
              WHERE id=$1",
        )
        .bind(s.id)
        .bind(&s.cron_expr)
        .bind(&s.timezone)
        .bind(s.enabled)
        .bind(&s.metadata)
        .bind(s.next_fire_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM cron_schedules WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        let rows = sqlx::query_as::<_, CronRow>(
            r"SELECT id, tenant_id, namespace, sequence_id, cron_expr, timezone, enabled,
                     metadata, last_triggered_at, next_fire_at, created_at, updated_at
              FROM cron_schedules
              WHERE enabled = TRUE AND next_fire_at <= $1
              ORDER BY next_fire_at
              FOR UPDATE SKIP LOCKED",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(CronRow::into_schedule).collect())
    }

    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE cron_schedules SET last_triggered_at=$2, next_fire_at=$3, updated_at=NOW() WHERE id=$1",
        )
        .bind(id)
        .bind(last_triggered_at)
        .bind(next_fire_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        Ok(())
    }
}

// --- Row types for sqlx deserialization ---

#[derive(sqlx::FromRow)]
struct SequenceRow {
    id: Uuid,
    tenant_id: String,
    namespace: String,
    name: String,
    definition: serde_json::Value,
    version: i32,
    created_at: DateTime<Utc>,
}

impl SequenceRow {
    fn into_definition(self) -> Result<SequenceDefinition, StorageError> {
        let blocks = serde_json::from_value(self.definition)?;
        Ok(SequenceDefinition {
            id: SequenceId(self.id),
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            name: self.name,
            version: self.version,
            blocks,
            created_at: self.created_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct InstanceRow {
    id: Uuid,
    sequence_id: Uuid,
    tenant_id: String,
    namespace: String,
    state: String,
    next_fire_at: Option<DateTime<Utc>>,
    priority: i16,
    timezone: String,
    metadata: serde_json::Value,
    context: serde_json::Value,
    concurrency_key: Option<String>,
    max_concurrency: Option<i32>,
    idempotency_key: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl InstanceRow {
    fn into_instance(self) -> Result<TaskInstance, StorageError> {
        let state = match self.state.as_str() {
            "scheduled" => InstanceState::Scheduled,
            "running" => InstanceState::Running,
            "waiting" => InstanceState::Waiting,
            "paused" => InstanceState::Paused,
            "completed" => InstanceState::Completed,
            "failed" => InstanceState::Failed,
            "cancelled" => InstanceState::Cancelled,
            other => {
                return Err(StorageError::Query(format!(
                    "unknown instance state: {other}"
                )))
            }
        };
        let priority = match self.priority {
            0 => orch8_types::instance::Priority::Low,
            2 => orch8_types::instance::Priority::High,
            3 => orch8_types::instance::Priority::Critical,
            _ => orch8_types::instance::Priority::Normal,
        };
        let context = serde_json::from_value(self.context)?;

        Ok(TaskInstance {
            id: InstanceId(self.id),
            sequence_id: SequenceId(self.sequence_id),
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            state,
            next_fire_at: self.next_fire_at,
            priority,
            timezone: self.timezone,
            metadata: self.metadata,
            context,
            concurrency_key: self.concurrency_key,
            max_concurrency: self.max_concurrency,
            idempotency_key: self.idempotency_key,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ExecutionNodeRow {
    id: Uuid,
    instance_id: Uuid,
    block_id: String,
    parent_id: Option<Uuid>,
    block_type: String,
    branch_index: Option<i16>,
    state: String,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
}

impl ExecutionNodeRow {
    fn into_node(self) -> ExecutionNode {
        use orch8_types::execution::BlockType;
        let block_type = match self.block_type.as_str() {
            "parallel" => BlockType::Parallel,
            "race" => BlockType::Race,
            "loop" => BlockType::Loop,
            "for_each" => BlockType::ForEach,
            "router" => BlockType::Router,
            "try_catch" => BlockType::TryCatch,
            _ => BlockType::Step, // "step" and unknown
        };
        let state = match self.state.as_str() {
            "running" => NodeState::Running,
            "completed" => NodeState::Completed,
            "failed" => NodeState::Failed,
            "cancelled" => NodeState::Cancelled,
            "skipped" => NodeState::Skipped,
            _ => NodeState::Pending, // "pending" and unknown
        };
        ExecutionNode {
            id: ExecutionNodeId(self.id),
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            parent_id: self.parent_id.map(ExecutionNodeId),
            block_type,
            branch_index: self.branch_index,
            state,
            started_at: self.started_at,
            completed_at: self.completed_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct BlockOutputRow {
    id: Uuid,
    instance_id: Uuid,
    block_id: String,
    output: serde_json::Value,
    output_ref: Option<String>,
    output_size: i32,
    attempt: i16,
    created_at: DateTime<Utc>,
}

impl BlockOutputRow {
    fn into_output(self) -> BlockOutput {
        BlockOutput {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            output: self.output,
            output_ref: self.output_ref,
            output_size: self.output_size,
            attempt: self.attempt,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct RateLimitRow {
    id: Uuid,
    #[allow(dead_code)]
    tenant_id: String,
    #[allow(dead_code)]
    resource_key: String,
    max_count: i32,
    window_seconds: i32,
    current_count: i32,
    window_start: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct SignalRow {
    id: Uuid,
    instance_id: Uuid,
    signal_type: String,
    payload: serde_json::Value,
    delivered: bool,
    created_at: DateTime<Utc>,
    delivered_at: Option<DateTime<Utc>>,
}

impl SignalRow {
    fn into_signal(self) -> Signal {
        let signal_type = match self.signal_type.as_str() {
            "pause" => orch8_types::signal::SignalType::Pause,
            "resume" => orch8_types::signal::SignalType::Resume,
            "cancel" => orch8_types::signal::SignalType::Cancel,
            "update_context" => orch8_types::signal::SignalType::UpdateContext,
            other => orch8_types::signal::SignalType::Custom(other.to_string()),
        };
        Signal {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            signal_type,
            payload: self.payload,
            delivered: self.delivered,
            created_at: self.created_at,
            delivered_at: self.delivered_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct CronRow {
    id: Uuid,
    tenant_id: String,
    namespace: String,
    sequence_id: Uuid,
    cron_expr: String,
    timezone: String,
    enabled: bool,
    metadata: serde_json::Value,
    last_triggered_at: Option<DateTime<Utc>>,
    next_fire_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl CronRow {
    fn into_schedule(self) -> CronSchedule {
        CronSchedule {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            sequence_id: SequenceId(self.sequence_id),
            cron_expr: self.cron_expr,
            timezone: self.timezone,
            enabled: self.enabled,
            metadata: self.metadata,
            last_triggered_at: self.last_triggered_at,
            next_fire_at: self.next_fire_at,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
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
