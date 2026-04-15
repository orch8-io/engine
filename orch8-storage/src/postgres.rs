// Postgres uses i32 for integer columns; our domain types use u32.
// The values are always small enough that wrapping/sign-loss cannot occur.
#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]

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
use orch8_types::worker::{WorkerTask, WorkerTaskState};

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
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        let row = sqlx::query_as::<_, SequenceRow>(
            "SELECT id, tenant_id, namespace, name, definition, version, deprecated, created_at FROM sequences WHERE id = $1",
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

    async fn list_sequence_versions(
        &self,
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
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(SequenceRow::into_definition)
            .collect()
    }

    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError> {
        sqlx::query("UPDATE sequences SET deprecated = TRUE WHERE id = $1")
            .bind(id.0)
            .execute(&self.pool)
            .await?;
        Ok(())
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
                     session_id, parent_instance_id,
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

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        let row = sqlx::query_as::<_, InstanceRow>(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      session_id, parent_instance_id, created_at, updated_at
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
            .fetch_all(&self.pool)
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
            .fetch_all(&self.pool)
            .await?
        };

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

    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: SequenceId,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE task_instances SET sequence_id = $2, updated_at = NOW() WHERE id = $1",
        )
        .bind(id.0)
        .bind(new_sequence_id.0)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn merge_context_data(
        &self,
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

    async fn bulk_reschedule(
        &self,
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

    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError> {
        if instance_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let uuids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
        let rows: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT instance_id, block_id FROM block_outputs WHERE instance_id = ANY($1)",
        )
        .bind(&uuids)
        .fetch_all(&self.pool)
        .await?;

        let mut map: std::collections::HashMap<InstanceId, Vec<BlockId>> =
            std::collections::HashMap::new();
        for (iid, bid) in rows {
            map.entry(InstanceId(iid))
                .or_default()
                .push(BlockId(bid));
        }
        Ok(map)
    }

    async fn save_output_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;

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
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE task_instances SET state = $2, next_fire_at = $3, updated_at = NOW() WHERE id = $1",
        )
        .bind(instance_id.0)
        .bind(new_state.to_string())
        .bind(next_fire_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
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

    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError> {
        if instance_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let uuids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
        let rows = sqlx::query_as::<_, SignalRow>(
            r"SELECT id, instance_id, signal_type, payload, delivered, created_at, delivered_at
               FROM signal_inbox WHERE instance_id = ANY($1) AND delivered = FALSE
               ORDER BY created_at",
        )
        .bind(&uuids)
        .fetch_all(&self.pool)
        .await?;

        let mut map: std::collections::HashMap<InstanceId, Vec<Signal>> =
            std::collections::HashMap::new();
        for row in rows {
            let signal = row.into_signal();
            map.entry(signal.instance_id).or_default().push(signal);
        }
        Ok(map)
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE signal_inbox SET delivered = TRUE, delivered_at = NOW() WHERE id = $1")
            .bind(signal_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError> {
        if signal_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "UPDATE signal_inbox SET delivered = TRUE, delivered_at = NOW() WHERE id = ANY($1)",
        )
        .bind(signal_ids)
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
                      session_id, parent_instance_id, created_at, updated_at
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
            WHERE state IN ('running', 'waiting')
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

    // === Worker Tasks ===

    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO worker_tasks
                (id, instance_id, block_id, handler_name, queue_name, params, context,
                 attempt, timeout_ms, state, created_at)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
              ON CONFLICT (instance_id, block_id) DO NOTHING",
        )
        .bind(task.id)
        .bind(task.instance_id.0)
        .bind(&task.block_id.0)
        .bind(&task.handler_name)
        .bind(&task.queue_name)
        .bind(&task.params)
        .bind(&task.context)
        .bind(task.attempt)
        .bind(task.timeout_ms)
        .bind(task.state.to_string())
        .bind(task.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError> {
        let row = sqlx::query_as::<_, WorkerTaskRow>(
            r"SELECT id, instance_id, block_id, handler_name, params, context,
                     attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                     completed_at, output, error_message, error_retryable, created_at
              FROM worker_tasks WHERE id = $1",
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(WorkerTaskRow::into_task))
    }

    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        let rows = sqlx::query_as::<_, WorkerTaskRow>(
            r"UPDATE worker_tasks
              SET state = 'claimed', worker_id = $2, claimed_at = NOW(), heartbeat_at = NOW()
              WHERE id IN (
                  SELECT id FROM worker_tasks
                  WHERE handler_name = $1 AND state = 'pending'
                  ORDER BY created_at
                  LIMIT $3
                  FOR UPDATE SKIP LOCKED
              )
              RETURNING id, instance_id, block_id, handler_name, params, context,
                        attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                        completed_at, output, error_message, error_retryable, created_at",
        )
        .bind(handler_name)
        .bind(worker_id)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
    }

    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            r"UPDATE worker_tasks
              SET state = 'completed', output = $3, completed_at = NOW()
              WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
        )
        .bind(task_id)
        .bind(worker_id)
        .bind(output)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            r"UPDATE worker_tasks
              SET state = 'failed', error_message = $3, error_retryable = $4, completed_at = NOW()
              WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
        )
        .bind(task_id)
        .bind(worker_id)
        .bind(message)
        .bind(retryable)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE worker_tasks SET heartbeat_at = NOW() WHERE id = $1 AND worker_id = $2 AND state = 'claimed'",
        )
        .bind(task_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM worker_tasks WHERE id = $1")
            .bind(task_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        let threshold_secs = stale_threshold.as_secs_f64();
        let result = sqlx::query(
            r"UPDATE worker_tasks
              SET state = 'pending', worker_id = NULL, claimed_at = NULL, heartbeat_at = NULL
              WHERE state = 'claimed'
                AND heartbeat_at < NOW() - make_interval(secs => $1::double precision)",
        )
        .bind(threshold_secs)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM worker_tasks WHERE instance_id = $1 AND block_id = $2 AND state IN ('pending', 'claimed')",
        )
        .bind(instance_id)
        .bind(block_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        let mut qb = sqlx::QueryBuilder::new(
            r"SELECT id, instance_id, block_id, handler_name, queue_name, params, context,
                     attempt, timeout_ms, state, worker_id, claimed_at, heartbeat_at,
                     completed_at, output, error_message, error_retryable, created_at
               FROM worker_tasks WHERE 1=1",
        );
        apply_worker_task_filter(&mut qb, filter);
        qb.push(" ORDER BY created_at DESC");
        qb.push(" LIMIT ")
            .push_bind(i64::from(pagination.limit.min(1000)));
        qb.push(" OFFSET ")
            .push_bind(i64::try_from(pagination.offset).unwrap_or(i64::MAX));

        let rows = qb
            .build_query_as::<WorkerTaskRow>()
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
    }

    async fn worker_task_stats(
        &self,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
        // Count by state + handler_name
        let counts: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT state, handler_name, COUNT(*) as cnt FROM worker_tasks GROUP BY state, handler_name",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut by_state = std::collections::HashMap::<String, u64>::new();
        let mut by_handler =
            std::collections::HashMap::<String, std::collections::HashMap<String, u64>>::new();
        for (state, handler, cnt) in counts {
            #[allow(clippy::cast_sign_loss)]
            let cnt = cnt.max(0) as u64;
            *by_state.entry(state.clone()).or_default() += cnt;
            *by_handler
                .entry(handler)
                .or_default()
                .entry(state)
                .or_default() += cnt;
        }

        // Active workers
        let workers: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT worker_id FROM worker_tasks WHERE state = 'claimed' AND worker_id IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;

        let active_workers = workers.into_iter().map(|(w,)| w).collect();

        Ok(orch8_types::worker_filter::WorkerTaskStats {
            by_state,
            by_handler,
            active_workers,
        })
    }

    // === Resource Pools ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO resource_pools (id, tenant_id, name, strategy, round_robin_index, created_at, updated_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(pool.id)
        .bind(&pool.tenant_id.0)
        .bind(&pool.name)
        .bind(serde_json::to_string(&pool.strategy).unwrap_or_default().trim_matches('"'))
        .bind(pool.round_robin_index as i32)
        .bind(pool.created_at)
        .bind(pool.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_resource_pool(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError> {
        let row = sqlx::query_as::<_, ResourcePoolRow>(
            "SELECT id, tenant_id, name, strategy, round_robin_index, created_at, updated_at FROM resource_pools WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(ResourcePoolRow::into_pool))
    }

    async fn list_resource_pools(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError> {
        let rows = sqlx::query_as::<_, ResourcePoolRow>(
            "SELECT id, tenant_id, name, strategy, round_robin_index, created_at, updated_at FROM resource_pools WHERE tenant_id = $1 ORDER BY name",
        )
        .bind(&tenant_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ResourcePoolRow::into_pool).collect())
    }

    async fn update_pool_round_robin_index(
        &self,
        pool_id: uuid::Uuid,
        index: u32,
    ) -> Result<(), StorageError> {
        sqlx::query("UPDATE resource_pools SET round_robin_index = $2, updated_at = NOW() WHERE id = $1")
            .bind(pool_id)
            .bind(index as i32)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM resource_pools WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO pool_resources
              (id, pool_id, resource_key, name, weight, enabled, daily_cap, daily_usage, daily_usage_date,
               warmup_start, warmup_days, warmup_start_cap, created_at)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(resource.id)
        .bind(resource.pool_id)
        .bind(&resource.resource_key.0)
        .bind(&resource.name)
        .bind(resource.weight as i32)
        .bind(resource.enabled)
        .bind(resource.daily_cap as i32)
        .bind(resource.daily_usage as i32)
        .bind(resource.daily_usage_date)
        .bind(resource.warmup_start)
        .bind(resource.warmup_days as i32)
        .bind(resource.warmup_start_cap as i32)
        .bind(resource.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_pool_resources(
        &self,
        pool_id: uuid::Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError> {
        let rows = sqlx::query_as::<_, PoolResourceRow>(
            r"SELECT id, pool_id, resource_key, name, weight, enabled, daily_cap, daily_usage, daily_usage_date,
                     warmup_start, warmup_days, warmup_start_cap, created_at
              FROM pool_resources WHERE pool_id = $1 ORDER BY name",
        )
        .bind(pool_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(PoolResourceRow::into_resource).collect())
    }

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"UPDATE pool_resources
              SET name = $2, weight = $3, enabled = $4, daily_cap = $5,
                  warmup_start = $6, warmup_days = $7, warmup_start_cap = $8
              WHERE id = $1",
        )
        .bind(resource.id)
        .bind(&resource.name)
        .bind(resource.weight as i32)
        .bind(resource.enabled)
        .bind(resource.daily_cap as i32)
        .bind(resource.warmup_start)
        .bind(resource.warmup_days as i32)
        .bind(resource.warmup_start_cap as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM pool_resources WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn increment_resource_usage(
        &self,
        resource_id: uuid::Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"UPDATE pool_resources
              SET daily_usage = CASE WHEN daily_usage_date = $2 THEN daily_usage + 1 ELSE 1 END,
                  daily_usage_date = $2
              WHERE id = $1",
        )
        .bind(resource_id)
        .bind(today)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // === Checkpoints ===

    async fn save_checkpoint(
        &self,
        cp: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO checkpoints (id, instance_id, checkpoint_data, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(cp.id)
        .bind(cp.instance_id.0)
        .bind(&cp.checkpoint_data)
        .bind(cp.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
        let row: Option<(Uuid, Uuid, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, instance_id, checkpoint_data, created_at FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(instance_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(id, inst_id, data, created_at)| orch8_types::checkpoint::Checkpoint {
            id,
            instance_id: InstanceId(inst_id),
            checkpoint_data: data,
            created_at,
        }))
    }

    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        let rows: Vec<(Uuid, Uuid, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
            "SELECT id, instance_id, checkpoint_data, created_at FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC",
        )
        .bind(instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, inst_id, data, created_at)| orch8_types::checkpoint::Checkpoint {
                id,
                instance_id: InstanceId(inst_id),
                checkpoint_data: data,
                created_at,
            })
            .collect())
    }

    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            r"DELETE FROM checkpoints WHERE instance_id = $1 AND id NOT IN (
                SELECT id FROM checkpoints WHERE instance_id = $1 ORDER BY created_at DESC LIMIT $2
              )",
        )
        .bind(instance_id.0)
        .bind(i64::from(keep))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    // === Externalized State ===

    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO externalized_state (id, instance_id, ref_key, payload, created_at)
              VALUES ($1, $2, $3, $4, NOW())
              ON CONFLICT (ref_key) DO UPDATE SET payload = $4",
        )
        .bind(Uuid::new_v4())
        .bind(instance_id.0)
        .bind(ref_key)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let row: Option<(serde_json::Value,)> =
            sqlx::query_as("SELECT payload FROM externalized_state WHERE ref_key = $1")
                .bind(ref_key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM externalized_state WHERE ref_key = $1")
            .bind(ref_key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // === Health ===

    // === Audit Log ===

    async fn append_audit_log(
        &self,
        entry: &orch8_types::audit::AuditLogEntry,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO audit_log (id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        )
        .bind(entry.id)
        .bind(entry.instance_id.0)
        .bind(&entry.tenant_id.0)
        .bind(&entry.event_type)
        .bind(&entry.from_state)
        .bind(&entry.to_state)
        .bind(&entry.block_id)
        .bind(&entry.details)
        .bind(entry.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        let rows = sqlx::query_as::<_, AuditLogRow>(
            r"SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
              FROM audit_log WHERE instance_id = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(instance_id.0)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(AuditLogRow::into_entry).collect())
    }

    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        let rows = sqlx::query_as::<_, AuditLogRow>(
            r"SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
              FROM audit_log WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
        )
        .bind(&tenant_id.0)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(AuditLogRow::into_entry).collect())
    }

    // === Sessions ===

    async fn create_session(
        &self,
        session: &orch8_types::session::Session,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO sessions (id, tenant_id, session_key, data, state, created_at, updated_at, expires_at)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
        )
        .bind(session.id)
        .bind(&session.tenant_id.0)
        .bind(&session.session_key)
        .bind(&session.data)
        .bind(session.state.to_string())
        .bind(session.created_at)
        .bind(session.updated_at)
        .bind(session.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_session(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r"SELECT id, tenant_id, session_key, data, state, created_at, updated_at, expires_at
              FROM sessions WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(SessionRow::into_session))
    }

    async fn get_session_by_key(
        &self,
        tenant_id: &TenantId,
        session_key: &str,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r"SELECT id, tenant_id, session_key, data, state, created_at, updated_at, expires_at
              FROM sessions WHERE tenant_id = $1 AND session_key = $2",
        )
        .bind(&tenant_id.0)
        .bind(session_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(SessionRow::into_session))
    }

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sqlx::query("UPDATE sessions SET data = $2, updated_at = NOW() WHERE id = $1")
            .bind(id)
            .bind(data)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError> {
        sqlx::query("UPDATE sessions SET state = $2, updated_at = NOW() WHERE id = $1")
            .bind(id)
            .bind(state.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let rows = sqlx::query_as::<_, InstanceRow>(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      session_id, parent_instance_id, created_at, updated_at
               FROM task_instances WHERE session_id = $1 ORDER BY created_at",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(InstanceRow::into_instance).collect()
    }

    // === Sub-Sequences ===

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let rows = sqlx::query_as::<_, InstanceRow>(
            r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                      priority, timezone, metadata, context,
                      concurrency_key, max_concurrency, idempotency_key,
                      session_id, parent_instance_id, created_at, updated_at
               FROM task_instances WHERE parent_instance_id = $1 ORDER BY created_at",
        )
        .bind(parent_instance_id.0)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(InstanceRow::into_instance).collect()
    }

    // === Task Queue Routing ===

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        let rows = sqlx::query_as::<_, WorkerTaskRow>(
            r"
            UPDATE worker_tasks
            SET state = 'claimed', worker_id = $4, claimed_at = NOW(), heartbeat_at = NOW()
            WHERE id IN (
                SELECT id FROM worker_tasks
                WHERE handler_name = $1 AND state = 'pending' AND queue_name = $5
                ORDER BY created_at ASC
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            ",
        )
        .bind(handler_name)
        .bind(worker_id)
        .bind(i64::from(limit))
        .bind(worker_id)
        .bind(queue_name)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(WorkerTaskRow::into_task).collect())
    }

    // === Dynamic Step Injection ===

    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError> {
        // Store injected blocks in instance metadata under `_injected_blocks`.
        sqlx::query(
            r"UPDATE task_instances
              SET metadata = jsonb_set(COALESCE(metadata, '{}'), '{_injected_blocks}',
                  COALESCE(metadata->'_injected_blocks', '[]'::jsonb) || $2),
                  updated_at = NOW()
              WHERE id = $1",
        )
        .bind(instance_id.0)
        .bind(blocks_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT metadata->'_injected_blocks' FROM task_instances WHERE id = $1",
        )
        .bind(instance_id.0)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(v,)| if v.is_null() { None } else { Some(v) }))
    }

    // === Cluster ===

    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO cluster_nodes (id, name, status, registered_at, last_heartbeat_at, drain)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (id) DO UPDATE SET status = $3, last_heartbeat_at = $5, drain = $6",
        )
        .bind(node.id)
        .bind(&node.name)
        .bind(node.status.to_string())
        .bind(node.registered_at)
        .bind(node.last_heartbeat_at)
        .bind(node.drain)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn heartbeat_node(&self, node_id: uuid::Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE cluster_nodes SET last_heartbeat_at = NOW() WHERE id = $1")
            .bind(node_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn drain_node(&self, node_id: uuid::Uuid) -> Result<(), StorageError> {
        sqlx::query(
            "UPDATE cluster_nodes SET drain = TRUE, status = 'draining' WHERE id = $1",
        )
        .bind(node_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn deregister_node(&self, node_id: uuid::Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE cluster_nodes SET status = 'stopped' WHERE id = $1")
            .bind(node_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
        let rows = sqlx::query_as::<_, ClusterNodeRow>(
            "SELECT id, name, status, registered_at, last_heartbeat_at, drain
             FROM cluster_nodes ORDER BY registered_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ClusterNodeRow::into_node).collect())
    }

    async fn should_drain(&self, node_id: uuid::Uuid) -> Result<bool, StorageError> {
        let row: (bool,) =
            sqlx::query_as("SELECT drain FROM cluster_nodes WHERE id = $1")
                .bind(node_id)
                .fetch_optional(&self.pool)
                .await?
                .unwrap_or((false,));
        Ok(row.0)
    }

    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        let threshold_secs = stale_threshold.as_secs() as i64;
        let result = sqlx::query(
            "UPDATE cluster_nodes SET status = 'stopped'
             WHERE status = 'active'
               AND last_heartbeat_at < NOW() - make_interval(secs => $1)",
        )
        .bind(threshold_secs)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
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
    deprecated: bool,
    created_at: DateTime<Utc>,
}

impl SequenceRow {
    fn into_definition(self) -> Result<SequenceDefinition, StorageError> {
        // Support both old format (array of blocks) and new format ({blocks, interceptors}).
        let (blocks, interceptors) = if self.definition.is_array() {
            (serde_json::from_value(self.definition)?, None)
        } else {
            let blocks = serde_json::from_value(
                self.definition.get("blocks").cloned().unwrap_or(serde_json::Value::Array(vec![])),
            )?;
            let interceptors = self.definition.get("interceptors")
                .and_then(|v| if v.is_null() { None } else { serde_json::from_value(v.clone()).ok() });
            (blocks, interceptors)
        };
        Ok(SequenceDefinition {
            id: SequenceId(self.id),
            tenant_id: TenantId(self.tenant_id),
            namespace: Namespace(self.namespace),
            name: self.name,
            version: self.version,
            deprecated: self.deprecated,
            blocks,
            interceptors,
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
    session_id: Option<Uuid>,
    parent_instance_id: Option<Uuid>,
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
            session_id: self.session_id,
            parent_instance_id: self.parent_instance_id.map(InstanceId),
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
            "sub_sequence" => BlockType::SubSequence,
            "ab_split" => BlockType::ABSplit,
            _ => BlockType::Step, // "step" and unknown
        };
        let state = match self.state.as_str() {
            "running" => NodeState::Running,
            "waiting" => NodeState::Waiting,
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

#[derive(sqlx::FromRow)]
struct WorkerTaskRow {
    id: Uuid,
    instance_id: Uuid,
    block_id: String,
    handler_name: String,
    queue_name: Option<String>,
    params: serde_json::Value,
    context: serde_json::Value,
    attempt: i16,
    timeout_ms: Option<i64>,
    state: String,
    worker_id: Option<String>,
    claimed_at: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    output: Option<serde_json::Value>,
    error_message: Option<String>,
    error_retryable: Option<bool>,
    created_at: DateTime<Utc>,
}

impl WorkerTaskRow {
    fn into_task(self) -> WorkerTask {
        let state = match self.state.as_str() {
            "claimed" => WorkerTaskState::Claimed,
            "completed" => WorkerTaskState::Completed,
            "failed" => WorkerTaskState::Failed,
            _ => WorkerTaskState::Pending,
        };
        WorkerTask {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            block_id: BlockId(self.block_id),
            handler_name: self.handler_name,
            queue_name: self.queue_name,
            params: self.params,
            context: self.context,
            attempt: self.attempt,
            timeout_ms: self.timeout_ms,
            state,
            worker_id: self.worker_id,
            claimed_at: self.claimed_at,
            heartbeat_at: self.heartbeat_at,
            completed_at: self.completed_at,
            output: self.output,
            error_message: self.error_message,
            error_retryable: self.error_retryable,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ResourcePoolRow {
    id: uuid::Uuid,
    tenant_id: String,
    name: String,
    strategy: String,
    round_robin_index: i32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ResourcePoolRow {
    fn into_pool(self) -> orch8_types::pool::ResourcePool {
        orch8_types::pool::ResourcePool {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            name: self.name,
            strategy: match self.strategy.as_str() {
                "weighted" => orch8_types::pool::RotationStrategy::Weighted,
                "random" => orch8_types::pool::RotationStrategy::Random,
                _ => orch8_types::pool::RotationStrategy::RoundRobin,
            },
            round_robin_index: self.round_robin_index as u32,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct PoolResourceRow {
    id: uuid::Uuid,
    pool_id: uuid::Uuid,
    resource_key: String,
    name: String,
    weight: i32,
    enabled: bool,
    daily_cap: i32,
    daily_usage: i32,
    daily_usage_date: Option<chrono::NaiveDate>,
    warmup_start: Option<chrono::NaiveDate>,
    warmup_days: i32,
    warmup_start_cap: i32,
    created_at: DateTime<Utc>,
}

impl PoolResourceRow {
    fn into_resource(self) -> orch8_types::pool::PoolResource {
        orch8_types::pool::PoolResource {
            id: self.id,
            pool_id: self.pool_id,
            resource_key: orch8_types::ids::ResourceKey(self.resource_key),
            name: self.name,
            weight: self.weight as u32,
            enabled: self.enabled,
            daily_cap: self.daily_cap as u32,
            daily_usage: self.daily_usage as u32,
            daily_usage_date: self.daily_usage_date,
            warmup_start: self.warmup_start,
            warmup_days: self.warmup_days as u32,
            warmup_start_cap: self.warmup_start_cap as u32,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct AuditLogRow {
    id: Uuid,
    instance_id: Uuid,
    tenant_id: String,
    event_type: String,
    from_state: Option<String>,
    to_state: Option<String>,
    block_id: Option<String>,
    details: serde_json::Value,
    created_at: DateTime<Utc>,
}

impl AuditLogRow {
    fn into_entry(self) -> orch8_types::audit::AuditLogEntry {
        orch8_types::audit::AuditLogEntry {
            id: self.id,
            instance_id: InstanceId(self.instance_id),
            tenant_id: TenantId(self.tenant_id),
            event_type: self.event_type,
            from_state: self.from_state,
            to_state: self.to_state,
            block_id: self.block_id,
            details: self.details,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SessionRow {
    id: Uuid,
    tenant_id: String,
    session_key: String,
    data: serde_json::Value,
    state: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
}

impl SessionRow {
    fn into_session(self) -> orch8_types::session::Session {
        let state = match self.state.as_str() {
            "completed" => orch8_types::session::SessionState::Completed,
            "expired" => orch8_types::session::SessionState::Expired,
            _ => orch8_types::session::SessionState::Active,
        };
        orch8_types::session::Session {
            id: self.id,
            tenant_id: TenantId(self.tenant_id),
            session_key: self.session_key,
            data: self.data,
            state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            expires_at: self.expires_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ClusterNodeRow {
    id: Uuid,
    name: String,
    status: String,
    registered_at: DateTime<Utc>,
    last_heartbeat_at: DateTime<Utc>,
    drain: bool,
}

impl ClusterNodeRow {
    fn into_node(self) -> orch8_types::cluster::ClusterNode {
        use orch8_types::cluster::NodeStatus;
        let status = match self.status.as_str() {
            "draining" => NodeStatus::Draining,
            "stopped" => NodeStatus::Stopped,
            _ => NodeStatus::Active,
        };
        orch8_types::cluster::ClusterNode {
            id: self.id,
            name: self.name,
            status,
            registered_at: self.registered_at,
            last_heartbeat_at: self.last_heartbeat_at,
            drain: self.drain,
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

/// Apply `WorkerTaskFilter` conditions to a query builder.
fn apply_worker_task_filter(
    qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    filter: &orch8_types::worker_filter::WorkerTaskFilter,
) {
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let state_strings: Vec<String> = states.iter().map(ToString::to_string).collect();
            qb.push(" AND state = ANY(")
                .push_bind(state_strings)
                .push(")");
        }
    }
    if let Some(ref handler) = filter.handler_name {
        qb.push(" AND handler_name = ").push_bind(handler.clone());
    }
    if let Some(ref wid) = filter.worker_id {
        qb.push(" AND worker_id = ").push_bind(wid.clone());
    }
    if let Some(ref queue) = filter.queue_name {
        qb.push(" AND queue_name = ").push_bind(queue.clone());
    }
}
