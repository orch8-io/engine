pub mod postgres;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
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
use orch8_types::worker::WorkerTask;

/// The core storage abstraction.
///
/// Object-safe for `dyn StorageBackend` dispatch.
/// Every method returns `Result<T, StorageError>`.
/// Batch methods exist for hot paths.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    // === Sequences ===

    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError>;

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError>;

    async fn get_sequence_by_name(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError>;

    // === Task Instances ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError>;

    async fn create_instances_batch(&self, instances: &[TaskInstance])
        -> Result<u64, StorageError>;

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError>;

    /// Hot path. Uses `FOR UPDATE SKIP LOCKED` on Postgres.
    /// Returns instances with `next_fire_at <= now`, ordered by priority DESC, `next_fire_at` ASC.
    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError>;

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError>;

    async fn bulk_update_state(
        &self,
        filter: &InstanceFilter,
        new_state: InstanceState,
    ) -> Result<u64, StorageError>;

    // === Execution Tree ===

    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError>;

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError>;

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<ExecutionNode>, StorageError>;

    async fn update_node_state(
        &self,
        node_id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), StorageError>;

    async fn get_children(
        &self,
        parent_id: ExecutionNodeId,
    ) -> Result<Vec<ExecutionNode>, StorageError>;

    // === Block Outputs ===

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError>;

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError>;

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Return just the block IDs that have outputs for this instance.
    /// Lighter than `get_all_outputs` — avoids deserializing full output JSON.
    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockId>, StorageError>;

    /// Batch variant: fetch completed block IDs for multiple instances in one query.
    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError>;

    /// Atomic: save block output + update instance state in a single transaction.
    /// Eliminates one DB round-trip on the hot path.
    async fn save_output_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    // === Rate Limits ===

    /// Atomic check-and-increment. Single DB round-trip.
    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError>;

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError>;

    // === Signals ===

    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError>;

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError>;

    /// Batch variant: fetch pending signals for multiple instances in one query.
    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError>;

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError>;

    /// Batch variant: mark multiple signals delivered in one query.
    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError>;

    // === Idempotency ===

    /// Find an instance by tenant + idempotency key.
    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError>;

    // === Concurrency ===

    /// Count running instances with the given concurrency key.
    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError>;

    /// Returns the 1-based position of an instance among running instances
    /// with the same concurrency key, ordered by ID.
    /// Used to deterministically pick which instances proceed vs. defer.
    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError>;

    // === Recovery ===

    /// Find all instances that were `Running` when the engine crashed
    /// and reset them to `Scheduled` for re-execution.
    async fn recover_stale_instances(&self, stale_threshold: Duration)
        -> Result<u64, StorageError>;

    // === Cron Schedules ===

    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError>;

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<CronSchedule>, StorageError>;

    async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;

    /// Fetch all enabled cron schedules whose `next_fire_at <= now`.
    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CronSchedule>, StorageError>;

    /// After triggering, update `last_triggered_at` and `next_fire_at`.
    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    // === Worker Tasks ===

    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError>;

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError>;

    /// Atomically claim up to `limit` pending tasks for a handler.
    /// Uses `FOR UPDATE SKIP LOCKED` for safe concurrent polling.
    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Mark a claimed task as completed with output. Verifies `worker_id` ownership.
    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError>;

    /// Mark a claimed task as failed. Verifies `worker_id` ownership.
    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError>;

    /// Update heartbeat timestamp for a claimed task. Verifies `worker_id` ownership.
    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError>;

    /// Delete a worker task (used when retryable failure needs re-dispatch).
    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;

    /// Reset stale claimed tasks (no heartbeat within threshold) back to pending.
    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError>;

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError>;
}
