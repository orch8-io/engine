pub mod postgres;
pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use uuid::Uuid;

use orch8_types::audit::AuditLogEntry;
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
use orch8_types::session::Session;
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

    /// List all versions of a sequence by name.
    async fn list_sequence_versions(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
    ) -> Result<Vec<SequenceDefinition>, StorageError>;

    /// Mark a sequence version as deprecated.
    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError>;

    // === Task Instances ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError>;

    async fn create_instances_batch(&self, instances: &[TaskInstance])
        -> Result<u64, StorageError>;

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError>;

    /// Hot path. Uses `FOR UPDATE SKIP LOCKED` on Postgres.
    /// Returns instances with `next_fire_at <= now`, ordered by priority DESC, `next_fire_at` ASC.
    /// When `max_per_tenant > 0`, applies per-tenant fairness cap (noisy-neighbor protection).
    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
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

    /// Hot migration: rebind an instance to a different sequence version.
    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: SequenceId,
    ) -> Result<(), StorageError>;

    /// Merge a key-value pair into context->'data' (JSONB merge).
    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
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

    /// Shift `next_fire_at` by `offset_secs` for scheduled instances matching the filter.
    async fn bulk_reschedule(
        &self,
        filter: &InstanceFilter,
        offset_secs: i64,
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
    async fn reap_stale_worker_tasks(&self, stale_threshold: Duration)
        -> Result<u64, StorageError>;

    /// Delete pending/claimed worker tasks for an instance + block (used when race cancels a branch).
    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError>;

    /// List worker tasks with optional filtering and pagination.
    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Aggregate worker task statistics: counts by state, by handler, and active workers.
    async fn worker_task_stats(
        &self,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError>;

    // === Resource Pools ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError>;

    async fn get_resource_pool(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError>;

    async fn list_resource_pools(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError>;

    async fn update_pool_round_robin_index(
        &self,
        pool_id: uuid::Uuid,
        index: u32,
    ) -> Result<(), StorageError>;

    async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError>;

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError>;

    async fn list_pool_resources(
        &self,
        pool_id: uuid::Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError>;

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError>;

    async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError>;

    /// Atomically increment the daily usage counter for a resource.
    /// Resets the counter if the date has changed.
    async fn increment_resource_usage(
        &self,
        resource_id: uuid::Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError>;

    // === Checkpoints ===

    /// Save a checkpoint for an instance.
    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError>;

    /// Get the latest checkpoint for an instance.
    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError>;

    /// List all checkpoints for an instance.
    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError>;

    /// Delete old checkpoints, keeping only the latest N.
    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError>;

    // === Externalized State ===

    /// Store a large payload externally, returning the `ref_key`.
    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Retrieve an externalized payload by `ref_key`.
    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    /// Delete externalized state by `ref_key`.
    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError>;

    // === Audit Log ===

    /// Append an audit log entry.
    async fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<(), StorageError>;

    /// List audit log entries for an instance.
    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<AuditLogEntry>, StorageError>;

    /// List audit log entries for a tenant.
    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<AuditLogEntry>, StorageError>;

    // === Sessions ===

    async fn create_session(&self, session: &Session) -> Result<(), StorageError>;

    async fn get_session(&self, id: Uuid) -> Result<Option<Session>, StorageError>;

    async fn get_session_by_key(
        &self,
        tenant_id: &TenantId,
        session_key: &str,
    ) -> Result<Option<Session>, StorageError>;

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError>;

    /// Get all instances belonging to a session.
    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    // === Sub-Sequences ===

    /// Get child instances of a parent.
    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    // === Task Queue Routing ===

    /// Claim worker tasks from a specific named queue.
    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    // === Dynamic Step Injection ===

    /// Append blocks to a running instance's sequence (stored as instance-level overrides).
    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Get injected blocks for an instance.
    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    // === Cluster ===

    /// Register a new cluster node.
    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError>;

    /// Update heartbeat timestamp for a node.
    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// Set the drain flag on a node, triggering coordinated shutdown.
    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// Mark a node as stopped.
    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// List all nodes (for admin dashboard / health check).
    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError>;

    /// Check if this node should drain (returns true if `drain = true` in DB).
    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError>;

    /// Remove stale nodes that haven't heartbeated within the threshold.
    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError>;

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError>;
}
