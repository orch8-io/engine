// Postgres uses i32 for integer columns; our domain types use u32.
// The values are always small enough that wrapping/sign-loss cannot occur.
#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]

mod audit;
mod checkpoints;
mod cluster;
mod cron;
mod execution_tree;
mod externalized;
mod instances;
mod misc;
mod outputs;
mod pools;
mod rate_limits;
mod rows;
mod sequences;
mod sessions;
mod signals;
mod workers;

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
use orch8_types::worker::WorkerTask;

use crate::StorageBackend;

pub struct PostgresStorage {
    pub(crate) pool: PgPool,
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
        sequences::create(self, seq).await
    }

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get(self, id).await
    }

    async fn get_sequence_by_name(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get_by_name(self, tenant_id, namespace, name, version).await
    }

    async fn list_sequence_versions(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
    ) -> Result<Vec<SequenceDefinition>, StorageError> {
        sequences::list_versions(self, tenant_id, namespace, name).await
    }

    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError> {
        sequences::deprecate(self, id).await
    }

    // === Task Instances ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        instances::create(self, instance).await
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        instances::create_batch(self, instances).await
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        instances::get(self, id).await
    }

    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::claim_due(self, now, limit, max_per_tenant).await
    }

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        instances::update_state(self, id, new_state, next_fire_at).await
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        instances::update_context(self, id, context).await
    }

    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: SequenceId,
    ) -> Result<(), StorageError> {
        instances::update_sequence(self, id, new_sequence_id).await
    }

    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        instances::merge_context_data(self, id, key, value).await
    }

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::list(self, filter, pagination).await
    }

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError> {
        instances::count(self, filter).await
    }

    async fn bulk_update_state(
        &self,
        filter: &InstanceFilter,
        new_state: InstanceState,
    ) -> Result<u64, StorageError> {
        instances::bulk_update_state(self, filter, new_state).await
    }

    async fn bulk_reschedule(
        &self,
        filter: &InstanceFilter,
        offset_secs: i64,
    ) -> Result<u64, StorageError> {
        instances::bulk_reschedule(self, filter, offset_secs).await
    }

    // === Execution Tree ===

    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError> {
        execution_tree::create_node(self, node).await
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError> {
        execution_tree::create_batch(self, nodes).await
    }

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        execution_tree::get_tree(self, instance_id).await
    }

    async fn update_node_state(
        &self,
        node_id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), StorageError> {
        execution_tree::update_node_state(self, node_id, state).await
    }

    async fn get_children(
        &self,
        parent_id: ExecutionNodeId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        execution_tree::get_children(self, parent_id).await
    }

    // === Block Outputs ===

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError> {
        outputs::save(self, output).await
    }

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError> {
        outputs::get(self, instance_id, block_id).await
    }

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_all(self, instance_id).await
    }

    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockId>, StorageError> {
        outputs::get_completed_ids(self, instance_id).await
    }

    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError> {
        outputs::get_completed_ids_batch(self, instance_ids).await
    }

    async fn save_output_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        outputs::save_output_and_transition(self, output, instance_id, new_state, next_fire_at)
            .await
    }

    // === Rate Limits ===

    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError> {
        rate_limits::check_rate_limit(self, tenant_id, resource_key, now).await
    }

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError> {
        rate_limits::upsert_rate_limit(self, limit).await
    }

    // === Signals ===

    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError> {
        signals::enqueue(self, signal).await
    }

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError> {
        signals::get_pending(self, instance_id).await
    }

    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError> {
        signals::get_pending_batch(self, instance_ids).await
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        signals::mark_delivered(self, signal_id).await
    }

    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError> {
        signals::mark_delivered_batch(self, signal_ids).await
    }

    // === Idempotency ===

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        misc::find_by_idempotency_key(self, tenant_id, idempotency_key).await
    }

    // === Concurrency ===

    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::count_running_by_concurrency_key(self, concurrency_key).await
    }

    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::concurrency_position(self, instance_id, concurrency_key).await
    }

    // === Recovery ===

    async fn recover_stale_instances(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        misc::recover_stale_instances(self, stale_threshold).await
    }

    // === Cron Schedules ===

    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError> {
        cron::create(self, schedule).await
    }

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError> {
        cron::get(self, id).await
    }

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        cron::list(self, tenant_id).await
    }

    async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError> {
        cron::update(self, schedule).await
    }

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError> {
        cron::delete(self, id).await
    }

    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        cron::claim_due(self, now).await
    }

    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        cron::update_fire_times(self, id, last_triggered_at, next_fire_at).await
    }

    // === Worker Tasks ===

    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError> {
        workers::create(self, task).await
    }

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError> {
        workers::get(self, task_id).await
    }

    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::claim(self, handler_name, worker_id, limit).await
    }

    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        workers::complete(self, task_id, worker_id, output).await
    }

    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError> {
        workers::fail(self, task_id, worker_id, message, retryable).await
    }

    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        workers::heartbeat(self, task_id, worker_id).await
    }

    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError> {
        workers::delete(self, task_id).await
    }

    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        workers::reap_stale(self, stale_threshold).await
    }

    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError> {
        workers::cancel_for_block(self, instance_id, block_id).await
    }

    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::list(self, filter, pagination).await
    }

    async fn worker_task_stats(
        &self,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
        workers::stats(self).await
    }

    // === Resource Pools ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError> {
        pools::create(self, pool).await
    }

    async fn get_resource_pool(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError> {
        pools::get(self, id).await
    }

    async fn list_resource_pools(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError> {
        pools::list(self, tenant_id).await
    }

    async fn update_pool_round_robin_index(
        &self,
        pool_id: uuid::Uuid,
        index: u32,
    ) -> Result<(), StorageError> {
        pools::update_round_robin(self, pool_id, index).await
    }

    async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        pools::delete(self, id).await
    }

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        pools::add_resource(self, resource).await
    }

    async fn list_pool_resources(
        &self,
        pool_id: uuid::Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError> {
        pools::list_resources(self, pool_id).await
    }

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        pools::update_resource(self, resource).await
    }

    async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        pools::delete_resource(self, id).await
    }

    async fn increment_resource_usage(
        &self,
        resource_id: uuid::Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError> {
        pools::increment_usage(self, resource_id, today).await
    }

    // === Checkpoints ===

    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError> {
        checkpoints::save(self, checkpoint).await
    }

    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
        checkpoints::get_latest(self, instance_id).await
    }

    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        checkpoints::list(self, instance_id).await
    }

    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError> {
        checkpoints::prune(self, instance_id, keep).await
    }

    // === Externalized State ===

    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        externalized::save(self, instance_id, ref_key, payload).await
    }

    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        externalized::get(self, ref_key).await
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        externalized::delete(self, ref_key).await
    }

    // === Audit Log ===

    async fn append_audit_log(
        &self,
        entry: &orch8_types::audit::AuditLogEntry,
    ) -> Result<(), StorageError> {
        audit::append(self, entry).await
    }

    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        audit::list_by_instance(self, instance_id, limit).await
    }

    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        audit::list_by_tenant(self, tenant_id, limit).await
    }

    // === Sessions ===

    async fn create_session(
        &self,
        session: &orch8_types::session::Session,
    ) -> Result<(), StorageError> {
        sessions::create(self, session).await
    }

    async fn get_session(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        sessions::get(self, id).await
    }

    async fn get_session_by_key(
        &self,
        tenant_id: &TenantId,
        session_key: &str,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        sessions::get_by_key(self, tenant_id, session_key).await
    }

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sessions::update_data(self, id, data).await
    }

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError> {
        sessions::update_state(self, id, state).await
    }

    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        sessions::list_instances(self, session_id).await
    }

    // === Sub-Sequences ===

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        misc::get_child_instances(self, parent_instance_id).await
    }

    // === Task Queue Routing ===

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        misc::claim_worker_tasks_from_queue(self, queue_name, handler_name, worker_id, limit).await
    }

    // === Dynamic Step Injection ===

    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError> {
        misc::inject_blocks(self, instance_id, blocks_json).await
    }

    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        misc::get_injected_blocks(self, instance_id).await
    }

    // === Cluster ===

    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError> {
        cluster::register(self, node).await
    }

    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::heartbeat(self, node_id).await
    }

    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::drain(self, node_id).await
    }

    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::deregister(self, node_id).await
    }

    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
        cluster::list(self).await
    }

    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError> {
        cluster::should_drain(self, node_id).await
    }

    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        cluster::reap_stale(self, stale_threshold).await
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError> {
        misc::ping(self).await
    }
}
