//! Encrypting storage decorator.
//!
//! Wraps any `StorageBackend` and transparently encrypts `context.data` on write
//! and decrypts on read using AES-256-GCM via [`FieldEncryptor`].

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::encryption::FieldEncryptor;
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::TaskInstance;

use crate::StorageBackend;

/// Wraps an inner `StorageBackend` and encrypts/decrypts `context.data` transparently.
pub struct EncryptingStorage {
    inner: Arc<dyn StorageBackend>,
    encryptor: FieldEncryptor,
}

impl EncryptingStorage {
    pub fn new(inner: Arc<dyn StorageBackend>, encryptor: FieldEncryptor) -> Self {
        Self { inner, encryptor }
    }

    fn encrypt_instance(&self, instance: &TaskInstance) -> Result<TaskInstance, StorageError> {
        let mut inst = instance.clone();
        inst.context.data = self
            .encryptor
            .encrypt_value(&inst.context.data)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        Ok(inst)
    }

    fn decrypt_instance(&self, instance: &mut TaskInstance) -> Result<(), StorageError> {
        if FieldEncryptor::is_encrypted(&instance.context.data) {
            instance.context.data = self
                .encryptor
                .decrypt_value(&instance.context.data)
                .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        }
        Ok(())
    }

    fn decrypt_instances(&self, instances: &mut [TaskInstance]) -> Result<(), StorageError> {
        for inst in instances.iter_mut() {
            self.decrypt_instance(inst)?;
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for EncryptingStorage {
    // === Sequences (pass-through) ===

    async fn create_sequence(
        &self,
        seq: &orch8_types::sequence::SequenceDefinition,
    ) -> Result<(), StorageError> {
        self.inner.create_sequence(seq).await
    }

    async fn get_sequence(
        &self,
        id: orch8_types::ids::SequenceId,
    ) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
        self.inner.get_sequence(id).await
    }

    async fn get_sequence_by_name(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        namespace: &orch8_types::ids::Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
        self.inner
            .get_sequence_by_name(tenant_id, namespace, name, version)
            .await
    }

    async fn list_sequence_versions(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        namespace: &orch8_types::ids::Namespace,
        name: &str,
    ) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
        self.inner
            .list_sequence_versions(tenant_id, namespace, name)
            .await
    }

    async fn deprecate_sequence(
        &self,
        id: orch8_types::ids::SequenceId,
    ) -> Result<(), StorageError> {
        self.inner.deprecate_sequence(id).await
    }

    // === Instances (encrypt/decrypt context.data) ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        let encrypted = self.encrypt_instance(instance)?;
        self.inner.create_instance(&encrypted).await
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        let encrypted: Vec<TaskInstance> = instances
            .iter()
            .map(|i| self.encrypt_instance(i))
            .collect::<Result<_, _>>()?;
        self.inner.create_instances_batch(&encrypted).await
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        let mut inst = self.inner.get_instance(id).await?;
        if let Some(ref mut i) = inst {
            self.decrypt_instance(i)?;
        }
        Ok(inst)
    }

    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self
            .inner
            .claim_due_instances(now, limit, max_per_tenant)
            .await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        self.inner
            .update_instance_state(id, new_state, next_fire_at)
            .await
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        let mut ctx = context.clone();
        ctx.data = self
            .encryptor
            .encrypt_value(&ctx.data)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        self.inner.update_instance_context(id, &ctx).await
    }

    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: orch8_types::ids::SequenceId,
    ) -> Result<(), StorageError> {
        self.inner
            .update_instance_sequence(id, new_sequence_id)
            .await
    }

    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        let encrypted = self
            .encryptor
            .encrypt_value(value)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        self.inner.merge_context_data(id, key, &encrypted).await
    }

    async fn list_instances(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.list_instances(filter, pagination).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    async fn count_instances(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
    ) -> Result<u64, StorageError> {
        self.inner.count_instances(filter).await
    }

    async fn bulk_update_state(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
        new_state: orch8_types::instance::InstanceState,
    ) -> Result<u64, StorageError> {
        self.inner.bulk_update_state(filter, new_state).await
    }

    async fn bulk_reschedule(
        &self,
        filter: &orch8_types::filter::InstanceFilter,
        offset_secs: i64,
    ) -> Result<u64, StorageError> {
        self.inner.bulk_reschedule(filter, offset_secs).await
    }

    // === Execution Tree (pass-through) ===

    async fn create_execution_node(
        &self,
        node: &orch8_types::execution::ExecutionNode,
    ) -> Result<(), StorageError> {
        self.inner.create_execution_node(node).await
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[orch8_types::execution::ExecutionNode],
    ) -> Result<(), StorageError> {
        self.inner.create_execution_nodes_batch(nodes).await
    }

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError> {
        self.inner.get_execution_tree(instance_id).await
    }

    async fn update_node_state(
        &self,
        node_id: orch8_types::ids::ExecutionNodeId,
        state: orch8_types::execution::NodeState,
    ) -> Result<(), StorageError> {
        self.inner.update_node_state(node_id, state).await
    }

    async fn get_children(
        &self,
        parent_id: orch8_types::ids::ExecutionNodeId,
    ) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError> {
        self.inner.get_children(parent_id).await
    }

    // === Block Outputs (pass-through) ===

    async fn save_block_output(
        &self,
        output: &orch8_types::output::BlockOutput,
    ) -> Result<(), StorageError> {
        self.inner.save_block_output(output).await
    }

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &orch8_types::ids::BlockId,
    ) -> Result<Option<orch8_types::output::BlockOutput>, StorageError> {
        self.inner.get_block_output(instance_id, block_id).await
    }

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        self.inner.get_all_outputs(instance_id).await
    }

    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::ids::BlockId>, StorageError> {
        self.inner.get_completed_block_ids(instance_id).await
    }

    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::ids::BlockId>>, StorageError>
    {
        self.inner.get_completed_block_ids_batch(instance_ids).await
    }

    async fn save_output_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        instance_id: InstanceId,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        self.inner
            .save_output_and_transition(output, instance_id, new_state, next_fire_at)
            .await
    }

    // === Rate Limits (pass-through) ===

    async fn check_rate_limit(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        resource_key: &orch8_types::ids::ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<orch8_types::rate_limit::RateLimitCheck, StorageError> {
        self.inner
            .check_rate_limit(tenant_id, resource_key, now)
            .await
    }

    async fn upsert_rate_limit(
        &self,
        limit: &orch8_types::rate_limit::RateLimit,
    ) -> Result<(), StorageError> {
        self.inner.upsert_rate_limit(limit).await
    }

    // === Signals (pass-through) ===

    async fn enqueue_signal(
        &self,
        signal: &orch8_types::signal::Signal,
    ) -> Result<(), StorageError> {
        self.inner.enqueue_signal(signal).await
    }

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::signal::Signal>, StorageError> {
        self.inner.get_pending_signals(instance_id).await
    }

    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::signal::Signal>>, StorageError>
    {
        self.inner.get_pending_signals_batch(instance_ids).await
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        self.inner.mark_signal_delivered(signal_id).await
    }

    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError> {
        self.inner.mark_signals_delivered(signal_ids).await
    }

    // === Idempotency ===

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        let mut inst = self
            .inner
            .find_by_idempotency_key(tenant_id, idempotency_key)
            .await?;
        if let Some(ref mut i) = inst {
            self.decrypt_instance(i)?;
        }
        Ok(inst)
    }

    // === Concurrency (pass-through) ===

    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        self.inner
            .count_running_by_concurrency_key(concurrency_key)
            .await
    }

    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        self.inner
            .concurrency_position(instance_id, concurrency_key)
            .await
    }

    // === Recovery (pass-through) ===

    async fn recover_stale_instances(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        self.inner.recover_stale_instances(stale_threshold).await
    }

    // === Cron Schedules (pass-through) ===

    async fn create_cron_schedule(
        &self,
        schedule: &orch8_types::cron::CronSchedule,
    ) -> Result<(), StorageError> {
        self.inner.create_cron_schedule(schedule).await
    }

    async fn get_cron_schedule(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::cron::CronSchedule>, StorageError> {
        self.inner.get_cron_schedule(id).await
    }

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError> {
        self.inner.list_cron_schedules(tenant_id).await
    }

    async fn update_cron_schedule(
        &self,
        schedule: &orch8_types::cron::CronSchedule,
    ) -> Result<(), StorageError> {
        self.inner.update_cron_schedule(schedule).await
    }

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError> {
        self.inner.delete_cron_schedule(id).await
    }

    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError> {
        self.inner.claim_due_cron_schedules(now).await
    }

    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.inner
            .update_cron_fire_times(id, last_triggered_at, next_fire_at)
            .await
    }

    // === Worker Tasks (pass-through) ===

    async fn create_worker_task(
        &self,
        task: &orch8_types::worker::WorkerTask,
    ) -> Result<(), StorageError> {
        self.inner.create_worker_task(task).await
    }

    async fn get_worker_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner.get_worker_task(task_id).await
    }

    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner
            .claim_worker_tasks(handler_name, worker_id, limit)
            .await
    }

    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        self.inner
            .complete_worker_task(task_id, worker_id, output)
            .await
    }

    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError> {
        self.inner
            .fail_worker_task(task_id, worker_id, message, retryable)
            .await
    }

    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        self.inner.heartbeat_worker_task(task_id, worker_id).await
    }

    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError> {
        self.inner.delete_worker_task(task_id).await
    }

    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        self.inner.reap_stale_worker_tasks(stale_threshold).await
    }

    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError> {
        self.inner
            .cancel_worker_tasks_for_block(instance_id, block_id)
            .await
    }

    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner.list_worker_tasks(filter, pagination).await
    }

    async fn worker_task_stats(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
        self.inner.worker_task_stats(tenant_id).await
    }

    // === Resource Pools (pass-through) ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError> {
        self.inner.create_resource_pool(pool).await
    }

    async fn get_resource_pool(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError> {
        self.inner.get_resource_pool(id).await
    }

    async fn list_resource_pools(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError> {
        self.inner.list_resource_pools(tenant_id).await
    }

    async fn update_pool_round_robin_index(
        &self,
        pool_id: Uuid,
        index: u32,
    ) -> Result<(), StorageError> {
        self.inner
            .update_pool_round_robin_index(pool_id, index)
            .await
    }

    async fn delete_resource_pool(&self, id: Uuid) -> Result<(), StorageError> {
        self.inner.delete_resource_pool(id).await
    }

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        self.inner.add_pool_resource(resource).await
    }

    async fn list_pool_resources(
        &self,
        pool_id: Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError> {
        self.inner.list_pool_resources(pool_id).await
    }

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        self.inner.update_pool_resource(resource).await
    }

    async fn delete_pool_resource(&self, id: Uuid) -> Result<(), StorageError> {
        self.inner.delete_pool_resource(id).await
    }

    async fn increment_resource_usage(
        &self,
        resource_id: Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError> {
        self.inner
            .increment_resource_usage(resource_id, today)
            .await
    }

    // === Checkpoints (pass-through) ===

    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError> {
        self.inner.save_checkpoint(checkpoint).await
    }

    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
        self.inner.get_latest_checkpoint(instance_id).await
    }

    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        self.inner.list_checkpoints(instance_id).await
    }

    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError> {
        self.inner.prune_checkpoints(instance_id, keep).await
    }

    // === Externalized State (pass-through) ===

    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.inner
            .save_externalized_state(instance_id, ref_key, payload)
            .await
    }

    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.inner.get_externalized_state(ref_key).await
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        self.inner.delete_externalized_state(ref_key).await
    }

    // === Audit Log (pass-through) ===

    async fn append_audit_log(
        &self,
        entry: &orch8_types::audit::AuditLogEntry,
    ) -> Result<(), StorageError> {
        self.inner.append_audit_log(entry).await
    }

    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        self.inner.list_audit_log(instance_id, limit).await
    }

    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        self.inner.list_audit_log_by_tenant(tenant_id, limit).await
    }

    // === Sessions ===

    async fn create_session(
        &self,
        session: &orch8_types::session::Session,
    ) -> Result<(), StorageError> {
        self.inner.create_session(session).await
    }

    async fn get_session(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        self.inner.get_session(id).await
    }

    async fn get_session_by_key(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        session_key: &str,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        self.inner.get_session_by_key(tenant_id, session_key).await
    }

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.inner.update_session_data(id, data).await
    }

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError> {
        self.inner.update_session_state(id, state).await
    }

    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.list_session_instances(session_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    // === Sub-Sequences ===

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.get_child_instances(parent_instance_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    // === Task Queue Routing (pass-through) ===

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner
            .claim_worker_tasks_from_queue(queue_name, handler_name, worker_id, limit)
            .await
    }

    // === Dynamic Step Injection (pass-through) ===

    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.inner.inject_blocks(instance_id, blocks_json).await
    }

    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.inner.get_injected_blocks(instance_id).await
    }

    // === Cluster (pass-through) ===

    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError> {
        self.inner.register_node(node).await
    }

    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        self.inner.heartbeat_node(node_id).await
    }

    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        self.inner.drain_node(node_id).await
    }

    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        self.inner.deregister_node(node_id).await
    }

    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
        self.inner.list_nodes().await
    }

    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError> {
        self.inner.should_drain(node_id).await
    }

    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        self.inner.reap_stale_nodes(stale_threshold).await
    }

    // === Plugins (pass-through) ===

    async fn create_plugin(
        &self,
        plugin: &orch8_types::plugin::PluginDef,
    ) -> Result<(), StorageError> {
        self.inner.create_plugin(plugin).await
    }

    async fn get_plugin(
        &self,
        name: &str,
    ) -> Result<Option<orch8_types::plugin::PluginDef>, StorageError> {
        self.inner.get_plugin(name).await
    }

    async fn list_plugins(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<Vec<orch8_types::plugin::PluginDef>, StorageError> {
        self.inner.list_plugins(tenant_id).await
    }

    async fn update_plugin(
        &self,
        plugin: &orch8_types::plugin::PluginDef,
    ) -> Result<(), StorageError> {
        self.inner.update_plugin(plugin).await
    }

    async fn delete_plugin(&self, name: &str) -> Result<(), StorageError> {
        self.inner.delete_plugin(name).await
    }

    // === Triggers (pass-through) ===

    async fn create_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        self.inner.create_trigger(trigger).await
    }

    async fn get_trigger(
        &self,
        slug: &str,
    ) -> Result<Option<orch8_types::trigger::TriggerDef>, StorageError> {
        self.inner.get_trigger(slug).await
    }

    async fn list_triggers(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        self.inner.list_triggers(tenant_id).await
    }

    async fn update_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        self.inner.update_trigger(trigger).await
    }

    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError> {
        self.inner.delete_trigger(slug).await
    }

    // === Credentials (pass-through) ===

    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        self.inner.create_credential(credential).await
    }

    async fn get_credential(
        &self,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError> {
        self.inner.get_credential(id).await
    }

    async fn list_credentials(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        self.inner.list_credentials(tenant_id).await
    }

    async fn update_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        self.inner.update_credential(credential).await
    }

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError> {
        self.inner.delete_credential(id).await
    }

    async fn list_credentials_due_for_refresh(
        &self,
        threshold: std::time::Duration,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        self.inner.list_credentials_due_for_refresh(threshold).await
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError> {
        self.inner.ping().await
    }
}
