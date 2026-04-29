//! Encrypting storage decorator.
//!
//! Wraps any `StorageBackend` and transparently encrypts `context.data` on write
//! and decrypts on read using AES-256-GCM via [`FieldEncryptor`].

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use orch8_types::context::ExecutionContext;
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

    /// Encrypt `context.data` on a `TaskInstance`, returning a `Cow` so callers
    /// can pass the result straight through when encryption is unnecessary.
    ///
    /// If `context.data` already carries the `enc:v1:` prefix, the instance is
    /// returned borrowed (no clone, no re-encryption — re-encrypting would
    /// produce a layered payload that `decrypt_value` cannot unwrap in a
    /// single pass, silently corrupting round-trips).
    fn encrypt_instance<'a>(
        &self,
        instance: &'a TaskInstance,
    ) -> Result<Cow<'a, TaskInstance>, StorageError> {
        if FieldEncryptor::is_encrypted(&instance.context.data) {
            return Ok(Cow::Borrowed(instance));
        }
        let mut inst = instance.clone();
        inst.context.data = self
            .encryptor
            .encrypt_value(&inst.context.data)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        Ok(Cow::Owned(inst))
    }

    /// Encrypt `context.data` on an `ExecutionContext`. Same guard as
    /// `encrypt_instance`: already-encrypted payloads short-circuit with a
    /// borrowed reference.
    fn encrypt_context<'a>(
        &self,
        context: &'a ExecutionContext,
    ) -> Result<Cow<'a, ExecutionContext>, StorageError> {
        if FieldEncryptor::is_encrypted(&context.data) {
            return Ok(Cow::Borrowed(context));
        }
        let mut ctx = context.clone();
        ctx.data = self
            .encryptor
            .encrypt_value(&ctx.data)
            .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        Ok(Cow::Owned(ctx))
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

    /// Encrypt the `value` and `refresh_token` fields of a credential before storage.
    fn encrypt_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<orch8_types::credential::CredentialDef, StorageError> {
        let mut cred = credential.clone();
        let plain_value = serde_json::Value::String(cred.value.expose().to_string());
        let encrypted_value = self
            .encryptor
            .encrypt_value(&plain_value)
            .map_err(|e| StorageError::Query(format!("credential encryption: {e}")))?;
        if let serde_json::Value::String(s) = encrypted_value {
            cred.value = orch8_types::config::SecretString::new(s);
        }
        if let Some(ref rt) = cred.refresh_token {
            let plain_rt = serde_json::Value::String(rt.expose().to_string());
            let encrypted_rt = self
                .encryptor
                .encrypt_value(&plain_rt)
                .map_err(|e| StorageError::Query(format!("credential encryption: {e}")))?;
            if let serde_json::Value::String(s) = encrypted_rt {
                cred.refresh_token = Some(orch8_types::config::SecretString::new(s));
            }
        }
        Ok(cred)
    }

    /// Decrypt the `value` and `refresh_token` fields of a credential after reading.
    fn decrypt_credential(
        &self,
        credential: &mut orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        let val = serde_json::Value::String(credential.value.expose().to_string());
        if orch8_types::encryption::FieldEncryptor::is_encrypted(&val) {
            let decrypted = self
                .encryptor
                .decrypt_value(&val)
                .map_err(|e| StorageError::Query(format!("credential decryption: {e}")))?;
            if let serde_json::Value::String(s) = decrypted {
                credential.value = orch8_types::config::SecretString::new(s);
            }
        }
        if let Some(ref rt) = credential.refresh_token {
            let rt_val = serde_json::Value::String(rt.expose().to_string());
            if orch8_types::encryption::FieldEncryptor::is_encrypted(&rt_val) {
                let decrypted = self
                    .encryptor
                    .decrypt_value(&rt_val)
                    .map_err(|e| StorageError::Query(format!("credential decryption: {e}")))?;
                if let serde_json::Value::String(s) = decrypted {
                    credential.refresh_token = Some(orch8_types::config::SecretString::new(s));
                }
            }
        }
        Ok(())
    }
}

/// Generates the full `#[async_trait] impl StorageBackend for EncryptingStorage`
/// block. Custom methods (with encryption logic) go in the `custom` section;
/// pass-through methods are listed as bare signatures in the `delegate` section
/// and automatically forwarded to `self.inner`.
macro_rules! impl_encrypting_storage {
    (
        custom { $($custom_body:tt)* }
        delegate {
            $(async fn $method:ident(&self $(, $arg:ident : $ty:ty)*) -> $ret:ty;)*
        }
    ) => {
        #[async_trait]
        impl StorageBackend for EncryptingStorage {
            $($custom_body)*

            $(
                async fn $method(&self $(, $arg: $ty)*) -> $ret {
                    self.inner.$method($($arg),*).await
                }
            )*
        }
    };
}

impl_encrypting_storage! {
    custom {
        // ================================================================
        // Methods with encryption/decryption logic
        // ================================================================

        async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
            let encrypted = self.encrypt_instance(instance)?;
            self.inner.create_instance(encrypted.as_ref()).await
        }

        async fn create_instances_batch(
            &self,
            instances: &[TaskInstance],
        ) -> Result<u64, StorageError> {
            if instances
                .iter()
                .all(|i| FieldEncryptor::is_encrypted(&i.context.data))
            {
                return self.inner.create_instances_batch(instances).await;
            }
            let encrypted: Vec<TaskInstance> = instances
                .iter()
                .map(|i| self.encrypt_instance(i).map(Cow::into_owned))
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

        async fn update_instance_context(
            &self,
            id: InstanceId,
            context: &orch8_types::context::ExecutionContext,
        ) -> Result<(), StorageError> {
            let encrypted = self.encrypt_context(context)?;
            self.inner
                .update_instance_context(id, encrypted.as_ref())
                .await
        }

        async fn merge_context_data(
            &self,
            id: InstanceId,
            key: &str,
            value: &serde_json::Value,
        ) -> Result<(), StorageError> {
            // When encryption is enabled, `context.data` is stored as a single
            // encrypted blob — the only safe path is read → decrypt → merge →
            // re-encrypt → write.  The underlying `update_instance_context` is
            // an unconditional UPDATE (not CAS), so contention detection via
            // `updated_at` comparison is not meaningful here — the write always
            // lands.
            let Some(mut instance) = self.inner.get_instance(id).await? else {
                return Ok(());
            };
            self.decrypt_instance(&mut instance)?;

            if !instance.context.data.is_object() {
                instance.context.data = serde_json::Value::Object(serde_json::Map::new());
            }
            if let Some(map) = instance.context.data.as_object_mut() {
                map.insert(key.to_string(), value.clone());
            }

            self.update_instance_context(id, &instance.context).await
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

        async fn list_waiting_with_trees(
            &self,
            filter: &orch8_types::filter::InstanceFilter,
            pagination: &orch8_types::filter::Pagination,
        ) -> Result<Vec<(TaskInstance, Vec<orch8_types::execution::ExecutionNode>)>, StorageError> {
            let mut pairs = self
                .inner
                .list_waiting_with_trees(filter, pagination)
                .await?;
            for (inst, _) in &mut pairs {
                self.decrypt_instance(inst)?;
            }
            Ok(pairs)
        }

        async fn save_output_merge_context_and_transition(
            &self,
            output: &orch8_types::output::BlockOutput,
            instance_id: InstanceId,
            context: &ExecutionContext,
            new_state: orch8_types::instance::InstanceState,
            next_fire_at: Option<DateTime<Utc>>,
        ) -> Result<(), StorageError> {
            let encrypted = self.encrypt_context(context)?;
            self.inner
                .save_output_merge_context_and_transition(
                    output,
                    instance_id,
                    encrypted.as_ref(),
                    new_state,
                    next_fire_at,
                )
                .await
        }

        async fn save_output_complete_node_merge_context_and_transition(
            &self,
            output: &orch8_types::output::BlockOutput,
            node_id: orch8_types::ids::ExecutionNodeId,
            instance_id: InstanceId,
            context: &ExecutionContext,
            new_state: orch8_types::instance::InstanceState,
            next_fire_at: Option<DateTime<Utc>>,
        ) -> Result<(), StorageError> {
            let encrypted = self.encrypt_context(context)?;
            self.inner
                .save_output_complete_node_merge_context_and_transition(
                    output,
                    node_id,
                    instance_id,
                    encrypted.as_ref(),
                    new_state,
                    next_fire_at,
                )
                .await
        }

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

        async fn list_session_instances(
            &self,
            session_id: Uuid,
        ) -> Result<Vec<TaskInstance>, StorageError> {
            let mut instances = self.inner.list_session_instances(session_id).await?;
            self.decrypt_instances(&mut instances)?;
            Ok(instances)
        }

        async fn get_child_instances(
            &self,
            parent_instance_id: InstanceId,
        ) -> Result<Vec<TaskInstance>, StorageError> {
            let mut instances = self.inner.get_child_instances(parent_instance_id).await?;
            self.decrypt_instances(&mut instances)?;
            Ok(instances)
        }

        async fn create_credential(
            &self,
            credential: &orch8_types::credential::CredentialDef,
        ) -> Result<(), StorageError> {
            let encrypted = self.encrypt_credential(credential)?;
            self.inner.create_credential(&encrypted).await
        }

        async fn get_credential(
            &self,
            id: &str,
        ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError> {
            let mut cred = self.inner.get_credential(id).await?;
            if let Some(ref mut c) = cred {
                self.decrypt_credential(c)?;
            }
            Ok(cred)
        }

        async fn list_credentials(
            &self,
            tenant_id: Option<&orch8_types::ids::TenantId>,
        ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
            let mut creds = self.inner.list_credentials(tenant_id).await?;
            for c in &mut creds {
                self.decrypt_credential(c)?;
            }
            Ok(creds)
        }

        async fn update_credential(
            &self,
            credential: &orch8_types::credential::CredentialDef,
        ) -> Result<(), StorageError> {
            let encrypted = self.encrypt_credential(credential)?;
            self.inner.update_credential(&encrypted).await
        }

        async fn list_credentials_due_for_refresh(
            &self,
            threshold: std::time::Duration,
        ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
            let mut creds = self
                .inner
                .list_credentials_due_for_refresh(threshold)
                .await?;
            for c in &mut creds {
                self.decrypt_credential(c)?;
            }
            Ok(creds)
        }

        async fn create_instance_with_dedupe(
            &self,
            scope: &crate::DedupeScope,
            key: &str,
            instance: &TaskInstance,
        ) -> Result<crate::EmitDedupeOutcome, StorageError> {
            let encrypted = self.encrypt_instance(instance)?;
            self.inner
                .create_instance_with_dedupe(scope, key, encrypted.as_ref())
                .await
        }
    }

    delegate {
        // --- Sequences ---
        async fn create_sequence(&self, seq: &orch8_types::sequence::SequenceDefinition) -> Result<(), StorageError>;
        async fn get_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn get_sequence_by_name(&self, tenant_id: &orch8_types::ids::TenantId, namespace: &orch8_types::ids::Namespace, name: &str, version: Option<i32>) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn list_sequence_versions(&self, tenant_id: &orch8_types::ids::TenantId, namespace: &orch8_types::ids::Namespace, name: &str) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn list_sequences(&self, tenant_id: Option<&orch8_types::ids::TenantId>, namespace: Option<&orch8_types::ids::Namespace>, limit: u32, offset: u32) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError>;
        async fn deprecate_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;
        async fn delete_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;

        // --- Instances (pass-through) ---
        async fn update_instance_state(&self, id: InstanceId, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn batch_reschedule_instances(&self, ids: &[InstanceId], fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn conditional_update_instance_state(&self, id: InstanceId, expected_state: orch8_types::instance::InstanceState, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn update_instance_sequence(&self, id: InstanceId, new_sequence_id: orch8_types::ids::SequenceId) -> Result<(), StorageError>;
        async fn count_instances(&self, filter: &orch8_types::filter::InstanceFilter) -> Result<u64, StorageError>;
        async fn bulk_update_state(&self, filter: &orch8_types::filter::InstanceFilter, new_state: orch8_types::instance::InstanceState) -> Result<u64, StorageError>;
        async fn bulk_reschedule(&self, filter: &orch8_types::filter::InstanceFilter, offset_secs: i64) -> Result<u64, StorageError>;

        // --- Execution Tree ---
        async fn create_execution_node(&self, node: &orch8_types::execution::ExecutionNode) -> Result<(), StorageError>;
        async fn create_execution_nodes_batch(&self, nodes: &[orch8_types::execution::ExecutionNode]) -> Result<(), StorageError>;
        async fn get_execution_tree(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError>;
        async fn update_node_state(&self, node_id: orch8_types::ids::ExecutionNodeId, state: orch8_types::execution::NodeState) -> Result<(), StorageError>;
        async fn batch_activate_nodes(&self, node_ids: &[orch8_types::ids::ExecutionNodeId]) -> Result<(), StorageError>;
        async fn update_nodes_state(&self, node_ids: &[orch8_types::ids::ExecutionNodeId], state: orch8_types::execution::NodeState) -> Result<(), StorageError>;
        async fn get_children(&self, parent_id: orch8_types::ids::ExecutionNodeId) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError>;
        async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError>;

        // --- Block Outputs ---
        async fn save_block_output(&self, output: &orch8_types::output::BlockOutput) -> Result<(), StorageError>;
        async fn get_block_output(&self, instance_id: InstanceId, block_id: &orch8_types::ids::BlockId) -> Result<Option<orch8_types::output::BlockOutput>, StorageError>;
        async fn get_block_outputs_batch(&self, keys: &[(InstanceId, orch8_types::ids::BlockId)]) -> Result<std::collections::HashMap<(InstanceId, orch8_types::ids::BlockId), orch8_types::output::BlockOutput>, StorageError>;
        async fn get_all_outputs(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError>;
        async fn get_outputs_after_created_at(&self, instance_id: InstanceId, after: Option<DateTime<Utc>>) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError>;
        async fn get_completed_block_ids(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::ids::BlockId>, StorageError>;
        async fn get_completed_block_ids_batch(&self, instance_ids: &[InstanceId]) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::ids::BlockId>>, StorageError>;
        async fn save_output_and_transition(&self, output: &orch8_types::output::BlockOutput, instance_id: InstanceId, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn save_output_complete_node_and_transition(&self, output: &orch8_types::output::BlockOutput, node_id: orch8_types::ids::ExecutionNodeId, instance_id: InstanceId, new_state: orch8_types::instance::InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn delete_block_outputs(&self, instance_id: InstanceId, block_id: &orch8_types::ids::BlockId) -> Result<u64, StorageError>;
        async fn delete_block_outputs_batch(&self, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId]) -> Result<u64, StorageError>;
        async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn delete_sentinel_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError>;

        // --- Rate Limits ---
        async fn check_rate_limit(&self, tenant_id: &orch8_types::ids::TenantId, resource_key: &orch8_types::ids::ResourceKey, now: DateTime<Utc>) -> Result<orch8_types::rate_limit::RateLimitCheck, StorageError>;
        async fn upsert_rate_limit(&self, limit: &orch8_types::rate_limit::RateLimit) -> Result<(), StorageError>;

        // --- Signals ---
        async fn enqueue_signal(&self, signal: &orch8_types::signal::Signal) -> Result<(), StorageError>;
        async fn enqueue_signal_if_active(&self, signal: &orch8_types::signal::Signal) -> Result<(), StorageError>;
        async fn get_pending_signals(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::signal::Signal>, StorageError>;
        async fn get_pending_signals_batch(&self, instance_ids: &[InstanceId]) -> Result<std::collections::HashMap<InstanceId, Vec<orch8_types::signal::Signal>>, StorageError>;
        async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError>;
        async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError>;
        async fn get_signalled_instance_ids(&self, limit: u32) -> Result<Vec<(InstanceId, orch8_types::instance::InstanceState)>, StorageError>;

        // --- Concurrency ---
        async fn count_running_by_concurrency_key(&self, concurrency_key: &str) -> Result<i64, StorageError>;
        async fn count_running_by_concurrency_keys(&self, concurrency_keys: &[String]) -> Result<std::collections::HashMap<String, i64>, StorageError>;
        async fn concurrency_position(&self, instance_id: InstanceId, concurrency_key: &str) -> Result<i64, StorageError>;

        // --- Recovery ---
        async fn recover_stale_instances(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;

        // --- Cron Schedules ---
        async fn create_cron_schedule(&self, schedule: &orch8_types::cron::CronSchedule) -> Result<(), StorageError>;
        async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<orch8_types::cron::CronSchedule>, StorageError>;
        async fn list_cron_schedules(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError>;
        async fn update_cron_schedule(&self, schedule: &orch8_types::cron::CronSchedule) -> Result<(), StorageError>;
        async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;
        async fn claim_due_cron_schedules(&self, now: DateTime<Utc>) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError>;
        async fn update_cron_fire_times(&self, id: Uuid, last_triggered_at: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError>;

        // --- Worker Tasks ---
        async fn create_worker_task(&self, task: &orch8_types::worker::WorkerTask) -> Result<(), StorageError>;
        async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<orch8_types::worker::WorkerTask>, StorageError>;
        async fn claim_worker_tasks(&self, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError>;
        async fn claim_worker_tasks_for_tenant(&self, handler_name: &str, worker_id: &str, tenant_id: &orch8_types::TenantId, limit: u32) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError>;
        async fn complete_worker_task(&self, task_id: Uuid, worker_id: &str, output: &serde_json::Value) -> Result<bool, StorageError>;
        async fn fail_worker_task(&self, task_id: Uuid, worker_id: &str, message: &str, retryable: bool) -> Result<bool, StorageError>;
        async fn heartbeat_worker_task(&self, task_id: Uuid, worker_id: &str) -> Result<bool, StorageError>;
        async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;
        async fn reap_stale_worker_tasks(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;
        async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError>;
        async fn cancel_worker_tasks_for_blocks(&self, instance_id: Uuid, block_ids: &[String]) -> Result<u64, StorageError>;
        async fn cancel_worker_tasks_for_block(&self, instance_id: Uuid, block_id: &str) -> Result<u64, StorageError>;
        async fn list_worker_tasks(&self, filter: &orch8_types::worker_filter::WorkerTaskFilter, pagination: &orch8_types::filter::Pagination) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError>;
        async fn worker_task_stats(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError>;

        // --- Resource Pools ---
        async fn create_resource_pool(&self, pool: &orch8_types::pool::ResourcePool) -> Result<(), StorageError>;
        async fn get_resource_pool(&self, id: Uuid) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError>;
        async fn list_resource_pools(&self, tenant_id: &orch8_types::ids::TenantId) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError>;
        async fn update_pool_round_robin_index(&self, pool_id: Uuid, index: u32) -> Result<(), StorageError>;
        async fn delete_resource_pool(&self, id: Uuid) -> Result<(), StorageError>;
        async fn add_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
        async fn list_pool_resources(&self, pool_id: Uuid) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError>;
        async fn update_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
        async fn delete_pool_resource(&self, id: Uuid) -> Result<(), StorageError>;
        async fn increment_resource_usage(&self, resource_id: Uuid, today: chrono::NaiveDate) -> Result<(), StorageError>;

        // --- Checkpoints ---
        async fn save_checkpoint(&self, checkpoint: &orch8_types::checkpoint::Checkpoint) -> Result<(), StorageError>;
        async fn get_latest_checkpoint(&self, instance_id: InstanceId) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError>;
        async fn list_checkpoints(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError>;
        async fn prune_checkpoints(&self, instance_id: InstanceId, keep: u32) -> Result<u64, StorageError>;

        // --- Externalized State ---
        async fn save_externalized_state(&self, instance_id: InstanceId, ref_key: &str, payload: &serde_json::Value) -> Result<(), StorageError>;
        async fn get_externalized_state(&self, ref_key: &str) -> Result<Option<serde_json::Value>, StorageError>;
        async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError>;

        // --- Audit Log ---
        async fn append_audit_log(&self, entry: &orch8_types::audit::AuditLogEntry) -> Result<(), StorageError>;
        async fn list_audit_log(&self, instance_id: InstanceId, limit: u32) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError>;
        async fn list_audit_log_by_tenant(&self, tenant_id: &orch8_types::ids::TenantId, limit: u32) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError>;

        // --- Sessions ---
        async fn create_session(&self, session: &orch8_types::session::Session) -> Result<(), StorageError>;
        async fn get_session(&self, id: Uuid) -> Result<Option<orch8_types::session::Session>, StorageError>;
        async fn get_session_by_key(&self, tenant_id: &orch8_types::ids::TenantId, session_key: &str) -> Result<Option<orch8_types::session::Session>, StorageError>;
        async fn update_session_data(&self, id: Uuid, data: &serde_json::Value) -> Result<(), StorageError>;
        async fn update_session_state(&self, id: Uuid, state: orch8_types::session::SessionState) -> Result<(), StorageError>;

        // --- Task Queue Routing ---
        async fn claim_worker_tasks_from_queue(&self, queue_name: &str, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError>;
        async fn claim_worker_tasks_from_queue_for_tenant(&self, queue_name: &str, handler_name: &str, worker_id: &str, tenant_id: &orch8_types::TenantId, limit: u32) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError>;

        // --- Dynamic Step Injection ---
        async fn inject_blocks(&self, instance_id: InstanceId, blocks_json: &serde_json::Value) -> Result<(), StorageError>;
        async fn inject_blocks_at_position(&self, instance_id: InstanceId, new_blocks_json: &serde_json::Value, position: Option<usize>) -> Result<serde_json::Value, StorageError>;
        async fn get_injected_blocks(&self, instance_id: InstanceId) -> Result<Option<serde_json::Value>, StorageError>;

        // --- Cluster ---
        async fn register_node(&self, node: &orch8_types::cluster::ClusterNode) -> Result<(), StorageError>;
        async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError>;
        async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError>;
        async fn reap_stale_nodes(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;

        // --- Plugins ---
        async fn create_plugin(&self, plugin: &orch8_types::plugin::PluginDef) -> Result<(), StorageError>;
        async fn get_plugin(&self, name: &str) -> Result<Option<orch8_types::plugin::PluginDef>, StorageError>;
        async fn list_plugins(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<Vec<orch8_types::plugin::PluginDef>, StorageError>;
        async fn update_plugin(&self, plugin: &orch8_types::plugin::PluginDef) -> Result<(), StorageError>;
        async fn delete_plugin(&self, name: &str) -> Result<(), StorageError>;

        // --- Triggers ---
        async fn create_trigger(&self, trigger: &orch8_types::trigger::TriggerDef) -> Result<(), StorageError>;
        async fn get_trigger(&self, slug: &str) -> Result<Option<orch8_types::trigger::TriggerDef>, StorageError>;
        async fn list_triggers(&self, tenant_id: Option<&orch8_types::ids::TenantId>) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError>;
        async fn update_trigger(&self, trigger: &orch8_types::trigger::TriggerDef) -> Result<(), StorageError>;
        async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError>;

        // --- Credentials (pass-through) ---
        async fn delete_credential(&self, id: &str) -> Result<(), StorageError>;

        // --- Emit Event Dedupe ---
        async fn record_or_get_emit_dedupe(&self, scope: &crate::DedupeScope, key: &str, candidate_child: InstanceId) -> Result<crate::EmitDedupeOutcome, StorageError>;
        async fn delete_expired_emit_event_dedupe(&self, older_than: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;

        // --- Health ---
        async fn ping(&self) -> Result<(), StorageError>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_encryptor() -> FieldEncryptor {
        FieldEncryptor::from_hex_key(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap()
    }

    #[test]
    fn is_encrypted_returns_true_for_encrypted_prefix() {
        let enc = test_encryptor();
        let encrypted = enc.encrypt_value(&json!({"secret": "data"})).unwrap();
        assert!(FieldEncryptor::is_encrypted(&encrypted));
    }

    #[test]
    fn is_encrypted_returns_false_for_plain_text() {
        assert!(!FieldEncryptor::is_encrypted(&json!("hello world")));
    }

    #[test]
    fn is_encrypted_returns_false_for_empty_string() {
        assert!(!FieldEncryptor::is_encrypted(&json!("")));
    }

    #[test]
    fn is_encrypted_returns_false_for_non_string_values() {
        assert!(!FieldEncryptor::is_encrypted(&json!(42)));
        assert!(!FieldEncryptor::is_encrypted(&json!(null)));
        assert!(!FieldEncryptor::is_encrypted(&json!(true)));
        assert!(!FieldEncryptor::is_encrypted(&json!([1, 2, 3])));
        assert!(!FieldEncryptor::is_encrypted(&json!({"key": "value"})));
    }

    #[test]
    fn decrypt_value_leaves_plain_string_unchanged() {
        let enc = test_encryptor();
        let plain = json!("not encrypted");
        let result = enc.decrypt_value(&plain).unwrap();
        assert_eq!(result, plain);
    }

    #[test]
    fn decrypt_value_leaves_empty_string_unchanged() {
        let enc = test_encryptor();
        let empty = json!("");
        let result = enc.decrypt_value(&empty).unwrap();
        assert_eq!(result, empty);
    }

    #[test]
    fn decrypt_value_leaves_non_string_unchanged() {
        let enc = test_encryptor();
        let obj = json!({"a": 1});
        assert_eq!(enc.decrypt_value(&obj).unwrap(), obj);

        let arr = json!([1, 2, 3]);
        assert_eq!(enc.decrypt_value(&arr).unwrap(), arr);

        let num = json!(42);
        assert_eq!(enc.decrypt_value(&num).unwrap(), num);

        let nul = json!(null);
        assert_eq!(enc.decrypt_value(&nul).unwrap(), nul);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_object() {
        let enc = test_encryptor();
        let original = json!({"user": "alice", "count": 7, "nested": {"ok": true}});
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_string() {
        let enc = test_encryptor();
        let original = json!("sensitive payload");
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_number() {
        let enc = test_encryptor();
        let original = json!(-123.456);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_null() {
        let enc = test_encryptor();
        let original = json!(null);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypt_decrypt_roundtrip_array() {
        let enc = test_encryptor();
        let original = json!([1, "two", null, {"three": 3}]);
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(decrypted, original);
    }

    #[test]
    fn encrypted_value_has_prefix() {
        let enc = test_encryptor();
        let encrypted = enc.encrypt_value(&json!("x")).unwrap();
        let s = encrypted.as_str().unwrap();
        assert!(s.starts_with("enc:v1:"));
    }

    #[test]
    fn encrypt_produces_different_ciphertext_each_time() {
        let enc = test_encryptor();
        let v = json!({"secret": "data"});
        let a = enc.encrypt_value(&v).unwrap();
        let b = enc.encrypt_value(&v).unwrap();
        assert_ne!(a, b, "nonce randomness should produce distinct ciphertexts");
    }
}
