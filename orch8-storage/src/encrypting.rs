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

/// Magic header prepended to encrypted artifact blobs (`"O8ENC" || 0x01`).
/// Lets reads distinguish an encrypted blob from one written before encryption
/// was enabled, so toggling the key on doesn't render pre-existing plaintext
/// blobs unreadable. Distinctive enough that a plaintext blob is extremely
/// unlikely to start with it.
const ARTIFACT_ENC_MAGIC: &[u8] = b"O8ENC\x01";

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
    /// returned borrowed (no clone, no re-encryption -- re-encrypting would
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

    /// Encrypt `context.data` on an `ExecutionContext` in-place, avoiding clones.
    fn encrypt_context_mut(&self, context: &mut ExecutionContext) -> Result<(), StorageError> {
        if !FieldEncryptor::is_encrypted(&context.data) {
            context.data = self
                .encryptor
                .encrypt_value(&context.data)
                .map_err(|e| StorageError::Query(format!("encryption: {e}")))?;
        }
        Ok(())
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

// ============================================================================
// Sub-trait 1: SequenceStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::SequenceStore for EncryptingStorage {
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
    async fn list_sequences(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        namespace: Option<&orch8_types::ids::Namespace>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
        self.inner
            .list_sequences(tenant_id, namespace, limit, offset)
            .await
    }
    async fn deprecate_sequence(
        &self,
        id: orch8_types::ids::SequenceId,
    ) -> Result<(), StorageError> {
        self.inner.deprecate_sequence(id).await
    }
    async fn delete_sequence(&self, id: orch8_types::ids::SequenceId) -> Result<(), StorageError> {
        self.inner.delete_sequence(id).await
    }
}

// ============================================================================
// Sub-trait 2: InstanceStore -- encryption on create/get/update context paths
// ============================================================================

#[async_trait]
impl crate::InstanceStore for EncryptingStorage {
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
        // Pre-allocate the key string outside the CAS loop to avoid
        // redundant heap allocations on each retry.
        let key_owned = key.to_string();

        // Encrypted context is a single blob -- read -> decrypt -> merge ->
        // encrypt -> CAS write. Retry on contention (another writer
        // updated `updated_at` between our read and write).
        for _ in 0..5 {
            let Some(mut instance) = self.inner.get_instance(id).await? else {
                return Ok(());
            };
            let snapshot_ts = instance.updated_at;
            self.decrypt_instance(&mut instance)?;

            if !instance.context.data.is_object() {
                instance.context.data = serde_json::Value::Object(serde_json::Map::new());
            }
            if let Some(map) = instance.context.data.as_object_mut() {
                map.insert(key_owned.clone(), value.clone());
            }

            self.encrypt_context_mut(&mut instance.context)?;
            if self
                .inner
                .update_instance_context_cas(id, &instance.context, snapshot_ts)
                .await?
            {
                return Ok(());
            }
        }
        Err(StorageError::Query(format!(
            "merge_context_data: CAS contention exceeded retries for instance {id}"
        )))
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

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        let mut instances = self.inner.get_child_instances(parent_instance_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    async fn create_instance_externalized(
        &self,
        instance: &TaskInstance,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_instance(instance)?;
        self.inner
            .create_instance_externalized(encrypted.as_ref(), threshold_bytes)
            .await
    }

    async fn create_instances_batch_externalized(
        &self,
        instances: &[TaskInstance],
        threshold_bytes: u32,
    ) -> Result<u64, StorageError> {
        if instances
            .iter()
            .all(|i| FieldEncryptor::is_encrypted(&i.context.data))
        {
            return self
                .inner
                .create_instances_batch_externalized(instances, threshold_bytes)
                .await;
        }
        let encrypted: Vec<TaskInstance> = instances
            .iter()
            .map(|i| self.encrypt_instance(i).map(Cow::into_owned))
            .collect::<Result<_, _>>()?;
        self.inner
            .create_instances_batch_externalized(&encrypted, threshold_bytes)
            .await
    }

    async fn update_instance_context_externalized(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        let encrypted = self.encrypt_context(context)?;
        self.inner
            .update_instance_context_externalized(id, encrypted.as_ref(), threshold_bytes)
            .await
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

    // --- Pass-through InstanceStore methods ---

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
    async fn batch_reschedule_instances(
        &self,
        ids: &[InstanceId],
        fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.inner.batch_reschedule_instances(ids, fire_at).await
    }
    async fn conditional_update_instance_state(
        &self,
        id: InstanceId,
        expected_state: orch8_types::instance::InstanceState,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        self.inner
            .conditional_update_instance_state(id, expected_state, new_state, next_fire_at)
            .await
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
    async fn update_instance_started_at(
        &self,
        id: InstanceId,
        started_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.inner.update_instance_started_at(id, started_at).await
    }
    async fn update_instance_current_step_started_at(
        &self,
        id: InstanceId,
        ts: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.inner
            .update_instance_current_step_started_at(id, ts)
            .await
    }
    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        self.inner
            .count_running_by_concurrency_key(concurrency_key)
            .await
    }
    async fn count_running_by_concurrency_keys(
        &self,
        concurrency_keys: &[&str],
    ) -> Result<std::collections::HashMap<String, i64>, StorageError> {
        self.inner
            .count_running_by_concurrency_keys(concurrency_keys)
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
    async fn recover_stale_instances(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        self.inner.recover_stale_instances(stale_threshold).await
    }
    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.inner.inject_blocks(instance_id, blocks_json).await
    }
    async fn inject_blocks_at_position(
        &self,
        instance_id: InstanceId,
        new_blocks_json: &serde_json::Value,
        position: Option<usize>,
    ) -> Result<serde_json::Value, StorageError> {
        self.inner
            .inject_blocks_at_position(instance_id, new_blocks_json, position)
            .await
    }
    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.inner.get_injected_blocks(instance_id).await
    }
    async fn record_or_get_emit_dedupe(
        &self,
        scope: &crate::DedupeScope,
        key: &str,
        candidate_child: InstanceId,
    ) -> Result<crate::EmitDedupeOutcome, StorageError> {
        self.inner
            .record_or_get_emit_dedupe(scope, key, candidate_child)
            .await
    }
    async fn delete_expired_emit_event_dedupe(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        self.inner
            .delete_expired_emit_event_dedupe(older_than, limit)
            .await
    }
    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        crate::InstanceStore::batch_save_externalized_state(&*self.inner, instance_id, entries)
            .await
    }
}

// ============================================================================
// Sub-trait 3: ExecutionTreeStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::ExecutionTreeStore for EncryptingStorage {
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
    async fn batch_activate_nodes(
        &self,
        node_ids: &[orch8_types::ids::ExecutionNodeId],
    ) -> Result<(), StorageError> {
        self.inner.batch_activate_nodes(node_ids).await
    }
    async fn update_nodes_state(
        &self,
        node_ids: &[orch8_types::ids::ExecutionNodeId],
        state: orch8_types::execution::NodeState,
    ) -> Result<(), StorageError> {
        self.inner.update_nodes_state(node_ids, state).await
    }
    async fn get_children(
        &self,
        parent_id: orch8_types::ids::ExecutionNodeId,
    ) -> Result<Vec<orch8_types::execution::ExecutionNode>, StorageError> {
        self.inner.get_children(parent_id).await
    }
    async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError> {
        self.inner.delete_execution_tree(instance_id).await
    }
}

// ============================================================================
// Sub-trait 4: OutputStore -- pass-through (context encryption handled via
// InstanceStore methods that OutputStore calls reference)
// ============================================================================

#[async_trait]
impl crate::OutputStore for EncryptingStorage {
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
    async fn get_block_outputs_batch(
        &self,
        keys: &[(InstanceId, orch8_types::ids::BlockId)],
    ) -> Result<
        std::collections::HashMap<
            (InstanceId, orch8_types::ids::BlockId),
            orch8_types::output::BlockOutput,
        >,
        StorageError,
    > {
        self.inner.get_block_outputs_batch(keys).await
    }
    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        self.inner.get_all_outputs(instance_id).await
    }
    async fn get_outputs_after_created_at(
        &self,
        instance_id: InstanceId,
        after: Option<DateTime<Utc>>,
    ) -> Result<Vec<orch8_types::output::BlockOutput>, StorageError> {
        self.inner
            .get_outputs_after_created_at(instance_id, after)
            .await
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
    async fn save_output_complete_node_and_transition(
        &self,
        output: &orch8_types::output::BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        new_state: orch8_types::instance::InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        self.inner
            .save_output_complete_node_and_transition(
                output,
                node_id,
                instance_id,
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
    async fn delete_block_outputs(
        &self,
        instance_id: InstanceId,
        block_id: &orch8_types::ids::BlockId,
    ) -> Result<u64, StorageError> {
        self.inner.delete_block_outputs(instance_id, block_id).await
    }
    async fn delete_block_outputs_batch(
        &self,
        instance_id: InstanceId,
        block_ids: &[orch8_types::ids::BlockId],
    ) -> Result<u64, StorageError> {
        self.inner
            .delete_block_outputs_batch(instance_id, block_ids)
            .await
    }
    async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError> {
        self.inner.delete_all_block_outputs(instance_id).await
    }
    async fn delete_sentinel_block_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<u64, StorageError> {
        self.inner.delete_sentinel_block_outputs(instance_id).await
    }
    async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError> {
        self.inner.delete_block_output_by_id(id).await
    }
}

// ============================================================================
// Sub-trait 5: SignalStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::SignalStore for EncryptingStorage {
    async fn enqueue_signal(
        &self,
        signal: &orch8_types::signal::Signal,
    ) -> Result<(), StorageError> {
        self.inner.enqueue_signal(signal).await
    }
    async fn enqueue_signal_if_active(
        &self,
        signal: &orch8_types::signal::Signal,
    ) -> Result<(), StorageError> {
        self.inner.enqueue_signal_if_active(signal).await
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
    async fn get_signalled_instance_ids(
        &self,
        limit: u32,
    ) -> Result<Vec<(InstanceId, orch8_types::instance::InstanceState)>, StorageError> {
        self.inner.get_signalled_instance_ids(limit).await
    }
}

// ============================================================================
// Sub-trait 6: WorkerStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::WorkerStore for EncryptingStorage {
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
    async fn claim_worker_tasks_for_tenant(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner
            .claim_worker_tasks_for_tenant(handler_name, worker_id, tenant_id, limit)
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
    async fn retry_worker_task(
        &self,
        old_task_id: Uuid,
        new_task: &orch8_types::worker::WorkerTask,
        node_id: Option<orch8_types::ids::ExecutionNodeId>,
        instance_id: orch8_types::ids::InstanceId,
        fire_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        self.inner
            .retry_worker_task(old_task_id, new_task, node_id, instance_id, fire_at)
            .await
    }
    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        self.inner.reap_stale_worker_tasks(stale_threshold).await
    }
    async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError> {
        self.inner.expire_timed_out_worker_tasks().await
    }
    async fn cancel_worker_tasks_for_blocks(
        &self,
        instance_id: Uuid,
        block_ids: &[String],
    ) -> Result<u64, StorageError> {
        self.inner
            .cancel_worker_tasks_for_blocks(instance_id, block_ids)
            .await
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
    async fn claim_worker_tasks_from_queue_for_tenant(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::worker::WorkerTask>, StorageError> {
        self.inner
            .claim_worker_tasks_from_queue_for_tenant(
                queue_name,
                handler_name,
                worker_id,
                tenant_id,
                limit,
            )
            .await
    }
}

// ============================================================================
// Sub-trait 7: SchedulingStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::SchedulingStore for EncryptingStorage {
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
        limit: u32,
    ) -> Result<Vec<orch8_types::cron::CronSchedule>, StorageError> {
        self.inner.list_cron_schedules(tenant_id, limit).await
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
}

// ============================================================================
// Sub-trait 8: AdminStore -- encryption on credential CRUD
// ============================================================================

#[async_trait]
impl crate::AdminStore for EncryptingStorage {
    // --- Sessions (pass-through) ---
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
        // Note: InstanceStore impl already decrypts; but AdminStore also
        // declares this method. We delegate to inner and decrypt.
        let mut instances = self.inner.list_session_instances(session_id).await?;
        self.decrypt_instances(&mut instances)?;
        Ok(instances)
    }

    // --- Plugins (pass-through) ---
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

    // --- Triggers (pass-through) ---
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
        limit: u32,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        self.inner.list_triggers(tenant_id, limit).await
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

    // --- Credentials (with encryption) ---
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
        limit: u32,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        let mut creds = self.inner.list_credentials(tenant_id, limit).await?;
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

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError> {
        self.inner.delete_credential(id).await
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

    // --- Cluster (pass-through) ---
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

    // --- Circuit Breakers (pass-through) ---
    async fn upsert_circuit_breaker(
        &self,
        state: &orch8_types::circuit_breaker::CircuitBreakerState,
    ) -> Result<(), StorageError> {
        self.inner.upsert_circuit_breaker(state).await
    }
    async fn list_open_circuit_breakers(
        &self,
    ) -> Result<Vec<orch8_types::circuit_breaker::CircuitBreakerState>, StorageError> {
        self.inner.list_open_circuit_breakers().await
    }
    async fn delete_circuit_breaker(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        handler: &str,
    ) -> Result<(), StorageError> {
        self.inner.delete_circuit_breaker(tenant_id, handler).await
    }

    // --- Audit Log (pass-through) ---
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

    // --- Health (pass-through) ---
    async fn ping(&self) -> Result<(), StorageError> {
        self.inner.ping().await
    }
}

// ============================================================================
// Sub-trait 9: TelemetryStore -- relies on default no-op impls from the trait
// ============================================================================
// The trait provides default no-op implementations for all methods, so we
// don't need to implement anything here. The EncryptingStorage decorator
// does not persist telemetry itself -- the inner backend handles it.
// However, to avoid hiding the inner backend's real implementations behind
// the defaults, we delegate to the inner backend.

#[async_trait]
impl crate::TelemetryStore for EncryptingStorage {
    async fn ingest_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.inner
            .ingest_telemetry_event(
                event_type,
                payload,
                device_id,
                os_name,
                os_version,
                app_version,
                sdk_version,
                tenant_id,
                created_at,
            )
            .await
    }

    async fn ingest_telemetry_events_batch(
        &self,
        events: &[crate::TelemetryEvent],
    ) -> Result<u64, StorageError> {
        self.inner.ingest_telemetry_events_batch(events).await
    }

    async fn ingest_telemetry_error(
        &self,
        error_type: &str,
        message: &str,
        stack_trace: Option<&str>,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        instance_id: Option<&str>,
        sequence_name: Option<&str>,
    ) -> Result<(), StorageError> {
        self.inner
            .ingest_telemetry_error(
                error_type,
                message,
                stack_trace,
                device_id,
                os_name,
                os_version,
                app_version,
                sdk_version,
                tenant_id,
                instance_id,
                sequence_name,
            )
            .await
    }

    async fn query_telemetry_dashboard(
        &self,
        query_type: &str,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        self.inner
            .query_telemetry_dashboard(query_type, tenant_id, start, end)
            .await
    }

    async fn delete_old_telemetry_events(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        self.inner
            .delete_old_telemetry_events(older_than, limit)
            .await
    }
    async fn record_usage_event(&self, event: &crate::UsageEvent) -> Result<(), StorageError> {
        self.inner.record_usage_event(event).await
    }
    async fn query_usage(
        &self,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<crate::UsageAggregate>, StorageError> {
        self.inner.query_usage(tenant_id, start, end).await
    }
}

// ============================================================================
// Sub-trait 10: ResourceStore -- pure pass-through
// ============================================================================

#[async_trait]
impl crate::ResourceStore for EncryptingStorage {
    // --- Artifacts ---
    // Artifact bytes are encrypted at rest (AES-256-GCM) in this wrapper before
    // they reach the object store, so blobs get the same protection as context
    // and credentials — independent of bucket-level SSE (which may be off, or
    // absent entirely on the local-filesystem backend).
    //
    // Encrypted blobs are framed with [`ARTIFACT_ENC_MAGIC`] so reads are
    // self-describing: a blob written *before* encryption was enabled (no magic)
    // is returned as-is rather than failing to decrypt. This makes turning the
    // encryption key on non-destructive for any pre-existing plaintext blobs.
    fn artifacts_enabled(&self) -> bool {
        self.inner.artifacts_enabled()
    }

    async fn put_artifact(
        &self,
        instance_id: InstanceId,
        content_type: &str,
        bytes: bytes::Bytes,
    ) -> Result<orch8_types::artifact::ArtifactRef, StorageError> {
        let plaintext_len = bytes.len() as u64;
        let ciphertext = self
            .encryptor
            .encrypt_bytes(&bytes)
            .map_err(|e| StorageError::Backend(format!("artifact encrypt: {e}")))?;
        let mut framed = Vec::with_capacity(ARTIFACT_ENC_MAGIC.len() + ciphertext.len());
        framed.extend_from_slice(ARTIFACT_ENC_MAGIC);
        framed.extend_from_slice(&ciphertext);
        let mut aref = self
            .inner
            .put_artifact(instance_id, content_type, bytes::Bytes::from(framed))
            .await?;
        // Report the *plaintext* size, not the on-disk ciphertext size, so the
        // ref the caller sees matches the bytes they put in.
        aref.size = plaintext_len;
        Ok(aref)
    }
    async fn get_artifact(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.inner.get_artifact(key).await? {
            Some(blob) => match blob.strip_prefix(ARTIFACT_ENC_MAGIC) {
                // Encrypted by this wrapper → decrypt.
                Some(ciphertext) => {
                    let plain = self
                        .encryptor
                        .decrypt_bytes(ciphertext)
                        .map_err(|e| StorageError::Backend(format!("artifact decrypt: {e}")))?;
                    Ok(Some(plain))
                }
                // No magic → stored before encryption was enabled; return as-is.
                None => Ok(Some(blob)),
            },
            None => Ok(None),
        }
    }
    async fn delete_artifact(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete_artifact(key).await
    }
    async fn list_artifacts(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::artifact::ArtifactMeta>, StorageError> {
        self.inner.list_artifacts(instance_id).await
    }
    async fn list_artifact_gc_candidates(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError> {
        self.inner.list_artifact_gc_candidates(cutoff, limit).await
    }
    async fn mark_artifacts_gced(&self, instance_id: InstanceId) -> Result<(), StorageError> {
        self.inner.mark_artifacts_gced(instance_id).await
    }

    // --- Instance KV State ---
    async fn set_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.inner.set_instance_kv(instance_id, key, value).await
    }
    async fn get_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.inner.get_instance_kv(instance_id, key).await
    }
    async fn get_all_instance_kv(
        &self,
        instance_id: InstanceId,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        self.inner.get_all_instance_kv(instance_id).await
    }
    async fn delete_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError> {
        self.inner.delete_instance_kv(instance_id, key).await
    }

    // --- Externalized State ---
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
    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        crate::ResourceStore::batch_save_externalized_state(&*self.inner, instance_id, entries)
            .await
    }
    async fn batch_get_externalized_state(
        &self,
        ref_keys: &[String],
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        self.inner.batch_get_externalized_state(ref_keys).await
    }
    async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError> {
        self.inner.delete_expired_externalized_state(limit).await
    }

    // --- Resource Pools ---
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

    // --- Checkpoints ---
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
        limit: u32,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        self.inner.list_checkpoints(instance_id, limit).await
    }
    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError> {
        self.inner.prune_checkpoints(instance_id, keep).await
    }
}

#[async_trait]
impl crate::MobileSyncStore for EncryptingStorage {}

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

    #[tokio::test]
    async fn artifact_bytes_are_encrypted_at_rest() {
        use crate::artifacts::ObjectArtifactStore;
        use crate::ResourceStore;
        use std::sync::Arc;

        let plain = b"\x89PNG sensitive user document".to_vec();
        let inner: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        let enc = EncryptingStorage::new(Arc::clone(&inner), test_encryptor());

        let aref = enc
            .put_artifact(
                InstanceId::new(),
                "image/png",
                bytes::Bytes::from(plain.clone()),
            )
            .await
            .unwrap();

        // The wrapper decrypts on read → caller sees plaintext.
        assert_eq!(enc.get_artifact(&aref.key).await.unwrap().unwrap(), plain);
        // The ref reports the PLAINTEXT size, not the ciphertext size (B2).
        assert_eq!(aref.size, plain.len() as u64);
        // The underlying store holds magic-framed ciphertext, NOT plaintext.
        let at_rest = inner.get_artifact(&aref.key).await.unwrap().unwrap();
        assert_ne!(at_rest, plain, "artifact must not be stored in plaintext");
        assert!(
            at_rest.starts_with(ARTIFACT_ENC_MAGIC),
            "encrypted blobs are framed with the magic header"
        );
    }

    #[tokio::test]
    async fn pre_encryption_plaintext_artifact_reads_back_unchanged() {
        // B1: a blob written before encryption was enabled (no magic header)
        // must read back as-is through the encrypting wrapper, not error.
        use crate::artifacts::ObjectArtifactStore;
        use crate::ResourceStore;
        use std::sync::Arc;

        let plain = b"legacy plaintext blob".to_vec();
        let inner: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        // Write WITHOUT the encrypting wrapper (simulates a pre-encryption blob).
        let aref = inner
            .put_artifact(
                InstanceId::new(),
                "text/plain",
                bytes::Bytes::from(plain.clone()),
            )
            .await
            .unwrap();

        let enc = EncryptingStorage::new(Arc::clone(&inner), test_encryptor());
        // Reading through the wrapper returns the plaintext as-is (no decrypt error).
        assert_eq!(enc.get_artifact(&aref.key).await.unwrap().unwrap(), plain);
    }

    #[tokio::test]
    async fn artifacts_enabled_reflects_backend_and_delegates() {
        // B3: artifacts_enabled() must be false without a backend, true with one,
        // and the encrypting wrapper delegates to inner.
        use crate::artifacts::ObjectArtifactStore;
        use crate::ResourceStore;
        use std::sync::Arc;

        let no_backend = crate::sqlite::SqliteStorage::in_memory().await.unwrap();
        assert!(!no_backend.artifacts_enabled());

        let with_backend: Arc<dyn crate::StorageBackend> = Arc::new(
            crate::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(ObjectArtifactStore::memory())),
        );
        assert!(with_backend.artifacts_enabled());
        let enc = EncryptingStorage::new(with_backend, test_encryptor());
        assert!(enc.artifacts_enabled(), "wrapper must delegate to inner");
    }
}
