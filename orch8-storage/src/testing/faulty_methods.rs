// One `fault_impl!` block per `StorageBackend` sub-trait, signatures copied
// from the trait definitions in `lib.rs` (default methods included).
// `faulty_tests.rs` fails when a trait method is missing here.

fault_impl! {
    [SEQUENCE_STORE_METHODS] impl SequenceStore {
        async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError>;
        async fn get_sequence(&self, id: SequenceId) -> Result<Option<SequenceDefinition>, StorageError>;
        async fn get_sequences(&self, ids: &[SequenceId]) -> Result<Vec<SequenceDefinition>, StorageError>;
        async fn get_sequence_by_name(&self, tenant_id: &TenantId, namespace: &Namespace, name: &str, version: Option<i32>) -> Result<Option<SequenceDefinition>, StorageError>;
        async fn list_sequence_versions(&self, tenant_id: &TenantId, namespace: &Namespace, name: &str) -> Result<Vec<SequenceDefinition>, StorageError>;
        async fn list_sequences(&self, tenant_id: Option<&TenantId>, namespace: Option<&Namespace>, limit: u32, offset: u32) -> Result<Vec<SequenceDefinition>, StorageError>;
        async fn list_embed_visible_sequences(&self, tenant_id: &TenantId, sub_tenant: &str, tenant_level: &crate::EmbedTenantLevel, limit: u32) -> Result<Vec<SequenceDefinition>, StorageError>;
        async fn count_sub_tenant_sequence_names(&self, tenant_id: &TenantId, sub_tenant: &str) -> Result<u64, StorageError>;
        async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError>;
        async fn update_sequence_status(&self, id: SequenceId, status: &str) -> Result<(), StorageError>;
        async fn delete_sequence(&self, id: SequenceId) -> Result<(), StorageError>;
        async fn replace_sequence(&self, old_id: SequenceId, new: &SequenceDefinition) -> Result<(), StorageError>;
        async fn create_release(&self, release: &orch8_types::release::WorkflowRelease) -> Result<(), StorageError>;
        async fn get_release(&self, id: Uuid) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;
        async fn list_releases(&self, tenant_id: Option<&TenantId>, limit: u32) -> Result<Vec<orch8_types::release::WorkflowRelease>, StorageError>;
        async fn cas_release_state(&self, id: Uuid, expected: orch8_types::release::ReleaseState, next: orch8_types::release::ReleaseState, canary_percent: Option<u8>, canary_started_at: Option<chrono::DateTime<chrono::Utc>>) -> Result<bool, StorageError>;
        async fn set_release_validation_summary(&self, id: Uuid, summary: &serde_json::Value) -> Result<(), StorageError>;
        async fn record_release_decision(&self, decision: &orch8_types::release::ReleaseDecision) -> Result<(), StorageError>;
        async fn list_release_decisions(&self, release_id: Uuid) -> Result<Vec<orch8_types::release::ReleaseDecision>, StorageError>;
        async fn find_routing_release_for_sequence(&self, baseline_sequence_id: SequenceId) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;
        async fn acquire_manifest_lock(&self, _tenant_id: &str) -> Result<ManifestLockGuard, StorageError>;
    }
}

fault_impl! {
    [INSTANCE_STORE_METHODS] impl InstanceStore {
        async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError>;
        async fn adopt_child_instance_claimed(&self, parent_id: InstanceId, child_id: InstanceId, expected_activation: &str, activation: &str, claim_token: DateTime<Utc>) -> Result<Option<crate::ChildAdoption>, StorageError>;
        async fn create_child_instance_claimed(&self, parent_id: InstanceId, child: &TaskInstance, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn restore_instance_snapshot(&self, snapshot: &crate::restore::InstanceSnapshot) -> Result<bool, StorageError>;
        async fn create_instance_admitted(&self, instance: &TaskInstance, max_active_instances: u64) -> Result<(), StorageError>;
        async fn create_instances_batch(&self, instances: &[TaskInstance]) -> Result<u64, StorageError>;
        async fn create_instances_batch_admitted(&self, instances: &[TaskInstance], limits: &std::collections::HashMap<TenantId, u64>) -> Result<u64, StorageError>;
        async fn create_instance_externalized(&self, instance: &TaskInstance, threshold_bytes: u32) -> Result<(), StorageError>;
        async fn create_instances_batch_externalized(&self, instances: &[TaskInstance], threshold_bytes: u32) -> Result<u64, StorageError>;
        async fn get_instance_for_caller(&self, caller: InstanceId, tenant: &TenantId, target: InstanceId) -> Result<Option<TaskInstance>, StorageError>;
        async fn get_mobile_instance(&self, access: &MobileInstanceAccess, id: InstanceId) -> Result<Option<TaskInstance>, StorageError>;
        async fn cancel_mobile_instance(&self, access: &MobileInstanceAccess, id: InstanceId) -> Result<Option<InstanceState>, StorageError>;
        async fn complete_mobile_step(&self, access: &MobileInstanceAccess, expected_updated_at: DateTime<Utc>, context: &orch8_types::context::ExecutionContext, signal: &orch8_types::signal::Signal) -> Result<bool, StorageError>;
        async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError>;
        async fn claim_due_instances(&self, now: DateTime<Utc>, limit: u32, max_per_tenant: u32) -> Result<Vec<TaskInstance>, StorageError>;
        async fn has_due_higher_priority(&self, now: DateTime<Utc>, priority: orch8_types::instance::Priority) -> Result<bool, StorageError>;
        async fn update_instance_state(&self, id: InstanceId, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn batch_reschedule_instances(&self, ids: &[InstanceId], fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn conditional_update_instance_state(&self, id: InstanceId, _expected_state: InstanceState, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn conditional_update_claimed_instance_state(&self, id: InstanceId, claimed_at: DateTime<Utc>, expected_state: InstanceState, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn conditional_update_instance_state_with_outbox(&self, id: InstanceId, expected_state: InstanceState, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>, entries: &[orch8_types::webhook_outbox::WebhookOutboxEntry]) -> Result<bool, StorageError>;
        async fn conditional_update_claimed_instance_state_with_outbox(&self, id: InstanceId, claimed_at: DateTime<Utc>, expected_state: InstanceState, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>, entries: &[orch8_types::webhook_outbox::WebhookOutboxEntry]) -> Result<bool, StorageError>;
        async fn reset_instance_run(&self, id: InstanceId, run_id: &str) -> Result<(), StorageError>;
        async fn update_instance_context(&self, id: InstanceId, context: &orch8_types::context::ExecutionContext) -> Result<(), StorageError>;
    async fn update_instance_context_cas_claimed(&self, id: InstanceId, context: &orch8_types::context::ExecutionContext, expected_updated_at: DateTime<Utc>, claim_token: DateTime<Utc>) -> Result<Option<bool>, StorageError>;

    async fn merge_context_data_claimed(&self, id: InstanceId, key: &str, value: &serde_json::Value, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

        async fn update_instance_context_cas(&self, id: InstanceId, context: &orch8_types::context::ExecutionContext, _expected_updated_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn ensure_instance_run_started(&self, id: InstanceId, run_id: &str, started_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn ensure_instance_run_started_claimed(&self, id: InstanceId, run_id: &str, started_at: DateTime<Utc>, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn update_instance_current_step_started_at(&self, id: InstanceId, ts: DateTime<Utc>) -> Result<(), StorageError>;
        async fn increment_total_steps(&self, id: InstanceId) -> Result<u32, StorageError>;
        async fn update_instance_context_externalized(&self, id: InstanceId, context: &orch8_types::context::ExecutionContext, threshold_bytes: u32) -> Result<(), StorageError>;
        async fn update_instance_sequence(&self, id: InstanceId, new_sequence_id: SequenceId) -> Result<(), StorageError>;
        async fn merge_context_data(&self, id: InstanceId, key: &str, value: &serde_json::Value) -> Result<(), StorageError>;
    async fn merge_instance_metadata_claimed(&self, id: InstanceId, patch: &serde_json::Value, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

        async fn merge_instance_metadata(&self, id: InstanceId, patch: &serde_json::Value) -> Result<(), StorageError>;
        async fn list_instances(&self, filter: &InstanceFilter, pagination: &Pagination) -> Result<Vec<TaskInstance>, StorageError>;
        async fn list_instances_keyset(&self, filter: &InstanceFilter, before: Option<InstanceId>, limit: u32) -> Result<Vec<TaskInstance>, StorageError>;
        async fn list_waiting_with_trees(&self, filter: &InstanceFilter, pagination: &Pagination) -> Result<Vec<(TaskInstance, Vec<ExecutionNode>)>, StorageError>;
        async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError>;
        async fn any_instance_in_state(&self, state: InstanceState) -> Result<bool, StorageError>;
        async fn bulk_update_state(&self, filter: &InstanceFilter, new_state: InstanceState) -> Result<u64, StorageError>;
        async fn bulk_reschedule(&self, filter: &InstanceFilter, offset_secs: i64) -> Result<u64, StorageError>;
        async fn find_by_idempotency_key(&self, tenant_id: &TenantId, idempotency_key: &str) -> Result<Option<TaskInstance>, StorageError>;
        async fn count_running_by_concurrency_key(&self, tenant_id: &str, concurrency_key: &str) -> Result<i64, StorageError>;
        async fn count_running_by_concurrency_keys(&self, keys: &[(&str, &str)]) -> Result<HashMap<(String, String), i64>, StorageError>;
        async fn concurrency_position(&self, instance_id: InstanceId, concurrency_key: &str) -> Result<i64, StorageError>;
        async fn recover_stale_instances(&self, stale_threshold: Duration) -> Result<u64, StorageError>;
        async fn verify_instance_claim(&self, instance_id: InstanceId, tenant_id: &TenantId, claimed_at: DateTime<Utc>) -> Result<bool, StorageError>;
    async fn heartbeat_instance(&self, instance_id: InstanceId, claimed_at: DateTime<Utc>) -> Result<InstanceLease, StorageError>;
        async fn get_child_instances(&self, parent_instance_id: InstanceId) -> Result<Vec<TaskInstance>, StorageError>;
        async fn get_child_for_activation(&self, parent_instance_id: InstanceId, block_id: &str, activation: &str) -> Result<Option<TaskInstance>, StorageError>;
        async fn get_child_for_adoption(&self, parent_instance_id: InstanceId, block_id: &str, activation_suffix: &str) -> Result<Option<TaskInstance>, StorageError>;
        async fn list_orphaned_children(&self, limit: u32) -> Result<Vec<InstanceId>, StorageError>;
        async fn inject_blocks(&self, instance_id: InstanceId, blocks_json: &serde_json::Value) -> Result<(), StorageError>;
        async fn inject_blocks_at_position(&self, instance_id: InstanceId, new_blocks_json: &serde_json::Value, position: Option<usize>) -> Result<serde_json::Value, StorageError>;
        async fn get_injected_blocks(&self, instance_id: InstanceId) -> Result<Option<serde_json::Value>, StorageError>;
        async fn record_or_get_emit_dedupe(&self, scope: &DedupeScope, key: &str, candidate_child: orch8_types::ids::InstanceId) -> Result<EmitDedupeOutcome, StorageError>;
        async fn create_instance_with_dedupe(&self, scope: &DedupeScope, key: &str, instance: &TaskInstance) -> Result<EmitDedupeOutcome, StorageError>;
        async fn delete_expired_emit_event_dedupe(&self, older_than: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn batch_save_externalized_state(&self, instance_id: InstanceId, entries: &[(String, serde_json::Value)]) -> Result<(), StorageError>;
    }
}

fault_impl! {
    [EXECUTION_TREE_STORE_METHODS] impl ExecutionTreeStore {
        async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError>;
        async fn create_execution_nodes_batch(&self, nodes: &[ExecutionNode]) -> Result<(), StorageError>;
    async fn create_execution_nodes_batch_claimed(&self, instance_id: InstanceId, nodes: &[orch8_types::execution::ExecutionNode], claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

    async fn update_nodes_state_claimed(&self, instance_id: InstanceId, node_ids: &[orch8_types::ids::ExecutionNodeId], state: orch8_types::execution::NodeState, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

    async fn batch_activate_nodes_claimed(&self, instance_id: InstanceId, node_ids: &[orch8_types::ids::ExecutionNodeId], claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

        async fn get_execution_tree(&self, instance_id: InstanceId) -> Result<Vec<ExecutionNode>, StorageError>;
        async fn update_node_state(&self, node_id: ExecutionNodeId, state: NodeState) -> Result<(), StorageError>;
    async fn update_node_state_claimed(&self, instance_id: InstanceId, node_id: orch8_types::ids::ExecutionNodeId,
        state: orch8_types::execution::NodeState, claim_token: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError>;

        async fn batch_activate_nodes(&self, node_ids: &[ExecutionNodeId]) -> Result<(), StorageError>;
        async fn update_nodes_state(&self, node_ids: &[ExecutionNodeId], state: NodeState) -> Result<(), StorageError>;
        async fn get_children(&self, parent_id: ExecutionNodeId) -> Result<Vec<ExecutionNode>, StorageError>;
        async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError>;
    }
}

fault_impl! {
    [OUTPUT_STORE_METHODS] impl OutputStore {
        async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError>;
        async fn save_block_output_claimed(&self, output: &BlockOutput, claim_token: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn get_block_output_claimed(&self, instance_id: InstanceId, tenant: &TenantId, block_id: &BlockId, claim_token: DateTime<Utc>) -> Result<crate::ClaimedRead<Option<BlockOutput>>, StorageError>;
        async fn get_block_output(&self, instance_id: InstanceId, block_id: &BlockId) -> Result<Option<BlockOutput>, StorageError>;
        async fn get_block_outputs_batch(&self, keys: &[(InstanceId, &BlockId)]) -> Result<HashMap<(InstanceId, BlockId), BlockOutput>, StorageError>;
        async fn get_latest_completed_outputs(&self, instance_id: InstanceId) -> Result<Vec<BlockOutput>, StorageError>;
        async fn get_latest_diagnostic_outputs(&self, instance_id: InstanceId) -> Result<Vec<BlockOutput>, StorageError>;
        async fn get_all_outputs(&self, instance_id: InstanceId) -> Result<Vec<BlockOutput>, StorageError>;
        async fn get_outputs_after_created_at(&self, instance_id: InstanceId, after: Option<DateTime<Utc>>) -> Result<Vec<BlockOutput>, StorageError>;
        async fn get_completed_block_ids(&self, instance_id: InstanceId) -> Result<Vec<BlockId>, StorageError>;
        async fn get_completed_block_ids_batch(&self, instance_ids: &[InstanceId]) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError>;
        async fn save_output_and_claimed_transition(&self, output: &BlockOutput, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn save_output_and_conditional_transition(&self, output: &BlockOutput, expected_state: InstanceState, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn save_output_and_transition(&self, output: &BlockOutput, instance_id: InstanceId, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn save_output_merge_context_and_transition(&self, output: &BlockOutput, instance_id: InstanceId, context: &orch8_types::context::ExecutionContext, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn save_output_complete_node_and_transition(&self, output: &BlockOutput, node_id: orch8_types::ids::ExecutionNodeId, instance_id: InstanceId, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn save_output_complete_node_merge_context_and_transition(&self, output: &BlockOutput, node_id: orch8_types::ids::ExecutionNodeId, instance_id: InstanceId, context: &orch8_types::context::ExecutionContext, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn delete_block_outputs(&self, instance_id: InstanceId, block_id: &BlockId) -> Result<u64, StorageError>;
    async fn delete_block_outputs_batch_claimed(&self, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId], claim_token: DateTime<Utc>) -> Result<Option<u64>, StorageError>;
    async fn delete_block_output_by_id_claimed(&self, instance_id: InstanceId, id: Uuid, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;

        async fn delete_block_outputs_batch(&self, instance_id: InstanceId, block_ids: &[BlockId]) -> Result<u64, StorageError>;
        async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn delete_sentinel_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError>;
        async fn get_outputs_page(&self, instance_id: InstanceId, limit: u32, offset: u64) -> Result<Vec<BlockOutput>, StorageError>;
        async fn copy_block_outputs(&self, src: InstanceId, dst: InstanceId, block_ids: &[BlockId]) -> Result<u64, StorageError>;
    }
}

fault_impl! {
    [SIGNAL_STORE_METHODS] impl SignalStore {
        async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError>;
        async fn enqueue_signal_for_caller(&self, caller: InstanceId, tenant: &TenantId, signal: &Signal, claim_token: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn enqueue_signal_if_active(&self, signal: &Signal) -> Result<(), StorageError>;
        async fn get_pending_signals(&self, instance_id: InstanceId) -> Result<Vec<Signal>, StorageError>;
        async fn ingest_event(&self, envelope: &orch8_types::event_correlation::EventEnvelope) -> Result<bool, StorageError>;
        async fn get_event(&self, id: Uuid) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError>;
        async fn get_event_by_identity(&self, tenant_id: &str, event_name: &str, producer_event_id: &str) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError>;
        async fn list_events(&self, tenant_id: &str, status: Option<orch8_types::event_correlation::EventStatus>, limit: u32) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;
        async fn list_events_page(&self, tenant_id: &str, status: Option<orch8_types::event_correlation::EventStatus>, limit: u32, offset: u32) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;
        async fn find_pending_events(&self, tenant_id: &str, event_names: &[String], correlation_key: &str) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;
        async fn consume_events(&self, event_ids: &[Uuid], instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn upsert_event_wait(&self, wait: &orch8_types::event_correlation::EventWait) -> Result<(), StorageError>;
        async fn register_event_wait(&self, wait: &orch8_types::event_correlation::EventWait) -> Result<orch8_types::event_correlation::EventWait, StorageError>;
        async fn match_event_wait(&self, wait: &orch8_types::event_correlation::EventWait, event_id: Option<Uuid>, notification: &orch8_types::signal::Signal) -> Result<Option<crate::EventMatch>, StorageError>;
        async fn upsert_event_wait_claimed(&self, wait: &orch8_types::event_correlation::EventWait, claim_token: DateTime<Utc>) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError>;
        async fn delete_event_waits_for_blocks(&self, instance_id: InstanceId, blocks: &[String]) -> Result<u64, StorageError>;
        async fn delete_event_waits_for_blocks_claimed(&self, instance_id: InstanceId, blocks: &[String], claim_token: DateTime<Utc>) -> Result<Option<u64>, StorageError>;
        async fn get_event_wait(&self, instance_id: InstanceId, block_id: &str) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError>;
        async fn find_waiting_event_waits(&self, tenant_id: &str, event_name: &str, correlation_key: &str) -> Result<Vec<orch8_types::event_correlation::EventWait>, StorageError>;
        async fn update_event_wait(&self, wait: &orch8_types::event_correlation::EventWait, expected_status: orch8_types::event_correlation::WaitStatus, expected_matched_event_ids: &[Uuid]) -> Result<bool, StorageError>;
        async fn expire_events_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<u64, StorageError>;
        async fn get_pending_signals_batch(&self, instance_ids: &[InstanceId]) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError>;
        async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError>;
        async fn mark_signal_delivered_claimed(&self, instance_id: InstanceId, signal_id: Uuid, claim_token: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError>;
        async fn get_signalled_instance_ids(&self, limit: u32) -> Result<Vec<(InstanceId, InstanceState)>, StorageError>;
    }
}

fault_impl! {
    [WORKER_STORE_METHODS] impl WorkerStore {
        async fn claim_worker_tasks_with_authority(&self,request:&crate::worker_claim::WorkerClaimRequest)->Result<Vec<WorkerTask>,StorageError>;
        async fn get_worker_issued_claim(&self,tenant:Option<&TenantId>,task_id:Uuid,claim:&WorkerClaim)->Result<Option<crate::worker_claim::WorkerIssuedClaim>,StorageError>;

        async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError>;
        async fn prepare_worker_dispatch(&self, instance: &orch8_types::instance::TaskInstance, block_id: &orch8_types::ids::BlockId, selection: &crate::worker_invocation::WorkerDispatchSelection) -> Result<Option<crate::worker_invocation::WorkerDispatchFence>, StorageError>;
        async fn admit_worker_dispatch(&self,intent:&crate::worker_admission::WorkerDispatchIntent)->Result<crate::worker_admission::WorkerDispatchPublication,StorageError>;
        async fn get_worker_dispatch_admission(&self,tenant_id:&TenantId,task_id:Uuid)->Result<Option<crate::worker_admission::WorkerDispatchAdmission>,StorageError>;
        async fn create_worker_task_bound(&self, task: &WorkerTask, fence: &crate::worker_invocation::WorkerDispatchFence) -> Result<Option<Uuid>, StorageError>;
        async fn get_worker_invocation(&self, tenant_id: &TenantId, task_id: Uuid) -> Result<Option<crate::worker_invocation::WorkerInvocation>, StorageError>;
        async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError>;
        async fn claim_worker_tasks(&self, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn claim_worker_tasks_for_tenant(&self, handler_name: &str, worker_id: &str, tenant_id: &TenantId, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn claim_worker_tasks_matching(&self, handler_name: &str, worker_id: &str, tenant_id: Option<&TenantId>, queue_name: Option<&str>, capabilities: &RuntimeCapabilities, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn get_worker_accepted_outcome(&self, tenant_id: Option<&TenantId>, task_id: Uuid, claim: &WorkerClaim) -> Result<Option<crate::worker_outcome::WorkerAcceptedOutcome>, StorageError>;
        async fn claim_worker_outcome_recovery_page(&self, limit: u32) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError>;
        async fn apply_worker_outcome_integration(&self, integration: &crate::worker_outcome::WorkerOutcomeIntegration) -> Result<crate::worker_outcome::WorkerOutcomeApplication, StorageError>;
        async fn accept_worker_completion(&self, task_id: Uuid, claim: &WorkerClaim, completion: &crate::worker_outcome::WorkerCompletion) -> Result<bool, StorageError>;
        async fn complete_worker_task(&self, task_id: Uuid, claim: &WorkerClaim, output: &serde_json::Value) -> Result<bool, StorageError>;
        async fn fail_worker_task(&self, task_id: Uuid, claim: &WorkerClaim, message: &str, retryable: bool) -> Result<bool, StorageError>;
        async fn heartbeat_worker_task(&self, task_id: Uuid, claim: &WorkerClaim) -> Result<bool, StorageError>;
        async fn checkpoint_worker_task(&self, task_id: Uuid, claim: &WorkerClaim, expected_seq: u64, checkpoint: &serde_json::Value) -> Result<Option<u64>, StorageError>;
        async fn record_worker_task_attempt_event(&self, event: &WorkerTaskAttemptEvent) -> Result<(), StorageError>;
        async fn list_worker_task_attempt_events(&self, task_id: Uuid, limit: u32) -> Result<Vec<WorkerTaskAttemptEvent>, StorageError>;
        async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;
        async fn retry_worker_task(&self, old_task_id: Uuid, new_task: &WorkerTask, node_id: Option<orch8_types::ids::ExecutionNodeId>, instance_id: InstanceId, fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn reap_stale_worker_tasks(&self, stale_threshold: Duration) -> Result<u64, StorageError>;
        async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError>;
        async fn list_expired_worker_leases(&self, default_lease: Duration, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn list_timed_out_worker_tasks(&self, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn list_stranded_worker_dispatches(&self, created_before: chrono::DateTime<chrono::Utc>, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn finalize_stranded_worker_dispatch(&self, task_id: Uuid, attempt: u16, effect: Option<&crate::worker_invocation::WorkerDispatchEffect>, continuity_epoch: Option<u64>) -> Result<bool, StorageError>;
        async fn resolve_worker_task(&self, resolution: &orch8_types::worker::WorkerTaskResolution) -> Result<bool, StorageError>;
        async fn resolve_worker_task_with_evidence(&self, resolution: &orch8_types::worker::WorkerTaskResolution, evidence:&crate::worker_resolution::WorkerResolutionEvidence) -> Result<bool, StorageError>;
        async fn get_worker_resolution_outcome_by_id(&self,tenant:&TenantId,id:Uuid)->Result<Option<crate::worker_resolution::WorkerResolutionOutcome>,StorageError>;
        async fn claim_worker_resolution_recovery_page(&self,limit:u32)->Result<Vec<crate::worker_resolution_application::WorkerResolutionRecovery>,StorageError>;
        async fn apply_worker_resolution_integration(&self,plan:&crate::worker_resolution_application::WorkerResolutionIntegration)->Result<crate::worker_resolution_application::WorkerResolutionApplication,StorageError>;
        async fn get_worker_resolution_outcome(&self,tenant_id:Option<&TenantId>,task_id:Uuid,claim:Option<&WorkerClaim>)->Result<Option<crate::worker_resolution::WorkerResolutionOutcome>,StorageError>;
        async fn cancel_worker_tasks_for_block(&self, instance_id: Uuid, block_id: &str) -> Result<u64, StorageError>;
    async fn cancel_worker_tasks_for_blocks_claimed(&self, instance_id: InstanceId, block_ids: &[String], claim_token: DateTime<Utc>) -> Result<Option<u64>, StorageError>;

        async fn cancel_worker_tasks_for_blocks(&self, instance_id: Uuid, block_ids: &[String]) -> Result<u64, StorageError>;
        async fn held_worker_tasks_for_reporter(
        &self,
        request: &crate::worker_claim::WorkerReconnectRequest,
    ) -> Result<Vec<WorkerTask>, StorageError>;
        async fn list_worker_tasks(&self, filter: &orch8_types::worker_filter::WorkerTaskFilter, pagination: &orch8_types::filter::Pagination) -> Result<Vec<WorkerTask>, StorageError>;
        async fn worker_task_stats(&self, tenant_id: Option<&orch8_types::ids::TenantId>, window_secs: Option<u32>) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError>;
        async fn pending_worker_task_depth(&self, limit: u32) -> Result<Vec<orch8_types::placement::QueueDepthRow>, StorageError>;
        async fn claim_worker_tasks_from_queue(&self, queue_name: &str, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn claim_worker_tasks_from_queue_for_tenant(&self, queue_name: &str, handler_name: &str, worker_id: &str, tenant_id: &TenantId, limit: u32) -> Result<Vec<WorkerTask>, StorageError>;
        async fn upsert_worker_registration(&self, registration: &orch8_types::worker::WorkerRegistration) -> Result<(), StorageError>;
        async fn list_worker_registrations(&self, seen_within_secs: Option<i64>) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError>;
        async fn claimed_task_counts_by_worker(&self, tenant_id: Option<&TenantId>) -> Result<Vec<(String, i64)>, StorageError>;
        async fn park_webhook(&self, entry: &orch8_types::webhook_outbox::WebhookOutboxEntry) -> Result<(), StorageError>;
        async fn list_webhook_outbox(&self, limit: u32) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;
        async fn get_webhook_outbox(&self, id: Uuid) -> Result<Option<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;
        async fn delete_webhook_outbox(&self, id: Uuid) -> Result<(), StorageError>;
        async fn claim_due_webhook_outbox(&self, now: DateTime<Utc>, limit: u32) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;
        async fn claim_webhook_outbox_row(&self, id: Uuid, claimed_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn fail_webhook_outbox_attempt(&self, id: Uuid, last_error: &str, next_attempt_at: Option<DateTime<Utc>>) -> Result<(), StorageError>;
        async fn fail_webhook_outbox_attempt_fenced(&self, id: Uuid, claimed_at: DateTime<Utc>, last_error: &str, next_attempt_at: Option<DateTime<Utc>>) -> Result<bool, StorageError>;
        async fn complete_webhook_outbox_claim(&self, id: Uuid, claimed_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn recover_stale_webhook_claims(&self, stale_before: DateTime<Utc>) -> Result<u64, StorageError>;
        async fn delete_parked_webhook_outbox_before(&self, cutoff: DateTime<Utc>) -> Result<u64, StorageError>;
        async fn record_webhook_attempt(&self, attempt: &orch8_types::webhook_delivery::WebhookDeliveryAttempt) -> Result<(), StorageError>;
        async fn list_webhook_deliveries(&self, filter: &orch8_types::webhook_delivery::DeliveryFilter, limit: u32) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliverySummary>, StorageError>;
        async fn get_webhook_delivery_attempts(&self, delivery_id: Uuid) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliveryAttempt>, StorageError>;
        async fn delete_webhook_attempts_before(&self, cutoff: chrono::DateTime<chrono::Utc>) -> Result<u64, StorageError>;
        async fn create_queue_routing_rule(&self, rule: &orch8_types::queue_routing::QueueRoutingRule) -> Result<(), StorageError>;
        async fn list_queue_routing_rules(&self, tenant_id: Option<&TenantId>, handler_name: Option<&str>) -> Result<Vec<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;
        async fn get_queue_routing_rule(&self, id: Uuid) -> Result<Option<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;
        async fn delete_queue_routing_rule(&self, id: Uuid) -> Result<(), StorageError>;
        async fn enqueue_worker_command(&self, command: &orch8_types::worker::WorkerCommand) -> Result<(), StorageError>;
        async fn list_worker_commands(&self, worker_id: &str) -> Result<Vec<orch8_types::worker::WorkerCommand>, StorageError>;
        async fn delete_worker_command(&self, id: Uuid) -> Result<(), StorageError>;
        async fn upsert_worker_version_pin(&self, pin: &orch8_types::worker::WorkerVersionPin) -> Result<(), StorageError>;
        async fn get_worker_version_pin(&self, tenant_id: &str, handler_name: &str) -> Result<Option<orch8_types::worker::WorkerVersionPin>, StorageError>;
        async fn list_worker_version_pins(&self, tenant_id: Option<&str>) -> Result<Vec<orch8_types::worker::WorkerVersionPin>, StorageError>;
        async fn delete_worker_version_pin(&self, tenant_id: &str, handler_name: &str) -> Result<(), StorageError>;
        async fn upsert_queue_dispatch(&self, config: &orch8_types::queue_dispatch::QueueDispatchConfig) -> Result<(), StorageError>;
        async fn set_queue_dispatch(&self, config: &orch8_types::queue_dispatch::QueueDispatchConfig, preserve_secret: bool) -> Result<orch8_types::queue_dispatch::QueueDispatchConfig, StorageError>;
        async fn get_queue_dispatch(&self, tenant_id: &str, queue_name: &str) -> Result<Option<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;
        async fn list_queue_dispatch(&self, tenant_id: Option<&str>) -> Result<Vec<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;
        async fn delete_queue_dispatch(&self, tenant_id: &str, queue_name: &str) -> Result<(), StorageError>;
        async fn append_step_logs(&self, instance_id: InstanceId, block_id: &BlockId, entries: &[orch8_types::step_log::StepLogEntry]) -> Result<(), StorageError>;
        async fn list_step_logs(&self, instance_id: InstanceId) -> Result<Vec<orch8_types::step_log::StepLog>, StorageError>;
    }
}

fault_impl! {
    [SCHEDULING_STORE_METHODS] impl SchedulingStore {
        async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;
        async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError>;
        async fn list_cron_schedules(&self, tenant_id: Option<&TenantId>, limit: u32) -> Result<Vec<CronSchedule>, StorageError>;
        async fn list_all_cron_schedules(&self, tenant_id: Option<&TenantId>) -> Result<Vec<CronSchedule>, StorageError>;
        async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;
        async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;
        async fn claim_due_cron_schedules(&self, now: DateTime<Utc>) -> Result<Vec<CronSchedule>, StorageError>;
        async fn update_cron_fire_times(&self, id: Uuid, last_triggered_at: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn record_cron_skip(&self, id: Uuid, now: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn active_instance_ids_for_cron(&self, cron_id: Uuid, limit: u32) -> Result<Vec<InstanceId>, StorageError>;
        async fn check_rate_limit(&self, tenant_id: &TenantId, resource_key: &ResourceKey, now: DateTime<Utc>) -> Result<RateLimitCheck, StorageError>;
        async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError>;
        async fn get_placement_policies(&self, tenant_id: &TenantId) -> Result<orch8_types::placement::PlacementPolicies, StorageError>;
        async fn put_placement_policies(&self, tenant_id: &TenantId, policies: &orch8_types::placement::PlacementPolicies) -> Result<(), StorageError>;
        async fn upsert_rate_budget(&self, budget: &orch8_types::placement::RateBudget) -> Result<orch8_types::placement::RateBudget, StorageError>;
        async fn list_rate_budgets(&self, tenant_id: &TenantId) -> Result<Vec<orch8_types::placement::RateBudget>, StorageError>;
        async fn delete_rate_budget(&self, tenant_id: &TenantId, key: &str) -> Result<bool, StorageError>;
        async fn take_rate_budget_token(&self, tenant_id: &TenantId, key: &str, now: DateTime<Utc>) -> Result<orch8_types::placement::RateBudgetCheck, StorageError>;
    }
}

fault_impl! {
    [ADMIN_STORE_METHODS] impl AdminStore {
        async fn create_session(&self, session: &Session) -> Result<(), StorageError>;
        async fn get_session(&self, id: Uuid) -> Result<Option<Session>, StorageError>;
        async fn get_session_by_key(&self, tenant_id: &TenantId, session_key: &str) -> Result<Option<Session>, StorageError>;
        async fn update_session_data(&self, id: Uuid, data: &serde_json::Value) -> Result<(), StorageError>;
        async fn update_session_state(&self, id: Uuid, state: orch8_types::session::SessionState) -> Result<(), StorageError>;
        async fn list_session_instances(&self, session_id: Uuid) -> Result<Vec<TaskInstance>, StorageError>;
        async fn create_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;
        async fn get_plugin(&self, tenant_id: Option<&TenantId>, name: &str) -> Result<Option<PluginDef>, StorageError>;
        async fn list_plugins(&self, tenant_id: Option<&TenantId>) -> Result<Vec<PluginDef>, StorageError>;
        async fn update_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;
        async fn delete_plugin(&self, name: &str) -> Result<(), StorageError>;
        async fn create_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;
        async fn get_trigger(&self, tenant_id: Option<&TenantId>, slug: &str) -> Result<Option<TriggerDef>, StorageError>;
        async fn list_triggers(&self, tenant_id: Option<&TenantId>, limit: u32) -> Result<Vec<TriggerDef>, StorageError>;
        async fn list_all_triggers(&self, tenant_id: Option<&TenantId>) -> Result<Vec<TriggerDef>, StorageError>;
        async fn update_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;
        async fn update_trigger_cas(&self, _trigger: &TriggerDef, _expected_updated_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError>;
        async fn claim_webhook_nonce(&self, slug: &str, nonce: &str, expires_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn create_approval_tokens(&self, tokens: &[orch8_types::approval_link::ApprovalActionToken]) -> Result<(), StorageError>;
        async fn get_approval_token(&self, token_hash: &str) -> Result<Option<orch8_types::approval_link::ApprovalActionToken>, StorageError>;
        async fn consume_approval_token(&self, token_hash: &str, now: DateTime<Utc>) -> Result<Option<orch8_types::approval_link::ApprovalActionToken>, StorageError>;
        async fn has_live_approval_tokens(&self, instance_id: InstanceId, block_id: &BlockId, channel: orch8_types::approval_link::ApprovalChannel, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn create_progress_share(&self, share: &orch8_types::progress_share::ProgressShare) -> Result<(), StorageError>;
        async fn get_progress_share_by_hash(&self, token_hash: &str) -> Result<Option<orch8_types::progress_share::ProgressShare>, StorageError>;
        async fn list_progress_shares(&self, tenant_id: &TenantId, instance_id: InstanceId) -> Result<Vec<orch8_types::progress_share::ProgressShare>, StorageError>;
        async fn revoke_progress_share(&self, tenant_id: &TenantId, instance_id: InstanceId, share_id: Uuid, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn create_alert_rule(&self, rule: &orch8_types::alert::AlertRule) -> Result<(), StorageError>;
        async fn get_alert_rule(&self, tenant_id: Option<&TenantId>, id: Uuid) -> Result<Option<orch8_types::alert::AlertRule>, StorageError>;
        async fn list_alert_rules(&self, tenant_id: Option<&TenantId>, limit: u32) -> Result<Vec<orch8_types::alert::AlertRule>, StorageError>;
        async fn list_all_alert_rules(&self, tenant_id: Option<&TenantId>) -> Result<Vec<orch8_types::alert::AlertRule>, StorageError>;
        async fn update_alert_rule(&self, rule: &orch8_types::alert::AlertRule) -> Result<bool, StorageError>;
        async fn delete_alert_rule(&self, tenant_id: &TenantId, id: Uuid) -> Result<bool, StorageError>;
        async fn get_alert_rule_state(&self, rule_id: Uuid) -> Result<Option<orch8_types::alert::AlertRuleState>, StorageError>;
        async fn cas_alert_rule_state(&self, state: &orch8_types::alert::AlertRuleState, expected_version: i64) -> Result<bool, StorageError>;
        async fn get_trigger_poll_state(&self, slug: &str) -> Result<Option<TriggerPollState>, StorageError>;
        async fn upsert_trigger_poll_state(&self, state: &TriggerPollState) -> Result<(), StorageError>;
        async fn try_acquire_trigger_poll_lease(&self, slug: &str, owner: &str, now: chrono::DateTime<chrono::Utc>, lease_until: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn create_credential(&self, credential: &orch8_types::credential::CredentialDef) -> Result<(), StorageError>;
        async fn get_credential(&self, tenant_id: Option<&TenantId>, id: &str) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError>;
        async fn get_credential_for_claim(&self, instance_id: InstanceId, tenant_id: &TenantId, token: DateTime<Utc>, id: &str) -> Result<crate::ClaimedRead<Option<orch8_types::credential::CredentialDef>>, StorageError>;
        async fn list_credentials(&self, tenant_id: Option<&TenantId>, limit: u32) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;
        async fn list_all_credentials(&self, tenant_id: Option<&TenantId>) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;
        async fn update_credential(&self, credential: &orch8_types::credential::CredentialDef) -> Result<(), StorageError>;
        async fn update_credential_cas(&self, credential: &orch8_types::credential::CredentialDef, expected_updated_at: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn claim_credential_refresh(&self, id: &str, now: chrono::DateTime<chrono::Utc>, lease_until: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn record_credential_refresh_failure(&self, id: &str, now: chrono::DateTime<chrono::Utc>, backoff: fn(u32) -> chrono::TimeDelta) -> Result<u32, StorageError>;
        async fn reset_credential_refresh_failures(&self, id: &str) -> Result<(), StorageError>;
        async fn delete_credential(&self, id: &str) -> Result<(), StorageError>;
        async fn list_credentials_due_for_refresh(&self, threshold: std::time::Duration) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;
        async fn create_api_key(&self, key: &orch8_types::api_key::ApiKeyRecord) -> Result<(), StorageError>;
        async fn lookup_api_key_by_hash(&self, key_hash: &str) -> Result<Option<orch8_types::api_key::ApiKeyRecord>, StorageError>;
        async fn list_api_keys(&self, tenant_id: &TenantId) -> Result<Vec<orch8_types::api_key::ApiKeyRecord>, StorageError>;
        async fn revoke_api_key(&self, id: &str) -> Result<bool, StorageError>;
        async fn touch_api_key(&self, id: &str, at: chrono::DateTime<chrono::Utc>) -> Result<(), StorageError>;
        async fn register_node(&self, node: &orch8_types::cluster::ClusterNode) -> Result<(), StorageError>;
        async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError>;
        async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError>;
        async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError>;
        async fn reap_stale_nodes(&self, stale_threshold: std::time::Duration) -> Result<u64, StorageError>;
        async fn upsert_circuit_breaker(&self, state: &CircuitBreakerState) -> Result<(), StorageError>;
        async fn list_open_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>, StorageError>;
        async fn delete_circuit_breaker(&self, tenant_id: &TenantId, handler: &str) -> Result<(), StorageError>;
        async fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<(), StorageError>;
        async fn list_audit_log(&self, instance_id: InstanceId, limit: u32) -> Result<Vec<AuditLogEntry>, StorageError>;
        async fn list_audit_log_by_tenant(&self, tenant_id: &TenantId, limit: u32) -> Result<Vec<AuditLogEntry>, StorageError>;
        async fn list_tenant_changes(&self, tenant_id: &TenantId, after: Option<ChangeCursor>, limit: u32) -> Result<Vec<AuditLogEntry>, StorageError>;
        async fn create_rollback_policy(&self, tenant_id: &str, sequence_name: &str, error_rate_threshold: f64, time_window_secs: i32, cooldown_secs: Option<i32>, confirmation_window_secs: Option<i32>, webhook_url: Option<&str>) -> Result<(), StorageError>;
        async fn get_rollback_policy(&self, tenant_id: &str, sequence_name: &str) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError>;
        async fn list_rollback_policies(&self, tenant_id: Option<&str>, limit: u32) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError>;
        async fn delete_rollback_policy(&self, tenant_id: &str, sequence_name: &str) -> Result<(), StorageError>;
        async fn record_rollback(&self, tenant_id: &str, sequence_name: &str, error_rate: f64, threshold: f64, reason: &str) -> Result<(), StorageError>;
        async fn query_error_rate(&self, tenant_id: &str, sequence_name: &str, window_secs: i64) -> Result<Option<f64>, StorageError>;
        async fn query_error_stats(&self, tenant_id: &str, sequence_name: &str, window_secs: i64) -> Result<(i64, i64), StorageError>;
        async fn set_rollback_min_samples(&self, tenant_id: &str, sequence_name: &str, min_samples: i32) -> Result<(), StorageError>;
        async fn list_rollback_history(&self, tenant_id: Option<&str>, sequence_name: Option<&str>, limit: u32) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError>;
        async fn ping(&self) -> Result<(), StorageError>;
    }
}

fault_impl! {
    [TELEMETRY_STORE_METHODS] impl TelemetryStore {
        async fn ingest_telemetry_event(&self, event_type: &str, payload: &str, device_id: &str, os_name: &str, os_version: &str, app_version: &str, sdk_version: &str, tenant_id: &str, created_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn ingest_telemetry_events_batch(&self, events: &[TelemetryEvent]) -> Result<u64, StorageError>;
        async fn ingest_telemetry_error(&self, error_type: &str, message: &str, stack_trace: Option<&str>, device_id: &str, os_name: &str, os_version: &str, app_version: &str, sdk_version: &str, tenant_id: &str, instance_id: Option<&str>, sequence_name: Option<&str>) -> Result<(), StorageError>;
        async fn query_telemetry_dashboard(&self, query_type: &str, tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<(String, i64)>, StorageError>;
        async fn delete_old_telemetry_events(&self, older_than: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), StorageError>;
        async fn query_usage(&self, tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<UsageAggregate>, StorageError>;
        async fn query_instance_usage_totals(&self, instance_id: InstanceId) -> Result<(i64, i64), StorageError>;
    }
}

fault_impl! {
    [RESOURCE_STORE_METHODS] impl ResourceStore {
        async fn set_instance_kv(&self, instance_id: InstanceId, key: &str, value: &serde_json::Value) -> Result<(), StorageError>;
        async fn set_instance_kv_claimed(&self, instance_id: InstanceId, key: &str, value: &serde_json::Value, claim_token: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn get_instance_kv_claimed(&self, instance_id: InstanceId, tenant: &TenantId, key: &str, claim_token: DateTime<Utc>) -> Result<crate::ClaimedRead<Option<serde_json::Value>>, StorageError>;
        async fn get_instance_kv(&self, instance_id: InstanceId, key: &str) -> Result<Option<serde_json::Value>, StorageError>;
        async fn get_all_instance_kv(&self, instance_id: InstanceId) -> Result<HashMap<String, serde_json::Value>, StorageError>;
        async fn delete_instance_kv_claimed(&self, instance_id: InstanceId, key: &str, claim_token: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn delete_instance_kv(&self, instance_id: InstanceId, key: &str) -> Result<(), StorageError>;
        async fn delete_instance_kv_batch(&self, instance_id: InstanceId, keys: &[String]) -> Result<(), StorageError>;
        async fn set_shared_knowledge(&self, tenant_id: &str, namespace: &str, key: &str, value: &serde_json::Value) -> Result<(), StorageError>;
        async fn get_shared_knowledge(&self, tenant_id: &str, namespace: &str, key: &str) -> Result<Option<serde_json::Value>, StorageError>;
        async fn list_shared_knowledge(&self, tenant_id: &str, namespace: &str, limit: u32) -> Result<HashMap<String, serde_json::Value>, StorageError>;
        async fn delete_shared_knowledge(&self, tenant_id: &str, namespace: &str, key: &str) -> Result<(), StorageError>;
        async fn delete_shared_knowledge_batch(&self, tenant_id: &str, namespace: &str, keys: &[String]) -> Result<(), StorageError>;
        fn artifacts_enabled(&self) -> bool;
        async fn put_artifact(&self, _instance_id: InstanceId, _content_type: &str, _bytes: bytes::Bytes) -> Result<orch8_types::artifact::ArtifactRef, StorageError>;
        async fn put_artifact_with_id(&self, _instance_id: InstanceId, _artifact_id: Uuid, _content_type: &str, _bytes: bytes::Bytes) -> Result<orch8_types::artifact::ArtifactRef, StorageError>;
        async fn get_artifact(&self, _key: &str) -> Result<Option<Vec<u8>>, StorageError>;
        async fn delete_artifact(&self, _key: &str) -> Result<(), StorageError>;
        async fn list_artifacts(&self, _instance_id: InstanceId) -> Result<Vec<orch8_types::artifact::ArtifactMeta>, StorageError>;
        async fn delete_instance_artifacts(&self, instance_id: InstanceId) -> Result<u64, StorageError>;
        async fn list_artifact_gc_candidates(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<Vec<InstanceId>, StorageError>;
        async fn mark_artifacts_gced(&self, instance_id: InstanceId) -> Result<(), StorageError>;
        async fn delete_terminal_instances(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn list_instance_gc_candidates(&self, cutoff: DateTime<Utc>, limit: u32) -> Result<Vec<InstanceId>, StorageError>;
        async fn purge_terminal_instances(&self, ids: &[InstanceId]) -> Result<u64, StorageError>;
        async fn delete_expired_auxiliary(&self, retention: &AuxiliaryRetention, limit: u32) -> Result<u64, StorageError>;
        async fn compact_storage(&self) -> Result<(), StorageError>;
        async fn save_externalized_state(&self, instance_id: InstanceId, ref_key: &str, payload: &serde_json::Value) -> Result<(), StorageError>;
        async fn save_externalized_state_claimed(&self, instance_id: InstanceId, ref_key: &str, payload: &serde_json::Value, claim_token: chrono::DateTime<chrono::Utc>) -> Result<bool, StorageError>;
        async fn batch_save_externalized_state(&self, instance_id: InstanceId, entries: &[(String, serde_json::Value)]) -> Result<(), StorageError>;
        async fn list_externalized_entries(&self, instance_id: InstanceId, after: Option<&str>, limit: u32) -> Result<Vec<crate::externalizing::ExternalizedEntry>, StorageError>;
        async fn restore_externalized_entry(&self, entry: &crate::externalizing::ExternalizedEntry) -> Result<(), StorageError>;
        async fn get_externalized_state(&self, instance_id: InstanceId, ref_key: &str) -> Result<Option<serde_json::Value>, StorageError>;
        async fn batch_get_externalized_state(&self, refs: &[(InstanceId, String)]) -> Result<HashMap<(InstanceId, String), serde_json::Value>, StorageError>;
        async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError>;
        async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError>;
        async fn create_resource_pool(&self, pool: &orch8_types::pool::ResourcePool) -> Result<(), StorageError>;
        async fn get_resource_pool(&self, id: uuid::Uuid) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError>;
        async fn list_resource_pools(&self, tenant_id: &TenantId) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError>;
        async fn update_pool_round_robin_index(&self, pool_id: uuid::Uuid, index: u32) -> Result<(), StorageError>;
        async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError>;
        async fn add_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
        async fn list_pool_resources(&self, pool_id: uuid::Uuid) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError>;
        async fn update_pool_resource(&self, resource: &orch8_types::pool::PoolResource) -> Result<(), StorageError>;
        async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError>;
        async fn increment_resource_usage(&self, resource_id: uuid::Uuid, today: chrono::NaiveDate) -> Result<(), StorageError>;
        async fn save_checkpoint(&self, checkpoint: &orch8_types::checkpoint::Checkpoint) -> Result<(), StorageError>;
        async fn get_latest_checkpoint(&self, instance_id: InstanceId) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError>;
        async fn list_checkpoints(&self, instance_id: InstanceId, limit: u32) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError>;
        async fn prune_checkpoints(&self, instance_id: InstanceId, keep: u32) -> Result<u64, StorageError>;
    }
}

fault_impl! {
    [MOBILE_SYNC_STORE_METHODS] impl MobileSyncStore {
        async fn register_mobile_device(&self, device: &MobileDevice) -> Result<(), StorageError>;
        async fn get_mobile_device(&self, device_id: &str) -> Result<Option<MobileDevice>, StorageError>;
        async fn update_device_last_sync(&self, device_id: &str) -> Result<(), StorageError>;
        async fn list_mobile_devices(&self, tenant_id: Option<&str>, limit: u32) -> Result<Vec<MobileDevice>, StorageError>;
        async fn mark_stale_devices_inactive(&self, stale_threshold_secs: i64) -> Result<u64, StorageError>;
        async fn upsert_mobile_instance_status(&self, status: &MobileInstanceStatus) -> Result<(), StorageError>;
        async fn upsert_mobile_instance_status_batch(&self, statuses: &[MobileInstanceStatus]) -> Result<(), StorageError>;
        async fn list_mobile_instance_status(&self, tenant_id: Option<&str>, device_id: Option<&str>, limit: u32) -> Result<Vec<MobileInstanceStatus>, StorageError>;
        async fn insert_mobile_approval(&self, approval: &MobileApprovalRequest) -> Result<bool, StorageError>;
        async fn get_mobile_approval(&self, id: &str) -> Result<Option<MobileApprovalRequest>, StorageError>;
        async fn resolve_mobile_approval(&self, id: &str, resolution: &str) -> Result<Option<MobileApprovalRequest>, StorageError>;
        async fn list_mobile_approvals(&self, tenant_id: Option<&str>, state: Option<&str>, limit: u32) -> Result<Vec<MobileApprovalRequest>, StorageError>;
        async fn list_mobile_approvals_page(&self, tenant_id: Option<&str>, state: Option<&str>, limit: u32, offset: u64) -> Result<Vec<MobileApprovalRequest>, StorageError>;
        async fn expire_mobile_approvals(&self) -> Result<u64, StorageError>;
        async fn create_mobile_command(&self, command: &MobileCommand) -> Result<(), StorageError>;
        async fn create_mobile_command_if_absent(&self, command: &MobileCommand) -> Result<bool, StorageError>;
        async fn insert_mobile_approvals_batch(&self, approvals: &[MobileApprovalRequest]) -> Result<u64, StorageError>;
        async fn create_mobile_command_with_wake(&self, command: &MobileCommand, tenant_id: &str, created_at: chrono::DateTime<chrono::Utc>) -> Result<uuid::Uuid, StorageError>;
        async fn fetch_pending_commands(&self, device_id: &str, limit: u32) -> Result<Vec<MobileCommand>, StorageError>;
        async fn ack_mobile_commands(&self, device_id: &str, command_ids: &[String]) -> Result<u64, StorageError>;
        async fn cleanup_acked_commands(&self, older_than_secs: i64) -> Result<u64, StorageError>;
        async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError>;
    }
}

fault_impl! {
    [CONTINUITY_STORE_METHODS] impl ContinuityStore {
    async fn get_delegation_publication(&self,tenant:&orch8_types::ids::TenantId,id:uuid::Uuid)->Result<Option<crate::delegation_publication::DelegationPublication>,StorageError>;
    async fn publish_delegation(&self,intent:&crate::delegation_publication::DelegationPublishIntent)->Result<crate::delegation_publication::DelegationPublishResult,StorageError>;

        async fn is_physical_instance_retired(&self, instance_id: InstanceId) -> Result<bool, StorageError>;
        async fn create_continuity_execution(&self, execution: &ContinuityExecution) -> Result<(), StorageError>;
        async fn ensure_continuity_execution(&self, candidate: &ContinuityExecution) -> Result<ContinuityExecution, StorageError>;
        async fn get_continuity_execution(&self, tenant_id: &TenantId, id: ContinuityId) -> Result<Option<ContinuityExecution>, StorageError>;
        async fn get_continuity_execution_by_instance(&self, tenant_id: &TenantId, instance_id: InstanceId) -> Result<Option<ContinuityExecution>, StorageError>;
        async fn get_continuity_execution_touching_instance(&self, tenant_id: &TenantId, instance_id: InstanceId) -> Result<Option<ContinuityExecution>, StorageError>;
        async fn list_continuity_locations(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity::ContinuityLocation>, StorageError>;
        async fn cas_continuity_owner(&self, tenant_id: &TenantId, id: ContinuityId, expected_epoch: ExecutionEpoch, expected_owner: RuntimeId, next: &ContinuityExecution) -> Result<bool, StorageError>;
        async fn create_handoff(&self, handoff: &ExecutionHandoff) -> Result<(), StorageError>;
        async fn get_handoff(&self, tenant_id: &TenantId, id: HandoffId) -> Result<Option<ExecutionHandoff>, StorageError>;
        async fn cas_handoff(&self, tenant_id: &TenantId, id: HandoffId, expected_state: HandoffState, expected_version: u64, next: &ExecutionHandoff) -> Result<bool, StorageError>;
        async fn accept_handoff(&self, tenant_id: &TenantId, expected_handoff: &ExecutionHandoff, accepted_handoff: &ExecutionHandoff, expected_execution: &ContinuityExecution, accepted_execution: &ContinuityExecution) -> Result<bool, StorageError>;
        async fn commit_handoff_export(&self, tenant_id: &TenantId, expected_handoff: &ExecutionHandoff, exported_handoff: &ExecutionHandoff, expected_execution: &ContinuityExecution, transferring_execution: &ContinuityExecution) -> Result<bool, StorageError>;
        async fn commit_handoff_export_with_provenance(&self, tenant_id: &TenantId, expected_handoff: &ExecutionHandoff, exported_handoff: &ExecutionHandoff, expected_execution: &ContinuityExecution, transferring_execution: &ContinuityExecution, provenance: &ProvenanceEntry) -> Result<bool, StorageError>;
        async fn resume_handoff(&self, tenant_id: &TenantId, expected_handoff: &ExecutionHandoff, resumed_handoff: &ExecutionHandoff, destination_instance_id: InstanceId) -> Result<bool, StorageError>;
        async fn save_capsule_manifest(&self, manifest: &CapsuleManifest) -> Result<(), StorageError>;
        async fn get_capsule_manifest(&self, tenant_id: &TenantId, id: CapsuleId) -> Result<Option<CapsuleManifest>, StorageError>;
        async fn import_capsule_instance(&self, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, instance: &TaskInstance, checkpoint: &orch8_types::checkpoint::Checkpoint) -> Result<InstanceId, StorageError>;
        async fn import_capsule_instance_with_outputs(&self, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, instance: &TaskInstance, checkpoint: &orch8_types::checkpoint::Checkpoint, outputs: &[orch8_types::output::BlockOutput]) -> Result<InstanceId, StorageError>;
        async fn import_capsule_snapshot(&self, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, snapshot: &crate::restore::InstanceSnapshot, checkpoint: &orch8_types::checkpoint::Checkpoint) -> Result<InstanceId, StorageError>;
        async fn import_capsule_recovery(&self, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, snapshot: &crate::restore::InstanceSnapshot, checkpoint: &orch8_types::checkpoint::Checkpoint, effects: Option<&crate::portable_effects::CapsuleEffectImport>) -> Result<InstanceId, StorageError>;
        async fn record_external_capsule_import(&self, tenant_id: &TenantId, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, instance_id: InstanceId, imported_at: DateTime<Utc>) -> Result<InstanceId, StorageError>;
        async fn prepare_local_migration_snapshot(&self, request: &crate::local_migration::LocalMigrationSnapshotRequest) -> Result<crate::local_migration::LocalMigrationSnapshotCapture,StorageError>;
        async fn capture_capsule_cut(&self, request: &crate::capsule_export::CapsuleCutRequest) -> Result<crate::capsule_export::CapsuleCutCapture, StorageError>;
        async fn seal_capsule_cut(&self, tenant: &TenantId, id: CapsuleId, manifest: &CapsuleManifest, sealed: &[u8]) -> Result<crate::capsule_export::CapsuleSourceCut, StorageError>;
        async fn abort_preparing_capsule_cut(&self, tenant: &TenantId, id: CapsuleId) -> Result<bool, StorageError>;
        async fn acknowledge_capsule_cut(&self, tenant: &TenantId, id: CapsuleId, accepted: &ContinuityExecution) -> Result<crate::capsule_export::CapsuleCutReceipt, StorageError>;
        async fn capsule_cut_publishable(&self, tenant: &TenantId, id: CapsuleId) -> Result<bool, StorageError>;
        async fn claim_retired_capsule_artifacts(&self, now: DateTime<Utc>, retry_at: DateTime<Utc>, limit: u32) -> Result<Vec<String>, StorageError>;
        async fn activate_capsule_import(&self, tenant: &TenantId, capsule: CapsuleId, runtime: RuntimeId, instance: InstanceId, now: DateTime<Utc>) -> Result<(), StorageError>;
        async fn is_capsule_import_instance(&self, tenant_id: &TenantId, capsule_id: CapsuleId, destination_runtime_id: RuntimeId, instance_id: InstanceId) -> Result<bool, StorageError>;
        async fn upsert_runtime_capabilities(&self, tenant_id: &TenantId, capabilities: &RuntimeCapabilities) -> Result<(), StorageError>;
        async fn list_runtime_capabilities(&self, tenant_id: &TenantId, observed_after: DateTime<Utc>, limit: u32) -> Result<Vec<RuntimeCapabilities>, StorageError>;
        async fn get_runtime_capability(&self, tenant_id: &TenantId, runtime_id: orch8_types::continuity::RuntimeId, observed_after: DateTime<Utc>) -> Result<Option<RuntimeCapabilities>, StorageError>;
        async fn list_runtime_capabilities_by_kind(&self, tenant_id: &TenantId, observed_after: DateTime<Utc>, kinds: &[orch8_types::continuity::RuntimeKind], per_kind_limit: u32) -> Result<Vec<RuntimeCapabilities>, StorageError>;
        async fn prune_expired_runtime_capabilities(&self, expired_before: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn create_effect_receipt(&self, receipt: &EffectReceipt) -> Result<(), StorageError>;
        async fn ensure_effect_receipt(&self, receipt: &EffectReceipt) -> Result<EffectReceipt, StorageError>;
        async fn ensure_effect_attempt(&self, candidate: &EffectReceipt, legacy_id: EffectId) -> Result<EffectReceipt, StorageError>;
        async fn find_unresolved_effect_receipt(&self, tenant_id: &TenantId, continuity_id: ContinuityId, instance_id: InstanceId, block_id: &orch8_types::ids::BlockId, attempt: u32) -> Result<Option<EffectReceipt>, StorageError>;
    async fn delete_effect_receipts_for_blocks_claimed(&self, tenant_id: &orch8_types::ids::TenantId, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId], claim_token: DateTime<Utc>) -> Result<Option<u64>, StorageError>;
    async fn retire_effect_attempts_for_blocks_claimed(&self, tenant_id: &orch8_types::ids::TenantId, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId], claim_token: DateTime<Utc>) -> Result<Option<u64>, StorageError>;

        async fn delete_effect_receipts_for_blocks(&self, tenant_id: &TenantId, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId]) -> Result<u64, StorageError>;
        async fn retire_effect_attempts_for_blocks(&self, tenant_id: &TenantId, instance_id: InstanceId, block_ids: &[orch8_types::ids::BlockId]) -> Result<u64, StorageError>;
        async fn get_effect_receipt(&self, tenant_id: &TenantId, id: EffectId) -> Result<Option<EffectReceipt>, StorageError>;
        async fn cas_effect_receipt(&self, tenant_id: &TenantId, id: EffectId, expected_state: EffectState, next: &EffectReceipt) -> Result<bool, StorageError>;
        async fn resolve_effect_with_provenance(&self, expected: &EffectReceipt, next: &EffectReceipt, entry: &ProvenanceEntry) -> Result<crate::effect_resolution::EffectResolutionCommit, StorageError>;
        async fn dispatch_effect_receipt_owned(&self, tenant_id: &TenantId, next: &EffectReceipt, fence: &crate::EffectDispatchFence, at_most_once: bool) -> Result<orch8_types::continuity::EffectDispatchOutcome, StorageError>;
        async fn dispatch_effect_receipt_at_most_once(&self, tenant_id: &TenantId, next: &EffectReceipt) -> Result<orch8_types::continuity::EffectDispatchOutcome, StorageError>;
        async fn list_effect_receipts(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<EffectReceipt>, StorageError>;
        async fn collect_compensation_effects(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId) -> Result<Vec<crate::CompensationEffect>, StorageError>;
        async fn summarize_effect_receipts(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId) -> Result<crate::EffectLedgerStatus, StorageError>;
        async fn audit_effect_at_most_once(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, kind: orch8_types::continuity::EffectKind, candidate: Option<&orch8_types::continuity::EffectReceipt>) -> Result<crate::EffectAtMostOnceAudit, StorageError>;
        async fn page_effect_receipts(&self, tenant_id: &orch8_types::ids::TenantId, continuity_id: orch8_types::continuity::ContinuityId, after: Option<&crate::EffectReceiptCursor>, limit: u32) -> Result<crate::EffectReceiptPage, StorageError>;
        async fn export_effect_attempts(&self, owner: &orch8_types::continuity::ContinuityExecution) -> Result<(Vec<orch8_types::continuity::CapsuleEffectBinding>, Vec<orch8_types::ids::BlockId>), StorageError>;
        async fn load_effect_guards(&self, tenant: &orch8_types::ids::TenantId, continuity: orch8_types::continuity::ContinuityId, sequence: orch8_types::ids::SequenceId, version: i32, kind: Option<orch8_types::continuity::EffectKind>) -> Result<Vec<orch8_types::continuity_advanced::WorkflowInvariant>, StorageError>;
        async fn list_instance_effect_receipts(&self, tenant_id: &TenantId, instance_id: InstanceId, limit: u32) -> Result<Vec<EffectReceipt>, StorageError>;
        async fn append_provenance(&self, entry: &ProvenanceEntry) -> Result<(), StorageError>;
        async fn get_provenance_head(&self, tenant_id: &TenantId, continuity_id: ContinuityId) -> Result<Option<ProvenanceEntry>, StorageError>;
        async fn list_provenance(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<ProvenanceEntry>, StorageError>;
        async fn accept_federation_message(&self, envelope: &FederationEnvelope, envelope_sha256: &str, accepted_at: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn upsert_federation_peer(&self, peer: &orch8_types::federation::FederationPeerRecord) -> Result<(), StorageError>;
        async fn get_federation_peer(&self, tenant_id: &TenantId, peer_id: orch8_types::continuity_advanced::FederationPeerId) -> Result<Option<orch8_types::federation::FederationPeerRecord>, StorageError>;
        async fn list_federation_peers(&self, tenant_id: &TenantId) -> Result<Vec<orch8_types::federation::FederationPeerRecord>, StorageError>;
        async fn delete_federation_peer(&self, tenant_id: &TenantId, peer_id: orch8_types::continuity_advanced::FederationPeerId) -> Result<bool, StorageError>;
        async fn create_federation_call(&self, call: &orch8_types::federation::FederationCall) -> Result<bool, StorageError>;
        async fn create_federation_call_claimed(&self, call: &orch8_types::federation::FederationCall, claim_token: DateTime<Utc>) -> Result<Option<bool>, StorageError>;
        async fn get_federation_call(&self, tenant_id: &TenantId, call_id: Uuid) -> Result<Option<orch8_types::federation::FederationCall>, StorageError>;
        async fn cas_federation_call(&self, expected_version: u64, next: &orch8_types::federation::FederationCall) -> Result<bool, StorageError>;
        async fn list_due_federation_calls(&self, now: DateTime<Utc>, limit: u32) -> Result<Vec<orch8_types::federation::FederationCall>, StorageError>;
        async fn get_region_fence(&self) -> Result<Option<orch8_types::federation::RegionFence>, StorageError>;
        async fn advance_region_fence(&self, expected_epoch: Option<u64>, next: &orch8_types::federation::RegionFence) -> Result<bool, StorageError>;
        async fn save_incident_reproduction(&self, reproduction: &DlqIncidentReproduction) -> Result<(), StorageError>;
        async fn list_incident_reproductions(&self, tenant_id: &TenantId, fingerprint: &str, limit: u32) -> Result<Vec<DlqIncidentReproduction>, StorageError>;
        async fn create_continuation_grant(&self, grant: &ContinuationGrant) -> Result<(), StorageError>;
        async fn get_continuation_grant(&self, tenant_id: &TenantId, id: ContinuationGrantId) -> Result<Option<ContinuationGrant>, StorageError>;
        async fn consume_continuation_grant(&self, tenant_id: &TenantId, id: ContinuationGrantId, nonce_sha256: &str, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn cas_continuation_grant_state(&self, tenant_id: &TenantId, id: ContinuationGrantId, expected: ContinuationGrantState, next: &ContinuationGrant) -> Result<bool, StorageError>;
        async fn save_placement_decision(&self, decision: &PlacementDecision) -> Result<(), StorageError>;
        async fn get_placement_decision(&self, tenant_id: &TenantId, id: PlacementDecisionId) -> Result<Option<PlacementDecision>, StorageError>;
        async fn create_continuity_stream(&self, stream: &ContinuityStream) -> Result<(), StorageError>;
        async fn get_continuity_stream(&self, tenant_id: &TenantId, stream_id: StreamId) -> Result<Option<ContinuityStream>, StorageError>;
        async fn append_stream_frame(&self, frame: &StreamFrame) -> Result<bool, StorageError>;
        async fn list_stream_frames(&self, tenant_id: &TenantId, stream_id: StreamId, after_sequence: Option<u64>, now: DateTime<Utc>, limit: u32) -> Result<Vec<StreamFrame>, StorageError>;
        async fn retract_stream_frames(&self, tenant_id: &TenantId, stream_id: StreamId, epoch: ExecutionEpoch, after_sequence: u64) -> Result<u64, StorageError>;
        async fn save_live_migration(&self, record: &orch8_types::continuity_advanced::LiveMigrationRecord) -> Result<(), StorageError>;
        async fn get_live_migration(&self, tenant_id: &TenantId, id: orch8_types::continuity_advanced::MigrationPlanId) -> Result<Option<orch8_types::continuity_advanced::LiveMigrationRecord>, StorageError>;
        async fn cas_live_migration(&self, tenant_id: &TenantId, expected: &orch8_types::continuity_advanced::LiveMigrationRecord, next: &orch8_types::continuity_advanced::LiveMigrationRecord) -> Result<bool, StorageError>;
        async fn commit_live_migration_transition(&self, transition: LiveMigrationTransition<'_>) -> Result<bool, StorageError>;
        async fn begin_compensation_run(&self, start: crate::CompensationStart<'_>) -> Result<bool, StorageError>;
        async fn create_compensation_run(&self, run: &orch8_types::continuity_advanced::CompensationRunRecord) -> Result<bool, StorageError>;
        async fn get_active_compensation_run(&self, tenant_id: &TenantId, continuity_id: orch8_types::continuity::ContinuityId) -> Result<Option<orch8_types::continuity_advanced::CompensationRunRecord>, StorageError>;
        async fn get_compensation_run(&self, tenant_id: &TenantId, id: orch8_types::continuity_advanced::CompensationRunId) -> Result<Option<orch8_types::continuity_advanced::CompensationRunRecord>, StorageError>;
        async fn cas_compensation_run(&self, tenant_id: &TenantId, expected_version: u64, next: &orch8_types::continuity_advanced::CompensationRunRecord) -> Result<bool, StorageError>;
    }
}

fault_impl! {
    [INVARIANT_STORE_METHODS] impl InvariantStore {
        async fn create_workflow_invariant(&self, invariant: &WorkflowInvariant) -> Result<(), StorageError>;
        async fn list_effect_commit_guards(&self, tenant_id: &TenantId, sequence_id: SequenceId, sequence_version: i32, kind: orch8_types::continuity::EffectKind, limit: u32) -> Result<Vec<WorkflowInvariant>, StorageError>;
        async fn list_workflow_invariants(&self, tenant_id: &TenantId, sequence_id: SequenceId, sequence_version: i32, limit: u32) -> Result<Vec<WorkflowInvariant>, StorageError>;
        async fn append_invariant_result(&self, tenant_id: &TenantId, result: &InvariantResult) -> Result<bool, StorageError>;
        async fn list_invariant_results(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<InvariantResult>, StorageError>;
        async fn save_what_if_run(&self, run: &orch8_types::continuity_advanced::WhatIfRunRecord) -> Result<(), StorageError>;
        async fn list_what_if_runs(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<orch8_types::continuity_advanced::WhatIfRunRecord>, StorageError>;
    }
}

fault_impl! {
    [EVALUATION_STORE_METHODS] impl EvaluationStore {
        async fn append_evaluation_score(&self, score: &EvaluationScore) -> Result<bool, StorageError>;
        async fn list_evaluation_scores(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<EvaluationScore>, StorageError>;
    }
}

fault_impl! {
    [ATTENTION_STORE_METHODS] impl AttentionStore {
        async fn create_attention_task(&self, task: &AttentionTask) -> Result<(), StorageError>;
        async fn get_attention_task(&self, tenant_id: &TenantId, id: AttentionTaskId) -> Result<Option<AttentionTask>, StorageError>;
        async fn claim_attention_task(&self, tenant_id: &TenantId, expected: &AttentionTask, assigned: &AttentionTask, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn reassign_expired_attention_task(&self, expected: &AttentionTask, assigned: &AttentionTask, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn decide_attention_task(&self, expected: &AttentionTask, decided: &AttentionTask, now: DateTime<Utc>) -> Result<bool, StorageError>;
        async fn reserve_budget(&self, reservation: &BudgetReservation, budget: &orch8_types::instance::Budget) -> Result<bool, StorageError>;
        async fn get_budget_reservation(&self, tenant_id: &TenantId, id: orch8_types::continuity_advanced::BudgetReservationId) -> Result<Option<BudgetReservation>, StorageError>;
        async fn list_budget_reservations(&self, tenant_id: &TenantId, continuity_id: ContinuityId, limit: u32) -> Result<Vec<BudgetReservation>, StorageError>;
        async fn settle_budget_reservation(&self, reservation: &BudgetReservation) -> Result<bool, StorageError>;
    }
}

fault_impl! {
    [AI_STORE_METHODS] impl AiStore {
        async fn insert_prompt_version(&self, prompt: &orch8_types::ai::PromptTemplate) -> Result<(), StorageError>;
        async fn get_prompt_version(&self, tenant_id: &str, name: &str, version: i32) -> Result<Option<orch8_types::ai::PromptTemplate>, StorageError>;
        async fn get_latest_prompt_version(&self, tenant_id: &str, name: &str) -> Result<Option<orch8_types::ai::PromptTemplate>, StorageError>;
        async fn list_prompt_versions(&self, tenant_id: &str, name: Option<&str>, limit: u32) -> Result<Vec<orch8_types::ai::PromptTemplate>, StorageError>;
        async fn upsert_prompt_label(&self, label: &orch8_types::ai::PromptLabel) -> Result<(), StorageError>;
        async fn get_prompt_label(&self, tenant_id: &str, name: &str, label: &str) -> Result<Option<orch8_types::ai::PromptLabel>, StorageError>;
        async fn list_prompt_labels(&self, tenant_id: &str, name: Option<&str>) -> Result<Vec<orch8_types::ai::PromptLabel>, StorageError>;
        async fn delete_prompt_label(&self, tenant_id: &str, name: &str, label: &str) -> Result<bool, StorageError>;
        async fn get_llm_cache_entry(&self, tenant_id: &str, cache_key: &str, now: DateTime<Utc>) -> Result<Option<orch8_types::ai::LlmCacheEntry>, StorageError>;
        async fn put_llm_cache_entry(&self, entry: &orch8_types::ai::LlmCacheEntry) -> Result<(), StorageError>;
        async fn list_llm_cache_partition(&self, tenant_id: &str, partition_key: &str, now: DateTime<Utc>, limit: u32) -> Result<Vec<orch8_types::ai::LlmCacheEntry>, StorageError>;
        async fn purge_llm_cache(&self, tenant_id: &str) -> Result<u64, StorageError>;
        async fn delete_expired_llm_cache(&self, now: DateTime<Utc>, limit: u32) -> Result<u64, StorageError>;
        async fn upsert_tenant_budget(&self, budget: &orch8_types::ai::TenantBudget) -> Result<(), StorageError>;
        async fn list_tenant_budgets(&self, tenant_id: &str) -> Result<Vec<orch8_types::ai::TenantBudget>, StorageError>;
        async fn delete_tenant_budget(&self, tenant_id: &str, id: uuid::Uuid) -> Result<bool, StorageError>;
        async fn record_budget_alert(&self, alert: &orch8_types::ai::BudgetAlert) -> Result<bool, StorageError>;
        async fn list_budget_alerts(&self, tenant_id: &str, limit: u32) -> Result<Vec<orch8_types::ai::BudgetAlert>, StorageError>;
    }
}

fault_impl! {
    [TENANCY_STORE_METHODS] impl TenancyStore {
        async fn create_sub_tenant_instances_admitted(&self, instances: &[TaskInstance], max_active_instances: u64, now: DateTime<Utc>) -> Result<u64, StorageError>;
        async fn get_sub_tenant_limits(&self, tenant_id: &TenantId, sub_tenant: &str) -> Result<Option<orch8_types::sub_tenant::SubTenantLimits>, StorageError>;
        async fn put_sub_tenant_limits(&self, tenant_id: &TenantId, sub_tenant: &str, limits: &orch8_types::sub_tenant::SubTenantLimits) -> Result<(), StorageError>;
        async fn sub_tenant_usage(&self, tenant_id: &TenantId, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<Vec<orch8_types::sub_tenant::SubTenantUsage>, StorageError>;
        async fn count_active_sub_tenants(&self, since: DateTime<Utc>) -> Result<u64, StorageError>;
        async fn ensure_sub_tenant_limits(&self, tenant_id: &TenantId, sub_tenant: &str, limits: &orch8_types::sub_tenant::SubTenantLimits) -> Result<bool, StorageError>;
        async fn set_sub_tenant_suspended(&self, tenant_id: &TenantId, sub_tenant: &str, suspended: bool) -> Result<(), StorageError>;
        async fn revoke_embed_token(&self, tenant_id: &TenantId, jti: Uuid, expires_at: DateTime<Utc>) -> Result<(), StorageError>;
        async fn embed_access_revoked(&self, tenant_id: &TenantId, sub_tenant: &str, jti: Uuid) -> Result<bool, StorageError>;
        async fn get_embed_theme(&self, tenant_id: &TenantId) -> Result<Option<orch8_types::sub_tenant::EmbedTheme>, StorageError>;
        async fn put_embed_theme(&self, tenant_id: &TenantId, theme: &orch8_types::sub_tenant::EmbedTheme) -> Result<(), StorageError>;
        async fn set_release_target(&self, release_id: Uuid, target: Option<&orch8_types::release::ReleaseTarget>) -> Result<bool, StorageError>;
    }
}

