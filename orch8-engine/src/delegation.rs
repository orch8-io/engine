//! Device-mesh delegation through the server mailbox (Feature 29).
//!
//! A claimed [`DeviceDelegation`] becomes an ordinary worker task *targeted*
//! at the destination runtime (`$runtime.runtime_id`), so it rides the same
//! lease protocol as every other runtime node: the destination polls
//! `handler_name = "orch8.delegation"`, runs the delegated sub-sequence
//! locally (no shared mutable execution state: the task carries only the
//! delegation identity and its explicit input), and completes or fails the
//! task. Completion is fenced (claim epoch + owner epoch), commits the effect
//! receipt created at enqueue, and integrates the result into the parent:
//! a `delegation-<id>` block output, `context.data.delegations.<id>`, and a
//! wake-up of the waiting parent. Failures and timeouts integrate a
//! `failed` outcome instead of failing the parent.

use chrono::Utc;
use orch8_storage::StorageBackend;
use orch8_types::continuity_advanced::DeviceDelegation;
use orch8_types::ids::BlockId;
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use serde_json::{Value, json};

use crate::error::EngineError;

/// Reserved handler name a destination runtime polls for delegated work.
pub const DELEGATION_HANDLER: &str = "orch8.delegation";

/// Block id under which a delegation's task and result live on the parent.
#[must_use]
pub fn delegation_block_id(delegation: &DeviceDelegation) -> BlockId {
    BlockId::new(format!("delegation-{}", delegation.id))
}

#[must_use]
pub fn is_delegation_task(task: &WorkerTask) -> bool {
    task.handler_name == DELEGATION_HANDLER
}

/// Enqueue the mailbox task for a validated, grant-consumed delegation.
/// Idempotent per delegation (the `(instance, block)` row is unique).
pub async fn enqueue_delegation_task(
    storage: &dyn StorageBackend,
    parent: &TaskInstance,
    delegation: &DeviceDelegation,
    sub_sequence: &orch8_types::sequence::SequenceDefinition,
    input: Value,
) -> Result<WorkerTask, EngineError> {
    let block_id = delegation_block_id(delegation);
    let params = json!({
        "delegation_id": delegation.id,
        "grant_id": delegation.grant_id,
        "parent_continuity_id": delegation.parent_continuity_id,
        "parent_epoch": delegation.parent_epoch,
        "source_runtime_id": delegation.source_runtime_id,
        "sub_sequence_id": sub_sequence.id,
        "sub_sequence_name": sub_sequence.name,
        "sub_sequence_version": sub_sequence.version,
        "expires_at": delegation.expires_at,
        "input": input,
    });
    let guard = crate::effect_guard::EffectGuard::begin(
        storage,
        &parent.tenant_id,
        parent.id,
        &block_id,
        DELEGATION_HANDLER,
        &params,
        0,
    )
    .await?;
    let now = Utc::now();
    let timeout_ms = (delegation.expires_at - now).num_milliseconds().max(1);
    let continuity_epoch = storage
        .get_continuity_execution_by_instance(&parent.tenant_id, parent.id)
        .await?
        .map(|execution| execution.epoch.get());
    let task = WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: parent.id,
        block_id,
        handler_name: DELEGATION_HANDLER.to_owned(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements {
            runtime_id: Some(delegation.destination_runtime_id),
            ..orch8_types::continuity::CapsuleRequirements::default()
        },
        params,
        // Delegation shares no mutable execution state with the parent.
        context: json!({}),
        attempt: 0,
        timeout_ms: Some(timeout_ms),
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: now,
        effect_id: guard
            .as_ref()
            .map(crate::effect_guard::EffectGuard::effect_id),
        continuity_epoch,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    };
    storage.create_worker_task(&task).await?;
    Ok(task)
}

/// Integrate a delegation's outcome into its parent instance: record the
/// result block output, merge it at `context.data.delegations.<id>`, and wake
/// the parent if it is waiting. `Err(reason)` integrates a failed outcome.
pub async fn integrate_delegation_outcome(
    storage: &dyn StorageBackend,
    task: &WorkerTask,
    outcome: Result<&Value, &str>,
) -> Result<(), EngineError> {
    let delegation_id = task
        .params
        .get("delegation_id")
        .cloned()
        .unwrap_or(Value::Null);
    let result = match outcome {
        Ok(output) => json!({
            "status": "completed",
            "delegation_id": delegation_id,
            "runtime_id": task.worker_id,
            "output": output,
        }),
        Err(reason) => json!({
            "status": "failed",
            "delegation_id": delegation_id,
            "runtime_id": task.worker_id,
            "error": reason,
        }),
    };
    let size = serde_json::to_vec(&result)
        .map_or(0, |bytes| u32::try_from(bytes.len()).unwrap_or(u32::MAX));
    storage
        .save_block_output(&orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: task.instance_id,
            block_id: task.block_id.clone(),
            output: result.clone(),
            output_ref: None,
            output_size: size,
            attempt: task.attempt,
            created_at: Utc::now(),
        })
        .await?;
    let Some(parent) = storage.get_instance(task.instance_id).await? else {
        return Ok(());
    };
    let mut delegations = parent
        .context
        .data
        .get("delegations")
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let (Some(map), Some(key)) = (delegations.as_object_mut(), delegation_id.as_str()) {
        map.insert(key.to_owned(), result);
    }
    storage
        .merge_context_data(task.instance_id, "delegations", &delegations)
        .await?;
    if parent.state == InstanceState::Waiting {
        let _ = storage
            .conditional_update_instance_state(
                parent.id,
                InstanceState::Waiting,
                InstanceState::Scheduled,
                Some(Utc::now()),
            )
            .await?;
    }
    Ok(())
}
