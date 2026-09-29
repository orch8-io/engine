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
//!
//! A parent hosted by a runtime (a workflow on a phone's local engine) has
//! no instance on the server. Its delegation is anchored on a **proxy**
//! instance whose id is the delegation id: the mailbox task hangs off the
//! proxy, the result is integrated into it the same way, and the proxy then
//! turns terminal. The hosting runtime learns the result by reading the
//! delegation (`GET /continuity/delegations/{id}`) and resumes its local
//! parent itself, fenced locally ([`resume_local_parent`]).

use chrono::Utc;
use orch8_storage::StorageBackend;
use orch8_types::continuity::ContinuityExecution;
use orch8_types::continuity_advanced::DeviceDelegation;
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::worker::{WorkerClaim, WorkerTask, WorkerTaskState};
use serde_json::{Value, json};

use crate::error::EngineError;

/// Reserved handler name a destination runtime polls for delegated work.
pub const DELEGATION_HANDLER: &str = "orch8.delegation";

/// Metadata key marking a delegation proxy instance (see the module docs).
pub const DELEGATION_PROXY_KEY: &str = "orch8_delegation_proxy";

/// Namespace of the one-step sequences an isolated delegated step runs as.
pub const STEP_SEQUENCE_NAMESPACE: &str = "default";

/// Deterministic id of the one-step sequence that runs `handler` as block
/// `block` for `tenant` (an isolated step delegated from a runtime-hosted
/// parent). The phone and the control plane derive the same id, so the
/// sequence is published at most once per (tenant, handler, block).
#[must_use]
pub fn step_sequence_id(tenant: &str, handler: &str, block: &str) -> uuid::Uuid {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(b"orch8-delegated-step-v1\0");
    for part in [tenant, handler, block] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

/// The one-step sequence (as its JSON document) an isolated delegated step
/// runs as: `handler` on `{{context.data.params}}` in block `block`, under
/// [`step_sequence_id`].
#[must_use]
pub fn step_sequence_document(tenant: &str, handler: &str, block: &str) -> Value {
    let id = step_sequence_id(tenant, handler, block);
    json!({
        "id": id, "tenant_id": tenant, "namespace": STEP_SEQUENCE_NAMESPACE,
        "name": format!("orch8-delegated-{}", &id.simple().to_string()[..12]),
        "version": 1, "deprecated": false, "interceptors": null,
        "blocks": [{"type": "step", "id": block, "handler": handler,
                    "params": "{{context.data.params}}", "cancellable": true}],
        "created_at": Utc::now().to_rfc3339(),
    })
}

/// Whether `instance` is the server-side proxy of a delegation whose parent
/// is hosted by a runtime.
#[must_use]
pub fn is_delegation_proxy(instance: &TaskInstance) -> bool {
    instance.metadata.get(DELEGATION_PROXY_KEY).is_some()
}

/// Create (or return, on a retried claim) the proxy that anchors the mailbox
/// task of a delegation whose parent `execution` is hosted by a runtime.
/// The proxy's id is the delegation id; it sits in `waiting` until the
/// outcome is integrated and never runs its sequence.
pub async fn ensure_delegation_proxy(
    storage: &dyn StorageBackend,
    execution: &ContinuityExecution,
    delegation: &DeviceDelegation,
    sub_sequence: &orch8_types::sequence::SequenceDefinition,
) -> Result<TaskInstance, EngineError> {
    let id = InstanceId::from_uuid(delegation.id.into_uuid());
    if let Some(existing) = storage.get_instance(id).await? {
        let same = existing.tenant_id == delegation.tenant_id
            && existing.metadata[DELEGATION_PROXY_KEY]["delegation"]["id"]
                == serde_json::to_value(delegation.id).unwrap_or(Value::Null);
        return if same {
            Ok(existing)
        } else {
            Err(EngineError::InvalidConfig(
                "delegation id collides with an existing instance".into(),
            ))
        };
    }
    let now = Utc::now();
    let proxy = TaskInstance {
        id,
        sequence_id: sub_sequence.id,
        tenant_id: delegation.tenant_id.clone(),
        namespace: sub_sequence.namespace.clone(),
        state: InstanceState::Waiting,
        next_fire_at: None,
        priority: orch8_types::instance::Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({ DELEGATION_PROXY_KEY: {
            "delegation": delegation,
            "parent_instance_id": execution.current_instance_id,
        }}),
        context: orch8_types::context::ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    storage.create_instance(&proxy).await?;
    Ok(proxy)
}

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
    if is_delegation_proxy(&parent) {
        // The proxy only holds the outcome for the hosting runtime to read;
        // it never runs, so it goes straight to a terminal state.
        let terminal = if outcome.is_ok() {
            InstanceState::Completed
        } else {
            InstanceState::Failed
        };
        if storage
            .conditional_update_instance_state(
                parent.id,
                InstanceState::Waiting,
                InstanceState::Running,
                None,
            )
            .await?
        {
            let _ = storage
                .conditional_update_instance_state(
                    parent.id,
                    InstanceState::Running,
                    terminal,
                    None,
                )
                .await?;
        }
    } else if parent.state == InstanceState::Waiting {
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

/// Resume a parked local step with the outcome of its delegation — the
/// hosting runtime's half of a delegation from a runtime-hosted parent.
///
/// The local step was dispatched as a local worker task (placed off this
/// runtime) that the runtime's delegation pump claimed as `claim`. `Ok`
/// completes it exactly like a worker completion (receipt committed, block
/// output saved, output merged into `context.data`, node completed, instance
/// rescheduled, all in one write); `Err((message, retryable))` fails it
/// through the fenced failure resolution (retry policy or fail the step).
/// `delegation_result` is merged at `context.data.delegations.<id>`, like a
/// server-hosted parent's integration.
///
/// Fenced and idempotent: `Ok(false)` when the task is no longer held by
/// `claim` and its completion needs no further work. A crash after the task
/// was marked completed but before the instance transition is finished by
/// the next call.
pub async fn resume_local_parent(
    storage: &dyn StorageBackend,
    task_id: uuid::Uuid,
    claim: &WorkerClaim,
    delegation_id: &str,
    delegation_result: &Value,
    outcome: Result<&Value, (&str, bool)>,
) -> Result<bool, EngineError> {
    let Some(task) = storage.get_worker_task(task_id).await? else {
        return Ok(false);
    };
    let held = task.worker_id.as_deref() == Some(claim.worker_id.as_str())
        && task.claim_epoch == claim.claim_epoch;
    if !held {
        return Ok(false);
    }
    let Some(instance) = storage.get_instance(task.instance_id).await? else {
        return Ok(false);
    };
    match outcome {
        Err((message, retryable)) => {
            if task.state != WorkerTaskState::Claimed {
                return Ok(false);
            }
            merge_delegation_result(storage, &instance, delegation_id, delegation_result).await?;
            crate::worker_lease::fail_worker_task(
                storage, &instance, &task, claim, message, retryable,
            )
            .await
        }
        Ok(output) => {
            if task.state == WorkerTaskState::Claimed {
                crate::effect_guard::commit_external_worker_effect(
                    storage,
                    &instance.tenant_id,
                    &task,
                    output,
                )
                .await?;
                if !storage.complete_worker_task(task.id, claim, output).await? {
                    return Ok(false);
                }
            } else if task.state != WorkerTaskState::Completed {
                return Ok(false);
            }
            finish_local_completion(storage, &task, delegation_id, delegation_result, output).await
        }
    }
}

async fn merge_delegation_result(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    delegation_id: &str,
    delegation_result: &Value,
) -> Result<(), EngineError> {
    let mut delegations = instance
        .context
        .data
        .get("delegations")
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some(map) = delegations.as_object_mut() {
        map.insert(delegation_id.to_owned(), delegation_result.clone());
    }
    storage
        .merge_context_data(instance.id, "delegations", &delegations)
        .await?;
    Ok(())
}

/// The transition half of a local completion: runs only while the step is
/// still pending (tree: its node is live; flat: the instance still waits on
/// it), so a repeated call never saves a second output.
async fn finish_local_completion(
    storage: &dyn StorageBackend,
    task: &WorkerTask,
    delegation_id: &str,
    delegation_result: &Value,
    output: &Value,
) -> Result<bool, EngineError> {
    let Some(mut instance) = storage.get_instance(task.instance_id).await? else {
        return Ok(false);
    };
    if instance.state.is_terminal() || instance.state == InstanceState::Paused {
        return Ok(false);
    }
    let tree = storage.get_execution_tree(task.instance_id).await?;
    let node = tree.iter().find(|node| {
        node.block_id == task.block_id
            && matches!(node.state, NodeState::Running | NodeState::Waiting)
    });
    let pending = if tree.is_empty() {
        instance.state == InstanceState::Waiting
            && instance
                .context
                .runtime
                .current_step
                .as_ref()
                .is_none_or(|step| *step == task.block_id)
    } else {
        node.is_some()
    };
    if !pending {
        return Ok(false);
    }
    if !instance.context.data.is_object() {
        instance.context.data = json!({});
    }
    if let Some(data) = instance.context.data.as_object_mut() {
        if let Some(fields) = output.as_object() {
            for (key, value) in fields {
                data.insert(key.clone(), value.clone());
            }
        }
        let delegations = data.entry("delegations").or_insert_with(|| json!({}));
        if !delegations.is_object() {
            *delegations = json!({});
        }
        if let Some(map) = delegations.as_object_mut() {
            map.insert(delegation_id.to_owned(), delegation_result.clone());
        }
    }
    let block_output = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: task.instance_id,
        block_id: task.block_id.clone(),
        output: output.clone(),
        output_ref: None,
        output_size: serde_json::to_vec(output)
            .map_or(0, |bytes| u32::try_from(bytes.len()).unwrap_or(u32::MAX)),
        attempt: task.attempt,
        created_at: Utc::now(),
    };
    let result = match node {
        Some(node) => {
            storage
                .save_output_complete_node_merge_context_and_transition(
                    &block_output,
                    node.id,
                    task.instance_id,
                    &instance.context,
                    InstanceState::Scheduled,
                    Some(Utc::now()),
                )
                .await
        }
        None => {
            storage
                .save_output_merge_context_and_transition(
                    &block_output,
                    task.instance_id,
                    &instance.context,
                    InstanceState::Scheduled,
                    Some(Utc::now()),
                )
                .await
        }
    };
    match result {
        Ok(()) => Ok(true),
        Err(orch8_types::error::StorageError::TerminalTarget { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}
