//! Validated logical dispatch, sealed again when an encryption decorator transforms it.
use crate::worker_invocation::{WorkerDispatchEffect, WorkerDispatchFence, WorkerInvocation};
use orch8_types::{
    continuity::{EffectId, EffectKind, EffectReceipt, EffectState},
    error::StorageError,
    worker::WorkerTask,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerEffectPlan {
    pub receipt: EffectReceipt,
    pub(crate) legacy_id: EffectId,
    pub guarded: bool,
}

impl WorkerEffectPlan {
    pub fn new(receipt: EffectReceipt, guarded: bool) -> Self {
        let legacy_id = legacy_identity(&receipt);
        Self {
            receipt,
            legacy_id,
            guarded,
        }
    }
}

fn legacy_identity(receipt: &EffectReceipt) -> EffectId {
    let mut hash = Sha256::new();
    hash.update(b"orch8-effect-v1\0");
    hash.update(receipt.continuity_id.as_uuid().as_bytes());
    hash.update(receipt.epoch.get().to_be_bytes());
    hash.update(receipt.instance_id.into_uuid().as_bytes());
    hash.update(receipt.block_id.as_str().as_bytes());
    hash.update(receipt.attempt.to_be_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    EffectId::from_uuid(Uuid::from_bytes(bytes))
}

/// Fields are private: callers cannot change the payload after validating its effect.
#[derive(Clone)]
pub struct WorkerDispatchIntent {
    pub(crate) task: WorkerTask,
    pub(crate) fence: WorkerDispatchFence,
    pub(crate) effect: Option<WorkerEffectPlan>,
    pub(crate) initial_owner: Option<orch8_types::continuity::ContinuityExecution>,
    pub(crate) stored_spec_sha256: String,
    logical_spec_sha256: String,
}

impl std::fmt::Debug for WorkerDispatchIntent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerDispatchIntent")
            .field("task_id", &self.task.id)
            .field("fence", &self.fence)
            .field("handler", &self.task.handler_name)
            .field("attempt", &self.task.attempt)
            .field("effect_id", &self.task.effect_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub enum WorkerDispatchPublication {
    Published {
        task_id: Uuid,
        receipt: Option<Box<EffectReceipt>>,
    },
    Stale,
    EffectBlocked(Box<EffectReceipt>),
    Duplicate(Box<EffectReceipt>),
}

/// Immutable admitted scope and hashes; contains no task inputs or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDispatchAdmission {
    pub invocation: WorkerInvocation,
    pub invocation_key: String,
    pub logical_spec_sha256: String,
    pub stored_spec_sha256: String,
    pub effect: Option<WorkerDispatchEffect>,
}

impl WorkerDispatchIntent {
    pub fn new(
        mut task: WorkerTask,
        mut fence: WorkerDispatchFence,
        effect: Option<WorkerEffectPlan>,
    ) -> Result<Self, StorageError> {
        fence.validate(&task)?;
        let initial_owner = if fence.continuity.is_none() {
            let owner = initial_owner(&fence);
            fence.continuity = Some(dispatch_scope(&fence));
            task.continuity_epoch = Some(0);
            Some(owner)
        } else {
            None
        };

        let runtime = task
            .context
            .get("runtime")
            .and_then(Value::as_object)
            .ok_or_else(invalid)?;
        let mode = match runtime.get("dry_run") {
            None => false,
            Some(value) => value.as_bool().ok_or_else(invalid)?,
        };
        if runtime.get("run_id").and_then(Value::as_str) != Some(fence.run_id.as_str())
            || mode != fence.dry_run
            || orch8_types::sequence::is_engine_only_builtin(&task.handler_name)
            || task.claimed_at.is_some()
            || task.heartbeat_at.is_some()
            || task.completed_at.is_some()
            || task.resume_checkpoint.is_some()
            || task.checkpoint_seq != 0
            || task.output.is_some()
            || task.error_message.is_some()
            || task.error_retryable.is_some()
            || task.lease_secs.is_some()
            || task.claimed_runtime_kind.is_some()
        {
            return Err(invalid());
        }
        let pure = orch8_types::sequence::is_pure_remote_builtin(&task.handler_name);
        match (&effect, &fence.continuity) {
            (None, Some(_)) if (pure || mode) && task.effect_id.is_none() => {}
            (Some(plan), Some(scope)) if !pure && !mode => {
                let receipt = &plan.receipt;
                let proof = dispatch_effect(&task, receipt.id);
                if plan.legacy_id != legacy_identity(receipt)
                    || receipt.state != EffectState::Planned
                    || receipt.tenant_id != fence.tenant_id
                    || receipt.instance_id != fence.instance_id
                    || receipt.continuity_id != scope.continuity_id
                    || receipt.epoch.get() != scope.epoch
                    || receipt.block_id != task.block_id
                    || receipt.attempt != u32::from(task.attempt)
                    || receipt.kind != proof.kind
                    || receipt.request_sha256 != proof.request_sha256
                    || receipt.destination_fingerprint != proof.destination_fingerprint
                    || task.effect_id.is_some_and(|id| id != receipt.id)
                {
                    return Err(invalid());
                }
                task.effect_id = Some(receipt.id);
            }
            _ => return Err(invalid()),
        }
        let stored_spec_sha256 = task_spec(&task);
        Ok(Self {
            task,
            fence,
            effect,
            initial_owner,
            logical_spec_sha256: stored_spec_sha256.clone(),
            stored_spec_sha256,
        })
    }

    pub(crate) fn invocation_key(&self) -> String {
        digest(
            &json!({"tenant":self.fence.tenant_id,"instance":self.fence.instance_id,
            "sequence":self.fence.sequence_id,"run":self.fence.run_id,"block":self.fence.block_id,
            "mode":self.fence.dry_run,"owner":self.fence.continuity,
            "target":self.fence.expected_target(),"attempt":self.task.attempt}),
        )
    }

    pub(crate) fn admission(
        &self,
        task_id: Uuid,
        receipt: Option<&EffectReceipt>,
    ) -> WorkerDispatchAdmission {
        let effect_id = receipt.map(|receipt| receipt.id);
        WorkerDispatchAdmission {
            invocation: WorkerInvocation {
                task_id,
                tenant_id: self.fence.tenant_id.clone(),
                instance_id: self.fence.instance_id,
                sequence_id: self.fence.sequence_id,
                block_id: self.fence.block_id.clone(),
                handler_name: self.task.handler_name.clone(),
                attempt: self.task.attempt,
                continuity_epoch: self.fence.continuity.as_ref().map(|scope| scope.epoch),
                continuity: self.fence.continuity.clone(),
                effect_id,
                run_id: Some(self.fence.run_id.clone()),
                dry_run: self.fence.dry_run,
                target: self.fence.expected_target(),
            },
            invocation_key: self.invocation_key(),
            logical_spec_sha256: self.logical_spec_sha256.clone(),
            stored_spec_sha256: self.stored_spec_sha256.clone(),
            effect: receipt.map(|receipt| WorkerDispatchEffect {
                effect_id: receipt.id,
                kind: receipt.kind,
                request_sha256: receipt.request_sha256.clone(),
                destination_fingerprint: receipt.destination_fingerprint.clone(),
            }),
        }
    }

    /// Only storage decorators can reseal a transformed payload. Logical effect
    /// evidence stays untouched and is never reconstructed from ciphertext.
    pub(crate) fn encrypted(&self, task: WorkerTask) -> Result<Self, StorageError> {
        self.validate_stored()?;
        if task.id != self.task.id
            || task.instance_id != self.task.instance_id
            || task.block_id != self.task.block_id
            || task.handler_name != self.task.handler_name
            || task.attempt != self.task.attempt
            || task.effect_id != self.task.effect_id
            || task.continuity_epoch != self.task.continuity_epoch
        {
            return Err(invalid());
        }
        Ok(Self {
            fence: self.fence.clone(),
            effect: self.effect.clone(),
            initial_owner: self.initial_owner.clone(),
            stored_spec_sha256: task_spec(&task),
            logical_spec_sha256: self.logical_spec_sha256.clone(),
            task,
        })
    }

    pub(crate) fn validate_stored(&self) -> Result<(), StorageError> {
        self.fence.validate(&self.task)?;
        if task_spec(&self.task) != self.stored_spec_sha256 {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Only the initial deterministic namespace can augment a captured unowned source.
/// The database must still prove the original claim/generation and absence of relocation.
pub fn dispatch_scope(
    fence: &WorkerDispatchFence,
) -> crate::worker_invocation::WorkerInvocationContinuity {
    fence.continuity.clone().unwrap_or_else(|| {
        let owner = initial_owner(fence);
        crate::worker_invocation::WorkerInvocationContinuity {
            continuity_id: owner.continuity_id,
            epoch: 0,
            owner_runtime_id: owner.owner_runtime_id,
        }
    })
}

fn initial_owner(fence: &WorkerDispatchFence) -> orch8_types::continuity::ContinuityExecution {
    use orch8_types::continuity::{
        ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId,
    };
    fn id(domain: &[u8], instance: orch8_types::ids::InstanceId) -> Uuid {
        let mut hash = Sha256::new();
        hash.update(domain);
        hash.update(instance.into_uuid().as_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hash.finalize()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }
    ContinuityExecution {
        continuity_id: ContinuityId::from_uuid(id(b"orch8-effect-scope-v1\0", fence.instance_id)),
        tenant_id: fence.tenant_id.clone(),
        current_instance_id: fence.instance_id,
        owner_runtime_id: RuntimeId::from_uuid(id(b"orch8-effect-runtime-v1\0", fence.instance_id)),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: chrono::Utc::now(),
    }
}

fn invalid() -> StorageError {
    StorageError::Constraint(
        "worker dispatch intent does not match its invocation or payload".into(),
    )
}

pub(crate) fn task_spec(task: &WorkerTask) -> String {
    digest(&json!({
        "handler":task.handler_name,"queue":task.queue_name,"requirements":task.requirements,
        "params":task.params,"context":task.context,"attempt":task.attempt,
        "timeout_ms":task.timeout_ms,"carries_credentials":task.carries_credentials,
    }))
}

pub(crate) fn digest(value: &Value) -> String {
    let canonical = orch8_types::ai::canonical_json(value);
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(canonical.as_bytes()) {
        write!(&mut result, "{byte:02x}").expect("writing to String cannot fail");
    }
    result
}

/// Hash the logical task before encryption, using the same sorted JSON as native effects.
pub fn dispatch_effect(task: &WorkerTask, effect_id: EffectId) -> WorkerDispatchEffect {
    let destination: serde_json::Map<String, Value> = [
        "url", "provider", "model", "topic", "queue", "resource", "endpoint",
    ]
    .iter()
    .filter_map(|key| {
        task.params
            .get(*key)
            .map(|value| ((*key).to_owned(), value.clone()))
    })
    .collect();
    let kind = match task.handler_name.as_str() {
        "http_request" => EffectKind::Http,
        "llm_call" | "agent" | "embed" => EffectKind::Model,
        "emit_event" | "send_signal" | "email" | "notify" => EffectKind::Message,
        "memory_store" | "memory_delete" | "blob_put" => EffectKind::Storage,
        handler if !orch8_types::sequence::BUILTIN_HANDLER_NAMES.contains(&handler) => {
            EffectKind::Worker
        }
        _ => EffectKind::Custom,
    };
    WorkerDispatchEffect {
        effect_id,
        kind,
        request_sha256: digest(&json!({"handler":task.handler_name,"params":task.params})),
        destination_fingerprint: digest(
            &json!({"handler":task.handler_name,"destination":destination}),
        ),
    }
}
