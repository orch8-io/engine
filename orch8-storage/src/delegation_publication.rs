//! Durable grant-authorized mailbox publication, distinct from native step admission.
use crate::worker_claim::WorkerClaimPrincipal;
use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{
        ContinuationGrant, ContinuityExecution, EffectReceipt, EffectState, GrantAction,
        OwnershipState, ProvenanceEntry, RuntimeCapabilities, RuntimeTrustLevel,
    },
    continuity_advanced::DeviceDelegation,
    error::StorageError,
    instance::{InstanceState, TaskInstance},
    sequence::SequenceDefinition,
    worker::{WorkerTask, WorkerTaskState},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub(crate) const INVOCATION_REASON: &str = "grant-authorized delegation mailbox";

pub(crate) fn conflict() -> StorageError {
    StorageError::Conflict("delegation publication scope changed or grant is unavailable".into())
}
fn digest<T: Serialize>(value: &T) -> Result<String, StorageError> {
    Ok(crate::worker_admission::digest(&serde_json::to_value(
        value,
    )?))
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegatedStep {
    pub handler: String,
    pub block_id: String,
}
/// Server-verified actor and decoded-token verifier; never deserialized from wire JSON.
#[derive(Clone)]
pub struct DelegationRequest {
    pub delegation: DeviceDelegation,
    pub grant: ContinuationGrant,
    pub nonce_sha256: String,
    pub input: Value,
    pub step: Option<DelegatedStep>,
    pub actor: WorkerClaimPrincipal,
    pub authority_expires_at: Option<DateTime<Utc>>,
}
impl DelegationRequest {
    pub fn validate(&self) -> Result<(), StorageError> {
        self.actor.validate()?;
        let d = &self.delegation;
        if self
            .actor
            .tenant_id()
            .is_some_and(|tenant| tenant != &d.tenant_id)
            || matches!(self.actor, WorkerClaimPrincipal::Legacy {})
            || self.grant.id != d.grant_id
            || self.grant.tenant_id != d.tenant_id
            || self.grant.continuity_id != d.parent_continuity_id
            || self.grant.expected_epoch != d.parent_epoch
            || self.grant.destination_runtime_id != d.destination_runtime_id
            || self.nonce_sha256 != self.grant.nonce_sha256
            || self.nonce_sha256.len() != 64
            || !self
                .nonce_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(conflict());
        }
        if matches!(self.actor, WorkerClaimPrincipal::Workload { .. })
            && self.authority_expires_at.is_none()
        {
            return Err(conflict());
        }
        if let WorkerClaimPrincipal::Runtime { runtime_id, .. } = &self.actor
            && (*runtime_id != d.source_runtime_id || self.authority_expires_at.is_none())
        {
            return Err(conflict());
        }
        if let Some(step) = &self.step
            && [&step.handler, &step.block_id]
                .iter()
                .any(|name| name.trim().is_empty() || name.len() > 256 || name.contains('\0'))
        {
            return Err(StorageError::Constraint(
                "delegated step names must be nonempty, NUL-free and at most 256 bytes".into(),
            ));
        }
        Ok(())
    }
    /// Session renewal does not change the stable actor or logical request.
    pub fn request_sha256(&self) -> Result<String, StorageError> {
        self.validate()?;
        digest(
            &json!({"version":1,"delegation":self.delegation,"grant":self.grant,
            "nonce_sha256":self.nonce_sha256,"input":self.input,"step":self.step,"actor":self.actor}),
        )
    }
    pub(crate) fn live(
        &self,
        grant: &ContinuationGrant,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        self.validate()?;
        let d = &self.delegation;
        if grant != &self.grant
            || d.expires_at <= now
            || self
                .authority_expires_at
                .is_some_and(|expiry| expiry <= now)
        {
            return Err(conflict());
        }
        grant
            .validate_claim(
                now,
                &d.tenant_id,
                d.parent_continuity_id,
                d.parent_epoch,
                d.destination_runtime_id,
                GrantAction::Accept,
            )
            .map_err(|_| conflict())
    }
}
/// Permanent metadata only: no token, input, context or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationPublication {
    pub version: u32,
    pub delegation: DeviceDelegation,
    pub request_sha256: String,
    pub actor: WorkerClaimPrincipal,
    pub task_id: Uuid,
    pub instance_id: orch8_types::ids::InstanceId,
    pub effect_id: orch8_types::continuity::EffectId,
    pub sequence_sha256: String,
    pub logical_spec_sha256: String,
    pub stored_spec_sha256: String,
    pub task_owner: crate::worker_invocation::WorkerInvocationContinuity,
    pub published_at: DateTime<Utc>,
}
impl DelegationPublication {
    pub fn validate(&self) -> Result<(), StorageError> {
        self.actor.validate()?;
        if self.version != 1
            || self.task_id.is_nil()
            || self.request_sha256.len() != 64
            || self.sequence_sha256.len() != 64
            || self.logical_spec_sha256.len() != 64
            || self.stored_spec_sha256.len() != 64
        {
            return Err(StorageError::Constraint(
                "unsupported or corrupt delegation publication receipt".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn matches_hot(&self, task: &WorkerTask) -> Result<bool, StorageError> {
        self.validate()?;
        Ok(self.task_id == task.id
            && self.instance_id == task.instance_id
            && self.effect_id == task.effect_id.ok_or_else(conflict)?
            && task.continuity_epoch == Some(self.task_owner.epoch)
            && task.block_id.as_str() == format!("delegation-{}", self.delegation.id)
            && self.stored_spec_sha256 == crate::worker_admission::task_spec(task))
    }
    pub fn matches(&self, request: &DelegationRequest) -> Result<bool, StorageError> {
        self.validate()?;
        Ok(self.delegation == request.delegation
            && self.actor == request.actor
            && self.request_sha256 == request.request_sha256()?)
    }
}
/// Preflight snapshots; the backend revalidates them before exposing any work.
pub struct DelegationPlan {
    pub request: DelegationRequest,
    pub parent_owner: ContinuityExecution,
    pub task_owner: ContinuityExecution,
    pub parent: TaskInstance,
    pub proxy: bool,
    pub sequence: SequenceDefinition,
    pub task: WorkerTask,
    pub effect: EffectReceipt,
    pub entry: ProvenanceEntry,
}
#[derive(Clone)]
pub struct DelegationPublishIntent {
    pub(crate) request: DelegationRequest,
    pub(crate) parent_owner: ContinuityExecution,
    pub(crate) task_owner: ContinuityExecution,
    pub(crate) parent: TaskInstance,
    pub(crate) proxy: bool,
    pub(crate) sequence: SequenceDefinition,
    pub(crate) task: WorkerTask,
    pub(crate) effect: crate::worker_admission::WorkerEffectPlan,
    pub(crate) entry: ProvenanceEntry,
    pub(crate) receipt: DelegationPublication,
    stored_task_sha256: String,
}
pub fn sequence_sha256(sequence: &SequenceDefinition) -> Result<String, StorageError> {
    let mut value = serde_json::to_value(sequence)?;
    value
        .as_object_mut()
        .ok_or_else(conflict)?
        .remove("created_at");
    digest(&value)
}
impl DelegationPublishIntent {
    pub fn new(plan: DelegationPlan) -> Result<Self, StorageError> {
        plan.request.validate()?;
        let d = &plan.request.delegation;
        let owner = &plan.parent_owner;
        let task = &plan.task;
        let effect = &plan.effect;
        let proof = crate::worker_admission::dispatch_effect(task, effect.id);
        let expected_params = mailbox_params(d, &plan.sequence, &plan.request.input);
        if owner.tenant_id != d.tenant_id
            || owner.continuity_id != d.parent_continuity_id
            || owner.epoch != d.parent_epoch
            || owner.owner_runtime_id != d.source_runtime_id
            || owner.state != OwnershipState::Owned
            || plan.parent.tenant_id != d.tenant_id
            || plan.task_owner.tenant_id != d.tenant_id
            || plan.task_owner.current_instance_id != plan.parent.id
            || plan.task_owner.state != OwnershipState::Owned
            || plan.sequence.id != d.sub_sequence_id
            || plan.sequence.tenant_id != d.tenant_id
            || task.instance_id != plan.parent.id
            || task.handler_name != "orch8.delegation"
            || task.params != expected_params
            || task.block_id.as_str() != format!("delegation-{}", d.id)
            || task.context != json!({})
            || task.requirements.runtime_id != Some(d.destination_runtime_id)
            || task.state != WorkerTaskState::Pending
            || task.effect_id != Some(effect.id)
            || task.continuity_epoch != Some(plan.task_owner.epoch.get())
            || task.claim_epoch != 0
            || task.attempt != 0
            || task.worker_id.is_some()
            || task.output.is_some()
            || effect.state != EffectState::Planned
            || effect.tenant_id != d.tenant_id
            || effect.instance_id != plan.parent.id
            || effect.continuity_id != plan.task_owner.continuity_id
            || effect.epoch != plan.task_owner.epoch
            || effect.block_id != task.block_id
            || effect.attempt != 0
            || effect.request_sha256 != proof.request_sha256
            || effect.destination_fingerprint != proof.destination_fingerprint
            || effect.kind != proof.kind
            || plan.entry.tenant_id != d.tenant_id
            || plan.entry.continuity_id != d.parent_continuity_id
            || plan.entry.epoch != d.parent_epoch
            || plan.entry.kind != "device_delegation"
            || plan.entry.payload_sha256 != plan.request.request_sha256()?
        {
            return Err(conflict());
        }
        validate_parent(&plan)?;
        let receipt = DelegationPublication {
            version: 1,
            delegation: d.clone(),
            request_sha256: plan.request.request_sha256()?,
            actor: plan.request.actor.clone(),
            task_id: task.id,
            instance_id: task.instance_id,
            effect_id: effect.id,
            sequence_sha256: sequence_sha256(&plan.sequence)?,
            logical_spec_sha256: crate::worker_admission::task_spec(task),
            stored_spec_sha256: crate::worker_admission::task_spec(task),
            task_owner: crate::worker_invocation::WorkerInvocationContinuity {
                continuity_id: plan.task_owner.continuity_id,
                epoch: plan.task_owner.epoch.get(),
                owner_runtime_id: plan.task_owner.owner_runtime_id,
            },
            published_at: task.created_at,
        };
        let stored_task_sha256 = digest(task)?;
        Ok(Self {
            request: plan.request,
            parent_owner: plan.parent_owner,
            task_owner: plan.task_owner,
            parent: plan.parent,
            proxy: plan.proxy,
            sequence: plan.sequence,
            task: plan.task,
            effect: crate::worker_admission::WorkerEffectPlan::new(plan.effect, false),
            entry: plan.entry,
            receipt,
            stored_task_sha256,
        })
    }
    pub(crate) fn validate_stored(&self) -> Result<(), StorageError> {
        if digest(&self.task)? != self.stored_task_sha256
            || self.receipt.stored_spec_sha256 != crate::worker_admission::task_spec(&self.task)
            || !self.receipt.matches(&self.request)?
        {
            return Err(conflict());
        }
        Ok(())
    }
    pub(crate) fn sealed(
        mut self,
        task: WorkerTask,
        parent: TaskInstance,
    ) -> Result<Self, StorageError> {
        let mut expected_task = self.task.clone();
        expected_task.params = task.params.clone();
        expected_task.context = task.context.clone();
        let mut expected_parent = self.parent.clone();
        expected_parent.context = parent.context.clone();
        if digest(&task)? != digest(&expected_task)?
            || digest(&parent)? != digest(&expected_parent)?
        {
            return Err(conflict());
        }
        self.receipt.stored_spec_sha256 = crate::worker_admission::task_spec(&task);
        self.task = task;
        self.parent = parent;
        self.stored_task_sha256 = digest(&self.task)?;
        Ok(self)
    }
    pub fn with_entry(mut self, entry: ProvenanceEntry) -> Result<Self, StorageError> {
        if entry.tenant_id != self.entry.tenant_id
            || entry.continuity_id != self.entry.continuity_id
            || entry.epoch != self.entry.epoch
            || entry.kind != self.entry.kind
            || entry.payload_sha256 != self.entry.payload_sha256
        {
            return Err(conflict());
        }
        self.entry = entry;
        Ok(self)
    }
    pub(crate) fn owner_matches(&self, actual: &ContinuityExecution) -> bool {
        actual.tenant_id == self.parent_owner.tenant_id
            && actual.continuity_id == self.parent_owner.continuity_id
            && actual.current_instance_id == self.parent_owner.current_instance_id
            && actual.owner_runtime_id == self.parent_owner.owner_runtime_id
            && actual.epoch == self.parent_owner.epoch
            && actual.state == OwnershipState::Owned
    }
    pub(crate) fn runtimes(
        &self,
        source: &RuntimeCapabilities,
        dest: &RuntimeCapabilities,
        now: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let d = &self.request.delegation;
        if source.runtime_id != d.source_runtime_id
            || dest.runtime_id != d.destination_runtime_id
            || source.expires_at <= now
            || dest.expires_at <= now
            || source.trust < RuntimeTrustLevel::Registered
            || dest.trust < RuntimeTrustLevel::Registered
            || self
                .sequence
                .handler_names()
                .iter()
                .any(|handler| !dest.handlers.contains(handler))
        {
            return Err(conflict());
        }
        Ok(())
    }
    pub(crate) fn native_live(&self, parent: &TaskInstance) -> Result<(), StorageError> {
        if parent.tenant_id != self.parent.tenant_id
            || parent.id != self.parent.id
            || parent.sequence_id != self.parent.sequence_id
            || parent.namespace != self.parent.namespace
            || parent.context.runtime.dry_run
            || !matches!(
                parent.state,
                InstanceState::Waiting | InstanceState::Scheduled
            )
        {
            return Err(conflict());
        }
        Ok(())
    }
}
pub fn mailbox_params(d: &DeviceDelegation, sequence: &SequenceDefinition, input: &Value) -> Value {
    json!({"delegation_id":d.id,"grant_id":d.grant_id,"parent_continuity_id":d.parent_continuity_id,
        "parent_epoch":d.parent_epoch,"source_runtime_id":d.source_runtime_id,"sub_sequence_id":sequence.id,
        "sub_sequence_name":sequence.name,"sub_sequence_version":sequence.version,"expires_at":d.expires_at,"input":input})
}
#[derive(Debug)]
pub enum DelegationPublishResult {
    Published(Box<DelegationPublication>),
    ProvenanceChanged,
}

pub fn proxy_owner(
    tenant: &orch8_types::ids::TenantId,
    id: orch8_types::ids::InstanceId,
    now: DateTime<Utc>,
) -> ContinuityExecution {
    use sha2::{Digest, Sha256};
    let identity = |domain: &[u8]| {
        let mut hash = Sha256::new();
        hash.update(domain);
        hash.update(id.into_uuid().as_bytes());
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash.finalize()[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    };
    ContinuityExecution {
        tenant_id: tenant.clone(),
        current_instance_id: id,
        continuity_id: orch8_types::continuity::ContinuityId::from_uuid(identity(
            b"orch8-effect-scope-v1\0",
        )),
        owner_runtime_id: orch8_types::continuity::RuntimeId::from_uuid(identity(
            b"orch8-effect-runtime-v1\0",
        )),
        epoch: orch8_types::continuity::ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    }
}
pub(crate) fn same_owner(a: &ContinuityExecution, b: &ContinuityExecution) -> bool {
    a.tenant_id == b.tenant_id
        && a.continuity_id == b.continuity_id
        && a.current_instance_id == b.current_instance_id
        && a.owner_runtime_id == b.owner_runtime_id
        && a.epoch == b.epoch
        && a.state == b.state
}

/// Pure candidate construction; publication performs all durable writes.
pub fn proxy_instance(
    owner: &ContinuityExecution,
    d: &DeviceDelegation,
    sequence: &SequenceDefinition,
    sub_tenant: Option<String>,
    now: DateTime<Utc>,
) -> TaskInstance {
    TaskInstance {
        claimed_at: None,
        id: orch8_types::ids::InstanceId::from_uuid(d.id.into_uuid()),
        sub_tenant,
        sequence_id: sequence.id,
        tenant_id: d.tenant_id.clone(),
        namespace: sequence.namespace.clone(),
        state: InstanceState::Waiting,
        next_fire_at: None,
        priority: orch8_types::instance::Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({"orch8_delegation_proxy":{"delegation":d,"parent_instance_id":owner.current_instance_id}}),
        context: orch8_types::context::ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}
pub fn mailbox_task(
    parent: &TaskInstance,
    d: &DeviceDelegation,
    sequence: &SequenceDefinition,
    input: &Value,
    effect: orch8_types::continuity::EffectId,
    epoch: orch8_types::continuity::ExecutionEpoch,
    now: DateTime<Utc>,
) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: parent.id,
        block_id: orch8_types::ids::BlockId::new(format!("delegation-{}", d.id)),
        handler_name: "orch8.delegation".into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements {
            runtime_id: Some(d.destination_runtime_id),
            ..Default::default()
        },
        params: mailbox_params(d, sequence, input),
        context: json!({}),
        attempt: 0,
        timeout_ms: Some((d.expires_at - now).num_milliseconds().max(1)),
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
        effect_id: Some(effect),
        continuity_epoch: Some(epoch.get()),
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

fn validate_parent(plan: &DelegationPlan) -> Result<(), StorageError> {
    let d = &plan.request.delegation;
    let owner = &plan.parent_owner;
    if plan.proxy {
        if plan.parent.id.into_uuid() != d.id.into_uuid()
            || plan.parent.sequence_id != plan.sequence.id
            || serde_json::from_value::<DeviceDelegation>(
                plan.parent.metadata["orch8_delegation_proxy"]["delegation"].clone(),
            )
            .ok()
            .as_ref()
                != Some(d)
            || plan.parent.metadata["orch8_delegation_proxy"]["parent_instance_id"]
                != json!(owner.current_instance_id)
            || plan.task_owner.epoch.get() != 0
            || !same_owner(
                &plan.task_owner,
                &proxy_owner(&d.tenant_id, plan.parent.id, plan.task_owner.updated_at),
            )
        {
            return Err(conflict());
        }
    } else if plan.parent.id != owner.current_instance_id || plan.task_owner != *owner {
        return Err(conflict());
    }

    Ok(())
}

/// Only a durable publication can authorize application to a synthetic mailbox block.
pub(crate) fn mailbox_id(
    invocation: &crate::worker_invocation::WorkerInvocation,
) -> Result<Option<Uuid>, StorageError> {
    if !invocation.is_published_mailbox() {
        return Ok(None);
    }
    invocation
        .block_id
        .as_str()
        .strip_prefix("delegation-")
        .and_then(|id| Uuid::parse_str(id).ok())
        .map(Some)
        .ok_or_else(conflict)
}
impl DelegationPublication {
    pub(crate) fn matches_outcome(
        &self,
        accepted: &crate::worker_outcome::WorkerAcceptedOutcome,
    ) -> Result<bool, StorageError> {
        self.validate()?;
        let Some(invocation) = &accepted.invocation else {
            return Ok(false);
        };
        Ok(
            mailbox_id(invocation)? == Some(self.delegation.id.into_uuid())
                && self.task_id == accepted.task_id
                && self.instance_id == accepted.instance_id
                && self.delegation.tenant_id == accepted.tenant_id
                && invocation.task_id == self.task_id
                && invocation.instance_id == self.instance_id
                && invocation.tenant_id == self.delegation.tenant_id
                && invocation.handler_name == "orch8.delegation"
                && invocation.attempt == 0
                && invocation.effect_id == Some(self.effect_id)
                && invocation.continuity.as_ref() == Some(&self.task_owner)
                && invocation.continuity_epoch == Some(self.task_owner.epoch)
                && accepted.claim.worker_id == self.delegation.destination_runtime_id.to_string(),
        )
    }
    pub(crate) fn is_proxy(&self, instance: &TaskInstance) -> Result<bool, StorageError> {
        let expected_owner =
            proxy_owner(&self.delegation.tenant_id, instance.id, self.published_at);
        let is_proxy = self.task_owner.continuity_id == expected_owner.continuity_id
            && self.task_owner.epoch == expected_owner.epoch.get()
            && self.task_owner.owner_runtime_id == expected_owner.owner_runtime_id;
        match (is_proxy, instance.metadata.get("orch8_delegation_proxy")) {
            (true, Some(proxy)) => {
                let delegation: DeviceDelegation =
                    serde_json::from_value(proxy["delegation"].clone())?;
                if delegation != self.delegation
                    || instance.id.into_uuid() != self.delegation.id.into_uuid()
                {
                    return Err(conflict());
                }
                Ok(true)
            }
            (false, None) => Ok(false),
            _ => Err(conflict()),
        }
    }
}
