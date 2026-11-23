//! Atomic logical instance imports, independent of archive representation.
use crate::externalizing::ExternalizedEntry;
use crate::{
    worker_admission::WorkerDispatchAdmission,
    worker_invocation::{WorkerInvocation, WorkerInvocationTarget},
};
use orch8_types::{
    continuity::{EffectReceipt, EffectState},
    error::StorageError,
    execution::{ExecutionNode, NodeState},
    instance::TaskInstance,
    output::BlockOutput,
    worker::{WorkerTask, WorkerTaskState},
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

/// One instance and all of its archived dependent rows.
#[derive(Debug, Clone)]
pub struct InstanceSnapshot {
    pub instance: TaskInstance,
    pub nodes: Vec<ExecutionNode>,
    pub outputs: Vec<BlockOutput>,
    pub payloads: Vec<ExternalizedEntry>,
}

/// Runtime state that is restored atomically with an instance snapshot.
#[derive(Debug, Clone, Default)]
pub struct InstanceHistory {
    pub checkpoints: Vec<orch8_types::checkpoint::Checkpoint>,
    pub signals: Vec<orch8_types::signal::Signal>,
    pub state: Vec<InstanceKvEntry>,
    pub effects: Option<EffectArchive>,
    pub worker_tasks: Vec<WorkerTaskArchive>,
    pub read_only_migration: Option<uuid::Uuid>,
}

impl InstanceHistory {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
            && self.signals.is_empty()
            && self.state.is_empty()
            && self.effects.is_none()
            && self.worker_tasks.is_empty()
            && self.read_only_migration.is_none()
    }

    pub fn validate(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        if self.read_only_migration.is_some() && !instance.state.is_terminal() {
            return Err(StorageError::Constraint(
                "read-only history requires a terminal instance".into(),
            ));
        }
        let instance_id = instance.id;
        let mut ids = HashSet::new();
        if self
            .checkpoints
            .iter()
            .any(|cp| cp.instance_id != instance_id || !ids.insert(cp.id))
        {
            return Err(StorageError::Constraint(
                "snapshot contains duplicate checkpoints or checkpoints owned by another instance"
                    .into(),
            ));
        }
        ids.clear();
        if self
            .signals
            .iter()
            .any(|signal| signal.instance_id != instance_id || !ids.insert(signal.id))
        {
            return Err(StorageError::Constraint(
                "snapshot contains duplicate signals or signals owned by another instance".into(),
            ));
        }
        let mut keys = HashSet::new();
        if self
            .state
            .iter()
            .any(|entry| entry.instance_id != instance_id || !keys.insert(&entry.key))
        {
            return Err(StorageError::Constraint(
                "snapshot contains duplicate state keys or state owned by another instance".into(),
            ));
        }
        if let Some(effects) = &self.effects {
            validate_effect_archive(effects, instance)?;
        }
        if self.worker_tasks.is_empty() {
            return Ok(());
        }
        let mut blocks = HashSet::new();
        ids.clear();
        let receipts: HashMap<_, _> = self
            .effects
            .iter()
            .flat_map(|ledger| &ledger.state.receipts)
            .map(|receipt| (receipt.id, receipt))
            .collect();
        for record in &self.worker_tasks {
            let receipt = record
                .task
                .effect_id
                .and_then(|id| receipts.get(&id).copied());
            record.validate(instance, self.effects.as_ref(), receipt)?;
            if !ids.insert(record.task.id) || !blocks.insert(&record.task.block_id) {
                return Err(invalid_worker_archive("duplicate worker task or block"));
            }
        }
        Ok(())
    }

    /// Validate runtime rows against the native instance and its execution tree.
    pub fn validate_snapshot(&self, snapshot: &InstanceSnapshot) -> Result<(), StorageError> {
        self.validate(&snapshot.instance)?;
        self.validate_worker_nodes(&snapshot.nodes)
    }

    fn validate_worker_nodes(&self, nodes: &[ExecutionNode]) -> Result<(), StorageError> {
        if self.worker_tasks.is_empty() {
            return Ok(());
        }
        let nodes_by_id: HashMap<_, _> = nodes.iter().map(|node| (node.id, node)).collect();
        for record in &self.worker_tasks {
            match record.invocation.as_ref().map(|source| &source.target) {
                Some(WorkerInvocationTarget::Tree { node_id, .. })
                    if nodes_by_id.get(node_id).is_none_or(|node| {
                        node.block_id != record.task.block_id
                            || !matches!(
                                node.state,
                                NodeState::Pending | NodeState::Running | NodeState::Waiting
                            )
                    }) =>
                {
                    return Err(invalid_worker_archive(
                        "worker invocation has no live execution node",
                    ));
                }
                Some(WorkerInvocationTarget::Flat { .. }) if !nodes.is_empty() => {
                    return Err(invalid_worker_archive(
                        "flat worker invocation has an execution tree",
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A hot task and its immutable dispatch evidence at the backup's source cut.
/// Issued claims, worker principals and attempt audit events are never imported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerTaskArchive {
    pub task: WorkerTask,
    pub awaiting_dispatch: bool,
    pub invocation: Option<WorkerInvocation>,
    pub admission: Option<WorkerDispatchAdmission>,
}

fn invalid_worker_archive(reason: &str) -> StorageError {
    StorageError::Constraint(format!("invalid worker task archive: {reason}"))
}

impl WorkerTaskArchive {
    /// Verify a task against its source instance and effect evidence before archiving.
    pub fn validate(
        &self,
        instance: &TaskInstance,
        effects: Option<&EffectArchive>,
        receipt: Option<&EffectReceipt>,
    ) -> Result<(), StorageError> {
        let task = &self.task;
        if task.instance_id != instance.id
            || instance.state.is_terminal()
            || !matches!(
                task.state,
                WorkerTaskState::Pending | WorkerTaskState::Claimed
            )
            || (self.awaiting_dispatch && task.state != WorkerTaskState::Pending)
            || task.handler_name == "orch8.delegation"
            || task.claim_epoch >= i64::MAX as u64
            || task.checkpoint_seq > i64::MAX as u64
            || (task.checkpoint_seq == 0 && task.resume_checkpoint.is_some())
        {
            return Err(invalid_worker_archive(
                "task is outside its instance or queue scope",
            ));
        }
        if let Some(source) = &self.invocation {
            source.validate_scope(&instance.tenant_id, task.id, instance.id)?;
            if source.sequence_id != instance.sequence_id
                || source.block_id != task.block_id
                || source.handler_name != task.handler_name
                || source.attempt != task.attempt
                || source.is_published_mailbox()
                || (!matches!(source.target, WorkerInvocationTarget::Unbound { .. })
                    && !source.matches_native_scope(instance, effects.map(|ledger| &ledger.owner)))
                || matches!(source.target, WorkerInvocationTarget::Flat { step_started_at }
                    if instance.context.runtime.current_step.as_ref() != Some(&task.block_id)
                        || instance.context.runtime.current_step_started_at != Some(step_started_at))
            {
                return Err(invalid_worker_archive("invocation does not match the task"));
            }
        }
        let binding = effects.and_then(|ledger| {
            ledger.state.active_attempts.iter().find(|binding| {
                binding.block_id == task.block_id && binding.attempt == u32::from(task.attempt)
            })
        });
        if !self.awaiting_dispatch && task.effect_id != binding.map(|binding| binding.effect_id) {
            return Err(invalid_worker_archive(
                "active task effect binding is missing or mismatched",
            ));
        }
        if let Some(effect_id) = task.effect_id {
            if self.invocation.as_ref().is_none_or(|source| {
                source.continuity.as_ref().map(|scope| scope.epoch) != task.continuity_epoch
            }) || task.continuity_epoch.is_none()
            {
                return Err(invalid_worker_archive(
                    "effectful task has no invocation ownership binding",
                ));
            }
            let Some(receipt) = receipt.filter(|receipt| {
                receipt.id == effect_id
                    && matches!(
                        receipt.state,
                        EffectState::Dispatched | EffectState::Unknown
                    )
            }) else {
                return Err(invalid_worker_archive(
                    "active task effect binding is missing or settled",
                ));
            };
            if (task.state == WorkerTaskState::Claimed || receipt.state == EffectState::Unknown)
                && task.checkpoint_seq == 0
            {
                return Err(invalid_worker_archive(
                    "an in-flight effect has no resumable checkpoint; settle it on the source before backup",
                ));
            }
        }
        if let Some(admission) = &self.admission {
            let proof_matches = match (&admission.effect, receipt) {
                (None, None) => true,
                (Some(proof), Some(receipt)) => {
                    proof.kind == receipt.kind
                        && proof.effect_id == receipt.id
                        && proof.request_sha256 == receipt.request_sha256
                        && proof.destination_fingerprint == receipt.destination_fingerprint
                }
                _ => false,
            };
            if self.awaiting_dispatch
                || self
                    .invocation
                    .as_ref()
                    .is_none_or(|source| !source.same_invocation_scope(&admission.invocation))
                || admission.invocation.handler_name != task.handler_name
                || admission.invocation.effect_id != task.effect_id
                || admission.invocation.continuity_epoch != task.continuity_epoch
                || admission.stored_spec_sha256 != crate::worker_admission::task_spec(task)
                || !proof_matches
            {
                return Err(invalid_worker_archive("admission does not match the task"));
            }
        }
        Ok(())
    }

    /// A source may have granted more leases after the backup's cut. A fresh
    /// task ID fences those leases too, and preserves the attempt's resume budget.
    pub(crate) fn requeue(&self) -> Self {
        let mut record = self.clone();
        let task = &mut record.task;
        task.id = Uuid::new_v4();
        task.state = WorkerTaskState::Pending;
        task.worker_id = None;
        task.claimed_at = None;
        task.heartbeat_at = None;
        task.lease_secs = None;
        task.claimed_runtime_kind = None;
        // serde decodes a persisted JSON null checkpoint as None. The sequence
        // records that the checkpoint was written even in this case.
        if task.checkpoint_seq > 0 && task.resume_checkpoint.is_none() {
            task.resume_checkpoint = Some(serde_json::Value::Null);
        }
        if let Some(source) = &mut record.invocation {
            source.task_id = task.id;
        }
        if let Some(admission) = &mut record.admission {
            admission.invocation.task_id = task.id;
        }
        record
    }
}

pub use orch8_types::migration::EffectArchive;

fn validate_effect_archive(
    effects: &EffectArchive,
    instance: &TaskInstance,
) -> Result<(), StorageError> {
    use orch8_types::continuity::OwnershipState;

    if effects.owner.current_instance_id != instance.id
        || effects.owner.tenant_id != instance.tenant_id
        || !matches!(
            effects.owner.state,
            OwnershipState::Owned | OwnershipState::Completed
        )
    {
        return Err(StorageError::Constraint(
            "invalid archived effect ledger owner".into(),
        ));
    }
    crate::portable_effects::validate_effect_evidence(
        &effects.owner,
        &effects.state,
        instance.sequence_id,
        effects.sequence_version,
    )
}

pub use orch8_types::migration::InstanceKvEntry;

impl InstanceSnapshot {
    /// Check ownership, duplicate identities and tree structure before import.
    pub fn validate(&self) -> Result<(), StorageError> {
        self.checked_nodes().map(|_| ())
    }

    /// Validate ownership and uniqueness, then order parents before children.
    pub(crate) fn checked_nodes(&self) -> Result<Vec<&ExecutionNode>, StorageError> {
        let id = self.instance.id;
        if self.nodes.iter().any(|node| node.instance_id != id)
            || self.outputs.iter().any(|output| output.instance_id != id)
            || self.payloads.iter().any(|entry| entry.instance_id != id)
        {
            return Err(StorageError::Query(
                "snapshot contains rows owned by another instance".into(),
            ));
        }
        let index: HashMap<_, _> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.id, i))
            .collect();
        let output_ids: HashSet<_> = self.outputs.iter().map(|output| output.id).collect();
        let references: HashSet<_> = self.payloads.iter().map(|entry| &entry.ref_key).collect();
        if index.len() != self.nodes.len()
            || output_ids.len() != self.outputs.len()
            || references.len() != self.payloads.len()
        {
            return Err(StorageError::Query(
                "snapshot contains duplicate row identities".into(),
            ));
        }
        let mut children = vec![Vec::new(); self.nodes.len()];
        let mut ready = VecDeque::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if let Some(parent) = node.parent_id {
                let parent = index.get(&parent).ok_or_else(|| {
                    StorageError::Query("snapshot execution node has no parent".into())
                })?;
                children[*parent].push(i);
            } else {
                ready.push_back(i);
            }
        }
        let mut ordered = Vec::with_capacity(self.nodes.len());
        while let Some(i) = ready.pop_front() {
            ordered.push(&self.nodes[i]);
            ready.extend(children[i].iter().copied());
        }
        if ordered.len() != self.nodes.len() {
            return Err(StorageError::Query(
                "snapshot execution tree contains a cycle".into(),
            ));
        }
        Ok(ordered)
    }
}

/// Check ownership before opening the capsule publication transaction.
pub(crate) fn validate_capsule_output_import(
    instance: &orch8_types::instance::TaskInstance,
    checkpoint: &orch8_types::checkpoint::Checkpoint,
    outputs: &[orch8_types::output::BlockOutput],
) -> Result<(), orch8_types::error::StorageError> {
    use orch8_types::error::StorageError;
    let mut ids = std::collections::HashSet::with_capacity(outputs.len());
    if checkpoint.instance_id != instance.id
        || outputs
            .iter()
            .any(|output| output.instance_id != instance.id || !ids.insert(output.id))
    {
        return Err(StorageError::Constraint(
            "capsule checkpoint/output ownership or duplicate output identity".into(),
        ));
    }
    Ok(())
}
