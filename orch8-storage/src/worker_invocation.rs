//! Immutable dispatch identity, separate from reusable native nodes and hot tasks.
use orch8_types::{
    continuity::{ContinuityExecution, ContinuityId, EffectId, OwnershipState, RuntimeId},
    ids::{BlockId, ExecutionNodeId, InstanceId, SequenceId, TenantId},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkerInvocationTarget {
    Tree {
        node_id: ExecutionNodeId,
        activation_id: Uuid,
    },
    Flat {
        step_started_at: chrono::DateTime<chrono::Utc>,
    },
    /// Retain uncertain legacy dispatch evidence without certifying a target.
    Unbound { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInvocationContinuity {
    pub continuity_id: ContinuityId,
    pub epoch: u64,
    pub owner_runtime_id: RuntimeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInvocation {
    pub task_id: Uuid,
    pub tenant_id: TenantId,
    pub instance_id: InstanceId,
    pub sequence_id: SequenceId,
    pub block_id: BlockId,
    pub handler_name: String,
    pub attempt: u16,
    pub continuity_epoch: Option<u64>,
    pub continuity: Option<WorkerInvocationContinuity>,
    pub effect_id: Option<EffectId>,
    pub run_id: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    pub target: WorkerInvocationTarget,
}

pub(crate) fn dispatch_target(
    run_id: Option<&str>,
    has_tree: bool,
    nodes: &[(ExecutionNodeId, Uuid)],
    flat_started_at: Option<chrono::DateTime<chrono::Utc>>,
) -> WorkerInvocationTarget {
    if run_id.is_none_or(|id| id.is_empty() || id.len() > 128) {
        return WorkerInvocationTarget::Unbound {
            reason: "missing_or_invalid_run_identity".into(),
        };
    }
    match (has_tree, nodes) {
        (false, _) => flat_started_at.map_or_else(
            || WorkerInvocationTarget::Unbound {
                reason: "missing_flat_step_identity".into(),
            },
            |step_started_at| WorkerInvocationTarget::Flat { step_started_at },
        ),
        (true, [(node_id, activation_id)]) => WorkerInvocationTarget::Tree {
            node_id: *node_id,
            activation_id: *activation_id,
        },
        _ => WorkerInvocationTarget::Unbound {
            reason: "missing_or_ambiguous_active_node".into(),
        },
    }
}

impl WorkerInvocation {
    /// Published mailbox evidence is distinct from an uncertain legacy target.
    #[must_use]
    pub fn is_published_mailbox(&self) -> bool {
        matches!(&self.target, WorkerInvocationTarget::Unbound { reason }
            if reason == crate::delegation_publication::INVOCATION_REASON)
    }

    pub(crate) fn validate_scope(
        &self,
        tenant: &TenantId,
        task: Uuid,
        instance: InstanceId,
    ) -> Result<(), orch8_types::error::StorageError> {
        if self.tenant_id != *tenant || self.task_id != task || self.instance_id != instance {
            return Err(orch8_types::error::StorageError::Constraint(
                "worker invocation indexed scope mismatch".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_continuity(&mut self, owner: Option<&ContinuityExecution>) {
        match (self.continuity_epoch, owner) {
            (None, None) => {}
            (Some(epoch), Some(owner))
                if owner.epoch.get() == epoch
                    && owner.tenant_id == self.tenant_id
                    && owner.current_instance_id == self.instance_id
                    && owner.state == OwnershipState::Owned =>
            {
                self.continuity = Some(WorkerInvocationContinuity {
                    continuity_id: owner.continuity_id,
                    epoch,
                    owner_runtime_id: owner.owner_runtime_id,
                });
            }
            _ => {
                self.target = WorkerInvocationTarget::Unbound {
                    reason: "missing_or_mismatched_dispatch_owner".into(),
                }
            }
        }
    }
}

/// Source authority captured by the engine before asynchronous step preparation.
#[derive(Debug, Clone)]
pub struct WorkerDispatchFence {
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub tenant_id: TenantId,
    pub sequence_id: SequenceId,
    pub claim_token: chrono::DateTime<chrono::Utc>,
    pub run_id: String,
    pub dry_run: bool,
    pub continuity: Option<WorkerInvocationContinuity>,
    pub target: WorkerDispatchTarget,
}

#[derive(Debug, Clone)]
pub enum WorkerDispatchTarget {
    Tree {
        node_id: ExecutionNodeId,
        activation_id: Uuid,
    },
    Flat {
        step_started_at: chrono::DateTime<chrono::Utc>,
    },
}

impl WorkerDispatchFence {
    pub(crate) fn validate(
        &self,
        task: &orch8_types::worker::WorkerTask,
    ) -> Result<(), orch8_types::error::StorageError> {
        if self.instance_id != task.instance_id
            || self.block_id != task.block_id
            || self.run_id.is_empty()
            || self.run_id.len() > 128
            || task.state != orch8_types::worker::WorkerTaskState::Pending
            || task.worker_id.is_some()
            || task.claim_epoch != 0
            || task.continuity_epoch != self.continuity.as_ref().map(|scope| scope.epoch)
        {
            return Err(orch8_types::error::StorageError::Constraint(
                "invalid worker dispatch fence".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn owner_matches(
        &self,
        instance: InstanceId,
        owner: Option<&ContinuityExecution>,
    ) -> bool {
        match (&self.continuity, owner) {
            (None, None) => true,
            (Some(expected), Some(owner)) => {
                owner.tenant_id == self.tenant_id
                    && owner.current_instance_id == instance
                    && owner.state == OwnershipState::Owned
                    && owner.continuity_id == expected.continuity_id
                    && owner.epoch.get() == expected.epoch
                    && owner.owner_runtime_id == expected.owner_runtime_id
            }
            _ => false,
        }
    }
    pub(crate) fn expected_target(&self) -> WorkerInvocationTarget {
        match self.target {
            WorkerDispatchTarget::Tree {
                node_id,
                activation_id,
            } => WorkerInvocationTarget::Tree {
                node_id,
                activation_id,
            },
            WorkerDispatchTarget::Flat { step_started_at } => {
                WorkerInvocationTarget::Flat { step_started_at }
            }
        }
    }
    pub(crate) fn matches(&self, binding: &WorkerInvocation) -> bool {
        binding.instance_id == self.instance_id
            && binding.block_id == self.block_id
            && binding.tenant_id == self.tenant_id
            && binding.sequence_id == self.sequence_id
            && binding.run_id.as_deref() == Some(self.run_id.as_str())
            && binding.dry_run == self.dry_run
            && binding.continuity == self.continuity
            && binding.target == self.expected_target()
    }
}

/// A flat dispatch cannot reset an already established step timeout baseline.
pub(crate) fn flat_stamp_matches(
    current_step: Option<&str>,
    current_start: Option<&str>,
    block: &BlockId,
    expected: chrono::DateTime<chrono::Utc>,
) -> bool {
    if current_step != Some(block.as_str()) {
        return true;
    }
    current_start.is_none_or(|value| {
        chrono::DateTime::parse_from_rfc3339(value)
            .is_ok_and(|time| time.with_timezone(&chrono::Utc) == expected)
    })
}

/// Select an invocation before any asynchronous parameter/credential preparation.
#[derive(Debug, Clone)]
pub enum WorkerDispatchSelection {
    Tree {
        node_id: ExecutionNodeId,
    },
    Flat {
        step_started_at: chrono::DateTime<chrono::Utc>,
    },
}

pub(crate) fn preparation_fence(
    instance: &orch8_types::instance::TaskInstance,
    block_id: &BlockId,
    selection: &WorkerDispatchSelection,
    activation_id: Option<Uuid>,
    owner: Option<ContinuityExecution>,
) -> Option<WorkerDispatchFence> {
    let claim_token = instance.claimed_at?;
    let run_id = instance.context.runtime.run_id.as_ref()?;
    if run_id.is_empty() || run_id.len() > 128 {
        return None;
    }
    let continuity = match owner {
        None => None,
        Some(owner)
            if owner.tenant_id == instance.tenant_id
                && owner.current_instance_id == instance.id
                && owner.state == OwnershipState::Owned =>
        {
            Some(WorkerInvocationContinuity {
                continuity_id: owner.continuity_id,
                epoch: owner.epoch.get(),
                owner_runtime_id: owner.owner_runtime_id,
            })
        }
        Some(_) => return None,
    };
    let target = match selection {
        WorkerDispatchSelection::Tree { node_id } => WorkerDispatchTarget::Tree {
            node_id: *node_id,
            activation_id: activation_id?,
        },
        WorkerDispatchSelection::Flat { step_started_at } => WorkerDispatchTarget::Flat {
            step_started_at: *step_started_at,
        },
    };
    Some(WorkerDispatchFence {
        instance_id: instance.id,
        block_id: block_id.clone(),
        tenant_id: instance.tenant_id.clone(),
        sequence_id: instance.sequence_id,
        claim_token,
        run_id: run_id.clone(),
        dry_run: instance.context.runtime.dry_run,
        continuity,
        target,
    })
}

impl WorkerInvocation {
    /// A current node with the same block is insufficient after loop/run reuse.
    pub(crate) fn same_invocation_scope(&self, other: &Self) -> bool {
        self.task_id == other.task_id
            && self.tenant_id == other.tenant_id
            && self.instance_id == other.instance_id
            && self.sequence_id == other.sequence_id
            && self.block_id == other.block_id
            && self.attempt == other.attempt
            && self.run_id == other.run_id
            && self.dry_run == other.dry_run
            && self.continuity == other.continuity
            && self.continuity_epoch == other.continuity_epoch
            && self.target == other.target
    }

    pub(crate) fn matches_source(&self, current: &Self) -> bool {
        !matches!(self.target, WorkerInvocationTarget::Unbound { .. })
            && self.task_id == current.task_id
            && self.tenant_id == current.tenant_id
            && self.instance_id == current.instance_id
            && self.sequence_id == current.sequence_id
            && self.block_id == current.block_id
            && self.handler_name == current.handler_name
            && self.attempt == current.attempt
            && self.run_id == current.run_id
            && self.dry_run == current.dry_run
            && self.continuity == current.continuity
            && self.continuity_epoch == current.continuity_epoch
            && self.target == current.target
            && self
                .effect_id
                .is_none_or(|id| current.effect_id == Some(id))
    }

    pub(crate) fn accepts_node(&self, node: Option<ExecutionNodeId>) -> bool {
        match self.target {
            WorkerInvocationTarget::Tree { node_id, .. } => node == Some(node_id),
            WorkerInvocationTarget::Flat { .. } => node.is_none(),
            WorkerInvocationTarget::Unbound { .. } => false,
        }
    }

    /// Reserve the next attempt from proven original scope, never from a new run.
    pub(crate) fn retry_binding(
        &self,
        task: &orch8_types::worker::WorkerTask,
        activation: Option<Uuid>,
    ) -> Result<Self, orch8_types::error::StorageError> {
        use orch8_types::error::StorageError;
        self.validate_retry_task(task)?;
        let target = match self.target {
            WorkerInvocationTarget::Tree { node_id, .. } => WorkerInvocationTarget::Tree {
                node_id,
                activation_id: activation.ok_or_else(|| {
                    StorageError::Constraint("missing reserved retry activation".into())
                })?,
            },
            WorkerInvocationTarget::Flat { step_started_at } => {
                WorkerInvocationTarget::Flat { step_started_at }
            }
            WorkerInvocationTarget::Unbound { .. } => {
                return Err(StorageError::Constraint("unknown retry invocation".into()));
            }
        };
        Ok(Self {
            task_id: task.id,
            attempt: task.attempt,
            target,
            effect_id: None,
            ..self.clone()
        })
    }
    pub(crate) fn validate_retry_task(
        &self,
        task: &orch8_types::worker::WorkerTask,
    ) -> Result<(), orch8_types::error::StorageError> {
        use orch8_types::{error::StorageError, worker::WorkerTaskState};
        if task.instance_id != self.instance_id
            || task.block_id != self.block_id
            || task.handler_name != self.handler_name
            || self.attempt.checked_add(1) != Some(task.attempt)
            || task.state != WorkerTaskState::Pending
            || task.worker_id.is_some()
            || task.claim_epoch != 0
            || task.resume_checkpoint.is_some()
            || task.checkpoint_seq != 0
            || task.effect_id.is_some()
            || task
                .continuity_epoch
                .is_some_and(|epoch| Some(epoch) != self.continuity_epoch)
        {
            return Err(StorageError::Constraint(
                "retry does not preserve its source invocation".into(),
            ));
        }

        Ok(())
    }

    pub(crate) fn validate_retry_output(
        &self,
        output: Option<&orch8_types::output::BlockOutput>,
    ) -> Result<(), orch8_types::error::StorageError> {
        if output.is_some_and(|output| {
            output.instance_id != self.instance_id
                || output.block_id != self.block_id
                || output.attempt != self.attempt
                || output.output_ref.as_deref() != Some("__retry__")
        }) {
            return Err(orch8_types::error::StorageError::Constraint(
                "retry output does not match original invocation".into(),
            ));
        }
        Ok(())
    }
}

impl WorkerInvocation {
    pub(crate) fn accepts_recovery_effect(
        &self,
        effect: Option<&WorkerDispatchEffect>,
        epoch: Option<u64>,
        receipt: Option<&orch8_types::continuity::EffectReceipt>,
    ) -> bool {
        if epoch != self.continuity_epoch {
            return false;
        }
        let pure = orch8_types::sequence::is_pure_remote_builtin(&self.handler_name);
        match (effect, receipt, &self.continuity) {
            (None, None, _) => (pure || self.dry_run) && self.effect_id.is_none(),
            (Some(proof), Some(receipt), Some(scope)) => {
                !pure
                    && !self.dry_run
                    && self.effect_id.is_none_or(|bound| bound == proof.effect_id)
                    && receipt.id == proof.effect_id
                    && proof.request_sha256.len() == 64
                    && proof
                        .request_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    && receipt.request_sha256 == proof.request_sha256
                    && proof.destination_fingerprint.len() == 64
                    && proof
                        .destination_fingerprint
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                    && receipt.destination_fingerprint == proof.destination_fingerprint
                    && receipt.kind == proof.kind
                    && receipt.tenant_id == self.tenant_id
                    && receipt.instance_id == self.instance_id
                    && receipt.block_id == self.block_id
                    && receipt.continuity_id == scope.continuity_id
                    && receipt.epoch.get() == scope.epoch
                    && receipt.attempt == u32::from(self.attempt)
                    && receipt.state == orch8_types::continuity::EffectState::Dispatched
            }
            _ => false,
        }
    }
}

impl WorkerInvocation {
    /// Read-only scope preflight. Transactional node/generation checks are still required.
    #[must_use]
    pub fn matches_native_scope(
        &self,
        instance: &orch8_types::instance::TaskInstance,
        owner: Option<&ContinuityExecution>,
    ) -> bool {
        let same_owner = match (&self.continuity, owner) {
            (None, None) => self.continuity_epoch.is_none(),
            (Some(scope), Some(owner)) => {
                self.continuity_epoch == Some(scope.epoch)
                    && owner.tenant_id == self.tenant_id
                    && owner.current_instance_id == self.instance_id
                    && owner.state == OwnershipState::Owned
                    && owner.continuity_id == scope.continuity_id
                    && owner.epoch.get() == scope.epoch
                    && owner.owner_runtime_id == scope.owner_runtime_id
            }
            _ => false,
        };
        same_owner
            && !matches!(self.target, WorkerInvocationTarget::Unbound { .. })
            && instance.id == self.instance_id
            && instance.tenant_id == self.tenant_id
            && instance.sequence_id == self.sequence_id
            && instance.context.runtime.run_id == self.run_id
            && instance.context.runtime.dry_run == self.dry_run
    }
}

/// Logical request evidence computed from the stored, decrypted worker payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDispatchEffect {
    pub effect_id: EffectId,
    pub request_sha256: String,
    pub destination_fingerprint: String,
    pub kind: orch8_types::continuity::EffectKind,
}
