//! Keep a claim until runnable work settles, then park at the earliest wake.
use std::collections::HashMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::ids::ExecutionNodeId;
use orch8_types::instance::{InstanceState, TaskInstance};

use crate::error::EngineError;

#[derive(Clone, Copy)]
pub(crate) enum StepDeferral {
    WakeAt(DateTime<Utc>),
    RetryAt(DateTime<Utc>),
}

#[derive(Clone, Copy)]
struct PendingWake {
    fire_at: DateTime<Utc>,
    audit_retry: bool,
}

impl PendingWake {
    fn merge(self, other: Self) -> Self {
        Self {
            fire_at: self.fire_at.min(other.fire_at),
            audit_retry: self.audit_retry || other.audit_retry,
        }
    }
}

/// Shared by one evaluation. Deferred nodes remain Running but are skipped
/// until the next evaluation, so other branches can finish their fenced writes.
#[derive(Default)]
pub(crate) struct PendingStepDeferrals(Mutex<HashMap<ExecutionNodeId, PendingWake>>);

impl PendingStepDeferrals {
    fn record(&self, node_id: ExecutionNodeId, request: StepDeferral) {
        let (fire_at, audit_retry) = match request {
            StepDeferral::WakeAt(at) => (at, false),
            StepDeferral::RetryAt(at) => (at, true),
        };
        let wake = PendingWake {
            fire_at,
            audit_retry,
        };
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(node_id)
            .and_modify(|previous| *previous = previous.merge(wake))
            .or_insert(wake);
    }

    pub(crate) fn contains(&self, node_id: ExecutionNodeId) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&node_id)
    }

    pub(crate) async fn apply(
        &self,
        storage: &dyn StorageBackend,
        instance: &TaskInstance,
        claim_token: DateTime<Utc>,
        tree: Option<&[ExecutionNode]>,
    ) -> Result<(), EngineError> {
        let pending = std::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        // A race winner can cancel a previously deferred leaf. Its deadline
        // must not park unrelated work or cause an unnecessary later wake.
        let wake = if let Some(tree) = tree {
            tree.iter()
                .filter(|node| node.state == NodeState::Running)
                .filter_map(|node| pending.get(&node.id))
                .copied()
                .reduce(PendingWake::merge)
        } else {
            pending.into_values().reduce(PendingWake::merge)
        };
        let Some(wake) = wake else {
            return Ok(());
        };
        if wake.audit_retry {
            crate::lifecycle::transition_claimed_instance(
                storage,
                instance.id,
                Some(&instance.tenant_id),
                claim_token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(wake.fire_at),
            )
            .await
        } else {
            crate::scheduler::park_tree_instance_until(
                storage,
                instance.id,
                claim_token,
                wake.fire_at,
            )
            .await
        }
    }
}

pub(crate) async fn defer_step(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node_id: ExecutionNodeId,
    claim_token: DateTime<Utc>,
    request: StepDeferral,
    pending: Option<&PendingStepDeferrals>,
) -> Result<(), EngineError> {
    if let Some(pending) = pending {
        pending.record(node_id, request);
        Ok(())
    } else {
        let pending = PendingStepDeferrals::default();
        pending.record(node_id, request);
        pending.apply(storage, instance, claim_token, None).await
    }
}
