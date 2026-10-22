//! Keep a parallel batch's claim until every sibling has persisted its result.
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use orch8_storage::StorageBackend;
use orch8_types::instance::{InstanceState, TaskInstance};

use crate::error::EngineError;

#[derive(Clone, Copy)]
pub(crate) enum StepDeferral {
    WakeAt(DateTime<Utc>),
    RetryAt(DateTime<Utc>),
}

struct PendingWake {
    fire_at: DateTime<Utc>,
    audit_retry: bool,
}

/// Shared by exactly one joined batch. Recording a deferral leaves the
/// instance Running so siblings can finish their claim-fenced writes.
#[derive(Default)]
pub(crate) struct PendingStepDeferrals(Mutex<Option<PendingWake>>);

impl PendingStepDeferrals {
    fn record(&self, request: StepDeferral) {
        let (fire_at, audit_retry) = match request {
            StepDeferral::WakeAt(at) => (at, false),
            StepDeferral::RetryAt(at) => (at, true),
        };
        let mut pending = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(wake) = pending.as_mut() {
            wake.fire_at = wake.fire_at.min(fire_at);
            wake.audit_retry |= audit_retry;
        } else {
            *pending = Some(PendingWake {
                fire_at,
                audit_retry,
            });
        }
    }

    pub(crate) async fn apply(
        &self,
        storage: &dyn StorageBackend,
        instance: &TaskInstance,
        claim_token: DateTime<Utc>,
    ) -> Result<(), EngineError> {
        let pending = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(wake) = pending else {
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
    claim_token: DateTime<Utc>,
    request: StepDeferral,
    batch: Option<&PendingStepDeferrals>,
) -> Result<(), EngineError> {
    if let Some(batch) = batch {
        batch.record(request);
        Ok(())
    } else {
        let pending = PendingStepDeferrals::default();
        pending.record(request);
        pending.apply(storage, instance, claim_token).await
    }
}
