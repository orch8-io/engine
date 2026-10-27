//! Inputs bound by one compensation-start transaction.
use crate::CompensationEffect;
use orch8_types::{
    continuity::{ContinuityExecution, EffectState, OwnershipState},
    continuity_advanced::{CompensationRunRecord, CompensationRunState, CompensationStepState},
    error::StorageError,
    instance::TaskInstance,
};
use std::collections::BTreeMap;

/// Trusted plan plus the source snapshot used to resolve its parameters.
/// Backends compare metadata and the complete compact effect set under the
/// ownership lock before creating any durable compensation state.
#[derive(Debug, Clone, Copy)]
pub struct CompensationStart<'a> {
    pub run: &'a CompensationRunRecord,
    pub execution: &'a ContinuityExecution,
    pub instance: &'a TaskInstance,
    pub effects: &'a [CompensationEffect],
}

impl CompensationStart<'_> {
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        let run = self.run;
        if run.tenant_id != self.execution.tenant_id
            || run.continuity_id != self.execution.continuity_id
            || run.source_instance_id != self.execution.current_instance_id
            || self.instance.id != run.source_instance_id
            || self.instance.tenant_id != run.tenant_id
            || !matches!(
                self.execution.state,
                OwnershipState::Owned | OwnershipState::Completed | OwnershipState::Compensating
            )
            || run.state != CompensationRunState::Planned
            || run.version != 0
            || run.steps.is_empty()
            || run
                .steps
                .iter()
                .any(|step| step.state != CompensationStepState::Pending)
        {
            return Err(StorageError::Constraint(
                "compensation source/plan identity mismatch".into(),
            ));
        }
        let effects: BTreeMap<_, _> = self.effects.iter().map(|e| (e.id, e)).collect();
        let mut seen = std::collections::BTreeSet::new();
        if run.steps.iter().any(|step| {
            !seen.insert(step.plan.effect_id)
                || effects.get(&step.plan.effect_id).is_none_or(|effect| {
                    effect.block_id != step.plan.effect_block_id
                        || !matches!(effect.state, EffectState::Committed | EffectState::Verified)
                })
        }) {
            return Err(StorageError::Constraint(
                "compensation steps do not match source effects".into(),
            ));
        }
        let hazards: std::collections::BTreeSet<_> =
            run.hazards.iter().map(String::as_str).collect();
        let residuals: std::collections::BTreeSet<_> =
            run.residual_effects.iter().map(String::as_str).collect();
        for effect in self.effects {
            let reason = match effect.state {
                EffectState::Planned | EffectState::Prepared => "PENDING_EFFECT_MUST_BE_SETTLED",
                EffectState::Dispatched => "IN_FLIGHT_EFFECT_MUST_BE_SETTLED",
                EffectState::Unknown => "UNKNOWN_EFFECT_MUST_BE_RESOLVED",
                EffectState::Committed | EffectState::Verified if !seen.contains(&effect.id) => {
                    "NO_COMPENSATION_RULE"
                }
                _ => continue,
            };
            let hazard = format!("{reason}:{}:{}", effect.block_id, effect.id);
            if !hazards.contains(hazard.as_str()) || !residuals.contains(hazard.as_str()) {
                return Err(StorageError::Constraint(
                    "compensation plan omitted an unsettled or uncompensated effect".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn matches_effects(&self, current: &[CompensationEffect]) -> bool {
        let expected: BTreeMap<_, _> = self.effects.iter().map(|e| (e.id, e)).collect();
        let observed: BTreeMap<_, _> = current.iter().map(|e| (e.id, e)).collect();
        expected.len() == self.effects.len()
            && observed.len() == current.len()
            && expected == observed
    }
}
