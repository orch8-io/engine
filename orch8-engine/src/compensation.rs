//! Receipt-backed compensation planning for externally visible effects.

use std::collections::{BTreeMap, BTreeSet};

use orch8_types::continuity::{EffectId, EffectReceipt, EffectState};
use orch8_types::ids::BlockId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompensationRule {
    pub effect_block_id: BlockId,
    pub handler: String,
    #[serde(default)]
    pub params: serde_json::Value,
    #[serde(default)]
    pub depends_on: Vec<BlockId>,
    #[serde(default)]
    pub verification_required: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompensationPlanStep {
    pub effect_id: EffectId,
    pub effect_block_id: BlockId,
    pub handler: String,
    pub params: serde_json::Value,
    pub idempotency_key: String,
    pub verification_required: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompensationPlan {
    pub steps: Vec<CompensationPlanStep>,
    pub hazards: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompensationPlanError {
    #[error("compensation dependencies contain a cycle")]
    DependencyCycle,
    #[error("duplicate compensation rule for block {0}")]
    DuplicateRule(BlockId),
}

fn visit_dependency<'a>(
    block: &BlockId,
    rules: &BTreeMap<BlockId, &'a CompensationRule>,
    committed: &BTreeMap<BlockId, &'a EffectReceipt>,
    visiting: &mut BTreeSet<BlockId>,
    visited: &mut BTreeSet<BlockId>,
    ordered: &mut Vec<BlockId>,
) -> Result<(), CompensationPlanError> {
    if visited.contains(block) || !committed.contains_key(block) {
        return Ok(());
    }
    if !visiting.insert(block.clone()) {
        return Err(CompensationPlanError::DependencyCycle);
    }
    if let Some(rule) = rules.get(block) {
        for dependency in &rule.depends_on {
            visit_dependency(dependency, rules, committed, visiting, visited, ordered)?;
        }
    }
    visiting.remove(block);
    visited.insert(block.clone());
    ordered.push(block.clone());
    Ok(())
}

pub fn build_compensation_plan(
    receipts: &[EffectReceipt],
    rules: &[CompensationRule],
) -> Result<CompensationPlan, CompensationPlanError> {
    let mut rules_by_block = BTreeMap::new();
    for rule in rules {
        if rules_by_block
            .insert(rule.effect_block_id.clone(), rule)
            .is_some()
        {
            return Err(CompensationPlanError::DuplicateRule(
                rule.effect_block_id.clone(),
            ));
        }
    }
    let committed: BTreeMap<_, _> = receipts
        .iter()
        .filter(|receipt| {
            matches!(
                receipt.state,
                EffectState::Committed | EffectState::Verified
            )
        })
        .map(|receipt| (receipt.block_id.clone(), receipt))
        .collect();
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut ordered = Vec::new();

    for block in committed.keys() {
        visit_dependency(
            block,
            &rules_by_block,
            &committed,
            &mut visiting,
            &mut visited,
            &mut ordered,
        )?;
    }
    ordered.reverse();

    let mut hazards = Vec::new();
    let steps = ordered
        .into_iter()
        .filter_map(|block| {
            let receipt = committed[&block];
            let Some(rule) = rules_by_block.get(&block) else {
                hazards.push(format!(
                    "NO_COMPENSATION_RULE:{}:{}",
                    block.as_str(),
                    receipt.id
                ));
                return None;
            };
            Some(CompensationPlanStep {
                effect_id: receipt.id,
                effect_block_id: block,
                handler: rule.handler.clone(),
                params: rule.params.clone(),
                idempotency_key: format!("compensate:{}", receipt.id),
                verification_required: rule.verification_required,
            })
        })
        .collect();
    for receipt in receipts
        .iter()
        .filter(|receipt| receipt.state == EffectState::Unknown)
    {
        hazards.push(format!(
            "UNKNOWN_EFFECT_MUST_BE_RESOLVED:{}:{}",
            receipt.block_id.as_str(),
            receipt.id
        ));
    }
    hazards.sort();
    Ok(CompensationPlan { steps, hazards })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_types::continuity::{ContinuityId, EffectKind, ExecutionEpoch};
    use orch8_types::ids::{InstanceId, TenantId};

    use super::*;

    fn receipt(block: &str, state: EffectState) -> EffectReceipt {
        EffectReceipt {
            id: EffectId::new(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            continuity_id: ContinuityId::new(),
            epoch: ExecutionEpoch::initial(),
            instance_id: InstanceId::new(),
            block_id: BlockId::new(block),
            kind: EffectKind::Http,
            state,
            destination_fingerprint: "provider".into(),
            idempotency_key: None,
            request_sha256: "a".repeat(64),
            provider_receipt_id: None,
            attempt: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn plan_uses_committed_receipts_and_reverse_dependencies() {
        let first = receipt("reserve", EffectState::Committed);
        let second = receipt("charge", EffectState::Verified);
        let unknown = receipt("notify", EffectState::Unknown);
        let plan = build_compensation_plan(
            &[first, second, unknown],
            &[
                CompensationRule {
                    effect_block_id: BlockId::new("reserve"),
                    handler: "release".into(),
                    params: serde_json::Value::Null,
                    depends_on: Vec::new(),
                    verification_required: false,
                },
                CompensationRule {
                    effect_block_id: BlockId::new("charge"),
                    handler: "refund".into(),
                    params: serde_json::Value::Null,
                    depends_on: vec![BlockId::new("reserve")],
                    verification_required: true,
                },
            ],
        )
        .unwrap();
        assert_eq!(plan.steps[0].handler, "refund");
        assert_eq!(plan.steps[1].handler, "release");
        assert!(
            plan.hazards
                .iter()
                .any(|hazard| hazard.starts_with("UNKNOWN_EFFECT_MUST_BE_RESOLVED"))
        );
    }
}
