//! Receipt-backed compensation planning for externally visible effects.

use std::collections::{BTreeMap, BTreeSet};

use orch8_types::continuity::{EffectReceipt, EffectState};
use orch8_types::continuity_advanced::{CompensationPlan, CompensationPlanStep};
use orch8_types::ids::BlockId;
use orch8_types::sequence::CompensationVerificationPolicy;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
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
    pub verification: CompensationVerificationPolicy,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompensationPlanError {
    #[error("compensation dependencies contain a cycle")]
    DependencyCycle,
    #[error("duplicate compensation rule for block {0}")]
    DuplicateRule(BlockId),
}

/// Extract typed recovery rules from every step in a sequence tree.
#[must_use]
pub fn rules_from_sequence(sequence: &SequenceDefinition) -> Vec<CompensationRule> {
    fn collect(blocks: &[BlockDefinition], rules: &mut Vec<CompensationRule>) {
        for block in blocks {
            match block {
                BlockDefinition::Step(step) => {
                    if let Some(compensation) = &step.compensation {
                        rules.push(CompensationRule {
                            effect_block_id: step.id.clone(),
                            handler: compensation.handler.clone(),
                            params: compensation.params.clone(),
                            depends_on: compensation.depends_on.clone(),
                            verification: compensation.verification,
                        });
                    }
                }
                BlockDefinition::Parallel(value) => {
                    for branch in &value.branches {
                        collect(branch, rules);
                    }
                }
                BlockDefinition::Race(value) => {
                    for branch in &value.branches {
                        collect(branch, rules);
                    }
                }
                BlockDefinition::Loop(value) => collect(&value.body, rules),
                BlockDefinition::ForEach(value) => collect(&value.body, rules),
                BlockDefinition::Router(value) => {
                    for route in &value.routes {
                        collect(&route.blocks, rules);
                    }
                    if let Some(default) = &value.default {
                        collect(default, rules);
                    }
                }
                BlockDefinition::TryCatch(value) => {
                    collect(&value.try_block, rules);
                    collect(&value.catch_block, rules);
                    if let Some(finally) = &value.finally_block {
                        collect(finally, rules);
                    }
                }
                BlockDefinition::ABSplit(value) => {
                    for variant in &value.variants {
                        collect(&variant.blocks, rules);
                    }
                }
                BlockDefinition::CancellationScope(value) => collect(&value.blocks, rules),
                BlockDefinition::Saga(value) => {
                    for step in &value.steps {
                        collect(std::slice::from_ref(step.action.as_ref()), rules);
                        if let Some(compensation) = &step.compensation {
                            collect(std::slice::from_ref(compensation.as_ref()), rules);
                        }
                    }
                }
                BlockDefinition::SubSequence(_) => {}
            }
        }
    }

    let mut rules = Vec::new();
    collect(&sequence.blocks, &mut rules);
    rules
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
    let committed_receipts: Vec<_> = receipts
        .iter()
        .filter(|receipt| {
            matches!(
                receipt.state,
                EffectState::Committed | EffectState::Verified
            )
        })
        .collect();
    let committed: BTreeMap<_, _> = committed_receipts
        .iter()
        .map(|receipt| (receipt.block_id.clone(), *receipt))
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
    let mut steps = Vec::new();
    for block in ordered {
        let mut block_receipts: Vec<_> = committed_receipts
            .iter()
            .copied()
            .filter(|receipt| receipt.block_id == block)
            .collect();
        block_receipts.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.to_string().cmp(&left.id.to_string()))
        });
        for receipt in block_receipts {
            let Some(rule) = rules_by_block.get(&block) else {
                hazards.push(format!(
                    "NO_COMPENSATION_RULE:{}:{}",
                    block.as_str(),
                    receipt.id
                ));
                continue;
            };
            steps.push(CompensationPlanStep {
                effect_id: receipt.id,
                effect_block_id: block.clone(),
                handler: rule.handler.clone(),
                params: rule.params.clone(),
                idempotency_key: format!("compensate:{}", receipt.id),
                verification: rule.verification,
            });
        }
    }
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
    use orch8_types::continuity::{ContinuityId, EffectId, EffectKind, ExecutionEpoch};
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{
        ABSplitDef, ABVariant, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef, RaceDef,
        RaceSemantics, Route, RouterDef, SagaDef, SagaStep, SequenceStatus, StepCompensation,
        StepDef, SubSequenceDef, TryCatchDef,
    };

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

    fn compensated_step(id: &str) -> BlockDefinition {
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new(id),
            handler: "effect".into(),
            params: serde_json::json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
            when: None,
            compensation: Some(StepCompensation {
                handler: format!("undo-{id}"),
                params: serde_json::json!({"source": id}),
                depends_on: Vec::new(),
                verification: CompensationVerificationPolicy::HandlerResult,
            }),
        }))
    }

    fn sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
        SequenceDefinition {
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            namespace: Namespace::new("default"),
            name: "compensation-rules".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::Production,
            blocks,
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: Utc::now(),
        }
    }

    fn rule(block: &str) -> CompensationRule {
        CompensationRule {
            effect_block_id: BlockId::new(block),
            handler: format!("undo-{block}"),
            params: serde_json::Value::Null,
            depends_on: Vec::new(),
            verification: CompensationVerificationPolicy::HandlerResult,
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
                    verification: CompensationVerificationPolicy::HandlerResult,
                },
                CompensationRule {
                    effect_block_id: BlockId::new("charge"),
                    handler: "refund".into(),
                    params: serde_json::Value::Null,
                    depends_on: vec![BlockId::new("reserve")],
                    verification: CompensationVerificationPolicy::ProviderReceipt,
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

    #[test]
    fn plan_rejects_compensation_dependency_cycles() {
        let first = receipt("reserve", EffectState::Committed);
        let second = receipt("charge", EffectState::Committed);
        let error = build_compensation_plan(
            &[first, second],
            &[
                CompensationRule {
                    effect_block_id: BlockId::new("reserve"),
                    handler: "release".into(),
                    params: serde_json::Value::Null,
                    depends_on: vec![BlockId::new("charge")],
                    verification: CompensationVerificationPolicy::HandlerResult,
                },
                CompensationRule {
                    effect_block_id: BlockId::new("charge"),
                    handler: "refund".into(),
                    params: serde_json::Value::Null,
                    depends_on: vec![BlockId::new("reserve")],
                    verification: CompensationVerificationPolicy::HandlerResult,
                },
            ],
        )
        .unwrap_err();
        assert_eq!(error, CompensationPlanError::DependencyCycle);
    }

    #[test]
    fn rules_are_extracted_from_every_nested_workflow_container() {
        let workflow = sequence(vec![
            BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("parallel"),
                branches: vec![
                    vec![BlockDefinition::Race(Box::new(RaceDef {
                        id: BlockId::new("race"),
                        branches: vec![vec![compensated_step("race-step")]],
                        semantics: RaceSemantics::FirstToResolve,
                    }))],
                    vec![BlockDefinition::Loop(Box::new(LoopDef {
                        id: BlockId::new("loop"),
                        condition: "true".into(),
                        body: vec![compensated_step("loop-step")],
                        max_iterations: 1,
                        break_on: None,
                        continue_on_error: false,
                        poll_interval: None,
                        retain_iterations: None,
                    }))],
                ],
            })),
            BlockDefinition::ForEach(Box::new(ForEachDef {
                id: BlockId::new("foreach"),
                collection: "data.items".into(),
                item_var: "item".into(),
                body: vec![compensated_step("foreach-step")],
                max_iterations: 10,
                retain_iterations: None,
            })),
            BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId::new("router"),
                routes: vec![Route {
                    condition: "true".into(),
                    blocks: vec![compensated_step("route-step")],
                }],
                default: Some(vec![compensated_step("default-step")]),
            })),
            BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("try-catch"),
                try_block: vec![compensated_step("try-step")],
                catch_block: vec![compensated_step("catch-step")],
                finally_block: Some(vec![compensated_step("finally-step")]),
            })),
            BlockDefinition::ABSplit(Box::new(ABSplitDef {
                id: BlockId::new("experiment"),
                variants: vec![ABVariant {
                    name: "control".into(),
                    weight: 1,
                    blocks: vec![compensated_step("variant-step")],
                }],
            })),
            BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
                id: BlockId::new("scope"),
                blocks: vec![compensated_step("scope-step")],
            })),
            BlockDefinition::Saga(Box::new(SagaDef {
                id: BlockId::new("saga"),
                steps: vec![SagaStep {
                    id: BlockId::new("saga-entry"),
                    action: Box::new(compensated_step("saga-action")),
                    compensation: Some(Box::new(compensated_step("saga-compensation"))),
                }],
            })),
            BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new("child"),
                sequence_name: "other".into(),
                version: None,
                input: serde_json::json!({}),
            })),
        ]);

        let extracted = rules_from_sequence(&workflow);
        let ids: Vec<_> = extracted
            .iter()
            .map(|value| value.effect_block_id.as_str())
            .collect();

        assert_eq!(
            ids,
            [
                "race-step",
                "loop-step",
                "foreach-step",
                "route-step",
                "default-step",
                "try-step",
                "catch-step",
                "finally-step",
                "variant-step",
                "scope-step",
                "saga-action",
                "saga-compensation",
            ]
        );
        assert_eq!(extracted[3].handler, "undo-route-step");
        assert_eq!(
            extracted[3].params,
            serde_json::json!({"source": "route-step"})
        );
    }

    #[test]
    fn plan_rejects_duplicate_rules_before_planning() {
        let receipt = receipt("charge", EffectState::Committed);

        let error =
            build_compensation_plan(&[receipt], &[rule("charge"), rule("charge")]).unwrap_err();

        assert_eq!(
            error,
            CompensationPlanError::DuplicateRule(BlockId::new("charge"))
        );
    }

    #[test]
    fn committed_effect_without_rule_becomes_an_explicit_hazard() {
        let receipt = receipt("charge", EffectState::Committed);
        let receipt_id = receipt.id;

        let plan = build_compensation_plan(&[receipt], &[]).unwrap();

        assert!(plan.steps.is_empty());
        assert_eq!(
            plan.hazards,
            [format!("NO_COMPENSATION_RULE:charge:{receipt_id}")]
        );
    }

    #[test]
    fn repeated_effects_compensate_newest_receipt_first() {
        let now = Utc::now();
        let mut older = receipt("charge", EffectState::Committed);
        older.created_at = now - chrono::Duration::seconds(1);
        let mut newer = receipt("charge", EffectState::Committed);
        newer.created_at = now;

        let plan =
            build_compensation_plan(&[older.clone(), newer.clone()], &[rule("charge")]).unwrap();

        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.steps[0].effect_id, newer.id);
        assert_eq!(plan.steps[1].effect_id, older.id);
        assert_eq!(
            plan.steps[0].idempotency_key,
            format!("compensate:{}", newer.id)
        );
    }
}
