//! Bounded evidence, migration, simulation, and intelligent-policy services.

use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use orch8_types::continuity::{EffectReceipt, EffectState, RuntimeTrustLevel};
use orch8_types::continuity_advanced::{
    AttentionState, AttentionTask, BudgetReservation, DeviceDelegation, DisclosureResult,
    EvidenceStatus, FaultInjection, FederationEnvelope, FederationPeer, GeneratedScenario,
    IncidentCaseId, IncidentReproduction, InvariantResult, InvariantResultId, InvariantRule,
    LiveMigrationPlan, MigrationDisposition, MigrationPlanId, OptimizationRecommendation,
    ProviderCandidate, ProviderDecision, ProviderDecisionId, RecommendationId, ReservationState,
    ResidencyEvidence, ReviewerCapabilities, ScenarioId, StateTransform, WhatIfScenario,
    WorkflowInvariant,
};
use orch8_types::instance::{Budget, BudgetUsage};
use sha2::{Digest, Sha256};

const MAX_SCENARIO_EVENTS: usize = 64;
const MAX_SCENARIO_FAULTS: usize = 32;
const MAX_MIGRATION_TRANSFORMS: usize = 128;

pub struct InvariantEvidence<'a> {
    pub receipts: &'a [EffectReceipt],
    pub terminal_state: Option<&'a str>,
    pub budget_breached: Option<bool>,
    pub output_paths: &'a BTreeSet<(orch8_types::ids::BlockId, String)>,
}

type InvariantOutcome = (EvidenceStatus, String, Vec<String>);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdvancedContinuityError {
    #[error("scenario exceeds a bounded continuity limit")]
    ScenarioTooLarge,
    #[error("migration transform set exceeds the bounded limit")]
    TooManyTransforms,
    #[error("migration transform is invalid: {0}")]
    InvalidTransform(String),
    #[error("budget reservation is negative or exceeds a configured hard limit: {0}")]
    BudgetDenied(&'static str),
    #[error("reservation is no longer active")]
    ReservationInactive,
    #[error("federation envelope is outside its validity window")]
    FederationEnvelopeExpired,
    #[error("federation peer is revoked or is not authorized for the tenant")]
    FederationPeerDenied,
    #[error("federation envelope identity or digest is invalid")]
    FederationEnvelopeInvalid,
    #[error("device delegation is stale or does not match its continuation grant")]
    DelegationDenied,
}

#[must_use]
pub fn evaluate_invariant(
    invariant: &WorkflowInvariant,
    continuity_id: orch8_types::continuity::ContinuityId,
    epoch: orch8_types::continuity::ExecutionEpoch,
    evidence: &InvariantEvidence<'_>,
    now: DateTime<Utc>,
) -> InvariantResult {
    let (status, summary, evidence_sha256) = evaluate_rule(&invariant.rule, evidence);
    InvariantResult {
        id: InvariantResultId::new(),
        invariant_id: invariant.id,
        continuity_id,
        epoch,
        status,
        dedupe_key: invariant_result_dedupe(invariant.id, continuity_id, epoch, &evidence_sha256),
        evidence_sha256,
        summary,
        evaluated_at: now,
    }
}

fn evaluate_rule(rule: &InvariantRule, evidence: &InvariantEvidence<'_>) -> InvariantOutcome {
    match rule {
        InvariantRule::EffectAtMostOnce { kind } => effect_at_most_once(*kind, evidence.receipts),
        InvariantRule::NoUnknownEffects => no_unknown_effects(evidence.receipts),
        InvariantRule::TerminalStateIn { states } => {
            terminal_state_in(states, evidence.terminal_state)
        }
        InvariantRule::BudgetWithinLimits => budget_within_limits(evidence.budget_breached),
        InvariantRule::OutputPathPresent { block_id, path } => {
            output_path_present(evidence.output_paths, block_id, path)
        }
    }
}

fn effect_at_most_once(
    kind: orch8_types::continuity::EffectKind,
    receipts: &[EffectReceipt],
) -> InvariantOutcome {
    let matching: Vec<_> = receipts
        .iter()
        .filter(|receipt| {
            receipt.kind == kind
                && matches!(
                    receipt.state,
                    EffectState::Dispatched
                        | EffectState::Committed
                        | EffectState::Unknown
                        | EffectState::Verified
                )
        })
        .collect();
    let mut observed_requests = std::collections::HashSet::with_capacity(matching.len());
    let duplicates = matching.iter().any(|receipt| {
        !observed_requests.insert((
            receipt.destination_fingerprint.as_str(),
            receipt.request_sha256.as_str(),
        ))
    });
    (
        if duplicates {
            EvidenceStatus::Fail
        } else {
            EvidenceStatus::Pass
        },
        if duplicates {
            "duplicate effect dispatch or commit evidence detected"
        } else {
            "no duplicate effect dispatch or commit evidence detected"
        }
        .to_owned(),
        matching
            .iter()
            .map(|receipt| receipt.request_sha256.clone())
            .collect(),
    )
}

fn no_unknown_effects(receipts: &[EffectReceipt]) -> InvariantOutcome {
    let unknown: Vec<_> = receipts
        .iter()
        .filter(|receipt| receipt.state == EffectState::Unknown)
        .map(|receipt| receipt.request_sha256.clone())
        .collect();
    (
        if unknown.is_empty() {
            EvidenceStatus::Pass
        } else {
            EvidenceStatus::Fail
        },
        format!("{} unresolved effect receipt(s)", unknown.len()),
        unknown,
    )
}

fn terminal_state_in(states: &[String], terminal_state: Option<&str>) -> InvariantOutcome {
    terminal_state.map_or_else(
        || {
            (
                EvidenceStatus::Unknown,
                "terminal state evidence is absent".into(),
                Vec::new(),
            )
        },
        |state| {
            let pass = states.iter().any(|allowed| allowed == state);
            (
                if pass {
                    EvidenceStatus::Pass
                } else {
                    EvidenceStatus::Fail
                },
                format!("terminal state is {state}"),
                Vec::new(),
            )
        },
    )
}

fn budget_within_limits(budget_breached: Option<bool>) -> InvariantOutcome {
    budget_breached.map_or_else(
        || {
            (
                EvidenceStatus::Unknown,
                "budget evidence is absent".into(),
                Vec::new(),
            )
        },
        |breached| {
            (
                if breached {
                    EvidenceStatus::Fail
                } else {
                    EvidenceStatus::Pass
                },
                if breached {
                    "a hard budget was exceeded"
                } else {
                    "all hard budgets are within limits"
                }
                .into(),
                Vec::new(),
            )
        },
    )
}

fn output_path_present(
    output_paths: &BTreeSet<(orch8_types::ids::BlockId, String)>,
    block_id: &orch8_types::ids::BlockId,
    path: &str,
) -> InvariantOutcome {
    let present = output_paths.contains(&(block_id.clone(), path.to_owned()));
    (
        if present {
            EvidenceStatus::Pass
        } else {
            EvidenceStatus::Unknown
        },
        if present {
            format!("output path {path} is present")
        } else {
            format!("output path {path} has no retained evidence")
        },
        Vec::new(),
    )
}

fn invariant_result_dedupe(
    invariant_id: orch8_types::continuity_advanced::InvariantId,
    continuity_id: orch8_types::continuity::ContinuityId,
    epoch: orch8_types::continuity::ExecutionEpoch,
    evidence: &[String],
) -> String {
    let material = format!(
        "{invariant_id}:{continuity_id}:{}:{}",
        epoch.get(),
        evidence.join(":")
    );
    hex_sha256(material.as_bytes())
}

pub fn compile_migration_plan(
    mut plan: LiveMigrationPlan,
    typed_findings: &[String],
    transforms: Vec<StateTransform>,
    historical_validation_passed: Option<bool>,
) -> Result<LiveMigrationPlan, AdvancedContinuityError> {
    if transforms.len() > MAX_MIGRATION_TRANSFORMS {
        return Err(AdvancedContinuityError::TooManyTransforms);
    }
    validate_state_transforms(&transforms)?;
    let incompatible = typed_findings.iter().any(|code| {
        matches!(
            code.as_str(),
            "MISSING_PRODUCER" | "SCHEMA_PATH_MISSING" | "INCOMPATIBLE_COERCION"
        ) || code.starts_with("INCOMPATIBLE:")
    });
    plan.id = MigrationPlanId::new();
    plan.transforms = transforms;
    plan.finding_codes = typed_findings.to_vec();
    plan.rollback_capsule_required = true;
    plan.disposition = if incompatible {
        MigrationDisposition::Incompatible
    } else if historical_validation_passed == Some(true) && plan.transforms.is_empty() {
        MigrationDisposition::Automatic
    } else if historical_validation_passed == Some(false) {
        MigrationDisposition::Pin
    } else {
        MigrationDisposition::ApprovalRequired
    };
    Ok(plan)
}

fn transform_path(path: &str) -> Result<Vec<&str>, AdvancedContinuityError> {
    if path.is_empty() || path.len() > 1_024 {
        return Err(AdvancedContinuityError::InvalidTransform(
            "paths must contain 1..=1024 bytes".into(),
        ));
    }
    let segments: Vec<_> = path.split('.').collect();
    if segments.len() > 32
        || segments
            .iter()
            .any(|segment| segment.is_empty() || segment.len() > 128)
    {
        return Err(AdvancedContinuityError::InvalidTransform(
            "paths must contain 1..=32 non-empty segments of at most 128 bytes".into(),
        ));
    }
    Ok(segments)
}

pub fn validate_state_transforms(
    transforms: &[StateTransform],
) -> Result<(), AdvancedContinuityError> {
    if transforms.len() > MAX_MIGRATION_TRANSFORMS {
        return Err(AdvancedContinuityError::TooManyTransforms);
    }
    let mut destinations = std::collections::BTreeSet::new();
    for transform in transforms {
        if transform.version != 1 {
            return Err(AdvancedContinuityError::InvalidTransform(format!(
                "unsupported transform version {}",
                transform.version
            )));
        }
        transform_path(&transform.from_path)?;
        match transform.transform.as_str() {
            "copy" | "move" => {
                transform_path(&transform.to_path)?;
                if !destinations.insert(transform.to_path.as_str()) {
                    return Err(AdvancedContinuityError::InvalidTransform(format!(
                        "duplicate destination path {}",
                        transform.to_path
                    )));
                }
            }
            "drop" if transform.to_path.is_empty() => {}
            operation => {
                return Err(AdvancedContinuityError::InvalidTransform(format!(
                    "unsupported operation {operation}"
                )));
            }
        }
    }
    Ok(())
}

fn take_path(value: &mut serde_json::Value, path: &[&str]) -> Option<serde_json::Value> {
    let (last, parents) = path.split_last()?;
    let mut current = value;
    for segment in parents {
        current = current.as_object_mut()?.get_mut(*segment)?;
    }
    current.as_object_mut()?.remove(*last)
}

fn get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, segment| current.as_object()?.get(*segment))
}

fn set_path(
    value: &mut serde_json::Value,
    path: &[&str],
    replacement: serde_json::Value,
) -> Result<(), AdvancedContinuityError> {
    let (last, parents) = path.split_last().ok_or_else(|| {
        AdvancedContinuityError::InvalidTransform("destination path is empty".into())
    })?;
    let mut current = value;
    for segment in parents {
        if current.is_null() {
            *current = serde_json::json!({});
        }
        let object = current.as_object_mut().ok_or_else(|| {
            AdvancedContinuityError::InvalidTransform(format!(
                "destination parent {segment} is not an object"
            ))
        })?;
        current = object
            .entry((*segment).to_owned())
            .or_insert(serde_json::Value::Null);
    }
    if current.is_null() {
        *current = serde_json::json!({});
    }
    let object = current.as_object_mut().ok_or_else(|| {
        AdvancedContinuityError::InvalidTransform("destination parent is not an object".into())
    })?;
    object.insert((*last).to_owned(), replacement);
    Ok(())
}

pub fn apply_state_transforms(
    source: &serde_json::Value,
    transforms: &[StateTransform],
) -> Result<serde_json::Value, AdvancedContinuityError> {
    validate_state_transforms(transforms)?;
    let mut result = source.clone();
    for transform in transforms {
        let from = transform_path(&transform.from_path)?;
        let replacement = match transform.transform.as_str() {
            "copy" => get_path(&result, &from).cloned(),
            "move" | "drop" => take_path(&mut result, &from),
            _ => unreachable!("validated operation"),
        }
        .ok_or_else(|| {
            AdvancedContinuityError::InvalidTransform(format!(
                "source path {} does not exist",
                transform.from_path
            ))
        })?;
        if transform.transform != "drop" {
            let to = transform_path(&transform.to_path)?;
            set_path(&mut result, &to, replacement)?;
        }
    }
    Ok(result)
}

pub fn generate_scenarios(
    events: &[String],
    faults: &[FaultInjection],
    max_scenarios: usize,
    seed: u64,
) -> Result<Vec<GeneratedScenario>, AdvancedContinuityError> {
    if events.len() > MAX_SCENARIO_EVENTS || faults.len() > MAX_SCENARIO_FAULTS {
        return Err(AdvancedContinuityError::ScenarioTooLarge);
    }
    let limit = max_scenarios.min(256);
    let mut scenarios = Vec::with_capacity(limit);
    for index in 0..limit {
        let mut event_order = events.to_vec();
        if !event_order.is_empty() {
            let rotation = usize::try_from(seed)
                .unwrap_or(usize::MAX)
                .wrapping_add(index)
                % event_order.len();
            event_order.rotate_left(rotation);
            if index % 2 == 1 {
                event_order.reverse();
            }
        }
        let selected_faults = if faults.is_empty() {
            Vec::new()
        } else {
            vec![faults[index % faults.len()].clone()]
        };
        scenarios.push(GeneratedScenario {
            id: ScenarioId::new(),
            event_order,
            faults: selected_faults,
            max_steps: 10_000,
            seed: seed.wrapping_add(u64::try_from(index).unwrap_or(u64::MAX)),
        });
    }
    Ok(scenarios)
}

pub fn minimize_reproducing_scenario(
    mut scenario: GeneratedScenario,
    mut reproduces: impl FnMut(&GeneratedScenario) -> bool,
) -> IncidentReproduction {
    let mut attempts = 1;
    if !reproduces(&scenario) {
        return IncidentReproduction {
            id: IncidentCaseId::new(),
            stable_failure_code: "NOT_REPRODUCED".into(),
            scenario: None,
            status: EvidenceStatus::Inconclusive,
            missing_evidence: Vec::new(),
            attempts,
        };
    }
    let mut index = scenario.faults.len();
    while index > 0 && attempts < 128 {
        index -= 1;
        let mut candidate = scenario.clone();
        candidate.faults.remove(index);
        attempts += 1;
        if reproduces(&candidate) {
            scenario = candidate;
        }
    }
    IncidentReproduction {
        id: IncidentCaseId::new(),
        stable_failure_code: "REPRODUCED".into(),
        scenario: Some(scenario),
        status: EvidenceStatus::Pass,
        missing_evidence: Vec::new(),
        attempts,
    }
}

pub fn reserve_budget(
    budget: &Budget,
    reservation: &mut BudgetReservation,
    already_reserved: BudgetUsage,
) -> Result<(), AdvancedContinuityError> {
    let requested = reservation.requested;
    let combined = add_usage(already_reserved, requested);
    if usage_has_negative(requested) {
        return Err(AdvancedContinuityError::BudgetDenied(
            "negative_reservation",
        ));
    }
    if let Some(breach) = budget.first_extended_breach(combined) {
        return Err(AdvancedContinuityError::BudgetDenied(breach.limit));
    }
    reservation.state = ReservationState::Reserved;
    Ok(())
}

pub fn reconcile_budget(
    reservation: &mut BudgetReservation,
    actual: BudgetUsage,
) -> Result<BudgetUsage, AdvancedContinuityError> {
    if reservation.state != ReservationState::Reserved {
        return Err(AdvancedContinuityError::ReservationInactive);
    }
    reservation.actual = Some(actual);
    reservation.state = ReservationState::Reconciled;
    Ok(sub_usage(reservation.requested, actual))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequirements {
    pub allowed_regions: BTreeSet<String>,
    pub max_price_microunits: Option<i64>,
    pub max_latency_ms: Option<u64>,
    pub minimum_quality_millipoints: Option<i64>,
    pub require_idempotency: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowAggregate {
    pub serial_work_millipoints: u16,
    pub retry_rate_millipoints: u16,
    pub average_payload_bytes: u64,
    pub average_cost_microunits: i64,
    pub dead_branch_count: u32,
}

#[must_use]
pub fn recommend_optimizations(
    aggregate: WorkflowAggregate,
    base_scenario: &WhatIfScenario,
) -> Vec<OptimizationRecommendation> {
    let mut recommendations = Vec::new();
    let mut add =
        |kind: &str, summary: &str, evidence: String, impact: i64, confidence: u16, risk: &str| {
            recommendations.push(OptimizationRecommendation {
                id: RecommendationId::new(),
                kind: kind.into(),
                summary: summary.into(),
                evidence: vec![evidence],
                estimated_impact_millipoints: impact,
                confidence_millipoints: confidence,
                risks: vec![risk.into()],
                what_if: base_scenario.clone(),
            });
        };
    if aggregate.serial_work_millipoints >= 600 {
        add(
            "parallelization",
            "Evaluate independent serial blocks as bounded parallel work",
            format!(
                "serial_work_millipoints={}",
                aggregate.serial_work_millipoints
            ),
            250,
            750,
            "parallel effects may require new ordering invariants",
        );
    }
    if aggregate.retry_rate_millipoints >= 100 {
        add(
            "retry_tuning",
            "Test receipt-aware retry limits and backoff",
            format!(
                "retry_rate_millipoints={}",
                aggregate.retry_rate_millipoints
            ),
            120,
            800,
            "tighter retries can reduce recovery under transient faults",
        );
    }
    if aggregate.average_payload_bytes >= 1_048_576 {
        add(
            "payload_compaction",
            "Externalize or compact oversized boundary payloads",
            format!("average_payload_bytes={}", aggregate.average_payload_bytes),
            180,
            900,
            "external artifacts require retention and access-policy checks",
        );
    }
    if aggregate.average_cost_microunits > 0 {
        add(
            "provider_experiment",
            "Run a gated provider cost-quality what-if experiment",
            format!(
                "average_cost_microunits={}",
                aggregate.average_cost_microunits
            ),
            100,
            600,
            "lower cost must not bypass quality or residency gates",
        );
    }
    if aggregate.dead_branch_count > 0 {
        add(
            "dead_branch",
            "Review branches with no retained execution evidence",
            format!("dead_branch_count={}", aggregate.dead_branch_count),
            50,
            500,
            "absence of evidence may reflect incomplete sampling",
        );
    }
    recommendations
}

#[must_use]
pub fn choose_provider(
    candidates: &[ProviderCandidate],
    requirements: &ProviderRequirements,
    cohort_key: &str,
    now: DateTime<Utc>,
) -> ProviderDecision {
    let mut findings = Vec::new();
    let mut eligible: Vec<_> = candidates
        .iter()
        .filter(|candidate| {
            !candidate.breaker_open
                && (requirements.allowed_regions.is_empty()
                    || requirements.allowed_regions.contains(&candidate.region))
                && requirements
                    .max_price_microunits
                    .is_none_or(|limit| candidate.price_microunits <= limit)
                && requirements
                    .max_latency_ms
                    .is_none_or(|limit| candidate.expected_latency_ms <= limit)
                && requirements
                    .minimum_quality_millipoints
                    .is_none_or(|minimum| candidate.quality_millipoints >= minimum)
                && (!requirements.require_idempotency || candidate.supports_idempotency)
        })
        .cloned()
        .collect();
    eligible.sort_by(|left, right| {
        provider_score(right)
            .cmp(&provider_score(left))
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| left.model.cmp(&right.model))
    });
    if eligible.is_empty() {
        findings.push("NO_ELIGIBLE_PROVIDER".into());
    } else {
        findings.push("PROVIDER_POLICY_SATISFIED".into());
    }
    ProviderDecision {
        id: ProviderDecisionId::new(),
        selected: eligible.into_iter().next(),
        finding_codes: findings,
        cohort: short_hash(cohort_key.as_bytes()),
        created_at: now,
    }
}

#[must_use]
pub fn evaluation_gate(
    baseline_scores: &[i64],
    candidate_scores: &[i64],
    minimum_samples: usize,
    maximum_regression_millipoints: i64,
) -> EvidenceStatus {
    if baseline_scores.len() < minimum_samples || candidate_scores.len() < minimum_samples {
        return EvidenceStatus::Inconclusive;
    }
    let baseline = mean(baseline_scores);
    let candidate = mean(candidate_scores);
    if candidate.saturating_add(maximum_regression_millipoints) < baseline {
        EvidenceStatus::Fail
    } else {
        EvidenceStatus::Pass
    }
}

#[must_use]
pub fn assign_attention_task(
    task: &mut AttentionTask,
    reviewers: &[ReviewerCapabilities],
    now: DateTime<Utc>,
    lease_seconds: i64,
) -> Option<String> {
    if task.state != AttentionState::Pending || task.deadline <= now || lease_seconds <= 0 {
        return None;
    }
    let mut eligible: Vec<_> = reviewers
        .iter()
        .filter(|reviewer| {
            reviewer.tenant_ids.contains(&task.tenant_id)
                && task
                    .required_skills
                    .iter()
                    .all(|skill| reviewer.skills.contains(skill))
                && (task.allowed_regions.is_empty()
                    || task.allowed_regions.contains(&reviewer.region))
                && reviewer.available_attention_units >= task.estimated_attention_units
                && (matches!(
                    task.classification,
                    orch8_types::continuity::DataClassification::Public
                        | orch8_types::continuity::DataClassification::Internal
                ) || reviewer.trust >= RuntimeTrustLevel::Signed)
        })
        .collect();
    eligible.sort_by(|left, right| {
        right
            .available_attention_units
            .cmp(&left.available_attention_units)
            .then_with(|| left.reviewer_id.cmp(&right.reviewer_id))
    });
    let reviewer = eligible.first()?;
    task.assignee = Some(reviewer.reviewer_id.clone());
    task.lease_expires_at = Some(now + chrono::Duration::seconds(lease_seconds.min(86_400)));
    task.state = AttentionState::Assigned;
    task.assignee.clone()
}

pub fn verify_federation_envelope(
    peer: &FederationPeer,
    envelope: &FederationEnvelope,
    expected_payload: &[u8],
    now: DateTime<Utc>,
) -> Result<(), AdvancedContinuityError> {
    if envelope.issued_at > now || envelope.expires_at <= now {
        return Err(AdvancedContinuityError::FederationEnvelopeExpired);
    }
    if peer.revoked_at.is_some_and(|revoked| revoked <= now)
        || !peer.allowed_tenants.contains(&envelope.tenant_id)
    {
        return Err(AdvancedContinuityError::FederationPeerDenied);
    }
    if envelope.peer_id != peer.id || envelope.payload_sha256 != hex_sha256(expected_payload) {
        return Err(AdvancedContinuityError::FederationEnvelopeInvalid);
    }
    let key_bytes: [u8; 32] = BASE64
        .decode(&peer.public_key)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(AdvancedContinuityError::FederationEnvelopeInvalid)?;
    if hex_sha256(&key_bytes) != peer.trust_root_sha256 {
        return Err(AdvancedContinuityError::FederationEnvelopeInvalid);
    }
    let key = VerifyingKey::from_bytes(&key_bytes)
        .map_err(|_| AdvancedContinuityError::FederationEnvelopeInvalid)?;
    let signature_bytes: [u8; 64] = BASE64
        .decode(&envelope.signature)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(AdvancedContinuityError::FederationEnvelopeInvalid)?;
    key.verify(
        &federation_signing_bytes(envelope),
        &Signature::from_bytes(&signature_bytes),
    )
    .map_err(|_| AdvancedContinuityError::FederationEnvelopeInvalid)?;
    Ok(())
}

#[must_use]
pub fn evaluate_residency(
    classification: orch8_types::continuity::DataClassification,
    operation: impl Into<String>,
    source_region: Option<String>,
    destination_region: Option<String>,
    allowed_regions: &BTreeSet<String>,
    destination_trust: Option<RuntimeTrustLevel>,
) -> ResidencyEvidence {
    use orch8_types::continuity::DataClassification;

    let regulated = matches!(
        classification,
        DataClassification::Confidential | DataClassification::Restricted
    );
    let (outcome, finding) = match (&destination_region, destination_trust) {
        (None, _) if regulated => (EvidenceStatus::Fail, "DESTINATION_REGION_UNKNOWN"),
        (None, _) => (EvidenceStatus::Unknown, "DESTINATION_REGION_UNKNOWN"),
        (Some(region), _) if !allowed_regions.is_empty() && !allowed_regions.contains(region) => {
            (EvidenceStatus::Fail, "DESTINATION_REGION_DENIED")
        }
        (_, None) if regulated => (EvidenceStatus::Fail, "DESTINATION_TRUST_UNKNOWN"),
        (_, Some(trust)) if regulated && trust < RuntimeTrustLevel::Signed => {
            (EvidenceStatus::Fail, "DESTINATION_TRUST_TOO_LOW")
        }
        _ => (EvidenceStatus::Pass, "RESIDENCY_POLICY_SATISFIED"),
    };
    ResidencyEvidence {
        classification,
        operation: operation.into(),
        source_region,
        destination_region,
        outcome,
        finding_codes: vec![finding.into()],
    }
}

pub fn validate_device_delegation(
    delegation: &DeviceDelegation,
    grant: &orch8_types::continuity::ContinuationGrant,
    now: DateTime<Utc>,
) -> Result<(), AdvancedContinuityError> {
    if delegation.source_runtime_id == delegation.destination_runtime_id
        || delegation.expires_at <= now
        || delegation.expires_at > grant.expires_at
        || delegation.grant_id != grant.id
        || delegation.tenant_id != grant.tenant_id
        || delegation.parent_continuity_id != grant.continuity_id
        || delegation.parent_epoch != grant.expected_epoch
        || delegation.destination_runtime_id != grant.destination_runtime_id
        || grant
            .validate_claim(
                now,
                &delegation.tenant_id,
                delegation.parent_continuity_id,
                delegation.parent_epoch,
                delegation.destination_runtime_id,
                orch8_types::continuity::GrantAction::Accept,
            )
            .is_err()
    {
        return Err(AdvancedContinuityError::DelegationDenied);
    }
    Ok(())
}

#[must_use]
pub fn minimize_disclosure(
    payload: &serde_json::Value,
    allowed_top_level_fields: &BTreeSet<String>,
    classification: orch8_types::continuity::DataClassification,
) -> DisclosureResult {
    use orch8_types::continuity::DataClassification;

    if classification == DataClassification::Public {
        return DisclosureResult {
            disclosed: payload.clone(),
            withheld_sha256: Vec::new(),
            classification,
        };
    }
    let mut disclosed = serde_json::Map::new();
    let mut withheld_sha256 = Vec::new();
    if let Some(object) = payload.as_object() {
        for (key, value) in object.iter().take(256) {
            if allowed_top_level_fields.contains(key) {
                disclosed.insert(key.clone(), value.clone());
            } else if let Ok(encoded) = serde_json::to_vec(value) {
                withheld_sha256.push(hex_sha256(&encoded));
            }
        }
    } else if let Ok(encoded) = serde_json::to_vec(payload) {
        withheld_sha256.push(hex_sha256(&encoded));
    }
    withheld_sha256.sort();
    DisclosureResult {
        disclosed: serde_json::Value::Object(disclosed),
        withheld_sha256,
        classification,
    }
}

pub fn federation_signing_bytes(envelope: &FederationEnvelope) -> Vec<u8> {
    let mut unsigned = envelope.clone();
    unsigned.signature.clear();
    serde_json::to_vec(&unsigned).expect("federation envelope serialization is infallible")
}

fn provider_score(candidate: &ProviderCandidate) -> i128 {
    i128::from(candidate.quality_millipoints) * 1_000_000
        - i128::from(candidate.price_microunits) * 1_000
        - i128::from(candidate.expected_latency_ms)
}

fn mean(values: &[i64]) -> i64 {
    values
        .iter()
        .fold(0_i128, |sum, value| sum + i128::from(*value))
        .checked_div(values.len() as i128)
        .and_then(|mean| i64::try_from(mean).ok())
        .unwrap_or_default()
}

fn usage_has_negative(usage: BudgetUsage) -> bool {
    usage.cost_microunits < 0
        || usage.wall_time_ms < 0
        || usage.external_calls < 0
        || usage.bytes_transferred < 0
        || usage.energy_millijoules < 0
        || usage.attention_units < 0
}

fn add_usage(left: BudgetUsage, right: BudgetUsage) -> BudgetUsage {
    BudgetUsage {
        cost_microunits: left.cost_microunits.saturating_add(right.cost_microunits),
        wall_time_ms: left.wall_time_ms.saturating_add(right.wall_time_ms),
        external_calls: left.external_calls.saturating_add(right.external_calls),
        bytes_transferred: left
            .bytes_transferred
            .saturating_add(right.bytes_transferred),
        energy_millijoules: left
            .energy_millijoules
            .saturating_add(right.energy_millijoules),
        attention_units: left.attention_units.saturating_add(right.attention_units),
    }
}

fn sub_usage(reserved: BudgetUsage, actual: BudgetUsage) -> BudgetUsage {
    BudgetUsage {
        cost_microunits: reserved
            .cost_microunits
            .saturating_sub(actual.cost_microunits),
        wall_time_ms: reserved.wall_time_ms.saturating_sub(actual.wall_time_ms),
        external_calls: reserved
            .external_calls
            .saturating_sub(actual.external_calls),
        bytes_transferred: reserved
            .bytes_transferred
            .saturating_sub(actual.bytes_transferred),
        energy_millijoules: reserved
            .energy_millijoules
            .saturating_sub(actual.energy_millijoules),
        attention_units: reserved
            .attention_units
            .saturating_sub(actual.attention_units),
    }
}

fn short_hash(bytes: &[u8]) -> String {
    hex_sha256(bytes)[..16].to_owned()
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use ed25519_dalek::{Signer, SigningKey};
    use orch8_types::continuity::{ContinuityId, ExecutionEpoch};
    use orch8_types::continuity_advanced::{
        BudgetReservationId, FaultKind, FaultPoint, ReservationState,
    };
    use orch8_types::ids::TenantId;

    use super::*;

    #[test]
    fn versioned_state_transforms_are_pure_and_deterministic() {
        let source = serde_json::json!({
            "profile": {"name": "Ada", "legacy_id": 7},
            "keep": true
        });
        let transforms = vec![
            StateTransform {
                version: 1,
                from_path: "profile.name".into(),
                to_path: "identity.display_name".into(),
                transform: "copy".into(),
            },
            StateTransform {
                version: 1,
                from_path: "profile.legacy_id".into(),
                to_path: "identity.id".into(),
                transform: "move".into(),
            },
        ];

        let first = apply_state_transforms(&source, &transforms).unwrap();
        let second = apply_state_transforms(&source, &transforms).unwrap();
        assert_eq!(first, second);
        assert_eq!(source["profile"]["legacy_id"], 7, "source is immutable");
        assert_eq!(first["identity"]["display_name"], "Ada");
        assert_eq!(first["identity"]["id"], 7);
        assert!(first["profile"].get("legacy_id").is_none());
        assert_eq!(first["keep"], true);
    }

    #[test]
    fn state_transforms_reject_unknown_versions_operations_and_destinations() {
        let invalid = [
            StateTransform {
                version: 2,
                from_path: "a".into(),
                to_path: "b".into(),
                transform: "copy".into(),
            },
            StateTransform {
                version: 1,
                from_path: "a".into(),
                to_path: "b".into(),
                transform: "javascript".into(),
            },
        ];
        for transform in invalid {
            assert!(matches!(
                validate_state_transforms(&[transform]),
                Err(AdvancedContinuityError::InvalidTransform(_))
            ));
        }
        let duplicate_destination = vec![
            StateTransform {
                version: 1,
                from_path: "a".into(),
                to_path: "target".into(),
                transform: "copy".into(),
            },
            StateTransform {
                version: 1,
                from_path: "b".into(),
                to_path: "target".into(),
                transform: "move".into(),
            },
        ];
        assert!(matches!(
            validate_state_transforms(&duplicate_destination),
            Err(AdvancedContinuityError::InvalidTransform(_))
        ));
    }

    fn migration_seed() -> LiveMigrationPlan {
        LiveMigrationPlan {
            id: MigrationPlanId::new(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            continuity_id: ContinuityId::new(),
            from_sequence_id: orch8_types::ids::SequenceId::new(),
            from_version: 1,
            to_sequence_id: orch8_types::ids::SequenceId::new(),
            to_version: 2,
            disposition: MigrationDisposition::Pin,
            transforms: Vec::new(),
            finding_codes: Vec::new(),
            rollback_capsule_required: false,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn migration_compiler_fails_closed_on_server_semantic_incompatibility() {
        let incompatible = compile_migration_plan(
            migration_seed(),
            &["INCOMPATIBLE:OUTPUT_REFERENCE_DANGLING".into()],
            Vec::new(),
            Some(true),
        )
        .unwrap();
        assert_eq!(incompatible.disposition, MigrationDisposition::Incompatible);
        assert!(incompatible.rollback_capsule_required);

        let automatic =
            compile_migration_plan(migration_seed(), &[], Vec::new(), Some(true)).unwrap();
        assert_eq!(automatic.disposition, MigrationDisposition::Automatic);
    }

    #[test]
    fn scenario_generation_is_bounded_and_incident_shrinking_is_deterministic() {
        let faults = vec![
            FaultInjection {
                point: FaultPoint::OwnershipClaim,
                kind: FaultKind::StaleOwner,
                occurrence: 1,
            },
            FaultInjection {
                point: FaultPoint::StorageAfterWrite,
                kind: FaultKind::DatabaseTimeout,
                occurrence: 1,
            },
        ];
        let scenarios = generate_scenarios(&["a".into(), "b".into()], &faults, 300, 7).unwrap();
        assert_eq!(scenarios.len(), 256);
        let mut scenario = scenarios[0].clone();
        scenario.faults = faults;
        let result = minimize_reproducing_scenario(scenario, |candidate| {
            candidate
                .faults
                .iter()
                .any(|fault| fault.kind == FaultKind::StaleOwner)
        });
        assert_eq!(result.status, EvidenceStatus::Pass);
        assert_eq!(result.scenario.unwrap().faults.len(), 1);
    }

    #[test]
    fn hard_budget_reservations_prevent_check_then_act_overspend() {
        let budget = Budget {
            max_cost_microunits: Some(100),
            ..Budget::default()
        };
        let mut reservation = BudgetReservation {
            id: BudgetReservationId::new(),
            tenant_id: TenantId::new("tenant-a").unwrap(),
            continuity_id: ContinuityId::new(),
            epoch: ExecutionEpoch::initial(),
            requested: BudgetUsage {
                cost_microunits: 30,
                ..BudgetUsage::default()
            },
            actual: None,
            estimation_version: "prices-1".into(),
            state: ReservationState::Released,
            created_at: Utc::now(),
        };
        assert_eq!(
            reserve_budget(
                &budget,
                &mut reservation,
                BudgetUsage {
                    cost_microunits: 80,
                    ..BudgetUsage::default()
                }
            ),
            Err(AdvancedContinuityError::BudgetDenied("max_cost_microunits"))
        );
    }

    #[test]
    fn provider_and_eval_policy_fail_closed() {
        let now = Utc::now();
        let candidates = vec![ProviderCandidate {
            provider: "a".into(),
            model: "m".into(),
            region: "br".into(),
            price_microunits: 10,
            expected_latency_ms: 20,
            quality_millipoints: 900,
            breaker_open: false,
            supports_idempotency: true,
            pricing_version: "v1".into(),
        }];
        let decision = choose_provider(
            &candidates,
            &ProviderRequirements {
                allowed_regions: BTreeSet::from(["br".into()]),
                max_price_microunits: Some(20),
                max_latency_ms: Some(100),
                minimum_quality_millipoints: Some(800),
                require_idempotency: true,
            },
            "tenant:release:cohort",
            now,
        );
        assert_eq!(decision.selected.unwrap().provider, "a");
        assert_eq!(
            evaluation_gate(&[900], &[950], 2, 0),
            EvidenceStatus::Inconclusive
        );
        assert_eq!(
            evaluation_gate(&[900, 900], &[700, 700], 2, 50),
            EvidenceStatus::Fail
        );
    }

    #[test]
    fn federation_requires_tenant_authority_digest_and_freshness() {
        let now = Utc::now();
        let tenant = TenantId::new("tenant-a").unwrap();
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let public_key = signing_key.verifying_key().to_bytes();
        let peer = FederationPeer {
            id: orch8_types::continuity_advanced::FederationPeerId::new(),
            name: "peer".into(),
            trust_root_sha256: hex_sha256(&public_key),
            public_key: BASE64.encode(public_key),
            endpoint: "https://peer.invalid".into(),
            allowed_tenants: vec![tenant.clone()],
            revoked_at: None,
        };
        let payload = b"ciphertext";
        let mut envelope = FederationEnvelope {
            id: orch8_types::continuity_advanced::FederationMessageId::new(),
            peer_id: peer.id,
            tenant_id: tenant,
            continuity_id: ContinuityId::new(),
            epoch: ExecutionEpoch::initial(),
            destination_runtime_id: orch8_types::continuity::RuntimeId::new(),
            payload_sha256: hex_sha256(payload),
            issued_at: now,
            expires_at: now + Duration::minutes(5),
            signature: String::new(),
        };
        envelope.signature = BASE64.encode(
            signing_key
                .sign(&federation_signing_bytes(&envelope))
                .to_bytes(),
        );
        verify_federation_envelope(&peer, &envelope, payload, now).unwrap();
        assert_eq!(
            verify_federation_envelope(&peer, &envelope, b"tampered", now),
            Err(AdvancedContinuityError::FederationEnvelopeInvalid)
        );
    }

    #[test]
    fn regulated_residency_fails_closed_and_disclosure_withholds_raw_fields() {
        let evidence = evaluate_residency(
            orch8_types::continuity::DataClassification::Restricted,
            "capsule_transfer",
            Some("br".into()),
            None,
            &BTreeSet::from(["br".into()]),
            None,
        );
        assert_eq!(evidence.outcome, EvidenceStatus::Fail);

        let result = minimize_disclosure(
            &serde_json::json!({"summary":"ok","raw_medical_record":"secret"}),
            &BTreeSet::from(["summary".into()]),
            orch8_types::continuity::DataClassification::Restricted,
        );
        assert_eq!(result.disclosed, serde_json::json!({"summary":"ok"}));
        assert_eq!(result.withheld_sha256.len(), 1);
        assert!(!result.disclosed.to_string().contains("secret"));
    }
}
