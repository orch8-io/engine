//! Deterministic capability routing and bounded locality-policy evaluation.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityId, DataClassification, ExecutionEpoch, LocalityPolicy,
    LocalityRule, PlacementDecision, PlacementDecisionId, PlacementEvidence, PlacementScoreFactors,
    PolicyOutcome, RuntimeCapabilities, RuntimeId, RuntimeTrustLevel,
};
use orch8_types::ids::TenantId;
use thiserror::Error;

use crate::continuity::{CompatibilityStatus, assess_compatibility};

pub const MAX_POLICY_RULES: usize = 256;
pub const MAX_POLICY_FACTS_PER_RULE: usize = 64;
pub const MAX_POLICY_FACT_LENGTH: usize = 256;
pub const MAX_CANDIDATES: usize = 1_000;
pub const MAX_REQUIREMENT_FACTS_PER_KIND: usize = 64;
pub const MAX_REQUIREMENT_FACT_LENGTH: usize = 256;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RequirementsValidationError {
    #[error("a placement requirement exceeds {MAX_REQUIREMENT_FACTS_PER_KIND} facts")]
    TooManyFacts,
    #[error("a placement requirement contains an empty fact")]
    EmptyFact,
    #[error("a placement requirement fact exceeds {MAX_REQUIREMENT_FACT_LENGTH} bytes")]
    FactTooLong,
}

pub fn validate_requirements(
    requirements: &CapsuleRequirements,
) -> Result<(), RequirementsValidationError> {
    let fact_groups = [
        requirements.handlers.as_slice(),
        requirements.plugins.as_slice(),
        requirements.credentials.as_slice(),
        requirements.regions.as_slice(),
        requirements.hardware.as_slice(),
    ];
    if fact_groups
        .iter()
        .any(|facts| facts.len() > MAX_REQUIREMENT_FACTS_PER_KIND)
    {
        return Err(RequirementsValidationError::TooManyFacts);
    }
    if fact_groups
        .iter()
        .any(|facts| facts.iter().any(|fact| fact.trim().is_empty()))
    {
        return Err(RequirementsValidationError::EmptyFact);
    }
    if fact_groups.iter().any(|facts| {
        facts
            .iter()
            .any(|fact| fact.len() > MAX_REQUIREMENT_FACT_LENGTH)
    }) {
        return Err(RequirementsValidationError::FactTooLong);
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyValidationError {
    #[error("locality policy version must be non-zero")]
    MissingVersion,
    #[error("locality policy exceeds {MAX_POLICY_RULES} rules")]
    TooManyRules,
    #[error("locality policy contains an empty region or hardware capability")]
    EmptyFact,
    #[error("a locality policy rule exceeds {MAX_POLICY_FACTS_PER_RULE} facts")]
    TooManyFacts,
    #[error("a locality policy string fact exceeds {MAX_POLICY_FACT_LENGTH} bytes")]
    FactTooLong,
    #[error("locality policy battery percentage must be at most 100")]
    InvalidBatteryPercentage,
    #[error("locality policy has contradictory region rules for {0:?}")]
    ContradictoryRegions(DataClassification),
    #[error("locality policy has contradictory runtime rules for {0:?}")]
    ContradictoryRuntimes(DataClassification),
    #[error("locality policy has contradictory runtime-kind rules for {0:?}")]
    ContradictoryRuntimeKinds(DataClassification),
    #[error("locality policy has contradictory connectivity rules for {0:?}")]
    ContradictoryConnectivity(DataClassification),
}

fn has_empty_intersection<T: Ord + Clone>(mut sets: impl Iterator<Item = BTreeSet<T>>) -> bool {
    let Some(mut intersection) = sets.next() else {
        return false;
    };
    for values in sets {
        intersection = intersection.intersection(&values).cloned().collect();
    }
    intersection.is_empty()
}

pub fn validate_policy(policy: &LocalityPolicy) -> Result<(), PolicyValidationError> {
    if policy.version == 0 {
        return Err(PolicyValidationError::MissingVersion);
    }
    if policy.rules.len() > MAX_POLICY_RULES {
        return Err(PolicyValidationError::TooManyRules);
    }
    if policy.rules.iter().any(|rule| {
        rule.allowed_runtime_ids.len() > MAX_POLICY_FACTS_PER_RULE
            || rule.allowed_runtime_kinds.len() > MAX_POLICY_FACTS_PER_RULE
            || rule.allowed_regions.len() > MAX_POLICY_FACTS_PER_RULE
            || rule.allowed_connectivity.len() > MAX_POLICY_FACTS_PER_RULE
    }) {
        return Err(PolicyValidationError::TooManyFacts);
    }
    if policy.rules.iter().any(|rule| {
        rule.allowed_regions
            .iter()
            .any(|value| value.trim().is_empty())
            || rule
                .require_hardware
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
    }) {
        return Err(PolicyValidationError::EmptyFact);
    }
    if policy.rules.iter().any(|rule| {
        rule.allowed_regions
            .iter()
            .any(|value| value.len() > MAX_POLICY_FACT_LENGTH)
            || rule
                .require_hardware
                .as_ref()
                .is_some_and(|value| value.len() > MAX_POLICY_FACT_LENGTH)
    }) {
        return Err(PolicyValidationError::FactTooLong);
    }
    if policy.rules.iter().any(|rule| {
        rule.minimum_battery_percent
            .is_some_and(|value| value > 100)
    }) {
        return Err(PolicyValidationError::InvalidBatteryPercentage);
    }
    for classification in [
        DataClassification::Public,
        DataClassification::Internal,
        DataClassification::Confidential,
        DataClassification::Restricted,
    ] {
        let region_sets = policy
            .rules
            .iter()
            .filter(|rule| rule.classification == classification)
            .filter(|rule| !rule.allowed_regions.is_empty())
            .map(|rule| {
                rule.allowed_regions
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>()
            });
        if has_empty_intersection(region_sets) {
            return Err(PolicyValidationError::ContradictoryRegions(classification));
        }
        let runtime_sets = policy
            .rules
            .iter()
            .filter(|rule| rule.classification == classification)
            .filter(|rule| !rule.allowed_runtime_ids.is_empty())
            .map(|rule| {
                rule.allowed_runtime_ids
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
            });
        if has_empty_intersection(runtime_sets) {
            return Err(PolicyValidationError::ContradictoryRuntimes(classification));
        }
        let kind_sets = policy
            .rules
            .iter()
            .filter(|rule| rule.classification == classification)
            .filter(|rule| !rule.allowed_runtime_kinds.is_empty())
            .map(|rule| rule.allowed_runtime_kinds.iter().copied().collect());
        if has_empty_intersection(kind_sets) {
            return Err(PolicyValidationError::ContradictoryRuntimeKinds(
                classification,
            ));
        }
        let connectivity_sets = policy
            .rules
            .iter()
            .filter(|rule| rule.classification == classification)
            .filter(|rule| !rule.allowed_connectivity.is_empty())
            .map(|rule| rule.allowed_connectivity.iter().copied().collect());
        if has_empty_intersection(connectivity_sets) {
            return Err(PolicyValidationError::ContradictoryConnectivity(
                classification,
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvaluation {
    pub outcome: PolicyOutcome,
    pub finding_codes: Vec<String>,
}

#[must_use]
pub fn evaluate_locality(
    policy: Option<&LocalityPolicy>,
    classification: DataClassification,
    runtime: &RuntimeCapabilities,
) -> PolicyEvaluation {
    let Some(policy) = policy else {
        return match classification {
            DataClassification::Public | DataClassification::Internal => PolicyEvaluation {
                outcome: PolicyOutcome::Allow,
                finding_codes: vec!["POLICY_DEFAULT_CURRENT_BOUNDARY".into()],
            },
            DataClassification::Confidential => PolicyEvaluation {
                outcome: PolicyOutcome::Unknown,
                finding_codes: vec!["CONFIDENTIAL_POLICY_MISSING".into()],
            },
            DataClassification::Restricted => PolicyEvaluation {
                outcome: PolicyOutcome::Deny,
                finding_codes: vec!["RESTRICTED_POLICY_MISSING".into()],
            },
        };
    };
    let rules: Vec<_> = policy
        .rules
        .iter()
        .filter(|rule| rule.classification == classification)
        .collect();
    if rules.is_empty() {
        return evaluate_locality(None, classification, runtime);
    }

    let mut codes: Vec<String> = Vec::new();
    let mut unknown = false;
    for rule in rules {
        evaluate_identity_and_residency(rule, runtime, &mut unknown, &mut codes);
        evaluate_runtime_environment(rule, runtime, &mut unknown, &mut codes);
    }
    codes.sort();
    codes.dedup();
    let denied = codes.iter().any(|code| code.ends_with("_DENIED"));
    let outcome = if denied {
        PolicyOutcome::Deny
    } else if unknown {
        PolicyOutcome::Unknown
    } else {
        PolicyOutcome::Allow
    };
    if codes.is_empty() {
        codes.push("POLICY_ALLOW".into());
    }
    PolicyEvaluation {
        outcome,
        finding_codes: codes,
    }
}

fn evaluate_identity_and_residency(
    rule: &LocalityRule,
    runtime: &RuntimeCapabilities,
    unknown: &mut bool,
    codes: &mut Vec<String>,
) {
    if !rule.allowed_runtime_ids.is_empty()
        && !rule.allowed_runtime_ids.contains(&runtime.runtime_id)
    {
        codes.push("RUNTIME_ID_DENIED".into());
    }
    if !rule.allowed_runtime_kinds.is_empty() && !rule.allowed_runtime_kinds.contains(&runtime.kind)
    {
        codes.push("RUNTIME_KIND_DENIED".into());
    }
    if !rule.allowed_regions.is_empty() {
        if runtime.regions.is_empty() {
            *unknown = true;
            codes.push("RUNTIME_REGION_UNKNOWN".into());
        } else if !rule
            .allowed_regions
            .iter()
            .any(|region| runtime.regions.contains(region))
        {
            codes.push("REGION_DENIED".into());
        }
    }
    if let Some(minimum) = rule.minimum_trust
        && runtime.trust < minimum
    {
        codes.push("TRUST_DENIED".into());
    }
}

fn evaluate_runtime_environment(
    rule: &LocalityRule,
    runtime: &RuntimeCapabilities,
    unknown: &mut bool,
    codes: &mut Vec<String>,
) {
    if let Some(required) = rule.require_offline
        && runtime.offline_capable != required
    {
        codes.push("CONNECTIVITY_DENIED".into());
    }
    if let Some(hardware) = &rule.require_hardware
        && !runtime.hardware.contains(hardware)
    {
        codes.push("HARDWARE_DENIED".into());
    }
    if !rule.allowed_connectivity.is_empty() {
        match runtime.connectivity {
            Some(connectivity) if rule.allowed_connectivity.contains(&connectivity) => {}
            Some(_) => codes.push("CONNECTIVITY_DENIED".into()),
            None => {
                *unknown = true;
                codes.push("RUNTIME_CONNECTIVITY_UNKNOWN".into());
            }
        }
    }
    compare_maximum(
        runtime.estimated_cost_microunits,
        rule.maximum_cost_microunits,
        "RUNTIME_COST_UNKNOWN",
        "COST_DENIED",
        unknown,
        codes,
    );
    compare_maximum(
        runtime.estimated_latency_ms,
        rule.maximum_latency_ms,
        "RUNTIME_LATENCY_UNKNOWN",
        "LATENCY_DENIED",
        unknown,
        codes,
    );
    if let Some(minimum) = rule.minimum_battery_percent {
        match runtime.battery_percent {
            Some(actual) if actual >= minimum => {}
            Some(_) => codes.push("BATTERY_DENIED".into()),
            None => {
                *unknown = true;
                codes.push("RUNTIME_BATTERY_UNKNOWN".into());
            }
        }
    }
}

fn compare_maximum(
    actual: Option<u64>,
    maximum: Option<u64>,
    unknown_code: &str,
    denied_code: &str,
    unknown: &mut bool,
    codes: &mut Vec<String>,
) {
    let Some(maximum) = maximum else {
        return;
    };
    match actual {
        Some(actual) if actual <= maximum => {}
        Some(_) => codes.push(denied_code.into()),
        None => {
            *unknown = true;
            codes.push(unknown_code.into());
        }
    }
}

fn trust_score(trust: RuntimeTrustLevel) -> i64 {
    match trust {
        RuntimeTrustLevel::Unverified => 0,
        RuntimeTrustLevel::Registered => 10,
        RuntimeTrustLevel::Signed => 20,
        RuntimeTrustLevel::Attested => 30,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn choose_runtime(
    tenant_id: TenantId,
    continuity_id: ContinuityId,
    epoch: ExecutionEpoch,
    requirements: &CapsuleRequirements,
    policy: Option<&LocalityPolicy>,
    classification: DataClassification,
    candidates: &[RuntimeCapabilities],
    current_runtime: Option<RuntimeId>,
    now: DateTime<Utc>,
) -> PlacementDecision {
    let bounded_candidates = &candidates[..candidates.len().min(MAX_CANDIDATES)];
    let battery_ranks = rank_candidate_metric(
        bounded_candidates,
        |runtime| runtime.battery_percent.map(u64::from),
        true,
    );
    let cost_ranks = rank_candidate_metric(
        bounded_candidates,
        |runtime| runtime.estimated_cost_microunits,
        false,
    );
    let latency_ranks = rank_candidate_metric(
        bounded_candidates,
        |runtime| runtime.estimated_latency_ms,
        false,
    );
    let mut evidence: Vec<_> = candidates
        .iter()
        .take(MAX_CANDIDATES)
        .map(|runtime| {
            let compatibility = assess_compatibility(requirements, runtime, now);
            let compatibility_denied = compatibility
                .iter()
                .any(|finding| finding.status == CompatibilityStatus::Fail);
            let compatibility_unknown = compatibility
                .iter()
                .any(|finding| finding.status == CompatibilityStatus::Unknown);
            let locality = evaluate_locality(policy, classification, runtime);
            let outcome = if compatibility_denied || locality.outcome == PolicyOutcome::Deny {
                PolicyOutcome::Deny
            } else if compatibility_unknown || locality.outcome == PolicyOutcome::Unknown {
                PolicyOutcome::Unknown
            } else {
                PolicyOutcome::Allow
            };
            let mut finding_codes: Vec<String> = compatibility
                .into_iter()
                .map(|finding| finding.code.to_owned())
                .chain(locality.finding_codes)
                .collect();
            finding_codes.sort();
            finding_codes.dedup();
            let score_factors = PlacementScoreFactors {
                trust: trust_score(runtime.trust),
                current_runtime: i64::from(current_runtime == Some(runtime.runtime_id)) * 5,
                offline_capable: i64::from(runtime.offline_capable) * 3,
                battery_rank: battery_ranks
                    .get(&runtime.runtime_id)
                    .copied()
                    .unwrap_or_default(),
                cost_rank: cost_ranks
                    .get(&runtime.runtime_id)
                    .copied()
                    .unwrap_or_default(),
                latency_rank: latency_ranks
                    .get(&runtime.runtime_id)
                    .copied()
                    .unwrap_or_default(),
            };
            let score = score_factors.trust
                + score_factors.current_runtime
                + score_factors.offline_capable
                + score_factors.battery_rank
                + score_factors.cost_rank
                + score_factors.latency_rank;
            PlacementEvidence {
                runtime_id: runtime.runtime_id,
                outcome,
                score,
                score_factors,
                finding_codes,
            }
        })
        .collect();
    evidence.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.runtime_id.cmp(&right.runtime_id))
    });
    let selected_runtime_id = evidence
        .iter()
        .find(|candidate| candidate.outcome == PolicyOutcome::Allow)
        .map(|candidate| candidate.runtime_id);
    PlacementDecision {
        id: PlacementDecisionId::new(),
        tenant_id,
        continuity_id,
        epoch,
        selected_runtime_id,
        requirements: requirements.clone(),
        policy: policy.cloned(),
        classification,
        policy_version: policy.map(|value| value.version),
        candidates: evidence,
        created_at: now,
    }
}

fn rank_candidate_metric(
    candidates: &[RuntimeCapabilities],
    value: impl Fn(&RuntimeCapabilities) -> Option<u64>,
    prefer_high: bool,
) -> BTreeMap<RuntimeId, i64> {
    let mut known: Vec<(RuntimeId, u64)> = candidates
        .iter()
        .filter_map(|runtime| value(runtime).map(|metric| (runtime.runtime_id, metric)))
        .collect();
    known.sort_by(|left, right| {
        let order = left.1.cmp(&right.1);
        let order = if prefer_high { order.reverse() } else { order };
        order.then_with(|| left.0.cmp(&right.0))
    });
    let count = known.len();
    let mut scores = BTreeMap::new();
    let mut previous_value = None;
    let mut previous_score = 0;
    for (index, (runtime_id, metric)) in known.into_iter().enumerate() {
        let score = if previous_value == Some(metric) {
            previous_score
        } else {
            i64::try_from((count - index) * 10 / count).unwrap_or(10)
        };
        scores.insert(runtime_id, score);
        previous_value = Some(metric);
        previous_score = score;
    }
    scores
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use orch8_types::continuity::{LocalityRule, RuntimeConnectivity, RuntimeKind};

    use super::*;

    fn runtime(id: RuntimeId, region: &str, trust: RuntimeTrustLevel) -> RuntimeCapabilities {
        let now = Utc::now();
        RuntimeCapabilities {
            runtime_id: id,
            kind: RuntimeKind::Mobile,
            trust,
            handlers: vec!["camera".into()],
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: vec![region.into()],
            hardware: vec!["camera".into()],
            offline_capable: true,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            observed_at: now,
            expires_at: now + Duration::minutes(1),
        }
    }

    fn locality_rule(classification: DataClassification) -> LocalityRule {
        LocalityRule {
            classification,
            allowed_runtime_ids: Vec::new(),
            allowed_runtime_kinds: Vec::new(),
            allowed_regions: Vec::new(),
            minimum_trust: None,
            require_offline: None,
            require_hardware: None,
            allowed_connectivity: Vec::new(),
            minimum_battery_percent: None,
            maximum_cost_microunits: None,
            maximum_latency_ms: None,
        }
    }

    #[test]
    fn restricted_data_fails_closed_without_policy() {
        let result = evaluate_locality(
            None,
            DataClassification::Restricted,
            &runtime(RuntimeId::new(), "br-south", RuntimeTrustLevel::Attested),
        );
        assert_eq!(result.outcome, PolicyOutcome::Deny);
    }

    #[test]
    fn contradictory_policy_is_rejected() {
        let policy = LocalityPolicy {
            version: 1,
            rules: vec![
                LocalityRule {
                    allowed_regions: vec!["br-south".into()],
                    ..locality_rule(DataClassification::Restricted)
                },
                LocalityRule {
                    allowed_regions: vec!["eu-west".into()],
                    ..locality_rule(DataClassification::Restricted)
                },
            ],
        };
        assert_eq!(
            validate_policy(&policy),
            Err(PolicyValidationError::ContradictoryRegions(
                DataClassification::Restricted
            ))
        );
    }

    #[test]
    fn contradictory_connectivity_policy_is_rejected() {
        let policy = LocalityPolicy {
            version: 1,
            rules: vec![
                LocalityRule {
                    allowed_connectivity: vec![RuntimeConnectivity::Wifi],
                    ..locality_rule(DataClassification::Confidential)
                },
                LocalityRule {
                    allowed_connectivity: vec![RuntimeConnectivity::Offline],
                    ..locality_rule(DataClassification::Confidential)
                },
            ],
        };
        assert_eq!(
            validate_policy(&policy),
            Err(PolicyValidationError::ContradictoryConnectivity(
                DataClassification::Confidential
            ))
        );
    }

    #[test]
    fn placement_requirements_are_bounded() {
        let requirements = CapsuleRequirements {
            handlers: vec!["handler".into(); MAX_REQUIREMENT_FACTS_PER_KIND + 1],
            ..CapsuleRequirements::default()
        };
        assert_eq!(
            validate_requirements(&requirements),
            Err(RequirementsValidationError::TooManyFacts)
        );
    }

    #[test]
    fn placement_filters_hard_requirements_and_breaks_ties_deterministically() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let eligible = RuntimeId::from_uuid(uuid::Uuid::from_u128(1));
        let wrong_region = RuntimeId::from_uuid(uuid::Uuid::from_u128(2));
        let policy = LocalityPolicy {
            version: 1,
            rules: vec![LocalityRule {
                allowed_regions: vec!["br-south".into()],
                minimum_trust: Some(RuntimeTrustLevel::Signed),
                require_offline: Some(true),
                require_hardware: Some("camera".into()),
                ..locality_rule(DataClassification::Restricted)
            }],
        };
        let decision = choose_runtime(
            tenant,
            ContinuityId::new(),
            ExecutionEpoch::initial(),
            &CapsuleRequirements {
                handlers: vec!["camera".into()],
                minimum_trust: Some(RuntimeTrustLevel::Signed),
                ..CapsuleRequirements::default()
            },
            Some(&policy),
            DataClassification::Restricted,
            &[
                runtime(wrong_region, "eu-west", RuntimeTrustLevel::Attested),
                runtime(eligible, "br-south", RuntimeTrustLevel::Signed),
            ],
            None,
            Utc::now(),
        );
        assert_eq!(decision.selected_runtime_id, Some(eligible));
        assert_eq!(decision.candidates.len(), 2);
    }

    #[test]
    fn stale_or_draining_camera_runtime_cannot_receive_work() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let now = Utc::now();
        let mut expired = runtime(RuntimeId::new(), "br-south", RuntimeTrustLevel::Registered);
        expired.expires_at = now;
        let mut draining = runtime(RuntimeId::new(), "br-south", RuntimeTrustLevel::Registered);
        draining.draining = true;
        let decision = choose_runtime(
            tenant,
            ContinuityId::new(),
            ExecutionEpoch::initial(),
            &CapsuleRequirements {
                handlers: vec!["camera".into()],
                ..CapsuleRequirements::default()
            },
            None,
            DataClassification::Internal,
            &[expired, draining],
            None,
            now,
        );
        assert_eq!(decision.selected_runtime_id, None);
        assert!(decision.candidates.iter().all(|candidate| {
            candidate.outcome == PolicyOutcome::Deny
                && candidate
                    .finding_codes
                    .iter()
                    .any(|code| code == "CAPABILITIES_EXPIRED" || code == "RUNTIME_DRAINING")
        }));
    }

    #[test]
    fn restricted_data_can_be_pinned_to_one_device() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let pinned = RuntimeId::new();
        let other = RuntimeId::new();
        let policy = LocalityPolicy {
            version: 7,
            rules: vec![LocalityRule {
                allowed_runtime_ids: vec![pinned],
                ..locality_rule(DataClassification::Restricted)
            }],
        };
        let decision = choose_runtime(
            tenant,
            ContinuityId::new(),
            ExecutionEpoch::initial(),
            &CapsuleRequirements::default(),
            Some(&policy),
            DataClassification::Restricted,
            &[
                runtime(other, "br-south", RuntimeTrustLevel::Registered),
                runtime(pinned, "br-south", RuntimeTrustLevel::Registered),
            ],
            None,
            Utc::now(),
        );
        assert_eq!(decision.selected_runtime_id, Some(pinned));
    }

    #[test]
    fn cloud_inference_requires_wifi_and_bounded_cost() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let eligible = RuntimeId::new();
        let metered = RuntimeId::new();
        let policy = LocalityPolicy {
            version: 3,
            rules: vec![LocalityRule {
                allowed_connectivity: vec![RuntimeConnectivity::Wifi],
                maximum_cost_microunits: Some(50),
                maximum_latency_ms: Some(250),
                ..locality_rule(DataClassification::Confidential)
            }],
        };
        let mut wifi = runtime(eligible, "br-south", RuntimeTrustLevel::Registered);
        wifi.connectivity = Some(RuntimeConnectivity::Wifi);
        wifi.estimated_cost_microunits = Some(40);
        wifi.estimated_latency_ms = Some(200);
        let mut expensive = runtime(metered, "br-south", RuntimeTrustLevel::Registered);
        expensive.connectivity = Some(RuntimeConnectivity::Metered);
        expensive.estimated_cost_microunits = Some(100);
        expensive.estimated_latency_ms = Some(200);
        let decision = choose_runtime(
            tenant,
            ContinuityId::new(),
            ExecutionEpoch::initial(),
            &CapsuleRequirements::default(),
            Some(&policy),
            DataClassification::Confidential,
            &[expensive, wifi],
            None,
            Utc::now(),
        );
        assert_eq!(decision.selected_runtime_id, Some(eligible));
        let rejected = decision
            .candidates
            .iter()
            .find(|candidate| candidate.runtime_id == metered)
            .unwrap();
        assert!(
            rejected
                .finding_codes
                .contains(&"CONNECTIVITY_DENIED".into())
        );
        assert!(rejected.finding_codes.contains(&"COST_DENIED".into()));
    }

    #[test]
    fn placement_score_explains_battery_cost_and_latency_tradeoffs() {
        let preferred_id = RuntimeId::new();
        let other_id = RuntimeId::new();
        let mut preferred = runtime(preferred_id, "br-south", RuntimeTrustLevel::Registered);
        preferred.battery_percent = Some(90);
        preferred.estimated_cost_microunits = Some(10);
        preferred.estimated_latency_ms = Some(50);
        let mut other = runtime(other_id, "br-south", RuntimeTrustLevel::Registered);
        other.battery_percent = Some(20);
        other.estimated_cost_microunits = Some(1_000);
        other.estimated_latency_ms = Some(500);

        let decision = choose_runtime(
            TenantId::new("tenant-a").unwrap(),
            ContinuityId::new(),
            ExecutionEpoch::initial(),
            &CapsuleRequirements::default(),
            None,
            DataClassification::Internal,
            &[other, preferred],
            None,
            Utc::now(),
        );

        assert_eq!(decision.selected_runtime_id, Some(preferred_id));
        let preferred = decision
            .candidates
            .iter()
            .find(|candidate| candidate.runtime_id == preferred_id)
            .unwrap();
        let other = decision
            .candidates
            .iter()
            .find(|candidate| candidate.runtime_id == other_id)
            .unwrap();
        assert!(preferred.score_factors.battery_rank > other.score_factors.battery_rank);
        assert!(preferred.score_factors.cost_rank > other.score_factors.cost_rank);
        assert!(preferred.score_factors.latency_rank > other.score_factors.latency_rank);
        assert!(preferred.score > other.score);
    }
}

#[cfg(test)]
#[path = "placement_coverage_tests.rs"]
mod placement_coverage_tests;
