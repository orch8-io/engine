//! Deterministic capability routing and bounded locality-policy evaluation.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use orch8_types::continuity::{
    CapsuleRequirements, ContinuityId, DataClassification, ExecutionEpoch, LocalityPolicy,
    PlacementDecision, PlacementDecisionId, PlacementEvidence, PolicyOutcome, RuntimeCapabilities,
    RuntimeId, RuntimeTrustLevel,
};
use orch8_types::ids::TenantId;
use thiserror::Error;

use crate::continuity::{CompatibilityStatus, assess_compatibility};

pub const MAX_POLICY_RULES: usize = 256;
pub const MAX_CANDIDATES: usize = 1_000;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolicyValidationError {
    #[error("locality policy version must be non-zero")]
    MissingVersion,
    #[error("locality policy exceeds {MAX_POLICY_RULES} rules")]
    TooManyRules,
    #[error("locality policy contains an empty region or hardware capability")]
    EmptyFact,
    #[error("locality policy has contradictory region rules for {0:?}")]
    ContradictoryRegions(DataClassification),
}

pub fn validate_policy(policy: &LocalityPolicy) -> Result<(), PolicyValidationError> {
    if policy.version == 0 {
        return Err(PolicyValidationError::MissingVersion);
    }
    if policy.rules.len() > MAX_POLICY_RULES {
        return Err(PolicyValidationError::TooManyRules);
    }
    if policy.rules.iter().any(|rule| {
        rule.allowed_regions.iter().any(String::is_empty)
            || rule.require_hardware.as_ref().is_some_and(String::is_empty)
    }) {
        return Err(PolicyValidationError::EmptyFact);
    }
    for classification in [
        DataClassification::Public,
        DataClassification::Internal,
        DataClassification::Confidential,
        DataClassification::Restricted,
    ] {
        let mut region_sets = policy
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
        if let Some(mut intersection) = region_sets.next() {
            for regions in region_sets {
                intersection = intersection.intersection(&regions).copied().collect();
            }
            if intersection.is_empty() {
                return Err(PolicyValidationError::ContradictoryRegions(classification));
            }
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
        if !rule.allowed_regions.is_empty() {
            if runtime.regions.is_empty() {
                unknown = true;
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
            let score = trust_score(runtime.trust)
                + i64::from(runtime.offline_capable) * 3
                + i64::from(current_runtime == Some(runtime.runtime_id)) * 5;
            PlacementEvidence {
                runtime_id: runtime.runtime_id,
                outcome,
                score,
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
        policy_version: policy.map(|value| value.version),
        candidates: evidence,
        created_at: now,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use orch8_types::continuity::{LocalityRule, RuntimeKind};

    use super::*;

    fn runtime(id: RuntimeId, region: &str, trust: RuntimeTrustLevel) -> RuntimeCapabilities {
        let now = Utc::now();
        RuntimeCapabilities {
            runtime_id: id,
            kind: RuntimeKind::Mobile,
            trust,
            handlers: vec!["camera".into()],
            plugins: Vec::new(),
            regions: vec![region.into()],
            hardware: vec!["camera".into()],
            offline_capable: true,
            observed_at: now,
            expires_at: now + Duration::minutes(1),
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
                    classification: DataClassification::Restricted,
                    allowed_regions: vec!["br-south".into()],
                    minimum_trust: None,
                    require_offline: None,
                    require_hardware: None,
                },
                LocalityRule {
                    classification: DataClassification::Restricted,
                    allowed_regions: vec!["eu-west".into()],
                    minimum_trust: None,
                    require_offline: None,
                    require_hardware: None,
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
    fn placement_filters_hard_requirements_and_breaks_ties_deterministically() {
        let tenant = TenantId::new("tenant-a").unwrap();
        let eligible = RuntimeId::from_uuid(uuid::Uuid::from_u128(1));
        let wrong_region = RuntimeId::from_uuid(uuid::Uuid::from_u128(2));
        let policy = LocalityPolicy {
            version: 1,
            rules: vec![LocalityRule {
                classification: DataClassification::Restricted,
                allowed_regions: vec!["br-south".into()],
                minimum_trust: Some(RuntimeTrustLevel::Signed),
                require_offline: Some(true),
                require_hardware: Some("camera".into()),
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
}
