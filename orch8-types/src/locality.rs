//! Bounded locality-policy evaluation shared by dispatch-time placement
//! (engine) and claim-time matching (storage).
//!
//! Lives in `orch8-types` so the atomic claim path can re-check a step's
//! residency policy against the claimant's live capabilities without the
//! storage crate depending on the engine.

use crate::continuity::{
    DataClassification, LocalityPolicy, LocalityRule, PolicyOutcome, RuntimeCapabilities,
};

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
