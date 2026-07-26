//! Coverage tests for explainable placement score factors.
//!
//! Count contract: 11 independently named unit tests.

use super::*;

fn factors() -> PlacementScoreFactors {
    PlacementScoreFactors {
        trust: 40,
        current_runtime: 25,
        offline_capable: 10,
        battery_rank: 3,
        cost_rank: 2,
        latency_rank: 1,
    }
}

fn evidence() -> PlacementEvidence {
    PlacementEvidence {
        runtime_id: RuntimeId::new(),
        outcome: PolicyOutcome::Allow,
        score: 81,
        score_factors: factors(),
        finding_codes: vec!["trust_ok".into()],
    }
}

#[test]
fn coverage_continuity_001_score_factors_default_to_zero() {
    let default = PlacementScoreFactors::default();
    assert_eq!(default.trust, 0);
    assert_eq!(default.current_runtime, 0);
    assert_eq!(default.offline_capable, 0);
    assert_eq!(default.battery_rank, 0);
    assert_eq!(default.cost_rank, 0);
    assert_eq!(default.latency_rank, 0);
}

#[test]
fn coverage_continuity_002_default_factors_contribute_zero_to_a_score() {
    // The documented contract: absent values contribute zero, so a defaulted
    // factor set sums to exactly zero and never skews a stored score.
    let default = PlacementScoreFactors::default();
    let sum = default.trust
        + default.current_runtime
        + default.offline_capable
        + default.battery_rank
        + default.cost_rank
        + default.latency_rank;
    assert_eq!(sum, 0);
}

#[test]
fn coverage_continuity_003_legacy_evidence_without_factors_deserializes() {
    // Evidence persisted before `score_factors` existed must still load; the
    // field carries `#[serde(default)]` for exactly this compatibility.
    let mut value = serde_json::to_value(evidence()).unwrap();
    value.as_object_mut().unwrap().remove("score_factors");
    let decoded: PlacementEvidence = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.score_factors, PlacementScoreFactors::default());
    assert_eq!(decoded.score, 81);
}

#[test]
fn coverage_continuity_004_evidence_serializes_the_factors_key() {
    let value = serde_json::to_value(evidence()).unwrap();
    let factors = value.get("score_factors").expect("score_factors key");
    assert_eq!(factors.get("trust").unwrap(), 40);
    assert_eq!(factors.get("latency_rank").unwrap(), 1);
}

#[test]
fn coverage_continuity_005_score_factors_serde_round_trip() {
    let original = factors();
    let back: PlacementScoreFactors =
        serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
    assert_eq!(back, original);
}

#[test]
fn coverage_continuity_006_score_factors_preserve_negative_values() {
    let negative = PlacementScoreFactors {
        trust: -50,
        ..PlacementScoreFactors::default()
    };
    let back: PlacementScoreFactors =
        serde_json::from_str(&serde_json::to_string(&negative).unwrap()).unwrap();
    assert_eq!(back.trust, -50);
}

#[test]
fn coverage_continuity_007_factors_reject_a_payload_with_missing_fields() {
    // Per-field defaults are intentionally absent: a partial factor payload
    // is a shape error, not a silent zero-fill.
    let payload = serde_json::json!({"trust": 10});
    assert!(serde_json::from_value::<PlacementScoreFactors>(payload).is_err());
}

#[test]
fn coverage_continuity_008_factors_equality_is_field_by_field() {
    let a = factors();
    let mut b = factors();
    assert_eq!(a, b);
    b.battery_rank += 1;
    assert_ne!(a, b);
}

#[test]
fn coverage_continuity_009_evidence_round_trip_preserves_factors_and_findings() {
    let original = evidence();
    let back: PlacementEvidence =
        serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
    assert_eq!(back, original);
    assert_eq!(back.finding_codes, ["trust_ok"]);
}

#[test]
fn coverage_continuity_010_factors_debug_lists_every_component() {
    let debug = format!("{:?}", factors());
    for field in [
        "trust",
        "current_runtime",
        "offline_capable",
        "battery_rank",
        "cost_rank",
        "latency_rank",
    ] {
        assert!(debug.contains(field), "missing {field} in: {debug}");
    }
}

#[test]
fn coverage_continuity_011_policy_outcome_serializes_snake_case() {
    for (outcome, wire) in [
        (PolicyOutcome::Allow, "\"allow\""),
        (PolicyOutcome::Deny, "\"deny\""),
        (PolicyOutcome::Unknown, "\"unknown\""),
    ] {
        assert_eq!(serde_json::to_string(&outcome).unwrap(), wire);
        let back: PolicyOutcome = serde_json::from_str(wire).unwrap();
        assert_eq!(back, outcome);
    }
    assert!(serde_json::from_str::<PolicyOutcome>("\"denied\"").is_err());
}
