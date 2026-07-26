//! Coverage tests for placement score factors and metric ranking.
//!
//! Pins [`rank_candidate_metric`] ordering/tie semantics and the additive
//! [`PlacementScoreFactors`] composition in [`choose_runtime`].
//!
//! Count contract: 16 independently named unit tests.

use chrono::Duration;
use orch8_types::continuity::RuntimeKind;

use super::*;

fn runtime(id: RuntimeId, trust: RuntimeTrustLevel) -> RuntimeCapabilities {
    let now = Utc::now();
    RuntimeCapabilities {
        runtime_id: id,
        kind: RuntimeKind::Mobile,
        trust,
        handlers: vec!["camera".into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: vec!["br-south".into()],
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

fn choose(candidates: &[RuntimeCapabilities], current: Option<RuntimeId>) -> PlacementDecision {
    choose_runtime(
        TenantId::new("tenant-a").unwrap(),
        ContinuityId::new(),
        ExecutionEpoch::initial(),
        &CapsuleRequirements::default(),
        None,
        DataClassification::Internal,
        candidates,
        current,
        Utc::now(),
    )
}

#[test]
fn coverage_placement_001_empty_candidates_rank_to_empty_map() {
    let ranks = rank_candidate_metric(&[], |r| r.battery_percent.map(u64::from), true);
    assert!(ranks.is_empty());
}

#[test]
fn coverage_placement_002_unknown_metrics_are_excluded() {
    let candidates = vec![runtime(RuntimeId::new(), RuntimeTrustLevel::Registered)];
    let ranks = rank_candidate_metric(&candidates, |r| r.battery_percent.map(u64::from), true);
    assert!(ranks.is_empty());
}

#[test]
fn coverage_placement_003_single_known_metric_scores_ten() {
    let id = RuntimeId::new();
    let mut candidate = runtime(id, RuntimeTrustLevel::Registered);
    candidate.battery_percent = Some(50);
    let ranks = rank_candidate_metric(&[candidate], |r| r.battery_percent.map(u64::from), true);
    assert_eq!(ranks.get(&id), Some(&10));
}

#[test]
fn coverage_placement_004_prefer_high_ranks_larger_value_higher() {
    let high = RuntimeId::new();
    let low = RuntimeId::new();
    let mut a = runtime(high, RuntimeTrustLevel::Registered);
    a.battery_percent = Some(90);
    let mut b = runtime(low, RuntimeTrustLevel::Registered);
    b.battery_percent = Some(20);
    let ranks = rank_candidate_metric(&[a, b], |r| r.battery_percent.map(u64::from), true);
    assert_eq!(ranks.get(&high), Some(&10));
    assert_eq!(ranks.get(&low), Some(&5));
}

#[test]
fn coverage_placement_005_prefer_low_ranks_smaller_value_higher() {
    let cheap = RuntimeId::new();
    let pricey = RuntimeId::new();
    let mut a = runtime(cheap, RuntimeTrustLevel::Registered);
    a.estimated_cost_microunits = Some(10);
    let mut b = runtime(pricey, RuntimeTrustLevel::Registered);
    b.estimated_cost_microunits = Some(1_000);
    let ranks = rank_candidate_metric(&[a, b], |r| r.estimated_cost_microunits, false);
    assert_eq!(ranks.get(&cheap), Some(&10));
    assert_eq!(ranks.get(&pricey), Some(&5));
}

#[test]
fn coverage_placement_006_tied_metrics_share_the_same_score() {
    let first = RuntimeId::new();
    let second = RuntimeId::new();
    let third = RuntimeId::new();
    let mut a = runtime(first, RuntimeTrustLevel::Registered);
    a.battery_percent = Some(80);
    let mut b = runtime(second, RuntimeTrustLevel::Registered);
    b.battery_percent = Some(80);
    let mut c = runtime(third, RuntimeTrustLevel::Registered);
    c.battery_percent = Some(10);
    let ranks = rank_candidate_metric(&[a, b, c], |r| r.battery_percent.map(u64::from), true);
    assert_eq!(ranks.get(&first), ranks.get(&second));
    assert!(ranks.get(&first) > ranks.get(&third));
}

#[test]
fn coverage_placement_007_three_distinct_values_scale_by_position() {
    let ids: Vec<RuntimeId> = (0..3).map(|_| RuntimeId::new()).collect();
    let mut candidates: Vec<RuntimeCapabilities> = ids
        .iter()
        .map(|id| runtime(*id, RuntimeTrustLevel::Registered))
        .collect();
    for (candidate, battery) in candidates.iter_mut().zip([90_u8, 50, 10]) {
        candidate.battery_percent = Some(battery);
    }
    let ranks = rank_candidate_metric(&candidates, |r| r.battery_percent.map(u64::from), true);
    assert_eq!(ranks.get(&ids[0]), Some(&10));
    assert_eq!(ranks.get(&ids[1]), Some(&6));
    assert_eq!(ranks.get(&ids[2]), Some(&3));
}

#[test]
fn coverage_placement_008_mixed_known_and_unknown_metrics_rank_only_known() {
    let known = RuntimeId::new();
    let unknown = RuntimeId::new();
    let mut a = runtime(known, RuntimeTrustLevel::Registered);
    a.estimated_latency_ms = Some(42);
    let b = runtime(unknown, RuntimeTrustLevel::Registered);
    let ranks = rank_candidate_metric(&[a, b], |r| r.estimated_latency_ms, false);
    assert_eq!(ranks.get(&known), Some(&10));
    assert!(!ranks.contains_key(&unknown));
}

#[test]
fn coverage_placement_009_score_equals_sum_of_score_factors() {
    let id = RuntimeId::new();
    let mut candidate = runtime(id, RuntimeTrustLevel::Signed);
    candidate.battery_percent = Some(100);
    candidate.estimated_cost_microunits = Some(5);
    candidate.estimated_latency_ms = Some(5);
    let decision = choose(&[candidate], Some(id));
    let evidence = &decision.candidates[0];
    let factors = &evidence.score_factors;
    let sum = factors.trust
        + factors.current_runtime
        + factors.offline_capable
        + factors.battery_rank
        + factors.cost_rank
        + factors.latency_rank;
    assert_eq!(evidence.score, sum);
}

#[test]
fn coverage_placement_010_current_runtime_receives_stickiness_bonus() {
    let current = RuntimeId::new();
    let other = RuntimeId::new();
    let a = runtime(current, RuntimeTrustLevel::Registered);
    let b = runtime(other, RuntimeTrustLevel::Registered);
    let decision = choose(&[a, b], Some(current));
    let current_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == current)
        .unwrap();
    let other_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == other)
        .unwrap();
    assert_eq!(current_ev.score_factors.current_runtime, 5);
    assert_eq!(other_ev.score_factors.current_runtime, 0);
    assert_eq!(decision.selected_runtime_id, Some(current));
}

#[test]
fn coverage_placement_011_offline_capable_adds_three_points() {
    let offline = RuntimeId::new();
    let online = RuntimeId::new();
    let a = runtime(offline, RuntimeTrustLevel::Registered);
    let mut b = runtime(online, RuntimeTrustLevel::Registered);
    b.offline_capable = false;
    let decision = choose(&[a, b], None);
    let offline_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == offline)
        .unwrap();
    let online_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == online)
        .unwrap();
    assert_eq!(offline_ev.score_factors.offline_capable, 3);
    assert_eq!(online_ev.score_factors.offline_capable, 0);
}

#[test]
fn coverage_placement_012_trust_levels_order_the_score() {
    let attested = RuntimeId::new();
    let unverified = RuntimeId::new();
    let a = runtime(attested, RuntimeTrustLevel::Attested);
    let b = runtime(unverified, RuntimeTrustLevel::Unverified);
    let decision = choose(&[a, b], None);
    let attested_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == attested)
        .unwrap();
    let unverified_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == unverified)
        .unwrap();
    assert_eq!(attested_ev.score_factors.trust, 30);
    assert_eq!(unverified_ev.score_factors.trust, 0);
    assert_eq!(decision.selected_runtime_id, Some(attested));
}

#[test]
fn coverage_placement_013_unknown_battery_contributes_zero_rank() {
    let id = RuntimeId::new();
    let candidate = runtime(id, RuntimeTrustLevel::Registered);
    let decision = choose(&[candidate], None);
    let evidence = &decision.candidates[0];
    assert_eq!(evidence.score_factors.battery_rank, 0);
    assert_eq!(evidence.score_factors.cost_rank, 0);
    assert_eq!(evidence.score_factors.latency_rank, 0);
}

#[test]
fn coverage_placement_014_better_metrics_raise_total_score() {
    let fit = RuntimeId::new();
    let unfit = RuntimeId::new();
    let mut a = runtime(fit, RuntimeTrustLevel::Registered);
    a.battery_percent = Some(95);
    a.estimated_cost_microunits = Some(1);
    a.estimated_latency_ms = Some(1);
    let mut b = runtime(unfit, RuntimeTrustLevel::Registered);
    b.battery_percent = Some(5);
    b.estimated_cost_microunits = Some(9_999);
    b.estimated_latency_ms = Some(9_999);
    let decision = choose(&[a, b], None);
    let fit_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == fit)
        .unwrap();
    let unfit_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == unfit)
        .unwrap();
    assert!(fit_ev.score > unfit_ev.score);
    assert_eq!(decision.selected_runtime_id, Some(fit));
}

#[test]
fn coverage_placement_015_signed_trust_scores_between_registered_and_attested() {
    let signed = RuntimeId::new();
    let registered = RuntimeId::new();
    let a = runtime(signed, RuntimeTrustLevel::Signed);
    let b = runtime(registered, RuntimeTrustLevel::Registered);
    let decision = choose(&[a, b], None);
    let signed_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == signed)
        .unwrap();
    let registered_ev = decision
        .candidates
        .iter()
        .find(|c| c.runtime_id == registered)
        .unwrap();
    assert_eq!(signed_ev.score_factors.trust, 20);
    assert_eq!(registered_ev.score_factors.trust, 10);
}

#[test]
fn coverage_placement_016_every_candidate_carries_factor_breakdown() {
    let ids: Vec<RuntimeId> = (0..3).map(|_| RuntimeId::new()).collect();
    let candidates: Vec<RuntimeCapabilities> = ids
        .iter()
        .map(|id| runtime(*id, RuntimeTrustLevel::Registered))
        .collect();
    let decision = choose(&candidates, None);
    assert_eq!(decision.candidates.len(), 3);
    for evidence in &decision.candidates {
        // Factors are always populated (never left as a hidden partial sum).
        assert_eq!(evidence.score_factors.trust, 10);
        assert_eq!(evidence.score_factors.offline_capable, 3);
    }
}
