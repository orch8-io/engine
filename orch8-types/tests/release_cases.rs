//! Exhaustive unit tests for `orch8_types::release`: the release state
//! machine, cohort assignment, gate evaluation, and serde contracts.

use std::str::FromStr;

use chrono::Utc;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::release::{
    DiffEntry, DiffSeverity, GateEvaluation, GateMetric, GateVerdict, InFlightPolicy,
    ReleaseDecision, ReleaseGate, ReleaseState, ReleaseVariant, SemanticDiff, VariantStats,
    WorkflowRelease, assign_variant, evaluate_gates,
};
use serde_json::json;
use uuid::Uuid;

const ALL_STATES: [ReleaseState; 8] = [
    ReleaseState::Draft,
    ReleaseState::Validating,
    ReleaseState::Ready,
    ReleaseState::Canary,
    ReleaseState::Promoted,
    ReleaseState::Paused,
    ReleaseState::RolledBack,
    ReleaseState::Failed,
];

/// Assert `from` transitions exactly to `allowed` and to nothing else,
/// checking the full row of the 8x8 matrix.
fn assert_transition_row(from: ReleaseState, allowed: &[ReleaseState]) {
    for next in ALL_STATES {
        let expected = allowed.contains(&next);
        assert_eq!(
            from.can_transition_to(next),
            expected,
            "{from:?} -> {next:?} should be {expected}"
        );
    }
}

// ---------------------------------------------------------------------------
// State machine: full 8x8 transition matrix, one row per test
// ---------------------------------------------------------------------------

#[test]
fn transitions_from_draft() {
    assert_transition_row(
        ReleaseState::Draft,
        &[
            ReleaseState::Validating,
            ReleaseState::Ready,
            ReleaseState::Failed,
        ],
    );
}

#[test]
fn transitions_from_validating() {
    assert_transition_row(
        ReleaseState::Validating,
        &[ReleaseState::Ready, ReleaseState::Failed],
    );
}

#[test]
fn transitions_from_ready() {
    assert_transition_row(
        ReleaseState::Ready,
        &[ReleaseState::Canary, ReleaseState::Failed],
    );
}

#[test]
fn transitions_from_canary() {
    assert_transition_row(
        ReleaseState::Canary,
        &[
            ReleaseState::Promoted,
            ReleaseState::Paused,
            ReleaseState::RolledBack,
        ],
    );
}

#[test]
fn transitions_from_promoted() {
    assert_transition_row(ReleaseState::Promoted, &[]);
}

#[test]
fn transitions_from_paused() {
    assert_transition_row(
        ReleaseState::Paused,
        &[ReleaseState::Canary, ReleaseState::RolledBack],
    );
}

#[test]
fn transitions_from_rolled_back() {
    assert_transition_row(ReleaseState::RolledBack, &[]);
}

#[test]
fn transitions_from_failed() {
    assert_transition_row(ReleaseState::Failed, &[]);
}

#[test]
fn no_state_transitions_to_itself() {
    for s in ALL_STATES {
        assert!(!s.can_transition_to(s), "{s:?} must not self-transition");
    }
}

#[test]
fn no_state_transitions_back_to_draft() {
    for s in ALL_STATES {
        assert!(
            !s.can_transition_to(ReleaseState::Draft),
            "{s:?} must never return to draft"
        );
    }
}

#[test]
fn terminal_states_have_no_outgoing_transitions() {
    for s in ALL_STATES.into_iter().filter(|s| s.is_terminal()) {
        for next in ALL_STATES {
            assert!(!s.can_transition_to(next), "terminal {s:?} -> {next:?}");
        }
    }
}

#[test]
fn non_terminal_states_have_at_least_one_outgoing_transition() {
    for s in ALL_STATES.into_iter().filter(|s| !s.is_terminal()) {
        assert!(
            ALL_STATES.into_iter().any(|next| s.can_transition_to(next)),
            "non-terminal {s:?} is a dead end"
        );
    }
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

#[test]
fn is_terminal_exact_set() {
    let terminal: Vec<ReleaseState> = ALL_STATES.into_iter().filter(|s| s.is_terminal()).collect();
    assert_eq!(
        terminal,
        vec![
            ReleaseState::Promoted,
            ReleaseState::RolledBack,
            ReleaseState::Failed
        ]
    );
}

#[test]
fn routes_traffic_exact_set() {
    let routing: Vec<ReleaseState> = ALL_STATES
        .into_iter()
        .filter(|s| s.routes_traffic())
        .collect();
    assert_eq!(routing, vec![ReleaseState::Canary, ReleaseState::Promoted]);
}

#[test]
fn promoted_is_both_terminal_and_routing() {
    // Documented overlap: a promoted release is frozen (no transitions)
    // yet still routes all new traffic to the candidate.
    assert!(ReleaseState::Promoted.is_terminal());
    assert!(ReleaseState::Promoted.routes_traffic());
}

// ---------------------------------------------------------------------------
// FromStr / as_str / serde parity
// ---------------------------------------------------------------------------

#[test]
fn as_str_values_are_stable_snake_case() {
    let strs: Vec<&str> = ALL_STATES.into_iter().map(ReleaseState::as_str).collect();
    assert_eq!(
        strs,
        vec![
            "draft",
            "validating",
            "ready",
            "canary",
            "promoted",
            "paused",
            "rolled_back",
            "failed"
        ]
    );
}

#[test]
fn from_str_round_trips_every_state() {
    for s in ALL_STATES {
        assert_eq!(ReleaseState::from_str(s.as_str()).unwrap(), s);
    }
}

#[test]
fn from_str_rejects_unknown_and_reports_input() {
    let err = ReleaseState::from_str("shipped").unwrap_err();
    assert!(err.contains("shipped"), "{err}");
}

#[test]
fn from_str_rejects_wrong_case() {
    assert!(ReleaseState::from_str("Draft").is_err());
    assert!(ReleaseState::from_str("CANARY").is_err());
    assert!(ReleaseState::from_str("RolledBack").is_err());
}

#[test]
fn from_str_rejects_padded_or_empty_input() {
    assert!(ReleaseState::from_str(" draft").is_err());
    assert!(ReleaseState::from_str("draft ").is_err());
    assert!(ReleaseState::from_str("").is_err());
}

#[test]
fn serde_repr_matches_as_str_for_every_state() {
    for s in ALL_STATES {
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            format!("\"{}\"", s.as_str())
        );
    }
}

#[test]
fn serde_deserializes_every_snake_case_state() {
    for s in ALL_STATES {
        let back: ReleaseState = serde_json::from_str(&format!("\"{}\"", s.as_str())).unwrap();
        assert_eq!(back, s);
    }
}

#[test]
fn serde_rejects_unknown_state_string() {
    assert!(serde_json::from_str::<ReleaseState>("\"launched\"").is_err());
    assert!(serde_json::from_str::<ReleaseState>("\"rolledback\"").is_err());
}

// ---------------------------------------------------------------------------
// Cohort assignment
// ---------------------------------------------------------------------------

fn fixed_release() -> Uuid {
    Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef)
}

#[test]
fn assignment_is_deterministic_across_repeat_calls() {
    let release = fixed_release();
    for key in ["tenant-a", "customer-42", "device-x", "a", "0"] {
        for pct in [1u8, 33, 50, 99] {
            let first = assign_variant(release, key, pct);
            for _ in 0..50 {
                assert_eq!(assign_variant(release, key, pct), first, "{key}@{pct}");
            }
        }
    }
}

#[test]
fn assignment_is_deterministic_for_unicode_keys() {
    let release = fixed_release();
    for key in ["клієнт-42", "顧客-7", "🚀-cohort", "ünïcode-ß", "मुवक्किल"]
    {
        let first = assign_variant(release, key, 37);
        for _ in 0..20 {
            assert_eq!(assign_variant(release, key, 37), first, "{key}");
        }
        // Both extremes still hold for unicode keys.
        assert_eq!(assign_variant(release, key, 0), ReleaseVariant::Baseline);
        assert_eq!(assign_variant(release, key, 100), ReleaseVariant::Candidate);
    }
}

#[test]
fn assignment_is_deterministic_for_empty_key() {
    let release = fixed_release();
    let first = assign_variant(release, "", 42);
    for _ in 0..50 {
        assert_eq!(assign_variant(release, "", 42), first);
    }
    assert_eq!(assign_variant(release, "", 0), ReleaseVariant::Baseline);
    assert_eq!(assign_variant(release, "", 100), ReleaseVariant::Candidate);
}

#[test]
fn assignment_is_deterministic_for_very_long_keys() {
    let release = fixed_release();
    let long_key = "k".repeat(100_000);
    let first = assign_variant(release, &long_key, 60);
    for _ in 0..5 {
        assert_eq!(assign_variant(release, &long_key, 60), first);
    }
    // A one-character difference at the end of a huge key still matters
    // deterministically (no truncation of input).
    let mut other = long_key.clone();
    other.push('x');
    let other_first = assign_variant(release, &other, 60);
    for _ in 0..5 {
        assert_eq!(assign_variant(release, &other, 60), other_first);
    }
}

#[test]
fn assignment_zero_percent_is_always_baseline() {
    let release = fixed_release();
    for i in 0..200 {
        assert_eq!(
            assign_variant(release, &format!("key-{i}"), 0),
            ReleaseVariant::Baseline
        );
    }
}

#[test]
fn assignment_hundred_percent_is_always_candidate() {
    let release = fixed_release();
    for i in 0..200 {
        assert_eq!(
            assign_variant(release, &format!("key-{i}"), 100),
            ReleaseVariant::Candidate
        );
    }
}

#[test]
fn assignment_over_hundred_percent_is_candidate() {
    let release = fixed_release();
    for pct in [101u8, 150, 200, u8::MAX] {
        assert_eq!(
            assign_variant(release, "any", pct),
            ReleaseVariant::Candidate,
            "{pct}"
        );
    }
}

#[test]
fn assignment_is_monotone_in_percent() {
    // Once a cohort lands on the candidate at p%, it stays there for every
    // higher percentage — raising the rollout never reshuffles cohorts.
    let release = fixed_release();
    for i in 0..300 {
        let key = format!("cohort-{i}");
        let mut was_candidate = false;
        for pct in 0..=100u8 {
            let v = assign_variant(release, &key, pct);
            if was_candidate {
                assert_eq!(
                    v,
                    ReleaseVariant::Candidate,
                    "cohort {key} flapped back at {pct}%"
                );
            }
            if v == ReleaseVariant::Candidate {
                was_candidate = true;
            }
        }
        assert!(was_candidate, "{key} must be candidate at 100%");
    }
}

#[test]
fn assignment_is_monotone_for_unicode_and_empty_keys() {
    let release = fixed_release();
    for key in ["", "клієнт", "顧客", "🚀"] {
        let mut was_candidate = false;
        for pct in 0..=100u8 {
            let v = assign_variant(release, key, pct);
            if was_candidate {
                assert_eq!(v, ReleaseVariant::Candidate, "{key:?} flapped at {pct}%");
            }
            if v == ReleaseVariant::Candidate {
                was_candidate = true;
            }
        }
    }
}

fn candidate_share(release: Uuid, percent: u8, n: u32) -> f64 {
    let candidates = (0..n)
        .filter(|i| {
            assign_variant(release, &format!("cohort-{i}"), percent) == ReleaseVariant::Candidate
        })
        .count();
    #[allow(clippy::cast_precision_loss)]
    let share = candidates as f64 / f64::from(n);
    share
}

#[test]
fn distribution_roughly_proportional_at_10_percent() {
    let share = candidate_share(fixed_release(), 10, 10_000);
    assert!((0.05..=0.15).contains(&share), "share was {share}");
}

#[test]
fn distribution_roughly_proportional_at_25_percent() {
    let share = candidate_share(fixed_release(), 25, 10_000);
    assert!((0.20..=0.30).contains(&share), "share was {share}");
}

#[test]
fn distribution_roughly_proportional_at_50_percent() {
    let share = candidate_share(fixed_release(), 50, 10_000);
    assert!((0.45..=0.55).contains(&share), "share was {share}");
}

#[test]
fn distribution_roughly_proportional_at_75_percent() {
    let share = candidate_share(fixed_release(), 75, 10_000);
    assert!((0.70..=0.80).contains(&share), "share was {share}");
}

#[test]
fn distribution_roughly_proportional_at_90_percent() {
    let share = candidate_share(fixed_release(), 90, 10_000);
    assert!((0.85..=0.95).contains(&share), "share was {share}");
}

#[test]
fn assignment_is_independent_across_releases() {
    // The same cohort key must not systematically land on the same side
    // for different releases (no global bias by key).
    let keys: Vec<String> = (0..200).map(|i| format!("k{i}")).collect();
    let r1 = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    let r2 = Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);
    let same = keys
        .iter()
        .filter(|k| assign_variant(r1, k, 50) == assign_variant(r2, k, 50))
        .count();
    assert!(
        (60..=140).contains(&same),
        "suspicious correlation between releases: {same}/200"
    );
}

#[test]
fn assignment_varies_across_keys_within_one_release() {
    // At 50% both variants must actually appear across distinct keys —
    // the hash must depend on the key, not only on the release.
    let release = fixed_release();
    let variants: Vec<ReleaseVariant> = (0..100)
        .map(|i| assign_variant(release, &format!("key-{i}"), 50))
        .collect();
    assert!(variants.contains(&ReleaseVariant::Baseline));
    assert!(variants.contains(&ReleaseVariant::Candidate));
}

#[test]
fn assignment_works_with_nil_uuid() {
    let nil = Uuid::nil();
    let first = assign_variant(nil, "cohort", 50);
    for _ in 0..20 {
        assert_eq!(assign_variant(nil, "cohort", 50), first);
    }
    assert_eq!(assign_variant(nil, "cohort", 0), ReleaseVariant::Baseline);
    assert_eq!(
        assign_variant(nil, "cohort", 100),
        ReleaseVariant::Candidate
    );
}

// ---------------------------------------------------------------------------
// VariantStats::rate
// ---------------------------------------------------------------------------

#[test]
fn rate_is_none_for_zero_total_on_both_metrics() {
    let stats = VariantStats {
        total: 0,
        completed: 0,
        failed: 0,
        cancelled: 0,
    };
    assert_eq!(stats.rate(GateMetric::ErrorRate), None);
    assert_eq!(stats.rate(GateMetric::CancelRate), None);
}

#[test]
fn rate_error_metric_reads_failed() {
    let stats = VariantStats {
        total: 8,
        completed: 6,
        failed: 2,
        cancelled: 0,
    };
    assert_eq!(stats.rate(GateMetric::ErrorRate), Some(0.25));
}

#[test]
fn rate_cancel_metric_reads_cancelled() {
    let stats = VariantStats {
        total: 4,
        completed: 1,
        failed: 0,
        cancelled: 3,
    };
    assert_eq!(stats.rate(GateMetric::CancelRate), Some(0.75));
    // The two metrics are independent views of the same stats.
    assert_eq!(stats.rate(GateMetric::ErrorRate), Some(0.0));
}

#[test]
fn rate_extremes_zero_and_one() {
    let all_failed = VariantStats {
        total: 16,
        completed: 0,
        failed: 16,
        cancelled: 0,
    };
    assert_eq!(all_failed.rate(GateMetric::ErrorRate), Some(1.0));
    assert_eq!(all_failed.rate(GateMetric::CancelRate), Some(0.0));
}

#[test]
fn default_variant_stats_is_zeroed_and_rateless() {
    let stats = VariantStats::default();
    assert_eq!(stats.total, 0);
    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.cancelled, 0);
    assert_eq!(stats.rate(GateMetric::ErrorRate), None);
}

// ---------------------------------------------------------------------------
// evaluate_gates
// ---------------------------------------------------------------------------

fn stats(total: u64, failed: u64) -> VariantStats {
    VariantStats {
        total,
        completed: total - failed,
        failed,
        cancelled: 0,
    }
}

fn error_gate(max_regression: f64, min_sample: u32) -> ReleaseGate {
    ReleaseGate {
        metric: GateMetric::ErrorRate,
        max_regression,
        min_sample,
    }
}

#[test]
fn gate_passes_within_regression_budget() {
    let gates = vec![error_gate(0.25, 10)];
    // candidate - baseline = 0.125, within 0.25.
    let evals = evaluate_gates(&gates, stats(16, 2), stats(16, 4));
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn gate_exact_max_regression_equality_passes() {
    // Rates chosen exactly representable in binary: 0.25 and 0.5.
    let gates = vec![error_gate(0.25, 10)];
    let evals = evaluate_gates(&gates, stats(100, 25), stats(100, 50));
    assert_eq!(evals[0].baseline_rate, Some(0.25));
    assert_eq!(evals[0].candidate_rate, Some(0.5));
    // regression == max_regression exactly → not `>` → Pass.
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn gate_epsilon_over_max_regression_fails() {
    // Same exact 0.25 regression, but the budget is a hair smaller.
    let gates = vec![error_gate(0.25 - 1e-12, 10)];
    let evals = evaluate_gates(&gates, stats(100, 25), stats(100, 50));
    assert_eq!(evals[0].verdict, GateVerdict::Fail);
}

#[test]
fn gate_failure_reports_both_rates() {
    let gates = vec![error_gate(0.05, 10)];
    let evals = evaluate_gates(&gates, stats(100, 0), stats(100, 50));
    assert_eq!(evals[0].verdict, GateVerdict::Fail);
    assert_eq!(evals[0].baseline_rate, Some(0.0));
    assert_eq!(evals[0].candidate_rate, Some(0.5));
}

#[test]
fn gate_min_sample_exactly_met_is_conclusive() {
    let gates = vec![error_gate(0.5, 10)];
    // Exactly N observations on both sides: conclusive.
    let evals = evaluate_gates(&gates, stats(10, 0), stats(10, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn gate_baseline_one_below_min_sample_is_inconclusive() {
    let gates = vec![error_gate(0.5, 10)];
    let evals = evaluate_gates(&gates, stats(9, 0), stats(100, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
}

#[test]
fn gate_candidate_one_below_min_sample_is_inconclusive() {
    let gates = vec![error_gate(0.5, 10)];
    let evals = evaluate_gates(&gates, stats(100, 0), stats(9, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
}

#[test]
fn gate_both_one_below_min_sample_is_inconclusive() {
    let gates = vec![error_gate(0.5, 10)];
    let evals = evaluate_gates(&gates, stats(9, 0), stats(9, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
}

#[test]
fn gate_perfect_candidate_below_min_sample_is_never_pass() {
    // Looks flawless, but 3 observations against min_sample 50 must not
    // count as a pass.
    let gates = vec![error_gate(0.0, 50)];
    let evals = evaluate_gates(&gates, stats(100, 50), stats(3, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
}

#[test]
fn gate_improvement_passes_with_zero_budget() {
    let gates = vec![error_gate(0.0, 10)];
    // Candidate strictly better than baseline: negative regression.
    let evals = evaluate_gates(&gates, stats(100, 50), stats(100, 10));
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn gate_equal_rates_pass_with_zero_budget() {
    let gates = vec![error_gate(0.0, 10)];
    let evals = evaluate_gates(&gates, stats(100, 25), stats(100, 25));
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn multiple_gates_yield_mixed_verdicts_independently() {
    let gates = vec![
        // Error rate regresses 0.5 with zero budget → Fail.
        error_gate(0.0, 10),
        // Cancel rate is identical on both sides → Pass.
        ReleaseGate {
            metric: GateMetric::CancelRate,
            max_regression: 0.0,
            min_sample: 10,
        },
        // Requires more samples than exist → Inconclusive.
        error_gate(0.0, 1_000),
    ];
    let baseline = VariantStats {
        total: 100,
        completed: 90,
        failed: 0,
        cancelled: 10,
    };
    let candidate = VariantStats {
        total: 100,
        completed: 40,
        failed: 50,
        cancelled: 10,
    };
    let evals = evaluate_gates(&gates, baseline, candidate);
    assert_eq!(evals.len(), 3);
    assert_eq!(evals[0].verdict, GateVerdict::Fail);
    assert_eq!(evals[1].verdict, GateVerdict::Pass);
    assert_eq!(evals[2].verdict, GateVerdict::Inconclusive);
}

#[test]
fn empty_gates_produce_empty_evaluations() {
    let evals = evaluate_gates(&[], stats(100, 0), stats(100, 100));
    assert!(evals.is_empty());
}

#[test]
fn gate_zero_min_sample_with_zero_totals_is_inconclusive() {
    // min_sample 0 makes the sample check pass vacuously, but with no
    // observations there are no rates — still never a Pass.
    let gates = vec![error_gate(0.5, 0)];
    let evals = evaluate_gates(&gates, stats(0, 0), stats(0, 0));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
    assert_eq!(evals[0].baseline_rate, None);
    assert_eq!(evals[0].candidate_rate, None);
}

#[test]
fn gate_inconclusive_still_reports_available_rates() {
    let gates = vec![error_gate(0.5, 100)];
    let evals = evaluate_gates(&gates, stats(4, 1), stats(8, 2));
    assert_eq!(evals[0].verdict, GateVerdict::Inconclusive);
    assert_eq!(evals[0].baseline_rate, Some(0.25));
    assert_eq!(evals[0].candidate_rate, Some(0.25));
}

#[test]
fn cancel_rate_gate_ignores_failures() {
    let gates = vec![ReleaseGate {
        metric: GateMetric::CancelRate,
        max_regression: 0.1,
        min_sample: 10,
    }];
    // Candidate fails a lot but cancels nothing: the cancel gate passes.
    let baseline = VariantStats {
        total: 100,
        completed: 95,
        failed: 0,
        cancelled: 5,
    };
    let candidate = VariantStats {
        total: 100,
        completed: 20,
        failed: 80,
        cancelled: 0,
    };
    let evals = evaluate_gates(&gates, baseline, candidate);
    assert_eq!(evals[0].verdict, GateVerdict::Pass);
}

#[test]
fn evaluations_preserve_gate_order_and_definition() {
    let gates = vec![error_gate(0.01, 7), error_gate(0.99, 3)];
    let evals = evaluate_gates(&gates, stats(50, 5), stats(50, 5));
    assert_eq!(evals.len(), 2);
    for (gate, eval) in gates.iter().zip(&evals) {
        assert_eq!(&eval.gate, gate);
    }
}

// ---------------------------------------------------------------------------
// Serde: enums, defaults, round trips, optional-field omission
// ---------------------------------------------------------------------------

#[test]
fn gate_verdict_serde_snake_case_round_trip() {
    for (v, s) in [
        (GateVerdict::Inconclusive, "\"inconclusive\""),
        (GateVerdict::Pass, "\"pass\""),
        (GateVerdict::Fail, "\"fail\""),
    ] {
        assert_eq!(serde_json::to_string(&v).unwrap(), s);
        let back: GateVerdict = serde_json::from_str(s).unwrap();
        assert_eq!(back, v);
    }
    assert!(serde_json::from_str::<GateVerdict>("\"Pass\"").is_err());
}

#[test]
fn gate_metric_serde_snake_case_round_trip() {
    for (m, s) in [
        (GateMetric::ErrorRate, "\"error_rate\""),
        (GateMetric::CancelRate, "\"cancel_rate\""),
    ] {
        assert_eq!(serde_json::to_string(&m).unwrap(), s);
        let back: GateMetric = serde_json::from_str(s).unwrap();
        assert_eq!(back, m);
    }
    assert!(serde_json::from_str::<GateMetric>("\"latency\"").is_err());
}

#[test]
fn in_flight_policy_default_is_pin_and_serde_is_snake_case() {
    assert_eq!(InFlightPolicy::default(), InFlightPolicy::Pin);
    assert_eq!(
        serde_json::to_string(&InFlightPolicy::Pin).unwrap(),
        "\"pin\""
    );
    assert_eq!(
        serde_json::to_string(&InFlightPolicy::OperatorDecision).unwrap(),
        "\"operator_decision\""
    );
    let back: InFlightPolicy = serde_json::from_str("\"operator_decision\"").unwrap();
    assert_eq!(back, InFlightPolicy::OperatorDecision);
}

#[test]
fn release_variant_serde_snake_case() {
    assert_eq!(
        serde_json::to_string(&ReleaseVariant::Baseline).unwrap(),
        "\"baseline\""
    );
    assert_eq!(
        serde_json::to_string(&ReleaseVariant::Candidate).unwrap(),
        "\"candidate\""
    );
    let back: ReleaseVariant = serde_json::from_str("\"candidate\"").unwrap();
    assert_eq!(back, ReleaseVariant::Candidate);
}

fn sample_release() -> WorkflowRelease {
    WorkflowRelease {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t1"),
        namespace: Namespace::new("default"),
        sequence_name: "checkout".into(),
        baseline_sequence_id: SequenceId::new(),
        baseline_version: 3,
        candidate_sequence_id: SequenceId::new(),
        candidate_version: 4,
        state: ReleaseState::Canary,
        canary_percent: 25,
        gates: vec![ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.05,
            min_sample: 20,
        }],
        in_flight_policy: InFlightPolicy::OperatorDecision,
        validation_summary: Some(json!({"replayed": 20, "matches": 19})),
        canary_started_at: Some(Utc::now()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn workflow_release_full_round_trip() {
    let r = sample_release();
    let back: WorkflowRelease = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
    assert_eq!(back, r);
}

#[test]
fn workflow_release_omits_empty_optional_fields() {
    let mut r = sample_release();
    r.gates = vec![];
    r.validation_summary = None;
    r.canary_started_at = None;
    let v = serde_json::to_value(&r).unwrap();
    let obj = v.as_object().unwrap();
    assert!(!obj.contains_key("gates"), "{v}");
    assert!(!obj.contains_key("validation_summary"), "{v}");
    assert!(!obj.contains_key("canary_started_at"), "{v}");
    // And they still round-trip.
    let back: WorkflowRelease = serde_json::from_value(v).unwrap();
    assert_eq!(back, r);
}

#[test]
fn workflow_release_deserializes_missing_optionals_with_defaults() {
    let v = json!({
        "id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "default",
        "sequence_name": "checkout",
        "baseline_sequence_id": Uuid::now_v7(),
        "baseline_version": 1,
        "candidate_sequence_id": Uuid::now_v7(),
        "candidate_version": 2,
        "state": "draft",
        "canary_percent": 0,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    });
    let r: WorkflowRelease = serde_json::from_value(v).unwrap();
    assert!(r.gates.is_empty());
    assert_eq!(r.in_flight_policy, InFlightPolicy::Pin);
    assert!(r.validation_summary.is_none());
    assert!(r.canary_started_at.is_none());
    assert_eq!(r.state, ReleaseState::Draft);
}

#[test]
fn release_gate_round_trip() {
    let g = ReleaseGate {
        metric: GateMetric::CancelRate,
        max_regression: 0.125,
        min_sample: 42,
    };
    let back: ReleaseGate = serde_json::from_str(&serde_json::to_string(&g).unwrap()).unwrap();
    assert_eq!(back, g);
    let v = serde_json::to_value(&g).unwrap();
    assert_eq!(v["metric"], "cancel_rate");
    assert_eq!(v["max_regression"], 0.125);
    assert_eq!(v["min_sample"], 42);
}

#[test]
fn release_decision_round_trip() {
    let d = ReleaseDecision {
        id: Uuid::now_v7(),
        release_id: Uuid::now_v7(),
        from_state: ReleaseState::Canary,
        to_state: ReleaseState::RolledBack,
        actor: "gate:ErrorRate".into(),
        reason: "candidate error rate exceeded budget".into(),
        decided_at: Utc::now(),
    };
    let text = serde_json::to_string(&d).unwrap();
    let back: ReleaseDecision = serde_json::from_str(&text).unwrap();
    assert_eq!(back, d);
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["from_state"], "canary");
    assert_eq!(v["to_state"], "rolled_back");
}

#[test]
fn variant_stats_round_trip() {
    let s = VariantStats {
        total: 10,
        completed: 7,
        failed: 2,
        cancelled: 1,
    };
    let back: VariantStats = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert_eq!(back, s);
}

// ---------------------------------------------------------------------------
// Diff report types
// ---------------------------------------------------------------------------

#[test]
fn diff_severity_is_totally_ordered() {
    assert!(DiffSeverity::Informational < DiffSeverity::Behavioral);
    assert!(DiffSeverity::Behavioral < DiffSeverity::SideEffectRisk);
    assert!(DiffSeverity::SideEffectRisk < DiffSeverity::Incompatible);
    let mut severities = vec![
        DiffSeverity::Incompatible,
        DiffSeverity::Informational,
        DiffSeverity::SideEffectRisk,
        DiffSeverity::Behavioral,
    ];
    severities.sort();
    assert_eq!(
        severities,
        vec![
            DiffSeverity::Informational,
            DiffSeverity::Behavioral,
            DiffSeverity::SideEffectRisk,
            DiffSeverity::Incompatible,
        ]
    );
}

#[test]
fn diff_severity_max_matches_ordering() {
    let max = [
        DiffSeverity::Behavioral,
        DiffSeverity::Incompatible,
        DiffSeverity::Informational,
    ]
    .into_iter()
    .max()
    .unwrap();
    assert_eq!(max, DiffSeverity::Incompatible);
}

#[test]
fn diff_severity_serde_snake_case() {
    for (s, text) in [
        (DiffSeverity::Informational, "\"informational\""),
        (DiffSeverity::Behavioral, "\"behavioral\""),
        (DiffSeverity::SideEffectRisk, "\"side_effect_risk\""),
        (DiffSeverity::Incompatible, "\"incompatible\""),
    ] {
        assert_eq!(serde_json::to_string(&s).unwrap(), text);
        let back: DiffSeverity = serde_json::from_str(text).unwrap();
        assert_eq!(back, s);
    }
}

#[test]
fn diff_entry_round_trip_and_block_id_omission() {
    let with_block = DiffEntry {
        category: "handler_changed".into(),
        severity: DiffSeverity::SideEffectRisk,
        block_id: Some("charge".into()),
        summary: "step 'charge' handler changed".into(),
    };
    let back: DiffEntry =
        serde_json::from_str(&serde_json::to_string(&with_block).unwrap()).unwrap();
    assert_eq!(back, with_block);

    let without_block = DiffEntry {
        category: "blocks_reordered".into(),
        severity: DiffSeverity::Behavioral,
        block_id: None,
        summary: "shared blocks execute in a different order".into(),
    };
    let v = serde_json::to_value(&without_block).unwrap();
    assert!(!v.as_object().unwrap().contains_key("block_id"), "{v}");
    let back: DiffEntry = serde_json::from_value(v).unwrap();
    assert_eq!(back, without_block);
}

#[test]
fn semantic_diff_round_trip_and_empty_omission() {
    let full = SemanticDiff {
        entries: vec![DiffEntry {
            category: "block_added".into(),
            severity: DiffSeverity::Behavioral,
            block_id: Some("extra".into()),
            summary: "step 'extra' was added".into(),
        }],
        max_severity: Some(DiffSeverity::Behavioral),
        candidate_lint: vec!["[extra] something looks off".into()],
    };
    let back: SemanticDiff = serde_json::from_str(&serde_json::to_string(&full).unwrap()).unwrap();
    assert_eq!(back, full);

    let empty = SemanticDiff {
        entries: vec![],
        max_severity: None,
        candidate_lint: vec![],
    };
    let v = serde_json::to_value(&empty).unwrap();
    let obj = v.as_object().unwrap();
    assert!(!obj.contains_key("max_severity"), "{v}");
    assert!(!obj.contains_key("candidate_lint"), "{v}");
    let back: SemanticDiff = serde_json::from_value(v).unwrap();
    assert_eq!(back, empty);
}

#[test]
fn gate_evaluation_omits_none_rates() {
    let eval = GateEvaluation {
        gate: ReleaseGate {
            metric: GateMetric::ErrorRate,
            max_regression: 0.05,
            min_sample: 100,
        },
        baseline_rate: None,
        candidate_rate: None,
        verdict: GateVerdict::Inconclusive,
    };
    let v = serde_json::to_value(&eval).unwrap();
    let obj = v.as_object().unwrap();
    assert!(!obj.contains_key("baseline_rate"), "{v}");
    assert!(!obj.contains_key("candidate_rate"), "{v}");
    assert_eq!(v["verdict"], "inconclusive");
    let back: GateEvaluation = serde_json::from_value(v).unwrap();
    assert_eq!(back, eval);
}

#[test]
fn gate_evaluation_round_trip_with_rates() {
    let eval = GateEvaluation {
        gate: ReleaseGate {
            metric: GateMetric::CancelRate,
            max_regression: 0.0,
            min_sample: 5,
        },
        baseline_rate: Some(0.25),
        candidate_rate: Some(0.5),
        verdict: GateVerdict::Fail,
    };
    let back: GateEvaluation =
        serde_json::from_str(&serde_json::to_string(&eval).unwrap()).unwrap();
    assert_eq!(back, eval);
}
