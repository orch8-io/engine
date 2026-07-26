//! Coverage tests for the strict release proof gate evaluation.
//!
//! Count contract: 27 independently named unit tests.

use super::*;

fn clean_preflight() -> Value {
    json!({"overall": "pass"})
}

fn clean_validation() -> Value {
    json!({"divergences": [], "inconclusive": 0})
}

macro_rules! gate_case {
    ($name:ident, $diff:expr, $preflight:expr, $validation:expr, $allow:expr, $max_div:expr, $max_inc:expr, $passed:expr) => {
        #[test]
        fn $name() {
            let report = evaluate_gate(
                Uuid::nil(),
                &$diff,
                &$preflight,
                &$validation,
                $allow,
                $max_div,
                $max_inc,
            );
            assert_eq!(report.passed, $passed);
        }
    };
}

gate_case!(
    coverage_gate_001_severity_none_passes,
    json!({"max_severity": "none"}),
    clean_preflight(),
    clean_validation(),
    false,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_002_informational_severity_passes,
    json!({"max_severity": "informational"}),
    clean_preflight(),
    clean_validation(),
    false,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_003_behavioral_severity_passes,
    json!({"max_severity": "behavioral"}),
    clean_preflight(),
    clean_validation(),
    false,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_004_side_effect_risk_passes_only_when_allowed,
    json!({"max_severity": "side_effect_risk"}),
    clean_preflight(),
    clean_validation(),
    true,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_005_side_effect_risk_fails_by_default,
    json!({"max_severity": "side_effect_risk"}),
    clean_preflight(),
    clean_validation(),
    false,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_006_breaking_severity_fails_even_when_allowed,
    json!({"max_severity": "breaking"}),
    clean_preflight(),
    clean_validation(),
    true,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_007_unrecognized_severity_fails_closed,
    json!({"max_severity": "cosmetic"}),
    clean_preflight(),
    clean_validation(),
    true,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_008_missing_severity_with_no_entries_means_none,
    json!({"entries": []}),
    clean_preflight(),
    clean_validation(),
    false,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_009_missing_severity_with_entries_fails_closed,
    json!({"entries": [{"severity": "informational"}]}),
    clean_preflight(),
    clean_validation(),
    true,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_010_warning_preflight_passes,
    json!({"max_severity": "none"}),
    json!({"overall": "warning"}),
    clean_validation(),
    false,
    0,
    0,
    true
);
gate_case!(
    coverage_gate_011_failing_preflight_fails_the_gate,
    json!({"max_severity": "none"}),
    json!({"overall": "fail"}),
    clean_validation(),
    false,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_012_missing_preflight_overall_fails_closed,
    json!({"max_severity": "none"}),
    json!({}),
    clean_validation(),
    false,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_013_divergences_within_budget_pass,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [{}], "inconclusive": 0}),
    false,
    1,
    0,
    true
);
gate_case!(
    coverage_gate_014_divergences_at_budget_boundary_pass,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [{}, {}], "inconclusive": 0}),
    false,
    2,
    0,
    true
);
gate_case!(
    coverage_gate_015_divergences_over_budget_fail,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [{}, {}, {}], "inconclusive": 0}),
    false,
    2,
    0,
    false
);
gate_case!(
    coverage_gate_016_missing_divergences_array_fails_closed,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"inconclusive": 0}),
    false,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_017_inconclusive_within_budget_passes,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [], "inconclusive": 2}),
    false,
    0,
    2,
    true
);
gate_case!(
    coverage_gate_018_inconclusive_over_budget_fails,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [], "inconclusive": 3}),
    false,
    0,
    2,
    false
);
gate_case!(
    coverage_gate_019_missing_inconclusive_count_fails_closed,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": []}),
    false,
    0,
    0,
    false
);
gate_case!(
    coverage_gate_020_non_numeric_inconclusive_fails_closed,
    json!({"max_severity": "none"}),
    clean_preflight(),
    json!({"divergences": [], "inconclusive": "many"}),
    false,
    0,
    1,
    false
);

#[test]
fn coverage_gate_021_report_checks_are_named_in_stable_order() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "none"}),
        &clean_preflight(),
        &clean_validation(),
        false,
        0,
        0,
    );
    let names: Vec<&str> = report.checks.iter().map(|check| check.name).collect();
    assert_eq!(
        names,
        [
            "semantic_diff",
            "candidate_preflight",
            "historical_validation"
        ]
    );
}

#[test]
fn coverage_gate_022_report_passed_is_the_conjunction_of_checks() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "side_effect_risk"}),
        &json!({"overall": "fail"}),
        &json!({"divergences": [{}], "inconclusive": 0}),
        false,
        0,
        0,
    );
    assert!(!report.passed);
    assert!(report.checks.iter().all(|check| !check.passed));
    // Exactly one relaxed check must not flip the others.
    let relaxed = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "side_effect_risk"}),
        &json!({"overall": "fail"}),
        &json!({"divergences": [{}], "inconclusive": 0}),
        true,
        1,
        0,
    );
    assert!(relaxed.checks[0].passed);
    assert!(!relaxed.checks[1].passed);
    assert!(relaxed.checks[2].passed);
    assert!(!relaxed.passed);
}

#[test]
fn coverage_gate_023_severity_evidence_names_the_observed_level() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "behavioral"}),
        &clean_preflight(),
        &clean_validation(),
        false,
        0,
        0,
    );
    assert_eq!(report.checks[0].evidence, "max severity: behavioral");
    assert_eq!(report.checks[1].evidence, "overall: pass");
}

#[test]
fn coverage_gate_024_validation_evidence_reports_counts_against_budgets() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "none"}),
        &clean_preflight(),
        &json!({"divergences": [{}], "inconclusive": 2}),
        false,
        5,
        7,
    );
    assert_eq!(
        report.checks[2].evidence,
        "divergences: 1/5, inconclusive: 2/7"
    );
}

#[test]
fn coverage_gate_025_report_serializes_stable_field_names() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "none"}),
        &clean_preflight(),
        &clean_validation(),
        false,
        0,
        0,
    );
    let value = serde_json::to_value(&report).unwrap();
    assert_eq!(
        value["release_id"],
        json!("00000000-0000-0000-0000-000000000000")
    );
    assert_eq!(value["passed"], json!(true));
    let check = &value["checks"][0];
    for key in ["name", "passed", "evidence"] {
        assert!(check.get(key).is_some(), "gate check missing {key}");
    }
}

#[test]
fn coverage_gate_026_empty_diff_object_means_no_entries_and_passes() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({}),
        &clean_preflight(),
        &clean_validation(),
        false,
        0,
        0,
    );
    assert!(report.checks[0].passed);
    assert_eq!(report.checks[0].evidence, "max severity: none");
    assert!(report.passed);
}

gate_case!(
    coverage_gate_027_severity_matching_is_case_sensitive_and_fails_closed,
    json!({"max_severity": "None"}),
    clean_preflight(),
    clean_validation(),
    true,
    0,
    0,
    false
);
