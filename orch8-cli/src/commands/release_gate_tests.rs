//! The release proof gate fails closed: it passes only when the semantic diff
//! severity is known and acceptable (`side_effect_risk` only with explicit
//! opt-in, `breaking` never), candidate preflight did not fail, and historical
//! replay divergences/inconclusive counts are present and within budget.
//! Missing or malformed evidence of any kind is a failure, never a pass.

use super::*;

use reqwest::StatusCode;

use crate::commands::test_support::mock_api_with_responses;

fn clean_preflight() -> Value {
    json!({"overall": "pass"})
}

fn clean_validation() -> Value {
    json!({"divergences": [], "inconclusive": 0})
}

fn none_diff() -> Value {
    json!({"max_severity": "none"})
}

struct GateCase {
    case: &'static str,
    diff: Value,
    preflight: Value,
    validation: Value,
    allow_side_effect_risk: bool,
    max_divergences: u32,
    max_inconclusive: u32,
    passed: bool,
}

fn diff_case(case: &'static str, diff: Value, allow: bool, passed: bool) -> GateCase {
    GateCase {
        case,
        diff,
        preflight: clean_preflight(),
        validation: clean_validation(),
        allow_side_effect_risk: allow,
        max_divergences: 0,
        max_inconclusive: 0,
        passed,
    }
}

fn preflight_case(case: &'static str, preflight: Value, passed: bool) -> GateCase {
    GateCase {
        preflight,
        ..diff_case(case, none_diff(), false, passed)
    }
}

fn validation_case(
    case: &'static str,
    validation: Value,
    max_divergences: u32,
    max_inconclusive: u32,
    passed: bool,
) -> GateCase {
    GateCase {
        validation,
        max_divergences,
        max_inconclusive,
        ..diff_case(case, none_diff(), false, passed)
    }
}

fn assert_gate_outcomes(cases: impl IntoIterator<Item = GateCase>) {
    for case in cases {
        let report = evaluate_gate(
            Uuid::nil(),
            &case.diff,
            &case.preflight,
            &case.validation,
            case.allow_side_effect_risk,
            case.max_divergences,
            case.max_inconclusive,
        );
        assert_eq!(report.passed, case.passed, "case {}", case.case);
    }
}

#[test]
fn semantic_diff_passes_only_on_a_known_acceptable_severity() {
    assert_gate_outcomes([
        diff_case("severity none", none_diff(), false, true),
        diff_case(
            "informational severity",
            json!({"max_severity": "informational"}),
            false,
            true,
        ),
        diff_case(
            "side-effect risk with opt-in",
            json!({"max_severity": "side_effect_risk"}),
            true,
            true,
        ),
        diff_case(
            "side-effect risk by default",
            json!({"max_severity": "side_effect_risk"}),
            false,
            false,
        ),
        diff_case(
            "breaking even with opt-in",
            json!({"max_severity": "breaking"}),
            true,
            false,
        ),
        diff_case(
            "unrecognized severity",
            json!({"max_severity": "cosmetic"}),
            true,
            false,
        ),
        diff_case(
            "severity is case-sensitive",
            json!({"max_severity": "None"}),
            true,
            false,
        ),
        diff_case(
            "no severity and no entries means none",
            json!({"entries": []}),
            false,
            true,
        ),
        diff_case(
            "non-string severity",
            json!({"max_severity": 12, "entries": []}),
            false,
            false,
        ),
    ]);
}

#[test]
fn failing_or_missing_candidate_preflight_fails_the_gate() {
    assert_gate_outcomes([
        preflight_case("warning preflight", json!({"overall": "warning"}), true),
        preflight_case("failing preflight", json!({"overall": "fail"}), false),
        preflight_case("missing preflight overall", json!({}), false),
    ]);
}

#[test]
fn historical_validation_passes_only_within_budget_and_with_complete_counts() {
    assert_gate_outcomes([
        validation_case(
            "divergences at budget",
            json!({"divergences": [{}, {}], "inconclusive": 0}),
            2,
            0,
            true,
        ),
        validation_case(
            "divergences over budget",
            json!({"divergences": [{}, {}, {}], "inconclusive": 0}),
            2,
            0,
            false,
        ),
        validation_case(
            "missing divergences",
            json!({"inconclusive": 0}),
            0,
            0,
            false,
        ),
        validation_case(
            "inconclusive at budget",
            json!({"divergences": [], "inconclusive": 2}),
            0,
            2,
            true,
        ),
        validation_case(
            "inconclusive over budget",
            json!({"divergences": [], "inconclusive": 3}),
            0,
            2,
            false,
        ),
        validation_case(
            "missing inconclusive count",
            json!({"divergences": []}),
            0,
            0,
            false,
        ),
    ]);
}

#[test]
fn report_names_its_checks_in_stable_order() {
    let report = evaluate_gate(
        Uuid::nil(),
        &none_diff(),
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
fn relaxing_one_threshold_never_flips_another_check() {
    let diff = json!({"max_severity": "side_effect_risk"});
    let preflight = json!({"overall": "fail"});
    let validation = json!({"divergences": [{}], "inconclusive": 0});

    let strict = evaluate_gate(Uuid::nil(), &diff, &preflight, &validation, false, 0, 0);
    let relaxed = evaluate_gate(Uuid::nil(), &diff, &preflight, &validation, true, 1, 0);

    assert!(strict.checks.iter().all(|check| !check.passed));
    let relaxed_checks: Vec<bool> = relaxed.checks.iter().map(|check| check.passed).collect();
    assert_eq!(relaxed_checks, [true, false, true]);
    assert!(!relaxed.passed);
}

#[test]
fn evidence_names_observed_severity_preflight_and_budgets() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({"max_severity": "behavioral"}),
        &clean_preflight(),
        &json!({"divergences": [{}], "inconclusive": 2}),
        false,
        5,
        7,
    );

    assert_eq!(report.checks[0].evidence, "max severity: behavioral");
    assert_eq!(report.checks[1].evidence, "overall: pass");
    assert_eq!(
        report.checks[2].evidence,
        "divergences: 1/5, inconclusive: 2/7"
    );
}

#[test]
fn empty_diff_object_is_missing_evidence_not_an_empty_diff() {
    let report = evaluate_gate(
        Uuid::nil(),
        &json!({}),
        &clean_preflight(),
        &clean_validation(),
        false,
        0,
        0,
    );

    assert!(!report.checks[0].passed);
    assert_eq!(report.checks[0].evidence, "max severity: unknown");
    assert!(!report.passed);
}

#[test]
fn malformed_validation_is_rejected_even_with_maximum_thresholds() {
    for validation in [
        json!({}),
        json!({"divergences": [], "inconclusive": "bad"}),
        json!({"divergences": null, "inconclusive": 0}),
    ] {
        let report = evaluate_gate(
            Uuid::nil(),
            &json!({"entries": []}),
            &clean_preflight(),
            &validation,
            true,
            u32::MAX,
            u32::MAX,
        );
        assert!(!report.passed, "{validation}");
        assert!(
            report.checks[2].evidence.contains("invalid"),
            "{validation}"
        );
    }
}

/// Release, then the concurrent diff + preflight (one body satisfies both).
fn release_responses(release: &Value) -> Vec<(StatusCode, String)> {
    let proof = json!({"entries": [], "overall": "pass"}).to_string();
    vec![
        (StatusCode::OK, release.to_string()),
        (StatusCode::OK, proof.clone()),
        (StatusCode::OK, proof),
    ]
}

#[tokio::test]
async fn draft_release_is_validated_against_history_before_gating() {
    let id = Uuid::now_v7();
    let release = json!({"candidate_sequence_id": Uuid::new_v4(), "state": "draft"});
    let mut responses = release_responses(&release);
    responses.push((StatusCode::OK, clean_validation().to_string()));
    let api = mock_api_with_responses(responses).await;

    let report = gate_report(&Client::new(), &api.base, id, Some(7), false, 0, 0)
        .await
        .unwrap();

    assert!(report.passed);
    let validate: Vec<_> = api
        .log
        .snapshot()
        .into_iter()
        .filter(|request| request.uri.ends_with(&format!("/releases/{id}/validate")))
        .collect();
    assert_eq!(validate.len(), 1);
    assert_eq!(validate[0].body, json!({"sample": 7, "skip": false}));
}

#[tokio::test]
async fn non_draft_release_gates_on_its_stored_validation_summary() {
    let release = json!({"candidate_sequence_id": Uuid::new_v4(), "state": "ready",
        "validation_summary": {"divergences": [{}], "inconclusive": 0}});
    let api = mock_api_with_responses(release_responses(&release)).await;

    let report = gate_report(&Client::new(), &api.base, Uuid::now_v7(), None, false, 0, 0)
        .await
        .unwrap();

    assert!(!report.passed, "stored divergence must fail the gate");
    assert_eq!(
        report.checks[2].evidence,
        "divergences: 1/0, inconclusive: 0/0"
    );
    assert!(
        api.log
            .snapshot()
            .iter()
            .all(|request| !request.uri.ends_with("/validate")),
        "a non-draft release must not be re-validated"
    );
}
