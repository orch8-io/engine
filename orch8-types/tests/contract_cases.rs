//! Extensive unit tests for `orch8_types::contract`: artifact serde shapes,
//! structural validation, and the pure assertion-evaluation functions.
//!
//! These deliberately go deeper than the module's inline tests: exotic
//! `resolve_path` segments, the full `AssertionOp` x value-type matrix,
//! boundary values for `in_range`, and report/expectation serde defaults.

use std::collections::BTreeMap;

use orch8_types::contract::{
    Assertion, AssertionOp, AssertionSubject, CONTRACT_SCHEMA_VERSION, CallCountExpectation,
    CaseReport, ContractCase, ContractSuite, ExpectedTerminalState, MockDef, MockOutcome,
    MockPolicy, PathExpectation, SuiteReport, UnmockedHandlerPolicy, check_call_counts, check_op,
    check_path, evaluate_assertion, resolve_path,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn suite_from(v: Value) -> ContractSuite {
    serde_json::from_value(v).expect("suite deserializes")
}

fn case_from(v: Value) -> ContractCase {
    serde_json::from_value(v).expect("case deserializes")
}

fn minimal_suite() -> ContractSuite {
    suite_from(json!({"cases": [{"name": "only", "expect": {}}]}))
}

fn empty_case(name: &str) -> ContractCase {
    case_from(json!({"name": name, "expect": {}}))
}

fn eq_op(v: Value) -> AssertionOp {
    AssertionOp::Equals { value: v }
}

fn contains_op(v: Value) -> AssertionOp {
    AssertionOp::Contains { value: v }
}

fn range_op(min: Option<f64>, max: Option<f64>) -> AssertionOp {
    AssertionOp::InRange { min, max }
}

fn type_op(t: &str) -> AssertionOp {
    AssertionOp::HasType {
        expected: t.to_string(),
    }
}

fn hist(items: &[&str]) -> Vec<String> {
    items.iter().map(ToString::to_string).collect()
}

fn path_expect(items: &[&str], ordered: bool) -> PathExpectation {
    PathExpectation {
        traversed: hist(items),
        ordered,
    }
}

fn calls_map(pairs: &[(&str, u32)]) -> BTreeMap<String, u32> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect()
}

fn cc(handler: &str, min: u32, max: Option<u32>) -> CallCountExpectation {
    CallCountExpectation {
        handler: handler.to_string(),
        min,
        max,
    }
}

fn case_report(name: &str, passed: bool) -> CaseReport {
    CaseReport {
        name: name.to_string(),
        passed,
        failures: if passed {
            vec![]
        } else {
            vec![format!("{name} diverged")]
        },
        final_state: if passed { "completed" } else { "failed" }.to_string(),
        executed_blocks: vec![],
        handler_calls: BTreeMap::new(),
        ticks: 1,
        logical_duration_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// ContractSuite / ContractCase serde
// ---------------------------------------------------------------------------

#[test]
fn minimal_suite_defaults_schema_version() {
    let suite = minimal_suite();
    assert_eq!(suite.schema_version, CONTRACT_SCHEMA_VERSION);
}

#[test]
fn minimal_suite_defaults_unmocked_policy_to_fail() {
    assert_eq!(
        minimal_suite().unmocked_handlers,
        UnmockedHandlerPolicy::Fail
    );
}

#[test]
fn minimal_suite_has_no_sequence_identity() {
    let suite = minimal_suite();
    assert_eq!(suite.sequence_name, None);
    assert_eq!(suite.sequence_version, None);
}

#[test]
fn suite_serialization_omits_absent_sequence_fields() {
    let v = serde_json::to_value(minimal_suite()).unwrap();
    let obj = v.as_object().unwrap();
    assert!(!obj.contains_key("sequence_name"));
    assert!(!obj.contains_key("sequence_version"));
    // schema_version is always written (no skip attribute).
    assert_eq!(obj.get("schema_version"), Some(&json!(1)));
}

#[test]
fn suite_serialization_includes_sequence_fields_when_present() {
    let suite = suite_from(json!({
        "sequence_name": "billing",
        "sequence_version": 7,
        "cases": [{"name": "c", "expect": {}}]
    }));
    let v = serde_json::to_value(&suite).unwrap();
    assert_eq!(v["sequence_name"], json!("billing"));
    assert_eq!(v["sequence_version"], json!(7));
}

#[test]
fn unmocked_policy_empty_success_parses() {
    let suite = suite_from(json!({
        "unmocked_handlers": "empty_success",
        "cases": [{"name": "c", "expect": {}}]
    }));
    assert_eq!(suite.unmocked_handlers, UnmockedHandlerPolicy::EmptySuccess);
}

#[test]
fn unmocked_policy_rejects_unknown_variant() {
    let res: Result<ContractSuite, _> = serde_json::from_value(json!({
        "unmocked_handlers": "explode",
        "cases": [{"name": "c", "expect": {}}]
    }));
    assert!(res.is_err());
}

#[test]
fn unmocked_policy_serializes_snake_case() {
    assert_eq!(
        serde_json::to_value(UnmockedHandlerPolicy::EmptySuccess).unwrap(),
        json!("empty_success")
    );
    assert_eq!(
        serde_json::to_value(UnmockedHandlerPolicy::Fail).unwrap(),
        json!("fail")
    );
}

#[test]
fn case_input_defaults_to_json_null() {
    assert_eq!(empty_case("c").input, Value::Null);
}

#[test]
fn case_config_defaults_to_none() {
    assert_eq!(empty_case("c").config, None);
}

#[test]
fn case_mocks_and_signals_default_empty() {
    let case = empty_case("c");
    assert!(case.mocks.is_empty());
    assert!(case.signals.is_empty());
}

#[test]
fn case_budgets_default_to_none() {
    let case = empty_case("c");
    assert_eq!(case.max_logical_duration_ms, None);
    assert_eq!(case.max_ticks, None);
}

#[test]
fn case_description_round_trips() {
    let case = case_from(json!({
        "name": "c",
        "description": "checks the refund path",
        "expect": {}
    }));
    assert_eq!(case.description.as_deref(), Some("checks the refund path"));
    let re: ContractCase = serde_json::from_value(serde_json::to_value(&case).unwrap()).unwrap();
    assert_eq!(re, case);
}

#[test]
fn case_budgets_round_trip() {
    let case = case_from(json!({
        "name": "c",
        "max_logical_duration_ms": 86_400_000u64,
        "max_ticks": 42,
        "expect": {}
    }));
    assert_eq!(case.max_logical_duration_ms, Some(86_400_000));
    assert_eq!(case.max_ticks, Some(42));
}

// ---------------------------------------------------------------------------
// MockDef / MockPolicy / MockOutcome serde
// ---------------------------------------------------------------------------

#[test]
fn mock_success_shape_round_trips() {
    let mock: MockDef = serde_json::from_value(json!({
        "handler": "charge",
        "type": "success",
        "output": {"charged": true}
    }))
    .unwrap();
    assert_eq!(mock.handler.as_deref(), Some("charge"));
    assert_eq!(mock.block, None);
    assert_eq!(
        mock.policy,
        MockPolicy::Success {
            output: json!({"charged": true})
        }
    );
    let v = serde_json::to_value(&mock).unwrap();
    assert_eq!(v["type"], json!("success"));
    // Flattened: policy fields sit at the same level as the target.
    assert_eq!(v["output"], json!({"charged": true}));
    assert_eq!(v["handler"], json!("charge"));
}

#[test]
fn mock_failure_retryable_defaults_false() {
    let mock: MockDef = serde_json::from_value(json!({
        "handler": "h",
        "type": "failure",
        "message": "boom"
    }))
    .unwrap();
    assert_eq!(
        mock.policy,
        MockPolicy::Failure {
            message: "boom".into(),
            retryable: false
        }
    );
}

#[test]
fn mock_failure_retryable_explicit_round_trips() {
    let mock: MockDef = serde_json::from_value(json!({
        "block": "b",
        "type": "failure",
        "message": "later",
        "retryable": true
    }))
    .unwrap();
    let re: MockDef = serde_json::from_value(serde_json::to_value(&mock).unwrap()).unwrap();
    assert_eq!(re, mock);
    assert!(matches!(
        re.policy,
        MockPolicy::Failure {
            retryable: true,
            ..
        }
    ));
}

#[test]
fn mock_recorded_needs_no_extra_fields() {
    let mock: MockDef =
        serde_json::from_value(json!({"handler": "h", "type": "recorded"})).unwrap();
    assert_eq!(mock.policy, MockPolicy::Recorded);
}

#[test]
fn mock_attempts_outcomes_round_trip() {
    let mock: MockDef = serde_json::from_value(json!({
        "handler": "flaky",
        "type": "attempts",
        "attempts": [
            {"kind": "failure", "message": "one", "retryable": true},
            {"kind": "failure", "message": "two"},
            {"kind": "success", "output": {"n": 3}}
        ]
    }))
    .unwrap();
    let MockPolicy::Attempts { attempts } = &mock.policy else {
        panic!("expected attempts policy");
    };
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts[0],
        MockOutcome::Failure {
            message: "one".into(),
            retryable: true
        }
    );
    // Outcome-level retryable also defaults to false.
    assert_eq!(
        attempts[1],
        MockOutcome::Failure {
            message: "two".into(),
            retryable: false
        }
    );
    assert_eq!(
        attempts[2],
        MockOutcome::Success {
            output: json!({"n": 3})
        }
    );
    let re: MockDef = serde_json::from_value(serde_json::to_value(&mock).unwrap()).unwrap();
    assert_eq!(re, mock);
}

#[test]
fn mock_unknown_policy_type_rejected() {
    let res: Result<MockDef, _> = serde_json::from_value(json!({"handler": "h", "type": "chaos"}));
    assert!(res.is_err());
}

#[test]
fn mock_outcome_unknown_kind_rejected() {
    let res: Result<MockOutcome, _> =
        serde_json::from_value(json!({"kind": "explode", "message": "x"}));
    assert!(res.is_err());
}

#[test]
fn block_targeted_mock_round_trips() {
    let mock: MockDef = serde_json::from_value(json!({
        "block": "notify",
        "type": "success",
        "output": null
    }))
    .unwrap();
    assert_eq!(mock.block.as_deref(), Some("notify"));
    assert_eq!(mock.handler, None);
    let v = serde_json::to_value(&mock).unwrap();
    assert!(!v.as_object().unwrap().contains_key("handler"));
}

// ---------------------------------------------------------------------------
// SignalFixture / Expectations serde
// ---------------------------------------------------------------------------

#[test]
fn signal_fixture_payload_defaults_to_null() {
    let case = case_from(json!({
        "name": "c",
        "signals": [{"signal_type": "resume"}],
        "expect": {}
    }));
    assert_eq!(case.signals.len(), 1);
    assert_eq!(case.signals[0].signal_type, "resume");
    assert_eq!(case.signals[0].payload, Value::Null);
}

#[test]
fn signal_fixture_round_trips_with_payload() {
    let case = case_from(json!({
        "name": "c",
        "signals": [
            {"signal_type": "custom:human_input:gate", "payload": {"value": "yes"}}
        ],
        "expect": {}
    }));
    let re: ContractCase = serde_json::from_value(serde_json::to_value(&case).unwrap()).unwrap();
    assert_eq!(re, case);
    assert_eq!(re.signals[0].payload["value"], json!("yes"));
}

#[test]
fn expectations_default_is_fully_empty() {
    let e = orch8_types::contract::Expectations::default();
    assert_eq!(e.terminal_state, None);
    assert_eq!(e.path, None);
    assert!(e.skipped_blocks.is_empty());
    assert!(e.assertions.is_empty());
    assert!(e.call_counts.is_empty());
}

#[test]
fn empty_expectations_serialize_to_empty_object() {
    let e = orch8_types::contract::Expectations::default();
    assert_eq!(serde_json::to_string(&e).unwrap(), "{}");
}

#[test]
fn path_expectation_ordered_defaults_false() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"path": {"traversed": ["a"]}}
    }));
    assert!(!case.expect.path.unwrap().ordered);
}

#[test]
fn call_count_min_defaults_zero_max_defaults_none() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"call_counts": [{"handler": "h"}]}
    }));
    let cc0 = &case.expect.call_counts[0];
    assert_eq!(cc0.min, 0);
    assert_eq!(cc0.max, None);
}

#[test]
fn assertion_output_subject_shape_parses() {
    let a: Assertion = serde_json::from_value(json!({
        "output": {"block": "charge", "path": "amount"},
        "op": "exists"
    }))
    .unwrap();
    assert_eq!(
        a.subject,
        AssertionSubject::Output {
            block: "charge".into(),
            path: "amount".into()
        }
    );
    assert_eq!(a.op, AssertionOp::Exists);
}

#[test]
fn assertion_context_subject_shape_parses() {
    let a: Assertion = serde_json::from_value(json!({
        "context": {"path": "user.id"},
        "op": "not_exists"
    }))
    .unwrap();
    assert_eq!(
        a.subject,
        AssertionSubject::Context {
            path: "user.id".into()
        }
    );
    assert_eq!(a.op, AssertionOp::NotExists);
}

#[test]
fn every_assertion_op_tag_parses() {
    let shapes = [
        (
            json!({"context": {"path": "x"}, "op": "equals", "value": 1}),
            "equals",
        ),
        (json!({"context": {"path": "x"}, "op": "exists"}), "exists"),
        (
            json!({"context": {"path": "x"}, "op": "not_exists"}),
            "not_exists",
        ),
        (
            json!({"context": {"path": "x"}, "op": "contains", "value": "a"}),
            "contains",
        ),
        (
            json!({"context": {"path": "x"}, "op": "in_range", "min": 0.0}),
            "in_range",
        ),
        (
            json!({"context": {"path": "x"}, "op": "has_type", "expected": "array"}),
            "has_type",
        ),
    ];
    for (shape, tag) in shapes {
        let a: Assertion = serde_json::from_value(shape).unwrap_or_else(|e| {
            panic!("op '{tag}' failed to parse: {e}");
        });
        let back = serde_json::to_value(&a).unwrap();
        assert_eq!(back["op"], json!(tag));
    }
}

#[test]
fn assertion_unknown_op_rejected() {
    let res: Result<Assertion, _> = serde_json::from_value(json!({
        "context": {"path": "x"},
        "op": "matches_regex",
        "value": ".*"
    }));
    assert!(res.is_err());
}

#[test]
fn in_range_min_only_round_trips() {
    let a: Assertion = serde_json::from_value(json!({
        "context": {"path": "n"}, "op": "in_range", "min": 2.5
    }))
    .unwrap();
    assert_eq!(a.op, range_op(Some(2.5), None));
    let v = serde_json::to_value(&a).unwrap();
    assert!(!v.as_object().unwrap().contains_key("max"));
}

#[test]
fn in_range_max_only_round_trips() {
    let a: Assertion = serde_json::from_value(json!({
        "context": {"path": "n"}, "op": "in_range", "max": -1
    }))
    .unwrap();
    assert_eq!(a.op, range_op(None, Some(-1.0)));
    let v = serde_json::to_value(&a).unwrap();
    assert!(!v.as_object().unwrap().contains_key("min"));
}

#[test]
fn expected_terminal_state_serde_all_variants() {
    for (s, variant) in [
        ("completed", ExpectedTerminalState::Completed),
        ("failed", ExpectedTerminalState::Failed),
        ("cancelled", ExpectedTerminalState::Cancelled),
    ] {
        let parsed: ExpectedTerminalState = serde_json::from_value(json!(s)).unwrap();
        assert_eq!(parsed, variant);
        assert_eq!(serde_json::to_value(variant).unwrap(), json!(s));
    }
}

#[test]
fn expected_terminal_state_rejects_non_terminal_names() {
    for bad in ["running", "waiting", "scheduled", "Completed", ""] {
        let res: Result<ExpectedTerminalState, _> = serde_json::from_value(json!(bad));
        assert!(res.is_err(), "'{bad}' should not parse");
    }
}

#[test]
fn complex_suite_round_trip_preserves_equality() {
    let suite = suite_from(json!({
        "schema_version": 1,
        "sequence_name": "checkout",
        "sequence_version": 3,
        "unmocked_handlers": "empty_success",
        "cases": [{
            "name": "full case",
            "description": "everything set",
            "input": {"amount": 250, "items": [1, 2]},
            "config": {"region": "eu"},
            "mocks": [
                {"handler": "charge", "type": "success", "output": {"ok": true}},
                {"block": "notify", "type": "attempts", "attempts": [
                    {"kind": "failure", "message": "x", "retryable": true},
                    {"kind": "success", "output": {}}
                ]},
                {"handler": "fetch", "type": "recorded"}
            ],
            "signals": [{"signal_type": "custom:human_input:gate", "payload": {"value": "no"}}],
            "expect": {
                "terminal_state": "failed",
                "path": {"traversed": ["a", "b"], "ordered": true},
                "skipped_blocks": ["c"],
                "assertions": [
                    {"output": {"block": "a", "path": ""}, "op": "has_type", "expected": "object"},
                    {"context": {"path": "amount"}, "op": "in_range", "min": 0, "max": 1000}
                ],
                "call_counts": [{"handler": "charge", "min": 1, "max": 2}]
            },
            "max_logical_duration_ms": 1000,
            "max_ticks": 99
        }]
    }));
    let text = serde_json::to_string(&suite).unwrap();
    let re: ContractSuite = serde_json::from_str(&text).unwrap();
    assert_eq!(re, suite);
}

#[test]
fn case_report_minimal_deserializes_with_defaults() {
    let report: CaseReport = serde_json::from_value(json!({
        "name": "c",
        "passed": true,
        "final_state": "completed",
        "ticks": 4,
        "logical_duration_ms": 0
    }))
    .unwrap();
    assert!(report.failures.is_empty());
    assert!(report.executed_blocks.is_empty());
    assert!(report.handler_calls.is_empty());
}

#[test]
fn case_report_omits_empty_failures_when_serialized() {
    let v = serde_json::to_value(case_report("ok", true)).unwrap();
    assert!(!v.as_object().unwrap().contains_key("failures"));
    let v = serde_json::to_value(case_report("bad", false)).unwrap();
    assert_eq!(v["failures"], json!(["bad diverged"]));
}

#[test]
fn suite_report_round_trips() {
    let report = SuiteReport {
        sequence_name: "s".into(),
        sequence_version: 9,
        passed: false,
        cases: vec![case_report("a", true), case_report("b", false)],
    };
    let re: SuiteReport = serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
    assert_eq!(re, report);
}

// ---------------------------------------------------------------------------
// ContractSuite::validate
// ---------------------------------------------------------------------------

#[test]
fn validate_accepts_minimal_suite() {
    minimal_suite().validate().unwrap();
}

#[test]
fn validate_rejects_schema_version_zero() {
    let mut suite = minimal_suite();
    suite.schema_version = 0;
    let err = suite.validate().unwrap_err();
    assert!(err.contains("schema_version 0"), "{err}");
}

#[test]
fn validate_rejects_next_schema_version() {
    let mut suite = minimal_suite();
    suite.schema_version = CONTRACT_SCHEMA_VERSION + 1;
    let err = suite.validate().unwrap_err();
    assert!(err.contains("unsupported"), "{err}");
}

#[test]
fn validate_error_names_the_supported_version() {
    let mut suite = minimal_suite();
    suite.schema_version = 42;
    let err = suite.validate().unwrap_err();
    assert!(
        err.contains(&format!("supports {CONTRACT_SCHEMA_VERSION}")),
        "{err}"
    );
}

#[test]
fn validate_rejects_suite_without_cases() {
    let suite = suite_from(json!({"cases": []}));
    assert_eq!(suite.validate().unwrap_err(), "contract suite has no cases");
}

#[test]
fn validate_rejects_whitespace_only_case_name() {
    let suite = suite_from(json!({"cases": [{"name": "   ", "expect": {}}]}));
    assert!(suite.validate().unwrap_err().contains("empty name"));
}

#[test]
fn validate_rejects_empty_case_name() {
    let suite = suite_from(json!({"cases": [{"name": "", "expect": {}}]}));
    assert!(suite.validate().unwrap_err().contains("empty name"));
}

#[test]
fn validate_duplicate_error_names_the_case() {
    let suite = suite_from(json!({"cases": [
        {"name": "same", "expect": {}},
        {"name": "other", "expect": {}},
        {"name": "same", "expect": {}}
    ]}));
    let err = suite.validate().unwrap_err();
    assert_eq!(err, "duplicate case name: same");
}

#[test]
fn validate_accepts_many_distinct_case_names() {
    let suite = suite_from(json!({"cases": [
        {"name": "a", "expect": {}},
        {"name": "b", "expect": {}},
        {"name": "a ", "expect": {}}
    ]}));
    // Trailing whitespace makes the name distinct for dedup purposes.
    suite.validate().unwrap();
}

#[test]
fn validate_prefixes_case_errors_with_case_name() {
    let suite = suite_from(json!({"cases": [
        {"name": "good", "expect": {}},
        {
            "name": "broken",
            "mocks": [{"type": "recorded"}],
            "expect": {}
        }
    ]}));
    let err = suite.validate().unwrap_err();
    assert!(err.starts_with("case 'broken':"), "{err}");
}

// ---------------------------------------------------------------------------
// ContractCase::validate
// ---------------------------------------------------------------------------

#[test]
fn case_rejects_mock_without_target() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"type": "success", "output": {}}],
        "expect": {}
    }));
    assert!(
        case.validate()
            .unwrap_err()
            .contains("either 'handler' or 'block'")
    );
}

#[test]
fn case_rejects_mock_with_both_targets() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"handler": "h", "block": "b", "type": "recorded"}],
        "expect": {}
    }));
    assert!(case.validate().unwrap_err().contains("not both"));
}

#[test]
fn case_accepts_handler_only_mock() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"handler": "h", "type": "recorded"}],
        "expect": {}
    }));
    case.validate().unwrap();
}

#[test]
fn case_accepts_block_only_mock() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"block": "b", "type": "failure", "message": "m"}],
        "expect": {}
    }));
    case.validate().unwrap();
}

#[test]
fn case_rejects_attempts_mock_with_no_attempts() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"handler": "h", "type": "attempts", "attempts": []}],
        "expect": {}
    }));
    assert!(case.validate().unwrap_err().contains("empty attempts"));
}

#[test]
fn case_accepts_single_attempt_mock() {
    let case = case_from(json!({
        "name": "c",
        "mocks": [{"handler": "h", "type": "attempts", "attempts": [
            {"kind": "success", "output": {}}
        ]}],
        "expect": {}
    }));
    case.validate().unwrap();
}

#[test]
fn case_rejects_call_count_min_above_max() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"call_counts": [{"handler": "h", "min": 2, "max": 1}]}
    }));
    let err = case.validate().unwrap_err();
    assert!(err.contains("min 2 > max 1"), "{err}");
}

#[test]
fn case_accepts_call_count_min_equal_max() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"call_counts": [{"handler": "h", "min": 5, "max": 5}]}
    }));
    case.validate().unwrap();
}

#[test]
fn case_accepts_zero_zero_call_count() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"call_counts": [{"handler": "h", "min": 0, "max": 0}]}
    }));
    case.validate().unwrap();
}

#[test]
fn case_accepts_large_min_with_unbounded_max() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"call_counts": [{"handler": "h", "min": u32::MAX}]}
    }));
    case.validate().unwrap();
}

#[test]
fn case_rejects_empty_traversed_path() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"path": {"traversed": [], "ordered": true}}
    }));
    assert!(case.validate().unwrap_err().contains("empty traversed"));
}

#[test]
fn case_accepts_single_block_path() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"path": {"traversed": ["only"]}}
    }));
    case.validate().unwrap();
}

#[test]
fn case_rejects_in_range_with_no_bounds() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"assertions": [
            {"context": {"path": "n"}, "op": "in_range"}
        ]}
    }));
    assert!(case.validate().unwrap_err().contains("neither min nor max"));
}

#[test]
fn case_accepts_in_range_with_min_only() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"assertions": [
            {"context": {"path": "n"}, "op": "in_range", "min": 0}
        ]}
    }));
    case.validate().unwrap();
}

#[test]
fn case_accepts_in_range_with_max_only() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"assertions": [
            {"context": {"path": "n"}, "op": "in_range", "max": 0}
        ]}
    }));
    case.validate().unwrap();
}

#[test]
fn case_accepts_every_known_has_type() {
    for t in ["string", "number", "boolean", "array", "object", "null"] {
        let case = case_from(json!({
            "name": "c",
            "expect": {"assertions": [
                {"context": {"path": "x"}, "op": "has_type", "expected": t}
            ]}
        }));
        case.validate()
            .unwrap_or_else(|e| panic!("type '{t}' rejected: {e}"));
    }
}

#[test]
fn case_rejects_capitalized_has_type() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"assertions": [
            {"context": {"path": "x"}, "op": "has_type", "expected": "String"}
        ]}
    }));
    assert!(
        case.validate()
            .unwrap_err()
            .contains("unknown type 'String'")
    );
}

#[test]
fn case_rejects_empty_has_type() {
    let case = case_from(json!({
        "name": "c",
        "expect": {"assertions": [
            {"context": {"path": "x"}, "op": "has_type", "expected": ""}
        ]}
    }));
    assert!(case.validate().unwrap_err().contains("unknown type ''"));
}

// ---------------------------------------------------------------------------
// resolve_path
// ---------------------------------------------------------------------------

#[test]
fn resolve_six_levels_of_object_nesting() {
    let v = json!({"a": {"b": {"c": {"d": {"e": {"f": "deep"}}}}}});
    assert_eq!(resolve_path(&v, "a.b.c.d.e.f"), Some(&json!("deep")));
}

#[test]
fn resolve_nested_arrays_by_index() {
    let v = json!([[10, 11], [20, 21]]);
    assert_eq!(resolve_path(&v, "1.0"), Some(&json!(20)));
    assert_eq!(resolve_path(&v, "0.1"), Some(&json!(11)));
}

#[test]
fn resolve_alternating_objects_and_arrays() {
    let v = json!({"orders": [{"lines": [{"sku": "X1"}]}]});
    assert_eq!(resolve_path(&v, "orders.0.lines.0.sku"), Some(&json!("X1")));
}

#[test]
fn resolve_numeric_segment_is_a_key_on_objects() {
    // On an object, "0" is a map key lookup, not an index.
    let v = json!({"0": "zero-key", "1": {"2": "nested"}});
    assert_eq!(resolve_path(&v, "0"), Some(&json!("zero-key")));
    assert_eq!(resolve_path(&v, "1.2"), Some(&json!("nested")));
}

#[test]
fn resolve_array_index_accepts_leading_zeros() {
    // usize::from_str accepts "01", so "01" addresses index 1.
    let v = json!(["a", "b", "c"]);
    assert_eq!(resolve_path(&v, "01"), Some(&json!("b")));
    assert_eq!(resolve_path(&v, "002"), Some(&json!("c")));
}

#[test]
fn resolve_array_index_accepts_plus_sign() {
    // usize::from_str accepts a leading '+'; documented quirk.
    let v = json!(["a", "b"]);
    assert_eq!(resolve_path(&v, "+1"), Some(&json!("b")));
}

#[test]
fn resolve_rejects_negative_array_index() {
    let v = json!(["a", "b"]);
    assert_eq!(resolve_path(&v, "-1"), None);
}

#[test]
fn resolve_rejects_index_beyond_usize() {
    let v = json!(["a"]);
    assert_eq!(resolve_path(&v, "18446744073709551616"), None);
}

#[test]
fn resolve_rejects_float_style_index() {
    let v = json!(["a", "b"]);
    assert_eq!(resolve_path(&v, "1.0"), None); // splits into "1" then "0" into a scalar
    assert_eq!(resolve_path(&v, "1e0"), None);
}

#[test]
fn resolve_empty_middle_segment_is_none() {
    let v = json!({"a": {"b": 1}});
    assert_eq!(resolve_path(&v, "a..b"), None);
}

#[test]
fn resolve_trailing_dot_is_none() {
    let v = json!({"a": {"b": 1}});
    assert_eq!(resolve_path(&v, "a.b."), None);
}

#[test]
fn resolve_leading_dot_is_none() {
    let v = json!({"a": 1});
    assert_eq!(resolve_path(&v, ".a"), None);
}

#[test]
fn resolve_empty_key_matches_actual_empty_key() {
    // An object genuinely keyed by "" is addressable via an empty segment.
    let v = json!({"": {"x": 7}});
    assert_eq!(resolve_path(&v, ".x"), Some(&json!(7)));
}

#[test]
fn resolve_cannot_descend_into_scalars() {
    let v = json!({"a": 42, "s": "text", "b": true});
    assert_eq!(resolve_path(&v, "a.anything"), None);
    assert_eq!(resolve_path(&v, "s.0"), None);
    assert_eq!(resolve_path(&v, "b.x"), None);
}

#[test]
fn resolve_cannot_descend_into_null() {
    let v = json!({"a": null});
    assert_eq!(resolve_path(&v, "a.b"), None);
}

#[test]
fn resolve_null_leaf_is_some_null() {
    let v = json!({"a": {"b": null}});
    assert_eq!(resolve_path(&v, "a.b"), Some(&Value::Null));
}

#[test]
fn resolve_empty_path_returns_scalar_root() {
    let v = json!(3.5);
    assert_eq!(resolve_path(&v, ""), Some(&v));
    let n = Value::Null;
    assert_eq!(resolve_path(&n, ""), Some(&Value::Null));
}

#[test]
fn resolve_unicode_and_spaced_keys() {
    let v = json!({"ключ": {"with space": 1}});
    assert_eq!(resolve_path(&v, "ключ.with space"), Some(&json!(1)));
}

#[test]
fn resolve_index_into_empty_array_is_none() {
    let v = json!({"items": []});
    assert_eq!(resolve_path(&v, "items.0"), None);
}

// ---------------------------------------------------------------------------
// check_op: Equals
// ---------------------------------------------------------------------------

#[test]
fn equals_null_matches_explicit_null() {
    check_op(&eq_op(json!(null)), Some(&json!(null))).unwrap();
}

#[test]
fn equals_null_expected_but_path_absent_fails() {
    // Equality against null still requires the path to resolve.
    let err = check_op(&eq_op(json!(null)), None).unwrap_err();
    assert!(err.contains("absent"), "{err}");
}

#[test]
fn equals_integer_and_float_forms_are_distinct() {
    // serde_json numbers: 1 (integer) != 1.0 (float). Contract authors must
    // match the mock's numeric representation exactly.
    assert!(check_op(&eq_op(json!(1)), Some(&json!(1.0))).is_err());
    assert!(check_op(&eq_op(json!(1.0)), Some(&json!(1))).is_err());
    check_op(&eq_op(json!(1)), Some(&json!(1))).unwrap();
    check_op(&eq_op(json!(1.0)), Some(&json!(1.0))).unwrap();
}

#[test]
fn equals_arrays_are_order_sensitive() {
    assert!(check_op(&eq_op(json!([1, 2])), Some(&json!([2, 1]))).is_err());
    check_op(&eq_op(json!([1, 2])), Some(&json!([1, 2]))).unwrap();
}

#[test]
fn equals_objects_ignore_key_order() {
    let expected = serde_json::from_str::<Value>(r#"{"a":1,"b":2}"#).unwrap();
    let actual = serde_json::from_str::<Value>(r#"{"b":2,"a":1}"#).unwrap();
    check_op(&eq_op(expected), Some(&actual)).unwrap();
}

#[test]
fn equals_matches_deeply_nested_structures() {
    let v = json!({"a": [{"b": {"c": [null, true, "x"]}}]});
    check_op(&eq_op(v.clone()), Some(&v)).unwrap();
    let almost = json!({"a": [{"b": {"c": [null, false, "x"]}}]});
    assert!(check_op(&eq_op(v), Some(&almost)).is_err());
}

#[test]
fn equals_bool_mismatch_reports_both_sides() {
    let err = check_op(&eq_op(json!(true)), Some(&json!(false))).unwrap_err();
    assert!(err.contains("expected true"), "{err}");
    assert!(err.contains("got false"), "{err}");
}

#[test]
fn equals_distinguishes_empty_containers() {
    check_op(&eq_op(json!({})), Some(&json!({}))).unwrap();
    check_op(&eq_op(json!([])), Some(&json!([]))).unwrap();
    assert!(check_op(&eq_op(json!({})), Some(&json!([]))).is_err());
}

#[test]
fn equals_string_number_cross_type_fails() {
    assert!(check_op(&eq_op(json!("1")), Some(&json!(1))).is_err());
    assert!(check_op(&eq_op(json!(0)), Some(&json!(false))).is_err());
}

// ---------------------------------------------------------------------------
// check_op: Exists / NotExists
// ---------------------------------------------------------------------------

#[test]
fn exists_accepts_falsey_but_present_values() {
    for v in [json!(0), json!(""), json!(false), json!([]), json!({})] {
        check_op(&AssertionOp::Exists, Some(&v))
            .unwrap_or_else(|e| panic!("{v} should exist: {e}"));
    }
}

#[test]
fn not_exists_rejects_every_non_null_value() {
    for v in [
        json!(0),
        json!(""),
        json!(false),
        json!([]),
        json!({}),
        json!("null"),
    ] {
        assert!(
            check_op(&AssertionOp::NotExists, Some(&v)).is_err(),
            "{v} should violate not_exists"
        );
    }
}

// ---------------------------------------------------------------------------
// check_op: Contains
// ---------------------------------------------------------------------------

#[test]
fn contains_empty_string_needle_always_matches_strings() {
    check_op(&contains_op(json!("")), Some(&json!("anything"))).unwrap();
    check_op(&contains_op(json!("")), Some(&json!(""))).unwrap();
}

#[test]
fn contains_string_matches_itself() {
    check_op(&contains_op(json!("whole")), Some(&json!("whole"))).unwrap();
}

#[test]
fn contains_string_is_case_sensitive() {
    assert!(check_op(&contains_op(json!("Hello")), Some(&json!("hello world"))).is_err());
    check_op(&contains_op(json!("hello")), Some(&json!("hello world"))).unwrap();
}

#[test]
fn contains_array_finds_null_element() {
    check_op(&contains_op(json!(null)), Some(&json!([1, null, 2]))).unwrap();
    assert!(check_op(&contains_op(json!(null)), Some(&json!([1, 2]))).is_err());
}

#[test]
fn contains_array_finds_object_element() {
    let arr = json!([{"id": 1}, {"id": 2}]);
    check_op(&contains_op(json!({"id": 2})), Some(&arr)).unwrap();
    assert!(check_op(&contains_op(json!({"id": 3})), Some(&arr)).is_err());
}

#[test]
fn contains_array_finds_nested_array_element() {
    let arr = json!([[1, 2], [3]]);
    check_op(&contains_op(json!([1, 2])), Some(&arr)).unwrap();
    // Element equality is exact: a sub-slice is not membership.
    assert!(check_op(&contains_op(json!([1])), Some(&arr)).is_err());
}

#[test]
fn contains_array_element_respects_numeric_representation() {
    // [1] contains 1 but not 1.0 (int vs float are distinct values).
    check_op(&contains_op(json!(1)), Some(&json!([1]))).unwrap();
    assert!(check_op(&contains_op(json!(1.0)), Some(&json!([1]))).is_err());
}

#[test]
fn contains_empty_object_subset_always_matches_objects() {
    check_op(&contains_op(json!({})), Some(&json!({"a": 1}))).unwrap();
    check_op(&contains_op(json!({})), Some(&json!({}))).unwrap();
}

#[test]
fn contains_object_multi_pair_subset() {
    let actual = json!({"a": 1, "b": "x", "c": [true]});
    check_op(&contains_op(json!({"a": 1, "c": [true]})), Some(&actual)).unwrap();
    assert!(check_op(&contains_op(json!({"a": 1, "d": 0})), Some(&actual)).is_err());
}

#[test]
fn contains_object_subset_values_must_match_exactly() {
    let actual = json!({"user": {"id": 1, "name": "n"}});
    // The subset value is compared with deep equality, not sub-subset logic.
    assert!(check_op(&contains_op(json!({"user": {"id": 1}})), Some(&actual)).is_err());
    check_op(
        &contains_op(json!({"user": {"id": 1, "name": "n"}})),
        Some(&actual),
    )
    .unwrap();
}

#[test]
fn contains_on_non_container_values_fails() {
    assert!(check_op(&contains_op(json!(2)), Some(&json!(42))).is_err());
    assert!(check_op(&contains_op(json!(true)), Some(&json!(true))).is_err());
    assert!(check_op(&contains_op(json!("x")), Some(&json!(null))).is_err());
}

#[test]
fn contains_object_needle_on_string_container_fails() {
    assert!(check_op(&contains_op(json!({"a": 1})), Some(&json!("{\"a\":1}"))).is_err());
}

#[test]
fn contains_absent_path_reports_absence() {
    let err = check_op(&contains_op(json!("x")), None).unwrap_err();
    assert!(err.contains("absent"), "{err}");
}

// ---------------------------------------------------------------------------
// check_op: InRange
// ---------------------------------------------------------------------------

#[test]
fn in_range_exact_min_boundary_is_inclusive() {
    check_op(&range_op(Some(10.0), Some(20.0)), Some(&json!(10))).unwrap();
    check_op(&range_op(Some(10.0), Some(20.0)), Some(&json!(10.0))).unwrap();
}

#[test]
fn in_range_exact_max_boundary_is_inclusive() {
    check_op(&range_op(Some(10.0), Some(20.0)), Some(&json!(20))).unwrap();
    check_op(&range_op(Some(10.0), Some(20.0)), Some(&json!(20.0))).unwrap();
}

#[test]
fn in_range_min_equals_max_accepts_only_that_value() {
    let op = range_op(Some(7.0), Some(7.0));
    check_op(&op, Some(&json!(7))).unwrap();
    assert!(check_op(&op, Some(&json!(6.999))).is_err());
    assert!(check_op(&op, Some(&json!(7.001))).is_err());
}

#[test]
fn in_range_handles_negative_bounds() {
    let op = range_op(Some(-10.0), Some(-1.0));
    check_op(&op, Some(&json!(-5))).unwrap();
    check_op(&op, Some(&json!(-10))).unwrap();
    check_op(&op, Some(&json!(-1))).unwrap();
    assert!(
        check_op(&op, Some(&json!(0)))
            .unwrap_err()
            .contains("above")
    );
    assert!(
        check_op(&op, Some(&json!(-11)))
            .unwrap_err()
            .contains("below")
    );
}

#[test]
fn in_range_accepts_fractional_values_inside_integer_bounds() {
    check_op(&range_op(Some(1.0), Some(2.0)), Some(&json!(1.5))).unwrap();
    assert!(check_op(&range_op(Some(1.0), Some(2.0)), Some(&json!(2.5))).is_err());
}

#[test]
fn in_range_handles_large_u64_values() {
    let op = range_op(Some(0.0), None);
    check_op(&op, Some(&json!(u64::MAX))).unwrap();
    let op = range_op(None, Some(1.0));
    assert!(check_op(&op, Some(&json!(u64::MAX))).is_err());
}

#[test]
fn in_range_min_only_is_unbounded_above() {
    check_op(&range_op(Some(0.0), None), Some(&json!(1e300))).unwrap();
    assert!(check_op(&range_op(Some(0.0), None), Some(&json!(-0.001))).is_err());
}

#[test]
fn in_range_max_only_is_unbounded_below() {
    check_op(&range_op(None, Some(0.0)), Some(&json!(-1e300))).unwrap();
    assert!(check_op(&range_op(None, Some(0.0)), Some(&json!(0.001))).is_err());
}

#[test]
fn in_range_rejects_boolean_values() {
    // true is not coerced to 1.
    let err = check_op(&range_op(Some(0.0), Some(2.0)), Some(&json!(true))).unwrap_err();
    assert!(err.contains("expected a number"), "{err}");
}

#[test]
fn in_range_rejects_null_and_names_it() {
    let err = check_op(&range_op(Some(0.0), None), Some(&json!(null))).unwrap_err();
    assert!(err.contains("expected a number"), "{err}");
    assert!(err.contains("null"), "{err}");
}

#[test]
fn in_range_rejects_arrays_and_objects() {
    assert!(check_op(&range_op(Some(0.0), None), Some(&json!([1]))).is_err());
    assert!(check_op(&range_op(Some(0.0), None), Some(&json!({"n": 1}))).is_err());
}

#[test]
fn in_range_absent_path_reports_absent() {
    let err = check_op(&range_op(Some(0.0), Some(1.0)), None).unwrap_err();
    assert!(err.contains("absent"), "{err}");
}

#[test]
fn in_range_numeric_string_is_not_a_number() {
    assert!(check_op(&range_op(Some(0.0), Some(10.0)), Some(&json!("5"))).is_err());
}

// ---------------------------------------------------------------------------
// check_op: HasType
// ---------------------------------------------------------------------------

#[test]
fn has_type_number_accepts_integers_and_floats() {
    check_op(&type_op("number"), Some(&json!(0))).unwrap();
    check_op(&type_op("number"), Some(&json!(-3))).unwrap();
    check_op(&type_op("number"), Some(&json!(2.75))).unwrap();
    check_op(&type_op("number"), Some(&json!(u64::MAX))).unwrap();
}

#[test]
fn has_type_null_only_matches_null_value() {
    check_op(&type_op("null"), Some(&json!(null))).unwrap();
    assert!(check_op(&type_op("null"), Some(&json!(0))).is_err());
    // But an absent path is NOT type null — it is an error.
    let err = check_op(&type_op("null"), None).unwrap_err();
    assert!(err.contains("absent"), "{err}");
}

#[test]
fn has_type_mismatch_names_the_actual_type() {
    let err = check_op(&type_op("array"), Some(&json!({"k": 1}))).unwrap_err();
    assert!(err.contains("expected type 'array'"), "{err}");
    assert!(err.contains("got 'object'"), "{err}");
}

#[test]
fn has_type_object_and_array_are_not_interchangeable() {
    assert!(check_op(&type_op("object"), Some(&json!([]))).is_err());
    assert!(check_op(&type_op("array"), Some(&json!({}))).is_err());
}

#[test]
fn has_type_boolean_rejects_zero_and_one() {
    assert!(check_op(&type_op("boolean"), Some(&json!(0))).is_err());
    assert!(check_op(&type_op("boolean"), Some(&json!(1))).is_err());
    check_op(&type_op("boolean"), Some(&json!(false))).unwrap();
}

#[test]
fn has_type_string_rejects_stringly_numbers_reversed() {
    check_op(&type_op("string"), Some(&json!("123"))).unwrap();
    assert!(check_op(&type_op("number"), Some(&json!("123"))).is_err());
}

#[test]
fn has_type_absent_path_message_names_expected_type() {
    let err = check_op(&type_op("object"), None).unwrap_err();
    assert!(err.contains("expected type 'object'"), "{err}");
}

// ---------------------------------------------------------------------------
// evaluate_assertion
// ---------------------------------------------------------------------------

fn outputs_with(block: &str, v: Value) -> BTreeMap<String, Value> {
    let mut m = BTreeMap::new();
    m.insert(block.to_string(), v);
    m
}

#[test]
fn assertion_output_empty_path_reads_whole_output() {
    let outputs = outputs_with("charge", json!({"ok": true}));
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "charge".into(),
            path: String::new(),
        },
        op: eq_op(json!({"ok": true})),
    };
    evaluate_assertion(&a, &outputs, &json!({})).unwrap();
}

#[test]
fn assertion_output_deep_path_resolves() {
    let outputs = outputs_with("fetch", json!({"body": {"items": [{"id": 9}]}}));
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "fetch".into(),
            path: "body.items.0.id".into(),
        },
        op: eq_op(json!(9)),
    };
    evaluate_assertion(&a, &outputs, &json!({})).unwrap();
}

#[test]
fn assertion_equals_on_missing_block_fails() {
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "ghost".into(),
            path: "x".into(),
        },
        op: eq_op(json!(1)),
    };
    let err = evaluate_assertion(&a, &BTreeMap::new(), &json!({})).unwrap_err();
    assert!(err.contains("output.ghost"), "{err}");
    assert!(err.contains("no output"), "{err}");
}

#[test]
fn assertion_exists_on_missing_block_fails_but_not_exists_passes() {
    let mk = |op: AssertionOp| Assertion {
        subject: AssertionSubject::Output {
            block: "never_ran".into(),
            path: String::new(),
        },
        op,
    };
    assert!(evaluate_assertion(&mk(AssertionOp::Exists), &BTreeMap::new(), &json!({})).is_err());
    evaluate_assertion(&mk(AssertionOp::NotExists), &BTreeMap::new(), &json!({})).unwrap();
}

#[test]
fn assertion_not_exists_passes_for_missing_path_in_present_block() {
    let outputs = outputs_with("step", json!({"present": 1}));
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "step".into(),
            path: "missing".into(),
        },
        op: AssertionOp::NotExists,
    };
    evaluate_assertion(&a, &outputs, &json!({})).unwrap();
}

#[test]
fn assertion_output_error_is_prefixed_with_block_and_path() {
    let outputs = outputs_with("charge", json!({"amount": 5}));
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "charge".into(),
            path: "amount".into(),
        },
        op: eq_op(json!(6)),
    };
    let err = evaluate_assertion(&a, &outputs, &json!({})).unwrap_err();
    assert!(err.starts_with("output.charge.amount:"), "{err}");
}

#[test]
fn assertion_context_error_is_prefixed_with_path() {
    let a = Assertion {
        subject: AssertionSubject::Context { path: "a.b".into() },
        op: AssertionOp::Exists,
    };
    let err = evaluate_assertion(&a, &BTreeMap::new(), &json!({})).unwrap_err();
    assert!(err.starts_with("context.a.b:"), "{err}");
}

#[test]
fn assertion_context_empty_path_reads_whole_data() {
    let data = json!({"only": "this"});
    let a = Assertion {
        subject: AssertionSubject::Context {
            path: String::new(),
        },
        op: eq_op(data.clone()),
    };
    evaluate_assertion(&a, &BTreeMap::new(), &data).unwrap();
}

#[test]
fn assertion_contains_applies_to_output_subject() {
    let outputs = outputs_with("list", json!({"tags": ["red", "green"]}));
    let a = Assertion {
        subject: AssertionSubject::Output {
            block: "list".into(),
            path: "tags".into(),
        },
        op: contains_op(json!("green")),
    };
    evaluate_assertion(&a, &outputs, &json!({})).unwrap();
}

#[test]
fn assertion_in_range_applies_to_context_subject() {
    let a = Assertion {
        subject: AssertionSubject::Context {
            path: "score".into(),
        },
        op: range_op(Some(0.0), Some(100.0)),
    };
    evaluate_assertion(&a, &BTreeMap::new(), &json!({"score": 88})).unwrap();
    assert!(evaluate_assertion(&a, &BTreeMap::new(), &json!({"score": 101})).is_err());
}

// ---------------------------------------------------------------------------
// check_path
// ---------------------------------------------------------------------------

#[test]
fn unordered_duplicate_expectations_only_need_membership() {
    // Unordered mode is a pure membership check: expecting [a, a] is
    // satisfied by a single execution of 'a'. Ordered mode is stricter.
    let e = path_expect(&["a", "a"], false);
    check_path(&e, &hist(&["a"])).unwrap();
}

#[test]
fn unordered_empty_history_fails() {
    let e = path_expect(&["a"], false);
    let err = check_path(&e, &[]).unwrap_err();
    assert!(err.contains("'a'"), "{err}");
}

#[test]
fn ordered_empty_history_fails_with_missing_not_out_of_order() {
    let e = path_expect(&["a"], true);
    let err = check_path(&e, &[]).unwrap_err();
    assert!(err.contains("did not"), "{err}");
    assert!(!err.contains("out of expected order"), "{err}");
}

#[test]
fn ordered_allows_arbitrary_interleaving() {
    let e = path_expect(&["a", "c", "e"], true);
    check_path(&e, &hist(&["x", "a", "b", "c", "d", "e", "f"])).unwrap();
}

#[test]
fn ordered_single_block_matches_anywhere() {
    let e = path_expect(&["m"], true);
    check_path(&e, &hist(&["x", "m", "y"])).unwrap();
    check_path(&e, &hist(&["m"])).unwrap();
}

#[test]
fn ordered_block_only_before_cursor_is_out_of_order() {
    // 'b' consumed at index 1; 'a' exists only at index 0 (< cursor).
    let e = path_expect(&["b", "a"], true);
    let err = check_path(&e, &hist(&["a", "b"])).unwrap_err();
    assert!(err.contains("out of expected order"), "{err}");
    assert!(err.contains("'a'"), "{err}");
}

#[test]
fn ordered_expectation_satisfied_by_later_occurrence() {
    // [b, a] is fine when 'a' runs again after 'b'.
    let e = path_expect(&["b", "a"], true);
    check_path(&e, &hist(&["a", "b", "a"])).unwrap();
}

#[test]
fn ordered_triple_visit_needs_three_occurrences() {
    let e = path_expect(&["a", "a", "a"], true);
    check_path(&e, &hist(&["a", "x", "a", "y", "a"])).unwrap();
    assert!(check_path(&e, &hist(&["a", "x", "a"])).is_err());
}

#[test]
fn empty_traversed_is_trivially_satisfied() {
    // validate() rejects this shape, but the pure function tolerates it.
    for ordered in [false, true] {
        let e = PathExpectation {
            traversed: vec![],
            ordered,
        };
        check_path(&e, &[]).unwrap();
        check_path(&e, &hist(&["a"])).unwrap();
    }
}

#[test]
fn unordered_subset_of_longer_history_passes() {
    let e = path_expect(&["z", "a"], false);
    check_path(&e, &hist(&["a", "b", "c", "z"])).unwrap();
}

// ---------------------------------------------------------------------------
// check_call_counts
// ---------------------------------------------------------------------------

#[test]
fn no_expectations_always_pass() {
    check_call_counts(&[], &BTreeMap::new()).unwrap();
    check_call_counts(&[], &calls_map(&[("h", 100)])).unwrap();
}

#[test]
fn exact_count_window_passes() {
    check_call_counts(&[cc("h", 3, Some(3))], &calls_map(&[("h", 3)])).unwrap();
}

#[test]
fn zero_max_fails_when_handler_was_called() {
    let err = check_call_counts(&[cc("h", 0, Some(0))], &calls_map(&[("h", 1)])).unwrap_err();
    assert!(err.contains("at most 0"), "{err}");
}

#[test]
fn zero_max_passes_when_handler_never_called() {
    check_call_counts(&[cc("h", 0, Some(0))], &BTreeMap::new()).unwrap();
}

#[test]
fn min_boundary_is_inclusive() {
    check_call_counts(&[cc("h", 2, None)], &calls_map(&[("h", 2)])).unwrap();
    assert!(check_call_counts(&[cc("h", 2, None)], &calls_map(&[("h", 1)])).is_err());
}

#[test]
fn max_boundary_is_inclusive() {
    check_call_counts(&[cc("h", 0, Some(2))], &calls_map(&[("h", 2)])).unwrap();
    assert!(check_call_counts(&[cc("h", 0, Some(2))], &calls_map(&[("h", 3)])).is_err());
}

#[test]
fn first_violated_expectation_is_reported() {
    let expectations = [cc("a", 1, None), cc("b", 1, None)];
    // 'a' passes, 'b' fails: the error names 'b'.
    let err = check_call_counts(&expectations, &calls_map(&[("a", 1)])).unwrap_err();
    assert!(err.contains("'b'"), "{err}");
    // Both fail: evaluation order reports 'a' first.
    let err = check_call_counts(&expectations, &BTreeMap::new()).unwrap_err();
    assert!(err.contains("'a'"), "{err}");
}

#[test]
fn unlisted_handlers_in_calls_map_are_ignored() {
    check_call_counts(
        &[cc("watched", 1, Some(1))],
        &calls_map(&[("watched", 1), ("noise", 999)]),
    )
    .unwrap();
}

#[test]
fn violation_message_includes_observed_count() {
    let err = check_call_counts(&[cc("h", 5, None)], &calls_map(&[("h", 2)])).unwrap_err();
    assert!(err.contains("called 2 times"), "{err}");
    assert!(err.contains("at least 5"), "{err}");
}

// ---------------------------------------------------------------------------
// ExpectedTerminalState / reports
// ---------------------------------------------------------------------------

#[test]
fn terminal_state_strings_are_lowercase_engine_names() {
    assert_eq!(ExpectedTerminalState::Completed.as_state_str(), "completed");
    assert_eq!(ExpectedTerminalState::Failed.as_state_str(), "failed");
    assert_eq!(ExpectedTerminalState::Cancelled.as_state_str(), "cancelled");
}

#[test]
fn terminal_state_str_matches_its_serde_form() {
    for v in [
        ExpectedTerminalState::Completed,
        ExpectedTerminalState::Failed,
        ExpectedTerminalState::Cancelled,
    ] {
        assert_eq!(
            serde_json::to_value(v).unwrap(),
            json!(v.as_state_str()),
            "serde and as_state_str must agree for {v:?}"
        );
    }
}

#[test]
fn failed_cases_is_empty_when_everything_passed() {
    let report = SuiteReport {
        sequence_name: "s".into(),
        sequence_version: 1,
        passed: true,
        cases: vec![case_report("a", true), case_report("b", true)],
    };
    assert!(report.failed_cases().is_empty());
}

#[test]
fn failed_cases_preserves_case_order() {
    let report = SuiteReport {
        sequence_name: "s".into(),
        sequence_version: 1,
        passed: false,
        cases: vec![
            case_report("first-bad", false),
            case_report("fine", true),
            case_report("second-bad", false),
        ],
    };
    let failed = report.failed_cases();
    assert_eq!(failed.len(), 2);
    assert_eq!(failed[0].name, "first-bad");
    assert_eq!(failed[1].name, "second-bad");
}

#[test]
fn failed_cases_on_empty_suite_report() {
    let report = SuiteReport {
        sequence_name: "s".into(),
        sequence_version: 1,
        passed: true,
        cases: vec![],
    };
    assert!(report.failed_cases().is_empty());
}
