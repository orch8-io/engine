//! Workflow contracts: a portable, version-controlled test artifact for
//! sequences.
//!
//! A contract suite lives beside a sequence definition
//! (`checkout.json` / `checkout.contracts.json`) and declares, per case:
//! the input fixture, mocked handler behavior, and the expected path,
//! terminal state, outputs, and invariants. The engine's contract runner
//! executes cases against an embedded engine under virtual time — mocks
//! never invoke real handlers, and delays never sleep on the wall clock.
//!
//! This module holds the artifact types, their validation, and the pure
//! assertion-evaluation logic. Driving the engine lives in the `orch8`
//! facade crate so the CLI and release validation share one runner.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Current contract schema version. Bump only with a migration story.
pub const CONTRACT_SCHEMA_VERSION: u32 = 1;

fn default_schema_version() -> u32 {
    CONTRACT_SCHEMA_VERSION
}

/// A suite of contract cases for one sequence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ContractSuite {
    /// Contract format version; readers must reject versions they do not
    /// understand rather than misinterpret them.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// The sequence this suite tests, by name. Informational when the
    /// runner is handed a definition directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_name: Option<String>,
    /// The sequence version this suite was written against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_version: Option<i64>,
    /// What happens when a step's handler has no mock:
    /// fail the case (default, safest) or return an empty success.
    #[serde(default)]
    pub unmocked_handlers: UnmockedHandlerPolicy,
    pub cases: Vec<ContractCase>,
}

/// Behavior for handlers that a case does not mock. Real handlers are
/// never invoked either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnmockedHandlerPolicy {
    /// Fail the case when an unmocked handler is called (default).
    #[default]
    Fail,
    /// Unmocked handlers return `{}` successfully.
    EmptySuccess,
}

/// One scenario: fixture in, expectations out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ContractCase {
    /// Unique case name within the suite.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Initial `context.data`.
    #[serde(default)]
    pub input: serde_json::Value,
    /// Outputs already durable at the selected replay boundary. The offline
    /// runner seeds these into its sandbox instance, making the fixture a
    /// true continuation from that boundary rather than a replay from block
    /// one. Real handlers are still never invoked.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub initial_outputs: BTreeMap<String, serde_json::Value>,
    /// Initial `context.config`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
    /// Handler / block mocks for this case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mocks: Vec<MockDef>,
    /// Signals delivered while the instance is waiting, in order. Each
    /// fixture is sent the next time the instance enters a waiting state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signals: Vec<SignalFixture>,
    /// Expected outcomes.
    pub expect: Expectations,
    /// Cap on total virtual time the runner may advance for this case
    /// (logical duration). Exceeding it fails the case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_logical_duration_ms: Option<u64>,
    /// Cap on scheduler ticks (default: runner's choice, currently 5000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ticks: Option<u32>,
}

/// A mock target: a handler name (all steps using it) or a single block.
/// Block mocks take precedence over handler mocks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MockDef {
    /// Handler name this mock applies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    /// Block id this mock applies to (overrides handler-level mocks).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block: Option<String>,
    #[serde(flatten)]
    pub policy: MockPolicy,
}

/// What a mocked handler does when called.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MockPolicy {
    /// Always return this output.
    Success { output: serde_json::Value },
    /// Always fail with this message.
    Failure {
        message: String,
        /// Whether the failure is retryable (drives retry policies).
        #[serde(default)]
        retryable: bool,
    },
    /// Return the recorded output for the executing block, supplied to the
    /// runner out-of-band (e.g. from `orch8 test record`). Missing
    /// recordings follow the suite's unmocked-handler policy.
    Recorded,
    /// A sequence of per-attempt outcomes; the last entry repeats once
    /// exhausted. Use to script "fail, fail, then succeed" retry tests.
    Attempts { attempts: Vec<MockOutcome> },
}

/// One scripted attempt outcome for [`MockPolicy::Attempts`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MockOutcome {
    Success {
        output: serde_json::Value,
    },
    Failure {
        message: String,
        #[serde(default)]
        retryable: bool,
    },
}

/// A signal delivered while the case's instance is waiting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SignalFixture {
    /// Signal type: `custom`, `resume`, `cancel`, ... (matches the engine's
    /// signal-type names).
    pub signal_type: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// Everything a case may assert.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, ToSchema)]
pub struct Expectations {
    /// Expected terminal state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_state: Option<ExpectedTerminalState>,
    /// Blocks that must have executed. With `ordered: true` they must have
    /// executed in this relative order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathExpectation>,
    /// Blocks that must NOT have executed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_blocks: Vec<String>,
    /// Value assertions over outputs and final context.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertions: Vec<Assertion>,
    /// Handler call-count invariants ("charge is called at most once").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub call_counts: Vec<CallCountExpectation>,
}

/// Terminal states a contract can expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedTerminalState {
    Completed,
    Failed,
    Cancelled,
}

impl ExpectedTerminalState {
    /// The instance-state string this expectation matches (the engine's
    /// serialized `InstanceState` names).
    #[must_use]
    pub const fn as_state_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Path expectation: blocks that must have run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PathExpectation {
    pub traversed: Vec<String>,
    /// When true, `traversed` must appear in this relative order in the
    /// execution history (other blocks may interleave).
    #[serde(default)]
    pub ordered: bool,
}

/// Handler call-count invariant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CallCountExpectation {
    pub handler: String,
    /// Minimum required calls (inclusive). Default 0.
    #[serde(default)]
    pub min: u32,
    /// Maximum allowed calls (inclusive). Absent = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u32>,
}

/// A single value assertion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Assertion {
    /// Where to read the actual value.
    #[serde(flatten)]
    pub subject: AssertionSubject,
    /// The check to apply.
    #[serde(flatten)]
    pub op: AssertionOp,
}

/// The value an assertion reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AssertionSubject {
    /// A recorded block output; `path` walks into it (dotted, empty = whole
    /// output).
    Output { block: String, path: String },
    /// The final `context.data`; `path` walks into it.
    Context { path: String },
}

/// Assertion operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum AssertionOp {
    /// Deep equality.
    Equals { value: serde_json::Value },
    /// The path resolves to a non-null value.
    Exists,
    /// The path is absent or null.
    NotExists,
    /// Strings: substring. Arrays: contains the element. Objects: contains
    /// the given subset of key/value pairs.
    Contains { value: serde_json::Value },
    /// Numeric range check, inclusive on both ends.
    InRange {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
    },
    /// JSON type check: `"string" | "number" | "boolean" | "array" |
    /// "object" | "null"`.
    HasType { expected: String },
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

impl ContractSuite {
    /// Structural validation independent of any sequence definition.
    ///
    /// # Errors
    /// Returns the first problem found as a human-readable message.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != CONTRACT_SCHEMA_VERSION {
            return Err(format!(
                "unsupported contract schema_version {} (this engine supports {})",
                self.schema_version, CONTRACT_SCHEMA_VERSION
            ));
        }
        if self.cases.is_empty() {
            return Err("contract suite has no cases".to_string());
        }
        let mut seen = std::collections::HashSet::new();
        for case in &self.cases {
            if case.name.trim().is_empty() {
                return Err("case with empty name".to_string());
            }
            if !seen.insert(case.name.as_str()) {
                return Err(format!("duplicate case name: {}", case.name));
            }
            case.validate()
                .map_err(|e| format!("case '{}': {e}", case.name))?;
        }
        Ok(())
    }
}

impl ContractCase {
    /// Per-case structural validation.
    ///
    /// # Errors
    /// Returns the first problem found as a human-readable message.
    pub fn validate(&self) -> Result<(), String> {
        for mock in &self.mocks {
            match (&mock.handler, &mock.block) {
                (None, None) => {
                    return Err("mock must set either 'handler' or 'block'".to_string());
                }
                (Some(_), Some(_)) => {
                    return Err(
                        "mock must set exactly one of 'handler' or 'block', not both".to_string(),
                    );
                }
                _ => {}
            }
            if let MockPolicy::Attempts { attempts } = &mock.policy
                && attempts.is_empty()
            {
                return Err("attempts mock with empty attempts list".to_string());
            }
        }
        for cc in &self.expect.call_counts {
            if let Some(max) = cc.max
                && cc.min > max
            {
                return Err(format!(
                    "call_count for '{}': min {} > max {max}",
                    cc.handler, cc.min
                ));
            }
        }
        if let Some(path) = &self.expect.path
            && path.traversed.is_empty()
        {
            return Err("path expectation with empty traversed list".to_string());
        }
        for a in &self.expect.assertions {
            if let AssertionOp::InRange {
                min: None,
                max: None,
            } = a.op
            {
                return Err("in_range assertion with neither min nor max".to_string());
            }
            if let AssertionOp::HasType { expected } = &a.op {
                const TYPES: &[&str] = &["string", "number", "boolean", "array", "object", "null"];
                if !TYPES.contains(&expected.as_str()) {
                    return Err(format!("has_type assertion with unknown type '{expected}'"));
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Pure evaluation
// ---------------------------------------------------------------------------

/// Walk a dotted path (`a.b.0.c`) into a JSON value. Empty path returns the
/// root. Array segments may be numeric indices.
#[must_use]
pub fn resolve_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    if path.is_empty() {
        return Some(root);
    }
    let mut current = root;
    for segment in path.split('.') {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(segment)?;
            }
            serde_json::Value::Array(items) => {
                let idx: usize = segment.parse().ok()?;
                current = items.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

/// Apply one assertion operator to a resolved value (`None` = path absent).
/// Returns `Ok(())` or a human-readable divergence description.
///
/// # Errors
/// Describes the first divergence, including expected vs. actual.
pub fn check_op(op: &AssertionOp, actual: Option<&serde_json::Value>) -> Result<(), String> {
    use serde_json::Value;
    match op {
        AssertionOp::Equals { value } => match actual {
            Some(a) if a == value => Ok(()),
            Some(a) => Err(format!("expected {value}, got {a}")),
            None => Err(format!("expected {value}, but path is absent")),
        },
        AssertionOp::Exists => match actual {
            Some(Value::Null) | None => {
                Err("expected value to exist, but it is absent/null".into())
            }
            Some(_) => Ok(()),
        },
        AssertionOp::NotExists => match actual {
            Some(Value::Null) | None => Ok(()),
            Some(a) => Err(format!("expected absence, but found {a}")),
        },
        AssertionOp::Contains { value } => {
            let Some(a) = actual else {
                return Err("expected container, but path is absent".into());
            };
            let ok = match (a, value) {
                (Value::String(s), Value::String(needle)) => s.contains(needle.as_str()),
                (Value::Array(items), needle) => items.contains(needle),
                (Value::Object(map), Value::Object(subset)) => subset
                    .iter()
                    .all(|(k, v)| map.get(k).is_some_and(|mv| mv == v)),
                _ => false,
            };
            if ok {
                Ok(())
            } else {
                Err(format!("{a} does not contain {value}"))
            }
        }
        AssertionOp::InRange { min, max } => {
            let Some(n) = actual.and_then(serde_json::Value::as_f64) else {
                return Err(format!(
                    "expected a number, got {}",
                    actual.map_or_else(|| "absent".to_string(), ToString::to_string)
                ));
            };
            if let Some(min) = min
                && n < *min
            {
                return Err(format!("{n} is below minimum {min}"));
            }
            if let Some(max) = max
                && n > *max
            {
                return Err(format!("{n} is above maximum {max}"));
            }
            Ok(())
        }
        AssertionOp::HasType { expected } => {
            let Some(a) = actual else {
                return Err(format!("expected type '{expected}', but path is absent"));
            };
            let actual_type = match a {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            if actual_type == expected {
                Ok(())
            } else {
                Err(format!("expected type '{expected}', got '{actual_type}'"))
            }
        }
    }
}

/// Evaluate one assertion against the case's recorded outputs and final
/// context data.
///
/// # Errors
/// Describes the divergence, prefixed with the subject.
pub fn evaluate_assertion(
    assertion: &Assertion,
    outputs: &BTreeMap<String, serde_json::Value>,
    context_data: &serde_json::Value,
) -> Result<(), String> {
    match &assertion.subject {
        AssertionSubject::Output { block, path } => {
            let Some(output) = outputs.get(block) else {
                // NotExists on a block that never ran is satisfied.
                if matches!(assertion.op, AssertionOp::NotExists) {
                    return Ok(());
                }
                return Err(format!("output.{block}: block produced no output"));
            };
            check_op(&assertion.op, resolve_path(output, path))
                .map_err(|e| format!("output.{block}.{path}: {e}"))
        }
        AssertionSubject::Context { path } => {
            check_op(&assertion.op, resolve_path(context_data, path))
                .map_err(|e| format!("context.{path}: {e}"))
        }
    }
}

/// Check a path expectation against the ordered execution history.
///
/// # Errors
/// Describes the first missing or out-of-order block.
pub fn check_path(expect: &PathExpectation, executed_in_order: &[String]) -> Result<(), String> {
    if expect.ordered {
        let mut cursor = 0usize;
        for want in &expect.traversed {
            let found = executed_in_order[cursor..].iter().position(|b| b == want);
            match found {
                Some(offset) => cursor += offset + 1,
                None => {
                    return if executed_in_order.contains(want) {
                        Err(format!("block '{want}' executed out of expected order"))
                    } else {
                        Err(format!(
                            "expected block '{want}' to execute, but it did not"
                        ))
                    };
                }
            }
        }
        Ok(())
    } else {
        for want in &expect.traversed {
            if !executed_in_order.contains(want) {
                return Err(format!(
                    "expected block '{want}' to execute, but it did not"
                ));
            }
        }
        Ok(())
    }
}

/// Check call-count invariants against observed handler call counts.
///
/// # Errors
/// Describes the first violated invariant.
pub fn check_call_counts(
    expectations: &[CallCountExpectation],
    calls: &BTreeMap<String, u32>,
) -> Result<(), String> {
    for e in expectations {
        let actual = calls.get(&e.handler).copied().unwrap_or(0);
        if actual < e.min {
            return Err(format!(
                "handler '{}' called {actual} times, expected at least {}",
                e.handler, e.min
            ));
        }
        if let Some(max) = e.max
            && actual > max
        {
            return Err(format!(
                "handler '{}' called {actual} times, expected at most {max}",
                e.handler
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Machine-readable result of running one contract case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CaseReport {
    pub name: String,
    pub passed: bool,
    /// Every divergence found (empty when passed). The first entry is the
    /// first divergence in evaluation order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
    /// Terminal state the instance reached (engine state name), or the
    /// non-terminal state it was stuck in when the runner gave up.
    pub final_state: String,
    /// Blocks that executed, in first-execution order.
    #[serde(default)]
    pub executed_blocks: Vec<String>,
    /// Observed handler call counts.
    #[serde(default)]
    pub handler_calls: BTreeMap<String, u32>,
    /// Scheduler ticks consumed.
    pub ticks: u32,
    /// Total virtual time advanced, in milliseconds.
    pub logical_duration_ms: u64,
}

/// Machine-readable result of running a whole suite.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SuiteReport {
    /// Sequence the suite ran against.
    pub sequence_name: String,
    pub sequence_version: i64,
    pub passed: bool,
    pub cases: Vec<CaseReport>,
}

impl SuiteReport {
    #[must_use]
    pub fn failed_cases(&self) -> Vec<&CaseReport> {
        self.cases.iter().filter(|c| !c.passed).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn suite_json() -> serde_json::Value {
        json!({
            "sequence_name": "checkout",
            "sequence_version": 2,
            "cases": [{
                "name": "happy path",
                "input": {"amount": 100},
                "mocks": [
                    {"handler": "charge_card", "type": "success", "output": {"charged": true}},
                    {"block": "notify", "type": "failure", "message": "smtp down", "retryable": true},
                    {"handler": "flaky", "type": "attempts", "attempts": [
                        {"kind": "failure", "message": "boom", "retryable": true},
                        {"kind": "success", "output": {"ok": 1}}
                    ]}
                ],
                "expect": {
                    "terminal_state": "completed",
                    "path": {"traversed": ["validate", "charge"], "ordered": true},
                    "skipped_blocks": ["refund"],
                    "assertions": [
                        {"output": {"block": "charge", "path": "charged"}, "op": "equals", "value": true},
                        {"context": {"path": "order.status"}, "op": "exists"}
                    ],
                    "call_counts": [{"handler": "charge_card", "min": 1, "max": 1}]
                }
            }]
        })
    }

    // --- serde round-trips ---

    #[test]
    fn suite_deserializes_from_documented_shape() {
        let suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        assert_eq!(suite.schema_version, CONTRACT_SCHEMA_VERSION);
        assert_eq!(suite.sequence_name.as_deref(), Some("checkout"));
        assert_eq!(suite.cases.len(), 1);
        let case = &suite.cases[0];
        assert_eq!(case.mocks.len(), 3);
        assert!(matches!(case.mocks[0].policy, MockPolicy::Success { .. }));
        assert!(matches!(case.mocks[1].policy, MockPolicy::Failure { .. }));
        assert!(matches!(case.mocks[2].policy, MockPolicy::Attempts { .. }));
        assert_eq!(
            case.expect.terminal_state,
            Some(ExpectedTerminalState::Completed)
        );
        assert_eq!(case.expect.call_counts[0].max, Some(1));
    }

    #[test]
    fn suite_round_trips() {
        let suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        let re: ContractSuite =
            serde_json::from_str(&serde_json::to_string(&suite).unwrap()).unwrap();
        assert_eq!(re, suite);
    }

    #[test]
    fn unmocked_policy_defaults_to_fail() {
        let suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        assert_eq!(suite.unmocked_handlers, UnmockedHandlerPolicy::Fail);
    }

    // --- validation ---

    #[test]
    fn validate_accepts_well_formed_suite() {
        let suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.validate().unwrap();
    }

    #[test]
    fn validate_rejects_wrong_schema_version() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.schema_version = 999;
        let err = suite.validate().unwrap_err();
        assert!(err.contains("schema_version 999"), "{err}");
    }

    #[test]
    fn validate_rejects_empty_suite() {
        let suite = ContractSuite {
            schema_version: CONTRACT_SCHEMA_VERSION,
            sequence_name: None,
            sequence_version: None,
            unmocked_handlers: UnmockedHandlerPolicy::default(),
            cases: vec![],
        };
        assert!(suite.validate().unwrap_err().contains("no cases"));
    }

    #[test]
    fn validate_rejects_duplicate_case_names() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases.push(suite.cases[0].clone());
        assert!(
            suite
                .validate()
                .unwrap_err()
                .contains("duplicate case name")
        );
    }

    #[test]
    fn validate_rejects_mock_without_target() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0].mocks.push(MockDef {
            handler: None,
            block: None,
            policy: MockPolicy::Recorded,
        });
        assert!(
            suite
                .validate()
                .unwrap_err()
                .contains("either 'handler' or 'block'")
        );
    }

    #[test]
    fn validate_rejects_mock_with_both_targets() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0].mocks.push(MockDef {
            handler: Some("h".into()),
            block: Some("b".into()),
            policy: MockPolicy::Recorded,
        });
        assert!(suite.validate().unwrap_err().contains("not both"));
    }

    #[test]
    fn validate_rejects_empty_attempts() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0].mocks.push(MockDef {
            handler: Some("h".into()),
            block: None,
            policy: MockPolicy::Attempts { attempts: vec![] },
        });
        assert!(suite.validate().unwrap_err().contains("empty attempts"));
    }

    #[test]
    fn validate_rejects_inverted_call_count_bounds() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0]
            .expect
            .call_counts
            .push(CallCountExpectation {
                handler: "h".into(),
                min: 3,
                max: Some(1),
            });
        assert!(suite.validate().unwrap_err().contains("min 3 > max 1"));
    }

    #[test]
    fn validate_rejects_unbounded_in_range() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0].expect.assertions.push(Assertion {
            subject: AssertionSubject::Context { path: "x".into() },
            op: AssertionOp::InRange {
                min: None,
                max: None,
            },
        });
        assert!(
            suite
                .validate()
                .unwrap_err()
                .contains("neither min nor max")
        );
    }

    #[test]
    fn validate_rejects_unknown_has_type() {
        let mut suite: ContractSuite = serde_json::from_value(suite_json()).unwrap();
        suite.cases[0].expect.assertions.push(Assertion {
            subject: AssertionSubject::Context { path: "x".into() },
            op: AssertionOp::HasType {
                expected: "integer".into(),
            },
        });
        assert!(
            suite
                .validate()
                .unwrap_err()
                .contains("unknown type 'integer'")
        );
    }

    // --- resolve_path ---

    #[test]
    fn resolve_path_walks_objects_and_arrays() {
        let v = json!({"a": {"b": [{"c": 42}]}});
        assert_eq!(resolve_path(&v, "a.b.0.c"), Some(&json!(42)));
    }

    #[test]
    fn resolve_path_empty_returns_root() {
        let v = json!({"a": 1});
        assert_eq!(resolve_path(&v, ""), Some(&v));
    }

    #[test]
    fn resolve_path_missing_segment_is_none() {
        let v = json!({"a": 1});
        assert_eq!(resolve_path(&v, "a.b"), None);
        assert_eq!(resolve_path(&v, "z"), None);
    }

    #[test]
    fn resolve_path_bad_array_index_is_none() {
        let v = json!([1, 2, 3]);
        assert_eq!(resolve_path(&v, "5"), None);
        assert_eq!(resolve_path(&v, "not_a_number"), None);
    }

    // --- check_op ---

    #[test]
    fn equals_matches_deeply() {
        check_op(
            &AssertionOp::Equals {
                value: json!({"a": [1, 2]}),
            },
            Some(&json!({"a": [1, 2]})),
        )
        .unwrap();
        assert!(
            check_op(&AssertionOp::Equals { value: json!(1) }, Some(&json!(2)))
                .unwrap_err()
                .contains("expected 1, got 2")
        );
    }

    #[test]
    fn equals_on_absent_path_fails() {
        assert!(
            check_op(&AssertionOp::Equals { value: json!(1) }, None)
                .unwrap_err()
                .contains("absent")
        );
    }

    #[test]
    fn exists_and_not_exists() {
        check_op(&AssertionOp::Exists, Some(&json!(0))).unwrap();
        assert!(check_op(&AssertionOp::Exists, Some(&json!(null))).is_err());
        assert!(check_op(&AssertionOp::Exists, None).is_err());
        check_op(&AssertionOp::NotExists, None).unwrap();
        check_op(&AssertionOp::NotExists, Some(&json!(null))).unwrap();
        assert!(check_op(&AssertionOp::NotExists, Some(&json!(false))).is_err());
    }

    #[test]
    fn contains_on_strings_arrays_objects() {
        let op = |v: serde_json::Value| AssertionOp::Contains { value: v };
        check_op(&op(json!("ell")), Some(&json!("hello"))).unwrap();
        check_op(&op(json!(2)), Some(&json!([1, 2, 3]))).unwrap();
        check_op(&op(json!({"a": 1})), Some(&json!({"a": 1, "b": 2}))).unwrap();
        assert!(check_op(&op(json!("xyz")), Some(&json!("hello"))).is_err());
        assert!(check_op(&op(json!(9)), Some(&json!([1, 2]))).is_err());
        assert!(check_op(&op(json!({"a": 2})), Some(&json!({"a": 1}))).is_err());
        // Type mismatch: number in string.
        assert!(check_op(&op(json!(1)), Some(&json!("1"))).is_err());
    }

    #[test]
    fn in_range_checks_bounds_inclusively() {
        let op = AssertionOp::InRange {
            min: Some(1.0),
            max: Some(3.0),
        };
        check_op(&op, Some(&json!(1))).unwrap();
        check_op(&op, Some(&json!(3.0))).unwrap();
        assert!(
            check_op(&op, Some(&json!(0.5)))
                .unwrap_err()
                .contains("below")
        );
        assert!(
            check_op(&op, Some(&json!(3.1)))
                .unwrap_err()
                .contains("above")
        );
        assert!(
            check_op(&op, Some(&json!("nan")))
                .unwrap_err()
                .contains("expected a number")
        );
        assert!(check_op(&op, None).is_err());
    }

    #[test]
    fn in_range_half_open() {
        check_op(
            &AssertionOp::InRange {
                min: Some(5.0),
                max: None,
            },
            Some(&json!(1e9)),
        )
        .unwrap();
        check_op(
            &AssertionOp::InRange {
                min: None,
                max: Some(5.0),
            },
            Some(&json!(-1e9)),
        )
        .unwrap();
    }

    #[test]
    fn has_type_matches_json_types() {
        for (ty, val) in [
            ("string", json!("s")),
            ("number", json!(4.2)),
            ("boolean", json!(true)),
            ("array", json!([])),
            ("object", json!({})),
            ("null", json!(null)),
        ] {
            check_op(
                &AssertionOp::HasType {
                    expected: ty.into(),
                },
                Some(&val),
            )
            .unwrap();
        }
        assert!(
            check_op(
                &AssertionOp::HasType {
                    expected: "string".into()
                },
                Some(&json!(1))
            )
            .unwrap_err()
            .contains("got 'number'")
        );
    }

    // --- evaluate_assertion ---

    #[test]
    fn assertion_reads_block_outputs() {
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "charge".to_string(),
            json!({"charged": true, "amount": 100}),
        );
        let a = Assertion {
            subject: AssertionSubject::Output {
                block: "charge".into(),
                path: "charged".into(),
            },
            op: AssertionOp::Equals { value: json!(true) },
        };
        evaluate_assertion(&a, &outputs, &json!({})).unwrap();
    }

    #[test]
    fn assertion_on_missing_block_fails_with_context() {
        let a = Assertion {
            subject: AssertionSubject::Output {
                block: "nope".into(),
                path: String::new(),
            },
            op: AssertionOp::Exists,
        };
        let err = evaluate_assertion(&a, &BTreeMap::new(), &json!({})).unwrap_err();
        assert!(err.contains("output.nope"), "{err}");
        assert!(err.contains("no output"), "{err}");
    }

    #[test]
    fn not_exists_on_missing_block_passes() {
        let a = Assertion {
            subject: AssertionSubject::Output {
                block: "refund".into(),
                path: "any".into(),
            },
            op: AssertionOp::NotExists,
        };
        evaluate_assertion(&a, &BTreeMap::new(), &json!({})).unwrap();
    }

    #[test]
    fn assertion_reads_context_data() {
        let a = Assertion {
            subject: AssertionSubject::Context {
                path: "order.status".into(),
            },
            op: AssertionOp::Equals {
                value: json!("paid"),
            },
        };
        evaluate_assertion(&a, &BTreeMap::new(), &json!({"order": {"status": "paid"}})).unwrap();
        let err = evaluate_assertion(&a, &BTreeMap::new(), &json!({})).unwrap_err();
        assert!(err.starts_with("context.order.status:"), "{err}");
    }

    // --- check_path ---

    fn history(items: &[&str]) -> Vec<String> {
        items.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn unordered_path_only_requires_membership() {
        let e = PathExpectation {
            traversed: vec!["b".into(), "a".into()],
            ordered: false,
        };
        check_path(&e, &history(&["a", "x", "b"])).unwrap();
        assert!(
            check_path(&e, &history(&["a"]))
                .unwrap_err()
                .contains("'b'")
        );
    }

    #[test]
    fn ordered_path_requires_relative_order() {
        let e = PathExpectation {
            traversed: vec!["a".into(), "c".into()],
            ordered: true,
        };
        check_path(&e, &history(&["a", "b", "c"])).unwrap();
        let err = check_path(&e, &history(&["c", "b", "a"])).unwrap_err();
        assert!(err.contains("out of expected order"), "{err}");
    }

    #[test]
    fn ordered_path_reports_missing_block() {
        let e = PathExpectation {
            traversed: vec!["a".into(), "z".into()],
            ordered: true,
        };
        let err = check_path(&e, &history(&["a", "b"])).unwrap_err();
        assert!(err.contains("'z'"), "{err}");
        assert!(err.contains("did not"), "{err}");
    }

    #[test]
    fn ordered_path_allows_duplicate_visits() {
        // Loop bodies execute repeatedly; expecting [a, a] must match a
        // history where 'a' ran twice.
        let e = PathExpectation {
            traversed: vec!["a".into(), "a".into()],
            ordered: true,
        };
        check_path(&e, &history(&["a", "b", "a"])).unwrap();
        assert!(check_path(&e, &history(&["a", "b"])).is_err());
    }

    // --- check_call_counts ---

    #[test]
    fn call_counts_enforce_min_and_max() {
        let mut calls = BTreeMap::new();
        calls.insert("charge".to_string(), 1u32);
        check_call_counts(
            &[CallCountExpectation {
                handler: "charge".into(),
                min: 1,
                max: Some(1),
            }],
            &calls,
        )
        .unwrap();

        let err = check_call_counts(
            &[CallCountExpectation {
                handler: "charge".into(),
                min: 2,
                max: None,
            }],
            &calls,
        )
        .unwrap_err();
        assert!(err.contains("at least 2"), "{err}");

        calls.insert("charge".to_string(), 5);
        let err = check_call_counts(
            &[CallCountExpectation {
                handler: "charge".into(),
                min: 0,
                max: Some(1),
            }],
            &calls,
        )
        .unwrap_err();
        assert!(err.contains("at most 1"), "{err}");
    }

    #[test]
    fn call_counts_treat_uncalled_as_zero() {
        // "at most once" on a handler that never ran passes; "at least
        // once" fails.
        check_call_counts(
            &[CallCountExpectation {
                handler: "ghost".into(),
                min: 0,
                max: Some(1),
            }],
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(
            check_call_counts(
                &[CallCountExpectation {
                    handler: "ghost".into(),
                    min: 1,
                    max: None
                }],
                &BTreeMap::new(),
            )
            .is_err()
        );
    }

    // --- reports ---

    #[test]
    fn suite_report_lists_failed_cases() {
        let report = SuiteReport {
            sequence_name: "s".into(),
            sequence_version: 1,
            passed: false,
            cases: vec![
                CaseReport {
                    name: "ok".into(),
                    passed: true,
                    failures: vec![],
                    final_state: "completed".into(),
                    executed_blocks: vec![],
                    handler_calls: BTreeMap::new(),
                    ticks: 3,
                    logical_duration_ms: 0,
                },
                CaseReport {
                    name: "bad".into(),
                    passed: false,
                    failures: vec!["divergence".into()],
                    final_state: "failed".into(),
                    executed_blocks: vec![],
                    handler_calls: BTreeMap::new(),
                    ticks: 3,
                    logical_duration_ms: 500,
                },
            ],
        };
        let failed = report.failed_cases();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].name, "bad");
    }

    #[test]
    fn expected_terminal_state_strings_match_instance_states() {
        assert_eq!(ExpectedTerminalState::Completed.as_state_str(), "completed");
        assert_eq!(ExpectedTerminalState::Failed.as_state_str(), "failed");
        assert_eq!(ExpectedTerminalState::Cancelled.as_state_str(), "cancelled");
    }
}
