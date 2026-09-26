//! Static preflight: the subset of the server's `POST /sequences/preflight`
//! checks that need no runtime inventory. Checks that DO need the server
//! (workers, credentials, plugins, providers) are reported as `unknown`,
//! never as `pass` — the playground cannot prove them.

use chrono::{DateTime, Utc};
use orch8_types::context::ExecutionContext;
use orch8_types::finding::{Confidence, Finding, FindingSeverity, ResourceRef};
use orch8_types::preflight::{PreflightCheck, PreflightReport, PreflightStatus};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};
use serde_json::{Value, json};

use crate::decode::decode;
use crate::expression;
use crate::interp::LOCAL_HANDLERS;

fn observed_at() -> DateTime<Utc> {
    // Virtual time: the playground has no trustworthy wall clock (and
    // `Utc::now()` is unavailable on wasm32-unknown-unknown without JS glue).
    DateTime::<Utc>::UNIX_EPOCH
}

fn finding(code: &str, severity: FindingSeverity, summary: String) -> Finding {
    Finding::new(code, severity, summary, Confidence::Certain, observed_at())
}

/// Every expression in the tree as `(block_id, field, expression)`.
fn collect_expressions(block: &BlockDefinition, out: &mut Vec<(String, &'static str, String)>) {
    let mut children: Vec<&BlockDefinition> = Vec::new();
    match block {
        BlockDefinition::Step(step) => {
            if let Some(when) = &step.when {
                out.push((step.id.as_str().into(), "when", when.clone()));
            }
            if step.handler == "assert"
                && let Some(condition) = step.params.get("condition").and_then(Value::as_str)
            {
                out.push((
                    step.id.as_str().into(),
                    "params.condition",
                    condition.into(),
                ));
            }
        }
        BlockDefinition::Router(def) => {
            for route in &def.routes {
                out.push((
                    def.id.as_str().into(),
                    "routes[].condition",
                    route.condition.clone(),
                ));
                children.extend(&route.blocks);
            }
            children.extend(def.default.iter().flatten());
        }
        BlockDefinition::Loop(def) => {
            out.push((def.id.as_str().into(), "condition", def.condition.clone()));
            if let Some(break_on) = &def.break_on {
                out.push((def.id.as_str().into(), "break_on", break_on.clone()));
            }
            children.extend(&def.body);
        }
        BlockDefinition::Parallel(def) => children.extend(def.branches.iter().flatten()),
        BlockDefinition::Race(def) => children.extend(def.branches.iter().flatten()),
        BlockDefinition::ForEach(def) => children.extend(&def.body),
        BlockDefinition::TryCatch(def) => {
            children.extend(&def.try_block);
            children.extend(&def.catch_block);
            children.extend(def.finally_block.iter().flatten());
        }
        BlockDefinition::ABSplit(def) => {
            children.extend(def.variants.iter().flat_map(|v| &v.blocks));
        }
        BlockDefinition::CancellationScope(def) => children.extend(&def.blocks),
        BlockDefinition::Saga(def) => {
            for step in &def.steps {
                children.push(&step.action);
                if let Some(compensation) = &step.compensation {
                    children.push(compensation);
                }
            }
        }
        BlockDefinition::SubSequence(_) => {}
    }
    for child in children {
        collect_expressions(child, out);
    }
}

fn all_blocks(sequence: &SequenceDefinition) -> impl Iterator<Item = &BlockDefinition> {
    sequence
        .blocks
        .iter()
        .chain(sequence.on_failure.iter().flatten())
        .chain(sequence.on_cancel.iter().flatten())
}

fn expressions_check(sequence: &SequenceDefinition) -> PreflightCheck {
    let mut expressions = Vec::new();
    for block in all_blocks(sequence) {
        collect_expressions(block, &mut expressions);
    }
    let empty = ExecutionContext::default();
    let findings: Vec<Finding> = expressions
        .iter()
        .filter_map(|(block_id, field, expr)| {
            let inner = expr.trim();
            let inner = inner
                .strip_prefix("{{")
                .and_then(|s| s.strip_suffix("}}"))
                .unwrap_or(inner);
            expression::try_evaluate(inner, &empty, &json!({}))
                .err()
                .map(|error| {
                    finding(
                        "EXPRESSION_PARSE_ERROR",
                        FindingSeverity::Error,
                        format!("`{field}` does not parse: {error}"),
                    )
                    .with_resource(ResourceRef::new("block", block_id.clone()))
                })
        })
        .collect();
    if findings.is_empty() {
        PreflightCheck::pass(
            "expressions_parse",
            format!("{} expression(s) parse", expressions.len()),
        )
    } else {
        PreflightCheck::with_status(
            "expressions_parse",
            PreflightStatus::Fail,
            format!("{} expression(s) do not parse", findings.len()),
            findings,
        )
    }
}

fn handlers_known_check(sequence: &SequenceDefinition) -> PreflightCheck {
    let warnings = sequence.unknown_handler_warnings();
    if warnings.is_empty() {
        return PreflightCheck::pass("handlers_known", "all handlers are engine built-ins");
    }
    let findings = warnings
        .into_iter()
        .map(|w| finding("UNKNOWN_HANDLER", FindingSeverity::Warning, w))
        .collect::<Vec<_>>();
    PreflightCheck::with_status(
        "handlers_known",
        PreflightStatus::Warning,
        format!("{} handler(s) are not built-ins", findings.len()),
        findings,
    )
}

fn input_schema_check(sequence: &SequenceDefinition) -> PreflightCheck {
    match &sequence.input_schema {
        None => PreflightCheck::pass("input_schema_valid", "no input schema declared"),
        Some(Value::Object(_)) => {
            PreflightCheck::pass("input_schema_valid", "input schema is a JSON object")
        }
        Some(_) => PreflightCheck::with_status(
            "input_schema_valid",
            PreflightStatus::Fail,
            "input_schema must be a JSON object",
            vec![],
        ),
    }
}

fn runtime_check(sequence: &SequenceDefinition) -> PreflightCheck {
    let external: Vec<String> = sequence
        .handler_names()
        .into_iter()
        .filter(|h| !LOCAL_HANDLERS.contains(&h.as_str()))
        .collect();
    if external.is_empty() {
        return PreflightCheck::pass(
            "runtime_inventory",
            "only local built-ins; nothing to verify on a server",
        );
    }
    let findings = external
        .iter()
        .map(|handler| {
            finding(
                "RUNTIME_NOT_VERIFIABLE",
                FindingSeverity::Info,
                format!(
                    "`{handler}` needs a worker, credential, plugin or provider that only a running server can verify"
                ),
            )
            .with_resource(ResourceRef::new("handler", handler.clone()))
        })
        .collect();
    PreflightCheck::with_status(
        "runtime_inventory",
        PreflightStatus::Unknown,
        format!(
            "{} handler(s) need runtime inventory; run `orch8 preflight` against a server",
            external.len()
        ),
        findings,
    )
}

/// Build a preflight report for sequence JSON text.
pub fn preflight(input: &str) -> PreflightReport {
    let name_hint = serde_json::from_str::<Value>(input)
        .ok()
        .and_then(|v| v.get("name").and_then(Value::as_str).map(str::to_string))
        .unwrap_or_else(|| "playground".into());
    match decode(input) {
        Err(error) => PreflightReport::new(
            name_hint,
            0,
            vec![PreflightCheck::with_status(
                "definition_valid",
                PreflightStatus::Fail,
                "definition does not decode or validate",
                vec![finding("INVALID_DEFINITION", FindingSeverity::Error, error)],
            )],
            observed_at(),
        ),
        Ok(decoded) => {
            let sequence = &decoded.sequence;
            let checks = vec![
                PreflightCheck::pass("definition_valid", "definition validates"),
                expressions_check(sequence),
                handlers_known_check(sequence),
                input_schema_check(sequence),
                runtime_check(sequence),
            ];
            PreflightReport::new(
                sequence.name.clone(),
                i64::from(sequence.version),
                checks,
                observed_at(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(report: &PreflightReport, id: &str) -> PreflightStatus {
        report.checks.iter().find(|c| c.id == id).unwrap().status
    }

    #[test]
    fn local_only_sequence_is_ready() {
        let report = preflight(r#"{"blocks":[{"type":"step","id":"a","handler":"log"}]}"#);
        assert_eq!(report.overall, PreflightStatus::Pass);
        assert!(report.is_ready());
    }

    #[test]
    fn external_handlers_are_unknown_not_pass() {
        let report = preflight(r#"{"blocks":[{"type":"step","id":"a","handler":"llm_call"}]}"#);
        assert_eq!(
            status_of(&report, "runtime_inventory"),
            PreflightStatus::Unknown
        );
        assert!(!report.is_ready());
    }

    #[test]
    fn broken_expression_fails() {
        let report = preflight(
            r#"{"blocks":[{"type":"router","id":"r","routes":[{"condition":"data.x ==","blocks":[{"type":"step","id":"a","handler":"noop"}]}]}]}"#,
        );
        assert_eq!(
            status_of(&report, "expressions_parse"),
            PreflightStatus::Fail
        );
    }

    #[test]
    fn typo_handler_warns_with_suggestion() {
        let report = preflight(r#"{"blocks":[{"type":"step","id":"a","handler":"sleap"}]}"#);
        let check = report
            .checks
            .iter()
            .find(|c| c.id == "handlers_known")
            .unwrap();
        assert_eq!(check.status, PreflightStatus::Warning);
        assert!(check.findings[0].summary.contains("sleep"));
    }

    #[test]
    fn invalid_definition_fails_fast() {
        let report = preflight(r#"{"name":"x","blocks":[]}"#);
        assert_eq!(report.overall, PreflightStatus::Fail);
        assert_eq!(report.sequence_name, "x");
    }
}
