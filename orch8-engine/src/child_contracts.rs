//! Validate the input and output boundaries of explicitly pinned sub-sequences.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use orch8_types::finding::{Confidence, Evidence, Finding, FindingSeverity, ResourceRef};
use orch8_types::sequence::SequenceDefinition;
use serde_json::{Value, json};

use crate::dataflow_analysis::{
    DataflowSeverity, RenderBudget, block_children, compile_with_output_schemas, expand_local_refs,
    schema_path,
};
use crate::template::{contains_template, is_template_path};

type Revision = (String, i32);

struct Call {
    block_id: String,
    pointer: String,
    revision: Revision,
    input: Value,
    reference: bool,
}

/// Return the exact revisions whose contracts must be loaded for preflight.
#[must_use]
pub fn pinned_child_revisions(parent: &SequenceDefinition) -> BTreeSet<(String, i32)> {
    calls(parent)
        .into_iter()
        .map(|call| call.revision)
        .collect()
}

fn calls(parent: &SequenceDefinition) -> Vec<Call> {
    fn visit(value: &Value, pointer: &str, calls: &mut Vec<Call>) {
        match value {
            Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    visit(item, &format!("{pointer}/{index}"), calls);
                }
            }
            Value::Object(map) => {
                if value["type"] == "sub_sequence"
                    && let Some(id) = value["id"].as_str()
                    && let Some(name) = value["sequence_name"].as_str()
                    && let Some(version) = value["version"]
                        .as_i64()
                        .and_then(|v| i32::try_from(v).ok())
                {
                    calls.push(Call {
                        block_id: id.into(),
                        pointer: pointer.into(),
                        revision: (name.into(), version),
                        input: value["input"].clone(),
                        reference: value["child_result"] == "reference",
                    });
                }
                for (key, child) in block_children(map) {
                    visit(child, &format!("{pointer}/{key}"), calls);
                }
            }
            _ => {}
        }
    }
    let mut calls = Vec::new();
    if let Ok(blocks) = serde_json::to_value(&parent.blocks) {
        visit(&blocks, "/blocks", &mut calls);
    }
    calls
}

/// Check exact published child boundaries. Dynamic expressions are validated
/// at execution when their values become available.
pub fn analyze(
    parent: &SequenceDefinition,
    children: &BTreeMap<(String, i32), SequenceDefinition>,
    now: DateTime<Utc>,
) -> Vec<Finding> {
    let mut input_findings = Vec::new();
    let mut output_schemas = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    for call in calls(parent) {
        let Some(child) = children.get(&call.revision).filter(|child| {
            child.tenant_id == parent.tenant_id
                && child.namespace == parent.namespace
                && (child.name.as_str(), child.version)
                    == (call.revision.0.as_str(), call.revision.1)
        }) else {
            continue;
        };
        check_input(parent, child, &call, now, &mut input_findings);
        if let Some(schema) = result_schema(child, &call) {
            output_schemas.insert(call.block_id.clone(), schema);
            outputs.insert(call.block_id.clone(), (call, child));
        }
    }
    if output_schemas.is_empty() {
        return input_findings;
    }
    let report = compile_with_output_schemas(parent, &output_schemas);
    let mut findings = input_findings;
    for result in report.findings {
        if result.severity != DataflowSeverity::Error
            || matches!(
                result.code.as_str(),
                "PRODUCER_RUNS_LATER" | "PRODUCER_MAY_NOT_RUN"
            )
        {
            continue;
        }
        let Some(block_id) = result
            .reference
            .strip_prefix("outputs.")
            .and_then(|path| path.split(['.', '[']).next())
        else {
            continue;
        };
        let Some((call, child)) = outputs.get(block_id) else {
            continue;
        };
        let Some(source) = result.source else {
            continue;
        };
        let child_location = if call.reference {
            "reference result contract"
        } else {
            "/input_schema/x-orch8-child-result-schema"
        };
        findings.push(finding(
            Boundary::Output,
            &result.consumer,
            child,
            &source.sequence_pointer,
            child_location,
            &format!("{}: {}", result.reference, result.summary),
            now,
        ));
    }
    findings
}

fn result_schema(child: &SequenceDefinition, call: &Call) -> Option<Value> {
    if call.reference {
        return Some(json!({
            "type": "object", "additionalProperties": false,
            "properties": {"child_instance_id": {"type": "string", "format": "uuid"}},
            "required": ["child_instance_id"],
        }));
    }
    child.child_result_schema().cloned()
}

fn check_input(
    parent: &SequenceDefinition,
    child: &SequenceDefinition,
    call: &Call,
    now: DateTime<Utc>,
    findings: &mut Vec<Finding>,
) {
    let Some(schema) = child.input_schema.as_ref() else {
        return;
    };
    let Ok(validator) = jsonschema::validator_for(schema) else {
        return;
    };
    let emit = |findings: &mut Vec<Finding>, input: &str, constraint: &str, summary: &str| {
        findings.push(finding(
            Boundary::Input,
            &call.block_id,
            child,
            &format!("{}/input{input}", call.pointer),
            &format!("/input_schema{constraint}"),
            summary,
            now,
        ));
    };
    for error in validator.iter_errors(&call.input) {
        let instance_path = error.instance_path().to_string();
        if let jsonschema::error::ValidationErrorKind::Required { property } = error.kind()
            && let Some(property) = property.as_str()
        {
            let missing = format!("{instance_path}/{}", escape_pointer(property));
            emit(
                findings,
                &missing,
                &error.schema_path().to_string(),
                "required field is absent",
            );
        } else if call
            .input
            .pointer(&instance_path)
            .is_some_and(|value| !contains_template(value))
        {
            emit(
                findings,
                &instance_path,
                &error.schema_path().to_string(),
                "provided value violates the child schema",
            );
        }
    }
    if contains_template(&call.input) {
        walk_template_inputs(&call.input, "", &mut |template, location| {
            let Some(expected) = schema_at_pointer(schema, location) else {
                return;
            };
            let Some(actual) = template_schema(parent, template) else {
                return;
            };
            if disjoint_types(&actual, &expected) {
                emit(
                    findings,
                    location,
                    &schema_property_pointer(location),
                    "parent template type conflicts with the child field",
                );
            }
        });
    }
}

fn walk_template_inputs(value: &Value, path: &str, visit: &mut impl FnMut(&str, &str)) {
    match value {
        Value::String(template) if template.contains("{{") => visit(template, path),
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk_template_inputs(item, &format!("{path}/{index}"), visit);
            }
        }
        Value::Object(map) => {
            for (name, item) in map {
                walk_template_inputs(item, &format!("{path}/{}", escape_pointer(name)), visit);
            }
        }
        _ => {}
    }
}

fn schema_at_pointer(schema: &Value, pointer: &str) -> Option<Value> {
    let path: Vec<_> = pointer
        .split('/')
        .skip(1)
        .map(|part| part.replace("~1", "/").replace("~0", "~"))
        .collect();
    expanded_field(schema, &path)
}

fn expanded_field(schema: &Value, path: &[String]) -> Option<Value> {
    let schema = expand_local_refs(schema, &mut RenderBudget::default()).ok()?;
    schema_path(&schema, path).1.cloned()
}

fn template_schema(parent: &SequenceDefinition, input: &str) -> Option<Value> {
    let expression = input.strip_prefix("{{")?.strip_suffix("}}")?.trim();
    if !is_template_path(expression) {
        return None;
    }
    let path = expression.strip_prefix("context.").unwrap_or(expression);
    let mut tokens = path.split(['.', '[', ']']).filter(|part| !part.is_empty());
    let root = match tokens.next()? {
        "data" => parent.input_schema.as_ref(),
        "config" => parent.config_schema(),
        "state" => parent.state_schema(),
        _ => None,
    }?;
    expanded_field(root, &tokens.map(str::to_owned).collect::<Vec<_>>())
}

fn disjoint_types(actual: &Value, expected: &Value) -> bool {
    fn kinds(schema: &Value) -> Vec<&str> {
        match &schema["type"] {
            Value::Array(values) => values.iter().filter_map(Value::as_str).collect(),
            Value::String(value) => vec![value],
            _ => Vec::new(),
        }
    }
    let actual = kinds(actual);
    let expected = kinds(expected);
    !actual.is_empty()
        && !expected.is_empty()
        && !actual.iter().any(|a| {
            expected.iter().any(|b| {
                a == b || matches!((*a, *b), ("integer", "number") | ("number", "integer"))
            })
        })
}

fn escape_pointer(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn schema_property_pointer(location: &str) -> String {
    location
        .split('/')
        .skip(1)
        .fold(String::new(), |mut path, part| {
            use std::fmt::Write as _;
            if part.parse::<usize>().is_ok() {
                path.push_str("/items");
            } else {
                write!(path, "/properties/{part}").expect("writing to a String cannot fail");
            }
            path
        })
}

#[derive(Clone, Copy)]
enum Boundary {
    Input,
    Output,
}

fn finding(
    boundary: Boundary,
    block_id: &str,
    child: &SequenceDefinition,
    parent_pointer: &str,
    child_pointer: &str,
    summary: &str,
    now: DateTime<Utc>,
) -> Finding {
    let code = match boundary {
        Boundary::Input => "CHILD_INPUT_CONTRACT_MISMATCH",
        Boundary::Output => "CHILD_OUTPUT_CONTRACT_MISMATCH",
    };
    Finding::new(code, FindingSeverity::Error,
        format!("'{parent_pointer}' and child '{}@{}' '{child_pointer}': {summary}", child.name, child.version),
        Confidence::Certain, now,
    ).with_resource(ResourceRef::new("block", block_id)).with_evidence(
        Evidence::new("child_contract", "Pinned workflow boundary").with_data(json!({
            "parent_pointer": parent_pointer, "child_sequence_id": child.id, "child_pointer": child_pointer,
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_contract_reports_wrong_input_type_and_missing_output_path() {
        let now = Utc::now();
        let make = |name: &str, version: i32, input_schema: Value, blocks: Value| {
            serde_json::from_value::<SequenceDefinition>(json!({
                "id": uuid::Uuid::now_v7(), "tenant_id": "team", "namespace": "default",
                "name": name, "version": version, "input_schema": input_schema,
                "blocks": blocks, "created_at": now,
            }))
            .unwrap()
        };
        let child = make(
            "bill",
            1,
            json!({
                "properties": {"amount": {"type": "integer"}},
                "required": ["amount"]
            }),
            json!([{"type": "step", "id": "post", "handler": "noop"}]),
        );
        let parent = make(
            "parent",
            2,
            json!({
                "properties": {"raw": {"type": "string"}}, "required": ["raw"]
            }),
            json!([
                {"type":"sub_sequence","id":"bill","sequence_name":"bill","version":1,
                 "child_result":"reference","input":{"amount":"{{ context.data.raw }}"}},
                {"type":"step","id":"store","handler":"noop",
                 "params":{"result":"{{ outputs.bill.unavailable }}"}}
            ]),
        );
        let children = BTreeMap::from([(("bill".to_owned(), 1), child)]);
        let findings = analyze(&parent, &children, now);
        assert_eq!(findings.len(), 2);
        assert_eq!(findings[0].code, "CHILD_INPUT_CONTRACT_MISMATCH");
        assert!(findings[0].summary.contains("/blocks/0/input/amount"));
        assert!(
            findings[0]
                .summary
                .contains("/input_schema/properties/amount")
        );
        assert_eq!(findings[1].code, "CHILD_OUTPUT_CONTRACT_MISMATCH");
        assert!(findings[1].summary.contains("/blocks/1/params/result"));
        assert!(findings[1].summary.contains("outputs.bill.unavailable"));
    }
}
