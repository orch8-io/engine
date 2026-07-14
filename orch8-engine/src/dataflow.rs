//! Static typed-dataflow evidence for workflow definitions.
//!
//! The compiler is deliberately conservative: explicit schema contradictions
//! are errors, while dynamic or undeclared shapes are reported as unknown.

use std::collections::{BTreeMap, BTreeSet};

use orch8_types::sequence::SequenceDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataflowSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowFinding {
    pub code: String,
    pub severity: DataflowSeverity,
    pub consumer: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataflowReport {
    pub findings: Vec<DataflowFinding>,
    pub references_checked: usize,
}

impl DataflowReport {
    #[must_use]
    pub fn is_compatible(&self) -> bool {
        !self
            .findings
            .iter()
            .any(|finding| finding.severity == DataflowSeverity::Error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Root {
    Data,
    Output(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Reference {
    root: Root,
    path: Vec<String>,
    display: String,
}

/// Compile template and expression references against declared schemas.
#[must_use]
pub fn compile(sequence: &SequenceDefinition) -> DataflowReport {
    let Ok(serialized) = serde_json::to_value(sequence) else {
        return DataflowReport {
            findings: vec![DataflowFinding {
                code: "SEQUENCE_SERIALIZATION_FAILED".into(),
                severity: DataflowSeverity::Error,
                consumer: "sequence".into(),
                reference: String::new(),
                summary: "sequence could not be normalized for dataflow analysis".into(),
            }],
            references_checked: 0,
        };
    };

    let mut schemas = BTreeMap::new();
    let mut consumers = Vec::new();
    if let Some(blocks) = serialized.get("blocks") {
        collect_blocks(blocks, &mut schemas, &mut consumers);
    }

    let mut report = DataflowReport::default();
    for (consumer, value) in consumers {
        let references = collect_references(&value);
        report.references_checked += references.len();
        for reference in references {
            validate_reference(
                &consumer,
                &reference,
                sequence.input_schema.as_ref(),
                &schemas,
                &mut report.findings,
            );
        }
    }
    report.findings.sort_by(|left, right| {
        (&left.consumer, &left.code, &left.reference).cmp(&(
            &right.consumer,
            &right.code,
            &right.reference,
        ))
    });
    report
}

fn collect_blocks(
    value: &Value,
    schemas: &mut BTreeMap<String, Option<Value>>,
    consumers: &mut Vec<(String, Value)>,
) {
    match value {
        Value::Object(map) => {
            if let Some(id) = map.get("id").and_then(Value::as_str) {
                schemas.insert(id.to_owned(), map.get("output_schema").cloned());
                let mut inspected = serde_json::Map::new();
                for key in ["params", "when", "condition"] {
                    if let Some(candidate) = map.get(key) {
                        inspected.insert(key.to_owned(), candidate.clone());
                    }
                }
                if !inspected.is_empty() {
                    consumers.push((id.to_owned(), Value::Object(inspected)));
                }
            }
            for child in map.values() {
                collect_blocks(child, schemas, consumers);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_blocks(child, schemas, consumers);
            }
        }
        _ => {}
    }
}

fn collect_references(value: &Value) -> BTreeSet<Reference> {
    fn walk(value: &Value, output: &mut BTreeSet<Reference>) {
        match value {
            Value::String(text) => parse_references(text, output),
            Value::Array(items) => {
                for item in items {
                    walk(item, output);
                }
            }
            Value::Object(map) => {
                for item in map.values() {
                    walk(item, output);
                }
            }
            _ => {}
        }
    }

    let mut references = BTreeSet::new();
    walk(value, &mut references);
    references
}

fn parse_references(text: &str, output: &mut BTreeSet<Reference>) {
    for (needle, is_output) in [("outputs.", true), ("data.", false)] {
        let mut remaining = text;
        while let Some(position) = remaining.find(needle) {
            let tail = &remaining[position + needle.len()..];
            let token: String = tail
                .chars()
                .take_while(|character| {
                    character.is_ascii_alphanumeric()
                        || matches!(character, '_' | '-' | '.' | '[' | ']')
                })
                .collect();
            let segments: Vec<String> = token
                .split(|character| ['.', '[', ']'].contains(&character))
                .filter(|segment| !segment.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if is_output {
                if let Some((producer, path)) = segments.split_first() {
                    output.insert(Reference {
                        root: Root::Output(producer.clone()),
                        path: path.to_vec(),
                        display: format!("outputs.{token}"),
                    });
                }
            } else if !segments.is_empty() {
                output.insert(Reference {
                    root: Root::Data,
                    path: segments,
                    display: format!("data.{token}"),
                });
            }
            remaining = tail.get(token.len()..).unwrap_or_default();
        }
    }
}

fn validate_reference(
    consumer: &str,
    reference: &Reference,
    input_schema: Option<&Value>,
    output_schemas: &BTreeMap<String, Option<Value>>,
    findings: &mut Vec<DataflowFinding>,
) {
    let schema = match &reference.root {
        Root::Data => input_schema,
        Root::Output(producer) => match output_schemas.get(producer) {
            None => {
                findings.push(finding(
                    "MISSING_PRODUCER",
                    DataflowSeverity::Error,
                    consumer,
                    reference,
                    format!("referenced producer '{producer}' does not exist"),
                ));
                return;
            }
            Some(schema) => schema.as_ref(),
        },
    };

    let Some(schema) = schema else {
        findings.push(finding(
            "TYPE_UNKNOWN",
            DataflowSeverity::Warning,
            consumer,
            reference,
            "reference cannot be proven because its producer has no schema".into(),
        ));
        return;
    };

    if !schema_contains(schema, &reference.path) {
        findings.push(finding(
            "SCHEMA_PATH_MISSING",
            DataflowSeverity::Error,
            consumer,
            reference,
            "declared schema cannot produce the referenced path".into(),
        ));
    }
}

fn schema_contains(schema: &Value, path: &[String]) -> bool {
    let mut current = schema;
    for segment in path {
        if segment.parse::<usize>().is_ok() {
            let Some(items) = current.get("items") else {
                return false;
            };
            current = items;
            continue;
        }
        let Some(properties) = current.get("properties").and_then(Value::as_object) else {
            return current.get("additionalProperties") != Some(&Value::Bool(false));
        };
        let Some(next) = properties.get(segment) else {
            return current.get("additionalProperties") != Some(&Value::Bool(false));
        };
        current = next;
    }
    true
}

fn finding(
    code: &str,
    severity: DataflowSeverity,
    consumer: &str,
    reference: &Reference,
    summary: String,
) -> DataflowFinding {
    DataflowFinding {
        code: code.to_owned(),
        severity,
        consumer: consumer.to_owned(),
        reference: reference.display.clone(),
        summary,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sequence(blocks: &Value, input_schema: Option<Value>) -> SequenceDefinition {
        let mut value = json!({
            "id": uuid::Uuid::now_v7(),
            "tenant_id": "test",
            "namespace": "default",
            "name": "typed",
            "version": 1,
            "blocks": blocks,
            "created_at": "2026-01-01T00:00:00Z"
        });
        value["input_schema"] = input_schema.unwrap_or(Value::Null);
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn rejects_missing_producer() {
        let report = compile(&sequence(
            &json!([{"type":"step","id":"use","handler":"noop","params":{"x":"{{ outputs.absent.id }}"}}]),
            None,
        ));
        assert!(!report.is_compatible());
        assert_eq!(report.findings[0].code, "MISSING_PRODUCER");
    }

    #[test]
    fn rejects_field_excluded_by_closed_schema() {
        let report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{},"output_schema":{
                    "type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false
                }},
                {"type":"step","id":"use","handler":"noop","params":{"x":"{{ outputs.source.missing }}"}}
            ]),
            None,
        ));
        assert!(!report.is_compatible());
        assert_eq!(report.findings[0].code, "SCHEMA_PATH_MISSING");
    }

    #[test]
    fn accepts_declared_input_and_output_paths() {
        let report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{"user":"{{ data.user.id }}"},"output_schema":{
                    "type":"object","properties":{"result":{"type":"object","properties":{"ok":{"type":"boolean"}},"additionalProperties":false}},"additionalProperties":false
                }},
                {"type":"step","id":"use","handler":"noop","params":{"x":"{{ outputs.source.result.ok }}"}}
            ]),
            Some(
                json!({"type":"object","properties":{"user":{"type":"object","properties":{"id":{"type":"string"}},"additionalProperties":false}},"additionalProperties":false}),
            ),
        ));
        assert!(report.is_compatible(), "{:?}", report.findings);
        assert_eq!(report.references_checked, 2);
    }

    #[test]
    fn undeclared_schema_is_unknown_not_false_pass() {
        let report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{}},
                {"type":"step","id":"use","handler":"noop","params":{"x":"{{ outputs.source.value }}"}}
            ]),
            None,
        ));
        assert!(report.is_compatible());
        assert_eq!(report.findings[0].code, "TYPE_UNKNOWN");
    }
}
