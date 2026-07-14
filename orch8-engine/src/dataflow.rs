//! Static typed-dataflow evidence for workflow definitions.
//!
//! The compiler is deliberately conservative: explicit schema contradictions
//! are errors, while dynamic or undeclared shapes are reported as unknown.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use orch8_types::sequence::SequenceDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DataflowSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowFinding {
    pub code: String,
    pub severity: DataflowSeverity,
    pub consumer: String,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowReport {
    pub findings: Vec<DataflowFinding>,
    pub references_checked: usize,
}

pub const GENERATOR_VERSION: &str = "orch8-dataflow-v1";
const MAX_SCHEMA_DEPTH: usize = 32;
const MAX_SCHEMA_NODES: usize = 4_096;

/// Deterministic language bindings and their canonical source schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GeneratedDataflowTypes {
    pub generator_version: String,
    pub sequence_sha256: String,
    pub schema: Value,
    pub typescript: String,
    pub python: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DataflowGenerationError {
    #[error("sequence cannot be serialized for dataflow generation: {0}")]
    Serialization(String),
    #[error("schema exceeds the dataflow generation depth limit")]
    DepthLimit,
    #[error("schema exceeds the dataflow generation node limit")]
    NodeLimit,
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
    State,
    Config,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Reference {
    root: Root,
    path: Vec<String>,
    display: String,
    has_fallback: bool,
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

/// Generate matching TypeScript and Python bindings from one canonical model.
pub fn generate_types(
    sequence: &SequenceDefinition,
) -> Result<GeneratedDataflowTypes, DataflowGenerationError> {
    let canonical = orch8_publisher::manifest::canonical_json(sequence)
        .map_err(|error| DataflowGenerationError::Serialization(error.to_string()))?;
    let sequence_sha256 = hex_sha256(canonical.as_bytes());
    let serialized: Value = serde_json::from_str(&canonical)
        .map_err(|error| DataflowGenerationError::Serialization(error.to_string()))?;
    let mut output_schemas = BTreeMap::new();
    if let Some(blocks) = serialized.get("blocks") {
        collect_output_schemas(blocks, &mut output_schemas);
    }
    let input_schema = sequence.input_schema.clone().unwrap_or(Value::Bool(true));
    let schema = serde_json::json!({
        "generator_version": GENERATOR_VERSION,
        "sequence": {
            "id": sequence.id,
            "name": sequence.name,
            "version": sequence.version,
            "sha256": sequence_sha256,
        },
        "input": input_schema,
        "outputs": output_schemas,
    });

    let mut ts_budget = RenderBudget::default();
    let input_ts = render_typescript(&schema["input"], 0, &mut ts_budget)?;
    let mut typescript = format!(
        "// Generated by {GENERATOR_VERSION}; sequence sha256: {sequence_sha256}. Do not edit.\n\n\
         export type SequenceInput = {input_ts};\n\n\
         export interface StepOutputs {{\n"
    );
    if let Some(outputs) = schema["outputs"].as_object() {
        let sorted: BTreeMap<_, _> = outputs.iter().collect();
        for (block_id, block_schema) in sorted {
            let rendered = render_typescript(block_schema, 0, &mut ts_budget)?;
            writeln!(
                &mut typescript,
                "  {}: {rendered};",
                serde_json::to_string(block_id).unwrap_or_else(|_| "\"invalid\"".into())
            )
            .expect("writing to String cannot fail");
        }
    }
    typescript.push_str("}\n");

    let mut python_context = PythonContext::default();
    let input_python = render_python(&schema["input"], "SequenceInput", 0, &mut python_context)?;
    let mut output_fields = BTreeMap::new();
    if let Some(outputs) = schema["outputs"].as_object() {
        let sorted: BTreeMap<_, _> = outputs.iter().collect();
        for (block_id, block_schema) in sorted {
            let name = format!("{}Output", pascal_case(block_id));
            let rendered = render_python(block_schema, &name, 0, &mut python_context)?;
            output_fields.insert(block_id.clone(), rendered);
        }
    }
    python_context.definitions.push(render_python_typed_dict(
        "StepOutputs",
        &output_fields,
        &BTreeSet::new(),
    ));
    let mut python = format!(
        "# Generated by {GENERATOR_VERSION}; sequence sha256: {sequence_sha256}. Do not edit.\n\
         from __future__ import annotations\n\n\
         from typing import Any, Literal, Never, NotRequired, Required, TypedDict\n\n"
    );
    python.push_str(&python_context.definitions.join("\n\n"));
    if input_python != "SequenceInput" {
        write!(&mut python, "\n\nSequenceInput = {input_python}")
            .expect("writing to String cannot fail");
    }
    python.push('\n');

    Ok(GeneratedDataflowTypes {
        generator_version: GENERATOR_VERSION.into(),
        sequence_sha256,
        schema,
        typescript,
        python,
    })
}

fn collect_output_schemas(value: &Value, output: &mut BTreeMap<String, Value>) {
    match value {
        Value::Object(map) => {
            if is_block_object(map)
                && let Some(id) = map.get("id").and_then(Value::as_str)
            {
                output.insert(
                    id.to_owned(),
                    map.get("output_schema")
                        .cloned()
                        .unwrap_or(Value::Bool(true)),
                );
            }
            for child in block_children(map) {
                collect_output_schemas(child, output);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_output_schemas(child, output);
            }
        }
        _ => {}
    }
}

#[derive(Default)]
struct RenderBudget {
    nodes: usize,
}

impl RenderBudget {
    fn enter(&mut self, depth: usize) -> Result<(), DataflowGenerationError> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(DataflowGenerationError::DepthLimit);
        }
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > MAX_SCHEMA_NODES {
            return Err(DataflowGenerationError::NodeLimit);
        }
        Ok(())
    }
}

fn render_typescript(
    schema: &Value,
    depth: usize,
    budget: &mut RenderBudget,
) -> Result<String, DataflowGenerationError> {
    budget.enter(depth)?;
    if schema == &Value::Bool(true) {
        return Ok("unknown".into());
    }
    if schema == &Value::Bool(false) {
        return Ok("never".into());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let mut rendered: Vec<String> = values.iter().map(Value::to_string).collect();
        rendered.sort();
        rendered.dedup();
        return Ok(rendered.join(" | "));
    }
    for union_key in ["anyOf", "oneOf"] {
        if let Some(items) = schema.get(union_key).and_then(Value::as_array) {
            let mut rendered = Vec::with_capacity(items.len());
            for item in items {
                rendered.push(render_typescript(item, depth + 1, budget)?);
            }
            rendered.sort();
            rendered.dedup();
            return Ok(rendered.join(" | "));
        }
    }
    if let Some(types) = schema.get("type").and_then(Value::as_array) {
        let mut rendered = Vec::with_capacity(types.len());
        for kind in types {
            rendered.push(render_typescript(
                &serde_json::json!({"type": kind}),
                depth + 1,
                budget,
            )?);
        }
        rendered.sort();
        rendered.dedup();
        return Ok(rendered.join(" | "));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("null") => Ok("null".into()),
        Some("boolean") => Ok("boolean".into()),
        Some("integer" | "number") => Ok("number".into()),
        Some("string") => Ok("string".into()),
        Some("array") => {
            let item = render_typescript(
                schema.get("items").unwrap_or(&Value::Bool(true)),
                depth + 1,
                budget,
            )?;
            Ok(format!("Array<{item}>"))
        }
        Some("object") | None if schema.get("properties").is_some() => {
            let required: BTreeSet<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let sorted: BTreeMap<_, _> = properties.iter().collect();
            let mut fields = Vec::with_capacity(sorted.len() + 1);
            for (name, property_schema) in sorted {
                let value = render_typescript(property_schema, depth + 1, budget)?;
                let optional = if required.contains(name.as_str()) {
                    ""
                } else {
                    "?"
                };
                fields.push(format!(
                    "{}{}: {value};",
                    serde_json::to_string(name).unwrap_or_else(|_| "\"invalid\"".into()),
                    optional
                ));
            }
            if schema.get("additionalProperties") != Some(&Value::Bool(false)) {
                fields.push("[key: string]: unknown;".into());
            }
            Ok(format!("{{ {} }}", fields.join(" ")))
        }
        Some("object") => Ok("Record<string, unknown>".into()),
        _ => Ok("unknown".into()),
    }
}

#[derive(Default)]
struct PythonContext {
    budget: RenderBudget,
    definitions: Vec<String>,
    used_names: BTreeMap<String, usize>,
}

impl PythonContext {
    fn unique_name(&mut self, requested: &str) -> String {
        let base = pascal_case(requested);
        let counter = self.used_names.entry(base.clone()).or_default();
        *counter += 1;
        if *counter == 1 {
            base
        } else {
            format!("{base}{counter}")
        }
    }
}

fn render_python(
    schema: &Value,
    requested_name: &str,
    depth: usize,
    context: &mut PythonContext,
) -> Result<String, DataflowGenerationError> {
    context.budget.enter(depth)?;
    if schema == &Value::Bool(true) {
        return Ok("Any".into());
    }
    if schema == &Value::Bool(false) {
        return Ok("Never".into());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        let mut rendered: Vec<String> = values.iter().map(python_literal).collect();
        rendered.sort();
        rendered.dedup();
        return Ok(format!("Literal[{}]", rendered.join(", ")));
    }
    for union_key in ["anyOf", "oneOf"] {
        if let Some(items) = schema.get(union_key).and_then(Value::as_array) {
            let mut rendered = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                rendered.push(render_python(
                    item,
                    &format!("{requested_name}Variant{index}"),
                    depth + 1,
                    context,
                )?);
            }
            rendered.sort();
            rendered.dedup();
            return Ok(rendered.join(" | "));
        }
    }
    if let Some(types) = schema.get("type").and_then(Value::as_array) {
        let mut rendered = Vec::with_capacity(types.len());
        for (index, kind) in types.iter().enumerate() {
            rendered.push(render_python(
                &serde_json::json!({"type": kind}),
                &format!("{requested_name}Variant{index}"),
                depth + 1,
                context,
            )?);
        }
        rendered.sort();
        rendered.dedup();
        return Ok(rendered.join(" | "));
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("null") => Ok("None".into()),
        Some("boolean") => Ok("bool".into()),
        Some("integer") => Ok("int".into()),
        Some("number") => Ok("float".into()),
        Some("string") => Ok("str".into()),
        Some("array") => {
            let item = render_python(
                schema.get("items").unwrap_or(&Value::Bool(true)),
                &format!("{requested_name}Item"),
                depth + 1,
                context,
            )?;
            Ok(format!("list[{item}]"))
        }
        Some("object") | None if schema.get("properties").is_some() => {
            let name = context.unique_name(requested_name);
            let required: BTreeSet<String> = schema
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect();
            let properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let sorted: BTreeMap<_, _> = properties.iter().collect();
            let mut fields = BTreeMap::new();
            for (field, property_schema) in sorted {
                fields.insert(
                    field.clone(),
                    render_python(
                        property_schema,
                        &format!("{name}{}", pascal_case(field)),
                        depth + 1,
                        context,
                    )?,
                );
            }
            context
                .definitions
                .push(render_python_typed_dict(&name, &fields, &required));
            Ok(name)
        }
        Some("object") => Ok("dict[str, Any]".into()),
        _ => Ok("Any".into()),
    }
}

fn render_python_typed_dict(
    name: &str,
    fields: &BTreeMap<String, String>,
    required: &BTreeSet<String>,
) -> String {
    let rendered = fields
        .iter()
        .map(|(field, value)| {
            let qualifier = if required.contains(field) {
                "Required"
            } else {
                "NotRequired"
            };
            format!("    {}: {qualifier}[{value}],", python_string(field))
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{name} = TypedDict(\n    {name:?},\n    {{\n{rendered}\n    }},\n    total=False,\n)")
}

fn pascal_case(value: &str) -> String {
    let mut output = String::new();
    for segment in value.split(|character: char| !character.is_ascii_alphanumeric()) {
        if segment.is_empty() {
            continue;
        }
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            output.extend(first.to_uppercase());
            output.extend(chars);
        }
    }
    if output.is_empty() || output.starts_with(|character: char| character.is_ascii_digit()) {
        output.insert_str(0, "Generated");
    }
    output
}

fn python_literal(value: &Value) -> String {
    match value {
        Value::Null => "None".into(),
        Value::Bool(value) => if *value { "True" } else { "False" }.into(),
        Value::String(value) => python_string(value),
        _ => value.to_string(),
    }
}

fn python_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"invalid\"".into())
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn collect_blocks(
    value: &Value,
    schemas: &mut BTreeMap<String, Option<Value>>,
    consumers: &mut Vec<(String, Value)>,
) {
    match value {
        Value::Object(map) => {
            if is_block_object(map)
                && let Some(id) = map.get("id").and_then(Value::as_str)
            {
                schemas.insert(id.to_owned(), map.get("output_schema").cloned());
                let mut inspected = serde_json::Map::new();
                for key in [
                    "params",
                    "input",
                    "when",
                    "condition",
                    "collection",
                    "break_on",
                ] {
                    if let Some(candidate) = map.get(key) {
                        inspected.insert(key.to_owned(), candidate.clone());
                    }
                }
                if let Some(routes) = map.get("routes").and_then(Value::as_array) {
                    for (index, route) in routes.iter().enumerate() {
                        if let Some(condition) = route.get("condition") {
                            inspected.insert(format!("route_condition_{index}"), condition.clone());
                        }
                    }
                }
                if !inspected.is_empty() {
                    consumers.push((id.to_owned(), Value::Object(inspected)));
                }
            }
            for child in block_children(map) {
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

fn is_block_object(map: &serde_json::Map<String, Value>) -> bool {
    matches!(
        map.get("type").and_then(Value::as_str),
        Some(
            "step"
                | "parallel"
                | "race"
                | "loop"
                | "for_each"
                | "router"
                | "try_catch"
                | "sub_sequence"
                | "ab_split"
                | "cancellation_scope"
                | "saga"
        )
    )
}

fn block_children(map: &serde_json::Map<String, Value>) -> impl Iterator<Item = &Value> {
    const CHILD_KEYS: &[&str] = &[
        "branches",
        "try_block",
        "catch_block",
        "finally_block",
        "body",
        "routes",
        "default",
        "variants",
        "blocks",
        "steps",
        "action",
        "compensation",
    ];
    CHILD_KEYS.iter().filter_map(|key| map.get(*key))
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
    for needle in ["outputs.", "data.", "state.", "config."] {
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
            if needle == "outputs." {
                if let Some((producer, path)) = segments.split_first() {
                    output.insert(Reference {
                        root: Root::Output(producer.clone()),
                        path: path.to_vec(),
                        display: format!("outputs.{token}"),
                        has_fallback: template_tail_has_fallback(
                            tail.get(token.len()..).unwrap_or_default(),
                        ),
                    });
                }
            } else if !segments.is_empty() {
                let root = match needle {
                    "data." => Root::Data,
                    "state." => Root::State,
                    "config." => Root::Config,
                    _ => unreachable!("reference needles are exhaustive"),
                };
                output.insert(Reference {
                    root,
                    path: segments,
                    display: format!("{needle}{token}"),
                    has_fallback: template_tail_has_fallback(
                        tail.get(token.len()..).unwrap_or_default(),
                    ),
                });
            }
            remaining = tail.get(token.len()..).unwrap_or_default();
        }
    }
}

fn template_tail_has_fallback(tail: &str) -> bool {
    let expression_tail = tail.split("}}").next().unwrap_or(tail);
    let bytes = expression_tail.as_bytes();
    bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'|'
            && bytes.get(index.wrapping_sub(1)) != Some(&b'|')
            && bytes.get(index + 1) != Some(&b'|')
    })
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
        Root::State | Root::Config => None,
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

    match schema_path_status(schema, &reference.path) {
        SchemaPathStatus::Present => {}
        SchemaPathStatus::Missing => findings.push(finding(
            "SCHEMA_PATH_MISSING",
            DataflowSeverity::Error,
            consumer,
            reference,
            "declared schema cannot produce the referenced path".into(),
        )),
        SchemaPathStatus::Dynamic => findings.push(finding(
            "TYPE_UNKNOWN",
            DataflowSeverity::Warning,
            consumer,
            reference,
            "reference traverses a dynamically typed schema path".into(),
        )),
        SchemaPathStatus::Optional if !reference.has_fallback => findings.push(finding(
            "VALUE_MAY_BE_ABSENT",
            DataflowSeverity::Error,
            consumer,
            reference,
            "referenced path is optional and has no explicit fallback".into(),
        )),
        SchemaPathStatus::Nullable if !reference.has_fallback => findings.push(finding(
            "VALUE_MAY_BE_NULL",
            DataflowSeverity::Error,
            consumer,
            reference,
            "referenced path is nullable and has no explicit fallback".into(),
        )),
        SchemaPathStatus::Optional | SchemaPathStatus::Nullable => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaPathStatus {
    Present,
    Missing,
    Dynamic,
    Optional,
    Nullable,
}

fn schema_path_status(schema: &Value, path: &[String]) -> SchemaPathStatus {
    let mut current = schema;
    let mut optional = false;
    for segment in path {
        if let Ok(index) = segment.parse::<usize>() {
            let Some(items) = current.get("items") else {
                return SchemaPathStatus::Missing;
            };
            let index = u64::try_from(index).map_or(u64::MAX, |value| value);
            optional |= current
                .get("minItems")
                .and_then(Value::as_u64)
                .is_none_or(|minimum| minimum <= index);
            current = items;
            continue;
        }
        let Some(properties) = current.get("properties").and_then(Value::as_object) else {
            return if current.get("additionalProperties") == Some(&Value::Bool(false)) {
                SchemaPathStatus::Missing
            } else {
                SchemaPathStatus::Dynamic
            };
        };
        let Some(next) = properties.get(segment) else {
            return if current.get("additionalProperties") == Some(&Value::Bool(false)) {
                SchemaPathStatus::Missing
            } else {
                SchemaPathStatus::Dynamic
            };
        };
        let required = current
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.iter().any(|value| value.as_str() == Some(segment)));
        optional |= !required;
        current = next;
    }
    if optional {
        SchemaPathStatus::Optional
    } else if schema_is_nullable(current) {
        SchemaPathStatus::Nullable
    } else {
        SchemaPathStatus::Present
    }
}

fn schema_is_nullable(schema: &Value) -> bool {
    schema.get("type").is_some_and(|kind| match kind {
        Value::String(kind) => kind == "null",
        Value::Array(kinds) => kinds.iter().any(|kind| kind.as_str() == Some("null")),
        _ => false,
    }) || ["anyOf", "oneOf"].iter().any(|key| {
        schema
            .get(*key)
            .and_then(Value::as_array)
            .is_some_and(|variants| variants.iter().any(schema_is_nullable))
    })
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
                    "type":"object","properties":{"result":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}},"required":["result"],"additionalProperties":false
                }},
                {"type":"step","id":"use","handler":"noop","params":{"x":"{{ outputs.source.result.ok }}"}}
            ]),
            Some(
                json!({"type":"object","properties":{"user":{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}},"required":["user"],"additionalProperties":false}),
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

    #[test]
    fn parameter_objects_cannot_spoof_producers() {
        let report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{
                    "payload":{"type":"step","id":"spoofed"}
                }},
                {"type":"step","id":"use","handler":"noop","params":{
                    "x":"{{ outputs.spoofed.id }}"
                }}
            ]),
            None,
        ));
        assert_eq!(report.findings[0].code, "MISSING_PRODUCER");
    }

    #[test]
    fn router_state_and_config_references_are_inspected() {
        let report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{},"output_schema":{
                    "type":"object","properties":{"ok":{"type":"boolean"}},
                    "required":["ok"],"additionalProperties":false
                }},
                {"type":"router","id":"route","routes":[{
                    "condition":"outputs.source.ok && state.phase == config.expected_phase",
                    "blocks":[]
                }]}
            ]),
            None,
        ));
        assert_eq!(report.references_checked, 3);
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.reference == "state.phase")
        );
        assert!(
            report
                .findings
                .iter()
                .any(|finding| finding.reference == "config.expected_phase")
        );
        assert!(report.is_compatible());
    }

    #[test]
    fn optional_and_nullable_paths_require_explicit_fallbacks() {
        let schema = json!({
            "type":"object",
            "properties":{
                "optional":{"type":"string"},
                "nullable":{"type":["string","null"]}
            },
            "required":["nullable"],
            "additionalProperties":false
        });
        let unsafe_report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{},"output_schema":schema},
                {"type":"step","id":"use","handler":"noop","params":{
                    "a":"{{ outputs.source.optional }}",
                    "b":"{{ outputs.source.nullable }}"
                }}
            ]),
            None,
        ));
        let codes: BTreeSet<_> = unsafe_report
            .findings
            .iter()
            .map(|finding| finding.code.as_str())
            .collect();
        assert!(codes.contains("VALUE_MAY_BE_ABSENT"));
        assert!(codes.contains("VALUE_MAY_BE_NULL"));
        assert!(!unsafe_report.is_compatible());

        let safe_report = compile(&sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{},"output_schema":schema},
                {"type":"step","id":"use","handler":"noop","params":{
                    "a":"{{ outputs.source.optional | fallback }}",
                    "b":"{{ outputs.source.nullable | default('fallback') }}"
                }}
            ]),
            None,
        ));
        assert!(safe_report.is_compatible(), "{:?}", safe_report.findings);
    }

    #[test]
    fn generates_deterministic_matching_types() {
        let sequence = sequence(
            &json!([
                {"type":"step","id":"source","handler":"noop","params":{},"output_schema":{
                    "type":"object",
                    "properties":{"id":{"type":"string"},"score":{"type":["number","null"]}},
                    "required":["id"],
                    "additionalProperties":false
                }}
            ]),
            Some(json!({
                "type":"object",
                "properties":{"order_id":{"type":"string"},"priority":{"enum":["high","low"]}},
                "required":["order_id"],
                "additionalProperties":false
            })),
        );
        let first = generate_types(&sequence).unwrap();
        let second = generate_types(&sequence).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.sequence_sha256.len(), 64);
        assert!(first.typescript.contains("\"order_id\": string"));
        assert!(first.typescript.contains("\"score\"?: null | number"));
        assert!(first.python.contains("\"order_id\": Required[str]"));
        assert!(
            first
                .python
                .contains("\"score\": NotRequired[None | float]")
        );
        assert_eq!(first.schema["generator_version"], GENERATOR_VERSION);
    }

    #[test]
    fn generation_is_bounded_by_schema_depth() {
        let mut schema = json!({"type":"string"});
        for _ in 0..=MAX_SCHEMA_DEPTH {
            schema = json!({
                "type":"object",
                "properties":{"child": schema},
                "required":["child"],
                "additionalProperties":false
            });
        }
        let error = generate_types(&sequence(&json!([]), Some(schema))).unwrap_err();
        assert_eq!(error, DataflowGenerationError::DepthLimit);
    }
}
