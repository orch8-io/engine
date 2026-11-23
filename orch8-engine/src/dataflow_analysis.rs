//! Pure typed-dataflow analysis shared by native and offline preflight.
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use orch8_types::finding::{Confidence, Evidence, Finding, FindingSeverity, ResourceRef};
use orch8_types::preflight::{PreflightCheck, PreflightStatus};
use orch8_types::sequence::SequenceDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<DataflowSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowSource {
    /// JSON Pointer to the string containing the reference in the sequence.
    pub sequence_pointer: String,
    /// UTF-8 byte range of the reference within that decoded string.
    pub start_byte: usize,
    pub end_byte: usize,
    /// JSON Pointer to the producer's declared schema, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_schema_pointer: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DataflowReport {
    pub findings: Vec<DataflowFinding>,
    pub references_checked: usize,
}

pub(crate) const MAX_SCHEMA_DEPTH: usize = 32;
pub(crate) const MAX_SCHEMA_NODES: usize = 4_096;

/// Finding codes emitted by this compiler that must force a live-migration
/// plan into the `Incompatible` disposition.
pub(crate) const MISSING_PRODUCER: &str = "MISSING_PRODUCER";
pub(crate) const SCHEMA_PATH_MISSING: &str = "SCHEMA_PATH_MISSING";
pub(crate) const PRODUCER_RUNS_LATER: &str = "PRODUCER_RUNS_LATER";
pub(crate) const PRODUCER_MAY_NOT_RUN: &str = "PRODUCER_MAY_NOT_RUN";

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

#[derive(Clone, Copy)]
struct ItemBinding<'a> {
    name: &'a str,
    collection: &'a str,
}

struct Consumer<'a> {
    id: &'a str,
    fields: Value,
    bindings: Vec<ItemBinding<'a>>,
    pointer: String,
}

/// Compile template and expression references against declared schemas.
#[must_use]
pub fn compile(sequence: &SequenceDefinition) -> DataflowReport {
    compile_with_output_schemas(sequence, &BTreeMap::new())
}

/// Analyze a definition using schemas resolved from its pinned child revisions.
#[must_use]
pub fn compile_with_output_schemas(
    sequence: &SequenceDefinition,
    pinned_outputs: &BTreeMap<String, Value>,
) -> DataflowReport {
    let Ok(serialized) = serde_json::to_value(sequence) else {
        return DataflowReport {
            findings: vec![DataflowFinding {
                code: "SEQUENCE_SERIALIZATION_FAILED".into(),
                severity: DataflowSeverity::Error,
                consumer: "sequence".into(),
                reference: String::new(),
                summary: "sequence could not be normalized for dataflow analysis".into(),
                source: None,
            }],
            references_checked: 0,
        };
    };

    let mut schemas = BTreeMap::new();
    let mut consumers = Vec::new();
    let mut positions = BTreeMap::new();
    let mut schema_pointers = BTreeMap::new();
    if let Some(blocks) = serialized.get("blocks") {
        collect_blocks(
            blocks,
            "/blocks",
            &mut schemas,
            &mut consumers,
            &mut Vec::new(),
            &mut schema_pointers,
        );
        collect_positions(blocks, &mut Vec::new(), &mut positions);
    }
    let mut budget = RenderBudget::default();
    let input_schema = expand_for_analysis(sequence.input_schema.as_ref(), &mut budget);
    let config_schema = expand_for_analysis(sequence.config_schema(), &mut budget);
    let state_schema = expand_for_analysis(sequence.state_schema(), &mut budget);
    let roots = RootSchemas {
        data: input_schema.as_ref(),
        config: config_schema.as_ref(),
        state: state_schema.as_ref(),
    };
    for (id, schema) in &mut schemas {
        *schema = expand_for_analysis(pinned_outputs.get(id).or(schema.as_ref()), &mut budget);
    }

    let mut report = DataflowReport::default();
    for Consumer {
        id: consumer,
        fields: value,
        bindings,
        pointer,
    } in consumers
    {
        let mut references = BTreeMap::new();
        for (key, field) in value.as_object().into_iter().flatten() {
            let field_pointer = match key.strip_prefix("route_condition_") {
                Some(index) => format!("{pointer}/routes/{index}/condition"),
                None => pointer_child(&pointer, key),
            };
            collect_sources(field, &field_pointer, &mut references);
        }
        let item_schemas =
            loop_item_schemas(&bindings, input_schema.as_ref(), &schemas, &schema_pointers);
        let mandatory: BTreeSet<_> = ["params", "input", "collection"]
            .iter()
            .filter_map(|key| value.get(*key))
            .flat_map(|value| collect_references(value, true))
            .filter(|reference| !reference.has_fallback)
            .collect();
        let numeric = collect_numeric_references(&value);
        report.references_checked += references.len();
        for (reference, locations) in references {
            let finding_start = report.findings.len();
            if mandatory.contains(&reference) {
                validate_producer_order(consumer, &reference, &positions, &mut report.findings);
            }
            validate_reference(
                consumer,
                &reference,
                &roots,
                &schemas,
                &item_schemas,
                numeric.contains(&reference.display),
                &mut report.findings,
            );
            let schema_pointer =
                producer_schema_pointer(&reference, &roots, &item_schemas, &schema_pointers);
            for finding in &mut report.findings[finding_start..] {
                let mut source = locations.clone();
                source.producer_schema_pointer.clone_from(&schema_pointer);
                finding.source = Some(source);
            }
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

#[derive(PartialEq, Eq, Clone, Copy)]
enum PositionPart {
    Sequential(usize),
    Alternative(usize),
    Concurrent(usize),
    ErrorPath(usize),
    RepeatedBody,
}

struct BlockPosition {
    path: Vec<PositionPart>,
    guarded: bool,
}

fn collect_positions<'a>(
    blocks: &'a Value,
    path: &mut Vec<PositionPart>,
    positions: &mut BTreeMap<&'a str, BlockPosition>,
) {
    fn visit<'a>(
        blocks: &'a Value,
        part: PositionPart,
        path: &mut Vec<PositionPart>,
        positions: &mut BTreeMap<&'a str, BlockPosition>,
    ) {
        path.push(part);
        collect_positions(blocks, path, positions);
        path.pop();
    }

    for (index, block) in blocks.as_array().into_iter().flatten().enumerate() {
        let (Some(id), Some(kind)) = (block["id"].as_str(), block["type"].as_str()) else {
            continue;
        };
        path.push(PositionPart::Sequential(index));
        positions.insert(
            id,
            BlockPosition {
                path: path.clone(),
                guarded: block["when"].is_string(),
            },
        );
        match kind {
            "parallel" | "race" => {
                for (index, branch) in block["branches"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    let part = if kind == "parallel" {
                        PositionPart::Concurrent(index)
                    } else {
                        PositionPart::Alternative(index)
                    };
                    visit(branch, part, path, positions);
                }
            }
            "router" => {
                if let Some(routes) = block["routes"].as_array() {
                    for (index, route) in routes.iter().enumerate() {
                        visit(
                            &route["blocks"],
                            PositionPart::Alternative(index),
                            path,
                            positions,
                        );
                    }
                    visit(
                        &block["default"],
                        PositionPart::Alternative(routes.len()),
                        path,
                        positions,
                    );
                }
            }
            "ab_split" | "a_b_split" => {
                for (index, variant) in block["variants"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    visit(
                        &variant["blocks"],
                        PositionPart::Alternative(index),
                        path,
                        positions,
                    );
                }
            }
            "try_catch" => {
                for (index, key) in ["try_block", "catch_block", "finally_block"]
                    .into_iter()
                    .enumerate()
                {
                    visit(&block[key], PositionPart::ErrorPath(index), path, positions);
                }
            }
            "loop" | "for_each" => {
                visit(&block["body"], PositionPart::RepeatedBody, path, positions);
            }
            "cancellation_scope" => collect_positions(&block["blocks"], path, positions),
            _ => {}
        }
        path.pop();
    }
}

fn validate_producer_order(
    consumer: &str,
    reference: &Reference,
    positions: &BTreeMap<&str, BlockPosition>,
    findings: &mut Vec<DataflowFinding>,
) {
    use PositionPart::{Alternative, Concurrent, ErrorPath, RepeatedBody, Sequential};

    let Root::Output(producer) = &reference.root else {
        return;
    };
    let (Some(producer), Some(consumer_position)) =
        (positions.get(producer.as_str()), positions.get(consumer))
    else {
        return;
    };
    let divergence = producer
        .path
        .iter()
        .zip(&consumer_position.path)
        .position(|(lhs, rhs)| lhs != rhs);
    let Some(divergence) = divergence else {
        // Ancestor blocks can expose runtime markers to their children.
        return;
    };
    match (
        producer.path[divergence],
        consumer_position.path[divergence],
    ) {
        (Sequential(producer_index), Sequential(consumer_index))
            if producer_index > consumer_index =>
        {
            findings.push(finding(
                PRODUCER_RUNS_LATER,
                DataflowSeverity::Error,
                consumer,
                reference,
                "the producer executes after this mandatory read".into(),
            ));
        }
        (Sequential(_), Sequential(_))
            if producer.guarded
                || producer.path[divergence + 1..].iter().any(|part| {
                    matches!(part, Alternative(_) | ErrorPath(0 | 1) | RepeatedBody)
                }) =>
        {
            findings.push(finding(
                PRODUCER_MAY_NOT_RUN,
                DataflowSeverity::Warning,
                consumer,
                reference,
                "only some execution paths reach the producer before this read".into(),
            ));
        }
        (Alternative(_) | Concurrent(_), Alternative(_) | Concurrent(_)) => {
            findings.push(finding(PRODUCER_MAY_NOT_RUN, DataflowSeverity::Warning, consumer, reference,
                "the producer is in a different branch and is not guaranteed to run before this read".into()));
        }
        _ => {}
    }
}

#[derive(Default)]
pub(crate) struct RenderBudget {
    nodes: usize,
}

impl RenderBudget {
    pub(crate) fn enter(&mut self, depth: usize) -> Result<(), DataflowGenerationError> {
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

fn expand_for_analysis(schema: Option<&Value>, budget: &mut RenderBudget) -> Option<Value> {
    schema.map(|schema| expand_local_refs(schema, budget).unwrap_or(Value::Bool(true)))
}

pub(crate) fn expand_local_refs(
    schema: &Value,
    budget: &mut RenderBudget,
) -> Result<Value, DataflowGenerationError> {
    LocalSchemaExpansion {
        budget,
        references: BTreeSet::new(),
    }
    .expand(schema, schema, 0)
}

struct LocalSchemaExpansion<'a, 'schema> {
    budget: &'a mut RenderBudget,
    references: BTreeSet<(*const Value, &'schema str)>,
}

impl<'schema> LocalSchemaExpansion<'_, 'schema> {
    fn expand(
        &mut self,
        schema: &'schema Value,
        scope: &'schema Value,
        depth: usize,
    ) -> Result<Value, DataflowGenerationError> {
        self.budget.enter(depth)?;
        let Some(object) = schema.as_object() else {
            return Ok(schema.clone());
        };
        let scope = if object.contains_key("$id") {
            schema
        } else {
            scope
        };
        if let Some(reference) = object.get("$ref") {
            return self.expand_reference(object, reference, scope, depth);
        }

        let mut expanded = serde_json::Map::new();
        for (key, value) in object {
            let value = match key.as_str() {
                // Each schema keeps its definitions scoped to its own document.
                // Only definitions reached from analysis or type emission expand.
                "$defs" => continue,
                "properties" if value.is_object() => Value::Object(
                    value
                        .as_object()
                        .expect("checked schema properties")
                        .iter()
                        .map(|(name, child)| {
                            Ok((name.clone(), self.expand(child, scope, depth + 1)?))
                        })
                        .collect::<Result<_, DataflowGenerationError>>()?,
                ),
                "anyOf" | "oneOf" | "prefixItems" if value.is_array() => Value::Array(
                    value
                        .as_array()
                        .expect("checked schema array")
                        .iter()
                        .map(|child| self.expand(child, scope, depth + 1))
                        .collect::<Result<_, _>>()?,
                ),
                "items" | "additionalProperties" => self.expand(value, scope, depth + 1)?,
                _ => value.clone(),
            };
            expanded.insert(key.clone(), value);
        }
        Ok(Value::Object(expanded))
    }

    fn expand_reference(
        &mut self,
        object: &serde_json::Map<String, Value>,
        reference: &'schema Value,
        scope: &'schema Value,
        depth: usize,
    ) -> Result<Value, DataflowGenerationError> {
        // Assertions alongside a reference form an intersection. The binding
        // emitters do not model intersections, so retain the dynamic type.
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "$ref"
                    | "$defs"
                    | "$id"
                    | "$schema"
                    | "$comment"
                    | "title"
                    | "description"
                    | "default"
                    | "examples"
                    | "deprecated"
                    | "readOnly"
                    | "writeOnly"
            )
        }) {
            return Ok(Value::Bool(true));
        }
        let Some(reference) = reference
            .as_str()
            .filter(|value| value.starts_with("#/$defs/"))
        else {
            return Ok(Value::Bool(true));
        };
        let Some(target) = scope.pointer(&reference[1..]) else {
            return Ok(Value::Bool(true));
        };
        let key = (std::ptr::from_ref(scope), reference);
        if !self.references.insert(key) {
            return Ok(Value::Bool(true));
        }
        let expanded = self.expand(target, scope, depth + 1);
        self.references.remove(&key);
        expanded
    }
}

fn collect_blocks<'a>(
    value: &'a Value,
    pointer: &str,
    schemas: &mut BTreeMap<String, Option<Value>>,
    consumers: &mut Vec<Consumer<'a>>,
    bindings: &mut Vec<ItemBinding<'a>>,
    schema_pointers: &mut BTreeMap<String, String>,
) {
    match value {
        Value::Object(map) => {
            if is_block_object(map)
                && let Some(id) = map.get("id").and_then(Value::as_str)
            {
                schemas.insert(id.to_owned(), map.get("output_schema").cloned());
                if map.contains_key("output_schema") {
                    schema_pointers.insert(id.to_owned(), pointer_child(pointer, "output_schema"));
                }
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
                    consumers.push(Consumer {
                        id,
                        fields: Value::Object(inspected),
                        bindings: bindings.clone(),
                        pointer: pointer.to_owned(),
                    });
                }
            }
            let loop_binding = match (
                map.get("type").and_then(Value::as_str),
                map.get("item_var").and_then(Value::as_str),
                map.get("collection").and_then(Value::as_str),
            ) {
                (Some("for_each"), Some(name), Some(collection)) => {
                    Some(ItemBinding { name, collection })
                }
                _ => None,
            };
            if let Some(binding) = loop_binding {
                bindings.push(binding);
            }
            for (key, child) in block_children(map) {
                collect_blocks(
                    child,
                    &pointer_child(pointer, key),
                    schemas,
                    consumers,
                    bindings,
                    schema_pointers,
                );
            }
            if loop_binding.is_some() {
                bindings.pop();
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_blocks(
                    child,
                    &format!("{pointer}/{index}"),
                    schemas,
                    consumers,
                    bindings,
                    schema_pointers,
                );
            }
        }
        _ => {}
    }
}

type ItemSchemas<'a> = BTreeMap<&'a str, (Option<&'a Value>, Option<&'a str>)>;

fn loop_item_schemas<'a>(
    bindings: &[ItemBinding<'a>],
    input_schema: Option<&'a Value>,
    output_schemas: &'a BTreeMap<String, Option<Value>>,
    schema_pointers: &'a BTreeMap<String, String>,
) -> ItemSchemas<'a> {
    let mut items = BTreeMap::new();
    for binding in bindings {
        let item = collection_schema(
            binding.collection,
            input_schema,
            output_schemas,
            schema_pointers,
            &items,
        )
        .filter(|(schema, _)| {
            schema
                .get("prefixItems")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty)
        })
        .and_then(|(schema, pointer)| Some((Some(schema.get("items")?), pointer)))
        .unwrap_or_default();
        items.insert(binding.name, item);
    }
    items
}

fn collection_schema<'a>(
    expression: &str,
    input_schema: Option<&'a Value>,
    output_schemas: &'a BTreeMap<String, Option<Value>>,
    schema_pointers: &'a BTreeMap<String, String>,
    items: &ItemSchemas<'a>,
) -> Option<(&'a Value, Option<&'a str>)> {
    let expression = expression.trim();
    let (schema, path, is_data, schema_pointer) = if let Some(template) = expression
        .strip_prefix("{{")
        .and_then(|value| value.strip_suffix("}}"))
    {
        let path = template
            .trim()
            .strip_prefix("context.")
            .unwrap_or(template.trim());
        if let Some(data) = path.strip_prefix("data.") {
            (
                input_schema,
                data,
                true,
                input_schema.map(|_| "/input_schema"),
            )
        } else {
            let output = path.strip_prefix("outputs.")?;
            let (producer, path) = output.split_once('.').unwrap_or((output, ""));
            (
                output_schemas.get(producer)?.as_ref(),
                path,
                false,
                schema_pointers.get(producer).map(String::as_str),
            )
        }
    } else {
        (
            input_schema,
            expression,
            true,
            input_schema.map(|_| "/input_schema"),
        )
    };
    if !path
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
    {
        return None;
    }
    let path: Vec<_> = path
        .split('.')
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect();
    let (schema, path, schema_pointer) = if is_data {
        let pointer = data_schema_pointer(input_schema, &path, items);
        let (schema, path) = data_root(&path, schema, items);
        (schema, path, pointer)
    } else {
        (schema, path.as_slice(), schema_pointer)
    };
    schema_path(schema?, path)
        .1
        .map(|value| (value, schema_pointer))
}

fn data_schema_pointer<'a>(
    input_schema: Option<&Value>,
    original_path: &[String],
    items: &ItemSchemas<'a>,
) -> Option<&'a str> {
    original_path
        .first()
        .and_then(|name| items.get(name.as_str()))
        .map_or_else(
            || input_schema.map(|_| "/input_schema"),
            |(_, pointer)| *pointer,
        )
}

fn data_root<'a, 'path>(
    path: &'path [String],
    input_schema: Option<&'a Value>,
    items: &ItemSchemas<'a>,
) -> (Option<&'a Value>, &'path [String]) {
    if let Some((name, rest)) = path.split_first()
        && let Some(schema) = items.get(name.as_str())
    {
        return (schema.0, rest);
    }
    (input_schema, path)
}

pub(crate) fn is_block_object(map: &serde_json::Map<String, Value>) -> bool {
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
                // Legacy spelling emitted before v0.7.1.
                | "a_b_split"
                | "cancellation_scope"
                | "saga"
        )
    )
}

pub(crate) fn block_children(
    map: &serde_json::Map<String, Value>,
) -> impl Iterator<Item = (&'static str, &Value)> {
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
    CHILD_KEYS
        .iter()
        .filter_map(|&key| map.get(key).map(|child| (key, child)))
}

fn pointer_child(pointer: &str, token: &str) -> String {
    format!("{pointer}/{}", token.replace('~', "~0").replace('/', "~1"))
}

fn collect_sources(value: &Value, pointer: &str, output: &mut BTreeMap<Reference, DataflowSource>) {
    match value {
        Value::String(text) => parse_references(text, |reference, range| {
            output.entry(reference).or_insert_with(|| DataflowSource {
                sequence_pointer: pointer.to_owned(),
                start_byte: range.start,
                end_byte: range.end,
                producer_schema_pointer: None,
            });
        }),
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_sources(child, &format!("{pointer}/{index}"), output);
            }
        }
        Value::Object(map) => {
            for (key, child) in map {
                collect_sources(child, &pointer_child(pointer, key), output);
            }
        }
        _ => {}
    }
}

fn collect_references(value: &Value, only_templates: bool) -> BTreeSet<Reference> {
    fn walk(value: &Value, only_templates: bool, output: &mut BTreeSet<Reference>) {
        match value {
            Value::String(text) if only_templates => {
                for fragment in text.split("{{").skip(1) {
                    if let Some((expression, _)) = fragment.split_once("}}") {
                        parse_references(expression, |reference, _| {
                            output.insert(reference);
                        });
                    }
                }
            }
            Value::String(text) => parse_references(text, |reference, _| {
                output.insert(reference);
            }),
            Value::Array(items) => {
                for item in items {
                    walk(item, only_templates, output);
                }
            }
            Value::Object(map) => {
                for item in map.values() {
                    walk(item, only_templates, output);
                }
            }
            _ => {}
        }
    }

    let mut references = BTreeSet::new();
    walk(value, only_templates, &mut references);
    references
}

fn collect_numeric_references(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| {
            matches!(key.as_str(), "when" | "condition" | "break_on")
                || key.starts_with("route_condition_")
        })
        .filter_map(|(_, value)| value.as_str())
        .flat_map(crate::expression::direct_numeric_operand_paths)
        .map(|path| path.strip_prefix("context.").unwrap_or(&path).to_owned())
        .collect()
}

fn parse_references(text: &str, mut emit: impl FnMut(Reference, std::ops::Range<usize>)) {
    for needle in ["outputs.", "data.", "state.", "config."] {
        let mut remaining = text;
        while let Some(position) = remaining.find(needle) {
            // Require a word boundary: a needle glued onto a larger
            // identifier ("metadata.user", "window.outputs.log") is prose,
            // not a typed reference.
            let absolute = text.len() - remaining.len() + position;
            let prefix = &text[..absolute];
            let start = absolute
                - if prefix.ends_with("context.") {
                    "context.".len()
                } else {
                    0
                };
            let prefix = prefix.strip_suffix("context.").unwrap_or(prefix);
            let glued_to_identifier = prefix.chars().next_back().is_some_and(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '.')
            });
            if glued_to_identifier {
                remaining = &remaining[position + needle.len()..];
                continue;
            }
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
            let span = start..absolute + needle.len() + token.len();
            if needle == "outputs." {
                if let Some((producer, path)) = segments.split_first() {
                    emit(
                        Reference {
                            root: Root::Output(producer.clone()),
                            path: path.to_vec(),
                            display: format!("outputs.{token}"),
                            has_fallback: template_tail_has_fallback(
                                tail.get(token.len()..).unwrap_or_default(),
                            ),
                        },
                        span,
                    );
                }
            } else if !segments.is_empty() {
                let root = match needle {
                    "data." => Root::Data,
                    "state." => Root::State,
                    "config." => Root::Config,
                    _ => unreachable!("reference needles are exhaustive"),
                };
                emit(
                    Reference {
                        root,
                        path: segments,
                        display: format!("{needle}{token}"),
                        has_fallback: template_tail_has_fallback(
                            tail.get(token.len()..).unwrap_or_default(),
                        ),
                    },
                    span,
                );
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

fn producer_schema_pointer(
    reference: &Reference,
    roots: &RootSchemas<'_>,
    items: &ItemSchemas<'_>,
    schema_pointers: &BTreeMap<String, String>,
) -> Option<String> {
    match &reference.root {
        Root::Output(producer) => schema_pointers.get(producer).cloned(),
        Root::Data => data_schema_pointer(roots.data, &reference.path, items).map(str::to_owned),
        Root::Config => roots
            .config
            .map(|_| "/input_schema/x-orch8-config-schema".into()),
        Root::State => roots
            .state
            .map(|_| "/input_schema/x-orch8-state-schema".into()),
    }
}

struct RootSchemas<'a> {
    data: Option<&'a Value>,
    config: Option<&'a Value>,
    state: Option<&'a Value>,
}

fn validate_reference(
    consumer: &str,
    reference: &Reference,
    roots: &RootSchemas<'_>,
    output_schemas: &BTreeMap<String, Option<Value>>,
    items: &ItemSchemas<'_>,
    numeric_operand: bool,
    findings: &mut Vec<DataflowFinding>,
) {
    let (schema, path) = match &reference.root {
        Root::Data => data_root(&reference.path, roots.data, items),
        Root::Config => (roots.config, reference.path.as_slice()),
        Root::State => (roots.state, reference.path.as_slice()),
        Root::Output(producer) => match output_schemas.get(producer) {
            None => {
                findings.push(finding(
                    MISSING_PRODUCER,
                    DataflowSeverity::Error,
                    consumer,
                    reference,
                    format!("referenced producer '{producer}' does not exist"),
                ));
                return;
            }
            Some(schema) => (schema.as_ref(), reference.path.as_slice()),
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

    let (status, value_schema) = schema_path(schema, path);
    if numeric_operand
        && value_schema
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            == Some("string")
    {
        findings.push(finding(
            "NUMERIC_OPERAND_TYPE",
            DataflowSeverity::Error,
            consumer,
            reference,
            "numeric operand requires a numeric schema, but the referenced value is declared as a string".into(),
        ));
    }
    match status {
        SchemaPathStatus::Present | SchemaPathStatus::Optional | SchemaPathStatus::Nullable
            if reference.has_fallback => {}
        SchemaPathStatus::Missing => findings.push(finding(
            SCHEMA_PATH_MISSING,
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
        SchemaPathStatus::Optional => findings.push(finding(
            "VALUE_MAY_BE_ABSENT",
            DataflowSeverity::Error,
            consumer,
            reference,
            "referenced path is optional and has no explicit fallback".into(),
        )),
        SchemaPathStatus::Nullable => findings.push(finding(
            "VALUE_MAY_BE_NULL",
            DataflowSeverity::Error,
            consumer,
            reference,
            "referenced path is nullable and has no explicit fallback".into(),
        )),
        SchemaPathStatus::Present => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaPathStatus {
    Present,
    Missing,
    Dynamic,
    Optional,
    Nullable,
}

pub(crate) fn schema_path<'a>(
    schema: &'a Value,
    path: &[String],
) -> (SchemaPathStatus, Option<&'a Value>) {
    let mut current = schema;
    let mut optional = false;
    let mut nullable = false;
    for segment in path {
        match current {
            Value::Bool(true) => return (SchemaPathStatus::Dynamic, None),
            Value::Bool(false) => return (SchemaPathStatus::Missing, None),
            _ => {}
        }
        nullable |= schema_is_nullable(current);
        // A segment that names a real property is a property, never an array
        // index — numeric property names are legal in JSON Schema.
        let property = current
            .get("properties")
            .and_then(|properties| properties.get(segment));
        let is_array = current.get("items").is_some()
            || current.get("prefixItems").is_some()
            || current.get("type").is_some_and(|kind| {
                kind == "array"
                    || kind
                        .as_array()
                        .is_some_and(|kinds| kinds.iter().any(|kind| kind == "array"))
            });
        if property.is_none()
            && is_array
            && let Ok(index) = segment.parse::<usize>()
        {
            let json_index = u64::try_from(index).unwrap_or(u64::MAX);
            if current
                .get("maxItems")
                .and_then(Value::as_u64)
                .is_some_and(|maximum| json_index >= maximum)
            {
                return (SchemaPathStatus::Missing, None);
            }
            let prefix = current.get("prefixItems").and_then(Value::as_array);
            let item = prefix
                .and_then(|items| items.get(index))
                .or_else(|| current.get("items"));
            let item = match item {
                Some(Value::Bool(false)) => return (SchemaPathStatus::Missing, None),
                Some(Value::Bool(true)) | None => return (SchemaPathStatus::Dynamic, None),
                Some(item) => item,
            };
            optional |= current
                .get("minItems")
                .and_then(Value::as_u64)
                .is_none_or(|minimum| minimum <= json_index);
            current = item;
            continue;
        }
        let next = if let Some(property) = property {
            property
        } else if current
            .get("patternProperties")
            .and_then(Value::as_object)
            .is_some_and(|patterns| !patterns.is_empty())
        {
            return (SchemaPathStatus::Dynamic, None);
        } else {
            match current.get("additionalProperties") {
                Some(Value::Bool(false)) => return (SchemaPathStatus::Missing, None),
                Some(next @ Value::Object(_)) => next,
                _ => return (SchemaPathStatus::Dynamic, None),
            }
        };
        let required = current
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.iter().any(|value| value.as_str() == Some(segment)));
        optional |= !required;
        current = next;
    }
    let status = if current == &Value::Bool(false) {
        SchemaPathStatus::Missing
    } else if optional {
        SchemaPathStatus::Optional
    } else if nullable || schema_is_nullable(current) {
        SchemaPathStatus::Nullable
    } else if current == &Value::Bool(true) {
        SchemaPathStatus::Dynamic
    } else {
        SchemaPathStatus::Present
    };
    (status, Some(current))
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
        source: None,
    }
}

pub(crate) fn check_typed_dataflow(seq: &SequenceDefinition, now: DateTime<Utc>) -> PreflightCheck {
    let report = compile(seq);
    if report.findings.is_empty() {
        return PreflightCheck::pass(
            "typed_dataflow_compatible",
            format!(
                "{} typed reference(s) are compatible",
                report.references_checked
            ),
        );
    }
    let findings: Vec<_> = report
        .findings
        .iter()
        .map(|finding| {
            let severity = match finding.severity {
                DataflowSeverity::Warning => FindingSeverity::Warning,
                DataflowSeverity::Error => FindingSeverity::Error,
            };
            let mut result = Finding::new(
                finding.code.clone(),
                severity,
                format!(
                    "producer/reference '{}' -> consumer '{}': {}",
                    finding.reference, finding.consumer, finding.summary
                ),
                Confidence::Certain,
                now,
            )
            .with_resource(ResourceRef::new("block", finding.consumer.clone()));
            if let Some(source) = &finding.source {
                result = result.with_evidence(
                    Evidence::new("dataflow_source", "Reference location in the sequence")
                        .with_data(serde_json::json!(source)),
                );
            }
            result
        })
        .collect();
    let status = if report.is_compatible() {
        PreflightStatus::Warning
    } else {
        PreflightStatus::Fail
    };
    PreflightCheck::with_status(
        "typed_dataflow_compatible",
        status,
        format!("{} typed dataflow finding(s)", findings.len()),
        findings,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn diagnostic_points_to_nested_schema_and_utf8_consumer_expression() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/typed_dataflow_source.json"))
                .unwrap();
        let sequence = orch8_types::sequence::deserialize_sequence_strict(&value).unwrap();
        let report = compile(&sequence);
        let [finding] = report.findings.as_slice() else {
            panic!("expected one finding: {report:?}");
        };
        assert_eq!(finding.code, SCHEMA_PATH_MISSING);
        let source = finding.source.as_ref().unwrap();
        assert_eq!(
            source.sequence_pointer,
            "/blocks/0/branches/0/1/params/value~1~0/0"
        );
        assert_eq!(
            source.producer_schema_pointer.as_deref(),
            Some("/blocks/0/branches/0/0/output_schema")
        );
        let text = value
            .pointer(&source.sequence_pointer)
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(
            &text[source.start_byte..source.end_byte],
            "context.outputs.create.missing"
        );
        assert_eq!(
            value
                .pointer(source.producer_schema_pointer.as_deref().unwrap())
                .unwrap()["$ref"],
            "#/$defs/record"
        );
    }

    #[test]
    fn foreach_item_schemas_follow_nested_lexical_shadowing() {
        let sequence = orch8_types::sequence::deserialize_sequence_strict(&json!({
            "name":"items","tenant_id":"test","namespace":"default","version":1,
            "id":"01900000-0000-7000-8000-000000000001","created_at":"2026-01-01T00:00:00Z",
            "input_schema":{"type":"object","additionalProperties":false,"required":["row","rows"],"properties":{
                "row":{"type":"object","additionalProperties":false,"required":["outside"],"properties":{"outside":{"type":"integer"}}},
                "rows":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["id","values"],"properties":{
                    "id":{"type":"string"},"values":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["value"],"properties":{"value":{"type":"integer"}}}}
                }}}
            }},
            "blocks":[{"type":"for_each","id":"rows","item_var":"row","collection":"rows","body":[
                {"type":"step","id":"outer_read","handler":"transform","params":{"id":"{{ data.row.id }}"}},
                {"type":"for_each","id":"nested","item_var":"row","collection":"{{context.data.row.values}}","body":[
                    {"type":"step","id":"inner_read","handler":"transform","params":{"value":"{{ data.row.value }}", "invalid":"{{ data.row.id }}"}}
                ]}
            ]},{"type":"step","id":"outside","handler":"transform","params":{"outside":"{{ data.row.outside }}"}}]
        })).unwrap();
        let report = compile(&sequence);
        assert_eq!(report.findings.len(), 1, "{report:?}");
        let finding = &report.findings[0];
        assert_eq!(
            (
                finding.consumer.as_str(),
                finding.reference.as_str(),
                finding.code.as_str()
            ),
            ("inner_read", "data.row.id", SCHEMA_PATH_MISSING)
        );
    }

    #[test]
    fn order_checks_distinguish_later_conditional_and_always_available_producers() {
        let blocks = json!([
            {"type":"step","id":"first","handler":"transform"},
            {"type":"step","id":"reader","handler":"transform","params":{
                "earlier":"{{ outputs.first.value }}",
                "later":"{{ outputs.last.value }}",
                "fallback":"{{ outputs.fallback.value | default(0) }}",
                "literal":"mentions outputs.fallback.value in the description"
            }},
            {"type":"router","id":"switch","routes":[{"condition":"data.pick","blocks":[
                {"type":"step","id":"chosen","handler":"transform"},
                {"type":"step","id":"branch_reader","handler":"transform","params":{
                    "value":"{{ outputs.chosen.value }}"
                }}
            ]}]},
            {"type":"step","id":"after_router","handler":"transform","params":{
                "value":"{{ outputs.chosen.value }}"
            }},
            {"type":"step","id":"last","handler":"transform"},
            {"type":"step","id":"fallback","handler":"transform"}
        ]);
        let sequence = orch8_types::sequence::deserialize_sequence_strict(&json!({
            "name":"test","tenant_id":"test","namespace":"default","version":1,
            "id":"01900000-0000-7000-8000-000000000001","created_at":"2026-01-01T00:00:00Z", "blocks":blocks,
        })).unwrap();
        let report = compile(&sequence);
        let ordered: Vec<_> = report
            .findings
            .iter()
            .filter(|finding| {
                matches!(
                    finding.code.as_str(),
                    PRODUCER_RUNS_LATER | PRODUCER_MAY_NOT_RUN
                )
            })
            .map(|finding| {
                (
                    finding.consumer.as_str(),
                    finding.code.as_str(),
                    finding.reference.as_str(),
                )
            })
            .collect();
        assert_eq!(
            ordered,
            [
                ("after_router", PRODUCER_MAY_NOT_RUN, "outputs.chosen.value"),
                ("reader", PRODUCER_RUNS_LATER, "outputs.last.value"),
            ]
        );
        assert!(!report.is_compatible());
    }
}
