//! Semantic diff between two sequence versions.
//!
//! Answers "what changes *operationally*?" rather than showing a JSON
//! diff: added/removed/reordered blocks, handler and queue changes,
//! retry/timeout/deadline/rate-limit changes, router and A/B changes,
//! template-reference breakage, new credential requirements, and
//! side-effect risk (a block that may execute again or differently).
//!
//! Pure: no storage, no clock.

use std::collections::BTreeMap;

use serde_json::Value;

pub use orch8_types::release::{DiffEntry, DiffSeverity, SemanticDiff};
use orch8_types::sequence::{BUILTIN_HANDLER_NAMES, SequenceDefinition};

use crate::lint::lint_sequence;

/// Handlers whose execution has (or may have) external side effects; a
/// change touching one is `SideEffectRisk`, not merely behavioral.
/// External (non-builtin) handlers are treated as side-effecting too —
/// the engine cannot prove otherwise.
const SIDE_EFFECT_BUILTINS: &[&str] = &[
    "http_request",
    "llm_call",
    "tool_call",
    "mcp_call",
    "agent",
    "emit_event",
    "send_signal",
    "self_modify",
    "memory_store",
    "blob_put",
    "embed",
];

pub(crate) fn handler_has_side_effects(handler: &str) -> bool {
    SIDE_EFFECT_BUILTINS.contains(&handler) || !BUILTIN_HANDLER_NAMES.contains(&handler)
}

/// Everything we track per step, extracted from the serialized form so
/// new DSL fields default to "compared as JSON".
#[derive(Debug, Clone, PartialEq, Default)]
struct StepFacts {
    handler: String,
    params: Value,
    queue: Option<String>,
    retry: Option<Value>,
    timeout: Option<Value>,
    deadline: Option<Value>,
    delay: Option<Value>,
    send_window: Option<Value>,
    rate_limit_key: Option<String>,
    wait_for_input: bool,
    fallback_handler: Option<String>,
    output_schema: Option<serde_json::Value>,
    when: Option<String>,
    /// DFS position for reorder detection.
    position: usize,
}

#[derive(Debug, Default)]
struct SequenceFacts {
    steps: BTreeMap<String, StepFacts>,
    /// DFS order of every block id.
    order: Vec<String>,
    /// Router id -> route conditions.
    routers: BTreeMap<String, Vec<String>>,
    /// AB split id -> (variant name, weight) list.
    ab_splits: BTreeMap<String, Vec<(String, i64)>>,
    /// Sub-sequence block id -> (name, version).
    sub_sequences: BTreeMap<String, (String, Option<i64>)>,
    /// All `credentials://ID` references.
    credentials: Vec<String>,
    /// All `outputs.<block>` references found in step params.
    output_refs: Vec<(String, String)>, // (referencing block, referenced block)
}

#[allow(clippy::too_many_lines)]
fn collect(seq: &SequenceDefinition) -> SequenceFacts {
    fn walk(value: &Value, facts: &mut SequenceFacts) {
        match value {
            Value::Object(map) => {
                let ty = map.get("type").and_then(Value::as_str);
                let id = map.get("id").and_then(Value::as_str);
                if let Some(id) = id {
                    facts.order.push(id.to_string());
                    if let Some(Value::String(handler)) = map.get("handler") {
                        let position = facts.order.len();
                        let params = map.get("params").cloned().unwrap_or(Value::Null);
                        collect_output_refs(id, &params, facts);
                        facts.steps.insert(
                            id.to_string(),
                            StepFacts {
                                handler: handler.clone(),
                                params,
                                queue: str_field(map, "queue_name"),
                                retry: map.get("retry").cloned().filter(|v| !v.is_null()),
                                timeout: map.get("timeout").cloned().filter(|v| !v.is_null()),
                                deadline: map.get("deadline").cloned().filter(|v| !v.is_null()),
                                delay: map.get("delay").cloned().filter(|v| !v.is_null()),
                                send_window: map
                                    .get("send_window")
                                    .cloned()
                                    .filter(|v| !v.is_null()),
                                rate_limit_key: str_field(map, "rate_limit_key"),
                                wait_for_input: map
                                    .get("wait_for_input")
                                    .is_some_and(|v| !v.is_null()),
                                fallback_handler: str_field(map, "fallback_handler"),
                                output_schema: map
                                    .get("output_schema")
                                    .cloned()
                                    .filter(|v| !v.is_null()),
                                when: str_field(map, "when"),
                                position,
                            },
                        );
                    }
                    if ty == Some("router")
                        && let Some(Value::Array(routes)) = map.get("routes")
                    {
                        let conditions = routes
                            .iter()
                            .filter_map(|r| r.get("condition").and_then(Value::as_str))
                            .map(ToString::to_string)
                            .collect();
                        facts.routers.insert(id.to_string(), conditions);
                    }
                    if ty == Some("a_b_split")
                        && let Some(Value::Array(variants)) = map.get("variants")
                    {
                        let weights = variants
                            .iter()
                            .map(|v| {
                                (
                                    v.get("name")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_string(),
                                    v.get("weight").and_then(Value::as_i64).unwrap_or(0),
                                )
                            })
                            .collect();
                        facts.ab_splits.insert(id.to_string(), weights);
                    }
                    if ty == Some("sub_sequence")
                        && let Some(name) = map.get("sequence_name").and_then(Value::as_str)
                    {
                        facts.sub_sequences.insert(
                            id.to_string(),
                            (name.to_string(), map.get("version").and_then(Value::as_i64)),
                        );
                    }
                }
                for child in map.values() {
                    walk(child, facts);
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item, facts);
                }
            }
            Value::String(s) => {
                if let Some(rest) = s.strip_prefix("credentials://") {
                    let id = rest.split('/').next().unwrap_or(rest);
                    if !id.is_empty() && !facts.credentials.contains(&id.to_string()) {
                        facts.credentials.push(id.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    let mut facts = SequenceFacts::default();
    if let Ok(v) = serde_json::to_value(seq)
        && let Some(blocks) = v.get("blocks")
    {
        walk(blocks, &mut facts);
    }
    facts
}

fn str_field(map: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    map.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

/// Record `outputs.<block>` references inside a step's params.
fn collect_output_refs(step_id: &str, params: &Value, facts: &mut SequenceFacts) {
    fn walk(step_id: &str, value: &Value, facts: &mut SequenceFacts) {
        match value {
            Value::String(s) => {
                let mut rest = s.as_str();
                while let Some(pos) = rest.find("outputs.") {
                    let after = &rest[pos + "outputs.".len()..];
                    let referenced: String = after
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                        .collect();
                    if !referenced.is_empty() {
                        facts.output_refs.push((step_id.to_string(), referenced));
                    }
                    rest = after;
                }
            }
            Value::Object(map) => {
                for v in map.values() {
                    walk(step_id, v, facts);
                }
            }
            Value::Array(items) => {
                for v in items {
                    walk(step_id, v, facts);
                }
            }
            _ => {}
        }
    }
    walk(step_id, params, facts);
}

/// Compute the semantic diff from `baseline` to `candidate`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn semantic_diff(
    baseline: &SequenceDefinition,
    candidate: &SequenceDefinition,
) -> SemanticDiff {
    let old = collect(baseline);
    let new = collect(candidate);
    let mut entries: Vec<DiffEntry> = Vec::new();

    let mut push =
        |category: &str, severity: DiffSeverity, block: Option<&str>, summary: String| {
            entries.push(DiffEntry {
                category: category.to_string(),
                severity,
                block_id: block.map(ToString::to_string),
                summary,
            });
        };

    // --- removed / added blocks ---
    for (id, facts) in &old.steps {
        if !new.steps.contains_key(id) {
            push(
                "block_removed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("step '{id}' (handler '{}') was removed", facts.handler),
            );
        }
    }
    for (id, facts) in &new.steps {
        if !old.steps.contains_key(id) {
            let severity = if handler_has_side_effects(&facts.handler) {
                DiffSeverity::SideEffectRisk
            } else {
                DiffSeverity::Behavioral
            };
            push(
                "block_added",
                severity,
                Some(id),
                format!(
                    "step '{id}' (handler '{}') was added{}",
                    facts.handler,
                    if severity == DiffSeverity::SideEffectRisk {
                        " — it has external side effects and will run for migrated in-flight instances"
                    } else {
                        ""
                    }
                ),
            );
        }
    }

    // --- reordering of common blocks ---
    let common_old: Vec<&String> = old
        .order
        .iter()
        .filter(|id| new.order.contains(id))
        .collect();
    let common_new: Vec<&String> = new
        .order
        .iter()
        .filter(|id| old.order.contains(id))
        .collect();
    if common_old != common_new {
        push(
            "blocks_reordered",
            DiffSeverity::Behavioral,
            None,
            "shared blocks execute in a different order".to_string(),
        );
    }

    // --- per-step field changes ---
    for (id, old_step) in &old.steps {
        let Some(new_step) = new.steps.get(id) else {
            continue;
        };
        if old_step.handler != new_step.handler {
            push(
                "handler_changed",
                DiffSeverity::SideEffectRisk,
                Some(id),
                format!(
                    "step '{id}' handler changed: '{}' → '{}'",
                    old_step.handler, new_step.handler
                ),
            );
        }
        if old_step.params != new_step.params {
            let severity = if handler_has_side_effects(&new_step.handler) {
                DiffSeverity::SideEffectRisk
            } else {
                DiffSeverity::Behavioral
            };
            push(
                "params_changed",
                severity,
                Some(id),
                format!("step '{id}' parameters changed"),
            );
        }
        if old_step.queue != new_step.queue {
            push(
                "queue_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!(
                    "step '{id}' queue changed: {:?} → {:?}",
                    old_step.queue, new_step.queue
                ),
            );
        }
        for (field, old_v, new_v) in [
            ("retry_changed", &old_step.retry, &new_step.retry),
            ("timeout_changed", &old_step.timeout, &new_step.timeout),
            ("deadline_changed", &old_step.deadline, &new_step.deadline),
            ("delay_changed", &old_step.delay, &new_step.delay),
            (
                "send_window_changed",
                &old_step.send_window,
                &new_step.send_window,
            ),
        ] {
            if old_v != new_v {
                push(
                    field,
                    DiffSeverity::Behavioral,
                    Some(id),
                    format!(
                        "step '{id}' {} configuration changed",
                        field.trim_end_matches("_changed")
                    ),
                );
            }
        }
        if old_step.rate_limit_key != new_step.rate_limit_key {
            push(
                "rate_limit_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("step '{id}' rate-limit key changed"),
            );
        }
        if old_step.wait_for_input != new_step.wait_for_input {
            push(
                "approval_gate_changed",
                DiffSeverity::Behavioral,
                Some(id),
                if new_step.wait_for_input {
                    format!("step '{id}' now requires human input")
                } else {
                    format!("step '{id}' no longer requires human input")
                },
            );
        }
        if old_step.fallback_handler != new_step.fallback_handler {
            push(
                "fallback_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("step '{id}' fallback handler changed"),
            );
        }
        if old_step.output_schema != new_step.output_schema {
            push(
                "output_schema_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("step '{id}' output schema changed"),
            );
        }
        if old_step.when != new_step.when {
            push(
                "when_guard_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("step '{id}' conditional guard changed"),
            );
        }
    }

    // --- routers, A/B, sub-sequences ---
    for (id, old_routes) in &old.routers {
        if let Some(new_routes) = new.routers.get(id)
            && old_routes != new_routes
        {
            push(
                "router_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("router '{id}' route conditions changed"),
            );
        }
    }
    for (id, old_weights) in &old.ab_splits {
        if let Some(new_weights) = new.ab_splits.get(id)
            && old_weights != new_weights
        {
            push(
                "ab_weights_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!("A/B split '{id}' weights changed"),
            );
        }
    }
    for (id, old_ref) in &old.sub_sequences {
        if let Some(new_ref) = new.sub_sequences.get(id)
            && old_ref != new_ref
        {
            push(
                "sub_sequence_changed",
                DiffSeverity::Behavioral,
                Some(id),
                format!(
                    "sub-sequence '{id}' target changed: {}@{:?} → {}@{:?}",
                    old_ref.0, old_ref.1, new_ref.0, new_ref.1
                ),
            );
        }
    }

    // --- dangling output references in the candidate ---
    for (referencing, referenced) in &new.output_refs {
        let exists = new.order.iter().any(|b| b == referenced);
        if !exists {
            push(
                "missing_output_reference",
                DiffSeverity::Incompatible,
                Some(referencing),
                format!(
                    "step '{referencing}' references outputs.{referenced}, but no block \
                     '{referenced}' exists in the candidate"
                ),
            );
        }
    }

    // --- new credential requirements ---
    for cred in &new.credentials {
        if !old.credentials.contains(cred) {
            push(
                "credential_requirement_added",
                DiffSeverity::Behavioral,
                None,
                format!("the candidate newly requires credential '{cred}'"),
            );
        }
    }

    // --- input schema narrowing ---
    let old_required = required_fields(baseline.input_schema.as_ref());
    let new_required = required_fields(candidate.input_schema.as_ref());
    let narrowed: Vec<&String> = new_required
        .iter()
        .filter(|f| !old_required.contains(*f))
        .collect();
    if !narrowed.is_empty() {
        push(
            "input_schema_narrowed",
            DiffSeverity::Incompatible,
            None,
            format!(
                "the candidate requires new input field(s): {} — existing callers will \
                 be rejected",
                narrowed
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    } else if baseline.input_schema != candidate.input_schema {
        push(
            "input_schema_changed",
            DiffSeverity::Behavioral,
            None,
            "the input schema changed (no new required fields)".to_string(),
        );
    }

    entries.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then(a.category.cmp(&b.category))
            .then(a.block_id.cmp(&b.block_id))
    });
    let max_severity = entries.first().map(|e| e.severity);
    let candidate_lint = lint_sequence(candidate)
        .iter()
        .map(ToString::to_string)
        .collect();

    SemanticDiff {
        entries,
        max_severity,
        candidate_lint,
    }
}

fn required_fields(schema: Option<&Value>) -> Vec<String> {
    schema
        .and_then(|s| s.get("required"))
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seq(blocks: Value) -> SequenceDefinition {
        seq_with_schema(blocks, None)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn seq_with_schema(blocks: Value, input_schema: Option<Value>) -> SequenceDefinition {
        let mut v = json!({
            "id": uuid::Uuid::now_v7(),
            "tenant_id": "t1",
            "namespace": "default",
            "name": "diff-seq",
            "version": 1,
            "blocks": blocks,
            "created_at": "2026-01-01T00:00:00Z"
        });
        if let Some(schema) = input_schema {
            v["input_schema"] = schema;
        }
        serde_json::from_value(v).expect("valid sequence")
    }

    fn categories(diff: &SemanticDiff) -> Vec<&str> {
        diff.entries.iter().map(|e| e.category.as_str()).collect()
    }

    fn entry<'a>(diff: &'a SemanticDiff, category: &str) -> &'a DiffEntry {
        diff.entries
            .iter()
            .find(|e| e.category == category)
            .unwrap_or_else(|| panic!("no {category} entry: {:#?}", diff.entries))
    }

    #[test]
    fn identical_sequences_have_empty_diff() {
        let blocks = json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {"x": 1}}
        ]);
        let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
        assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
        assert!(diff.max_severity.is_none());
    }

    #[test]
    fn added_and_removed_blocks_detected() {
        let old = seq(json!([
            {"type": "step", "id": "keep", "handler": "noop", "params": {}},
            {"type": "step", "id": "gone", "handler": "log", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "keep", "handler": "noop", "params": {}},
            {"type": "step", "id": "fresh", "handler": "transform", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert_eq!(
            entry(&diff, "block_removed").block_id.as_deref(),
            Some("gone")
        );
        let added = entry(&diff, "block_added");
        assert_eq!(added.block_id.as_deref(), Some("fresh"));
        // transform has no side effects → behavioral only.
        assert_eq!(added.severity, DiffSeverity::Behavioral);
    }

    #[test]
    fn added_side_effecting_block_is_side_effect_risk() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "charge", "handler": "http_request", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert_eq!(
            entry(&diff, "block_added").severity,
            DiffSeverity::SideEffectRisk
        );
        assert_eq!(diff.max_severity, Some(DiffSeverity::SideEffectRisk));
    }

    #[test]
    fn external_handlers_count_as_side_effecting() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "x", "handler": "custom_worker_thing", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert_eq!(
            entry(&diff, "block_added").severity,
            DiffSeverity::SideEffectRisk
        );
    }

    #[test]
    fn handler_change_is_side_effect_risk() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "log", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "http_request", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        let e = entry(&diff, "handler_changed");
        assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
        assert!(e.summary.contains("'log' → 'http_request'"));
    }

    #[test]
    fn reorder_detected() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "b", "handler": "noop", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "b", "handler": "noop", "params": {}},
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert!(categories(&diff).contains(&"blocks_reordered"));
    }

    #[test]
    fn retry_timeout_deadline_changes_are_behavioral() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {},
             "retry": {"max_attempts": 3, "initial_backoff": 1000, "max_backoff": 5000},
             "timeout": 30_000}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {},
             "retry": {"max_attempts": 5, "initial_backoff": 1000, "max_backoff": 5000},
             "timeout": 60_000, "deadline": 120_000}
        ]));
        let diff = semantic_diff(&old, &new);
        let cats = categories(&diff);
        assert!(cats.contains(&"retry_changed"), "{cats:?}");
        assert!(cats.contains(&"timeout_changed"), "{cats:?}");
        assert!(cats.contains(&"deadline_changed"), "{cats:?}");
        assert_eq!(diff.max_severity, Some(DiffSeverity::Behavioral));
    }

    #[test]
    fn params_change_severity_follows_handler() {
        let old = seq(json!([
            {"type": "step", "id": "safe", "handler": "transform", "params": {"a": 1}},
            {"type": "step", "id": "risky", "handler": "http_request", "params": {"url": "x"}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "safe", "handler": "transform", "params": {"a": 2}},
            {"type": "step", "id": "risky", "handler": "http_request", "params": {"url": "y"}}
        ]));
        let diff = semantic_diff(&old, &new);
        let params_entries: Vec<&DiffEntry> = diff
            .entries
            .iter()
            .filter(|e| e.category == "params_changed")
            .collect();
        assert_eq!(params_entries.len(), 2);
        let safe = params_entries
            .iter()
            .find(|e| e.block_id.as_deref() == Some("safe"))
            .unwrap();
        let risky = params_entries
            .iter()
            .find(|e| e.block_id.as_deref() == Some("risky"))
            .unwrap();
        assert_eq!(safe.severity, DiffSeverity::Behavioral);
        assert_eq!(risky.severity, DiffSeverity::SideEffectRisk);
    }

    #[test]
    fn router_and_ab_changes_detected() {
        let old = seq(json!([
            {"type": "router", "id": "r", "routes": [
                {"condition": "kind == \"a\"", "blocks": [
                    {"type": "step", "id": "s1", "handler": "noop", "params": {}}
                ]}
            ]},
            {"type": "a_b_split", "id": "ab", "variants": [
                {"name": "control", "weight": 70, "blocks": []},
                {"name": "test", "weight": 30, "blocks": []}
            ]}
        ]));
        let new = seq(json!([
            {"type": "router", "id": "r", "routes": [
                {"condition": "kind == \"b\"", "blocks": [
                    {"type": "step", "id": "s1", "handler": "noop", "params": {}}
                ]}
            ]},
            {"type": "a_b_split", "id": "ab", "variants": [
                {"name": "control", "weight": 50, "blocks": []},
                {"name": "test", "weight": 50, "blocks": []}
            ]}
        ]));
        let diff = semantic_diff(&old, &new);
        let cats = categories(&diff);
        assert!(cats.contains(&"router_changed"), "{cats:?}");
        assert!(cats.contains(&"ab_weights_changed"), "{cats:?}");
    }

    #[test]
    fn dangling_output_reference_is_incompatible() {
        let old = seq(json!([
            {"type": "step", "id": "fetch", "handler": "noop", "params": {}},
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}}
        ]));
        // Candidate removes `fetch` but `use` still references it.
        let new = seq(json!([
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}}
        ]));
        let diff = semantic_diff(&old, &new);
        let e = entry(&diff, "missing_output_reference");
        assert_eq!(e.severity, DiffSeverity::Incompatible);
        assert!(e.summary.contains("outputs.fetch"));
        assert_eq!(diff.max_severity, Some(DiffSeverity::Incompatible));
    }

    #[test]
    fn input_schema_narrowing_is_incompatible() {
        let old = seq_with_schema(
            json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
            Some(json!({"type": "object", "required": ["email"]})),
        );
        let new = seq_with_schema(
            json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
            Some(json!({"type": "object", "required": ["email", "phone"]})),
        );
        let diff = semantic_diff(&old, &new);
        let e = entry(&diff, "input_schema_narrowed");
        assert_eq!(e.severity, DiffSeverity::Incompatible);
        assert!(e.summary.contains("phone"));
    }

    #[test]
    fn input_schema_widening_is_behavioral_only() {
        let old = seq_with_schema(
            json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
            Some(json!({"type": "object", "required": ["email", "phone"]})),
        );
        let new = seq_with_schema(
            json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
            Some(json!({"type": "object", "required": ["email"]})),
        );
        let diff = semantic_diff(&old, &new);
        assert!(categories(&diff).contains(&"input_schema_changed"));
        assert!(!categories(&diff).contains(&"input_schema_narrowed"));
    }

    #[test]
    fn new_credential_requirement_detected() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "noop",
             "params": {"key": "credentials://stripe_key/secret"}}
        ]));
        let diff = semantic_diff(&old, &new);
        let e = entry(&diff, "credential_requirement_added");
        assert!(e.summary.contains("stripe_key"));
    }

    #[test]
    fn sub_sequence_retarget_detected() {
        let old = seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 1, "input": {}}
        ]));
        let new = seq(json!([
            {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 2, "input": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert!(categories(&diff).contains(&"sub_sequence_changed"));
    }

    #[test]
    fn entries_sorted_most_severe_first() {
        let old = seq(json!([
            {"type": "step", "id": "fetch", "handler": "noop", "params": {}},
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}},
            {"type": "step", "id": "notify", "handler": "http_request", "params": {}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert_eq!(diff.entries[0].severity, DiffSeverity::Incompatible);
        // Severities are non-increasing down the list.
        for pair in diff.entries.windows(2) {
            assert!(pair[0].severity >= pair[1].severity);
        }
    }

    #[test]
    fn approval_gate_change_detected() {
        let old = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}}
        ]));
        let new = seq(json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {},
             "wait_for_input": {"prompt": "approve?"}}
        ]));
        let diff = semantic_diff(&old, &new);
        assert!(
            entry(&diff, "approval_gate_changed")
                .summary
                .contains("now requires")
        );
    }
}
