//! Extensive unit tests for `orch8_engine::release_diff::semantic_diff`:
//! every diff category, severity attribution, nesting, sorting, and the
//! candidate lint pass-through.

use orch8_engine::release_diff::{DiffEntry, DiffSeverity, SemanticDiff, semantic_diff};
use orch8_types::sequence::SequenceDefinition;
use serde_json::{Value, json};

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

fn step(id: &str, handler: &str) -> Value {
    json!({"type": "step", "id": id, "handler": handler, "params": {}})
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

fn entry_for<'a>(diff: &'a SemanticDiff, category: &str, block: &str) -> &'a DiffEntry {
    diff.entries
        .iter()
        .find(|e| e.category == category && e.block_id.as_deref() == Some(block))
        .unwrap_or_else(|| panic!("no {category} entry for '{block}': {:#?}", diff.entries))
}

fn count(diff: &SemanticDiff, category: &str) -> usize {
    diff.entries
        .iter()
        .filter(|e| e.category == category)
        .count()
}

// ---------------------------------------------------------------------------
// block_added
// ---------------------------------------------------------------------------

#[test]
fn added_pure_builtin_block_is_behavioral() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("fresh", "transform")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "block_added");
    assert_eq!(e.block_id.as_deref(), Some("fresh"));
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.summary.contains("transform"), "{}", e.summary);
}

#[test]
fn added_side_effecting_builtin_block_is_side_effect_risk() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("charge", "http_request")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "block_added");
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
    assert!(e.summary.contains("external side effects"), "{}", e.summary);
}

#[test]
fn added_llm_call_block_is_side_effect_risk() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("gen", "llm_call")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "block_added").severity,
        DiffSeverity::SideEffectRisk
    );
}

#[test]
fn added_external_handler_block_is_side_effect_risk() {
    // A non-builtin handler cannot be proven pure — treated as risky.
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("x", "my_custom_worker")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "block_added").severity,
        DiffSeverity::SideEffectRisk
    );
}

#[test]
fn multiple_added_blocks_each_get_an_entry() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([
        step("a", "noop"),
        step("b", "transform"),
        step("c", "log"),
        step("d", "http_request")
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(count(&diff, "block_added"), 3, "{:#?}", diff.entries);
    assert_eq!(
        entry_for(&diff, "block_added", "b").severity,
        DiffSeverity::Behavioral
    );
    assert_eq!(
        entry_for(&diff, "block_added", "c").severity,
        DiffSeverity::Behavioral
    );
    assert_eq!(
        entry_for(&diff, "block_added", "d").severity,
        DiffSeverity::SideEffectRisk
    );
}

// ---------------------------------------------------------------------------
// block_removed
// ---------------------------------------------------------------------------

#[test]
fn removed_block_is_behavioral_and_names_handler() {
    let old = seq(json!([step("keep", "noop"), step("gone", "log")]));
    let new = seq(json!([step("keep", "noop")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "block_removed");
    assert_eq!(e.block_id.as_deref(), Some("gone"));
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.summary.contains("log"), "{}", e.summary);
}

#[test]
fn removed_side_effecting_block_is_still_behavioral() {
    // Actual behavior: removal severity does not escalate for
    // side-effecting handlers — the step simply stops running.
    let old = seq(json!([
        step("keep", "noop"),
        step("charge", "http_request")
    ]));
    let new = seq(json!([step("keep", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "block_removed").severity,
        DiffSeverity::Behavioral
    );
}

#[test]
fn renamed_block_reports_removed_and_added() {
    let old = seq(json!([step("old_name", "transform")]));
    let new = seq(json!([step("new_name", "transform")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "block_removed").block_id.as_deref(),
        Some("old_name")
    );
    assert_eq!(
        entry(&diff, "block_added").block_id.as_deref(),
        Some("new_name")
    );
}

#[test]
fn multiple_removed_blocks_each_get_an_entry() {
    let old = seq(json!([
        step("a", "noop"),
        step("b", "log"),
        step("c", "transform")
    ]));
    let new = seq(json!([step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(count(&diff, "block_removed"), 2);
}

// ---------------------------------------------------------------------------
// blocks_reordered
// ---------------------------------------------------------------------------

#[test]
fn top_level_reorder_detected() {
    let old = seq(json!([step("a", "noop"), step("b", "noop")]));
    let new = seq(json!([step("b", "noop"), step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "blocks_reordered");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.block_id.is_none());
}

#[test]
fn reorder_inside_parallel_branch_detected() {
    let old = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [step("a", "noop"), step("b", "noop")]
        ]}
    ]));
    let new = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [step("b", "noop"), step("a", "noop")]
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"blocks_reordered"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn swapping_parallel_branches_is_a_reorder() {
    let old = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [step("a", "noop")],
            [step("b", "noop")]
        ]}
    ]));
    let new = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [step("b", "noop")],
            [step("a", "noop")]
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"blocks_reordered"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn insertion_between_blocks_is_not_a_reorder() {
    let old = seq(json!([step("a", "noop"), step("b", "noop")]));
    let new = seq(json!([
        step("a", "noop"),
        step("mid", "log"),
        step("b", "noop")
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        !categories(&diff).contains(&"blocks_reordered"),
        "{:#?}",
        diff.entries
    );
    assert!(categories(&diff).contains(&"block_added"));
}

#[test]
fn removal_alone_is_not_a_reorder() {
    let old = seq(json!([
        step("a", "noop"),
        step("b", "noop"),
        step("c", "noop")
    ]));
    let new = seq(json!([step("a", "noop"), step("c", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert!(
        !categories(&diff).contains(&"blocks_reordered"),
        "{:#?}",
        diff.entries
    );
}

// ---------------------------------------------------------------------------
// handler_changed
// ---------------------------------------------------------------------------

#[test]
fn handler_change_between_pure_handlers_is_side_effect_risk() {
    // Actual behavior: any handler swap is flagged as risk, even
    // pure→pure — the operational identity of the step changed.
    let old = seq(json!([step("a", "log")]));
    let new = seq(json!([step("a", "transform")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "handler_changed");
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
    assert!(e.summary.contains("'log' → 'transform'"), "{}", e.summary);
}

#[test]
fn handler_change_to_side_effecting_carries_block_id() {
    let old = seq(json!([step("notify", "log")]));
    let new = seq(json!([step("notify", "emit_event")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "handler_changed");
    assert_eq!(e.block_id.as_deref(), Some("notify"));
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
}

// ---------------------------------------------------------------------------
// params_changed
// ---------------------------------------------------------------------------

#[test]
fn params_change_on_pure_handler_is_behavioral() {
    let old = seq(json!([{"type": "step", "id": "a", "handler": "transform", "params": {"v": 1}}]));
    let new = seq(json!([{"type": "step", "id": "a", "handler": "transform", "params": {"v": 2}}]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").severity,
        DiffSeverity::Behavioral
    );
}

#[test]
fn params_change_on_side_effecting_handler_is_side_effect_risk() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "http_request", "params": {"url": "x"}}]),
    );
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "http_request", "params": {"url": "y"}}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").severity,
        DiffSeverity::SideEffectRisk
    );
}

#[test]
fn params_change_on_external_handler_is_side_effect_risk() {
    let old =
        seq(json!([{"type": "step", "id": "a", "handler": "acme_worker", "params": {"n": 1}}]));
    let new =
        seq(json!([{"type": "step", "id": "a", "handler": "acme_worker", "params": {"n": 2}}]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").severity,
        DiffSeverity::SideEffectRisk
    );
}

#[test]
fn params_severity_follows_new_handler_when_handler_also_changes() {
    let old = seq(json!([{"type": "step", "id": "a", "handler": "transform", "params": {"v": 1}}]));
    let new =
        seq(json!([{"type": "step", "id": "a", "handler": "http_request", "params": {"v": 2}}]));
    let diff = semantic_diff(&old, &new);
    assert!(categories(&diff).contains(&"handler_changed"));
    assert_eq!(
        entry(&diff, "params_changed").severity,
        DiffSeverity::SideEffectRisk
    );
}

#[test]
fn deeply_nested_param_value_change_detected() {
    let old = seq(json!([{"type": "step", "id": "a", "handler": "transform",
        "params": {"outer": {"inner": {"list": [1, 2, 3]}}}}]));
    let new = seq(json!([{"type": "step", "id": "a", "handler": "transform",
        "params": {"outer": {"inner": {"list": [1, 2, 4]}}}}]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").block_id.as_deref(),
        Some("a")
    );
}

// ---------------------------------------------------------------------------
// queue_changed
// ---------------------------------------------------------------------------

#[test]
fn queue_added_detected() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "queue_name": "priority"}]),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "queue_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.summary.contains("priority"), "{}", e.summary);
}

#[test]
fn queue_removed_detected() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "queue_name": "priority"}]),
    );
    let new = seq(json!([step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert!(categories(&diff).contains(&"queue_changed"));
}

#[test]
fn queue_value_change_detected() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "queue_name": "fast"}]),
    );
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "queue_name": "slow"}]),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "queue_changed");
    assert!(
        e.summary.contains("fast") && e.summary.contains("slow"),
        "{}",
        e.summary
    );
}

// ---------------------------------------------------------------------------
// retry / timeout / deadline / delay / send_window — added, removed, changed
// ---------------------------------------------------------------------------

/// Build (old, new) sequences with three steps per timing field:
/// `add` gains the field, `del` loses it, `chg` changes its value.
#[allow(clippy::needless_pass_by_value)]
fn timing_pair(field: &str, v1: Value, v2: Value) -> (SequenceDefinition, SequenceDefinition) {
    let plain = |id: &str| json!({"type": "step", "id": id, "handler": "noop", "params": {}});
    let with = |id: &str, v: &Value| json!({"type": "step", "id": id, "handler": "noop", "params": {}, field: v});
    let old = seq(json!([plain("add"), with("del", &v1), with("chg", &v1)]));
    let new = seq(json!([with("add", &v1), plain("del"), with("chg", &v2)]));
    (old, new)
}

fn assert_add_del_chg(diff: &SemanticDiff, category: &str) {
    assert_eq!(count(diff, category), 3, "{category}: {:#?}", diff.entries);
    for block in ["add", "del", "chg"] {
        let e = entry_for(diff, category, block);
        assert_eq!(e.severity, DiffSeverity::Behavioral, "{category}/{block}");
    }
}

#[test]
fn retry_added_removed_and_changed_detected() {
    let (old, new) = timing_pair(
        "retry",
        json!({"max_attempts": 3, "initial_backoff": 1000, "max_backoff": 5000}),
        json!({"max_attempts": 5, "initial_backoff": 1000, "max_backoff": 5000}),
    );
    let diff = semantic_diff(&old, &new);
    assert_add_del_chg(&diff, "retry_changed");
}

#[test]
fn timeout_added_removed_and_changed_detected() {
    let (old, new) = timing_pair("timeout", json!(30_000), json!(60_000));
    let diff = semantic_diff(&old, &new);
    assert_add_del_chg(&diff, "timeout_changed");
}

#[test]
fn deadline_added_removed_and_changed_detected() {
    let (old, new) = timing_pair("deadline", json!(120_000), json!(240_000));
    let diff = semantic_diff(&old, &new);
    assert_add_del_chg(&diff, "deadline_changed");
}

#[test]
fn delay_added_removed_and_changed_detected() {
    let (old, new) = timing_pair(
        "delay",
        json!({"duration": 1000}),
        json!({"duration": 5000}),
    );
    let diff = semantic_diff(&old, &new);
    assert_add_del_chg(&diff, "delay_changed");
}

#[test]
fn send_window_added_removed_and_changed_detected() {
    let (old, new) = timing_pair(
        "send_window",
        json!({"start_hour": 9, "end_hour": 17}),
        json!({"start_hour": 8, "end_hour": 20}),
    );
    let diff = semantic_diff(&old, &new);
    assert_add_del_chg(&diff, "send_window_changed");
}

#[test]
fn timing_summaries_name_the_field() {
    let (old, new) = timing_pair("timeout", json!(30_000), json!(60_000));
    let diff = semantic_diff(&old, &new);
    let e = entry_for(&diff, "timeout_changed", "chg");
    assert!(e.summary.contains("timeout"), "{}", e.summary);
}

// ---------------------------------------------------------------------------
// rate_limit_changed
// ---------------------------------------------------------------------------

#[test]
fn rate_limit_key_added_detected() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "rate_limit_key": "provider-api"}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "rate_limit_changed").severity,
        DiffSeverity::Behavioral
    );
}

#[test]
fn rate_limit_key_removed_detected() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "rate_limit_key": "provider-api"}]),
    );
    let new = seq(json!([step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert!(categories(&diff).contains(&"rate_limit_changed"));
}

#[test]
fn rate_limit_key_value_change_detected() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "rate_limit_key": "k1"}]),
    );
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "rate_limit_key": "k2"}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "rate_limit_changed").block_id.as_deref(),
        Some("a")
    );
}

// ---------------------------------------------------------------------------
// approval_gate_changed
// ---------------------------------------------------------------------------

#[test]
fn approval_gate_added_says_now_requires() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "wait_for_input": {"prompt": "approve?"}}]),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "approval_gate_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.summary.contains("now requires"), "{}", e.summary);
}

#[test]
fn approval_gate_removed_says_no_longer_requires() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "wait_for_input": {"prompt": "approve?"}}]),
    );
    let new = seq(json!([step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "approval_gate_changed");
    assert!(e.summary.contains("no longer requires"), "{}", e.summary);
}

// ---------------------------------------------------------------------------
// fallback_changed
// ---------------------------------------------------------------------------

#[test]
fn fallback_handler_added_detected() {
    let old = seq(json!([step("a", "http_request")]));
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "http_request", "params": {},
        "fallback_handler": "log"}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "fallback_changed").severity,
        DiffSeverity::Behavioral
    );
}

#[test]
fn fallback_handler_value_change_detected() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "http_request", "params": {},
        "fallback_handler": "log"}]),
    );
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "http_request", "params": {},
        "fallback_handler": "noop"}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "fallback_changed").block_id.as_deref(),
        Some("a")
    );
}

// ---------------------------------------------------------------------------
// router_changed
// ---------------------------------------------------------------------------

#[test]
fn router_condition_change_detected() {
    let old = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("s1", "noop")]}
        ]}
    ]));
    let new = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"b\"", "blocks": [step("s1", "noop")]}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "router_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert_eq!(e.block_id.as_deref(), Some("r"));
}

#[test]
fn router_route_added_flags_router_changed_and_new_blocks() {
    // Adding a route changes the condition list → router_changed; the new
    // route's inner steps are also independent block_added entries.
    let old = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("s1", "noop")]}
        ]}
    ]));
    let new = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("s1", "noop")]},
            {"condition": "kind == \"b\"", "blocks": [step("s2", "transform")]}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"router_changed"),
        "{:#?}",
        diff.entries
    );
    assert_eq!(entry(&diff, "block_added").block_id.as_deref(), Some("s2"));
}

#[test]
fn identical_router_produces_no_router_entry() {
    let blocks = json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("s1", "noop")]}
        ]}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(
        !categories(&diff).contains(&"router_changed"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn router_removed_entirely_reports_inner_steps_not_router_changed() {
    // Actual behavior: router_changed only compares routers present in
    // both versions; removing the router surfaces via its steps.
    let old = seq(json!([
        step("keep", "noop"),
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("s1", "noop")]}
        ]}
    ]));
    let new = seq(json!([step("keep", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert!(
        !categories(&diff).contains(&"router_changed"),
        "{:#?}",
        diff.entries
    );
    assert_eq!(
        entry(&diff, "block_removed").block_id.as_deref(),
        Some("s1")
    );
}

// ---------------------------------------------------------------------------
// ab_weights_changed
// ---------------------------------------------------------------------------

#[test]
fn ab_weight_change_detected() {
    let old = seq(json!([
        {"type": "a_b_split", "id": "ab", "variants": [
            {"name": "control", "weight": 70, "blocks": []},
            {"name": "test", "weight": 30, "blocks": []}
        ]}
    ]));
    let new = seq(json!([
        {"type": "a_b_split", "id": "ab", "variants": [
            {"name": "control", "weight": 50, "blocks": []},
            {"name": "test", "weight": 50, "blocks": []}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "ab_weights_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert_eq!(e.block_id.as_deref(), Some("ab"));
}

#[test]
fn ab_variant_rename_counts_as_weights_change() {
    let old = seq(json!([
        {"type": "a_b_split", "id": "ab", "variants": [
            {"name": "control", "weight": 50, "blocks": []},
            {"name": "test", "weight": 50, "blocks": []}
        ]}
    ]));
    let new = seq(json!([
        {"type": "a_b_split", "id": "ab", "variants": [
            {"name": "control", "weight": 50, "blocks": []},
            {"name": "challenger", "weight": 50, "blocks": []}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"ab_weights_changed"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn identical_ab_split_produces_no_entry() {
    let blocks = json!([
        {"type": "a_b_split", "id": "ab", "variants": [
            {"name": "control", "weight": 70, "blocks": []},
            {"name": "test", "weight": 30, "blocks": []}
        ]}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
}

// ---------------------------------------------------------------------------
// sub_sequence_changed
// ---------------------------------------------------------------------------

#[test]
fn sub_sequence_version_change_detected() {
    let old = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 1, "input": {}}
    ]));
    let new = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 2, "input": {}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "sub_sequence_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.summary.contains("refund"), "{}", e.summary);
}

#[test]
fn sub_sequence_name_change_detected() {
    let old = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 1, "input": {}}
    ]));
    let new = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "chargeback", "version": 1, "input": {}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "sub_sequence_changed");
    assert!(
        e.summary.contains("refund") && e.summary.contains("chargeback"),
        "{}",
        e.summary
    );
}

#[test]
fn sub_sequence_unpinned_to_pinned_version_detected() {
    // version omitted (latest) → pinned version is a retarget.
    let old = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "input": {}}
    ]));
    let new = seq(json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 3, "input": {}}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"sub_sequence_changed"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn identical_sub_sequence_produces_no_entry() {
    let blocks = json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "refund", "version": 1, "input": {}}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(
        !categories(&diff).contains(&"sub_sequence_changed"),
        "{:#?}",
        diff.entries
    );
}

// ---------------------------------------------------------------------------
// missing_output_reference
// ---------------------------------------------------------------------------

#[test]
fn removed_producer_makes_reference_incompatible() {
    let old = seq(json!([
        step("fetch", "noop"),
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}}
    ]));
    let new = seq(json!([
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "missing_output_reference");
    assert_eq!(e.severity, DiffSeverity::Incompatible);
    assert_eq!(e.block_id.as_deref(), Some("use"));
    assert!(e.summary.contains("outputs.fetch"), "{}", e.summary);
    assert_eq!(diff.max_severity, Some(DiffSeverity::Incompatible));
}

#[test]
fn renamed_producer_breaks_stale_reference() {
    let old = seq(json!([
        step("fetch", "noop"),
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}}
    ]));
    let new = seq(json!([
        step("fetch_v2", "noop"),
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"missing_output_reference"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn dangling_reference_inside_loop_body_detected() {
    let old = seq(json!([
        step("fetch", "noop"),
        {"type": "loop", "id": "l", "condition": "count < 3", "body": [
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}}
        ]}
    ]));
    let new = seq(json!([
        {"type": "loop", "id": "l", "condition": "count < 3", "body": [
            {"type": "step", "id": "use", "handler": "noop",
             "params": {"v": "{{ outputs.fetch.data }}"}}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "missing_output_reference");
    assert_eq!(e.block_id.as_deref(), Some("use"));
}

#[test]
fn dangling_reference_in_inline_string_detected() {
    // Reference embedded inside prose, not a bare template expression.
    let old = seq(json!([
        step("gone", "noop"),
        {"type": "step", "id": "note", "handler": "log",
         "params": {"message": "see outputs.gone.value for details"}}
    ]));
    let new = seq(json!([
        {"type": "step", "id": "note", "handler": "log",
         "params": {"message": "see outputs.gone.value for details"}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "missing_output_reference");
    assert!(e.summary.contains("outputs.gone"), "{}", e.summary);
}

#[test]
fn valid_reference_to_existing_block_is_not_flagged() {
    let blocks = json!([
        step("fetch", "noop"),
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(
        !categories(&diff).contains(&"missing_output_reference"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn reference_to_composite_block_id_is_not_flagged() {
    // `outputs.par` resolves to the parallel block itself, which exists.
    let blocks = json!([
        {"type": "parallel", "id": "par", "branches": [[step("a", "noop")]]},
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.par.results }}"}}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(
        !categories(&diff).contains(&"missing_output_reference"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn dangling_reference_flagged_even_for_identical_sequences() {
    // Actual behavior: the check inspects only the candidate, so a
    // pre-existing dangling reference is reported even with no changes.
    let blocks = json!([
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.ghost.data }}"}}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    let e = entry(&diff, "missing_output_reference");
    assert_eq!(e.severity, DiffSeverity::Incompatible);
    assert_eq!(diff.max_severity, Some(DiffSeverity::Incompatible));
}

#[test]
fn multiple_dangling_references_each_reported() {
    let old = seq(json!([step("p1", "noop"), step("p2", "noop")]));
    let new = seq(json!([
        {"type": "step", "id": "u1", "handler": "noop", "params": {"v": "{{ outputs.p1.x }}"}},
        {"type": "step", "id": "u2", "handler": "noop", "params": {"v": "{{ outputs.p2.x }}"}}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        count(&diff, "missing_output_reference"),
        2,
        "{:#?}",
        diff.entries
    );
}

// ---------------------------------------------------------------------------
// credential_requirement_added
// ---------------------------------------------------------------------------

#[test]
fn new_credential_requirement_detected() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([
        {"type": "step", "id": "a", "handler": "noop",
         "params": {"key": "credentials://stripe_key"}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "credential_requirement_added");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(e.block_id.is_none());
    assert!(e.summary.contains("stripe_key"), "{}", e.summary);
}

#[test]
fn credential_id_parsed_from_path_suffix() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([
        {"type": "step", "id": "a", "handler": "noop",
         "params": {"key": "credentials://vault_id/secret/field"}}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "credential_requirement_added");
    assert!(e.summary.contains("'vault_id'"), "{}", e.summary);
}

#[test]
fn credential_removal_is_not_flagged() {
    // Actual behavior: only *new* requirements are surfaced; dropping a
    // credential is silent.
    let old = seq(json!([
        {"type": "step", "id": "a", "handler": "noop",
         "params": {"key": "credentials://stripe_key"}}
    ]));
    let new = seq(json!([step("a", "noop")]));
    let diff = semantic_diff(&old, &new);
    assert!(
        !categories(&diff).contains(&"credential_requirement_added"),
        "{:#?}",
        diff.entries
    );
}

#[test]
fn unchanged_credential_is_not_flagged() {
    let blocks = json!([
        {"type": "step", "id": "a", "handler": "noop",
         "params": {"key": "credentials://stripe_key"}}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
}

#[test]
fn each_new_credential_gets_its_own_entry() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([
        {"type": "step", "id": "a", "handler": "noop",
         "params": {"k1": "credentials://cred_one", "k2": "credentials://cred_two"}}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        count(&diff, "credential_requirement_added"),
        2,
        "{:#?}",
        diff.entries
    );
}

// ---------------------------------------------------------------------------
// input schema
// ---------------------------------------------------------------------------

fn one_step() -> Value {
    json!([step("a", "noop")])
}

#[test]
fn input_schema_narrowed_is_incompatible_and_lists_fields() {
    let old = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"]})),
    );
    let new = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email", "phone"]})),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "input_schema_narrowed");
    assert_eq!(e.severity, DiffSeverity::Incompatible);
    assert!(e.summary.contains("phone"), "{}", e.summary);
    assert!(
        !e.summary.contains("email, "),
        "only new fields: {}",
        e.summary
    );
}

#[test]
fn input_schema_changed_without_new_required_is_behavioral() {
    let old = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"],
            "properties": {"email": {"type": "string"}}})),
    );
    let new = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"],
            "properties": {"email": {"type": "string", "format": "email"}}})),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "input_schema_changed");
    assert_eq!(e.severity, DiffSeverity::Behavioral);
    assert!(!categories(&diff).contains(&"input_schema_narrowed"));
}

#[test]
fn identical_input_schemas_produce_no_entry() {
    let schema = json!({"type": "object", "required": ["email"]});
    let diff = semantic_diff(
        &seq_with_schema(one_step(), Some(schema.clone())),
        &seq_with_schema(one_step(), Some(schema)),
    );
    assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
}

#[test]
fn adding_schema_with_required_fields_is_narrowing() {
    let old = seq(one_step());
    let new = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"]})),
    );
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "input_schema_narrowed");
    assert_eq!(e.severity, DiffSeverity::Incompatible);
    assert!(e.summary.contains("email"), "{}", e.summary);
}

#[test]
fn adding_schema_without_required_fields_is_only_changed() {
    let old = seq(one_step());
    let new = seq_with_schema(one_step(), Some(json!({"type": "object"})));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"input_schema_changed"),
        "{:#?}",
        diff.entries
    );
    assert!(!categories(&diff).contains(&"input_schema_narrowed"));
}

#[test]
fn removing_schema_is_changed_not_narrowed() {
    let old = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"]})),
    );
    let new = seq(one_step());
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"input_schema_changed"),
        "{:#?}",
        diff.entries
    );
    assert!(!categories(&diff).contains(&"input_schema_narrowed"));
}

#[test]
fn widening_required_fields_is_changed_not_narrowed() {
    let old = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email", "phone"]})),
    );
    let new = seq_with_schema(
        one_step(),
        Some(json!({"type": "object", "required": ["email"]})),
    );
    let diff = semantic_diff(&old, &new);
    assert!(categories(&diff).contains(&"input_schema_changed"));
    assert!(!categories(&diff).contains(&"input_schema_narrowed"));
}

// ---------------------------------------------------------------------------
// empty diff / sorting / max_severity
// ---------------------------------------------------------------------------

#[test]
fn identical_simple_sequences_have_empty_diff() {
    let blocks = json!([step("a", "noop"), step("b", "transform")]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
    assert!(diff.max_severity.is_none());
    assert!(diff.candidate_lint.is_empty(), "{:?}", diff.candidate_lint);
}

#[test]
fn identical_deeply_nested_sequences_have_empty_diff() {
    let blocks = json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [
             {"type": "loop", "id": "l", "condition": "n < 5", "body": [
                 {"type": "parallel", "id": "p", "branches": [
                     [step("s1", "transform")],
                     [step("s2", "log")]
                 ]}
             ]}
         ],
         "catch_block": [step("cleanup", "noop")]}
    ]);
    let diff = semantic_diff(&seq(blocks.clone()), &seq(blocks));
    assert!(diff.entries.is_empty(), "{:#?}", diff.entries);
    assert!(diff.max_severity.is_none());
}

#[test]
fn entries_sorted_by_non_increasing_severity() {
    // Mix of incompatible (dangling ref), side-effect (http added),
    // behavioral (timeout change).
    let old = seq(json!([
        step("fetch", "noop"),
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}, "timeout": 30000}
    ]));
    let new = seq(json!([
        {"type": "step", "id": "use", "handler": "noop",
         "params": {"v": "{{ outputs.fetch.data }}"}, "timeout": 60000},
        step("notify", "http_request")
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(diff.entries.len() >= 3, "{:#?}", diff.entries);
    for pair in diff.entries.windows(2) {
        assert!(
            pair[0].severity >= pair[1].severity,
            "unsorted: {:#?}",
            diff.entries
        );
    }
    assert_eq!(diff.entries[0].severity, DiffSeverity::Incompatible);
}

#[test]
fn entries_with_equal_severity_sorted_by_category_then_block() {
    let old = seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {}, "timeout": 1000},
        {"type": "step", "id": "b", "handler": "noop", "params": {}, "queue_name": "q1"}
    ]));
    let new = seq(json!([
        {"type": "step", "id": "a", "handler": "noop", "params": {}, "timeout": 2000},
        {"type": "step", "id": "b", "handler": "noop", "params": {}, "queue_name": "q2"}
    ]));
    let diff = semantic_diff(&old, &new);
    let cats = categories(&diff);
    // Both behavioral → alphabetical category order.
    assert_eq!(
        cats,
        vec!["queue_changed", "timeout_changed"],
        "{:#?}",
        diff.entries
    );
}

#[test]
fn max_severity_matches_first_entry() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("charge", "http_request")]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(diff.max_severity, Some(diff.entries[0].severity));
    assert_eq!(diff.max_severity, Some(DiffSeverity::SideEffectRisk));
}

#[test]
fn max_severity_behavioral_for_pure_changes_only() {
    let old = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "timeout": 1000}]),
    );
    let new = seq(
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {},
        "timeout": 2000}]),
    );
    let diff = semantic_diff(&old, &new);
    assert_eq!(diff.max_severity, Some(DiffSeverity::Behavioral));
}

// ---------------------------------------------------------------------------
// candidate lint pass-through
// ---------------------------------------------------------------------------

#[test]
fn candidate_lint_included_for_unparked_wait_for_event() {
    // A wait_for_event step without wait_for_input is a classic footgun
    // the linter flags; the diff report must carry it through.
    let blocks = json!([step("wait", "wait_for_event")]);
    let diff = semantic_diff(&seq(json!([step("a", "noop")])), &seq(blocks));
    assert!(
        diff.candidate_lint
            .iter()
            .any(|w| w.contains("wait_for_input") && w.contains("wait")),
        "expected wait_for_event lint warning, got {:?}",
        diff.candidate_lint
    );
}

#[test]
fn candidate_lint_reflects_candidate_not_baseline() {
    // Baseline has the lint problem; candidate is clean → no lint.
    let dirty = json!([step("wait", "wait_for_event")]);
    let clean = json!([step("a", "noop")]);
    let diff = semantic_diff(&seq(dirty), &seq(clean));
    assert!(
        !diff
            .candidate_lint
            .iter()
            .any(|w| w.contains("wait_for_input")),
        "baseline lint leaked into candidate report: {:?}",
        diff.candidate_lint
    );
}

#[test]
fn clean_candidate_has_empty_lint() {
    let old = seq(json!([step("a", "noop")]));
    let new = seq(json!([step("a", "noop"), step("b", "transform")]));
    let diff = semantic_diff(&old, &new);
    assert!(diff.candidate_lint.is_empty(), "{:?}", diff.candidate_lint);
}

// ---------------------------------------------------------------------------
// deep nesting: changes inside composites
// ---------------------------------------------------------------------------

#[test]
fn params_change_inside_loop_body_detected() {
    let old = seq(json!([
        {"type": "loop", "id": "l", "condition": "n < 3", "body": [
            {"type": "step", "id": "poll", "handler": "transform", "params": {"v": 1}}
        ]}
    ]));
    let new = seq(json!([
        {"type": "loop", "id": "l", "condition": "n < 3", "body": [
            {"type": "step", "id": "poll", "handler": "transform", "params": {"v": 2}}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").block_id.as_deref(),
        Some("poll")
    );
}

#[test]
fn block_added_inside_loop_body_detected() {
    let old = seq(json!([
        {"type": "loop", "id": "l", "condition": "n < 3", "body": [step("poll", "noop")]}
    ]));
    let new = seq(json!([
        {"type": "loop", "id": "l", "condition": "n < 3", "body": [
            step("poll", "noop"),
            step("charge", "http_request")
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "block_added");
    assert_eq!(e.block_id.as_deref(), Some("charge"));
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
}

#[test]
fn handler_change_inside_try_catch_catch_arm_detected() {
    let old = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("work", "noop")],
         "catch_block": [step("recover", "log")]}
    ]));
    let new = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("work", "noop")],
         "catch_block": [step("recover", "send_signal")]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "handler_changed");
    assert_eq!(e.block_id.as_deref(), Some("recover"));
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
}

#[test]
fn block_removed_from_try_catch_finally_detected() {
    let old = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("work", "noop")],
         "catch_block": [step("recover", "log")],
         "finally_block": [step("cleanup", "noop")]}
    ]));
    let new = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("work", "noop")],
         "catch_block": [step("recover", "log")]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "block_removed").block_id.as_deref(),
        Some("cleanup")
    );
}

#[test]
fn params_change_inside_for_each_body_detected() {
    let old = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "{{ data.items }}", "body": [
            {"type": "step", "id": "each", "handler": "transform", "params": {"mode": "fast"}}
        ]}
    ]));
    let new = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "{{ data.items }}", "body": [
            {"type": "step", "id": "each", "handler": "transform", "params": {"mode": "slow"}}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "params_changed").block_id.as_deref(),
        Some("each")
    );
}

#[test]
fn queue_change_inside_cancellation_scope_detected() {
    let old = seq(json!([
        {"type": "cancellation_scope", "id": "cs", "blocks": [
            {"type": "step", "id": "final", "handler": "noop", "params": {},
             "queue_name": "critical"}
        ]}
    ]));
    let new = seq(json!([
        {"type": "cancellation_scope", "id": "cs", "blocks": [
            {"type": "step", "id": "final", "handler": "noop", "params": {},
             "queue_name": "bulk"}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert_eq!(
        entry(&diff, "queue_changed").block_id.as_deref(),
        Some("final")
    );
}

#[test]
fn block_added_inside_race_branch_detected() {
    let old = seq(json!([
        {"type": "race", "id": "rc", "branches": [
            [step("fast", "noop")],
            [step("slow", "noop")]
        ]}
    ]));
    let new = seq(json!([
        {"type": "race", "id": "rc", "branches": [
            [step("fast", "noop")],
            [step("slow", "noop"), step("extra", "emit_event")]
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let e = entry(&diff, "block_added");
    assert_eq!(e.block_id.as_deref(), Some("extra"));
    assert_eq!(e.severity, DiffSeverity::SideEffectRisk);
}

#[test]
fn changes_at_multiple_nesting_levels_all_reported() {
    let old = seq(json!([
        step("top", "noop"),
        {"type": "router", "id": "r", "routes": [
            {"condition": "x == 1", "blocks": [
                {"type": "loop", "id": "l", "condition": "n < 2", "body": [
                    {"type": "step", "id": "inner", "handler": "transform", "params": {"v": 1}}
                ]}
            ]}
        ]}
    ]));
    let new = seq(json!([
        {"type": "step", "id": "top", "handler": "noop", "params": {}, "timeout": 9000},
        {"type": "router", "id": "r", "routes": [
            {"condition": "x == 2", "blocks": [
                {"type": "loop", "id": "l", "condition": "n < 2", "body": [
                    {"type": "step", "id": "inner", "handler": "transform", "params": {"v": 2}}
                ]}
            ]}
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    let cats = categories(&diff);
    assert!(cats.contains(&"timeout_changed"), "{cats:?}");
    assert!(cats.contains(&"router_changed"), "{cats:?}");
    assert!(cats.contains(&"params_changed"), "{cats:?}");
}

#[test]
fn sub_sequence_nested_in_parallel_retarget_detected() {
    let old = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [{"type": "sub_sequence", "id": "child", "sequence_name": "billing",
              "version": 1, "input": {}}]
        ]}
    ]));
    let new = seq(json!([
        {"type": "parallel", "id": "p", "branches": [
            [{"type": "sub_sequence", "id": "child", "sequence_name": "billing",
              "version": 5, "input": {}}]
        ]}
    ]));
    let diff = semantic_diff(&old, &new);
    assert!(
        categories(&diff).contains(&"sub_sequence_changed"),
        "{:#?}",
        diff.entries
    );
}
