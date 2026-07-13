//! End-to-end tests for the contract runner (`orch8::contract`): every case
//! spins up a real embedded engine (in-memory sqlite, manual clock) and runs
//! a `ContractSuite` against a sequence definition through the public API.
//!
//! Covered here, beyond the module's inline tests: all composite block
//! types (parallel / router / `try_catch` / `for_each` / loop), retry scripting
//! via `attempts` mocks, recorded outputs, virtual-time budgets, path and
//! call-count divergences, multi-case aggregation, and signal fixtures for
//! `wait_for_input` gates.

use std::collections::BTreeMap;

use orch8::SequenceDefinition;
use orch8::contract::{RunOptions, run_case, run_suite};
use orch8_types::contract::{
    CaseReport, ContractCase, ContractSuite, SuiteReport, UnmockedHandlerPolicy,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(clippy::needless_pass_by_value)]
fn seq(blocks: Value) -> SequenceDefinition {
    serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "contract-e2e-seq",
        "version": 1,
        "blocks": blocks,
        "created_at": "2026-01-01T00:00:00Z"
    }))
    .expect("valid sequence definition")
}

#[allow(clippy::needless_pass_by_value)]
fn suite(cases: Value) -> ContractSuite {
    serde_json::from_value(json!({"cases": cases})).expect("valid suite")
}

fn case(v: Value) -> ContractCase {
    serde_json::from_value(v).expect("valid case")
}

async fn run(seq_def: &SequenceDefinition, s: &ContractSuite) -> SuiteReport {
    run_suite(seq_def, s, &RunOptions::default())
        .await
        .expect("runner infrastructure must not error")
}

/// Run a one-case suite and return that case's report.
async fn run_one(seq_def: &SequenceDefinition, case_json: Value) -> CaseReport {
    let s = suite(json!([case_json]));
    let mut report = run(seq_def, &s).await;
    report.cases.remove(0)
}

/// Like [`run_one`] but with the virtual-time epoch anchored to the wall
/// clock. Instance creation stamps `next_fire_at` with the *wall* clock (not
/// the injected manual clock), so cases that assert absolute logical
/// durations must start virtual time near "now" to avoid a huge initial
/// virtual jump (see `creation_schedule_stamp_leaks_wall_clock...` below).
async fn run_one_anchored_to_wall_clock(
    seq_def: &SequenceDefinition,
    case_json: Value,
) -> CaseReport {
    let s = suite(json!([case_json]));
    let opts = RunOptions {
        start_time: chrono::Utc::now(),
        ..RunOptions::default()
    };
    let mut report = run_suite(seq_def, &s, &opts)
        .await
        .expect("runner infrastructure must not error");
    report.cases.remove(0)
}

fn assert_case_passed(report: &CaseReport) {
    assert!(
        report.passed,
        "case '{}' failed: {:?} (final_state={})",
        report.name, report.failures, report.final_state
    );
}

fn assert_failure_containing(report: &CaseReport, needle: &str) {
    assert!(!report.passed, "case '{}' unexpectedly passed", report.name);
    assert!(
        report.failures.iter().any(|f| f.contains(needle)),
        "no failure containing '{needle}' in {:?}",
        report.failures
    );
}

fn step(id: &str, handler: &str) -> Value {
    json!({"type": "step", "id": id, "handler": handler, "params": {}})
}

#[allow(clippy::needless_pass_by_value)]
fn success_mock(handler: &str, output: Value) -> Value {
    json!({"handler": handler, "type": "success", "output": output})
}

// ---------------------------------------------------------------------------
// Basic step happy paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_step_completes_with_output() {
    let seq_def = seq(json!([step("only", "worker")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "single",
            "mocks": [success_mock("worker", json!({"result": "done", "n": 3}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "only", "path": "result"}, "op": "equals", "value": "done"},
                    {"output": {"block": "only", "path": "n"}, "op": "in_range", "min": 3, "max": 3},
                    {"output": {"block": "only", "path": ""}, "op": "has_type", "expected": "object"}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(report.final_state, "completed");
}

#[tokio::test]
async fn three_steps_execute_in_order() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h"), step("c", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "ordering",
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["a", "b", "c"], "ordered": true}
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(report.executed_blocks, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn context_input_passes_through_to_final_context() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "input passthrough",
            "input": {"user": {"id": 7, "tags": ["vip"]}, "amount": 12.5},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"context": {"path": "user.id"}, "op": "equals", "value": 7},
                    {"context": {"path": "user.tags"}, "op": "contains", "value": "vip"},
                    {"context": {"path": "amount"}, "op": "in_range", "min": 12.0, "max": 13.0},
                    {"context": {"path": "missing_key"}, "op": "not_exists"}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn handler_calls_are_recorded_per_handler() {
    let seq_def = seq(json!([
        step("a", "alpha"),
        step("b", "beta"),
        step("c", "alpha")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "call accounting",
            "mocks": [
                success_mock("alpha", json!({})),
                success_mock("beta", json!({}))
            ],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    let mut expected = BTreeMap::new();
    expected.insert("alpha".to_string(), 2u32);
    expected.insert("beta".to_string(), 1u32);
    assert_eq!(report.handler_calls, expected);
}

/// Regression test: the runner used to inherit `create_instance`'s
/// wall-clock `next_fire_at` stamp, so a delay-free case under the fixed
/// 2026-01-01 epoch reported months of phantom logical time. The runner
/// now pins the first fire to the virtual epoch — a delay-free case
/// reports zero logical duration and is fully deterministic.
#[tokio::test]
async fn delay_free_case_reports_zero_logical_duration() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "wall clock leak",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(
        report.logical_duration_ms, 0,
        "delay-free cases must not accrue phantom logical time"
    );
    assert!(report.ticks > 0);
}

/// Anchoring the virtual epoch at the wall clock sidesteps the creation
/// stamp leak: a delay-free case then reports only the ~1s idle-jump the
/// runner performs to reach the creation stamp.
#[tokio::test]
async fn wall_anchored_epoch_keeps_delay_free_duration_small() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h")]));
    let report = run_one_anchored_to_wall_clock(
        &seq_def,
        json!({
            "name": "near zero time",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(
        report.logical_duration_ms < 60_000,
        "expected < 1 minute of logical time, got {} ms",
        report.logical_duration_ms
    );
}

#[tokio::test]
async fn run_case_directly_returns_case_report() {
    let seq_def = seq(json!([step("s", "h")]));
    let c = case(json!({
        "name": "direct",
        "mocks": [success_mock("h", json!({"ok": true}))],
        "expect": {"terminal_state": "completed"}
    }));
    let report = run_case(
        &seq_def,
        UnmockedHandlerPolicy::Fail,
        &c,
        &RunOptions::default(),
    )
    .await
    .expect("run_case infra");
    assert_case_passed(&report);
    assert_eq!(report.name, "direct");
    assert_eq!(report.executed_blocks, vec!["s"]);
}

#[tokio::test]
async fn suite_report_carries_sequence_identity() {
    let seq_def = seq(json!([step("s", "h")]));
    let s = suite(json!([{
        "name": "id",
        "mocks": [success_mock("h", json!({}))],
        "expect": {}
    }]));
    let report = run(&seq_def, &s).await;
    assert_eq!(report.sequence_name, "contract-e2e-seq");
    assert_eq!(report.sequence_version, 1);
}

// ---------------------------------------------------------------------------
// Parallel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parallel_branches_all_execute() {
    let seq_def = seq(json!([
        {"type": "parallel", "id": "par", "branches": [
            [step("left", "lh")],
            [step("right", "rh")]
        ]},
        step("after", "ah")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "fan out",
            "mocks": [
                success_mock("lh", json!({"side": "l"})),
                success_mock("rh", json!({"side": "r"})),
                success_mock("ah", json!({}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["left", "right", "after"], "ordered": false}
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn parallel_branch_outputs_are_all_recorded() {
    let seq_def = seq(json!([
        {"type": "parallel", "id": "par", "branches": [
            [step("b1", "h")],
            [step("b2", "h")],
            [step("b3", "h")]
        ]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "branch outputs",
            "mocks": [success_mock("h", json!({"done": true}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "b1", "path": "done"}, "op": "equals", "value": true},
                    {"output": {"block": "b2", "path": "done"}, "op": "equals", "value": true},
                    {"output": {"block": "b3", "path": "done"}, "op": "equals", "value": true}
                ],
                "call_counts": [{"handler": "h", "min": 3, "max": 3}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn parallel_completes_after_ordered_follow_up() {
    let seq_def = seq(json!([
        step("before", "h"),
        {"type": "parallel", "id": "par", "branches": [
            [step("p1", "h")],
            [step("p2", "h")]
        ]},
        step("join", "h")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "before and after parallel",
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["before", "join"], "ordered": true},
                "call_counts": [{"handler": "h", "min": 4, "max": 4}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

fn two_route_router() -> SequenceDefinition {
    seq(json!([
        {"type": "router", "id": "route", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("route_a", "h")]},
            {"condition": "kind == \"b\"", "blocks": [step("route_b", "h")]}
        ], "default": [step("route_default", "h")]}
    ]))
}

#[tokio::test]
async fn router_takes_second_matching_route() {
    let report = run_one(
        &two_route_router(),
        json!({
            "name": "route b",
            "input": {"kind": "b"},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["route_b"]},
                "skipped_blocks": ["route_a", "route_default"]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn router_falls_back_to_default_branch() {
    let report = run_one(
        &two_route_router(),
        json!({
            "name": "no match",
            "input": {"kind": "zzz"},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["route_default"]},
                "skipped_blocks": ["route_a", "route_b"]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn router_numeric_condition_selects_branch() {
    let seq_def = seq(json!([
        {"type": "router", "id": "amount_router", "routes": [
            {"condition": "amount > 100", "blocks": [step("big", "h")]},
            {"condition": "amount <= 100", "blocks": [step("small", "h")]}
        ]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "boundary goes small",
            "input": {"amount": 100},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["small"]},
                "skipped_blocks": ["big"]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn router_condition_can_read_case_config() {
    let seq_def = seq(json!([
        {"type": "router", "id": "mode_router", "routes": [
            {"condition": "config.mode == \"live\"", "blocks": [step("live_path", "h")]}
        ], "default": [step("test_path", "h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "config routed",
            "config": {"mode": "live"},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["live_path"]},
                "skipped_blocks": ["test_path"]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn router_then_shared_tail_step() {
    let seq_def = seq(json!([
        {"type": "router", "id": "r", "routes": [
            {"condition": "kind == \"a\"", "blocks": [step("route_a", "h")]}
        ], "default": [step("route_default", "h")]},
        step("tail", "h")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "tail always runs",
            "input": {"kind": "a"},
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["route_a", "tail"], "ordered": true},
                "skipped_blocks": ["route_default"]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// TryCatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn try_catch_failure_runs_catch_and_completes() {
    let seq_def = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("risky", "risky_h")],
         "catch_block": [step("recover", "recover_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "catch path",
            "mocks": [
                {"handler": "risky_h", "type": "failure", "message": "kaboom"},
                success_mock("recover_h", json!({"recovered": true}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["recover"]},
                "assertions": [
                    {"output": {"block": "recover", "path": "recovered"}, "op": "equals", "value": true}
                ],
                "call_counts": [
                    {"handler": "risky_h", "min": 1, "max": 1},
                    {"handler": "recover_h", "min": 1, "max": 1}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn try_catch_success_skips_catch() {
    let seq_def = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("risky", "risky_h")],
         "catch_block": [step("recover", "recover_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "no catch",
            "mocks": [
                success_mock("risky_h", json!({"fine": 1})),
                success_mock("recover_h", json!({}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["risky"]},
                "skipped_blocks": ["recover"],
                "call_counts": [{"handler": "recover_h", "min": 0, "max": 0}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn try_catch_finally_runs_after_success() {
    let seq_def = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("risky", "risky_h")],
         "catch_block": [step("recover", "recover_h")],
         "finally_block": [step("cleanup", "cleanup_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "finally on success",
            "mocks": [
                success_mock("risky_h", json!({})),
                success_mock("recover_h", json!({})),
                success_mock("cleanup_h", json!({"cleaned": true}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["risky", "cleanup"], "ordered": true},
                "skipped_blocks": ["recover"],
                "call_counts": [{"handler": "cleanup_h", "min": 1, "max": 1}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn try_catch_finally_runs_after_catch() {
    let seq_def = seq(json!([
        {"type": "try_catch", "id": "tc",
         "try_block": [step("risky", "risky_h")],
         "catch_block": [step("recover", "recover_h")],
         "finally_block": [step("cleanup", "cleanup_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "finally on failure",
            "mocks": [
                {"handler": "risky_h", "type": "failure", "message": "nope"},
                success_mock("recover_h", json!({})),
                success_mock("cleanup_h", json!({}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["recover", "cleanup"], "ordered": true},
                "call_counts": [{"handler": "cleanup_h", "min": 1, "max": 1}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// ForEach
// ---------------------------------------------------------------------------

#[tokio::test]
async fn for_each_runs_body_once_per_item() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items",
         "body": [step("process", "proc")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "three items",
            "input": {"items": ["x", "y", "z"]},
            "mocks": [success_mock("proc", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["process"]},
                "call_counts": [{"handler": "proc", "min": 3, "max": 3}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn for_each_marker_output_records_progress() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items",
         "body": [step("process", "proc")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "marker",
            "input": {"items": [10, 20]},
            "mocks": [success_mock("proc", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "fe", "path": "_index"}, "op": "equals", "value": 2},
                    {"output": {"block": "fe", "path": "_total"}, "op": "equals", "value": 2}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn for_each_empty_collection_completes_without_calls() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items",
         "body": [step("process", "proc")]},
        step("after", "after_h")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "empty collection",
            "input": {"items": []},
            "mocks": [
                success_mock("proc", json!({})),
                success_mock("after_h", json!({}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["after"]},
                "skipped_blocks": ["process"],
                "call_counts": [{"handler": "proc", "min": 0, "max": 0}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn for_each_missing_collection_completes_and_skips_body() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "nonexistent",
         "body": [step("process", "proc")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "missing collection",
            "input": {"unrelated": 1},
            "mocks": [success_mock("proc", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "skipped_blocks": ["process"],
                "call_counts": [{"handler": "proc", "min": 0, "max": 0}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn for_each_cleans_up_item_var_after_completion() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items", "item_var": "current",
         "body": [step("process", "proc")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "item var cleanup",
            "input": {"items": [1, 2]},
            "mocks": [success_mock("proc", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"context": {"path": "current"}, "op": "not_exists"},
                    {"context": {"path": "items"}, "op": "equals", "value": [1, 2]}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// Loop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn loop_with_false_condition_never_runs_body() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "false",
         "body": [step("body_step", "body_h")]},
        step("after", "after_h")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "dead loop",
            "mocks": [
                success_mock("body_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["after"]},
                "skipped_blocks": ["body_step"],
                "call_counts": [{"handler": "body_h", "min": 0, "max": 0}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn loop_runs_until_iteration_cap() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "true", "max_iterations": 3,
         "body": [step("body_step", "body_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "capped loop",
            "mocks": [success_mock("body_h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "call_counts": [{"handler": "body_h", "min": 3, "max": 3}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn loop_marker_output_counts_iterations() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "true", "max_iterations": 2,
         "body": [step("body_step", "body_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "loop marker",
            "mocks": [success_mock("body_h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "lp", "path": "_iterations"}, "op": "equals", "value": 2}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn loop_condition_reads_input_data() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "keep_going", "max_iterations": 2,
         "body": [step("body_step", "body_h")]}
    ]));
    // Truthy input flag: loop runs until the cap (condition never flips).
    let report = run_one(
        &seq_def,
        json!({
            "name": "truthy flag",
            "input": {"keep_going": true},
            "mocks": [success_mock("body_h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "call_counts": [{"handler": "body_h", "min": 2, "max": 2}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);

    // Falsy input flag: body never runs.
    let report = run_one(
        &seq_def,
        json!({
            "name": "falsy flag",
            "input": {"keep_going": false},
            "mocks": [success_mock("body_h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "call_counts": [{"handler": "body_h", "min": 0, "max": 0}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn loop_body_failure_fails_the_instance() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "true", "max_iterations": 5,
         "body": [step("body_step", "body_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "loop body fails",
            "mocks": [{"handler": "body_h", "type": "failure", "message": "iteration broke"}],
            "expect": {
                "terminal_state": "failed",
                "call_counts": [{"handler": "body_h", "min": 1, "max": 1}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(report.final_state, "failed");
}

#[tokio::test]
async fn loop_continue_on_error_reaches_cap_despite_failures() {
    let seq_def = seq(json!([
        {"type": "loop", "id": "lp", "condition": "true", "max_iterations": 2,
         "continue_on_error": true,
         "body": [step("body_step", "body_h")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "tolerant loop",
            "mocks": [{"handler": "body_h", "type": "failure", "message": "always"}],
            "expect": {
                "terminal_state": "completed",
                "call_counts": [{"handler": "body_h", "min": 2, "max": 2}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// Retry via attempts mocks
// ---------------------------------------------------------------------------

fn retrying_step(max_attempts: u32) -> SequenceDefinition {
    seq(json!([
        {"type": "step", "id": "flaky", "handler": "flaky_h", "params": {},
         "retry": {"max_attempts": max_attempts,
                   "initial_backoff": 1000, "max_backoff": 60_000}}
    ]))
}

#[tokio::test]
async fn retry_succeeds_after_one_failure() {
    let report = run_one(
        &retrying_step(3),
        json!({
            "name": "second attempt wins",
            "mocks": [{"handler": "flaky_h", "type": "attempts", "attempts": [
                {"kind": "failure", "message": "transient", "retryable": true},
                {"kind": "success", "output": {"attempt": 2}}
            ]}],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "flaky", "path": "attempt"}, "op": "equals", "value": 2}
                ],
                "call_counts": [{"handler": "flaky_h", "min": 2, "max": 2}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn retry_exhaustion_fails_the_instance() {
    // `max_attempts: 2` counts RETRIES, not total invocations: the handler
    // runs once, then retries twice, and only fails the instance once the
    // attempt counter reaches the cap — 3 calls in total.
    let report = run_one(
        &retrying_step(2),
        json!({
            "name": "budget exhausted",
            "mocks": [{"handler": "flaky_h", "type": "attempts", "attempts": [
                {"kind": "failure", "message": "still broken", "retryable": true}
            ]}],
            "expect": {
                "terminal_state": "failed",
                "call_counts": [{"handler": "flaky_h", "min": 3, "max": 3}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(report.final_state, "failed");
}

#[tokio::test]
async fn permanent_failure_bypasses_remaining_retry_budget() {
    let report = run_one(
        &retrying_step(5),
        json!({
            "name": "permanent short circuit",
            "mocks": [{"handler": "flaky_h", "type": "failure",
                       "message": "bad request", "retryable": false}],
            "expect": {
                "terminal_state": "failed",
                "call_counts": [{"handler": "flaky_h", "min": 1, "max": 1}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn retry_backoff_advances_virtual_time_only() {
    let started = std::time::Instant::now();
    let report = run_one(
        &retrying_step(3),
        json!({
            "name": "backoff on the manual clock",
            "mocks": [{"handler": "flaky_h", "type": "attempts", "attempts": [
                {"kind": "failure", "message": "one", "retryable": true},
                {"kind": "failure", "message": "two", "retryable": true},
                {"kind": "success", "output": {}}
            ]}],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    // Two backoffs (1s then ~2s) must appear as logical time...
    assert!(
        report.logical_duration_ms >= 1000,
        "logical_duration_ms = {}",
        report.logical_duration_ms
    );
    // ...without sleeping on the wall clock.
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
}

#[tokio::test]
async fn attempts_are_tracked_per_block_not_per_handler() {
    // Two blocks share one handler with an `attempts` script: each block
    // starts at attempt index 0, so both observe the FIRST outcome.
    let seq_def = seq(json!([step("first", "shared"), step("second", "shared")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "per block attempt cursor",
            "mocks": [{"handler": "shared", "type": "attempts", "attempts": [
                {"kind": "success", "output": {"n": 1}},
                {"kind": "success", "output": {"n": 2}}
            ]}],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "first", "path": "n"}, "op": "equals", "value": 1},
                    {"output": {"block": "second", "path": "n"}, "op": "equals", "value": 1}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

// ---------------------------------------------------------------------------
// Mock precedence & unmocked policies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn block_failure_mock_overrides_handler_success_mock() {
    let seq_def = seq(json!([step("a", "shared"), step("b", "shared")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "block precedence with failure",
            "mocks": [
                success_mock("shared", json!({"from": "handler"})),
                {"block": "b", "type": "failure", "message": "block-level boom"}
            ],
            "expect": {
                "terminal_state": "failed",
                "assertions": [
                    {"output": {"block": "a", "path": "from"}, "op": "equals", "value": "handler"}
                ],
                "call_counts": [{"handler": "shared", "min": 2, "max": 2}]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn handler_mock_covers_every_block_using_it() {
    let seq_def = seq(json!([
        step("x", "common"),
        step("y", "common"),
        step("z", "common")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "one mock many blocks",
            "mocks": [success_mock("common", json!({"same": true}))],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"output": {"block": "x", "path": "same"}, "op": "equals", "value": true},
                    {"output": {"block": "y", "path": "same"}, "op": "equals", "value": true},
                    {"output": {"block": "z", "path": "same"}, "op": "equals", "value": true}
                ]
            }
        }),
    )
    .await;
    assert_case_passed(&report);
}

#[tokio::test]
async fn partial_mocks_fail_only_the_unmocked_handler() {
    let seq_def = seq(json!([
        step("first", "mocked_h"),
        step("second", "forgotten_h")
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "partially mocked",
            "mocks": [success_mock("mocked_h", json!({"ok": 1}))],
            "expect": {}
        }),
    )
    .await;
    assert_failure_containing(&report, "'forgotten_h'");
    assert_failure_containing(&report, "no mock");
    // The mocked step still ran and produced output before the failure.
    assert!(report.executed_blocks.contains(&"first".to_string()));
    assert_eq!(report.final_state, "failed");
}

#[tokio::test]
async fn unmocked_empty_success_policy_returns_empty_object() {
    let seq_def = seq(json!([step("s", "anything")]));
    let mut s = suite(json!([{
        "name": "tolerant empty output",
        "expect": {
            "terminal_state": "completed",
            "assertions": [
                {"output": {"block": "s", "path": ""}, "op": "equals", "value": {}},
                {"output": {"block": "s", "path": ""}, "op": "has_type", "expected": "object"}
            ]
        }
    }]));
    s.unmocked_handlers = UnmockedHandlerPolicy::EmptySuccess;
    let report = run(&seq_def, &s).await;
    assert_case_passed(&report.cases[0]);
}

#[tokio::test]
async fn empty_success_policy_still_honors_explicit_mocks() {
    let seq_def = seq(json!([step("mocked", "mh"), step("unmocked", "uh")]));
    let mut s = suite(json!([{
        "name": "mix explicit and fallback",
        "mocks": [success_mock("mh", json!({"explicit": true}))],
        "expect": {
            "terminal_state": "completed",
            "assertions": [
                {"output": {"block": "mocked", "path": "explicit"}, "op": "equals", "value": true},
                {"output": {"block": "unmocked", "path": ""}, "op": "equals", "value": {}}
            ]
        }
    }]));
    s.unmocked_handlers = UnmockedHandlerPolicy::EmptySuccess;
    let report = run(&seq_def, &s).await;
    assert_case_passed(&report.cases[0]);
}

// ---------------------------------------------------------------------------
// Recorded outputs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn recorded_outputs_are_keyed_by_block_not_handler() {
    // Two blocks share one recorded handler; each block replays its own
    // recording.
    let seq_def = seq(json!([
        step("fetch_users", "fetcher"),
        step("fetch_orders", "fetcher")
    ]));
    let s = suite(json!([{
        "name": "per block recordings",
        "mocks": [{"handler": "fetcher", "type": "recorded"}],
        "expect": {
            "terminal_state": "completed",
            "assertions": [
                {"output": {"block": "fetch_users", "path": "kind"}, "op": "equals", "value": "users"},
                {"output": {"block": "fetch_orders", "path": "kind"}, "op": "equals", "value": "orders"}
            ]
        }
    }]));
    let mut opts = RunOptions::default();
    opts.recorded_outputs
        .insert("fetch_users".to_string(), json!({"kind": "users"}));
    opts.recorded_outputs
        .insert("fetch_orders".to_string(), json!({"kind": "orders"}));
    let report = run_suite(&seq_def, &s, &opts).await.unwrap();
    assert_case_passed(&report.cases[0]);
}

#[tokio::test]
async fn recorded_mock_with_partial_recordings_fails_missing_block() {
    let seq_def = seq(json!([step("have", "fetcher"), step("missing", "fetcher")]));
    let s = suite(json!([{
        "name": "one recording missing",
        "mocks": [{"handler": "fetcher", "type": "recorded"}],
        "expect": {}
    }]));
    let mut opts = RunOptions::default();
    opts.recorded_outputs
        .insert("have".to_string(), json!({"cached": 1}));
    let report = run_suite(&seq_def, &s, &opts).await.unwrap();
    let c = &report.cases[0];
    assert_failure_containing(c, "no recorded output");
    assert_failure_containing(c, "'missing'");
    assert_eq!(c.final_state, "failed");
}

#[tokio::test]
async fn recorded_and_success_mocks_mix_in_one_case() {
    let seq_def = seq(json!([
        step("live", "live_h"),
        step("replayed", "replay_h")
    ]));
    let s = suite(json!([{
        "name": "mixed mock styles",
        "mocks": [
            success_mock("live_h", json!({"src": "mock"})),
            {"handler": "replay_h", "type": "recorded"}
        ],
        "expect": {
            "terminal_state": "completed",
            "assertions": [
                {"output": {"block": "live", "path": "src"}, "op": "equals", "value": "mock"},
                {"output": {"block": "replayed", "path": "src"}, "op": "equals", "value": "tape"}
            ]
        }
    }]));
    let mut opts = RunOptions::default();
    opts.recorded_outputs
        .insert("replayed".to_string(), json!({"src": "tape"}));
    let report = run_suite(&seq_def, &s, &opts).await.unwrap();
    assert_case_passed(&report.cases[0]);
}

// ---------------------------------------------------------------------------
// Virtual time & budgets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn five_minute_delay_advances_virtual_time() {
    let seq_def = seq(json!([
        step("now", "h"),
        {"type": "step", "id": "later", "handler": "h", "params": {},
         "delay": {"duration": 300_000}} // 5 minutes
    ]));
    let started = std::time::Instant::now();
    let report = run_one(
        &seq_def,
        json!({
            "name": "five minutes",
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["now", "later"], "ordered": true}
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(report.logical_duration_ms >= 300_000);
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
}

#[tokio::test]
async fn twelve_hour_delay_advances_virtual_time() {
    let seq_def = seq(json!([
        {"type": "step", "id": "overnight", "handler": "h", "params": {},
         "delay": {"duration": 43_200_000}} // 12 hours
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "overnight",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(report.logical_duration_ms >= 43_200_000);
}

#[tokio::test]
async fn seven_day_delay_completes_in_fast_wall_time() {
    let seq_def = seq(json!([
        {"type": "step", "id": "week_later", "handler": "h", "params": {},
         "delay": {"duration": 604_800_000}} // 7 days
    ]));
    let started = std::time::Instant::now();
    let report = run_one(
        &seq_def,
        json!({
            "name": "one week",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(report.logical_duration_ms >= 604_800_000);
    assert!(started.elapsed() < std::time::Duration::from_secs(60));
}

#[tokio::test]
async fn consecutive_delays_accumulate_logical_time() {
    let seq_def = seq(json!([
        {"type": "step", "id": "d1", "handler": "h", "params": {},
         "delay": {"duration": 3_600_000}}, // 1 hour
        {"type": "step", "id": "d2", "handler": "h", "params": {},
         "delay": {"duration": 7_200_000}} // 2 hours
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "delays add up",
            "mocks": [success_mock("h", json!({}))],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["d1", "d2"], "ordered": true}
            }
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(
        report.logical_duration_ms >= 10_800_000,
        "expected >= 3h of logical time, got {} ms",
        report.logical_duration_ms
    );
}

#[tokio::test]
async fn max_logical_duration_budget_passes_when_within_limit() {
    let seq_def = seq(json!([
        {"type": "step", "id": "later", "handler": "h", "params": {},
         "delay": {"duration": 3_600_000}} // 1 hour
    ]));
    // Wall-anchored epoch: with the default 2026-01-01 epoch, the creation
    // stamp leak (see above) alone blows any sub-months budget.
    let report = run_one_anchored_to_wall_clock(
        &seq_def,
        json!({
            "name": "within budget",
            "max_logical_duration_ms": 7_200_000u64, // 2 hour budget
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(report.logical_duration_ms >= 3_600_000);
    assert!(report.logical_duration_ms <= 7_200_000);
}

#[tokio::test]
async fn max_logical_duration_exceeded_names_the_limit() {
    let seq_def = seq(json!([
        {"type": "step", "id": "later", "handler": "h", "params": {},
         "delay": {"duration": 172_800_000}} // 2 days
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "over budget",
            "max_logical_duration_ms": 60_000u64, // 1 minute budget
            "mocks": [success_mock("h", json!({}))],
            "expect": {}
        }),
    )
    .await;
    assert_failure_containing(&report, "logical duration exceeded");
    assert_failure_containing(&report, "60000 ms");
}

#[tokio::test]
async fn max_ticks_exhaustion_reports_stuck_instance() {
    // A delayed second step needs an idle clock-jump tick plus another
    // execution tick, so a budget of one tick can never finish the case
    // (a delay-free flat sequence now completes within a single tick).
    let seq_def = seq(json!([
        step("a", "h"),
        {"type": "step", "id": "b", "handler": "h", "params": {},
         "delay": {"duration": 3_600_000}}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "one tick only",
            "max_ticks": 1,
            "mocks": [success_mock("h", json!({}))],
            "expect": {}
        }),
    )
    .await;
    assert_failure_containing(&report, "did not reach a terminal state");
    assert_eq!(report.ticks, 1);
    assert_ne!(report.final_state, "completed");
}

#[tokio::test]
async fn generous_max_ticks_allows_completion() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "plenty of ticks",
            "max_ticks": 500,
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert!(report.ticks <= 500);
}

// ---------------------------------------------------------------------------
// Expectation divergences
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wrong_terminal_state_divergence_names_both_states() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "expected failure got success",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "failed"}
        }),
    )
    .await;
    assert_failure_containing(&report, "expected terminal state 'failed'");
    assert_failure_containing(&report, "'completed'");
}

#[tokio::test]
async fn expected_completed_but_failed_is_a_divergence() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "expected success got failure",
            "mocks": [{"handler": "h", "type": "failure", "message": "oops"}],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert_failure_containing(&report, "expected terminal state 'completed'");
    assert_eq!(report.final_state, "failed");
}

#[tokio::test]
async fn ordered_path_in_wrong_order_is_reported() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "reversed expectation",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"path": {"traversed": ["b", "a"], "ordered": true}}
        }),
    )
    .await;
    assert_failure_containing(&report, "out of expected order");
}

#[tokio::test]
async fn unordered_path_missing_block_is_reported() {
    let seq_def = seq(json!([step("a", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "phantom block",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"path": {"traversed": ["a", "ghost"]}}
        }),
    )
    .await;
    assert_failure_containing(&report, "'ghost'");
    assert_failure_containing(&report, "did not");
}

#[tokio::test]
async fn skipped_block_that_executed_is_reported() {
    let seq_def = seq(json!([step("a", "h"), step("b", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "should have skipped",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"skipped_blocks": ["b"]}
        }),
    )
    .await;
    assert_failure_containing(&report, "'b' executed but was expected to be skipped");
}

#[tokio::test]
async fn output_assertion_divergence_is_prefixed_with_subject() {
    let seq_def = seq(json!([step("charge", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "wrong amount",
            "mocks": [success_mock("h", json!({"amount": 6}))],
            "expect": {"assertions": [
                {"output": {"block": "charge", "path": "amount"}, "op": "equals", "value": 5}
            ]}
        }),
    )
    .await;
    assert_failure_containing(&report, "output.charge.amount:");
    assert_failure_containing(&report, "expected 5, got 6");
}

#[tokio::test]
async fn context_assertion_divergence_is_prefixed_with_path() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "context mismatch",
            "input": {"status": "new"},
            "mocks": [success_mock("h", json!({}))],
            "expect": {"assertions": [
                {"context": {"path": "status"}, "op": "equals", "value": "done"}
            ]}
        }),
    )
    .await;
    assert_failure_containing(&report, "context.status:");
}

#[tokio::test]
async fn call_count_minimum_violation_is_reported() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "expected more calls",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"call_counts": [{"handler": "h", "min": 2}]}
        }),
    )
    .await;
    assert_failure_containing(&report, "called 1 times");
    assert_failure_containing(&report, "at least 2");
}

#[tokio::test]
async fn call_count_maximum_violation_is_reported() {
    let seq_def = seq(json!([
        {"type": "for_each", "id": "fe", "collection": "items",
         "body": [step("process", "proc")]}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "called too often",
            "input": {"items": [1, 2, 3]},
            "mocks": [success_mock("proc", json!({}))],
            "expect": {"call_counts": [{"handler": "proc", "min": 0, "max": 2}]}
        }),
    )
    .await;
    assert_failure_containing(&report, "called 3 times");
    assert_failure_containing(&report, "at most 2");
}

#[tokio::test]
async fn multiple_divergences_are_all_collected() {
    let seq_def = seq(json!([step("s", "h")]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "several problems",
            "mocks": [success_mock("h", json!({"v": 1}))],
            "expect": {
                "terminal_state": "failed",
                "path": {"traversed": ["ghost"]},
                "assertions": [
                    {"output": {"block": "s", "path": "v"}, "op": "equals", "value": 2}
                ]
            }
        }),
    )
    .await;
    assert!(!report.passed);
    assert!(
        report.failures.len() >= 3,
        "expected 3+ divergences, got {:?}",
        report.failures
    );
}

// ---------------------------------------------------------------------------
// Suite-level behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_case_suite_aggregates_pass_and_fail() {
    let seq_def = seq(json!([step("s", "h")]));
    let s = suite(json!([
        {
            "name": "passes",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "completed"}
        },
        {
            "name": "diverges",
            "mocks": [success_mock("h", json!({}))],
            "expect": {"terminal_state": "cancelled"}
        },
        {
            "name": "also passes",
            "mocks": [{"handler": "h", "type": "failure", "message": "x"}],
            "expect": {"terminal_state": "failed"}
        }
    ]));
    let report = run(&seq_def, &s).await;
    assert!(!report.passed);
    assert_eq!(report.cases.len(), 3);
    assert_eq!(
        report
            .cases
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["passes", "diverges", "also passes"]
    );
    assert!(report.cases[0].passed);
    assert!(!report.cases[1].passed);
    assert!(report.cases[2].passed);
    let failed = report.failed_cases();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].name, "diverges");
}

#[tokio::test]
async fn all_passing_suite_is_marked_passed() {
    let seq_def = seq(json!([step("s", "h")]));
    let s = suite(json!([
        {"name": "a", "mocks": [success_mock("h", json!({}))], "expect": {}},
        {"name": "b", "mocks": [success_mock("h", json!({}))], "expect": {}}
    ]));
    let report = run(&seq_def, &s).await;
    assert!(report.passed);
    assert!(report.failed_cases().is_empty());
}

#[tokio::test]
async fn duplicate_case_names_are_rejected_before_running() {
    let seq_def = seq(json!([step("s", "h")]));
    let s = suite(json!([
        {"name": "dup", "expect": {}},
        {"name": "dup", "expect": {}}
    ]));
    let err = run_suite(&seq_def, &s, &RunOptions::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("duplicate case name"), "{err}");
}

#[tokio::test]
async fn mock_without_target_is_rejected_before_running() {
    let seq_def = seq(json!([step("s", "h")]));
    let s = suite(json!([{
        "name": "bad mock",
        "mocks": [{"type": "success", "output": {}}],
        "expect": {}
    }]));
    let err = run_suite(&seq_def, &s, &RunOptions::default())
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("invalid contract suite"), "{msg}");
    assert!(msg.contains("either 'handler' or 'block'"), "{msg}");
}

// ---------------------------------------------------------------------------
// Signal fixtures (wait_for_input gates)
// ---------------------------------------------------------------------------

#[allow(clippy::needless_pass_by_value)]
fn gated_sequence(gate_extra: Value) -> SequenceDefinition {
    let mut gate = json!({
        "type": "step", "id": "gate", "handler": "gate_h", "params": {},
        "wait_for_input": {"prompt": "approve?"}
    });
    if let (Some(gate_obj), Some(extra)) = (gate.as_object_mut(), gate_extra.as_object()) {
        for (k, v) in extra {
            gate_obj["wait_for_input"][k] = v.clone();
        }
    }
    seq(json!([gate, step("after", "after_h")]))
}

/// Regression test: a valid `wait_for_input` signal fixture used to
/// stall the case in 'scheduled' — the wake stamps `next_fire_at` with
/// the wall clock (behind the jumped virtual clock) and the runner's
/// idle check treated the due-but-unclaimed instance as a dead end. The
/// runner now keeps ticking through due work, so the gate resolves and
/// the case completes.
#[tokio::test]
async fn human_gate_valid_signal_completes_the_case() {
    let report = run_one(
        &gated_sequence(json!({})),
        json!({
            "name": "approved",
            "mocks": [
                success_mock("gate_h", json!({"handled": true})),
                success_mock("after_h", json!({}))
            ],
            "signals": [
                {"signal_type": "custom:human_input:gate", "payload": {"value": "yes"}}
            ],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"context": {"path": "gate"}, "op": "equals", "value": "yes"}
                ],
                "call_counts": [{"handler": "gate_h", "min": 1, "max": 1}]
            }
        }),
    )
    .await;
    assert!(report.passed, "failures: {:?}", report.failures);
    assert_eq!(report.final_state, "completed");
}

/// Companion regression: once the gate resolves (here with "no"), both
/// handlers run and both blocks execute.
#[tokio::test]
async fn human_gate_resolution_runs_handlers() {
    let report = run_one(
        &gated_sequence(json!({})),
        json!({
            "name": "declined",
            "mocks": [
                success_mock("gate_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "signals": [
                {"signal_type": "custom:human_input:gate", "payload": {"value": "no"}}
            ],
            "expect": {"terminal_state": "completed"}
        }),
    )
    .await;
    assert!(report.passed, "failures: {:?}", report.failures);
    assert_eq!(report.handler_calls.get("gate_h"), Some(&1));
    assert_eq!(report.handler_calls.get("after_h"), Some(&1));
    assert_eq!(report.executed_blocks, vec!["gate", "after"]);
}

/// With custom choices and `store_as`, the resolved choice value lands
/// under the configured context key once the gate opens.
#[tokio::test]
async fn human_gate_store_as_merges_choice_into_context() {
    let seq_def = gated_sequence(json!({
        "store_as": "decision",
        "choices": [
            {"label": "Approve", "value": "approve"},
            {"label": "Reject", "value": "reject"}
        ]
    }));
    let report = run_one(
        &seq_def,
        json!({
            "name": "custom choices",
            "mocks": [
                success_mock("gate_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "signals": [
                {"signal_type": "custom:human_input:gate", "payload": {"value": "reject"}}
            ],
            "expect": {
                "terminal_state": "completed",
                "assertions": [
                    {"context": {"path": "decision"}, "op": "equals", "value": "reject"}
                ]
            }
        }),
    )
    .await;
    assert!(report.passed, "failures: {:?}", report.failures);
}

#[tokio::test]
async fn invalid_signal_type_fixture_fails_the_case() {
    let report = run_one(
        &gated_sequence(json!({})),
        json!({
            "name": "typo signal",
            "mocks": [
                success_mock("gate_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "signals": [{"signal_type": "bogus_signal", "payload": {}}],
            "expect": {}
        }),
    )
    .await;
    assert_failure_containing(&report, "invalid signal fixture");
    assert_failure_containing(&report, "bogus_signal");
}

#[tokio::test]
async fn rejected_signal_value_never_completes_the_gate() {
    // "maybe" is not one of the default yes/no choices: the engine rejects
    // the payload and keeps the gate unresolved, so the case reports a
    // stuck (non-completed) instance rather than completing.
    let report = run_one(
        &gated_sequence(json!({})),
        json!({
            "name": "invalid choice",
            "mocks": [
                success_mock("gate_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "signals": [
                {"signal_type": "custom:human_input:gate", "payload": {"value": "maybe"}}
            ],
            "expect": {}
        }),
    )
    .await;
    assert_failure_containing(&report, "did not reach a terminal state");
    assert_ne!(report.final_state, "completed");
}

#[tokio::test]
async fn cancel_signal_fixture_cancels_waiting_instance() {
    let report = run_one(
        &gated_sequence(json!({})),
        json!({
            "name": "cancelled at the gate",
            "mocks": [
                success_mock("gate_h", json!({})),
                success_mock("after_h", json!({}))
            ],
            "signals": [{"signal_type": "cancel", "payload": {}}],
            "expect": {"terminal_state": "cancelled"}
        }),
    )
    .await;
    assert_case_passed(&report);
    assert_eq!(report.final_state, "cancelled");
    // The gate never resolved, so the post-gate step must not have run.
    assert!(!report.executed_blocks.contains(&"after".to_string()));
}

/// Regression companion: with the tick-loop fix, both gates resolve in
/// fixture order — the first answer lands under the block id, the second
/// under its `store_as` key, and both handlers run exactly once.
#[tokio::test]
async fn both_signal_fixtures_resolve_sequential_gates() {
    let seq_def = seq(json!([
        {"type": "step", "id": "gate1", "handler": "g1", "params": {},
         "wait_for_input": {"prompt": "first?"}},
        {"type": "step", "id": "gate2", "handler": "g2", "params": {},
         "wait_for_input": {"prompt": "second?", "store_as": "second_answer"}}
    ]));
    let report = run_one(
        &seq_def,
        json!({
            "name": "double approval",
            "mocks": [
                success_mock("g1", json!({})),
                success_mock("g2", json!({}))
            ],
            "signals": [
                {"signal_type": "custom:human_input:gate1", "payload": {"value": "yes"}},
                {"signal_type": "custom:human_input:gate2", "payload": {"value": "no"}}
            ],
            "expect": {
                "terminal_state": "completed",
                "path": {"traversed": ["gate1", "gate2"], "ordered": true},
                "assertions": [
                    {"context": {"path": "gate1"}, "op": "equals", "value": "yes"},
                    {"context": {"path": "second_answer"}, "op": "equals", "value": "no"}
                ],
                "call_counts": [
                    {"handler": "g1", "min": 1, "max": 1},
                    {"handler": "g2", "min": 1, "max": 1}
                ]
            }
        }),
    )
    .await;
    assert!(report.passed, "failures: {:?}", report.failures);
}
