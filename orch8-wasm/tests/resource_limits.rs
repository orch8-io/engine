//! Exercise the browser-facing API: strict decode must still admit these
//! workflows, while execution must stop before control flow exhausts memory.
use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

fn skipped_loop() -> Value {
    json!({"type":"loop","id":"loop","condition":"true","max_iterations":100_000,
        "continue_on_error":true,
        "body":[{"type":"step","id":"skip","handler":"noop","when":"false"}]})
}

fn run(block: &Value) -> Value {
    serde_json::from_str(&run_dry_with_options(
        &json!({"blocks":[block]}).to_string(),
        r#"{"max_ticks":1}"#,
    ))
    .unwrap()
}

fn assert_bounded(result: &Value) {
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["status"], "operation_limit");
    assert_eq!(result["ticks"], 0);
    assert!(result["timeline"].as_array().unwrap().len() <= 10_000);
}

#[test]
fn skipped_steps_cannot_bypass_execution_limit() {
    assert_bounded(&run(&skipped_loop()));
}

#[test]
fn router_decisions_cannot_bypass_execution_limit() {
    let mut block = skipped_loop();
    block["body"] = json!([{"type":"router","id":"route","routes":[{
        "condition":"false","blocks":[{"type":"step","id":"unused","handler":"noop"}]
    }]}]);
    assert_bounded(&run(&block));
}

#[test]
fn try_catch_cannot_catch_execution_limit() {
    let result = run(&json!({"type":"try_catch","id":"protect",
        "try_block":[skipped_loop()],
        "catch_block":[{"type":"step","id":"catch","handler":"noop"}],
        "finally_block":[{"type":"step","id":"finally","handler":"noop"}]}));
    assert_bounded(&result);
    assert!(result["outputs"].get("catch").is_none());
    assert!(result["outputs"].get("finally").is_none());
}

#[test]
fn parallel_and_race_propagate_execution_limit() {
    for kind in ["parallel", "race"] {
        assert_bounded(&run(&json!({"type":kind,"id":"branches",
            "branches":[[skipped_loop()],[{"type":"step","id":"other","handler":"noop"}]]})));
    }
}

#[test]
fn normal_skip_does_not_consume_step_budget() {
    let result = run(
        &json!({"type":"loop","id":"short","condition":"true","max_iterations":3,
        "body":[{"type":"step","id":"skip","handler":"noop","when":"false"}]}),
    );
    assert_eq!(result["status"], "completed");
    assert_eq!(result["ticks"], 0);
    assert_eq!(result["outputs"]["short"]["iterations"], 3);
}

#[test]
fn large_tick_budget_still_has_a_hard_operation_ceiling() {
    let mut block = skipped_loop();
    block["body"][0].as_object_mut().unwrap().remove("when");
    let result: Value = serde_json::from_str(&run_dry_with_options(
        &json!({"blocks":[block]}).to_string(),
        r#"{"max_ticks":4294967295}"#,
    ))
    .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["status"], "operation_limit");
    assert!(result["ticks"].as_u64().unwrap() > 1_000);
    assert!(result["timeline"].as_array().unwrap().len() <= 10_000);
}

#[test]
fn failure_cleanup_shares_the_operation_ceiling() {
    let result: Value = serde_json::from_str(&run_dry_with_options(
        &json!({"blocks":[{"type":"step","id":"fail","handler":"fail"}],
            "on_failure":[skipped_loop()]})
        .to_string(),
        "{}",
    ))
    .unwrap();
    // Preserve the original failure, even when best-effort cleanup is halted.
    assert_eq!(result["ok"], true);
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error"]["block_id"], "fail");
    assert_eq!(result["ticks"], 1);
    assert!(result["timeline"].as_array().unwrap().len() <= 10_000);
}

#[test]
fn saga_compensation_cannot_swallow_the_operation_ceiling() {
    let result: Value = serde_json::from_str(&run_dry_with_options(
        &json!({"blocks":[{"type":"saga","id":"transaction","steps":[
            {"id":"first","action":{"type":"step","id":"done","handler":"noop"},"compensation":skipped_loop()},
            {"id":"second","action":{"type":"step","id":"fail","handler":"fail"}}
        ]}]})
        .to_string(),
        "{}",
    ))
    .unwrap();
    assert_eq!(result["ok"], true, "{result}");
    assert_eq!(result["status"], "operation_limit");
    assert_eq!(result["ticks"], 2);
    assert!(result["timeline"].as_array().unwrap().len() <= 10_000);
}

#[test]
fn for_each_with_skipped_steps_is_bounded() {
    let result: Value = serde_json::from_str(&run_dry_with_options(
        &json!({"blocks":[{"type":"for_each","id":"items","collection":"items",
            "max_iterations":100_000,
            "body":[{"type":"step","id":"skip","handler":"noop","when":"false"}]}]})
        .to_string(),
        &json!({"max_ticks":1,"input":{"items":vec![0; 6000]}}).to_string(),
    ))
    .unwrap();
    assert_bounded(&result);
}
