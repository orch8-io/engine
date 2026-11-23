use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

fn run(sequence: &Value, boundary: &str) -> Value {
    let options = json!({"cancel_before":boundary,"max_ticks":20});
    serde_json::from_str(&run_dry_with_options(
        &sequence.to_string(),
        &options.to_string(),
    ))
    .unwrap()
}

#[test]
fn cancellation_skips_the_boundary_and_runs_cleanup_once() {
    let result = run(
        &json!({
            "blocks":[
                {"type":"step","id":"prepare","handler":"noop"},
                {"type":"try_catch","id":"run","try_block":[{"type":"step","id":"dispatch","handler":"worker"}],
                 "catch_block":[{"type":"step","id":"catch","handler":"noop"}]},
                {"type":"step","id":"after","handler":"noop"}
            ],
            "on_failure":[{"type":"step","id":"failure","handler":"noop"}],
            "on_cancel":[
                {"type":"step","id":"first_cleanup","handler":"fail"},
                {"type":"step","id":"close","handler":"transform","params":{"closed":true}}
            ]
        }),
        "dispatch",
    );
    assert_eq!(result["status"], "cancelled", "{result}");
    assert_eq!(result["outputs"]["close"], json!({"closed":true}));
    for skipped in ["dispatch", "catch", "after", "failure"] {
        assert!(result["outputs"].get(skipped).is_none(), "{result}");
    }
    let timeline = result["timeline"].as_array().unwrap();
    assert_eq!(
        timeline
            .iter()
            .filter(|entry| entry["kind"] == "cancel")
            .count(),
        1
    );
    assert_eq!(
        timeline
            .iter()
            .filter(|entry| entry["path"] == "on_cancel/close")
            .count(),
        1
    );
    assert_eq!(result["would_call"], json!([]));
}

#[test]
fn a_protected_scope_drains_before_cleanup() {
    let result = run(
        &json!({
            "blocks":[{"type":"cancellation_scope","id":"shield","blocks":[
                {"type":"step","id":"protected","handler":"transform","params":{"ran":true}}
            ]},{"type":"step","id":"after","handler":"noop"}],
            "on_cancel":[{"type":"step","id":"observe","handler":"transform",
                "params":{"ran":"{{ outputs.protected.ran }}"}}]
        }),
        "protected",
    );
    assert_eq!(result["status"], "cancelled", "{result}");
    assert_eq!(result["outputs"]["observe"], json!({"ran":true}));
    assert!(result["outputs"].get("after").is_none());
}

#[test]
fn cancellation_cleanup_shares_the_operation_budget() {
    let result = run(
        &json!({
            "blocks":[{"type":"step","id":"run","handler":"noop"}],
            "on_cancel":[
                {"type":"loop","id":"cleanup","condition":"true","max_iterations":100_000,
                 "body":[{"type":"step","id":"skip","handler":"noop","when":"false"}]},
                {"type":"step","id":"after","handler":"noop"}
            ]
        }),
        "run",
    );
    assert_eq!(result["status"], "cancelled");
    assert_eq!(result["operations"], 10_000);
    assert_eq!(result["ticks"], 0);
    assert!(result["outputs"].get("after").is_none());
}
