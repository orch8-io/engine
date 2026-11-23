use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

fn run(signals: &Value, timeout: Option<u64>) -> Value {
    let mut gate = json!({"store_as":"choice"});
    if let Some(timeout) = timeout {
        gate["timeout"] = json!(timeout);
    }
    let graph = json!({"blocks":[
        {"type":"step","id":"prepare","handler":"noop","delay":{"duration":500}},
        {"type":"step","id":"gate","handler":"noop","wait_for_input":gate},
        {"type":"step","id":"read","handler":"transform","params":{"choice":"{{ data.choice }}"}}
    ]});
    let options = json!({"signals":signals});
    serde_json::from_str(&run_dry_with_options(
        &graph.to_string(),
        &options.to_string(),
    ))
    .unwrap()
}

fn answer(at_ms: u64, value: &str) -> Value {
    json!({"at_ms":at_ms,"signal_type":"custom:human_input:gate","payload":{"value":value}})
}

#[test]
fn answers_before_and_during_the_wait_resume_at_the_correct_virtual_time() {
    for (at_ms, expected) in [(100, 500), (750, 750), (1500, 1500)] {
        let result = run(&json!([answer(at_ms, "yes")]), Some(1000));
        assert_eq!(result["status"], "completed", "{result}");
        assert_eq!(result["virtual_duration_ms"], expected);
        assert_eq!(result["outputs"]["read"], json!({"choice":"yes"}));
    }
}

#[test]
fn sorts_fixtures_and_rejects_invalid_payloads_in_submission_order() {
    let result = run(
        &json!([
            answer(1200, "yes"),
            answer(800, "invalid"),
            answer(800, "no")
        ]),
        Some(1000),
    );
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(result["virtual_duration_ms"], 800);
    assert_eq!(result["outputs"]["read"], json!({"choice":"no"}));
    assert_eq!(
        result["timeline"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["kind"] == "signal_delivered")
            .count(),
        1
    );
}

#[test]
fn a_late_answer_cannot_complete_the_gate_and_an_open_wait_is_reported() {
    let timed_out = run(&json!([answer(1501, "yes")]), Some(1000));
    assert_eq!(timed_out["status"], "failed", "{timed_out}");
    assert_eq!(timed_out["virtual_duration_ms"], 1500);
    assert_eq!(timed_out["error"]["block_id"], "gate");
    assert!(timed_out["outputs"].get("read").is_none());
    let waiting = run(&json!([]), None);
    assert_eq!(waiting["status"], "waiting", "{waiting}");
    assert_eq!(waiting["virtual_duration_ms"], 500);
}
