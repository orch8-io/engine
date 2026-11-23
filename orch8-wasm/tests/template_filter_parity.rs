use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

#[test]
fn preview_runs_the_same_filter_fixtures_as_the_engine() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../orch8-engine/tests/fixtures/template_filter_parity.json"
    ))
    .unwrap();
    let options = json!({"input":fixture["input"]}).to_string();
    let run = |params: &Value| {
        let sequence = json!({"blocks":[
            {"type":"step","id":"transform","handler":"transform","params":params},
            {"type":"step","id":"next","handler":"noop"}
        ]});
        serde_json::from_str::<Value>(&run_dry_with_options(&sequence.to_string(), &options))
            .unwrap()
    };
    let valid = run(&fixture["templates"]);
    assert_eq!(valid["status"], "completed", "{valid}");
    assert_eq!(valid["outputs"]["transform"], fixture["expected"]);
    for invalid in fixture["invalid"].as_array().unwrap() {
        let result = run(&json!({"value":invalid["template"]}));
        assert_eq!(result["status"], "failed", "{result}");
        assert_eq!(result["error"]["block_id"], "transform");
        assert!(result["outputs"].get("next").is_none());
        assert!(
            result["error"]["message"]
                .as_str()
                .unwrap()
                .contains(invalid["message"].as_str().unwrap()),
            "{result}"
        );
    }
}
