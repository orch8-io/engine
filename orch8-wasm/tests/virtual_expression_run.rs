use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

const SEQUENCE: &str = r#"{
  "blocks":[
    {"type":"step","id":"before","handler":"transform","params":{"at":"{{ now() }}"}},
    {"type":"step","id":"after","handler":"transform","delay":{"duration":3600000},
     "when":"now() == outputs.before.at",
     "params":{"at":"{{ now() }}","roll":"{{ random(0,100) }}","id":"{{ uuid() }}"}},
    {"type":"router","id":"select","routes":[{"condition":"outputs.after.roll < 50",
      "blocks":[{"type":"step","id":"low","handler":"noop"}]}],
      "default":[{"type":"step","id":"high","handler":"noop"}]}
  ]
}"#;

fn preview(seed: u64) -> Value {
    let options = json!({"start_at":"2026-02-01T12:00:00-03:00","seed":seed});
    serde_json::from_str(&run_dry_with_options(SEQUENCE, &options.to_string())).unwrap()
}

#[test]
fn virtual_time_is_visible_before_and_after_a_one_hour_delay() {
    let result = preview(42);
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(result["virtual_duration_ms"], 3_600_000);
    assert_eq!(
        result["outputs"]["before"]["at"],
        "2026-02-01T15:00:00+00:00"
    );
    assert_eq!(
        result["outputs"]["after"]["at"],
        "2026-02-01T16:00:00+00:00"
    );
}

#[test]
fn a_seed_replays_values_and_branch_selection() {
    let first = preview(42);
    assert_eq!(first["status"], "completed", "{first}");
    assert_eq!(first, preview(42));
    assert_ne!(
        first["outputs"]["after"]["id"],
        preview(43)["outputs"]["after"]["id"]
    );
    let id = uuid::Uuid::parse_str(first["outputs"]["after"]["id"].as_str().unwrap()).unwrap();
    assert_eq!(id.get_version_num(), 4);
    let selected = if first["outputs"]["after"]["roll"].as_u64().unwrap() < 50 {
        "low"
    } else {
        "high"
    };
    assert!(first["outputs"].get(selected).is_some(), "{first}");
}
