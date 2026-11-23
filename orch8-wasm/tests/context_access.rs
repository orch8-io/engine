use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

const INPUT: &str =
    r#"{"input":{"public":"invoice","secret":"restricted"},"config":{"key":"restricted"}}"#;

#[test]
fn filters_context_before_resolving_parameters_and_running_local_handlers() {
    let graph = r#"{"blocks":[
      {"type":"step","id":"visible","handler":"lookup",
       "context_access":{"data":{"fields":["public"]},"config":false},
       "params":{"public":"{{ data.public }}","secret":"{{ data.secret }}","key":"{{ config.key }}"}},
      {"type":"step","id":"assert_visible","handler":"assert",
       "when":"data.secret != null",
       "context_access":{"data":{"fields":["public"]},"config":false},
       "params":{"condition":"data.secret != null","message":"handler requires the restricted field"}}
    ]}"#;
    let result: Value = serde_json::from_str(&run_dry_with_options(graph, INPUT)).unwrap();
    assert_eq!(result["status"], "failed", "{result}");
    assert_eq!(result["error"]["block_id"], "assert_visible");
    assert_eq!(
        result["would_call"][0]["params"],
        json!({
            "public":"invoice","secret":null,"key":null
        })
    );
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("handler requires")
    );
}

#[test]
fn filtering_one_step_preserves_context_for_later_steps() {
    let graph = r#"{"blocks":[
      {"type":"step","id":"redacted","handler":"transform",
       "context_access":{"data":false},"params":{"value":"{{ data.secret }}"}},
      {"type":"step","id":"allowed","handler":"transform","params":{"value":"{{ data.secret }}"}}
    ]}"#;
    let result: Value = serde_json::from_str(&run_dry_with_options(graph, INPUT)).unwrap();
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(result["outputs"]["redacted"]["value"], Value::Null);
    assert_eq!(result["outputs"]["allowed"]["value"], "restricted");
}
