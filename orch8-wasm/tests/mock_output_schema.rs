use orch8_wasm::run_dry_with_options;
use serde_json::{Value, json};

const GRAPH: &str = r##"{
  "blocks":[
    {"type":"step","id":"produce","handler":"lookup","retry":{"max_attempts":3},
      "output_schema":{
        "type":"object","required":["record"],"additionalProperties":false,
        "$defs":{"value":{"type":"integer","minimum":1}},
        "properties":{"record":{"type":"object","required":["value"],
          "properties":{"value":{"$ref":"#/$defs/value"}}}}
      }
    },
    {"type":"step","id":"consume","handler":"transform",
      "params":{"amount":"{{ outputs.produce.record.value }}"}}
  ]
}"##;

fn run(mocked_value: &Value) -> Value {
    let options = json!({"mocks":{"produce":{"output":{"record":{"value":mocked_value}}}}});
    serde_json::from_str(&run_dry_with_options(GRAPH, &options.to_string())).unwrap()
}

#[test]
fn rejects_mock_output_at_the_producer_before_the_consumer_reads_it() {
    let result = run(&json!("4"));
    assert_eq!(result["status"], "failed", "{result}");
    assert_eq!(result["error"]["block_id"], "produce");
    assert_eq!(result["ticks"], 1);
    assert_eq!(result["outputs"], json!({}));
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("at /record/value")
    );
}

#[test]
fn valid_mock_output_preserves_native_json_types() {
    let result = run(&json!(4));
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(result["outputs"]["consume"], json!({"amount":4}));
}

#[test]
fn explicit_mock_null_is_validated_as_null() {
    let graph = r#"{"blocks":[{"type":"step","id":"x","handler":"worker","output_schema":{"type":"null"}}]}"#;
    let result: Value = serde_json::from_str(&run_dry_with_options(
        graph,
        r#"{"mocks":{"x":{"output":null}}}"#,
    ))
    .unwrap();
    assert_eq!(result["status"], "completed", "{result}");
    assert_eq!(result["outputs"], json!({"x":null}));
}
