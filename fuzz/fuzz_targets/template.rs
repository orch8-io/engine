#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    let context = orch8_types::context::ExecutionContext::default();
    let outputs = serde_json::Value::Null;
    let value = serde_json::Value::String(input.to_owned());
    let _ = orch8_engine::template::resolve(&value, &context, &outputs);
});
