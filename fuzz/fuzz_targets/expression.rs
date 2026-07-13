#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    let context = orch8_types::context::ExecutionContext::default();
    let outputs = serde_json::Value::Null;
    let _ = orch8_engine::expression::try_evaluate(input, &context, &outputs);
});
