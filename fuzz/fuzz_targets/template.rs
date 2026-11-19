#![no_main]

use libfuzzer_sys::fuzz_target;
use orch8_types::context::ExecutionContext;
use serde_json::Value;

fuzz_target!(|input: &str| {
    if input.len() > 16 * 1024 {
        return;
    }
    // This form exercises nested templates in inputs, with references to
    // populated execution context and earlier step outputs.
    if let Ok([value, data, config, outputs]) = serde_json::from_str::<[Value; 4]>(input) {
        let context = ExecutionContext {
            data,
            config,
            ..ExecutionContext::default()
        };
        let _ = orch8_engine::template::resolve(&value, &context, &outputs);
    } else {
        let _ = orch8_engine::template::resolve(
            &Value::String(input.to_owned()),
            &ExecutionContext::default(),
            &Value::Null,
        );
    }
});
