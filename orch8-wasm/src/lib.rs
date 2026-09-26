//! Orch8 in the browser: sequence validation, static preflight and a dry-run
//! interpreter with virtual time, exported to JavaScript via `wasm-bindgen`.
//!
//! `orch8-engine` cannot target `wasm32-unknown-unknown` (tokio runtime,
//! sqlx, reqwest, wasmtime), so this crate is deliberately the smallest
//! honest subset: it reuses `orch8-types` (the exact server decoder and
//! validator) and the engine's expression evaluator source, and adds a pure
//! interpreter that never performs I/O. See `docs/PLAYGROUND.md` for limits.
//!
//! Every export takes and returns JSON strings and never throws: failures
//! come back as `{"ok": false, "error": "..."}`.

// The engine's expression evaluator, compiled from the same source file so
// playground conditions behave exactly like server conditions.
#[allow(dead_code)]
#[path = "../../orch8-engine/src/expression.rs"]
mod expression;

pub mod decode;
pub mod interp;
pub mod preflight;

use serde_json::{Value, json};
use wasm_bindgen::prelude::wasm_bindgen;

fn to_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| {
        format!(r#"{{"ok":false,"error":"serialization failed: {error}"}}"#)
    })
}

/// Strictly decode + validate a sequence exactly like the server. Returns
/// `{"ok": true, "sequence": ..., "defaults_applied": [...], "warnings": [...]}`
/// or `{"ok": false, "error": "..."}`.
#[wasm_bindgen]
pub fn validate_sequence(sequence_json: &str) -> String {
    let result = match decode::decode(sequence_json) {
        Ok(decoded) => json!({
            "ok": true,
            "warnings": decoded.sequence.unknown_handler_warnings(),
            "defaults_applied": decoded.defaults_applied,
            "sequence": decoded.sequence,
        }),
        Err(error) => json!({ "ok": false, "error": error }),
    };
    to_string(&result)
}

/// Static preflight report (same shape as the server's `PreflightReport`).
#[wasm_bindgen]
pub fn preflight(sequence_json: &str) -> String {
    to_string(&json!(preflight::preflight(sequence_json)))
}

/// Dry-run with `input_json` as `context.data` and a step-attempt budget.
#[wasm_bindgen]
pub fn run_dry(sequence_json: &str, input_json: &str, max_ticks: u32) -> String {
    let input = if input_json.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(input_json) {
            Ok(value) => value,
            Err(error) => {
                return to_string(
                    &json!({ "ok": false, "error": format!("invalid input JSON: {error}") }),
                );
            }
        }
    };
    let options = interp::RunOptions {
        input,
        max_ticks,
        ..Default::default()
    };
    run_with(sequence_json, &options)
}

/// Dry-run with full options:
/// `{"input", "config", "mocks": {"<block_id>": {"output", "error",
/// "retryable", "duration_ms", "fail_attempts"}}, "max_ticks", "instance_id"}`.
#[wasm_bindgen]
pub fn run_dry_with_options(sequence_json: &str, options_json: &str) -> String {
    let options: interp::RunOptions = if options_json.trim().is_empty() {
        interp::RunOptions::default()
    } else {
        match serde_json::from_str(options_json) {
            Ok(options) => options,
            Err(error) => {
                return to_string(
                    &json!({ "ok": false, "error": format!("invalid options: {error}") }),
                );
            }
        }
    };
    run_with(sequence_json, &options)
}

fn run_with(sequence_json: &str, options: &interp::RunOptions) -> String {
    match decode::decode(sequence_json) {
        Ok(decoded) => {
            let mut result = interp::run(&decoded.sequence, options);
            result["ok"] = json!(true);
            result["defaults_applied"] = json!(decoded.defaults_applied);
            to_string(&result)
        }
        Err(error) => to_string(&json!({ "ok": false, "error": error })),
    }
}

/// Sequence schema version this build understands.
#[wasm_bindgen]
pub fn sequence_schema_version() -> u32 {
    orch8_types::sequence::SEQUENCE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn exports_never_throw_and_report_errors_as_json() {
        assert_eq!(parse(&validate_sequence("nope"))["ok"], false);
        assert_eq!(parse(&run_dry("{}", "", 10))["ok"], false);
        assert_eq!(
            parse(&run_dry(
                r#"{"blocks":[{"type":"step","id":"a","handler":"noop"}]}"#,
                "[",
                10
            ))["ok"],
            false
        );
        assert_eq!(
            parse(&run_dry_with_options("{}", r#"{"bogus":1}"#))["ok"],
            false
        );
    }

    #[test]
    fn run_dry_round_trip() {
        let out = parse(&run_dry(
            r#"{"blocks":[{"type":"step","id":"a","handler":"log","params":{"message":"{{ data.x }}"}}]}"#,
            r#"{"x":"hello"}"#,
            100,
        ));
        assert_eq!(out["ok"], true);
        assert_eq!(out["status"], "completed");
        assert_eq!(out["outputs"]["a"]["message"], "hello");
    }

    #[test]
    fn run_with_mocks() {
        let out = parse(&run_dry_with_options(
            r#"{"blocks":[{"type":"step","id":"ask","handler":"llm_call"},
                {"type":"router","id":"r","routes":[{"condition":"outputs.ask.label == \"spam\"","blocks":[{"type":"step","id":"drop","handler":"noop"}]}]}]}"#,
            r#"{"mocks":{"ask":{"output":{"label":"spam"},"duration_ms":1200}}}"#,
        ));
        assert_eq!(out["status"], "completed");
        assert_eq!(out["virtual_duration_ms"], 1200);
        assert!(out["outputs"].get("drop").is_some());
    }

    #[test]
    fn validate_and_preflight_shapes() {
        let seq = r#"{"blocks":[{"type":"step","id":"a","handler":"noop"}]}"#;
        let v = parse(&validate_sequence(seq));
        assert_eq!(v["ok"], true);
        assert_eq!(v["sequence"]["name"], "playground");
        let p = parse(&preflight(seq));
        assert_eq!(p["overall"], "pass");
        assert_eq!(sequence_schema_version(), 1);
    }
}
