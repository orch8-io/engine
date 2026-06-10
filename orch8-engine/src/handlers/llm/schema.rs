//! `response_schema` support for `llm_call` — structured output validated
//! against a workflow-supplied JSON Schema, with bounded auto-repair.
//!
//! When a step declares `response_schema`, the assistant's text response is
//! parsed as JSON (tolerating markdown fences and surrounding prose) and
//! validated. On failure, the handler appends a repair turn (the raw response
//! as an assistant message plus a corrective user message) and re-calls the
//! *same* provider, up to `max_repair_attempts` times.

use serde_json::{json, Value};

use orch8_types::error::StepError;

use super::common::safe_truncate;

/// Default number of repair re-calls when `max_repair_attempts` is unset.
const DEFAULT_MAX_REPAIR_ATTEMPTS: u32 = 2;

/// Hard cap on `max_repair_attempts` — each repair is a real LLM call.
const MAX_REPAIR_ATTEMPTS_CAP: u32 = 5;

/// Max bytes of the last (failing) response echoed into error `details`.
pub(super) const LAST_RESPONSE_TRUNCATE_BYTES: usize = 2000;

/// A `response_schema` compiled once per step, plus the repair budget.
#[derive(Debug)]
pub(super) struct CompiledSchema {
    validator: jsonschema::Validator,
    /// Number of repair re-calls allowed after the initial call.
    pub(super) max_repair_attempts: u32,
}

impl CompiledSchema {
    /// Validate `candidate` (raw candidate JSON text) against the schema.
    ///
    /// Returns the parsed value on success, or a list of human-readable
    /// validation errors (including JSON parse errors) on failure.
    pub(super) fn validate_text(&self, candidate: &str) -> Result<Value, Vec<String>> {
        let parsed: Value = serde_json::from_str(candidate)
            .map_err(|e| vec![format!("response is not valid JSON: {e}")])?;
        let errors: Vec<String> = self
            .validator
            .iter_errors(&parsed)
            .map(|e| format!("at {}: {e}", e.instance_path()))
            .collect();
        if errors.is_empty() {
            Ok(parsed)
        } else {
            Err(errors)
        }
    }
}

/// Parse and compile the optional `response_schema` param.
///
/// Returns `Ok(None)` when the param is absent. A present-but-invalid schema
/// (not a JSON object, or not a valid JSON Schema) is a configuration error:
/// `StepError::Permanent`, raised before any provider call.
pub(super) fn compile_response_schema(params: &Value) -> Result<Option<CompiledSchema>, StepError> {
    let Some(schema) = params.get("response_schema") else {
        return Ok(None);
    };

    if !schema.is_object() {
        return Err(StepError::Permanent {
            message: "response_schema must be a JSON Schema object".to_string(),
            details: None,
        });
    }

    let validator = jsonschema::validator_for(schema).map_err(|e| StepError::Permanent {
        message: format!("response_schema is not a valid JSON Schema: {e}"),
        details: None,
    })?;

    let max_repair_attempts = params
        .get("max_repair_attempts")
        .and_then(Value::as_u64)
        .map_or(DEFAULT_MAX_REPAIR_ATTEMPTS, |n| {
            u32::try_from(n)
                .unwrap_or(MAX_REPAIR_ATTEMPTS_CAP)
                .min(MAX_REPAIR_ATTEMPTS_CAP)
        });

    Ok(Some(CompiledSchema {
        validator,
        max_repair_attempts,
    }))
}

/// Extract the most plausible JSON document from an LLM text response.
///
/// Steps: trim whitespace; strip a surrounding markdown code fence
/// (```` ```json … ``` ```` or ```` ``` … ``` ````); if the remainder still
/// isn't pure JSON, fall back to the first balanced top-level `{…}` or `[…]`.
/// The returned slice may still fail to parse — validation reports that.
pub(super) fn extract_candidate_json(text: &str) -> &str {
    let trimmed = text.trim();
    let unfenced = strip_code_fence(trimmed);
    if serde_json::from_str::<Value>(unfenced).is_ok() {
        return unfenced;
    }
    balanced_json_slice(unfenced).unwrap_or(unfenced)
}

/// Strip a single surrounding markdown code fence, tolerating a language tag
/// on the opening fence (```` ```json ````). Returns the input unchanged when
/// it isn't fenced.
fn strip_code_fence(text: &str) -> &str {
    let Some(rest) = text.strip_prefix("```") else {
        return text;
    };
    // Drop the rest of the opening fence line (e.g. a `json` language tag).
    let body = match rest.find('\n') {
        Some(i) => &rest[i + 1..],
        None => return text, // single-line ``` … without a body — not a fence
    };
    let Some(body) = body.trim_end().strip_suffix("```") else {
        return text; // no closing fence — leave as-is
    };
    body.trim()
}

/// Find the first balanced top-level `{…}` or `[…]` in `text`, skipping
/// braces inside JSON string literals (quote/escape aware).
fn balanced_json_slice(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let start = bytes.iter().position(|&b| b == b'{' || b == b'[')?;
    let (open, close) = if bytes[start] == b'{' {
        (b'{', b'}')
    } else {
        (b'[', b']')
    };

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            _ if b == open => depth += 1,
            _ if b == close => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(&text[start..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Build the corrective user message for a repair turn.
pub(super) fn repair_message(errors: &[String]) -> String {
    let mut bullets = String::new();
    for e in errors {
        bullets.push_str("- ");
        bullets.push_str(e);
        bullets.push('\n');
    }
    format!(
        "Your previous response was not valid against the required JSON schema. \
         Validation errors:\n{bullets}Return ONLY the corrected JSON, no prose, no code fences."
    )
}

/// Append a repair turn to `params["messages"]`: the model's raw response as
/// an assistant message, then a corrective user message listing the errors.
pub(super) fn append_repair_turn(params: &mut Value, raw_response: &str, errors: &[String]) {
    let turn_assistant = json!({ "role": "assistant", "content": raw_response });
    let turn_user = json!({ "role": "user", "content": repair_message(errors) });

    let Some(obj) = params.as_object_mut() else {
        return;
    };
    let messages = obj.entry("messages").or_insert_with(|| json!([]));
    if let Some(arr) = messages.as_array_mut() {
        arr.push(turn_assistant);
        arr.push(turn_user);
    }
}

/// Build the `details` payload for a schema-exhaustion failure.
pub(super) fn exhaustion_details(errors: &[String], last_response: &str) -> Value {
    json!({
        "validation_errors": errors,
        "last_response": safe_truncate(last_response, LAST_RESPONSE_TRUNCATE_BYTES),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- extract_candidate_json / fence stripping ---------------------------

    #[test]
    fn extract_pure_json_passthrough() {
        assert_eq!(extract_candidate_json(r#"{"a": 1}"#), r#"{"a": 1}"#);
        assert_eq!(extract_candidate_json("[1, 2]"), "[1, 2]");
    }

    #[test]
    fn extract_trims_whitespace() {
        assert_eq!(extract_candidate_json("  \n {\"a\": 1} \n"), "{\"a\": 1}");
    }

    #[test]
    fn extract_strips_json_fence() {
        let text = "```json\n{\"a\": 1}\n```";
        assert_eq!(extract_candidate_json(text), "{\"a\": 1}");
    }

    #[test]
    fn extract_strips_plain_fence() {
        let text = "```\n[1, 2, 3]\n```";
        assert_eq!(extract_candidate_json(text), "[1, 2, 3]");
    }

    #[test]
    fn extract_balanced_object_from_prose() {
        let text = "Sure! Here is the JSON: {\"a\": {\"b\": 2}} hope that helps";
        assert_eq!(extract_candidate_json(text), "{\"a\": {\"b\": 2}}");
    }

    #[test]
    fn extract_balanced_array_from_prose() {
        let text = "Result: [1, [2, 3]] done";
        assert_eq!(extract_candidate_json(text), "[1, [2, 3]]");
    }

    #[test]
    fn extract_braces_inside_strings_are_skipped() {
        let text = r#"note {"msg": "shape } is ok", "n": 1} tail"#;
        assert_eq!(
            extract_candidate_json(text),
            r#"{"msg": "shape } is ok", "n": 1}"#
        );
    }

    #[test]
    fn extract_escaped_quote_in_string() {
        let text = r#"x {"msg": "quote \" then }", "n": 2} y"#;
        assert_eq!(
            extract_candidate_json(text),
            r#"{"msg": "quote \" then }", "n": 2}"#
        );
    }

    #[test]
    fn extract_unbalanced_returns_input() {
        let text = "{\"a\": 1"; // never closed
        assert_eq!(extract_candidate_json(text), text);
    }

    #[test]
    fn extract_no_json_at_all_returns_input() {
        assert_eq!(extract_candidate_json("just words"), "just words");
    }

    #[test]
    fn extract_fence_without_closing_left_alone() {
        let text = "```json\n{\"a\": 1}";
        // No closing fence — fence stripping bails, balanced extraction finds the object.
        assert_eq!(extract_candidate_json(text), "{\"a\": 1}");
    }

    #[test]
    fn extract_fenced_json_with_prose_inside_falls_back_to_balanced() {
        let text = "```\nhere: {\"a\": 1}\n```";
        assert_eq!(extract_candidate_json(text), "{\"a\": 1}");
    }

    // --- compile_response_schema --------------------------------------------

    #[test]
    fn compile_absent_schema_is_none() {
        assert!(compile_response_schema(&json!({}))
            .expect("absent schema is fine")
            .is_none());
    }

    #[test]
    fn compile_non_object_schema_is_permanent() {
        let err = compile_response_schema(&json!({"response_schema": "nope"}))
            .expect_err("string schema must be rejected");
        assert!(matches!(err, StepError::Permanent { .. }), "{err}");
        assert!(err.to_string().contains("JSON Schema object"));
    }

    #[test]
    fn compile_invalid_schema_is_permanent() {
        let err = compile_response_schema(&json!({"response_schema": {"type": "not-a-real-type"}}))
            .expect_err("bogus type keyword must fail compilation");
        assert!(matches!(err, StepError::Permanent { .. }), "{err}");
        assert!(err.to_string().contains("not a valid JSON Schema"));
    }

    #[test]
    fn compile_defaults_and_caps_repair_attempts() {
        let schema = json!({"type": "object"});
        let c = compile_response_schema(&json!({"response_schema": schema}))
            .unwrap()
            .unwrap();
        assert_eq!(c.max_repair_attempts, DEFAULT_MAX_REPAIR_ATTEMPTS);

        let c = compile_response_schema(
            &json!({"response_schema": {"type": "object"}, "max_repair_attempts": 99}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.max_repair_attempts, MAX_REPAIR_ATTEMPTS_CAP);

        let c = compile_response_schema(
            &json!({"response_schema": {"type": "object"}, "max_repair_attempts": 0}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.max_repair_attempts, 0);
    }

    #[test]
    fn validate_text_reports_parse_and_schema_errors() {
        let c = compile_response_schema(&json!({
            "response_schema": {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            }
        }))
        .unwrap()
        .unwrap();

        // Parse error
        let errs = c.validate_text("{not json").unwrap_err();
        assert!(errs[0].contains("not valid JSON"), "{errs:?}");

        // Schema violation
        let errs = c.validate_text(r#"{"name": 42}"#).unwrap_err();
        assert!(errs.iter().any(|e| e.contains("/name")), "{errs:?}");

        // Valid
        let v = c.validate_text(r#"{"name": "ok"}"#).unwrap();
        assert_eq!(v["name"], "ok");
    }

    // --- repair turn construction --------------------------------------------

    #[test]
    fn repair_message_bullets_errors() {
        let msg = repair_message(&["e1".into(), "e2".into()]);
        assert!(msg.contains("- e1\n"));
        assert!(msg.contains("- e2\n"));
        assert!(msg.starts_with("Your previous response was not valid"));
        assert!(msg.ends_with("Return ONLY the corrected JSON, no prose, no code fences."));
    }

    #[test]
    fn append_repair_turn_appends_two_messages() {
        let mut params = json!({"messages": [{"role": "user", "content": "go"}]});
        append_repair_turn(&mut params, "bad output", &["missing field".into()]);
        let msgs = params["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1]["role"], "assistant");
        assert_eq!(msgs[1]["content"], "bad output");
        assert_eq!(msgs[2]["role"], "user");
        assert!(msgs[2]["content"]
            .as_str()
            .unwrap()
            .contains("- missing field"));
    }

    #[test]
    fn append_repair_turn_creates_messages_when_absent() {
        let mut params = json!({});
        append_repair_turn(&mut params, "raw", &["err".into()]);
        assert_eq!(params["messages"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn exhaustion_details_truncates_last_response() {
        let long = "x".repeat(LAST_RESPONSE_TRUNCATE_BYTES + 500);
        let d = exhaustion_details(&["e".into()], &long);
        assert_eq!(
            d["last_response"].as_str().unwrap().len(),
            LAST_RESPONSE_TRUNCATE_BYTES
        );
        assert_eq!(d["validation_errors"], json!(["e"]));
    }
}
