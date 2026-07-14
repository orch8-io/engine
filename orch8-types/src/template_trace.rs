//! Template resolution traces: what value did each parameter actually
//! get, and where did it come from?
//!
//! Produced by the inspector-only trace evaluation in
//! `orch8-engine::template_trace`. Never populated during normal
//! execution, and always redacted before leaving the engine.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Outcome of resolving one template expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    /// Resolved to a non-null value.
    Ok,
    /// The referenced path exists but its value is null (explicit null —
    /// fallbacks do NOT fire for it when the producing block stored null).
    Null,
    /// The referenced path does not exist in the context/outputs.
    Missing,
    /// The resolved value was withheld by the redaction policy.
    Redacted,
    /// Evaluation failed (unknown root, filter error, type problem).
    Error,
}

/// One template expression's resolution provenance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ResolutionEntry {
    /// Dotted location of the parameter inside the block's `params`
    /// (e.g. `body.email`, `headers.0.value`).
    pub param_path: String,
    /// The original template expression (inner text of `{{ ... }}`).
    pub expression: String,
    pub status: ResolutionStatus,
    /// The resolved value, redacted. Absent for `missing` / `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// JSON type of the resolved value (`string`, `number`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_type: Option<String>,
    /// The fallback-chain segment that supplied the value (a source path
    /// like `context.data.email`, or `literal` for a quoted default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// True when the value came from a fallback segment, not the first.
    #[serde(default)]
    pub fallback_used: bool,
    /// True when a non-string value was coerced into a string because the
    /// expression sits inside a larger interpolated string.
    #[serde(default)]
    pub coerced_to_string: bool,
    /// The evaluation error, when `status == error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The full trace for one block's parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ResolutionTrace {
    pub block_id: String,
    /// The block's params with every template resolved, redacted. `None`
    /// when whole-params resolution failed (see `entries` for the error).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_params: Option<serde_json::Value>,
    /// One entry per template expression found in the params.
    pub entries: Vec<ResolutionEntry>,
}

/// Request body for the lightweight template debugger.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct DebugTemplateRequest {
    /// Raw template string to resolve (e.g. `"{{ context.data.name | upper }}"`).
    pub template: String,
    /// `context.data` fixture.
    #[serde(default)]
    pub context_data: serde_json::Value,
    /// `context.config` fixture.
    #[serde(default)]
    pub context_config: serde_json::Value,
    /// Prior block outputs fixture: `{block_id: output}`.
    #[serde(default)]
    pub outputs: serde_json::Value,
}

/// Response from the template debugger.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DebugTemplateResponse {
    /// The resolved value after template evaluation, redacted.
    /// `None` when the entire resolution failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// JSON type of the resolved value (`string`, `number`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_type: Option<String>,
    /// Per-expression provenance trace.
    pub entries: Vec<ResolutionEntry>,
    /// Top-level error when the template string is malformed or resolution
    /// fails entirely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&ResolutionStatus::Missing).unwrap(),
            "\"missing\""
        );
        assert_eq!(
            serde_json::to_string(&ResolutionStatus::Redacted).unwrap(),
            "\"redacted\""
        );
    }

    #[test]
    fn entry_round_trips_and_omits_empty_fields() {
        let entry = ResolutionEntry {
            param_path: "body.email".into(),
            expression: "context.data.customer.email".into(),
            status: ResolutionStatus::Missing,
            value: None,
            result_type: None,
            source: None,
            fallback_used: false,
            coerced_to_string: false,
            error: None,
        };
        let v = serde_json::to_value(&entry).unwrap();
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("value"));
        assert!(!obj.contains_key("error"));
        let back: ResolutionEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back, entry);
    }

    #[test]
    fn trace_round_trips() {
        let trace = ResolutionTrace {
            block_id: "charge".into(),
            resolved_params: Some(serde_json::json!({"amount": 100})),
            entries: vec![],
        };
        let back: ResolutionTrace =
            serde_json::from_str(&serde_json::to_string(&trace).unwrap()).unwrap();
        assert_eq!(back, trace);
    }
}
