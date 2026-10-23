//! Location-aware parsing of authored sequence JSON.
//!
//! `serde_json::from_value::<SequenceDefinition>` answers "is this valid?"
//! with an error that names the field but not *where* it is (`missing field
//! "handler"` — in which of 40 blocks?), and silently drops keys it does not
//! know, so a typo like `"retires"` becomes a step with no retry policy. This
//! module wraps deserialization so that:
//!
//! - every error carries a JSON path (`blocks[2].handler`) via
//!   `serde_path_to_error`;
//! - unknown fields are collected, with a did-you-mean suggestion, instead of
//!   being discarded;
//! - the optional top-level `schema_version` is checked against
//!   [`SEQUENCE_SCHEMA_VERSION`] and consumed.
//!
//! Unknown-field detection is two-layered. `serde_ignored` reports keys the
//! derived `Deserialize` impls ignore, but it cannot see inside
//! `BlockDefinition`: an internally tagged enum buffers its content before
//! dispatching on `type`, and the re-deserialization from that buffer bypasses
//! the wrapper. So a structural scan ([`scan_unknown_fields`]) driven by the
//! static field tables in this module walks the block tree; `serde_ignored`
//! stays as a safety net for plain structs outside the block tree. Results
//! are merged and de-duplicated. The tables are kept honest by tests that
//! serialize fully-populated samples of every struct and compare key sets.

use serde_json::Value;

use crate::sequence::SequenceDefinition;
use crate::sequence::validation_codes::SEQ_SCHEMA_VERSION_UNSUPPORTED;
use crate::suggest::did_you_mean;

/// The sequence document format version this build understands. A document
/// may declare `"schema_version": N`; `N > SEQUENCE_SCHEMA_VERSION` is
/// rejected (the author is using features this server does not know). A
/// missing `schema_version` means version 1.
pub const SEQUENCE_SCHEMA_VERSION: u32 = 1;

/// A key the parser did not recognise and therefore ignored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownField {
    /// Path of the object that contains the field, e.g. `blocks[0]` or
    /// `blocks[3].retry`. Empty for the top-level object.
    pub path: String,
    /// The unrecognised key.
    pub field: String,
    /// Closest known field name at that location, if one is close enough.
    pub suggestion: Option<String>,
}

/// A successfully parsed definition plus the keys that were ignored.
#[derive(Debug, Clone)]
pub struct ParsedSequence {
    pub definition: SequenceDefinition,
    pub unknown_fields: Vec<UnknownField>,
}

/// Why a document could not be turned into a [`SequenceDefinition`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SequenceParseError {
    /// The JSON is well-formed but does not match the sequence schema.
    /// `path` locates the problem (`blocks[2].handler`; `$` for the root).
    #[error("{path}: {message}")]
    Invalid { path: String, message: String },
    /// The input is not valid JSON at all.
    #[error("invalid JSON: {0}")]
    Json(String),
}

/// Parse a sequence document from JSON text.
pub fn parse_sequence_str(s: &str) -> Result<ParsedSequence, SequenceParseError> {
    let value: Value =
        serde_json::from_str(s).map_err(|e| SequenceParseError::Json(e.to_string()))?;
    parse_sequence_value(value)
}

/// Parse a sequence document from an already-decoded JSON value.
pub fn parse_sequence_value(mut value: Value) -> Result<ParsedSequence, SequenceParseError> {
    check_and_strip_schema_version(&mut value)?;

    let mut ignored: Vec<UnknownField> = Vec::new();
    let deserializer = serde_ignored::Deserializer::new(&value, |path: serde_ignored::Path| {
        if let Some(unknown) = unknown_from_ignored_path(&path) {
            ignored.push(unknown);
        }
    });
    let definition: SequenceDefinition = serde_path_to_error::deserialize(deserializer)
        .map_err(|e| SequenceParseError::Invalid {
            path: render_error_path(e.path()),
            message: e.inner().to_string(),
        })?;

    let mut unknown_fields = Vec::new();
    scan_unknown_fields(&value, Kind::Root, "", &mut unknown_fields);
    // `serde_ignored` cannot report suggestions (it does not know the struct),
    // so the scan's entries win; only add ignored keys the scan missed.
    for entry in ignored {
        if !unknown_fields
            .iter()
            .any(|u| u.path == entry.path && u.field == entry.field)
        {
            unknown_fields.push(entry);
        }
    }

    Ok(ParsedSequence {
        definition,
        unknown_fields,
    })
}

/// Human-readable warning for one unknown field, e.g.
/// `unknown field "retires" at blocks[0] (did you mean "retry"?)`.
#[must_use]
pub fn format_unknown_field_warning(u: &UnknownField) -> String {
    let mut msg = format!("unknown field \"{}\"", u.field);
    if !u.path.is_empty() {
        msg.push_str(&format!(" at {}", u.path));
    }
    if let Some(s) = &u.suggestion {
        msg.push_str(&format!(" (did you mean \"{s}\"?)"));
    }
    msg
}

fn check_and_strip_schema_version(value: &mut Value) -> Result<(), SequenceParseError> {
    let Some(obj) = value.as_object_mut() else {
        // Not an object: let deserialization produce the proper error.
        return Ok(());
    };
    let Some(sv) = obj.remove("schema_version") else {
        return Ok(());
    };
    let invalid = |message: String| SequenceParseError::Invalid {
        path: "schema_version".into(),
        message,
    };
    let n = sv
        .as_u64()
        .ok_or_else(|| invalid(format!("must be a positive integer, got {sv}")))?;
    if n == 0 {
        return Err(invalid("must be >= 1".into()));
    }
    if n > u64::from(SEQUENCE_SCHEMA_VERSION) {
        return Err(invalid(format!(
            "{SEQ_SCHEMA_VERSION_UNSUPPORTED}: schema_version {n} is newer than this server \
             supports (max {SEQUENCE_SCHEMA_VERSION}); upgrade the server or author against \
             schema_version {SEQUENCE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

/// Render a `serde_path_to_error` path as `blocks[2].retry.max_attempts`
/// (`$` for the root).
fn render_error_path(path: &serde_path_to_error::Path) -> String {
    use serde_path_to_error::Segment;
    let mut out = String::new();
    for seg in path.iter() {
        match seg {
            Segment::Map { key } => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            Segment::Seq { index } => out.push_str(&format!("[{index}]")),
            // Enum variant names are an implementation detail of the tagged
            // representation, not a location the author wrote.
            Segment::Enum { .. } => {}
            Segment::Unknown => out.push('?'),
        }
    }
    if out.is_empty() { "$".into() } else { out }
}

/// Render a `serde_ignored` path into (`parent path`, `field`) using the
/// same `a[0].b` convention. `None` when the ignored node is not a map key
/// (cannot happen for struct fields, but the type allows it).
fn unknown_from_ignored_path(path: &serde_ignored::Path) -> Option<UnknownField> {
    fn render(path: &serde_ignored::Path, out: &mut String) {
        use serde_ignored::Path;
        match path {
            Path::Root => {}
            Path::Seq { parent, index } => {
                render(parent, out);
                out.push_str(&format!("[{index}]"));
            }
            Path::Map { parent, key } => {
                render(parent, out);
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str(key);
            }
            Path::Some { parent }
            | Path::NewtypeStruct { parent }
            | Path::NewtypeVariant { parent } => render(parent, out),
        }
    }
    match path {
        serde_ignored::Path::Map { parent, key } => {
            let mut parent_path = String::new();
            render(parent, &mut parent_path);
            Some(UnknownField {
                path: parent_path,
                field: key.to_string(),
                suggestion: None,
            })
        }
        _ => None,
    }
}

// ─── Structural scan ────────────────────────────────────────────────────────

/// What a JSON node is expected to be, so the scan knows which field table
/// applies (or that the subtree is free-form and must not be inspected).
#[derive(Debug, Clone, Copy)]
enum Kind {
    Root,
    /// A `BlockDefinition`; the field table depends on its `type`.
    Block,
    Retry,
    Delay,
    SendWindow,
    ContextAccess,
    WaitForInput,
    Escalation,
    StepCompensation,
    Sla,
    Interceptors,
    InterceptorAction,
    HumanChoice,
    Route,
    AbVariant,
    SagaStep,
    /// An array whose elements are all of the inner kind.
    List(&'static Kind),
    /// Free-form JSON (`params`, `input`, JSON Schemas, scalars): never
    /// descended into.
    Opaque,
}

const BLOCKS: Kind = Kind::List(&Kind::Block);
const BRANCHES: Kind = Kind::List(&BLOCKS);

type FieldTable = &'static [(&'static str, Kind)];

/// Top-level `SequenceDefinition` keys (plus the parser-level
/// `schema_version`).
pub const ROOT_FIELDS: FieldTable = &[
    ("schema_version", Kind::Opaque),
    ("id", Kind::Opaque),
    ("tenant_id", Kind::Opaque),
    ("namespace", Kind::Opaque),
    ("name", Kind::Opaque),
    ("version", Kind::Opaque),
    ("deprecated", Kind::Opaque),
    ("status", Kind::Opaque),
    ("blocks", BLOCKS),
    ("interceptors", Kind::Interceptors),
    ("input_schema", Kind::Opaque),
    ("sla", Kind::Sla),
    ("on_failure", BLOCKS),
    ("on_cancel", BLOCKS),
    ("created_at", Kind::Opaque),
];

const STEP_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("handler", Kind::Opaque),
    ("params", Kind::Opaque),
    ("delay", Kind::Delay),
    ("retry", Kind::Retry),
    ("timeout", Kind::Opaque),
    ("rate_limit_key", Kind::Opaque),
    ("send_window", Kind::SendWindow),
    ("context_access", Kind::ContextAccess),
    ("cancellable", Kind::Opaque),
    ("wait_for_input", Kind::WaitForInput),
    ("queue_name", Kind::Opaque),
    ("deadline", Kind::Opaque),
    ("on_deadline_breach", Kind::Escalation),
    ("fallback_handler", Kind::Opaque),
    ("cache_key", Kind::Opaque),
    ("output_schema", Kind::Opaque),
    ("when", Kind::Opaque),
    ("compensation", Kind::StepCompensation),
];
const PARALLEL_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("branches", BRANCHES),
];
const RACE_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("branches", BRANCHES),
    ("semantics", Kind::Opaque),
];
const LOOP_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("condition", Kind::Opaque),
    ("body", BLOCKS),
    ("max_iterations", Kind::Opaque),
    ("break_on", Kind::Opaque),
    ("continue_on_error", Kind::Opaque),
    ("poll_interval", Kind::Opaque),
    ("retain_iterations", Kind::Opaque),
];
const FOR_EACH_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("collection", Kind::Opaque),
    ("item_var", Kind::Opaque),
    ("body", BLOCKS),
    ("max_iterations", Kind::Opaque),
    ("retain_iterations", Kind::Opaque),
];
const ROUTER_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("routes", Kind::List(&Kind::Route)),
    ("default", BLOCKS),
];
const TRY_CATCH_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("try_block", BLOCKS),
    ("catch_block", BLOCKS),
    ("finally_block", BLOCKS),
];
const SUB_SEQUENCE_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("sequence_name", Kind::Opaque),
    ("version", Kind::Opaque),
    ("input", Kind::Opaque),
];
const AB_SPLIT_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("variants", Kind::List(&Kind::AbVariant)),
];
const CANCELLATION_SCOPE_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("blocks", BLOCKS),
];
const SAGA_FIELDS: FieldTable = &[
    ("type", Kind::Opaque),
    ("id", Kind::Opaque),
    ("steps", Kind::List(&Kind::SagaStep)),
];
const RETRY_FIELDS: FieldTable = &[
    ("max_attempts", Kind::Opaque),
    ("initial_backoff", Kind::Opaque),
    ("max_backoff", Kind::Opaque),
    ("backoff_multiplier", Kind::Opaque),
    ("jitter", Kind::Opaque),
    ("retry_if", Kind::Opaque),
    ("non_retryable_codes", Kind::Opaque),
];
const DELAY_FIELDS: FieldTable = &[
    ("duration", Kind::Opaque),
    ("business_days_only", Kind::Opaque),
    ("jitter", Kind::Opaque),
    ("holidays", Kind::Opaque),
    ("fire_at_local", Kind::Opaque),
    ("timezone", Kind::Opaque),
];
const SEND_WINDOW_FIELDS: FieldTable = &[
    ("start_hour", Kind::Opaque),
    ("end_hour", Kind::Opaque),
    ("days", Kind::Opaque),
];
const CONTEXT_ACCESS_FIELDS: FieldTable = &[
    ("data", Kind::Opaque),
    ("config", Kind::Opaque),
    ("audit", Kind::Opaque),
    ("runtime", Kind::Opaque),
];
const WAIT_FOR_INPUT_FIELDS: FieldTable = &[
    ("prompt", Kind::Opaque),
    ("timeout", Kind::Opaque),
    ("escalation_handler", Kind::Opaque),
    ("choices", Kind::List(&Kind::HumanChoice)),
    ("store_as", Kind::Opaque),
    ("allow_comment", Kind::Opaque),
];
const ESCALATION_FIELDS: FieldTable = &[("handler", Kind::Opaque), ("params", Kind::Opaque)];
const STEP_COMPENSATION_FIELDS: FieldTable = &[
    ("handler", Kind::Opaque),
    ("params", Kind::Opaque),
    ("depends_on", Kind::Opaque),
    ("verification", Kind::Opaque),
];
const SLA_FIELDS: FieldTable = &[
    ("max_runtime", Kind::Opaque),
    ("max_step_runtime", Kind::Opaque),
];
const INTERCEPTORS_FIELDS: FieldTable = &[
    ("before_step", Kind::InterceptorAction),
    ("after_step", Kind::InterceptorAction),
    ("on_signal", Kind::InterceptorAction),
    ("on_complete", Kind::InterceptorAction),
    ("on_failure", Kind::InterceptorAction),
];
const INTERCEPTOR_ACTION_FIELDS: FieldTable =
    &[("handler", Kind::Opaque), ("params", Kind::Opaque)];
const HUMAN_CHOICE_FIELDS: FieldTable = &[("label", Kind::Opaque), ("value", Kind::Opaque)];
const ROUTE_FIELDS: FieldTable = &[("condition", Kind::Opaque), ("blocks", BLOCKS)];
const AB_VARIANT_FIELDS: FieldTable = &[
    ("name", Kind::Opaque),
    ("weight", Kind::Opaque),
    ("blocks", BLOCKS),
];
const SAGA_STEP_FIELDS: FieldTable = &[
    ("id", Kind::Opaque),
    ("action", Kind::Block),
    ("compensation", Kind::Block),
];

/// Field table for a block, by its (canonical or legacy) `type` tag.
fn block_fields(block_type: &str) -> Option<FieldTable> {
    Some(match block_type {
        "step" => STEP_FIELDS,
        "parallel" => PARALLEL_FIELDS,
        "race" => RACE_FIELDS,
        "loop" => LOOP_FIELDS,
        "for_each" => FOR_EACH_FIELDS,
        "router" => ROUTER_FIELDS,
        "try_catch" => TRY_CATCH_FIELDS,
        "sub_sequence" => SUB_SEQUENCE_FIELDS,
        "ab_split" | "a_b_split" => AB_SPLIT_FIELDS,
        "cancellation_scope" => CANCELLATION_SCOPE_FIELDS,
        "saga" => SAGA_FIELDS,
        _ => return None,
    })
}

fn table_for(kind: Kind, value: &Value) -> Option<FieldTable> {
    Some(match kind {
        Kind::Root => ROOT_FIELDS,
        Kind::Block => block_fields(value.get("type")?.as_str()?)?,
        Kind::Retry => RETRY_FIELDS,
        Kind::Delay => DELAY_FIELDS,
        Kind::SendWindow => SEND_WINDOW_FIELDS,
        Kind::ContextAccess => CONTEXT_ACCESS_FIELDS,
        Kind::WaitForInput => WAIT_FOR_INPUT_FIELDS,
        Kind::Escalation => ESCALATION_FIELDS,
        Kind::StepCompensation => STEP_COMPENSATION_FIELDS,
        Kind::Sla => SLA_FIELDS,
        Kind::Interceptors => INTERCEPTORS_FIELDS,
        Kind::InterceptorAction => INTERCEPTOR_ACTION_FIELDS,
        Kind::HumanChoice => HUMAN_CHOICE_FIELDS,
        Kind::Route => ROUTE_FIELDS,
        Kind::AbVariant => AB_VARIANT_FIELDS,
        Kind::SagaStep => SAGA_STEP_FIELDS,
        Kind::List(_) | Kind::Opaque => return None,
    })
}

/// Walk `value` as a node of kind `kind` at `path`, reporting keys that are
/// not in the applicable field table. Free-form subtrees are skipped.
fn scan_unknown_fields(value: &Value, kind: Kind, path: &str, out: &mut Vec<UnknownField>) {
    match kind {
        Kind::Opaque => {}
        Kind::List(inner) => {
            if let Some(items) = value.as_array() {
                for (i, item) in items.iter().enumerate() {
                    scan_unknown_fields(item, *inner, &format!("{path}[{i}]"), out);
                }
            }
        }
        _ => {
            let (Some(obj), Some(table)) = (value.as_object(), table_for(kind, value)) else {
                return;
            };
            let known: Vec<&str> = table.iter().map(|(name, _)| *name).collect();
            for (key, child) in obj {
                match table.iter().find(|(name, _)| name == key) {
                    Some((_, child_kind)) => {
                        let child_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        scan_unknown_fields(child, *child_kind, &child_path, out);
                    }
                    None => out.push(UnknownField {
                        path: path.to_owned(),
                        field: key.clone(),
                        suggestion: did_you_mean(key, &known).map(str::to_owned),
                    }),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{BlockId, Namespace, SequenceId, TenantId};
    use crate::interceptor::{InterceptorAction, InterceptorDef};
    use crate::sequence::*;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::time::Duration;

    fn minimal() -> Value {
        json!({
            "tenant_id": "t",
            "namespace": "default",
            "name": "s",
            "version": 1,
            "blocks": [
                {"type": "step", "id": "a", "handler": "noop"},
                {"type": "step", "id": "b", "handler": "noop"}
            ]
        })
    }

    #[test]
    fn minimal_document_parses_without_id_or_created_at() {
        let parsed = parse_sequence_value(minimal()).unwrap();
        assert!(parsed.unknown_fields.is_empty());
        assert_eq!(parsed.definition.blocks.len(), 2);
        // Defaults are stamped so the round-trip is complete.
        let out = serde_json::to_value(&parsed.definition).unwrap();
        assert!(out.get("id").is_some_and(|v| v.is_string()));
        assert!(out.get("created_at").is_some_and(|v| v.is_string()));
    }

    #[test]
    fn missing_handler_reports_block_path() {
        let mut v = minimal();
        v["blocks"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type": "step", "id": "c"}));
        let err = parse_sequence_value(v).unwrap_err();
        match &err {
            SequenceParseError::Invalid { path, message } => {
                assert_eq!(path, "blocks[2]", "{err}");
                assert!(message.contains("handler"), "{err}");
            }
            SequenceParseError::Json(_) => panic!("expected Invalid, got {err}"),
        }
        assert!(err.to_string().starts_with("blocks[2]: "), "{err}");
    }

    #[test]
    fn wrong_type_deep_in_tree_reports_full_path() {
        let mut v = minimal();
        v["blocks"][1]["retry"] = json!({"max_attempts": "three"});
        let err = parse_sequence_value(v).unwrap_err();
        assert!(
            err.to_string().starts_with("blocks[1].retry.max_attempts: "),
            "{err}"
        );
    }

    #[test]
    fn root_type_error_uses_dollar_path() {
        let err = parse_sequence_value(json!([])).unwrap_err();
        assert!(err.to_string().starts_with("$: "), "{err}");
    }

    #[test]
    fn invalid_json_text_is_json_error() {
        let err = parse_sequence_str("{not json").unwrap_err();
        assert!(matches!(err, SequenceParseError::Json(_)), "{err}");
        assert!(err.to_string().starts_with("invalid JSON: "));
    }

    #[test]
    fn unknown_root_field_is_reported_once_with_suggestion() {
        let mut v = minimal();
        v["namespce"] = json!("x");
        let parsed = parse_sequence_value(v).unwrap();
        assert_eq!(parsed.unknown_fields.len(), 1, "{:?}", parsed.unknown_fields);
        let u = &parsed.unknown_fields[0];
        assert_eq!(u.path, "");
        assert_eq!(u.field, "namespce");
        assert_eq!(u.suggestion.as_deref(), Some("namespace"));
        assert_eq!(
            format_unknown_field_warning(u),
            "unknown field \"namespce\" (did you mean \"namespace\"?)"
        );
    }

    #[test]
    fn unknown_step_field_is_reported_with_path_and_suggestion() {
        let mut v = minimal();
        v["blocks"][0]["retires"] = json!({"max_attempts": 3});
        let parsed = parse_sequence_value(v).unwrap();
        assert_eq!(parsed.unknown_fields.len(), 1, "{:?}", parsed.unknown_fields);
        let u = &parsed.unknown_fields[0];
        assert_eq!(u.path, "blocks[0]");
        assert_eq!(u.field, "retires");
        assert_eq!(u.suggestion.as_deref(), Some("retry"));
        assert_eq!(
            format_unknown_field_warning(u),
            "unknown field \"retires\" at blocks[0] (did you mean \"retry\"?)"
        );
    }

    #[test]
    fn unknown_field_without_close_match_has_no_suggestion() {
        let mut v = minimal();
        v["blocks"][0]["completely_bogus_xyz"] = json!(1);
        let parsed = parse_sequence_value(v).unwrap();
        assert_eq!(parsed.unknown_fields.len(), 1);
        assert_eq!(parsed.unknown_fields[0].suggestion, None);
        assert_eq!(
            format_unknown_field_warning(&parsed.unknown_fields[0]),
            "unknown field \"completely_bogus_xyz\" at blocks[0]"
        );
    }

    #[test]
    fn unknown_fields_inside_nested_blocks_and_retry_are_found() {
        let v = json!({
            "tenant_id": "t", "namespace": "default", "name": "s", "version": 1,
            "blocks": [{
                "type": "router", "id": "r",
                "routes": [{
                    "condition": "true",
                    "blocks": [{
                        "type": "step", "id": "s", "handler": "noop",
                        "retry": {"max_attempts": 2, "max_backof": 5}
                    }]
                }],
                "defualt": []
            }]
        });
        let parsed = parse_sequence_value(v).unwrap();
        let mut found: Vec<(String, String, Option<String>)> = parsed
            .unknown_fields
            .iter()
            .map(|u| (u.path.clone(), u.field.clone(), u.suggestion.clone()))
            .collect();
        found.sort();
        assert_eq!(
            found,
            vec![
                (
                    "blocks[0]".to_string(),
                    "defualt".to_string(),
                    Some("default".to_string())
                ),
                (
                    "blocks[0].routes[0].blocks[0].retry".to_string(),
                    "max_backof".to_string(),
                    Some("max_backoff".to_string())
                ),
            ]
        );
    }

    #[test]
    fn free_form_params_are_not_scanned() {
        let mut v = minimal();
        v["blocks"][0]["params"] = json!({"anything": {"goes": ["here"]}, "retires": 1});
        v["input_schema"] = json!({"type": "object", "properties": {"foo": {}}});
        let parsed = parse_sequence_value(v).unwrap();
        assert!(parsed.unknown_fields.is_empty(), "{:?}", parsed.unknown_fields);
    }

    #[test]
    fn legacy_ab_split_tag_is_scanned_with_the_ab_split_table() {
        let v = json!({
            "tenant_id": "t", "namespace": "default", "name": "s", "version": 1,
            "blocks": [{
                "type": "a_b_split", "id": "ab",
                "variants": [
                    {"name": "a", "weight": 1, "blocks": [], "wieght": 2},
                    {"name": "b", "weight": 1, "blocks": []}
                ]
            }]
        });
        let parsed = parse_sequence_value(v).unwrap();
        assert_eq!(parsed.unknown_fields.len(), 1);
        assert_eq!(parsed.unknown_fields[0].path, "blocks[0].variants[0]");
        assert_eq!(parsed.unknown_fields[0].suggestion.as_deref(), Some("weight"));
    }

    // ─── schema_version ───

    #[test]
    fn schema_version_current_is_accepted_and_consumed() {
        let mut v = minimal();
        v["schema_version"] = json!(SEQUENCE_SCHEMA_VERSION);
        let parsed = parse_sequence_value(v).unwrap();
        assert!(parsed.unknown_fields.is_empty(), "{:?}", parsed.unknown_fields);
    }

    #[test]
    fn schema_version_newer_is_rejected_with_code() {
        let mut v = minimal();
        v["schema_version"] = json!(SEQUENCE_SCHEMA_VERSION + 1);
        let err = parse_sequence_value(v).unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("schema_version: "), "{msg}");
        assert!(msg.contains(SEQ_SCHEMA_VERSION_UNSUPPORTED), "{msg}");
    }

    #[test]
    fn schema_version_must_be_positive_integer() {
        for bad in [json!("1"), json!(0), json!(-1), json!(1.5)] {
            let mut v = minimal();
            v["schema_version"] = bad.clone();
            let err = parse_sequence_value(v).unwrap_err();
            assert!(err.to_string().starts_with("schema_version: "), "{bad}: {err}");
        }
    }

    // ─── field tables stay in sync with the structs ───

    fn keys(v: &Value) -> BTreeSet<String> {
        v.as_object().unwrap().keys().cloned().collect()
    }

    fn table_keys(table: FieldTable) -> BTreeSet<String> {
        table.iter().map(|(k, _)| (*k).to_string()).collect()
    }

    fn assert_table_matches(table: FieldTable, sample: &Value, extra: &[&str]) {
        let mut expected = keys(sample);
        for e in extra {
            expected.insert((*e).to_string());
        }
        assert_eq!(
            table_keys(table),
            expected,
            "field table out of sync with the struct's serialized form"
        );
    }

    fn full_step() -> StepDef {
        StepDef {
            id: BlockId::new("s"),
            handler: "noop".into(),
            params: json!({}),
            delay: Some(full_delay()),
            retry: Some(full_retry()),
            timeout: Some(Duration::from_secs(1)),
            rate_limit_key: Some("k".into()),
            send_window: Some(SendWindow {
                start_hour: 9,
                end_hour: 17,
                days: vec![0],
            }),
            context_access: Some(ContextAccess {
                data: FieldAccess::ALL,
                config: true,
                audit: false,
                runtime: false,
            }),
            cancellable: true,
            wait_for_input: Some(full_wait_for_input()),
            queue_name: Some("q".into()),
            deadline: Some(Duration::from_secs(1)),
            on_deadline_breach: Some(EscalationDef {
                handler: "h".into(),
                params: json!({}),
            }),
            fallback_handler: Some("f".into()),
            cache_key: Some("c".into()),
            output_schema: Some(json!({})),
            when: Some("true".into()),
            compensation: Some(StepCompensation {
                handler: "h".into(),
                params: json!({}),
                depends_on: vec![BlockId::new("x")],
                verification: CompensationVerificationPolicy::default(),
            }),
        }
    }

    fn full_retry() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(2),
            backoff_multiplier: 2.0,
            jitter: Some(true),
            retry_if: Some("true".into()),
            non_retryable_codes: Some(vec!["x".into()]),
        }
    }

    fn full_delay() -> DelaySpec {
        DelaySpec {
            duration: Duration::from_secs(1),
            business_days_only: true,
            jitter: Some(Duration::from_secs(1)),
            holidays: vec!["2026-01-01".into()],
            fire_at_local: Some("2026-01-01T00:00:00".into()),
            timezone: Some("UTC".into()),
        }
    }

    fn full_wait_for_input() -> HumanInputDef {
        HumanInputDef {
            prompt: "p".into(),
            timeout: Some(Duration::from_secs(1)),
            escalation_handler: Some("e".into()),
            choices: Some(vec![HumanChoice {
                label: "l".into(),
                value: "v".into(),
            }]),
            store_as: Some("s".into()),
            allow_comment: true,
        }
    }

    fn step_block() -> BlockDefinition {
        BlockDefinition::Step(Box::new(full_step()))
    }

    fn to_json<T: serde::Serialize>(t: &T) -> Value {
        serde_json::to_value(t).unwrap()
    }

    #[test]
    fn root_table_matches_sequence_definition() {
        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("default"),
            name: "s".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![step_block()],
            interceptors: Some(InterceptorDef::default()),
            input_schema: Some(json!({})),
            sla: Some(SlaPolicy::default()),
            on_failure: Some(vec![]),
            on_cancel: Some(vec![]),
            created_at: chrono::Utc::now(),
        };
        assert_table_matches(ROOT_FIELDS, &to_json(&seq), &["schema_version"]);
    }

    #[test]
    fn block_tables_match_block_structs() {
        assert_table_matches(STEP_FIELDS, &to_json(&step_block()), &[]);
        assert_table_matches(
            PARALLEL_FIELDS,
            &to_json(&BlockDefinition::Parallel(Box::new(ParallelDef {
                id: BlockId::new("p"),
                branches: vec![],
            }))),
            &[],
        );
        assert_table_matches(
            RACE_FIELDS,
            &to_json(&BlockDefinition::Race(Box::new(RaceDef {
                id: BlockId::new("r"),
                branches: vec![],
                semantics: RaceSemantics::default(),
            }))),
            &[],
        );
        assert_table_matches(
            LOOP_FIELDS,
            &to_json(&BlockDefinition::Loop(Box::new(LoopDef {
                id: BlockId::new("l"),
                condition: "true".into(),
                body: vec![],
                max_iterations: 1,
                break_on: Some("false".into()),
                continue_on_error: false,
                poll_interval: Some(1),
                retain_iterations: Some(1),
            }))),
            &[],
        );
        assert_table_matches(
            FOR_EACH_FIELDS,
            &to_json(&BlockDefinition::ForEach(Box::new(ForEachDef {
                id: BlockId::new("f"),
                collection: "data.items".into(),
                item_var: "item".into(),
                body: vec![],
                max_iterations: 1,
                retain_iterations: Some(1),
            }))),
            &[],
        );
        assert_table_matches(
            ROUTER_FIELDS,
            &to_json(&BlockDefinition::Router(Box::new(RouterDef {
                id: BlockId::new("r"),
                routes: vec![],
                default: Some(vec![]),
            }))),
            &[],
        );
        assert_table_matches(
            TRY_CATCH_FIELDS,
            &to_json(&BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("t"),
                try_block: vec![],
                catch_block: vec![],
                finally_block: Some(vec![]),
            }))),
            &[],
        );
        assert_table_matches(
            SUB_SEQUENCE_FIELDS,
            &to_json(&BlockDefinition::SubSequence(Box::new(SubSequenceDef {
                id: BlockId::new("s"),
                sequence_name: "child".into(),
                version: Some(1),
                input: json!({}),
            }))),
            &[],
        );
        assert_table_matches(
            AB_SPLIT_FIELDS,
            &to_json(&BlockDefinition::ABSplit(Box::new(ABSplitDef {
                id: BlockId::new("ab"),
                variants: vec![],
            }))),
            &[],
        );
        assert_table_matches(
            CANCELLATION_SCOPE_FIELDS,
            &to_json(&BlockDefinition::CancellationScope(Box::new(
                CancellationScopeDef {
                    id: BlockId::new("c"),
                    blocks: vec![],
                },
            ))),
            &[],
        );
        assert_table_matches(
            SAGA_FIELDS,
            &to_json(&BlockDefinition::Saga(Box::new(SagaDef {
                id: BlockId::new("sg"),
                steps: vec![],
            }))),
            &[],
        );
    }

    #[test]
    fn nested_tables_match_nested_structs() {
        assert_table_matches(RETRY_FIELDS, &to_json(&full_retry()), &[]);
        assert_table_matches(DELAY_FIELDS, &to_json(&full_delay()), &[]);
        assert_table_matches(
            SEND_WINDOW_FIELDS,
            &to_json(&full_step().send_window.unwrap()),
            &[],
        );
        assert_table_matches(
            CONTEXT_ACCESS_FIELDS,
            &to_json(&full_step().context_access.unwrap()),
            &[],
        );
        assert_table_matches(WAIT_FOR_INPUT_FIELDS, &to_json(&full_wait_for_input()), &[]);
        assert_table_matches(
            ESCALATION_FIELDS,
            &to_json(&full_step().on_deadline_breach.unwrap()),
            &[],
        );
        assert_table_matches(
            STEP_COMPENSATION_FIELDS,
            &to_json(&full_step().compensation.unwrap()),
            &[],
        );
        assert_table_matches(
            SLA_FIELDS,
            &to_json(&SlaPolicy {
                max_runtime: Some(Duration::from_secs(1)),
                max_step_runtime: Some(Duration::from_secs(1)),
            }),
            &[],
        );
        let action = InterceptorAction {
            handler: "h".into(),
            params: json!({}),
        };
        assert_table_matches(
            INTERCEPTORS_FIELDS,
            &to_json(&InterceptorDef {
                before_step: Some(action.clone()),
                after_step: Some(action.clone()),
                on_signal: Some(action.clone()),
                on_complete: Some(action.clone()),
                on_failure: Some(action.clone()),
            }),
            &[],
        );
        assert_table_matches(INTERCEPTOR_ACTION_FIELDS, &to_json(&action), &[]);
        assert_table_matches(
            HUMAN_CHOICE_FIELDS,
            &to_json(&HumanChoice {
                label: "l".into(),
                value: "v".into(),
            }),
            &[],
        );
        assert_table_matches(
            ROUTE_FIELDS,
            &to_json(&Route {
                condition: "true".into(),
                blocks: vec![],
            }),
            &[],
        );
        assert_table_matches(
            AB_VARIANT_FIELDS,
            &to_json(&ABVariant {
                name: "a".into(),
                weight: 1,
                blocks: vec![],
            }),
            &[],
        );
        assert_table_matches(
            SAGA_STEP_FIELDS,
            &to_json(&SagaStep {
                id: BlockId::new("st"),
                action: Box::new(step_block()),
                compensation: Some(Box::new(step_block())),
            }),
            &[],
        );
    }
}
