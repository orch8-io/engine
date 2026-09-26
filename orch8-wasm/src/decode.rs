//! Strict sequence decoding with playground defaults.
//!
//! The server assigns `id`, `tenant_id`, `namespace`, `version` and
//! `created_at` when a sequence is created; people pasting a workflow into
//! the playground usually leave them out. We fill fixed placeholders for the
//! missing ones (and say which) before running the exact same strict decoder
//! and validator the server uses.

use orch8_types::sequence::{SequenceDefinition, deserialize_sequence_strict};
use serde_json::{Value, json};

/// Placeholder values for server-assigned fields. Fixed (not random) so a
/// shared playground link always decodes to the same definition.
const DEFAULTS: &[(&str, &str)] = &[
    ("id", "00000000-0000-7000-8000-000000000000"),
    ("tenant_id", "playground"),
    ("namespace", "default"),
    ("name", "playground"),
    ("created_at", "2026-01-01T00:00:00Z"),
];

/// A decoded, validated sequence plus the defaults that were filled in.
#[derive(Debug)]
pub struct Decoded {
    pub sequence: SequenceDefinition,
    pub defaults_applied: Vec<String>,
}

/// Parse JSON text, fill playground defaults, strictly decode and validate.
pub fn decode(input: &str) -> Result<Decoded, String> {
    let mut value: Value =
        serde_json::from_str(input).map_err(|error| format!("invalid JSON: {error}"))?;
    let Some(object) = value.as_object_mut() else {
        return Err("a sequence must be a JSON object".into());
    };
    let mut defaults_applied = Vec::new();
    for (field, default) in DEFAULTS {
        if !object.contains_key(*field) {
            object.insert((*field).to_string(), json!(default));
            defaults_applied.push((*field).to_string());
        }
    }
    if !object.contains_key("version") {
        object.insert("version".into(), json!(1));
        defaults_applied.push("version".into());
    }
    let sequence = deserialize_sequence_strict(&value).map_err(|error| error.to_string())?;
    sequence.validate().map_err(|error| error.to_string())?;
    Ok(Decoded {
        sequence,
        defaults_applied,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_server_assigned_fields() {
        let decoded = decode(r#"{"blocks":[{"type":"step","id":"a","handler":"noop"}]}"#).unwrap();
        assert_eq!(decoded.sequence.name, "playground");
        assert!(decoded.defaults_applied.contains(&"tenant_id".to_string()));
        assert!(decoded.defaults_applied.contains(&"version".to_string()));
    }

    #[test]
    fn keeps_explicit_fields() {
        let decoded = decode(
            r#"{"name":"mine","version":3,"blocks":[{"type":"step","id":"a","handler":"noop"}]}"#,
        )
        .unwrap();
        assert_eq!(decoded.sequence.name, "mine");
        assert_eq!(decoded.sequence.version, 3);
        assert!(!decoded.defaults_applied.contains(&"name".to_string()));
    }

    #[test]
    fn rejects_unknown_fields_like_the_server() {
        let err = decode(r#"{"blocks":[{"type":"step","id":"a","handler":"noop","bogus":1}]}"#)
            .unwrap_err();
        assert!(err.contains("bogus"), "{err}");
    }

    #[test]
    fn rejects_duplicate_ids() {
        let err = decode(
            r#"{"blocks":[{"type":"step","id":"a","handler":"noop"},{"type":"step","id":"a","handler":"noop"}]}"#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate"), "{err}");
    }

    #[test]
    fn rejects_non_objects_and_bad_json() {
        assert!(decode("[]").is_err());
        assert!(decode("{").unwrap_err().starts_with("invalid JSON"));
    }
}
