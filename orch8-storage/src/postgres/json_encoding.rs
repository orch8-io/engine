//! Lossless JSON text for values JSONB cannot faithfully represent.
use orch8_types::{context::ExecutionContext, error::StorageError};
use serde_json::Value;

const CONTEXT_ENCODING: &str = "__orch8_context_encoding";
pub(super) const JSON_TEXT: &str = "json_text_v1";

/// Ordinary values keep their existing indexed JSONB representation.
pub(super) fn needs_text(value: &Value) -> bool {
    match value {
        Value::String(value) => value.contains('\0'),
        Value::Number(value) => value.is_f64(),
        Value::Array(values) => values.iter().any(needs_text),
        Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.contains('\0') || needs_text(value)),
        _ => false,
    }
}

pub(super) struct Encoded {
    pub value: Value,
    pub encoding: Option<&'static str>,
}
pub(super) fn encode(value: &Value) -> Result<Encoded, StorageError> {
    if needs_text(value) {
        Ok(Encoded {
            value: Value::String(serde_json::to_string(value)?),
            encoding: Some(JSON_TEXT),
        })
    } else {
        Ok(Encoded {
            value: value.clone(),
            encoding: None,
        })
    }
}
pub(super) fn encode_owned(value: Value) -> Result<Encoded, StorageError> {
    if needs_text(&value) {
        Ok(Encoded {
            value: Value::String(serde_json::to_string(&value)?),
            encoding: Some(JSON_TEXT),
        })
    } else {
        Ok(Encoded {
            value,
            encoding: None,
        })
    }
}
pub(super) fn decode(value: Value, encoding: Option<&str>) -> Result<Value, StorageError> {
    match encoding {
        None => Ok(value),
        Some(JSON_TEXT) => Ok(serde_json::from_str(value.as_str().ok_or_else(|| {
            StorageError::Constraint("encoded JSON payload must be text".into())
        })?)?),
        Some(_) => Err(StorageError::Constraint(
            "unsupported native JSON encoding".into(),
        )),
    }
}

/// Runtime stays structured for claim and ownership predicates. Application
/// sections are independently encoded, without reserving any application key.
pub(super) fn encode_context(context: &ExecutionContext) -> Result<Value, StorageError> {
    let mut stored = serde_json::to_value(context)?;
    let mut sections = Vec::new();
    for section in ["data", "config", "audit"] {
        let encoded = encode_owned(stored[section].take())?;
        stored[section] = encoded.value;
        if encoded.encoding.is_some() {
            sections.push(section);
        }
    }
    if !sections.is_empty() {
        stored[CONTEXT_ENCODING] = serde_json::json!({"codec":JSON_TEXT,"sections":sections});
    }
    Ok(stored)
}
pub(super) fn decode_context(mut stored: Value) -> Result<ExecutionContext, StorageError> {
    if let Some(encoding) = stored.get(CONTEXT_ENCODING).cloned() {
        if encoding["codec"].as_str() != Some(JSON_TEXT) {
            return Err(StorageError::Constraint(
                "unsupported native context encoding".into(),
            ));
        }
        let sections = encoding["sections"]
            .as_array()
            .ok_or_else(|| StorageError::Constraint("invalid native context encoding".into()))?;
        let mut seen = std::collections::HashSet::new();
        for section in sections {
            let section = section
                .as_str()
                .filter(|section| matches!(*section, "data" | "config" | "audit"))
                .ok_or_else(|| StorageError::Constraint("invalid native context section".into()))?;
            if !seen.insert(section) {
                return Err(StorageError::Constraint(
                    "duplicate native context section".into(),
                ));
            }
            stored[section] = decode(stored[section].take(), Some(JSON_TEXT))?;
        }
        stored
            .as_object_mut()
            .ok_or_else(|| StorageError::Constraint("invalid native context".into()))?
            .remove(CONTEXT_ENCODING);
    }
    Ok(serde_json::from_value(stored)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn context_roundtrip_preserves_json_text_and_application_marker_keys() {
        let value = serde_json::json!({"__orch8_context_encoding":{"codec":"application-value"},"nul\0key":"x\0λ","negative_zero":-0.0,"float":1.0,"nested":[1e18]});
        let context = ExecutionContext {
            data: value.clone(),
            config: value.clone(),
            audit: Vec::new(),
            ..ExecutionContext::default()
        };
        let stored = encode_context(&context).unwrap();
        assert_eq!(
            stored["runtime"],
            serde_json::to_value(&context.runtime).unwrap()
        );
        assert!(!needs_text(&stored));
        let decoded = decode_context(stored).unwrap();
        assert_eq!(
            serde_json::to_string(&decoded).unwrap(),
            serde_json::to_string(&context).unwrap()
        );
    }
    #[test]
    fn ordinary_payloads_keep_their_existing_representation() {
        let value = serde_json::json!({"array":[1,true,null,"λ"]});
        let encoded = encode(&value).unwrap();
        assert!(encoded.encoding.is_none());
        assert_eq!(encoded.value, value);
        assert_eq!(decode(encoded.value, None).unwrap(), value);
    }
    #[test]
    fn malformed_or_unknown_codecs_fail_closed() {
        assert!(decode(serde_json::json!({}), Some(JSON_TEXT)).is_err());
        assert!(decode(Value::String("not json".into()), Some(JSON_TEXT)).is_err());
        assert!(decode(Value::Null, Some("future")).is_err());
        for marker in [
            serde_json::json!({"codec":"future","sections":[]}),
            serde_json::json!({"codec":JSON_TEXT,"sections":["runtime"]}),
            serde_json::json!({"codec":JSON_TEXT,"sections":["data","data"]}),
        ] {
            let mut context = serde_json::json!({"data":"null"});
            context[CONTEXT_ENCODING] = marker;
            assert!(decode_context(context).is_err());
        }
    }
}
