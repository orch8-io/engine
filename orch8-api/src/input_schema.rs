//! Sequence `input_schema` and step `output_schema` validation.
//!
//! A sequence may declare an optional JSON Schema (`input_schema`). When
//! present, `context.data` of every new instance is validated against it at
//! create time — a failing create is rejected with 422 before the instance is
//! persisted, so a malformed payload never reaches the engine. The schema also
//! doubles as the contract the dashboard renders an input form from.
//!
//! Steps may declare an optional `output_schema`. Well-formedness is checked
//! at sequence-create time; actual output validation happens in the engine
//! after the handler returns.

use crate::error::ApiError;

/// Validate that `schema` is itself a usable JSON Schema. Called at
/// sequence-create time so a broken schema is caught when the sequence is
/// authored, not on the first instance.
pub fn validate_schema_is_well_formed(schema: &serde_json::Value) -> Result<(), ApiError> {
    if !schema.is_object() {
        return Err(ApiError::InvalidArgument(
            "input_schema must be a JSON object".into(),
        ));
    }
    jsonschema::validator_for(schema).map_err(|e| {
        ApiError::InvalidArgument(format!("input_schema is not a valid JSON Schema: {e}"))
    })?;
    Ok(())
}

/// Validate instance `data` against a sequence's optional `input_schema`.
/// `None` schema (or absent) always passes. On failure returns a 422 with
/// pointer paths for each violation.
pub fn validate_input(
    input_schema: Option<&serde_json::Value>,
    data: &serde_json::Value,
) -> Result<(), ApiError> {
    let Some(schema) = input_schema else {
        return Ok(());
    };
    // A schema that fails to compile here should have been rejected at
    // sequence-create. If it somehow slipped through, surface the error so
    // operators know the stored schema is corrupt rather than silently letting
    // invalid data through.
    let validator = jsonschema::validator_for(schema).map_err(|e| {
        ApiError::UnprocessableEntity(format!("stored input_schema is invalid: {e}"))
    })?;
    let errors: Vec<String> = validator
        .iter_errors(data)
        .map(|e| {
            let path = e.instance_path().to_string();
            if path.is_empty() {
                e.to_string()
            } else {
                format!("at {path}: {e}")
            }
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ApiError::UnprocessableEntity(format!(
            "context.data failed input_schema validation: {}",
            errors.join("; ")
        )))
    }
}

/// Validate that a step-level `output_schema` is a well-formed JSON Schema.
/// Called at sequence-create time so a broken schema is caught when the
/// sequence is authored.
pub fn validate_output_schema_is_well_formed(schema: &serde_json::Value) -> Result<(), ApiError> {
    if !schema.is_object() {
        return Err(ApiError::InvalidArgument(
            "output_schema must be a JSON object".into(),
        ));
    }
    jsonschema::validator_for(schema).map_err(|e| {
        ApiError::InvalidArgument(format!("output_schema is not a valid JSON Schema: {e}"))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "email": { "type": "string" },
                "age": { "type": "integer", "minimum": 0 }
            },
            "required": ["email"]
        })
    }

    #[test]
    fn none_schema_always_passes() {
        assert!(validate_input(None, &json!({ "anything": true })).is_ok());
    }

    #[test]
    fn valid_data_passes() {
        let s = schema();
        assert!(validate_input(Some(&s), &json!({ "email": "a@b.com", "age": 30 })).is_ok());
    }

    #[test]
    fn missing_required_field_fails() {
        let s = schema();
        let err = validate_input(Some(&s), &json!({ "age": 30 })).unwrap_err();
        assert!(matches!(err, ApiError::UnprocessableEntity(_)));
    }

    #[test]
    fn wrong_type_fails() {
        let s = schema();
        let err = validate_input(Some(&s), &json!({ "email": "a@b.com", "age": -1 })).unwrap_err();
        assert!(matches!(err, ApiError::UnprocessableEntity(_)));
    }

    #[test]
    fn well_formed_check_rejects_non_object() {
        assert!(validate_schema_is_well_formed(&json!("nope")).is_err());
    }

    #[test]
    fn well_formed_check_accepts_valid_schema() {
        assert!(validate_schema_is_well_formed(&schema()).is_ok());
    }
}
