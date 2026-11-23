//! Input validation shared with trusted embedding admission paths.
use crate::error::EngineError;
use serde_json::Value;

/// Validate outside a storage writer reservation. Errors omit private input and
/// schema values; schema resolution has no network/file features enabled.
pub fn validate(schema: Option<&Value>, input: &Value) -> Result<(), EngineError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let validator = jsonschema::validator_for(schema).map_err(|_| {
        EngineError::InvalidConfig("stored workflow input schema is invalid".into())
    })?;
    if !validator.is_valid(input) {
        return Err(EngineError::InvalidConfig(
            "workflow input does not match its schema".into(),
        ));
    }
    Ok(())
}

/// Validate the immutable configuration for a new activation. Avoid including
/// raw tenant configuration values in errors or logs.
pub fn validate_config(schema: Option<&Value>, config: &Value) -> Result<(), EngineError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let validator = crate::handlers::step::compiled_schema_validator(schema).map_err(|_| {
        EngineError::InvalidConfig("stored workflow configuration schema is invalid".into())
    })?;
    if !validator.is_valid(config) {
        return Err(EngineError::InvalidConfig(
            "context.config does not match its schema".into(),
        ));
    }
    Ok(())
}

pub fn validate_config_schema(schema: Option<&Value>) -> Result<(), EngineError> {
    if let Some(schema) = schema
        && (!schema.is_object()
            || crate::handlers::step::compiled_schema_validator(schema).is_err())
    {
        return Err(EngineError::InvalidConfig(
            "x-orch8-config-schema must be a valid JSON Schema object".into(),
        ));
    }
    Ok(())
}

/// A state schema describes independently writable keys. Root constraints
/// involving multiple keys cannot be guaranteed by one-key mutations.
pub fn validate_state_schema(schema: Option<&Value>) -> Result<(), EngineError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let valid = schema.as_object().is_some_and(|object| {
        object.get("type").and_then(Value::as_str) == Some("object")
            && object.keys().all(|key| {
                matches!(
                    key.as_str(),
                    "$schema"
                        | "$id"
                        | "$defs"
                        | "definitions"
                        | "$comment"
                        | "title"
                        | "description"
                        | "type"
                        | "properties"
                        | "patternProperties"
                        | "propertyNames"
                        | "additionalProperties"
                )
            })
    });
    if !valid || crate::handlers::step::compiled_schema_validator(schema).is_err() {
        return Err(EngineError::InvalidConfig(
            "x-orch8-state-schema must be an object schema with independent key constraints".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_state_write(
    schema: &Value,
    key: &str,
    value: &Value,
) -> Result<(), orch8_types::error::StepError> {
    let valid = crate::handlers::step::compiled_schema_validator(schema)
        .is_ok_and(|validator| validator.is_valid(&serde_json::json!({key:value})));
    if !valid {
        return Err(orch8_types::error::StepError::Permanent {
            message: format!(
                "value for state key '{key}' does not match the sequence state schema"
            ),
            details: None,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn required_configuration_rejects_missing_fields_and_invalid_values() {
        let schema = json!({"type":"object","required":["level"],"additionalProperties":false,
            "properties":{"level":{"type":"integer"}}});
        assert!(validate_config(Some(&schema), &json!({"level":5})).is_ok());
        assert!(validate_config(Some(&schema), &json!({})).is_err());
        assert!(validate_config(Some(&schema), &json!({"level":"five"})).is_err());
        assert!(validate_config_schema(Some(&json!({"type":"nonsense"}))).is_err());
    }
    #[test]
    fn state_schema_validates_independent_keys_with_local_references() {
        let schema = json!({
            "type":"object","additionalProperties":false,
            "$defs":{"count":{"type":"integer"}},
            "properties":{"count":{"$ref":"#/$defs/count"}}
        });
        assert!(validate_state_schema(Some(&schema)).is_ok());
        assert!(validate_state_write(&schema, "count", &json!(1)).is_ok());
        assert!(validate_state_write(&schema, "count", &json!("1")).is_err());
        assert!(validate_state_write(&schema, "other", &json!(1)).is_err());
        assert!(
            validate_state_schema(Some(&json!({"type":"object","required":["count"]}))).is_err()
        );
    }
}
