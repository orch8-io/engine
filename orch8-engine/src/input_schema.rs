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
