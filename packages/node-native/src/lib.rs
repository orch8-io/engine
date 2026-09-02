use napi_derive::napi;

/// Strictly decode and validate a sequence using the same Rust types as the server.
#[napi]
pub fn validate_sequence_json(input: String) -> napi::Result<String> {
    let value = serde_json::from_str(&input)
        .map_err(|error| napi::Error::from_reason(format!("invalid JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    sequence
        .validate()
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    serde_json::to_string(&sequence).map_err(|error| napi::Error::from_reason(error.to_string()))
}

#[napi]
pub fn sequence_schema_version() -> u32 {
    orch8_types::sequence::SEQUENCE_SCHEMA_VERSION
}

/// Run a workflow in an isolated, in-memory Orch8 engine.
#[napi]
pub async fn run_sequence_json(
    sequence_json: String,
    input_json: Option<String>,
    max_ticks: Option<u32>,
) -> napi::Result<String> {
    let value = serde_json::from_str(&sequence_json)
        .map_err(|error| napi::Error::from_reason(format!("invalid sequence JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    let input = input_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| napi::Error::from_reason(format!("invalid input JSON: {error}")))?
        .unwrap_or_else(|| serde_json::json!({}));
    let result = orch8::run_sequence_once(sequence, input, max_ticks.unwrap_or(1_000))
        .await
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    serde_json::to_string(&result).map_err(|error| napi::Error::from_reason(error.to_string()))
}
