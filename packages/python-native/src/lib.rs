use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn validate_sequence_json(input: &str) -> PyResult<String> {
    let value = serde_json::from_str(input)
        .map_err(|error| PyValueError::new_err(format!("invalid JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    sequence
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    serde_json::to_string(&sequence).map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
fn sequence_schema_version() -> u32 {
    orch8_types::sequence::SEQUENCE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(signature = (sequence_json, input_json=None, max_ticks=1000))]
fn run_sequence_json(
    py: Python<'_>,
    sequence_json: &str,
    input_json: Option<&str>,
    max_ticks: u32,
) -> PyResult<String> {
    let value = serde_json::from_str(sequence_json)
        .map_err(|error| PyValueError::new_err(format!("invalid sequence JSON: {error}")))?;
    let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let input = input_json
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| PyValueError::new_err(format!("invalid input JSON: {error}")))?
        .unwrap_or_else(|| serde_json::json!({}));

    py.detach(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        let result = runtime
            .block_on(orch8::run_sequence_once(sequence, input, max_ticks))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        serde_json::to_string(&result).map_err(|error| PyValueError::new_err(error.to_string()))
    })
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(validate_sequence_json, module)?)?;
    module.add_function(wrap_pyfunction!(sequence_schema_version, module)?)?;
    module.add_function(wrap_pyfunction!(run_sequence_json, module)?)?;
    Ok(())
}
