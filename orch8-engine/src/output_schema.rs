use serde_json::Value;

/// Format validation failures consistently in the scheduler and the preview.
pub(crate) fn violation(validator: &jsonschema::Validator, output: &Value) -> Option<String> {
    let errors: Vec<String> = validator
        .iter_errors(output)
        .map(|error| {
            let path = error.instance_path().to_string();
            if path.is_empty() {
                error.to_string()
            } else {
                format!("at {path}: {error}")
            }
        })
        .collect();
    (!errors.is_empty()).then(|| {
        format!(
            "output failed output_schema validation: {}",
            errors.join("; ")
        )
    })
}
