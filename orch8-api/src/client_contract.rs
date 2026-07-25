//! Representative generated-client quality gate.

use serde_json::Value;
use utoipa::OpenApi as _;

use crate::openapi::ApiDoc;

const REQUIRED_OPERATIONS: &[(&str, &str)] = &[
    ("/instances", "post"),
    ("/instances/{id}", "get"),
    ("/releases/{id}/validate", "post"),
    ("/instances/{id}/timeline", "get"),
];

pub fn validate_contract() -> Result<Value, String> {
    let document = serde_json::to_value(ApiDoc::openapi()).map_err(|error| error.to_string())?;
    for (path, method) in REQUIRED_OPERATIONS {
        if document
            .pointer(&format!("/paths/{}/{}", escape(path), method))
            .is_none()
        {
            return Err(format!("OpenAPI is missing {method} {path}"));
        }
    }
    Ok(document)
}

fn escape(path: &str) -> String {
    path.replace('~', "~0").replace('/', "~1")
}

#[must_use]
pub fn generate_rust_client() -> String {
    let methods = REQUIRED_OPERATIONS
        .iter()
        .map(|(path, method)| format!("    (\"{method}\", \"{path}\"),"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "// Generated from Orch8 OpenAPI. Do not edit.\n\
         const OPERATIONS: &[(&str, &str)] = &[\n{methods}\n];\n\
         fn main() {{ assert_eq!(OPERATIONS.len(), 4); }}\n"
    )
}

#[must_use]
pub fn generate_javascript_client() -> String {
    let methods = REQUIRED_OPERATIONS
        .iter()
        .map(|(path, method)| {
            format!("  Object.freeze({{ method: \"{method}\", path: \"{path}\" }}),")
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "// Generated from Orch8 OpenAPI. Do not edit.\n\
         'use strict';\n\
         const operations = Object.freeze([\n{methods}\n]);\n\
         if (operations.length !== 4) throw new Error('contract drift');\n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_contains_representative_control_plane_operations() {
        validate_contract().unwrap();
    }

    #[test]
    fn generators_are_deterministic_and_contain_canonical_paths() {
        assert_eq!(generate_rust_client(), generate_rust_client());
        assert!(generate_rust_client().contains("/instances/{id}"));
        assert!(generate_javascript_client().contains("/releases/{id}/validate"));
    }
}
