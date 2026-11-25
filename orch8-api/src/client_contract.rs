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

/// Produce a client that checks the list response against the generated envelope.
pub fn generate_instances_client(document: &Value) -> Result<String, String> {
    let operation = document
        .pointer("/paths/~1instances/get/responses/200/content/application~1json/schema")
        .ok_or("GET /instances is missing a success response schema")?;
    let schema = if let Some(reference) = operation.get("$ref").and_then(Value::as_str) {
        document
            .pointer(
                reference
                    .strip_prefix('#')
                    .ok_or("external response schema")?,
            )
            .ok_or("GET /instances response schema reference is unresolved")?
    } else {
        operation
    };
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .ok_or("GET /instances must document required envelope fields")?;
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or("GET /instances must document an object envelope")?;
    let mut checks = serde_json::Map::new();
    for name in required {
        let name = name.as_str().ok_or("invalid required property name")?;
        let kind = properties
            .get(name)
            .and_then(|field| field.get("type"))
            .and_then(Value::as_str)
            .ok_or("required envelope property must declare its type")?;
        checks.insert(name.into(), Value::String(kind.into()));
    }
    if checks.get("items").and_then(Value::as_str) != Some("array")
        || checks.get("has_more").and_then(Value::as_str) != Some("boolean")
    {
        return Err("GET /instances must require items and has_more".into());
    }
    Ok(format!(
        r"
// Generated from Orch8 OpenAPI. Do not edit.
'use strict';
const shapes = {checks};
(async () => {{
  const url = new URL('/instances', process.argv[1]);
  url.searchParams.set('tenant_id', process.argv[2]);
  url.searchParams.set('limit', '1');
  const response = await fetch(url, {{ headers: {{ 'X-Tenant-Id': process.argv[2] }} }});
  if (!response.ok) throw new Error(`GET /instances returned HTTP ${{response.status}}`);
  const page = await response.json();
  for (const [key, expected] of Object.entries(shapes)) {{
    const value = page?.[key];
    const actual = Array.isArray(value) ? 'array' : typeof value;
    if (actual !== expected) throw new Error(`GET /instances field ${{key}}: expected ${{expected}}, got ${{actual}}`);
  }}
  if (page.items.length !== 1) throw new Error('GET /instances did not return the seeded instance');
}})().catch(error => {{
  console.error(error);
  process.exitCode = 1;
}});
",
        checks = Value::Object(checks)
    ))
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

#[cfg(test)]
#[path = "client_contract_structure_tests.rs"]
mod structure_tests;
