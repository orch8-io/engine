//! Structural coverage for the generated-client contract: the shape of the
//! validated `OpenAPI` document and the exact scaffolding both client
//! generators emit around the required operations.
//!
//! Count contract: 28 independently named unit tests.

use super::*;

fn contract() -> Value {
    validate_contract().expect("contract must validate against the live OpenAPI document")
}

macro_rules! contract_op_case {
    ($name:ident, $path:expr, $method:expr) => {
        #[test]
        fn $name() {
            let document = contract();
            let pointer = format!("/paths/{}/{}", escape($path), $method);
            let operation = document
                .pointer(&pointer)
                .unwrap_or_else(|| panic!("missing {pointer}"));
            assert!(
                operation.is_object(),
                "{pointer} must be an operation object"
            );
            assert!(
                operation.get("responses").is_some(),
                "{pointer} must declare responses"
            );
        }
    };
}

contract_op_case!(
    coverage_contract_001_instance_create_operation_is_documented,
    "/instances",
    "post"
);
contract_op_case!(
    coverage_contract_002_instance_get_operation_is_documented,
    "/instances/{id}",
    "get"
);
contract_op_case!(
    coverage_contract_003_release_validate_operation_is_documented,
    "/releases/{id}/validate",
    "post"
);
contract_op_case!(
    coverage_contract_004_instance_timeline_operation_is_documented,
    "/instances/{id}/timeline",
    "get"
);

#[test]
fn coverage_contract_005_document_declares_an_openapi_3_version() {
    let document = contract();
    let version = document["openapi"]
        .as_str()
        .expect("openapi version string");
    assert!(
        version.starts_with("3."),
        "expected an OpenAPI 3.x document, got {version}"
    );
}

#[test]
fn coverage_contract_006_document_has_a_non_empty_title() {
    let document = contract();
    let title = document["info"]["title"].as_str().expect("info.title");
    assert!(!title.trim().is_empty());
}

#[test]
fn coverage_contract_007_document_covers_more_than_the_required_operations() {
    // The required four are a representative sample; if the whole surface
    // ever collapses to just them, the gate stopped being representative.
    let document = contract();
    let paths = document["paths"].as_object().expect("paths object");
    assert!(
        paths.len() > REQUIRED_OPERATIONS.len(),
        "expected more than {} paths, found {}",
        REQUIRED_OPERATIONS.len(),
        paths.len()
    );
}

#[test]
fn coverage_contract_008_validation_is_deterministic() {
    assert_eq!(contract(), contract());
}

#[test]
fn coverage_contract_009_every_required_path_escapes_to_a_single_pointer_segment() {
    for (path, _method) in REQUIRED_OPERATIONS {
        let escaped = escape(path);
        assert!(
            !escaped.contains('/'),
            "escaped pointer segment must not contain '/': {escaped}"
        );
    }
}

#[test]
fn coverage_contract_010_required_operations_have_no_duplicate_paths() {
    let mut paths: Vec<&str> = REQUIRED_OPERATIONS.iter().map(|(path, _)| *path).collect();
    paths.sort_unstable();
    paths.dedup();
    assert_eq!(paths.len(), REQUIRED_OPERATIONS.len());
}

// --- Rust client scaffolding. ---

#[test]
fn coverage_contract_011_rust_client_starts_with_the_generated_header() {
    assert!(generate_rust_client().starts_with("// Generated from Orch8 OpenAPI. Do not edit.\n"));
}

#[test]
fn coverage_contract_012_rust_client_declares_the_operations_table() {
    assert!(generate_rust_client().contains("const OPERATIONS: &[(&str, &str)] = &["));
}

#[test]
fn coverage_contract_013_rust_client_emits_exactly_one_entry_per_operation() {
    let entries = generate_rust_client()
        .lines()
        .filter(|line| line.starts_with("    (\""))
        .count();
    assert_eq!(entries, REQUIRED_OPERATIONS.len());
}

#[test]
fn coverage_contract_014_rust_client_self_checks_the_operation_count() {
    assert!(generate_rust_client().contains("assert_eq!(OPERATIONS.len(), 4)"));
}

#[test]
fn coverage_contract_015_rust_client_methods_are_lowercase() {
    let client = generate_rust_client();
    assert!(!client.contains("\"GET\""));
    assert!(!client.contains("\"POST\""));
}

#[test]
fn coverage_contract_016_rust_client_preserves_required_operation_order() {
    let client = generate_rust_client();
    let mut last = 0;
    for (path, _method) in REQUIRED_OPERATIONS {
        let position = client
            .find(&format!("\"{path}\""))
            .unwrap_or_else(|| panic!("missing {path}"));
        assert!(position >= last, "operations out of order at {path}");
        last = position;
    }
}

#[test]
fn coverage_contract_017_rust_client_ends_with_a_newline() {
    assert!(generate_rust_client().ends_with('\n'));
}

// --- JavaScript client scaffolding. ---

#[test]
fn coverage_contract_018_javascript_client_starts_with_the_generated_header() {
    assert!(
        generate_javascript_client().starts_with("// Generated from Orch8 OpenAPI. Do not edit.\n")
    );
}

#[test]
fn coverage_contract_019_javascript_client_enables_strict_mode() {
    assert!(generate_javascript_client().contains("'use strict';"));
}

#[test]
fn coverage_contract_020_javascript_client_freezes_the_operations_array() {
    assert!(generate_javascript_client().contains("const operations = Object.freeze(["));
}

#[test]
fn coverage_contract_021_javascript_client_freezes_every_entry() {
    let frozen = generate_javascript_client()
        .matches("Object.freeze({")
        .count();
    assert_eq!(frozen, REQUIRED_OPERATIONS.len());
}

#[test]
fn coverage_contract_022_javascript_client_throws_on_contract_drift() {
    let client = generate_javascript_client();
    assert!(client.contains("if (operations.length !== 4)"));
    assert!(client.contains("throw new Error('contract drift')"));
}

#[test]
fn coverage_contract_023_javascript_client_preserves_required_operation_order() {
    let client = generate_javascript_client();
    let mut last = 0;
    for (path, _method) in REQUIRED_OPERATIONS {
        let position = client
            .find(&format!("path: \"{path}\""))
            .unwrap_or_else(|| panic!("missing {path}"));
        assert!(position >= last, "operations out of order at {path}");
        last = position;
    }
}

#[test]
fn coverage_contract_024_javascript_client_ends_with_a_newline() {
    assert!(generate_javascript_client().ends_with('\n'));
}

// --- Cross-generator parity. ---

#[test]
fn coverage_contract_025_both_generators_cover_every_required_path() {
    for (path, _method) in REQUIRED_OPERATIONS {
        assert!(generate_rust_client().contains(path), "rust missing {path}");
        assert!(
            generate_javascript_client().contains(path),
            "javascript missing {path}"
        );
    }
}

#[test]
fn coverage_contract_026_both_generators_cover_every_required_method() {
    for (_path, method) in REQUIRED_OPERATIONS {
        assert!(
            generate_rust_client().contains(&format!("\"{method}\"")),
            "rust missing {method}"
        );
        assert!(
            generate_javascript_client().contains(&format!("method: \"{method}\"")),
            "javascript missing {method}"
        );
    }
}

#[test]
fn coverage_contract_027_generators_emit_the_same_operation_count() {
    let rust_entries = generate_rust_client()
        .lines()
        .filter(|line| line.starts_with("    (\""))
        .count();
    let js_entries = generate_javascript_client()
        .matches("Object.freeze({")
        .count();
    assert_eq!(rust_entries, js_entries);
}

#[test]
fn coverage_contract_028_required_operation_set_is_pinned_at_four() {
    // Both generated clients hard-code a length-4 self-check; growing the
    // representative set must update generators and this pin together.
    assert_eq!(REQUIRED_OPERATIONS.len(), 4);
}
