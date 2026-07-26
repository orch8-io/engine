//! Coverage tests for the portable runtime type-generation foundations.
//!
//! Pins the Swift/Kotlin native renderers (primitive mappings, nullability,
//! identifier sanitization, reserved words, unique naming, render budgets)
//! added by the portable runtime foundations feature.
//!
//! Count contract: 35 independently named unit tests.

use serde_json::json;

use super::*;

fn swift_ctx() -> NativeContext {
    NativeContext::new(NativeLanguage::Swift)
}

fn kotlin_ctx() -> NativeContext {
    NativeContext::new(NativeLanguage::Kotlin)
}

macro_rules! native_type_case {
    ($name:ident, $language:expr, $schema:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let mut ctx = NativeContext::new($language);
            let rendered = render_native(&$schema, "Root", 0, &mut ctx).unwrap();
            assert_eq!(rendered, $expected);
        }
    };
}

native_type_case!(
    coverage_dataflow_001_swift_boolean_maps_to_bool,
    NativeLanguage::Swift,
    json!({"type": "boolean"}),
    "Bool"
);
native_type_case!(
    coverage_dataflow_002_kotlin_boolean_maps_to_boolean,
    NativeLanguage::Kotlin,
    json!({"type": "boolean"}),
    "Boolean"
);
native_type_case!(
    coverage_dataflow_003_swift_integer_maps_to_int64,
    NativeLanguage::Swift,
    json!({"type": "integer"}),
    "Int64"
);
native_type_case!(
    coverage_dataflow_004_kotlin_integer_maps_to_long,
    NativeLanguage::Kotlin,
    json!({"type": "integer"}),
    "Long"
);
native_type_case!(
    coverage_dataflow_005_swift_number_maps_to_double,
    NativeLanguage::Swift,
    json!({"type": "number"}),
    "Double"
);
native_type_case!(
    coverage_dataflow_006_string_maps_to_string,
    NativeLanguage::Kotlin,
    json!({"type": "string"}),
    "String"
);
native_type_case!(
    coverage_dataflow_007_swift_array_wraps_item_type,
    NativeLanguage::Swift,
    json!({"type": "array", "items": {"type": "string"}}),
    "[String]"
);
native_type_case!(
    coverage_dataflow_008_kotlin_array_wraps_item_type,
    NativeLanguage::Kotlin,
    json!({"type": "array", "items": {"type": "integer"}}),
    "List<Long>"
);
native_type_case!(
    coverage_dataflow_009_true_schema_is_swift_unknown,
    NativeLanguage::Swift,
    Value::Bool(true),
    "JSONValue"
);
native_type_case!(
    coverage_dataflow_010_false_schema_is_kotlin_unknown,
    NativeLanguage::Kotlin,
    Value::Bool(false),
    "JsonElement"
);
native_type_case!(
    coverage_dataflow_011_string_enum_maps_to_string,
    NativeLanguage::Swift,
    json!({"enum": ["high", "low"]}),
    "String"
);
native_type_case!(
    coverage_dataflow_012_mixed_enum_falls_back_to_unknown,
    NativeLanguage::Kotlin,
    json!({"enum": ["high", 1]}),
    "JsonElement"
);
native_type_case!(
    coverage_dataflow_013_nullable_any_of_becomes_optional,
    NativeLanguage::Swift,
    json!({"anyOf": [{"type": "string"}, {"type": "null"}]}),
    "String?"
);
native_type_case!(
    coverage_dataflow_014_type_array_with_null_becomes_optional,
    NativeLanguage::Kotlin,
    json!({"type": ["number", "null"]}),
    "Double?"
);
native_type_case!(
    coverage_dataflow_015_multi_type_union_is_unknown,
    NativeLanguage::Swift,
    json!({"anyOf": [{"type": "string"}, {"type": "number"}]}),
    "JSONValue"
);
native_type_case!(
    coverage_dataflow_016_bare_object_is_swift_dictionary,
    NativeLanguage::Swift,
    json!({"type": "object"}),
    "[String: JSONValue]"
);
native_type_case!(
    coverage_dataflow_017_bare_object_is_kotlin_map,
    NativeLanguage::Kotlin,
    json!({"type": "object"}),
    "Map<String, JsonElement>"
);

#[test]
fn coverage_dataflow_018_object_with_properties_generates_named_definition() {
    let mut ctx = swift_ctx();
    let name = render_native(
        &json!({
            "type": "object",
            "properties": {
                "order_id": {"type": "string"},
                "score": {"type": "number"}
            },
            "required": ["order_id"]
        }),
        "SequenceInput",
        0,
        &mut ctx,
    )
    .unwrap();
    assert_eq!(name, "SequenceInput");
    assert_eq!(ctx.definitions.len(), 1);
    let definition = &ctx.definitions[0];
    assert!(definition.contains("public let order_id: String"));
    assert!(definition.contains("public let score: Double?"));
}

#[test]
fn coverage_dataflow_019_kotlin_optional_fields_default_to_null() {
    let mut ctx = kotlin_ctx();
    render_native(
        &json!({
            "type": "object",
            "properties": {"score": {"type": "number"}}
        }),
        "Input",
        0,
        &mut ctx,
    )
    .unwrap();
    assert!(ctx.definitions[0].contains("val score: Double? = null"));
}

#[test]
fn coverage_dataflow_020_swift_renamed_field_emits_coding_key() {
    let mut ctx = swift_ctx();
    render_native(
        &json!({
            "type": "object",
            "properties": {"order-id": {"type": "string"}},
            "required": ["order-id"]
        }),
        "Input",
        0,
        &mut ctx,
    )
    .unwrap();
    let definition = &ctx.definitions[0];
    assert!(definition.contains("public let order_id: String"));
    assert!(definition.contains("case order_id = \"order-id\""));
}

#[test]
fn coverage_dataflow_021_kotlin_renamed_field_emits_serial_name() {
    let mut ctx = kotlin_ctx();
    render_native(
        &json!({
            "type": "object",
            "properties": {"order-id": {"type": "string"}},
            "required": ["order-id"]
        }),
        "Input",
        0,
        &mut ctx,
    )
    .unwrap();
    assert!(ctx.definitions[0].contains("@SerialName(\"order-id\") val order_id: String"));
}

#[test]
fn coverage_dataflow_022_swift_empty_object_renders_empty_init() {
    let mut ctx = swift_ctx();
    render_native(
        &json!({"type": "object", "properties": {}}),
        "Empty",
        0,
        &mut ctx,
    )
    .unwrap();
    assert!(ctx.definitions[0].contains("public init() {}"));
    assert!(!ctx.definitions[0].contains("CodingKeys"));
}

#[test]
fn coverage_dataflow_023_native_depth_limit_is_enforced() {
    let mut ctx = swift_ctx();
    let error = render_native(
        &json!({"type": "string"}),
        "Root",
        MAX_SCHEMA_DEPTH + 1,
        &mut ctx,
    )
    .unwrap_err();
    assert_eq!(error, DataflowGenerationError::DepthLimit);
}

#[test]
fn coverage_dataflow_024_render_budget_enforces_node_limit() {
    let mut budget = RenderBudget::default();
    for _ in 0..MAX_SCHEMA_NODES {
        budget.enter(0).unwrap();
    }
    assert_eq!(budget.enter(0), Err(DataflowGenerationError::NodeLimit));
}

#[test]
fn coverage_dataflow_025_render_budget_allows_depth_at_limit() {
    let mut budget = RenderBudget::default();
    assert!(budget.enter(MAX_SCHEMA_DEPTH).is_ok());
}

macro_rules! identifier_case {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(native_identifier($input), $expected);
        }
    };
}

identifier_case!(
    coverage_dataflow_026_identifier_keeps_safe_characters,
    "order_id",
    "order_id"
);
identifier_case!(
    coverage_dataflow_027_identifier_replaces_invalid_characters,
    "order-id.v2",
    "order_id_v2"
);
identifier_case!(
    coverage_dataflow_028_identifier_escapes_leading_digit,
    "9lives",
    "_9lives"
);
identifier_case!(
    coverage_dataflow_029_identifier_escapes_reserved_word,
    "class",
    "_class"
);
identifier_case!(
    coverage_dataflow_030_identifier_escapes_empty_string,
    "",
    "_"
);

#[test]
fn coverage_dataflow_031_reserved_word_list_spans_both_languages() {
    for word in [
        "class", "struct", "func", "when", "data", "sealed", "self", "Self",
    ] {
        assert!(native_reserved_word(word), "{word}");
    }
    for word in ["classes", "dataflow", "SELF", "x"] {
        assert!(!native_reserved_word(word), "{word}");
    }
}

#[test]
fn coverage_dataflow_032_make_nullable_is_idempotent() {
    assert_eq!(make_nullable("String"), "String?");
    assert_eq!(make_nullable("String?"), "String?");
}

#[test]
fn coverage_dataflow_033_unique_name_suffixes_collisions() {
    let mut ctx = swift_ctx();
    assert_eq!(ctx.unique_name("order"), "Order");
    assert_eq!(ctx.unique_name("order"), "Order2");
    assert_eq!(ctx.unique_name("order"), "Order3");
    assert_eq!(ctx.unique_name("order-id"), "OrderId");
    assert_eq!(ctx.unknown_type(), "JSONValue");
    assert_eq!(kotlin_ctx().unknown_type(), "JsonElement");
}

#[test]
fn coverage_dataflow_034_pascal_case_handles_segments_and_digits() {
    assert_eq!(pascal_case("order_id"), "OrderId");
    assert_eq!(pascal_case("foo-bar_baz"), "FooBarBaz");
    assert_eq!(pascal_case("123abc"), "Generated123abc");
    assert_eq!(pascal_case(""), "Generated");
    assert_eq!(pascal_case("!!!"), "Generated");
}

native_type_case!(
    coverage_dataflow_035_kotlin_number_maps_to_double,
    NativeLanguage::Kotlin,
    json!({"type": "number"}),
    "Double"
);
