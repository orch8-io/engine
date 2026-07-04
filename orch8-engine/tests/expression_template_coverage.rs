#![allow(clippy::too_many_lines)]
//! Expression evaluation and template resolution coverage tests.
//!
//! 100 tests covering:
//! - Expression comparisons (==, !=, >, <, >=, <=)
//! - Boolean logic (&&, ||, !)
//! - Path access (context.data, outputs, nested paths, array indexing)
//! - Literals and edge cases (true/false, null, missing fields)
//! - Template resolution (basic, outputs, edge cases)
//! - Param resolution (full object resolution)

use serde_json::json;

use orch8_engine::expression::{evaluate, evaluate_condition, is_truthy};
use orch8_engine::template::{contains_template, resolve, resolve_with_state};
use orch8_types::context::{ExecutionContext, RuntimeContext};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mk_ctx(data: serde_json::Value, config: serde_json::Value) -> ExecutionContext {
    ExecutionContext {
        data,
        config,
        audit: vec![],
        runtime: RuntimeContext::default(),
    }
}

fn empty_ctx() -> ExecutionContext {
    mk_ctx(json!({}), json!({}))
}

fn sample_ctx() -> ExecutionContext {
    mk_ctx(
        json!({
            "name": "Alice",
            "age": 30,
            "score": 85.5,
            "active": true,
            "role": "admin",
            "items": ["a", "b", "c"],
            "nested": {"deep": {"value": 42}},
            "empty_str": "",
            "zero": 0,
            "null_field": null
        }),
        json!({
            "api_key": "sk_test_123",
            "max_retries": 3,
            "debug": false
        }),
    )
}

fn sample_outputs() -> serde_json::Value {
    json!({
        "step_a": {"value": "approve", "confidence": 0.95, "count": 10},
        "step_b": {"status": "done", "results": [1, 2, 3]},
        "generate": {"content": "Hello world", "tokens": 50},
        "fetch_data": {
            "response": {
                "body": {"users": [{"name": "Bob"}, {"name": "Carol"}]},
                "status_code": 200
            }
        }
    })
}

// ===========================================================================
// Expression evaluation -- comparisons (tests 1-20)
// ===========================================================================

// --- Equality (==) with strings, numbers, booleans (tests 1-5) ---

#[test]
fn expr_01_equality_string_match() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.value == \"approve\"", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_02_equality_string_mismatch() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.value == \"reject\"", &ctx, &out);
    assert_eq!(result, json!(false));
}

#[test]
fn expr_03_equality_number() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age == 30", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_04_equality_boolean_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.active == true", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_05_equality_boolean_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.config.debug == false", &ctx, &out);
    assert_eq!(result, json!(true));
}

// --- Inequality (!=) (tests 6-10) ---

#[test]
fn expr_06_inequality_string() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.value != \"reject\"", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_07_inequality_number() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age != 25", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_08_inequality_same_values() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age != 30", &ctx, &out);
    assert_eq!(result, json!(false));
}

#[test]
fn expr_09_inequality_boolean() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.active != false", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_10_inequality_null_vs_value() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.null_field != \"something\"", &ctx, &out);
    assert_eq!(result, json!(true));
}

// --- Greater than (>), less than (<), >=, <= (tests 11-15) ---

#[test]
fn expr_11_greater_than_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age > 25", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_12_greater_than_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age > 35", &ctx, &out);
    assert_eq!(result, json!(false));
}

#[test]
fn expr_13_less_than_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age < 35", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_14_greater_than_or_equal() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age >= 30", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_15_less_than_or_equal() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age <= 30", &ctx, &out);
    assert_eq!(result, json!(true));
}

// --- Mixed type comparisons, null handling (tests 16-20) ---

#[test]
fn expr_16_compare_float_and_int() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.score > 80", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_17_null_equality() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.null_field == null", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_18_missing_field_is_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.nonexistent == null", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_19_null_not_equal_to_zero() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.null_field != 0", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_20_string_comparison_lexicographic() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("\"apple\" < \"banana\"", &ctx, &out);
    assert_eq!(result, json!(true));
}

// ===========================================================================
// Expression evaluation -- boolean logic (tests 21-35)
// ===========================================================================

// --- AND (&&) combinations (tests 21-25) ---

#[test]
fn expr_21_and_both_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == true && context.data.age > 18",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

#[test]
fn expr_22_and_left_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == false && context.data.age > 18",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(false));
}

#[test]
fn expr_23_and_right_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == true && context.data.age > 50",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(false));
}

#[test]
fn expr_24_and_both_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == false && context.data.age > 50",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(false));
}

#[test]
fn expr_25_and_triple_condition() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == true && context.data.age >= 30 && context.data.role == \"admin\"",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

// --- OR (||) combinations (tests 26-30) ---

#[test]
fn expr_26_or_both_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == true || context.data.age > 18",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

#[test]
fn expr_27_or_left_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == true || context.data.age > 50",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

#[test]
fn expr_28_or_right_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == false || context.data.age > 18",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

#[test]
fn expr_29_or_both_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate(
        "context.data.active == false || context.data.age > 50",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(false));
}

#[test]
fn expr_30_or_with_and_precedence() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    // AND binds tighter: (false && true) || true => false || true => true
    let result = evaluate(
        "context.data.active == false && context.data.age > 18 || context.data.role == \"admin\"",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

// --- NOT (!), complex nested boolean expressions (tests 31-35) ---

#[test]
fn expr_31_not_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("!context.data.active", &ctx, &out);
    assert_eq!(result, json!(false));
}

#[test]
fn expr_32_not_false() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("!context.config.debug", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_33_not_null_is_true() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("!context.data.null_field", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_34_double_negation() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("!!context.data.active", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_35_complex_nested_boolean() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    // (true && (30 > 25)) || (!false) => (true && true) || true => true || true => true
    let result = evaluate(
        "(context.data.active && context.data.age > 25) || !context.config.debug",
        &ctx,
        &out,
    );
    assert_eq!(result, json!(true));
}

// ===========================================================================
// Expression evaluation -- path access (tests 36-50)
// ===========================================================================

// --- context.data.field access (tests 36-40) ---

#[test]
fn expr_36_context_data_string() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.name", &ctx, &out);
    assert_eq!(result, json!("Alice"));
}

#[test]
fn expr_37_context_data_number() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.age", &ctx, &out);
    assert_eq!(result, json!(30));
}

#[test]
fn expr_38_context_data_boolean() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.active", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_39_context_config_access() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.config.api_key", &ctx, &out);
    assert_eq!(result, json!("sk_test_123"));
}

#[test]
fn expr_40_context_data_missing_returns_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.missing_field", &ctx, &out);
    assert_eq!(result, json!(null));
}

// --- outputs.step_id.field access (tests 41-45) ---

#[test]
fn expr_41_outputs_string_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.value", &ctx, &out);
    assert_eq!(result, json!("approve"));
}

#[test]
fn expr_42_outputs_number_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.count", &ctx, &out);
    assert_eq!(result, json!(10));
}

#[test]
fn expr_43_steps_alias_for_outputs() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("steps.step_a.value", &ctx, &out);
    assert_eq!(result, json!("approve"));
}

#[test]
fn expr_44_outputs_missing_step() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.nonexistent.field", &ctx, &out);
    assert_eq!(result, json!(null));
}

#[test]
fn expr_45_outputs_missing_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.step_a.nonexistent", &ctx, &out);
    assert_eq!(result, json!(null));
}

// --- Nested path access (a.b.c.d), array indexing (tests 46-50) ---

#[test]
fn expr_46_deeply_nested_path() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.nested.deep.value", &ctx, &out);
    assert_eq!(result, json!(42));
}

#[test]
fn expr_47_array_index_access() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.items.0", &ctx, &out);
    assert_eq!(result, json!("a"));
}

#[test]
fn expr_48_array_index_last() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("context.data.items.2", &ctx, &out);
    assert_eq!(result, json!("c"));
}

#[test]
fn expr_49_nested_output_path() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.fetch_data.response.status_code", &ctx, &out);
    assert_eq!(result, json!(200));
}

#[test]
fn expr_50_nested_output_array_access() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate("outputs.fetch_data.response.body.users.0.name", &ctx, &out);
    assert_eq!(result, json!("Bob"));
}

// ===========================================================================
// Expression evaluation -- literals and edge cases (tests 51-60)
// ===========================================================================

// --- true/false literals (tests 51-55) ---

#[test]
fn expr_51_true_literal() {
    let ctx = empty_ctx();
    let out = json!({});
    let result = evaluate("true", &ctx, &out);
    assert_eq!(result, json!(true));
}

#[test]
fn expr_52_false_literal() {
    let ctx = empty_ctx();
    let out = json!({});
    let result = evaluate("false", &ctx, &out);
    assert_eq!(result, json!(false));
}

#[test]
fn expr_53_null_literal() {
    let ctx = empty_ctx();
    let out = json!({});
    let result = evaluate("null", &ctx, &out);
    assert_eq!(result, json!(null));
}

#[test]
fn expr_54_number_literal() {
    let ctx = empty_ctx();
    let out = json!({});
    let result = evaluate("42", &ctx, &out);
    assert_eq!(result, json!(42.0));
}

#[test]
fn expr_55_string_literal() {
    let ctx = empty_ctx();
    let out = json!({});
    let result = evaluate("\"hello\"", &ctx, &out);
    assert_eq!(result, json!("hello"));
}

// --- Null comparisons, missing fields, empty strings (tests 56-60) ---

#[test]
fn expr_56_null_is_falsy() {
    let result = is_truthy(&json!(null));
    assert!(!result);
}

#[test]
fn expr_57_empty_string_is_falsy() {
    let result = is_truthy(&json!(""));
    assert!(!result);
}

#[test]
fn expr_58_zero_is_falsy() {
    let result = is_truthy(&json!(0));
    assert!(!result);
}

#[test]
fn expr_59_nonempty_string_is_truthy() {
    let result = is_truthy(&json!("hello"));
    assert!(result);
}

#[test]
fn expr_60_evaluate_condition_with_context() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let result = evaluate_condition("context.data.age > 5", &ctx, &out);
    assert!(result);
}

// ===========================================================================
// Template resolution -- basic (tests 61-75)
// ===========================================================================

// --- Simple variable substitution (tests 61-65) ---

#[test]
fn tmpl_61_simple_data_substitution() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.name}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Alice"));
}

#[test]
fn tmpl_62_config_substitution() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.config.api_key}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("sk_test_123"));
}

#[test]
fn tmpl_63_number_preserved_single_expression() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.age}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(30));
    assert!(result.is_number());
}

#[test]
fn tmpl_64_boolean_preserved_single_expression() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.active}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(true));
    assert!(result.is_boolean());
}

#[test]
fn tmpl_65_data_shortcut_path() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{data.name}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Alice"));
}

// --- Multiple templates in one string (tests 66-70) ---

#[test]
fn tmpl_66_two_templates_in_string() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("Hello {{context.data.name}}, age {{context.data.age}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Hello Alice, age 30"));
}

#[test]
fn tmpl_67_three_templates_in_string() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.name}} is {{context.data.age}} and {{context.data.role}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Alice is 30 and admin"));
}

#[test]
fn tmpl_68_template_with_surrounding_text() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("prefix_{{context.data.name}}_suffix");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("prefix_Alice_suffix"));
}

#[test]
fn tmpl_69_mixed_templates_and_static() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("Name: {{context.data.name}}, Key: {{context.config.api_key}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Name: Alice, Key: sk_test_123"));
}

#[test]
fn tmpl_70_adjacent_templates() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.name}}{{context.data.role}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Aliceadmin"));
}

// --- Nested object access in templates (tests 71-75) ---

#[test]
fn tmpl_71_nested_data_access() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.nested.deep.value}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(42));
}

#[test]
fn tmpl_72_nested_output_access() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.fetch_data.response.status_code}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(200));
}

#[test]
fn tmpl_73_object_returned_as_whole() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.step_a}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert!(result.is_object());
    assert_eq!(result["value"], json!("approve"));
    assert_eq!(result["count"], json!(10));
}

#[test]
fn tmpl_74_array_returned_as_whole() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.step_b.results}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!([1, 2, 3]));
}

#[test]
fn tmpl_75_array_index_in_template() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.items.1}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("b"));
}

// ===========================================================================
// Template resolution -- outputs (tests 76-85)
// ===========================================================================

// --- {{outputs.step_id.field}} resolution (tests 76-80) ---

#[test]
fn tmpl_76_output_string_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.step_a.value}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("approve"));
}

#[test]
fn tmpl_77_output_number_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.generate.tokens}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(50));
}

#[test]
fn tmpl_78_output_inline_interpolation() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("Result: {{outputs.step_b.status}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Result: done"));
}

#[test]
fn tmpl_79_steps_alias_in_template() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{steps.generate.content}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Hello world"));
}

#[test]
fn tmpl_80_output_float_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.step_a.confidence}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(0.95));
}

// --- Deeply nested output paths (tests 81-85) ---

#[test]
fn tmpl_81_deeply_nested_output() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.fetch_data.response.body.users.0.name}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Bob"));
}

#[test]
fn tmpl_82_deeply_nested_output_second_element() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.fetch_data.response.body.users.1.name}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Carol"));
}

#[test]
fn tmpl_83_nested_output_inline() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!(
        "User: {{outputs.fetch_data.response.body.users.0.name}}, Status: {{outputs.fetch_data.response.status_code}}"
    );
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("User: Bob, Status: 200"));
}

#[test]
fn tmpl_84_nested_output_object() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.fetch_data.response.body}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert!(result.is_object());
    assert!(result["users"].is_array());
}

#[test]
fn tmpl_85_nested_output_missing_deep_field() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.fetch_data.response.body.users.0.email}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(null));
}

// ===========================================================================
// Template resolution -- edge cases (tests 86-95)
// ===========================================================================

// --- Missing path returns empty/null (tests 86-90) ---

#[test]
fn tmpl_86_missing_context_path_returns_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.nonexistent}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(null));
}

#[test]
fn tmpl_87_missing_output_step_returns_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{outputs.missing_step.field}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(null));
}

#[test]
fn tmpl_88_missing_nested_field_returns_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.nested.deep.nonexistent}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!(null));
}

#[test]
fn tmpl_89_missing_in_inline_renders_null() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("value: {{context.data.nonexistent}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("value: null"));
}

#[test]
fn tmpl_90_fallback_on_missing_path() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{context.data.nonexistent|fallback_value}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("fallback_value"));
}

// --- Special characters in values, JSON in templates (tests 91-95) ---

#[test]
fn tmpl_91_value_with_special_chars() {
    let ctx = mk_ctx(json!({"msg": "he said \"hi\" & <bye>"}), json!({}));
    let out = json!({});
    let input = json!("{{context.data.msg}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("he said \"hi\" & <bye>"));
}

#[test]
fn tmpl_92_value_with_unicode() {
    let ctx = mk_ctx(json!({"greeting": "Hola mundo! 🌍"}), json!({}));
    let out = json!({});
    let input = json!("{{context.data.greeting}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("Hola mundo! 🌍"));
}

#[test]
fn tmpl_93_json_function_serializes_object() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("{{json(outputs.step_a)}}");
    let result = resolve(&input, &ctx, &out).unwrap();
    let s = result.as_str().unwrap();
    assert!(s.contains("\"value\""));
    assert!(s.contains("approve"));
}

#[test]
fn tmpl_94_no_template_passthrough() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!("plain string with no templates");
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, json!("plain string with no templates"));
}

#[test]
fn tmpl_95_contains_template_detection() {
    assert!(contains_template(&json!("{{context.data.x}}")));
    assert!(contains_template(&json!("prefix {{x}} suffix")));
    assert!(!contains_template(&json!("no templates")));
    assert!(!contains_template(&json!(42)));
    assert!(!contains_template(&json!(null)));
}

// ===========================================================================
// Param resolution (tests 96-100)
// ===========================================================================

#[test]
fn param_96_full_object_with_mixed_values() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!({
        "url": "https://api.example.com/users/{{context.data.name}}",
        "method": "POST",
        "timeout": 30,
        "headers": {
            "Authorization": "Bearer {{context.config.api_key}}",
            "Content-Type": "application/json"
        }
    });
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result["url"], json!("https://api.example.com/users/Alice"));
    assert_eq!(result["method"], json!("POST"));
    assert_eq!(result["timeout"], json!(30));
    assert_eq!(
        result["headers"]["Authorization"],
        json!("Bearer sk_test_123")
    );
    assert_eq!(result["headers"]["Content-Type"], json!("application/json"));
}

#[test]
fn param_97_array_params_with_templates() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!({
        "recipients": ["{{context.data.name}}", "static@email.com"],
        "subject": "Hello {{context.data.name}}"
    });
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result["recipients"][0], json!("Alice"));
    assert_eq!(result["recipients"][1], json!("static@email.com"));
    assert_eq!(result["subject"], json!("Hello Alice"));
}

#[test]
fn param_98_nested_params_with_output_refs() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!({
        "body": {
            "prompt": "{{outputs.generate.content}}",
            "max_tokens": "{{outputs.generate.tokens}}",
            "metadata": {
                "user": "{{context.data.name}}",
                "step_result": "{{outputs.step_a.value}}"
            }
        }
    });
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result["body"]["prompt"], json!("Hello world"));
    assert_eq!(result["body"]["max_tokens"], json!(50));
    assert_eq!(result["body"]["metadata"]["user"], json!("Alice"));
    assert_eq!(result["body"]["metadata"]["step_result"], json!("approve"));
}

#[test]
fn param_99_params_without_templates_pass_through() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let input = json!({
        "static_key": "static_value",
        "number": 42,
        "flag": true,
        "nothing": null,
        "list": [1, 2, 3]
    });
    let result = resolve(&input, &ctx, &out).unwrap();
    assert_eq!(result, input);
}

#[test]
fn param_100_resolve_with_state() {
    let ctx = sample_ctx();
    let out = sample_outputs();
    let state = json!({"counter": 7, "label": "iteration_7"});
    let input = json!({
        "count": "{{state.counter}}",
        "label": "{{state.label}}",
        "user": "{{context.data.name}}"
    });
    let result = resolve_with_state(&input, &ctx, &out, Some(&state)).unwrap();
    assert_eq!(result["count"], json!(7));
    assert_eq!(result["label"], json!("iteration_7"));
    assert_eq!(result["user"], json!("Alice"));
}
