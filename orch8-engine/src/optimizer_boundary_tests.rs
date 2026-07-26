//! Unit coverage for guard folding and canonical workflow identity.
//!
//! Count contract: 60 independently named unit tests.

use chrono::Utc;
use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
use serde_json::json;

use super::*;

fn sequence_named(name: &str) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("tenant"),
        namespace: Namespace::new("default"),
        name: name.into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("step"),
            handler: "noop".into(),
            params: json!({"value": 1}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
            output_schema: None,
            when: None,
            compensation: None,
        }))],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

macro_rules! guard_case {
    ($name:ident, $guard:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let actual = classify_guard($guard);
            assert_eq!(actual, $expected);
        }
    };
}

guard_case!(
    coverage_optimizer_001_missing_guard_is_always,
    None,
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_002_true_guard_is_always,
    Some("true"),
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_003_true_with_leading_space_is_always,
    Some(" true"),
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_004_true_with_trailing_space_is_always,
    Some("true "),
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_005_true_with_tabs_is_always,
    Some("\ttrue\t"),
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_006_true_with_newlines_is_always,
    Some("\ntrue\n"),
    GuardPlan::Always
);
guard_case!(
    coverage_optimizer_007_false_guard_is_never,
    Some("false"),
    GuardPlan::Never
);
guard_case!(
    coverage_optimizer_008_false_with_leading_space_is_never,
    Some(" false"),
    GuardPlan::Never
);
guard_case!(
    coverage_optimizer_009_false_with_trailing_space_is_never,
    Some("false "),
    GuardPlan::Never
);
guard_case!(
    coverage_optimizer_010_false_with_tabs_is_never,
    Some("\tfalse\t"),
    GuardPlan::Never
);
guard_case!(
    coverage_optimizer_011_false_with_newlines_is_never,
    Some("\nfalse\n"),
    GuardPlan::Never
);
guard_case!(
    coverage_optimizer_012_empty_guard_is_dynamic,
    Some(""),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_013_spaces_only_guard_is_dynamic,
    Some("   "),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_014_uppercase_true_is_dynamic,
    Some("TRUE"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_015_titlecase_true_is_dynamic,
    Some("True"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_016_uppercase_false_is_dynamic,
    Some("FALSE"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_017_titlecase_false_is_dynamic,
    Some("False"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_018_numeric_one_is_dynamic,
    Some("1"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_019_numeric_zero_is_dynamic,
    Some("0"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_020_null_text_is_dynamic,
    Some("null"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_021_template_reference_is_dynamic,
    Some("{{ data.ready }}"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_022_output_reference_is_dynamic,
    Some("{{ outputs.check.ok }}"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_023_equality_expression_is_dynamic,
    Some("data.value == 1"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_024_inequality_expression_is_dynamic,
    Some("data.value != 1"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_025_greater_expression_is_dynamic,
    Some("data.value > 1"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_026_less_expression_is_dynamic,
    Some("data.value < 1"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_027_boolean_and_expression_is_dynamic,
    Some("a && b"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_028_boolean_or_expression_is_dynamic,
    Some("a || b"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_029_negated_expression_is_dynamic,
    Some("!a"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_030_json_true_is_dynamic,
    Some("{\"value\":true}"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_031_quoted_true_is_dynamic,
    Some("\"true\""),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_032_true_with_comment_is_dynamic,
    Some("true # comment"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_033_false_with_comment_is_dynamic,
    Some("false # comment"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_034_true_prefix_is_dynamic,
    Some("true_value"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_035_false_prefix_is_dynamic,
    Some("false_value"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_036_unicode_expression_is_dynamic,
    Some("pronto"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_037_path_expression_is_dynamic,
    Some("$.ready"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_038_parenthesized_true_is_dynamic,
    Some("(true)"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_039_double_negation_is_dynamic,
    Some("!!ready"),
    GuardPlan::Dynamic
);
guard_case!(
    coverage_optimizer_040_multiline_expression_is_dynamic,
    Some("a\n&& b"),
    GuardPlan::Dynamic
);

macro_rules! identity_case {
    ($name:ident, $changed_name:expr) => {
        #[test]
        fn $name() {
            let baseline = sequence_named("baseline");
            let changed = sequence_named($changed_name);
            let baseline_hash = source_hash(&baseline).unwrap();
            let changed_hash = source_hash(&changed).unwrap();
            assert_ne!(baseline_hash, changed_hash);
            assert_eq!(changed_hash.len(), 64);
        }
    };
}

identity_case!(
    coverage_optimizer_041_name_change_updates_identity,
    "changed"
);
identity_case!(
    coverage_optimizer_042_single_letter_name_updates_identity,
    "x"
);
identity_case!(coverage_optimizer_043_numeric_name_updates_identity, "123");
identity_case!(
    coverage_optimizer_044_hyphenated_name_updates_identity,
    "base-line"
);
identity_case!(
    coverage_optimizer_045_underscored_name_updates_identity,
    "base_line"
);
identity_case!(
    coverage_optimizer_046_dotted_name_updates_identity,
    "base.line"
);
identity_case!(
    coverage_optimizer_047_slashed_name_updates_identity,
    "base/line"
);
identity_case!(
    coverage_optimizer_048_spaced_name_updates_identity,
    "base line"
);
identity_case!(
    coverage_optimizer_049_uppercase_name_updates_identity,
    "BASELINE"
);
identity_case!(
    coverage_optimizer_050_mixed_case_name_updates_identity,
    "BaseLine"
);
identity_case!(
    coverage_optimizer_051_unicode_name_updates_identity,
    "sequência"
);
identity_case!(
    coverage_optimizer_052_emoji_name_updates_identity,
    "baseline-🚀"
);
identity_case!(
    coverage_optimizer_053_newline_name_updates_identity,
    "base\nline"
);
identity_case!(
    coverage_optimizer_054_tab_name_updates_identity,
    "base\tline"
);
identity_case!(
    coverage_optimizer_055_quote_name_updates_identity,
    "base\"line"
);
identity_case!(
    coverage_optimizer_056_backslash_name_updates_identity,
    "base\\line"
);
identity_case!(
    coverage_optimizer_057_tilde_name_updates_identity,
    "base~line"
);
identity_case!(
    coverage_optimizer_058_braced_name_updates_identity,
    "base{line}"
);
identity_case!(
    coverage_optimizer_059_long_name_updates_identity,
    "x".repeat(512).as_str()
);
identity_case!(
    coverage_optimizer_060_zero_width_name_updates_identity,
    "base\u{200b}line"
);
