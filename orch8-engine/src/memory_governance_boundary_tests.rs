//! Boundary tests for governed durable-memory policies.
//!
//! Count contract: 70 independently named unit tests.

use super::*;

fn valid_policy() -> MemoryNamespacePolicy {
    MemoryNamespacePolicy {
        policy_version: 1,
        allowed_sequence_ids: vec![SequenceId::new()],
        operations: vec![MemoryOperation::Store],
        residency: "br-south-1".into(),
        default_retention_secs: 60,
        max_retention_secs: 3_600,
    }
}

macro_rules! namespace_case {
    ($name:ident, $value:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let actual = namespace_is_valid($value);
            assert_eq!(actual, $valid);
        }
    };
}

fn namespace_is_valid(namespace: impl AsRef<str>) -> bool {
    validate_target_namespace(namespace.as_ref()).is_ok()
}

namespace_case!(
    coverage_memory_001_empty_namespace_is_rejected,
    String::new(),
    false
);
namespace_case!(
    coverage_memory_002_single_letter_namespace_is_allowed,
    "a",
    true
);
namespace_case!(
    coverage_memory_003_single_digit_namespace_is_allowed,
    "7",
    true
);
namespace_case!(
    coverage_memory_004_hyphen_namespace_is_allowed,
    "customer-support",
    true
);
namespace_case!(
    coverage_memory_005_underscore_namespace_is_allowed,
    "customer_support",
    true
);
namespace_case!(
    coverage_memory_006_dot_namespace_is_allowed,
    "customer.support",
    true
);
namespace_case!(
    coverage_memory_007_slash_namespace_is_allowed,
    "customer/support",
    true
);
namespace_case!(
    coverage_memory_008_mixed_safe_namespace_is_allowed,
    "A9-b_c.d/e",
    true
);
namespace_case!(
    coverage_memory_009_128_byte_namespace_is_allowed,
    "a".repeat(128),
    true
);
namespace_case!(
    coverage_memory_010_129_byte_namespace_is_rejected,
    "a".repeat(129),
    false
);
namespace_case!(
    coverage_memory_011_space_namespace_is_rejected,
    "customer support",
    false
);
namespace_case!(
    coverage_memory_012_colon_namespace_is_rejected,
    "customer:support",
    false
);
namespace_case!(
    coverage_memory_013_at_namespace_is_rejected,
    "customer@support",
    false
);
namespace_case!(
    coverage_memory_014_backslash_namespace_is_rejected,
    "customer\\support",
    false
);
namespace_case!(
    coverage_memory_015_unicode_namespace_is_rejected,
    "memória",
    false
);
namespace_case!(
    coverage_memory_016_newline_namespace_is_rejected,
    "customer\nsupport",
    false
);
namespace_case!(
    coverage_memory_017_tab_namespace_is_rejected,
    "customer\tsupport",
    false
);
namespace_case!(
    coverage_memory_018_hash_namespace_is_rejected,
    "customer#support",
    false
);
namespace_case!(
    coverage_memory_019_question_namespace_is_rejected,
    "customer?support",
    false
);
namespace_case!(
    coverage_memory_020_percent_namespace_is_rejected,
    "customer%support",
    false
);
namespace_case!(
    coverage_memory_021_policy_namespace_is_rejected,
    POLICY_NAMESPACE,
    false
);
namespace_case!(
    coverage_memory_022_reserved_prefix_is_rejected,
    "__orch8_custom",
    false
);
namespace_case!(
    coverage_memory_023_reserved_prefix_alone_is_rejected,
    "__orch8_",
    false
);
namespace_case!(
    coverage_memory_024_similar_non_reserved_prefix_is_allowed,
    "_orch8_custom",
    true
);
namespace_case!(
    coverage_memory_025_reserved_text_after_prefix_is_allowed,
    "team/__orch8_custom",
    true
);

macro_rules! residency_case {
    ($name:ident, $value:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let actual = residency_is_valid($value);
            assert_eq!(actual, $valid);
        }
    };
}

fn residency_is_valid(residency: impl AsRef<str>) -> bool {
    validate_residency(residency.as_ref()).is_ok()
}

residency_case!(
    coverage_memory_026_empty_residency_is_rejected,
    String::new(),
    false
);
residency_case!(
    coverage_memory_027_single_letter_residency_is_allowed,
    "a",
    true
);
residency_case!(
    coverage_memory_028_single_digit_residency_is_allowed,
    "1",
    true
);
residency_case!(
    coverage_memory_029_hyphenated_residency_is_allowed,
    "br-south-1",
    true
);
residency_case!(
    coverage_memory_030_underscored_residency_is_allowed,
    "br_south_1",
    true
);
residency_case!(
    coverage_memory_031_dotted_residency_is_allowed,
    "br.south.1",
    true
);
residency_case!(
    coverage_memory_032_uppercase_residency_is_allowed,
    "BR-SOUTH-1",
    true
);
residency_case!(
    coverage_memory_033_64_byte_residency_is_allowed,
    "r".repeat(64),
    true
);
residency_case!(
    coverage_memory_034_65_byte_residency_is_rejected,
    "r".repeat(65),
    false
);
residency_case!(
    coverage_memory_035_slash_residency_is_rejected,
    "br/south",
    false
);
residency_case!(
    coverage_memory_036_space_residency_is_rejected,
    "br south",
    false
);
residency_case!(
    coverage_memory_037_colon_residency_is_rejected,
    "br:south",
    false
);
residency_case!(
    coverage_memory_038_at_residency_is_rejected,
    "br@south",
    false
);
residency_case!(
    coverage_memory_039_unicode_residency_is_rejected,
    "são-paulo",
    false
);
residency_case!(
    coverage_memory_040_newline_residency_is_rejected,
    "br\nsouth",
    false
);
residency_case!(
    coverage_memory_041_backslash_residency_is_rejected,
    "br\\south",
    false
);
residency_case!(
    coverage_memory_042_plus_residency_is_rejected,
    "br+south",
    false
);
residency_case!(
    coverage_memory_043_comma_residency_is_rejected,
    "br,south",
    false
);
residency_case!(
    coverage_memory_044_semicolon_residency_is_rejected,
    "br;south",
    false
);
residency_case!(
    coverage_memory_045_mixed_safe_residency_is_allowed,
    "BR_2.south-prod",
    true
);

macro_rules! policy_case {
    ($name:ident, $mutate:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let mut policy = valid_policy();
            ($mutate)(&mut policy);
            let result = policy.validate();
            assert_eq!(result.is_ok(), $valid);
        }
    };
}

policy_case!(
    coverage_memory_046_zero_policy_version_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.policy_version = 0,
    false
);
policy_case!(
    coverage_memory_047_version_one_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.policy_version = 1,
    true
);
policy_case!(
    coverage_memory_048_maximum_policy_version_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.policy_version = u64::MAX,
    true
);
policy_case!(
    coverage_memory_049_empty_sequence_allowlist_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.allowed_sequence_ids.clear(),
    false
);
policy_case!(
    coverage_memory_050_two_unique_sequences_are_allowed,
    |p: &mut MemoryNamespacePolicy| p.allowed_sequence_ids.push(SequenceId::new()),
    true
);
policy_case!(
    coverage_memory_051_duplicate_sequence_is_rejected,
    |p: &mut MemoryNamespacePolicy| {
        let id = p.allowed_sequence_ids[0];
        p.allowed_sequence_ids.push(id);
    },
    false
);
policy_case!(
    coverage_memory_052_1024_sequences_are_allowed,
    |p: &mut MemoryNamespacePolicy| p.allowed_sequence_ids =
        (0..1024).map(|_| SequenceId::new()).collect(),
    true
);
policy_case!(
    coverage_memory_053_1025_sequences_are_rejected,
    |p: &mut MemoryNamespacePolicy| p.allowed_sequence_ids =
        (0..1025).map(|_| SequenceId::new()).collect(),
    false
);
policy_case!(
    coverage_memory_054_empty_operations_are_rejected,
    |p: &mut MemoryNamespacePolicy| p.operations.clear(),
    false
);
policy_case!(
    coverage_memory_055_store_operation_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.operations = vec![MemoryOperation::Store],
    true
);
policy_case!(
    coverage_memory_056_search_operation_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.operations = vec![MemoryOperation::Search],
    true
);
policy_case!(
    coverage_memory_057_delete_operation_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.operations = vec![MemoryOperation::Delete],
    true
);
policy_case!(
    coverage_memory_058_all_operations_are_allowed,
    |p: &mut MemoryNamespacePolicy| p.operations = vec![
        MemoryOperation::Store,
        MemoryOperation::Search,
        MemoryOperation::Delete
    ],
    true
);
policy_case!(
    coverage_memory_059_duplicate_operation_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.operations =
        vec![MemoryOperation::Store, MemoryOperation::Store],
    false
);
policy_case!(
    coverage_memory_060_invalid_residency_rejects_policy,
    |p: &mut MemoryNamespacePolicy| p.residency = "br/south".into(),
    false
);
policy_case!(
    coverage_memory_061_zero_default_retention_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.default_retention_secs = 0,
    false
);
policy_case!(
    coverage_memory_062_zero_max_retention_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.max_retention_secs = 0,
    false
);
policy_case!(
    coverage_memory_063_default_equal_to_max_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.default_retention_secs = p.max_retention_secs,
    true
);
policy_case!(
    coverage_memory_064_default_above_max_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.default_retention_secs = p.max_retention_secs + 1,
    false
);
policy_case!(
    coverage_memory_065_one_second_retention_is_allowed,
    |p: &mut MemoryNamespacePolicy| {
        p.default_retention_secs = 1;
        p.max_retention_secs = 1;
    },
    true
);
policy_case!(
    coverage_memory_066_ten_year_retention_is_allowed,
    |p: &mut MemoryNamespacePolicy| {
        p.default_retention_secs = MAX_RETENTION_SECS;
        p.max_retention_secs = MAX_RETENTION_SECS;
    },
    true
);
policy_case!(
    coverage_memory_067_more_than_ten_years_is_rejected,
    |p: &mut MemoryNamespacePolicy| p.max_retention_secs = MAX_RETENTION_SECS + 1,
    false
);
policy_case!(
    coverage_memory_068_default_cannot_exceed_ten_year_cap,
    |p: &mut MemoryNamespacePolicy| {
        p.default_retention_secs = MAX_RETENTION_SECS + 1;
        p.max_retention_secs = MAX_RETENTION_SECS + 1;
    },
    false
);
policy_case!(
    coverage_memory_069_safe_nested_residency_label_is_allowed,
    |p: &mut MemoryNamespacePolicy| p.residency = "prod.br_south-1".into(),
    true
);
policy_case!(
    coverage_memory_070_combined_valid_policy_is_accepted,
    |p: &mut MemoryNamespacePolicy| {
        p.policy_version = 9;
        p.allowed_sequence_ids.push(SequenceId::new());
        p.operations = vec![MemoryOperation::Store, MemoryOperation::Search];
        p.default_retention_secs = 86_400;
        p.max_retention_secs = 604_800;
    },
    true
);
