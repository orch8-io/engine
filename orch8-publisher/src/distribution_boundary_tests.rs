//! Distribution input-validation boundaries.
//!
//! Count contract: 70 independently named unit tests.

use super::*;

macro_rules! path_case {
    ($name:ident, $path:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let result = validate_relative_path($path);
            assert_eq!(result.is_ok(), $valid);
        }
    };
}

path_case!(
    coverage_distribution_001_simple_file_is_valid,
    "a.txt",
    true
);
path_case!(
    coverage_distribution_002_nested_file_is_valid,
    "dir/a.txt",
    true
);
path_case!(
    coverage_distribution_003_deep_file_is_valid,
    "a/b/c.txt",
    true
);
path_case!(
    coverage_distribution_004_dot_component_is_valid,
    "./a.txt",
    true
);
path_case!(
    coverage_distribution_005_hidden_file_is_valid,
    ".manifest",
    true
);
path_case!(
    coverage_distribution_006_hyphenated_file_is_valid,
    "my-file.txt",
    true
);
path_case!(
    coverage_distribution_007_underscored_file_is_valid,
    "my_file.txt",
    true
);
path_case!(
    coverage_distribution_008_spaced_file_is_valid,
    "my file.txt",
    true
);
path_case!(
    coverage_distribution_009_unicode_file_is_valid,
    "dados/ação.json",
    true
);
path_case!(
    coverage_distribution_010_colon_file_is_valid,
    "schema:v1.json",
    true
);
path_case!(
    coverage_distribution_011_percent_file_is_valid,
    "asset%20name",
    true
);
path_case!(
    coverage_distribution_012_tilde_file_is_valid,
    "asset~backup",
    true
);
path_case!(coverage_distribution_013_single_dot_is_valid, ".", true);
path_case!(
    coverage_distribution_014_parent_text_is_valid,
    "..file",
    true
);
path_case!(
    coverage_distribution_015_long_relative_file_is_valid,
    "a".repeat(1024).as_str(),
    true
);
path_case!(coverage_distribution_016_empty_path_is_invalid, "", false);
path_case!(coverage_distribution_017_root_path_is_invalid, "/", false);
path_case!(
    coverage_distribution_018_absolute_unix_path_is_invalid,
    "/etc/passwd",
    false
);
path_case!(
    coverage_distribution_019_parent_component_is_invalid,
    "../a.txt",
    false
);
path_case!(
    coverage_distribution_020_nested_parent_component_is_invalid,
    "a/../b.txt",
    false
);
path_case!(
    coverage_distribution_021_trailing_parent_component_is_invalid,
    "a/..",
    false
);
path_case!(
    coverage_distribution_022_double_slash_is_invalid,
    "a//b",
    false
);
path_case!(
    coverage_distribution_023_trailing_slash_is_invalid,
    "a/",
    false
);
path_case!(
    coverage_distribution_024_leading_double_slash_is_invalid,
    "//server",
    false
);
path_case!(
    coverage_distribution_025_backslash_is_invalid,
    "a\\b",
    false
);
path_case!(
    coverage_distribution_026_windows_drive_path_is_invalid,
    "C:\\a",
    false
);
path_case!(coverage_distribution_027_nul_is_invalid, "a\0b", false);
path_case!(
    coverage_distribution_028_parent_between_deep_parts_is_invalid,
    "a/b/../c",
    false
);
path_case!(
    coverage_distribution_029_empty_component_after_dot_is_invalid,
    ".//a",
    false
);
path_case!(
    coverage_distribution_030_only_parent_component_is_invalid,
    "..",
    false
);

macro_rules! hash_case {
    ($name:ident, $hash:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let actual = hash_is_valid($hash);
            assert_eq!(actual, $valid);
        }
    };
}

fn hash_is_valid(hash: impl AsRef<str>) -> bool {
    validate_hash(hash.as_ref()).is_ok()
}

hash_case!(
    coverage_distribution_031_lowercase_a_hash_is_valid,
    "a".repeat(64),
    true
);
hash_case!(
    coverage_distribution_032_lowercase_f_hash_is_valid,
    "f".repeat(64),
    true
);
hash_case!(
    coverage_distribution_033_uppercase_a_hash_is_valid,
    "A".repeat(64),
    true
);
hash_case!(
    coverage_distribution_034_uppercase_f_hash_is_valid,
    "F".repeat(64),
    true
);
hash_case!(
    coverage_distribution_035_zero_hash_is_valid,
    "0".repeat(64),
    true
);
hash_case!(
    coverage_distribution_036_nine_hash_is_valid,
    "9".repeat(64),
    true
);
hash_case!(
    coverage_distribution_037_mixed_case_hash_is_valid,
    "aF".repeat(32),
    true
);
hash_case!(
    coverage_distribution_038_numeric_hash_is_valid,
    "0123456789".repeat(6) + "0123",
    true
);
hash_case!(
    coverage_distribution_039_full_hex_pattern_is_valid,
    "0123456789abcdef".repeat(4),
    true
);
hash_case!(
    coverage_distribution_040_upper_hex_pattern_is_valid,
    "0123456789ABCDEF".repeat(4),
    true
);
hash_case!(
    coverage_distribution_041_empty_hash_is_invalid,
    String::new(),
    false
);
hash_case!(
    coverage_distribution_042_one_character_hash_is_invalid,
    "a",
    false
);
hash_case!(
    coverage_distribution_043_63_character_hash_is_invalid,
    "a".repeat(63),
    false
);
hash_case!(
    coverage_distribution_044_65_character_hash_is_invalid,
    "a".repeat(65),
    false
);
hash_case!(
    coverage_distribution_045_sha1_length_hash_is_invalid,
    "a".repeat(40),
    false
);
hash_case!(
    coverage_distribution_046_g_character_hash_is_invalid,
    "g".repeat(64),
    false
);
hash_case!(
    coverage_distribution_047_z_character_hash_is_invalid,
    "z".repeat(64),
    false
);
hash_case!(
    coverage_distribution_048_hyphenated_hash_is_invalid,
    format!("{}-{}", "a".repeat(31), "b".repeat(32)),
    false
);
hash_case!(
    coverage_distribution_049_spaced_hash_is_invalid,
    format!("{} {}", "a".repeat(31), "b".repeat(32)),
    false
);
hash_case!(
    coverage_distribution_050_prefixed_hash_is_invalid,
    format!("sha256:{}", "a".repeat(64)),
    false
);
hash_case!(
    coverage_distribution_051_newline_hash_is_invalid,
    format!("{}\n", "a".repeat(63)),
    false
);
hash_case!(
    coverage_distribution_052_unicode_hash_is_invalid,
    "é".repeat(32),
    false
);
hash_case!(
    coverage_distribution_053_slash_hash_is_invalid,
    "/".repeat(64),
    false
);
hash_case!(
    coverage_distribution_054_nul_hash_is_invalid,
    "\0".repeat(64),
    false
);
hash_case!(
    coverage_distribution_055_128_character_hash_is_invalid,
    "a".repeat(128),
    false
);

macro_rules! requirement_case {
    ($name:ident, $kind:expr, $required:expr, $available:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let required: Vec<String> = $required.into_iter().map(str::to_string).collect();
            let available: Vec<String> = $available.into_iter().map(str::to_string).collect();
            let result = require_all($kind, &required, &available);
            assert_eq!(result.is_ok(), $valid);
        }
    };
}

requirement_case!(
    coverage_distribution_056_empty_requirements_are_satisfied,
    "handler",
    Vec::<&str>::new(),
    Vec::<&str>::new(),
    true
);
requirement_case!(
    coverage_distribution_057_single_handler_is_satisfied,
    "handler",
    vec!["noop"],
    vec!["noop"],
    true
);
requirement_case!(
    coverage_distribution_058_handler_can_be_among_extras,
    "handler",
    vec!["noop"],
    vec!["log", "noop", "http"],
    true
);
requirement_case!(
    coverage_distribution_059_multiple_handlers_are_satisfied,
    "handler",
    vec!["noop", "log"],
    vec!["log", "noop"],
    true
);
requirement_case!(
    coverage_distribution_060_plugin_requirement_is_satisfied,
    "plugin",
    vec!["ocr"],
    vec!["ocr"],
    true
);
requirement_case!(
    coverage_distribution_061_region_requirement_is_satisfied,
    "region",
    vec!["br"],
    vec!["us", "br"],
    true
);
requirement_case!(
    coverage_distribution_062_hardware_requirement_is_satisfied,
    "hardware",
    vec!["camera"],
    vec!["camera", "gps"],
    true
);
requirement_case!(
    coverage_distribution_063_credential_requirement_is_satisfied,
    "credential",
    vec!["vault"],
    vec!["vault"],
    true
);
requirement_case!(
    coverage_distribution_064_missing_handler_is_rejected,
    "handler",
    vec!["noop"],
    Vec::<&str>::new(),
    false
);
requirement_case!(
    coverage_distribution_065_one_missing_of_two_is_rejected,
    "handler",
    vec!["noop", "log"],
    vec!["noop"],
    false
);
requirement_case!(
    coverage_distribution_066_case_changed_handler_is_rejected,
    "handler",
    vec!["noop"],
    vec!["NOOP"],
    false
);
requirement_case!(
    coverage_distribution_067_prefix_handler_is_rejected,
    "handler",
    vec!["noop"],
    vec!["noop-v2"],
    false
);
requirement_case!(
    coverage_distribution_068_missing_plugin_is_rejected,
    "plugin",
    vec!["ocr"],
    vec!["vision"],
    false
);
requirement_case!(
    coverage_distribution_069_missing_region_is_rejected,
    "region",
    vec!["br"],
    vec!["us"],
    false
);
requirement_case!(
    coverage_distribution_070_missing_hardware_is_rejected,
    "hardware",
    vec!["gpu"],
    vec!["cpu"],
    false
);
