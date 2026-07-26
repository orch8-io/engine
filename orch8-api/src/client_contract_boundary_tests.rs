//! Generated-client contract edge cases.
//!
//! Count contract: 20 independently named unit tests.

use super::*;

macro_rules! escape_case {
    ($name:ident, $input:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let actual = escape($input);
            assert_eq!(actual, $expected);
        }
    };
}

escape_case!(coverage_client_contract_001_empty_pointer_segment, "", "");
escape_case!(coverage_client_contract_002_root_slash, "/", "~1");
escape_case!(
    coverage_client_contract_003_instances_path,
    "/instances",
    "~1instances"
);
escape_case!(
    coverage_client_contract_004_nested_path,
    "/instances/{id}",
    "~1instances~1{id}"
);
escape_case!(
    coverage_client_contract_005_tilde_is_escaped_first,
    "~",
    "~0"
);
escape_case!(coverage_client_contract_006_tilde_then_slash, "~/", "~0~1");
escape_case!(coverage_client_contract_007_slash_then_tilde, "/~", "~1~0");
escape_case!(coverage_client_contract_008_double_tilde, "~~", "~0~0");
escape_case!(coverage_client_contract_009_double_slash, "//", "~1~1");
escape_case!(
    coverage_client_contract_010_literal_escape_text,
    "~1",
    "~01"
);
escape_case!(
    coverage_client_contract_011_unicode_path,
    "/sequências/ação",
    "~1sequências~1ação"
);
escape_case!(
    coverage_client_contract_012_query_like_text,
    "/items?x=1",
    "~1items?x=1"
);

macro_rules! generated_contains_case {
    ($name:ident, $generator:expr, $needle:expr) => {
        #[test]
        fn $name() {
            let generated = ($generator)();
            assert!(generated.contains($needle));
        }
    };
}

generated_contains_case!(
    coverage_client_contract_013_rust_has_instances_create,
    generate_rust_client,
    "(\"post\", \"/instances\")"
);
generated_contains_case!(
    coverage_client_contract_014_rust_has_instance_get,
    generate_rust_client,
    "(\"get\", \"/instances/{id}\")"
);
generated_contains_case!(
    coverage_client_contract_015_rust_has_release_validate,
    generate_rust_client,
    "(\"post\", \"/releases/{id}/validate\")"
);
generated_contains_case!(
    coverage_client_contract_016_rust_has_timeline_get,
    generate_rust_client,
    "(\"get\", \"/instances/{id}/timeline\")"
);
generated_contains_case!(
    coverage_client_contract_017_javascript_has_instances_create,
    generate_javascript_client,
    "method: \"post\", path: \"/instances\""
);
generated_contains_case!(
    coverage_client_contract_018_javascript_has_instance_get,
    generate_javascript_client,
    "method: \"get\", path: \"/instances/{id}\""
);
generated_contains_case!(
    coverage_client_contract_019_javascript_has_release_validate,
    generate_javascript_client,
    "method: \"post\", path: \"/releases/{id}/validate\""
);
generated_contains_case!(
    coverage_client_contract_020_javascript_has_timeline_get,
    generate_javascript_client,
    "method: \"get\", path: \"/instances/{id}/timeline\""
);
