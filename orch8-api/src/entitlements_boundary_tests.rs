//! Additional boundary coverage for provider-neutral plan admission.
//!
//! Count contract: 40 independently named unit tests.

use super::*;

fn valid_plan() -> PlanEntitlements {
    PlanEntitlements {
        plan_id: "team".into(),
        max_active_instances: 10,
        max_batch_instances: 100,
        max_context_bytes: 4096,
        allowed_namespaces: BTreeSet::new(),
        features: BTreeSet::new(),
    }
}

macro_rules! validation_case {
    ($name:ident, $mutate:expr, $valid:expr) => {
        #[test]
        fn $name() {
            let mut plan = valid_plan();
            ($mutate)(&mut plan);
            let result = plan.validate();
            assert_eq!(result.is_ok(), $valid);
        }
    };
}

validation_case!(
    coverage_entitlement_001_empty_plan_id_is_invalid,
    |p: &mut PlanEntitlements| p.plan_id.clear(),
    false
);
validation_case!(
    coverage_entitlement_002_single_character_plan_id_is_valid,
    |p: &mut PlanEntitlements| p.plan_id = "x".into(),
    true
);
validation_case!(
    coverage_entitlement_003_whitespace_plan_id_is_valid,
    |p: &mut PlanEntitlements| p.plan_id = " ".into(),
    true
);
validation_case!(
    coverage_entitlement_004_unicode_plan_id_is_valid,
    |p: &mut PlanEntitlements| p.plan_id = "plano-pró".into(),
    true
);
validation_case!(
    coverage_entitlement_005_hyphenated_plan_id_is_valid,
    |p: &mut PlanEntitlements| p.plan_id = "team-pro".into(),
    true
);
validation_case!(
    coverage_entitlement_006_long_plan_id_does_not_change_limits,
    |p: &mut PlanEntitlements| p.plan_id = "x".repeat(512),
    true
);
validation_case!(
    coverage_entitlement_007_zero_active_instances_is_invalid,
    |p: &mut PlanEntitlements| p.max_active_instances = 0,
    false
);
validation_case!(
    coverage_entitlement_008_one_active_instance_is_valid,
    |p: &mut PlanEntitlements| p.max_active_instances = 1,
    true
);
validation_case!(
    coverage_entitlement_009_large_active_instance_limit_is_valid,
    |p: &mut PlanEntitlements| p.max_active_instances = 1_000_000,
    true
);
validation_case!(
    coverage_entitlement_010_unlimited_active_instances_is_valid,
    |p: &mut PlanEntitlements| p.max_active_instances = u64::MAX,
    true
);
validation_case!(
    coverage_entitlement_011_zero_batch_limit_is_invalid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 0,
    false
);
validation_case!(
    coverage_entitlement_012_one_batch_item_is_valid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 1,
    true
);
validation_case!(
    coverage_entitlement_013_batch_limit_99_is_valid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 99,
    true
);
validation_case!(
    coverage_entitlement_014_batch_limit_100_is_valid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 100,
    true
);
validation_case!(
    coverage_entitlement_015_batch_limit_9999_is_valid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 9_999,
    true
);
validation_case!(
    coverage_entitlement_016_batch_limit_10000_is_valid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 10_000,
    true
);
validation_case!(
    coverage_entitlement_017_batch_limit_10001_is_invalid,
    |p: &mut PlanEntitlements| p.max_batch_instances = 10_001,
    false
);
validation_case!(
    coverage_entitlement_018_maximum_u32_batch_is_invalid,
    |p: &mut PlanEntitlements| p.max_batch_instances = u32::MAX,
    false
);
validation_case!(
    coverage_entitlement_019_zero_context_bytes_is_invalid,
    |p: &mut PlanEntitlements| p.max_context_bytes = 0,
    false
);
validation_case!(
    coverage_entitlement_020_one_context_byte_is_valid,
    |p: &mut PlanEntitlements| p.max_context_bytes = 1,
    true
);
validation_case!(
    coverage_entitlement_021_context_limit_1024_is_valid,
    |p: &mut PlanEntitlements| p.max_context_bytes = 1024,
    true
);
validation_case!(
    coverage_entitlement_022_context_limit_one_megabyte_is_valid,
    |p: &mut PlanEntitlements| p.max_context_bytes = 1_048_576,
    true
);
validation_case!(
    coverage_entitlement_023_maximum_context_limit_is_valid,
    |p: &mut PlanEntitlements| p.max_context_bytes = u32::MAX,
    true
);
validation_case!(
    coverage_entitlement_024_empty_namespace_set_is_valid,
    |p: &mut PlanEntitlements| p.allowed_namespaces.clear(),
    true
);
validation_case!(
    coverage_entitlement_025_single_namespace_is_valid,
    |p: &mut PlanEntitlements| p.allowed_namespaces.insert("prod".into()),
    true
);
validation_case!(
    coverage_entitlement_026_unicode_namespace_policy_is_valid,
    |p: &mut PlanEntitlements| p.allowed_namespaces.insert("produção".into()),
    true
);
validation_case!(
    coverage_entitlement_027_empty_feature_set_is_valid,
    |p: &mut PlanEntitlements| p.features.clear(),
    true
);
validation_case!(
    coverage_entitlement_028_named_feature_is_valid,
    |p: &mut PlanEntitlements| p.features.insert("continuity".into()),
    true
);
validation_case!(
    coverage_entitlement_029_multiple_features_are_valid,
    |p: &mut PlanEntitlements| {
        p.features.insert("continuity".into());
        p.features.insert("mobile".into());
    },
    true
);
validation_case!(
    coverage_entitlement_030_unlimited_plan_validates,
    |p: &mut PlanEntitlements| *p = PlanEntitlements::unlimited(),
    true
);

macro_rules! catalog_case {
    ($name:ident, $configured_tenant:expr, $lookup_tenant:expr, $expected_plan:expr) => {
        #[test]
        fn $name() {
            let configured = TenantId::new($configured_tenant).unwrap();
            let mut plan = valid_plan();
            plan.plan_id = $expected_plan.into();
            let catalog = StaticEntitlementCatalog::new(
                HashMap::from([(configured, plan)]),
                PlanEntitlements::unlimited(),
            )
            .unwrap();
            let actual = catalog
                .entitlements_for(&TenantId::new($lookup_tenant).unwrap())
                .plan_id;
            assert_eq!(actual, $expected_plan);
        }
    };
}

catalog_case!(
    coverage_entitlement_031_catalog_matches_simple_tenant,
    "tenant-a",
    "tenant-a",
    "team-a"
);
catalog_case!(
    coverage_entitlement_032_catalog_matches_numeric_tenant,
    "tenant-42",
    "tenant-42",
    "team-42"
);
catalog_case!(
    coverage_entitlement_033_catalog_matches_dotted_tenant,
    "tenant.eu",
    "tenant.eu",
    "team-eu"
);
catalog_case!(
    coverage_entitlement_034_catalog_matches_underscored_tenant,
    "tenant_ops",
    "tenant_ops",
    "ops"
);
catalog_case!(
    coverage_entitlement_035_catalog_matches_long_tenant,
    "tenant-enterprise-production",
    "tenant-enterprise-production",
    "enterprise"
);

macro_rules! fallback_case {
    ($name:ident, $configured_tenant:expr, $lookup_tenant:expr) => {
        #[test]
        fn $name() {
            let catalog = StaticEntitlementCatalog::new(
                HashMap::from([(TenantId::new($configured_tenant).unwrap(), valid_plan())]),
                PlanEntitlements::unlimited(),
            )
            .unwrap();
            let actual = catalog
                .entitlements_for(&TenantId::new($lookup_tenant).unwrap())
                .plan_id;
            assert_eq!(actual, "self_managed");
        }
    };
}

fallback_case!(
    coverage_entitlement_036_catalog_falls_back_for_other_tenant,
    "tenant-a",
    "tenant-b"
);
fallback_case!(
    coverage_entitlement_037_catalog_falls_back_for_prefix_collision,
    "tenant",
    "tenant-child"
);
fallback_case!(
    coverage_entitlement_038_catalog_falls_back_for_case_difference,
    "tenant-a",
    "Tenant-a"
);
fallback_case!(
    coverage_entitlement_039_catalog_falls_back_for_suffix_difference,
    "tenant-a",
    "tenant-a-1"
);
fallback_case!(
    coverage_entitlement_040_catalog_falls_back_for_numeric_difference,
    "tenant-1",
    "tenant-2"
);
