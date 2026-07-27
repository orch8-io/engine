//! Admission-decision coverage for [`admit_instances`] plus serde and
//! catalog-construction edges not exercised by the boundary tests.
//!
//! `admit_instances` only reads `state.entitlements`, but [`AppState`] is a
//! concrete struct, so tests build it with the same in-memory `SQLite`
//! backend the shared test harness uses. Storage is never touched by the
//! code under test.
//!
//! Count contract: 55 independently named unit tests.

use super::*;

use tokio_util::sync::CancellationToken;

/// Provider stub that hands every tenant the same plan.
struct FixedProvider(PlanEntitlements);

impl EntitlementProvider for FixedProvider {
    fn entitlements_for(&self, _tenant_id: &TenantId) -> PlanEntitlements {
        self.0.clone()
    }
}

async fn state_with(provider: Arc<dyn EntitlementProvider>) -> AppState {
    let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
        .await
        .expect("in-memory sqlite storage must initialise for tests");
    AppState {
        storage: Arc::new(storage),
        shutdown: CancellationToken::new(),
        max_context_bytes: 0,
        externalization_mode: orch8_types::config::ExternalizationMode::default(),
        circuit_breakers: None,
        stream_limiter: Arc::new(tokio::sync::Semaphore::new(1)),
        publisher: None,
        push_provider: Arc::new(orch8_push::NoopPushProvider),
        mobile_sync_enabled: false,
        entitlements: provider,
        builtin_handlers: Arc::new(Vec::new()),
        engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        continuity_crypto: None,
        federation_peers: Arc::new(Vec::new()),
        continuity_lab_enabled: false,
    }
}

async fn state_with_plan(plan: PlanEntitlements) -> AppState {
    state_with(Arc::new(FixedProvider(plan))).await
}

/// Restrictive plan: 3 per batch, 1024 context bytes, `prod` namespace only.
fn team_plan() -> PlanEntitlements {
    PlanEntitlements {
        plan_id: "team".into(),
        max_active_instances: 10,
        max_batch_instances: 3,
        max_context_bytes: 1024,
        allowed_namespaces: BTreeSet::from(["prod".to_string()]),
        features: BTreeSet::new(),
    }
}

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).expect("test tenant id must be valid")
}

fn ns(list: &[&str]) -> Vec<Namespace> {
    list.iter().map(|s| Namespace::new(*s)).collect()
}

macro_rules! admit_ok_case {
    ($name:ident, $plan:expr, $namespaces:expr, $requested:expr, $context:expr) => {
        #[tokio::test]
        async fn $name() {
            let state = state_with_plan($plan).await;
            let result = admit_instances(
                &state,
                &tenant("tenant-a"),
                &$namespaces,
                $requested,
                $context,
            );
            assert!(result.is_ok(), "expected admission, got {result:?}");
        }
    };
}

macro_rules! admit_err_case {
    ($name:ident, $plan:expr, $namespaces:expr, $requested:expr, $context:expr, $variant:pat) => {
        #[tokio::test]
        async fn $name() {
            let state = state_with_plan($plan).await;
            let err = admit_instances(
                &state,
                &tenant("tenant-a"),
                &$namespaces,
                $requested,
                $context,
            )
            .expect_err("admission must fail");
            assert!(matches!(err, $variant), "wrong error variant: {err:?}");
        }
    };
}

// --- Batch-size boundary (max_batch_instances = 3). ---

admit_ok_case!(
    coverage_admission_001_zero_requested_is_admitted,
    team_plan(),
    ns(&["prod"]),
    0,
    0
);
admit_ok_case!(
    coverage_admission_002_single_instance_is_admitted,
    team_plan(),
    ns(&["prod"]),
    1,
    0
);
admit_ok_case!(
    coverage_admission_003_exact_batch_limit_is_admitted,
    team_plan(),
    ns(&["prod"]),
    3,
    0
);
admit_err_case!(
    coverage_admission_004_one_over_batch_limit_is_rate_limited,
    team_plan(),
    ns(&["prod"]),
    4,
    0,
    ApiError::RateLimited(_)
);
admit_err_case!(
    coverage_admission_005_huge_request_is_rate_limited,
    team_plan(),
    ns(&["prod"]),
    1_000_000,
    0,
    ApiError::RateLimited(_)
);

#[tokio::test]
async fn coverage_admission_006_batch_error_names_the_plan() {
    let state = state_with_plan(team_plan()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 4, 0)
        .expect_err("must be rate limited");
    let ApiError::RateLimited(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains("team"),
        "message must name the plan: {message}"
    );
}

#[tokio::test]
async fn coverage_admission_007_batch_error_quotes_the_limit() {
    let state = state_with_plan(team_plan()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 4, 0)
        .expect_err("must be rate limited");
    let ApiError::RateLimited(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains('3'),
        "message must quote the limit: {message}"
    );
}

// --- Context-size boundary (max_context_bytes = 1024). ---

admit_ok_case!(
    coverage_admission_008_zero_context_is_admitted,
    team_plan(),
    ns(&["prod"]),
    1,
    0
);
admit_ok_case!(
    coverage_admission_009_exact_context_limit_is_admitted,
    team_plan(),
    ns(&["prod"]),
    1,
    1024
);
admit_err_case!(
    coverage_admission_010_one_byte_over_context_limit_is_rejected,
    team_plan(),
    ns(&["prod"]),
    1,
    1025,
    ApiError::PayloadTooLarge(_)
);

#[tokio::test]
async fn coverage_admission_011_context_error_names_the_plan() {
    let state = state_with_plan(team_plan()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 1, 2048)
        .expect_err("must be payload too large");
    let ApiError::PayloadTooLarge(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains("team"),
        "message must name the plan: {message}"
    );
}

#[tokio::test]
async fn coverage_admission_012_context_error_quotes_the_limit() {
    let state = state_with_plan(team_plan()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 1, 2048)
        .expect_err("must be payload too large");
    let ApiError::PayloadTooLarge(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains("1024"),
        "message must quote the byte limit: {message}"
    );
}

// When both the batch and the context limits are exceeded the batch check
// fires first — clients fix limits one at a time in a stable order.
admit_err_case!(
    coverage_admission_013_batch_is_checked_before_context,
    team_plan(),
    ns(&["prod"]),
    4,
    2048,
    ApiError::RateLimited(_)
);

// --- Namespace entitlement. ---

#[test]
fn coverage_admission_014_plan_fixture_is_valid() {
    // Guards the fixture itself: every admission case above assumes
    // team_plan() passes validate(), so pin that here.
    assert!(team_plan().validate().is_ok());
}

admit_ok_case!(
    coverage_admission_015_empty_namespace_set_admits_any_namespace,
    PlanEntitlements {
        allowed_namespaces: BTreeSet::new(),
        ..team_plan()
    },
    ns(&["anything", "else"]),
    1,
    0
);
admit_ok_case!(
    coverage_admission_016_entitled_namespace_is_admitted,
    team_plan(),
    ns(&["prod"]),
    1,
    0
);
admit_err_case!(
    coverage_admission_017_unentitled_namespace_is_forbidden,
    team_plan(),
    ns(&["dev"]),
    1,
    0,
    ApiError::Forbidden(_)
);

#[tokio::test]
async fn coverage_admission_018_namespace_error_names_the_plan() {
    let state = state_with_plan(team_plan()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["dev"]), 1, 0)
        .expect_err("must be forbidden");
    let ApiError::Forbidden(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains("team"),
        "message must name the plan: {message}"
    );
}

// Namespace matching is case-sensitive: "Prod" is not "prod".
admit_err_case!(
    coverage_admission_019_namespace_matching_is_case_sensitive,
    team_plan(),
    ns(&["Prod"]),
    1,
    0,
    ApiError::Forbidden(_)
);
admit_ok_case!(
    coverage_admission_020_all_listed_namespaces_entitled_is_admitted,
    PlanEntitlements {
        allowed_namespaces: BTreeSet::from(["prod".to_string(), "staging".to_string()]),
        ..team_plan()
    },
    ns(&["prod", "staging"]),
    1,
    0
);
admit_err_case!(
    coverage_admission_021_one_unentitled_namespace_fails_the_whole_request,
    PlanEntitlements {
        allowed_namespaces: BTreeSet::from(["prod".to_string(), "staging".to_string()]),
        ..team_plan()
    },
    ns(&["prod", "dev"]),
    1,
    0,
    ApiError::Forbidden(_)
);
admit_err_case!(
    coverage_admission_022_empty_namespace_string_is_not_implicitly_entitled,
    team_plan(),
    ns(&[""]),
    1,
    0,
    ApiError::Forbidden(_)
);
// The context check fires before the namespace scan.
admit_err_case!(
    coverage_admission_023_context_is_checked_before_namespaces,
    team_plan(),
    ns(&["dev"]),
    1,
    2048,
    ApiError::PayloadTooLarge(_)
);

// --- Misconfigured plans fail closed with 503 Unavailable. ---

admit_err_case!(
    coverage_admission_024_zero_active_instance_plan_is_unavailable,
    PlanEntitlements {
        max_active_instances: 0,
        ..team_plan()
    },
    ns(&["prod"]),
    1,
    0,
    ApiError::Unavailable(_)
);

#[tokio::test]
async fn coverage_admission_025_unavailable_error_describes_misconfiguration() {
    let plan = PlanEntitlements {
        max_active_instances: 0,
        ..team_plan()
    };
    let state = state_with_plan(plan).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 1, 0)
        .expect_err("must be unavailable");
    let ApiError::Unavailable(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert!(
        message.contains("invalid entitlement configuration"),
        "message must describe the misconfiguration: {message}"
    );
}

admit_err_case!(
    coverage_admission_026_zero_batch_plan_is_unavailable,
    PlanEntitlements {
        max_batch_instances: 0,
        ..team_plan()
    },
    ns(&["prod"]),
    1,
    0,
    ApiError::Unavailable(_)
);
admit_err_case!(
    coverage_admission_027_over_bound_batch_plan_is_unavailable,
    PlanEntitlements {
        max_batch_instances: 10_001,
        ..team_plan()
    },
    ns(&["prod"]),
    1,
    0,
    ApiError::Unavailable(_)
);
admit_err_case!(
    coverage_admission_028_empty_plan_id_is_unavailable,
    PlanEntitlements {
        plan_id: String::new(),
        ..team_plan()
    },
    ns(&["prod"]),
    1,
    0,
    ApiError::Unavailable(_)
);
admit_err_case!(
    coverage_admission_029_zero_context_plan_is_unavailable,
    PlanEntitlements {
        max_context_bytes: 0,
        ..team_plan()
    },
    ns(&["prod"]),
    1,
    0,
    ApiError::Unavailable(_)
);

// --- The admitted plan is returned for downstream storage admission. ---

#[tokio::test]
async fn coverage_admission_030_success_returns_the_admitted_plan() {
    let state = state_with_plan(team_plan()).await;
    let plan = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 2, 512)
        .expect("must be admitted");
    assert_eq!(plan.plan_id, "team");
    assert_eq!(plan.max_active_instances, 10);
}

#[tokio::test]
async fn coverage_admission_031_all_limits_at_exact_boundary_are_admitted() {
    let state = state_with_plan(team_plan()).await;
    let result = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 3, 1024);
    assert!(result.is_ok(), "exact boundaries must admit: {result:?}");
}

// --- Tenant scoping through StaticEntitlementCatalog. ---

#[tokio::test]
async fn coverage_admission_032_catalog_plan_restricts_configured_tenant() {
    let catalog = StaticEntitlementCatalog::new(
        HashMap::from([(tenant("tenant-a"), team_plan())]),
        PlanEntitlements::unlimited(),
    )
    .expect("catalog must build");
    let state = state_with(Arc::new(catalog)).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&["prod"]), 4, 0)
        .expect_err("configured tenant must hit the batch limit");
    assert!(matches!(err, ApiError::RateLimited(_)));
}

#[tokio::test]
async fn coverage_admission_033_catalog_fallback_admits_other_tenants() {
    let catalog = StaticEntitlementCatalog::new(
        HashMap::from([(tenant("tenant-a"), team_plan())]),
        PlanEntitlements::unlimited(),
    )
    .expect("catalog must build");
    let state = state_with(Arc::new(catalog)).await;
    let result = admit_instances(&state, &tenant("tenant-b"), &ns(&["dev"]), 4, 2048);
    assert!(result.is_ok(), "fallback plan must admit: {result:?}");
}

#[tokio::test]
async fn coverage_admission_034_unlimited_provider_admits_api_maximum_batch() {
    let state = state_with(unlimited_provider()).await;
    let result = admit_instances(
        &state,
        &tenant("tenant-a"),
        &ns(&["any"]),
        10_000,
        u32::MAX as usize,
    );
    assert!(
        result.is_ok(),
        "unlimited plan must admit the API maximum: {result:?}"
    );
}

#[tokio::test]
async fn coverage_admission_035_unlimited_provider_still_enforces_api_batch_bound() {
    let state = state_with(unlimited_provider()).await;
    let err = admit_instances(&state, &tenant("tenant-a"), &ns(&[]), 10_001, 0)
        .expect_err("even the unlimited plan is capped at 10_000 per request");
    assert!(matches!(err, ApiError::RateLimited(_)));
}

// --- PlanEntitlements serde contract (provider-neutral catalog loading). ---

#[test]
fn coverage_admission_036_serde_round_trip_preserves_every_field() {
    let plan = team_plan();
    let json = serde_json::to_string(&plan).expect("serialize");
    let back: PlanEntitlements = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, plan);
}

#[test]
fn coverage_admission_037_missing_namespaces_default_to_open() {
    let json = r#"{
        "plan_id": "team",
        "max_active_instances": 10,
        "max_batch_instances": 3,
        "max_context_bytes": 1024
    }"#;
    let plan: PlanEntitlements = serde_json::from_str(json).expect("defaults must apply");
    assert!(plan.allowed_namespaces.is_empty());
    assert!(plan.features.is_empty());
}

#[test]
fn coverage_admission_038_missing_required_limit_fails_to_parse() {
    let json = r#"{
        "plan_id": "team",
        "max_batch_instances": 3,
        "max_context_bytes": 1024
    }"#;
    assert!(serde_json::from_str::<PlanEntitlements>(json).is_err());
}

#[test]
fn coverage_admission_039_missing_plan_id_fails_to_parse() {
    let json = r#"{
        "max_active_instances": 10,
        "max_batch_instances": 3,
        "max_context_bytes": 1024
    }"#;
    assert!(serde_json::from_str::<PlanEntitlements>(json).is_err());
}

#[test]
fn coverage_admission_040_unknown_fields_are_ignored_for_forward_compatibility() {
    let json = r#"{
        "plan_id": "team",
        "max_active_instances": 10,
        "max_batch_instances": 3,
        "max_context_bytes": 1024,
        "billing_provider_customer_id": "cus_123"
    }"#;
    let plan: PlanEntitlements =
        serde_json::from_str(json).expect("unknown provider fields must not break parsing");
    assert_eq!(plan.plan_id, "team");
}

#[test]
fn coverage_admission_041_namespaces_serialize_in_sorted_order() {
    let plan = PlanEntitlements {
        allowed_namespaces: BTreeSet::from(["zebra".to_string(), "alpha".to_string()]),
        ..team_plan()
    };
    let value = serde_json::to_value(&plan).expect("serialize");
    let namespaces = value["allowed_namespaces"].as_array().expect("array");
    assert_eq!(namespaces[0], "alpha");
    assert_eq!(namespaces[1], "zebra");
}

#[test]
fn coverage_admission_055_serde_round_trip_preserves_non_empty_policy_sets() {
    // 036 round-trips the fixture whose policy sets are empty; pin that
    // populated namespaces/features survive the round trip verbatim too.
    let plan = PlanEntitlements {
        allowed_namespaces: BTreeSet::from(["prod".to_string(), "staging".to_string()]),
        features: BTreeSet::from(["continuity".to_string()]),
        ..team_plan()
    };
    let json = serde_json::to_string(&plan).expect("serialize");
    let back: PlanEntitlements = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back, plan);
}

// --- Catalog constructor validation. ---

#[test]
fn coverage_admission_042_catalog_rejects_invalid_fallback() {
    let fallback = PlanEntitlements {
        max_active_instances: 0,
        ..PlanEntitlements::unlimited()
    };
    assert!(StaticEntitlementCatalog::new(HashMap::new(), fallback).is_err());
}

#[test]
fn coverage_admission_043_catalog_rejects_invalid_member_plan() {
    let bad = PlanEntitlements {
        max_batch_instances: 0,
        ..team_plan()
    };
    let result = StaticEntitlementCatalog::new(
        HashMap::from([(tenant("tenant-a"), bad)]),
        PlanEntitlements::unlimited(),
    );
    assert!(result.is_err());
}

#[test]
fn coverage_admission_044_catalog_error_message_is_the_validation_message() {
    let bad = PlanEntitlements {
        plan_id: String::new(),
        ..team_plan()
    };
    let err = StaticEntitlementCatalog::new(
        HashMap::from([(tenant("tenant-a"), bad)]),
        PlanEntitlements::unlimited(),
    )
    .err()
    .expect("must reject the invalid plan");
    assert_eq!(err, "plan limits must be non-zero and within API bounds");
}

#[test]
fn coverage_admission_045_catalog_accepts_an_empty_plan_map() {
    let catalog = StaticEntitlementCatalog::new(HashMap::new(), PlanEntitlements::unlimited())
        .expect("empty catalog must build");
    assert_eq!(
        catalog.entitlements_for(&tenant("nobody")).plan_id,
        "self_managed"
    );
}

#[test]
fn coverage_admission_046_lookup_returns_an_independent_clone() {
    let catalog = StaticEntitlementCatalog::new(
        HashMap::from([(tenant("tenant-a"), team_plan())]),
        PlanEntitlements::unlimited(),
    )
    .expect("catalog must build");
    let mut first = catalog.entitlements_for(&tenant("tenant-a"));
    first.plan_id = "mutated".into();
    first.max_batch_instances = 1;
    let second = catalog.entitlements_for(&tenant("tenant-a"));
    assert_eq!(second.plan_id, "team");
    assert_eq!(second.max_batch_instances, 3);
}

// --- The unlimited (self-managed) contract. ---

#[test]
fn coverage_admission_047_unlimited_plan_id_is_self_managed() {
    assert_eq!(PlanEntitlements::unlimited().plan_id, "self_managed");
}

#[test]
fn coverage_admission_048_unlimited_active_instances_is_u64_max() {
    assert_eq!(PlanEntitlements::unlimited().max_active_instances, u64::MAX);
}

#[test]
fn coverage_admission_049_unlimited_batch_is_the_api_maximum() {
    assert_eq!(PlanEntitlements::unlimited().max_batch_instances, 10_000);
}

#[test]
fn coverage_admission_050_unlimited_context_is_u32_max() {
    assert_eq!(PlanEntitlements::unlimited().max_context_bytes, u32::MAX);
}

#[test]
fn coverage_admission_051_unlimited_leaves_policy_sets_open() {
    let plan = PlanEntitlements::unlimited();
    assert!(plan.allowed_namespaces.is_empty());
    assert!(plan.features.is_empty());
}

#[test]
fn coverage_admission_052_unlimited_entitlements_default_serves_any_tenant() {
    let provider = UnlimitedEntitlements;
    assert_eq!(
        provider.entitlements_for(&tenant("tenant-x")).plan_id,
        "self_managed"
    );
    assert_eq!(
        provider.entitlements_for(&tenant("tenant-y")).plan_id,
        "self_managed"
    );
}

#[test]
fn coverage_admission_053_unlimited_plan_passes_its_own_validation() {
    assert!(PlanEntitlements::unlimited().validate().is_ok());
}

#[test]
fn coverage_admission_054_validation_error_message_is_stable() {
    let plan = PlanEntitlements {
        max_active_instances: 0,
        ..PlanEntitlements::unlimited()
    };
    let err = plan.validate().expect_err("must be invalid");
    assert_eq!(err, "plan limits must be non-zero and within API bounds");
}
