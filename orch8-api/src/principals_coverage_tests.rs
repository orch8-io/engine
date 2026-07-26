//! Coverage for capability-scoped principal admission (`capabilities_allow`)
//! and the [`PrincipalContext`] extension.
//!
//! These tests pin the route-family matrix enforced by `api_key_middleware`
//! before routing: which capability grants which path family, how the
//! `/api/v1` prefix is stripped, that matching is prefix-based (not
//! segment-based), that `Auditor` is method-gated, and that an empty grant
//! denies everything.
//!
//! Count contract: 67 independently named unit tests.

use super::*;

use axum::http::Method;

macro_rules! allow_case {
    ($name:ident, $caps:expr, $method:expr, $path:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let caps: &[ApiCapability] = $caps;
            let actual = capabilities_allow(caps, &$method, $path);
            assert_eq!(
                actual, $expected,
                "caps={:?} method={} path={}",
                caps, $method, $path
            );
        }
    };
}

// --- Operator: full control plane, short-circuits every check. ---

allow_case!(
    coverage_principal_001_operator_allows_instance_create,
    &[ApiCapability::Operator],
    Method::POST,
    "/api/v1/instances",
    true
);
allow_case!(
    coverage_principal_002_operator_allows_release_delete,
    &[ApiCapability::Operator],
    Method::DELETE,
    "/api/v1/releases/rel-1",
    true
);
allow_case!(
    coverage_principal_003_operator_allows_unknown_path,
    &[ApiCapability::Operator],
    Method::GET,
    "/api/v1/not-a-real-route",
    true
);
allow_case!(
    coverage_principal_004_operator_allows_root_path,
    &[ApiCapability::Operator],
    Method::GET,
    "/",
    true
);
allow_case!(
    coverage_principal_005_operator_allows_empty_path,
    &[ApiCapability::Operator],
    Method::GET,
    "",
    true
);
allow_case!(
    coverage_principal_006_operator_in_mixed_grant_still_allows_everything,
    &[ApiCapability::Worker, ApiCapability::Operator],
    Method::POST,
    "/api/v1/releases",
    true
);

// --- Worker: /workers family plus exactly /handlers. ---

allow_case!(
    coverage_principal_007_worker_allows_task_poll,
    &[ApiCapability::Worker],
    Method::POST,
    "/api/v1/workers/tasks/poll",
    true
);
allow_case!(
    coverage_principal_008_worker_allows_workers_collection,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/workers",
    true
);
allow_case!(
    coverage_principal_009_worker_allows_handlers_exact,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/handlers",
    true
);
allow_case!(
    coverage_principal_010_worker_denies_handlers_subpath,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/handlers/echo",
    false
);
allow_case!(
    coverage_principal_011_worker_denies_releases,
    &[ApiCapability::Worker],
    Method::POST,
    "/api/v1/releases",
    false
);
allow_case!(
    coverage_principal_012_worker_denies_mobile,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/mobile/devices",
    false
);
allow_case!(
    coverage_principal_013_worker_denies_approvals,
    &[ApiCapability::Worker],
    Method::POST,
    "/api/v1/approvals",
    false
);
allow_case!(
    coverage_principal_014_worker_denies_instance_signals,
    &[ApiCapability::Worker],
    Method::POST,
    "/api/v1/instances/inst-1/signals",
    false
);
allow_case!(
    coverage_principal_015_worker_method_is_irrelevant_within_family,
    &[ApiCapability::Worker],
    Method::DELETE,
    "/api/v1/workers/worker-9",
    true
);
allow_case!(
    coverage_principal_016_worker_allows_unversioned_path,
    &[ApiCapability::Worker],
    Method::POST,
    "/workers/tasks/poll",
    true
);
// Matching is prefix-based, not segment-based: "/workersmith" starts with
// "/workers" and is therefore admitted. Pinned so a future tightening to
// segment matching is a deliberate, reviewed change.
allow_case!(
    coverage_principal_017_worker_prefix_collision_is_admitted,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/workersmith",
    true
);
allow_case!(
    coverage_principal_018_worker_denies_singular_worker,
    &[ApiCapability::Worker],
    Method::GET,
    "/api/v1/worker",
    false
);

// --- Device: /mobile family only. ---

allow_case!(
    coverage_principal_019_device_allows_mobile_sync,
    &[ApiCapability::Device],
    Method::POST,
    "/api/v1/mobile/sync",
    true
);
allow_case!(
    coverage_principal_020_device_allows_mobile_root,
    &[ApiCapability::Device],
    Method::GET,
    "/api/v1/mobile",
    true
);
allow_case!(
    coverage_principal_021_device_denies_workers,
    &[ApiCapability::Device],
    Method::GET,
    "/api/v1/workers",
    false
);
allow_case!(
    coverage_principal_022_device_prefix_collision_is_admitted,
    &[ApiCapability::Device],
    Method::GET,
    "/api/v1/mobilex",
    true
);
allow_case!(
    coverage_principal_023_device_denies_truncated_prefix,
    &[ApiCapability::Device],
    Method::GET,
    "/api/v1/mobil",
    false
);
allow_case!(
    coverage_principal_024_device_method_is_irrelevant_within_family,
    &[ApiCapability::Device],
    Method::DELETE,
    "/api/v1/mobile/devices/dev-1",
    true
);

// --- Publisher: /releases, /plugins, /sequences. ---

allow_case!(
    coverage_principal_025_publisher_allows_release_publish,
    &[ApiCapability::Publisher],
    Method::POST,
    "/api/v1/releases",
    true
);
allow_case!(
    coverage_principal_026_publisher_allows_release_read,
    &[ApiCapability::Publisher],
    Method::GET,
    "/api/v1/releases/rel-1",
    true
);
allow_case!(
    coverage_principal_027_publisher_allows_plugins,
    &[ApiCapability::Publisher],
    Method::POST,
    "/api/v1/plugins",
    true
);
allow_case!(
    coverage_principal_028_publisher_allows_sequences,
    &[ApiCapability::Publisher],
    Method::GET,
    "/api/v1/sequences/seq-1",
    true
);
allow_case!(
    coverage_principal_029_publisher_denies_instances,
    &[ApiCapability::Publisher],
    Method::POST,
    "/api/v1/instances",
    false
);
allow_case!(
    coverage_principal_030_publisher_prefix_collision_is_admitted,
    &[ApiCapability::Publisher],
    Method::GET,
    "/api/v1/releasesx",
    true
);
allow_case!(
    coverage_principal_031_publisher_allows_plugin_delete,
    &[ApiCapability::Publisher],
    Method::DELETE,
    "/api/v1/plugins/plug-1",
    true
);
allow_case!(
    coverage_principal_032_publisher_denies_bare_prefix,
    &[ApiCapability::Publisher],
    Method::GET,
    "/api/v1",
    false
);

// --- Approver: /approvals plus the instance-signals action only. ---

allow_case!(
    coverage_principal_033_approver_allows_approval_decision,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/approvals/appr-1/decide",
    true
);
allow_case!(
    coverage_principal_034_approver_allows_instance_signal,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/instances/inst-1/signals",
    true
);
allow_case!(
    coverage_principal_035_approver_signal_method_is_irrelevant,
    &[ApiCapability::Approver],
    Method::GET,
    "/api/v1/instances/inst-1/signals",
    true
);
allow_case!(
    coverage_principal_036_approver_denies_instance_state_transition,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/instances/inst-1/state",
    false
);
allow_case!(
    coverage_principal_037_approver_denies_instance_collection,
    &[ApiCapability::Approver],
    Method::GET,
    "/api/v1/instances",
    false
);
allow_case!(
    coverage_principal_038_approver_denies_bare_signals_path,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/signals",
    false
);
allow_case!(
    coverage_principal_039_approver_denies_instances_trailing_slash,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/instances/",
    false
);
// The signal grant is `starts_with("/instances/") && ends_with("/signals")`,
// so "/instances/signals" (no id segment) also matches. Pinned, not endorsed.
allow_case!(
    coverage_principal_040_approver_signals_without_id_segment_is_admitted,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/instances/signals",
    true
);
allow_case!(
    coverage_principal_041_approver_allows_approvals_collection,
    &[ApiCapability::Approver],
    Method::GET,
    "/api/v1/approvals",
    true
);
allow_case!(
    coverage_principal_042_approver_denies_workers,
    &[ApiCapability::Approver],
    Method::POST,
    "/api/v1/workers/tasks/poll",
    false
);

// --- Auditor: read-only verbs on any path. ---

allow_case!(
    coverage_principal_043_auditor_allows_get_changes,
    &[ApiCapability::Auditor],
    Method::GET,
    "/api/v1/changes",
    true
);
allow_case!(
    coverage_principal_044_auditor_allows_head,
    &[ApiCapability::Auditor],
    Method::HEAD,
    "/api/v1/changes",
    true
);
allow_case!(
    coverage_principal_045_auditor_get_works_in_any_family,
    &[ApiCapability::Auditor],
    Method::GET,
    "/api/v1/releases",
    true
);
allow_case!(
    coverage_principal_046_auditor_get_allows_unknown_path,
    &[ApiCapability::Auditor],
    Method::GET,
    "/api/v1/anything/at/all",
    true
);
allow_case!(
    coverage_principal_047_auditor_denies_post,
    &[ApiCapability::Auditor],
    Method::POST,
    "/api/v1/changes",
    false
);
allow_case!(
    coverage_principal_048_auditor_denies_put,
    &[ApiCapability::Auditor],
    Method::PUT,
    "/api/v1/instances/inst-1",
    false
);
allow_case!(
    coverage_principal_049_auditor_denies_patch,
    &[ApiCapability::Auditor],
    Method::PATCH,
    "/api/v1/instances/inst-1/state",
    false
);
allow_case!(
    coverage_principal_050_auditor_denies_delete,
    &[ApiCapability::Auditor],
    Method::DELETE,
    "/api/v1/instances/inst-1",
    false
);
allow_case!(
    coverage_principal_051_auditor_denies_options,
    &[ApiCapability::Auditor],
    Method::OPTIONS,
    "/api/v1/changes",
    false
);
allow_case!(
    coverage_principal_052_auditor_get_allows_unversioned_path,
    &[ApiCapability::Auditor],
    Method::GET,
    "/changes",
    true
);

// --- Grant union and deny-by-default. ---

allow_case!(
    coverage_principal_053_worker_plus_publisher_allows_releases,
    &[ApiCapability::Worker, ApiCapability::Publisher],
    Method::POST,
    "/api/v1/releases",
    true
);
allow_case!(
    coverage_principal_054_worker_plus_publisher_allows_workers,
    &[ApiCapability::Worker, ApiCapability::Publisher],
    Method::POST,
    "/api/v1/workers/tasks/poll",
    true
);
allow_case!(
    coverage_principal_055_worker_plus_publisher_denies_mobile,
    &[ApiCapability::Worker, ApiCapability::Publisher],
    Method::POST,
    "/api/v1/mobile/sync",
    false
);
allow_case!(
    coverage_principal_056_device_plus_auditor_allows_mobile_write,
    &[ApiCapability::Device, ApiCapability::Auditor],
    Method::POST,
    "/api/v1/mobile/sync",
    true
);
allow_case!(
    coverage_principal_057_device_plus_auditor_allows_foreign_read,
    &[ApiCapability::Device, ApiCapability::Auditor],
    Method::GET,
    "/api/v1/releases",
    true
);
allow_case!(
    coverage_principal_058_device_plus_auditor_denies_foreign_write,
    &[ApiCapability::Device, ApiCapability::Auditor],
    Method::POST,
    "/api/v1/releases",
    false
);
allow_case!(
    coverage_principal_059_empty_grant_denies_reads,
    &[],
    Method::GET,
    "/api/v1/changes",
    false
);
allow_case!(
    coverage_principal_060_empty_grant_denies_worker_family,
    &[],
    Method::GET,
    "/api/v1/workers",
    false
);
allow_case!(
    coverage_principal_061_duplicate_capability_behaves_like_single,
    &[ApiCapability::Worker, ApiCapability::Worker],
    Method::POST,
    "/api/v1/workers/tasks/poll",
    true
);
// The prefix is stripped at most once: a doubled prefix leaves a path that
// no longer starts with "/workers".
allow_case!(
    coverage_principal_062_doubled_prefix_is_denied_for_worker,
    &[ApiCapability::Worker],
    Method::POST,
    "/api/v1/api/v1/workers/tasks/poll",
    false
);
allow_case!(
    coverage_principal_063_approver_plus_auditor_allows_signal_write,
    &[ApiCapability::Approver, ApiCapability::Auditor],
    Method::POST,
    "/api/v1/instances/inst-1/signals",
    true
);
allow_case!(
    coverage_principal_064_approver_plus_auditor_denies_other_writes,
    &[ApiCapability::Approver, ApiCapability::Auditor],
    Method::POST,
    "/api/v1/instances",
    false
);

// --- PrincipalContext extension value semantics. ---

#[test]
fn coverage_principal_065_principal_context_clone_preserves_grant() {
    let principal = PrincipalContext {
        key_id: "ak_123".into(),
        capabilities: vec![ApiCapability::Worker, ApiCapability::Auditor],
    };
    let cloned = principal.clone();
    assert_eq!(cloned.key_id, "ak_123");
    assert_eq!(
        cloned.capabilities,
        vec![ApiCapability::Worker, ApiCapability::Auditor]
    );
}

#[test]
fn coverage_principal_066_principal_context_debug_mentions_key_id() {
    // The middleware logs/echoes principals through Debug in several
    // observability paths; the key id (a public `ak_…` identifier, never the
    // secret) must remain visible for audit trails.
    let principal = PrincipalContext {
        key_id: "ak_audit".into(),
        capabilities: vec![ApiCapability::Device],
    };
    let rendered = format!("{principal:?}");
    assert!(rendered.contains("ak_audit"));
}

#[test]
fn coverage_principal_067_principal_context_preserves_capability_order() {
    // Grants are stored verbatim from the key record; ordering is part of
    // what operators see when they inspect a principal.
    let principal = PrincipalContext {
        key_id: "ak_order".into(),
        capabilities: vec![
            ApiCapability::Auditor,
            ApiCapability::Publisher,
            ApiCapability::Worker,
        ],
    };
    assert_eq!(
        principal.capabilities,
        vec![
            ApiCapability::Auditor,
            ApiCapability::Publisher,
            ApiCapability::Worker
        ]
    );
}
