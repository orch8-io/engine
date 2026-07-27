//! Coverage tests for hardened node role assembly (`NodeAssembly`) and the
//! per-role gRPC surface allow-lists.
//!
//! Count contract: 47 independently named unit tests.

use super::*;

macro_rules! assembly_case {
    ($name:ident, $role:expr, $field:ident, $expected:expr) => {
        #[test]
        fn $name() {
            let assembly = NodeAssembly::for_role($role);
            assert_eq!(assembly.$field, $expected);
        }
    };
}

// --- All-in-one: every surface enabled ---
assembly_case!(
    coverage_role_001_all_in_one_enables_full_api,
    NodeRole::AllInOne,
    full_api,
    true
);
assembly_case!(
    coverage_role_002_all_in_one_disables_continuity_gateway,
    NodeRole::AllInOne,
    continuity_gateway,
    false
);
assembly_case!(
    coverage_role_003_all_in_one_enables_public_webhooks,
    NodeRole::AllInOne,
    public_webhooks,
    true
);
assembly_case!(
    coverage_role_004_all_in_one_enables_engine,
    NodeRole::AllInOne,
    engine,
    true
);
assembly_case!(
    coverage_role_005_all_in_one_enables_push_outbox,
    NodeRole::AllInOne,
    push_outbox,
    true
);
assembly_case!(
    coverage_role_006_all_in_one_grpc_is_full,
    NodeRole::AllInOne,
    grpc,
    GrpcSurface::Full
);

// --- Control: API + push, no engine ---
assembly_case!(
    coverage_role_007_control_enables_full_api,
    NodeRole::Control,
    full_api,
    true
);
assembly_case!(
    coverage_role_008_control_disables_continuity_gateway,
    NodeRole::Control,
    continuity_gateway,
    false
);
assembly_case!(
    coverage_role_009_control_enables_public_webhooks,
    NodeRole::Control,
    public_webhooks,
    true
);
assembly_case!(
    coverage_role_010_control_disables_engine,
    NodeRole::Control,
    engine,
    false
);
assembly_case!(
    coverage_role_011_control_enables_push_outbox,
    NodeRole::Control,
    push_outbox,
    true
);
assembly_case!(
    coverage_role_012_control_grpc_is_full,
    NodeRole::Control,
    grpc,
    GrpcSurface::Full
);

// --- Executor: engine + bounded gRPC, no API at all ---
assembly_case!(
    coverage_role_013_executor_disables_full_api,
    NodeRole::Executor,
    full_api,
    false
);
assembly_case!(
    coverage_role_014_executor_disables_continuity_gateway,
    NodeRole::Executor,
    continuity_gateway,
    false
);
assembly_case!(
    coverage_role_015_executor_disables_public_webhooks,
    NodeRole::Executor,
    public_webhooks,
    false
);
assembly_case!(
    coverage_role_016_executor_enables_engine,
    NodeRole::Executor,
    engine,
    true
);
assembly_case!(
    coverage_role_017_executor_disables_push_outbox,
    NodeRole::Executor,
    push_outbox,
    false
);
assembly_case!(
    coverage_role_018_executor_grpc_is_bounded,
    NodeRole::Executor,
    grpc,
    GrpcSurface::Executor
);

// --- Gateway: continuity router + minimal gRPC only ---
assembly_case!(
    coverage_role_019_gateway_disables_full_api,
    NodeRole::Gateway,
    full_api,
    false
);
assembly_case!(
    coverage_role_020_gateway_enables_continuity_gateway,
    NodeRole::Gateway,
    continuity_gateway,
    true
);
assembly_case!(
    coverage_role_021_gateway_disables_public_webhooks,
    NodeRole::Gateway,
    public_webhooks,
    false
);
assembly_case!(
    coverage_role_022_gateway_disables_engine,
    NodeRole::Gateway,
    engine,
    false
);
assembly_case!(
    coverage_role_023_gateway_disables_push_outbox,
    NodeRole::Gateway,
    push_outbox,
    false
);
assembly_case!(
    coverage_role_024_gateway_grpc_is_continuity_gateway,
    NodeRole::Gateway,
    grpc,
    GrpcSurface::ContinuityGateway
);

// --- Edge: engine only, no listeners ---
assembly_case!(
    coverage_role_025_edge_disables_full_api,
    NodeRole::Edge,
    full_api,
    false
);
assembly_case!(
    coverage_role_026_edge_disables_continuity_gateway,
    NodeRole::Edge,
    continuity_gateway,
    false
);
assembly_case!(
    coverage_role_027_edge_disables_public_webhooks,
    NodeRole::Edge,
    public_webhooks,
    false
);
assembly_case!(
    coverage_role_028_edge_enables_engine,
    NodeRole::Edge,
    engine,
    true
);
assembly_case!(
    coverage_role_029_edge_disables_push_outbox,
    NodeRole::Edge,
    push_outbox,
    false
);
assembly_case!(
    coverage_role_030_edge_grpc_is_disabled,
    NodeRole::Edge,
    grpc,
    GrpcSurface::Disabled
);

// --- Cross-role invariants ---

const ALL_ROLES: [NodeRole; 5] = [
    NodeRole::AllInOne,
    NodeRole::Control,
    NodeRole::Executor,
    NodeRole::Gateway,
    NodeRole::Edge,
];

#[test]
fn coverage_role_031_push_outbox_implies_full_api() {
    for role in ALL_ROLES {
        let assembly = NodeAssembly::for_role(role);
        assert!(
            !assembly.push_outbox || assembly.full_api,
            "{role:?} runs the push outbox without the full API"
        );
    }
}

#[test]
fn coverage_role_032_continuity_gateway_implies_gateway_grpc_surface() {
    for role in ALL_ROLES {
        let assembly = NodeAssembly::for_role(role);
        assert_eq!(
            assembly.continuity_gateway,
            assembly.grpc == GrpcSurface::ContinuityGateway,
            "{role:?} mixes the continuity router with a foreign gRPC surface"
        );
    }
}

#[test]
fn coverage_role_033_disabled_grpc_implies_no_api_surfaces() {
    for role in ALL_ROLES {
        let assembly = NodeAssembly::for_role(role);
        if assembly.grpc == GrpcSurface::Disabled {
            assert!(!assembly.full_api, "{role:?} exposes API without gRPC");
            assert!(
                !assembly.continuity_gateway,
                "{role:?} routes continuity without gRPC"
            );
            assert!(
                !assembly.public_webhooks,
                "{role:?} exposes webhooks without gRPC"
            );
        }
    }
}

#[test]
fn coverage_role_034_exactly_engine_roles_run_the_scheduler() {
    let engine_roles: Vec<NodeRole> = ALL_ROLES
        .into_iter()
        .filter(|role| NodeAssembly::for_role(*role).engine)
        .collect();
    assert_eq!(
        engine_roles,
        vec![NodeRole::AllInOne, NodeRole::Executor, NodeRole::Edge]
    );
}

#[test]
fn coverage_role_035_exactly_api_roles_serve_the_full_api() {
    let api_roles: Vec<NodeRole> = ALL_ROLES
        .into_iter()
        .filter(|role| NodeAssembly::for_role(*role).full_api)
        .collect();
    assert_eq!(api_roles, vec![NodeRole::AllInOne, NodeRole::Control]);
}

#[test]
fn coverage_role_036_public_webhooks_track_the_full_api() {
    for role in ALL_ROLES {
        let assembly = NodeAssembly::for_role(role);
        assert_eq!(
            assembly.public_webhooks, assembly.full_api,
            "{role:?} decouples public webhooks from the authenticated API"
        );
    }
}

#[test]
fn coverage_role_037_every_role_exposes_at_least_one_capability() {
    for role in ALL_ROLES {
        let assembly = NodeAssembly::for_role(role);
        assert!(
            assembly.full_api
                || assembly.continuity_gateway
                || assembly.public_webhooks
                || assembly.engine
                || assembly.push_outbox
                || assembly.grpc != GrpcSurface::Disabled,
            "{role:?} assembles into a dead node"
        );
    }
}

#[test]
fn coverage_role_038_control_differs_from_all_in_one_only_in_engine() {
    let all_in_one = NodeAssembly::for_role(NodeRole::AllInOne);
    let control = NodeAssembly::for_role(NodeRole::Control);
    assert!(all_in_one.engine && !control.engine);
    assert_eq!(all_in_one.full_api, control.full_api);
    assert_eq!(all_in_one.continuity_gateway, control.continuity_gateway);
    assert_eq!(all_in_one.public_webhooks, control.public_webhooks);
    assert_eq!(all_in_one.push_outbox, control.push_outbox);
    assert_eq!(all_in_one.grpc, control.grpc);
}

#[test]
fn coverage_role_039_executor_and_edge_differ_only_in_grpc_surface() {
    let executor = NodeAssembly::for_role(NodeRole::Executor);
    let edge = NodeAssembly::for_role(NodeRole::Edge);
    assert_eq!(executor.full_api, edge.full_api);
    assert_eq!(executor.continuity_gateway, edge.continuity_gateway);
    assert_eq!(executor.public_webhooks, edge.public_webhooks);
    assert_eq!(executor.engine, edge.engine);
    assert_eq!(executor.push_outbox, edge.push_outbox);
    assert_ne!(executor.grpc, edge.grpc);
}

// --- gRPC surface allow-lists ---

#[test]
fn coverage_role_040_executor_allow_list_is_bounded_and_unique() {
    assert_eq!(EXECUTOR_GRPC_RPCS.len(), 8);
    let mut sorted = EXECUTOR_GRPC_RPCS.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        EXECUTOR_GRPC_RPCS.len(),
        "duplicate RPC paths"
    );
}

#[test]
fn coverage_role_041_all_executor_rpcs_are_orch8_service_paths() {
    for path in EXECUTOR_GRPC_RPCS {
        assert!(
            path.starts_with("/orch8.Orch8Service/"),
            "foreign RPC path {path}"
        );
    }
}

#[test]
fn coverage_role_042_executor_allow_list_contains_task_lifecycle_rpcs() {
    for required in [
        "/orch8.Orch8Service/Health",
        "/orch8.Orch8Service/PollTasks",
        "/orch8.Orch8Service/CompleteTask",
        "/orch8.Orch8Service/FailTask",
        "/orch8.Orch8Service/HeartbeatTask",
        "/orch8.Orch8Service/WorkerStream",
        "/orch8.Orch8Service/ArtifactTransfer",
        "/orch8.Orch8Service/IngestTelemetryBatch",
    ] {
        assert!(EXECUTOR_GRPC_RPCS.contains(&required), "missing {required}");
    }
}

#[test]
fn coverage_role_043_gateway_allow_list_is_minimal() {
    assert_eq!(
        GATEWAY_GRPC_RPCS,
        &[
            "/orch8.Orch8Service/Health",
            "/orch8.Orch8Service/ArtifactTransfer"
        ]
    );
}

/// The gateway surface must never expose task-delivery or worker-placement
/// RPCs: it only proxies continuity artifacts and answers health probes.
#[test]
fn coverage_role_044_gateway_allow_list_excludes_task_delivery_rpcs() {
    for forbidden in [
        "/orch8.Orch8Service/PollTasks",
        "/orch8.Orch8Service/CompleteTask",
        "/orch8.Orch8Service/FailTask",
        "/orch8.Orch8Service/HeartbeatTask",
        "/orch8.Orch8Service/WorkerStream",
        "/orch8.Orch8Service/IngestTelemetryBatch",
    ] {
        assert!(
            !GATEWAY_GRPC_RPCS.contains(&forbidden),
            "gateway must not expose {forbidden}"
        );
    }
}

#[test]
fn coverage_role_045_gateway_allow_list_is_a_subset_of_executor() {
    for path in GATEWAY_GRPC_RPCS {
        assert!(
            EXECUTOR_GRPC_RPCS.contains(path),
            "gateway RPC {path} is not on the executor allow-list"
        );
    }
}

#[test]
fn coverage_role_046_grpc_surface_variants_are_distinct() {
    let surfaces = [
        GrpcSurface::Full,
        GrpcSurface::Executor,
        GrpcSurface::ContinuityGateway,
        GrpcSurface::Disabled,
    ];
    for (index, left) in surfaces.iter().enumerate() {
        for right in &surfaces[index + 1..] {
            assert_ne!(left, right);
        }
    }
}

/// The executor surface is bounded to task-lifecycle RPCs: tenant-facing
/// control-plane RPCs (sequences, instances, signals, crons, pools) must
/// never be reachable on it.
#[test]
fn coverage_role_047_executor_allow_list_excludes_control_plane_rpcs() {
    for forbidden in [
        "/orch8.Orch8Service/CreateSequence",
        "/orch8.Orch8Service/CreateInstance",
        "/orch8.Orch8Service/UpdateState",
        "/orch8.Orch8Service/SendSignal",
        "/orch8.Orch8Service/CreateCron",
        "/orch8.Orch8Service/CreatePool",
        "/orch8.Orch8Service/DeleteResource",
    ] {
        assert!(
            !EXECUTOR_GRPC_RPCS.contains(&forbidden),
            "executor must not expose {forbidden}"
        );
        assert!(
            !GATEWAY_GRPC_RPCS.contains(&forbidden),
            "gateway must not expose {forbidden}"
        );
    }
}
