//! Coverage tests for semantics-safe workflow plan compilation.
//!
//! Pins the block-kind mapping, edge roles, constant interning, top-level
//! execution-mode flags, and the equivalence proof produced by [`optimize`].
//!
//! Count contract: 34 independently named unit tests.

use chrono::Utc;
use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
use orch8_types::sequence::{
    ABSplitDef, ABVariant, BlockDefinition, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef,
    RaceDef, Route, RouterDef, SagaDef, SagaStep, SequenceDefinition, SequenceStatus, StepDef,
    SubSequenceDef, TryCatchDef,
};
use serde_json::{Value, json};

use super::*;

fn step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: "noop".into(),
        params: json!({}),
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
    }))
}

fn sequence_with(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked("tenant"),
        namespace: Namespace::new("default"),
        name: "plan".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks,
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
    }
}

macro_rules! kind_case {
    ($name:ident, $block:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let ir = optimize(&sequence_with(vec![$block])).unwrap();
            assert_eq!(ir.nodes[0].kind, $expected);
            assert_eq!(ir.roots, vec![0]);
        }
    };
}

kind_case!(
    coverage_plan_compile_001_step_maps_to_step_kind,
    step("s"),
    OptimizedBlockKind::Step
);
kind_case!(
    coverage_plan_compile_002_parallel_maps_to_parallel_kind,
    BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("p"),
        branches: vec![vec![step("a")], vec![step("b")]],
    })),
    OptimizedBlockKind::Parallel
);
kind_case!(
    coverage_plan_compile_003_race_maps_to_race_kind,
    BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("r"),
        branches: vec![vec![step("a")]],
        semantics: Default::default(),
    })),
    OptimizedBlockKind::Race
);
kind_case!(
    coverage_plan_compile_004_loop_maps_to_loop_kind,
    BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("l"),
        condition: "true".into(),
        body: vec![step("a")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    })),
    OptimizedBlockKind::Loop
);
kind_case!(
    coverage_plan_compile_005_for_each_maps_to_for_each_kind,
    BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("f"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![step("a")],
        max_iterations: 3,
        retain_iterations: None,
    })),
    OptimizedBlockKind::ForEach
);
kind_case!(
    coverage_plan_compile_006_router_maps_to_router_kind,
    BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![step("a")],
        }],
        default: None,
    })),
    OptimizedBlockKind::Router
);
kind_case!(
    coverage_plan_compile_007_try_catch_maps_to_try_catch_kind,
    BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("t"),
        try_block: vec![step("a")],
        catch_block: vec![step("b")],
        finally_block: None,
    })),
    OptimizedBlockKind::TryCatch
);
kind_case!(
    coverage_plan_compile_008_sub_sequence_maps_to_sub_sequence_kind,
    BlockDefinition::SubSequence(Box::new(SubSequenceDef {
        id: BlockId::new("sub"),
        sequence_name: "child".into(),
        version: None,
        input: json!({}),
    })),
    OptimizedBlockKind::SubSequence
);
kind_case!(
    coverage_plan_compile_009_ab_split_maps_to_ab_split_kind,
    BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![step("a")],
            },
            ABVariant {
                name: "treatment".into(),
                weight: 50,
                blocks: vec![step("b")],
            },
        ],
    })),
    OptimizedBlockKind::AbSplit
);
kind_case!(
    coverage_plan_compile_010_cancellation_scope_maps_to_cancellation_scope_kind,
    BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![step("a")],
    })),
    OptimizedBlockKind::CancellationScope
);
kind_case!(
    coverage_plan_compile_011_saga_maps_to_saga_kind,
    BlockDefinition::Saga(Box::new(SagaDef {
        id: BlockId::new("sg"),
        steps: vec![SagaStep {
            id: BlockId::new("sg1"),
            action: Box::new(step("a")),
            compensation: None,
        }],
    })),
    OptimizedBlockKind::Saga
);

#[test]
fn coverage_plan_compile_012_parallel_edges_carry_branch_roles() {
    let block = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("p"),
        branches: vec![vec![step("a")], vec![step("b")]],
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    assert_eq!(ir.nodes.len(), 3);
    let edges = &ir.nodes[0].edges;
    assert_eq!(edges.len(), 2);
    assert_eq!(edges[0].role, "branch:0:0");
    assert_eq!(edges[1].role, "branch:1:0");
    assert_eq!(ir.nodes[edges[0].child].id.as_str(), "a");
    assert_eq!(ir.nodes[edges[1].child].id.as_str(), "b");
}

#[test]
fn coverage_plan_compile_013_router_edges_cover_routes_and_default() {
    let block = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![
            Route {
                condition: "a".into(),
                blocks: vec![step("ra")],
            },
            Route {
                condition: "b".into(),
                blocks: vec![step("rb")],
            },
        ],
        default: Some(vec![step("rd")]),
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["route:0:0", "route:1:0", "default:0"]);
}

#[test]
fn coverage_plan_compile_014_router_without_default_has_no_default_edge() {
    let block = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![Route {
            condition: "a".into(),
            blocks: vec![step("ra")],
        }],
        default: None,
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["route:0:0"]);
}

#[test]
fn coverage_plan_compile_015_try_catch_finally_edges_are_ordered() {
    let block = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("t"),
        try_block: vec![step("t1")],
        catch_block: vec![step("c1")],
        finally_block: Some(vec![step("f1")]),
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["try:0", "catch:0", "finally:0"]);
}

#[test]
fn coverage_plan_compile_016_saga_edges_pair_actions_and_compensations() {
    let block = BlockDefinition::Saga(Box::new(SagaDef {
        id: BlockId::new("sg"),
        steps: vec![
            SagaStep {
                id: BlockId::new("sg1"),
                action: Box::new(step("a1")),
                compensation: Some(Box::new(step("c1"))),
            },
            SagaStep {
                id: BlockId::new("sg2"),
                action: Box::new(step("a2")),
                compensation: None,
            },
        ],
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["action:0", "compensation:0", "action:1"]);
}

#[test]
fn coverage_plan_compile_017_loop_body_edge_uses_body_role() {
    let block = BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("l"),
        condition: "true".into(),
        body: vec![step("b1"), step("b2")],
        max_iterations: 3,
        break_on: None,
        continue_on_error: false,
        poll_interval: None,
        retain_iterations: None,
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["body:0", "body:1"]);
}

#[test]
fn coverage_plan_compile_018_cancellation_scope_edge_uses_scope_role() {
    let block = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
        id: BlockId::new("cs"),
        blocks: vec![step("s1")],
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    assert_eq!(ir.nodes[0].edges[0].role, "scope:0");
}

#[test]
fn coverage_plan_compile_019_ab_split_edges_use_variant_roles() {
    let block = BlockDefinition::ABSplit(Box::new(ABSplitDef {
        id: BlockId::new("ab"),
        variants: vec![
            ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![step("va")],
            },
            ABVariant {
                name: "treatment".into(),
                weight: 50,
                blocks: vec![step("vb")],
            },
        ],
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    let roles: Vec<&str> = ir.nodes[0].edges.iter().map(|e| e.role.as_str()).collect();
    assert_eq!(roles, vec!["variant:0:0", "variant:1:0"]);
}

#[test]
fn coverage_plan_compile_020_composite_blocks_have_no_handler() {
    let block = BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("p"),
        branches: vec![vec![step("a")]],
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    assert_eq!(ir.nodes[0].handler, None);
    assert_eq!(ir.nodes[1].handler.as_deref(), Some("noop"));
}

#[test]
fn coverage_plan_compile_021_loop_guard_follows_condition_literal() {
    let make = |condition: &str| {
        BlockDefinition::Loop(Box::new(LoopDef {
            id: BlockId::new("l"),
            condition: condition.into(),
            body: vec![step("a")],
            max_iterations: 3,
            break_on: None,
            continue_on_error: false,
            poll_interval: None,
            retain_iterations: None,
        }))
    };
    let always = optimize(&sequence_with(vec![make("true")])).unwrap();
    assert_eq!(always.nodes[0].guard, GuardPlan::Always);
    let never = optimize(&sequence_with(vec![make("false")])).unwrap();
    assert_eq!(never.nodes[0].guard, GuardPlan::Never);
    let dynamic = optimize(&sequence_with(vec![make("data.keep_going")])).unwrap();
    assert_eq!(dynamic.nodes[0].guard, GuardPlan::Dynamic);
}

#[test]
fn coverage_plan_compile_022_for_each_and_router_guards_are_always_dynamic() {
    let for_each = BlockDefinition::ForEach(Box::new(ForEachDef {
        id: BlockId::new("f"),
        collection: "items".into(),
        item_var: "item".into(),
        body: vec![step("a")],
        max_iterations: 3,
        retain_iterations: None,
    }));
    let router = BlockDefinition::Router(Box::new(RouterDef {
        id: BlockId::new("r"),
        routes: vec![Route {
            condition: "true".into(),
            blocks: vec![step("b")],
        }],
        default: None,
    }));
    let ir = optimize(&sequence_with(vec![for_each, router])).unwrap();
    assert_eq!(ir.nodes[0].guard, GuardPlan::Dynamic);
    assert_eq!(ir.nodes[2].guard, GuardPlan::Dynamic);
}

#[test]
fn coverage_plan_compile_023_output_schema_is_interned_as_second_constant() {
    let mut seq = sequence_with(vec![step("s")]);
    let BlockDefinition::Step(step_def) = &mut seq.blocks[0] else {
        unreachable!();
    };
    step_def.output_schema = Some(json!({"type": "object"}));
    let ir = optimize(&seq).unwrap();
    assert_eq!(ir.nodes[0].constants.len(), 2);
    assert_eq!(
        ir.constant_pool[ir.nodes[0].constants[1]],
        json!({"type": "object"})
    );
}

#[test]
fn coverage_plan_compile_024_identical_params_share_one_pool_entry() {
    let mut seq = sequence_with(vec![step("a"), step("b")]);
    for block in &mut seq.blocks {
        let BlockDefinition::Step(step_def) = block else {
            unreachable!();
        };
        step_def.params = json!({"shared": true});
    }
    let ir = optimize(&seq).unwrap();
    assert_eq!(ir.constant_pool.len(), 1);
    assert_eq!(ir.nodes[0].constants, ir.nodes[1].constants);
}

#[test]
fn coverage_plan_compile_025_distinct_params_occupy_distinct_pool_entries() {
    let mut seq = sequence_with(vec![step("a"), step("b")]);
    let BlockDefinition::Step(step_def) = &mut seq.blocks[1] else {
        unreachable!();
    };
    step_def.params = json!({"different": 1});
    let ir = optimize(&seq).unwrap();
    assert_eq!(ir.constant_pool.len(), 2);
    assert_ne!(ir.nodes[0].constants, ir.nodes[1].constants);
}

#[test]
fn coverage_plan_compile_026_sub_sequence_inputs_are_interned() {
    let sub = |id: &str, input: Value| {
        BlockDefinition::SubSequence(Box::new(SubSequenceDef {
            id: BlockId::new(id),
            sequence_name: "child".into(),
            version: None,
            input,
        }))
    };
    let ir = optimize(&sequence_with(vec![
        sub("s1", json!({"x": 1})),
        sub("s2", json!({"x": 1})),
        sub("s3", json!({"x": 2})),
    ]))
    .unwrap();
    assert_eq!(ir.constant_pool.len(), 2);
    assert_eq!(ir.nodes[0].constants, ir.nodes[1].constants);
    assert_ne!(ir.nodes[0].constants, ir.nodes[2].constants);
}

#[test]
fn coverage_plan_compile_027_roots_track_top_level_order() {
    let ir = optimize(&sequence_with(vec![step("a"), step("b"), step("c")])).unwrap();
    assert_eq!(ir.roots, vec![0, 1, 2]);
    assert_eq!(ir.nodes[0].id.as_str(), "a");
    assert_eq!(ir.nodes[2].id.as_str(), "c");
}

#[test]
fn coverage_plan_compile_028_top_level_composite_flag_detects_non_steps() {
    let flat = optimize(&sequence_with(vec![step("a"), step("b")])).unwrap();
    assert!(!flat.top_level_has_composite);

    let composite = optimize(&sequence_with(vec![
        step("a"),
        BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
            id: BlockId::new("cs"),
            blocks: vec![step("b")],
        })),
    ]))
    .unwrap();
    assert!(composite.top_level_has_composite);
}

#[test]
fn coverage_plan_compile_029_plugin_handler_flag_detects_each_prefix() {
    for handler in [
        "ap://gmail.send",
        "grpc://plugin",
        "grpcs://plugin",
        "wasm://plugin",
    ] {
        let mut seq = sequence_with(vec![step("s")]);
        let BlockDefinition::Step(step_def) = &mut seq.blocks[0] else {
            unreachable!();
        };
        step_def.handler = handler.into();
        let ir = optimize(&seq).unwrap();
        assert!(ir.top_level_has_plugin_handler, "{handler}");
    }
}

#[test]
fn coverage_plan_compile_030_builtin_handler_is_not_a_plugin() {
    let ir = optimize(&sequence_with(vec![step("s")])).unwrap();
    assert!(!ir.top_level_has_plugin_handler);
}

#[test]
fn coverage_plan_compile_031_verify_equivalent_reports_invalid_workflow_variant() {
    let seq = sequence_with(vec![step("a")]);
    let ir = optimize(&seq).unwrap();
    let mut changed = seq.clone();
    changed.name = "renamed".into();
    let error = ir.verify_equivalent(&changed).unwrap_err();
    assert_eq!(
        error,
        OptimizationError::InvalidWorkflow(
            "optimized plan source hash does not match workflow definition".into()
        )
    );
    assert!(error.to_string().contains("source hash does not match"));
}

#[test]
fn coverage_plan_compile_032_empty_workflow_is_rejected_as_invalid() {
    let error = optimize(&sequence_with(Vec::new())).unwrap_err();
    assert!(matches!(error, OptimizationError::InvalidWorkflow(_)));
    assert!(error.to_string().starts_with("workflow is invalid:"));
}

#[test]
fn coverage_plan_compile_033_error_display_strings_are_stable() {
    assert_eq!(
        OptimizationError::NodeLimit.to_string(),
        "workflow exceeds the 10000-node optimization limit"
    );
    assert_eq!(
        OptimizationError::Serialization("bad".into()).to_string(),
        "workflow cannot be canonicalized: bad"
    );
}

#[test]
fn coverage_plan_compile_034_ir_round_trips_through_serde() {
    let block = BlockDefinition::TryCatch(Box::new(TryCatchDef {
        id: BlockId::new("t"),
        try_block: vec![step("t1")],
        catch_block: vec![step("c1")],
        finally_block: Some(vec![step("f1")]),
    }));
    let ir = optimize(&sequence_with(vec![block])).unwrap();
    assert_eq!(ir.optimizer_version, OPTIMIZER_VERSION);
    let value = serde_json::to_value(&ir).unwrap();
    let restored: OptimizationIr = serde_json::from_value(value).unwrap();
    assert_eq!(restored.source_sha256, ir.source_sha256);
    assert_eq!(restored.nodes, ir.nodes);
    assert_eq!(restored.constant_pool, ir.constant_pool);
    assert_eq!(restored.roots, ir.roots);
    assert_eq!(restored.top_level_has_composite, ir.top_level_has_composite);
    assert_eq!(
        restored.top_level_has_plugin_handler,
        ir.top_level_has_plugin_handler
    );
}
