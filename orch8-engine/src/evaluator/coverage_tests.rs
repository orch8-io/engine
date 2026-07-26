//! Coverage tests for the evaluator's parallel-dispatch hot path.
//!
//! Pins [`find_parallel_step_pair`] branch-pairing semantics (the function
//! whose HashMap was replaced with a flat Vec on the allocation hot path),
//! plus [`nearest_parallel_branch`] and the node index helpers.
//!
//! Count contract: 14 independently named unit tests.

use orch8_types::sequence::{ParallelDef, StepDef};

use super::*;

fn mk_step(id: &str) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId::new(id),
        handler: "h".into(),
        params: serde_json::Value::Null,
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

fn mk_node(
    id: ExecutionNodeId,
    parent: Option<ExecutionNodeId>,
    block_id: &str,
    block_type: BlockType,
    state: NodeState,
    branch_index: Option<i16>,
) -> ExecutionNode {
    ExecutionNode {
        id,
        instance_id: InstanceId::new(),
        parent_id: parent,
        block_id: BlockId::new(block_id),
        block_type,
        branch_index,
        state,
        started_at: None,
        completed_at: None,
    }
}

fn parallel_block(id: &str, branches: Vec<Vec<BlockDefinition>>) -> BlockDefinition {
    BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new(id),
        branches,
    }))
}

fn registry() -> HandlerRegistry {
    let mut handlers = HandlerRegistry::new();
    handlers.register("h", |_ctx| async { Ok(serde_json::Value::Null) });
    handlers
}

/// Tree shape: one running Parallel root with two running step children in
/// branches 0 and 1.
fn two_branch_fixture() -> (Vec<BlockDefinition>, Vec<ExecutionNode>) {
    let par_id = ExecutionNodeId::new();
    let left_id = ExecutionNodeId::new();
    let right_id = ExecutionNodeId::new();
    let blocks = vec![parallel_block(
        "p",
        vec![vec![mk_step("a")], vec![mk_step("b")]],
    )];
    let tree = vec![
        mk_node(
            par_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            left_id,
            Some(par_id),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            right_id,
            Some(par_id),
            "b",
            BlockType::Step,
            NodeState::Running,
            Some(1),
        ),
    ];
    (blocks, tree)
}

#[test]
fn coverage_evaluator_001_pair_found_across_distinct_branches() {
    let (blocks, tree) = two_branch_fixture();
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    let pair = find_parallel_step_pair(&tree, &block_map, &handlers, &index).unwrap();
    assert_eq!(pair, [1, 2]);
}

#[test]
fn coverage_evaluator_002_same_branch_steps_are_never_paired() {
    let par_id = ExecutionNodeId::new();
    let blocks = vec![parallel_block("p", vec![vec![mk_step("a"), mk_step("b")]])];
    let tree = vec![
        mk_node(
            par_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "b",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
    ];
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_003_pending_steps_are_not_paired() {
    let (blocks, mut tree) = two_branch_fixture();
    tree[2].state = NodeState::Pending;
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_004_unregistered_handler_is_not_paired() {
    let (blocks, tree) = two_branch_fixture();
    let handlers = HandlerRegistry::new();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_005_steps_without_parallel_ancestor_are_not_paired() {
    let blocks = vec![mk_step("a"), mk_step("b")];
    let tree = vec![
        mk_node(
            ExecutionNodeId::new(),
            None,
            "a",
            BlockType::Step,
            NodeState::Running,
            None,
        ),
        mk_node(
            ExecutionNodeId::new(),
            None,
            "b",
            BlockType::Step,
            NodeState::Running,
            None,
        ),
    ];
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_006_distinct_parallels_are_not_cross_paired() {
    let par_a = ExecutionNodeId::new();
    let par_b = ExecutionNodeId::new();
    let blocks = vec![
        parallel_block("pa", vec![vec![mk_step("a")]]),
        parallel_block("pb", vec![vec![mk_step("b")]]),
    ];
    let tree = vec![
        mk_node(
            par_a,
            None,
            "pa",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_a),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            par_b,
            None,
            "pb",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_b),
            "b",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
    ];
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_007_empty_tree_yields_no_pair() {
    let blocks = vec![mk_step("a")];
    let tree: Vec<ExecutionNode> = Vec::new();
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(find_parallel_step_pair(&tree, &block_map, &handlers, &index).is_none());
}

#[test]
fn coverage_evaluator_008_three_branches_pair_the_first_two_seen() {
    let par_id = ExecutionNodeId::new();
    let blocks = vec![parallel_block(
        "p",
        vec![vec![mk_step("a")], vec![mk_step("b")], vec![mk_step("c")]],
    )];
    let tree = vec![
        mk_node(
            par_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "b",
            BlockType::Step,
            NodeState::Running,
            Some(1),
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "c",
            BlockType::Step,
            NodeState::Running,
            Some(2),
        ),
    ];
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert_eq!(
        find_parallel_step_pair(&tree, &block_map, &handlers, &index),
        Some([1, 2])
    );
}

#[test]
fn coverage_evaluator_009_nearest_parallel_branch_reports_parent_and_index() {
    let (blocks, tree) = two_branch_fixture();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    let (parallel_id, branch) = nearest_parallel_branch(&tree[2], &block_map, &index).unwrap();
    assert_eq!(parallel_id, tree[0].id);
    assert_eq!(branch, 1);
}

#[test]
fn coverage_evaluator_010_nearest_parallel_branch_walks_through_composites() {
    // step -> try/catch (transparent) -> parallel: the walk must pass through
    // the intermediate composite and still find the parallel ancestor.
    use orch8_types::sequence::TryCatchDef;
    let par_id = ExecutionNodeId::new();
    let tc_id = ExecutionNodeId::new();
    let step_id = ExecutionNodeId::new();
    let blocks = vec![parallel_block(
        "p",
        vec![vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: BlockId::new("tc"),
            try_block: vec![mk_step("a")],
            catch_block: vec![mk_step("c")],
            finally_block: None,
        }))]],
    )];
    let tree = vec![
        mk_node(
            par_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            tc_id,
            Some(par_id),
            "tc",
            BlockType::TryCatch,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            step_id,
            Some(tc_id),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
    ];
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    let (parallel_id, branch) = nearest_parallel_branch(&tree[2], &block_map, &index).unwrap();
    assert_eq!(parallel_id, par_id);
    assert_eq!(branch, 0);
}

#[test]
fn coverage_evaluator_011_nearest_parallel_branch_returns_none_at_root() {
    let (blocks, tree) = two_branch_fixture();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert!(nearest_parallel_branch(&tree[0], &block_map, &index).is_none());
}

#[test]
fn coverage_evaluator_012_step_inside_parallel_dispatches_with_transparent_nesting() {
    // Nested step under try/catch in one branch pairs with a step in the
    // other branch (nearest-parallel rule through transparent composites).
    use orch8_types::sequence::TryCatchDef;
    let par_id = ExecutionNodeId::new();
    let blocks = vec![parallel_block(
        "p",
        vec![
            vec![BlockDefinition::TryCatch(Box::new(TryCatchDef {
                id: BlockId::new("tc"),
                try_block: vec![mk_step("a")],
                catch_block: vec![mk_step("c")],
                finally_block: None,
            }))],
            vec![mk_step("b")],
        ],
    )];
    let tc_id = ExecutionNodeId::new();
    let tree = vec![
        mk_node(
            par_id,
            None,
            "p",
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            tc_id,
            Some(par_id),
            "tc",
            BlockType::TryCatch,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(tc_id),
            "a",
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            ExecutionNodeId::new(),
            Some(par_id),
            "b",
            BlockType::Step,
            NodeState::Running,
            Some(1),
        ),
    ];
    let handlers = registry();
    let block_map = flatten_blocks(&blocks);
    let index = build_node_index(&tree);
    assert_eq!(
        find_parallel_step_pair(&tree, &block_map, &handlers, &index),
        Some([2, 3])
    );
}

#[test]
fn coverage_evaluator_013_node_index_finds_every_node_by_id() {
    let (_, tree) = two_branch_fixture();
    let index = build_node_index(&tree);
    for node in &tree {
        let found = get_node(&index, node.id).unwrap();
        assert!(std::ptr::eq(found, node));
    }
}

#[test]
fn coverage_evaluator_014_node_index_misses_unknown_id() {
    let (_, tree) = two_branch_fixture();
    let index = build_node_index(&tree);
    assert!(get_node(&index, ExecutionNodeId::new()).is_none());
}
