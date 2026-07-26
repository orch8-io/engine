//! Coverage tests for the cancellation hot path helpers.
//!
//! Pins [`is_inside_finally_branch`] (finally-branch protection over the
//! sorted-vec children index that replaced a HashMap) and
//! [`is_descendant_of_any`] ancestry checks used by `cancel_scoped`.
//!
//! Count contract: 14 independently named unit tests.

use orch8_types::execution::{BlockType, ExecutionNode};
use orch8_types::ids::{BlockId, ExecutionNodeId};

use super::*;

fn mk_node(
    id: ExecutionNodeId,
    parent: Option<ExecutionNodeId>,
    block_type: BlockType,
    state: NodeState,
    branch_index: Option<i16>,
) -> ExecutionNode {
    ExecutionNode {
        id,
        instance_id: InstanceId::new(),
        parent_id: parent,
        block_id: BlockId::new("b"),
        block_type,
        branch_index,
        state,
        started_at: None,
        completed_at: None,
    }
}

/// Build the sorted node index and sorted children-of vec exactly the way
/// `cancel_scoped` does, so the partition_point lookup contract is exercised.
fn indexes(
    tree: &[ExecutionNode],
) -> (Vec<&ExecutionNode>, Vec<(ExecutionNodeId, &ExecutionNode)>) {
    let mut node_index: Vec<&ExecutionNode> = tree.iter().collect();
    node_index.sort_unstable_by_key(|n| n.id);
    let mut children_of: Vec<(ExecutionNodeId, &ExecutionNode)> = Vec::with_capacity(tree.len());
    for n in tree {
        if let Some(parent_id) = n.parent_id {
            children_of.push((parent_id, n));
        }
    }
    children_of.sort_unstable_by_key(|&(p, _)| p);
    (node_index, children_of)
}

/// Tree: try/catch root with a try child (branch 0) and a finally child
/// (branch 2) whose states are configurable.
fn try_catch_tree(finally_state: NodeState) -> (Vec<ExecutionNode>, [ExecutionNodeId; 3]) {
    let tc = ExecutionNodeId::new();
    let try_child = ExecutionNodeId::new();
    let finally_child = ExecutionNodeId::new();
    let tree = vec![
        mk_node(tc, None, BlockType::TryCatch, NodeState::Running, None),
        mk_node(
            try_child,
            Some(tc),
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(
            finally_child,
            Some(tc),
            BlockType::Step,
            finally_state,
            Some(2),
        ),
    ];
    (tree, [tc, try_child, finally_child])
}

#[test]
fn coverage_signals_001_try_catch_with_running_finally_is_protected() {
    let (tree, _) = try_catch_tree(NodeState::Running);
    let (node_index, children_of) = indexes(&tree);
    assert!(is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
}

#[test]
fn coverage_signals_002_try_catch_with_pending_finally_is_protected() {
    let (tree, _) = try_catch_tree(NodeState::Pending);
    let (node_index, children_of) = indexes(&tree);
    assert!(is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
}

#[test]
fn coverage_signals_003_try_catch_with_waiting_finally_is_protected() {
    let (tree, _) = try_catch_tree(NodeState::Waiting);
    let (node_index, children_of) = indexes(&tree);
    assert!(is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
}

#[test]
fn coverage_signals_004_try_catch_with_completed_finally_is_cancellable() {
    let (tree, _) = try_catch_tree(NodeState::Completed);
    let (node_index, children_of) = indexes(&tree);
    assert!(!is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
}

#[test]
fn coverage_signals_005_try_catch_without_children_is_cancellable() {
    let tc = ExecutionNodeId::new();
    let tree = vec![mk_node(
        tc,
        None,
        BlockType::TryCatch,
        NodeState::Running,
        None,
    )];
    let (node_index, children_of) = indexes(&tree);
    assert!(!is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
}

#[test]
fn coverage_signals_006_try_branch_child_is_not_finally_protected() {
    let (tree, [_, try_child, _]) = try_catch_tree(NodeState::Running);
    let (node_index, children_of) = indexes(&tree);
    let node = tree.iter().find(|n| n.id == try_child).unwrap();
    assert!(!is_inside_finally_branch(&node_index, &children_of, node));
}

#[test]
fn coverage_signals_007_finally_branch_child_is_protected() {
    let (tree, [_, _, finally_child]) = try_catch_tree(NodeState::Completed);
    let (node_index, children_of) = indexes(&tree);
    let node = tree.iter().find(|n| n.id == finally_child).unwrap();
    assert!(is_inside_finally_branch(&node_index, &children_of, node));
}

#[test]
fn coverage_signals_008_grandchild_of_finally_branch_is_protected() {
    let (mut tree, [_, _, finally_child]) = try_catch_tree(NodeState::Completed);
    let grandchild = ExecutionNodeId::new();
    tree.push(mk_node(
        grandchild,
        Some(finally_child),
        BlockType::Step,
        NodeState::Running,
        None,
    ));
    let (node_index, children_of) = indexes(&tree);
    let node = tree.iter().find(|n| n.id == grandchild).unwrap();
    assert!(is_inside_finally_branch(&node_index, &children_of, node));
}

#[test]
fn coverage_signals_009_catch_branch_child_is_not_finally_protected() {
    let tc = ExecutionNodeId::new();
    let catch_child = ExecutionNodeId::new();
    let tree = vec![
        mk_node(tc, None, BlockType::TryCatch, NodeState::Running, None),
        mk_node(
            catch_child,
            Some(tc),
            BlockType::Step,
            NodeState::Running,
            Some(1),
        ),
    ];
    let (node_index, children_of) = indexes(&tree);
    assert!(!is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[1]
    ));
}

#[test]
fn coverage_signals_010_branch_two_under_non_try_catch_is_not_protected() {
    let scope = ExecutionNodeId::new();
    let child = ExecutionNodeId::new();
    let tree = vec![
        mk_node(
            scope,
            None,
            BlockType::CancellationScope,
            NodeState::Running,
            None,
        ),
        mk_node(
            child,
            Some(scope),
            BlockType::Step,
            NodeState::Running,
            Some(2),
        ),
    ];
    let (node_index, children_of) = indexes(&tree);
    assert!(!is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[1]
    ));
}

#[test]
fn coverage_signals_011_descendant_of_scope_is_found() {
    let scope = ExecutionNodeId::new();
    let child = ExecutionNodeId::new();
    let grandchild = ExecutionNodeId::new();
    let tree = vec![
        mk_node(
            scope,
            None,
            BlockType::CancellationScope,
            NodeState::Running,
            None,
        ),
        mk_node(
            child,
            Some(scope),
            BlockType::Step,
            NodeState::Running,
            None,
        ),
        mk_node(
            grandchild,
            Some(child),
            BlockType::Step,
            NodeState::Running,
            None,
        ),
    ];
    let (node_index, _) = indexes(&tree);
    assert!(is_descendant_of_any(&node_index, &tree[2], &[scope]));
    assert!(is_descendant_of_any(&node_index, &tree[1], &[scope]));
}

#[test]
fn coverage_signals_012_root_is_not_descendant_of_anything() {
    let root = ExecutionNodeId::new();
    let tree = vec![mk_node(
        root,
        None,
        BlockType::Step,
        NodeState::Running,
        None,
    )];
    let (node_index, _) = indexes(&tree);
    assert!(!is_descendant_of_any(&node_index, &tree[0], &[root]));
}

#[test]
fn coverage_signals_013_unrelated_node_is_not_a_descendant() {
    let scope = ExecutionNodeId::new();
    let other = ExecutionNodeId::new();
    let tree = vec![
        mk_node(
            scope,
            None,
            BlockType::CancellationScope,
            NodeState::Running,
            None,
        ),
        mk_node(other, None, BlockType::Step, NodeState::Running, None),
    ];
    let (node_index, _) = indexes(&tree);
    assert!(!is_descendant_of_any(&node_index, &tree[1], &[scope]));
}

#[test]
fn coverage_signals_014_sorted_children_index_skips_other_parents() {
    // Multiple parents with children interleaved in creation order: the
    // partition_point lookup must still isolate exactly one parent's row.
    let tc = ExecutionNodeId::new();
    let parallel = ExecutionNodeId::new();
    let a = ExecutionNodeId::new();
    let b = ExecutionNodeId::new();
    let tree = vec![
        mk_node(tc, None, BlockType::TryCatch, NodeState::Running, None),
        mk_node(
            parallel,
            None,
            BlockType::Parallel,
            NodeState::Running,
            None,
        ),
        mk_node(
            a,
            Some(parallel),
            BlockType::Step,
            NodeState::Running,
            Some(0),
        ),
        mk_node(b, Some(tc), BlockType::Step, NodeState::Running, Some(2)),
    ];
    let (node_index, children_of) = indexes(&tree);
    // The try/catch has an active finally child (b) even though another
    // parent's child (a) sorts adjacent to it.
    assert!(is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[0]
    ));
    // The parallel node itself is never finally-protected.
    assert!(!is_inside_finally_branch(
        &node_index,
        &children_of,
        &tree[1]
    ));
}
