use std::collections::BTreeMap;

use tracing::debug;

use orch8_storage::StorageBackend;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::ParallelDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Execute a parallel block: all branches run concurrently, but within each
/// branch, blocks execute sequentially.
///
/// ## Per-branch cursor semantics
///
/// A branch is an ordered list of blocks. At most one block per branch is
/// active at a time — just like a top-level sequence. Multiple branches run
/// concurrently with each other.
///
/// Each direct child of the parallel node is tagged with `branch_index`
/// (set by [`evaluator::build_branch_nodes`]). On every tick this handler:
///   1. Groups children by `branch_index`, preserving source order within
///      each group.
///   2. For each branch, finds the first non-terminal node (the branch's
///      "cursor"). If that node is Pending, it is activated (transitioned
///      to Running) so the scheduler can dispatch it. If Running/Waiting,
///      the branch is in progress and nothing is done.
///   3. Once every branch has drained (all nodes terminal), the parallel
///      node itself transitions: Completed if every branch completed,
///      Failed if any branch had a failed node.
///
/// Prior behaviour (pre-fix) flattened all branches and activated every
/// pending child at once, producing Temporal-style "fork all" semantics
/// rather than "N concurrent sequences". That broke nested composites,
/// per-branch cancellation, and concurrency guards.
pub async fn execute_parallel(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    par_def: &ParallelDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let children = evaluator::children_of(tree, node.id, None);

    if children.is_empty() {
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // Group by branch_index. BTreeMap keeps branches in declaration order,
    // which makes log output deterministic. Within each group, children
    // are preserved in the insertion order from `children_of` — which
    // mirrors source order because `build_branch_nodes` appends in
    // block-sequence order.
    let mut branches: BTreeMap<i16, Vec<&ExecutionNode>> = BTreeMap::new();
    for c in &children {
        // Direct children of a parallel should always be tagged by
        // build_branch_nodes. Any stray untagged child is treated as
        // branch 0 defensively.
        let idx = c.branch_index.unwrap_or(0);
        branches.entry(idx).or_default().push(*c);
    }

    let mut all_branches_done = true;
    let mut any_branch_failed = false;

    for (branch_idx, branch_nodes) in &branches {
        // Cursor = first non-terminal node in this branch.
        let cursor = branch_nodes.iter().find(|n| {
            !matches!(
                n.state,
                NodeState::Completed
                    | NodeState::Failed
                    | NodeState::Cancelled
                    | NodeState::Skipped
            )
        });

        match cursor {
            Some(n) if n.state == NodeState::Pending => {
                // Activate the branch's next step. Subsequent steps in
                // this branch stay Pending until this one is terminal.
                storage.update_node_state(n.id, NodeState::Running).await?;
                all_branches_done = false;
                debug!(
                    instance_id = %instance.id,
                    block_id = %par_def.id,
                    branch = branch_idx,
                    cursor_block = %n.block_id,
                    "parallel: activating branch cursor"
                );
            }
            Some(_) => {
                // Cursor is Running/Waiting — branch is making progress.
                all_branches_done = false;
            }
            None => {
                // Every node in this branch is terminal.
                if branch_nodes
                    .iter()
                    .any(|n| matches!(n.state, NodeState::Failed))
                {
                    any_branch_failed = true;
                }
            }
        }
    }

    if all_branches_done {
        if any_branch_failed {
            evaluator::fail_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block failed — one or more branches failed"
            );
        } else {
            evaluator::complete_node(storage, node.id).await?;
            debug!(
                instance_id = %instance.id,
                block_id = %par_def.id,
                "parallel block completed — all branches succeeded"
            );
        }
    }

    Ok(true)
}
