use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

mod dispatch;
mod sla;
#[cfg(test)]
mod tests;

use dispatch::dispatch_block;
use sla::check_sla_deadlines;

/// Pre-built index mapping each node to its parent. Built once per evaluate
/// iteration and shared across helper functions to avoid repeated O(n)
/// allocations.
type ParentMap = HashMap<ExecutionNodeId, Option<ExecutionNodeId>>;

fn build_parent_map(tree: &[ExecutionNode]) -> ParentMap {
    tree.iter().map(|n| (n.id, n.parent_id)).collect()
}

use crate::error::EngineError;
use crate::handlers::param_resolve::OutputsSnapshot;
use crate::handlers::HandlerRegistry;

/// Result of a single `evaluate()` call, carrying enough state for the caller
/// to transition the instance without re-reading the tree or instance from DB.
#[derive(Debug)]
pub enum EvalOutcome {
    /// All root nodes are terminal — instance is done.
    Done {
        any_failed: bool,
        any_cancelled: bool,
    },
    /// More work remains — re-schedule or wait.
    MoreWork { has_waiting_nodes: bool },
}

/// In-memory cache for the evaluation loop. Pre-loaded at the start and only
/// refreshed from storage when a write operation invalidates the cached state.
struct EvalContext {
    tree: Vec<ExecutionNode>,
    instance: TaskInstance,
    /// When true, the tree must be re-read from storage before the next use.
    tree_stale: bool,
    /// When true, the instance must be re-read from storage before the next use.
    instance_stale: bool,
}

impl EvalContext {
    /// Refresh the execution tree from storage if stale.
    async fn refresh_tree(
        &mut self,
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
    ) -> Result<(), EngineError> {
        if self.tree_stale {
            self.tree = storage.get_execution_tree(instance_id).await?;
            self.tree_stale = false;
        }
        Ok(())
    }

    /// Refresh the instance from storage if stale.
    async fn refresh_instance(
        &mut self,
        storage: &dyn StorageBackend,
        instance_id: InstanceId,
    ) -> Result<(), EngineError> {
        if self.instance_stale {
            self.instance = storage
                .get_instance(instance_id)
                .await?
                .ok_or_else(|| EngineError::NotFound(format!("instance {instance_id}")))?;
            self.instance_stale = false;
        }
        Ok(())
    }

    /// Replace the cached tree with a pre-fetched snapshot and clear staleness.
    fn set_tree(&mut self, tree: Vec<ExecutionNode>) {
        self.tree = tree;
        self.tree_stale = false;
    }
}

/// Action returned by each evaluation phase to direct the main loop.
enum IterAction {
    /// Phase handled work — restart the loop from the top.
    Continue,
    /// Phase determined a terminal result — return immediately.
    Return(EvalOutcome),
    /// Phase had no actionable work — fall through to the next phase.
    FallThrough,
}

/// Fetch any dynamically injected blocks and merge them with the sequence
/// blocks.
///
/// The common case on the scheduler hot path is "no injection" — the entire
/// block list is the sequence's own. Returning [`Cow::Borrowed`] in that case
/// avoids cloning the `Vec<BlockDefinition>` on every tick. Only when extra
/// blocks have actually been injected do we materialise an owned vector.
pub async fn merged_blocks<'s>(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    sequence: &'s SequenceDefinition,
) -> Result<Cow<'s, [BlockDefinition]>, EngineError> {
    let injected = storage.get_injected_blocks(instance_id).await?;
    match injected {
        Some(val) if val.is_array() => {
            debug!(instance_id = %instance_id, "merged_blocks: found injected blocks");
            let extra: Vec<BlockDefinition> = match serde_json::from_value(val) {
                Ok(v) => v,
                Err(e) => {
                    warn!(instance_id = %instance_id, error = %e, "failed to deserialize injected blocks, ignoring");
                    return Ok(Cow::Borrowed(sequence.blocks.as_slice()));
                }
            };
            if extra.is_empty() {
                return Ok(Cow::Borrowed(sequence.blocks.as_slice()));
            }
            let mut merged = sequence.blocks.clone();
            merged.extend(extra);
            Ok(Cow::Owned(merged))
        }
        _ => Ok(Cow::Borrowed(sequence.blocks.as_slice())),
    }
}

/// Build the execution tree from a sequence definition if it doesn't exist yet.
/// Returns the root-level nodes for the instance.
pub async fn ensure_execution_tree(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    blocks: &[BlockDefinition],
) -> Result<Vec<ExecutionNode>, EngineError> {
    let existing = storage.get_execution_tree(instance.id).await?;
    if !existing.is_empty() {
        // Check for newly injected blocks that don't have execution nodes yet.
        let mut existing_block_ids = std::collections::HashSet::with_capacity(existing.len());
        for n in &existing {
            existing_block_ids.insert(n.block_id.as_str());
        }
        let mut new_nodes = Vec::with_capacity(blocks.len());
        for block in blocks {
            let (bid, _) = block_meta(block);
            if !existing_block_ids.contains(bid.as_str()) {
                build_nodes(
                    instance.id,
                    None,
                    None,
                    std::slice::from_ref(block),
                    &mut new_nodes,
                );
            }
        }
        if !new_nodes.is_empty() {
            storage.create_execution_nodes_batch(&new_nodes).await?;
            debug!(
                instance_id = %instance.id,
                new_count = new_nodes.len(),
                "injected blocks added to execution tree"
            );
            return storage
                .get_execution_tree(instance.id)
                .await
                .map_err(Into::into);
        }
        return Ok(existing);
    }

    // Build tree from blocks.
    let mut nodes = Vec::with_capacity(blocks.len() * 2);
    build_nodes(instance.id, None, None, blocks, &mut nodes);

    if !nodes.is_empty() {
        storage.create_execution_nodes_batch(&nodes).await?;
        debug!(
            instance_id = %instance.id,
            node_count = nodes.len(),
            "execution tree created"
        );
    }

    Ok(nodes)
}

/// Recursively build execution nodes from block definitions.
fn build_nodes(
    instance_id: InstanceId,
    parent_id: Option<ExecutionNodeId>,
    branch_index: Option<usize>,
    blocks: &[BlockDefinition],
    out: &mut Vec<ExecutionNode>,
) {
    let branch_i16 = branch_index.and_then(|i| i16::try_from(i).ok());
    for block in blocks {
        let (block_id, block_type) = block_meta(block);
        let node_id = ExecutionNodeId::new();

        out.push(ExecutionNode {
            id: node_id,
            instance_id,
            block_id: block_id.clone(),
            parent_id,
            block_type,
            branch_index: branch_i16,
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        });

        // Recurse into children.
        match block {
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
            BlockDefinition::Parallel(p) => {
                for (i, branch) in p.branches.iter().enumerate() {
                    build_nodes(instance_id, Some(node_id), Some(i), branch, out);
                }
            }
            BlockDefinition::Race(r) => {
                for (i, branch) in r.branches.iter().enumerate() {
                    build_nodes(instance_id, Some(node_id), Some(i), branch, out);
                }
            }
            BlockDefinition::Loop(l) => {
                build_nodes(instance_id, Some(node_id), None, &l.body, out);
            }
            BlockDefinition::ForEach(f) => {
                build_nodes(instance_id, Some(node_id), None, &f.body, out);
            }
            BlockDefinition::Router(r) => {
                for (i, route) in r.routes.iter().enumerate() {
                    build_nodes(instance_id, Some(node_id), Some(i), &route.blocks, out);
                }
                if let Some(default) = &r.default {
                    build_nodes(
                        instance_id,
                        Some(node_id),
                        Some(r.routes.len()),
                        default,
                        out,
                    );
                }
            }
            BlockDefinition::TryCatch(tc) => {
                build_nodes(instance_id, Some(node_id), Some(0), &tc.try_block, out);
                build_nodes(instance_id, Some(node_id), Some(1), &tc.catch_block, out);
                if let Some(finally) = &tc.finally_block {
                    build_nodes(instance_id, Some(node_id), Some(2), finally, out);
                }
            }
            BlockDefinition::ABSplit(ab) => {
                for (i, variant) in ab.variants.iter().enumerate() {
                    build_nodes(instance_id, Some(node_id), Some(i), &variant.blocks, out);
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                build_nodes(instance_id, Some(node_id), None, &cs.blocks, out);
            }
        }
    }
}

fn block_meta(block: &BlockDefinition) -> (&BlockId, BlockType) {
    match block {
        BlockDefinition::Step(s) => (&s.id, BlockType::Step),
        BlockDefinition::Parallel(p) => (&p.id, BlockType::Parallel),
        BlockDefinition::Race(r) => (&r.id, BlockType::Race),
        BlockDefinition::Loop(l) => (&l.id, BlockType::Loop),
        BlockDefinition::ForEach(f) => (&f.id, BlockType::ForEach),
        BlockDefinition::Router(r) => (&r.id, BlockType::Router),
        BlockDefinition::TryCatch(tc) => (&tc.id, BlockType::TryCatch),
        BlockDefinition::SubSequence(ss) => (&ss.id, BlockType::SubSequence),
        BlockDefinition::ABSplit(ab) => (&ab.id, BlockType::ABSplit),
        BlockDefinition::CancellationScope(cs) => (&cs.id, BlockType::CancellationScope),
    }
}

/// Evaluate the execution tree: dispatch actionable nodes until no more
/// progress can be made within this tick.
///
/// Returns [`EvalOutcome`] describing why evaluation stopped, so the caller
/// can transition the instance without re-reading the tree from DB.
#[allow(clippy::too_many_lines)]
pub async fn evaluate(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<EvalOutcome, EngineError> {
    // Merge sequence blocks with any dynamically injected blocks.
    let blocks = merged_blocks(storage.as_ref(), instance.id, sequence).await?;

    // Build a flat block map once per evaluation — avoids O(n) recursive scans
    // in find_block on the hot path.
    let block_map: HashMap<&BlockId, &BlockDefinition> = flatten_blocks(&blocks);

    // Ensure the execution tree exists (creates on first call, adds new injected nodes).
    let initial_tree = ensure_execution_tree(storage.as_ref(), instance, &blocks).await?;

    // Pre-load instance and tree into in-memory context to reduce DB round-trips.
    // The context tracks staleness: after a storage write that may have mutated
    // tree or instance state, the relevant flag is set so the next iteration
    // re-reads only what changed.
    let initial_instance = storage
        .get_instance(instance.id)
        .await?
        .ok_or_else(|| EngineError::NotFound(format!("instance {}", instance.id)))?;

    let mut ctx = EvalContext {
        tree: initial_tree,
        instance: initial_instance,
        tree_stale: false,
        instance_stale: false,
    };

    let max_iterations = 200;
    let instance_id = instance.id;

    for _ in 0..max_iterations {
        let outputs_snapshot = OutputsSnapshot::new();

        // Refresh stale caches before this iteration.
        ctx.refresh_instance(storage.as_ref(), instance_id).await?;
        ctx.refresh_tree(storage.as_ref(), instance_id).await?;

        // If a dispatch within this tick moved the instance out of Running,
        // stop iterating. The outer scheduler owns the next transition.
        if ctx.instance.state != InstanceState::Running {
            debug!(
                instance_id = %instance_id,
                state = %ctx.instance.state,
                "evaluate: instance no longer Running, exiting loop early"
            );
            return Ok(EvalOutcome::MoreWork {
                has_waiting_nodes: ctx.tree.iter().any(|n| n.state == NodeState::Waiting),
            });
        }

        // SLA deadline check: fail any step nodes that have breached their deadline.
        let deadlines_breached =
            check_sla_deadlines(storage, handlers, &ctx.instance, &blocks, &ctx.tree).await?;
        if deadlines_breached {
            ctx.tree_stale = true;
            ctx.refresh_tree(storage.as_ref(), instance_id).await?;
        }

        // Check termination conditions on root nodes.
        if let Some(outcome) = check_termination(&ctx.tree) {
            return Ok(outcome);
        }

        // Phase 1: activate the first Pending root node.
        match phase_root_activation(
            storage,
            handlers,
            &mut ctx,
            instance_id,
            &block_map,
            sequence,
            &outputs_snapshot,
        )
        .await?
        {
            IterAction::Continue => continue,
            IterAction::Return(outcome) => return Ok(outcome),
            IterAction::FallThrough => {}
        }

        // Phase 2: execute Running step nodes (leaf work first).
        match phase_running_steps(
            storage,
            handlers,
            &mut ctx,
            &block_map,
            sequence,
            &outputs_snapshot,
        )
        .await?
        {
            IterAction::Continue => continue,
            IterAction::Return(outcome) => return Ok(outcome),
            IterAction::FallThrough => {}
        }

        // Phase 3: re-evaluate Running composite nodes.
        match phase_composite_reevaluation(
            storage,
            handlers,
            &mut ctx,
            instance_id,
            &block_map,
            sequence,
            &outputs_snapshot,
        )
        .await?
        {
            IterAction::Continue => continue,
            IterAction::Return(outcome) => return Ok(outcome),
            IterAction::FallThrough => {}
        }

        // No actionable work this tick — either waiting for external work or stuck.
        if tracing::enabled!(tracing::Level::WARN) {
            warn!(
                instance_id = %instance_id,
                nodes = ?ctx.tree.iter().map(|n| format!("{}:{}:{:?}", n.block_id, n.block_type, n.state)).collect::<Vec<_>>(),
                "evaluate: no actionable work"
            );
        } else {
            warn!(
                instance_id = %instance_id,
                "evaluate: no actionable work"
            );
        }
        return Ok(EvalOutcome::MoreWork {
            has_waiting_nodes: ctx.tree.iter().any(|n| n.state == NodeState::Waiting),
        });
    }

    // Safety limit — defer to next tick. Report `has_waiting_nodes = true`
    // so the scheduler transitions to Waiting instead of Scheduled, preventing
    // an immediate re-claim that would busy-spin on every tick.
    warn!(
        instance_id = %instance.id,
        max_iterations,
        "evaluate: iteration limit reached, deferring to next tick"
    );
    Ok(EvalOutcome::MoreWork {
        has_waiting_nodes: true,
    })
}

/// Check if root nodes indicate the instance is done (all completed/skipped,
/// or any failed/cancelled).
fn check_termination(tree: &[ExecutionNode]) -> Option<EvalOutcome> {
    let root_nodes: Vec<&ExecutionNode> = tree.iter().filter(|n| n.parent_id.is_none()).collect();

    // Termination: all root nodes done.
    if root_nodes
        .iter()
        .all(|n| matches!(n.state, NodeState::Completed | NodeState::Skipped))
    {
        return Some(EvalOutcome::Done {
            any_failed: false,
            any_cancelled: false,
        });
    }
    // Termination: any root node failed/cancelled.
    if root_nodes
        .iter()
        .any(|n| matches!(n.state, NodeState::Failed | NodeState::Cancelled))
    {
        let any_failed = root_nodes.iter().any(|n| n.state == NodeState::Failed);
        let any_cancelled = root_nodes.iter().any(|n| n.state == NodeState::Cancelled);
        return Some(EvalOutcome::Done {
            any_failed,
            any_cancelled,
        });
    }

    None
}

/// Phase 1: activate the first Pending root node, but only if all preceding
/// roots are terminal. Root nodes execute sequentially — block N+1 must not
/// start until block N completes.
async fn phase_root_activation(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    ctx: &mut EvalContext,
    instance_id: InstanceId,
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    sequence: &SequenceDefinition,
    outputs_snapshot: &OutputsSnapshot,
) -> Result<IterAction, EngineError> {
    let root_nodes: Vec<&ExecutionNode> =
        ctx.tree.iter().filter(|n| n.parent_id.is_none()).collect();

    let first_pending_idx = root_nodes
        .iter()
        .position(|n| n.state == NodeState::Pending);
    let Some(idx) = first_pending_idx else {
        return Ok(IterAction::FallThrough);
    };

    // Only activate if all prior roots are done.
    let all_prior_done = root_nodes[..idx].iter().all(|n| {
        matches!(
            n.state,
            NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped
        )
    });
    if !all_prior_done {
        return Ok(IterAction::FallThrough);
    }

    // Before activating new work, check if a cancel/pause signal arrived
    // mid-tick. Process it now to avoid dispatching already-cancelled or
    // already-paused work.
    let pending_signals = storage.get_pending_signals(instance_id).await?;
    let has_control_signal = pending_signals.iter().any(|s| {
        matches!(
            s.signal_type,
            orch8_types::signal::SignalType::Cancel | orch8_types::signal::SignalType::Pause
        )
    });
    if has_control_signal {
        let abort = crate::signals::process_signals_prefetched(
            storage.as_ref(),
            instance_id,
            ctx.instance.state,
            pending_signals,
            Some(sequence),
        )
        .await?;
        if abort {
            // Distinguish Pause from Cancel so the scheduler does not
            // transition a paused instance to Cancelled.
            let now_paused = storage
                .get_instance(instance_id)
                .await?
                .is_some_and(|i| i.state == InstanceState::Paused);
            return Ok(IterAction::Return(EvalOutcome::Done {
                any_failed: false,
                any_cancelled: !now_paused,
            }));
        }
        ctx.instance_stale = true;
        ctx.tree_stale = true;
        return Ok(IterAction::Continue);
    }

    let node = root_nodes[idx];
    if let Some(block) = block_map.get(&node.block_id).copied() {
        dispatch_block(
            storage,
            handlers,
            &ctx.instance,
            node,
            block,
            &ctx.tree,
            sequence.interceptors.as_ref(),
            outputs_snapshot,
        )
        .await?;
        ctx.tree_stale = true;
        if may_mutate_instance(block) {
            ctx.instance_stale = true;
        }
        return Ok(IterAction::Continue);
    }

    Ok(IterAction::FallThrough)
}

/// Phase 2: execute Running step nodes (leaf work first).
///
/// Builds parent/node maps internally to avoid borrow conflicts with the
/// mutable `EvalContext`.
async fn phase_running_steps(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    ctx: &mut EvalContext,
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    sequence: &SequenceDefinition,
    outputs_snapshot: &OutputsSnapshot,
) -> Result<IterAction, EngineError> {
    let parent_map = build_parent_map(&ctx.tree);
    let node_map: HashMap<_, _> = ctx.tree.iter().map(|n| (n.id, n)).collect();

    // Find the running step; extract the index so we can drop the borrows
    // into ctx.tree before dispatching.
    let found_idx = find_running_step_index(&ctx.tree, block_map, handlers, &parent_map, &node_map);
    let Some(idx) = found_idx else {
        return Ok(IterAction::FallThrough);
    };

    // Look up node and block by index — these are short-lived borrows that
    // end before we mutate ctx below.
    let node = &ctx.tree[idx];
    let Some(block) = block_map.get(&node.block_id).copied() else {
        return Ok(IterAction::FallThrough);
    };

    dispatch_block(
        storage,
        handlers,
        &ctx.instance,
        node,
        block,
        &ctx.tree,
        sequence.interceptors.as_ref(),
        outputs_snapshot,
    )
    .await?;
    ctx.tree_stale = true;
    ctx.instance_stale = true;
    Ok(IterAction::Continue)
}

/// Phase 3: re-evaluate Running composite nodes (parents check child
/// completion, activate next phases like catch/finally, dispatch pending
/// children). Process deepest-first; if a composite produces no state change,
/// try the next (shallower) composite before parking.
///
/// Builds parent map internally to avoid borrow conflicts with the mutable
/// `EvalContext`.
async fn phase_composite_reevaluation(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    ctx: &mut EvalContext,
    instance_id: InstanceId,
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    sequence: &SequenceDefinition,
    outputs_snapshot: &OutputsSnapshot,
) -> Result<IterAction, EngineError> {
    let parent_map = build_parent_map(&ctx.tree);

    // Collect composite indices (into ctx.tree) so we can drop the borrows.
    let composite_indices = find_all_running_composite_indices(&ctx.tree, block_map, &parent_map);
    if composite_indices.is_empty() {
        return Ok(IterAction::FallThrough);
    }

    // Snapshot node states before dispatching any composite.
    let pre_states: Vec<(ExecutionNodeId, NodeState)> =
        ctx.tree.iter().map(|n| (n.id, n.state)).collect();

    let mut early_restart = false;
    for idx in &composite_indices {
        let node = &ctx.tree[*idx];
        let Some(block) = block_map.get(&node.block_id).copied() else {
            continue;
        };

        dispatch_block(
            storage,
            handlers,
            &ctx.instance,
            node,
            block,
            &ctx.tree,
            sequence.interceptors.as_ref(),
            outputs_snapshot,
        )
        .await?;
        if may_mutate_instance(block) {
            ctx.instance_stale = true;
            let mid_tree = storage.get_execution_tree(instance_id).await?;
            let mid_states: Vec<(ExecutionNodeId, NodeState)> =
                mid_tree.iter().map(|n| (n.id, n.state)).collect();
            if pre_states != mid_states {
                ctx.set_tree(mid_tree);
                early_restart = true;
                break;
            }
        }
    }
    if early_restart {
        return Ok(IterAction::Continue);
    }

    let post_tree = storage.get_execution_tree(instance_id).await?;
    let post_states: Vec<(ExecutionNodeId, NodeState)> =
        post_tree.iter().map(|n| (n.id, n.state)).collect();
    if pre_states != post_states {
        ctx.set_tree(post_tree);
        return Ok(IterAction::Continue);
    }
    debug!(
        instance_id = %instance_id,
        "evaluate: all composites re-entry produced no state change; parking tick"
    );
    Ok(IterAction::Return(EvalOutcome::MoreWork {
        has_waiting_nodes: post_tree.iter().any(|n| n.state == NodeState::Waiting),
    }))
}

fn may_mutate_instance(block: &BlockDefinition) -> bool {
    matches!(
        block,
        BlockDefinition::Step(_)
            | BlockDefinition::ForEach(_)
            | BlockDefinition::TryCatch(_)
            | BlockDefinition::Loop(_)
    )
}

/// Return indices (into `tree`) of all Running composite nodes, deepest first.
fn find_all_running_composite_indices(
    tree: &[ExecutionNode],
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    parent_map: &ParentMap,
) -> Vec<usize> {
    let mut composites: Vec<(usize, usize)> = tree
        .iter()
        .enumerate()
        .filter(|(_, n)| {
            n.state == NodeState::Running
                || (n.state == NodeState::Waiting
                    && n.block_type == orch8_types::execution::BlockType::SubSequence)
        })
        .filter(|(_, n)| {
            block_map
                .get(&n.block_id)
                .copied()
                .is_some_and(|b| !matches!(b, BlockDefinition::Step(_)))
        })
        .map(|(i, n)| (i, count_ancestors(parent_map, n.id)))
        .collect();
    // Sort deepest first (most ancestors first).
    composites.sort_by_key(|(_, depth)| std::cmp::Reverse(*depth));
    composites.into_iter().map(|(i, _)| i).collect()
}

/// Find the index (into `tree`) of the first Running step node that can be
/// executed. Returns an index rather than a reference to avoid holding a
/// borrow into the tree across mutable operations.
fn find_running_step_index(
    tree: &[ExecutionNode],
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    handlers: &HandlerRegistry,
    parent_map: &ParentMap,
    node_map: &HashMap<ExecutionNodeId, &ExecutionNode>,
) -> Option<usize> {
    for (i, node) in tree.iter().enumerate() {
        if node.state != NodeState::Running {
            continue;
        }
        if let Some(BlockDefinition::Step(step_def)) = block_map.get(&node.block_id).copied() {
            if is_inside_decided_race(tree, block_map, node, parent_map, node_map) {
                continue;
            }
            if handlers.contains(&step_def.handler)
                && has_racing_composite_sibling(tree, block_map, node, parent_map, node_map)
            {
                continue;
            }
            return Some(i);
        }
    }
    None
}

/// Find a Running step node that can be executed.
///
/// Kept for use in unit tests; the main evaluation loop uses
/// [`find_running_step_index`] which returns an index to avoid borrow conflicts.
#[cfg(test)]
fn find_running_step<'a>(
    tree: &'a [ExecutionNode],
    block_map: &HashMap<&BlockId, &'a BlockDefinition>,
    handlers: &HandlerRegistry,
    parent_map: &ParentMap,
    node_map: &HashMap<ExecutionNodeId, &'a ExecutionNode>,
) -> Option<(&'a ExecutionNode, &'a BlockDefinition)> {
    for node in tree {
        if node.state != NodeState::Running {
            continue;
        }
        if let Some(block) = block_map.get(&node.block_id).copied() {
            if let BlockDefinition::Step(step_def) = block {
                // Skip steps inside a race where another branch already won.
                if is_inside_decided_race(tree, block_map, node, parent_map, node_map) {
                    continue;
                }
                // Defer *in-process* steps that race against a composite sibling
                // branch that hasn't finished yet.
                if handlers.contains(&step_def.handler)
                    && has_racing_composite_sibling(tree, block_map, node, parent_map, node_map)
                {
                    continue;
                }
                return Some((node, block));
            }
        }
    }
    None
}

/// Check if a node is inside a Race composite that already has a winner
/// (another branch's direct child is Completed). If so, this node's
/// branch lost and should not be dispatched.
fn is_inside_decided_race(
    tree: &[ExecutionNode],
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    node: &ExecutionNode,
    parent_map: &ParentMap,
    node_map: &HashMap<ExecutionNodeId, &ExecutionNode>,
) -> bool {
    // Walk up tracking which direct-child-of-race we came through.
    let mut current_id = node.id;
    while let Some(Some(parent_id)) = parent_map.get(&current_id) {
        if let Some(parent_node) = node_map.get(parent_id).copied() {
            if let Some(parent_block) = block_map.get(&parent_node.block_id).copied() {
                if matches!(parent_block, BlockDefinition::Race(_)) {
                    let my_branch = node_map
                        .get(&current_id)
                        .copied()
                        .and_then(|n| n.branch_index);
                    let sibling_completed =
                        children_of(tree, *parent_id, None).into_iter().any(|c| {
                            c.branch_index != my_branch && matches!(c.state, NodeState::Completed)
                        });
                    if sibling_completed {
                        return true;
                    }
                }
            }
        }
        current_id = *parent_id;
    }
    false
}

/// Check if a step node is inside a Race and a sibling branch contains a
/// Running composite. When true, the evaluator should defer executing this
/// step to avoid blocking: the composite sibling may complete quickly (e.g.
/// a try-catch that fails-and-recovers instantly), and executing the step
/// inline (e.g. a 1000ms sleep) would starve the composite branch.
fn has_racing_composite_sibling(
    tree: &[ExecutionNode],
    block_map: &HashMap<&BlockId, &BlockDefinition>,
    node: &ExecutionNode,
    parent_map: &ParentMap,
    node_map: &HashMap<ExecutionNodeId, &ExecutionNode>,
) -> bool {
    let mut current_id = node.id;
    while let Some(Some(parent_id)) = parent_map.get(&current_id) {
        if let Some(parent_node) = node_map.get(parent_id).copied() {
            if let Some(parent_block) = block_map.get(&parent_node.block_id).copied() {
                if matches!(parent_block, BlockDefinition::Race(_)) {
                    let my_branch = node_map
                        .get(&current_id)
                        .copied()
                        .and_then(|n| n.branch_index);
                    let sibling_composite_running =
                        children_of(tree, *parent_id, None).into_iter().any(|c| {
                            c.branch_index != my_branch
                                && c.state == NodeState::Running
                                && block_map
                                    .get(&c.block_id)
                                    .copied()
                                    .is_some_and(|b| !matches!(b, BlockDefinition::Step(_)))
                        });
                    if sibling_composite_running {
                        return true;
                    }
                }
            }
        }
        current_id = *parent_id;
    }
    false
}

/// Count ancestors to determine tree depth.
fn count_ancestors(parent_map: &ParentMap, mut node_id: ExecutionNodeId) -> usize {
    let mut depth = 0;
    while let Some(Some(parent_id)) = parent_map.get(&node_id) {
        depth += 1;
        node_id = *parent_id;
    }
    depth
}

/// Flatten a nested block tree into a `HashMap` for O(1) lookups.
pub fn flatten_blocks(blocks: &[BlockDefinition]) -> HashMap<&BlockId, &BlockDefinition> {
    fn walk<'b>(
        blocks: &'b [BlockDefinition],
        map: &mut HashMap<&'b BlockId, &'b BlockDefinition>,
    ) {
        for block in blocks {
            let id = match block {
                BlockDefinition::Step(s) => &s.id,
                BlockDefinition::Parallel(p) => &p.id,
                BlockDefinition::Race(r) => &r.id,
                BlockDefinition::Loop(l) => &l.id,
                BlockDefinition::ForEach(f) => &f.id,
                BlockDefinition::Router(r) => &r.id,
                BlockDefinition::TryCatch(tc) => &tc.id,
                BlockDefinition::SubSequence(ss) => &ss.id,
                BlockDefinition::ABSplit(ab) => &ab.id,
                BlockDefinition::CancellationScope(cs) => &cs.id,
            };
            map.insert(id, block);
            match block {
                BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
                BlockDefinition::Parallel(p) => {
                    for branch in &p.branches {
                        walk(branch, map);
                    }
                }
                BlockDefinition::Race(r) => {
                    for branch in &r.branches {
                        walk(branch, map);
                    }
                }
                BlockDefinition::Loop(l) => walk(&l.body, map),
                BlockDefinition::ForEach(f) => walk(&f.body, map),
                BlockDefinition::Router(r) => {
                    for route in &r.routes {
                        walk(&route.blocks, map);
                    }
                    if let Some(default) = &r.default {
                        walk(default, map);
                    }
                }
                BlockDefinition::TryCatch(tc) => {
                    walk(&tc.try_block, map);
                    walk(&tc.catch_block, map);
                    if let Some(finally) = &tc.finally_block {
                        walk(finally, map);
                    }
                }
                BlockDefinition::ABSplit(ab) => {
                    for variant in &ab.variants {
                        walk(&variant.blocks, map);
                    }
                }
                BlockDefinition::CancellationScope(cs) => walk(&cs.blocks, map),
            }
        }
    }
    let mut map = HashMap::with_capacity(blocks.len() * 2);
    walk(blocks, &mut map);
    map
}

/// Find a block definition by ID in the block tree.
#[allow(clippy::needless_lifetimes)]
pub fn find_block<'a>(
    blocks: &'a [BlockDefinition],
    target_id: &BlockId,
) -> Option<&'a BlockDefinition> {
    for block in blocks {
        let id = match block {
            BlockDefinition::Step(s) => &s.id,
            BlockDefinition::Parallel(p) => &p.id,
            BlockDefinition::Race(r) => &r.id,
            BlockDefinition::Loop(l) => &l.id,
            BlockDefinition::ForEach(f) => &f.id,
            BlockDefinition::Router(r) => &r.id,
            BlockDefinition::TryCatch(tc) => &tc.id,
            BlockDefinition::SubSequence(ss) => &ss.id,
            BlockDefinition::ABSplit(ab) => &ab.id,
            BlockDefinition::CancellationScope(cs) => &cs.id,
        };
        if id == target_id {
            return Some(block);
        }
        // Recurse into children.
        let children = match block {
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => None,
            BlockDefinition::Parallel(p) => {
                for branch in &p.branches {
                    if let Some(found) = find_block(branch, target_id) {
                        return Some(found);
                    }
                }
                None
            }
            BlockDefinition::Race(r) => {
                for branch in &r.branches {
                    if let Some(found) = find_block(branch, target_id) {
                        return Some(found);
                    }
                }
                None
            }
            BlockDefinition::Loop(l) => Some(&l.body),
            BlockDefinition::ForEach(f) => Some(&f.body),
            BlockDefinition::Router(r) => {
                for route in &r.routes {
                    if let Some(found) = find_block(&route.blocks, target_id) {
                        return Some(found);
                    }
                }
                r.default.as_ref()
            }
            BlockDefinition::TryCatch(tc) => {
                if let Some(found) = find_block(&tc.try_block, target_id) {
                    return Some(found);
                }
                if let Some(found) = find_block(&tc.catch_block, target_id) {
                    return Some(found);
                }
                tc.finally_block.as_ref()
            }
            BlockDefinition::ABSplit(ab) => {
                for variant in &ab.variants {
                    if let Some(found) = find_block(&variant.blocks, target_id) {
                        return Some(found);
                    }
                }
                None
            }
            BlockDefinition::CancellationScope(cs) => Some(&cs.blocks),
        };
        if let Some(children) = children {
            if let Some(found) = find_block(children, target_id) {
                return Some(found);
            }
        }
    }
    None
}

/// Mark a node as completed.
pub async fn complete_node(
    storage: &dyn StorageBackend,
    node_id: ExecutionNodeId,
) -> Result<(), EngineError> {
    debug!(node_id = %node_id, "node → Completed");
    storage
        .update_node_state(node_id, NodeState::Completed)
        .await?;
    Ok(())
}

/// Mark a node as failed.
pub async fn fail_node(
    storage: &dyn StorageBackend,
    node_id: ExecutionNodeId,
) -> Result<(), EngineError> {
    debug!(node_id = %node_id, "node → Failed");
    storage
        .update_node_state(node_id, NodeState::Failed)
        .await?;
    Ok(())
}

/// Get child nodes for a parent, optionally filtered by branch index.
pub fn children_of(
    tree: &[ExecutionNode],
    parent_id: ExecutionNodeId,
    branch_index: Option<i16>,
) -> Vec<&ExecutionNode> {
    tree.iter()
        .filter(|n| {
            n.parent_id == Some(parent_id)
                && (branch_index.is_none() || n.branch_index == branch_index)
        })
        .collect()
}

/// Check if all nodes in a set are in a terminal state.
pub fn all_terminal(nodes: &[&ExecutionNode]) -> bool {
    nodes.iter().all(|n| {
        matches!(
            n.state,
            NodeState::Completed | NodeState::Failed | NodeState::Skipped | NodeState::Cancelled
        )
    })
}

/// Check if any node in a set has completed.
pub fn any_completed(nodes: &[&ExecutionNode]) -> bool {
    nodes.iter().any(|n| n.state == NodeState::Completed)
}

/// Check if all nodes completed successfully.
pub fn all_completed(nodes: &[&ExecutionNode]) -> bool {
    nodes.iter().all(|n| n.state == NodeState::Completed)
}

/// Check if any node failed.
pub fn any_failed(nodes: &[&ExecutionNode]) -> bool {
    nodes.iter().any(|n| n.state == NodeState::Failed)
}

/// Check if the tree has any nodes waiting for external work.
pub fn has_waiting_nodes(tree: &[ExecutionNode]) -> bool {
    tree.iter().any(|n| n.state == NodeState::Waiting)
}

/// Recursively cancel every node in the subtree rooted at `parent_id`,
/// including cancelling any pending worker tasks for `Waiting` descendants.
///
/// The nodes are updated bottom-up so parent state changes don't interfere
/// with descendant lookups.
pub async fn cancel_subtree(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    tree: &[ExecutionNode],
    parent_id: ExecutionNodeId,
) -> Result<(), EngineError> {
    // Collect all descendants of parent_id (not including parent_id itself).
    let mut to_cancel: Vec<ExecutionNodeId> = Vec::new();
    let mut stack = vec![parent_id];
    while let Some(current) = stack.pop() {
        for node in tree {
            if node.parent_id == Some(current) {
                stack.push(node.id);
                to_cancel.push(node.id);
            }
        }
    }

    // Build a HashSet for O(1) lookup when scanning the tree.
    let to_cancel_set: std::collections::HashSet<ExecutionNodeId> =
        to_cancel.iter().copied().collect();

    // Cancel worker tasks for any Waiting descendants before we flip their state.
    for node in tree {
        if to_cancel_set.contains(&node.id) && node.state == NodeState::Waiting {
            storage
                .cancel_worker_tasks_for_block(instance_id.into_uuid(), node.block_id.as_str())
                .await?;
        }
    }

    // Mark every descendant as Cancelled.
    for id in to_cancel {
        storage.update_node_state(id, NodeState::Cancelled).await?;
    }

    Ok(())
}

/// Activate all `Pending` children by flipping them to `Running`.
///
/// Use this for composites that need concurrent fan-out (`race`, `parallel`).
/// For sequential bodies use [`activate_first_pending_child`] so that only
/// the cursor block runs and subsequent blocks remain Pending until their
/// predecessor finishes.
pub async fn activate_pending_children(
    storage: &dyn StorageBackend,
    children: &[&ExecutionNode],
) -> Result<(), EngineError> {
    let pending_ids: Vec<ExecutionNodeId> = children
        .iter()
        .filter(|c| c.state == NodeState::Pending)
        .map(|c| c.id)
        .collect();
    if !pending_ids.is_empty() {
        storage.batch_activate_nodes(&pending_ids).await?;
    }
    Ok(())
}

/// Activate only the first `Pending` child (sequential cursor semantics).
///
/// Used by composites whose body is an ordered sequence: `router` branches,
/// `try_catch` phases, `loop` bodies, and `for_each` bodies. Activating every
/// pending child at once would turn sequential execution into parallel fan-out.
pub async fn activate_first_pending_child(
    storage: &dyn StorageBackend,
    children: &[&ExecutionNode],
) -> Result<(), EngineError> {
    for child in children {
        match child.state {
            NodeState::Completed
            | NodeState::Skipped
            | NodeState::Failed
            | NodeState::Cancelled => {}
            NodeState::Pending => {
                storage
                    .update_node_state(child.id, NodeState::Running)
                    .await?;
                return Ok(());
            }
            NodeState::Running | NodeState::Waiting | _ => {
                return Ok(());
            }
        }
    }
    Ok(())
}
