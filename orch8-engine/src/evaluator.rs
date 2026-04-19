use std::borrow::Cow;
use std::sync::Arc;

use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

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
        let existing_block_ids: std::collections::HashSet<&str> =
            existing.iter().map(|n| n.block_id.0.as_str()).collect();
        let mut new_nodes = Vec::new();
        for block in blocks {
            let (bid, _) = block_meta(block);
            if !existing_block_ids.contains(bid.0.as_str()) {
                build_nodes(
                    instance.id,
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
    let mut nodes = Vec::new();
    build_nodes(instance.id, None, blocks, &mut nodes);

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
    blocks: &[BlockDefinition],
    out: &mut Vec<ExecutionNode>,
) {
    for block in blocks {
        let (block_id, block_type) = block_meta(block);
        let node_id = ExecutionNodeId::new();

        out.push(ExecutionNode {
            id: node_id,
            instance_id,
            block_id: block_id.clone(),
            parent_id,
            block_type,
            branch_index: None,
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        });

        // Recurse into children.
        match block {
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
            BlockDefinition::Parallel(p) => {
                for (i, branch) in p.branches.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, branch, out);
                }
            }
            BlockDefinition::Race(r) => {
                for (i, branch) in r.branches.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, branch, out);
                }
            }
            BlockDefinition::Loop(l) => {
                build_nodes(instance_id, Some(node_id), &l.body, out);
            }
            BlockDefinition::ForEach(f) => {
                build_nodes(instance_id, Some(node_id), &f.body, out);
            }
            BlockDefinition::Router(r) => {
                for (i, route) in r.routes.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, &route.blocks, out);
                }
                if let Some(default) = &r.default {
                    build_branch_nodes(instance_id, node_id, r.routes.len(), default, out);
                }
            }
            BlockDefinition::TryCatch(tc) => {
                build_branch_nodes(instance_id, node_id, 0, &tc.try_block, out);
                build_branch_nodes(instance_id, node_id, 1, &tc.catch_block, out);
                if let Some(finally) = &tc.finally_block {
                    build_branch_nodes(instance_id, node_id, 2, finally, out);
                }
            }
            BlockDefinition::ABSplit(ab) => {
                for (i, variant) in ab.variants.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, &variant.blocks, out);
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                build_nodes(instance_id, Some(node_id), &cs.blocks, out);
            }
        }
    }
}

fn build_branch_nodes(
    instance_id: InstanceId,
    parent_id: ExecutionNodeId,
    branch_index: usize,
    blocks: &[BlockDefinition],
    out: &mut Vec<ExecutionNode>,
) {
    for block in blocks {
        let (block_id, block_type) = block_meta(block);
        let node_id = ExecutionNodeId::new();

        out.push(ExecutionNode {
            id: node_id,
            instance_id,
            block_id: block_id.clone(),
            parent_id: Some(parent_id),
            block_type,
            branch_index: Some(i16::try_from(branch_index).unwrap_or(i16::MAX)),
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        });

        // Recurse for nested composites within the branch.
        match block {
            BlockDefinition::Step(_) | BlockDefinition::SubSequence(_) => {}
            BlockDefinition::Parallel(p) => {
                for (i, branch) in p.branches.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, branch, out);
                }
            }
            BlockDefinition::Race(r) => {
                for (i, branch) in r.branches.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, branch, out);
                }
            }
            BlockDefinition::Loop(l) => {
                build_nodes(instance_id, Some(node_id), &l.body, out);
            }
            BlockDefinition::ForEach(f) => {
                build_nodes(instance_id, Some(node_id), &f.body, out);
            }
            BlockDefinition::Router(r) => {
                for (i, route) in r.routes.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, &route.blocks, out);
                }
                if let Some(default) = &r.default {
                    build_branch_nodes(instance_id, node_id, r.routes.len(), default, out);
                }
            }
            BlockDefinition::TryCatch(tc) => {
                build_branch_nodes(instance_id, node_id, 0, &tc.try_block, out);
                build_branch_nodes(instance_id, node_id, 1, &tc.catch_block, out);
                if let Some(finally) = &tc.finally_block {
                    build_branch_nodes(instance_id, node_id, 2, finally, out);
                }
            }
            BlockDefinition::ABSplit(ab) => {
                for (i, variant) in ab.variants.iter().enumerate() {
                    build_branch_nodes(instance_id, node_id, i, &variant.blocks, out);
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                build_nodes(instance_id, Some(node_id), &cs.blocks, out);
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
/// Returns `true` if there is more work to do (instance should be re-scheduled).
#[allow(clippy::too_many_lines)]
pub async fn evaluate(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<bool, EngineError> {
    // Merge sequence blocks with any dynamically injected blocks.
    let blocks = merged_blocks(storage.as_ref(), instance.id, sequence).await?;

    // Ensure the execution tree exists (creates on first call, adds new injected nodes).
    ensure_execution_tree(storage.as_ref(), instance, &blocks).await?;

    // Loop: each iteration dispatches one actionable node, then re-reads
    // the tree to find more work. This lets parallel branches, try-catch
    // phases, etc. all make progress within a single scheduler tick.
    let max_iterations = 200;
    for _ in 0..max_iterations {
        // Re-fetch instance to pick up context mutations from composite
        // handlers (e.g. for_each::bind_item_var writes context.data).
        let instance = &storage
            .get_instance(instance.id)
            .await?
            .ok_or_else(|| EngineError::NotFound(format!("instance {}", instance.id)))?;

        // Re-fetch tree to see latest state after each dispatch.
        let mut tree = storage.get_execution_tree(instance.id).await?;

        // SLA deadline check: fail any step nodes that have breached their deadline.
        let deadlines_breached =
            check_sla_deadlines(storage, handlers, instance, &blocks, &tree).await?;
        // Re-fetch tree only if deadlines were breached (state may have changed).
        if deadlines_breached {
            tree = storage.get_execution_tree(instance.id).await?;
        }
        let root_nodes: Vec<&ExecutionNode> =
            tree.iter().filter(|n| n.parent_id.is_none()).collect();

        // Termination: all root nodes done.
        if root_nodes
            .iter()
            .all(|n| matches!(n.state, NodeState::Completed | NodeState::Skipped))
        {
            return Ok(false);
        }
        // Termination: any root node failed/cancelled.
        if root_nodes
            .iter()
            .any(|n| matches!(n.state, NodeState::Failed | NodeState::Cancelled))
        {
            return Ok(false);
        }

        // Phase 1: activate the first Pending root node, but only if all
        // preceding roots are terminal. Root nodes execute sequentially —
        // block N+1 must not start until block N completes.
        let first_pending_idx = root_nodes
            .iter()
            .position(|n| n.state == NodeState::Pending);
        if let Some(idx) = first_pending_idx {
            // Only activate if all prior roots are done.
            let all_prior_done = root_nodes[..idx].iter().all(|n| {
                matches!(
                    n.state,
                    NodeState::Completed
                        | NodeState::Failed
                        | NodeState::Cancelled
                        | NodeState::Skipped
                )
            });
            if all_prior_done {
                // Before activating new work, check if a cancel/pause signal
                // arrived mid-tick (e.g. during a sleep inside a finally
                // block). Process it now to avoid dispatching already-cancelled
                // work.
                let pending_signals = storage.get_pending_signals(instance.id).await?;
                let has_cancel = pending_signals
                    .iter()
                    .any(|s| matches!(s.signal_type, orch8_types::signal::SignalType::Cancel));
                if has_cancel {
                    let abort = crate::signals::process_signals_prefetched(
                        storage.as_ref(),
                        instance.id,
                        instance.state,
                        pending_signals,
                        Some(sequence),
                    )
                    .await?;
                    if abort {
                        return Ok(false);
                    }
                    let _updated = storage.get_execution_tree(instance.id).await?;
                    continue;
                }
                let node = root_nodes[idx];
                if let Some(block) = find_block(&blocks, &node.block_id) {
                    dispatch_block(
                        storage,
                        handlers,
                        instance,
                        node,
                        block,
                        &tree,
                        sequence.interceptors.as_ref(),
                    )
                    .await?;
                    continue;
                }
            }
        }

        // Phase 2: execute Running step nodes (leaf work first).
        if let Some((node, block)) = find_running_step(&tree, &blocks, handlers) {
            dispatch_block(
                storage,
                handlers,
                instance,
                &node,
                block,
                &tree,
                sequence.interceptors.as_ref(),
            )
            .await?;
            continue;
        }

        // Phase 3: re-evaluate Running composite nodes (parents check child completion,
        // activate next phases like catch/finally, dispatch pending children).
        // Process deepest-first; if a composite produces no state change, try the
        // next (shallower) composite before parking — a parent race may be able to
        // complete even when a child try_catch inside it is stalled.
        {
            let composites = find_all_running_composites(&tree, &blocks);
            let mut any_progressed = false;
            for (node, block) in &composites {
                let pre_states: Vec<(ExecutionNodeId, NodeState)> =
                    tree.iter().map(|n| (n.id, n.state)).collect();
                dispatch_block(
                    storage,
                    handlers,
                    instance,
                    node,
                    block,
                    &tree,
                    sequence.interceptors.as_ref(),
                )
                .await?;
                let post_tree = storage.get_execution_tree(instance.id).await?;
                let post_states: Vec<(ExecutionNodeId, NodeState)> =
                    post_tree.iter().map(|n| (n.id, n.state)).collect();
                if pre_states != post_states {
                    any_progressed = true;
                    break;
                }
            }
            if !composites.is_empty() {
                if any_progressed {
                    continue;
                }
                debug!(
                    instance_id = %instance.id,
                    "evaluate: all composites re-entry produced no state change; parking tick"
                );
                return Ok(true);
            }
        }

        // No actionable work this tick — either waiting for external work or stuck.
        warn!(
            instance_id = %instance.id,
            nodes = ?tree.iter().map(|n| format!("{}:{}:{:?}", n.block_id, n.block_type, n.state)).collect::<Vec<_>>(),
            "evaluate: no actionable work"
        );
        return Ok(true);
    }

    // Safety limit — re-schedule for next tick.
    warn!(
        instance_id = %instance.id,
        max_iterations,
        "evaluate: iteration limit reached, re-scheduling for next tick"
    );
    Ok(true)
}

/// Return all Running composite nodes, deepest first, for iterative evaluation.
fn find_all_running_composites<'a>(
    tree: &'a [ExecutionNode],
    blocks: &'a [BlockDefinition],
) -> Vec<(ExecutionNode, &'a BlockDefinition)> {
    let mut composites: Vec<(ExecutionNode, &BlockDefinition)> = tree
        .iter()
        .filter(|n| {
            n.state == NodeState::Running
                || (n.state == NodeState::Waiting
                    && n.block_type == orch8_types::execution::BlockType::SubSequence)
        })
        .filter_map(|n| {
            find_block(blocks, &n.block_id)
                .filter(|b| !matches!(b, BlockDefinition::Step(_)))
                .map(|b| (n.clone(), b))
        })
        .collect();
    // Sort deepest first (most ancestors → processed first).
    composites.sort_by_key(|(b, _)| std::cmp::Reverse(count_ancestors(tree, b.id)));
    composites
}

/// Find a Running step node that can be executed.
fn find_running_step<'a>(
    tree: &'a [ExecutionNode],
    blocks: &'a [BlockDefinition],
    handlers: &HandlerRegistry,
) -> Option<(ExecutionNode, &'a BlockDefinition)> {
    for node in tree {
        if node.state != NodeState::Running {
            continue;
        }
        if let Some(block) = find_block(blocks, &node.block_id) {
            if let BlockDefinition::Step(step_def) = block {
                // Skip steps inside a race where another branch already won.
                if is_inside_decided_race(tree, blocks, node) {
                    continue;
                }
                // Defer *in-process* steps that race against a composite sibling
                // branch that hasn't finished yet. Processing a blocking handler
                // (e.g. sleep) inline would starve the composite branch. Let
                // Phase 3 advance the composite first; once it completes, the
                // race is decided and this step is either skipped (race lost) or
                // dispatched next iteration.
                //
                // External worker steps (unregistered handlers) are NOT deferred
                // because they dispatch asynchronously and return immediately —
                // they don't block the evaluator.
                if handlers.contains(&step_def.handler)
                    && has_racing_composite_sibling(tree, blocks, node)
                {
                    continue;
                }
                return Some((node.clone(), block));
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
    blocks: &[BlockDefinition],
    node: &ExecutionNode,
) -> bool {
    let parent_map: std::collections::HashMap<ExecutionNodeId, Option<ExecutionNodeId>> =
        tree.iter().map(|n| (n.id, n.parent_id)).collect();
    // Walk up tracking which direct-child-of-race we came through.
    let mut current_id = node.id;
    while let Some(Some(parent_id)) = parent_map.get(&current_id) {
        if let Some(parent_node) = tree.iter().find(|n| n.id == *parent_id) {
            if let Some(parent_block) = find_block(blocks, &parent_node.block_id) {
                if matches!(parent_block, BlockDefinition::Race(_)) {
                    // `current_id` is the direct child of the race through
                    // which our original node descends. Find its branch index.
                    let my_branch = tree
                        .iter()
                        .find(|n| n.id == current_id)
                        .and_then(|n| n.branch_index);
                    // Check if a sibling branch's direct child already completed.
                    let sibling_completed = children_of(tree, *parent_id, None).iter().any(|c| {
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
    blocks: &[BlockDefinition],
    node: &ExecutionNode,
) -> bool {
    let parent_map: std::collections::HashMap<ExecutionNodeId, Option<ExecutionNodeId>> =
        tree.iter().map(|n| (n.id, n.parent_id)).collect();
    let mut current_id = node.id;
    while let Some(Some(parent_id)) = parent_map.get(&current_id) {
        if let Some(parent_node) = tree.iter().find(|n| n.id == *parent_id) {
            if let Some(parent_block) = find_block(blocks, &parent_node.block_id) {
                if matches!(parent_block, BlockDefinition::Race(_)) {
                    let my_branch = tree
                        .iter()
                        .find(|n| n.id == current_id)
                        .and_then(|n| n.branch_index);
                    // Check if a sibling branch's root is a Running composite.
                    let sibling_composite_running =
                        children_of(tree, *parent_id, None).iter().any(|c| {
                            c.branch_index != my_branch
                                && c.state == NodeState::Running
                                && find_block(blocks, &c.block_id)
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
/// Uses a `HashMap` index for O(depth) lookup instead of O(n) per ancestor.
fn count_ancestors(tree: &[ExecutionNode], mut node_id: ExecutionNodeId) -> usize {
    let parent_map: std::collections::HashMap<ExecutionNodeId, Option<ExecutionNodeId>> =
        tree.iter().map(|n| (n.id, n.parent_id)).collect();
    let mut depth = 0;
    while let Some(Some(parent_id)) = parent_map.get(&node_id) {
        depth += 1;
        node_id = *parent_id;
    }
    depth
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

/// Check all Running/Waiting step nodes for SLA deadline breaches.
/// On breach: invoke escalation handler (if configured), then fail the node.
/// Returns `true` if any deadline was breached (tree state was modified).
async fn check_sla_deadlines(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    blocks: &[BlockDefinition],
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    let now = chrono::Utc::now();
    let mut breached = false;
    for node in tree {
        if !matches!(node.state, NodeState::Running | NodeState::Waiting) {
            continue;
        }
        let Some(started_at) = node.started_at else {
            continue;
        };
        let Some(BlockDefinition::Step(step_def)) = find_block(blocks, &node.block_id) else {
            continue;
        };
        let Some(deadline) = step_def.deadline else {
            continue;
        };
        let elapsed = now - started_at;
        if elapsed < chrono::Duration::from_std(deadline).unwrap_or(chrono::TimeDelta::MAX) {
            continue;
        }
        // Deadline breached!
        breached = true;
        warn!(
            instance_id = %instance.id,
            block_id = %node.block_id,
            deadline_ms = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
            elapsed_ms = elapsed.num_milliseconds(),
            "SLA deadline breached"
        );

        // Invoke escalation handler if configured.
        if let Some(ref escalation) = step_def.on_deadline_breach {
            if let Some(handler) = handlers.get(&escalation.handler) {
                let mut params = escalation.params.clone();
                // Inject breach metadata into escalation params.
                if let serde_json::Value::Object(ref mut map) = params {
                    map.insert(
                        "_breach_block_id".into(),
                        serde_json::json!(node.block_id.0),
                    );
                    map.insert(
                        "_breach_instance_id".into(),
                        serde_json::json!(instance.id.0),
                    );
                    map.insert(
                        "_breach_elapsed_ms".into(),
                        serde_json::json!(elapsed.num_milliseconds()),
                    );
                    map.insert(
                        "_breach_deadline_ms".into(),
                        serde_json::json!(u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX)),
                    );
                }
                let step_ctx = crate::handlers::StepContext {
                    instance_id: instance.id,
                    tenant_id: instance.tenant_id.clone(),
                    block_id: node.block_id.clone(),
                    params,
                    context: instance.context.clone(),
                    attempt: 0,
                    storage: Arc::clone(storage),
                };
                // Fire-and-forget: escalation handler failure doesn't block the deadline fail.
                if let Err(e) = handler(step_ctx).await {
                    warn!(
                        instance_id = %instance.id,
                        block_id = %node.block_id,
                        error = %e,
                        "SLA escalation handler failed"
                    );
                }
            } else {
                warn!(
                    instance_id = %instance.id,
                    handler = %escalation.handler,
                    "SLA escalation handler not found"
                );
            }
        }

        // Fail the node.
        fail_node(storage.as_ref(), node.id).await?;

        // Record as block output for diagnostics.
        let output = orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: instance.id,
            block_id: node.block_id.clone(),
            output: serde_json::json!({
                "_error": "sla_deadline_breached",
                "_deadline_ms": u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
                "_elapsed_ms": elapsed.num_milliseconds(),
            }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: now,
        };
        storage.save_block_output(&output).await?;
    }
    Ok(breached)
}

/// Dispatch a single execution node to the appropriate block handler.
/// Returns `true` if the instance has more work to do.
#[allow(clippy::too_many_lines)]
async fn dispatch_block(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    block: &BlockDefinition,
    tree: &[ExecutionNode],
    interceptors: Option<&orch8_types::interceptor::InterceptorDef>,
) -> Result<bool, EngineError> {
    // Mark node as running.
    if node.state == NodeState::Pending {
        storage
            .update_node_state(node.id, NodeState::Running)
            .await?;
    }

    match block {
        BlockDefinition::Step(step_def) => {
            // Interceptor: before_step
            if let Some(ic) = interceptors {
                crate::interceptors::emit_before_step(
                    storage.as_ref(),
                    ic,
                    instance.id,
                    &step_def.id,
                )
                .await;
            }
            let result = crate::handlers::step_block::execute_step_node(
                storage, handlers, instance, node, step_def,
            )
            .await;
            // Interceptor: after_step
            if let Some(ic) = interceptors {
                crate::interceptors::emit_after_step(
                    storage.as_ref(),
                    ic,
                    instance.id,
                    &step_def.id,
                )
                .await;
            }
            result
        }
        BlockDefinition::Parallel(par_def) => {
            crate::handlers::parallel::execute_parallel(
                storage.as_ref(),
                handlers,
                instance,
                node,
                par_def,
                tree,
            )
            .await
        }
        BlockDefinition::Race(race_def) => {
            crate::handlers::race::execute_race(
                storage.as_ref(),
                handlers,
                instance,
                node,
                race_def,
                tree,
            )
            .await
        }
        BlockDefinition::Loop(loop_def) => {
            crate::handlers::loop_block::execute_loop(
                storage.as_ref(),
                handlers,
                instance,
                node,
                loop_def,
                tree,
            )
            .await
        }
        BlockDefinition::ForEach(fe_def) => {
            crate::handlers::for_each::execute_for_each(
                storage.as_ref(),
                handlers,
                instance,
                node,
                fe_def,
                tree,
            )
            .await
        }
        BlockDefinition::Router(router_def) => {
            crate::handlers::router::execute_router(
                storage.as_ref(),
                handlers,
                instance,
                node,
                router_def,
                tree,
            )
            .await
        }
        BlockDefinition::TryCatch(tc_def) => {
            crate::handlers::try_catch::execute_try_catch(
                storage.as_ref(),
                handlers,
                instance,
                node,
                tc_def,
                tree,
            )
            .await
        }
        BlockDefinition::ABSplit(ab_def) => {
            crate::handlers::ab_split::execute_ab_split(
                storage.as_ref(),
                handlers,
                instance,
                node,
                ab_def,
                tree,
            )
            .await
        }
        BlockDefinition::CancellationScope(cs_def) => {
            crate::handlers::cancellation_scope::execute_cancellation_scope(
                storage.as_ref(),
                handlers,
                instance,
                node,
                cs_def,
                tree,
            )
            .await
        }
        BlockDefinition::SubSequence(ss_def) => {
            // Sub-sequence: create a child instance and wait for it to complete.
            // Check if child already exists for this block.
            let children = storage.get_child_instances(instance.id).await?;
            let existing_child = children.iter().find(|c| {
                c.metadata.get("_parent_block_id").and_then(|v| v.as_str()) == Some(&ss_def.id.0)
            });

            if let Some(child) = existing_child {
                // Child exists — check if it's done.
                if child.state == orch8_types::instance::InstanceState::Completed {
                    // Save child outputs as this block's output.
                    let child_outputs = storage.get_all_outputs(child.id).await?;
                    let output_val = serde_json::to_value(&child_outputs).unwrap_or_default();
                    let block_output = orch8_types::output::BlockOutput {
                        id: uuid::Uuid::now_v7(),
                        instance_id: instance.id,
                        block_id: ss_def.id.clone(),
                        output: output_val,
                        output_ref: None,
                        output_size: 0,
                        attempt: 0,
                        created_at: chrono::Utc::now(),
                    };
                    storage.save_block_output(&block_output).await?;
                    complete_node(storage.as_ref(), node.id).await?;
                    Ok(true)
                } else if child.state.is_terminal() {
                    // Child failed or cancelled.
                    fail_node(storage.as_ref(), node.id).await?;
                    Ok(true)
                } else {
                    // Still running — wait.
                    storage
                        .update_node_state(node.id, NodeState::Waiting)
                        .await?;
                    Ok(true)
                }
            } else {
                // Create the child instance.
                let child_seq = storage
                    .get_sequence_by_name(
                        &instance.tenant_id,
                        &instance.namespace,
                        &ss_def.sequence_name,
                        ss_def.version,
                    )
                    .await?
                    .ok_or_else(|| EngineError::StepFailed {
                        instance_id: instance.id,
                        block_id: ss_def.id.clone(),
                        message: format!("sub-sequence '{}' not found", ss_def.sequence_name),
                        retryable: false,
                    })?;

                let now = chrono::Utc::now();
                let child_context = orch8_types::context::ExecutionContext {
                    data: ss_def.input.clone(),
                    ..Default::default()
                };

                let child = orch8_types::instance::TaskInstance {
                    id: orch8_types::ids::InstanceId::new(),
                    sequence_id: child_seq.id,
                    tenant_id: instance.tenant_id.clone(),
                    namespace: instance.namespace.clone(),
                    state: orch8_types::instance::InstanceState::Scheduled,
                    next_fire_at: Some(now),
                    priority: instance.priority,
                    timezone: instance.timezone.clone(),
                    metadata: serde_json::json!({ "_parent_block_id": ss_def.id.0 }),
                    context: child_context,
                    concurrency_key: None,
                    max_concurrency: None,
                    idempotency_key: None,
                    session_id: instance.session_id,
                    parent_instance_id: Some(instance.id),
                    created_at: now,
                    updated_at: now,
                };
                storage.create_instance(&child).await?;
                storage
                    .update_node_state(node.id, NodeState::Waiting)
                    .await?;
                Ok(true) // Re-schedule to check child status later
            }
        }
    }
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
#[allow(clippy::needless_lifetimes)]
pub fn children_of<'a>(
    tree: &'a [ExecutionNode],
    parent_id: ExecutionNodeId,
    branch_index: Option<i16>,
) -> Vec<&'a ExecutionNode> {
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

/// Activate all `Pending` children by flipping them to `Running`.
///
/// Every composite handler (`race`, `router`, `try_catch`, `parallel`, `loop`,
/// `foreach`, `cancellation_scope`) needs the same "wake up pending descendants so the
/// scheduler can dispatch them" step. Centralizing it here keeps the
/// transition atomic and the tracing consistent, and lets handlers focus on
/// their own branching/completion logic.
///
/// Silently skips any child not in `Pending`; caller decides whether to check
/// terminal state beforehand.
pub async fn activate_pending_children(
    storage: &dyn StorageBackend,
    children: &[&ExecutionNode],
) -> Result<(), EngineError> {
    for child in children {
        if child.state == NodeState::Pending {
            storage
                .update_node_state(child.id, NodeState::Running)
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::sequence::StepDef;

    #[test]
    fn find_block_in_flat_list() {
        let blocks = vec![BlockDefinition::Step(StepDef {
            id: BlockId("step_1".into()),
            handler: "test".into(),
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
        })];

        let found = find_block(&blocks, &BlockId("step_1".into()));
        assert!(found.is_some());

        let not_found = find_block(&blocks, &BlockId("step_999".into()));
        assert!(not_found.is_none());
    }

    #[test]
    fn find_block_nested_in_parallel() {
        let blocks = vec![BlockDefinition::Parallel(
            orch8_types::sequence::ParallelDef {
                id: BlockId("par_1".into()),
                branches: vec![vec![BlockDefinition::Step(StepDef {
                    id: BlockId("nested_step".into()),
                    handler: "test".into(),
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
                })]],
            },
        )];

        let found = find_block(&blocks, &BlockId("nested_step".into()));
        assert!(found.is_some());
    }

    #[test]
    fn block_meta_returns_correct_types() {
        let step = BlockDefinition::Step(StepDef {
            id: BlockId("s".into()),
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
        });
        let (id, bt) = block_meta(&step);
        assert_eq!(id.0, "s");
        assert_eq!(bt, BlockType::Step);
    }

    fn mk_step(id: &str) -> BlockDefinition {
        BlockDefinition::Step(StepDef {
            id: BlockId(id.into()),
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
        })
    }

    fn mk_node(
        id: ExecutionNodeId,
        parent: Option<ExecutionNodeId>,
        block_id: &str,
        bt: BlockType,
        state: NodeState,
        branch_index: Option<i16>,
    ) -> ExecutionNode {
        ExecutionNode {
            id,
            instance_id: orch8_types::ids::InstanceId::new(),
            parent_id: parent,
            block_id: BlockId(block_id.into()),
            block_type: bt,
            branch_index,
            state,
            started_at: None,
            completed_at: None,
        }
    }

    #[test]
    fn find_block_nested_in_loop_body() {
        use orch8_types::sequence::LoopDef;
        let loop_block = BlockDefinition::Loop(LoopDef {
            id: BlockId("loop".into()),
            condition: "true".into(),
            body: vec![mk_step("inner")],
            max_iterations: 5,
        });
        assert!(find_block(std::slice::from_ref(&loop_block), &BlockId("inner".into())).is_some());
        assert!(find_block(&[loop_block], &BlockId("loop".into())).is_some());
    }

    #[test]
    fn find_block_nested_in_for_each_and_router() {
        use orch8_types::sequence::{ForEachDef, Route, RouterDef};
        let fe = BlockDefinition::ForEach(ForEachDef {
            id: BlockId("fe".into()),
            collection: "xs".into(),
            item_var: "item".into(),
            body: vec![mk_step("fe-child")],
            max_iterations: 5,
        });
        let router = BlockDefinition::Router(RouterDef {
            id: BlockId("r".into()),
            routes: vec![Route {
                condition: "true".into(),
                blocks: vec![mk_step("route-child")],
            }],
            default: Some(vec![mk_step("default-child")]),
        });
        assert!(find_block(std::slice::from_ref(&fe), &BlockId("fe-child".into())).is_some());
        assert!(find_block(
            std::slice::from_ref(&router),
            &BlockId("route-child".into())
        )
        .is_some());
        assert!(find_block(&[router], &BlockId("default-child".into())).is_some());
    }

    #[test]
    fn find_block_nested_in_try_catch_finally() {
        use orch8_types::sequence::TryCatchDef;
        let tc = BlockDefinition::TryCatch(TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![mk_step("t")],
            catch_block: vec![mk_step("c")],
            finally_block: Some(vec![mk_step("f")]),
        });
        assert!(find_block(std::slice::from_ref(&tc), &BlockId("t".into())).is_some());
        assert!(find_block(std::slice::from_ref(&tc), &BlockId("c".into())).is_some());
        assert!(find_block(&[tc], &BlockId("f".into())).is_some());
    }

    #[test]
    fn find_block_nested_in_ab_split_and_cancellation_scope() {
        use orch8_types::sequence::{ABSplitDef, ABVariant, CancellationScopeDef};
        let ab = BlockDefinition::ABSplit(ABSplitDef {
            id: BlockId("ab".into()),
            variants: vec![ABVariant {
                name: "control".into(),
                weight: 50,
                blocks: vec![mk_step("ab-inner")],
            }],
        });
        let cs = BlockDefinition::CancellationScope(CancellationScopeDef {
            id: BlockId("cs".into()),
            blocks: vec![mk_step("cs-inner")],
        });
        assert!(find_block(&[ab], &BlockId("ab-inner".into())).is_some());
        assert!(find_block(&[cs], &BlockId("cs-inner".into())).is_some());
    }

    #[test]
    fn find_block_sub_sequence_only_finds_root_not_internals() {
        // SubSequence's child sequence lives in another definition; find_block
        // inside the *parent* definition must not descend into it.
        use orch8_types::sequence::SubSequenceDef;
        let sub = BlockDefinition::SubSequence(SubSequenceDef {
            id: BlockId("sub".into()),
            sequence_name: "child".into(),
            version: None,
            input: serde_json::Value::Null,
        });
        assert!(find_block(std::slice::from_ref(&sub), &BlockId("sub".into())).is_some());
        assert!(find_block(&[sub], &BlockId("child-step".into())).is_none());
    }

    #[test]
    fn find_block_returns_none_for_unknown_id() {
        let blocks = vec![mk_step("a"), mk_step("b")];
        assert!(find_block(&blocks, &BlockId("not-there".into())).is_none());
    }

    #[test]
    fn all_terminal_true_for_mixed_terminal_states() {
        let a = mk_node(
            ExecutionNodeId::new(),
            None,
            "a",
            BlockType::Step,
            NodeState::Completed,
            None,
        );
        let b = mk_node(
            ExecutionNodeId::new(),
            None,
            "b",
            BlockType::Step,
            NodeState::Failed,
            None,
        );
        let c = mk_node(
            ExecutionNodeId::new(),
            None,
            "c",
            BlockType::Step,
            NodeState::Skipped,
            None,
        );
        let d = mk_node(
            ExecutionNodeId::new(),
            None,
            "d",
            BlockType::Step,
            NodeState::Cancelled,
            None,
        );
        let refs = vec![&a, &b, &c, &d];
        assert!(all_terminal(&refs));
    }

    #[test]
    fn all_terminal_false_when_any_running_or_waiting() {
        let a = mk_node(
            ExecutionNodeId::new(),
            None,
            "a",
            BlockType::Step,
            NodeState::Completed,
            None,
        );
        let b = mk_node(
            ExecutionNodeId::new(),
            None,
            "b",
            BlockType::Step,
            NodeState::Running,
            None,
        );
        assert!(!all_terminal(&[&a, &b]));
        let c = mk_node(
            ExecutionNodeId::new(),
            None,
            "c",
            BlockType::Step,
            NodeState::Waiting,
            None,
        );
        assert!(!all_terminal(&[&a, &c]));
        let d = mk_node(
            ExecutionNodeId::new(),
            None,
            "d",
            BlockType::Step,
            NodeState::Pending,
            None,
        );
        assert!(!all_terminal(&[&a, &d]));
    }

    #[test]
    fn any_completed_and_all_completed_contract() {
        let a = mk_node(
            ExecutionNodeId::new(),
            None,
            "a",
            BlockType::Step,
            NodeState::Completed,
            None,
        );
        let b = mk_node(
            ExecutionNodeId::new(),
            None,
            "b",
            BlockType::Step,
            NodeState::Failed,
            None,
        );
        assert!(any_completed(&[&a, &b]));
        assert!(!all_completed(&[&a, &b]));
        assert!(all_completed(&[&a]));
        assert!(!any_completed(&[&b]));
        // empty slice — all_completed is vacuously true; any_completed is false.
        assert!(all_completed(&[]));
        assert!(!any_completed(&[]));
    }

    #[test]
    fn any_failed_and_has_waiting_nodes() {
        let ok = mk_node(
            ExecutionNodeId::new(),
            None,
            "ok",
            BlockType::Step,
            NodeState::Completed,
            None,
        );
        let bad = mk_node(
            ExecutionNodeId::new(),
            None,
            "bad",
            BlockType::Step,
            NodeState::Failed,
            None,
        );
        let wait = mk_node(
            ExecutionNodeId::new(),
            None,
            "w",
            BlockType::Step,
            NodeState::Waiting,
            None,
        );
        assert!(any_failed(&[&ok, &bad]));
        assert!(!any_failed(&[&ok]));
        let tree = vec![ok.clone(), wait.clone()];
        assert!(has_waiting_nodes(&tree));
        assert!(!has_waiting_nodes(&[ok]));
    }

    #[test]
    fn children_of_filters_by_parent_and_branch() {
        let parent_id = ExecutionNodeId::new();
        let c1 = mk_node(
            ExecutionNodeId::new(),
            Some(parent_id),
            "c1",
            BlockType::Step,
            NodeState::Pending,
            Some(0),
        );
        let c2 = mk_node(
            ExecutionNodeId::new(),
            Some(parent_id),
            "c2",
            BlockType::Step,
            NodeState::Pending,
            Some(1),
        );
        let unrelated = mk_node(
            ExecutionNodeId::new(),
            Some(ExecutionNodeId::new()),
            "u",
            BlockType::Step,
            NodeState::Pending,
            None,
        );
        let tree = vec![c1.clone(), c2.clone(), unrelated.clone()];
        assert_eq!(children_of(&tree, parent_id, None).len(), 2);
        assert_eq!(children_of(&tree, parent_id, Some(0)).len(), 1);
        assert_eq!(children_of(&tree, parent_id, Some(1)).len(), 1);
        assert_eq!(children_of(&tree, parent_id, Some(2)).len(), 0);
    }

    #[test]
    fn children_of_returns_empty_for_unknown_parent() {
        let tree: Vec<ExecutionNode> = vec![];
        assert!(children_of(&tree, ExecutionNodeId::new(), None).is_empty());
    }

    #[test]
    fn count_ancestors_measures_depth() {
        let root_id = ExecutionNodeId::new();
        let mid_id = ExecutionNodeId::new();
        let leaf_id = ExecutionNodeId::new();
        let root = mk_node(
            root_id,
            None,
            "r",
            BlockType::Parallel,
            NodeState::Running,
            None,
        );
        let mid = mk_node(
            mid_id,
            Some(root_id),
            "m",
            BlockType::Parallel,
            NodeState::Running,
            None,
        );
        let leaf = mk_node(
            leaf_id,
            Some(mid_id),
            "l",
            BlockType::Step,
            NodeState::Running,
            None,
        );
        let tree = vec![root, mid, leaf];
        assert_eq!(count_ancestors(&tree, root_id), 0);
        assert_eq!(count_ancestors(&tree, mid_id), 1);
        assert_eq!(count_ancestors(&tree, leaf_id), 2);
        // Unknown id returns 0 (no parent found).
        assert_eq!(count_ancestors(&tree, ExecutionNodeId::new()), 0);
    }

    #[test]
    fn block_meta_recognizes_each_variant() {
        use orch8_types::sequence::{
            ABSplitDef, ABVariant, CancellationScopeDef, ForEachDef, LoopDef, ParallelDef, RaceDef,
            RaceSemantics, RouterDef, SubSequenceDef, TryCatchDef,
        };
        let par = BlockDefinition::Parallel(ParallelDef {
            id: BlockId("p".into()),
            branches: vec![],
        });
        let race = BlockDefinition::Race(RaceDef {
            id: BlockId("race".into()),
            branches: vec![],
            semantics: RaceSemantics::default(),
        });
        let lp = BlockDefinition::Loop(LoopDef {
            id: BlockId("lp".into()),
            condition: "false".into(),
            body: vec![],
            max_iterations: 1,
        });
        let fe = BlockDefinition::ForEach(ForEachDef {
            id: BlockId("fe".into()),
            collection: "x".into(),
            item_var: "i".into(),
            body: vec![],
            max_iterations: 1,
        });
        let router = BlockDefinition::Router(RouterDef {
            id: BlockId("r".into()),
            routes: vec![],
            default: None,
        });
        let tc = BlockDefinition::TryCatch(TryCatchDef {
            id: BlockId("tc".into()),
            try_block: vec![],
            catch_block: vec![],
            finally_block: None,
        });
        let sub = BlockDefinition::SubSequence(SubSequenceDef {
            id: BlockId("sub".into()),
            sequence_name: "s".into(),
            version: None,
            input: serde_json::Value::Null,
        });
        let ab = BlockDefinition::ABSplit(ABSplitDef {
            id: BlockId("ab".into()),
            variants: vec![ABVariant {
                name: "a".into(),
                weight: 1,
                blocks: vec![],
            }],
        });
        let cs = BlockDefinition::CancellationScope(CancellationScopeDef {
            id: BlockId("cs".into()),
            blocks: vec![],
        });
        assert_eq!(block_meta(&par).1, BlockType::Parallel);
        assert_eq!(block_meta(&race).1, BlockType::Race);
        assert_eq!(block_meta(&lp).1, BlockType::Loop);
        assert_eq!(block_meta(&fe).1, BlockType::ForEach);
        assert_eq!(block_meta(&router).1, BlockType::Router);
        assert_eq!(block_meta(&tc).1, BlockType::TryCatch);
        assert_eq!(block_meta(&sub).1, BlockType::SubSequence);
        assert_eq!(block_meta(&ab).1, BlockType::ABSplit);
        assert_eq!(block_meta(&cs).1, BlockType::CancellationScope);
    }

    #[test]
    fn find_block_nested_in_race_branches() {
        use orch8_types::sequence::{RaceDef, RaceSemantics};
        let race = BlockDefinition::Race(RaceDef {
            id: BlockId("race".into()),
            branches: vec![vec![mk_step("fast")], vec![mk_step("slow")]],
            semantics: RaceSemantics::default(),
        });
        assert!(find_block(std::slice::from_ref(&race), &BlockId("fast".into())).is_some());
        assert!(find_block(&[race], &BlockId("slow".into())).is_some());
    }

    #[test]
    fn find_running_step_prefers_step_over_composite() {
        use orch8_types::sequence::ParallelDef;
        let par_node_id = ExecutionNodeId::new();
        let step_node_id = ExecutionNodeId::new();
        let par = BlockDefinition::Parallel(ParallelDef {
            id: BlockId("p".into()),
            branches: vec![vec![mk_step("s")]],
        });
        let blocks = vec![par];
        let tree = vec![
            mk_node(
                par_node_id,
                None,
                "p",
                BlockType::Parallel,
                NodeState::Running,
                None,
            ),
            mk_node(
                step_node_id,
                Some(par_node_id),
                "s",
                BlockType::Step,
                NodeState::Running,
                None,
            ),
        ];
        let handlers = crate::handlers::HandlerRegistry::new();
        let (found_node, found_block) =
            find_running_step(&tree, &blocks, &handlers).expect("should find the running step");
        assert_eq!(found_node.block_id.0, "s");
        assert!(matches!(found_block, BlockDefinition::Step(_)));
    }
}
