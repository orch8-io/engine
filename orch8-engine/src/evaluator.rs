use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

use crate::error::EngineError;
use crate::handlers::HandlerRegistry;

/// Build the execution tree from a sequence definition if it doesn't exist yet.
/// Returns the root-level nodes for the instance.
pub async fn ensure_execution_tree(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<Vec<ExecutionNode>, EngineError> {
    let existing = storage.get_execution_tree(instance.id).await?;
    if !existing.is_empty() {
        return Ok(existing);
    }

    // Build tree from sequence blocks.
    let mut nodes = Vec::new();
    build_nodes(instance.id, None, &sequence.blocks, &mut nodes);

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
    }
}

/// Evaluate the execution tree: dispatch actionable nodes until no more
/// progress can be made within this tick.
/// Returns `true` if there is more work to do (instance should be re-scheduled).
pub async fn evaluate(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    sequence: &SequenceDefinition,
) -> Result<bool, EngineError> {
    // Ensure the execution tree exists (creates on first call).
    ensure_execution_tree(storage, instance, sequence).await?;

    // Loop: each iteration dispatches one actionable node, then re-reads
    // the tree to find more work. This lets parallel branches, try-catch
    // phases, etc. all make progress within a single scheduler tick.
    let max_iterations = 200;
    for _ in 0..max_iterations {
        // Re-fetch tree to see latest state after each dispatch.
        let tree = storage.get_execution_tree(instance.id).await?;
        let root_nodes: Vec<&ExecutionNode> = tree.iter().filter(|n| n.parent_id.is_none()).collect();

        // Termination: all root nodes done.
        if root_nodes.iter().all(|n| {
            matches!(
                n.state,
                NodeState::Completed | NodeState::Skipped
            )
        }) {
            return Ok(false);
        }
        // Termination: any root node failed/cancelled.
        if root_nodes.iter().any(|n| {
            matches!(
                n.state,
                NodeState::Failed | NodeState::Cancelled
            )
        }) {
            return Ok(false);
        }

        // Phase 1: activate the first Pending root node.
        if let Some(node) = root_nodes.iter().find(|n| n.state == NodeState::Pending) {
            if let Some(block) = find_block(&sequence.blocks, &node.block_id) {
                dispatch_block(storage, handlers, instance, node, block, &tree).await?;
                continue;
            }
        }

        // Phase 2: execute Running step nodes (leaf work first).
        if let Some((node, block)) = find_running_step(&tree, &sequence.blocks) {
            dispatch_block(storage, handlers, instance, &node, block, &tree).await?;
            // Whether the step completed or deferred (external worker), continue
            // the loop. Deferred steps are now in Waiting state and won't be
            // found again by find_running_step. Other sibling steps can proceed.
            continue;
        }

        // Phase 3: re-evaluate Running composite nodes (parents check child completion,
        // activate next phases like catch/finally, dispatch pending children).
        if let Some((node, block)) = find_running_composite(&tree, &sequence.blocks) {
            dispatch_block(storage, handlers, instance, &node, block, &tree).await?;
            continue;
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
    Ok(true)
}

/// Find the first Running composite node (deepest first for proper nesting).
fn find_running_composite<'a>(
    tree: &'a [ExecutionNode],
    blocks: &'a [BlockDefinition],
) -> Option<(ExecutionNode, &'a BlockDefinition)> {
    // Process deepest composites first (children before parents) so inner
    // composites complete before their parents re-evaluate.
    let mut composites: Vec<_> = tree
        .iter()
        .filter(|n| n.state == NodeState::Running)
        .filter_map(|n| {
            find_block(blocks, &n.block_id)
                .filter(|b| !matches!(b, BlockDefinition::Step(_)))
                .map(|b| (n.clone(), b))
        })
        .collect();
    // Sort by tree depth (deeper = more ancestors = processed first).
    composites.sort_by_key(|(n, _)| {
        std::cmp::Reverse(count_ancestors(tree, n.id))
    });
    composites.into_iter().next()
}

/// Find a Running step node that can be executed.
fn find_running_step<'a>(
    tree: &'a [ExecutionNode],
    blocks: &'a [BlockDefinition],
) -> Option<(ExecutionNode, &'a BlockDefinition)> {
    for node in tree {
        if node.state != NodeState::Running {
            continue;
        }
        if let Some(block) = find_block(blocks, &node.block_id) {
            if matches!(block, BlockDefinition::Step(_)) {
                return Some((node.clone(), block));
            }
        }
    }
    None
}

/// Count ancestors to determine tree depth.
fn count_ancestors(tree: &[ExecutionNode], mut node_id: ExecutionNodeId) -> usize {
    let mut depth = 0;
    while let Some(parent_id) = tree.iter().find(|n| n.id == node_id).and_then(|n| n.parent_id) {
        depth += 1;
        node_id = parent_id;
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
        };
        if let Some(children) = children {
            if let Some(found) = find_block(children, target_id) {
                return Some(found);
            }
        }
    }
    None
}

/// Dispatch a single execution node to the appropriate block handler.
/// Returns `true` if the instance has more work to do.
#[allow(clippy::too_many_lines)]
async fn dispatch_block(
    storage: &dyn StorageBackend,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    block: &BlockDefinition,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    // Mark node as running.
    if node.state == NodeState::Pending {
        storage
            .update_node_state(node.id, NodeState::Running)
            .await?;
    }

    match block {
        BlockDefinition::Step(step_def) => {
            crate::handlers::step_block::execute_step_node(
                storage, handlers, instance, node, step_def,
            )
            .await
        }
        BlockDefinition::Parallel(par_def) => {
            crate::handlers::parallel::execute_parallel(
                storage, handlers, instance, node, par_def, tree,
            )
            .await
        }
        BlockDefinition::Race(race_def) => {
            crate::handlers::race::execute_race(storage, handlers, instance, node, race_def, tree)
                .await
        }
        BlockDefinition::Loop(loop_def) => {
            crate::handlers::loop_block::execute_loop(
                storage, handlers, instance, node, loop_def, tree,
            )
            .await
        }
        BlockDefinition::ForEach(fe_def) => {
            crate::handlers::for_each::execute_for_each(
                storage, handlers, instance, node, fe_def, tree,
            )
            .await
        }
        BlockDefinition::Router(router_def) => {
            crate::handlers::router::execute_router(
                storage, handlers, instance, node, router_def, tree,
            )
            .await
        }
        BlockDefinition::TryCatch(tc_def) => {
            crate::handlers::try_catch::execute_try_catch(
                storage, handlers, instance, node, tc_def, tree,
            )
            .await
        }
        BlockDefinition::SubSequence(ss_def) => {
            // Sub-sequence: create a child instance and wait for it to complete.
            // Check if child already exists for this block.
            let children = storage.get_child_instances(instance.id).await?;
            let existing_child = children
                .iter()
                .find(|c| c.metadata.get("_parent_block_id").and_then(|v| v.as_str()) == Some(&ss_def.id.0));

            if let Some(child) = existing_child {
                // Child exists — check if it's done.
                if child.state == orch8_types::instance::InstanceState::Completed {
                    // Save child outputs as this block's output.
                    let child_outputs = storage.get_all_outputs(child.id).await?;
                    let output_val = serde_json::to_value(&child_outputs).unwrap_or_default();
                    let block_output = orch8_types::output::BlockOutput {
                        id: uuid::Uuid::new_v4(),
                        instance_id: instance.id,
                        block_id: ss_def.id.clone(),
                        output: output_val,
                        output_ref: None,
                        output_size: 0,
                        attempt: 0,
                        created_at: chrono::Utc::now(),
                    };
                    storage.save_block_output(&block_output).await?;
                    complete_node(storage, node.id).await?;
                    Ok(true)
                } else if child.state.is_terminal() {
                    // Child failed or cancelled.
                    fail_node(storage, node.id).await?;
                    Ok(true)
                } else {
                    // Still running — wait.
                    storage.update_node_state(node.id, NodeState::Waiting).await?;
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
                storage.update_node_state(node.id, NodeState::Waiting).await?;
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
        });
        let (id, bt) = block_meta(&step);
        assert_eq!(id.0, "s");
        assert_eq!(bt, BlockType::Step);
    }
}
