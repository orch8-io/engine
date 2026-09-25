//! Dispatch a single execution node to the appropriate block handler.

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::instance::TaskInstance;
use orch8_types::sequence::BlockDefinition;

use super::{complete_node, fail_node};
use crate::error::EngineError;
use crate::handlers::HandlerRegistry;
use crate::handlers::param_resolve::OutputsSnapshot;

/// Metadata key recording how many `SubSequence` spawns deep a child instance
/// is from its top-level ancestor. Absent (root instance) is depth 0.
const SUB_SEQUENCE_DEPTH_KEY: &str = "_sub_sequence_depth";

/// Maximum `SubSequence` spawn depth. Without a cap, a definition that spawns
/// itself (directly, or A→B→A) recurses forever, each level creating a new
/// instance -- unbounded storage growth and scheduler load with no operator
/// signal until the database fills up.
const MAX_SUB_SEQUENCE_DEPTH: u64 = 16;

/// Metadata key identifying which activation of the parent's `SubSequence`
/// node spawned a child (see [`sub_sequence_activation_key`]).
const PARENT_ACTIVATION_KEY: &str = "_parent_activation";

/// Identity of one activation of a `SubSequence` node: its execution-node id
/// plus the current iteration of every enclosing `loop` / `for_each`
/// (innermost first). Node ids are stable across iterations — a loop resets
/// its body in place — so the iteration counters are what distinguish
/// iteration N's child from iteration N+1's.
async fn sub_sequence_activation_key(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    node: &ExecutionNode,
    tree: &[ExecutionNode],
) -> Result<String, EngineError> {
    use orch8_types::execution::BlockType;

    let mut key = node.id.to_string();
    let mut parent_id = node.parent_id;
    while let Some(pid) = parent_id {
        let Some(parent) = tree.iter().find(|n| n.id == pid) else {
            break;
        };
        let counter_field = match parent.block_type {
            BlockType::Loop => Some("_iterations"),
            BlockType::ForEach => Some("_index"),
            _ => None,
        };
        if let Some(field) = counter_field {
            let iteration = storage
                .get_block_output(instance.id, &parent.block_id)
                .await?
                .and_then(|o| o.output.get(field).and_then(serde_json::Value::as_u64))
                .unwrap_or(0);
            key.push('/');
            key.push_str(parent.block_id.as_str());
            key.push(':');
            key.push_str(&iteration.to_string());
        }
        parent_id = parent.parent_id;
    }
    Ok(key)
}

/// Find the child spawned by this activation. Children created before
/// activation keys existed carry only `_parent_block_id`; they are matched by
/// block id so in-flight instances keep working across an upgrade.
fn find_activation_child<'c>(
    children: &'c [TaskInstance],
    block_id: &str,
    activation: &str,
) -> Option<&'c TaskInstance> {
    let meta_str = |c: &'c TaskInstance, key: &str| -> Option<&'c str> {
        c.metadata.get(key).and_then(serde_json::Value::as_str)
    };
    children
        .iter()
        .find(|c| meta_str(c, PARENT_ACTIVATION_KEY) == Some(activation))
        .or_else(|| {
            children.iter().find(|c| {
                c.metadata.get(PARENT_ACTIVATION_KEY).is_none()
                    && meta_str(c, "_parent_block_id") == Some(block_id)
            })
        })
}

/// Build a spawned child's [`ExecutionContext`](orch8_types::context::ExecutionContext)
/// from its parent, seeded with the child's `input` and inheriting execution-mode
/// invariants from the parent.
///
/// Invariant: **a dry-run parent must only spawn dry-run children** — otherwise
/// a simulation would launch a real sub-sequence. Extracted into a named,
/// unit-tested function so this guarantee can't be silently dropped by a future
/// `..Default::default()` cleanup.
fn child_context_from(
    parent: &orch8_types::context::ExecutionContext,
    input: serde_json::Value,
) -> orch8_types::context::ExecutionContext {
    let mut ctx = orch8_types::context::ExecutionContext {
        data: input,
        ..Default::default()
    };
    ctx.runtime.dry_run = parent.runtime.dry_run;
    ctx
}

/// Read a `SubSequence` parent's spawn depth from its `metadata` (0 if the
/// key is absent, i.e. this is a root instance) and return the depth a new
/// child of it would have.
fn next_sub_sequence_depth(parent_metadata: &serde_json::Value) -> u64 {
    let parent_depth = parent_metadata
        .get(SUB_SEQUENCE_DEPTH_KEY)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    parent_depth + 1
}

/// Dispatch a single execution node to the appropriate block handler.
/// Returns `true` if the instance has more work to do. Note: all current
/// call sites discard the return value and rely on the error channel only.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(super) async fn dispatch_block(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    block: &BlockDefinition,
    tree: &[ExecutionNode],
    interceptors: Option<&orch8_types::interceptor::InterceptorDef>,
    outputs: &OutputsSnapshot,
    clock: &orch8_types::clock::SharedClock,
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
            let result = crate::handlers::step_block::execute_step_node_with_clock(
                storage, handlers, instance, node, step_def, outputs, clock,
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
            // Bump the per-instance step counter so max_steps_per_instance
            // enforcement (checked at the scheduler level) sees accurate counts.
            // `increment_total_steps` touches only the counter path atomically,
            // so concurrent context mutations made during step execution (e.g.
            // check_human_input's merge_context_data) are not clobbered, and
            // two steps completing in the same tick can't lose an increment.
            if matches!(result, Ok(true))
                && let Err(e) = storage.increment_total_steps(instance.id).await
            {
                tracing::warn!(instance_id = %instance.id, error = %e, "failed to update step counter");
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
            crate::handlers::loop_block::execute_loop_with_clock(
                storage.as_ref(),
                instance,
                node,
                loop_def,
                tree,
                clock,
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
                outputs,
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
                outputs,
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
        BlockDefinition::Saga(saga_def) => {
            crate::handlers::saga::execute_saga(
                storage.as_ref(),
                handlers,
                instance,
                node,
                saga_def,
                tree,
            )
            .await
        }
        BlockDefinition::SubSequence(ss_def) => {
            // Sub-sequence: create a child instance and wait for it to complete.
            // Check if child already exists for this block.
            // Key the child by this *activation* of the node, not just its
            // block id: inside a loop / for_each the same node runs once per
            // iteration and each iteration must spawn (and wait on) its own
            // child rather than reuse iteration 1's completed one.
            let activation =
                sub_sequence_activation_key(storage.as_ref(), instance, node, tree).await?;
            let children = storage.get_child_instances(instance.id).await?;
            let existing_child = find_activation_child(&children, ss_def.id.as_str(), &activation);

            if let Some(child) = existing_child {
                // Child exists — check if it's done.
                if child.state == orch8_types::instance::InstanceState::Completed {
                    // Save child outputs as this block's output.
                    let child_outputs = storage.get_all_outputs(child.id).await?;
                    let output_val = serde_json::to_value(&child_outputs).map_err(|e| {
                        tracing::warn!(
                            instance_id = %instance.id,
                            child_id = %child.id,
                            error = %e,
                            "failed to serialize child outputs"
                        );
                        EngineError::Storage(StorageError::Serialization(e))
                    })?;
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
                } else if child.state.is_terminal() {
                    // Child failed or cancelled.
                    fail_node(storage.as_ref(), node.id).await?;
                } else {
                    // Still running — wait.
                    storage
                        .update_node_state(node.id, NodeState::Waiting)
                        .await?;
                }
            } else {
                // Depth guard: an A→A (or A→B→A) sub-sequence spawn has no
                // other bound, so cap how many levels deep this instance
                // already is before minting another child.
                let child_depth = next_sub_sequence_depth(&instance.metadata);
                if child_depth > MAX_SUB_SEQUENCE_DEPTH {
                    return Err(EngineError::StepFailed {
                        instance_id: instance.id,
                        block_id: ss_def.id.clone(),
                        message: format!(
                            "sub-sequence spawn depth exceeds the maximum of {MAX_SUB_SEQUENCE_DEPTH}"
                        ),
                        retryable: false,
                        details: None,
                    });
                }

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
                        details: None,
                    })?;

                // Resolve `{{…}}` templates in `input` exactly like step params
                // (context + prior outputs), so each activation passes its own
                // values — e.g. the current for_each item.
                let input = match crate::handlers::param_resolve::resolve_templates_in_params(
                    storage.as_ref(),
                    instance,
                    &instance.context,
                    &ss_def.input,
                    outputs,
                )
                .await
                {
                    Ok(input) => input,
                    Err(e @ EngineError::Storage(_)) => return Err(e),
                    Err(e) => {
                        tracing::error!(
                            instance_id = %instance.id,
                            block_id = %ss_def.id,
                            error = %e,
                            "failed to resolve sub-sequence input templates"
                        );
                        fail_node(storage.as_ref(), node.id).await?;
                        return Ok(true);
                    }
                };

                let now = clock.now();
                let child_context = child_context_from(&instance.context, input);

                let child = orch8_types::instance::TaskInstance {
                    id: orch8_types::ids::InstanceId::new(),
                    sequence_id: child_seq.id,
                    tenant_id: instance.tenant_id.clone(),
                    namespace: instance.namespace.clone(),
                    state: orch8_types::instance::InstanceState::Scheduled,
                    next_fire_at: Some(now),
                    priority: instance.priority,
                    timezone: instance.timezone.clone(),
                    metadata: serde_json::json!({
                        "_parent_block_id": ss_def.id.as_str(),
                        PARENT_ACTIVATION_KEY: activation,
                        SUB_SEQUENCE_DEPTH_KEY: child_depth,
                    }),
                    context: child_context,
                    concurrency_key: None,
                    max_concurrency: None,
                    idempotency_key: None,
                    session_id: instance.session_id,
                    parent_instance_id: Some(instance.id),
                    // Propagate the parent's budget so a chain of
                    // sub-sequences can't escape a configured resource cap by
                    // spawning children that each start with `None`.
                    budget: instance.budget.clone(),
                    created_at: now,
                    updated_at: now,
                };
                storage.create_instance(&child).await?;
                storage
                    .update_node_state(node.id, NodeState::Waiting)
                    .await?;
            }
            Ok(true) // Re-schedule to check child status later
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_SUB_SEQUENCE_DEPTH, PARENT_ACTIVATION_KEY, SUB_SEQUENCE_DEPTH_KEY, child_context_from,
        find_activation_child, next_sub_sequence_depth, sub_sequence_activation_key,
    };
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    #[test]
    fn child_inherits_dry_run_from_parent() {
        // A dry-run parent must spawn dry-run children (and vice-versa).
        let mut dry_parent = ExecutionContext::default();
        dry_parent.runtime.dry_run = true;
        let child = child_context_from(&dry_parent, json!({ "x": 1 }));
        assert!(child.runtime.dry_run, "dry-run must propagate to children");
        assert_eq!(child.data, json!({ "x": 1 }));

        let real_parent = ExecutionContext::default();
        let child = child_context_from(&real_parent, json!({}));
        assert!(!child.runtime.dry_run, "a real parent spawns real children");
    }

    /// H-4: a root instance (no depth key in metadata) has depth 0, so its
    /// first `SubSequence` child is depth 1.
    #[test]
    fn root_instance_spawns_depth_one_child() {
        assert_eq!(next_sub_sequence_depth(&json!({})), 1);
    }

    /// H-4: depth accumulates across a chain of `SubSequence` spawns.
    #[test]
    fn depth_increments_across_chain() {
        let mut metadata = json!({});
        for expected in 1..=(MAX_SUB_SEQUENCE_DEPTH + 5) {
            let depth = next_sub_sequence_depth(&metadata);
            assert_eq!(depth, expected);
            metadata = json!({ SUB_SEQUENCE_DEPTH_KEY: depth });
        }
    }

    /// H-4: a self-recursive (or A→B→A) definition must eventually be
    /// stopped by the depth cap rather than spawning children forever.
    #[test]
    fn depth_eventually_exceeds_cap() {
        let deep_metadata = json!({ SUB_SEQUENCE_DEPTH_KEY: MAX_SUB_SEQUENCE_DEPTH });
        let depth = next_sub_sequence_depth(&deep_metadata);
        assert!(
            depth > MAX_SUB_SEQUENCE_DEPTH,
            "depth {depth} should exceed the cap of {MAX_SUB_SEQUENCE_DEPTH}"
        );
    }

    fn instance_with_metadata(metadata: serde_json::Value) -> orch8_types::instance::TaskInstance {
        let now = chrono::Utc::now();
        orch8_types::instance::TaskInstance {
            id: orch8_types::ids::InstanceId::new(),
            sequence_id: orch8_types::ids::SequenceId::new(),
            tenant_id: orch8_types::ids::TenantId::unchecked("t"),
            namespace: orch8_types::ids::Namespace::new("ns"),
            state: orch8_types::instance::InstanceState::Running,
            next_fire_at: None,
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".into(),
            metadata,
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// ENG-C-N4: a `SubSequence` inside a loop gets a distinct activation
    /// key per iteration, so iteration 2 never reuses iteration 1's child.
    #[tokio::test]
    async fn activation_key_changes_per_enclosing_iteration() {
        use orch8_storage::{InstanceStore, OutputStore, sqlite::SqliteStorage};
        use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
        use orch8_types::ids::{BlockId, ExecutionNodeId};

        let s = SqliteStorage::in_memory().await.unwrap();
        let parent = instance_with_metadata(json!({}));
        s.create_instance(&parent).await.unwrap();
        let node = |block: &str, bt, parent_id| ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: parent.id,
            block_id: BlockId::new(block),
            parent_id,
            block_type: bt,
            branch_index: None,
            state: NodeState::Running,
            started_at: None,
            completed_at: None,
        };
        let lp = node("lp", BlockType::Loop, None);
        let ss = node("ss", BlockType::SubSequence, Some(lp.id));
        let tree = vec![lp.clone(), ss.clone()];
        let marker = |n: u64| orch8_types::output::BlockOutput {
            id: uuid::Uuid::now_v7(),
            instance_id: parent.id,
            block_id: BlockId::new("lp"),
            output: json!({ "_iterations": n }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        };

        s.save_block_output(&marker(0)).await.unwrap();
        let k0 = sub_sequence_activation_key(&s, &parent, &ss, &tree)
            .await
            .unwrap();
        s.save_block_output(&marker(1)).await.unwrap();
        let k1 = sub_sequence_activation_key(&s, &parent, &ss, &tree)
            .await
            .unwrap();
        assert_ne!(k0, k1);
        assert!(k0.starts_with(&ss.id.to_string()));

        let child0 = instance_with_metadata(json!({
            "_parent_block_id": "ss",
            PARENT_ACTIVATION_KEY: k0,
        }));
        let children = vec![child0.clone()];
        assert_eq!(
            find_activation_child(&children, "ss", &k0).map(|c| c.id),
            Some(child0.id)
        );
        assert!(
            find_activation_child(&children, "ss", &k1).is_none(),
            "iteration 2 must spawn its own child"
        );

        // Children spawned before activation keys existed still match.
        let legacy = instance_with_metadata(json!({ "_parent_block_id": "ss" }));
        let children = vec![legacy.clone()];
        assert_eq!(
            find_activation_child(&children, "ss", &k1).map(|c| c.id),
            Some(legacy.id)
        );
    }
}
