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
                storage, handlers, instance, node, step_def, outputs,
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
            let children = storage.get_child_instances(instance.id).await?;
            let existing_child = children.iter().find(|c| {
                c.metadata.get("_parent_block_id").and_then(|v| v.as_str())
                    == Some(ss_def.id.as_str())
            });

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

                let now = chrono::Utc::now();
                let child_context = child_context_from(&instance.context, ss_def.input.clone());

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
        MAX_SUB_SEQUENCE_DEPTH, SUB_SEQUENCE_DEPTH_KEY, child_context_from, next_sub_sequence_depth,
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
}
