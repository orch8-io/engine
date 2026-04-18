use tracing::{debug, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId};
use orch8_types::instance::TaskInstance;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::ForEachDef;

use crate::error::EngineError;
use crate::evaluator;
use crate::handlers::HandlerRegistry;

/// Absolute upper bound on `for_each` iterations.
///
/// Mirrors [`crate::handlers::loop_block::LOOP_ABSOLUTE_MAX`]: a last-resort
/// safety net that protects the scheduler from a misconfigured sequence or
/// a regression in iteration tracking. The handler also caps iterations at
/// the collection length, so this only trips if the collection is
/// absurdly large — see `docs/plans/workflow-validation-infinite-loops.md`
/// for the plan to reject such workflows at submit time.
pub const FOR_EACH_ABSOLUTE_MAX: u32 = 1_000_000;

/// Execute a `for_each` block: iterate over each element of `collection`,
/// binding it to `item_var` in the instance context and running the body
/// once per element, up to the lesser of `items.len()`, the user's
/// `max_iterations` cap, and [`FOR_EACH_ABSOLUTE_MAX`].
///
/// Iteration bookkeeping is persisted as a `BlockOutput` keyed by the
/// `for_each`'s own `block_id`, carrying `{ "_index": N, "_total": L,
/// "_item_var": name }`. Under the write-append storage model (see
/// migration 027) this produces one marker row per completed iteration, so
/// the history of iterations is observable via `get_all_outputs`.
///
/// On each tick the handler:
///   1. Resolves the collection from `context.data`. Missing or non-array
///      → complete. Empty → complete.
///   2. Reads the current iteration index from its marker output.
///   3. Trips the cap (completes the node) if the counter has reached the
///      effective max.
///   4. If body children are `Pending`, binds `item_var = items[index]` in
///      the instance context and activates the children to `Running`.
///   5. If every body child is terminal, either fails the `for_each` (on any
///      child failure) or increments the index, persists the marker, and
///      resets the body subtree to `Pending` so the next tick re-executes
///      it against the next element.
///
/// Returns `Ok(true)` to indicate more work; the scheduler will re-dispatch.
#[allow(clippy::too_many_lines)]
pub async fn execute_for_each(
    storage: &dyn StorageBackend,
    _handlers: &HandlerRegistry,
    instance: &TaskInstance,
    node: &ExecutionNode,
    fe_def: &ForEachDef,
    tree: &[ExecutionNode],
) -> Result<bool, EngineError> {
    // Empty body: nothing to run.
    if fe_def.body.is_empty() {
        warn!(
            instance_id = %instance.id,
            block_id = %fe_def.id,
            "for_each has empty body; completing"
        );
        evaluator::complete_node(storage, node.id).await?;
        return Ok(true);
    }

    // ------------------------------------------------------------------
    // Collection snapshot semantics (fix for A5a).
    //
    // The collection is snapshotted on FIRST visit and cached in the
    // handler's own marker output. Subsequent ticks read the snapshot
    // from the marker and never re-resolve the collection from the live
    // instance context. This makes the iteration stable under concurrent
    // context mutations (from emit_event, self_modify, nested handlers,
    // etc.) — without which `_total` and `items[index]` drifted mid-loop.
    //
    // Snapshot marker schema:
    //   { "_index": N, "_total": L, "_item_var": name, "_items": [...] }
    // ------------------------------------------------------------------

    let prior_marker = storage.get_block_output(instance.id, &fe_def.id).await?;

    let (snapshot_items, index): (Vec<serde_json::Value>, u32) = match &prior_marker {
        Some(m) => {
            // Subsequent tick — use the previously snapshotted items.
            let items_snap = m
                .output
                .get("_items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let idx = m
                .output
                .get("_index")
                .and_then(serde_json::Value::as_u64)
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(0);
            (items_snap, idx)
        }
        None => {
            // First visit — resolve live and persist a snapshot marker so
            // subsequent ticks see a stable collection.
            let Some(items) = resolve_collection(&fe_def.collection, &instance.context) else {
                warn!(
                    instance_id = %instance.id,
                    block_id = %fe_def.id,
                    collection = %fe_def.collection,
                    "for_each collection not found or not an array; completing"
                );
                evaluator::complete_node(storage, node.id).await?;
                return Ok(true);
            };
            if items.is_empty() {
                evaluator::complete_node(storage, node.id).await?;
                return Ok(true);
            }
            // Persist the initial snapshot so later ticks stay consistent
            // with this exact collection, regardless of context mutation.
            let init_marker = BlockOutput {
                id: uuid::Uuid::new_v4(),
                instance_id: instance.id,
                block_id: fe_def.id.clone(),
                output: serde_json::json!({
                    "_index": 0,
                    "_total": items.len(),
                    "_item_var": fe_def.item_var,
                    "_items": items,
                }),
                output_ref: None,
                output_size: 0,
                attempt: 0,
                created_at: chrono::Utc::now(),
            };
            storage.save_block_output(&init_marker).await?;
            (items, 0)
        }
    };

    // From here on, all iteration math uses the snapshot.
    let snapshot_len = u32::try_from(snapshot_items.len()).unwrap_or(u32::MAX);
    let user_max = fe_def.max_iterations.min(FOR_EACH_ABSOLUTE_MAX);
    let effective_max = snapshot_len.min(user_max);

    // Hard cap: completed.
    if index >= effective_max {
        evaluator::complete_node(storage, node.id).await?;
        debug!(
            instance_id = %instance.id,
            block_id = %fe_def.id,
            total = snapshot_items.len(),
            "for_each completed"
        );
        return Ok(true);
    }

    let children = evaluator::children_of(tree, node.id, None);

    // Start-of-iteration: when the body children are still Pending, bind
    // `item_var = items[index]` and activate them. We gate on "any child
    // Pending" so the bind happens exactly once per iteration — subsequent
    // ticks while the body is running observe no Pending children and skip.
    let has_pending_child = children.iter().any(|c| c.state == NodeState::Pending);
    if has_pending_child {
        if let Some(item) = snapshot_items.get(index as usize) {
            bind_item_var(storage, instance, &fe_def.item_var, item.clone()).await?;
        }
        for child in &children {
            if child.state == NodeState::Pending {
                storage
                    .update_node_state(child.id, NodeState::Running)
                    .await?;
            }
        }
    }

    // End-of-iteration: if every body child is terminal, either fail (on
    // any child failure) or advance the index, save the marker, and reset
    // the body subtree so the next tick starts the next iteration.
    if !children.is_empty() && evaluator::all_terminal(&children) {
        if evaluator::any_failed(&children) {
            evaluator::fail_node(storage, node.id).await?;
            return Ok(true);
        }

        let next_index = index.saturating_add(1);
        let marker = BlockOutput {
            id: uuid::Uuid::new_v4(),
            instance_id: instance.id,
            block_id: fe_def.id.clone(),
            output: serde_json::json!({
                "_index": next_index,
                "_total": snapshot_items.len(),
                "_item_var": fe_def.item_var,
                "_items": snapshot_items,
            }),
            output_ref: None,
            output_size: 0,
            attempt: i16::try_from(next_index).unwrap_or(i16::MAX),
            created_at: chrono::Utc::now(),
        };
        storage.save_block_output(&marker).await?;

        debug!(
            instance_id = %instance.id,
            block_id = %fe_def.id,
            index = next_index,
            total = snapshot_items.len(),
            "for_each iteration completed"
        );

        // Cap reached after this iteration: leave body terminal. The
        // top-of-function guard will complete the node on the next tick.
        if next_index >= effective_max {
            return Ok(true);
        }

        // Reset the body subtree so the next tick re-runs it against
        // `items[next_index]`. The bind+activate happens on that tick.
        reset_subtree_to_pending(storage, tree, instance.id, node.id).await?;
    }

    Ok(true)
}

/// Merge `{ item_var: item }` into `instance.context.data` and persist.
///
/// If `context.data` is not already a JSON object the handler normalises it
/// to one — losing any non-object root value, which is the correct
/// behaviour here because a non-object root can never hold a variable
/// binding and the body would otherwise fail to read `item_var` via the
/// usual dotted-path lookup.
async fn bind_item_var(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    item_var: &str,
    item: serde_json::Value,
) -> Result<(), EngineError> {
    let mut new_ctx = instance.context.clone();
    match new_ctx.data {
        serde_json::Value::Object(ref mut obj) => {
            obj.insert(item_var.to_string(), item);
        }
        _ => {
            let mut obj = serde_json::Map::new();
            obj.insert(item_var.to_string(), item);
            new_ctx.data = serde_json::Value::Object(obj);
        }
    }
    storage
        .update_instance_context(instance.id, &new_ctx)
        .await?;
    Ok(())
}

/// Walk the subtree rooted at `root_id` (exclusive) and transition every
/// descendant back to [`NodeState::Pending`]. Additionally purge composite
/// iteration-counter markers from descendant `Loop` / `ForEach` blocks via
/// [`StorageBackend::delete_block_outputs`].
///
/// Mirror of [`crate::handlers::loop_block::reset_subtree_to_pending`].
/// Called at iteration boundaries so the body can re-execute without stale
/// terminal state. Step body outputs are left in place — under the
/// write-append model each iteration appends a fresh row for each step.
/// Composite markers are internal iteration state and MUST be purged here
/// or the descendant's cap guard would observe the previous iteration's
/// counter on the next tick and complete without running.
///
/// The root node's own marker is intentionally NOT purged: the caller is
/// the composite that owns it, and it has already advanced its own
/// counter for the next outer iteration.
async fn reset_subtree_to_pending(
    storage: &dyn StorageBackend,
    tree: &[ExecutionNode],
    instance_id: InstanceId,
    root_id: ExecutionNodeId,
) -> Result<(), EngineError> {
    let mut frontier: Vec<ExecutionNodeId> = vec![root_id];
    let mut descendants: Vec<(ExecutionNodeId, BlockType, BlockId)> = Vec::new();
    while let Some(parent) = frontier.pop() {
        for node in tree.iter().filter(|n| n.parent_id == Some(parent)) {
            descendants.push((node.id, node.block_type, node.block_id.clone()));
            frontier.push(node.id);
        }
    }
    for (id, _, _) in &descendants {
        storage.update_node_state(*id, NodeState::Pending).await?;
    }
    for (_, block_type, block_id) in &descendants {
        if matches!(block_type, BlockType::Loop | BlockType::ForEach) {
            storage.delete_block_outputs(instance_id, block_id).await?;
        }
    }
    Ok(())
}

fn resolve_collection(
    path: &str,
    context: &orch8_types::context::ExecutionContext,
) -> Option<Vec<serde_json::Value>> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = &context.data;
    for part in &parts {
        current = current.get(part)?;
    }
    current.as_array().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    #[test]
    fn resolve_collection_from_context() {
        let ctx = ExecutionContext {
            data: json!({"users": [1, 2, 3]}),
            ..Default::default()
        };
        let items = resolve_collection("users", &ctx);
        assert_eq!(items, Some(vec![json!(1), json!(2), json!(3)]));
    }

    #[test]
    fn resolve_nested_collection() {
        let ctx = ExecutionContext {
            data: json!({"data": {"items": ["a", "b"]}}),
            ..Default::default()
        };
        let items = resolve_collection("data.items", &ctx);
        assert_eq!(items, Some(vec![json!("a"), json!("b")]));
    }

    #[test]
    fn resolve_missing_returns_none() {
        let ctx = ExecutionContext::default();
        assert!(resolve_collection("missing", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_returns_none_when_value_is_not_array() {
        let ctx = ExecutionContext {
            data: json!({"users": "not-an-array"}),
            ..Default::default()
        };
        assert!(resolve_collection("users", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_returns_empty_vec_for_empty_array() {
        let ctx = ExecutionContext {
            data: json!({"xs": []}),
            ..Default::default()
        };
        let v = resolve_collection("xs", &ctx).expect("empty array must still resolve");
        assert!(v.is_empty());
    }

    #[test]
    fn resolve_collection_stops_descending_on_non_object() {
        let ctx = ExecutionContext {
            data: json!({"a": 42}),
            ..Default::default()
        };
        assert!(resolve_collection("a.b.c", &ctx).is_none());
    }

    #[test]
    fn resolve_collection_deep_nested_array() {
        let ctx = ExecutionContext {
            data: json!({"l1": {"l2": {"l3": [10, 20]}}}),
            ..Default::default()
        };
        let v = resolve_collection("l1.l2.l3", &ctx).unwrap();
        assert_eq!(v, vec![json!(10), json!(20)]);
    }

    #[test]
    fn absolute_max_is_one_million() {
        assert_eq!(FOR_EACH_ABSOLUTE_MAX, 1_000_000);
    }

    // ----- subtree-reset purge tests (mirrors loop_block::tests L1..L6) -----

    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::RuntimeContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority};

    fn mk_node(
        instance_id: InstanceId,
        block_id: &str,
        block_type: BlockType,
        parent_id: Option<ExecutionNodeId>,
    ) -> ExecutionNode {
        ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            block_id: BlockId(block_id.into()),
            parent_id,
            block_type,
            branch_index: None,
            state: NodeState::Completed,
            started_at: None,
            completed_at: None,
        }
    }

    fn mk_marker(inst: InstanceId, block: &str, value: i64) -> BlockOutput {
        BlockOutput {
            id: uuid::Uuid::new_v4(),
            instance_id: inst,
            block_id: BlockId(block.into()),
            output: json!({ "_iterations": value }),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        }
    }

    async fn seed_instance(s: &SqliteStorage, inst_id: InstanceId, ctx: serde_json::Value) {
        use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
        let now = chrono::Utc::now();
        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "test".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(StepDef {
                id: BlockId("noop".into()),
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
            })],
            interceptors: None,
            created_at: now,
        };
        s.create_sequence(&seq).await.unwrap();
        let inst_row = TaskInstance {
            id: inst_id,
            sequence_id: seq.id,
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: ctx,
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        s.create_instance(&inst_row).await.unwrap();
    }

    fn mk_instance_for(inst_id: InstanceId, ctx: serde_json::Value) -> TaskInstance {
        let now = chrono::Utc::now();
        TaskInstance {
            id: inst_id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: ctx,
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn fe_reset_subtree_purges_descendant_loop_markers() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst, json!({})).await;

        let outer = mk_node(inst, "outer_fe", BlockType::ForEach, None);
        let inner = mk_node(inst, "inner_loop", BlockType::Loop, Some(outer.id));
        let tree = vec![outer.clone(), inner.clone()];

        s.save_block_output(&mk_marker(inst, "inner_loop", 2))
            .await
            .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();
        assert!(s
            .get_block_output(inst, &inner.block_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn fe_reset_subtree_preserves_step_outputs() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst = InstanceId::new();
        seed_instance(&s, inst, json!({})).await;

        let outer = mk_node(inst, "outer_fe", BlockType::ForEach, None);
        let step = mk_node(inst, "body_step", BlockType::Step, Some(outer.id));
        let tree = vec![outer.clone(), step.clone()];

        s.save_block_output(&BlockOutput {
            id: uuid::Uuid::new_v4(),
            instance_id: inst,
            block_id: step.block_id.clone(),
            output: json!({"result": "ok"}),
            output_ref: None,
            output_size: 0,
            attempt: 0,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

        reset_subtree_to_pending(&s, &tree, inst, outer.id)
            .await
            .unwrap();

        let got = s
            .get_block_output(inst, &step.block_id)
            .await
            .unwrap()
            .expect("step output preserved");
        assert_eq!(got.output["result"], "ok");
    }

    // H2: for_each item bind uses fresh index after reset.
    #[tokio::test]
    async fn h2_for_each_item_bind_uses_fresh_index_after_reset() {
        let s = SqliteStorage::in_memory().await.unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&s, inst_id, json!({"items": ["a", "b", "c", "d"]})).await;
        let inst = mk_instance_for(inst_id, json!({"items": ["a", "b", "c", "d"]}));

        let mut fe = mk_node(inst_id, "fe", BlockType::ForEach, None);
        fe.state = NodeState::Running;
        let mut step = mk_node(inst_id, "body", BlockType::Step, Some(fe.id));
        step.state = NodeState::Pending;
        s.create_execution_nodes_batch(&[fe.clone(), step.clone()])
            .await
            .unwrap();

        // Stale prior-run marker (_index=3 means it had iterated 3 times).
        s.save_block_output(&BlockOutput {
            id: uuid::Uuid::new_v4(),
            instance_id: inst_id,
            block_id: BlockId("fe".into()),
            output: json!({"_index": 3, "_total": 4, "_item_var": "item"}),
            output_ref: None,
            output_size: 0,
            attempt: 3,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

        // Reset purges fe's marker only as a descendant — but in this test
        // we want H2 to reflect "after reset" semantics, so purge directly.
        s.delete_block_outputs(inst_id, &BlockId("fe".into()))
            .await
            .unwrap();

        let fe_def = ForEachDef {
            id: BlockId("fe".into()),
            collection: "items".into(),
            item_var: "item".into(),
            body: vec![orch8_types::sequence::BlockDefinition::Step(
                orch8_types::sequence::StepDef {
                    id: BlockId("body".into()),
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
                },
            )],
            max_iterations: 4,
        };
        let registry = HandlerRegistry::new();
        let tree = s.get_execution_tree(inst_id).await.unwrap();
        let fe_node = tree.iter().find(|n| n.block_id.0 == "fe").unwrap().clone();

        execute_for_each(&s, &registry, &inst, &fe_node, &fe_def, &tree)
            .await
            .unwrap();

        // After execution, the body should be Running and the bound item
        // should be `items[0]`.
        let after = s.get_execution_tree(inst_id).await.unwrap();
        let body = after.iter().find(|n| n.block_id.0 == "body").unwrap();
        assert_eq!(body.state, NodeState::Running);

        let updated = s.get_instance(inst_id).await.unwrap().unwrap();
        assert_eq!(updated.context.data["item"], json!("a"));
    }
}
