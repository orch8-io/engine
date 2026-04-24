use chrono::Utc;
use tracing::{debug, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::NodeState;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::{Signal, SignalAction};

use crate::error::EngineError;

/// Process pending signals for an instance before executing blocks.
/// Returns `true` if execution should be aborted (e.g., pause or cancel).
pub async fn process_signals(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    current_state: InstanceState,
) -> Result<bool, EngineError> {
    let signals = storage.get_pending_signals(instance_id).await?;
    process_signals_inner(storage, instance_id, current_state, signals, None).await
}

/// Process pre-fetched signals (from batch query).
/// Returns `true` if execution should be aborted (e.g., pause or cancel).
pub async fn process_signals_prefetched(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    current_state: InstanceState,
    signals: Vec<Signal>,
    sequence_def: Option<&SequenceDefinition>,
) -> Result<bool, EngineError> {
    process_signals_inner(storage, instance_id, current_state, signals, sequence_def).await
}

#[allow(clippy::too_many_lines)]
async fn process_signals_inner(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    current_state: InstanceState,
    signals: Vec<Signal>,
    sequence_def: Option<&SequenceDefinition>,
) -> Result<bool, EngineError> {
    if signals.is_empty() {
        return Ok(false);
    }

    for signal in &signals {
        // Re-read current state before every signal so control signals
        // (pause/resume/cancel) act on storage reality, not the stale
        // snapshot from when the batch was assembled. Non-aborting signals
        // (update_context, custom) see fresh state too, which is harmless.
        let current_state = storage
            .get_instance(instance_id)
            .await?
            .map_or(current_state, |i| i.state);

        info!(
            instance_id = %instance_id,
            signal_type = %signal.signal_type,
            "processing signal"
        );

        // Interceptor: on_signal
        if let Some(seq) = sequence_def {
            if let Some(ref interceptors) = seq.interceptors {
                let signal_info = serde_json::json!({
                    "signal_type": signal.signal_type.to_string(),
                    "payload": signal.payload,
                });
                crate::interceptors::emit_on_signal(
                    storage,
                    interceptors,
                    instance_id,
                    &signal_info,
                )
                .await;
            }
        }

        // Lift `(signal_type, payload)` into a typed variant once. Decode
        // failures are non-fatal (malformed payloads must not poison the
        // whole signal queue), so they mark the signal delivered and move on.
        let action = match signal.action() {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    instance_id = %instance_id,
                    signal_type = %signal.signal_type,
                    error = %e,
                    "malformed signal payload — discarding"
                );
                storage.mark_signal_delivered(signal.id).await?;
                continue;
            }
        };

        match action {
            SignalAction::Pause => {
                if current_state.can_transition_to(InstanceState::Paused) {
                    crate::lifecycle::transition_instance(
                        storage,
                        instance_id,
                        None,
                        current_state,
                        InstanceState::Paused,
                        None,
                    )
                    .await?;
                    storage.mark_signal_delivered(signal.id).await?;
                    return Ok(true);
                }
                warn!(
                    instance_id = %instance_id,
                    current_state = %current_state,
                    "cannot pause instance in current state"
                );
            }
            SignalAction::Resume => {
                if current_state == InstanceState::Paused {
                    crate::lifecycle::transition_instance(
                        storage,
                        instance_id,
                        None,
                        InstanceState::Paused,
                        InstanceState::Scheduled,
                        Some(Utc::now()),
                    )
                    .await?;
                    storage.mark_signal_delivered(signal.id).await?;
                    return Ok(true);
                }
                // If not paused, just mark delivered — already running.
            }
            SignalAction::Cancel => {
                if !current_state.can_transition_to(InstanceState::Cancelled) {
                    warn!(
                        instance_id = %instance_id,
                        current_state = %current_state,
                        "cannot cancel instance in current state"
                    );
                    storage.mark_signal_delivered(signal.id).await?;
                    continue;
                }

                // Scoped cancellation: if we have the sequence definition, cancel only
                // cancellable nodes and let non-cancellable ones finish.
                if let Some(seq) = sequence_def {
                    let has_non_cancellable = cancel_scoped(storage, instance_id, seq).await?;
                    if has_non_cancellable {
                        debug!(
                            instance_id = %instance_id,
                            "cancel signal: non-cancellable nodes still running, deferring full cancel"
                        );
                        storage.mark_signal_delivered(signal.id).await?;
                        // Don't abort — let the evaluator continue running
                        // non-cancellable nodes. The evaluator will check for
                        // all-terminal and complete the cancellation.
                        continue;
                    }
                }

                // No non-cancellable nodes (or no sequence def) — cancel immediately.
                crate::lifecycle::transition_instance(
                    storage,
                    instance_id,
                    None,
                    current_state,
                    InstanceState::Cancelled,
                    None,
                )
                .await?;
                storage.mark_signal_delivered(signal.id).await?;
                return Ok(true);
            }
            SignalAction::UpdateContext(ctx) => {
                // Payload was validated by `Signal::action()` — no runtime
                // decode fallback needed here.
                storage.update_instance_context(instance_id, &ctx).await?;
                info!(instance_id = %instance_id, "context updated via signal");
            }
            SignalAction::Custom { name, .. } => {
                // Human-input signals are consumed by scheduler::check_human_input
                // when the evaluator reaches the wait_for_input step. Leave them
                // pending so check_human_input (which calls get_pending_signals)
                // can see them.
                //
                // If the instance is in Waiting state (tree-based execution),
                // transition it back to Scheduled so the scheduler tick picks
                // it up and runs check_human_input.
                if name.starts_with("human_input:") {
                    debug!(
                        instance_id = %instance_id,
                        signal_name = %name,
                        "custom signal reserved for wait_for_input — deferring delivery"
                    );
                    if current_state == InstanceState::Waiting {
                        // Tree-based execution: set the matching step node back
                        // to Running so the evaluator can re-dispatch it and
                        // check_human_input will find the signal.
                        let block_id = name.strip_prefix("human_input:").unwrap_or(&name);
                        let tree = storage.get_execution_tree(instance_id).await?;
                        for node in &tree {
                            if node.block_id.0 == block_id
                                && node.state == orch8_types::execution::NodeState::Waiting
                            {
                                storage
                                    .update_node_state(
                                        node.id,
                                        orch8_types::execution::NodeState::Running,
                                    )
                                    .await?;
                                break;
                            }
                        }

                        crate::lifecycle::transition_instance(
                            storage,
                            instance_id,
                            None,
                            InstanceState::Waiting,
                            InstanceState::Scheduled,
                            Some(Utc::now()),
                        )
                        .await?;
                        return Ok(true);
                    }
                    continue;
                }
                info!(
                    instance_id = %instance_id,
                    signal_name = %name,
                    "custom signal received (no built-in handler)"
                );
            }
        }

        storage.mark_signal_delivered(signal.id).await?;
    }

    Ok(false)
}

/// Cancel all cancellable nodes and return `true` if any non-cancellable nodes are still active.
async fn cancel_scoped(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    sequence_def: &SequenceDefinition,
) -> Result<bool, EngineError> {
    use orch8_types::execution::BlockType;
    use orch8_types::sequence::BlockDefinition;

    let tree = storage.get_execution_tree(instance_id).await?;
    let mut has_non_cancellable_active = false;

    // Collect IDs of CancellationScope nodes so we can check ancestry.
    let scope_node_ids: std::collections::HashSet<_> = tree
        .iter()
        .filter(|n| n.block_type == BlockType::CancellationScope)
        .map(|n| n.id)
        .collect();

    let block_map = crate::evaluator::flatten_blocks(&sequence_def.blocks);

    for node in &tree {
        let is_active = matches!(
            node.state,
            NodeState::Pending | NodeState::Running | NodeState::Waiting
        );
        if !is_active {
            continue;
        }

        // A node inside a CancellationScope is non-cancellable. The scope
        // node itself is also non-cancellable — cancelling it would
        // terminate the scope (and its children) before they had a chance
        // to drain, defeating the purpose of the scope in the first place.
        let inside_scope = is_descendant_of_any(&tree, node, &scope_node_ids);
        let is_scope_node = node.block_type == BlockType::CancellationScope;

        // Nodes inside a try-catch `finally` branch (branch_index == 2) are
        // non-cancellable: the finally block must run to completion regardless
        // of external signals. This mirrors Java/Python try-finally semantics.
        let inside_finally = is_inside_finally_branch(&tree, node);

        // Check per-step cancellable flag.
        let step_cancellable = block_map.get(&node.block_id)
            .and_then(|block| match block {
                BlockDefinition::Step(step) => Some(step.cancellable),
                _ => None,
            })
            .unwrap_or(true);

        let is_cancellable = step_cancellable && !inside_scope && !is_scope_node && !inside_finally;

        if is_cancellable {
            storage
                .update_node_state(node.id, NodeState::Cancelled)
                .await?;
        } else {
            has_non_cancellable_active = true;
        }
    }

    Ok(has_non_cancellable_active)
}

/// Check if `node` is inside the `finally` branch (`branch_index` == 2) of a
/// `TryCatch` block, OR is a `TryCatch` node with an active finally branch.
/// In either case the node must not be cancelled so the finally block can
/// run to completion.
fn is_inside_finally_branch(
    tree: &[orch8_types::execution::ExecutionNode],
    node: &orch8_types::execution::ExecutionNode,
) -> bool {
    use orch8_types::execution::BlockType;

    // Case 1: The node itself is a TryCatch with active finally children.
    // Cancelling it would orphan the finally branch.
    if node.block_type == BlockType::TryCatch {
        let finally_children: Vec<_> = tree
            .iter()
            .filter(|c| c.parent_id == Some(node.id) && c.branch_index == Some(2))
            .collect();
        let has_active_finally = finally_children.iter().any(|c| {
            matches!(
                c.state,
                NodeState::Pending | NodeState::Running | NodeState::Waiting
            )
        });
        if has_active_finally
            || (!finally_children.is_empty()
                && finally_children
                    .iter()
                    .all(|c| c.state == NodeState::Pending))
        {
            return true;
        }
    }

    // Case 2: Walk up looking for an ancestor with branch_index == 2
    // whose parent is a TryCatch.
    let mut current = node;
    loop {
        let Some(parent_id) = current.parent_id else {
            return false;
        };
        let Some(parent) = tree.iter().find(|n| n.id == parent_id) else {
            return false;
        };
        if current.branch_index == Some(2) && parent.block_type == BlockType::TryCatch {
            return true;
        }
        current = parent;
    }
}

/// Check if `node` is a descendant of any node whose ID is in `ancestor_ids`.
fn is_descendant_of_any(
    tree: &[orch8_types::execution::ExecutionNode],
    node: &orch8_types::execution::ExecutionNode,
    ancestor_ids: &std::collections::HashSet<orch8_types::ids::ExecutionNodeId>,
) -> bool {
    let node_map: std::collections::HashMap<_, _> = tree.iter().map(|n| (n.id, n)).collect();
    let mut current_parent = node.parent_id;
    while let Some(pid) = current_parent {
        if ancestor_ids.contains(&pid) {
            return true;
        }
        current_parent = node_map.get(&pid).copied().and_then(|n| n.parent_id);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::ids::{Namespace, SequenceId, TenantId};
    use orch8_types::instance::{Priority, TaskInstance};
    use orch8_types::signal::SignalType;
    use serde_json::json;

    fn mk_signal(instance_id: InstanceId, st: SignalType, payload: serde_json::Value) -> Signal {
        Signal {
            id: uuid::Uuid::now_v7(),
            instance_id,
            signal_type: st,
            payload,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        }
    }

    fn mk_instance_with_state(state: InstanceState) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state,
            next_fire_at: Some(now),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: json!({}),
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
    async fn empty_signal_list_returns_false() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            InstanceId::new(),
            InstanceState::Running,
            vec![],
            None,
        )
        .await
        .unwrap();
        assert!(!r);
    }

    #[tokio::test]
    async fn pause_signal_on_running_aborts_and_pauses() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let mut inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        // Put it in Running state so pause is a valid transition.
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();
        inst.state = InstanceState::Running;

        let sig = mk_signal(inst.id, SignalType::Pause, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Running, vec![sig], None)
                .await
                .unwrap();
        assert!(r, "pause must return true to abort execution");
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Paused);
    }

    #[tokio::test]
    async fn pause_signal_on_completed_does_not_transition() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let mut inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Completed, None)
            .await
            .unwrap();
        inst.state = InstanceState::Completed;

        let sig = mk_signal(inst.id, SignalType::Pause, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Completed,
            vec![sig.clone()],
            None,
        )
        .await
        .unwrap();
        assert!(!r);
        // Signal should have been marked delivered even though the transition was refused.
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Completed);
    }

    #[tokio::test]
    async fn resume_signal_on_paused_reschedules() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Paused);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Resume, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Paused, vec![sig], None)
                .await
                .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Scheduled);
        assert!(stored.next_fire_at.is_some());
    }

    #[tokio::test]
    async fn resume_signal_on_running_is_noop_and_not_aborted() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Resume, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Running, vec![sig], None)
                .await
                .unwrap();
        assert!(
            !r,
            "resume on a non-paused instance must not abort execution"
        );
    }

    #[tokio::test]
    async fn cancel_signal_on_scheduled_cancels() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            vec![sig],
            None,
        )
        .await
        .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Cancelled);
    }

    #[tokio::test]
    async fn cancel_signal_on_completed_does_not_transition() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let mut inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Completed, None)
            .await
            .unwrap();
        inst.state = InstanceState::Completed;

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Completed,
            vec![sig],
            None,
        )
        .await
        .unwrap();
        assert!(!r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Completed);
    }

    #[tokio::test]
    async fn update_context_signal_updates_instance_context() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let new_ctx = ExecutionContext {
            data: json!({"answer": 42}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        };
        let sig = mk_signal(
            inst.id,
            SignalType::UpdateContext,
            serde_json::to_value(&new_ctx).unwrap(),
        );
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            vec![sig],
            None,
        )
        .await
        .unwrap();
        assert!(!r, "UpdateContext must not abort execution");
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.context.data["answer"], 42);
    }

    #[tokio::test]
    async fn update_context_signal_with_garbage_payload_is_swallowed() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        // payload is not a valid ExecutionContext JSON — should warn but not fail.
        let sig = mk_signal(
            inst.id,
            SignalType::UpdateContext,
            json!("not an object at all"),
        );
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            vec![sig],
            None,
        )
        .await
        .unwrap();
        assert!(!r);
    }

    #[tokio::test]
    async fn custom_signal_is_logged_and_marked_delivered() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(
            inst.id,
            SignalType::Custom("my-event".into()),
            json!({"ping": "pong"}),
        );
        let sig_id = sig.id;
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            vec![sig],
            None,
        )
        .await
        .unwrap();
        assert!(!r, "custom signals don't abort execution");
        // Pending list must no longer include this signal.
        let pending = storage.get_pending_signals(inst.id).await.unwrap();
        assert!(pending.iter().all(|s| s.id != sig_id));
    }

    #[tokio::test]
    async fn multiple_signals_processed_until_pause_wins() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        // Custom + UpdateContext + Pause — pause must win and abort.
        let ctx = ExecutionContext {
            data: json!({"x": 1}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        };
        let s1 = mk_signal(inst.id, SignalType::Custom("a".into()), json!({}));
        let s2 = mk_signal(
            inst.id,
            SignalType::UpdateContext,
            serde_json::to_value(&ctx).unwrap(),
        );
        let s3 = mk_signal(inst.id, SignalType::Pause, json!({}));
        storage.enqueue_signal(&s1).await.unwrap();
        storage.enqueue_signal(&s2).await.unwrap();
        storage.enqueue_signal(&s3).await.unwrap();

        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Running,
            vec![s1, s2, s3],
            None,
        )
        .await
        .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Paused);
        // Context was also updated by s2 before pause fired.
        assert_eq!(stored.context.data["x"], 1);
    }

    #[tokio::test]
    async fn cancel_on_paused_instance_cancels() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Paused);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Paused, vec![sig], None)
                .await
                .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Cancelled);
    }

    #[tokio::test]
    async fn process_signals_public_entry_delegates_to_prefetched() {
        // Covers the public fn that fetches signals itself.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();

        let r = process_signals(&storage, inst.id, InstanceState::Scheduled)
            .await
            .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Cancelled);
    }

    #[tokio::test]
    async fn delivered_signals_do_not_appear_in_pending_list_next_time() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Custom("n".into()), json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        process_signals(&storage, inst.id, InstanceState::Scheduled)
            .await
            .unwrap();
        // Subsequent fetch must return empty.
        let pending = storage.get_pending_signals(inst.id).await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn pause_on_paused_instance_is_ignored_but_delivered() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Paused);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Pause, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Paused, vec![sig], None)
                .await
                .unwrap();
        assert!(!r, "pause on already-paused instance must not abort");
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Paused);
    }

    // ----- TEST_PLAN items 79, 85-89 -----

    #[tokio::test]
    async fn cancel_signal_on_running_cancels_instance() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r =
            process_signals_prefetched(&storage, inst.id, InstanceState::Running, vec![sig], None)
                .await
                .unwrap();
        assert!(r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Cancelled);
    }

    #[tokio::test]
    async fn signal_delivery_marks_delivered_flag_set() {
        // Item 89: after processing, the signal must be `delivered=true` and
        // therefore must not appear in the next get_pending_signals call.
        // (SQLite backend does not populate delivered_at — that column is
        // surfaced by Postgres only; the delivered flag is the portable
        // invariant.)
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();

        let sig = mk_signal(inst.id, SignalType::Custom("ping".into()), json!({}));
        let id = sig.id;
        storage.enqueue_signal(&sig).await.unwrap();

        // Sanity: visible in pending before processing.
        let before = storage.get_pending_signals(inst.id).await.unwrap();
        assert!(before.iter().any(|s| s.id == id));

        process_signals_prefetched(&storage, inst.id, InstanceState::Scheduled, vec![sig], None)
            .await
            .unwrap();

        let after = storage.get_pending_signals(inst.id).await.unwrap();
        assert!(
            after.iter().all(|s| s.id != id),
            "delivered signal must be absent from pending list"
        );
    }

    // ---- Scoped cancel helpers ----

    fn mk_step_block(id: &str, cancellable: bool) -> orch8_types::sequence::BlockDefinition {
        use orch8_types::ids::BlockId;
        use orch8_types::sequence::{BlockDefinition, StepDef};
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId(id.into()),
            handler: "noop".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
        }))
    }

    fn mk_sequence(blocks: Vec<orch8_types::sequence::BlockDefinition>) -> SequenceDefinition {
        use orch8_types::ids::{Namespace, SequenceId, TenantId};
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: "seq".into(),
            version: 1,
            deprecated: false,
            blocks,
            interceptors: None,
            created_at: Utc::now(),
        }
    }

    async fn insert_node(
        storage: &SqliteStorage,
        instance_id: InstanceId,
        parent_id: Option<orch8_types::ids::ExecutionNodeId>,
        block_id: &str,
        block_type: orch8_types::execution::BlockType,
        branch_index: Option<i16>,
        state: NodeState,
    ) -> orch8_types::execution::ExecutionNode {
        use orch8_types::execution::ExecutionNode;
        use orch8_types::ids::{BlockId, ExecutionNodeId};
        let node = ExecutionNode {
            id: ExecutionNodeId(uuid::Uuid::now_v7()),
            instance_id,
            parent_id,
            block_id: BlockId(block_id.into()),
            block_type,
            branch_index,
            state,
            started_at: None,
            completed_at: None,
        };
        storage.create_execution_node(&node).await.unwrap();
        node
    }

    #[tokio::test]
    async fn scoped_cancel_skips_non_cancellable_steps() {
        // Item 85: a running step with `cancellable: false` must not be
        // marked Cancelled; the instance stays Running and the signal is
        // delivered so the evaluator can drain the non-cancellable work.
        use orch8_types::execution::BlockType;
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        let seq = mk_sequence(vec![mk_step_block("s1", false)]);
        insert_node(
            &storage,
            inst.id,
            None,
            "s1",
            BlockType::Step,
            None,
            NodeState::Running,
        )
        .await;

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Running,
            vec![sig],
            Some(&seq),
        )
        .await
        .unwrap();
        assert!(!r, "must not abort while non-cancellable step still active");
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Running);
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        let step = tree.iter().find(|n| n.block_id.0 == "s1").unwrap();
        assert_eq!(step.state, NodeState::Running);
    }

    #[tokio::test]
    async fn scoped_cancel_skips_cancellation_scope_children() {
        // Item 86: nodes inside a CancellationScope are non-cancellable, even
        // if their per-step flag says otherwise.
        use orch8_types::execution::BlockType;
        use orch8_types::sequence::{BlockDefinition, CancellationScopeDef};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        let scope = BlockDefinition::CancellationScope(Box::new(CancellationScopeDef {
            id: orch8_types::ids::BlockId("scope".into()),
            blocks: vec![mk_step_block("inner", true)],
        }));
        let seq = mk_sequence(vec![scope]);

        let scope_node = insert_node(
            &storage,
            inst.id,
            None,
            "scope",
            BlockType::CancellationScope,
            None,
            NodeState::Running,
        )
        .await;
        insert_node(
            &storage,
            inst.id,
            Some(scope_node.id),
            "inner",
            BlockType::Step,
            None,
            NodeState::Running,
        )
        .await;

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Running,
            vec![sig],
            Some(&seq),
        )
        .await
        .unwrap();
        assert!(!r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Running);
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        for n in &tree {
            assert_ne!(
                n.state,
                NodeState::Cancelled,
                "node {:?} inside CancellationScope was cancelled",
                n.block_id
            );
        }
    }

    #[tokio::test]
    async fn scoped_cancel_skips_try_catch_finally_nodes() {
        // Item 87: try-catch finally branch (branch_index == 2) is
        // non-cancellable so teardown steps always run.
        use orch8_types::execution::BlockType;
        use orch8_types::sequence::{BlockDefinition, TryCatchDef};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        let tc = BlockDefinition::TryCatch(Box::new(TryCatchDef {
            id: orch8_types::ids::BlockId("tc".into()),
            try_block: vec![mk_step_block("t", true)],
            catch_block: vec![],
            finally_block: Some(vec![mk_step_block("f", true)]),
        }));
        let seq = mk_sequence(vec![tc]);

        let tc_node = insert_node(
            &storage,
            inst.id,
            None,
            "tc",
            BlockType::TryCatch,
            None,
            NodeState::Running,
        )
        .await;
        insert_node(
            &storage,
            inst.id,
            Some(tc_node.id),
            "f",
            BlockType::Step,
            Some(2),
            NodeState::Running,
        )
        .await;

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Running,
            vec![sig],
            Some(&seq),
        )
        .await
        .unwrap();
        assert!(!r, "finally branch must keep the instance alive");
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Running);
        let tree = storage.get_execution_tree(inst.id).await.unwrap();
        let finally_node = tree.iter().find(|n| n.block_id.0 == "f").unwrap();
        assert_eq!(finally_node.state, NodeState::Running);
    }

    #[tokio::test]
    async fn cancel_with_all_nodes_non_cancellable_does_not_cancel_instance() {
        // Item 88: if every active node is non-cancellable the instance
        // must remain Running and the signal is consumed (delivered) but not
        // escalated to a full cancellation.
        use orch8_types::execution::BlockType;
        let storage = SqliteStorage::in_memory().await.unwrap();
        let inst = mk_instance_with_state(InstanceState::Scheduled);
        storage.create_instance(&inst).await.unwrap();
        storage
            .update_instance_state(inst.id, InstanceState::Running, None)
            .await
            .unwrap();

        let seq = mk_sequence(vec![mk_step_block("a", false), mk_step_block("b", false)]);
        insert_node(
            &storage,
            inst.id,
            None,
            "a",
            BlockType::Step,
            None,
            NodeState::Running,
        )
        .await;
        insert_node(
            &storage,
            inst.id,
            None,
            "b",
            BlockType::Step,
            None,
            NodeState::Pending,
        )
        .await;

        let sig = mk_signal(inst.id, SignalType::Cancel, json!({}));
        let sig_id = sig.id;
        storage.enqueue_signal(&sig).await.unwrap();
        let r = process_signals_prefetched(
            &storage,
            inst.id,
            InstanceState::Running,
            vec![sig],
            Some(&seq),
        )
        .await
        .unwrap();
        assert!(!r);
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Running);
        // Signal is marked delivered so we don't reprocess forever.
        let pending = storage.get_pending_signals(inst.id).await.unwrap();
        assert!(pending.iter().all(|s| s.id != sig_id));
    }

    #[tokio::test]
    async fn is_descendant_of_any_detects_chain() {
        use orch8_types::execution::{BlockType, ExecutionNode};
        use orch8_types::ids::{BlockId, ExecutionNodeId};
        let instance_id = InstanceId::new();
        let root = ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            parent_id: None,
            block_id: BlockId("root".into()),
            block_type: BlockType::CancellationScope,
            branch_index: None,
            state: NodeState::Running,
            started_at: None,
            completed_at: None,
        };
        let child = ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            parent_id: Some(root.id),
            block_id: BlockId("child".into()),
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Running,
            started_at: None,
            completed_at: None,
        };
        let grandchild = ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id,
            parent_id: Some(child.id),
            block_id: BlockId("grandchild".into()),
            block_type: BlockType::Step,
            branch_index: None,
            state: NodeState::Running,
            started_at: None,
            completed_at: None,
        };
        let tree = vec![root.clone(), child.clone(), grandchild.clone()];
        let ancestors = std::collections::HashSet::from([root.id]);
        assert!(is_descendant_of_any(&tree, &grandchild, &ancestors));
        assert!(is_descendant_of_any(&tree, &child, &ancestors));
        // Root is not a descendant of itself.
        assert!(!is_descendant_of_any(&tree, &root, &ancestors));
        // Unknown ancestor — returns false.
        assert!(!is_descendant_of_any(
            &tree,
            &grandchild,
            &std::collections::HashSet::from([ExecutionNodeId::new()])
        ));
    }
}
