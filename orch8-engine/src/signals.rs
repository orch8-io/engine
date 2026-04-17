use chrono::Utc;
use tracing::{debug, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::execution::NodeState;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::{Signal, SignalType};

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
        info!(
            instance_id = %instance_id,
            signal_type = %signal.signal_type,
            "processing signal"
        );

        match &signal.signal_type {
            SignalType::Pause => {
                if current_state.can_transition_to(InstanceState::Paused) {
                    crate::lifecycle::transition_instance(
                        storage,
                        instance_id,
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
            SignalType::Resume => {
                if current_state == InstanceState::Paused {
                    crate::lifecycle::transition_instance(
                        storage,
                        instance_id,
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
            SignalType::Cancel => {
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
                    current_state,
                    InstanceState::Cancelled,
                    None,
                )
                .await?;
                storage.mark_signal_delivered(signal.id).await?;
                return Ok(true);
            }
            SignalType::UpdateContext => {
                // Payload should be an ExecutionContext JSON.
                if let Ok(ctx) = serde_json::from_value::<orch8_types::context::ExecutionContext>(
                    signal.payload.clone(),
                ) {
                    storage.update_instance_context(instance_id, &ctx).await?;
                    info!(instance_id = %instance_id, "context updated via signal");
                } else {
                    warn!(
                        instance_id = %instance_id,
                        "invalid context payload in UpdateContext signal"
                    );
                }
            }
            SignalType::Custom(name) => {
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
    let scope_node_ids: Vec<_> = tree
        .iter()
        .filter(|n| n.block_type == BlockType::CancellationScope)
        .map(|n| n.id)
        .collect();

    for node in &tree {
        let is_active = matches!(
            node.state,
            NodeState::Pending | NodeState::Running | NodeState::Waiting
        );
        if !is_active {
            continue;
        }

        // A node inside a CancellationScope is non-cancellable.
        let inside_scope = is_descendant_of_any(&tree, node, &scope_node_ids);

        // Check per-step cancellable flag.
        let step_cancellable = crate::evaluator::find_block(&sequence_def.blocks, &node.block_id)
            .and_then(|block| match block {
                BlockDefinition::Step(step) => Some(step.cancellable),
                _ => None,
            })
            .unwrap_or(true);

        let is_cancellable = step_cancellable && !inside_scope;

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

/// Check if `node` is a descendant of any node whose ID is in `ancestor_ids`.
fn is_descendant_of_any(
    tree: &[orch8_types::execution::ExecutionNode],
    node: &orch8_types::execution::ExecutionNode,
    ancestor_ids: &[orch8_types::ids::ExecutionNodeId],
) -> bool {
    let mut current_parent = node.parent_id;
    while let Some(pid) = current_parent {
        if ancestor_ids.contains(&pid) {
            return true;
        }
        current_parent = tree.iter().find(|n| n.id == pid).and_then(|n| n.parent_id);
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
    use serde_json::json;

    fn mk_signal(instance_id: InstanceId, st: SignalType, payload: serde_json::Value) -> Signal {
        Signal {
            id: uuid::Uuid::new_v4(),
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
        assert!(!r, "resume on a non-paused instance must not abort execution");
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
        assert!(is_descendant_of_any(&tree, &grandchild, &[root.id]));
        assert!(is_descendant_of_any(&tree, &child, &[root.id]));
        // Root is not a descendant of itself.
        assert!(!is_descendant_of_any(&tree, &root, &[root.id]));
        // Unknown ancestor — returns false.
        assert!(!is_descendant_of_any(
            &tree,
            &grandchild,
            &[ExecutionNodeId::new()]
        ));
    }
}
