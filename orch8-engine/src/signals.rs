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
