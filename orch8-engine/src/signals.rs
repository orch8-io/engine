use chrono::Utc;
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
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
    process_signals_inner(storage, instance_id, current_state, signals).await
}

/// Process pre-fetched signals (from batch query).
/// Returns `true` if execution should be aborted (e.g., pause or cancel).
pub async fn process_signals_prefetched(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    current_state: InstanceState,
    signals: Vec<Signal>,
) -> Result<bool, EngineError> {
    process_signals_inner(storage, instance_id, current_state, signals).await
}

async fn process_signals_inner(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    current_state: InstanceState,
    signals: Vec<Signal>,
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
                if current_state.can_transition_to(InstanceState::Cancelled) {
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
                warn!(
                    instance_id = %instance_id,
                    current_state = %current_state,
                    "cannot cancel instance in current state"
                );
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
