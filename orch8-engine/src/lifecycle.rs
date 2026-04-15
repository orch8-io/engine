use chrono::{DateTime, Utc};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;

use crate::error::EngineError;

/// Transition an instance to a new state, validating the transition.
pub async fn transition_instance(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    from: InstanceState,
    to: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), EngineError> {
    if !from.can_transition_to(to) {
        warn!(
            instance_id = %instance_id,
            from = %from,
            to = %to,
            "invalid state transition attempted"
        );
        return Err(EngineError::InvalidTransition {
            instance_id,
            from,
            to,
        });
    }

    storage
        .update_instance_state(instance_id, to, next_fire_at)
        .await?;

    info!(
        instance_id = %instance_id,
        from = %from,
        to = %to,
        "instance state transitioned"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use orch8_types::instance::InstanceState;

    #[test]
    fn scheduled_to_running_valid() {
        assert!(InstanceState::Scheduled.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn completed_to_running_invalid() {
        assert!(!InstanceState::Completed.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn running_to_waiting_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Waiting));
    }

    #[test]
    fn running_to_scheduled_valid_for_retry() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Scheduled));
    }
}
