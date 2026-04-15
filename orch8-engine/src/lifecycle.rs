use chrono::{DateTime, Utc};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::audit::AuditLogEntry;
use orch8_types::ids::{InstanceId, TenantId};
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

/// Record an audit log entry for a state transition.
/// This is best-effort — audit failures are logged but do not block execution.
pub async fn audit_transition(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    tenant_id: &TenantId,
    from: InstanceState,
    to: InstanceState,
    block_id: Option<&str>,
) {
    let entry = AuditLogEntry {
        id: uuid::Uuid::new_v4(),
        instance_id,
        tenant_id: tenant_id.clone(),
        event_type: "state_transition".into(),
        from_state: Some(from.to_string()),
        to_state: Some(to.to_string()),
        block_id: block_id.map(String::from),
        details: serde_json::Value::Null,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.append_audit_log(&entry).await {
        tracing::warn!(error = %e, "failed to append audit log entry");
    }
}

/// Record a generic audit event.
pub async fn audit_event(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    tenant_id: &TenantId,
    event_type: &str,
    block_id: Option<&str>,
    details: serde_json::Value,
) {
    let entry = AuditLogEntry {
        id: uuid::Uuid::new_v4(),
        instance_id,
        tenant_id: tenant_id.clone(),
        event_type: event_type.into(),
        from_state: None,
        to_state: None,
        block_id: block_id.map(String::from),
        details,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.append_audit_log(&entry).await {
        tracing::warn!(error = %e, "failed to append audit log entry");
    }
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
