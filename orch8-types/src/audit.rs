use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{InstanceId, TenantId};

/// An immutable audit log entry recording a state transition or lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AuditLogEntry {
    pub id: Uuid,
    pub instance_id: InstanceId,
    pub tenant_id: TenantId,
    /// Event type (e.g. `state_transition`, `signal_received`, `step_completed`, `step_failed`).
    pub event_type: String,
    /// Previous state (for transitions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_state: Option<String>,
    /// New state (for transitions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_state: Option<String>,
    /// Block ID related to this event, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    /// Additional event data (e.g., error message, signal payload).
    #[serde(default)]
    pub details: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_log_entry_serde_roundtrip() {
        let entry = AuditLogEntry {
            id: Uuid::now_v7(),
            instance_id: InstanceId(Uuid::now_v7()),
            tenant_id: TenantId("tenant_1".into()),
            event_type: "state_transition".into(),
            from_state: Some("running".into()),
            to_state: Some("completed".into()),
            block_id: Some("step_1".into()),
            details: serde_json::json!({"duration_ms": 150}),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: AuditLogEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event_type, "state_transition");
        assert_eq!(back.from_state.as_deref(), Some("running"));
        assert_eq!(back.to_state.as_deref(), Some("completed"));
        assert_eq!(back.block_id.as_deref(), Some("step_1"));
    }

    #[test]
    fn audit_log_entry_optional_fields_skipped_when_none() {
        let entry = AuditLogEntry {
            id: Uuid::now_v7(),
            instance_id: InstanceId(Uuid::now_v7()),
            tenant_id: TenantId("t".into()),
            event_type: "signal_received".into(),
            from_state: None,
            to_state: None,
            block_id: None,
            details: serde_json::json!({}),
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("from_state"));
        assert!(!json.contains("to_state"));
        assert!(!json.contains("block_id"));
    }
}
