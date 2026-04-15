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
