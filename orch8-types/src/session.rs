use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::TenantId;

/// A session groups related instances and holds shared data across them.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Session {
    pub id: Uuid,
    pub tenant_id: TenantId,
    /// Human-readable session key (e.g., "user:123:onboarding").
    pub session_key: String,
    /// Session-scoped shared data accessible by all instances in this session.
    #[serde(default)]
    pub data: serde_json::Value,
    pub state: SessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Optional TTL — session expires after this time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Active,
    Completed,
    Expired,
}

impl std::fmt::Display for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Completed => f.write_str("completed"),
            Self::Expired => f.write_str("expired"),
        }
    }
}
