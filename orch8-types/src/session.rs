use std::str::FromStr;

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
    /// Session temporarily suspended; resumable back to Active.
    Paused,
    Completed,
    Expired,
}

impl FromStr for SessionState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "expired" => Ok(Self::Expired),
            other => Err(format!("unknown session state: {other}")),
        }
    }
}

impl std::fmt::Display for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Paused => f.write_str("paused"),
            Self::Completed => f.write_str("completed"),
            Self::Expired => f.write_str("expired"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_state_from_str_all_variants() {
        assert_eq!(
            "active".parse::<SessionState>().unwrap(),
            SessionState::Active
        );
        assert_eq!(
            "paused".parse::<SessionState>().unwrap(),
            SessionState::Paused
        );
        assert_eq!(
            "completed".parse::<SessionState>().unwrap(),
            SessionState::Completed
        );
        assert_eq!(
            "expired".parse::<SessionState>().unwrap(),
            SessionState::Expired
        );
    }

    #[test]
    fn session_state_from_str_unknown() {
        let err = "deleted".parse::<SessionState>().unwrap_err();
        assert!(err.contains("unknown session state: deleted"));
    }

    #[test]
    fn session_state_display_roundtrip() {
        let states = [
            SessionState::Active,
            SessionState::Paused,
            SessionState::Completed,
            SessionState::Expired,
        ];
        for state in states {
            let s = state.to_string();
            let parsed: SessionState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn session_state_serde_roundtrip() {
        let state = SessionState::Paused;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"paused\"");
        let back: SessionState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }
}
