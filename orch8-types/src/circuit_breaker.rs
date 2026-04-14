use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::TenantId;

/// Circuit breaker state for a (tenant, handler) pair.
///
/// Breakers are scoped per-tenant so a noisy customer cannot trip the circuit
/// for an unrelated tenant sharing the same handler name.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CircuitBreakerState {
    /// Owning tenant — breakers are isolated per-tenant.
    pub tenant_id: TenantId,
    /// Handler name this breaker protects.
    pub handler: String,
    pub state: BreakerState,
    /// Consecutive failure count.
    pub failure_count: u32,
    /// Threshold of failures before opening the circuit.
    pub failure_threshold: u32,
    /// Cooldown period in seconds before transitioning from Open to `HalfOpen`.
    pub cooldown_secs: u64,
    /// When the circuit was last opened (to compute cooldown expiry).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opened_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl std::fmt::Display for BreakerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("closed"),
            Self::Open => f.write_str("open"),
            Self::HalfOpen => f.write_str("half_open"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid() -> TenantId {
        TenantId("test-tenant".into())
    }

    #[test]
    fn breaker_state_display() {
        assert_eq!(BreakerState::Closed.to_string(), "closed");
        assert_eq!(BreakerState::Open.to_string(), "open");
        assert_eq!(BreakerState::HalfOpen.to_string(), "half_open");
    }

    #[test]
    fn breaker_state_serde_roundtrip() {
        let state = BreakerState::HalfOpen;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"half_open\"");
        let back: BreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, state);
    }

    #[test]
    fn circuit_breaker_state_serde_roundtrip() {
        let cb = CircuitBreakerState {
            tenant_id: tid(),
            handler: "my_handler".into(),
            state: BreakerState::Open,
            failure_count: 5,
            failure_threshold: 10,
            cooldown_secs: 30,
            opened_at: Some(chrono::Utc::now()),
        };
        let json = serde_json::to_string(&cb).unwrap();
        let back: CircuitBreakerState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.tenant_id, tid());
        assert_eq!(back.handler, "my_handler");
        assert_eq!(back.state, BreakerState::Open);
        assert_eq!(back.failure_count, 5);
        assert!(back.opened_at.is_some());
    }

    #[test]
    fn circuit_breaker_state_opened_at_skipped_when_none() {
        let cb = CircuitBreakerState {
            tenant_id: tid(),
            handler: "h".into(),
            state: BreakerState::Closed,
            failure_count: 0,
            failure_threshold: 5,
            cooldown_secs: 60,
            opened_at: None,
        };
        let json = serde_json::to_string(&cb).unwrap();
        assert!(!json.contains("opened_at"));
    }
}
