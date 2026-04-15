use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Circuit breaker state for a handler.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CircuitBreakerState {
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
