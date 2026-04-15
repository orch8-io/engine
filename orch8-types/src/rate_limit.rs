use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::{ResourceKey, TenantId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimit {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub resource_key: ResourceKey,
    pub max_count: i32,
    pub window_seconds: i32,
    pub current_count: i32,
    pub window_start: DateTime<Utc>,
}

/// Result of checking a rate limit.
/// Making this an enum forces callers to handle both cases.
#[derive(Debug)]
pub enum RateLimitCheck {
    Allowed,
    Exceeded { retry_after: DateTime<Utc> },
}
