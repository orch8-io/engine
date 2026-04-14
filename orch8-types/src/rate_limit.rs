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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_serde_roundtrip() {
        let rl = RateLimit {
            id: Uuid::now_v7(),
            tenant_id: TenantId("t".into()),
            resource_key: ResourceKey("api:create".into()),
            max_count: 100,
            window_seconds: 60,
            current_count: 42,
            window_start: Utc::now(),
        };
        let json = serde_json::to_string(&rl).unwrap();
        let back: RateLimit = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_count, 100);
        assert_eq!(back.window_seconds, 60);
        assert_eq!(back.current_count, 42);
        assert_eq!(back.resource_key.0, "api:create");
    }

    #[test]
    fn rate_limit_check_allowed_variant() {
        let check = RateLimitCheck::Allowed;
        assert!(matches!(check, RateLimitCheck::Allowed));
    }

    #[test]
    fn rate_limit_check_exceeded_variant() {
        let retry = Utc::now();
        let check = RateLimitCheck::Exceeded { retry_after: retry };
        match check {
            RateLimitCheck::Exceeded { retry_after } => assert_eq!(retry_after, retry),
            RateLimitCheck::Allowed => panic!("expected Exceeded"),
        }
    }
}
