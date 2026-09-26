//! Public, read-only progress links for a single instance.
//!
//! `POST /instances/{id}/share` mints an unguessable token; only its SHA-256
//! is stored. The unauthenticated `GET /public/progress/{token}` endpoint
//! returns a redacted view (step labels, states, counts, timestamps) and
//! exposes `context.data` fields only when they were explicitly allowlisted
//! at share time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{InstanceId, TenantId};

/// Maximum number of allowlisted `context.data` fields per share.
pub const MAX_ALLOWED_FIELDS: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProgressShare {
    pub id: Uuid,
    /// Lowercase hex SHA-256 of the raw token. Never serialized to clients.
    #[serde(skip_serializing, default)]
    pub token_hash: String,
    pub tenant_id: TenantId,
    pub instance_id: InstanceId,
    /// Top-level `context.data` keys the public view may include.
    #[serde(default)]
    pub allowed_fields: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
}

impl ProgressShare {
    /// Live = not revoked and not expired at `now`.
    #[must_use]
    pub fn is_live(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && now < self.expires_at
    }
}

/// Validate one allowlisted field name: a plain top-level key.
#[must_use]
pub fn allowed_field_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn liveness() {
        let now = Utc::now();
        let mut s = ProgressShare {
            id: Uuid::now_v7(),
            token_hash: "h".into(),
            tenant_id: TenantId::unchecked("t"),
            instance_id: InstanceId::new(),
            allowed_fields: vec![],
            created_at: now,
            expires_at: now + chrono::Duration::hours(1),
            revoked_at: None,
        };
        assert!(s.is_live(now));
        assert!(!s.is_live(now + chrono::Duration::hours(2)));
        s.revoked_at = Some(now);
        assert!(!s.is_live(now));
        let json = serde_json::to_value(&s).unwrap();
        assert!(
            json.get("token_hash").is_none(),
            "hash must never be serialized"
        );
    }

    #[test]
    fn field_names() {
        assert!(allowed_field_is_valid("order_id"));
        assert!(!allowed_field_is_valid("a.b"));
        assert!(!allowed_field_is_valid(""));
    }
}
