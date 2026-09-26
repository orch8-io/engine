//! Out-of-band approval action tokens for `human_review` gates.
//!
//! When a `human_review` step posts interactive approvals (Slack buttons,
//! Teams card actions, email magic links), each *choice* on each channel gets
//! its own unguessable 256-bit token. Only the SHA-256 of the token is
//! stored; the raw token lives solely in the message sent to the reviewer.
//!
//! Tokens are single-use per gate: consuming any token for an
//! `(instance, block)` burns every sibling token for that gate, so a second
//! click (or a different reviewer's click on another channel) cannot record
//! a second decision. Tokens also expire.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::ids::{BlockId, InstanceId, TenantId};

/// Channel an approval token was issued for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalChannel {
    Slack,
    Teams,
    Email,
}

impl ApprovalChannel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Slack => "slack",
            Self::Teams => "teams",
            Self::Email => "email",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "slack" => Some(Self::Slack),
            "teams" => Some(Self::Teams),
            "email" => Some(Self::Email),
            _ => None,
        }
    }
}

/// One stored approval action (hash only — never the raw token).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalActionToken {
    /// Lowercase hex SHA-256 of the raw token.
    pub token_hash: String,
    pub tenant_id: TenantId,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    /// The `wait_for_input` choice value this action submits.
    pub choice: String,
    pub channel: ApprovalChannel,
    /// Non-secret label for the audit trail (email address, Slack channel).
    pub recipient: Option<String>,
    /// Credential reference used to verify the inbound interaction (Slack
    /// signing secret). `None` for channels authenticated by the token alone.
    pub verify_secret_ref: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub used_at: Option<DateTime<Utc>>,
}

/// Hash a raw token for storage / lookup.
#[must_use]
pub fn hash_token(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Plausibility check for a raw token presented by an unauthenticated
/// caller, before any storage lookup: URL-safe base64 alphabet, bounded
/// length.
#[must_use]
pub fn token_looks_valid(raw: &str) -> bool {
    (32..=128).contains(&raw.len())
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_hex() {
        let h = hash_token("abc");
        assert_eq!(h.len(), 64);
        assert_eq!(h, hash_token("abc"));
        assert_ne!(h, hash_token("abd"));
    }

    #[test]
    fn token_shape_check() {
        assert!(token_looks_valid(&"a".repeat(43)));
        assert!(!token_looks_valid("short"));
        assert!(!token_looks_valid(&format!("{}/", "a".repeat(43))));
    }

    #[test]
    fn channel_round_trip() {
        for c in [
            ApprovalChannel::Slack,
            ApprovalChannel::Teams,
            ApprovalChannel::Email,
        ] {
            assert_eq!(ApprovalChannel::parse(c.as_str()), Some(c));
        }
    }
}
