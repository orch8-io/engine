//! Per-tenant API key records.
//!
//! A request authenticates by presenting a secret in the `x-api-key` header.
//! Two kinds of secret are accepted by the server:
//!
//!   - the global **root** key (`api.api_key` in config) — unscoped/admin. It
//!     may act as any tenant via the `X-Tenant-Id` header (the legacy and
//!     bootstrap path) and is the only key allowed to mint/revoke other keys;
//!   - a **per-tenant** key minted through the management API and stored here.
//!     Such a key is *bound* to exactly one tenant: the tenant identity is
//!     taken from the key record, never from the (unauthenticated) header. A
//!     mismatching `X-Tenant-Id` is rejected.
//!
//! Only the SHA-256 hash of the secret is persisted. The plaintext is shown to
//! the operator exactly once at creation and is unrecoverable afterwards.

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

/// Hash an API key secret for storage and lookup.
///
/// SHA-256 is sufficient here: keys are high-entropy random tokens (244 bits —
/// two `UUIDv4` at 122 bits each), not low-entropy human passwords, so a
/// deliberately slow password hash buys nothing. The hash is the lookup key,
/// so the stored value never reveals the secret even if the database leaks.
#[must_use]
pub fn hash_api_key(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A stored API key. The secret itself is never persisted — only `key_hash`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    /// Public identifier (`ak_…`) used for listing and revocation.
    pub id: String,
    /// The tenant this key authenticates as.
    pub tenant_id: String,
    /// Human label for the key.
    pub name: String,
    /// Hex SHA-256 of the secret.
    pub key_hash: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked: bool,
}

impl ApiKeyRecord {
    /// `true` if the key may currently authenticate: not revoked and not past
    /// its expiry.
    #[must_use]
    pub fn is_active(&self, now: DateTime<Utc>) -> bool {
        !self.revoked && self.expires_at.is_none_or(|exp| exp > now)
    }
}

/// A freshly minted key: the record to persist plus the one-time plaintext
/// secret to return to the caller.
pub struct NewApiKey {
    pub record: ApiKeyRecord,
    /// The plaintext secret — exists only here, shown to the caller once.
    pub secret: String,
}

/// Mint a new API key bound to `tenant_id`.
///
/// Returns the [`ApiKeyRecord`] to persist and the one-time plaintext `secret`.
/// The secret carries 244 bits of randomness (two `UUIDv4`) — well beyond brute
/// force — and is prefixed `sk_` so it is recognisable in logs/configs.
#[must_use]
pub fn mint(
    tenant_id: impl Into<String>,
    name: impl Into<String>,
    expires_at: Option<DateTime<Utc>>,
) -> NewApiKey {
    let id = format!("ak_{}", uuid::Uuid::new_v4().simple());
    let secret = format!(
        "sk_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let key_hash = hash_api_key(&secret);
    let now = Utc::now();
    NewApiKey {
        record: ApiKeyRecord {
            id,
            tenant_id: tenant_id.into(),
            name: name.into(),
            key_hash,
            created_at: now,
            last_used_at: None,
            expires_at,
            revoked: false,
        },
        secret,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable_and_hex() {
        let h = hash_api_key("sk_test");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(h, hash_api_key("sk_test"));
        assert_ne!(h, hash_api_key("sk_other"));
    }

    #[test]
    fn mint_binds_tenant_and_hides_secret() {
        let minted = mint("acme", "ci-key", None);
        assert_eq!(minted.record.tenant_id, "acme");
        assert_eq!(minted.record.name, "ci-key");
        assert!(minted.record.id.starts_with("ak_"));
        assert!(minted.secret.starts_with("sk_"));
        // The stored hash matches the plaintext, but the plaintext is not stored.
        assert_eq!(minted.record.key_hash, hash_api_key(&minted.secret));
        assert!(!minted.record.revoked);
    }

    #[test]
    fn is_active_respects_revocation_and_expiry() {
        let now = Utc::now();
        let mut k = mint("t", "k", None).record;
        assert!(k.is_active(now));

        k.revoked = true;
        assert!(!k.is_active(now));

        k.revoked = false;
        k.expires_at = Some(now - chrono::Duration::seconds(1));
        assert!(!k.is_active(now));

        k.expires_at = Some(now + chrono::Duration::seconds(60));
        assert!(k.is_active(now));
    }
}
