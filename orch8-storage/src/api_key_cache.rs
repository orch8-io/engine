//! Short-TTL cache for per-tenant API-key authentication.
//!
//! Both the HTTP middleware (`orch8-api`) and the gRPC interceptor
//! (`orch8-grpc`) resolve a presented secret to an [`ApiKeyRecord`] by hash on
//! *every* request. Without a cache that is one indexed `SELECT` per request
//! on the hottest path in the system — and, previously, an additional
//! fire-and-forget `last_used_at` write per request. This module collapses both
//! into at most one read + one write per key per [`CACHE_TTL`] window.
//!
//! ## Staleness & revocation
//!
//! Only *active* records are cached, and entries live for [`CACHE_TTL`].
//! Inactive/absent keys are never cached (no negative caching), so a *failed*
//! auth always reflects current database state.
//!
//! Revocation is **immediate**: `revoke_api_key` calls [`invalidate`] inside the
//! same operation that flips the `revoked` flag, evicting the cache entry. The
//! TTL therefore only bounds staleness for *time-based expiry* (a key passing
//! its `expires_at` while cached) — a benign window, since expiry is not an
//! urgent operator action — and acts as a backstop against any path that
//! mutates a key without going through `revoke_api_key`.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use moka::future::Cache;

use orch8_types::api_key::ApiKeyRecord;
use orch8_types::error::StorageError;

use crate::StorageBackend;

/// How long an authenticated key record stays cached. Revocation is evicted
/// eagerly (see [`invalidate`]), so this only bounds staleness for time-based
/// expiry and serves as a safety backstop.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// Ceiling on distinct cached hashes, so a flood of unique (e.g. bogus) keys
/// can't grow the cache unbounded. Only active keys are ever inserted, so this
/// caps *legitimate* key cardinality — generous for any realistic deployment.
const CACHE_CAPACITY: u64 = 50_000;

fn cache() -> &'static Cache<String, ApiKeyRecord> {
    static CACHE: OnceLock<Cache<String, ApiKeyRecord>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(CACHE_CAPACITY)
            .time_to_live(CACHE_TTL)
            .build()
    })
}

/// Resolve a presented secret's SHA-256 `key_hash` to its [`ApiKeyRecord`],
/// using the process-wide cache.
///
/// Returns the record exactly as [`StorageBackend::lookup_api_key_by_hash`]
/// would (active *or* inactive, or `None`) — callers keep their own
/// [`ApiKeyRecord::is_active`] check. The only behavioural change is that an
/// active record is served from memory for up to [`CACHE_TTL`], and its
/// `last_used_at` is refreshed (best-effort, off the request path) at most once
/// per window rather than on every request.
pub async fn authenticate(
    storage: &Arc<dyn StorageBackend>,
    key_hash: &str,
) -> Result<Option<ApiKeyRecord>, StorageError> {
    if let Some(record) = cache().get(key_hash).await {
        // Cache holds only active records; the TTL bounds staleness.
        return Ok(Some(record));
    }

    let now = Utc::now();
    let fetched = storage.lookup_api_key_by_hash(key_hash).await?;
    if let Some(ref record) = fetched {
        if record.is_active(now) {
            touch(storage, record, now);
            cache().insert(key_hash.to_string(), record.clone()).await;
        }
    }
    Ok(fetched)
}

/// Drop the cached entry for `key_hash` so a revoked or rotated key stops
/// authenticating *immediately* rather than after [`CACHE_TTL`].
///
/// Called by the storage backends from within `revoke_api_key`, so every
/// revocation path (HTTP, gRPC, the encrypting decorator) invalidates the
/// cache without the caller having to know it exists.
pub async fn invalidate(key_hash: &str) {
    cache().invalidate(key_hash).await;
}

/// Best-effort audit hygiene: refresh `last_used_at` without blocking the
/// request. Only reached on a cache miss, so writes are naturally coalesced to
/// at most one per key per [`CACHE_TTL`] window.
fn touch(storage: &Arc<dyn StorageBackend>, record: &ApiKeyRecord, now: DateTime<Utc>) {
    let storage = Arc::clone(storage);
    let id = record.id.clone();
    tokio::spawn(async move {
        if let Err(e) = storage.touch_api_key(&id, now).await {
            tracing::warn!(error = %e, "failed to touch api_key last_used_at");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn sqlite() -> Arc<dyn StorageBackend> {
        Arc::new(crate::sqlite::SqliteStorage::in_memory().await.unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn active_key_is_inserted_into_the_cache() {
        let storage = sqlite().await;
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        let hash = minted.record.key_hash.clone();
        storage.create_api_key(&minted.record).await.unwrap();

        let first = authenticate(&storage, &hash).await.unwrap();
        assert_eq!(first.unwrap().tenant_id, "acme");
        // The active record must now live in the cache (proves the hit path is
        // populated — distinct random hash means no cross-test interference).
        assert!(
            cache().get(&hash).await.is_some(),
            "active key should cache"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revoke_invalidates_cache_for_instant_revocation() {
        let storage = sqlite().await;
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        let hash = minted.record.key_hash.clone();
        storage.create_api_key(&minted.record).await.unwrap();

        authenticate(&storage, &hash).await.unwrap(); // populate cache
        assert!(cache().get(&hash).await.is_some());

        // Revoking must drop the cache entry synchronously with the write…
        storage.revoke_api_key(&minted.record.id).await.unwrap();
        assert!(
            cache().get(&hash).await.is_none(),
            "revoke must evict the cached entry"
        );
        // …so the very next auth reflects revocation immediately, not after TTL.
        let after = authenticate(&storage, &hash).await.unwrap();
        assert!(
            after.is_some_and(|rec| !rec.is_active(Utc::now())),
            "revoked key must authenticate as inactive immediately"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn does_not_cache_absent_keys() {
        let storage = sqlite().await;
        let hash = "deadbeef".repeat(8);
        let missing = authenticate(&storage, &hash).await.unwrap();
        assert!(missing.is_none());
        assert!(
            cache().get(&hash).await.is_none(),
            "absent key must not cache"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn does_not_cache_revoked_keys() {
        let storage = sqlite().await;
        let minted = orch8_types::api_key::mint("acme", "ci", None);
        storage.create_api_key(&minted.record).await.unwrap();
        storage.revoke_api_key(&minted.record.id).await.unwrap();

        let hash = &minted.record.key_hash;
        let r = authenticate(&storage, hash).await.unwrap();
        assert!(
            r.is_some_and(|rec| !rec.is_active(Utc::now())),
            "revoked key must be reported inactive, never cached active"
        );
        assert!(
            cache().get(hash).await.is_none(),
            "inactive key must not cache"
        );
    }
}
