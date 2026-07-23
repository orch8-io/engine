//! Short-TTL cache for per-tenant API-key authentication.
//!
//! Both the HTTP middleware (`orch8-api`) and the gRPC interceptor
//! (`orch8-grpc`) resolve a presented secret to an [`ApiKeyRecord`] by hash on
//! *every* request. Without a cache that is one indexed `SELECT` per request
//! on the hottest path in the system — and, previously, an additional
//! fire-and-forget `last_used_at` write per request. This module collapses both
//! into at most one read + one write per key per `CACHE_TTL` window.
//!
//! ## Staleness & revocation
//!
//! Only *active* records are cached in the positive cache, and entries live for
//! `CACHE_TTL`. Inactive/absent keys go into a *negative* cache with the much
//! shorter `NEGATIVE_CACHE_TTL`, so a flood of bogus keys can't hammer the
//! database while a newly-created key still becomes valid within a few
//! seconds.
//!
//! This cache is **process-local** (a `static OnceLock`, not shared storage).
//! Revocation is immediate *on the node that handles the revoke*: `revoke_api_key`
//! calls [`invalidate`] inside the same operation that flips the `revoked`
//! flag, evicting that node's cache entry. In a single-node deployment this is
//! a true immediate-revocation guarantee. In a multi-node/clustered deployment,
//! peer nodes keep serving the stale cached record from memory until their own
//! copy naturally expires — so cluster-wide revocation is only bounded by
//! `CACHE_TTL` (30s), not instantaneous. Treat that window as the real SLA for
//! incident response (e.g. compromised-key revocation) until cross-node
//! invalidation (e.g. Postgres `LISTEN`/`NOTIFY` or a shared cache) is added.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use moka::future::Cache;

use orch8_types::api_key::ApiKeyRecord;
use orch8_types::error::StorageError;

use crate::StorageBackend;

/// How long an authenticated key record stays cached. Revocation is evicted
/// eagerly on the local node (see [`invalidate`]), so on a single node this
/// only bounds staleness for time-based expiry. Across a cluster it is also
/// the upper bound on how long a *revoked* key keeps authenticating on peer
/// nodes — see the module-level doc.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// Ceiling on distinct cached hashes, so a flood of unique (e.g. bogus) keys
/// can't grow the cache unbounded. Only active keys are ever inserted, so this
/// caps *legitimate* key cardinality — generous for any realistic deployment.
const CACHE_CAPACITY: u64 = 50_000;

/// Short TTL for negative cache entries (invalid/absent keys). Keeps a flood of
/// bogus keys from hitting the database on every request while still allowing
/// newly-created keys to become valid within a few seconds.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

fn cache() -> &'static Cache<String, ApiKeyRecord> {
    static CACHE: OnceLock<Cache<String, ApiKeyRecord>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(CACHE_CAPACITY)
            .time_to_live(CACHE_TTL)
            .build()
    })
}

fn negative_cache() -> &'static Cache<String, ()> {
    static CACHE: OnceLock<Cache<String, ()>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(CACHE_CAPACITY)
            .time_to_live(NEGATIVE_CACHE_TTL)
            .build()
    })
}

/// Resolve a presented secret's SHA-256 `key_hash` to its [`ApiKeyRecord`],
/// using the process-wide cache.
///
/// Returns the record exactly as `StorageBackend::lookup_api_key_by_hash`
/// would (active *or* inactive, or `None`) — callers keep their own
/// [`ApiKeyRecord::is_active`] check. The only behavioural change is that an
/// active record is served from memory for up to `CACHE_TTL`, and its
/// `last_used_at` is refreshed (best-effort, off the request path) at most once
/// per window rather than on every request.
pub async fn authenticate(
    storage: &Arc<dyn StorageBackend>,
    key_hash: &str,
) -> Result<Option<ApiKeyRecord>, StorageError> {
    if let Some(record) = cache().get(key_hash).await {
        // The cache holds only records that were active at insertion time, but
        // a cached entry can still stale-expire between nodes or exceed its
        // `expires_at`. Re-check activity before returning it so revocation and
        // expiry are honored on the hot path.
        let now = Utc::now();
        if record.is_active(now) {
            return Ok(Some(record));
        }
        cache().invalidate(key_hash).await;
    }

    // Negative cache: a recently-observed invalid/absent key is still invalid
    // for a short window, protecting the DB from a flood of bogus attempts.
    if negative_cache().get(key_hash).await.is_some() {
        return Ok(None);
    }

    let now = Utc::now();
    let fetched = storage.lookup_api_key_by_hash(key_hash).await?;
    if let Some(ref record) = fetched {
        if record.is_active(now) {
            touch(storage, record, now);
            cache().insert(key_hash.to_string(), record.clone()).await;
        } else {
            negative_cache().insert(key_hash.to_string(), ()).await;
        }
    } else {
        negative_cache().insert(key_hash.to_string(), ()).await;
    }
    Ok(fetched)
}

/// Drop the cached entry for `key_hash` so a revoked or rotated key stops
/// authenticating *immediately* rather than after `CACHE_TTL`.
///
/// Called by the storage backends from within `revoke_api_key`, so every
/// revocation path (HTTP, gRPC, the encrypting decorator) invalidates the
/// cache without the caller having to know it exists.
pub async fn invalidate(key_hash: &str) {
    cache().invalidate(key_hash).await;
    negative_cache().invalidate(key_hash).await;
}

/// Best-effort audit hygiene: refresh `last_used_at` without blocking the
/// request. Only reached on a cache miss, so writes are naturally coalesced to
/// at most one per key per `CACHE_TTL` window.
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
    async fn caches_absent_keys_negatively() {
        let storage = sqlite().await;
        let hash = "cafebabe".repeat(8);

        let first = authenticate(&storage, &hash).await.unwrap();
        assert!(first.is_none());
        assert!(
            negative_cache().get(&hash).await.is_some(),
            "absent key should be cached negatively"
        );

        // A second lookup with the same hash should not need to query storage.
        // We can't easily observe "no DB query" here, but the cache state is
        // enough to document the behaviour.
        let second = authenticate(&storage, &hash).await.unwrap();
        assert!(second.is_none());
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
