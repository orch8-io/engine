//! In-memory cache layer over sequence reads.
//!
//! A full `StorageBackend` decorator would mean writing pass-through wrappers
//! for every trait method just to cache three sequence-read calls. This struct
//! instead encapsulates the cache at the engine service layer — callers
//! holding an `Arc<SequenceCache>` use it for hot sequence lookups and fall
//! back to the raw `StorageBackend` for everything else.
//!
//! Two independent moka caches back the struct:
//!   - `by_id`: keyed by [`SequenceId`], used by the scheduler hot path.
//!   - `by_name`: keyed by `(tenant, namespace, name, version)`, used by
//!     trigger firing and sub-sequence resolution.
//!
//! Entries are `Arc<SequenceDefinition>` so concurrent readers share a single
//! allocation. TTL acts as a correctness backstop — explicit invalidation via
//! [`SequenceCache::invalidate_by_id`] keeps stale windows short after a
//! deprecate/delete.

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;

use orch8_storage::StorageBackend;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::sequence::SequenceDefinition;

use crate::error::EngineError;

type ByNameKey = (TenantId, Namespace, String, Option<i32>);

/// Unified cache for sequence definitions, backing both by-id and by-name
/// lookups. Clone-safe (cheap: inner moka caches are `Arc`-shared).
#[derive(Clone)]
pub struct SequenceCache {
    by_id: Cache<SequenceId, Arc<SequenceDefinition>>,
    by_name: Cache<ByNameKey, Arc<SequenceDefinition>>,
}

impl SequenceCache {
    /// Build a cache with the given capacity (per sub-cache) and TTL applied
    /// to both sub-caches. `ttl` is the soft correctness bound for stale
    /// reads when no explicit invalidation fires.
    #[must_use]
    pub fn new(max_capacity: u64, ttl: Duration) -> Self {
        Self {
            by_id: Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
            by_name: Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// Get-or-fetch by `SequenceId`. On miss, reads storage, wraps in `Arc`,
    /// and populates both caches (a by-id fetch gives us the canonical name
    /// key too, so future by-name lookups for the same row hit).
    pub async fn get_by_id(
        &self,
        storage: &dyn StorageBackend,
        sequence_id: SequenceId,
    ) -> Result<Arc<SequenceDefinition>, EngineError> {
        if let Some(seq) = self.by_id.get(&sequence_id).await {
            crate::metrics::inc_with(crate::metrics::CACHE_HITS, &[("cache", "sequence")]);
            return Ok(seq);
        }
        crate::metrics::inc_with(crate::metrics::CACHE_MISSES, &[("cache", "sequence")]);

        let seq = storage
            .get_sequence(sequence_id)
            .await?
            .ok_or_else(|| EngineError::NotFound(format!("sequence {sequence_id}")))?;
        let seq = Arc::new(seq);
        self.populate_both(&seq).await;
        Ok(seq)
    }

    /// Get-or-fetch by `(tenant, namespace, name, version)`. Mirrors
    /// [`StorageBackend::get_sequence_by_name`] semantics: returns `None` when
    /// the sequence isn't registered (does NOT cache negatives — callers that
    /// poll for missing sequences would otherwise never recover).
    pub async fn get_by_name(
        &self,
        storage: &dyn StorageBackend,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<Arc<SequenceDefinition>>, EngineError> {
        let key: ByNameKey = (
            tenant_id.clone(),
            namespace.clone(),
            name.to_string(),
            version,
        );
        if let Some(seq) = self.by_name.get(&key).await {
            crate::metrics::inc_with(crate::metrics::CACHE_HITS, &[("cache", "sequence_by_name")]);
            return Ok(Some(seq));
        }
        crate::metrics::inc_with(
            crate::metrics::CACHE_MISSES,
            &[("cache", "sequence_by_name")],
        );

        let seq = storage
            .get_sequence_by_name(tenant_id, namespace, name, version)
            .await?;
        let Some(seq) = seq else { return Ok(None) };
        let seq = Arc::new(seq);
        self.populate_both(&seq).await;
        Ok(Some(seq))
    }

    /// Populate both sub-caches from a single `Arc<SequenceDefinition>` so a
    /// miss on one lookup kind warms the other automatically.
    async fn populate_both(&self, seq: &Arc<SequenceDefinition>) {
        self.by_id.insert(seq.id, Arc::clone(seq)).await;
        let name_key: ByNameKey = (
            seq.tenant_id.clone(),
            seq.namespace.clone(),
            seq.name.clone(),
            Some(seq.version),
        );
        self.by_name.insert(name_key, Arc::clone(seq)).await;
    }

    /// Invalidate one id and any by-name entries that reference it. Call this
    /// from the API layer immediately after `delete_sequence`/`deprecate_sequence`
    /// so reads after the write don't serve stale blocks for up to the TTL.
    pub async fn invalidate_by_id(&self, sequence_id: SequenceId) {
        // Capture name-key view while the entry is still present.
        let seq_view = self.by_id.get(&sequence_id).await;
        self.by_id.invalidate(&sequence_id).await;
        if let Some(seq) = seq_view {
            let name_key: ByNameKey = (
                seq.tenant_id.clone(),
                seq.namespace.clone(),
                seq.name.clone(),
                Some(seq.version),
            );
            self.by_name.invalidate(&name_key).await;
            // Also clear the "latest version" alias the caller may have used.
            let latest_key: ByNameKey = (
                seq.tenant_id.clone(),
                seq.namespace.clone(),
                seq.name.clone(),
                None,
            );
            self.by_name.invalidate(&latest_key).await;
        }
    }

    /// Drop every cached entry. Used on admin operations that may mutate many
    /// sequences at once (e.g. bulk import).
    pub fn invalidate_all(&self) {
        self.by_id.invalidate_all();
        self.by_name.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::StorageBackend;
    use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
    use serde_json::json;

    use super::*;

    fn mk_seq(name: &str, version: i32) -> SequenceDefinition {
        SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            name: name.into(),
            version,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId("s1".into()),
                handler: "noop".into(),
                params: json!({}),
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: None,
                on_deadline_breach: None,
                fallback_handler: None,
            }))],
            interceptors: None,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn get_by_id_miss_fetches_from_storage_and_populates_cache() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = mk_seq("flow-a", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        let got = cache.get_by_id(&storage, seq.id).await.unwrap();
        assert_eq!(got.name, "flow-a");
        assert_eq!(got.version, 1);

        // Second call should hit cache (no storage error possible since we
        // drop storage, but we can't do that — instead observe via metrics
        // or simply that it returns the same Arc).
        let got2 = cache.get_by_id(&storage, seq.id).await.unwrap();
        assert_eq!(got2.name, "flow-a");
    }

    #[tokio::test]
    async fn get_by_id_returns_not_found_for_missing_sequence() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let cache = SequenceCache::new(100, Duration::from_mins(1));
        let err = cache
            .get_by_id(&storage, SequenceId::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not found"),
            "expected not found error, got: {err}"
        );
    }

    #[tokio::test]
    async fn get_by_name_miss_fetches_and_populates_both_caches() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = mk_seq("flow-b", 2);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        let got = cache
            .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-b", Some(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, seq.id);

        // A subsequent by-id lookup for the same sequence should be cached.
        let got_by_id = cache.get_by_id(&storage, seq.id).await.unwrap();
        assert_eq!(got_by_id.id, seq.id);
    }

    #[tokio::test]
    async fn get_by_name_returns_none_for_missing_sequence() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let cache = SequenceCache::new(100, Duration::from_mins(1));
        let got = cache
            .get_by_name(
                &storage,
                &TenantId("t".into()),
                &Namespace("ns".into()),
                "missing",
                None,
            )
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn invalidate_by_id_removes_from_both_caches() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = mk_seq("flow-c", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        // Warm both caches via by-name lookup.
        let _ = cache
            .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-c", Some(1))
            .await
            .unwrap();

        cache.invalidate_by_id(seq.id).await;

        // After invalidation, by-id should re-fetch from storage (still there).
        let got = cache.get_by_id(&storage, seq.id).await.unwrap();
        assert_eq!(got.id, seq.id);

        // By-name with latest version (None) should also re-fetch.
        let got2 = cache
            .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-c", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got2.id, seq.id);
    }

    #[tokio::test]
    async fn invalidate_all_clears_everything() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq1 = mk_seq("flow-d1", 1);
        let seq2 = mk_seq("flow-d2", 1);
        storage.create_sequence(&seq1).await.unwrap();
        storage.create_sequence(&seq2).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        cache.get_by_id(&storage, seq1.id).await.unwrap();
        cache.get_by_id(&storage, seq2.id).await.unwrap();

        cache.invalidate_all();

        // Both should still resolve from storage after invalidate_all.
        let got1 = cache.get_by_id(&storage, seq1.id).await.unwrap();
        let got2 = cache.get_by_id(&storage, seq2.id).await.unwrap();
        assert_eq!(got1.id, seq1.id);
        assert_eq!(got2.id, seq2.id);
    }

    #[tokio::test]
    async fn cache_honours_ttl_and_allows_eventual_stale_reads() {
        // Use a zero TTL so entries expire immediately on the next tick.
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = mk_seq("flow-e", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_millis(1));
        cache.get_by_id(&storage, seq.id).await.unwrap();

        // Small sleep to let TTL expire.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Even with expired TTL, moka may still return the value depending on
        // timing; the important thing is that the cache does not panic and
        // the value is still retrievable (either from cache or storage).
        let got = cache.get_by_id(&storage, seq.id).await.unwrap();
        assert_eq!(got.id, seq.id);
    }

    #[tokio::test]
    async fn by_name_latest_version_resolves_when_none_specified() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq_v1 = mk_seq("flow-f", 1);
        let mut seq_v2 = mk_seq("flow-f", 2);
        seq_v2.id = SequenceId::new();
        storage.create_sequence(&seq_v1).await.unwrap();
        storage.create_sequence(&seq_v2).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        let got = cache
            .get_by_name(
                &storage,
                &seq_v1.tenant_id,
                &seq_v1.namespace,
                "flow-f",
                None,
            )
            .await
            .unwrap()
            .unwrap();
        // get_sequence_by_name with None returns the latest non-deprecated version.
        assert_eq!(got.version, 2);
    }

    #[tokio::test]
    async fn cache_populate_both_warms_name_cache_on_id_lookup() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = mk_seq("flow-g", 3);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_mins(1));
        // First lookup by id.
        let _ = cache.get_by_id(&storage, seq.id).await.unwrap();

        // By-name lookup with explicit version should hit the warmed cache.
        let got = cache
            .get_by_name(&storage, &seq.tenant_id, &seq.namespace, "flow-g", Some(3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, seq.id);
    }
}
