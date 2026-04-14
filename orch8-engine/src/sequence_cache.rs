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

// Unit tests live alongside the scheduler integration — a focused test
// against `SequenceCache` would need a full mock `StorageBackend` which is
// unwieldy for three pass-through methods. The behaviour is covered by the
// existing e2e suite, which exercises both lookup paths under load.
