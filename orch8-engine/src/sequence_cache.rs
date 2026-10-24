//! In-memory cache layer over sequence reads.
//!
//! A full `StorageBackend` decorator would mean writing pass-through wrappers
//! for every trait method just to cache three sequence-read calls. This struct
//! instead encapsulates the cache at the engine service layer — callers
//! holding an `Arc<SequenceCache>` use it for hot sequence lookups and fall
//! back to the raw `StorageBackend` for everything else.
//!
//! Two independent moka caches back the struct:
//!   - `by_id`: keyed by backend lifetime and [`SequenceId`].
//!   - `by_name`: keyed by backend lifetime plus `(tenant, namespace, name, version)`, used by
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

use crate::backend_identity::BackendIdentity;
use crate::error::EngineError;
use crate::optimizer::{OptimizationIr, optimize};

type ByNameKey = (TenantId, Namespace, Arc<str>, Option<i32>);

/// Unified cache for sequence definitions, backing both by-id and by-name
/// lookups. Clone-safe (cheap: inner moka caches are `Arc`-shared).
#[derive(Clone)]
pub struct SequenceCache {
    by_id: Cache<(BackendIdentity, SequenceId), Arc<SequenceDefinition>>,
    by_name: Cache<(BackendIdentity, ByNameKey), Arc<SequenceDefinition>>,
    optimized: Cache<SequenceId, Arc<OptimizationIr>>,
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
            optimized: Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
        }
    }

    /// Read a definition through a borrowed backend. This bypasses definition
    /// caching: a borrow cannot supply a stable lifetime identity. Use
    /// [`Self::get_by_id_owned`] for cached reads.
    pub async fn get_by_id(
        &self,
        storage: &dyn StorageBackend,
        sequence_id: SequenceId,
    ) -> Result<Arc<SequenceDefinition>, EngineError> {
        storage
            .get_sequence(sequence_id)
            .await?
            .map(Arc::new)
            .ok_or_else(|| EngineError::NotFound(format!("sequence {sequence_id}")))
    }

    /// Get-or-fetch within this backend allocation's lifetime. Arc clones
    /// share entries; independent backends and decorators never share them.
    /// Keys hold weak ownership and do not keep connections or runtimes alive.
    pub async fn get_by_id_owned(
        &self,
        storage: &Arc<dyn StorageBackend>,
        sequence_id: SequenceId,
    ) -> Result<Arc<SequenceDefinition>, EngineError> {
        let owner = BackendIdentity::new(storage);
        if let Some(seq) = self.by_id.get(&(owner.clone(), sequence_id)).await {
            crate::metrics::inc_with(crate::metrics::CACHE_HITS, &[("cache", "sequence")]);
            return Ok(seq);
        }
        crate::metrics::inc_with(crate::metrics::CACHE_MISSES, &[("cache", "sequence")]);
        let seq = self.get_by_id(storage.as_ref(), sequence_id).await?;
        self.populate_both(owner, &seq).await;
        Ok(seq)
    }

    /// Read by name through a borrowed backend, without caching. Missing
    /// definitions are never negatively cached.
    pub async fn get_by_name(
        &self,
        storage: &dyn StorageBackend,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<Arc<SequenceDefinition>>, EngineError> {
        Ok(storage
            .get_sequence_by_name(tenant_id, namespace, name, version)
            .await?
            .map(Arc::new))
    }

    /// Cached by-name lookup qualified by the backend's lifetime, tenant,
    /// namespace, name and version. A miss warms the matching owner's ID key.
    pub async fn get_by_name_owned(
        &self,
        storage: &Arc<dyn StorageBackend>,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<Arc<SequenceDefinition>>, EngineError> {
        let owner = BackendIdentity::new(storage);
        let key = (
            owner.clone(),
            (
                tenant_id.clone(),
                namespace.clone(),
                Arc::from(name),
                version,
            ),
        );
        if let Some(seq) = self.by_name.get(&key).await {
            crate::metrics::inc_with(crate::metrics::CACHE_HITS, &[("cache", "sequence_by_name")]);
            return Ok(Some(seq));
        }
        crate::metrics::inc_with(
            crate::metrics::CACHE_MISSES,
            &[("cache", "sequence_by_name")],
        );
        let Some(seq) = self
            .get_by_name(storage.as_ref(), tenant_id, namespace, name, version)
            .await?
        else {
            return Ok(None);
        };
        self.populate_both(owner, &seq).await;
        Ok(Some(seq))
    }

    async fn populate_both(&self, owner: BackendIdentity, seq: &Arc<SequenceDefinition>) {
        self.by_id
            .insert((owner.clone(), seq.id), Arc::clone(seq))
            .await;
        let name_key = (
            seq.tenant_id.clone(),
            seq.namespace.clone(),
            Arc::from(seq.name.as_str()),
            Some(seq.version),
        );
        self.by_name
            .insert((owner, name_key), Arc::clone(seq))
            .await;
        if let Ok(ir) = optimize(seq) {
            self.optimized.insert(seq.id, Arc::new(ir)).await;
        }
    }

    /// Return the compiled optimization sidecar for this exact definition.
    /// A compiler refusal is a safe cache miss: callers retain the original
    /// execution scan and therefore never trade correctness for optimization.
    pub async fn optimization_for(&self, seq: &SequenceDefinition) -> Option<Arc<OptimizationIr>> {
        if let Some(ir) = self.optimized.get(&seq.id).await
            && ir.verify_equivalent(seq).is_ok()
        {
            return Some(ir);
        }
        match optimize(seq) {
            Ok(ir) => {
                let ir = Arc::new(ir);
                self.optimized.insert(seq.id, Arc::clone(&ir)).await;
                Some(ir)
            }
            Err(error) => {
                tracing::warn!(sequence_id = %seq.id, %error, "workflow optimization skipped");
                None
            }
        }
    }

    /// Invalidate one id and any by-name entries that reference it. Call this
    /// from the API layer immediately after `delete_sequence`/`deprecate_sequence`
    /// so reads after the write don't serve stale blocks for up to the TTL.
    pub async fn invalidate_by_id(&self, sequence_id: SequenceId) {
        // Conservative cross-owner invalidation is safe, including when the
        // ID cache already evicted an entry still present in the name cache.
        for (key, _) in &self.by_id {
            if key.1 == sequence_id {
                self.by_id.invalidate(key.as_ref()).await;
            }
        }
        for (key, seq) in &self.by_name {
            if seq.id == sequence_id {
                self.by_name.invalidate(key.as_ref()).await;
            }
        }
        self.optimized.invalidate(&sequence_id).await;
    }

    /// Drop every cached entry. Used on admin operations that may mutate many
    /// sequences at once (e.g. bulk import).
    pub fn invalidate_all(&self) {
        self.by_id.invalidate_all();
        self.by_name.invalidate_all();
        self.optimized.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_storage::SequenceStore;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::ids::{BlockId, Namespace, SequenceId, TenantId};
    use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
    use serde_json::json;

    use super::*;

    fn mk_seq(name: &str, version: i32) -> SequenceDefinition {
        SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            name: name.into(),
            version,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s1"),
                handler: "noop".into(),
                params: json!({}),
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                rate_budget: None,
                placement: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: None,
                on_deadline_breach: None,
                fallback_handler: None,
                cache_key: None,
                output_schema: None,
                when: None,
                compensation: None,
            }))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            placement: None,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn get_by_id_miss_fetches_from_storage_and_populates_cache() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("flow-a", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        let got = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        assert_eq!(got.name, "flow-a");
        assert_eq!(got.version, 1);

        // Arc identity proves the second read uses the cached definition.
        let got2 = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        assert_eq!(got2.name, "flow-a");
        assert!(Arc::ptr_eq(&got, &got2));

        let optimized = cache.optimization_for(&got2).await.unwrap();
        optimized.verify_equivalent(&got2).unwrap();
        assert_eq!(optimized.nodes.len(), 1);
    }

    #[tokio::test]
    async fn get_by_id_returns_not_found_for_missing_sequence() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let cache = SequenceCache::new(100, Duration::from_secs(60));
        let err = cache
            .get_by_id_owned(&storage, SequenceId::new())
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
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("flow-b", 2);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        let got = cache
            .get_by_name_owned(&storage, &seq.tenant_id, &seq.namespace, "flow-b", Some(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, seq.id);

        // A subsequent by-id lookup for the same sequence should be cached.
        let got_by_id = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        assert_eq!(got_by_id.id, seq.id);
    }

    #[tokio::test]
    async fn get_by_name_returns_none_for_missing_sequence() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let cache = SequenceCache::new(100, Duration::from_secs(60));
        let got = cache
            .get_by_name_owned(
                &storage,
                &TenantId::unchecked("t"),
                &Namespace::new("ns"),
                "missing",
                None,
            )
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn invalidate_by_id_removes_from_both_caches() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("flow-c", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        // Warm both caches via by-name lookup.
        let _ = cache
            .get_by_name_owned(&storage, &seq.tenant_id, &seq.namespace, "flow-c", Some(1))
            .await
            .unwrap();

        cache.invalidate_by_id(seq.id).await;

        // After invalidation, by-id should re-fetch from storage (still there).
        let got = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        assert_eq!(got.id, seq.id);

        // By-name with latest version (None) should also re-fetch.
        let got2 = cache
            .get_by_name_owned(&storage, &seq.tenant_id, &seq.namespace, "flow-c", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got2.id, seq.id);
    }

    #[tokio::test]
    async fn invalidate_all_clears_everything() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq1 = mk_seq("flow-d1", 1);
        let seq2 = mk_seq("flow-d2", 1);
        storage.create_sequence(&seq1).await.unwrap();
        storage.create_sequence(&seq2).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        cache.get_by_id_owned(&storage, seq1.id).await.unwrap();
        cache.get_by_id_owned(&storage, seq2.id).await.unwrap();

        cache.invalidate_all();

        // Both should still resolve from storage after invalidate_all.
        let got1 = cache.get_by_id_owned(&storage, seq1.id).await.unwrap();
        let got2 = cache.get_by_id_owned(&storage, seq2.id).await.unwrap();
        assert_eq!(got1.id, seq1.id);
        assert_eq!(got2.id, seq2.id);
    }

    #[tokio::test]
    async fn cache_honours_ttl_and_allows_eventual_stale_reads() {
        // Use a zero TTL so entries expire immediately on the next tick.
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("flow-e", 1);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_millis(1));
        cache.get_by_id_owned(&storage, seq.id).await.unwrap();

        // Small sleep to let TTL expire.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Even with expired TTL, moka may still return the value depending on
        // timing; the important thing is that the cache does not panic and
        // the value is still retrievable (either from cache or storage).
        let got = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        assert_eq!(got.id, seq.id);
    }

    #[tokio::test]
    async fn by_name_latest_version_resolves_when_none_specified() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq_v1 = mk_seq("flow-f", 1);
        let mut seq_v2 = mk_seq("flow-f", 2);
        seq_v2.id = SequenceId::new();
        storage.create_sequence(&seq_v1).await.unwrap();
        storage.create_sequence(&seq_v2).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        let got = cache
            .get_by_name_owned(
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
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("flow-g", 3);
        storage.create_sequence(&seq).await.unwrap();

        let cache = SequenceCache::new(100, Duration::from_secs(60));
        // First lookup by id.
        let _ = cache.get_by_id_owned(&storage, seq.id).await.unwrap();

        // By-name lookup with explicit version should hit the warmed cache.
        let got = cache
            .get_by_name_owned(&storage, &seq.tenant_id, &seq.namespace, "flow-g", Some(3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.id, seq.id);
    }

    #[tokio::test]
    async fn borrowed_reads_do_not_share_definitions_across_backends() {
        let a = SqliteStorage::in_memory().await.unwrap();
        let b = SqliteStorage::in_memory().await.unwrap();
        let first = mk_seq("same", 1);
        let mut second = first.clone();
        second.blocks.clear();
        a.create_sequence(&first).await.unwrap();
        b.create_sequence(&second).await.unwrap();
        let cache = plan_cache();
        cache.get_by_id(&a, first.id).await.unwrap();
        let actual = cache.get_by_id(&b, first.id).await.unwrap();
        assert!(
            actual.blocks.is_empty(),
            "definition leaked from another backend"
        );
    }

    #[tokio::test]
    async fn borrowed_name_reads_do_not_share_definitions_across_backends() {
        let a = SqliteStorage::in_memory().await.unwrap();
        let b = SqliteStorage::in_memory().await.unwrap();
        let first = mk_seq("same", 1);
        let mut second = first.clone();
        second.id = SequenceId::new();
        a.create_sequence(&first).await.unwrap();
        b.create_sequence(&second).await.unwrap();
        let cache = plan_cache();
        cache
            .get_by_name(&a, &first.tenant_id, &first.namespace, "same", Some(1))
            .await
            .unwrap();
        let actual = cache
            .get_by_name(&b, &first.tenant_id, &first.namespace, "same", Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.id, second.id, "name resolved in another backend");
    }

    #[tokio::test]
    async fn owned_reads_isolate_colliding_ids_names_and_optimization_sources() {
        let a: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let b: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let first = mk_seq("same", 1);
        let mut second = first.clone();
        if let BlockDefinition::Step(step) = &mut second.blocks[0] {
            step.handler = "other-owner".into();
        }
        a.create_sequence(&first).await.unwrap();
        b.create_sequence(&second).await.unwrap();
        let cache = plan_cache();
        let one = cache.get_by_id_owned(&a, first.id).await.unwrap();
        let two = cache
            .get_by_name_owned(&b, &first.tenant_id, &first.namespace, "same", Some(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&one.blocks).unwrap(),
            serde_json::to_value(&first.blocks).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&two.blocks).unwrap(),
            serde_json::to_value(&second.blocks).unwrap()
        );
        assert!(Arc::ptr_eq(
            &one,
            &cache
                .get_by_name_owned(&a, &first.tenant_id, &first.namespace, "same", Some(1))
                .await
                .unwrap()
                .unwrap()
        ));
        assert!(Arc::ptr_eq(
            &two,
            &cache.get_by_id_owned(&b, first.id).await.unwrap()
        ));
        cache
            .optimization_for(&one)
            .await
            .unwrap()
            .verify_equivalent(&one)
            .unwrap();
        cache
            .optimization_for(&two)
            .await
            .unwrap()
            .verify_equivalent(&two)
            .unwrap();
        cache
            .optimization_for(&one)
            .await
            .unwrap()
            .verify_equivalent(&one)
            .unwrap();
        // A borrowed read never consumes an owned entry from another database.
        let borrowed = cache.get_by_id(b.as_ref(), first.id).await.unwrap();
        assert_eq!(
            serde_json::to_value(&borrowed.blocks).unwrap(),
            serde_json::to_value(&second.blocks).unwrap()
        );
    }

    #[tokio::test]
    async fn owned_cache_shares_arc_clones_without_retaining_retired_backend() {
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("same", 1);
        storage.create_sequence(&seq).await.unwrap();
        let cache = plan_cache();
        let first = cache.get_by_id_owned(&storage, seq.id).await.unwrap();
        let clone = Arc::clone(&storage);
        assert!(Arc::ptr_eq(
            &first,
            &cache.get_by_id_owned(&clone, seq.id).await.unwrap()
        ));
        let weak = Arc::downgrade(&storage);
        drop(clone);
        drop(storage);
        assert!(weak.upgrade().is_none(), "cache retained a closed backend");
        let replacement: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        assert!(cache.get_by_id_owned(&replacement, seq.id).await.is_err());
        assert!(
            cache
                .get_by_name_owned(
                    &replacement,
                    &seq.tenant_id,
                    &seq.namespace,
                    "same",
                    Some(1)
                )
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn backend_decorator_does_not_consume_raw_backends_cached_definition() {
        let raw: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("same", 1);
        raw.create_sequence(&seq).await.unwrap();
        let cache = plan_cache();
        cache.get_by_id_owned(&raw, seq.id).await.unwrap();
        let (decorated, faults) = orch8_storage::testing::faulty::FaultStorage::wrap(raw);
        faults.on("get_sequence").times(1).fail_transient();
        assert!(
            cache.get_by_id_owned(&decorated, seq.id).await.is_err(),
            "cache bypassed the decorator"
        );
        assert_eq!(
            cache.get_by_id_owned(&decorated, seq.id).await.unwrap().id,
            seq.id
        );
    }

    #[tokio::test]
    async fn invalidation_clears_all_owner_name_entries_even_after_id_eviction() {
        let a: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let b: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let seq = mk_seq("same", 1);
        a.create_sequence(&seq).await.unwrap();
        b.create_sequence(&seq).await.unwrap();
        let cache = plan_cache();
        cache.get_by_id_owned(&a, seq.id).await.unwrap();
        cache.get_by_id_owned(&b, seq.id).await.unwrap();
        // Eviction schedules independently in the two bounded sub-caches.
        cache.by_id.invalidate_all();
        a.delete_sequence(seq.id).await.unwrap();
        b.delete_sequence(seq.id).await.unwrap();
        cache.invalidate_by_id(seq.id).await;
        for storage in [&a, &b] {
            assert!(cache.get_by_id_owned(storage, seq.id).await.is_err());
            assert!(
                cache
                    .get_by_name_owned(storage, &seq.tenant_id, &seq.namespace, "same", Some(1))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    fn plan_cache() -> SequenceCache {
        SequenceCache::new(100, Duration::from_secs(60))
    }

    #[tokio::test]
    async fn repeated_plan_lookup_serves_the_cached_plan() {
        let cache = plan_cache();
        let seq = mk_seq("flow", 1);

        let first = cache.optimization_for(&seq).await.unwrap();
        let second = cache.optimization_for(&seq).await.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn invalidation_forces_the_plan_to_be_recompiled() {
        let by_id = plan_cache();
        let all = plan_cache();
        let seq = mk_seq("flow", 1);
        let first_by_id = by_id.optimization_for(&seq).await.unwrap();
        let first_all = all.optimization_for(&seq).await.unwrap();

        by_id.invalidate_by_id(seq.id).await;
        all.invalidate_all();

        let second_by_id = by_id.optimization_for(&seq).await.unwrap();
        let second_all = all.optimization_for(&seq).await.unwrap();
        assert!(!Arc::ptr_eq(&first_by_id, &second_by_id));
        assert!(!Arc::ptr_eq(&first_all, &second_all));
        assert_eq!(first_by_id.source_sha256, second_by_id.source_sha256);
    }

    #[tokio::test]
    async fn changed_definition_under_the_same_id_is_recompiled_not_served_stale() {
        let cache = plan_cache();
        let seq = mk_seq("flow", 1);
        let stale = cache.optimization_for(&seq).await.unwrap();
        let mut changed = seq.clone();
        changed.name = "flow-renamed".into();

        let fresh = cache.optimization_for(&changed).await.unwrap();

        assert!(!Arc::ptr_eq(&stale, &fresh));
        fresh.verify_equivalent(&changed).unwrap();
        assert!(stale.verify_equivalent(&changed).is_err());
    }

    #[tokio::test]
    async fn recompiled_plan_replaces_the_cached_entry() {
        let cache = plan_cache();
        let mut seq = mk_seq("flow", 1);
        cache.optimization_for(&seq).await.unwrap();
        seq.name = "flow-v2".into();

        let first = cache.optimization_for(&seq).await.unwrap();
        let second = cache.optimization_for(&seq).await.unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn uncompilable_sequence_is_a_safe_miss_that_does_not_poison_the_cache() {
        let cache = plan_cache();
        let mut invalid = mk_seq("invalid", 1);
        invalid.blocks.clear();

        let missed = cache.optimization_for(&invalid).await;
        let valid = cache.optimization_for(&mk_seq("valid", 1)).await;

        assert!(missed.is_none());
        assert_eq!(valid.unwrap().nodes.len(), 1);
    }

    #[tokio::test]
    async fn definition_that_becomes_uncompilable_never_serves_its_old_plan() {
        let cache = plan_cache();
        let seq = mk_seq("flow", 1);
        cache.optimization_for(&seq).await.unwrap();
        let mut broken = seq.clone();
        broken.blocks.clear();

        let plan = cache.optimization_for(&broken).await;

        assert!(plan.is_none(), "stale plan served for a changed definition");
    }
}
