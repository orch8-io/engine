//! Batch preload of externalized `context.data` payloads for a tick's
//! claimed instances.
//!
//! Without this hook, each step handler that touches an externalized field
//! triggers a separate `get_externalized_state` call — an N+1 per tick.
//! The preload collects every top-level marker across every claimed
//! instance, issues one `batch_get_externalized_state`, and hydrates the
//! markers in place so downstream resolution (`handlers::step_block::
//! resolve_markers`) observes already-inflated values and becomes a no-op.
//!
//! Only top-level `context.data` keys can be externalized (see
//! `docs/CONTEXT_MANAGEMENT.md` §8.5), so the scan is shallow by design.
//!
//! Contract:
//! - Instances with no markers contribute nothing to the batched fetch.
//! - Missing payloads leave markers intact so a later resolver can detect
//!   the broken reference (same semantics as the single-item
//!   `resolve_markers` path).
//! - Storage errors are non-fatal: hydration is a best-effort optimization
//!   — if the batch fetch fails, markers remain and the per-step resolver
//!   will retry (and surface the error in its own context).

use orch8_storage::StorageBackend;
use orch8_types::instance::TaskInstance;

use crate::externalized::extract_ref_key;
use crate::metrics;

/// Hydrate every top-level externalized marker present in the given
/// instances' `context.data`, via a single [`StorageBackend::
/// batch_get_externalized_state`] call.
///
/// Mutates each instance's `context.data` in place, replacing marker
/// objects with their resolved payloads. Markers whose `ref_key` fails
/// to resolve are left untouched.
///
/// This function is intentionally tolerant of storage errors — the
/// preload is a throughput optimization, not a correctness boundary.
/// If the batch fetch fails, the function returns without mutating
/// instances and the caller continues as if the preload never ran.
#[tracing::instrument(skip_all, fields(instance_count = instances.len(), ref_count = tracing::field::Empty))]
pub async fn preload_externalized_markers(
    storage: &dyn StorageBackend,
    instances: &mut [TaskInstance],
) {
    // Phase 1: scan every claimed instance for top-level marker ref_keys.
    // Dedup globally so the same payload referenced by multiple instances
    // (fan-in pattern) is fetched once.
    // Upper-bound capacity: every instance could have every top-level field as a
    // ref key. In practice ref keys are sparse, so this is a generous reserve.
    let mut ref_keys: Vec<String> = Vec::with_capacity(instances.len() * 4);
    for inst in instances.iter() {
        if let Some(obj) = inst.context.data.as_object() {
            for value in obj.values() {
                if let Some(k) = extract_ref_key(value) {
                    ref_keys.push(k.to_string());
                }
            }
        }
    }

    if ref_keys.is_empty() {
        return;
    }

    ref_keys.sort_unstable();
    ref_keys.dedup();

    tracing::Span::current().record("ref_count", ref_keys.len());
    metrics::inc_by(metrics::PRELOAD_REFS_SCANNED, ref_keys.len() as u64);

    // Phase 2: single batched fetch. Storage errors are swallowed and
    // logged; per-step resolution will see un-hydrated markers and either
    // succeed via the fallback single-fetch path or surface the error
    // contextually.
    //
    // Timer records only on the success path. On failure we `mem::forget`
    // it so error-path durations don't pollute the success histogram's
    // p50/p99 — error count lives on its own counter.
    let timer = metrics::Timer::start(metrics::PRELOAD_BATCH_DURATION);
    let resolved = match storage.batch_get_externalized_state(&ref_keys).await {
        Ok(map) => map,
        Err(e) => {
            std::mem::forget(timer);
            tracing::warn!(error = %e, "preload batch fetch failed; markers left un-hydrated");
            metrics::inc(metrics::PRELOAD_ERRORS);
            return;
        }
    };
    drop(timer);

    if resolved.is_empty() {
        return;
    }

    // Phase 3: hydrate in place. Walks the same top-level keys the scan
    // visited; markers whose ref_key is absent from `resolved` stay intact.
    //
    // Two distinct metrics:
    //   - REFS_HYDRATED: unique refs resolved (comparable against REFS_SCANNED
    //     for a hit-rate ratio).
    //   - SLOTS_HYDRATED: total marker slots mutated (fan-in patterns inflate
    //     this relative to REFS_HYDRATED; useful for measuring work done).
    let mut slots_hydrated: u64 = 0;
    for inst in instances.iter_mut() {
        let Some(obj) = inst.context.data.as_object_mut() else {
            continue;
        };
        for value in obj.values_mut() {
            // Resolve the payload first — `extract_ref_key` returns `&str`
            // borrowed from `value`, so the whole lookup must complete
            // (and the borrow must end) before `*value = ...` can execute.
            // Cloning into an `Option<Value>` severs the borrow explicitly
            // so the mutation below cannot accidentally grow a dependency
            // on `extract_ref_key`'s return lifetime.
            let payload = extract_ref_key(value)
                .and_then(|k| resolved.get(k))
                .cloned();
            if let Some(payload) = payload {
                *value = payload;
                slots_hydrated += 1;
            }
        }
    }
    metrics::inc_by(metrics::PRELOAD_REFS_HYDRATED, resolved.len() as u64);
    metrics::inc_by(metrics::PRELOAD_SLOTS_HYDRATED, slots_hydrated);
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use orch8_storage::StorageBackend;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use serde_json::json;

    use super::*;

    fn mk_instance(data: serde_json::Value) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data,
                config: json!({}),
                audit: vec![],
                runtime: orch8_types::context::RuntimeContext::default(),
            },
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn marker(ref_key: &str) -> serde_json::Value {
        json!({ "_externalized": true, "_ref": ref_key })
    }

    /// Seed the parent `task_instances` row required by the FK on
    /// `externalized_state.instance_id` (enforced in `SQLite` since M4).
    async fn seed_instance<S: StorageBackend>(storage: &S, id: InstanceId) {
        let mut inst = mk_instance(json!({}));
        inst.id = id;
        storage.create_instance(&inst).await.unwrap();
    }

    #[tokio::test]
    async fn preload_hydrates_top_level_markers_in_place() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&storage, inst_id).await;
        let ref_k = format!("{}:ctx:data:big", inst_id.0);
        let payload = json!({ "huge": "x".repeat(100) });
        storage
            .save_externalized_state(inst_id, &ref_k, &payload)
            .await
            .unwrap();

        let mut inst = mk_instance(json!({
            "small": "keep",
            "big": marker(&ref_k),
        }));
        inst.id = inst_id;

        let mut batch = vec![inst];
        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(batch[0].context.data["small"], json!("keep"));
        assert_eq!(batch[0].context.data["big"], payload);
    }

    #[tokio::test]
    async fn preload_leaves_unresolved_markers_intact() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let broken = marker("nonexistent:ref");
        let mut batch = vec![mk_instance(json!({ "missing": broken.clone() }))];

        preload_externalized_markers(&storage, &mut batch).await;

        // Still a marker — preload did not invent data.
        assert_eq!(batch[0].context.data["missing"], broken);
    }

    #[tokio::test]
    async fn preload_is_noop_when_no_markers() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut batch = vec![mk_instance(json!({ "a": 1, "b": "two" }))];

        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(batch[0].context.data, json!({ "a": 1, "b": "two" }));
    }

    #[tokio::test]
    async fn preload_is_noop_for_empty_batch() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut batch: Vec<TaskInstance> = Vec::new();
        // Must not panic, must not make storage calls that matter.
        preload_externalized_markers(&storage, &mut batch).await;
    }

    #[tokio::test]
    async fn preload_dedupes_shared_refs_across_instances() {
        // Fan-in pattern: two instances reference the same externalized
        // payload. The batch fetch should still hydrate both.
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let shared_ref = "shared:ctx:data:payload";
        let shared_payload = json!({ "shared": true });
        let owner = InstanceId::new();
        seed_instance(&storage, owner).await;
        storage
            .save_externalized_state(owner, shared_ref, &shared_payload)
            .await
            .unwrap();

        let mut batch = vec![
            mk_instance(json!({ "x": marker(shared_ref) })),
            mk_instance(json!({ "x": marker(shared_ref) })),
        ];

        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(batch[0].context.data["x"], shared_payload);
        assert_eq!(batch[1].context.data["x"], shared_payload);
    }

    #[tokio::test]
    async fn preload_ignores_non_object_data() {
        // If context.data happens to be a scalar (unusual but valid JSON),
        // preload must not panic.
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut batch = vec![mk_instance(json!("scalar_data"))];

        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(batch[0].context.data, json!("scalar_data"));
    }

    /// Top-level-only invariant: nested markers (one level inside a plain
    /// object field) MUST NOT be hydrated — the save site never produces
    /// them, and the preload scan deliberately walks only top-level keys.
    /// If this test ever hydrates the nested marker, the hydration scope
    /// has silently grown and the documented §8.5 contract is broken.
    #[tokio::test]
    async fn preload_does_not_hydrate_nested_markers() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&storage, inst_id).await;
        let ref_k = format!("{}:ctx:data:nested", inst_id.0);
        let payload = json!({ "secret": "should_not_appear" });
        storage
            .save_externalized_state(inst_id, &ref_k, &payload)
            .await
            .unwrap();

        let nested_marker = marker(&ref_k);
        let mut inst = mk_instance(json!({
            "outer": {
                "inner": nested_marker.clone(),
            },
        }));
        inst.id = inst_id;

        let mut batch = vec![inst];
        preload_externalized_markers(&storage, &mut batch).await;

        // The nested marker must remain untouched — preload only walks
        // the top level. The payload from storage must NOT leak through.
        assert_eq!(
            batch[0].context.data["outer"]["inner"], nested_marker,
            "nested marker must not be hydrated; only top-level keys are externalizable"
        );
    }

    /// Partial-hit scenario: one marker resolves, one doesn't. Verifies the
    /// hydration loop continues past a missing key and mutates only the
    /// keys it can satisfy. This is the path the `SLOTS_HYDRATED` vs
    /// `REFS_HYDRATED` split metric distinguishes — if the loop ever
    /// short-circuited on first miss, the slot count would be wrong.
    #[tokio::test]
    async fn preload_partial_hit_hydrates_resolvable_leaves_others() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&storage, inst_id).await;
        let ref_ok = format!("{}:ctx:data:ok", inst_id.0);
        let payload = json!({"data": "resolved"});
        storage
            .save_externalized_state(inst_id, &ref_ok, &payload)
            .await
            .unwrap();

        let missing_marker = marker("nonexistent:missing");
        let mut inst = mk_instance(json!({
            "ok": marker(&ref_ok),
            "missing": missing_marker.clone(),
            "plain": "untouched",
        }));
        inst.id = inst_id;

        let mut batch = vec![inst];
        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(
            batch[0].context.data["ok"], payload,
            "resolvable marker should hydrate"
        );
        assert_eq!(
            batch[0].context.data["missing"], missing_marker,
            "unresolvable marker must stay intact for downstream error reporting"
        );
        assert_eq!(batch[0].context.data["plain"], json!("untouched"));
    }

    /// Multiple top-level markers in one instance: every top-level key that
    /// carries a marker must be hydrated in the same pass. Regression guard
    /// for a future refactor that accidentally breaks out of the inner loop
    /// after the first hit.
    #[tokio::test]
    async fn preload_hydrates_multiple_top_level_markers() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&storage, inst_id).await;
        let ref_a = format!("{}:ctx:data:a", inst_id.0);
        let ref_b = format!("{}:ctx:data:b", inst_id.0);
        let payload_a = json!({"id": "A"});
        let payload_b = json!({"id": "B"});
        storage
            .save_externalized_state(inst_id, &ref_a, &payload_a)
            .await
            .unwrap();
        storage
            .save_externalized_state(inst_id, &ref_b, &payload_b)
            .await
            .unwrap();

        let mut inst = mk_instance(json!({
            "a": marker(&ref_a),
            "b": marker(&ref_b),
        }));
        inst.id = inst_id;

        let mut batch = vec![inst];
        preload_externalized_markers(&storage, &mut batch).await;

        assert_eq!(batch[0].context.data["a"], payload_a);
        assert_eq!(batch[0].context.data["b"], payload_b);
    }

    /// After the hydration loop's borrow-pattern fix (MEDIUM #8), the
    /// mutation path no longer depends on NLL extending the borrow of
    /// `value`. This test exercises the exact shape — a marker at the same
    /// key that is then mutated — to catch any regression that reintroduces
    /// a borrow-over-mutation compile hazard.
    #[tokio::test]
    async fn preload_hydration_mutates_in_place_without_borrow_regression() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
        seed_instance(&storage, inst_id).await;
        let ref_k = format!("{}:ctx:data:field", inst_id.0);
        let payload = json!({ "huge": "x".repeat(1024) });
        storage
            .save_externalized_state(inst_id, &ref_k, &payload)
            .await
            .unwrap();

        let mut inst = mk_instance(json!({ "field": marker(&ref_k) }));
        inst.id = inst_id;
        let mut batch = vec![inst];
        preload_externalized_markers(&storage, &mut batch).await;

        // Value was replaced by the hydrated payload — not left as marker,
        // not corrupted, not duplicated into a new key.
        let obj = batch[0]
            .context
            .data
            .as_object()
            .expect("data must remain an object");
        assert_eq!(obj.len(), 1, "hydration must not invent keys");
        assert_eq!(obj["field"], payload);
    }
}
