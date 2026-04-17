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
    let mut ref_keys: Vec<String> = Vec::new();
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

    // Phase 2: single batched fetch. Storage errors are swallowed and
    // logged; per-step resolution will see un-hydrated markers and either
    // succeed via the fallback single-fetch path or surface the error
    // contextually.
    let resolved = match storage.batch_get_externalized_state(&ref_keys).await {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!(error = %e, "preload batch fetch failed; markers left un-hydrated");
            return;
        }
    };

    if resolved.is_empty() {
        return;
    }

    // Phase 3: hydrate in place. Walks the same top-level keys the scan
    // visited; markers whose ref_key is absent from `resolved` stay intact.
    for inst in instances.iter_mut() {
        let Some(obj) = inst.context.data.as_object_mut() else {
            continue;
        };
        for value in obj.values_mut() {
            if let Some(k) = extract_ref_key(value) {
                if let Some(payload) = resolved.get(k) {
                    *value = payload.clone();
                }
            }
        }
    }
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

    #[tokio::test]
    async fn preload_hydrates_top_level_markers_in_place() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst_id = InstanceId::new();
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
        storage
            .save_externalized_state(InstanceId::new(), shared_ref, &shared_payload)
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
}
