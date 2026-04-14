//! `emit_event` builtin: fire an event trigger → spawn a new workflow instance.
//!
//! Same-tenant only. Cross-tenant attempts return `Permanent`. The caller's
//! instance id is captured in the child's metadata as `parent_instance_id`.
//!
//! Supports an optional `dedupe_key` param with a selectable `dedupe_scope`
//! (R7). Scope options:
//! - `"parent"` (default) — dedupe identity is `(parent_instance_id,
//!   dedupe_key)`. Idempotent retry within a single workflow run.
//! - `"tenant"` — dedupe identity is `(tenant_id, dedupe_key)`. Tenant-wide
//!   "at-most-once" fan-out across every caller in the tenant. The tenant is
//!   always the caller's own (`ctx.tenant_id`); cross-tenant dedupe is not
//!   representable.
//!
//! When present, the dedupe row and child instance are written in a single
//! transaction via `storage.create_instance_with_dedupe`: the first call
//! creates both rows atomically; any subsequent call with the same scope+key
//! returns the existing child id and `deduped: true` without creating a new
//! instance. This closes the orphan window described in architectural
//! finding #2 — a crash mid-operation cannot leave a dedupe row pointing at
//! a non-existent child.

use serde_json::{json, Map, Value};

use orch8_storage::{DedupeScope, EmitDedupeOutcome};
use orch8_types::{error::StepError, ids::InstanceId};

use super::util::{check_same_tenant, map_storage_err, permanent};
use super::StepContext;

/// Lightweight wire-side enum for the `dedupe_scope` param. Kept private so
/// we parse exactly twice (match on string, then build `DedupeScope` once we
/// also have the `tenant_id` / `instance_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DedupeScopeKind {
    Parent,
    Tenant,
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn handle_emit_event(ctx: StepContext) -> Result<Value, StepError> {
    let trigger_slug = ctx
        .params
        .get("trigger_slug")
        .and_then(|v| v.as_str())
        .ok_or_else(|| permanent("missing 'trigger_slug' string param"))?
        .to_string();

    let data = ctx.params.get("data").cloned().unwrap_or_else(|| json!({}));

    let user_meta = ctx.params.get("meta").cloned().unwrap_or_else(|| json!({}));

    // Optional dedupe key. If the caller passes the key at all, it must be a
    // non-empty string — an empty key would create a scope-wide global lock
    // which is almost certainly a bug, so reject explicitly as Permanent.
    let dedupe_key: Option<String> = match ctx.params.get("dedupe_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            if s.is_empty() {
                return Err(permanent("'dedupe_key' cannot be empty"));
            }
            Some(s.clone())
        }
        Some(_) => {
            return Err(permanent("'dedupe_key' must be a string"));
        }
    };

    // Optional dedupe scope — selects the key's namespace. Defaults to
    // "parent" so pre-R7 callers keep their existing per-parent behavior.
    // We reject unknown scope strings as Permanent (typo in the workflow
    // config should fail loudly, not silently fall back to a different
    // namespace). Only matters when a dedupe_key is present; we still parse
    // so a bare `dedupe_scope` without `dedupe_key` surfaces a config error.
    let dedupe_scope_str: &str = match ctx.params.get("dedupe_scope") {
        None | Some(Value::Null) => "parent",
        Some(Value::String(s)) => s.as_str(),
        Some(_) => {
            return Err(permanent("'dedupe_scope' must be a string"));
        }
    };
    let dedupe_scope_kind = match dedupe_scope_str {
        "parent" => DedupeScopeKind::Parent,
        "tenant" => DedupeScopeKind::Tenant,
        other => {
            return Err(permanent(format!(
                "invalid dedupe_scope: '{other}' (expected 'parent' or 'tenant')"
            )));
        }
    };

    let storage = ctx.storage.as_ref();

    let trigger = storage
        .get_trigger(&trigger_slug)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| permanent(format!("trigger '{trigger_slug}' not found")))?;

    if !trigger.enabled {
        return Err(permanent(format!("trigger '{trigger_slug}' is disabled")));
    }

    // Cross-tenant guard: caller's tenant comes from StepContext; the trigger's
    // tenant is now a TenantId newtype — compare directly.
    check_same_tenant(&ctx.tenant_id, &trigger.tenant_id, "emit_event")?;

    // Build meta: start from user-provided meta, then overwrite system-set
    // fields so callers can't spoof `source` / `parent_instance_id`.
    let mut meta_obj: Map<String, Value> = match user_meta {
        Value::Object(m) => m,
        Value::Null => Map::new(),
        other => {
            let mut m = Map::new();
            m.insert("user_meta".into(), other);
            m
        }
    };
    meta_obj.insert("source".into(), Value::String("emit_event".into()));
    meta_obj.insert(
        "parent_instance_id".into(),
        Value::String(ctx.instance_id.0.to_string()),
    );
    let meta_with_source = Value::Object(meta_obj);

    let candidate_child_id = InstanceId::new();
    let sequence_name = trigger.sequence_name.clone();

    // Dedupe branch: record (scope, key) AND create the child instance in a
    // single transaction via `create_instance_with_dedupe`. Closes the orphan
    // window where the dedupe row and the child used to be separate writes
    // (finding #2). The child is built in memory first so the storage layer
    // can insert both rows atomically.
    if let Some(key) = dedupe_key.as_deref() {
        let sequence = crate::triggers::resolve_trigger_sequence(storage, &trigger)
            .await
            .map_err(|e| permanent(format!("failed to resolve trigger sequence: {e}")))?;
        let instance = crate::triggers::build_trigger_instance(
            &trigger,
            sequence.id,
            data,
            meta_with_source,
            Some(candidate_child_id),
        );
        // Tenant scope always derives from the caller's own tenant
        // (StepContext); a workflow cannot punch into another tenant's
        // dedupe namespace by design.
        let scope = match dedupe_scope_kind {
            DedupeScopeKind::Parent => DedupeScope::Parent(ctx.instance_id),
            DedupeScopeKind::Tenant => DedupeScope::Tenant(ctx.tenant_id.clone()),
        };
        let outcome = storage
            .create_instance_with_dedupe(&scope, key, &instance)
            .await
            .map_err(|e| map_storage_err(&e))?;
        return Ok(match outcome {
            EmitDedupeOutcome::AlreadyExists(existing_id) => json!({
                "instance_id": existing_id.0.to_string(),
                "sequence_name": sequence_name,
                "deduped": true,
            }),
            EmitDedupeOutcome::Inserted => json!({
                "instance_id": candidate_child_id.0.to_string(),
                "sequence_name": sequence_name,
                "deduped": false,
            }),
        });
    }

    // Non-dedupe path: unchanged single-insert create.
    crate::triggers::create_trigger_instance(
        storage,
        &trigger,
        data,
        meta_with_source,
        Some(candidate_child_id),
    )
    .await
    .map_err(|e| permanent(format!("failed to create child instance: {e}")))?;

    Ok(json!({
        "instance_id": candidate_child_id.0.to_string(),
        "sequence_name": sequence_name,
        "deduped": false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        ids::{BlockId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
        sequence::SequenceDefinition,
        trigger::{TriggerDef, TriggerType},
    };
    use serde_json::json;
    use std::sync::Arc;

    fn mk_instance(tenant: &str, state: InstanceState) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId(tenant.into()),
            namespace: Namespace("default".into()),
            state,
            next_fire_at: Some(now),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: json!({}),
            context: ExecutionContext {
                data: json!({}),
                config: json!({}),
                audit: vec![],
                runtime: RuntimeContext::default(),
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

    async fn seed_sequence(storage: &SqliteStorage, tenant: &str, name: &str) -> SequenceId {
        let id = SequenceId::new();
        let seq = SequenceDefinition {
            id,
            tenant_id: TenantId(tenant.into()),
            namespace: Namespace("default".into()),
            name: name.into(),
            version: 1,
            deprecated: false,
            blocks: vec![],
            interceptors: None,
            created_at: Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
        id
    }

    fn mk_trigger(slug: &str, tenant: &str, seq_name: &str) -> TriggerDef {
        let now = Utc::now();
        TriggerDef {
            slug: slug.into(),
            sequence_name: seq_name.into(),
            version: None,
            tenant_id: TenantId(tenant.into()),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Event,
            config: Value::Null,
            created_at: now,
            updated_at: now,
        }
    }

    fn mk_ctx(
        caller: &TaskInstance,
        storage: Arc<dyn StorageBackend>,
        params: Value,
    ) -> StepContext {
        StepContext {
            instance_id: caller.id,
            tenant_id: caller.tenant_id.clone(),
            block_id: BlockId("emit".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 1,
            storage,
            wait_for_input: None,
        }
    }

    #[tokio::test]
    async fn emit_event_creates_child_instance_for_same_tenant() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "data": {"order_id": 42},
                "meta": {"source": "spoofed", "custom": "value"},
            }),
        );
        let result = handle_emit_event(ctx).await.unwrap();

        let child_id_str = result
            .get("instance_id")
            .and_then(|v| v.as_str())
            .expect("instance_id in response");
        assert_eq!(
            result.get("sequence_name").and_then(|v| v.as_str()),
            Some("child_seq")
        );
        assert_eq!(result.get("deduped"), Some(&json!(false)));

        let child_uuid = uuid::Uuid::parse_str(child_id_str).unwrap();
        let child = storage
            .get_instance(InstanceId(child_uuid))
            .await
            .unwrap()
            .expect("child instance exists");
        assert_eq!(child.tenant_id.0, "T1");
        // The child's _trigger_event meta should contain our system-set fields,
        // with "source" overwritten by the handler (not "spoofed").
        let event_meta = &child.metadata["_trigger_event"];
        assert_eq!(event_meta["source"], json!("emit_event"));
        assert_eq!(
            event_meta["parent_instance_id"],
            json!(caller.id.0.to_string())
        );
        assert_eq!(event_meta["custom"], json!("value"));
        // Payload preserved.
        assert_eq!(child.context.data, json!({"order_id": 42}));
    }

    #[tokio::test]
    async fn emit_event_rejects_when_trigger_not_found() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "does-not-exist",
            }),
        );
        let err = handle_emit_event(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn emit_event_rejects_when_trigger_disabled() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let mut trigger = mk_trigger("on-order", "T1", "child_seq");
        trigger.enabled = false;
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
            }),
        );
        let err = handle_emit_event(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[tokio::test]
    async fn emit_event_denies_cross_tenant() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T2", "child_seq").await;
        let trigger = mk_trigger("on-order", "T2", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "data": {"order_id": 42},
            }),
        );
        let err = handle_emit_event(ctx).await.unwrap_err();
        match &err {
            StepError::Permanent { message, .. } => {
                assert!(
                    message.contains("cross-tenant"),
                    "expected 'cross-tenant' in message, got: {message}"
                );
            }
            other @ StepError::Retryable { .. } => {
                panic!("expected Permanent error, got: {other:?}")
            }
        }

        // Verify no child instance was created in either tenant (no leakage).
        let filter_t1 = orch8_types::filter::InstanceFilter {
            tenant_id: Some(TenantId("T1".into())),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination::default();
        let instances_t1 = storage
            .list_instances(&filter_t1, &pagination)
            .await
            .unwrap();
        // Only the caller should exist in T1.
        assert_eq!(instances_t1.len(), 1);
        assert_eq!(instances_t1[0].id, caller.id);
        let filter_t2 = orch8_types::filter::InstanceFilter {
            tenant_id: Some(TenantId("T2".into())),
            ..Default::default()
        };
        let instances_t2 = storage
            .list_instances(&filter_t2, &pagination)
            .await
            .unwrap();
        assert!(
            instances_t2.is_empty(),
            "no child instance should exist in T2"
        );
    }

    #[tokio::test]
    async fn emit_event_dedupe_first_call_creates_child() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "data": {"order_id": 1},
                "dedupe_key": "order-1",
            }),
        );
        let result = handle_emit_event(ctx).await.unwrap();

        assert_eq!(result.get("deduped"), Some(&json!(false)));
        let child_id_str = result
            .get("instance_id")
            .and_then(|v| v.as_str())
            .expect("instance_id in response");
        let child_uuid = uuid::Uuid::parse_str(child_id_str).unwrap();
        let child = storage
            .get_instance(InstanceId(child_uuid))
            .await
            .unwrap()
            .expect("child instance exists");
        assert_eq!(child.tenant_id.0, "T1");
    }

    #[tokio::test]
    async fn emit_event_dedupe_second_call_returns_existing() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let make = || {
            mk_ctx(
                &caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "data": {"order_id": 1},
                    "dedupe_key": "order-1",
                }),
            )
        };

        let first = handle_emit_event(make()).await.unwrap();
        assert_eq!(first.get("deduped"), Some(&json!(false)));
        let first_id = first
            .get("instance_id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();

        let second = handle_emit_event(make()).await.unwrap();
        assert_eq!(second.get("deduped"), Some(&json!(true)));
        let second_id = second
            .get("instance_id")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        assert_eq!(first_id, second_id, "second call should return same id");
        assert_eq!(
            second.get("sequence_name").and_then(|v| v.as_str()),
            Some("child_seq")
        );

        // Only ONE child instance exists in storage (caller + one child = 2).
        let filter = orch8_types::filter::InstanceFilter {
            tenant_id: Some(TenantId("T1".into())),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination::default();
        let instances = storage.list_instances(&filter, &pagination).await.unwrap();
        assert_eq!(
            instances.len(),
            2,
            "expected caller + exactly one child instance"
        );
    }

    #[tokio::test]
    async fn emit_event_dedupe_isolates_by_parent() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller_a = mk_instance("T1", InstanceState::Running);
        let caller_b = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller_a).await.unwrap();
        storage.create_instance(&caller_b).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let make = |caller: &TaskInstance| {
            mk_ctx(
                caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "dedupe_key": "shared-key",
                }),
            )
        };

        let ra = handle_emit_event(make(&caller_a)).await.unwrap();
        let rb = handle_emit_event(make(&caller_b)).await.unwrap();

        assert_eq!(ra.get("deduped"), Some(&json!(false)));
        assert_eq!(rb.get("deduped"), Some(&json!(false)));

        let id_a = ra.get("instance_id").and_then(|v| v.as_str()).unwrap();
        let id_b = rb.get("instance_id").and_then(|v| v.as_str()).unwrap();
        assert_ne!(
            id_a, id_b,
            "different parents with same key must produce distinct children"
        );

        // Two callers + two distinct children in T1 = 4 instances.
        let filter = orch8_types::filter::InstanceFilter {
            tenant_id: Some(TenantId("T1".into())),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination::default();
        let instances = storage.list_instances(&filter, &pagination).await.unwrap();
        assert_eq!(instances.len(), 4);
    }

    /// R4 / finding #2: dedupe insert and child instance creation must be
    /// atomic. Storage invariant: for every response carrying an
    /// `instance_id` the corresponding row must exist in `task_instances`.
    /// This is the observable consequence of
    /// `StorageBackend::create_instance_with_dedupe` committing both rows in
    /// one transaction (no orphan window).
    #[tokio::test]
    async fn emit_event_dedupe_atomic_instance_exists_after_insert() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "dedupe_key": "atomic-check",
            }),
        );

        let result = handle_emit_event(ctx).await.unwrap();
        assert_eq!(result.get("deduped"), Some(&json!(false)));
        let child_id_str = result
            .get("instance_id")
            .and_then(|v| v.as_str())
            .expect("instance_id present");
        let child_uuid = uuid::Uuid::parse_str(child_id_str).unwrap();

        // Invariant: the returned child id MUST resolve in storage. Before
        // the atomic method existed, a crash between the dedupe insert and
        // the instance create could leave this lookup returning None while
        // the dedupe row still pointed at the id.
        let fetched = storage.get_instance(InstanceId(child_uuid)).await.unwrap();
        assert!(
            fetched.is_some(),
            "dedupe Inserted must imply child instance persisted (finding #2)"
        );
    }

    #[tokio::test]
    async fn emit_event_rejects_missing_trigger_slug_param() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(&caller, storage_dyn, json!({}));
        let err = handle_emit_event(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    // === R7: dedupe_scope param ============================================

    /// Tenant scope: two DIFFERENT parents in the SAME tenant using the same
    /// dedupe key → second call is deduped to the first child. Proves
    /// tenant-wide at-most-once.
    #[tokio::test]
    async fn emit_event_dedupe_tenant_scope_dedupes_across_parents() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller_a = mk_instance("T1", InstanceState::Running);
        let caller_b = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller_a).await.unwrap();
        storage.create_instance(&caller_b).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let make = |caller: &TaskInstance| {
            mk_ctx(
                caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "dedupe_key": "welcome-1",
                    "dedupe_scope": "tenant",
                }),
            )
        };

        let ra = handle_emit_event(make(&caller_a)).await.unwrap();
        let rb = handle_emit_event(make(&caller_b)).await.unwrap();

        assert_eq!(ra.get("deduped"), Some(&json!(false)));
        assert_eq!(rb.get("deduped"), Some(&json!(true)));
        assert_eq!(
            ra.get("instance_id"),
            rb.get("instance_id"),
            "tenant-scope dedupe must return the same child id across parents"
        );

        // Two callers + exactly one child = 3 instances in T1.
        let filter = orch8_types::filter::InstanceFilter {
            tenant_id: Some(TenantId("T1".into())),
            ..Default::default()
        };
        let pagination = orch8_types::filter::Pagination::default();
        let instances = storage.list_instances(&filter, &pagination).await.unwrap();
        assert_eq!(instances.len(), 3);
    }

    /// Tenant scope across TWO tenants with the same key → both succeed
    /// independently. Proves tenant isolation at the handler layer.
    #[tokio::test]
    async fn emit_event_dedupe_tenant_scope_isolates_tenants() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller_a = mk_instance("T1", InstanceState::Running);
        let caller_b = mk_instance("T2", InstanceState::Running);
        storage.create_instance(&caller_a).await.unwrap();
        storage.create_instance(&caller_b).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        seed_sequence(&storage, "T2", "child_seq").await;
        let trigger_a = mk_trigger("on-order", "T1", "child_seq");
        let trigger_b = mk_trigger("on-order-b", "T2", "child_seq");
        storage.create_trigger(&trigger_a).await.unwrap();
        storage.create_trigger(&trigger_b).await.unwrap();

        let ra = handle_emit_event(mk_ctx(
            &caller_a,
            storage_dyn.clone(),
            json!({
                "trigger_slug": "on-order",
                "dedupe_key": "shared",
                "dedupe_scope": "tenant",
            }),
        ))
        .await
        .unwrap();
        let rb = handle_emit_event(mk_ctx(
            &caller_b,
            storage_dyn,
            json!({
                "trigger_slug": "on-order-b",
                "dedupe_key": "shared",
                "dedupe_scope": "tenant",
            }),
        ))
        .await
        .unwrap();

        assert_eq!(ra.get("deduped"), Some(&json!(false)));
        assert_eq!(rb.get("deduped"), Some(&json!(false)));
        assert_ne!(
            ra.get("instance_id"),
            rb.get("instance_id"),
            "same key in two tenants must NOT collide"
        );
    }

    /// Parent scope and tenant scope with the same (caller, key) are
    /// independent namespaces — both succeed.
    #[tokio::test]
    async fn emit_event_dedupe_parent_and_tenant_scopes_are_independent() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let make_parent = || {
            mk_ctx(
                &caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "dedupe_key": "k",
                    "dedupe_scope": "parent",
                }),
            )
        };
        let make_tenant = || {
            mk_ctx(
                &caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "dedupe_key": "k",
                    "dedupe_scope": "tenant",
                }),
            )
        };

        let rp = handle_emit_event(make_parent()).await.unwrap();
        let rt = handle_emit_event(make_tenant()).await.unwrap();

        assert_eq!(rp.get("deduped"), Some(&json!(false)));
        assert_eq!(rt.get("deduped"), Some(&json!(false)));
        assert_ne!(rp.get("instance_id"), rt.get("instance_id"));
    }

    #[tokio::test]
    async fn emit_event_rejects_invalid_dedupe_scope() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "dedupe_key": "k",
                "dedupe_scope": "global",
            }),
        );
        let err = handle_emit_event(ctx).await.unwrap_err();
        match err {
            StepError::Permanent { message, .. } => {
                assert!(
                    message.contains("invalid dedupe_scope"),
                    "expected 'invalid dedupe_scope' in message, got: {message}"
                );
            }
            other @ StepError::Retryable { .. } => {
                panic!("expected Permanent error, got: {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn emit_event_rejects_non_string_dedupe_scope() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "trigger_slug": "on-order",
                "dedupe_key": "k",
                "dedupe_scope": 42,
            }),
        );
        let err = handle_emit_event(ctx).await.unwrap_err();
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    /// Absence of `dedupe_scope` must not change pre-R7 per-parent behavior.
    /// Two different parents with the same key → both create distinct
    /// children (regression guard for the default).
    #[tokio::test]
    async fn emit_event_default_scope_is_parent() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller_a = mk_instance("T1", InstanceState::Running);
        let caller_b = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller_a).await.unwrap();
        storage.create_instance(&caller_b).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let make = |caller: &TaskInstance| {
            mk_ctx(
                caller,
                storage_dyn.clone(),
                json!({
                    "trigger_slug": "on-order",
                    "dedupe_key": "k",
                    // No dedupe_scope; handler must default to "parent".
                }),
            )
        };

        let ra = handle_emit_event(make(&caller_a)).await.unwrap();
        let rb = handle_emit_event(make(&caller_b)).await.unwrap();
        assert_eq!(ra.get("deduped"), Some(&json!(false)));
        assert_eq!(rb.get("deduped"), Some(&json!(false)));
        assert_ne!(ra.get("instance_id"), rb.get("instance_id"));
    }
}
