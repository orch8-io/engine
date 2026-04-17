//! `emit_event` builtin: fire an event trigger → spawn a new workflow instance.
//!
//! Same-tenant only. Cross-tenant attempts return `Permanent`. The caller's
//! instance id is captured in the child's metadata as `parent_instance_id`.
//!
//! NOTE: dedupe is NOT implemented yet (T13). The `dedupe_key` param is
//! accepted but ignored in T11.

use serde_json::{json, Map, Value};
use tracing::warn;

use orch8_storage::StorageBackend;
use orch8_types::{
    error::{StepError, StorageError},
    ids::InstanceId,
};

use super::StepContext;

#[allow(dead_code)] // registered in register_builtins (T14)
pub(crate) async fn handle_emit_event(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let trigger_slug = ctx
        .params
        .get("trigger_slug")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "missing 'trigger_slug' string param".into(),
            details: None,
        })?
        .to_string();

    let data = ctx
        .params
        .get("data")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let user_meta = ctx
        .params
        .get("meta")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let trigger = storage
        .get_trigger(&trigger_slug)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| StepError::Permanent {
            message: format!("trigger '{trigger_slug}' not found"),
            details: None,
        })?;

    if !trigger.enabled {
        return Err(StepError::Permanent {
            message: format!("trigger '{trigger_slug}' is disabled"),
            details: None,
        });
    }

    let caller = storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| StepError::Permanent {
            message: "caller instance not found".into(),
            details: None,
        })?;

    if caller.tenant_id.0 != trigger.tenant_id {
        warn!(
            caller_tenant = %caller.tenant_id.0,
            trigger_tenant = %trigger.tenant_id,
            caller_instance_id = %ctx.instance_id.0,
            trigger_slug = %trigger_slug,
            "emit_event: cross-tenant emit_event denied"
        );
        return Err(StepError::Permanent {
            message: "cross-tenant emit_event denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "trigger_tenant": trigger.tenant_id,
            })),
        });
    }

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

    let child_id = InstanceId::new();
    let sequence_name = trigger.sequence_name.clone();

    crate::triggers::create_trigger_instance(
        storage,
        &trigger,
        data,
        meta_with_source,
        Some(child_id),
    )
    .await
    .map_err(|e| StepError::Permanent {
        message: format!("failed to create child instance: {e}"),
        details: None,
    })?;

    Ok(json!({
        "instance_id": child_id.0.to_string(),
        "sequence_name": sequence_name,
    }))
}

#[allow(dead_code)] // used by handle_emit_event (registered in T14)
fn map_storage_err(e: &StorageError) -> StepError {
    match e {
        StorageError::Connection(_) | StorageError::PoolExhausted | StorageError::Query(_) => {
            StepError::Retryable {
                message: format!("storage: {e}"),
                details: None,
            }
        }
        _ => StepError::Permanent {
            message: format!("storage: {e}"),
            details: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        ids::{BlockId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
        sequence::SequenceDefinition,
        trigger::{TriggerDef, TriggerType},
    };
    use serde_json::json;

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
            tenant_id: tenant.into(),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Event,
            config: Value::Null,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn emit_event_creates_child_instance_for_same_tenant() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        seed_sequence(&storage, "T1", "child_seq").await;
        let trigger = mk_trigger("on-order", "T1", "child_seq");
        storage.create_trigger(&trigger).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("emit".into()),
            params: json!({
                "trigger_slug": "on-order",
                "data": {"order_id": 42},
                "meta": {"source": "spoofed", "custom": "value"},
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let result = handle_emit_event(ctx, &storage).await.unwrap();

        let child_id_str = result
            .get("instance_id")
            .and_then(|v| v.as_str())
            .expect("instance_id in response");
        assert_eq!(
            result.get("sequence_name").and_then(|v| v.as_str()),
            Some("child_seq")
        );

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
}
