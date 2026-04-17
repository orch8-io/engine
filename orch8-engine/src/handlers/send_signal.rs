//! `send_signal` builtin: enqueue a signal to another instance.
//!
//! Same-tenant only. Cross-tenant attempts return `Permanent` (does not leak
//! existence). Rejects if the target is in a terminal state.

use chrono::Utc;
use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use orch8_storage::StorageBackend;
use orch8_types::{
    error::{StepError, StorageError},
    ids::InstanceId,
    signal::{Signal, SignalType},
};

use super::StepContext;

#[allow(dead_code)] // registered in register_builtins (T14)
pub(crate) async fn handle_send_signal(
    ctx: StepContext,
    storage: &dyn StorageBackend,
) -> Result<Value, StepError> {
    let target_id = parse_instance_id(&ctx.params)?;

    let signal_type_val =
        ctx.params
            .get("signal_type")
            .cloned()
            .ok_or_else(|| StepError::Permanent {
                message: "missing 'signal_type' param".into(),
                details: None,
            })?;
    let signal_type: SignalType = serde_json::from_value(signal_type_val).map_err(|e| {
        StepError::Permanent {
            message: format!("invalid 'signal_type': {e}"),
            details: None,
        }
    })?;

    let payload = ctx
        .params
        .get("payload")
        .cloned()
        .unwrap_or(Value::Null);

    let caller = storage
        .get_instance(ctx.instance_id)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| StepError::Permanent {
            message: "caller instance not found".into(),
            details: None,
        })?;

    let target = storage
        .get_instance(target_id)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| StepError::Permanent {
            message: "target instance not found".into(),
            details: None,
        })?;

    if target.tenant_id != caller.tenant_id {
        warn!(
            caller_tenant = %caller.tenant_id.0,
            target_tenant = %target.tenant_id.0,
            caller_instance_id = %ctx.instance_id.0,
            target_instance_id = %target_id.0,
            "send_signal: cross-tenant send_signal denied"
        );
        return Err(StepError::Permanent {
            message: "cross-tenant send_signal denied".into(),
            details: Some(json!({
                "caller_tenant": caller.tenant_id.0,
                "target_tenant": target.tenant_id.0,
            })),
        });
    }

    if target.state.is_terminal() {
        return Err(StepError::Permanent {
            message: format!(
                "cannot send signal to terminal instance (state: {})",
                target.state
            ),
            details: Some(json!({ "state": target.state.to_string() })),
        });
    }

    let signal = Signal {
        id: Uuid::new_v4(),
        instance_id: target_id,
        signal_type,
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };

    storage
        .enqueue_signal(&signal)
        .await
        .map_err(|e| map_storage_err(&e))?;

    Ok(json!({ "signal_id": signal.id.to_string() }))
}

#[allow(dead_code)] // used by handle_send_signal (registered in T14)
fn parse_instance_id(params: &Value) -> Result<InstanceId, StepError> {
    let id_str = params
        .get("instance_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| StepError::Permanent {
            message: "missing 'instance_id' string param".into(),
            details: None,
        })?;
    Ok(InstanceId(Uuid::parse_str(id_str).map_err(|e| {
        StepError::Permanent {
            message: format!("invalid 'instance_id' uuid: {e}"),
            details: None,
        }
    })?))
}

#[allow(dead_code)] // used by handle_send_signal (registered in T14)
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

    #[tokio::test]
    async fn send_signal_enqueues_signal_for_same_tenant_target() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let result = handle_send_signal(ctx, &storage).await.unwrap();

        assert!(
            result.get("signal_id").and_then(|v| v.as_str()).is_some(),
            "expected signal_id in response, got: {result}"
        );

        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert_eq!(pending.len(), 1, "expected 1 pending signal for target");
        assert_eq!(pending[0].signal_type, SignalType::Cancel);
        assert_eq!(pending[0].instance_id, target.id);
    }

    #[tokio::test]
    async fn send_signal_rejects_when_target_missing() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let missing_target = InstanceId::new();
        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": missing_target.0.to_string(),
                "signal_type": "cancel",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_send_signal(ctx, &storage).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_rejects_when_target_in_terminal_state() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Completed);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_send_signal(ctx, &storage).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_denies_cross_tenant() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T2", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_send_signal(ctx, &storage).await.unwrap_err();
        if let StepError::Permanent { message, .. } = &err {
            assert!(
                message.contains("cross-tenant"),
                "expected 'cross-tenant' in message, got: {message}"
            );
            assert!(
                !message.to_lowercase().contains("terminal")
                    && !message.to_lowercase().contains("state"),
                "message should not leak target state info, got: {message}"
            );
        } else {
            panic!("expected Permanent, got: {err:?}");
        }

        // verify no signal was enqueued
        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert!(pending.is_empty(), "no signals should be enqueued cross-tenant");
    }

    #[tokio::test]
    async fn send_signal_rejects_invalid_uuid_param() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": "not-a-uuid",
                "signal_type": "cancel",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_send_signal(ctx, &storage).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_rejects_unknown_signal_type() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = StepContext {
            instance_id: caller.id,
            block_id: BlockId("s".into()),
            params: json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "bogus_signal",
            }),
            context: ExecutionContext::default(),
            attempt: 1,
        };
        let err = handle_send_signal(ctx, &storage).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }
}
