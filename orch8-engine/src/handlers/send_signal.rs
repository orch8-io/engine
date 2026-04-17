//! `send_signal` builtin: enqueue a signal to another instance.
//!
//! Same-tenant only. Cross-tenant attempts return `Permanent` (does not leak
//! existence). Rejects if the target is in a terminal state.
//!
//! # Concurrency
//!
//! The terminal-state check and signal INSERT run inside a single storage
//! transaction via [`StorageBackend::enqueue_signal_if_active`], closing the
//! TOCTOU window where the target could transition to terminal between a
//! pre-read and the INSERT. The handler still fetches the target once up
//! front — but *only* for the cross-tenant guard, which must run before any
//! write side effect. The read-path state of the target is NOT trusted: the
//! atomic method re-reads state under a row lock (PG: `FOR UPDATE`; `SQLite`:
//! write-txn semantics) and rejects terminal targets itself.

use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use orch8_types::{
    error::{StepError, StorageError},
    signal::{Signal, SignalType},
};

use super::util::{check_same_tenant, map_storage_err, parse_instance_id, permanent};
use super::StepContext;

pub(crate) async fn handle_send_signal(ctx: StepContext) -> Result<Value, StepError> {
    let target_id = parse_instance_id(&ctx.params, "instance_id")?;

    let signal_type_val = ctx
        .params
        .get("signal_type")
        .cloned()
        .ok_or_else(|| permanent("missing 'signal_type' param"))?;
    let signal_type: SignalType = serde_json::from_value(signal_type_val)
        .map_err(|e| permanent(format!("invalid 'signal_type': {e}")))?;

    let payload = ctx.params.get("payload").cloned().unwrap_or(Value::Null);

    let storage = ctx.storage.as_ref();

    // Tenant check MUST happen before any write. Cross-tenant attempts must
    // never leave side effects or leak target existence — so we do this read
    // up front, even though the atomic path below re-checks state itself.
    let target = storage
        .get_instance(target_id)
        .await
        .map_err(|e| map_storage_err(&e))?
        .ok_or_else(|| permanent("target instance not found"))?;

    check_same_tenant(&ctx.tenant_id, &target.tenant_id, "send_signal")?;

    let signal = Signal {
        id: Uuid::new_v4(),
        instance_id: target_id,
        signal_type,
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };

    // Atomic: BEGIN → SELECT state (locked) → INSERT (or reject) → COMMIT.
    // Terminal state is surfaced as a dedicated `StorageError::TerminalTarget`
    // variant (distinct from generic `Conflict`, which would also match
    // idempotency-key dupes and constraint violations — those must stay
    // unambiguous for the handler).
    match storage.enqueue_signal_if_active(&signal).await {
        Ok(()) => Ok(json!({ "signal_id": signal.id.to_string() })),
        Err(StorageError::NotFound { .. }) => Err(permanent("target instance not found")),
        Err(StorageError::TerminalTarget { .. }) => Err(StepError::Permanent {
            message: "cannot send signal to terminal instance".to_string(),
            details: Some(json!({ "instance_id": target_id.0.to_string() })),
        }),
        Err(other) => Err(map_storage_err(&other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
    use orch8_types::{
        context::{ExecutionContext, RuntimeContext},
        ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId},
        instance::{InstanceState, Priority, TaskInstance},
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

    fn mk_ctx(
        caller: &TaskInstance,
        storage: Arc<dyn StorageBackend>,
        params: Value,
    ) -> StepContext {
        StepContext {
            instance_id: caller.id,
            tenant_id: caller.tenant_id.clone(),
            block_id: BlockId("s".into()),
            params,
            context: ExecutionContext::default(),
            attempt: 1,
            storage,
        }
    }

    #[tokio::test]
    async fn send_signal_enqueues_signal_for_same_tenant_target() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
        );
        let result = handle_send_signal(ctx).await.unwrap();

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
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let missing_target = InstanceId::new();
        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": missing_target.0.to_string(),
                "signal_type": "cancel",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_rejects_when_target_in_terminal_state() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Completed);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_denies_cross_tenant() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T2", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
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
        assert!(
            pending.is_empty(),
            "no signals should be enqueued cross-tenant"
        );
    }

    #[tokio::test]
    async fn send_signal_rejects_invalid_uuid_param() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": "not-a-uuid",
                "signal_type": "cancel",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }

    /// TOCTOU regression: seed target as Running, transition it to Completed
    /// (simulating the race where another worker completes the instance), then
    /// invoke the handler. The atomic path must reject with `Permanent` and
    /// leave no row behind in `pending_signals`.
    #[tokio::test]
    async fn send_signal_atomic_path_rejects_terminal_transition() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        // Simulate the racing worker that transitions the target to terminal
        // after the handler's tenant-check read but before enqueue.
        storage
            .update_instance_state(target.id, InstanceState::Completed, None)
            .await
            .unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "cancel",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
        match &err {
            StepError::Permanent { message, .. } => {
                assert!(
                    message.contains("terminal"),
                    "expected 'terminal' in message, got: {message}"
                );
            }
            StepError::Retryable { .. } => panic!("expected Permanent, got: {err:?}"),
        }

        // Core invariant: the atomic path MUST NOT have left a signal row.
        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert!(
            pending.is_empty(),
            "rejected enqueue must not leave a row in pending_signals, got: {pending:?}"
        );
    }

    #[tokio::test]
    async fn send_signal_rejects_unknown_signal_type() {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let storage_dyn: Arc<dyn StorageBackend> = storage.clone();
        let caller = mk_instance("T1", InstanceState::Running);
        let target = mk_instance("T1", InstanceState::Running);
        storage.create_instance(&caller).await.unwrap();
        storage.create_instance(&target).await.unwrap();

        let ctx = mk_ctx(
            &caller,
            storage_dyn,
            json!({
                "instance_id": target.id.0.to_string(),
                "signal_type": "bogus_signal",
            }),
        );
        let err = handle_send_signal(ctx).await.unwrap_err();
        assert!(
            matches!(err, StepError::Permanent { .. }),
            "expected Permanent, got: {err:?}"
        );
    }
}
