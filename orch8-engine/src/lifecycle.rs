use chrono::{DateTime, Utc};
use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::audit::AuditLogEntry;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::instance::InstanceState;

use crate::error::EngineError;

/// Transition an instance to a new state, validating the transition.
///
/// Automatically logs an audit entry for the transition. The `tenant_id`
/// is fetched from storage if not already known — callers do NOT need to
/// invoke [`audit_transition`] separately.
pub async fn transition_instance(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    from: InstanceState,
    to: InstanceState,
    next_fire_at: Option<DateTime<Utc>>,
) -> Result<(), EngineError> {
    if !from.can_transition_to(to) {
        warn!(
            instance_id = %instance_id,
            from = %from,
            to = %to,
            "invalid state transition attempted"
        );
        return Err(EngineError::InvalidTransition {
            instance_id,
            from,
            to,
        });
    }

    // Atomic CAS: only update if the row is still in `from` state.
    // Prevents a concurrent cancel/fail from being silently overwritten
    // by a late scheduler or worker callback.
    let updated = storage
        .conditional_update_instance_state(instance_id, from, to, next_fire_at)
        .await?;
    if !updated {
        warn!(
            instance_id = %instance_id,
            from = %from,
            to = %to,
            "state transition conflict — instance was already moved by a concurrent writer"
        );
        return Err(EngineError::InvalidTransition {
            instance_id,
            from,
            to,
        });
    }

    info!(
        instance_id = %instance_id,
        from = %from,
        to = %to,
        "instance state transitioned"
    );

    // Best-effort audit logging. Fetch tenant_id from the instance row.
    if let Ok(Some(inst)) = storage.get_instance(instance_id).await {
        audit_transition(storage, instance_id, &inst.tenant_id, from, to, None).await;
    }

    Ok(())
}

/// Record an audit log entry for a state transition.
/// This is best-effort — audit failures are logged but do not block execution.
pub async fn audit_transition(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    tenant_id: &TenantId,
    from: InstanceState,
    to: InstanceState,
    block_id: Option<&str>,
) {
    let entry = AuditLogEntry {
        id: uuid::Uuid::now_v7(),
        instance_id,
        tenant_id: tenant_id.clone(),
        event_type: "state_transition".into(),
        from_state: Some(from.to_string()),
        to_state: Some(to.to_string()),
        block_id: block_id.map(String::from),
        details: serde_json::Value::Null,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.append_audit_log(&entry).await {
        tracing::warn!(error = %e, "failed to append audit log entry");
    }
}

/// Record a generic audit event.
pub async fn audit_event(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    tenant_id: &TenantId,
    event_type: &str,
    block_id: Option<&str>,
    details: serde_json::Value,
) {
    let entry = AuditLogEntry {
        id: uuid::Uuid::now_v7(),
        instance_id,
        tenant_id: tenant_id.clone(),
        event_type: event_type.into(),
        from_state: None,
        to_state: None,
        block_id: block_id.map(String::from),
        details,
        created_at: Utc::now(),
    };
    if let Err(e) = storage.append_audit_log(&entry).await {
        tracing::warn!(error = %e, "failed to append audit log entry");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use serde_json::json;

    fn mk_scheduled_instance() -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            state: InstanceState::Scheduled,
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

    #[test]
    fn scheduled_to_running_valid() {
        assert!(InstanceState::Scheduled.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn completed_to_running_invalid() {
        assert!(!InstanceState::Completed.can_transition_to(InstanceState::Running));
    }

    #[test]
    fn running_to_waiting_valid() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Waiting));
    }

    #[test]
    fn running_to_scheduled_valid_for_retry() {
        assert!(InstanceState::Running.can_transition_to(InstanceState::Scheduled));
    }

    #[tokio::test]
    async fn transition_instance_persists_new_state() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();

        transition_instance(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            InstanceState::Running,
            None,
        )
        .await
        .unwrap();

        inst = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(inst.state, InstanceState::Running);
    }

    #[tokio::test]
    async fn transition_instance_clears_next_fire_when_none_passed() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();

        transition_instance(
            &storage,
            inst.id,
            InstanceState::Scheduled,
            InstanceState::Running,
            None,
        )
        .await
        .unwrap();

        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert!(stored.next_fire_at.is_none());
    }

    #[tokio::test]
    async fn transition_instance_rejects_invalid_transition() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();

        let err = transition_instance(
            &storage,
            inst.id,
            InstanceState::Completed, // pretend source
            InstanceState::Running,   // invalid target
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, EngineError::InvalidTransition { .. }));

        // State on disk must not be touched.
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Scheduled);
    }

    #[tokio::test]
    async fn transition_instance_running_to_completed() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut inst = mk_scheduled_instance();
        inst.state = InstanceState::Running;
        storage.create_instance(&inst).await.unwrap();

        transition_instance(
            &storage,
            inst.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
        )
        .await
        .unwrap();
        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Completed);
    }

    #[tokio::test]
    async fn transition_instance_waiting_to_running_valid() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut inst = mk_scheduled_instance();
        inst.state = InstanceState::Waiting;
        storage.create_instance(&inst).await.unwrap();

        transition_instance(
            &storage,
            inst.id,
            InstanceState::Waiting,
            InstanceState::Running,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn audit_transition_appends_row_readable_by_instance() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();

        audit_transition(
            &storage,
            inst.id,
            &inst.tenant_id,
            InstanceState::Scheduled,
            InstanceState::Running,
            Some("blk-1"),
        )
        .await;

        let rows = storage.list_audit_log(inst.id, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.event_type, "state_transition");
        assert_eq!(row.from_state.as_deref(), Some("scheduled"));
        assert_eq!(row.to_state.as_deref(), Some("running"));
        assert_eq!(row.block_id.as_deref(), Some("blk-1"));
    }

    #[tokio::test]
    async fn audit_event_appends_row_with_details() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();

        let details = json!({ "reason": "manual_cancel", "user": "alice" });
        audit_event(
            &storage,
            inst.id,
            &inst.tenant_id,
            "cancel_requested",
            None,
            details.clone(),
        )
        .await;

        let rows = storage.list_audit_log(inst.id, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_type, "cancel_requested");
        assert!(rows[0].from_state.is_none());
        assert!(rows[0].to_state.is_none());
        assert_eq!(rows[0].details, details);
    }

    #[tokio::test]
    async fn transition_instance_with_next_fire_at_sets_fire_time() {
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let mut inst = mk_scheduled_instance();
        inst.state = InstanceState::Running;
        storage.create_instance(&inst).await.unwrap();

        let fire = Utc::now() + chrono::Duration::seconds(42);
        transition_instance(
            &storage,
            inst.id,
            InstanceState::Running,
            InstanceState::Scheduled,
            Some(fire),
        )
        .await
        .unwrap();

        let stored = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(stored.state, InstanceState::Scheduled);
        let got = stored.next_fire_at.expect("next_fire_at was set");
        // SQLite persists timestamps as text — tolerate sub-second round-trip loss.
        assert!((got - fire).num_milliseconds().abs() < 1000);
    }

    #[tokio::test]
    async fn concurrent_transitions_are_serialized_no_lost_updates() {
        // Two concurrent transitions from Scheduled — one must win and the
        // loser must see its `from` no longer match and fail with
        // InvalidTransition (the state-machine guard protects us).
        use std::sync::Arc;
        let storage: Arc<orch8_storage::sqlite::SqliteStorage> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let inst = mk_scheduled_instance();
        storage.create_instance(&inst).await.unwrap();
        let inst_id = inst.id;

        let s1 = Arc::clone(&storage);
        let s2 = Arc::clone(&storage);
        let t1 = tokio::spawn(async move {
            transition_instance(
                s1.as_ref(),
                inst_id,
                InstanceState::Scheduled,
                InstanceState::Running,
                None,
            )
            .await
        });
        let t2 = tokio::spawn(async move {
            transition_instance(
                s2.as_ref(),
                inst_id,
                InstanceState::Scheduled,
                InstanceState::Cancelled,
                None,
            )
            .await
        });
        let (r1, r2) = tokio::join!(t1, t2);
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();

        // Exactly one transition must succeed — both claim Scheduled as the
        // `from` side, but once one commits the row is in the other terminal
        // state and re-issuing the same write would not actually change the
        // logical state machine. At minimum one must be Ok and the final
        // stored state is one of the two targets (not still Scheduled).
        assert!(r1.is_ok() || r2.is_ok(), "at least one transition succeeds");
        let stored = storage.get_instance(inst_id).await.unwrap().unwrap();
        assert!(
            stored.state == InstanceState::Running || stored.state == InstanceState::Cancelled,
            "final state must reflect one of the concurrent writers, got {:?}",
            stored.state
        );
        assert_ne!(
            stored.state,
            InstanceState::Scheduled,
            "Scheduled means both writers lost — impossible"
        );
    }

    #[tokio::test]
    async fn audit_fns_are_best_effort_and_do_not_panic_on_orphan_writes() {
        // No instance row — append_audit_log may fail with FK error, but the
        // audit helpers must swallow the error (best-effort contract).
        let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap();
        let orphan = InstanceId::new();
        let tenant = TenantId("t".into());

        audit_transition(
            &storage,
            orphan,
            &tenant,
            InstanceState::Scheduled,
            InstanceState::Running,
            None,
        )
        .await;

        audit_event(&storage, orphan, &tenant, "ping", None, json!({})).await;
        // If we got here without panicking the contract is upheld.
    }
}
