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

    storage
        .update_instance_state(instance_id, to, next_fire_at)
        .await?;

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
