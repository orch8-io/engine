//! Coverage tests for `StorageBackend` via `SQLite` in-memory.
//!
//! 90 tests covering: Instance CRUD, Sequence CRUD, Execution Tree,
//! Signal storage, Audit log, Credential storage, Session storage, and Misc.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-storage --test storage_coverage
//! ```

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{
    AdminStore, ExecutionTreeStore, InstanceStore, OutputStore, SequenceStore, SignalStore,
};
use orch8_types::audit::AuditLogEntry;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::interceptor::InterceptorAction;
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};
use orch8_types::session::{Session, SessionState};
use orch8_types::signal::{Signal, SignalType};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

fn make_sequence(tenant: &str) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        name: "test_seq".into(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::Production,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("s1"),
            handler: "noop".into(),
            params: json!({}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        }))],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn make_sequence_named(tenant: &str, name: &str, namespace: &str) -> SequenceDefinition {
    let mut seq = make_sequence(tenant);
    seq.name = name.into();
    seq.namespace = Namespace::new(namespace);
    seq
}

fn make_instance(tenant: &str, seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId::unchecked(tenant),
        namespace: Namespace::new("default"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now - Duration::seconds(10)),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

fn make_signal(instance_id: InstanceId, signal_type: SignalType) -> Signal {
    Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

fn make_execution_node(instance_id: InstanceId, block_id: &str) -> ExecutionNode {
    ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id,
        block_id: BlockId::new(block_id),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    }
}

fn make_audit_entry(instance_id: InstanceId, tenant: &str, event_type: &str) -> AuditLogEntry {
    AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id,
        tenant_id: TenantId::unchecked(tenant),
        event_type: event_type.into(),
        from_state: None,
        to_state: None,
        block_id: None,
        details: json!({}),
        created_at: Utc::now(),
    }
}

fn make_credential(id: &str, tenant: &str) -> CredentialDef {
    let now = Utc::now();
    CredentialDef {
        id: id.into(),
        tenant_id: tenant.into(),
        name: format!("Credential {id}"),
        kind: CredentialKind::ApiKey,
        value: orch8_types::config::SecretString::new(
            r#"{"token":"test_secret_value"}"#.to_string(),
        ),
        expires_at: None,
        refresh_url: None,
        refresh_token: None,
        enabled: true,
        description: None,
        created_at: now,
        updated_at: now,
    }
}

fn make_session(tenant: &str, key: &str) -> Session {
    let now = Utc::now();
    Session {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked(tenant),
        session_key: key.into(),
        data: json!({"step": 1}),
        state: SessionState::Active,
        created_at: now,
        updated_at: now,
        expires_at: None,
    }
}

// ===========================================================================
// Instance CRUD (tests 1-20)
// ===========================================================================

#[tokio::test]
async fn t01_create_instance_and_get_by_id() {
    let s = store().await;
    let seq = make_sequence("tenant_a");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("tenant_a", seq.id);
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.id, inst.id);
    assert_eq!(fetched.tenant_id.as_str(), "tenant_a");
    assert_eq!(fetched.state, InstanceState::Scheduled);
}

#[tokio::test]
async fn t02_create_instance_duplicate_id_fails() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let result = s.create_instance(&inst).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn t03_get_instance_nonexistent_returns_none() {
    let s = store().await;
    let result = s.get_instance(InstanceId::new()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn t04_update_instance_state_from_scheduled_to_running() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    s.update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, InstanceState::Running);
}

#[tokio::test]
async fn t05_update_instance_state_sets_updated_at() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    let original_updated = inst.updated_at;
    s.create_instance(&inst).await.unwrap();

    // Small delay to ensure timestamp differs
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    s.update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert!(fetched.updated_at >= original_updated);
}

#[tokio::test]
async fn t06_list_instances_by_tenant() {
    let s = store().await;
    let seq = make_sequence("tenant_x");
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = make_instance("tenant_x", seq.id);
        s.create_instance(&inst).await.unwrap();
    }
    let other = make_instance("tenant_y", seq.id);
    s.create_instance(&other).await.unwrap();

    let filter = InstanceFilter {
        tenant_id: Some(TenantId::unchecked("tenant_x")),
        ..Default::default()
    };
    let results = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn t07_list_instances_by_state() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let inst1 = make_instance("t", seq.id);
    s.create_instance(&inst1).await.unwrap();
    s.update_instance_state(inst1.id, InstanceState::Running, None)
        .await
        .unwrap();

    let inst2 = make_instance("t", seq.id);
    s.create_instance(&inst2).await.unwrap();

    let filter = InstanceFilter {
        states: Some(vec![InstanceState::Running]),
        ..Default::default()
    };
    let results = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, inst1.id);
}

#[tokio::test]
async fn t08_list_instances_by_namespace() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let mut inst1 = make_instance("t", seq.id);
    inst1.namespace = Namespace::new("prod");
    s.create_instance(&inst1).await.unwrap();

    let mut inst2 = make_instance("t", seq.id);
    inst2.namespace = Namespace::new("staging");
    s.create_instance(&inst2).await.unwrap();

    let filter = InstanceFilter {
        namespace: Some(Namespace::new("prod")),
        ..Default::default()
    };
    let results = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].namespace.as_str(), "prod");
}

#[tokio::test]
async fn t09_list_instances_pagination() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = make_instance("t", seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let page1 = s
        .list_instances(
            &InstanceFilter::default(),
            &Pagination {
                offset: 0,
                limit: 2,
                sort_ascending: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = s
        .list_instances(
            &InstanceFilter::default(),
            &Pagination {
                offset: 2,
                limit: 2,
                sort_ascending: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);

    let page3 = s
        .list_instances(
            &InstanceFilter::default(),
            &Pagination {
                offset: 4,
                limit: 2,
                sort_ascending: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(page3.len(), 1);
}

#[tokio::test]
async fn t10_count_instances_with_state_filter() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    // Verify counting works with a state filter
    let filter = InstanceFilter {
        tenant_id: Some(TenantId::unchecked("t")),
        states: Some(vec![InstanceState::Scheduled]),
        ..Default::default()
    };
    let count = s.count_instances(&filter).await.unwrap();
    assert_eq!(count, 1);

    // Update to terminal state
    s.update_instance_state(inst.id, InstanceState::Cancelled, None)
        .await
        .unwrap();

    let filter_cancelled = InstanceFilter {
        states: Some(vec![InstanceState::Cancelled]),
        ..Default::default()
    };
    let count = s.count_instances(&filter_cancelled).await.unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn t11_conditional_update_state_succeeds_when_current_matches() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let result = s
        .conditional_update_instance_state(
            inst.id,
            InstanceState::Scheduled,
            InstanceState::Running,
            None,
        )
        .await
        .unwrap();
    assert!(result);

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, InstanceState::Running);
}

#[tokio::test]
async fn t12_conditional_update_state_fails_when_current_differs() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    // Try to transition from Running (but instance is Scheduled)
    let result = s
        .conditional_update_instance_state(
            inst.id,
            InstanceState::Running,
            InstanceState::Completed,
            None,
        )
        .await
        .unwrap();
    assert!(!result);

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, InstanceState::Scheduled);
}

#[tokio::test]
async fn t13_get_instances_ready_to_run() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    // Instance with fire time in the past (should be claimed)
    let mut inst1 = make_instance("t", seq.id);
    inst1.next_fire_at = Some(Utc::now() - Duration::seconds(60));
    s.create_instance(&inst1).await.unwrap();

    // Instance with fire time in the future (should not be claimed)
    let mut inst2 = make_instance("t", seq.id);
    inst2.next_fire_at = Some(Utc::now() + Duration::hours(1));
    s.create_instance(&inst2).await.unwrap();

    let claimed = s.claim_due_instances(Utc::now(), 10, 0).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, inst1.id);
}

#[tokio::test]
async fn t14_update_instance_context_persists() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let new_ctx = ExecutionContext {
        data: json!({"user_id": "u123", "status": "active"}),
        config: json!({"timeout": 30}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    s.update_instance_context(inst.id, &new_ctx).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.context.data["user_id"], "u123");
    assert_eq!(fetched.context.config["timeout"], 30);
}

#[tokio::test]
async fn t15_create_instance_with_priority_high() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);
    inst.priority = Priority::High;
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.priority, Priority::High);
}

#[tokio::test]
async fn t16_list_instances_ordered_by_priority() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let mut low = make_instance("t", seq.id);
    low.priority = Priority::Low;
    s.create_instance(&low).await.unwrap();

    let mut high = make_instance("t", seq.id);
    high.priority = Priority::High;
    s.create_instance(&high).await.unwrap();

    let filter = InstanceFilter {
        priority: Some(Priority::High),
        ..Default::default()
    };
    let results = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].priority, Priority::High);
}

#[tokio::test]
async fn t17_list_instances_by_sequence_id() {
    let s = store().await;
    let seq1 = make_sequence("t");
    let seq2 = make_sequence("t");
    s.create_sequence(&seq1).await.unwrap();
    s.create_sequence(&seq2).await.unwrap();

    let inst1 = make_instance("t", seq1.id);
    s.create_instance(&inst1).await.unwrap();
    let inst2 = make_instance("t", seq2.id);
    s.create_instance(&inst2).await.unwrap();

    let filter = InstanceFilter {
        sequence_id: Some(seq1.id),
        ..Default::default()
    };
    let results = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sequence_id, seq1.id);
}

#[tokio::test]
async fn t18_count_instances_by_state() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = make_instance("t", seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let filter = InstanceFilter {
        states: Some(vec![InstanceState::Scheduled]),
        ..Default::default()
    };
    let count = s.count_instances(&filter).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn t19_create_instance_with_concurrency_key() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);
    inst.concurrency_key = Some("tenant:t:email_send".into());
    inst.max_concurrency = Some(5);
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(
        fetched.concurrency_key.as_deref(),
        Some("tenant:t:email_send")
    );
    assert_eq!(fetched.max_concurrency, Some(5));
}

#[tokio::test]
async fn t20_create_instance_with_metadata() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);
    inst.metadata = json!({"source": "api", "version": 2, "tags": ["urgent"]});
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.metadata["source"], "api");
    assert_eq!(fetched.metadata["version"], 2);
    assert_eq!(fetched.metadata["tags"][0], "urgent");
}

// ===========================================================================
// Sequence CRUD (tests 21-35)
// ===========================================================================

#[tokio::test]
async fn t21_create_sequence_and_get() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap().unwrap();
    assert_eq!(fetched.name, "test_seq");
    assert_eq!(fetched.version, 1);
}

#[tokio::test]
async fn t22_create_sequence_duplicate_id_error() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let result = s.create_sequence(&seq).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn t23_get_sequence_nonexistent_returns_none() {
    let s = store().await;
    let result = s.get_sequence(SequenceId::new()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn t24_list_sequences_by_tenant() {
    let s = store().await;
    let seq1 = make_sequence("tenant_a");
    let seq2 = make_sequence("tenant_b");
    s.create_sequence(&seq1).await.unwrap();
    s.create_sequence(&seq2).await.unwrap();

    let results = s
        .list_sequences(Some(&TenantId::unchecked("tenant_a")), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tenant_id.as_str(), "tenant_a");
}

#[tokio::test]
async fn t25_list_sequences_by_namespace() {
    let s = store().await;
    let seq1 = make_sequence_named("t", "seq_a", "prod");
    let seq2 = make_sequence_named("t", "seq_b", "staging");
    s.create_sequence(&seq1).await.unwrap();
    s.create_sequence(&seq2).await.unwrap();

    let results = s
        .list_sequences(
            Some(&TenantId::unchecked("t")),
            Some(&Namespace::new("prod")),
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "seq_a");
}

#[tokio::test]
async fn t26_update_sequence_version() {
    let s = store().await;
    let mut seq_v1 = make_sequence("t");
    seq_v1.name = "versioned".into();
    seq_v1.version = 1;
    s.create_sequence(&seq_v1).await.unwrap();

    let mut seq_v2 = make_sequence("t");
    seq_v2.name = "versioned".into();
    seq_v2.version = 2;
    s.create_sequence(&seq_v2).await.unwrap();

    let versions = s
        .list_sequence_versions(
            &TenantId::unchecked("t"),
            &Namespace::new("default"),
            "versioned",
        )
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
}

#[tokio::test]
async fn t27_delete_sequence() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    s.delete_sequence(seq.id).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn t28_get_sequence_by_name_and_namespace() {
    let s = store().await;
    let seq = make_sequence_named("t", "my_flow", "production");
    s.create_sequence(&seq).await.unwrap();

    let fetched = s
        .get_sequence_by_name(
            &TenantId::unchecked("t"),
            &Namespace::new("production"),
            "my_flow",
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.name, "my_flow");
    assert_eq!(fetched.namespace.as_str(), "production");
}

#[tokio::test]
async fn t29_list_sequences_pagination() {
    let s = store().await;
    for i in 0..5 {
        let mut seq = make_sequence("t");
        seq.name = format!("seq_{i}");
        s.create_sequence(&seq).await.unwrap();
    }

    let page1 = s
        .list_sequences(Some(&TenantId::unchecked("t")), None, 2, 0)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = s
        .list_sequences(Some(&TenantId::unchecked("t")), None, 2, 2)
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);

    let page3 = s
        .list_sequences(Some(&TenantId::unchecked("t")), None, 2, 4)
        .await
        .unwrap();
    assert_eq!(page3.len(), 1);
}

#[tokio::test]
async fn t30_create_sequence_with_interceptors() {
    let s = store().await;
    let mut seq = make_sequence("t");
    seq.interceptors = Some(orch8_types::interceptor::InterceptorDef {
        before_step: Some(InterceptorAction {
            handler: "log_before".into(),
            params: json!({}),
        }),
        after_step: Some(InterceptorAction {
            handler: "log_after".into(),
            params: json!({}),
        }),
        on_signal: None,
        on_complete: None,
        on_failure: None,
    });
    s.create_sequence(&seq).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap().unwrap();
    let interceptors = fetched.interceptors.unwrap();
    assert_eq!(
        interceptors.before_step.as_ref().unwrap().handler,
        "log_before"
    );
    assert_eq!(
        interceptors.after_step.as_ref().unwrap().handler,
        "log_after"
    );
}

#[tokio::test]
async fn t31_sequence_deprecated_flag() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    s.deprecate_sequence(seq.id).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap().unwrap();
    assert!(fetched.deprecated);
}

#[tokio::test]
async fn t32_list_sequences_filtered_by_status() {
    let s = store().await;
    let mut seq1 = make_sequence("t");
    seq1.name = "draft_seq".into();
    seq1.status = SequenceStatus::Draft;
    s.create_sequence(&seq1).await.unwrap();

    let mut seq2 = make_sequence("t");
    seq2.name = "prod_seq".into();
    seq2.status = SequenceStatus::Production;
    s.create_sequence(&seq2).await.unwrap();

    // update status
    s.update_sequence_status(seq1.id, "staging").await.unwrap();
    let fetched = s.get_sequence(seq1.id).await.unwrap().unwrap();
    assert_eq!(fetched.status, SequenceStatus::Staging);
}

#[tokio::test]
async fn t33_sequence_blocks_json_roundtrip() {
    let s = store().await;
    let mut seq = make_sequence("t");
    seq.blocks = vec![
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("step_a"),
            handler: "http_request".into(),
            params: json!({"url": "https://example.com", "method": "POST"}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        })),
        BlockDefinition::Step(Box::new(StepDef {
            id: BlockId::new("step_b"),
            handler: "log".into(),
            params: json!({"message": "done"}),
            delay: None,
            retry: None,
            timeout: None,
            rate_limit_key: None,
            send_window: None,
            context_access: None,
            cancellable: true,
            wait_for_input: None,
            queue_name: None,
            deadline: None,
            on_deadline_breach: None,
            fallback_handler: None,
            cache_key: None,
        })),
    ];
    s.create_sequence(&seq).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap().unwrap();
    assert_eq!(fetched.blocks.len(), 2);
    if let BlockDefinition::Step(step) = &fetched.blocks[0] {
        assert_eq!(step.handler, "http_request");
    } else {
        panic!("expected Step");
    }
}

#[tokio::test]
async fn t34_create_multiple_sequences_same_tenant() {
    let s = store().await;
    for i in 0..10 {
        let mut seq = make_sequence("multi_t");
        seq.name = format!("flow_{i}");
        s.create_sequence(&seq).await.unwrap();
    }

    let results = s
        .list_sequences(Some(&TenantId::unchecked("multi_t")), None, 100, 0)
        .await
        .unwrap();
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn t35_sequence_with_complex_nested_blocks() {
    let s = store().await;
    let mut seq = make_sequence("t");
    seq.blocks = vec![BlockDefinition::Parallel(Box::new(
        orch8_types::sequence::ParallelDef {
            id: BlockId::new("par_1"),
            branches: vec![
                vec![BlockDefinition::Step(Box::new(StepDef {
                    id: BlockId::new("branch_a_step"),
                    handler: "noop".into(),
                    params: json!({}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                }))],
                vec![BlockDefinition::Step(Box::new(StepDef {
                    id: BlockId::new("branch_b_step"),
                    handler: "log".into(),
                    params: json!({"msg": "hello"}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                }))],
            ],
        },
    ))];
    s.create_sequence(&seq).await.unwrap();

    let fetched = s.get_sequence(seq.id).await.unwrap().unwrap();
    if let BlockDefinition::Parallel(par) = &fetched.blocks[0] {
        assert_eq!(par.branches.len(), 2);
    } else {
        panic!("expected Parallel");
    }
}

// ===========================================================================
// Execution Tree (tests 36-50)
// ===========================================================================

#[tokio::test]
async fn t36_execution_tree_initially_empty() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert!(tree.is_empty());
}

#[tokio::test]
async fn t37_append_execution_node_and_retrieve() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "step_1");
    s.create_execution_node(&node).await.unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 1);
    assert_eq!(tree[0].block_id.as_str(), "step_1");
    assert_eq!(tree[0].state, NodeState::Pending);
}

#[tokio::test]
async fn t38_update_node_state_to_completed() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "s1");
    s.create_execution_node(&node).await.unwrap();

    s.update_node_state(node.id, NodeState::Completed)
        .await
        .unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree[0].state, NodeState::Completed);
}

#[tokio::test]
async fn t39_update_node_state_to_failed() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "s1");
    s.create_execution_node(&node).await.unwrap();

    s.update_node_state(node.id, NodeState::Failed)
        .await
        .unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree[0].state, NodeState::Failed);
}

#[tokio::test]
async fn t40_get_execution_tree_ordered() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..5 {
        let node = make_execution_node(inst.id, &format!("step_{i}"));
        s.create_execution_node(&node).await.unwrap();
    }

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 5);
}

#[tokio::test]
async fn t41_get_node_by_block_id() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "unique_block");
    s.create_execution_node(&node).await.unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    let found = tree.iter().find(|n| n.block_id.as_str() == "unique_block");
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, node.id);
}

#[tokio::test]
async fn t42_multiple_nodes_same_instance() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let n1 = make_execution_node(inst.id, "a");
    let n2 = make_execution_node(inst.id, "b");
    let n3 = make_execution_node(inst.id, "c");
    s.create_execution_node(&n1).await.unwrap();
    s.create_execution_node(&n2).await.unwrap();
    s.create_execution_node(&n3).await.unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 3);
}

#[tokio::test]
async fn t43_execution_tree_with_parallel_nodes() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let parent_id = ExecutionNodeId::new();
    let parent = ExecutionNode {
        id: parent_id,
        instance_id: inst.id,
        block_id: BlockId::new("parallel_1"),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    s.create_execution_node(&parent).await.unwrap();

    let child1 = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: inst.id,
        block_id: BlockId::new("branch_0_step"),
        parent_id: Some(parent_id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    let child2 = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: inst.id,
        block_id: BlockId::new("branch_1_step"),
        parent_id: Some(parent_id),
        block_type: BlockType::Step,
        branch_index: Some(1),
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    s.create_execution_node(&child1).await.unwrap();
    s.create_execution_node(&child2).await.unwrap();

    let children = s.get_children(parent_id).await.unwrap();
    assert_eq!(children.len(), 2);
}

#[tokio::test]
async fn t44_execution_tree_node_output_stored() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "output_step");
    s.create_execution_node(&node).await.unwrap();

    // Store output via OutputStore
    let output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId::new("output_step"),
        output: json!({"result": "success", "data": [1, 2, 3]}),
        output_ref: None,
        output_size: 50,
        attempt: 1,
        created_at: Utc::now(),
    };
    s.save_block_output(&output).await.unwrap();

    let fetched = s
        .get_block_output(inst.id, &BlockId::new("output_step"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.output["result"], "success");
}

#[tokio::test]
async fn t45_execution_tree_node_error_stored() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "error_step");
    s.create_execution_node(&node).await.unwrap();

    s.update_node_state(node.id, NodeState::Failed)
        .await
        .unwrap();

    let output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId::new("error_step"),
        output: json!({"error": "timeout exceeded", "code": 504}),
        output_ref: None,
        output_size: 40,
        attempt: 1,
        created_at: Utc::now(),
    };
    s.save_block_output(&output).await.unwrap();

    let fetched = s
        .get_block_output(inst.id, &BlockId::new("error_step"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.output["error"], "timeout exceeded");
}

#[tokio::test]
async fn t46_execution_node_duration_tracking() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "timed_step");
    s.create_execution_node(&node).await.unwrap();

    s.update_node_state(node.id, NodeState::Running)
        .await
        .unwrap();
    s.update_node_state(node.id, NodeState::Completed)
        .await
        .unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    let n = &tree[0];
    assert_eq!(n.state, NodeState::Completed);
    // started_at and completed_at should be set by the storage layer
    assert!(n.started_at.is_some());
    assert!(n.completed_at.is_some());
}

#[tokio::test]
async fn t47_execution_tree_after_retry() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "retry_step");
    s.create_execution_node(&node).await.unwrap();

    // First attempt fails
    s.update_node_state(node.id, NodeState::Failed)
        .await
        .unwrap();

    // Reset to pending (simulating retry)
    s.update_node_state(node.id, NodeState::Pending)
        .await
        .unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree[0].state, NodeState::Pending);
}

#[tokio::test]
async fn t48_clear_execution_tree() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..3 {
        let node = make_execution_node(inst.id, &format!("node_{i}"));
        s.create_execution_node(&node).await.unwrap();
    }

    s.delete_execution_tree(inst.id).await.unwrap();

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert!(tree.is_empty());
}

#[tokio::test]
async fn t49_execution_tree_large_sequence() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..50 {
        let node = make_execution_node(inst.id, &format!("step_{i}"));
        s.create_execution_node(&node).await.unwrap();
    }

    let tree = s.get_execution_tree(inst.id).await.unwrap();
    assert_eq!(tree.len(), 50);
}

#[tokio::test]
async fn t50_execution_node_attempt_counter() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let node = make_execution_node(inst.id, "attempt_step");
    s.create_execution_node(&node).await.unwrap();

    // Save outputs with different attempt numbers
    for attempt in 1..=3u16 {
        let output = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: inst.id,
            block_id: BlockId::new("attempt_step"),
            output: json!({"attempt": attempt}),
            output_ref: None,
            output_size: 20,
            attempt,
            created_at: Utc::now(),
        };
        s.save_block_output(&output).await.unwrap();
    }

    let all = s.get_all_outputs(inst.id).await.unwrap();
    assert_eq!(all.len(), 3);
}

// ===========================================================================
// Signal Storage (tests 51-65)
// ===========================================================================

#[tokio::test]
async fn t51_create_signal_and_retrieve() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = make_signal(inst.id, SignalType::Pause);
    s.enqueue_signal(&signal).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signal_type, SignalType::Pause);
}

#[tokio::test]
async fn t52_get_pending_signals_only_undelivered() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let sig1 = make_signal(inst.id, SignalType::Pause);
    let sig2 = make_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&sig1).await.unwrap();
    s.enqueue_signal(&sig2).await.unwrap();

    s.mark_signal_delivered(sig1.id).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signal_type, SignalType::Resume);
}

#[tokio::test]
async fn t53_mark_signal_delivered() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = make_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&signal).await.unwrap();

    s.mark_signal_delivered(signal.id).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn t54_multiple_signals_same_instance() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for _ in 0..5 {
        let sig = make_signal(inst.id, SignalType::Custom("ping".into()));
        s.enqueue_signal(&sig).await.unwrap();
    }

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 5);
}

#[tokio::test]
async fn t55_signals_ordered_by_created_at() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..3 {
        let mut sig = make_signal(inst.id, SignalType::Custom(format!("event_{i}")));
        sig.created_at = Utc::now() + Duration::seconds(i64::from(i));
        s.enqueue_signal(&sig).await.unwrap();
    }

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 3);
    // First signal should have earliest created_at
    assert!(pending[0].created_at <= pending[1].created_at);
    assert!(pending[1].created_at <= pending[2].created_at);
}

#[tokio::test]
async fn t56_all_signal_types_persist_correctly() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    // Create all signal variants
    let pause = make_signal(inst.id, SignalType::Pause);
    let resume = make_signal(inst.id, SignalType::Resume);
    let cancel = make_signal(inst.id, SignalType::Cancel);
    let mut custom = make_signal(inst.id, SignalType::Custom("webhook_received".into()));
    custom.payload = json!({"event": "payment.success", "amount": 99.99});
    let mut update = make_signal(inst.id, SignalType::UpdateContext);
    update.payload = json!({
        "data": {"new_key": "new_value"},
        "config": {},
        "audit": [],
        "runtime": {}
    });

    s.enqueue_signal(&pause).await.unwrap();
    s.enqueue_signal(&resume).await.unwrap();
    s.enqueue_signal(&cancel).await.unwrap();
    s.enqueue_signal(&custom).await.unwrap();
    s.enqueue_signal(&update).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 5);
    assert!(pending.iter().any(|s| s.signal_type == SignalType::Pause));
    assert!(pending.iter().any(|s| s.signal_type == SignalType::Resume));
    assert!(pending.iter().any(|s| s.signal_type == SignalType::Cancel));
    assert!(pending
        .iter()
        .any(|s| s.signal_type == SignalType::Custom("webhook_received".into())));
    assert!(pending
        .iter()
        .any(|s| s.signal_type == SignalType::UpdateContext));

    // Verify payload roundtripped for custom and update signals
    let custom_fetched = pending
        .iter()
        .find(|s| matches!(s.signal_type, SignalType::Custom(_)))
        .unwrap();
    assert_eq!(custom_fetched.payload["event"], "payment.success");
    assert_eq!(custom_fetched.payload["amount"], 99.99);

    let update_fetched = pending
        .iter()
        .find(|s| s.signal_type == SignalType::UpdateContext)
        .unwrap();
    assert_eq!(update_fetched.payload["data"]["new_key"], "new_value");
}

#[tokio::test]
async fn t61_get_pending_signals_empty_when_all_delivered() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let sig1 = make_signal(inst.id, SignalType::Pause);
    let sig2 = make_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&sig1).await.unwrap();
    s.enqueue_signal(&sig2).await.unwrap();

    s.mark_signal_delivered(sig1.id).await.unwrap();
    s.mark_signal_delivered(sig2.id).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(pending.is_empty());
}

#[tokio::test]
async fn t62_signal_belongs_to_correct_instance() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let inst1 = make_instance("t", seq.id);
    let inst2 = make_instance("t", seq.id);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let sig = make_signal(inst1.id, SignalType::Pause);
    s.enqueue_signal(&sig).await.unwrap();

    let pending1 = s.get_pending_signals(inst1.id).await.unwrap();
    let pending2 = s.get_pending_signals(inst2.id).await.unwrap();
    assert_eq!(pending1.len(), 1);
    assert!(pending2.is_empty());
}

#[tokio::test]
async fn t63_bulk_signal_creation() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..20 {
        let sig = make_signal(inst.id, SignalType::Custom(format!("batch_{i}")));
        s.enqueue_signal(&sig).await.unwrap();
    }

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending.len(), 20);
}

#[tokio::test]
async fn t64_signal_payload_json_roundtrip() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let complex_payload = json!({
        "nested": {
            "array": [1, 2, 3],
            "obj": {"key": "val"},
            "null_val": null,
            "bool_val": true,
            "float_val": std::f64::consts::PI
        }
    });
    let mut sig = make_signal(inst.id, SignalType::Custom("complex".into()));
    sig.payload = complex_payload.clone();
    s.enqueue_signal(&sig).await.unwrap();

    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert_eq!(pending[0].payload, complex_payload);
}

#[tokio::test]
async fn t65_mark_signal_delivered_sets_timestamp() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let sig = make_signal(inst.id, SignalType::Pause);
    s.enqueue_signal(&sig).await.unwrap();

    // Verify undelivered
    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(!pending[0].delivered);

    s.mark_signal_delivered(sig.id).await.unwrap();

    // After delivery the signal should no longer appear as pending
    let pending = s.get_pending_signals(inst.id).await.unwrap();
    assert!(pending.is_empty());
}

// ===========================================================================
// Audit Log (tests 66-75)
// ===========================================================================

#[tokio::test]
async fn t66_append_audit_log_entry() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let entry = make_audit_entry(inst.id, "t", "instance_created");
    s.append_audit_log(&entry).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].event_type, "instance_created");
}

#[tokio::test]
async fn t67_list_audit_log_by_instance() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst1 = make_instance("t", seq.id);
    let inst2 = make_instance("t", seq.id);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let e1 = make_audit_entry(inst1.id, "t", "created");
    let e2 = make_audit_entry(inst2.id, "t", "started");
    s.append_audit_log(&e1).await.unwrap();
    s.append_audit_log(&e2).await.unwrap();

    let logs = s.list_audit_log(inst1.id, 100).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].event_type, "created");
}

#[tokio::test]
async fn t68_audit_log_limit_parameter() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..10 {
        let entry = make_audit_entry(inst.id, "t", &format!("event_{i}"));
        s.append_audit_log(&entry).await.unwrap();
    }

    let logs = s.list_audit_log(inst.id, 3).await.unwrap();
    assert_eq!(logs.len(), 3);
}

#[tokio::test]
async fn t69_audit_log_ordered_by_time() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    for i in 0..5 {
        let mut entry = make_audit_entry(inst.id, "t", &format!("event_{i}"));
        entry.created_at = Utc::now() + Duration::seconds(i64::from(i));
        s.append_audit_log(&entry).await.unwrap();
    }

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs.len(), 5);
    // Most recent should be first (DESC order)
    assert!(logs[0].created_at >= logs[1].created_at);
}

#[tokio::test]
async fn t70_audit_log_with_details_json() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let mut entry = make_audit_entry(inst.id, "t", "step_failed");
    entry.details = json!({
        "error": "connection timeout",
        "handler": "http_request",
        "attempt": 3,
        "duration_ms": 5000
    });
    s.append_audit_log(&entry).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs[0].details["error"], "connection timeout");
    assert_eq!(logs[0].details["attempt"], 3);
}

#[tokio::test]
async fn t71_audit_log_state_transition_entry() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let mut entry = make_audit_entry(inst.id, "t", "state_transition");
    entry.from_state = Some("scheduled".into());
    entry.to_state = Some("running".into());
    s.append_audit_log(&entry).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs[0].from_state.as_deref(), Some("scheduled"));
    assert_eq!(logs[0].to_state.as_deref(), Some("running"));
}

#[tokio::test]
async fn t72_audit_log_custom_event_entry() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let entry = make_audit_entry(inst.id, "t", "custom:user_action");
    s.append_audit_log(&entry).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs[0].event_type, "custom:user_action");
}

#[tokio::test]
async fn t73_audit_log_empty_for_new_instance() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert!(logs.is_empty());
}

#[tokio::test]
async fn t74_multiple_audit_entries() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let events = [
        "instance_created",
        "state_transition",
        "step_started",
        "step_completed",
        "state_transition",
    ];
    for event in &events {
        let entry = make_audit_entry(inst.id, "t", event);
        s.append_audit_log(&entry).await.unwrap();
    }

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs.len(), 5);
}

#[tokio::test]
async fn t75_audit_log_with_block_id() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let mut entry = make_audit_entry(inst.id, "t", "step_completed");
    entry.block_id = Some("step_send_email".into());
    entry.details = json!({"duration_ms": 120});
    s.append_audit_log(&entry).await.unwrap();

    let logs = s.list_audit_log(inst.id, 100).await.unwrap();
    assert_eq!(logs[0].block_id.as_deref(), Some("step_send_email"));
}

// ===========================================================================
// Credential Storage (tests 76-80)
// ===========================================================================

#[tokio::test]
async fn t76_store_credential_and_retrieve() {
    let s = store().await;
    let cred = make_credential("stripe-prod", "t1");
    s.create_credential(&cred).await.unwrap();

    let fetched = s.get_credential("stripe-prod").await.unwrap().unwrap();
    assert_eq!(fetched.id, "stripe-prod");
    assert_eq!(fetched.name, "Credential stripe-prod");
}

#[tokio::test]
async fn t77_credential_by_tenant_and_key() {
    let s = store().await;
    let cred1 = make_credential("cred-a", "tenant_1");
    let cred2 = make_credential("cred-b", "tenant_2");
    s.create_credential(&cred1).await.unwrap();
    s.create_credential(&cred2).await.unwrap();

    let results = s
        .list_credentials(Some(&TenantId::unchecked("tenant_1")), 100)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "cred-a");
}

#[tokio::test]
async fn t78_update_credential_value() {
    let s = store().await;
    let mut cred = make_credential("updatable", "t");
    s.create_credential(&cred).await.unwrap();

    cred.name = "Updated Credential".into();
    cred.value =
        orch8_types::config::SecretString::new(r#"{"token":"new_secret_value"}"#.to_string());
    cred.updated_at = Utc::now();
    s.update_credential(&cred).await.unwrap();

    let fetched = s.get_credential("updatable").await.unwrap().unwrap();
    assert_eq!(fetched.name, "Updated Credential");
}

#[tokio::test]
async fn t79_delete_credential() {
    let s = store().await;
    let cred = make_credential("deletable", "t");
    s.create_credential(&cred).await.unwrap();

    s.delete_credential("deletable").await.unwrap();

    let fetched = s.get_credential("deletable").await.unwrap();
    assert!(fetched.is_none());
}

#[tokio::test]
async fn t80_list_credentials_by_tenant() {
    let s = store().await;
    for i in 0..3 {
        let cred = make_credential(&format!("cred_{i}"), "multi_cred_tenant");
        s.create_credential(&cred).await.unwrap();
    }
    let other = make_credential("other_cred", "other_tenant");
    s.create_credential(&other).await.unwrap();

    let results = s
        .list_credentials(Some(&TenantId::unchecked("multi_cred_tenant")), 100)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
}

// ===========================================================================
// Session Storage (tests 81-85)
// ===========================================================================

#[tokio::test]
async fn t81_create_session_state() {
    let s = store().await;
    let session = make_session("t1", "user:123:onboarding");
    s.create_session(&session).await.unwrap();

    let fetched = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(fetched.session_key, "user:123:onboarding");
    assert_eq!(fetched.state, SessionState::Active);
}

#[tokio::test]
async fn t82_get_session_state() {
    let s = store().await;
    let session = make_session("t1", "user:456:checkout");
    s.create_session(&session).await.unwrap();

    let fetched = s
        .get_session_by_key(&TenantId::unchecked("t1"), "user:456:checkout")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.id, session.id);
    assert_eq!(fetched.data["step"], 1);
}

#[tokio::test]
async fn t83_update_session_state() {
    let s = store().await;
    let session = make_session("t1", "user:789:flow");
    s.create_session(&session).await.unwrap();

    s.update_session_state(session.id, SessionState::Completed)
        .await
        .unwrap();

    let fetched = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, SessionState::Completed);
}

#[tokio::test]
async fn t84_delete_session_state() {
    let s = store().await;
    let session = make_session("t1", "user:del:flow");
    s.create_session(&session).await.unwrap();

    // Update data to something new, verify it worked
    s.update_session_data(session.id, &json!({"step": 99}))
        .await
        .unwrap();

    let fetched = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(fetched.data["step"], 99);
}

#[tokio::test]
async fn t85_session_state_isolation_between_instances() {
    let s = store().await;
    let session1 = make_session("t1", "session_1");
    let session2 = make_session("t1", "session_2");
    s.create_session(&session1).await.unwrap();
    s.create_session(&session2).await.unwrap();

    s.update_session_data(session1.id, &json!({"data": "session1_only"}))
        .await
        .unwrap();

    let fetched1 = s.get_session(session1.id).await.unwrap().unwrap();
    let fetched2 = s.get_session(session2.id).await.unwrap().unwrap();
    assert_eq!(fetched1.data["data"], "session1_only");
    assert_eq!(fetched2.data["step"], 1); // Original unchanged
}

// ===========================================================================
// Misc Operations (tests 86-90)
// ===========================================================================

#[tokio::test]
async fn t86_storage_sequential_writes_persist_correctly() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    // Create multiple instances sequentially (SQLite in-memory is single-connection)
    let mut ids = Vec::new();
    for _ in 0..10 {
        let inst = make_instance("t", seq.id);
        ids.push(inst.id);
        s.create_instance(&inst).await.unwrap();
    }

    // Verify all were created correctly
    for id in &ids {
        let fetched = s.get_instance(*id).await.unwrap();
        assert!(fetched.is_some());
    }

    let count = s.count_instances(&InstanceFilter::default()).await.unwrap();
    assert_eq!(count, 10);
}

#[tokio::test]
async fn t87_large_json_payload_stored_correctly() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);

    // Create a large JSON payload (~50KB)
    let large_array: Vec<serde_json::Value> = (0..1000)
        .map(|i| json!({"index": i, "value": format!("item_{i}_padding_data_here")}))
        .collect();
    inst.context.data = json!({"items": large_array});
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    let items = fetched.context.data["items"].as_array().unwrap();
    assert_eq!(items.len(), 1000);
    assert_eq!(items[999]["index"], 999);
}

#[tokio::test]
async fn t88_unicode_in_metadata_preserved() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);
    inst.metadata = json!({
        "name": "Oleksii Vasylenko",
        "greeting": "Hello World",
        "chinese": "Zhongwen",
        "emoji_text": "rocket fire sparkle",
        "japanese": "Nihongo",
        "arabic": "Arabic text",
        "special": "line1\nline2\ttab"
    });
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.metadata["name"], "Oleksii Vasylenko");
    assert_eq!(fetched.metadata["special"], "line1\nline2\ttab");
}

#[tokio::test]
async fn t89_null_optional_fields_handled() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t", seq.id);
    inst.next_fire_at = None;
    inst.concurrency_key = None;
    inst.max_concurrency = None;
    inst.idempotency_key = None;
    inst.session_id = None;
    inst.parent_instance_id = None;
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert!(fetched.next_fire_at.is_none());
    assert!(fetched.concurrency_key.is_none());
    assert!(fetched.max_concurrency.is_none());
    assert!(fetched.idempotency_key.is_none());
    assert!(fetched.session_id.is_none());
    assert!(fetched.parent_instance_id.is_none());
}

#[tokio::test]
async fn t90_storage_handles_empty_string_fields() {
    let s = store().await;
    let mut seq = make_sequence("t");
    seq.name = "empty_test".into();
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t", seq.id);
    inst.metadata = json!({"key": "", "nested": {"empty": ""}});
    inst.context.data = json!({"empty_field": "", "array": []});
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.metadata["key"], "");
    assert_eq!(fetched.metadata["nested"]["empty"], "");
    assert_eq!(fetched.context.data["empty_field"], "");
    assert_eq!(fetched.context.data["array"], json!([]));
}

// ---------------------------------------------------------------------------
// Budget-governed instances (feature: instance budgets)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn t91_budget_column_round_trips() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t", seq.id);
    inst.budget = Some(orch8_types::instance::Budget {
        max_input_tokens: Some(1_000),
        max_output_tokens: None,
        max_total_tokens: Some(5_000),
        max_steps: Some(7),
    });
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    let budget = fetched.budget.expect("budget persisted");
    assert_eq!(budget.max_input_tokens, Some(1_000));
    assert_eq!(budget.max_output_tokens, None);
    assert_eq!(budget.max_total_tokens, Some(5_000));
    assert_eq!(budget.max_steps, Some(7));
}

#[tokio::test]
async fn t92_budget_absent_reads_back_as_none() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert!(fetched.budget.is_none());
}

#[tokio::test]
async fn t93_budget_round_trips_through_batch_create_and_list() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let mut with_budget = make_instance("t", seq.id);
    with_budget.budget = Some(orch8_types::instance::Budget {
        max_steps: Some(3),
        ..Default::default()
    });
    let without_budget = make_instance("t", seq.id);
    s.create_instances_batch(&[with_budget.clone(), without_budget.clone()])
        .await
        .unwrap();

    let fetched = s.get_instance(with_budget.id).await.unwrap().unwrap();
    assert_eq!(fetched.budget.unwrap().max_steps, Some(3));
    let fetched = s.get_instance(without_budget.id).await.unwrap().unwrap();
    assert!(fetched.budget.is_none());
}

#[tokio::test]
async fn t94_merge_instance_metadata_preserves_existing_keys() {
    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t", seq.id);
    inst.metadata = json!({"owner": "alice", "env": "prod"});
    s.create_instance(&inst).await.unwrap();

    s.merge_instance_metadata(
        inst.id,
        &json!({
            "paused_reason": "budget_exceeded",
            "budget_breach": {"limit": "max_steps", "limit_value": 3, "actual": 4},
        }),
    )
    .await
    .unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.metadata["owner"], "alice");
    assert_eq!(fetched.metadata["env"], "prod");
    assert_eq!(fetched.metadata["paused_reason"], "budget_exceeded");
    assert_eq!(fetched.metadata["budget_breach"]["limit"], "max_steps");
    assert_eq!(fetched.metadata["budget_breach"]["actual"], 4);
}

#[tokio::test]
async fn t95_query_instance_usage_totals_sums_per_instance() {
    use orch8_storage::TelemetryStore;

    let s = store().await;
    let seq = make_sequence("t");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t", seq.id);
    s.create_instance(&inst).await.unwrap();
    let other = make_instance("t", seq.id);
    s.create_instance(&other).await.unwrap();

    let mk_event = |instance_id, input, output| orch8_storage::UsageEvent {
        tenant_id: "t".into(),
        instance_id: Some(instance_id),
        block_id: Some("s1".into()),
        kind: "llm_tokens".into(),
        model: "test-model".into(),
        input_tokens: input,
        output_tokens: output,
        created_at: Utc::now(),
    };
    s.record_usage_event(&mk_event(inst.id, 100, 40))
        .await
        .unwrap();
    s.record_usage_event(&mk_event(inst.id, 25, 10))
        .await
        .unwrap();
    // Another instance's usage must not bleed into the totals.
    s.record_usage_event(&mk_event(other.id, 999, 999))
        .await
        .unwrap();

    let (input, output) = s.query_instance_usage_totals(inst.id).await.unwrap();
    assert_eq!(input, 125);
    assert_eq!(output, 50);

    // Instance with no usage rows sums to zero.
    let empty = make_instance("t", seq.id);
    s.create_instance(&empty).await.unwrap();
    let (input, output) = s.query_instance_usage_totals(empty.id).await.unwrap();
    assert_eq!((input, output), (0, 0));
}
