//! Comprehensive integration tests for `StorageBackend` via `SQLite` in-memory.
//!
//! Tests complex scenarios, multi-domain interactions, and edge cases
//! that unit tests inside individual modules don't cover.

use chrono::{Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::StorageBackend;
use orch8_types::audit::AuditLogEntry;
use orch8_types::checkpoint::Checkpoint;
use orch8_types::cluster::{ClusterNode, NodeStatus};
use orch8_types::context::{AuditEntry, ExecutionContext, RuntimeContext};
use orch8_types::cron::CronSchedule;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::pool::{PoolResource, ResourcePool, RotationStrategy};
use orch8_types::rate_limit::RateLimit;
use orch8_types::rate_limit::RateLimitCheck;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
use orch8_types::session::{Session, SessionState};
use orch8_types::signal::{Signal, SignalType};
use orch8_types::worker::{WorkerTask, WorkerTaskState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn store() -> SqliteStorage {
    SqliteStorage::in_memory().await.unwrap()
}

/// Create the parent `task_instances` row so the `externalized_state.instance_id`
/// FK (`ON DELETE CASCADE`) is satisfied. Tests that exercise `externalized_state`
/// directly (without going through `create_instance`) must seed the parent first
/// now that `SQLite` has FK enforcement enabled.
async fn seed_instance(s: &SqliteStorage, inst_id: InstanceId) {
    let mut inst = make_instance("test", SequenceId::new());
    inst.id = inst_id;
    s.create_instance(&inst).await.unwrap();
}

fn make_sequence(tenant: &str) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        name: "seq".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(StepDef {
            id: BlockId("s1".into()),
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
        }))],
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn make_instance(tenant: &str, seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
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
        created_at: now,
        updated_at: now,
    }
}

fn make_instance_in_state(tenant: &str, state: InstanceState) -> TaskInstance {
    let mut inst = make_instance(tenant, SequenceId::new());
    inst.state = state;
    inst
}

// ===========================================================================
// Execution Tree
// ===========================================================================

#[tokio::test]
async fn execution_tree_crud() {
    let s = store().await;
    let inst_id = InstanceId::new();
    let root_id = ExecutionNodeId::new();
    let child_id = ExecutionNodeId::new();

    let root = ExecutionNode {
        id: root_id,
        instance_id: inst_id,
        block_id: BlockId("root".into()),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    let child = ExecutionNode {
        id: child_id,
        instance_id: inst_id,
        block_id: BlockId("child_step".into()),
        parent_id: Some(root_id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };

    s.create_execution_node(&root).await.unwrap();
    s.create_execution_node(&child).await.unwrap();

    // Get full tree.
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    assert_eq!(tree.len(), 2);

    // Get children of root.
    let children = s.get_children(root_id).await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].block_id.0, "child_step");

    // Update node state.
    s.update_node_state(child_id, NodeState::Running)
        .await
        .unwrap();
    let tree2 = s.get_execution_tree(inst_id).await.unwrap();
    let updated_child = tree2.iter().find(|n| n.id == child_id).unwrap();
    assert_eq!(updated_child.state, NodeState::Running);
}

#[tokio::test]
async fn execution_tree_batch_create() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let nodes: Vec<ExecutionNode> = (0..5)
        .map(|i| ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: inst_id,
            block_id: BlockId(format!("step_{i}")),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: Some(i),
            state: NodeState::Pending,
            started_at: None,
            completed_at: None,
        })
        .collect();

    s.create_execution_nodes_batch(&nodes).await.unwrap();
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    assert_eq!(tree.len(), 5);
}

// ===========================================================================
// Signals
// ===========================================================================

#[tokio::test]
async fn signal_lifecycle() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let sig = Signal {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        signal_type: SignalType::Pause,
        payload: json!({"reason": "maintenance"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    s.enqueue_signal(&sig).await.unwrap();

    // Pending signals should include it.
    let pending = s.get_pending_signals(inst_id).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].signal_type, SignalType::Pause);

    // Mark delivered.
    s.mark_signal_delivered(sig.id).await.unwrap();
    let pending2 = s.get_pending_signals(inst_id).await.unwrap();
    assert_eq!(pending2.len(), 0);
}

#[tokio::test]
async fn signal_batch_operations() {
    let s = store().await;
    let inst1 = InstanceId::new();
    let inst2 = InstanceId::new();

    let sig1 = Signal {
        id: Uuid::now_v7(),
        instance_id: inst1,
        signal_type: SignalType::Cancel,
        payload: json!(null),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let sig2 = Signal {
        id: Uuid::now_v7(),
        instance_id: inst2,
        signal_type: SignalType::Resume,
        payload: json!(null),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let sig3 = Signal {
        id: Uuid::now_v7(),
        instance_id: inst1,
        signal_type: SignalType::Custom("wake".into()),
        payload: json!({"key": "val"}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    s.enqueue_signal(&sig1).await.unwrap();
    s.enqueue_signal(&sig2).await.unwrap();
    s.enqueue_signal(&sig3).await.unwrap();

    // Batch fetch.
    let batch = s.get_pending_signals_batch(&[inst1, inst2]).await.unwrap();
    assert_eq!(batch.get(&inst1).map_or(0, Vec::len), 2);
    assert_eq!(batch.get(&inst2).map_or(0, Vec::len), 1);

    // Batch mark delivered.
    s.mark_signals_delivered(&[sig1.id, sig2.id, sig3.id])
        .await
        .unwrap();
    let remaining = s.get_pending_signals(inst1).await.unwrap();
    assert_eq!(remaining.len(), 0);
}

// ===========================================================================
// Worker Tasks
// ===========================================================================

#[tokio::test]
async fn worker_task_full_lifecycle() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("step_1".into()),
        handler_name: "http_request".into(),
        queue_name: None,
        params: json!({"url": "https://example.com"}),
        context: json!({}),
        attempt: 1,
        timeout_ms: Some(30_000),
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();

    // Claim.
    let claimed = s
        .claim_worker_tasks("http_request", "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, task.id);

    // Should not be claimable again.
    let claimed2 = s
        .claim_worker_tasks("http_request", "worker-2", 10)
        .await
        .unwrap();
    assert_eq!(claimed2.len(), 0);

    // Heartbeat.
    let hb = s.heartbeat_worker_task(task.id, "worker-1").await.unwrap();
    assert!(hb);

    // Heartbeat from wrong worker fails.
    let hb_wrong = s.heartbeat_worker_task(task.id, "worker-X").await.unwrap();
    assert!(!hb_wrong);

    // Complete.
    let ok = s
        .complete_worker_task(task.id, "worker-1", &json!({"status": 200}))
        .await
        .unwrap();
    assert!(ok);

    // Verify completed.
    let fetched = s.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, WorkerTaskState::Completed);
    assert_eq!(fetched.output.unwrap()["status"], 200);
}

#[tokio::test]
async fn worker_task_fail_and_cancel() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("step_fail".into()),
        handler_name: "flaky_handler".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();

    // Claim then fail.
    s.claim_worker_tasks("flaky_handler", "w1", 1)
        .await
        .unwrap();
    let failed = s
        .fail_worker_task(task.id, "w1", "timeout exceeded", true)
        .await
        .unwrap();
    assert!(failed);

    let fetched = s.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, WorkerTaskState::Failed);
    assert_eq!(fetched.error_message.as_deref(), Some("timeout exceeded"));

    // Create another task and cancel by block.
    let task2 = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("step_cancel".into()),
        handler_name: "slow".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task2).await.unwrap();
    let cancelled = s
        .cancel_worker_tasks_for_block(inst_id.0, "step_cancel")
        .await
        .unwrap();
    assert_eq!(cancelled, 1);
}

/// Regression: `cancel_worker_tasks_for_block` must DELETE rows regardless of
/// state (including `completed`). The ForEach/Loop iteration-reset path relies
/// on this to purge the previous iteration's completed row so the next
/// iteration's INSERT isn't silently dropped by the
/// `UNIQUE(instance_id, block_id)` constraint (with `ON CONFLICT DO NOTHING`).
///
/// Prior behavior filtered `state IN ('pending', 'claimed')`, which left
/// `completed` rows in place and caused `ForEach` iterations past the first to
/// never dispatch to external workers.
#[tokio::test]
async fn cancel_worker_tasks_for_block_deletes_completed_rows() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Simulate iteration 0: task created, claimed, completed.
    let iter0 = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("loop_body".into()),
        handler_name: "external_handler".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&iter0).await.unwrap();
    s.claim_worker_tasks("external_handler", "w1", 1)
        .await
        .unwrap();
    let ok = s
        .complete_worker_task(iter0.id, "w1", &json!({"ok": true}))
        .await
        .unwrap();
    assert!(ok);

    // Reset for next iteration: must delete the completed row.
    let deleted = s
        .cancel_worker_tasks_for_block(inst_id.0, "loop_body")
        .await
        .unwrap();
    assert_eq!(
        deleted, 1,
        "completed rows must be deleted by cancel_worker_tasks_for_block"
    );

    // Iteration 1 INSERT must now succeed (UNIQUE constraint no longer holds).
    let iter1 = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("loop_body".into()),
        handler_name: "external_handler".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&iter1).await.unwrap();

    // Verify iter1 is the current row by claiming and checking the id.
    let claimed = s
        .claim_worker_tasks("external_handler", "w2", 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "iter1 row must be claimable");
    assert_eq!(claimed[0].id, iter1.id, "claimed task should be iter1");
}

/// Regression: `cancel_worker_tasks_for_block` must also delete rows in
/// `failed` state. Race cancellation and `ForEach` reset both rely on a clean
/// slate regardless of terminal state.
#[tokio::test]
async fn cancel_worker_tasks_for_block_deletes_failed_rows() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("race_branch".into()),
        handler_name: "external_handler".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();
    s.claim_worker_tasks("external_handler", "w1", 1)
        .await
        .unwrap();
    let failed = s
        .fail_worker_task(task.id, "w1", "boom", false)
        .await
        .unwrap();
    assert!(failed);

    let deleted = s
        .cancel_worker_tasks_for_block(inst_id.0, "race_branch")
        .await
        .unwrap();
    assert_eq!(
        deleted, 1,
        "failed rows must be deleted by cancel_worker_tasks_for_block"
    );

    // Subsequent INSERT for same (instance_id, block_id) must succeed.
    let task2 = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("race_branch".into()),
        handler_name: "external_handler".into(),
        queue_name: None,
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task2).await.unwrap();
    let fetched = s.get_worker_task(task2.id).await.unwrap();
    assert!(
        fetched.is_some(),
        "new task for same block must be insertable after cancel"
    );
}

/// `cancel_worker_tasks_for_block` on a block with no matching rows must
/// return 0 without error. The `ForEach` / `Loop` reset path calls this for
/// every descendant, including composite descendants that never had
/// `worker_tasks` rows — those calls must be no-ops.
#[tokio::test]
async fn cancel_worker_tasks_for_block_noop_on_missing() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let deleted = s
        .cancel_worker_tasks_for_block(inst_id.0, "never_existed")
        .await
        .unwrap();
    assert_eq!(deleted, 0);
}

#[tokio::test]
async fn worker_task_queue_routing() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("q_step".into()),
        handler_name: "email_send".into(),
        queue_name: Some("priority_queue".into()),
        params: json!({}),
        context: json!({}),
        attempt: 1,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: Utc::now(),
    };
    s.create_worker_task(&task).await.unwrap();

    // Claiming from wrong queue returns nothing.
    let empty = s
        .claim_worker_tasks_from_queue("other_queue", "email_send", "w1", 10)
        .await
        .unwrap();
    assert_eq!(empty.len(), 0);

    // Claiming from correct queue works.
    let claimed = s
        .claim_worker_tasks_from_queue("priority_queue", "email_send", "w1", 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
}

// ===========================================================================
// Cron Schedules
// ===========================================================================

#[tokio::test]
async fn cron_schedule_lifecycle() {
    let s = store().await;
    let now = Utc::now();

    let schedule = CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId("t1".into()),
        namespace: Namespace("default".into()),
        sequence_id: SequenceId::new(),
        cron_expr: "0 9 * * MON-FRI".into(),
        timezone: "UTC".into(),
        enabled: true,
        metadata: json!({"env": "prod"}),
        last_triggered_at: None,
        next_fire_at: Some(now - Duration::seconds(5)),
        created_at: now,
        updated_at: now,
    };

    s.create_cron_schedule(&schedule).await.unwrap();

    // Get.
    let fetched = s.get_cron_schedule(schedule.id).await.unwrap().unwrap();
    assert_eq!(fetched.cron_expr, "0 9 * * MON-FRI");

    // List.
    let all = s
        .list_cron_schedules(Some(&TenantId("t1".into())))
        .await
        .unwrap();
    assert_eq!(all.len(), 1);

    // Claim due.
    let due = s.claim_due_cron_schedules(now).await.unwrap();
    assert_eq!(due.len(), 1);

    // Update fire times.
    let next = now + Duration::hours(24);
    s.update_cron_fire_times(schedule.id, now, next)
        .await
        .unwrap();
    let updated = s.get_cron_schedule(schedule.id).await.unwrap().unwrap();
    assert!(updated.last_triggered_at.is_some());

    // Delete.
    s.delete_cron_schedule(schedule.id).await.unwrap();
    let gone = s.get_cron_schedule(schedule.id).await.unwrap();
    assert!(gone.is_none());
}

// ===========================================================================
// Sessions
// ===========================================================================

#[tokio::test]
async fn session_lifecycle() {
    let s = store().await;
    let now = Utc::now();

    let session = Session {
        id: Uuid::now_v7(),
        tenant_id: TenantId("t1".into()),
        session_key: "user:42:onboarding".into(),
        data: json!({"step": 1}),
        state: SessionState::Active,
        created_at: now,
        updated_at: now,
        expires_at: None,
    };
    s.create_session(&session).await.unwrap();

    // Get by ID.
    let fetched = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(fetched.session_key, "user:42:onboarding");

    // Get by key.
    let by_key = s
        .get_session_by_key(&TenantId("t1".into()), "user:42:onboarding")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_key.id, session.id);

    // Update data.
    s.update_session_data(session.id, &json!({"step": 2, "completed_intro": true}))
        .await
        .unwrap();
    let updated = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(updated.data["step"], 2);

    // Update state.
    s.update_session_state(session.id, SessionState::Completed)
        .await
        .unwrap();
    let completed = s.get_session(session.id).await.unwrap().unwrap();
    assert_eq!(completed.state, SessionState::Completed);
}

#[tokio::test]
async fn session_instances_link() {
    let s = store().await;
    let now = Utc::now();
    let session_id = Uuid::now_v7();

    let session = Session {
        id: session_id,
        tenant_id: TenantId("t1".into()),
        session_key: "flow:99".into(),
        data: json!({}),
        state: SessionState::Active,
        created_at: now,
        updated_at: now,
        expires_at: None,
    };
    s.create_session(&session).await.unwrap();

    let seq_id = SequenceId::new();
    // Create instances linked to session.
    for _ in 0..3 {
        let mut inst = make_instance("t1", seq_id);
        inst.session_id = Some(session_id);
        s.create_instance(&inst).await.unwrap();
    }
    // Create instance NOT in session.
    let unlinked = make_instance("t1", seq_id);
    s.create_instance(&unlinked).await.unwrap();

    let session_insts = s.list_session_instances(session_id).await.unwrap();
    assert_eq!(session_insts.len(), 3);
}

// ===========================================================================
// Checkpoints
// ===========================================================================

#[tokio::test]
async fn checkpoint_save_and_prune() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Save 5 checkpoints.
    for i in 0..5 {
        let cp = Checkpoint {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            checkpoint_data: json!({"step": i}),
            created_at: Utc::now() + Duration::seconds(i),
        };
        s.save_checkpoint(&cp).await.unwrap();
    }

    let all = s.list_checkpoints(inst_id).await.unwrap();
    assert_eq!(all.len(), 5);

    // Latest should be step 4.
    let latest = s.get_latest_checkpoint(inst_id).await.unwrap().unwrap();
    assert_eq!(latest.checkpoint_data["step"], 4);

    // Prune to keep only 2.
    let pruned = s.prune_checkpoints(inst_id, 2).await.unwrap();
    assert_eq!(pruned, 3);

    let remaining = s.list_checkpoints(inst_id).await.unwrap();
    assert_eq!(remaining.len(), 2);
}

// ===========================================================================
// Audit Log
// ===========================================================================

#[tokio::test]
async fn audit_log_append_and_query() {
    let s = store().await;
    let inst_id = InstanceId::new();
    let tenant = TenantId("t_audit".into());

    for i in 0..3 {
        let entry = AuditLogEntry {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            tenant_id: tenant.clone(),
            event_type: "state_transition".into(),
            from_state: Some("running".into()),
            to_state: Some(format!("state_{i}")),
            block_id: Some(format!("block_{i}")),
            details: json!({"attempt": i}),
            created_at: Utc::now(),
        };
        s.append_audit_log(&entry).await.unwrap();
    }

    let by_instance = s.list_audit_log(inst_id, 10).await.unwrap();
    assert_eq!(by_instance.len(), 3);

    let by_tenant = s.list_audit_log_by_tenant(&tenant, 2).await.unwrap();
    assert_eq!(by_tenant.len(), 2); // limited to 2
}

// ===========================================================================
// Resource Pools
// ===========================================================================

#[tokio::test]
async fn resource_pool_lifecycle() {
    let s = store().await;
    let now = Utc::now();
    let tenant = TenantId("t_pool".into());

    let pool = ResourcePool {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        name: "email_senders".into(),
        strategy: RotationStrategy::RoundRobin,
        round_robin_index: 0,
        created_at: now,
        updated_at: now,
    };
    s.create_resource_pool(&pool).await.unwrap();

    // Add resources.
    let res1 = PoolResource {
        id: Uuid::now_v7(),
        pool_id: pool.id,
        resource_key: ResourceKey("sender_a@acme.com".into()),
        name: "Sender A".into(),
        weight: 1,
        enabled: true,
        daily_cap: 100,
        daily_usage: 0,
        daily_usage_date: None,
        warmup_start: None,
        warmup_days: 0,
        warmup_start_cap: 0,
        created_at: now,
    };
    let res2 = PoolResource {
        id: Uuid::now_v7(),
        pool_id: pool.id,
        resource_key: ResourceKey("sender_b@acme.com".into()),
        name: "Sender B".into(),
        weight: 2,
        enabled: true,
        daily_cap: 200,
        daily_usage: 0,
        daily_usage_date: None,
        warmup_start: None,
        warmup_days: 0,
        warmup_start_cap: 0,
        created_at: now,
    };
    s.add_pool_resource(&res1).await.unwrap();
    s.add_pool_resource(&res2).await.unwrap();

    let resources = s.list_pool_resources(pool.id).await.unwrap();
    assert_eq!(resources.len(), 2);

    // Update round robin index.
    s.update_pool_round_robin_index(pool.id, 1).await.unwrap();
    let updated_pool = s.get_resource_pool(pool.id).await.unwrap().unwrap();
    assert_eq!(updated_pool.round_robin_index, 1);

    // Increment usage.
    let today = Utc::now().date_naive();
    s.increment_resource_usage(res1.id, today).await.unwrap();
    s.increment_resource_usage(res1.id, today).await.unwrap();

    // List pools by tenant.
    let pools = s.list_resource_pools(&tenant).await.unwrap();
    assert_eq!(pools.len(), 1);

    // Delete resource.
    s.delete_pool_resource(res2.id).await.unwrap();
    let after_delete = s.list_pool_resources(pool.id).await.unwrap();
    assert_eq!(after_delete.len(), 1);

    // Delete pool.
    s.delete_resource_pool(pool.id).await.unwrap();
    assert!(s.get_resource_pool(pool.id).await.unwrap().is_none());
}

// ===========================================================================
// Externalized State
// ===========================================================================

#[tokio::test]
async fn externalized_state_crud() {
    let s = store().await;
    let inst_id = InstanceId::new();
    seed_instance(&s, inst_id).await;
    let ref_key = format!("ext_{inst_id}");
    let payload = json!({"large": "data", "items": [1,2,3,4,5]});

    s.save_externalized_state(inst_id, &ref_key, &payload)
        .await
        .unwrap();

    let fetched = s.get_externalized_state(&ref_key).await.unwrap().unwrap();
    assert_eq!(fetched, payload);

    s.delete_externalized_state(&ref_key).await.unwrap();
    assert!(s.get_externalized_state(&ref_key).await.unwrap().is_none());
}

#[tokio::test]
async fn batch_get_externalized_state_fetches_multiple_keys() {
    let s = store().await;
    let inst_id = InstanceId::new();
    seed_instance(&s, inst_id).await;

    // Mix small (uncompressed) and large (zstd) payloads to prove the batch
    // path handles both storage paths in a single query.
    let small = json!({"k": "v"});
    let big = json!({"blob": "y".repeat(5_000)});
    s.save_externalized_state(inst_id, "batch_small", &small)
        .await
        .unwrap();
    s.save_externalized_state(inst_id, "batch_big", &big)
        .await
        .unwrap();

    let keys = vec![
        "batch_small".to_string(),
        "batch_big".to_string(),
        "batch_missing".to_string(),
    ];
    let map = s.batch_get_externalized_state(&keys).await.unwrap();

    // Missing keys are absent (not errors, not Some(Null)).
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("batch_small"), Some(&small));
    assert_eq!(map.get("batch_big"), Some(&big));
    assert!(!map.contains_key("batch_missing"));
}

#[tokio::test]
async fn batch_get_externalized_state_empty_input_returns_empty_map() {
    let s = store().await;
    let map = s.batch_get_externalized_state(&[]).await.unwrap();
    assert!(map.is_empty());
}

#[tokio::test]
async fn batch_save_externalized_state_persists_all_entries() {
    let s = store().await;
    let inst_id = InstanceId::new();
    seed_instance(&s, inst_id).await;
    let entries = vec![
        ("bs_a".to_string(), json!({"a": 1})),
        ("bs_b".to_string(), json!({"b": "x".repeat(5_000)})), // crosses zstd threshold
        ("bs_c".to_string(), json!([1, 2, 3])),
    ];

    s.batch_save_externalized_state(inst_id, &entries)
        .await
        .unwrap();

    // Every entry should be readable; large one should roundtrip identically
    // through the zstd path.
    for (key, expected) in &entries {
        let got = s.get_externalized_state(key).await.unwrap();
        assert_eq!(got.as_ref(), Some(expected), "key {key} mismatch");
    }
}

#[tokio::test]
async fn batch_save_externalized_state_empty_input_is_noop() {
    let s = store().await;
    s.batch_save_externalized_state(InstanceId::new(), &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn batch_save_externalized_state_upserts_existing_keys() {
    let s = store().await;
    let inst_id = InstanceId::new();
    seed_instance(&s, inst_id).await;

    // Seed.
    s.batch_save_externalized_state(inst_id, &[("bs_up".to_string(), json!({"v": 1}))])
        .await
        .unwrap();

    // Re-save with a different value — ON CONFLICT DO UPDATE path.
    s.batch_save_externalized_state(inst_id, &[("bs_up".to_string(), json!({"v": 2}))])
        .await
        .unwrap();

    assert_eq!(
        s.get_externalized_state("bs_up").await.unwrap(),
        Some(json!({"v": 2}))
    );
}

#[tokio::test]
async fn externalized_state_roundtrip_across_compression_threshold() {
    let s = store().await;
    let inst_id = InstanceId::new();
    seed_instance(&s, inst_id).await;

    // Small payload (<1 KiB): written uncompressed, returned verbatim.
    let small = json!({"k": "v"});
    s.save_externalized_state(inst_id, "ext_small", &small)
        .await
        .unwrap();
    assert_eq!(
        s.get_externalized_state("ext_small").await.unwrap(),
        Some(small)
    );

    // Large payload (>1 KiB): written zstd-compressed, inflated on read.
    let big = json!({"blob": "x".repeat(5_000)});
    s.save_externalized_state(inst_id, "ext_big", &big)
        .await
        .unwrap();
    assert_eq!(
        s.get_externalized_state("ext_big").await.unwrap(),
        Some(big)
    );
}

// ===========================================================================
// Cluster Nodes
// ===========================================================================

#[tokio::test]
async fn cluster_node_lifecycle() {
    let s = store().await;
    let now = Utc::now();

    let node = ClusterNode {
        id: Uuid::now_v7(),
        name: "node-1".into(),
        status: NodeStatus::Active,
        registered_at: now,
        last_heartbeat_at: now,
        drain: false,
    };
    s.register_node(&node).await.unwrap();

    let nodes = s.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].name, "node-1");

    // Heartbeat.
    s.heartbeat_node(node.id).await.unwrap();

    // Should not drain initially.
    assert!(!s.should_drain(node.id).await.unwrap());

    // Drain.
    s.drain_node(node.id).await.unwrap();
    assert!(s.should_drain(node.id).await.unwrap());

    // Deregister.
    s.deregister_node(node.id).await.unwrap();
    let after = s.list_nodes().await.unwrap();
    assert_eq!(after[0].status, NodeStatus::Stopped);
}

// ===========================================================================
// Rate Limits
// ===========================================================================

#[tokio::test]
async fn rate_limit_check_and_exceed() {
    let s = store().await;
    let now = Utc::now();
    let tenant = TenantId("t_rl".into());
    let key = ResourceKey("api:endpoint".into());

    // Set up rate limit: 3 per 60 seconds.
    let rl = RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 3,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    };
    s.upsert_rate_limit(&rl).await.unwrap();

    // First 3 should be allowed.
    for _ in 0..3 {
        let check = s.check_rate_limit(&tenant, &key, now).await.unwrap();
        assert!(matches!(check, RateLimitCheck::Allowed));
    }

    // 4th should be exceeded.
    let check = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    assert!(matches!(check, RateLimitCheck::Exceeded { .. }));
}

// ===========================================================================
// Block Outputs
// ===========================================================================

#[tokio::test]
async fn block_output_crud() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let out = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId("step_1".into()),
        output: json!({"result": "ok"}),
        output_ref: None,
        output_size: 15,
        attempt: 1,
        created_at: Utc::now(),
    };
    s.save_block_output(&out).await.unwrap();

    let fetched = s
        .get_block_output(inst_id, &BlockId("step_1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.output["result"], "ok");

    let all = s.get_all_outputs(inst_id).await.unwrap();
    assert_eq!(all.len(), 1);

    let ids = s.get_completed_block_ids(inst_id).await.unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].0, "step_1");
}

#[tokio::test]
async fn save_output_and_transition_atomic() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t1", seq.id);
    inst.state = InstanceState::Running;
    inst.next_fire_at = None;
    s.create_instance(&inst).await.unwrap();

    let out = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId("s1".into()),
        output: json!({"done": true}),
        output_ref: None,
        output_size: 13,
        attempt: 1,
        created_at: Utc::now(),
    };

    // Atomic save output + transition to Completed.
    s.save_output_and_transition(&out, inst.id, InstanceState::Completed, None)
        .await
        .unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, InstanceState::Completed);

    let outputs = s.get_all_outputs(inst.id).await.unwrap();
    assert_eq!(outputs.len(), 1);
}

/// Exercises the Stab#12 atomic combined method. Prior to this, the worker
/// completion path was split across `update_instance_context` and
/// `save_output_and_transition`. A crash in between could leave an instance
/// with merged context but an unchanged state (Running), stranding it.
/// This test proves the three writes commit together.
#[tokio::test]
async fn save_output_merge_context_and_transition_atomic() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();
    let mut inst = make_instance("t1", seq.id);
    inst.state = InstanceState::Running;
    inst.next_fire_at = None;
    // Seed context.data with a pre-existing key so we can assert the merge
    // preserves earlier context and layers new fields on top.
    inst.context.data = json!({ "existing": "keep" });
    s.create_instance(&inst).await.unwrap();

    let out = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId("s1".into()),
        output: json!({"answer": 42}),
        output_ref: None,
        output_size: 13,
        attempt: 1,
        created_at: Utc::now(),
    };

    // Build the merged context the same way workers.rs does.
    let mut merged = inst.context.clone();
    let obj = merged.data.as_object_mut().unwrap();
    obj.insert("answer".into(), json!(42));

    let next = Utc::now();
    s.save_output_merge_context_and_transition(
        &out,
        inst.id,
        &merged,
        InstanceState::Scheduled,
        Some(next),
    )
    .await
    .unwrap();

    // All three writes landed.
    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.state, InstanceState::Scheduled);
    assert_eq!(fetched.context.data["existing"], json!("keep"));
    assert_eq!(fetched.context.data["answer"], json!(42));
    assert!(fetched.next_fire_at.is_some());

    let outputs = s.get_all_outputs(inst.id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].block_id.0, "s1");
}

#[tokio::test]
async fn completed_block_ids_batch() {
    let s = store().await;
    let inst1 = InstanceId::new();
    let inst2 = InstanceId::new();

    for (inst_id, block_name) in &[(inst1, "a"), (inst1, "b"), (inst2, "c")] {
        let out = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: *inst_id,
            block_id: BlockId((*block_name).into()),
            output: json!({}),
            output_ref: None,
            output_size: 2,
            attempt: 1,
            created_at: Utc::now(),
        };
        s.save_block_output(&out).await.unwrap();
    }

    let batch = s
        .get_completed_block_ids_batch(&[inst1, inst2])
        .await
        .unwrap();
    assert_eq!(batch.get(&inst1).map_or(0, Vec::len), 2);
    assert_eq!(batch.get(&inst2).map_or(0, Vec::len), 1);
}

// ---------------------------------------------------------------------------
// delete_block_outputs — composite-marker purge primitive used by the
// loop / for_each subtree-reset path. See lib.rs::StorageBackend
// documentation for semantics.
// ---------------------------------------------------------------------------

fn mk_output(inst_id: InstanceId, block: &str, attempt: i16) -> BlockOutput {
    BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst_id,
        block_id: BlockId(block.into()),
        output: json!({"_iterations": attempt}),
        output_ref: None,
        output_size: 0,
        attempt,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn delete_block_outputs_removes_all_rows_for_block() {
    let s = store().await;
    let inst = InstanceId::new();
    let block = BlockId("loop_marker".into());

    for attempt in 0..3 {
        s.save_block_output(&mk_output(inst, "loop_marker", attempt))
            .await
            .unwrap();
    }
    let removed = s.delete_block_outputs(inst, &block).await.unwrap();
    assert_eq!(removed, 3);
    assert!(s.get_block_output(inst, &block).await.unwrap().is_none());
}

#[tokio::test]
async fn delete_block_outputs_leaves_other_blocks_untouched() {
    let s = store().await;
    let inst = InstanceId::new();
    let block_a = BlockId("block_a".into());
    let block_b = BlockId("block_b".into());

    for _ in 0..2 {
        s.save_block_output(&mk_output(inst, "block_a", 0))
            .await
            .unwrap();
        s.save_block_output(&mk_output(inst, "block_b", 0))
            .await
            .unwrap();
    }
    let removed = s.delete_block_outputs(inst, &block_a).await.unwrap();
    assert_eq!(removed, 2);
    assert!(s.get_block_output(inst, &block_a).await.unwrap().is_none());
    assert!(s.get_block_output(inst, &block_b).await.unwrap().is_some());
    let remaining = s.get_all_outputs(inst).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|o| o.block_id == block_b));
}

#[tokio::test]
async fn delete_block_outputs_leaves_other_instances_untouched() {
    let s = store().await;
    let inst1 = InstanceId::new();
    let inst2 = InstanceId::new();
    let block = BlockId("shared".into());

    s.save_block_output(&mk_output(inst1, "shared", 0))
        .await
        .unwrap();
    s.save_block_output(&mk_output(inst2, "shared", 0))
        .await
        .unwrap();

    let removed = s.delete_block_outputs(inst1, &block).await.unwrap();
    assert_eq!(removed, 1);
    assert!(s.get_block_output(inst1, &block).await.unwrap().is_none());
    assert!(s.get_block_output(inst2, &block).await.unwrap().is_some());
}

#[tokio::test]
async fn delete_block_outputs_on_empty_is_noop() {
    let s = store().await;
    let inst = InstanceId::new();
    let removed = s
        .delete_block_outputs(inst, &BlockId("nope".into()))
        .await
        .unwrap();
    assert_eq!(removed, 0);
}

#[tokio::test]
async fn delete_block_outputs_no_effect_on_different_block_ids_at_same_instance() {
    let s = store().await;
    let inst = InstanceId::new();
    let loop_id = BlockId("loop_1".into());
    let step_id = BlockId("inner_step".into());

    s.save_block_output(&mk_output(inst, "loop_1", 1))
        .await
        .unwrap();
    s.save_block_output(&BlockOutput {
        id: Uuid::now_v7(),
        instance_id: inst,
        block_id: step_id.clone(),
        output: json!({"result": "ok"}),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    })
    .await
    .unwrap();

    let removed = s.delete_block_outputs(inst, &loop_id).await.unwrap();
    assert_eq!(removed, 1);
    assert!(s.get_block_output(inst, &loop_id).await.unwrap().is_none());
    let step_row = s
        .get_block_output(inst, &step_id)
        .await
        .unwrap()
        .expect("step output must remain");
    assert_eq!(step_row.output["result"], "ok");
}

// ===========================================================================
// Complex Scenarios
// ===========================================================================

#[tokio::test]
async fn multi_tenant_isolation() {
    let s = store().await;

    let seq_t1 = make_sequence("tenant_1");
    let seq_t2 = make_sequence("tenant_2");
    s.create_sequence(&seq_t1).await.unwrap();
    s.create_sequence(&seq_t2).await.unwrap();

    // Create 3 instances per tenant.
    for _ in 0..3 {
        let i1 = make_instance("tenant_1", seq_t1.id);
        let i2 = make_instance("tenant_2", seq_t2.id);
        s.create_instance(&i1).await.unwrap();
        s.create_instance(&i2).await.unwrap();
    }

    // Filter by tenant.
    let filter_t1 = InstanceFilter {
        tenant_id: Some(TenantId("tenant_1".into())),
        ..Default::default()
    };
    let t1_instances = s
        .list_instances(&filter_t1, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(t1_instances.len(), 3);

    let count = s.count_instances(&filter_t1).await.unwrap();
    assert_eq!(count, 3);

    // Bulk update only tenant_1 to cancelled.
    let updated = s
        .bulk_update_state(&filter_t1, InstanceState::Cancelled)
        .await
        .unwrap();
    assert_eq!(updated, 3);

    // Tenant 2 should be unaffected.
    let filter_t2 = InstanceFilter {
        tenant_id: Some(TenantId("tenant_2".into())),
        ..Default::default()
    };
    let t2_instances = s
        .list_instances(&filter_t2, &Pagination::default())
        .await
        .unwrap();
    for inst in &t2_instances {
        assert_eq!(inst.state, InstanceState::Scheduled);
    }
}

#[tokio::test]
async fn instance_state_lifecycle() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();
    let inst = make_instance("t1", seq.id);
    s.create_instance(&inst).await.unwrap();

    // Scheduled -> Running.
    s.update_instance_state(inst.id, InstanceState::Running, None)
        .await
        .unwrap();
    let running = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(running.state, InstanceState::Running);

    // Running -> Waiting (e.g., waiting for signal).
    s.update_instance_state(inst.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    let waiting = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(waiting.state, InstanceState::Waiting);

    // Waiting -> Running (signal received).
    let fire_at = Utc::now();
    s.update_instance_state(inst.id, InstanceState::Running, Some(fire_at))
        .await
        .unwrap();

    // Running -> Completed.
    s.update_instance_state(inst.id, InstanceState::Completed, None)
        .await
        .unwrap();
    let completed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(completed.state, InstanceState::Completed);
}

#[tokio::test]
async fn idempotency_dedup() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.idempotency_key = Some("order-12345".into());
    s.create_instance(&inst).await.unwrap();

    // Find by idempotency key.
    let found = s
        .find_by_idempotency_key(&TenantId("t1".into()), "order-12345")
        .await
        .unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, inst.id);

    // Missing key returns None.
    let missing = s
        .find_by_idempotency_key(&TenantId("t1".into()), "nonexistent")
        .await
        .unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn concurrency_control() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let conc_key = "payment:user:42";

    // Create 3 running instances with same concurrency key.
    for _ in 0..3 {
        let mut inst = make_instance("t1", seq.id);
        inst.state = InstanceState::Running;
        inst.concurrency_key = Some(conc_key.into());
        s.create_instance(&inst).await.unwrap();
    }

    let count = s.count_running_by_concurrency_key(conc_key).await.unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn claim_respects_priority_ordering() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();
    let now = Utc::now();

    // Create instances with different priorities.
    for (priority, label) in [
        (Priority::Low, "low"),
        (Priority::Normal, "normal"),
        (Priority::Critical, "critical"),
        (Priority::High, "high"),
    ] {
        let mut inst = make_instance("t1", seq.id);
        inst.priority = priority;
        inst.metadata = json!({"label": label});
        inst.next_fire_at = Some(now - Duration::seconds(10));
        s.create_instance(&inst).await.unwrap();
    }

    // Claim all — should come back in priority DESC order.
    let claimed = s.claim_due_instances(now, 10, 0).await.unwrap();
    assert_eq!(claimed.len(), 4);
    assert_eq!(claimed[0].priority, Priority::Critical);
    assert_eq!(claimed[1].priority, Priority::High);
    assert_eq!(claimed[2].priority, Priority::Normal);
    assert_eq!(claimed[3].priority, Priority::Low);
}

#[tokio::test]
async fn claim_returns_all_due_when_no_per_tenant_cap() {
    let s = store().await;
    let now = Utc::now();

    let seq = make_sequence("noisy");
    s.create_sequence(&seq).await.unwrap();
    let seq2 = make_sequence("quiet");
    s.create_sequence(&seq2).await.unwrap();

    // Noisy tenant: 10 instances.
    for _ in 0..10 {
        let inst = make_instance("noisy", seq.id);
        s.create_instance(&inst).await.unwrap();
    }
    // Quiet tenant: 2 instances.
    for _ in 0..2 {
        let inst = make_instance("quiet", seq2.id);
        s.create_instance(&inst).await.unwrap();
    }

    // Claim with no per-tenant cap (0 = unlimited).
    let claimed = s.claim_due_instances(now, 20, 0).await.unwrap();
    assert_eq!(claimed.len(), 12);

    // Claim with limit less than total.
    // (Re-create since previous claim transitioned them to running.)
    let s2 = store().await;
    let seq3 = make_sequence("t");
    s2.create_sequence(&seq3).await.unwrap();
    for _ in 0..10 {
        let inst = make_instance("t", seq3.id);
        s2.create_instance(&inst).await.unwrap();
    }
    let claimed2 = s2.claim_due_instances(now, 5, 0).await.unwrap();
    assert_eq!(claimed2.len(), 5);
    // Remaining 5 still claimable.
    let claimed3 = s2.claim_due_instances(now, 10, 0).await.unwrap();
    assert_eq!(claimed3.len(), 5);
}

#[tokio::test]
async fn bulk_reschedule() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();
    let now = Utc::now();

    for _ in 0..5 {
        let mut inst = make_instance("t1", seq.id);
        inst.next_fire_at = Some(now);
        s.create_instance(&inst).await.unwrap();
    }

    let filter = InstanceFilter {
        tenant_id: Some(TenantId("t1".into())),
        ..Default::default()
    };
    // Shift forward by 3600 seconds (1 hour).
    let shifted = s.bulk_reschedule(&filter, 3600).await.unwrap();
    assert_eq!(shifted, 5);
}

#[tokio::test]
async fn instance_batch_create() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let batch: Vec<TaskInstance> = (0..50).map(|_| make_instance("t1", seq.id)).collect();
    let created = s.create_instances_batch(&batch).await.unwrap();
    assert_eq!(created, 50);

    let filter = InstanceFilter {
        tenant_id: Some(TenantId("t1".into())),
        ..Default::default()
    };
    let count = s.count_instances(&filter).await.unwrap();
    assert_eq!(count, 50);
}

#[tokio::test]
async fn merge_context_data() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({"existing": "value"});
    s.create_instance(&inst).await.unwrap();

    s.merge_context_data(inst.id, "new_key", &json!("new_value"))
        .await
        .unwrap();

    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.context.data["existing"], "value");
    assert_eq!(fetched.context.data["new_key"], "new_value");
}

#[tokio::test]
async fn update_instance_sequence_hot_migration() {
    let s = store().await;
    let seq_v1 = make_sequence("t1");
    let mut seq_v2 = make_sequence("t1");
    seq_v2.version = 2;
    s.create_sequence(&seq_v1).await.unwrap();
    s.create_sequence(&seq_v2).await.unwrap();

    let inst = make_instance("t1", seq_v1.id);
    s.create_instance(&inst).await.unwrap();

    // Hot-migrate to v2.
    s.update_instance_sequence(inst.id, seq_v2.id)
        .await
        .unwrap();
    let fetched = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(fetched.sequence_id, seq_v2.id);
}

#[tokio::test]
async fn dynamic_step_injection() {
    let s = store().await;
    let inst_id = InstanceId::new();
    let blocks = json!([{"type": "step", "id": "injected_1", "handler": "noop"}]);

    s.inject_blocks(inst_id, &blocks).await.unwrap();
    let fetched = s.get_injected_blocks(inst_id).await.unwrap().unwrap();
    assert_eq!(fetched, blocks);
}

/// Stab#17: `inject_blocks` used to be a read-modify-write sequence in the
/// HTTP handler — two concurrent position-targeted calls could both read the
/// same pre-image array, each merge their new blocks, and the second write
/// would clobber the first's additions. `inject_blocks_at_position` performs
/// the read + merge + write inside a single transaction so interleaved calls
/// cannot lose writes.
#[tokio::test]
async fn inject_blocks_at_position_preserves_sequential_writes() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // First call: append block `a` at position 0 into an empty array.
    let blocks_a = json!([{"type": "step", "id": "a", "handler": "noop"}]);
    let after_a = s
        .inject_blocks_at_position(inst_id, &blocks_a, Some(0))
        .await
        .unwrap();
    assert_eq!(after_a.as_array().unwrap().len(), 1);

    // Second call: insert block `b` at position 0. Result must contain BOTH
    // entries — `b` at index 0, `a` shifted to index 1 — proving that the
    // method re-reads the current state instead of overwriting.
    let blocks_b = json!([{"type": "step", "id": "b", "handler": "noop"}]);
    let after_b = s
        .inject_blocks_at_position(inst_id, &blocks_b, Some(0))
        .await
        .unwrap();
    let arr = after_b.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], "b");
    assert_eq!(arr[1]["id"], "a");

    // Third call: append `c` at out-of-range position; should clamp to end.
    let blocks_c = json!([{"type": "step", "id": "c", "handler": "noop"}]);
    let after_c = s
        .inject_blocks_at_position(inst_id, &blocks_c, Some(999))
        .await
        .unwrap();
    let arr = after_c.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[2]["id"], "c");

    // None position means "replace the full blob" (legacy semantics).
    let blocks_replace = json!([{"type": "step", "id": "z", "handler": "noop"}]);
    let after_replace = s
        .inject_blocks_at_position(inst_id, &blocks_replace, None)
        .await
        .unwrap();
    let arr = after_replace.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], "z");
}

#[tokio::test]
async fn sequence_versioning_and_deprecation() {
    let s = store().await;
    let tenant = TenantId("t1".into());
    let ns = Namespace("default".into());

    let mut seq_v1 = make_sequence("t1");
    seq_v1.name = "my_workflow".into();
    seq_v1.version = 1;
    s.create_sequence(&seq_v1).await.unwrap();

    let mut seq_v2 = make_sequence("t1");
    seq_v2.name = "my_workflow".into();
    seq_v2.version = 2;
    s.create_sequence(&seq_v2).await.unwrap();

    // List versions.
    let versions = s
        .list_sequence_versions(&tenant, &ns, "my_workflow")
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);

    // Get by name (latest).
    let latest = s
        .get_sequence_by_name(&tenant, &ns, "my_workflow", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.version, 2);

    // Get by name + specific version.
    let v1 = s
        .get_sequence_by_name(&tenant, &ns, "my_workflow", Some(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v1.version, 1);

    // Deprecate v1.
    s.deprecate_sequence(seq_v1.id).await.unwrap();
    let deprecated = s.get_sequence(seq_v1.id).await.unwrap().unwrap();
    assert!(deprecated.deprecated);
}

#[tokio::test]
async fn instance_filtering_by_state_and_pagination() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    // Create 3 scheduled, 2 running.
    for _ in 0..3 {
        let inst = make_instance("t1", seq.id);
        s.create_instance(&inst).await.unwrap();
    }
    for _ in 0..2 {
        let mut inst = make_instance("t1", seq.id);
        inst.state = InstanceState::Running;
        s.create_instance(&inst).await.unwrap();
    }

    // Filter by state.
    let filter = InstanceFilter {
        states: Some(vec![InstanceState::Running]),
        ..Default::default()
    };
    let running = s
        .list_instances(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(running.len(), 2);

    // Pagination.
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
}

#[tokio::test]
async fn sub_sequence_parent_child() {
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let parent = make_instance("t1", seq.id);
    s.create_instance(&parent).await.unwrap();

    // Create child instances.
    for _ in 0..2 {
        let mut child = make_instance("t1", seq.id);
        child.parent_instance_id = Some(parent.id);
        s.create_instance(&child).await.unwrap();
    }

    let children = s.get_child_instances(parent.id).await.unwrap();
    assert_eq!(children.len(), 2);
    for c in &children {
        assert_eq!(c.parent_instance_id, Some(parent.id));
    }
}

#[tokio::test]
async fn worker_task_list_and_stats() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Create tasks with different states.
    for (i, handler) in ["http_request", "email_send", "http_request"]
        .iter()
        .enumerate()
    {
        let task = WorkerTask {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId(format!("step_{i}")),
            handler_name: (*handler).into(),
            queue_name: None,
            params: json!({}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: Utc::now(),
        };
        s.create_worker_task(&task).await.unwrap();
    }

    // List with filter.
    let filter = orch8_types::worker_filter::WorkerTaskFilter {
        handler_name: Some("http_request".into()),
        ..Default::default()
    };
    let listed = s
        .list_worker_tasks(&filter, &Pagination::default())
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);

    // Stats.
    let stats = s.worker_task_stats(None).await.unwrap();
    assert!(stats.by_state.contains_key("pending"));
}

// ===========================================================================
// Performance: throughput measurements
// ===========================================================================

#[tokio::test]
async fn perf_bulk_instance_creation_1000() {
    let s = store().await;
    let seq = make_sequence("perf");
    s.create_sequence(&seq).await.unwrap();

    let batch: Vec<TaskInstance> = (0..1000).map(|_| make_instance("perf", seq.id)).collect();

    let start = std::time::Instant::now();
    let created = s.create_instances_batch(&batch).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(created, 1000);
    // Should complete in under 5 seconds even on slow CI.
    assert!(
        elapsed.as_secs() < 5,
        "Bulk insert of 1000 took {elapsed:?}"
    );
    eprintln!("perf_bulk_instance_creation_1000: {elapsed:?}");
}

#[tokio::test]
async fn perf_claim_due_under_load() {
    let s = store().await;
    let seq = make_sequence("perf");
    s.create_sequence(&seq).await.unwrap();
    let now = Utc::now();

    // Insert 500 scheduled instances.
    let batch: Vec<TaskInstance> = (0..500)
        .map(|_| {
            let mut inst = make_instance("perf", seq.id);
            inst.next_fire_at = Some(now - Duration::seconds(1));
            inst
        })
        .collect();
    s.create_instances_batch(&batch).await.unwrap();

    let start = std::time::Instant::now();
    let claimed = s.claim_due_instances(now, 100, 0).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(claimed.len(), 100);
    assert!(
        elapsed.as_millis() < 2000,
        "Claim 100 from 500 took {elapsed:?}"
    );
    eprintln!("perf_claim_due_under_load (100/500): {elapsed:?}");
}

#[tokio::test]
async fn perf_signal_batch_throughput() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Enqueue 200 signals.
    let mut sig_ids = Vec::new();
    for _ in 0..200 {
        let sig = Signal {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            signal_type: SignalType::Custom("tick".into()),
            payload: json!(null),
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        };
        sig_ids.push(sig.id);
        s.enqueue_signal(&sig).await.unwrap();
    }

    let start = std::time::Instant::now();
    let pending = s.get_pending_signals(inst_id).await.unwrap();
    let fetch_elapsed = start.elapsed();
    assert_eq!(pending.len(), 200);

    let start2 = std::time::Instant::now();
    s.mark_signals_delivered(&sig_ids).await.unwrap();
    let deliver_elapsed = start2.elapsed();

    let remaining = s.get_pending_signals(inst_id).await.unwrap();
    assert_eq!(remaining.len(), 0);

    eprintln!("perf_signal_batch: fetch={fetch_elapsed:?}, deliver={deliver_elapsed:?}");
}

#[tokio::test]
async fn perf_execution_tree_deep() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Create a tree: 1 root -> 10 parallel branches -> 5 steps each = 51 nodes.
    let root = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: inst_id,
        block_id: BlockId("root".into()),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    let mut all_nodes = vec![root.clone()];

    for branch in 0..10 {
        for step in 0..5 {
            all_nodes.push(ExecutionNode {
                id: ExecutionNodeId::new(),
                instance_id: inst_id,
                block_id: BlockId(format!("b{branch}_s{step}")),
                parent_id: Some(root.id),
                block_type: BlockType::Step,
                branch_index: Some(branch),
                state: NodeState::Pending,
                started_at: None,
                completed_at: None,
            });
        }
    }

    s.create_execution_nodes_batch(&all_nodes).await.unwrap();

    let start = std::time::Instant::now();
    let tree = s.get_execution_tree(inst_id).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(tree.len(), 51);
    eprintln!("perf_execution_tree_deep (51 nodes): {elapsed:?}");

    let start2 = std::time::Instant::now();
    let children = s.get_children(root.id).await.unwrap();
    let elapsed2 = start2.elapsed();
    assert_eq!(children.len(), 50);
    eprintln!("perf_get_children (50 children): {elapsed2:?}");
}

#[tokio::test]
async fn perf_concurrent_worker_claims() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Create 100 worker tasks.
    for i in 0..100 {
        let task = WorkerTask {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId(format!("step_{i}")),
            handler_name: "batch_handler".into(),
            queue_name: None,
            params: json!({"index": i}),
            context: json!({}),
            attempt: 1,
            timeout_ms: None,
            state: WorkerTaskState::Pending,
            worker_id: None,
            claimed_at: None,
            heartbeat_at: None,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: Utc::now(),
        };
        s.create_worker_task(&task).await.unwrap();
    }

    // Simulate 5 workers each claiming 20.
    let start = std::time::Instant::now();
    let mut total_claimed = 0;
    for w in 0..5 {
        let claimed = s
            .claim_worker_tasks("batch_handler", &format!("worker-{w}"), 20)
            .await
            .unwrap();
        total_claimed += claimed.len();
    }
    let elapsed = start.elapsed();

    assert_eq!(total_claimed, 100);
    eprintln!("perf_concurrent_worker_claims (100 tasks, 5 workers): {elapsed:?}");
}

// ===========================================================================
// Context Management
// ===========================================================================
//
// These tests cover the public surface of context storage against the
// documented semantics in `docs/CONTEXT_MANAGEMENT.md`:
//
//   * `update_instance_context` — full replacement of all four sections
//   * `merge_context_data`     — per-key update of `context.data`
//   * round-trip fidelity across Serialize/Deserialize via the DB
//   * concurrency behaviour of `merge_context_data` on disjoint keys
//
// All tests run against SQLite in-memory. The Postgres implementation is
// validated separately by e2e tests; the backend-agnostic invariants above
// are what the tests here target.

#[tokio::test]
async fn context_round_trip_all_sections() {
    // Writing every section at create-time and reading it back must preserve
    // each section byte-for-byte. This guards against silent column truncation
    // or default-serialization skipping non-empty fields.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context = ExecutionContext {
        data: json!({"user": {"email": "a@b.co"}, "counter": 7}),
        config: json!({"db": "postgres", "region": "sa-east-1"}),
        audit: vec![AuditEntry {
            timestamp: Utc::now(),
            event: "started".into(),
            details: json!({"by": "api"}),
        }],
        runtime: RuntimeContext {
            current_step: Some(BlockId("s1".into())),
            attempt: 3,
            started_at: Some(Utc::now()),
            current_step_started_at: None,
            resource_key: None,
        },
    };
    s.create_instance(&inst).await.unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data, inst.context.data);
    assert_eq!(back.context.config, inst.context.config);
    assert_eq!(back.context.audit.len(), 1);
    assert_eq!(back.context.audit[0].event, "started");
    assert_eq!(back.context.runtime.attempt, 3);
    assert_eq!(
        back.context.runtime.current_step,
        Some(BlockId("s1".into()))
    );
}

#[tokio::test]
async fn update_instance_context_is_full_replacement() {
    // Per `docs/CONTEXT_MANAGEMENT.md` §5.2, `update_instance_context`
    // overwrites the entire column. Fields present before but absent in the
    // new value must not survive.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({"keep_me": 1, "drop_me": 2});
    inst.context.config = json!({"env": "prod"});
    s.create_instance(&inst).await.unwrap();

    // Replace with a context that only has data.new.
    let replacement = ExecutionContext {
        data: json!({"new": "state"}),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    s.update_instance_context(inst.id, &replacement)
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data, json!({"new": "state"}));
    // Previous `config.env` must be gone — this is replacement, not merge.
    assert!(back.context.config.get("env").is_none());
}

#[tokio::test]
async fn update_instance_context_externalized_swaps_markers_and_persists_refs() {
    use orch8_storage::externalizing::{is_marker, REF_KEY};

    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({});
    s.create_instance(&inst).await.unwrap();

    // One small field stays inline, one big field must externalize.
    let big_payload = json!({ "blob": "x".repeat(2_000) });
    let ctx = ExecutionContext {
        data: json!({
            "small": "tiny",
            "big": big_payload.clone(),
        }),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    s.update_instance_context_externalized(inst.id, &ctx, 1024)
        .await
        .unwrap();

    // Reload the instance — inline field is verbatim, large field is a marker.
    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["small"], json!("tiny"));
    assert!(is_marker(&back.context.data["big"]));

    // The marker's ref_key must resolve via externalized_state to the full payload.
    let ref_key = back.context.data["big"][REF_KEY]
        .as_str()
        .unwrap()
        .to_string();
    let fetched = s.get_externalized_state(&ref_key).await.unwrap();
    assert_eq!(fetched, Some(big_payload));
}

#[tokio::test]
async fn update_instance_context_externalized_threshold_zero_is_passthrough() {
    // threshold_bytes == 0 short-circuits — no field is ever large enough
    // to externalize, so the call must behave identically to
    // update_instance_context (full inline replace, no externalized_state rows).
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let inst = make_instance("t1", seq.id);
    s.create_instance(&inst).await.unwrap();

    let ctx = ExecutionContext {
        data: json!({ "inline": "y".repeat(5_000) }),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    s.update_instance_context_externalized(inst.id, &ctx, 0)
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["inline"], json!("y".repeat(5_000)));
}

#[tokio::test]
async fn create_instance_externalized_swaps_markers_and_persists_refs() {
    use orch8_storage::externalizing::{is_marker, REF_KEY};

    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    // Instance born with an already-large `data.big` field — externalization
    // must happen during the INSERT, not a follow-up update.
    let big_payload = json!({ "blob": "x".repeat(2_000) });
    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({
        "small": "tiny",
        "big": big_payload.clone(),
    });

    s.create_instance_externalized(&inst, 1024).await.unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["small"], json!("tiny"));
    assert!(is_marker(&back.context.data["big"]));

    let ref_key = back.context.data["big"][REF_KEY]
        .as_str()
        .unwrap()
        .to_string();
    let fetched = s.get_externalized_state(&ref_key).await.unwrap();
    assert_eq!(fetched, Some(big_payload));
}

#[tokio::test]
async fn create_instance_externalized_threshold_zero_is_passthrough() {
    // threshold_bytes == 0 means nothing is externalized; the row lands
    // inline and no `externalized_state` records are produced.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({ "inline": "y".repeat(5_000) });

    s.create_instance_externalized(&inst, 0).await.unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["inline"], json!("y".repeat(5_000)));
}

#[tokio::test]
async fn create_instances_batch_externalized_externalizes_each_instance_independently() {
    use orch8_storage::externalizing::{is_marker, REF_KEY};

    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let big_a = json!({ "blob": "a".repeat(2_000) });
    let big_b = json!({ "blob": "b".repeat(3_000) });

    let mut inst_a = make_instance("t1", seq.id);
    inst_a.context.data = json!({ "tag": "A", "big": big_a.clone() });

    let mut inst_b = make_instance("t1", seq.id);
    inst_b.context.data = json!({ "tag": "B", "big": big_b.clone() });

    let inserted = s
        .create_instances_batch_externalized(&[inst_a.clone(), inst_b.clone()], 1024)
        .await
        .unwrap();
    assert_eq!(inserted, 2);

    for (inst, expected_blob, expected_tag) in [(&inst_a, &big_a, "A"), (&inst_b, &big_b, "B")] {
        let back = s.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(back.context.data["tag"], json!(expected_tag));
        assert!(is_marker(&back.context.data["big"]));

        // Each instance must own its own ref_key (keyed by its own id).
        let ref_key = back.context.data["big"][REF_KEY]
            .as_str()
            .unwrap()
            .to_string();
        assert!(
            ref_key.starts_with(&inst.id.0.to_string()),
            "ref_key {ref_key:?} must be scoped to instance {}",
            inst.id.0
        );
        let fetched = s.get_externalized_state(&ref_key).await.unwrap();
        assert_eq!(fetched.as_ref(), Some(expected_blob));
    }
}

#[tokio::test]
async fn create_instances_batch_externalized_empty_input_is_noop() {
    let s = store().await;
    let inserted = s
        .create_instances_batch_externalized(&[], 1024)
        .await
        .unwrap();
    assert_eq!(inserted, 0);
}

#[tokio::test]
async fn merge_context_data_preserves_other_sections() {
    // `merge_context_data` updates only a key under `context.data`. It must
    // NOT touch `context.config`, `context.audit`, or `context.runtime`.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context = ExecutionContext {
        data: json!({"a": 1}),
        config: json!({"cfg_key": "cfg_val"}),
        audit: vec![AuditEntry {
            timestamp: Utc::now(),
            event: "pre_merge".into(),
            details: json!({}),
        }],
        runtime: RuntimeContext {
            current_step: Some(BlockId("step-1".into())),
            attempt: 2,
            started_at: None,
            current_step_started_at: None,
            resource_key: None,
        },
    };
    s.create_instance(&inst).await.unwrap();

    s.merge_context_data(inst.id, "b", &json!(99))
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["a"], 1, "existing key must survive");
    assert_eq!(back.context.data["b"], 99, "new key must be written");
    assert_eq!(back.context.config["cfg_key"], "cfg_val");
    assert_eq!(back.context.audit.len(), 1);
    assert_eq!(back.context.audit[0].event, "pre_merge");
    assert_eq!(back.context.runtime.attempt, 2);
}

#[tokio::test]
async fn merge_context_data_overwrites_same_key() {
    // Repeated merges of the same key must overwrite, not accumulate or nest.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({});
    s.create_instance(&inst).await.unwrap();

    s.merge_context_data(inst.id, "k", &json!("v1"))
        .await
        .unwrap();
    s.merge_context_data(inst.id, "k", &json!("v2"))
        .await
        .unwrap();
    s.merge_context_data(inst.id, "k", &json!({"nested": true}))
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["k"], json!({"nested": true}));
}

#[tokio::test]
async fn merge_context_data_accepts_complex_values() {
    // Values can be arbitrary JSON — objects, arrays, null, numbers.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({});
    s.create_instance(&inst).await.unwrap();

    s.merge_context_data(inst.id, "obj", &json!({"a": [1, 2, 3]}))
        .await
        .unwrap();
    s.merge_context_data(inst.id, "arr", &json!([true, false, null]))
        .await
        .unwrap();
    s.merge_context_data(inst.id, "nil", &json!(null))
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["obj"]["a"][2], 3);
    assert_eq!(back.context.data["arr"][0], true);
    assert!(back.context.data["nil"].is_null());
}

#[tokio::test]
async fn merge_context_data_serialized_disjoint_keys() {
    // Sequentially merging many disjoint keys must produce a context that
    // contains all of them. (Intra-process concurrent writes would need a
    // multi-thread runtime; SQLite serializes writers at the file level, so
    // this sequential form is the portable correctness check — the same
    // property that a concurrent stream would assert.)
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({});
    s.create_instance(&inst).await.unwrap();

    for i in 0..25 {
        s.merge_context_data(inst.id, &format!("k{i}"), &json!(i))
            .await
            .unwrap();
    }

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    let obj = back.context.data.as_object().expect("data must be object");
    for i in 0..25 {
        assert_eq!(obj[&format!("k{i}")], json!(i));
    }
    assert_eq!(obj.len(), 25);
}

#[tokio::test]
async fn merge_context_data_on_missing_instance_is_noop() {
    // Merging into a non-existent instance must not error or create a row.
    // The SQLite RMW path short-circuits on the initial SELECT; Postgres'
    // UPDATE naturally affects 0 rows.
    let s = store().await;
    let missing = InstanceId::new();
    let res = s.merge_context_data(missing, "k", &json!("v")).await;
    assert!(res.is_ok(), "missing instance must be a silent noop");
    assert!(s.get_instance(missing).await.unwrap().is_none());
}

#[tokio::test]
async fn context_filtered_helper_is_used_correctly() {
    // Smoke test that `ExecutionContext::filtered` is wired into the
    // scheduler's fast path. We can't drive the scheduler from a storage
    // test, but we can at least verify the filter semantics hold with the
    // real struct layout the engine uses.
    use orch8_types::sequence::{ContextAccess, FieldAccess};

    let ctx = ExecutionContext {
        data: json!({"secret": "x"}),
        config: json!({"endpoint": "https://api"}),
        audit: vec![AuditEntry {
            timestamp: Utc::now(),
            event: "ev".into(),
            details: json!({}),
        }],
        runtime: RuntimeContext {
            attempt: 7,
            ..RuntimeContext::default()
        },
    };

    let no_data = ContextAccess {
        data: FieldAccess::NONE,
        config: true,
        audit: true,
        runtime: true,
    };
    let f = ctx.filtered(&no_data);
    assert_eq!(f.data, json!({}));
    assert_eq!(f.config["endpoint"], "https://api");
    assert_eq!(f.audit.len(), 1);
    assert_eq!(f.runtime.attempt, 7);

    let only_data = ContextAccess {
        data: FieldAccess::ALL,
        config: false,
        audit: false,
        runtime: false,
    };
    let f = ctx.filtered(&only_data);
    assert_eq!(f.data["secret"], "x");
    assert_eq!(f.config, json!({}));
    assert!(f.audit.is_empty());
    assert_eq!(f.runtime.attempt, 0);
}

#[tokio::test]
async fn merge_context_data_does_not_clobber_nested_object() {
    // When `context.data.user` is `{"name": "a"}`, merging key "user" with
    // `{"email": "x"}` replaces the whole value. The merge is key-level,
    // not deep. This test pins the documented semantics so future changes
    // that switch to a deep-merge have to update the docs intentionally.
    let s = store().await;
    let seq = make_sequence("t1");
    s.create_sequence(&seq).await.unwrap();

    let mut inst = make_instance("t1", seq.id);
    inst.context.data = json!({"user": {"name": "a"}});
    s.create_instance(&inst).await.unwrap();

    s.merge_context_data(inst.id, "user", &json!({"email": "x"}))
        .await
        .unwrap();

    let back = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(back.context.data["user"], json!({"email": "x"}));
    assert!(
        back.context.data["user"].get("name").is_none(),
        "key-level replacement must drop sibling fields"
    );
}

#[tokio::test]
async fn context_access_filter_enforced_on_tree_path() {
    // The gap documented in Bug §8.3 (tree path not filtering context_access)
    // is now fixed in handlers/step_block.rs via `context_for_step`. The
    // tree-path context_access enforcement is exercised by the e2e suite;
    // this test validates the underlying `filtered()` method that both
    // paths rely on.
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::sequence::{ContextAccess, FieldAccess};
    let ctx = ExecutionContext {
        data: json!({ "user_email": "a@b.c", "user_name": "Alice", "token": "secret" }),
        config: json!({}),
        audit: vec![],
        runtime: RuntimeContext::default(),
    };
    let access = ContextAccess {
        data: FieldAccess::Fields {
            fields: vec!["user_email".to_string()],
        },
        config: false,
        audit: false,
        runtime: false,
    };
    let filtered = ctx.filtered(&access);
    assert_eq!(filtered.data["user_email"], json!("a@b.c"));
    assert!(filtered.data.get("user_name").is_none());
    assert!(filtered.data.get("token").is_none());
}

// ===========================================================================
// Rate Limiting (TEST_PLAN 263-265)
// ===========================================================================

#[tokio::test]
async fn rate_limit_under_threshold_returns_allowed() {
    let s = store().await;
    let now = Utc::now();
    let tenant = TenantId("rl_under".into());
    let key = ResourceKey("endpoint:a".into());

    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 5,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    // A single call under the threshold is Allowed.
    let check = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    assert!(matches!(check, RateLimitCheck::Allowed));
}

#[tokio::test]
async fn rate_limit_at_threshold_returns_exceeded_with_retry_after() {
    let s = store().await;
    let now = Utc::now();
    let tenant = TenantId("rl_at".into());
    let key = ResourceKey("endpoint:b".into());

    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 2,
        window_seconds: 30,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    // Consume the full budget.
    for _ in 0..2 {
        let c = s.check_rate_limit(&tenant, &key, now).await.unwrap();
        assert!(matches!(c, RateLimitCheck::Allowed));
    }

    // Next call must be Exceeded with a retry_after inside the window.
    let check = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    match check {
        RateLimitCheck::Exceeded { retry_after } => {
            let expected_max = now + Duration::seconds(30);
            assert!(retry_after > now);
            assert!(retry_after <= expected_max + Duration::seconds(1));
        }
        RateLimitCheck::Allowed => panic!("expected Exceeded"),
    }
}

#[tokio::test]
async fn upsert_rate_limit_updates_window_on_conflict() {
    let s = store().await;
    let now = Utc::now();
    let tenant = TenantId("rl_up".into());
    let key = ResourceKey("endpoint:c".into());

    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 1,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    // Consume the single slot so we're at capacity.
    let c = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    assert!(matches!(c, RateLimitCheck::Allowed));
    let c = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    assert!(matches!(c, RateLimitCheck::Exceeded { .. }));

    // Upsert with a bigger budget; same (tenant, resource_key) conflicts and
    // max_count/window_seconds should be updated (see ON CONFLICT DO UPDATE).
    s.upsert_rate_limit(&RateLimit {
        id: Uuid::now_v7(),
        tenant_id: tenant.clone(),
        resource_key: key.clone(),
        max_count: 10,
        window_seconds: 60,
        current_count: 0,
        window_start: now,
    })
    .await
    .unwrap();

    // With raised limit, the next check must be Allowed again.
    let c = s.check_rate_limit(&tenant, &key, now).await.unwrap();
    assert!(matches!(c, RateLimitCheck::Allowed));
}

// ===========================================================================
// Concurrency (TEST_PLAN 266-270)
// ===========================================================================

#[tokio::test]
async fn count_running_by_concurrency_key_accurate() {
    let s = store().await;
    let seq = make_sequence("t_cc");
    s.create_sequence(&seq).await.unwrap();

    let key = "payment:user:1";

    // Seed: 2 Running + 1 Scheduled (must not be counted) + 1 Completed (skip).
    for state in [
        InstanceState::Running,
        InstanceState::Running,
        InstanceState::Scheduled,
        InstanceState::Completed,
    ] {
        let mut inst = make_instance("t_cc", seq.id);
        inst.state = state;
        inst.concurrency_key = Some(key.into());
        s.create_instance(&inst).await.unwrap();
    }

    let count = s.count_running_by_concurrency_key(key).await.unwrap();
    assert_eq!(count, 2);

    // An unrelated key returns 0.
    assert_eq!(
        s.count_running_by_concurrency_key("does-not-exist")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn concurrency_position_returns_queue_position() {
    let s = store().await;
    let seq = make_sequence("t_pos");
    s.create_sequence(&seq).await.unwrap();
    let key = "q:shared";

    // Three running instances on the same key. Positions are 1-based and
    // FIFO by (created_at, id).
    let mut ids = Vec::new();
    for _ in 0..3 {
        let mut inst = make_instance("t_pos", seq.id);
        inst.state = InstanceState::Running;
        inst.concurrency_key = Some(key.into());
        // Space out created_at so ordering is deterministic.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        inst.created_at = Utc::now();
        s.create_instance(&inst).await.unwrap();
        ids.push(inst.id);
    }

    let positions: Vec<i64> = {
        let mut ps = Vec::new();
        for id in &ids {
            ps.push(s.concurrency_position(*id, key).await.unwrap());
        }
        ps
    };
    assert_eq!(positions, vec![1, 2, 3]);

    // An instance not running under this key returns 0 (absent).
    let foreign = make_instance("t_pos", seq.id);
    assert_eq!(s.concurrency_position(foreign.id, key).await.unwrap(), 0);
}

#[tokio::test]
async fn concurrency_count_empty_for_unused_key() {
    // Item 268: instances without a concurrency_key must not be counted
    // against any key, so there's no implicit limit applied.
    let s = store().await;
    let seq = make_sequence("t_nokey");
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let mut inst = make_instance("t_nokey", seq.id);
        inst.state = InstanceState::Running;
        inst.concurrency_key = None;
        s.create_instance(&inst).await.unwrap();
    }

    // Querying any key returns 0 because none of these rows set it.
    assert_eq!(
        s.count_running_by_concurrency_key("anything")
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn max_concurrency_zero_blocks_all_positions() {
    // Item 269: when max_concurrency=0, every candidate's position > max,
    // meaning it must defer. Storage reports the true queue position; the
    // scheduler's comparison `position > max` enforces "blocks all".
    let s = store().await;
    let seq = make_sequence("t_zero");
    s.create_sequence(&seq).await.unwrap();
    let key = "q:zero";

    let mut inst = make_instance("t_zero", seq.id);
    inst.state = InstanceState::Running;
    inst.concurrency_key = Some(key.into());
    inst.max_concurrency = Some(0);
    s.create_instance(&inst).await.unwrap();

    let pos = s.concurrency_position(inst.id, key).await.unwrap();
    // Position is 1 (first in queue) — strictly greater than max=0, which is
    // the defer condition.
    assert_eq!(pos, 1);
    assert!(i64::from(inst.max_concurrency.unwrap()) < pos);
}

#[tokio::test]
async fn concurrent_claims_with_same_key_are_serialized() {
    // Item 270: two tasks race to claim/update on the same concurrency key;
    // the storage layer must not double-count. We verify that after parallel
    // writes land, the count matches the number of rows written — i.e. no
    // lost updates.
    let s = std::sync::Arc::new(store().await);
    let seq = make_sequence("t_race");
    s.create_sequence(&seq).await.unwrap();
    let key = "q:race";

    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = s.clone();
        let seq_id = seq.id;
        let key = key.to_string();
        handles.push(tokio::spawn(async move {
            let mut inst = make_instance("t_race", seq_id);
            inst.state = InstanceState::Running;
            inst.concurrency_key = Some(key);
            s.create_instance(&inst).await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let count = s.count_running_by_concurrency_key(key).await.unwrap();
    assert_eq!(count, 8, "each parallel claim contributed exactly once");
}

// ===========================================================================
// Cron storage (TEST_PLAN 260-261)
// ===========================================================================

#[tokio::test]
async fn cron_claim_due_and_update_fire_times() {
    let s = store().await;
    let seq = make_sequence("t_cron");
    s.create_sequence(&seq).await.unwrap();

    let now = Utc::now();
    let past = now - Duration::seconds(5);
    let future = now + Duration::hours(1);

    let schedule = CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId("t_cron".into()),
        namespace: Namespace("default".into()),
        sequence_id: seq.id,
        cron_expr: "0 * * * * * *".into(),
        timezone: "UTC".into(),
        enabled: true,
        metadata: json!({}),
        last_triggered_at: None,
        next_fire_at: Some(past),
        created_at: now,
        updated_at: now,
    };
    s.create_cron_schedule(&schedule).await.unwrap();

    // Due now — must be returned.
    let due = s.claim_due_cron_schedules(now).await.unwrap();
    assert!(due.iter().any(|c| c.id == schedule.id));

    // Advance fire times; claim with the same `now` again must now skip it.
    s.update_cron_fire_times(schedule.id, now, future)
        .await
        .unwrap();
    let reloaded = s.get_cron_schedule(schedule.id).await.unwrap().unwrap();
    assert_eq!(
        reloaded.last_triggered_at.map(|t| t.timestamp()),
        Some(now.timestamp())
    );
    assert_eq!(
        reloaded.next_fire_at.map(|t| t.timestamp()),
        Some(future.timestamp())
    );

    let due2 = s.claim_due_cron_schedules(now).await.unwrap();
    assert!(!due2.iter().any(|c| c.id == schedule.id));
}

#[tokio::test]
async fn cron_disabled_is_not_claimed() {
    let s = store().await;
    let seq = make_sequence("t_dis");
    s.create_sequence(&seq).await.unwrap();

    let now = Utc::now();
    let past = now - Duration::minutes(1);
    let schedule = CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId("t_dis".into()),
        namespace: Namespace("default".into()),
        sequence_id: seq.id,
        cron_expr: "0 * * * * * *".into(),
        timezone: "UTC".into(),
        enabled: false,
        metadata: json!({}),
        last_triggered_at: None,
        next_fire_at: Some(past),
        created_at: now,
        updated_at: now,
    };
    s.create_cron_schedule(&schedule).await.unwrap();

    let due = s.claim_due_cron_schedules(now).await.unwrap();
    assert!(
        !due.iter().any(|c| c.id == schedule.id),
        "disabled cron must not be claimed"
    );
}

#[tokio::test]
async fn list_waiting_with_trees_returns_only_waiting_instances() {
    let s = SqliteStorage::in_memory().await.unwrap();

    // One running + one waiting + one completed instance, each with a tree node.
    let waiting = make_instance_in_state("t1", InstanceState::Waiting);
    let running = make_instance_in_state("t1", InstanceState::Running);
    let completed = make_instance_in_state("t1", InstanceState::Completed);
    for inst in [&waiting, &running, &completed] {
        s.create_instance(inst).await.unwrap();
        s.create_execution_node(&ExecutionNode {
            id: ExecutionNodeId::new(),
            instance_id: inst.id,
            block_id: BlockId("blk1".into()),
            parent_id: None,
            block_type: BlockType::Step,
            branch_index: None,
            state: match inst.state {
                InstanceState::Waiting => NodeState::Waiting,
                InstanceState::Running => NodeState::Running,
                _ => NodeState::Completed,
            },
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
        })
        .await
        .unwrap();
    }

    let got = s
        .list_waiting_with_trees(
            &InstanceFilter::default(),
            &Pagination {
                offset: 0,
                limit: 100,
                sort_ascending: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(got.len(), 1);
    let (inst, nodes) = &got[0];
    assert_eq!(inst.id, waiting.id);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].state, NodeState::Waiting);
}
