//! Comprehensive integration tests for `StorageBackend` via SQLite in-memory.
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
use orch8_types::context::ExecutionContext;
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

fn make_sequence(tenant: &str) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId(tenant.into()),
        namespace: Namespace("default".into()),
        name: "seq".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(StepDef {
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
        })],
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
        instance_id: inst1,
        signal_type: SignalType::Cancel,
        payload: json!(null),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let sig2 = Signal {
        id: Uuid::new_v4(),
        instance_id: inst2,
        signal_type: SignalType::Resume,
        payload: json!(null),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    let sig3 = Signal {
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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

#[tokio::test]
async fn worker_task_queue_routing() {
    let s = store().await;
    let inst_id = InstanceId::new();

    let task = WorkerTask {
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
    let session_id = Uuid::new_v4();

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
            id: Uuid::new_v4(),
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
            id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
    let ref_key = format!("ext_{}", inst_id);
    let payload = json!({"large": "data", "items": [1,2,3,4,5]});

    s.save_externalized_state(inst_id, &ref_key, &payload)
        .await
        .unwrap();

    let fetched = s.get_externalized_state(&ref_key).await.unwrap().unwrap();
    assert_eq!(fetched, payload);

    s.delete_externalized_state(&ref_key).await.unwrap();
    assert!(s.get_externalized_state(&ref_key).await.unwrap().is_none());
}

// ===========================================================================
// Cluster Nodes
// ===========================================================================

#[tokio::test]
async fn cluster_node_lifecycle() {
    let s = store().await;
    let now = Utc::now();

    let node = ClusterNode {
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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
        id: Uuid::new_v4(),
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

#[tokio::test]
async fn completed_block_ids_batch() {
    let s = store().await;
    let inst1 = InstanceId::new();
    let inst2 = InstanceId::new();

    for (inst_id, block_name) in &[(inst1, "a"), (inst1, "b"), (inst2, "c")] {
        let out = BlockOutput {
            id: Uuid::new_v4(),
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
            id: Uuid::new_v4(),
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
    let stats = s.worker_task_stats().await.unwrap();
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
        "Bulk insert of 1000 took {:?}",
        elapsed
    );
    eprintln!("perf_bulk_instance_creation_1000: {:?}", elapsed);
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
        "Claim 100 from 500 took {:?}",
        elapsed
    );
    eprintln!("perf_claim_due_under_load (100/500): {:?}", elapsed);
}

#[tokio::test]
async fn perf_signal_batch_throughput() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Enqueue 200 signals.
    let mut sig_ids = Vec::new();
    for _ in 0..200 {
        let sig = Signal {
            id: Uuid::new_v4(),
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

    eprintln!(
        "perf_signal_batch: fetch={:?}, deliver={:?}",
        fetch_elapsed, deliver_elapsed
    );
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
    eprintln!("perf_execution_tree_deep (51 nodes): {:?}", elapsed);

    let start2 = std::time::Instant::now();
    let children = s.get_children(root.id).await.unwrap();
    let elapsed2 = start2.elapsed();
    assert_eq!(children.len(), 50);
    eprintln!("perf_get_children (50 children): {:?}", elapsed2);
}

#[tokio::test]
async fn perf_concurrent_worker_claims() {
    let s = store().await;
    let inst_id = InstanceId::new();

    // Create 100 worker tasks.
    for i in 0..100 {
        let task = WorkerTask {
            id: Uuid::new_v4(),
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
    eprintln!(
        "perf_concurrent_worker_claims (100 tasks, 5 workers): {:?}",
        elapsed
    );
}
