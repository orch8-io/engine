//! End-to-end coverage for critical engine features previously untested
//! at the integration level.
//!
//! Covers:
//! - Circuit breaker tripping, fallback, and cooldown recovery

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use orch8_engine::circuit_breaker::CircuitBreakerRegistry;
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::ids::{BlockId, ExecutionNodeId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};
use uuid::Uuid;

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

fn mk_step(id: &str, handler: &str) -> StepDef {
    StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
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
    }
}

fn mk_instance(seq_id: SequenceId) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id: seq_id,
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        state: InstanceState::Running,
        next_fire_at: None,
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

async fn setup_single_step(
    step: StepDef,
) -> (
    Arc<dyn StorageBackend>,
    TaskInstance,
    ExecutionNode,
    StepDef,
) {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let step_id = step.id.clone();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "gap-repro".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(step.clone()))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let instance = mk_instance(seq.id);
    storage.create_instance(&instance).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance.id,
        block_id: step_id,
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&node).await.unwrap();

    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);
    (storage_dyn, instance, node, step)
}

// --------------------------------------------------------------------------
// Circuit breaker E2E
// --------------------------------------------------------------------------

#[tokio::test]
async fn circuit_breaker_trips_after_threshold_failures() {
    let step = mk_step("s1", "flaky");
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    let cb = CircuitBreakerRegistry::new(2, 60);
    let mut registry = HandlerRegistry::new().with_circuit_breakers(Arc::new(cb));
    registry.set_mock_error("flaky", "transient".into(), true);

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();

    // First failure — breaker still Closed.
    let r1 = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(
        r1.is_ok(),
        "first retryable failure should defer, not error"
    );

    // Second failure — breaker trips to Open.
    let r2 = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(r2.is_ok(), "second retryable failure should defer");

    // Third call — breaker is Open, instance deferred immediately.
    let r3 = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(
        r3.is_ok(),
        "open breaker should defer without calling handler"
    );

    // Verify breaker state is Open.
    let cb_ref = registry.circuit_breakers().unwrap();
    let state = cb_ref
        .get(&instance.tenant_id, "flaky")
        .expect("breaker should be tracked");
    assert!(
        matches!(
            state.state,
            orch8_types::circuit_breaker::BreakerState::Open
        ),
        "breaker must be Open after threshold"
    );
}

#[tokio::test]
async fn circuit_breaker_fallback_handler_when_primary_open() {
    let mut step = mk_step("s1", "flaky");
    step.fallback_handler = Some("safe".into());

    let (storage, instance, node, step_def) = setup_single_step(step).await;

    let cb = CircuitBreakerRegistry::new(1, 60);
    let mut registry = HandlerRegistry::new().with_circuit_breakers(Arc::new(cb));
    registry.set_mock_error("flaky", "boom".into(), true);
    registry.set_mock("safe", json!({"recovered": true}));

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();

    // First call fails, breaker trips to Open.
    let _ = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;

    // Second call — primary is Open, should dispatch to fallback handler.
    let r2 = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(r2.is_ok(), "fallback dispatch should succeed");

    // Verify fallback output was saved (look for the most recent output).
    let outs = storage.get_all_outputs(instance.id).await.unwrap();
    let fallback_out = outs.iter().rfind(|o| o.block_id.0 == "s1");
    assert!(fallback_out.is_some(), "fallback output must be saved");
    assert_eq!(
        fallback_out.unwrap().output,
        json!({"recovered": true}),
        "fallback handler response must be persisted"
    );
}

#[tokio::test]
async fn circuit_breaker_records_success_and_resets_failures() {
    let step = mk_step("s1", "flaky");
    let (storage, instance, node, step_def) = setup_single_step(step).await;

    // Short cooldown so the test doesn't hang.
    let cb = CircuitBreakerRegistry::new(2, 1);
    let mut registry = HandlerRegistry::new().with_circuit_breakers(Arc::new(cb));

    // A counter that fails twice then succeeds.
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    registry.register("flaky", move |_ctx| {
        let c = Arc::clone(&counter);
        async move {
            let n = c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 2 {
                Err(orch8_types::error::StepError::Retryable {
                    message: "transient".into(),
                    details: None,
                })
            } else {
                Ok(json!({"ok": true}))
            }
        }
    });

    let outputs = orch8_engine::handlers::param_resolve::OutputsSnapshot::new();

    // Two failures.
    let _ = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    let _ = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;

    // Breaker should be Open after two failures.
    let cb_ref = registry.circuit_breakers().unwrap();
    let state = cb_ref
        .get(&instance.tenant_id, "flaky")
        .expect("breaker should exist");
    assert!(
        matches!(
            state.state,
            orch8_types::circuit_breaker::BreakerState::Open
        ),
        "breaker must be Open after 2 failures"
    );

    // Wait for cooldown to elapse (1s cooldown + 100ms margin).
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Third call succeeds (HalfOpen → Closed on success).
    let r3 = orch8_engine::handlers::step_block::execute_step_node(
        &storage, &registry, &instance, &node, &step_def, &outputs,
    )
    .await;
    assert!(r3.is_ok(), "third call after cooldown should succeed");

    let state_after = cb_ref
        .get(&instance.tenant_id, "flaky")
        .expect("breaker should still exist");
    assert_eq!(
        state_after.failure_count, 0,
        "success must reset failure count"
    );
    assert!(
        matches!(
            state_after.state,
            orch8_types::circuit_breaker::BreakerState::Closed
        ),
        "success must close breaker"
    );
}

// --------------------------------------------------------------------------
// Atomic worker completion + CAS guard
// --------------------------------------------------------------------------

#[tokio::test]
async fn save_output_complete_node_and_transition_is_atomic() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "atomic-test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(mk_step("s1", "noop")))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let instance = mk_instance(seq.id);
    storage.create_instance(&instance).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance.id,
        block_id: BlockId("s1".into()),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Waiting,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&node).await.unwrap();

    let output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId("s1".into()),
        output: json!({"ok": true}),
        output_ref: None,
        output_size: 13,
        attempt: 1,
        created_at: Utc::now(),
    };

    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);
    storage_dyn
        .save_output_complete_node_and_transition(
            &output,
            node.id,
            instance.id,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await
        .unwrap();

    let tree = storage_dyn.get_execution_tree(instance.id).await.unwrap();
    assert_eq!(
        tree[0].state,
        NodeState::Completed,
        "node must be Completed"
    );

    let inst = storage_dyn
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inst.state,
        InstanceState::Scheduled,
        "instance must be Scheduled"
    );

    let outs = storage_dyn.get_all_outputs(instance.id).await.unwrap();
    assert_eq!(outs.len(), 1, "output must be saved");
}

#[tokio::test]
async fn save_output_complete_node_and_transition_rejects_terminal_instance() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let seq = SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "cas-test".into(),
        version: 1,
        deprecated: false,
        blocks: vec![BlockDefinition::Step(Box::new(mk_step("s1", "noop")))],
        interceptors: None,
        created_at: Utc::now(),
    };
    storage.create_sequence(&seq).await.unwrap();

    let mut instance = mk_instance(seq.id);
    instance.state = InstanceState::Completed;
    storage.create_instance(&instance).await.unwrap();

    let node = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance.id,
        block_id: BlockId("s1".into()),
        parent_id: None,
        block_type: BlockType::Step,
        branch_index: None,
        state: NodeState::Waiting,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&node).await.unwrap();

    let output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId("s1".into()),
        output: json!({"ok": true}),
        output_ref: None,
        output_size: 13,
        attempt: 1,
        created_at: Utc::now(),
    };

    let storage_dyn: Arc<dyn StorageBackend> = Arc::new(storage);
    let result = storage_dyn
        .save_output_complete_node_and_transition(
            &output,
            node.id,
            instance.id,
            InstanceState::Scheduled,
            Some(Utc::now()),
        )
        .await;

    assert!(
        matches!(
            result,
            Err(orch8_types::error::StorageError::TerminalTarget { .. })
        ),
        "must reject terminal instance with TerminalTarget"
    );

    // Transaction rolled back — node and output must be untouched.
    let tree = storage_dyn.get_execution_tree(instance.id).await.unwrap();
    assert_eq!(
        tree[0].state,
        NodeState::Waiting,
        "node must still be Waiting"
    );

    let outs = storage_dyn.get_all_outputs(instance.id).await.unwrap();
    assert!(outs.is_empty(), "no output must be saved");
}

// --------------------------------------------------------------------------
// SQLite get_batch chunking
// --------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_get_batch_chunking_does_not_drop_keys() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let instance = InstanceId::new();

    // Seed 450 outputs to exercise the 400-key chunk boundary.
    let mut keys = Vec::with_capacity(450);
    for i in 0..450 {
        let block_id = BlockId(format!("block_{i:03}"));
        let out = BlockOutput {
            id: Uuid::now_v7(),
            instance_id: instance,
            block_id: block_id.clone(),
            output: json!({"idx": i}),
            output_ref: None,
            output_size: 10,
            attempt: 1,
            created_at: Utc::now(),
        };
        storage.save_block_output(&out).await.unwrap();
        keys.push((instance, block_id));
    }

    let batch = storage.get_block_outputs_batch(&keys).await.unwrap();
    assert_eq!(batch.len(), 450, "batch must return all 450 outputs");

    // Spot-check a few keys.
    assert_eq!(batch[&keys[0]].output, json!({"idx": 0}));
    assert_eq!(batch[&keys[399]].output, json!({"idx": 399}));
    assert_eq!(batch[&keys[449]].output, json!({"idx": 449}));
}

// --------------------------------------------------------------------------
// Sequential composite activation (regression for ordered-body fan-out)
// --------------------------------------------------------------------------

#[tokio::test]
async fn activate_first_pending_child_only_flips_first() {
    use orch8_engine::evaluator;

    let storage = SqliteStorage::in_memory().await.unwrap();
    let instance = InstanceId::new();

    // Parent node
    let parent = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("parent".into()),
        parent_id: None,
        block_type: BlockType::Router,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&parent).await.unwrap();

    // Two pending children
    let child_a = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("a".into()),
        parent_id: Some(parent.id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    let child_b = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("b".into()),
        parent_id: Some(parent.id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Pending,
        started_at: None,
        completed_at: None,
    };
    storage.create_execution_node(&child_a).await.unwrap();
    storage.create_execution_node(&child_b).await.unwrap();

    let tree = storage.get_execution_tree(instance).await.unwrap();
    let children: Vec<&ExecutionNode> = tree
        .iter()
        .filter(|n| n.parent_id == Some(parent.id))
        .collect();

    evaluator::activate_first_pending_child(&storage, &children)
        .await
        .unwrap();

    let after = storage.get_execution_tree(instance).await.unwrap();
    let state_a = after.iter().find(|n| n.block_id.0 == "a").unwrap().state;
    let state_b = after.iter().find(|n| n.block_id.0 == "b").unwrap().state;

    assert_eq!(
        state_a,
        NodeState::Running,
        "first pending child must activate"
    );
    assert_eq!(
        state_b,
        NodeState::Pending,
        "second pending child must stay Pending"
    );
}

// --------------------------------------------------------------------------
// Race recursive cancellation
// --------------------------------------------------------------------------

#[tokio::test]
async fn cancel_subtree_recursively_cancels_deep_descendants() {
    use orch8_engine::evaluator;

    let storage = SqliteStorage::in_memory().await.unwrap();
    let instance = InstanceId::new();

    // Root -> mid -> deep
    let root = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("root".into()),
        parent_id: None,
        block_type: BlockType::Parallel,
        branch_index: None,
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&root).await.unwrap();

    let mid = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("mid".into()),
        parent_id: Some(root.id),
        block_type: BlockType::Parallel,
        branch_index: Some(0),
        state: NodeState::Running,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&mid).await.unwrap();

    let deep = ExecutionNode {
        id: ExecutionNodeId::new(),
        instance_id: instance,
        block_id: BlockId("deep".into()),
        parent_id: Some(mid.id),
        block_type: BlockType::Step,
        branch_index: Some(0),
        state: NodeState::Waiting,
        started_at: Some(Utc::now()),
        completed_at: None,
    };
    storage.create_execution_node(&deep).await.unwrap();

    let tree = storage.get_execution_tree(instance).await.unwrap();
    evaluator::cancel_subtree(&storage, instance, &tree, root.id)
        .await
        .unwrap();

    let after = storage.get_execution_tree(instance).await.unwrap();
    let state_mid = after.iter().find(|n| n.block_id.0 == "mid").unwrap().state;
    let state_deep = after.iter().find(|n| n.block_id.0 == "deep").unwrap().state;

    assert_eq!(state_mid, NodeState::Cancelled, "mid must be cancelled");
    assert_eq!(state_deep, NodeState::Cancelled, "deep must be cancelled");
}
