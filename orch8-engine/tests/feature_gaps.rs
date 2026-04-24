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
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

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
