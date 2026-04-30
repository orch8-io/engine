//! Integration tests for 0.3.1 features:
//! - `transform` built-in handler
//! - `assert` built-in handler
//! - `merge_state` built-in handler
//! - `state.*` template resolution
//! - Step output caching via `cache_key`

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use orch8_engine::evaluator::{self, EvalOutcome};
use orch8_engine::handlers::{builtin::register_builtins, HandlerRegistry};
use orch8_storage::{sqlite::SqliteStorage, StorageBackend};
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::execution::NodeState;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::sequence::{BlockDefinition, SequenceDefinition, StepDef};

// ================================================================
// HELPERS
// ================================================================

fn mk_step(id: &str, handler: &str, params: serde_json::Value) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params,
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
    }))
}

fn mk_step_cached(
    id: &str,
    handler: &str,
    params: serde_json::Value,
    cache_key: &str,
) -> BlockDefinition {
    BlockDefinition::Step(Box::new(StepDef {
        id: BlockId(id.into()),
        handler: handler.into(),
        params,
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
        cache_key: Some(cache_key.into()),
    }))
}

fn mk_sequence(blocks: Vec<BlockDefinition>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId("t".into()),
        namespace: Namespace("ns".into()),
        name: "test-flow".into(),
        version: 1,
        deprecated: false,
        blocks,
        interceptors: None,
        created_at: Utc::now(),
    }
}

fn mk_instance_with_ctx(seq_id: SequenceId, data: serde_json::Value) -> TaskInstance {
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
            data,
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

async fn setup(
    blocks: Vec<BlockDefinition>,
    ctx_data: serde_json::Value,
) -> (Arc<dyn StorageBackend>, SequenceDefinition, TaskInstance) {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let seq = mk_sequence(blocks);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, ctx_data);
    storage.create_instance(&inst).await.unwrap();
    (storage, seq, inst)
}

async fn drive(
    storage: &Arc<dyn StorageBackend>,
    handlers: &HandlerRegistry,
    instance_id: InstanceId,
    sequence: &SequenceDefinition,
) {
    for _ in 0..500 {
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        match inst.state {
            InstanceState::Completed
            | InstanceState::Failed
            | InstanceState::Cancelled
            | InstanceState::Waiting
            | InstanceState::Paused => return,
            InstanceState::Scheduled => {
                storage
                    .update_instance_state(instance_id, InstanceState::Running, None)
                    .await
                    .unwrap();
            }
            _ => {}
        }
        let inst = storage.get_instance(instance_id).await.unwrap().unwrap();
        let outcome = evaluator::evaluate(storage, handlers, &inst, sequence)
            .await
            .unwrap();
        if let EvalOutcome::Done {
            any_failed,
            any_cancelled,
        } = outcome
        {
            let new_state = if any_cancelled && !any_failed {
                InstanceState::Cancelled
            } else if any_failed {
                InstanceState::Failed
            } else {
                InstanceState::Completed
            };
            storage
                .update_instance_state(instance_id, new_state, None)
                .await
                .unwrap();
            return;
        }
    }
    panic!("drive loop did not terminate");
}

fn default_handlers() -> HandlerRegistry {
    let mut r = HandlerRegistry::new();
    register_builtins(&mut r);
    r
}

// ================================================================
// TRANSFORM HANDLER - E2E
// ================================================================

#[tokio::test]
async fn transform_end_to_end() {
    let (storage, seq, inst) = setup(
        vec![mk_step(
            "t1",
            "transform",
            json!({"result": "computed", "value": 42}),
        )],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("t1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["result"], "computed");
    assert_eq!(output.output["value"], 42);
}

#[tokio::test]
async fn transform_resolves_context_templates() {
    let (storage, seq, inst) = setup(
        vec![mk_step(
            "t1",
            "transform",
            json!({"greeting": "hello {{ data.name }}"}),
        )],
        json!({"name": "world"}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("t1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["greeting"], "hello world");
}

// ================================================================
// ASSERT HANDLER - E2E
// ================================================================

#[tokio::test]
async fn assert_passing_completes() {
    let (storage, seq, inst) = setup(
        vec![mk_step("a1", "assert", json!({"condition": "true"}))],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("a1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["passed"], true);
}

#[tokio::test]
async fn assert_failing_causes_instance_failure() {
    let (storage, seq, inst) = setup(
        vec![mk_step(
            "a1",
            "assert",
            json!({"condition": "false", "message": "should not be zero"}),
        )],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Failed);

    let nodes = storage.get_execution_tree(inst.id).await.unwrap();
    let node = nodes
        .iter()
        .find(|n| n.block_id == BlockId("a1".into()))
        .unwrap();
    assert_eq!(node.state, NodeState::Failed);
}

// ================================================================
// MERGE_STATE HANDLER - E2E
// ================================================================

#[tokio::test]
async fn merge_state_stores_and_readable_via_get_state() {
    let (storage, seq, inst) = setup(
        vec![
            mk_step(
                "ms1",
                "merge_state",
                json!({"values": {"color": "green", "score": 99}}),
            ),
            mk_step("gs1", "get_state", json!({"key": "color"})),
            mk_step("gs2", "get_state", json!({"key": "score"})),
        ],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let out1 = storage
        .get_block_output(inst.id, &BlockId("gs1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out1.output["value"], "green");

    let out2 = storage
        .get_block_output(inst.id, &BlockId("gs2".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out2.output["value"], 99);
}

// ================================================================
// STATE.* TEMPLATE RESOLUTION - E2E
// ================================================================

#[tokio::test]
async fn state_template_reads_kv_state() {
    let (storage, seq, inst) = setup(
        vec![
            mk_step(
                "s1",
                "set_state",
                json!({"key": "flavor", "value": "vanilla"}),
            ),
            mk_step("t1", "transform", json!({"picked": "{{ state.flavor }}"})),
        ],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("t1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["picked"], "vanilla");
}

#[tokio::test]
async fn state_template_with_merge_state() {
    let (storage, seq, inst) = setup(
        vec![
            mk_step("ms1", "merge_state", json!({"values": {"x": 10, "y": 20}})),
            mk_step(
                "t1",
                "transform",
                json!({"sum_label": "{{ state.x }}+{{ state.y }}"}),
            ),
        ],
        json!({}),
    )
    .await;
    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("t1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["sum_label"], "10+20");
}

// ================================================================
// STEP OUTPUT CACHING - E2E
// ================================================================

#[tokio::test]
async fn cache_key_caches_step_output() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let seq = mk_sequence(vec![mk_step_cached(
        "c1",
        "transform",
        json!({"data": "first_run"}),
        "my-cache-key",
    )]);
    storage.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_with_ctx(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();

    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("c1".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["data"], "first_run");

    let cached = storage
        .get_instance_kv(inst.id, "_cache:my-cache-key")
        .await
        .unwrap();
    assert!(cached.is_some(), "cache entry should exist in KV state");
    assert_eq!(cached.unwrap()["data"], "first_run");
}

#[tokio::test]
async fn cache_key_serves_from_cache_on_second_instance() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());

    let instance_id = InstanceId::new();
    let seq_id = SequenceId::new();

    storage
        .set_instance_kv(
            instance_id,
            "_cache:reuse-key",
            &json!({"cached": true, "val": 777}),
        )
        .await
        .unwrap();

    let seq = mk_sequence(vec![mk_step_cached(
        "c2",
        "transform",
        json!({"data": "should_not_appear"}),
        "reuse-key",
    )]);
    let seq = SequenceDefinition { id: seq_id, ..seq };
    storage.create_sequence(&seq).await.unwrap();

    let now = Utc::now();
    let inst = TaskInstance {
        id: instance_id,
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
    };
    storage.create_instance(&inst).await.unwrap();

    let handlers = default_handlers();
    drive(&storage, &handlers, inst.id, &seq).await;

    let final_inst = storage.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(final_inst.state, InstanceState::Completed);

    let output = storage
        .get_block_output(inst.id, &BlockId("c2".into()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.output["cached"], true);
    assert_eq!(output.output["val"], 777);
}
