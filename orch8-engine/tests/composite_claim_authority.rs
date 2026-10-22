//! In-flight composite decisions must not mutate a replacement execution.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use chrono::Utc;
use orch8_engine::{
    error::EngineError,
    evaluator::{EvalOutcome, evaluate_with_claim_token},
    handlers::HandlerRegistry,
};
use orch8_storage::testing::{
    faulty::FaultStorage,
    fixtures::{InstanceBuilder, SequenceBuilder},
};
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::{
    clock::SharedClock,
    execution::NodeState,
    ids::BlockId,
    instance::InstanceState,
    output::BlockOutput,
    sequence::{BlockDefinition, Route, RouterDef},
};
use serde_json::json;

#[tokio::test]
async fn stale_router_cannot_replace_the_current_claims_decision_or_complete_its_node() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let sequence = SequenceBuilder::new("router-handoff")
        .block(BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("route"),
            routes: vec![Route {
                condition: "true".into(),
                blocks: Vec::new(),
            }],
            default: Some(Vec::new()),
        })))
        .build();
    inner.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    inner.create_instance(&instance).await.unwrap();
    let first_at = Utc::now() + chrono::Duration::seconds(1);
    let first = inner
        .claim_due_instances(first_at, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let old_token = first.claimed_at.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    // Memoized route was read as absent. Pause while collecting condition
    // inputs, before this in-flight activation persists its selected branch.
    let pause = faults.on("get_all_outputs").times(1).pause();
    let registry = HandlerRegistry::new();
    let clock = SharedClock::default();
    let stop = AtomicBool::new(false);
    let mut evaluation = Box::pin(evaluate_with_claim_token(
        &storage, &registry, &first, &sequence, &clock, old_token, &stop,
    ));
    tokio::select! {
        () = pause.entered() => {},
        result = &mut evaluation => panic!("evaluation finished before handoff: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(10)) => panic!("router did not reach decision inputs"),
    }
    let second_at = first_at + chrono::Duration::seconds(1);
    assert!(
        inner
            .conditional_update_claimed_instance_state(
                first.id,
                old_token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(second_at)
            )
            .await
            .unwrap()
    );
    let replacement = inner
        .claim_due_instances(second_at, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let new_token = replacement.claimed_at.unwrap();
    assert_ne!(old_token, new_token);
    let marker = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: first.id,
        block_id: BlockId::new("route"),
        output: json!({"_selected_branch": 1}),
        output_ref: None,
        output_size: 24,
        attempt: 0,
        created_at: Utc::now(),
    };
    assert!(
        inner
            .save_block_output_claimed(&marker, new_token)
            .await
            .unwrap()
    );
    let before = inner.get_execution_tree(first.id).await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, NodeState::Running);
    // The heartbeat already noticed the handoff. An operation in flight must
    // still be fenced, rather than relying only on the next-iteration check.
    stop.store(true, Ordering::Release);
    pause.release();
    let result = evaluation.await;
    let outputs = inner.get_all_outputs(first.id).await.unwrap();
    assert_eq!(
        outputs.len(),
        1,
        "a stale router appended its old route after handoff: {outputs:?}"
    );
    assert_eq!(outputs[0].id, marker.id);
    assert_eq!(outputs[0].output, marker.output);
    let after = inner.get_execution_tree(first.id).await.unwrap();
    assert_eq!(
        after[0].state, before[0].state,
        "a stale router settled a replacement owner's node"
    );
    assert!(
        matches!(
            result,
            Err(EngineError::ClaimLost { .. }) | Ok(EvalOutcome::MoreWork { .. })
        ),
        "unexpected ownership-loss outcome: {result:?}"
    );
}

#[tokio::test]
async fn router_output_read_failure_is_transient_and_does_not_persist_a_default_decision() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let sequence = SequenceBuilder::new("router-io-failure")
        .block(BlockDefinition::Router(Box::new(RouterDef {
            id: BlockId::new("route"),
            routes: vec![Route {
                condition: "outputs.prev.val > 100".into(),
                blocks: Vec::new(),
            }],
            default: Some(Vec::new()),
        })))
        .build();
    inner.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    inner.create_instance(&instance).await.unwrap();
    let current = inner
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let prior = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: instance.id,
        block_id: BlockId::new("prev"),
        output: json!({"val": 101}),
        output_ref: None,
        output_size: 11,
        attempt: 0,
        created_at: Utc::now(),
    };
    inner.save_block_output(&prior).await.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    faults.on("get_all_outputs").times(1).fail_transient();
    let result = evaluate_with_claim_token(
        &storage,
        &HandlerRegistry::new(),
        &current,
        &sequence,
        &SharedClock::default(),
        current.claimed_at.unwrap(),
        &AtomicBool::new(false),
    )
    .await;
    assert_eq!(faults.injected("get_all_outputs"), 1);
    assert!(
        matches!(&result, Err(error) if error.is_transient_storage()),
        "a failed condition-input read must remain retryable: {result:?}"
    );
    assert!(
        inner
            .get_block_output(instance.id, &BlockId::new("route"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(inner.get_all_outputs(instance.id).await.unwrap().len(), 1);
}

struct LoopReplacement {
    marker: BlockOutput,
    sentinel: BlockOutput,
    tree: serde_json::Value,
}

async fn replace_loop_owner(
    inner: &dyn StorageBackend,
    first: &orch8_types::instance::TaskInstance,
    work: orch8_types::ids::ExecutionNodeId,
    at: chrono::DateTime<Utc>,
    token: chrono::DateTime<Utc>,
    read_number: u64,
) -> LoopReplacement {
    let next = at + chrono::Duration::seconds(1);
    assert!(
        inner
            .conditional_update_claimed_instance_state(
                first.id,
                token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next)
            )
            .await
            .unwrap()
    );
    let replacement = inner
        .claim_due_instances(next, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let current = replacement.claimed_at.unwrap();
    let iteration = if read_number == 2 { 1 } else { 4 };
    let marker = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: first.id,
        block_id: BlockId::new("loop"),
        output: json!({"_iterations": iteration}),
        output_ref: None,
        output_size: 17,
        attempt: 0,
        created_at: Utc::now(),
    };
    assert!(
        inner
            .save_block_output_claimed(&marker, current)
            .await
            .unwrap()
    );
    assert!(
        inner
            .update_node_state_claimed(first.id, work, NodeState::Running, current)
            .await
            .unwrap()
    );
    let sentinel = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: first.id,
        block_id: BlockId::new("work"),
        output: json!({}),
        output_ref: Some("__in_progress__".into()),
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    };
    assert!(
        inner
            .save_block_output_claimed(&sentinel, current)
            .await
            .unwrap()
    );
    let before = serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap();
    LoopReplacement {
        marker,
        sentinel,
        tree: before,
    }
}

async fn loop_handoff_during_read(read_number: u64) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let block: BlockDefinition = serde_json::from_value(json!({
        "type": "loop", "id": "loop", "condition": "true", "max_iterations": 10,
        "body": [{"type": "step", "id": "work", "handler": "noop"}]
    }))
    .unwrap();
    let sequence = SequenceBuilder::new("loop-handoff").block(block).build();
    inner.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    inner.create_instance(&instance).await.unwrap();
    let at = Utc::now() + chrono::Duration::seconds(1);
    let first = inner
        .claim_due_instances(at, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let token = first.claimed_at.unwrap();
    let tree =
        orch8_engine::evaluator::ensure_execution_tree(inner.as_ref(), &first, &sequence.blocks)
            .await
            .unwrap();
    let work = tree
        .iter()
        .find(|node| node.block_id == BlockId::new("work"))
        .unwrap()
        .id;
    inner
        .update_node_state(work, NodeState::Completed)
        .await
        .unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    // Read 1 is the loop counter; read 2 is the body bookkeeping lookup
    // after the old owner's advanced counter and node reset were committed.
    let pause = faults.on("get_block_output").nth(read_number).pause();
    let registry = HandlerRegistry::new();
    let clock = SharedClock::default();
    let stop = AtomicBool::new(false);
    let mut evaluation = Box::pin(evaluate_with_claim_token(
        &storage, &registry, &first, &sequence, &clock, token, &stop,
    ));
    tokio::select! {
        () = pause.entered() => {},
        result = &mut evaluation => panic!("loop finished before handoff: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(10)) => panic!("loop did not reach the read rendezvous"),
    }
    if read_number == 2 {
        let advanced = inner
            .get_block_output(first.id, &BlockId::new("loop"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(advanced.output["_iterations"], 1);
        assert_eq!(advanced.output["_reset_pending"], true);
    }
    let replacement =
        replace_loop_owner(inner.as_ref(), &first, work, at, token, read_number).await;
    stop.store(true, Ordering::Release);
    pause.release();
    let result = evaluation.await;
    let actual = inner
        .get_block_output(first.id, &BlockId::new("loop"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        actual.id, replacement.marker.id,
        "a stale loop advanced the new owner's iteration"
    );
    assert_eq!(
        inner
            .get_block_output(first.id, &BlockId::new("work"))
            .await
            .unwrap()
            .unwrap()
            .id,
        replacement.sentinel.id,
        "a stale reset erased the new owner's in-flight evidence"
    );
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap(),
        replacement.tree,
        "a stale loop rewound the replacement's nodes"
    );
    assert!(
        matches!(
            result,
            Err(EngineError::ClaimLost { .. }) | Ok(EvalOutcome::MoreWork { .. })
        ),
        "unexpected ownership outcome: {result:?}"
    );
}

#[tokio::test]
async fn stale_loop_cannot_advance_a_replacement_iteration() {
    loop_handoff_during_read(1).await;
}

#[tokio::test]
async fn stale_loop_reset_cannot_erase_replacement_bookkeeping() {
    loop_handoff_during_read(2).await;
}
