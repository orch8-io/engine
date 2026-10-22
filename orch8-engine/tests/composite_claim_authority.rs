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
