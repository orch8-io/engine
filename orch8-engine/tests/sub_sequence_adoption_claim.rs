//! Adopting an earlier child must retain the parent's original execution claim.
use chrono::Utc;
use orch8_engine::{
    error::EngineError,
    evaluator::{EvalOutcome, evaluate_with_claim_token},
    handlers::HandlerRegistry,
};
use orch8_storage::{
    StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    clock::SharedClock,
    instance::{InstanceState, TaskInstance},
    sequence::{BlockDefinition, SequenceDefinition},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

async fn previous_child(
    store: &dyn StorageBackend,
    state: InstanceState,
) -> (SequenceDefinition, TaskInstance, TaskInstance) {
    let child_sequence = SequenceBuilder::new("adoption-handoff")
        .name("child")
        .step("work", "noop")
        .build();
    store.create_sequence(&child_sequence).await.unwrap();
    let block: BlockDefinition = serde_json::from_value(json!({
        "type":"sub_sequence", "id":"spawn", "sequence_name":"child", "input":{}
    }))
    .unwrap();
    let sequence = SequenceBuilder::new("adoption-handoff")
        .block(block)
        .build();
    store.create_sequence(&sequence).await.unwrap();
    let parent = InstanceBuilder::for_sequence(&sequence).due_now().build();
    store.create_instance(&parent).await.unwrap();
    let first = store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut child = InstanceBuilder::for_sequence(&child_sequence)
        .state(state)
        .build();
    child.parent_instance_id = Some(parent.id);
    child.metadata = json!({"_parent_block_id":"spawn", "_parent_activation":"previous-node"});
    child.context.runtime.run_id = Some("previous-run".into());
    store.create_instance(&child).await.unwrap();
    orch8_engine::evaluator::ensure_execution_tree(store, &child, &child_sequence.blocks)
        .await
        .unwrap();
    (sequence, first, child)
}

async fn stale_adoption(state: InstanceState) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (sequence, first, child) = previous_child(inner.as_ref(), state).await;
    let token = first.claimed_at.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let pause = faults.on("merge_instance_metadata").times(1).pause();
    let guarded = faults.on("adopt_child_instance_claimed").times(1).pause();
    let registry = HandlerRegistry::new();
    let clock = SharedClock::default();
    let stop = AtomicBool::new(false);
    let mut evaluation = Box::pin(evaluate_with_claim_token(
        &storage, &registry, &first, &sequence, &clock, token, &stop,
    ));
    tokio::select! {
        () = pause.entered() => {},
        () = guarded.entered() => {},
        result = &mut evaluation => panic!("parent finished before adopting previous child: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(10)) => panic!("parent did not reach adoption write"),
    }
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(
        inner
            .conditional_update_claimed_instance_state(
                first.id,
                token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next),
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
    assert_eq!(replacement.id, first.id);
    assert_ne!(token, replacement.claimed_at.unwrap());
    let before =
        serde_json::to_value(inner.get_instance(child.id).await.unwrap().unwrap()).unwrap();
    let tree = serde_json::to_value(inner.get_execution_tree(child.id).await.unwrap()).unwrap();
    let parent_tree =
        serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap();
    stop.store(true, Ordering::Release);
    pause.release();
    guarded.release();
    let result = evaluation.await;
    assert_eq!(
        serde_json::to_value(inner.get_instance(child.id).await.unwrap().unwrap()).unwrap(),
        before,
        "stale parent rebound or retried a child after losing its claim"
    );
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(child.id).await.unwrap()).unwrap(),
        tree
    );
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap(),
        parent_tree
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
async fn stale_parent_cannot_retry_a_previous_failed_child() {
    stale_adoption(InstanceState::Failed).await;
}

#[tokio::test]
async fn stale_parent_cannot_rebind_a_previous_completed_child() {
    stale_adoption(InstanceState::Completed).await;
}
