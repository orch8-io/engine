//! A recovered parent must not inherit children spawned by its stale worker.
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
use orch8_types::{clock::SharedClock, instance::InstanceState, sequence::BlockDefinition};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

#[tokio::test]
async fn stale_parent_cannot_spawn_a_child_after_claim_replacement() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let child_sequence = SequenceBuilder::new("sub-sequence-handoff")
        .name("child")
        .step("work", "noop")
        .build();
    inner.create_sequence(&child_sequence).await.unwrap();
    let block: BlockDefinition = serde_json::from_value(json!({
        "type":"sub_sequence", "id":"spawn", "sequence_name":"child", "input":{}
    }))
    .unwrap();
    let sequence = SequenceBuilder::new("sub-sequence-handoff")
        .block(block)
        .build();
    inner.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    inner.create_instance(&instance).await.unwrap();
    let first = inner
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let token = first.claimed_at.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let pause = faults.on("get_sequence_by_name").times(1).pause();
    let registry = HandlerRegistry::new();
    let clock = SharedClock::default();
    let stop = AtomicBool::new(false);
    let mut evaluation = Box::pin(evaluate_with_claim_token(
        &storage, &registry, &first, &sequence, &clock, token, &stop,
    ));
    tokio::select! {
        () = pause.entered() => {},
        result = &mut evaluation => panic!("parent finished before spawn lookup: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(10)) => panic!("parent did not reach spawn lookup"),
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
    assert_ne!(token, replacement.claimed_at.unwrap());
    let before = serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap();
    assert!(
        inner
            .get_child_instances(first.id)
            .await
            .unwrap()
            .is_empty()
    );
    stop.store(true, Ordering::Release);
    pause.release();
    let result = evaluation.await;
    assert!(
        inner
            .get_child_instances(first.id)
            .await
            .unwrap()
            .is_empty(),
        "stale worker spawned a runnable child after losing its parent claim"
    );
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap(),
        before
    );
    assert!(
        matches!(
            result,
            Err(EngineError::ClaimLost { .. }) | Ok(EvalOutcome::MoreWork { .. })
        ),
        "unexpected ownership-loss outcome: {result:?}"
    );
}
