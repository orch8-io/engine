//! A saga rollback must retain the claim that began its evaluation.
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
    execution::NodeState,
    ids::BlockId,
    instance::{InstanceState, TaskInstance},
    sequence::{BlockDefinition, SequenceDefinition},
};
use serde_json::json;

async fn failed_saga(store: &dyn StorageBackend) -> (SequenceDefinition, TaskInstance) {
    let block: BlockDefinition = serde_json::from_value(json!({
        "type":"saga", "id":"saga", "steps":[{
            "id":"first", "action":{"type":"step", "id":"work", "handler":"noop"}
        }]
    }))
    .unwrap();
    let sequence = SequenceBuilder::new("saga-handoff").block(block).build();
    store.create_sequence(&sequence).await.unwrap();
    let instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    store.create_instance(&instance).await.unwrap();
    let first = store
        .claim_due_instances(Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let tree = orch8_engine::evaluator::ensure_execution_tree(store, &first, &sequence.blocks)
        .await
        .unwrap();
    let work = tree
        .iter()
        .find(|node| node.block_id == BlockId::new("work"))
        .unwrap();
    store
        .update_node_state(work.id, NodeState::Failed)
        .await
        .unwrap();
    (sequence, first)
}

#[tokio::test]
async fn stale_saga_cannot_overwrite_replacement_context_or_settle_its_tree() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (sequence, first) = failed_saga(inner.as_ref()).await;
    let token = first.claimed_at.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    // Support the same handoff rendezvous before and after the claimed API
    // conversion, so the regression demonstrates the old production failure.
    let plain = faults.on("merge_context_data").times(1).pause();
    let claimed = faults.on("merge_context_data_claimed").times(1).pause();
    let handlers = HandlerRegistry::new();
    let clock = SharedClock::default();
    let stop = AtomicBool::new(false);
    let mut evaluation = Box::pin(evaluate_with_claim_token(
        &storage, &handlers, &first, &sequence, &clock, token, &stop,
    ));
    tokio::select! {
        () = plain.entered() => {},
        () = claimed.entered() => {},
        result = &mut evaluation => panic!("saga finished before context write: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(10)) => panic!("saga did not reach context write"),
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
    let current = replacement.claimed_at.unwrap();
    assert_ne!(token, current);
    let replacement_error = json!({"owner":"replacement", "details":{"keep":true}});
    assert!(
        inner
            .merge_context_data_claimed(first.id, "_error", &replacement_error, current)
            .await
            .unwrap()
    );
    let before = inner.get_instance(first.id).await.unwrap().unwrap();
    let tree = serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap();
    stop.store(true, Ordering::Release);
    plain.release();
    claimed.release();
    let result = evaluation.await;
    let after = inner.get_instance(first.id).await.unwrap().unwrap();
    assert_eq!(
        after.context.data, before.context.data,
        "stale saga overwrote replacement context"
    );
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(first.id).await.unwrap()).unwrap(),
        tree
    );
    assert!(
        matches!(
            result,
            Err(EngineError::ClaimLost { .. }) | Ok(EvalOutcome::MoreWork { .. })
        ),
        "unexpected ownership-loss outcome: {result:?}"
    );
}
