//! Storage outages during collection reads must leave `for_each` retryable.
use chrono::Utc;
use orch8_engine::{evaluator::evaluate_with_claim_token, handlers::HandlerRegistry};
use orch8_storage::{
    StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder},
    },
};
use orch8_types::{
    clock::SharedClock, execution::NodeState, ids::BlockId, output::BlockOutput,
    sequence::BlockDefinition,
};
use serde_json::json;
use std::sync::{Arc, atomic::AtomicBool};

async fn collection_read_failure(method: &'static str, persisted_snapshot: bool) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let block: BlockDefinition = serde_json::from_value(json!({
        "type":"for_each", "id":"each", "collection":"{{ steps.seed.items }}",
        "item_var":"item", "body":[{"type":"step", "id":"work", "handler":"noop"}]
    }))
    .unwrap();
    let sequence = SequenceBuilder::new("collection-read-error")
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
    let output = BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: first.id,
        block_id: BlockId::new(if persisted_snapshot { "each" } else { "seed" }),
        output: if persisted_snapshot {
            json!({"_index":0,"_total":1,"_item_var":"item"})
        } else {
            json!({"items":["keep"]})
        },
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    };
    inner.save_block_output(&output).await.unwrap();
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    faults.on(method).times(1).fail_transient();
    let result = evaluate_with_claim_token(
        &storage,
        &HandlerRegistry::new(),
        &first,
        &sequence,
        &SharedClock::default(),
        first.claimed_at.unwrap(),
        &AtomicBool::new(false),
    )
    .await;
    assert_eq!(faults.injected(method), 1);
    assert!(
        matches!(&result, Err(error) if error.is_transient_storage()),
        "collection storage failure must retain retry classification: {result:?}"
    );
    let outputs = inner.get_all_outputs(first.id).await.unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].id, output.id);
    assert!(
        inner
            .get_execution_tree(first.id)
            .await
            .unwrap()
            .iter()
            .all(|node| !matches!(
                node.state,
                NodeState::Completed | NodeState::Failed | NodeState::Skipped
            ))
    );
}

#[tokio::test]
async fn first_collection_output_read_failure_remains_retryable() {
    collection_read_failure("get_latest_completed_outputs", false).await;
}

#[tokio::test]
async fn persisted_snapshot_read_failure_remains_retryable() {
    collection_read_failure("get_externalized_state", true).await;
}
