//! Budget and delay bookkeeping must retain the scheduler's original claim.
use chrono::Utc;
use orch8_engine::testing::TestEngine;
use orch8_storage::{
    StorageBackend,
    sqlite::SqliteStorage,
    testing::{
        faulty::FaultStorage,
        fixtures::{InstanceBuilder, SequenceBuilder, step},
    },
};
use orch8_types::{
    ids::BlockId,
    instance::{Budget, InstanceState},
    sequence::{BlockDefinition, ParallelDef},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Copy)]
enum Boundary {
    Budget,
    FlatDelay,
    TreeDelay,
}

async fn replace_owner(
    storage: &dyn StorageBackend,
    id: orch8_types::ids::InstanceId,
) -> orch8_types::instance::TaskInstance {
    let old = storage.get_instance(id).await.unwrap().unwrap();
    let old_token = old.claimed_at.unwrap();
    let next = Utc::now() + chrono::Duration::seconds(2);
    assert!(
        storage
            .conditional_update_claimed_instance_state(
                id,
                old_token,
                InstanceState::Running,
                InstanceState::Scheduled,
                Some(next)
            )
            .await
            .unwrap()
    );
    let replacement = storage
        .claim_due_instances(next, 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(replacement.id, id);
    let token = replacement.claimed_at.unwrap();
    assert_ne!(token, old_token);
    assert!(
        storage
            .merge_instance_metadata_claimed(
                id,
                &json!({
                    "owner":"replacement", "paused_reason":"operator", "budget_breach":null,
                    "_delay_until:work": (next + chrono::Duration::hours(24)).to_rfc3339()
                }),
                token
            )
            .await
            .unwrap()
    );
    storage.get_instance(id).await.unwrap().unwrap()
}

async fn metadata_handoff(boundary: Boundary) {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let ordinary = faults.on("merge_instance_metadata").times(1).pause();
    let claimed = faults
        .on("merge_instance_metadata_claimed")
        .times(1)
        .pause();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = Arc::new(
        TestEngine::builder()
            .storage(storage)
            .register("noop", move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({})) }
            })
            .build()
            .await,
    );
    let mut work = step("work", "noop");
    if !matches!(boundary, Boundary::Budget) {
        work.delay = Some(serde_json::from_value(json!({"duration":60_000})).unwrap());
    }
    let block = BlockDefinition::Step(Box::new(work));
    let block = if matches!(boundary, Boundary::TreeDelay) {
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![vec![block]],
        }))
    } else {
        block
    };
    let sequence = SequenceBuilder::new("preamble-handoff")
        .block(block)
        .build();
    inner.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    if matches!(boundary, Boundary::Budget) {
        instance.budget = Some(Budget {
            max_steps: Some(1),
            ..Budget::default()
        });
        instance.context.runtime.total_steps_executed = 2;
    }
    let id = instance.id;
    inner.create_instance(&instance).await.unwrap();
    let old_engine = Arc::clone(&engine);
    let mut old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::select! {
        () = ordinary.entered() => {},
        () = claimed.entered() => {},
        result = &mut old_tick => panic!("tick ended before metadata persistence: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("preamble did not reach metadata persistence"),
    }
    let before = replace_owner(inner.as_ref(), id).await;
    let tree = serde_json::to_value(inner.get_execution_tree(id).await.unwrap()).unwrap();
    let calls_before = calls.load(Ordering::SeqCst);
    ordinary.release();
    claimed.release();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = inner.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        after.metadata, before.metadata,
        "stale preamble overwrote replacement metadata"
    );
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.state, before.state);
    assert_eq!(after.next_fire_at, before.next_fire_at);
    assert_eq!(after.claimed_at, before.claimed_at);
    assert_eq!(
        serde_json::to_value(inner.get_execution_tree(id).await.unwrap()).unwrap(),
        tree
    );
    assert_eq!(calls.load(Ordering::SeqCst), calls_before);
}

#[tokio::test]
async fn stale_budget_check_cannot_record_a_breach_for_a_replacement_claim() {
    metadata_handoff(Boundary::Budget).await;
}
#[tokio::test]
async fn stale_flat_delay_cannot_replace_the_current_delay_marker() {
    metadata_handoff(Boundary::FlatDelay).await;
}
#[tokio::test]
async fn stale_tree_delay_cannot_replace_the_current_delay_marker() {
    metadata_handoff(Boundary::TreeDelay).await;
}
