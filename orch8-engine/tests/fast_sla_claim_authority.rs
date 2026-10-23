//! A delayed fast-path SLA check cannot fail a newly claimed run.
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
    instance::InstanceState,
    output::BlockOutput,
    sequence::{BlockDefinition, EscalationDef},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn replace_sla_owner(storage: &dyn StorageBackend, id: orch8_types::ids::InstanceId) {
    let old_token = storage
        .get_instance(id)
        .await
        .unwrap()
        .unwrap()
        .claimed_at
        .unwrap();
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
            .save_block_output_claimed(
                &BlockOutput {
                    id: uuid::Uuid::now_v7(),
                    instance_id: id,
                    block_id: BlockId::new("work"),
                    output: json!({"owner":"replacement"}),
                    output_ref: None,
                    output_size: 23,
                    attempt: 0,
                    created_at: Utc::now(),
                },
                token
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn stale_fast_sla_check_cannot_fail_or_escalate_the_replacement_claim() {
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    let ordinary = faults
        .on("save_output_and_conditional_transition")
        .times(1)
        .pause();
    let claimed = faults
        .on("save_output_and_claimed_transition")
        .times(1)
        .pause();
    let escalations = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&escalations);
    let engine = Arc::new(
        TestEngine::builder()
            .storage(storage)
            .register("escalate", move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Ok(json!({})) }
            })
            .build()
            .await,
    );
    let mut work = step("work", "noop");
    work.deadline = Some(Duration::from_secs(1));
    work.on_deadline_breach = Some(EscalationDef {
        handler: "escalate".into(),
        params: json!({}),
    });
    let sequence = SequenceBuilder::new("fast-sla-handoff")
        .block(BlockDefinition::Step(Box::new(work)))
        .build();
    inner.create_sequence(&sequence).await.unwrap();
    let mut instance = InstanceBuilder::for_sequence(&sequence).due_now().build();
    instance.context.runtime.started_at = Some(engine.now() - chrono::Duration::hours(1));
    let id = instance.id;
    inner.create_instance(&instance).await.unwrap();
    let old_engine = Arc::clone(&engine);
    let mut old_tick = tokio::spawn(async move { old_engine.try_tick().await });
    tokio::select! {
        () = ordinary.entered() => {},
        () = claimed.entered() => {},
        result = &mut old_tick => panic!("tick ended before SLA persistence: {result:?}"),
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("SLA check did not reach persistence"),
    }
    replace_sla_owner(inner.as_ref(), id).await;
    let before = inner.get_instance(id).await.unwrap().unwrap();
    let outputs = serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap();
    ordinary.release();
    claimed.release();
    tokio::time::timeout(Duration::from_secs(5), old_tick)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = inner.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        after.state, before.state,
        "stale SLA check failed the replacement run"
    );
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(after.claimed_at, before.claimed_at);
    assert_eq!(
        serde_json::to_value(inner.get_all_outputs(id).await.unwrap()).unwrap(),
        outputs
    );
    assert_eq!(
        escalations.load(Ordering::SeqCst),
        0,
        "stale SLA check invoked escalation"
    );
}
