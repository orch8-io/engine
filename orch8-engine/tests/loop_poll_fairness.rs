//! A loop's polling interval must not delay independent parallel branches.
use orch8_engine::testing::TestEngine;
use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::{
    ids::BlockId,
    instance::InstanceState,
    sequence::{BlockDefinition, LoopDef, ParallelDef},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn poll_start() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc)
}

fn polling_loop() -> BlockDefinition {
    BlockDefinition::Loop(Box::new(LoopDef {
        id: BlockId::new("poller"),
        condition: "true".into(),
        body: vec![BlockDefinition::Step(Box::new(step("poll_body", "poll")))],
        max_iterations: 2,
        break_on: None,
        continue_on_error: false,
        poll_interval: Some(600),
        retain_iterations: None,
    }))
}

#[tokio::test]
async fn polling_loop_preserves_its_interval_without_parking_ready_siblings() {
    let iterations = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&iterations);
    let engine = TestEngine::builder()
        .start_at(poll_start())
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    let loop_block = polling_loop();
    let sequence = SequenceBuilder::new("loop-poll-fairness")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![
                vec![loop_block],
                vec![
                    BlockDefinition::Step(Box::new(step("ready1", "noop"))),
                    BlockDefinition::Step(Box::new(step("ready2", "noop"))),
                    BlockDefinition::Step(Box::new(step("ready3", "noop"))),
                ],
            ],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    for name in ["ready1", "ready2", "ready3"] {
        assert!(
            engine
                .storage
                .get_block_output(id, &BlockId::new(name))
                .await
                .unwrap()
                .is_some(),
            "ready sibling {name} was delayed by an unrelated polling loop"
        );
    }
    assert_eq!(
        iterations.load(Ordering::SeqCst),
        1,
        "poll interval was bypassed in the same tick"
    );
    let parked = engine.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(parked.state, InstanceState::Scheduled);
    assert_eq!(
        parked.next_fire_at,
        Some(engine.now() + chrono::Duration::seconds(600))
    );
    engine.advance(Duration::from_secs(599));
    engine.tick().await;
    assert_eq!(
        iterations.load(Ordering::SeqCst),
        1,
        "loop executed before its poll deadline"
    );
    engine.advance(Duration::from_secs(2));
    engine.tick().await;
    assert_eq!(iterations.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

#[tokio::test]
async fn early_wake_after_restart_keeps_the_original_poll_deadline() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = TestEngine::builder()
        .start_at(poll_start())
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    let sequence = SequenceBuilder::new("poll-restart")
        .block(polling_loop())
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let original_due = engine.now() + chrono::Duration::seconds(600);
    let restart_at = engine.now() + chrono::Duration::seconds(30);
    let storage = Arc::clone(&engine.storage);
    drop(engine);
    // An operator or external signal may re-arm the parent earlier than the
    // poll deadline. A fresh evaluator must recover the original deadline.
    assert!(
        storage
            .conditional_update_instance_state(
                id,
                InstanceState::Scheduled,
                InstanceState::Scheduled,
                Some(restart_at)
            )
            .await
            .unwrap()
    );
    let counted = Arc::clone(&calls);
    let restarted = TestEngine::builder()
        .storage(storage)
        .start_at(restart_at)
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    restarted.tick().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "early wake bypassed the persisted poll interval"
    );
    let parked = restarted.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(
        parked.next_fire_at,
        Some(original_due),
        "early wake restarted the poll delay"
    );
    restarted.advance(Duration::from_secs(570));
    restarted.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        restarted
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

#[tokio::test]
async fn evaluation_budget_yields_keep_a_poll_deadline_while_ready_work_progresses() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = TestEngine::builder()
        .start_at(poll_start())
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    let ready = (0..220)
        .map(|i| BlockDefinition::Step(Box::new(step(&format!("ready{i}"), "noop"))))
        .collect();
    let sequence = SequenceBuilder::new("poll-budget-yields")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![vec![polling_loop()], ready],
        })))
        .build();
    let id = engine.start(&sequence).await;
    for _ in 0..10 {
        engine.tick().await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "poll interval was lost across an evaluation-budget yield"
        );
        if engine
            .storage
            .get_block_output(id, &BlockId::new("ready219"))
            .await
            .unwrap()
            .is_some()
        {
            let parked = engine.storage.get_instance(id).await.unwrap().unwrap();
            assert_eq!(parked.state, InstanceState::Scheduled);
            assert_eq!(
                parked.next_fire_at,
                Some(engine.now() + chrono::Duration::seconds(600))
            );
            return;
        }
    }
    panic!("polling loop starved the ready branch across evaluation-budget yields");
}

#[tokio::test]
async fn poll_deadline_rounds_up_to_the_native_storage_clock_precision() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let start = poll_start() + chrono::Duration::nanoseconds(789);
    let engine = TestEngine::builder()
        .start_at(start)
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    let sequence = SequenceBuilder::new("poll-clock-precision")
        .block(polling_loop())
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    let due = poll_start() + chrono::Duration::seconds(600) + chrono::Duration::microseconds(1);
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .next_fire_at,
        Some(due)
    );
    engine.advance(Duration::from_secs(600));
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    engine.advance(Duration::from_micros(1));
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Completed
    );
}

#[tokio::test]
async fn interrupted_iteration_reset_retains_the_original_poll_deadline() {
    use orch8_storage::{StorageBackend, sqlite::SqliteStorage, testing::faulty::FaultStorage};
    let inner: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let (storage, faults) = FaultStorage::wrap(Arc::clone(&inner));
    faults
        .on("retire_effect_attempts_for_blocks_claimed")
        .times(1)
        .fail_transient();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);
    let engine = TestEngine::builder()
        .storage(storage)
        .start_at(poll_start())
        .register("poll", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Ok(json!({})) }
        })
        .build()
        .await;
    let sequence = SequenceBuilder::new("poll-reset-recovery")
        .block(polling_loop())
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    let progress = inner
        .get_block_output(id, &BlockId::new("poller"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.output["_iterations"], 1);
    assert_eq!(progress.output["_reset_pending"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let due = poll_start() + chrono::Duration::seconds(600);
    engine.advance(Duration::from_secs(60));
    engine.tick().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "recovery started a body before its poll deadline"
    );
    assert_eq!(
        inner.get_instance(id).await.unwrap().unwrap().next_fire_at,
        Some(due)
    );
    let progress = inner
        .get_block_output(id, &BlockId::new("poller"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(progress.output["_iterations"], 1);
    assert!(progress.output.get("_reset_pending").is_none());
    engine.advance(Duration::from_secs(540));
    engine.tick().await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        inner.get_instance(id).await.unwrap().unwrap().state,
        InstanceState::Completed
    );
}

async fn race_against_polling_loop(nested: bool) {
    use orch8_types::sequence::{RaceDef, RaceSemantics};
    let engine = TestEngine::builder()
        .start_at(poll_start())
        .register("poll", |_| async { Ok(json!({})) })
        .build()
        .await;
    let branch = if nested {
        BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("nested"),
            branches: vec![
                vec![polling_loop()],
                vec![BlockDefinition::Step(Box::new(step(
                    "inside_ready",
                    "noop",
                )))],
            ],
        }))
    } else {
        polling_loop()
    };
    let sequence = SequenceBuilder::new("race-poll-deferral")
        .block(BlockDefinition::Race(Box::new(RaceDef {
            id: BlockId::new("race"),
            semantics: RaceSemantics::FirstToResolve,
            branches: vec![
                vec![branch],
                vec![BlockDefinition::Step(Box::new(step("winner", "noop")))],
            ],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("winner"))
            .await
            .unwrap()
            .is_some(),
        "deferred composite prevented an independent race winner from running"
    );
    let instance = engine.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(instance.state, InstanceState::Completed);
    assert!(
        instance.next_fire_at.is_none(),
        "cancelled polling loser scheduled a spurious wake"
    );
}

#[tokio::test]
async fn deferred_loop_does_not_hold_an_independent_race_winner() {
    race_against_polling_loop(false).await;
}
#[tokio::test]
async fn deferred_nested_composite_does_not_hold_an_independent_race_winner() {
    race_against_polling_loop(true).await;
}

#[tokio::test]
async fn many_deferred_race_leaves_cannot_exhaust_the_ready_winners_work_budget() {
    use orch8_types::sequence::{RaceDef, RaceSemantics};
    let engine = TestEngine::builder()
        .start_at(poll_start())
        .register("poll", |_| async { Ok(json!({})) })
        .build()
        .await;
    let deferred = (0..220)
        .map(|i| {
            let mut delayed = step(&format!("delayed{i}"), "noop");
            delayed.delay = Some(serde_json::from_value(json!({"duration":60_000})).unwrap());
            vec![BlockDefinition::Step(Box::new(delayed))]
        })
        .collect();
    let sequence = SequenceBuilder::new("race-deferral-budget")
        .block(BlockDefinition::Race(Box::new(RaceDef {
            id: BlockId::new("race"),
            semantics: RaceSemantics::FirstToResolve,
            branches: vec![
                vec![polling_loop()],
                vec![BlockDefinition::Parallel(Box::new(ParallelDef {
                    id: BlockId::new("delayed_parallel"),
                    branches: deferred,
                }))],
                vec![BlockDefinition::Step(Box::new(step("winner", "noop")))],
            ],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("winner"))
            .await
            .unwrap()
            .is_some(),
        "fresh deferrals exhausted the work budget before a ready race winner could run"
    );
    let instance = engine.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(instance.state, InstanceState::Completed);
    assert!(instance.next_fire_at.is_none());
}
