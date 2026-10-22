//! Deferred branches must not block runnable work elsewhere in the tree.
use std::time::Duration;

use orch8_engine::testing::TestEngine;
use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, ParallelDef, StepDef};
use serde_json::json;

fn delayed(id: &str, millis: u64) -> StepDef {
    let mut work = step(id, "noop");
    work.delay = Some(serde_json::from_value(json!({"duration": millis})).unwrap());
    work
}

#[tokio::test]
async fn parallel_delay_does_not_park_a_third_ready_branch() {
    let engine = TestEngine::builder()
        .register("noop", |ctx| async move {
            Ok(json!({"block": ctx.block_id.as_str()}))
        })
        .build()
        .await;
    let sequence = SequenceBuilder::new("wide-parallel-deferral")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![
                vec![BlockDefinition::Step(Box::new(delayed("delayed", 60_000)))],
                vec![BlockDefinition::Step(Box::new(step("ready1", "noop")))],
                vec![BlockDefinition::Step(Box::new(step("ready2", "noop")))],
            ],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    for name in ["ready1", "ready2"] {
        assert!(
            engine
                .storage
                .get_block_output(id, &BlockId::new(name))
                .await
                .unwrap()
                .is_some(),
            "ready branch {name} must run before the delayed branch wakes"
        );
    }
    assert!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("delayed"))
            .await
            .unwrap()
            .is_none()
    );
    engine.advance(Duration::from_secs(61));
    engine.tick().await;
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
async fn parallel_delay_does_not_park_a_ready_siblings_next_step() {
    let engine = TestEngine::builder()
        .register("noop", |ctx| async move {
            Ok(json!({"block": ctx.block_id.as_str()}))
        })
        .build()
        .await;
    let sequence = SequenceBuilder::new("sequential-parallel-deferral")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![
                vec![BlockDefinition::Step(Box::new(delayed("delayed", 60_000)))],
                vec![
                    BlockDefinition::Step(Box::new(step("ready1", "noop"))),
                    BlockDefinition::Step(Box::new(step("ready2", "noop"))),
                ],
            ],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    for name in ["ready1", "ready2"] {
        assert!(
            engine
                .storage
                .get_block_output(id, &BlockId::new(name))
                .await
                .unwrap()
                .is_some(),
            "ready sibling step {name} must run before the delayed branch wakes"
        );
    }
    engine.advance(Duration::from_secs(61));
    engine.tick().await;
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
async fn evaluation_budget_yields_runnable_work_to_later_ticks() {
    let engine = TestEngine::builder()
        .register("noop", |_| async { Ok(json!(true)) })
        .build()
        .await;
    // Force tree evaluation, followed by enough ordered roots to exceed a
    // single evaluator pass. No external signal is required to finish them.
    let mut builder = SequenceBuilder::new("evaluation-budget").block(BlockDefinition::Parallel(
        Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![vec![BlockDefinition::Step(Box::new(step("first", "noop")))]],
        }),
    ));
    for index in 0..220 {
        builder = builder.step(&format!("work{index}"), "noop");
    }
    let id = engine.start(&builder.build()).await;
    for _ in 0..10 {
        engine.tick().await;
        if engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state
            == InstanceState::Completed
        {
            assert_eq!(engine.storage.get_all_outputs(id).await.unwrap().len(), 221);
            return;
        }
    }
    panic!(
        "runnable work stalled in {:?}",
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state
    );
}

#[tokio::test]
async fn evaluation_budget_does_not_starve_ready_work_behind_many_deferrals() {
    let engine = TestEngine::builder()
        .register("noop", |_| async { Ok(json!(true)) })
        .build()
        .await;
    let mut branches = Vec::new();
    for group in 0..2 {
        let leaves = (0..256)
            .map(|index| {
                vec![BlockDefinition::Step(Box::new(delayed(
                    &format!("deferred{group}_{index}"),
                    60_000,
                )))]
            })
            .collect();
        branches.push(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new(format!("group{group}")),
            branches: leaves,
        }))]);
    }
    branches.push(vec![BlockDefinition::Parallel(Box::new(ParallelDef {
        id: BlockId::new("ready-group"),
        branches: vec![vec![BlockDefinition::Step(Box::new(step("ready", "noop")))]],
    }))]);
    let sequence = SequenceBuilder::new("many-deferrals")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("outer"),
            branches,
        })))
        .build();
    let id = engine.start(&sequence).await;
    for _ in 0..5 {
        engine.tick().await;
    }
    assert!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("ready"))
            .await
            .unwrap()
            .is_some(),
        "revisiting deferred leaves must not consume every evaluation budget"
    );
    engine.advance(Duration::from_secs(61));
    for _ in 0..10 {
        engine.tick().await;
        if engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state
            == InstanceState::Completed
        {
            assert_eq!(engine.storage.get_all_outputs(id).await.unwrap().len(), 513);
            return;
        }
    }
    panic!("all deferred branches must eventually finish");
}

#[tokio::test]
async fn cancelled_race_loser_does_not_schedule_a_spurious_wake() {
    use orch8_types::execution::NodeState;
    use orch8_types::sequence::{RaceDef, RaceSemantics};

    let engine = TestEngine::builder()
        .register("noop", |_| async { Ok(json!(true)) })
        .build()
        .await;
    let mut gate = step("gate", "noop");
    gate.wait_for_input = Some(serde_json::from_value(json!({"prompt": "continue?"})).unwrap());
    let race = BlockDefinition::Race(Box::new(RaceDef {
        id: BlockId::new("race"),
        semantics: RaceSemantics::FirstToResolve,
        branches: vec![
            vec![BlockDefinition::Step(Box::new(delayed("loser", 60_000)))],
            vec![BlockDefinition::Step(Box::new(step("winner", "noop")))],
        ],
    }));
    let sequence = SequenceBuilder::new("cancelled-deferral")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![vec![race], vec![BlockDefinition::Step(Box::new(gate))]],
        })))
        .build();
    let id = engine.start(&sequence).await;
    engine.tick().await;
    let instance = engine.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(instance.state, InstanceState::Waiting);
    assert!(instance.next_fire_at.is_none());
    let tree = engine.storage.get_execution_tree(id).await.unwrap();
    assert_eq!(
        tree.iter()
            .find(|node| node.block_id.as_str() == "loser")
            .unwrap()
            .state,
        NodeState::Cancelled
    );
    assert!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("loser"))
            .await
            .unwrap()
            .is_none()
    );
}
