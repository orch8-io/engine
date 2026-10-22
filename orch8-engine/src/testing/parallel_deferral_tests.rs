//! Parallel wake-ups must preserve sibling results and individual backoff.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::error::StepError;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, ParallelDef, SequenceDefinition, StepDef};
use serde_json::json;

use super::TestEngine;

fn parallel(left: StepDef, right: StepDef) -> SequenceDefinition {
    SequenceBuilder::new("parallel-deferral")
        .block(BlockDefinition::Parallel(Box::new(ParallelDef {
            id: BlockId::new("parallel"),
            branches: vec![
                vec![BlockDefinition::Step(Box::new(left))],
                vec![BlockDefinition::Step(Box::new(right))],
            ],
        })))
        .build()
}

fn delayed(id: &str, millis: u64) -> StepDef {
    let mut work = step(id, "noop");
    work.delay = Some(serde_json::from_value(json!({"duration": millis})).unwrap());
    work
}

#[tokio::test]
async fn parallel_delay_preserves_a_ready_siblings_output() {
    let engine = TestEngine::builder()
        .register("noop", |ctx| async move {
            Ok(json!({"block": ctx.block_id.as_str()}))
        })
        .build()
        .await;
    let sequence = parallel(delayed("delayed", 10_000), step("ready", "noop"));
    let id = engine.start(&sequence).await;
    engine.tick().await;
    assert_eq!(
        engine
            .storage
            .get_instance(id)
            .await
            .unwrap()
            .unwrap()
            .state,
        InstanceState::Scheduled
    );
    let ready = engine
        .storage
        .get_block_output(id, &BlockId::new("ready"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.output, json!({"block": "ready"}));
    engine.advance(Duration::from_secs(11));
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
    assert_eq!(
        engine
            .storage
            .get_block_output(id, &BlockId::new("ready"))
            .await
            .unwrap()
            .unwrap()
            .id,
        ready.id
    );
}

#[tokio::test]
async fn parallel_delays_resume_at_each_siblings_own_deadline() {
    let left = Arc::new(AtomicUsize::new(0));
    let right = Arc::new(AtomicUsize::new(0));
    let engine = TestEngine::builder()
        .register("noop", {
            let left = Arc::clone(&left);
            let right = Arc::clone(&right);
            move |ctx| {
                let counter = if ctx.block_id.as_str() == "left" {
                    Arc::clone(&left)
                } else {
                    Arc::clone(&right)
                };
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(json!(true))
                }
            }
        })
        .build()
        .await;
    let id = engine
        .start(&parallel(delayed("left", 10_000), delayed("right", 20_000)))
        .await;
    engine.tick().await;
    assert_eq!(
        (left.load(Ordering::SeqCst), right.load(Ordering::SeqCst)),
        (0, 0)
    );
    engine.advance(Duration::from_secs(11));
    engine.tick().await;
    assert_eq!(
        (left.load(Ordering::SeqCst), right.load(Ordering::SeqCst)),
        (1, 0)
    );
    engine.advance(Duration::from_secs(10));
    engine.tick().await;
    assert_eq!(
        (left.load(Ordering::SeqCst), right.load(Ordering::SeqCst)),
        (1, 1)
    );
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
async fn parallel_retries_preserve_backoff_after_an_earlier_sibling_wake() {
    let fast = Arc::new(AtomicUsize::new(0));
    let slow = Arc::new(AtomicUsize::new(0));
    let engine = TestEngine::builder()
        .register("noop", {
            let fast = Arc::clone(&fast);
            let slow = Arc::clone(&slow);
            move |ctx| {
                let counter = if ctx.block_id.as_str() == "fast" {
                    Arc::clone(&fast)
                } else {
                    Arc::clone(&slow)
                };
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(StepError::Retryable {
                            message: "try again".into(),
                            details: None,
                        })
                    } else {
                        Ok(json!(true))
                    }
                }
            }
        })
        .build()
        .await;
    let mut fast_step = step("fast", "noop");
    fast_step.retry = Some(
        serde_json::from_value(
            json!({"max_attempts": 3, "initial_backoff": 1000, "max_backoff": 1000}),
        )
        .unwrap(),
    );
    let mut slow_step = step("slow", "noop");
    slow_step.retry = Some(
        serde_json::from_value(
            json!({"max_attempts": 3, "initial_backoff": 20000, "max_backoff": 20000}),
        )
        .unwrap(),
    );
    let id = engine.start(&parallel(fast_step, slow_step)).await;
    engine.tick().await;
    assert_eq!(
        (fast.load(Ordering::SeqCst), slow.load(Ordering::SeqCst)),
        (1, 1)
    );
    engine.advance(Duration::from_secs(2));
    engine.tick().await;
    assert_eq!(
        (fast.load(Ordering::SeqCst), slow.load(Ordering::SeqCst)),
        (2, 1)
    );
    engine.tick().await;
    assert_eq!(
        slow.load(Ordering::SeqCst),
        1,
        "early wake must not bypass slow backoff"
    );
    engine.advance(Duration::from_secs(30));
    engine.tick().await;
    assert_eq!(
        (fast.load(Ordering::SeqCst), slow.load(Ordering::SeqCst)),
        (2, 2)
    );
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
