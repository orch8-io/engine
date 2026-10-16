//! Retry back-off and dead-lettering through the real scheduler on virtual
//! time (`orch8_engine::testing::TestEngine`), instead of `drive()` setting
//! the final state itself.
//!
//! Pattern to copy: build a `TestEngine`, `start` a sequence, then either
//! `tick()` and assert the row the scheduler wrote, or `drive()` to the
//! settled state (the clock jumps to each `next_fire_at`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use orch8_engine::testing::TestEngine;
use orch8_storage::testing::fixtures::{SequenceBuilder, retry_policy, step};
use orch8_types::error::StepError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;

const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
/// The engine's `RetryPolicy::max_attempts` counts *re*-tries after the
/// first execution (`orch8_engine::jobs` converts the jobs API's
/// total-executions value by subtracting one).
const MAX_RETRIES: u32 = 3;

/// An engine whose `always_retryable` handler fails retryably and counts
/// its invocations, running one instance with a 3-retry policy.
async fn retrying_instance() -> (TestEngine, InstanceId, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&calls);
    let engine = TestEngine::builder()
        .register("always_retryable", move |_ctx| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err(StepError::Retryable {
                    message: "upstream 503".into(),
                    details: None,
                })
            }
        })
        .build()
        .await;
    let mut s = step("call", "always_retryable");
    s.retry = Some(retry_policy(MAX_RETRIES, INITIAL_BACKOFF));
    let id = engine
        .start(&SequenceBuilder::new("t").step_def(s).build())
        .await;
    (engine, id, calls)
}

#[tokio::test]
async fn retryable_failure_reschedules_with_the_initial_backoff() {
    let (engine, id, calls) = retrying_instance().await;
    let ticked_at = engine.now();

    engine.tick().await;

    let inst = engine.instance(id).await;
    assert_eq!(inst.state, InstanceState::Scheduled);
    let wait_ms = (inst.next_fire_at.expect("retry scheduled") - ticked_at).num_milliseconds();
    // 10 s with the scheduler's 80-120 % jitter.
    assert!((8_000..=12_000).contains(&wait_ms), "backoff {wait_ms} ms");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_does_not_run_before_its_backoff_elapses() {
    let (engine, id, calls) = retrying_instance().await;
    engine.tick().await;

    engine.advance(Duration::from_secs(5));
    engine.tick().await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "retried during back-off");
    assert_eq!(engine.instance(id).await.state, InstanceState::Scheduled);
}

#[tokio::test]
async fn retry_runs_once_the_backoff_has_elapsed() {
    let (engine, id, calls) = retrying_instance().await;
    engine.tick().await;
    let due = engine.instance(id).await.next_fire_at.unwrap();

    engine.clock.set(due);
    engine.tick().await;

    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn exhausted_retries_dead_letter_the_instance_after_the_last_retry() {
    let (engine, id, calls) = retrying_instance().await;

    let settled = engine.drive(id).await;

    assert_eq!(settled.state, InstanceState::Failed, "lands in the DLQ");
    assert_eq!(calls.load(Ordering::SeqCst), 1 + MAX_RETRIES);
}

#[tokio::test]
async fn backoff_grows_between_attempts() {
    let (engine, id, _calls) = retrying_instance().await;
    engine.tick().await;
    let first_due = engine.instance(id).await.next_fire_at.unwrap();
    let first_wait = first_due - engine.now();

    engine.clock.set(first_due);
    engine.tick().await;
    let second_wait = engine.instance(id).await.next_fire_at.unwrap() - engine.now();

    // 10 s then 20 s, each jittered 80-120 %: the ranges cannot overlap.
    assert!(
        second_wait > first_wait,
        "{first_wait:?} then {second_wait:?}"
    );
}
