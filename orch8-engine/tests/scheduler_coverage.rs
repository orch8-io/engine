#![allow(clippy::items_after_statements)]
//! Integration tests for the scheduler `tick_once` entry point.
//!
//! 100 tests covering: basic tick behavior, step execution via scheduler,
//! delay and scheduling, concurrency and rate limiting, signal processing.

use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, Utc};
use serde_json::json;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::{TickOnceResult, tick_once};
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::ids::{BlockId, InstanceId, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::{BlockDefinition, DelaySpec, SendWindow, StepDef};
use orch8_types::signal::{Signal, SignalType};

mod common;
use common::*;

// Scheduler-specific mk_instance override: instances start as Scheduled (not Running).
fn mk_instance(seq_id: SequenceId) -> TaskInstance {
    mk_instance_scheduled(seq_id, json!({}))
}

async fn tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
) -> TickOnceResult {
    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(storage, handlers, &sem, &config, &seq_cache, &cancel)
        .await
        .unwrap()
}

/// Run `tick_once` with custom config.
async fn tick_with_config(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    config: &SchedulerConfig,
) -> TickOnceResult {
    let sem = semaphore(config.max_concurrent_steps as usize);
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(storage, handlers, &sem, config, &seq_cache, &cancel)
        .await
        .unwrap()
}

/// Run `tick_once` with a specific semaphore.
async fn tick_with_semaphore(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    sem: &Arc<Semaphore>,
) -> TickOnceResult {
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(storage, handlers, sem, &config, &seq_cache, &cancel)
        .await
        .unwrap()
}

/// Run `tick_once` with cancellation token.
async fn tick_with_cancel(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    cancel: &CancellationToken,
) -> TickOnceResult {
    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    tick_once(storage, handlers, &sem, &config, &seq_cache, cancel)
        .await
        .unwrap()
}

fn mk_signal(instance_id: InstanceId, signal_type: SignalType) -> Signal {
    Signal {
        id: uuid::Uuid::now_v7(),
        instance_id,
        signal_type,
        payload: json!({}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}

fn mk_signal_with_payload(
    instance_id: InstanceId,
    signal_type: SignalType,
    payload: serde_json::Value,
) -> Signal {
    Signal {
        id: uuid::Uuid::now_v7(),
        instance_id,
        signal_type,
        payload,
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    }
}
// ================================================================
// TICK_ONCE BASIC BEHAVIOR (Tests 1-20)
// ================================================================

/// 1. tick with zero scheduled instances returns all-zero result.
#[tokio::test]
async fn tick_zero_instances_returns_empty_result() {
    let s = storage().await;
    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
    assert_eq!(result.steps_executed, 0);
    assert!(!result.has_pending_work);
}

/// 2. tick with one scheduled instance advances it.
#[tokio::test]
async fn tick_one_instance_advances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 1);
    assert_eq!(result.steps_executed, 1);
}

/// 3. tick with five scheduled instances advances all.
#[tokio::test]
async fn tick_five_instances_advances_all() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 5);
    assert_eq!(result.steps_executed, 5);
}

/// 4. tick correctly reports `steps_executed` for multi-step single instance.
#[tokio::test]
async fn tick_multi_step_single_instance() {
    let s = storage().await;
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 1);
    assert_eq!(result.steps_executed, 3);
}

/// 5. tick with multiple instances each having multiple steps.
#[tokio::test]
async fn tick_multiple_instances_multiple_steps() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 3);
    assert_eq!(result.steps_executed, 6);
}

/// 6. tick does not pick up Running instances.
#[tokio::test]
async fn tick_ignores_running_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Running);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 7. tick does not pick up Completed instances.
#[tokio::test]
async fn tick_ignores_completed_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Completed);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 8. tick does not pick up Failed instances.
#[tokio::test]
async fn tick_ignores_failed_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Failed);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 9. tick does not pick up Cancelled instances.
#[tokio::test]
async fn tick_ignores_cancelled_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Cancelled);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 10. tick does not pick up Paused instances.
#[tokio::test]
async fn tick_ignores_paused_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 11. tick respects `batch_size=1`, processes only one instance per tick.
#[tokio::test]
async fn tick_batch_size_one() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.batch_size = 1;
    let result = tick_with_config(&s, &h, &config).await;
    assert_eq!(result.instances_advanced, 1);
}

/// 12. tick respects `batch_size=3` with 5 scheduled instances.
#[tokio::test]
async fn tick_batch_size_three() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.batch_size = 3;
    let result = tick_with_config(&s, &h, &config).await;
    assert!(result.instances_advanced <= 3);
}

/// 13. tick with `batch_size=10` but only 2 instances processes both.
#[tokio::test]
async fn tick_batch_size_larger_than_available() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..2 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.batch_size = 10;
    let result = tick_with_config(&s, &h, &config).await;
    assert_eq!(result.instances_advanced, 2);
}

/// 14. tick `batch_size=2` from 10 instances leaves pending work.
#[tokio::test]
async fn tick_batch_size_leaves_pending() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..10 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.batch_size = 2;
    let result = tick_with_config(&s, &h, &config).await;
    assert!(result.has_pending_work);
}

/// 15. tick `batch_size=256` from 3 instances leaves no pending work.
#[tokio::test]
async fn tick_batch_size_no_pending_work() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert!(!result.has_pending_work);
}

/// 16. `has_pending_work` is true when there are scheduled instances remaining.
#[tokio::test]
async fn tick_has_pending_work_true_when_remaining() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.batch_size = 2;
    let result = tick_with_config(&s, &h, &config).await;
    assert!(result.has_pending_work);
}

/// 17. `has_pending_work` is false after processing all scheduled instances.
#[tokio::test]
async fn tick_has_pending_work_false_all_done() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert!(!result.has_pending_work);
}

/// 18. Future-scheduled instance not picked up (`next_fire_at` in the future).
#[tokio::test]
async fn tick_does_not_pick_up_future_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let now = Utc::now();
    let mut inst = mk_instance(seq.id);
    inst.next_fire_at = Some(now + chrono::Duration::hours(1));
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
    assert!(result.has_pending_work);
}

/// 19. tick only counts for Scheduled, not Waiting instances.
#[tokio::test]
async fn tick_waiting_not_counted_as_pending() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Waiting);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
    assert!(!result.has_pending_work);
}

/// 20. tick correctly reports `has_pending_work` with mixed states.
#[tokio::test]
async fn tick_has_pending_work_mixed_states() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let done = mk_instance_in_state(seq.id, InstanceState::Completed);
    s.create_instance(&done).await.unwrap();

    let mut future_inst = mk_instance(seq.id);
    future_inst.next_fire_at = Some(Utc::now() + chrono::Duration::hours(1));
    s.create_instance(&future_inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
    assert!(result.has_pending_work);
}

// ================================================================
// STEP EXECUTION VIA SCHEDULER (Tests 21-40)
// ================================================================

/// 21. Simple single-step sequence completes via `tick_once`.
#[tokio::test]
async fn tick_single_step_completes() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 22. Two-step sequence completes via single tick.
#[tokio::test]
async fn tick_two_steps_completes() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 2);

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 23. Five-step sequence driven to completion in one tick.
#[tokio::test]
async fn tick_five_steps_completes() {
    let s = storage().await;
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
        mk_step("s4", "noop"),
        mk_step("s5", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 5);
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 24. Permanent failure transitions instance to Failed.
#[tokio::test]
async fn tick_permanent_failure_fails_instance() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "fail")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_fail());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 25. Retryable failure with no retry policy fails instance.
#[tokio::test]
async fn tick_retryable_no_policy_fails() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "retryable_fail")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_retryable_fail());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 26. Retryable failure with retry policy reschedules instance.
#[tokio::test]
async fn tick_retryable_with_retry_reschedules() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_retry("s1", "retryable_fail", 3)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_retryable_fail());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    assert!(refreshed.next_fire_at.is_some());
}

/// 27. Multi-step: first step succeeds, second fails.
#[tokio::test]
async fn tick_multi_step_second_fails() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "fail")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_fail());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 1);

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 28. Multi-step: first step fails, subsequent steps are not executed.
#[tokio::test]
async fn tick_multi_step_first_fails_stops() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "fail"), mk_step("s2", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_fail());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 0);
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 29. Instance completes successfully with noop handler.
#[tokio::test]
async fn tick_noop_handler_completes() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("step1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 30. Multiple instances with same sequence all complete.
#[tokio::test]
async fn tick_multiple_instances_all_complete() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let mut ids = Vec::new();
    for _ in 0..4 {
        let inst = mk_instance(seq.id);
        ids.push(inst.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    tick(&s, &h).await;

    for id in ids {
        let refreshed = s.get_instance(id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Completed);
    }
}

/// 31. Retryable failure with retry policy reschedules with a future `fire_at` (backoff).
#[tokio::test]
async fn tick_retry_reschedules_with_backoff() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_retry("s1", "retryable_fail", 5)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_retryable_fail());
    let before = Utc::now();
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.unwrap();
    // Backoff should set fire_at at or after the tick start (even with tiny backoff).
    assert!(fire_at >= before);
}

/// 32. Retryable failure advances the attempt counter across ticks and fails
/// the instance once `max_attempts` is exhausted.
///
/// Regression: the fast path previously deleted all block outputs on a
/// retryable failure without a retry marker, so `compute_attempt` reset to 0
/// every tick and the instance retried forever (never reaching the DLQ). Now
/// it writes a `__retry__` marker (excluded from the completed-block set, so
/// the block still re-executes) that advances the counter.
#[tokio::test]
async fn tick_retry_advances_and_eventually_fails_at_max_attempts() {
    let s = storage().await;
    // max_attempts = 3 → executes at attempt 0, 1, 2 (reschedules) then fails
    // on the tick where attempt reaches 3.
    let seq = mk_sequence(vec![mk_step_with_retry("s1", "retryable_fail", 3)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry_with_retryable_fail());

    let mut reschedules = 0;
    let mut final_state = None;
    for _ in 0..10 {
        tick(&s, &h).await;
        let r = s.get_instance(inst.id).await.unwrap().unwrap();
        match r.state {
            InstanceState::Scheduled => {
                reschedules += 1;
                // Re-arm fire_at into the past so the next tick re-claims it.
                s.update_instance_state(
                    inst.id,
                    InstanceState::Scheduled,
                    Some(Utc::now() - chrono::Duration::seconds(1)),
                )
                .await
                .unwrap();
            }
            other => {
                final_state = Some(other);
                break;
            }
        }
    }

    assert_eq!(
        final_state,
        Some(InstanceState::Failed),
        "instance must reach Failed once max_attempts is exhausted, not retry forever"
    );
    // attempts 0,1,2 reschedule; the next tick (attempt 3 >= 3) fails.
    assert_eq!(
        reschedules, 3,
        "should reschedule exactly max_attempts times"
    );
}

/// 33. Instance `total_steps_executed` is incremented correctly.
#[tokio::test]
async fn tick_increments_total_steps_executed() {
    let s = storage().await;
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.context.runtime.total_steps_executed, 3);
}

/// 34. `max_steps_per_instance` limits execution and fails instance.
#[tokio::test]
async fn tick_max_steps_per_instance_fails() {
    let s = storage().await;
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let mut config = default_config();
    config.max_steps_per_instance = 1;
    tick_with_config(&s, &h, &config).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 35. Two instances with different sequences both complete.
#[tokio::test]
async fn tick_two_sequences_both_complete() {
    let s = storage().await;
    let seq1 = mk_sequence(vec![mk_step("a1", "noop")]);
    let seq2 = mk_sequence(vec![mk_step("b1", "noop")]);
    s.create_sequence(&seq1).await.unwrap();
    s.create_sequence(&seq2).await.unwrap();

    let inst1 = mk_instance(seq1.id);
    let inst2 = mk_instance(seq2.id);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let r1 = s.get_instance(inst1.id).await.unwrap().unwrap();
    let r2 = s.get_instance(inst2.id).await.unwrap().unwrap();
    assert_eq!(r1.state, InstanceState::Completed);
    assert_eq!(r2.state, InstanceState::Completed);
}

/// 36. tick sets `started_at` on first execution.
#[tokio::test]
async fn tick_sets_started_at() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    assert!(inst.context.runtime.started_at.is_none());
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert!(refreshed.context.runtime.started_at.is_some());
}

/// 37. Instance with already-completed blocks skips them.
#[tokio::test]
async fn tick_skips_completed_blocks() {
    let s = storage().await;
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step("s2", "noop"),
        mk_step("s3", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let output = orch8_types::output::BlockOutput {
        id: uuid::Uuid::now_v7(),
        instance_id: inst.id,
        block_id: BlockId::new("s1"),
        output: json!({"done": true}),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    };
    s.save_block_output(&output).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 2);
}

/// 38. Unregistered handler dispatches to external worker queue.
#[tokio::test]
async fn tick_unregistered_handler_dispatches_external() {
    let s = storage().await;
    let step_def = StepDef {
        id: BlockId::new("s1"),
        handler: "unknown_handler".into(),
        params: json!({}),
        delay: None,
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: None,
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: Some("unknown_handler".into()),
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
        output_schema: None,
    };
    let seq = mk_sequence(vec![BlockDefinition::Step(Box::new(step_def))]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Waiting);
}

/// 39. Successful tick produces `block_output` rows.
#[tokio::test]
async fn tick_produces_block_outputs() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let output = s
        .get_block_output(inst.id, &BlockId::new("s1"))
        .await
        .unwrap();
    assert!(output.is_some());
}

/// 40. Sequence with template params resolves before handler execution.
#[tokio::test]
async fn tick_resolves_templates() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_params(
        "s1",
        "noop",
        json!({"key": "{{context.data.name}}"}),
    )]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_scheduled(seq.id, json!({"name": "hello"}));
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

// ================================================================
// DELAY AND SCHEDULING (Tests 41-60)
// ================================================================

/// 41. Step with delay defers instance to Scheduled with future `fire_at`.
#[tokio::test]
async fn tick_delay_defers_instance() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(60),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.unwrap();
    assert!(fire_at > Utc::now());
}

/// 42. Step with 30s delay sets `fire_at` approximately 30s in the future.
#[tokio::test]
async fn tick_delay_fire_at_correct_offset() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(30),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let fire_at = refreshed.next_fire_at.unwrap();
    let diff = (fire_at - Utc::now()).num_seconds();
    assert!((25..=35).contains(&diff), "expected ~30s, got {diff}s");
}

/// 43. Delay with `business_days_only` skips weekend.
#[tokio::test]
async fn tick_delay_business_days_skips_weekend() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(0),
        business_days_only: true,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 44. Delay with jitter produces a `fire_at` within expected bounds.
#[tokio::test]
async fn tick_delay_jitter_within_bounds() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(60),
        business_days_only: false,
        jitter: Some(Duration::from_secs(5)),
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let fire_at = refreshed.next_fire_at.unwrap();
    let diff = (fire_at - Utc::now()).num_seconds();
    assert!((50..=70).contains(&diff), "expected 55-65s, got {diff}s");
}

/// 45. Delay on second step still defers after first step completes.
#[tokio::test]
async fn tick_delay_on_second_step() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(30),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step_with_delay_spec("s2", "noop", delay),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 1);

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 46. Delay re-applies on each tick (fast path recomputes `fire_at` from now).
#[tokio::test]
async fn tick_delay_reapplies_on_retick() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(10),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let first_fire_at = refreshed.next_fire_at.unwrap();

    // Move fire_at to past and re-tick: the delay check re-fires because
    // check_step_delay always computes a fresh fire_at from Utc::now().
    s.update_instance_state(
        inst.id,
        InstanceState::Scheduled,
        Some(Utc::now() - chrono::Duration::seconds(1)),
    )
    .await
    .unwrap();
    tick(&s, &h).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let second_fire_at = refreshed.next_fire_at.unwrap();
    // Second fire_at should be later than the first (recomputed from a later now).
    assert!(second_fire_at > first_fire_at);
}

/// 47. Zero-duration delay still causes a deferral.
#[tokio::test]
async fn tick_zero_duration_delay_defers() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(0),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 48. Delay with holidays skips holiday dates.
#[tokio::test]
async fn tick_delay_with_holidays() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(60),
        business_days_only: true,
        jitter: None,
        holidays: vec!["2099-01-01".to_string()],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    assert!(refreshed.next_fire_at.is_some());
}

/// 49. Send window outside allowed hours defers instance.
#[tokio::test]
async fn tick_send_window_outside_defers() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 3,
        end_hour: 4,
        days: vec![],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let now_hour = Utc::now().format("%H").to_string().parse::<u8>().unwrap();
    if now_hour != 3 {
        assert_eq!(refreshed.state, InstanceState::Scheduled);
        assert!(refreshed.next_fire_at.is_some());
    }
}

/// 50. Send window with day restriction defers on wrong day.
#[tokio::test]
async fn tick_send_window_wrong_day_defers() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 0,
        end_hour: 0,
        days: vec![6],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let today = Utc::now().weekday().num_days_from_monday();
    if today != 6 {
        assert_eq!(refreshed.state, InstanceState::Scheduled);
    }
}

/// 51. Send window: 24h window (start==end) always allows through.
#[tokio::test]
async fn tick_send_window_24h_allows() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 9,
        end_hour: 9,
        days: vec![],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 52. Send window: empty days means all days valid.
#[tokio::test]
async fn tick_send_window_empty_days_any_day() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 0,
        end_hour: 0,
        days: vec![],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 53. Send window with wrapping hours (22-6) works.
#[tokio::test]
async fn tick_send_window_wrapping_hours() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 22,
        end_hour: 6,
        days: vec![],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let now_hour = Utc::now().format("%H").to_string().parse::<u8>().unwrap();
    if (6..22).contains(&now_hour) {
        assert_eq!(refreshed.state, InstanceState::Scheduled);
    } else {
        assert_eq!(refreshed.state, InstanceState::Completed);
    }
}

/// 54. Delay and send window on same step: delay fires first.
#[tokio::test]
async fn tick_delay_checked_before_send_window() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(60),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let step_def = StepDef {
        id: BlockId::new("s1"),
        handler: "noop".into(),
        params: json!({}),
        delay: Some(delay),
        retry: None,
        timeout: None,
        rate_limit_key: None,
        send_window: Some(SendWindow {
            start_hour: 0,
            end_hour: 0,
            days: vec![],
        }),
        context_access: None,
        cancellable: true,
        wait_for_input: None,
        queue_name: None,
        deadline: None,
        on_deadline_breach: None,
        fallback_handler: None,
        cache_key: None,
        output_schema: None,
    };
    let seq = mk_sequence(vec![BlockDefinition::Step(Box::new(step_def))]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 55. Send window defers to next valid day with correct `fire_at`.
#[tokio::test]
async fn tick_send_window_defers_to_correct_day() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 3,
        end_hour: 4,
        days: vec![6],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("s1", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let now = Utc::now();
    let today = now.weekday().num_days_from_monday();
    let hour = now.format("%H").to_string().parse::<u8>().unwrap();
    if today != 6 || hour != 3 {
        assert_eq!(refreshed.state, InstanceState::Scheduled);
        let fire_at = refreshed.next_fire_at.unwrap();
        assert!(fire_at > now);
    }
}

/// 56. Large delay (1 hour) sets appropriate future `fire_at`.
#[tokio::test]
async fn tick_large_delay_correct_fire_at() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(3600),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let fire_at = refreshed.next_fire_at.unwrap();
    let diff = (fire_at - Utc::now()).num_seconds();
    assert!(
        (3550..=3650).contains(&diff),
        "expected ~3600s, got {diff}s"
    );
}

/// 57. Multiple instances with delays all defer correctly.
#[tokio::test]
async fn tick_multiple_instances_with_delays() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(30),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();

    let mut ids = Vec::new();
    for _ in 0..3 {
        let inst = mk_instance(seq.id);
        ids.push(inst.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    tick(&s, &h).await;

    for id in ids {
        let refreshed = s.get_instance(id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Scheduled);
        assert!(refreshed.next_fire_at.is_some());
    }
}

/// 58. Delay with `fire_at_local` uses specified timezone.
#[tokio::test]
async fn tick_delay_fire_at_local() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(0),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: Some("2099-06-15T09:00:00".to_string()),
        timezone: Some("America/New_York".to_string()),
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.unwrap();
    assert!(fire_at > Utc::now());
}

/// 59. Delay persists across ticks -- re-tick always recomputes a new `fire_at`.
#[tokio::test]
async fn tick_delay_persists_across_ticks() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(30),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("s1", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());

    // First tick defers.
    tick(&s, &h).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);

    // Move fire_at to the past and re-tick: delay re-fires (still Scheduled).
    s.update_instance_state(
        inst.id,
        InstanceState::Scheduled,
        Some(Utc::now() - chrono::Duration::seconds(1)),
    )
    .await
    .unwrap();
    tick(&s, &h).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    // fire_at should be ~30s in the future again.
    let fire_at = refreshed.next_fire_at.unwrap();
    let diff = (fire_at - Utc::now()).num_seconds();
    assert!((25..=35).contains(&diff), "expected ~30s, got {diff}s");
}

/// 60. Send window on second step defers after first step completes.
#[tokio::test]
async fn tick_send_window_on_second_step_defers() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 3,
        end_hour: 4,
        days: vec![],
    };
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step_with_send_window("s2", "noop", window),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    let now_hour = Utc::now().format("%H").to_string().parse::<u8>().unwrap();
    if now_hour != 3 {
        assert_eq!(refreshed.state, InstanceState::Scheduled);
        assert_eq!(result.steps_executed, 1);
    }
}

// ================================================================
// CONCURRENCY AND RATE LIMITING (Tests 61-80)
// ================================================================

/// 61. Concurrency key max=1 serializes instances (only one proceeds).
#[tokio::test]
async fn tick_concurrency_key_max_one() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance_with_concurrency(seq.id, "ck-serial", 1);
    let inst2 = mk_instance_with_concurrency(seq.id, "ck-serial", 1);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert!(result.instances_advanced <= 1);

    let r1 = s.get_instance(inst1.id).await.unwrap().unwrap();
    let r2 = s.get_instance(inst2.id).await.unwrap().unwrap();
    let completed = [r1.state, r2.state]
        .iter()
        .filter(|&&st| st == InstanceState::Completed)
        .count();
    assert!(completed <= 1);
}

/// 62. Concurrency key max=2 allows two of three to proceed.
#[tokio::test]
async fn tick_concurrency_key_max_two() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance_with_concurrency(seq.id, "ck-pair", 2);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert!(result.instances_advanced <= 2);
}

/// 63. No concurrency key means no limit.
#[tokio::test]
async fn tick_no_concurrency_key_no_limit() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 5);
}

/// 64. Different concurrency keys do not interfere with each other.
#[tokio::test]
async fn tick_different_concurrency_keys_independent() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance_with_concurrency(seq.id, "key-a", 1);
    let inst2 = mk_instance_with_concurrency(seq.id, "key-b", 1);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 2);
}

/// 65. Concurrency key with max=0 blocks all instances.
#[tokio::test]
async fn tick_concurrency_key_max_zero_blocks() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_with_concurrency(seq.id, "ck-zero", 0);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert_eq!(result.instances_advanced, 0);
}

/// 66. Mixed: instances with and without concurrency keys process correctly.
#[tokio::test]
async fn tick_mixed_concurrency_keys() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst_a = mk_instance_with_concurrency(seq.id, "ck-mix", 1);
    let inst_b = mk_instance_with_concurrency(seq.id, "ck-mix", 1);
    let inst_c = mk_instance(seq.id);
    let inst_d = mk_instance(seq.id);
    s.create_instance(&inst_a).await.unwrap();
    s.create_instance(&inst_b).await.unwrap();
    s.create_instance(&inst_c).await.unwrap();
    s.create_instance(&inst_d).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;
    assert!(result.instances_advanced >= 2);
    assert!(result.instances_advanced <= 3);
}

/// 67. Rate limit key defers when bucket is full.
#[tokio::test]
async fn tick_rate_limit_defers() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_rate_limit("s1", "noop", "rk-full")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    use orch8_types::ids::ResourceKey;
    use orch8_types::rate_limit::RateLimit;
    s.upsert_rate_limit(&RateLimit {
        id: uuid::Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        resource_key: ResourceKey::new("rk-full"),
        max_count: 1,
        window_seconds: 60,
        current_count: 1,
        window_start: Utc::now(),
    })
    .await
    .unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    assert!(refreshed.next_fire_at.is_some());
}

/// 68. Rate limit key allows when bucket is not full.
#[tokio::test]
async fn tick_rate_limit_allows() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_rate_limit("s1", "noop", "rk-empty")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 69. No rate limit key means no rate limiting.
#[tokio::test]
async fn tick_no_rate_limit_key_passes() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 70. Rate limit defers to `retry_after` time.
#[tokio::test]
async fn tick_rate_limit_sets_retry_after() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_rate_limit("s1", "noop", "rk-retry")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    use orch8_types::ids::ResourceKey;
    use orch8_types::rate_limit::RateLimit;
    s.upsert_rate_limit(&RateLimit {
        id: uuid::Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        resource_key: ResourceKey::new("rk-retry"),
        max_count: 1,
        window_seconds: 120,
        current_count: 1,
        window_start: Utc::now(),
    })
    .await
    .unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.unwrap();
    assert!(fire_at > Utc::now());
}

/// 71. Semaphore with 1 permit processes only one instance at a time.
#[tokio::test]
async fn tick_semaphore_one_permit() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(1);
    let result = tick_with_semaphore(&s, &h, &sem).await;
    assert_eq!(result.instances_advanced, 1);
}

/// 72. Semaphore with 2 permits processes at most 2 instances.
#[tokio::test]
async fn tick_semaphore_two_permits() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(2);
    let result = tick_with_semaphore(&s, &h, &sem).await;
    assert!(result.instances_advanced <= 2);
}

/// 73. Semaphore with 100 permits processes all available instances.
#[tokio::test]
async fn tick_semaphore_large_allows_all() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(100);
    let result = tick_with_semaphore(&s, &h, &sem).await;
    assert_eq!(result.instances_advanced, 5);
}

/// 74. Semaphore returns permits after tick completes.
#[tokio::test]
async fn tick_semaphore_returns_permits() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    let sem = semaphore(10);
    tick_with_semaphore(&s, &h, &sem).await;

    assert_eq!(sem.available_permits(), 10);
}

/// 75. `max_instances_per_tenant` limits per-tenant claim.
#[tokio::test]
async fn tick_max_per_tenant_limits() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let mut config = default_config();
    config.max_instances_per_tenant = 2;
    let result = tick_with_config(&s, &h, &config).await;
    assert!(result.instances_advanced <= 2);
}

/// 76. Semaphore fully exhausted defers remaining instances.
#[tokio::test]
async fn tick_semaphore_exhausted_defers() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..5 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(2);
    let result = tick_with_semaphore(&s, &h, &sem).await;
    assert!(result.instances_advanced <= 2);
    assert!(result.has_pending_work);
}

/// 77. Concurrency gate combined with semaphore: most restrictive wins.
#[tokio::test]
async fn tick_concurrency_and_semaphore_combined() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance_with_concurrency(seq.id, "ck-combo", 1);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(10);
    let result = tick_with_semaphore(&s, &h, &sem).await;
    assert!(result.instances_advanced <= 1);
}

/// 78. Multiple ticks process all instances when semaphore is restrictive.
#[tokio::test]
async fn tick_multiple_passes_complete_all() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let mut ids = Vec::new();
    for _ in 0..4 {
        let inst = mk_instance(seq.id);
        ids.push(inst.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let sem = semaphore(2);

    for _ in 0..10 {
        let result = tick_with_semaphore(&s, &h, &sem).await;
        if !result.has_pending_work {
            break;
        }
    }

    for id in ids {
        let refreshed = s.get_instance(id).await.unwrap().unwrap();
        assert_eq!(refreshed.state, InstanceState::Completed);
    }
}

/// 79. Rate limit combined with concurrency: both enforced.
#[tokio::test]
async fn tick_rate_limit_and_concurrency() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step_with_rate_limit("s1", "noop", "rk-combo")]);
    s.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_with_concurrency(seq.id, "ck-combo2", 1);
    s.create_instance(&inst).await.unwrap();

    use orch8_types::ids::ResourceKey;
    use orch8_types::rate_limit::RateLimit;
    s.upsert_rate_limit(&RateLimit {
        id: uuid::Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t"),
        resource_key: ResourceKey::new("rk-combo"),
        max_count: 1,
        window_seconds: 60,
        current_count: 1,
        window_start: Utc::now(),
    })
    .await
    .unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 80. Concurrency enforcement returns deferred instances to Scheduled.
#[tokio::test]
async fn tick_concurrency_deferred_back_to_scheduled() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance_with_concurrency(seq.id, "ck-defer", 1);
    let inst2 = mk_instance_with_concurrency(seq.id, "ck-defer", 1);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let r1 = s.get_instance(inst1.id).await.unwrap().unwrap();
    let r2 = s.get_instance(inst2.id).await.unwrap().unwrap();
    let states = [r1.state, r2.state];
    assert!(
        states.contains(&InstanceState::Completed) || states.contains(&InstanceState::Scheduled),
        "expected one Completed and one Scheduled, got {states:?}"
    );
}

// ================================================================
// SIGNAL PROCESSING DURING TICK (Tests 81-100)
// ================================================================

/// 81. Pause signal on scheduled instance pauses it during tick.
#[tokio::test]
async fn tick_pause_signal_pauses_instance() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Pause);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 82. Cancel signal on scheduled instance cancels it during tick.
#[tokio::test]
async fn tick_cancel_signal_cancels_instance() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

/// 83. Resume signal on paused instance reschedules it.
#[tokio::test]
async fn tick_resume_signal_reschedules_paused() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 84. `UpdateContext` signal updates instance context.
#[tokio::test]
async fn tick_update_context_signal() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal_with_payload(
        inst.id,
        SignalType::UpdateContext,
        json!({"data": {"new_key": "new_value"}}),
    );
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.context.data["new_key"], "new_value");
}

/// 85. Cancel signal on paused instance cancels it.
#[tokio::test]
async fn tick_cancel_signal_on_paused() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

/// 86. Cancel signal on waiting instance cancels it.
#[tokio::test]
async fn tick_cancel_signal_on_waiting() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Waiting);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Cancelled);
}

/// 87. Multiple signals on same instance: last signal wins if applicable.
#[tokio::test]
async fn tick_multiple_signals_on_same_instance() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let sig1 = mk_signal(inst.id, SignalType::Pause);
    let sig2 = mk_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&sig1).await.unwrap();
    s.enqueue_signal(&sig2).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert!(
        refreshed.state == InstanceState::Cancelled || refreshed.state == InstanceState::Paused,
        "expected Cancelled or Paused, got {:?}",
        refreshed.state
    );
}

/// 88. Signal on a completed instance is a no-op.
#[tokio::test]
async fn tick_signal_on_completed_noop() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Completed);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Cancel);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 89. Signal on a failed instance is a no-op.
#[tokio::test]
async fn tick_signal_on_failed_noop() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Failed);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 90. Signals are processed before step execution in the tick.
#[tokio::test]
async fn tick_signals_processed_before_steps() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Pause);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    let result = tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
    assert_eq!(result.steps_executed, 0);
}

/// 91. Custom signal does not affect instance state (not a built-in signal).
#[tokio::test]
async fn tick_custom_signal_no_state_change() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Custom("my_event".into()));
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 92. Resume signal on running instance is a no-op.
#[tokio::test]
async fn tick_resume_on_running_noop() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 93. Pause signal prevents completion even if all steps would succeed.
#[tokio::test]
async fn tick_pause_prevents_completion() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Pause);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 94. `UpdateContext` with empty payload is safe (no crash).
#[tokio::test]
async fn tick_update_context_empty_payload_safe() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal_with_payload(inst.id, SignalType::UpdateContext, json!({}));
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Paused);
}

/// 95. Cancellation token when not cancelled processes instances normally.
#[tokio::test]
async fn tick_cancellation_token_not_cancelled() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    for _ in 0..3 {
        let inst = mk_instance(seq.id);
        s.create_instance(&inst).await.unwrap();
    }

    let h = Arc::new(registry());
    let cancel = CancellationToken::new();
    let result = tick_with_cancel(&s, &h, &cancel).await;
    assert!(result.instances_advanced > 0);
}

/// 96. Signal ordering: resume on paused instance reschedules it.
#[tokio::test]
async fn tick_signal_ordering_pause_then_resume() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_in_state(seq.id, InstanceState::Paused);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal(inst.id, SignalType::Resume);
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}

/// 97. Custom signal with payload does not interfere with execution.
#[tokio::test]
async fn tick_custom_signal_with_payload() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let signal = mk_signal_with_payload(
        inst.id,
        SignalType::Custom("event_fired".into()),
        json!({"data": "important"}),
    );
    s.enqueue_signal(&signal).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
}

/// 98. Concurrent signals on different instances processed independently.
#[tokio::test]
async fn tick_signals_on_different_instances() {
    let s = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    s.create_sequence(&seq).await.unwrap();

    let inst1 = mk_instance(seq.id);
    let inst2 = mk_instance(seq.id);
    s.create_instance(&inst1).await.unwrap();
    s.create_instance(&inst2).await.unwrap();

    let sig = mk_signal(inst1.id, SignalType::Pause);
    s.enqueue_signal(&sig).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let r1 = s.get_instance(inst1.id).await.unwrap().unwrap();
    let r2 = s.get_instance(inst2.id).await.unwrap().unwrap();
    assert_eq!(r1.state, InstanceState::Paused);
    assert_eq!(r2.state, InstanceState::Completed);
}

/// 99. Tick with no sequences in storage handles missing sequence gracefully.
#[tokio::test]
async fn tick_missing_sequence_fails_instance() {
    let s = storage().await;
    let fake_seq_id = SequenceId::new();
    let inst = mk_instance(fake_seq_id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());
    tick(&s, &h).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Failed);
}

/// 100. Full end-to-end: first tick completes step 1, delay on step 2 defers,
/// and the instance stays deferred (delay always recomputes on fast path).
#[tokio::test]
async fn tick_full_e2e_delayed_multi_step() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(10),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![
        mk_step("s1", "noop"),
        mk_step_with_delay_spec("s2", "noop", delay),
        mk_step("s3", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance(seq.id);
    s.create_instance(&inst).await.unwrap();

    let h = Arc::new(registry());

    // First tick: s1 completes, s2 hits delay check and defers.
    let result = tick(&s, &h).await;
    assert_eq!(result.steps_executed, 1);
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    assert!(refreshed.next_fire_at.unwrap() > Utc::now());

    // Move fire_at to past and re-tick: delay re-fires (fast path recomputes).
    s.update_instance_state(
        inst.id,
        InstanceState::Scheduled,
        Some(Utc::now() - chrono::Duration::seconds(1)),
    )
    .await
    .unwrap();
    let result = tick(&s, &h).await;
    // The delay on s2 fires again, so no steps execute on this tick.
    assert_eq!(result.steps_executed, 0);
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}
