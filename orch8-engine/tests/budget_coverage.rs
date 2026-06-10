#![allow(clippy::items_after_statements)]
//! E2E tests for budget-governed instances: token/step budgets enforced by
//! the scheduler with pause-on-breach.
//!
//! Running only this file:
//!
//! ```text
//! cargo test -p orch8-engine --test budget_coverage
//! ```

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, SequenceId};
use orch8_types::instance::{Budget, InstanceState, TaskInstance};

mod common;
use common::*;

async fn tick(storage: &Arc<dyn StorageBackend>, handlers: &Arc<HandlerRegistry>) {
    let sem = semaphore(128);
    let config = default_config();
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(storage, handlers, &sem, &config, &seq_cache, &cancel)
        .await
        .unwrap();
}

fn mk_budgeted_instance(seq_id: SequenceId, budget: Budget) -> TaskInstance {
    let mut inst = mk_instance_scheduled(seq_id, json!({}));
    inst.budget = Some(budget);
    inst
}

fn mk_usage_event(instance_id: InstanceId, input: i64, output: i64) -> orch8_storage::UsageEvent {
    orch8_storage::UsageEvent {
        tenant_id: "t".into(),
        instance_id: Some(instance_id),
        block_id: Some("s1".into()),
        kind: "llm_tokens".into(),
        model: "test-model".into(),
        input_tokens: input,
        output_tokens: output,
        created_at: Utc::now(),
    }
}

async fn fetch(storage: &Arc<dyn StorageBackend>, id: InstanceId) -> TaskInstance {
    storage.get_instance(id).await.unwrap().unwrap()
}

/// Token budget exceeded before the tick: the scheduler pauses the instance
/// with a machine-readable breach reason instead of executing steps.
#[tokio::test]
async fn budget_total_tokens_exceeded_pauses_instance_with_reason() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_total_tokens: Some(100),
            ..Budget::default()
        },
    );
    storage.create_instance(&inst).await.unwrap();

    // Recorded usage already exceeds the budget (60 + 50 = 110 > 100).
    storage
        .record_usage_event(&mk_usage_event(inst.id, 60, 50))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Paused);
    assert_eq!(fetched.metadata["paused_reason"], "budget_exceeded");
    assert_eq!(
        fetched.metadata["budget_breach"]["limit"],
        "max_total_tokens"
    );
    assert_eq!(fetched.metadata["budget_breach"]["limit_value"], 100);
    assert_eq!(fetched.metadata["budget_breach"]["actual"], 110);

    // No step output: the pause happened before any work this tick.
    let outputs = storage.get_all_outputs(inst.id).await.unwrap();
    assert!(outputs.is_empty(), "no steps should have executed");
}

/// Input-token limit breach reports the specific limit that tripped.
#[tokio::test]
async fn budget_input_tokens_exceeded_reports_input_limit() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_input_tokens: Some(10),
            ..Budget::default()
        },
    );
    storage.create_instance(&inst).await.unwrap();
    storage
        .record_usage_event(&mk_usage_event(inst.id, 11, 0))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Paused);
    assert_eq!(
        fetched.metadata["budget_breach"]["limit"],
        "max_input_tokens"
    );
}

/// An instance with a budget that is NOT exceeded proceeds normally.
#[tokio::test]
async fn budget_within_limits_instance_completes_normally() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_total_tokens: Some(1_000),
            max_steps: Some(100),
            ..Budget::default()
        },
    );
    storage.create_instance(&inst).await.unwrap();
    storage
        .record_usage_event(&mk_usage_event(inst.id, 10, 10))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Completed);
    assert!(fetched.metadata.get("paused_reason").is_none());
}

/// An instance without a budget is unaffected by recorded usage.
#[tokio::test]
async fn no_budget_instance_unaffected_by_heavy_usage() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_instance_scheduled(seq.id, json!({}));
    storage.create_instance(&inst).await.unwrap();
    storage
        .record_usage_event(&mk_usage_event(inst.id, 1_000_000, 1_000_000))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Completed);
    assert!(fetched.metadata.get("paused_reason").is_none());
}

/// Step-count budget breach (no token limits configured — zero usage queries).
#[tokio::test]
async fn budget_max_steps_exceeded_pauses_instance() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop"), mk_step("s2", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let mut inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_steps: Some(3),
            ..Budget::default()
        },
    );
    // Simulate prior ticks having executed 4 steps already.
    inst.context.runtime.total_steps_executed = 4;
    storage.create_instance(&inst).await.unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Paused);
    assert_eq!(fetched.metadata["paused_reason"], "budget_exceeded");
    assert_eq!(fetched.metadata["budget_breach"]["limit"], "max_steps");
    assert_eq!(fetched.metadata["budget_breach"]["limit_value"], 3);
    assert_eq!(fetched.metadata["budget_breach"]["actual"], 4);
}

/// Breach metadata merges into existing metadata instead of clobbering it.
#[tokio::test]
async fn budget_breach_preserves_existing_metadata() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let mut inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_output_tokens: Some(5),
            ..Budget::default()
        },
    );
    inst.metadata = json!({"owner": "alice"});
    storage.create_instance(&inst).await.unwrap();
    storage
        .record_usage_event(&mk_usage_event(inst.id, 0, 6))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;

    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Paused);
    assert_eq!(fetched.metadata["owner"], "alice");
    assert_eq!(fetched.metadata["paused_reason"], "budget_exceeded");
    assert_eq!(
        fetched.metadata["budget_breach"]["limit"],
        "max_output_tokens"
    );
}

/// Resume semantics: a Paused -> Scheduled transition lets the instance run
/// one more tick; if the budget is still exceeded it pauses again (no
/// infinite loop — Paused instances leave the claim set).
#[tokio::test]
async fn resumed_instance_pauses_again_when_budget_still_exceeded() {
    let storage = storage().await;
    let seq = mk_sequence(vec![mk_step("s1", "noop")]);
    storage.create_sequence(&seq).await.unwrap();

    let inst = mk_budgeted_instance(
        seq.id,
        Budget {
            max_total_tokens: Some(10),
            ..Budget::default()
        },
    );
    storage.create_instance(&inst).await.unwrap();
    storage
        .record_usage_event(&mk_usage_event(inst.id, 20, 0))
        .await
        .unwrap();

    let handlers = Arc::new(registry());
    tick(&storage, &handlers).await;
    assert_eq!(fetch(&storage, inst.id).await.state, InstanceState::Paused);

    // Resume via the existing Paused -> Scheduled transition.
    storage
        .update_instance_state(inst.id, InstanceState::Scheduled, Some(Utc::now()))
        .await
        .unwrap();

    // The budget is still exceeded — the next tick pauses again.
    tick(&storage, &handlers).await;
    let fetched = fetch(&storage, inst.id).await;
    assert_eq!(fetched.state, InstanceState::Paused);
    assert_eq!(fetched.metadata["paused_reason"], "budget_exceeded");

    // A further tick must not touch the paused instance (it is out of the
    // claim set), so this is not an infinite pause/claim loop.
    tick(&storage, &handlers).await;
    assert_eq!(fetch(&storage, inst.id).await.state, InstanceState::Paused);
}
