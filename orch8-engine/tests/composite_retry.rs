use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::{Duration, Utc};
use orch8_engine::{
    clock::{ManualClock, SharedClock},
    scheduler::tick_once,
    testing::TestEngine,
};
use orch8_storage::StorageBackend;
use orch8_types::{
    continuity::EffectState,
    error::StepError,
    ids::{BlockId, InstanceId},
    instance::InstanceState,
    sequence::BlockDefinition,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

mod common;
use common::*;

fn retry_policy() -> Value {
    json!({
        "max_attempts": 1,
        "initial_backoff": 1000,
        "max_backoff": 1000,
        "jitter": "none",
        "rules": [{"error_equals":["Fixture.Failure"],"max_attempts":1,
            "initial_backoff":1000,"max_backoff":1000,"jitter":"none"}]
    })
}

fn failure() -> StepError {
    StepError::Permanent {
        message: "fixture failure".into(),
        details: Some(json!({"error_code":"Fixture.Failure"})),
    }
}

async fn run(composite: Value, ambiguous: bool) -> (InstanceState, Vec<EffectState>, usize, usize) {
    let backend = storage().await;
    let block: BlockDefinition = serde_json::from_value(composite).unwrap();
    let mut sequence = mk_sequence(vec![block]);
    sequence.input_schema = Some(json!({"x-orch8-composite-retry":{"aggregate":retry_policy()}}));
    sequence.validate().unwrap();
    backend.create_sequence(&sequence).await.unwrap();
    let mut instance = mk_instance_scheduled(sequence.id, json!({"items":["good","bad"]}));
    let clock = Arc::new(ManualClock::new(Utc::now()));
    instance.next_fire_at = Some(Utc::now() - Duration::seconds(1));
    backend.create_instance(&instance).await.unwrap();

    let pure_calls = Arc::new(AtomicUsize::new(0));
    let effects = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicUsize::new(0));
    let mut handlers = registry();
    handlers.register("tool_call", {
        let effects = effects.clone();
        move |_| {
            let effects = effects.clone();
            async move {
                effects.fetch_add(1, Ordering::SeqCst);
                if ambiguous {
                    Err(failure())
                } else {
                    Ok(json!({"ok":true}))
                }
            }
        }
    });
    handlers.register("noop", {
        let pure_calls = pure_calls.clone();
        move |ctx| {
            let pure_calls = pure_calls.clone();
            let fail_once = fail_once.clone();
            async move {
                pure_calls.fetch_add(1, Ordering::SeqCst);
                if ctx.params["fail"] == true
                    && ctx.params.get("item").is_none_or(|item| item == "bad")
                    && fail_once.fetch_add(1, Ordering::SeqCst) == 0
                {
                    Err(failure())
                } else {
                    Ok(json!({"ok":true}))
                }
            }
        }
    });
    let handlers = Arc::new(handlers);

    let mut config = default_config();
    config.clock = SharedClock::from_arc(clock.clone());
    let cache = cache();
    let permits = semaphore(8);
    let cancel = CancellationToken::new();
    for _ in 0..5 {
        tick_once(&backend, &handlers, &permits, &config, &cache, &cancel)
            .await
            .unwrap();
        clock.advance(Duration::seconds(1));
    }
    let final_state = backend
        .get_instance(instance.id)
        .await
        .unwrap()
        .unwrap()
        .state;
    let receipts = backend
        .list_instance_effect_receipts(&instance.tenant_id, instance.id, 100)
        .await
        .unwrap()
        .into_iter()
        .filter(|receipt| receipt.block_id == BlockId::new("effect"))
        .map(|receipt| receipt.state)
        .collect();
    (
        final_state,
        receipts,
        pure_calls.load(Ordering::SeqCst),
        effects.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn parallel_retry_replays_successful_effect_with_new_identity() {
    let (state, receipts, _, calls) = run(
        json!({
            "type":"parallel","id":"aggregate",
            "branches":[[
                {"type":"step","id":"effect","handler":"tool_call"},
                {"type":"step","id":"after","handler":"noop","params":{"fail":true}}
            ],[{"type":"step","id":"sibling","handler":"noop"}]]
        }),
        false,
    )
    .await;
    assert_eq!(state, InstanceState::Completed);
    assert_eq!(calls, 2);
    assert_eq!(receipts, vec![EffectState::Committed; 2]);
}

#[tokio::test]
async fn map_retry_restarts_all_items_and_retains_effect_history() {
    let (state, receipts, pure_calls, effects) = run(json!({
        "type":"for_each","id":"aggregate","collection":"items","item_var":"item",
        "max_iterations":3,"body":[
            {"type":"step","id":"effect","handler":"tool_call","params":{"item":"{{data.item}}"}},
            {"type":"step","id":"after","handler":"noop","params":{"fail":true,"item":"{{data.item}}"}}
        ]
    }), false).await;
    assert_eq!(state, InstanceState::Completed);
    assert_eq!(pure_calls, 4);
    assert_eq!(effects, 4);
    assert_eq!(receipts, vec![EffectState::Committed; 4]);
}

#[tokio::test]
async fn ambiguous_branch_effect_refuses_aggregate_redispatch() {
    let (state, receipts, _, calls) = run(
        json!({
            "type":"parallel","id":"aggregate","branches":[[
                {"type":"step","id":"effect","handler":"tool_call"}
            ]]
        }),
        true,
    )
    .await;
    assert_eq!(state, InstanceState::Failed);
    assert_eq!(calls, 1);
    assert_eq!(receipts, vec![EffectState::Unknown]);
}

fn child_call() -> Value {
    json!({"type":"sub_sequence", "id":"child", "sequence_name":"child-flow",
        "input":{"item":"{{data.item}}"}, "child_result":"reference"})
}

async fn create_child_retry_parent(
    engine: &TestEngine,
    backend: &dyn StorageBackend,
    child: BlockDefinition,
    parent: Value,
) -> InstanceId {
    let mut child = mk_sequence(vec![child]);
    child.name = "child-flow".into();
    backend.create_sequence(&child).await.unwrap();

    let mut sequence = mk_sequence(vec![serde_json::from_value(parent).unwrap()]);
    let mut retry = retry_policy();
    retry["rules"][0]["error_equals"] = json!(["States.ALL"]);
    sequence.input_schema = Some(json!({"x-orch8-composite-retry":{"aggregate":retry}}));
    sequence.validate().unwrap();
    backend.create_sequence(&sequence).await.unwrap();
    let mut run =
        mk_instance_scheduled(sequence.id, json!({"items":["good","bad"],"item":"single"}));
    run.next_fire_at = Some(engine.now());
    engine.create(&run).await
}

async fn tick_to_terminal(engine: &TestEngine, id: InstanceId) -> InstanceState {
    for _ in 0..40 {
        engine.tick().await;
        let state = engine.instance(id).await.state;
        if state.is_terminal() {
            return state;
        }
        engine.advance(std::time::Duration::from_secs(1));
    }
    panic!(
        "parent {id} did not settle: {:?}",
        engine.instance(id).await.state
    );
}

async fn completed_child_retry(backend: Arc<dyn StorageBackend>, inline_map: bool) {
    let fails = Arc::new(AtomicUsize::new(0));
    let builder = || {
        let fails = fails.clone();
        TestEngine::builder()
            .storage(backend.clone())
            .register("tool_call", |_| async { Ok(json!({"completed":true})) })
            .register("noop", move |ctx| {
                let fails = fails.clone();
                async move {
                    if ctx.params["fail"] == true
                        && (!inline_map || ctx.context.data["item"] == "bad")
                        && fails.fetch_add(1, Ordering::SeqCst) == 0
                    {
                        Err(failure())
                    } else {
                        Ok(json!({}))
                    }
                }
            })
    };
    let engine = builder().build().await;
    let branch = json!([child_call(), {"type":"step","id":"sibling_failure","handler":"noop","params":{"fail":true}}]);
    let parent = if inline_map {
        json!({"type":"for_each","id":"aggregate","collection":"items","item_var":"item","max_iterations":3,"body":branch})
    } else {
        json!({"type":"parallel","id":"aggregate","branches":[branch,[{"type":"step","id":"other","handler":"noop"}]]})
    };
    let id = create_child_retry_parent(
        &engine,
        backend.as_ref(),
        mk_step("effect", "tool_call"),
        parent,
    )
    .await;
    let mut retry_persisted = false;
    for _ in 0..24 {
        engine.tick().await;
        retry_persisted = backend
            .get_all_outputs(id)
            .await
            .unwrap()
            .iter()
            .any(|entry| {
                entry.output_ref.as_deref() == Some("__retry__")
                    && entry.output["retry_at"].is_string()
            });
        if retry_persisted {
            break;
        }
        engine.advance(std::time::Duration::from_secs(1));
    }
    assert!(
        retry_persisted,
        "aggregate retry must be persisted before restart"
    );
    let checkpoint = engine.now();
    drop(engine);
    let engine = builder().start_at(checkpoint).build().await;
    assert_eq!(
        tick_to_terminal(&engine, id).await,
        InstanceState::Completed,
        "parent outputs: {:?}, children: {:?}",
        backend.get_all_outputs(id).await.unwrap(),
        backend.get_child_instances(id).await.unwrap()
    );
    let children = backend.get_child_instances(id).await.unwrap();
    assert_eq!(children.len(), if inline_map { 4 } else { 2 });
    let mut effects = std::collections::HashSet::new();
    for child in children {
        assert_eq!(child.state, InstanceState::Completed);
        let receipts = backend
            .list_instance_effect_receipts(&child.tenant_id, child.id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Committed);
        assert!(effects.insert(receipts[0].id));
    }
}

orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn parallel_retries_synchronous_children_with_fresh_effects(b) {
    completed_child_retry(b.storage.clone(), false).await;
});

orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn inline_map_retries_preceding_synchronous_children(b) {
    completed_child_retry(b.storage.clone(), true).await;
});

orch8_storage::backend_test!(kinds = [sqlite, postgres], async fn child_unknown_effect_prevents_composite_replay(b) {
    let engine = TestEngine::builder().storage(b.storage.clone())
        .register("tool_call", |_| async { Err(failure()) }).build().await;
    let id = create_child_retry_parent(&engine, b.storage.as_ref(), mk_step("effect", "tool_call"),
        json!({"type":"parallel","id":"aggregate","branches":[[child_call()]]})).await;
    assert_eq!(tick_to_terminal(&engine, id).await, InstanceState::Failed);
    let children = b.storage.get_child_instances(id).await.unwrap();
    assert_eq!(children.len(), 1);
    let effects = b.storage.list_instance_effect_receipts(&children[0].tenant_id, children[0].id, 10).await.unwrap();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].state, EffectState::Unknown);
});

#[tokio::test]
async fn parallel_cancels_a_waiting_child_before_opening_the_next_attempt() {
    use orch8_types::signal::{Signal, SignalType};

    let backend = storage().await;
    let fails = Arc::new(AtomicUsize::new(0));
    let engine = TestEngine::builder()
        .storage(backend.clone())
        .register("noop", move |ctx| {
            let fails = fails.clone();
            async move {
                if ctx.params["fail"] == true && fails.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(failure())
                } else {
                    Ok(json!({}))
                }
            }
        })
        .build()
        .await;
    let child = serde_json::from_value(json!({"type":"step","id":"approval","handler":"noop",
        "wait_for_input":{"prompt":"Approve child"}}))
    .unwrap();
    let id = create_child_retry_parent(
        &engine,
        backend.as_ref(),
        child,
        json!({"type":"parallel","id":"aggregate","branches":[[child_call()],[
            {"type":"step","id":"sibling_failure","handler":"noop","params":{"fail":true},"delay":{"duration":4000}}
        ]]}),
    )
    .await;

    let mut saw_waiting = false;
    let mut fresh = None;
    for _ in 0..20 {
        engine.tick().await;
        let children = backend.get_child_instances(id).await.unwrap();
        if let [first] = children.as_slice() {
            saw_waiting |= first.state == InstanceState::Waiting;
        } else if let [first, second] = children.as_slice() {
            assert_eq!(first.state, InstanceState::Cancelled);
            assert_ne!(first.id, second.id);
            fresh = Some(second.id);
            break;
        }
        engine.advance(std::time::Duration::from_secs(1));
    }
    assert!(saw_waiting);
    let Some(fresh) = fresh else {
        panic!(
            "fresh child must be dispatched after cancellation, parent: {:?}, outputs: {:?}, children: {:?}",
            engine.instance(id).await.state,
            backend.get_all_outputs(id).await.unwrap(),
            backend.get_child_instances(id).await.unwrap()
        );
    };
    backend
        .enqueue_signal(&Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: fresh,
            signal_type: SignalType::Custom("human_input:approval".into()),
            payload: json!({"value":"yes"}),
            delivered: false,
            created_at: engine.now(),
            delivered_at: None,
        })
        .await
        .unwrap();
    assert_eq!(
        tick_to_terminal(&engine, id).await,
        InstanceState::Completed
    );
}
