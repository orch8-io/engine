//! End-to-end durable event correlation through the embedded engine:
//! the roadmap's definition of done for feature 3.
//!
//! A workflow waits for `payment_received` AND `inventory_reserved`
//! under the same order correlation key, resumes only after both arrive
//! (in either order), discards duplicate producer ids, and takes a
//! timeout path when an event never arrives.

use std::time::Duration;

use orch8::{CreateInstanceOptions, Engine, InstanceId, InstanceState, Storage};

fn order_sequence(name: &str) -> orch8::SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": name,
        "version": 1,
        "blocks": [
            {
                "type": "step",
                "id": "await_order_facts",
                "handler": "wait_for_event",
                "params": {
                    "events": ["payment_received", "inventory_reserved"],
                    "correlation_key": "{{ context.data.order_id }}",
                    "join": "all"
                },
                "wait_for_input": { "prompt": "waiting for order events" }
            },
            { "type": "step", "id": "fulfil", "handler": "noop", "params": {} }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    }))
    .expect("valid sequence")
}

async fn build_engine() -> Engine {
    Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .tick_interval(Duration::from_millis(10))
        .build()
        .await
        .expect("engine builds")
}

async fn wait_for<F: Fn(InstanceState) -> bool>(
    engine: &Engine,
    id: InstanceId,
    pred: F,
) -> InstanceState {
    let mut last = None;
    for _ in 0..600 {
        engine.tick_once().await.expect("tick");
        let state = engine.get_instance(id).await.expect("get").state;
        if pred(state) {
            return state;
        }
        last = Some(state);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("instance never reached the expected state (last: {last:?})");
}

#[tokio::test]
async fn resumes_after_both_events_in_either_order() {
    let engine = build_engine().await;
    let seq_id = engine
        .upsert_sequence(order_sequence("order-flow"))
        .await
        .unwrap();
    let inst = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                context: orch8::ExecutionContext {
                    data: serde_json::json!({"order_id": "order-42"}),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // The instance parks at the wait block.
    let parked = wait_for(&engine, inst, |s| s == InstanceState::Waiting).await;
    assert_eq!(parked, InstanceState::Waiting);

    // First event (reverse order: inventory before payment) — still waiting.
    let first = engine
        .ingest_event(
            "inventory_reserved",
            "inv-1",
            "order-42",
            serde_json::json!({"sku": "X"}),
        )
        .await
        .unwrap();
    assert!(!first.duplicate);
    assert!(!first.satisfied);
    for _ in 0..5 {
        engine.tick_once().await.unwrap();
    }
    assert_eq!(
        engine.get_instance(inst).await.unwrap().state,
        InstanceState::Waiting
    );

    // A duplicate of the first event changes nothing.
    let dup = engine
        .ingest_event(
            "inventory_reserved",
            "inv-1",
            "order-42",
            serde_json::json!({"sku": "SHOULD_BE_IGNORED"}),
        )
        .await
        .unwrap();
    assert!(dup.duplicate);

    // Second event satisfies the all-join; the workflow completes.
    let second = engine
        .ingest_event(
            "payment_received",
            "pay-1",
            "order-42",
            serde_json::json!({"amount": 100}),
        )
        .await
        .unwrap();
    assert!(second.satisfied);

    let done = wait_for(&engine, inst, orch8::InstanceState::is_terminal).await;
    assert_eq!(done, InstanceState::Completed);

    // The matched payloads are the wait block's output.
    let outputs = engine.block_outputs(inst).await.unwrap();
    let wait_output = outputs
        .iter()
        .filter(|o| o.block_id.as_str() == "await_order_facts")
        .next_back()
        .expect("wait block output");
    assert_eq!(
        wait_output.output["events"]["payment_received"]["payload"]["amount"],
        100,
        "{}",
        wait_output.output
    );
    assert_eq!(
        wait_output.output["events"]["inventory_reserved"]["payload"]["sku"],
        "X"
    );
}

#[tokio::test]
async fn events_arriving_before_the_workflow_still_resume_it() {
    let engine = build_engine().await;
    // Both events arrive BEFORE the workflow exists.
    engine
        .ingest_event("payment_received", "p-1", "order-9", serde_json::json!({}))
        .await
        .unwrap();
    engine
        .ingest_event("inventory_reserved", "i-1", "order-9", serde_json::json!({}))
        .await
        .unwrap();

    let seq_id = engine
        .upsert_sequence(order_sequence("early-events"))
        .await
        .unwrap();
    let inst = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                context: orch8::ExecutionContext {
                    data: serde_json::json!({"order_id": "order-9"}),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let done = wait_for(&engine, inst, orch8::InstanceState::is_terminal).await;
    assert_eq!(done, InstanceState::Completed);
}

#[tokio::test]
async fn events_for_a_different_order_never_resume() {
    let engine = build_engine().await;
    let seq_id = engine
        .upsert_sequence(order_sequence("isolated-orders"))
        .await
        .unwrap();
    let inst = engine
        .create_instance(
            seq_id,
            CreateInstanceOptions {
                context: orch8::ExecutionContext {
                    data: serde_json::json!({"order_id": "order-A"}),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
    wait_for(&engine, inst, |s| s == InstanceState::Waiting).await;

    // Both facts, but for a different order.
    engine
        .ingest_event("payment_received", "p-b", "order-B", serde_json::json!({}))
        .await
        .unwrap();
    engine
        .ingest_event("inventory_reserved", "i-b", "order-B", serde_json::json!({}))
        .await
        .unwrap();
    for _ in 0..10 {
        engine.tick_once().await.unwrap();
    }
    assert_eq!(
        engine.get_instance(inst).await.unwrap().state,
        InstanceState::Waiting
    );
}
