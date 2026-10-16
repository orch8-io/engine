//! Self-tests of the `TestEngine` harness.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use serde_json::json;

use orch8_storage::testing::fixtures::{SequenceBuilder, step};
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::DelaySpec;

use super::{TestEngine, wait_until};

fn three_day_delay() -> DelaySpec {
    DelaySpec {
        duration: Duration::from_secs(3 * 24 * 3600),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    }
}

#[tokio::test]
async fn drive_runs_every_step_through_the_scheduler_to_completion() {
    let engine = TestEngine::builder().build().await;
    let seq = SequenceBuilder::new("t")
        .step("a", "noop")
        .step("b", "noop")
        .build();
    let id = engine.start(&seq).await;

    let done = engine.drive(id).await;

    assert_eq!(done.state, InstanceState::Completed);
    let outputs = engine.storage.get_all_outputs(id).await.unwrap();
    let blocks: Vec<_> = outputs.iter().map(|o| o.block_id.as_str()).collect();
    assert!(blocks.contains(&"a") && blocks.contains(&"b"), "{blocks:?}");
}

#[tokio::test]
async fn drive_jumps_the_virtual_clock_over_a_delay() {
    let engine = TestEngine::builder().build().await;
    let mut delayed = step("wait", "noop");
    delayed.delay = Some(three_day_delay());
    let seq = SequenceBuilder::new("t")
        .step_def(delayed)
        .step("after", "noop")
        .build();
    let id = engine.start(&seq).await;
    let started = engine.now();

    let done = engine.drive(id).await;

    assert_eq!(done.state, InstanceState::Completed);
    assert!(engine.now() - started >= chrono::Duration::days(3));
}

#[tokio::test]
async fn drive_stops_at_waiting_for_human_input() {
    let engine = TestEngine::builder().build().await;
    let mut gate = step("approve", "noop");
    gate.wait_for_input = Some(serde_json::from_value(json!({"prompt": "ok?"})).unwrap());
    let seq = SequenceBuilder::new("t").step_def(gate).build();
    let id = engine.start(&seq).await;

    let parked = engine.drive(id).await;

    assert_eq!(parked.state, InstanceState::Waiting);
}

#[tokio::test]
#[should_panic(expected = "did not settle within 0 ticks")]
async fn drive_panics_with_the_last_state_when_the_tick_budget_runs_out() {
    let engine = TestEngine::builder().build().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();
    let id = engine.start(&seq).await;

    engine.drive_for(id, 0).await;
}

#[tokio::test]
async fn registered_handler_receives_the_step_params() {
    let seen = Arc::new(AtomicU32::new(0));
    let probe = Arc::clone(&seen);
    let engine = TestEngine::builder()
        .register("count", move |ctx| {
            let probe = Arc::clone(&probe);
            async move {
                let n = ctx.params["n"].as_u64().unwrap_or(0);
                probe.fetch_add(u32::try_from(n).unwrap(), Ordering::SeqCst);
                Ok(json!({}))
            }
        })
        .build()
        .await;
    let mut s = step("a", "count");
    s.params = json!({"n": 7});
    let id = engine
        .start(&SequenceBuilder::new("t").step_def(s).build())
        .await;

    engine.drive(id).await;

    assert_eq!(seen.load(Ordering::SeqCst), 7);
}

#[tokio::test]
async fn spawned_engine_runs_the_real_loop_and_stops_cleanly() {
    let engine = TestEngine::builder().build().await;
    let seq = SequenceBuilder::new("t").step("a", "noop").build();
    let id = engine.start(&seq).await;
    let running = engine.spawn();

    let storage = Arc::clone(&running.storage);
    let state = wait_until("instance completes", Duration::from_secs(10), || {
        let storage = Arc::clone(&storage);
        async move {
            let inst = storage.get_instance(id).await.unwrap()?;
            inst.state.is_terminal().then_some(inst.state)
        }
    })
    .await;

    assert_eq!(state, InstanceState::Completed);
    running
        .stop()
        .await
        .expect("engine exits cleanly on cancel");
    assert!(
        storage
            .get_block_output(id, &BlockId::new("a"))
            .await
            .unwrap()
            .is_some()
    );
}
