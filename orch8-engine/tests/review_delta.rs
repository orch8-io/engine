//! Regression tests for the delta code review (D-04, D-06) through the real
//! scheduler.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio_util::sync::CancellationToken;

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::liveness::EngineLiveness;
use orch8_engine::scheduler::run_tick_loop_with_liveness;
use orch8_engine::testing::{TestEngine, test_scheduler_config};
use orch8_storage::testing::faulty::FaultStorage;
use orch8_storage::testing::fixtures::{SequenceBuilder, step_with_params};
use orch8_types::instance::InstanceState;

/// D-06: a persisted `sleep` step over the in-process cap is parked durably
/// for its duration (like `delay`) and then completes, instead of failing
/// permanently.
#[tokio::test]
async fn over_cap_sleep_is_a_durable_reschedule_not_a_failure() {
    let engine = TestEngine::builder().build().await;
    let seq = SequenceBuilder::new("t")
        .step_def(step_with_params(
            "nap",
            "sleep",
            json!({"duration_ms": 3_600_000_u64}),
        ))
        .step("after", "noop")
        .build();
    let id = engine.start(&seq).await;
    let started = engine.now();

    engine.tick().await;

    let inst = engine.instance(id).await;
    assert_eq!(
        inst.state,
        InstanceState::Scheduled,
        "parked, not failed: {inst:?}"
    );
    let wait = inst.next_fire_at.expect("deferred") - started;
    assert!(
        (3_599..=3_601).contains(&wait.num_seconds()),
        "parked for about an hour, got {wait}"
    );

    let wall = std::time::Instant::now();
    let done = engine.drive(id).await;
    assert_eq!(done.state, InstanceState::Completed);
    assert!(
        wall.elapsed() < Duration::from_secs(30),
        "no in-process hour-long sleep"
    );
}

/// D-04: the liveness stamp is refreshed between the claim phase and the
/// later sweeps, so one slow sweep does not age the whole iteration.
#[tokio::test]
async fn liveness_is_stamped_between_phases_of_one_iteration() {
    let inner = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let (storage, faults) = FaultStorage::wrap(inner);
    // The second iteration's signal sweep stalls for 1.2 s.
    faults
        .on("get_signalled_instance_ids")
        .nth(2)
        .delay(Duration::from_millis(1_200));

    let mut config = test_scheduler_config();
    config.tick_interval_ms = 400;
    let liveness = Arc::new(EngineLiveness::new(
        Duration::from_millis(config.tick_interval_ms),
        Duration::ZERO,
    ));
    liveness.arm();
    let cancel = CancellationToken::new();
    let handle = tokio::spawn({
        let liveness = Arc::clone(&liveness);
        let cancel = cancel.clone();
        let storage = Arc::clone(&storage);
        async move {
            run_tick_loop_with_liveness(
                storage,
                Arc::new(HandlerRegistry::new()),
                &config,
                cancel,
                Some(liveness),
            )
            .await
        }
    });

    // Iteration 1 at t=0, iteration 2 starts at t=400 ms and sits in the
    // stalled sweep until ~1600 ms. At t=1000 ms the last stamp must be the
    // post-claim one from iteration 2 (age ~600 ms), not iteration 1's
    // (age ~1000 ms).
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let age = liveness.tick_age().expect("armed");
    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        age < Duration::from_millis(850),
        "no mid-iteration stamp: tick age {age:?}"
    );
}
