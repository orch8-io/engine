//! Virtual-time e2e tests: drive `tick_once` with a `ManualClock` injected
//! via `SchedulerConfig::clock` and advance time manually, so workflows with
//! multi-day delays and send windows complete in a millisecond-scale test run.

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use tokio_util::sync::CancellationToken;

use orch8_engine::clock::{Clock, ManualClock, SharedClock};
use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::tick_once;
use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use orch8_types::ids::BlockId;
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{DelaySpec, SendWindow};

mod common;
use common::*;

/// Build a scheduler config whose scheduling decisions read the given clock.
fn config_with_clock(manual: &Arc<ManualClock>) -> SchedulerConfig {
    SchedulerConfig {
        clock: SharedClock::from_arc(Arc::clone(manual) as Arc<dyn Clock>),
        ..default_config()
    }
}

/// Run one scheduling pass under the supplied config.
async fn tick(
    storage: &Arc<dyn StorageBackend>,
    handlers: &Arc<HandlerRegistry>,
    config: &SchedulerConfig,
) -> orch8_engine::scheduler::TickOnceResult {
    let sem = semaphore(128);
    let seq_cache = cache();
    let cancel = CancellationToken::new();
    tick_once(storage, handlers, &sem, config, &seq_cache, &cancel)
        .await
        .unwrap()
}

/// The payoff test: a sequence with a 3-day delay completes in a fast test
/// run by advancing a `ManualClock` instead of sleeping.
#[tokio::test]
async fn three_day_delay_completes_with_manual_clock() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(3 * 24 * 60 * 60), // 3 days
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![
        mk_step_with_delay_spec("wait_3d", "noop", delay),
        mk_step("after_delay", "noop"),
    ]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_scheduled(seq.id, serde_json::json!({}));
    s.create_instance(&inst).await.unwrap();

    // ManualClock discipline: start at (or after) real time, move forward only.
    let start = Utc::now();
    let manual = Arc::new(ManualClock::new(start));
    let config = config_with_clock(&manual);
    let h = Arc::new(registry());

    // Tick 1: the delayed step defers the instance ~3 virtual days out.
    tick(&s, &h, &config).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let fire_at = refreshed.next_fire_at.expect("deferred fire_at");
    let diff_secs = (fire_at - start).num_seconds();
    assert!(
        (3 * 24 * 3600 - 5..=3 * 24 * 3600 + 5).contains(&diff_secs),
        "expected ~3 days deferral, got {diff_secs}s"
    );

    // Tick 2 without advancing time: nothing is due, nothing runs.
    let result = tick(&s, &h, &config).await;
    assert_eq!(result.steps_executed, 0, "instance must not be claimed yet");
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);

    // Advance 3 days (+1 minute of slack) — the delay has now been served.
    manual.advance(chrono::Duration::days(3) + chrono::Duration::minutes(1));
    tick(&s, &h, &config).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(
        refreshed.state,
        InstanceState::Completed,
        "after advancing the clock past the delay the instance must complete"
    );
    // Both blocks actually executed.
    for block in ["wait_3d", "after_delay"] {
        let output = s
            .get_block_output(inst.id, &BlockId::new(block))
            .await
            .unwrap();
        assert!(output.is_some(), "block {block} must have an output");
    }
}

/// A step outside its send window is deferred to the window's opening, and
/// advancing the clock across the boundary lets it execute.
#[tokio::test]
async fn send_window_boundary_crossed_with_manual_clock() {
    let s = storage().await;
    let window = SendWindow {
        start_hour: 9,
        end_hour: 17,
        days: vec![],
    };
    let seq = mk_sequence(vec![mk_step_with_send_window("notify", "noop", window)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_scheduled(seq.id, serde_json::json!({}));
    s.create_instance(&inst).await.unwrap();

    // A fixed future instant outside the window: 2030-01-07 (a Monday) 03:00
    // UTC. Being in the future keeps the forward-only ManualClock discipline.
    let night = Utc.with_ymd_and_hms(2030, 1, 7, 3, 0, 0).unwrap();
    let manual = Arc::new(ManualClock::new(night));
    let config = config_with_clock(&manual);
    let h = Arc::new(registry());

    // Tick 1: outside the window — deferred to 09:00 the same (virtual) day.
    tick(&s, &h, &config).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
    let next_open = refreshed.next_fire_at.expect("deferred fire_at");
    assert_eq!(
        next_open,
        Utc.with_ymd_and_hms(2030, 1, 7, 9, 0, 0).unwrap(),
        "must defer to the window opening"
    );

    // Tick 2 while still at night: not due, no execution.
    let result = tick(&s, &h, &config).await;
    assert_eq!(result.steps_executed, 0);

    // Jump into the window and tick: the step now runs to completion.
    manual.set(Utc.with_ymd_and_hms(2030, 1, 7, 9, 30, 0).unwrap());
    tick(&s, &h, &config).await;

    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Completed);
    let output = s
        .get_block_output(inst.id, &BlockId::new("notify"))
        .await
        .unwrap();
    assert!(
        output.is_some(),
        "step must have executed inside the window"
    );
}

/// With the default `SystemClock` nothing changes: a delayed instance stays
/// deferred within a fast test run (sanity check that virtual time is opt-in).
#[tokio::test]
async fn system_clock_default_keeps_delay_pending() {
    let s = storage().await;
    let delay = DelaySpec {
        duration: Duration::from_secs(3600),
        business_days_only: false,
        jitter: None,
        holidays: vec![],
        fire_at_local: None,
        timezone: None,
    };
    let seq = mk_sequence(vec![mk_step_with_delay_spec("wait_1h", "noop", delay)]);
    s.create_sequence(&seq).await.unwrap();
    let inst = mk_instance_scheduled(seq.id, serde_json::json!({}));
    s.create_instance(&inst).await.unwrap();

    let config = default_config(); // SystemClock
    let h = Arc::new(registry());

    tick(&s, &h, &config).await;
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);

    // Re-tick immediately: still pending — real time has not advanced 1h.
    let result = tick(&s, &h, &config).await;
    assert_eq!(result.steps_executed, 0);
    let refreshed = s.get_instance(inst.id).await.unwrap().unwrap();
    assert_eq!(refreshed.state, InstanceState::Scheduled);
}
