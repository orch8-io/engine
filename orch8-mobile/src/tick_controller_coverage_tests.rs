//! Tick controller coverage: interval arithmetic edges, scheduler wake
//! gating, GC deadlines, and the dirty-scan retry schedule.
//!
//! Count contract: 8 independently named unit tests.

use super::*;

#[test]
fn coverage_tick_001_effective_interval_saturates_instead_of_overflowing() {
    let huge = Duration::from_secs(u64::MAX / 2);

    let scaled = effective_tick_interval(huge, PowerState::CriticalBattery);

    assert_eq!(scaled, huge.saturating_mul(4));
}

#[test]
fn coverage_tick_002_idle_backoff_shift_is_capped_at_four() {
    let base = Duration::from_millis(500);

    assert_eq!(
        scheduled_tick_interval(base, PowerState::Unplugged, 100),
        MAX_IDLE_INTERVAL,
        "streaks beyond four must not shift further than 1 << 4"
    );
    assert_eq!(
        scheduled_tick_interval(base, PowerState::Unplugged, u32::MAX),
        MAX_IDLE_INTERVAL
    );
}

#[test]
fn coverage_tick_003_idle_backoff_never_shrinks_below_active_interval() {
    let base = Duration::from_secs(10);

    assert_eq!(
        scheduled_tick_interval(base, PowerState::Unplugged, 3),
        base,
        "the 5s idle ceiling must not shorten an already-slower configured tick"
    );
}

#[test]
fn coverage_tick_004_active_scheduler_runs_on_any_wake() {
    assert!(should_run_scheduler(false, SchedulerWake::Work));
    assert!(should_run_scheduler(false, SchedulerWake::Timer));
}

#[test]
fn coverage_tick_005_gc_not_due_before_deadline_when_active() {
    let now = Instant::now();

    assert!(!instance_gc_due(now, now + Duration::from_secs(1), false));
    assert!(!instance_gc_due(now, now, true));
}

#[test]
fn coverage_tick_006_failed_scan_retries_only_after_interval() {
    let now = Instant::now();
    let mut schedule = SyncScanSchedule::new(now);

    schedule.record_attempt(now, false);

    assert!(
        !schedule.is_due(now + Duration::from_secs(1), false),
        "a failed scan must wait out the retry interval"
    );
    assert!(
        schedule.is_due(now + SYNC_SCAN_INTERVAL, false),
        "the dirty flag survives failure, so the retry fires at the deadline"
    );
    assert!(
        schedule.is_due(now + Duration::from_secs(1), true),
        "a due sync still scans immediately after a failure"
    );
}

#[test]
fn coverage_tick_007_power_state_reports_all_battery_levels() {
    let ctrl = TickController::new();

    ctrl.report_power_state(PowerState::Charging);
    assert_eq!(ctrl.power_state.load(Ordering::Acquire), 0);

    ctrl.report_power_state(PowerState::Unplugged);
    assert_eq!(ctrl.power_state.load(Ordering::Acquire), 1);

    ctrl.report_power_state(PowerState::LowBattery);
    assert_eq!(ctrl.power_state.load(Ordering::Acquire), 2);

    ctrl.report_power_state(PowerState::CriticalBattery);
    assert_eq!(ctrl.power_state.load(Ordering::Acquire), 3);
}

#[test]
fn coverage_tick_008_maintenance_constants_keep_mobile_cadence() {
    assert_eq!(SYNC_SCAN_INTERVAL, Duration::from_secs(5));
    assert_eq!(INSTANCE_GC_INTERVAL, Duration::from_secs(60));
    assert_eq!(QUIESCENT_PARK_INTERVAL, Duration::from_secs(24 * 60 * 60));
    assert_eq!(MAX_IDLE_INTERVAL, Duration::from_secs(5));
}
