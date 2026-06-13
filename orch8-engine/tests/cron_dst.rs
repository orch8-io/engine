//! DST edge-case semantics for cron schedules.
//!
//! Policy (documented on `calculate_next_fire_after`):
//! - Nonexistent local times (spring forward) fire at the first valid
//!   instant after the gap — never silently skipped.
//! - Ambiguous local times (fall back) fire once, at the first occurrence.
//!
//! Reference transitions, America/New_York:
//! - Spring forward: 2026-03-08 02:00 EST -> 03:00 EDT (02:xx does not exist)
//! - Fall back:      2026-11-01 02:00 EDT -> 01:00 EST (01:xx happens twice)

use chrono::{TimeZone, Utc};
use orch8_engine::cron::calculate_next_fire_after;
use orch8_types::cron::CronSchedule;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use uuid::Uuid;

fn mk(expr: &str, tz: &str) -> CronSchedule {
    let now = Utc::now();
    CronSchedule {
        id: Uuid::now_v7(),
        tenant_id: TenantId::unchecked("t1"),
        namespace: Namespace::new("ns"),
        sequence_id: SequenceId::new(),
        cron_expr: expr.into(),
        timezone: tz.into(),
        enabled: true,
        metadata: serde_json::Value::Null,
        last_triggered_at: None,
        next_fire_at: None,
        created_at: now,
        updated_at: now,
    }
}

#[test]
fn spring_forward_nonexistent_time_fires_at_gap_end() {
    let s = mk("30 2 * * *", "America/New_York");
    // 01:00 EST on transition day, one hour before the gap.
    let now = Utc.with_ymd_and_hms(2026, 3, 8, 6, 0, 0).unwrap();
    let next = calculate_next_fire_after(&s, now).unwrap();
    // 02:30 EST does not exist; the gap ends at 03:00 EDT = 07:00 UTC.
    assert_eq!(
        next,
        Utc.with_ymd_and_hms(2026, 3, 8, 7, 0, 0).unwrap(),
        "occurrence inside the DST gap must fire at the gap's end, not skip the day"
    );
    // The day after, the schedule is back to its normal local time.
    let after = calculate_next_fire_after(&s, next).unwrap();
    assert_eq!(after, Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap());
}

#[test]
fn spring_forward_does_not_affect_times_outside_gap() {
    let s = mk("30 5 * * *", "America/New_York");
    let now = Utc.with_ymd_and_hms(2026, 3, 8, 6, 0, 0).unwrap();
    let next = calculate_next_fire_after(&s, now).unwrap();
    // 05:30 EDT = 09:30 UTC on the transition day — fires normally.
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 8, 9, 30, 0).unwrap());
}

#[test]
fn fall_back_ambiguous_time_fires_once_at_first_occurrence() {
    let s = mk("30 1 * * *", "America/New_York");
    // 00:00 EDT on transition day, 90 minutes before the first 01:30.
    let now = Utc.with_ymd_and_hms(2026, 11, 1, 4, 0, 0).unwrap();
    let first = calculate_next_fire_after(&s, now).unwrap();
    // First occurrence: 01:30 EDT = 05:30 UTC.
    assert_eq!(first, Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap());
    // Advancing past it must NOT yield the repeated 01:30 EST (06:30 UTC) —
    // the schedule fires once and moves to the next day.
    let second = calculate_next_fire_after(&s, first).unwrap();
    assert_eq!(
        second,
        Utc.with_ymd_and_hms(2026, 11, 2, 6, 30, 0).unwrap(),
        "ambiguous local time must fire once, not twice"
    );
}

#[test]
fn multi_hour_schedule_clamps_only_the_gap_occurrence() {
    // Fires at 01:30 and 02:30 daily. On the spring-forward day the 01:30
    // occurrence is real and the 02:30 occurrence clamps to 03:00 EDT.
    let s = mk("30 1,2 * * *", "America/New_York");
    let now = Utc.with_ymd_and_hms(2026, 3, 8, 6, 0, 0).unwrap();
    let first = calculate_next_fire_after(&s, now).unwrap();
    assert_eq!(first, Utc.with_ymd_and_hms(2026, 3, 8, 6, 30, 0).unwrap()); // 01:30 EST
    let second = calculate_next_fire_after(&s, first).unwrap();
    assert_eq!(second, Utc.with_ymd_and_hms(2026, 3, 8, 7, 0, 0).unwrap()); // 03:00 EDT (clamped 02:30)
}

#[test]
fn utc_schedules_are_untouched_by_dst_logic() {
    let s = mk("30 2 * * *", "UTC");
    let now = Utc.with_ymd_and_hms(2026, 3, 8, 0, 0, 0).unwrap();
    let next = calculate_next_fire_after(&s, now).unwrap();
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 8, 2, 30, 0).unwrap());
}

#[test]
fn southern_hemisphere_gap_also_clamps() {
    // Chile: 2026-09-06 00:00 -> 01:00 (spring forward, southern hemisphere).
    let s = mk("30 0 * * *", "America/Santiago");
    // 22:00 local on Sep 5 = 02:00 UTC Sep 6 (UTC-4 before transition).
    let now = Utc.with_ymd_and_hms(2026, 9, 6, 2, 0, 0).unwrap();
    let next = calculate_next_fire_after(&s, now).unwrap();
    // 00:30 local does not exist; gap ends at 01:00 UTC-3 = 04:00 UTC.
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 9, 6, 4, 0, 0).unwrap());
}
