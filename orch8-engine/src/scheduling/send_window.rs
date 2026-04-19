use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use tracing::warn;

use orch8_types::sequence::SendWindow;

/// Check if the given time falls within the send window for the given timezone.
/// Returns `None` if inside the window (ok to send), or `Some(next_open)` if outside.
pub fn check_window(
    now: DateTime<Utc>,
    window: &SendWindow,
    timezone: &str,
) -> Option<DateTime<Utc>> {
    let tz: chrono_tz::Tz = timezone.parse().unwrap_or_else(|_| {
        warn!(timezone = %timezone, "invalid timezone, falling back to UTC");
        chrono_tz::UTC
    });
    let local = now.with_timezone(&tz);

    #[allow(clippy::cast_possible_truncation)]
    let hour = local.hour() as u8;
    #[allow(clippy::cast_possible_truncation)]
    let weekday_num = local.weekday().num_days_from_monday() as u8; // 0=Mon

    // Check day-of-week restriction.
    let day_ok = window.days.is_empty() || window.days.contains(&weekday_num);

    // Check hour window.
    let hour_ok = match window.start_hour.cmp(&window.end_hour) {
        std::cmp::Ordering::Less => hour >= window.start_hour && hour < window.end_hour,
        // Wrapping window (e.g., 22:00-06:00)
        std::cmp::Ordering::Greater => hour >= window.start_hour || hour < window.end_hour,
        // start == end means 24h window
        std::cmp::Ordering::Equal => true,
    };

    if day_ok && hour_ok {
        return None; // Inside window
    }

    // Calculate next window open time.
    Some(next_window_open(local, window, tz))
}

/// Calculate the next time the send window opens.
fn next_window_open(
    local: chrono::DateTime<chrono_tz::Tz>,
    window: &SendWindow,
    _tz: chrono_tz::Tz,
) -> DateTime<Utc> {
    let mut candidate = local;

    // Try up to 8 days ahead (handles full week + 1).
    for _ in 0..8 {
        #[allow(clippy::cast_possible_truncation)]
        let weekday_num = candidate.weekday().num_days_from_monday() as u8;
        let day_ok = window.days.is_empty() || window.days.contains(&weekday_num);
        #[allow(clippy::cast_possible_truncation)]
        let hour = candidate.hour() as u8;

        if day_ok && hour < window.start_hour {
            // Same day, advance to start_hour.
            let target = candidate
                .with_hour(u32::from(window.start_hour))
                .unwrap_or(candidate)
                .with_minute(0)
                .unwrap_or(candidate)
                .with_second(0)
                .unwrap_or(candidate);
            return target.with_timezone(&Utc);
        }
        // If we're past end_hour on a valid day, fall through to next day.

        // Advance to start of next day.
        candidate = (candidate + Duration::days(1))
            .with_hour(0)
            .unwrap_or(candidate)
            .with_minute(0)
            .unwrap_or(candidate)
            .with_second(0)
            .unwrap_or(candidate);
    }

    // Fallback: return start_hour tomorrow (shouldn't reach here).
    (local + Duration::days(1))
        .with_hour(u32::from(window.start_hour))
        .unwrap_or(local)
        .with_minute(0)
        .unwrap_or(local)
        .with_second(0)
        .unwrap_or(local)
        .with_timezone(&Utc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window_9_17() -> SendWindow {
        SendWindow {
            start_hour: 9,
            end_hour: 17,
            days: vec![],
        }
    }

    fn make_utc(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, 0)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn inside_window_returns_none() {
        let now = make_utc(2024, 1, 8, 10, 0); // Monday 10:00
        assert!(check_window(now, &window_9_17(), "UTC").is_none());
    }

    #[test]
    fn before_window_defers_to_start() {
        let now = make_utc(2024, 1, 8, 7, 30); // Monday 7:30
        let next = check_window(now, &window_9_17(), "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.hour(), 9);
        assert_eq!(local.minute(), 0);
        assert_eq!(local.day(), 8); // Same day
    }

    #[test]
    fn after_window_defers_to_next_day() {
        let now = make_utc(2024, 1, 8, 18, 0); // Monday 18:00
        let next = check_window(now, &window_9_17(), "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.hour(), 9);
        assert_eq!(local.day(), 9); // Tuesday
    }

    #[test]
    fn friday_evening_defers_to_monday_with_day_restriction() {
        let window = SendWindow {
            start_hour: 9,
            end_hour: 17,
            days: vec![0, 1, 2, 3, 4], // Mon-Fri
        };
        let now = make_utc(2024, 1, 5, 18, 0); // Friday 18:00
        let next = check_window(now, &window, "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.hour(), 9);
        assert_eq!(local.weekday(), chrono::Weekday::Mon);
    }

    #[test]
    fn respects_timezone() {
        // 2024-01-08 06:00 UTC = 2024-01-08 09:00 Sao Paulo (UTC-3)
        let now = make_utc(2024, 1, 8, 12, 0); // UTC 12:00 = SP 09:00
        assert!(check_window(now, &window_9_17(), "America/Sao_Paulo").is_none());

        // UTC 05:00 = SP 02:00 — outside window
        let early = make_utc(2024, 1, 8, 5, 0);
        assert!(check_window(early, &window_9_17(), "America/Sao_Paulo").is_some());
    }

    #[test]
    fn wrapping_window() {
        let window = SendWindow {
            start_hour: 22,
            end_hour: 6,
            days: vec![],
        };
        // 23:00 — inside
        let late = make_utc(2024, 1, 8, 23, 0);
        assert!(check_window(late, &window, "UTC").is_none());
        // 03:00 — inside
        let early = make_utc(2024, 1, 9, 3, 0);
        assert!(check_window(early, &window, "UTC").is_none());
        // 12:00 — outside
        let mid = make_utc(2024, 1, 8, 12, 0);
        assert!(check_window(mid, &window, "UTC").is_some());
    }

    #[test]
    fn empty_days_means_any_day_is_valid() {
        let window = window_9_17();
        // 2024-01-06 Saturday, 2024-01-07 Sunday, 2024-01-08 Monday — all 10:00 UTC.
        for d in [6u32, 7, 8] {
            let now = make_utc(2024, 1, d, 10, 0);
            assert!(
                check_window(now, &window, "UTC").is_none(),
                "day {d} should be inside window when days is empty"
            );
        }
    }

    #[test]
    fn weekend_only_window_defers_weekday_to_saturday() {
        let window = SendWindow {
            start_hour: 9,
            end_hour: 17,
            days: vec![5, 6], // Sat, Sun
        };
        // Monday 2024-01-08 10:00 UTC — outside (weekday)
        let now = make_utc(2024, 1, 8, 10, 0);
        let next = check_window(now, &window, "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.weekday(), chrono::Weekday::Sat);
        assert_eq!(local.hour(), 9);
    }

    #[test]
    fn start_equal_end_means_24h_window() {
        let window = SendWindow {
            start_hour: 9,
            end_hour: 9,
            days: vec![],
        };
        for h in [0u32, 8, 9, 15, 23] {
            let now = make_utc(2024, 1, 8, h, 0);
            assert!(
                check_window(now, &window, "UTC").is_none(),
                "hour {h} should be inside 24h window"
            );
        }
    }

    #[test]
    fn timezone_conversion_est_vs_utc() {
        // 2024-01-08 14:00 UTC = 09:00 EST (America/New_York is UTC-5 in January)
        let now = make_utc(2024, 1, 8, 14, 0);
        // In UTC terms, 14:00 is inside 9-17 window
        assert!(check_window(now, &window_9_17(), "UTC").is_none());
        // In EST terms, 14:00 UTC = 09:00 EST — also inside
        assert!(check_window(now, &window_9_17(), "America/New_York").is_none());

        // 13:00 UTC = 08:00 EST — outside EST window but inside UTC window
        let borderline = make_utc(2024, 1, 8, 13, 0);
        assert!(check_window(borderline, &window_9_17(), "UTC").is_none());
        assert!(check_window(borderline, &window_9_17(), "America/New_York").is_some());
    }

    #[test]
    fn next_window_calculation_crosses_week_boundary() {
        // Window: Monday only, 9-17. Time is Sunday 2024-01-07 23:00 UTC →
        // next open is Monday 2024-01-08 09:00 UTC.
        let window = SendWindow {
            start_hour: 9,
            end_hour: 17,
            days: vec![0], // Mon only
        };
        let sun_evening = make_utc(2024, 1, 7, 23, 0);
        let next = check_window(sun_evening, &window, "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.weekday(), chrono::Weekday::Mon);
        assert_eq!(local.day(), 8);
        assert_eq!(local.hour(), 9);

        // And from Tuesday 2024-01-09 10:00 UTC → jump 6 days to Mon 2024-01-15 09:00 UTC.
        let tue = make_utc(2024, 1, 9, 10, 0);
        let next = check_window(tue, &window, "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.weekday(), chrono::Weekday::Mon);
        assert_eq!(local.day(), 15);
        assert_eq!(local.hour(), 9);
    }

    #[test]
    fn invalid_timezone_falls_back_to_utc() {
        // Inside 9-17 in UTC terms → should be treated as inside.
        let now = make_utc(2024, 1, 8, 10, 0);
        assert!(check_window(now, &window_9_17(), "Not/A_Zone").is_none());

        // Outside 9-17 UTC → should be outside (confirming UTC fallback was used).
        let night = make_utc(2024, 1, 8, 3, 0);
        assert!(check_window(night, &window_9_17(), "Not/A_Zone").is_some());
    }

    #[test]
    fn single_valid_day_finds_correct_next_occurrence() {
        // Only Wednesday (day=2). From Thursday 2024-01-11 10:00 UTC, next should be
        // Wednesday 2024-01-17 09:00 UTC.
        let window = SendWindow {
            start_hour: 9,
            end_hour: 17,
            days: vec![2], // Wed only
        };
        let thu = make_utc(2024, 1, 11, 10, 0);
        let next = check_window(thu, &window, "UTC").unwrap();
        let local = next.with_timezone(&chrono_tz::UTC);
        assert_eq!(local.weekday(), chrono::Weekday::Wed);
        assert_eq!(local.day(), 17);
        assert_eq!(local.hour(), 9);
    }
}
