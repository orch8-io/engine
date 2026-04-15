use chrono::{DateTime, Datelike, Duration, Timelike, Utc};

use orch8_types::sequence::SendWindow;

/// Check if the given time falls within the send window for the given timezone.
/// Returns `None` if inside the window (ok to send), or `Some(next_open)` if outside.
pub fn check_window(
    now: DateTime<Utc>,
    window: &SendWindow,
    timezone: &str,
) -> Option<DateTime<Utc>> {
    let tz: chrono_tz::Tz = timezone.parse().unwrap_or(chrono_tz::UTC);
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
}
