use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc, Weekday};
use rand::Rng;

use orch8_types::sequence::DelaySpec;

/// Calculate the next fire time given a base time, delay spec, timezone, and
/// optional tenant-level config (for holiday calendar merging).
pub fn calculate_next_fire_at(
    from: DateTime<Utc>,
    delay: &DelaySpec,
    timezone: &str,
    context_config: Option<&serde_json::Value>,
) -> DateTime<Utc> {
    // If fire_at_local is specified, compute the UTC equivalent using the
    // delay's timezone (falling back to the instance timezone).
    let mut target = if let Some(ref local_str) = delay.fire_at_local {
        let tz_name = delay.timezone.as_deref().unwrap_or(timezone);
        resolve_fire_at_local(local_str, tz_name).unwrap_or_else(|| {
            from + Duration::from_std(delay.duration).unwrap_or_else(|_| Duration::zero())
        })
    } else {
        from + Duration::from_std(delay.duration).unwrap_or_else(|_| Duration::zero())
    };

    if delay.business_days_only {
        let holidays = collect_holidays(&delay.holidays, context_config);
        target = skip_non_business_days(target, timezone, &holidays);
    }

    if let Some(jitter) = delay.jitter {
        let jitter_ms = i64::try_from(jitter.as_millis()).unwrap_or(i64::MAX);
        if jitter_ms > 0 {
            let offset = rand::thread_rng().gen_range(-jitter_ms..=jitter_ms);
            target += Duration::milliseconds(offset);
        }
    }

    target
}

/// Merge step-level holidays with tenant-level holidays from `context.config.holidays`.
fn collect_holidays(
    step_holidays: &[String],
    context_config: Option<&serde_json::Value>,
) -> Vec<NaiveDate> {
    let mut dates = Vec::new();

    for s in step_holidays {
        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            dates.push(d);
        }
    }

    if let Some(config) = context_config {
        if let Some(arr) = config.get("holidays").and_then(|v| v.as_array()) {
            for val in arr {
                if let Some(s) = val.as_str() {
                    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        dates.push(d);
                    }
                }
            }
        }
    }

    dates
}

/// Resolve a `fire_at_local` string (`NaiveDateTime`) to UTC using the given
/// IANA timezone. Handles DST gaps by rolling forward to the next valid time.
fn resolve_fire_at_local(local_str: &str, tz_name: &str) -> Option<DateTime<Utc>> {
    use chrono::TimeZone;

    let naive = NaiveDateTime::parse_from_str(local_str, "%Y-%m-%dT%H:%M:%S").ok()?;
    let tz: chrono_tz::Tz = tz_name.parse().ok()?;

    match tz.from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
        chrono::LocalResult::Ambiguous(earliest, _) => {
            // Fall-back (e.g. autumn DST): use the earlier interpretation.
            Some(earliest.with_timezone(&Utc))
        }
        chrono::LocalResult::None => {
            // Spring-forward gap: roll forward by trying +1 hour.
            let rolled = naive + Duration::hours(1);
            match tz.from_local_datetime(&rolled) {
                chrono::LocalResult::Single(dt) => Some(dt.with_timezone(&Utc)),
                chrono::LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
                chrono::LocalResult::None => None,
            }
        }
    }
}

/// If the target lands on a weekend or holiday, advance to the next business day.
fn skip_non_business_days(
    dt: DateTime<Utc>,
    timezone: &str,
    holidays: &[NaiveDate],
) -> DateTime<Utc> {
    let tz: chrono_tz::Tz = timezone.parse().unwrap_or_else(|_| {
        tracing::warn!(timezone = %timezone, "invalid timezone, falling back to UTC");
        chrono_tz::UTC
    });
    let mut current = dt;

    // Advance past weekends and holidays (max 30 days to avoid infinite loop).
    for _ in 0..30 {
        let local = current.with_timezone(&tz);
        let weekday = local.weekday();
        let local_date = local.date_naive();

        if weekday == Weekday::Sat {
            current += Duration::days(2);
        } else if weekday == Weekday::Sun || holidays.contains(&local_date) {
            current += Duration::days(1);
        } else {
            break;
        }
    }

    current
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;

    #[test]
    fn simple_delay() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_hours(1),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        let diff = result - from;
        assert_eq!(diff.num_seconds(), 3600);
    }

    #[test]
    fn saturday_skips_to_monday() {
        // 2024-01-06 is a Saturday
        let sat = chrono::NaiveDate::from_ymd_opt(2024, 1, 6)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(sat, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn sunday_skips_to_monday() {
        // 2024-01-07 is a Sunday
        let sun = chrono::NaiveDate::from_ymd_opt(2024, 1, 7)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(sun, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn weekday_stays() {
        // 2024-01-08 is a Monday
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_hours(1),
            business_days_only: false,
            jitter: Some(StdDuration::from_mins(1)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        for _ in 0..100 {
            let result = calculate_next_fire_at(from, &delay, "UTC", None);
            let diff = (result - from).num_seconds();
            assert!((3540..=3660).contains(&diff), "diff was {diff}");
        }
    }

    #[test]
    fn jitter_produces_variation() {
        // With 10s jitter, 200 samples should not all be identical.
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_mins(1),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(10)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let mut results: Vec<i64> = (0..200)
            .map(|_| {
                let r = calculate_next_fire_at(from, &delay, "UTC", None);
                (r - from).num_milliseconds()
            })
            .collect();
        results.sort_unstable();
        let min = results[0];
        let max = results[results.len() - 1];
        // With 10s jitter range (±10 000ms), 200 samples should span
        // at least 1 second of spread.
        assert!(
            max - min > 1_000,
            "jitter should produce variation; got range {min}..{max} ms"
        );
    }

    #[test]
    fn zero_jitter_has_no_effect() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(30),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(0)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        for _ in 0..50 {
            let result = calculate_next_fire_at(from, &delay, "UTC", None);
            let diff = (result - from).num_seconds();
            assert_eq!(diff, 30, "zero jitter should not alter the delay");
        }
    }

    #[test]
    fn no_jitter_is_deterministic() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_mins(2),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let results: Vec<i64> = (0..50)
            .map(|_| (calculate_next_fire_at(from, &delay, "UTC", None) - from).num_seconds())
            .collect();
        assert!(
            results.iter().all(|&d| d == 120),
            "without jitter, every result must be exactly the delay"
        );
    }

    #[test]
    fn jitter_with_zero_duration() {
        // Jitter alone (no base delay) should scatter around `from`.
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(5)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let mut any_negative = false;
        let mut any_positive = false;
        for _ in 0..200 {
            let diff_ms =
                (calculate_next_fire_at(from, &delay, "UTC", None) - from).num_milliseconds();
            assert!(
                (-5_000..=5_000).contains(&diff_ms),
                "jitter must stay within ±5s, got {diff_ms}ms"
            );
            if diff_ms < 0 {
                any_negative = true;
            }
            if diff_ms > 0 {
                any_positive = true;
            }
        }
        assert!(any_negative, "jitter should produce some negative offsets");
        assert!(any_positive, "jitter should produce some positive offsets");
    }

    #[test]
    fn large_jitter_stays_within_bounds() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(10),
            business_days_only: false,
            jitter: Some(StdDuration::from_mins(1)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        for _ in 0..200 {
            let diff_ms =
                (calculate_next_fire_at(from, &delay, "UTC", None) - from).num_milliseconds();
            // base=10s, jitter=±60s → range [-50s, +70s]
            assert!(
                (-50_000..=70_000).contains(&diff_ms),
                "large jitter out of bounds: {diff_ms}ms"
            );
        }
    }

    #[test]
    fn jitter_combined_with_business_days() {
        // 2024-01-08 Monday 10:00 UTC, 1s jitter, business_days_only
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: Some(StdDuration::from_secs(1)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        for _ in 0..50 {
            let result = calculate_next_fire_at(mon, &delay, "UTC", None);
            // Should still land on Monday (business day skip first, jitter second)
            assert_eq!(result.weekday(), Weekday::Mon);
            let diff_ms = (result - mon).num_milliseconds();
            assert!(
                (-1_000..=1_000).contains(&diff_ms),
                "jitter must stay within ±1s, got {diff_ms}ms"
            );
        }
    }

    #[test]
    fn holiday_skips_to_next_business_day() {
        // 2024-01-08 is a Monday — mark it as a holiday
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec!["2024-01-08".to_string()],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Tue);
    }

    #[test]
    fn friday_holiday_skips_weekend_to_monday() {
        // 2024-01-05 is a Friday — mark it as holiday
        let fri = chrono::NaiveDate::from_ymd_opt(2024, 1, 5)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec!["2024-01-05".to_string()],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(fri, &delay, "UTC", None);
        // Friday holiday → Saturday → Monday
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn context_holidays_merged() {
        // 2024-01-09 is a Tuesday — tenant holiday
        let tue = chrono::NaiveDate::from_ymd_opt(2024, 1, 9)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let config = serde_json::json!({"holidays": ["2024-01-09"]});
        let result = calculate_next_fire_at(tue, &delay, "UTC", Some(&config));
        assert_eq!(result.weekday(), Weekday::Wed);
    }

    #[test]
    fn five_consecutive_holidays_all_skipped() {
        // Mon 2024-01-08 through Fri 2024-01-12 all holidays → lands on Mon 2024-01-15
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![
                "2024-01-08".to_string(),
                "2024-01-09".to_string(),
                "2024-01-10".to_string(),
                "2024-01-11".to_string(),
                "2024-01-12".to_string(),
            ],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
        assert_eq!(
            result.date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        );
    }

    #[test]
    fn zero_duration_returns_from_unchanged() {
        let from = chrono::NaiveDate::from_ymd_opt(2024, 3, 15)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        assert_eq!(result, from);
    }

    #[test]
    fn negative_jitter_offset_produces_time_before_base() {
        // Large jitter relative to zero base → many samples must fall before `from`.
        let from = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(10)),
            holidays: vec![],
            fire_at_local: None,
            timezone: None,
        };
        let mut saw_negative = false;
        for _ in 0..500 {
            let result = calculate_next_fire_at(from, &delay, "UTC", None);
            if (result - from).num_milliseconds() < 0 {
                saw_negative = true;
                break;
            }
        }
        assert!(saw_negative, "negative jitter offset must be reachable");
    }

    #[test]
    fn fire_at_local_valid_timezone_resolves_to_utc() {
        // 2024-06-15 09:00 America/New_York (EDT, UTC-4) → 13:00 UTC
        let from = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: Some("2024-06-15T09:00:00".to_string()),
            timezone: Some("America/New_York".to_string()),
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(13, 0, 0)
            .unwrap()
            .and_utc();
        assert_eq!(result, expected);
    }

    #[test]
    fn fire_at_local_invalid_timezone_falls_back_to_duration() {
        let from = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_mins(2),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: Some("2024-06-15T09:00:00".to_string()),
            timezone: Some("Not/A_Real_Zone".to_string()),
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        assert_eq!((result - from).num_seconds(), 120);
    }

    #[test]
    fn fire_at_local_during_dst_spring_forward_rolls_forward() {
        // US spring-forward: 2024-03-10 02:30 America/New_York does not exist
        // (clocks jump 02:00 → 03:00). Should roll forward to 03:30 EDT = 07:30 UTC.
        let from = chrono::NaiveDate::from_ymd_opt(2024, 3, 10)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: Some("2024-03-10T02:30:00".to_string()),
            timezone: Some("America/New_York".to_string()),
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 3, 10)
            .unwrap()
            .and_hms_opt(7, 30, 0)
            .unwrap()
            .and_utc();
        assert_eq!(result, expected);
    }

    #[test]
    fn fire_at_local_during_dst_fall_back_uses_earliest() {
        // US fall-back: 2024-11-03 01:30 America/New_York occurs twice.
        // Earliest interpretation is EDT (UTC-4) → 05:30 UTC.
        let from = chrono::NaiveDate::from_ymd_opt(2024, 11, 3)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: Some("2024-11-03T01:30:00".to_string()),
            timezone: Some("America/New_York".to_string()),
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 11, 3)
            .unwrap()
            .and_hms_opt(5, 30, 0)
            .unwrap()
            .and_utc();
        assert_eq!(result, expected);
    }

    #[test]
    fn saturday_holiday_skips_to_monday() {
        // 2024-01-06 is already a Saturday; also marking it a holiday still lands on Monday.
        let sat = chrono::NaiveDate::from_ymd_opt(2024, 1, 6)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec!["2024-01-06".to_string()],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(sat, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
        assert_eq!(
            result.date_naive(),
            NaiveDate::from_ymd_opt(2024, 1, 8).unwrap()
        );
    }

    #[test]
    fn step_and_context_holidays_merged() {
        // Mon 2024-01-08 holiday via step, Tue 2024-01-09 via context.config → Wed.
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec!["2024-01-08".to_string()],
            fire_at_local: None,
            timezone: None,
        };
        let config = serde_json::json!({"holidays": ["2024-01-09"]});
        let result = calculate_next_fire_at(mon, &delay, "UTC", Some(&config));
        assert_eq!(result.weekday(), Weekday::Wed);
    }

    #[test]
    fn invalid_holiday_date_string_ignored_gracefully() {
        // One bogus date and one real one — only the valid one applies.
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec![
                "not-a-date".to_string(),
                "2024-13-40".to_string(),
                "2024-01-08".to_string(),
            ],
            fire_at_local: None,
            timezone: None,
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Tue);
    }

    #[test]
    fn fire_at_local_parse_error_falls_back_to_duration() {
        let from = chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_mins(5),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
            fire_at_local: Some("totally bogus".to_string()),
            timezone: Some("UTC".to_string()),
        };
        let result = calculate_next_fire_at(from, &delay, "UTC", None);
        assert_eq!((result - from).num_seconds(), 300);
    }
}
