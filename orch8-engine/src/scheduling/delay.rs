use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc, Weekday};
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
    let mut target = from + Duration::from_std(delay.duration).unwrap_or_else(|_| Duration::zero());

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

/// If the target lands on a weekend or holiday, advance to the next business day.
fn skip_non_business_days(
    dt: DateTime<Utc>,
    timezone: &str,
    holidays: &[NaiveDate],
) -> DateTime<Utc> {
    let tz: chrono_tz::Tz = timezone.parse().unwrap_or(chrono_tz::UTC);
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
            duration: StdDuration::from_secs(3600),
            business_days_only: false,
            jitter: None,
            holidays: vec![],
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
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(3600),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(60)),
            holidays: vec![],
        };
        for _ in 0..100 {
            let result = calculate_next_fire_at(from, &delay, "UTC", None);
            let diff = (result - from).num_seconds();
            assert!(diff >= 3540 && diff <= 3660, "diff was {diff}");
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
        };
        let config = serde_json::json!({"holidays": ["2024-01-09"]});
        let result = calculate_next_fire_at(tue, &delay, "UTC", Some(&config));
        assert_eq!(result.weekday(), Weekday::Wed);
    }

    #[test]
    fn consecutive_holidays_skipped() {
        // 2024-01-08 Mon, 2024-01-09 Tue both holidays
        let mon = chrono::NaiveDate::from_ymd_opt(2024, 1, 8)
            .unwrap()
            .and_hms_opt(10, 0, 0)
            .unwrap()
            .and_utc();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(0),
            business_days_only: true,
            jitter: None,
            holidays: vec!["2024-01-08".to_string(), "2024-01-09".to_string()],
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC", None);
        assert_eq!(result.weekday(), Weekday::Wed);
    }
}
