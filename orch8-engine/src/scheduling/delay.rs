use chrono::{DateTime, Datelike, Duration, Utc, Weekday};
use rand::Rng;

use orch8_types::sequence::DelaySpec;

/// Calculate the next fire time given a base time, delay spec, and timezone.
pub fn calculate_next_fire_at(
    from: DateTime<Utc>,
    delay: &DelaySpec,
    timezone: &str,
) -> DateTime<Utc> {
    let mut target = from + Duration::from_std(delay.duration).unwrap_or_else(|_| Duration::zero());

    if delay.business_days_only {
        target = skip_non_business_days(target, timezone);
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

/// If the target lands on a weekend, advance to Monday.
fn skip_non_business_days(dt: DateTime<Utc>, timezone: &str) -> DateTime<Utc> {
    // Parse timezone; fall back to UTC if invalid.
    let tz: chrono_tz::Tz = timezone.parse().unwrap_or(chrono_tz::UTC);
    let local = dt.with_timezone(&tz);
    let weekday = local.weekday();

    let days_to_add = match weekday {
        Weekday::Sat => 2,
        Weekday::Sun => 1,
        _ => 0,
    };

    if days_to_add > 0 {
        dt + Duration::days(days_to_add)
    } else {
        dt
    }
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
        };
        let result = calculate_next_fire_at(from, &delay, "UTC");
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
        };
        let result = calculate_next_fire_at(sat, &delay, "UTC");
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
        };
        let result = calculate_next_fire_at(sun, &delay, "UTC");
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
        };
        let result = calculate_next_fire_at(mon, &delay, "UTC");
        assert_eq!(result.weekday(), Weekday::Mon);
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let from = Utc::now();
        let delay = DelaySpec {
            duration: StdDuration::from_secs(3600),
            business_days_only: false,
            jitter: Some(StdDuration::from_secs(60)),
        };
        for _ in 0..100 {
            let result = calculate_next_fire_at(from, &delay, "UTC");
            let diff = (result - from).num_seconds();
            assert!(diff >= 3540 && diff <= 3660, "diff was {diff}");
        }
    }
}
