//! Pure calendar arithmetic for durable scheduling and dry runs.
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Timelike, Utc, Weekday};
use orch8_types::sequence::DelaySpec;

/// Turn a resolved ASL wait parameter into an absolute scheduler deadline.
pub(crate) fn asl_wait_deadline(
    now: DateTime<Utc>,
    params: &serde_json::Value,
) -> Result<DateTime<Utc>, &'static str> {
    match (params.get("seconds"), params.get("timestamp")) {
        (Some(seconds), None) => {
            let seconds = seconds
                .as_i64()
                .filter(|value| (0..=99_999_999).contains(value))
                .ok_or("ASL SecondsPath must resolve to an integer from 0 to 99999999")?;
            now.checked_add_signed(Duration::seconds(seconds))
                .ok_or("ASL wait deadline is outside the supported timestamp range")
        }
        (None, Some(timestamp)) => {
            let timestamp = timestamp
                .as_str()
                .filter(|value| value.contains('T') && value.ends_with('Z'))
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .and_then(|value| value.with_timezone(&Utc).with_nanosecond(0))
                .ok_or("ASL TimestampPath must resolve to a UTC RFC3339 timestamp")?;
            Ok(timestamp)
        }
        _ => Err("ASL Wait requires exactly one resolved seconds or timestamp parameter"),
    }
}

/// Calculate the next fire time given a base time, delay spec, timezone, and
/// optional tenant-level config (for holiday calendar merging).
pub(crate) fn calendar_deadline(
    from: DateTime<Utc>,
    delay: &DelaySpec,
    timezone: &str,
    context_config: Option<&serde_json::Value>,
    jitter_offset: impl FnOnce(i64) -> i64,
) -> DateTime<Utc> {
    // If fire_at_local is specified, compute the UTC equivalent using the
    // delay's timezone (falling back to the instance timezone).
    let mut target = if let Some(ref local_str) = delay.fire_at_local {
        let tz_name = delay.timezone.as_deref().unwrap_or(timezone);
        resolve_fire_at_local(local_str, tz_name)
            .unwrap_or_else(|| add_duration_clamped(from, delay.duration))
    } else {
        add_duration_clamped(from, delay.duration)
    };

    let holidays = if delay.business_days_only {
        let holidays = collect_holidays(&delay.holidays, context_config);
        target = skip_non_business_days(target, timezone, &holidays);
        Some(holidays)
    } else {
        None
    };

    if let Some(jitter) = delay.jitter {
        let jitter_ms = i64::try_from(jitter.as_millis()).unwrap_or(i64::MAX);
        if jitter_ms > 0 {
            let offset = jitter_offset(jitter_ms);
            let jittered = target
                .checked_add_signed(Duration::milliseconds(offset))
                .unwrap_or(DateTime::<Utc>::MAX_UTC);
            // T1-GAP-037: jitter must not push a business-day target onto a
            // weekend or holiday (Monday 00:00 - 1h is Sunday). Keep the
            // un-jittered business-day target for such a draw.
            let lands_off_business_day = holidays.as_deref().is_some_and(|holidays| {
                skip_non_business_days(jittered, timezone, holidays) != jittered
            });
            if !lands_off_business_day {
                target = jittered;
            }
        }
    }

    target
}

/// Add a `std` duration to a timestamp, clamping instead of panicking or
/// collapsing to zero. `DateTime + Duration` panics when the sum overflows
/// the representable range, and `from_std` rejects durations beyond
/// chrono's range — both only reachable with an absurd user-configured
/// delay, where "far in the future" is the safe direction (the old code
/// fired *immediately* in the first case and panicked in the second).
fn add_duration_clamped(from: DateTime<Utc>, duration: std::time::Duration) -> DateTime<Utc> {
    let delta = Duration::from_std(duration).unwrap_or(Duration::MAX);
    from.checked_add_signed(delta)
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
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

    if let Some(config) = context_config
        && let Some(arr) = config.get("holidays").and_then(|v| v.as_array())
    {
        for val in arr {
            if let Some(s) = val.as_str()
                && let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d")
            {
                dates.push(d);
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
            let rolled = naive.checked_add_signed(Duration::hours(1))?;
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
            current = add_duration_clamped(current, std::time::Duration::from_secs(172_800));
        } else if weekday == Weekday::Sun || holidays.contains(&local_date) {
            current = add_duration_clamped(current, std::time::Duration::from_secs(86_400));
        } else {
            break;
        }
    }

    current
}
