use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron::Schedule;
use tokio::task::JoinSet;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::clock::SharedClock;
use orch8_types::context::ExecutionContext;
use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};

use crate::error::EngineError;

/// Run the cron tick loop on the system clock. Checks every `tick_interval`
/// for due cron schedules, creates instances for each, and advances their
/// `next_fire_at`. See [`run_cron_loop_with_clock`] for virtual-time use.
pub async fn run_cron_loop(
    storage: Arc<dyn StorageBackend>,
    tick_interval: Duration,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    run_cron_loop_with_clock(storage, tick_interval, SharedClock::default(), cancel).await
}

/// [`run_cron_loop`] reading "now" from the supplied scheduler clock.
///
/// Note: the loop still *ticks* on real (tokio) time; the clock only governs
/// which schedules are considered due and how their next fire is computed.
pub async fn run_cron_loop_with_clock(
    storage: Arc<dyn StorageBackend>,
    tick_interval: Duration,
    clock: SharedClock,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        tick_secs = tick_interval.as_secs(),
        "starting cron tick loop"
    );

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                info!("cron loop cancelled");
                return Ok(());
            }
            _ = ticker.tick() => {
                if let Err(e) = process_cron_tick(&storage, &clock).await {
                    error!(error = %e, "cron tick failed");
                }
            }
        }
    }
}

/// Run a single cron evaluation pass: claim due schedules, apply overlap
/// policies, fire instances, advance fire times. Public so tests and tooling
/// (e.g. `orch8 dev`) can drive cron deterministically without the loop.
pub async fn process_cron_tick(
    storage: &Arc<dyn StorageBackend>,
    clock: &SharedClock,
) -> Result<(), EngineError> {
    let now = clock.now();
    let schedules = storage.claim_due_cron_schedules(now).await?;

    if schedules.is_empty() {
        return Ok(());
    }

    debug!(count = schedules.len(), "processing due cron schedules");

    // Each claimed schedule is independent: fire the instance and advance the
    // schedule's fire times. Process them concurrently so a slow storage write
    // on one schedule doesn't stall the rest of the tick.
    let mut tasks: JoinSet<()> = JoinSet::new();
    for schedule in schedules {
        let storage = Arc::clone(storage);
        let clock = clock.clone();
        tasks.spawn(async move {
            let cron_id = schedule.id;
            if let Err(e) = trigger_cron_schedule(storage.as_ref(), &schedule, &clock).await {
                error!(
                    cron_id = %cron_id,
                    error = %e,
                    "failed to trigger cron schedule"
                );
            }
        });
    }
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            error!(error = %e, "cron task panicked");
        }
    }

    Ok(())
}

/// Stamp the originating schedule's ID into instance metadata so overlap
/// policies (and operators) can attribute runs back to their schedule.
/// Non-object metadata is passed through unstamped — such instances are
/// invisible to overlap detection, which only degrades policies to `allow`.
fn stamp_cron_id(metadata: serde_json::Value, cron_id: uuid::Uuid) -> serde_json::Value {
    match metadata {
        serde_json::Value::Object(mut map) => {
            map.insert(
                "cron_schedule_id".to_string(),
                serde_json::json!(cron_id.to_string()),
            );
            serde_json::Value::Object(map)
        }
        serde_json::Value::Null => {
            serde_json::json!({ "cron_schedule_id": cron_id.to_string() })
        }
        other => other,
    }
}

/// How long a `buffer_one` deferral re-arms the schedule for. Short enough
/// that the buffered fire lands on the first tick after the previous run
/// finishes, long enough not to spin.
const BUFFER_RETRY_SECS: i64 = 5;

/// Apply the schedule's overlap policy against any still-active previous
/// runs. Returns `Ok(true)` to proceed with firing, `Ok(false)` when the
/// occurrence was skipped or deferred (the policy has already advanced /
/// re-armed the schedule's fire times).
async fn apply_overlap_policy(
    storage: &dyn StorageBackend,
    schedule: &CronSchedule,
    now: DateTime<Utc>,
) -> Result<bool, EngineError> {
    use orch8_types::cron::OverlapPolicy;

    if schedule.overlap_policy == OverlapPolicy::Allow {
        return Ok(true);
    }
    let active = storage
        .active_instance_ids_for_cron(schedule.id, 100)
        .await?;
    if active.is_empty() {
        return Ok(true);
    }

    match schedule.overlap_policy {
        OverlapPolicy::Skip => {
            if let Some(next_at) = calculate_next_fire_after(schedule, now) {
                storage.record_cron_skip(schedule.id, now, next_at).await?;
            }
            crate::metrics::inc(crate::metrics::CRON_SKIPPED);
            info!(
                cron_id = %schedule.id,
                active_runs = active.len(),
                "cron occurrence skipped (overlap policy: previous run still active)"
            );
            Ok(false)
        }
        OverlapPolicy::BufferOne => {
            // Re-arm shortly instead of advancing: the occurrence stays
            // pending and fires on the first tick after the previous run
            // finishes. Missed occurrences collapse into one buffered fire.
            let retry_at = now + chrono::Duration::seconds(BUFFER_RETRY_SECS);
            storage
                .update_cron_fire_times(schedule.id, now, retry_at)
                .await?;
            debug!(
                cron_id = %schedule.id,
                active_runs = active.len(),
                "cron occurrence deferred (buffer_one: previous run still active)"
            );
            Ok(false)
        }
        OverlapPolicy::CancelPrevious => {
            // Enqueue a `Cancel` signal instead of writing `Cancelled`
            // directly: the direct write bypassed `can_transition_to`'s CAS
            // guard (could clobber a state the instance moved to concurrently)
            // and skipped `on_cancel` cleanup hooks / scoped-cancellation
            // handling that the normal signal path (`SignalAction::Cancel` in
            // `signals.rs`) applies. `enqueue_signal_if_active` also closes
            // the TOCTOU between the `active` read above and this write — an
            // instance that finished in between is simply not signalled.
            for instance_id in &active {
                let signal = orch8_types::signal::Signal {
                    id: uuid::Uuid::now_v7(),
                    instance_id: *instance_id,
                    signal_type: orch8_types::signal::SignalType::Cancel,
                    payload: serde_json::json!({}),
                    delivered: false,
                    created_at: now,
                    delivered_at: None,
                };
                if let Err(e) = storage.enqueue_signal_if_active(&signal).await {
                    warn!(
                        cron_id = %schedule.id,
                        instance_id = %instance_id,
                        error = %e,
                        "cancel_previous: failed to signal-cancel still-active run"
                    );
                }
            }
            info!(
                cron_id = %schedule.id,
                cancelled = active.len(),
                "cancel_previous: signalled still-active runs to cancel before firing"
            );
            Ok(true)
        }
        // `Allow` returned early above; future policies (the enum is
        // non_exhaustive) default to firing.
        _ => Ok(true),
    }
}

async fn trigger_cron_schedule(
    storage: &dyn StorageBackend,
    schedule: &CronSchedule,
    clock: &SharedClock,
) -> Result<(), EngineError> {
    let now = clock.now();

    // Overlap policy gate: skip/defer when a previous run is still active.
    if !apply_overlap_policy(storage, schedule, now).await? {
        return Ok(());
    }

    // Create a new instance for this schedule.
    //
    // `idempotency_key` binds this instance to the exact (schedule, fire_at)
    // pair: `create_instance` then `update_cron_fire_times` are two separate
    // writes, and a crash between them would otherwise re-fire the same tick
    // on the next scheduler pass (fire time never advanced) and create a
    // duplicate instance. With the key set, the retry's `create_instance`
    // hits the existing unique-index violation instead of duplicating —
    // surfaced below as `StorageError::Conflict`.
    //
    // Keyed on `schedule.next_fire_at` (the due timestamp this schedule was
    // *claimed* at), not `now` (wall-clock at execution time): a crash-retry
    // runs on a later tick, so `now` would differ between the original
    // attempt and the retry and the key would never collide, silently
    // defeating the whole point of setting it.
    let idempotency_key = schedule
        .next_fire_at
        .map(|fire_at| format!("cron:{}:{}", schedule.id, fire_at.to_rfc3339()));
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: schedule.sequence_id,
        tenant_id: schedule.tenant_id.clone(),
        namespace: schedule.namespace.clone(),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: schedule.timezone.clone(),
        metadata: stamp_cron_id(schedule.metadata.clone(), schedule.id),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key,
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };

    match storage.create_instance(&instance).await {
        Ok(()) => {
            info!(
                cron_id = %schedule.id,
                instance_id = %instance.id,
                sequence_id = %schedule.sequence_id,
                "cron triggered new instance"
            );
        }
        Err(StorageError::Conflict(_)) => {
            // A crash between `create_instance` and `update_cron_fire_times`
            // on a previous attempt at this exact fire — the instance for
            // this tick already exists. Nothing to do but advance the fire
            // time below.
            info!(
                cron_id = %schedule.id,
                sequence_id = %schedule.sequence_id,
                "cron instance for this fire already exists (crash-recovery retry), not duplicating"
            );
        }
        Err(e) => {
            // The referenced sequence may have been deleted between the cron
            // schedule creation and this tick. Log and advance the fire time
            // so the schedule doesn't retry every tick.
            warn!(
                cron_id = %schedule.id,
                sequence_id = %schedule.sequence_id,
                error = %e,
                "cron failed to create instance (sequence may have been deleted)"
            );
        }
    }

    // Calculate next fire time — always advance even on failure so we don't
    // retry the same tick in a hot error loop.
    let next = calculate_next_fire_after(schedule, now);
    match next {
        Some(next_at) => {
            storage
                .update_cron_fire_times(schedule.id, now, next_at)
                .await?;
            debug!(
                cron_id = %schedule.id,
                next_fire_at = %next_at,
                "cron schedule advanced"
            );
        }
        None => {
            warn!(
                cron_id = %schedule.id,
                cron_expr = %schedule.cron_expr,
                "could not calculate next fire time for cron expression"
            );
        }
    }

    crate::metrics::inc(crate::metrics::CRON_TRIGGERED);

    Ok(())
}

/// Normalize a cron expression so that standard 5-field Unix cron
/// (`m h dom mon dow`) is accepted alongside the `cron` crate's native
/// 6-field (`s m h dom mon dow`) and 7-field (`s m h dom mon dow year`)
/// formats. Other shapes are returned unchanged and passed through to the
/// underlying parser (which will reject them with a useful error).
fn normalize_cron_expr(expr: &str) -> String {
    let trimmed = expr.trim();
    let fields: Vec<&str> = trimmed.split_whitespace().collect();
    match fields.len() {
        // Standard Unix cron: prepend "0" seconds, append "*" year.
        5 => format!("0 {} *", fields.join(" ")),
        _ => trimmed.to_string(),
    }
}

/// Calculate the next fire time from a cron expression after the real current
/// time, respecting the schedule's timezone. Falls back to UTC if the
/// timezone is invalid.
pub fn calculate_next_fire(schedule: &CronSchedule) -> Option<chrono::DateTime<Utc>> {
    calculate_next_fire_after(schedule, Utc::now())
}

/// [`calculate_next_fire`] evaluated against an explicit "now" — the cron
/// loop passes its scheduler clock's instant so virtual time controls
/// schedule advancement in tests.
///
/// ## DST semantics (explicit, tested in `tests/cron_dst.rs`)
///
/// - **Nonexistent local time** (spring forward — e.g. a 02:30 schedule in
///   `America/New_York` on the day 02:00 jumps to 03:00): the occurrence
///   fires at the **first valid instant after the gap** (03:00) instead of
///   silently skipping the day. The underlying cron iterator skips
///   nonexistent wall times entirely; we detect the gap and clamp.
/// - **Ambiguous local time** (fall back — e.g. a 01:30 schedule on the day
///   01:59 rewinds to 01:00): the occurrence fires **once**, at the first
///   (pre-transition) occurrence. No double fire.
pub fn calculate_next_fire_after(
    schedule: &CronSchedule,
    now: DateTime<Utc>,
) -> Option<chrono::DateTime<Utc>> {
    use chrono::{LocalResult, Offset, TimeZone};

    let normalized = normalize_cron_expr(&schedule.cron_expr);
    let cron_schedule = Schedule::from_str(&normalized).ok()?;
    let Ok(tz) = schedule.timezone.parse::<chrono_tz::Tz>() else {
        return cron_schedule.after(&now).next();
    };

    let now_tz = now.with_timezone(&tz);
    let next = cron_schedule
        .after(&now_tz)
        .next()
        .map(|dt| dt.with_timezone(&Utc));

    // DST-gap detection: re-evaluate the schedule against a fixed offset
    // frozen at "now". That iterator advances in pure wall-clock terms, so
    // its first candidate is the next *wall time* the expression matches —
    // including wall times the real timezone skips. If that candidate does
    // not exist in the real timezone, the occurrence fell inside a DST gap:
    // fire at the first valid instant after the gap rather than losing the
    // occurrence (the failure mode documented as unfixed in Temporal #8205).
    let now_fixed = now.with_timezone(&now_tz.offset().fix());
    if let Some(candidate) = cron_schedule.after(&now_fixed).next() {
        let wall = candidate.naive_local();
        if matches!(tz.from_local_datetime(&wall), LocalResult::None) {
            // Probe forward minute-by-minute for the gap's end. DST gaps are
            // 30–120 minutes in practice; 48h bounds pathological zones.
            let mut probe = wall;
            for _ in 0..(48 * 60) {
                probe += chrono::Duration::minutes(1);
                match tz.from_local_datetime(&probe) {
                    LocalResult::Single(dt) | LocalResult::Ambiguous(dt, _) => {
                        let gap_end = dt.with_timezone(&Utc);
                        // Keep the regular result if a valid occurrence
                        // happens before the gap closes.
                        return match next {
                            Some(n) if n <= gap_end => Some(n),
                            _ => Some(gap_end),
                        };
                    }
                    LocalResult::None => {}
                }
            }
        }
    }

    next
}

/// Validate a cron expression. Returns an error message if invalid.
pub fn validate_cron_expr(expr: &str) -> Result<(), String> {
    let normalized = normalize_cron_expr(expr);
    Schedule::from_str(&normalized)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Next fire time (UTC) for a bare cron expression, accepting the same
/// 5/6/7-field shapes as [`validate_cron_expr`]. Returns `None` if the
/// expression is invalid or never fires again. Used by schedule consumers
/// that don't carry a full [`CronSchedule`] row (e.g. polling triggers).
pub fn next_fire_from_expr(expr: &str) -> Option<chrono::DateTime<Utc>> {
    let normalized = normalize_cron_expr(expr);
    Schedule::from_str(&normalized).ok()?.upcoming(Utc).next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::ids::{Namespace, SequenceId, TenantId};

    #[test]
    fn valid_cron_expressions() {
        assert!(validate_cron_expr("0 0 9 * * MON-FRI *").is_ok());
        assert!(validate_cron_expr("0 */5 * * * * *").is_ok());
        assert!(validate_cron_expr("0 0 0 1 * * *").is_ok());
    }

    #[test]
    fn invalid_cron_expressions() {
        assert!(validate_cron_expr("not a cron").is_err());
        assert!(validate_cron_expr("").is_err());
    }

    #[test]
    fn next_fire_calculation() {
        let schedule = CronSchedule {
            id: uuid::Uuid::now_v7(),
            tenant_id: TenantId::unchecked("test"),
            namespace: Namespace::new("default"),
            sequence_id: SequenceId::new(),
            cron_expr: "0 * * * * * *".into(), // every minute
            timezone: "UTC".into(),
            enabled: true,
            metadata: serde_json::json!({}),
            overlap_policy: orch8_types::cron::OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let next = calculate_next_fire(&schedule);
        assert!(next.is_some());
        assert!(next.unwrap() > Utc::now());
    }

    fn mk_schedule(expr: &str) -> CronSchedule {
        CronSchedule {
            id: uuid::Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            sequence_id: SequenceId::new(),
            cron_expr: expr.into(),
            timezone: "UTC".into(),
            enabled: true,
            metadata: serde_json::json!({}),
            overlap_policy: orch8_types::cron::OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn valid_expr_with_weekday_and_seconds() {
        assert!(validate_cron_expr("0 30 9 * * MON *").is_ok());
        assert!(validate_cron_expr("0 0 12 * * SUN *").is_ok());
    }

    #[test]
    fn standard_unix_cron_is_accepted() {
        // We normalise 5-field standard Unix cron (`m h dom mon dow`) into the
        // `cron` crate's 7-field form, so these must parse.
        assert!(validate_cron_expr("* * * * *").is_ok());
        assert!(validate_cron_expr("0 0 * * *").is_ok());
    }

    #[test]
    fn invalid_expr_with_out_of_range_minute() {
        assert!(validate_cron_expr("0 99 * * * * *").is_err());
    }

    #[test]
    fn invalid_expr_with_gibberish_tokens() {
        assert!(validate_cron_expr("banana * * * * * *").is_err());
    }

    #[test]
    fn calculate_next_fire_returns_none_for_bad_expression() {
        let schedule = mk_schedule("not a cron expr");
        assert!(calculate_next_fire(&schedule).is_none());
    }

    #[test]
    fn calculate_next_fire_every_second_is_within_two_seconds() {
        // "* * * * * * *" = every second of every year
        let schedule = mk_schedule("* * * * * * *");
        let next = calculate_next_fire(&schedule).expect("next fire time");
        let delta = (next - Utc::now()).num_milliseconds();
        assert!(
            (0..=2_000).contains(&delta),
            "expected next fire within 2s, got {delta}ms",
        );
    }

    #[test]
    fn hourly_expression_fires_at_top_of_hour_within_one_hour() {
        use chrono::Timelike;
        // Standard 5-field Unix cron "0 * * * *" = at minute 0 of every hour.
        let schedule = mk_schedule("0 * * * *");
        let next = calculate_next_fire(&schedule).expect("next fire time");
        assert_eq!(next.minute(), 0);
        assert_eq!(next.second(), 0);
        let delta = (next - Utc::now()).num_seconds();
        assert!(
            (0..=3600).contains(&delta),
            "expected next fire within 1 hour, got {delta}s",
        );
    }

    #[test]
    fn invalid_cron_expr_rejected_at_validation() {
        // validate_cron_expr is the gate used when creating a schedule.
        let err = validate_cron_expr("nonsense").unwrap_err();
        assert!(!err.is_empty());
        let err = validate_cron_expr("60 0 0 1 1 * *").unwrap_err();
        assert!(!err.is_empty());
    }

    #[test]
    fn five_field_unix_cron_normalizes_to_seven_fields() {
        use chrono::{Datelike, Timelike, Weekday};
        // "0 9 * * MON-FRI" (5-field Unix) must be accepted, since it is
        // normalized to "0 0 9 * * MON-FRI *".
        assert!(validate_cron_expr("0 9 * * MON-FRI").is_ok());
        let schedule = mk_schedule("0 9 * * MON-FRI");
        let next = calculate_next_fire(&schedule).expect("next fire time");
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
        assert!(!matches!(next.weekday(), Weekday::Sat | Weekday::Sun));
    }

    #[test]
    fn invalid_timezone_falls_back_to_utc_instead_of_none() {
        // Documented contract: an unparseable IANA timezone name must not
        // make the schedule stop firing — it silently degrades to UTC.
        let mut schedule = mk_schedule("0 * * * * * *");
        schedule.timezone = "Not/A_Real_Zone".into();
        let next = calculate_next_fire(&schedule);
        assert!(
            next.is_some(),
            "invalid timezone must fall back to UTC, not disable the schedule"
        );
    }

    #[test]
    fn empty_and_whitespace_only_cron_expr_rejected() {
        assert!(validate_cron_expr("   ").is_err());
        assert!(calculate_next_fire(&mk_schedule("")).is_none());
        assert!(calculate_next_fire(&mk_schedule("   ")).is_none());
    }

    #[test]
    fn calculate_next_fire_daily_at_midnight() {
        use chrono::Timelike;
        // Every day at 00:00:00 — next fire must be at a midnight in UTC.
        let schedule = mk_schedule("0 0 0 * * * *");
        let next = calculate_next_fire(&schedule).expect("next fire time");
        assert_eq!(next.hour(), 0);
        assert_eq!(next.minute(), 0);
        assert_eq!(next.second(), 0);
    }
}
