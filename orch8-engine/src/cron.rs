use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
use tokio::task::JoinSet;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::context::ExecutionContext;
use orch8_types::cron::CronSchedule;
use orch8_types::ids::InstanceId;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};

use crate::error::EngineError;

/// Run the cron tick loop. Checks every `tick_interval` for due cron schedules,
/// creates instances for each, and advances their `next_fire_at`.
pub async fn run_cron_loop(
    storage: Arc<dyn StorageBackend>,
    tick_interval: Duration,
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
                if let Err(e) = process_cron_tick(&storage).await {
                    error!(error = %e, "cron tick failed");
                }
            }
        }
    }
}

async fn process_cron_tick(storage: &Arc<dyn StorageBackend>) -> Result<(), EngineError> {
    let now = Utc::now();
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
        tasks.spawn(async move {
            let cron_id = schedule.id;
            if let Err(e) = trigger_cron_schedule(storage.as_ref(), &schedule).await {
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

async fn trigger_cron_schedule(
    storage: &dyn StorageBackend,
    schedule: &CronSchedule,
) -> Result<(), EngineError> {
    let now = Utc::now();

    // Create a new instance for this schedule.
    let instance = TaskInstance {
        id: InstanceId::new(),
        sequence_id: schedule.sequence_id,
        tenant_id: schedule.tenant_id.clone(),
        namespace: schedule.namespace.clone(),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: schedule.timezone.clone(),
        metadata: schedule.metadata.clone(),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: None,
        created_at: now,
        updated_at: now,
    };

    storage.create_instance(&instance).await?;

    info!(
        cron_id = %schedule.id,
        instance_id = %instance.id,
        sequence_id = %schedule.sequence_id,
        "cron triggered new instance"
    );

    // Calculate next fire time.
    let next = calculate_next_fire(schedule);
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

/// Calculate the next fire time from a cron expression.
pub fn calculate_next_fire(schedule: &CronSchedule) -> Option<chrono::DateTime<Utc>> {
    let normalized = normalize_cron_expr(&schedule.cron_expr);
    let cron_schedule = Schedule::from_str(&normalized).ok()?;
    cron_schedule.upcoming(Utc).next()
}

/// Validate a cron expression. Returns an error message if invalid.
pub fn validate_cron_expr(expr: &str) -> Result<(), String> {
    let normalized = normalize_cron_expr(expr);
    Schedule::from_str(&normalized)
        .map(|_| ())
        .map_err(|e| e.to_string())
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
            tenant_id: TenantId("test".into()),
            namespace: Namespace("default".into()),
            sequence_id: SequenceId(uuid::Uuid::now_v7()),
            cron_expr: "0 * * * * * *".into(), // every minute
            timezone: "UTC".into(),
            enabled: true,
            metadata: serde_json::json!({}),
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
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            sequence_id: SequenceId(uuid::Uuid::now_v7()),
            cron_expr: expr.into(),
            timezone: "UTC".into(),
            enabled: true,
            metadata: serde_json::json!({}),
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
