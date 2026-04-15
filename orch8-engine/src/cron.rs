use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule;
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

    for schedule in &schedules {
        if let Err(e) = trigger_cron_schedule(storage.as_ref(), schedule).await {
            error!(
                cron_id = %schedule.id,
                error = %e,
                "failed to trigger cron schedule"
            );
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

/// Calculate the next fire time from a cron expression.
pub fn calculate_next_fire(schedule: &CronSchedule) -> Option<chrono::DateTime<Utc>> {
    let cron_schedule = Schedule::from_str(&schedule.cron_expr).ok()?;
    cron_schedule.upcoming(Utc).next()
}

/// Validate a cron expression. Returns an error message if invalid.
pub fn validate_cron_expr(expr: &str) -> Result<(), String> {
    Schedule::from_str(expr).map(|_| ()).map_err(|e| e.to_string())
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
            id: uuid::Uuid::new_v4(),
            tenant_id: TenantId("test".into()),
            namespace: Namespace("default".into()),
            sequence_id: SequenceId(uuid::Uuid::new_v4()),
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
}
