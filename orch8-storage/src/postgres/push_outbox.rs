use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orch8_push::{ClaimedWake, PushOutboxStore, PushTerminalReason, WakeAttemptOutcome};
use sqlx::Row;
use uuid::Uuid;

use super::PostgresStorage;

#[async_trait]
impl PushOutboxStore for PostgresStorage {
    async fn enqueue_wake(
        &self,
        tenant_id: &str,
        device_id: &str,
        command_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<Uuid, String> {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO push_wake_outbox (id,tenant_id,device_id,command_id,created_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,device_id,command_id) DO NOTHING")
            .bind(id).bind(tenant_id).bind(device_id).bind(command_id).bind(created_at).execute(&self.pool).await.map_err(|error| error.to_string())?;
        sqlx::query_scalar(
            "SELECT id FROM push_wake_outbox WHERE tenant_id=$1 AND device_id=$2 AND command_id=$3",
        )
        .bind(tenant_id)
        .bind(device_id)
        .bind(command_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| error.to_string())
    }

    async fn claim_due_wakes(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ClaimedWake>, String> {
        let rows = sqlx::query("UPDATE push_wake_outbox SET status='in_flight',lease_until=$2 WHERE id IN (SELECT o.id FROM push_wake_outbox o JOIN mobile_devices d ON d.device_id=o.device_id AND d.tenant_id=o.tenant_id WHERE d.active=TRUE AND d.push_token IS NOT NULL AND ((o.status='pending' AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=$1)) OR (o.status='in_flight' AND o.lease_until<=$1)) ORDER BY o.created_at LIMIT $3 FOR UPDATE SKIP LOCKED) RETURNING id,tenant_id,device_id,command_id,attempts")
            .bind(now).bind(lease_until).bind(i64::from(limit)).fetch_all(&self.pool).await.map_err(|error| error.to_string())?;
        let mut wakes = Vec::with_capacity(rows.len());
        for row in rows {
            let device_id: String = row.get("device_id");
            let target =
                sqlx::query("SELECT push_token,platform FROM mobile_devices WHERE device_id=$1")
                    .bind(&device_id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|error| error.to_string())?;
            wakes.push(ClaimedWake {
                id: row.get("id"),
                tenant_id: row.get("tenant_id"),
                device_id,
                command_id: row.get("command_id"),
                push_token: target.get("push_token"),
                platform: target.get("platform"),
                attempts: u32::try_from(row.get::<i32, _>("attempts"))
                    .map_err(|error| error.to_string())?,
                lease_until,
            });
        }
        Ok(wakes)
    }

    async fn record_wake_outcome(
        &self,
        wake: &ClaimedWake,
        outcome: &WakeAttemptOutcome,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), String> {
        let (status, next, error, reason, delivered) =
            super::push_outbox::outcome_fields(outcome, recorded_at);
        let result = sqlx::query("UPDATE push_wake_outbox SET attempts=attempts+1,status=$2,next_attempt_at=$3,lease_until=NULL,last_error=$4,terminal_reason=$5,delivered_at=$6 WHERE id=$1 AND status='in_flight' AND lease_until=$7")
            .bind(wake.id).bind(status).bind(next).bind(error).bind(reason).bind(delivered).bind(wake.lease_until).execute(&self.pool).await.map_err(|error| error.to_string())?;
        if result.rows_affected() != 1 {
            return Err("push wake lease was lost before outcome persistence".into());
        }
        Ok(())
    }

    async fn record_command_acks(
        &self,
        device_id: &str,
        command_ids: &[String],
        acked_at: DateTime<Utc>,
    ) -> Result<u64, String> {
        if command_ids.is_empty() {
            return Ok(0);
        }
        sqlx::query("UPDATE push_wake_outbox SET command_acked_at=$3 WHERE device_id=$1 AND command_id=ANY($2)")
            .bind(device_id).bind(command_ids).bind(acked_at).execute(&self.pool).await.map(|result| result.rows_affected()).map_err(|error| error.to_string())
    }
}

pub(super) type OutcomeFields<'a> = (
    &'static str,
    Option<DateTime<Utc>>,
    Option<&'a str>,
    Option<&'static str>,
    Option<DateTime<Utc>>,
);

pub(super) fn outcome_fields(
    outcome: &WakeAttemptOutcome,
    now: DateTime<Utc>,
) -> OutcomeFields<'_> {
    match outcome {
        WakeAttemptOutcome::Delivered => ("delivered", None, None, None, Some(now)),
        WakeAttemptOutcome::Retry {
            next_attempt_at,
            error,
        } => ("pending", Some(*next_attempt_at), Some(error), None, None),
        WakeAttemptOutcome::Terminal { reason, error } => (
            "terminal",
            None,
            Some(error),
            Some(reason_name(*reason)),
            None,
        ),
    }
}

fn reason_name(reason: PushTerminalReason) -> &'static str {
    match reason {
        PushTerminalReason::InvalidToken => "invalid_token",
        PushTerminalReason::PermanentFailure => "permanent_failure",
        PushTerminalReason::Misconfigured => "misconfigured",
        PushTerminalReason::RetryLimit => "retry_limit",
    }
}
