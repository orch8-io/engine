use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orch8_push::{ClaimedWake, PushOutboxStore, PushTerminalReason, WakeAttemptOutcome};
use sqlx::Row;
use uuid::Uuid;

use super::SqliteStorage;
use super::helpers::ts;

#[async_trait]
impl PushOutboxStore for SqliteStorage {
    async fn enqueue_wake(
        &self,
        tenant_id: &str,
        device_id: &str,
        command_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<Uuid, String> {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO push_wake_outbox (id,tenant_id,device_id,command_id,created_at) VALUES (?,?,?,?,?) ON CONFLICT(tenant_id,device_id,command_id) DO NOTHING")
            .bind(id.to_string()).bind(tenant_id).bind(device_id).bind(command_id).bind(ts(created_at))
            .execute(&self.pool).await.map_err(|error| error.to_string())?;
        let stored: String = sqlx::query_scalar(
            "SELECT id FROM push_wake_outbox WHERE tenant_id=? AND device_id=? AND command_id=?",
        )
        .bind(tenant_id)
        .bind(device_id)
        .bind(command_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| error.to_string())?;
        Uuid::parse_str(&stored).map_err(|error| error.to_string())
    }

    async fn claim_due_wakes(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ClaimedWake>, String> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .map_err(|error| error.to_string())?;
        let result = async {
            let rows = sqlx::query("SELECT o.id,o.tenant_id,o.device_id,o.command_id,o.attempts,d.push_token,d.platform FROM push_wake_outbox o JOIN mobile_devices d ON d.device_id=o.device_id AND d.tenant_id=o.tenant_id WHERE d.active=1 AND d.push_token IS NOT NULL AND ((o.status='pending' AND (o.next_attempt_at IS NULL OR o.next_attempt_at<=?)) OR (o.status='in_flight' AND o.lease_until<=?)) ORDER BY o.created_at LIMIT ?")
                .bind(ts(now)).bind(ts(now)).bind(limit).fetch_all(&mut *connection).await.map_err(|error| error.to_string())?;
            let wakes = rows.iter().map(|row| Ok(ClaimedWake {
                id: Uuid::parse_str(row.get::<&str,_>("id")).map_err(|error| error.to_string())?,
                tenant_id: row.get("tenant_id"), device_id: row.get("device_id"), command_id: row.get("command_id"),
                push_token: row.get::<String,_>("push_token"), platform: row.get("platform"),
                attempts: u32::try_from(row.get::<i64,_>("attempts")).map_err(|error| error.to_string())?,
                lease_until,
            })).collect::<Result<Vec<_>,String>>()?;
            for wake in &wakes {
                sqlx::query("UPDATE push_wake_outbox SET status='in_flight',lease_until=? WHERE id=?")
                    .bind(ts(lease_until)).bind(wake.id.to_string()).execute(&mut *connection).await.map_err(|error| error.to_string())?;
            }
            Ok::<_,String>(wakes)
        }.await;
        match result {
            Ok(wakes) => {
                sqlx::query("COMMIT")
                    .execute(&mut *connection)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(wakes)
            }
            Err(error) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;
                Err(error)
            }
        }
    }

    async fn record_wake_outcome(
        &self,
        wake: &ClaimedWake,
        outcome: &WakeAttemptOutcome,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), String> {
        let (status, next, error, reason, delivered) = outcome_fields(outcome, recorded_at);
        let result = sqlx::query("UPDATE push_wake_outbox SET attempts=attempts+1,status=?,next_attempt_at=?,lease_until=NULL,last_error=?,terminal_reason=?,delivered_at=? WHERE id=? AND status='in_flight' AND lease_until=?")
            .bind(status).bind(next.map(ts)).bind(error).bind(reason).bind(delivered.map(ts)).bind(wake.id.to_string())
            .bind(ts(wake.lease_until))
            .execute(&self.pool).await.map_err(|error| error.to_string())?;
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
        let mut builder = sqlx::QueryBuilder::new("UPDATE push_wake_outbox SET command_acked_at=");
        builder
            .push_bind(ts(acked_at))
            .push(" WHERE device_id=")
            .push_bind(device_id)
            .push(" AND command_id IN (");
        let mut separated = builder.separated(",");
        for id in command_ids {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        builder
            .build()
            .execute(&self.pool)
            .await
            .map(|result| result.rows_affected())
            .map_err(|error| error.to_string())
    }
}

type OutcomeFields<'a> = (
    &'static str,
    Option<DateTime<Utc>>,
    Option<&'a str>,
    Option<&'static str>,
    Option<DateTime<Utc>>,
);

fn outcome_fields(outcome: &WakeAttemptOutcome, now: DateTime<Utc>) -> OutcomeFields<'_> {
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

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use orch8_push::PushOutboxStore;

    use super::*;
    use crate::{MobileCommand, MobileDevice, MobileSyncStore};

    async fn storage_with_device() -> SqliteStorage {
        let storage = SqliteStorage::in_memory().await.unwrap();
        storage
            .register_mobile_device(&MobileDevice {
                device_id: "device-a".into(),
                tenant_id: "tenant-a".into(),
                push_token: Some("token-a".into()),
                platform: "ios".into(),
                app_version: None,
                active: true,
                last_sync_at: None,
                registered_at: String::new(),
            })
            .await
            .unwrap();
        storage
    }

    fn command(id: &str) -> MobileCommand {
        MobileCommand {
            id: id.into(),
            device_id: "device-a".into(),
            command_type: "resume".into(),
            payload: "{}".into(),
            created_at: String::new(),
            acked_at: None,
        }
    }

    #[tokio::test]
    async fn atomic_command_wake_claim_retry_delivery_and_ack() {
        let storage = storage_with_device().await;
        let now = Utc::now();
        storage
            .create_mobile_command_with_wake(&command("command-a"), "tenant-a", now)
            .await
            .unwrap();

        let wakes = storage
            .claim_due_wakes(now, now + Duration::seconds(30), 10)
            .await
            .unwrap();
        assert_eq!(wakes.len(), 1);
        assert!(
            storage
                .claim_due_wakes(now, now + Duration::seconds(30), 10)
                .await
                .unwrap()
                .is_empty()
        );

        let retry_at = now + Duration::minutes(1);
        storage
            .record_wake_outcome(
                &wakes[0],
                &WakeAttemptOutcome::Retry {
                    next_attempt_at: retry_at,
                    error: "provider busy".into(),
                },
                now,
            )
            .await
            .unwrap();
        assert!(
            storage
                .claim_due_wakes(now, now + Duration::seconds(30), 10)
                .await
                .unwrap()
                .is_empty()
        );
        let retried = storage
            .claim_due_wakes(retry_at, retry_at + Duration::seconds(30), 10)
            .await
            .unwrap();
        assert_eq!(retried[0].attempts, 1);
        storage
            .record_wake_outcome(&retried[0], &WakeAttemptOutcome::Delivered, retry_at)
            .await
            .unwrap();
        storage
            .record_command_acks("device-a", &["command-a".into()], retry_at)
            .await
            .unwrap();

        let row: (String, i64, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT status,attempts,delivered_at,command_acked_at FROM push_wake_outbox",
        )
        .fetch_one(&storage.pool)
        .await
        .unwrap();
        assert_eq!(row.0, "delivered");
        assert_eq!(row.1, 2);
        assert!(row.2.is_some() && row.3.is_some());
    }

    #[tokio::test]
    async fn failed_command_insert_rolls_back_wake() {
        let storage = storage_with_device().await;
        let command = command("duplicate");
        storage.create_mobile_command(&command).await.unwrap();
        assert!(
            storage
                .create_mobile_command_with_wake(&command, "tenant-a", Utc::now())
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM push_wake_outbox")
            .fetch_one(&storage.pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn expired_claim_cannot_overwrite_newer_lease() {
        let storage = storage_with_device().await;
        let now = Utc::now();
        storage
            .create_mobile_command_with_wake(&command("leased"), "tenant-a", now)
            .await
            .unwrap();
        let first = storage
            .claim_due_wakes(now, now + Duration::seconds(30), 1)
            .await
            .unwrap()
            .remove(0);
        let reclaimed_at = now + Duration::seconds(31);
        let second = storage
            .claim_due_wakes(reclaimed_at, reclaimed_at + Duration::seconds(30), 1)
            .await
            .unwrap()
            .remove(0);

        assert!(
            storage
                .record_wake_outcome(&first, &WakeAttemptOutcome::Delivered, reclaimed_at)
                .await
                .is_err()
        );
        storage
            .record_wake_outcome(&second, &WakeAttemptOutcome::Delivered, reclaimed_at)
            .await
            .unwrap();
    }
}
