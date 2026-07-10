// Trait impl methods are exempt from `too_many_arguments`/`type_complexity`
// (clippy skips lints on a signature the impl doesn't control), but these
// free functions -- extracted from what used to be trait impl bodies -- are
// not, so the allows that used to be implicit must be explicit here.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;

use super::PostgresStorage;

pub(super) async fn create_rollback_policy(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    error_rate_threshold: f64,
    time_window_secs: i32,
    cooldown_secs: Option<i32>,
    confirmation_window_secs: Option<i32>,
    webhook_url: Option<&str>,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO rollback_policies (tenant_id, sequence_name, error_rate_threshold, time_window_secs, cooldown_secs, confirmation_window_secs, webhook_url)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (tenant_id, sequence_name) DO UPDATE SET
           error_rate_threshold = EXCLUDED.error_rate_threshold,
           time_window_secs = EXCLUDED.time_window_secs,
           cooldown_secs = EXCLUDED.cooldown_secs,
           confirmation_window_secs = EXCLUDED.confirmation_window_secs,
           webhook_url = EXCLUDED.webhook_url,
           enabled = 1,
           updated_at = NOW()"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(error_rate_threshold)
    .bind(time_window_secs)
    .bind(cooldown_secs.unwrap_or(3600))
    .bind(confirmation_window_secs.unwrap_or(60))
    .bind(webhook_url)
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn get_rollback_policy(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError> {
    let row: Option<(i64, String, String, f64, i32, bool, i32, i32, Option<String>, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, created_at, updated_at FROM rollback_policies WHERE tenant_id = $1 AND sequence_name = $2"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .fetch_optional(&storage.pool)
    .await?;
    Ok(row.map(
        |(
            id,
            tenant_id,
            sequence_name,
            error_rate_threshold,
            time_window_secs,
            enabled,
            cooldown_secs,
            confirmation_window_secs,
            webhook_url,
            created_at,
            updated_at,
        )| {
            orch8_types::rollback::RollbackPolicy {
                id,
                tenant_id,
                sequence_name,
                error_rate_threshold,
                time_window_secs,
                enabled,
                cooldown_secs,
                confirmation_window_secs,
                webhook_url,
                created_at,
                updated_at,
            }
        },
    ))
}

pub(super) async fn list_rollback_policies(
    storage: &PostgresStorage,
    tenant_id: Option<&str>,
    limit: u32,
) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError> {
    let rows: Vec<(
        i64,
        String,
        String,
        f64,
        i32,
        bool,
        i32,
        i32,
        Option<String>,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = if let Some(t) = tenant_id {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, created_at, updated_at FROM rollback_policies WHERE tenant_id = $1 LIMIT $2"
        )
        .bind(t)
        .bind(i64::from(limit))
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, created_at, updated_at FROM rollback_policies LIMIT $1"
        )
        .bind(i64::from(limit))
        .fetch_all(&storage.pool)
        .await?
    };
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                tenant_id,
                sequence_name,
                error_rate_threshold,
                time_window_secs,
                enabled,
                cooldown_secs,
                confirmation_window_secs,
                webhook_url,
                created_at,
                updated_at,
            )| {
                orch8_types::rollback::RollbackPolicy {
                    id,
                    tenant_id,
                    sequence_name,
                    error_rate_threshold,
                    time_window_secs,
                    enabled,
                    cooldown_secs,
                    confirmation_window_secs,
                    webhook_url,
                    created_at,
                    updated_at,
                }
            },
        )
        .collect())
}

pub(super) async fn delete_rollback_policy(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM rollback_policies WHERE tenant_id = $1 AND sequence_name = $2")
        .bind(tenant_id)
        .bind(sequence_name)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn record_rollback(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    error_rate: f64,
    threshold: f64,
    reason: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO rollback_history (tenant_id, sequence_name, error_rate, threshold, reason) VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(error_rate)
    .bind(threshold)
    .bind(reason)
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn query_error_rate(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    window_secs: i64,
) -> Result<Option<f64>, StorageError> {
    let start = Utc::now() - chrono::Duration::seconds(window_secs);
    let row: Option<(i64, i64)> = sqlx::query_as(
        r"SELECT
           COUNT(*) FILTER (WHERE event_type = 'InstanceFailed') as failed,
           COUNT(*) as total
         FROM telemetry_mobile_events
         WHERE tenant_id = $1
           AND payload->>'sequence_name' = $2
           AND created_at >= $3",
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(start)
    .fetch_optional(&storage.pool)
    .await?;
    Ok(row.and_then(|(failed, total)| {
        if total > 0 {
            #[allow(clippy::cast_precision_loss)]
            Some(failed as f64 / total as f64)
        } else {
            None
        }
    }))
}

pub(super) async fn list_rollback_history(
    storage: &PostgresStorage,
    tenant_id: Option<&str>,
    sequence_name: Option<&str>,
    limit: u32,
) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError> {
    use std::fmt::Write;
    let mut query = String::from(
        "SELECT id, tenant_id, sequence_name, triggered_at, error_rate, threshold, previous_manifest_version, reason, alert_sent FROM rollback_history WHERE 1=1",
    );
    let mut param_idx = 1u32;
    if tenant_id.is_some() {
        let _ = write!(query, " AND tenant_id = ${param_idx}");
        param_idx += 1;
    }
    if sequence_name.is_some() {
        let _ = write!(query, " AND sequence_name = ${param_idx}");
        param_idx += 1;
    }
    let _ = write!(query, " ORDER BY triggered_at DESC LIMIT ${param_idx}");

    let mut q = sqlx::query_as(&query);
    if let Some(t) = tenant_id {
        q = q.bind(t);
    }
    if let Some(s) = sequence_name {
        q = q.bind(s);
    }
    q = q.bind(i64::from(limit));

    let rows: Vec<(
        i64,
        String,
        String,
        DateTime<Utc>,
        f64,
        f64,
        Option<String>,
        String,
        bool,
    )> = q.fetch_all(&storage.pool).await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                tenant_id,
                sequence_name,
                triggered_at,
                error_rate,
                threshold,
                previous_manifest_version,
                reason,
                alert_sent,
            )| {
                orch8_types::rollback::RollbackHistory {
                    id,
                    tenant_id,
                    sequence_name,
                    triggered_at,
                    error_rate,
                    threshold,
                    previous_manifest_version,
                    reason,
                    alert_sent,
                }
            },
        )
        .collect())
}
