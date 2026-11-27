// Trait impl methods are exempt from `too_many_arguments`/`type_complexity`
// (clippy skips lints on a signature the impl doesn't control), but these
// free functions -- extracted from what used to be trait impl bodies -- are
// not, so the allows that used to be implicit must be explicit here.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use chrono::{DateTime, Utc};
use sqlx::AssertSqlSafe;

use orch8_types::error::StorageError;
use orch8_types::rollback::{DEFAULT_MIN_SAMPLES, RollbackPolicy};

use super::PostgresStorage;

pub(super) async fn save_rollback_policy(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    update: crate::RollbackPolicyUpdate<'_>,
) -> Result<RollbackPolicy, StorageError> {
    let mut tx = storage.pool.begin().await?;
    sqlx::query(
        "INSERT INTO rollback_policies (
             tenant_id, sequence_name, error_rate_threshold, time_window_secs,
             cooldown_secs, confirmation_window_secs, webhook_url, min_samples
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8::integer, $9))
         ON CONFLICT (tenant_id, sequence_name) DO UPDATE SET
           error_rate_threshold = excluded.error_rate_threshold,
           time_window_secs = excluded.time_window_secs,
           cooldown_secs = excluded.cooldown_secs,
           confirmation_window_secs = excluded.confirmation_window_secs,
           webhook_url = excluded.webhook_url,
           min_samples = COALESCE($8, rollback_policies.min_samples),
           enabled = TRUE,
           updated_at = NOW()",
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(update.error_rate_threshold)
    .bind(update.time_window_secs)
    .bind(update.cooldown_secs.unwrap_or(3600))
    .bind(update.confirmation_window_secs.unwrap_or(60))
    .bind(update.webhook_url)
    .bind(update.min_samples)
    .bind(DEFAULT_MIN_SAMPLES)
    .execute(&mut *tx)
    .await?;

    let policy = read_rollback_policy(&mut *tx, tenant_id, sequence_name)
        .await?
        .ok_or_else(|| StorageError::Query("rollback policy missing after save".into()))?;
    tx.commit().await?;
    Ok(policy)
}

pub(super) async fn get_rollback_policy(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<Option<RollbackPolicy>, StorageError> {
    read_rollback_policy(&storage.pool, tenant_id, sequence_name).await
}

async fn read_rollback_policy<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<Option<RollbackPolicy>, StorageError> {
    let row: Option<(i64, String, String, f64, i32, bool, i32, i32, Option<String>, i32, DateTime<Utc>, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, min_samples, created_at, updated_at FROM rollback_policies WHERE tenant_id = $1 AND sequence_name = $2"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .fetch_optional(executor)
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
            min_samples,
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
                min_samples,
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
        i32,
        DateTime<Utc>,
        DateTime<Utc>,
    )> = if let Some(t) = tenant_id {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, min_samples, created_at, updated_at FROM rollback_policies WHERE tenant_id = $1 ORDER BY tenant_id, sequence_name LIMIT $2"
        )
        .bind(t)
        .bind(i64::from(limit))
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, COALESCE(cooldown_secs, 3600), COALESCE(confirmation_window_secs, 60), webhook_url, min_samples, created_at, updated_at FROM rollback_policies ORDER BY tenant_id, sequence_name LIMIT $1"
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
                min_samples,
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
                    min_samples,
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

pub(super) async fn set_rollback_min_samples(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    min_samples: i32,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE rollback_policies SET min_samples = $3, updated_at = NOW() WHERE tenant_id = $1 AND sequence_name = $2",
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(min_samples)
    .execute(&storage.pool)
    .await?;
    Ok(())
}

/// `(failed, total)` run counts for a sequence inside the window.
///
/// The window is evaluated against the **server** receive time
/// (`received_at`), never the client-supplied `created_at`, so a device cannot
/// back-date or future-date reports into (or out of) the window. Error
/// reports (`ingest_telemetry_error`) land in `telemetry_mobile_errors`; each
/// counts once as a failed run, matching `SQLite`, which stores them as
/// `InstanceFailed` telemetry events. Served by the composite indexes from
/// migration 118 (`tenant_id, (payload->>'sequence_name'), received_at` and
/// `tenant_id, sequence_name, received_at`).
pub(super) async fn query_error_stats(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    window_secs: i64,
) -> Result<(i64, i64), StorageError> {
    let start = Utc::now() - chrono::Duration::seconds(window_secs);
    let row: (i64, i64) = sqlx::query_as(
        r"WITH ev AS (
           SELECT
             COUNT(*) FILTER (WHERE event_type = 'InstanceFailed') AS failed,
             COUNT(*) AS total
           FROM telemetry_mobile_events
           WHERE tenant_id = $1
             AND payload->>'sequence_name' = $2
             AND received_at >= $3
         ), er AS (
           SELECT COUNT(*) AS n
           FROM telemetry_mobile_errors
           WHERE tenant_id = $1
             AND sequence_name = $2
             AND received_at >= $3
         )
         SELECT ev.failed + er.n, ev.total + er.n FROM ev, er",
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(start)
    .fetch_one(&storage.pool)
    .await?;
    Ok(row)
}

pub(super) async fn query_error_rate(
    storage: &PostgresStorage,
    tenant_id: &str,
    sequence_name: &str,
    window_secs: i64,
) -> Result<Option<f64>, StorageError> {
    let (failed, total) = query_error_stats(storage, tenant_id, sequence_name, window_secs).await?;
    Ok(rate_from(failed, total))
}

fn rate_from(failed: i64, total: i64) -> Option<f64> {
    if total > 0 {
        #[allow(clippy::cast_precision_loss)]
        Some(failed as f64 / total as f64)
    } else {
        None
    }
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
    let _ = write!(
        query,
        " ORDER BY triggered_at DESC, id DESC LIMIT ${param_idx}"
    );

    let mut q = sqlx::query_as(AssertSqlSafe(query.as_str()));
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
