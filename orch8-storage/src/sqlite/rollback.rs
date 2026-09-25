// Trait impl methods are exempt from `too_many_arguments`/`type_complexity`
// (clippy skips lints on a signature the impl doesn't control), but these
// free functions -- extracted from what used to be trait impl bodies -- are
// not, so the allows that used to be implicit must be explicit here.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use super::SqliteStorage;
use super::helpers::parse_ts;
use orch8_types::error::StorageError;

pub(super) async fn create_rollback_policy(
    storage: &SqliteStorage,
    tenant_id: &str,
    sequence_name: &str,
    error_rate_threshold: f64,
    time_window_secs: i32,
    cooldown_secs: Option<i32>,
    confirmation_window_secs: Option<i32>,
    webhook_url: Option<&str>,
) -> Result<(), StorageError> {
    let cooldown = cooldown_secs.unwrap_or(3600);
    let confirmation = confirmation_window_secs.unwrap_or(60);
    let mut tx = storage.pool.begin().await?;
    sqlx::query(
        "INSERT INTO rollback_policies (tenant_id, sequence_name, error_rate_threshold, time_window_secs, cooldown_secs, confirmation_window_secs, webhook_url)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(tenant_id, sequence_name) DO UPDATE SET
           error_rate_threshold = excluded.error_rate_threshold,
           time_window_secs = excluded.time_window_secs,
           cooldown_secs = excluded.cooldown_secs,
           confirmation_window_secs = excluded.confirmation_window_secs,
           webhook_url = excluded.webhook_url,
           enabled = 1,
           updated_at = datetime('now')"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .bind(error_rate_threshold)
    .bind(time_window_secs)
    .bind(cooldown)
    .bind(confirmation)
    .bind(webhook_url)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub(super) async fn get_rollback_policy(
    storage: &SqliteStorage,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError> {
    let row: Option<(i64, String, String, f64, i32, i32, i32, i32, Option<String>, String, String)> = sqlx::query_as(
        "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
         FROM rollback_policies WHERE tenant_id = ? AND sequence_name = ?"
    )
    .bind(tenant_id)
    .bind(sequence_name)
    .fetch_optional(&storage.pool)
    .await?;
    row.map(
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
        )|
         -> Result<orch8_types::rollback::RollbackPolicy, StorageError> {
            Ok(orch8_types::rollback::RollbackPolicy {
                id,
                tenant_id,
                sequence_name,
                error_rate_threshold,
                time_window_secs,
                enabled: enabled != 0,
                cooldown_secs,
                confirmation_window_secs,
                webhook_url,
                created_at: parse_ts(&created_at)?,
                updated_at: parse_ts(&updated_at)?,
            })
        },
    )
    .transpose()
}

pub(super) async fn list_rollback_policies(
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
    limit: u32,
) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError> {
    let rows: Vec<(
        i64,
        String,
        String,
        f64,
        i32,
        i32,
        i32,
        i32,
        Option<String>,
        String,
        String,
    )> = if let Some(t) = tenant_id {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
             FROM rollback_policies WHERE tenant_id = ? LIMIT ?"
        )
        .bind(t)
        .bind(limit)
        .fetch_all(&storage.pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
             FROM rollback_policies LIMIT ?"
        )
        .bind(limit)
        .fetch_all(&storage.pool)
        .await?
    };
    rows.into_iter()
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
            )|
             -> Result<orch8_types::rollback::RollbackPolicy, StorageError> {
                Ok(orch8_types::rollback::RollbackPolicy {
                    id,
                    tenant_id,
                    sequence_name,
                    error_rate_threshold,
                    time_window_secs,
                    enabled: enabled != 0,
                    cooldown_secs,
                    confirmation_window_secs,
                    webhook_url,
                    created_at: parse_ts(&created_at)?,
                    updated_at: parse_ts(&updated_at)?,
                })
            },
        )
        .collect()
}

pub(super) async fn delete_rollback_policy(
    storage: &SqliteStorage,
    tenant_id: &str,
    sequence_name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM rollback_policies WHERE tenant_id = ? AND sequence_name = ?")
        .bind(tenant_id)
        .bind(sequence_name)
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn record_rollback(
    storage: &SqliteStorage,
    tenant_id: &str,
    sequence_name: &str,
    error_rate: f64,
    threshold: f64,
    reason: &str,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO rollback_history (tenant_id, sequence_name, error_rate, threshold, reason)
         VALUES (?, ?, ?, ?, ?)",
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
    storage: &SqliteStorage,
    tenant_id: &str,
    sequence_name: &str,
    window_secs: i64,
) -> Result<Option<f64>, StorageError> {
    let start = chrono::Utc::now() - chrono::Duration::seconds(window_secs);
    let start_str = start.to_rfc3339();
    let row: Option<(i64, i64)> = sqlx::query_as(
        r"SELECT
           COUNT(*) FILTER (WHERE event_type = 'InstanceFailed') as failed,
           COUNT(*) as total
         FROM telemetry_events
         WHERE created_at >= ?1
           AND json_extract(payload, '$.sequence_name') = ?2
           AND json_extract(payload, '$.tenant_id') = ?3",
    )
    .bind(start_str)
    .bind(sequence_name)
    .bind(tenant_id)
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
    storage: &SqliteStorage,
    tenant_id: Option<&str>,
    sequence_name: Option<&str>,
    limit: u32,
) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError> {
    let mut query = String::from(
        "SELECT id, tenant_id, sequence_name, triggered_at, error_rate, threshold, previous_manifest_version, reason, alert_sent FROM rollback_history WHERE 1=1",
    );
    if tenant_id.is_some() {
        query.push_str(" AND tenant_id = ?");
    }
    if sequence_name.is_some() {
        query.push_str(" AND sequence_name = ?");
    }
    query.push_str(" ORDER BY triggered_at DESC LIMIT ?");

    let mut q = sqlx::query_as(&query);
    if let Some(t) = tenant_id {
        q = q.bind(t);
    }
    if let Some(s) = sequence_name {
        q = q.bind(s);
    }
    q = q.bind(limit);

    let rows: Vec<(
        i64,
        String,
        String,
        String,
        f64,
        f64,
        Option<String>,
        String,
        i32,
    )> = q.fetch_all(&storage.pool).await?;

    rows.into_iter()
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
            )|
             -> Result<orch8_types::rollback::RollbackHistory, StorageError> {
                Ok(orch8_types::rollback::RollbackHistory {
                    id,
                    tenant_id,
                    sequence_name,
                    triggered_at: parse_ts(&triggered_at)?,
                    error_rate,
                    threshold,
                    previous_manifest_version,
                    reason,
                    alert_sent: alert_sent != 0,
                })
            },
        )
        .collect()
}
