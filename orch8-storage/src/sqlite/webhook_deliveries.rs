//! Webhook delivery attempt history (delivery inspector) — SQLite.

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_delivery::{
    DeliveryErrorClass, DeliveryFilter, WebhookDeliveryAttempt, WebhookDeliverySummary,
};

use super::SqliteStorage;
use super::helpers::{parse_ts, ts};

fn class_to_str(class: Option<DeliveryErrorClass>) -> Option<&'static str> {
    class.map(DeliveryErrorClass::as_str)
}

fn class_from_str(s: Option<&str>) -> Option<DeliveryErrorClass> {
    s.and_then(|s| serde_json::from_value(serde_json::Value::String(s.to_string())).ok())
}

fn parse_uuid(s: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(s).map_err(|e| StorageError::Query(e.to_string()))
}

fn row_to_attempt(row: &sqlx::sqlite::SqliteRow) -> Result<WebhookDeliveryAttempt, StorageError> {
    let instance_id: Option<String> = row.get("instance_id");
    Ok(WebhookDeliveryAttempt {
        id: parse_uuid(row.get::<&str, _>("id"))?,
        delivery_id: parse_uuid(row.get::<&str, _>("delivery_id"))?,
        url: row.get("url"),
        event_type: row.get("event_type"),
        instance_id: instance_id.as_deref().and_then(|s| Uuid::parse_str(s).ok()),
        attempt_number: row.get("attempt_number"),
        attempted_at: parse_ts(row.get::<&str, _>("attempted_at"))?,
        duration_ms: row.get("duration_ms"),
        success: row.get::<i64, _>("success") != 0,
        status_code: row.get("status_code"),
        error_class: class_from_str(row.get::<Option<&str>, _>("error_class")),
        error_excerpt: row.get("error_excerpt"),
        signed: row.get::<i64, _>("signed") != 0,
    })
}

pub(super) async fn record(
    storage: &SqliteStorage,
    attempt: &WebhookDeliveryAttempt,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO webhook_delivery_attempts
            (id, delivery_id, url, event_type, instance_id, attempt_number,
             attempted_at, duration_ms, success, status_code, error_class,
             error_excerpt, signed)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
    )
    .bind(attempt.id.to_string())
    .bind(attempt.delivery_id.to_string())
    .bind(&attempt.url)
    .bind(&attempt.event_type)
    .bind(attempt.instance_id.map(|u| u.to_string()))
    .bind(attempt.attempt_number)
    .bind(ts(attempt.attempted_at))
    .bind(attempt.duration_ms)
    .bind(i64::from(attempt.success))
    .bind(attempt.status_code)
    .bind(class_to_str(attempt.error_class))
    .bind(&attempt.error_excerpt)
    .bind(i64::from(attempt.signed))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_deliveries(
    storage: &SqliteStorage,
    filter: &DeliveryFilter,
    limit: u32,
) -> Result<Vec<WebhookDeliverySummary>, StorageError> {
    let rows = sqlx::query(
        "SELECT
            a.delivery_id AS delivery_id,
            MIN(a.url) AS url,
            MIN(a.event_type) AS event_type,
            MIN(a.instance_id) AS instance_id,
            COUNT(*) AS attempts,
            MAX(a.success) AS delivered,
            MIN(a.attempted_at) AS first_attempt_at,
            MAX(a.attempted_at) AS last_attempt_at,
            (SELECT b.error_class FROM webhook_delivery_attempts b
              WHERE b.delivery_id = a.delivery_id
              ORDER BY b.attempt_number DESC LIMIT 1) AS last_error_class
          FROM webhook_delivery_attempts a
          WHERE (?1 IS NULL OR a.url = ?1)
            AND (?2 IS NULL OR a.event_type = ?2)
          GROUP BY a.delivery_id
          HAVING (?3 IS NULL OR MAX(a.success) = ?3)
          ORDER BY MAX(a.attempted_at) DESC
          LIMIT ?4",
    )
    .bind(&filter.url)
    .bind(&filter.event_type)
    .bind(filter.delivered.map(i64::from))
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let instance_id: Option<String> = row.get("instance_id");
        let summary = WebhookDeliverySummary {
            delivery_id: parse_uuid(row.get::<&str, _>("delivery_id"))?,
            url: row.get("url"),
            event_type: row.get("event_type"),
            instance_id: instance_id.as_deref().and_then(|s| Uuid::parse_str(s).ok()),
            attempts: row.get("attempts"),
            delivered: row.get::<i64, _>("delivered") != 0,
            first_attempt_at: parse_ts(row.get::<&str, _>("first_attempt_at"))?,
            last_attempt_at: parse_ts(row.get::<&str, _>("last_attempt_at"))?,
            last_error_class: class_from_str(row.get::<Option<&str>, _>("last_error_class")),
        };
        if let Some(want) = filter.error_class
            && summary.last_error_class != Some(want)
        {
            continue;
        }
        out.push(summary);
    }
    Ok(out)
}

pub(super) async fn get_attempts(
    storage: &SqliteStorage,
    delivery_id: Uuid,
) -> Result<Vec<WebhookDeliveryAttempt>, StorageError> {
    let rows = sqlx::query(
        "SELECT id, delivery_id, url, event_type, instance_id, attempt_number,
                attempted_at, duration_ms, success, status_code, error_class,
                error_excerpt, signed
         FROM webhook_delivery_attempts
         WHERE delivery_id = ?1
         ORDER BY attempt_number ASC",
    )
    .bind(delivery_id.to_string())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_attempt).collect()
}

pub(super) async fn delete_before(
    storage: &SqliteStorage,
    cutoff: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query("DELETE FROM webhook_delivery_attempts WHERE attempted_at < ?1")
        .bind(ts(cutoff))
        .execute(&storage.pool)
        .await?;
    Ok(result.rows_affected())
}
