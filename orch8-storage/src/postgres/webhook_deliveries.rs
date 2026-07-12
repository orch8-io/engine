//! Webhook delivery attempt history (delivery inspector).

use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::webhook_delivery::{
    DeliveryErrorClass, DeliveryFilter, WebhookDeliveryAttempt, WebhookDeliverySummary,
};

use super::PostgresStorage;

fn class_to_str(class: Option<DeliveryErrorClass>) -> Option<&'static str> {
    class.map(DeliveryErrorClass::as_str)
}

fn class_from_str(s: Option<&str>) -> Option<DeliveryErrorClass> {
    s.and_then(|s| serde_json::from_value(serde_json::Value::String(s.to_string())).ok())
}

fn row_to_attempt(row: &sqlx::postgres::PgRow) -> WebhookDeliveryAttempt {
    WebhookDeliveryAttempt {
        id: row.get("id"),
        delivery_id: row.get("delivery_id"),
        url: row.get("url"),
        event_type: row.get("event_type"),
        instance_id: row.get("instance_id"),
        attempt_number: row.get("attempt_number"),
        attempted_at: row.get("attempted_at"),
        duration_ms: row.get("duration_ms"),
        success: row.get("success"),
        status_code: row.get("status_code"),
        error_class: class_from_str(row.get::<Option<&str>, _>("error_class")),
        error_excerpt: row.get("error_excerpt"),
        signed: row.get("signed"),
    }
}

pub(super) async fn record(
    store: &PostgresStorage,
    attempt: &WebhookDeliveryAttempt,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO webhook_delivery_attempts
            (id, delivery_id, url, event_type, instance_id, attempt_number,
             attempted_at, duration_ms, success, status_code, error_class,
             error_excerpt, signed)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
    )
    .bind(attempt.id)
    .bind(attempt.delivery_id)
    .bind(&attempt.url)
    .bind(&attempt.event_type)
    .bind(attempt.instance_id)
    .bind(attempt.attempt_number)
    .bind(attempt.attempted_at)
    .bind(attempt.duration_ms)
    .bind(attempt.success)
    .bind(attempt.status_code)
    .bind(class_to_str(attempt.error_class))
    .bind(&attempt.error_excerpt)
    .bind(attempt.signed)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_deliveries(
    store: &PostgresStorage,
    filter: &DeliveryFilter,
    limit: u32,
) -> Result<Vec<WebhookDeliverySummary>, StorageError> {
    // Aggregate per delivery; the final attempt's error class comes from a
    // lateral-style correlated subquery on the max attempt_number.
    let rows = sqlx::query(
        r"SELECT
            a.delivery_id,
            MIN(a.url) AS url,
            MIN(a.event_type) AS event_type,
            MIN(a.instance_id::text)::uuid AS instance_id,
            COUNT(*) AS attempts,
            BOOL_OR(a.success) AS delivered,
            MIN(a.attempted_at) AS first_attempt_at,
            MAX(a.attempted_at) AS last_attempt_at,
            (SELECT b.error_class FROM webhook_delivery_attempts b
              WHERE b.delivery_id = a.delivery_id
              ORDER BY b.attempt_number DESC LIMIT 1) AS last_error_class
          FROM webhook_delivery_attempts a
          WHERE ($1::text IS NULL OR a.url = $1)
            AND ($2::text IS NULL OR a.event_type = $2)
          GROUP BY a.delivery_id
          HAVING ($3::bool IS NULL OR BOOL_OR(a.success) = $3)
          ORDER BY MAX(a.attempted_at) DESC
          LIMIT $4",
    )
    .bind(&filter.url)
    .bind(&filter.event_type)
    .bind(filter.delivered)
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let summary = WebhookDeliverySummary {
            delivery_id: row.get("delivery_id"),
            url: row.get("url"),
            event_type: row.get("event_type"),
            instance_id: row.get("instance_id"),
            attempts: row.get("attempts"),
            delivered: row.get("delivered"),
            first_attempt_at: row.get("first_attempt_at"),
            last_attempt_at: row.get("last_attempt_at"),
            last_error_class: class_from_str(row.get::<Option<&str>, _>("last_error_class")),
        };
        // error_class filtering happens on the final attempt's class.
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
    store: &PostgresStorage,
    delivery_id: Uuid,
) -> Result<Vec<WebhookDeliveryAttempt>, StorageError> {
    let rows = sqlx::query(
        r"SELECT id, delivery_id, url, event_type, instance_id, attempt_number,
                 attempted_at, duration_ms, success, status_code, error_class,
                 error_excerpt, signed
          FROM webhook_delivery_attempts
          WHERE delivery_id = $1
          ORDER BY attempt_number ASC",
    )
    .bind(delivery_id)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.iter().map(row_to_attempt).collect())
}

pub(super) async fn delete_before(
    store: &PostgresStorage,
    cutoff: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let result = sqlx::query("DELETE FROM webhook_delivery_attempts WHERE attempted_at < $1")
        .bind(cutoff)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected())
}
