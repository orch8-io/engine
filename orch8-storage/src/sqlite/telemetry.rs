// Trait impl methods are exempt from `too_many_arguments` (clippy skips
// lints on a signature the impl doesn't control), but these free functions
// -- extracted from what used to be trait impl bodies -- are not, so the
// allow that used to be implicit must be explicit here.
#![allow(clippy::too_many_arguments)]

use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::SqliteStorage;

pub(super) async fn ingest_telemetry_event(
    storage: &SqliteStorage,
    event_type: &str,
    payload: &str,
    _device_id: &str,
    _os_name: &str,
    _os_version: &str,
    _app_version: &str,
    _sdk_version: &str,
    tenant_id: &str,
    created_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let enriched = match serde_json::from_str::<serde_json::Value>(payload) {
        Ok(mut v) => {
            if let Some(obj) = v.as_object_mut() {
                obj.entry("tenant_id")
                    .or_insert_with(|| serde_json::Value::String(tenant_id.to_string()));
            }
            v.to_string()
        }
        Err(_) => payload.to_string(),
    };
    sqlx::query(
        "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
    )
    .bind(event_type)
    .bind(&enriched)
    .bind(created_at.to_rfc3339())
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub(super) async fn record_usage_event(
    storage: &SqliteStorage,
    event: &crate::UsageEvent,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO usage_events \
         (tenant_id, instance_id, block_id, kind, model, input_tokens, output_tokens, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    )
    .bind(&event.tenant_id)
    .bind(event.instance_id.map(|i| i.to_string()))
    .bind(&event.block_id)
    .bind(&event.kind)
    .bind(&event.model)
    .bind(event.input_tokens)
    .bind(event.output_tokens)
    .bind(event.created_at.to_rfc3339())
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub(super) async fn query_usage(
    storage: &SqliteStorage,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<crate::UsageAggregate>, StorageError> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT kind, model, COUNT(*) AS events, \
                COALESCE(SUM(input_tokens), 0) AS input_tokens, \
                COALESCE(SUM(output_tokens), 0) AS output_tokens \
         FROM usage_events \
         WHERE tenant_id = ?1 AND created_at >= ?2 AND created_at < ?3 \
         GROUP BY kind, model ORDER BY kind, model",
    )
    .bind(tenant_id)
    .bind(start.to_rfc3339())
    .bind(end.to_rfc3339())
    .fetch_all(storage.pool())
    .await?;
    Ok(rows
        .iter()
        .map(|r| crate::UsageAggregate {
            kind: r.get("kind"),
            model: r.get("model"),
            events: r.get("events"),
            input_tokens: r.get("input_tokens"),
            output_tokens: r.get("output_tokens"),
        })
        .collect())
}

pub(super) async fn query_instance_usage_totals(
    storage: &SqliteStorage,
    instance_id: InstanceId,
) -> Result<(i64, i64), StorageError> {
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT COALESCE(SUM(input_tokens), 0) AS input_tokens, \
                COALESCE(SUM(output_tokens), 0) AS output_tokens \
         FROM usage_events WHERE instance_id = ?1",
    )
    .bind(instance_id.to_string())
    .fetch_one(storage.pool())
    .await?;
    Ok((row.get("input_tokens"), row.get("output_tokens")))
}

pub(super) async fn ingest_telemetry_events_batch(
    storage: &SqliteStorage,
    events: &[crate::TelemetryEvent],
) -> Result<u64, StorageError> {
    if events.is_empty() {
        return Ok(0);
    }
    let mut total = 0u64;
    // SQLite has a variable limit of 999 by default; 3 params per row -> batch <= 333.
    for chunk in events.chunks(333) {
        let mut qb: sqlx::QueryBuilder<'_, sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "INSERT INTO telemetry_events (event_type, payload, created_at) ",
        );
        qb.push_values(chunk, |mut b, event| {
            let enriched = match serde_json::from_str::<serde_json::Value>(&event.payload) {
                Ok(mut v) => {
                    if let Some(obj) = v.as_object_mut() {
                        obj.entry("tenant_id")
                            .or_insert_with(|| serde_json::Value::String(event.tenant_id.clone()));
                    }
                    v.to_string()
                }
                Err(_) => event.payload.clone(),
            };
            b.push_bind(&event.event_type);
            b.push_bind(enriched);
            b.push_bind(event.created_at.to_rfc3339());
        });
        let result = qb.build().execute(storage.pool()).await?;
        total += result.rows_affected();
    }
    Ok(total)
}

pub(super) async fn ingest_telemetry_error(
    storage: &SqliteStorage,
    error_type: &str,
    message: &str,
    _stack_trace: Option<&str>,
    _device_id: &str,
    _os_name: &str,
    _os_version: &str,
    _app_version: &str,
    _sdk_version: &str,
    tenant_id: &str,
    instance_id: Option<&str>,
    sequence_name: Option<&str>,
) -> Result<(), StorageError> {
    let payload = serde_json::json!({
        "error_type": error_type,
        "message": message,
        "instance_id": instance_id,
        "sequence_name": sequence_name,
        "tenant_id": tenant_id,
    });
    sqlx::query(
        "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
    )
    .bind("InstanceFailed")
    .bind(payload.to_string())
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub(super) async fn query_telemetry_dashboard(
    _storage: &SqliteStorage,
    _query_type: &str,
    _tenant_id: &str,
    _start: DateTime<Utc>,
    _end: DateTime<Utc>,
) -> Result<Vec<(String, i64)>, StorageError> {
    // Dashboard queries are server-side (Postgres) only.
    Ok(Vec::new())
}

pub(super) async fn delete_old_telemetry_events(
    storage: &SqliteStorage,
    older_than: DateTime<Utc>,
    limit: u32,
) -> Result<u64, StorageError> {
    let result = sqlx::query(
        "DELETE FROM telemetry_events WHERE id IN (
            SELECT id FROM telemetry_events WHERE created_at < ?1 LIMIT ?2
        )",
    )
    .bind(older_than.to_rfc3339())
    .bind(limit)
    .execute(storage.pool())
    .await?;
    Ok(result.rows_affected())
}
