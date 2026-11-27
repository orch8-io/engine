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
    os_name: &str,
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
    let received_at = Utc::now();
    sqlx::query(
        "INSERT INTO telemetry_events (event_type, payload, created_at, received_at,
            dashboard_tenant_id, dashboard_received_at_us, dashboard_os_name)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(event_type)
    .bind(&enriched)
    .bind(created_at.to_rfc3339())
    .bind(received_at.to_rfc3339())
    .bind(tenant_id)
    .bind(received_at.timestamp_micros())
    .bind(os_name)
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub(super) async fn record_usage_event(
    storage: &SqliteStorage,
    event: &crate::UsageEvent,
) -> Result<(), StorageError> {
    crate::validate_usage_event(event)?;
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
         FROM usage_events WHERE instance_id = ?1 AND kind = 'llm_tokens'",
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
    // Keep each 7-parameter row within SQLite's legacy 999-parameter limit.
    for chunk in events.chunks(142) {
        let mut qb: sqlx::QueryBuilder<sqlx::Sqlite> = sqlx::QueryBuilder::new(
            "INSERT INTO telemetry_events (event_type, payload, created_at, received_at,
                dashboard_tenant_id, dashboard_received_at_us, dashboard_os_name) ",
        );
        let received_at = Utc::now();
        let received_at_text = received_at.to_rfc3339();
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
            b.push_bind(received_at_text.clone());
            b.push_bind(&event.tenant_id);
            b.push_bind(received_at.timestamp_micros());
            b.push_bind(&event.os_name);
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
    let received_at = Utc::now();
    sqlx::query(
        "INSERT INTO telemetry_events (event_type, payload, created_at, received_at,
            dashboard_tenant_id, dashboard_received_at_us, dashboard_is_error)
         VALUES (?1, ?2, ?3, ?3, ?4, ?5, 1)",
    )
    .bind("InstanceFailed")
    .bind(payload.to_string())
    .bind(received_at.to_rfc3339())
    .bind(tenant_id)
    .bind(received_at.timestamp_micros())
    .execute(storage.pool())
    .await?;
    Ok(())
}

pub(super) async fn query_telemetry_dashboard(
    storage: &SqliteStorage,
    query_type: &str,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<(String, i64)>, StorageError> {
    let (event_type, dimension_path, is_error) = match query_type {
        "sync_completed_versions" => (Some("SyncCompleted"), Some("$.version"), false),
        "error_rate_per_sequence" => (None, Some("$.sequence_name"), true),
        "top_failing_steps" => (Some("InstanceFailed"), Some("$.step_name"), false),
        "device_os_breakdown" => (None, None, false),
        _ => return Ok(Vec::new()),
    };

    let rows = sqlx::query_as(
        r"WITH dimensions AS (
            SELECT CASE
                WHEN ?6 IS NULL THEN dashboard_os_name
                WHEN json_valid(payload) THEN CASE json_type(payload, ?6)
                    WHEN 'null' THEN NULL
                    WHEN 'text' THEN json_extract(payload, ?6)
                    ELSE payload -> ?6
                END
            END AS dimension
            FROM telemetry_events
            WHERE dashboard_tenant_id = ?1 AND dashboard_received_at_us BETWEEN ?2 AND ?3
              AND dashboard_is_error = ?4 AND (?5 IS NULL OR event_type = ?5)
         )
         SELECT COALESCE(dimension, 'unknown'), COUNT(*) AS count
         FROM dimensions GROUP BY dimension ORDER BY count DESC, dimension LIMIT 50",
    )
    .bind(tenant_id)
    .bind(start.timestamp_micros())
    .bind(end.timestamp_micros())
    .bind(is_error)
    .bind(event_type)
    .bind(dimension_path)
    .fetch_all(storage.pool())
    .await?;
    Ok(rows)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TelemetryStore as _;

    #[tokio::test]
    async fn mobile_dashboard_aggregates_trusted_scope_and_receive_time() {
        let store = SqliteStorage::in_memory().await.unwrap();
        let start = Utc::now() - chrono::TimeDelta::seconds(1);
        let client_time = start - chrono::TimeDelta::days(30);
        store
            .ingest_telemetry_event(
                "SyncCompleted",
                r#"{"version":"1.2","tenant_id":"foreign"}"#,
                "device",
                "iOS",
                "17",
                "1",
                "1",
                "reviewed",
                client_time,
            )
            .await
            .unwrap();
        store
            .ingest_telemetry_events_batch(&[
                crate::TelemetryEvent {
                    event_type: "InstanceFailed".into(),
                    payload: r#"{"step_name":"read"}"#.into(),
                    device_id: "device".into(),
                    os_name: "Android".into(),
                    os_version: "14".into(),
                    app_version: "1".into(),
                    sdk_version: "1".into(),
                    tenant_id: "reviewed".into(),
                    created_at: client_time,
                },
                crate::TelemetryEvent {
                    event_type: "SyncCompleted".into(),
                    payload: r#"{"version":"foreign","tenant_id":"reviewed"}"#.into(),
                    device_id: "foreign".into(),
                    os_name: "foreign".into(),
                    os_version: "1".into(),
                    app_version: "1".into(),
                    sdk_version: "1".into(),
                    tenant_id: "foreign".into(),
                    created_at: client_time,
                },
            ])
            .await
            .unwrap();
        store
            .ingest_telemetry_error(
                "fault",
                "message",
                None,
                "device",
                "iOS",
                "17",
                "1",
                "1",
                "reviewed",
                None,
                Some("checkout"),
            )
            .await
            .unwrap();
        sqlx::query(
            r#"INSERT INTO telemetry_events (event_type, payload, received_at)
            VALUES ('SyncCompleted', '{"tenant_id":"reviewed","version":"legacy"}', ?1)"#,
        )
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();

        let end = Utc::now() + chrono::TimeDelta::seconds(1);
        for (query, expected) in [
            ("sync_completed_versions", vec![("1.2".into(), 1)]),
            ("error_rate_per_sequence", vec![("checkout".into(), 1)]),
            ("top_failing_steps", vec![("read".into(), 1)]),
            (
                "device_os_breakdown",
                vec![("Android".into(), 1), ("iOS".into(), 1)],
            ),
        ] {
            assert_eq!(
                store
                    .query_telemetry_dashboard(query, "reviewed", start, end)
                    .await
                    .unwrap(),
                expected
            );
        }
        assert!(
            store
                .query_telemetry_dashboard(
                    "sync_completed_versions",
                    "reviewed",
                    client_time,
                    start
                )
                .await
                .unwrap()
                .is_empty()
        );
    }
}
