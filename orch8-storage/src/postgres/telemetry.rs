use async_trait::async_trait;
use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::InstanceId;

use super::PostgresStorage;

#[async_trait]
impl crate::TelemetryStore for PostgresStorage {
    async fn ingest_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO telemetry_mobile_events
               (event_type, payload, device_id, os_name, os_version, app_version, sdk_version, tenant_id, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ",
        )
        .bind(event_type)
        .bind(payload)
        .bind(device_id)
        .bind(os_name)
        .bind(os_version)
        .bind(app_version)
        .bind(sdk_version)
        .bind(tenant_id)
        .bind(created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_usage_event(&self, event: &crate::UsageEvent) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO usage_events
               (tenant_id, instance_id, block_id, kind, model, input_tokens, output_tokens, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(&event.tenant_id)
        .bind(event.instance_id.map(orch8_types::ids::InstanceId::into_uuid))
        .bind(&event.block_id)
        .bind(&event.kind)
        .bind(&event.model)
        .bind(event.input_tokens)
        .bind(event.output_tokens)
        .bind(event.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn query_usage(
        &self,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<crate::UsageAggregate>, StorageError> {
        use sqlx::Row;
        // SUM(bigint) is NUMERIC in Postgres — cast back to BIGINT for i64 decode.
        let rows = sqlx::query(
            r"SELECT kind, model,
                     COUNT(*)::BIGINT AS events,
                     COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                     COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens
              FROM usage_events
              WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
              GROUP BY kind, model ORDER BY kind, model",
        )
        .bind(tenant_id)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
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

    async fn query_instance_usage_totals(
        &self,
        instance_id: InstanceId,
    ) -> Result<(i64, i64), StorageError> {
        use sqlx::Row;
        // SUM(bigint) is NUMERIC in Postgres — cast back to BIGINT for i64 decode.
        let row = sqlx::query(
            r"SELECT COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
                     COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens
              FROM usage_events WHERE instance_id = $1",
        )
        .bind(instance_id.into_uuid())
        .fetch_one(&self.pool)
        .await?;
        Ok((row.get("input_tokens"), row.get("output_tokens")))
    }

    async fn ingest_telemetry_events_batch(
        &self,
        events: &[crate::TelemetryEvent],
    ) -> Result<u64, StorageError> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut total = 0u64;
        // Postgres limit: 65535 params. 9 params per row -> batch <= ~7000.
        for chunk in events.chunks(7000) {
            let mut qb: sqlx::QueryBuilder<'_, sqlx::Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO telemetry_mobile_events (event_type, payload, device_id, os_name, os_version, app_version, sdk_version, tenant_id, created_at) ",
            );
            qb.push_values(chunk, |mut b, event| {
                b.push_bind(&event.event_type);
                b.push_bind(&event.payload);
                b.push_bind(&event.device_id);
                b.push_bind(&event.os_name);
                b.push_bind(&event.os_version);
                b.push_bind(&event.app_version);
                b.push_bind(&event.sdk_version);
                b.push_bind(&event.tenant_id);
                b.push_bind(event.created_at);
            });
            let result = qb.build().execute(&self.pool).await?;
            total += result.rows_affected();
        }
        Ok(total)
    }

    async fn ingest_telemetry_error(
        &self,
        error_type: &str,
        message: &str,
        stack_trace: Option<&str>,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        instance_id: Option<&str>,
        sequence_name: Option<&str>,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r"INSERT INTO telemetry_mobile_errors
               (error_type, message, stack_trace, device_id, os_name, os_version, app_version, sdk_version, tenant_id, instance_id, sequence_name)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ",
        )
        .bind(error_type)
        .bind(message)
        .bind(stack_trace)
        .bind(device_id)
        .bind(os_name)
        .bind(os_version)
        .bind(app_version)
        .bind(sdk_version)
        .bind(tenant_id)
        .bind(instance_id)
        .bind(sequence_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn query_telemetry_dashboard(
        &self,
        query_type: &str,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        let rows: Vec<(String, i64)> = match query_type {
            "sync_completed_versions" => {
                sqlx::query_as(
                    r"SELECT COALESCE(payload->>'version', 'unknown') as version, COUNT(*) as cnt
                       FROM telemetry_mobile_events
                       WHERE event_type = 'SyncCompleted'
                         AND tenant_id = $1
                         AND received_at BETWEEN $2 AND $3
                       GROUP BY payload->>'version'
                       ORDER BY cnt DESC
                       LIMIT 50",
                )
                .bind(tenant_id)
                .bind(start)
                .bind(end)
                .fetch_all(&self.pool)
                .await?
            }
            "error_rate_per_sequence" => {
                sqlx::query_as(
                    r"SELECT COALESCE(sequence_name, 'unknown') as name, COUNT(*) as cnt
                       FROM telemetry_mobile_errors
                       WHERE tenant_id = $1
                         AND received_at BETWEEN $2 AND $3
                       GROUP BY sequence_name
                       ORDER BY cnt DESC
                       LIMIT 50",
                )
                .bind(tenant_id)
                .bind(start)
                .bind(end)
                .fetch_all(&self.pool)
                .await?
            }
            "top_failing_steps" => sqlx::query_as(
                r"SELECT COALESCE(payload->>'step_name', 'unknown') as step_name, COUNT(*) as cnt
                       FROM telemetry_mobile_events
                       WHERE event_type = 'InstanceFailed'
                         AND tenant_id = $1
                         AND received_at BETWEEN $2 AND $3
                       GROUP BY payload->>'step_name'
                       ORDER BY cnt DESC
                       LIMIT 50",
            )
            .bind(tenant_id)
            .bind(start)
            .bind(end)
            .fetch_all(&self.pool)
            .await?,
            "device_os_breakdown" => {
                sqlx::query_as(
                    r"SELECT COALESCE(os_name, 'unknown') as os_name, COUNT(*) as cnt
                       FROM telemetry_mobile_events
                       WHERE tenant_id = $1
                         AND received_at BETWEEN $2 AND $3
                       GROUP BY os_name
                       ORDER BY cnt DESC
                       LIMIT 50",
                )
                .bind(tenant_id)
                .bind(start)
                .bind(end)
                .fetch_all(&self.pool)
                .await?
            }
            _ => Vec::new(),
        };
        Ok(rows)
    }

    async fn delete_old_telemetry_events(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM telemetry_mobile_events WHERE id IN (
                SELECT id FROM telemetry_mobile_events WHERE received_at < $1 LIMIT $2
            )",
        )
        .bind(older_than)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
