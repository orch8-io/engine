//! Payload-free, keyset-paginated execution workbench events.

use chrono::{DateTime, Utc};
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    output::BlockOutput,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::StorageBackend;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EventKey {
    pub timestamp: DateTime<Utc>,
    pub rank: i32,
    pub id: Uuid,
}

#[derive(Debug)]
pub enum EventData {
    Audit {
        event_type: String,
        from: Option<String>,
        to: Option<String>,
    },
    Output {
        block_id: String,
        output_ref: Option<String>,
        attempt: u16,
        size: u32,
    },
    Log {
        block_id: String,
        level: String,
        message: String,
    },
}

#[derive(Debug)]
pub struct EventRow {
    pub key: EventKey,
    pub data: EventData,
}

/// Select a bounded prefix from each source, then merge those prefixes. Keep
/// the timestamp and native ID in the cursor tests so source indexes can seek.
/// Output JSON, audit details, and long log tails never leave the database.
macro_rules! event_page_query {
    ($minimum_timestamp:literal) => {
        concat!(
            "
WITH states AS (
    SELECT created_at AS timestamp, 0 AS rank, id, NULL AS block_id, event_type AS name,
           from_state AS subject, to_state AS detail, CAST(NULL AS BIGINT) AS attempt,
           CAST(NULL AS BIGINT) AS size
    FROM audit_log
    WHERE instance_id=$1 AND tenant_id=$6 AND event_type='state_transition'
      AND created_at >= COALESCE($2, ",
            $minimum_timestamp,
            ")
      AND ($2 IS NULL OR (created_at, 0, id) > ($2, $3, $4))
    ORDER BY created_at, id LIMIT $5
), audits AS (
    SELECT created_at, 1, id, NULL, event_type, from_state, to_state,
           CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
    FROM audit_log
    WHERE instance_id=$1 AND tenant_id=$6 AND event_type<>'state_transition'
      AND created_at >= COALESCE($2, ",
            $minimum_timestamp,
            ")
      AND ($2 IS NULL OR (created_at, 1, id) > ($2, $3, $4))
    ORDER BY created_at, id LIMIT $5
), outputs AS (
    SELECT created_at, 2, id, block_id, output_ref, NULL, NULL,
           CAST(attempt AS BIGINT), CAST(output_size AS BIGINT)
    FROM block_outputs
    WHERE instance_id=$1 AND substr(block_id, 1, 1) <> '_'
      AND created_at >= COALESCE($2, ",
            $minimum_timestamp,
            ")
      AND ($2 IS NULL OR (created_at, 2, id) > ($2, $3, $4))
    ORDER BY created_at, id LIMIT $5
), logs AS (
    SELECT ts, 3, id, block_id, level, NULL, substr(message, 1, 201),
           CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
    FROM step_logs WHERE instance_id=$1
      AND ts >= COALESCE($2, ",
            $minimum_timestamp,
            ")
      AND ($2 IS NULL OR (ts, 3, id) > ($2, $3, $4))
    ORDER BY ts, id LIMIT $5
)
SELECT timestamp, rank, CAST(id AS TEXT) AS id, block_id, name, subject, detail, attempt, size
FROM (
    SELECT * FROM states
    UNION ALL SELECT * FROM audits
    UNION ALL SELECT * FROM outputs
    UNION ALL SELECT * FROM logs
) events
ORDER BY timestamp, rank, events.id LIMIT $5
",
        )
    };
}

pub(crate) const SQLITE_EVENT_PAGE_QUERY: &str = event_page_query!("''");
#[cfg(feature = "postgres")]
pub(crate) const POSTGRES_EVENT_PAGE_QUERY: &str = event_page_query!("'-infinity'");

#[derive(sqlx::FromRow)]
pub(crate) struct EventRecord {
    timestamp: DateTime<Utc>,
    rank: i32,
    id: String,
    block_id: Option<String>,
    name: Option<String>,
    subject: Option<String>,
    detail: Option<String>,
    attempt: Option<i64>,
    size: Option<i64>,
}

impl EventRecord {
    pub(crate) fn into_event(self) -> Result<EventRow, StorageError> {
        let key = EventKey {
            timestamp: self.timestamp,
            rank: self.rank,
            id: Uuid::parse_str(&self.id)
                .map_err(|error| StorageError::Constraint(error.to_string()))?,
        };
        let data = match self.rank {
            0 | 1 => EventData::Audit {
                event_type: self.name.unwrap_or_default(),
                from: self.subject,
                to: self.detail,
            },
            2 => EventData::Output {
                block_id: self.block_id.unwrap_or_default(),
                output_ref: self.name,
                attempt: u16::try_from(self.attempt.unwrap_or(0)).map_err(|_| {
                    StorageError::Constraint("workbench output attempt out of range".into())
                })?,
                size: u32::try_from(self.size.unwrap_or(0)).map_err(|_| {
                    StorageError::Constraint("workbench output size out of range".into())
                })?,
            },
            _ => EventData::Log {
                block_id: self.block_id.unwrap_or_default(),
                level: self.name.unwrap_or_default(),
                message: self.detail.unwrap_or_default(),
            },
        };
        Ok(EventRow { key, data })
    }
}

fn from_output(row: BlockOutput) -> EventRow {
    EventRow {
        key: EventKey {
            timestamp: row.created_at,
            rank: 2,
            id: row.id,
        },
        data: EventData::Output {
            block_id: row.block_id.to_string(),
            output_ref: row.output_ref,
            attempt: row.attempt,
            size: row.output_size,
        },
    }
}

/// Request a workbench page from storage. External adapters can implement the
/// fast read without changing their existing output, log, or audit methods.
pub async fn event_page(
    storage: &dyn StorageBackend,
    instance: InstanceId,
    tenant: &TenantId,
    after: Option<EventKey>,
    limit: u32,
) -> Result<Vec<EventRow>, StorageError> {
    if let Some(rows) = storage
        .get_bounded_workbench_events(instance, tenant, after, limit)
        .await?
    {
        return Ok(rows);
    }

    let (outputs, logs, audit) = tokio::try_join!(
        storage.get_all_outputs(instance),
        storage.list_step_logs(instance),
        storage.list_audit_log(instance, u32::MAX),
    )?;
    let mut rows: Vec<_> = outputs
        .into_iter()
        .filter(|row| !row.block_id.as_str().starts_with('_'))
        .map(from_output)
        .collect();
    rows.extend(logs.into_iter().enumerate().map(|(index, log)| EventRow {
        key: EventKey {
            timestamp: log.ts,
            rank: 3,
            id: Uuid::from_u128(index as u128),
        },
        data: EventData::Log {
            block_id: log.block_id,
            level: log.level,
            message: log.message,
        },
    }));
    rows.extend(
        audit
            .into_iter()
            .filter(|row| row.tenant_id == *tenant)
            .map(|row| EventRow {
                key: EventKey {
                    timestamp: row.created_at,
                    rank: i32::from(row.event_type != "state_transition"),
                    id: row.id,
                },
                data: EventData::Audit {
                    event_type: row.event_type,
                    from: row.from_state,
                    to: row.to_state,
                },
            }),
    );
    rows.retain(|row| after.is_none_or(|cursor| row.key > cursor));
    rows.sort_unstable_by_key(|row| row.key);
    rows.truncate(limit as usize);
    Ok(rows)
}
