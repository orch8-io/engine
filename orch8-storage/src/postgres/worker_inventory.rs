use chrono::Utc;
use orch8_types::error::StorageError;
use uuid::Uuid;

use crate::worker_inventory::{RouteObservation, WorkerObservation, WorkerRoute};

pub(super) async fn record(
    pool: &sqlx::PgPool,
    observation: &WorkerObservation<'_>,
) -> Result<(), StorageError> {
    if observation.handlers.is_empty() {
        return Ok(());
    }
    let tenant = observation
        .tenant
        .map_or("", orch8_types::ids::TenantId::as_str);
    let (transport, session) = observation.transport.key();
    let runtime_id = observation
        .runtime_id
        .map(orch8_types::continuity::RuntimeId::into_uuid);
    let shared_runtime = observation
        .shared_runtime
        .map(serde_json::to_value)
        .transpose()
        .map_err(StorageError::Serialization)?;
    let now = Utc::now();
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO worker_route_observations
         (tenant_id, handler_name, worker_id, queue_name, version, last_seen_at,
          available_poll_slots, transport, all_queues, runtime_id, shared_runtime,
          session_id, draining) ",
    );
    query.push_values(observation.handlers.iter(), |mut row, handler| {
        row.push_bind(tenant)
            .push_bind(handler)
            .push_bind(observation.worker_id)
            .push_bind(observation.queue_name.unwrap_or_default())
            .push_bind(observation.version)
            .push_bind(now)
            .push_bind(
                observation
                    .available_poll_slots
                    .and_then(|slots| i32::try_from(slots).ok()),
            )
            .push_bind(transport)
            .push_bind(observation.all_queues)
            .push_bind(runtime_id)
            .push_bind(&shared_runtime)
            .push_bind(&session)
            .push_bind(observation.draining);
    });
    query.push(
        " ON CONFLICT(tenant_id, handler_name, worker_id, queue_name, all_queues, transport, session_id)
          DO UPDATE SET version=excluded.version, last_seen_at=excluded.last_seen_at,
              available_poll_slots=excluded.available_poll_slots, runtime_id=excluded.runtime_id,
              shared_runtime=excluded.shared_runtime, draining=excluded.draining
          WHERE excluded.last_seen_at >= worker_route_observations.last_seen_at",
    );
    query.build().execute(pool).await?;
    Ok(())
}

pub(super) async fn close(pool: &sqlx::PgPool, session_id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM worker_route_observations WHERE session_id=$1 AND session_id<>''")
        .bind(session_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub(super) async fn route(
    pool: &sqlx::PgPool,
    route: &WorkerRoute<'_>,
) -> Result<Vec<RouteObservation>, StorageError> {
    type Row = (
        String,
        bool,
        Option<String>,
        Option<i32>,
        Option<serde_json::Value>,
        bool,
        bool,
    );
    // Limit observations before looking up capabilities. The route indexes
    // scope both live observations and rows produced by previous binaries.
    let rows: Vec<Row> = sqlx::query_as(
        "WITH recent AS (
             SELECT tenant_id, worker_id, version, available_poll_slots, last_seen_at,
                    CASE WHEN all_queues THEN 1 ELSE 0 END AS wildcard,
                    transport, session_id, runtime_id, shared_runtime, draining
             FROM worker_route_observations
             WHERE tenant_id IN ('', $1) AND handler_name=$2 AND last_seen_at>=$4
               AND ((all_queues=false AND queue_name=$3) OR (all_queues=true AND queue_name=''))
             UNION ALL
             SELECT COALESCE(tenant_id, ''), worker_id, version, available_poll_slots,
                    last_seen_at, 0, 'legacy', '', CASE WHEN worker_id ~ '^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$'
                          THEN worker_id::uuid END, NULL::jsonb, false
             FROM worker_registrations
             WHERE COALESCE(tenant_id, '') IN ('', $1) AND handler_name=$2
               AND COALESCE(queue_name, '')=$3 AND last_seen_at>=$4
             ORDER BY last_seen_at DESC, tenant_id, worker_id, wildcard, transport, session_id
             LIMIT $5
         )
         SELECT recent.worker_id, recent.tenant_id='', recent.version, recent.available_poll_slots,
                COALESCE(recent.shared_runtime, runtime.record), recent.draining,
                recent.transport='http' AND recent.runtime_id IS NOT NULL
                    AND recent.shared_runtime IS NULL AND runtime.runtime_id IS NULL
         FROM recent
         LEFT JOIN runtime_capabilities runtime
           ON runtime.tenant_id=recent.tenant_id AND runtime.runtime_id=recent.runtime_id
           AND runtime.expires_at>$6 AND recent.tenant_id<>''
         ORDER BY recent.last_seen_at DESC, recent.tenant_id, recent.worker_id,
                  recent.wildcard, recent.transport, recent.session_id",
    )
    .bind(route.tenant.as_str())
    .bind(route.handler)
    .bind(route.queue.unwrap_or_default())
    .bind(route.seen_since)
    .bind(i64::from(route.limit))
    .bind(route.now)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(
            |(worker_id, shared, version, slots, runtime, draining, capability_unavailable)| {
                Ok(RouteObservation {
                    worker_id,
                    shared,
                    version,
                    available_poll_slots: slots.and_then(|slots| u32::try_from(slots).ok()),
                    runtime: runtime
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(StorageError::Serialization)?,
                    draining,
                    capability_unavailable,
                })
            },
        )
        .collect()
}
