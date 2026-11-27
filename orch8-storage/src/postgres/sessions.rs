use sqlx::{QueryBuilder, Row};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::session::{SessionExecutionFilter, SessionExecutionSummary};
use orch8_types::session::{SessionState, SessionSummary};

use super::PostgresStorage;
use super::rows::{InstanceRow, SessionRow};

pub(super) async fn list_summaries(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    before: Option<Uuid>,
    search: Option<&str>,
    state: Option<SessionState>,
    limit: u32,
) -> Result<Vec<SessionSummary>, StorageError> {
    let mut query = QueryBuilder::new(
        "SELECT id, tenant_id, session_key, state, created_at, updated_at, expires_at FROM sessions WHERE tenant_id = ",
    );
    query.push_bind(tenant_id.as_str());
    if let Some(before) = before {
        query.push(" AND id < ").push_bind(before);
    }
    if let Some(search) = search {
        query
            .push(" AND strpos(session_key, ")
            .push_bind(search)
            .push(") > 0");
    }
    if let Some(state) = state {
        query.push(" AND state = ").push_bind(state.to_string());
    }
    let rows = query
        .push(" ORDER BY id DESC LIMIT ")
        .push_bind(i64::from(limit))
        .build()
        .fetch_all(&store.pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok(SessionSummary {
                id: row.try_get("id")?,
                tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
                session_key: row.try_get("session_key")?,
                state: row
                    .try_get::<&str, _>("state")?
                    .parse()
                    .map_err(StorageError::Query)?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .collect()
}

pub(super) async fn create(
    store: &PostgresStorage,
    session: &orch8_types::session::Session,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO sessions (id, tenant_id, session_key, data, state, created_at, updated_at, expires_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(session.id)
    .bind(session.tenant_id.as_str())
    .bind(&session.session_key)
    .bind(&session.data)
    .bind(session.state.to_string())
    .bind(session.created_at)
    .bind(session.updated_at)
    .bind(session.expires_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: Uuid,
) -> Result<Option<orch8_types::session::Session>, StorageError> {
    let row = sqlx::query_as::<_, SessionRow>(
        r"SELECT id, tenant_id, session_key, data, state, created_at, updated_at, expires_at
          FROM sessions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    row.map(SessionRow::into_session).transpose()
}

pub(super) async fn get_by_key(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    session_key: &str,
) -> Result<Option<orch8_types::session::Session>, StorageError> {
    let row = sqlx::query_as::<_, SessionRow>(
        r"SELECT id, tenant_id, session_key, data, state, created_at, updated_at, expires_at
          FROM sessions WHERE tenant_id = $1 AND session_key = $2",
    )
    .bind(tenant_id.as_str())
    .bind(session_key)
    .fetch_optional(&store.pool)
    .await?;
    row.map(SessionRow::into_session).transpose()
}

pub(super) async fn update_data(
    store: &PostgresStorage,
    id: Uuid,
    data: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET data = $2, updated_at = NOW() WHERE id = $1")
        .bind(id)
        .bind(data)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn update_state(
    store: &PostgresStorage,
    id: Uuid,
    state: orch8_types::session::SessionState,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET state = $2, updated_at = NOW() WHERE id = $1")
        .bind(id)
        .bind(state.to_string())
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn list_instances(
    store: &PostgresStorage,
    session_id: Uuid,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows = sqlx::query_as::<_, InstanceRow>(
        r"SELECT id, sequence_id, tenant_id, namespace, state, next_fire_at,
                  priority, timezone, metadata, context,
                  concurrency_key, max_concurrency, idempotency_key,
                  session_id, parent_instance_id, budget, created_at, updated_at, claimed_at, sub_tenant
           FROM task_instances WHERE session_id = $1 ORDER BY created_at, id",
    )
    .bind(session_id)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(InstanceRow::into_instance).collect()
}

pub(super) async fn list_execution_summaries(
    store: &PostgresStorage,
    filter: &SessionExecutionFilter<'_>,
) -> Result<Vec<SessionExecutionSummary>, StorageError> {
    let mut query = QueryBuilder::new(
        "SELECT id, sequence_id, tenant_id, session_id, namespace, state, priority, created_at, updated_at FROM task_instances WHERE tenant_id = ",
    );
    query.push_bind(filter.tenant_id.as_str());
    query
        .push(" AND session_id = ")
        .push_bind(filter.session_id);
    if let Some(before) = filter.before {
        query.push(" AND id < ").push_bind(before.into_uuid());
    }
    if let Some(state) = filter.state {
        query.push(" AND state = ").push_bind(state.to_string());
    }
    for term in filter.terms {
        query.push(" AND strpos(lower((id::text || ' ' || sequence_id::text || ' ' || namespace) COLLATE \"C\"), ").push_bind(term.as_str()).push(") > 0");
    }
    let rows = query
        .push(" ORDER BY id DESC LIMIT ")
        .push_bind(i64::from(filter.limit))
        .build()
        .fetch_all(&store.pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok(SessionExecutionSummary {
                id: InstanceId::from_uuid(row.try_get("id")?),
                sequence_id: SequenceId::from_uuid(row.try_get("sequence_id")?),
                tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
                session_id: row.try_get("session_id")?,
                namespace: Namespace::new(row.try_get::<String, _>("namespace")?),
                state: row
                    .try_get::<&str, _>("state")?
                    .parse::<InstanceState>()
                    .map_err(StorageError::Query)?,
                priority: Priority::try_from(row.try_get::<i16, _>("priority")?)
                    .map_err(|error| StorageError::Query(error.to_string()))?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            })
        })
        .collect()
}
