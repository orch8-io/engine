use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::TaskInstance;

use super::rows::{InstanceRow, SessionRow};
use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    session: &orch8_types::session::Session,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO sessions (id, tenant_id, session_key, data, state, created_at, updated_at, expires_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
    )
    .bind(session.id)
    .bind(&session.tenant_id.0)
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
    Ok(row.map(SessionRow::into_session))
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
    .bind(&tenant_id.0)
    .bind(session_key)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(SessionRow::into_session))
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
                  session_id, parent_instance_id, created_at, updated_at
           FROM task_instances WHERE session_id = $1 ORDER BY created_at",
    )
    .bind(session_id)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter().map(InstanceRow::into_instance).collect()
}
