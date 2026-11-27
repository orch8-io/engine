use chrono::Utc;
use sqlx::{QueryBuilder, Row};
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::session::{Session, SessionState, SessionSummary};
use orch8_types::session::{SessionExecutionFilter, SessionExecutionSummary};

use super::SqliteStorage;
use super::helpers::{parse_ts, parse_ts_opt, parse_uuid, row_to_instance, row_to_session, ts};

pub(super) async fn create(storage: &SqliteStorage, s: &Session) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO sessions (id,tenant_id,session_key,data,state,created_at,updated_at,expires_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)")
        .bind(s.id.to_string())
        .bind(s.tenant_id.as_str())
        .bind(&s.session_key)
        .bind(serde_json::to_string(&s.data)?)
        .bind(s.state.to_string())
        .bind(ts(s.created_at))
        .bind(ts(s.updated_at))
        .bind(s.expires_at.map(ts))
        .execute(&storage.pool).await?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<Session>, StorageError> {
    let row = sqlx::query("SELECT * FROM sessions WHERE id=?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_session).transpose()
}

pub(super) async fn get_by_key(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    session_key: &str,
) -> Result<Option<Session>, StorageError> {
    let row = sqlx::query("SELECT * FROM sessions WHERE tenant_id=?1 AND session_key=?2")
        .bind(tenant_id.as_str())
        .bind(session_key)
        .fetch_optional(&storage.pool)
        .await?;
    row.as_ref().map(row_to_session).transpose()
}

pub(super) async fn list_summaries(
    storage: &SqliteStorage,
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
        query.push(" AND id < ").push_bind(before.to_string());
    }
    if let Some(search) = search {
        query
            .push(" AND instr(session_key, ")
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
        .fetch_all(&storage.pool)
        .await?;
    rows.iter()
        .map(|row| {
            Ok(SessionSummary {
                id: parse_uuid(row.try_get("id")?)?,
                tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
                session_key: row.try_get("session_key")?,
                state: row
                    .try_get::<&str, _>("state")?
                    .parse()
                    .map_err(StorageError::Query)?,
                created_at: parse_ts(row.try_get("created_at")?)?,
                updated_at: parse_ts(row.try_get("updated_at")?)?,
                expires_at: parse_ts_opt(row.try_get("expires_at")?)?,
            })
        })
        .collect()
}

pub(super) async fn update_data(
    storage: &SqliteStorage,
    id: Uuid,
    data: &serde_json::Value,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET data=?2, updated_at=?3 WHERE id=?1")
        .bind(id.to_string())
        .bind(serde_json::to_string(data)?)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn update_state(
    storage: &SqliteStorage,
    id: Uuid,
    state: SessionState,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE sessions SET state=?2, updated_at=?3 WHERE id=?1")
        .bind(id.to_string())
        .bind(state.to_string())
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await?;
    Ok(())
}

pub(super) async fn list_instances(
    storage: &SqliteStorage,
    session_id: Uuid,
) -> Result<Vec<TaskInstance>, StorageError> {
    let rows =
        sqlx::query("SELECT * FROM task_instances WHERE session_id=?1 ORDER BY created_at, id")
            .bind(session_id.to_string())
            .fetch_all(&storage.pool)
            .await?;
    rows.iter().map(row_to_instance).collect()
}

pub(super) async fn list_execution_summaries(
    store: &SqliteStorage,
    filter: &SessionExecutionFilter<'_>,
) -> Result<Vec<SessionExecutionSummary>, StorageError> {
    let mut query = QueryBuilder::new(
        "SELECT id, sequence_id, tenant_id, session_id, namespace, state, priority, created_at, updated_at FROM task_instances WHERE tenant_id = ",
    );
    query.push_bind(filter.tenant_id.as_str());
    query
        .push(" AND session_id = ")
        .push_bind(filter.session_id.to_string());
    if let Some(before) = filter.before {
        query.push(" AND id < ").push_bind(before.to_string());
    }
    if let Some(state) = filter.state {
        query.push(" AND state = ").push_bind(state.to_string());
    }
    for term in filter.terms {
        query
            .push(" AND instr(lower(id || ' ' || sequence_id || ' ' || namespace), ")
            .push_bind(term.as_str())
            .push(") > 0");
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
                id: InstanceId::from_uuid(parse_uuid(row.try_get("id")?)?),
                sequence_id: SequenceId::from_uuid(parse_uuid(row.try_get("sequence_id")?)?),
                tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
                session_id: parse_uuid(row.try_get("session_id")?)?,
                namespace: Namespace::new(row.try_get::<String, _>("namespace")?),
                state: row
                    .try_get::<&str, _>("state")?
                    .parse::<InstanceState>()
                    .map_err(StorageError::Query)?,
                priority: Priority::try_from(row.try_get::<i16, _>("priority")?)
                    .map_err(StorageError::Query)?,
                created_at: parse_ts(row.try_get("created_at")?)?,
                updated_at: parse_ts(row.try_get("updated_at")?)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_persisted_state_is_not_treated_as_active() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let session = Session {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("tenant"),
            session_key: "key".into(),
            data: serde_json::json!({}),
            state: SessionState::Active,
            created_at: now,
            updated_at: now,
            expires_at: None,
        };
        create(&storage, &session).await.unwrap();
        sqlx::query("UPDATE sessions SET state = 'future-terminal' WHERE id = ?1")
            .bind(session.id.to_string())
            .execute(storage.pool())
            .await
            .unwrap();

        for result in [
            get(&storage, session.id).await,
            get_by_key(&storage, &session.tenant_id, &session.session_key).await,
        ] {
            assert!(matches!(
                result,
                Err(StorageError::Query(message)) if message.contains("future-terminal")
            ));
        }
    }
}
