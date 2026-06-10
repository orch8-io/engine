use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerPollState, TriggerType};

use super::helpers::{parse_ts, parse_ts_opt};
use super::SqliteStorage;

pub(super) async fn create(
    store: &SqliteStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    let config_str = trigger.config.to_string();
    let created = trigger.created_at.to_rfc3339();
    let updated = trigger.updated_at.to_rfc3339();
    sqlx::query(
        r"INSERT INTO triggers (slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(trigger.tenant_id.as_str())
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(trigger.secret.as_ref().map(|s| s.expose().to_string()))
    .bind(trigger.trigger_type.to_string())
    .bind(&config_str)
    .bind(&created)
    .bind(&updated)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &SqliteStorage,
    slug: &str,
) -> Result<Option<TriggerDef>, StorageError> {
    let row: Option<TriggerRow> = sqlx::query_as(
        r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
          FROM triggers WHERE slug = ?1",
    )
    .bind(slug)
    .fetch_optional(&store.pool)
    .await?;
    row.map(TriggerRow::into_trigger).transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<TriggerDef>, StorageError> {
    let cap = limit.min(1000) as i64;
    let rows: Vec<TriggerRow> = match tenant_id {
        Some(tid) => {
            sqlx::query_as(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers WHERE tenant_id = ?1 ORDER BY created_at LIMIT ?2",
            )
            .bind(tid.as_str())
            .bind(cap)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers ORDER BY created_at LIMIT ?1",
            )
            .bind(cap)
            .fetch_all(&store.pool)
            .await?
        }
    };
    rows.into_iter()
        .map(TriggerRow::into_trigger)
        .collect::<Result<Vec<_>, _>>()
}

pub(super) async fn update(
    store: &SqliteStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    let config_str = trigger.config.to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        r"UPDATE triggers SET sequence_name=?2, version=?3, tenant_id=?4, namespace=?5,
          enabled=?6, secret=?7, trigger_type=?8, config=?9, updated_at=?10
          WHERE slug=?1",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(trigger.tenant_id.as_str())
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(trigger.secret.as_ref().map(|s| s.expose().to_string()))
    .bind(trigger.trigger_type.to_string())
    .bind(&config_str)
    .bind(&now)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &SqliteStorage, slug: &str) -> Result<(), StorageError> {
    // Remove poll state first so a polling trigger never leaves an orphaned
    // cursor behind (SQLite schema declares no FK cascade for this table).
    sqlx::query("DELETE FROM trigger_poll_state WHERE slug = ?1")
        .bind(slug)
        .execute(&store.pool)
        .await?;
    sqlx::query("DELETE FROM triggers WHERE slug = ?1")
        .bind(slug)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn get_poll_state(
    store: &SqliteStorage,
    slug: &str,
) -> Result<Option<TriggerPollState>, StorageError> {
    let row: Option<PollStateRow> = sqlx::query_as(
        r"SELECT slug, state, last_poll_at, last_error, consecutive_failures, updated_at
          FROM trigger_poll_state WHERE slug = ?1",
    )
    .bind(slug)
    .fetch_optional(&store.pool)
    .await?;
    row.map(PollStateRow::into_state).transpose()
}

pub(super) async fn upsert_poll_state(
    store: &SqliteStorage,
    state: &TriggerPollState,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO trigger_poll_state (slug, state, last_poll_at, last_error, consecutive_failures, updated_at)
          VALUES (?1,?2,?3,?4,?5,?6)
          ON CONFLICT(slug) DO UPDATE SET
            state=excluded.state, last_poll_at=excluded.last_poll_at,
            last_error=excluded.last_error,
            consecutive_failures=excluded.consecutive_failures,
            updated_at=excluded.updated_at",
    )
    .bind(&state.slug)
    .bind(state.state.to_string())
    .bind(state.last_poll_at.map(|t| t.to_rfc3339()))
    .bind(&state.last_error)
    .bind(state.consecutive_failures)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&store.pool)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct PollStateRow {
    slug: String,
    state: String,
    last_poll_at: Option<String>,
    last_error: Option<String>,
    consecutive_failures: i32,
    updated_at: String,
}

impl PollStateRow {
    fn into_state(self) -> Result<TriggerPollState, StorageError> {
        Ok(TriggerPollState {
            slug: self.slug,
            state: serde_json::from_str(&self.state).map_err(StorageError::Serialization)?,
            last_poll_at: parse_ts_opt(self.last_poll_at)?,
            last_error: self.last_error,
            consecutive_failures: self.consecutive_failures,
            updated_at: parse_ts(&self.updated_at)?,
        })
    }
}

#[derive(sqlx::FromRow)]
struct TriggerRow {
    slug: String,
    sequence_name: String,
    version: Option<i32>,
    tenant_id: String,
    namespace: String,
    enabled: bool,
    secret: Option<String>,
    trigger_type: String,
    config: String,
    created_at: String,
    updated_at: String,
}

impl TriggerRow {
    fn into_trigger(self) -> Result<TriggerDef, StorageError> {
        Ok(TriggerDef {
            slug: self.slug,
            sequence_name: self.sequence_name,
            version: self.version,
            tenant_id: TenantId::unchecked(self.tenant_id),
            namespace: self.namespace,
            enabled: self.enabled,
            secret: self.secret.map(orch8_types::config::SecretString::new),
            trigger_type: TriggerType::from_str_loose(&self.trigger_type).ok_or_else(|| {
                StorageError::Query(format!("unknown trigger type: {}", self.trigger_type))
            })?,
            config: serde_json::from_str(&self.config).map_err(StorageError::Serialization)?,
            created_at: parse_ts(&self.created_at)?,
            updated_at: parse_ts(&self.updated_at)?,
        })
    }
}
