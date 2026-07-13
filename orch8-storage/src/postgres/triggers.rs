use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerPollState, TriggerType};

use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO triggers (slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(trigger.tenant_id.as_str())
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(trigger.secret.as_ref().map(|s| s.expose().to_string()))
    .bind(trigger.trigger_type.to_string())
    // `config` column is TEXT — bind the JSON value serialized as a string
    // rather than relying on sqlx's JSONB encoder.
    .bind(trigger.config.to_string())
    .bind(trigger.created_at)
    .bind(trigger.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    slug: &str,
) -> Result<Option<TriggerDef>, StorageError> {
    // `tenant_id: None` is an intentionally unscoped lookup -- e.g. the
    // public webhook endpoint, where resolving *which* tenant owns `slug` is
    // the lookup's job, not something the caller already knows.
    let row = match tenant_id {
        Some(tid) => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers WHERE slug = $1 AND tenant_id = $2",
            )
            .bind(slug)
            .bind(tid.as_str())
            .fetch_optional(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers WHERE slug = $1",
            )
            .bind(slug)
            .fetch_optional(&store.pool)
            .await?
        }
    };
    row.map(TriggerRow::into_trigger).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<TriggerDef>, StorageError> {
    let cap = i64::from(limit.min(1000));
    let rows = match tenant_id {
        Some(tid) => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers WHERE tenant_id = $1 ORDER BY created_at LIMIT $2",
            )
            .bind(tid.as_str())
            .bind(cap)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers ORDER BY created_at LIMIT $1",
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
    store: &PostgresStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE triggers SET sequence_name=$2, version=$3, tenant_id=$4, namespace=$5,
          enabled=$6, secret=$7, trigger_type=$8, config=$9, updated_at=NOW()
          WHERE slug=$1",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(trigger.tenant_id.as_str())
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(trigger.secret.as_ref().map(|s| s.expose().to_string()))
    .bind(trigger.trigger_type.to_string())
    .bind(trigger.config.to_string())
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, slug: &str) -> Result<(), StorageError> {
    // `trigger_poll_state.slug` has ON DELETE CASCADE in Postgres, so the
    // trigger delete also removes any poll cursor.
    sqlx::query("DELETE FROM triggers WHERE slug = $1")
        .bind(slug)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn claim_nonce(
    store: &PostgresStorage,
    slug: &str,
    nonce: &str,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool, StorageError> {
    let mut tx = store.pool.begin().await?;
    // Keep cleanup work bounded on the request path. The conflict clause
    // below can reclaim the current nonce even if more expired rows remain.
    sqlx::query(
        r"DELETE FROM webhook_replay_nonces WHERE ctid IN (
              SELECT ctid FROM webhook_replay_nonces
              WHERE expires_at <= NOW() LIMIT 100
          )",
    )
    .execute(&mut *tx)
    .await?;
    let result = sqlx::query(
        r"INSERT INTO webhook_replay_nonces (trigger_slug, nonce, expires_at)
          VALUES ($1, $2, $3)
          ON CONFLICT (trigger_slug, nonce) DO UPDATE
          SET expires_at = EXCLUDED.expires_at
          WHERE webhook_replay_nonces.expires_at <= NOW()",
    )
    .bind(slug)
    .bind(nonce)
    .bind(expires_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn get_poll_state(
    store: &PostgresStorage,
    slug: &str,
) -> Result<Option<TriggerPollState>, StorageError> {
    let row = sqlx::query_as::<_, PollStateRow>(
        r"SELECT slug, state, last_poll_at, last_error, consecutive_failures, updated_at
          FROM trigger_poll_state WHERE slug = $1",
    )
    .bind(slug)
    .fetch_optional(&store.pool)
    .await?;
    row.map(PollStateRow::into_state).transpose()
}

pub(super) async fn upsert_poll_state(
    store: &PostgresStorage,
    state: &TriggerPollState,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO trigger_poll_state (slug, state, last_poll_at, last_error, consecutive_failures, updated_at)
          VALUES ($1,$2,$3,$4,$5,NOW())
          ON CONFLICT(slug) DO UPDATE SET
            state=EXCLUDED.state, last_poll_at=EXCLUDED.last_poll_at,
            last_error=EXCLUDED.last_error,
            consecutive_failures=EXCLUDED.consecutive_failures,
            updated_at=NOW()",
    )
    .bind(&state.slug)
    .bind(state.state.to_string())
    .bind(state.last_poll_at)
    .bind(&state.last_error)
    .bind(state.consecutive_failures)
    .execute(&store.pool)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct PollStateRow {
    slug: String,
    // `state` column is TEXT (matching `triggers.config`) — decode as String
    // then parse.
    state: String,
    last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    last_error: Option<String>,
    consecutive_failures: i32,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl PollStateRow {
    fn into_state(self) -> Result<TriggerPollState, StorageError> {
        let state = if self.state.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&self.state).map_err(StorageError::Serialization)?
        };
        Ok(TriggerPollState {
            slug: self.slug,
            state,
            last_poll_at: self.last_poll_at,
            last_error: self.last_error,
            consecutive_failures: self.consecutive_failures,
            updated_at: self.updated_at,
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
    // `config` column is `TEXT` in migration 020 (not JSONB), so decode as
    // String then parse. Empty/invalid strings fall back to `Value::Null`.
    config: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl TriggerRow {
    fn into_trigger(self) -> Result<TriggerDef, StorageError> {
        let config = if self.config.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&self.config).map_err(StorageError::Serialization)?
        };
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
            config,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}
