use sqlx::Row;
use uuid::Uuid;

use orch8_types::alert::{AlertRule, AlertRuleState};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::PostgresStorage;

const COLUMNS: &str =
    "id, tenant_id, name, enabled, condition, destination, cooldown_secs, created_at, updated_at";

fn row_to_rule(row: &sqlx::postgres::PgRow) -> Result<AlertRule, StorageError> {
    let cooldown: i64 = row.try_get("cooldown_secs")?;
    Ok(AlertRule {
        id: row.try_get("id")?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        condition: serde_json::from_value(row.try_get("condition")?)
            .map_err(StorageError::Serialization)?,
        destination: serde_json::from_value(row.try_get("destination")?)
            .map_err(StorageError::Serialization)?,
        cooldown_secs: u64::try_from(cooldown).unwrap_or(0),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) async fn create(store: &PostgresStorage, r: &AlertRule) -> Result<(), StorageError> {
    sqlx::query(&format!(
        "INSERT INTO alert_rules ({COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
    ))
    .bind(r.id)
    .bind(r.tenant_id.as_str())
    .bind(&r.name)
    .bind(r.enabled)
    .bind(serde_json::to_value(&r.condition)?)
    .bind(serde_json::to_value(&r.destination)?)
    .bind(i64::try_from(r.cooldown_secs).unwrap_or(i64::MAX))
    .bind(r.created_at)
    .bind(r.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    id: Uuid,
) -> Result<Option<AlertRule>, StorageError> {
    let row = match tenant_id {
        Some(t) => {
            sqlx::query(&format!(
                "SELECT {COLUMNS} FROM alert_rules WHERE id = $1 AND tenant_id = $2"
            ))
            .bind(id)
            .bind(t.as_str())
            .fetch_optional(&store.pool)
            .await?
        }
        None => {
            sqlx::query(&format!("SELECT {COLUMNS} FROM alert_rules WHERE id = $1"))
                .bind(id)
                .fetch_optional(&store.pool)
                .await?
        }
    };
    row.as_ref().map(row_to_rule).transpose()
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<AlertRule>, StorageError> {
    let cap = i64::from(limit.min(1000));
    let rows = match tenant_id {
        Some(t) => sqlx::query(&format!(
            "SELECT {COLUMNS} FROM alert_rules WHERE tenant_id = $1 ORDER BY created_at LIMIT $2"
        ))
        .bind(t.as_str())
        .bind(cap)
        .fetch_all(&store.pool)
        .await?,
        None => {
            sqlx::query(&format!(
                "SELECT {COLUMNS} FROM alert_rules ORDER BY created_at LIMIT $1"
            ))
            .bind(cap)
            .fetch_all(&store.pool)
            .await?
        }
    };
    rows.iter().map(row_to_rule).collect()
}

pub(super) async fn update(store: &PostgresStorage, r: &AlertRule) -> Result<bool, StorageError> {
    let res = sqlx::query(
        r"UPDATE alert_rules SET name = $3, enabled = $4, condition = $5, destination = $6,
              cooldown_secs = $7, updated_at = $8
          WHERE id = $1 AND tenant_id = $2",
    )
    .bind(r.id)
    .bind(r.tenant_id.as_str())
    .bind(&r.name)
    .bind(r.enabled)
    .bind(serde_json::to_value(&r.condition)?)
    .bind(serde_json::to_value(&r.destination)?)
    .bind(i64::try_from(r.cooldown_secs).unwrap_or(i64::MAX))
    .bind(r.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn delete(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    id: Uuid,
) -> Result<bool, StorageError> {
    let mut tx = store.pool.begin().await?;
    let res = sqlx::query("DELETE FROM alert_rules WHERE id = $1 AND tenant_id = $2")
        .bind(id)
        .bind(tenant_id.as_str())
        .execute(&mut *tx)
        .await?;
    if res.rows_affected() == 1 {
        sqlx::query("DELETE FROM alert_rule_state WHERE rule_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn get_state(
    store: &PostgresStorage,
    rule_id: Uuid,
) -> Result<Option<AlertRuleState>, StorageError> {
    let row = sqlx::query("SELECT state, version FROM alert_rule_state WHERE rule_id = $1")
        .bind(rule_id)
        .fetch_optional(&store.pool)
        .await?;
    row.map(|r| {
        let mut s: AlertRuleState =
            serde_json::from_value(r.try_get("state")?).map_err(StorageError::Serialization)?;
        s.version = r.try_get("version")?;
        Ok(s)
    })
    .transpose()
}

pub(super) async fn cas_state(
    store: &PostgresStorage,
    state: &AlertRuleState,
    expected_version: i64,
) -> Result<bool, StorageError> {
    let json = serde_json::to_value(state)?;
    let res = if expected_version == 0 {
        sqlx::query(
            r"INSERT INTO alert_rule_state (rule_id, state, version, updated_at)
              VALUES ($1, $2, $3, NOW()) ON CONFLICT (rule_id) DO NOTHING",
        )
        .bind(state.rule_id)
        .bind(json)
        .bind(state.version)
        .execute(&store.pool)
        .await?
    } else {
        sqlx::query(
            r"UPDATE alert_rule_state SET state = $2, version = $3, updated_at = NOW()
              WHERE rule_id = $1 AND version = $4",
        )
        .bind(state.rule_id)
        .bind(json)
        .bind(state.version)
        .bind(expected_version)
        .execute(&store.pool)
        .await?
    };
    Ok(res.rows_affected() == 1)
}
