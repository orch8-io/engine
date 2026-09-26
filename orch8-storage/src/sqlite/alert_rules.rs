use sqlx::Row;
use uuid::Uuid;

use orch8_types::alert::{AlertRule, AlertRuleState};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::SqliteStorage;
use super::approval_tokens::sortable_ts;
use super::helpers::{begin_immediate, parse_ts};

fn row_to_rule(row: &sqlx::sqlite::SqliteRow) -> Result<AlertRule, StorageError> {
    let id: String = row.try_get("id")?;
    let cooldown: i64 = row.try_get("cooldown_secs")?;
    Ok(AlertRule {
        id: Uuid::parse_str(&id).map_err(|e| StorageError::Query(e.to_string()))?,
        tenant_id: TenantId::unchecked(row.try_get::<String, _>("tenant_id")?),
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        condition: serde_json::from_str(&row.try_get::<String, _>("condition")?)
            .map_err(StorageError::Serialization)?,
        destination: serde_json::from_str(&row.try_get::<String, _>("destination")?)
            .map_err(StorageError::Serialization)?,
        cooldown_secs: u64::try_from(cooldown).unwrap_or(0),
        created_at: parse_ts(&row.try_get::<String, _>("created_at")?)?,
        updated_at: parse_ts(&row.try_get::<String, _>("updated_at")?)?,
    })
}

pub(super) async fn create(store: &SqliteStorage, r: &AlertRule) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO alert_rules
          (id, tenant_id, name, enabled, condition, destination, cooldown_secs, created_at, updated_at)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .bind(r.id.to_string())
    .bind(r.tenant_id.as_str())
    .bind(&r.name)
    .bind(r.enabled)
    .bind(serde_json::to_string(&r.condition)?)
    .bind(serde_json::to_string(&r.destination)?)
    .bind(i64::try_from(r.cooldown_secs).unwrap_or(i64::MAX))
    .bind(sortable_ts(r.created_at))
    .bind(sortable_ts(r.updated_at))
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    id: Uuid,
) -> Result<Option<AlertRule>, StorageError> {
    let row = match tenant_id {
        Some(t) => {
            sqlx::query("SELECT * FROM alert_rules WHERE id = ?1 AND tenant_id = ?2")
                .bind(id.to_string())
                .bind(t.as_str())
                .fetch_optional(&store.pool)
                .await?
        }
        None => {
            sqlx::query("SELECT * FROM alert_rules WHERE id = ?1")
                .bind(id.to_string())
                .fetch_optional(&store.pool)
                .await?
        }
    };
    row.as_ref().map(row_to_rule).transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
    limit: u32,
) -> Result<Vec<AlertRule>, StorageError> {
    let cap = i64::from(limit.min(1000));
    let rows = match tenant_id {
        Some(t) => {
            sqlx::query(
                "SELECT * FROM alert_rules WHERE tenant_id = ?1 ORDER BY created_at LIMIT ?2",
            )
            .bind(t.as_str())
            .bind(cap)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query("SELECT * FROM alert_rules ORDER BY created_at LIMIT ?1")
                .bind(cap)
                .fetch_all(&store.pool)
                .await?
        }
    };
    rows.iter().map(row_to_rule).collect()
}

pub(super) async fn update(store: &SqliteStorage, r: &AlertRule) -> Result<bool, StorageError> {
    let res = sqlx::query(
        r"UPDATE alert_rules SET name = ?3, enabled = ?4, condition = ?5, destination = ?6,
              cooldown_secs = ?7, updated_at = ?8
          WHERE id = ?1 AND tenant_id = ?2",
    )
    .bind(r.id.to_string())
    .bind(r.tenant_id.as_str())
    .bind(&r.name)
    .bind(r.enabled)
    .bind(serde_json::to_string(&r.condition)?)
    .bind(serde_json::to_string(&r.destination)?)
    .bind(i64::try_from(r.cooldown_secs).unwrap_or(i64::MAX))
    .bind(sortable_ts(r.updated_at))
    .execute(&store.pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn delete(
    store: &SqliteStorage,
    tenant_id: &TenantId,
    id: Uuid,
) -> Result<bool, StorageError> {
    let mut tx = begin_immediate(&store.pool).await?;
    let res = sqlx::query("DELETE FROM alert_rules WHERE id = ?1 AND tenant_id = ?2")
        .bind(id.to_string())
        .bind(tenant_id.as_str())
        .execute(&mut *tx)
        .await?;
    if res.rows_affected() == 1 {
        sqlx::query("DELETE FROM alert_rule_state WHERE rule_id = ?1")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(res.rows_affected() == 1)
}

pub(super) async fn get_state(
    store: &SqliteStorage,
    rule_id: Uuid,
) -> Result<Option<AlertRuleState>, StorageError> {
    let row = sqlx::query("SELECT state, version FROM alert_rule_state WHERE rule_id = ?1")
        .bind(rule_id.to_string())
        .fetch_optional(&store.pool)
        .await?;
    row.map(|r| {
        let mut s: AlertRuleState = serde_json::from_str(&r.try_get::<String, _>("state")?)
            .map_err(StorageError::Serialization)?;
        s.version = r.try_get("version")?;
        Ok(s)
    })
    .transpose()
}

pub(super) async fn cas_state(
    store: &SqliteStorage,
    state: &AlertRuleState,
    expected_version: i64,
) -> Result<bool, StorageError> {
    let json = serde_json::to_string(state)?;
    let now = sortable_ts(chrono::Utc::now());
    let res = if expected_version == 0 {
        sqlx::query(
            r"INSERT INTO alert_rule_state (rule_id, state, version, updated_at)
              VALUES (?1, ?2, ?3, ?4) ON CONFLICT (rule_id) DO NOTHING",
        )
        .bind(state.rule_id.to_string())
        .bind(json)
        .bind(state.version)
        .bind(now)
        .execute(&store.pool)
        .await?
    } else {
        sqlx::query(
            r"UPDATE alert_rule_state SET state = ?2, version = ?3, updated_at = ?4
              WHERE rule_id = ?1 AND version = ?5",
        )
        .bind(state.rule_id.to_string())
        .bind(json)
        .bind(state.version)
        .bind(now)
        .bind(expected_version)
        .execute(&store.pool)
        .await?
    };
    Ok(res.rows_affected() == 1)
}
