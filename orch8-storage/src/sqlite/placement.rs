//! Tenant placement policies, global rate budgets, and the pending-task
//! backlog grouping used by the autoscaling metrics (Postgres migration 098).

use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::Priority;
use orch8_types::placement::{
    PlacementPolicies, QueueDepthRow, RateBudget, RateBudgetCheck, take_token,
};

use super::SqliteStorage;
use super::helpers::{begin_immediate, parse_ts, ts};

pub(super) async fn get_policies(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
) -> Result<PlacementPolicies, StorageError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT policies FROM placement_policies WHERE tenant_id = ?1")
            .bind(tenant_id.as_str())
            .fetch_optional(&storage.pool)
            .await?;
    match row {
        None => Ok(PlacementPolicies::default()),
        Some((text,)) => serde_json::from_str(&text).map_err(StorageError::Serialization),
    }
}

pub(super) async fn put_policies(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    policies: &PlacementPolicies,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO placement_policies (tenant_id, policies, updated_at) VALUES (?1, ?2, ?3) \
         ON CONFLICT (tenant_id) DO UPDATE SET policies = excluded.policies, updated_at = excluded.updated_at",
    )
    .bind(tenant_id.as_str())
    .bind(serde_json::to_string(policies)?)
    .bind(ts(Utc::now()))
    .execute(&storage.pool)
    .await?;
    Ok(())
}

fn row_to_budget(row: &sqlx::sqlite::SqliteRow) -> Result<RateBudget, StorageError> {
    Ok(RateBudget {
        tenant_id: row.get("tenant_id"),
        key: row.get("budget_key"),
        capacity: u32::try_from(row.get::<i64, _>("capacity")).unwrap_or(1),
        refill_per_sec: row.get("refill_per_sec"),
        tokens: row.get("tokens"),
        updated_at: parse_ts(row.get::<&str, _>("updated_at"))?,
    })
}

const BUDGET_COLUMNS: &str = "tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at";

pub(super) async fn upsert_budget(
    storage: &SqliteStorage,
    budget: &RateBudget,
) -> Result<RateBudget, StorageError> {
    let mut tx = begin_immediate(&storage.pool).await?;
    sqlx::query(
        "INSERT INTO rate_budgets (tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?3, ?5) \
         ON CONFLICT (tenant_id, budget_key) DO UPDATE SET \
           capacity = excluded.capacity, refill_per_sec = excluded.refill_per_sec, \
           tokens = MIN(rate_budgets.tokens, excluded.capacity)",
    )
    .bind(&budget.tenant_id)
    .bind(&budget.key)
    .bind(i64::from(budget.capacity))
    .bind(budget.refill_per_sec)
    .bind(ts(budget.updated_at))
    .execute(&mut *tx)
    .await?;
    let row = sqlx::query(&format!(
        "SELECT {BUDGET_COLUMNS} FROM rate_budgets WHERE tenant_id = ?1 AND budget_key = ?2"
    ))
    .bind(&budget.tenant_id)
    .bind(&budget.key)
    .fetch_one(&mut *tx)
    .await?;
    let stored = row_to_budget(&row)?;
    tx.commit().await?;
    Ok(stored)
}

pub(super) async fn list_budgets(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
) -> Result<Vec<RateBudget>, StorageError> {
    let rows = sqlx::query(&format!(
        "SELECT {BUDGET_COLUMNS} FROM rate_budgets WHERE tenant_id = ?1 ORDER BY budget_key"
    ))
    .bind(tenant_id.as_str())
    .fetch_all(&storage.pool)
    .await?;
    rows.iter().map(row_to_budget).collect()
}

pub(super) async fn delete_budget(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    key: &str,
) -> Result<bool, StorageError> {
    let result = sqlx::query("DELETE FROM rate_budgets WHERE tenant_id = ?1 AND budget_key = ?2")
        .bind(tenant_id.as_str())
        .bind(key)
        .execute(&storage.pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub(super) async fn take_budget_token(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    key: &str,
    now: DateTime<Utc>,
) -> Result<RateBudgetCheck, StorageError> {
    // BEGIN IMMEDIATE holds the write reservation across read-decide-write.
    let mut tx = begin_immediate(&storage.pool).await?;
    let row = sqlx::query(&format!(
        "SELECT {BUDGET_COLUMNS} FROM rate_budgets WHERE tenant_id = ?1 AND budget_key = ?2"
    ))
    .bind(tenant_id.as_str())
    .bind(key)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(RateBudgetCheck::Unconfigured);
    };
    let budget = row_to_budget(&row)?;
    let now = now.max(budget.updated_at);
    let (tokens, check) = take_token(
        budget.capacity,
        budget.refill_per_sec,
        budget.tokens,
        budget.updated_at,
        now,
    );
    sqlx::query(
        "UPDATE rate_budgets SET tokens = ?3, updated_at = ?4 WHERE tenant_id = ?1 AND budget_key = ?2",
    )
    .bind(tenant_id.as_str())
    .bind(key)
    .bind(tokens)
    .bind(ts(now))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(check)
}

pub(super) async fn pending_depth(
    storage: &SqliteStorage,
    limit: u32,
) -> Result<Vec<QueueDepthRow>, StorageError> {
    let rows = sqlx::query(
        "SELECT ti.tenant_id AS tenant_id, wt.handler_name AS handler_name, \
                wt.requirements AS requirements, ti.priority AS priority, COUNT(*) AS n \
         FROM worker_tasks wt JOIN task_instances ti ON ti.id = wt.instance_id \
         WHERE wt.state = 'pending' AND wt.awaiting_dispatch = 0 \
           AND ti.state NOT IN ('completed', 'failed', 'cancelled') \
         GROUP BY ti.tenant_id, wt.handler_name, wt.requirements, ti.priority \
         ORDER BY n DESC LIMIT ?1",
    )
    .bind(i64::from(limit))
    .fetch_all(&storage.pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(QueueDepthRow {
                tenant_id: row.get("tenant_id"),
                handler_name: row.get("handler_name"),
                requirements: serde_json::from_str(row.get::<&str, _>("requirements"))
                    .map_err(StorageError::Serialization)?,
                priority: i16::try_from(row.get::<i64, _>("priority"))
                    .ok()
                    .and_then(|value| Priority::try_from(value).ok())
                    .unwrap_or_default(),
                count: u64::try_from(row.get::<i64, _>("n")).unwrap_or(0),
            })
        })
        .collect()
}
