//! Tenant placement policies, global rate budgets, and the pending-task
//! backlog grouping used by the autoscaling metrics (migration 097).

use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::instance::Priority;
use orch8_types::placement::{
    PlacementPolicies, QueueDepthRow, RateBudget, RateBudgetCheck, take_token,
};

use super::PostgresStorage;

pub(super) async fn get_policies(
    store: &PostgresStorage,
    tenant_id: &TenantId,
) -> Result<PlacementPolicies, StorageError> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT policies FROM placement_policies WHERE tenant_id = $1")
            .bind(tenant_id.as_str())
            .fetch_optional(&store.pool)
            .await?;
    match row {
        None => Ok(PlacementPolicies::default()),
        Some((value,)) => serde_json::from_value(value).map_err(StorageError::Serialization),
    }
}

pub(super) async fn put_policies(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    policies: &PlacementPolicies,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO placement_policies (tenant_id, policies, updated_at)
          VALUES ($1, $2, NOW())
          ON CONFLICT (tenant_id) DO UPDATE SET policies = EXCLUDED.policies, updated_at = NOW()",
    )
    .bind(tenant_id.as_str())
    .bind(serde_json::to_value(policies)?)
    .execute(&store.pool)
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct BudgetRow {
    tenant_id: String,
    budget_key: String,
    capacity: i32,
    refill_per_sec: f64,
    tokens: f64,
    updated_at: DateTime<Utc>,
}

impl From<BudgetRow> for RateBudget {
    fn from(row: BudgetRow) -> Self {
        Self {
            tenant_id: row.tenant_id,
            key: row.budget_key,
            capacity: u32::try_from(row.capacity).unwrap_or(1),
            refill_per_sec: row.refill_per_sec,
            tokens: row.tokens,
            updated_at: row.updated_at,
        }
    }
}

/// Create a budget full, or update its shape keeping the current tokens
/// (clamped to the new capacity).
pub(super) async fn upsert_budget(
    store: &PostgresStorage,
    budget: &RateBudget,
) -> Result<RateBudget, StorageError> {
    let row = sqlx::query_as::<_, BudgetRow>(
        r"INSERT INTO rate_budgets (tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at)
          VALUES ($1, $2, $3, $4, $3, $5)
          ON CONFLICT (tenant_id, budget_key) DO UPDATE
          SET capacity = EXCLUDED.capacity,
              refill_per_sec = EXCLUDED.refill_per_sec,
              tokens = LEAST(rate_budgets.tokens, EXCLUDED.capacity::double precision)
          RETURNING tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at",
    )
    .bind(&budget.tenant_id)
    .bind(&budget.key)
    .bind(i32::try_from(budget.capacity).unwrap_or(i32::MAX))
    .bind(budget.refill_per_sec)
    .bind(budget.updated_at)
    .fetch_one(&store.pool)
    .await?;
    Ok(row.into())
}

pub(super) async fn list_budgets(
    store: &PostgresStorage,
    tenant_id: &TenantId,
) -> Result<Vec<RateBudget>, StorageError> {
    let rows = sqlx::query_as::<_, BudgetRow>(
        "SELECT tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at \
         FROM rate_budgets WHERE tenant_id = $1 ORDER BY budget_key",
    )
    .bind(tenant_id.as_str())
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

pub(super) async fn delete_budget(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    key: &str,
) -> Result<bool, StorageError> {
    let result = sqlx::query("DELETE FROM rate_budgets WHERE tenant_id = $1 AND budget_key = $2")
        .bind(tenant_id.as_str())
        .bind(key)
        .execute(&store.pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Take one token under a row lock so every engine node shares one bucket.
pub(super) async fn take_budget_token(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    key: &str,
    now: DateTime<Utc>,
) -> Result<RateBudgetCheck, StorageError> {
    let mut tx = store.pool.begin().await?;
    let row = sqlx::query_as::<_, BudgetRow>(
        "SELECT tenant_id, budget_key, capacity, refill_per_sec, tokens, updated_at \
         FROM rate_budgets WHERE tenant_id = $1 AND budget_key = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_str())
    .bind(key)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(RateBudgetCheck::Unconfigured);
    };
    // Never move `updated_at` backwards (clock skew between nodes).
    let now = now.max(row.updated_at);
    let capacity = u32::try_from(row.capacity).unwrap_or(1);
    let (tokens, check) = take_token(
        capacity,
        row.refill_per_sec,
        row.tokens,
        row.updated_at,
        now,
    );
    sqlx::query(
        "UPDATE rate_budgets SET tokens = $3, updated_at = $4 WHERE tenant_id = $1 AND budget_key = $2",
    )
    .bind(tenant_id.as_str())
    .bind(key)
    .bind(tokens)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(check)
}

/// Pending, dispatch-bound worker tasks of live instances grouped by
/// tenant, handler, requirements, and instance priority.
pub(super) async fn pending_depth(
    store: &PostgresStorage,
    limit: u32,
) -> Result<Vec<QueueDepthRow>, StorageError> {
    let rows: Vec<(String, String, serde_json::Value, i16, i64)> = sqlx::query_as(
        r"SELECT ti.tenant_id, wt.handler_name, wt.requirements, ti.priority, COUNT(*)
          FROM worker_tasks wt
          JOIN task_instances ti ON ti.id = wt.instance_id
          WHERE wt.state = 'pending' AND NOT wt.awaiting_dispatch
            AND ti.state NOT IN ('completed', 'failed', 'cancelled')
          GROUP BY ti.tenant_id, wt.handler_name, wt.requirements, ti.priority
          ORDER BY COUNT(*) DESC
          LIMIT $1",
    )
    .bind(i64::from(limit))
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(|(tenant_id, handler_name, requirements, priority, count)| {
            Ok(QueueDepthRow {
                tenant_id,
                handler_name,
                requirements: serde_json::from_value(requirements)
                    .map_err(StorageError::Serialization)?,
                priority: Priority::try_from(priority).unwrap_or_default(),
                count: u64::try_from(count).unwrap_or(0),
            })
        })
        .collect()
}
