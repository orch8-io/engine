use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::rows::{PoolResourceRow, ResourcePoolRow};
use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    pool: &orch8_types::pool::ResourcePool,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO resource_pools (id, tenant_id, name, strategy, round_robin_index, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(pool.id)
    .bind(&pool.tenant_id.0)
    .bind(&pool.name)
    .bind(serde_json::to_string(&pool.strategy)?.trim_matches('"'))
    .bind(pool.round_robin_index as i32)
    .bind(pool.created_at)
    .bind(pool.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    id: uuid::Uuid,
) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError> {
    let row = sqlx::query_as::<_, ResourcePoolRow>(
        "SELECT id, tenant_id, name, strategy, round_robin_index, created_at, updated_at FROM resource_pools WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(ResourcePoolRow::into_pool))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: &TenantId,
) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError> {
    let rows = sqlx::query_as::<_, ResourcePoolRow>(
        "SELECT id, tenant_id, name, strategy, round_robin_index, created_at, updated_at FROM resource_pools WHERE tenant_id = $1 ORDER BY name",
    )
    .bind(&tenant_id.0)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows.into_iter().map(ResourcePoolRow::into_pool).collect())
}

pub(super) async fn update_round_robin(
    store: &PostgresStorage,
    pool_id: uuid::Uuid,
    index: u32,
) -> Result<(), StorageError> {
    sqlx::query(
        "UPDATE resource_pools SET round_robin_index = $2, updated_at = NOW() WHERE id = $1",
    )
    .bind(pool_id)
    .bind(index as i32)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, id: uuid::Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM resource_pools WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn add_resource(
    store: &PostgresStorage,
    resource: &orch8_types::pool::PoolResource,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO pool_resources
          (id, pool_id, resource_key, name, weight, enabled, daily_cap, daily_usage, daily_usage_date,
           warmup_start, warmup_days, warmup_start_cap, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
    )
    .bind(resource.id)
    .bind(resource.pool_id)
    .bind(&resource.resource_key.0)
    .bind(&resource.name)
    .bind(resource.weight as i32)
    .bind(resource.enabled)
    .bind(resource.daily_cap as i32)
    .bind(resource.daily_usage as i32)
    .bind(resource.daily_usage_date)
    .bind(resource.warmup_start)
    .bind(resource.warmup_days as i32)
    .bind(resource.warmup_start_cap as i32)
    .bind(resource.created_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn list_resources(
    store: &PostgresStorage,
    pool_id: uuid::Uuid,
) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError> {
    let rows = sqlx::query_as::<_, PoolResourceRow>(
        r"SELECT id, pool_id, resource_key, name, weight, enabled, daily_cap, daily_usage, daily_usage_date,
                 warmup_start, warmup_days, warmup_start_cap, created_at
          FROM pool_resources WHERE pool_id = $1 ORDER BY name",
    )
    .bind(pool_id)
    .fetch_all(&store.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(PoolResourceRow::into_resource)
        .collect())
}

pub(super) async fn update_resource(
    store: &PostgresStorage,
    resource: &orch8_types::pool::PoolResource,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE pool_resources
          SET name = $2, weight = $3, enabled = $4, daily_cap = $5,
              warmup_start = $6, warmup_days = $7, warmup_start_cap = $8
          WHERE id = $1",
    )
    .bind(resource.id)
    .bind(&resource.name)
    .bind(resource.weight as i32)
    .bind(resource.enabled)
    .bind(resource.daily_cap as i32)
    .bind(resource.warmup_start)
    .bind(resource.warmup_days as i32)
    .bind(resource.warmup_start_cap as i32)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete_resource(
    store: &PostgresStorage,
    id: uuid::Uuid,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM pool_resources WHERE id = $1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn increment_usage(
    store: &PostgresStorage,
    resource_id: uuid::Uuid,
    today: chrono::NaiveDate,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE pool_resources
          SET daily_usage = CASE WHEN daily_usage_date = $2 THEN daily_usage + 1 ELSE 1 END,
              daily_usage_date = $2
          WHERE id = $1",
    )
    .bind(resource_id)
    .bind(today)
    .execute(&store.pool)
    .await?;
    Ok(())
}
