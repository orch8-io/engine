use chrono::Utc;
use uuid::Uuid;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::pool::{PoolResource, ResourcePool};

use super::helpers::{row_to_pool, row_to_pool_resource, ts};
use super::SqliteStorage;

pub(super) async fn create(
    storage: &SqliteStorage,
    pool: &ResourcePool,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO resource_pools (id,tenant_id,name,strategy,round_robin_index,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7)")
        .bind(pool.id.to_string())
        .bind(&pool.tenant_id.0)
        .bind(&pool.name)
        .bind(serde_json::to_string(&pool.strategy)?.trim_matches('"'))
        .bind(pool.round_robin_index as i64)
        .bind(ts(pool.created_at))
        .bind(ts(pool.updated_at))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn get(
    storage: &SqliteStorage,
    id: Uuid,
) -> Result<Option<ResourcePool>, StorageError> {
    let row = sqlx::query("SELECT * FROM resource_pools WHERE id=?1")
        .bind(id.to_string())
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    row.as_ref().map(row_to_pool).transpose()
}

pub(super) async fn list(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
) -> Result<Vec<ResourcePool>, StorageError> {
    let rows = sqlx::query("SELECT * FROM resource_pools WHERE tenant_id=?1")
        .bind(&tenant_id.0)
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_pool).collect()
}

pub(super) async fn update_round_robin(
    storage: &SqliteStorage,
    pool_id: Uuid,
    index: u32,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE resource_pools SET round_robin_index=?2, updated_at=?3 WHERE id=?1")
        .bind(pool_id.to_string())
        .bind(index as i64)
        .bind(ts(Utc::now()))
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn delete(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM resource_pools WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn add_resource(
    storage: &SqliteStorage,
    r: &PoolResource,
) -> Result<(), StorageError> {
    sqlx::query("INSERT INTO pool_resources (id,pool_id,resource_key,name,weight,enabled,daily_cap,daily_usage,daily_usage_date,warmup_start,warmup_days,warmup_start_cap,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)")
        .bind(r.id.to_string())
        .bind(r.pool_id.to_string())
        .bind(&r.resource_key.0)
        .bind(&r.name)
        .bind(r.weight as i64)
        .bind(r.enabled as i32)
        .bind(r.daily_cap as i64)
        .bind(r.daily_usage as i64)
        .bind(r.daily_usage_date.map(|d| d.to_string()))
        .bind(r.warmup_start.map(|d| d.to_string()))
        .bind(r.warmup_days as i64)
        .bind(r.warmup_start_cap as i64)
        .bind(ts(r.created_at))
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn list_resources(
    storage: &SqliteStorage,
    pool_id: Uuid,
) -> Result<Vec<PoolResource>, StorageError> {
    let rows = sqlx::query("SELECT * FROM pool_resources WHERE pool_id=?1")
        .bind(pool_id.to_string())
        .fetch_all(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    rows.iter().map(row_to_pool_resource).collect()
}

pub(super) async fn update_resource(
    storage: &SqliteStorage,
    r: &PoolResource,
) -> Result<(), StorageError> {
    sqlx::query("UPDATE pool_resources SET name=?2, weight=?3, enabled=?4, daily_cap=?5, warmup_start=?6, warmup_days=?7, warmup_start_cap=?8 WHERE id=?1")
        .bind(r.id.to_string())
        .bind(&r.name)
        .bind(r.weight as i64)
        .bind(r.enabled as i32)
        .bind(r.daily_cap as i64)
        .bind(r.warmup_start.map(|d| d.to_string()))
        .bind(r.warmup_days as i64)
        .bind(r.warmup_start_cap as i64)
        .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn delete_resource(storage: &SqliteStorage, id: Uuid) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM pool_resources WHERE id=?1")
        .bind(id.to_string())
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}

pub(super) async fn increment_usage(
    storage: &SqliteStorage,
    resource_id: Uuid,
    today: chrono::NaiveDate,
) -> Result<(), StorageError> {
    let today_str = today.to_string();
    sqlx::query("UPDATE pool_resources SET daily_usage = CASE WHEN daily_usage_date = ?2 THEN daily_usage + 1 ELSE 1 END, daily_usage_date = ?2 WHERE id = ?1")
        .bind(resource_id.to_string())
        .bind(&today_str)
        .execute(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
