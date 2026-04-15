use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};

use super::helpers::{parse_ts, ts};
use super::SqliteStorage;

pub(super) async fn check_rate_limit(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    resource_key: &ResourceKey,
    now: DateTime<Utc>,
) -> Result<RateLimitCheck, StorageError> {
    let row = sqlx::query("SELECT * FROM rate_limits WHERE tenant_id=?1 AND resource_key=?2")
        .bind(&tenant_id.0)
        .bind(&resource_key.0)
        .fetch_optional(&storage.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

    if let Some(row) = row {
        let max_count: i64 = row.get("max_count");
        let window_seconds: i64 = row.get("window_seconds");
        let current: i64 = row.get("current_count");
        let window_start = parse_ts(row.get::<&str, _>("window_start"));

        let window_elapsed = (now - window_start).num_seconds();
        if window_elapsed >= window_seconds {
            // Reset window
            sqlx::query("UPDATE rate_limits SET current_count=1, window_start=?3 WHERE tenant_id=?1 AND resource_key=?2")
                .bind(&tenant_id.0).bind(&resource_key.0).bind(ts(now))
                .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
            return Ok(RateLimitCheck::Allowed);
        }

        if current < max_count {
            sqlx::query("UPDATE rate_limits SET current_count=current_count+1 WHERE tenant_id=?1 AND resource_key=?2")
                .bind(&tenant_id.0).bind(&resource_key.0)
                .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
            return Ok(RateLimitCheck::Allowed);
        }

        let retry_after = window_start + chrono::Duration::seconds(window_seconds);
        Ok(RateLimitCheck::Exceeded { retry_after })
    } else {
        Ok(RateLimitCheck::Allowed)
    }
}

pub(super) async fn upsert_rate_limit(
    storage: &SqliteStorage,
    limit: &RateLimit,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO rate_limits (id,tenant_id,resource_key,max_count,window_seconds,current_count,window_start) VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(tenant_id,resource_key) DO UPDATE SET max_count=?4, window_seconds=?5"
    )
    .bind(limit.id.to_string())
    .bind(&limit.tenant_id.0)
    .bind(&limit.resource_key.0)
    .bind(i64::from(limit.max_count))
    .bind(i64::from(limit.window_seconds))
    .bind(i64::from(limit.current_count))
    .bind(ts(limit.window_start))
    .execute(&storage.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
    Ok(())
}
