use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::*;
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};

use super::SqliteStorage;
use super::helpers::{begin_immediate, parse_ts, ts};

pub(super) async fn check_rate_limit(
    storage: &SqliteStorage,
    tenant_id: &TenantId,
    resource_key: &ResourceKey,
    now: DateTime<Utc>,
) -> Result<RateLimitCheck, StorageError> {
    // Keep the write reservation while deciding and incrementing. SQLx owns
    // transaction cleanup, including when this future is cancelled.
    let mut tx = begin_immediate(&storage.pool).await?;
    let check = check_rate_limit_inner(&mut tx, tenant_id, resource_key, now).await?;
    tx.commit().await?;
    Ok(check)
}

/// Fallible body of [`check_rate_limit`], run inside an already-open
/// `BEGIN IMMEDIATE` transaction. Never commits/rolls back — the caller owns
/// transaction lifecycle so a single rollback covers every `?` site here.
async fn check_rate_limit_inner(
    conn: &mut sqlx::SqliteConnection,
    tenant_id: &TenantId,
    resource_key: &ResourceKey,
    now: DateTime<Utc>,
) -> Result<RateLimitCheck, StorageError> {
    let now_str = ts(now);

    // Atomic UPDATE: reset window if expired, otherwise increment if under limit.
    let result = sqlx::query(
        r"
        UPDATE rate_limits
        SET
            current_count = CASE
                WHEN (julianday(?3) - julianday(window_start)) * 86400 >= window_seconds
                    THEN 1
                ELSE current_count + 1
            END,
            window_start = CASE
                WHEN (julianday(?3) - julianday(window_start)) * 86400 >= window_seconds
                    THEN ?3
                ELSE window_start
            END
        WHERE tenant_id = ?1
          AND resource_key = ?2
          AND max_count > 0
          AND (
                (julianday(?3) - julianday(window_start)) * 86400 >= window_seconds
                OR current_count < max_count
              )
        ",
    )
    .bind(tenant_id.as_str())
    .bind(resource_key.as_str())
    .bind(&now_str)
    .execute(&mut *conn)
    .await?;

    if result.rows_affected() > 0 {
        return Ok(RateLimitCheck::Allowed);
    }

    // UPDATE matched nothing — either no row exists or limit is reached.
    let row = sqlx::query(
        "SELECT window_start, window_seconds FROM rate_limits WHERE tenant_id=?1 AND resource_key=?2",
    )
    .bind(tenant_id.as_str())
    .bind(resource_key.as_str())
    .fetch_optional(&mut *conn)
    .await?;

    match row {
        None => Ok(RateLimitCheck::Allowed),
        Some(row) => {
            let window_start = parse_ts(row.get::<&str, _>("window_start"))?;
            let window_seconds: i64 = row.get("window_seconds");
            let retry_after = window_start + chrono::Duration::seconds(window_seconds);
            Ok(RateLimitCheck::Exceeded { retry_after })
        }
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
    .bind(limit.tenant_id.as_str())
    .bind(limit.resource_key.as_str())
    .bind(i64::from(limit.max_count))
    .bind(i64::from(limit.window_seconds))
    .bind(i64::from(limit.current_count))
    .bind(ts(limit.window_start))
    .execute(&storage.pool).await?;
    Ok(())
}
