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
    // Atomic check-and-increment inside a serialised transaction.
    //
    // `BEGIN IMMEDIATE` acquires a RESERVED lock up-front, preventing
    // concurrent writers from interleaving between our SELECT and UPDATE.
    // sqlx's `begin()` always uses DEFERRED, so we acquire a raw connection
    // and start the transaction manually.
    let mut conn = storage.pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

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
          AND (
                (julianday(?3) - julianday(window_start)) * 86400 >= window_seconds
                OR current_count < max_count
              )
        ",
    )
    .bind(&tenant_id.0)
    .bind(&resource_key.0)
    .bind(&now_str)
    .execute(&mut *conn)
    .await?;

    if result.rows_affected() > 0 {
        sqlx::query("COMMIT").execute(&mut *conn).await?;
        return Ok(RateLimitCheck::Allowed);
    }

    // UPDATE matched nothing — either no row exists or limit is reached.
    let row = sqlx::query(
        "SELECT window_start, window_seconds FROM rate_limits WHERE tenant_id=?1 AND resource_key=?2",
    )
    .bind(&tenant_id.0)
    .bind(&resource_key.0)
    .fetch_optional(&mut *conn)
    .await
    ?;

    sqlx::query("COMMIT").execute(&mut *conn).await?;

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
    .bind(&limit.tenant_id.0)
    .bind(&limit.resource_key.0)
    .bind(i64::from(limit.max_count))
    .bind(i64::from(limit.window_seconds))
    .bind(i64::from(limit.current_count))
    .bind(ts(limit.window_start))
    .execute(&storage.pool).await?;
    Ok(())
}
