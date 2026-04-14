use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::{ResourceKey, TenantId};
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};

use super::PostgresStorage;

pub(super) async fn check_rate_limit(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    resource_key: &ResourceKey,
    now: DateTime<Utc>,
) -> Result<RateLimitCheck, StorageError> {
    // Atomic check-and-increment using a single UPDATE … RETURNING.
    //
    // The UPDATE handles two cases in one statement:
    //   1. Window expired  → reset current_count to 1 and start a new window.
    //   2. Window still active and under limit → increment current_count.
    //
    // If the WHERE clause matches no rows (either no rate-limit row exists,
    // or the limit is already reached within the active window) the UPDATE
    // returns nothing, and we fall through to distinguish the two sub-cases
    // with a cheap SELECT.
    let row = sqlx::query_as::<_, AtomicResult>(
        r"
        UPDATE rate_limits
        SET
            current_count = CASE
                WHEN window_start + make_interval(secs => window_seconds::double precision) <= $3
                    THEN 1
                ELSE current_count + 1
            END,
            window_start = CASE
                WHEN window_start + make_interval(secs => window_seconds::double precision) <= $3
                    THEN $3
                ELSE window_start
            END
        WHERE tenant_id = $1
          AND resource_key = $2
          AND (
                -- window expired (always allow reset)
                window_start + make_interval(secs => window_seconds::double precision) <= $3
                OR
                -- window active and still under limit
                current_count < max_count
              )
        RETURNING window_start, window_seconds
        ",
    )
    .bind(&tenant_id.0)
    .bind(&resource_key.0)
    .bind(now)
    .fetch_optional(&store.pool)
    .await?;

    if row.is_some() {
        // The UPDATE matched and incremented — caller is allowed.
        return Ok(RateLimitCheck::Allowed);
    }

    // UPDATE matched nothing. Either the row doesn't exist (no limit
    // configured) or the limit was reached. A simple SELECT tells us which.
    let info = sqlx::query_as::<_, WindowInfo>(
        "SELECT window_start, window_seconds FROM rate_limits WHERE tenant_id = $1 AND resource_key = $2",
    )
    .bind(&tenant_id.0)
    .bind(&resource_key.0)
    .fetch_optional(&store.pool)
    .await?;

    match info {
        None => Ok(RateLimitCheck::Allowed),
        Some(info) => {
            let window_end =
                info.window_start + chrono::Duration::seconds(i64::from(info.window_seconds));
            Ok(RateLimitCheck::Exceeded {
                retry_after: window_end,
            })
        }
    }
}

/// Minimal row returned from the atomic UPDATE … RETURNING.
#[derive(sqlx::FromRow)]
struct AtomicResult {
    #[allow(dead_code)]
    window_start: DateTime<Utc>,
    #[allow(dead_code)]
    window_seconds: i32,
}

/// Used only for the fallback SELECT when the UPDATE matched zero rows.
#[derive(sqlx::FromRow)]
struct WindowInfo {
    window_start: DateTime<Utc>,
    window_seconds: i32,
}

pub(super) async fn upsert_rate_limit(
    store: &PostgresStorage,
    limit: &RateLimit,
) -> Result<(), StorageError> {
    sqlx::query(
        r"
        INSERT INTO rate_limits (id, tenant_id, resource_key, max_count, window_seconds, current_count, window_start)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (tenant_id, resource_key) DO UPDATE
        SET max_count = $4, window_seconds = $5
        ",
    )
    .bind(limit.id)
    .bind(&limit.tenant_id.0)
    .bind(&limit.resource_key.0)
    .bind(limit.max_count)
    .bind(limit.window_seconds)
    .bind(limit.current_count)
    .bind(limit.window_start)
    .execute(&store.pool)
    .await?;
    Ok(())
}
