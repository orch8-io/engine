use chrono::{DateTime, Utc};

use orch8_types::error::StorageError;
use orch8_types::ids::{ResourceKey, TenantId};
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};

use super::rows::RateLimitRow;
use super::PostgresStorage;

pub(super) async fn check_rate_limit(
    store: &PostgresStorage,
    tenant_id: &TenantId,
    resource_key: &ResourceKey,
    now: DateTime<Utc>,
) -> Result<RateLimitCheck, StorageError> {
    // Atomic: read the rate limit, check window, increment or reset.
    let row = sqlx::query_as::<_, RateLimitRow>(
        "SELECT id, tenant_id, resource_key, max_count, window_seconds, current_count, window_start FROM rate_limits WHERE tenant_id = $1 AND resource_key = $2",
    )
    .bind(&tenant_id.0)
    .bind(&resource_key.0)
    .fetch_optional(&store.pool)
    .await?;

    let Some(rl) = row else {
        // No rate limit configured for this resource — allowed.
        return Ok(RateLimitCheck::Allowed);
    };

    let window_end = rl.window_start + chrono::Duration::seconds(i64::from(rl.window_seconds));

    if now >= window_end {
        // Window expired — reset and allow.
        sqlx::query("UPDATE rate_limits SET current_count = 1, window_start = $2 WHERE id = $1")
            .bind(rl.id)
            .bind(now)
            .execute(&store.pool)
            .await?;
        return Ok(RateLimitCheck::Allowed);
    }

    if rl.current_count < rl.max_count {
        // Within window and under limit — increment and allow.
        sqlx::query("UPDATE rate_limits SET current_count = current_count + 1 WHERE id = $1")
            .bind(rl.id)
            .execute(&store.pool)
            .await?;
        return Ok(RateLimitCheck::Allowed);
    }

    // Exceeded — return retry_after as window end.
    Ok(RateLimitCheck::Exceeded {
        retry_after: window_end,
    })
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
