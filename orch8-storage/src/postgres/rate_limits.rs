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
    // Single round-trip, genuinely atomic check-and-increment.
    //
    // `current_state` locks the row (`FOR UPDATE`) and evaluates
    // `window_expired`/`current_count`/`max_count` against the *pre-update*
    // values; the `UPDATE ... FROM` then writes the new counters computed
    // from that same locked snapshot, and `RETURNING` reports exactly the
    // values the decision was based on. Earlier this was two statements (an
    // UPDATE, then -- only on a zero-row match -- a separate SELECT to
    // compute `retry_after`), which raced: a concurrent writer could reset
    // the window between the two statements, so `retry_after` could reflect
    // a window this call never actually saw. Locking + deciding + writing in
    // one statement removes that window entirely.
    let row = sqlx::query_as::<_, CheckResult>(
        r"
        WITH current_state AS (
            SELECT
                tenant_id, resource_key, current_count, max_count,
                window_start, window_seconds,
                (window_start + make_interval(secs => window_seconds::double precision) <= $3)
                    AS window_expired
            FROM rate_limits
            WHERE tenant_id = $1 AND resource_key = $2
            FOR UPDATE
        )
        UPDATE rate_limits r
        SET
            current_count = CASE
                WHEN cs.window_expired THEN 1
                WHEN cs.current_count < cs.max_count THEN cs.current_count + 1
                ELSE cs.current_count
            END,
            window_start = CASE
                WHEN cs.window_expired THEN $3
                ELSE cs.window_start
            END
        FROM current_state cs
        WHERE r.tenant_id = cs.tenant_id AND r.resource_key = cs.resource_key
        RETURNING
            cs.window_expired, cs.current_count, cs.max_count,
            cs.window_start, cs.window_seconds
        ",
    )
    .bind(tenant_id.as_str())
    .bind(resource_key.as_str())
    .bind(now)
    .fetch_optional(&store.pool)
    .await?;

    match row {
        // No row: no rate limit configured for this (tenant, resource) pair.
        None => Ok(RateLimitCheck::Allowed),
        Some(r) if r.window_expired || r.current_count < r.max_count => Ok(RateLimitCheck::Allowed),
        Some(r) => {
            let window_end =
                r.window_start + chrono::Duration::seconds(i64::from(r.window_seconds));
            Ok(RateLimitCheck::Exceeded {
                retry_after: window_end,
            })
        }
    }
}

/// Row returned from the atomic lock-decide-write query.
#[derive(sqlx::FromRow)]
struct CheckResult {
    window_expired: bool,
    current_count: i32,
    max_count: i32,
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
    .bind(limit.tenant_id.as_str())
    .bind(limit.resource_key.as_str())
    .bind(limit.max_count)
    .bind(limit.window_seconds)
    .bind(limit.current_count)
    .bind(limit.window_start)
    .execute(&store.pool)
    .await?;
    Ok(())
}
