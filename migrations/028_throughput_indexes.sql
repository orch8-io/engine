-- Optimise the batched concurrency-count query introduced in the N+1 fix.
-- The existing idx_instances_concurrency only covers (concurrency_key) WHERE
-- state = 'running' — the GROUP BY query benefits from the key being the
-- leading column, which it already is. This is a no-op safety net; keep for
-- documentation that the index was validated.

-- Claim-due hot path: composite index for tenant-scoped priority ordering.
-- Covers the ROW_NUMBER window when max_instances_per_tenant > 0.
CREATE INDEX IF NOT EXISTS idx_instances_claim_priority
    ON task_instances (state, priority DESC, next_fire_at ASC)
    WHERE state = 'scheduled';
