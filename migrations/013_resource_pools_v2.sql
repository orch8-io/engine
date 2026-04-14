-- Upgrade resource_pools: add strategy, round_robin_index, timestamps.
-- Rename 'rotation' -> use 'strategy' column instead.
ALTER TABLE resource_pools ADD COLUMN IF NOT EXISTS strategy TEXT NOT NULL DEFAULT 'round_robin';
ALTER TABLE resource_pools ADD COLUMN IF NOT EXISTS round_robin_index INTEGER NOT NULL DEFAULT 0;
ALTER TABLE resource_pools ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE resource_pools ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Copy legacy rotation values to strategy if present.
UPDATE resource_pools SET strategy = rotation WHERE rotation IS NOT NULL;

-- Create separate pool_resources table for per-resource tracking.
CREATE TABLE IF NOT EXISTS pool_resources (
    id                UUID PRIMARY KEY,
    pool_id           UUID NOT NULL REFERENCES resource_pools(id) ON DELETE CASCADE,
    resource_key      TEXT NOT NULL,
    name              TEXT NOT NULL,
    weight            INTEGER NOT NULL DEFAULT 1,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    daily_cap         INTEGER NOT NULL DEFAULT 0,
    daily_usage       INTEGER NOT NULL DEFAULT 0,
    daily_usage_date  DATE,
    warmup_start      DATE,
    warmup_days       INTEGER NOT NULL DEFAULT 0,
    warmup_start_cap  INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pool_resources_pool_id ON pool_resources(pool_id);
CREATE INDEX IF NOT EXISTS idx_pool_resources_resource_key ON pool_resources(resource_key);
