-- Add compression + size tracking to externalized_state.
--
-- `payload` (JSONB) is used when `compression IS NULL`.
-- `payload_bytes` (BYTEA) is used when `compression = 'zstd'`.
-- Legacy rows — written before this migration — have `compression = NULL`
-- and `payload` populated; they remain readable as-is.
ALTER TABLE externalized_state
    ADD COLUMN IF NOT EXISTS compression   TEXT,
    ADD COLUMN IF NOT EXISTS size_bytes    BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS payload_bytes BYTEA;

-- Allow payload to be NULL now that compressed rows store to payload_bytes instead.
ALTER TABLE externalized_state
    ALTER COLUMN payload DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_externalized_state_instance
    ON externalized_state(instance_id);
