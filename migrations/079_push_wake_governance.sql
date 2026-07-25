-- Execution-aware push collapse evidence. Historical rows remain immutable;
-- a newer pending command terminally supersedes only its exact collapse scope.
ALTER TABLE push_wake_outbox ADD COLUMN IF NOT EXISTS execution_id TEXT;
ALTER TABLE push_wake_outbox ADD COLUMN IF NOT EXISTS topic TEXT;
ALTER TABLE push_wake_outbox ADD COLUMN IF NOT EXISTS collapse_key TEXT;
ALTER TABLE push_wake_outbox ADD COLUMN IF NOT EXISTS superseded_by TEXT;

CREATE INDEX IF NOT EXISTS idx_push_wake_collapse_pending
    ON push_wake_outbox(tenant_id, device_id, collapse_key, created_at)
    WHERE status = 'pending' AND collapse_key IS NOT NULL;
