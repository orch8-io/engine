-- Revert the NULLS FIRST claim index and restore the 028 claim index.
CREATE INDEX IF NOT EXISTS idx_instances_claim_priority
    ON task_instances (state, priority DESC, next_fire_at ASC)
    WHERE state = 'scheduled';
DROP INDEX IF EXISTS idx_instances_claim_due;
