-- Dispatch-time effect classification resolves the current continuity owner
-- from its runtime-local instance without scanning every execution record.
CREATE UNIQUE INDEX IF NOT EXISTS idx_continuity_executions_current_instance
    ON continuity_executions (tenant_id, ((record->>'current_instance_id')));

CREATE INDEX IF NOT EXISTS idx_effect_receipts_unresolved_instance
    ON effect_receipts (tenant_id, continuity_id, instance_id, state, created_at DESC);
