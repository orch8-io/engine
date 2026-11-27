CREATE INDEX IF NOT EXISTS idx_execution_handoff_history
    ON execution_handoffs (tenant_id, continuity_id, created_at DESC, id DESC);
