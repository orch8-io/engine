CREATE UNIQUE INDEX IF NOT EXISTS idx_compensation_runs_active
    ON compensation_runs (tenant_id, continuity_id)
    WHERE state IN ('planned', 'running', 'awaiting_verification');
