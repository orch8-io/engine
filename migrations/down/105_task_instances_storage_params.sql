-- Revert the task_instances churn settings and restore the duplicate index.
ALTER TABLE execution_tree RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
ALTER TABLE signal_inbox RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
ALTER TABLE webhook_outbox RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
ALTER TABLE worker_tasks RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor);
ALTER TABLE task_instances RESET (
    fillfactor,
    autovacuum_vacuum_scale_factor,
    autovacuum_analyze_scale_factor,
    autovacuum_vacuum_cost_limit
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_idempotency
    ON task_instances (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
