-- L4-11: task_instances churn settings.
--
-- * idx_instances_idempotency (010) and uq_task_instances_tenant_idempotency
--   (030) are byte-identical unique partial indexes; every insert and
--   non-HOT update maintained both. Keep the 030 one.
-- * fillfactor 85 leaves room on each page so the per-step updates can be
--   HOT more often (it applies to newly written pages only; rewrite existing
--   tables with pg_repack / VACUUM FULL in a maintenance window to apply it
--   retroactively).
-- * Per-table autovacuum: at the default scale factor (0.2) a 100M-row table
--   is vacuumed only every 20M dead tuples, and the partial indexes the
--   claim and reapers depend on fill with dead entries in between.
--
-- ALTER TABLE ... SET takes a SHARE UPDATE EXCLUSIVE lock (does not block
-- reads or writes) and rewrites nothing; safe online.
DROP INDEX IF EXISTS idx_instances_idempotency;

ALTER TABLE task_instances SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit = 2000
);
ALTER TABLE worker_tasks SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.05
);
ALTER TABLE webhook_outbox SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.05
);
ALTER TABLE signal_inbox SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.05
);
ALTER TABLE execution_tree SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.05
);
