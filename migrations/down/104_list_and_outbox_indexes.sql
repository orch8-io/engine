-- Revert the list / outbox ordering indexes and restore idx_instances_tenant.
DROP INDEX IF EXISTS idx_webhook_outbox_inflight;
DROP INDEX IF EXISTS idx_webhook_outbox_pending_created;
CREATE INDEX IF NOT EXISTS idx_instances_tenant
    ON task_instances (tenant_id, state);
DROP INDEX IF EXISTS idx_task_instances_state_updated;
DROP INDEX IF EXISTS idx_task_instances_tenant_id_desc;
DROP INDEX IF EXISTS idx_task_instances_tenant_state_updated;
