-- no-transaction
-- Rollback 084_task_instances_tenant_updated_index.sql
DROP INDEX CONCURRENTLY IF EXISTS idx_task_instances_tenant_updated;
