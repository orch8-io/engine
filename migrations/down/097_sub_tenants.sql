-- Undo 097: sub-tenants, limits, execution ledger, embed themes, release target.
DROP TABLE IF EXISTS embed_themes;
DROP TABLE IF EXISTS sub_tenant_executions;
DROP TABLE IF EXISTS sub_tenant_limits;
ALTER TABLE workflow_releases DROP COLUMN IF EXISTS target;
DROP INDEX IF EXISTS idx_task_instances_sub_tenant;
ALTER TABLE task_instances DROP COLUMN IF EXISTS sub_tenant;
