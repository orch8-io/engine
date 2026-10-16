-- Revert the worker_tasks tenant column and reaper/list indexes.
DROP INDEX IF EXISTS idx_worker_tasks_created;
DROP INDEX IF EXISTS idx_worker_tasks_stranded;
DROP INDEX IF EXISTS idx_worker_tasks_inflight_created;
DROP INDEX IF EXISTS idx_worker_tasks_untenanted_pending;
DROP INDEX IF EXISTS idx_worker_tasks_tenant_poll;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS tenant_id;
