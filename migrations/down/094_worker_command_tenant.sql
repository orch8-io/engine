-- Undo 094: drop the worker_commands owning-tenant column.
ALTER TABLE worker_commands DROP COLUMN IF EXISTS tenant_id;
