-- list_instances pages `WHERE tenant_id = $1 ... ORDER BY updated_at DESC, id DESC`
-- (the API default listing). Without a matching index Postgres sorts every row
-- of the tenant on each page request.
--
-- Deliberately NOT `CONCURRENTLY`: a concurrent build waits for every open
-- transaction, including other nodes blocked on sqlx's migration advisory
-- lock, so simultaneous migrators deadlock. The plain build blocks writes to
-- task_instances while it runs; on large installs pre-create the index online
-- before upgrading and this statement becomes a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_tenant_updated
--       ON task_instances (tenant_id, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_instances_tenant_updated
    ON task_instances (tenant_id, updated_at DESC, id DESC);
