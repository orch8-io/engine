-- no-transaction
-- list_instances pages `WHERE tenant_id = $1 ... ORDER BY updated_at DESC, id DESC`
-- (the API default listing). Without a matching index Postgres sorts every row
-- of the tenant on each page request.
--
-- CONCURRENTLY so the build does not block writes to task_instances; that
-- requires running outside a transaction (the directive above) and a
-- single statement per file. If the build is interrupted it leaves an
-- INVALID index that IF NOT EXISTS would skip: drop it with
-- `DROP INDEX CONCURRENTLY idx_task_instances_tenant_updated` and rerun.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_tenant_updated
    ON task_instances (tenant_id, updated_at DESC, id DESC);
