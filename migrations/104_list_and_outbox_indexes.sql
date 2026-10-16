-- L4-07 / L4-09: ordering indexes for the instance list endpoints and the
-- webhook outbox drain.
--
-- * (tenant_id, state, updated_at DESC, id DESC): tenant + state filtered
--   lists (and the admission COUNT, L4-06) without sorting the tenant's
--   whole history. Its (tenant_id, state) prefix supersedes
--   idx_instances_tenant, which is dropped.
-- * (tenant_id, id DESC): the keyset list (`ORDER BY id DESC`, jobs API)
--   walked the entire primary key backwards filtering by tenant (9 s at 4M
--   rows for a tenant with 10 matching rows).
-- * (state, updated_at DESC, id DESC): tenantless (self-host / admin) lists
--   and the DLQ `state = 'failed'` scan.
-- * webhook_outbox: the drain claims `status = 'pending' ORDER BY created_at`
--   and stale recovery filters `status = 'in_flight'`; both walked every
--   parked row every second on every node.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create them online before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_tenant_state_updated
--       ON task_instances (tenant_id, state, updated_at DESC, id DESC);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_tenant_id_desc
--       ON task_instances (tenant_id, id DESC);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_state_updated
--       ON task_instances (state, updated_at DESC, id DESC);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_webhook_outbox_pending_created
--       ON webhook_outbox (created_at) WHERE status = 'pending';
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_webhook_outbox_inflight
--       ON webhook_outbox (claimed_at) WHERE status = 'in_flight';
CREATE INDEX IF NOT EXISTS idx_task_instances_tenant_state_updated
    ON task_instances (tenant_id, state, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_instances_tenant_id_desc
    ON task_instances (tenant_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_task_instances_state_updated
    ON task_instances (state, updated_at DESC, id DESC);
DROP INDEX IF EXISTS idx_instances_tenant;

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_pending_created
    ON webhook_outbox (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_webhook_outbox_inflight
    ON webhook_outbox (claimed_at) WHERE status = 'in_flight';
