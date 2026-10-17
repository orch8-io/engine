-- MS-02: nothing ever pruned `runtime_capabilities` (one row per phone,
-- re-written on every heartbeat, never deleted). The GC now deletes rows that
-- expired more than a retention period ago across all tenants
-- (`prune_expired_runtime_capabilities`: `WHERE expires_at < $cutoff`). The
-- only existing index is `(tenant_id, expires_at DESC)`, which cannot serve a
-- tenant-less range predicate, so add an `expires_at` index for the sweep.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create it online before upgrading (outside a transaction):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runtime_capabilities_expiry
--       ON runtime_capabilities (expires_at);
CREATE INDEX IF NOT EXISTS idx_runtime_capabilities_expiry
    ON runtime_capabilities (expires_at);
