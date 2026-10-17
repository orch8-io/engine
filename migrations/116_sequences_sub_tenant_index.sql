-- EM-02: the embed sequence listing filters `sequences` by the owning
-- sub-tenant, which lives in the `definition` JSON. Expression index so the
-- visibility filter is an index scan rather than a tenant-wide JSON scan.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create it online before upgrading (outside a transaction):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sequences_owner_sub_tenant
--       ON sequences (tenant_id, (definition->>'sub_tenant'), namespace, name, version DESC)
--       WHERE definition->>'sub_tenant' IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sequences_owner_sub_tenant
    ON sequences (tenant_id, (definition->>'sub_tenant'), namespace, name, version DESC)
    WHERE definition->>'sub_tenant' IS NOT NULL;
