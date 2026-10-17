-- MS-01 / MS-12: auto-rollback hardening and telemetry index coverage.
--
-- 1. `rollback_policies.min_samples`: the error rate is only trusted once the
--    window holds at least this many observed runs, so a single forged or
--    unlucky error report can no longer unpublish a sequence for all devices.
--    Metadata-only ADD COLUMN with a constant default (no table rewrite);
--    old binaries ignore the column.
-- 2. Composite indexes so the per-report rollback check (and the dashboard)
--    stop scanning every row of the tenant:
--      telemetry_mobile_events (tenant_id, (payload->>'sequence_name'), received_at)
--      telemetry_mobile_errors (tenant_id, sequence_name, received_at)
--      telemetry_mobile_events / _errors (tenant_id, received_at)
-- 3. Drops the two single-column tenant indexes, which are strict prefixes of
--    the new composites (pure write amplification). DROP INDEX is not
--    destructive to data and old binaries never name these indexes.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create the indexes online before upgrading (outside a
-- transaction, one at a time); the IF NOT EXISTS below is then a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telemetry_events_tenant_seq_received
--       ON telemetry_mobile_events (tenant_id, (payload->>'sequence_name'), received_at);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telemetry_errors_tenant_seq_received
--       ON telemetry_mobile_errors (tenant_id, sequence_name, received_at);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telemetry_events_tenant_received
--       ON telemetry_mobile_events (tenant_id, received_at);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_telemetry_errors_tenant_received
--       ON telemetry_mobile_errors (tenant_id, received_at);

ALTER TABLE rollback_policies ADD COLUMN IF NOT EXISTS min_samples INTEGER NOT NULL DEFAULT 20;

CREATE INDEX IF NOT EXISTS idx_telemetry_events_tenant_seq_received
    ON telemetry_mobile_events (tenant_id, (payload->>'sequence_name'), received_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_errors_tenant_seq_received
    ON telemetry_mobile_errors (tenant_id, sequence_name, received_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_tenant_received
    ON telemetry_mobile_events (tenant_id, received_at);
CREATE INDEX IF NOT EXISTS idx_telemetry_errors_tenant_received
    ON telemetry_mobile_errors (tenant_id, received_at);

DROP INDEX IF EXISTS idx_telemetry_events_tenant;
DROP INDEX IF EXISTS idx_telemetry_errors_tenant;
