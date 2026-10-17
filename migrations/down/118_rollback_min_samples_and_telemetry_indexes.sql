-- Revert 118. Restores the single-column tenant indexes first so telemetry
-- queries never run without a tenant index.
CREATE INDEX IF NOT EXISTS idx_telemetry_events_tenant ON telemetry_mobile_events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_errors_tenant ON telemetry_mobile_errors(tenant_id);
DROP INDEX IF EXISTS idx_telemetry_errors_tenant_received;
DROP INDEX IF EXISTS idx_telemetry_events_tenant_received;
DROP INDEX IF EXISTS idx_telemetry_errors_tenant_seq_received;
DROP INDEX IF EXISTS idx_telemetry_events_tenant_seq_received;
ALTER TABLE rollback_policies DROP COLUMN IF EXISTS min_samples;
