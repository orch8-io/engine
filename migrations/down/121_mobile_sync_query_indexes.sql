-- Revert the mobile-sync query indexes.
DROP INDEX IF EXISTS idx_mobile_commands_unacked_created;
DROP INDEX IF EXISTS idx_mobile_commands_acked_at;
DROP INDEX IF EXISTS idx_mobile_approvals_tenant_created_id;
DROP INDEX IF EXISTS idx_mobile_devices_tenant_registered;
