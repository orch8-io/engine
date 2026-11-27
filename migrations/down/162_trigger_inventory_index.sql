-- Remove the tenant and slug index added for trigger inventory lookups.
DROP INDEX IF EXISTS idx_triggers_tenant_slug;
