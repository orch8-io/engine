-- Remove the tenant and session ID index added for session inventory pagination.
DROP INDEX IF EXISTS idx_sessions_tenant_id;
