CREATE INDEX IF NOT EXISTS idx_sessions_tenant_id ON sessions(tenant_id, id DESC);
