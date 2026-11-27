CREATE INDEX IF NOT EXISTS idx_triggers_tenant_slug ON triggers (tenant_id, slug COLLATE "C");
