-- Plugin aliases are local to one tenant. The empty tenant remains shared.
-- allow-destructive: replace the global primary key with a tenant-scoped key in the same statement. All plugin rows are retained.
ALTER TABLE plugins DROP CONSTRAINT plugins_pkey, ADD PRIMARY KEY (tenant_id, name);
CREATE INDEX IF NOT EXISTS idx_plugins_name ON plugins (name, tenant_id);
