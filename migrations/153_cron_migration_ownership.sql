CREATE TABLE cron_migration_imports (
    schedule_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    migration_id UUID NOT NULL,
    ownership_epoch BIGINT NOT NULL CHECK (ownership_epoch > 0),
    last_fire_at TIMESTAMPTZ,
    snapshot_sha256 TEXT NOT NULL
);
