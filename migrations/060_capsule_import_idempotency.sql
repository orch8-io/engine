CREATE TABLE IF NOT EXISTS capsule_imports (
    tenant_id TEXT NOT NULL,
    capsule_id UUID NOT NULL,
    destination_runtime_id UUID NOT NULL,
    instance_id UUID NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, capsule_id, destination_runtime_id),
    UNIQUE (instance_id)
);
CREATE INDEX IF NOT EXISTS idx_capsule_imports_instance
    ON capsule_imports (tenant_id, instance_id);
