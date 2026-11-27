CREATE TABLE migration_import_receipts (
    tenant_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    receipt JSONB NOT NULL,
    PRIMARY KEY (tenant_id, digest)
);
