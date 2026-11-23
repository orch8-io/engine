CREATE TABLE migration_import_chunks (
    tenant_id TEXT NOT NULL,
    digest TEXT NOT NULL,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0 AND chunk_index < 64),
    payload BYTEA NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, digest, chunk_index)
);
CREATE INDEX migration_import_chunks_expiry ON migration_import_chunks (tenant_id, expires_at);
