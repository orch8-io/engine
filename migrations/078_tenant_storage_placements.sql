-- Authoritative tenant-to-storage placement. Epoch fences stale control-plane
-- writers while the primary key guarantees exactly one active placement.
CREATE TABLE IF NOT EXISTS tenant_storage_placements (
    tenant_id TEXT PRIMARY KEY,
    backend_id TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch > 0),
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tenant_storage_placements_backend
    ON tenant_storage_placements (backend_id);
