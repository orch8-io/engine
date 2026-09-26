-- Revocable public progress links. Only the SHA-256 of the token is stored.
CREATE TABLE IF NOT EXISTS progress_shares (
    id UUID PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    instance_id UUID NOT NULL,
    allowed_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_progress_shares_instance
    ON progress_shares (tenant_id, instance_id);
