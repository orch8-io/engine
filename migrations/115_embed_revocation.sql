-- EM-04: embed-token revocation. A sub-tenant row with status 'suspended'
-- makes every embed token of that sub-tenant fail; a revoked-jti row
-- revokes a single token. Both are checked by the embed extractor behind a
-- few-second in-memory cache. Revoked-token rows are purgeable once
-- `expires_at` has passed (the signed token is dead by then anyway).
CREATE TABLE IF NOT EXISTS sub_tenant_status (
    tenant_id  TEXT        NOT NULL,
    sub_tenant TEXT        NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'active',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sub_tenant)
);

CREATE TABLE IF NOT EXISTS embed_revoked_tokens (
    jti        UUID        PRIMARY KEY,
    tenant_id  TEXT        NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_embed_revoked_tokens_expires
    ON embed_revoked_tokens (expires_at);
