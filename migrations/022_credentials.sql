-- Credentials registry: holds shared secrets (API keys, OAuth2 tokens, basic auth)
-- referenced from step params via the `credentials://<id>` URI scheme. The step
-- handler resolves the reference at dispatch time, so the sequence JSON never
-- contains raw secrets.
CREATE TABLE IF NOT EXISTS credentials (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL DEFAULT '',
    name          TEXT NOT NULL,
    kind          TEXT NOT NULL DEFAULT 'api_key',
    value         TEXT NOT NULL,
    expires_at    TIMESTAMPTZ,
    refresh_url   TEXT,
    refresh_token TEXT,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    description   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_credentials_tenant ON credentials (tenant_id);
-- Background refresh loop scans this index for oauth2 credentials nearing expiry.
CREATE INDEX IF NOT EXISTS idx_credentials_expires ON credentials (expires_at)
    WHERE kind = 'oauth2' AND refresh_url IS NOT NULL AND enabled = TRUE;
