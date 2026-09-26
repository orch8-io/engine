-- Prompt registry: immutable, tenant-scoped prompt versions plus movable
-- labels (`production`, `canary`, ...) referenced from `llm_call`'s `prompt`
-- param. Versions are append-only; the full version lives in `record`.
CREATE TABLE IF NOT EXISTS prompt_versions (
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    content_hash TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name, version)
);

CREATE TABLE IF NOT EXISTS prompt_labels (
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    label TEXT NOT NULL,
    version INTEGER NOT NULL,
    canary_version INTEGER,
    canary_percent SMALLINT NOT NULL DEFAULT 0
        CHECK (canary_percent BETWEEN 0 AND 100),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name, label),
    FOREIGN KEY (tenant_id, name, version)
        REFERENCES prompt_versions (tenant_id, name, version),
    FOREIGN KEY (tenant_id, name, canary_version)
        REFERENCES prompt_versions (tenant_id, name, version)
);
