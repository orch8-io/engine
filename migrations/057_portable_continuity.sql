CREATE TABLE IF NOT EXISTS continuity_executions (
    continuity_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    owner_runtime_id UUID NOT NULL,
    state TEXT NOT NULL,
    record JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_continuity_executions_tenant
    ON continuity_executions (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS execution_handoffs (
    id UUID PRIMARY KEY,
    continuity_id UUID NOT NULL REFERENCES continuity_executions(continuity_id),
    tenant_id TEXT NOT NULL,
    state TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_handoffs_continuity
    ON execution_handoffs (tenant_id, continuity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS execution_capsules (
    id UUID PRIMARY KEY,
    continuity_id UUID NOT NULL REFERENCES continuity_executions(continuity_id),
    tenant_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    manifest JSONB NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_capsules_expiry
    ON execution_capsules (tenant_id, expires_at);

CREATE TABLE IF NOT EXISTS runtime_capabilities (
    tenant_id TEXT NOT NULL,
    runtime_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    record JSONB NOT NULL,
    PRIMARY KEY (tenant_id, runtime_id)
);
CREATE INDEX IF NOT EXISTS idx_runtime_capabilities_live
    ON runtime_capabilities (tenant_id, expires_at DESC);

CREATE TABLE IF NOT EXISTS effect_receipts (
    id UUID PRIMARY KEY,
    continuity_id UUID NOT NULL REFERENCES continuity_executions(continuity_id),
    tenant_id TEXT NOT NULL,
    instance_id UUID NOT NULL,
    state TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_effect_receipts_continuity
    ON effect_receipts (tenant_id, continuity_id, created_at, id);

CREATE TABLE IF NOT EXISTS provenance_entries (
    id UUID PRIMARY KEY,
    continuity_id UUID NOT NULL REFERENCES continuity_executions(continuity_id),
    tenant_id TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    entry_sha256 TEXT NOT NULL,
    previous_sha256 TEXT,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, continuity_id, entry_sha256),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_provenance_entries_chain
    ON provenance_entries (tenant_id, continuity_id, epoch, created_at, id);
