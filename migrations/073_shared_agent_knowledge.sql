-- Tenant-isolated long-term agent knowledge shared across workflow instances.
CREATE TABLE IF NOT EXISTS shared_agent_knowledge (
    tenant_id   TEXT        NOT NULL,
    namespace   TEXT        NOT NULL,
    key         TEXT        NOT NULL,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, namespace, key)
);

CREATE INDEX IF NOT EXISTS idx_shared_agent_knowledge_updated
    ON shared_agent_knowledge (tenant_id, namespace, updated_at DESC);
