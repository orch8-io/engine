-- Structured usage/billing events (e.g. LLM token consumption emitted by
-- llm_call/agent) so a control plane can aggregate cost without scanning
-- every block output.

CREATE TABLE IF NOT EXISTS usage_events (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    instance_id   UUID,
    block_id      TEXT,
    kind          TEXT NOT NULL,
    model         TEXT NOT NULL DEFAULT '',
    input_tokens  BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_usage_tenant_created ON usage_events(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_kind_model ON usage_events(kind, model);
