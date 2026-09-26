-- Opt-in `llm_call` response cache (`cache: {mode, ttl}`). Keyed per tenant by
-- a hash of the normalized request; `response`/`embedding` are encrypted at
-- rest when field encryption is enabled. `partition_key` groups entries that
-- agree on everything but message text, for semantic (embedding) lookups.
CREATE TABLE IF NOT EXISTS llm_response_cache (
    tenant_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    response JSONB NOT NULL,
    embedding JSONB,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, cache_key)
);
CREATE INDEX IF NOT EXISTS idx_llm_response_cache_partition
    ON llm_response_cache (tenant_id, partition_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_response_cache_expiry
    ON llm_response_cache (expires_at);
