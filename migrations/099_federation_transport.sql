-- Explicit, opt-in federation transport and active-passive region fencing.
--
-- federation_peers  per-tenant trust registry (peer identity, endpoint,
--                   public key, mutual sequence allowlists, disclosure policy,
--                   expiry/revocation). `record` holds the full JSON entry.
-- federation_calls  durable outbound calls driven by the federation poller.
--                   `record.input`/`record.result` are sealed by the
--                   encrypting storage layer when encryption at rest is on.
-- region_fence      singleton active-region fence; `epoch` only ever
--                   increases by one per promotion (CAS in the application).
CREATE TABLE IF NOT EXISTS federation_peers (
    tenant_id TEXT NOT NULL,
    peer_id UUID NOT NULL,
    name TEXT NOT NULL,
    record JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, peer_id),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS federation_calls (
    tenant_id TEXT NOT NULL,
    call_id UUID NOT NULL,
    instance_id UUID NOT NULL,
    state TEXT NOT NULL,
    notified BOOLEAN NOT NULL DEFAULT FALSE,
    next_poll_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, call_id)
);
CREATE INDEX IF NOT EXISTS idx_federation_calls_due
    ON federation_calls (next_poll_at) WHERE notified = FALSE;
CREATE INDEX IF NOT EXISTS idx_federation_calls_instance
    ON federation_calls (tenant_id, instance_id);

CREATE TABLE IF NOT EXISTS region_fence (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    active_region TEXT NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 1),
    record JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
