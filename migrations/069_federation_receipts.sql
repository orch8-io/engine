-- Durable, race-safe replay protection for explicitly configured federation peers.
CREATE TABLE IF NOT EXISTS federation_receipts (
    tenant_id TEXT NOT NULL,
    peer_id UUID NOT NULL,
    message_id UUID NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    envelope_sha256 TEXT NOT NULL CHECK (length(envelope_sha256) = 64),
    accepted_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    record JSONB NOT NULL,
    PRIMARY KEY (tenant_id, peer_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_federation_receipts_continuity
    ON federation_receipts (tenant_id, continuity_id, epoch, accepted_at);
CREATE INDEX IF NOT EXISTS idx_federation_receipts_expiry
    ON federation_receipts (expires_at);
