-- Durable, cluster-wide replay claims for inbound webhooks.
CREATE TABLE IF NOT EXISTS webhook_replay_nonces (
    trigger_slug TEXT NOT NULL REFERENCES triggers(slug) ON DELETE CASCADE,
    nonce        TEXT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trigger_slug, nonce)
);

CREATE INDEX IF NOT EXISTS idx_webhook_replay_nonces_expiry
    ON webhook_replay_nonces (expires_at);
