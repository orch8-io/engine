-- Webhook outbox: parked outbound webhook deliveries that exhausted their
-- retries. Instead of dropping a failed delivery, the engine parks it here so
-- an operator can inspect it (GET /webhooks/outbox) and redeliver it. One row
-- per failed (url, event) attempt.
--
-- payload    — the full serialized WebhookEvent, replayed verbatim on redelivery.
-- attempts   — how many delivery attempts were made before parking.
-- last_error — the last HTTP status / transport error seen before parking.
CREATE TABLE IF NOT EXISTS webhook_outbox (
    id          UUID PRIMARY KEY,
    url         TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    instance_id UUID,
    payload     JSONB NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_created
    ON webhook_outbox (created_at DESC);
