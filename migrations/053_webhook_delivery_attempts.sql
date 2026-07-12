-- Webhook delivery attempt history (delivery inspector).
--
-- One row per outbound delivery attempt. `delivery_id` groups the attempts
-- of one delivery pass (the initial emit for one URL, or one redelivery).
-- Bounded, redacted metadata only: never the payload or a response body.
-- `error_excerpt` is capped at insert time (512 chars).
CREATE TABLE IF NOT EXISTS webhook_delivery_attempts (
    id             UUID PRIMARY KEY,
    delivery_id    UUID NOT NULL,
    url            TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    instance_id    UUID,
    attempt_number INTEGER NOT NULL,
    attempted_at   TIMESTAMPTZ NOT NULL,
    duration_ms    BIGINT NOT NULL DEFAULT 0,
    success        BOOLEAN NOT NULL DEFAULT FALSE,
    status_code    INTEGER,
    error_class    TEXT,
    error_excerpt  TEXT,
    signed         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_webhook_attempts_delivery
    ON webhook_delivery_attempts (delivery_id, attempt_number);
CREATE INDEX IF NOT EXISTS idx_webhook_attempts_time
    ON webhook_delivery_attempts (attempted_at DESC);

-- Link parked outbox entries to their attempt history.
ALTER TABLE webhook_outbox ADD COLUMN IF NOT EXISTS delivery_id UUID;
