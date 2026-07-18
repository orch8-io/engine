-- Webhook outbox becomes the delivery queue, not just a parking lot: rows
-- are written `pending` in the same transaction as the instance's terminal
-- state change, claimed by the drain loop / in-memory fast path, rescheduled
-- with backoff via `next_attempt_at`, and flipped to `parked` only after
-- retries are exhausted (the pre-existing behavior for parked rows, which is
-- why existing rows default to 'parked').
--
-- status          — pending | in_flight | parked (see drain loop claim).
-- next_attempt_at — when a pending row is due; NULL = immediately due.
-- claimed_at      — bounds an in_flight claim so a dispatcher that died
--                   mid-dispatch is recovered back to pending.
ALTER TABLE webhook_outbox ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'parked';
ALTER TABLE webhook_outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
ALTER TABLE webhook_outbox ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

-- The drain loop's due-row scan. Partial: parked rows (the long tail of this
-- table) stay out of the index.
CREATE INDEX IF NOT EXISTS idx_webhook_outbox_due
    ON webhook_outbox (next_attempt_at) WHERE status = 'pending';
