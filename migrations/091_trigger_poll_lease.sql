-- Per-trigger poll lease so only one engine node polls an
-- `activepieces_poll` trigger at a time (every node runs the trigger loop).
-- `lease_owner` identifies the holding node; `lease_until` expires the lease
-- so a crashed holder is replaced.
ALTER TABLE trigger_poll_state ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE trigger_poll_state ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
