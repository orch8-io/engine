-- Roll back the per-trigger poll lease.
ALTER TABLE trigger_poll_state DROP COLUMN IF EXISTS lease_until;
ALTER TABLE trigger_poll_state DROP COLUMN IF EXISTS lease_owner;
