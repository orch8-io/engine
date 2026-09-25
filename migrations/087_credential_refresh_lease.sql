-- OAuth2 refresh lease. Every node runs the credential refresh loop; without
-- a claim, two nodes refreshed the same credential with the same refresh
-- token, and with rotating refresh tokens the loser's write (or the
-- provider's reuse detection) left a dead credential. The refresh loop now
-- claims the row by setting `refresh_claimed_until` (conditional update)
-- before calling the token endpoint. Token writes use a CAS on `updated_at`.
ALTER TABLE credentials ADD COLUMN IF NOT EXISTS refresh_claimed_until TIMESTAMPTZ;
