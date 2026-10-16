-- OAuth2 refresh back-off. A credential whose refresh keeps failing (revoked
-- app, `invalid_grant`, dead token endpoint) used to be retried every pass,
-- sorted first, ahead of healthy credentials. The refresh loop now counts
-- consecutive failures here and holds `refresh_claimed_until` for an
-- exponential back-off; the due listing skips held rows, orders by this
-- count and is bounded. A successful refresh resets it to 0.
ALTER TABLE credentials ADD COLUMN IF NOT EXISTS refresh_failures INTEGER NOT NULL DEFAULT 0;
