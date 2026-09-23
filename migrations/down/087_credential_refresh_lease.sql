-- Roll back the OAuth2 credential refresh lease.
ALTER TABLE credentials DROP COLUMN IF EXISTS refresh_claimed_until;
