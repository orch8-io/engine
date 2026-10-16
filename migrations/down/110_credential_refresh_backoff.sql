-- Roll back the OAuth2 credential refresh back-off counter.
ALTER TABLE credentials DROP COLUMN IF EXISTS refresh_failures;
