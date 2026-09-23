-- Roll back lease-based cron claims.
ALTER TABLE cron_schedules DROP COLUMN IF EXISTS claimed_until;
