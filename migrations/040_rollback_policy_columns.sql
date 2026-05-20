-- Add missing columns to rollback_policies that the storage layer expects.
ALTER TABLE rollback_policies ADD COLUMN IF NOT EXISTS cooldown_secs INTEGER NOT NULL DEFAULT 3600;
ALTER TABLE rollback_policies ADD COLUMN IF NOT EXISTS confirmation_window_secs INTEGER NOT NULL DEFAULT 60;
ALTER TABLE rollback_policies ADD COLUMN IF NOT EXISTS webhook_url TEXT;
