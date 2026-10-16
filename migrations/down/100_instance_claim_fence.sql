-- Revert the instance lease claim token.
ALTER TABLE task_instances DROP COLUMN IF EXISTS claimed_at;
