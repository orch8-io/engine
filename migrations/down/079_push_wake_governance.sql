DROP INDEX IF EXISTS idx_push_wake_collapse_pending;
ALTER TABLE push_wake_outbox DROP COLUMN IF EXISTS superseded_by;
ALTER TABLE push_wake_outbox DROP COLUMN IF EXISTS collapse_key;
ALTER TABLE push_wake_outbox DROP COLUMN IF EXISTS topic;
ALTER TABLE push_wake_outbox DROP COLUMN IF EXISTS execution_id;
