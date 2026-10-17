-- Revert the split-claim partial indexes.
DROP INDEX IF EXISTS idx_push_wake_inflight_lease;
DROP INDEX IF EXISTS idx_push_wake_pending_created;
