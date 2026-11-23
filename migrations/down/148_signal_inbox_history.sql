-- Rollback of 148: Remove the archive pagination index for the signal inbox.
DROP INDEX IF EXISTS idx_signal_inbox_history;
