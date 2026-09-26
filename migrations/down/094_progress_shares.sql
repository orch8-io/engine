-- Undo 094: drop public progress shares.
DROP INDEX IF EXISTS idx_progress_shares_instance;
DROP TABLE IF EXISTS progress_shares;
