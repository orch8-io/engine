-- Undo 096: drop the retry dispatch-binding flag.
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS awaiting_dispatch;
