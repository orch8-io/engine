-- Undo 072_resumable_worker_activities: drop the checkpoint columns added
-- for resumable worker-activity execution.
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS checkpoint_seq;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS resume_checkpoint;
