-- Reverses 072_resumable_worker_activities
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS checkpoint_seq;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS resume_checkpoint;
