ALTER TABLE worker_tasks
    ADD COLUMN resume_checkpoint JSONB;

ALTER TABLE worker_tasks
    ADD COLUMN checkpoint_seq BIGINT NOT NULL DEFAULT 0 CHECK (checkpoint_seq >= 0);
