ALTER TABLE worker_tasks
    ADD COLUMN IF NOT EXISTS requirements JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN worker_tasks.requirements IS
    'Durable capability, locality, connectivity, UI, and trust requirements for atomic worker matching';
