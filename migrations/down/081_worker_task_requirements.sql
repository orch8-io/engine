-- Roll back worker task capability requirements.
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS requirements;
