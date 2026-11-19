ALTER TABLE worker_version_pins ADD COLUMN active INTEGER NOT NULL DEFAULT 1;
ALTER TABLE worker_version_pins ADD COLUMN staged_min_version TEXT;
ALTER TABLE worker_version_pins ADD COLUMN previous_min_version TEXT;
ALTER TABLE worker_version_pins ADD COLUMN rollout_revision TEXT;
ALTER TABLE worker_version_pins ADD COLUMN can_rollback INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_registrations ADD COLUMN available_poll_slots INTEGER;
