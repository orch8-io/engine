ALTER TABLE worker_version_pins
    ADD COLUMN active BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN staged_min_version TEXT,
    ADD COLUMN previous_min_version TEXT,
    ADD COLUMN rollout_revision UUID,
    ADD COLUMN can_rollback BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE worker_registrations ADD COLUMN available_poll_slots INTEGER;
