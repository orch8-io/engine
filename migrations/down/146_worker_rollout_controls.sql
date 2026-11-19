-- Remove rollout controls and worker poll capacity. Discard inactive pins so
-- the older schema cannot interpret them as active version requirements.
DELETE FROM worker_version_pins WHERE NOT active;
ALTER TABLE worker_version_pins
    DROP COLUMN active,
    DROP COLUMN staged_min_version,
    DROP COLUMN previous_min_version,
    DROP COLUMN rollout_revision,
    DROP COLUMN can_rollback;

ALTER TABLE worker_registrations DROP COLUMN available_poll_slots;
