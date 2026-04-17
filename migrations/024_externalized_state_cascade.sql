-- Enforce ON DELETE CASCADE for externalized_state -> task_instances.
--
-- Pre-M4 the FK allowed an instance row to be deleted while its payloads
-- remained, orphaning externalized_state rows. The GC sweeper (M4) handles
-- TTL-based deletion, but instance-level deletion should be atomic with its
-- externalized payload cleanup. Dropping and re-adding the constraint is the
-- only idempotent way to add cascading behavior to an existing FK.
ALTER TABLE externalized_state
    DROP CONSTRAINT IF EXISTS externalized_state_instance_id_fkey;

ALTER TABLE externalized_state
    ADD CONSTRAINT externalized_state_instance_id_fkey
    FOREIGN KEY (instance_id) REFERENCES task_instances(id) ON DELETE CASCADE;
