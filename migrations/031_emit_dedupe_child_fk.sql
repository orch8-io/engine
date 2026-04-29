-- Fix #4: emit_event_dedupe.child_instance_id is missing a foreign key
-- to task_instances(id). Orphaned dedupe rows survive child deletion,
-- permanently blocking re-emission of that dedupe key.
--
-- Add FK with ON DELETE CASCADE so deleting a child instance also removes
-- the dedupe guard, allowing re-emission.

ALTER TABLE emit_event_dedupe
    ADD CONSTRAINT fk_emit_dedupe_child_instance
    FOREIGN KEY (child_instance_id) REFERENCES task_instances(id)
    ON DELETE CASCADE
    NOT VALID;

-- Validate in a separate statement to avoid holding ACCESS EXCLUSIVE for
-- the full table scan on large tables.
ALTER TABLE emit_event_dedupe
    VALIDATE CONSTRAINT fk_emit_dedupe_child_instance;
