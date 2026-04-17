-- Enforce ON DELETE CASCADE for externalized_state -> task_instances.
--
-- Pre-M4 the FK allowed an instance row to be deleted while its payloads
-- remained, orphaning externalized_state rows. The GC sweeper (M4) handles
-- TTL-based deletion; instance-level deletion should be atomic with its
-- externalized payload cleanup, which is what cascade gives us.
--
-- Lock discipline:
--  - DROP CONSTRAINT acquires ACCESS EXCLUSIVE briefly; unavoidable but cheap
--    because the constraint has no index attached to it.
--  - ADD CONSTRAINT ... NOT VALID acquires ACCESS EXCLUSIVE briefly too, but
--    skips the full-table scan that a plain ADD CONSTRAINT would run while
--    holding the lock. Rows inserted after this statement are checked; the
--    existing backlog is not yet validated.
--  - VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE (concurrent reads/writes
--    allowed, only other DDL is blocked) and then scans the table. On a large
--    externalized_state this is the slow step, but it no longer blocks
--    foreground traffic.
ALTER TABLE externalized_state
    DROP CONSTRAINT IF EXISTS externalized_state_instance_id_fkey;

ALTER TABLE externalized_state
    ADD CONSTRAINT externalized_state_instance_id_fkey
    FOREIGN KEY (instance_id) REFERENCES task_instances(id) ON DELETE CASCADE
    NOT VALID;

ALTER TABLE externalized_state
    VALIDATE CONSTRAINT externalized_state_instance_id_fkey;
