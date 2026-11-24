-- Rollback of 151: Remove the read-only dispatch fence only if no archived instances remain.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM readonly_instance_history) THEN
        RAISE EXCEPTION 'cannot remove the dispatch fence for archived instance history';
    END IF;
END $$;
DROP TRIGGER readonly_instance_history_state ON task_instances;
DROP FUNCTION reject_history_state_change();
DROP TABLE readonly_instance_history;
