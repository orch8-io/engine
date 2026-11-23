CREATE TABLE readonly_instance_history (
    instance_id UUID PRIMARY KEY REFERENCES task_instances(id) ON DELETE CASCADE,
    migration_id UUID NOT NULL
);

CREATE FUNCTION reject_history_state_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state
        AND EXISTS (SELECT 1 FROM readonly_instance_history WHERE instance_id = OLD.id) THEN
        RAISE EXCEPTION 'archived instance history cannot change state' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER readonly_instance_history_state
BEFORE UPDATE OF state ON task_instances
FOR EACH ROW EXECUTE FUNCTION reject_history_state_change();
