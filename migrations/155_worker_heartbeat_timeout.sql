ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS heartbeat_timeout_secs BIGINT
    CHECK (heartbeat_timeout_secs BETWEEN 1 AND 4294967295);

CREATE FUNCTION preserve_worker_heartbeat_timeout() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.heartbeat_timeout_secs IS DISTINCT FROM OLD.heartbeat_timeout_secs
        AND EXISTS (SELECT 1 FROM worker_task_admissions WHERE task_id = OLD.id)
    THEN
        RAISE EXCEPTION 'admitted worker heartbeat timeout is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER worker_heartbeat_timeout_update
    BEFORE UPDATE OF heartbeat_timeout_secs ON worker_tasks
    FOR EACH ROW EXECUTE FUNCTION preserve_worker_heartbeat_timeout();
