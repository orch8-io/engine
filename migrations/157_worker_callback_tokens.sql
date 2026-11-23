ALTER TABLE worker_tasks ADD COLUMN wait_for_callback BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE worker_callback_tokens (
    task_id UUID NOT NULL REFERENCES worker_tasks(id) ON DELETE CASCADE,
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
    tenant_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    token TEXT NOT NULL UNIQUE CHECK (length(token) = 64),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (task_id, claim_epoch)
);

CREATE FUNCTION preserve_worker_callback_mode() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.wait_for_callback IS DISTINCT FROM OLD.wait_for_callback
        AND EXISTS (SELECT 1 FROM worker_task_admissions WHERE task_id = OLD.id)
    THEN
        RAISE EXCEPTION 'admitted callback mode is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER worker_callback_mode_update
    BEFORE UPDATE OF wait_for_callback ON worker_tasks
    FOR EACH ROW EXECUTE FUNCTION preserve_worker_callback_mode();
