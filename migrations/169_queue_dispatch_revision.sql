ALTER TABLE queue_dispatch ADD COLUMN revision TEXT NOT NULL DEFAULT '';
ALTER TABLE queue_dispatch ALTER COLUMN revision SET DEFAULT replace(gen_random_uuid()::text, '-', '');
UPDATE queue_dispatch SET revision = replace(gen_random_uuid()::text, '-', '') WHERE revision = '';

CREATE FUNCTION refresh_queue_dispatch_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.revision := replace(gen_random_uuid()::text, '-', '');
    RETURN NEW;
END;
$$;

CREATE TRIGGER queue_dispatch_refresh_revision
    BEFORE UPDATE ON queue_dispatch
    FOR EACH ROW EXECUTE FUNCTION refresh_queue_dispatch_revision();
