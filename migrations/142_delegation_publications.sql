-- A retained mailbox publication is independent of hot task/native history.
CREATE TABLE delegation_publications (
 delegation_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 grant_id UUID NOT NULL UNIQUE,
 task_id UUID NOT NULL UNIQUE,
 instance_id UUID NOT NULL,
 record JSONB NOT NULL CHECK (jsonb_typeof(record->'version') IS NOT DISTINCT FROM 'number' AND record->>'version' IS NOT DISTINCT FROM '1'),
 CHECK(record #>> '{delegation,id}' IS NOT DISTINCT FROM delegation_id::text),
 CHECK(record #>> '{delegation,tenant_id}' IS NOT DISTINCT FROM tenant_id),
 CHECK(record #>> '{delegation,grant_id}' IS NOT DISTINCT FROM grant_id::text),
 CHECK(record->>'task_id' IS NOT DISTINCT FROM task_id::text),
 CHECK(record->>'instance_id' IS NOT DISTINCT FROM instance_id::text),
 created_at TIMESTAMPTZ NOT NULL
);
CREATE FUNCTION preserve_delegation_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'delegation publication receipts are immutable'; END $$;
CREATE TRIGGER delegation_publication_update BEFORE UPDATE ON delegation_publications
 FOR EACH ROW EXECUTE FUNCTION preserve_delegation_publication();
CREATE TRIGGER delegation_publication_delete BEFORE DELETE ON delegation_publications
 FOR EACH ROW EXECUTE FUNCTION preserve_delegation_publication();
