CREATE TABLE IF NOT EXISTS delegation_publications (
 delegation_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 grant_id TEXT NOT NULL UNIQUE,
 task_id TEXT NOT NULL UNIQUE,
 instance_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(json_type(record,'$.version') IS 'integer' AND json_extract(record,'$.version') IS 1),
 created_at TEXT NOT NULL,
 CHECK(json_extract(record,'$.delegation.id') IS delegation_id),
 CHECK(json_extract(record,'$.delegation.tenant_id') IS tenant_id),
 CHECK(json_extract(record,'$.delegation.grant_id') IS grant_id),
 CHECK(json_extract(record,'$.task_id') IS task_id),
 CHECK(json_extract(record,'$.instance_id') IS instance_id)
);
CREATE TRIGGER IF NOT EXISTS delegation_publication_update BEFORE UPDATE ON delegation_publications
 BEGIN SELECT RAISE(ABORT,'delegation publication receipts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_publication_delete BEFORE DELETE ON delegation_publications
 BEGIN SELECT RAISE(ABORT,'delegation publication receipts are immutable'); END;
