ALTER TABLE signal_inbox ADD COLUMN deferred_cancel BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX signal_inbox_deferred_cancel ON signal_inbox(instance_id) WHERE deferred_cancel;
