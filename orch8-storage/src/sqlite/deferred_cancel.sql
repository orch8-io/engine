ALTER TABLE signal_inbox ADD COLUMN deferred_cancel INTEGER NOT NULL DEFAULT 0;
CREATE INDEX signal_inbox_deferred_cancel ON signal_inbox(instance_id) WHERE deferred_cancel=1;
