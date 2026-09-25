-- Worker commands are addressed by a client-chosen worker_id; record the
-- owning tenant so tenant-scoped worker streams only see (and ack) their own
-- tenant's commands. Existing rows keep '' = deliverable to unscoped
-- (root/admin) sessions only.
ALTER TABLE worker_commands
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '';
