-- Per-parent dedupe table for the emit_event builtin handler.
-- Rows are GC'd by the externalized-state TTL sweeper (default 30d).
CREATE TABLE IF NOT EXISTS emit_event_dedupe (
    parent_instance_id  UUID         NOT NULL,
    dedupe_key          TEXT         NOT NULL,
    child_instance_id   UUID         NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_instance_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS emit_event_dedupe_created_at_idx
    ON emit_event_dedupe (created_at);
