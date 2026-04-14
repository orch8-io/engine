-- R7: generalize emit_event_dedupe to support per-parent OR per-tenant scope.
--
-- Before: (parent_instance_id, dedupe_key) was the only dedupe identity,
-- forcing callers who wanted tenant-wide at-most-once fan-out to maintain
-- external state.
--
-- After: a `scope_kind` discriminator ('parent' | 'tenant') plus a
-- `scope_value` column (parent uuid as text, or tenant id as text) lets the
-- handler pick the namespace at call time. Both scopes coexist in the same
-- table so the existing TTL sweeper handles both with no schema fan-out.
--
-- Safe to DROP the old table: emit_event dedupe is a brand-new feature from
-- migration 025 and carries no production data in supported deployments.
DROP TABLE IF EXISTS emit_event_dedupe;

CREATE TABLE emit_event_dedupe (
    scope_kind          TEXT         NOT NULL,   -- 'parent' | 'tenant'
    scope_value         TEXT         NOT NULL,   -- parent uuid text OR tenant id
    dedupe_key          TEXT         NOT NULL,
    child_instance_id   UUID         NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (scope_kind, scope_value, dedupe_key)
);

CREATE INDEX IF NOT EXISTS emit_event_dedupe_created_at_idx
    ON emit_event_dedupe (created_at);
