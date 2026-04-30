-- Per-instance key-value state store. Allows workflows to persist arbitrary
-- state across ticks via set_state/get_state/delete_state built-in handlers.
-- Keyed by (instance_id, key) so each instance has its own namespace.
CREATE TABLE IF NOT EXISTS instance_kv_state (
    instance_id   UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    key           TEXT        NOT NULL,
    value         JSONB       NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instance_id, key)
);
