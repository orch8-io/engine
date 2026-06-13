-- Worker version pins: a minimum worker version per (tenant, handler). At poll
-- time, a worker reporting a version below the pin is given no tasks for that
-- handler — letting an operator roll a fixed worker build out before older
-- workers can pick up affected work.
CREATE TABLE IF NOT EXISTS worker_version_pins (
    tenant_id    TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    min_version  TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, handler_name)
);
