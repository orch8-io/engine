-- Plugin registry: maps handler names to external implementations (WASM, gRPC).
CREATE TABLE IF NOT EXISTS plugins (
    name        TEXT PRIMARY KEY,
    plugin_type TEXT NOT NULL DEFAULT 'wasm',
    source      TEXT NOT NULL,
    tenant_id   TEXT NOT NULL DEFAULT '',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    config      TEXT NOT NULL DEFAULT '{}',
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plugins_tenant ON plugins (tenant_id);
