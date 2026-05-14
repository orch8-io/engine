-- Mobile telemetry ingestion tables.

CREATE TABLE IF NOT EXISTS telemetry_mobile_events (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    device_id TEXT,
    os_name TEXT,
    os_version TEXT,
    app_version TEXT,
    sdk_version TEXT,
    tenant_id TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_telemetry_events_type ON telemetry_mobile_events(event_type);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_device ON telemetry_mobile_events(device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_tenant ON telemetry_mobile_events(tenant_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_received ON telemetry_mobile_events(received_at);

CREATE TABLE IF NOT EXISTS telemetry_mobile_errors (
    id BIGSERIAL PRIMARY KEY,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    stack_trace TEXT,
    device_id TEXT,
    os_name TEXT,
    os_version TEXT,
    app_version TEXT,
    sdk_version TEXT,
    tenant_id TEXT,
    instance_id TEXT,
    sequence_name TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_telemetry_errors_device ON telemetry_mobile_errors(device_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_errors_tenant ON telemetry_mobile_errors(tenant_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_errors_received ON telemetry_mobile_errors(received_at);
