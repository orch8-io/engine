CREATE TABLE worker_route_observations (
    tenant_id TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    version TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL,
    available_poll_slots INTEGER,
    transport TEXT NOT NULL,
    all_queues BOOLEAN NOT NULL,
    runtime_id UUID,
    shared_runtime JSONB,
    session_id TEXT NOT NULL,
    draining BOOLEAN NOT NULL,
    PRIMARY KEY(tenant_id, handler_name, worker_id, queue_name, all_queues, transport, session_id)
);
CREATE INDEX idx_worker_route_observations_route
    ON worker_route_observations(tenant_id, handler_name, all_queues, queue_name, last_seen_at DESC);
CREATE INDEX idx_worker_route_observations_session
    ON worker_route_observations(session_id) WHERE session_id<>'';
CREATE INDEX idx_worker_route_observations_last_seen
    ON worker_route_observations(last_seen_at);
CREATE INDEX idx_worker_registrations_route
    ON worker_registrations(COALESCE(tenant_id, ''), handler_name, COALESCE(queue_name, ''), last_seen_at DESC);
