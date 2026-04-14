-- Persisted `Open` circuit breaker state. Only tripped (open) breakers are
-- persisted; closed is the default and stored in-memory only. Primary key is
-- (tenant_id, handler) so breakers are per-tenant — a noisy tenant cannot
-- trip the circuit for others sharing the same handler name.
CREATE TABLE IF NOT EXISTS circuit_breakers (
    tenant_id         TEXT        NOT NULL,
    handler           TEXT        NOT NULL,
    state             TEXT        NOT NULL,
    failure_count     INTEGER     NOT NULL,
    failure_threshold INTEGER     NOT NULL,
    cooldown_secs     BIGINT      NOT NULL,
    opened_at         TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, handler)
);

-- Boot-time rehydration scans by state = 'open' — keep that fast as the
-- table grows. Closed/half_open rows shouldn't normally exist but the
-- partial predicate also guards against stray entries.
CREATE INDEX IF NOT EXISTS idx_circuit_breakers_open
    ON circuit_breakers (state)
    WHERE state = 'open';
