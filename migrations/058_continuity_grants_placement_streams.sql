CREATE TABLE IF NOT EXISTS continuation_grants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    state TEXT NOT NULL,
    nonce_sha256 TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    record JSONB NOT NULL,
    UNIQUE (tenant_id, nonce_sha256),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_continuation_grants_active
    ON continuation_grants (tenant_id, continuity_id, expires_at)
    WHERE state = 'active';

CREATE TABLE IF NOT EXISTS placement_decisions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    selected_runtime_id UUID,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_placement_decisions_execution
    ON placement_decisions (tenant_id, continuity_id, epoch, created_at DESC);

CREATE TABLE IF NOT EXISTS continuity_streams (
    stream_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    next_sequence BIGINT NOT NULL DEFAULT 0 CHECK (next_sequence >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, stream_id),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_continuity_streams_execution
    ON continuity_streams (tenant_id, continuity_id, epoch);

CREATE TABLE IF NOT EXISTS continuity_stream_frames (
    stream_id UUID NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 0),
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    state TEXT NOT NULL,
    checkpoint_sha256 TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stream_id, sequence),
    FOREIGN KEY (tenant_id, stream_id)
        REFERENCES continuity_streams(tenant_id, stream_id),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_continuity_stream_resume
    ON continuity_stream_frames (tenant_id, stream_id, sequence)
    WHERE state IN ('committed', 'retracted');
CREATE INDEX IF NOT EXISTS idx_continuity_stream_expiry
    ON continuity_stream_frames (expires_at);
