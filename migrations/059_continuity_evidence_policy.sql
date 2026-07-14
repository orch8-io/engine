CREATE TABLE IF NOT EXISTS workflow_invariants (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    sequence_id UUID NOT NULL,
    sequence_version INTEGER,
    enabled BOOLEAN NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workflow_invariants_scope
    ON workflow_invariants (tenant_id, sequence_id, sequence_version)
    WHERE enabled;

CREATE TABLE IF NOT EXISTS invariant_results (
    id UUID PRIMARY KEY,
    invariant_id UUID NOT NULL REFERENCES workflow_invariants(id),
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    status TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    record JSONB NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, dedupe_key),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_invariant_results_execution
    ON invariant_results (tenant_id, continuity_id, epoch, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS evaluation_scores (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    dedupe_key TEXT NOT NULL,
    deferred BOOLEAN NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, dedupe_key),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_evaluation_scores_execution
    ON evaluation_scores (tenant_id, continuity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS attention_tasks (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    state TEXT NOT NULL,
    assignee TEXT,
    lease_expires_at TIMESTAMPTZ,
    deadline TIMESTAMPTZ NOT NULL,
    record JSONB NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_attention_tasks_pending
    ON attention_tasks (tenant_id, deadline, id)
    WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_attention_tasks_lease
    ON attention_tasks (lease_expires_at)
    WHERE state = 'assigned';

CREATE TABLE IF NOT EXISTS budget_reservations (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    state TEXT NOT NULL,
    cost_microunits BIGINT NOT NULL CHECK (cost_microunits >= 0),
    wall_time_ms BIGINT NOT NULL CHECK (wall_time_ms >= 0),
    external_calls BIGINT NOT NULL CHECK (external_calls >= 0),
    bytes_transferred BIGINT NOT NULL CHECK (bytes_transferred >= 0),
    energy_millijoules BIGINT NOT NULL CHECK (energy_millijoules >= 0),
    attention_units BIGINT NOT NULL CHECK (attention_units >= 0),
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_active
    ON budget_reservations (tenant_id, continuity_id, epoch)
    WHERE state = 'reserved';
