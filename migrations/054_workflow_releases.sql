-- Safe workflow releases (release control plane).
--
-- A release pins exact baseline/candidate sequence ids + versions (never
-- inferred later from mutable names) and moves through a conditional,
-- audited state machine: draft → validating → ready → canary →
-- promoted | paused | rolled_back | failed. State changes use
-- compare-and-swap on `state` so concurrent promote/rollback cannot both
-- win. Decisions are the immutable audit trail.
CREATE TABLE IF NOT EXISTS workflow_releases (
    id                    UUID PRIMARY KEY,
    tenant_id             TEXT NOT NULL,
    namespace             TEXT NOT NULL,
    sequence_name         TEXT NOT NULL,
    baseline_sequence_id  UUID NOT NULL,
    baseline_version      INTEGER NOT NULL,
    candidate_sequence_id UUID NOT NULL,
    candidate_version     INTEGER NOT NULL,
    state                 TEXT NOT NULL,
    canary_percent        SMALLINT NOT NULL DEFAULT 0,
    gates                 JSONB NOT NULL DEFAULT '[]',
    in_flight_policy      TEXT NOT NULL DEFAULT 'pin',
    validation_summary    JSONB,
    canary_started_at     TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_releases_tenant
    ON workflow_releases (tenant_id, state);
-- Routing lookup: at most one traffic-routing release per baseline.
CREATE INDEX IF NOT EXISTS idx_releases_baseline
    ON workflow_releases (baseline_sequence_id, state);

CREATE TABLE IF NOT EXISTS release_decisions (
    id         UUID PRIMARY KEY,
    release_id UUID NOT NULL,
    from_state TEXT NOT NULL,
    to_state   TEXT NOT NULL,
    actor      TEXT NOT NULL,
    reason     TEXT NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_decisions_release
    ON release_decisions (release_id, decided_at);
