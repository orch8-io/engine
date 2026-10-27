-- Freeze a consistent source snapshot before transport publication. Retain a
-- stable identity and encrypted payload for retries after an ambiguous upload.
CREATE TABLE IF NOT EXISTS capsule_source_cuts (
 capsule_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 continuity_id UUID NOT NULL,
 epoch BIGINT NOT NULL,
 source_instance_id UUID NOT NULL,
 source_runtime_id UUID NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('captured','sealed','aborted','acknowledged','retired')),
 request JSONB NOT NULL,
 snapshot JSONB,
 manifest JSONB,
 sealed_payload BYTEA,
 acknowledgement JSONB,
 created_at TIMESTAMPTZ NOT NULL,
 FOREIGN KEY(tenant_id,continuity_id) REFERENCES continuity_executions(tenant_id,continuity_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_capsule_source_cut_active
 ON capsule_source_cuts(tenant_id,continuity_id,epoch) WHERE state IN ('captured','sealed');
CREATE INDEX IF NOT EXISTS idx_capsule_source_cut_instance
 ON capsule_source_cuts(source_instance_id) WHERE state IN ('captured','sealed');

-- No parent FK: a late upload must remain collectible after source-history purge.
CREATE TABLE IF NOT EXISTS capsule_artifact_retirements (
 artifact_key TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 capsule_id UUID NOT NULL,
 next_sweep_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_capsule_artifact_retirement_due
 ON capsule_artifact_retirements(next_sweep_at,artifact_key);
