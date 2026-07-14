-- Immutable owner-per-epoch evidence for global execution identity.
CREATE TABLE IF NOT EXISTS continuity_locations (
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL REFERENCES continuity_executions(continuity_id),
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    runtime_id UUID NOT NULL,
    instance_id UUID NOT NULL,
    handoff_id UUID,
    record JSONB NOT NULL,
    entered_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, continuity_id, epoch),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);

CREATE INDEX IF NOT EXISTS idx_continuity_locations_runtime
    ON continuity_locations (tenant_id, runtime_id, entered_at DESC);

-- Existing installations can prove their current location. New ownership
-- transfers append every epoch transactionally from this point forward.
INSERT INTO continuity_locations
    (tenant_id, continuity_id, epoch, runtime_id, instance_id, handoff_id, record, entered_at)
SELECT tenant_id,
       continuity_id,
       epoch,
       owner_runtime_id,
       (record->>'current_instance_id')::UUID,
       NULL,
       jsonb_build_object(
           'continuity_id', continuity_id,
           'tenant_id', tenant_id,
           'epoch', epoch,
           'runtime_id', owner_runtime_id,
           'instance_id', record->>'current_instance_id',
           'handoff_id', NULL,
           'entered_at', updated_at
       ),
       updated_at
FROM continuity_executions
ON CONFLICT (tenant_id, continuity_id, epoch) DO NOTHING;
