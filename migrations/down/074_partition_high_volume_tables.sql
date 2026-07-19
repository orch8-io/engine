ALTER TABLE block_outputs RENAME TO block_outputs_partitioned_074;
ALTER TABLE block_outputs_partitioned_074
    DROP CONSTRAINT IF EXISTS block_outputs_pkey;

CREATE TABLE block_outputs (
    id          UUID PRIMARY KEY,
    instance_id UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    block_id    TEXT        NOT NULL,
    output      JSONB,
    output_ref  TEXT,
    output_size INTEGER     NOT NULL DEFAULT 0,
    attempt     SMALLINT    NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO block_outputs
SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
FROM block_outputs_partitioned_074;
DROP TABLE block_outputs_partitioned_074;
CREATE INDEX idx_block_outputs_instance_block ON block_outputs (instance_id, block_id);
CREATE INDEX idx_block_outputs_instance_block_created
    ON block_outputs (instance_id, block_id, created_at DESC);

ALTER TABLE audit_log RENAME TO audit_log_partitioned_074;
ALTER TABLE audit_log_partitioned_074
    DROP CONSTRAINT IF EXISTS audit_log_pkey;

CREATE TABLE audit_log (
    id          UUID PRIMARY KEY,
    instance_id UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    tenant_id   TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    from_state  TEXT,
    to_state    TEXT,
    block_id    TEXT,
    details     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO audit_log
SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
FROM audit_log_partitioned_074;
DROP TABLE audit_log_partitioned_074;
CREATE INDEX idx_audit_log_instance ON audit_log (instance_id, created_at);
CREATE INDEX idx_audit_log_tenant ON audit_log (tenant_id, created_at);
