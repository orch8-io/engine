-- Proactively partition the two append-heavy PostgreSQL tables while copying
-- them is still cheap. Fixed hash partitions avoid future calendar-partition
-- maintenance and keep the dominant lookup keys partition-prunable.

ALTER TABLE block_outputs RENAME TO block_outputs_unpartitioned_074;
ALTER TABLE block_outputs_unpartitioned_074
    DROP CONSTRAINT IF EXISTS block_outputs_pkey;

CREATE TABLE block_outputs (
    id          UUID        NOT NULL,
    instance_id UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    block_id    TEXT        NOT NULL,
    output      JSONB,
    output_ref  TEXT,
    output_size INTEGER     NOT NULL DEFAULT 0,
    attempt     SMALLINT    NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instance_id, id)
) PARTITION BY HASH (instance_id);

DO $partition$
BEGIN
    FOR partition_number IN 0..15 LOOP
        EXECUTE format(
            'CREATE TABLE block_outputs_p%1$s PARTITION OF block_outputs
             FOR VALUES WITH (MODULUS 16, REMAINDER %1$s)',
            partition_number
        );
    END LOOP;
END
$partition$;

INSERT INTO block_outputs
    (id, instance_id, block_id, output, output_ref, output_size, attempt, created_at)
SELECT id, instance_id, block_id, output, output_ref, output_size, attempt, created_at
FROM block_outputs_unpartitioned_074;
-- allow-destructive: rows were just copied into the new partitioned
-- block_outputs above; this drops only the renamed pre-partition original.
DROP TABLE block_outputs_unpartitioned_074;

CREATE INDEX idx_block_outputs_id ON block_outputs (id);
CREATE INDEX idx_block_outputs_instance_block ON block_outputs (instance_id, block_id);
CREATE INDEX idx_block_outputs_instance_block_created
    ON block_outputs (instance_id, block_id, created_at DESC);
CREATE INDEX idx_block_outputs_instance_created
    ON block_outputs (instance_id, created_at);

ALTER TABLE audit_log RENAME TO audit_log_unpartitioned_074;
ALTER TABLE audit_log_unpartitioned_074
    DROP CONSTRAINT IF EXISTS audit_log_pkey;

CREATE TABLE audit_log (
    id          UUID        NOT NULL,
    instance_id UUID        NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    tenant_id   TEXT        NOT NULL,
    event_type  TEXT        NOT NULL,
    from_state  TEXT,
    to_state    TEXT,
    block_id    TEXT,
    details     JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, id)
) PARTITION BY HASH (tenant_id);

DO $partition$
BEGIN
    FOR partition_number IN 0..15 LOOP
        EXECUTE format(
            'CREATE TABLE audit_log_p%1$s PARTITION OF audit_log
             FOR VALUES WITH (MODULUS 16, REMAINDER %1$s)',
            partition_number
        );
    END LOOP;
END
$partition$;

INSERT INTO audit_log
    (id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at)
SELECT id, instance_id, tenant_id, event_type, from_state, to_state, block_id, details, created_at
FROM audit_log_unpartitioned_074;
-- allow-destructive: rows were just copied into the new partitioned
-- audit_log above; this drops only the renamed pre-partition original.
DROP TABLE audit_log_unpartitioned_074;

CREATE INDEX idx_audit_log_id ON audit_log (id);
CREATE INDEX idx_audit_log_instance ON audit_log (instance_id, created_at DESC);
CREATE INDEX idx_audit_log_tenant ON audit_log (tenant_id, created_at DESC);
