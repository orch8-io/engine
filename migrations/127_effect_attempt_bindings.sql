-- One durable provider identity per active logical invocation. Receipts reset
-- by a loop retire their binding; the next invocation allocates a fresh UUID.
-- Coordinate runtime upgrades: old binaries do not participate in this protocol.
CREATE TABLE IF NOT EXISTS effect_attempt_bindings (
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    instance_id UUID NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    block_id TEXT NOT NULL,
    attempt BIGINT NOT NULL CHECK (attempt >= 0),
    effect_id UUID NOT NULL REFERENCES effect_receipts(id) ON DELETE CASCADE,
    PRIMARY KEY (tenant_id, continuity_id, epoch, instance_id, block_id, attempt),
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);
CREATE INDEX IF NOT EXISTS idx_effect_attempt_bindings_receipt
    ON effect_attempt_bindings (effect_id);

CREATE INDEX IF NOT EXISTS idx_effect_attempt_bindings_instance ON effect_attempt_bindings(instance_id);
CREATE TABLE IF NOT EXISTS effect_block_resets (
    tenant_id TEXT NOT NULL,
    instance_id UUID NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    block_id TEXT NOT NULL,
    PRIMARY KEY (instance_id, tenant_id, block_id)
);
