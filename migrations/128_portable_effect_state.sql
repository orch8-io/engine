-- Portable receipt recovery proofs and per-execution required commit guards.

CREATE TABLE IF NOT EXISTS capsule_effect_imports (
 tenant_id TEXT NOT NULL,
 capsule_id UUID NOT NULL,
 destination_runtime_id UUID NOT NULL,
 instance_id UUID NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
 continuity_id UUID NOT NULL,
 manifest_sha256 TEXT NOT NULL,
 PRIMARY KEY (tenant_id,capsule_id,destination_runtime_id),
 FOREIGN KEY (tenant_id,continuity_id) REFERENCES continuity_executions(tenant_id,continuity_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_capsule_effect_imports_instance ON capsule_effect_imports(instance_id);
CREATE TABLE IF NOT EXISTS continuity_effect_guards (
 tenant_id TEXT NOT NULL,
 continuity_id UUID NOT NULL,
 invariant_id UUID NOT NULL REFERENCES workflow_invariants(id),
 record JSONB NOT NULL,
 PRIMARY KEY (tenant_id,continuity_id,invariant_id),
 FOREIGN KEY (tenant_id,continuity_id) REFERENCES continuity_executions(tenant_id,continuity_id) ON DELETE CASCADE
);
