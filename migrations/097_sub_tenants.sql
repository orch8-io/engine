-- Sub-tenants (end customers of a vendor, scoped inside a tenant), per
-- sub-tenant caps, a durable execution ledger for Embedded metering, the
-- tenant embed theme, and the sub-tenant staged-rollout target of releases.
--
-- Rolling-deploy safe: every column is nullable without a default (no table
-- rewrite) and older binaries never read the new tables.
ALTER TABLE task_instances ADD COLUMN IF NOT EXISTS sub_tenant TEXT;

-- Admission counts a sub-tenant's non-terminal instances and list endpoints
-- filter by sub-tenant. Partial: tenant-level rows (the vast majority on
-- non-embedded installs) are not indexed. Not CONCURRENTLY (see 084); on
-- large installs pre-create it online before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_instances_sub_tenant
--       ON task_instances (tenant_id, sub_tenant, state) WHERE sub_tenant IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_instances_sub_tenant
    ON task_instances (tenant_id, sub_tenant, state)
    WHERE sub_tenant IS NOT NULL;

ALTER TABLE workflow_releases ADD COLUMN IF NOT EXISTS target JSONB;

CREATE TABLE IF NOT EXISTS sub_tenant_limits (
    tenant_id TEXT NOT NULL,
    sub_tenant TEXT NOT NULL,
    max_executions_per_month BIGINT CHECK (max_executions_per_month >= 0),
    max_concurrent INTEGER CHECK (max_concurrent >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, sub_tenant)
);

-- Append-only ledger: one row per execution a sub-tenant started. Written in
-- the same transaction as the instance row; deliberately no FK so instance
-- retention/pruning never erases billable history.
CREATE TABLE IF NOT EXISTS sub_tenant_executions (
    instance_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    sub_tenant TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sub_tenant_executions_window
    ON sub_tenant_executions (tenant_id, started_at, sub_tenant);
CREATE INDEX IF NOT EXISTS idx_sub_tenant_executions_sub
    ON sub_tenant_executions (tenant_id, sub_tenant, started_at);
CREATE INDEX IF NOT EXISTS idx_sub_tenant_executions_started
    ON sub_tenant_executions (started_at);

CREATE TABLE IF NOT EXISTS embed_themes (
    tenant_id TEXT PRIMARY KEY,
    record JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
