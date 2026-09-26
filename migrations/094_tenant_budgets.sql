-- Tenant spend budgets (daily/monthly, optionally per model prefix) over the
-- estimated LLM cost derived from usage_events, plus the once-per-period
-- threshold alert records (`budget.threshold_crossed`).
CREATE TABLE IF NOT EXISTS tenant_budgets (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    model TEXT,
    period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    limit_usd DOUBLE PRECISION NOT NULL CHECK (limit_usd > 0),
    hard_cap BOOLEAN NOT NULL DEFAULT TRUE,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tenant_budgets_tenant ON tenant_budgets (tenant_id);

CREATE TABLE IF NOT EXISTS tenant_budget_alerts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    budget_id UUID NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    threshold_percent SMALLINT NOT NULL CHECK (threshold_percent BETWEEN 1 AND 100),
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (budget_id, period_start, threshold_percent)
);
CREATE INDEX IF NOT EXISTS idx_tenant_budget_alerts_tenant
    ON tenant_budget_alerts (tenant_id, created_at DESC);
