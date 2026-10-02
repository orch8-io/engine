-- Placement policies and global rate budgets (docs/PLACEMENT.md).
--
-- placement_policies  one document per tenant: the ordered list served by
--                     GET|PUT /placement/policies. Matched at dispatch and
--                     compiled into the step's capability requirements.
-- rate_budgets        durable token bucket per (tenant, key), shared by every
--                     engine node. Steps with `rate_budget` take one token
--                     before dispatch and are deferred (never failed) while
--                     the bucket is empty.
CREATE TABLE IF NOT EXISTS placement_policies (
    tenant_id   TEXT PRIMARY KEY,
    policies    JSONB NOT NULL DEFAULT '{"items": []}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rate_budgets (
    tenant_id       TEXT NOT NULL,
    budget_key      TEXT NOT NULL,
    capacity        INTEGER NOT NULL CHECK (capacity > 0),
    refill_per_sec  DOUBLE PRECISION NOT NULL CHECK (refill_per_sec > 0),
    tokens          DOUBLE PRECISION NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, budget_key)
);
