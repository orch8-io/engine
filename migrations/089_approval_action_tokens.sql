-- Out-of-band approval actions for human_review gates (Slack buttons, Teams
-- card actions, email magic links). Only the SHA-256 of each token is stored;
-- consuming one token burns every sibling token of the same gate.
CREATE TABLE IF NOT EXISTS approval_action_tokens (
    token_hash TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    instance_id UUID NOT NULL,
    block_id TEXT NOT NULL,
    choice TEXT NOT NULL,
    channel TEXT NOT NULL,
    recipient TEXT,
    verify_secret_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_approval_action_tokens_gate
    ON approval_action_tokens (instance_id, block_id);
CREATE INDEX IF NOT EXISTS idx_approval_action_tokens_expiry
    ON approval_action_tokens (expires_at);
