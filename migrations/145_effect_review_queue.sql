CREATE INDEX IF NOT EXISTS idx_effect_receipts_review
    ON effect_receipts (tenant_id, updated_at, id) WHERE state = 'unknown';
