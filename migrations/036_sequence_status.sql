-- Add sequence lifecycle status.

ALTER TABLE sequences ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'production';

CREATE INDEX IF NOT EXISTS idx_sequences_status ON sequences(status);
