-- Add deprecated flag for workflow versioning.
-- Running instances continue on their bound version (sequence_id FK).
-- New instances should use the latest non-deprecated version.
ALTER TABLE sequences ADD COLUMN IF NOT EXISTS deprecated BOOLEAN NOT NULL DEFAULT FALSE;
