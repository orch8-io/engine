-- Revert the parked-row prune index.
DROP INDEX IF EXISTS idx_webhook_outbox_parked_created;
