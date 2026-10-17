-- D-02: partial index for the parked-row retention prune
-- (`delete_parked_webhook_outbox_before`: `status = 'parked' AND created_at
-- < cutoff`, run hourly by the webhook drain loop). The only pre-existing
-- index on the outbox age (`idx_webhook_outbox_created`) is a full-table
-- `created_at DESC` index, so after a long receiver outage the prune had to
-- filter millions of pending/parked rows. The prune is now batched and
-- capped per tick; this index makes each batch a short ordered scan.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create it online before upgrading (outside a transaction):
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_webhook_outbox_parked_created
--       ON webhook_outbox (created_at) WHERE status = 'parked';
CREATE INDEX IF NOT EXISTS idx_webhook_outbox_parked_created
    ON webhook_outbox (created_at) WHERE status = 'parked';
