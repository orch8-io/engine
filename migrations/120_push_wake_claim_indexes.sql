-- MS-03: `claim_due_wakes` ORed a `status='pending'` branch with an
-- unindexed `status='in_flight' AND lease_until<=now` branch, which made the
-- partial `idx_push_wake_due` unusable. The claim is now two UNION ALL
-- branches, each with its own partial index:
--   * pending, oldest first:      (created_at)  WHERE status = 'pending'
--   * expired leases to re-drive: (lease_until) WHERE status = 'in_flight'
-- Both partials stay tiny (terminal rows, the bulk of the table, are excluded).
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create them online before upgrading (outside a transaction,
-- one at a time); the IF NOT EXISTS below is then a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_push_wake_pending_created
--       ON push_wake_outbox (created_at) WHERE status = 'pending';
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_push_wake_inflight_lease
--       ON push_wake_outbox (lease_until) WHERE status = 'in_flight';
CREATE INDEX IF NOT EXISTS idx_push_wake_pending_created
    ON push_wake_outbox (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_push_wake_inflight_lease
    ON push_wake_outbox (lease_until) WHERE status = 'in_flight';
