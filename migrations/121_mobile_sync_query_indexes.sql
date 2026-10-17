-- MS-13: indexes matching the real mobile-sync queries (migration 038 only
-- created single-column ones). Each statement below was checked against the
-- WHERE / ORDER BY it serves:
--
--   mobile_devices (tenant_id, registered_at DESC)
--     list_mobile_devices:  WHERE tenant_id = $1 ORDER BY registered_at DESC LIMIT $2
--   mobile_approval_requests (tenant_id, created_at DESC, id DESC)
--     list_mobile_approvals_page:  WHERE tenant_id = $1 [AND state = $2]
--     ORDER BY created_at DESC, id DESC LIMIT .. OFFSET ..  (keyset-ordered)
--   mobile_commands (acked_at) WHERE acked_at IS NOT NULL
--     cleanup_acked_commands:  acked_at IS NOT NULL AND acked_at < now() - ..
--   mobile_commands (created_at) WHERE acked_at IS NULL
--     cleanup_expired_commands:  acked_at IS NULL AND created_at < now() - ..
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create them online before upgrading (outside a transaction,
-- one at a time); the IF NOT EXISTS below is then a no-op:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_devices_tenant_registered
--       ON mobile_devices (tenant_id, registered_at DESC);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_approvals_tenant_created_id
--       ON mobile_approval_requests (tenant_id, created_at DESC, id DESC);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_commands_acked_at
--       ON mobile_commands (acked_at) WHERE acked_at IS NOT NULL;
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_commands_unacked_created
--       ON mobile_commands (created_at) WHERE acked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_mobile_devices_tenant_registered
    ON mobile_devices (tenant_id, registered_at DESC);
CREATE INDEX IF NOT EXISTS idx_mobile_approvals_tenant_created_id
    ON mobile_approval_requests (tenant_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_mobile_commands_acked_at
    ON mobile_commands (acked_at) WHERE acked_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mobile_commands_unacked_created
    ON mobile_commands (created_at) WHERE acked_at IS NULL;
