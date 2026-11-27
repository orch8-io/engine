-- Index each tenant's release inventory in cursor order.
CREATE INDEX IF NOT EXISTS idx_releases_inventory ON workflow_releases(tenant_id, created_at DESC, id DESC);
