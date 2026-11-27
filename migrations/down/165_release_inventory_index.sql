-- Remove the index added for paging a tenant's release inventory.
DROP INDEX IF EXISTS idx_releases_inventory;
