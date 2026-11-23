-- Rollback of 149: Remove absolute expiry metadata from instance KV rows.
ALTER TABLE instance_kv_state DROP COLUMN IF EXISTS expires_at;
