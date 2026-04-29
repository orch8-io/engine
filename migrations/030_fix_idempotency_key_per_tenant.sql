-- Fix #3: idempotency_key UNIQUE must be scoped per-tenant.
-- The original migration 010 created a global UNIQUE(idempotency_key) constraint,
-- meaning tenant A's key "order-123" would block tenant B from using the same key.
--
-- Drop the global unique index and replace with a composite (tenant_id, idempotency_key).

-- Postgres: drop old constraint, add composite unique.
-- The column was added via `ALTER TABLE ... ADD COLUMN idempotency_key TEXT UNIQUE`
-- which creates an auto-named unique constraint.

DO $$
BEGIN
    -- Drop the auto-generated unique constraint (name varies by PG version).
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'task_instances'::regclass
          AND contype = 'u'
          AND array_length(conkey, 1) = 1
          AND conkey[1] = (
              SELECT attnum FROM pg_attribute
              WHERE attrelid = 'task_instances'::regclass
                AND attname = 'idempotency_key'
          )
    ) THEN
        EXECUTE (
            SELECT format('ALTER TABLE task_instances DROP CONSTRAINT %I',
                          conname)
            FROM pg_constraint
            WHERE conrelid = 'task_instances'::regclass
              AND contype = 'u'
              AND array_length(conkey, 1) = 1
              AND conkey[1] = (
                  SELECT attnum FROM pg_attribute
                  WHERE attrelid = 'task_instances'::regclass
                    AND attname = 'idempotency_key'
              )
            LIMIT 1
        );
    END IF;
END$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_task_instances_tenant_idempotency
    ON task_instances (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
