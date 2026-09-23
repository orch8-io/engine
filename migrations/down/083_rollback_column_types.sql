-- Rollback 083_rollback_column_types.sql
--
-- Restores the original 037 INTEGER/REAL column types. Note that the storage
-- layer decodes BOOLEAN/DOUBLE PRECISION, so rolling back reintroduces the
-- rollback-policy decode failure this migration fixed.

ALTER TABLE rollback_history
    ALTER COLUMN alert_sent DROP DEFAULT,
    ALTER COLUMN alert_sent TYPE INTEGER USING (CASE WHEN alert_sent THEN 1 ELSE 0 END),
    ALTER COLUMN alert_sent SET DEFAULT 0,
    ALTER COLUMN error_rate TYPE REAL,
    ALTER COLUMN threshold TYPE REAL;

ALTER TABLE rollback_policies
    ALTER COLUMN enabled DROP DEFAULT,
    ALTER COLUMN enabled TYPE INTEGER USING (CASE WHEN enabled THEN 1 ELSE 0 END),
    ALTER COLUMN enabled SET DEFAULT 1,
    ALTER COLUMN error_rate_threshold TYPE REAL;
