-- 037 created these columns as INTEGER/REAL, but the Postgres storage layer
-- binds and decodes them as BOOLEAN/DOUBLE PRECISION (matching the Rust
-- model's bool/f64). The mismatch made every rollback-policy/history read
-- fail with a decode error on Postgres, so auto-rollback never fired.
--
-- Both tables are tiny control-plane tables, so the rewrite is cheap.

ALTER TABLE rollback_policies
    ALTER COLUMN enabled DROP DEFAULT,
    ALTER COLUMN enabled TYPE BOOLEAN USING (enabled <> 0),
    ALTER COLUMN enabled SET DEFAULT TRUE,
    ALTER COLUMN error_rate_threshold TYPE DOUBLE PRECISION;

ALTER TABLE rollback_history
    ALTER COLUMN alert_sent DROP DEFAULT,
    ALTER COLUMN alert_sent TYPE BOOLEAN USING (alert_sent <> 0),
    ALTER COLUMN alert_sent SET DEFAULT FALSE,
    ALTER COLUMN error_rate TYPE DOUBLE PRECISION,
    ALTER COLUMN threshold TYPE DOUBLE PRECISION;
